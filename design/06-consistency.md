# 06 — Consistency Model

Mist promises **close-to-open consistency across the VM boundary with bounded, observable
staleness everywhere else**. This doc states the guarantees precisely, shows the mechanism that
enforces each, and specifies behavior under every failure we expect.

## 1. Definitions

- *Journal lag* `L`: time from a guest kernel event to its replica application (target p99 ≤ 5 ms
  idle, ≤ 50 ms under 10k-events/s storm [E]).
- *Visible-on-open*: an `open(2)` on one side observes the other side's last close-commit.
- *Commit (guest→Mac)*: guest `close(2)` of a written file (CLOSE_WRITE) or `fsync`.
- *Commit (Mac→guest)*: NFS COMMIT (or FILE_SYNC write) acknowledged — i.e. Mac `close()` of a
  written file returns (macOS client flushes on close per close-to-open).

## 2. Guarantees

**G1 (guest → Mac, close-to-open).** If a guest process closes file F (or fsyncs) at time T, any
Mac `open(F)` issued after T + L observes the new content and attributes.
*Mechanism:* CLOSE_WRITE → `Rec::Content{in_progress:false}` → replica attrs (mtime, size,
content_version) updated within L → Mac open triggers NFS GETATTR (close-to-open revalidation,
unconditional) → changed mtime/version ⇒ client purges cached pages → READ fetches fresh data
(CAS keyed by content_version cannot serve stale). With a v4.1 delegation held, CB_RECALL
replaces the GETATTR-on-open as the freshness edge and typically lands *before* the app reopens.

**G2 (Mac → guest, close-to-open + durability).** When a Mac `close()`/`fsync()` returns, the
data is applied **and fdatasync'd** in the guest (share `commit = fsync` default); any guest
`open()` after that observes it.
*Mechanism:* macOS client flushes UNSTABLE writes + COMMIT on close; hostd's COMMIT handler
blocks on the guest `Commit` RPC (pwritev + fdatasync) — never optimistic.

**G3 (same-side semantics untouched).** Guest-local processes see native ext4 semantics (Mist is
not in their path). Mac-local processes see standard macOS-NFS-client semantics against a
journal-fresh server.

**G4 (rename atomicity).** A guest `rename()` appears on the Mac as one atomic transition
(single `Renamed` record → single applier mutation under the dir locks); never both-names or
neither-name. Mac→guest renames likewise (optimistic apply is a single replica op; guest
`renameat2` is atomic).

**G5 (no lost guest truth).** The guest filesystem is always the authority: any divergence path
(missed events, crash, conflict) terminates in the replica converging to the guest state via
journal, resync diff, or scrub — never the reverse (Mist never "restores" replica state into the
guest, except explicit Mac-originated mutations).

## 3. Staleness table (what a Mac process can observe, default profile)

| Mac observation | Worst-case staleness vs guest | Bounded by |
|---|---|---|
| `open()` + read | L (~ms) | G1 |
| read of already-open file | until next open (POSIX close-to-open norm); with v4.1 delegation: recall latency (~ms) | client cache / CB_RECALL |
| `stat()` (no open) | `actimeo` (5 s) after a guest change | client attr cache; v4.1 delegation removes this for delegated files |
| `readdir` | `actimeo` (5 s) | client dir cache; READDIRPLUS repopulates from journal-fresh replica |
| `tail -f` style growth | ~1 s (MODIFY coalescer) + L | `Rec::Content{in_progress}` 1 Hz |
| mmap-written-by-guest, never closed/fsynced | **unbounded** (documented blind spot) | verify-on-access backstop on next guest-side `Read` RPC; scrubber; `mist sync`; `verify-on-open` share option for the paranoid |

Guest observation of Mac writes: 0 after G2 commit (it's the real filesystem).

## 4. Ordering

- The journal is a **per-share total order** (seq). Replica application preserves it exactly
  (single applier). Therefore Mac-visible metadata never reorders against guest causality within
  a share. No cross-share ordering promise.
- Mac-originated mutations interleave at the point the optimistic apply enqueues (per-node
  serialization guarantees per-file order matches guest apply order). Cross-file Mac-write
  ordering follows NFS client semantics (unordered until COMMIT), unchanged from any NFS.

## 5. Seeding correctness (consistent cut)

Claim: journal-before-snapshot + idempotent upserts ⇒ post-seed replica ≡ guest tree at
`SnapDone` time (modulo records still in flight, covered by L).
Sketch: every mutation m during the walk either (a) lands before the walker visits the affected
dir ⇒ snapshot reflects it; replaying m's journal record is an idempotent no-op; or (b) lands
after the visit ⇒ snapshot misses it, but m's record is in the buffered journal and applies
afterward; or (c) races within a dir read ⇒ getdents is not atomic, but the journal record for m
exists (journal started first) and repairs in replay. Deletions of visited dirs are handled by
tombstone-on-`Removed` replay; the visited-set sweep covers subtrees deleted mid-walk.

## 6. Conflicts (concurrent cross-boundary writes)

Window: Mac holds DIRTY(F) (optimistic mutation in flight or uncommitted write-back) while a
journal record for F arrives that is **not** our echo (echo fences distinguish; `02` §6.5).

| Mac in-flight | Guest event | Resolution |
|---|---|---|
| data write | `Content` | **last-close-wins**: guest version applied to replica; Mac's still-buffered pages proceed; whoever COMMITs/closes last ends up as F's content. Conflict logged with both versions' (size, mtime). |
| data write | `Removed`/`SelfRemoved` | guest wins: node tombstoned; Mac's COMMIT gets ESTALE (client surfaces EIO to the app — correct: the file is gone) |
| metadata (chmod/utimes) | `AttrChanged` | guest wins (its record is later truth); our RPC will re-apply on top anyway — final state = guest kernel's serialization of both ops |
| create(name) | `Created(name)` | guest entry wins the dentry; our Create RPC returns EEXIST → NFS EEXIST (apps handle) |
| rename | any on same names | applier order in guest kernel decides; replica converges via records; anomalies self-heal via RescanDir |

All conflicts increment `mist_conflicts_total` and append a structured row to
`~/Library/Application Support/Mist/conflicts.log` (`mist conflicts` to view). Close-to-open
workloads (the design target) make these rare; we make them *visible*, not silent.

## 7. Degraded & failure behavior

Deadlines: mutation-path RPC unavailable ⇒ NFS replies JUKEBOX-delay (client retries) for up to
**30 s**, then EIO. Read-path: replica serves regardless; cold data reads follow the same
deadline. State is surfaced (`mist status`: LIVE / SEEDING / RESYNCING / DEGRADED / OFFLINE).

| Event | Replica | Mounts/NFS | Recovery |
|---|---|---|---|
| mistd restart | intact, stale | serve stale; mutations JUKEBOX | epoch mismatch ⇒ diff-resync; lag metric resets |
| hostd crash | lost (RAM) | client retries against restarted server (handles survive via persisted HMAC secret); uncommitted UNSTABLE writes replayed by client (COMMIT verifier mismatch) | launchd restart ≤ 1 s; reseed (SEEDING serves as it fills); CAS + side-store persistent |
| bridge/session drop | intact, staleness grows | as mistd restart | redial; same-epoch ⇒ diff-resync |
| guest reboot | invalid | JUKEBOX/EIO per deadline | new epoch ⇒ full reseed; delegations recalled-all before serving |
| Mac sleep/wake | intact | NFS client reconnects TCP | session ping resumes; vsock-over-bridge re-dialed if dropped; e2e verifies AVF behavior across VM pause |
| journal overflow | suspect | serve (stale-bounded) | quiescent-storm ladder (`03` §4.6) → diff-resync; recalls fire only for real diffs |
| scrub divergence | localized suspect | serve | targeted RescanDir; counter alarms if recurring |
| ENOSPC/EDQUOT in guest | intact | writes fail with correct errno | share health flag in status |

## 8. Invariants (checked in debug builds & model tests)

- I1 per-dir cookies strictly increase; no reuse within an epoch.
- I2 a live NodeKey has ≥ 1 dentry or is the root; tombstones have 0.
- I3 every DIRTY node has an in-flight or queued mutation txn; cleared on ack/rollback.
- I4 delegation table ⊆ live nodes; recalled before tombstone completes.
- I5 side-store anchor keys ⊆ live nodes ∪ linger window.
- I6 `complete=false` dirs never serve READDIR pages.
- I7 replica never holds a lock across an `await` (compile-time lint via clippy `await_holding_lock` + custom check).
- I8 applied seq is contiguous per share-epoch; gap ⇒ RESYNCING within one batch.
