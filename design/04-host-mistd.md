# 04 — `mist-hostd` (Host Daemon) Low-Level Design

The host daemon owns everything on the Mac: VM sessions, the metadata replicas, the data caches,
the mount surfaces, and the control plane. It is the component where almost all Mist code runs.

## 1. Runtime & task map

tokio multi-thread runtime (default threads = cores). Replica operations are short, synchronous,
lock-protected RAM mutations executed inline (no per-op task hops); only transport and disk I/O
are async/offloaded.

```
mist-hostd
├── control server            UDS jsonl API (mist CLI, editors)            §9
├── per-VM session supervisor  dial → hello → lanes → health (ping)        §2
│   ├── journal pump           batches → per-share applier (single task)   §4
│   ├── rpc multiplexer        seq map, timeouts, retries                  §5.1
│   ├── bulk scheduler         snapshot ingest + read transfers + readahead §5.2
│   └── snapshot assembler     SnapDir stream → replica                    §4.4
├── per-share
│   ├── replica                                                            §3
│   ├── resync controller      overflow/epoch → diff walk                  §4.5
│   ├── scrubber               background sample verification              §4.6
│   └── side-store                                                          §7
├── CAS manager                ingest, eviction, scrub                     §6
├── surfaces                   NFSv3 / NFSv4.1 servers (05), FSKit         §8
└── metrics + health           Prometheus optional                         §10
```

Lock ordering (deadlock rule, enforced by convention + debug assertions):
`session → share → node-shard → dir`. **No guest RPC is ever awaited while holding any replica
lock** — handlers copy what they need, drop locks, await, re-lock to apply.

## 2. Bridge attach & session supervision

- Connector `vsock`: connect UDS `…/Mist/vms/<vm>.sock`, write `CONNECT 6478\n`, expect
  `OK 6478\n` (Firecracker convention), then the stream is raw MWP. One UDS connection per lane.
- `MistBridge` (Swift, ~150 LoC): `attach(vm:device:socketURL:)` — a `VZVirtioSocketListener` is
  *not* needed (host always dials); the bridge accepts UDS, parses CONNECT, calls
  `device.connect(toPort:)`, then splices fds with two `DispatchIO` pumps. Ships with
  `mist-vmshim`, a 100-line reference supervisor for tests/e2e.
- Reconnect: exponential backoff 100 ms → 5 s; on HelloAck compare `(boot_id, share epochs)`
  against the live session record → diff-resync (same epoch — cheap walk skips unchanged dirs)
  or full reseed (epoch changed). Mounts and replica keep serving throughout (`06` §7).

## 3. Replica — data structures

```rust
pub struct ShareReplica {
    share: ShareId,
    epoch: AtomicU64,
    state: AtomicCell<ShareState>,        // Seeding | Live | Resyncing | Offline | Degraded
    shards: [RwLock<HashMap<NodeKey, Node>>; 256],   // parking_lot; shard = hash(NodeKey)
    root: NodeKey,
    last_seq: AtomicU64,
    counters: ReplicaStats,
}

struct Node {
    attr: Attr,                            // 96 B fixed + optional symlink target
    parent: NodeKey,                       // one of its parents (hardlinks: primary)
    dir: Option<Box<DirState>>,            // Kind::Dir only
    flags: NodeFlags,                      // PENDING | TOMBSTONE | DIRTY | SUSPECT
    dirty: Option<Box<DirtyState>>,        // Mac-originated mutation in flight     §4.3
    deleg: Option<DelegId>,                // granted delegation
}

struct DirState {
    by_cookie: BTreeMap<u64, DirEnt>,      // iteration order = insertion cookies
    by_name: HashMap<NameBuf, u64>,        // exact-name → cookie
    norm: HashMap<u64, SmallVec<u64>>,     // NFC-casefold hash → cookies (Finder lookups, 05 §6.4)
    next_cookie: u64,                      // starts at 3 (1=".", 2=".." synthesized)
    dirgen: u64,                           // bumped on any entry mutation (cookieverf, resync)
    complete: bool,                        // false until SnapDir(last) lands
}
struct DirEnt { name: NameBuf, node: NodeKey, kind: Kind }
```

- **Cookie stability** (NFS requirement): cookies are per-dir monotonic, never reused; rename
  within a dir keeps the entry's cookie; concurrent inserts appear at the iteration tail. A
  client resuming readdir with a cookie from a previous `dirgen` gets `NFSERR_BAD_COOKIE` only
  if that cookie vanished (else we continue — standard server behavior).
- **Pending nodes**: created by journal records that reference unknown NodeKeys; carry no attrs;
  resolved by snapshot/`Lookup` RPC; 60 s TTL → drop + anomaly counter.
- **Tombstones**: nodes whose last dentry was removed but which an NFS client may still hold a
  handle to; GETATTR on a tombstone returns `ESTALE` after a 60 s linger (macOS client copes).
- Memory math [E]: 1M nodes ≈ node table (1M × ~160 B incl. map overhead) + dentries
  (1M × ~90 B incl. 30 B avg name ×2 indexes) ≈ **~260 MB/1M nodes**; release budget ≤ 700 MB
  at 1M (headroom for pending/norm/dirty). Compact layout: name arena + u32 ids +
  open-addressing maps ⇒ target ≤ 350 MB. Hard cap `max_nodes` (default 10M/share) → SEEDING
  aborts with a clear error rather than OOM.

## 4. Replica writers

Single-writer discipline: exactly one task per share (the **applier**) mutates the replica —
journal batches, snapshot records, resync diffs, and optimistic Mac-mutation applies are all
funneled to it via an spsc queue (optimistic applies enqueue synchronously from NFS handlers and
wait ≤1 ms; in practice the queue is near-empty).

### 4.2 Journal apply (normative pseudocode)

```
apply(rec):
  match rec:
    Created{p, name, node, attr}:
      d = dir(p) or { rescan(p); return }
      if let Some(old) = d.by_name.insert(name → new cookie for node):
          unlink_dentry(old)                       # upsert: replaces silently
      upsert_node(node, attr, parent=p)            # None attr ⇒ PENDING
      d.dirgen += 1; inval(p, DirChanged)          # recall hook
    Removed{p, name}:
      d = dir(p) or return
      if let Some(c) = d.by_name.remove(name):
          n = d.by_cookie.remove(c).node
          d.dirgen += 1; inval(p, DirChanged)
          drop_link(n)                              # nlink--; 0 ⇒ tombstone + sidestore/CAS GC + recall
    Renamed{fp, fname, tp, tname}:
      take dentry (fp,fname); if missing ⇒ Lookup-RPC repair path (anomaly++)
      upsert into (tp,tname) keeping node identity; both dirgens bump; inval both
    AttrChanged{node, attr}: upsert attrs; inval(node, AttrChanged)
    Content{node, v, size, mtime, in_progress}:
      n.attr.{size,mtime,content_version} = …
      if n.dirty.is_some() and !echo_expected(node): conflict!(node)        # 06 §6
      inval(node, ContentChanged{v})                # recall read delegation
    SelfRemoved{node}: tombstone(node)
    Overflow{}: resync_controller.trigger(OverflowGap)
    EchoMarker{tag}: fences.complete(tag)
invariant-violation anywhere ⇒ RescanDir(parent) + anomaly counter (self-heal, never panic)
```

`inval(…)` fans out to `MountSurface::subscribe_invalidations` listeners (delegation manager,
FSKit-compatible front, `mist events` subscribers).

### 4.3 Optimistic Mac mutations & dirty state

NFS mutation handler flow (e.g. WRITE/CREATE/RENAME):
1. Validate against replica (perms via mapped identity, existence, type checks) — fail fast with
   correct NFS errors without bothering the guest.
2. Enqueue optimistic apply: mutate replica as if done; set `DIRTY(txid)`; record pre/post attrs
   needed by NFS wcc data.
3. Issue ordered guest RPC (per-node serialization); on reply: clear DIRTY, reconcile attrs from
   guest truth (guest attrs win — e.g. real ctime), request echo fence.
4. On RPC error: **roll back** the optimistic apply (inverse op recorded in DirtyState), return
   the mapped errno to NFS. On session loss mid-flight: DIRTY nodes become SUSPECT; resync
   reconciles; NFS gets `JUKEBOX`-delay then EIO per `06` §7 deadlines.

COMMIT blocks on the Commit RPC ack (fdatasync chain) — that is guarantee G2, never optimistic.

### 4.4 Snapshot assembler

Consumes `SnapDir` records: upsert dir node + entries (same upsert primitive as journal),
`complete=true` on `last`. Orphans (child dir record before parent) park in pending; resolved as
parents land. After `SnapDone`: replay buffered journal batches (recorded since `AttachShare`),
then state → LIVE. Readdir on incomplete dirs blocks ≤5 s then `JUKEBOX`-delays (client retries).

### 4.5 Resync controller (diff mode)

Trigger: Overflow, epoch change, reconnect, `mist sync`, scrubber escalation.
Mechanism: ordinary snapshot stream, diffed against the live replica:
- Dir record where `(dirgen-relevant content)` matches: compare entry sets cheaply
  (names+NodeKeys+`(size,mtime,ctime,content_version-relevant attrs)`); identical ⇒ skip.
- Differences ⇒ synthesize the minimal journal-equivalent records through the same `apply()`
  (so recalls/conflict checks/side-store GC fire exactly as if events had arrived).
- Nodes never visited by the walk that still exist in replica ⇒ removed subtree ⇒ tombstone pass
  (the assembler tracks visited dirs; sweep at SnapDone).
During RESYNCING the share keeps serving (staleness contract `06` §7).

### 4.6 Scrubber (silent-divergence insurance)

Background task, default 200 nodes/s (<1 % CPU): rotating `StatBatch` over the replica, compare,
on mismatch ⇒ targeted `RescanDir` + `mist_scrub_divergence` counter. A nonzero rate means a
fanotify assumption broke — surfaced loudly in `mist status`/doctor. Full pass over 1M nodes ≈
1.5 h; rate adaptive (backs off when session busy).

## 5. RPC client & data movement

### 5.1 Multiplexer
seq→waiter map; per-class timeouts (`02` §5); retries only for idempotent reads (Stat/Read) after
reconnect; mutations are never auto-retried (the resync/DIRTY reconciliation handles uncertainty —
no double-apply risk).

### 5.2 Bulk scheduler & readahead
Per-open-file sequential detector (last offset + stride): grows a readahead window
(256 KiB → 16 MiB, halve on miss) issuing pipelined `Read`s sized 1–4 MiB across the 2 bulk
lanes. NFS READ latency hides behind the window after the first hit. Cancels on close/random-seek.
Cold first-byte budget [E]: ≤ 300 µs + size/throughput.

## 6. CAS (content cache)

- Layout: `~/Library/Caches/Mist/cas/{aa}/{blake3-hex}` blob files (chunk granularity);
  `manifests.redb`: `(share, NodeKey, content_version) → Vec<(off, len, hash)>`;
  `chunks.redb`: `hash → (len, refcount, last_access_day)`.
- Chunking: whole-blob ≤ 256 KiB; larger files FastCDC (min 256 KiB / avg 1 MiB / max 4 MiB) —
  dedup across content_versions (git packfiles, build artifacts) without tiny-chunk overhead.
- Read path: manifest hit → `pread` blob (macOS UBC makes repeats RAM-speed); partial-manifest
  miss → fetch missing ranges only. Ingest is write-through from guest reads (hash verify on
  ingest). `version` mismatch reported by guest `Read` ⇒ drop manifest, refetch.
- Eviction: LRU by `last_access_day`, low/high watermarks (default 18/20 GiB), never evict
  manifest-referenced chunks of files opened in the last 10 min. `mist cache stats|clear`.
- Integrity: `mist cache scrub` re-hashes sampled blobs; corrupt ⇒ drop (refetch on demand).
- First-run: `tmutil addexclusion` + `mdutil -i off`-equivalent xattr on the cas dir.

## 7. Side-store (Apple metadata virtualization)

Purpose: Finder/editor noise never reaches the guest; Mac-only metadata persists.

- Store: `sidestore-<share>.redb`: key `(NodeKey | (parent NodeKey, name))` →
  `{ kind: DsStore | AppleDouble | Xattr{name}, data ≤ 1 MiB }` + small blob spillover dir.
- Interception matrix (decision happens in the NFS layer before MountSurface; `05` §6):
  creates/writes/reads/deletes of `.DS_Store`, `._*`, `.fseventsd/*`, `.Spotlight-V100/*`,
  `.Trashes/*`, `.TemporaryItems/*` → side-store, synthesized into readdir listings only when
  `dsstore = allow`-style profiles demand (default: hidden from listings, served on direct
  lookup — Finder is satisfied, `ls` on the Mac stays clean, guest never sees them).
- If a same-named *real* entry exists in the replica (guest actually has a `.DS_Store`), the real
  one wins and pass-through applies — rule: side-store only owns names absent in the guest.
- GC: journal `Removed`/tombstone of the anchor node drops its side-store rows.
- xattrs written by the Mac (quarantine, FinderInfo, labels): stored as `Xattr` rows
  (NFSv3 surface: arrive as AppleDouble `._*` files — stored structured, not as junk files;
  v4.1 maps real XATTR ops if enabled; either way same store).

## 8. `MountSurface` & front integration

Trait per `01` §5. The NFS servers run inside hostd (no extra process hop): ONC-RPC over TCP
127.0.0.1, each surface a tokio listener. Per-request flow: decode XDR → handle lookup
(HMAC-verified, `05` §4.1) → replica/CAS/RPC per the routing table → encode. Handlers are async
only when they must await (cold read, mutation ack, COMMIT); the metadata hot path is
lock + RAM + encode, target ≤ 40 µs in-process [E].

## 9. Control plane (UDS, newline-delimited JSON)

`{ "v":1, "cmd":"status" }` → typed replies. Verbs: `status` (sessions, shares, states, lag,
anomalies), `shares`, `attach/detach {vm}`, `mount/umount {vm, share}`, `sync {path|share}`
(targeted RescanDir / full resync), `events {follow: bool, path?: prefix}` (journal tail as
JSON — feeds editor integrations), `conflicts {clear?}`, `cache {stats|clear|scrub}`,
`doctor` (runs the checklist: bridge reachable, RTT probe, kernel ≥ floor, fanotify limits,
mount options match profile, mdutil/TM exclusions, scrub divergence == 0), `metrics`, `version`.
Auth: UDS file mode 0600 (same-user only) — consistent with the single-user trust model.

## 10. Metrics (Prometheus names)

`mist_journal_lag_ms{share}` (guest_mono → applied), `mist_journal_records_total`,
`mist_overflows_total`, `mist_resyncs_total{reason}`, `mist_scrub_divergence_total`,
`mist_nfs_op_duration_us{proc,quantile}`, `mist_rpc_duration_us{kind,quantile}`,
`mist_replica_nodes{share}`, `mist_replica_bytes{share}`, `mist_cas_{hits,misses,bytes}`,
`mist_recalls_total{reason}`, `mist_conflicts_total`, `mist_session_state{vm}`.

## 11. Degradation ladder & ceilings

Memory ceiling (default 2 GiB): approaching ⇒ shed in order: CAS in-RAM buffers → norm indexes
(rebuildable) → refuse new share attach → (never) drop replica of mounted shares — instead
surface `mist status` ERROR. Disk ceilings: CAS watermarks; side-store cap 1 GiB/share [E].
All ceilings are config keys under `[daemon.limits]`.

## 12. launchd

`dev.mist.hostd.plist` (LaunchAgent): `KeepAlive`, `RunAtLoad`, `ProcessType Interactive`,
stdout/err to `~/Library/Logs/Mist/hostd.log` (tracing also self-rotates), `ThrottleInterval 1`.
Install via Homebrew formula (`brew services`) or `mist doctor --install-agent`.
