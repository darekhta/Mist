# 02 — Mist Wire Protocol (MWP/1)

Transport-agnostic protocol between `mist-hostd` (dialer) and `mistd` (listener) over N reliable
ordered byte streams ("lanes"). Design principles:

1. **Host-authoritative replica, guest-authoritative files.** The guest never tells the host what
   to *do*, only what *changed* (journal) and what *is* (snapshot/RPC replies). The host never
   sends the guest a path string — only `(parent NodeKey, name)` pairs and NodeKeys.
2. **Idempotent, order-tolerant application.** Every journal/snapshot record can be applied twice
   or arrive interleaved with a snapshot without corrupting the replica (upsert semantics, §6.3).
   This is what makes the consistent-cut seeding trivial and reconnects safe.
3. **Guest input is hostile input.** Every field has a hard bound; decode failures kill the
   session, never the daemon (`07-security.md` §3).

Encoding: [postcard](https://docs.rs/postcard) (compact serde) for all payloads. Integers are
varint where postcard says so; explicitly-sized fields below are inside fixed structs. Byte order
in the frame header: little-endian.

## 1. Framing

Every lane carries a sequence of frames:

```
┌────────┬────────┬────────┬────────┬───────────────┐
│ len:u32│ kind:u8│ flags:u8│ rsv:u16│ seq:u64        │ payload[len]
└────────┴────────┴────────┴────────┴───────────────┘
header = 16 bytes; len = payload bytes only
```

- `kind`: `0=CTL, 1=REQ, 2=RESP, 3=EVENT, 4=BULK`.
- `flags`: bit0 `MORE` (payload continues in next frame, same seq — used for streamed
  bulk transfers), bit1 `COMPRESSED` (lz4; only on journal/snapshot lanes, negotiated).
- `seq`: per-lane monotonically increasing for REQ; RESP echoes its REQ's seq; EVENT carries the
  emitter's stream sequence; CTL seq is informational.
- Limits: `len ≤ 1 MiB` on ctl/journal/rpc lanes; `≤ 4 MiB` on bulk lanes. Violation ⇒ session
  abort with `ProtocolError`.

## 2. Session establishment

1. Host opens the **ctl** lane first, sends `Hello`; mistd replies `HelloAck` (or `AuthFail` and
   closes).
2. Host opens remaining lanes; first frame on each is `StreamHello { session_id, lane }`.
3. Host sends `AttachShare` per configured share; mistd starts the journal for it immediately
   (before snapshot — consistent cut, §6.4), then host requests `SnapshotStart`.

```rust
struct Hello {
    proto: u32,            // MWP version = 1
    features: u64,         // bitset, §9
    token: [u8; 32],       // BLAKE3(token_file contents); constant-time compare
    host_name: String,     // ≤64 bytes, logging only
    host_version: String,  // ≤32
}
struct HelloAck {
    proto: u32,
    features: u64,         // intersection is the effective feature set
    boot_id: u64,          // random per mistd start
    session_id: u64,       // random; echoed in StreamHello
    shares: Vec<ShareInfo>,// ≤64
    guest: GuestInfo,      // kernel version, fanotify limits — for `mist doctor`
}
struct ShareInfo {
    id: ShareId,           // u16, stable per mistd config order
    name: String,          // ≤64, config section name
    epoch: u64,            // hash(boot_id, fsid, mount instance) — reseed trigger
    fsid: u64,             // guest fs identity; pinned by mistd for handle checks
    root: NodeKey,
    flags: u32,            // SUBTREE, RDONLY, COMMIT_FSYNC, …
    ino_bits: u8,          // 32 for ext4
}
```

## 3. Core types

```rust
struct ShareId(u16);
struct NodeKey { ino: u64, gen: u32 }          // ext4: ino fits in u32; wire keeps u64
struct Ts { sec: i64, nsec: u32 }
enum Kind { Reg, Dir, Symlink, Fifo, Sock, Chr, Blk }
struct Attr {
    kind: Kind,
    mode: u16,            // permission bits + suid/sgid/sticky
    nlink: u32,
    uid: u32, gid: u32,
    size: u64,
    blocks: u64,          // 512B units
    mtime: Ts, ctime: Ts, // atime intentionally absent (served as mtime; ADR-16)
    rdev: u64,            // Chr/Blk only
    content_version: u64, // data-cache epoch (mistd counter)
    symlink_target: Option<Bytes>, // Kind::Symlink only, ≤4096; set at create, immutable
}
struct Name(Bytes);       // single path component: 1..=255 bytes, no '/', no NUL, not "."/".."
```

`Name` validation is enforced on **decode** (both directions). `Attr` total wire size ≤ 4 KiB
(symlink target included).

## 4. CTL messages (lane `ctl`)

| Msg | Dir | Payload | Semantics |
|---|---|---|---|
| `Hello`/`HelloAck`/`AuthFail` | h→g / g→h | §2 | once |
| `StreamHello { session_id, lane: LaneId }` | h→g | — | first frame of every non-ctl lane |
| `AttachShare { share }` | h→g | — | mistd marks fanotify (if not yet), starts journaling this share |
| `DetachShare { share }` | h→g | — | stop journaling (refcounted across sessions) |
| `SnapshotStart { share, snap_id: u64 }` | h→g | — | walker begins; records arrive on a bulk lane |
| `SnapshotAbort { snap_id }` | h→g | — | cancel walker |
| `RescanDir { share, dir: NodeKey, snap_id }` | h→g | — | targeted re-enumeration (self-heal); same record format as snapshot |
| `Ping { nonce } / Pong { nonce, guest_mono_ns }` | h→g / g→h | — | 1 s cadence; 3 misses ⇒ DEGRADED; mono clock feeds lag metrics |
| `ShareGone { share }` | g→h | — | guest unmounted the share's fs; host sets OFFLINE |
| `Goodbye { reason }` | both | — | graceful close |

## 5. RPC (lane `rpc`; requests h→g, responses g→h, out-of-order by seq)

Errors: every RESP is `Result<T, RpcErr>`; `RpcErr { errno: i32, msg: String≤128 }` carries the
guest errno verbatim (mapped to NFS status by the front; table in `05` §4.3). Timeouts (host
side): reads 10 s, mutations 30 s, control 5 s; timeout ⇒ session DEGRADED (the NFS layer holds
or replies `JUKEBOX`-style delay per `06` §7).

Read-side:

```rust
Stat       { share, node: NodeKey }                       -> Attr
StatBatch  { share, nodes: Vec<NodeKey> /* ≤4096 */ }     -> Vec<Option<Attr>>   // scrub, resync
Lookup     { share, dir: NodeKey, name: Name }            -> (NodeKey, Attr)      // pending-node resolution only
ReadDirRaw { share, dir: NodeKey }                        -> streamed SnapDir     // verify path, not hot path
Read       { share, node: NodeKey, version_hint: u64,
             off: u64, len: u32 /*≤4MiB*/, ra: u32 }      -> ReadResp (bulk lane)
// ReadResp { version: u64, eof: bool, data follows as BULK frames }
// version != version_hint ⇒ host invalidates CAS manifest for old version, restarts read.
// mistd side effect: re-stat on open; if drift detected vs replica-known attrs ⇒ emits
// Rec::Content (the verify-on-access backstop for mmap writers).
```

Mutations (host applies optimistically to the replica first; `04` §4.3):

```rust
Create  { share, dir: NodeKey, name: Name, kind: Kind, mode: u16,
          ident: Identity, rdev: u64, symlink_target: Option<Bytes> }
        -> (NodeKey, Attr, parent_post: Attr)
Unlink  { share, dir: NodeKey, name: Name }                -> parent_post: Attr
Rmdir   { share, dir: NodeKey, name: Name }                -> parent_post: Attr
Rename  { share, from: (NodeKey, Name), to: (NodeKey, Name) } -> (from_post: Attr, to_post: Attr)
Link    { share, node: NodeKey, dir: NodeKey, name: Name } -> (Attr, parent_post: Attr)
SetAttr { share, node: NodeKey, set: SetMask, mode, uid, gid,
          size /*truncate*/, mtime, atime }                -> Attr
WriteStart { share, node: NodeKey, off: u64, len_hint: u64, wid: u64 }
   // data follows as BULK frames tagged wid; mistd pwritev()s as it lands
WriteEnd   { wid }                                         -> Attr        // not yet durable
Commit  { share, node: NodeKey }                           -> Attr        // fdatasync (per share `commit` policy)
Identity { uid: u32, gid: u32, groups: Vec<u32> /*≤32*/ }  // mapped identity; applier setfsuid
```

Concurrency: ≤256 in-flight RPCs per session [E, tunable]; per-node mutations are serialized by
hostd (issue order = replica-apply order) so guest apply order matches replica order.

## 6. Journal (lane `journal`, EVENT frames g→h)

### 6.1 Batch envelope

```rust
struct JournalBatch {
    share: ShareId,
    first_seq: u64,           // per-share, starts at 1 per epoch, gap ⇒ host forces resync
    guest_mono_ns: u64,       // batch emission time (lag metric)
    records: Vec<Rec>,        // ≤512 records or ≤64 KiB, ≤2 ms linger, whichever first
}
```

### 6.2 Records

```rust
enum Rec {
    Created   { parent: NodeKey, name: Name, node: NodeKey, attr: Option<Attr> },
    // attr None ⇒ stat raced with deletion; host creates a *pending* node.
    CreatedBatch { parent: NodeKey, entries: Vec<(Name, NodeKey, Attr)> }, // untar storms
    Removed   { parent: NodeKey, name: Name },
    Renamed   { from_parent: NodeKey, from_name: Name,
                to_parent: NodeKey, to_name: Name },        // FAN_RENAME: one atomic record
    AttrChanged   { node: NodeKey, attr: Attr },             // chmod/chown/utimes/link-count
    Content   { node: NodeKey, version: u64, size: u64, mtime: Ts, in_progress: bool },
    // in_progress=true: MODIFY coalescer tick (≤1 Hz/file) — growing log visibility.
    // in_progress=false: CLOSE_WRITE — the close-to-open commit signal. Triggers recall.
    SelfRemoved { node: NodeKey },                           // DELETE_SELF (last link gone)
    Overflow {},                                             // FAN_Q_OVERFLOW ⇒ resync protocol
    EchoMarker { tag: u64 },                                 // see 6.5
}
```

### 6.3 Apply semantics (normative; full pseudocode `04` §4.2)

All applies are **upserts** keyed by `NodeKey`; this gives idempotence and snapshot-interleave
safety:

- `Created` onto an existing `(parent,name)`: replace the dentry (the old target node loses this
  link; if its nlink hits 0 and it isn't open, tombstone it). Onto an existing node with a new
  parent: it's a hard link discovery — add dentry.
- `Removed` of a missing dentry: no-op. Removing the last dentry tombstones the node (delegations
  recalled, side-store GC'd, CAS manifests dropped lazily).
- `Renamed` where source dentry is missing: degrade to `Created`-like upsert at destination using
  a `Stat` RPC to fetch attrs (rare race); log `journal_anomaly` metric.
- `AttrChanged`/`Content` on unknown node: stash in *pending* map; resolved by later `Created`,
  snapshot record, or on-demand `Lookup`; expired after 60 s (anomaly counter).
- Any apply that contradicts replica invariants (e.g. dir-entry for a non-dir parent) ⇒ targeted
  `RescanDir(parent)` — self-healing instead of trust-the-stream.
- Per-dir effects: every dentry mutation bumps `dirgen` (NFS cookieverf + readdirplus revalidation
  + diff-resync anchor) and assigns fresh monotonic cookies to new entries.

### 6.4 Consistent cut (seed while live)

mistd starts journaling a share **before** the snapshot walker starts; hostd buffers journal
batches during SEEDING and applies them after the snapshot stream completes (upserts make replay
of already-reflected changes harmless). Directories arrive with `complete=false` until their
snapshot record lands; READDIR on an incomplete dir blocks (bounded) rather than serving a
partial listing. Proof sketch in `06` §5.

### 6.5 Echo suppression (Mac-originated writes)

The applier in mistd tags its own syscalls by **pid**: the fanotify reader drops events whose
`pid == mistd_pid` *except* it first emits `EchoMarker { tag }` for ordering fences — hostd uses
markers to know when a mutation's own events have drained, so a subsequent guest-side event for
the same file is a *real* concurrent change (conflict detection, `06` §6). The replica was
already updated optimistically; suppressed echoes prevent double-apply and self-recall.

## 7. Snapshot stream (bulk lane, EVENT frames g→h)

```rust
struct SnapDir {
    snap_id: u64,
    dir: NodeKey,
    dir_attr: Attr,
    parent: NodeKey,            // root: parent == dir
    entries: Vec<(Name, NodeKey, Attr)>,   // ≤2048/record; large dirs span records
    last: bool,                 // final record for this dir
}
struct SnapDone { snap_id: u64, dirs: u64, entries: u64, errors: u32 }
```

Walk order is parallel BFS (parents *usually* precede children); the assembler tolerates orphans
via the pending map. `errors` counts unreadable entries (skipped + logged guest-side).
Resume: there is none — snapshots are cheap by design; an interrupted snapshot restarts (records
already applied are harmless upserts).

## 8. Flow control & resource bounds

- Journal lane: hostd applies fast (RAM ops); if the applier falls behind (queue > 64 batches)
  the socket stops being read ⇒ vsock/TCP backpressure ⇒ mistd's bounded emit queue (8 MiB)
  fills ⇒ mistd escalates to `Overflow` + drops its queue rather than blocking the fanotify
  reader (losing events safely — overflow means resync — beats stalling the guest kernel queue).
- Bulk lanes: one in-flight snapshot per share + read transfers scheduled by hostd's readahead
  engine; per-session in-flight bulk ≤ 64 MiB [E].
- All Vec/Bytes fields carry decode-time caps (listed inline above); violations ⇒ `ProtocolError`,
  session teardown, redial-with-resync.

## 9. Versioning & features

- `proto` major bump = incompatible (refuse). Within MWP/1, all evolution is additive via
  `features` bits, negotiated as the intersection:
  `F_RENAME_EV(0)` (FAN_RENAME available; else mistd synthesizes Renamed from MOVED_FROM/TO
  pairing), `F_LZ4(1)`, `F_CREATED_BATCH(2)`, `F_INO64(3)`, `F_XATTR(4)` (reserved),
  `F_JOURNAL_REPLAY(5)` (v2: ring-buffer replay from `last_seq` on reconnect, skipping reseed),
  `F_STATX_BTIME(6)` (reserved).
- Unknown enum variants: postcard + `#[non_exhaustive]` handling — unknown `Rec` variants are a
  decode error by default; minor-version tolerance is achieved by gating new variants behind
  feature bits so an old peer never receives them.

## 10. Worked sequences

**Guest edit → Mac sees it:**
```
guest: vim writes /srv/code/a.c, close(2)
mistd: FAN_CLOSE_WRITE → statx → Rec::Content{node, v=42, size, mtime, in_progress:false}
hostd: apply (attr.size/mtime/content_version updated; dirgen untouched)
       recall read delegation on node if granted
mac:   next open() → NFS GETATTR → replica answers v42 mtime → client invalidates pages → READ
       → CAS miss(v42) → guest Read RPC → bulk stream → CAS fill → data
elapsed guest-close→mac-open-fresh: journal lag (≈ms) [E]
```

**Mac save → guest build sees it:**
```
mac:   editor write+close → NFS WRITE×n (UNSTABLE) → COMMIT
hostd: replica optimistic apply (size/mtime, dirty flag) → WriteStart/BULK/WriteEnd → Commit
mistd: pwritev (setfsuid identity) → fdatasync → ack; echo events suppressed (pid), EchoMarker fences
hostd: COMMIT replies with post-attrs; dirty flag cleared
guest: immediately visible (it's the real fs)
```

**Overflow:**
```
mistd: FAN_Q_OVERFLOW → Rec::Overflow → keeps journaling (post-gap events still flow)
hostd: share → RESYNCING; SnapshotStart(diff mode = ordinary snapshot);
       assembler diffs incoming records against replica by (NodeKey, mtime, ctime, size, dirgen):
       unchanged dirs skipped cheaply, real diffs patch replica + recall + conflict-check
       serve-stale policy during resync per 06 §7; on SnapDone → LIVE
```
