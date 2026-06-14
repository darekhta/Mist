# 03 — `mistd` (Guest Daemon) Low-Level Design

Stateless root daemon in the Debian guest. Three jobs: (1) turn kernel filesystem events into the
journal, (2) enumerate trees fast for seeding/resync, (3) execute reads and contained, identity-
mapped mutations on behalf of the host. It holds **no persistent state** beyond config + token;
the filesystem is the database, the host replica is the index.

## 1. Process model

```
main (tokio, 2 worker threads)
├── listener task            vsock:6478 / tcp — accepts sessions, HELLO/auth
├── per-session tasks        ctl handler · rpc executor pool · bulk writers
├── journal pipeline (per marked filesystem)
│   ├── [OS thread] fanotify reader      blocking read(2) loop, 256 KiB event buffer
│   ├── [OS thread ×2] resolvers         handle decode → statx → Rec synthesis
│   └── encoder task                     batch (≤512/64KiB/2ms) → journal lane
├── snapshot walkers (per SnapshotStart) bounded-parallel BFS, io_uring statx (feature)
└── applier (per share)                  serialized mutation executor, setfsuid pool
```

Threads that touch blocking syscalls (fanotify read, open_by_handle_at, io_uring submit) are
dedicated OS threads; tokio handles transport + orchestration only. Bounded channels everywhere;
the overflow ladder (§4.6) is the designed response to sustained overload — mistd never blocks
the fanotify queue reader for longer than the channel-full grace (1 s).

## 2. Startup sequence

1. Parse `/etc/mist/mistd.toml`; resolve shares; for each share `open(path, O_PATH|O_DIRECTORY)`
   → `root_fd`, `statfs`/`statx` → pin `fsid` (every later by-handle open verifies fsid identity).
2. Raise `fs.fanotify.max_queued_events` to configured value (default 262144) via sysctl;
   record actuals for `HelloAck.guest`.
3. `fanotify_init(FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME, O_RDONLY | O_LARGEFILE)` — one group
   per distinct filesystem hosting shares.
4. `fanotify_mark(FAN_MARK_ADD | FAN_MARK_FILESYSTEM, MASK, AT_FDCWD, share_mountpoint)` with
   `MASK = FAN_CREATE | FAN_DELETE | FAN_DELETE_SELF | FAN_RENAME | FAN_CLOSE_WRITE |
   FAN_MODIFY | FAN_ATTRIB | FAN_ONDIR`.
   (Directory-entry events require FILESYSTEM marks — they are EINVAL on mount marks.)
5. Open `/proc/self/mountinfo`, register epoll on it (POLLPRI) → mount-table watcher: share's
   mount disappears ⇒ `ShareGone`, reappears ⇒ epoch bump.
6. Listen; serve sessions. Journal pipelines start marked but **paused**; `AttachShare` unpauses
   (refcounted across sessions).

## 3. Identity & epoch

- `boot_id`: 8 random bytes at process start.
- Share `epoch = blake3(boot_id ‖ fsid ‖ mount_instance_counter)` truncated to u64. Any mistd
  restart or share remount changes epochs ⇒ host reseeds. (F_JOURNAL_REPLAY in v2 relaxes this
  with a journal ring buffer; designed but out of v1.)

## 4. Fanotify engine

### 4.1 Event decode

Events arrive as `fanotify_event_metadata` + info records. With `FAN_REPORT_DFID_NAME` we get
`FAN_EVENT_INFO_TYPE_DFID_NAME`: the **parent directory's file handle** + the **entry name**.
ext4 handles are `FILEID_INO32_GEN` (ino u32 + gen u32) → `NodeKey` decodes **statelessly from
the handle bytes**, no syscall. This is the property that keeps mistd stateless. (Non-ext4
handle types: feature-gated ino64 path.)

`FAN_RENAME` (kernel ≥5.17) delivers OLD_DFID_NAME + NEW_DFID_NAME in one event → one atomic
`Rec::Renamed`. On 6.1-floor kernels this is always available; the MOVED_FROM/TO pairing fallback
(F_RENAME_EV unset) is specified but not built in v1 (Debian 12+ only).

### 4.2 Event → record synthesis

| fanotify event | Action in resolver | Record |
|---|---|---|
| FAN_CREATE (±ONDIR) | `open_by_handle_at(parent, O_PATH)` → `statx(parent_fd, name, AT_SYMLINK_NOFOLLOW)`; read link target if symlink | `Created { parent, name, node, attr }`; stat ENOENT (raced delete) ⇒ `attr: None` |
| FAN_DELETE | none (child is gone; host resolves by `(parent,name)`) | `Removed { parent, name }` |
| FAN_RENAME | decode both DFIDs | `Renamed { from, to }` |
| FAN_ATTRIB | statx by child handle (DFID events carry parent; ATTRIB carries the object's own FID — request `FAN_REPORT_FID` too) | `AttrChanged { node, attr }` |
| FAN_CLOSE_WRITE | statx by FID; bump version counter | `Content { in_progress: false }` |
| FAN_MODIFY | coalescer (§4.3) | `Content { in_progress: true }` (≤1 Hz/file) |
| FAN_DELETE_SELF | — | `SelfRemoved { node }` |
| FAN_Q_OVERFLOW | escalate (§4.6) | `Overflow {}` |

Group init therefore uses `FAN_REPORT_DFID_NAME | FAN_REPORT_FID` (object FID available for
non-dentry events). The `content_version` counter is per-NodeKey, kept in a bounded LRU map
(1M entries [E]); eviction restarts a file's counter at `mtime_ns ^ size` — uniqueness across
restarts/evictions is what CAS keying needs, monotonicity is not required.

### 4.3 MODIFY coalescer

Token bucket per NodeKey: first MODIFY emits immediately (`in_progress: true`), then at most one
per second per file while writes continue; entry expires 5 s after last event. Purpose: `tail -f`
and progress-watching from the Mac with bounded journal cost. CLOSE_WRITE always emits and resets
the bucket. Map bounded (64k entries), LRU-evicted (eviction merely means an extra emit later).

### 4.4 Subtree filtering

FILESYSTEM marks see whole-fs events. For `subtree = true` shares the resolver must test
containment: decode parent NodeKey → if not in share, resolve the parent's path
(`open_by_handle_at` + `readlink(/proc/self/fd/N)`) and prefix-test against the share root
(~1–2 µs [E]). Containment results are cached per parent NodeKey (LRU 64k; invalidated on
Renamed/Removed of dirs). Whole-mount shares skip all of this — the documented recommendation.

### 4.5 Echo suppression

Events whose `metadata.pid == getpid()` are the applier's own work: dropped after emitting
`EchoMarker{tag}` when the pending-mutation table says a fence was requested (`02` §6.5). Note
pid-reuse is irrelevant (it's *our* live pid), and `FAN_REPORT_TID` is unnecessary.

### 4.6 Overload ladder

1. Reader drains aggressively (256 KiB reads ≈ ~1000 events/syscall) into a 64k-record channel.
2. Channel full > 1 s ⇒ emit `Overflow`, drop channel contents, continue reading (post-gap events
   still produce records — the host resync diff covers the gap).
3. Kernel queue overflow (`FAN_Q_OVERFLOW` event) ⇒ same path. Both increment `mistd_overflows`.
4. Sustained storm (> 3 overflows / 60 s) ⇒ journal enters *quiescent* mode: stop per-event work,
   emit `Overflow` once, wait for storm to subside (event rate < 1k/s for 5 s), resume. Host
   stays in RESYNCING the whole time and runs one diff at the end. This makes `untar -xf
   linux.tar` in the guest cost one resync, not a million records.

## 5. Snapshot walker

```
walk(share, snap_id):
  sem = Semaphore(64)                       # dirs in flight
  queue = [root]
  per dir (task):
    fd = openat2(root_fd, relpath, RESOLVE_BENEATH) or open_by_handle_at
    getdents64 loop (256 KiB buffer)
    statx each entry — io_uring batch (ring 256, IORING_OP_STATX) if feature enabled,
                       else plain statx loop (still ~1–2 µs warm [E])
    emit SnapDir records (≤2048 entries each, last flag on final)
    push child dirs
  emit SnapDone{dirs, entries, errors}
```

- Unreadable entries: count in `errors`, log path hash (not name — log hygiene), skip.
- Throttle: walker yields to RPC reads (priority: journal > rpc > snapshot on the wire; bulk lane
  scheduling in hostd handles the read side).
- io_uring is a build feature + runtime probe (`io_uring_setup` EPERM ⇒ fallback): hardened guests
  disable it via sysctl, and mistd must run there too.
- Budget [E]: ≥150k entries/s wire+apply on the reference Apple Silicon Pro machine; 1M-file
  share seeds ≤10 s cold.

## 6. Read service

`Read { node, version_hint, off, len, ra }`:
1. `open_by_handle_at(mount_fd, handle(node), O_RDONLY|O_NOFOLLOW|O_NONBLOCK)`; verify `fsid`.
   Open fds cached per NodeKey (LRU 1024, 30 s TTL) — repeated chunk reads skip reopen.
2. `statx` → reconcile: if `(mtime,size)` differ from what `content_version` map says, bump
   version and emit `Rec::Content` (the **verify-on-access backstop** that catches mmap writers).
3. `preadv2` (or io_uring read) into pooled 4 MiB buffers → BULK frames. `ra` is a hint to also
   warm the page cache (`posix_fadvise(WILLNEED)` on the next `ra` bytes) — guest page cache is
   the second-level cache for the whole system.
4. EOF semantics: short read + `eof: true` (size may have changed since attr — host trusts the
   stream, not the stale attr).

## 7. Mutation applier

One logical applier per share; operations for the same NodeKey are serialized (hash-sharded
queues, 16 shards). Mac-side request order = hostd issue order = apply order (per node).

### 7.1 Containment (every op)

- All object access is `(parent NodeKey, Name)`: `open_by_handle_at(parent)` → verify `fsid`
  matches the share; for subtree shares additionally containment-check the parent (cache from
  §4.4). Then the single-component operation uses
  `openat2(parent_fd, name, RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | O_NOFOLLOW)` or the matching
  `*at` syscall (`unlinkat`, `renameat2`, `linkat`, `mknodat`, `symlinkat`, `fchownat`,
  `utimensat` with `AT_SYMLINK_NOFOLLOW`).
- A `Name` is one component by construction (decode-validated), so path traversal is impossible
  by grammar; `RESOLVE_NO_SYMLINKS` stops symlink-component tricks; fsid pinning stops
  cross-mount escapes via bind games.

### 7.2 Identity

`Identity { uid, gid, groups }` comes mapped from hostd per share policy (`squash` default:
everything acts as the share's configured guest user, e.g. 1000). Applier threads wrap each op in
`setfsuid/setfsgid/setgroups` (per-thread on Linux; applier pool threads are dedicated). Result:
guest-kernel permission enforcement applies exactly as if the mapped user did it — no manual
permission emulation, EACCES surfaces naturally and maps back to NFS.

### 7.3 Write path

`WriteStart/BULK/WriteEnd/Commit` per `02` §5. fd held open across the wid; `pwritev` as frames
land (no buffering); `WriteEnd` → statx → reply attr (not durable); `Commit` → `fdatasync`
(share policy `commit = fsync`) or no-op (`writeback`) → statx → reply. O_TMPFILE atomic-create
optimization for whole-file rewrites remains optional because rename-into-place gives Mac saves
crash-atomicity guest-side).

### 7.4 Errors

Guest errno passes through verbatim (`RpcErr.errno`); applier additionally classifies
`EDQUOT/ENOSPC` to set a share health flag (surfaces in `mist status`).

## 8. Config reference (`/etc/mist/mistd.toml`)

```toml
listen     = ["vsock:6478"]        # also "tcp:0.0.0.0:6478"; both allowed
token_file = "/etc/mist/token"     # 0600 root; 32+ random bytes
vmid_file  = "/etc/mist/vmid"      # stable guest identity, 32 lowercase hex chars
avahi_service_file = "/etc/avahi/services/mist.service" # empty disables _mist._tcp advert
log        = "info"                # tracing filter; journald output

[limits]
inflight_rpc            = 256
walker_parallelism      = 16
snap_entries_per_record = 2048

[share.code]
path    = "/srv/code"
subtree = false                    # path is a mountpoint (recommended)
commit  = "fsync"                  # fsync | writeback
apply_uid = 1000                   # identity squash target
apply_gid = 1000
readonly  = false
```

## 9. Privileges, packaging, ops

- Capabilities (systemd `AmbientCapabilities`, full root not required):
  `CAP_SYS_ADMIN` (fanotify FILESYSTEM marks, open_by_handle_at), `CAP_DAC_READ_SEARCH`
  (by-handle opens), `CAP_DAC_OVERRIDE`, `CAP_CHOWN`, `CAP_FOWNER`, `CAP_SETUID`, `CAP_SETGID`
  (setfsuid/setfsgid/setgroups), `CAP_MKNOD`. Plus `NoNewPrivileges=yes`, `ProtectSystem=strict`
  with `ReadWritePaths=` the share roots, `PrivateTmp`, `RestrictAddressFamilies=AF_VSOCK AF_INET
  AF_INET6 AF_UNIX`, `MemoryMax=256M`, `Restart=always`, `RestartSec=200ms`, `WatchdogSec=10`
  (sd_notify heartbeat from the main loop).
- Package: `mist-guest.deb` → `/usr/sbin/mistd`, `/lib/systemd/system/mistd.service`,
  `/etc/mist/mistd.toml` (conffile), postinst generates `/etc/mist/token` if absent. Built via
  cargo-deb in CI; also a plain `install.sh` and a cloud-init snippet in `packaging/`.
- Crash policy: panics abort the process (no unwinding across FFI); systemd restarts; statelessness
  makes this safe — host reseeds. Watchdog catches livelocks.
- Resource budget [E]: RSS ≤ 150 MiB at 1M-node share (version map + caches), idle CPU < 1 %,
  storm CPU ≤ 1 core (then the §4.6 ladder).
- Observability: `tracing` → journald; counters (events_in, records_out, overflows, anomalies,
  rpc latencies, applier errors) exposed via a `Metrics` CTL reply to hostd, which re-exports to
  Prometheus — one scrape point on the host for the whole system.
