# 01 — High-Level Architecture

## 1. Components and ownership

| Component | Runs | Language | Privilege | State |
|---|---|---|---|---|
| `mistd` | guest, systemd | Rust | root (capability-trimmed) | **stateless** (all tree state lives host-side; config + token only) |
| `mist-hostd` | host, launchd LaunchAgent (per user) | Rust | user | replica (RAM), CAS + side-store (disk caches), mounts |
| `mist` CLI | host, on demand | Rust | user | none (talks to hostd over control UDS) |
| `MistBridge` | inside the VM supervisor process | Swift package | whatever the supervisor has | none (byte forwarder) |
| `Mist.app` | host, menu-bar app | SwiftUI shell over hostd control API | user + optional helper | no protocol state |
| `MistMountHelper` | host, SMAppService daemon | Swift/XPC | root when registered | no protocol state |
| FSKit appex | host, appex | Swift shim → Rust core | sandboxed appex | future optional surface |

Design rule: **everything that can be Rust is Rust; Swift exists only where an Apple API forces a
process identity or bundle integration** (AVF/vsock bridging, SMAppService, menu-bar UX, future
FSKit appex). Swift shims contain no Mist protocol logic.

## 2. Process topology & why

```
┌─ VM supervisor (user's app / mist-vmshim) ────────────┐
│  VZVirtualMachine ── VZVirtioSocketDevice             │
│  MistBridge.attach(device, socketDir:)                │
│     listens: ~/Library/Application Support/Mist/vms/<vm>.sock
└───────────────────────────────────────────────────────┘
            ▲ UDS, Firecracker-style "CONNECT <port>\n"
            │
┌─ mist-hostd (launchd) ────────────────────────────────┐
│ auto resolver / bridge dialer ⇒ N lanes ⇒ session(vm) │
│ per-share: replica · journal applier · resync · scrub │
│ CAS · side-store · readahead engine                   │
│ loopback NFS server per mounted share                 │
│ control UDS: .../Mist/control.sock  ◄── mist CLI      │
└───────────────────────────────────────────────────────┘
            ▲ TCP 127.0.0.1 (NFS)
   macOS in-kernel NFS client ◄── Finder/editors/shell
```

Rationale:

- **AVF constraint**: only the `VZVirtualMachine`-owning process can open vsock connections.
  MistBridge therefore lives *in* the supervisor and exposes each VM's vsock as a UDS using the
  Firecracker vsock-UDS convention (`CONNECT 6478\n` → `OK 6478\n` → raw bytes). This convention
  interoperates with existing tooling and keeps the supervisor's surface area trivial. The current
  onboarding path also supports TCP over vmnet and uses `bridge = "auto"` to resolve a guest by
  stable VM identity instead of a stored DHCP address.
- **hostd is a separate daemon**, not supervisor-embedded: it must outlive supervisor restarts
  (mounts stay up; NFS client reconnects), be restartable independently (launchd `KeepAlive`),
  and serve multiple VMs from multiple supervisors.
- **mistd is stateless** so a guest daemon crash/restart loses nothing: hostd detects the new
  epoch in `Hello` and reseeds (diff-resync keeps it cheap). No on-disk index to corrupt in the
  guest; the filesystem itself is the database.

## 3. Transport: lanes over a dialable byte-stream

Mist needs exactly one primitive from a transport: *open an independent reliable ordered byte
stream to the peer*. Concrete connectors:

| Connector | Path | Use |
|---|---|---|
| `auto` | hostd resolver → mDNS/cache/vmnet probe → `tcp:<guest-ip>:6478` | default for VMs added via `mist add` |
| `bridge` | hostd → vm UDS → MistBridge → `VZVirtioSocketDevice.connect(6478)` → mistd listener | AVF supervisor integration |
| `tcp` | hostd → `<guest-ip>:6478` over virtio-net | QEMU/UTM, remote Linux, kernels < 6.12, benchmarking |
| `uds` | hostd → local socket → fake guest | tests, CI on macOS |

mistd listens (vsock port **6478**, and/or TCP if configured); the host always dials. `auto` is a
hostd config value, not a transport endpoint: it resolves to a concrete TCP endpoint after
checking `VmIdentity`. A *session* = 5+ streams, each beginning with a `StreamHello` naming its
lane:

| Lane | Direction | Traffic | Ordering need |
|---|---|---|---|
| `ctl` | bidi | Hello/auth, share attach, snapshot control, pings, epoch signals | strict |
| `journal` | guest→host | change-record batches | strict (defines consistency) |
| `rpc` | host→guest req / guest→host resp | metadata verify, mutations, small reads | pipelined, out-of-order completion by `seq` |
| `bulk` ×2 | mixed | snapshot streams, large `Read` payloads, write streams | per-transfer ordering |

Separate lanes exist so a 4 MiB read can never head-of-line-block a journal batch or a recall
trigger. Flow control: TCP/vsock backpressure + bounded application queues (`02-protocol.md` §8).

## 4. Host data planes

1. **Replica** (per share): inode table + directory maps + per-dir generation counters. Single
   writer (journal applier task), many lock-free-ish readers (NFS handlers). Authoritative for
   all metadata answers. Detailed structures: `04-host-mistd.md` §3.
2. **CAS** (shared across shares): chunked file content keyed by BLAKE3, manifests keyed by
   `(share, NodeKey, content_version)`. Write-through on cold reads; LRU eviction; survives
   restarts; excluded from Time Machine/Spotlight. (`04-host-mistd.md` §6.)
3. **Side-store** (per share): Apple-only metadata written by the Mac (`.DS_Store`, `._*`
  AppleDouble, xattrs/labels, `.fseventsd` probes) absorbed host-side, served back to the Mac,
  never forwarded to the guest. GC'd by journal `Removed` records. (`04-host-mistd.md` §7.)

## 5. Mount surfaces — the `MountSurface` seam

The replica/CAS core is front-agnostic behind one trait (sketch; final in `04-host-mistd.md` §8):

```rust
/// Implemented by the core; consumed by NFSv3, NFSv4.1, FSKit fronts.
pub trait MountSurface: Send + Sync {
    fn root(&self, share: ShareId) -> Result<NodeKey>;
    fn getattr(&self, n: NodeRef) -> Result<Attr>;
    fn lookup(&self, dir: NodeRef, name: &Name) -> Result<(NodeKey, Attr)>;
    fn readdir(&self, dir: NodeRef, cookie: u64, want_attrs: bool, max: usize)
        -> Result<ReadDirPage>;
    fn readlink(&self, n: NodeRef) -> Result<Bytes>;
    fn read(&self, n: NodeRef, off: u64, len: u32, ctx: &OpenCtx) -> ReadFuture;
    fn mutate(&self, m: Mutation, ident: &Identity) -> MutFuture; // create/write/rename/…
    fn access(&self, n: NodeRef, ident: &Identity, mask: u32) -> Result<u32>;
    fn statfs(&self, share: ShareId) -> Result<FsStat>;
    /// Coherence hooks: the core calls these on journal events so fronts
    /// holding client-visible state (delegations) can recall precisely.
    fn subscribe_invalidations(&self, f: Box<dyn Fn(InvalEvent) + Send + Sync>);
}
```

Available fronts: **NFSv3** for the conservative default and **NFSv4.1 + delegations** for the
coherence upgrade. **FSKit** stays behind the same core as an optional surface once Apple's
kernel-side caching is useful for this workload.

## 6. Operation routing (authoritative table)

| VFS op (Mac) | NFS proc | Handler path | Guest RPC | Notes |
|---|---|---|---|---|
| `stat`/`lstat` | GETATTR | replica | no | journal-fresh |
| path walk | LOOKUP | replica dir map (+NFC-normalized index) | no | negative entries answered from replica too |
| `access` | ACCESS | replica perms × mapped identity | no | mirrors guest enforcement |
| `opendir`+`readdir` | READDIRPLUS | replica cookie-ordered page | no | 1 RPC/dir on the loopback only |
| `readlink` | READLINK | replica (target stored at create) | no | |
| `open(O_RDONLY)` | (v3: implicit; v4: OPEN) | replica attrs; v4 may grant read delegation | no | close-to-open GETATTR served fresh |
| `read` cold | READ | CAS miss → guest `Read` RPC (+readahead) | yes | bulk lane |
| `read` warm | READ | client page cache; else CAS | no | NFSv4.1 delegation removes revalidation |
| `write`/`create`/`mkdir`/`rename`/`unlink`/`chmod`/`utimes`/`truncate` | WRITE/CREATE/… | optimistic replica apply + ordered guest mutation | yes | durable at COMMIT (fdatasync) |
| `fsync`/close-flush | COMMIT | wait guest fdatasync ack | yes | G2 guarantee |
| Finder metadata (`.DS_Store`, `._*`, xattr) | CREATE/WRITE/… | **side-store**, never guest | no | interception matrix in `05` §6 |
| guest change | — | journal → replica (+ recall) | push | ≤5 ms p99 apply [E] |

## 7. Lifecycles (state machines)

**Session (per VM):**
`IDLE → DIALING → HELLO → READY` ; ping misses (3×1 s) or stream error → `DEGRADED` →
re-dial with backoff (100 ms→5 s cap). On reconnect: compare `boot_id`/share epochs —
match ⇒ resume (v1: reseed shares cheaply via diff-resync; v2: journal-replay from `last_seq`),
mismatch ⇒ full reseed. NFS keeps serving the (possibly stale) replica throughout `DEGRADED`,
flagged in `mist status`; mutations fail fast with `JUKEBOX`/delay then `EIO` after a deadline.

**Share:**
`CONFIGURED → SEEDING (snapshot streaming; serve partial tree with `complete=false` dirs
blocking readdir until filled) → LIVE → RESYNCING (overflow/epoch; serve stale per
`06-consistency.md` §7) → LIVE`, or `OFFLINE` (guest unmounted the share; serve nothing).

**Mount:** `mist mount <vm> <share>` ⇒ hostd ensures share ≥ SEEDING, starts a loopback NFS server
on an ephemeral port, and spawns `mount_nfs` at
`$MIST_STATE_DIR/mnt/<vm>/<share>` (default
`~/Library/Application Support/Mist/mnt/...`). It records mount state in `mounts.json` so a hostd
restart can rebind the same port and adopt an existing hard mount. Finder-visible
`~/Mist/<vm>/<share>` mounts are a Mac-app/helper goal, not the current hostd default.

## 8. Failure domains & blast radii

| Failure | Blast radius | Recovery |
|---|---|---|
| mistd crash | journal gap | systemd restarts; hostd sees new epoch → diff-resync; mounts stay up |
| hostd crash | NFS stalls (hard mount semantics) | launchd restarts ≤1 s; replica reseeds; NFS client retries — in-flight Mac writes not yet COMMITted are re-sent by the client (write verifier mismatch forces it) |
| supervisor/bridge drop | session DEGRADED | redial loop; replica serves stale read-only-ish (mutations queue/fail per deadline) |
| guest reboot | epoch change | full reseed; delegations recalled-all; mounts survive |
| Mac sleep | TCP idle | NFS + session keepalives resume; vsock connections survive VM pause in e2e runs |
| journal overflow | replica suspect | `Overflow` record → RESYNCING diff walk; recalls only for real diffs |

## 9. Crate map (workspace)

```
Mist/
├── Cargo.toml                 # workspace, MSRV pinned, cargo-deny config
├── crates/
│   ├── mist-proto/            # wire types, postcard codec, framing, feature bits  (no I/O)
│   ├── mist-transport/        # lane mux, connectors: uds/tcp/vsock-uds-bridge dialer
│   ├── mist-replica/          # tree structures, journal apply, snapshot assemble, scrub
│   ├── mist-cas/              # chunking (FastCDC), BLAKE3, redb index, eviction
│   ├── mist-nfs/              # ONC-RPC + XDR + NFSv3 + NFSv4.1 servers
│   ├── mist-hostd/            # daemon: sessions, surfaces, side-store, control API, metrics
│   ├── mistd/                 # guest daemon (linux-only): fanotify, walker, applier, services
│   ├── mist-cli/              # `mist` command
│   └── mist-bench/            # scenario harness + fixtures (08-performance §7)
├── swift/MistBridge/          # SwiftPM package: UDS↔vsock forwarder (+ mist-vmshim example)
├── swift/MistApp/             # SwiftUI menu-bar shell + SMAppService helper scaffold
├── packaging/                 # guest deb/installer; Mac app, appcast, and cask scaffolding
├── design/                    # this pack
└── tests/                     # cross-crate integration + e2e scripts
```

Dependency rules: `mist-proto` depends on nothing internal; `mistd`/`mist-hostd` are the only
binaries; `mist-nfs` sees the core only through `MountSurface`; nothing links Swift. Keep the
dependency tree shallow and audited (tokio, rustix, postcard/serde, parking_lot, blake3, redb,
tracing, io-uring (guest, optional feature), proptest/criterion dev-side). `cargo-deny` in CI.

## 10. Configuration & paths (canonical)

Host (`~/Library/Application Support/Mist/config.toml`):

```toml
[daemon]
control_socket = "control.sock"        # relative to app-support dir
cache_max_bytes = 21474836480          # bytes; 0 disables the cache
log = "info"                           # optional tracing filter

[vm.dev]                               # one section per VM
bridge  = "auto"                       # resolver keyed by vm_uuid
vm_uuid = "4f2b0b30a9d84f8ab5c40122c7be1c13"
token   = "@vms/dev.token"             # shared secret path, 0600; @ resolves under state dir
autoattach = true
```

Manual/test bridge values are `tcp:<host-or-ip>:<port>`, `bridge:/path/to/socket[#6478]`, and
`uds:/path/to/fake-guest.sock`. Per-share mount profiles and custom mountpoints are not accepted
by the current host config; share definitions live on the guest and host mounts are derived from
announced shares.

Guest (`/etc/mist/mistd.toml`):

```toml
listen = ["vsock:6478"]                # and/or "tcp:0.0.0.0:6478"
token_file = "/etc/mist/token"
vmid_file = "/etc/mist/vmid"
avahi_service_file = "/etc/avahi/services/mist.service"

[share.code]
path = "/srv/code"                     # whole-mount share recommended (zero filter cost)
# subtree = true                       # allowed; pays ~1–2 µs path-filter per event
commit = "fsync"                       # fsync | writeback   (G2 durability vs speed)
```

CLI verbs (full schema in `04-host-mistd.md` §9): `mist status | shares | attach | detach |
mount | umount | sync [path] | events [--json] | conflicts | cache stats|clear | doctor | version`.

## 11. Compatibility floors

| Thing | Floor | Recommended | Why |
|---|---|---|---|
| Guest kernel | 6.1 (Debian 12) | ≥ 6.12 (Debian 13) | FAN_RENAME ≥5.17; vsock TX fix in 6.12 [R] |
| Guest FS | ext4 | dedicated ext4 volume per share-set | stable 32-bit ino + i_generation, exportable handles; xfs/btrfs fit the u64 inode model when enabled |
| macOS | 15.x (nfs3 surface) | 26.x | v4.1 + delegations verified on 26.5; FSKit front needs 26+ |
| Transport | TCP/virtio-net | vsock + MistBridge | works everywhere; vsock removes the IP stack |
