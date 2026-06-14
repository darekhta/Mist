# Mist — Overview

**Mist** (міст — *bridge*) gives macOS near-native access to files that live on ext4 inside a
Debian guest running under stock Apple Virtualization.framework (AVF). It is the inverse of the
WSL2 problem, solved the way WSL2 never did: instead of paying a VM round trip per filesystem
operation, Mist **replicates the guest's metadata tree into the macOS host daemon** and keeps it
coherent with a real-time **fanotify change journal streamed over vsock**. The Mac mounts a
loopback NFS server that answers every `stat`/`readdir`/`lookup` from local RAM; only cold data
reads, writes, and change events ever cross the VM boundary.

Status: **implemented project reference**. This pack documents the architecture, operating model,
and validation evidence for the finished Mist system.

---

## 1. Problem statement

The source of truth is a source tree (or several) on a native ext4 volume inside the VM, so Linux
builds, git, and tooling run at native speed by construction. The host needs to browse, edit,
search, and run `git status` on those trees from Finder, VS Code/Zed, and the Mac shell — on
workloads dominated by **metadata storms**: hundreds of thousands of `stat`/`open`/`getdents`
calls over many small files.

### Why every existing approach fails this workload

Verified research (June 2026, adversarially fact-checked; sources in §7):

| Approach | Measured reality | Verdict |
|---|---|---|
| FSKit custom FS (macOS 26) | ~**121 µs/op** XPC floor even for a 2-entry readdir; Apple DTS: caching "probably not enough to be truly useful"; zero cache-control API in the 26.5 SDK | Per-op cost is fatal today; revisit when Apple lands caching |
| macFUSE kext | Deprecated kext path; closed source; "system lockups under high load" (Buildbarn) | Dead end |
| Userspace NFS server proxying per-op into the VM | What EdenFS ("lackluster, numerous scalability and reliability issues"), Buildbarn, and OrbStack all converged on; every op still pays server-side work + (for VM shares) guest RTT; no server→client cache invalidation in practice (CB_NOTIFY "rarely implemented by clients") | Best-available *mount surface*, wrong *backend* |
| WSL2 `\\wsl$` (9p per-op) | Canonical cautionary tale; minutes for large-tree scans | Anti-pattern |
| Two-way sync (Mutagen-style) | Native speed both sides but two copies, convergence lag, conflict surface | Rejected: we want one source of truth |

### The arithmetic that drives the architecture

A `git status` on a large repo issues ~200k metadata ops. At a 100–120 µs per-op RPC floor that is
**20–24 s**; native is ~0.2–0.5 s. No transport tuning closes a 100× structural gap. The only
design that can be near-native is one where **the metadata storm never crosses a process or VM
boundary at all** — the host must already hold the answers.

NFS `READDIRPLUS` is the one batching lever the in-kernel macOS client gives us: one RPC per
*directory* (with attributes for every entry), feeding the kernel's name/attribute caches. Against
a local RAM-backed server at ~40–60 µs per request, a 5k-directory tree enumerates in ~0.3 s and a
40k-directory monster in ~2–4 s — and warm scans are pure kernel-cache hits at native speed.

## 2. The Mist theses

1. **Replicate, don't proxy.** The host daemon holds a complete metadata replica of each exported
   share (an inode table + directory entries + attributes). All metadata reads are answered
   locally, forever. Cost: RAM (≈120–250 B/node v1) and a seed pass (~seconds per million files).
2. **The guest can tell us every change.** `fanotify(FAN_MARK_FILESYSTEM | FAN_REPORT_DFID_NAME)`
   watches an entire ext4 filesystem with one mark, race-free, with parent handle + entry name per
   event — on every Debian ≥12 kernel. A root guest daemon turns this into an ordered journal,
   streamed over vsock, applied to the replica within milliseconds. Close-to-open consistency
   falls out: a file closed in the guest is visible to the next Mac `open()`.
3. **Use the boring mount surface, make it fast behind the curtain.** Three production systems
   (EdenFS, Buildbarn, OrbStack) proved the macOS in-kernel NFS client is the least-bad mount
  surface — Finder, editors, git all just work. Their pain (per-op backend cost, no invalidation
  push) is exactly what the replica + journal removes. Mist ships NFSv3 and NFSv4.1 with
   **journal-driven delegation recall** — we grant read delegations freely because, uniquely among
   NFS servers, Mist *knows* when a file really changed. An FSKit front is a pluggable later
   surface behind the same core.

## 3. Goals

- **G1 — metadata at memory speed.** Post-seed, `LOOKUP/GETATTR/ACCESS/READDIR(PLUS)/READLINK`
  never RPC to the guest. Cold full-tree scan of linux.git-scale (~85k files): ≤ 2 s. Warm:
  ≤ 1.3× native. (Full budget table: `08-performance.md`.)
- **G2 — close-to-open coherence, both directions.** Guest `close()` → visible to Mac `open()`
  in ≤ 10 ms p99. Mac `close()` → durable in guest (fdatasync) before `COMMIT` acks.
- **G3 — feels native on the Mac, invisible on Linux.** Finder metadata (`.DS_Store`, `._*`,
  xattrs, labels) is absorbed into a host-side side-store and never pollutes the guest tree.
- **G4 — survives reality.** Journal overflow, daemon crashes, VM reboots, Mac sleep: every path
  ends in automatic resync with bounded staleness, never silent divergence (a background scrubber
  converts "missed event" into "detected + healed + counted").
- **G5 — boring to operate.** One `.deb` in the guest, one launchd agent + CLI on the host, one
  tiny Swift bridge API for whatever VM supervisor embeds it. TCP fallback transport means Mist
  also works with QEMU/UTM/remote Linux boxes, degraded only by transport latency.

## 4. Non-goals

- Not a sync tool: one source of truth (guest), no second copy of data on the Mac beyond caches.
- Not a multi-writer collaborative FS: concurrent cross-boundary writes to the *same file* get
  last-close-wins + a conflict log, not merge semantics.
- Not a security boundary between mutually distrusting human users on the Mac (single-user
  assumption; see `07-security.md` for the honest local-NFS exposure analysis).
- Not strict POSIX coherence (no cross-boundary byte-range lock enforcement in v1; mmap-based
  guest writes have a documented visibility blind spot with mitigations).
- Not WAN file sharing. LAN/TCP mode is supported but tuned targets assume vsock/virtio latencies.
- Mac→Linux bulk write workloads (e.g. running `npm install` *from the Mac side*) are supported
  correctly but are explicitly not a performance target — run package managers in the guest,
  where the files live.

## 5. Hard constraints (environment)

- **Stock AVF.** Device surface is fixed: virtio-blk/NVMe storage, virtiofs (unused by Mist),
  **vsock** (`VZVirtioSocketDevice`), virtio-net. No custom virtio devices, no DAX.
  Host-side vsock is only reachable from the process that owns `VZVirtualMachine` → the VM
  supervisor must embed the ~150-line `MistBridge` shim (Firecracker-style UDS↔vsock forwarder).
- **Guest:** Debian 12+ (kernel ≥ 6.1 floor: fanotify `FAN_RENAME` needs ≥ 5.17). Recommended
  Debian 13 / kernel ≥ 6.12 (vsock TX workqueue fix; on older kernels prefer the TCP transport).
  Shares must be on filesystems with stable inode numbers + exportable handles (ext4 v1 target).
- **Host:** macOS 15+ for the NFSv3 surface; macOS 26.5 verified locally: NFS client supports
  `vers=4.1`, callbacks/delegations enabled by default (`nocallback` exists to disable), full
  attribute-cache tuning (`actimeo`, `noac`, and per-type knobs). Apple silicon.
- **Language:** Rust workspace (daemons, protocol, NFS server, CLI); thin Swift shims only where
  Apple APIs force it (MistBridge, Mist.app/SMAppService helper, later the FSKit appex).

## 6. System at a glance

```
macOS host                                             Debian guest (AVF)
┌──────────────────────────────────────┐               ┌──────────────────────────────────────┐
│ Finder · VS Code · git · rg          │               │ builds & tools on native ext4        │
│        │ syscalls                    │               │  (zero Mist involvement)             │
│        ▼                             │               │                                      │
│ in-kernel NFS client                 │               │ mistd (root, stateless)              │
│  v3 · v4.1+delegations               │               │  ├ fanotify engine ── change journal │
│        │ TCP 127.0.0.1:<port>        │               │  ├ snapshot walker (seed/resync)     │
│        ▼                             │   AF_VSOCK    │  ├ read service (pread/io_uring)     │
│ mist-hostd                           │  port 6478    │  └ write applier (openat2-contained, │
│  ├ metadata replica (RAM, per share) │◄─────────────►│     setfsuid identity, echo-tagged)  │
│  ├ journal applier + resync/scrub    │ lanes: ctl ·  │                                      │
│  ├ CAS content cache (APFS, BLAKE3)  │ journal · rpc │ systemd unit, .deb package           │
│  ├ side-store (.DS_Store/xattrs)     │ · bulk ×2     │                                      │
│  └ NFS server ── deleg/recall mgr    │               │                                      │
│        ▲                             │               │                                      │
│ mist CLI · launchd agent             │               │                                      │
│ MistBridge.swift in VM supervisor    │               │                                      │
│  (UDS ↔ vsock forwarder)             │               │                                      │
└──────────────────────────────────────┘               └──────────────────────────────────────┘
```

Op routing (the "why it's fast" table — full version in `01-architecture.md` §6):

| Mac operation | Backend | Guest RPC? |
|---|---|---|
| lookup / getattr / access / readdir(+) / readlink | replica RAM | **never** (post-seed) |
| read (warm) | macOS client cache → CAS | never |
| read (cold) | guest `Read` RPC + readahead, fills CAS | once per range |
| write / create / rename / chmod … | optimistic replica apply + guest mutation RPC | yes (async; durable at COMMIT) |
| guest-side change | journal record → replica (+ delegation recall) | n/a (push) |

## 7. Research provenance

Key verified facts this design rests on (deep-research run, 2026-06-11; 105 agents, 3-vote
adversarial verification):

- FSKit ≈121 µs/op + DTS confirmation: developer.apple.com/forums/thread/793013; network-FS
  capability only since macOS 26: thread/776322, macFUSE 5.1.0 notes; `reclaimItem` ≈ FUSE forget:
  thread/766793.
- EdenFS NFSv3-on-loopback verdict ("lackluster"): thread/766793 + sapling `eden/fs/docs`.
- Buildbarn ADR 0009 (macFUSE lockups; CB_NOTIFY "rarely implemented"): github.com/buildbarn/bb-adrs.
- OrbStack `~/OrbStack` is NFS: orbstack.dev blog 1.2 + docs (architecture, native-files).
- fanotify FILESYSTEM marks + DFID_NAME semantics & caveats: man7 fanotify(7), fanotify_mark(2).
- vsock TX workqueue fix in Linux 6.12: LPC 2023 ByteDance slides + Phoronix 6.12 note.
- macOS 26.5 NFS client (v4.1, delegations on by default, ac* knobs): local `mount_nfs(8)` /
  `nfs.conf(5)` man pages, verified on this machine.
- WWDC26 "Container Machines" (session 389) ships Mac→Linux mirroring only — the Linux→Mac
  direction remains an open niche.

Numbers tagged **[R]** in this pack come from those sources; **[E]** marks engineering estimates
tracked by the benchmark and acceptance evidence summarized in `09-release-status.md`.

## 8. Document map

| Doc | Contents |
|---|---|
| `01-architecture.md` | HLD: components, processes, transports/lanes, lifecycles, crate map, deployment |
| `02-protocol.md` | Wire protocol: framing, lanes, every message + journal record, apply semantics, versioning |
| `03-guest-mistd.md` | Guest daemon LLD: fanotify engine, snapshot walker, applier, containment, packaging |
| `04-host-mistd.md` | Host daemon LLD: replica structures, journal apply, CAS, side-store, control plane |
| `05-nfs-frontend.md` | NFSv3/v4.1 servers, mount options, macOS client quirk catalog, delegations & recall |
| `06-consistency.md` | Formal guarantees, staleness tables, conflicts, crash matrices, resync protocol |
| `07-security.md` | Threat model, hostile-guest hardening, local NFS exposure, privileges |
| `08-performance.md` | Cost model, budgets & gates, tuning knobs, bench harness spec |
| `09-release-status.md` | Release status, acceptance evidence, risk register, release packaging |
| `10-testing.md` | Test strategy: model-based replica tests, fuzzing, conformance, chaos, perf gates |
| `11-onboarding.md` | Onboarding: `bridge="auto"` autodiscovery, copy-the-token-once `mist add`, guest identity (`vm_uuid`), installer, Mac menu-bar app/package scaffolding |
| `ADR.md` | Numbered architecture decision records |

## 9. Glossary

- **Share** — an exported guest directory tree (ideally a whole ext4 mount). Unit of replication,
  mounting, and configuration.
- **NodeKey** — `(ino: u64, gen: u32)` of a guest inode; the canonical, *derivable* identity of a
  file (stateless across mistd restarts; ABA-safe via ext4 `i_generation`).
- **Replica** — host-side in-RAM mirror of a share's metadata tree.
- **Journal** — ordered stream of change records emitted by mistd from fanotify events.
- **Seed / resync** — full or diff enumeration of a share streamed into the replica.
- **Epoch** — share-instance identifier; mismatch (guest reboot, remount) forces reseed.
- **CAS** — host content-addressed cache of file data chunks (BLAKE3).
- **Side-store** — host-local store absorbing Apple-only metadata (`.DS_Store`, `._*`, xattrs).
- **Lane** — one logical byte stream of the transport (ctl, journal, rpc, bulk×N).
- **content_version** — mistd-maintained per-file counter; the data-cache epoch.
