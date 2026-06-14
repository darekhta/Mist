# Mist

**Mist** (міст — *bridge*) — near-native macOS access to files living inside a Linux VM.

Your code lives on native ext4 inside a Debian guest under Apple's Virtualization framework, so
builds, git, and tooling in the VM run at full native speed. Mist makes the *Mac* side feel
native too: Finder, VS Code, `git`, `rg` work on the guest's files without paying a VM round trip
per file operation.

## How

Every prior system (Docker/EdenFS/OrbStack-style shares, WSL2's `\\wsl$`) proxies each filesystem
operation across a boundary — and metadata-heavy workloads (source trees) issue hundreds of
thousands of them. Mist doesn't proxy. It **replicates**:

- A guest daemon (`mistd`) watches the entire filesystem with one fanotify mark and streams an
  ordered **change journal** over vsock, plus fast snapshots for seeding.
- The host daemon (`mist-hostd`) holds a complete **in-RAM metadata replica** of each share and a
  content-addressed data cache, kept coherent by the journal within milliseconds.
- The Mac mounts a **loopback NFS server** backed by that replica: every `stat`/`readdir`/`open`
  is answered on the host at memory speed — zero guest round trips after seeding. NFSv4.1 read
  delegations are granted freely and **recalled precisely when the journal says a file actually
  changed** — exact close-to-open coherence with no polling and no timeout guesswork.

```
Finder / VS Code / git / rg
        │  (macOS in-kernel NFS client, 127.0.0.1)
   mist-hostd ── metadata replica · CAS cache · side-store (.DS_Store never reaches the guest)
        │  (vsock, journal + RPC + bulk lanes)
   mistd in the Debian guest ── fanotify journal · snapshot walker · contained write applier
        │
   native ext4 — the single source of truth
```

Status: **complete and usable.** The workspace ships the guest daemon, host daemon, CLI, NFSv3
read/write surface, opt-in NFSv4.1 delegations, CAS, e2e validation scripts, packaging templates,
and operating docs. The design pack remains in [`design/`](design/) as the implementation and
operations reference:

| | |
|---|---|
| [00-overview](design/00-overview.md) | problem, physics, theses, research provenance |
| [01-architecture](design/01-architecture.md) | components, transports, lifecycles, crate map |
| [02-protocol](design/02-protocol.md) | MWP/1 wire protocol & journal semantics |
| [03-guest-mistd](design/03-guest-mistd.md) · [04-host-mistd](design/04-host-mistd.md) | daemon low-level designs |
| [05-nfs-frontend](design/05-nfs-frontend.md) | NFSv3/v4.1, macOS client quirk catalog |
| [06-consistency](design/06-consistency.md) | guarantees, conflicts, failure matrices |
| [07-security](design/07-security.md) · [08-performance](design/08-performance.md) | threat model · budgets/gates |
| [09-release-status](design/09-release-status.md) · [10-testing](design/10-testing.md) | release status · test strategy |
| [ADR](design/ADR.md) | decision records |

Everyday UX:

```console
$ mist attach dev                 # VM supervisor exposes vsock via MistBridge
$ mist mount dev code             # prints .../Mist/mnt/dev/code, journal-fresh, READDIRPLUS-fast
$ git -C "$HOME/Library/Application Support/Mist/mnt/dev/code" status
$ mist events --follow src/       # the journal as a change feed (better than FSEvents)
```

Requirements (v1 targets): macOS 15+ (Apple silicon; NFSv4.1 surface verified on macOS 26),
Debian 12+ guest (kernel ≥ 6.1; ≥ 6.12 recommended), shares on ext4. Stock Apple
Virtualization.framework — supervisors embed the tiny `MistBridge` Swift package; QEMU/UTM and
remote Linux hosts work over the TCP transport.

License: Apache-2.0 OR MIT (proposed — see ADR-20).
