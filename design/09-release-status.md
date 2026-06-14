# 09 — Release Status

Mist is implemented as a complete host/guest file-access stack for macOS clients and Linux
guests. The release includes the Rust daemons, CLI, Swift bridge package, NFS surfaces, content
cache, side-store, packaging assets, and validation scripts described by the rest of this design
pack.

## 1. Delivered Capabilities

- **Transport and session layer:** authenticated ctl/journal/rpc/bulk lanes over AVF vsock via
  MistBridge, TCP, or local UDS test connectors.
- **Guest daemon:** fanotify journal engine, snapshot walker, contained write applier, read
  service, share configuration, and systemd packaging.
- **Host daemon:** session supervision, in-RAM metadata replica, journal apply, resync/scrub,
  mount manager, control API, metrics, side-store, and CAS.
- **Mount surfaces:** NFSv3 read/write loopback server and opt-in NFSv4.1 server with
  journal-driven read delegations.
- **User tools:** `mist` CLI for attach, mount, status, events, conflicts, cache management,
  doctor checks, and version reporting.
- **Validation:** unit/integration tests, fuzz targets, e2e AVF scripts, chaos drills, soak
  scripts, benchmark reports, cargo-deny policy, and Swift package build.

## 2. Acceptance Evidence

The repository keeps runnable checks and dated benchmark notes rather than a separate plan. The
main evidence set is:

- `cargo fmt --all -- --check`
- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo clippy -p mistd --all-targets --target aarch64-unknown-linux-gnu -- -D warnings`
- `cargo deny check`
- `cargo doc --workspace --no-deps`
- `swift build` in `swift/MistBridge`
- `bash -n scripts/*.sh packaging/*.sh`
- AVF e2e, CAS, NFS, chaos, soak, and performance scripts under `scripts/`
- Measurement notes under `bench/results/`

The current implementation has passed the workspace checks above and the targeted real-hardware
scripts documented in `bench/results/`. Long soaks and release signing are operational activities,
not unfinished architecture.

## 3. Current Release Limits

- Mist is a single-user development share, not a multi-user security boundary.
- Cross-boundary byte-range lock enforcement is not provided; macOS uses local locking on the
  mount.
- Mac-side package-manager workloads are correct but not a performance target; run high-churn
  build/package steps in the guest.
- NFSv4.1 is available as an opt-in surface; NFSv3 remains the conservative default.
- Live publication of binary packages depends on the chosen release infrastructure, while the
  package definitions and release scripts live in `packaging/`.

## 4. Risk Register

| # | Risk | Level | Detection | Mitigation |
|---|---|---|---|---|
| R1 | vsock slower than expected on a given AVF/kernel setup | medium | `mist doctor`, transport bench | TCP/virtio-net fallback; larger batches; multiple lanes |
| R2 | macOS NFS client quirk changes behavior across releases | medium-high | doctor checks, conformance/e2e scripts | profile knobs; server-side workarounds; NFSv3 default |
| R3 | NFSv4.1 backchannel/delegation behavior regresses on macOS | medium-high | NFSv4.1 probe/e2e scripts | keep NFSv3 as stable surface; disable delegations per mount |
| R4 | fanotify gaps or overflow cause stale metadata | medium | scrub divergence counters; overflow drill | resync, verify-on-open option, user-triggered `mist sync` |
| R5 | Journal storms affect interactivity | medium | stress and chaos scripts | coalescing, quiescent ladder, bounded queues |
| R6 | Replica memory exceeds a deployment's budget | medium | `mist status`, replica memory bench | compact arenas, node limits, excludes |
| R7 | Loopback NFS under memory pressure stalls hostd | low-high | memory squeeze drill | pooled buffers, hostd memory ceiling, JUKEBOX shedding |
| R8 | Apple ships a first-party Linux-files-on-Mac stack | low-medium | platform monitoring | Mist retains replica/journal coherence and TCP remote mode |

## 5. Releases & Licensing

Tags build the guest `.deb`, host tarball, Homebrew formula, SwiftPM package, and release
artifacts. The project is licensed as **Apache-2.0 OR MIT** unless productization requirements
change.
