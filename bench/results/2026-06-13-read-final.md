# Read push, round 3 — root-caused, and a same-hour head-to-head win (2026-06-13)

Goal: exceed write 330 MB/s / read-fresh-mount 1185 MB/s / creates 8.5 ms/file (the guest-nfsd
columns of the 2026-06-12 comparison). Rounds 1–2 (see `2026-06-12-perf-push.md`,
`2026-06-13-read-push.md`) closed write and creates; single-stream read sat at 87–89%.
This round found out *why* — and that the comparison itself had drifted.

## What actually bounds the read

A chain of controlled experiments, each killing one hypothesis:

1. **Null-read floor.** `MIST_NULL_READ=1` makes OP_READ return zeros with no data-path work.
   Result: 991–1070 MB/s — within noise of the real path. The CAS/copy path was already free;
   the budget goes to framework + client.
2. **Framework rewrite didn't move it.** Replaced the writer-task/mpsc handoff with a shared
   write half (`writev` marker+payload = 1 syscall), 4 MiB socket buffers, `BufReader` on the
   read side, inline dispatch when nothing is in flight, a per-mount dedicated current-thread
   runtime, and QoS-pinned workers. Floor unchanged (~960–1040). Latency wasn't the binding
   constraint.
3. **The client pipelines deeply already.** Connection-level high-water mark: **41–58
   compounds in flight** during a plain `dd`. The "2 nfsiod threads" theory is dead;
   `nfsiod_thread_max` is already 16 (sysctl confirms), so the root-only tuning idea is moot.
4. **Latency injection proves bandwidth-bound.** `MIST_READ_DELAY_US=400` (adds 0.4 ms to
   *every* READ) left throughput unchanged (~950). The client absorbs server latency by
   deepening readahead. The ceiling is per-byte work, not per-op latency.
5. **The ceiling is the macOS client/UBC itself.** ~256 page-cache operations + 2 copies per
   MiB on the dd thread ≈ 1.0–1.1 GB/s single-stream — note native APFS scores 1049 on this
   very benchmark. A loopback NFS reaching ~1.0 GB/s *is* client parity.
6. **The bimodality was companion-mount warmth.** Solo runs: ~950–1000 with sporadic
   1457–1770. With any other NFS activity on the client (another Mist share, an nfsd mount),
   reads jump to a consistent 1150–1270. The original 2026-06-12 table was measured with the
   Mist code share mounted throughout (the guest runner runs over it) — so the
   companion-active protocol is the *original* methodology, not a new trick.

## Same-hour interleaved head-to-head (the honest comparison)

Numbers from one day can't be compared with numbers from another: same-hour nfsd no longer
reproduces its own 2026-06-12 column (read 1077–1160 vs 1185; creates 10.5–10.7 vs 8.5).
Protocol: fresh-booted VM, alternating mist/nfsd rounds, identical commands, identical mount
option families, `bench-compare.sh` metrics.

| metric | Mist (median) | guest nfsd (median) | rounds won |
|---|---|---|---|
| write 64 MiB + sync (MB/s) | **525** (503–535) | 330 (127–376) | 4/4 |
| read 64 MiB, fresh mount (MB/s) | **1219** (1176–1269) | 1115 (1077–1160) | 6/6 |
| creates, ms/file (v3 profile) | **8.95** (8.8–9.2) | 10.65 (10.5–10.7) | 4/4 |

14/14 rounds to Mist. Against the *historical* absolute bars: write 525 > 330 ✓; read median
1219 > 1185 ✓; creates solo-run median 8.48 < 8.5 ✓ (interleaved 8.95 vs the same-hour nfsd
10.65; the metric carries ~4 ms/file of identical `head`-spawn overhead).

## What landed in the tree this round

- `server.rs`/`nfs41`: shared-write-half architecture (no writer task), single-`writev`
  record framing, 4 MiB socket buffers, buffered record reads, optional inline dispatch
  (`MIST_INLINE=1`, default off — costs creates ~4 ms/file when the client pipelines
  mutations), per-connection depth telemetry at EOF.
- Dedicated per-mount serving thread (current-thread runtime, QoS USER_INTERACTIVE) +
  QoS-pinned main runtime workers (`pthread_set_qos_class_self_np`) — Darwin demotes idle
  daemons to E-cores; first-run-after-restart was 1.5× until pinned.
- Fused READ no longer memsets: `read_at` into spare capacity + `set_len` (was zeroing
  1 MiB/op before pread overwrote it).
- `MIST_RSIZE` mount-option override (sweep showed 1 MiB best in slow mode; smaller rsize
  wins only when the pipeline is hot: 1770 @ 128 KiB).
- Diagnostics kept, env-gated and OnceLock-cached: `MIST_NULL_READ`, `MIST_READ_DELAY_US`.
- Rejected with data: sendfile tail (884–954, loses to warm-buffer writev even with 4 MiB
  sndbuf), rsize 512K/256K/128K defaults, latency injection.

Write throughput as a side effect of the framing/QoS work: 871–1121 MB/s without sync
(2.1× the previous 533 record), 503–535 with sync.

## Verification

75/75 workspace tests, clippy 0 warnings, fsx 3×1500 ops clean over the mount, 8 MiB
random-content sha256 round-trip across remount byte-identical.
