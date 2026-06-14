# Mist measured vs projected (perf snapshot, 2026-06-12)

Setup: Apple Silicon Pro, macOS 26.5, Debian 13 guest (6 vcpu/4 GiB) under AVF, loopback NFSv3 mount →
vsock via MistBridge (DispatchSource). Sources: m1/m2/m3 e2e results + this live run.

## Transport (design 08 §3 targets)

| Metric | Target | Measured | Verdict |
|---|---|---|---|
| vsock RTT p50 (bridge path) | ≤ 80 µs | 125 µs before bridge rewrite → **63 µs** post-kqueue | ✅ |
| vsock RTT p99 under load | ≤ 300 µs | 4.9 ms before bridge rewrite → **93 µs** post-kqueue | ✅ |
| vsock single-stream | ≥ 1.5 GB/s | 0.15 → **0.38 GiB/s** | ❌ (~4× short) |
| vsock 4-lane aggregate | ≥ 3 GB/s | 0.82 → **0.86 GiB/s** (AVF ceiling) | ❌ |
| TCP/virtio-net reference | measure | **9.5 GiB/s single / 12.0 GiB/s ×4** | ✅ (the designed mitigation) |
| NFS-stack cold seq read (256 MiB) | — | **554 MiB/s** (bulk lanes + client readahead) | informative |
| NFS-stack seq write (256 MiB, fsync) | — | **143 MiB/s** (chunked inline writes; bulk streams) | informative |

Verdict: latency goals exceeded after the bridge rewrite; raw vsock throughput misses are the
AVF vsock aggregate ceiling, not our code — the design's fallback (TCP/virtio-net for bulk)
measures 8–14× above target and rides in as the bulk-lane option.

## Component budgets (design 08 §4)

| Budget | Target | Measured | Verdict |
|---|---|---|---|
| Seed rate (wire+apply) | ≥ 150k entries/s | **311–389k entries/s** | ✅ 2.2–2.6× |
| 1 MiB save incl. COMMIT (write-path target) | ≤ 25 ms | **p50 9.3 ms, p90 11.2, max 13.7** | ✅ |
| Warm stat (client cache, "0 RPC" model) | ~µs | **1.9 µs/stat** | ✅ native-class |
| Cold stat after readdirplus walk | amortized ≪1 RPC | **2.8 µs/stat** (page pre-fill works) | ✅ |
| Cold dir walk | 1 RPC per ~4k-entry page | 1003 entries in **1–2 ms** | ✅ |
| fsx mixed ops over mount | — | ~**0.8 ms/op** (1500 ops ≈ 1.3 s) | ✅ healthy |
| Small-file create+write+close | ~0.5–1 ms/file (anti-pattern row) | **5.3 ms/file** (189 files/s) | ❌ 5–10×: dominated by per-close guest fdatasync (G2); batching/UNSTABLE-aware COMMIT remains future tuning; "run builds in the guest" stays the answer |
| Journal lag p99 | ≤ 5 ms idle | bounded only at e2e poll granularity (≤1.5 s); functional via conflicts e2e | ⚠ unmeasured at precision |
| Replica getattr (in-process) | ≤ 3 µs p50 | no criterion bench yet | ⚠ unmeasured |
| NFS NULL/GETATTR e2e | ≤ 40/60 µs p50 | not measured directly (implied sub-ms by walk page math) | ⚠ unmeasured |
| hostd/mistd RSS | ≤ 700 MB @1M / ≤150 MB | not measured | ⚠ unmeasured |
| Scenario table (linux.git rg/git-status) | ≤ 2 s / ≤ 4 s cold | needs linux.git-scale tree in guest | ⚠ not captured in this run |

## Bug found by this measurement run (fixed)

**1 MiB NFS WRITE hung the mount forever**: `RpcReq::Write` data is inline and the rpc lane's
frame cap is `MAX_FRAME` = 1 MiB, so a full-wsize WRITE didn't fit one frame; the transport
error was mapped to JUKEBOX silently and the hard-mount client retried forever. fsx never saw
it (≤128 KiB ops). Fixed: the surface chunks writes at 512 KiB/RPC, fdatasync only on the last
chunk; transport errors in the mutation path are now logged. (Streamed bulk-lane writes per
the design-02 bulk stream design handles that path.)

## Robustness backlog (observed during the run, not yet fixed)

- hostd dial/handshake has no timeout: if the bridge's `device.connect` never completes (seen
  when guest vsock is half-up), the supervise loop wedges in "connecting" forever.
- mistd hit systemd's start-limit during the oversized-frame episode (crash-loop ×5 → permanent
  stop; guest needed a reboot). mistd must survive arbitrary bad frames + unit gets
  `StartLimitIntervalSec=0`.
- AVF `VZVirtioSocketDevice.connect` can hang without completion in degraded guest states —
  MistBridge should time out the connect and reply ERR.
