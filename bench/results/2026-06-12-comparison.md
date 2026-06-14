# Mist vs. alternatives — same tree, same hardware (2026-06-12)

> **Superseded for bulk throughput + creates (2026-06-13):** after the perf pushes, a
> fresh-boot same-hour interleaved head-to-head gives Mist the win on all three contested
> rows — write 525 vs 330 MB/s (with sync), read-fresh-mount 1219 vs 1115 MB/s, creates
> 8.95 vs 10.65 ms/file — 14/14 rounds. The nfsd column below does not reproduce on the same
> machine today (day-to-day drift); see `2026-06-13-read-final.md` for the protocol and the
> root-cause analysis (single-stream reads are bounded by the macOS client/UBC at ~1.0–1.2
> GB/s — native APFS itself scores 1049 here).

Protocol: design 08 §6. Host macOS 26.5 (M-series); guest Debian 13 / 6.12.90 on AVF (6 vCPU,
4 GiB). Tree: 20,000 × 1 KiB files in 2,000 dirs on guest ext4 (`/srv/code/tree`), generated
natively in the guest. Every Mac-side target reads the SAME guest tree. "Cold" = fresh mount
(client caches dropped); server-side state stays warm — that's each product's architecture.
Driver: `scripts/bench-compare.sh` (orchestrated through the guest `mist-runner` unit — guest
exec over the Mist mount itself, no SSH).

| metric | guest ext4 (floor) | Mac APFS (floor) | **Mist v4.1** | **Mist v3** | guest nfsd (virtio-net) | sshfs |
|---|---|---|---|---|---|---|
| cold enumerate 20k files | 27 ms | 110 ms | **257 ms** | 665 ms | 463 ms | n/a¹ |
| warm enumerate | 11 ms | 82 ms | **80 ms** | 79 ms | 88 ms | n/a¹ |
| stat storm (`find -ls`, warm) | 46 ms | 135 ms | **181 ms** | 186 ms | 353 ms | n/a¹ |
| hot open/read/close (µs/iter) | 2.8 | 13.0 | 17.8 | 16.1 | 19.3 | n/a¹ |
| …server ops during 10k-loop | — | — | **3** (delegation) | ~10k GETATTRs² | ~10k GETATTRs² | n/a¹ |
| write 64 MiB (MB/s) | — | 508 | 141 | 89 | **330** | n/a¹ |
| read 64 MiB, fresh mount (MB/s) | — | 1049 | 340 | 350 | **1185** | n/a¹ |
| create 200 × 1 KiB (ms/file)³ | — | 4.4 | 17.1 | 15.0 | **8.5** | n/a¹ |
| guest change → Mac visibility | 0 | — | **~0 (exact: 1009 ms @ 1 Hz writer)** | ~4.9 s (actimeo) | ~5.0 s (actimeo) | 20 s+ (typ.) |

¹ macFUSE kernel extension not approved on this machine (requires manual System Settings
  action + reboot) — sshfs would not mount. That install friction is itself a data point;
  known sshfs figures put per-op latency at 100s of µs–ms and coherence at its cache timeout.
² By protocol (close-to-open: one GETATTR per open). The loop runs fast anyway because the
  attr cache absorbs them within actimeo; the difference is server load and freshness, not
  client-side µs.
³ Includes ~4 ms/file of Mac-side process-spawn overhead (`head` per file — identical across
  targets; the APFS row is effectively that overhead).

## Reading

- **Metadata: Mist already beats the kernel nfsd baseline.** Cold enumeration 1.8× faster
  (257 vs 463 ms), stat storms 2× faster (181 vs 353 ms) — the RAM replica answers locally
  in ~60 µs instead of crossing the VM per directory. Warm enumeration MATCHES native APFS
  (80 vs 82 ms): once the macOS kernel caches are hot, Mist adds nothing.
- **Freshness is the category win.** v4.1 sees every 1 Hz guest write the second it lands
  (journal → CB_RECALL p99 1.2 ms). Plain NFS and Mist v3 are blind for `actimeo` (≈5 s);
  sshfs for its cache timeout. No attr-cache tuning tradeoff: exact + zero hot-loop RPCs
  simultaneously.
- **Bulk throughput: kernel nfsd wins today** (1185 vs ~350 MB/s read, 330 vs 141 write).
  Known levers, not physics: our loopback server processes one compound at a time per
  connection and data double-hops (loopback + vsock/TCP). Parallel slot execution and
  nconnect-style multi-connection are hardening candidates. Note Mist reads come from the CAS —
  they survive hostd/VM restarts and don't touch the guest at all (nfsd's page cache dies
  with the guest).
- **Creates favor nfsd** (8.5 vs ~16 ms/file): single hop vs our NFS→hostd→RPC→applier
  double hop with per-create fdatasync. `commit=writeback` shares halve this; batching the
  create+write+commit sequence is another hardening lever.
- **OrbStack**: not installed on this machine; the design's honesty-kit comparison against
  `~/OrbStack` remains open for a release comparison.
- And the part no table shows: plain nfsd needed guest-side packages, /etc/exports, and an
  open TCP port; sshfs needed a kext the OS refused; Mist needs `mist mount dev code`.
