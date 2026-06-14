# 08 — Performance Model, Budgets & Bench Harness

Numbers are tagged **[R]** (verified research / local man-page facts) or **[E]** (engineering
estimate tracked by the benchmark and acceptance scripts). There is no standalone benchmark
phase; performance checks live with the runnable validation suite and are summarized in
`09-release-status.md`.

## 1. The physics (recap)

- Per-op RPC floors make per-op designs unwinnable for metadata storms: FSKit ≈ 121 µs/op [R];
  any VM crossing ≥ tens of µs; 200k-op `git status` ⇒ tens of seconds. Native APFS/ext4 metadata
  ops are ~1–3 µs warm [E].
- Therefore Mist's hot path budget is: **0 guest RPCs** for metadata, **1 loopback RPC per
  directory** for cold enumeration (READDIRPLUS), **0** for warm (kernel caches), and data reads
  amortized via readahead + caches.
- The loopback RPC cost = macOS kernel NFS client → TCP loopback → hostd handler (RAM) → back.
  Budget ≤ 40 µs server-side, ≤ 60 µs end-to-end p50 [E].

## 2. Cost model per operation class

| Op class | Cold (first touch) | Warm (kernel caches valid) | Expired (ac timeout passed) |
|---|---|---|---|
| stat/lookup | covered by parent dir's READDIRPLUS page (amortized ≪ 1 RPC/file) | ~1 µs (in-kernel) | per-dir revalidation GETATTR ≈ 60 µs, entries re-trusted |
| readdir | 1 READDIRPLUS/«~4k entries» page ≈ 60–200 µs | ~µs | cookieverf check + reuse or refill |
| open (v3) | GETATTR ≈ 60 µs | GETATTR ≈ 60 µs (close-to-open, always) | same |
| open (v4.1 + delegation) | OPEN once ≈ 100 µs, delegation granted | **0 RPC** | 0 RPC until recall |
| read | RPC + transport (see §3) + CAS fill | client page cache ≈ RAM | revalidate-by-attr unless delegated |
| write+close | WRITE×n loopback + COMMIT (guest pwritev+fdatasync ≈ 1–10 ms) | — | — |
| guest change visibility | journal lag ≈ ms | — | — |

### Workload projections [E]

| Scenario (on reference Apple Silicon Pro machine) | Native | Mist target | Per-op mechanics |
|---|---|---|---|
| `rg --files` linux.git (~87k files, ~5.4k dirs), cold mount | ~0.3 s | **≤ 2 s** | ~5.4k READDIRPLUS pages |
| same, warm | ~0.15 s | **≤ 1.3×native** | kernel caches |
| `git status` linux.git cold (incl. reading .git index+objects) | ~0.5 s | **≤ 4 s** | enumeration + index read (~200 MB packs hit readahead path) |
| `git status` warm | ~0.2 s | **≤ 1.3×** | caches |
| chromium-scale (~400k files/40k dirs) cold scan | ~1–2 s | **≤ 15 s** (stretch ≤ 8 s) | 40k dir pages |
| VS Code "open folder" index of 100k files | ~2 s | **≤ 2× native** | enumeration + sampled reads |
| guest `make` touches 5k files → Mac rebuild-watcher correctness | n/a | events ≤ 10 ms p99 via `mist events`; mount view fresh per G1 | journal |
| 4 GiB file sequential read, cold | ~3 s (disk) | **≥ 1.5 GB/s** sustained [transport acceptance] | bulk lanes + readahead |
| `npm install` executed on the Mac mount (anti-pattern) | — | works; ~0.5–1 ms/file overhead; documented "run it in the guest" | per-file Create/Write/Commit chain |

## 3. Transport budgets

| Metric | Budget [E] | Fallback if missed |
|---|---|---|
| vsock RPC RTT p50 (4-byte echo, via MistBridge UDS path) | ≤ 80 µs | direct-TCP virtio-net path; batch harder (StatBatch, larger pages) |
| vsock RTT p99 under load | ≤ 300 µs | same |
| vsock single-stream throughput | ≥ 1.5 GB/s | multiple bulk lanes (already designed); virtio-net multiqueue TCP |
| 4-lane aggregate | ≥ 3 GB/s | same |
| TCP/virtio-net RTT (reference) | measure | informs default transport choice per setup |
| VM pause/resume (Mac sleep) connection survival | survives | bridge redial logic (already designed) |

The transport bench binary (`mist-bench transport`) ships permanently — `mist doctor` runs a 1 s RTT
probe and flags degraded transports in the field.

## 4. Component budgets

| Component | Budget [E] | Check |
|---|---|---|
| journal lag p99 idle / storm | ≤ 5 ms / ≤ 50 ms\@10k ev/s | journal/e2e |
| snapshot rate (wire+apply) | ≥ 150k entries/s; 1M files ≤ 10 s | seed bench |
| replica GETATTR/LOOKUP (in-process) | ≤ 3 µs p50 | criterion |
| READDIRPLUS page build (4k entries) | ≤ 1 ms | NFS bench |
| NFS loopback NULL / GETATTR e2e | ≤ 40 / 60 µs p50 | NFS bench |
| applier mutation (optimistic, in-process) | ≤ 10 µs | mutation bench |
| Mac 1 MiB create+write+close (COMMIT incl. guest fdatasync) | ≤ 25 ms | write e2e |
| CAS hit read 1 MiB | ≤ 200 µs + memcpy | CAS bench |
| delegation recall → DELEGRETURN p99 | ≤ 10 ms | NFSv4.1 bench |
| hostd RSS @ 1M nodes | ≤ 350 MB compact layout | replica memory bench |
| mistd RSS / idle CPU | ≤ 150 MB / < 1 % | daemon bench |
| seed of linux.git share, cold guest cache | ≤ 3 s | seed bench |

## 5. Tuning knobs (reference)

Host: mount profiles (ac* tiers, `05` §3), readahead window cap, CAS size/watermarks, bulk lane
count, RPC concurrency, JUKEBOX deadline. Guest: `max_queued_events`, journal batch size/linger,
MODIFY coalescer rate, io_uring on/off, snapshot parallelism, commit policy (`fsync`/`writeback`).
Defaults are the tested configuration; `mist doctor` diffs live settings against defaults.

## 6. Competitive comparison protocol (honesty kit)

Same hardware, same tree (linux.git + chromium snapshot), scripted:
1. Native ext4 in guest (the floor for guest ops) + native APFS on host (Mac floor).
2. Mist (each surface/profile).
3. OrbStack `~/OrbStack` (same tree inside an OrbStack machine).
4. AVF virtiofs in the *other* direction (context, not a competitor).
5. Strawman: NFS server in guest mounted directly by macOS over virtio-net (what "just use NFS"
   gives — isolates the replica's contribution).
Metrics: cold/warm scan, git status, open-file latency distribution, change-visibility latency
(guest touch → Mac stat observes), single-file throughput. Output: one markdown table, committed
to `bench/results/<date>-<machine>.md` by `mist-bench compare --all`.

## 7. Bench harness (`mist-bench`)

- Scenarios as code (Rust, criterion for micro; scripted flows for macro) + fixtures
  (`linux.git` pinned tag; synthetic trees: 1M-files flat-ish, deep-narrow, 100k-dirs).
- Cache-state control: cold = `umount/mount` + guest `echo 3 > drop_caches`; warm = repeat ×2;
  expired = warm + sleep past `actimeo` with clock injection (or remount with a tiny cache timeout
  and scale).
- N ≥ 10 runs, report p50/p95, fail CI gate on > 15 % regression vs committed baseline
  (`bench/baselines/`, updated deliberately).
- Profiling playbook: host — Instruments (System Trace + Allocations) on hostd, `nfsstat -m`
  client-side counters; guest — `perf`, io_uring stats, `mist_*` Prometheus series; wire — lane
  byte/frame counters per session.
