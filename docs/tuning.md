# Tuning

Defaults are chosen to beat in-guest knfsd on the standard benchmarks (see
`bench/results/2026-06-13-read-final.md`); most installs should change nothing. The knobs
below are for unusual workloads, listed by impact.

## Share-level

| knob | where | default | when to change |
|---|---|---|---|
| `commit = "writeback"` | guest `mistd.toml` per share | `fsync` | Build trees / scratch dirs: COMMIT stops fsync-ing in the guest and WRITEs are acked FILE_SYNC (knfsd `async` semantics). Small-file saves ~2× faster; a guest crash can lose the last seconds of Mac-side writes. Source-of-truth repos: keep `fsync`. |
| `--nfs41` | `mist mount` | v3 | Use for editor/IDE workflows: read delegations give 0-RPC hot loops and ~1 s guest→Mac freshness. Plain v3 has slightly better small-create latency. |

## Transport

- mistd announces TCP endpoints over virtio-net automatically; hostd prefers them per lane
  (journal/rpc/bulk) and falls back to vsock. Nothing to configure; `mist status` shows the
  active endpoint. TCP is cleartext in this release — keep it on the host-only vmnet.
- vsock-only setups work everywhere AVF runs but cap bulk lanes around ~0.9 GiB/s aggregate.

## Mac NFS client (env vars read by hostd at mount time)

| env | default | notes |
|---|---|---|
| `MIST_RSIZE` | 1048576 | The macOS client hard-caps at 1 MiB (larger values fall back to 32 KiB!). Smaller (256-512 KiB) only helps when many parallel readers share one mount. |
| `MIST_BIND_IP` | 127.0.0.1 | Bind the NFS server elsewhere (e.g. the vmnet gateway IP). Measured: no throughput difference; exists for experiments. |
| `MIST_INLINE` | off | Inline request dispatch when the connection is idle. Measured: costs creates ~4 ms/file when the client pipelines mutations; no reliable read win. Leave off. |
| `MIST_SENDFILE` | off | Zero-copy READ replies via sendfile(2). Measured SLOWER than warm-buffer writes on loopback (~880 vs ~1000 MB/s). Leave off. |

## Daemon

- `--cache-max-bytes` (or `cache_max_bytes` in config.toml): CAS high watermark, default
  20 GiB. The cache is content-addressed and survives restarts; eviction starts at 90%.
  `mist cache stats` shows hit rates; a low hit rate with a big working set wants a bigger cap.
- Replica RAM: ~262 B/node measured (1M files+dirs ≈ 263 MB). It is rebuilt from the guest on
  every (re)seed; there is nothing to tune, but monster trees can be excluded per share in the
  guest config.

## What NOT to do

- Don't raise `readahead` past 128 or `rsize` past 1 MiB — the client clamps or misbehaves.
- Don't run `npm install`-style per-file storms through the mount when you can run them in the
  guest; each create crosses the VM boundary (~0.5-1 ms/file even tuned). That's physics, not
  config (see design/08).
- Don't disable the CAS to "save disk" if you read big files repeatedly — warm reads come from
  the cache without touching the guest at all.
