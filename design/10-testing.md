# 10 — Testing Strategy

The system's risk concentrates in three places: (1) the replica's correctness under arbitrary
event interleavings, (2) protocol robustness against hostile/corrupt input, (3) integration with
two opaque kernels (Linux fanotify semantics, macOS NFS client behavior). The strategy attacks
each with the cheapest tool that can falsify it.

## 1. Unit layer

- `mist-proto`: proptest round-trip (encode∘decode = id) for every message; cap-violation
  rejection tests; golden wire vectors committed (cross-version compatibility tripwire).
- `mist-replica`: table-driven apply tests per record type × edge (missing parent, dup create,
  rename-over, tombstone races, pending resolution, cookie stability across mutations).
- `mist-cas`: chunking determinism, manifest invalidation on version change, eviction watermarks,
  crash-mid-ingest (redb transactionality).
- `mist-nfs`: XDR golden vectors (captured from macOS client traffic), errno mapping table.

## 2. Model-based replica testing (the centerpiece)

A reference model: a plain in-memory tree (`HashMap<PathBuf, RefNode>`) with naive semantics.
Generators produce random op sequences applied to **both** a real guest-side tree simulation and
the model:

```
ops := create|write|close|chmod|rename|unlink|mkdir|rmdir|link|symlink  (weighted)
pipeline A: ops → synthetic fanotify events (incl. reorder-within-bounds, drops→Overflow,
            duplicate delivery) → journal records → replica apply
pipeline B: ops → reference model directly
invariant: after quiesce (+ resync if Overflow injected), replica ≡ model
           (tree shape, attrs modulo atime, dirgen monotonicity, invariants I1–I8)
```

Plus targeted properties: apply idempotence (`apply(r); apply(r)` ≡ once), snapshot⊕journal
consistent-cut (run snapshot of model at random mid-sequence point, buffer+replay journal,
compare), conflict-policy outcomes (dirty×event matrix from `06` §6).

This suite runs in milliseconds per case, thousands of cases per CI run, and is where journal/
resync bugs die before ever touching a kernel.

## 3. Fuzzing (release gate)

cargo-fuzz targets: frame decoder (arbitrary bytes), `Rec` stream applier (decoded-but-hostile
records against a seeded replica — must never panic/OOM/violate invariants), XDR request parser,
side-store keys. CI: short runs per PR; nightly long runs; corpora committed.

## 4. Integration layer

- **Fake-guest** (runs on macOS CI): in-process `mistd` simulator speaking MWP over UDS —
  scripted journals, snapshot streams, fault injection (gaps, dup batches, mid-stream
  disconnects, Overflow). Drives hostd + real NFS server + real macOS mount in CI.
- **Linux-only mistd tests** (Linux CI, root container): loop-device ext4 fixture; real fanotify;
  storm generator (parallel untar/rm -rf/checkout loops); asserts records vs ground truth
  (re-walk); overload ladder behavior; applier containment (escape-attempt suite: crafted names
  rejected at decode, symlink-component traps, cross-mount handles → fsid rejection).
- **E2E** (local + nightly self-hosted Mac): `tests/e2e/run.sh` — boots Debian via `mist-vmshim`
  (AVF), full stack, runs the scenario suite: seed, mount, Finder-ish ops (scripted via `osascript`
  where needed), git/rg flows, guest-churn visibility, sleep/wake, daemon-kill matrix.

## 5. Conformance & data integrity

- **pjdfstest** against a write-enabled mounted share: expected-fail manifest committed and reviewed
  (anticipated: atime behaviors, utimensat granularity under squash, mknod corner cases on v3,
  locking suite excluded). Run in `passthrough` identity mode (squash intentionally changes
  ownership semantics).
- **fsx** (long runs, both small and multi-GB files) over the mount — write-path torture incl.
  truncate/extend interleavings; run against v3 and v4.1 surfaces.
- macOS client matrix: latest macOS + previous major (e.g. 26.x, 15.x) on the nightly box.

## 6. Chaos Suite

kill -9 each of {mistd, hostd, supervisor} under load → assert recovery per `06` §7 matrix;
guest reboot mid-write; Mac sleep mid-read; bridge socket deletion; journal overflow storm
(sysctl-shrunk queue); ENOSPC in guest; CAS disk-full; memory squeeze on hostd (R8 drill:
`memory_pressure -S` while fsx runs); clock jumps (guest NTP step) — assert no consistency
violation, bounded unavailability, accurate `mist status`, zero scrub divergence after settle.

## 7. Performance gates in CI

`mist-bench` scenarios from `08` §7 run nightly on the self-hosted M-series box; compare against
committed baselines; > 15 % regression fails. Micro-benches (criterion: replica ops, READDIRPLUS
page build, codec) run per-PR with lighter thresholds (50 %— catch catastrophes, not noise).

## 8. Quality bars

- `cargo fmt` + clippy (deny warnings; `await_holding_lock` enforced) + `cargo-deny` per PR.
- `unsafe` policy: forbidden except in `mistd::fanotify` FFI, `mistd::uring`, and audited buffer
  pools; each block carries a SAFETY comment; Miri on the data-structure crates.
- MSRV pinned in workspace; bumped deliberately.
- Coverage: no hard % target; the model-based suite + fuzz corpora are the real bar. Every bug
  found in integration/e2e gets a minimized model-layer or unit reproduction before the fix lands
  ("bugs move down the pyramid").
