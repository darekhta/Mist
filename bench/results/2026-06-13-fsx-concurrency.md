# Soak: the fsx data-mismatch hunt (2026-06-13)

After the memory leak was fixed, the soak still tripped on **fsx data mismatches** (1/842 in
the soak; up to 66/2500 under a 4-way concurrent stress). fsx read a byte and got the wrong
value — silent data corruption, the worst class of bug for a filesystem. Root-causing it took
three layered findings.

## Repro

`e2e-work/fsx-stress.sh`: 4 concurrent fsx loops (each its own file, 800 ops, ≤3 MiB) plus a
guest churn writer, ~12 min. Single-threaded fsx (the write-path gate) was always clean; the bug only
appears under concurrent load. Baseline: **66 mismatches / ~2500 runs**.

## Finding 1 — fingerprint collision (contributing)

The CAS warm-read keys each chunk on `content_fingerprint = FNV(mtime.sec, mtime.nsec, size)`.
The optimistic mtime was set by a per-node read-modify-write (`read base → +1ns`). The macOS
client pipelines several concurrent UNSTABLE writes to one file; two `write()` calls read the
same base mtime and both produce `base+1ns` → an **identical fingerprint for different
content**, so a read could match a stale manifest. Fixed with a process-global monotonic
timestamp (`next_optimistic_ts`) floored at both wall-clock and the node's current mtime: every
write gets a distinct, strictly-increasing stamp → a distinct fingerprint. Also folded the
guest journal's `content_version` into the fingerprint (rotates the key on a guest in-place
same-size write).

## Finding 2 — finalize bound a fresh fingerprint to a stale blob (contributing)

`finalize_stash` carried an `ingested: HashSet<offset>` "already done" filter. A chunk
rewritten after an earlier ingest was **skipped**, but `rebind_manifest` still moved the WIP
manifest to the new fingerprint — binding it to the old blob (under-coverage / stale rebind).
Removed the filter: `covered_chunks` already rebuilds each chunk's *current* content from the
stash, so finalize now ingests the full current set every time (it runs only at idle/commit,
so the re-hash cost is bounded).

## Finding 3 — write-behind applied overlapping writes OUT OF ORDER (the real cause)

Even with both CAS fixes AND a "read straight from the guest for recently-written nodes"
bypass, the stress still failed — now **66 mismatches reading from the guest itself**. So the
*guest data was wrong*, not just the cache.

`write()` spawns a `write_through` task per UNSTABLE batch. The design assumed per-node arrival
order ("the guest sees this node's batches strictly in arrival order"), but spawned tasks race:
two overlapping writes to the same range applied in either order, and the guest kept whichever
landed last. The macOS client serializes overlapping writes on our ack (so the *prefixes* run
in wire order), but the spawned guest-application did not.

**Fix:** a per-node ordering chain. Each `NodeWrites` holds the previous batch's completion
`oneshot::Receiver`; a new batch installs its own and awaits the previous before calling
`write_through`, so guest application is strictly arrival-ordered per node. Cross-node
concurrency and the immediate optimistic ack are unchanged; only same-node application
serializes (measured negligible — a 1 MiB chunk applies in ~0.2 ms, well under the inter-batch
arrival gap even at 1 GB/s). Non-overlapping writes don't need the order but pay only the chain
hop.

## Defense in depth (kept)

- **Reads bypass the CAS for mac-dirty nodes** (written within 5 s): serve the guest
  authoritatively while a file is hot, since the fingerprint can race during active churn. The
  common warm case — GUEST-produced content read from the Mac — is never mac-dirty, so the
  CAS warm path (and the 0-guest-read gate) is unaffected.
- Write-behind backpressure semaphore (96 slots) bounds in-flight payload memory.

## Result

`fsx-stress.sh` after the chain: **0 mismatches / ~3000+ runs** (was 66). Single-file fsx and
the write-path and CAS gates remain clean. 75 workspace tests green, clippy clean.

## Lesson

"The client serializes overlapping writes" is true on the wire but says nothing about how the
*server* applies them. Any write-behind that spawns per-batch tasks must re-establish per-object
ordering explicitly. The bug hid for so long because single-threaded fsx never exercised
concurrent same-file batches.
