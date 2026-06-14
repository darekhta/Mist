# pjdfstest expected-fail manifest (NFSv3, identity squash)

Run context: pjdfstest @ master built on macOS, driven by `prove` as a **non-root** user over the
mist mount (loopback NFSv3 → vsock → guest ext4); `scripts/e2e-m3.sh harden` records the summary,
`scripts/pjd-classify.sh` buckets failures from verbose logs. Numbers from 2026-06-11
(bench/results/2026-06-11-harden.md).

## Results

| area | tests | failed | verdict |
|---|---|---|---|
| chmod | 327 | 175 | expected-fail classes below |
| ftruncate | 89 | 20 | " |
| mkdir | 118 | 47 | " |
| open | 337 | 217 | " |
| rename | 4857 | 3212 | " |
| rmdir | 145 | 58 | " |
| symlink | 95 | 26 | " |
| truncate | 84 | 17 | " |
| unlink | 440 | 266 | " |
| utimensat | 10 | 0 | **PASS** |
| link | 359 | 222 | LINK unimplemented (below) |
| mkfifo | 120 | 72 | MKNOD unimplemented (below) |

## Expected-fail classes (by design or release scope)

1. **Permission-matrix subtests (`-u`/`-g` uid-switching)** — the single largest class (e.g.
   open: 172 of 217; rename: 1006). Mist v1 is a **single-user share with identity squash**
   (design 05 §7): every requester maps to the share's apply identity, so "user A may not touch
   user B's file" semantics are intentionally absent across the boundary. Additionally the suite
   runs non-root, so the `-u` setuid in the pjdfstest binary itself fails. Permanent for v1.
2. **`chown`/`lchown` subtests** — SETATTR uid/gid is deliberately ignored under squash (the Mac
   sends its squashed owner; honoring it would corrupt guest ownership). Permanent for v1.
3. **Hard links (`link` area + `nlink=2` expectations elsewhere)** — LINK is not implemented
   (server replies NFS3ERR_NOTSUPP with a well-formed body). rename/00.t et al. use `link` to
   set up nlink=2 fixtures, so each failure cascades through the dependent subtests. Planned
   (the protocol reserves `RpcReq` space; applier support is a small addition).
4. **mknod family (`mkfifo` area + every fifo/block/char/socket loop iteration)** — MKNOD is not
   implemented (NOTSUPP). pjdfstest loops most areas over file *types*; 4 of 5 iterations need
   mknod, so their whole chains (chmod/stat/unlink of the never-created node) report ENOENT.
   This cascade is the bulk of the remaining chmod/rename/unlink failures. Low priority: fifos,
   sockets and device nodes over a host share are niche; revisit with pjdfstest-under-root in CI.

## Real server bugs this suite caught (fixed during write-path hardening)

- **ACCESS gating broke owner-chmod**: MODIFY/EXTEND/DELETE were granted only when the mode had
  the owner-write bit, so the macOS client refused `chmod +w` on a 0444/0111 file client-side.
  Now granted unconditionally on writable shares; real write enforcement stays in the guest
  (the applier opens as the apply identity → kernel DAC still yields EACCES where due).
- **Names > 255 bytes desynced the reply**: the XDR name read was capped at 255 and a longer
  name failed the parse, producing a garbage reply (mkdir of a 256-char name *appeared to
  succeed*). All name-taking procs now reply NFS3ERR_NAMETOOLONG with correct error bodies.
- **MKNOD/LINK replied with a bare status** (no wcc_data/post_op_attr body) → the client saw
  EBADRPC. Both now reply NOTSUPP with well-formed result bodies.
- **setattr required read access**: chmod/utimes reopened the file O_RDONLY, failing on 0222
  files (and EISDIR risk on dirs). They now operate through the privileged O_PATH `/proc` path
  (owner semantics via CAP_FOWNER); only truncation opens for write as the apply identity.

## Notes

- `utimensat` passes 10/10 — mtime propagation through SETATTR is exact (atime is ignored by
  design, and the area's atime cases tolerate that).
- fsx (3 seeds × 1500 ops: pwrite/truncate/read-verify/reopen against an in-RAM model) is clean
  over the mount — the data path itself is byte-exact; the failures above are all
  semantics/permission classes, not data corruption.
- Re-run: `scripts/e2e-m3.sh harden` (summaries) + `prove -rv /tmp/pjdfstest/tests/<area>` on a
  live mount + `scripts/pjd-classify.sh` (buckets).
