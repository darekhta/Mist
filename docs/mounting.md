# Mounting: privileges, failure modes, and the privileged fallback

`mist mount <vm> <share>` drives `/sbin/mount_nfs` against hostd's loopback NFS server and
attaches it at `$MIST_STATE_DIR/mnt/<vm>/<share>` (default `~/Library/Application
Support/Mist/mnt/...`). This page documents when that works without root, why, and what to do
when it doesn't.

## Why the unprivileged mount works

macOS permits NFS mounts by a non-root user when **all** of these hold (XNU's user-mount rules):

1. **The mountpoint directory is owned by the mounting user.** hostd creates it under the
   user-owned state dir, so this holds by construction.
2. **`nosuid` and `nodev` are in effect.** The kernel forces them for user mounts; mist passes
   them explicitly anyway (see the options string in `mount.rs`), so nothing is silently changed.
3. The mount does not shadow another user's files (loopback into your own state dir never does).

This is the same mechanism that lets `hdiutil attach` work unprivileged. It is **not** a setuid
helper: `/sbin/mount_nfs` runs as you, and the resulting mount is flagged `mounted by <you>` in
`mount(8)` output. `umount` of your own user mount is likewise unprivileged.

## Known environment-dependent failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `mount_nfs: ... Operation not permitted` | MDM / parental-control profile sets `vfs.generic.skip_user_mounts=1`, or the mountpoint isn't owned by you (e.g. `MIST_STATE_DIR` pointed somewhere root-owned) | use the privileged fallback below, or point `MIST_STATE_DIR` at a directory you own |
| `mount_nfs: ... mountpoint is not a directory` / `No such file or directory` | state dir on a volume that disallows your user (external disk with “ignore ownership”) | move `MIST_STATE_DIR` to the boot volume |
| mount succeeds but Finder shows a generic network volume and stalls | Spotlight tried to index before `mdutil -i off` landed | retry; hostd also synthesizes `.metadata_never_index` at the share root via the side-store, which persistent-fixes this |
| `rm -rf` of the state dir hangs after a hostd crash | a stale mount whose server is gone (`hard` mount semantics) | `umount -f <mountpoint>` first; the e2e harness does this automatically (`clear_stale_mounts`) |

## Privileged fallback

If your environment blocks user mounts, mount manually as root against the same loopback server.
Get the port from `mist status` (each mounted share lists its `port`), then:

```sh
sudo /sbin/mount_nfs \
  -o vers=3,tcp,port=<PORT>,mountport=<PORT>,rw,nolocks,locallocks,rdirplus,\
rsize=1048576,wsize=1048576,hard,intr,noatime,nosuid,nodev,actimeo=5 \
  127.0.0.1:/<share> <mountpoint>
```

For a persistent setup, allow exactly that command via sudoers (visudo):

```
%staff ALL=(root) NOPASSWD: /sbin/mount_nfs -o vers=3\,tcp\,port=* 127.0.0.1\:/* *
%staff ALL=(root) NOPASSWD: /sbin/umount /Users/*/Library/Application\ Support/Mist/mnt/*
```

Scope it tighter (a specific user, a specific share path) where policy demands. mist itself never
invokes `sudo`; the daemon's mount attempt fails with the `mount_nfs` stderr verbatim, and this
page is the documented next step.

## Hardening notes

- The NFS server binds `127.0.0.1` on an ephemeral port — it is never reachable off-host, and
  file handles are HMAC'd with a per-install secret (`handle.secret`), so a local process can't
  forge handles without it.
- `nosuid,nodev,noatime` are always passed; the share's executable bits are preserved (build
  trees work) but setuid binaries inside the share do not gain privilege on the Mac.
- Locks: `nolocks,locallocks` — locking is Mac-local only; cross-boundary `flock` is documented
  as unsupported in v1.
