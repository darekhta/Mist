# Troubleshooting

Start with `mist doctor` — it checks the daemon, config, token/secret hygiene, sessions and
mounts, and its ⚠/✗ lines point here.

## The mount

**`mount_nfs failed: Operation not permitted`** — unprivileged NFS mounts require a
user-owned mountpoint; see docs/mounting.md (including the sudoers fallback for shared
machines).

**Mount hangs (Finder beachball / `ls` stuck)** — the NFS client is `hard`-mounted and hostd
is down or wedged. `mist doctor` distinguishes the cases:
- daemon dead → start it; the kernel mount reconnects by itself (hostd rebinds the same port
  on restart and adopts surviving mounts).
- daemon up but mount stale ("kernel disagrees") → `umount -f <mountpoint>` then
  `mist mount` again.

**Stale data on a v3 mount** — expected for up to `actimeo` (5 s) after a guest-side change.
Use `--nfs41` mounts for ~1 s freshness, or `mist events --follow` for an exact feed.

## Sessions

**`mist status` shows `[disconnected]` / `[attaching]`** — the bridge socket or VM is gone.
Check the VM is booted and `mistd` is active (`systemctl status mistd` in the guest). The
supervisor redials with backoff forever; status returns to `[live]` on its own once the guest
is reachable.

**`auth failure` in guest logs** — token mismatch. The token file on both sides must contain
the same ≥32 bytes (`mist doctor` checks the Mac side's mode and size).

**Status `[seeding]` for a long time** — first contact with a huge share; seeding runs at
hundreds of thousands of entries/s, so minutes mean millions of files. Mounts created during
seeding serve as the tree fills (directories answer `EAGAIN`-style retries until complete).

## Writes

**Mac writes EIO after ~30 s** — the mutation path to the guest was down past the JUKEBOX
deadline (design 06 §7). Fix the session (above); writes resume immediately.

**`ENOSPC`/`EDQUOT`** — the *guest* filesystem is full; the errno passes through verbatim.
Free space in the guest; nothing on the Mac side to clear.

**Guest-vs-Mac write collision** — last-close-wins is applied automatically;
`mist conflicts` lists every detected collision with paths and directions. If you see
repeated conflicts on the same path, something on both sides is writing it (e.g. a guest
build artifact also edited on the Mac) — pick one side.

## Cache & integrity

**`mist cache stats` shows corrupt-dropped > 0** — a CAS blob failed its hash on read; it was
dropped and refetched (self-heal). Recurring corruption points at host disk problems.

**Suspected divergence (mount shows different bytes than the guest)** — run
`mist cache scrub`, then compare again after a few seconds; the scrubber re-verifies and the
journal heals metadata. If a *file listing* diverges persistently, capture
`RUST_LOG=debug mist-hostd` output and file a bug — the chaos suite (scripts/chaos-m6.sh)
asserts settle-equality, so a reproducible divergence is a real bug.

## Logs

- Host: hostd logs to stderr (`RUST_LOG=info` default). Paths never appear in logs at info+
  (privacy by design); debug builds accept `MIST_LOG_NAMES=1`.
- Guest: `journalctl -u mistd`.
- NFS client (rarely needed): `nfsstat -m` shows negotiated mount parameters.
