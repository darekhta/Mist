# 05 — NFS Frontend (v3 and v4.1 with journal-driven delegations)

The mount surface strategy ships **NFSv3** as the conservative default because it is the smallest
correct thing the macOS in-kernel client consumes well (EdenFS chose v3 for exactly this reason).
Mist also ships **NFSv4.1** for journal-driven read delegations. Both sit behind `MountSurface`,
and `mist mount --nfs41` opts a mount into the v4.1 server.

Both servers are in-process in hostd and bind loopback by default. The current implementation uses
one ephemeral TCP port per mounted share and serves both NFS and MOUNT on that port for v3; v4.1
uses the same single-port shape without MOUNT. `MIST_BIND_IP` can move the listener to another
local interface for experiments.

## 1. ONC-RPC layer (`mist-nfs` crate)

Record-marking TCP framing; AUTH_SYS accepted (uid/gid noted for `identity = passthrough`,
ignored under `squash`); AUTH_NONE accepted for NULL. XDR via hand-written encode/decode against
fuzz tests (`10` §3) — no codegen dependency. Max request 1 MiB + wsize; replies streamed.
The server is bespoke instead of `xetdata/nfsserve` because caching headers, wcc data, mutation
semantics, and delegation hooks need full control.

## 2. File handles

```
fh = "MST1" (4) ‖ share:u16 ‖ flags:u16 ‖ ino:u64 ‖ gen:u32 ‖ mac:u64   = 28 bytes
mac = trunc8(blake3_keyed(hostd_secret, fields))
```

Fits v3 (≤64 B) and v4 (≤128 B). The MAC stops local handle forgery (NFS handles are bearer
tokens; see `07` §4). `hostd_secret` is generated at first run, persisted (handles survive hostd
restarts; NFS clients hold them across our restarts). ESTALE on MAC failure, unknown NodeKey
(post-tombstone-linger), or epoch-evicted share.

## 3. MOUNT3 + exports

`MNT(/<share>)` → root fh (share must be ≥ SEEDING). `EXPORT` lists the active share. No
`umntall` bookkeeping (stateless). Mount command issued by hostd (mount manager):

```
mount_nfs -o vers=3,tcp,port=<port>,mountport=<port>,rw,nolocks,       \
  locallocks,rdirplus,readahead=128,rsize=<rsize>,wsize=1048576,       \
  hard,intr,noatime,nosuid,nodev,actimeo=5,                            \
  127.0.0.1:/<share>  "$MIST_STATE_DIR/mnt/<vm>/<share>"
```

(For NFSv4.1, the mount spec is `127.0.0.1:/`, `vers=4.1`, and there is no `mountport`,
`nolocks`, or `locallocks` option.) `rsize` defaults to 1 MiB and can be overridden with
`MIST_RSIZE` for experiments.

### Profiles

The current product does not expose per-share mount profiles in host config. It uses one conservative
option set with `actimeo=5`; `nfs.conf(5)` remains a user escape hatch for global client defaults.
The close-to-open guarantee (G1) is independent of this cache timeout — open() revalidates.

## 4. NFSv3 procedure mapping

| Proc | Backend | Guest RPC | Notes |
|---|---|---|---|
| NULL | — | no | health probe |
| GETATTR | replica | no | tombstone ⇒ ESTALE after linger |
| SETATTR | optimistic + `SetAttr` | yes | guard ctime checked against replica |
| LOOKUP | replica `by_name`, then `norm` index | no | miss ⇒ ENOENT from replica (authoritative); PENDING node ⇒ resolve via `Lookup` RPC once |
| ACCESS | replica perms × mapped identity | no | algorithm below §4.2 |
| READLINK | replica | no | |
| READ | client cache → CAS → `Read` RPC | cold only | readahead engine `04` §5.2 |
| WRITE | optimistic + WriteStart/End | yes | UNSTABLE honored; FILE_SYNC ⇒ +Commit |
| CREATE/MKDIR/SYMLINK/MKNOD | optimistic + `Create` | yes | exclusive CREATE: verifier stored in replica dirty-state (standard verf-in-mtime trick avoided — we have real state) |
| REMOVE/RMDIR | optimistic + `Unlink/Rmdir` | yes | side-store names intercepted before this (§6) |
| RENAME | optimistic + `Rename` | yes | atomic in replica (single applier op) |
| LINK | optimistic + `Link` | yes | |
| READDIR | replica cookies | no | |
| READDIRPLUS | replica cookies + attrs + fh per entry | no | **the hot path**; pages ≤ `dircount/maxcount` honoring client sizes; cookieverf = dirgen at page 0, checked on continuation: changed ⇒ retry-from-cookie if cookie still exists else BAD_COOKIE |
| FSSTAT/FSINFO/PATHCONF | cached guest statfs (5 s lazy) / static | no | rtmax=wtmax=1 MiB, name_max 255, case-sensitive=true, no_trunc |
| COMMIT | `Commit` RPC | yes | writeverf = hostd boot nonce ⇒ client replays unstable writes after our crash (correct semantics for free) |

### 4.2 ACCESS evaluation

Identity `squash` (default): requester is presented as guest uid/gid from share config; evaluate
classic Unix bits against replica attrs (owner/group/other walk, CAP-less). `passthrough`: map
the AUTH_SYS uid through an optional `[idmap]` table. Errors mirror what the guest applier would
hit, so optimistic-apply rejections and guest rejections agree (modulo ACLs — non-goal v1,
`noacl` semantics declared).

### 4.3 errno → NFS status

Verbatim table (EPERM→NFS3ERR_PERM, ENOENT→NOENT, EACCES→ACCES, EEXIST→EXIST, ENOTDIR→NOTDIR,
EISDIR→ISDIR, EINVAL→INVAL, EFBIG→FBIG, ENOSPC→NOSPC, EROFS→ROFS, EMLINK→MLINK,
ENAMETOOLONG→NAMETOOLONG, ENOTEMPTY→NOTEMPTY, EDQUOT→DQUOT, ESTALE→STALE; anything else →
NFS3ERR_IO + log). Session-degraded mutations: NFS3ERR_JUKEBOX (client retries) until the `06` §7
deadline, then EIO.

## 5. NFSv4.1 — the coherence upgrade

Scope: minimal-but-correct single-client v4.1 server. Op coverage: SEQUENCE, EXCHANGE_ID,
CREATE_SESSION, DESTROY_SESSION/CLIENTID, RECLAIM_COMPLETE, SECINFO_NO_NAME, PUTROOTFH/PUTFH/
GETFH/SAVEFH/RESTOREFH, LOOKUP/LOOKUPP, GETATTR/SETATTR, ACCESS, OPEN/CLOSE/OPEN_DOWNGRADE,
READ/WRITE/COMMIT, CREATE/REMOVE/RENAME/LINK/READLINK, READDIR, DELEGRETURN, TEST_STATEID/
FREE_STATEID, and the backchannel ops CB_SEQUENCE/CB_RECALL. Byte-range LOCK ops are not
advertised; mounts use `nolocks` semantics as with v3.

- **State model**: one client (loopback), table-driven: clientid → sessions → slot tables
  (fore 64 slots, back 16 [E]); open-owners → stateids; delegation table keyed by NodeKey.
  Grace period: trivially skipped via immediate RECLAIM_COMPLETE handling (single client, no
  reboot reclaim semantics needed — server restart invalidates state; client recovers; writes
  replay via COMMIT verifier same as v3).
- **Backchannel**: v4.1 binds it on the fore connection (no client-side listener — the v4.0
  callback-port mess is why we skip 4.0 entirely).
- **Delegation policy**: grant OPEN4_DELEGATE_READ on every read-open while `deleg_count <
  cap (16384 [E])` and node not DIRTY/SUSPECT and share LIVE. Write delegations are not granted;
  the benefit is unproven for this workload.
- **Journal-driven recall** (the novel mechanism): `inval(node, ContentChanged|AttrChanged|
  unlink)` → if delegation held → CB_RECALL (dedup per node; batch storms by 5 ms window);
  DELEGRETURN clears; no return in 100 ms ⇒ retry; 2 s ⇒ revoke (SEQ flag set, client copes).
  Echo fences (`02` §6.5) ensure a Mac client writing through us never gets recalled by its own
  write's journal echo.
- **Why it matters**: with a read delegation held, the macOS client *stops revalidating* on open
  — open/read/close storms on the hot working set run with **zero** loopback RPCs and exact
  freshness (we recall within ms of a real guest change). attr-cache timeouts stop being the
  staleness bound for delegated files; READDIRPLUS continues to cover enumeration.
- Default-flip criteria (`surface = nfs41` becomes default): backchannel verified on macOS 26.x +
  recall p99 ≤ 10 ms + no client regressions through pjdfstest/fsx/soak.
- v4.1 xattr ops (RFC 8276): optional, replaces `._*` AppleDouble path if the macOS client uses
  them; otherwise the side-store continues to handle it.

## 6. macOS client quirk catalog & mitigations

| Quirk | Mitigation |
|---|---|
| Finder writes `.DS_Store` everywhere | side-store virtualization (default `virtualize`); `deny` profile returns EACCES (Finder tolerates, complains on copies); `allow` passes through to guest |
| AppleDouble `._*` for xattrs/rsrc forks on v3 | side-store rows keyed to anchor node; never forwarded; GC with anchor (`04` §7) |
| Spotlight tries to index network mounts (varies) | post-mount `mdutil -i off <mnt>` best-effort + `.metadata_never_index` synthesized at root via side-store; doctor asserts |
| `.fseventsd` probe | synthesize `no_log` marker dir in side-store |
| Trash: Finder deletes on network vols are immediate when `.Trashes` absent | keep absent (document); no fake trash |
| Negative name caching can hide fast guest-side creates | acceptable within close-to-open (lookup storm cost favors keeping it); `fresh` profile shortens; v4.1 dir delegations are not relied on |
| `nfsiod` async thread limits | `nfs.client.nfsiod_thread_max` documented in tuning page; defaults fine for loopback |
| Hard-mount hangs if hostd dies | `deadtimeout=60` + launchd `KeepAlive` (restart ≤1 s, well inside) + crash-only design; `soft` documented but not default (EIO surprises editors) |
| "Server connections interrupted" dialogs (EdenFS pain) | never-block rule: NFS handlers always answer (JUKEBOX-delay over stall); watchdogged applier |
| Unicode: Finder sends NFD, Linux stores NFC (or bytes) | `norm` index: exact match first, NFC-casefold map second; replies always return stored bytes; collision (two names same normalization) ⇒ exact-only for that pair + log |
| Case sensitivity | server declares case-sensitive; Finder handles it; casefold ext4 dirs out of scope v1 |
| `O_EXCL` create verifier games on v3 | real state in DirtyState (no mtime-stuffing) |
| Locks: NLM sidecar protocol | not implemented; `nolocks,locallocks` → local-only locking on the Mac; cross-boundary locking documented unsupported v1 (v4.1 LOCK phase 2) |
| **No FSEvents on NFS mounts** → VS Code/Zed watchers fall back to polling | (a) docs: workspace `files.watcherExclude` tuning; (b) `mist events --follow --json [path]` exposes the journal as a change feed; (c) optional editor extension can consume it because the journal is *better* than FSEvents; (d) FSKit front may restore real FSEvents if enabled |
| TextEdit/Xcode atomic-save rename dances | plain rename support suffices; safe-save creates temp + rename — both are real ops passed through; guest sees one atomic rename |

## 7. Identity mapping summary

| Mode | Mac→guest writes | Guest attrs → Mac | ACCESS |
|---|---|---|---|
| `squash` (default) | applier setfsuid to share's `apply_uid/gid` | uid/gid rewritten to the Mac user's uid/gid in every Attr returned (files all "yours") | evaluated as `apply_uid` against real guest perms |
| `passthrough` | AUTH_SYS uid mapped via `[idmap]` (default identity) | raw guest uid/gid | evaluated as mapped uid |

Squash keeps `git`/editors on the Mac happy (no foreign-uid permission surprises) while the guest
tree stays owned by the guest user. Mode is per share, fixed at mount time.
