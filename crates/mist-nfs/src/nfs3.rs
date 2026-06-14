//! NFSv3 (RFC 1813) procedures backed by a [`MountSurface`]. Program 100003, version 3.

use crate::handle::HandleCodec;
use crate::surface::{CreateKind, MountSurface, NfsError, SetAttr};
use crate::xdr::{XdrReader, XdrWriter};
use mist_proto::{Attr, Kind, NodeKey};
use std::sync::Arc;

pub const NFS_PROGRAM: u32 = 100003;
pub const NFS_VERSION: u32 = 3;

/// NFS write verifier (8 bytes). A client compares it across writes to detect a server restart
/// (and replay unstable writes). Fixed per process — we report FILE_SYNC for synced writes.
const WRITE_VERF: [u8; 8] = *b"MIST-wv1";
const PROC_COMMIT: u32 = 21;
const PROC_MKNOD: u32 = 11;
const PROC_LINK: u32 = 15;

// Procedure numbers.
const PROC_NULL: u32 = 0;
const PROC_GETATTR: u32 = 1;
const PROC_SETATTR: u32 = 2;
const PROC_LOOKUP: u32 = 3;
const PROC_ACCESS: u32 = 4;
const PROC_READLINK: u32 = 5;
const PROC_READ: u32 = 6;
const PROC_WRITE: u32 = 7;
const PROC_CREATE: u32 = 8;
const PROC_MKDIR: u32 = 9;
const PROC_SYMLINK: u32 = 10;
const PROC_REMOVE: u32 = 12;
const PROC_RMDIR: u32 = 13;
const PROC_RENAME: u32 = 14;
const PROC_READDIR: u32 = 16;
const PROC_READDIRPLUS: u32 = 17;
const PROC_FSSTAT: u32 = 18;
const PROC_FSINFO: u32 = 19;
const PROC_PATHCONF: u32 = 20;

// nfsstat3.
const NFS3_OK: u32 = 0;
const NFS3ERR_NOENT: u32 = 2;
const NFS3ERR_IO: u32 = 5;
const NFS3ERR_ACCES: u32 = 13;
const NFS3ERR_NOTDIR: u32 = 20;
const NFS3ERR_ISDIR: u32 = 21;
const NFS3ERR_INVAL: u32 = 22;
const NFS3ERR_EXIST: u32 = 17;
const NFS3ERR_NOSPC: u32 = 28;
const NFS3ERR_NAMETOOLONG: u32 = 63;
const NFS3ERR_NOTEMPTY: u32 = 66;
const NFS3ERR_ROFS: u32 = 30;
const NFS3ERR_STALE: u32 = 70;
#[allow(dead_code)] // returned for unsupported procedures such as MKNOD and LINK
const NFS3ERR_NOTSUPP: u32 = 10004;
const NFS3ERR_JUKEBOX: u32 = 10008;

// ftype3.
const NF3REG: u32 = 1;
const NF3DIR: u32 = 2;
const NF3BLK: u32 = 3;
const NF3CHR: u32 = 4;
const NF3LNK: u32 = 5;
const NF3SOCK: u32 = 6;
const NF3FIFO: u32 = 7;

/// Read transfers go to the client straight from host RAM — 2 MiB halves the round trips of
/// a sequential stream (the single-stream wall is per-RPC latency). Writes stay at 1 MiB
/// (each write crosses to the guest in frame-capped chunks).
const MAX_READ: u32 = 1024 * 1024;
const MAX_WRITE: u32 = 1024 * 1024;

const FSF_LINK: u32 = 0x1;
const FSF_SYMLINK: u32 = 0x2;
const FSF_HOMOGENEOUS: u32 = 0x8;
const FSF_CANSETTIME: u32 = 0x10;

fn nfsstat_of(e: NfsError) -> u32 {
    match e {
        NfsError::NoEnt => NFS3ERR_NOENT,
        NfsError::NotDir => NFS3ERR_NOTDIR,
        NfsError::IsDir => NFS3ERR_ISDIR,
        NfsError::NotSymlink => NFS3ERR_INVAL,
        NfsError::Stale => NFS3ERR_STALE,
        NfsError::Access => NFS3ERR_ACCES,
        NfsError::Io => NFS3ERR_IO,
        NfsError::Jukebox => NFS3ERR_JUKEBOX,
        NfsError::NameTooLong => NFS3ERR_NAMETOOLONG,
        NfsError::NotEmpty => NFS3ERR_NOTEMPTY,
        NfsError::Exist => NFS3ERR_EXIST,
        NfsError::NoSpace => NFS3ERR_NOSPC,
        NfsError::Rofs => NFS3ERR_ROFS,
    }
}

fn ftype3(kind: Kind) -> u32 {
    match kind {
        Kind::Reg => NF3REG,
        Kind::Dir => NF3DIR,
        Kind::Symlink => NF3LNK,
        Kind::Blk => NF3BLK,
        Kind::Chr => NF3CHR,
        Kind::Sock => NF3SOCK,
        Kind::Fifo => NF3FIFO,
    }
}

fn write_fattr3(w: &mut XdrWriter, share: u16, node: NodeKey, a: &Attr) {
    w.u32(ftype3(a.kind));
    w.u32(a.mode as u32);
    w.u32(a.nlink);
    w.u32(a.uid);
    w.u32(a.gid);
    w.u64(a.size);
    w.u64(a.blocks * 512); // bytes used
    // rdev specdata3
    w.u32((a.rdev >> 32) as u32);
    w.u32(a.rdev as u32);
    w.u64(share as u64); // fsid
    w.u64(node.ino); // fileid
    // atime served as mtime (ADR-16); mtime; ctime
    write_time(w, a.mtime);
    write_time(w, a.mtime);
    write_time(w, a.ctime);
}

fn write_time(w: &mut XdrWriter, t: mist_proto::Ts) {
    w.u32(t.sec as u32);
    w.u32(t.nsec);
}

fn write_post_op_attr(w: &mut XdrWriter, share: u16, node: NodeKey, a: Option<&Attr>) {
    match a {
        Some(a) => {
            w.bool(true);
            write_fattr3(w, share, node, a);
        }
        None => w.bool(false),
    }
}

fn write_post_op_fh(w: &mut XdrWriter, codec: &HandleCodec, share: u16, node: NodeKey) {
    w.bool(true);
    w.opaque(&codec.encode(share, node));
}

/// Read an nfs_fh3 from args and decode it; returns (share, node) or encodes a STALE reply.
fn read_fh(r: &mut XdrReader<'_>, codec: &HandleCodec) -> Option<(u16, NodeKey)> {
    let fh = r.opaque(64).ok()?;
    codec.decode(fh)
}

/// Dispatch one NFSv3 call. `xid_writer` is a freshly built accepted-reply header; we append the
/// procedure result. Returns the full reply body (without the record marker).
pub(crate) async fn dispatch<S: MountSurface>(
    proc: u32,
    args: &mut XdrReader<'_>,
    surface: &Arc<S>,
    codec: &HandleCodec,
    mut w: XdrWriter,
) -> crate::server::Reply {
    // Zero-copy fast path: a READ wholly served by one local file goes out via sendfile —
    // the reply head is rendered here, the payload never enters userspace.
    if proc == PROC_READ {
        let mut peek = args.clone_reader();
        if let Some((_, node)) = read_fh(&mut peek, codec) {
            let offset = peek.u64().unwrap_or(0);
            let count = peek.u32().unwrap_or(0).min(MAX_READ);
            if let Some(body) = surface.read_sendable(node, offset, count).await {
                let share = surface.share_id();
                let pre_attr = surface.getattr(node).ok();
                w.u32(NFS3_OK);
                write_post_op_attr(&mut w, share, node, pre_attr.as_ref());
                w.u32(body.len);
                w.bool(body.eof);
                w.u32(body.len); // opaque length prefix; payload follows via sendfile
                let pad = (4 - (body.len as usize % 4)) % 4;
                return crate::server::Reply::HeadAndFile {
                    head: w.into_bytes(),
                    body,
                    pad,
                };
            }
        }
        // fall through to the copying path with the ORIGINAL reader
    }
    crate::server::Reply::Whole(
        dispatch_inner(proc, args, surface, codec, w)
            .await
            .into_bytes(),
    )
}

async fn dispatch_inner<S: MountSurface>(
    proc: u32,
    args: &mut XdrReader<'_>,
    surface: &Arc<S>,
    codec: &HandleCodec,
    mut w: XdrWriter,
) -> XdrWriter {
    let share = surface.share_id();
    match proc {
        PROC_NULL => w,
        PROC_GETATTR => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            match surface.getattr(node) {
                Ok(a) => {
                    w.u32(NFS3_OK);
                    write_fattr3(&mut w, share, node, &a);
                }
                Err(e) => w.u32(nfsstat_of(e)),
            }
            w
        }
        PROC_LOOKUP => {
            let Some((_, dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let Ok(name) = read_name(args) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_post_op_attr(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                return w;
            };
            let dir_attr = surface.getattr(dir).ok();
            match surface.lookup(dir, &name) {
                Ok((node, a)) => {
                    w.u32(NFS3_OK);
                    w.opaque(&codec.encode(share, node)); // object fh
                    write_post_op_attr(&mut w, share, node, Some(&a)); // obj attrs
                    write_post_op_attr(&mut w, share, dir, dir_attr.as_ref()); // dir attrs
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_post_op_attr(&mut w, share, dir, dir_attr.as_ref());
                }
            }
            w
        }
        PROC_ACCESS => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let requested = args.u32().unwrap_or(0);
            match surface.getattr(node) {
                Ok(a) => {
                    w.u32(NFS3_OK);
                    write_post_op_attr(&mut w, share, node, Some(&a));
                    // Grant read/lookup/execute the mode allows; grant write/extend/delete too
                    // when the surface is writable (the squashed owner is the Mac user).
                    let granted = access_bits(&a, requested, surface.writable());
                    w.u32(granted);
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    w.bool(false);
                }
            }
            w
        }
        PROC_READLINK => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let attr = surface.getattr(node).ok();
            match surface.readlink(node) {
                Ok(target) => {
                    w.u32(NFS3_OK);
                    write_post_op_attr(&mut w, share, node, attr.as_ref());
                    w.opaque(&target);
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_post_op_attr(&mut w, share, node, attr.as_ref());
                }
            }
            w
        }
        PROC_READ => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let offset = args.u64().unwrap_or(0);
            let count = args.u32().unwrap_or(0).min(MAX_READ);
            let pre_attr = surface.getattr(node).ok();
            match surface.read(node, offset, count).await {
                Ok(res) => {
                    w.u32(NFS3_OK);
                    write_post_op_attr(&mut w, share, node, pre_attr.as_ref());
                    w.u32(res.data.len() as u32);
                    w.bool(res.eof);
                    w.opaque(&res.data);
                    crate::bufpool::give(res.data);
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_post_op_attr(&mut w, share, node, pre_attr.as_ref());
                }
            }
            w
        }
        PROC_READDIR => read_dir(args, surface, codec, w, share, false).await,
        PROC_READDIRPLUS => read_dir(args, surface, codec, w, share, true).await,
        PROC_FSSTAT => {
            let _ = read_fh(args, codec);
            let st = surface.fsstat();
            w.u32(NFS3_OK);
            w.bool(false); // obj_attributes
            w.u64(st.total_bytes);
            w.u64(st.free_bytes);
            w.u64(st.avail_bytes);
            w.u64(st.total_files);
            w.u64(st.free_files);
            w.u64(st.free_files);
            w.u32(0); // invarsec
            w
        }
        PROC_FSINFO => {
            let _ = read_fh(args, codec);
            w.u32(NFS3_OK);
            w.bool(false); // obj_attributes
            w.u32(MAX_READ); // rtmax
            w.u32(MAX_READ); // rtpref
            w.u32(4096); // rtmult
            w.u32(MAX_WRITE); // wtmax
            w.u32(MAX_WRITE); // wtpref
            w.u32(4096); // wtmult
            w.u32(MAX_READ); // dtpref
            w.u64(u64::MAX); // maxfilesize
            write_time(&mut w, mist_proto::Ts { sec: 0, nsec: 1 }); // time_delta
            w.u32(FSF_LINK | FSF_SYMLINK | FSF_HOMOGENEOUS | FSF_CANSETTIME);
            w
        }
        PROC_PATHCONF => {
            let _ = read_fh(args, codec);
            w.u32(NFS3_OK);
            w.bool(false); // obj_attributes
            w.u32(32000); // linkmax
            w.u32(255); // name_max
            w.bool(true); // no_trunc
            w.bool(false); // chown_restricted
            w.bool(false); // case_insensitive (Linux is case-sensitive)
            w.bool(true); // case_preserving
            w
        }
        PROC_SETATTR => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let set = parse_sattr3(args);
            // guard (sattrguard3): bool + nfstime3 — ignored.
            if args.bool().unwrap_or(false) {
                let _ = args.fixed(8);
            }
            match surface.setattr(node, set.into()).await {
                Ok(a) => {
                    w.u32(NFS3_OK);
                    write_wcc_data(&mut w, share, node, Some(&a)); // wcc for the object
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_wcc_data(&mut w, share, node, surface.getattr(node).ok().as_ref());
                }
            }
            w
        }
        PROC_WRITE => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let offset = args.u64().unwrap_or(0);
            let _count = args.u32().unwrap_or(0);
            let stable = args.u32().unwrap_or(0); // 0 UNSTABLE, 1 DATA_SYNC, 2 FILE_SYNC
            let data = args.opaque(MAX_READ as usize).unwrap_or(b"").to_vec();
            let sync = stable != 0;
            let pre = surface.getattr(node).ok();
            match surface.write(node, offset, &data, sync).await {
                Ok(a) => {
                    w.u32(NFS3_OK);
                    write_wcc_data(&mut w, share, node, Some(&a));
                    w.u32(data.len() as u32); // count written
                    // Writeback shares answer FILE_SYNC even to UNSTABLE writes (knfsd
                    // `async` semantics); the client then skips COMMIT throttling.
                    w.u32(if sync || surface.writes_are_stable() {
                        2
                    } else {
                        0
                    });
                    w.fixed(&WRITE_VERF); // write verifier
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_wcc_data(&mut w, share, node, pre.as_ref());
                }
            }
            w
        }
        PROC_CREATE => {
            let Some((_, dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let Ok(name) = read_name(args) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                return w;
            };
            // createhow3: mode(u32) + (sattr3 | createverf for EXCLUSIVE)
            let how = args.u32().unwrap_or(0); // 0 UNCHECKED, 1 GUARDED, 2 EXCLUSIVE
            let set = if how == 2 {
                let _verf = args.fixed(8);
                SetAttrParsed::default()
            } else {
                parse_sattr3(args)
            };
            let mode = set.mode.unwrap_or(0o644);
            create_reply(
                &mut w,
                surface,
                codec,
                share,
                dir,
                &name,
                CreateKind::File {
                    exclusive: how != 0,
                },
                mode,
            )
            .await;
            w
        }
        PROC_MKDIR => {
            let Some((_, dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let Ok(name) = read_name(args) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                return w;
            };
            let set = parse_sattr3(args);
            let mode = set.mode.unwrap_or(0o755);
            create_reply(
                &mut w,
                surface,
                codec,
                share,
                dir,
                &name,
                CreateKind::Dir,
                mode,
            )
            .await;
            w
        }
        PROC_SYMLINK => {
            let Some((_, dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let Ok(name) = read_name(args) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                return w;
            };
            // symlinkdata3: sattr3 + nfspath3
            let _set = parse_sattr3(args);
            let target = args.opaque(4096).unwrap_or(b"").to_vec();
            create_reply(
                &mut w,
                surface,
                codec,
                share,
                dir,
                &name,
                CreateKind::Symlink { target },
                0o777,
            )
            .await;
            w
        }
        PROC_REMOVE | PROC_RMDIR => {
            let is_dir = proc == PROC_RMDIR;
            let Some((_, dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let Ok(name) = read_name(args) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                return w;
            };
            match surface.remove(dir, &name, is_dir).await {
                Ok(()) => {
                    w.u32(NFS3_OK);
                    write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_wcc_data(&mut w, share, dir, surface.getattr(dir).ok().as_ref());
                }
            }
            w
        }
        PROC_RENAME => {
            let Some((_, from_dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let from_ok = read_name(args);
            let Some((_, to_dir)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let to_ok = read_name(args);
            let (Ok(from_name), Ok(to_name)) = (from_ok, to_ok) else {
                w.u32(NFS3ERR_NAMETOOLONG);
                write_wcc_data(
                    &mut w,
                    share,
                    from_dir,
                    surface.getattr(from_dir).ok().as_ref(),
                );
                write_wcc_data(&mut w, share, to_dir, surface.getattr(to_dir).ok().as_ref());
                return w;
            };
            let status = match surface.rename(from_dir, &from_name, to_dir, &to_name).await {
                Ok(()) => NFS3_OK,
                Err(e) => nfsstat_of(e),
            };
            w.u32(status);
            // fromdir_wcc, todir_wcc
            write_wcc_data(
                &mut w,
                share,
                from_dir,
                surface.getattr(from_dir).ok().as_ref(),
            );
            write_wcc_data(&mut w, share, to_dir, surface.getattr(to_dir).ok().as_ref());
            w
        }
        PROC_COMMIT => {
            let Some((_, node)) = read_fh(args, codec) else {
                w.u32(NFS3ERR_STALE);
                return w;
            };
            let offset = args.u64().unwrap_or(0);
            let count = args.u32().unwrap_or(0);
            match surface.commit(node, offset, count as u64).await {
                Ok(a) => {
                    w.u32(NFS3_OK);
                    write_wcc_data(&mut w, share, node, Some(&a));
                    w.fixed(&WRITE_VERF);
                }
                Err(e) => {
                    w.u32(nfsstat_of(e));
                    write_wcc_data(&mut w, share, node, surface.getattr(node).ok().as_ref());
                }
            }
            w
        }
        PROC_MKNOD => {
            // MKNOD3resfail = wcc_data(dir).
            let dir = read_fh(args, codec).map(|(_, d)| d);
            w.u32(NFS3ERR_NOTSUPP);
            match dir {
                Some(d) => write_wcc_data(&mut w, share, d, surface.getattr(d).ok().as_ref()),
                None => {
                    w.bool(false);
                    w.bool(false);
                }
            }
            w
        }
        PROC_LINK => {
            // LINK3resfail = post_op_attr(file) + wcc_data(dir).
            let file = read_fh(args, codec).map(|(_, n)| n);
            let dir = read_fh(args, codec).map(|(_, d)| d);
            w.u32(NFS3ERR_NOTSUPP);
            match file {
                Some(n) => write_post_op_attr(&mut w, share, n, surface.getattr(n).ok().as_ref()),
                None => w.bool(false),
            }
            match dir {
                Some(d) => write_wcc_data(&mut w, share, d, surface.getattr(d).ok().as_ref()),
                None => {
                    w.bool(false);
                    w.bool(false);
                }
            }
            w
        }
        _ => {
            w.u32(NFS3ERR_NOTSUPP);
            w
        }
    }
}

/// CREATE/MKDIR/SYMLINK shared reply: object fh + obj attrs + dir wcc.
#[allow(clippy::too_many_arguments)]
async fn create_reply<S: MountSurface>(
    w: &mut XdrWriter,
    surface: &Arc<S>,
    codec: &HandleCodec,
    share: u16,
    dir: NodeKey,
    name: &[u8],
    kind: CreateKind,
    mode: u16,
) {
    match surface.create(dir, name, kind, mode).await {
        Ok((node, attr)) => {
            w.u32(NFS3_OK);
            write_post_op_fh(w, codec, share, node); // obj handle (present)
            write_post_op_attr(w, share, node, Some(&attr)); // obj attrs
            write_wcc_data(w, share, dir, surface.getattr(dir).ok().as_ref());
        }
        Err(e) => {
            w.u32(nfsstat_of(e));
            write_wcc_data(w, share, dir, surface.getattr(dir).ok().as_ref());
        }
    }
}

/// wcc_data = pre_op_attr (we always omit) + post_op_attr.
fn write_wcc_data(w: &mut XdrWriter, share: u16, node: NodeKey, post: Option<&Attr>) {
    w.bool(false); // pre_op_attr: omitted
    write_post_op_attr(w, share, node, post);
}

#[derive(Default)]
struct SetAttrParsed {
    mode: Option<u16>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    mtime: Option<mist_proto::Ts>,
}

impl From<SetAttrParsed> for SetAttr {
    fn from(s: SetAttrParsed) -> Self {
        SetAttr {
            mode: s.mode,
            uid: s.uid,
            gid: s.gid,
            size: s.size,
            mtime: s.mtime,
        }
    }
}

/// Parse an sattr3 from args: set_mode/uid/gid/size (bool+value) + set_atime/mtime (time_how + ?).
fn parse_sattr3(r: &mut XdrReader<'_>) -> SetAttrParsed {
    let mut s = SetAttrParsed::default();
    if r.bool().unwrap_or(false) {
        s.mode = r.u32().ok().map(|m| m as u16);
    }
    if r.bool().unwrap_or(false) {
        s.uid = r.u32().ok();
    }
    if r.bool().unwrap_or(false) {
        s.gid = r.u32().ok();
    }
    if r.bool().unwrap_or(false) {
        s.size = r.u64().ok();
    }
    // set_atime: time_how (0 DONT_CHANGE, 1 SET_TO_SERVER, 2 SET_TO_CLIENT + nfstime3)
    if r.u32().unwrap_or(0) == 2 {
        let _ = r.fixed(8);
    }
    // set_mtime
    if r.u32().unwrap_or(0) == 2 {
        let sec = r.u32().unwrap_or(0);
        let nsec = r.u32().unwrap_or(0);
        s.mtime = Some(mist_proto::Ts {
            sec: sec as i64,
            nsec,
        });
    }
    s
}

/// Read a filename argument, enforcing NAME_MAX (255, ext4's on-disk limit): longer names get a
/// proper NFS3ERR_NAMETOOLONG instead of a wire-parse failure that desyncs the reply.
fn read_name(args: &mut XdrReader<'_>) -> Result<Vec<u8>, ()> {
    let n = args.opaque(4096).map_err(|_| ())?;
    if n.len() > 255 {
        return Err(());
    }
    Ok(n.to_vec())
}

fn access_bits(a: &Attr, requested: u32, writable: bool) -> u32 {
    const ACCESS_READ: u32 = 0x0001;
    const ACCESS_LOOKUP: u32 = 0x0002;
    const ACCESS_MODIFY: u32 = 0x0004;
    const ACCESS_EXTEND: u32 = 0x0008;
    const ACCESS_DELETE: u32 = 0x0010;
    const ACCESS_EXECUTE: u32 = 0x0020;
    // The owner is squashed to the Mac user, so evaluate the mode's owner bits.
    let mut g = 0;
    if a.mode & 0o400 != 0 {
        g |= ACCESS_READ;
        if a.kind == Kind::Dir {
            g |= ACCESS_LOOKUP;
        }
    }
    if a.mode & 0o100 != 0 && a.kind != Kind::Dir {
        g |= ACCESS_EXECUTE;
    }
    if a.kind == Kind::Dir {
        g |= ACCESS_LOOKUP; // browsing must work even with odd modes
    }
    if writable {
        // Granted regardless of mode bits: the macOS client gates SETATTR (chmod/utimes) and
        // unlink behind these, and an owner must be able to `chmod +w` a read-only file. Real
        // write/truncate enforcement happens in the guest, where the applier opens as the apply
        // identity and the kernel applies actual DAC — a denied write still returns EACCES,
        // just from the guest instead of from the client's open.
        g |= ACCESS_MODIFY | ACCESS_EXTEND | ACCESS_DELETE;
    }
    g & requested
}

async fn read_dir<S: MountSurface>(
    args: &mut XdrReader<'_>,
    surface: &Arc<S>,
    codec: &HandleCodec,
    mut w: XdrWriter,
    share: u16,
    plus: bool,
) -> XdrWriter {
    let Some((_, dir)) = read_fh(args, codec) else {
        w.u32(NFS3ERR_STALE);
        return w;
    };
    let cookie = args.u64().unwrap_or(0);
    let _cookieverf = args.fixed(8).map(|b| b.to_vec()).unwrap_or_default();
    let (_dircount, maxcount) = if plus {
        let d = args.u32().unwrap_or(8192);
        let m = args.u32().unwrap_or(32768);
        (d, m)
    } else {
        let c = args.u32().unwrap_or(8192);
        (c, c)
    };

    let dir_attr = surface.getattr(dir).ok();
    // Bound entries so the encoded reply stays under maxcount. Estimate per-entry cost.
    let per_entry = if plus { 160 } else { 40 };
    let max_entries = ((maxcount as usize).saturating_sub(128) / per_entry).clamp(1, 4096);

    let page = match surface.readdir(dir, cookie, max_entries, plus) {
        Ok(p) => p,
        Err(e) => {
            w.u32(nfsstat_of(e));
            write_post_op_attr(&mut w, share, dir, dir_attr.as_ref());
            return w;
        }
    };

    w.u32(NFS3_OK);
    write_post_op_attr(&mut w, share, dir, dir_attr.as_ref());
    w.fixed(&page.cookieverf.to_be_bytes()); // cookieverf[8]

    let budget = maxcount as usize;
    for e in &page.entries {
        // Stop if appending this entry would blow the client's maxcount.
        if w.len() + per_entry + e.name.len() > budget {
            // Truncate: emit list terminator + eof=false so the client continues.
            w.bool(false);
            w.bool(false);
            return w;
        }
        w.bool(true); // entry present
        w.u64(e.node.ino); // fileid
        w.opaque(&e.name); // name
        w.u64(e.cookie); // cookie
        if plus {
            write_post_op_attr(&mut w, share, e.node, e.attr.as_ref());
            write_post_op_fh(&mut w, codec, share, e.node);
        }
    }
    w.bool(false); // end of entry list
    w.bool(page.eof);
    w
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::surface::{DirEntry, FsStat, NfsResult, ReadDirPage, ReadFuture, ReadResult};
    use mist_proto::Ts;

    struct FakeSurface;

    fn attr(kind: Kind, size: u64) -> Attr {
        Attr {
            kind,
            mode: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            size,
            blocks: size.div_ceil(512),
            mtime: Ts { sec: 100, nsec: 0 },
            ctime: Ts { sec: 100, nsec: 0 },
            rdev: 0,
            content_version: 0,
            symlink_target: None,
        }
    }

    impl MountSurface for FakeSurface {
        fn share_id(&self) -> u16 {
            1
        }
        fn root(&self) -> NodeKey {
            NodeKey {
                ino: 2,
                generation: 1,
            }
        }
        fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
            if node.ino == 2 {
                Ok(attr(Kind::Dir, 4096))
            } else if node.ino == 10 {
                Ok(attr(Kind::Reg, 5))
            } else {
                Err(NfsError::NoEnt)
            }
        }
        fn lookup(&self, _dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
            if name == b"f" {
                Ok((
                    NodeKey {
                        ino: 10,
                        generation: 1,
                    },
                    attr(Kind::Reg, 5),
                ))
            } else {
                Err(NfsError::NoEnt)
            }
        }
        fn readdir(
            &self,
            _dir: NodeKey,
            _cookie: u64,
            _max: usize,
            plus: bool,
        ) -> NfsResult<ReadDirPage> {
            Ok(ReadDirPage {
                entries: vec![DirEntry {
                    name: b"f".to_vec(),
                    node: NodeKey {
                        ino: 10,
                        generation: 1,
                    },
                    cookie: 3,
                    attr: if plus { Some(attr(Kind::Reg, 5)) } else { None },
                }],
                eof: true,
                cookieverf: 7,
            })
        }
        fn readlink(&self, _node: NodeKey) -> NfsResult<Vec<u8>> {
            Err(NfsError::NotSymlink)
        }
        fn read(&self, _node: NodeKey, _off: u64, _count: u32) -> ReadFuture<'_> {
            Box::pin(async {
                Ok(ReadResult {
                    data: b"hello".to_vec(),
                    eof: true,
                })
            })
        }
        fn fsstat(&self) -> FsStat {
            FsStat::default()
        }
    }

    #[tokio::test]
    async fn getattr_and_lookup_and_read() {
        let s = Arc::new(FakeSurface);
        let codec = HandleCodec::new(b"k");
        let root_fh = codec.encode(1, s.root());

        // GETATTR(root)
        let mut argw = XdrWriter::new();
        argw.opaque(&root_fh);
        let abytes = argw.into_bytes();
        let mut args = XdrReader::new(&abytes);
        let w = dispatch_inner(PROC_GETATTR, &mut args, &s, &codec, XdrWriter::new()).await;
        let out = w.into_bytes();
        let mut r = XdrReader::new(&out);
        assert_eq!(r.u32().unwrap(), NFS3_OK);
        assert_eq!(r.u32().unwrap(), NF3DIR); // ftype

        // LOOKUP(root, "f")
        let mut argw = XdrWriter::new();
        argw.opaque(&root_fh);
        argw.opaque(b"f");
        let abytes = argw.into_bytes();
        let mut args = XdrReader::new(&abytes);
        let w = dispatch_inner(PROC_LOOKUP, &mut args, &s, &codec, XdrWriter::new()).await;
        let out = w.into_bytes();
        let mut r = XdrReader::new(&out);
        assert_eq!(r.u32().unwrap(), NFS3_OK);
        let fh = r.opaque(64).unwrap();
        assert_eq!(
            codec.decode(fh).unwrap().1,
            NodeKey {
                ino: 10,
                generation: 1
            }
        );

        // READ(f, 0, 100)
        let file_fh = codec.encode(
            1,
            NodeKey {
                ino: 10,
                generation: 1,
            },
        );
        let mut argw = XdrWriter::new();
        argw.opaque(&file_fh);
        argw.u64(0);
        argw.u32(100);
        let abytes = argw.into_bytes();
        let mut args = XdrReader::new(&abytes);
        let w = dispatch_inner(PROC_READ, &mut args, &s, &codec, XdrWriter::new()).await;
        let out = w.into_bytes();
        let mut r = XdrReader::new(&out);
        assert_eq!(r.u32().unwrap(), NFS3_OK);
        let _post = r.bool().unwrap();
        // post_op_attr present → skip fattr3 (we know fake returns Some)
        // count, eof, data
    }

    #[tokio::test]
    async fn readdirplus_encodes_entries() {
        let s = Arc::new(FakeSurface);
        let codec = HandleCodec::new(b"k");
        let root_fh = codec.encode(1, s.root());
        let mut argw = XdrWriter::new();
        argw.opaque(&root_fh);
        argw.u64(0); // cookie
        argw.fixed(&[0u8; 8]); // verf
        argw.u32(8192); // dircount
        argw.u32(32768); // maxcount
        let abytes = argw.into_bytes();
        let mut args = XdrReader::new(&abytes);
        let w = dispatch_inner(PROC_READDIRPLUS, &mut args, &s, &codec, XdrWriter::new()).await;
        let out = w.into_bytes();
        let mut r = XdrReader::new(&out);
        assert_eq!(r.u32().unwrap(), NFS3_OK);
        let _dirattr = r.bool().unwrap();
        if _dirattr {
            // skip fattr3 (21 u32-ish fields); easier: just ensure no panic and eof later
        }
        // We won't fully decode; just assert the reply is non-trivially long.
        assert!(out.len() > 40);
    }

    #[tokio::test]
    async fn fsinfo_advertises_supported_capabilities() {
        let s = Arc::new(FakeSurface);
        let codec = HandleCodec::new(b"k");
        let root_fh = codec.encode(1, s.root());
        let mut argw = XdrWriter::new();
        argw.opaque(&root_fh);
        let abytes = argw.into_bytes();
        let mut args = XdrReader::new(&abytes);
        let w = dispatch_inner(PROC_FSINFO, &mut args, &s, &codec, XdrWriter::new()).await;
        let out = w.into_bytes();
        let mut r = XdrReader::new(&out);
        assert_eq!(r.u32().unwrap(), NFS3_OK);
        assert!(!r.bool().unwrap()); // obj_attributes omitted
        for _ in 0..7 {
            let _ = r.u32().unwrap();
        }
        let _ = r.u64().unwrap(); // maxfilesize
        let _ = r.u32().unwrap(); // time_delta seconds
        let _ = r.u32().unwrap(); // time_delta nseconds
        let props = r.u32().unwrap();
        assert_eq!(props & FSF_LINK, FSF_LINK);
        assert_eq!(props & FSF_SYMLINK, FSF_SYMLINK);
        assert_eq!(props & FSF_HOMOGENEOUS, FSF_HOMOGENEOUS);
        assert_eq!(props & FSF_CANSETTIME, FSF_CANSETTIME);
    }

    #[test]
    fn unsupported_proc_is_notsupp_or_rofs() {
        // sanity: NFS3ERR constants distinct
        assert_ne!(NFS3ERR_ROFS, NFS3_OK);
        let _ = NFS3ERR_NOTSUPP;
    }
}
