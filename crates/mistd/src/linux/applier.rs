//! Write applier: executes Mac-originated mutations inside the guest, contained to the share and
//! squashed to the share's apply identity (design 03 §7). Every op is a single-component `*at`
//! call against the parent's directory fd, so path traversal is impossible by construction; the
//! `Name` grammar (decode-validated) plus `O_NOFOLLOW` close the symlink-component gap.
//!
//! `setfsuid`/`setfsgid` are per-thread on Linux; the RPC executor runs these on the blocking
//! pool, so each op sets the identity and restores it before returning (threads are reused).
#![allow(unsafe_code)]

use super::rpc::stat_attr;
use super::shares::{Share, attr_from_statx};
use mist_proto::{Attr, Kind, NodeKey, RpcErr, RpcResp, Ts};
use rustix::fs::{AtFlags, StatxFlags};
use std::ffi::CString;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

/// Open a node for I/O by handle, then reopen it through `/proc/self/fd` for the requested access.
///
/// `open_by_handle_at` with `O_WRONLY`/`O_RDWR` returns EPERM on ext4 even with
/// CAP_DAC_READ_SEARCH; only `O_PATH`/`O_RDONLY` opens are permitted by handle. So we take the
/// privileged `O_PATH` handle (as root, where CAP_DAC_READ_SEARCH applies) and reopen the magic
/// `/proc/self/fd/N` symlink as the apply identity — a normal path open whose DAC check the file's
/// owner (apply_uid) satisfies. `access` is `O_RDONLY`/`O_WRONLY`/`O_RDWR`.
fn reopen_for_io(share: &Share, node: NodeKey, access: libc::c_int) -> std::io::Result<OwnedFd> {
    let pathfd = share.open_node(node, libc::O_PATH | libc::O_NOFOLLOW)?; // root: CAP_DAC_READ_SEARCH
    let _id = IdentityGuard::enter(share); // reopen as the apply identity (owns the files)
    let proc = format!("/proc/self/fd/{}\0", pathfd.as_fd().as_raw_fd());
    // SAFETY: valid NUL-terminated path; access is a standard open mode.
    let fd = unsafe {
        libc::open(
            proc.as_ptr() as *const libc::c_char,
            access | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fd is a fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn err(e: &std::io::Error, what: &str) -> RpcResp {
    RpcResp::Err(RpcErr {
        errno: e.raw_os_error().unwrap_or(libc::EIO),
        msg: what.into(),
    })
}

fn rofs() -> RpcResp {
    RpcResp::Err(RpcErr {
        errno: libc::EROFS,
        msg: "read-only share".into(),
    })
}

/// Sets fsuid/fsgid to the share identity for the current thread; restores on drop.
struct IdentityGuard {
    prev_uid: u32,
    prev_gid: u32,
}

impl IdentityGuard {
    fn enter(share: &Share) -> Self {
        // setfsgid before setfsuid (dropping uid privilege first would block the gid change).
        // SAFETY: setfsgid/setfsuid are per-thread, always succeed, return the previous id.
        let prev_gid = unsafe { libc::setfsgid(share.apply_gid) };
        let prev_uid = unsafe { libc::setfsuid(share.apply_uid) };
        IdentityGuard {
            prev_uid: prev_uid as u32,
            prev_gid: prev_gid as u32,
        }
    }
}

impl Drop for IdentityGuard {
    fn drop(&mut self) {
        // SAFETY: restore the thread's previous fs identity.
        unsafe {
            libc::setfsuid(self.prev_uid);
            libc::setfsgid(self.prev_gid);
        }
    }
}

fn cname(name: &[u8]) -> std::io::Result<CString> {
    CString::new(name).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
}

fn st_mode_kind(stx: &rustix::fs::Statx) -> Kind {
    match stx.stx_mode as u32 & libc::S_IFMT {
        libc::S_IFDIR => Kind::Dir,
        libc::S_IFLNK => Kind::Symlink,
        libc::S_IFIFO => Kind::Fifo,
        libc::S_IFSOCK => Kind::Sock,
        libc::S_IFCHR => Kind::Chr,
        libc::S_IFBLK => Kind::Blk,
        _ => Kind::Reg,
    }
}

/// Open the parent directory by NodeKey for `*at` operations.
fn open_parent(share: &Share, dir: NodeKey) -> std::io::Result<OwnedFd> {
    share.open_node(dir, libc::O_DIRECTORY | libc::O_NOFOLLOW)
}

/// Resolve a just-created/renamed entry under `parent` to (NodeKey, Attr).
fn entry_node(
    _share: &Share,
    parent_fd: &OwnedFd,
    name: &[u8],
) -> std::io::Result<(NodeKey, Attr)> {
    let c = cname(name)?;
    let stx = rustix::fs::statx(
        parent_fd.as_fd(),
        c.as_c_str(),
        AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::BASIC_STATS,
    )
    .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
    let node = match super::handles::entry_handle(parent_fd.as_fd(), name) {
        Ok(super::handles::HandleKind::Ino32Gen(k)) => k,
        _ => NodeKey {
            ino: stx.stx_ino,
            generation: 0,
        },
    };
    let target = if st_mode_kind(&stx) == Kind::Symlink {
        rustix::fs::readlinkat(parent_fd.as_fd(), c.as_c_str(), Vec::new())
            .ok()
            .map(|t| t.into_bytes())
    } else {
        None
    };
    Ok((node, attr_from_statx(&stx, target)))
}

#[allow(clippy::too_many_arguments)]
pub fn create(
    share: &Share,
    dir: NodeKey,
    name: &[u8],
    kind: Kind,
    mode: u16,
    rdev: u64,
    symlink_target: Option<&[u8]>,
    exclusive: bool,
) -> RpcResp {
    if share.readonly {
        return rofs();
    }
    let inner = || -> std::io::Result<(NodeKey, Attr)> {
        let pfd = open_parent(share, dir)?;
        let _id = IdentityGuard::enter(share);
        let c = cname(name)?;
        let raw = pfd.as_fd();
        let rc = match kind {
            Kind::Reg => {
                let mut flags = libc::O_CREAT | libc::O_WRONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
                if exclusive {
                    flags |= libc::O_EXCL;
                }
                // SAFETY: openat with a valid dirfd + C name.
                let fd = unsafe {
                    libc::openat(
                        rustix::fd::AsRawFd::as_raw_fd(&raw),
                        c.as_ptr(),
                        flags,
                        mode as libc::c_uint,
                    )
                };
                if fd < 0 {
                    -1
                } else {
                    // SAFETY: close our handle; the file persists.
                    unsafe { libc::close(fd) };
                    0
                }
            }
            Kind::Dir => unsafe {
                libc::mkdirat(
                    rustix::fd::AsRawFd::as_raw_fd(&raw),
                    c.as_ptr(),
                    mode as libc::c_uint,
                )
            },
            Kind::Symlink => {
                let target = cname(symlink_target.unwrap_or(b""))?;
                // SAFETY: symlinkat(target, dirfd, linkpath).
                unsafe {
                    libc::symlinkat(
                        target.as_ptr(),
                        rustix::fd::AsRawFd::as_raw_fd(&raw),
                        c.as_ptr(),
                    )
                }
            }
            Kind::Fifo | Kind::Sock | Kind::Chr | Kind::Blk => {
                let ifmt = match kind {
                    Kind::Fifo => libc::S_IFIFO,
                    Kind::Sock => libc::S_IFSOCK,
                    Kind::Chr => libc::S_IFCHR,
                    _ => libc::S_IFBLK,
                };
                // SAFETY: mknodat with a valid dirfd + name.
                unsafe {
                    libc::mknodat(
                        rustix::fd::AsRawFd::as_raw_fd(&raw),
                        c.as_ptr(),
                        ifmt | mode as libc::c_uint,
                        rdev as libc::dev_t,
                    )
                }
            }
        };
        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }
        entry_node(share, &pfd, name)
    };
    match inner() {
        Ok((node, attr)) => RpcResp::Entry { node, attr },
        Err(e) => err(&e, "create"),
    }
}

pub fn unlink(share: &Share, dir: NodeKey, name: &[u8], is_dir: bool) -> RpcResp {
    if share.readonly {
        return rofs();
    }
    let inner = || -> std::io::Result<()> {
        let pfd = open_parent(share, dir)?;
        let _id = IdentityGuard::enter(share);
        let c = cname(name)?;
        let flags = if is_dir { libc::AT_REMOVEDIR } else { 0 };
        // SAFETY: unlinkat with a valid dirfd + name.
        let rc = unsafe {
            libc::unlinkat(
                rustix::fd::AsRawFd::as_raw_fd(&pfd.as_fd()),
                c.as_ptr(),
                flags,
            )
        };
        if rc < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    };
    match inner() {
        Ok(()) => RpcResp::Ok,
        Err(e) => err(&e, if is_dir { "rmdir" } else { "unlink" }),
    }
}

pub fn rename(
    share: &Share,
    from_dir: NodeKey,
    from_name: &[u8],
    to_dir: NodeKey,
    to_name: &[u8],
) -> RpcResp {
    if share.readonly {
        return rofs();
    }
    let inner = || -> std::io::Result<()> {
        let ffd = open_parent(share, from_dir)?;
        let tfd = open_parent(share, to_dir)?;
        let _id = IdentityGuard::enter(share);
        let fc = cname(from_name)?;
        let tc = cname(to_name)?;
        // SAFETY: renameat with two valid dirfds + names.
        let rc = unsafe {
            libc::renameat(
                rustix::fd::AsRawFd::as_raw_fd(&ffd.as_fd()),
                fc.as_ptr(),
                rustix::fd::AsRawFd::as_raw_fd(&tfd.as_fd()),
                tc.as_ptr(),
            )
        };
        if rc < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    };
    match inner() {
        Ok(()) => RpcResp::Ok,
        Err(e) => err(&e, "rename"),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn setattr(
    share: &Share,
    node: NodeKey,
    mode: Option<u16>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    mtime: Option<Ts>,
) -> RpcResp {
    if share.readonly {
        return rofs();
    }
    let inner = || -> std::io::Result<Attr> {
        // chmod/utimes are *owner* operations — they must work on files with no read or write
        // permission (e.g. `chmod +w` on a 0444 file), so they go through the privileged O_PATH
        // handle's /proc path with the daemon's CAP_FOWNER (the file's owner is apply_uid, and
        // the squash model makes the Mac user that owner). Only truncation opens for write as
        // the apply identity — POSIX wants real write permission for that.
        let pathfd = share.open_node(node, libc::O_PATH | libc::O_NOFOLLOW)?;
        let proc = format!("/proc/self/fd/{}\0", pathfd.as_fd().as_raw_fd());
        let procp = proc.as_ptr() as *const libc::c_char;
        if let Some(m) = mode {
            // SAFETY: chmod on the proc-path of an owned O_PATH fd (follows to the real inode).
            if unsafe { libc::chmod(procp, m as libc::mode_t) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        // uid/gid are intentionally ignored under identity squash: the Mac sends the *squashed*
        // owner (the Mac user), which is meaningless in the guest. Ownership is fixed at create.
        let _ = (uid, gid);
        if let Some(sz) = size {
            let wfd = reopen_for_io(share, node, libc::O_WRONLY)?;
            // SAFETY: ftruncate on a valid write fd.
            if unsafe { libc::ftruncate(wfd.as_fd().as_raw_fd(), sz as libc::off_t) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        if let Some(t) = mtime {
            let times = [
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                },
                libc::timespec {
                    tv_sec: t.sec as libc::time_t,
                    tv_nsec: t.nsec as _,
                },
            ];
            // SAFETY: utimensat on the proc-path with a 2-element timespec array.
            if unsafe { libc::utimensat(libc::AT_FDCWD, procp, times.as_ptr(), 0) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        stat_attr(share, node)
    };
    match inner() {
        Ok(a) => RpcResp::Attr(a),
        Err(e) => err(&e, "setattr"),
    }
}

const WRITE_FD_CACHE: usize = 128;

/// O_WRONLY fd for `node`, served from the share's write-fd cache (the open_by_handle +
/// /proc reopen pair per chunk costs more than the pwrite at streaming rates).
fn cached_write_fd(share: &Share, node: NodeKey) -> std::io::Result<std::sync::Arc<OwnedFd>> {
    {
        let cache = share.write_fds.lock();
        if let Some((_, fd)) = cache.iter().find(|(n, _)| *n == node) {
            return Ok(fd.clone());
        }
    }
    let fd = std::sync::Arc::new(reopen_for_io(share, node, libc::O_WRONLY)?);
    let mut cache = share.write_fds.lock();
    if cache.len() >= WRITE_FD_CACHE {
        cache.remove(0);
    }
    cache.push((node, fd.clone()));
    Ok(fd)
}

pub fn write(share: &Share, node: NodeKey, off: u64, sync: bool, data: &[u8]) -> RpcResp {
    if share.readonly {
        return rofs();
    }
    let inner = || -> std::io::Result<Attr> {
        let fd = cached_write_fd(share, node)?;
        rustix::io::pwrite(fd.as_fd(), data, off)
            .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
        if sync && share.commit == crate::config::CommitPolicy::Fsync {
            rustix::fs::fdatasync(fd.as_fd())
                .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
        }
        stat_attr(share, node)
    };
    match inner() {
        Ok(a) => RpcResp::Attr(a),
        Err(e) => err(&e, "write"),
    }
}

pub fn commit(share: &Share, node: NodeKey) -> RpcResp {
    // writeback policy: COMMIT is a no-op (data rides guest writeback) — just return attrs.
    if share.commit == crate::config::CommitPolicy::Writeback {
        return match stat_attr(share, node) {
            Ok(a) => RpcResp::Attr(a),
            Err(e) => err(&e, "commit"),
        };
    }
    let inner = || -> std::io::Result<Attr> {
        // Prefer the cached write fd (this is the commit-after-write path); else fdatasync
        // needs a real fd — a write-only (0200) file can't open O_RDONLY, so fall back.
        let fd = match cached_write_fd(share, node) {
            Ok(fd) => fd,
            Err(_) => std::sync::Arc::new(match reopen_for_io(share, node, libc::O_RDONLY) {
                Ok(fd) => fd,
                Err(e) if e.raw_os_error() == Some(libc::EACCES) => {
                    reopen_for_io(share, node, libc::O_WRONLY)?
                }
                Err(e) => return Err(e),
            }),
        };
        rustix::fs::fdatasync(fd.as_fd())
            .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
        stat_attr(share, node)
    };
    match inner() {
        Ok(a) => RpcResp::Attr(a),
        Err(e) => err(&e, "commit"),
    }
}

/// Ranged COMMIT: the userspace twin of knfsd's `vfs_fsync_range`. A bounded range only
/// writes back + waits on those pages (the client's mid-stream commits cover data that's
/// usually already under writeback → cheap); `len == 0` = whole file = full fdatasync,
/// which the close-time commit always uses (close-to-open durability stays intact).
pub fn commit_range(share: &Share, node: NodeKey, off: u64, len: u64) -> RpcResp {
    if len == 0 || share.commit == crate::config::CommitPolicy::Writeback {
        return commit(share, node);
    }
    let inner = || -> std::io::Result<Attr> {
        let fd = cached_write_fd(share, node)?;
        // SAFETY: plain syscall on an owned fd; flags are the documented write-and-wait set.
        #[allow(unsafe_code)]
        let rc = unsafe {
            libc::sync_file_range(
                std::os::fd::AsRawFd::as_raw_fd(&fd),
                off as libc::off64_t,
                len as libc::off64_t,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        stat_attr(share, node)
    };
    match inner() {
        Ok(a) => RpcResp::Attr(a),
        Err(e) => err(&e, "commit_range"),
    }
}
