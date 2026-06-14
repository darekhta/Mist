//! RPC handlers: Stat / StatBatch / Lookup / Read.

use super::handles::{self, HandleKind};
use super::shares::{Share, attr_from_statx};
use mist_proto::{Attr, NodeKey, RpcErr, RpcResp};
use rustix::fs::{AtFlags, StatxFlags};
use std::os::fd::AsFd;
use std::sync::Arc;

pub const READ_CHUNK: usize = 256 * 1024;

fn errno_of(e: &std::io::Error) -> i32 {
    e.raw_os_error().unwrap_or(libc::EIO)
}

fn err(e: &std::io::Error, what: &str) -> RpcResp {
    RpcResp::Err(RpcErr {
        errno: errno_of(e),
        msg: what.to_string(),
    })
}

pub fn stat(share: &Share, node: NodeKey) -> RpcResp {
    match stat_attr(share, node) {
        Ok(a) => RpcResp::Attr(a),
        Err(e) => err(&e, "stat"),
    }
}

pub fn stat_attr(share: &Share, node: NodeKey) -> std::io::Result<Attr> {
    let fd = share.open_node(node, libc::O_PATH | libc::O_NOFOLLOW)?;
    let stx = rustix::fs::statx(fd.as_fd(), "", AtFlags::EMPTY_PATH, StatxFlags::BASIC_STATS)
        .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
    // Symlink targets: O_PATH|O_NOFOLLOW opens the link itself; read target via /proc-free path.
    let target = if stx.stx_mode as u32 & libc::S_IFMT == libc::S_IFLNK {
        rustix::fs::readlinkat(fd.as_fd(), "", Vec::new())
            .ok()
            .map(|c| c.into_bytes())
    } else {
        None
    };
    Ok(attr_from_statx(&stx, target))
}

pub fn stat_batch(share: &Share, nodes: &[NodeKey]) -> RpcResp {
    RpcResp::Attrs(nodes.iter().map(|&n| stat_attr(share, n).ok()).collect())
}

/// Stat a node by NodeKey (alias used by the fanotify engine).
pub fn stat_node(share: &Share, node: NodeKey) -> std::io::Result<Attr> {
    stat_attr(share, node)
}

/// Resolve a directory entry to (NodeKey, Attr) — used by the fanotify engine to attribute a
/// freshly created entry. Same logic as the `lookup` RPC handler but returns a typed result.
pub fn stat_entry(share: &Share, dir: NodeKey, name: &[u8]) -> std::io::Result<(NodeKey, Attr)> {
    let dirfd = share.open_node(dir, libc::O_DIRECTORY | libc::O_NOFOLLOW)?;
    let cname = std::ffi::CString::new(name)
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let stx = rustix::fs::statx(
        dirfd.as_fd(),
        cname.as_c_str(),
        AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::BASIC_STATS,
    )
    .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
    let node = if share.degraded {
        NodeKey {
            ino: stx.stx_ino,
            generation: 0,
        }
    } else {
        match handles::entry_handle(dirfd.as_fd(), name) {
            Ok(HandleKind::Ino32Gen(k)) => k,
            _ => NodeKey {
                ino: stx.stx_ino,
                generation: 0,
            },
        }
    };
    let target = if stx.stx_mode as u32 & libc::S_IFMT == libc::S_IFLNK {
        rustix::fs::readlinkat(dirfd.as_fd(), cname.as_c_str(), Vec::new())
            .ok()
            .map(|c| c.into_bytes())
    } else {
        None
    };
    Ok((node, attr_from_statx(&stx, target)))
}

pub fn lookup(share: &Share, dir: NodeKey, name: &[u8]) -> RpcResp {
    let inner = || -> std::io::Result<(NodeKey, Attr)> {
        let dirfd = share.open_node(dir, libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW)?;
        let cname = std::ffi::CString::new(name)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let stx = rustix::fs::statx(
            dirfd.as_fd(),
            cname.as_c_str(),
            AtFlags::SYMLINK_NOFOLLOW,
            StatxFlags::BASIC_STATS,
        )
        .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
        let node = if share.degraded {
            NodeKey {
                ino: stx.stx_ino,
                generation: 0,
            }
        } else {
            match handles::entry_handle(dirfd.as_fd(), name) {
                Ok(HandleKind::Ino32Gen(k)) => k,
                _ => NodeKey {
                    ino: stx.stx_ino,
                    generation: 0,
                },
            }
        };
        let target = if stx.stx_mode as u32 & libc::S_IFMT == libc::S_IFLNK {
            rustix::fs::readlinkat(dirfd.as_fd(), cname.as_c_str(), Vec::new())
                .ok()
                .map(|c| c.into_bytes())
        } else {
            None
        };
        Ok((node, attr_from_statx(&stx, target)))
    };
    match inner() {
        Ok((node, attr)) => RpcResp::Entry { node, attr },
        Err(e) => err(&e, "lookup"),
    }
}

#[derive(Debug)]
pub struct ReadPlan {
    pub header: RpcResp,
    /// (fd, offset, length) when there are bytes to stream.
    pub stream: Option<(std::os::fd::OwnedFd, u64, u64)>,
}

/// Open + size the read. The server streams `stream` as bulk frames after sending `header`.
pub fn read_plan(share: &Arc<Share>, node: NodeKey, off: u64, len: u32) -> ReadPlan {
    let inner = || -> std::io::Result<ReadPlan> {
        let fd = share.open_node(node, libc::O_RDONLY | libc::O_NOFOLLOW)?;
        let stx = rustix::fs::statx(fd.as_fd(), "", AtFlags::EMPTY_PATH, StatxFlags::BASIC_STATS)
            .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
        if stx.stx_mode as u32 & libc::S_IFMT != libc::S_IFREG {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }
        let size = stx.stx_size;
        let end = size.min(off.saturating_add(len as u64));
        let n = end.saturating_sub(off);
        let eof = end >= size;
        Ok(ReadPlan {
            header: RpcResp::ReadStart {
                version: 0,
                len: n,
                eof,
            },
            stream: if n > 0 { Some((fd, off, n)) } else { None },
        })
    };
    match inner() {
        Ok(p) => p,
        Err(e) => ReadPlan {
            header: err(&e, "read"),
            stream: None,
        },
    }
}

/// Blocking pread of one chunk; returns the bytes read (empty = unexpected EOF).
pub fn pread_chunk(fd: &std::os::fd::OwnedFd, off: u64, want: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; want.min(READ_CHUNK)];
    let n = rustix::io::pread(fd.as_fd(), &mut buf, off)
        .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))?;
    buf.truncate(n);
    Ok(buf)
}
