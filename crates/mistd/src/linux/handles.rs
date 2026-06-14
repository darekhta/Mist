//! NodeKey ⇔ kernel file-handle plumbing.
//!
//! ext4 (and ext2/3) export `FILEID_INO32_GEN` handles: 8 bytes = ino:u32 ‖ gen:u32, which is
//! exactly our `NodeKey` — derived with zero per-node state. `name_to_handle_at` is unprivileged;
//! `open_by_handle_at` needs CAP_DAC_READ_SEARCH (mistd runs as root in production; an
//! unprivileged path-map fallback exists for dev/test, see `shares.rs`).
//!
//! This module owns the only `unsafe` in mistd (raw syscalls; libc lacks stable wrappers for
//! the handle calls on all targets).
#![allow(unsafe_code)]

use mist_proto::NodeKey;
use std::ffi::CString;
use std::io;
use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

const FILEID_INO32_GEN: i32 = 1;
const FILEID_INO32_GEN_PARENT: i32 = 2;
const MAX_HANDLE_SZ: usize = 128;

#[repr(C)]
struct FileHandleBuf {
    handle_bytes: u32,
    handle_type: i32,
    f_handle: [u8; MAX_HANDLE_SZ],
}

/// Result of resolving a directory entry to a handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleKind {
    /// Proper ext4-style handle; NodeKey.generation is real.
    Ino32Gen(NodeKey),
    /// Exotic filesystem: caller falls back to statx ino with generation 0 (degraded ids).
    Other,
}

fn name_to_handle(dirfd: RawFd, name: &CString, flags: libc::c_int) -> io::Result<(i32, Vec<u8>)> {
    let mut buf = FileHandleBuf {
        handle_bytes: MAX_HANDLE_SZ as u32,
        handle_type: 0,
        f_handle: [0; MAX_HANDLE_SZ],
    };
    let mut mount_id: libc::c_int = 0;
    // SAFETY: buf is a properly sized file_handle with handle_bytes set; name is a valid
    // NUL-terminated C string; kernel writes within handle_bytes.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_name_to_handle_at,
            dirfd,
            name.as_ptr(),
            &mut buf as *mut FileHandleBuf,
            &mut mount_id as *mut libc::c_int,
            flags,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((
        buf.handle_type,
        buf.f_handle[..buf.handle_bytes as usize].to_vec(),
    ))
}

/// Handle for `dirfd`'s entry `name` (no symlink following — default semantics).
pub fn entry_handle(dirfd: BorrowedFd<'_>, name: &[u8]) -> io::Result<HandleKind> {
    let c = CString::new(name).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let (ty, bytes) = name_to_handle(dirfd.as_raw_fd(), &c, 0)?;
    Ok(decode(ty, &bytes))
}

/// Handle for the object `fd` itself (AT_EMPTY_PATH).
pub fn self_handle(fd: BorrowedFd<'_>) -> io::Result<HandleKind> {
    let empty = CString::new("").unwrap();
    let (ty, bytes) = name_to_handle(fd.as_raw_fd(), &empty, libc::AT_EMPTY_PATH)?;
    Ok(decode(ty, &bytes))
}

/// Handle for a path (used once per share at setup).
#[allow(dead_code)] // kept for symmetry; share setup uses self_handle on the O_PATH fd
pub fn path_handle(path: &Path) -> io::Result<HandleKind> {
    let c = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let (ty, bytes) = name_to_handle(libc::AT_FDCWD, &c, 0)?;
    Ok(decode(ty, &bytes))
}

/// Decode a raw kernel file handle (`handle_type` + `f_handle` bytes) into a NodeKey.
/// Used by the fanotify engine, which receives handles inline in event records.
pub fn decode_handle(handle_type: i32, f_handle: &[u8]) -> HandleKind {
    decode(handle_type, f_handle)
}

fn decode(ty: i32, bytes: &[u8]) -> HandleKind {
    if (ty == FILEID_INO32_GEN || ty == FILEID_INO32_GEN_PARENT) && bytes.len() >= 8 {
        let ino = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
        let generation = u32::from_ne_bytes(bytes[4..8].try_into().unwrap());
        HandleKind::Ino32Gen(NodeKey {
            ino: ino as u64,
            generation,
        })
    } else {
        HandleKind::Other
    }
}

/// Open a NodeKey via `open_by_handle_at` against the share's mount fd.
/// Requires CAP_DAC_READ_SEARCH; callers handle EPERM by using the degraded path map.
pub fn open_node(
    mount_fd: BorrowedFd<'_>,
    key: NodeKey,
    oflags: libc::c_int,
) -> io::Result<OwnedFd> {
    let mut buf = FileHandleBuf {
        handle_bytes: 8,
        handle_type: FILEID_INO32_GEN,
        f_handle: [0; MAX_HANDLE_SZ],
    };
    buf.f_handle[0..4].copy_from_slice(&(key.ino as u32).to_ne_bytes());
    buf.f_handle[4..8].copy_from_slice(&key.generation.to_ne_bytes());
    // SAFETY: buf is a valid 8-byte INO32_GEN handle; mount_fd identifies the filesystem.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_open_by_handle_at,
            mount_fd.as_raw_fd(),
            &mut buf as *mut FileHandleBuf,
            oflags | libc::O_CLOEXEC,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: rc is a fresh fd we own.
    Ok(unsafe { OwnedFd::from_raw_fd(rc as RawFd) })
}

use std::os::fd::AsRawFd;
