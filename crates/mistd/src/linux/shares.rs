//! Share setup and node access (privileged handle path or degraded path-map fallback).

use super::handles::{self, HandleKind};
use crate::config::ShareConfig;
use mist_proto::{Attr, Kind, NodeKey, ShareId, ShareInfo, Ts, share_flags};
use parking_lot::Mutex;
use rustix::fs::{AtFlags, Mode, OFlags, ResolveFlags, StatxFlags, openat2};
use std::collections::{BTreeMap, HashMap};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::PathBuf;
use std::sync::Arc;

pub struct Share {
    pub info_template: ShareInfo, // epoch filled per process; flags reflect capability probe
    pub root_fd: OwnedFd,         // readable directory fd; also the mount fd for open_by_handle
    pub degraded: bool,           // true ⇒ generation-less keys + path map
    /// NodeKey → relative path from root. Only populated in degraded mode (dev/test).
    pub path_map: Mutex<HashMap<NodeKey, PathBuf>>,
    /// Filesystem path of the share root (for the fanotify FILESYSTEM mark).
    pub mount_path: String,
    /// True if the share root is a *subtree* of its filesystem (not a mountpoint). A FILESYSTEM
    /// fanotify mark sees the whole fs, so subtree shares must filter events by path containment.
    pub subtree: bool,
    /// Identity the write applier squashes to (default: the share root's owner) so Mac-originated
    /// files are owned by the guest user, not root (mistd runs as root).
    pub apply_uid: u32,
    pub apply_gid: u32,
    pub readonly: bool,
    /// COMMIT durability policy (fsync | writeback) for Mac-originated writes.
    pub commit: crate::config::CommitPolicy,
    /// Write-fd cache: NodeKey → O_WRONLY fd reused across Write/Commit RPCs. The per-chunk
    /// open_by_handle + /proc reopen pair costs more than the pwrite itself at 1 MiB chunks.
    /// An open fd stays valid across rename/unlink/truncate, so there is nothing to
    /// invalidate; entries are evicted FIFO at capacity.
    pub write_fds: Mutex<Vec<(NodeKey, std::sync::Arc<OwnedFd>)>>,
}

impl std::fmt::Debug for Share {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Share")
            .field("name", &self.info_template.name)
            .field("degraded", &self.degraded)
            .finish()
    }
}

#[derive(Debug)]
pub struct Shares {
    pub by_id: BTreeMap<ShareId, Arc<Share>>,
    pub boot_id: u64,
}

impl Shares {
    pub fn infos(&self) -> Vec<ShareInfo> {
        self.by_id
            .values()
            .map(|s| s.info_template.clone())
            .collect()
    }

    pub fn get(&self, id: ShareId) -> Option<Arc<Share>> {
        self.by_id.get(&id).cloned()
    }
}

pub fn setup(config: &BTreeMap<String, ShareConfig>) -> anyhow::Result<Shares> {
    let boot_id = rand::random::<u64>();
    let mut by_id = BTreeMap::new();

    for (idx, (name, sc)) in config.iter().enumerate() {
        let id = ShareId(idx as u16);
        // Open as a real readable directory fd (not O_PATH): it doubles as the mount fd for
        // open_by_handle_at, which rejects O_PATH fds with EBADF on some kernels.
        let root_fd = rustix::fs::open(
            &sc.path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| anyhow::anyhow!("share {name}: open {}: {e}", sc.path.display()))?;

        let stx = statx_fd(root_fd.as_fd())?;
        // Filesystem identity = containing device id (stable across mounts of the same fs,
        // distinct across filesystems — sufficient for confinement checks and host display).
        let fsid = ((stx.stx_dev_major as u64) << 32) | stx.stx_dev_minor as u64;

        // Subtree detection: a mountpoint has a different device than its parent. If `path/..`
        // is on the same device, `path` is a subtree of its filesystem (fanotify FILESYSTEM
        // marks see the whole fs, so the journal engine must filter to the subtree).
        let parent_dev = rustix::fs::open(
            sc.path.join(".."),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .ok()
        .and_then(|pf| statx_fd(pf.as_fd()).ok())
        .map(|p| ((p.stx_dev_major as u64) << 32) | p.stx_dev_minor as u64);
        let subtree = parent_dev == Some(fsid);

        // Identity probe: do we get real (ino, generation) handles, and can we open by handle?
        let (root_key, mut degraded) = match handles::self_handle(root_fd.as_fd()) {
            Ok(HandleKind::Ino32Gen(k)) => (k, false),
            _ => (
                NodeKey {
                    ino: stx.stx_ino,
                    generation: 0,
                },
                true,
            ),
        };
        if !degraded {
            // open_by_handle_at needs CAP_DAC_READ_SEARCH; probe once.
            match handles::open_node(root_fd.as_fd(), root_key, libc::O_PATH) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::EPERM) => degraded = true,
                Err(e) => {
                    tracing::warn!(share = %name, error = %e, "open_by_handle probe failed; using path map");
                    degraded = true;
                }
            }
        }

        let mut flags = 0u32;
        if degraded {
            flags |= share_flags::DEGRADED_IDS;
        }
        if sc.commit == crate::config::CommitPolicy::Writeback {
            flags |= share_flags::WRITEBACK;
        }
        if sc.readonly {
            flags |= share_flags::RDONLY;
        }

        let epoch = {
            let mut h = blake3::Hasher::new();
            h.update(&boot_id.to_le_bytes());
            h.update(&fsid.to_le_bytes());
            h.update(sc.path.as_os_str().as_encoded_bytes());
            u64::from_le_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
        };

        let share = Share {
            info_template: ShareInfo {
                id,
                name: name.clone(),
                epoch,
                fsid,
                root: root_key,
                flags,
                ino_bits: 32,
            },
            root_fd,
            degraded,
            path_map: Mutex::new(HashMap::new()),
            mount_path: sc.path.to_string_lossy().into_owned(),
            subtree,
            apply_uid: sc.apply_uid.unwrap_or(stx.stx_uid),
            apply_gid: sc.apply_gid.unwrap_or(stx.stx_gid),
            readonly: sc.readonly,
            commit: sc.commit,
            write_fds: Mutex::new(Vec::new()),
        };
        if degraded {
            share.path_map.lock().insert(root_key, PathBuf::from("."));
        }
        tracing::info!(share = %name, path = %sc.path.display(), degraded, "share ready");
        by_id.insert(id, Arc::new(share));
    }

    if by_id.is_empty() {
        anyhow::bail!("no shares configured");
    }
    Ok(Shares { by_id, boot_id })
}

impl Share {
    /// Open a node for reading metadata/data. Handle path in production; path map when degraded.
    pub fn open_node(&self, key: NodeKey, oflags: libc::c_int) -> std::io::Result<OwnedFd> {
        if !self.degraded {
            return handles::open_node(self.root_fd.as_fd(), key, oflags);
        }
        let rel = self
            .path_map
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
        let mut of = OFlags::CLOEXEC;
        if oflags & libc::O_PATH != 0 {
            of |= OFlags::PATH;
        }
        if oflags & libc::O_DIRECTORY != 0 {
            of |= OFlags::DIRECTORY;
        }
        if oflags & libc::O_NOFOLLOW != 0 {
            of |= OFlags::NOFOLLOW;
        }
        openat2(
            self.root_fd.as_fd(),
            &rel,
            of,
            Mode::empty(),
            ResolveFlags::BENEATH,
        )
        .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))
    }

    pub fn remember_path(&self, key: NodeKey, rel: PathBuf) {
        if self.degraded {
            self.path_map.lock().insert(key, rel);
        }
    }

    /// Open a path relative to the share root, confined by RESOLVE_BENEATH.
    pub fn open_node_rel(
        &self,
        rel: &std::path::Path,
        oflags: libc::c_int,
    ) -> std::io::Result<OwnedFd> {
        let mut of = OFlags::CLOEXEC;
        if oflags & libc::O_PATH != 0 {
            of |= OFlags::PATH;
        }
        if oflags & libc::O_DIRECTORY != 0 {
            of |= OFlags::DIRECTORY;
        }
        if oflags & libc::O_NOFOLLOW != 0 {
            of |= OFlags::NOFOLLOW;
        }
        openat2(
            self.root_fd.as_fd(),
            rel,
            of,
            Mode::empty(),
            ResolveFlags::BENEATH,
        )
        .map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error()))
    }
}

pub fn statx_fd(fd: BorrowedFd<'_>) -> anyhow::Result<rustix::fs::Statx> {
    Ok(rustix::fs::statx(
        fd,
        "",
        AtFlags::EMPTY_PATH,
        StatxFlags::BASIC_STATS,
    )?)
}

/// Convert a statx result to a wire Attr (symlink target supplied by the caller when relevant).
pub fn attr_from_statx(stx: &rustix::fs::Statx, symlink_target: Option<Vec<u8>>) -> Attr {
    let mode = stx.stx_mode as u32;
    let kind = match mode & libc::S_IFMT {
        libc::S_IFDIR => Kind::Dir,
        libc::S_IFLNK => Kind::Symlink,
        libc::S_IFIFO => Kind::Fifo,
        libc::S_IFSOCK => Kind::Sock,
        libc::S_IFCHR => Kind::Chr,
        libc::S_IFBLK => Kind::Blk,
        _ => Kind::Reg,
    };
    Attr {
        kind,
        mode: (mode & 0o7777) as u16,
        nlink: stx.stx_nlink,
        uid: stx.stx_uid,
        gid: stx.stx_gid,
        size: stx.stx_size,
        blocks: stx.stx_blocks,
        mtime: Ts {
            sec: stx.stx_mtime.tv_sec,
            nsec: stx.stx_mtime.tv_nsec,
        },
        ctime: Ts {
            sec: stx.stx_ctime.tv_sec,
            nsec: stx.stx_ctime.tv_nsec,
        },
        rdev: ((stx.stx_rdev_major as u64) << 32) | stx.stx_rdev_minor as u64,
        content_version: 0, // journal engine owns this
        symlink_target: if kind == Kind::Symlink {
            symlink_target
        } else {
            None
        },
    }
}
