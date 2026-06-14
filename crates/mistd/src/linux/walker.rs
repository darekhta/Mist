//! Snapshot walker: parallel BFS over a share, streaming `SnapDir` records.
//!
//! Each directory scan runs on the blocking pool (getdents + statx are sync syscalls);
//! an async orchestrator bounds parallelism and feeds discovered subdirectories back in.
//! Record emission goes through an mpsc with backpressure (the bulk lane's send queue).

use super::handles::{self, HandleKind};
use super::shares::{Share, attr_from_statx};
use mist_proto::{Attr, Name, NodeKey, SnapDir, SnapDone, SnapEntry};
use rustix::fs::{AtFlags, StatxFlags};
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::sync::{Semaphore, mpsc};

#[derive(Debug)]
pub struct WalkStats {
    pub dirs: AtomicU64,
    pub entries: AtomicU64,
    pub errors: AtomicU32,
}

struct DirJob {
    rel: PathBuf, // relative to share root ("." for root)
    node: NodeKey,
    attr: Attr,
    parent: NodeKey,
}

struct ScanOutput {
    records: Vec<SnapDir>,
    subdirs: Vec<DirJob>,
    entries: u64,
    errors: u32,
}

/// Walk the share, sending `SnapDir`s through `tx`; returns the `SnapDone` to emit.
pub async fn walk(
    share: Arc<Share>,
    snap_id: u64,
    parallelism: usize,
    entries_per_record: usize,
    tx: mpsc::Sender<SnapDir>,
) -> SnapDone {
    let stats = Arc::new(WalkStats {
        dirs: AtomicU64::new(0),
        entries: AtomicU64::new(0),
        errors: AtomicU32::new(0),
    });
    let sem = Arc::new(Semaphore::new(parallelism.max(1)));
    let share_id = share.info_template.id;

    // Root job.
    let root_attr = match super::shares::statx_fd(share.root_fd.as_fd()) {
        Ok(stx) => attr_from_statx(&stx, None),
        Err(e) => {
            tracing::error!(error = %e, "stat of share root failed");
            return SnapDone {
                snap_id,
                share: share_id,
                dirs: 0,
                entries: 0,
                errors: 1,
            };
        }
    };
    let root = DirJob {
        rel: PathBuf::from("."),
        node: share.info_template.root,
        attr: root_attr,
        parent: share.info_template.root,
    };

    let mut join: tokio::task::JoinSet<ScanOutput> = tokio::task::JoinSet::new();
    let mut queue: Vec<DirJob> = vec![root];

    loop {
        // Saturate the pool from the frontier.
        while !queue.is_empty() {
            let Ok(permit) = sem.clone().try_acquire_owned() else {
                break;
            };
            let job = queue.pop().unwrap();
            let share = share.clone();
            join.spawn_blocking(move || {
                let out = scan_dir(&share, snap_id, &job, entries_per_record);
                drop(permit);
                out
            });
        }
        let Some(res) = join.join_next().await else {
            break; // frontier empty and no scans in flight ⇒ done
        };
        match res {
            Ok(mut out) => {
                stats.dirs.fetch_add(1, Ordering::Relaxed);
                stats.entries.fetch_add(out.entries, Ordering::Relaxed);
                stats.errors.fetch_add(out.errors, Ordering::Relaxed);
                for rec in out.records.drain(..) {
                    if tx.send(rec).await.is_err() {
                        // Receiver gone (session died): abort the walk.
                        join.abort_all();
                        return done(snap_id, share_id, &stats);
                    }
                }
                queue.append(&mut out.subdirs);
            }
            Err(e) => {
                tracing::warn!(error = %e, "walker task panicked/cancelled");
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    done(snap_id, share_id, &stats)
}

fn done(snap_id: u64, share: mist_proto::ShareId, stats: &WalkStats) -> SnapDone {
    SnapDone {
        snap_id,
        share,
        dirs: stats.dirs.load(Ordering::Relaxed),
        entries: stats.entries.load(Ordering::Relaxed),
        errors: stats.errors.load(Ordering::Relaxed),
    }
}

/// Scan one directory: getdents + per-entry statx (+ handle), chunked into SnapDir records.
fn scan_dir(share: &Share, snap_id: u64, job: &DirJob, per_record: usize) -> ScanOutput {
    let share_id = share.info_template.id;
    let mut out = ScanOutput {
        records: Vec::new(),
        subdirs: Vec::new(),
        entries: 0,
        errors: 0,
    };

    let dirfd = match share.open_node_rel(&job.rel, libc::O_DIRECTORY | libc::O_NOFOLLOW) {
        Ok(fd) => fd,
        Err(e) => {
            tracing::debug!(rel = %job.rel.display(), error = %e, "open dir failed");
            out.errors += 1;
            // Still emit an empty terminal record so the host marks the dir complete.
            out.records.push(SnapDir {
                snap_id,
                share: share_id,
                dir: job.node,
                dir_attr: job.attr.clone(),
                parent: job.parent,
                entries: vec![],
                last: true,
            });
            return out;
        }
    };

    let mut entries: Vec<SnapEntry> = Vec::with_capacity(per_record.min(1024));
    let flush = |entries: &mut Vec<SnapEntry>, last: bool, out: &mut ScanOutput| {
        out.records.push(SnapDir {
            snap_id,
            share: share_id,
            dir: job.node,
            dir_attr: job.attr.clone(),
            parent: job.parent,
            entries: std::mem::take(entries),
            last,
        });
    };

    let dir = match rustix::fs::Dir::read_from(dirfd.as_fd()) {
        Ok(d) => d,
        Err(e) => {
            tracing::debug!(rel = %job.rel.display(), error = %e, "getdents failed");
            out.errors += 1;
            flush(&mut entries, true, &mut out);
            return out;
        }
    };

    for ent in dir {
        let ent = match ent {
            Ok(e) => e,
            Err(_) => {
                out.errors += 1;
                continue;
            }
        };
        let name_bytes = ent.file_name().to_bytes();
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        let Ok(name) = Name::new(name_bytes.to_vec()) else {
            out.errors += 1;
            continue;
        };

        // statx the entry (no symlink following).
        let stx = match rustix::fs::statx(
            dirfd.as_fd(),
            ent.file_name(),
            AtFlags::SYMLINK_NOFOLLOW,
            StatxFlags::BASIC_STATS,
        ) {
            Ok(s) => s,
            Err(_) => {
                out.errors += 1; // raced with deletion — journal/resync covers it
                continue;
            }
        };

        // NodeKey: real handle or degraded statx ino.
        let node = if share.degraded {
            NodeKey {
                ino: stx.stx_ino,
                generation: 0,
            }
        } else {
            match handles::entry_handle(dirfd.as_fd(), name_bytes) {
                Ok(HandleKind::Ino32Gen(k)) => k,
                _ => NodeKey {
                    ino: stx.stx_ino,
                    generation: 0,
                },
            }
        };

        let mode = stx.stx_mode as u32;
        let is_dir = mode & libc::S_IFMT == libc::S_IFDIR;
        let is_symlink = mode & libc::S_IFMT == libc::S_IFLNK;

        let symlink_target = if is_symlink {
            match rustix::fs::readlinkat(dirfd.as_fd(), ent.file_name(), Vec::new()) {
                Ok(t) => Some(t.into_bytes()),
                Err(_) => {
                    out.errors += 1;
                    None
                }
            }
        } else {
            None
        };
        let attr = attr_from_statx(&stx, symlink_target);
        if is_symlink && attr.symlink_target.is_none() {
            continue; // unreadable symlink — skip rather than ship an invalid Attr
        }

        use std::os::unix::ffi::OsStrExt;
        let rel = job.rel.join(std::ffi::OsStr::from_bytes(name_bytes));
        share.remember_path(node, rel.clone());

        if is_dir {
            out.subdirs.push(DirJob {
                rel,
                node,
                attr: attr.clone(),
                parent: job.node,
            });
        }

        out.entries += 1;
        entries.push(SnapEntry { name, node, attr });
        if entries.len() >= per_record {
            flush(&mut entries, false, &mut out);
        }
    }
    flush(&mut entries, true, &mut out);
    out
}
