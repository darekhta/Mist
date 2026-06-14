//! `MountSurface` implementation: metadata from the share's replica, reads via the VM RPC client.

use crate::session::{ShareHandle, VmHandle};
use mist_nfs::{
    CreateKind, DirEntry, FsStat, MountSurface, MutFuture, NfsError, NfsResult, ReadDirPage,
    ReadFuture, ReadResult, SendableFuture, SendableRead, SetAttr as NfsSetAttr,
};
use mist_proto::{Attr, Kind, Name, NodeKey, Rec, RpcReq, RpcResp, ShareId, Ts};
use mist_replica::ReplicaError;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

/// Write-behind state for one node (design 04 §4.3, narrowed to UNSTABLE writes). The NFS
/// reply goes out as soon as the data is queued; batches fly to the guest CONCURRENTLY —
/// RFC 1813/5661 forbid a client from keeping overlapping WRITEs in flight, so concurrent
/// apply is exactly what kernel nfsd does too. COMMIT is the durability + error barrier.
#[derive(Debug, Default)]
struct NodeWrites {
    /// Queued-but-unacked batches; 0 = drained.
    inflight: AtomicU32,
    /// Ranges of batches still in flight (ranged COMMIT waits only for intersecting ones —
    /// the client's mid-stream commits cover EARLIER data and must not pay for the tail).
    pending_ranges: Mutex<Vec<(u64, u64, u32)>>,
    next_batch_id: AtomicU32,
    drained: tokio::sync::Notify,
    /// First background failure; surfaced (once) at the next COMMIT.
    err: Mutex<Option<NfsError>>,
    /// Bytes acked since the last advisory pre-sync (see PRESYNC_BYTES).
    presync: std::sync::atomic::AtomicU64,
    /// True once any write was queued after the last successful full commit: a clean full
    /// COMMIT is honestly a no-op (nothing new to make durable) and skips the guest fdatasync
    /// — the macOS client commits twice around CLOSE and the second one was pure latency.
    dirty: std::sync::atomic::AtomicBool,
    /// 4 MiB chunk offsets already ingested under the WIP manifest key.
    ingested: Mutex<std::collections::HashSet<u64>>,
    /// Write-behind payloads kept for CAS ingest at the final COMMIT (when the attrs — and
    /// therefore the manifest fingerprint — settle). `None` = stash abandoned (over cap).
    /// This is what lets a freshly written file read back at cache speed, the same way
    /// kernel nfsd serves it from the guest page cache.
    stash: Mutex<Option<Stash>>,
    /// Last time a write was queued for this node. The reaper drops idle entries whose COMMIT
    /// never came (e.g. a file written UNSTABLE then unlinked — every editor save-via-rename
    /// orphans the old inode's queue), so the map can't grow without bound.
    last_active: Mutex<Option<std::time::Instant>>,
    /// Completion signal of the most recently queued write-behind batch. Each new batch awaits
    /// the previous one before applying to the guest, so per-node writes land IN ARRIVAL ORDER.
    /// Without this, spawned `write_through` tasks race and overlapping writes can be applied
    /// out of order — the guest ends up with stale bytes (observed as fsx mismatches under
    /// concurrent load). The client serializes overlapping writes on our ack, so the order this
    /// chain captures is the client's wire order.
    chain_tail: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

/// (offset, payload) write-behind records pending CAS ingest.
type Stash = Vec<(u64, Arc<Vec<u8>>)>;

/// Stash cap per node; bigger writes just lose the warm-read shortcut.
const STASH_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Manifest version sentinel for chunks ingested while the file is still being written (its
/// fingerprint is still moving); one cheap rebind moves them under the final key.
const WIP_FP: u64 = u64::MAX;

/// After this many acked write-behind bytes, fire an advisory background Commit (guest
/// fdatasync) so ext4 flushes incrementally — the client's real COMMIT then finds almost
/// nothing dirty. Kernel nfsd gets this for free from guest writeback; without it one mid-dd
/// COMMIT measured 180 ms and the client stalls the whole stream on it.
const PRESYNC_BYTES: u64 = 16 * 1024 * 1024;

/// One exported share. Always reads the *current* replica (so a reseed swap is transparent) and
/// the *current* RPC client (so a reconnect is transparent).
#[derive(Debug)]
pub struct ShareSurface {
    vm: Arc<VmHandle>,
    share: Arc<ShareHandle>,
    share_id: u16,
    root: NodeKey,
    /// Identity squash (design 05 §7): every attribute returned to the macOS client has its
    /// uid/gid rewritten to the Mac user, so files appear "yours" and writes are permitted. The
    /// guest still owns the files as its `apply_uid` (set by the applier).
    squash_uid: u32,
    squash_gid: u32,
    /// Per-node write-behind queues (entries removed when drained at COMMIT).
    /// Per-node write-behind queues. Arc-shared so [`detached`] copies (used by the spawned
    /// write-behind/cleanup tasks) operate on the SAME map — a fresh map per copy would orphan
    /// every entry the cleanup tries to remove.
    writes: Arc<Mutex<HashMap<NodeKey, Arc<NodeWrites>>>>,
    /// Manifest LRU: the key embeds the content fingerprint, so entries self-invalidate on
    /// any change — saves a redb lookup (~30 µs) per streaming READ.
    manifests: Arc<Mutex<HashMap<mist_cas::ManifestKey, Arc<Vec<mist_cas::ChunkRef>>>>>,
    /// Backpressure for UNSTABLE write-behind: each in-flight batch holds one permit, so the
    /// retained payload memory is bounded at `WRITE_BEHIND_SLOTS × ≤1 MiB`. Without this the
    /// macOS client (which gets an instant ack and keeps writing) outruns the guest apply rate
    /// and the spawned tasks' `Arc<Vec<u8>>` payloads pile up unbounded (measured: 613 MB / 8k
    /// pending writes under sustained fsx). Awaiting a permit just delays the WRITE reply —
    /// exactly how knfsd throttles via its bounded nfsd thread pool. Shared across copies.
    write_slots: Arc<tokio::sync::Semaphore>,
}

/// Max concurrent UNSTABLE write-behind batches per share (memory bound = this × max batch,
/// where the NFS layer caps a batch at 1 MiB → ≤96 MiB of in-flight payloads).
const WRITE_BEHIND_SLOTS: usize = 96;

/// Reap a write-behind entry once it's idle (no in-flight batches) for this long. Keeps the
/// post-write warm-read window (finalize_stash ingests well within it) while bounding the map.
const WRITE_IDLE_REAP: std::time::Duration = std::time::Duration::from_secs(20);

impl ShareSurface {
    pub fn new(vm: Arc<VmHandle>, share: Arc<ShareHandle>) -> Self {
        let info = share.info.read();
        let share_id = info.id.0;
        let root = info.root;
        drop(info);
        // SAFETY: geteuid/getegid are always-succeeding scalar syscalls.
        #[allow(unsafe_code)]
        let (squash_uid, squash_gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        let writes: Arc<Mutex<HashMap<NodeKey, Arc<NodeWrites>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        Self::spawn_write_reaper(&writes);
        ShareSurface {
            vm,
            share,
            share_id,
            root,
            squash_uid,
            squash_gid,
            writes,
            manifests: Arc::new(Mutex::new(HashMap::new())),
            write_slots: Arc::new(tokio::sync::Semaphore::new(WRITE_BEHIND_SLOTS)),
        }
    }

    fn node_writes(&self, node: NodeKey) -> Arc<NodeWrites> {
        self.writes
            .lock()
            .entry(node)
            .or_insert_with(|| {
                let q = NodeWrites::default();
                *q.stash.lock() = Some(Vec::new());
                Arc::new(q)
            })
            .clone()
    }

    /// Background reaper: drop write-behind entries that have gone idle. The COMMIT-time
    /// cleanup only fires when a full COMMIT actually arrives; a file written UNSTABLE then
    /// unlinked (every editor save-via-rename orphans the previous inode) would otherwise leave
    /// its `NodeWrites` — and the whole stash of write payloads it holds — in the map forever.
    /// Holds a `Weak` so it exits when the surface (and all detached copies) drop.
    fn spawn_write_reaper(writes: &Arc<Mutex<HashMap<NodeKey, Arc<NodeWrites>>>>) {
        let weak = Arc::downgrade(writes);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                let Some(map) = weak.upgrade() else { return };
                map.lock().retain(|_, q| {
                    if q.inflight.load(AtomicOrdering::Acquire) > 0 {
                        return true;
                    }
                    match *q.last_active.lock() {
                        Some(t) => t.elapsed() < WRITE_IDLE_REAP,
                        None => false, // created but never written: drop
                    }
                });
            }
        });
    }

    /// Wait until all queued write-behind batches for `node` reached the guest. Fast path
    /// (no entry / already drained) is one mutex-protected map lookup.
    async fn drain_writes(&self, node: NodeKey) -> Option<NfsError> {
        self.drain_writes_range(node, 0, u64::MAX).await
    }

    /// Wait only for in-flight batches intersecting `[off, off+len)` (`len == u64::MAX` =
    /// everything). A ranged COMMIT for already-landed data returns immediately instead of
    /// queueing behind the freshly written tail.
    async fn drain_writes_range(&self, node: NodeKey, off: u64, len: u64) -> Option<NfsError> {
        let q = {
            let map = self.writes.lock();
            match map.get(&node) {
                Some(q) if q.inflight.load(AtomicOrdering::Acquire) > 0 => q.clone(),
                _ => return None,
            }
        };
        let end = off.saturating_add(len);
        loop {
            let notified = q.drained.notified();
            let blocking = {
                let ranges = q.pending_ranges.lock();
                ranges
                    .iter()
                    .any(|(b_off, b_len, _)| *b_off < end && b_off + b_len > off)
            };
            if !blocking {
                break;
            }
            notified.await;
        }
        q.err.lock().take()
    }

    /// Rewrite an attribute's owner to the Mac user (squash). Returns the same attr otherwise.
    fn squash(&self, mut a: Attr) -> Attr {
        a.uid = self.squash_uid;
        a.gid = self.squash_gid;
        a
    }
}

fn map_err(e: ReplicaError) -> NfsError {
    match e {
        ReplicaError::NoEnt => NfsError::NoEnt,
        ReplicaError::NotDir => NfsError::NotDir,
        ReplicaError::NotSymlink => NfsError::NotSymlink,
        ReplicaError::Incomplete => NfsError::Jukebox, // not seeded yet → client retries
        ReplicaError::BadPath => NfsError::NoEnt,
    }
}

impl MountSurface for ShareSurface {
    fn share_id(&self) -> u16 {
        self.share_id
    }

    fn root(&self) -> NodeKey {
        self.root
    }

    fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
        self.share
            .replica()
            .getattr(node)
            .map(|a| self.squash(a))
            .map_err(map_err)
    }

    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        self.share
            .replica()
            .lookup(dir, name)
            .map(|(n, a)| (n, self.squash(a)))
            .map_err(map_err)
    }

    fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        max_entries: usize,
        want_attrs: bool,
    ) -> NfsResult<ReadDirPage> {
        let replica = self.share.replica();
        let page = replica.readdir(dir, cookie, max_entries).map_err(map_err)?;
        let entries = page
            .entries
            .into_iter()
            .map(|e| DirEntry {
                attr: if want_attrs {
                    replica.getattr(e.node).ok().map(|a| self.squash(a))
                } else {
                    None
                },
                name: e.name.into_bytes(),
                node: e.node,
                cookie: e.cookie,
            })
            .collect();
        Ok(ReadDirPage {
            entries,
            eof: page.eof,
            cookieverf: page.dirgen,
        })
    }

    fn readlink(&self, node: NodeKey) -> NfsResult<Vec<u8>> {
        self.share.replica().readlink(node).map_err(map_err)
    }

    fn parent(&self, node: NodeKey) -> NfsResult<NodeKey> {
        self.share.replica().parent_of(node).ok_or(NfsError::NoEnt)
    }

    fn touch_dir(&self, dir: NodeKey) {
        self.bump_dir(dir);
    }

    fn writes_are_stable(&self) -> bool {
        self.share.info.read().flags & mist_proto::share_flags::WRITEBACK != 0
    }

    fn read_sendable(&self, node: NodeKey, offset: u64, count: u32) -> SendableFuture<'_> {
        Box::pin(async move {
            // A/B-measured on loopback: sendfile(2) tops out ~870 MB/s while write(2) from a
            // pooled warm buffer reaches ~1000+ (macOS sendfile pays per-page VM work that a
            // hot-buffer copyout doesn't). Keep the machinery, prefer the copy path.
            if !std::env::var("MIST_SENDFILE").is_ok_and(|v| v == "1") {
                return None;
            }
            // Strictly a fast path: any complication (pending writes, multi-chunk span, CAS
            // miss) falls back to the copying read. Never waits.
            let cas = self.vm.cas.clone()?;
            if self
                .writes
                .lock()
                .get(&node)
                .is_some_and(|q| q.inflight.load(AtomicOrdering::Acquire) > 0)
            {
                return None;
            }
            let attr = self.share.replica().getattr(node).ok()?;
            let size = attr.size;
            if offset >= size || count == 0 {
                return None; // edge cases go through the slow path's exact semantics
            }
            let end = size.min(offset + count as u64);
            if offset / CAS_CHUNK != (end - 1) / CAS_CHUNK {
                return None;
            }
            let chunk_off = offset / CAS_CHUNK * CAS_CHUNK;
            let key = mist_cas::ManifestKey {
                share: self.share_id,
                ino: node.ino,
                generation: node.generation,
                content_version: content_fingerprint(&attr),
            };
            let m = self.manifest(&cas, key)?;
            let c = m.iter().find(|c| c.off == chunk_off)?;
            let inner = (offset - chunk_off) as u32;
            let len = (end - offset) as u32;
            let (file, abs_off) = cas.blob_file_range(&c.hash, inner, len)?;
            Some(SendableRead {
                file,
                off: abs_off,
                len,
                eof: end >= size,
            })
        })
    }

    fn read_into<'a>(
        &'a self,
        node: NodeKey,
        offset: u64,
        count: u32,
        out: &'a mut Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<NfsResult<bool>>> + Send + 'a>>
    {
        Box::pin(async move {
            // Fused warm path only: pending writes / recently-Mac-written (CAS may race the
            // fingerprint — see read()) / multi-chunk / CAS miss → caller falls back to the
            // copying read, which serves the guest authoritatively. Payload preads STRAIGHT
            // into the wire buffer.
            let cas = self.vm.cas.clone()?;
            if self.vm.is_mac_dirty(self.share_id, node) {
                return None;
            }
            if self
                .writes
                .lock()
                .get(&node)
                .is_some_and(|q| q.inflight.load(AtomicOrdering::Acquire) > 0)
            {
                return None;
            }
            let attr = self.share.replica().getattr(node).ok()?;
            let size = attr.size;
            if offset >= size || count == 0 {
                return None;
            }
            let end = size.min(offset + count as u64);
            if offset / CAS_CHUNK != (end - 1) / CAS_CHUNK {
                return None;
            }
            let chunk_off = offset / CAS_CHUNK * CAS_CHUNK;
            let key = mist_cas::ManifestKey {
                share: self.share_id,
                ino: node.ino,
                generation: node.generation,
                content_version: content_fingerprint(&attr),
            };
            let m = self.manifest(&cas, key)?;
            let c = m.iter().find(|c| c.off == chunk_off)?;
            let inner = (offset - chunk_off) as u32;
            let len = (end - offset) as usize;
            let (file, abs_off) = cas.blob_file_range(&c.hash, inner, len as u32)?;
            use std::os::unix::fs::FileExt;
            let start = out.len();
            out.reserve(len);
            // Read into the spare capacity directly: a `resize(.., 0)` here memsets 1 MiB that
            // the pread immediately overwrites — measurable at per-op streaming rates.
            #[allow(unsafe_code)]
            // SAFETY: `reserve` guarantees `len` writable bytes past `start`; the region is
            // exposed via `set_len` only after `read_at` filled all of it, and on a short or
            // failed read the length stays at `start`.
            unsafe {
                let dst = std::slice::from_raw_parts_mut(out.as_mut_ptr().add(start), len);
                if file.read_at(dst, abs_off).is_ok_and(|n| n == len) {
                    out.set_len(start + len);
                    Some(Ok(end >= size))
                } else {
                    None
                }
            }
        })
    }

    fn read(&self, node: NodeKey, offset: u64, count: u32) -> ReadFuture<'_> {
        Box::pin(async move {
            // Read-after-write: queued write-behind batches must land in the guest before any
            // guest fetch (the CAS can't serve them either — the optimistic mtime rotated the
            // fingerprint). No-op unless the node is dirty.
            if let Some(e) = self.drain_writes(node).await {
                return Err(e);
            }
            // Authoritative read for recently-Mac-written nodes. The CAS warm path keys chunks
            // on a (mtime,size) fingerprint; under concurrent client write pipelining that
            // fingerprint can momentarily disagree with the freshest content, which surfaced as
            // rare fsx data mismatches under load. While a node is in the mac-dirty window we
            // read straight from the guest (which holds every drained write) — correct by
            // construction. The CAS resumes serving once the node settles (the common warm case
            // is GUEST-produced content, which is never mac-dirty).
            if self.vm.cas.is_some() && self.vm.is_mac_dirty(self.share_id, node) {
                let (data, eof) = self.guest_read(node, offset, count as u64).await?;
                return Ok(ReadResult { data, eof });
            }
            let Some(cas) = self.vm.cas.clone() else {
                // Cache disabled: plain guest RPC read.
                let (data, eof) = self.guest_read(node, offset, count as u64).await?;
                return Ok(ReadResult { data, eof });
            };
            // Size + content_version from the replica: the version keys the cache, so a guest
            // write (journal Content rec) atomically invalidates by changing the key.
            let attr = self.share.replica().getattr(node).map_err(map_err)?;
            let size = attr.size;
            if offset >= size {
                return Ok(ReadResult {
                    data: Vec::new(),
                    eof: true,
                });
            }
            let key = mist_cas::ManifestKey {
                share: self.share_id,
                ino: node.ino,
                generation: node.generation,
                content_version: content_fingerprint(&attr),
            };
            let end = size.min(offset + count as u64);
            // Fast path: the read fits one CAS chunk (every rsize=1MiB read does) — return the
            // chunk slice directly instead of copying through an accumulator.
            if offset / CAS_CHUNK == (end - 1) / CAS_CHUNK {
                let chunk_off = offset / CAS_CHUNK * CAS_CHUNK;
                let chunk_len = CAS_CHUNK.min(size - chunk_off);
                let piece = self
                    .chunk_read(
                        &cas,
                        key,
                        node,
                        chunk_off,
                        chunk_len,
                        offset - chunk_off,
                        end - offset,
                        size,
                    )
                    .await?;
                let eof = offset + piece.len() as u64 >= size;
                return Ok(ReadResult { data: piece, eof });
            }
            let mut data = Vec::with_capacity((end - offset) as usize);
            let mut pos = offset;
            while pos < end {
                let chunk_off = pos / CAS_CHUNK * CAS_CHUNK;
                let chunk_len = CAS_CHUNK.min(size - chunk_off);
                let inner = pos - chunk_off;
                let want = (end - pos).min(chunk_len - inner);
                let piece = self
                    .chunk_read(&cas, key, node, chunk_off, chunk_len, inner, want, size)
                    .await?;
                let n = piece.len() as u64;
                data.extend_from_slice(&piece);
                pos += n;
                if n < want {
                    break; // file shrank under us; serve what we got
                }
            }
            let eof = pos >= size;
            Ok(ReadResult { data, eof })
        })
    }

    fn fsstat(&self) -> FsStat {
        FsStat::default()
    }

    fn writable(&self) -> bool {
        true
    }

    fn create<'a>(
        &'a self,
        dir: NodeKey,
        name: &'a [u8],
        kind: CreateKind,
        mode: u16,
    ) -> MutFuture<'a, (NodeKey, Attr)> {
        Box::pin(async move {
            let (mkind, rdev, target) = match kind {
                CreateKind::File { exclusive } => ((Kind::Reg, exclusive), 0u64, None),
                CreateKind::Dir => ((Kind::Dir, false), 0, None),
                CreateKind::Symlink { target } => ((Kind::Symlink, false), 0, Some(target)),
                CreateKind::Fifo => ((Kind::Fifo, false), 0, None),
                CreateKind::Socket => ((Kind::Sock, false), 0, None),
                CreateKind::Device {
                    is_block,
                    major,
                    minor,
                } => (
                    (if is_block { Kind::Blk } else { Kind::Chr }, false),
                    ((major as u64) << 32) | minor as u64,
                    None,
                ),
            };
            let req = RpcReq::Create {
                share: ShareId(self.share_id),
                dir,
                name: name_of(name)?,
                kind: mkind.0,
                mode,
                rdev,
                symlink_target: target,
                exclusive: mkind.1,
            };
            let (node, attr) = self.mutate_entry(req).await?;
            // Apply to the replica; fence both the new node's and the directory's echoes.
            self.share.replica().apply_rec(&Rec::Created {
                parent: dir,
                name: name_of(name)?,
                node,
                attr: Some(attr.clone()),
            });
            self.vm.note_mac_dirty(self.share_id, node);
            self.bump_dir(dir);
            Ok((node, self.squash(attr)))
        })
    }

    fn remove<'a>(&'a self, dir: NodeKey, name: &'a [u8], is_dir: bool) -> MutFuture<'a, ()> {
        Box::pin(async move {
            let req = if is_dir {
                RpcReq::Rmdir {
                    share: ShareId(self.share_id),
                    dir,
                    name: name_of(name)?,
                }
            } else {
                RpcReq::Unlink {
                    share: ShareId(self.share_id),
                    dir,
                    name: name_of(name)?,
                }
            };
            // Conflict tracking: resolve the doomed node before it leaves the replica.
            let doomed = self.share.replica().lookup(dir, name).ok().map(|(n, _)| n);
            self.mutate_ok(req).await?;
            if let Some(n) = doomed {
                self.vm.conflicts.note_mac(
                    self.share_id,
                    n,
                    self.share.replica().path_of(n),
                    "remove",
                );
                self.invalidate_cas(n);
                // Drop any write-behind state for the unlinked node now (don't wait for the
                // reaper): editor save-via-rename unlinks the old inode after writing it
                // UNSTABLE, and that queue + its stashed payloads would otherwise linger.
                // ONE lock (parking_lot is not reentrant — a second `.lock()` while the guard
                // from the `if let` is still alive deadlocks the whole daemon).
                {
                    let mut w = self.writes.lock();
                    if w.get(&n)
                        .is_some_and(|q| q.inflight.load(AtomicOrdering::Acquire) == 0)
                    {
                        w.remove(&n);
                    }
                }
            }
            self.share.replica().apply_rec(&Rec::Removed {
                parent: dir,
                name: name_of(name)?,
            });
            self.bump_dir(dir);
            Ok(())
        })
    }

    fn rename<'a>(
        &'a self,
        from_dir: NodeKey,
        from_name: &'a [u8],
        to_dir: NodeKey,
        to_name: &'a [u8],
    ) -> MutFuture<'a, ()> {
        Box::pin(async move {
            let req = RpcReq::Rename {
                share: ShareId(self.share_id),
                from_dir,
                from_name: name_of(from_name)?,
                to_dir,
                to_name: name_of(to_name)?,
            };
            self.mutate_ok(req).await?;
            self.share.replica().apply_rec(&Rec::Renamed {
                from_parent: from_dir,
                from_name: name_of(from_name)?,
                to_parent: to_dir,
                to_name: name_of(to_name)?,
            });
            self.bump_dir(from_dir);
            if to_dir != from_dir {
                self.bump_dir(to_dir);
            }
            Ok(())
        })
    }

    fn setattr(&self, node: NodeKey, set: NfsSetAttr) -> MutFuture<'_, Attr> {
        Box::pin(async move {
            // Truncate must not race queued writes (a late write would resurrect the tail).
            if set.size.is_some()
                && let Some(e) = self.drain_writes(node).await
            {
                return Err(e);
            }
            let req = RpcReq::SetAttr {
                share: ShareId(self.share_id),
                node,
                mode: set.mode,
                uid: set.uid,
                gid: set.gid,
                size: set.size,
                mtime: set.mtime,
            };
            let truncating = set.size.is_some();
            let mut attr = self.mutate_attr(req).await?;
            self.vm.note_mac_dirty(self.share_id, node);
            self.vm.conflicts.note_mac(
                self.share_id,
                node,
                self.share.replica().path_of(node),
                "setattr",
            );
            if truncating {
                self.invalidate_cas(node);
            }
            // Monotonic mtime (guest clock lags our optimistic stamps; a backward step reads
            // as a foreign change). Size stays authoritative — truncate must shrink.
            if let Ok(cur) = self.share.replica().getattr(node)
                && (attr.mtime.sec, attr.mtime.nsec) < (cur.mtime.sec, cur.mtime.nsec)
            {
                attr.mtime = cur.mtime;
            }
            self.share.replica().apply_rec(&Rec::AttrChanged {
                node,
                attr: attr.clone(),
            });
            Ok(self.squash(attr))
        })
    }

    fn write<'a>(
        &'a self,
        node: NodeKey,
        offset: u64,
        data: &'a [u8],
        sync: bool,
    ) -> MutFuture<'a, Attr> {
        Box::pin(async move {
            if sync || data.is_empty() {
                // FILE_SYNC write: drain pending unstable batches first (durability covers
                // them too), then write through. A single-chunk write carries sync inline —
                // ONE guest RPC (pwrite+fdatasync) instead of write+Commit; this is the
                // small-file create path, where the saved round trip is most of the budget.
                if let Some(e) = self.drain_writes(node).await {
                    return Err(e);
                }
                let attr = if data.len() <= WRITE_CHUNK && !data.is_empty() {
                    self.mutate_attr(RpcReq::Write {
                        share: ShareId(self.share_id),
                        node,
                        off: offset,
                        sync: true,
                        data: data.to_vec(),
                    })
                    .await?
                } else {
                    self.write_through(node, offset, data).await?;
                    self.mutate_attr(RpcReq::Commit {
                        share: ShareId(self.share_id),
                        node,
                    })
                    .await?
                };
                self.note_write(node);
                let attr = self.apply_mac_attr(node, attr);
                return Ok(self.squash(attr));
            }

            // UNSTABLE write (the bulk path): validate against the replica, queue the batch,
            // reply NOW with an optimistic attr — kernel-nfsd async-write parity. The guest
            // sees this node's batches strictly in arrival order (successive client flushes
            // may rewrite overlapping ranges; reordering would persist stale bytes); COMMIT
            // is the durability + error barrier.
            let mut attr = self.share.replica().getattr(node).map_err(map_err)?;
            // Backpressure BEFORE registering the batch: bound retained write-behind payloads.
            // The permit is held by the spawned task until the guest applies this batch, so the
            // permit count == in-flight batches. Awaiting here throttles the client's WRITE
            // reply when the guest can't keep up — without it the instant-ack write-behind lets
            // the client outrun the guest and pile up `Arc<Vec<u8>>` payloads without bound.
            let permit = self
                .write_slots
                .clone()
                .acquire_owned()
                .await
                .expect("write_slots semaphore is never closed");
            let q = self.node_writes(node);
            *q.last_active.lock() = Some(std::time::Instant::now());
            q.inflight.fetch_add(1, AtomicOrdering::AcqRel);
            q.dirty.store(true, AtomicOrdering::Release);
            let batch_id = q.next_batch_id.fetch_add(1, AtomicOrdering::Relaxed);
            q.pending_ranges
                .lock()
                .push((offset, data.len() as u64, batch_id));
            let me = self.detached();
            let buf = Arc::new(data.to_vec());
            {
                // Keep a zero-copy reference for CAS ingest at COMMIT; drop the whole stash
                // if it grows past the cap (only the warm-read shortcut is lost).
                let mut stash = q.stash.lock();
                if let Some(v) = stash.as_mut() {
                    let total: u64 = v.iter().map(|(_, b)| b.len() as u64).sum();
                    if total + data.len() as u64 > STASH_MAX_BYTES {
                        *stash = None;
                    } else {
                        v.push((offset, buf.clone()));
                    }
                }
            }
            // Per-node ordering: this batch applies to the guest only after the previous one
            // finished, so overlapping writes land in arrival order (the client serializes
            // overlapping writes on our ack, so this is its wire order). Established here in the
            // synchronous prefix — which, for client-serialized overlapping writes, runs in
            // order because the client doesn't send the next until this one's reply.
            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            let prev_done = q.chain_tail.lock().replace(done_rx);
            let qq = q.clone();
            tokio::spawn(async move {
                let _permit = permit; // released when this batch finishes → frees a slot
                // Wait for the previous batch to land first (Err = its task died; proceed).
                if let Some(prev) = prev_done {
                    let _ = prev.await;
                }
                let n = buf.len() as u64;
                let mut presync = false;
                let write_res = me.write_through(node, offset, &buf).await;
                // Signal the next batch it may apply now (ordering barrier), regardless of result.
                let _ = done_tx.send(());
                if let Err(e) = write_res {
                    let mut slot = qq.err.lock();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                } else if qq.presync.fetch_add(n, AtomicOrdering::AcqRel) + n >= PRESYNC_BYTES {
                    qq.presync.store(0, AtomicOrdering::Release);
                    presync = true;
                }
                // Unregister BEFORE any advisory flush: a COMMIT's drain must never wait on
                // an advisory fdatasync (that re-couples exactly what pre-sync decouples).
                qq.pending_ranges
                    .lock()
                    .retain(|(_, _, id)| *id != batch_id);
                let now_idle = qq.inflight.fetch_sub(1, AtomicOrdering::AcqRel) == 1;
                qq.drained.notify_waiters();
                // CAS warm-write ingest at the write-idle boundary, VOLATILE (no fsync):
                // durable ingest here contended with the read that typically follows; the
                // volatile form is ~40 ms of hashing + page-cache writes, done before any
                // realistic close-to-reopen read. Durability lands via the delayed persist.
                if now_idle {
                    let me2 = me.detached();
                    let qq2 = qq.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                        if qq2.inflight.load(AtomicOrdering::Acquire) == 0 {
                            me2.finalize_stash(node, &qq2);
                        }
                    });
                }
                if presync {
                    // Advisory incremental flush; errors surface at the real COMMIT.
                    let _ = me
                        .mutate_attr(RpcReq::Commit {
                            share: ShareId(me.share_id),
                            node,
                        })
                        .await;
                }
            });

            // Optimistic replica update: size grows, mtime moves MONOTONICALLY (a backward
            // step reads as a foreign change and triggers client cache invalidation + RMW).
            // The CAS is invalidated explicitly (note_write); the journal echo gets absorbed
            // (vm.mac_dirty) so it can't flap the attrs either.
            attr.size = attr.size.max(offset + data.len() as u64);
            // Globally-unique optimistic mtime: distinct writes (even concurrent ones to the
            // same file) get distinct timestamps strictly above the current mtime, so the CAS
            // fingerprint never collides across contents and never steps backward.
            attr.mtime = next_optimistic_ts(attr.mtime);
            self.share.replica().apply_rec(&Rec::AttrChanged {
                node,
                attr: attr.clone(),
            });
            self.note_write(node);
            Ok(self.squash(attr))
        })
    }

    fn commit(&self, node: NodeKey, off: u64, len: u64) -> MutFuture<'_, Attr> {
        Box::pin(async move {
            let t0 = std::time::Instant::now();
            let drain_len = if len == 0 { u64::MAX } else { len };
            if let Some(e) = self.drain_writes_range(node, off, drain_len).await {
                return Err(e); // a background write failed: client re-sends FILE_SYNC
            }
            let drain_us = t0.elapsed().as_micros() as u64;
            tracing::debug!(drain_us, off, len, "commit drain");
            // Clean full commit = no-op. A COMMIT only owes durability for UNSTABLE data this
            // server still holds; FILE_SYNC writes were fdatasync'd inline, and a node with no
            // (or fully committed) write-behind state has nothing to flush. The macOS client
            // commits twice around CLOSE — both were pure guest-fdatasync latency.
            if len == 0 {
                let dirty = self
                    .writes
                    .lock()
                    .get(&node)
                    .is_some_and(|q| q.dirty.swap(false, AtomicOrdering::AcqRel));
                if !dirty {
                    let attr = self.share.replica().getattr(node).map_err(map_err)?;
                    return Ok(self.squash(attr));
                }
            }
            // Snapshot (don't take) the stash on full commits: the client commits mid-stream
            // too, and stealing the stash early leaves later chunks uncached. Earlier ingests
            // under a not-yet-final fingerprint are swept by the manifest version cleanup.
            let stash = if len == 0 {
                self.writes
                    .lock()
                    .get(&node)
                    .and_then(|q| q.stash.lock().clone())
            } else {
                None
            };
            let attr = match self
                .mutate_attr(RpcReq::CommitRange {
                    share: ShareId(self.share_id),
                    node,
                    off,
                    len,
                })
                .await
            {
                Ok(a) => a,
                Err(e) => {
                    // The durability barrier failed: the node is NOT clean.
                    if let Some(q) = self.writes.lock().get(&node) {
                        q.dirty.store(true, AtomicOrdering::Release);
                    }
                    return Err(e);
                }
            };
            self.vm.note_mac_dirty(self.share_id, node);
            let attr = self.apply_mac_attr(node, attr);
            // Warm the CAS from the stashed write payloads now that the fingerprint is
            // settled — a read-back of the file then runs at cache speed with zero guest I/O
            // (kernel-nfsd page-cache parity). Off the reply path.
            if stash.is_some()
                && let Some(q) = self.writes.lock().get(&node).cloned()
            {
                self.finalize_stash(node, &q);
            }
            // Always reclaim the NodeWrites entry once the node goes quiet — even when the
            // stash was abandoned over-cap (the entry, its `ingested` set and `pending_ranges`
            // would otherwise persist forever for that node, leaking per distinct file).
            let me = self.detached();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let mut map = me.writes.lock();
                if let Some(q) = map.get(&node)
                    && q.inflight.load(AtomicOrdering::Acquire) == 0
                {
                    map.remove(&node);
                }
            });
            Ok(self.squash(attr))
        })
    }
}

/// Chunk-assemble the stash: returns CAS-chunk-aligned buffers fully covered by stashed
/// writes. `size` bounds the tail (None = whole chunks only, the streaming case).
fn covered_chunks(stash: &[(u64, Arc<Vec<u8>>)], size: Option<u64>) -> Vec<(u64, Vec<u8>)> {
    let max_end = stash
        .iter()
        .map(|(o, d)| o + d.len() as u64)
        .max()
        .unwrap_or(0);
    let end = size.unwrap_or(max_end).min(max_end);
    let mut out = Vec::new();
    let nchunks = end.div_ceil(CAS_CHUNK);
    for c in 0..nchunks {
        let c_off = c * CAS_CHUNK;
        let c_len = if let Some(sz) = size {
            (CAS_CHUNK.min(sz - c_off)) as usize
        } else {
            if c_off + CAS_CHUNK > end {
                continue; // partial tail only assembled when the final size is known
            }
            CAS_CHUNK as usize
        };
        let mut buf = vec![0u8; c_len];
        let mut covered = 0usize;
        for (off, data) in stash {
            let (off, dlen) = (*off, data.len() as u64);
            if off + dlen <= c_off || off >= c_off + c_len as u64 {
                continue;
            }
            let s = off.max(c_off);
            let e = (off + dlen).min(c_off + c_len as u64);
            buf[(s - c_off) as usize..(e - c_off) as usize]
                .copy_from_slice(&data[(s - off) as usize..(e - off) as usize]);
            covered += (e - s) as usize;
        }
        if covered == c_len {
            out.push((c_off, buf));
        }
    }
    out
}

fn now_ts() -> Ts {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Ts {
        sec: d.as_secs() as i64,
        nsec: d.subsec_nanos(),
    }
}

/// A process-globally-unique, monotonically increasing optimistic timestamp (nanoseconds since
/// epoch), guaranteed strictly greater than `floor` (the node's current mtime, so it never
/// steps backward). The per-node `read base mtime → +1ns` it replaces races: the macOS client
/// pipelines several concurrent UNSTABLE writes to one file, two `write()` calls read the same
/// base and both produce `base+1ns` → an IDENTICAL (mtime,size) for DIFFERENT content. Since
/// the CAS fingerprint is `FNV(mtime,nsec,size)`, that collision lets a read serve a stale
/// chunk (observed as an fsx data mismatch under load). One global counter, floored at both
/// wall clock and `floor`, gives every write a distinct timestamp → a distinct fingerprint.
fn next_optimistic_ts(floor: Ts) -> Ts {
    use std::sync::atomic::AtomicU64;
    static CLOCK: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let floor_nanos = (floor.sec.max(0) as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(floor.nsec as u64);
    let base = now.max(floor_nanos);
    let mut prev = CLOCK.load(AtomicOrdering::Relaxed);
    let next = loop {
        let want = base.max(prev) + 1; // strictly greater than both `base` and any prior tick
        match CLOCK.compare_exchange_weak(
            prev,
            want,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Relaxed,
        ) {
            Ok(_) => break want,
            Err(cur) => prev = cur,
        }
    };
    Ts {
        sec: (next / 1_000_000_000) as i64,
        nsec: (next % 1_000_000_000) as u32,
    }
}

fn name_of(name: &[u8]) -> NfsResult<Name> {
    Name::new(name.to_vec()).map_err(|_| NfsError::Io)
}

/// Await all futures, preserving order (std-only join_all; avoids the `futures` crate).
async fn futures_join_all<F: std::future::Future>(futs: Vec<F>) -> Vec<F::Output> {
    let mut futs: Vec<_> = futs.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (i, f) in futs.iter_mut().enumerate() {
            if out[i].is_none() {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => out[i] = Some(v),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    out.into_iter().map(|v| v.unwrap()).collect()
}

fn map_rpc_resp_err(resp: &RpcResp) -> Option<NfsError> {
    if let RpcResp::Err(e) = resp {
        Some(NfsError::from_errno(e.errno))
    } else {
        None
    }
}

/// CAS chunk granularity (design 04 §6): guest fetches are aligned to this, so a warm cache
/// serves any read pattern from at most ⌈count/4 MiB⌉+1 blobs.
const CAS_CHUNK: u64 = 4 * 1024 * 1024;

/// Inline Write RPC chunk: one full NFS wsize write per RPC (the frame cap is 2 MiB).
const WRITE_CHUNK: usize = 1024 * 1024;

/// CAS version key: an FNV mix of (mtime, size). Unlike the journal's in-memory write counter
/// (which restarts at 0 on reseed and would cold the cache on every hostd restart), the stat
/// fingerprint is stable across restarts/reseeds for unchanged files — that's what makes the
/// warm-after-restart gate (zero guest reads) possible. Any stat-visible content change
/// (journal Content recs carry mtime+size) rotates the key; same-nanosecond same-size rewrites
/// are additionally covered by the explicit Mac-write invalidation.
fn content_fingerprint(a: &Attr) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    // content_version is the guest journal's per-node write counter — folding it in rotates the
    // CAS key on a guest in-place write even when mtime+size are unchanged (sub-nanosecond
    // same-size overwrite). Mac writes preserve content_version and rely on the unique mtime.
    for v in [
        a.mtime.sec as u64,
        a.mtime.nsec as u64,
        a.size,
        a.content_version,
    ] {
        h ^= v;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

impl ShareSurface {
    /// Plain guest RPC read, counting toward the guest-read metrics (the warm-cache gate
    /// asserts these stay at 0).
    async fn guest_read(
        &self,
        node: NodeKey,
        offset: u64,
        count: u64,
    ) -> NfsResult<(Vec<u8>, bool)> {
        let rpc = self.vm.rpc().ok_or(NfsError::Jukebox)?; // not connected → client retries
        let (data, eof) = rpc
            .read(ShareId(self.share_id), node, offset, count as u32)
            .await
            .map_err(|_| NfsError::Io)?;
        self.vm.guest_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
        self.vm
            .guest_read_bytes
            .fetch_add(data.len() as u64, AtomicOrdering::Relaxed);
        Ok((data, eof))
    }

    /// Manifest lookup through the in-RAM LRU (fingerprint-keyed → self-invalidating).
    fn manifest(
        &self,
        cas: &mist_cas::CasStore,
        key: mist_cas::ManifestKey,
    ) -> Option<Arc<Vec<mist_cas::ChunkRef>>> {
        if let Some(m) = self.manifests.lock().get(&key) {
            return Some(m.clone());
        }
        let m = Arc::new(cas.get_manifest(key).ok()??);
        let mut cache = self.manifests.lock();
        if cache.len() >= 256 {
            cache.clear(); // crude but rare; entries rebuild on demand
        }
        cache.insert(key, m.clone());
        Some(m)
    }

    /// Serve `want` bytes at `inner` within the chunk at `chunk_off`: CAS hit → local pread of
    /// just the requested range; miss → fetch the whole aligned chunk from the guest, ingest it
    /// write-through, kick a prefetch of the next chunk, and serve the slice.
    #[allow(clippy::too_many_arguments)]
    async fn chunk_read(
        &self,
        cas: &Arc<mist_cas::CasStore>,
        key: mist_cas::ManifestKey,
        node: NodeKey,
        chunk_off: u64,
        chunk_len: u64,
        inner: u64,
        want: u64,
        file_size: u64,
    ) -> NfsResult<Vec<u8>> {
        // Hit path stays inline: cached manifest + cached blob fd + positional read into a
        // pooled (page-warm) buffer — no open/seek/alloc per READ.
        if let Some(m) = self.manifest(cas, key)
            && let Some(c) = m.iter().find(|c| c.off == chunk_off)
            && let Some((file, abs_off)) = cas.blob_file_range(&c.hash, inner as u32, want as u32)
        {
            use std::os::unix::fs::FileExt;
            let mut buf = mist_nfs::bufpool::take(want as usize);
            buf.resize(want as usize, 0); // pooled pages are already faulted in
            if file
                .read_at(&mut buf, abs_off)
                .is_ok_and(|n| n == want as usize)
            {
                return Ok(buf);
            }
            mist_nfs::bufpool::give(buf);
        }
        // Miss: single-flight per (node, version, chunk) so concurrent readers of the same cold
        // chunk produce one guest fetch.
        let guard = self.vm.fetch_guard(key, chunk_off).await;
        let _guard = guard.lock().await;
        if let Ok(Some(m)) = cas.get_manifest(key)
            && let Some(c) = m.iter().find(|c| c.off == chunk_off)
            && let Ok(Some(buf)) = cas.get_blob_range(&c.hash, inner as u32, want as u32, false)
        {
            return Ok(buf); // raced: another reader ingested it while we waited
        }
        if let Ok(a) = self.share.replica().getattr(node) {
            tracing::debug!(
                ino = node.ino,
                generation = node.generation,
                fp = key.content_version,
                mtime_s = a.mtime.sec,
                mtime_ns = a.mtime.nsec,
                size = a.size,
                chunk_off,
                "cas chunk miss; fetching from guest"
            );
        }
        let (data, _eof) = self.guest_read(node, chunk_off, chunk_len).await?;
        let slice_end = data.len().min((inner + want) as usize);
        let piece = data
            .get(inner as usize..slice_end)
            .map(|s| s.to_vec())
            .unwrap_or_default();
        // Ingest write-through only when the chunk is complete (a short read means the file
        // changed; the new content_version will refetch it). Await the ingest while still
        // holding the single-flight guard: otherwise the next read of this chunk lands between
        // fetch and manifest commit and refetches the whole chunk from the guest.
        if data.len() as u64 == chunk_len {
            let cas2 = cas.clone();
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = cas2.ingest_chunk(key, chunk_off, &data) {
                    tracing::warn!(error = %e, "cas ingest failed");
                }
            })
            .await;
            self.prefetch(cas.clone(), key, node, chunk_off + CAS_CHUNK, file_size);
        }
        Ok(piece)
    }

    /// Background readahead of the next chunk (best-effort, single-flight guarded).
    fn prefetch(
        &self,
        cas: Arc<mist_cas::CasStore>,
        key: mist_cas::ManifestKey,
        node: NodeKey,
        chunk_off: u64,
        file_size: u64,
    ) {
        if chunk_off >= file_size {
            return;
        }
        let vm = self.vm.clone();
        let share_id = self.share_id;
        let chunk_len = CAS_CHUNK.min(file_size - chunk_off);
        tokio::spawn(async move {
            if let Ok(Some(m)) = cas.get_manifest(key)
                && m.iter().any(|c| c.off == chunk_off)
            {
                return; // already cached
            }
            let guard = vm.fetch_guard(key, chunk_off).await;
            let Ok(_g) = guard.try_lock() else {
                return; // a foreground read is already fetching it
            };
            let Some(rpc) = vm.rpc() else { return };
            let Ok((data, _)) = rpc
                .read(ShareId(share_id), node, chunk_off, chunk_len as u32)
                .await
            else {
                return;
            };
            vm.guest_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
            vm.guest_read_bytes
                .fetch_add(data.len() as u64, AtomicOrdering::Relaxed);
            if data.len() as u64 == chunk_len {
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = cas.ingest_chunk(key, chunk_off, &data);
                })
                .await;
            }
        });
    }

    /// Ingest stash-covered 4 MiB chunks under the WIP manifest key (idempotent per chunk).
    /// Currently unused: mid-stream/idle ingest contends with the live write or the read
    /// that follows (measured); kept for background-ingest tuning.
    #[allow(dead_code)]
    fn ingest_ready_chunks(&self, node: NodeKey, q: &Arc<NodeWrites>) {
        let Some(cas) = self.vm.cas.clone() else {
            return;
        };
        let ready: Vec<(u64, Vec<u8>)> = {
            let stash = q.stash.lock();
            let Some(stash) = stash.as_ref() else { return };
            let mut ingested = q.ingested.lock();
            covered_chunks(stash, None)
                .into_iter()
                .filter(|(off, _)| ingested.insert(*off))
                .collect()
        };
        if ready.is_empty() {
            return;
        }
        let key = mist_cas::ManifestKey {
            share: self.share_id,
            ino: node.ino,
            generation: node.generation,
            content_version: WIP_FP,
        };
        tokio::task::spawn_blocking(move || {
            let _ = cas.ingest_chunks(key, ready);
        });
    }

    /// Final ingest: the tail (partial) chunk plus a rebind of the WIP manifest to the settled
    /// fingerprint. Called at full COMMIT and at write-idle (writeback shares never commit).
    fn finalize_stash(&self, node: NodeKey, q: &Arc<NodeWrites>) {
        let Some(cas) = self.vm.cas.clone() else {
            return;
        };
        let Ok(attr) = self.share.replica().getattr(node) else {
            return;
        };
        let size = attr.size;
        // Ingest the FULL current covered content (no per-offset dedup): `covered_chunks`
        // rebuilds the live content of each chunk from the stash, so a chunk rewritten after an
        // earlier ingest MUST be re-ingested — skipping it (the old `ingested` filter) bound the
        // settled fingerprint to a stale blob. ingest_chunks replaces the WIP manifest with this
        // full set, then the rebind moves it to the settled key.
        let tail: Vec<(u64, Vec<u8>)> = {
            let stash = q.stash.lock();
            let Some(stash) = stash.as_ref() else { return };
            covered_chunks(stash, Some(size))
        };
        let wip = mist_cas::ManifestKey {
            share: self.share_id,
            ino: node.ino,
            generation: node.generation,
            content_version: WIP_FP,
        };
        let fin = mist_cas::ManifestKey {
            content_version: content_fingerprint(&attr),
            ..wip
        };
        let persist_pending = self.vm.cas_persist_pending.clone();
        tokio::spawn(async move {
            // Volatile: visible to reads immediately, no fsync to contend with them.
            let cas2 = cas.clone();
            let _ = tokio::task::spawn_blocking(move || {
                if !tail.is_empty() {
                    let _ = cas2.ingest_chunks_volatile(wip, tail);
                }
                let _ = cas2.rebind_manifest(wip, fin);
            })
            .await;
            // Debounced durability: one delayed fsync covers every volatile commit so far.
            if !persist_pending.swap(true, AtomicOrdering::AcqRel) {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                persist_pending.store(false, AtomicOrdering::Release);
                let _ = tokio::task::spawn_blocking(move || cas.persist()).await;
            }
        });
    }

    /// Owned copy for write-behind tasks (the trait hands out `&self`; every field is an
    /// Arc/Copy, and the detached copy shares the same VM + share handles).
    fn detached(&self) -> ShareSurface {
        ShareSurface {
            vm: self.vm.clone(),
            share: self.share.clone(),
            share_id: self.share_id,
            root: self.root,
            squash_uid: self.squash_uid,
            squash_gid: self.squash_gid,
            // Share the live state (not fresh empties): the cleanup task removes from THIS map.
            writes: self.writes.clone(),
            manifests: self.manifests.clone(),
            write_slots: self.write_slots.clone(),
        }
    }

    /// Chunked write to the guest (rpc-lane frame cap is 1 MiB; 512 KiB inline chunks fly
    /// concurrently — disjoint ranges within one call).
    async fn write_through(&self, node: NodeKey, offset: u64, data: &[u8]) -> NfsResult<()> {
        const CHUNK: usize = WRITE_CHUNK;
        let rpc = self.vm.rpc_write().ok_or(NfsError::Jukebox)?;
        let mut futs = Vec::new();
        let mut off = offset;
        for chunk in data.chunks(CHUNK) {
            let req = RpcReq::Write {
                share: ShareId(self.share_id),
                node,
                off,
                sync: false,
                data: chunk.to_vec(),
            };
            let rpc = rpc.clone();
            futs.push(async move {
                match rpc.call(req).await {
                    Ok(RpcResp::Attr(_)) | Ok(RpcResp::Ok) => Ok(()),
                    Ok(RpcResp::Err(e)) => Err(NfsError::from_errno(e.errno)),
                    Ok(_) => Err(NfsError::Io),
                    Err(e) => {
                        tracing::warn!(error = %e, "bulk write rpc transport error");
                        Err(NfsError::Jukebox)
                    }
                }
            });
            off += chunk.len() as u64;
        }
        for r in futures_join_all(futs).await {
            r?; // any chunk failure fails the batch (surfaced at COMMIT for unstable writes)
        }
        Ok(())
    }

    /// Apply guest-returned attrs to the replica with MONOTONIC clamping: the guest's clock
    /// runs slightly behind our optimistic timestamps, and any backward mtime/size step reads
    /// to the macOS client as a foreign change — cache invalidation + read-modify-write storm
    /// (measured: every 1 MiB of a dd got read back and written twice).
    fn apply_mac_attr(&self, node: NodeKey, mut attr: Attr) -> Attr {
        if let Ok(cur) = self.share.replica().getattr(node) {
            if (attr.mtime.sec, attr.mtime.nsec) < (cur.mtime.sec, cur.mtime.nsec) {
                attr.mtime = cur.mtime;
            }
            attr.size = attr.size.max(cur.size);
        }
        self.share.replica().apply_rec(&Rec::AttrChanged {
            node,
            attr: attr.clone(),
        });
        attr
    }

    /// Optimistically advance a directory's mtime after a Mac-side namespace op and fence its
    /// journal echo. The v4 change_info computed from the replica then moves exactly once per
    /// op (atomic), and the echo can't flap it afterwards — without this the macOS client
    /// re-validated the whole directory after every create.
    fn bump_dir(&self, dir: NodeKey) {
        if let Ok(mut a) = self.share.replica().getattr(dir) {
            let now = now_ts();
            a.mtime = if (now.sec, now.nsec) > (a.mtime.sec, a.mtime.nsec) {
                now
            } else {
                Ts {
                    sec: a.mtime.sec,
                    nsec: a.mtime.nsec + 1,
                }
            };
            a.ctime = a.mtime;
            self.share
                .replica()
                .apply_rec(&Rec::AttrChanged { node: dir, attr: a });
        }
        self.vm.note_mac_dirty(self.share_id, dir);
    }

    /// Conflict-log + CAS invalidation + echo-fence bookkeeping shared by both write paths.
    fn note_write(&self, node: NodeKey) {
        self.vm.note_mac_dirty(self.share_id, node);
        self.vm.conflicts.note_mac(
            self.share_id,
            node,
            self.share.replica().path_of(node),
            "write",
        );
        self.invalidate_cas(node);
    }

    /// Drop the node's manifests after a Mac-side content mutation (write/truncate/remove).
    /// Cheap when the node was never read (read-only existence check in the CAS).
    fn invalidate_cas(&self, node: NodeKey) {
        if let Some(cas) = self.vm.cas.clone() {
            let share = self.share_id;
            tokio::task::spawn_blocking(move || {
                if let Err(e) = cas.drop_node(share, node.ino, node.generation) {
                    tracing::warn!(error = %e, "cas invalidate failed");
                }
            });
        }
    }

    /// Issue a mutation RPC; on transport failure → Jukebox (client retries), on guest errno →
    /// the mapped NFS error.
    async fn mutate(&self, req: RpcReq) -> NfsResult<RpcResp> {
        let rpc = self.vm.rpc().ok_or(NfsError::Jukebox)?;
        let op = match &req {
            RpcReq::Create { .. } => "create",
            RpcReq::Write { .. } => "write",
            RpcReq::Commit { .. } => "commit",
            RpcReq::SetAttr { .. } => "setattr",
            RpcReq::Unlink { .. } | RpcReq::Rmdir { .. } => "remove",
            RpcReq::Rename { .. } => "rename",
            _ => "other",
        };
        let t0 = std::time::Instant::now();
        // Jukebox makes the client retry — right for transient disconnects, but log it: a
        // *persistent* transport error here would otherwise be an invisible infinite retry loop
        // (exactly how the >MAX_FRAME inline-write bug presented).
        let resp = rpc.call(req).await;
        tracing::debug!(
            op,
            us = t0.elapsed().as_micros() as u64,
            "guest mutation rtt"
        );
        let resp = resp.map_err(|e| {
            tracing::warn!(vm = %self.vm.name, error = %e, "mutation rpc transport error (client will retry)");
            NfsError::Jukebox
        })?;
        if let RpcResp::Err(e) = &resp {
            tracing::warn!(errno = e.errno, msg = %e.msg, "guest mutation failed");
        }
        if let Some(e) = map_rpc_resp_err(&resp) {
            return Err(e);
        }
        Ok(resp)
    }

    async fn mutate_ok(&self, req: RpcReq) -> NfsResult<()> {
        match self.mutate(req).await? {
            RpcResp::Ok => Ok(()),
            _ => Err(NfsError::Io),
        }
    }

    async fn mutate_attr(&self, req: RpcReq) -> NfsResult<Attr> {
        match self.mutate(req).await? {
            RpcResp::Attr(a) => Ok(a),
            _ => Err(NfsError::Io),
        }
    }

    async fn mutate_entry(&self, req: RpcReq) -> NfsResult<(NodeKey, Attr)> {
        match self.mutate(req).await? {
            RpcResp::Entry { node, attr } => Ok((node, attr)),
            _ => Err(NfsError::Io),
        }
    }
}
