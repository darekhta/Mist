//! Side-store: Apple metadata virtualization (design 04 §7, 05 §6).
//!
//! Finder/editor noise (`.DS_Store`, AppleDouble `._*`, `.fseventsd`, …) never reaches the guest:
//! a [`SideStoreSurface`] decorator intercepts those names before the inner [`MountSurface`] and
//! serves them from a small host-side persistent store. Default profile: hidden from `readdir`
//! listings, served on direct lookup — Finder is satisfied, `ls` stays clean, the guest tree is
//! never polluted. Rule: the side-store only owns names *absent* in the guest; if the replica has
//! a real entry of the same name, the real one wins and the op passes through.
//!
//! Synthesized nodes use ino with bit 63 set (real ext4 FILEID_INO32_GEN inos are ≤ u32::MAX), so
//! they round-trip through NFS handles without colliding with real NodeKeys.

use mist_nfs::{
    CreateKind, DirEntry, FsStat, MountSurface, MutFuture, NfsError, NfsResult, ReadDirPage,
    ReadFuture, ReadResult, SetAttr,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Synthesized-node marker bit. Real ext4 ino32 inos can't reach it.
const SIDE_INO_BIT: u64 = 1 << 63;
const SIDE_GEN: u32 = u32::MAX;
/// Per-row data cap (matches design 04 §7).
const MAX_ROW: usize = 1024 * 1024;
/// Total store cap; oldest rows evicted beyond this.
const MAX_TOTAL: usize = 64 * 1024 * 1024;
/// Orphaned AppleDouble rows (anchor gone) survive at least this long (write-order races).
const LINGER_SECS: i64 = 60;

/// Is this a name the side-store owns (when no real guest entry exists)?
pub fn is_side_name(name: &[u8]) -> bool {
    name == b".DS_Store"
        || name.starts_with(b"._")
        || name == b".fseventsd"
        || name == b".Spotlight-V100"
        || name == b".Trashes"
        || name == b".TemporaryItems"
        || name == b".metadata_never_index"
        || name == b".hidden"
}

fn is_side_node(node: NodeKey) -> bool {
    node.ino & SIDE_INO_BIT != 0
}

fn synth_key(parent: NodeKey, name: &[u8]) -> NodeKey {
    let mut h = blake3::Hasher::new();
    h.update(&parent.ino.to_le_bytes());
    h.update(&parent.generation.to_le_bytes());
    h.update(name);
    let d = h.finalize();
    let mut b = [0u8; 8];
    b.copy_from_slice(&d.as_bytes()[..8]);
    NodeKey {
        ino: SIDE_INO_BIT | (u64::from_le_bytes(b) >> 1),
        generation: SIDE_GEN,
    }
}

fn now_sec() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Row {
    kind: u8, // 0 = Reg, 1 = Dir
    mode: u16,
    mtime_sec: i64,
    version: u64,
    data: Vec<u8>,
}

impl Row {
    fn kind(&self) -> Kind {
        if self.kind == 1 { Kind::Dir } else { Kind::Reg }
    }
}

type RowKey = (NodeKey, Vec<u8>); // (parent, name)

#[derive(Debug, Default)]
struct Inner {
    rows: HashMap<RowKey, Row>,
    by_node: HashMap<NodeKey, RowKey>,
}

/// Persistent store for one mounted share.
#[derive(Debug)]
pub struct SideStore {
    path: Option<PathBuf>,
    inner: Mutex<Inner>,
    /// Set by mutations; a debounced flusher persists ≤1×/500 ms. macOS creates an AppleDouble
    /// row per file it writes, so synchronous whole-store rewrites were a per-create tax
    /// (measured ~1 ms/file); sub-second loss of Finder-metadata rows on crash is acceptable.
    dirty: std::sync::atomic::AtomicBool,
}

impl SideStore {
    /// Load (or start empty). `path = None` keeps the store memory-only (tests).
    pub fn open(path: Option<PathBuf>) -> Self {
        let mut inner = Inner::default();
        if let Some(p) = &path
            && let Ok(bytes) = std::fs::read(p)
            && let Ok(rows) = postcard::from_bytes::<Vec<(NodeKey, Vec<u8>, Row)>>(&bytes)
        {
            for (parent, name, row) in rows {
                let node = synth_key(parent, &name);
                inner.by_node.insert(node, (parent, name.clone()));
                inner.rows.insert((parent, name), row);
            }
        }
        SideStore {
            path,
            inner: Mutex::new(inner),
            dirty: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Mark dirty; the flusher (or drop) writes the snapshot out.
    fn persist(&self, _inner: &Inner) {
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Write the current snapshot to disk if dirty. Called by the debounced flusher task.
    pub fn flush_if_dirty(&self) {
        if !self.dirty.swap(false, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let Some(p) = &self.path else { return };
        let rows: Vec<(NodeKey, Vec<u8>, Row)> = {
            let g = self.inner.lock();
            g.rows
                .iter()
                .map(|((parent, name), row)| (*parent, name.clone(), row.clone()))
                .collect()
        };
        if let Ok(bytes) = postcard::to_allocvec(&rows) {
            let tmp = p.with_extension("tmp");
            if std::fs::write(&tmp, &bytes).is_ok() {
                let _ = std::fs::rename(&tmp, p);
            }
        }
    }

    fn attr_for(row: &Row, uid: u32, gid: u32) -> Attr {
        Attr {
            kind: row.kind(),
            mode: row.mode,
            nlink: 1,
            uid,
            gid,
            size: row.data.len() as u64,
            blocks: (row.data.len() as u64).div_ceil(512),
            mtime: Ts {
                sec: row.mtime_sec,
                nsec: 0,
            },
            ctime: Ts {
                sec: row.mtime_sec,
                nsec: 0,
            },
            rdev: 0,
            content_version: row.version,
            symlink_target: None,
        }
    }
}

/// `MountSurface` decorator: side-store names handled host-side, everything else passes through.
#[derive(Debug)]
pub struct SideStoreSurface<S> {
    inner: Arc<S>,
    store: Arc<SideStore>,
    uid: u32,
    gid: u32,
}

impl Drop for SideStore {
    fn drop(&mut self) {
        self.flush_if_dirty(); // final snapshot on unmount/shutdown
    }
}

impl<S: MountSurface> SideStoreSurface<S> {
    pub fn new(inner: Arc<S>, store: SideStore) -> Self {
        // SAFETY: geteuid/getegid are always-succeeding scalar syscalls.
        #[allow(unsafe_code)]
        let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
        let s = SideStoreSurface {
            inner,
            store: Arc::new(store),
            uid,
            gid,
        };
        // Synthesize `.metadata_never_index` at the root so Spotlight never indexes the mount
        // even where `mdutil -i off` is ignored (design 05 §6).
        s.ensure_row(s.inner.root(), b".metadata_never_index", 0, 0o444);
        s
    }

    /// Handle for the debounced persistence flusher (weak-held so unmount stops it).
    pub fn store(&self) -> &Arc<SideStore> {
        &self.store
    }

    fn ensure_row(&self, parent: NodeKey, name: &[u8], kind: u8, mode: u16) {
        let mut g = self.store.inner.lock();
        let key = (parent, name.to_vec());
        if !g.rows.contains_key(&key) {
            let node = synth_key(parent, name);
            g.by_node.insert(node, key.clone());
            g.rows.insert(
                key,
                Row {
                    kind,
                    mode,
                    mtime_sec: now_sec(),
                    version: 1,
                    data: Vec::new(),
                },
            );
        }
    }

    /// True when the side-store should own (dir, name): the name matches and no real guest entry
    /// shadows it (real wins), or the parent itself is synthesized.
    fn owns(&self, dir: NodeKey, name: &[u8]) -> bool {
        if is_side_node(dir) {
            return true;
        }
        is_side_name(name) && self.inner.lookup(dir, name).is_err()
    }

    /// Drop orphaned rows: parents gone, or AppleDouble anchors gone past the linger window.
    fn gc_sweep(&self) {
        let now = now_sec();
        let mut g = self.store.inner.lock();
        let doomed: Vec<RowKey> = g
            .rows
            .iter()
            .filter(|((parent, name), row)| {
                if is_side_node(*parent) {
                    // Cascade: child of a synthesized dir whose row vanished.
                    return !g.by_node.contains_key(parent);
                }
                // Parent dir gone from the replica → all rows under it go.
                if matches!(
                    self.inner.getattr(*parent),
                    Err(NfsError::NoEnt | NfsError::Stale)
                ) {
                    return true;
                }
                // AppleDouble whose anchor file vanished (guest-side delete), past linger.
                if let Some(anchor) = name.strip_prefix(b"._")
                    && !anchor.is_empty()
                    && matches!(self.inner.lookup(*parent, anchor), Err(NfsError::NoEnt))
                    && now - row.mtime_sec > LINGER_SECS
                {
                    return true;
                }
                false
            })
            .map(|(k, _)| k.clone())
            .collect();
        if doomed.is_empty() {
            return;
        }
        for key in doomed {
            let node = synth_key(key.0, &key.1);
            g.by_node.remove(&node);
            g.rows.remove(&key);
        }
        self.store.persist(&g);
    }

    /// Evict oldest rows when over the total cap. Caller persists.
    fn evict_locked(g: &mut Inner) {
        let mut total: usize = g.rows.values().map(|r| r.data.len()).sum();
        while total > MAX_TOTAL {
            let Some(oldest) = g
                .rows
                .iter()
                .min_by_key(|(_, r)| r.mtime_sec)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(r) = g.rows.remove(&oldest) {
                total -= r.data.len();
            }
            g.by_node.remove(&synth_key(oldest.0, &oldest.1));
        }
    }

    fn row_attr(&self, node: NodeKey) -> NfsResult<Attr> {
        let g = self.store.inner.lock();
        let key = g.by_node.get(&node).ok_or(NfsError::Stale)?;
        let row = g.rows.get(key).ok_or(NfsError::Stale)?;
        Ok(SideStore::attr_for(row, self.uid, self.gid))
    }
}

impl<S: MountSurface> MountSurface for SideStoreSurface<S> {
    fn share_id(&self) -> u16 {
        self.inner.share_id()
    }

    fn root(&self) -> NodeKey {
        self.inner.root()
    }

    fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
        if is_side_node(node) {
            return self.row_attr(node);
        }
        self.inner.getattr(node)
    }

    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        if !is_side_node(dir)
            && is_side_name(name)
            && let Ok(real) = self.inner.lookup(dir, name)
        {
            return Ok(real); // real guest entry wins
        }
        if self.owns(dir, name) {
            let g = self.store.inner.lock();
            let key = (dir, name.to_vec());
            let row = g.rows.get(&key).ok_or(NfsError::NoEnt)?;
            return Ok((
                synth_key(dir, name),
                SideStore::attr_for(row, self.uid, self.gid),
            ));
        }
        self.inner.lookup(dir, name)
    }

    fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        max_entries: usize,
        want_attrs: bool,
    ) -> NfsResult<ReadDirPage> {
        if !is_side_node(dir) {
            // Default profile: side-store rows are hidden from listings.
            return self.inner.readdir(dir, cookie, max_entries, want_attrs);
        }
        // Listing of a synthesized directory (e.g. `.fseventsd`).
        let g = self.store.inner.lock();
        let mut names: Vec<(&RowKey, &Row)> =
            g.rows.iter().filter(|((p, _), _)| *p == dir).collect();
        names.sort_by(|a, b| a.0.1.cmp(&b.0.1));
        let mut entries = Vec::new();
        for (i, ((_, name), row)) in names.iter().enumerate() {
            let c = i as u64 + 3;
            if c <= cookie {
                continue;
            }
            if entries.len() >= max_entries {
                return Ok(ReadDirPage {
                    entries,
                    eof: false,
                    cookieverf: 1,
                });
            }
            entries.push(DirEntry {
                name: name.clone(),
                node: synth_key(dir, name),
                cookie: c,
                attr: want_attrs.then(|| SideStore::attr_for(row, self.uid, self.gid)),
            });
        }
        Ok(ReadDirPage {
            entries,
            eof: true,
            cookieverf: 1,
        })
    }

    fn readlink(&self, node: NodeKey) -> NfsResult<Vec<u8>> {
        if is_side_node(node) {
            return Err(NfsError::NotSymlink);
        }
        self.inner.readlink(node)
    }

    fn read(&self, node: NodeKey, offset: u64, count: u32) -> ReadFuture<'_> {
        if !is_side_node(node) {
            return self.inner.read(node, offset, count);
        }
        Box::pin(async move {
            let g = self.store.inner.lock();
            let key = g.by_node.get(&node).ok_or(NfsError::Stale)?;
            let row = g.rows.get(key).ok_or(NfsError::Stale)?;
            let s = (offset as usize).min(row.data.len());
            let e = row.data.len().min(s + count as usize);
            Ok(ReadResult {
                data: row.data[s..e].to_vec(),
                eof: e >= row.data.len(),
            })
        })
    }

    fn fsstat(&self) -> FsStat {
        self.inner.fsstat()
    }

    fn writable(&self) -> bool {
        self.inner.writable()
    }

    fn create<'a>(
        &'a self,
        dir: NodeKey,
        name: &'a [u8],
        kind: CreateKind,
        mode: u16,
    ) -> MutFuture<'a, (NodeKey, Attr)> {
        if !self.owns(dir, name) {
            return self.inner.create(dir, name, kind, mode);
        }
        Box::pin(async move {
            let row_kind = match kind {
                CreateKind::File { exclusive } => {
                    if exclusive
                        && self
                            .store
                            .inner
                            .lock()
                            .rows
                            .contains_key(&(dir, name.to_vec()))
                    {
                        return Err(NfsError::Exist);
                    }
                    0
                }
                CreateKind::Dir => 1,
                // No symlinks/devices in the metadata store.
                _ => return Err(NfsError::Access),
            };
            let node = synth_key(dir, name);
            {
                let mut g = self.store.inner.lock();
                let key = (dir, name.to_vec());
                g.by_node.insert(node, key.clone());
                let row = g.rows.entry(key).or_insert(Row {
                    kind: row_kind,
                    mode,
                    mtime_sec: now_sec(),
                    version: 0,
                    data: Vec::new(),
                });
                row.kind = row_kind;
                row.mtime_sec = now_sec();
                row.version += 1;
                self.store.persist(&g);
            }
            self.gc_sweep();
            self.inner.touch_dir(dir);
            let attr = self.row_attr(node)?;
            Ok((node, attr))
        })
    }

    fn remove<'a>(&'a self, dir: NodeKey, name: &'a [u8], is_dir: bool) -> MutFuture<'a, ()> {
        if self.owns(dir, name) {
            return Box::pin(async move {
                let mut g = self.store.inner.lock();
                let key = (dir, name.to_vec());
                let row = g.rows.get(&key).ok_or(NfsError::NoEnt)?;
                if is_dir {
                    if row.kind() != Kind::Dir {
                        return Err(NfsError::NotDir);
                    }
                    let me = synth_key(dir, name);
                    if g.rows.keys().any(|(p, _)| *p == me) {
                        return Err(NfsError::NotEmpty);
                    }
                }
                g.rows.remove(&key);
                g.by_node.remove(&synth_key(dir, name));
                self.store.persist(&g);
                drop(g);
                self.inner.touch_dir(dir);
                Ok(())
            });
        }
        Box::pin(async move {
            self.inner.remove(dir, name, is_dir).await?;
            // GC: the Mac deleted a real file — drop its AppleDouble companion immediately.
            if !is_side_name(name) {
                let mut companion = b"._".to_vec();
                companion.extend_from_slice(name);
                let mut g = self.store.inner.lock();
                if g.rows.remove(&(dir, companion.clone())).is_some() {
                    g.by_node.remove(&synth_key(dir, &companion));
                    self.store.persist(&g);
                }
            }
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
        let from_side = self.owns(from_dir, from_name);
        let to_side = self.owns(to_dir, to_name) || is_side_name(to_name);
        if !from_side && !to_side {
            return self.inner.rename(from_dir, from_name, to_dir, to_name);
        }
        if from_side != to_side {
            // Materializing a side row into the guest (or vice versa) is not supported.
            return Box::pin(async { Err(NfsError::Access) });
        }
        Box::pin(async move {
            let mut g = self.store.inner.lock();
            let from_key = (from_dir, from_name.to_vec());
            let row = g.rows.remove(&from_key).ok_or(NfsError::NoEnt)?;
            g.by_node.remove(&synth_key(from_dir, from_name));
            let to_key = (to_dir, to_name.to_vec());
            g.by_node.insert(synth_key(to_dir, to_name), to_key.clone());
            g.rows.insert(to_key, row);
            self.store.persist(&g);
            Ok(())
        })
    }

    fn setattr(&self, node: NodeKey, set: SetAttr) -> MutFuture<'_, Attr> {
        if !is_side_node(node) {
            return self.inner.setattr(node, set);
        }
        Box::pin(async move {
            {
                let mut g = self.store.inner.lock();
                let key = g.by_node.get(&node).cloned().ok_or(NfsError::Stale)?;
                let row = g.rows.get_mut(&key).ok_or(NfsError::Stale)?;
                if let Some(m) = set.mode {
                    row.mode = m;
                }
                if let Some(sz) = set.size {
                    if sz as usize > MAX_ROW {
                        return Err(NfsError::NoSpace);
                    }
                    row.data.resize(sz as usize, 0);
                }
                if let Some(t) = set.mtime {
                    row.mtime_sec = t.sec;
                } else if set.size.is_some() {
                    row.mtime_sec = now_sec();
                }
                row.version += 1;
                self.store.persist(&g);
            }
            self.row_attr(node)
        })
    }

    fn write<'a>(
        &'a self,
        node: NodeKey,
        offset: u64,
        data: &'a [u8],
        sync: bool,
    ) -> MutFuture<'a, Attr> {
        if !is_side_node(node) {
            return self.inner.write(node, offset, data, sync);
        }
        Box::pin(async move {
            {
                let mut g = self.store.inner.lock();
                let key = g.by_node.get(&node).cloned().ok_or(NfsError::Stale)?;
                let row = g.rows.get_mut(&key).ok_or(NfsError::Stale)?;
                let end = offset as usize + data.len();
                if end > MAX_ROW {
                    return Err(NfsError::NoSpace);
                }
                if row.data.len() < end {
                    row.data.resize(end, 0);
                }
                row.data[offset as usize..end].copy_from_slice(data);
                row.mtime_sec = now_sec();
                row.version += 1;
                Self::evict_locked(&mut g);
                self.store.persist(&g);
            }
            self.row_attr(node)
        })
    }

    fn writes_are_stable(&self) -> bool {
        self.inner.writes_are_stable()
    }

    fn read_sendable(
        &self,
        node: NodeKey,
        offset: u64,
        count: u32,
    ) -> mist_nfs::SendableFuture<'_> {
        if is_side_node(node) {
            return Box::pin(async { None });
        }
        self.inner.read_sendable(node, offset, count)
    }

    fn read_into<'a>(
        &'a self,
        node: NodeKey,
        offset: u64,
        count: u32,
        out: &'a mut Vec<u8>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<NfsResult<bool>>> + Send + 'a>>
    {
        if is_side_node(node) {
            return Box::pin(async { None });
        }
        self.inner.read_into(node, offset, count, out)
    }

    fn commit(&self, node: NodeKey, off: u64, len: u64) -> MutFuture<'_, Attr> {
        if !is_side_node(node) {
            return self.inner.commit(node, off, len);
        }
        Box::pin(async move { self.row_attr(node) })
    }
}
