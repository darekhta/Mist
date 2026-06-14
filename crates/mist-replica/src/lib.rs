//! Host-side metadata replica: the RAM mirror of one share's tree.
//!
//! Snapshot assembly, path lookup, readdir, and journal application all converge here. The
//! assembler starts in `Seeding`, applies `SnapDir` batches, then transitions to `Live` on
//! `SnapDone`; journal records are idempotent so replay and reconnect paths can be conservative.
//!
//! Locking: nodes live in hash shards (`parking_lot::RwLock<HashMap>`); a directory's entry
//! table lives inside its node. No operation holds two shard locks at once.

use mist_proto::{Attr, Kind, Name, NodeKey, Rec, ShareId, ShareInfo, SnapDir, SnapDone, Ts};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

const SHARDS: usize = 64;

/// First real cookie; 1 and 2 are reserved for the synthetic "." and ".." a mount surface emits.
const FIRST_COOKIE: u64 = 3;

/// Cap on stashed out-of-order journal records (Attr/Content before Created). Bounded to avoid
/// unbounded growth under a hostile stream; overflow drops the stash (resolved by snapshot/scrub).
const MAX_PENDING: usize = 65536;

/// Result of applying a journal record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    /// The guest signalled `Overflow`: the replica may have missed events; trigger a resync.
    NeedResync,
}

/// A journal record stashed because it referenced a node not yet known.
enum Pending {
    Attr(Attr),
    Content { version: u64, size: u64, mtime: Ts },
}

fn placeholder_attr(kind: Kind) -> Attr {
    Attr {
        kind,
        mode: if kind == Kind::Dir { 0o755 } else { 0o644 },
        nlink: 1,
        uid: 0,
        gid: 0,
        size: 0,
        blocks: 0,
        mtime: Ts { sec: 0, nsec: 0 },
        ctime: Ts { sec: 0, nsec: 0 },
        rdev: 0,
        content_version: 0,
        symlink_target: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareState {
    Seeding,
    Live,
    Degraded,
    Offline,
}

/// One dentry, compact layout: the name lives in the directory's arena, the node key
/// is packed inline. 32 bytes vs the old `BTreeMap<u64, DirEnt>` + double-allocated name
/// (~200 B/entry measured).
#[derive(Debug, Clone, Copy)]
struct CEnt {
    cookie: u64,
    ino: u64,
    nkgen: u32,
    name_off: u32,
    name_len: u16,
    kind: Kind,
    dead: bool,
}

impl CEnt {
    fn node(&self) -> NodeKey {
        NodeKey {
            ino: self.ino,
            generation: self.nkgen,
        }
    }
}

/// Shared SipHash state for the per-directory name indexes (one random seed per process).
fn name_hash(name: &[u8]) -> u64 {
    use std::hash::BuildHasher;
    static SEED: std::sync::OnceLock<std::hash::RandomState> = std::sync::OnceLock::new();
    SEED.get_or_init(std::hash::RandomState::new).hash_one(name)
}

/// Directory entry table: names in an append-only arena, entries in a
/// cookie-sorted vec (cookies are monotonic, so appends keep it sorted), removals tombstoned
/// and compacted when half the vec is dead. `by_name` is an open-addressed index into `ents`.
#[derive(Debug)]
struct DirState {
    arena: Vec<u8>,
    ents: Vec<CEnt>,
    by_name: hashbrown::HashTable<u32>,
    dead: u32,
    next_cookie: u64,
    dirgen: u64,
    complete: bool,
}

impl DirState {
    fn new() -> Self {
        DirState {
            arena: Vec::new(),
            ents: Vec::new(),
            by_name: hashbrown::HashTable::new(),
            dead: 0,
            next_cookie: FIRST_COOKIE,
            dirgen: 0,
            complete: false,
        }
    }

    fn name_of(&self, e: &CEnt) -> &[u8] {
        &self.arena[e.name_off as usize..e.name_off as usize + e.name_len as usize]
    }

    fn live_len(&self) -> usize {
        self.ents.len() - self.dead as usize
    }

    /// Index into `ents` for a live entry with this name.
    fn find_idx(&self, name: &[u8]) -> Option<usize> {
        self.by_name
            .find(name_hash(name), |&i| {
                let e = &self.ents[i as usize];
                !e.dead && self.name_of(e) == name
            })
            .map(|&i| i as usize)
    }

    fn find(&self, name: &[u8]) -> Option<(NodeKey, Kind)> {
        let e = &self.ents[self.find_idx(name)?];
        Some((e.node(), e.kind))
    }

    /// Idempotent dentry upsert: same (name → node) is a no-op (cookie + dirgen preserved);
    /// a different target replaces the entry under a fresh cookie.
    fn upsert(&mut self, name: &Name, node: NodeKey, kind: Kind) -> UpsertResult {
        if let Some(i) = self.find_idx(name.as_bytes()) {
            if self.ents[i].node() == node {
                self.ents[i].kind = kind;
                return UpsertResult {
                    gained_link: false,
                    displaced: None,
                    dentry_delta: 0,
                };
            }
            let old = self.ents[i].node();
            self.kill(i);
            self.insert_new(name.as_bytes(), node, kind);
            // One slot out, one in: dentry count unchanged.
            return UpsertResult {
                gained_link: true,
                displaced: Some(old),
                dentry_delta: 0,
            };
        }
        self.insert_new(name.as_bytes(), node, kind);
        UpsertResult {
            gained_link: true,
            displaced: None,
            dentry_delta: 1,
        }
    }

    fn insert_new(&mut self, name: &[u8], node: NodeKey, kind: Kind) {
        let cookie = self.next_cookie;
        self.next_cookie += 1;
        self.dirgen += 1;
        let name_off = self.arena.len() as u32;
        self.arena.extend_from_slice(name);
        let idx = self.ents.len() as u32;
        self.ents.push(CEnt {
            cookie,
            ino: node.ino,
            nkgen: node.generation,
            name_off,
            name_len: name.len() as u16,
            kind,
            dead: false,
        });
        let (ents, arena) = (&self.ents, &self.arena);
        self.by_name.insert_unique(name_hash(name), idx, |&i| {
            let e = &ents[i as usize];
            name_hash(&arena[e.name_off as usize..e.name_off as usize + e.name_len as usize])
        });
    }

    /// Tombstone entry `i` and drop it from the name index; compact when half-dead.
    fn kill(&mut self, i: usize) {
        let name = self.name_of(&self.ents[i]).to_vec();
        if let Ok(slot) = self
            .by_name
            .find_entry(name_hash(&name), |&j| j as usize == i)
        {
            slot.remove();
        }
        self.ents[i].dead = true;
        self.dead += 1;
        if self.dead as usize > self.ents.len() / 2 && self.dead >= 16 {
            self.compact();
        }
    }

    /// Rebuild arena/ents/index from live entries. Cookies are preserved (readdir resume
    /// positions stay valid); only dead slots and their arena bytes are reclaimed.
    fn compact(&mut self) {
        let mut arena = Vec::with_capacity(self.arena.len() / 2);
        let mut ents = Vec::with_capacity(self.live_len());
        for e in self.ents.iter().filter(|e| !e.dead) {
            let name_off = arena.len() as u32;
            arena.extend_from_slice(self.name_of(e));
            ents.push(CEnt { name_off, ..*e });
        }
        let mut by_name = hashbrown::HashTable::with_capacity(ents.len());
        for (i, e) in ents.iter().enumerate() {
            let name = &arena[e.name_off as usize..e.name_off as usize + e.name_len as usize];
            by_name.insert_unique(name_hash(name), i as u32, |&j| {
                let e = &ents[j as usize];
                name_hash(&arena[e.name_off as usize..e.name_off as usize + e.name_len as usize])
            });
        }
        self.arena = arena;
        self.ents = ents;
        self.by_name = by_name;
        self.dead = 0;
    }

    /// Remove a dentry by name. Returns the node it pointed at (whose link count drops).
    fn remove(&mut self, name: &[u8]) -> Option<NodeKey> {
        let i = self.find_idx(name)?;
        let node = self.ents[i].node();
        self.dirgen += 1;
        self.kill(i);
        Some(node)
    }

    /// Live entries with cookie > `after`, in cookie order.
    fn range_after(&self, after: u64) -> impl Iterator<Item = &CEnt> {
        let start = self.ents.partition_point(|e| e.cookie <= after);
        self.ents[start..].iter().filter(|e| !e.dead)
    }

    fn live(&self) -> impl Iterator<Item = &CEnt> {
        self.ents.iter().filter(|e| !e.dead)
    }
}

struct UpsertResult {
    /// The target node gained a (name → node) link; its link count must rise (Created path).
    gained_link: bool,
    /// A different node previously held this name; its link count must drop.
    displaced: Option<NodeKey>,
    /// Change in this directory's dentry count (for the `entries` stat): +1 fresh, 0 replace/noop.
    dentry_delta: i64,
}

/// One tree node, compact layout: `Attr` fields are inlined without the
/// `symlink_target` vec (side table — symlinks are rare) and `rdev` (side table — device nodes
/// are rarer). 88 bytes + the dir box vs ~144 with an embedded `Attr`.
#[derive(Debug)]
struct Node {
    size: u64,
    blocks: u64,
    content_version: u64,
    mtime_sec: i64,
    ctime_sec: i64,
    parent_ino: u64,
    parent_gen: u32,
    mtime_nsec: u32,
    ctime_nsec: u32,
    nlink: u32,
    uid: u32,
    gid: u32,
    /// Number of (parent, name) dentries pointing at this node. The root is exempt (never 0-GC'd).
    /// When this reaches 0 for a non-root node, the node is removed.
    links: u32,
    mode: u16,
    kind: Kind,
    /// Set when the node has an entry in a side table (symlink target / rdev) that removal
    /// must clean up.
    has_side: bool,
    dir: Option<Box<DirState>>,
}

impl Node {
    fn parent(&self) -> NodeKey {
        NodeKey {
            ino: self.parent_ino,
            generation: self.parent_gen,
        }
    }

    fn set_parent(&mut self, p: NodeKey) {
        self.parent_ino = p.ino;
        self.parent_gen = p.generation;
    }

    /// Overwrite the attr-derived fields. `content_version` is NOT touched here: it is owned
    /// by Content records (the journal's per-node write counter); stat-built Attrs carry 0 and
    /// must not reset it — a reset would let stale CAS manifests keyed at version 0 alias new
    /// content.
    fn set_attr(&mut self, a: &Attr) {
        self.size = a.size;
        self.blocks = a.blocks;
        self.mtime_sec = a.mtime.sec;
        self.mtime_nsec = a.mtime.nsec;
        self.ctime_sec = a.ctime.sec;
        self.ctime_nsec = a.ctime.nsec;
        self.nlink = a.nlink;
        self.uid = a.uid;
        self.gid = a.gid;
        self.mode = a.mode;
        self.kind = a.kind;
    }

    /// Reconstruct the wire `Attr` (symlink target / rdev grafted by the caller when present).
    fn attr(&self) -> Attr {
        Attr {
            kind: self.kind,
            mode: self.mode,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            size: self.size,
            blocks: self.blocks,
            mtime: Ts {
                sec: self.mtime_sec,
                nsec: self.mtime_nsec,
            },
            ctime: Ts {
                sec: self.ctime_sec,
                nsec: self.ctime_nsec,
            },
            rdev: 0,
            content_version: self.content_version,
            symlink_target: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicaStats {
    pub nodes: u64,
    pub dirs: u64,
    pub entries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDirEntry {
    pub cookie: u64,
    pub name: Name,
    pub node: NodeKey,
    pub kind: Kind,
}

#[derive(Debug, Clone)]
pub struct ReadDirPage {
    pub entries: Vec<ReadDirEntry>,
    pub eof: bool,
    pub dirgen: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReplicaError {
    #[error("no such node")]
    NoEnt,
    #[error("not a directory")]
    NotDir,
    #[error("not a symlink")]
    NotSymlink,
    #[error("directory not yet complete")]
    Incomplete,
    #[error("invalid path")]
    BadPath,
}

pub struct ShareReplica {
    pub info: ShareInfo,
    state: RwLock<ShareState>,
    // Boxed nodes: an inline 96-B value makes each (pow2-doubled) bucket 112 B; boxing cuts
    // buckets to 24 B and the payload lands in an exact 96-B malloc size class.
    shards: Vec<RwLock<HashMap<NodeKey, Box<Node>>>>,
    pending: Mutex<HashMap<NodeKey, Vec<Pending>>>,
    /// Symlink targets, keyed off-node (rare; immutable after create — design 02).
    symlinks: Mutex<HashMap<NodeKey, Box<[u8]>>>,
    /// Device numbers for Chr/Blk nodes (rarer still).
    rdevs: Mutex<HashMap<NodeKey, u64>>,
    nodes: AtomicU64,
    dirs: AtomicU64,
    entries: AtomicU64,
}

impl std::fmt::Debug for ShareReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareReplica")
            .field("share", &self.info.id)
            .field("stats", &self.stats())
            .finish()
    }
}

fn shard_of(key: NodeKey) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % SHARDS
}

impl ShareReplica {
    pub fn new(info: ShareInfo) -> Self {
        ShareReplica {
            info,
            state: RwLock::new(ShareState::Seeding),
            shards: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            pending: Mutex::new(HashMap::new()),
            symlinks: Mutex::new(HashMap::new()),
            rdevs: Mutex::new(HashMap::new()),
            nodes: AtomicU64::new(0),
            dirs: AtomicU64::new(0),
            entries: AtomicU64::new(0),
        }
    }

    pub fn share_id(&self) -> ShareId {
        self.info.id
    }

    pub fn root(&self) -> NodeKey {
        self.info.root
    }

    pub fn state(&self) -> ShareState {
        *self.state.read()
    }

    pub fn set_state(&self, s: ShareState) {
        *self.state.write() = s;
    }

    pub fn stats(&self) -> ReplicaStats {
        ReplicaStats {
            nodes: self.nodes.load(Ordering::Relaxed),
            dirs: self.dirs.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
        }
    }

    /// Upsert a node's attributes (and parent), creating it if unknown.
    fn upsert_node(&self, key: NodeKey, attr: &Attr, parent: NodeKey) {
        let has_side = self.note_side(key, attr);
        let mut shard = self.shards[shard_of(key)].write();
        match shard.get_mut(&key) {
            Some(n) => {
                let version = n.content_version;
                n.set_attr(attr);
                n.content_version = version;
                n.set_parent(parent);
                n.has_side |= has_side;
                if attr.kind == Kind::Dir && n.dir.is_none() {
                    n.dir = Some(Box::new(DirState::new()));
                    self.dirs.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => {
                let dir = if attr.kind == Kind::Dir {
                    self.dirs.fetch_add(1, Ordering::Relaxed);
                    Some(Box::new(DirState::new()))
                } else {
                    None
                };
                let mut n = Node {
                    size: 0,
                    blocks: 0,
                    content_version: attr.content_version,
                    mtime_sec: 0,
                    ctime_sec: 0,
                    parent_ino: parent.ino,
                    parent_gen: parent.generation,
                    mtime_nsec: 0,
                    ctime_nsec: 0,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    links: 0,
                    mode: 0,
                    kind: attr.kind,
                    has_side,
                    dir,
                };
                n.set_attr(attr);
                shard.insert(key, Box::new(n));
                self.nodes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Stash the rare off-node attr parts (symlink target, rdev). Returns whether any exist.
    fn note_side(&self, key: NodeKey, attr: &Attr) -> bool {
        let mut side = false;
        if let Some(t) = &attr.symlink_target {
            self.symlinks
                .lock()
                .insert(key, t.clone().into_boxed_slice());
            side = true;
        }
        if attr.rdev != 0 {
            self.rdevs.lock().insert(key, attr.rdev);
            side = true;
        }
        side
    }

    /// Graft the side-table parts back onto a reconstructed `Attr`.
    fn graft_side(&self, key: NodeKey, has_side: bool, attr: &mut Attr) {
        if !has_side {
            return;
        }
        if attr.kind == Kind::Symlink {
            attr.symlink_target = self.symlinks.lock().get(&key).map(|t| t.to_vec());
        }
        if matches!(attr.kind, Kind::Chr | Kind::Blk) {
            attr.rdev = self.rdevs.lock().get(&key).copied().unwrap_or(0);
        }
    }

    fn drop_side(&self, key: NodeKey) {
        self.symlinks.lock().remove(&key);
        self.rdevs.lock().remove(&key);
    }

    fn entries_add(&self, delta: i64) {
        if delta >= 0 {
            self.entries.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.entries.fetch_sub((-delta) as u64, Ordering::Relaxed);
        }
    }

    /// Record a new incoming dentry link for `key` (does not touch the `entries` stat).
    fn link_inc(&self, key: NodeKey) {
        if let Some(n) = self.shards[shard_of(key)].write().get_mut(&key) {
            n.links += 1;
        }
    }

    /// Drop an incoming dentry link for `key`; GC the node (and its subtree if a dir) at 0.
    /// Does not touch the `entries` stat (the caller adjusts it for the dentry it removed).
    fn link_dec(&self, key: NodeKey) {
        let gc = {
            let mut shard = self.shards[shard_of(key)].write();
            match shard.get_mut(&key) {
                Some(n) => {
                    n.links = n.links.saturating_sub(1);
                    n.links == 0 && key != self.info.root
                }
                None => false,
            }
        };
        if gc {
            self.remove_node(key);
        }
    }

    /// Remove a node entirely. If it's a directory, drop the links of all its children first
    /// (recursively GC'ing an orphaned subtree — e.g. `rm -rf` surfaced as one Removed of the top).
    fn remove_node(&self, key: NodeKey) {
        let removed = {
            let mut shard = self.shards[shard_of(key)].write();
            shard.remove(&key)
        };
        let Some(node) = removed else { return };
        self.nodes.fetch_sub(1, Ordering::Relaxed);
        if node.has_side {
            self.drop_side(key);
        }
        if let Some(dir) = node.dir {
            self.dirs.fetch_sub(1, Ordering::Relaxed);
            // Each child loses the dentry this directory held.
            for ent in dir.live() {
                self.entries_add(-1);
                self.link_dec(ent.node());
            }
        }
    }

    /// Apply one snapshot record. Idempotent; tolerant of arrival order (children before
    /// parents, a directory's listing split across multiple records).
    pub fn apply_snap_dir(&self, d: &SnapDir) {
        debug_assert_eq!(d.share, self.info.id);
        // 1. The directory node itself.
        self.upsert_node(d.dir, &d.dir_attr, d.parent);

        // 2. Child nodes (one shard lock at a time, never while holding the parent's).
        for e in &d.entries {
            self.upsert_node(e.node, &e.attr, d.dir);
        }

        // 3. Parent dentries. Collect link adjustments under the dir lock, apply them after
        //    releasing it — never hold two shard locks at once (deadlock discipline).
        let mut to_link: Vec<NodeKey> = Vec::new();
        let mut to_unlink: Vec<NodeKey> = Vec::new();
        {
            let mut shard = self.shards[shard_of(d.dir)].write();
            let node = shard.get_mut(&d.dir).expect("just upserted");
            let dir = node.dir.as_mut().expect("dir attr implies DirState");
            for e in &d.entries {
                let r = dir.upsert(&e.name, e.node, e.attr.kind);
                self.entries_add(r.dentry_delta);
                if r.gained_link {
                    to_link.push(e.node);
                }
                if let Some(old) = r.displaced {
                    to_unlink.push(old);
                }
            }
            if d.last {
                dir.complete = true;
            }
        }
        for k in to_link {
            self.link_inc(k);
        }
        for k in to_unlink {
            self.link_dec(k);
        }
    }

    /// Apply one journal record to the live tree (design 04 §4.2). Idempotent and order-tolerant
    /// via the same upsert primitive as snapshot assembly. Returns whether a resync is required.
    ///
    /// Records that reference an unknown node (`AttrChanged`/`Content` arriving before `Created`)
    /// are stashed in a pending map and replayed when the node appears.
    pub fn apply_rec(&self, rec: &Rec) -> ApplyOutcome {
        match rec {
            Rec::Created {
                parent,
                name,
                node,
                attr,
            } => {
                let kind = attr.as_ref().map(|a| a.kind).unwrap_or(Kind::Reg);
                if let Some(a) = attr {
                    self.upsert_node(*node, a, *parent);
                    self.drain_pending(*node);
                } else if self.getattr(*node).is_err() {
                    // Raced with its own deletion: create a minimal placeholder.
                    self.upsert_node(*node, &placeholder_attr(kind), *parent);
                }
                // A freshly created dir is complete by construction (empty; every future entry
                // arrives via the journal). Incompleteness only exists during snapshot seeding.
                if kind == Kind::Dir {
                    self.mark_dir_complete(*node);
                }
                self.dentry_set(*parent, name, *node, kind);
                ApplyOutcome::Applied
            }
            Rec::CreatedBatch { parent, entries } => {
                for e in entries {
                    self.upsert_node(e.node, &e.attr, *parent);
                    self.drain_pending(e.node);
                    if e.attr.kind == Kind::Dir {
                        self.mark_dir_complete(e.node);
                    }
                    self.dentry_set(*parent, &e.name, e.node, e.attr.kind);
                }
                ApplyOutcome::Applied
            }
            Rec::Removed { parent, name } => {
                self.dentry_remove(*parent, name.as_bytes());
                ApplyOutcome::Applied
            }
            Rec::Renamed {
                from_parent,
                from_name,
                to_parent,
                to_name,
            } => {
                // Take the source dentry's node (link preserved); re-link it at the destination
                // without changing its link count (a move keeps the same number of names).
                if let Some((node, kind)) = self.dentry_take(*from_parent, from_name.as_bytes()) {
                    self.dentry_relink(*to_parent, to_name, node, kind);
                }
                ApplyOutcome::Applied
            }
            Rec::AttrChanged { node, attr } => {
                if self.update_attr(*node, attr) {
                    ApplyOutcome::Applied
                } else {
                    self.stash_pending(*node, Pending::Attr(attr.clone()));
                    ApplyOutcome::Applied
                }
            }
            Rec::Content {
                node,
                version,
                size,
                mtime,
                ..
            } => {
                if !self.update_content(*node, *version, *size, *mtime) {
                    self.stash_pending(
                        *node,
                        Pending::Content {
                            version: *version,
                            size: *size,
                            mtime: *mtime,
                        },
                    );
                }
                ApplyOutcome::Applied
            }
            Rec::SelfRemoved { node } => {
                // Force-remove regardless of link count (last hardlink gone in the guest).
                if self.exists(*node) && *node != self.info.root {
                    self.force_remove(*node);
                }
                ApplyOutcome::Applied
            }
            Rec::Overflow => ApplyOutcome::NeedResync,
            Rec::EchoMarker { .. } => ApplyOutcome::Applied,
        }
    }

    fn exists(&self, key: NodeKey) -> bool {
        self.shards[shard_of(key)].read().contains_key(&key)
    }

    fn mark_dir_complete(&self, key: NodeKey) {
        if let Some(dir) = self.shards[shard_of(key)]
            .write()
            .get_mut(&key)
            .and_then(|n| n.dir.as_mut())
        {
            dir.complete = true;
        }
    }

    /// Set a dentry (parent, name) → node, managing link counts. Creates the parent dir lazily
    /// only if it already exists as a directory; a missing parent drops the dentry (the snapshot
    /// or a later Created for the parent will reconcile).
    fn dentry_set(&self, parent: NodeKey, name: &Name, node: NodeKey, kind: Kind) {
        let r = {
            let mut shard = self.shards[shard_of(parent)].write();
            match shard.get_mut(&parent).and_then(|n| n.dir.as_mut()) {
                Some(dir) => dir.upsert(name, node, kind),
                None => return,
            }
        };
        self.entries_add(r.dentry_delta);
        if r.gained_link {
            self.link_inc(node);
        }
        if let Some(old) = r.displaced {
            self.link_dec(old);
        }
    }

    /// Re-link a node at (parent, name) for a rename: the moved node's link count is *preserved*
    /// (a move doesn't change how many names it has), but a clobbered node at the destination
    /// loses its link.
    fn dentry_relink(&self, parent: NodeKey, name: &Name, node: NodeKey, kind: Kind) {
        let r = {
            let mut shard = self.shards[shard_of(parent)].write();
            match shard.get_mut(&parent).and_then(|n| n.dir.as_mut()) {
                Some(dir) => dir.upsert(name, node, kind),
                None => return,
            }
        };
        self.entries_add(r.dentry_delta);
        // Do NOT link_inc(node): its link was preserved by dentry_take.
        if let Some(old) = r.displaced {
            self.link_dec(old);
        }
        // The moved node's primary parent is now the destination dir (so path_of walks correctly).
        if let Some(n) = self.shards[shard_of(node)].write().get_mut(&node) {
            n.set_parent(parent);
        }
    }

    fn dentry_remove(&self, parent: NodeKey, name: &[u8]) {
        let removed = {
            let mut shard = self.shards[shard_of(parent)].write();
            shard
                .get_mut(&parent)
                .and_then(|n| n.dir.as_mut())
                .and_then(|d| d.remove(name))
        };
        if let Some(node) = removed {
            self.entries_add(-1);
            self.link_dec(node);
        }
    }

    /// Remove a dentry and return its target node + kind without dropping the node's link
    /// (for a rename move). Adjusts the `entries` stat for the removed dentry.
    fn dentry_take(&self, parent: NodeKey, name: &[u8]) -> Option<(NodeKey, Kind)> {
        let mut shard = self.shards[shard_of(parent)].write();
        let dir = shard.get_mut(&parent).and_then(|n| n.dir.as_mut())?;
        let (_, kind) = dir.find(name)?;
        let node = dir.remove(name)?;
        self.entries_add(-1);
        Some((node, kind))
    }

    fn update_attr(&self, key: NodeKey, attr: &Attr) -> bool {
        let has_side = self.note_side(key, attr);
        let mut shard = self.shards[shard_of(key)].write();
        match shard.get_mut(&key) {
            Some(n) => {
                // Don't let a non-dir AttrChanged clobber an existing dir's DirState (the
                // DirState lives beside the attr fields, untouched here). set_attr preserves
                // content_version (see its doc).
                n.set_attr(attr);
                n.has_side |= has_side;
                true
            }
            None => false,
        }
    }

    fn update_content(&self, key: NodeKey, version: u64, size: u64, mtime: mist_proto::Ts) -> bool {
        let mut shard = self.shards[shard_of(key)].write();
        match shard.get_mut(&key) {
            Some(n) => {
                n.size = size;
                n.mtime_sec = mtime.sec;
                n.mtime_nsec = mtime.nsec;
                n.content_version = version;
                true
            }
            None => false,
        }
    }

    fn force_remove(&self, key: NodeKey) {
        // Drop all incoming dentries by zeroing links, then GC.
        {
            let mut shard = self.shards[shard_of(key)].write();
            if let Some(n) = shard.get_mut(&key) {
                n.links = 0;
            }
        }
        self.remove_node(key);
    }

    fn stash_pending(&self, key: NodeKey, p: Pending) {
        let mut pend = self.pending.lock();
        if pend.len() >= MAX_PENDING {
            pend.clear(); // bounded; lost stashes resolve on next snapshot/scrub
        }
        pend.entry(key).or_default().push(p);
    }

    fn drain_pending(&self, key: NodeKey) {
        let items = self.pending.lock().remove(&key);
        let Some(items) = items else { return };
        for p in items {
            match p {
                Pending::Attr(a) => {
                    self.update_attr(key, &a);
                }
                Pending::Content {
                    version,
                    size,
                    mtime,
                } => {
                    self.update_content(key, version, size, mtime);
                }
            }
        }
    }

    /// Mark the snapshot finished; computes the authoritative entry count.
    pub fn finish_snapshot(&self, _done: &SnapDone) -> ReplicaStats {
        let mut entries = 0u64;
        for shard in &self.shards {
            let s = shard.read();
            for n in s.values() {
                if let Some(d) = &n.dir {
                    entries += d.live_len() as u64;
                }
            }
        }
        self.entries.store(entries, Ordering::Relaxed);
        self.set_state(ShareState::Live);
        self.stats()
    }

    pub fn getattr(&self, key: NodeKey) -> Result<Attr, ReplicaError> {
        let (mut attr, has_side) = {
            let shard = self.shards[shard_of(key)].read();
            let n = shard.get(&key).ok_or(ReplicaError::NoEnt)?;
            (n.attr(), n.has_side)
        };
        self.graft_side(key, has_side, &mut attr);
        Ok(attr)
    }

    pub fn lookup(&self, dir: NodeKey, name: &[u8]) -> Result<(NodeKey, Attr), ReplicaError> {
        let child = {
            let shard = self.shards[shard_of(dir)].read();
            let n = shard.get(&dir).ok_or(ReplicaError::NoEnt)?;
            let d = n.dir.as_ref().ok_or(ReplicaError::NotDir)?;
            d.find(name).ok_or(ReplicaError::NoEnt)?.0
        };
        let attr = self.getattr(child)?;
        Ok((child, attr))
    }

    /// Page a directory starting *after* `cookie` (0 = from the beginning).
    pub fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        max: usize,
    ) -> Result<ReadDirPage, ReplicaError> {
        let shard = self.shards[shard_of(dir)].read();
        let n = shard.get(&dir).ok_or(ReplicaError::NoEnt)?;
        let d = n.dir.as_ref().ok_or(ReplicaError::NotDir)?;
        if !d.complete {
            return Err(ReplicaError::Incomplete);
        }
        let mut entries = Vec::with_capacity(max.min(d.live_len()));
        for e in d.range_after(cookie) {
            if entries.len() >= max {
                return Ok(ReadDirPage {
                    entries,
                    eof: false,
                    dirgen: d.dirgen,
                });
            }
            entries.push(ReadDirEntry {
                cookie: e.cookie,
                name: Name::new(d.name_of(e).to_vec()).expect("names validated on insert"),
                node: e.node(),
                kind: e.kind,
            });
        }
        Ok(ReadDirPage {
            entries,
            eof: true,
            dirgen: d.dirgen,
        })
    }

    pub fn readlink(&self, key: NodeKey) -> Result<Vec<u8>, ReplicaError> {
        let attr = self.getattr(key)?;
        attr.symlink_target.ok_or(ReplicaError::NotSymlink)
    }

    /// Number of incoming dentries for a node (0 if absent). Test/diagnostic helper.
    #[doc(hidden)]
    pub fn link_count(&self, key: NodeKey) -> u32 {
        self.shards[shard_of(key)]
            .read()
            .get(&key)
            .map(|n| n.links)
            .unwrap_or(0)
    }

    /// Total live node count (test/diagnostic helper).
    #[doc(hidden)]
    pub fn node_count(&self) -> u64 {
        self.nodes.load(Ordering::Relaxed)
    }

    /// Nudge a directory's mtime/ctime forward by ≥1 ns. Guest-side namespace changes must
    /// move the parent dir's attrs: the macOS client caches NEGATIVE dentries keyed on dir
    /// mtime, and fanotify emits no AttrChanged for the parent on child create/remove — a
    /// frozen mtime pins "no such file" until something else touches the dir (found by the
    /// chaos suite's guest-runner probe).
    pub fn bump_dir_attr(&self, dir: NodeKey) {
        let mut shard = self.shards[shard_of(dir)].write();
        if let Some(n) = shard.get_mut(&dir)
            && n.kind == Kind::Dir
        {
            if n.mtime_nsec >= 999_999_999 {
                n.mtime_sec += 1;
                n.mtime_nsec = 0;
            } else {
                n.mtime_nsec += 1;
            }
            n.ctime_sec = n.mtime_sec;
            n.ctime_nsec = n.mtime_nsec;
        }
    }

    /// Parent of a node (NFSv4 LOOKUPP). The root is its own parent.
    pub fn parent_of(&self, node: NodeKey) -> Option<NodeKey> {
        Some(self.shards[shard_of(node)].read().get(&node)?.parent())
    }

    /// Reconstruct a node's path from the root by walking parent links (best-effort; bounded
    /// depth). Used by `mist events` to render journal changes as paths. One shard lock at a time.
    pub fn path_of(&self, node: NodeKey) -> Option<String> {
        if node == self.root() {
            return Some("/".to_string());
        }
        let mut parts: Vec<Vec<u8>> = Vec::new();
        let mut cur = node;
        for _ in 0..512 {
            let parent = self.shards[shard_of(cur)].read().get(&cur)?.parent();
            if parent == cur {
                break; // reached a self-parent (root)
            }
            let name = {
                let sh = self.shards[shard_of(parent)].read();
                let d = sh.get(&parent)?.dir.as_ref()?;
                d.live()
                    .find(|e| e.node() == cur)
                    .map(|e| d.name_of(e).to_vec())?
            };
            parts.push(name);
            cur = parent;
            if cur == self.root() {
                break;
            }
        }
        parts.reverse();
        let joined: Vec<String> = parts
            .iter()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect();
        Some(format!("/{}", joined.join("/")))
    }

    /// Debug/CLI path resolution: `/a/b/c` from the share root, no symlink following.
    pub fn resolve_path(&self, path: &str) -> Result<(NodeKey, Attr), ReplicaError> {
        let mut cur = self.root();
        let mut attr = self.getattr(cur)?;
        for comp in path.split('/').filter(|c| !c.is_empty()) {
            if comp == "." || comp == ".." {
                return Err(ReplicaError::BadPath);
            }
            let (next, a) = self.lookup(cur, comp.as_bytes())?;
            cur = next;
            attr = a;
        }
        Ok((cur, attr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mist_proto::{SnapEntry, Ts};
    use std::collections::BTreeMap;

    fn attr(kind: Kind, size: u64) -> Attr {
        Attr {
            kind,
            mode: 0o755,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            size,
            blocks: size / 512,
            mtime: Ts { sec: 1, nsec: 0 },
            ctime: Ts { sec: 1, nsec: 0 },
            rdev: 0,
            content_version: 0,
            symlink_target: if kind == Kind::Symlink {
                Some(b"target".to_vec())
            } else {
                None
            },
        }
    }

    fn key(ino: u64) -> NodeKey {
        NodeKey { ino, generation: 1 }
    }

    fn name(s: &str) -> Name {
        Name::new(s.as_bytes().to_vec()).unwrap()
    }

    fn info() -> ShareInfo {
        ShareInfo {
            id: ShareId(1),
            name: "code".into(),
            epoch: 1,
            fsid: 42,
            root: key(2),
            flags: 0,
            ino_bits: 32,
        }
    }

    fn snap(dir: u64, parent: u64, entries: &[(&str, u64, Kind)], last: bool) -> SnapDir {
        SnapDir {
            snap_id: 1,
            share: ShareId(1),
            dir: key(dir),
            dir_attr: attr(Kind::Dir, 4096),
            parent: key(parent),
            entries: entries
                .iter()
                .map(|(n, ino, k)| SnapEntry {
                    name: name(n),
                    node: key(*ino),
                    attr: attr(*k, 10),
                })
                .collect(),
            last,
        }
    }

    fn done() -> SnapDone {
        SnapDone {
            snap_id: 1,
            share: ShareId(1),
            dirs: 0,
            entries: 0,
            errors: 0,
        }
    }

    fn dump_dir(r: &ShareReplica, dir: NodeKey) -> (Vec<(u64, Vec<u8>, NodeKey)>, u64) {
        let shard = r.shards[shard_of(dir)].read();
        let d = shard.get(&dir).unwrap().dir.as_ref().unwrap();
        (
            d.live()
                .map(|e| (e.cookie, d.name_of(e).to_vec(), e.node()))
                .collect(),
            d.dirgen,
        )
    }

    #[test]
    fn assemble_lookup_readdir() {
        let r = ShareReplica::new(info());
        r.apply_snap_dir(&snap(
            2,
            2,
            &[("src", 10, Kind::Dir), ("README", 11, Kind::Reg)],
            true,
        ));
        r.apply_snap_dir(&snap(10, 2, &[("main.rs", 20, Kind::Reg)], true));
        r.finish_snapshot(&done());

        assert_eq!(r.state(), ShareState::Live);
        let (src, a) = r.lookup(key(2), b"src").unwrap();
        assert_eq!(src, key(10));
        assert_eq!(a.kind, Kind::Dir);
        let (file, _) = r.resolve_path("/src/main.rs").unwrap();
        assert_eq!(file, key(20));

        let page = r.readdir(key(2), 0, 10).unwrap();
        assert!(page.eof);
        let names: Vec<_> = page
            .entries
            .iter()
            .map(|e| String::from_utf8_lossy(e.name.as_bytes()).into_owned())
            .collect();
        assert_eq!(names, vec!["src", "README"]); // insertion order via cookies
        assert_eq!(r.stats().entries, 3);
    }

    #[test]
    fn orphan_child_before_parent() {
        let r = ShareReplica::new(info());
        // Child dir's record arrives first.
        r.apply_snap_dir(&snap(10, 2, &[("main.rs", 20, Kind::Reg)], true));
        r.apply_snap_dir(&snap(2, 2, &[("src", 10, Kind::Dir)], true));
        r.finish_snapshot(&done());
        assert_eq!(r.resolve_path("/src/main.rs").unwrap().0, key(20));
    }

    #[test]
    fn idempotent_double_apply_preserves_cookies() {
        let r = ShareReplica::new(info());
        let d = snap(2, 2, &[("a", 10, Kind::Reg), ("b", 11, Kind::Reg)], true);
        r.apply_snap_dir(&d);
        let s1 = dump_dir(&r, key(2));
        r.apply_snap_dir(&d);
        let s2 = dump_dir(&r, key(2));
        assert_eq!(s1, s2, "double apply must not churn cookies or dirgen");
    }

    #[test]
    fn replaced_entry_gets_fresh_cookie() {
        let r = ShareReplica::new(info());
        r.apply_snap_dir(&snap(2, 2, &[("a", 10, Kind::Reg)], true));
        let (v1, gen1) = dump_dir(&r, key(2));
        // Same name now points at a different inode (resync after guest replace).
        r.apply_snap_dir(&snap(2, 2, &[("a", 99, Kind::Reg)], true));
        let (v2, gen2) = dump_dir(&r, key(2));
        assert_eq!(v1.len(), 1);
        assert_eq!(v2.len(), 1);
        assert_ne!(v1[0].0, v2[0].0, "replacement must use a fresh cookie");
        assert_eq!(v2[0].2, key(99));
        assert!(gen2 > gen1);
    }

    #[test]
    fn multi_record_directory() {
        let r = ShareReplica::new(info());
        r.apply_snap_dir(&snap(2, 2, &[("a", 10, Kind::Reg)], false));
        assert_eq!(
            r.readdir(key(2), 0, 10).unwrap_err(),
            ReplicaError::Incomplete
        );
        r.apply_snap_dir(&snap(2, 2, &[("b", 11, Kind::Reg)], true));
        let page = r.readdir(key(2), 0, 10).unwrap();
        assert_eq!(page.entries.len(), 2);
    }

    #[test]
    fn readdir_paging_resumes_by_cookie() {
        let r = ShareReplica::new(info());
        let entries: Vec<(String, u64, Kind)> = (0..10)
            .map(|i| (format!("f{i:02}"), 100 + i, Kind::Reg))
            .collect();
        let borrowed: Vec<(&str, u64, Kind)> = entries
            .iter()
            .map(|(n, i, k)| (n.as_str(), *i, *k))
            .collect();
        r.apply_snap_dir(&snap(2, 2, &borrowed, true));
        r.finish_snapshot(&done());

        let p1 = r.readdir(key(2), 0, 4).unwrap();
        assert!(!p1.eof);
        let p2 = r
            .readdir(key(2), p1.entries.last().unwrap().cookie, 4)
            .unwrap();
        let p3 = r
            .readdir(key(2), p2.entries.last().unwrap().cookie, 4)
            .unwrap();
        assert!(p3.eof);
        let mut all: Vec<_> = p1
            .entries
            .iter()
            .chain(&p2.entries)
            .chain(&p3.entries)
            .map(|e| e.name.as_bytes().to_vec())
            .collect();
        assert_eq!(all.len(), 10);
        all.dedup();
        assert_eq!(all.len(), 10, "no duplicates across pages");
    }

    #[test]
    fn symlink_readlink() {
        let r = ShareReplica::new(info());
        r.apply_snap_dir(&snap(2, 2, &[("ln", 30, Kind::Symlink)], true));
        r.finish_snapshot(&done());
        assert_eq!(r.readlink(key(30)).unwrap(), b"target");
        assert_eq!(r.readlink(key(2)).unwrap_err(), ReplicaError::NotSymlink);
    }

    // ---- Journal apply -------------------------------------------------------------------

    /// A seeded, empty, Live replica rooted at ino 2.
    fn seeded_root() -> ShareReplica {
        let r = ShareReplica::new(info());
        r.apply_snap_dir(&snap(2, 2, &[], true));
        r.finish_snapshot(&done());
        r
    }

    fn created(parent: u64, nm: &str, node: u64, kind: Kind) -> Rec {
        Rec::Created {
            parent: key(parent),
            name: name(nm),
            node: key(node),
            attr: Some(attr(kind, 0)),
        }
    }

    #[test]
    fn journal_create_then_remove() {
        let r = seeded_root();
        r.apply_rec(&created(2, "a.txt", 100, Kind::Reg));
        assert_eq!(r.resolve_path("/a.txt").unwrap().0, key(100));
        assert_eq!(r.link_count(key(100)), 1);
        assert_eq!(r.stats().entries, 1);

        r.apply_rec(&Rec::Removed {
            parent: key(2),
            name: name("a.txt"),
        });
        assert_eq!(r.resolve_path("/a.txt").unwrap_err(), ReplicaError::NoEnt);
        assert_eq!(r.link_count(key(100)), 0, "node GC'd");
        assert_eq!(r.stats().entries, 0);
    }

    #[test]
    fn journal_create_dir_tree_then_rm_rf() {
        let r = seeded_root();
        // mkdir /d ; touch /d/f ; mkdir /d/sub ; touch /d/sub/g
        r.apply_rec(&created(2, "d", 10, Kind::Dir));
        r.apply_rec(&created(10, "f", 11, Kind::Reg));
        r.apply_rec(&created(10, "sub", 12, Kind::Dir));
        r.apply_rec(&created(12, "g", 13, Kind::Reg));
        assert_eq!(r.resolve_path("/d/sub/g").unwrap().0, key(13));
        assert_eq!(r.node_count(), 5); // root + d + f + sub + g

        // rm -rf /d surfaces as a single Removed of (root, "d"); the whole subtree must GC.
        r.apply_rec(&Rec::Removed {
            parent: key(2),
            name: name("d"),
        });
        assert_eq!(r.resolve_path("/d").unwrap_err(), ReplicaError::NoEnt);
        assert_eq!(r.node_count(), 1, "only root remains after subtree GC");
        assert_eq!(r.stats().entries, 0);
        assert_eq!(r.stats().dirs, 1); // just root
    }

    #[test]
    fn journal_rename_moves_node_identity() {
        let r = seeded_root();
        r.apply_rec(&created(2, "old", 100, Kind::Reg));
        r.apply_rec(&Rec::Content {
            node: key(100),
            version: 5,
            size: 1234,
            mtime: Ts { sec: 9, nsec: 0 },
            in_progress: false,
        });
        r.apply_rec(&created(2, "dir", 10, Kind::Dir));

        r.apply_rec(&Rec::Renamed {
            from_parent: key(2),
            from_name: name("old"),
            to_parent: key(10),
            to_name: name("new"),
        });
        assert_eq!(r.resolve_path("/old").unwrap_err(), ReplicaError::NoEnt);
        let (n, a) = r.resolve_path("/dir/new").unwrap();
        assert_eq!(n, key(100), "node identity preserved across rename");
        assert_eq!(a.size, 1234, "content survives rename");
        assert_eq!(r.link_count(key(100)), 1);
        assert_eq!(r.stats().entries, 2); // dir + dir/new
    }

    #[test]
    fn journal_content_and_attr_update() {
        let r = seeded_root();
        r.apply_rec(&created(2, "f", 100, Kind::Reg));
        r.apply_rec(&Rec::Content {
            node: key(100),
            version: 1,
            size: 4096,
            mtime: Ts { sec: 7, nsec: 0 },
            in_progress: false,
        });
        assert_eq!(r.getattr(key(100)).unwrap().size, 4096);

        let mut a = attr(Kind::Reg, 4096);
        a.mode = 0o600;
        r.apply_rec(&Rec::AttrChanged {
            node: key(100),
            attr: a,
        });
        assert_eq!(r.getattr(key(100)).unwrap().mode, 0o600);
    }

    #[test]
    fn journal_pending_content_before_created() {
        let r = seeded_root();
        // Content arrives before the node is known (reordered stream): stashed, then resolved.
        r.apply_rec(&Rec::Content {
            node: key(100),
            version: 3,
            size: 999,
            mtime: Ts { sec: 1, nsec: 0 },
            in_progress: false,
        });
        assert!(r.getattr(key(100)).is_err());
        r.apply_rec(&created(2, "f", 100, Kind::Reg));
        let a = r.getattr(key(100)).unwrap();
        assert_eq!(a.size, 999, "pending content replayed on Created");
        assert_eq!(a.content_version, 3);
    }

    #[test]
    fn attrchanged_preserves_content_version() {
        let r = seeded_root();
        r.apply_rec(&created(2, "f", 100, Kind::Reg));
        r.apply_rec(&Rec::Content {
            node: key(100),
            version: 7,
            size: 10,
            mtime: Ts { sec: 1, nsec: 0 },
            in_progress: false,
        });
        // Stat-built attrs carry content_version 0 (e.g. chmod echo); must not reset it.
        let mut a = r.getattr(key(100)).unwrap();
        a.mode = 0o600;
        a.content_version = 0;
        r.apply_rec(&Rec::AttrChanged {
            node: key(100),
            attr: a,
        });
        let after = r.getattr(key(100)).unwrap();
        assert_eq!(after.mode, 0o600);
        assert_eq!(
            after.content_version, 7,
            "AttrChanged must not clobber version"
        );
    }

    #[test]
    fn journal_hardlink_refcount() {
        let r = seeded_root();
        r.apply_rec(&created(2, "a", 100, Kind::Reg));
        // Second name for the same inode (hardlink discovery).
        r.apply_rec(&created(2, "b", 100, Kind::Reg));
        assert_eq!(r.link_count(key(100)), 2);
        r.apply_rec(&Rec::Removed {
            parent: key(2),
            name: name("a"),
        });
        assert_eq!(
            r.link_count(key(100)),
            1,
            "node survives while another link exists"
        );
        assert_eq!(r.resolve_path("/b").unwrap().0, key(100));
        r.apply_rec(&Rec::Removed {
            parent: key(2),
            name: name("b"),
        });
        assert_eq!(r.link_count(key(100)), 0, "GC'd after last link");
    }

    #[test]
    fn journal_idempotent_double_apply() {
        let r = seeded_root();
        let rec = created(2, "f", 100, Kind::Reg);
        r.apply_rec(&rec);
        r.apply_rec(&rec);
        assert_eq!(
            r.link_count(key(100)),
            1,
            "double Created must not double-link"
        );
        assert_eq!(r.stats().entries, 1);
    }

    #[test]
    fn journal_overflow_signals_resync() {
        let r = seeded_root();
        assert_eq!(r.apply_rec(&Rec::Overflow), ApplyOutcome::NeedResync);
        assert_eq!(
            r.apply_rec(&Rec::EchoMarker { tag: 1 }),
            ApplyOutcome::Applied
        );
    }

    #[test]
    fn path_of_reconstructs_paths() {
        let r = seeded_root();
        r.apply_rec(&created(2, "src", 10, Kind::Dir));
        r.apply_rec(&created(10, "sub", 11, Kind::Dir));
        r.apply_rec(&created(11, "deep.txt", 12, Kind::Reg));
        assert_eq!(r.path_of(key(2)).as_deref(), Some("/"));
        assert_eq!(r.path_of(key(10)).as_deref(), Some("/src"));
        assert_eq!(r.path_of(key(12)).as_deref(), Some("/src/sub/deep.txt"));
        r.apply_rec(&Rec::Renamed {
            from_parent: key(11),
            from_name: name("deep.txt"),
            to_parent: key(2),
            to_name: name("moved.txt"),
        });
        assert_eq!(r.path_of(key(12)).as_deref(), Some("/moved.txt"));
    }

    // ---- Model-based test: replica tree must equal a reference model after random journals ---

    /// A simple reference filesystem model. Generates the Rec records mistd would emit and
    /// maintains the ground-truth tree independently; the replica must converge to it.
    struct Model {
        kind: HashMap<u64, Kind>,
        size: HashMap<u64, u64>,
        children: HashMap<u64, BTreeMap<String, u64>>, // dir ino -> name -> ino
        parent: HashMap<u64, u64>,
        next_ino: u64,
        root: u64,
    }

    impl Model {
        fn new() -> Self {
            let mut m = Model {
                kind: HashMap::new(),
                size: HashMap::new(),
                children: HashMap::new(),
                parent: HashMap::new(),
                next_ino: 100,
                root: 2,
            };
            m.kind.insert(2, Kind::Dir);
            m.children.insert(2, BTreeMap::new());
            m.parent.insert(2, 2);
            m
        }

        fn dirs(&self) -> Vec<u64> {
            self.kind
                .iter()
                .filter(|(_, k)| **k == Kind::Dir)
                .map(|(i, _)| *i)
                .collect()
        }

        fn fresh(&mut self) -> u64 {
            let i = self.next_ino;
            self.next_ino += 1;
            i
        }

        fn create(&mut self, parent: u64, nm: &str, kind: Kind) -> Option<Rec> {
            let pc = self.children.get(&parent)?;
            if pc.contains_key(nm) {
                return None;
            }
            let node = self.fresh();
            self.kind.insert(node, kind);
            self.size.insert(node, 0);
            self.parent.insert(node, parent);
            if kind == Kind::Dir {
                self.children.insert(node, BTreeMap::new());
            }
            self.children
                .get_mut(&parent)
                .unwrap()
                .insert(nm.to_string(), node);
            Some(Rec::Created {
                parent: key(parent),
                name: name(nm),
                node: key(node),
                attr: Some(attr(kind, 0)),
            })
        }

        fn remove_subtree(&mut self, node: u64) {
            if self.kind.get(&node) == Some(&Kind::Dir) {
                let kids: Vec<u64> = self
                    .children
                    .get(&node)
                    .map(|c| c.values().copied().collect())
                    .unwrap_or_default();
                for k in kids {
                    self.remove_subtree(k);
                }
                self.children.remove(&node);
            }
            self.kind.remove(&node);
            self.size.remove(&node);
            self.parent.remove(&node);
        }

        fn remove(&mut self, parent: u64, nm: &str) -> Option<Rec> {
            let node = *self.children.get(&parent)?.get(nm)?;
            self.children.get_mut(&parent).unwrap().remove(nm);
            self.remove_subtree(node);
            Some(Rec::Removed {
                parent: key(parent),
                name: name(nm),
            })
        }

        /// Compare the model tree to a replica, walking from root.
        fn assert_matches(&self, r: &ShareReplica) {
            self.walk(self.root, r);
            // Node count: model nodes (incl. root) == replica live nodes.
            assert_eq!(
                self.kind.len() as u64,
                r.node_count(),
                "node count mismatch"
            );
        }

        fn walk(&self, ino: u64, r: &ShareReplica) {
            let kind = self.kind[&ino];
            let got = r
                .getattr(key(ino))
                .unwrap_or_else(|_| panic!("missing node {ino}"));
            assert_eq!(got.kind, kind, "kind mismatch at ino {ino}");
            if kind == Kind::Dir {
                let model_children = &self.children[&ino];
                // Every model child is present with the right target.
                for (nm, &cino) in model_children {
                    let (gn, _) = r
                        .lookup(key(ino), nm.as_bytes())
                        .unwrap_or_else(|_| panic!("missing /{nm} under {ino}"));
                    assert_eq!(gn, key(cino), "wrong target for {nm} under {ino}");
                }
                // Replica has no extra children.
                let mut cookie = 0u64;
                let mut count = 0;
                loop {
                    let page = r.readdir(key(ino), cookie, 256).unwrap();
                    for e in &page.entries {
                        let nm = String::from_utf8_lossy(e.name.as_bytes()).into_owned();
                        assert!(
                            model_children.contains_key(&nm),
                            "extra entry {nm} under {ino}"
                        );
                        count += 1;
                    }
                    if page.eof {
                        break;
                    }
                    cookie = page.entries.last().unwrap().cookie;
                }
                assert_eq!(
                    count,
                    model_children.len(),
                    "entry count mismatch under {ino}"
                );
                for &cino in model_children.values() {
                    self.walk(cino, r);
                }
            }
        }
    }

    #[test]
    fn model_random_journal_converges() {
        // Deterministic PRNG (no external dep): xorshift.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for trial in 0..50 {
            let r = seeded_root();
            let mut m = Model::new();

            for _ in 0..300 {
                let op = rng() % 5;
                let dirs = m.dirs();
                let parent = dirs[(rng() as usize) % dirs.len()];
                let nm = format!("n{}", rng() % 40);
                let rec = match op {
                    0 => m.create(parent, &nm, Kind::Reg),
                    1 => m.create(parent, &nm, Kind::Dir),
                    2 => {
                        // content update on an existing file under parent
                        let f = m
                            .children
                            .get(&parent)
                            .and_then(|c| c.values().copied().find(|i| m.kind[i] == Kind::Reg));
                        if let Some(ino) = f {
                            let sz = rng() % 100000;
                            m.size.insert(ino, sz);
                            Some(Rec::Content {
                                node: key(ino),
                                version: rng(),
                                size: sz,
                                mtime: Ts { sec: 1, nsec: 0 },
                                in_progress: false,
                            })
                        } else {
                            None
                        }
                    }
                    3 => m.remove(parent, &nm),
                    _ => {
                        // rename within the tree
                        let names: Vec<String> = m
                            .children
                            .get(&parent)
                            .map(|c| c.keys().cloned().collect())
                            .unwrap_or_default();
                        if names.is_empty() {
                            None
                        } else {
                            let from = &names[(rng() as usize) % names.len()];
                            let node = m.children[&parent][from];
                            let to_dirs = m.dirs();
                            let to_parent = to_dirs[(rng() as usize) % to_dirs.len()];
                            let to_name = format!("r{}", rng() % 40);
                            // Skip renames into own subtree (the guest kernel would reject; our
                            // model doesn't track that) and onto existing names.
                            if m.children.get(&to_parent).map(|c| c.contains_key(&to_name))
                                == Some(true)
                                || is_ancestor(&m, node, to_parent)
                            {
                                None
                            } else {
                                m.children.get_mut(&parent).unwrap().remove(from);
                                m.parent.insert(node, to_parent);
                                m.children
                                    .get_mut(&to_parent)
                                    .unwrap()
                                    .insert(to_name.clone(), node);
                                Some(Rec::Renamed {
                                    from_parent: key(parent),
                                    from_name: name(from),
                                    to_parent: key(to_parent),
                                    to_name: name(&to_name),
                                })
                            }
                        }
                    }
                };
                if let Some(rec) = rec {
                    r.apply_rec(&rec);
                }
            }
            m.assert_matches(&r);
            let _ = trial;
        }
    }

    fn is_ancestor(m: &Model, anc: u64, mut node: u64) -> bool {
        loop {
            if node == anc {
                return true;
            }
            let p = m.parent[&node];
            if p == node {
                return false;
            }
            node = p;
        }
    }
}
