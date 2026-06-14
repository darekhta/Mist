//! Content-addressed store for Mist's data plane (design 04 §6).
//!
//! Layout: `<root>/blobs/<2-hex>/<blake3-hex>` chunk files + `<root>/manifests.redb` with two
//! tables: manifests `(share, ino, gen, content_version) → postcard Vec<ChunkRef>` and chunks
//! `hash → (len, last_access_secs)`.
//!
//! Performance contract: the hit path is one redb read + one `pread` — **no hashing** (the
//! 1 MiB-hit ≤ 200 µs gate forbids it). Integrity comes from hash-verify on ingest plus
//! `scrub()`; a corrupt or missing blob is dropped and surfaces as a miss, and the caller
//! refetches from the guest (self-heal). There are no chunk refcounts for the same reason:
//! evicting a still-referenced chunk merely turns the next read into a partial miss.
//!
//! All methods are synchronous; callers on async runtimes wrap cold paths in `spawn_blocking`
//! (a warm `pread` out of UBC is cheaper than the task hop, so hits may be called inline).

use parking_lot::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// manifests: (share, ino, generation, content_version) → postcard `Vec<ChunkRef>`
const MANIFESTS: TableDefinition<(u16, u64, u32, u64), &[u8]> = TableDefinition::new("manifests");
/// chunks: blake3 → (len, last_access unix-secs)
const CHUNKS: TableDefinition<[u8; 32], (u32, u64)> = TableDefinition::new("chunks");

/// Stored last-access is only rewritten when it is older than this, bounding write
/// amplification on hot chunks to ≤1 redb commit per chunk per interval.
const ACCESS_WRITE_GRANULARITY: Duration = Duration::from_secs(3600);
/// Chunks accessed within this window are not evicted (design: "opened in the last 10 min").
const EVICTION_PROTECTION: Duration = Duration::from_secs(600);

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("db: {0}")]
    Db(Box<redb::Error>),
}

macro_rules! from_redb {
    ($($t:ty),*) => {$(
        impl From<$t> for CasError {
            fn from(e: $t) -> Self {
                CasError::Db(Box::new(e.into()))
            }
        }
    )*};
}
from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError
);

/// Identifies one immutable file content version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ManifestKey {
    pub share: u16,
    pub ino: u64,
    pub generation: u32,
    pub content_version: u64,
}

impl ManifestKey {
    fn tuple(&self) -> (u16, u64, u32, u64) {
        (self.share, self.ino, self.generation, self.content_version)
    }
}

/// One chunk of a file: `len` bytes at `off`, stored as blob `hash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub off: u64,
    pub len: u32,
    pub hash: [u8; 32],
}

#[derive(Debug, Default, Clone)]
pub struct CasStats {
    pub hits: u64,
    pub misses: u64,
    pub blobs: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
    pub ingested_bytes: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
    pub corrupt_dropped: u64,
}

#[derive(Debug, Default, Clone)]
pub struct ScrubReport {
    pub checked: u64,
    pub corrupt: u64,
    pub missing: u64,
}

#[derive(Debug, Clone)]
pub struct CasConfig {
    pub root: PathBuf,
    /// High watermark: ingest that crosses this triggers eviction…
    pub max_bytes: u64,
    /// …down to this fraction of `max_bytes` (default 0.9).
    pub evict_to: f64,
}

impl CasConfig {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        CasConfig {
            root: root.into(),
            max_bytes,
            evict_to: 0.9,
        }
    }
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    ingested_bytes: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
    corrupt_dropped: AtomicU64,
}

pub struct CasStore {
    cfg: CasConfig,
    db: Database,
    /// Sum of stored chunk lens; kept exact (rebuilt at open, adjusted on ingest/evict/drop).
    total_bytes: AtomicU64,
    blobs: AtomicU64,
    /// In-process fine-grained recency: hash → unix-secs of last touch. The redb copy is only
    /// rewritten when stale by `ACCESS_WRITE_GRANULARITY`; eviction merges both views.
    recency: Mutex<HashMap<[u8; 32], u64>>,
    /// Open blob file handles for the zero-copy (sendfile) read path. An open fd stays valid
    /// even if the blob is evicted/unlinked, so there's nothing to invalidate; FIFO capped.
    blob_fds: Mutex<Vec<([u8; 32], std::sync::Arc<std::fs::File>)>>,
    c: Counters,
}

impl std::fmt::Debug for CasStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CasStore")
            .field("root", &self.cfg.root)
            .field("total_bytes", &self.total_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn hex(hash: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in hash {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

impl CasStore {
    pub fn open(cfg: CasConfig) -> Result<Self, CasError> {
        std::fs::create_dir_all(cfg.root.join("blobs"))?;
        let db = Database::create(cfg.root.join("manifests.redb"))?;
        // Ensure both tables exist, and rebuild the byte/blob accounting.
        let (total, blobs) = {
            let w = db.begin_write()?;
            {
                w.open_table(MANIFESTS)?;
                w.open_table(CHUNKS)?;
            }
            w.commit()?;
            let r = db.begin_read()?;
            let t = r.open_table(CHUNKS)?;
            let mut total = 0u64;
            let mut blobs = 0u64;
            for row in t.iter()? {
                let (_, v) = row?;
                total += v.value().0 as u64;
                blobs += 1;
            }
            (total, blobs)
        };
        Ok(CasStore {
            cfg,
            db,
            total_bytes: AtomicU64::new(total),
            blobs: AtomicU64::new(blobs),
            recency: Mutex::new(HashMap::new()),
            blob_fds: Mutex::new(Vec::new()),
            c: Counters::default(),
        })
    }

    /// Write transaction with no per-commit fsync — and, in redb, no survival of process death
    /// either (non-durable commits live in process memory until the next durable commit).
    /// Only safe for state whose loss self-heals: access times, eviction accounting, drops
    /// (fingerprint rotation re-invalidates), clears. Ingest paths MUST use `write_txn_durable`
    /// or a killed hostd restarts with an empty cache and the warm-restart gate fails.
    fn write_txn(&self) -> Result<redb::WriteTransaction, CasError> {
        let mut w = self.db.begin_write()?;
        let _ = w.set_durability(redb::Durability::None);
        Ok(w)
    }

    /// Durable write transaction (default redb durability): one fsync per commit. Used for
    /// ingest so cached chunks survive hostd restarts; also persists any earlier non-durable
    /// commits as a side effect.
    fn write_txn_durable(&self) -> Result<redb::WriteTransaction, CasError> {
        Ok(self.db.begin_write()?)
    }

    fn blob_path(&self, hash: &[u8; 32]) -> PathBuf {
        let h = hex(hash);
        self.cfg.root.join("blobs").join(&h[..2]).join(&h)
    }

    // ---- manifests ------------------------------------------------------------------------

    /// Record the chunk list for one content version, dropping any stale versions of the same
    /// node (their chunks age out via LRU; shared chunks stay warm).
    pub fn put_manifest(&self, key: ManifestKey, chunks: &[ChunkRef]) -> Result<(), CasError> {
        let enc = postcard::to_allocvec(chunks).expect("postcard vec encode");
        let w = self.write_txn_durable()?;
        {
            let mut t = w.open_table(MANIFESTS)?;
            let (share, ino, generation, _) = key.tuple();
            let stale: Vec<(u16, u64, u32, u64)> = t
                .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .filter(|k| k.3 != key.content_version)
                .collect();
            for k in stale {
                t.remove(k)?;
            }
            t.insert(key.tuple(), enc.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    /// Insert/replace one chunk in a manifest as a single read-modify-write transaction
    /// (concurrent chunk ingests for the same file must not lose each other's entries).
    /// Stale versions of the node are dropped like in `put_manifest`.
    pub fn merge_chunk(&self, key: ManifestKey, chunk: ChunkRef) -> Result<(), CasError> {
        let w = self.write_txn_durable()?;
        {
            let mut t = w.open_table(MANIFESTS)?;
            let (share, ino, generation, _) = key.tuple();
            let stale: Vec<(u16, u64, u32, u64)> = t
                .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .filter(|k| k.3 != key.content_version)
                .collect();
            for k in stale {
                t.remove(k)?;
            }
            let mut chunks: Vec<ChunkRef> = t
                .get(key.tuple())?
                .and_then(|v| postcard::from_bytes(v.value()).ok())
                .unwrap_or_default();
            match chunks.binary_search_by_key(&chunk.off, |c| c.off) {
                Ok(i) => chunks[i] = chunk,
                Err(i) => chunks.insert(i, chunk),
            }
            let enc = postcard::to_allocvec(&chunks).expect("postcard vec encode");
            t.insert(key.tuple(), enc.as_slice())?;
        }
        w.commit()?;
        Ok(())
    }

    /// Exact-version lookup; a different content version simply misses.
    pub fn get_manifest(&self, key: ManifestKey) -> Result<Option<Vec<ChunkRef>>, CasError> {
        let r = self.db.begin_read()?;
        let t = r.open_table(MANIFESTS)?;
        let Some(v) = t.get(key.tuple())? else {
            return Ok(None);
        };
        Ok(postcard::from_bytes(v.value()).ok())
    }

    /// Re-key a manifest (the incremental write-ingest path assembles chunks under a WIP key
    /// while the fingerprint is still moving, then rebinds once the attrs settle). Cheap: the
    /// blobs stay put, one small durable transaction.
    pub fn rebind_manifest(&self, from: ManifestKey, to: ManifestKey) -> Result<(), CasError> {
        // Volatile like the stash ingest it follows; the caller's delayed persist covers it.
        let w = self.write_txn()?;
        let mut moved = false;
        {
            let mut t = w.open_table(MANIFESTS)?;
            let enc = match t.remove(from.tuple())? {
                Some(v) => {
                    moved = true;
                    v.value().to_vec()
                }
                None => Vec::new(),
            };
            if !moved {
                drop(t);
                drop(enc);
                // fall through to commit-noop below
            } else {
                let (share, ino, generation, _) = to.tuple();
                let stale: Vec<(u16, u64, u32, u64)> = t
                    .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                    .filter_map(|r| r.ok())
                    .map(|(k, _)| k.value())
                    .filter(|k| k.3 != to.content_version)
                    .collect();
                for k in stale {
                    t.remove(k)?;
                }
                t.insert(to.tuple(), enc.as_slice())?;
            }
        }
        w.commit()?;
        Ok(())
    }

    /// Drop all versions of a node (Mac-side write invalidation, unlink). Fast no-op when the
    /// node has no manifests — the common case for written-but-never-read files — so callers
    /// can invoke it on every Mac write without paying a write transaction.
    pub fn drop_node(&self, share: u16, ino: u64, generation: u32) -> Result<(), CasError> {
        {
            let r = self.db.begin_read()?;
            let t = r.open_table(MANIFESTS)?;
            if t.range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .next()
                .is_none()
            {
                return Ok(());
            }
        }
        let w = self.write_txn()?;
        {
            let mut t = w.open_table(MANIFESTS)?;
            let keys: Vec<(u16, u64, u32, u64)> = t
                .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();
            for k in keys {
                t.remove(k)?;
            }
        }
        w.commit()?;
        Ok(())
    }

    /// Drop every manifest of a share (reseed/epoch change: content versions restart at 0, so
    /// old manifests would alias new content). Blobs age out via LRU.
    pub fn drop_share(&self, share: u16) -> Result<(), CasError> {
        let w = self.write_txn()?;
        {
            let mut t = w.open_table(MANIFESTS)?;
            let keys: Vec<(u16, u64, u32, u64)> = t
                .range((share, 0, 0, 0)..=(share, u64::MAX, u32::MAX, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();
            for k in keys {
                t.remove(k)?;
            }
        }
        w.commit()?;
        Ok(())
    }

    // ---- blobs ----------------------------------------------------------------------------

    /// Write-through ingest of one chunk: blob file + CHUNKS row + manifest merge in a single
    /// durable transaction (one fsync per 4 MiB chunk). The combined form halves the fsyncs of
    /// `put_blob` + `merge_chunk` and is what the read path uses.
    pub fn ingest_chunk(
        &self,
        key: ManifestKey,
        off: u64,
        data: &[u8],
    ) -> Result<[u8; 32], CasError> {
        let hash = *blake3::hash(data).as_bytes();
        self.write_blob_file(&hash, data)?;
        let now = now_secs();
        let fresh;
        let w = self.write_txn_durable()?;
        {
            let mut t = w.open_table(CHUNKS)?;
            fresh = t.insert(hash, (data.len() as u32, now))?.is_none();
            let mut m = w.open_table(MANIFESTS)?;
            let (share, ino, generation, _) = key.tuple();
            let stale: Vec<(u16, u64, u32, u64)> = m
                .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .filter(|k| k.3 != key.content_version)
                .collect();
            for k in stale {
                m.remove(k)?;
            }
            let mut chunks: Vec<ChunkRef> = m
                .get(key.tuple())?
                .and_then(|v| postcard::from_bytes(v.value()).ok())
                .unwrap_or_default();
            let chunk = ChunkRef {
                off,
                len: data.len() as u32,
                hash,
            };
            match chunks.binary_search_by_key(&off, |c| c.off) {
                Ok(i) => chunks[i] = chunk,
                Err(i) => chunks.insert(i, chunk),
            }
            let enc = postcard::to_allocvec(&chunks).expect("postcard vec encode");
            m.insert(key.tuple(), enc.as_slice())?;
        }
        w.commit()?;
        self.account_ingest(&hash, data.len(), fresh, now)?;
        Ok(hash)
    }

    /// Batch ingest: all chunks of one manifest in a SINGLE durable transaction (one fsync
    /// for the whole file instead of one per chunk — the write-stash warm-read path needs the
    /// manifest visible within the client's close-to-reopen gap).
    pub fn ingest_chunks(
        &self,
        key: ManifestKey,
        chunks: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), CasError> {
        self.ingest_chunks_inner(key, chunks, true)
    }

    /// Fsync-free batch ingest: visible immediately (serves reads), durable only after the
    /// next [`Self::persist`]. The fsync of a durable ingest contends with an immediately-following
    /// read of the same data — exactly the read-after-write window the stash exists for.
    pub fn ingest_chunks_volatile(
        &self,
        key: ManifestKey,
        chunks: Vec<(u64, Vec<u8>)>,
    ) -> Result<(), CasError> {
        self.ingest_chunks_inner(key, chunks, false)
    }

    /// Empty durable commit: persists all earlier volatile commits (one fsync).
    pub fn persist(&self) -> Result<(), CasError> {
        let w = self.write_txn_durable()?;
        w.commit()?;
        Ok(())
    }

    fn ingest_chunks_inner(
        &self,
        key: ManifestKey,
        chunks: Vec<(u64, Vec<u8>)>,
        durable: bool,
    ) -> Result<(), CasError> {
        let now = now_secs();
        let mut refs = Vec::with_capacity(chunks.len());
        let mut fresh_total = 0u64;
        let mut fresh_blobs = 0u64;
        let hashed: Vec<([u8; 32], u64, Vec<u8>)> = chunks
            .into_iter()
            .map(|(off, data)| {
                let h = *blake3::hash(&data).as_bytes();
                (h, off, data)
            })
            .collect();
        for (h, _, data) in &hashed {
            self.write_blob_file(h, data)?;
        }
        let w = if durable {
            self.write_txn_durable()?
        } else {
            self.write_txn()?
        };
        {
            let mut t = w.open_table(CHUNKS)?;
            for (h, off, data) in &hashed {
                if t.insert(*h, (data.len() as u32, now))?.is_none() {
                    fresh_total += data.len() as u64;
                    fresh_blobs += 1;
                }
                refs.push(ChunkRef {
                    off: *off,
                    len: data.len() as u32,
                    hash: *h,
                });
                self.c
                    .ingested_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            let mut m = w.open_table(MANIFESTS)?;
            let (share, ino, generation, _) = key.tuple();
            let stale: Vec<(u16, u64, u32, u64)> = m
                .range((share, ino, generation, 0)..=(share, ino, generation, u64::MAX))?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .filter(|k| k.3 != key.content_version)
                .collect();
            for k in stale {
                m.remove(k)?;
            }
            refs.sort_by_key(|c| c.off);
            let enc = postcard::to_allocvec(&refs).expect("postcard vec encode");
            m.insert(key.tuple(), enc.as_slice())?;
        }
        w.commit()?;
        self.total_bytes.fetch_add(fresh_total, Ordering::Relaxed);
        self.blobs.fetch_add(fresh_blobs, Ordering::Relaxed);
        {
            let mut r = self.recency.lock();
            for (h, _, _) in &hashed {
                r.insert(*h, now);
            }
        }
        if self.total_bytes.load(Ordering::Relaxed) > self.cfg.max_bytes {
            self.evict()?;
        }
        Ok(())
    }

    fn write_blob_file(&self, hash: &[u8; 32], data: &[u8]) -> Result<(), CasError> {
        let path = self.blob_path(hash);
        if !path.exists() {
            let dir = path.parent().expect("blob dir");
            std::fs::create_dir_all(dir)?;
            let tmp = dir.join(format!(".tmp.{}", hex(hash)));
            std::fs::write(&tmp, data)?;
            std::fs::rename(&tmp, &path)?;
        }
        Ok(())
    }

    fn account_ingest(
        &self,
        hash: &[u8; 32],
        len: usize,
        fresh: bool,
        now: u64,
    ) -> Result<(), CasError> {
        if fresh {
            self.total_bytes.fetch_add(len as u64, Ordering::Relaxed);
            self.blobs.fetch_add(1, Ordering::Relaxed);
        }
        self.c
            .ingested_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.recency.lock().insert(*hash, now);
        if self.total_bytes.load(Ordering::Relaxed) > self.cfg.max_bytes {
            self.evict()?;
        }
        Ok(())
    }

    /// Ingest one chunk: hash, write blob if absent (tmp+rename), account, evict if over the
    /// watermark. Returns the chunk hash.
    pub fn put_blob(&self, data: &[u8]) -> Result<[u8; 32], CasError> {
        let hash = *blake3::hash(data).as_bytes();
        self.write_blob_file(&hash, data)?;
        let now = now_secs();
        // Accounting follows the CHUNKS row, not the file: new row = new blob.
        let fresh;
        let w = self.write_txn_durable()?;
        {
            let mut t = w.open_table(CHUNKS)?;
            fresh = t.insert(hash, (data.len() as u32, now))?.is_none();
        }
        w.commit()?;
        self.account_ingest(&hash, data.len(), fresh, now)?;
        Ok(hash)
    }

    /// Read `len` bytes at `off` within a blob. `None` = miss (evicted, never stored, or —
    /// when `verify` is set or the read fails — corrupt, in which case the blob is dropped so
    /// the caller refetches). The fast path is a single `pread`, no hashing.
    pub fn get_blob_range(
        &self,
        hash: &[u8; 32],
        off: u32,
        len: u32,
        verify: bool,
    ) -> Result<Option<Vec<u8>>, CasError> {
        self.get_blob_range_into(hash, off, len, verify, None)
    }

    /// Zero-copy read handle: the blob's open File + absolute offset for `sendfile`-style
    /// transmission. Returns None on a missing blob (caller falls back to the copying path).
    pub fn blob_file_range(
        &self,
        hash: &[u8; 32],
        off: u32,
        len: u32,
    ) -> Option<(std::sync::Arc<std::fs::File>, u64)> {
        let file = {
            let cache = self.blob_fds.lock();
            cache
                .iter()
                .find(|(h, _)| h == hash)
                .map(|(_, f)| f.clone())
        };
        let file = match file {
            Some(f) => f,
            None => {
                let f = std::sync::Arc::new(std::fs::File::open(self.blob_path(hash)).ok()?);
                let mut cache = self.blob_fds.lock();
                if cache.len() >= 32 {
                    cache.remove(0);
                }
                cache.push((*hash, f.clone()));
                f
            }
        };
        let _ = len;
        self.touch(hash);
        self.c.hits.fetch_add(1, Ordering::Relaxed);
        Some((file, off as u64))
    }

    /// Like [`Self::get_blob_range`], reusing `out` (a recycled buffer) when provided.
    pub fn get_blob_range_into(
        &self,
        hash: &[u8; 32],
        off: u32,
        len: u32,
        verify: bool,
        out: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, CasError> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.blob_path(hash);
        let Ok(mut f) = std::fs::File::open(&path) else {
            self.c.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if verify {
            let mut whole = Vec::new();
            if f.read_to_end(&mut whole).is_err()
                || blake3::hash(&whole).as_bytes() != hash
                || (off as usize + len as usize) > whole.len()
            {
                drop(f);
                self.drop_corrupt(hash)?;
                self.c.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            self.touch(hash);
            self.c.hits.fetch_add(1, Ordering::Relaxed);
            whole.drain(..off as usize);
            whole.truncate(len as usize);
            return Ok(Some(whole));
        }
        // No zero-init: seek + bounded read_to_end appends into reserved capacity.
        let mut buf = out.unwrap_or_else(|| Vec::with_capacity(len as usize));
        buf.clear();
        buf.reserve(len as usize);
        let seek_ok = f.seek(SeekFrom::Start(off as u64)).is_ok();
        let ok = seek_ok
            && std::io::Read::take(&mut f, len as u64)
                .read_to_end(&mut buf)
                .is_ok()
            && buf.len() == len as usize;
        if !ok {
            // Short blob = truncation corruption: drop it, report a miss, caller refetches.
            drop(f);
            self.drop_corrupt(hash)?;
            self.c.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.touch(hash);
        self.c.hits.fetch_add(1, Ordering::Relaxed);
        Ok(Some(buf))
    }

    /// Whole-blob read (no verify): `get_blob_range(hash, 0, stored_len)`.
    pub fn get_blob(&self, hash: &[u8; 32]) -> Result<Option<Vec<u8>>, CasError> {
        let len = {
            let r = self.db.begin_read()?;
            let t = r.open_table(CHUNKS)?;
            match t.get(*hash)? {
                Some(v) => v.value().0,
                None => {
                    self.c.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        };
        self.get_blob_range(hash, 0, len, false)
    }

    /// Bump in-process recency; write back to redb only when the stored value is stale enough.
    fn touch(&self, hash: &[u8; 32]) {
        let now = now_secs();
        let stale = {
            let mut r = self.recency.lock();
            match r.insert(*hash, now) {
                Some(prev) => now.saturating_sub(prev) >= ACCESS_WRITE_GRANULARITY.as_secs(),
                None => true, // unknown in-process: stored copy may be very old
            }
        };
        if stale {
            let res: Result<(), CasError> = (|| {
                let w = self.write_txn()?;
                {
                    let mut t = w.open_table(CHUNKS)?;
                    let len = t.get(*hash)?.map(|v| v.value().0);
                    if let Some(len) = len {
                        t.insert(*hash, (len, now))?;
                    }
                }
                w.commit()?;
                Ok(())
            })();
            if let Err(e) = res {
                tracing::warn!(error = %e, "cas access write-back failed");
            }
        }
    }

    fn drop_corrupt(&self, hash: &[u8; 32]) -> Result<(), CasError> {
        let _ = std::fs::remove_file(self.blob_path(hash));
        let w = self.write_txn()?;
        let removed = {
            let mut t = w.open_table(CHUNKS)?;
            t.remove(*hash)?.map(|v| v.value().0)
        };
        w.commit()?;
        if let Some(len) = removed {
            self.total_bytes.fetch_sub(len as u64, Ordering::Relaxed);
            self.blobs.fetch_sub(1, Ordering::Relaxed);
        }
        self.recency.lock().remove(hash);
        self.c.corrupt_dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(hash = %hex(hash), "cas blob corrupt/short; dropped (will refetch)");
        Ok(())
    }

    // ---- eviction & scrub -----------------------------------------------------------------

    /// LRU-evict down to `evict_to × max_bytes`. Chunks touched within the protection window
    /// survive even if that leaves us above target (soft watermark; logged).
    pub fn evict(&self) -> Result<u64, CasError> {
        let target = (self.cfg.max_bytes as f64 * self.cfg.evict_to) as u64;
        let mut total = self.total_bytes.load(Ordering::Relaxed);
        if total <= target {
            return Ok(0);
        }
        let now = now_secs();
        // Merge stored + in-process recency, oldest first.
        let mut rows: Vec<([u8; 32], u32, u64)> = {
            let r = self.db.begin_read()?;
            let t = r.open_table(CHUNKS)?;
            let recency = self.recency.lock();
            t.iter()?
                .filter_map(|r| r.ok())
                .map(|(k, v)| {
                    let h = k.value();
                    let (len, stored) = v.value();
                    let last = recency.get(&h).copied().unwrap_or(0).max(stored);
                    (h, len, last)
                })
                .collect()
        };
        rows.sort_by_key(|&(_, _, last)| last);
        let mut victims = Vec::new();
        let mut freed = 0u64;
        for &(h, len, last) in &rows {
            if total.saturating_sub(freed) <= target {
                break;
            }
            if now.saturating_sub(last) < EVICTION_PROTECTION.as_secs() {
                continue; // hot: protected
            }
            victims.push(h);
            freed += len as u64;
        }
        if total.saturating_sub(freed) > self.cfg.max_bytes {
            // Protection couldn't reach even the hard cap (e.g. a streaming workload where
            // everything is recent): max_bytes wins — evict protected chunks oldest-first.
            let chosen: std::collections::HashSet<[u8; 32]> = victims.iter().copied().collect();
            for &(h, len, _) in &rows {
                if total.saturating_sub(freed) <= target {
                    break;
                }
                if chosen.contains(&h) {
                    continue;
                }
                victims.push(h);
                freed += len as u64;
            }
            tracing::info!(total, "cas hard cap: evicting inside protection window");
        }
        if victims.is_empty() {
            return Ok(0);
        }
        let w = self.write_txn()?;
        {
            let mut t = w.open_table(CHUNKS)?;
            for h in &victims {
                t.remove(*h)?;
            }
        }
        w.commit()?;
        let mut recency = self.recency.lock();
        for h in &victims {
            let _ = std::fs::remove_file(self.blob_path(h));
            recency.remove(h);
        }
        drop(recency);
        total = self
            .total_bytes
            .fetch_sub(freed, Ordering::Relaxed)
            .saturating_sub(freed);
        self.blobs
            .fetch_sub(victims.len() as u64, Ordering::Relaxed);
        self.c
            .evictions
            .fetch_add(victims.len() as u64, Ordering::Relaxed);
        self.c.evicted_bytes.fetch_add(freed, Ordering::Relaxed);
        tracing::info!(
            evicted = victims.len(),
            freed,
            total,
            "cas eviction complete"
        );
        Ok(freed)
    }

    /// Re-hash up to `sample` blobs (0 = all); drop corrupt/missing ones.
    pub fn scrub(&self, sample: usize) -> Result<ScrubReport, CasError> {
        let hashes: Vec<[u8; 32]> = {
            let r = self.db.begin_read()?;
            let t = r.open_table(CHUNKS)?;
            let it = t.iter()?.filter_map(|r| r.ok()).map(|(k, _)| k.value());
            if sample == 0 {
                it.collect()
            } else {
                it.take(sample).collect()
            }
        };
        let mut rep = ScrubReport::default();
        for h in hashes {
            rep.checked += 1;
            match std::fs::read(self.blob_path(&h)) {
                Ok(data) if blake3::hash(&data).as_bytes() == &h => {}
                Ok(_) => {
                    rep.corrupt += 1;
                    self.drop_corrupt(&h)?;
                }
                Err(_) => {
                    rep.missing += 1;
                    self.drop_corrupt(&h)?;
                }
            }
        }
        Ok(rep)
    }

    /// Drop everything (`mist cache clear`).
    pub fn clear(&self) -> Result<(), CasError> {
        let w = self.write_txn()?;
        let _ = w.delete_table(MANIFESTS)?;
        let _ = w.delete_table(CHUNKS)?;
        // Recreate so later transactions find them.
        {
            w.open_table(MANIFESTS)?;
            w.open_table(CHUNKS)?;
        }
        w.commit()?;
        let blobs = self.cfg.root.join("blobs");
        let _ = std::fs::remove_dir_all(&blobs);
        std::fs::create_dir_all(&blobs)?;
        self.total_bytes.store(0, Ordering::Relaxed);
        self.blobs.store(0, Ordering::Relaxed);
        self.recency.lock().clear();
        Ok(())
    }

    pub fn stats(&self) -> CasStats {
        CasStats {
            hits: self.c.hits.load(Ordering::Relaxed),
            misses: self.c.misses.load(Ordering::Relaxed),
            blobs: self.blobs.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            max_bytes: self.cfg.max_bytes,
            ingested_bytes: self.c.ingested_bytes.load(Ordering::Relaxed),
            evictions: self.c.evictions.load(Ordering::Relaxed),
            evicted_bytes: self.c.evicted_bytes.load(Ordering::Relaxed),
            corrupt_dropped: self.c.corrupt_dropped.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(max_bytes: u64) -> (tempfile::TempDir, CasStore) {
        let dir = tempfile::tempdir().unwrap();
        let cas = CasStore::open(CasConfig::new(dir.path().join("cas"), max_bytes)).unwrap();
        (dir, cas)
    }

    fn key(ino: u64, version: u64) -> ManifestKey {
        ManifestKey {
            share: 0,
            ino,
            generation: 1,
            content_version: version,
        }
    }

    #[test]
    fn roundtrip_blob_and_manifest() {
        let (_d, cas) = store(1 << 30);
        let data = vec![7u8; 300_000];
        let hash = cas.put_blob(&data).unwrap();
        let chunks = vec![ChunkRef {
            off: 0,
            len: data.len() as u32,
            hash,
        }];
        cas.put_manifest(key(42, 5), &chunks).unwrap();

        let m = cas.get_manifest(key(42, 5)).unwrap().unwrap();
        assert_eq!(m, chunks);
        let got = cas.get_blob(&hash).unwrap().unwrap();
        assert_eq!(got, data);
        let part = cas.get_blob_range(&hash, 100, 50, false).unwrap().unwrap();
        assert_eq!(part, &data[100..150]);
        let s = cas.stats();
        assert_eq!(s.blobs, 1);
        assert_eq!(s.total_bytes, data.len() as u64);
        assert!(s.hits >= 2);
    }

    #[test]
    fn version_mismatch_misses_and_new_version_drops_stale() {
        let (_d, cas) = store(1 << 30);
        let h = cas.put_blob(b"v1 content").unwrap();
        cas.put_manifest(
            key(7, 1),
            &[ChunkRef {
                off: 0,
                len: 10,
                hash: h,
            }],
        )
        .unwrap();
        // Different content_version: miss.
        assert!(cas.get_manifest(key(7, 2)).unwrap().is_none());
        // Publishing v2 drops the v1 manifest.
        let h2 = cas.put_blob(b"v2 content!").unwrap();
        cas.put_manifest(
            key(7, 2),
            &[ChunkRef {
                off: 0,
                len: 11,
                hash: h2,
            }],
        )
        .unwrap();
        assert!(cas.get_manifest(key(7, 1)).unwrap().is_none());
        assert!(cas.get_manifest(key(7, 2)).unwrap().is_some());
    }

    #[test]
    fn corrupt_blob_drill() {
        let (_d, cas) = store(1 << 30);
        let data = vec![9u8; 100_000];
        let hash = cas.put_blob(&data).unwrap();
        // Flip bytes on disk behind the store's back.
        let path = cas.blob_path(&hash);
        let mut raw = std::fs::read(&path).unwrap();
        raw[50_000] ^= 0xff;
        std::fs::write(&path, &raw).unwrap();

        // Unverified range read can't see the corruption (by design)…
        assert!(cas.get_blob_range(&hash, 0, 1000, false).unwrap().is_some());
        // …but a verified read detects, drops, and self-heals to a miss.
        assert!(cas.get_blob_range(&hash, 0, 1000, true).unwrap().is_none());
        assert!(!path.exists());
        assert!(cas.get_blob(&hash).unwrap().is_none());
        let s = cas.stats();
        assert_eq!(s.corrupt_dropped, 1);
        assert_eq!(s.blobs, 0);
        assert_eq!(s.total_bytes, 0);

        // Truncation is caught even without verify (short read).
        let h2 = cas.put_blob(&data).unwrap();
        let p2 = cas.blob_path(&h2);
        std::fs::write(&p2, &data[..10]).unwrap();
        assert!(
            cas.get_blob_range(&h2, 0, data.len() as u32, false)
                .unwrap()
                .is_none()
        );
        assert_eq!(cas.stats().corrupt_dropped, 2);
    }

    #[test]
    fn scrub_detects_corruption() {
        let (_d, cas) = store(1 << 30);
        let h1 = cas.put_blob(&vec![1u8; 50_000]).unwrap();
        let h2 = cas.put_blob(&vec![2u8; 50_000]).unwrap();
        let p = cas.blob_path(&h2);
        let mut raw = std::fs::read(&p).unwrap();
        raw[0] ^= 1;
        std::fs::write(&p, &raw).unwrap();

        let rep = cas.scrub(0).unwrap();
        assert_eq!(rep.checked, 2);
        assert_eq!(rep.corrupt, 1);
        assert!(cas.get_blob(&h1).unwrap().is_some());
        assert!(cas.get_blob(&h2).unwrap().is_none());
    }

    #[test]
    fn eviction_honors_watermarks() {
        // 1 MiB cap, 100 KiB blobs; protection window ignored via backdated access times.
        let (_d, cas) = store(1 << 20);
        let mut hashes = Vec::new();
        for i in 0..9u8 {
            let h = cas.put_blob(&vec![i; 100_000]).unwrap();
            hashes.push(h);
        }
        assert_eq!(cas.stats().total_bytes, 900_000);
        // Backdate every chunk's access far past the protection window.
        {
            let w = cas.db.begin_write().unwrap();
            {
                let mut t = w.open_table(CHUNKS).unwrap();
                let old = now_secs() - 7200;
                for (i, h) in hashes.iter().enumerate() {
                    let len = t.get(*h).unwrap().unwrap().value().0;
                    t.insert(*h, (len, old + i as u64)).unwrap();
                }
            }
            w.commit().unwrap();
            cas.recency.lock().clear();
        }
        // Two more 100 KiB blobs push us over 1 MiB → evict down to ≤ 90% (943 718).
        for i in 9..11u8 {
            cas.put_blob(&vec![i; 100_000]).unwrap();
        }
        let s = cas.stats();
        assert!(
            s.total_bytes <= (1u64 << 20) * 9 / 10,
            "total {} above low watermark",
            s.total_bytes
        );
        assert!(s.evictions >= 1);
        // Oldest (backdated) chunks went first; the two fresh ones survive.
        assert!(cas.get_blob(&hashes[0]).unwrap().is_none());
        let s2 = cas.stats();
        assert!(s2.total_bytes <= s.total_bytes);
    }

    #[test]
    fn eviction_hard_cap_beats_protection_window() {
        // Streaming workload: every chunk is recent (protected), yet total must never stay
        // above max_bytes — the hard cap overrides the protection window.
        let (_d, cas) = store(1 << 20);
        for i in 0..12u8 {
            cas.put_blob(&vec![i; 100_000]).unwrap();
        }
        let s = cas.stats();
        assert!(
            s.total_bytes <= 1 << 20,
            "total {} exceeds hard cap",
            s.total_bytes
        );
        assert!(s.evictions >= 1);
    }

    #[test]
    fn ingest_survives_unclean_process_death() {
        // The regression this guards: redb non-durable commits live in process memory, so a
        // SIGKILL'd hostd used to restart with an EMPTY cache. Ingest must commit durably.
        // Child mode (re-exec'd below): ingest one chunk, then die without unwinding.
        if let Ok(root) = std::env::var("CAS_KILL_CHILD_ROOT") {
            let cas = CasStore::open(CasConfig::new(root, 1 << 30)).unwrap();
            cas.ingest_chunk(key(9, 1), 0, &vec![5u8; 200_000]).unwrap();
            std::process::abort();
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cas");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "tests::ingest_survives_unclean_process_death",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("CAS_KILL_CHILD_ROOT", &root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "child must die by abort");
        let cas2 = CasStore::open(CasConfig::new(&root, 1 << 30)).unwrap();
        let m = cas2
            .get_manifest(key(9, 1))
            .unwrap()
            .expect("manifest survived process death");
        assert_eq!(m[0].len, 200_000);
        assert_eq!(
            cas2.get_blob(&m[0].hash).unwrap().unwrap().len(),
            200_000,
            "blob readable after unclean death"
        );
    }

    #[test]
    fn reopen_rebuilds_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cas");
        let hash;
        {
            let cas = CasStore::open(CasConfig::new(&root, 1 << 30)).unwrap();
            hash = cas.put_blob(&vec![3u8; 123_456]).unwrap();
            cas.put_manifest(
                key(1, 1),
                &[ChunkRef {
                    off: 0,
                    len: 123_456,
                    hash,
                }],
            )
            .unwrap();
        }
        let cas = CasStore::open(CasConfig::new(&root, 1 << 30)).unwrap();
        let s = cas.stats();
        assert_eq!(s.blobs, 1);
        assert_eq!(s.total_bytes, 123_456);
        assert!(cas.get_manifest(key(1, 1)).unwrap().is_some());
        assert_eq!(cas.get_blob(&hash).unwrap().unwrap().len(), 123_456);
    }

    /// Perf check (design 09): CAS hit of 1 MiB ≤ 200 µs. Run explicitly:
    /// `cargo test -p mist-cas --release -- --ignored --nocapture gate_hit`
    #[test]
    #[ignore = "perf gate; run --release on quiet machine"]
    fn gate_hit_1mib_under_200us() {
        let (_d, cas) = store(1 << 30);
        let data = vec![0xabu8; 1 << 20];
        let hash = cas.put_blob(&data).unwrap();
        cas.put_manifest(
            key(1, 1),
            &[ChunkRef {
                off: 0,
                len: 1 << 20,
                hash,
            }],
        )
        .unwrap();
        // Warm (page cache + redb).
        for _ in 0..16 {
            assert!(cas.get_manifest(key(1, 1)).unwrap().is_some());
            assert!(
                cas.get_blob_range(&hash, 0, 1 << 20, false)
                    .unwrap()
                    .is_some()
            );
        }
        let n = 200;
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            let m = cas.get_manifest(key(1, 1)).unwrap().unwrap();
            let c = &m[0];
            let b = cas
                .get_blob_range(&c.hash, 0, c.len, false)
                .unwrap()
                .unwrap();
            assert_eq!(b.len(), 1 << 20);
        }
        let per = t0.elapsed() / n;
        println!("CAS hit 1 MiB (manifest + pread): {per:?}/op");
        assert!(per < Duration::from_micros(200), "gate: {per:?} ≥ 200 µs");
    }

    #[test]
    fn drop_node_invalidates_all_versions() {
        let (_d, cas) = store(1 << 30);
        let h = cas.put_blob(b"x").unwrap();
        let c = [ChunkRef {
            off: 0,
            len: 1,
            hash: h,
        }];
        cas.put_manifest(key(5, 1), &c).unwrap();
        cas.drop_node(0, 5, 1).unwrap();
        assert!(cas.get_manifest(key(5, 1)).unwrap().is_none());
    }
}
