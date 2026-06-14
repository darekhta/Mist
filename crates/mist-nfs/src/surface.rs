//! The seam between the NFS server and the Mist core. The server knows nothing about replicas,
//! vsock, or journals — only this trait. hostd implements it over the replica + RPC client.

use mist_proto::{Attr, NodeKey};
use std::future::Future;
use std::pin::Pin;

/// NFS-facing error; maps to nfsstat3 in `nfs3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NfsError {
    NoEnt,
    NotDir,
    IsDir,
    NotSymlink,
    Stale,
    Access,
    Io,
    /// Directory not fully seeded yet — the server replies JUKEBOX so the client retries.
    Jukebox,
    NameTooLong,
    NotEmpty,
    Exist,
    NoSpace,
    Rofs,
}

impl NfsError {
    /// Map a guest errno (from a mutation RPC) to an NFS error.
    pub fn from_errno(errno: i32) -> NfsError {
        match errno {
            2 => NfsError::NoEnt,        // ENOENT
            13 | 1 => NfsError::Access,  // EACCES / EPERM
            17 => NfsError::Exist,       // EEXIST
            20 => NfsError::NotDir,      // ENOTDIR
            21 => NfsError::IsDir,       // EISDIR
            28 => NfsError::NoSpace,     // ENOSPC
            30 => NfsError::Rofs,        // EROFS
            36 => NfsError::NameTooLong, // ENAMETOOLONG
            39 => NfsError::NotEmpty,    // ENOTEMPTY
            116 => NfsError::Stale,      // ESTALE
            _ => NfsError::Io,
        }
    }
}

pub type NfsResult<T> = Result<T, NfsError>;

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: Vec<u8>,
    pub node: NodeKey,
    pub cookie: u64,
    /// Present only for READDIRPLUS.
    pub attr: Option<Attr>,
}

#[derive(Debug, Clone)]
pub struct ReadDirPage {
    pub entries: Vec<DirEntry>,
    pub eof: bool,
    /// Directory generation — used as the NFS cookieverf so the client detects concurrent change.
    pub cookieverf: u64,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub data: Vec<u8>,
    pub eof: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct FsStat {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub avail_bytes: u64,
    pub total_files: u64,
    pub free_files: u64,
}

impl Default for FsStat {
    fn default() -> Self {
        // Plausible defaults; the guest's real statfs can refine these later.
        FsStat {
            total_bytes: 1 << 50,
            free_bytes: 1 << 49,
            avail_bytes: 1 << 49,
            total_files: 1 << 30,
            free_files: 1 << 29,
        }
    }
}

pub type ReadFuture<'a> = Pin<Box<dyn Future<Output = NfsResult<ReadResult>> + Send + 'a>>;

/// Zero-copy read descriptor: send `len` bytes at `off` of `file` straight to the client
/// socket (`sendfile`). Only offered when the whole request is served by one local file.
#[derive(Debug, Clone)]
pub struct SendableRead {
    pub file: std::sync::Arc<std::fs::File>,
    pub off: u64,
    pub len: u32,
    pub eof: bool,
}

pub type SendableFuture<'a> = Pin<Box<dyn Future<Output = Option<SendableRead>> + Send + 'a>>;
pub type MutFuture<'a, T> = Pin<Box<dyn Future<Output = NfsResult<T>> + Send + 'a>>;

/// The kind of object to create (NFSv3 CREATE/MKDIR/SYMLINK/MKNOD).
#[derive(Debug, Clone)]
pub enum CreateKind {
    File {
        exclusive: bool,
    },
    Dir,
    Symlink {
        target: Vec<u8>,
    },
    Fifo,
    Socket,
    Device {
        is_block: bool,
        major: u32,
        minor: u32,
    },
}

/// Fields to change in SETATTR; `None` leaves a field untouched.
#[derive(Debug, Clone, Default)]
pub struct SetAttr {
    pub mode: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub mtime: Option<mist_proto::Ts>,
}

/// Implemented by the Mist core; consumed by the NFS server. Metadata reads are synchronous
/// (answered from the RAM replica); reads and all mutations are async (they cross the VM boundary).
pub trait MountSurface: Send + Sync + 'static {
    /// The exported share's identity (for fsid in fattr3) and root node.
    fn share_id(&self) -> u16;
    fn root(&self) -> NodeKey;

    fn getattr(&self, node: NodeKey) -> NfsResult<Attr>;
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)>;
    fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        max_entries: usize,
        want_attrs: bool,
    ) -> NfsResult<ReadDirPage>;
    fn readlink(&self, node: NodeKey) -> NfsResult<Vec<u8>>;
    /// Parent directory of a node (NFSv4 LOOKUPP). Surfaces without parent tracking may leave
    /// the default.
    fn parent(&self, _node: NodeKey) -> NfsResult<NodeKey> {
        Err(NfsError::NoEnt)
    }
    fn read(&self, node: NodeKey, offset: u64, count: u32) -> ReadFuture<'_>;
    /// Zero-copy fast path for `read` (servers try this first and fall back). Default: none.
    fn read_sendable(&self, _node: NodeKey, _offset: u64, _count: u32) -> SendableFuture<'_> {
        Box::pin(async { None })
    }

    /// Fused read: append the payload DIRECTLY into `out` (the wire buffer) and return eof.
    /// Saves the intermediate data buffer and its copy on the streaming path. `None` = caller
    /// falls back to `read`.
    fn read_into<'a>(
        &'a self,
        _node: NodeKey,
        _offset: u64,
        _count: u32,
        _out: &'a mut Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Option<NfsResult<bool>>> + Send + 'a>> {
        Box::pin(async { None })
    }
    fn fsstat(&self) -> FsStat;

    /// Whether this surface accepts mutations (false ⇒ the server replies ROFS to writes).
    fn writable(&self) -> bool {
        false
    }

    // ---- mutations; default to ROFS so read-only surfaces need not implement them ----------
    #[allow(clippy::too_many_arguments)]
    fn create<'a>(
        &'a self,
        _dir: NodeKey,
        _name: &'a [u8],
        _kind: CreateKind,
        _mode: u16,
    ) -> MutFuture<'a, (NodeKey, Attr)> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
    fn remove<'a>(&'a self, _dir: NodeKey, _name: &'a [u8], _is_dir: bool) -> MutFuture<'a, ()> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
    fn rename<'a>(
        &'a self,
        _from_dir: NodeKey,
        _from_name: &'a [u8],
        _to_dir: NodeKey,
        _to_name: &'a [u8],
    ) -> MutFuture<'a, ()> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
    fn setattr(&self, _node: NodeKey, _set: SetAttr) -> MutFuture<'_, Attr> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
    fn write<'a>(
        &'a self,
        _node: NodeKey,
        _offset: u64,
        _data: &'a [u8],
        _sync: bool,
    ) -> MutFuture<'a, Attr> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
    /// True when the share's durability policy makes every write as durable as it will ever
    /// get at reply time (writeback shares: data rides guest page-cache writeback, the exact
    /// semantics of a kernel-nfsd `async` export). Servers then answer UNSTABLE writes with
    /// `committed = FILE_SYNC`, and the client never throttles on COMMIT.
    fn writes_are_stable(&self) -> bool {
        false
    }

    /// A decorator created/removed a synthesized entry under `dir` (e.g. AppleDouble rows in
    /// the side-store): the directory's change attribute must still move deterministically or
    /// the client sees a successful create with an unchanged directory and falls back to
    /// invalidation. Default: no-op.
    fn touch_dir(&self, _dir: NodeKey) {}

    /// NFS COMMIT for `[off, off+len)`; `len == 0` = whole file (full durability — the
    /// close-time form). Bounded ranges may use cheaper range-writeback (knfsd parity).
    fn commit(&self, _node: NodeKey, _off: u64, _len: u64) -> MutFuture<'_, Attr> {
        Box::pin(async { Err(NfsError::Rofs) })
    }
}
