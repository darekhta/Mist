//! Lane messages. Variant order is wire ABI — append only.

use crate::caps;
use crate::types::*;
use crate::validate::{Validate, ValidateError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lane {
    Ctl,
    Journal,
    Rpc,
    Bulk,
}

/// Control-lane messages (also the first frame of every non-ctl lane: `StreamHello`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CtlMsg {
    Hello {
        proto: u32,
        features: u64,
        /// blake3 of the shared token file contents.
        token_hash: [u8; 32],
        host_name: String,
        host_version: String,
    },
    HelloAck {
        proto: u32,
        features: u64,
        boot_id: u64,
        session_id: u64,
        shares: Vec<ShareInfo>,
        guest: GuestInfo,
    },
    AuthFail,
    StreamHello {
        session_id: u64,
        lane: Lane,
        idx: u8,
    },
    AttachShare {
        share: ShareId,
    },
    DetachShare {
        share: ShareId,
    },
    SnapshotStart {
        share: ShareId,
        snap_id: u64,
    },
    SnapshotAbort {
        snap_id: u64,
    },
    RescanDir {
        share: ShareId,
        dir: NodeKey,
        snap_id: u64,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
        guest_mono_ns: u64,
    },
    ShareGone {
        share: ShareId,
    },
    Goodbye {
        reason: String,
    },
    /// Guest → host, right after `HelloAck`: extra dialable endpoints for secondary lanes.
    /// `tcp` entries are `addr:port` strings reachable from the host (virtio-net). The host may
    /// dial rpc/bulk/journal lanes there for throughput (vsock stays the ctl path + fallback).
    /// Appended as an ABI-compatible enum extension.
    Endpoints {
        tcp: Vec<String>,
    },
    /// Guest → host, sent after `HelloAck` (before `Endpoints`) **only when the `VM_IDENTITY`
    /// feature was negotiated** (design 11 §6). Carries the guest's stable, reboot-persistent
    /// identity so the resolver can bind `bridge="auto"` to the right guest and the authenticated
    /// probe can reject a cloned/reused token. An identifier, not a secret.
    ///
    /// Appended as an ABI-compatible enum extension: an old guest never emits it, and an old host
    /// (which would not set the feature bit) never causes a new guest to emit it.
    VmIdentity {
        vm_uuid: [u8; 16],
    },
}

impl Validate for CtlMsg {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            CtlMsg::Hello {
                host_name,
                host_version,
                ..
            } => {
                if host_name.len() > caps::MAX_STR || host_version.len() > caps::MAX_STR {
                    return Err(ValidateError::new("hello string length"));
                }
                Ok(())
            }
            CtlMsg::HelloAck { shares, guest, .. } => {
                if shares.len() > caps::MAX_SHARES {
                    return Err(ValidateError::new("too many shares"));
                }
                shares.validate()?;
                guest.validate()
            }
            CtlMsg::Goodbye { reason } => {
                if reason.len() > caps::MAX_STR {
                    return Err(ValidateError::new("goodbye reason length"));
                }
                Ok(())
            }
            CtlMsg::Endpoints { tcp } => {
                if tcp.len() > 8 || tcp.iter().any(|a| a.len() > caps::MAX_STR) {
                    return Err(ValidateError::new("endpoints list"));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Host → guest requests (rpc lane). Mutation requests use append-only ABI entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcReq {
    Stat {
        share: ShareId,
        node: NodeKey,
    },
    StatBatch {
        share: ShareId,
        nodes: Vec<NodeKey>,
    },
    Lookup {
        share: ShareId,
        dir: NodeKey,
        name: Name,
    },
    /// Reply: `RpcResp::ReadStart` on the rpc lane, then raw `Bulk` frames carrying the data,
    /// chained with `FLAG_MORE`, all under this request's `seq`.
    Read {
        share: ShareId,
        node: NodeKey,
        version_hint: u64,
        off: u64,
        len: u32,
        ra: u32,
    },

    // ---- mutations — append-only ---------------------------------------------------------
    /// Create a regular file / dir / symlink / fifo / sock / device under `dir`.
    /// Reply: `RpcResp::Entry { node, attr }`.
    Create {
        share: ShareId,
        dir: NodeKey,
        name: Name,
        kind: Kind,
        mode: u16,
        rdev: u64,
        /// `Some` for symlinks: the link target. Validated ≤ 4096 bytes.
        symlink_target: Option<Vec<u8>>,
        /// If true, fail with EEXIST when the name already exists (NFS exclusive create).
        exclusive: bool,
    },
    /// Remove a non-directory entry. Reply: `RpcResp::Ok`.
    Unlink {
        share: ShareId,
        dir: NodeKey,
        name: Name,
    },
    /// Remove an empty directory. Reply: `RpcResp::Ok`.
    Rmdir {
        share: ShareId,
        dir: NodeKey,
        name: Name,
    },
    /// Atomically rename. Reply: `RpcResp::Ok`.
    Rename {
        share: ShareId,
        from_dir: NodeKey,
        from_name: Name,
        to_dir: NodeKey,
        to_name: Name,
    },
    /// Change metadata (chmod/chown/truncate/utimes). Any `Some` field is applied.
    /// Reply: `RpcResp::Attr` (post-op attrs).
    SetAttr {
        share: ShareId,
        node: NodeKey,
        mode: Option<u16>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        mtime: Option<Ts>,
    },
    /// Write `data` at `off`. `sync` ⇒ fdatasync before replying. Reply: `RpcResp::Attr`.
    /// Data is inline (≤ 1 MiB, the NFS wsize) rather than streamed.
    Write {
        share: ShareId,
        node: NodeKey,
        off: u64,
        sync: bool,
        data: Vec<u8>,
    },
    /// fdatasync the file (NFS COMMIT). Reply: `RpcResp::Attr`.
    Commit {
        share: ShareId,
        node: NodeKey,
    },
    /// Ranged NFS COMMIT. `len == 0` ⇒ whole file (full fdatasync).
    /// A bounded range maps to `sync_file_range(WAIT_BEFORE|WRITE|WAIT_AFTER)` — the
    /// userspace twin of knfsd's `vfs_fsync_range`; the close-time full commit restores
    /// complete durability (close-to-open is the consistency contract). Reply: `RpcResp::Attr`.
    CommitRange {
        share: ShareId,
        node: NodeKey,
        off: u64,
        len: u64,
    },
}

impl Validate for RpcReq {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            RpcReq::StatBatch { nodes, .. } => {
                if nodes.len() > caps::MAX_STATBATCH {
                    return Err(ValidateError::new("statbatch too large"));
                }
                Ok(())
            }
            RpcReq::Read { len, .. } => {
                if *len == 0 || *len > caps::MAX_READ_LEN {
                    return Err(ValidateError::new("read len out of range"));
                }
                Ok(())
            }
            RpcReq::Create {
                kind,
                symlink_target,
                ..
            } => {
                if let Some(t) = symlink_target {
                    if *kind != Kind::Symlink {
                        return Err(ValidateError::new("symlink_target on non-symlink create"));
                    }
                    if t.is_empty() || t.len() > caps::MAX_SYMLINK {
                        return Err(ValidateError::new("symlink target length"));
                    }
                } else if *kind == Kind::Symlink {
                    return Err(ValidateError::new("symlink create without target"));
                }
                Ok(())
            }
            RpcReq::Write { data, .. } => {
                if data.len() > caps::MAX_READ_LEN as usize {
                    return Err(ValidateError::new("write data too large"));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcErr {
    pub errno: i32,
    pub msg: String,
}

impl Validate for RpcErr {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.msg.len() > caps::MAX_STR {
            return Err(ValidateError::new("rpc error message length"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcResp {
    Attr(Attr),
    Attrs(Vec<Option<Attr>>),
    Entry {
        node: NodeKey,
        attr: Attr,
    },
    /// Data follows as Bulk frames (same seq), MORE-chained; final frame has MORE clear.
    /// `len` is the total byte count that will follow.
    ReadStart {
        version: u64,
        len: u64,
        eof: bool,
    },
    Err(RpcErr),
    /// Generic mutation success with no payload (unlink/rmdir/rename). Append-only.
    Ok,
}

impl Validate for RpcResp {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            RpcResp::Attr(a) => a.validate(),
            RpcResp::Attrs(v) => {
                if v.len() > caps::MAX_STATBATCH {
                    return Err(ValidateError::new("attrs batch too large"));
                }
                v.validate()
            }
            RpcResp::Entry { attr, .. } => attr.validate(),
            RpcResp::Err(e) => e.validate(),
            RpcResp::ReadStart { .. } => Ok(()),
            RpcResp::Ok => Ok(()),
        }
    }
}

/// One observed guest filesystem change. Apply semantics: design/02 §6.3 (idempotent upserts).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Rec {
    Created {
        parent: NodeKey,
        name: Name,
        node: NodeKey,
        attr: Option<Attr>,
    },
    CreatedBatch {
        parent: NodeKey,
        entries: Vec<SnapEntry>,
    },
    Removed {
        parent: NodeKey,
        name: Name,
    },
    Renamed {
        from_parent: NodeKey,
        from_name: Name,
        to_parent: NodeKey,
        to_name: Name,
    },
    AttrChanged {
        node: NodeKey,
        attr: Attr,
    },
    Content {
        node: NodeKey,
        version: u64,
        size: u64,
        mtime: Ts,
        in_progress: bool,
    },
    SelfRemoved {
        node: NodeKey,
    },
    Overflow,
    EchoMarker {
        tag: u64,
    },
}

impl Validate for Rec {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            Rec::Created { attr, .. } => attr.validate(),
            Rec::CreatedBatch { entries, .. } => {
                if entries.len() > caps::MAX_SNAP_ENTRIES {
                    return Err(ValidateError::new("created batch too large"));
                }
                entries.validate()
            }
            Rec::AttrChanged { attr, .. } => attr.validate(),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalBatch {
    pub share: ShareId,
    /// Per-share, contiguous within an epoch; a gap forces resync.
    pub first_seq: u64,
    pub guest_mono_ns: u64,
    pub records: Vec<Rec>,
}

impl Validate for JournalBatch {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.records.is_empty() || self.records.len() > caps::MAX_RECORDS_PER_BATCH {
            return Err(ValidateError::new("journal batch size"));
        }
        self.records.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapEntry {
    pub name: Name,
    pub node: NodeKey,
    pub attr: Attr,
}

impl Validate for SnapEntry {
    fn validate(&self) -> Result<(), ValidateError> {
        self.attr.validate()
    }
}

/// One directory's snapshot contents. Large directories span multiple records;
/// the final one carries `last = true`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapDir {
    pub snap_id: u64,
    pub share: ShareId,
    pub dir: NodeKey,
    pub dir_attr: Attr,
    /// Root's parent is itself.
    pub parent: NodeKey,
    pub entries: Vec<SnapEntry>,
    pub last: bool,
}

impl Validate for SnapDir {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.entries.len() > caps::MAX_SNAP_ENTRIES {
            return Err(ValidateError::new("snapdir too large"));
        }
        self.dir_attr.validate()?;
        if self.dir_attr.kind != Kind::Dir {
            return Err(ValidateError::new("snapdir attr not a dir"));
        }
        self.entries.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapDone {
    pub snap_id: u64,
    pub share: ShareId,
    pub dirs: u64,
    pub entries: u64,
    pub errors: u32,
}

/// Event-lane messages (guest → host).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventMsg {
    Journal(JournalBatch),
    SnapDir(SnapDir),
    SnapDone(SnapDone),
}

impl Validate for EventMsg {
    fn validate(&self) -> Result<(), ValidateError> {
        match self {
            EventMsg::Journal(b) => b.validate(),
            EventMsg::SnapDir(d) => d.validate(),
            EventMsg::SnapDone(_) => Ok(()),
        }
    }
}
