//! Domain types shared by every message.

use crate::caps;
use crate::validate::{Validate, ValidateError};
use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShareId(pub u16);

/// Canonical file identity: guest `(ino, i_generation)`.
///
/// Derived statelessly from ext4 NFS handles (`FILEID_INO32_GEN`); ABA-safe because ext4 bumps
/// `i_generation` on inode reuse. `ino` is `u64` on the wire to leave room for ino64 filesystems
/// (feature-gated).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeKey {
    pub ino: u64,
    pub generation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ts {
    pub sec: i64,
    pub nsec: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    Reg,
    Dir,
    Symlink,
    Fifo,
    Sock,
    Chr,
    Blk,
}

/// File attributes as the guest reports them.
///
/// `atime` is deliberately absent (ADR-16). `content_version` is mistd's data-cache epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attr {
    pub kind: Kind,
    pub mode: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blocks: u64,
    pub mtime: Ts,
    pub ctime: Ts,
    pub rdev: u64,
    pub content_version: u64,
    /// `Some` iff `kind == Symlink`; immutable after create.
    pub symlink_target: Option<Vec<u8>>,
}

impl Validate for Attr {
    fn validate(&self) -> Result<(), ValidateError> {
        if let Some(t) = &self.symlink_target {
            if self.kind != Kind::Symlink {
                return Err(ValidateError::new("symlink_target on non-symlink"));
            }
            if t.is_empty() || t.len() > caps::MAX_SYMLINK {
                return Err(ValidateError::new("symlink target length"));
            }
        }
        Ok(())
    }
}

/// A single path component: 1..=255 bytes, no `/`, no NUL, not `.` or `..`.
///
/// Grammar is enforced on construction *and* on deserialize, so a hostile peer cannot smuggle
/// path traversal through the type system.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Name(Vec<u8>);

impl Name {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, ValidateError> {
        let b = bytes.into();
        Self::check(&b)?;
        Ok(Name(b))
    }

    pub fn check(b: &[u8]) -> Result<(), ValidateError> {
        if b.is_empty() || b.len() > caps::MAX_NAME {
            return Err(ValidateError::new("name length"));
        }
        if b.contains(&b'/') || b.contains(&0) {
            return Err(ValidateError::new("name contains '/' or NUL"));
        }
        if b == b"." || b == b".." {
            return Err(ValidateError::new("name is '.' or '..'"));
        }
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Name({})", String::from_utf8_lossy(&self.0))
    }
}

impl Serialize for Name {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Name;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a valid path component")
            }
            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Name, E> {
                Name::new(v).map_err(|e| E::custom(e))
            }
            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Name, E> {
                Name::new(v).map_err(|e| E::custom(e))
            }
            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Name, A::Error> {
                let mut b = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(caps::MAX_NAME));
                while let Some(x) = seq.next_element::<u8>()? {
                    if b.len() >= caps::MAX_NAME {
                        return Err(de::Error::custom("name too long"));
                    }
                    b.push(x);
                }
                Name::new(b).map_err(de::Error::custom)
            }
        }
        d.deserialize_bytes(V)
    }
}

/// Share flag bits (`ShareInfo::flags`).
pub mod share_flags {
    pub const SUBTREE: u32 = 1 << 0;
    pub const RDONLY: u32 = 1 << 1;
    /// Filesystem handles were not `FILEID_INO32_GEN`; NodeKey.gen is 0 (dev/test environments).
    pub const DEGRADED_IDS: u32 = 1 << 2;
    /// CommitPolicy::Writeback — writes are as durable at reply time as they will ever get
    /// (knfsd `async`-export semantics); the host replies FILE_SYNC to UNSTABLE writes.
    pub const WRITEBACK: u32 = 1 << 3;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareInfo {
    pub id: ShareId,
    pub name: String,
    /// Changes whenever mistd restarts or the share's filesystem is remounted ⇒ host reseeds.
    pub epoch: u64,
    /// Guest filesystem identity; pinned for handle-confinement checks.
    pub fsid: u64,
    pub root: NodeKey,
    pub flags: u32,
    pub ino_bits: u8,
}

impl Validate for ShareInfo {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.name.is_empty() || self.name.len() > caps::MAX_STR {
            return Err(ValidateError::new("share name length"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuestInfo {
    pub kernel: String,
    pub fanotify_max_queued: u32,
    pub mistd_pid: u32,
}

impl Validate for GuestInfo {
    fn validate(&self) -> Result<(), ValidateError> {
        if self.kernel.len() > caps::MAX_STR {
            return Err(ValidateError::new("kernel string length"));
        }
        Ok(())
    }
}
