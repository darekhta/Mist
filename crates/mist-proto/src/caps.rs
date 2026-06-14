//! Hard caps enforced at decode time (semantic) and framing time (size).

/// Max payload bytes on ctl/journal/rpc lanes.
pub const MAX_FRAME: usize = 2 * 1024 * 1024; // fits a full 1 MiB NFS wsize write + envelope in ONE chunk
/// Max payload bytes on bulk lanes.
pub const MAX_FRAME_BULK: usize = 4 * 1024 * 1024;

/// A `Name` is one path component.
pub const MAX_NAME: usize = 255;
/// Symlink target bytes.
pub const MAX_SYMLINK: usize = 4096;
/// Short strings (host_name, versions, share names, error messages).
pub const MAX_STR: usize = 128;

pub const MAX_SHARES: usize = 64;
pub const MAX_RECORDS_PER_BATCH: usize = 512;
pub const MAX_SNAP_ENTRIES: usize = 2048;
pub const MAX_STATBATCH: usize = 4096;
pub const MAX_READ_LEN: u32 = 4 * 1024 * 1024;
pub const MAX_GROUPS: usize = 32;
