//! Mist NFS frontend: loopback NFS surfaces backed by a [`MountSurface`].
//!
//! The macOS in-kernel NFS client mounts this over loopback TCP; every metadata op is answered
//! from the surface (the host replica), while reads and mutations cross through the surface's
//! async data path. Design 05.
//!
//! Implemented surfaces:
//! - NFSv3 + MOUNT3 with read/write procedures used by macOS user mounts.
//! - NFSv4.1 sessions with read delegations and recall hooks for opt-in coherence testing.

pub mod bufpool;
mod handle;
mod mount3;
mod nfs3;
pub mod nfs41;
mod rpc;
pub(crate) mod server;
mod surface;
mod xdr;

pub use handle::{HANDLE_LEN, HandleCodec};
pub use nfs41::Nfs41Server;
pub use server::NfsServer;
pub use surface::{
    CreateKind, DirEntry, FsStat, MountSurface, MutFuture, NfsError, NfsResult, ReadDirPage,
    ReadFuture, ReadResult, SendableFuture, SendableRead, SetAttr,
};

/// Fuzzing hooks (compiled only with the `fuzzing` feature): expose the internal parse layer
/// to `fuzz/` targets without making it part of the real API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    /// Run the ONC-RPC call parser + per-program argument scanners over hostile bytes.
    pub fn parse_call(data: &[u8]) {
        let _ = crate::rpc::parse_call(data);
    }
}
