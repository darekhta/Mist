//! MOUNT v3 (RFC 1813 appendix) — just enough for `mount_nfs` to obtain the root handle.
//! Program 100005, version 3. Single export: the share root, mountable at any path.

use crate::handle::HandleCodec;
use crate::surface::MountSurface;
use crate::xdr::{XdrReader, XdrWriter};
use std::sync::Arc;

pub const MOUNT_PROGRAM: u32 = 100005;
pub const MOUNT_VERSION: u32 = 3;

const PROC_NULL: u32 = 0;
const PROC_MNT: u32 = 1;
const PROC_UMNT: u32 = 3;
const PROC_EXPORT: u32 = 5;

const MNT3_OK: u32 = 0;

const AUTH_NONE: u32 = 0;
const AUTH_SYS: u32 = 1;

pub fn dispatch<S: MountSurface>(
    proc: u32,
    args: &mut XdrReader<'_>,
    surface: &Arc<S>,
    codec: &HandleCodec,
    mut w: XdrWriter,
) -> XdrWriter {
    match proc {
        PROC_NULL => w,
        PROC_MNT => {
            let path = args.opaque(1024).unwrap_or(b"/");
            let root_fh = codec.encode(surface.share_id(), surface.root());
            tracing::debug!(path = %String::from_utf8_lossy(path), "MOUNT MNT");
            w.u32(MNT3_OK);
            w.opaque(&root_fh); // fhandle3
            // auth_flavors<>: we accept AUTH_SYS and AUTH_NONE.
            w.u32(2);
            w.u32(AUTH_SYS);
            w.u32(AUTH_NONE);
            w
        }
        PROC_UMNT => {
            // Stateless: nothing to forget.
            let _ = args.opaque(1024);
            w
        }
        PROC_EXPORT => {
            // One export entry: "/" with no group restrictions.
            w.bool(true); // entry present
            w.opaque(b"/"); // dirpath
            w.bool(false); // groups list empty
            w.bool(false); // no next entry
            w
        }
        _ => {
            // Unknown MOUNT proc: empty accepted reply (the RPC layer set GARBAGE/PROC status).
            w
        }
    }
}
