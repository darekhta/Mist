//! ONC-RPC (RFC 5531) message layer: TCP record marking + call/reply headers. We accept
//! AUTH_SYS and AUTH_NONE (the requester's uid is noted but, under squash identity, ignored).

use crate::xdr::{XdrError, XdrReader, XdrWriter};

pub const RPC_VERSION: u32 = 2;

// Reply / accept status codes.
const MSG_TYPE_CALL: u32 = 0;
const MSG_TYPE_REPLY: u32 = 1;
const REPLY_ACCEPTED: u32 = 0;

pub const ACCEPT_SUCCESS: u32 = 0;
pub const ACCEPT_PROG_UNAVAIL: u32 = 1;
pub const ACCEPT_PROG_MISMATCH: u32 = 2;
#[allow(dead_code)] // reserved for stricter dispatch
pub const ACCEPT_PROC_UNAVAIL: u32 = 3;
#[allow(dead_code)]
pub const ACCEPT_GARBAGE_ARGS: u32 = 4;

const AUTH_NONE: u32 = 0;
const AUTH_SYS: u32 = 1;

/// A decoded RPC call header + the requester's Unix credentials (if AUTH_SYS).
#[derive(Debug, Clone)]
pub struct RpcCall {
    pub xid: u32,
    pub program: u32,
    pub version: u32,
    pub procedure: u32,
    #[allow(dead_code)] // used by passthrough identity
    pub uid: u32,
    #[allow(dead_code)]
    pub gid: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcDecodeError {
    #[error("xdr: {0}")]
    Xdr(#[from] XdrError),
    #[error("not a call message")]
    NotCall,
    #[error("unsupported rpc version {0}")]
    BadVersion(u32),
}

/// Parse an RPC call message. Returns the header and a reader positioned at the procedure args.
pub fn parse_call(buf: &[u8]) -> Result<(RpcCall, XdrReader<'_>), RpcDecodeError> {
    let mut r = XdrReader::new(buf);
    let xid = r.u32()?;
    let msg_type = r.u32()?;
    if msg_type != MSG_TYPE_CALL {
        return Err(RpcDecodeError::NotCall);
    }
    let rpcvers = r.u32()?;
    if rpcvers != RPC_VERSION {
        return Err(RpcDecodeError::BadVersion(rpcvers));
    }
    let program = r.u32()?;
    let version = r.u32()?;
    let procedure = r.u32()?;

    // Credentials: flavor + body.
    let (uid, gid) = read_auth(&mut r)?;
    // Verifier: flavor + body — ignored.
    let _vflavor = r.u32()?;
    r.skip_opaque(1024)?;

    Ok((
        RpcCall {
            xid,
            program,
            version,
            procedure,
            uid,
            gid,
        },
        r,
    ))
}

fn read_auth(r: &mut XdrReader<'_>) -> Result<(u32, u32), XdrError> {
    let flavor = r.u32()?;
    let body = r.opaque(1024)?;
    if flavor == AUTH_SYS {
        // authsys_parms: stamp(u32) machinename(string) uid(u32) gid(u32) gids<>
        let mut b = XdrReader::new(body);
        let _stamp = b.u32()?;
        let _machine = b.opaque(255)?;
        let uid = b.u32()?;
        let gid = b.u32()?;
        Ok((uid, gid))
    } else {
        let _ = (AUTH_NONE,);
        Ok((0, 0))
    }
}

/// Build an accepted reply header (verifier = AUTH_NONE) into a fresh writer; the caller appends
/// the procedure results.
pub fn reply_accepted(xid: u32, accept_status: u32) -> XdrWriter {
    // READ replies carry up to rsize (1 MiB); a pooled, already-faulted buffer avoids both
    // realloc copies and ~200 µs of soft page faults per streaming reply.
    let mut w = XdrWriter::from_vec(crate::bufpool::take((1 << 20) + 4096));
    w.u32(xid);
    w.u32(MSG_TYPE_REPLY);
    w.u32(REPLY_ACCEPTED);
    // verifier: AUTH_NONE, empty body
    w.u32(AUTH_NONE);
    w.opaque(&[]);
    w.u32(accept_status);
    w
}

/// PROG_MISMATCH carries the supported version range.
pub fn reply_prog_mismatch(xid: u32, low: u32, high: u32) -> XdrWriter {
    let mut w = reply_accepted(xid, ACCEPT_PROG_MISMATCH);
    w.u32(low);
    w.u32(high);
    w
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal AUTH_SYS call and parse it back.
    #[test]
    fn parse_authsys_call() {
        let mut w = XdrWriter::new();
        w.u32(0x1234); // xid
        w.u32(MSG_TYPE_CALL);
        w.u32(RPC_VERSION);
        w.u32(100003); // NFS
        w.u32(3);
        w.u32(1); // GETATTR
        // cred: AUTH_SYS
        w.u32(AUTH_SYS);
        let mut cred = XdrWriter::new();
        cred.u32(0); // stamp
        cred.opaque(b"mac"); // machine
        cred.u32(501); // uid
        cred.u32(20); // gid
        cred.u32(0); // gids count
        w.opaque(&cred.into_bytes());
        // verifier: AUTH_NONE
        w.u32(AUTH_NONE);
        w.opaque(&[]);
        // args would follow
        w.u32(0xCAFE);

        let bytes = w.into_bytes();
        let (call, mut args) = parse_call(&bytes).unwrap();
        assert_eq!(call.xid, 0x1234);
        assert_eq!(call.program, 100003);
        assert_eq!(call.procedure, 1);
        assert_eq!(call.uid, 501);
        assert_eq!(call.gid, 20);
        assert_eq!(args.u32().unwrap(), 0xCAFE);
    }

    #[test]
    fn rejects_non_call() {
        let mut w = XdrWriter::new();
        w.u32(1);
        w.u32(MSG_TYPE_REPLY);
        let bytes = w.into_bytes();
        assert!(matches!(parse_call(&bytes), Err(RpcDecodeError::NotCall)));
    }
}
