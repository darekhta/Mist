//! NFSv4 attribute bitmaps (RFC 5661 §5): encode fattr4 for the attrs we support, decode the
//! settable subset for SETATTR/OPEN-create. Hand-written like the rest of the XDR layer.

use crate::surface::{FsStat, SetAttr};
use crate::xdr::{XdrError, XdrReader, XdrWriter};
use mist_proto::{Attr, Kind, Ts};

// Attribute numbers (RFC 5661 §5.6/5.7).
pub const A_SUPPORTED_ATTRS: u32 = 0;
pub const A_TYPE: u32 = 1;
pub const A_FH_EXPIRE_TYPE: u32 = 2;
pub const A_CHANGE: u32 = 3;
pub const A_SIZE: u32 = 4;
pub const A_LINK_SUPPORT: u32 = 5;
pub const A_SYMLINK_SUPPORT: u32 = 6;
pub const A_NAMED_ATTR: u32 = 7;
pub const A_FSID: u32 = 8;
pub const A_UNIQUE_HANDLES: u32 = 9;
pub const A_LEASE_TIME: u32 = 10;
pub const A_RDATTR_ERROR: u32 = 11;
pub const A_ACLSUPPORT: u32 = 13;
pub const A_CANSETTIME: u32 = 15;
pub const A_CASE_INSENSITIVE: u32 = 16;
pub const A_CASE_PRESERVING: u32 = 17;
pub const A_CHOWN_RESTRICTED: u32 = 18;
pub const A_FILEHANDLE: u32 = 19;
pub const A_FILEID: u32 = 20;
pub const A_FILES_AVAIL: u32 = 21;
pub const A_FILES_FREE: u32 = 22;
pub const A_FILES_TOTAL: u32 = 23;
pub const A_MAXFILESIZE: u32 = 27;
pub const A_MAXLINK: u32 = 28;
pub const A_MAXNAME: u32 = 29;
pub const A_MAXREAD: u32 = 30;
pub const A_MAXWRITE: u32 = 31;
pub const A_MODE: u32 = 33;
pub const A_NO_TRUNC: u32 = 34;
pub const A_NUMLINKS: u32 = 35;
pub const A_OWNER: u32 = 36;
pub const A_OWNER_GROUP: u32 = 37;
pub const A_RAWDEV: u32 = 41;
pub const A_SPACE_AVAIL: u32 = 42;
pub const A_SPACE_FREE: u32 = 43;
pub const A_SPACE_TOTAL: u32 = 44;
pub const A_SPACE_USED: u32 = 45;
pub const A_TIME_ACCESS: u32 = 47;
pub const A_TIME_ACCESS_SET: u32 = 48;
pub const A_TIME_DELTA: u32 = 51;
pub const A_TIME_METADATA: u32 = 52;
pub const A_TIME_MODIFY: u32 = 53;
pub const A_TIME_MODIFY_SET: u32 = 54;
pub const A_MOUNTED_ON_FILEID: u32 = 55;
pub const A_SUPPATTR_EXCLCREAT: u32 = 75;

/// Attrs this server can return (queried via GETATTR / READDIR).
pub const SUPPORTED: &[u32] = &[
    A_SUPPORTED_ATTRS,
    A_TYPE,
    A_FH_EXPIRE_TYPE,
    A_CHANGE,
    A_SIZE,
    A_LINK_SUPPORT,
    A_SYMLINK_SUPPORT,
    A_NAMED_ATTR,
    A_FSID,
    A_UNIQUE_HANDLES,
    A_LEASE_TIME,
    A_RDATTR_ERROR,
    A_ACLSUPPORT,
    A_CANSETTIME,
    A_CASE_INSENSITIVE,
    A_CASE_PRESERVING,
    A_CHOWN_RESTRICTED,
    A_FILEHANDLE,
    A_FILEID,
    A_FILES_AVAIL,
    A_FILES_FREE,
    A_FILES_TOTAL,
    A_MAXFILESIZE,
    A_MAXLINK,
    A_MAXNAME,
    A_MAXREAD,
    A_MAXWRITE,
    A_MODE,
    A_NO_TRUNC,
    A_NUMLINKS,
    A_OWNER,
    A_OWNER_GROUP,
    A_RAWDEV,
    A_SPACE_AVAIL,
    A_SPACE_FREE,
    A_SPACE_TOTAL,
    A_SPACE_USED,
    A_TIME_ACCESS,
    A_TIME_DELTA,
    A_TIME_METADATA,
    A_TIME_MODIFY,
    A_MOUNTED_ON_FILEID,
    A_SUPPATTR_EXCLCREAT,
];

/// Attrs the client may set (SETATTR / OPEN create attrs).
pub const SETTABLE: &[u32] = &[
    A_SIZE,
    A_MODE,
    A_OWNER,
    A_OWNER_GROUP,
    A_TIME_ACCESS_SET,
    A_TIME_MODIFY_SET,
];

const MAX_READ: u32 = 1024 * 1024;
const MAX_WRITE: u32 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bitmap {
    pub words: [u32; 3],
}

impl Bitmap {
    pub fn from_attrs(attrs: &[u32]) -> Self {
        let mut b = Bitmap::default();
        for &a in attrs {
            b.set(a);
        }
        b
    }

    pub fn set(&mut self, attr: u32) {
        let w = (attr / 32) as usize;
        if w < self.words.len() {
            self.words[w] |= 1 << (attr % 32);
        }
    }

    pub fn has(&self, attr: u32) -> bool {
        let w = (attr / 32) as usize;
        w < self.words.len() && self.words[w] & (1 << (attr % 32)) != 0
    }

    pub fn intersect(&self, other: &Bitmap) -> Bitmap {
        let mut out = Bitmap::default();
        for i in 0..3 {
            out.words[i] = self.words[i] & other.words[i];
        }
        out
    }

    pub fn encode(&self, w: &mut XdrWriter) {
        // Trim trailing zero words (clients are picky about canonical bitmaps).
        let n = if self.words[2] != 0 {
            3
        } else if self.words[1] != 0 {
            2
        } else {
            1
        };
        w.u32(n as u32);
        for i in 0..n {
            w.u32(self.words[i]);
        }
    }

    pub fn decode(r: &mut XdrReader<'_>) -> Result<Bitmap, XdrError> {
        let n = r.u32()? as usize;
        if n > 16 {
            return Err(XdrError);
        }
        let mut b = Bitmap::default();
        for i in 0..n {
            let v = r.u32()?;
            if i < 3 {
                b.words[i] = v;
            }
        }
        Ok(b)
    }

    /// Iterate set attribute numbers in ascending order (fattr4 values are in this order).
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        (0u32..96).filter(|&a| self.has(a))
    }
}

/// The v4 `change` attribute: must differ whenever content or metadata changes. Mixes
/// mtime+ctime+size (the same stat-visible signal the rest of Mist keys caching on).
pub fn change_of(a: &Attr) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for v in [
        a.mtime.sec as u64,
        a.mtime.nsec as u64,
        a.ctime.sec as u64,
        a.ctime.nsec as u64,
        a.size,
    ] {
        h ^= v;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn type_of(k: Kind) -> u32 {
    match k {
        Kind::Reg => 1,
        Kind::Dir => 2,
        Kind::Blk => 3,
        Kind::Chr => 4,
        Kind::Symlink => 5,
        Kind::Sock => 6,
        Kind::Fifo => 7,
    }
}

/// Encode fattr4: bitmap of attrs actually returned + length-prefixed attr values.
/// `requested ∩ SUPPORTED`, in ascending attr order.
#[allow(clippy::too_many_arguments)]
pub fn encode_fattr4(
    w: &mut XdrWriter,
    requested: &Bitmap,
    attr: &Attr,
    node_fileid: u64,
    share: u16,
    fh: Option<&[u8]>,
    fsstat: &FsStat,
) {
    let supported = Bitmap::from_attrs(SUPPORTED);
    let mut giving = requested.intersect(&supported);
    if giving.has(A_FILEHANDLE) && fh.is_none() {
        let mut without = giving;
        without.words[0] &= !(1 << A_FILEHANDLE);
        giving = without;
    }
    giving.encode(w);
    let mut vals = XdrWriter::new();
    for a in giving.iter() {
        encode_one(&mut vals, a, attr, node_fileid, share, fh, fsstat);
    }
    w.opaque(&vals.into_bytes());
}

fn encode_one(
    w: &mut XdrWriter,
    a: u32,
    attr: &Attr,
    fileid: u64,
    share: u16,
    fh: Option<&[u8]>,
    fs: &FsStat,
) {
    match a {
        A_SUPPORTED_ATTRS => Bitmap::from_attrs(SUPPORTED).encode(w),
        A_TYPE => w.u32(type_of(attr.kind)),
        A_FH_EXPIRE_TYPE => w.u32(0), // FH4_PERSISTENT
        A_CHANGE => w.u64(change_of(attr)),
        A_SIZE => w.u64(attr.size),
        A_LINK_SUPPORT => w.bool(true),
        A_SYMLINK_SUPPORT => w.bool(true),
        A_NAMED_ATTR => w.bool(false),
        A_FSID => {
            w.u64(0x4D53_5400 | share as u64); // major: "MST" + share
            w.u64(0);
        }
        A_UNIQUE_HANDLES => w.bool(false),
        A_LEASE_TIME => w.u32(super::state::LEASE_TIME),
        A_RDATTR_ERROR => w.u32(0),
        A_ACLSUPPORT => w.u32(0),
        A_CANSETTIME => w.bool(true),
        A_CASE_INSENSITIVE => w.bool(false),
        A_CASE_PRESERVING => w.bool(true),
        A_CHOWN_RESTRICTED => w.bool(true),
        A_FILEHANDLE => w.opaque(fh.unwrap_or(&[])),
        A_FILEID => w.u64(fileid),
        A_FILES_AVAIL => w.u64(fs.free_files),
        A_FILES_FREE => w.u64(fs.free_files),
        A_FILES_TOTAL => w.u64(fs.total_files),
        A_MAXFILESIZE => w.u64(u64::MAX >> 1),
        A_MAXLINK => w.u32(32000),
        A_MAXNAME => w.u32(255),
        A_MAXREAD => w.u64(MAX_READ as u64),
        A_MAXWRITE => w.u64(MAX_WRITE as u64),
        A_MODE => w.u32(attr.mode as u32 & 0o7777),
        A_NO_TRUNC => w.bool(true),
        A_NUMLINKS => w.u32(attr.nlink),
        A_OWNER => w.opaque(attr.uid.to_string().as_bytes()),
        A_OWNER_GROUP => w.opaque(attr.gid.to_string().as_bytes()),
        A_RAWDEV => {
            w.u32((attr.rdev >> 32) as u32);
            w.u32(attr.rdev as u32);
        }
        A_SPACE_AVAIL => w.u64(fs.avail_bytes),
        A_SPACE_FREE => w.u64(fs.free_bytes),
        A_SPACE_TOTAL => w.u64(fs.total_bytes),
        A_SPACE_USED => w.u64(attr.blocks * 512),
        A_TIME_ACCESS => time4(w, attr.mtime), // we don't track atime; mtime is the honest stand-in
        A_TIME_DELTA => {
            w.u64(0);
            w.u32(1); // 1 ns resolution
        }
        A_TIME_METADATA => time4(w, attr.ctime),
        A_TIME_MODIFY => time4(w, attr.mtime),
        A_MOUNTED_ON_FILEID => w.u64(fileid),
        A_SUPPATTR_EXCLCREAT => Bitmap::from_attrs(SETTABLE).encode(w),
        _ => {}
    }
}

fn time4(w: &mut XdrWriter, t: Ts) {
    w.u64(t.sec as u64);
    w.u32(t.nsec);
}

/// Decode the settable attrs of a fattr4 (SETATTR / OPEN createattrs) into a SetAttr.
/// Returns (SetAttr, attrs-set bitmap) or Err on malformed input / unsettable attr present.
pub fn decode_settable(r: &mut XdrReader<'_>) -> Result<(SetAttr, Bitmap), XdrError> {
    let bm = Bitmap::decode(r)?;
    let vals = r.opaque(64 * 1024)?;
    let mut vr = XdrReader::new(vals);
    let mut out = SetAttr::default();
    for a in bm.iter() {
        match a {
            A_SIZE => out.size = Some(vr.u64()?),
            A_MODE => out.mode = Some((vr.u32()? & 0o7777) as u16),
            A_OWNER => {
                let s = vr.opaque(256)?;
                out.uid = std::str::from_utf8(s).ok().and_then(|s| s.parse().ok());
            }
            A_OWNER_GROUP => {
                let s = vr.opaque(256)?;
                out.gid = std::str::from_utf8(s).ok().and_then(|s| s.parse().ok());
            }
            A_TIME_ACCESS_SET => {
                // settime4: 0 = server time, 1 = client time (time4 follows). atime untracked.
                if vr.u32()? == 1 {
                    let _ = vr.u64()?;
                    let _ = vr.u32()?;
                }
            }
            A_TIME_MODIFY_SET => {
                if vr.u32()? == 1 {
                    let sec = vr.u64()? as i64;
                    let nsec = vr.u32()?;
                    out.mtime = Some(Ts { sec, nsec });
                } else {
                    out.mtime = Some(now_ts());
                }
            }
            // Anything else in the bitmap is unsettable here → protocol error upstream.
            _ => return Err(XdrError),
        }
    }
    Ok((out, bm))
}

fn now_ts() -> Ts {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Ts {
        sec: d.as_secs() as i64,
        nsec: d.subsec_nanos(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_roundtrip() {
        let b = Bitmap::from_attrs(&[
            A_TYPE,
            A_CHANGE,
            A_MODE,
            A_MOUNTED_ON_FILEID,
            A_SUPPATTR_EXCLCREAT,
        ]);
        let mut w = XdrWriter::new();
        b.encode(&mut w);
        let bytes = w.into_bytes();
        let mut r = XdrReader::new(&bytes);
        let d = Bitmap::decode(&mut r).unwrap();
        assert_eq!(b, d);
        assert!(d.has(A_SUPPATTR_EXCLCREAT));
        assert!(!d.has(A_SIZE));
        let set: Vec<u32> = d.iter().collect();
        assert_eq!(
            set,
            vec![
                A_TYPE,
                A_CHANGE,
                A_MODE,
                A_MOUNTED_ON_FILEID,
                A_SUPPATTR_EXCLCREAT
            ]
        );
    }

    #[test]
    fn change_attr_rotates_on_content_change() {
        let mut a = Attr {
            kind: Kind::Reg,
            mode: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 10,
            blocks: 1,
            mtime: Ts { sec: 5, nsec: 1 },
            ctime: Ts { sec: 5, nsec: 1 },
            rdev: 0,
            content_version: 0,
            symlink_target: None,
        };
        let c1 = change_of(&a);
        a.mtime.nsec = 2;
        assert_ne!(c1, change_of(&a));
    }

    #[test]
    fn settable_decode() {
        let mut w = XdrWriter::new();
        let bm = Bitmap::from_attrs(&[A_SIZE, A_MODE]);
        bm.encode(&mut w);
        let mut vals = XdrWriter::new();
        vals.u64(4096); // size
        vals.u32(0o600); // mode
        w.opaque(&vals.into_bytes());
        let bytes = w.into_bytes();
        let mut r = XdrReader::new(&bytes);
        let (sa, got) = decode_settable(&mut r).unwrap();
        assert_eq!(sa.size, Some(4096));
        assert_eq!(sa.mode, Some(0o600));
        assert!(got.has(A_SIZE));
    }
}
