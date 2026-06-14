//! fanotify journal engine: one filesystem-wide mark per share's filesystem, decoded into the
//! ordered change journal (design 03 §4).
//!
//! Group init: `FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME | FAN_REPORT_FID` — directory-entry events
//! (create/delete/move) carry the parent's file handle + the entry name (`DFID_NAME`), and
//! object events (close/modify/attrib/delete-self) carry the object's own handle (`FID`). Both
//! decode to a `NodeKey` with zero syscalls on ext4.
//!
//! The reader runs on a dedicated OS thread (blocking `read(2)`); decoded records flow over a
//! bounded channel to the journal encoder. Channel-full for too long → `Overflow` + drop, the
//! designed safe response to overload (the host resync covers the gap).
#![allow(unsafe_code)]

use super::handles::{HandleKind, decode_handle};
use super::shares::Share;
use mist_proto::{Name, NodeKey, Rec, Ts};
use std::collections::HashMap;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::mpsc::SyncSender;

// ---- constants not reliably in libc -------------------------------------------------------

const FAN_EVENT_INFO_TYPE_FID: u8 = 1;
const FAN_EVENT_INFO_TYPE_DFID_NAME: u8 = 2;
const FAN_EVENT_INFO_TYPE_DFID: u8 = 3;
const FAN_EVENT_INFO_TYPE_OLD_DFID_NAME: u8 = 10;
const FAN_EVENT_INFO_TYPE_NEW_DFID_NAME: u8 = 12;

const FAN_REPORT_FID: libc::c_uint = 0x0000_0200;
const FAN_REPORT_DIR_FID: libc::c_uint = 0x0000_0400;
const FAN_REPORT_NAME: libc::c_uint = 0x0000_0800;
const FAN_REPORT_DFID_NAME: libc::c_uint = FAN_REPORT_DIR_FID | FAN_REPORT_NAME;

const FAN_CREATE: u64 = 0x0000_0100;
const FAN_DELETE: u64 = 0x0000_0200;
const FAN_DELETE_SELF: u64 = 0x0000_0400;
const FAN_MOVED_FROM: u64 = 0x0000_0040;
const FAN_MOVED_TO: u64 = 0x0000_0080;
const FAN_RENAME: u64 = 0x1000_0000;
const FAN_MODIFY: u64 = 0x0000_0002;
const FAN_CLOSE_WRITE: u64 = 0x0000_0008;
const FAN_ATTRIB: u64 = 0x0000_0004;
const FAN_ONDIR: u64 = 0x4000_0000;
const FAN_Q_OVERFLOW: u64 = 0x0000_4000;

const FAN_MARK_ADD: libc::c_uint = 0x0000_0001;
const FAN_MARK_FILESYSTEM: libc::c_uint = 0x0000_0100;

#[cfg(test)]
const FAN_NOFD: i32 = -1;

const METADATA_LEN: usize = 24; // sizeof(struct fanotify_event_metadata)
const INFO_HDR_LEN: usize = 4; // sizeof(struct fanotify_event_info_header)

/// Mask we register. `FAN_RENAME` requires kernel ≥5.17; on older kernels the kernel rejects it
/// at mark time and we fall back to MOVED_FROM/MOVED_TO pairing.
fn event_mask(rename: bool) -> u64 {
    let base = FAN_CREATE
        | FAN_DELETE
        | FAN_DELETE_SELF
        | FAN_MODIFY
        | FAN_CLOSE_WRITE
        | FAN_ATTRIB
        | FAN_ONDIR;
    if rename {
        base | FAN_RENAME
    } else {
        base | FAN_MOVED_FROM | FAN_MOVED_TO
    }
}

// ---- raw syscalls -------------------------------------------------------------------------

fn fanotify_init(flags: libc::c_uint, event_f_flags: libc::c_uint) -> std::io::Result<OwnedFd> {
    // SAFETY: fanotify_init takes two scalar args and returns a new fd or -1.
    let rc = unsafe { libc::fanotify_init(flags, event_f_flags) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: rc is a fresh owned fd.
    Ok(unsafe { OwnedFd::from_raw_fd(rc) })
}

fn fanotify_mark(
    group: BorrowedFd<'_>,
    flags: libc::c_uint,
    mask: u64,
    dirfd: RawFd,
    path: Option<&std::ffi::CStr>,
) -> std::io::Result<()> {
    let p = path.map(|c| c.as_ptr()).unwrap_or(std::ptr::null());
    // SAFETY: group is a valid fanotify fd; p is null or a valid C string.
    let rc = unsafe { libc::fanotify_mark(group.as_raw_fd(), flags, mask, dirfd, p) };
    if rc < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

// ---- event parsing (pure; unit-tested) ----------------------------------------------------

/// One decoded info record: a (handle → NodeKey) plus optional entry name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FidName {
    pub node: Option<NodeKey>, // None if the handle wasn't ext4-style
    pub name: Option<Vec<u8>>,
}

/// The parsed shape of one fanotify event: its mask plus the info records we care about.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedEvent {
    pub mask: u64,
    pub pid: i32,
    pub fid: Option<FidName>,       // object's own handle (FID)
    pub dfid_name: Option<FidName>, // parent dir handle + name (DFID_NAME)
    pub old: Option<FidName>,       // FAN_RENAME old (OLD_DFID_NAME)
    pub new: Option<FidName>,       // FAN_RENAME new (NEW_DFID_NAME)
}

fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_ne_bytes([b[o], b[o + 1]])
}
fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_ne_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn rd_i32(b: &[u8], o: usize) -> i32 {
    i32::from_ne_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn rd_u64(b: &[u8], o: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[o..o + 8]);
    u64::from_ne_bytes(a)
}

/// Parse one info record body (after the 4-byte header) of an FID/DFID_NAME type into a FidName.
/// Layout: `__kernel_fsid_t fsid` (8 bytes) then `struct file_handle { u32 handle_bytes; i32
/// handle_type; u8 f_handle[handle_bytes] }`, then (for *_NAME types) a NUL-terminated name.
fn parse_fid_body(body: &[u8], with_name: bool) -> FidName {
    // body starts at fsid.
    if body.len() < 8 + 8 {
        return FidName {
            node: None,
            name: None,
        };
    }
    let fh = &body[8..]; // skip fsid
    let handle_bytes = rd_u32(fh, 0) as usize;
    let handle_type = rd_i32(fh, 4);
    let fstart = 8;
    if fh.len() < fstart + handle_bytes {
        return FidName {
            node: None,
            name: None,
        };
    }
    let f_handle = &fh[fstart..fstart + handle_bytes];
    let node = match decode_handle(handle_type, f_handle) {
        HandleKind::Ino32Gen(k) => Some(k),
        HandleKind::Other => None,
    };
    let name = if with_name {
        let after = fstart + handle_bytes;
        let rest = &fh[after..];
        // Name is NUL-terminated; trailing padding may follow.
        let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        if end == 0 {
            None
        } else {
            Some(rest[..end].to_vec())
        }
    } else {
        None
    };
    FidName { node, name }
}

/// Parse a single event starting at offset 0 of `buf`; returns (parsed, event_len consumed).
/// Returns None if the buffer is too short to hold the metadata.
pub fn parse_event(buf: &[u8]) -> Option<(ParsedEvent, usize)> {
    if buf.len() < METADATA_LEN {
        return None;
    }
    let event_len = rd_u32(buf, 0) as usize;
    if event_len < METADATA_LEN || event_len > buf.len() {
        return None;
    }
    let metadata_len = rd_u16(buf, 6) as usize;
    let mask = rd_u64(buf, 8);
    let pid = rd_i32(buf, 20);
    let mut ev = ParsedEvent {
        mask,
        pid,
        ..Default::default()
    };

    let mut off = metadata_len.max(METADATA_LEN);
    while off + INFO_HDR_LEN <= event_len {
        let info_type = buf[off];
        let info_len = rd_u16(buf, off + 2) as usize;
        if info_len < INFO_HDR_LEN || off + info_len > event_len {
            break;
        }
        let body = &buf[off + INFO_HDR_LEN..off + info_len];
        match info_type {
            FAN_EVENT_INFO_TYPE_FID | FAN_EVENT_INFO_TYPE_DFID => {
                ev.fid = Some(parse_fid_body(body, false));
            }
            FAN_EVENT_INFO_TYPE_DFID_NAME => {
                ev.dfid_name = Some(parse_fid_body(body, true));
            }
            FAN_EVENT_INFO_TYPE_OLD_DFID_NAME => {
                ev.old = Some(parse_fid_body(body, true));
            }
            FAN_EVENT_INFO_TYPE_NEW_DFID_NAME => {
                ev.new = Some(parse_fid_body(body, true));
            }
            _ => {}
        }
        off += info_len;
    }
    Some((ev, event_len))
}

// ---- engine -------------------------------------------------------------------------------

/// Per-share content-version counters + MODIFY coalescer state.
struct VersionState {
    versions: HashMap<NodeKey, u64>,
    modify_seen: HashMap<NodeKey, ()>, // simple per-batch dedup; reset each drain
    /// Subtree containment cache (NodeKey → is-under-share). A FILESYSTEM mark sees the whole
    /// filesystem; for a share that is a subtree of its fs we filter events to the subtree.
    contain: HashMap<NodeKey, bool>,
}

impl VersionState {
    fn new() -> Self {
        VersionState {
            versions: HashMap::new(),
            modify_seen: HashMap::new(),
            contain: HashMap::new(),
        }
    }
    fn bump(&mut self, node: NodeKey) -> u64 {
        if self.versions.len() > 1_000_000 {
            // Bound memory; eviction just restarts a file's counter (uniqueness still holds via
            // size^mtime when the host re-keys CAS — monotonicity is not required).
            self.versions.clear();
        }
        let v = self.versions.entry(node).or_insert(0);
        *v += 1;
        *v
    }

    /// Is `dir` inside the share subtree? Resolves the node's path via open_by_handle_at +
    /// /proc/self/fd and prefix-checks the share root; cached. ~1–2 µs warm.
    fn contains(&mut self, share: &Share, dir: NodeKey) -> bool {
        if !share.subtree {
            return true; // whole-mount share: every fs event is in the share
        }
        if let Some(&v) = self.contain.get(&dir) {
            return v;
        }
        if self.contain.len() > 65536 {
            self.contain.clear();
        }
        let v = resolve_abs_path(share, dir)
            .map(|p| p == share.mount_path || p.starts_with(&format!("{}/", share.mount_path)))
            .unwrap_or(false);
        self.contain.insert(dir, v);
        v
    }
}

/// Absolute path of a node via open_by_handle_at(O_PATH) + readlink(/proc/self/fd/N).
fn resolve_abs_path(share: &Share, node: NodeKey) -> Option<String> {
    let fd = share.open_node(node, libc::O_PATH).ok()?;
    let link = format!("/proc/self/fd/{}", fd.as_raw_fd());
    std::fs::read_link(link)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

/// Spawn the fanotify reader for a share on a dedicated OS thread. Records flow over `tx`.
/// `mistd_pid` is used for echo suppression (drop our own applier's events).
pub fn spawn(
    share: Arc<Share>,
    tx: SyncSender<Rec>,
    mistd_pid: i32,
) -> std::io::Result<FanotifyHandle> {
    let group = fanotify_init(
        libc::FAN_CLASS_NOTIF | FAN_REPORT_FID | FAN_REPORT_DFID_NAME,
        (libc::O_RDONLY | libc::O_LARGEFILE | libc::O_NONBLOCK) as libc::c_uint,
    )?;

    // Mark the whole filesystem the share lives on. Try with FAN_RENAME; fall back without.
    let mp = std::ffi::CString::new(share_mount_path(&share)).unwrap();
    let rename = match fanotify_mark(
        group.as_fd(),
        FAN_MARK_ADD | FAN_MARK_FILESYSTEM,
        event_mask(true),
        libc::AT_FDCWD,
        Some(&mp),
    ) {
        Ok(()) => true,
        Err(_) => {
            fanotify_mark(
                group.as_fd(),
                FAN_MARK_ADD | FAN_MARK_FILESYSTEM,
                event_mask(false),
                libc::AT_FDCWD,
                Some(&mp),
            )?;
            false
        }
    };
    tracing::info!(share = %share.info_template.name, rename, "fanotify mark established");

    let group_raw = group.as_raw_fd();
    let handle = FanotifyHandle { _group: group };
    let share2 = share.clone();
    std::thread::Builder::new()
        .name(format!("mist-fan-{}", share.info_template.name))
        .spawn(move || reader_loop(group_raw, share2, tx, mistd_pid))?;
    Ok(handle)
}

/// Keeps the fanotify group fd alive for the share's lifetime.
#[derive(Debug)]
pub struct FanotifyHandle {
    _group: OwnedFd,
}

fn share_mount_path(share: &Share) -> String {
    // For whole-mount shares this is the mountpoint; for subtree shares fanotify still marks the
    // whole filesystem (we pass the share root and the kernel resolves its fs).
    share.mount_path.clone()
}

fn reader_loop(group_raw: RawFd, share: Arc<Share>, tx: SyncSender<Rec>, mistd_pid: i32) {
    // group_raw stays valid for the reader's lifetime: FanotifyHandle owns the fd on the caller.
    let mut buf = vec![0u8; 256 * 1024];
    let mut vstate = VersionState::new();
    let pollfd = group_raw;

    loop {
        // Block until readable (poll), then drain.
        let mut pfd = libc::pollfd {
            fd: pollfd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: single valid pollfd.
        let pr = unsafe { libc::poll(&mut pfd, 1, 1000) };
        if pr <= 0 {
            continue;
        }
        // SAFETY: read into our owned buffer.
        let n = unsafe { libc::read(group_raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n <= 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::EAGAIN) || e.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            tracing::warn!(error = %e, "fanotify read failed; ending reader");
            return;
        }
        let mut off = 0usize;
        let total = n as usize;
        vstate.modify_seen.clear();
        while off < total {
            let Some((ev, elen)) = parse_event(&buf[off..total]) else {
                break;
            };
            off += elen;
            if ev.mask & FAN_Q_OVERFLOW != 0 {
                let _ = tx.try_send(Rec::Overflow);
                continue;
            }
            // Echo suppression: drop our applier's own events.
            if ev.pid == mistd_pid {
                continue;
            }
            for rec in synthesize(&ev, &share, &mut vstate) {
                if tx.try_send(rec).is_err() {
                    // Channel full: signal overflow and drop the rest of this batch.
                    let _ = tx.try_send(Rec::Overflow);
                    break;
                }
            }
        }
    }
}

/// Turn one parsed event into zero or more journal records.
fn synthesize(ev: &ParsedEvent, share: &Share, vstate: &mut VersionState) -> Vec<Rec> {
    let mask = ev.mask;

    // Rename (atomic, kernel ≥5.17). Filter by subtree containment of source/dest dirs: a rename
    // across the subtree boundary degrades to Removed (moved out) or Created (moved in).
    if mask & FAN_RENAME != 0 {
        if let (Some(old), Some(new)) = (&ev.old, &ev.new)
            && let (Some(fp), Some(fn_), Some(tp), Some(tn)) =
                (old.node, name_of(old), new.node, name_of(new))
        {
            let from_in = vstate.contains(share, fp);
            let to_in = vstate.contains(share, tp);
            return match (from_in, to_in) {
                (true, true) => vec![Rec::Renamed {
                    from_parent: fp,
                    from_name: fn_,
                    to_parent: tp,
                    to_name: tn,
                }],
                (true, false) => vec![Rec::Removed {
                    parent: fp,
                    name: fn_,
                }],
                (false, true) => created_rec(share, tp, tn),
                (false, false) => vec![],
            };
        }
        return vec![];
    }

    // Directory-entry create/delete: DFID_NAME = parent + name. Skip if the parent dir is outside
    // the share subtree (whole-mount shares short-circuit `contains` to true).
    let dn_parent_name = ev
        .dfid_name
        .as_ref()
        .and_then(|dn| Some((dn.node?, name_of(dn)?)));
    if let Some((parent, _)) = &dn_parent_name
        && !vstate.contains(share, *parent)
    {
        return vec![];
    }
    if mask & FAN_CREATE != 0
        && let Some((parent, nm)) = &dn_parent_name
    {
        return created_rec(share, *parent, nm.clone());
    }
    if mask & (FAN_DELETE | FAN_MOVED_FROM) != 0
        && let Some((parent, nm)) = dn_parent_name
    {
        return vec![Rec::Removed { parent, name: nm }];
    }
    if mask & FAN_MOVED_TO != 0
        && let Some((parent, nm)) = &dn_parent_name
    {
        // Fallback rename path (no FAN_RENAME): treat as a create at the destination.
        return created_rec(share, *parent, nm.clone());
    }

    // Object events: FID = the object's own handle. Skip if outside the share subtree.
    let obj = ev.fid.as_ref().and_then(|f| f.node);
    if let Some(node) = obj
        && !vstate.contains(share, node)
    {
        return vec![];
    }
    if mask & FAN_DELETE_SELF != 0
        && let Some(node) = obj
    {
        return vec![Rec::SelfRemoved { node }];
    }
    if mask & FAN_CLOSE_WRITE != 0
        && let Some(node) = obj
    {
        return content_rec(share, node, vstate, false);
    }
    if mask & FAN_ATTRIB != 0
        && let Some(node) = obj
    {
        return attr_rec(share, node);
    }
    if mask & FAN_MODIFY != 0
        && let Some(node) = obj
        && vstate.modify_seen.insert(node, ()).is_none()
    {
        // Coalesce: at most one in-progress Content per node per drain batch.
        return content_rec(share, node, vstate, true);
    }
    vec![]
}

fn name_of(fid: &FidName) -> Option<Name> {
    fid.name.as_ref().and_then(|n| Name::new(n.clone()).ok())
}

/// Build a Created record by statting the new entry under its parent.
fn created_rec(share: &Share, parent: NodeKey, nm: Name) -> Vec<Rec> {
    match super::rpc::stat_entry(share, parent, nm.as_bytes()) {
        Ok((node, attr)) => vec![Rec::Created {
            parent,
            name: nm,
            node,
            attr: Some(attr),
        }],
        Err(_) => {
            // Raced with deletion or unreadable: emit with attr None (host creates a placeholder).
            // We can't know the node key without a stat, so drop — the host's scrub/resync heals.
            vec![]
        }
    }
}

fn content_rec(
    share: &Share,
    node: NodeKey,
    vstate: &mut VersionState,
    in_progress: bool,
) -> Vec<Rec> {
    let version = vstate.bump(node);
    match super::rpc::stat_node(share, node) {
        Ok(attr) => vec![Rec::Content {
            node,
            version,
            size: attr.size,
            mtime: attr.mtime,
            in_progress,
        }],
        Err(_) => vec![Rec::Content {
            node,
            version,
            size: 0,
            mtime: Ts { sec: 0, nsec: 0 },
            in_progress,
        }],
    }
}

fn attr_rec(share: &Share, node: NodeKey) -> Vec<Rec> {
    match super::rpc::stat_node(share, node) {
        Ok(attr) => vec![Rec::AttrChanged { node, attr }],
        Err(_) => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-build a CREATE event with a DFID_NAME record (parent handle ino=2 gen=1, name="foo")
    /// and verify the parser extracts parent NodeKey + name.
    #[test]
    fn parse_create_dfid_name() {
        let parent_ino: u32 = 2;
        let parent_gen: u32 = 1;
        // file_handle: handle_bytes=8, handle_type=1 (FILEID_INO32_GEN), f_handle = ino||gen
        let mut fh = Vec::new();
        fh.extend_from_slice(&8u32.to_ne_bytes());
        fh.extend_from_slice(&1i32.to_ne_bytes());
        fh.extend_from_slice(&parent_ino.to_ne_bytes());
        fh.extend_from_slice(&parent_gen.to_ne_bytes());
        // info body = fsid(8) + file_handle + name + NUL
        let mut body = Vec::new();
        body.extend_from_slice(&[0u8; 8]); // fsid
        body.extend_from_slice(&fh);
        body.extend_from_slice(b"foo\0");
        // info record = header(type=2, pad, len) + body, padded to 4
        let info_len = INFO_HDR_LEN + body.len();
        let mut info = Vec::new();
        info.push(FAN_EVENT_INFO_TYPE_DFID_NAME);
        info.push(0);
        info.extend_from_slice(&(info_len as u16).to_ne_bytes());
        info.extend_from_slice(&body);
        // metadata(24) + info
        let event_len = METADATA_LEN + info.len();
        let mut ev = Vec::new();
        ev.extend_from_slice(&(event_len as u32).to_ne_bytes()); // event_len
        ev.push(0); // vers
        ev.push(0); // reserved
        ev.extend_from_slice(&(METADATA_LEN as u16).to_ne_bytes()); // metadata_len
        ev.extend_from_slice(&FAN_CREATE.to_ne_bytes()); // mask
        ev.extend_from_slice(&FAN_NOFD.to_ne_bytes()); // fd
        ev.extend_from_slice(&1234i32.to_ne_bytes()); // pid
        ev.extend_from_slice(&info);

        let (parsed, consumed) = parse_event(&ev).expect("parse");
        assert_eq!(consumed, event_len);
        assert_eq!(parsed.mask, FAN_CREATE);
        assert_eq!(parsed.pid, 1234);
        let dn = parsed.dfid_name.expect("dfid_name");
        assert_eq!(
            dn.node,
            Some(NodeKey {
                ino: 2,
                generation: 1
            })
        );
        assert_eq!(dn.name.as_deref(), Some(&b"foo"[..]));
    }

    #[test]
    fn parse_rejects_truncated() {
        assert!(parse_event(&[0u8; 8]).is_none());
        // event_len says 1000 but buffer is short
        let mut ev = vec![0u8; METADATA_LEN];
        ev[0..4].copy_from_slice(&1000u32.to_ne_bytes());
        ev[6..8].copy_from_slice(&(METADATA_LEN as u16).to_ne_bytes());
        assert!(parse_event(&ev).is_none());
    }
}
