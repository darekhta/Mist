//! MWP/1 — the Mist wire protocol.
//!
//! Everything that crosses the host↔guest boundary is defined here and nowhere else.
//! Wire ABI rules:
//! - enum variant order and struct field order are the wire format (postcard): **append only**,
//!   never reorder or remove; new variants must be gated behind a negotiated feature bit so an
//!   old peer never receives them.
//! - every collection and byte field has a hard cap, enforced by [`Validate`] on decode; the
//!   framing layer enforces the outer size caps. Guest input is hostile input.

pub mod caps;
pub mod codec;
pub mod frame;
pub mod msg;
pub mod types;
pub mod validate;

pub use codec::{DecodeError, decode, encode};
pub use frame::{FLAG_MORE, FrameHeader, FrameKind};
pub use msg::*;
pub use types::*;
pub use validate::Validate;

/// MWP major version. Mismatch ⇒ refuse session.
pub const PROTO_VERSION: u32 = 1;

/// Guest vsock port mistd listens on ("MIST" on a T9 keypad).
pub const VSOCK_PORT: u32 = 6478;

/// Default TCP port (fallback transport), same number.
pub const TCP_PORT: u16 = 6478;

/// Negotiated feature bits (effective set = intersection of both Hellos).
pub mod features {
    /// Guest kernel delivers FAN_RENAME (atomic rename records).
    pub const RENAME_EV: u64 = 1 << 0;
    /// lz4 frame compression permitted on journal/snapshot lanes.
    pub const LZ4: u64 = 1 << 1;
    /// Rec::CreatedBatch may be emitted.
    pub const CREATED_BATCH: u64 = 1 << 2;
    /// 64-bit inode filesystems (non-ext4) supported.
    pub const INO64: u64 = 1 << 3;
    /// Reserved: xattr RPCs.
    pub const XATTR: u64 = 1 << 4;
    /// Reserved: journal ring-buffer replay on reconnect (v2).
    pub const JOURNAL_REPLAY: u64 = 1 << 5;
    /// Reserved bit (1 << 6) — kept free so VM_IDENTITY lands on the documented bit.
    const _RESERVED_6: u64 = 1 << 6;
    /// Stable VM identity exchange (design 11 §6): when both peers set this bit, the guest sends
    /// [`CtlMsg::VmIdentity`] right after `HelloAck`. The host sets it for `bridge="auto"` VMs and
    /// in the resolver's authenticated probe; the guest always advertises support. An old guest
    /// that lacks the bit simply never sends `VmIdentity` — the host then treats the session as
    /// not identity-bound (legacy explicit bridges only).
    pub const VM_IDENTITY: u64 = 1 << 7;

    /// Features this build implements.
    pub const SUPPORTED: u64 = RENAME_EV | CREATED_BATCH | VM_IDENTITY;
}

/// Format a 16-byte `vm_uuid` as 32 lowercase hex characters (the form stored in `config.toml`
/// and advertised in the `_mist._tcp` TXT record). Identity, never a secret (design 11 §6).
pub fn vm_uuid_hex(uuid: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in uuid {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Parse a `vm_uuid` from hex. Accepts 32 hex chars with optional `-` grouping (the UI may render
/// a grouped form); returns `None` on any malformed input.
pub fn vm_uuid_from_hex(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = s.bytes().filter(|b| *b != b'-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod uuid_tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let u = [
            0x4f, 0x2b, 0x0b, 0x30, 0xa9, 0xd8, 0x4f, 0x8a, 0xb5, 0xc4, 0x01, 0x22, 0xc7, 0xbe,
            0x1c, 0x13,
        ];
        let h = vm_uuid_hex(&u);
        assert_eq!(h, "4f2b0b30a9d84f8ab5c40122c7be1c13");
        assert_eq!(vm_uuid_from_hex(&h), Some(u));
        // Grouped rendering parses back to the same bytes.
        assert_eq!(
            vm_uuid_from_hex("4f2b0b30-a9d8-4f8a-b5c4-0122c7be1c13"),
            Some(u)
        );
    }

    #[test]
    fn hex_rejects_malformed() {
        assert_eq!(vm_uuid_from_hex("nothex"), None);
        assert_eq!(vm_uuid_from_hex("4f2b"), None); // too short
        assert_eq!(vm_uuid_from_hex("zz2b0b30a9d84f8ab5c40122c7be1c13"), None);
    }
}
