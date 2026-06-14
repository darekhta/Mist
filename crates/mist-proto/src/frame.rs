//! 16-byte frame header: `len:u32 | kind:u8 | flags:u8 | rsv:u16(=0) | seq:u64`, little-endian.

use crate::caps;
use crate::validate::ValidateError;

pub const HEADER_LEN: usize = 16;

/// Payload continues in the next frame with the same `seq`.
pub const FLAG_MORE: u8 = 1 << 0;
/// Payload is lz4-compressed when negotiated; the current implementation does not set it.
pub const FLAG_COMPRESSED: u8 = 1 << 1;

const KNOWN_FLAGS: u8 = FLAG_MORE | FLAG_COMPRESSED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Ctl = 0,
    Req = 1,
    Resp = 2,
    Event = 3,
    Bulk = 4,
}

impl FrameKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => FrameKind::Ctl,
            1 => FrameKind::Req,
            2 => FrameKind::Resp,
            3 => FrameKind::Event,
            4 => FrameKind::Bulk,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub len: u32,
    pub kind: FrameKind,
    pub flags: u8,
    pub seq: u64,
}

impl FrameHeader {
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0..4].copy_from_slice(&self.len.to_le_bytes());
        b[4] = self.kind as u8;
        b[5] = self.flags;
        // b[6..8] reserved = 0
        b[8..16].copy_from_slice(&self.seq.to_le_bytes());
        b
    }

    /// Decode and validate against the lane's payload cap.
    pub fn decode(b: &[u8; HEADER_LEN], max_payload: usize) -> Result<Self, ValidateError> {
        let len = u32::from_le_bytes(b[0..4].try_into().unwrap());
        let kind = FrameKind::from_u8(b[4]).ok_or(ValidateError::new("unknown frame kind"))?;
        let flags = b[5];
        if flags & !KNOWN_FLAGS != 0 {
            return Err(ValidateError::new("unknown frame flags"));
        }
        if b[6] != 0 || b[7] != 0 {
            return Err(ValidateError::new("reserved header bytes nonzero"));
        }
        if len as usize > max_payload {
            return Err(ValidateError::new("frame exceeds lane cap"));
        }
        let seq = u64::from_le_bytes(b[8..16].try_into().unwrap());
        Ok(FrameHeader {
            len,
            kind,
            flags,
            seq,
        })
    }

    /// The payload cap for a lane: bulk lanes get the big cap.
    pub const fn cap_for_bulk(is_bulk_lane: bool) -> usize {
        if is_bulk_lane {
            caps::MAX_FRAME_BULK
        } else {
            caps::MAX_FRAME
        }
    }
}
