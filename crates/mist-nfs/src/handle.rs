//! NFS file handles: opaque bearer tokens, so each carries a keyed-BLAKE3 MAC to stop local
//! forgery (design 05 §2). 28 bytes — fits NFSv3's 64-byte limit with room to spare.
//!
//! Layout: `"MST1"(4) | share:u16 | flags:u16 | ino:u64 | gen:u32 | mac:u64` (big-endian).

use mist_proto::NodeKey;

const MAGIC: &[u8; 4] = b"MST1";
pub const HANDLE_LEN: usize = 4 + 2 + 2 + 8 + 4 + 8;

#[derive(Debug, Clone)]
pub struct HandleCodec {
    key: blake3::Hash,
}

impl HandleCodec {
    pub fn new(secret: &[u8]) -> Self {
        HandleCodec {
            key: blake3::hash(secret),
        }
    }

    fn mac(&self, share: u16, flags: u16, node: NodeKey) -> u64 {
        let mut h = blake3::Hasher::new_keyed(self.key.as_bytes());
        h.update(MAGIC);
        h.update(&share.to_be_bytes());
        h.update(&flags.to_be_bytes());
        h.update(&node.ino.to_be_bytes());
        h.update(&node.generation.to_be_bytes());
        u64::from_be_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
    }

    pub fn encode(&self, share: u16, node: NodeKey) -> Vec<u8> {
        let flags = 0u16;
        let mut b = Vec::with_capacity(HANDLE_LEN);
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&share.to_be_bytes());
        b.extend_from_slice(&flags.to_be_bytes());
        b.extend_from_slice(&node.ino.to_be_bytes());
        b.extend_from_slice(&node.generation.to_be_bytes());
        b.extend_from_slice(&self.mac(share, flags, node).to_be_bytes());
        b
    }

    /// Decode + verify. Returns (share, NodeKey) or None on bad magic/length/MAC.
    pub fn decode(&self, fh: &[u8]) -> Option<(u16, NodeKey)> {
        if fh.len() != HANDLE_LEN || &fh[0..4] != MAGIC {
            return None;
        }
        let share = u16::from_be_bytes(fh[4..6].try_into().unwrap());
        let flags = u16::from_be_bytes(fh[6..8].try_into().unwrap());
        let ino = u64::from_be_bytes(fh[8..16].try_into().unwrap());
        let generation = u32::from_be_bytes(fh[16..20].try_into().unwrap());
        let mac = u64::from_be_bytes(fh[20..28].try_into().unwrap());
        let node = NodeKey { ino, generation };
        // Constant-time-ish compare (u64 eq); the MAC space is 2^64 against a rate-limited service.
        if mac != self.mac(share, flags, node) {
            return None;
        }
        Some((share, node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let c = HandleCodec::new(b"secret");
        let node = NodeKey {
            ino: 0xABCD,
            generation: 9,
        };
        let fh = c.encode(3, node);
        assert_eq!(fh.len(), HANDLE_LEN);
        assert_eq!(c.decode(&fh), Some((3, node)));
    }

    #[test]
    fn forgery_rejected() {
        let c = HandleCodec::new(b"secret");
        let node = NodeKey {
            ino: 1,
            generation: 1,
        };
        let mut fh = c.encode(0, node);
        fh[10] ^= 0xFF; // tamper with the inode
        assert_eq!(c.decode(&fh), None);
    }

    #[test]
    fn wrong_key_rejected() {
        let a = HandleCodec::new(b"key-a");
        let b = HandleCodec::new(b"key-b");
        let fh = a.encode(
            0,
            NodeKey {
                ino: 5,
                generation: 0,
            },
        );
        assert_eq!(b.decode(&fh), None);
    }

    #[test]
    fn bad_length_and_magic() {
        let c = HandleCodec::new(b"k");
        assert_eq!(c.decode(&[0u8; 10]), None);
        assert_eq!(c.decode(&[0u8; HANDLE_LEN]), None); // bad magic
    }
}
