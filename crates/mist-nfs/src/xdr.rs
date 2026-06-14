//! Minimal XDR (RFC 4506) codec: big-endian, 4-byte aligned. Hand-written so we control exactly
//! what goes on the wire — macOS's NFS client is unforgiving about encoding.

/// Encodes XDR primitives into a growing buffer.
#[derive(Debug, Default)]
pub struct XdrWriter {
    buf: Vec<u8>,
}

impl XdrWriter {
    pub fn new() -> Self {
        XdrWriter {
            buf: Vec::with_capacity(512),
        }
    }

    /// Pre-sized writer (streaming READ replies grow to rsize; doubling reallocs copy ~2× the
    /// payload before settling).
    pub fn with_capacity(cap: usize) -> Self {
        XdrWriter {
            buf: Vec::with_capacity(cap),
        }
    }

    /// Wrap a recycled buffer (must be empty).
    pub fn from_vec(buf: Vec<u8>) -> Self {
        debug_assert!(buf.is_empty());
        XdrWriter { buf }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Current write position (for later patching).
    pub fn pos(&self) -> usize {
        self.buf.len()
    }

    /// Overwrite a u32 written earlier at `pos` (compound status/numres back-patching).
    pub fn patch_u32(&mut self, pos: usize, v: u32) {
        self.buf[pos..pos + 4].copy_from_slice(&v.to_be_bytes());
    }

    /// Rewind to `pos`, discarding everything after (replay-cache restart).
    pub fn truncate(&mut self, pos: usize) {
        self.buf.truncate(pos);
    }

    /// Borrow bytes written from `pos` to the end (replay-cache capture without a copy chain).
    pub fn since(&self, pos: usize) -> &[u8] {
        &self.buf[pos..]
    }

    /// Direct buffer access for fused payload writes (callers must restore 4-byte alignment
    /// with [`pad_to_4`]).
    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Pad with zeros to a 4-byte boundary (XDR opaque alignment).
    pub fn pad_to_4(&mut self) {
        while !self.buf.len().is_multiple_of(4) {
            self.buf.push(0);
        }
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn bool(&mut self, v: bool) {
        self.u32(if v { 1 } else { 0 });
    }

    /// Variable-length opaque: u32 length prefix + data + zero padding to 4 bytes.
    pub fn opaque(&mut self, data: &[u8]) {
        self.u32(data.len() as u32);
        self.buf.extend_from_slice(data);
        self.pad(data.len());
    }

    /// Fixed-length opaque: data + padding, no length prefix.
    pub fn fixed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        self.pad(data.len());
    }

    fn pad(&mut self, len: usize) {
        let rem = len % 4;
        if rem != 0 {
            self.buf.extend(std::iter::repeat_n(0u8, 4 - rem));
        }
    }
}

/// Decodes XDR primitives from a byte slice.
#[derive(Debug)]
pub struct XdrReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("XDR underflow")]
pub struct XdrError;

type R<T> = Result<T, XdrError>;

impl<'a> XdrReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        XdrReader { buf, pos: 0 }
    }

    /// Independent reader at the same position (peeking a fast path without consuming).
    pub fn clone_reader(&self) -> XdrReader<'a> {
        XdrReader {
            buf: self.buf,
            pos: self.pos,
        }
    }

    #[allow(dead_code)] // used in tests + by future request validators
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(XdrError);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u32(&mut self) -> R<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> R<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    #[allow(dead_code)] // used in tests; NFS args are u32-coded bools
    pub fn bool(&mut self) -> R<bool> {
        Ok(self.u32()? != 0)
    }

    /// Variable-length opaque with a cap to bound allocation.
    pub fn opaque(&mut self, max: usize) -> R<&'a [u8]> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(XdrError);
        }
        let data = self.take(len)?;
        // Skip padding.
        let rem = len % 4;
        if rem != 0 {
            self.take(4 - rem)?;
        }
        Ok(data)
    }

    pub fn fixed(&mut self, n: usize) -> R<&'a [u8]> {
        let data = self.take(n)?;
        let rem = n % 4;
        if rem != 0 {
            self.take(4 - rem)?;
        }
        Ok(data)
    }

    /// Skip `n` 4-byte-aligned opaque bytes (e.g. an auth body we ignore).
    pub fn skip_opaque(&mut self, max: usize) -> R<()> {
        self.opaque(max)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_primitives() {
        let mut w = XdrWriter::new();
        w.u32(0xDEAD_BEEF);
        w.u64(0x0102_0304_0506_0708);
        w.bool(true);
        w.opaque(b"hello"); // 5 bytes → padded to 8
        w.fixed(&[1, 2, 3, 4]);
        let bytes = w.into_bytes();

        let mut r = XdrReader::new(&bytes);
        assert_eq!(r.u32().unwrap(), 0xDEAD_BEEF);
        assert_eq!(r.u64().unwrap(), 0x0102_0304_0506_0708);
        assert!(r.bool().unwrap());
        assert_eq!(r.opaque(16).unwrap(), b"hello");
        assert_eq!(r.fixed(4).unwrap(), &[1, 2, 3, 4]);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn opaque_padding_is_4_byte_aligned() {
        let mut w = XdrWriter::new();
        w.opaque(b"abc"); // 3 bytes
        let b = w.into_bytes();
        assert_eq!(b.len(), 4 + 4); // length prefix + 3 data + 1 pad
        assert_eq!(&b[4..7], b"abc");
        assert_eq!(b[7], 0);
    }

    #[test]
    fn underflow_errors_not_panics() {
        let mut r = XdrReader::new(&[0, 0, 0]);
        assert_eq!(r.u32(), Err(XdrError));
    }

    #[test]
    fn opaque_cap_enforced() {
        let mut w = XdrWriter::new();
        let big = [0u8; 100];
        w.opaque(&big);
        let b = w.into_bytes();
        let mut r = XdrReader::new(&b);
        assert_eq!(r.opaque(16), Err(XdrError));
    }
}
