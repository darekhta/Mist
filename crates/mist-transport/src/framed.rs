//! Frame I/O over a byte stream: 16-byte header + payload, lane-capped.

use mist_proto::frame::HEADER_LEN;
use mist_proto::{DecodeError, FrameHeader, FrameKind, Validate, caps};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    #[error("frame: {0}")]
    Frame(#[from] mist_proto::validate::ValidateError),
    #[error("protocol: {0}")]
    Protocol(&'static str),
    #[error("bad endpoint: {0}")]
    BadEndpoint(String),
    #[error("bridge handshake failed: {0}")]
    BridgeHandshake(String),
    #[error("dial timed out")]
    Timeout,
    #[error("peer closed")]
    Closed,
}

#[derive(Debug)]
pub struct RecvFrame {
    pub kind: FrameKind,
    pub flags: u8,
    pub seq: u64,
    pub payload: Vec<u8>,
}

/// `recv()` and `send_*()` are NOT cancel-safe (partial frames desync the stream). Never put
/// both directions of one stream in a `tokio::select!` — use `into_split()` and give each half
/// its own task instead.
pub struct FramedStream {
    io: crate::Stream,
    max_payload: usize,
}

impl std::fmt::Debug for FramedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedStream")
            .field("max_payload", &self.max_payload)
            .finish()
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(
    io: &mut W,
    max_payload: usize,
    kind: FrameKind,
    flags: u8,
    seq: u64,
    payload: &[u8],
) -> Result<(), TransportError> {
    if payload.len() > max_payload {
        return Err(TransportError::Protocol("frame exceeds lane cap"));
    }
    let hdr = FrameHeader {
        len: payload.len() as u32,
        kind,
        flags,
        seq,
    };
    // Two writes; the OS coalesces (TCP_NODELAY set; vsock/uds unaffected). Header is tiny.
    io.write_all(&hdr.encode()).await?;
    io.write_all(payload).await?;
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    io: &mut R,
    max_payload: usize,
) -> Result<RecvFrame, TransportError> {
    let mut hdr = [0u8; HEADER_LEN];
    match io.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(TransportError::Closed);
        }
        Err(e) => return Err(e.into()),
    }
    let h = FrameHeader::decode(&hdr, max_payload)?;
    let mut payload = vec![0u8; h.len as usize];
    io.read_exact(&mut payload).await?;
    Ok(RecvFrame {
        kind: h.kind,
        flags: h.flags,
        seq: h.seq,
        payload,
    })
}

impl FramedStream {
    pub fn new(io: crate::Stream, bulk: bool) -> Self {
        let max_payload = if bulk {
            caps::MAX_FRAME_BULK
        } else {
            caps::MAX_FRAME
        };
        FramedStream { io, max_payload }
    }

    pub fn with_bulk_cap(mut self, bulk: bool) -> Self {
        self.max_payload = if bulk {
            caps::MAX_FRAME_BULK
        } else {
            caps::MAX_FRAME
        };
        self
    }

    /// Split into independently-owned read/write halves so each direction can live on its own
    /// task. Required for any lane with concurrent traffic both ways (rpc, ctl).
    pub fn into_split(self) -> (FramedReader, FramedWriter) {
        let (r, w) = tokio::io::split(self.io);
        (
            FramedReader {
                io: r,
                max_payload: self.max_payload,
            },
            FramedWriter {
                io: w,
                max_payload: self.max_payload,
            },
        )
    }

    /// Send one frame with an already-encoded payload.
    pub async fn send_frame(
        &mut self,
        kind: FrameKind,
        flags: u8,
        seq: u64,
        payload: &[u8],
    ) -> Result<(), TransportError> {
        write_frame(&mut self.io, self.max_payload, kind, flags, seq, payload).await
    }

    /// Encode + send a message as a single frame.
    pub async fn send_msg<T: serde::Serialize>(
        &mut self,
        kind: FrameKind,
        seq: u64,
        msg: &T,
    ) -> Result<(), TransportError> {
        let payload = mist_proto::encode(msg);
        if payload.len() > self.max_payload {
            return Err(TransportError::Protocol("encoded message exceeds lane cap"));
        }
        self.send_frame(kind, 0, seq, &payload).await
    }

    pub async fn flush(&mut self) -> Result<(), TransportError> {
        self.io.flush().await?;
        Ok(())
    }

    /// Receive one frame. Returns `Closed` on clean EOF at a frame boundary.
    pub async fn recv(&mut self) -> Result<RecvFrame, TransportError> {
        read_frame(&mut self.io, self.max_payload).await
    }

    /// Receive and decode a single-frame message of the expected frame kind.
    pub async fn recv_msg<T: serde::de::DeserializeOwned + Validate>(
        &mut self,
        expect: FrameKind,
    ) -> Result<(u64, T), TransportError> {
        let f = self.recv().await?;
        if f.kind != expect {
            return Err(TransportError::Protocol("unexpected frame kind"));
        }
        Ok((f.seq, mist_proto::decode(&f.payload)?))
    }
}

/// Read half of a split [`FramedStream`].
pub struct FramedReader {
    io: tokio::io::ReadHalf<crate::Stream>,
    max_payload: usize,
}

impl std::fmt::Debug for FramedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedReader")
            .field("max_payload", &self.max_payload)
            .finish()
    }
}

impl FramedReader {
    /// Receive one frame. Returns `Closed` on clean EOF at a frame boundary.
    pub async fn recv(&mut self) -> Result<RecvFrame, TransportError> {
        read_frame(&mut self.io, self.max_payload).await
    }
}

/// Write half of a split [`FramedStream`].
pub struct FramedWriter {
    io: tokio::io::WriteHalf<crate::Stream>,
    max_payload: usize,
}

impl std::fmt::Debug for FramedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedWriter")
            .field("max_payload", &self.max_payload)
            .finish()
    }
}

impl FramedWriter {
    pub async fn send_frame(
        &mut self,
        kind: FrameKind,
        flags: u8,
        seq: u64,
        payload: &[u8],
    ) -> Result<(), TransportError> {
        write_frame(&mut self.io, self.max_payload, kind, flags, seq, payload).await
    }

    pub async fn send_msg<T: serde::Serialize>(
        &mut self,
        kind: FrameKind,
        seq: u64,
        msg: &T,
    ) -> Result<(), TransportError> {
        let payload = mist_proto::encode(msg);
        if payload.len() > self.max_payload {
            return Err(TransportError::Protocol("encoded message exceeds lane cap"));
        }
        self.send_frame(kind, 0, seq, &payload).await
    }

    pub async fn flush(&mut self) -> Result<(), TransportError> {
        self.io.flush().await?;
        Ok(())
    }
}
