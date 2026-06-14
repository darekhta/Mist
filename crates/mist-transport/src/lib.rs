//! Transport: framed MWP streams over a dialable byte-stream, with lane setup.
//!
//! The only primitive Mist needs from a transport is "open an independent reliable ordered byte
//! stream to the peer". Concrete connectors:
//! - `tcp:<host>:<port>` — virtio-net fallback / remote Linux / tests
//! - `uds:<path>` — local tests, fake guests
//! - `bridge:<path>` — UDS exposed by `MistBridge` inside the VM supervisor; speaks the
//!   Firecracker vsock convention (`CONNECT <port>\n` → `OK <port>\n`) before raw bytes.

mod framed;

pub use framed::{FramedReader, FramedStream, FramedWriter, RecvFrame, TransportError};

use mist_proto::{CtlMsg, Lane, VSOCK_PORT};
use std::fmt;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, UnixStream};

/// Where the peer lives. Parsed from config strings like `tcp:192.168.64.5:6478`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(String),
    Uds(std::path::PathBuf),
    /// MistBridge UDS; `port` is the guest vsock port to request (default [`VSOCK_PORT`]).
    Bridge {
        path: std::path::PathBuf,
        port: u32,
    },
}

impl Endpoint {
    pub fn parse(s: &str) -> Result<Self, TransportError> {
        if let Some(rest) = s.strip_prefix("tcp:") {
            Ok(Endpoint::Tcp(rest.to_string()))
        } else if let Some(rest) = s.strip_prefix("uds:") {
            Ok(Endpoint::Uds(rest.into()))
        } else if let Some(rest) = s.strip_prefix("bridge:") {
            // Optional "#<port>" suffix selects a non-default guest vsock port.
            let (path, port) = match rest.rsplit_once('#') {
                Some((p, port)) => (
                    p,
                    port.parse()
                        .map_err(|_| TransportError::BadEndpoint(s.to_string()))?,
                ),
                None => (rest, VSOCK_PORT),
            };
            Ok(Endpoint::Bridge {
                path: path.into(),
                port,
            })
        } else {
            Err(TransportError::BadEndpoint(s.to_string()))
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Tcp(a) => write!(f, "tcp:{a}"),
            Endpoint::Uds(p) => write!(f, "uds:{}", p.display()),
            Endpoint::Bridge { path, port } => write!(f, "bridge:{}#{port}", path.display()),
        }
    }
}

/// A connected byte stream, type-erased.
pub type Stream = Box<dyn StreamIo>;

pub trait StreamIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> StreamIo for T {}

/// Hard ceiling on connect + bridge handshake. Without it a bridge whose `device.connect`
/// never completes (seen on AVF when the guest's vsock is half-up) wedges the caller forever.
const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Dial one raw stream to the endpoint (performing the bridge CONNECT handshake if needed).
pub async fn dial(ep: &Endpoint) -> Result<Stream, TransportError> {
    tokio::time::timeout(DIAL_TIMEOUT, dial_inner(ep))
        .await
        .map_err(|_| TransportError::Timeout)?
}

async fn dial_inner(ep: &Endpoint) -> Result<Stream, TransportError> {
    match ep {
        Endpoint::Tcp(addr) => {
            let s = TcpStream::connect(addr).await?;
            s.set_nodelay(true)?;
            Ok(Box::new(s) as Stream)
        }
        Endpoint::Uds(path) => Ok(Box::new(UnixStream::connect(path).await?) as Stream),
        Endpoint::Bridge { path, port } => {
            let mut s = UnixStream::connect(path).await?;
            firecracker_connect(&mut s, *port).await?;
            Ok(Box::new(s) as Stream)
        }
    }
}

/// Firecracker vsock-UDS convention: send `CONNECT <port>\n`, expect `OK <assigned>\n`.
async fn firecracker_connect(s: &mut UnixStream, port: u32) -> Result<(), TransportError> {
    use tokio::io::AsyncWriteExt;
    s.write_all(format!("CONNECT {port}\n").as_bytes()).await?;
    // Read the ack line byte-by-byte: we must not consume bytes past '\n' — they belong to MWP.
    let mut line = Vec::with_capacity(16);
    loop {
        let b = s.read_u8().await?;
        if b == b'\n' {
            break;
        }
        line.push(b);
        if line.len() > 64 {
            return Err(TransportError::BridgeHandshake("ack line too long".into()));
        }
    }
    let line = String::from_utf8_lossy(&line);
    if line.starts_with("OK") {
        Ok(())
    } else {
        Err(TransportError::BridgeHandshake(line.into_owned()))
    }
}

/// Dial a lane: open a stream and, for non-ctl lanes, send `StreamHello` as the first frame.
///
/// The ctl lane carries `Hello` as its first frame instead; the listener classifies streams by
/// their first ctl message.
pub async fn dial_lane(
    ep: &Endpoint,
    session_id: u64,
    lane: Lane,
    idx: u8,
) -> Result<FramedStream, TransportError> {
    let is_bulk = lane == Lane::Bulk;
    let stream = dial(ep).await?;
    let mut framed = FramedStream::new(stream, is_bulk);
    if lane != Lane::Ctl {
        framed
            .send_msg(
                mist_proto::FrameKind::Ctl,
                0,
                &CtlMsg::StreamHello {
                    session_id,
                    lane,
                    idx,
                },
            )
            .await?;
    }
    Ok(framed)
}

/// Listener side: read the first frame of an accepted stream and return its ctl message
/// (`Hello` ⇒ new session ctl lane; `StreamHello` ⇒ secondary lane). Used by mistd and fakes.
pub async fn classify_accepted(stream: Stream) -> Result<(FramedStream, CtlMsg), TransportError> {
    // The first frame is always a small control message. After decoding `StreamHello`, switch
    // only true bulk lanes to the larger cap; ctl/rpc/journal keep the normal frame ceiling.
    let mut framed = FramedStream::new(stream, false);
    let first = framed.recv().await?;
    if first.kind != mist_proto::FrameKind::Ctl {
        return Err(TransportError::Protocol(
            "first frame on stream was not ctl",
        ));
    }
    let msg = mist_proto::decode::<CtlMsg>(&first.payload)?;
    let framed = framed.with_bulk_cap(matches!(
        msg,
        CtlMsg::StreamHello {
            lane: Lane::Bulk,
            ..
        }
    ));
    Ok((framed, msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_endpoint_parse() {
        match Endpoint::parse("bridge:/tmp/x.sock").unwrap() {
            Endpoint::Bridge { path, port } => {
                assert_eq!(path.to_str().unwrap(), "/tmp/x.sock");
                assert_eq!(port, mist_proto::VSOCK_PORT);
            }
            other => panic!("{other:?}"),
        }
        match Endpoint::parse("bridge:/tmp/x.sock#6479").unwrap() {
            Endpoint::Bridge { path, port } => {
                assert_eq!(
                    path.to_str().unwrap(),
                    "/tmp/x.sock",
                    "fragment must be stripped"
                );
                assert_eq!(port, 6479);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            Endpoint::parse("tcp:1.2.3.4:9").unwrap(),
            Endpoint::Tcp(_)
        ));
        assert!(Endpoint::parse("bogus:x").is_err());
    }
}
