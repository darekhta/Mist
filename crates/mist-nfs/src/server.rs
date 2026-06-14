//! NFS+MOUNT TCP server: ONC-RPC record-marking framing, per-connection loop, program dispatch.
//! One instance serves one share (NFS program 100003 + MOUNT 100005) on a loopback port.
//!
//! Requests on a connection are processed CONCURRENTLY (bounded) and replies may go out on the
//! wire out of order — ONC-RPC matches replies by xid, and NFS clients pipeline many requests
//! per connection (readahead, async writes). A serial loop here was measured to cap sequential
//! reads at ~1/4 of what the same client gets once the pipeline is honored.
//!
//! Hot-path latency: when nothing is in flight (the sequential-read case — macOS keeps ONE
//! 1 MiB READ outstanding per stream), the request is handled INLINE on the reader task and the
//! reply leaves in a single `writev` — no spawn, no channel handoff, no extra wakeups. Replies
//! from concurrent handlers serialize on a shared write half instead of a writer task.

use crate::handle::HandleCodec;
use crate::surface::{MountSurface, SendableRead};
use crate::{mount3, nfs3, rpc};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// One reply on its way out: either fully rendered, or a rendered head + a file range that the
/// writer streams with `sendfile` (zero copies through userspace — the knfsd trick).
pub enum Reply {
    Whole(Vec<u8>),
    HeadAndFile {
        head: Vec<u8>,
        body: SendableRead,
        pad: usize,
    },
}

/// Write half shared between the inline path, spawned handlers, and (v4.1) the backchannel.
/// The lock is held per-reply; record framing stays intact because each reply is written whole.
pub(crate) type SharedWriter = Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>;

const MAX_RECORD: usize = 2 * 1024 * 1024; // request cap (1 MiB write + envelope)
/// Per-connection concurrent request cap (macOS keeps ≤ readahead+slots in flight).
const INFLIGHT: usize = 128;
/// Read-side buffer: coalesces the 4-byte marker + small request into one `read(2)`.
pub(crate) const READ_BUF: usize = 256 * 1024;

/// Grow the socket buffers so a 1 MiB reply leaves in one `write` instead of cycling through
/// kqueue writability waits against the ~600 KiB default. Tries 4 MiB, halves on rejection.
pub(crate) fn tune_socket(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    for opt in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        for sz in [4 << 20, 2 << 20, 1 << 20] {
            let v: libc::c_int = sz;
            // SAFETY: setsockopt on an fd we own, with a properly sized c_int.
            #[allow(unsafe_code)]
            let rc = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    opt,
                    std::ptr::from_ref(&v).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if rc == 0 {
                break;
            }
        }
    }
}

/// Send one record-marked reply over the shared write half: marker + payload in a single
/// `writev`, falling back to plain writes on partial progress.
pub(crate) async fn write_reply(wr: &SharedWriter, reply: Reply) -> std::io::Result<()> {
    match reply {
        Reply::Whole(payload) => {
            if payload.is_empty() {
                return Ok(());
            }
            let marker = (0x8000_0000u32 | (payload.len() as u32 & 0x7FFF_FFFF)).to_be_bytes();
            {
                let mut w = wr.lock().await;
                write_marked(&mut w, &marker, &payload).await?;
            }
            crate::bufpool::give(payload);
            Ok(())
        }
        Reply::HeadAndFile { head, body, pad } => {
            let total = head.len() + body.len as usize + pad;
            let marker = (0x8000_0000u32 | (total as u32 & 0x7FFF_FFFF)).to_be_bytes();
            {
                let mut w = wr.lock().await;
                write_marked(&mut w, &marker, &head).await?;
                sendfile_all(&w, &body).await?;
                if pad > 0 {
                    w.write_all(&[0u8; 3][..pad]).await?;
                }
            }
            crate::bufpool::give(head);
            Ok(())
        }
    }
}

/// `writev` the 4-byte marker and the payload together; one syscall in the common case.
async fn write_marked(
    wr: &mut tokio::net::tcp::OwnedWriteHalf,
    marker: &[u8; 4],
    payload: &[u8],
) -> std::io::Result<()> {
    use std::io::IoSlice;
    let total = 4 + payload.len();
    let mut done = 0usize;
    while done < total {
        let n = if done < 4 {
            wr.write_vectored(&[IoSlice::new(&marker[done..]), IoSlice::new(payload)])
                .await?
        } else {
            wr.write(&payload[done - 4..]).await?
        };
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "socket closed mid-reply",
            ));
        }
        done += n;
    }
    Ok(())
}

pub struct NfsServer<S: MountSurface> {
    surface: Arc<S>,
    codec: HandleCodec,
}

impl<S: MountSurface> std::fmt::Debug for NfsServer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NfsServer")
    }
}

impl<S: MountSurface> NfsServer<S> {
    pub fn new(surface: Arc<S>, handle_secret: &[u8]) -> Self {
        NfsServer {
            surface,
            codec: HandleCodec::new(handle_secret),
        }
    }

    /// Serve until the listener errors. Bind with [`TcpListener::bind`] on `127.0.0.1:<port>`.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, _peer) = listener.accept().await?;
            stream.set_nodelay(true).ok();
            tune_socket(&stream);
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.serve_conn(stream).await {
                    tracing::debug!(error = %e, "nfs connection ended");
                }
            });
        }
    }

    async fn serve_conn(self: &Arc<Self>, stream: TcpStream) -> std::io::Result<()> {
        let (rd, wr) = stream.into_split();
        let mut rd = tokio::io::BufReader::with_capacity(READ_BUF, rd);
        let wr: SharedWriter = Arc::new(tokio::sync::Mutex::new(wr));
        let sem = Arc::new(Semaphore::new(INFLIGHT));
        loop {
            let Some(record) = read_record(&mut rd).await? else {
                break; // clean EOF
            };
            // Inline fast path: nothing in flight and nothing else already buffered — handle on
            // this task and skip both the spawn and the channel handoff.
            if crate::nfs41::inline_dispatch()
                && sem.available_permits() == INFLIGHT
                && rd.buffer().is_empty()
            {
                let reply = self.handle_record(&record).await;
                if write_reply(&wr, reply).await.is_err() {
                    break;
                }
                continue;
            }
            let Ok(permit) = sem.clone().acquire_owned().await else {
                break;
            };
            let me = self.clone();
            let wr2 = wr.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let reply = me.handle_record(&record).await;
                let _ = write_reply(&wr2, reply).await;
            });
        }
        // In-flight handlers hold clones of `wr`; the socket closes when the last one drops.
        Ok(())
    }

    async fn handle_record(&self, record: &[u8]) -> Reply {
        let (call, mut args) = match rpc::parse_call(record) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "rpc parse failed");
                // Can't extract xid reliably; drop (client will time out / retry).
                return Reply::Whole(Vec::new());
            }
        };

        let w = match (call.program, call.version) {
            (nfs3::NFS_PROGRAM, nfs3::NFS_VERSION) => {
                let w = rpc::reply_accepted(call.xid, rpc::ACCEPT_SUCCESS);
                return nfs3::dispatch(call.procedure, &mut args, &self.surface, &self.codec, w)
                    .await;
            }
            (mount3::MOUNT_PROGRAM, mount3::MOUNT_VERSION) => {
                let w = rpc::reply_accepted(call.xid, rpc::ACCEPT_SUCCESS);
                mount3::dispatch(call.procedure, &mut args, &self.surface, &self.codec, w)
            }
            (nfs3::NFS_PROGRAM, _) => rpc::reply_prog_mismatch(call.xid, 3, 3),
            (mount3::MOUNT_PROGRAM, _) => rpc::reply_prog_mismatch(call.xid, 3, 3),
            _ => rpc::reply_accepted(call.xid, rpc::ACCEPT_PROG_UNAVAIL),
        };
        Reply::Whole(w.into_bytes())
    }
}

/// Stream a file range to the (nonblocking) socket. The Mist host runs on macOS, where this
/// uses BSD `sendfile(2)` for a zero-copy path. The crate is also *compiled* (never run) for
/// Linux by `cargo test --workspace`; there `sendfile(2)` has a different (4-arg) signature, so
/// that target gets a portable pread + nonblocking-write fallback. Loops on partial sends,
/// awaiting socket writability between attempts.
#[cfg(target_os = "macos")]
pub(crate) async fn sendfile_all(
    wr: &tokio::net::tcp::OwnedWriteHalf,
    body: &SendableRead,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let sock = wr.as_ref().as_raw_fd();
    let file = body.file.as_raw_fd();
    let mut off = body.off as libc::off_t;
    let mut remaining = body.len as libc::off_t;
    while remaining > 0 {
        wr.as_ref().writable().await?;
        let mut sent: libc::off_t = remaining;
        // SAFETY: plain syscall over two owned fds; `sent` is in/out (bytes written).
        #[allow(unsafe_code)]
        let rc = unsafe { libc::sendfile(file, sock, off, &mut sent, std::ptr::null_mut(), 0) };
        if rc == -1 {
            let e = std::io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EAGAIN) | Some(libc::EINTR) => {
                    // Partial progress is reported through `sent` even on EAGAIN.
                    off += sent;
                    remaining -= sent;
                    continue;
                }
                _ => return Err(e),
            }
        }
        off += sent;
        remaining -= sent;
        if sent == 0 && rc == 0 && remaining > 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "sendfile: file shorter than promised",
            ));
        }
    }
    Ok(())
}

/// Portable fallback for non-macOS builds (compile-only; the server is macOS-resident).
/// pread into a buffer, then nonblocking `write(2)` to the socket fd.
#[cfg(not(target_os = "macos"))]
pub(crate) async fn sendfile_all(
    wr: &tokio::net::tcp::OwnedWriteHalf,
    body: &SendableRead,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    let sock = wr.as_ref().as_raw_fd();
    let mut off = body.off;
    let mut remaining = body.len as usize;
    let mut buf = vec![0u8; 256 * 1024];
    while remaining > 0 {
        let want = remaining.min(buf.len());
        let got = body.file.read_at(&mut buf[..want], off)?;
        if got == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "sendfile: file shorter than promised",
            ));
        }
        let mut written = 0;
        while written < got {
            wr.as_ref().writable().await?;
            // SAFETY: write(2) on an owned nonblocking socket fd from a valid buffer slice.
            #[allow(unsafe_code)]
            let rc = unsafe {
                libc::write(
                    sock,
                    buf[written..got].as_ptr().cast(),
                    (got - written) as libc::size_t,
                )
            };
            if rc < 0 {
                let e = std::io::Error::last_os_error();
                if matches!(e.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EINTR)) {
                    continue;
                }
                return Err(e);
            }
            written += rc as usize;
        }
        off += got as u64;
        remaining -= got;
    }
    Ok(())
}

/// Read one RPC record (record-marking: 4-byte marker, high bit = last fragment). Returns None
/// on clean EOF at a record boundary.
async fn read_record<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut record = Vec::new();
    loop {
        let mut hdr = [0u8; 4];
        match stream.read_exact(&mut hdr).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && record.is_empty() => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        let marker = u32::from_be_bytes(hdr);
        let last = marker & 0x8000_0000 != 0;
        let len = (marker & 0x7FFF_FFFF) as usize;
        if record.len() + len > MAX_RECORD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "rpc record exceeds cap",
            ));
        }
        let start = record.len();
        record.resize(start + len, 0);
        stream.read_exact(&mut record[start..]).await?;
        if last {
            return Ok(Some(record));
        }
    }
}
