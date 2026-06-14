//! Session server: listeners, Hello/auth, ctl loop, rpc loop, bulk writers, snapshot pump.

use super::shares::{self, Shares};
use super::{rpc, walker};
use crate::config::Config;
use mist_proto::{
    CtlMsg, EventMsg, FLAG_MORE, FrameKind, Lane, PROTO_VERSION, RpcReq, RpcResp, encode, features,
};
use mist_transport::{FramedStream, Stream, classify_accepted};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::{Notify, Semaphore, mpsc};

fn mono_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Messages funneled to a bulk-lane writer task.
enum BulkMsg {
    Event(EventMsg),
    Raw {
        seq: u64,
        more: bool,
        bytes: Vec<u8>,
    },
}

struct Session {
    id: u64,
    bulks: Mutex<Vec<mpsc::Sender<BulkMsg>>>,
    bulk_ready: Notify,
    rpc_sem: Arc<Semaphore>,
}

impl Session {
    async fn bulk_sender(&self, hint: u64) -> Option<mpsc::Sender<BulkMsg>> {
        for _ in 0..50 {
            {
                let b = self.bulks.lock();
                if !b.is_empty() {
                    return Some(b[(hint as usize) % b.len()].clone());
                }
            }
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                self.bulk_ready.notified(),
            )
            .await
            .ok();
        }
        None
    }
}

type Sessions = Arc<Mutex<HashMap<u64, Arc<Session>>>>;

pub async fn serve(cfg: Config) -> anyhow::Result<()> {
    let token_hash = load_or_create_token(&cfg.token_file)?;
    let vm_uuid = super::identity::load_or_create_vmid(&cfg.vmid_file)?;
    let shares = Arc::new(shares::setup(&cfg.share)?);
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let limits = Arc::new(cfg.limits.clone());

    // Start fanotify engines + journal batchers now (before any session) so the consistent cut
    // captures every change from snapshot-start onward. Engine handles must outlive the process.
    let (hub, _engines) = super::journal::start(&shares, std::process::id() as i32);

    // TCP listener ports — announced to the host (CtlMsg::Endpoints) so it can move
    // bandwidth-sensitive lanes onto virtio-net (vsock stays the zero-config ctl path).
    let tcp_ports: Arc<Vec<u16>> = Arc::new(
        cfg.listen
            .iter()
            .filter_map(|ep| ep.strip_prefix("tcp:"))
            .filter_map(|addr| addr.rsplit(':').next()?.parse().ok())
            .collect(),
    );

    // Publish the _mist._tcp advert (design 11 §2): the host then resolves host+port+vm_uuid in
    // one mDNS shot. Best-effort — discovery degrades to lease/ARP scan + authenticated probe.
    super::identity::publish_avahi(
        &cfg.avahi_service_file,
        &vm_uuid,
        tcp_ports.first().copied(),
        shares.by_id.len(),
        &guest_info().kernel,
    );

    let mut accept_tasks = Vec::new();
    for ep in &cfg.listen {
        let (shares, sessions, limits, hub) = (
            shares.clone(),
            sessions.clone(),
            limits.clone(),
            hub.clone(),
        );
        let tcp_ports = tcp_ports.clone();
        let ep = ep.clone();
        let token_hash_owned = token_hash;
        accept_tasks.push(tokio::spawn(async move {
            if let Err(e) = listen_loop(
                &ep,
                token_hash_owned,
                vm_uuid,
                shares,
                sessions,
                limits,
                hub,
                tcp_ports,
            )
            .await
            {
                tracing::error!(endpoint = %ep, error = %e, "listener failed");
            }
        }));
    }

    tracing::info!(shares = shares.by_id.len(), "mistd ready");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn listen_loop(
    ep: &str,
    token_hash: [u8; 32],
    vm_uuid: [u8; 16],
    shares: Arc<Shares>,
    sessions: Sessions,
    limits: Arc<crate::config::Limits>,
    hub: super::journal::JournalHub,
    tcp_ports: Arc<Vec<u16>>,
) -> anyhow::Result<()> {
    if let Some(port) = ep.strip_prefix("vsock:") {
        let port: u32 = port.parse()?;
        let addr = tokio_vsock::VsockAddr::new(libc::VMADDR_CID_ANY, port);
        let listener = tokio_vsock::VsockListener::bind(addr)?;
        tracing::info!(port, "listening on vsock");
        loop {
            let (stream, peer) = listener.accept().await?;
            tracing::debug!(?peer, "vsock connection");
            spawn_conn(
                Box::new(stream),
                token_hash,
                vm_uuid,
                shares.clone(),
                sessions.clone(),
                limits.clone(),
                hub.clone(),
                tcp_ports.clone(),
            );
        }
    } else if let Some(addr) = ep.strip_prefix("tcp:") {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(%addr, "listening on tcp");
        loop {
            let (stream, peer) = listener.accept().await?;
            stream.set_nodelay(true)?;
            tracing::debug!(%peer, "tcp connection");
            spawn_conn(
                Box::new(stream),
                token_hash,
                vm_uuid,
                shares.clone(),
                sessions.clone(),
                limits.clone(),
                hub.clone(),
                tcp_ports.clone(),
            );
        }
    } else {
        anyhow::bail!("unknown listen endpoint {ep:?} (want vsock:PORT or tcp:ADDR)")
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_conn(
    stream: Stream,
    token_hash: [u8; 32],
    vm_uuid: [u8; 16],
    shares: Arc<Shares>,
    sessions: Sessions,
    limits: Arc<crate::config::Limits>,
    hub: super::journal::JournalHub,
    tcp_ports: Arc<Vec<u16>>,
) {
    tokio::spawn(async move {
        match classify_accepted(stream).await {
            Ok((
                framed,
                CtlMsg::Hello {
                    proto,
                    features: host_features,
                    token_hash: th,
                    host_name,
                    ..
                },
            )) => {
                if proto != PROTO_VERSION {
                    tracing::warn!(proto, "protocol mismatch");
                    return;
                }
                if blake3::Hash::from(th) != blake3::Hash::from(token_hash) {
                    let mut framed = framed;
                    let _ = framed.send_msg(FrameKind::Ctl, 0, &CtlMsg::AuthFail).await;
                    tracing::warn!(%host_name, "auth failure");
                    return;
                }
                ctl_session(
                    framed,
                    host_features,
                    vm_uuid,
                    shares,
                    sessions,
                    limits,
                    tcp_ports,
                )
                .await;
            }
            Ok((
                framed,
                CtlMsg::StreamHello {
                    session_id,
                    lane,
                    idx,
                },
            )) => {
                let session = sessions.lock().get(&session_id).cloned();
                match session {
                    Some(s) => attach_lane(framed, s, shares, lane, idx, hub).await,
                    None => tracing::warn!(session_id, "StreamHello for unknown session"),
                }
            }
            Ok((_, other)) => {
                tracing::warn!(?other, "unexpected first message");
            }
            Err(e) => tracing::debug!(error = %e, "connection setup failed"),
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn ctl_session(
    mut framed: FramedStream,
    host_features: u64,
    vm_uuid: [u8; 16],
    shares: Arc<Shares>,
    sessions: Sessions,
    limits: Arc<crate::config::Limits>,
    tcp_ports: Arc<Vec<u16>>,
) {
    let session = Arc::new(Session {
        id: rand::random(),
        bulks: Mutex::new(Vec::new()),
        bulk_ready: Notify::new(),
        rpc_sem: Arc::new(Semaphore::new(limits.inflight_rpc)),
    });
    sessions.lock().insert(session.id, session.clone());

    let ack = CtlMsg::HelloAck {
        proto: PROTO_VERSION,
        features: features::SUPPORTED,
        boot_id: shares.boot_id,
        session_id: session.id,
        shares: shares.infos(),
        guest: guest_info(),
    };
    if framed.send_msg(FrameKind::Ctl, 0, &ack).await.is_err() {
        sessions.lock().remove(&session.id);
        return;
    }
    // Stable identity (design 11 §6): when both peers set VM_IDENTITY, send vm_uuid right after
    // HelloAck and *before* Endpoints (the host's post-hello reader expects that order). An old
    // host never sets the bit, so an old peer never receives this frame.
    if (host_features & features::SUPPORTED & features::VM_IDENTITY) != 0
        && framed
            .send_msg(FrameKind::Ctl, 0, &CtlMsg::VmIdentity { vm_uuid })
            .await
            .is_err()
    {
        sessions.lock().remove(&session.id);
        return;
    }
    // Announce dialable TCP endpoints (possibly empty) so the host can place lanes on
    // virtio-net for throughput. Sent unconditionally right after HelloAck.
    let tcp: Vec<String> = tcp_ports
        .iter()
        .flat_map(|p| {
            local_ipv4_addrs()
                .into_iter()
                .map(move |ip| format!("{ip}:{p}"))
        })
        .take(8)
        .collect();
    if framed
        .send_msg(FrameKind::Ctl, 0, &CtlMsg::Endpoints { tcp })
        .await
        .is_err()
    {
        sessions.lock().remove(&session.id);
        return;
    }
    tracing::info!(session = session.id, "session established");

    loop {
        let frame = match framed.recv().await {
            Ok(f) => f,
            Err(_) => break,
        };
        if frame.kind != FrameKind::Ctl {
            tracing::warn!("non-ctl frame on ctl lane");
            break;
        }
        let msg = match mist_proto::decode::<CtlMsg>(&frame.payload) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "ctl decode failed; closing session");
                break;
            }
        };
        match msg {
            CtlMsg::Ping { nonce } => {
                let pong = CtlMsg::Pong {
                    nonce,
                    guest_mono_ns: mono_ns(),
                };
                if framed
                    .send_msg(FrameKind::Ctl, frame.seq, &pong)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            CtlMsg::AttachShare { share } => {
                if shares.get(share).is_none() {
                    tracing::warn!(?share, "attach for unknown share");
                }
                // Attach is a validation no-op until a share is announced.
            }
            CtlMsg::SnapshotStart {
                share: share_id,
                snap_id,
            } => {
                let Some(share) = shares.get(share_id) else {
                    tracing::warn!(?share_id, "snapshot for unknown share");
                    continue;
                };
                let Some(bulk) = session.bulk_sender(0).await else {
                    tracing::warn!("snapshot requested but no bulk lane attached");
                    continue;
                };
                let limits = limits.clone();
                tokio::spawn(async move {
                    let (tx, mut rx) = mpsc::channel::<mist_proto::SnapDir>(256);
                    let walk = tokio::spawn(walker::walk(
                        share,
                        snap_id,
                        limits.walker_parallelism,
                        limits.snap_entries_per_record,
                        tx,
                    ));
                    while let Some(rec) = rx.recv().await {
                        if bulk
                            .send(BulkMsg::Event(EventMsg::SnapDir(rec)))
                            .await
                            .is_err()
                        {
                            walk.abort();
                            return;
                        }
                    }
                    if let Ok(done) = walk.await {
                        tracing::info!(
                            snap_id,
                            dirs = done.dirs,
                            entries = done.entries,
                            errors = done.errors,
                            "snapshot complete"
                        );
                        let _ = bulk.send(BulkMsg::Event(EventMsg::SnapDone(done))).await;
                    }
                });
            }
            CtlMsg::SnapshotAbort { .. } | CtlMsg::DetachShare { .. } => {}
            CtlMsg::Goodbye { reason } => {
                tracing::info!(%reason, "peer said goodbye");
                break;
            }
            other => tracing::debug!(?other, "ignored ctl message"),
        }
    }
    sessions.lock().remove(&session.id);
    tracing::info!(session = session.id, "session closed");
}

async fn attach_lane(
    framed: FramedStream,
    session: Arc<Session>,
    shares: Arc<Shares>,
    lane: Lane,
    idx: u8,
    hub: super::journal::JournalHub,
) {
    match lane {
        Lane::Bulk => bulk_writer(framed, session, idx).await,
        Lane::Rpc => rpc_loop(framed, session, shares).await,
        Lane::Journal => journal_writer(framed, hub).await,
        Lane::Ctl => tracing::warn!("StreamHello must not declare a ctl lane"),
    }
}

/// The journal lane is guest→host: subscribe to the hub and stream `JournalBatch`es as Event
/// frames. A broadcast lag (host fell behind) is surfaced as a single `Overflow` batch, which
/// drives the host to resync — the designed safe response to a gap.
async fn journal_writer(mut framed: FramedStream, hub: super::journal::JournalHub) {
    use mist_proto::{EventMsg, JournalBatch, Rec};
    let mut sub = hub.subscribe();
    loop {
        match sub.recv().await {
            Ok(batch) => {
                let ev = EventMsg::Journal(batch);
                if framed
                    .send_frame(FrameKind::Event, 0, 0, &encode(&ev))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(dropped = n, "journal broadcast lagged; signalling overflow");
                let ov = EventMsg::Journal(JournalBatch {
                    share: mist_proto::ShareId(u16::MAX), // all-shares overflow sentinel
                    first_seq: 0,
                    guest_mono_ns: 0,
                    records: vec![Rec::Overflow],
                });
                if framed
                    .send_frame(FrameKind::Event, 0, 0, &encode(&ov))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

async fn bulk_writer(mut framed: FramedStream, session: Arc<Session>, idx: u8) {
    let (tx, mut rx) = mpsc::channel::<BulkMsg>(64);
    {
        let mut b = session.bulks.lock();
        let i = idx as usize;
        if b.len() <= i {
            b.resize_with(i + 1, || tx.clone());
        }
        b[i] = tx;
    }
    session.bulk_ready.notify_waiters();
    tracing::debug!(session = session.id, idx, "bulk lane up");

    while let Some(msg) = rx.recv().await {
        let r = match msg {
            BulkMsg::Event(ev) => {
                framed
                    .send_frame(FrameKind::Event, 0, 0, &encode(&ev))
                    .await
            }
            BulkMsg::Raw { seq, more, bytes } => {
                framed
                    .send_frame(
                        FrameKind::Bulk,
                        if more { FLAG_MORE } else { 0 },
                        seq,
                        &bytes,
                    )
                    .await
            }
        };
        if r.is_err() {
            break;
        }
    }
}

async fn rpc_loop(framed: FramedStream, session: Arc<Session>, shares: Arc<Shares>) {
    // recv()/send_msg() are not cancel-safe: requests and responses flow concurrently on this
    // lane, so each direction gets its own task instead of a shared select! (which desyncs
    // framing the moment it cancels a half-transferred frame).
    let (mut reader, mut writer) = framed.into_split();
    let (resp_tx, mut resp_rx) = mpsc::channel::<(u64, RpcResp)>(128);
    let writer_task = tokio::spawn(async move {
        while let Some((seq, resp)) = resp_rx.recv().await {
            if writer.send_msg(FrameKind::Resp, seq, &resp).await.is_err() {
                break;
            }
        }
    });
    loop {
        let frame = match reader.recv().await {
            Ok(f) => f,
            Err(_) => break,
        };
        if frame.kind != FrameKind::Req {
            tracing::warn!("non-req frame on rpc lane");
            continue;
        }
        let req = match mist_proto::decode::<RpcReq>(&frame.payload) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "rpc decode failed; closing lane");
                break;
            }
        };
        let permit = match session.rpc_sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };
        dispatch(
            req,
            frame.seq,
            resp_tx.clone(),
            session.clone(),
            shares.clone(),
            permit,
        );
    }
    drop(resp_tx);
    // In-flight dispatch tasks may still hold resp_tx clones; the writer drains them and
    // exits once the last clone drops.
    let _ = writer_task.await;
}

fn dispatch(
    req: RpcReq,
    seq: u64,
    resp_tx: mpsc::Sender<(u64, RpcResp)>,
    session: Arc<Session>,
    shares: Arc<Shares>,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        let resp = match req {
            RpcReq::Stat { share, node } => match shares.get(share) {
                Some(s) => tokio::task::spawn_blocking(move || rpc::stat(&s, node))
                    .await
                    .unwrap_or_else(|_| io_err("stat task")),
                None => no_share(),
            },
            RpcReq::StatBatch { share, nodes } => match shares.get(share) {
                Some(s) => tokio::task::spawn_blocking(move || rpc::stat_batch(&s, &nodes))
                    .await
                    .unwrap_or_else(|_| io_err("statbatch task")),
                None => no_share(),
            },
            RpcReq::Lookup { share, dir, name } => match shares.get(share) {
                Some(s) => {
                    tokio::task::spawn_blocking(move || rpc::lookup(&s, dir, name.as_bytes()))
                        .await
                        .unwrap_or_else(|_| io_err("lookup task"))
                }
                None => no_share(),
            },
            RpcReq::Read {
                share,
                node,
                off,
                len,
                ..
            } => {
                let Some(s) = shares.get(share) else {
                    let _ = resp_tx.send((seq, no_share())).await;
                    return;
                };
                let plan = tokio::task::spawn_blocking({
                    let s = s.clone();
                    move || rpc::read_plan(&s, node, off, len)
                })
                .await
                .unwrap_or_else(|_| rpc::ReadPlan {
                    header: io_err("read task"),
                    stream: None,
                });
                let is_start = matches!(plan.header, RpcResp::ReadStart { .. });
                let _ = resp_tx.send((seq, plan.header)).await;
                if let (true, Some((fd, off, n))) = (is_start, plan.stream) {
                    stream_read(session, seq, fd, off, n).await;
                }
                return;
            }

            // ---- mutations ---------------------------------------------------------------
            RpcReq::Create {
                share,
                dir,
                name,
                kind,
                mode,
                rdev,
                symlink_target,
                exclusive,
            } => {
                run_mut(&shares, share, move |s| {
                    super::applier::create(
                        &s,
                        dir,
                        name.as_bytes(),
                        kind,
                        mode,
                        rdev,
                        symlink_target.as_deref(),
                        exclusive,
                    )
                })
                .await
            }
            RpcReq::Unlink { share, dir, name } => {
                run_mut(&shares, share, move |s| {
                    super::applier::unlink(&s, dir, name.as_bytes(), false)
                })
                .await
            }
            RpcReq::Rmdir { share, dir, name } => {
                run_mut(&shares, share, move |s| {
                    super::applier::unlink(&s, dir, name.as_bytes(), true)
                })
                .await
            }
            RpcReq::Rename {
                share,
                from_dir,
                from_name,
                to_dir,
                to_name,
            } => {
                run_mut(&shares, share, move |s| {
                    super::applier::rename(
                        &s,
                        from_dir,
                        from_name.as_bytes(),
                        to_dir,
                        to_name.as_bytes(),
                    )
                })
                .await
            }
            RpcReq::SetAttr {
                share,
                node,
                mode,
                uid,
                gid,
                size,
                mtime,
            } => {
                run_mut(&shares, share, move |s| {
                    super::applier::setattr(&s, node, mode, uid, gid, size, mtime)
                })
                .await
            }
            RpcReq::Write {
                share,
                node,
                off,
                sync,
                data,
            } => {
                run_mut(&shares, share, move |s| {
                    super::applier::write(&s, node, off, sync, &data)
                })
                .await
            }
            RpcReq::Commit { share, node } => {
                run_mut(&shares, share, move |s| super::applier::commit(&s, node)).await
            }
            RpcReq::CommitRange {
                share,
                node,
                off,
                len,
            } => {
                run_mut(&shares, share, move |s| {
                    super::applier::commit_range(&s, node, off, len)
                })
                .await
            }
        };
        let _ = resp_tx.send((seq, resp)).await;
    });
}

/// Run a mutation on the blocking pool against the named share.
async fn run_mut<F>(shares: &Arc<Shares>, share: mist_proto::ShareId, f: F) -> RpcResp
where
    F: FnOnce(Arc<super::shares::Share>) -> RpcResp + Send + 'static,
{
    match shares.get(share) {
        Some(s) => tokio::task::spawn_blocking(move || f(s))
            .await
            .unwrap_or_else(|_| io_err("mutation task")),
        None => no_share(),
    }
}

async fn stream_read(session: Arc<Session>, seq: u64, fd: std::os::fd::OwnedFd, off: u64, n: u64) {
    let Some(bulk) = session.bulk_sender(seq).await else {
        return;
    };
    let fd = Arc::new(fd);
    let mut sent = 0u64;
    while sent < n {
        let want = ((n - sent) as usize).min(rpc::READ_CHUNK);
        let fd2 = fd.clone();
        let at = off + sent;
        let chunk =
            match tokio::task::spawn_blocking(move || rpc::pread_chunk(&fd2, at, want)).await {
                Ok(Ok(c)) => c,
                _ => break,
            };
        if chunk.is_empty() {
            break; // file shrank underneath us; host sees a short stream and re-stats
        }
        sent += chunk.len() as u64;
        let more = sent < n;
        if bulk
            .send(BulkMsg::Raw {
                seq,
                more,
                bytes: chunk,
            })
            .await
            .is_err()
        {
            return;
        }
    }
    if sent < n {
        // Terminate the MORE chain so the host doesn't hang on a short stream.
        let _ = bulk
            .send(BulkMsg::Raw {
                seq,
                more: false,
                bytes: Vec::new(),
            })
            .await;
    }
}

fn no_share() -> RpcResp {
    RpcResp::Err(mist_proto::RpcErr {
        errno: libc::ENOENT,
        msg: "unknown share".into(),
    })
}

fn io_err(what: &str) -> RpcResp {
    RpcResp::Err(mist_proto::RpcErr {
        errno: libc::EIO,
        msg: what.into(),
    })
}

fn guest_info() -> mist_proto::GuestInfo {
    let kernel = rustix::system::uname()
        .release()
        .to_string_lossy()
        .into_owned();
    let fanotify_max_queued = std::fs::read_to_string("/proc/sys/fs/fanotify/max_queued_events")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    mist_proto::GuestInfo {
        kernel,
        fanotify_max_queued,
        mistd_pid: std::process::id(),
    }
}

fn load_or_create_token(path: &std::path::Path) -> anyhow::Result<[u8; 32]> {
    use std::io::Write;
    let bytes = match std::fs::read(path) {
        Ok(b) if b.len() >= 32 => b,
        Ok(b) => anyhow::bail!(
            "token {} is {} bytes; needs at least 32 random bytes",
            path.display(),
            b.len()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let tok: [u8; 32] = rand::random();
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(path)?;
            f.write_all(&tok)?;
            tracing::info!(path = %path.display(), "generated new token");
            tok.to_vec()
        }
        Err(e) => return Err(anyhow::anyhow!("reading token {}: {e}", path.display())),
    };
    Ok(*blake3::hash(&bytes).as_bytes())
}

use std::os::unix::fs::OpenOptionsExt;

/// Non-loopback IPv4 addresses of up interfaces (for `CtlMsg::Endpoints`).
#[allow(unsafe_code)]
fn local_ipv4_addrs() -> Vec<std::net::Ipv4Addr> {
    let mut out = Vec::new();
    let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: standard getifaddrs/freeifaddrs pattern; pointers only read while owned.
    unsafe {
        if libc::getifaddrs(&mut ifap) != 0 {
            return out;
        }
        let mut cur = ifap;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null()
                && i32::from((*ifa.ifa_addr).sa_family) == libc::AF_INET
                && ifa.ifa_flags & (libc::IFF_LOOPBACK as u32) == 0
                && ifa.ifa_flags & (libc::IFF_UP as u32) != 0
            {
                let sin = &*(ifa.ifa_addr as *const libc::sockaddr_in);
                out.push(std::net::Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr)));
            }
            cur = ifa.ifa_next;
        }
        libc::freeifaddrs(ifap);
    }
    out
}
