//! VM session: dial lanes, hello, seed replicas from the snapshot stream, serve reads.
//!
//! Replicas are swapped atomically per share so readers (control API and NFS surfaces) never
//! observe a half-built tree during seed/reseed.

use crate::config::VmConfig;
use mist_proto::{
    CtlMsg, EventMsg, FLAG_MORE, FrameKind, Lane, NodeKey, PROTO_VERSION, Rec, RpcReq, RpcResp,
    ShareId, ShareInfo, features,
};
use mist_replica::{ShareReplica, ShareState};
use mist_transport::{Endpoint, FramedStream, TransportError, dial_lane};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot, watch};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(1);
const PING_MISSES: u32 = 3;
/// Sentinel "expected journal seq": adopt the next live batch's seq as the baseline (set when a
/// seed buffered no journal batches, so the guest's running seq is unknown).
const SEQ_ADOPT: u64 = u64::MAX;

/// Dial a secondary lane over a guest-announced TCP endpoint when one answers quickly, falling
/// back to the configured (vsock/bridge) endpoint. A dead/filtered TCP addr costs ≤2 s once.
async fn dial_lane_pref(
    tcp_eps: &[String],
    fallback: &Endpoint,
    session_id: u64,
    lane: Lane,
    idx: u8,
) -> Result<FramedStream, TransportError> {
    for addr in tcp_eps {
        let ep = Endpoint::Tcp(addr.clone());
        match tokio::time::timeout(
            Duration::from_secs(2),
            dial_lane(&ep, session_id, lane, idx),
        )
        .await
        {
            Ok(Ok(f)) => {
                tracing::info!(%addr, ?lane, idx, "lane attached over tcp");
                return Ok(f);
            }
            Ok(Err(e)) => tracing::debug!(%addr, ?lane, error = %e, "tcp lane dial failed"),
            Err(_) => tracing::debug!(%addr, ?lane, "tcp lane dial timed out"),
        }
    }
    dial_lane(fallback, session_id, lane, idx).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmState {
    Disconnected,
    Connecting,
    Ready,
    Degraded,
}

#[derive(Debug)]
pub struct ShareHandle {
    pub info: RwLock<ShareInfo>,
    replica: RwLock<Arc<ShareReplica>>,
    pub seeded: Notify,
}

impl ShareHandle {
    pub fn replica(&self) -> Arc<ShareReplica> {
        self.replica.read().clone()
    }

    fn swap_replica(&self, r: Arc<ShareReplica>) {
        *self.replica.write() = r;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_short_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, b"short").unwrap();
        let cfg = VmConfig {
            bridge: "tcp:127.0.0.1:1".into(),
            token,
            vm_uuid: None,
            autoattach: false,
            automount: false,
        };

        let err = VmHandle::new("dev".into(), &cfg, None).unwrap_err();
        assert!(err.to_string().contains("needs at least 32 random bytes"));
    }
}

type FetchGuards = HashMap<(mist_cas::ManifestKey, u64), Arc<tokio::sync::Mutex<()>>>;

pub struct VmHandle {
    pub name: String,
    /// Last endpoint used/resolved (for status). For `bridge="auto"` this is updated on every
    /// connect by the resolver; for a fixed bridge it is the parsed config endpoint.
    endpoint: RwLock<Endpoint>,
    /// `true` when located by the resolver (`bridge="auto"`) rather than a pinned endpoint.
    auto: bool,
    /// Auto-mount this VM's shares at `~/Mist/<vm>/<share>` whenever it's connected (design 11 §7).
    pub automount: bool,
    /// Identity bound at pairing (design 11 §6); verified against the guest's `VmIdentity`.
    expected_uuid: Option<[u8; 16]>,
    resolver: crate::resolve::Resolver,
    token_hash: [u8; 32],
    pub state: watch::Sender<VmState>,
    /// Shares by name, populated at first HelloAck and kept across reconnects.
    pub shares: RwLock<HashMap<String, Arc<ShareHandle>>>,
    rpc: RwLock<Option<Arc<RpcClient>>>,
    /// Second rpc lane dedicated to bulk Write chunks: streaming writes stop queueing behind
    /// (and being queued behind) latency-sensitive mutations, and ingest bandwidth doubles.
    rpc_bulk: RwLock<Option<Arc<RpcClient>>>,
    pub last_seed_rate: AtomicU64, // entries/sec of the last completed seed (for status/bench)
    pub journal_records: AtomicU64, // total journal records applied (metrics)
    pub overflows: AtomicU64,      // resyncs triggered by overflow/gap (metrics)
    /// Live change feed (JSON lines) consumed by `mist events --follow`.
    pub events: tokio::sync::broadcast::Sender<String>,
    /// Guest-side invalidations (share, node) — drives NFSv4.1 delegation recalls (design 05 §5).
    pub inval: tokio::sync::broadcast::Sender<(u16, NodeKey)>,
    /// Nodes recently written FROM THE MAC (design 02 §6.5 echo fences): their journal echoes
    /// are absorbed — attrs clamped monotonic, no inval — so the client's own writes never
    /// look like a foreign change (mtime flapping made the macOS client fall into
    /// read-modify-write + cache invalidation storms) and never recall its own delegations.
    pub mac_dirty: Mutex<HashMap<(u16, NodeKey), std::time::Instant>>,
    /// Guest-vs-Mac collision log, surfaced by `mist conflicts`.
    pub conflicts: Arc<crate::conflicts::ConflictTracker>,
    /// Content cache. `None` = disabled (config or open failure); reads fall back to RPC.
    pub cas: Option<Arc<mist_cas::CasStore>>,
    /// Single-flight guards for cold-chunk fetches: (manifest key, chunk off) → mutex. Entries
    /// are pruned opportunistically once the map grows past a soft cap.
    fetch_guards: Mutex<FetchGuards>,
    /// Debounce for the delayed durable CAS persist after volatile stash ingests.
    pub cas_persist_pending: Arc<std::sync::atomic::AtomicBool>,
    /// Reads served by guest RPC (cache misses + fallback); the warm-cache gate wants 0.
    pub guest_read_ops: AtomicU64,
    pub guest_read_bytes: AtomicU64,
    /// Abort handle for this VM's `supervise()` loop, so re-adding the same name can stop the old
    /// (otherwise-immortal) reconnect task instead of orphaning it (review finding).
    supervise_abort: Mutex<Option<tokio::task::AbortHandle>>,
}

impl std::fmt::Debug for VmHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VmHandle")
            .field("name", &self.name)
            .finish()
    }
}

impl VmHandle {
    pub fn new(
        name: String,
        cfg: &VmConfig,
        cas_cfg: Option<mist_cas::CasConfig>,
    ) -> anyhow::Result<Arc<Self>> {
        let auto = cfg.is_auto();
        // For `auto`, the endpoint is resolved per-connect; seed a placeholder for display. For a
        // fixed bridge, parse it once now so a bad endpoint string fails fast at startup.
        let endpoint = if auto {
            Endpoint::Tcp("auto".into())
        } else {
            Endpoint::parse(&cfg.bridge)?
        };
        let token_path = cfg.resolved_token();
        let token = std::fs::read(&token_path)
            .map_err(|e| anyhow::anyhow!("reading token {}: {e}", token_path.display()))?;
        anyhow::ensure!(
            token.len() >= 32,
            "token {} is {} bytes; needs at least 32 random bytes",
            cfg.token.display(),
            token.len()
        );
        // A broken cache must never block the mount: log and run uncached.
        let cas = cas_cfg.and_then(|c| match mist_cas::CasStore::open(c) {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::warn!(vm = %name, error = %e, "content cache disabled (open failed)");
                None
            }
        });
        Ok(Arc::new(VmHandle {
            name,
            endpoint: RwLock::new(endpoint),
            auto,
            automount: cfg.automount,
            expected_uuid: cfg.expected_uuid(),
            resolver: crate::resolve::Resolver::new(),
            token_hash: *blake3::hash(&token).as_bytes(),
            state: watch::Sender::new(VmState::Disconnected),
            shares: RwLock::new(HashMap::new()),
            rpc: RwLock::new(None),
            rpc_bulk: RwLock::new(None),
            last_seed_rate: AtomicU64::new(0),
            journal_records: AtomicU64::new(0),
            overflows: AtomicU64::new(0),
            events: tokio::sync::broadcast::channel(1024).0,
            inval: tokio::sync::broadcast::channel(4096).0,
            conflicts: Arc::new(crate::conflicts::ConflictTracker::default()),
            cas,
            mac_dirty: Mutex::new(HashMap::new()),
            fetch_guards: Mutex::new(HashMap::new()),
            cas_persist_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            guest_read_ops: AtomicU64::new(0),
            guest_read_bytes: AtomicU64::new(0),
            supervise_abort: Mutex::new(None),
        }))
    }

    /// Spawn the connect→serve→reconnect loop and record its abort handle. Use this (not a bare
    /// `tokio::spawn(vm.supervise())`) so the task can be stopped on re-add / shutdown.
    pub fn spawn_supervise(self: &Arc<Self>) {
        let handle = tokio::spawn(self.clone().supervise());
        *self.supervise_abort.lock() = Some(handle.abort_handle());
    }

    /// Stop this VM's supervise loop (the loop has no other exit path). Used when a VM with the same
    /// name is re-added, to avoid two tasks racing on the same guest.
    pub fn stop_supervise(&self) {
        if let Some(h) = self.supervise_abort.lock().take() {
            h.abort();
        }
    }

    pub fn rpc(&self) -> Option<Arc<RpcClient>> {
        self.rpc.read().clone()
    }

    /// The bound `vm_uuid` (hex) for status output, if this VM is identity-bound.
    pub fn vm_uuid_hex(&self) -> Option<String> {
        self.expected_uuid.map(|u| mist_proto::vm_uuid_hex(&u))
    }

    /// Human-readable endpoint for status output. `auto` VMs show the resolved address once
    /// connected (or `auto (resolving)` before the first successful resolve).
    pub fn endpoint_display(&self) -> String {
        let ep = self.endpoint.read();
        match (self.auto, &*ep) {
            (true, Endpoint::Tcp(a)) if a == "auto" => "auto (resolving)".into(),
            (true, ep) => format!("auto → {ep}"),
            (false, ep) => ep.to_string(),
        }
    }

    /// The bulk-write rpc lane (falls back to the main lane before it attaches).
    pub fn rpc_write(&self) -> Option<Arc<RpcClient>> {
        self.rpc_bulk.read().clone().or_else(|| self.rpc())
    }

    /// Echo-fence window: journal records arriving within this of a Mac write are echoes.
    const MAC_DIRTY_WINDOW: Duration = Duration::from_secs(5);

    /// Mark a node as Mac-written (its upcoming journal echoes get absorbed).
    pub fn note_mac_dirty(&self, share: u16, node: NodeKey) {
        let mut m = self.mac_dirty.lock();
        if m.len() > 4096 {
            let now = std::time::Instant::now();
            m.retain(|_, t| now.duration_since(*t) < Self::MAC_DIRTY_WINDOW);
        }
        m.insert((share, node), std::time::Instant::now());
    }

    pub fn is_mac_dirty(&self, share: u16, node: NodeKey) -> bool {
        self.mac_dirty
            .lock()
            .get(&(share, node))
            .is_some_and(|t| t.elapsed() < Self::MAC_DIRTY_WINDOW)
    }

    /// Single-flight mutex for one cold-chunk fetch. Lock it to fetch; concurrent readers of
    /// the same chunk queue behind it and find the chunk ingested when they re-check the CAS.
    pub async fn fetch_guard(
        &self,
        key: mist_cas::ManifestKey,
        chunk_off: u64,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut g = self.fetch_guards.lock();
        if g.len() > 1024 {
            // Stale guards (no current waiters) are safe to drop: a future fetch re-creates one.
            g.retain(|_, m| Arc::strong_count(m) > 1);
        }
        g.entry((key, chunk_off))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Run the connect→serve→reconnect loop forever.
    pub async fn supervise(self: Arc<Self>) {
        let mut backoff = Duration::from_millis(100);
        loop {
            self.state.send_replace(VmState::Connecting);
            match self.clone().run_session().await {
                Ok(()) => backoff = Duration::from_millis(100),
                Err(e) => {
                    tracing::warn!(vm = %self.name, error = %e, "session ended");
                }
            }
            self.state.send_replace(VmState::Degraded);
            *self.rpc.write() = None;
            *self.rpc_bulk.write() = None;
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }

    /// Cache of the guest's last-announced TCP endpoints. Lets a restarted hostd reach the
    /// guest over virtio-net even when the AVF vsock device is wedged (vsock "half-up" — a
    /// connect that never completes — survives until a guest reboot, but TCP keeps working).
    fn endpoints_cache_path(&self) -> std::path::PathBuf {
        crate::config::state_dir().join(format!("endpoints-{}.txt", self.name))
    }

    fn cached_endpoints(&self) -> Vec<String> {
        std::fs::read_to_string(self.endpoints_cache_path())
            .map(|s| {
                s.lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn run_session(self: Arc<Self>) -> Result<(), SessionError> {
        // 0. resolve the endpoint. `bridge="auto"` runs the resolver chain (mDNS → lease/ARP scan
        // → authenticated probe), keyed by vm_uuid; a fixed bridge uses the parsed endpoint. The
        // resolved address is never stored in config — DHCP drift is a non-event (design 11 §2).
        let endpoint = if self.auto {
            let ep = self
                .resolver
                .resolve(&self.name, self.expected_uuid, self.token_hash)
                .await
                .map_err(|e| SessionError::Other(e.to_string()))?;
            *self.endpoint.write() = ep.clone();
            ep
        } else {
            self.endpoint.read().clone()
        };

        // 1. ctl lane + Hello. Prefer cached TCP endpoints (fast, immune to vsock wedge),
        // fall back to the bridge. A stale cache entry (IP reuse) is rejected by the token
        // check guest-side and by per-endpoint timeouts here.
        let mut ctl = {
            let mut ctl = None;
            for ep in self.cached_endpoints() {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    mist_transport::dial(&Endpoint::Tcp(ep.clone())),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        tracing::info!(vm = %self.name, addr = %ep, "ctl lane over cached tcp");
                        ctl = Some(FramedStream::new(stream, false));
                        break;
                    }
                    _ => continue,
                }
            }
            match ctl {
                Some(c) => c,
                None => dial_lane(&endpoint, 0, Lane::Ctl, 0).await?,
            }
        };
        ctl.send_msg(
            FrameKind::Ctl,
            0,
            &CtlMsg::Hello {
                proto: PROTO_VERSION,
                features: features::SUPPORTED,
                token_hash: self.token_hash,
                host_name: hostname(),
                host_version: env!("CARGO_PKG_VERSION").into(),
            },
        )
        .await?;
        let (_, ack): (u64, CtlMsg) = ctl.recv_msg(FrameKind::Ctl).await?;
        let (session_id, share_infos) = match ack {
            CtlMsg::HelloAck {
                proto,
                session_id,
                shares,
                guest,
                ..
            } => {
                if proto != PROTO_VERSION {
                    return Err(SessionError::Proto("version mismatch"));
                }
                tracing::info!(vm = %self.name, kernel = %guest.kernel, session_id, "hello ok");
                (session_id, shares)
            }
            CtlMsg::AuthFail => return Err(SessionError::Auth),
            _ => return Err(SessionError::Proto("expected HelloAck")),
        };

        // 1b. After HelloAck the guest sends, in order: VmIdentity (only when VM_IDENTITY was
        // negotiated, design 11 §6) then Endpoints. Read a couple of frames to capture both.
        // TCP/virtio-net measures ~10× vsock throughput on AVF, so bandwidth-sensitive lanes
        // prefer the announced endpoints; vsock stays the zero-config ctl path and per-lane fallback.
        let mut tcp_eps: Vec<String> = Vec::new();
        let mut got_uuid: Option<[u8; 16]> = None;
        for _ in 0..2 {
            match tokio::time::timeout(
                Duration::from_secs(3),
                ctl.recv_msg::<CtlMsg>(FrameKind::Ctl),
            )
            .await
            {
                Ok(Ok((_, CtlMsg::VmIdentity { vm_uuid }))) => got_uuid = Some(vm_uuid),
                Ok(Ok((_, CtlMsg::Endpoints { tcp }))) => {
                    tcp_eps = tcp;
                    break; // Endpoints is the last of the post-hello pair
                }
                Ok(Ok(_)) | Ok(Err(_)) | Err(_) => break,
            }
        }
        // Identity gate (design 11 §6): a paired auto VM must answer with the bound vm_uuid. A
        // mismatch means a cloned/reused token landed on the wrong guest — fail the session rather
        // than silently bind it. An absent VmIdentity on a non-identity-bound VM is fine (legacy).
        if let Some(want) = self.expected_uuid {
            match got_uuid {
                Some(got) if got == want => {}
                Some(_) => return Err(SessionError::IdentityMismatch),
                None => tracing::warn!(
                    vm = %self.name,
                    "guest sent no VmIdentity though one is configured; proceeding unbound"
                ),
            }
        }
        // Persist for the next hostd run; when the announcement raced DHCP and came up empty,
        // reuse the cache so lanes still land on TCP (stale entries fall back per-lane).
        let tcp_eps = if tcp_eps.is_empty() {
            self.cached_endpoints()
        } else {
            let body = tcp_eps.join("\n");
            let path = self.endpoints_cache_path();
            let tmp = path.with_extension("tmp");
            if std::fs::write(&tmp, &body)
                .and_then(|_| std::fs::rename(&tmp, &path))
                .is_err()
            {
                tracing::debug!(vm = %self.name, "endpoints cache write failed");
            }
            tcp_eps
        };

        // 2. secondary lanes (TCP-preferred, vsock fallback).
        let journal = dial_lane_pref(&tcp_eps, &endpoint, session_id, Lane::Journal, 0).await?;
        let rpc_stream = dial_lane_pref(&tcp_eps, &endpoint, session_id, Lane::Rpc, 0).await?;
        let rpc_bulk_stream = dial_lane_pref(&tcp_eps, &endpoint, session_id, Lane::Rpc, 1).await?;
        let bulk0 = dial_lane_pref(&tcp_eps, &endpoint, session_id, Lane::Bulk, 0).await?;
        let bulk1 = dial_lane_pref(&tcp_eps, &endpoint, session_id, Lane::Bulk, 1).await?;

        // 3. share handles (created once; replicas swapped per seed).
        {
            let mut shares = self.shares.write();
            for info in &share_infos {
                shares
                    .entry(info.name.clone())
                    .or_insert_with(|| {
                        Arc::new(ShareHandle {
                            info: RwLock::new(info.clone()),
                            replica: RwLock::new(Arc::new(ShareReplica::new(info.clone()))),
                            seeded: Notify::new(),
                        })
                    })
                    .info
                    .write()
                    .clone_from(info);
            }
        }

        // 4. wire tasks.
        let pending = Arc::new(Pending::default());
        let (rpc, mut rpc_dead) = RpcClient::start_with_death(rpc_stream, pending.clone());
        let rpc = Arc::new(rpc);
        *self.rpc.write() = Some(rpc.clone());
        let pending_bulk = Arc::new(Pending::default());
        let (rpc_bulk, mut rpc_bulk_dead) =
            RpcClient::start_with_death(rpc_bulk_stream, pending_bulk);
        *self.rpc_bulk.write() = Some(Arc::new(rpc_bulk));

        // A dead rpc lane must end the session (→ supervise reconnects with full lanes).
        let lane_watch = async move {
            tokio::select! {
                _ = rpc_dead.changed() => {}
                _ = rpc_bulk_dead.changed() => {}
            }
            Err(SessionError::Proto("rpc lane died"))
        };

        let (ctl_tx, ctl_rx) = mpsc::channel::<CtlMsg>(16);
        let assembler = Assembler::new(self.clone(), share_infos.clone(), ctl_tx.clone());
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(bulk_reader(bulk0, assembler.clone(), pending.clone()));
        tasks.spawn(bulk_reader(bulk1, assembler.clone(), pending.clone()));
        tasks.spawn(journal_reader(journal, assembler.clone()));
        tasks.spawn(ctl_driver(ctl, ctl_rx));
        tasks.spawn(lane_watch);

        // 5. seed every share.
        for info in &share_infos {
            ctl_tx
                .send(CtlMsg::AttachShare { share: info.id })
                .await
                .map_err(closed)?;
            let snap_id: u64 = rand::random();
            assembler.begin(info.id, snap_id);
            ctl_tx
                .send(CtlMsg::SnapshotStart {
                    share: info.id,
                    snap_id,
                })
                .await
                .map_err(closed)?;
        }

        self.state.send_replace(VmState::Ready);

        // 6. run until any wire task dies.
        let res = tasks.join_next().await;
        tasks.abort_all();
        match res {
            Some(Ok(Err(e))) => Err(e),
            Some(Err(e)) => Err(SessionError::Other(format!("task panic: {e}"))),
            _ => Err(SessionError::Proto("wire task exited")),
        }
    }

    pub fn share_by_id(&self, id: ShareId) -> Option<Arc<ShareHandle>> {
        self.shares
            .read()
            .values()
            .find(|s| s.info.read().id == id)
            .cloned()
    }

    pub fn share(&self, name: &str) -> Option<Arc<ShareHandle>> {
        self.shares.read().get(name).cloned()
    }

    /// Names of all announced shares (for error messages and doctor output).
    pub fn share_names(&self) -> Vec<String> {
        self.shares.read().keys().cloned().collect()
    }
}

fn closed<T>(_: T) -> SessionError {
    SessionError::Proto("ctl channel closed")
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("transport: {0}")]
    Transport(#[from] TransportError),
    #[error("decode: {0}")]
    Decode(#[from] mist_proto::DecodeError),
    #[error("authentication rejected (token mismatch)")]
    Auth,
    #[error("guest vm_uuid did not match the paired identity (cloned/reused token?) — re-pair")]
    IdentityMismatch,
    #[error("protocol: {0}")]
    Proto(&'static str),
    #[error("{0}")]
    Other(String),
}

fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "mac".into())
}

/// ctl lane driver: forwards queued ctl messages, answers pings, detects ping loss.
async fn ctl_driver(
    ctl: FramedStream,
    mut out: mpsc::Receiver<CtlMsg>,
) -> Result<(), SessionError> {
    // Reader on its own task: recv() is not cancel-safe, so it must never share a select!
    // with the sending side.
    let (mut reader, mut writer) = ctl.into_split();
    let outstanding = Arc::new(AtomicU64::new(0));
    let pongs = outstanding.clone();
    let mut reader_task = tokio::spawn(async move {
        loop {
            let f = reader.recv().await.map_err(SessionError::from)?;
            if f.kind != FrameKind::Ctl {
                continue;
            }
            match mist_proto::decode::<CtlMsg>(&f.payload).map_err(SessionError::from)? {
                CtlMsg::Pong { .. } => {
                    let _ = pongs.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(1))
                    });
                }
                CtlMsg::ShareGone { share } => {
                    tracing::warn!(?share, "share gone offline in guest");
                }
                CtlMsg::Goodbye { reason } => {
                    tracing::info!(%reason, "guest goodbye");
                    return Err(SessionError::Proto("guest closed session"));
                }
                other => tracing::debug!(?other, "ctl message ignored"),
            }
        }
    });
    let mut ticker = tokio::time::interval(PING_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut nonce: u64 = 0;
    loop {
        // Sends happen inside arm bodies (after the select resolves), so they are never
        // cancelled mid-frame. Polling a JoinHandle is cancel-safe.
        tokio::select! {
            _ = ticker.tick() => {
                if outstanding.load(Ordering::Relaxed) >= PING_MISSES as u64 {
                    reader_task.abort();
                    return Err(SessionError::Proto("ping timeout"));
                }
                nonce += 1;
                outstanding.fetch_add(1, Ordering::Relaxed);
                writer.send_msg(FrameKind::Ctl, nonce, &CtlMsg::Ping { nonce }).await?;
            }
            msg = out.recv() => {
                let Some(m) = msg else {
                    reader_task.abort();
                    return Err(SessionError::Proto("ctl queue closed"));
                };
                writer.send_msg(FrameKind::Ctl, 0, &m).await?;
            }
            r = &mut reader_task => {
                return r.unwrap_or(Err(SessionError::Proto("ctl reader panicked")));
            }
        }
    }
}

/// Journal lane reader: decode `JournalBatch`es and apply them (consistent-cut aware).
async fn journal_reader(mut j: FramedStream, assembler: Assembler) -> Result<(), SessionError> {
    loop {
        let f = j.recv().await?;
        if f.kind != FrameKind::Event {
            tracing::warn!(kind = ?f.kind, "non-event frame on journal lane");
            continue;
        }
        let ev = mist_proto::decode::<EventMsg>(&f.payload)?;
        let asm = assembler.clone();
        // Apply off the lane reader so a slow shard never stalls journal intake.
        tokio::task::spawn_blocking(move || asm.on_event(ev))
            .await
            .ok();
    }
}

// ---------------------------------------------------------------------------
// Snapshot assembly
// ---------------------------------------------------------------------------

/// Render a journal record as a `mist events` JSON line. Called *before* apply so a Removed
/// node's path still resolves.
fn event_line(replica: &ShareReplica, share: &str, rec: &mist_proto::Rec) -> Option<String> {
    use mist_proto::Rec;
    let child_path = |parent: NodeKey, name: &mist_proto::Name| -> String {
        let p = replica.path_of(parent).unwrap_or_default();
        let n = String::from_utf8_lossy(name.as_bytes());
        if p == "/" {
            format!("/{n}")
        } else {
            format!("{p}/{n}")
        }
    };
    let v = match rec {
        Rec::Created { parent, name, .. } => {
            serde_json::json!({"op":"created","share":share,"path":child_path(*parent, name)})
        }
        Rec::CreatedBatch { parent, entries } => serde_json::json!({
            "op":"created_batch","share":share,"dir":replica.path_of(*parent),"count":entries.len()
        }),
        Rec::Removed { parent, name } => {
            serde_json::json!({"op":"removed","share":share,"path":child_path(*parent, name)})
        }
        Rec::Renamed {
            from_parent,
            from_name,
            to_parent,
            to_name,
        } => serde_json::json!({
            "op":"renamed","share":share,
            "from":child_path(*from_parent, from_name),"to":child_path(*to_parent, to_name)
        }),
        Rec::AttrChanged { node, .. } => {
            serde_json::json!({"op":"attr","share":share,"path":replica.path_of(*node)})
        }
        Rec::Content { node, size, .. } => serde_json::json!({
            "op":"content","share":share,"path":replica.path_of(*node),"size":size
        }),
        Rec::SelfRemoved { node } => {
            serde_json::json!({"op":"removed","share":share,"path":replica.path_of(*node)})
        }
        Rec::Overflow | Rec::EchoMarker { .. } => return None,
    };
    Some(v.to_string())
}

/// Routes SnapDir/SnapDone events into per-share staging replicas, swapping on completion, and
/// applies journal batches to the live replica with consistent-cut buffering during seeding.
#[derive(Clone)]
struct Assembler {
    vm: Arc<VmHandle>,
    staging: Arc<Mutex<HashMap<ShareId, Staging>>>,
    infos: Arc<HashMap<ShareId, ShareInfo>>,
    /// Per-share next expected journal seq (set at seed completion). Absent ⇒ not yet live.
    seq: Arc<Mutex<HashMap<ShareId, u64>>>,
    /// Channel to request resyncs (SnapshotStart) from the ctl driver.
    ctl_tx: mpsc::Sender<CtlMsg>,
}

struct Staging {
    snap_id: u64,
    replica: Arc<ShareReplica>,
    started: std::time::Instant,
    /// Journal batches that arrived during this snapshot; replayed after the swap (consistent cut).
    journal_buf: Vec<mist_proto::JournalBatch>,
}

impl Assembler {
    fn new(vm: Arc<VmHandle>, infos: Vec<ShareInfo>, ctl_tx: mpsc::Sender<CtlMsg>) -> Self {
        Assembler {
            vm,
            staging: Arc::new(Mutex::new(HashMap::new())),
            infos: Arc::new(infos.into_iter().map(|i| (i.id, i)).collect()),
            seq: Arc::new(Mutex::new(HashMap::new())),
            ctl_tx,
        }
    }

    fn begin(&self, share: ShareId, snap_id: u64) {
        let Some(info) = self.infos.get(&share) else {
            return;
        };
        // Leaving Live during a reseed: stop applying live journal until the swap completes.
        // CAS manifests survive reseeds intentionally: they are keyed by stat fingerprint
        // (mtime+size, see surface::content_fingerprint), not by the restartable journal
        // counter, so unchanged files stay warm across hostd restarts.
        self.seq.lock().remove(&share);
        self.staging.lock().insert(
            share,
            Staging {
                snap_id,
                replica: Arc::new(ShareReplica::new(info.clone())),
                started: std::time::Instant::now(),
                journal_buf: Vec::new(),
            },
        );
    }

    fn on_event(&self, ev: EventMsg) {
        match ev {
            EventMsg::SnapDir(d) => {
                let staging = self.staging.lock();
                if let Some(s) = staging.get(&d.share)
                    && s.snap_id == d.snap_id
                {
                    s.replica.apply_snap_dir(&d);
                }
            }
            EventMsg::SnapDone(done) => self.finish_seed(done),
            EventMsg::Journal(batch) => self.on_journal(batch),
        }
    }

    fn finish_seed(&self, done: mist_proto::SnapDone) {
        let staged = {
            let mut staging = self.staging.lock();
            match staging.get(&done.share) {
                Some(s) if s.snap_id == done.snap_id => staging.remove(&done.share),
                _ => None,
            }
        };
        let Some(s) = staged else { return };
        let stats = s.replica.finish_snapshot(&done);
        let secs = s.started.elapsed().as_secs_f64().max(1e-9);
        let rate = (stats.entries as f64 / secs) as u64;
        self.vm.last_seed_rate.store(rate, Ordering::Relaxed);

        // Consistent cut: replay journal batches buffered during the walk onto the fresh replica.
        // Idempotent upserts make replaying already-reflected changes harmless. Track the highest
        // seq seen so live application continues contiguously.
        //
        // The guest's batcher numbers batches from *its* start, not from our snapshot, so when the
        // walk was fast enough that nothing got buffered we have no baseline — the first live
        // batch's seq is unknowable here. SEQ_ADOPT makes `on_journal` adopt that batch's seq as
        // the baseline instead of declaring a gap (sound: lanes are ordered and reliable, and
        // everything before the walk is already in the walked tree). Without this, a guest that
        // writes every second (≈ any real workload) trapped the session in a permanent
        // gap→resync→gap loop — found by the write-path churn e2e.
        let mut next_seq = SEQ_ADOPT;
        let mut replayed = 0usize;
        for b in &s.journal_buf {
            for rec in &b.records {
                s.replica.apply_rec(rec);
                replayed += 1;
            }
            let end = b.first_seq + b.records.len() as u64;
            next_seq = if next_seq == SEQ_ADOPT {
                end
            } else {
                next_seq.max(end)
            };
        }

        if let Some(h) = self.vm.share_by_id(done.share) {
            s.replica.set_state(ShareState::Live);
            h.swap_replica(s.replica);
            h.seeded.notify_waiters();
        }
        // From here, live batches apply directly; baseline the expected seq.
        self.seq.lock().insert(done.share, next_seq);
        tracing::info!(
            vm = %self.vm.name, share = ?done.share, nodes = stats.nodes, entries = stats.entries,
            errors = done.errors, rate_per_s = rate, journal_replayed = replayed, "seed complete"
        );
    }

    fn on_journal(&self, batch: mist_proto::JournalBatch) {
        // All-shares overflow sentinel (broadcast lag): resync everything.
        if batch.share == ShareId(u16::MAX) {
            tracing::warn!(vm = %self.vm.name, "journal overflow sentinel; resyncing all shares");
            let ids: Vec<ShareId> = self.infos.keys().copied().collect();
            for id in ids {
                self.request_resync(id);
            }
            return;
        }

        // If a snapshot is staging for this share, buffer (replayed after the swap).
        {
            let mut staging = self.staging.lock();
            if let Some(s) = staging.get_mut(&batch.share) {
                s.journal_buf.push(batch);
                return;
            }
        }

        // Live application.
        let Some(h) = self.vm.share_by_id(batch.share) else {
            return;
        };
        let replica = h.replica();
        if replica.state() != ShareState::Live {
            return; // dropped between staging removal and swap; rare, covered by next resync
        }

        // Seq contiguity: a gap means we missed events → resync.
        {
            let mut seqs = self.seq.lock();
            match seqs.get_mut(&batch.share) {
                Some(expected) => {
                    if *expected == SEQ_ADOPT {
                        // First live batch after a buffer-less seed: adopt its seq as baseline.
                        *expected = batch.first_seq + batch.records.len() as u64;
                    } else if batch.first_seq != *expected {
                        tracing::warn!(
                            vm = %self.vm.name, share = ?batch.share,
                            expected = *expected, got = batch.first_seq, "journal seq gap; resync"
                        );
                        drop(seqs);
                        self.request_resync(batch.share);
                        return;
                    } else {
                        *expected = batch.first_seq + batch.records.len() as u64;
                    }
                }
                None => return, // not live yet
            }
        }

        let share_name = h.info.read().name.clone();
        let has_subscribers = self.vm.events.receiver_count() > 0;
        let mut need_resync = false;
        for rec in &batch.records {
            if has_subscribers && let Some(line) = event_line(&replica, &share_name, rec) {
                let _ = self.vm.events.send(line);
            }
            // Conflict tracking: resolve the affected node *before* apply (a Removed entry is
            // unresolvable afterwards).
            let conflicted_node = match rec {
                Rec::Content { node, .. } | Rec::AttrChanged { node, .. } => Some(*node),
                Rec::Removed { parent, name } => replica
                    .lookup(*parent, name.as_bytes())
                    .ok()
                    .map(|(n, _)| n),
                _ => None,
            };
            // Echo fences (design 02 §6.5): records caused by the Mac's own writes must not
            // look like foreign changes — clamp attrs monotonic (a guest-clock mtime slightly
            // behind our optimistic one read as "file changed by someone else" and pushed the
            // macOS client into read-modify-write storms), skip the recall, skip the conflict.
            let echo = conflicted_node
                .map(|n| self.vm.is_mac_dirty(batch.share.0, n))
                .unwrap_or(false);
            if let Some(node) = conflicted_node
                && !echo
            {
                self.vm
                    .conflicts
                    .note_guest(batch.share.0, node, replica.path_of(node));
                // Recall pipeline: a held read delegation on this node must be recalled.
                let _ = self.vm.inval.send((batch.share.0, node));
            }
            // Echoed attr records are DROPPED, not clamped: the replica's optimistic attrs
            // are exactly what the NFS client has seen; any deviation — forward or backward
            // (guest clocks drift both ways) — reads as a foreign change and re-triggers the
            // revalidation storm. Namespace echoes (Created/Removed/Renamed) still apply:
            // idempotent upserts of dentries we already applied.
            if echo && matches!(rec, Rec::Content { .. } | Rec::AttrChanged { .. }) {
                continue;
            }
            if replica.apply_rec(rec) == mist_replica::ApplyOutcome::NeedResync {
                need_resync = true;
            }
            // Guest-side namespace changes must nudge the parent dir's mtime: the macOS
            // client pins negative dentries until the dir attr changes, and the journal
            // carries no dir AttrChanged for child create/remove. Mac-originated echoes are
            // excluded via the dir's mac-dirty window — bump_dir already moved it optimistically
            // and a second forward bump would re-trigger the per-create revalidation storm.
            // The echo signal is the affected NODE (Mac creates mark the new node mac-dirty),
            // not the parent dir — the parent is mac-dirty whenever the Mac touched the same
            // dir within the window, which is exactly when a guest's prompt response file
            // (create .cmd → guest writes .done) would be wrongly suppressed.
            let parents: [Option<NodeKey>; 2] = match rec {
                Rec::Created { parent, node, .. }
                    if !self.vm.is_mac_dirty(batch.share.0, *node) =>
                {
                    [Some(*parent), None]
                }
                Rec::CreatedBatch { parent, entries }
                    if entries
                        .first()
                        .is_none_or(|e| !self.vm.is_mac_dirty(batch.share.0, e.node)) =>
                {
                    [Some(*parent), None]
                }
                Rec::Removed { parent, .. } if !echo => [Some(*parent), None],
                Rec::Renamed {
                    from_parent,
                    to_parent,
                    ..
                } if !self.vm.is_mac_dirty(batch.share.0, *from_parent)
                    && !self.vm.is_mac_dirty(batch.share.0, *to_parent) =>
                {
                    [Some(*from_parent), Some(*to_parent)]
                }
                _ => [None, None],
            };
            for p in parents.into_iter().flatten() {
                replica.bump_dir_attr(p);
            }
        }
        self.vm
            .journal_records
            .fetch_add(batch.records.len() as u64, Ordering::Relaxed);
        if need_resync {
            self.vm.overflows.fetch_add(1, Ordering::Relaxed);
            self.request_resync(batch.share);
        }
    }

    fn request_resync(&self, share: ShareId) {
        let snap_id: u64 = rand::random();
        self.begin(share, snap_id);
        let tx = self.ctl_tx.clone();
        // Fire-and-forget; the ctl driver serializes sends.
        tokio::spawn(async move {
            let _ = tx.send(CtlMsg::SnapshotStart { share, snap_id }).await;
        });
    }
}

/// Reads one bulk lane: Event frames feed the assembler; Bulk frames feed pending reads.
async fn bulk_reader(
    mut lane: FramedStream,
    assembler: Assembler,
    pending: Arc<Pending>,
) -> Result<(), SessionError> {
    loop {
        let f = lane.recv().await?;
        match f.kind {
            FrameKind::Event => {
                let ev = mist_proto::decode::<EventMsg>(&f.payload)?;
                let asm = assembler.clone();
                // Apply on the blocking pool: applies take locks and may contend; never stall
                // the lane reader behind a slow shard.
                tokio::task::spawn_blocking(move || asm.on_event(ev))
                    .await
                    .ok();
            }
            FrameKind::Bulk => {
                pending.deliver_chunk(f.seq, f.payload, f.flags & FLAG_MORE != 0);
            }
            _ => tracing::warn!(kind = ?f.kind, "unexpected frame on bulk lane"),
        }
    }
}

// ---------------------------------------------------------------------------
// RPC client
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Pending {
    resp: Mutex<HashMap<u64, oneshot::Sender<RpcResp>>>,
    reads: Mutex<HashMap<u64, ReadSink>>,
}

struct ReadSink {
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl Pending {
    fn deliver_chunk(&self, seq: u64, bytes: Vec<u8>, more: bool) {
        let mut reads = self.reads.lock();
        if let Some(sink) = reads.get(&seq) {
            if !bytes.is_empty() {
                let _ = sink.tx.send(bytes);
            }
            if !more {
                reads.remove(&seq);
            }
        } else {
            tracing::debug!(seq, "bulk chunk with no pending read (late cancel?)");
        }
    }
}

pub struct RpcClient {
    out: mpsc::Sender<(u64, RpcReq)>,
    pending: Arc<Pending>,
    next_seq: AtomicU64,
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RpcClient")
    }
}

impl RpcClient {
    /// Start the client and also return a watch that flips true when the lane dies — a dead
    /// rpc lane previously left the session "healthy" (ctl pings fine) while every mutation
    /// failed forever; the session supervisor must observe lane death and reconnect.
    fn start_with_death(
        stream: FramedStream,
        pending: Arc<Pending>,
    ) -> (Self, watch::Receiver<bool>) {
        let (dead_tx, dead_rx) = watch::channel(false);
        let (out, mut rx) = mpsc::channel::<(u64, RpcReq)>(128);
        // send_msg/recv are not cancel-safe, so the two directions live on separate tasks
        // (a select! that cancels a half-read frame desyncs the stream).
        let (mut reader, mut writer) = stream.into_split();
        let writer_task = tokio::spawn(async move {
            while let Some((seq, req)) = rx.recv().await {
                if writer.send_msg(FrameKind::Req, seq, &req).await.is_err() {
                    break;
                }
            }
        });
        let p = pending.clone();
        tokio::spawn(async move {
            loop {
                let Ok(f) = reader.recv().await else { break };
                if f.kind != FrameKind::Resp {
                    continue;
                }
                match mist_proto::decode::<RpcResp>(&f.payload) {
                    Ok(resp) => {
                        if let Some(tx) = p.resp.lock().remove(&f.seq) {
                            let _ = tx.send(resp);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "rpc response decode failed");
                        break;
                    }
                }
            }
            // Session is dying: fail all waiters, stop the writer, signal the supervisor.
            writer_task.abort();
            p.resp.lock().clear();
            p.reads.lock().clear();
            let _ = dead_tx.send(true);
        });
        (
            RpcClient {
                out,
                pending,
                next_seq: AtomicU64::new(1),
            },
            dead_rx,
        )
    }

    pub async fn call(&self, req: RpcReq) -> Result<RpcResp, SessionError> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.resp.lock().insert(seq, tx);
        self.out
            .send((seq, req))
            .await
            .map_err(|_| SessionError::Proto("rpc lane closed"))?;
        match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(SessionError::Proto("rpc waiter dropped (session died)")),
            Err(_) => {
                self.pending.resp.lock().remove(&seq);
                Err(SessionError::Proto("rpc timeout"))
            }
        }
    }

    /// Read up to `len` bytes at `off`. Returns (bytes, eof).
    pub async fn read(
        &self,
        share: ShareId,
        node: NodeKey,
        off: u64,
        len: u32,
    ) -> Result<(Vec<u8>, bool), SessionError> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let (ctx, mut crx) = mpsc::unbounded_channel();
        self.pending.resp.lock().insert(seq, tx);
        self.pending.reads.lock().insert(seq, ReadSink { tx: ctx });

        let req = RpcReq::Read {
            share,
            node,
            version_hint: 0,
            off,
            len,
            ra: 0,
        };
        if self.out.send((seq, req)).await.is_err() {
            self.pending.reads.lock().remove(&seq);
            return Err(SessionError::Proto("rpc lane closed"));
        }

        let header = match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(h)) => h,
            _ => {
                self.pending.resp.lock().remove(&seq);
                self.pending.reads.lock().remove(&seq);
                return Err(SessionError::Proto("read header timeout"));
            }
        };
        let (total, eof) = match header {
            RpcResp::ReadStart { len, eof, .. } => (len, eof),
            RpcResp::Err(e) => {
                self.pending.reads.lock().remove(&seq);
                return Err(SessionError::Other(format!(
                    "read failed: errno {} ({})",
                    e.errno, e.msg
                )));
            }
            _ => {
                self.pending.reads.lock().remove(&seq);
                return Err(SessionError::Proto("unexpected read response"));
            }
        };

        let mut buf = Vec::with_capacity(total as usize);
        if total > 0 {
            loop {
                match tokio::time::timeout(RPC_TIMEOUT, crx.recv()).await {
                    Ok(Some(chunk)) => {
                        buf.extend_from_slice(&chunk);
                        if buf.len() as u64 >= total {
                            // Sink entry is removed by the final !MORE frame.
                            break;
                        }
                    }
                    Ok(None) => break, // terminated early (file shrank) — short read
                    Err(_) => {
                        self.pending.reads.lock().remove(&seq);
                        return Err(SessionError::Proto("read body timeout"));
                    }
                }
            }
        }
        Ok((buf, eof))
    }
}
