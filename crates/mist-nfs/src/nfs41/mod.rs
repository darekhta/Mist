//! NFSv4.1 server (design 05 §5): minimal-but-correct single-client implementation.
//! COMPOUND dispatch over the same `MountSurface` as v3; sessions with slot reply caching;
//! OPEN/CLOSE stateids; read delegations with journal-driven CB_RECALL on the fore connection
//! (v4.1 backchannel — no client-side listener).

pub mod attrs;
pub mod state;

use crate::handle::HandleCodec;
use crate::surface::{CreateKind, MountSurface, NfsError};
use crate::xdr::{XdrReader, XdrWriter};
use attrs::Bitmap;
use mist_proto::NodeKey;
use parking_lot::Mutex;
use state::{BACK_SLOTS, FORE_SLOTS, Session, State};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub const NFS_PROGRAM: u32 = 100003;
pub const NFS_VERSION: u32 = 4;
const PROC_NULL: u32 = 0;
const PROC_COMPOUND: u32 = 1;
const MAX_RECORD: usize = 2 * 1024 * 1024;
const MAX_OPS: usize = 64;
const MAX_READ: u32 = 1024 * 1024;
const MAX_WRITE: u32 = 1024 * 1024;

// ---- op numbers (RFC 5661 §16) ----------------------------------------------------------------
const OP_ACCESS: u32 = 3;
const OP_CLOSE: u32 = 4;
const OP_COMMIT: u32 = 5;
const OP_CREATE: u32 = 6;
const OP_DELEGRETURN: u32 = 8;
const OP_GETATTR: u32 = 9;
const OP_GETFH: u32 = 10;
const OP_LINK: u32 = 11;
const OP_LOCK: u32 = 12;
const OP_LOCKT: u32 = 13;
const OP_LOCKU: u32 = 14;
const OP_LOOKUP: u32 = 15;
const OP_LOOKUPP: u32 = 16;
const OP_NVERIFY: u32 = 17;
const OP_OPEN: u32 = 18;
const OP_OPEN_DOWNGRADE: u32 = 21;
const OP_PUTFH: u32 = 22;
const OP_PUTPUBFH: u32 = 23;
const OP_PUTROOTFH: u32 = 24;
const OP_READ: u32 = 25;
const OP_READDIR: u32 = 26;
const OP_READLINK: u32 = 27;
const OP_REMOVE: u32 = 28;
const OP_RENAME: u32 = 29;
const OP_RESTOREFH: u32 = 31;
const OP_SAVEFH: u32 = 32;
const OP_SETATTR: u32 = 34;
const OP_VERIFY: u32 = 37;
const OP_WRITE: u32 = 38;
const OP_BIND_CONN_TO_SESSION: u32 = 41;
const OP_EXCHANGE_ID: u32 = 42;
const OP_CREATE_SESSION: u32 = 43;
const OP_DESTROY_SESSION: u32 = 44;
const OP_FREE_STATEID: u32 = 45;
const OP_SECINFO_NO_NAME: u32 = 52;
const OP_SEQUENCE: u32 = 53;
const OP_TEST_STATEID: u32 = 55;
const OP_DESTROY_CLIENTID: u32 = 57;
const OP_RECLAIM_COMPLETE: u32 = 58;
const OP_ILLEGAL: u32 = 10044;

// Callback ops.
const CB_OP_RECALL: u32 = 4;
const CB_OP_SEQUENCE: u32 = 11;

// ---- status codes ------------------------------------------------------------------------------
const NFS4_OK: u32 = 0;
const ERR_NOENT: u32 = 2;
const ERR_IO: u32 = 5;
const ERR_ACCESS: u32 = 13;
const ERR_EXIST: u32 = 17;
const ERR_NOTDIR: u32 = 20;
const ERR_ISDIR: u32 = 21;
const ERR_INVAL: u32 = 22;
const ERR_NOSPC: u32 = 28;
const ERR_ROFS: u32 = 30;
const ERR_NAMETOOLONG: u32 = 63;
const ERR_NOTEMPTY: u32 = 66;
const ERR_STALE: u32 = 70;
const ERR_BADHANDLE: u32 = 10001;
const ERR_NOTSUPP: u32 = 10004;
const ERR_DELAY: u32 = 10008;
const ERR_NOFILEHANDLE: u32 = 10020;
const ERR_STALE_CLIENTID: u32 = 10022;
const ERR_STALE_STATEID: u32 = 10023;
const ERR_BAD_STATEID: u32 = 10025;
const ERR_BADXDR: u32 = 10036;
const ERR_BADSESSION: u32 = 10052;
const ERR_BADSLOT: u32 = 10053;
const ERR_SEQ_MISORDERED: u32 = 10063;
const ERR_SEQUENCE_POS: u32 = 10064;
const ERR_RETRY_UNCACHED_REP: u32 = 10068;
const ERR_OP_ILLEGAL: u32 = 10044;

fn status_of(e: NfsError) -> u32 {
    match e {
        NfsError::NoEnt => ERR_NOENT,
        NfsError::NotDir => ERR_NOTDIR,
        NfsError::IsDir => ERR_ISDIR,
        NfsError::NotSymlink => ERR_INVAL,
        NfsError::Stale => ERR_STALE,
        NfsError::Access => ERR_ACCESS,
        NfsError::Io => ERR_IO,
        NfsError::Jukebox => ERR_DELAY,
        NfsError::NameTooLong => ERR_NAMETOOLONG,
        NfsError::NotEmpty => ERR_NOTEMPTY,
        NfsError::Exist => ERR_EXIST,
        NfsError::NoSpace => ERR_NOSPC,
        NfsError::Rofs => ERR_ROFS,
    }
}

/// Observed delegation-recall timings (p99 target ≤ 10 ms).
#[derive(Debug, Default)]
pub struct RecallMetrics {
    pub recalls_sent: AtomicU64,
    pub returns: AtomicU64,
    pub total_recall_ns: AtomicU64,
    pub max_recall_ns: AtomicU64,
    /// Recall→DELEGRETURN latency samples (ns), capped; enough for an honest p99.
    pub samples_ns: Mutex<Vec<u64>>,
}

impl RecallMetrics {
    pub fn percentile_ns(&self, p: f64) -> Option<u64> {
        let mut s = self.samples_ns.lock().clone();
        if s.is_empty() {
            return None;
        }
        s.sort_unstable();
        let idx = ((s.len() as f64 - 1.0) * p).round() as usize;
        Some(s[idx.min(s.len() - 1)])
    }
}

pub struct Nfs41Server<S: MountSurface> {
    surface: Arc<S>,
    codec: HandleCodec,
    state: Arc<State>,
    pub recall_metrics: Arc<RecallMetrics>,
    /// Total fore-channel ops served — the "0 loopback RPCs in a delegated hot loop" gate
    /// compares this before/after the loop.
    pub ops_served: AtomicU64,
    /// Live backchannel writers, by session id (set when a session's connection binds).
    backchannels: Mutex<HashMap<[u8; 16], BackChannel>>,
    next_cb_xid: AtomicU32,
}

/// Dispatch-path counters (logged at connection EOF when tracing is on): how many calls took
/// the inline fast path vs a spawned handler. Diagnostic only.
static INLINE_CALLS: AtomicU64 = AtomicU64::new(0);
static SPAWNED_CALLS: AtomicU64 = AtomicU64::new(0);

/// Whether the idle-pipeline inline fast path is enabled (`MIST_INLINE=1`, default OFF). The
/// macOS client often keeps 2 READs in flight (its nfsiod pair) and pipelines the next
/// mutation behind a CREATE's guest round-trip; inline handling serializes both (measured:
/// creates +4 ms/file) for no reliable read gain, so it stays opt-in for experiments.
pub(crate) fn inline_dispatch() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("MIST_INLINE").is_ok_and(|v| v == "1"))
}

#[derive(Debug, Clone)]
struct BackChannel {
    wr: crate::server::SharedWriter,
    pending: Arc<Mutex<HashMap<u32, oneshot::Sender<u32>>>>,
    cb_program: u32,
}

impl<S: MountSurface> std::fmt::Debug for Nfs41Server<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Nfs41Server")
    }
}

/// Per-compound evaluation state.
struct Ctx {
    cfh: Option<(u16, NodeKey)>,
    sfh: Option<(u16, NodeKey)>,
    /// Most recent stateid an op produced (the v4.1 "current stateid").
    current_stateid: Option<([u8; 12], u32)>,
    session: Option<Arc<Session>>,
    /// True while evaluating the compound's final op (enables the sendfile tail-READ).
    is_last_op: bool,
    /// False when this compound's reply must be cached for replay (cache needs the bytes).
    allow_sendfile: bool,
    /// Set by a tail READ served zero-copy: the payload to sendfile after the rendered head.
    tail_file: Option<crate::surface::SendableRead>,
}

impl<S: MountSurface> Nfs41Server<S> {
    pub fn new(surface: Arc<S>, handle_secret: &[u8]) -> Self {
        Nfs41Server {
            surface,
            codec: HandleCodec::new(handle_secret),
            state: Arc::new(State::new()),
            recall_metrics: Arc::new(RecallMetrics::default()),
            ops_served: AtomicU64::new(0),
            backchannels: Mutex::new(HashMap::new()),
            next_cb_xid: AtomicU32::new(0x4D53_0001),
        }
    }

    /// Delegation counters for `mist delegs`: (granted, recalls, returned, revoked, held-now).
    pub fn deleg_snapshot(&self) -> (u64, u64, u64, u64, usize) {
        let s = &self.state.deleg_stats;
        (
            s.granted.load(Ordering::Relaxed),
            s.recalls.load(Ordering::Relaxed),
            s.returned.load(Ordering::Relaxed),
            s.revoked.load(Ordering::Relaxed),
            self.state.delegations.lock().len(),
        )
    }

    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, _peer) = listener.accept().await?;
            stream.set_nodelay(true).ok();
            crate::server::tune_socket(&stream);
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.serve_conn(stream).await {
                    tracing::debug!(error = %e, "nfs41 connection ended");
                }
            });
        }
    }

    /// Connection actor: the read loop routes REPLYs (backchannel responses) and dispatches
    /// CALLs. v4.1 slots make concurrency safe by construction — the client keeps at most one
    /// compound in flight per slot and replies match by xid, so out-of-order completion across
    /// slots is exactly what sessions are for. A serial loop here capped pipelined reads/writes
    /// at a fraction of the client's slot depth. When NOTHING is in flight (sequential reads —
    /// macOS keeps one READ outstanding per stream) the compound is handled inline on this task
    /// and the reply leaves in one `writev` — no spawn, no handoff wakeups.
    async fn serve_conn(self: &Arc<Self>, stream: tokio::net::TcpStream) -> std::io::Result<()> {
        const FORE: usize = state::FORE_SLOTS as usize;
        let (rd, wr) = stream.into_split();
        let mut rd = tokio::io::BufReader::with_capacity(crate::server::READ_BUF, rd);
        let wr: crate::server::SharedWriter = Arc::new(tokio::sync::Mutex::new(wr));
        let pending: Arc<Mutex<HashMap<u32, oneshot::Sender<u32>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Sessions this connection has carried — used to unbind the backchannel on EOF.
        let bound: Arc<Mutex<Vec<[u8; 16]>>> = Arc::new(Mutex::new(Vec::new()));
        let sem = Arc::new(tokio::sync::Semaphore::new(FORE));
        let mut max_depth = 0usize;

        loop {
            let Some(record) = read_record(&mut rd).await? else {
                break;
            };
            let mut r = XdrReader::new(&record);
            let Ok(xid) = r.u32() else { continue };
            let Ok(msg_type) = r.u32() else { continue };
            if msg_type == 1 {
                // REPLY — a backchannel response. Extract the compound status (best-effort).
                let status = parse_cb_reply_status(&record).unwrap_or(ERR_IO);
                if let Some(waiter) = pending.lock().remove(&xid) {
                    let _ = waiter.send(status);
                }
                continue;
            }
            if inline_dispatch() && sem.available_permits() == FORE && rd.buffer().is_empty() {
                INLINE_CALLS.fetch_add(1, Ordering::Relaxed);
                let reply = self.handle_call(&record, &wr, &pending, &bound).await;
                if crate::server::write_reply(&wr, reply).await.is_err() {
                    break;
                }
                continue;
            }
            SPAWNED_CALLS.fetch_add(1, Ordering::Relaxed);
            let Ok(permit) = sem.clone().acquire_owned().await else {
                break;
            };
            // Depth high-water mark for this connection (diagnostic, logged at EOF).
            let depth = FORE - sem.available_permits();
            if depth > max_depth {
                max_depth = depth;
            }
            let me = self.clone();
            let wr2 = wr.clone();
            let pending2 = pending.clone();
            let bound2 = bound.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let reply = me.handle_call(&record, &wr2, &pending2, &bound2).await;
                let _ = crate::server::write_reply(&wr2, reply).await;
            });
        }
        // In-flight handlers hold clones of `wr`; the socket closes when the last one drops.
        tracing::debug!(
            inline = INLINE_CALLS.load(Ordering::Relaxed),
            spawned = SPAWNED_CALLS.load(Ordering::Relaxed),
            max_depth,
            ops = self.ops_served.load(Ordering::Relaxed),
            "nfs41 dispatch-path totals"
        );
        // Connection gone: drop its backchannel bindings.
        let mut bcs = self.backchannels.lock();
        for sid in bound.lock().iter() {
            bcs.remove(sid);
        }
        Ok(())
    }

    async fn handle_call(
        self: &Arc<Self>,
        record: &[u8],
        conn_tx: &crate::server::SharedWriter,
        pending: &Arc<Mutex<HashMap<u32, oneshot::Sender<u32>>>>,
        bound: &Mutex<Vec<[u8; 16]>>,
    ) -> crate::server::Reply {
        use crate::server::Reply;
        let (call, mut args) = match crate::rpc::parse_call(record) {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "nfs41 rpc parse failed");
                return Reply::Whole(Vec::new());
            }
        };
        if call.program != NFS_PROGRAM {
            return Reply::Whole(
                crate::rpc::reply_accepted(call.xid, crate::rpc::ACCEPT_PROG_UNAVAIL).into_bytes(),
            );
        }
        if call.version != NFS_VERSION {
            return Reply::Whole(crate::rpc::reply_prog_mismatch(call.xid, 4, 4).into_bytes());
        }
        match call.procedure {
            PROC_NULL => Reply::Whole(
                crate::rpc::reply_accepted(call.xid, crate::rpc::ACCEPT_SUCCESS).into_bytes(),
            ),
            PROC_COMPOUND => {
                let mut w = crate::rpc::reply_accepted(call.xid, crate::rpc::ACCEPT_SUCCESS);
                let tail = self
                    .compound(&mut args, &mut w, conn_tx, pending, bound)
                    .await;
                match tail {
                    Some(body) => {
                        let pad = (4 - (body.len as usize % 4)) % 4;
                        Reply::HeadAndFile {
                            head: w.into_bytes(),
                            body,
                            pad,
                        }
                    }
                    None => Reply::Whole(w.into_bytes()),
                }
            }
            _ => Reply::Whole(
                crate::rpc::reply_accepted(call.xid, crate::rpc::ACCEPT_PROC_UNAVAIL).into_bytes(),
            ),
        }
    }

    /// Evaluate one COMPOUND. Encodes `status, tag, numres, results…` into `w`.
    async fn compound(
        self: &Arc<Self>,
        r: &mut XdrReader<'_>,
        w: &mut XdrWriter,
        conn_tx: &crate::server::SharedWriter,
        pending: &Arc<Mutex<HashMap<u32, oneshot::Sender<u32>>>>,
        bound: &Mutex<Vec<[u8; 16]>>,
    ) -> Option<crate::surface::SendableRead> {
        let (tag, minor, nops) = match (|| -> Result<_, crate::xdr::XdrError> {
            let tag = r.opaque(1024)?.to_vec();
            let minor = r.u32()?;
            let nops = r.u32()? as usize;
            Ok((tag, minor, nops))
        })() {
            Ok(t) => t,
            Err(_) => {
                w.u32(ERR_BADXDR);
                w.opaque(&[]);
                w.u32(0);
                return None;
            }
        };
        if minor != 1 {
            w.u32(10021); // NFS4ERR_MINOR_VERS_MISMATCH
            w.opaque(&tag);
            w.u32(0);
            return None;
        }
        if nops > MAX_OPS {
            w.u32(10065); // REQ_TOO_BIG
            w.opaque(&tag);
            w.u32(0);
            return None;
        }

        let mut ctx = Ctx {
            cfh: None,
            sfh: None,
            current_stateid: None,
            session: None,
            is_last_op: false,
            allow_sendfile: true,
            tail_file: None,
        };
        // Single-buffer assembly: op results encode DIRECTLY into the reply writer; status and
        // numres are back-patched. The old two-buffer scheme copied every 1 MiB READ payload an
        // extra time — measurable at streaming rates.
        let restart = w.pos();
        let status_pos = w.pos();
        w.u32(NFS4_OK);
        w.opaque(&tag);
        let numres_pos = w.pos();
        w.u32(0);
        let body_pos = w.pos();
        let mut status = NFS4_OK;
        let mut nres = 0u32;
        // Slot bookkeeping for the reply cache (set when op 0 is SEQUENCE).
        let mut cache_slot: Option<(Arc<Session>, u32, u32, bool)> = None;

        for i in 0..nops {
            let Ok(op) = r.u32() else {
                status = ERR_BADXDR;
                break;
            };
            // SEQUENCE handling is special: replay detection consults the slot cache.
            if op == OP_SEQUENCE {
                if i != 0 {
                    w.u32(OP_SEQUENCE);
                    w.u32(ERR_SEQUENCE_POS);
                    nres += 1;
                    status = ERR_SEQUENCE_POS;
                    break;
                }
                match self.op_sequence(r, w, &mut ctx, bound, conn_tx, pending) {
                    SeqOutcome::Ok {
                        sess,
                        slot,
                        seqid,
                        cachethis,
                    } => {
                        nres += 1;
                        ctx.allow_sendfile = !cachethis;
                        cache_slot = Some((sess, slot, seqid, cachethis));
                        continue;
                    }
                    SeqOutcome::Replay(cached) => {
                        // Whole-reply replay: rewind and emit the cached bytes verbatim.
                        w.truncate(restart);
                        w.u32(NFS4_OK);
                        w.opaque(&tag);
                        w.fixed(&cached);
                        return None;
                    }
                    SeqOutcome::Err(e) => {
                        nres += 1;
                        status = e;
                        break;
                    }
                }
            }
            let t_op = std::time::Instant::now();
            ctx.is_last_op = i == nops - 1;
            let st = self.eval_op(op, r, w, &mut ctx).await;
            self.ops_served.fetch_add(1, Ordering::Relaxed);
            tracing::trace!(op, st, us = t_op.elapsed().as_micros() as u64, "nfs41 op");
            nres += 1;
            if st != NFS4_OK {
                status = st;
                break;
            }
        }

        w.patch_u32(status_pos, status);
        w.patch_u32(numres_pos, nres);

        // Populate the reply cache (numres+body, copied ONLY when the client asked; the
        // sendfile tail is disabled for cached replies so the body is always complete here).
        if let Some((sess, slot, seqid, cachethis)) = cache_slot {
            let mut slots = sess.fore_slots.lock();
            if let Some(s) = slots.get_mut(slot as usize) {
                s.seqid = seqid;
                if cachethis {
                    let mut cached = XdrWriter::new();
                    cached.u32(nres);
                    cached.fixed(w.since(body_pos));
                    s.reply = Some(cached.into_bytes());
                    s.had_uncached = false;
                } else {
                    s.reply = None;
                    s.had_uncached = true;
                }
            }
        }
        ctx.tail_file
    }

    fn op_sequence(
        self: &Arc<Self>,
        r: &mut XdrReader<'_>,
        out: &mut XdrWriter,
        ctx: &mut Ctx,
        bound: &Mutex<Vec<[u8; 16]>>,
        conn_tx: &crate::server::SharedWriter,
        pending: &Arc<Mutex<HashMap<u32, oneshot::Sender<u32>>>>,
    ) -> SeqOutcome {
        let parsed = (|| -> Result<_, crate::xdr::XdrError> {
            let sid: [u8; 16] = r.fixed(16)?.try_into().unwrap();
            let seqid = r.u32()?;
            let slot = r.u32()?;
            let highest = r.u32()?;
            let cachethis = r.bool()?;
            Ok((sid, seqid, slot, highest, cachethis))
        })();
        let Ok((sid, seqid, slot, _highest, cachethis)) = parsed else {
            out.u32(OP_SEQUENCE);
            out.u32(ERR_BADXDR);
            return SeqOutcome::Err(ERR_BADXDR);
        };
        let Some(sess) = self.state.session(&sid) else {
            out.u32(OP_SEQUENCE);
            out.u32(ERR_BADSESSION);
            return SeqOutcome::Err(ERR_BADSESSION);
        };
        if slot >= FORE_SLOTS {
            out.u32(OP_SEQUENCE);
            out.u32(ERR_BADSLOT);
            return SeqOutcome::Err(ERR_BADSLOT);
        }
        {
            let slots = sess.fore_slots.lock();
            let s = &slots[slot as usize];
            if seqid == s.seqid {
                // Replay.
                if let Some(cached) = s.reply.clone() {
                    return SeqOutcome::Replay(cached);
                }
                if s.had_uncached {
                    out.u32(OP_SEQUENCE);
                    out.u32(ERR_RETRY_UNCACHED_REP);
                    return SeqOutcome::Err(ERR_RETRY_UNCACHED_REP);
                }
                // Slot never used (seqid 0): treat seqid 0 as misordered.
                out.u32(OP_SEQUENCE);
                out.u32(ERR_SEQ_MISORDERED);
                return SeqOutcome::Err(ERR_SEQ_MISORDERED);
            }
            if seqid != s.seqid.wrapping_add(1) {
                out.u32(OP_SEQUENCE);
                out.u32(ERR_SEQ_MISORDERED);
                return SeqOutcome::Err(ERR_SEQ_MISORDERED);
            }
        }
        // This connection carries the session: make sure the backchannel rides it.
        let fresh_binding = {
            let mut b = bound.lock();
            if b.contains(&sid) {
                false
            } else {
                b.push(sid);
                true
            }
        };
        if fresh_binding {
            self.backchannels.lock().insert(
                sid,
                BackChannel {
                    wr: conn_tx.clone(),
                    pending: pending.clone(),
                    cb_program: sess.cb_program,
                },
            );
        }
        ctx.session = Some(sess.clone());
        out.u32(OP_SEQUENCE);
        out.u32(NFS4_OK);
        out.fixed(&sid);
        out.u32(seqid);
        out.u32(slot);
        out.u32(FORE_SLOTS - 1); // sr_highest_slotid
        out.u32(FORE_SLOTS - 1); // sr_target_highest_slotid
        out.u32(0); // sr_status_flags
        SeqOutcome::Ok {
            sess,
            slot,
            seqid,
            cachethis,
        }
    }

    async fn eval_op(
        self: &Arc<Self>,
        op: u32,
        r: &mut XdrReader<'_>,
        out: &mut XdrWriter,
        ctx: &mut Ctx,
    ) -> u32 {
        macro_rules! xdr {
            ($e:expr) => {
                match $e {
                    Ok(v) => v,
                    Err(_) => {
                        out.u32(op);
                        out.u32(ERR_BADXDR);
                        return ERR_BADXDR;
                    }
                }
            };
        }
        macro_rules! need_cfh {
            () => {
                match ctx.cfh {
                    Some(fh) => fh,
                    None => {
                        out.u32(op);
                        out.u32(ERR_NOFILEHANDLE);
                        return ERR_NOFILEHANDLE;
                    }
                }
            };
        }
        macro_rules! fail {
            ($st:expr) => {{
                out.u32(op);
                out.u32($st);
                return $st;
            }};
        }

        match op {
            OP_EXCHANGE_ID => {
                let verifier: [u8; 8] = xdr!(r.fixed(8)).try_into().unwrap();
                let owner = xdr!(r.opaque(1024)).to_vec();
                let _flags = xdr!(r.u32());
                let sp_how = xdr!(r.u32());
                if sp_how != 0 {
                    fail!(ERR_NOTSUPP); // SP4_NONE only
                }
                // eia_client_impl_id<1>
                let n_impl = xdr!(r.u32());
                if n_impl > 0 {
                    let _domain = xdr!(r.opaque(1024));
                    let _name = xdr!(r.opaque(1024));
                    let _sec = xdr!(r.u64());
                    let _nsec = xdr!(r.u32());
                }
                let (clientid, seqid) = self.state.exchange_id(&owner, verifier);
                out.u32(op);
                out.u32(NFS4_OK);
                out.u64(clientid);
                out.u32(seqid);
                out.u32(0x0002_0000); // EXCHGID4_FLAG_USE_NON_PNFS
                out.u32(0); // SP4_NONE
                out.u64(0x4D49_5354); // server_owner.minor ("MIST")
                out.opaque(b"mist-hostd"); // server_owner.major
                out.opaque(b"mist"); // server scope
                out.u32(0); // no impl id
                NFS4_OK
            }
            OP_CREATE_SESSION => {
                let clientid = xdr!(r.u64());
                let csa_seq = xdr!(r.u32());
                let flags = xdr!(r.u32());
                let fore = xdr!(read_chan_attrs(r));
                let _back = xdr!(read_chan_attrs(r));
                let cb_program = xdr!(r.u32());
                let n_sec = xdr!(r.u32());
                for _ in 0..n_sec {
                    let flavor = xdr!(r.u32());
                    match flavor {
                        0 => {}
                        1 => {
                            let _stamp = xdr!(r.u32());
                            let _machine = xdr!(r.opaque(256));
                            let _uid = xdr!(r.u32());
                            let _gid = xdr!(r.u32());
                            let n = xdr!(r.u32());
                            for _ in 0..n {
                                let _ = xdr!(r.u32());
                            }
                        }
                        _ => fail!(ERR_NOTSUPP),
                    }
                }
                let Some(sess) = self.state.create_session(clientid, cb_program) else {
                    fail!(ERR_STALE_CLIENTID);
                };
                out.u32(op);
                out.u32(NFS4_OK);
                out.fixed(&sess.id);
                out.u32(csa_seq);
                out.u32(flags & 0x1); // echo CONN_BACK_CHAN if requested
                // fore channel attrs (echo sane caps)
                write_chan_attrs(out, fore.max_req.min(MAX_RECORD as u32), FORE_SLOTS);
                write_chan_attrs(out, MAX_RECORD as u32, BACK_SLOTS);
                NFS4_OK
            }
            OP_DESTROY_SESSION => {
                let sid: [u8; 16] = xdr!(r.fixed(16)).try_into().unwrap();
                let st = if self.state.destroy_session(&sid) {
                    self.backchannels.lock().remove(&sid);
                    NFS4_OK
                } else {
                    ERR_BADSESSION
                };
                out.u32(op);
                out.u32(st);
                st
            }
            OP_DESTROY_CLIENTID => {
                let clientid = xdr!(r.u64());
                let st = if self.state.destroy_client(clientid) {
                    NFS4_OK
                } else {
                    ERR_STALE_CLIENTID
                };
                out.u32(op);
                out.u32(st);
                st
            }
            OP_BIND_CONN_TO_SESSION => {
                let sid: [u8; 16] = xdr!(r.fixed(16)).try_into().unwrap();
                let dir = xdr!(r.u32());
                let _rdma = xdr!(r.bool());
                if self.state.session(&sid).is_none() {
                    fail!(ERR_BADSESSION);
                }
                out.u32(op);
                out.u32(NFS4_OK);
                out.fixed(&sid);
                out.u32(dir.clamp(1, 3));
                out.bool(false);
                NFS4_OK
            }
            OP_RECLAIM_COMPLETE => {
                let _one_fs = xdr!(r.bool());
                out.u32(op);
                out.u32(NFS4_OK);
                NFS4_OK
            }
            OP_SECINFO_NO_NAME => {
                let _style = xdr!(r.u32());
                // cfh required and consumed by this op.
                let _ = need_cfh!();
                ctx.cfh = None;
                out.u32(op);
                out.u32(NFS4_OK);
                out.u32(2); // two flavors
                out.u32(1); // AUTH_SYS
                out.u32(0); // AUTH_NONE
                NFS4_OK
            }
            OP_PUTROOTFH | OP_PUTPUBFH => {
                ctx.cfh = Some((self.surface.share_id(), self.surface.root()));
                out.u32(op);
                out.u32(NFS4_OK);
                NFS4_OK
            }
            OP_PUTFH => {
                let fh = xdr!(r.opaque(128));
                match self.codec.decode(fh) {
                    Some((share, node)) => {
                        ctx.cfh = Some((share, node));
                        out.u32(op);
                        out.u32(NFS4_OK);
                        NFS4_OK
                    }
                    None => fail!(ERR_BADHANDLE),
                }
            }
            OP_GETFH => {
                let (share, node) = need_cfh!();
                out.u32(op);
                out.u32(NFS4_OK);
                out.opaque(&self.codec.encode(share, node));
                NFS4_OK
            }
            OP_SAVEFH => {
                let fh = need_cfh!();
                ctx.sfh = Some(fh);
                out.u32(op);
                out.u32(NFS4_OK);
                NFS4_OK
            }
            OP_RESTOREFH => match ctx.sfh {
                Some(fh) => {
                    ctx.cfh = Some(fh);
                    out.u32(op);
                    out.u32(NFS4_OK);
                    NFS4_OK
                }
                None => fail!(10030), // NFS4ERR_RESTOREFH
            },
            OP_LOOKUP => {
                let name = xdr!(r.opaque(4096)).to_vec();
                let (share, dir) = need_cfh!();
                if name.len() > 255 {
                    fail!(ERR_NAMETOOLONG);
                }
                match self.surface.lookup(dir, &name) {
                    Ok((node, _attr)) => {
                        ctx.cfh = Some((share, node));
                        out.u32(op);
                        out.u32(NFS4_OK);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_LOOKUPP => {
                let (share, node) = need_cfh!();
                match self.surface.parent(node) {
                    Ok(parent) => {
                        ctx.cfh = Some((share, parent));
                        out.u32(op);
                        out.u32(NFS4_OK);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_GETATTR => {
                let req = xdr!(Bitmap::decode(r));
                let (share, node) = need_cfh!();
                match self.surface.getattr(node) {
                    Ok(attr) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        let fh = self.codec.encode(share, node);
                        attrs::encode_fattr4(
                            out,
                            &req,
                            &attr,
                            node.ino,
                            share,
                            Some(&fh),
                            &self.surface.fsstat(),
                        );
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_ACCESS => {
                let want = xdr!(r.u32());
                let (_share, node) = need_cfh!();
                match self.surface.getattr(node) {
                    Ok(attr) => {
                        // Same policy as v3 (design 05 §7): squash identity, guest enforces
                        // real DAC; grant read/lookup always, mutations iff writable.
                        let is_dir = matches!(attr.kind, mist_proto::Kind::Dir);
                        let mut granted = 0x01 | 0x02; // READ | LOOKUP
                        if self.surface.writable() {
                            granted |= 0x04 | 0x08 | 0x10; // MODIFY | EXTEND | DELETE
                        }
                        if !is_dir {
                            granted &= !0x02;
                            granted |= 0x20; // EXECUTE
                        }
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.u32(granted & want | (granted & 0x3F));
                        out.u32(granted & want);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_READLINK => {
                let (_share, node) = need_cfh!();
                match self.surface.readlink(node) {
                    Ok(target) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.opaque(&target);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_READDIR => {
                let cookie = xdr!(r.u64());
                let _verf = xdr!(r.fixed(8));
                let _dircount = xdr!(r.u32());
                let maxcount = xdr!(r.u32());
                let req = xdr!(Bitmap::decode(r));
                let (share, dir) = need_cfh!();
                self.op_readdir(out, share, dir, cookie, maxcount, &req)
            }
            OP_OPEN => self.op_open(r, out, ctx).await,
            OP_CLOSE => {
                let _seqid = xdr!(r.u32());
                let (sid_seq, sid_other) = xdr!(read_stateid(r));
                let other = self.resolve_stateid(ctx, sid_seq, &sid_other);
                let mut opens = self.state.opens.lock();
                match opens.remove(&other) {
                    Some(_) => {
                        drop(opens);
                        tracing::debug!(other = ?other, "close");
                        out.u32(op);
                        out.u32(NFS4_OK);
                        // Returned stateid: seqid bumped, other invalid-after-close.
                        out.u32(sid_seq.wrapping_add(1));
                        out.fixed(&other);
                        NFS4_OK
                    }
                    None => fail!(ERR_BAD_STATEID),
                }
            }
            OP_OPEN_DOWNGRADE => {
                // Args: od_open_stateid, od_seqid, access, deny — stateid FIRST (RFC 5661
                // §18.18), unlike CLOSE where the seqid comes first. Parsing them swapped
                // shifts the stateid by 4 bytes and every downgrade looks unknown.
                let (sid_seq, sid_other) = xdr!(read_stateid(r));
                let _seqid = xdr!(r.u32());
                let access = xdr!(r.u32());
                let _deny = xdr!(r.u32());
                let other = self.resolve_stateid(ctx, sid_seq, &sid_other);
                let mut opens = self.state.opens.lock();
                match opens.get_mut(&other) {
                    Some(os) => {
                        os.share_access = access & 3;
                        os.seqid = os.seqid.wrapping_add(1);
                        let seq = os.seqid;
                        drop(opens);
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.u32(seq);
                        out.fixed(&other);
                        NFS4_OK
                    }
                    None => {
                        tracing::debug!(
                            sid_seq,
                            other = ?other,
                            known = opens.len(),
                            "open_downgrade: unknown stateid"
                        );
                        drop(opens);
                        fail!(ERR_BAD_STATEID)
                    }
                }
            }
            OP_READ => {
                let (sid_seq, sid_other) = xdr!(read_stateid(r));
                let offset = xdr!(r.u64());
                let count = xdr!(r.u32()).min(MAX_READ);
                tracing::trace!(offset, count, "v41 read args");
                let (_share, node) = need_cfh!();
                // Perf experiment knobs, cached once (an env lookup per READ is measurable):
                // MIST_READ_DELAY_US adds reply latency (the client absorbs it by deepening its
                // readahead — throughput held constant in tests, proving the path is bandwidth-
                // not latency-bound); MIST_NULL_READ serves zeros to measure the framework floor.
                static DELAY_US: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
                static NULL_READ: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
                let delay = *DELAY_US.get_or_init(|| {
                    std::env::var("MIST_READ_DELAY_US")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0)
                });
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_micros(delay)).await;
                }
                if *NULL_READ
                    .get_or_init(|| std::env::var("MIST_NULL_READ").is_ok_and(|v| v == "1"))
                {
                    out.u32(op);
                    out.u32(NFS4_OK);
                    out.bool(false);
                    let len_pos = out.pos();
                    out.u32(count);
                    let payload = out.pos();
                    out.buf_mut().resize(payload + count as usize, 0);
                    let _ = len_pos;
                    return NFS4_OK;
                }
                if !self.stateid_ok(ctx, sid_seq, &sid_other) {
                    if self.stateid_is_stale(&sid_other) {
                        fail!(ERR_STALE_STATEID);
                    }
                    fail!(ERR_BAD_STATEID);
                }
                // Zero-copy tail: a CAS-hit READ as the compound's final op renders only the
                // head; the payload goes out via sendfile. (Not when the reply must be cached
                // for replay — the cache needs the bytes.)
                if ctx.is_last_op
                    && ctx.allow_sendfile
                    && let Some(body) = self.surface.read_sendable(node, offset, count).await
                {
                    out.u32(op);
                    out.u32(NFS4_OK);
                    out.bool(body.eof);
                    out.u32(body.len); // opaque length; payload follows out-of-band
                    ctx.tail_file = Some(body);
                    return NFS4_OK;
                }
                // Fused warm read: payload preads straight into the wire buffer (one copy
                // fewer + no intermediate allocation). Head first, then patch eof.
                {
                    let head = out.pos();
                    out.u32(op);
                    out.u32(NFS4_OK);
                    let eof_pos = out.pos();
                    out.bool(false);
                    let len_pos = out.pos();
                    out.u32(0);
                    let payload = out.pos();
                    match self
                        .surface
                        .read_into(node, offset, count, out.buf_mut())
                        .await
                    {
                        Some(Ok(eof)) => {
                            let n = (out.pos() - payload) as u32;
                            out.patch_u32(eof_pos, eof as u32);
                            out.patch_u32(len_pos, n);
                            out.pad_to_4();
                            return NFS4_OK;
                        }
                        Some(Err(e)) => {
                            out.truncate(head);
                            fail!(status_of(e));
                        }
                        None => out.truncate(head), // fall back to the copying path
                    }
                }
                match self.surface.read(node, offset, count).await {
                    Ok(res) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.bool(res.eof);
                        out.opaque(&res.data);
                        crate::bufpool::give(res.data);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_WRITE => {
                let (sid_seq, sid_other) = xdr!(read_stateid(r));
                let offset = xdr!(r.u64());
                let stable = xdr!(r.u32());
                tracing::trace!(offset, stable, "v41 write args");
                let data = xdr!(r.opaque(MAX_WRITE as usize + 16));
                let (_share, node) = need_cfh!();
                if !self.surface.writable() {
                    fail!(ERR_ROFS);
                }
                if !self.stateid_ok(ctx, sid_seq, &sid_other) {
                    if self.stateid_is_stale(&sid_other) {
                        fail!(ERR_STALE_STATEID);
                    }
                    fail!(ERR_BAD_STATEID);
                }
                // Writeback shares answer FILE_SYNC even to UNSTABLE writes (knfsd `async`
                // semantics): the client then never throttles the stream on COMMIT.
                let stable_reply = stable != 0 || self.surface.writes_are_stable();
                match self.surface.write(node, offset, data, stable != 0).await {
                    Ok(_attr) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.u32(data.len() as u32);
                        out.u32(if stable_reply { 2 } else { 0 }); // FILE_SYNC | UNSTABLE
                        out.fixed(&self.state.boot_verifier);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_COMMIT => {
                let offset = xdr!(r.u64());
                let count = xdr!(r.u32());
                let (_share, node) = need_cfh!();
                match self.surface.commit(node, offset, count as u64).await {
                    Ok(_) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        out.fixed(&self.state.boot_verifier);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_CREATE => {
                // Non-regular objects (dirs, symlinks, fifos…). Regular files come via OPEN.
                let objtype = xdr!(r.u32());
                let kind = match objtype {
                    2 => CreateKind::Dir,
                    5 => {
                        let target = xdr!(r.opaque(4096)).to_vec();
                        CreateKind::Symlink { target }
                    }
                    3 | 4 => {
                        let major = xdr!(r.u32());
                        let minor = xdr!(r.u32());
                        CreateKind::Device {
                            is_block: objtype == 3,
                            major,
                            minor,
                        }
                    }
                    6 => CreateKind::Socket,
                    7 => CreateKind::Fifo,
                    _ => fail!(10007), // NFS4ERR_BADTYPE
                };
                let name = xdr!(r.opaque(4096)).to_vec();
                let (sa, _bm) = xdr!(attrs::decode_settable(r));
                let (share, dir) = need_cfh!();
                if name.len() > 255 {
                    fail!(ERR_NAMETOOLONG);
                }
                let before = self.dir_change(dir);
                let mode = sa.mode.unwrap_or(0o755);
                match self.surface.create(dir, &name, kind, mode).await {
                    Ok((node, _attr)) => {
                        ctx.cfh = Some((share, node));
                        out.u32(op);
                        out.u32(NFS4_OK);
                        change_info(out, before, self.dir_change(dir));
                        let mut bm = Bitmap::default();
                        if sa.mode.is_some() {
                            bm.set(attrs::A_MODE);
                        }
                        bm.encode(out);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_REMOVE => {
                let name = xdr!(r.opaque(4096)).to_vec();
                let (_share, dir) = need_cfh!();
                if name.len() > 255 {
                    fail!(ERR_NAMETOOLONG);
                }
                let is_dir = self
                    .surface
                    .lookup(dir, &name)
                    .map(|(_, a)| matches!(a.kind, mist_proto::Kind::Dir))
                    .unwrap_or(false);
                let before = self.dir_change(dir);
                match self.surface.remove(dir, &name, is_dir).await {
                    Ok(()) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        change_info(out, before, self.dir_change(dir));
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_RENAME => {
                let old = xdr!(r.opaque(4096)).to_vec();
                let new = xdr!(r.opaque(4096)).to_vec();
                let Some((_, src_dir)) = ctx.sfh else {
                    fail!(ERR_NOFILEHANDLE);
                };
                let (_share, dst_dir) = need_cfh!();
                if old.len() > 255 || new.len() > 255 {
                    fail!(ERR_NAMETOOLONG);
                }
                let sb = self.dir_change(src_dir);
                let db = self.dir_change(dst_dir);
                match self.surface.rename(src_dir, &old, dst_dir, &new).await {
                    Ok(()) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        change_info(out, sb, self.dir_change(src_dir));
                        change_info(out, db, self.dir_change(dst_dir));
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_LINK => {
                let _name = xdr!(r.opaque(4096));
                // Hardlinks are NOTSUPP across the whole Mist surface (same as v3).
                fail!(ERR_NOTSUPP)
            }
            OP_SETATTR => {
                let (sid_seq, sid_other) = xdr!(read_stateid(r));
                let (sa, bm) = xdr!(attrs::decode_settable(r));
                let (_share, node) = need_cfh!();
                if !self.surface.writable() {
                    fail!(ERR_ROFS);
                }
                if sa.size.is_some() && !self.stateid_ok(ctx, sid_seq, &sid_other) {
                    fail!(ERR_BAD_STATEID);
                }
                match self.surface.setattr(node, sa).await {
                    Ok(_attr) => {
                        out.u32(op);
                        out.u32(NFS4_OK);
                        bm.encode(out);
                        NFS4_OK
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            OP_DELEGRETURN => {
                let (_sid_seq, sid_other) = xdr!(read_stateid(r));
                let st = self.delegreturn(&sid_other);
                out.u32(op);
                out.u32(st);
                st
            }
            OP_TEST_STATEID => {
                let n = xdr!(r.u32()) as usize;
                if n > 64 {
                    fail!(ERR_BADXDR);
                }
                let mut ids = Vec::with_capacity(n);
                for _ in 0..n {
                    ids.push(xdr!(read_stateid(r)));
                }
                out.u32(op);
                out.u32(NFS4_OK);
                out.u32(ids.len() as u32);
                for (_seq, other) in ids {
                    let known = self.state.opens.lock().contains_key(&other)
                        || self
                            .state
                            .delegations
                            .lock()
                            .values()
                            .any(|d| d.stateid_other == other);
                    out.u32(if known {
                        NFS4_OK
                    } else if self.stateid_is_stale(&other) {
                        ERR_STALE_STATEID
                    } else {
                        ERR_BAD_STATEID
                    });
                }
                NFS4_OK
            }
            OP_FREE_STATEID => {
                let (_seq, other) = xdr!(read_stateid(r));
                self.state.opens.lock().remove(&other);
                self.state.locks.lock().remove(&other);
                out.u32(op);
                out.u32(NFS4_OK);
                NFS4_OK
            }
            OP_LOCK => {
                // Always-grant (single loopback client: nothing to enforce server-side —
                // v3 `locallocks` parity; v4.1 clients require LOCK to succeed or they fail
                // the file with EIO).
                let _locktype = xdr!(r.u32());
                let _reclaim = xdr!(r.bool());
                let _offset = xdr!(r.u64());
                let _length = xdr!(r.u64());
                let new_owner = xdr!(r.bool());
                let (node, lock_other) = if new_owner {
                    let _open_seqid = xdr!(r.u32());
                    let (oseq, oother) = xdr!(read_stateid(r));
                    let _lock_seqid = xdr!(r.u32());
                    let _clientid = xdr!(r.u64());
                    let _owner = xdr!(r.opaque(1024));
                    let oother = self.resolve_stateid(ctx, oseq, &oother);
                    let Some(node) = self.state.opens.lock().get(&oother).map(|o| o.node) else {
                        fail!(ERR_BAD_STATEID);
                    };
                    let lo = self.state.new_other(3);
                    self.state.locks.lock().insert(lo, node);
                    (node, lo)
                } else {
                    let (_lseq, lother) = xdr!(read_stateid(r));
                    let _lock_seqid = xdr!(r.u32());
                    let Some(node) = self.state.locks.lock().get(&lother).copied() else {
                        fail!(ERR_BAD_STATEID);
                    };
                    (node, lother)
                };
                let _ = node;
                ctx.current_stateid = Some((lock_other, 1));
                out.u32(op);
                out.u32(NFS4_OK);
                out.u32(1); // lock stateid.seqid
                out.fixed(&lock_other);
                NFS4_OK
            }
            OP_LOCKT => {
                let _locktype = xdr!(r.u32());
                let _offset = xdr!(r.u64());
                let _length = xdr!(r.u64());
                let _clientid = xdr!(r.u64());
                let _owner = xdr!(r.opaque(1024));
                out.u32(op);
                out.u32(NFS4_OK); // never a conflict
                NFS4_OK
            }
            OP_LOCKU => {
                let _locktype = xdr!(r.u32());
                let _seqid = xdr!(r.u32());
                let (lseq, lother) = xdr!(read_stateid(r));
                let _offset = xdr!(r.u64());
                let _length = xdr!(r.u64());
                let lother = self.resolve_stateid(ctx, lseq, &lother);
                if !self.state.locks.lock().contains_key(&lother) {
                    fail!(ERR_BAD_STATEID);
                }
                out.u32(op);
                out.u32(NFS4_OK);
                out.u32(lseq.wrapping_add(1));
                out.fixed(&lother);
                NFS4_OK
            }
            OP_NVERIFY | OP_VERIFY => {
                // Attr comparison ops: decode and report NOTSUPP (macOS copes).
                let _bm = xdr!(Bitmap::decode(r));
                let _vals = xdr!(r.opaque(64 * 1024));
                fail!(ERR_NOTSUPP)
            }
            OP_ILLEGAL => fail!(ERR_OP_ILLEGAL),
            other => {
                tracing::debug!(op = other, "unsupported nfs41 op");
                out.u32(other);
                out.u32(ERR_NOTSUPP);
                ERR_NOTSUPP
            }
        }
    }

    fn op_readdir(
        self: &Arc<Self>,
        out: &mut XdrWriter,
        share: u16,
        dir: NodeKey,
        cookie: u64,
        maxcount: u32,
        req: &Bitmap,
    ) -> u32 {
        // v4 cookies 0/1/2 are reserved ("."/".." are NOT returned in v4).
        let page = match self.surface.readdir(dir, cookie, 4096, true) {
            Ok(p) => p,
            Err(e) => {
                out.u32(OP_READDIR);
                out.u32(status_of(e));
                return status_of(e);
            }
        };
        out.u32(OP_READDIR);
        out.u32(NFS4_OK);
        out.fixed(&page.cookieverf.to_be_bytes());
        let fsstat = self.surface.fsstat();
        let mut budget = (maxcount as usize).min(MAX_RECORD - 512);
        let mut wrote_all = true;
        for e in &page.entries {
            let mut ent = XdrWriter::new();
            ent.u64(e.cookie.max(3)); // never emit reserved cookies
            ent.opaque(&e.name);
            let attr = e.attr.clone().unwrap_or_default_attr();
            let fh = self.codec.encode(share, e.node);
            attrs::encode_fattr4(&mut ent, req, &attr, e.node.ino, share, Some(&fh), &fsstat);
            let bytes = ent.into_bytes();
            if bytes.len() + 8 > budget {
                wrote_all = false;
                break;
            }
            budget -= bytes.len() + 4;
            out.bool(true); // another entry follows
            out.fixed(&bytes);
        }
        out.bool(false); // end of entries
        out.bool(page.eof && wrote_all);
        NFS4_OK
    }

    async fn op_open(
        self: &Arc<Self>,
        r: &mut XdrReader<'_>,
        out: &mut XdrWriter,
        ctx: &mut Ctx,
    ) -> u32 {
        macro_rules! xdr {
            ($e:expr) => {
                match $e {
                    Ok(v) => v,
                    Err(_) => {
                        out.u32(OP_OPEN);
                        out.u32(ERR_BADXDR);
                        return ERR_BADXDR;
                    }
                }
            };
        }
        macro_rules! fail {
            ($st:expr) => {{
                out.u32(OP_OPEN);
                out.u32($st);
                return $st;
            }};
        }
        let _owner_seqid = xdr!(r.u32());
        let share_access = xdr!(r.u32());
        let _share_deny = xdr!(r.u32());
        let _clientid = xdr!(r.u64());
        let owner = xdr!(r.opaque(1024)).to_vec();
        let opentype = xdr!(r.u32());
        let mut createattrs: Option<crate::surface::SetAttr> = None;
        let mut exclusive = false;
        let mut guarded = false;
        if opentype == 1 {
            let how = xdr!(r.u32());
            match how {
                0 | 1 => {
                    guarded = how == 1;
                    let (sa, _) = xdr!(attrs::decode_settable(r));
                    createattrs = Some(sa);
                }
                2 => {
                    let _verf = xdr!(r.fixed(8));
                    exclusive = true;
                }
                3 => {
                    let _verf = xdr!(r.fixed(8));
                    let (sa, _) = xdr!(attrs::decode_settable(r));
                    createattrs = Some(sa);
                    exclusive = true;
                }
                _ => fail!(ERR_BADXDR),
            }
        }
        let claim = xdr!(r.u32());
        let Some((share, dirfh)) = ctx.cfh else {
            fail!(ERR_NOFILEHANDLE);
        };

        let (node, dir_before, dir_after, created) = match claim {
            0 => {
                // CLAIM_NULL: cfh is the directory; component name follows.
                let name = xdr!(r.opaque(4096)).to_vec();
                if name.len() > 255 {
                    fail!(ERR_NAMETOOLONG);
                }
                let before = self.dir_change(dirfh);
                match self.surface.lookup(dirfh, &name) {
                    Ok((node, attr)) => {
                        if matches!(attr.kind, mist_proto::Kind::Dir) {
                            fail!(ERR_ISDIR);
                        }
                        if opentype == 1 && guarded {
                            fail!(ERR_EXIST);
                        }
                        // UNCHECKED create on existing file with size=0 truncates.
                        if let Some(sa) = createattrs.filter(|s| s.size == Some(0)) {
                            if !self.surface.writable() {
                                fail!(ERR_ROFS);
                            }
                            if let Err(e) = self
                                .surface
                                .setattr(
                                    node,
                                    crate::surface::SetAttr {
                                        size: Some(0),
                                        ..sa
                                    },
                                )
                                .await
                            {
                                fail!(status_of(e));
                            }
                        }
                        (node, before, self.dir_change(dirfh), false)
                    }
                    Err(NfsError::NoEnt) if opentype == 1 => {
                        if !self.surface.writable() {
                            fail!(ERR_ROFS);
                        }
                        let mode = createattrs.as_ref().and_then(|s| s.mode).unwrap_or(0o644);
                        match self
                            .surface
                            .create(dirfh, &name, CreateKind::File { exclusive }, mode)
                            .await
                        {
                            Ok((node, _)) => (node, before, self.dir_change(dirfh), true),
                            Err(e) => fail!(status_of(e)),
                        }
                    }
                    Err(e) => fail!(status_of(e)),
                }
            }
            4 => {
                // CLAIM_FH: cfh is the file itself.
                (dirfh, 0, 0, false)
            }
            2 => {
                // CLAIM_DELEGATE_CUR: open under a held delegation; cfh is the dir, the args
                // carry the delegation stateid + component name.
                let (dseq, dother) = xdr!(read_stateid(r));
                let name = xdr!(r.opaque(4096)).to_vec();
                let _ = (dseq, dother);
                match self.surface.lookup(dirfh, &name) {
                    Ok((node, _)) => (node, 0, 0, false),
                    Err(e) => fail!(status_of(e)),
                }
            }
            5 => {
                // CLAIM_DELEG_CUR_FH: open under a held delegation; cfh IS the file, args carry
                // the delegation stateid. The client does this to convert a delegated open
                // (e.g. re-open for write) — rejecting it wedges fsx-style reopen loops.
                let (dseq, dother) = xdr!(read_stateid(r));
                let _ = (dseq, dother);
                (dirfh, 0, 0, false)
            }
            1 => {
                // CLAIM_PREVIOUS: reboot reclaim — cfh is the file, args carry the previous
                // delegation type. GRANT it like CLAIM_FH: replying NO_GRACE makes the macOS
                // client declare recovery failed and queue every stateful op FOREVER while
                // stateless ops keep working (found by the chaos suite's hostd-crash drill —
                // reads fine, creates wedge). knfsd survives because it runs a real grace
                // period; for a single loopback client whose handles are HMAC-verified,
                // accepting the reclaim is safe and unwedges recovery.
                let _prev_deleg_type = xdr!(r.u32());
                (dirfh, 0, 0, false)
            }
            6 => fail!(10033), // CLAIM_DELEG_PREV: we never grant across restarts → NO_GRACE
            _ => fail!(ERR_BADXDR),
        };

        // Register the open stateid. v4 open-owner semantics: a second OPEN by the same owner
        // on the same file returns the SAME stateid with a bumped seqid and unioned access —
        // minting a fresh one desyncs the client's stateid model (its OPEN_DOWNGRADE then
        // references a stateid we dropped → BAD_STATEID → client "reboot" recovery wedge).
        let (other, stateid_seq) = {
            let mut opens = self.state.opens.lock();
            let existing = opens
                .iter_mut()
                .find(|(_, os)| os.node == node && os.owner == owner);
            match existing {
                Some((o, os)) => {
                    os.seqid = os.seqid.wrapping_add(1);
                    os.share_access |= share_access & 3;
                    (*o, os.seqid)
                }
                None => {
                    let other = self.state.new_other(1);
                    opens.insert(
                        other,
                        state::OpenState {
                            node,
                            seqid: 1,
                            share_access: share_access & 3,
                            owner: owner.clone(),
                        },
                    );
                    (other, 1)
                }
            }
        };
        ctx.cfh = Some((share, node));
        ctx.current_stateid = Some((other, stateid_seq));
        tracing::debug!(other = ?other, stateid_seq, ino = node.ino, claim, "open granted");

        // Delegation policy (design 05 §5): grant READ on read-opens while under cap.
        let deleg = self.maybe_grant_delegation(node, share_access, created);

        out.u32(OP_OPEN);
        out.u32(NFS4_OK);
        out.u32(stateid_seq);
        out.fixed(&other);
        change_info(out, dir_before, dir_after);
        out.u32(0x4); // rflags: OPEN4_RESULT_PRESERVE_UNLINKED... (no CONFIRM in 4.1)
        let mut bm = Bitmap::default();
        if created {
            bm.set(attrs::A_MODE);
        }
        bm.encode(out);
        match deleg {
            Some(deleg_other) => {
                out.u32(1); // OPEN_DELEGATE_READ
                out.u32(1); // deleg stateid.seqid
                out.fixed(&deleg_other);
                out.bool(false); // recall not pending at grant
                // nfsace4 — permit-everyone placeholder ACE
                out.u32(0); // type ALLOW
                out.u32(0); // flag
                out.u32(0); // access mask (clients ignore for read delegs)
                out.opaque(b"EVERYONE@");
            }
            None => out.u32(0), // OPEN_DELEGATE_NONE
        }
        NFS4_OK
    }

    fn maybe_grant_delegation(
        self: &Arc<Self>,
        node: NodeKey,
        share_access: u32,
        created: bool,
    ) -> Option<[u8; 12]> {
        const DELEG_CAP: usize = 16384;
        // Read-only open of an existing file, no current delegation on the node.
        if created || share_access & 2 != 0 {
            return None;
        }
        let mut delegs = self.state.delegations.lock();
        if delegs.len() >= DELEG_CAP || delegs.contains_key(&node) {
            return None;
        }
        let other = self.state.new_other(2);
        delegs.insert(
            node,
            state::Delegation {
                stateid_other: other,
                node,
                recalling: false,
                granted_at: std::time::Instant::now(),
                recalled_at: None,
            },
        );
        self.state
            .deleg_stats
            .granted
            .fetch_add(1, Ordering::Relaxed);
        Some(other)
    }

    fn delegreturn(self: &Arc<Self>, other: &[u8; 12]) -> u32 {
        let mut delegs = self.state.delegations.lock();
        let Some(node) = delegs
            .iter()
            .find(|(_, d)| d.stateid_other == *other)
            .map(|(n, _)| *n)
        else {
            if self.stateid_is_stale(other) {
                return ERR_STALE_STATEID;
            }
            return ERR_BAD_STATEID;
        };
        let d = delegs.remove(&node).expect("present");
        drop(delegs);
        self.state
            .deleg_stats
            .returned
            .fetch_add(1, Ordering::Relaxed);
        if let Some(t) = d.recalled_at {
            let ns = t.elapsed().as_nanos() as u64;
            self.recall_metrics.returns.fetch_add(1, Ordering::Relaxed);
            self.recall_metrics
                .total_recall_ns
                .fetch_add(ns, Ordering::Relaxed);
            self.recall_metrics
                .max_recall_ns
                .fetch_max(ns, Ordering::Relaxed);
            let mut samples = self.recall_metrics.samples_ns.lock();
            if samples.len() < 100_000 {
                samples.push(ns);
            }
        }
        NFS4_OK
    }

    /// Journal-driven invalidation entry point: a guest-side change touched `node` — recall any
    /// read delegation on it via CB_RECALL on the bound backchannel.
    pub fn recall_node(self: &Arc<Self>, node: NodeKey) {
        let (other, sid) = {
            let mut delegs = self.state.delegations.lock();
            let Some(d) = delegs.get_mut(&node) else {
                return;
            };
            if d.recalling {
                return; // dedup: recall already in flight
            }
            d.recalling = true;
            d.recalled_at = Some(std::time::Instant::now());
            // Any live session's backchannel will do (single client).
            let sid = self.backchannels.lock().keys().next().copied();
            (d.stateid_other, sid)
        };
        let Some(sid) = sid else {
            // No backchannel: revoke immediately (client will recover via TEST_STATEID).
            self.state.delegations.lock().remove(&node);
            self.state
                .deleg_stats
                .revoked
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let me = self.clone();
        tokio::spawn(async move {
            me.send_cb_recall(sid, other, node).await;
        });
    }

    async fn send_cb_recall(self: Arc<Self>, sid: [u8; 16], other: [u8; 12], node: NodeKey) {
        let Some((bc, sess)) = ({
            let bcs = self.backchannels.lock();
            bcs.get(&sid).cloned().zip(self.state.session(&sid))
        }) else {
            return;
        };
        self.recall_metrics
            .recalls_sent
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .deleg_stats
            .recalls
            .fetch_add(1, Ordering::Relaxed);

        let xid = self.next_cb_xid.fetch_add(1, Ordering::Relaxed);
        let cb_seq = sess.back_slot_seqid.fetch_add(1, Ordering::Relaxed) + 1;
        let share = self.surface.share_id();
        let fh = self.codec.encode(share, node);

        let mut w = XdrWriter::new();
        w.u32(xid);
        w.u32(0); // CALL
        w.u32(2); // RPC version
        w.u32(bc.cb_program);
        w.u32(1); // CB program version
        w.u32(1); // CB_COMPOUND
        w.u32(0); // cred AUTH_NONE
        w.opaque(&[]);
        w.u32(0); // verf AUTH_NONE
        w.opaque(&[]);
        // CB_COMPOUND4args: tag, minorversion, callback_ident, ops
        w.opaque(b"recall");
        w.u32(1);
        w.u32(0);
        w.u32(2); // two ops
        w.u32(CB_OP_SEQUENCE);
        w.fixed(&sid);
        w.u32(cb_seq);
        w.u32(0); // slot 0
        w.u32(0); // highest slot
        w.bool(false); // cachethis
        w.u32(0); // no referring call lists
        w.u32(CB_OP_RECALL);
        w.u32(1); // stateid.seqid
        w.fixed(&other);
        w.bool(false); // truncate
        w.opaque(&fh);

        let (otx, orx) = oneshot::channel();
        bc.pending.lock().insert(xid, otx);
        if crate::server::write_reply(&bc.wr, crate::server::Reply::Whole(w.into_bytes()))
            .await
            .is_err()
        {
            bc.pending.lock().remove(&xid);
            return;
        }
        // 100 ms per-recall retry budget; 2 s total → revoke (design 05 §5).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        match tokio::time::timeout_at(deadline, orx).await {
            Ok(Ok(_status)) => {
                // CB delivered; DELEGRETURN arrives as a fore op and clears the table.
            }
            _ => {
                tracing::debug!(?node, "cb_recall reply missing");
            }
        }
        // Revoke if the client still hasn't returned after the budget.
        tokio::time::sleep_until(deadline).await;
        let mut delegs = self.state.delegations.lock();
        if let Some(d) = delegs.get(&node)
            && d.stateid_other == other
        {
            delegs.remove(&node);
            self.state
                .deleg_stats
                .revoked
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(?node, "delegation revoked (no DELEGRETURN within 2s)");
        }
    }

    /// A stateid minted by a PREVIOUS server instance (boot tag mismatch). Must be answered
    /// with NFS4ERR_STALE_STATEID, not BAD_STATEID: stale tells the macOS client "server
    /// rebooted, purge and re-open"; BAD_STATEID reads as a protocol error and wedges its
    /// recovery state machine with every stateful op queued (chaos-suite hostd-crash drill).
    fn stateid_is_stale(&self, other: &[u8; 12]) -> bool {
        other != &[0u8; 12]
            && other != &[0xffu8; 12]
            && other[9..12] != self.state.boot_verifier[..3]
    }

    /// Accept anonymous (all-0), bypass (all-1), the compound's current stateid, any known open
    /// stateid, or a delegation stateid.
    fn stateid_ok(&self, _ctx: &Ctx, seq: u32, other: &[u8; 12]) -> bool {
        if other == &[0u8; 12] {
            // seq 0 = anonymous; seq 1 = the compound's "current stateid" (must exist or we
            // permit anyway — I/O with special stateids is always safe for this server).
            return seq == 0 || seq == 1;
        }
        if other == &[0xffu8; 12] {
            return true;
        }
        if self.state.opens.lock().contains_key(other) {
            return true;
        }
        if self.state.locks.lock().contains_key(other) {
            return true;
        }
        self.state
            .delegations
            .lock()
            .values()
            .any(|d| d.stateid_other == *other)
    }

    /// Map the special "current stateid" (seq 1, other all-zero) to the compound's current one.
    fn resolve_stateid(&self, ctx: &Ctx, seq: u32, other: &[u8; 12]) -> [u8; 12] {
        if other == &[0u8; 12]
            && seq == 1
            && let Some((cur, _)) = ctx.current_stateid
        {
            return cur;
        }
        *other
    }

    fn dir_change(&self, dir: NodeKey) -> u64 {
        self.surface
            .getattr(dir)
            .map(|a| attrs::change_of(&a))
            .unwrap_or(0)
    }
}

enum SeqOutcome {
    Ok {
        sess: Arc<Session>,
        slot: u32,
        seqid: u32,
        cachethis: bool,
    },
    Replay(Vec<u8>),
    Err(u32),
}

// ---- small wire helpers -------------------------------------------------------------------------

struct ChanAttrs {
    max_req: u32,
}

fn read_chan_attrs(r: &mut XdrReader<'_>) -> Result<ChanAttrs, crate::xdr::XdrError> {
    let _headerpad = r.u32()?;
    let max_req = r.u32()?;
    let _max_resp = r.u32()?;
    let _max_resp_cached = r.u32()?;
    let _max_ops = r.u32()?;
    let _max_reqs = r.u32()?;
    let n_rdma = r.u32()?;
    for _ in 0..n_rdma {
        let _ = r.u32()?;
    }
    Ok(ChanAttrs { max_req })
}

fn write_chan_attrs(w: &mut XdrWriter, max_req: u32, slots: u32) {
    w.u32(0); // headerpad
    w.u32(max_req);
    w.u32(max_req); // max response
    w.u32(64 * 1024); // max cached response
    w.u32(MAX_OPS as u32);
    w.u32(slots);
    w.u32(0); // no rdma_ird
}

fn read_stateid(r: &mut XdrReader<'_>) -> Result<(u32, [u8; 12]), crate::xdr::XdrError> {
    let seq = r.u32()?;
    let other: [u8; 12] = r.fixed(12)?.try_into().unwrap();
    Ok((seq, other))
}

fn change_info(w: &mut XdrWriter, before: u64, after: u64) {
    // atomic=true: single-client loopback server — nothing else can interleave with the op,
    // so the client may update its cached directory in place. atomic=false made the macOS
    // client invalidate the whole dir on EVERY create and re-LOOKUP all siblings (measured:
    // a 30-file create loop issued 540 LOOKUPs + 1783 GETATTRs + 56 read-verifies).
    w.bool(true);
    w.u64(before);
    w.u64(after);
}

/// Best-effort: pull the compound status out of a CB reply (accepted, success).
fn parse_cb_reply_status(record: &[u8]) -> Option<u32> {
    let mut r = XdrReader::new(record);
    let _xid = r.u32().ok()?;
    let msg_type = r.u32().ok()?;
    if msg_type != 1 {
        return None;
    }
    let reply_stat = r.u32().ok()?;
    if reply_stat != 0 {
        return Some(ERR_IO);
    }
    let _verf_flavor = r.u32().ok()?;
    r.skip_opaque(1024).ok()?;
    let accept_stat = r.u32().ok()?;
    if accept_stat != 0 {
        return Some(ERR_IO);
    }
    r.u32().ok() // cb compound status
}

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

trait AttrOrDefault {
    fn unwrap_or_default_attr(self) -> mist_proto::Attr;
}

impl AttrOrDefault for Option<mist_proto::Attr> {
    fn unwrap_or_default_attr(self) -> mist_proto::Attr {
        self.unwrap_or(mist_proto::Attr {
            kind: mist_proto::Kind::Reg,
            mode: 0,
            nlink: 1,
            uid: 0,
            gid: 0,
            size: 0,
            blocks: 0,
            mtime: mist_proto::Ts { sec: 0, nsec: 0 },
            ctime: mist_proto::Ts { sec: 0, nsec: 0 },
            rdev: 0,
            content_version: 0,
            symlink_target: None,
        })
    }
}
