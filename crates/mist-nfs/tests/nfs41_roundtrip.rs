//! Drive the NFSv4.1 server over loopback TCP with a hand-rolled v4.1 client:
//! EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE → PUTROOTFH/GETFH/GETATTR → LOOKUP →
//! OPEN (expect a read delegation) → READ → journal-driven recall (CB_RECALL arrives on this
//! same connection) → DELEGRETURN → CLOSE. Validates wire-format self-consistency end-to-end
//! before the macOS kernel client probe.

use mist_nfs::{
    DirEntry, FsStat, MountSurface, Nfs41Server, NfsError, NfsResult, ReadDirPage, ReadFuture,
    ReadResult,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

type FakeNode = (Attr, Option<Vec<u8>>, Option<Vec<(String, u64)>>);

struct Fake {
    nodes: HashMap<u64, FakeNode>,
}

fn at(kind: Kind, size: u64) -> Attr {
    Attr {
        kind,
        mode: 0o644,
        nlink: 1,
        uid: 501,
        gid: 20,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts { sec: 7, nsec: 1 },
        ctime: Ts { sec: 7, nsec: 1 },
        rdev: 0,
        content_version: 1,
        symlink_target: None,
    }
}

impl Fake {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            2,
            (
                at(Kind::Dir, 4096),
                None,
                Some(vec![("hello.txt".into(), 10)]),
            ),
        );
        nodes.insert(
            10,
            (at(Kind::Reg, 12), Some(b"hello, mist\n".to_vec()), None),
        );
        Fake { nodes }
    }
    fn key(ino: u64) -> NodeKey {
        NodeKey { ino, generation: 1 }
    }
}

impl MountSurface for Fake {
    fn share_id(&self) -> u16 {
        0
    }
    fn root(&self) -> NodeKey {
        Fake::key(2)
    }
    fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
        self.nodes
            .get(&node.ino)
            .map(|(a, _, _)| a.clone())
            .ok_or(NfsError::Stale)
    }
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        let (_, _, children) = self.nodes.get(&dir.ino).ok_or(NfsError::Stale)?;
        let children = children.as_ref().ok_or(NfsError::NotDir)?;
        let name = std::str::from_utf8(name).map_err(|_| NfsError::NoEnt)?;
        for (n, ino) in children {
            if n == name {
                return Ok((Fake::key(*ino), self.getattr(Fake::key(*ino))?));
            }
        }
        Err(NfsError::NoEnt)
    }
    fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        max: usize,
        want_attrs: bool,
    ) -> NfsResult<ReadDirPage> {
        let (_, _, children) = self.nodes.get(&dir.ino).ok_or(NfsError::Stale)?;
        let children = children.as_ref().ok_or(NfsError::NotDir)?;
        let mut entries = Vec::new();
        for (i, (name, ino)) in children.iter().enumerate() {
            let c = (i + 3) as u64;
            if c <= cookie {
                continue;
            }
            if entries.len() >= max {
                return Ok(ReadDirPage {
                    entries,
                    eof: false,
                    cookieverf: 1,
                });
            }
            entries.push(DirEntry {
                name: name.clone().into_bytes(),
                node: Fake::key(*ino),
                cookie: c,
                attr: want_attrs.then(|| self.getattr(Fake::key(*ino)).unwrap()),
            });
        }
        Ok(ReadDirPage {
            entries,
            eof: true,
            cookieverf: 1,
        })
    }
    fn readlink(&self, _node: NodeKey) -> NfsResult<Vec<u8>> {
        Err(NfsError::NotSymlink)
    }
    fn read(&self, node: NodeKey, offset: u64, count: u32) -> ReadFuture<'_> {
        Box::pin(async move {
            let (_, data, _) = self.nodes.get(&node.ino).ok_or(NfsError::Stale)?;
            let data = data.as_ref().ok_or(NfsError::IsDir)?;
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            Ok(ReadResult {
                data: data[start..end].to_vec(),
                eof: end >= data.len(),
            })
        })
    }
    fn fsstat(&self) -> FsStat {
        FsStat::default()
    }
}

// ---- tiny v4.1 client ---------------------------------------------------------------------------

struct Xw(Vec<u8>);
impl Xw {
    fn new() -> Self {
        Xw(Vec::new())
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn opaque(&mut self, d: &[u8]) {
        self.u32(d.len() as u32);
        self.0.extend_from_slice(d);
        while !self.0.len().is_multiple_of(4) {
            self.0.push(0);
        }
    }
    fn fixed(&mut self, d: &[u8]) {
        self.0.extend_from_slice(d);
        while !self.0.len().is_multiple_of(4) {
            self.0.push(0);
        }
    }
}

struct Xr<'a>(&'a [u8], usize);
impl<'a> Xr<'a> {
    fn u32(&mut self) -> u32 {
        let v = u32::from_be_bytes(self.0[self.1..self.1 + 4].try_into().unwrap());
        self.1 += 4;
        v
    }
    fn u64(&mut self) -> u64 {
        let v = u64::from_be_bytes(self.0[self.1..self.1 + 8].try_into().unwrap());
        self.1 += 8;
        v
    }
    fn opaque(&mut self) -> &'a [u8] {
        let n = self.u32() as usize;
        let d = &self.0[self.1..self.1 + n];
        self.1 += n + (4 - n % 4) % 4;
        d
    }
    fn fixed(&mut self, n: usize) -> &'a [u8] {
        let d = &self.0[self.1..self.1 + n];
        self.1 += n + (4 - n % 4) % 4;
        d
    }
}

async fn send_record(s: &mut TcpStream, payload: &[u8]) {
    let marker = 0x8000_0000u32 | payload.len() as u32;
    s.write_all(&marker.to_be_bytes()).await.unwrap();
    s.write_all(payload).await.unwrap();
}

async fn recv_record(s: &mut TcpStream) -> Vec<u8> {
    let mut hdr = [0u8; 4];
    s.read_exact(&mut hdr).await.unwrap();
    let marker = u32::from_be_bytes(hdr);
    assert!(marker & 0x8000_0000 != 0);
    let mut buf = vec![0u8; (marker & 0x7FFF_FFFF) as usize];
    s.read_exact(&mut buf).await.unwrap();
    buf
}

/// Build a COMPOUND call; `ops` is the encoded op list, `nops` its count.
fn compound(xid: u32, nops: u32, ops: &[u8]) -> Vec<u8> {
    let mut w = Xw::new();
    w.u32(xid);
    w.u32(0); // CALL
    w.u32(2);
    w.u32(100003);
    w.u32(4);
    w.u32(1); // COMPOUND
    w.u32(0);
    w.opaque(&[]); // cred AUTH_NONE
    w.u32(0);
    w.opaque(&[]); // verf
    w.opaque(b"t"); // tag
    w.u32(1); // minor
    w.u32(nops);
    w.fixed(ops);
    w.0
}

/// Parse a COMPOUND reply: returns (status, results reader start after numres).
fn parse_reply(rec: &[u8]) -> (u32, Xr<'_>) {
    let mut r = Xr(rec, 0);
    let _xid = r.u32();
    assert_eq!(r.u32(), 1, "reply");
    assert_eq!(r.u32(), 0, "accepted");
    let _vf = r.u32();
    let _ = r.opaque();
    assert_eq!(r.u32(), 0, "accept success");
    let status = r.u32();
    let _tag = r.opaque();
    let _numres = r.u32();
    (status, r)
}

fn seq_op(w: &mut Xw, sid: &[u8; 16], seqid: u32, slot: u32, cachethis: bool) {
    w.u32(53);
    w.fixed(sid);
    w.u32(seqid);
    w.u32(slot);
    w.u32(0);
    w.u32(if cachethis { 1 } else { 0 });
}

#[tokio::test]
async fn v41_session_open_delegation_recall() {
    let surface = Arc::new(Fake::new());
    let server = Arc::new(Nfs41Server::new(surface, b"test-secret"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let metrics = server.recall_metrics.clone();
    let recall_handle = server.clone();
    tokio::spawn(server.serve(listener));

    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // EXCHANGE_ID
    let mut ops = Xw::new();
    ops.u32(42);
    ops.fixed(&[9u8; 8]); // verifier
    ops.opaque(b"test-client");
    ops.u32(0); // flags
    ops.u32(0); // SP4_NONE
    ops.u32(0); // no impl id
    send_record(&mut s, &compound(1, 1, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0, "EXCHANGE_ID failed");
    assert_eq!(r.u32(), 42);
    assert_eq!(r.u32(), 0);
    let clientid = r.u64();
    let _eir_seq = r.u32();

    // CREATE_SESSION
    let mut ops = Xw::new();
    ops.u32(43);
    ops.u64(clientid);
    ops.u32(1); // csa_sequence
    ops.u32(1); // CONN_BACK_CHAN
    for _ in 0..2 {
        // fore + back chan attrs
        ops.u32(0);
        ops.u32(1 << 20);
        ops.u32(1 << 20);
        ops.u32(64 << 10);
        ops.u32(16);
        ops.u32(64);
        ops.u32(0);
    }
    ops.u32(0x40000000); // cb_program
    ops.u32(1); // one sec_parm
    ops.u32(0); // AUTH_NONE
    send_record(&mut s, &compound(2, 1, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0, "CREATE_SESSION failed");
    assert_eq!(r.u32(), 43);
    assert_eq!(r.u32(), 0);
    let sid: [u8; 16] = r.fixed(16).try_into().unwrap();

    // SEQUENCE + RECLAIM_COMPLETE
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 1, 0, false);
    ops.u32(58);
    ops.u32(0); // rca_one_fs = false
    send_record(&mut s, &compound(3, 2, &ops.0)).await;
    let (st, _) = parse_reply(&recv_record(&mut s).await);
    assert_eq!(st, 0, "RECLAIM_COMPLETE failed");

    // SEQUENCE + PUTROOTFH + GETFH + GETATTR(type,change,size)
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 2, 0, false);
    ops.u32(24); // PUTROOTFH
    ops.u32(10); // GETFH
    ops.u32(9); // GETATTR
    ops.u32(2); // bitmap len
    ops.u32((1 << 1) | (1 << 3) | (1 << 4)); // type|change|size
    ops.u32(0);
    send_record(&mut s, &compound(4, 4, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0, "mount-path compound failed");
    // skip SEQUENCE result
    assert_eq!(r.u32(), 53);
    assert_eq!(r.u32(), 0);
    r.fixed(16);
    r.u32();
    r.u32();
    r.u32();
    r.u32();
    r.u32();
    assert_eq!(r.u32(), 24); // PUTROOTFH
    assert_eq!(r.u32(), 0);
    assert_eq!(r.u32(), 10); // GETFH
    assert_eq!(r.u32(), 0);
    let rootfh = r.opaque().to_vec();
    assert!(!rootfh.is_empty());
    assert_eq!(r.u32(), 9); // GETATTR
    assert_eq!(r.u32(), 0);
    let bm_len = r.u32();
    for _ in 0..bm_len {
        r.u32();
    }
    let vals = r.opaque();
    let mut vr = Xr(vals, 0);
    assert_eq!(vr.u32(), 2, "root is a directory");

    // SEQUENCE + PUTFH(root) + LOOKUP(hello.txt) + GETFH + OPEN(read) + READ
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 3, 0, false);
    ops.u32(22); // PUTFH
    ops.opaque(&rootfh);
    ops.u32(15); // LOOKUP
    ops.opaque(b"hello.txt");
    ops.u32(10); // GETFH
    // OPEN: CLAIM_FH on the looked-up file
    ops.u32(18);
    ops.u32(0); // owner seqid
    ops.u32(1); // share_access READ
    ops.u32(0); // deny
    ops.u64(clientid);
    ops.opaque(b"owner-1");
    ops.u32(0); // NOCREATE
    ops.u32(4); // CLAIM_FH
    // READ with the anonymous stateid
    ops.u32(25);
    ops.u32(0);
    ops.fixed(&[0u8; 12]);
    ops.u64(0);
    ops.u32(1024);
    send_record(&mut s, &compound(5, 6, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0, "open compound failed");
    // skip SEQUENCE
    assert_eq!(r.u32(), 53);
    assert_eq!(r.u32(), 0);
    r.fixed(16);
    for _ in 0..5 {
        r.u32();
    }
    assert_eq!(r.u32(), 22);
    assert_eq!(r.u32(), 0);
    assert_eq!(r.u32(), 15);
    assert_eq!(r.u32(), 0);
    assert_eq!(r.u32(), 10);
    assert_eq!(r.u32(), 0);
    let filefh = r.opaque().to_vec();
    assert_eq!(r.u32(), 18); // OPEN
    assert_eq!(r.u32(), 0);
    let _open_seq = r.u32();
    let _open_other = r.fixed(12).to_vec();
    r.u32(); // atomic
    r.u64();
    r.u64(); // change_info
    let _rflags = r.u32();
    let bml = r.u32();
    for _ in 0..bml {
        r.u32();
    }
    let deleg_type = r.u32();
    assert_eq!(deleg_type, 1, "read delegation granted");
    let _dseq = r.u32();
    let deleg_other: [u8; 12] = r.fixed(12).try_into().unwrap();
    let _recall_flag = r.u32();
    r.u32();
    r.u32();
    r.u32();
    let _who = r.opaque();
    assert_eq!(r.u32(), 25); // READ
    assert_eq!(r.u32(), 0);
    let eof = r.u32();
    let data = r.opaque();
    assert_eq!(data, b"hello, mist\n");
    assert_eq!(eof, 1);

    // Journal-driven recall: server should emit CB_COMPOUND(CB_SEQUENCE+CB_RECALL) on THIS conn.
    recall_handle.recall_node(Fake::key(10));
    let cb = recv_record(&mut s).await;
    let mut cr = Xr(&cb, 0);
    let cb_xid = cr.u32();
    assert_eq!(cr.u32(), 0, "backchannel message is a CALL");
    assert_eq!(cr.u32(), 2);
    assert_eq!(cr.u32(), 0x40000000, "cb_program");
    assert_eq!(cr.u32(), 1);
    assert_eq!(cr.u32(), 1, "CB_COMPOUND");
    cr.u32();
    cr.opaque(); // cred
    cr.u32();
    cr.opaque(); // verf
    let _tag = cr.opaque();
    assert_eq!(cr.u32(), 1, "minor");
    cr.u32(); // ident
    assert_eq!(cr.u32(), 2, "two cb ops");
    assert_eq!(cr.u32(), 11, "CB_SEQUENCE");
    cr.fixed(16);
    cr.u32();
    cr.u32();
    cr.u32();
    cr.u32();
    assert_eq!(cr.u32(), 0, "no referring lists");
    assert_eq!(cr.u32(), 4, "CB_RECALL");
    let _sid_seq = cr.u32();
    let recalled: [u8; 12] = cr.fixed(12).try_into().unwrap();
    assert_eq!(recalled, deleg_other, "recalled the granted delegation");
    let _trunc = cr.u32();
    let recalled_fh = cr.opaque();
    assert_eq!(recalled_fh, filefh);

    // Reply to the CB call (accepted, CB_COMPOUND ok), then DELEGRETURN.
    let mut w = Xw::new();
    w.u32(cb_xid);
    w.u32(1); // REPLY
    w.u32(0); // accepted
    w.u32(0);
    w.opaque(&[]); // verf
    w.u32(0); // success
    w.u32(0); // cb status OK
    w.opaque(b""); // tag
    w.u32(1); // one result
    w.u32(11); // CB_SEQUENCE
    w.u32(0);
    send_record(&mut s, &w.0).await;

    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 4, 0, false);
    ops.u32(22);
    ops.opaque(&filefh);
    ops.u32(8); // DELEGRETURN
    ops.u32(1);
    ops.fixed(&deleg_other);
    send_record(&mut s, &compound(6, 3, &ops.0)).await;
    let (st, _) = parse_reply(&recv_record(&mut s).await);
    assert_eq!(st, 0, "DELEGRETURN failed");

    // Recall metrics recorded a completed recall.
    assert_eq!(
        metrics
            .recalls_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics.returns.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let ns = metrics
        .total_recall_ns
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(ns > 0, "recall latency recorded");
    println!("recall latency: {} µs", ns / 1000);
}

#[tokio::test]
async fn v41_sequence_replay_returns_cached_reply() {
    let surface = Arc::new(Fake::new());
    let server = Arc::new(Nfs41Server::new(surface, b"test-secret"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(server.serve(listener));
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let mut ops = Xw::new();
    ops.u32(42);
    ops.fixed(&[1u8; 8]);
    ops.opaque(b"c2");
    ops.u32(0);
    ops.u32(0);
    ops.u32(0);
    send_record(&mut s, &compound(1, 1, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0);
    r.u32();
    r.u32();
    let clientid = r.u64();

    let mut ops = Xw::new();
    ops.u32(43);
    ops.u64(clientid);
    ops.u32(1);
    ops.u32(0);
    for _ in 0..2 {
        ops.u32(0);
        ops.u32(1 << 20);
        ops.u32(1 << 20);
        ops.u32(64 << 10);
        ops.u32(16);
        ops.u32(64);
        ops.u32(0);
    }
    ops.u32(0x40000000);
    ops.u32(1);
    ops.u32(0);
    send_record(&mut s, &compound(2, 1, &ops.0)).await;
    let rec = recv_record(&mut s).await;
    let (st, mut r) = parse_reply(&rec);
    assert_eq!(st, 0);
    r.u32();
    r.u32();
    let sid: [u8; 16] = r.fixed(16).try_into().unwrap();

    // Cached compound (cachethis = true).
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 1, 0, true);
    ops.u32(24); // PUTROOTFH
    ops.u32(10); // GETFH
    send_record(&mut s, &compound(3, 3, &ops.0)).await;
    let first = recv_record(&mut s).await;
    let (st, _) = parse_reply(&first);
    assert_eq!(st, 0);

    // Same slot, same seqid → byte-identical replay (modulo xid which we keep equal too).
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 1, 0, true);
    ops.u32(24);
    ops.u32(10);
    send_record(&mut s, &compound(3, 3, &ops.0)).await;
    let second = recv_record(&mut s).await;
    assert_eq!(first, second, "replay must return the cached reply");

    // Misordered seqid (jump by 2) → SEQ_MISORDERED.
    let mut ops = Xw::new();
    seq_op(&mut ops, &sid, 4, 0, false);
    ops.u32(24);
    send_record(&mut s, &compound(4, 2, &ops.0)).await;
    let (st, _) = parse_reply(&recv_record(&mut s).await);
    assert_eq!(st, 10063, "SEQ_MISORDERED");
}
