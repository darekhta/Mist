//! End-to-end on macOS/Linux without a VM: an in-process fake mistd over UDS.
//!
//! Exercises the full host path: dial → Hello/auth → lanes → snapshot stream → replica swap →
//! replica reads → Read RPC with MORE-chained bulk data.

use mist_proto::{
    Attr, CtlMsg, EventMsg, FLAG_MORE, FrameKind, Kind, Name, NodeKey, PROTO_VERSION, RpcReq,
    RpcResp, ShareId, ShareInfo, SnapDir, SnapDone, SnapEntry, Ts, encode,
};
use mist_replica::ShareState;
use mist_transport::classify_accepted;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::sync::{Mutex, mpsc};

const TOKEN: &[u8] = b"test-token-bytes-0123456789abcdef";
const CHUNK: usize = 64 * 1024;

fn key(ino: u64) -> NodeKey {
    NodeKey { ino, generation: 7 }
}

fn attr(kind: Kind, size: u64) -> Attr {
    Attr {
        kind,
        mode: if kind == Kind::Dir { 0o755 } else { 0o644 },
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts {
            sec: 1_750_000_000,
            nsec: 0,
        },
        ctime: Ts {
            sec: 1_750_000_000,
            nsec: 0,
        },
        rdev: 0,
        content_version: 1,
        symlink_target: None,
    }
}

enum BulkItem {
    Event(Vec<u8>),
    Raw {
        seq: u64,
        more: bool,
        payload: Vec<u8>,
    },
}

type BulkTxs = Arc<Mutex<Vec<mpsc::Sender<BulkItem>>>>;
type JournalTx = Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>;

struct FakeFs {
    dirs: Vec<SnapDir>,
    files: HashMap<NodeKey, Vec<u8>>,
}

/// Synthetic tree: / { src/{lib.rs, sub/{deep.txt}}, README.md }.
fn fake_fs(share: ShareId) -> FakeFs {
    let (root, src, sub) = (key(2), key(10), key(11));
    let (lib, readme, deep) = (key(20), key(21), key(22));

    let lib_data = b"pub fn hello() {}\n".repeat(40_000); // ~720 KB: spans many bulk chunks
    let readme_data = b"# Mist test tree\n".to_vec();
    let deep_data = vec![0xA5u8; 3];

    let e = |n: &str, k: NodeKey, a: Attr| SnapEntry {
        name: Name::new(n.as_bytes().to_vec()).unwrap(),
        node: k,
        attr: a,
    };
    let d = |dir, parent, entries| SnapDir {
        snap_id: 0, // patched per request
        share,
        dir,
        dir_attr: attr(Kind::Dir, 4096),
        parent,
        entries,
        last: true,
    };

    FakeFs {
        dirs: vec![
            d(
                root,
                root,
                vec![
                    e("src", src, attr(Kind::Dir, 4096)),
                    e(
                        "README.md",
                        readme,
                        attr(Kind::Reg, readme_data.len() as u64),
                    ),
                ],
            ),
            // Deliberately out of order: a subdir's record precedes its parent's listing.
            d(sub, src, vec![e("deep.txt", deep, attr(Kind::Reg, 3))]),
            d(
                src,
                root,
                vec![
                    e("lib.rs", lib, attr(Kind::Reg, lib_data.len() as u64)),
                    e("sub", sub, attr(Kind::Dir, 4096)),
                ],
            ),
        ],
        files: HashMap::from([(lib, lib_data), (readme, readme_data), (deep, deep_data)]),
    }
}

/// A single-record journal batch creating a regular file under `parent`.
fn journal_created(first_seq: u64, parent: u64, nm: &str, node: u64) -> mist_proto::JournalBatch {
    mist_proto::JournalBatch {
        share: ShareId(0),
        first_seq,
        guest_mono_ns: 0,
        records: vec![mist_proto::Rec::Created {
            parent: key(parent),
            name: Name::new(nm.as_bytes().to_vec()).unwrap(),
            node: key(node),
            attr: Some(attr(Kind::Reg, 7)),
        }],
    }
}

/// Minimal mistd: Hello/auth, snapshot on request, Stat/Read RPCs, MORE-chained reads.
async fn fake_guest(listener: UnixListener, epoch: u64) {
    let share_info = ShareInfo {
        id: ShareId(0),
        name: "code".into(),
        epoch,
        fsid: 0xF5,
        root: key(2),
        flags: 0,
        ino_bits: 32,
    };
    let session_id: u64 = 0x5E55;
    let bulk_txs: BulkTxs = Arc::default();
    let journal_tx: JournalTx = Arc::default();
    let fs = Arc::new(fake_fs(share_info.id));

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok((mut framed, first)) = classify_accepted(Box::new(stream)).await else {
            continue;
        };
        match first {
            CtlMsg::Hello { token_hash, .. } => {
                if blake3::hash(TOKEN).as_bytes() != &token_hash {
                    let _ = framed.send_msg(FrameKind::Ctl, 0, &CtlMsg::AuthFail).await;
                    continue;
                }
                let ack = CtlMsg::HelloAck {
                    proto: PROTO_VERSION,
                    features: mist_proto::features::SUPPORTED,
                    boot_id: epoch,
                    session_id,
                    shares: vec![share_info.clone()],
                    guest: mist_proto::GuestInfo {
                        kernel: "6.12-test".into(),
                        fanotify_max_queued: 16384,
                        mistd_pid: 1,
                    },
                };
                framed.send_msg(FrameKind::Ctl, 0, &ack).await.unwrap();
                tokio::spawn(ctl_loop(
                    framed,
                    bulk_txs.clone(),
                    journal_tx.clone(),
                    fs.clone(),
                ));
            }
            CtlMsg::StreamHello { lane, .. } => match lane {
                mist_proto::Lane::Bulk => {
                    let (tx, mut rx) = mpsc::channel::<BulkItem>(64);
                    bulk_txs.lock().await.push(tx);
                    tokio::spawn(async move {
                        while let Some(item) = rx.recv().await {
                            let r = match item {
                                BulkItem::Event(p) => {
                                    framed.send_frame(FrameKind::Event, 0, 0, &p).await
                                }
                                BulkItem::Raw { seq, more, payload } => {
                                    let flags = if more { FLAG_MORE } else { 0 };
                                    framed
                                        .send_frame(FrameKind::Bulk, flags, seq, &payload)
                                        .await
                                }
                            };
                            if r.is_err() {
                                return;
                            }
                        }
                    });
                }
                mist_proto::Lane::Rpc => {
                    tokio::spawn(rpc_loop(framed, bulk_txs.clone(), fs.clone()));
                }
                mist_proto::Lane::Journal => {
                    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64);
                    *journal_tx.lock().await = Some(tx);
                    tokio::spawn(async move {
                        while let Some(payload) = rx.recv().await {
                            if framed
                                .send_frame(FrameKind::Event, 0, 0, &payload)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    });
                }
                _ => {
                    tokio::spawn(async move {
                        let mut framed = framed;
                        while framed.recv().await.is_ok() {}
                    });
                }
            },
            other => panic!("unexpected first message {other:?}"),
        }
    }
}

async fn ctl_loop(
    mut framed: mist_transport::FramedStream,
    bulks: BulkTxs,
    journal: JournalTx,
    fs: Arc<FakeFs>,
) {
    loop {
        let Ok(f) = framed.recv().await else { return };
        let Ok(msg) = mist_proto::decode::<CtlMsg>(&f.payload) else {
            return;
        };
        match msg {
            CtlMsg::Ping { nonce } => {
                let pong = CtlMsg::Pong {
                    nonce,
                    guest_mono_ns: 0,
                };
                if framed.send_msg(FrameKind::Ctl, f.seq, &pong).await.is_err() {
                    return;
                }
            }
            CtlMsg::SnapshotStart { snap_id, .. } => {
                let tx = bulks.lock().await.first().cloned();
                let Some(tx) = tx else { continue };
                for d in &fs.dirs {
                    let mut d = d.clone();
                    d.snap_id = snap_id;
                    tx.send(BulkItem::Event(encode(&EventMsg::SnapDir(d))))
                        .await
                        .ok();
                }
                // Consistent-cut exercise: a journal Created arrives *during* the snapshot
                // (before SnapDone) — the host must buffer and replay it onto the fresh replica.
                if let Some(jtx) = journal.lock().await.clone() {
                    let batch = journal_created(1, 2, "during.txt", 50);
                    jtx.send(encode(&EventMsg::Journal(batch))).await.ok();
                }
                let done = SnapDone {
                    snap_id,
                    share: ShareId(0),
                    dirs: fs.dirs.len() as u64,
                    entries: 5,
                    errors: 0,
                };
                tx.send(BulkItem::Event(encode(&EventMsg::SnapDone(done))))
                    .await
                    .ok();

                // A live journal Created after the seed swaps in — applied directly. seq must be
                // contiguous with the buffered batch (first_seq 1, len 1 → next 2).
                if let Some(jtx) = journal.lock().await.clone() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    let batch = journal_created(2, 2, "live.txt", 51);
                    jtx.send(encode(&EventMsg::Journal(batch))).await.ok();
                }
            }
            _ => {}
        }
    }
}

async fn rpc_loop(mut framed: mist_transport::FramedStream, bulks: BulkTxs, fs: Arc<FakeFs>) {
    loop {
        let Ok(f) = framed.recv().await else { return };
        if f.kind != FrameKind::Req {
            continue;
        }
        let Ok(req) = mist_proto::decode::<RpcReq>(&f.payload) else {
            return;
        };
        match req {
            RpcReq::Read { node, off, len, .. } => match fs.files.get(&node) {
                Some(data) => {
                    let start = (off as usize).min(data.len());
                    let end = data.len().min(start + len as usize);
                    let body = data[start..end].to_vec();
                    let header = RpcResp::ReadStart {
                        version: 1,
                        len: body.len() as u64,
                        eof: end >= data.len(),
                    };
                    framed.send_msg(FrameKind::Resp, f.seq, &header).await.ok();
                    if !body.is_empty() {
                        let tx = bulks.lock().await.first().cloned();
                        if let Some(tx) = tx {
                            let mut sent = 0usize;
                            while sent < body.len() {
                                let n = CHUNK.min(body.len() - sent);
                                let more = sent + n < body.len();
                                tx.send(BulkItem::Raw {
                                    seq: f.seq,
                                    more,
                                    payload: body[sent..sent + n].to_vec(),
                                })
                                .await
                                .ok();
                                sent += n;
                            }
                        }
                    }
                }
                None => {
                    let resp = RpcResp::Err(mist_proto::RpcErr {
                        errno: 2,
                        msg: "no such file".into(),
                    });
                    framed.send_msg(FrameKind::Resp, f.seq, &resp).await.ok();
                }
            },
            RpcReq::Stat { node, .. } => {
                let resp = match fs.files.get(&node) {
                    Some(d) => RpcResp::Attr(attr(Kind::Reg, d.len() as u64)),
                    None => RpcResp::Attr(attr(Kind::Dir, 4096)),
                };
                framed.send_msg(FrameKind::Resp, f.seq, &resp).await.ok();
            }
            _ => {
                let resp = RpcResp::Err(mist_proto::RpcErr {
                    errno: 38,
                    msg: "not implemented in fake".into(),
                });
                framed.send_msg(FrameKind::Resp, f.seq, &resp).await.ok();
            }
        }
    }
}

#[tokio::test]
async fn full_session_seed_ls_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("guest.sock");
    let token_path = dir.path().join("token");
    std::fs::write(&token_path, TOKEN).unwrap();

    let listener = UnixListener::bind(&sock).unwrap();
    tokio::spawn(fake_guest(listener, 0xE10C));

    let vm = mist_hostd::session::VmHandle::new(
        "dev".into(),
        &mist_hostd::config::VmConfig {
            bridge: format!("uds:{}", sock.display()),
            token: token_path,
            vm_uuid: None,
            autoattach: true,
            automount: false,
        },
        Some(mist_cas::CasConfig::new(dir.path().join("cas"), 1 << 30)),
    )
    .unwrap();
    tokio::spawn(vm.clone().supervise());

    let share = wait_for_share(&vm, "code").await;
    let replica = wait_live(&share).await;

    // Snapshot tree shape.
    let (root, root_attr) = replica.resolve_path("/").unwrap();
    assert_eq!(root_attr.kind, Kind::Dir);
    assert_eq!(replica.resolve_path("/src").unwrap().1.kind, Kind::Dir);
    assert_eq!(
        replica.resolve_path("/README.md").unwrap().1.kind,
        Kind::Reg
    );
    let (deep, deep_attr) = replica.resolve_path("/src/sub/deep.txt").unwrap();
    assert_eq!(deep_attr.size, 3);
    assert_eq!(
        deep,
        NodeKey {
            ino: 22,
            generation: 7
        }
    );

    // Consistent cut: the journal Created that arrived *during* the snapshot was buffered and
    // replayed onto the fresh replica, so it's visible the moment the share goes Live.
    assert_eq!(
        replica
            .resolve_path("/during.txt")
            .expect("during-seed journal record applied")
            .0,
        key(50)
    );

    // Live journal apply: poll for the record sent after the seed swap.
    let live = wait_for_path(&share, "/live.txt").await;
    assert_eq!(live, key(51), "live journal record applied");
    let _ = root;

    // Read RPC with multi-chunk bulk streaming.
    let rpc = vm.rpc().expect("rpc up");
    let (lib_node, lib_attr) = replica.resolve_path("/src/lib.rs").unwrap();
    let (bytes, eof) = rpc
        .read(ShareId(0), lib_node, 0, lib_attr.size as u32)
        .await
        .expect("read ok");
    assert!(eof);
    assert_eq!(bytes.len(), lib_attr.size as usize);
    assert!(bytes.starts_with(b"pub fn hello()"));

    // Ranged read.
    let (bytes, eof) = rpc
        .read(ShareId(0), lib_node, 4, 10)
        .await
        .expect("ranged read");
    assert_eq!(bytes, b"fn hello()".to_vec());
    assert!(!eof);

    // Missing file surfaces the guest errno.
    let err = rpc.read(ShareId(0), key(999), 0, 16).await.unwrap_err();
    assert!(err.to_string().contains("errno 2"), "got: {err}");
}

async fn wait_for_share(
    vm: &Arc<mist_hostd::session::VmHandle>,
    name: &str,
) -> Arc<mist_hostd::session::ShareHandle> {
    for _ in 0..200 {
        if let Some(s) = vm.share(name) {
            return s;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("share {name:?} never appeared");
}

async fn wait_for_path(share: &Arc<mist_hostd::session::ShareHandle>, path: &str) -> NodeKey {
    for _ in 0..200 {
        if let Ok((n, _)) = share.replica().resolve_path(path) {
            return n;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("path {path} never appeared");
}

async fn wait_live(
    share: &Arc<mist_hostd::session::ShareHandle>,
) -> Arc<mist_replica::ShareReplica> {
    for _ in 0..200 {
        let r = share.replica();
        if r.state() == ShareState::Live {
            return r;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("share never went live");
}
