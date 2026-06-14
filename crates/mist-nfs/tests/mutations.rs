//! Drive the NFS server's mutation procedures over real TCP with a hand-rolled client and a
//! writable in-memory surface: CREATE → WRITE → READ-back → SETATTR(truncate) → REMOVE.
//! Validates the mutation wire format (sattr3, createhow3, write/wcc replies) end-to-end.

use mist_nfs::{
    CreateKind, FsStat, MountSurface, MutFuture, NfsError, NfsResult, NfsServer, ReadDirPage,
    ReadFuture, ReadResult, SetAttr,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Node {
    attr: Attr,
    data: Vec<u8>,
    children: HashMap<Vec<u8>, u64>, // dir only
}

struct Writable {
    nodes: Mutex<HashMap<u64, Node>>,
    next: Mutex<u64>,
}

fn dir_attr(ino: u64) -> Attr {
    file_attr(ino, 0, Kind::Dir)
}
fn file_attr(ino: u64, size: u64, kind: Kind) -> Attr {
    Attr {
        kind,
        mode: if kind == Kind::Dir { 0o755 } else { 0o644 },
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts { sec: 1, nsec: 0 },
        ctime: Ts { sec: 1, nsec: 0 },
        rdev: 0,
        content_version: ino,
        symlink_target: None,
    }
}

impl Writable {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            2,
            Node {
                attr: dir_attr(2),
                data: vec![],
                children: HashMap::new(),
            },
        );
        Writable {
            nodes: Mutex::new(nodes),
            next: Mutex::new(100),
        }
    }
    fn fresh(&self) -> u64 {
        let mut n = self.next.lock().unwrap();
        let v = *n;
        *n += 1;
        v
    }
}

impl MountSurface for Writable {
    fn share_id(&self) -> u16 {
        1
    }
    fn root(&self) -> NodeKey {
        NodeKey {
            ino: 2,
            generation: 1,
        }
    }
    fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
        self.nodes
            .lock()
            .unwrap()
            .get(&node.ino)
            .map(|n| n.attr.clone())
            .ok_or(NfsError::NoEnt)
    }
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        let nodes = self.nodes.lock().unwrap();
        let d = nodes.get(&dir.ino).ok_or(NfsError::NoEnt)?;
        let ino = *d.children.get(name).ok_or(NfsError::NoEnt)?;
        Ok((NodeKey { ino, generation: 1 }, nodes[&ino].attr.clone()))
    }
    fn readdir(&self, dir: NodeKey, _c: u64, _m: usize, plus: bool) -> NfsResult<ReadDirPage> {
        let nodes = self.nodes.lock().unwrap();
        let d = nodes.get(&dir.ino).ok_or(NfsError::NoEnt)?;
        let entries = d
            .children
            .iter()
            .enumerate()
            .map(|(i, (name, ino))| mist_nfs::DirEntry {
                name: name.clone(),
                node: NodeKey {
                    ino: *ino,
                    generation: 1,
                },
                cookie: i as u64 + 3,
                attr: if plus {
                    Some(nodes[ino].attr.clone())
                } else {
                    None
                },
            })
            .collect();
        Ok(ReadDirPage {
            entries,
            eof: true,
            cookieverf: 1,
        })
    }
    fn readlink(&self, _n: NodeKey) -> NfsResult<Vec<u8>> {
        Err(NfsError::NotSymlink)
    }
    fn read(&self, node: NodeKey, off: u64, count: u32) -> ReadFuture<'_> {
        Box::pin(async move {
            let nodes = self.nodes.lock().unwrap();
            let n = nodes.get(&node.ino).ok_or(NfsError::NoEnt)?;
            let s = (off as usize).min(n.data.len());
            let e = n.data.len().min(s + count as usize);
            Ok(ReadResult {
                data: n.data[s..e].to_vec(),
                eof: e >= n.data.len(),
            })
        })
    }
    fn fsstat(&self) -> FsStat {
        FsStat::default()
    }
    fn writable(&self) -> bool {
        true
    }
    fn create<'a>(
        &'a self,
        dir: NodeKey,
        name: &'a [u8],
        kind: CreateKind,
        mode: u16,
    ) -> MutFuture<'a, (NodeKey, Attr)> {
        Box::pin(async move {
            let ino = self.fresh();
            let mut nodes = self.nodes.lock().unwrap();
            if !nodes.contains_key(&dir.ino) {
                return Err(NfsError::NoEnt);
            }
            if let CreateKind::File { exclusive: true } = kind
                && nodes[&dir.ino].children.contains_key(name)
            {
                return Err(NfsError::Exist);
            }
            let k = match kind {
                CreateKind::Dir => Kind::Dir,
                CreateKind::Symlink { .. } => Kind::Symlink,
                _ => Kind::Reg,
            };
            let mut a = file_attr(ino, 0, k);
            a.mode = mode;
            nodes.insert(
                ino,
                Node {
                    attr: a.clone(),
                    data: vec![],
                    children: HashMap::new(),
                },
            );
            nodes
                .get_mut(&dir.ino)
                .unwrap()
                .children
                .insert(name.to_vec(), ino);
            Ok((NodeKey { ino, generation: 1 }, a))
        })
    }
    fn remove<'a>(&'a self, dir: NodeKey, name: &'a [u8], _is_dir: bool) -> MutFuture<'a, ()> {
        Box::pin(async move {
            let mut nodes = self.nodes.lock().unwrap();
            let ino = nodes
                .get_mut(&dir.ino)
                .ok_or(NfsError::NoEnt)?
                .children
                .remove(name)
                .ok_or(NfsError::NoEnt)?;
            nodes.remove(&ino);
            Ok(())
        })
    }
    fn write<'a>(
        &'a self,
        node: NodeKey,
        offset: u64,
        data: &'a [u8],
        _sync: bool,
    ) -> MutFuture<'a, Attr> {
        Box::pin(async move {
            let mut nodes = self.nodes.lock().unwrap();
            let n = nodes.get_mut(&node.ino).ok_or(NfsError::NoEnt)?;
            let end = offset as usize + data.len();
            if n.data.len() < end {
                n.data.resize(end, 0);
            }
            n.data[offset as usize..end].copy_from_slice(data);
            n.attr.size = n.data.len() as u64;
            Ok(n.attr.clone())
        })
    }
    fn setattr(&self, node: NodeKey, set: SetAttr) -> MutFuture<'_, Attr> {
        Box::pin(async move {
            let mut nodes = self.nodes.lock().unwrap();
            let n = nodes.get_mut(&node.ino).ok_or(NfsError::NoEnt)?;
            if let Some(sz) = set.size {
                n.data.resize(sz as usize, 0);
                n.attr.size = sz;
            }
            if let Some(m) = set.mode {
                n.attr.mode = m;
            }
            Ok(n.attr.clone())
        })
    }
    fn commit(&self, node: NodeKey, _off: u64, _len: u64) -> MutFuture<'_, Attr> {
        Box::pin(async move { self.getattr(node) })
    }
}

// --- tiny client (shared shape with client_roundtrip.rs) ---
const NFS_PROG: u32 = 100003;
const MOUNT_PROG: u32 = 100005;

struct Client {
    s: TcpStream,
    xid: u32,
}
impl Client {
    async fn connect(port: u16) -> Self {
        Client {
            s: TcpStream::connect(("127.0.0.1", port)).await.unwrap(),
            xid: 1,
        }
    }
    async fn call(&mut self, prog: u32, proc: u32, args: &[u8]) -> Vec<u8> {
        self.xid += 1;
        let mut m = Vec::new();
        for v in [self.xid, 0, 2, prog, 3, proc, 0, 0, 0, 0] {
            m.extend_from_slice(&v.to_be_bytes());
        }
        m.extend_from_slice(args);
        let marker = 0x8000_0000u32 | m.len() as u32;
        self.s.write_all(&marker.to_be_bytes()).await.unwrap();
        self.s.write_all(&m).await.unwrap();
        self.s.flush().await.unwrap();
        let mut hdr = [0u8; 4];
        self.s.read_exact(&mut hdr).await.unwrap();
        let len = (u32::from_be_bytes(hdr) & 0x7FFF_FFFF) as usize;
        let mut body = vec![0u8; len];
        self.s.read_exact(&mut body).await.unwrap();
        let mut p = 0;
        assert_eq!(rd32(&body, &mut p), self.xid);
        p += 8; // REPLY, ACCEPTED
        let _vf = rd32(&body, &mut p);
        let vl = rd32(&body, &mut p) as usize;
        p += vl.div_ceil(4) * 4 + 4; // verf body + accept_status
        body[p..].to_vec()
    }
}
fn be32(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_be_bytes());
}
fn be64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_be_bytes());
}
fn opaque(v: &mut Vec<u8>, d: &[u8]) {
    be32(v, d.len() as u32);
    v.extend_from_slice(d);
    let r = d.len() % 4;
    if r != 0 {
        v.extend(std::iter::repeat_n(0u8, 4 - r));
    }
}
fn rd32(b: &[u8], p: &mut usize) -> u32 {
    let x = u32::from_be_bytes(b[*p..*p + 4].try_into().unwrap());
    *p += 4;
    x
}
fn rd_opaque(b: &[u8], p: &mut usize) -> Vec<u8> {
    let len = rd32(b, p) as usize;
    let d = b[*p..*p + len].to_vec();
    *p += len.div_ceil(4) * 4;
    d
}
fn skip_poa(b: &[u8], p: &mut usize) {
    if rd32(b, p) != 0 {
        *p += 84;
    }
}
/// empty sattr3 with set_mode = mode.
fn sattr3_mode(v: &mut Vec<u8>, mode: u32) {
    be32(v, 1);
    be32(v, mode); // set_mode
    be32(v, 0); // set_uid
    be32(v, 0); // set_gid
    be32(v, 0); // set_size
    be32(v, 0); // set_atime DONT_CHANGE
    be32(v, 0); // set_mtime DONT_CHANGE
}

#[tokio::test]
async fn nfs_client_mutations() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(NfsServer::new(Arc::new(Writable::new()), b"k"));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let mut c = Client::connect(port).await;

    // root fh via MOUNT
    let mut a = Vec::new();
    opaque(&mut a, b"/");
    let b = c.call(MOUNT_PROG, 1, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0);
    let root = rd_opaque(&b, &mut p);

    // CREATE "note.txt" (UNCHECKED)
    let mut a = Vec::new();
    opaque(&mut a, &root);
    opaque(&mut a, b"note.txt");
    be32(&mut a, 0); // how = UNCHECKED
    sattr3_mode(&mut a, 0o644);
    let b = c.call(NFS_PROG, 8, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0, "create ok");
    assert_eq!(rd32(&b, &mut p), 1, "obj fh present");
    let file = rd_opaque(&b, &mut p);
    assert!(!file.is_empty());

    // WRITE "hello mist" at 0, FILE_SYNC
    let payload = b"hello mist";
    let mut a = Vec::new();
    opaque(&mut a, &file);
    be64(&mut a, 0);
    be32(&mut a, payload.len() as u32);
    be32(&mut a, 2); // FILE_SYNC
    opaque(&mut a, payload);
    let b = c.call(NFS_PROG, 7, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0, "write ok");
    skip_poa(&b, &mut p); // wcc pre
    skip_poa(&b, &mut p); // wcc post
    let count = rd32(&b, &mut p);
    assert_eq!(count, payload.len() as u32);

    // READ back
    let mut a = Vec::new();
    opaque(&mut a, &file);
    be64(&mut a, 0);
    be32(&mut a, 100);
    let b = c.call(NFS_PROG, 6, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0, "read ok");
    skip_poa(&b, &mut p);
    let _count = rd32(&b, &mut p);
    let _eof = rd32(&b, &mut p);
    let data = rd_opaque(&b, &mut p);
    assert_eq!(data, payload, "read returns written bytes");

    // SETATTR truncate to 5
    let mut a = Vec::new();
    opaque(&mut a, &file);
    // sattr3 with set_size = 5
    be32(&mut a, 0); // set_mode
    be32(&mut a, 0); // set_uid
    be32(&mut a, 0); // set_gid
    be32(&mut a, 1);
    be64(&mut a, 5); // set_size
    be32(&mut a, 0); // atime
    be32(&mut a, 0); // mtime
    be32(&mut a, 0); // guard
    let b = c.call(NFS_PROG, 2, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0, "setattr ok");

    // READ again → truncated to "hello"
    let mut a = Vec::new();
    opaque(&mut a, &file);
    be64(&mut a, 0);
    be32(&mut a, 100);
    let b = c.call(NFS_PROG, 6, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0);
    skip_poa(&b, &mut p);
    let _ = rd32(&b, &mut p);
    let _ = rd32(&b, &mut p);
    assert_eq!(rd_opaque(&b, &mut p), b"hello", "truncated");

    // REMOVE
    let mut a = Vec::new();
    opaque(&mut a, &root);
    opaque(&mut a, b"note.txt");
    let b = c.call(NFS_PROG, 12, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 0, "remove ok");

    // LOOKUP now fails
    let mut a = Vec::new();
    opaque(&mut a, &root);
    opaque(&mut a, b"note.txt");
    let b = c.call(NFS_PROG, 3, &a).await;
    let mut p = 0;
    assert_eq!(rd32(&b, &mut p), 2, "NFS3ERR_NOENT after remove");
}
