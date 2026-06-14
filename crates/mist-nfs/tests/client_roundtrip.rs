//! Drive the real NFS server over a loopback TCP socket with a hand-rolled NFSv3 client,
//! exercising the full ONC-RPC record-marking + XDR path: MOUNT MNT → GETATTR → LOOKUP →
//! READDIRPLUS → READ → READLINK. This validates wire-format self-consistency end-to-end
//! (the macOS kernel client is validated separately in the VM e2e, which needs root to mount).

use mist_nfs::{
    DirEntry, FsStat, MountSurface, NfsError, NfsResult, NfsServer, ReadDirPage, ReadFuture,
    ReadResult,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;

/// (attr, file data, dir children name→ino)
type FakeNode = (Attr, Option<Vec<u8>>, Option<Vec<(String, u64)>>);
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---- a tiny fake tree -----------------------------------------------------------------------

struct Fake {
    nodes: HashMap<u64, FakeNode>,
}
fn at(kind: Kind, size: u64, target: Option<&[u8]>) -> Attr {
    Attr {
        kind,
        mode: 0o644,
        nlink: 1,
        uid: 0,
        gid: 0,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts { sec: 5, nsec: 0 },
        ctime: Ts { sec: 5, nsec: 0 },
        rdev: 0,
        content_version: 1,
        symlink_target: target.map(|t| t.to_vec()),
    }
}
impl Fake {
    fn new() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            2,
            (
                at(Kind::Dir, 4096, None),
                None,
                Some(vec![("f.txt".into(), 10), ("ln".into(), 11)]),
            ),
        );
        nodes.insert(10, (at(Kind::Reg, 5, None), Some(b"hello".to_vec()), None));
        nodes.insert(11, (at(Kind::Symlink, 5, Some(b"f.txt")), None, None));
        Fake { nodes }
    }
}
impl MountSurface for Fake {
    fn share_id(&self) -> u16 {
        1
    }
    fn root(&self) -> NodeKey {
        NodeKey {
            ino: 2,
            generation: 1,
        }
    }
    fn getattr(&self, n: NodeKey) -> NfsResult<Attr> {
        self.nodes
            .get(&n.ino)
            .map(|x| x.0.clone())
            .ok_or(NfsError::NoEnt)
    }
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        let kids = self
            .nodes
            .get(&dir.ino)
            .and_then(|x| x.2.as_ref())
            .ok_or(NfsError::NotDir)?;
        for (n, ino) in kids {
            if n.as_bytes() == name {
                return Ok((
                    NodeKey {
                        ino: *ino,
                        generation: 1,
                    },
                    self.nodes[ino].0.clone(),
                ));
            }
        }
        Err(NfsError::NoEnt)
    }
    fn readdir(&self, dir: NodeKey, cookie: u64, _m: usize, plus: bool) -> NfsResult<ReadDirPage> {
        let kids = self
            .nodes
            .get(&dir.ino)
            .and_then(|x| x.2.as_ref())
            .ok_or(NfsError::NotDir)?;
        let mut entries = Vec::new();
        for (i, (name, ino)) in kids.iter().enumerate() {
            let c = i as u64 + 3;
            if c <= cookie {
                continue;
            }
            entries.push(DirEntry {
                name: name.as_bytes().to_vec(),
                node: NodeKey {
                    ino: *ino,
                    generation: 1,
                },
                cookie: c,
                attr: if plus {
                    Some(self.nodes[ino].0.clone())
                } else {
                    None
                },
            });
        }
        Ok(ReadDirPage {
            entries,
            eof: true,
            cookieverf: 42,
        })
    }
    fn readlink(&self, n: NodeKey) -> NfsResult<Vec<u8>> {
        self.nodes
            .get(&n.ino)
            .and_then(|x| x.0.symlink_target.clone())
            .ok_or(NfsError::NotSymlink)
    }
    fn read(&self, n: NodeKey, off: u64, count: u32) -> ReadFuture<'_> {
        let data = self.nodes.get(&n.ino).and_then(|x| x.1.clone());
        Box::pin(async move {
            let d = data.ok_or(NfsError::IsDir)?;
            let s = (off as usize).min(d.len());
            let e = d.len().min(s + count as usize);
            Ok(ReadResult {
                data: d[s..e].to_vec(),
                eof: e >= d.len(),
            })
        })
    }
    fn fsstat(&self) -> FsStat {
        FsStat::default()
    }
}

// ---- minimal NFS client (encode calls, parse replies) ---------------------------------------

const NFS_PROG: u32 = 100003;
const MOUNT_PROG: u32 = 100005;

struct Client {
    stream: TcpStream,
    xid: u32,
}

impl Client {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        Client { stream, xid: 1 }
    }

    /// Build + send a call; return the accepted-reply body positioned after accept_status.
    async fn call(&mut self, prog: u32, proc: u32, args: &[u8]) -> Vec<u8> {
        self.xid += 1;
        let mut m = Vec::new();
        be32(&mut m, self.xid);
        be32(&mut m, 0); // CALL
        be32(&mut m, 2); // rpcvers
        be32(&mut m, prog);
        be32(&mut m, 3); // version
        be32(&mut m, proc);
        be32(&mut m, 0); // cred AUTH_NONE
        be32(&mut m, 0); // cred len 0
        be32(&mut m, 0); // verf AUTH_NONE
        be32(&mut m, 0); // verf len 0
        m.extend_from_slice(args);

        // record marker (last fragment)
        let marker = 0x8000_0000u32 | m.len() as u32;
        self.stream.write_all(&marker.to_be_bytes()).await.unwrap();
        self.stream.write_all(&m).await.unwrap();
        self.stream.flush().await.unwrap();

        // read reply record
        let mut hdr = [0u8; 4];
        self.stream.read_exact(&mut hdr).await.unwrap();
        let len = (u32::from_be_bytes(hdr) & 0x7FFF_FFFF) as usize;
        let mut body = vec![0u8; len];
        self.stream.read_exact(&mut body).await.unwrap();

        // parse reply header: xid, REPLY(1), accepted(0), verf flavor, verf len(+pad), accept_status
        let mut p = 0usize;
        let rxid = rd32(&body, &mut p);
        assert_eq!(rxid, self.xid);
        assert_eq!(rd32(&body, &mut p), 1); // REPLY
        assert_eq!(rd32(&body, &mut p), 0); // ACCEPTED
        let _vflavor = rd32(&body, &mut p);
        let vlen = rd32(&body, &mut p) as usize;
        p += vlen.div_ceil(4) * 4;
        let _accept = rd32(&body, &mut p);
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
    let rem = d.len() % 4;
    if rem != 0 {
        v.extend(std::iter::repeat_n(0u8, 4 - rem));
    }
}
fn rd32(b: &[u8], p: &mut usize) -> u32 {
    let x = u32::from_be_bytes(b[*p..*p + 4].try_into().unwrap());
    *p += 4;
    x
}
fn rd64(b: &[u8], p: &mut usize) -> u64 {
    let x = u64::from_be_bytes(b[*p..*p + 8].try_into().unwrap());
    *p += 8;
    x
}
fn rd_opaque(b: &[u8], p: &mut usize) -> Vec<u8> {
    let len = rd32(b, p) as usize;
    let d = b[*p..*p + len].to_vec();
    *p += len.div_ceil(4) * 4;
    d
}
/// Skip a post_op_attr (bool + fattr3 if present). fattr3 is 21 u32-words = 84 bytes.
fn skip_post_op_attr(b: &[u8], p: &mut usize) {
    if rd32(b, p) != 0 {
        *p += 84;
    }
}

#[tokio::test]
async fn nfs_client_full_roundtrip() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = Arc::new(NfsServer::new(Arc::new(Fake::new()), b"secret"));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let mut c = Client::connect(port).await;

    // MOUNT MNT("/") → mountstat3 + root fh + auth flavors
    let mut args = Vec::new();
    opaque(&mut args, b"/");
    let body = c.call(MOUNT_PROG, 1, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "MNT3_OK");
    let root_fh = rd_opaque(&body, &mut p);
    assert_eq!(root_fh.len(), mist_nfs::HANDLE_LEN);

    // GETATTR(root) → ftype DIR
    let mut args = Vec::new();
    opaque(&mut args, &root_fh);
    let body = c.call(NFS_PROG, 1, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "NFS3_OK");
    assert_eq!(rd32(&body, &mut p), 2, "ftype3 = NF3DIR");

    // LOOKUP(root, "f.txt")
    let mut args = Vec::new();
    opaque(&mut args, &root_fh);
    opaque(&mut args, b"f.txt");
    let body = c.call(NFS_PROG, 3, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "lookup OK");
    let file_fh = rd_opaque(&body, &mut p);
    assert!(!file_fh.is_empty());

    // READ(f.txt, 0, 100) → "hello"
    let mut args = Vec::new();
    opaque(&mut args, &file_fh);
    be64(&mut args, 0);
    be32(&mut args, 100);
    let body = c.call(NFS_PROG, 6, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "read OK");
    skip_post_op_attr(&body, &mut p);
    let count = rd32(&body, &mut p);
    let eof = rd32(&body, &mut p);
    let data = rd_opaque(&body, &mut p);
    assert_eq!(count, 5);
    assert_eq!(eof, 1);
    assert_eq!(data, b"hello");

    // READDIRPLUS(root) → entries f.txt, ln
    let mut args = Vec::new();
    opaque(&mut args, &root_fh);
    be64(&mut args, 0); // cookie
    args.extend_from_slice(&[0u8; 8]); // cookieverf
    be32(&mut args, 8192); // dircount
    be32(&mut args, 32768); // maxcount
    let body = c.call(NFS_PROG, 17, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "readdirplus OK");
    skip_post_op_attr(&body, &mut p); // dir_attributes
    let _verf = rd64(&body, &mut p); // cookieverf[8]
    let mut names = Vec::new();
    while rd32(&body, &mut p) != 0 {
        // entry present
        let _fileid = rd64(&body, &mut p);
        let name = rd_opaque(&body, &mut p);
        let _cookie = rd64(&body, &mut p);
        skip_post_op_attr(&body, &mut p); // name_attributes
        // name_handle: post_op_fh3 = bool + nfs_fh3
        if rd32(&body, &mut p) != 0 {
            let _fh = rd_opaque(&body, &mut p);
        }
        names.push(String::from_utf8_lossy(&name).into_owned());
    }
    let eof = rd32(&body, &mut p);
    assert_eq!(eof, 1);
    assert_eq!(names, vec!["f.txt", "ln"]);

    // READLINK(ln) → "f.txt"
    let mut args = Vec::new();
    opaque(&mut args, &file_fh); // wrong target on purpose? no — look up ln first
    // Look up "ln"
    let mut la = Vec::new();
    opaque(&mut la, &root_fh);
    opaque(&mut la, b"ln");
    let lb = c.call(NFS_PROG, 3, &la).await;
    let mut lp = 0;
    assert_eq!(rd32(&lb, &mut lp), 0);
    let ln_fh = rd_opaque(&lb, &mut lp);
    args.clear();
    opaque(&mut args, &ln_fh);
    let body = c.call(NFS_PROG, 5, &args).await;
    let mut p = 0;
    assert_eq!(rd32(&body, &mut p), 0, "readlink OK");
    skip_post_op_attr(&body, &mut p);
    let target = rd_opaque(&body, &mut p);
    assert_eq!(target, b"f.txt");
}
