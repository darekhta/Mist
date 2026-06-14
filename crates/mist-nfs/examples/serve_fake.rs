//! Serve a tiny in-memory tree over NFSv3 on a loopback port, for validating the macOS NFS
//! client against our XDR without a VM:
//!
//!   cargo run -p mist-nfs --example serve_fake -- 12049
//!   sudo mount_nfs -o vers=3,tcp,port=12049,mountport=12049,ro,nolocks,rdirplus 127.0.0.1:/ /tmp/m
//!   ls -la /tmp/m ; cat /tmp/m/hello.txt ; stat /tmp/m/dir
//!   sudo umount /tmp/m

use mist_nfs::{
    DirEntry, FsStat, MountSurface, NfsError, NfsResult, NfsServer, ReadDirPage, ReadFuture,
    ReadResult,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;

/// (attr, file data, dir children name→ino)
type FakeNode = (Attr, Option<Vec<u8>>, Option<Vec<(String, u64)>>);

struct FakeTree {
    // ino -> (attr, optional file data, optional dir children name->ino)
    nodes: HashMap<u64, FakeNode>,
}

fn attr(kind: Kind, _ino: u64, size: u64) -> Attr {
    Attr {
        kind,
        mode: if kind == Kind::Dir { 0o755 } else { 0o644 },
        nlink: 1,
        uid: 501,
        gid: 20,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts {
            sec: 1_700_000_000,
            nsec: 0,
        },
        ctime: Ts {
            sec: 1_700_000_000,
            nsec: 0,
        },
        rdev: 0,
        content_version: 1,
        symlink_target: if kind == Kind::Symlink {
            Some(b"hello.txt".to_vec())
        } else {
            None
        },
    }
}

impl FakeTree {
    fn new() -> Self {
        let hello = b"hello from mist-nfs\n".to_vec();
        let mut nodes = HashMap::new();
        // root ino 2
        nodes.insert(
            2,
            (
                attr(Kind::Dir, 2, 4096),
                None,
                Some(vec![
                    ("hello.txt".into(), 10),
                    ("dir".into(), 11),
                    ("link".into(), 12),
                ]),
            ),
        );
        nodes.insert(
            10,
            (attr(Kind::Reg, 10, hello.len() as u64), Some(hello), None),
        );
        nodes.insert(
            11,
            (
                attr(Kind::Dir, 11, 4096),
                None,
                Some(vec![("deep.txt".into(), 13)]),
            ),
        );
        nodes.insert(12, (attr(Kind::Symlink, 12, 9), None, None));
        nodes.insert(13, (attr(Kind::Reg, 13, 5), Some(b"deep\n".to_vec()), None));
        FakeTree { nodes }
    }
}

impl MountSurface for FakeTree {
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
            .get(&node.ino)
            .map(|(a, _, _)| a.clone())
            .ok_or(NfsError::NoEnt)
    }
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        let (_, _, children) = self.nodes.get(&dir.ino).ok_or(NfsError::NoEnt)?;
        let children = children.as_ref().ok_or(NfsError::NotDir)?;
        for (n, ino) in children {
            if n.as_bytes() == name {
                let a = self.nodes[ino].0.clone();
                return Ok((
                    NodeKey {
                        ino: *ino,
                        generation: 1,
                    },
                    a,
                ));
            }
        }
        Err(NfsError::NoEnt)
    }
    fn readdir(
        &self,
        dir: NodeKey,
        cookie: u64,
        _max: usize,
        plus: bool,
    ) -> NfsResult<ReadDirPage> {
        let (_, _, children) = self.nodes.get(&dir.ino).ok_or(NfsError::NoEnt)?;
        let children = children.as_ref().ok_or(NfsError::NotDir)?;
        let mut entries = Vec::new();
        for (i, (name, child_ino)) in children.iter().enumerate() {
            let c = (i as u64) + 3; // cookies start past "." ".."
            if c <= cookie {
                continue;
            }
            entries.push(DirEntry {
                name: name.as_bytes().to_vec(),
                node: NodeKey {
                    ino: *child_ino,
                    generation: 1,
                },
                cookie: c,
                attr: if plus {
                    Some(self.nodes[child_ino].0.clone())
                } else {
                    None
                },
            });
        }
        Ok(ReadDirPage {
            entries,
            eof: true,
            cookieverf: 1,
        })
    }
    fn readlink(&self, node: NodeKey) -> NfsResult<Vec<u8>> {
        self.nodes
            .get(&node.ino)
            .and_then(|(a, _, _)| a.symlink_target.clone())
            .ok_or(NfsError::NotSymlink)
    }
    fn read(&self, node: NodeKey, offset: u64, count: u32) -> ReadFuture<'_> {
        let data = self.nodes.get(&node.ino).and_then(|(_, d, _)| d.clone());
        Box::pin(async move {
            let data = data.ok_or(NfsError::IsDir)?;
            let start = (offset as usize).min(data.len());
            let end = data.len().min(start + count as usize);
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

#[tokio::main]
async fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(12049);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    eprintln!("mist-nfs fake server on 127.0.0.1:{port}");
    eprintln!(
        "  sudo mount_nfs -o vers=3,tcp,port={port},mountport={port},ro,nolocks,rdirplus \
         127.0.0.1:/ /tmp/mistmnt"
    );
    let server = Arc::new(NfsServer::new(Arc::new(FakeTree::new()), b"example-secret"));
    server.serve(listener).await.unwrap();
}
