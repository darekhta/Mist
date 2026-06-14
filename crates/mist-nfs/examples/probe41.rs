//! macOS NFSv4.1 client probe: serve a tiny in-memory tree over v4.1 on
//! a loopback port and print it, so a human/script can try `mount_nfs -o vers=4.1,...` against
//! it and observe exactly which COMPOUND ops the real client sends.
//!
//!   cargo run -p mist-nfs --example probe41
//!   mkdir -p /tmp/p41 && mount_nfs -o vers=4.1,tcp,port=<P>,nolocks,soft 127.0.0.1:/ /tmp/p41

use mist_nfs::{
    DirEntry, FsStat, MountSurface, Nfs41Server, NfsError, NfsResult, ReadDirPage, ReadFuture,
    ReadResult,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;

type FakeNode = (Attr, Option<Vec<u8>>, Option<Vec<(String, u64)>>);

struct Fake {
    nodes: HashMap<u64, FakeNode>,
}

fn at(kind: Kind, size: u64) -> Attr {
    Attr {
        kind,
        mode: if matches!(kind, Kind::Dir) {
            0o755
        } else {
            0o644
        },
        nlink: 1,
        uid: 501,
        gid: 20,
        size,
        blocks: size.div_ceil(512),
        mtime: Ts {
            sec: 1_780_000_000,
            nsec: 0,
        },
        ctime: Ts {
            sec: 1_780_000_000,
            nsec: 0,
        },
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
                Some(vec![("hello.txt".into(), 10), ("sub".into(), 11)]),
            ),
        );
        nodes.insert(
            10,
            (at(Kind::Reg, 15), Some(b"hello from v41\n".to_vec()), None),
        );
        nodes.insert(
            11,
            (
                at(Kind::Dir, 4096),
                None,
                Some(vec![("inner.txt".into(), 12)]),
            ),
        );
        nodes.insert(12, (at(Kind::Reg, 6), Some(b"inner\n".to_vec()), None));
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
    fn parent(&self, node: NodeKey) -> NfsResult<NodeKey> {
        for (ino, (_, _, ch)) in &self.nodes {
            if let Some(ch) = ch
                && ch.iter().any(|(_, c)| *c == node.ino)
            {
                return Ok(Fake::key(*ino));
            }
        }
        Ok(self.root())
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mist_nfs=trace".into()),
        )
        .init();
    let server = Arc::new(Nfs41Server::new(Arc::new(Fake::new()), b"probe"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("PROBE41 PORT={port}");
    println!(
        "try: mkdir -p /tmp/p41 && mount_nfs -o vers=4.1,tcp,port={port},nolocks,soft 127.0.0.1:/ /tmp/p41"
    );
    server.serve(listener).await.unwrap();
}
