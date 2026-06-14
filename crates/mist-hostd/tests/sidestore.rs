//! Side-store decorator semantics against a fake inner surface: interception, real-wins,
//! hidden listings, synthesized dirs, AppleDouble GC, persistence across reopen.

use mist_hostd::sidestore::{SideStore, SideStoreSurface};
use mist_nfs::{
    CreateKind, DirEntry, FsStat, MountSurface, MutFuture, NfsError, NfsResult, ReadDirPage,
    ReadFuture, ReadResult, SetAttr,
};
use mist_proto::{Attr, Kind, NodeKey, Ts};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

const ROOT: NodeKey = NodeKey {
    ino: 2,
    generation: 1,
};

fn attr(ino: u64, kind: Kind) -> Attr {
    Attr {
        kind,
        mode: if kind == Kind::Dir { 0o755 } else { 0o644 },
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size: 0,
        blocks: 0,
        mtime: Ts { sec: 1, nsec: 0 },
        ctime: Ts { sec: 1, nsec: 0 },
        rdev: 0,
        content_version: ino,
        symlink_target: None,
    }
}

/// Minimal writable inner surface: one flat root dir of regular files.
#[derive(Default)]
struct Flat {
    files: Mutex<HashMap<Vec<u8>, u64>>, // name → ino
    created: Mutex<Vec<Vec<u8>>>,        // names that reached the inner surface via create()
}

impl MountSurface for Flat {
    fn share_id(&self) -> u16 {
        1
    }
    fn root(&self) -> NodeKey {
        ROOT
    }
    fn getattr(&self, node: NodeKey) -> NfsResult<Attr> {
        if node == ROOT {
            return Ok(attr(2, Kind::Dir));
        }
        let files = self.files.lock().unwrap();
        if files.values().any(|i| *i == node.ino) {
            Ok(attr(node.ino, Kind::Reg))
        } else {
            Err(NfsError::NoEnt)
        }
    }
    fn lookup(&self, dir: NodeKey, name: &[u8]) -> NfsResult<(NodeKey, Attr)> {
        if dir != ROOT {
            return Err(NfsError::NoEnt);
        }
        let files = self.files.lock().unwrap();
        let ino = *files.get(name).ok_or(NfsError::NoEnt)?;
        Ok((NodeKey { ino, generation: 1 }, attr(ino, Kind::Reg)))
    }
    fn readdir(&self, _d: NodeKey, _c: u64, _m: usize, plus: bool) -> NfsResult<ReadDirPage> {
        let files = self.files.lock().unwrap();
        let entries = files
            .iter()
            .enumerate()
            .map(|(i, (name, ino))| DirEntry {
                name: name.clone(),
                node: NodeKey {
                    ino: *ino,
                    generation: 1,
                },
                cookie: i as u64 + 3,
                attr: plus.then(|| attr(*ino, Kind::Reg)),
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
    fn read(&self, _n: NodeKey, _o: u64, _c: u32) -> ReadFuture<'_> {
        Box::pin(async {
            Ok(ReadResult {
                data: vec![],
                eof: true,
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
        _dir: NodeKey,
        name: &'a [u8],
        _kind: CreateKind,
        _mode: u16,
    ) -> MutFuture<'a, (NodeKey, Attr)> {
        Box::pin(async move {
            let ino = 100 + self.files.lock().unwrap().len() as u64;
            self.files.lock().unwrap().insert(name.to_vec(), ino);
            self.created.lock().unwrap().push(name.to_vec());
            Ok((NodeKey { ino, generation: 1 }, attr(ino, Kind::Reg)))
        })
    }
    fn remove<'a>(&'a self, _dir: NodeKey, name: &'a [u8], _is_dir: bool) -> MutFuture<'a, ()> {
        Box::pin(async move {
            self.files
                .lock()
                .unwrap()
                .remove(name)
                .map(|_| ())
                .ok_or(NfsError::NoEnt)
        })
    }
    fn write<'a>(&'a self, n: NodeKey, _o: u64, _d: &'a [u8], _s: bool) -> MutFuture<'a, Attr> {
        Box::pin(async move { self.getattr(n) })
    }
    fn setattr(&self, n: NodeKey, _s: SetAttr) -> MutFuture<'_, Attr> {
        Box::pin(async move { self.getattr(n) })
    }
    fn commit(&self, n: NodeKey, _off: u64, _len: u64) -> MutFuture<'_, Attr> {
        Box::pin(async move { self.getattr(n) })
    }
}

fn surface() -> (Arc<Flat>, SideStoreSurface<Flat>) {
    let inner = Arc::new(Flat::default());
    let s = SideStoreSurface::new(inner.clone(), SideStore::open(None));
    (inner, s)
}

#[tokio::test]
async fn appledouble_intercepted_and_hidden() {
    let (inner, s) = surface();
    // Create the anchor through the surface (reaches the guest), then its AppleDouble (must not).
    s.create(
        ROOT,
        b"file.txt",
        CreateKind::File { exclusive: false },
        0o644,
    )
    .await
    .unwrap();
    let (node, _) = s
        .create(
            ROOT,
            b"._file.txt",
            CreateKind::File { exclusive: false },
            0o644,
        )
        .await
        .unwrap();
    assert_eq!(
        inner.created.lock().unwrap().as_slice(),
        &[b"file.txt".to_vec()],
        "._ create must not reach the inner surface"
    );

    // Write + read round-trip through the row.
    s.write(node, 0, b"xattr-blob", true).await.unwrap();
    let r = s.read(node, 0, 100).await.unwrap();
    assert_eq!(r.data, b"xattr-blob");

    // Direct lookup serves it; readdir hides it.
    assert!(s.lookup(ROOT, b"._file.txt").is_ok());
    let page = s.readdir(ROOT, 0, 100, false).unwrap();
    assert!(
        page.entries.iter().all(|e| e.name != b"._file.txt"),
        "side rows are hidden from listings"
    );
}

#[tokio::test]
async fn real_guest_entry_wins() {
    let (inner, s) = surface();
    // The guest really has a .DS_Store: pass-through, side-store must not shadow it.
    inner
        .files
        .lock()
        .unwrap()
        .insert(b".DS_Store".to_vec(), 77);
    let (node, _) = s.lookup(ROOT, b".DS_Store").unwrap();
    assert_eq!(node.ino, 77, "real entry wins over side-store");
}

#[tokio::test]
async fn remove_of_real_file_drops_companion() {
    let (_inner, s) = surface();
    s.create(
        ROOT,
        b"doc.txt",
        CreateKind::File { exclusive: false },
        0o644,
    )
    .await
    .unwrap();
    s.create(
        ROOT,
        b"._doc.txt",
        CreateKind::File { exclusive: false },
        0o644,
    )
    .await
    .unwrap();
    s.remove(ROOT, b"doc.txt", false).await.unwrap();
    assert!(
        matches!(s.lookup(ROOT, b"._doc.txt"), Err(NfsError::NoEnt)),
        "AppleDouble companion is GC'd with its anchor"
    );
}

#[tokio::test]
async fn synthesized_directory_lists_children() {
    let (inner, s) = surface();
    let (dir, _) = s
        .create(ROOT, b".fseventsd", CreateKind::Dir, 0o755)
        .await
        .unwrap();
    s.create(dir, b"no_log", CreateKind::File { exclusive: false }, 0o644)
        .await
        .unwrap();
    assert!(
        inner.created.lock().unwrap().is_empty(),
        "nothing reached the guest"
    );
    let page = s.readdir(dir, 0, 100, true).unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, b"no_log");
    // Non-empty synth dir refuses rmdir; after removing the child it succeeds.
    assert!(matches!(
        s.remove(ROOT, b".fseventsd", true).await,
        Err(NfsError::NotEmpty)
    ));
    s.remove(dir, b"no_log", false).await.unwrap();
    s.remove(ROOT, b".fseventsd", true).await.unwrap();
}

#[tokio::test]
async fn orphan_appledouble_swept_after_linger() {
    let (_inner, s) = surface();
    // ._ghost has no anchor ("ghost" was never created / guest deleted it).
    let (node, _) = s
        .create(
            ROOT,
            b"._ghost",
            CreateKind::File { exclusive: false },
            0o644,
        )
        .await
        .unwrap();
    // Age the row past the linger window via mtime, then trigger a sweep with another create.
    s.setattr(
        node,
        SetAttr {
            mtime: Some(Ts { sec: 1, nsec: 0 }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    s.create(
        ROOT,
        b".DS_Store",
        CreateKind::File { exclusive: false },
        0o644,
    )
    .await
    .unwrap();
    assert!(
        matches!(s.lookup(ROOT, b"._ghost"), Err(NfsError::NoEnt)),
        "orphaned AppleDouble swept after linger"
    );
}

#[tokio::test]
async fn persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sidestore-test.bin");
    {
        let inner = Arc::new(Flat::default());
        let s = SideStoreSurface::new(inner, SideStore::open(Some(path.clone())));
        let (node, _) = s
            .create(
                ROOT,
                b".DS_Store",
                CreateKind::File { exclusive: false },
                0o644,
            )
            .await
            .unwrap();
        s.write(node, 0, b"finder-state", true).await.unwrap();
    }
    // Fresh surface over the same file: the row is back.
    let inner = Arc::new(Flat::default());
    let s = SideStoreSurface::new(inner, SideStore::open(Some(path)));
    let (node, a) = s.lookup(ROOT, b".DS_Store").unwrap();
    assert_eq!(a.size, 12);
    let r = s.read(node, 0, 100).await.unwrap();
    assert_eq!(r.data, b"finder-state");
}

#[tokio::test]
async fn metadata_never_index_synthesized_at_root() {
    let (_inner, s) = surface();
    let (_, a) = s.lookup(ROOT, b".metadata_never_index").unwrap();
    assert_eq!(a.kind, Kind::Reg);
    assert_eq!(a.size, 0);
}
