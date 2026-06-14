//! Hostile journal stream vs a seeded replica (design 10 §3): decoded-but-adversarial `Rec`s
//! must never panic, loop, or break the read API. Input = concatenated length-prefixed
//! postcard `Rec`s; junk segments are skipped (the decode layer already rejects them).
#![no_main]
use libfuzzer_sys::fuzz_target;
use mist_proto::{NodeKey, Rec, ShareId, ShareInfo};

fuzz_target!(|data: &[u8]| {
    let root = NodeKey { ino: 2, generation: 1 };
    let r = mist_replica::ShareReplica::new(ShareInfo {
        id: ShareId(1),
        name: "fuzz".into(),
        epoch: 1,
        fsid: 1,
        root,
        flags: 0,
        ino_bits: 64,
    });
    // Seed a tiny live tree so records can hit real nodes, not just misses.
    let mk = |ino: u64| NodeKey { ino, generation: 1 };
    let attr = |kind| {
        let mut a = mist_proto::Attr {
            kind,
            mode: 0o755,
            nlink: 1,
            uid: 1,
            gid: 1,
            size: 0,
            blocks: 0,
            mtime: mist_proto::Ts { sec: 0, nsec: 0 },
            ctime: mist_proto::Ts { sec: 0, nsec: 0 },
            rdev: 0,
            content_version: 0,
            symlink_target: None,
        };
        if kind == mist_proto::Kind::Symlink {
            a.symlink_target = Some(b"t".to_vec());
        }
        a
    };
    r.apply_snap_dir(&mist_proto::SnapDir {
        snap_id: 1,
        share: ShareId(1),
        dir: root,
        dir_attr: attr(mist_proto::Kind::Dir),
        parent: root,
        entries: (0..8)
            .map(|i| mist_proto::SnapEntry {
                name: mist_proto::Name::new(format!("f{i}").into_bytes()).unwrap(),
                node: mk(10 + i),
                attr: attr(if i % 3 == 0 {
                    mist_proto::Kind::Dir
                } else {
                    mist_proto::Kind::Reg
                }),
            })
            .collect(),
        last: true,
    });

    // Walk the input as [u16 len][postcard Rec] segments.
    let mut buf = data;
    while buf.len() >= 2 {
        let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        buf = &buf[2..];
        let take = len.min(buf.len());
        if let Ok(rec) = mist_proto::decode::<Rec>(&buf[..take]) {
            let _ = r.apply_rec(&rec);
        }
        buf = &buf[take..];
        if take == 0 {
            break;
        }
    }
    // Read API must stay total regardless of what the stream did.
    let _ = r.getattr(root);
    let _ = r.readdir(root, 0, 64);
    let _ = r.lookup(root, b"f1");
    let _ = r.path_of(mk(11));
    let _ = r.stats();
});
