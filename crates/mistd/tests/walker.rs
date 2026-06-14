//! Walker correctness against a real filesystem tree (Linux only; runs in Linux CI).

#![cfg(target_os = "linux")]

use mist_proto::{Kind, ShareId, SnapDir};
use mist_replica::{ShareReplica, ShareState};
use std::collections::BTreeMap;
use std::path::Path;

fn build_tree(root: &Path) {
    let mk = |p: &str| std::fs::create_dir_all(root.join(p)).unwrap();
    let wr = |p: &str, data: &[u8]| std::fs::write(root.join(p), data).unwrap();
    mk("src/sub");
    mk("empty");
    wr("README.md", b"hello mist\n");
    wr("src/lib.rs", b"pub fn x() {}\n");
    wr("src/sub/deep.txt", &[0xA5; 3]);
    std::os::unix::fs::symlink("README.md", root.join("link")).unwrap();
}

#[tokio::test]
async fn walker_matches_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());

    let mut cfg = BTreeMap::new();
    cfg.insert(
        "t".to_string(),
        mistd::config::ShareConfig {
            path: dir.path().to_path_buf(),
            readonly: false,
            apply_uid: None,
            apply_gid: None,
            commit: Default::default(),
        },
    );
    let shares = mistd::linux::shares::setup(&cfg).unwrap();
    let share = shares.by_id.values().next().unwrap().clone();
    let info = share.info_template.clone();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<SnapDir>(64);
    let walk = tokio::spawn(mistd::linux::walker::walk(share.clone(), 1, 4, 2048, tx));

    let replica = ShareReplica::new(info);
    while let Some(d) = rx.recv().await {
        replica.apply_snap_dir(&d);
    }
    let done = walk.await.unwrap();
    let stats = replica.finish_snapshot(&done);

    assert_eq!(replica.state(), ShareState::Live);
    assert_eq!(done.errors, 0, "no errors expected");
    // Entries: src, empty, README.md, link (root) + lib.rs, sub (src) + deep.txt (sub) = 7.
    assert_eq!(stats.entries, 7, "stats: {stats:?}");
    assert_eq!(done.dirs, 4); // root, src, src/sub, empty

    let (_, a) = replica.resolve_path("/README.md").unwrap();
    assert_eq!(a.kind, Kind::Reg);
    assert_eq!(a.size, 11);
    let (_, a) = replica.resolve_path("/src/sub/deep.txt").unwrap();
    assert_eq!(a.size, 3);
    let (n, a) = replica.resolve_path("/link").unwrap();
    assert_eq!(a.kind, Kind::Symlink);
    assert_eq!(replica.readlink(n).unwrap(), b"README.md");
    let (e, _) = replica.resolve_path("/empty").unwrap();
    let page = replica.readdir(e, 0, 10).unwrap();
    assert!(page.entries.is_empty() && page.eof);

    assert_eq!(replica.share_id(), ShareId(0));
}

/// Handle-mode (non-degraded) checks; requires root (open_by_handle_at).
#[tokio::test]
#[ignore = "needs CAP_DAC_READ_SEARCH; run in CI via: sudo cargo test -p mistd -- --ignored privileged_"]
async fn privileged_handle_mode_stat_read() {
    let dir = tempfile::tempdir().unwrap();
    build_tree(dir.path());
    let mut cfg = BTreeMap::new();
    cfg.insert(
        "t".to_string(),
        mistd::config::ShareConfig {
            path: dir.path().to_path_buf(),
            readonly: false,
            apply_uid: None,
            apply_gid: None,
            commit: Default::default(),
        },
    );
    let shares = mistd::linux::shares::setup(&cfg).unwrap();
    let share = shares.by_id.values().next().unwrap().clone();
    assert!(!share.degraded, "expected handle mode as root on a real fs");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<SnapDir>(64);
    let walk = tokio::spawn(mistd::linux::walker::walk(share.clone(), 1, 4, 2048, tx));
    let replica = ShareReplica::new(share.info_template.clone());
    while let Some(d) = rx.recv().await {
        replica.apply_snap_dir(&d);
    }
    let done = walk.await.unwrap();
    replica.finish_snapshot(&done);

    let (n, a) = replica.resolve_path("/README.md").unwrap();
    // Stat through the handle path agrees with the walked attrs.
    let stat = mistd::linux::rpc::stat_attr(&share, n).unwrap();
    assert_eq!(stat.size, a.size);
}
