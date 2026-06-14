//! Wire-format snapshot: any unintended ABI change (field/variant reorder) fails this test.
//! Regenerate deliberately with `MIST_BLESS=1 cargo test -p mist-proto golden` and review the diff.

use mist_proto::*;

fn samples() -> Vec<(&'static str, Vec<u8>)> {
    let attr = Attr {
        kind: Kind::Reg,
        mode: 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size: 42,
        blocks: 8,
        mtime: Ts {
            sec: 1_700_000_000,
            nsec: 500,
        },
        ctime: Ts {
            sec: 1_700_000_001,
            nsec: 0,
        },
        rdev: 0,
        content_version: 7,
        symlink_target: None,
    };
    let node = NodeKey {
        ino: 0xABCD,
        generation: 3,
    };
    vec![
        (
            "hello",
            encode(&CtlMsg::Hello {
                proto: PROTO_VERSION,
                features: features::SUPPORTED,
                token_hash: [0x11; 32],
                host_name: "mac".into(),
                host_version: "0.1.0".into(),
            }),
        ),
        ("ping", encode(&CtlMsg::Ping { nonce: 0xDEAD_BEEF })),
        (
            "vm_identity",
            encode(&CtlMsg::VmIdentity {
                vm_uuid: [
                    0x4f, 0x2b, 0x0b, 0x30, 0xa9, 0xd8, 0x4f, 0x8a, 0xb5, 0xc4, 0x01, 0x22, 0xc7,
                    0xbe, 0x1c, 0x13,
                ],
            }),
        ),
        (
            "stream_hello_bulk1",
            encode(&CtlMsg::StreamHello {
                session_id: 5,
                lane: Lane::Bulk,
                idx: 1,
            }),
        ),
        (
            "stat",
            encode(&RpcReq::Stat {
                share: ShareId(2),
                node,
            }),
        ),
        (
            "read",
            encode(&RpcReq::Read {
                share: ShareId(0),
                node,
                version_hint: 7,
                off: 4096,
                len: 65536,
                ra: 1 << 20,
            }),
        ),
        ("resp_attr", encode(&RpcResp::Attr(attr.clone()))),
        (
            "journal_created",
            encode(&EventMsg::Journal(JournalBatch {
                share: ShareId(1),
                first_seq: 100,
                guest_mono_ns: 12345,
                records: vec![Rec::Created {
                    parent: NodeKey {
                        ino: 2,
                        generation: 0,
                    },
                    name: Name::new(*b"main.rs").unwrap(),
                    node,
                    attr: Some(attr.clone()),
                }],
            })),
        ),
        (
            "snapdone",
            encode(&EventMsg::SnapDone(SnapDone {
                snap_id: 9,
                share: ShareId(1),
                dirs: 10,
                entries: 100,
                errors: 0,
            })),
        ),
        (
            "frame_header",
            FrameHeader {
                len: 1234,
                kind: FrameKind::Event,
                flags: FLAG_MORE,
                seq: 77,
            }
            .encode()
            .to_vec(),
        ),
    ]
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn golden_vectors() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden_vectors.txt");
    let rendered: String = samples()
        .iter()
        .map(|(name, bytes)| format!("{name} {}\n", hex(bytes)))
        .collect();
    if std::env::var_os("MIST_BLESS").is_some() {
        std::fs::write(path, &rendered).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(path)
        .expect("golden_vectors.txt missing — run with MIST_BLESS=1 once");
    assert_eq!(
        committed, rendered,
        "wire format changed! If intentional, re-bless and bump a feature/proto gate."
    );
}
