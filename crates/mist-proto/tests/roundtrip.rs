//! Property tests: encode∘decode = id; hostile bytes never panic; caps enforced.

use mist_proto::*;
use proptest::prelude::*;

fn arb_name() -> impl Strategy<Value = Name> {
    proptest::collection::vec(any::<u8>(), 1..=64)
        .prop_filter_map("valid name", |b| Name::new(b).ok())
}

fn arb_ts() -> impl Strategy<Value = Ts> {
    (any::<i64>(), 0u32..1_000_000_000).prop_map(|(sec, nsec)| Ts { sec, nsec })
}

fn arb_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Reg),
        Just(Kind::Dir),
        Just(Kind::Symlink),
        Just(Kind::Fifo),
        Just(Kind::Sock),
        Just(Kind::Chr),
        Just(Kind::Blk),
    ]
}

fn arb_node() -> impl Strategy<Value = NodeKey> {
    (any::<u64>(), any::<u32>()).prop_map(|(ino, generation)| NodeKey { ino, generation })
}

prop_compose! {
    fn arb_attr()(
        kind in arb_kind(),
        mode in any::<u16>(),
        nlink in any::<u32>(),
        uid in any::<u32>(),
        gid in any::<u32>(),
        size in any::<u64>(),
        blocks in any::<u64>(),
        mtime in arb_ts(),
        ctime in arb_ts(),
        rdev in any::<u64>(),
        content_version in any::<u64>(),
        target in proptest::collection::vec(any::<u8>(), 1..32),
    ) -> Attr {
        let symlink_target = if kind == Kind::Symlink { Some(target) } else { None };
        Attr { kind, mode, nlink, uid, gid, size, blocks, mtime, ctime, rdev,
               content_version, symlink_target }
    }
}

fn arb_rec() -> impl Strategy<Value = Rec> {
    prop_oneof![
        (
            arb_node(),
            arb_name(),
            arb_node(),
            proptest::option::of(arb_attr())
        )
            .prop_map(|(parent, name, node, attr)| Rec::Created {
                parent,
                name,
                node,
                attr
            }),
        (arb_node(), arb_name()).prop_map(|(parent, name)| Rec::Removed { parent, name }),
        (arb_node(), arb_name(), arb_node(), arb_name()).prop_map(|(fp, fnm, tp, tnm)| {
            Rec::Renamed {
                from_parent: fp,
                from_name: fnm,
                to_parent: tp,
                to_name: tnm,
            }
        }),
        (arb_node(), arb_attr()).prop_map(|(node, attr)| Rec::AttrChanged { node, attr }),
        (
            arb_node(),
            any::<u64>(),
            any::<u64>(),
            arb_ts(),
            any::<bool>()
        )
            .prop_map(|(node, version, size, mtime, in_progress)| Rec::Content {
                node,
                version,
                size,
                mtime,
                in_progress
            }),
        arb_node().prop_map(|node| Rec::SelfRemoved { node }),
        Just(Rec::Overflow),
        any::<u64>().prop_map(|tag| Rec::EchoMarker { tag }),
    ]
}

proptest! {
    #[test]
    fn rec_roundtrip(rec in arb_rec()) {
        let batch = JournalBatch { share: ShareId(1), first_seq: 1, guest_mono_ns: 0,
                                   records: vec![rec] };
        let msg = EventMsg::Journal(batch);
        let bytes = encode(&msg);
        let back: EventMsg = decode(&bytes).unwrap();
        prop_assert_eq!(back, msg);
    }

    #[test]
    fn snapdir_roundtrip(
        entries in proptest::collection::vec((arb_name(), arb_node(), arb_attr()), 0..16),
        dir in arb_node(), parent in arb_node(), last in any::<bool>(),
    ) {
        let mut dir_attr = Attr {
            kind: Kind::Dir, mode: 0o755, nlink: 2, uid: 0, gid: 0, size: 4096, blocks: 8,
            mtime: Ts { sec: 1, nsec: 2 }, ctime: Ts { sec: 3, nsec: 4 }, rdev: 0,
            content_version: 0, symlink_target: None,
        };
        dir_attr.kind = Kind::Dir;
        let msg = EventMsg::SnapDir(SnapDir {
            snap_id: 7, share: ShareId(0), dir, dir_attr, parent,
            entries: entries.into_iter()
                .map(|(name, node, attr)| SnapEntry { name, node, attr }).collect(),
            last,
        });
        let bytes = encode(&msg);
        let back: EventMsg = decode(&bytes).unwrap();
        prop_assert_eq!(back, msg);
    }

    #[test]
    fn ctl_roundtrip(nonce in any::<u64>(), reason in "[a-z]{0,32}") {
        for msg in [CtlMsg::Ping { nonce }, CtlMsg::Goodbye { reason: reason.clone() }] {
            let bytes = encode(&msg);
            let back: CtlMsg = decode(&bytes).unwrap();
            prop_assert_eq!(back, msg);
        }
    }

    /// Hostile input: arbitrary bytes must never panic, only error.
    #[test]
    fn decoder_robust_ctl(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode::<CtlMsg>(&bytes);
        let _ = decode::<RpcReq>(&bytes);
        let _ = decode::<RpcResp>(&bytes);
        let _ = decode::<EventMsg>(&bytes);
    }

    #[test]
    fn frame_header_robust(bytes in proptest::array::uniform16(any::<u8>())) {
        let _ = FrameHeader::decode(&bytes, caps::MAX_FRAME);
    }
}

#[test]
fn name_grammar() {
    assert!(Name::new(*b"ok").is_ok());
    assert!(Name::new(*b".hidden").is_ok());
    assert!(Name::new(*b"").is_err());
    assert!(Name::new(*b".").is_err());
    assert!(Name::new(*b"..").is_err());
    assert!(Name::new(*b"a/b").is_err());
    assert!(Name::new(b"a\0b".to_vec()).is_err());
    assert!(Name::new(vec![b'x'; 256]).is_err());
    assert!(Name::new(vec![b'x'; 255]).is_ok());
}

#[test]
fn traversal_cannot_deserialize() {
    // A hand-encoded Removed record whose name is ".." must fail at decode.
    #[derive(serde::Serialize)]
    struct FakeName<'a>(#[serde(with = "serde_bytes_shim")] &'a [u8]);
    mod serde_bytes_shim {
        pub fn serialize<S: serde::Serializer>(b: &&[u8], s: S) -> Result<S::Ok, S::Error> {
            s.serialize_bytes(b)
        }
    }
    #[derive(serde::Serialize)]
    enum FakeRec<'a> {
        #[allow(dead_code)]
        Created,
        #[allow(dead_code)]
        CreatedBatch,
        Removed {
            parent: mist_proto::NodeKey,
            name: FakeName<'a>,
        },
    }
    let evil = FakeRec::Removed {
        parent: NodeKey {
            ino: 1,
            generation: 1,
        },
        name: FakeName(b".."),
    };
    let bytes = postcard::to_stdvec(&evil).unwrap();
    assert!(decode::<Rec>(&bytes).is_err());
}

#[test]
fn batch_caps_enforced() {
    let rec = Rec::Overflow;
    let too_many = JournalBatch {
        share: ShareId(0),
        first_seq: 1,
        guest_mono_ns: 0,
        records: vec![rec; caps::MAX_RECORDS_PER_BATCH + 1],
    };
    let bytes = encode(&EventMsg::Journal(too_many));
    assert!(decode::<EventMsg>(&bytes).is_err());
}

#[test]
fn trailing_bytes_rejected() {
    let mut bytes = encode(&CtlMsg::Ping { nonce: 9 });
    bytes.push(0);
    assert!(matches!(
        decode::<CtlMsg>(&bytes),
        Err(DecodeError::Trailing)
    ));
}
