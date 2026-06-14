//! Frame round-trips over real sockets (UDS + TCP) and the bridge handshake.

use mist_proto::{CtlMsg, FrameHeader, FrameKind, Lane, caps};
use mist_transport::{Endpoint, FramedStream, classify_accepted, dial, dial_lane};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn uds_frame_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();

    let server = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        let (mut framed, msg) = classify_accepted(Box::new(s)).await.unwrap();
        assert!(matches!(msg, CtlMsg::Ping { nonce: 42 }));
        framed
            .send_msg(
                FrameKind::Ctl,
                0,
                &CtlMsg::Pong {
                    nonce: 42,
                    guest_mono_ns: 1,
                },
            )
            .await
            .unwrap();
        framed.flush().await.unwrap();
    });

    let ep = Endpoint::parse(&format!("uds:{}", path.display())).unwrap();
    let mut framed = FramedStream::new(dial(&ep).await.unwrap(), false);
    framed
        .send_msg(FrameKind::Ctl, 0, &CtlMsg::Ping { nonce: 42 })
        .await
        .unwrap();
    framed.flush().await.unwrap();
    let (_, pong): (u64, CtlMsg) = framed.recv_msg(FrameKind::Ctl).await.unwrap();
    assert!(matches!(pong, CtlMsg::Pong { nonce: 42, .. }));
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_lane_hello() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        let (_framed, msg) = classify_accepted(Box::new(s)).await.unwrap();
        match msg {
            CtlMsg::StreamHello {
                session_id,
                lane,
                idx,
            } => {
                assert_eq!(session_id, 7);
                assert_eq!(lane, Lane::Bulk);
                assert_eq!(idx, 1);
            }
            other => panic!("unexpected: {other:?}"),
        }
    });

    let ep = Endpoint::Tcp(addr.to_string());
    let _lane = dial_lane(&ep, 7, Lane::Bulk, 1).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn bridge_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vm.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();

    // Fake MistBridge: parse CONNECT line, ack, then echo one frame.
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut line = Vec::new();
        loop {
            let mut b = [0u8; 1];
            s.read_exact(&mut b).await.unwrap();
            if b[0] == b'\n' {
                break;
            }
            line.push(b[0]);
        }
        assert_eq!(
            line,
            format!("CONNECT {}", mist_proto::VSOCK_PORT).into_bytes()
        );
        s.write_all(b"OK 6478\n").await.unwrap();
        let (mut framed, msg) = classify_accepted(Box::new(s)).await.unwrap();
        assert!(matches!(msg, CtlMsg::Ping { nonce: 9 }));
        framed
            .send_msg(
                FrameKind::Ctl,
                0,
                &CtlMsg::Pong {
                    nonce: 9,
                    guest_mono_ns: 0,
                },
            )
            .await
            .unwrap();
    });

    let ep = Endpoint::parse(&format!("bridge:{}", path.display())).unwrap();
    let mut framed = FramedStream::new(dial(&ep).await.unwrap(), false);
    framed
        .send_msg(FrameKind::Ctl, 0, &CtlMsg::Ping { nonce: 9 })
        .await
        .unwrap();
    let (_, pong): (u64, CtlMsg) = framed.recv_msg(FrameKind::Ctl).await.unwrap();
    assert!(matches!(pong, CtlMsg::Pong { nonce: 9, .. }));
    server.await.unwrap();
}

#[tokio::test]
async fn oversized_frame_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();

    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        // Hand-craft a header claiming 2 MiB payload on a non-bulk stream.
        let hdr = mist_proto::FrameHeader {
            len: 2 * 1024 * 1024,
            kind: FrameKind::Ctl,
            flags: 0,
            seq: 0,
        };
        s.write_all(&hdr.encode()).await.unwrap();
    });

    let ep = Endpoint::Uds(path.clone());
    let mut framed = FramedStream::new(dial(&ep).await.unwrap(), false);
    assert!(framed.recv().await.is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn oversized_send_frame_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();

    let server = tokio::spawn(async move {
        let (_s, _) = listener.accept().await.unwrap();
    });

    let ep = Endpoint::Uds(path.clone());
    let mut framed = FramedStream::new(dial(&ep).await.unwrap(), false);
    let payload = vec![0u8; mist_proto::caps::MAX_FRAME + 1];
    let err = framed
        .send_frame(FrameKind::Ctl, 0, 0, &payload)
        .await
        .unwrap_err();
    assert!(matches!(err, mist_transport::TransportError::Protocol(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn classify_keeps_rpc_lane_on_normal_cap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();

    let server = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        let (mut framed, msg) = classify_accepted(Box::new(s)).await.unwrap();
        assert!(matches!(
            msg,
            CtlMsg::StreamHello {
                lane: Lane::Rpc,
                ..
            }
        ));
        assert!(framed.recv().await.is_err());
    });

    let ep = Endpoint::Uds(path.clone());
    let mut s = dial(&ep).await.unwrap();
    let hello = mist_proto::encode(&CtlMsg::StreamHello {
        session_id: 1,
        lane: Lane::Rpc,
        idx: 0,
    });
    let hdr = FrameHeader {
        len: hello.len() as u32,
        kind: FrameKind::Ctl,
        flags: 0,
        seq: 0,
    };
    s.write_all(&hdr.encode()).await.unwrap();
    s.write_all(&hello).await.unwrap();

    let hdr = FrameHeader {
        len: (caps::MAX_FRAME + 1) as u32,
        kind: FrameKind::Req,
        flags: 0,
        seq: 1,
    };
    s.write_all(&hdr.encode()).await.unwrap();
    server.await.unwrap();
}
