//! mist-bench — transport and seed measurement harness.
//!
//! `serve` runs an echo/sink peer (run it in the guest for vsock numbers, or anywhere for TCP).
//! `transport` measures RTT (p50/p99) and bulk throughput (1 and 4 lanes).
//! `seed` drives a real mistd snapshot into a throwaway replica and reports entries/s.

mod fsx;

use anyhow::Context;
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use mist_proto::{CtlMsg, FLAG_MORE, FrameKind, Lane, caps};
use mist_transport::{Endpoint, FramedStream, dial, dial_lane};
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
#[command(name = "mist-bench", about = "Mist transport/seed benchmarks")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Echo/sink server (frames: Ping→Pong, Bulk→discard, Goodbye→byte count).
    Serve {
        /// tcp:0.0.0.0:6479 (mistd-style listen string; vsock:PORT on Linux).
        #[arg(long, default_value = "tcp:0.0.0.0:6479")]
        listen: String,
    },
    /// RTT + throughput against `serve` (or anything frame-echoing).
    Transport {
        /// Endpoint: tcp:host:port | uds:/path | bridge:/path
        #[arg(long)]
        connect: String,
        /// Pings for the RTT histogram.
        #[arg(long, default_value_t = 5000)]
        pings: usize,
        /// Bytes to push for each throughput pass.
        #[arg(long, default_value_t = 2u64 * 1024 * 1024 * 1024)]
        bytes: u64,
        /// Parallel lanes for the aggregate pass.
        #[arg(long, default_value_t = 4)]
        lanes: usize,
    },
    /// Attach to a real mistd, snapshot a share, report entries/s (throwaway replica).
    Seed {
        #[arg(long)]
        connect: String,
        #[arg(long)]
        token: std::path::PathBuf,
        /// Share name (default: first announced).
        #[arg(long)]
        share: Option<String>,
    },
    /// Build a synthetic tree in-process and report replica RSS cost.
    ReplicaMem {
        /// Total file count.
        #[arg(long, default_value_t = 1_000_000)]
        files: u64,
        /// Directory count (files spread evenly).
        #[arg(long, default_value_t = 50_000)]
        dirs: u64,
    },
    /// fsx-style data torture: random write/truncate/read against an in-RAM model, verify equal.
    Fsx {
        /// Target file (created; on a mist mount this tortures the full write path).
        #[arg(long)]
        file: std::path::PathBuf,
        /// Number of operations.
        #[arg(long, default_value_t = 2000)]
        ops: u64,
        /// Deterministic seed.
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Maximum file size the torture may grow to.
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        max_size: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    match Args::parse().cmd {
        Cmd::Serve { listen } => serve(&listen).await,
        Cmd::Transport {
            connect,
            pings,
            bytes,
            lanes,
        } => transport(&connect, pings, bytes, lanes).await,
        Cmd::Seed {
            connect,
            token,
            share,
        } => seed(&connect, &token, share.as_deref()).await,
        Cmd::ReplicaMem { files, dirs } => replica_mem(files, dirs),
        Cmd::Fsx {
            file,
            ops,
            seed,
            max_size,
        } => fsx::run(&file, ops, seed, max_size),
    }
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

async fn serve(listen: &str) -> anyhow::Result<()> {
    fn reply_for(msg: CtlMsg, received: u64) -> Option<CtlMsg> {
        match msg {
            CtlMsg::Ping { nonce } => Some(CtlMsg::Pong {
                nonce,
                guest_mono_ns: 0,
            }),
            CtlMsg::Goodbye { .. } => Some(CtlMsg::Goodbye {
                reason: received.to_string(),
            }),
            _ => None,
        }
    }

    async fn conn(stream: mist_transport::Stream) {
        // Uniform echo loop: an RTT connection sends Ctl Ping frames; a throughput connection
        // sends Bulk frames then a Ctl Goodbye. No lane classification — the first frame is
        // handled the same as any other (a Bulk first frame must not be rejected).
        let mut framed = FramedStream::new(stream, true);
        let mut received: u64 = 0;
        loop {
            let f = match framed.recv().await {
                Ok(f) => f,
                Err(_) => return,
            };
            match f.kind {
                FrameKind::Bulk => received += f.payload.len() as u64,
                FrameKind::Ctl => {
                    if let Ok(msg) = mist_proto::decode::<CtlMsg>(&f.payload)
                        && let Some(reply) = reply_for(msg, received)
                        && framed
                            .send_msg(FrameKind::Ctl, f.seq, &reply)
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(addr) = listen.strip_prefix("tcp:") {
        let l = tokio::net::TcpListener::bind(addr).await?;
        eprintln!("mist-bench serve: tcp {addr}");
        loop {
            let (s, _) = l.accept().await?;
            s.set_nodelay(true)?;
            tokio::spawn(conn(Box::new(s)));
        }
    }
    #[cfg(target_os = "linux")]
    if let Some(port) = listen.strip_prefix("vsock:") {
        let port: u32 = port.parse()?;
        let l = tokio_vsock::VsockListener::bind(tokio_vsock::VsockAddr::new(
            libc::VMADDR_CID_ANY,
            port,
        ))?;
        eprintln!("mist-bench serve: vsock {port}");
        loop {
            let (s, _) = l.accept().await?;
            tokio::spawn(conn(Box::new(s)));
        }
    }
    anyhow::bail!("unsupported listen {listen:?} on this OS")
}

// ---------------------------------------------------------------------------
// transport
// ---------------------------------------------------------------------------

async fn transport(connect: &str, pings: usize, bytes: u64, lanes: usize) -> anyhow::Result<()> {
    let ep = Endpoint::parse(connect)?;

    // RTT.
    let mut framed = FramedStream::new(dial(&ep).await.context("dial")?, false);
    let mut hist = Histogram::<u64>::new(3)?;
    // Warmup.
    for n in 0..100u64 {
        ping_once(&mut framed, n).await?;
    }
    for n in 0..pings as u64 {
        let t = Instant::now();
        ping_once(&mut framed, 1000 + n).await?;
        hist.record(t.elapsed().as_nanos() as u64)?;
    }
    println!(
        "rtt: p50 {:.1}µs  p90 {:.1}µs  p99 {:.1}µs  max {:.1}µs  (n={pings})",
        hist.value_at_quantile(0.50) as f64 / 1000.0,
        hist.value_at_quantile(0.90) as f64 / 1000.0,
        hist.value_at_quantile(0.99) as f64 / 1000.0,
        hist.max() as f64 / 1000.0,
    );

    // Throughput: single lane, then aggregate.
    let single = push_bytes(&ep, bytes).await?;
    println!("throughput 1 lane:  {:.2} GiB/s", single);
    let mut joins = Vec::new();
    let per = bytes / lanes as u64;
    let t = Instant::now();
    for _ in 0..lanes {
        let ep = ep.clone();
        joins.push(tokio::spawn(async move { push_bytes_raw(&ep, per).await }));
    }
    for j in joins {
        j.await??;
    }
    let agg = (bytes as f64 / (1 << 30) as f64) / t.elapsed().as_secs_f64();
    println!("throughput {lanes} lanes: {agg:.2} GiB/s aggregate");
    Ok(())
}

async fn ping_once(framed: &mut FramedStream, nonce: u64) -> anyhow::Result<()> {
    framed
        .send_msg(FrameKind::Ctl, nonce, &CtlMsg::Ping { nonce })
        .await?;
    let (_, msg): (u64, CtlMsg) = framed.recv_msg(FrameKind::Ctl).await?;
    anyhow::ensure!(matches!(msg, CtlMsg::Pong { .. }), "expected Pong");
    Ok(())
}

async fn push_bytes(ep: &Endpoint, total: u64) -> anyhow::Result<f64> {
    let t = Instant::now();
    push_bytes_raw(ep, total).await?;
    Ok((total as f64 / (1 << 30) as f64) / t.elapsed().as_secs_f64())
}

/// Push `total` bytes of bulk frames, then confirm receipt via Goodbye echo.
async fn push_bytes_raw(ep: &Endpoint, total: u64) -> anyhow::Result<()> {
    let mut framed = FramedStream::new(dial(ep).await?, true);
    let chunk = vec![0u8; caps::MAX_FRAME_BULK.min(1 << 20)];
    let mut sent = 0u64;
    let mut seq = 0u64;
    while sent < total {
        let n = chunk.len().min((total - sent) as usize);
        let more = sent + (n as u64) < total;
        framed
            .send_frame(
                FrameKind::Bulk,
                if more { FLAG_MORE } else { 0 },
                seq,
                &chunk[..n],
            )
            .await?;
        sent += n as u64;
        seq += 1;
    }
    framed
        .send_msg(
            FrameKind::Ctl,
            0,
            &CtlMsg::Goodbye {
                reason: "stats".into(),
            },
        )
        .await?;
    let (_, reply): (u64, CtlMsg) = framed.recv_msg(FrameKind::Ctl).await?;
    match reply {
        CtlMsg::Goodbye { reason } => {
            let got: u64 = reason.parse().unwrap_or(0);
            anyhow::ensure!(got >= sent, "server received {got} < sent {sent}");
            Ok(())
        }
        _ => anyhow::bail!("expected Goodbye echo"),
    }
}

// ---------------------------------------------------------------------------
// seed
// ---------------------------------------------------------------------------

async fn seed(connect: &str, token: &std::path::Path, share: Option<&str>) -> anyhow::Result<()> {
    use mist_proto::{EventMsg, PROTO_VERSION};
    let ep = Endpoint::parse(connect)?;
    let token_hash = *blake3_hash_file(token)?.as_bytes();

    let mut ctl = dial_lane(&ep, 0, Lane::Ctl, 0).await?;
    ctl.send_msg(
        FrameKind::Ctl,
        0,
        &CtlMsg::Hello {
            proto: PROTO_VERSION,
            features: mist_proto::features::SUPPORTED,
            token_hash,
            host_name: "mist-bench".into(),
            host_version: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await?;
    let (_, ack): (u64, CtlMsg) = ctl.recv_msg(FrameKind::Ctl).await?;
    let (session_id, infos) = match ack {
        CtlMsg::HelloAck {
            session_id, shares, ..
        } => (session_id, shares),
        CtlMsg::AuthFail => anyhow::bail!("auth failed"),
        _ => anyhow::bail!("expected HelloAck"),
    };
    let info = match share {
        Some(name) => infos
            .iter()
            .find(|i| i.name == name)
            .with_context(|| format!("share {name:?} not announced"))?
            .clone(),
        None => infos.first().context("no shares announced")?.clone(),
    };
    println!(
        "share {:?} (id {:?}, epoch {:x})",
        info.name, info.id, info.epoch
    );

    let mut bulk = dial_lane(&ep, session_id, Lane::Bulk, 0).await?;
    let replica = mist_replica::ShareReplica::new(info.clone());
    let snap_id: u64 = 7;
    ctl.send_msg(FrameKind::Ctl, 0, &CtlMsg::AttachShare { share: info.id })
        .await?;
    ctl.send_msg(
        FrameKind::Ctl,
        0,
        &CtlMsg::SnapshotStart {
            share: info.id,
            snap_id,
        },
    )
    .await?;

    let t = Instant::now();
    let mut last_print = Instant::now();
    loop {
        let f = bulk.recv().await?;
        if f.kind != FrameKind::Event {
            continue;
        }
        match mist_proto::decode::<EventMsg>(&f.payload)? {
            EventMsg::SnapDir(d) => {
                replica.apply_snap_dir(&d);
                if last_print.elapsed() > Duration::from_secs(2) {
                    let s = replica.stats();
                    eprintln!("  … {} nodes, {} dirs", s.nodes, s.dirs);
                    last_print = Instant::now();
                }
            }
            EventMsg::SnapDone(done) => {
                let stats = replica.finish_snapshot(&done);
                let secs = t.elapsed().as_secs_f64();
                println!(
                    "seed: {} entries, {} dirs, {} errors in {:.2}s = {:.0} entries/s",
                    stats.entries,
                    done.dirs,
                    done.errors,
                    secs,
                    stats.entries as f64 / secs.max(1e-9),
                );
                return Ok(());
            }
            EventMsg::Journal(_) => {}
        }
    }
}

fn blake3_hash_file(p: &std::path::Path) -> anyhow::Result<blake3::Hash> {
    Ok(blake3::hash(&std::fs::read(p)?))
}

// ---------------------------------------------------------------------------
// replica-mem (hostd RSS @ 1M nodes ≤ 350 MB)
// ---------------------------------------------------------------------------

fn rss_mb() -> f64 {
    let out = std::process::Command::new("/bin/ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<f64>()
        .unwrap_or(0.0)
        / 1024.0
}

fn replica_mem(files: u64, dirs: u64) -> anyhow::Result<()> {
    use mist_proto::{Attr, Kind, Name, NodeKey, ShareId, ShareInfo, Ts};
    let attr = |kind: Kind, ino: u64| Attr {
        kind,
        mode: 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        size: 1024,
        blocks: 2,
        mtime: Ts {
            sec: 1_700_000_000,
            nsec: 0,
        },
        ctime: Ts {
            sec: 1_700_000_000,
            nsec: 0,
        },
        rdev: 0,
        content_version: ino,
        symlink_target: None,
    };
    let root = NodeKey {
        ino: 2,
        generation: 1,
    };
    let base = rss_mb();
    let t = Instant::now();
    let replica = mist_replica::ShareReplica::new(ShareInfo {
        id: ShareId(1),
        name: "mem".into(),
        epoch: 1,
        fsid: 1,
        root,
        flags: 0,
        ino_bits: 64,
    });
    // Realistic shape: files spread across dirs, names like real source trees (8–24 bytes).
    let per_dir = files.div_ceil(dirs.max(1));
    let mut ino = 100u64;
    let mut emitted = 0u64;
    for d in 0..dirs {
        let dir_key = NodeKey { ino, generation: 1 };
        ino += 1;
        let mut entries = Vec::with_capacity(per_dir as usize + 1);
        for f in 0..per_dir {
            if emitted >= files {
                break;
            }
            emitted += 1;
            let key = NodeKey { ino, generation: 1 };
            ino += 1;
            entries.push(mist_proto::SnapEntry {
                name: Name::new(format!("src_file_{d:05}_{f:04}.rs").into_bytes()).unwrap(),
                node: key,
                attr: attr(Kind::Reg, key.ino),
            });
        }
        // Dir itself hangs off root.
        replica.apply_snap_dir(&mist_proto::SnapDir {
            snap_id: 1,
            share: ShareId(1),
            dir: root,
            dir_attr: attr(Kind::Dir, 2),
            parent: root,
            entries: vec![mist_proto::SnapEntry {
                name: Name::new(format!("dir_{d:06}").into_bytes()).unwrap(),
                node: dir_key,
                attr: attr(Kind::Dir, dir_key.ino),
            }],
            last: false,
        });
        replica.apply_snap_dir(&mist_proto::SnapDir {
            snap_id: 1,
            share: ShareId(1),
            dir: dir_key,
            dir_attr: attr(Kind::Dir, dir_key.ino),
            parent: root,
            entries,
            last: false,
        });
    }
    let built = t.elapsed();
    let stats = replica.stats();
    let after = rss_mb();
    println!(
        "replica-mem: {} nodes / {} dirs / {} entries in {:.2}s",
        stats.nodes,
        stats.dirs,
        stats.entries,
        built.as_secs_f64()
    );
    println!(
        "RSS: {after:.1} MB (baseline {base:.1}) → replica cost ≈ {:.1} MB = {:.0} B/node",
        after - base,
        (after - base) * 1024.0 * 1024.0 / stats.nodes.max(1) as f64
    );
    // Keep the replica alive so RSS reflects it.
    drop(replica);
    Ok(())
}
