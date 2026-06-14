//! Journal hub: owns the per-share fanotify engines, batches their `Rec` streams into
//! `JournalBatch`es (per-share contiguous seq), and broadcasts them to whatever journal lane is
//! currently attached. One global broadcast carries batches for all shares (each tagged by id).

use super::fanotify;
use super::shares::Shares;
use mist_proto::{JournalBatch, Rec, ShareId};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, sync_channel};
use std::time::Duration;
use tokio::sync::broadcast;

const CHANNEL_DEPTH: usize = 65536;
const MAX_RECORDS: usize = 512;
const MAX_BATCH_BYTES: usize = 64 * 1024;
const LINGER: Duration = Duration::from_millis(2);
const BROADCAST_DEPTH: usize = 1024;

#[derive(Clone)]
pub struct JournalHub {
    tx: broadcast::Sender<JournalBatch>,
}

impl std::fmt::Debug for JournalHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JournalHub")
    }
}

impl JournalHub {
    pub fn subscribe(&self) -> broadcast::Receiver<JournalBatch> {
        self.tx.subscribe()
    }
}

/// Start fanotify engines for every share and the per-share batchers feeding the hub.
/// Returns the hub plus the engine handles (kept alive for the process lifetime).
pub fn start(shares: &Arc<Shares>, mistd_pid: i32) -> (JournalHub, Vec<fanotify::FanotifyHandle>) {
    let (tx, _rx0) = broadcast::channel(BROADCAST_DEPTH);
    let hub = JournalHub { tx: tx.clone() };
    let mut handles = Vec::new();

    for share in shares.by_id.values() {
        let (rec_tx, rec_rx) = sync_channel::<Rec>(CHANNEL_DEPTH);
        match fanotify::spawn(share.clone(), rec_tx, mistd_pid) {
            Ok(h) => handles.push(h),
            Err(e) => {
                tracing::error!(share = %share.info_template.name, error = %e,
                    "fanotify engine failed to start; this share will not journal");
                continue;
            }
        }
        let share_id = share.info_template.id;
        let batch_tx = tx.clone();
        // Batcher runs on its own OS thread (it blocks on the std channel with a linger timeout).
        std::thread::Builder::new()
            .name(format!("mist-jbatch-{}", share.info_template.name))
            .spawn(move || batcher(share_id, rec_rx, batch_tx))
            .expect("spawn batcher");
    }
    (hub, handles)
}

fn batcher(share: ShareId, rx: Receiver<Rec>, tx: broadcast::Sender<JournalBatch>) {
    let mut seq: u64 = 1;
    loop {
        // Block for the first record of a batch.
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => return, // engine gone
        };
        let mut records = vec![first];
        let mut bytes = 64usize;
        let deadline = std::time::Instant::now() + LINGER;
        // Accumulate until full or linger expires.
        while records.len() < MAX_RECORDS && bytes < MAX_BATCH_BYTES {
            let now = std::time::Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(r) => {
                    bytes += approx_size(&r);
                    records.push(r);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        let first_seq = seq;
        seq += records.len() as u64;
        let batch = JournalBatch {
            share,
            first_seq,
            guest_mono_ns: 0,
            records,
        };
        // Best-effort broadcast: if no lane is attached, the batch is dropped (the host does a
        // full snapshot on connect, so it only needs events from snapshot-start onward).
        let _ = tx.send(batch);
    }
}

fn approx_size(r: &Rec) -> usize {
    match r {
        Rec::Created { name, .. } => 48 + name.as_bytes().len(),
        Rec::CreatedBatch { entries, .. } => 16 + entries.len() * 64,
        Rec::Removed { name, .. } => 24 + name.as_bytes().len(),
        Rec::Renamed {
            from_name, to_name, ..
        } => 48 + from_name.as_bytes().len() + to_name.as_bytes().len(),
        Rec::AttrChanged { .. } => 64,
        Rec::Content { .. } => 40,
        Rec::SelfRemoved { .. } => 16,
        Rec::Overflow | Rec::EchoMarker { .. } => 8,
    }
}
