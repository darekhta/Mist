//! Conflict detection (design 06 §conflicts): last-close-wins with a *visible* log.
//!
//! With the write-through surface there is no host-side dirty state, so a conflict is two
//! sides mutating the same node within a close-to-open window: a guest journal record landing
//! soon after a Mac mutation (the journal apply overwrites what the Mac believes it wrote), or a
//! Mac write landing soon after a guest change (the Mac clobbers fresh guest work). Both are
//! legal under last-close-wins; the tracker's job is to make them *observable* via
//! `mist conflicts` instead of silent.

use mist_proto::NodeKey;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Two mutations of one node within this window count as a conflict (close-to-open granularity).
const WINDOW: Duration = Duration::from_secs(5);
const MAX_ROWS: usize = 1024;
const MAX_TRACKED: usize = 8192;

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConflictRow {
    pub at_unix_ms: u64,
    pub share: u16,
    pub ino: u64,
    pub generation: u32,
    pub path: Option<String>,
    /// `mac-over-guest`: Mac wrote ≤window after a guest change (Mac wins).
    /// `guest-over-mac`: guest changed ≤window after a Mac write (guest wins).
    pub kind: &'static str,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct ConflictTracker {
    recent_mac: Mutex<HashMap<(u16, NodeKey), Instant>>,
    recent_guest: Mutex<HashMap<(u16, NodeKey), Instant>>,
    rows: Mutex<VecDeque<ConflictRow>>,
    total: AtomicU64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn prune(map: &mut HashMap<(u16, NodeKey), Instant>) {
    if map.len() > MAX_TRACKED {
        map.retain(|_, t| t.elapsed() < WINDOW);
    }
}

impl ConflictTracker {
    /// A Mac-originated mutation of `node` (write/truncate/remove target). Logs a conflict if the
    /// guest changed the same node within the window.
    pub fn note_mac(&self, share: u16, node: NodeKey, path: Option<String>, op: &str) {
        let guest_recent = self
            .recent_guest
            .lock()
            .get(&(share, node))
            .is_some_and(|t| t.elapsed() < WINDOW);
        if guest_recent {
            self.push(ConflictRow {
                at_unix_ms: now_ms(),
                share,
                ino: node.ino,
                generation: node.generation,
                path,
                kind: "mac-over-guest",
                detail: format!(
                    "Mac {op} ≤{}s after a guest-side change; last-close-wins → Mac version kept",
                    WINDOW.as_secs()
                ),
            });
        }
        let mut m = self.recent_mac.lock();
        m.insert((share, node), Instant::now());
        prune(&mut m);
    }

    /// A guest-originated journal change of `node`. Logs a conflict if the Mac mutated the same
    /// node within the window.
    pub fn note_guest(&self, share: u16, node: NodeKey, path: Option<String>) {
        let mac_recent = self
            .recent_mac
            .lock()
            .get(&(share, node))
            .is_some_and(|t| t.elapsed() < WINDOW);
        if mac_recent {
            self.push(ConflictRow {
                at_unix_ms: now_ms(),
                share,
                ino: node.ino,
                generation: node.generation,
                path,
                kind: "guest-over-mac",
                detail: format!(
                    "guest changed ≤{}s after a Mac write; last-close-wins → guest version kept",
                    WINDOW.as_secs()
                ),
            });
        }
        let mut m = self.recent_guest.lock();
        m.insert((share, node), Instant::now());
        prune(&mut m);
    }

    fn push(&self, row: ConflictRow) {
        self.total.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            share = row.share,
            ino = row.ino,
            path = row.path.as_deref().unwrap_or("?"),
            kind = row.kind,
            "conflict detected"
        );
        let mut rows = self.rows.lock();
        if rows.len() >= MAX_ROWS {
            rows.pop_front();
        }
        rows.push_back(row);
    }

    pub fn list(&self) -> Vec<ConflictRow> {
        self.rows.lock().iter().cloned().collect()
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: NodeKey = NodeKey {
        ino: 42,
        generation: 7,
    };

    #[test]
    fn guest_then_mac_conflicts() {
        let t = ConflictTracker::default();
        t.note_guest(1, N, Some("/f".into()));
        t.note_mac(1, N, Some("/f".into()), "write");
        let rows = t.list();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, "mac-over-guest");
        assert_eq!(t.total(), 1);
    }

    #[test]
    fn mac_then_guest_conflicts() {
        let t = ConflictTracker::default();
        t.note_mac(1, N, None, "write");
        t.note_guest(1, N, None);
        assert_eq!(t.list()[0].kind, "guest-over-mac");
    }

    #[test]
    fn distinct_nodes_do_not_conflict() {
        let t = ConflictTracker::default();
        t.note_mac(1, N, None, "write");
        t.note_guest(
            1,
            NodeKey {
                ino: 43,
                generation: 7,
            },
            None,
        );
        assert!(t.list().is_empty());
    }
}
