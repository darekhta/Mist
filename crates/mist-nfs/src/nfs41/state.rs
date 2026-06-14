//! NFSv4.1 server state (design 05 §5): single-client table-driven model.
//! clientid → sessions → slot tables; open-owners → stateids; delegations keyed by NodeKey.
//! Grace handling is trivial (loopback single client): RECLAIM_COMPLETE is accepted
//! immediately; a server restart invalidates all state and the client recovers.

use mist_proto::NodeKey;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const FORE_SLOTS: u32 = 64;
pub const BACK_SLOTS: u32 = 16;
pub const LEASE_TIME: u32 = 90;

/// One reply-cache slot of a session's fore channel (RFC 5661 §2.10.6).
#[derive(Debug, Default)]
pub struct Slot {
    pub seqid: u32,
    /// Cached encoded result ops for replay (only when the client set sa_cachethis).
    pub reply: Option<Vec<u8>>,
    /// Status-only marker for uncached replies (replay ⇒ NFS4ERR_RETRY_UNCACHED_REP).
    pub had_uncached: bool,
}

#[derive(Debug)]
pub struct Session {
    pub id: [u8; 16],
    pub clientid: u64,
    pub fore_slots: Mutex<Vec<Slot>>,
    /// Client's callback program number (CB_COMPOUND calls use it on the bound connection).
    pub cb_program: u32,
    /// Backchannel sequencing: one in-flight CB at a time is plenty for recalls.
    pub back_slot_seqid: AtomicU32,
}

#[derive(Debug)]
pub struct Client {
    pub clientid: u64,
    pub owner: Vec<u8>,
    pub verifier: [u8; 8],
    pub confirmed: bool,
    pub sequence_id: u32, // EXCHANGE_ID/CREATE_SESSION ordering (csa_sequence)
}

/// An open file's state: stateid "other" → this.
#[derive(Debug, Clone)]
pub struct OpenState {
    pub node: NodeKey,
    pub seqid: u32,
    pub share_access: u32,
    pub owner: Vec<u8>,
}

/// A granted read delegation.
#[derive(Debug, Clone)]
pub struct Delegation {
    pub stateid_other: [u8; 12],
    pub node: NodeKey,
    /// Recall in flight (CB_RECALL sent, DELEGRETURN pending).
    pub recalling: bool,
    pub granted_at: std::time::Instant,
    pub recalled_at: Option<std::time::Instant>,
}

#[derive(Debug, Default)]
pub struct DelegStats {
    pub granted: AtomicU64,
    pub recalls: AtomicU64,
    pub returned: AtomicU64,
    pub revoked: AtomicU64,
}

/// Whole-server NFSv4.1 state. One per exported share server instance.
#[derive(Debug)]
pub struct State {
    pub clients: Mutex<HashMap<u64, Client>>,
    pub sessions: Mutex<HashMap<[u8; 16], Arc<Session>>>,
    pub opens: Mutex<HashMap<[u8; 12], OpenState>>,
    /// Byte-range lock stateids (always-grant policy: single loopback client, so its own lock
    /// bookkeeping is self-consistent; nothing to enforce server-side — v3 `locallocks` parity).
    pub locks: Mutex<HashMap<[u8; 12], NodeKey>>,
    /// node → delegation (at most one per node: single client).
    pub delegations: Mutex<HashMap<NodeKey, Delegation>>,
    pub deleg_stats: DelegStats,
    next_clientid: AtomicU64,
    next_other: AtomicU64,
    /// Server boot verifier (RFC: distinguishes instance restarts).
    pub boot_verifier: [u8; 8],
}

impl State {
    pub fn new() -> Self {
        let boot: u64 = rand_u64();
        State {
            clients: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            delegations: Mutex::new(HashMap::new()),
            deleg_stats: DelegStats::default(),
            next_clientid: AtomicU64::new(rand_u64() | 1),
            next_other: AtomicU64::new(1),
            boot_verifier: boot.to_be_bytes(),
        }
    }

    /// EXCHANGE_ID: find-or-create the client record for `owner`.
    pub fn exchange_id(&self, owner: &[u8], verifier: [u8; 8]) -> (u64, u32) {
        let mut clients = self.clients.lock();
        // Same owner re-exchanging: same clientid if verifier matches, fresh state otherwise.
        if let Some(existing) = clients.values().find(|c| c.owner == owner) {
            if existing.verifier == verifier {
                return (existing.clientid, existing.sequence_id);
            }
            let stale = existing.clientid;
            clients.remove(&stale);
            // Client rebooted: drop its sessions + opens + delegations.
            self.sessions.lock().retain(|_, s| s.clientid != stale);
            self.opens.lock().clear();
            self.delegations.lock().clear();
        }
        let clientid = self.next_clientid.fetch_add(1, Ordering::Relaxed);
        clients.insert(
            clientid,
            Client {
                clientid,
                owner: owner.to_vec(),
                verifier,
                confirmed: false,
                sequence_id: 1,
            },
        );
        (clientid, 1)
    }

    pub fn create_session(&self, clientid: u64, cb_program: u32) -> Option<Arc<Session>> {
        let mut clients = self.clients.lock();
        let c = clients.get_mut(&clientid)?;
        c.confirmed = true;
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&clientid.to_be_bytes());
        id[8..].copy_from_slice(&rand_u64().to_be_bytes());
        let s = Arc::new(Session {
            id,
            clientid,
            fore_slots: Mutex::new((0..FORE_SLOTS).map(|_| Slot::default()).collect()),
            cb_program,
            back_slot_seqid: AtomicU32::new(0),
        });
        self.sessions.lock().insert(id, s.clone());
        Some(s)
    }

    pub fn session(&self, id: &[u8; 16]) -> Option<Arc<Session>> {
        self.sessions.lock().get(id).cloned()
    }

    pub fn destroy_session(&self, id: &[u8; 16]) -> bool {
        self.sessions.lock().remove(id).is_some()
    }

    pub fn destroy_client(&self, clientid: u64) -> bool {
        let removed = self.clients.lock().remove(&clientid).is_some();
        if removed {
            self.sessions.lock().retain(|_, s| s.clientid != clientid);
            self.opens.lock().clear();
            self.locks.lock().clear();
            self.delegations.lock().clear();
        }
        removed
    }

    /// Mint a fresh stateid "other" field.
    pub fn new_other(&self, kind: u8) -> [u8; 12] {
        let n = self.next_other.fetch_add(1, Ordering::Relaxed);
        let mut other = [0u8; 12];
        other[..8].copy_from_slice(&n.to_be_bytes());
        other[8] = kind; // 1 = open, 2 = delegation (debug aid)
        other[9..12].copy_from_slice(&self.boot_verifier[..3]);
        other
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

fn rand_u64() -> u64 {
    // No rand dep in this crate: hash the current time + a counter via blake3.
    use std::time::{SystemTime, UNIX_EPOCH};
    static C: AtomicU64 = AtomicU64::new(0);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut h = blake3::Hasher::new();
    h.update(&t.as_nanos().to_be_bytes());
    h.update(&C.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    h.update(&std::process::id().to_be_bytes());
    u64::from_be_bytes(h.finalize().as_bytes()[..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_then_session() {
        let st = State::new();
        let (cid, seq) = st.exchange_id(b"mac-client", [1; 8]);
        assert_eq!(seq, 1);
        let (cid2, _) = st.exchange_id(b"mac-client", [1; 8]);
        assert_eq!(cid, cid2, "same owner+verifier → same clientid");
        let s = st.create_session(cid, 0x40000000).unwrap();
        assert_eq!(s.clientid, cid);
        assert!(st.session(&s.id).is_some());
        assert!(st.destroy_session(&s.id));
        assert!(st.session(&s.id).is_none());
    }

    #[test]
    fn client_reboot_drops_state() {
        let st = State::new();
        let (cid, _) = st.exchange_id(b"o", [1; 8]);
        st.create_session(cid, 0).unwrap();
        st.opens.lock().insert(
            [3; 12],
            OpenState {
                node: NodeKey {
                    ino: 1,
                    generation: 1,
                },
                seqid: 1,
                share_access: 1,
                owner: vec![],
            },
        );
        let (cid2, _) = st.exchange_id(b"o", [2; 8]); // new verifier = reboot
        assert_ne!(cid, cid2);
        assert!(st.opens.lock().is_empty());
        assert!(st.sessions.lock().is_empty());
    }

    #[test]
    fn destroy_client_drops_stateids() {
        let st = State::new();
        let (cid, _) = st.exchange_id(b"o", [1; 8]);
        st.create_session(cid, 0).unwrap();
        let node = NodeKey {
            ino: 1,
            generation: 1,
        };
        st.opens.lock().insert(
            [3; 12],
            OpenState {
                node,
                seqid: 1,
                share_access: 1,
                owner: vec![],
            },
        );
        st.locks.lock().insert([4; 12], node);
        st.delegations.lock().insert(
            node,
            Delegation {
                stateid_other: [5; 12],
                node,
                recalling: false,
                granted_at: std::time::Instant::now(),
                recalled_at: None,
            },
        );

        assert!(st.destroy_client(cid));
        assert!(st.sessions.lock().is_empty());
        assert!(st.opens.lock().is_empty());
        assert!(st.locks.lock().is_empty());
        assert!(st.delegations.lock().is_empty());
        assert!(!st.destroy_client(cid));
    }
}
