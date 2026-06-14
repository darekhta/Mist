//! Large-buffer pool for the streaming read path. A fresh ≥1 MiB Vec is mmap-backed and pays
//! soft page faults on first touch (~200 µs per reply at rsize) — recycling buffers keeps the
//! pages resident. Strictly an optimization: take() falls back to a fresh allocation.

use parking_lot::Mutex;

static POOL: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

const POOL_CAP: usize = 32;
const MIN_KEEP: usize = 1024 * 1024;
const MAX_KEEP: usize = 4 * 1024 * 1024 + 4096;

/// An empty Vec with at least `min_cap` capacity (recycled when possible).
pub fn take(min_cap: usize) -> Vec<u8> {
    {
        let mut pool = POOL.lock();
        if let Some(idx) = pool.iter().position(|b| b.capacity() >= min_cap) {
            let mut b = pool.swap_remove(idx);
            b.clear();
            return b;
        }
    }
    Vec::with_capacity(min_cap)
}

/// Return a buffer to the pool (dropped if tiny, huge, or the pool is full).
pub fn give(buf: Vec<u8>) {
    let cap = buf.capacity();
    if !(MIN_KEEP..=MAX_KEEP).contains(&cap) {
        return;
    }
    let mut pool = POOL.lock();
    if pool.len() < POOL_CAP {
        pool.push(buf);
    }
}
