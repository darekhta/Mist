//! fsx-style data torture (design 10): random pwrite/truncate/read against an in-RAM model of
//! the file, verifying byte equality throughout. Deterministic from `--seed`, so a failure
//! reproduces exactly. Run with `--file` on a mist mount to torture the full Mac→guest write
//! path (NFS client → loopback server → vsock RPC → guest ext4 → read-back).

use anyhow::{Context, bail};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

/// SplitMix64: tiny deterministic PRNG; no dependency, stable across platforms.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

pub fn run(path: &Path, ops: u64, seed: u64, max_size: usize) -> anyhow::Result<()> {
    let mut rng = Rng(seed);
    let mut model: Vec<u8> = Vec::new();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut log: VecDeque<String> = VecDeque::with_capacity(16);
    let mut counts = [0u64; 4]; // write, truncate, read, reopen
    let started = std::time::Instant::now();

    for op in 0..ops {
        let dice = rng.below(100);
        if dice < 50 {
            // pwrite: random offset within (or right at the end of) the file, random fill byte.
            let off = rng.below(model.len() + 1).min(max_size.saturating_sub(1));
            let len = (1 + rng.below(128 * 1024)).min(max_size - off);
            let byte = (rng.next() & 0xFF) as u8;
            let data = vec![byte; len];
            file.write_at(&data, off as u64)
                .with_context(|| format!("op {op}: pwrite {len}@{off}"))?;
            if model.len() < off + len {
                model.resize(off + len, 0);
            }
            model[off..off + len].copy_from_slice(&data);
            push(
                &mut log,
                format!("{op}: write {len}@{off} byte={byte:#04x}"),
            );
            counts[0] += 1;
        } else if dice < 65 {
            // truncate (grow or shrink).
            let new = rng.below(max_size + 1);
            file.set_len(new as u64)
                .with_context(|| format!("op {op}: truncate {new}"))?;
            model.resize(new, 0);
            push(&mut log, format!("{op}: truncate {new}"));
            counts[1] += 1;
        } else if dice < 95 {
            // read-verify a random range.
            let off = rng.below(model.len() + 1);
            let len = rng.below(256 * 1024).min(model.len().saturating_sub(off));
            let mut buf = vec![0u8; len];
            file.read_exact_at(&mut buf, off as u64)
                .with_context(|| format!("op {op}: read {len}@{off}"))?;
            if buf != model[off..off + len] {
                dump_mismatch(&log, &buf, &model[off..off + len], off);
                bail!("op {op}: read {len}@{off} mismatch (seed {seed})");
            }
            push(&mut log, format!("{op}: read {len}@{off} ok"));
            counts[2] += 1;
        } else {
            // close + reopen: forces close-to-open revalidation on an NFS mount.
            drop(file);
            file = OpenOptions::new().read(true).write(true).open(path)?;
            push(&mut log, format!("{op}: reopen"));
            counts[3] += 1;
        }

        if op % 256 == 255 {
            full_compare(&file, &model, &log, op)?;
        }
    }

    full_compare(&file, &model, &log, ops)?;
    let meta = file.metadata()?;
    if meta.len() != model.len() as u64 {
        bail!(
            "final size mismatch: file {} vs model {}",
            meta.len(),
            model.len()
        );
    }
    println!(
        "fsx ok: {ops} ops in {:.1}s (writes {} truncates {} reads {} reopens {}), final size {}, seed {seed}",
        started.elapsed().as_secs_f64(),
        counts[0],
        counts[1],
        counts[2],
        counts[3],
        model.len()
    );
    let _ = std::fs::remove_file(path);
    Ok(())
}

fn full_compare(
    file: &std::fs::File,
    model: &[u8],
    log: &VecDeque<String>,
    op: u64,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; model.len()];
    file.read_exact_at(&mut buf, 0)
        .with_context(|| format!("op {op}: full read of {} bytes", model.len()))?;
    if buf != model {
        dump_mismatch(log, &buf, model, 0);
        bail!("op {op}: full-file mismatch");
    }
    Ok(())
}

fn push(log: &mut VecDeque<String>, line: String) {
    if log.len() == 16 {
        log.pop_front();
    }
    log.push_back(line);
}

fn dump_mismatch(log: &VecDeque<String>, got: &[u8], want: &[u8], base: usize) {
    let first = got
        .iter()
        .zip(want)
        .position(|(a, b)| a != b)
        .unwrap_or(got.len().min(want.len()));
    eprintln!(
        "MISMATCH at byte {} (got {:#04x} want {:#04x}); last ops:",
        base + first,
        got.get(first).copied().unwrap_or(0),
        want.get(first).copied().unwrap_or(0)
    );
    for l in log {
        eprintln!("  {l}");
    }
}
