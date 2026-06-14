#!/usr/bin/env bash
# One-target measurement pass for the competitive comparison (design 08 §6).
# Usage: bench-compare.sh <label> <mountpoint> <remount-cmd...>
# The tree lives at <mountpoint>/tree (20k files / 2k dirs, 1 KiB each).
# Emits "metric=value" lines; the orchestrator collects them into the table.
set -uo pipefail
LABEL=$1; MP=$2; shift 2
REMOUNT=("$@")

remount() { "${REMOUNT[@]}" >/dev/null 2>&1; sleep 0.3; }

ms() { python3 -c 'import time;print(int(time.monotonic()*1000))'; }

echo "## $LABEL"

# Cold enumeration (fresh mount = cold client caches).
remount
t0=$(ms); n=$(find "$MP/tree" -type f 2>/dev/null | wc -l | tr -d ' '); t1=$(ms)
echo "cold_enum_ms=$((t1-t0)) files=$n"

# Warm enumeration.
t0=$(ms); find "$MP/tree" -type f > /dev/null 2>&1; t1=$(ms)
echo "warm_enum_ms=$((t1-t0))"

# Stat storm (lstat every entry, warm).
t0=$(ms); find "$MP/tree" -ls > /dev/null 2>&1; t1=$(ms)
echo "stat_storm_ms=$((t1-t0))"

# Hot open/read/close loop (in-process).
python3 - "$MP/tree/d0000/f0.c" <<'EOF'
import sys, time
p = sys.argv[1]
with open(p, 'rb') as f:
    f.read()  # warm it once
t0 = time.monotonic()
N = 5000
for _ in range(N):
    with open(p, 'rb') as f:
        f.read()
dt = time.monotonic() - t0
print(f"hotloop_us_per={dt/N*1e6:.1f}")
EOF

# 64 MiB sequential write.
t0=$(ms); dd if=/dev/zero of="$MP/tp64.bin" bs=1048576 count=64 2>/dev/null; sync; t1=$(ms)
echo "write64_MBps=$(python3 -c "print(f'{64*1000/($t1-$t0):.0f}')")"

# 64 MiB read after remount (cold client cache; server/CAS may be warm — that's the product).
remount
t0=$(ms); dd if="$MP/tp64.bin" of=/dev/null bs=1048576 2>/dev/null; t1=$(ms)
echo "read64_MBps=$(python3 -c "print(f'{64*1000/($t1-$t0):.0f}')")"
rm -f "$MP/tp64.bin" 2>/dev/null

# Small-file creates (200 × 1 KiB).
mkdir -p "$MP/mk" 2>/dev/null
t0=$(ms)
for i in $(seq 1 200); do head -c 1024 /dev/zero > "$MP/mk/c$i.txt"; done
t1=$(ms)
echo "create_ms_per=$(python3 -c "print(f'{($t1-$t0)/200:.2f}')")"
rm -rf "$MP/mk" 2>/dev/null

# Freshness: guest writes epoch into churn.txt at 1 Hz. A coherent mount sees a new value
# ~every 1 s; an attr-cached mount sees them in jumps of ~actimeo. Report median interval
# between observed distinct values over 12 s.
python3 - "$MP/churn.txt" <<'EOF'
import sys, time, statistics
p = sys.argv[1]
try:
    last = open(p).read()
except OSError:
    print("freshness_ms=n/a")
    sys.exit()
changes = []
t_last = time.monotonic()
t_end = t_last + 12
while time.monotonic() < t_end:
    try:
        cur = open(p).read()
    except OSError:
        time.sleep(0.02)
        continue
    if cur != last:
        now = time.monotonic()
        changes.append((now - t_last) * 1000)
        t_last = now
        last = cur
    time.sleep(0.02)
if changes:
    print(f"freshness_ms={statistics.median(changes):.0f}")
else:
    print("freshness_ms=stale(>12000)")
EOF
