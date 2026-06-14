#!/usr/bin/env bash
# CAS acceptance checks on real hardware (design 09): warm-after-restart zero guest reads, CAS hit
# throughput, corrupt-blob drill, churn invalidation, eviction watermarks.
# Reuses the write-path guest provisioning: run `e2e-m3.sh prepare` first.
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=e2e-work
STATE="$PWD/$WORK/m4-state"
VMSHIM=swift/MistBridge/.build/release/mist-vmshim
BRIDGE_SOCK=/tmp/mist-m4.sock
MAC="5a:94:ef:e4:0c:04"
CACHE_MAX=$((256 * 1024 * 1024)) # small cap so the eviction gate runs in minutes

mist() { MIST_STATE_DIR="$STATE" target/release/mist "$@"; }
MP="$STATE/mnt/dev/code"

clear_stale_mounts() {
  for mp in $(mount | grep -iE "127.0.0.1:/" | awk '{print $3}'); do
    umount -f "$mp" 2>/dev/null || true
  done
}

hostd_stop() {
  [ -f "$WORK/m4-hostd.pid" ] && kill "$(cat "$WORK/m4-hostd.pid")" 2>/dev/null || true
  rm -f "$WORK/m4-hostd.pid"
  sleep 1
}

# Start hostd against the running VM. Returns 1 if the seed never arrives.
hostd_start() {
  MIST_STATE_DIR="$STATE" nohup target/release/mist-hostd \
    --vm "dev=bridge:$BRIDGE_SOCK,token=$PWD/$WORK/token" --cache-max-bytes "$CACHE_MAX" \
    >> "$WORK/m4-hostd.log" 2>&1 &
  echo $! > "$WORK/m4-hostd.pid"
  for i in $(seq 1 60); do
    sleep 1
    mist status 2>/dev/null | grep -q "\[live\]" && return 0
  done
  echo "hostd never seeded"
  return 1
}

vm_boot() {
  rm -f "$BRIDGE_SOCK"
  "$VMSHIM" --disk "$WORK/disk.raw" --disk "$WORK/tools-m3.img" --disk "$WORK/seed-m3.iso" \
    --cpus 6 --memory 4 --bridge-sock "$BRIDGE_SOCK" --mac "$MAC" > "$WORK/m4-console.log" 2>&1 &
  echo $! > "$WORK/m4-vmshim.pid"
}

# AVF gotcha: a boot can come up with vsock "half-up" (connects never complete) and that
# sticks until the VM reboots. Retry the whole VM+hostd bring-up a few times.
boot_until_live() {
  for attempt in 1 2 3; do
    vm_boot
    echo "booting guest (attempt $attempt)…"
    sleep 3
    hostd_start && return 0
    echo "boot $attempt: guest never reachable; rebooting VM"
    hostd_stop
    kill "$(cat "$WORK/m4-vmshim.pid")" 2>/dev/null || true
    sleep 2
  done
  echo "VM never reachable after 3 boots"; exit 1
}

vm_stop() {
  clear_stale_mounts
  [ -f "$WORK/m4-vmshim.pid" ] && kill "$(cat "$WORK/m4-vmshim.pid")" 2>/dev/null || true
  rm -f "$WORK/m4-vmshim.pid"
  hostd_stop
}

remount() {
  mist umount dev code 2>/dev/null || true
  # A hostd restart leaves the previous mount stale (dead server); force-clear it so the new
  # mount lands on a clean dir instead of hanging on the dead one.
  umount -f "$MP" 2>/dev/null || true
  mist mount dev code >/dev/null
}

# jq-less JSON field reader: cache_field <field> (from `mist cache --json`, vm dev)
cache_field() {
  mist cache --json | python3 -c "
import json,sys
v=json.load(sys.stdin)['vms'][0]
f='$1'
print(v.get(f) if f in v else v['stats'][f])"
}

cleanup() { vm_stop; }
trap cleanup EXIT

# ---- boot ---------------------------------------------------------------------------------
[ -x "$VMSHIM" ] || { echo "run e2e-m3.sh prepare first"; exit 1; }
pkill -f mist-vmshim 2>/dev/null || true; pkill -f mist-hostd 2>/dev/null || true; sleep 1
clear_stale_mounts; rm -rf "$STATE"; mkdir -p "$STATE"; rm -f "$BRIDGE_SOCK" "$WORK/m4-hostd.log"
boot_until_live
mist mount dev code >/dev/null

out="bench/results/$(date +%Y-%m-%d)-cas-gates.md"; mkdir -p bench/results
exec > >(tee "$out") 2>&1
echo "# Mist CAS gates — $(date '+%Y-%m-%d %H:%M')"
echo
echo "cache cap for this run: $CACHE_MAX bytes (256 MiB; eviction gate at test scale)"
fail=0

# ---- gate 1: cold → warm read, cache populated ---------------------------------------------
echo; echo "## Gate 1 — cold vs warm read (64 MiB)"
dd if=/dev/urandom of="$MP/blob64.bin" bs=1048576 count=64 2>/dev/null
SHA0=$(shasum -a 256 "$MP/blob64.bin" | awk '{print $1}')
remount   # defeat the macOS client cache
t0=$(python3 -c 'import time;print(time.monotonic())')
SHA_COLD=$(shasum -a 256 "$MP/blob64.bin" | awk '{print $1}')
t1=$(python3 -c 'import time;print(time.monotonic())')
remount
t2=$(python3 -c 'import time;print(time.monotonic())')
SHA_WARM=$(shasum -a 256 "$MP/blob64.bin" | awk '{print $1}')
t3=$(python3 -c 'import time;print(time.monotonic())')
COLD=$(python3 -c "print(f'{64/($t1-$t0):.0f}')")
WARM=$(python3 -c "print(f'{64/($t3-$t2):.0f}')")
echo "cold: ${COLD} MiB/s   warm: ${WARM} MiB/s   blobs=$(cache_field blobs) hits=$(cache_field hits)"
[ "$SHA0" = "$SHA_COLD" ] && [ "$SHA0" = "$SHA_WARM" ] && echo "sha integrity ✓" || { echo "SHA MISMATCH ✗"; fail=1; }
[ "$(cache_field blobs)" -ge 16 ] && echo "cache populated ✓" || { echo "cache not populated ✗"; fail=1; }

# ---- gate 2: warm after hostd restart, ZERO guest reads ------------------------------------
echo; echo "## Gate 2 — warm after hostd restart (zero guest reads)"
hostd_stop
hostd_start || { echo "hostd restart never reattached ✗"; fail=1; }
remount
OPS_BEFORE=$(cache_field guest_read_ops)
SHA_RESTART=$(shasum -a 256 "$MP/blob64.bin" | awk '{print $1}')
OPS_AFTER=$(cache_field guest_read_ops)
echo "guest_read_ops during warm read: $((OPS_AFTER - OPS_BEFORE)) (want 0)"
[ "$SHA0" = "$SHA_RESTART" ] && echo "sha integrity ✓" || { echo "SHA MISMATCH ✗"; fail=1; }
[ "$((OPS_AFTER - OPS_BEFORE))" -eq 0 ] && echo "zero guest reads ✓" || { echo "guest reads observed ✗"; fail=1; }

# ---- gate 3: corrupt-blob drill -------------------------------------------------------------
echo; echo "## Gate 3 — corrupt-blob drill"
BLOB=$(find "$STATE/cas/dev/blobs" -type f | head -1)
python3 - "$BLOB" <<'EOF'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
b[len(b)//2] ^= 0xff
open(p, 'wb').write(b)
EOF
echo "flipped one byte in $(basename "$BLOB")"
mist cache scrub | sed 's/^/  /'
CORRUPT=$(cache_field corrupt_dropped)
remount
SHA_HEAL=$(shasum -a 256 "$MP/blob64.bin" | awk '{print $1}')
[ "$CORRUPT" -ge 1 ] && echo "scrub detected+dropped ✓" || { echo "scrub missed corruption ✗"; fail=1; }
[ "$SHA0" = "$SHA_HEAL" ] && echo "self-heal refetch ✓" || { echo "SHA MISMATCH after heal ✗"; fail=1; }

# ---- gate 4: invalidation under churn --------------------------------------------------------
echo; echo "## Gate 4 — content_version invalidation under churn (guest writes 1 Hz)"
A=$(cat "$MP/churn.txt")
sleep 7   # > actimeo=5 so the client revalidates; fingerprint must rotate with guest mtime
B=$(cat "$MP/churn.txt")
echo "t0: $A"; echo "t1: $B"
[ "$A" != "$B" ] && echo "fresh content under churn ✓" || { echo "stale read under churn ✗"; fail=1; }

# ---- gate 5: eviction honors watermarks under streaming --------------------------------------
echo; echo "## Gate 5 — eviction watermarks (stream 512 MiB through a 256 MiB cache)"
dd if=/dev/urandom of="$MP/stream512.bin" bs=1048576 count=512 2>/dev/null
remount
shasum -a 256 "$MP/stream512.bin" >/dev/null   # streams 512 MiB of reads through the CAS
sleep 2   # let trailing async ingests settle before reading stats
TOTAL=$(cache_field total_bytes)
EVICTIONS=$(cache_field evictions)
echo "total_bytes=$TOTAL (cap $CACHE_MAX)  evictions=$EVICTIONS"
[ "$TOTAL" -le "$CACHE_MAX" ] && echo "hard cap honored ✓" || { echo "cache exceeded cap ✗"; fail=1; }
[ "$EVICTIONS" -ge 1 ] && echo "eviction ran ✓" || { echo "no eviction ✗"; fail=1; }

# ---- summary ---------------------------------------------------------------------------------
echo; echo "## Summary"
mist cache | sed 's/^/  /'
rm -f "$MP/blob64.bin" "$MP/stream512.bin" 2>/dev/null || true
if [ "$fail" = 0 ]; then echo; echo "ALL CAS GATES PASS"; else echo; echo "GATE FAILURES"; exit 1; fi
