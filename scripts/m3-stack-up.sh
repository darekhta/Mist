#!/usr/bin/env bash
# Boot the write-path VM + hostd and mount code+fast, then EXIT LEAVING THE STACK RUNNING
# (unlike e2e-m3.sh whose EXIT trap tears everything down). For interactive perf work.
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=e2e-work
STATE="$PWD/$WORK/m3-state"
VMSHIM=swift/MistBridge/.build/release/mist-vmshim
BRIDGE_SOCK=/tmp/mist-m3.sock
MAC="5a:94:ef:e4:0c:03"

mist() { MIST_STATE_DIR="$STATE" target/release/mist "$@"; }

for mp in $(mount | grep -iE "127.0.0.1:/" | awk '{print $3}'); do
  umount -f "$mp" 2>/dev/null || true
done
[ -f "$WORK/m3-vmshim.pid" ] && kill "$(cat "$WORK/m3-vmshim.pid")" 2>/dev/null || true
pkill -f mist-vmshim 2>/dev/null || true
pkill -f mist-hostd 2>/dev/null || true
sleep 1
rm -f "$BRIDGE_SOCK"

nohup "$VMSHIM" --disk "$WORK/disk.raw" --disk "$WORK/tools-m3.img" --disk "$WORK/seed-m3.iso" \
  --cpus 6 --memory 4 --bridge-sock "$BRIDGE_SOCK" --mac "$MAC" > "$WORK/m3-console.log" 2>&1 &
echo $! > "$WORK/m3-vmshim.pid"
echo "booting guest…"
rm -rf "$STATE"; mkdir -p "$STATE"
MIST_STATE_DIR="$STATE" nohup target/release/mist-hostd \
  --vm "dev=bridge:$BRIDGE_SOCK,token=$PWD/$WORK/token" > "$WORK/m3-hostd.log" 2>&1 &
echo $! > "$WORK/m3-hostd.pid"

for i in $(seq 1 150); do
  sleep 2
  if mist status 2>/dev/null | grep -q "\[live\]"; then echo "seeded (~$((i*2))s)"; break; fi
  [ "$i" = 150 ] && { echo "seed timeout"; tail -20 "$WORK/m3-console.log" | tr -d '\r'; exit 1; }
done
mist mount dev code
mist mount dev fast
mist status
echo "stack up: hostd pid $(cat "$WORK/m3-hostd.pid"), mounts under $STATE/mnt/dev/"
