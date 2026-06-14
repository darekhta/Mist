#!/usr/bin/env bash
# Compressed soak: sustained mixed load (Mac fsx + Mac metadata storms + guest churn)
# with periodic integrity + leak checks. Usage: soak-m6.sh [minutes] (default 25).
set -uo pipefail
cd "$(dirname "$0")/.."
MINUTES=${1:-25}
export MIST_STATE_DIR=$PWD/e2e-work/m4-state
MPF=$MIST_STATE_DIR/mnt/dev/fast
MPC=$MIST_STATE_DIR/mnt/dev/code
BRIDGE=/tmp/mist-m4.sock
FAIL=0

guest() {
  local n="c$$-${RANDOM}${RANDOM}"
  printf '%s\n' "$*" > "$MPC/.runner/$n.cmd" 2>/dev/null || return 1
  for _ in $(seq 1 60); do
    [ -f "$MPC/.runner/$n.done" ] && { sleep 1; cat "$MPC/.runner/$n.out" 2>/dev/null; return 0; }
    sleep 1
  done
  return 1
}

# Fresh stack
pkill -f mist-hostd 2>/dev/null; sleep 1; pkill -f mist-vmshim 2>/dev/null; sleep 2
for mp in $(mount | grep -E "127.0.0.1:/" | awk '{print $3}'); do umount -f "$mp" 2>/dev/null; done
rm -f "$BRIDGE"
swift/MistBridge/.build/release/mist-vmshim --disk e2e-work/disk.raw --disk e2e-work/tools-m3.img \
  --disk e2e-work/seed-m3.iso --cpus 6 --memory 4 --bridge-sock "$BRIDGE" \
  --mac 5a:94:ef:e4:0c:04 > e2e-work/soak-console.log 2>&1 &
target/release/mist-hostd --vm dev=bridge:$BRIDGE,token=$PWD/e2e-work/token > e2e-work/soak-hostd.log 2>&1 &
HOSTD_PID=$!
until target/release/mist status 2>/dev/null | grep -q '\[live\]'; do sleep 3; done
target/release/mist mount dev code --nfs41 >/dev/null
target/release/mist mount dev fast >/dev/null
rm -f "$MPC/.runner/"* 2>/dev/null; sleep 2

# Guest churn: 2 writers (1 Hz file rewrite + create/delete cycle)
# Create the sentinel BEFORE any loop that guards on it (a guest churn writer whose
# `while [ -f .soak ]` runs before the file exists exits instantly -> false STALE).
touch "$MPF/.soak"; sleep 2   # let the guest see it
guest "nohup sh -c 'i=0; while [ -f /srv/fast/.soak ]; do echo c\$i > /srv/fast/churn.txt; i=\$((i+1)); sleep 1; done' >/dev/null 2>&1 & echo churn1" >/dev/null
guest "nohup sh -c 'while [ -f /srv/fast/.soak ]; do touch /srv/fast/gc-\$\$.tmp; sleep 2; rm -f /srv/fast/gc-\$\$.tmp; sleep 1; done' >/dev/null 2>&1 & echo churn2" >/dev/null

# Mac loads
( s=1; while [ -f "$MPF/.soak" ]; do
    target/release/mist-bench fsx --file "$MPF/soak-fsx.dat" --ops 600 --seed $s --max-size $((2*1024*1024)) >> e2e-work/soak-fsx.log 2>&1 \
      || echo "FSX-FAIL seed=$s" >> e2e-work/soak-fsx.log
    s=$((s+1))
  done ) &
FSX_LOOP=$!
( while [ -f "$MPF/.soak" ]; do find "$MPC/tree" -type f > /dev/null 2>&1; sleep 5; done ) &
STORM_LOOP=$!

RSS0=$(ps -o rss= -p $HOSTD_PID | tr -d ' ')
echo "soak started: ${MINUTES}m, hostd rss ${RSS0}KB"
END=$(( $(date +%s) + MINUTES*60 ))
TICK=0
while [ "$(date +%s)" -lt "$END" ]; do
  sleep 60
  TICK=$((TICK+1))
  RSS=$(ps -o rss= -p $HOSTD_PID | tr -d ' ' 2>/dev/null)
  [ -z "$RSS" ] && { echo "SOAK FAIL: hostd died"; FAIL=1; break; }
  # churn freshness: guest 1 Hz writer must be visible on the mount within actimeo+2s
  C1=$(cat "$MPF/churn.txt" 2>/dev/null); sleep 8; C2=$(cat "$MPF/churn.txt" 2>/dev/null)
  FRESH=ok; [ "$C1" = "$C2" ] && { FRESH=STALE; FAIL=1; }
  echo "t+${TICK}m rss=${RSS}KB churn=$FRESH fsx_runs=$(grep -c 'fsx ok' e2e-work/soak-fsx.log 2>/dev/null)"
done
rm -f "$MPF/.soak"
sleep 3; kill $FSX_LOOP $STORM_LOOP 2>/dev/null

FSXOK=$(grep -c 'fsx ok' e2e-work/soak-fsx.log 2>/dev/null); FSXFAIL=$(grep -c 'FSX-FAIL' e2e-work/soak-fsx.log 2>/dev/null)
RSS1=$(ps -o rss= -p $HOSTD_PID | tr -d ' ' 2>/dev/null)
CONFLICTS=$(target/release/mist conflicts 2>/dev/null | tail -1)
# settle integrity
sleep 3
MISMATCH=0
while read -r f; do
  [ -z "$f" ] && continue
  gs=$(guest "sha256sum /srv/fast/$f 2>/dev/null | cut -d' ' -f1")
  ms=$(shasum -a 256 "$MPF/$f" 2>/dev/null | cut -d' ' -f1)
  [ "$gs" = "$ms" ] || { MISMATCH=1; echo "divergence: $f"; }
done < <(guest "cd /srv/fast && find . -type f -name '*.dat' -o -type f -name 'churn*' | sed 's|^\./||' | head -5")
[ $MISMATCH = 1 ] && FAIL=1
[ "$FSXFAIL" != "0" ] && FAIL=1

echo "soak done: fsx ok=$FSXOK fail=$FSXFAIL, rss ${RSS0}->${RSS1}KB, conflicts: $CONFLICTS"
target/release/mist umount dev fast >/dev/null 2>&1; target/release/mist umount dev code >/dev/null 2>&1
pkill -f mist-hostd 2>/dev/null; pkill -f mist-vmshim 2>/dev/null
[ $FAIL = 0 ] && echo "SOAK: PASS" || echo "SOAK: FAIL"
exit $FAIL
