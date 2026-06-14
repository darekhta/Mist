#!/usr/bin/env bash
# Chaos suite (design 10 §6): scripted failure drills against the real AVF stack.
# Each drill asserts recovery per the design 06 §7 matrix, accurate `mist status`, and
# settle-equality between guest truth and the replica/mount view.
#
# Usage: chaos-m6.sh            (boots its own stack; tears down at the end)
# Emits "DRILL <name>: PASS|FAIL (detail)" lines; exit 1 if any drill failed.
set -uo pipefail
cd "$(dirname "$0")/.."

export MIST_STATE_DIR=$PWD/e2e-work/m4-state
MPF=$MIST_STATE_DIR/mnt/dev/fast
MPC=$MIST_STATE_DIR/mnt/dev/code
BRIDGE=/tmp/mist-m4.sock
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
pass() { echo "DRILL $1: PASS${2:+ ($2)}"; }
fail() { echo "DRILL $1: FAIL${2:+ ($2)}"; FAIL=1; }

# --- guest exec over the runner unit (code share) ---------------------------
guest() { # guest <cmd...> -> stdout of command
  # Unique per call even inside subshells (a shared counter diverges across process
  # substitution and collides — a later call then reads the EARLIER command's output).
  local n="c$$-${RANDOM}${RANDOM}"
  mount | grep -q " on $MPC " || return 1
  printf '%s\n' "$*" > "$MPC/.runner/$n.cmd" 2>/dev/null || return 1
  for _ in $(seq 1 90); do
    if [ -f "$MPC/.runner/$n.done" ]; then
      # .out and .done are separate files: wait for .out to be visible too.
      for _ in 1 2 3 4 5; do
        [ -f "$MPC/.runner/$n.out" ] && { cat "$MPC/.runner/$n.out" 2>/dev/null; return 0; }
        sleep 1
      done
      return 0
    fi
    sleep 1
  done
  echo "RUNNER-TIMEOUT"; return 1
}

wait_code_mount() { # block until the code share is kernel-mounted again (restore window)
  for _ in $(seq 1 40); do
    mount | grep -q " on $MPC " && return 0
    sleep 1
  done
  return 1
}

stack_up() {
  pkill -f mist-hostd 2>/dev/null; sleep 1
  pkill -f mist-vmshim 2>/dev/null; sleep 2
  for mp in $(mount | grep -E "127.0.0.1:/" | awk '{print $3}'); do umount -f "$mp" 2>/dev/null; done
  rm -f "$BRIDGE"
  swift/MistBridge/.build/release/mist-vmshim --disk e2e-work/disk.raw --disk e2e-work/tools-m3.img \
    --disk e2e-work/seed-m3.iso --cpus 6 --memory 4 --bridge-sock "$BRIDGE" \
    --mac 5a:94:ef:e4:0c:04 > e2e-work/chaos-console.log 2>&1 &
  echo $! > e2e-work/chaos-vmshim.pid
  start_hostd
  until target/release/mist status 2>/dev/null | grep -q '\[live\]'; do sleep 3; done
  target/release/mist mount dev code --nfs41 >/dev/null
  target/release/mist mount dev fast >/dev/null   # v3+writeback: the mutation-heavy surface
  # Purge stale runner queue (an unprocessed leftover .cmd would head-of-line block us).
  rm -f "$MPC/.runner/"*.cmd "$MPC/.runner/"*.done "$MPC/.runner/"*.out 2>/dev/null
  sleep 2
}

start_hostd() {
  target/release/mist-hostd --vm dev=bridge:$BRIDGE,token=$PWD/e2e-work/token \
    > e2e-work/chaos-hostd.log 2>&1 &
  echo $! > e2e-work/chaos-hostd.pid
  until target/release/mist status >/dev/null 2>&1; do sleep 1; done
}

status_of_vm() { target/release/mist status 2>/dev/null | head -1 | sed 's/.*\[\(.*\)\].*/\1/'; }

wait_live() { # wait_live <secs>
  for _ in $(seq 1 "$1"); do
    target/release/mist status 2>/dev/null | grep -q '\[live\]' && return 0
    sleep 1
  done
  return 1
}

# Background load: continuous small writes + a streaming read on the mount.
load_start() {
  ( i=0; while :; do echo "load-$i" > "$MPF/load-a.txt" 2>/dev/null; i=$((i+1)); sleep 0.05; done ) &
  LOAD1=$!
  ( while :; do cat "$MPF/tp8.bin" > /dev/null 2>&1; sleep 0.2; done ) &
  LOAD2=$!
}
load_stop() { kill "$LOAD1" "$LOAD2" 2>/dev/null; wait "$LOAD1" "$LOAD2" 2>/dev/null; }

# Settle-equality: per-file sha compare, guest truth vs mount view (sampled).
settle_check() { # settle_check <drill>
  sleep 3
  local mismatch=0
  while read -r f; do
    [ -z "$f" ] && continue
    gs=$(guest "sha256sum /srv/fast/$f 2>/dev/null | cut -d' ' -f1")
    ms=$(shasum -a 256 "$MPF/$f" 2>/dev/null | cut -d' ' -f1)
    [ "$gs" = "$ms" ] || { mismatch=1; echo "  divergence: $f guest=$gs mac=$ms"; }
  done < <(guest "cd /srv/fast && find . -type f -not -name 'load-*' | sed 's|^\./||' | head -20")
  return $mismatch
}

# --- boot --------------------------------------------------------------------
say "stack up"
stack_up
dd if=/dev/zero of="$MPF/tp8.bin" bs=1048576 count=8 2>/dev/null; sync
guest "echo guest-ok" | grep -q guest-ok || { echo "runner unusable; aborting"; exit 1; }

# --- drill 1: kill -9 mistd under load ----------------------------------------
say "drill 1: kill -9 mistd under load"
load_start
guest "pkill -9 mistd; sleep 1; systemctl restart mistd; echo restarted" >/dev/null
if wait_live 60; then
  echo "post-restart write" > "$MPF/d1.txt" 2>/dev/null
  sleep 2
  if [ "$(guest "cat /srv/fast/d1.txt 2>/dev/null")" = "post-restart write" ]; then
    pass mistd-kill "session re-established, mutations flow"
  else
    fail mistd-kill "write after recovery did not land"
  fi
else
  fail mistd-kill "session never returned to live"
fi
load_stop

# --- drill 2: kill -9 hostd under load (mount must survive via restore) -------
say "drill 2: kill -9 hostd under load"
load_start
kill -9 "$(cat e2e-work/chaos-hostd.pid)" 2>/dev/null
sleep 2
start_hostd        # stands in for launchd KeepAlive
if wait_live 90; then
  sleep 3          # v3 adoption is immediate; the v4.1 code mount needs a forced remount
  wait_code_mount || echo "(code mount not back after 40s)"
  ok=1
  ls "$MPF" >/dev/null 2>&1 || ok=0
  echo "post-hostd-crash" > "$MPF/d2.txt" 2>/dev/null || ok=0
  sleep 2
  [ "$(guest "cat /srv/fast/d2.txt 2>/dev/null")" = "post-hostd-crash" ] || ok=0
  if [ $ok = 1 ]; then
    pass hostd-kill "kernel mount adopted (same port), read+write resumed without remount"
  else
    fail hostd-kill "mount did not recover after hostd restart"
  fi
else
  fail hostd-kill "hostd restart never reached live"
fi
load_stop

# --- drill 3: journal overflow storm ------------------------------------------
say "drill 3: journal overflow storm (shrunken fanotify queue)"
before=$(guest "cat /proc/sys/fs/fanotify/max_queued_events")
guest "echo 64 > /proc/sys/fs/fanotify/max_queued_events" >/dev/null
guest "cd /srv/fast && mkdir -p storm && for i in \$(seq 1 3000); do echo x > storm/s\$i; done; echo stormed" >/dev/null
guest "echo $before > /proc/sys/fs/fanotify/max_queued_events" >/dev/null
# Overflow forces a resync; poll until the mount view converges (resync of a 22k-node
# share takes seconds; counting mid-reseed reads a partial dir).
sleep 8; wait_live 120
gcount=""; mcount=0
for _ in $(seq 1 12); do
  mcount=$(ls "$MPF/storm" 2>/dev/null | wc -l | tr -d ' ')
  [ "$mcount" = "3000" ] && break
  sleep 5
done
for _ in 1 2 3; do
  gcount=$(guest "ls /srv/fast/storm | wc -l" | tr -d ' ')
  [ -n "$gcount" ] && break
  sleep 3
done
if [ "$gcount" = "3000" ] && [ "$mcount" = "3000" ]; then
  pass overflow "3000/3000 visible after overflow→resync"
else
  fail overflow "guest=$gcount mac=$mcount"
fi
guest "rm -rf /srv/fast/storm" >/dev/null; sleep 3

# --- drill 4: ENOSPC in guest ---------------------------------------------------
say "drill 4: ENOSPC in guest"
guest "sync; sleep 2; sync; echo settled" >/dev/null   # let ext4 release freed blocks first
# A hard-full root fs takes the runner down with it (.out/.done need blocks, and even the
# Mac-side .cmd create crosses the applier into the same fs) — arm a timed cleanup FIRST.
guest "nohup sh -c 'sleep 30; rm -f /srv/fast/fill.bin /srv/fast/fill2.bin; sync' >/dev/null 2>&1 & echo armed" >/dev/null
guest "fallocate -l \$(( \$(df --output=avail -B1 /srv/fast | tail -1) - 65536 )) /srv/fast/fill.bin 2>/dev/null; echo filled" >/dev/null
# df 'avail' excludes the ext4 root reserve (~5%); the applier can dip into it, so top the
# filesystem off completely with a to-ENOSPC dd as root.
guest "dd if=/dev/zero of=/srv/fast/fill2.bin bs=1048576 2>/dev/null; sync; echo topped" >/dev/null || true
if dd if=/dev/zero of="$MPF/d4-big.bin" bs=1048576 count=32 2>/dev/null; then
  # Writes are write-behind; the error must surface at sync/close time at the latest.
  sync2=$(sync 2>&1; echo $?)
  sleep 3
  sleep 1
  if [ -s "$MPF/d4-big.bin" ] && [ "$(stat -f %z "$MPF/d4-big.bin" 2>/dev/null)" = "33554432" ]; then
    # Even claimed-success must not silently lose data; the sync barrier surfaces it.
    fail enospc "write claimed success on a full fs"
  else
    pass enospc "write failed/short as expected (full fs)"
  fi
else
  pass enospc "dd surfaced the error synchronously"
fi
# Wait for the timed guest-side cleanup to free space, then verify recovery.
sleep 35
guest "rm -f /srv/fast/d4-big.bin; sync; echo cleaned" >/dev/null
rm -f "$MPF/d4-big.bin" 2>/dev/null; sleep 2
echo "post-enospc" > "$MPF/d4.txt" && sleep 2
[ "$(guest "cat /srv/fast/d4.txt 2>/dev/null")" = "post-enospc" ] \
  && pass enospc-recovery "writes flow again after space freed" \
  || fail enospc-recovery "writes broken after ENOSPC"

# --- drill 5: memory squeeze on the host while fsx runs -------------------------
say "drill 5: host memory squeeze during fsx"
( memory_pressure -S -l critical >/dev/null 2>&1 & echo $! > e2e-work/chaos-mp.pid )
if target/release/mist-bench fsx --file "$MPF/fsx-chaos.dat" --ops 800 --seed 99 --max-size $((1024*1024)); then
  pass mem-squeeze "fsx 800 ops clean under simulated critical pressure"
else
  fail mem-squeeze "fsx failed under memory pressure"
fi
kill "$(cat e2e-work/chaos-mp.pid 2>/dev/null)" 2>/dev/null
rm -f "$MPF/fsx-chaos.dat"

# --- drill 6: guest clock jump ---------------------------------------------------
say "drill 6: guest clock jump (+2h) mid-churn"
guest "date -s '+2 hours' >/dev/null; echo jumped" >/dev/null
echo "clock-jump-write" > "$MPF/d6.txt"; sleep 2
g6=$(guest "cat /srv/fast/d6.txt 2>/dev/null")
guest "echo t > /srv/fast/d6-guest.txt" >/dev/null
m6=""
for _ in $(seq 1 12); do
  m6=$(cat "$MPF/d6-guest.txt" 2>/dev/null) && [ -n "$m6" ] && break
  sleep 1
done
guest "date -s '-2 hours' >/dev/null; chronyc makestep >/dev/null 2>&1; echo back" >/dev/null
if [ "$g6" = "clock-jump-write" ] && [ "$m6" = "t" ]; then
  pass clock-jump "both directions visible across a +2h guest step"
else
  fail clock-jump "mac→guest=$g6 guest→mac=$m6"
fi

# --- drill 7: guest hard death mid-write (vmshim kill) + full restart ------------
say "drill 7: VM hard death mid-write, then reboot"
load_start
kill -9 "$(cat e2e-work/chaos-vmshim.pid)" 2>/dev/null
sleep 3
load_stop
st=$(status_of_vm)
case "$st" in
  live) fail vm-death "status still [live] with the VM dead";;
  *)    pass vm-death-status "status degraded to [$st]";;
esac
# Replica must still serve stale metadata reads on the mount.
if ls "$MPF" >/dev/null 2>&1; then
  pass vm-death-stale-reads "mount lists from replica with VM down"
else
  fail vm-death-stale-reads "mount unusable while VM down"
fi
# Full stack back up (new epoch => reseed).
stack_up
dd if=/dev/zero of="$MPF/tp8.bin" bs=1048576 count=8 2>/dev/null
echo "post-reboot" > "$MPF/d7.txt"; sleep 2
[ "$(guest "cat /srv/fast/d7.txt 2>/dev/null")" = "post-reboot" ] \
  && pass vm-reboot-recovery "reseeded and read-write after VM reboot" \
  || fail vm-reboot-recovery "stack did not recover after VM reboot"

# --- settle equality + doctor ----------------------------------------------------
say "final settle equality"
if settle_check final; then pass settle "guest and mount agree (sha-verified sample)"; else fail settle "divergence detected"; fi
say "doctor"
target/release/mist doctor && pass doctor "exit 0" || { rc=$?; [ $rc = 1 ] && pass doctor "warnings only" || fail doctor "exit $rc"; }

# --- teardown ----------------------------------------------------------------
say "teardown"
target/release/mist umount dev fast >/dev/null 2>&1
target/release/mist umount dev code >/dev/null 2>&1
pkill -f mist-hostd 2>/dev/null; pkill -f mist-vmshim 2>/dev/null
for mp in $(mount | grep -E "127.0.0.1:/" | awk '{print $3}'); do umount -f "$mp" 2>/dev/null; done

echo
if [ $FAIL = 0 ]; then echo "CHAOS SUITE: ALL DRILLS PASS"; else echo "CHAOS SUITE: FAILURES PRESENT"; fi
exit $FAIL
