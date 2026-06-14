#!/bin/bash
# Mist write-path end-to-end: write through the Mac NFS mount → lands in the guest ext4.
#
# Validates the write path on a real Debian guest under AVF: mount the share read-write via
# the loopback NFSv3 server, then create/write/edit/rename/rm files *from the Mac* through the
# kernel NFS client and confirm each change in the guest filesystem (checked over SSH).
#
#   scripts/e2e-m3.sh prepare | run | clean
#
# Reuses the e2e-work disk + ssh key from the journal harness; deploys mistd with the write
# capabilities (CAP_SETUID/SETGID/CHOWN/FOWNER/DAC_OVERRIDE/MKNOD).

set -euo pipefail
cd "$(dirname "$0")/.."
WORK=e2e-work
IMG_URL="https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-arm64.qcow2"
BRIDGE_SOCK=/tmp/mist-m3.sock
MAC="5a:94:ef:e4:0c:03"
VMSHIM=swift/MistBridge/.build/release/mist-vmshim
MUSL=target/aarch64-unknown-linux-musl/release
STATE="$PWD/$WORK/m3-state"
KEY="$WORK/m2_id_ed25519"

prepare() {
  mkdir -p "$WORK"
  if [ ! -f "$WORK/disk.raw" ]; then
    [ -f "$WORK/debian.qcow2" ] || curl -fL --progress-bar -o "$WORK/debian.qcow2" "$IMG_URL"
    qemu-img convert -O raw "$WORK/debian.qcow2" "$WORK/disk.raw"
    qemu-img resize -f raw "$WORK/disk.raw" 10G
  fi
  RUSTFLAGS="-C linker=rust-lld -C link-self-contained=yes" \
    cargo build --release --target aarch64-unknown-linux-musl -p mistd
  ./scripts/build-vmshim.sh
  cargo build --release -p mist-hostd -p mist-cli
  [ -f "$WORK/token" ] || head -c 32 /dev/urandom > "$WORK/token"
  [ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -f "$KEY" -q
  local pubkey; pubkey=$(cat "$KEY.pub")

  rm -f "$WORK/tools-m3.img" "$WORK/tools-m3.dmg"
  hdiutil create -size 64m -fs "MS-DOS FAT32" -layout NONE -volname MISTWRITE "$WORK/tools-m3.dmg" >/dev/null
  local MNT; MNT=$(hdiutil attach "$WORK/tools-m3.dmg" | awk '/Volumes/ {print $NF}')
  cp "$MUSL/mistd" "$WORK/token" "$MNT/"
  cat > "$MNT/make-tree.sh" <<'EOS'
#!/bin/sh
root="$1"; mkdir -p "$root"; echo "# mist m3" > "$root/README.md"
# /srv/code owned by a non-root dev user so Mac-created files inherit that identity.
useradd -m -u 1000 dev 2>/dev/null || true
chown -R 1000:1000 "$root"
# Second share with commit=writeback for the durability/perf A-B (mistd --share fast=...,writeback).
mkdir -p /srv/fast && chown 1000:1000 /srv/fast
EOS
  hdiutil detach "$MNT" >/dev/null
  mv "$WORK/tools-m3.dmg" "$WORK/tools-m3.img"

  rm -rf "$WORK/seed-m3" "$WORK/seed-m3.iso"; mkdir -p "$WORK/seed-m3"
  cat > "$WORK/seed-m3/meta-data" <<EOS
instance-id: mist-m3-$(date +%s)
local-hostname: mist-m3
EOS
  cat > "$WORK/seed-m3/user-data" <<EOS
#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - $pubkey
disable_root: false
write_files:
  - path: /etc/systemd/system/mistd.service
    content: |
      [Unit]
      Description=Mist guest daemon
      After=network.target
      StartLimitIntervalSec=0
      [Service]
      ExecStart=/usr/local/sbin/mistd --share code=/srv/code --share fast=/srv/fast,writeback --listen vsock:6478 --listen tcp:0.0.0.0:6478 --token-file /etc/mist/token
      Restart=always
      RestartSec=1
      AmbientCapabilities=CAP_SYS_ADMIN CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE CAP_CHOWN CAP_FOWNER CAP_SETUID CAP_SETGID CAP_MKNOD
      [Install]
      WantedBy=multi-user.target
  - path: /etc/systemd/system/mist-debug.service
    content: |
      [Unit]
      Description=Dump mistd status into the share so the host can read it over vsock
      After=mistd.service
      StartLimitIntervalSec=0
      [Service]
      ExecStart=/bin/sh -c 'while true; do { date; echo "mistd: \$(systemctl is-active mistd)"; journalctl -u mistd -n 40 --no-pager 2>&1 | tail -40; echo ---mem---; free -m | head -2; } > /srv/code/.mist-debug.tmp 2>&1; mv /srv/code/.mist-debug.tmp /srv/code/mist-debug.log; sleep 5; done'
      Restart=always
      [Install]
      WantedBy=multi-user.target
  - path: /usr/local/sbin/mist-runner.sh
    permissions: '0755'
    content: |
      #!/bin/sh
      # Execute commands dropped into the share as .runner/<n>.cmd; write <n>.out + <n>.done.
      # Bench orchestration without SSH (the share itself is the control channel).
      mkdir -p /srv/code/.runner; chmod 777 /srv/code/.runner
      while true; do
        for c in /srv/code/.runner/*.cmd; do
          [ -f "\$c" ] || continue
          b="\${c%.cmd}"
          [ -f "\$b.done" ] && continue
          sh "\$c" > "\$b.out" 2>&1
          sync "\$b.out"
          : > "\$b.done"
          sync "\$b.done"
        done
        sleep 0.5
      done
  - path: /etc/systemd/system/mist-runner.service
    content: |
      [Unit]
      Description=Execute commands dropped into the share (bench orchestration without SSH)
      After=mistd.service
      StartLimitIntervalSec=0
      [Service]
      ExecStart=/bin/sh /usr/local/sbin/mist-runner.sh
      Restart=always
      [Install]
      WantedBy=multi-user.target
  - path: /etc/systemd/system/mist-churn.service
    content: |
      [Unit]
      Description=Guest-side churn for the conflict-detection e2e
      After=mistd.service
      [Service]
      # Run as the dev user (the share's apply identity) so the Mac — squashed to the same
      # identity — can write churn.txt back: the conflict demo needs both sides writing one file.
      User=dev
      # %% — systemd expands % specifiers in ExecStart (%s = user shell!); escape it or the
      # service runs `date +/bin/bash` and churn.txt never changes content.
      ExecStart=/bin/sh -c 'while true; do date +%%s > /srv/code/churn.txt; sleep 1; done'
      Restart=always
      [Install]
      WantedBy=multi-user.target
runcmd:
  - exec >/dev/hvc0 2>&1; set -x
  - mkdir -p /srv/tools && (mount -t vfat /dev/vdb /srv/tools || mount -t vfat /dev/vdc /srv/tools)
  - install -m755 /srv/tools/mistd /usr/local/sbin/mistd
  - mkdir -p /etc/mist && install -m600 /srv/tools/token /etc/mist/token
  - sh /srv/tools/make-tree.sh /srv/code
  - systemctl daemon-reload && systemctl enable ssh mistd mist-churn mist-debug mist-runner && systemctl restart ssh mistd mist-churn mist-debug mist-runner
  - echo MIST-WRITE-READY
EOS
  hdiutil makehybrid -iso -joliet -default-volume-name cidata -o "$WORK/seed-m3.iso" "$WORK/seed-m3" >/dev/null
  echo "prepare: done"
}

# Force-unmount any leftover loopback NFS mounts. A killed hostd leaves a stale mount whose
# server is gone; with hard,intr a later `rm -rf "$STATE"` hangs forever on it. Always clear
# these before touching $STATE.
clear_stale_mounts() {
  for mp in $(mount | grep -iE "127.0.0.1:/|/mnt/dev/" | awk '{print $3}'); do
    umount -f "$mp" 2>/dev/null || true
  done
}
vm_stop() {
  clear_stale_mounts
  [ -f "$WORK/m3-vmshim.pid" ] && kill "$(cat "$WORK/m3-vmshim.pid")" 2>/dev/null || true
  rm -f "$WORK/m3-vmshim.pid"; pkill -f mist-vmshim 2>/dev/null || true; sleep 1
}
guest_ip() {
  awk '/^\{/{ip="";nm=""} /ip_address=/{split($0,a,"=");ip=a[2]} /name=/{split($0,a,"=");nm=a[2]} /^\}/{if(nm=="mist-m3"&&ip!=""){print ip}}' /var/db/dhcpd_leases 2>/dev/null | tail -1
}
mist() { MIST_STATE_DIR="$STATE" target/release/mist "$@"; }

# Boot the VM + hostd and wait for the vsock seed. Sets HOSTD_PID.
# Readiness + guest verification are entirely over vsock (mist status/cat/ls) — no SSH, no NAT,
# no cloud-init console marker (which is skipped on a reprovisioned disk). mistd auto-starts
# (enabled) and seeds; the seed reaching the host IS the readiness signal. `mist cat` reads the
# guest's real ext4 (open_by_handle + pread in mistd), confirming Mac writes land in the guest.
boot_stack() {
  [ -x "$VMSHIM" ] || { echo "run prepare first"; exit 1; }
  vm_stop; rm -f "$BRIDGE_SOCK"
  "$VMSHIM" --disk "$WORK/disk.raw" --disk "$WORK/tools-m3.img" --disk "$WORK/seed-m3.iso" \
    --cpus 6 --memory 4 --bridge-sock "$BRIDGE_SOCK" --mac "$MAC" > "$WORK/m3-console.log" 2>&1 &
  echo $! > "$WORK/m3-vmshim.pid"; trap vm_stop EXIT
  echo "booting write-path guest…"
  clear_stale_mounts
  rm -rf "$STATE"; mkdir -p "$STATE"
  MIST_STATE_DIR="$STATE" target/release/mist-hostd \
    --vm "dev=bridge:$BRIDGE_SOCK,token=$PWD/$WORK/token" > "$WORK/m3-hostd.log" 2>&1 &
  HOSTD_PID=$!
  echo "waiting for mistd to come up + seed over vsock…"
  local seeded=0
  for i in $(seq 1 150); do
    sleep 2
    if mist status 2>/dev/null | grep -q "\[live\]"; then seeded=1; echo "seeded (~$((i*2))s)"; break; fi
    [ $((i % 20)) -eq 0 ] && echo "  waiting for seed (${i}x2s)…"
  done
  if [ "$seeded" != 1 ]; then
    echo "no guest (mistd never seeded over vsock)"; tail -25 "$WORK/m3-console.log" | tr -d '\r'
    kill $HOSTD_PID 2>/dev/null || true; exit 1
  fi
}

run() {
  boot_stack
  local hostd=$HOSTD_PID
  local out="bench/results/$(date +%Y-%m-%d)-write-e2e.md"; mkdir -p bench/results
  exec > >(tee "$out") 2>&1
  echo "# Mist write-path e2e — $(date '+%Y-%m-%d %H:%M')"; echo '```'
  sw_vers | head -2; echo "guest: Debian 13 (verification over vsock; no SSH)"; echo '```'
  echo; echo "## Seed"; echo '```'; mist status; echo '```'

  echo; echo "## Mount read-write"; echo '```'
  if ! mist mount dev code 2>"$WORK/m3-mount.err"; then
    echo "mount failed (needs root on macOS):"; cat "$WORK/m3-mount.err"
    echo "→ write path still validated by the in-crate NFS mutation round-trip test."
    kill $hostd 2>/dev/null || true; echo '```'; return
  fi
  MP="$STATE/mnt/dev/code"
  echo "mounted rw at $MP"; mount | grep "$MP" | sed 's/.*(\(.*\))/opts: \1/' || true
  echo "owner the macOS client sees (squash should show your uid $(id -u)):"
  ls -lnd "$MP" 2>&1 || true
  echo '```'

  set +e  # diagnostics: keep going past a failed write so we capture everything

  echo; echo "## Write a file FROM THE MAC → read it back from the guest's ext4 (via vsock)"
  echo '```'
  echo "\$ echo 'hello from the mac' > $MP/from-mac.txt"
  echo "hello from the mac" > "$MP/from-mac.txt" && echo "[write ok]" || echo "[WRITE FAILED]"
  sleep 0.5
  echo "\$ mist cat dev code /from-mac.txt   (reads guest ext4 over vsock)"
  mist cat dev code /from-mac.txt
  echo "\$ mist stat dev code /from-mac.txt"
  mist stat dev code /from-mac.txt 2>&1 | grep -E 'kind|size|uid' || true

  echo; echo "\$ mkdir $MP/macdir ; echo nested > $MP/macdir/inner.txt"
  mkdir "$MP/macdir" && echo "nested" > "$MP/macdir/inner.txt"
  sleep 0.5
  echo "\$ mist ls dev code /macdir ; mist cat .../inner.txt"
  mist ls dev code /macdir; mist cat dev code /macdir/inner.txt

  echo; echo "\$ (edit) echo 'line 2' >> $MP/from-mac.txt"
  echo "line 2" >> "$MP/from-mac.txt"
  sleep 0.5
  echo "\$ mist cat dev code /from-mac.txt"
  mist cat dev code /from-mac.txt

  echo; echo "\$ mv $MP/from-mac.txt $MP/renamed.txt"
  mv "$MP/from-mac.txt" "$MP/renamed.txt"
  sleep 0.5
  echo "\$ mist ls dev code /   (from-mac.txt gone, renamed.txt present)"
  mist ls dev code / | grep -E 'from-mac|renamed' || echo "(neither — check)"

  echo; echo "\$ rm $MP/renamed.txt ; rm -rf $MP/macdir"
  rm "$MP/renamed.txt"; rm -rf "$MP/macdir"
  sleep 0.5
  echo "\$ mist ls dev code /   (renamed.txt + macdir gone)"
  mist ls dev code / | grep -E 'renamed|macdir' && echo "STILL PRESENT" || echo "(both gone ✓)"
  echo '```'

  echo; echo "## Durability: cp a 200 KB blob via the mount, read back over vsock, compare sha"
  echo '```'
  head -c 200000 /dev/urandom > /tmp/m3blob
  cp /tmp/m3blob "$MP/blob.bin"; sync; sleep 0.5
  hsha=$(shasum -a256 /tmp/m3blob | awk '{print $1}')
  gsha=$(mist cat dev code /blob.bin 2>/dev/null | shasum -a256 | awk '{print $1}')
  echo "host  sha: $hsha"
  echo "guest sha: $gsha"
  [ "$hsha" = "$gsha" ] && echo "MATCH ✓ (Mac write landed in guest ext4 byte-for-byte)" || echo "MISMATCH ✗"
  rm -f "$MP/blob.bin" /tmp/m3blob
  echo '```'

  mist umount dev code || true
  kill $hostd 2>/dev/null || true
  echo; echo "results: $out"
}

# Write-path hardening suite: side-store proof, conflict detection, fsx torture, pjdfstest subset.
# Requires a freshly-prepared disk (the conflict demo needs the mist-churn guest unit).
harden() {
  boot_stack
  local hostd=$HOSTD_PID
  local out="bench/results/$(date +%Y-%m-%d)-write-harden.md"; mkdir -p bench/results
  exec > >(tee "$out") 2>&1
  echo "# Mist write-path hardening e2e — $(date '+%Y-%m-%d %H:%M')"
  echo '```'; sw_vers | head -2; echo "guest: Debian 13 (verification over vsock)"; echo '```'

  echo; echo "## Mount"; echo '```'
  if ! mist mount dev code 2>"$WORK/m3-mount.err"; then
    echo "mount failed:"; cat "$WORK/m3-mount.err"; kill $hostd 2>/dev/null; echo '```'; exit 1
  fi
  MP="$STATE/mnt/dev/code"
  echo "mounted rw at $MP"; echo '```'

  set +e

  echo; echo "## Side-store: AppleDouble/.DS_Store stay host-side"
  echo '```'
  echo "\$ echo data > side.txt ; xattr -w user.mist demo-value side.txt"
  echo "data" > "$MP/side.txt"
  xattr -w user.mist demo-value "$MP/side.txt" && echo "[xattr write ok]" || echo "[xattr write FAILED]"
  sleep 0.5
  echo "\$ xattr -p user.mist side.txt   (round-trip through the side-store)"
  xattr -p user.mist "$MP/side.txt"
  echo "\$ touch .DS_Store via Finder-style write"
  printf 'fake-dsstore' > "$MP/.DS_Store" && echo "[.DS_Store write ok]"
  sleep 0.5
  echo "\$ mist ls dev code /   (the GUEST tree — must show NO ._side.txt, NO .DS_Store)"
  mist ls dev code /
  if mist ls dev code / | grep -qE '\._side|\.DS_Store'; then
    echo "SIDE-STORE FAILED: Apple metadata leaked into the guest ✗"
  else
    echo "side-store OK: guest tree clean ✓"
  fi
  echo "\$ ls \$MP   (the Mac listing — side rows hidden too)"
  ls "$MP"
  echo '```'

  echo; echo "## Conflicts: guest churn (1 Hz writer) vs Mac write on the same file"
  echo '```'
  echo "waiting for guest churn.txt…"
  for i in $(seq 1 20); do mist stat dev code /churn.txt >/dev/null 2>&1 && break; sleep 1; done
  if mist stat dev code /churn.txt >/dev/null 2>&1; then
    echo "\$ echo mac-clobber > \$MP/churn.txt   (guest wrote ≤1s ago → conflict window)"
    echo "mac-clobber" > "$MP/churn.txt"
    sleep 3   # let the next guest write land too (guest-over-mac direction)
    echo "\$ mist conflicts"
    mist conflicts
    total=$(mist conflicts --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])' 2>/dev/null)
    [ "${total:-0}" -ge 1 ] && echo "conflict detection OK (total=$total) ✓" || echo "NO CONFLICTS DETECTED ✗"
  else
    echo "churn.txt never appeared — disk lacks the mist-churn unit (re-run prepare on a pristine disk) ✗"
  fi
  echo '```'

  echo; echo "## fsx data torture over the mount (write/truncate/read vs in-RAM model)"
  echo '```'
  for s in 1 2 3; do
    target/release/mist-bench fsx --file "$MP/fsx-$s.dat" --ops 1500 --seed "$s" --max-size $((2*1024*1024)) \
      || echo "fsx seed $s FAILED ✗"
  done
  echo '```'

  echo; echo "## pjdfstest subset (POSIX semantics over the mount)"
  echo '```'
  PJD=/tmp/pjdfstest
  if [ -x "$PJD/pjdfstest" ] && command -v prove >/dev/null; then
    workdir=$PWD
    mkdir -p "$MP/pjd" && cd "$MP/pjd"
    for area in chmod ftruncate mkdir open rename rmdir symlink truncate unlink utimensat link mkfifo; do
      out=$(prove -r "$PJD/tests/$area" 2>&1)
      echo "$out" > "$workdir/$WORK/pjd-$area.log"
      total=$(echo "$out" | sed -n 's/.*Tests=\([0-9]*\).*/\1/p' | tail -1)
      failed=$(echo "$out" | grep -oE 'Failed: [0-9]+' | awk '{s+=$2} END{print s+0}')
      verdict=$(echo "$out" | grep -oE 'Result: [A-Z]+' | tail -1)
      printf '%-12s tests=%-5s failed-subtests=%-4s %s\n' "$area" "${total:-?}" "$failed" "$verdict"
    done
    cd "$workdir"; rm -rf "$MP/pjd"
    echo "(per-area detail in $WORK/pjd-<area>.log)"
  else
    echo "pjdfstest not built at $PJD (clone+make) or prove missing — skipped"
  fi
  echo '```'

  rm -f "$MP/side.txt" "$MP/fsx-"*.dat 2>/dev/null
  mist umount dev code || true
  kill $hostd 2>/dev/null || true
  echo; echo "results: $out"
}

case "${1:-}" in
  prepare) prepare ;;
  run) run ;;
  harden) harden ;;
  clean) vm_stop; rm -f "$WORK"/m3-* "$WORK"/seed-m3.iso; rm -rf "$WORK"/seed-m3 "$STATE"; echo cleaned ;;
  *) echo "usage: $0 prepare|run|harden|clean"; exit 2 ;;
esac
