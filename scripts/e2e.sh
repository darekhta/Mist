#!/bin/bash
# Mist transport/seed end-to-end: boot a real Debian guest under AVF, run mistd inside, measure the acceptance checks.
#
#   scripts/e2e.sh prepare   — download image, build artifacts (idempotent)
#   scripts/e2e.sh run       — boot VM, run transport + seed benches, mist ls/cat demo
#   scripts/e2e.sh clean     — stop VM, remove work dir
#
# Everything lives under e2e-work/ (gitignored).

set -euo pipefail
cd "$(dirname "$0")/.."
WORK=e2e-work
IMG_URL="https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-arm64.qcow2"
BRIDGE_SOCK=/tmp/mist-e2e.sock
MAC="5a:94:ef:e4:0c:01"
VMSHIM=swift/MistBridge/.build/release/mist-vmshim
MUSL=target/aarch64-unknown-linux-musl/release

prepare() {
  mkdir -p "$WORK"

  if [ ! -f "$WORK/disk.raw" ]; then
    [ -f "$WORK/debian.qcow2" ] || curl -fL --progress-bar -o "$WORK/debian.qcow2" "$IMG_URL"
    qemu-img convert -O raw "$WORK/debian.qcow2" "$WORK/disk.raw"
    qemu-img resize -f raw "$WORK/disk.raw" 10G
  fi

  RUSTFLAGS="-C linker=rust-lld -C link-self-contained=yes" \
    cargo build --release --target aarch64-unknown-linux-musl -p mistd -p mist-bench
  ./scripts/build-vmshim.sh
  cargo build --release -p mist-hostd -p mist-cli -p mist-bench

  [ -f "$WORK/token" ] || head -c 32 /dev/urandom > "$WORK/token"

  # Tools disk: raw FAT32 image with binaries + token + tree generator.
  rm -f "$WORK/tools.dmg" "$WORK/tools.img"
  hdiutil create -size 64m -fs "MS-DOS FAT32" -layout NONE -volname MISTTOOLS \
    "$WORK/tools.dmg" >/dev/null
  MNT=$(hdiutil attach "$WORK/tools.dmg" | awk '/Volumes/ {print $NF}')
  cp "$MUSL/mistd" "$MUSL/mist-bench" "$WORK/token" "$MNT/"
  cat > "$MNT/make-tree.sh" <<'EOS'
#!/bin/sh
# make-tree.sh ROOT DIRS FILES_PER_DIR
root="$1"; dirs="$2"; files="$3"
mkdir -p "$root"
echo "# Mist e2e tree" > "$root/README.md"
echo "hello from the guest at $(date)" >> "$root/README.md"
i=0
while [ "$i" -lt "$dirs" ]; do
  d="$root/d$i"; mkdir -p "$d"
  j=0
  while [ "$j" -lt "$files" ]; do : > "$d/f$j.txt"; j=$((j+1)); done
  echo "content $i" > "$d/f0.txt"
  i=$((i+1))
done
EOS
  hdiutil detach "$MNT" >/dev/null
  mv "$WORK/tools.dmg" "$WORK/tools.img"

  # cloud-init NoCloud seed ISO.
  rm -rf "$WORK/seed" "$WORK/seed.iso"
  mkdir -p "$WORK/seed"
  cat > "$WORK/seed/meta-data" <<'EOS'
instance-id: mist-e2e-005
local-hostname: mist-e2e
EOS
  cat > "$WORK/seed/user-data" <<'EOS'
#cloud-config
chpasswd:
  expire: false
  list: |
    root:mist
write_files:
  - path: /etc/systemd/system/mistd.service
    content: |
      [Unit]
      Description=Mist guest daemon
      After=network.target
      [Service]
      ExecStart=/usr/local/sbin/mistd --share code=/srv/code --listen vsock:6478 --token-file /etc/mist/token
      Restart=always
      RestartSec=1
      [Install]
      WantedBy=multi-user.target
  - path: /etc/systemd/system/mist-bench-serve.service
    content: |
      [Unit]
      Description=Mist transport echo server
      [Service]
      ExecStart=/usr/local/sbin/mist-bench serve --listen vsock:6479
      Restart=always
      RestartSec=1
      [Install]
      WantedBy=multi-user.target
runcmd:
  - exec >/dev/hvc0 2>&1; set -x
  - mkdir -p /srv/tools
  - mount -t vfat /dev/vdb /srv/tools || mount -t vfat /dev/vdc /srv/tools
  - install -m755 /srv/tools/mistd /usr/local/sbin/mistd
  - install -m755 /srv/tools/mist-bench /usr/local/sbin/mist-bench
  - mkdir -p /etc/mist && install -m600 /srv/tools/token /etc/mist/token
  - sh /srv/tools/make-tree.sh /srv/code 200 250
  - systemctl daemon-reload
  - systemctl enable --now mist-bench-serve mistd
  - sleep 3
  - echo "=== DIAG vsock ==="; ls -la /dev/vsock /dev/vhost-vsock 2>&1; lsmod | grep -i vsock
  - echo "=== DIAG cid ==="; cat /sys/devices/virtual/misc/vsock/* 2>/dev/null; dmesg | grep -i vsock | tail -5
  - echo "=== DIAG mistd ==="; systemctl is-active mistd; journalctl -u mistd --no-pager | tail -8
  - echo "=== DIAG serve ==="; journalctl -u mist-bench-serve --no-pager | tail -4
  - echo "=== DIAG listeners ==="; ss -l -A vsock 2>&1 || cat /proc/net/vsock 2>&1 || echo "no ss vsock"
  - echo MIST-E2E-READY
EOS
  hdiutil makehybrid -iso -joliet -default-volume-name cidata \
    -o "$WORK/seed.iso" "$WORK/seed" >/dev/null
  echo "prepare: done"
}

vm_start() {
  rm -f "$BRIDGE_SOCK"
  "$VMSHIM" --disk "$WORK/disk.raw" --disk "$WORK/tools.img" --disk "$WORK/seed.iso" \
    --cpus 6 --memory 4 --bridge-sock "$BRIDGE_SOCK" --mac "$MAC" \
    > "$WORK/console.log" 2>&1 &
  echo $! > "$WORK/vmshim.pid"
  echo "vm: started (pid $(cat "$WORK/vmshim.pid")), console → $WORK/console.log"
}

vm_stop() {
  if [ -f "$WORK/vmshim.pid" ]; then
    kill "$(cat "$WORK/vmshim.pid")" 2>/dev/null || true
    rm -f "$WORK/vmshim.pid"
  fi
  pkill -f mist-vmshim 2>/dev/null || true
  sleep 1
}

wait_ready() {
  echo "vm: waiting for mistd on vsock (bridge $BRIDGE_SOCK) ..."
  for i in $(seq 1 300); do
    if target/release/mist-bench transport \
        --connect "bridge:$BRIDGE_SOCK#6479" --pings 10 --bytes 1048576 --lanes 1 \
        > /dev/null 2>&1; then
      echo "vm: guest services up (after ~${i}s)"
      return 0
    fi
    sleep 1
    [ $((i % 30)) -eq 0 ] && echo "vm: still waiting (${i}s)"
  done
  echo "vm: guest never came up; tail of console:"
  tail -40 "$WORK/console.log"
  return 1
}

guest_ip() {
  # NAT DHCP lease by fixed MAC (leading zeros stripped in the lease file).
  local mac_lease="5a:94:ef:e4:c:1"
  awk -v mac="$mac_lease" '
    /^\{/ { ip=""; hw="" }
    /ip_address=/ { split($0,a,"="); ip=a[2] }
    /hw_address=/ { split($0,a,","); hw=a[2] }
    /^\}/ { if (hw==mac && ip!="") { print ip; exit } }
  ' /var/db/dhcpd_leases 2>/dev/null || true
}

run() {
  [ -x "$VMSHIM" ] || { echo "run prepare first"; exit 1; }
  vm_stop
  vm_start
  trap vm_stop EXIT
  wait_ready

  local out="bench/results/$(date +%Y-%m-%d)-transport-seed-e2e.md"
  mkdir -p bench/results
  {
    echo "# Mist transport and seed e2e — $(date '+%Y-%m-%d %H:%M')"
    echo
    echo '```'
    sw_vers | head -2
    sysctl -n machdep.cpu.brand_string
    echo "guest: Debian 13 genericcloud arm64, 6 vcpu, 4 GiB"
    echo '```'
    echo
    echo "## vsock (MistBridge UDS → VZVirtioSocketDevice)"
    echo '```'
    target/release/mist-bench transport --connect "bridge:$BRIDGE_SOCK#6479" \
      --pings 5000 --bytes $((2*1024*1024*1024)) --lanes 4
    echo '```'

    local ip
    ip=$(guest_ip)
    if [ -n "$ip" ]; then
      echo
      echo "## TCP over virtio-net (guest $ip)"
      echo '```'
      target/release/mist-bench transport --connect "tcp:$ip:6479" \
        --pings 5000 --bytes $((2*1024*1024*1024)) --lanes 4 || echo "(tcp bench failed)"
      echo '```'
    else
      echo
      echo "## TCP over virtio-net: skipped (no DHCP lease found for $MAC)"
    fi

    echo
    echo "## Seed (50k files, 201 dirs, real mistd walker → replica)"
    echo '```'
    target/release/mist-bench seed --connect "bridge:$BRIDGE_SOCK#6478" --token "$WORK/token"
    echo '```'
  } | tee "$out"

  echo
  echo "## mist ls / cat demo (hostd end-to-end)" | tee -a "$out"
  local state="$WORK/state"
  mkdir -p "$state"
  MIST_STATE_DIR="$state" target/release/mist-hostd \
    --vm "dev=bridge:$BRIDGE_SOCK,token=$WORK/token" > "$WORK/hostd.log" 2>&1 &
  local hostd_pid=$!
  sleep 2
  {
    echo '```'
    echo "$ mist status"
    MIST_STATE_DIR="$state" target/release/mist status || true
    echo
    echo "$ mist ls dev code /"
    MIST_STATE_DIR="$state" target/release/mist ls dev code / | head -8 || true
    echo
    echo "$ mist ls dev code /d7 | head -5"
    MIST_STATE_DIR="$state" target/release/mist ls dev code /d7 | head -5 || true
    echo
    echo "$ mist cat dev code /README.md"
    MIST_STATE_DIR="$state" target/release/mist cat dev code /README.md || true
    echo
    echo "$ mist cat dev code /d7/f0.txt"
    MIST_STATE_DIR="$state" target/release/mist cat dev code /d7/f0.txt || true
    echo '```'
  } | tee -a "$out"
  kill $hostd_pid 2>/dev/null || true

  echo
  echo "results: $out"
}

case "${1:-}" in
  prepare) prepare ;;
  run) run ;;
  clean) vm_stop; rm -rf "$WORK"; echo cleaned ;;
  *) echo "usage: $0 prepare|run|clean"; exit 2 ;;
esac
