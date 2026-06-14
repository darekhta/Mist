#!/bin/bash
# Mist journal end-to-end on a real Debian guest under AVF.
#
# Validates the journal core: a guest-side filesystem mutation (mkdir/touch/rm/mv) reflects in the
# host metadata replica within journal lag — proving fanotify → vsock → replica on real hardware.
# Also exercises the NFSv3 server (cold reads via mist cat) and the `mist events` change feed.
#
#   scripts/e2e-m2.sh prepare   — build artifacts, image, ssh key, seed.iso (idempotent)
#   scripts/e2e-m2.sh run       — boot VM, run the journal-reflection suite, record results
#   scripts/e2e-m2.sh clean
#
# Guest mutations are driven over SSH (key injected via cloud-init; non-interactive).

set -euo pipefail
cd "$(dirname "$0")/.."
WORK=e2e-work
IMG_URL="https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-arm64.qcow2"
BRIDGE_SOCK=/tmp/mist-m2.sock
MAC="5a:94:ef:e4:0c:02"
MAC_LEASE="5a:94:ef:e4:c:2"
VMSHIM=swift/MistBridge/.build/release/mist-vmshim
MUSL=target/aarch64-unknown-linux-musl/release
STATE="$PWD/$WORK/m2-state"
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
  cargo build --release -p mist-hostd -p mist-cli -p mist-bench

  [ -f "$WORK/token" ] || head -c 32 /dev/urandom > "$WORK/token"
  [ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -f "$KEY" -q
  local pubkey; pubkey=$(cat "$KEY.pub")

  # Tools disk (FAT32): mistd + token + tree generator.
  rm -f "$WORK/tools-m2.img" "$WORK/tools-m2.dmg"
  hdiutil create -size 64m -fs "MS-DOS FAT32" -layout NONE -volname MISTJOURNAL "$WORK/tools-m2.dmg" >/dev/null
  local MNT; MNT=$(hdiutil attach "$WORK/tools-m2.dmg" | awk '/Volumes/ {print $NF}')
  cp "$MUSL/mistd" "$WORK/token" "$MNT/"
  cat > "$MNT/make-tree.sh" <<'EOS'
#!/bin/sh
root="$1"; dirs="$2"; files="$3"
mkdir -p "$root"; echo "# mist m2 tree" > "$root/README.md"
i=0; while [ "$i" -lt "$dirs" ]; do d="$root/d$i"; mkdir -p "$d"
  j=0; while [ "$j" -lt "$files" ]; do : > "$d/f$j.txt"; j=$((j+1)); done
  echo "content $i" > "$d/f0.txt"; i=$((i+1)); done
EOS
  hdiutil detach "$MNT" >/dev/null
  mv "$WORK/tools-m2.dmg" "$WORK/tools-m2.img"

  # cloud-init: ssh key, mistd service, seed tree.
  rm -rf "$WORK/seed-m2" "$WORK/seed-m2.iso"; mkdir -p "$WORK/seed-m2"
  cat > "$WORK/seed-m2/meta-data" <<EOS
instance-id: mist-m2-$(date +%s)
local-hostname: mist-m2
EOS
  cat > "$WORK/seed-m2/user-data" <<EOS
#cloud-config
ssh_pwauth: false
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
      [Service]
      ExecStart=/usr/local/sbin/mistd --share code=/srv/code --listen vsock:6478 --token-file /etc/mist/token
      Restart=always
      RestartSec=1
      AmbientCapabilities=CAP_SYS_ADMIN CAP_DAC_READ_SEARCH CAP_DAC_OVERRIDE
      [Install]
      WantedBy=multi-user.target
runcmd:
  - exec >/dev/hvc0 2>&1; set -x
  - mkdir -p /srv/tools && (mount -t vfat /dev/vdb /srv/tools || mount -t vfat /dev/vdc /srv/tools)
  - install -m755 /srv/tools/mistd /usr/local/sbin/mistd
  - mkdir -p /etc/mist && install -m600 /srv/tools/token /etc/mist/token
  - sh /srv/tools/make-tree.sh /srv/code 50 100
  - sysctl -w fs.fanotify.max_queued_events=1048576 || true
  - systemctl daemon-reload && systemctl enable --now ssh mistd
  - sleep 2; systemctl --no-pager status mistd | head -5
  - echo MIST-JOURNAL-READY
EOS
  hdiutil makehybrid -iso -joliet -default-volume-name cidata -o "$WORK/seed-m2.iso" "$WORK/seed-m2" >/dev/null
  echo "prepare: done"
}

vm_stop() {
  [ -f "$WORK/m2-vmshim.pid" ] && kill "$(cat "$WORK/m2-vmshim.pid")" 2>/dev/null || true
  rm -f "$WORK/m2-vmshim.pid"
  pkill -f "bridge-sock $BRIDGE_SOCK" 2>/dev/null || true
  sleep 1
}

guest_ip() {
  awk '/^\{/{ip="";nm=""} /ip_address=/{split($0,a,"=");ip=a[2]} /name=/{split($0,a,"=");nm=a[2]} /^\}/{if(nm=="mist-m2"&&ip!=""){print ip}}' /var/db/dhcpd_leases 2>/dev/null | tail -1
}

GSSH=(ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
      -o ConnectTimeout=5 -o LogLevel=ERROR)
gx() { "${GSSH[@]}" "root@$GIP" "$@"; }   # run a command in the guest

mist() { MIST_STATE_DIR="$STATE" target/release/mist "$@"; }

run() {
  [ -x "$VMSHIM" ] || { echo "run prepare first"; exit 1; }
  vm_stop
  rm -f "$BRIDGE_SOCK"
  "$VMSHIM" --disk "$WORK/disk.raw" --disk "$WORK/tools-m2.img" --disk "$WORK/seed-m2.iso" \
    --cpus 6 --memory 4 --bridge-sock "$BRIDGE_SOCK" --mac "$MAC" > "$WORK/m2-console.log" 2>&1 &
  echo $! > "$WORK/m2-vmshim.pid"
  trap vm_stop EXIT
  echo "booting journal e2e guest…"

  # Wait for the guest IP + SSH.
  GIP=""
  for i in $(seq 1 120); do
    sleep 2
    GIP=$(guest_ip)
    if [ -n "$GIP" ] && gx true 2>/dev/null; then echo "guest up at $GIP (~$((i*2))s)"; break; fi
    [ $((i % 15)) -eq 0 ] && echo "  waiting (${i}x2s)…"
  done
  [ -n "$GIP" ] || { echo "no guest"; tail -30 "$WORK/m2-console.log"; exit 1; }

  # Start hostd.
  rm -rf "$STATE"; mkdir -p "$STATE"
  MIST_STATE_DIR="$STATE" target/release/mist-hostd \
    --vm "dev=bridge:$BRIDGE_SOCK,token=$PWD/$WORK/token" > "$WORK/m2-hostd.log" 2>&1 &
  local hostd=$!
  for i in $(seq 1 30); do sleep 1; mist status >/dev/null 2>&1 && break; done

  local out="bench/results/$(date +%Y-%m-%d)-journal-e2e.md"
  mkdir -p bench/results
  exec > >(tee "$out") 2>&1

  echo "# Mist journal e2e — $(date '+%Y-%m-%d %H:%M')"
  echo; echo '```'
  sw_vers | head -2; sysctl -n machdep.cpu.brand_string
  gx 'uname -r; echo kernel'; echo "guest IP $GIP"
  echo '```'

  echo; echo "## Seed"; echo '```'; mist status; echo '```'

  echo; echo "## Journal reflection (guest mutation → host replica)"
  echo '```'
  echo "\$ (guest) mkdir /srv/code/m2new ; echo hi > /srv/code/hello.txt"
  gx 'mkdir -p /srv/code/m2new && echo hi > /srv/code/hello.txt && touch /srv/code/m2new/inner.txt'
  sleep 1
  echo "\$ mist ls dev code /   (expect m2new, hello.txt present)"
  mist ls dev code / | grep -E 'm2new|hello.txt|README' || echo "MISSING"
  echo "\$ mist cat dev code /hello.txt   (cold read via NFS-less Read RPC path)"
  mist cat dev code /hello.txt
  echo "\$ mist stat dev code /m2new/inner.txt"
  mist stat dev code /m2new/inner.txt | grep -E 'kind|ino' || true

  echo; echo "\$ (guest) rm hello.txt ; rmdir m2new/inner.txt-less ; mv README.md DOCS.md"
  gx 'rm /srv/code/hello.txt; mv /srv/code/README.md /srv/code/DOCS.md'
  sleep 1
  echo "\$ mist ls dev code /   (expect hello.txt gone, DOCS.md present, README.md gone)"
  mist ls dev code / | grep -E 'hello.txt|DOCS.md|README.md' || echo "(none of the three — check)"
  echo '```'

  echo; echo "## Change feed (mist events --follow)"
  echo '```'
  ( mist events --follow > "$WORK/m2-events.log" 2>&1 & echo $! > "$WORK/m2-events.pid" )
  sleep 1
  gx 'echo feedtest > /srv/code/feed.txt && mkdir /srv/code/feeddir'
  sleep 2
  kill "$(cat "$WORK/m2-events.pid")" 2>/dev/null || true
  echo "captured events:"; grep -E 'feed.txt|feeddir' "$WORK/m2-events.log" || cat "$WORK/m2-events.log" | head -5
  echo '```'

  echo; echo "## NFS mount (macOS kernel client)"
  echo '```'
  if mist mount dev code 2>"$WORK/m2-mount.err"; then
    local mp="$STATE/mnt/dev/code"
    echo "mounted; ls:"; ls "$mp" | head -5
    echo "cat DOCS.md via NFS:"; cat "$mp/DOCS.md" 2>/dev/null || cat "$mp/README.md" 2>/dev/null || true
    mist umount dev code || true
  else
    echo "mount needs root on macOS (no vfs.usermount). Error:"
    cat "$WORK/m2-mount.err"
    echo "→ validated instead by the in-crate NFS client round-trip test (mist-nfs)."
  fi
  echo '```'

  echo; echo "## Final status"; echo '```'; mist status; echo '```'
  kill $hostd 2>/dev/null || true
  echo; echo "results: $out"
}

case "${1:-}" in
  prepare) prepare ;;
  run) run ;;
  clean) vm_stop; rm -f "$WORK"/m2-* "$WORK"/seed-m2.iso "$KEY" "$KEY.pub"; rm -rf "$WORK"/seed-m2 "$STATE"; echo cleaned ;;
  *) echo "usage: $0 prepare|run|clean"; exit 2 ;;
esac
