#!/usr/bin/env bash
# Build a mistd .deb. Parameterized so one script serves local use and the CI matrix
# (per architecture × per target distro). The binary is a static musl build, so it is
# distro-independent — the per-codename packages differ only in version suffix + metadata.
#
# Env (all optional):
#   VERSION    package version            (default: workspace Cargo.toml version)
#   DEB_ARCH   arm64 | amd64              (default: arm64 — the Apple-Virtualization guest arch)
#   CODENAME   target distro codename     (e.g. trixie; appends ~CODENAME to the version + filename)
#   MISTD_BIN  path to a prebuilt static  (default: cross-build for DEB_ARCH with rust-lld)
#              mistd binary
# Output: dist/mistd_<version>[~codename]_<deb_arch>.deb
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${VERSION:-$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)}
DEB_ARCH=${DEB_ARCH:-arm64}
CODENAME=${CODENAME:-}

case "$DEB_ARCH" in
  arm64) RUST_TARGET=aarch64-unknown-linux-musl ;;
  amd64) RUST_TARGET=x86_64-unknown-linux-musl ;;
  *) echo "DEB_ARCH must be arm64 or amd64 (got '$DEB_ARCH')" >&2; exit 1 ;;
esac

BIN=${MISTD_BIN:-target/$RUST_TARGET/release/mistd}
if [ ! -f "$BIN" ]; then
  rustup target add "$RUST_TARGET" >/dev/null 2>&1 || true
  RUSTFLAGS="-C linker=rust-lld -C link-self-contained=yes" \
    cargo build --release --target "$RUST_TARGET" -p mistd
fi

PKGVER="$VERSION${CODENAME:+~$CODENAME}"
ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT"' EXIT
mkdir -p "$ROOT/DEBIAN" "$ROOT/usr/sbin" "$ROOT/lib/systemd/system" "$ROOT/etc/mist"
install -m 755 "$BIN" "$ROOT/usr/sbin/mistd"
install -m 644 packaging/mistd.service "$ROOT/lib/systemd/system/mistd.service"
cat > "$ROOT/etc/mist/mistd.toml" <<TOML
# Mist guest daemon configuration. See docs/install.md.
# listen is a list; the token field is token_file (config rejects unknown fields).
listen = ["vsock:6478"]
token_file = "/etc/mist/token"
vmid_file = "/etc/mist/vmid"

# [share.code]
# path = "/srv/code"
# commit = "fsync"      # or "writeback" for build/scratch trees
TOML
cat > "$ROOT/DEBIAN/control" <<CTRL
Package: mistd
Version: $PKGVER
Architecture: $DEB_ARCH
Maintainer: Mist <noreply@mist.invalid>
Section: admin
Priority: optional
Recommends: avahi-daemon
Description: Mist guest daemon — near-native macOS access to this VM's files
 Journals filesystem changes (fanotify) and applies Mac-side mutations for the
 Mist host daemon. Static musl binary, no runtime deps.
 .
 Atomic-rename journaling needs Linux >= 5.17 (FAN_RENAME); >= 6.1 recommended.
 On older kernels (e.g. Debian bullseye's 5.10) mistd runs in a degraded mode.
CTRL
cat > "$ROOT/DEBIAN/conffiles" <<'CONF'
/etc/mist/mistd.toml
CONF
cat > "$ROOT/DEBIAN/postinst" <<'PI'
#!/bin/sh
set -e
# A token is provisioned by pairing (mist pair / --enroll); seed a random one only if pairing
# isn't used, so a hand-started mistd still authenticates.
[ -s /etc/mist/token ] || { umask 077; head -c 32 /dev/urandom > /etc/mist/token; }
systemctl daemon-reload 2>/dev/null || true
echo "mistd installed. Preferred next step is to pair from the Mac (no token copy-paste):"
echo "  mist pair <name> --ssh $(id -un 2>/dev/null || echo user)@$(hostname 2>/dev/null).local"
echo "Or add shares to /etc/mist/mistd.toml and: systemctl enable --now mistd"
PI
chmod 755 "$ROOT/DEBIAN/postinst"

mkdir -p dist
OUT="dist/mistd_${PKGVER}_${DEB_ARCH}.deb"
# -Zxz: newer dpkg-deb defaults to zstd, which older targets (Debian bullseye's dpkg 1.20)
# cannot read ("unknown compression for member control.tar.zst"). xz installs everywhere.
dpkg-deb -Zxz --build --root-owner-group "$ROOT" "$OUT" 2>/dev/null \
  || docker run --rm -v "$ROOT":/r -v "$PWD/dist":/out debian:stable-slim \
       dpkg-deb -Zxz --build --root-owner-group /r "/out/$(basename "$OUT")"
echo "built $OUT"
