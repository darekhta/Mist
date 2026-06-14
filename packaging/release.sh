#!/usr/bin/env bash
# Cut a release locally: macOS tarball + guest mistd .debs (arm64 + amd64) + checksums.
# The canonical per-distro matrix (bullseye/trixie/ubuntu × arch, install-tested) is the
# `release` GitHub Actions workflow; this script is the local/offline equivalent and emits
# generic (codename-less) .debs unless CODENAMES is set.
#   MIST_SIGN_ID   Developer ID Application identity → codesign the Mac binaries
#   CODENAMES      space-separated distro codenames to stamp (default: one generic build)
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=${1:-$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2)}
mkdir -p dist

echo "== mac binaries (arm64) =="
cargo build --release -p mist-cli -p mist-hostd
if [ -n "${MIST_SIGN_ID:-}" ]; then
  codesign --force --options runtime --sign "$MIST_SIGN_ID" \
    target/release/mist target/release/mist-hostd
  echo "signed with $MIST_SIGN_ID (notarize the tarball separately: xcrun notarytool)"
else
  echo "MIST_SIGN_ID unset — binaries are unsigned (fine for local installs)"
fi
tar -czf "dist/mist-${VERSION}-macos-arm64.tar.gz" -C target/release mist mist-hostd

echo "== guest .debs (arm64 + amd64) =="
for arch in arm64 amd64; do
  if [ -n "${CODENAMES:-}" ]; then
    for cn in $CODENAMES; do
      VERSION="$VERSION" DEB_ARCH="$arch" CODENAME="$cn" packaging/package-deb.sh
    done
  else
    VERSION="$VERSION" DEB_ARCH="$arch" packaging/package-deb.sh
  fi
done

echo "== checksums =="
# SHA256SUMS is the file install.sh verifies (design 11 §5). Keep the per-version .sha256 too.
( cd dist && shasum -a 256 ./*.deb ./*.tar.gz 2>/dev/null | sed 's| dist/| |; s|  \./|  |' > SHA256SUMS )
( cd dist && shasum -a 256 ./* 2>/dev/null | tee "mist-${VERSION}.sha256" >/dev/null )

echo "== minisign SHA256SUMS =="
# install.sh verifies SHA256SUMS.minisig against the pubkey embedded in the installer. Sign with
# the release key (MINISIGN_SECRET_KEY / MINISIGN_PASSWORD), else emit a loud unsigned warning.
if [ -n "${MINISIGN_SECRET_KEY:-}" ] && command -v minisign >/dev/null 2>&1; then
  printf '%s\n' "${MINISIGN_PASSWORD:-}" | \
    minisign -S -s "$MINISIGN_SECRET_KEY" -m dist/SHA256SUMS -x dist/SHA256SUMS.minisig
  echo "signed dist/SHA256SUMS.minisig — embed the matching pubkey in packaging/install.sh"
else
  echo "WARNING: MINISIGN_SECRET_KEY unset (or minisign missing) — SHA256SUMS is UNSIGNED."
  echo "         install.sh will refuse unless minisign is bypassed; sign before publishing."
fi
echo "done: dist/"
