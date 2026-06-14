#!/usr/bin/env bash
# Assemble, sign, and (optionally) notarize Mist.app + a DMG (design 11 §8). Pipeline discipline:
# sign BOTTOM-UP (mist-hostd, mist, mount-helper, app) with Developer ID and `-o runtime`, NO
# `--deep`; archive with `ditto -c -k --keepParent`; `notarytool submit --wait`; fail unless
# Accepted; `stapler staple` the app AND the DMG.
#
# Env:
#   MIST_SIGN_ID    "Developer ID Application: … (TEAMID)"  — required to sign (else dry assemble)
#   NOTARY_PROFILE  notarytool keychain profile name        — set to notarize + staple
#   VERSION         defaults to workspace Cargo.toml version
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${VERSION:-$(grep -m1 '^version' Cargo.toml | cut -d'"' -f2 || true)}
: "${VERSION:?could not parse version from Cargo.toml}"
APP="dist/Mist.app"
DMG="dist/Mist-${VERSION}.dmg"
SWIFT_DIR="swift/MistApp"
RES="swift/MistApp/Resources"

echo "== building rust binaries (universal) =="
for tgt in aarch64-apple-darwin x86_64-apple-darwin; do
  rustup target add "$tgt" >/dev/null 2>&1 || true
  cargo build --release --target "$tgt" -p mist-cli -p mist-hostd
done
mkdir -p dist/bin
for b in mist mist-hostd; do
  lipo -create -output "dist/bin/$b" \
    "target/aarch64-apple-darwin/release/$b" \
    "target/x86_64-apple-darwin/release/$b"
done

echo "== building swift (app + mount-helper) =="
( cd "$SWIFT_DIR" && swift build -c release --arch arm64 --arch x86_64 )
SWIFT_BIN="$SWIFT_DIR/.build/apple/Products/Release"
[ -d "$SWIFT_BIN" ] || SWIFT_BIN="$SWIFT_DIR/.build/release"

echo "== assembling $APP =="
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" \
         "$APP/Contents/Resources" \
         "$APP/Contents/Library/HelperTools" \
         "$APP/Contents/Library/LaunchDaemons" \
         "$APP/Contents/Library/LaunchAgents"
install -m755 "$SWIFT_BIN/Mist" "$APP/Contents/MacOS/Mist"
install -m755 "$SWIFT_BIN/mist-mount-helper" "$APP/Contents/Library/HelperTools/mist-mount-helper"
install -m755 dist/bin/mist-hostd "$APP/Contents/Resources/mist-hostd"
install -m755 dist/bin/mist "$APP/Contents/Resources/mist"
install -m644 "$RES/Info.plist" "$APP/Contents/Info.plist"
install -m644 "$RES/LaunchDaemons/dev.mist.mount-helper.plist" "$APP/Contents/Library/LaunchDaemons/"
install -m644 "$RES/LaunchAgents/dev.mist.hostd.plist" "$APP/Contents/Library/LaunchAgents/"

# Embed Sparkle.framework (the SPM build produces it; an SPM executable doesn't auto-embed it).
SPARKLE_FW=""
for cand in "$SWIFT_BIN/Sparkle.framework" "$SWIFT_DIR/.build/artifacts"/*/Sparkle*/Sparkle.framework; do
  [ -d "$cand" ] && { SPARKLE_FW="$cand"; break; }
done
# The version-dir letter (Versions/A vs B) isn't contractually stable across Sparkle releases —
# always resolve it via the Current symlink so the XPC strip + nested signing target real paths.
sparkle_versdir() { echo "$1/Versions/$(readlink "$1/Versions/Current" 2>/dev/null || echo Current)"; }
if [ -n "$SPARKLE_FW" ]; then
  mkdir -p "$APP/Contents/Frameworks"
  cp -R "$SPARKLE_FW" "$APP/Contents/Frameworks/"
  install_name_tool -add_rpath "@executable_path/../Frameworks" "$APP/Contents/MacOS/Mist" 2>/dev/null || true
  # Mist isn't sandboxed → in-process installer; drop the XPC helpers (fewer binaries to sign).
  rm -rf "$(sparkle_versdir "$APP/Contents/Frameworks/Sparkle.framework")/XPCServices"
  echo "embedded Sparkle.framework (XPCServices stripped)"
else
  echo "WARNING: Sparkle.framework not found — auto-update will be inert until it's embedded"
fi

# Signing is optional: a DMG doesn't need an Apple cert (only notarization does). Without
# MIST_SIGN_ID we still produce an UNSIGNED .app + DMG (good for testing the packaging in CI),
# and the build upgrades to signed → notarized automatically once the secrets are present.
SIGNED=0
if [ -n "${MIST_SIGN_ID:-}" ]; then
  # A signed release that still carries the placeholder Sparkle public key would ship an app that
  # rejects every (correctly signed) update. Refuse to build it.
  if grep -q REPLACE_WITH_SPARKLE "$APP/Contents/Info.plist"; then
    echo "ERROR: Info.plist SUPublicEDKey is still the placeholder — run Sparkle's generate_keys and" >&2
    echo "       fill SUPublicEDKey before a signed build (the public key is not a secret)." >&2
    exit 1
  fi
  echo "== signing bottom-up ($MIST_SIGN_ID, Team 9YA6F7T5Z4) =="
  sign() { codesign --force --options runtime --timestamp --sign "$MIST_SIGN_ID" "$@"; }
  # Stable code-signing identifier — the mount-helper's client requirement pins dev.mist.app /
  # dev.mist.hostd, and a changed identifier on upgrade triggers an AMFI launch-constraint violation.
  sign_id() { local id="$1"; shift; codesign --force --options runtime --timestamp --identifier "$id" --sign "$MIST_SIGN_ID" "$@"; }

  # Sparkle's nested code must be signed first (XPCServices, Autoupdate, Updater.app), then the framework.
  SF="$APP/Contents/Frameworks/Sparkle.framework"
  if [ -d "$SF" ]; then
    V="$(sparkle_versdir "$SF")"
    for x in "$V/XPCServices/Installer.xpc" "$V/XPCServices/Downloader.xpc" \
             "$V/Updater.app/Contents/MacOS/Updater" "$V/Updater.app" "$V/Autoupdate" "$V/Sparkle"; do
      [ -e "$x" ] && sign "$x"
    done
    sign "$SF"
  fi

  # Inner executables next, each with its entitlements + a stable identifier; then the app (never --deep).
  sign_id dev.mist.mount-helper --entitlements "$RES/mist-mount-helper.entitlements" "$APP/Contents/Library/HelperTools/mist-mount-helper"
  sign_id dev.mist.hostd "$APP/Contents/Resources/mist-hostd"
  sign_id dev.mist.cli "$APP/Contents/Resources/mist"
  sign --entitlements "$RES/Mist.entitlements" "$APP/Contents/MacOS/Mist"
  sign --entitlements "$RES/Mist.entitlements" "$APP"
  codesign --verify --strict --verbose=2 "$APP"
  SIGNED=1
else
  echo "== MIST_SIGN_ID unset — building an UNSIGNED .app + DMG (testing only) =="
  echo "   Gatekeeper will block it and SMAppService won't register; set MIST_SIGN_ID to ship."
fi

# notarize_staple <file-to-submit> <path-to-staple>: submit to Apple, wait, staple on success.
# Grep a FILE (not the live pipe): `… | grep -q` would close the pipe on first match and SIGPIPE
# notarytool/tee, which under `set -o pipefail` misreads an Accepted notarization as a failure.
notarize_staple() {
  echo "== notarizing $(basename "$2") =="
  set +e
  xcrun notarytool submit "$1" --keychain-profile "$NOTARY_PROFILE" --wait 2>&1 | tee /tmp/notary.log
  local rc=${PIPESTATUS[0]}
  set -e
  if [ "$rc" -ne 0 ] || ! grep -q "status: Accepted" /tmp/notary.log; then
    echo "notarization NOT accepted — fetching log:"
    local sub; sub=$(grep -m1 'id:' /tmp/notary.log | awk '{print $2}' || true)
    [ -n "$sub" ] && xcrun notarytool log "$sub" --keychain-profile "$NOTARY_PROFILE"
    exit 1
  fi
  xcrun stapler staple "$2"
  echo "stapled $2"
}

# Notarize + staple the APP first so the DMG is built from an already-stapled app (offline-capable
# first launch); then notarize + staple the DMG, which needs its OWN ticket — stapling a DMG fails
# unless the DMG itself (not just a zip of the app) was submitted to the notary service.
if [ "$SIGNED" = 1 ] && [ -n "${NOTARY_PROFILE:-}" ]; then
  zip="dist/Mist-${VERSION}.zip"
  ditto -c -k --keepParent "$APP" "$zip"
  notarize_staple "$zip" "$APP"
fi

echo "== building DMG =="
rm -f "$DMG"
if command -v create-dmg >/dev/null 2>&1; then
  create-dmg --volname "Mist" --app-drop-link 480 170 "$DMG" "$APP" || true  # exits non-zero even on success
fi
if [ ! -f "$DMG" ]; then
  # create-dmg absent or flaked — fall back to a plain DMG with an /Applications symlink.
  staging="$(mktemp -d)"; cp -R "$APP" "$staging/"; ln -s /Applications "$staging/Applications"
  hdiutil create -volname Mist -srcfolder "$staging" -ov -format UDZO "$DMG"
  rm -rf "$staging"
fi
[ "$SIGNED" = 1 ] && sign "$DMG"

if [ "$SIGNED" = 1 ] && [ -n "${NOTARY_PROFILE:-}" ]; then
  notarize_staple "$DMG" "$DMG"
fi

# Generate the EdDSA-signed Sparkle appcast — ONLY when the private key is provided. The key is a
# SECRET (CI: $SPARKLE_PRIVATE_KEY, base64). Without it we do NOT touch packaging/appcast.xml: a
# stale or placeholder appcast must never be published, since every client fetches it and a bad
# signature bricks auto-update. A real generate_appcast failure is fatal here (no swallowing).
GEN_APPCAST="$(find "$SWIFT_DIR/.build/artifacts" -name generate_appcast -type f 2>/dev/null | head -1 || true)"
if [ -n "${SPARKLE_PRIVATE_KEY:-}" ] && [ -n "$GEN_APPCAST" ] && [ -f "$DMG" ]; then
  echo "== generating appcast =="
  outdir="dist/appcast"; rm -rf "$outdir"; mkdir -p "$outdir"; cp "$DMG" "$outdir/"
  prefix="https://github.com/darekhta/Mist/releases/download/v${VERSION}/"
  keyfile="$(mktemp)"; trap 'rm -f "$keyfile"' EXIT   # secret never left on disk, even on abort
  printf '%s' "$SPARKLE_PRIVATE_KEY" > "$keyfile"
  "$GEN_APPCAST" --ed-key-file "$keyfile" --download-url-prefix "$prefix" "$outdir"
  rm -f "$keyfile"; trap - EXIT
  cp "$outdir/appcast.xml" packaging/appcast.xml
  echo "wrote packaging/appcast.xml"
  # Tell CI a fresh appcast exists, so it only attaches one it actually regenerated.
  [ -n "${GITHUB_ENV:-}" ] && echo "APPCAST_GENERATED=1" >> "$GITHUB_ENV"
else
  echo "skip appcast — no SPARKLE_PRIVATE_KEY (or generate_appcast/DMG missing); appcast.xml untouched"
fi
echo "done: $APP  $DMG"
