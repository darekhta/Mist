#!/bin/bash
# Build and ad-hoc sign mist-vmshim with the virtualization entitlement.
set -euo pipefail
cd "$(dirname "$0")/../swift/MistBridge"
swift build -c release
BIN=.build/release/mist-vmshim
codesign --force --sign - --entitlements vmshim.entitlements "$BIN"
echo "signed: $PWD/$BIN"
