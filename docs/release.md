# Releasing Mist.app (signed, notarized, auto-updating)

This is the one-time setup + per-release steps to ship `Mist.app` as a notarized DMG that
auto-updates via Sparkle. **No credential below is committed** — they live in your login keychain
(local builds) or GitHub repo secrets (CI). The Team ID and the Sparkle *public* key are not
secrets; everything else is.

## What's public vs. secret

| Value | Public? | Where it lives |
|---|---|---|
| Apple Team ID `9YA6F7T5Z4` | public | baked into the helper requirement + signing identity |
| Sparkle **public** EdDSA key | public | `Info.plist` → `SUPublicEDKey` |
| Developer ID Application **cert** (`.p12`) | secret | login keychain / `MACOS_CERT_P12` |
| App Store Connect notary key (`.p8`) | secret | `NOTARY_KEY` + `NOTARY_KEY_ID` + `NOTARY_ISSUER` |
| Sparkle **private** EdDSA key | secret | login keychain / `SPARKLE_PRIVATE_KEY` |

## One-time setup

1. **Developer ID Application certificate.** In Xcode → Settings → Accounts (Team `9YA6F7T5Z4`),
   create a *Developer ID Application* certificate so it lands in your login keychain. Confirm:
   ```
   security find-identity -v -p codesigning | grep "Developer ID Application"
   ```
   The full string (e.g. `Developer ID Application: Your Name (9YA6F7T5Z4)`) is your `MIST_SIGN_ID`.

2. **Notary credentials.** App Store Connect → Users and Access → Integrations → Keys → create an
   API key with *Developer* access. Note the **Key ID** and **Issuer ID**; download the `.p8` once.
   Store a local notary profile:
   ```
   xcrun notarytool store-credentials mist-notary --key AuthKey_XXXX.p8 \
     --key-id <KEY_ID> --issuer <ISSUER_ID>
   ```

3. **Sparkle keypair.** Download Sparkle's tools (`Sparkle-2.x/bin/`) and run once:
   ```
   ./bin/generate_keys          # stores the PRIVATE key in your login keychain, prints the PUBLIC key
   ```
   Paste the printed public key into `swift/MistApp/Resources/Info.plist` → `SUPublicEDKey`
   (replacing `REPLACE_WITH_SPARKLE_ED25519_PUBLIC_KEY`). Commit that — it's public.

## Local signed build

```
MIST_SIGN_ID="Developer ID Application: Your Name (9YA6F7T5Z4)" \
NOTARY_PROFILE=mist-notary \
  bash packaging/build-app.sh
```
Produces `dist/Mist-<version>.dmg` (signed, notarized, stapled) and `packaging/appcast.xml` (signed
with the keychain private key). Without `MIST_SIGN_ID` the script assembles an **unsigned** bundle
and stops — useful for a quick local check, but `SMAppService` won't register an unsigned app.

## CI release (tag `vX.Y.Z`)

Pushing a `v*` tag runs `.github/workflows/mac-app.yml`, which signs, notarizes, generates the
appcast, and attaches `Mist-*.dmg` + `appcast.xml` to the GitHub Release. Set these **repo secrets**
(Settings → Secrets and variables → Actions) first:

| Secret | How to produce it |
|---|---|
| `MACOS_CERT_P12` | export the Developer ID cert+key as `.p12`, then `base64 -i cert.p12 \| pbcopy` |
| `MACOS_CERT_PASSWORD` | the `.p12` export password |
| `MACOS_SIGN_ID` | `Developer ID Application: Your Name (9YA6F7T5Z4)` |
| `NOTARY_KEY` | `base64 -i AuthKey_XXXX.p8 \| pbcopy` |
| `NOTARY_KEY_ID` / `NOTARY_ISSUER` | from App Store Connect |
| `SPARKLE_PRIVATE_KEY` | `./bin/generate_keys -x sparkle_priv.key`, then set the secret to that file's **verbatim** contents — do NOT base64 it (build-app.sh passes it straight to `generate_appcast --ed-key-file`); delete the file after |

The appcast's `SUFeedURL` is `https://github.com/darekhta/Mist/releases/latest/download/appcast.xml`
and each enclosure points at `…/releases/download/v<version>/Mist-<version>.dmg`, so updates are
served straight from GitHub Releases — no external host.

## Why these choices

- **Not sandboxed** (the app opens hostd's UDS + drives a root mount-helper), so Sparkle's
  Installer/Downloader XPC services are stripped and the in-process installer is used
  (`SUEnableInstallerLauncherService = false`).
- **Stable code-sign identifiers** (`dev.mist.app`, `dev.mist.hostd`, …) — the mount-helper's client
  requirement pins them, and a changed identifier on upgrade trips an AMFI launch constraint.
- **Bottom-up signing, never `--deep`** — inner binaries + `Sparkle.framework` are signed before the
  outer app, each with its own entitlements.
