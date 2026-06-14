# Installing Mist

Mist has two halves: **`mist-hostd` + `mist`** on the Mac, and **`mistd`** inside the Linux VM.
Target: clean install on a fresh Mac + fresh Debian VM in ≤ 10 minutes.

## 0. Onboarding (recommended) — copy the token, the rest is autodiscovered

The onboarding model is deliberately simple: **the token is the one thing you copy by hand; the
address is never stored or typed.** `bridge="auto"` makes hostd locate the guest on every connect
(mDNS `_mist._tcp` → vmnet lease/ARP scan → token-authenticated probe), keyed by a stable
`vm_uuid`, so DHCP drift is a non-event. No ssh, no sudo dance, no codes.

**1. Guest** — install + advertise (responsible `curl | sh`, GitHub-hosted):

```sh
curl --proto '=https' --tlsv1.2 -sSf \
  https://raw.githubusercontent.com/darekhta/Mist/main/packaging/install.sh | sh -s -- --share code=/srv/code
```

It auto-detects distro/arch, verifies SHA-256 (and minisign if a key is published) before
`dpkg -i`, writes `/etc/mist/mistd.toml` with a vmnet-bound TCP listener, mints `/etc/mist/vmid`,
drops the `_mist._tcp` avahi advert, and starts `mistd`. (`--share` is optional — `mistd` only
starts once at least one share is configured; without it the installer tells you how to add one.)

**2. Copy the token once** — the guest's `/etc/mist/token`, however you like, e.g.:

```sh
# on the guest:                          # on the Mac:
sudo base64 /etc/mist/token              # ← copy the line, then on the Mac:
                                         echo 'PASTED' | base64 -d > ~/dev.token
```

**3. Mac** — add it; autodiscovery + identity binding are automatic:

```sh
mist discover                 # (optional) list _mist._tcp guests Mist can see
mist add dev --token ~/dev.token
mist mount dev code           # auto-resolves the guest by vm_uuid every connect
```

`mist add` copies the token to `@vms/dev.token`, finds the guest that authenticates with it, binds
its `vm_uuid`, and writes `[vm.dev] bridge="auto"` programmatically (`toml_edit`, never hand-edited).

Or do all of step 3 from the **menu-bar app** (`Mist.app`): it lists discovered guests under
**Discovered**, you click **Add…**, pick the token file, and it appears with one-click **Mount**.

The **Mist.app** menu-bar client exists in `swift/MistApp` and wraps discovery, **Add…**,
status, health, and mount/reveal over hostd's control socket. DMG/notarization/Sparkle/Cask files
are scaffolded but still require real signing credentials, Sparkle keys, and checksums before they
are a public distribution channel. The sections below are the **manual** equivalent for scripted
setups or when you'd rather wire it by hand.

## 1. Mac side

### From a release tarball

```sh
tar -xzf mist-<version>-macos-arm64.tar.gz
sudo install -m 755 mist mist-hostd /usr/local/bin/
```

### From source

```sh
cargo build --release -p mist-hostd -p mist-cli
install -m 755 target/release/{mist,mist-hostd} /usr/local/bin/
```

Requirements: macOS 15+ on Apple silicon for the CLI/daemon path (NFSv4.1 delegations verified on
macOS 26.x), Xcode CLT only if building from source. No kernel extension is required. The Swift app
uses `SMAppService` and targets macOS 13+, but app distribution is still scaffolded.

## 2. Guest side (Debian/Ubuntu, kernel ≥ 6.1)

### From the .deb

The `release` CI workflow publishes a package per distro × arch, install-tested in each:
`mistd_<version>~<codename>_<arch>.deb` — Debian **bullseye**, **trixie**, and the two latest
Ubuntu releases, for **arm64** (the Apple-Virtualization guest) and **amd64**. `mistd` is a
static musl binary with no runtime deps, so the package installs cleanly on any of them.

```sh
# pick the build matching your VM's distro+arch (arm64 on Apple silicon):
sudo dpkg -i mistd_<version>~trixie_arm64.deb   # installs /usr/sbin/mistd + systemd unit + token
sudo systemctl enable --now mistd
```

> bullseye ships Linux 5.10, below the 5.17 (FAN_RENAME) the journal prefers — mistd installs
> and runs there in a degraded mode. Use trixie / a backported kernel for full fidelity.

To build the packages yourself: `packaging/release.sh` (mac tarball + arm64/amd64 debs), or a
single one with `DEB_ARCH=amd64 CODENAME=trixie packaging/package-deb.sh`.

### From source (cross-compiled on the Mac)

```sh
RUSTFLAGS="-C linker=rust-lld -C link-self-contained=yes" \
  cargo build --release --target aarch64-unknown-linux-musl -p mistd
# copy target/aarch64-unknown-linux-musl/release/mistd into the guest, e.g. over ssh
```

`mistd` needs root in the guest (fanotify FAN_MARK_FILESYSTEM + open_by_handle_at). The unit
in `packaging/mistd.service` carries the hardening options (`ProtectSystem=full` is
incompatible with the applier — see comments in the unit).
Kernel 6.12+ is recommended for newer vsock fixes; on older supported kernels, prefer the TCP
transport if vsock bulk throughput is unstable.

## 3. Shared token

Both sides authenticate with a 32-byte token file:

```sh
# on the Mac
mkdir -p ~/Library/Application\ Support/Mist
head -c 32 /dev/urandom > ~/Library/Application\ Support/Mist/token-devvm
chmod 600 ~/Library/Application\ Support/Mist/token-devvm
# copy the SAME bytes to the guest as /etc/mist/token (mode 600, owner root)
```

## 4. Configure + run

`~/Library/Application Support/Mist/config.toml`:

```toml
[vm.devvm]
# How to reach the guest's vsock: a Virtualization.framework bridge socket,
# or tcp:IP:PORT if you expose mistd over virtio-net (faster bulk, see docs/tuning.md).
bridge = "bridge:/path/to/vm-vsock.sock"
token = "/Users/you/Library/Application Support/Mist/token-devvm"
```

Guest `/etc/mist/mistd.toml`:

```toml
listen = ["vsock:6478"]      # a list — e.g. ["vsock:6478", "tcp:0.0.0.0:6478"]
token_file = "/etc/mist/token"

[share.code]
path = "/srv/code"
# commit = "writeback"   # faster small-file saves; fsync (default) for strict durability
```

Validate the file before starting: `mistd --check-config`.

Then:

```sh
mist-hostd &           # or install packaging/com.mist.hostd.plist as a LaunchAgent
mist status            # wait for [live]
mist mount devvm code  # -> mounted at .../mnt/devvm/code
mist doctor            # verify everything is green
```

## 5. Verify

```sh
mist doctor            # exit 0 = healthy; ⚠/✗ lines explain themselves
echo hello > "$(mist mount devvm code 2>/dev/null || true)"/hello.txt
```

A change made inside the guest appears on the Mac in ≤ 1 s on `--nfs41` mounts
(journal-driven delegation recall); ≤ `actimeo` (5 s) on plain v3 mounts.
