# 11 — Onboarding, Discovery & App Integration

This document describes the onboarding architecture as it exists in the current worktree. The model
is deliberately small: **you copy the guest's token once; everything else is autodiscovered.** There
is no ssh-bootstrap, no enrollment code, and no sudo provisioning dance — those were removed as
complexity that did not earn its keep. What remains is a resolver (`bridge="auto"`), a stable guest
identity, an mDNS advertisement, a one-shot `mist add`, a guest installer, and a Swift menu-bar app
that turns discovery + mount into clicks.

Tag legend (extends `08-performance.md`): **[R]** researched / platform fact · **[E]** engineering
estimate · **[V]** verified on-device 2026-06-14 on the reference rig (UTM Apple-Virtualization
backend, Debian guest kernel 6.1, macOS 26, vmnet "Shared" / `bridge100` / 192.168.64.0/24) ·
**[I]** implemented in this worktree.

## 0. Current Shape

The old manual path was: install Mac binaries, hunt the guest DHCP address, hand-edit
`/etc/mist/mistd.toml` for TCP, generate a token and copy the bytes to both machines, then hand-write
the Mac `config.toml`. The implemented path keeps exactly one of those steps — the token copy — and
autodiscovers the rest:

```sh
# guest: install + advertise (one share required to serve)
curl --proto '=https' --tlsv1.2 -sSf \
  https://raw.githubusercontent.com/darekhta/Mist/main/packaging/install.sh | sh -s -- --share code=/srv/code

# copy the token once, any way you like (e.g. `sudo base64 /etc/mist/token` → decode on the Mac)

# Mac: add it — autodiscovery + identity binding are automatic
mist discover                       # optional: list _mist._tcp guests
mist add dev --token ~/dev.token
mist mount dev code
```

The host config written by `mist add` stores identity, never an address:

```toml
[vm.dev]
bridge  = "auto"
vm_uuid = "4f2b0b30a9d84f8ab5c40122c7be1c13"
token   = "@vms/dev.token"
```

`@...` paths resolve under `~/Library/Application Support/Mist` or `$MIST_STATE_DIR`. Because no IP
is stored, DHCP drift is a non-event — the resolver re-derives the address on every connect.

The whole of the Mac step is also one flow in **`Mist.app`**: discovered guests appear under
**Discovered**, you click **Add…**, pick the token file, and the VM appears with one-click **Mount**.

## 1. Implemented Components

| Component | Implemented surface |
|---|---|
| `mist-hostd` | `bridge="auto"` resolver, the `add` control verb (token → autodiscover → bind → hot-add), config writer, doctor checks for auto/vmnet/mDNS/VPN route |
| `mist` CLI | `discover`, `add`, updated `doctor`, and the existing mount/status/events verbs |
| `mistd` | stable `/etc/mist/vmid`, `VmIdentity` response, avahi service-file publication |
| `mist-proto` | feature bit `VM_IDENTITY`, appended `CtlMsg::VmIdentity` |
| guest packaging | `.deb` includes `vmid_file`; `install.sh` writes config, optionally configures a share, and starts `mistd` only once a share exists |
| Mac app | SwiftUI `MenuBarExtra` over the hostd control socket: Discovered list → **Add…** (token file picker) → status/mount/health |
| app packaging | `packaging/build-app.sh`, `mac-app` workflow, Sparkle appcast template, and cask template |

The Mac app and app distribution are scaffolded but not yet a published, polished release: signing
identities, Sparkle keys, appcast enclosure data, cask checksum, and helper code-signing team ID are
placeholders until release credentials are installed.

## 2. Resolver: `bridge = "auto"`

Implemented in [resolve.rs](/Users/darekhta/Development/Mist/crates/mist-hostd/src/resolve.rs).
A VM configured with `bridge = "auto"` is resolved on every connect. The configured `vm_uuid` (when
present) is the expected identity; the resolved IP is never written back to config.

Resolver chain:

| Step | Implementation | Notes |
|---|---|---|
| 1 | `dns-sd -B _mist._tcp`, then `dns-sd -L <instance> _mist._tcp` | Uses the system `dns-sd` tool through `mDNSResponder`; no native DNS-SD dependency yet |
| 2 | `getaddrinfo(<cached-host>.local)` | Cache stores `vm_uuid -> { instance, host }` in `resolver-cache.toml`; no IP cache |
| 3 | vmnet model + candidate list | `getifaddrs()` finds private `bridge1xx`; `/var/db/dhcpd_leases` and `arp -an` provide candidates; if empty, a bounded 256-address sweep is used |
| 4 | authenticated probe | Sends `Hello` with token hash and `VM_IDENTITY`; accepts only a matching `VmIdentity` when an expected UUID is configured |

A VM may also be bound token-only (no `vm_uuid` — when the guest wasn't reachable at `add` time): the
resolver then accepts the guest the token authenticates, and identity is observed on first connect.

Explicit bridges still exist for manual and test setups:

```toml
bridge = "tcp:<host-or-ip>:<port>"
bridge = "bridge:/path/to/MistBridge.sock"       # optional #<vsock-port> suffix
bridge = "uds:/path/to/fake-guest.sock"
```

Doctor currently checks: `bridge="auto"` entries have a plausible `vm_uuid`; vmnet bridges are
discoverable; `route -n get <vmnet-gateway>` does not egress through `utun*`; `_mist._tcp` mDNS is
browsable (warning when discovery must fall back to scan/probe); cleartext TCP is authenticated but
not encrypted until the future Noise transport.

## 3. Stable Guest Identity

Implemented in [identity.rs](/Users/darekhta/Development/Mist/crates/mistd/src/linux/identity.rs),
[msg.rs](/Users/darekhta/Development/Mist/crates/mist-proto/src/msg.rs), and
[lib.rs](/Users/darekhta/Development/Mist/crates/mist-proto/src/lib.rs).

`mistd` loads `/etc/mist/vmid`, minting 16 random bytes if the file is absent or malformed, written
as 32 lowercase hex characters, mode 0644. This is an identifier, not a secret.

Wire compatibility is preserved by appending a control message rather than changing `HelloAck`:

```rust
pub const VM_IDENTITY: u64 = 1 << 7;

enum CtlMsg {
    // existing variants keep their order
    VmIdentity { vm_uuid: [u8; 16] },
}
```

When the host sets `VM_IDENTITY`, `mistd` sends `VmIdentity` immediately after `HelloAck` and before
`Endpoints`. Old hosts never set the bit, so old peers never receive the new variant.

## 4. Guest Advertisement

`mistd` publishes `_mist._tcp` by writing an avahi static service file (not avahi D-Bus). The default
path is `/etc/avahi/services/mist.service`; an empty `avahi_service_file` disables publication. TXT
contents are non-secret:

```text
v=1
uuid=<vm_uuid hex>
tx=tcp
shares=<count>
kver=<kernel>
```

The installer also drops a floor advert when `/etc/avahi/services` already exists; at runtime `mistd`
overwrites it with the live `vm_uuid`. If avahi is not installed, installation still works — host
discovery degrades to lease/ARP scan plus authenticated probe.

## 5. Adding a VM (token + autodiscovery)

Implemented as the `add` control verb in
[control.rs](/Users/darekhta/Development/Mist/crates/mist-hostd/src/control.rs), surfaced through
`mist add <name> --token <file> [--uuid <hex>]`. This is the whole of pairing — there is no ssh, no
sudo, and no code.

Flow:

1. Read the token file; require ≥ 32 bytes. Copy it to `@vms/<name>.token` (0600).
2. Best-effort autodiscovery: run the resolver with the token's `Hello` hash. The guest that
   authenticates returns its `VmIdentity`; that becomes the bound `vm_uuid`. If `--uuid` was given,
   a mismatch is rejected (wrong token / wrong guest).
3. Write `[vm.<name>]` with `bridge="auto"`, `token="@vms/<name>.token"`, and `vm_uuid` (when known)
   via `toml_edit` — never hand-edited.
4. Hot-add the VM to the running daemon. If the guest wasn't reachable at add time, the VM is still
   added (token-only `auto`); the resolver connects and binds identity when the guest appears.

The only secret that crosses machines is the 32-byte token, copied once by the operator. It is never
displayed by Mist, never put on the wire in the clear (the `Hello` carries `BLAKE3(token)`), and
never stored in `config.toml` (only its `@vms/...` path is).

## 6. Guest Installer and Package

Implemented in [install.sh](/Users/darekhta/Development/Mist/packaging/install.sh) and
[package-deb.sh](/Users/darekhta/Development/Mist/packaging/package-deb.sh). Installer behavior:

- detects Debian/Ubuntu-like distro and `arm64`/`amd64`;
- resolves the matching `.deb` via the GitHub Releases API (robust to GitHub's `~`→`.` asset-name
  sanitization and to codename gaps — the static-musl binary is distro-independent);
- downloads `.deb` + `SHA256SUMS` (+ `SHA256SUMS.minisig` when present); always verifies SHA-256
  (normalizing `~`/`.` in the listed names), and verifies minisign when a real key is embedded
  (currently a placeholder, so release copies warn and skip signature enforcement until the real key
  is embedded);
- `dpkg -i` with `--force-confdef --force-confold` so a conffile prompt can't abort the `curl | sh`;
- writes `/etc/mist/mistd.toml` programmatically, preserving existing `[share.*]` blocks, binding TCP
  to the derived guest vmnet IP (or `0.0.0.0` with `--listen-all`);
- with `--share NAME=/path`, configures a share inline; `mistd` requires ≥ 1 share, so the installer
  only `enable --now`s it once a share exists, otherwise prints how to add one (no crash-loop);
- mints `/etc/mist/vmid` and drops an avahi floor service when avahi is installed.

## 7. Mac App and Helper

Implemented under [swift/MistApp](/Users/darekhta/Development/Mist/swift/MistApp). The app is a
SwiftUI `MenuBarExtra` client over hostd's newline-delimited JSON control socket; it implements no
Mist protocol logic in Swift (ADR-14). Current UI surface:

- **This VM / Shares**: status + per-share **Mount / Unmount / Reveal** via hostd's mount verbs;
- **Discovered**: guests from `discover` not yet configured, each with **Add…** → an `NSOpenPanel`
  token picker → the `add` verb (the panel equivalent of `mist add`);
- **Health**: rendered straight from `mist doctor`;
- `mist events --follow` stream plus DiskArbitration callbacks as refresh triggers, with a coarse
  poll backstop. mDNS discovery is polled on a slower timer.

A tiny root mount-helper (SMAppService daemon, `mount`/`unmount` XPC, code-signing-gated) is provided
for policy-blocked Macs; `SMAppService` registration is opt-in (`MIST_REGISTER_SERVICES=1`) so a
dev/unsigned launch never triggers a Login-Items or admin prompt. Current limitations: the menu still
mounts via hostd's unprivileged `mount_nfs` path; hostd mounts under `$MIST_STATE_DIR/mnt/<vm>/<share>`
rather than the future visible `~/Mist/<vm>/<share>`; the helper's `TEAMID` requirement is a template.

## 8. Distribution State

Implemented scaffolding: the `release` workflow builds guest `.deb`s and a Mac CLI tarball; the
`mac-app` workflow calls `packaging/build-app.sh` (universal Rust + Swift, bottom-up signing when
`MIST_SIGN_ID` is set, DMG + notarize/staple when `NOTARY_PROFILE` is set); `packaging/appcast.xml`
and `packaging/Casks/mist.rb` are templates.

Release blockers before this is a finished distribution channel: embed the real minisign key in
`install.sh`; replace the Sparkle public key + generate real appcast signatures/lengths; replace the
helper code-signing `TEAMID`; set the cask checksum; provide Developer ID + notary secrets in CI.

## 9. Security Boundaries

- The data transport over TCP/vmnet is authenticated by the token hash in `Hello`, but not encrypted.
  Noise TCP remains future work.
- `_mist._tcp` TXT carries identity and version metadata only, never tokens or token fingerprints.
- The token is the single shared secret. It is copied once by the operator, stored 0600 on both
  sides, never displayed by Mist, and only its path lives in `config.toml`.
- The Mac control socket is mode 0600.
- `mist doctor` checks config parseability, token length/mode, state-dir/handle-secret mode,
  auto/vm_uuid binding, vmnet discovery, mDNS availability, route hijack, and live mounts.

## 10. Remaining Gaps

- Native DNS-SD and richer firewall diagnostics are not implemented; the resolver shells out to
  `dns-sd`, `arp`, and `route`.
- The app/helper mount path is not yet the automatic fallback for policy-blocked user mounts.
- Finder-visible `~/Mist/<vm>/<share>` mounts are not the hostd default yet.
- Release signing/notarization/Sparkle/Cask files are templates until real credentials are installed;
  installer signature enforcement is incomplete until the real minisign public key is embedded.
