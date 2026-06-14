# 07 — Security

## 1. Assets & principals

Assets: the guest source trees (integrity + confidentiality), the Mac user account (hostd runs as
the user), guest system integrity (mistd runs privileged *inside* the guest).
Principals: the Mac user (owner/operator), the guest OS (possibly compromised), other local Mac
processes/users, the LAN (TCP transport mode only).

Trust model: **single human operator**; the guest is *semi-trusted* (it's the user's own VM, but
VMs run third-party code — treat guest input as hostile at every parse and apply point). Mist is
explicitly **not** a boundary between mutually distrusting humans (§5).

## 2. Threat table

| Threat | Vector | Mitigation |
|---|---|---|
| Compromised guest sends malicious protocol data | journal/snapshot/RPC replies | §3 hardening: bounded decode, name grammar, invariant-checked apply, no host-path construction from guest bytes |
| Guest tries to escape its share via the applier | crafted parent handles/names | applier containment: fsid pinning, single-component names by grammar, `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS)`, no path strings accepted (`03` §7.1) — note this protects the *guest* from a confused host, and the host's view from cross-share leaks |
| Malicious local Mac process reads/writes the share via the loopback NFS port | TCP 127.0.0.1:2049x is connectable by any local process; AUTH_SYS is forgeable | accepted residual risk under the single-user model, reduced by: random per-boot *handle* MAC (can't mint handles without a MOUNT/EXPORT walk), exports root fh only via mountd on loopback, optional `--harden` pf anchor (loopback port allow-list to the user's processes is not expressible in pf — the anchor instead restricts to uid via `pf` `user` match on loopback), and documented: multi-user Macs should not run Mist without an external access-control boundary |
| Handle forgery/guessing | NFS fh is a bearer token | 8-byte keyed-BLAKE3 MAC in every handle (`05` §2); secret 0600, regenerable (`mist doctor --rotate-handles` forces remount) |
| Token theft → fake guest session | UDS/vsock dial | token file 0600 root (guest) / 0600 user (host); vsock is host-local by construction; TCP mode requires a token and is documented for trusted networks or external tunnels only |
| hostd memory exhaustion via journal/snapshot bombs | event storms, giant names/dirs | decode caps (`02` §8), per-share node cap, memory ceiling + shed ladder (`04` §11), quiescent-storm mode guest-side |
| CAS/side-store path injection | hash-named blobs only; redb keys are binary NodeKeys — no guest-controlled file names ever touch host paths | by construction |
| Supply chain | crates | minimal pinned tree, `cargo-deny` (licenses+advisories) in CI, lockfile committed, release builds reproducible-ish (`--locked`, pinned toolchain), signed release artifacts |
| Privileged guest daemon as attack surface | mistd parses host input as root | host is *more* trusted than guest in our model, but mistd still bounds-checks all decodes (symmetric hardening), drops to capability set (`03` §9), systemd sandboxing |

## 3. Guest-input hardening rules (normative)

1. Every decoded `Vec`/`Bytes`/`String` has a compile-time cap (`02`); postcard decoding is
   length-prefixed — reject before allocate.
2. `Name` grammar enforced at decode: 1..=255 bytes, no `/`, no NUL, ≠ "." and "..".
3. Depth/count caps: tree depth ≤ 512 (PENDING-parent chains), entries/dir ≤ 8M, records/batch,
   batches/s budget (excess ⇒ session DEGRADED, never OOM).
4. Apply-time invariant checks degrade to RescanDir/resync (self-heal), never UB or panic-loop.
5. Decode failure or invariant storm (> 100/s) ⇒ session teardown + backoff + `mist status` alarm.
6. Fuzzing is a release gate: protocol decoder + journal applier + XDR (`10` §3).

## 4. Cryptography (deliberately boring)

- Session auth: 32-byte random token, BLAKE3-hashed in Hello, constant-time compare.
- Handle MAC: keyed BLAKE3, 8-byte truncation (forgery ≈ 2⁶⁴ online attempts against a loopback
  service that rate-limits ESTALE storms).
- TCP-mode encryption: out of scope for this release because vsock/UDS are host-local. Use an
  external encrypted tunnel when remote-Linux mode crosses an untrusted network.

## 5. Explicit non-guarantees

- Any process running as the Mac user can read/write mounted shares (it's a filesystem) and the
  control socket. That is the *point* — but it means malware-as-user gets the shares too, same as
  local files.
- AUTH_SYS uid assertions from other local users are not authenticated on the loopback NFS port;
  on a multi-user Mac, another user reaching the per-share 127.0.0.1 listener with a
  stolen/guessed handle could access share data. Single-user assumption documented in README +
  `mist doctor` warns when other active local users exist.
- mistd runs privileged in the guest; a guest-root compromise owns the shares trivially (they
  live there) — Mist adds no new guest-side exposure beyond the listener (token-gated, vsock).

## 6. Operational hygiene

- Logs never contain file names or data; paths appear as `share:NodeKey` (debug builds may opt
  in to names via `MIST_LOG_NAMES=1`).
- `mist doctor` security section: token file modes, handle-secret age, other-users warning,
  cleartext TCP warning, pf anchor status.
- Secrets rotation: token (regenerate both sides, restart), handle secret (rotate + remount).
- Crash artifacts (panics) scrubbed of payload bytes by the tracing layer.
