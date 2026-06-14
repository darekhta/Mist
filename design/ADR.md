# Architecture Decision Records

Format: context → decision → consequences. Statuses: A=accepted.

**ADR-1 (A) Replicate metadata host-side; never proxy per-op.**
Per-op RPC floors (FSKit ≈121 µs [R]; any VM hop ≥ tens of µs) × 200k-op storms = seconds-to-
minutes; three production systems converged on the proxy design and document its ceiling.
Consequence: RAM cost (~260 MB/1M nodes v1), seed/resync machinery, journal as the coherence
spine — accepted as the core bet of the project.

**ADR-2 (A) NFS (in-kernel macOS client, loopback userspace server) is the v1 mount surface.**
EdenFS/Buildbarn/OrbStack proved viability + Finder/editor compatibility; FSKit's per-op cost and
missing cache controls (verified in 26.5 SDK) disqualify it today. FSKit kept behind
`MountSurface` for when Apple lands caching.

**ADR-3 (A) NodeKey = guest (ino, i_generation), derived statelessly from fanotify handles.**
ext4 `FILEID_INO32_GEN` handles decode to (ino,gen) without syscalls → mistd needs no persistent
index, restarts are free, ABA (ino reuse) is handled by gen. Consequence: ext4-class filesystems
only in v1 (stable ino + exportable handles); ino64 reserved in the wire format.

**ADR-4 (A) fanotify FILESYSTEM marks (+DFID_NAME, +FID, FAN_RENAME), not recursive inotify.**
One mark per fs, race-free, names included [R man pages]; inotify needs per-dir watches (1M+
watches, racy setup). Consequences: kernel ≥6.1 floor; whole-mount shares recommended (subtree
filtering costs ~µs/event); overflow → resync path is mandatory design, not afterthought.

**ADR-5 (A) Journal-before-snapshot with idempotent upsert application (consistent cut).**
Standard change-feed pattern; removes any need for filesystem freezing. Consequence: replica
apply must be upsert/idempotent everywhere; anomalies degrade to targeted rescans (self-heal).

**ADR-6 (A) vsock via supervisor-embedded MistBridge (Firecracker UDS convention); TCP fallback.**
AVF restricts vsock to the VM-owning process; a 150-line Swift forwarder is the minimal
imposition on any supervisor, and the UDS convention interops with existing tooling. TCP keeps
Mist usable on QEMU/UTM/remote boxes and is the escape hatch if vsock underperforms (R1).

**ADR-7 (A) Close-to-open via journal freshness + unconditional open-revalidation; no per-op
guest verification by default.** The macOS client GETATTRs on open; our replica is journal-fresh
(ms); that composes to G1 without paying guest RTTs. `verify-on-open` exists per-share for
paranoid trees (mmap-writer blind spot).

**ADR-8 (A) NFSv3 first, then NFSv4.1 with read delegations recalled from the journal; skip v4.0.**
v3 = smallest correct thing (EdenFS precedent); v4.1 delegations turn our perfect change feed
into exact client-cache coherence for the hot set; v4.0's separate callback connection is legacy
pain. macOS 26.5 client supports vers=4.1 with callbacks on by default (verified locally).

**ADR-9 (A) Bespoke `mist-nfs` with a bounded spike on reusing `nfsserve` for read-only scaffolding.**
We need full control of handles (MAC), READDIRPLUS paging, wcc data, and the delegation hooks;
reuse only if it accelerates read-only scaffolding without constraining writes and delegations.

**ADR-10 (A) Optimistic replica apply for Mac mutations, with rollback; COMMIT is never
optimistic.** Gives native-feel write latency while G2 (durability at close) stays strict.
Consequence: DIRTY/rollback machinery and conflict policy (last-close-wins + visible log).

**ADR-11 (A) Side-store virtualizes Apple-only metadata; guest tree stays pristine.**
`.DS_Store`/AppleDouble/xattr noise is the top "VM share feels gross" complaint; absorbing it
host-side is cheap (redb) and reversible. Rule: side-store owns only names absent in the guest.

**ADR-12 (A) Identity squash by default.** Single-operator system; foreign-uid friction on the
Mac (git, editors) outweighs fidelity. `passthrough` retained for the exceptions; pjdfstest runs
in passthrough mode.

**ADR-13 (A) No standalone benchmark phase (user decision).** Transport/client validation
is folded into the runnable checks; `mist-bench transport` ships permanently inside `mist doctor`.
Consequence: if vsock numbers disappoint on a target setup, fallback is a config default change (TCP), not a
redesign — lanes and batching were sized for that from day one.

**ADR-14 (A) Rust workspace; Swift confined to MistBridge + optional FSKit appex.**
Daemons, protocol, NFS, replica, CAS, CLI: Rust (tokio, rustix, postcard, parking_lot, blake3,
redb, io-uring opt-in). No Mist logic in Swift; shims stay ~100–200 LoC.

**ADR-15 (A) Readdir cookies = per-dir monotonic insertion sequence; cookieverf = dirgen.**
Stable under concurrent mutation (NFS requirement), cheap, resumable; new entries appear at the
tail of in-progress enumerations — standard server behavior.

**ADR-16 (A) atime is not replicated; served as mtime; mounts use noatime.**
Real atime would make every read a journal event. Nobody close-to-open cares; documented.

**ADR-17 (A) Conflict policy: last-close-wins on content, guest-wins on metadata races, always
logged, never silent.** Close-to-open workloads make true conflicts rare; visibility
(`mist conflicts`) beats clever merging in a filesystem.

**ADR-18 (A) Names are bytes end-to-end; Unicode handled by a normalization-insensitive lookup
index, not by rewriting.** Linux names aren't UTF-8; rewriting breaks round-trips. NFC/NFD
mismatch (Finder) is solved at lookup; stored bytes are always returned.

**ADR-19 (A) Scrubber (sampled StatBatch verification) ships in v1.**
Converts the class of "fanotify missed something" bugs from silent divergence into a counted,
self-healing, alarmed condition. Cost <1 % CPU; the insurance is worth it for a system whose
whole thesis is "trust the journal."

**ADR-20 (A) License Apache-2.0 OR MIT (proposed; final call before first public tag).**
Maximizes adoption by VM supervisors/tools; the moat is execution + the replica design, not the
text of the license.

**ADR-21 (A) O_TMPFILE guest-side save staging: NOT implemented; rename-path atomicity is
the contract.** The design-03 §7.3 polish assumed the WriteStart/WriteEnd streaming protocol;
Mist uses inline writes + ranged COMMIT instead, so the applier never knows "whole-file
rewrite" intent up front. macOS's standard save pattern (NSDocument: temp file + rename) is
already crash-atomic guest-side through the atomic rename path; only in-place rewrites
(`>file` redirects) can tear on a mid-write crash — same exposure as local POSIX. Retrofitting
O_TMPFILE staging would require either protocol surgery or speculative buffering in the proven
write path days before 1.0 — bad risk for a corner the OS itself doesn't protect. Revisit only
if a write-id streaming protocol lands alongside write delegations.

**ADR-22 (A) Discovery is mDNS-first with a scan+probe fallback; an `auto` bridge never stores an
IP.** `_mist._tcp` advertised by mistd gives host+port+`vm_uuid` with no root; verified to cross the
UTM vmnet to the Mac (`11-onboarding.md` §2 [V]). DHCP-drift is designed out by re-resolving each
connect and caching identity, not address. Fallback chain (mDNS → vmnet lease/ARP scan →
token-authenticated probe) means discovery degrades but never dead-ends; the existing token is the
first disambiguator and the paired `vm_uuid` binding prevents a cloned/reused token from silently
binding the wrong guest. Consequence: a new stable `vm_uuid` (ADR-26), dynamic vmnet-subnet
detection (never hardcode 192.168.64), and a `mist doctor` line for the VPN route-hijack failure
that otherwise looks like "Mist is broken."

**ADR-23 (A, revised 2026-06-14) The token is copied once by hand; everything else is
autodiscovered. The wire token is unchanged.** An earlier build automated token provisioning via
ssh-bootstrap (mint on the Mac, push over ssh with host-key TOFU) plus single-use BLAKE3 enrollment
codes. **That was reverted as over-engineering** — it solved copying 32 bytes by adding key-based
ssh + passwordless sudo + a remote provisioning script + a bearer-code protocol, friction that
exceeded the problem. The accepted decision: the operator copies the guest's `/etc/mist/token` once
(any way they like), and `mist add <name> --token <file>` (or the app's **Add…**) stores it,
autodiscovers the guest that authenticates with it (the resolver, ADR-22), binds `vm_uuid`
(ADR-26), and writes `bridge="auto"` programmatically. The 32-byte `BLAKE3(token)` `Hello`
(`02` §2, `07` §4) is untouched. Consequence: pair.rs/enroll.rs and the enrollment wire protocol
are removed; the token is the single shared secret, never displayed by Mist, only its path stored
in config. Removed code: `crates/mist-hostd/src/{pair,enroll}.rs`, `crates/mist-proto/src/enroll.rs`,
`crates/mistd/src/linux/enroll_client.rs`, the `pair`/`enroll` verbs, and `mistd --enroll`.

**ADR-24 (A) The Mac app is a thin, non-sandboxed Developer-ID client; mount privilege is confined
to an XPC helper.** SwiftUI `MenuBarExtra` over the control UDS, no Mist logic in Swift (ADR-14).
App Sandbox/MAS is rejected because the shipped bundle needs a privileged helper, Finder-visible
mount control, and resolver fallback paths that do not fit the sandbox model. Discovery still
lives in `mist-hostd`/CLI, so the bundle carries local-network Info.plist declarations. The only
privileged grain is a tiny root mount-helper (`SMAppService` daemon, code-sign-gated XPC, single
`mount`/`unmount` method, allowlisted to loopback servers + `~/Mist` mountpoints + valid share
names/ports). The helper does not yet prove the port is owned by hostd; hostd's normal CLI/app path
still uses the unprivileged `mount_nfs` flow under `$MIST_STATE_DIR/mnt`. Running all of hostd as
root is rejected.

**ADR-25 (A) Distribution target is notarized DMG + Sparkle + Homebrew Cask; daemons use
SMAppService.** The worktree has `packaging/build-app.sh`, a `mac-app` workflow, Sparkle appcast
template, and cask template. Post-Sequoia the unsigned tarball remains a hard install blocker for
non-experts, so signing/notarization is the release target, not polish. Current release blockers:
real Developer ID/notary credentials, Sparkle key/signature generation, cask checksum, helper
TEAMID, and minisign release key. Source formula stays for CLI-only users; the SwiftUI app ships
prebuilt once those release gates are satisfied.

**ADR-26 (A) Add a stable per-guest `vm_uuid` to identity, distinct from `boot_id`/`epoch`.**
Discovery and config binding need an identity stable across guest reboots; `boot_id` is per-start
and share `epoch` is per-mount (`02` §2). `vm_uuid` is minted at install, persisted at
`/etc/mist/vmid`, advertised in the mDNS TXT, returned over a feature-gated appended
`CtlMsg::VmIdentity` after `HelloAck`, and stored in `config.toml` as the expected identity.
Do not add a field to `HelloAck`: postcard exact-consumption decoding makes that wire-incompatible
with old peers. `vm_uuid` is an identifier, not a secret.
