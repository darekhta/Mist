#!/bin/sh
# Mist guest installer — a responsible `curl | sh`, modeled on rustup (design 11 §5).
#
#   curl --proto '=https' --tlsv1.2 -sSf \
#     https://raw.githubusercontent.com/darekhta/Mist/main/packaging/install.sh | sh
#
# Installs mistd, generates its config, and starts it advertising over mDNS. Then, on the Mac, you
# copy this guest's token once and run `mist add <name> --token <file>` — autodiscovery does the rest.
#
# Flags (after `-s --`):
#   --share NAME=/path               configure a share inline (else add one to mistd.toml after)
#   --version vX.Y.Z                 install a specific release (default: latest)
#   --listen-all                     bind tcp:0.0.0.0:6478 (EXPERT; default binds the vmnet IP only)
#
# The ENTIRE body is a function invoked only on the last line, so a truncated download can never
# execute half a script. It is hosted from GitHub only and committed at packaging/install.sh so it
# is auditable; the Mac app links to the committed copy.
set -eu

REPO="darekhta/Mist"
# minisign public key for SHA256SUMS verification. Replace at release time; the placeholder makes
# signature verification mandatory-but-skippable with a loud warning (see verify_release()).
MINISIGN_PUBKEY="RWQPLACEHOLDERPLACEHOLDERPLACEHOLDERPLACEHOLDERPLACEHOLDER000="

say()  { printf 'mist: %s\n' "$1" >&2; }
err()  { printf 'mist: error: %s\n' "$1" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

need() { have "$1" || err "missing required command '$1' — install it and re-run"; }

# Pull a curl that refuses protocol downgrades (the design's `--proto '=https' --tlsv1.2`).
dl() { # dl URL OUTFILE
  curl --proto '=https' --tlsv1.2 -fsSL "$1" -o "$2"
}

detect_arch() {
  arch="$(dpkg --print-architecture 2>/dev/null || true)"
  case "$arch" in
    arm64|amd64) printf '%s' "$arch" ;;
    *) err "unsupported dpkg architecture '${arch:-unknown}' (need arm64 or amd64)" ;;
  esac
}

detect_distro() {
  [ -r /etc/os-release ] || err "no /etc/os-release — this installer targets Debian/Ubuntu guests"
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID:-}" in
    debian|ubuntu|raspbian) : ;;
    *) say "warning: untested distro '${ID:-?}'; the static musl .deb usually still installs" ;;
  esac
}

resolve_version() { # echoes the tag to install
  if [ -n "${MIST_VERSION:-}" ]; then printf '%s' "$MIST_VERSION"; return; fi
  # Follow the /releases/latest redirect to discover the tag — GitHub only, no API token.
  loc="$(curl --proto '=https' --tlsv1.2 -fsSLI -o /dev/null -w '%{url_effective}' \
           "https://github.com/$REPO/releases/latest" 2>/dev/null || true)"
  tag="${loc##*/tag/}"
  [ -n "$tag" ] && [ "$tag" != "$loc" ] || err "could not resolve the latest release tag; pass --version vX.Y.Z"
  printf '%s' "$tag"
}

# Verify SHA256SUMS via minisign (if a real key is embedded), then the .deb's checksum (design 11
# §5: both gates close the curl|sh trust gap). sha256 is always enforced; signature is enforced
# whenever minisign is present and a real pubkey is configured.
verify_release() { # verify_release DEB SUMS SIG
  deb="$1"; sums="$2"; sig="$3"

  case "$MINISIGN_PUBKEY" in
    RWQPLACEHOLDER*)
      say "warning: no release signing key embedded in this installer copy — skipping minisign"
      ;;
    *)
      if have minisign; then
        minisign -V -P "$MINISIGN_PUBKEY" -m "$sums" -x "$sig" >/dev/null 2>&1 \
          || err "minisign verification of SHA256SUMS FAILED — refusing to install"
        say "minisign signature on SHA256SUMS verified"
      elif [ "${MIST_INSECURE_SKIP_SIG:-0}" = "1" ]; then
        say "warning: minisign not installed and MIST_INSECURE_SKIP_SIG=1 — skipping signature check"
      else
        err "minisign not installed (apt-get install -y minisign) — install it, or set \
MIST_INSECURE_SKIP_SIG=1 to bypass (NOT recommended for a root daemon)"
      fi
      ;;
  esac

  # sha256: match the .deb's line in SHA256SUMS (always enforced). SHA256SUMS is generated from the
  # build filenames (with '~'), but GitHub serves the asset with '~' rewritten to '.', so normalize
  # the listed name before comparing basenames (the file CONTENT, and thus the hash, is identical).
  base="$(basename "$deb")"
  want="$(awk -v f="$base" '{ n=$2; gsub(/~/,".",n); sub(/^\.\//,"",n); if (n==f) { print $1; exit } }' "$sums")"
  [ -n "$want" ] || err "no checksum for $base in SHA256SUMS"
  got="$(sha256sum "$deb" | awk '{print $1}')"
  [ "$want" = "$got" ] || err "sha256 mismatch for $base (corrupt or tampered download)"
  say "sha256 verified"
}

guest_vmnet_ip() {
  # The vmnet address the host reaches us on = the source IP of our default route (the host is the
  # vmnet gateway). Used to bind the tcp listener to the right interface.
  ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -n1
}

install_deb() { # install_deb TAG ARCH
  tag="$1"; arch="$2"
  base="https://github.com/$REPO/releases/download/$tag"
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  cn="$(. /etc/os-release 2>/dev/null; printf '%s' "${VERSION_CODENAME:-}")"

  # Resolve the .deb's real download URL via the GitHub API. This is robust to two things a
  # constructed URL is not: (a) GitHub rewrites '~' to '.' in uploaded asset names, and (b) a guest
  # whose codename has no dedicated build — mistd is a static musl binary, distro-independent, so
  # ANY arch-matching .deb works; we just prefer the one tagged for this distro's codename.
  say "locating mistd $tag ($arch) on GitHub…"
  assets="$(curl --proto '=https' --tlsv1.2 -fsSL "https://api.github.com/repos/$REPO/releases/tags/$tag" \
    | grep -oE 'https://[^"]*/mistd_[^"]*_'"${arch}"'\.deb')" \
    || err "could not query GitHub release $tag (network, or the release has no assets yet)"
  url="$(printf '%s\n' "$assets" | grep -iE "[._~]${cn}_${arch}\.deb\$" | head -n1)"
  [ -n "$url" ] || url="$(printf '%s\n' "$assets" | head -n1)"  # any arch match (musl = portable)
  [ -n "$url" ] || err "no mistd .deb for $arch in release $tag"

  deb="$tmp/$(basename "$url")"
  say "downloading $(basename "$url")…"
  dl "$url" "$deb" || err "could not download $url"
  dl "$base/SHA256SUMS" "$tmp/SHA256SUMS" || err "could not download SHA256SUMS"
  dl "$base/SHA256SUMS.minisig" "$tmp/SHA256SUMS.minisig" 2>/dev/null || true

  verify_release "$deb" "$tmp/SHA256SUMS" "$tmp/SHA256SUMS.minisig"

  say "installing (dpkg -i)…"
  # Non-interactive conffile handling: under `curl | sh`, dpkg's stdin is the pipe (EOF), so a
  # conffile prompt would abort with "end of file on stdin". --force-confdef/--force-confold keep
  # the current /etc/mist/mistd.toml without asking; write_config() then (re)generates it anyway.
  $SUDO dpkg -i --force-confdef --force-confold "$deb" >/dev/null
}

write_config() { # write_config LISTEN_ALL(0|1) SHARE_SPEC(name=/path or "")
  listen_all="$1"; share_spec="$2"
  ip="$(guest_vmnet_ip || true)"
  $SUDO install -d -m 755 /etc/mist
  if [ "$listen_all" = "1" ] || [ -z "$ip" ]; then
    [ "$listen_all" = "1" ] || say "warning: could not derive the vmnet IP; binding tcp:0.0.0.0"
    listen='listen = ["vsock:6478","tcp:0.0.0.0:6478"]'
  else
    listen="listen = [\"vsock:6478\",\"tcp:$ip:6478\"]"
  fi
  # Generated, never hand-edited (design 11 §5). Preserve any existing [share.*] blocks.
  if [ -f /etc/mist/mistd.toml ] && grep -q '^\[share' /etc/mist/mistd.toml; then
    say "keeping existing share definitions in /etc/mist/mistd.toml; updating listener only"
    tmpcfg="$(mktemp)"
    grep -v '^listen' /etc/mist/mistd.toml > "$tmpcfg" || true
    { printf '%s\n' "$listen"; cat "$tmpcfg"; } | $SUDO tee /etc/mist/mistd.toml >/dev/null
    rm -f "$tmpcfg"
  else
    printf '%s\ntoken_file = "/etc/mist/token"\nvmid_file = "/etc/mist/vmid"\n' \
      "$listen" | $SUDO tee /etc/mist/mistd.toml >/dev/null
  fi
  # Optional inline share: --share name=/path → append a [share.<name>] block if absent.
  if [ -n "$share_spec" ]; then
    sname="${share_spec%%=*}"; spath="${share_spec#*=}"
    [ "$sname" != "$share_spec" ] && [ -n "$spath" ] || err "--share must be NAME=/path (got '$share_spec')"
    if ! grep -q "^\[share\.$sname\]" /etc/mist/mistd.toml 2>/dev/null; then
      printf '\n[share.%s]\npath = "%s"\n' "$sname" "$spath" | $SUDO tee -a /etc/mist/mistd.toml >/dev/null
    fi
  fi
  $SUDO mistd --check-config --config /etc/mist/mistd.toml >/dev/null \
    || err "generated /etc/mist/mistd.toml failed mistd --check-config"
}

has_share() { grep -q '^\[share' /etc/mist/mistd.toml 2>/dev/null; }

main() {
  share_spec=""
  listen_all=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --share) share_spec="${2:?--share needs NAME=/path}"; shift 2 ;;
      --share=*) share_spec="${1#--share=}"; shift ;;
      --version) MIST_VERSION="${2:?--version needs a tag}"; shift 2 ;;
      --listen-all) listen_all=1; shift ;;
      *) err "unknown argument '$1'" ;;
    esac
  done

  need curl; need dpkg; need sha256sum; need awk
  [ "$(id -u)" = 0 ] && SUDO="" || { have sudo || err "need root or sudo"; SUDO="sudo"; }

  detect_distro
  arch="$(detect_arch)"
  tag="$(resolve_version)"

  install_deb "$tag" "$arch"
  write_config "$listen_all" "$share_spec"
  drop_avahi

  # mistd refuses to start with zero shares; only enable+start it once a share exists, otherwise
  # tell the user how to add one (instead of leaving systemd to crash-loop).
  if has_share; then
    $SUDO systemctl enable --now mistd 2>/dev/null \
      || say "warning: 'systemctl enable --now mistd' did not succeed (no systemd?) — start mistd manually"
    say "done — mistd is serving + advertising over mDNS."
    say "next, on the Mac: copy this guest's token once, then:  mist add <name> --token <file>"
    say "  (token is /etc/mist/token — e.g.  sudo base64 /etc/mist/token  → decode on the Mac)"
  else
    say "installed, but no share is configured yet, so mistd is NOT started."
    say "add a share, then start it:"
    say "  sudo sh -c 'printf \"\\n[share.code]\\npath = \\\"/path/to/dir\\\"\\n\" >> /etc/mist/mistd.toml'"
    say "  (or re-run this installer with  --share code=/path/to/dir )"
    say "  then:  sudo systemctl enable --now mistd"
  fi
}

drop_avahi() {
  # Floor advert so the host can discover us even before mistd's runtime registration (design 11
  # §2). mistd overwrites this with the live vm_uuid on start; if avahi isn't installed, skip.
  [ -d /etc/avahi/services ] || { say "avahi-daemon not installed; mDNS discovery disabled (lease/ARP scan still works)"; return 0; }
  $SUDO tee /etc/avahi/services/mist.service >/dev/null <<'XML'
<?xml version="1.0" standalone='no'?>
<!DOCTYPE service-group SYSTEM "avahi-service.dtd">
<!-- Floor advert; mistd rewrites this with the live vm_uuid on start (design 11 §2). -->
<service-group>
  <name replace-wildcards="yes">Mist on %h</name>
  <service>
    <type>_mist._tcp</type>
    <port>6478</port>
    <txt-record>v=1</txt-record>
    <txt-record>tx=tcp</txt-record>
  </service>
</service-group>
XML
}

main "$@"
