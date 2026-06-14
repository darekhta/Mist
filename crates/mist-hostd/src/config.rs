//! Host daemon configuration.
//!
//! Pairing writes `[vm.<name>]` blocks **programmatically** via [`config_writer`] (design 11 §4) —
//! a `bridge="auto"` VM stores no network coordinate, only its stable `vm_uuid` and token path.
//! The resolver (design 11 §2) derives a fresh address on every connect, so DHCP drift is a
//! non-event: there is no stored IP to go stale.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub daemon: Daemon,
    #[serde(default)]
    pub vm: BTreeMap<String, VmConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Daemon {
    /// Control socket path; relative paths resolve under the state dir.
    pub control_socket: PathBuf,
    pub log: Option<String>,
    /// Content cache (CAS) high watermark in bytes; 0 disables the cache.
    pub cache_max_bytes: u64,
}

impl Default for Daemon {
    fn default() -> Self {
        Daemon {
            control_socket: "control.sock".into(),
            log: None,
            cache_max_bytes: 20 << 30, // design 04 §6: 18/20 GiB watermarks
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VmConfig {
    /// Transport: `"auto"` (resolver chain keyed by `vm_uuid`, what pairing writes) |
    /// `"tcp:host:port"` | `"bridge:<path>[#port]"` | `"uds:<path>"` (design 11 §2.1).
    pub bridge: String,
    /// Path to the shared token file (32+ raw bytes). A leading `@` resolves under the state dir
    /// (e.g. `@vms/dev.token` → `<state_dir>/vms/dev.token`) — the form pairing writes.
    pub token: PathBuf,
    /// Expected stable VM identity (32 hex chars), bound at pairing for `bridge="auto"`. The
    /// resolver matches on it; the authenticated probe rejects a guest whose `vm_uuid` differs.
    #[serde(default)]
    pub vm_uuid: Option<String>,
    /// Attach automatically at startup.
    #[serde(default = "default_true")]
    pub autoattach: bool,
    /// Auto-mount this VM's shares at `~/Mist/<vm>/<share>` whenever it connects (best-UX default
    /// for VMs added via `mist add` / the app; the resolver makes reconnect a non-event).
    #[serde(default)]
    pub automount: bool,
}

fn default_true() -> bool {
    true
}

impl VmConfig {
    /// `true` when this VM is located by the resolver rather than a pinned endpoint.
    pub fn is_auto(&self) -> bool {
        self.bridge.eq_ignore_ascii_case("auto")
    }

    /// The token path with a leading `@` resolved under the state dir.
    pub fn resolved_token(&self) -> PathBuf {
        resolve_at(&self.token)
    }

    /// The expected `vm_uuid` as raw bytes, if configured and well-formed.
    pub fn expected_uuid(&self) -> Option<[u8; 16]> {
        self.vm_uuid
            .as_deref()
            .and_then(mist_proto::vm_uuid_from_hex)
    }
}

/// Resolve a `@`-prefixed path under the state dir; pass through anything else unchanged.
pub fn resolve_at(p: &std::path::Path) -> PathBuf {
    resolve_at_under(&state_dir(), p)
}

/// Pure form of [`resolve_at`] with an explicit base (testable without touching the environment).
pub fn resolve_at_under(base: &std::path::Path, p: &std::path::Path) -> PathBuf {
    match p.to_str().and_then(|s| s.strip_prefix('@')) {
        Some(rest) => base.join(rest),
        None => p.to_path_buf(),
    }
}

pub fn state_dir() -> PathBuf {
    if let Ok(d) = std::env::var("MIST_STATE_DIR") {
        return d.into();
    }
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join("Library/Application Support/Mist")
}

pub fn default_config_path() -> PathBuf {
    state_dir().join("config.toml")
}

/// Where shares are mounted: the **visible** `~/Mist` (design 11 §7, OrbStack pattern) — inside
/// `$HOME` so Finder reaches it and the user can drag-to-pin, not the Finder-hidden state dir.
/// `MIST_MOUNT_ROOT` overrides it (tests/e2e).
pub fn mount_root() -> PathBuf {
    if let Ok(d) = std::env::var("MIST_MOUNT_ROOT") {
        return d.into();
    }
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join("Mist")
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        Ok(toml::from_str(&text)?)
    }

    pub fn empty() -> Self {
        toml::from_str("").expect("empty config valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_and_token_resolution() {
        let toml_src = r#"
[vm.dev]
bridge = "auto"
vm_uuid = "4f2b0b30a9d84f8ab5c40122c7be1c13"
token = "@vms/dev.token"
"#;
        let cfg: Config = toml::from_str(toml_src).unwrap();
        let vm = &cfg.vm["dev"];
        assert!(vm.is_auto());
        assert_eq!(
            vm.expected_uuid().map(|u| mist_proto::vm_uuid_hex(&u)),
            Some("4f2b0b30a9d84f8ab5c40122c7be1c13".to_string())
        );
        // `@`-relative token resolves under the (here explicit) state dir.
        assert_eq!(
            resolve_at_under(std::path::Path::new("/tmp/mist-state"), &vm.token),
            PathBuf::from("/tmp/mist-state/vms/dev.token")
        );
    }

    #[test]
    fn fixed_bridge_is_not_auto() {
        let cfg: Config =
            toml::from_str("[vm.x]\nbridge=\"tcp:1.2.3.4:6478\"\ntoken=\"/etc/t\"\n").unwrap();
        assert!(!cfg.vm["x"].is_auto());
        assert_eq!(cfg.vm["x"].resolved_token(), PathBuf::from("/etc/t"));
        assert_eq!(cfg.vm["x"].expected_uuid(), None);
    }
}
