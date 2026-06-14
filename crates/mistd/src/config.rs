//! /etc/mist/mistd.toml — guest daemon configuration.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const DEFAULT_CONFIG: &str = "/etc/mist/mistd.toml";
pub const DEFAULT_TOKEN: &str = "/etc/mist/token";
pub const DEFAULT_VMID: &str = "/etc/mist/vmid";
/// Directory avahi watches for static service files; mistd drops `mist.service` here so the
/// `_mist._tcp` advert carries the live `vm_uuid`/port/share count (design 11 §2).
pub const DEFAULT_AVAHI_SERVICE: &str = "/etc/avahi/services/mist.service";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// e.g. ["vsock:6478", "tcp:0.0.0.0:6478"]
    #[serde(default = "default_listen")]
    pub listen: Vec<String>,
    #[serde(default = "default_token_file")]
    pub token_file: PathBuf,
    /// Stable VM identity file (design 11 §6); minted on first start, persisted across reboots.
    #[serde(default = "default_vmid_file")]
    pub vmid_file: PathBuf,
    /// Where to drop the avahi `_mist._tcp` service file. Empty string disables the advert
    /// (e.g. when a packaging postinst owns a static one). Default: [`DEFAULT_AVAHI_SERVICE`].
    #[serde(default = "default_avahi_service")]
    pub avahi_service_file: PathBuf,
    #[serde(default)]
    pub log: Option<String>,
    #[serde(default)]
    pub share: BTreeMap<String, ShareConfig>,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub readonly: bool,
    /// Identity the write applier squashes to (default: the share root's owner).
    #[serde(default)]
    pub apply_uid: Option<u32>,
    #[serde(default)]
    pub apply_gid: Option<u32>,
    /// Durability of Mac-side COMMIT/sync writes (design 03 §applier).
    #[serde(default)]
    pub commit: CommitPolicy,
}

/// `fsync`: COMMIT/sync-write fdatasyncs in the guest (G2 durability — the default).
/// `writeback`: COMMIT is a no-op; data rides the guest's normal writeback. Trades the
/// power-loss window for one guest disk flush per save — the right call for build trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitPolicy {
    #[default]
    Fsync,
    Writeback,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Limits {
    pub inflight_rpc: usize,
    pub walker_parallelism: usize,
    pub snap_entries_per_record: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            inflight_rpc: 256,
            walker_parallelism: 16,
            snap_entries_per_record: 2048,
        }
    }
}

fn default_listen() -> Vec<String> {
    vec![format!("vsock:{}", mist_proto::VSOCK_PORT)]
}

fn default_token_file() -> PathBuf {
    DEFAULT_TOKEN.into()
}

fn default_vmid_file() -> PathBuf {
    DEFAULT_VMID.into()
}

fn default_avahi_service() -> PathBuf {
    DEFAULT_AVAHI_SERVICE.into()
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        Ok(toml::from_str(&text)?)
    }
}
