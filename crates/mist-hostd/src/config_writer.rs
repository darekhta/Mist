//! Programmatic `config.toml` editing (design 11 §4: "never hand-edited TOML, ever").
//!
//! `mist add` writes exactly one `[vm.<name>]` block and must not disturb the daemon section, other
//! VMs' blocks, comments, or whitespace. `toml_edit` parses into a format-preserving document, so
//! we mutate a single table and re-serialize the rest verbatim. The write is atomic (temp + rename)
//! and the file is forced to mode 0600 — it holds token *paths* and identities, not secrets, but
//! the state dir is a single-user trust boundary (design 07).

use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use toml_edit::{DocumentMut, Item, Table, value};

/// The fields `mist add` binds for a discovered VM.
#[derive(Debug, Clone)]
pub struct VmBinding {
    pub name: String,
    /// `"auto"` for resolver-located VMs; an explicit endpoint string otherwise.
    pub bridge: String,
    pub vm_uuid: [u8; 16],
    /// Token path as written (e.g. `@vms/dev.token`).
    pub token: String,
}

/// Insert or replace `[vm.<name>]` in the document text, preserving everything else. Returns the
/// new document text. Pure (no I/O) so it is directly unit-testable.
pub fn upsert_vm(existing: &str, b: &VmBinding) -> anyhow::Result<String> {
    let mut doc: DocumentMut = existing
        .parse()
        .map_err(|e| anyhow::anyhow!("existing config.toml is malformed: {e}"))?;

    // Ensure a `[vm]` table exists and is rendered as a real table (not inline).
    if !doc.contains_key("vm") {
        let mut t = Table::new();
        t.set_implicit(true);
        doc["vm"] = Item::Table(t);
    }
    let vm_tbl = doc["vm"]
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("[vm] is not a table"))?;

    let mut block = Table::new();
    block["bridge"] = value(&b.bridge);
    block["vm_uuid"] = value(mist_proto::vm_uuid_hex(&b.vm_uuid));
    block["token"] = value(&b.token);
    block["automount"] = value(true);
    vm_tbl.insert(&b.name, Item::Table(block));

    Ok(doc.to_string())
}

/// Like [`upsert_vm`] but with no `vm_uuid` — used when the guest wasn't reachable at add time, so
/// the VM is bound token-only and binds its identity on first connect. Pure; tested.
pub fn upsert_vm_no_uuid(existing: &str, name: &str, token: &str) -> anyhow::Result<String> {
    let mut doc: DocumentMut = existing
        .parse()
        .map_err(|e| anyhow::anyhow!("existing config.toml is malformed: {e}"))?;
    if !doc.contains_key("vm") {
        let mut t = Table::new();
        t.set_implicit(true);
        doc["vm"] = Item::Table(t);
    }
    let vm_tbl = doc["vm"]
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("[vm] is not a table"))?;
    let mut block = Table::new();
    block["bridge"] = value("auto");
    block["token"] = value(token);
    block["automount"] = value(true);
    vm_tbl.insert(name, Item::Table(block));
    Ok(doc.to_string())
}

/// Atomically write the binding into `config.toml`, creating the file (and `[vm]` section) if
/// absent. Concurrent `mist add` runs are not expected (the app serializes them through hostd),
/// so a last-writer-wins atomic replace is sufficient.
pub fn write_binding(config_path: &Path, b: &VmBinding) -> anyhow::Result<()> {
    let existing = std::fs::read_to_string(config_path).unwrap_or_default();
    let updated = upsert_vm(&existing, b)?;

    if let Some(dir) = config_path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = config_path.with_extension("toml.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(updated.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, config_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: [u8; 16] = [
        0x4f, 0x2b, 0x0b, 0x30, 0xa9, 0xd8, 0x4f, 0x8a, 0xb5, 0xc4, 0x01, 0x22, 0xc7, 0xbe, 0x1c,
        0x13,
    ];

    fn binding(name: &str) -> VmBinding {
        VmBinding {
            name: name.into(),
            bridge: "auto".into(),
            vm_uuid: UUID,
            token: format!("@vms/{name}.token"),
        }
    }

    #[test]
    fn writes_expected_block() {
        let out = upsert_vm("", &binding("dev")).unwrap();
        // Re-parse to assert the values landed (don't pin exact whitespace).
        let cfg: crate::config::Config = toml::from_str(&out).unwrap();
        let vm = &cfg.vm["dev"];
        assert_eq!(vm.bridge, "auto");
        assert!(vm.is_auto());
        assert_eq!(vm.expected_uuid(), Some(UUID));
        assert_eq!(vm.token, std::path::PathBuf::from("@vms/dev.token"));
    }

    #[test]
    fn preserves_other_vms_and_comments() {
        let existing = "\
# my mist config
[daemon]
cache_max_bytes = 1073741824

[vm.other]
bridge = \"tcp:1.2.3.4:6478\"
token = \"/etc/mist/other.token\"
";
        let out = upsert_vm(existing, &binding("dev")).unwrap();
        assert!(out.contains("# my mist config"), "comment preserved");
        assert!(out.contains("cache_max_bytes = 1073741824"));
        let cfg: crate::config::Config = toml::from_str(&out).unwrap();
        assert_eq!(cfg.vm["other"].bridge, "tcp:1.2.3.4:6478");
        assert_eq!(cfg.vm["dev"].bridge, "auto");
        assert_eq!(cfg.daemon.cache_max_bytes, 1_073_741_824);
    }

    #[test]
    fn replaces_existing_block_idempotently() {
        let first = upsert_vm("", &binding("dev")).unwrap();
        let second = upsert_vm(&first, &binding("dev")).unwrap();
        let cfg: crate::config::Config = toml::from_str(&second).unwrap();
        assert_eq!(cfg.vm.len(), 1, "no duplicate [vm.dev] block");
        assert_eq!(cfg.vm["dev"].expected_uuid(), Some(UUID));
    }

    #[test]
    fn rejects_malformed_existing() {
        assert!(upsert_vm("this is = = not toml", &binding("dev")).is_err());
    }
}
