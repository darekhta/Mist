//! Control API: newline-delimited JSON over a unix socket (consumed by `mist` CLI and tools).

use crate::config::VmConfig;
use crate::mount::MountManager;
use crate::session::{VmHandle, VmState};
use mist_proto::Kind;
use mist_replica::ShareState;
use parking_lot::RwLock;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

const MAX_CAT: u32 = 10 * 1024 * 1024;
const LS_LIMIT: usize = 100_000;

#[derive(Debug)]
pub struct Control {
    /// VMs by name. A `RwLock` because the `add` verb hot-adds a VM into the live set without
    /// restarting hostd (design 11 §5).
    pub vms: RwLock<BTreeMap<String, Arc<VmHandle>>>,
    pub mounts: Arc<MountManager>,
    pub mount_root: PathBuf,
    /// Where `mist add` writes `[vm.<name>]` blocks (design 11 §5).
    pub config_path: PathBuf,
    /// CAS high-watermark, so a hot-added VM gets the same content cache as startup VMs.
    pub cache_max_bytes: u64,
}

impl Control {
    /// Snapshot of (name, handle) pairs — avoids holding the lock across an `.await`.
    fn vm_list(&self) -> Vec<(String, Arc<VmHandle>)> {
        self.vms
            .read()
            .iter()
            .map(|(n, v)| (n.clone(), v.clone()))
            .collect()
    }

    fn vm(&self, name: &str) -> Option<Arc<VmHandle>> {
        self.vms.read().get(name).cloned()
    }

    /// Build a `VmHandle`, start its supervise loop, and insert it into the live set. Used by the
    /// `add` verb once a VM's token is provided.
    pub fn add_vm(self: &Arc<Self>, name: &str, cfg: &VmConfig) -> anyhow::Result<Arc<VmHandle>> {
        let cas_cfg = (self.cache_max_bytes > 0).then(|| {
            mist_cas::CasConfig::new(
                crate::config::state_dir().join("cas").join(name),
                self.cache_max_bytes,
            )
        });
        let vm = VmHandle::new(name.to_string(), cfg, cas_cfg)?;
        vm.spawn_supervise();
        // Replacing an existing name: stop the old supervise loop first so we don't leave two
        // immortal reconnect tasks racing on the same guest (review finding).
        if let Some(old) = self.vms.write().insert(name.to_string(), vm.clone()) {
            old.stop_supervise();
        }
        self.spawn_automount(vm.clone());
        tracing::info!(vm = name, "VM added to live set");
        Ok(vm)
    }

    /// Spawn automount watchers for every already-configured VM (called once at startup; hot-added
    /// VMs get theirs in `add_vm`).
    pub fn start_automount(self: &Arc<Self>) {
        for (_, vm) in self.vm_list() {
            self.spawn_automount(vm);
        }
    }

    /// While `vm` is connected, mount its shares at `~/Mist/<vm>/<share>` (best-UX automount, design
    /// 11 §7). Polls — so it also covers reconnects — and the per-share mount is idempotent.
    fn spawn_automount(self: &Arc<Self>, vm: Arc<VmHandle>) {
        if !vm.automount {
            return;
        }
        let ctl = self.clone();
        let mount_root = ctl.mount_root.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                if !matches!(*vm.state.borrow(), VmState::Ready) {
                    continue;
                }
                for sname in vm.share_names() {
                    if ctl.mounts.is_mounted(&vm.name, &sname) {
                        continue;
                    }
                    let Some(sh) = vm.share(&sname) else { continue };
                    // Mount only once the share's root is seeded/mountable.
                    if sh.replica().getattr(sh.replica().root()).is_err() {
                        continue;
                    }
                    match ctl
                        .mounts
                        .mount(vm.clone(), &sname, &mount_root, false)
                        .await
                    {
                        Ok(mp) => tracing::info!(vm = %vm.name, share = %sname,
                            mountpoint = %mp.display(), "automounted"),
                        Err(e) => tracing::warn!(vm = %vm.name, share = %sname,
                            error = %e, "automount failed"),
                    }
                }
            }
        });
    }
}

pub async fn serve(path: std::path::PathBuf, ctl: Arc<Control>) -> anyhow::Result<()> {
    let _ = std::fs::remove_file(&path);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let listener = UnixListener::bind(&path)?;
    // Same-user only.
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    tracing::info!(path = %path.display(), "control socket ready");

    loop {
        let (stream, _) = listener.accept().await?;
        let ctl = ctl.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // `events --follow` switches the connection into a streaming feed.
                if let Ok(req) = serde_json::from_str::<Value>(&line)
                    && req["cmd"] == "events"
                    && req["follow"] == true
                {
                    stream_events(&ctl, &req, &mut w).await;
                    break;
                }
                let reply = handle(&ctl, &line).await;
                let mut text = reply.to_string();
                text.push('\n');
                if w.write_all(text.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Stream the live change feed (all VMs, optional path-prefix filter) as JSON lines until the
/// client disconnects.
async fn stream_events(ctl: &Control, req: &Value, w: &mut (impl tokio::io::AsyncWrite + Unpin)) {
    let prefix = req["path"].as_str().map(|s| s.to_string());
    // Subscribe to every VM's feed and merge.
    let mut subs: Vec<_> = ctl
        .vms
        .read()
        .values()
        .map(|vm| vm.events.subscribe())
        .collect();
    if subs.is_empty() {
        return;
    }
    let hello = json!({"ok": true, "following": true}).to_string() + "\n";
    if w.write_all(hello.as_bytes()).await.is_err() {
        return;
    }
    loop {
        // Poll all subscriptions; emit whichever yields first.
        let mut futs = Vec::new();
        for s in &mut subs {
            futs.push(Box::pin(s.recv()));
        }
        let (res, _idx, _rest) = futures_select_all(futs).await;
        match res {
            Ok(line) => {
                if let Some(p) = &prefix
                    && let Ok(v) = serde_json::from_str::<Value>(&line)
                    && !event_matches_prefix(&v, p)
                {
                    continue;
                }
                if w.write_all((line + "\n").as_bytes()).await.is_err() {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn event_matches_prefix(v: &Value, prefix: &str) -> bool {
    for key in ["path", "from", "to", "dir"] {
        if let Some(p) = v[key].as_str()
            && p.starts_with(prefix)
        {
            return true;
        }
    }
    false
}

/// Minimal select-all over a Vec of futures (avoids pulling in the `futures` crate for one use).
async fn futures_select_all<F: std::future::Future + Unpin>(
    mut futs: Vec<F>,
) -> (F::Output, usize, Vec<F>) {
    std::future::poll_fn(move |cx| {
        for (i, f) in futs.iter_mut().enumerate() {
            if let std::task::Poll::Ready(v) = std::pin::Pin::new(f).poll(cx) {
                let rest = std::mem::take(&mut futs);
                return std::task::Poll::Ready((v, i, rest));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

async fn handle(ctl: &Arc<Control>, line: &str) -> Value {
    let req: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return json!({"ok": false, "error": format!("bad json: {e}")}),
    };
    let cmd = req["cmd"].as_str().unwrap_or("");
    match cmd {
        "status" => status(ctl),
        "ls" | "stat" | "cat" => fs_op(ctl, cmd, &req).await,
        "mount" => mount(ctl, &req).await,
        "umount" => umount(ctl, &req).await,
        "conflicts" => conflicts(ctl),
        "cache" => cache(ctl, &req).await,
        "delegs" => delegs(ctl),
        "mounts" => mounts(ctl),
        // Onboarding verbs (design 11): discover guests, then add one with its token.
        "discover" => discover().await,
        "add" => add(ctl, &req).await,
        "events" => json!({"ok": true, "note": "pass follow:true to stream the change feed"}),
        "version" => json!({"ok": true, "version": env!("CARGO_PKG_VERSION")}),
        other => json!({"ok": false, "error": format!("unknown cmd {other:?}")}),
    }
}

/// `mist discover` — what the resolver can see right now: `_mist._tcp` instances (mDNS) and the
/// vmnet bridge(s) it would scan. No token required; identity only (design 11 §2).
async fn discover() -> Value {
    let instances = crate::resolve::mdns_browse(std::time::Duration::from_secs(3)).await;
    let models = crate::resolve::derive_vmnet();
    let mdns: Vec<Value> = instances
        .iter()
        .map(|i| {
            json!({
                "instance": i.instance,
                "host": i.host,
                "port": i.port,
                "vm_uuid": i.vm_uuid.map(|u| mist_proto::vm_uuid_hex(&u)),
            })
        })
        .collect();
    let vmnet: Vec<Value> = models
        .iter()
        .map(|m| {
            json!({
                "iface": m.iface,
                "gateway": m.gateway.to_string(),
                "netmask": m.netmask.to_string(),
            })
        })
        .collect();
    json!({"ok": true, "mdns": mdns, "vmnet": vmnet})
}

/// `mist add <name> --token <file> [--uuid <hex>]` — the whole onboarding, elegantly: you copy the
/// guest's token once (any way you like) and point Mist at it. Mist stores the token, autodiscovers
/// the guest that answers to it, binds its `vm_uuid`, writes `bridge="auto"` (no IP, ever), and
/// brings the VM live. No ssh, no sudo, no codes.
async fn add(ctl: &Arc<Control>, req: &Value) -> Value {
    use crate::config_writer::{VmBinding, write_binding};

    let name = req["name"].as_str().unwrap_or("").to_string();
    let token_src = req["token"].as_str().unwrap_or("");
    if name.is_empty() || token_src.is_empty() {
        return json!({"ok": false, "error": "add needs a name and a token file path"});
    }
    if name.contains(['/', '.', ' ']) {
        return json!({"ok": false, "error": "vm name must be a bare identifier (no / . or space)"});
    }
    let want_uuid = req["uuid"].as_str().and_then(mist_proto::vm_uuid_from_hex);

    // 1. Copy the token into the canonical @vms/<name>.token (0600). Idempotent if already there.
    let token_bytes = match std::fs::read(token_src) {
        Ok(b) if b.len() >= 32 => b,
        Ok(b) => {
            return json!({"ok": false, "error": format!("token {token_src} is {} bytes; needs ≥32", b.len())});
        }
        Err(e) => return json!({"ok": false, "error": format!("reading token {token_src}: {e}")}),
    };
    let dest = crate::config::state_dir()
        .join("vms")
        .join(format!("{name}.token"));
    if let Err(e) = write_token_file(&dest, &token_bytes) {
        return json!({"ok": false, "error": format!("writing {}: {e}", dest.display())});
    }
    let token_hash = *blake3::hash(&token_bytes).as_bytes();

    // 2. Best-effort autodiscovery: find the guest that authenticates with this token and read back
    // its vm_uuid. If unreachable right now, we still add the VM — the resolver binds on first
    // connect; bridge="auto" works token-only.
    let resolver = crate::resolve::Resolver::new();
    // Bound the discovery so `mist add` always returns promptly; on timeout we add the VM anyway
    // and the resolver binds identity on first connect.
    let (found_uuid, endpoint) = match tokio::time::timeout(
        std::time::Duration::from_secs(8),
        resolver.resolve(&name, want_uuid, token_hash),
    )
    .await
    {
        Ok(Ok(ep)) => {
            let uuid = crate::resolve::probe(&ep, token_hash).await.ok().flatten();
            (uuid, Some(ep.to_string()))
        }
        _ => (None, None),
    };
    if let (Some(want), Some(got)) = (want_uuid, found_uuid)
        && want != got
    {
        return json!({"ok": false, "error":
            "the guest answering to this token has a different vm_uuid than requested — wrong token?"});
    }
    let bound_uuid = want_uuid.or(found_uuid);

    // 3. Write [vm.<name>] bridge="auto" (+ vm_uuid when known) programmatically.
    let token_ref = format!("@vms/{name}.token");
    if let Some(uuid) = bound_uuid {
        if let Err(e) = write_binding(
            &ctl.config_path,
            &VmBinding {
                name: name.clone(),
                bridge: "auto".into(),
                vm_uuid: uuid,
                token: token_ref.clone(),
            },
        ) {
            return json!({"ok": false, "error": format!("writing config: {e}")});
        }
    } else if let Err(e) = write_binding_no_uuid(&ctl.config_path, &name, &token_ref) {
        return json!({"ok": false, "error": format!("writing config: {e}")});
    }

    // 4. Bring it live (resolver connects now, or whenever the guest appears). Added VMs automount
    // their shares at ~/Mist/<vm>/<share> — the best-UX default.
    let cfg = VmConfig {
        bridge: "auto".into(),
        token: std::path::PathBuf::from(&token_ref),
        vm_uuid: bound_uuid.map(|u| mist_proto::vm_uuid_hex(&u)),
        autoattach: true,
        automount: true,
    };
    match ctl.add_vm(&name, &cfg) {
        Ok(_) => json!({
            "ok": true, "vm": name,
            "vm_uuid": bound_uuid.map(|u| mist_proto::vm_uuid_hex(&u)),
            "endpoint": endpoint,
            "reachable": endpoint.is_some(),
        }),
        Err(e) => {
            json!({"ok": false, "error": format!("config written but live-attach failed: {e}")})
        }
    }
}

/// Write the token into `@vms/<name>.token` (0600). No-op rewrite if the bytes already match.
fn write_token_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)
}

/// Write a `bridge="auto"` block with no `vm_uuid` yet (guest wasn't reachable at add time; it
/// binds on first connect). Token-only `auto` is valid — the resolver accepts the guest the token
/// authenticates.
fn write_binding_no_uuid(
    config_path: &std::path::Path,
    name: &str,
    token: &str,
) -> anyhow::Result<()> {
    let existing = std::fs::read_to_string(config_path).unwrap_or_default();
    let updated = crate::config_writer::upsert_vm_no_uuid(&existing, name, token)?;
    if let Some(dir) = config_path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = config_path.with_extension("toml.tmp");
    std::fs::write(&tmp, updated)?;
    std::fs::rename(&tmp, config_path)?;
    Ok(())
}

/// Guest-vs-Mac collision log across all VMs (design 06: last-close-wins + visible log).
fn conflicts(ctl: &Control) -> Value {
    let mut rows = Vec::new();
    let mut total = 0u64;
    for (name, vm) in ctl.vms.read().iter() {
        total += vm.conflicts.total();
        for r in vm.conflicts.list() {
            let mut v = serde_json::to_value(&r).unwrap_or_else(|_| json!({}));
            v["vm"] = json!(name);
            rows.push(v);
        }
    }
    rows.sort_by_key(|v| v["at_unix_ms"].as_u64().unwrap_or(0));
    json!({"ok": true, "total": total, "conflicts": rows})
}

/// NFSv4.1 delegation/recall stats per v4.1 mount (`mist delegs`). The recall p99 is the
/// Recall-latency target (≤ 10 ms).
fn delegs(ctl: &Control) -> Value {
    let mounts: Vec<Value> = ctl
        .mounts
        .v41_servers()
        .into_iter()
        .map(|(vm, share, srv)| {
            let (granted, recalls, returned, revoked, held) = srv.deleg_snapshot();
            let m = &srv.recall_metrics;
            use std::sync::atomic::Ordering::Relaxed;
            json!({
                "vm": vm,
                "share": share,
                "granted": granted,
                "recalls": recalls,
                "returned": returned,
                "revoked": revoked,
                "held": held,
                "recall_p50_us": m.percentile_ns(0.50).map(|n| n / 1000),
                "recall_p99_us": m.percentile_ns(0.99).map(|n| n / 1000),
                "recall_max_us": m.max_recall_ns.load(Relaxed) / 1000,
                "recall_samples": m.returns.load(Relaxed),
                "ops_served": srv.ops_served.load(Relaxed),
            })
        })
        .collect();
    json!({"ok": true, "mounts": mounts})
}

/// Content-cache stats / clear / scrub (`mist cache [clear|scrub]`).
async fn cache(ctl: &Control, req: &Value) -> Value {
    let action = req["action"].as_str().unwrap_or("stats");
    let mut vms = Vec::new();
    for (name, vm) in ctl.vm_list() {
        let name = name.as_str();
        let Some(cas) = vm.cas.clone() else {
            vms.push(json!({"vm": name, "enabled": false}));
            continue;
        };
        let mut entry = json!({
            "vm": name,
            "enabled": true,
            "guest_read_ops": vm.guest_read_ops.load(Ordering::Relaxed),
            "guest_read_bytes": vm.guest_read_bytes.load(Ordering::Relaxed),
        });
        match action {
            "clear" => {
                let res = tokio::task::spawn_blocking(move || {
                    let r = cas.clear();
                    (r, cas.stats())
                })
                .await;
                match res {
                    Ok((Ok(()), stats)) => entry["stats"] = stats_json(&stats),
                    Ok((Err(e), _)) => entry["error"] = json!(e.to_string()),
                    Err(e) => entry["error"] = json!(e.to_string()),
                }
            }
            "scrub" => {
                let sample = req["sample"].as_u64().unwrap_or(0) as usize;
                let res =
                    tokio::task::spawn_blocking(move || (cas.scrub(sample), cas.stats())).await;
                match res {
                    Ok((Ok(rep), stats)) => {
                        entry["scrub"] = json!({
                            "checked": rep.checked,
                            "corrupt": rep.corrupt,
                            "missing": rep.missing,
                        });
                        entry["stats"] = stats_json(&stats);
                    }
                    Ok((Err(e), _)) => entry["error"] = json!(e.to_string()),
                    Err(e) => entry["error"] = json!(e.to_string()),
                }
            }
            _ => entry["stats"] = stats_json(&cas.stats()),
        }
        vms.push(entry);
    }
    json!({"ok": true, "vms": vms})
}

fn stats_json(s: &mist_cas::CasStats) -> Value {
    json!({
        "hits": s.hits,
        "misses": s.misses,
        "blobs": s.blobs,
        "total_bytes": s.total_bytes,
        "max_bytes": s.max_bytes,
        "ingested_bytes": s.ingested_bytes,
        "evictions": s.evictions,
        "evicted_bytes": s.evicted_bytes,
        "corrupt_dropped": s.corrupt_dropped,
    })
}

async fn mount(ctl: &Control, req: &Value) -> Value {
    let vm_name = req["vm"].as_str().unwrap_or("");
    let share = req["share"].as_str().unwrap_or("");
    let Some(vm) = ctl.vm(vm_name) else {
        let known: Vec<String> = ctl.vms.read().keys().cloned().collect();
        return json!({"ok": false, "error": format!("unknown vm {vm_name:?} (configured: {})",
            if known.is_empty() { "none".into() } else { known.join(", ") })});
    };
    let nfs41 = req["nfs41"].as_bool().unwrap_or(false);
    // A just-added VM may still be connecting; its shares are announced on HelloAck a beat after
    // `add` returns, so an immediate `mount` can race them (the add→mount race). Wait for the named
    // share to appear — but only until the VM is Ready, after which an absent share is genuinely
    // unknown and we fail fast instead of stalling on a typo.
    {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while vm.share(share).is_none()
            && !matches!(*vm.state.borrow(), VmState::Ready)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
    }
    // Then wait for the share to seed: a fresh replica has no root node, so mounting it before the
    // first seed completes fails with a confusing ENOENT. Poll until the root is mountable (returns
    // immediately for already-seeded shares, and for a reseed where the old tree is still served);
    // a very large tree (millions of entries) can take a while, so the bound is generous.
    if let Some(sh) = vm.share(share) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        while sh.replica().getattr(sh.replica().root()).is_err()
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        if sh.replica().getattr(sh.replica().root()).is_err() {
            let n = sh.replica().stats().nodes;
            return json!({"ok": false, "error": format!(
                "share {share:?} is still seeding ({n} nodes so far) — run `mist status` and retry \
                 `mist mount` once it shows [live]")});
        }
    }
    match ctl
        .mounts
        .mount(vm.clone(), share, &ctl.mount_root, nfs41)
        .await
    {
        Ok(mp) => json!({"ok": true, "mountpoint": mp.to_string_lossy()}),
        Err(e) => json!({"ok": false, "error": e.to_string()}),
    }
}

async fn umount(ctl: &Control, req: &Value) -> Value {
    let vm = req["vm"].as_str().unwrap_or("");
    let share = req["share"].as_str().unwrap_or("");
    match ctl.mounts.umount(vm, share).await {
        Ok(()) => json!({"ok": true}),
        Err(e) => json!({"ok": false, "error": e.to_string()}),
    }
}

fn mounts(ctl: &Control) -> Value {
    let list: Vec<Value> = ctl
        .mounts
        .list()
        .into_iter()
        .map(|(vm, share, port, mp)| {
            json!({"vm": vm, "share": share, "port": port, "mountpoint": mp.to_string_lossy()})
        })
        .collect();
    json!({"ok": true, "mounts": list})
}

fn status(ctl: &Control) -> Value {
    let vms: Vec<Value> = ctl
        .vm_list()
        .iter()
        .map(|(name, vm)| {
            let state = match *vm.state.borrow() {
                VmState::Disconnected => "disconnected",
                VmState::Connecting => "connecting",
                VmState::Ready => "ready",
                VmState::Degraded => "degraded",
            };
            let shares: Vec<Value> = vm
                .shares
                .read()
                .iter()
                .map(|(sname, sh)| {
                    let r = sh.replica();
                    let stats = r.stats();
                    json!({
                        "name": sname,
                        "state": match r.state() {
                            ShareState::Seeding => "seeding",
                            ShareState::Live => "live",
                            ShareState::Degraded => "degraded",
                            ShareState::Offline => "offline",
                        },
                        "nodes": stats.nodes,
                        "dirs": stats.dirs,
                        "entries": stats.entries,
                        "epoch": sh.info.read().epoch,
                    })
                })
                .collect();
            json!({
                "name": name,
                "state": state,
                "vm_uuid": vm.vm_uuid_hex(),
                "endpoint": vm.endpoint_display(),
                "seed_rate_per_s": vm.last_seed_rate.load(Ordering::Relaxed),
                "journal_records": vm.journal_records.load(Ordering::Relaxed),
                "resyncs": vm.overflows.load(Ordering::Relaxed),
                "shares": shares,
            })
        })
        .collect();
    let mounts: Vec<Value> = ctl
        .mounts
        .list()
        .into_iter()
        .map(|(vm, share, port, mp)| {
            json!({"vm": vm, "share": share, "port": port, "mountpoint": mp.to_string_lossy()})
        })
        .collect();
    json!({"ok": true, "vms": vms, "mounts": mounts})
}

async fn fs_op(ctl: &Control, cmd: &str, req: &Value) -> Value {
    let vm_name = req["vm"].as_str().unwrap_or("");
    let share_name = req["share"].as_str().unwrap_or("");
    let path = req["path"].as_str().unwrap_or("/");

    let Some(vm) = ctl.vm(vm_name) else {
        return json!({"ok": false, "error": format!("unknown vm {vm_name:?}")});
    };
    let Some(share) = vm.share(share_name) else {
        return json!({"ok": false, "error": format!("unknown share {share_name:?}")});
    };
    let replica = share.replica();
    if replica.state() != ShareState::Live {
        // Bounded wait for first seed (covers the just-started case).
        let waited =
            tokio::time::timeout(std::time::Duration::from_secs(15), share.seeded.notified())
                .await
                .is_ok();
        if !waited && share.replica().state() != ShareState::Live {
            return json!({"ok": false, "error": "share not seeded yet"});
        }
    }
    let replica = share.replica();

    let (node, attr) = match replica.resolve_path(path) {
        Ok(x) => x,
        Err(e) => return json!({"ok": false, "error": format!("{path}: {e}")}),
    };

    match cmd {
        "stat" => {
            json!({"ok": true, "path": path, "attr": attr_json(&attr), "ino": node.ino, "generation": node.generation})
        }
        "ls" => {
            if attr.kind != Kind::Dir {
                return json!({"ok": true, "entries": [ {"name": path.rsplit('/').next().unwrap_or(path), "attr": attr_json(&attr)} ]});
            }
            let mut entries = Vec::new();
            let mut cookie = 0u64;
            loop {
                match replica.readdir(node, cookie, 4096) {
                    Ok(page) => {
                        for e in &page.entries {
                            let a = replica.getattr(e.node).ok();
                            entries.push(json!({
                                "name": String::from_utf8_lossy(e.name.as_bytes()),
                                "kind": kind_str(e.kind),
                                "size": a.as_ref().map(|a| a.size),
                                "mode": a.as_ref().map(|a| a.mode),
                                "mtime": a.as_ref().map(|a| a.mtime.sec),
                            }));
                            if entries.len() >= LS_LIMIT {
                                return json!({"ok": true, "truncated": true, "entries": entries});
                            }
                        }
                        if page.eof {
                            break;
                        }
                        cookie = page.entries.last().map(|e| e.cookie).unwrap_or(cookie);
                    }
                    Err(e) => return json!({"ok": false, "error": e.to_string()}),
                }
            }
            json!({"ok": true, "entries": entries})
        }
        "cat" => {
            if attr.kind != Kind::Reg {
                return json!({"ok": false, "error": "not a regular file"});
            }
            let Some(rpc) = vm.rpc() else {
                return json!({"ok": false, "error": "vm not connected"});
            };
            let want = (attr.size.min(MAX_CAT as u64)) as u32;
            // Chunk at the protocol cap: a Read RPC larger than MAX_READ_LEN fails guest-side
            // validation and (worse) used to kill the rpc lane.
            let mut bytes = Vec::with_capacity(want as usize);
            let mut err = None;
            while (bytes.len() as u32) < want.max(1) {
                let n = (want.max(1) - bytes.len() as u32).min(mist_proto::caps::MAX_READ_LEN);
                match rpc
                    .read(replica.share_id(), node, bytes.len() as u64, n)
                    .await
                {
                    Ok((chunk, eof)) => {
                        let done = chunk.is_empty() || eof;
                        bytes.extend_from_slice(&chunk);
                        if done {
                            break;
                        }
                    }
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            match err {
                None => {
                    let _eof = ();
                    json!({
                        "ok": true,
                        "size": attr.size,
                        "returned": bytes.len(),
                        "truncated": attr.size > MAX_CAT as u64,
                        "data_b64": b64(&bytes),
                    })
                }
                Some(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }
        _ => unreachable!(),
    }
}

fn attr_json(a: &mist_proto::Attr) -> Value {
    json!({
        "kind": kind_str(a.kind),
        "mode": a.mode,
        "nlink": a.nlink,
        "uid": a.uid,
        "gid": a.gid,
        "size": a.size,
        "mtime": a.mtime.sec,
        "ctime": a.ctime.sec,
        "symlink": a.symlink_target.as_ref().map(|t| String::from_utf8_lossy(t).into_owned()),
    })
}

fn kind_str(k: Kind) -> &'static str {
    match k {
        Kind::Reg => "file",
        Kind::Dir => "dir",
        Kind::Symlink => "symlink",
        Kind::Fifo => "fifo",
        Kind::Sock => "sock",
        Kind::Chr => "chr",
        Kind::Blk => "blk",
    }
}

/// Minimal base64 (std-only; avoids a dependency for one call site).
fn b64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}
