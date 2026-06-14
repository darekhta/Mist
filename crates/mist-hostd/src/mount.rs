//! Mount manager: start a loopback NFS server per (vm, share) and drive `mount_nfs` to attach it
//! under the Mist state directory. Tracks active read-write mounts for `umount` and status.

use crate::session::VmHandle;
use crate::sidestore::{SideStore, SideStoreSurface};
use crate::surface::ShareSurface;
use mist_nfs::{Nfs41Server, NfsServer};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// The concrete v4.1 server type a mount holds (surface stack is fixed in `mount`).
pub type V41Handle = Arc<Nfs41Server<SideStoreSurface<ShareSurface>>>;

/// Run an NFS serve loop on its OWN thread with a current-thread runtime: socket readiness,
/// request handling, and reply writes all stay on one QoS-pinned thread, so per-op wakeups
/// never cross runtime workers. Returns a task whose `abort()` stops the loop (the umount
/// contract), via a drop-guard that signals the dedicated thread to exit.
fn spawn_dedicated_server<F, Fut>(
    label: &'static str,
    listener: TcpListener,
    serve: F,
) -> JoinHandle<()>
where
    F: FnOnce(TcpListener) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = std::io::Result<()>>,
{
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let std_listener = listener.into_std();
    let spawned = std::thread::Builder::new()
        .name(format!("mist-{label}"))
        .spawn(move || {
            crate::pin_thread_user_interactive();
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!(error = %e, label, "nfs runtime build failed");
                    return;
                }
            };
            rt.block_on(async move {
                // Re-register the listener with THIS runtime's reactor (it was bound on the
                // main one) so accepts don't cross-wake either.
                let listener = match std_listener.and_then(TcpListener::from_std) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!(error = %e, label, "nfs listener rebind failed");
                        return;
                    }
                };
                tokio::select! {
                    r = serve(listener) => {
                        if let Err(e) = r {
                            tracing::error!(error = %e, label, "nfs server ended");
                        }
                    }
                    _ = stop_rx => {}
                }
            });
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, label, "nfs server thread spawn failed");
    }
    tokio::spawn(async move {
        /// Fires the stop signal when this task is aborted (or dropped at shutdown).
        struct StopOnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for StopOnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let _stop = StopOnDrop(Some(stop_tx));
        std::future::pending::<()>().await;
    })
}

#[derive(Debug)]
pub struct ActiveMount {
    pub vm: String,
    pub share: String,
    pub port: u16,
    pub mountpoint: PathBuf,
    pub nfs41: bool,
    server_task: JoinHandle<()>,
    /// Present when the share is served over NFSv4.1 (`mist mount --nfs41`).
    pub v41: Option<Arc<Nfs41Server<SideStoreSurface<ShareSurface>>>>,
}

#[derive(Default)]
pub struct MountManager {
    mounts: Mutex<HashMap<(String, String), ActiveMount>>,
    /// Persisted NFS handle secret (handles survive hostd restarts within a run).
    handle_secret: Vec<u8>,
    /// State dir for per-share side-store persistence.
    state_dir: PathBuf,
}

impl std::fmt::Debug for MountManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MountManager")
            .field("mounts", &self.mounts.lock().len())
            .finish()
    }
}

impl MountManager {
    pub fn new(handle_secret: Vec<u8>, state_dir: PathBuf) -> Self {
        MountManager {
            mounts: Mutex::new(HashMap::new()),
            handle_secret,
            state_dir,
        }
    }

    pub fn list(&self) -> Vec<(String, String, u16, PathBuf)> {
        self.mounts
            .lock()
            .values()
            .map(|m| (m.vm.clone(), m.share.clone(), m.port, m.mountpoint.clone()))
            .collect()
    }

    /// Live NFSv4.1 servers (for `mist delegs`): (vm, share, server).
    pub fn v41_servers(&self) -> Vec<(String, String, V41Handle)> {
        self.mounts
            .lock()
            .values()
            .filter_map(|m| m.v41.clone().map(|s| (m.vm.clone(), m.share.clone(), s)))
            .collect()
    }

    pub fn is_mounted(&self, vm: &str, share: &str) -> bool {
        self.mounts
            .lock()
            .contains_key(&(vm.to_string(), share.to_string()))
    }

    /// Start an NFS server for the share and mount it. Idempotent: a second call returns the
    /// existing mountpoint. `nfs41` serves NFSv4.1 with read delegations (design 05 §5 opt-in).
    pub async fn mount(
        &self,
        vm: Arc<VmHandle>,
        share_name: &str,
        mount_root: &std::path::Path,
        nfs41: bool,
    ) -> anyhow::Result<PathBuf> {
        let key = (vm.name.clone(), share_name.to_string());
        if let Some(m) = self.mounts.lock().get(&key) {
            return Ok(m.mountpoint.clone());
        }

        let share = vm.share(share_name).ok_or_else(|| {
            anyhow::anyhow!(
                "unknown share {share_name:?} on vm {} (announced: {})",
                vm.name,
                vm.share_names().join(", ")
            )
        })?;

        // Bind an ephemeral loopback port; serve both NFS (100003) and MOUNT (100005) on it.
        // MIST_BIND_IP overrides the address (e.g. the vmnet bridge IP — the macOS client's
        // single-stream read cadence differs measurably by interface).
        let bind_ip = std::env::var("MIST_BIND_IP").unwrap_or_else(|_| "127.0.0.1".into());
        let listener = TcpListener::bind(format!("{bind_ip}:0")).await?;
        let port = listener.local_addr()?.port();
        self.serve_share(
            vm, share_name, share, listener, nfs41, port, mount_root, true,
        )
        .await
    }

    /// Inner mount step: start the NFS server on `listener` and (unless restoring) drive
    /// mount_nfs. Split out so crash-restore can rebind the SAME port without re-mounting —
    /// the kernel client's existing mount reconnects to it (handles survive via the persisted
    /// HMAC secret; design 06 §7 hostd-crash row).
    #[allow(clippy::too_many_arguments)]
    async fn serve_share(
        &self,
        vm: Arc<VmHandle>,
        share_name: &str,
        share: Arc<crate::session::ShareHandle>,
        listener: TcpListener,
        nfs41: bool,
        port: u16,
        mount_root: &std::path::Path,
        run_mount_nfs: bool,
    ) -> anyhow::Result<PathBuf> {
        let key = (vm.name.clone(), share_name.to_string());
        let bind_ip = std::env::var("MIST_BIND_IP").unwrap_or_else(|_| "127.0.0.1".into());
        let share_id = share.info.read().id.0;
        let surface = Arc::new(ShareSurface::new(vm.clone(), share));
        // Side-store decorator: Apple metadata (.DS_Store, ._*) stays host-side, never the guest.
        let store = SideStore::open(Some(
            self.state_dir
                .join(format!("sidestore-{}-{share_name}.bin", vm.name)),
        ));
        let surface = Arc::new(SideStoreSurface::new(surface, store));
        // Debounced side-store persistence: flush ≤1×/500 ms while the mount lives (the store's
        // Drop does the final flush when the surface goes away).
        {
            let weak = Arc::downgrade(surface.store());
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
                loop {
                    tick.tick().await;
                    match weak.upgrade() {
                        Some(store) => store.flush_if_dirty(),
                        None => return,
                    }
                }
            });
        }
        let mut v41 = None;
        let server_task = if nfs41 {
            let server = Arc::new(Nfs41Server::new(surface, &self.handle_secret));
            v41 = Some(server.clone());
            // Journal-driven recall: guest invalidations on this share recall delegations.
            {
                let server = server.clone();
                let mut rx = vm.inval.subscribe();
                tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok((sh, node)) if sh == share_id => server.recall_node(node),
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(_) => return,
                        }
                    }
                });
            }
            spawn_dedicated_server("nfs41", listener, move |l| server.serve(l))
        } else {
            let server = Arc::new(NfsServer::new(surface, &self.handle_secret));
            spawn_dedicated_server("nfs3", listener, move |l| server.serve(l))
        };

        let mountpoint = mount_root.join(&vm.name).join(share_name);
        std::fs::create_dir_all(&mountpoint)?;

        // mount_nfs to the loopback server. Read-write: the surface accepts mutations.
        // v4.1 has no MOUNT protocol: the path is resolved from PUTROOTFH (we export the share
        // root as the v4 root), so the spec path is "/".
        // rsize tunes the client's read pipeline shape: fewer/larger RPCs vs more/smaller ones
        // in flight (XNU readahead issues per-buffer). 1 MiB measured best by default; the env
        // override exists for experiments (the client hard-caps at 1 MiB; 2 MiB → 32 KiB).
        let rsize: u32 = std::env::var("MIST_RSIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_048_576);
        let (mount_spec, opts) = if nfs41 {
            (
                // nolocks/locallocks are v3-only options: locking is integral to v4 and
                // mount_nfs rejects them with EINVAL (probe finding). The server grants all
                // LOCK ops (single loopback client — v3 `locallocks` parity).
                format!("{bind_ip}:/"),
                format!(
                    "vers=4.1,tcp,port={port},rw,readahead=128,\
                     rsize={rsize},wsize=1048576,hard,intr,noatime,nosuid,nodev,actimeo=5"
                ),
            )
        } else {
            (
                format!("{bind_ip}:/{share_name}"),
                format!(
                    "vers=3,tcp,port={port},mountport={port},rw,nolocks,locallocks,rdirplus,readahead=128,\
                     rsize={rsize},wsize=1048576,hard,intr,noatime,nosuid,nodev,actimeo=5"
                ),
            )
        };
        if run_mount_nfs {
            let out = tokio::process::Command::new("/sbin/mount_nfs")
                .arg("-o")
                .arg(&opts)
                .arg(&mount_spec)
                .arg(&mountpoint)
                .output()
                .await?;
            if !out.status.success() {
                server_task.abort();
                anyhow::bail!(
                    "mount_nfs failed: {} (unprivileged mounts need a user-owned mountpoint; \
                     see docs/mounting.md for the privileged fallback)",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }

            // Best-effort: keep Spotlight from indexing the mount.
            let _ = tokio::process::Command::new("/usr/bin/mdutil")
                .args(["-i", "off"])
                .arg(&mountpoint)
                .output()
                .await;
        }

        tracing::info!(vm = %vm.name, share = %share_name, port, nfs41, restored = !run_mount_nfs, mountpoint = %mountpoint.display(), "mounted");
        self.mounts.lock().insert(
            key,
            ActiveMount {
                vm: vm.name.clone(),
                share: share_name.to_string(),
                port,
                mountpoint: mountpoint.clone(),
                nfs41,
                server_task,
                v41,
            },
        );
        self.persist_registry();
        Ok(mountpoint)
    }

    /// Registry of live mounts (vm, share, port, nfs41, mountpoint) — survives a hostd crash
    /// so restart can rebind the same ports and adopt the kernel's still-present mounts.
    fn registry_path(&self) -> PathBuf {
        self.state_dir.join("mounts.json")
    }

    fn persist_registry(&self) {
        let rows: Vec<serde_json::Value> = self
            .mounts
            .lock()
            .values()
            .map(|m| {
                serde_json::json!({
                    "vm": m.vm, "share": m.share, "port": m.port, "nfs41": m.nfs41,
                    "mountpoint": m.mountpoint.to_string_lossy(),
                })
            })
            .collect();
        if let Err(e) = std::fs::write(
            self.registry_path(),
            serde_json::to_vec(&rows).unwrap_or_default(),
        ) {
            tracing::warn!(error = %e, "mount registry persist failed");
        }
    }

    /// Crash recovery (design 06 §7): adopt kernel mounts left over from a previous hostd.
    /// For each registry entry whose mountpoint the kernel still serves, rebind the SAME
    /// loopback port once the share reappears and start a fresh server — the macOS client's
    /// hard mount reconnects and resumes (handles verify against the persisted secret).
    pub fn restore(
        self: &Arc<Self>,
        vms: std::collections::BTreeMap<String, Arc<VmHandle>>,
        mount_root: PathBuf,
    ) {
        let Ok(bytes) = std::fs::read(self.registry_path()) else {
            return;
        };
        let Ok(rows) = serde_json::from_slice::<Vec<serde_json::Value>>(&bytes) else {
            return;
        };
        let mounted = std::process::Command::new("/sbin/mount")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        for row in rows {
            let (vm_name, share, mp) = (
                row["vm"].as_str().unwrap_or("").to_string(),
                row["share"].as_str().unwrap_or("").to_string(),
                row["mountpoint"].as_str().unwrap_or("").to_string(),
            );
            let port = row["port"].as_u64().unwrap_or(0) as u16;
            let nfs41 = row["nfs41"].as_bool().unwrap_or(false);
            let still_mounted = mounted.lines().any(|l| l.contains(&format!(" on {mp} ")));
            let Some(vm) = vms.get(&vm_name).cloned() else {
                continue;
            };
            if !still_mounted || port == 0 {
                continue;
            }
            let me = self.clone();
            let root = mount_root.clone();
            tokio::spawn(async move {
                // The share is announced by the guest after the session re-establishes.
                for _ in 0..240 {
                    if let Some(sh) = vm.share(&share) {
                        if nfs41 {
                            // v4.1 sessions don't survive adoption: the macOS client clears its
                            // reboot recovery (EXCHANGE_ID → … → RECLAIM_COMPLETE, stale
                            // stateids answered 10023) but then NEVER issues another OPEN —
                            // stateful ops queue forever while stateless ones flow (measured;
                            // no self-heal after 8+ min). Force a fresh kernel mount instead:
                            // open handles on the old mount get EIO once, everything after
                            // works. v3 is stateless and adopts seamlessly below.
                            let _ = tokio::process::Command::new("/sbin/umount")
                                .arg("-f")
                                .arg(&mp)
                                .output()
                                .await;
                            // Forced NFS unmounts tear down asynchronously; retry the fresh
                            // mount until the kernel lets go of the mountpoint.
                            for attempt in 0..10 {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                let _ = std::fs::create_dir_all(&mp);
                                match me.mount(vm.clone(), &share, &root, true).await {
                                    Ok(_) => {
                                        tracing::info!(vm = %vm.name, share = %share, attempt, "restored v4.1 mount (forced remount)");
                                        return;
                                    }
                                    Err(e) if attempt == 9 => {
                                        tracing::warn!(vm = %vm.name, share = %share, error = %e, "v4.1 restore remount failed");
                                    }
                                    Err(_) => {}
                                }
                            }
                            return;
                        }
                        let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{port}")).await
                        else {
                            tracing::warn!(vm = %vm.name, share = %share, port, "restore: port taken; mount needs manual remount");
                            return;
                        };
                        match me
                            .serve_share(
                                vm.clone(),
                                &share,
                                sh,
                                listener,
                                nfs41,
                                port,
                                &root,
                                false,
                            )
                            .await
                        {
                            Ok(_) => {
                                tracing::info!(vm = %vm.name, share = %share, port, "restored mount server")
                            }
                            Err(e) => {
                                tracing::warn!(vm = %vm.name, share = %share, error = %e, "restore failed")
                            }
                        }
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                tracing::warn!(vm = %vm.name, share = %share, "restore: share never reappeared");
            });
        }
    }

    pub async fn umount(&self, vm: &str, share: &str) -> anyhow::Result<()> {
        let m = self
            .mounts
            .lock()
            .remove(&(vm.to_string(), share.to_string()))
            .ok_or_else(|| anyhow::anyhow!("{vm}/{share} is not mounted"))?;
        let out = tokio::process::Command::new("/sbin/umount")
            .arg(&m.mountpoint)
            .output()
            .await?;
        m.server_task.abort();
        if !out.status.success() {
            // Force-unmount if the graceful one failed (e.g. busy).
            let _ = tokio::process::Command::new("/sbin/umount")
                .arg("-f")
                .arg(&m.mountpoint)
                .output()
                .await;
        }
        tracing::info!(vm = %vm, share = %share, "unmounted");
        self.persist_registry();
        Ok(())
    }

    /// Unmount everything (shutdown).
    pub async fn umount_all(&self) {
        let keys: Vec<(String, String)> = self.mounts.lock().keys().cloned().collect();
        for (vm, share) in keys {
            let _ = self.umount(&vm, &share).await;
        }
    }
}
