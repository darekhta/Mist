//! mist-hostd — the Mist host daemon.

use clap::Parser;
use mist_hostd::{config, control, session};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(name = "mist-hostd", about = "Mist host daemon")]
struct Args {
    /// Config file (default: ~/Library/Application Support/Mist/config.toml or $MIST_STATE_DIR).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Ad-hoc VM attach, repeatable: name=endpoint,token=/path (bench/e2e convenience).
    #[arg(long = "vm", value_name = "NAME=EP,token=PATH")]
    vms: Vec<String>,
    /// Override the content-cache high watermark in bytes (0 disables; default 20 GiB).
    #[arg(long)]
    cache_max_bytes: Option<u64>,
}

fn main() -> anyhow::Result<()> {
    // Darwin demotes an idle daemon's threads to E-cores / lower clocks under default QoS: the
    // first dd after a restart streamed ~1.5 GB/s and settled to ~1.0 once the scheduler cooled
    // the process. Pin every runtime worker to USER_INTERACTIVE so the serving path stays on
    // P-cores — knfsd never pays this tax because kernel threads aren't QoS-managed.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(mist_hostd::pin_thread_user_interactive)
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let cfg_path = args.config.unwrap_or_else(config::default_config_path);
    let mut cfg = if cfg_path.exists() {
        config::Config::load(&cfg_path)?
    } else {
        config::Config::empty()
    };

    for spec in &args.vms {
        let (name, rest) = spec
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--vm must be NAME=EP,token=PATH, got {spec:?}"))?;
        let (ep, token) = match rest.split_once(",token=") {
            Some((ep, token)) => (ep.to_string(), PathBuf::from(token)),
            None => anyhow::bail!("--vm missing ,token=PATH in {spec:?}"),
        };
        cfg.vm.insert(
            name.to_string(),
            config::VmConfig {
                bridge: ep,
                token,
                vm_uuid: None,
                autoattach: true,
                automount: false,
            },
        );
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                cfg.daemon
                    .log
                    .clone()
                    .unwrap_or_else(|| "info".into())
                    .into()
            }),
        )
        .init();

    if let Some(b) = args.cache_max_bytes {
        cfg.daemon.cache_max_bytes = b;
    }

    if cfg.vm.is_empty() {
        tracing::warn!(
            "no VMs configured (config: {}); control API will be empty",
            cfg_path.display()
        );
    }

    let mut vms: BTreeMap<String, Arc<session::VmHandle>> = BTreeMap::new();
    for (name, vc) in &cfg.vm {
        // One CAS per VM (share ids are per-VM, so manifests can't be pooled across VMs).
        let cas_cfg = (cfg.daemon.cache_max_bytes > 0).then(|| {
            mist_cas::CasConfig::new(
                config::state_dir().join("cas").join(name),
                cfg.daemon.cache_max_bytes,
            )
        });
        let vm = session::VmHandle::new(name.clone(), vc, cas_cfg)?;
        if vc.autoattach {
            vm.spawn_supervise();
        }
        vms.insert(name.clone(), vm);
    }

    let control_path = if cfg.daemon.control_socket.is_absolute() {
        cfg.daemon.control_socket.clone()
    } else {
        config::state_dir().join(&cfg.daemon.control_socket)
    };

    let handle_secret = load_or_create_handle_secret()?;
    let mounts = Arc::new(mist_hostd::mount::MountManager::new(
        handle_secret,
        config::state_dir(),
    ));
    let mount_root = config::mount_root();
    // Crash recovery: adopt kernel mounts left by a previous hostd (rebind same ports).
    mounts.restore(vms.clone(), mount_root.clone());
    let ctl = Arc::new(control::Control {
        vms: parking_lot::RwLock::new(vms),
        mounts: mounts.clone(),
        mount_root,
        config_path: cfg_path.clone(),
        cache_max_bytes: cfg.daemon.cache_max_bytes,
    });
    // Auto-mount shares for VMs that opted in (best-UX default for `mist add`).
    ctl.start_automount();

    tokio::select! {
        r = control::serve(control_path, ctl) => r,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down; unmounting");
            mounts.umount_all().await;
            Ok(())
        }
    }
}

/// NFS handles are bearer tokens; the per-run secret keys their MAC. Persisted so handles survive
/// a hostd restart within a machine's lifetime (the NFS client holds them across our restarts).
fn load_or_create_handle_secret() -> anyhow::Result<Vec<u8>> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let path = config::state_dir().join("handle.secret");
    if let Ok(b) = std::fs::read(&path)
        && b.len() >= 32
    {
        return Ok(b);
    }
    std::fs::create_dir_all(config::state_dir())?;
    let secret: [u8; 32] = rand::random();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)?;
    f.write_all(&secret)?;
    Ok(secret.to_vec())
}
