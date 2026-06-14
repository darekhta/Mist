//! Linux implementation of the guest daemon.

pub mod applier;
pub mod fanotify;
pub mod handles;
pub mod identity;
pub mod journal;
pub mod rpc;
pub mod server;
pub mod shares;
pub mod walker;

use crate::config::Config;
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "mistd", about = "Mist guest daemon")]
struct Args {
    /// Config file path.
    #[arg(long, default_value = crate::config::DEFAULT_CONFIG)]
    config: PathBuf,
    /// Extra share, repeatable: name=/abs/path (overrides/extends config).
    #[arg(long = "share", value_name = "NAME=PATH")]
    shares: Vec<String>,
    /// Extra listen endpoint, repeatable: vsock:6478 | tcp:0.0.0.0:6478 (replaces config when given).
    #[arg(long = "listen", value_name = "EP")]
    listen: Vec<String>,
    /// Token file override.
    #[arg(long)]
    token_file: Option<PathBuf>,
    /// Load + validate the config (and CLI overrides), print a summary, and exit without
    /// serving. Used by packaging CI to catch a malformed `/etc/mist/mistd.toml`.
    #[arg(long = "check-config")]
    check_config: bool,
}

pub fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut cfg = if args.config.exists() {
        Config::load(&args.config)?
    } else if args.shares.is_empty() {
        anyhow::bail!(
            "no config at {} and no --share given; refusing to start with nothing to serve",
            args.config.display()
        );
    } else {
        // CLI-only operation (e2e/bench convenience).
        toml::from_str("").expect("empty config is valid")
    };

    for s in &args.shares {
        let (name, rest) = s
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--share must be NAME=PATH[,writeback], got {s:?}"))?;
        let (path, commit) = match rest.split_once(',') {
            Some((p, "writeback")) => (p, crate::config::CommitPolicy::Writeback),
            Some((p, "fsync")) => (p, crate::config::CommitPolicy::Fsync),
            Some((_, other)) => anyhow::bail!("unknown share option {other:?} in {s:?}"),
            None => (rest, crate::config::CommitPolicy::Fsync),
        };
        cfg.share.insert(
            name.to_string(),
            crate::config::ShareConfig {
                path: path.into(),
                readonly: false,
                apply_uid: None,
                apply_gid: None,
                commit,
            },
        );
    }
    if !args.listen.is_empty() {
        cfg.listen = args.listen.clone();
    }
    if let Some(t) = args.token_file {
        cfg.token_file = t;
    }

    if args.check_config {
        println!(
            "config ok: {} listener(s), {} share(s), token_file {}, vmid_file {}",
            cfg.listen.len(),
            cfg.share.len(),
            cfg.token_file.display(),
            cfg.vmid_file.display(),
        );
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cfg.log.clone().unwrap_or_else(|| "info".into()).into()),
        )
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(server::serve(cfg))
}
