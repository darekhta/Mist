//! mist — control CLI for mist-hostd.

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::io::Write;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Debug, Parser)]
#[command(
    name = "mist",
    about = "Mist — near-native access to Linux VM files",
    version
)]
struct Args {
    /// Control socket (default: $MIST_STATE_DIR or ~/Library/Application Support/Mist + control.sock).
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Daemon, VM and share status.
    Status {
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// List a directory in a share.
    Ls {
        vm: String,
        share: String,
        #[arg(default_value = "/")]
        path: String,
    },
    /// Stat a path in a share.
    Stat {
        vm: String,
        share: String,
        path: String,
    },
    /// Print a file's contents (debug; capped at 10 MiB).
    Cat {
        vm: String,
        share: String,
        path: String,
    },
    /// Mount a share read-write via the loopback NFS server.
    Mount {
        vm: String,
        share: String,
        /// Serve NFSv4.1 with read delegations instead of NFSv3.
        #[arg(long)]
        nfs41: bool,
    },
    /// Unmount a share.
    Umount { vm: String, share: String },
    /// Stream the live change feed (the journal as JSON lines — better than FSEvents).
    Events {
        /// Follow the feed continuously (otherwise prints a note and exits).
        #[arg(long)]
        follow: bool,
        /// Only show events whose path starts with this prefix.
        #[arg(long)]
        path: Option<String>,
    },
    /// Show detected guest-vs-Mac write collisions (last-close-wins is applied; this makes it visible).
    Conflicts {
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// NFSv4.1 delegation + recall-latency stats.
    Delegs {
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Content-cache stats, clear, or scrub.
    Cache {
        /// Action: stats (default), clear, scrub.
        #[arg(default_value = "stats")]
        action: String,
        /// Scrub sample size (0 = all blobs).
        #[arg(long, default_value_t = 0)]
        sample: u64,
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Health + security checkup: daemon, config, token/secret hygiene, mounts, sessions.
    Doctor {
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Show discoverable guests: `_mist._tcp` mDNS instances + the vmnet bridge(s) we'd scan.
    Discover {
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Add a VM: copy its token once, and Mist autodiscovers it, binds `bridge="auto"`, mounts.
    Add {
        /// VM name (the `[vm.<name>]` block + `~/Mist/<name>` mount root).
        vm: String,
        /// Path to the guest's token file (the 32 bytes from the guest's `/etc/mist/token`).
        #[arg(long)]
        token: PathBuf,
        /// Optionally pin the expected `vm_uuid` (32 hex chars); otherwise it's autodiscovered.
        #[arg(long)]
        uuid: Option<String>,
    },
    /// Daemon version.
    Version,
}

fn default_socket() -> PathBuf {
    if let Ok(d) = std::env::var("MIST_STATE_DIR") {
        return PathBuf::from(d).join("control.sock");
    }
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join("Library/Application Support/Mist/control.sock")
}

async fn call(socket: &PathBuf, req: Value) -> anyhow::Result<Value> {
    let stream = UnixStream::connect(socket).await.with_context(|| {
        format!(
            "connecting to mist-hostd at {} — is the daemon running? Start it with \
                 `mist-hostd` (or check `mist doctor`)",
            socket.display()
        )
    })?;
    let (r, mut w) = stream.into_split();
    let mut line = req.to_string();
    line.push('\n');
    w.write_all(line.as_bytes()).await?;
    let mut reader = BufReader::new(r).lines();
    let reply = reader
        .next_line()
        .await?
        .context("daemon closed connection")?;
    let v: Value = serde_json::from_str(&reply)?;
    if !v["ok"].as_bool().unwrap_or(false) {
        bail!("{}", v["error"].as_str().unwrap_or("unknown daemon error"));
    }
    Ok(v)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let socket = args.socket.clone().unwrap_or_else(default_socket);

    match args.cmd {
        Cmd::Status { json } => {
            let v = call(&socket, json!({"cmd": "status"})).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
                return Ok(());
            }
            for vm in v["vms"].as_array().into_iter().flatten() {
                println!(
                    "{}  [{}]  {}  seed-rate {}/s",
                    vm["name"].as_str().unwrap_or("?"),
                    vm["state"].as_str().unwrap_or("?"),
                    vm["endpoint"].as_str().unwrap_or(""),
                    vm["seed_rate_per_s"]
                );
                for sh in vm["shares"].as_array().into_iter().flatten() {
                    println!(
                        "  {}  [{}]  nodes {}  dirs {}  entries {}",
                        sh["name"].as_str().unwrap_or("?"),
                        sh["state"].as_str().unwrap_or("?"),
                        sh["nodes"],
                        sh["dirs"],
                        sh["entries"],
                    );
                }
            }
        }
        Cmd::Ls { vm, share, path } => {
            let v = call(
                &socket,
                json!({"cmd": "ls", "vm": vm, "share": share, "path": path}),
            )
            .await?;
            for e in v["entries"].as_array().into_iter().flatten() {
                let kind = e["kind"].as_str().unwrap_or("?");
                let mode = e["mode"].as_u64().unwrap_or(0);
                let size = e["size"].as_u64().unwrap_or(0);
                let name = e["name"].as_str().unwrap_or("?");
                let tag = match kind {
                    "dir" => "d",
                    "symlink" => "l",
                    "file" => "-",
                    other => &other[..1],
                };
                println!("{tag}{:03o} {size:>12}  {name}", mode & 0o777);
            }
            if v["truncated"].as_bool() == Some(true) {
                eprintln!("(listing truncated)");
            }
        }
        Cmd::Stat { vm, share, path } => {
            let v = call(
                &socket,
                json!({"cmd": "stat", "vm": vm, "share": share, "path": path}),
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Cmd::Cat { vm, share, path } => {
            let v = call(
                &socket,
                json!({"cmd": "cat", "vm": vm, "share": share, "path": path}),
            )
            .await?;
            let data = b64decode(v["data_b64"].as_str().unwrap_or(""))?;
            std::io::stdout().write_all(&data)?;
            if v["truncated"].as_bool() == Some(true) {
                eprintln!("\n(content truncated at 10 MiB)");
            }
        }
        Cmd::Mount { vm, share, nfs41 } => {
            let v = call(
                &socket,
                json!({"cmd": "mount", "vm": vm, "share": share, "nfs41": nfs41}),
            )
            .await?;
            println!("mounted at {}", v["mountpoint"].as_str().unwrap_or("?"));
        }
        Cmd::Umount { vm, share } => {
            call(&socket, json!({"cmd": "umount", "vm": vm, "share": share})).await?;
            println!("unmounted {vm}/{share}");
        }
        Cmd::Events { follow, path } => {
            if !follow {
                let v = call(&socket, json!({"cmd": "events"})).await?;
                println!("{}", v["note"].as_str().unwrap_or("use --follow"));
                return Ok(());
            }
            // Streaming: send the request, then print lines until the daemon closes.
            let stream = UnixStream::connect(&socket).await?;
            let (r, mut w) = stream.into_split();
            let mut req = json!({"cmd": "events", "follow": true});
            if let Some(p) = &path {
                req["path"] = json!(p);
            }
            w.write_all((req.to_string() + "\n").as_bytes()).await?;
            let mut lines = BufReader::new(r).lines();
            // First line is the {ok, following} ack.
            let _ = lines.next_line().await?;
            eprintln!("following change feed (ctrl-c to stop)…");
            while let Some(line) = lines.next_line().await? {
                println!("{line}");
            }
        }
        Cmd::Conflicts { json } => {
            let v = call(&socket, json!({"cmd": "conflicts"})).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
                return Ok(());
            }
            let rows = v["conflicts"].as_array().cloned().unwrap_or_default();
            if rows.is_empty() {
                println!("no conflicts (total ever: {})", v["total"]);
                return Ok(());
            }
            for r in &rows {
                let ms = r["at_unix_ms"].as_u64().unwrap_or(0);
                println!(
                    "{}  {}  {}:{}  {}  {}",
                    ms / 1000,
                    r["kind"].as_str().unwrap_or("?"),
                    r["vm"].as_str().unwrap_or("?"),
                    r["share"],
                    r["path"].as_str().unwrap_or("?"),
                    r["detail"].as_str().unwrap_or(""),
                );
            }
            println!("{} shown, {} total ever", rows.len(), v["total"]);
        }
        Cmd::Delegs { json } => {
            let v = call(&socket, json!({"cmd": "delegs"})).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
                return Ok(());
            }
            let mounts = v["mounts"].as_array().cloned().unwrap_or_default();
            if mounts.is_empty() {
                println!("no NFSv4.1 mounts (mount with --nfs41)");
                return Ok(());
            }
            for m in &mounts {
                println!(
                    "{}/{}: held {}  granted {}  recalls {}  returned {}  revoked {}",
                    m["vm"].as_str().unwrap_or("?"),
                    m["share"].as_str().unwrap_or("?"),
                    m["held"],
                    m["granted"],
                    m["recalls"],
                    m["returned"],
                    m["revoked"],
                );
                println!(
                    "  recall latency: p50 {} µs  p99 {} µs  max {} µs  ({} samples)",
                    m["recall_p50_us"], m["recall_p99_us"], m["recall_max_us"], m["recall_samples"],
                );
            }
        }
        Cmd::Cache {
            action,
            sample,
            json,
        } => {
            if !["stats", "clear", "scrub"].contains(&action.as_str()) {
                bail!("action must be stats|clear|scrub, got {action:?}");
            }
            let v = call(
                &socket,
                json!({"cmd": "cache", "action": action, "sample": sample}),
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
                return Ok(());
            }
            for vm in v["vms"].as_array().into_iter().flatten() {
                let name = vm["vm"].as_str().unwrap_or("?");
                if vm["enabled"] != true {
                    println!("{name}: cache disabled");
                    continue;
                }
                let s = &vm["stats"];
                let gib = |b: &Value| b.as_u64().unwrap_or(0) as f64 / (1u64 << 30) as f64;
                println!(
                    "{name}: {} blobs, {:.2}/{:.0} GiB  hits {}  misses {}  evictions {}  corrupt {}",
                    s["blobs"],
                    gib(&s["total_bytes"]),
                    gib(&s["max_bytes"]),
                    s["hits"],
                    s["misses"],
                    s["evictions"],
                    s["corrupt_dropped"],
                );
                println!(
                    "  guest reads: {} ops, {} bytes (0 = fully cache-served)",
                    vm["guest_read_ops"], vm["guest_read_bytes"],
                );
                if let Some(sc) = vm.get("scrub").filter(|s| !s.is_null()) {
                    println!(
                        "  scrub: checked {}  corrupt {}  missing {}",
                        sc["checked"], sc["corrupt"], sc["missing"],
                    );
                }
            }
        }
        Cmd::Doctor { json } => {
            return doctor(&socket, json).await;
        }
        Cmd::Discover { json } => {
            let v = call(&socket, json!({"cmd": "discover"})).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&v)?);
                return Ok(());
            }
            let mdns = v["mdns"].as_array().cloned().unwrap_or_default();
            if mdns.is_empty() {
                println!(
                    "no _mist._tcp instances on mDNS (guests may still be lease/ARP-scannable)"
                );
            } else {
                println!("discovered guests (_mist._tcp):");
                for i in &mdns {
                    println!(
                        "  {}  →  {}:{}  uuid={}",
                        i["instance"].as_str().unwrap_or("?"),
                        i["host"].as_str().unwrap_or("?"),
                        i["port"],
                        i["vm_uuid"].as_str().unwrap_or("(none)"),
                    );
                }
            }
            for m in v["vmnet"].as_array().into_iter().flatten() {
                println!(
                    "vmnet {}: gateway {} netmask {}",
                    m["iface"].as_str().unwrap_or("?"),
                    m["gateway"].as_str().unwrap_or("?"),
                    m["netmask"].as_str().unwrap_or("?"),
                );
            }
        }
        Cmd::Add { vm, token, uuid } => {
            let tok = token
                .canonicalize()
                .unwrap_or(token)
                .to_string_lossy()
                .into_owned();
            let mut req = json!({"cmd": "add", "name": vm, "token": tok});
            if let Some(u) = uuid {
                req["uuid"] = json!(u);
            }
            let v = call(&socket, req).await?;
            let uuid = v["vm_uuid"].as_str().unwrap_or("(binds on first connect)");
            if v["reachable"].as_bool() == Some(true) {
                println!(
                    "added {vm}: autodiscovered at {}, identity {uuid}",
                    v["endpoint"].as_str().unwrap_or("?")
                );
            } else {
                println!("added {vm}: not reachable yet — it'll connect when the guest comes up");
            }
            println!("bridge=\"auto\" written — no IP stored. `mist mount {vm} <share>` to mount.");
        }
        Cmd::Version => {
            let v = call(&socket, json!({"cmd": "version"})).await?;
            println!("mist-hostd {}", v["version"].as_str().unwrap_or("?"));
            println!("mist       {}", env!("CARGO_PKG_VERSION"));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// doctor — health + security checkup (design 07 §6 hygiene + general triage)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
enum Level {
    Ok,
    Warn,
    Fail,
}

struct Report {
    checks: Vec<(Level, String, String)>,
}

impl Report {
    fn add(&mut self, level: Level, what: impl Into<String>, detail: impl Into<String>) {
        self.checks.push((level, what.into(), detail.into()));
    }
}

fn state_dir() -> PathBuf {
    if let Ok(d) = std::env::var("MIST_STATE_DIR") {
        return PathBuf::from(d);
    }
    let home = std::env::var("HOME").expect("HOME not set");
    PathBuf::from(home).join("Library/Application Support/Mist")
}

fn mode_of(p: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(p).ok().map(|m| m.mode() & 0o777)
}

/// The egress interface the kernel would use to reach `gateway`, via `route -n get`. Returns the
/// `interface:` field (e.g. `bridge100`, or `utun4` when a VPN has hijacked the subnet).
fn route_egress(gateway: &str) -> Option<String> {
    if gateway.is_empty() {
        return None;
    }
    let out = std::process::Command::new("/sbin/route")
        .args(["-n", "get", gateway])
        .output()
        .ok()?;
    parse_route_interface(&String::from_utf8_lossy(&out.stdout))
}

/// Parse the `interface:` line out of `route -n get` output. Pure; tested.
fn parse_route_interface(out: &str) -> Option<String> {
    out.lines()
        .find_map(|l| l.trim().strip_prefix("interface:"))
        .map(|s| s.trim().to_string())
}

async fn doctor(socket: &PathBuf, json_out: bool) -> anyhow::Result<()> {
    use Level::*;
    let mut r = Report { checks: Vec::new() };
    let dir = state_dir();

    // -- daemon ------------------------------------------------------------
    let daemon = call(socket, json!({"cmd": "version"})).await.ok();
    match &daemon {
        Some(v) => {
            let dv = v["version"].as_str().unwrap_or("?");
            if dv == env!("CARGO_PKG_VERSION") {
                r.add(Ok, "daemon", format!("mist-hostd {dv} running"));
            } else {
                r.add(
                    Warn,
                    "daemon",
                    format!(
                        "version skew: hostd {dv}, CLI {} — restart whichever is older",
                        env!("CARGO_PKG_VERSION")
                    ),
                );
            }
        }
        None => r.add(
            Fail,
            "daemon",
            format!(
                "mist-hostd not reachable at {} — is it running? (launchctl/`mist-hostd`)",
                socket.display()
            ),
        ),
    }

    // -- state dir + secrets -----------------------------------------------
    if dir.is_dir() {
        match mode_of(&dir) {
            Some(m) if m & 0o077 != 0 => r.add(
                Warn,
                "state dir",
                format!(
                    "{} is mode {m:03o}; other users can reach handles/tokens — chmod 700 it",
                    dir.display()
                ),
            ),
            _ => r.add(Ok, "state dir", format!("{} (mode 700)", dir.display())),
        }
    } else {
        r.add(
            Warn,
            "state dir",
            format!(
                "{} does not exist yet (created on first daemon start)",
                dir.display()
            ),
        );
    }
    let secret = dir.join("handle.secret");
    if secret.is_file() {
        let m = mode_of(&secret).unwrap_or(0);
        if m & 0o077 != 0 {
            r.add(
                Warn,
                "handle secret",
                format!("mode {m:03o} — NFS handles are forgeable by local users; chmod 600"),
            );
        } else {
            let age_days = std::fs::metadata(&secret)
                .and_then(|md| md.modified())
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs() / 86400)
                .unwrap_or(0);
            if age_days > 180 {
                r.add(
                    Warn,
                    "handle secret",
                    format!("{age_days} days old — rotate (delete it + restart hostd + remount)"),
                );
            } else {
                r.add(
                    Ok,
                    "handle secret",
                    format!("mode 600, {age_days} days old"),
                );
            }
        }
    }

    // -- config + tokens + transports ----------------------------------------
    let cfg_path = dir.join("config.toml");
    if cfg_path.is_file() {
        match std::fs::read_to_string(&cfg_path)
            .map_err(anyhow::Error::from)
            .and_then(|s| toml::from_str::<toml::Value>(&s).map_err(Into::into))
        {
            Result::Ok(cfg) => {
                r.add(Ok, "config", format!("{} parses", cfg_path.display()));
                for (name, vm) in cfg
                    .get("vm")
                    .and_then(|v| v.as_table())
                    .into_iter()
                    .flatten()
                {
                    let bridge = vm.get("bridge").and_then(|v| v.as_str()).unwrap_or("");
                    if bridge.eq_ignore_ascii_case("auto") {
                        match vm.get("vm_uuid").and_then(|v| v.as_str()) {
                            Some(u) if u.len() == 32 => r.add(
                                Ok,
                                format!("vm {name}"),
                                "bridge=auto (resolved per-connect; no stored IP to drift)",
                            ),
                            _ => r.add(
                                Warn,
                                format!("vm {name}"),
                                "bridge=auto but vm_uuid missing/short — re-pair to bind identity",
                            ),
                        }
                    } else if bridge.starts_with("tcp:")
                        && !bridge.starts_with("tcp:127.")
                        && !bridge.starts_with("tcp:localhost")
                    {
                        r.add(
                            Warn,
                            format!("vm {name}"),
                            format!("endpoint {bridge} is cleartext TCP; prefer bridge:/uds:/auto or an external encrypted tunnel"),
                        );
                    }
                    let Some(tok) = vm.get("token").and_then(|v| v.as_str()) else {
                        r.add(Fail, format!("vm {name}"), "no token path in config");
                        continue;
                    };
                    // Resolve the `@vms/...` form pairing writes (design 11 §4).
                    let tp = if let Some(rest) = tok.strip_prefix('@') {
                        dir.join(rest)
                    } else {
                        PathBuf::from(tok)
                    };
                    match std::fs::metadata(&tp) {
                        Result::Ok(md) if md.len() < 32 => r.add(
                            Fail,
                            format!("vm {name} token"),
                            format!("{tok} is {} bytes; needs ≥32 random bytes", md.len()),
                        ),
                        Result::Ok(_) => match mode_of(&tp) {
                            Some(m) if m & 0o077 != 0 => r.add(
                                Warn,
                                format!("vm {name} token"),
                                format!("{tok} is mode {m:03o} — chmod 600"),
                            ),
                            _ => r.add(Ok, format!("vm {name} token"), format!("{tok} (mode 600)")),
                        },
                        Err(_) => r.add(
                            Fail,
                            format!("vm {name} token"),
                            format!("{tok} missing — sessions to this VM cannot authenticate"),
                        ),
                    }
                }
            }
            Err(e) => r.add(
                Fail,
                "config",
                format!("{} unreadable: {e}", cfg_path.display()),
            ),
        }
    } else {
        r.add(
            Ok,
            "config",
            "no config.toml (ad-hoc --vm attachments only)",
        );
    }

    // -- environment ---------------------------------------------------------
    if std::path::Path::new("/sbin/mount_nfs").exists() {
        r.add(Ok, "mount_nfs", "/sbin/mount_nfs present");
    } else {
        r.add(
            Fail,
            "mount_nfs",
            "missing /sbin/mount_nfs — cannot mount shares",
        );
    }

    // -- network / discovery (design 11 §3, §10) -----------------------------
    if daemon.is_some()
        && let Result::Ok(disc) = call(socket, json!({"cmd": "discover"})).await
    {
        let vmnet = disc["vmnet"].as_array().cloned().unwrap_or_default();
        if vmnet.is_empty() {
            r.add(
                Warn,
                "vmnet",
                "no vmnet bridge (bridge1xx) found — start the VM, or this Mac isn't hosting one",
            );
        }
        for m in &vmnet {
            let gw = m["gateway"].as_str().unwrap_or("");
            let iface = m["iface"].as_str().unwrap_or("?");
            // VPN route hijack: a VPN routinely steals 192.168.64.0/24 through a utun*, severing
            // host↔guest while looking "up" (design 11 §3 — the single biggest real-world failure).
            match route_egress(gw) {
                Some(dev) if dev.starts_with("utun") => r.add(
                    Fail,
                    "vpn route",
                    format!(
                        "traffic to the vmnet gateway {gw} egresses {dev} (a VPN tunnel), not {iface} — \
                         split-exclude the vmnet subnet in your VPN client to restore host↔guest"
                    ),
                ),
                Some(dev) if dev == iface => {
                    r.add(Ok, "vpn route", format!("gateway {gw} routes via {iface} (no VPN hijack)"))
                }
                Some(dev) => r.add(
                    Warn,
                    "vpn route",
                    format!("gateway {gw} egresses {dev}, expected {iface} — verify routing"),
                ),
                None => {}
            }
        }
        // mDNS: empty browse + a firewall on the BPF mDNSResponder/bootpd need degrades discovery
        // to the lease/ARP scan (still works, just slower to first connect).
        let mdns_empty = disc["mdns"].as_array().is_none_or(|a| a.is_empty());
        if mdns_empty {
            r.add(
                Warn,
                "mDNS",
                "no _mist._tcp instances right now — discovery falls back to lease/ARP scan + \
                 authenticated probe; allow mDNSResponder/bootpd on the bridge to restore fast discovery",
            );
        } else {
            r.add(
                Ok,
                "mDNS",
                format!(
                    "{} _mist._tcp instance(s) browsable",
                    disc["mdns"].as_array().map(|a| a.len()).unwrap_or(0)
                ),
            );
        }
        // Transport honesty: TCP-over-vmnet is authenticated but not encrypted until Noise TCP (M7).
        r.add(
            Ok,
            "transport",
            "auto/tcp over vmnet (authenticated, not yet encrypted — Noise TCP is M7; vsock is never \
             attempted under a foreign supervisor: a non-owner socket(PF_VSOCK) is ENODEV)",
        );
    }

    // -- sessions + shares + mounts (daemon-side) ----------------------------
    if daemon.is_some() {
        if let Result::Ok(v) = call(socket, json!({"cmd": "status"})).await {
            for vm in v["vms"].as_array().into_iter().flatten() {
                let name = vm["name"].as_str().unwrap_or("?");
                let st = vm["state"].as_str().unwrap_or("?");
                match st {
                    "live" | "ready" => r.add(Ok, format!("vm {name}"), format!("session {st}")),
                    "seeding" | "resyncing" => r.add(
                        Warn,
                        format!("vm {name}"),
                        format!("session {st} (transient)"),
                    ),
                    _ => r.add(
                        Fail,
                        format!("vm {name}"),
                        format!("session {st} — check the VM is booted and mistd is running"),
                    ),
                }
                for sh in vm["shares"].as_array().into_iter().flatten() {
                    let sn = sh["name"].as_str().unwrap_or("?");
                    let ss = sh["state"].as_str().unwrap_or("?");
                    if ss != "live" {
                        r.add(Warn, format!("share {name}/{sn}"), format!("state {ss}"));
                    }
                }
            }
        }
        if let Result::Ok(v) = call(socket, json!({"cmd": "mounts"})).await {
            let mount_out = std::process::Command::new("/sbin/mount")
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
                .unwrap_or_default();
            for m in v["mounts"].as_array().into_iter().flatten() {
                let mp = m["mountpoint"].as_str().unwrap_or("");
                let label = format!(
                    "mount {}/{}",
                    m["vm"].as_str().unwrap_or("?"),
                    m["share"].as_str().unwrap_or("?")
                );
                let line = mount_out
                    .lines()
                    .find(|l| l.contains(&format!(" on {mp} ")));
                match line {
                    Some(l) if l.starts_with("127.0.0.1:") => {
                        r.add(Ok, label, format!("{mp} (loopback NFS, port {})", m["port"]))
                    }
                    Some(l) => r.add(
                        Warn,
                        label,
                        format!("served from {} — expected loopback", l.split(' ').next().unwrap_or("?")),
                    ),
                    None => r.add(
                        Fail,
                        label,
                        format!("daemon thinks {mp} is mounted but the kernel disagrees — `mist umount` then remount"),
                    ),
                }
            }
        }
    }

    // -- emit ----------------------------------------------------------------
    let worst = r
        .checks
        .iter()
        .map(|(l, _, _)| *l)
        .max_by_key(|l| match l {
            Ok => 0,
            Warn => 1,
            Fail => 2,
        })
        .unwrap_or(Ok);
    if json_out {
        let checks: Vec<Value> = r
            .checks
            .iter()
            .map(|(l, w, d)| {
                json!({"level": match l { Ok => "ok", Warn => "warn", Fail => "fail" }, "check": w, "detail": d})
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({"ok": worst != Fail, "checks": checks}))?
        );
    } else {
        for (l, what, detail) in &r.checks {
            let tag = match l {
                Ok => "✓",
                Warn => "⚠",
                Fail => "✗",
            };
            println!("{tag} {what}: {detail}");
        }
        println!();
        match worst {
            Ok => println!("all checks passed"),
            Warn => println!("warnings present (functional, but tighten the ⚠ items)"),
            Fail => println!("problems found — fix the ✗ items"),
        }
    }
    std::process::exit(match worst {
        Ok => 0,
        Warn => 1,
        Fail => 2,
    });
}

fn b64decode(s: &str) -> anyhow::Result<Vec<u8>> {
    fn val(c: u8) -> anyhow::Result<u32> {
        Ok(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("bad base64 byte {c}"),
        })
    }
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        let bytes = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        out.extend_from_slice(&bytes[..chunk.len() - 1]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_interface_parse() {
        let out = "   route to: 192.168.64.1\ndestination: 192.168.64.1\n  interface: bridge100\n     flags: <UP>\n";
        assert_eq!(parse_route_interface(out).as_deref(), Some("bridge100"));
        let vpn = "  interface: utun4\n";
        assert_eq!(parse_route_interface(vpn).as_deref(), Some("utun4"));
        assert_eq!(parse_route_interface("no interface here"), None);
    }

    #[test]
    fn b64decode_known_vector() {
        // "Mist" → "TWlzdA==".
        assert_eq!(b64decode("TWlzdA==").unwrap(), b"Mist");
    }
}
