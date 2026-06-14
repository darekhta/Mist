//! The resolver — `bridge = "auto"` (design 11 §2).
//!
//! A VM whose bridge is `auto` is *located* on every connect, keyed by its stable `vm_uuid` and
//! authenticated by its token. No network coordinate is ever stored, so DHCP drift is a non-event.
//! The chain degrades, never dead-ends:
//!
//! | Step | Mechanism | Yields |
//! |------|-----------|--------|
//! | 1 | mDNS browse `_mist._tcp` (TXT) | host, port, vm_uuid |
//! | 2 | mDNS A `<saved-host>.local` (cached) | IP for a known host |
//! | 3 | lease/ARP scan of the vmnet subnet | candidate IPs |
//! | 4 | authenticated probe of each candidate | the one whose `Hello` + `VmIdentity` matches |
//!
//! Steps 1–2 cost milliseconds; the scan+probe fallback (3–4) is what makes discovery work behind
//! a firewall / Little Snitch that blocks mDNS. Only the genuinely-paired guest completes `Hello`,
//! so probing a multi-VM subnet is safe; `vm_uuid` then rejects a cloned/reused token.

use crate::config::state_dir;
use mist_proto::{CtlMsg, FrameKind, PROTO_VERSION, TCP_PORT, features};
use mist_transport::{Endpoint, FramedStream};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::time::Duration;

/// One DHCP lease (macOS `/var/db/dhcpd_leases`). Matched by **hostname**, never MAC — leases are
/// DUID-keyed on macOS 15+, so the `hw_address` no longer identifies the guest (design 11 §3; this
/// bit us in e2e).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub name: Option<String>,
    pub ip: Ipv4Addr,
}

/// A host-side view of one vmnet bridge: the host's own address on it is the guest's gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmnetModel {
    pub iface: String,
    pub gateway: Ipv4Addr,
    pub netmask: Ipv4Addr,
}

impl VmnetModel {
    /// Whether `ip` is inside this bridge's subnet (so it could be one of our guests).
    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        let m = u32::from(self.netmask);
        (u32::from(ip) & m) == (u32::from(self.gateway) & m)
    }

    /// All host addresses in the subnet (excluding network/broadcast/gateway) — the scan space
    /// for step 3. Capped so a misconfigured /16 can't generate 65k probes.
    pub fn candidate_ips(&self, cap: usize) -> Vec<Ipv4Addr> {
        let net = u32::from(self.gateway) & u32::from(self.netmask);
        let bcast = net | !u32::from(self.netmask);
        let gw = u32::from(self.gateway);
        let mut out = Vec::new();
        let mut a = net + 1;
        while a < bcast && out.len() < cap {
            if a != gw {
                out.push(Ipv4Addr::from(a));
            }
            a += 1;
        }
        out
    }
}

/// One `_mist._tcp` instance as resolved from mDNS: host+port+identity, no secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdnsInstance {
    pub instance: String,
    pub host: String,
    pub port: u16,
    pub vm_uuid: Option<[u8; 16]>,
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error(
        "no candidate located for vm {vm:?} (mDNS empty, lease/ARP scan found nothing on the \
             vmnet subnet, no probe answered): {detail}"
    )]
    Degraded { vm: String, detail: String },
    #[error(
        "located a guest but its vm_uuid did not match the paired identity — a cloned or \
             reused token? Re-pair the VM."
    )]
    IdentityMismatch,
    #[error("transport: {0}")]
    Transport(#[from] mist_transport::TransportError),
}

/// Persistent, **IP-free** resolver hint cache: `vm_uuid → (last mDNS instance, last hostname)`.
/// The IP is re-derived every connect (design 11 §2 "never cache an IP").
#[derive(Debug, Default)]
pub struct Resolver;

impl Resolver {
    pub fn new() -> Self {
        Resolver
    }

    fn cache_path() -> PathBuf {
        state_dir().join("resolver-cache.toml")
    }

    fn load_cache() -> BTreeMap<String, CachedHint> {
        std::fs::read_to_string(Self::cache_path())
            .ok()
            .and_then(|s| toml::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_hint(uuid_hex: &str, hint: CachedHint) {
        let mut map = Self::load_cache();
        map.insert(uuid_hex.to_string(), hint);
        if let Ok(s) = toml::to_string(&map) {
            let _ = std::fs::create_dir_all(state_dir());
            let _ = std::fs::write(Self::cache_path(), s);
        }
    }

    /// Locate a `bridge="auto"` VM, returning a concrete TCP endpoint. `expected_uuid` is the
    /// identity bound at pairing; a candidate whose `vm_uuid` differs is rejected.
    pub async fn resolve(
        &self,
        vm: &str,
        expected_uuid: Option<[u8; 16]>,
        token_hash: [u8; 32],
    ) -> Result<Endpoint, ResolveError> {
        let want_hex = expected_uuid.map(|u| mist_proto::vm_uuid_hex(&u));

        // Step 1: mDNS browse. The happy path — host+port+uuid in one shot, no root.
        let instances = mdns_browse(Duration::from_secs(3)).await;
        for inst in &instances {
            if uuid_matches(inst.vm_uuid, expected_uuid)
                && let Some(ip) = resolve_host_ipv4(&inst.host)
            {
                let ep = Endpoint::Tcp(format!("{ip}:{}", inst.port));
                if self
                    .confirm(&ep, token_hash, expected_uuid)
                    .await
                    .unwrap_or(false)
                {
                    if let Some(h) = &want_hex {
                        Self::save_hint(
                            h,
                            CachedHint {
                                instance: inst.instance.clone(),
                                host: inst.host.clone(),
                            },
                        );
                    }
                    tracing::info!(vm, %ip, port = inst.port, "resolved via mDNS");
                    return Ok(ep);
                }
            }
        }

        // Step 2: cached `<host>.local` A record (last paired name), still token+uuid gated.
        if let Some(h) = &want_hex
            && let Some(hint) = Self::load_cache().get(h)
            && let Some(ip) = resolve_host_ipv4(&hint.host)
        {
            let ep = Endpoint::Tcp(format!("{ip}:{TCP_PORT}"));
            if self
                .confirm(&ep, token_hash, expected_uuid)
                .await
                .unwrap_or(false)
            {
                tracing::info!(vm, host = %hint.host, %ip, "resolved via cached .local name");
                return Ok(ep);
            }
        }

        // Steps 3+4: lease/ARP scan of the vmnet subnet, then authenticated probe of each.
        let models = derive_vmnet();
        let mut candidates: Vec<Ipv4Addr> = Vec::new();
        let leases = read_dhcpd_leases();
        let arp = read_arp_table();
        for m in &models {
            for ip in leases.iter().map(|l| l.ip).chain(arp.iter().copied()) {
                if m.contains(ip) && !candidates.contains(&ip) {
                    candidates.push(ip);
                }
            }
        }
        // If neither table yielded anything (fresh boot, empty ARP), fall back to a bounded sweep
        // of the first model's subnet.
        if candidates.is_empty()
            && let Some(m) = models.first()
        {
            candidates = m.candidate_ips(256);
        }

        // Probe candidates with BOUNDED CONCURRENCY (not serially): a full /24 sweep behind a
        // firewall that silently drops SYNs would otherwise be ~253 × 2 s ≈ 8 min. With a fan-out
        // cap it's a few seconds. First authenticated match wins; the rest are aborted.
        if let Some(ep) = probe_candidates_concurrent(candidates, token_hash, expected_uuid).await {
            tracing::info!(vm, endpoint = %ep, "resolved via lease/ARP scan + authenticated probe");
            return Ok(ep);
        }

        Err(ResolveError::Degraded {
            vm: vm.to_string(),
            detail: format!(
                "{} vmnet bridge(s), {} lease(s), {} arp entr(ies)",
                models.len(),
                leases.len(),
                arp.len()
            ),
        })
    }

    /// Probe a candidate: complete `Hello` with the token, read back `VmIdentity`, and check it
    /// matches `expected_uuid` (or accept any identity when none is bound yet). Returns `Ok(false)`
    /// for "answered but not our guest", `Err` for "did not answer".
    async fn confirm(
        &self,
        ep: &Endpoint,
        token_hash: [u8; 32],
        expected_uuid: Option<[u8; 16]>,
    ) -> Result<bool, ResolveError> {
        let got = probe(ep, token_hash).await?;
        Ok(identity_ok(got, expected_uuid))
    }
}

/// Does the probed identity satisfy the expectation? `None` expected ⇒ accept any (token-only bind,
/// first connect); `Some` expected ⇒ require an exact `VmIdentity` match.
fn identity_ok(got: Option<[u8; 16]>, expected: Option<[u8; 16]>) -> bool {
    match (got, expected) {
        (Some(u), Some(want)) => u == want,
        (Some(_), None) => true,  // not identity-bound yet (first add)
        (None, Some(_)) => false, // a guest that can't be identity-bound
        (None, None) => true,
    }
}

/// Probe many candidate IPs with a bounded fan-out and return the first whose `Hello` + `VmIdentity`
/// satisfies `expected_uuid`. Caps in-flight probes so a firewalled /24 sweep takes seconds, not
/// minutes; aborts the stragglers on the first hit.
async fn probe_candidates_concurrent(
    candidates: Vec<Ipv4Addr>,
    token_hash: [u8; 32],
    expected_uuid: Option<[u8; 16]>,
) -> Option<Endpoint> {
    const MAX_INFLIGHT: usize = 24;
    type ProbeOut = (
        Endpoint,
        Result<Option<[u8; 16]>, mist_transport::TransportError>,
    );
    let mut iter = candidates.into_iter();
    let mut set: tokio::task::JoinSet<ProbeOut> = tokio::task::JoinSet::new();
    let mut spawn_next = |set: &mut tokio::task::JoinSet<ProbeOut>| {
        if let Some(ip) = iter.next() {
            let ep = Endpoint::Tcp(format!("{ip}:{TCP_PORT}"));
            set.spawn(async move {
                let r = probe(&ep, token_hash).await;
                (ep, r)
            });
        }
    };
    for _ in 0..MAX_INFLIGHT {
        spawn_next(&mut set);
    }
    while let Some(joined) = set.join_next().await {
        if let Ok((ep, Ok(got))) = joined
            && identity_ok(got, expected_uuid)
        {
            set.abort_all();
            return Some(ep);
        }
        spawn_next(&mut set);
    }
    None
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedHint {
    instance: String,
    host: String,
}

fn uuid_matches(got: Option<[u8; 16]>, want: Option<[u8; 16]>) -> bool {
    match (got, want) {
        (Some(g), Some(w)) => g == w,
        (_, None) => true,
        (None, Some(_)) => false,
    }
}

/// Send `Hello` (with `VM_IDENTITY` set) and read the guest's `VmIdentity` if it sends one.
/// `Ok(None)` ⇒ authenticated but the guest is too old to be identity-bound; `Err` ⇒ no answer or
/// auth rejected. A short per-candidate timeout keeps a subnet sweep bounded.
pub async fn probe(
    ep: &Endpoint,
    token_hash: [u8; 32],
) -> Result<Option<[u8; 16]>, mist_transport::TransportError> {
    let fut = async {
        let stream = mist_transport::dial(ep).await?;
        let mut ctl = FramedStream::new(stream, false);
        ctl.send_msg(
            FrameKind::Ctl,
            0,
            &CtlMsg::Hello {
                proto: PROTO_VERSION,
                features: features::SUPPORTED, // includes VM_IDENTITY → guest replies if it can
                token_hash,
                host_name: "mist-resolver".into(),
                host_version: env!("CARGO_PKG_VERSION").into(),
            },
        )
        .await?;
        let (_, ack): (u64, CtlMsg) = ctl.recv_msg(FrameKind::Ctl).await?;
        match ack {
            CtlMsg::HelloAck { .. } => {}
            CtlMsg::AuthFail => {
                return Err(mist_transport::TransportError::Protocol("auth rejected"));
            }
            _ => {
                return Err(mist_transport::TransportError::Protocol(
                    "expected HelloAck",
                ));
            }
        }
        // VmIdentity (if negotiated) arrives before Endpoints; read a couple of frames for it.
        for _ in 0..2 {
            match ctl.recv_msg::<CtlMsg>(FrameKind::Ctl).await {
                Ok((_, CtlMsg::VmIdentity { vm_uuid })) => return Ok(Some(vm_uuid)),
                Ok((_, CtlMsg::Endpoints { .. })) => continue,
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        Ok(None)
    };
    match tokio::time::timeout(Duration::from_secs(2), fut).await {
        Ok(r) => r,
        Err(_) => Err(mist_transport::TransportError::Timeout),
    }
}

/// Resolve a hostname (e.g. `darekhta-debian.local`) to an IPv4 via `getaddrinfo` — which on macOS
/// routes `.local` through mDNSResponder (step 2 of the chain).
pub fn resolve_host_ipv4(host: &str) -> Option<Ipv4Addr> {
    use std::net::ToSocketAddrs;
    // Port is irrelevant for name→addr; 0 is fine.
    (host, 0u16)
        .to_socket_addrs()
        .ok()?
        .find_map(|sa| match sa.ip() {
            std::net::IpAddr::V4(v4) => Some(v4),
            _ => None,
        })
}

// ---------------------------------------------------------------------------
// vmnet model derivation
// ---------------------------------------------------------------------------

/// Derive the vmnet bridge(s) from `getifaddrs`: a `bridge1xx` interface with a private IPv4.
/// (The host's address on the bridge is the guest's gateway, e.g. 192.168.64.1.)
#[allow(unsafe_code)]
pub fn derive_vmnet() -> Vec<VmnetModel> {
    let mut out = Vec::new();
    let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: standard getifaddrs/freeifaddrs pattern; pointers only read while the list is owned.
    unsafe {
        if libc::getifaddrs(&mut ifap) != 0 {
            return out;
        }
        let mut cur = ifap;
        while !cur.is_null() {
            let ifa = &*cur;
            cur = ifa.ifa_next;
            if ifa.ifa_addr.is_null() || ifa.ifa_netmask.is_null() {
                continue;
            }
            if i32::from((*ifa.ifa_addr).sa_family) != libc::AF_INET {
                continue;
            }
            let name = std::ffi::CStr::from_ptr(ifa.ifa_name).to_string_lossy();
            if !is_vmnet_bridge_name(&name) {
                continue;
            }
            let sin = &*(ifa.ifa_addr as *const libc::sockaddr_in);
            let mask = &*(ifa.ifa_netmask as *const libc::sockaddr_in);
            let gw = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let netmask = Ipv4Addr::from(u32::from_be(mask.sin_addr.s_addr));
            if is_private_v4(gw) {
                out.push(VmnetModel {
                    iface: name.into_owned(),
                    gateway: gw,
                    netmask,
                });
            }
        }
        libc::freeifaddrs(ifap);
    }
    out
}

fn is_vmnet_bridge_name(name: &str) -> bool {
    // Apple's vmnet uses bridge100, bridge101, …; `container` uses bridge1xx too. Match bridge1\d\d.
    name.strip_prefix("bridge").is_some_and(|n| {
        n.len() == 3 && n.starts_with('1') && n.bytes().all(|b| b.is_ascii_digit())
    })
}

fn is_private_v4(ip: Ipv4Addr) -> bool {
    ip.is_private()
}

fn read_dhcpd_leases() -> Vec<Lease> {
    std::fs::read_to_string("/var/db/dhcpd_leases")
        .map(|s| parse_dhcpd_leases(&s))
        .unwrap_or_default()
}

/// Parse macOS `/var/db/dhcpd_leases`. Pure; tested.
pub fn parse_dhcpd_leases(text: &str) -> Vec<Lease> {
    let mut out = Vec::new();
    let mut name: Option<String> = None;
    let mut ip: Option<Ipv4Addr> = None;
    for line in text.lines() {
        let l = line.trim();
        if l == "{" {
            name = None;
            ip = None;
        } else if l == "}" {
            if let Some(ip) = ip.take() {
                out.push(Lease {
                    name: name.take(),
                    ip,
                });
            }
        } else if let Some(v) = l.strip_prefix("name=") {
            name = Some(v.to_string());
        } else if let Some(v) = l.strip_prefix("ip_address=") {
            ip = v.parse().ok();
        }
    }
    out
}

fn read_arp_table() -> Vec<Ipv4Addr> {
    let out = std::process::Command::new("/usr/sbin/arp")
        .arg("-an")
        .output();
    match out {
        Ok(o) => parse_arp(&String::from_utf8_lossy(&o.stdout)),
        Err(_) => Vec::new(),
    }
}

/// Parse `arp -an` output, extracting the IPv4 addresses in parentheses. Pure; tested.
pub fn parse_arp(text: &str) -> Vec<Ipv4Addr> {
    let mut out = Vec::new();
    for line in text.lines() {
        if let Some(start) = line.find('(')
            && let Some(end) = line[start + 1..].find(')')
        {
            let inside = &line[start + 1..start + 1 + end];
            if let Ok(ip) = inside.parse::<Ipv4Addr>()
                && !out.contains(&ip)
            {
                out.push(ip);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// mDNS via dns-sd (ships on macOS; browsing goes through mDNSResponder)
// ---------------------------------------------------------------------------

/// Browse `_mist._tcp` and resolve each instance to host+port+uuid. Runs the system `dns-sd`
/// tool with a hard timeout (it streams forever otherwise). Returns `[]` when mDNS is blocked —
/// the caller then falls through to the lease/ARP scan (design 11 §10).
pub async fn mdns_browse(timeout: Duration) -> Vec<MdnsInstance> {
    let names = match dns_sd_capture(&["-B", "_mist._tcp"], timeout).await {
        Some(out) => parse_dns_sd_browse(&out),
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for name in names {
        if let Some(cap) = dns_sd_capture(&["-L", &name, "_mist._tcp"], timeout).await
            && let Some(inst) = parse_dns_sd_resolve(&name, &cap)
        {
            out.push(inst);
        }
    }
    out
}

/// Run `dns-sd <args>`, killing it after `timeout`, and return its stdout. `dns-sd` never exits on
/// its own, so the timeout *is* the stop condition.
async fn dns_sd_capture(args: &[&str], timeout: Duration) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let mut child = tokio::process::Command::new("dns-sd")
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(timeout, stdout.read_to_end(&mut buf)).await;
    let _ = child.start_kill();
    let _ = child.wait().await;
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// Extract instance names from `dns-sd -B` output (the `Add` rows). Pure; tested.
pub fn parse_dns_sd_browse(out: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in out.lines() {
        // Columns: Timestamp  A/R Flags if Domain  ServiceType  InstanceName
        // An "Add" row has "Add" in the A/R column.
        let cols: Vec<&str> = line.split_whitespace().collect();
        if let Some(pos) = cols.iter().position(|c| *c == "Add") {
            // InstanceName is everything after the service type; instance names can contain
            // spaces, so re-join the tail after the `_mist._tcp.` column.
            if let Some(svc_idx) = cols.iter().position(|c| c.contains("_mist._tcp")) {
                let name = cols[svc_idx + 1..].join(" ");
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            } else if cols.len() > pos + 3 {
                let name = cols[pos + 4..].join(" ");
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Parse `dns-sd -L` output into host+port+TXT. Pure; tested.
pub fn parse_dns_sd_resolve(instance: &str, out: &str) -> Option<MdnsInstance> {
    let mut host = None;
    let mut port = None;
    let mut vm_uuid = None;
    for line in out.lines() {
        let l = line.trim();
        if let Some(idx) = l.find("can be reached at ") {
            let rest = &l[idx + "can be reached at ".len()..];
            // "<host>.:<port> (interface N)" — take up to the first whitespace, split on last ':'.
            let hostport = rest.split_whitespace().next().unwrap_or("");
            if let Some((h, p)) = hostport.rsplit_once(':') {
                host = Some(h.trim_end_matches('.').to_string());
                port = p.parse::<u16>().ok();
            }
        }
        // TXT lines carry space-separated key=value tokens; pull uuid= if present.
        for tok in l.split_whitespace() {
            if let Some(v) = tok.strip_prefix("uuid=") {
                vm_uuid = mist_proto::vm_uuid_from_hex(v);
            }
        }
    }
    Some(MdnsInstance {
        instance: instance.to_string(),
        host: host?,
        port: port.unwrap_or(TCP_PORT),
        vm_uuid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dhcpd_leases_parse_by_name() {
        let text = "\
{
\tname=darekhta-debian
\tip_address=192.168.64.2
\thw_address=1,a1:b2:c3:d4:e5:f6
\tidentifier=1,a1:b2:c3:d4:e5:f6
\tlease=0x6890abcd
}
{
\tname=other
\tip_address=192.168.64.7
}
";
        let leases = parse_dhcpd_leases(text);
        assert_eq!(leases.len(), 2);
        assert_eq!(leases[0].name.as_deref(), Some("darekhta-debian"));
        assert_eq!(leases[0].ip, Ipv4Addr::new(192, 168, 64, 2));
        assert_eq!(leases[1].ip, Ipv4Addr::new(192, 168, 64, 7));
    }

    #[test]
    fn arp_parse_extracts_ips() {
        let text = "\
? (192.168.64.1) at 0:1:2:3:4:5 on bridge100 ifscope [ethernet]
? (192.168.64.2) at a1:b2:c3:d4:e5:f6 on bridge100 ifscope [ethernet]
? (192.168.64.2) at a1:b2:c3:d4:e5:f6 on bridge100 ifscope [ethernet]
";
        let ips = parse_arp(text);
        assert_eq!(
            ips,
            vec![
                Ipv4Addr::new(192, 168, 64, 1),
                Ipv4Addr::new(192, 168, 64, 2)
            ]
        );
    }

    #[test]
    fn vmnet_subnet_membership() {
        let m = VmnetModel {
            iface: "bridge100".into(),
            gateway: Ipv4Addr::new(192, 168, 64, 1),
            netmask: Ipv4Addr::new(255, 255, 255, 0),
        };
        assert!(m.contains(Ipv4Addr::new(192, 168, 64, 200)));
        assert!(!m.contains(Ipv4Addr::new(192, 168, 65, 1)));
        let cands = m.candidate_ips(1000);
        assert_eq!(cands.len(), 253); // .0 net, .255 bcast, .1 gateway excluded
        assert!(!cands.contains(&m.gateway));
        assert!(cands.contains(&Ipv4Addr::new(192, 168, 64, 2)));
    }

    #[test]
    fn bridge_name_matching() {
        assert!(is_vmnet_bridge_name("bridge100"));
        assert!(is_vmnet_bridge_name("bridge105"));
        assert!(!is_vmnet_bridge_name("bridge0"));
        assert!(!is_vmnet_bridge_name("bridge10"));
        assert!(!is_vmnet_bridge_name("en0"));
        assert!(!is_vmnet_bridge_name("bridge1000"));
    }

    #[test]
    fn dns_sd_browse_parse() {
        let out = "\
DATE: ---Sat 14 Jun 2026---
10:00:00.000  ...STARTING...
Timestamp     A/R    Flags  if Domain   Service Type     Instance Name
10:00:01.234  Add        3   9 local.   _mist._tcp.      darekhta-debian
10:00:01.235  Add        2   9 local.   _mist._tcp.      lab vm two
";
        let names = parse_dns_sd_browse(out);
        assert!(names.contains(&"darekhta-debian".to_string()));
        assert!(names.contains(&"lab vm two".to_string()));
    }

    #[test]
    fn dns_sd_resolve_parse() {
        let out = "\
Lookup darekhta-debian._mist._tcp.local
DATE: ---Sat 14 Jun 2026---
10:00:02.000  darekhta-debian._mist._tcp.local. can be reached at darekhta-debian.local.:6478 (interface 9)
 v=1 uuid=4f2b0b30a9d84f8ab5c40122c7be1c13 tx=tcp shares=3 kver=6.1
";
        let inst = parse_dns_sd_resolve("darekhta-debian", out).unwrap();
        assert_eq!(inst.host, "darekhta-debian.local");
        assert_eq!(inst.port, 6478);
        assert_eq!(
            inst.vm_uuid.map(|u| mist_proto::vm_uuid_hex(&u)).as_deref(),
            Some("4f2b0b30a9d84f8ab5c40122c7be1c13")
        );
    }
}
