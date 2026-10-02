//! This node's identity, discovered once at startup the way the Go node does
//! (`discoverNodeIdentity`, `pkg/driver/node_identity.go`): the NVMe host NQN
//! from `nvme show-hostnqn` (else `/etc/nvme/hostnqn`), the iSCSI IQN from
//! `/etc/iscsi/initiatorname.iscsi`, the IPs from `NODE_IP`/`NODE_IPS` (the
//! chart sets status.hostIP) or else the host's interface addresses, plus,
//! with `nfs.nodeIdentityNetworks`, every interface address inside one of
//! those storage networks (Go `nodeIdentityIPs`). The vectors in
//! `pkg/driver/testdata/node_identity_vectors.json` hold both to the same
//! result.

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use log::warn;

use crate::exec;
use crate::node_id::{self, ISCSI_DENY_ALL_SENTINEL_IQN, NodeIdentity, canonical_ips};

type EnvLookup = Box<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Where discovery reads from; the tests point it at fixtures.
pub struct Sources {
    pub root: PathBuf,
    pub env: EnvLookup,
    pub nvme_timeout: Duration,
    pub run_nvme: bool,
    pub interface_ips: Box<dyn Fn() -> Vec<IpAddr> + Send + Sync>,
}

impl Sources {
    pub fn host(nvme_timeout: Duration) -> Self {
        Sources {
            root: PathBuf::from("/"),
            env: Box::new(|name| std::env::var(name).ok()),
            nvme_timeout,
            run_nvme: true,
            interface_ips: Box::new(interface_ips),
        }
    }
}

pub async fn discover(name: &str, sources: &Sources, networks: &[IdentityNetwork]) -> NodeIdentity {
    let mut identity = NodeIdentity {
        name: name.to_string(),
        ..Default::default()
    };
    if sources.run_nvme {
        let limits = exec::Limits {
            timeout: sources.nvme_timeout,
            rpc_deadline: None,
        };
        match exec::run("nvme", &["show-hostnqn"], limits, true).await {
            Ok(out) if out.success() => identity.nvme_nqn = String::from_utf8_lossy(&out.stdout).trim().to_string(),
            Ok(out) => warn!("nvme show-hostnqn failed ({:?}): {}", out.code, out.combined().trim()),
            Err(e) => warn!("nvme show-hostnqn: {e}"),
        }
    }
    if identity.nvme_nqn.is_empty()
        && let Ok(text) = std::fs::read_to_string(sources.root.join("etc/nvme/hostnqn"))
    {
        identity.nvme_nqn = text.trim().to_string();
    }
    if let Ok(text) = std::fs::read_to_string(sources.root.join("etc/iscsi/initiatorname.iscsi")) {
        for line in text.split('\n') {
            if let Some((key, value)) = line.trim().split_once('=')
                && key.trim().eq_ignore_ascii_case("InitiatorName")
            {
                identity.iscsi_iqn = value.trim().to_string();
                break;
            }
        }
    }
    if identity.iscsi_iqn == ISCSI_DENY_ALL_SENTINEL_IQN {
        // Never this node's IQN; carried as a flag so the controller refuses
        // iSCSI publication instead of treating it as a missing IQN.
        warn!(
            "node {name} reported the reserved iSCSI fencing identity {ISCSI_DENY_ALL_SENTINEL_IQN:?}; ignoring it as a node IQN"
        );
        identity.iscsi_iqn.clear();
        identity.iscsi_reported_sentinel = true;
    }
    let env = |name: &str| (sources.env)(name).unwrap_or_default();
    identity.ips = identity_ips(
        &env("NODE_IP"),
        &env("NODE_IPS"),
        || (sources.interface_ips)(),
        networks,
    );
    identity
}

/// The identity's IPs (Go `nodeIdentityIPs`): those in `NODE_IP`/`NODE_IPS`,
/// or every interface address when they hold none (status.hostIP is stable;
/// interface enumeration is the fallback), plus every interface address inside
/// one of `networks`. With no networks the interfaces are read only for the
/// fallback, so the default node id is what it always was.
pub fn identity_ips(
    node_ip: &str,
    node_ips: &str,
    interface_ips: impl FnOnce() -> Vec<IpAddr>,
    networks: &[IdentityNetwork],
) -> Vec<IpAddr> {
    let mut ips: Vec<IpAddr> = [node_ip, node_ips]
        .iter()
        .flat_map(|value| value.split([',', ' ']))
        .filter(|s| !s.is_empty())
        .filter_map(parse_go_ip)
        .collect();
    let interfaces = if ips.is_empty() || !networks.is_empty() {
        interface_ips()
    } else {
        Vec::new()
    };
    if ips.is_empty() {
        ips.extend(interfaces.iter().copied());
    }
    if !networks.is_empty() {
        ips.extend(
            canonical_ips(&interfaces)
                .into_iter()
                .filter(|ip| networks_contain(networks, ip)),
        );
    }
    canonical_ips(&ips)
}

/// Most entries `nfs.nodeIdentityNetworks` may hold.
pub const MAX_IDENTITY_NETWORKS: usize = 16;

/// One `nfs.nodeIdentityNetworks` entry: a CIDR, or a single address that
/// matches only itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityNetwork {
    Cidr(IpAddr, u8),
    Address(IpAddr),
}

impl IdentityNetwork {
    /// Whether a canonical identity IP is in it: an IPv4 network holds only
    /// IPv4 addresses, an IPv6 one only IPv6.
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match (self, ip) {
            (IdentityNetwork::Address(a), ip) => a == ip,
            (IdentityNetwork::Cidr(IpAddr::V4(net), bits), IpAddr::V4(ip)) => {
                let mask = u32::MAX.checked_shl(32 - u32::from(*bits)).unwrap_or(0);
                u32::from(*net) & mask == u32::from(*ip) & mask
            }
            (IdentityNetwork::Cidr(IpAddr::V6(net), bits), IpAddr::V6(ip)) => {
                let mask = u128::MAX.checked_shl(128 - u32::from(*bits)).unwrap_or(0);
                u128::from(*net) & mask == u128::from(*ip) & mask
            }
            _ => false,
        }
    }
}

pub fn networks_contain(networks: &[IdentityNetwork], ip: &IpAddr) -> bool {
    networks.iter().any(|n| n.contains(ip))
}

/// Validates `nfs.nodeIdentityNetworks` as the Go driver does (net.ParseCIDR
/// or net.ParseIP, after trimming): no zone, a decimal prefix within the
/// family's width, and no IPv4-mapped IPv6 network (write the IPv4 form).
pub fn parse_identity_networks(entries: &[String]) -> Result<Vec<IdentityNetwork>, String> {
    if entries.len() > MAX_IDENTITY_NETWORKS {
        return Err(format!(
            "nfs.nodeIdentityNetworks must contain at most {MAX_IDENTITY_NETWORKS} entries (got {})",
            entries.len()
        ));
    }
    let mut out = Vec::with_capacity(entries.len());
    for (i, entry) in entries.iter().enumerate() {
        let invalid = || format!("nfs.nodeIdentityNetworks[{i}] {entry:?} is not a CIDR or an IP address");
        let value = entry.trim();
        if let Some((address, bits)) = value.split_once('/') {
            let ip: IpAddr = address.parse().map_err(|_| invalid())?;
            // Go's dtoi: decimal digits only, leading zeros allowed.
            if bits.is_empty() || !bits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid());
            }
            let width = if ip.is_ipv4() { 32 } else { 128 };
            let bits = bits
                .bytes()
                .try_fold(0u32, |n, b| n.checked_mul(10)?.checked_add(u32::from(b - b'0')))
                .filter(|n| *n <= width)
                .ok_or_else(invalid)?;
            if let IpAddr::V6(v6) = ip
                && v6.to_ipv4_mapped().is_some()
            {
                return Err(format!(
                    "nfs.nodeIdentityNetworks[{i}] {entry:?} is an IPv4-mapped IPv6 network; write it as an IPv4 CIDR"
                ));
            }
            out.push(IdentityNetwork::Cidr(ip, bits as u8));
            continue;
        }
        let ip = parse_go_ip(value).ok_or_else(invalid)?;
        let ip = match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
            v4 => v4,
        };
        out.push(IdentityNetwork::Address(ip));
    }
    Ok(out)
}

/// The identity-network addresses the CSI 256-byte limit left out of
/// `node_id` (Go `nodeIdentityDroppedIPs`): the controller cannot grant them.
pub fn dropped_ips(identity: &NodeIdentity, networks: &[IdentityNetwork], node_id: &str) -> Vec<IpAddr> {
    if networks.is_empty() || identity.ips.is_empty() {
        return Vec::new();
    }
    let Ok(encoded) = node_id::parse(node_id) else {
        return Vec::new();
    };
    canonical_ips(&identity.ips)
        .into_iter()
        .filter(|ip| !encoded.ips.contains(ip) && networks_contain(networks, ip))
        .collect()
}

/// Go's net.ParseIP: dotted IPv4 or IPv6 text, no zone, no brackets.
fn parse_go_ip(text: &str) -> Option<IpAddr> {
    text.parse().ok()
}

fn interface_ips() -> Vec<IpAddr> {
    let mut ips = Vec::new();
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs allocates a list we walk read-only and free once.
    unsafe {
        if libc::getifaddrs(&mut head) != 0 {
            return ips;
        }
        let mut cur = head;
        while !cur.is_null() {
            let addr = (*cur).ifa_addr;
            if !addr.is_null() {
                match i32::from((*addr).sa_family) {
                    libc::AF_INET => {
                        let sin = &*(addr as *const libc::sockaddr_in);
                        ips.push(IpAddr::from(u32::from_be(sin.sin_addr.s_addr).to_be_bytes()));
                    }
                    libc::AF_INET6 => {
                        let sin6 = &*(addr as *const libc::sockaddr_in6);
                        ips.push(IpAddr::from(sin6.sin6_addr.s6_addr));
                    }
                    _ => {}
                }
            }
            cur = (*cur).ifa_next;
        }
        libc::freeifaddrs(head);
    }
    ips
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sources(root: &std::path::Path, env: &'static [(&'static str, &'static str)]) -> Sources {
        Sources {
            root: root.to_path_buf(),
            env: Box::new(move |name| env.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())),
            nvme_timeout: Duration::from_secs(1),
            run_nvme: false,
            interface_ips: Box::new(|| vec!["127.0.0.1".parse().unwrap(), "198.51.100.7".parse().unwrap()]),
        }
    }

    fn write(root: &std::path::Path, path: &str, text: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[tokio::test]
    async fn files_and_env() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "etc/nvme/hostnqn", "nqn.2014-08.org.nvmexpress:uuid:abc\n");
        write(
            dir.path(),
            "etc/iscsi/initiatorname.iscsi",
            "## comment\n initiatorname = iqn.2004-10.com.example:node \nInitiatorName=second\n",
        );
        let id = discover(
            "k8s-0",
            &sources(
                dir.path(),
                &[("NODE_IP", "192.0.2.10"), ("NODE_IPS", "192.0.2.11, 2001:db8::1,bogus")],
            ),
            &[],
        )
        .await;
        assert_eq!(id.nvme_nqn, "nqn.2014-08.org.nvmexpress:uuid:abc");
        assert_eq!(
            id.iscsi_iqn, "iqn.2004-10.com.example:node",
            "first InitiatorName line, key case-insensitive, trimmed"
        );
        let ips: Vec<String> = id.ips.iter().map(|i| i.to_string()).collect();
        assert_eq!(ips, ["192.0.2.10", "192.0.2.11", "2001:db8::1"]);
    }

    #[tokio::test]
    async fn interfaces_only_without_env() {
        let dir = tempfile::tempdir().unwrap();
        let id = discover("k8s-0", &sources(dir.path(), &[]), &[]).await;
        assert_eq!(
            id.ips,
            vec!["198.51.100.7".parse::<IpAddr>().unwrap()],
            "loopback dropped"
        );
        assert!(id.nvme_nqn.is_empty() && id.iscsi_iqn.is_empty());
    }

    #[tokio::test]
    async fn the_sentinel_iqn_becomes_a_flag() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "etc/iscsi/initiatorname.iscsi",
            &format!("InitiatorName={ISCSI_DENY_ALL_SENTINEL_IQN}\n"),
        );
        let id = discover("k8s-0", &sources(dir.path(), &[]), &[]).await;
        assert!(id.iscsi_iqn.is_empty() && id.iscsi_reported_sentinel);
    }

    #[tokio::test]
    async fn identity_networks_add_the_fabric_address() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = sources(dir.path(), &[("NODE_IP", "198.51.100.7")]);
        sources.interface_ips = Box::new(|| {
            ["198.51.100.7", "192.168.201.21", "10.244.1.7", "fe80::1"]
                .iter()
                .map(|s| s.parse().unwrap())
                .collect()
        });
        let networks = parse_identity_networks(&["192.168.201.0/24".into(), "fe80::/10".into()]).unwrap();
        let id = discover("k8s-0", &sources, &networks).await;
        let ips: Vec<String> = id.ips.iter().map(|i| i.to_string()).collect();
        assert_eq!(ips, ["192.168.201.21", "198.51.100.7"]);
        let id = discover("k8s-0", &sources, &[]).await;
        assert_eq!(
            id.ips,
            vec!["198.51.100.7".parse::<IpAddr>().unwrap()],
            "the default is unchanged"
        );
    }
}
