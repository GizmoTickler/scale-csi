//! This node's identity, discovered once at startup the way the Go node does
//! (`discoverNodeIdentity`, `pkg/driver/node_identity.go`): the NVMe host NQN
//! from `nvme show-hostnqn` (else `/etc/nvme/hostnqn`), the iSCSI IQN from
//! `/etc/iscsi/initiatorname.iscsi`, the IPs from `NODE_IP`/`NODE_IPS` (the
//! chart sets status.hostIP) or else the host's interface addresses.

use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use log::warn;

use crate::exec;
use crate::node_id::{ISCSI_DENY_ALL_SENTINEL_IQN, NodeIdentity, canonical_ips};

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

pub async fn discover(name: &str, sources: &Sources) -> NodeIdentity {
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
    for var in ["NODE_IP", "NODE_IPS"] {
        if let Some(value) = (sources.env)(var) {
            identity.ips.extend(
                value
                    .split([',', ' '])
                    .filter(|s| !s.is_empty())
                    .filter_map(parse_go_ip),
            );
        }
    }
    // status.hostIP is stable; interface enumeration is the fallback.
    if identity.ips.is_empty() {
        identity.ips = (sources.interface_ips)();
    }
    identity.ips = canonical_ips(&identity.ips);
    identity
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
        let id = discover("k8s-0", &sources(dir.path(), &[])).await;
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
        let id = discover("k8s-0", &sources(dir.path(), &[])).await;
        assert!(id.iscsi_iqn.is_empty() && id.iscsi_reported_sentinel);
    }
}
