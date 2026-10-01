//! The part of the driver's config file (one ConfigMap for the controller and
//! the node) the node reads. Loading follows the Go driver
//! (`pkg/driver/config.go` LoadConfig): `$VAR`/`${VAR}` are expanded first with
//! Go's `os.ExpandEnv` rules, and a protocol block that is present without an
//! `enabled` key counts as enabled. Keys the node does not use are left to the
//! controller, which validates the whole file; the keys read here are validated
//! here.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;

pub const DEFAULT_DRIVER_NAME: &str = "csi.scale.io";
pub const DEFAULT_UBLK_SOCKET: &str = "/run/nvmeublk/nvmeublkd.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataPath {
    Kernel,
    Ublk,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub driver: String,
    pub nfs_enabled: bool,
    pub nfs: NfsConfig,
    pub iscsi_enabled: bool,
    pub nvmeof: NvmeofConfig,
    pub node: NodeConfig,
    pub command_timeouts: CommandTimeouts,
    pub session_gc: SessionGcConfig,
}

/// `nfs`: the keys an NFS mount on the node reads.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NfsConfig {
    /// `nconnect=<n>` on every NFS mount, 1..16; absent: the option is omitted.
    pub nconnect: Option<i64>,
    /// Storage networks whose interface addresses join this node's identity IPs.
    pub node_identity_networks: Vec<String>,
}

/// `sessionGC`: orphaned-session cleanup on the node (Go defaults).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct SessionGcConfig {
    pub enabled: bool,
    /// Seconds; 0 or less takes 300.
    pub interval: i64,
    /// Seconds an orphan must stay orphaned; 0 takes 60.
    pub grace_period: i64,
    pub dry_run: bool,
    /// Absent: true.
    pub run_on_startup: Option<bool>,
    /// Seconds; 0 takes 5.
    pub startup_delay: i64,
    #[serde(rename = "nvmeofEnabled")]
    pub nvmeof_enabled: Option<bool>,
}

impl SessionGcConfig {
    fn seconds(value: i64, default: u64) -> Duration {
        Duration::from_secs(u64::try_from(value).ok().filter(|v| *v > 0).unwrap_or(default))
    }

    pub fn interval(&self) -> Duration {
        Self::seconds(self.interval, 300)
    }

    pub fn grace_period(&self) -> Duration {
        Self::seconds(self.grace_period, 60)
    }

    pub fn startup_delay(&self) -> Duration {
        Self::seconds(self.startup_delay, 5)
    }

    pub fn run_on_startup(&self) -> bool {
        self.run_on_startup.unwrap_or(true)
    }
}

/// `commandTimeouts`, in seconds; 0 takes the default.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct CommandTimeouts {
    pub mount: i64,
    pub format: i64,
    pub iscsi: i64,
    pub nvme: i64,
}

fn seconds_or(value: i64, default: u64) -> Duration {
    Duration::from_secs(u64::try_from(value).ok().filter(|v| *v > 0).unwrap_or(default))
}

impl CommandTimeouts {
    pub fn mount(&self) -> Duration {
        seconds_or(self.mount, 30)
    }

    pub fn format(&self) -> Duration {
        seconds_or(self.format, 300)
    }

    pub fn nvme(&self) -> Duration {
        seconds_or(self.nvme, 30)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NvmeofConfig {
    #[serde(skip)]
    pub enabled: bool,
    pub transport: String,
    pub data_path: String,
    /// Around a volume's subsystem name: `<prefix><share name><suffix>`.
    pub name_prefix: String,
    pub name_suffix: String,
    pub transport_address: String,
    pub transport_service_id: i64,
    /// Multipath across `transportAddress` and `addresses`.
    pub multipath: bool,
    pub addresses: Vec<String>,
    /// Seconds a stage waits for a connected device; 0 takes 60.
    pub device_wait_timeout: i64,
    pub connect: ConnectConfig,
    pub ublk: UblkConfig,
}

/// `nvmeof.connect`: knobs for `nvme connect`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ConnectConfig {
    /// Seconds. 0: 15 with multipath, omitted without; negative: omitted.
    #[serde(rename = "fastIOFailTmo")]
    pub fast_io_fail_tmo: i64,
    #[serde(rename = "nrIOQueues")]
    pub nr_io_queues: Option<i64>,
    pub nr_write_queues: Option<i64>,
    pub keep_alive_tmo: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct UblkConfig {
    pub enabled: bool,
    pub socket_path: String,
    pub queues: u32,
    pub depth: u32,
    pub zero_copy: bool,
    pub napi_us: u32,
    pub max_volumes_per_node: u32,
    /// Seconds.
    pub attach_timeout: u64,
}

impl Default for UblkConfig {
    fn default() -> Self {
        UblkConfig {
            enabled: false,
            socket_path: DEFAULT_UBLK_SOCKET.to_string(),
            queues: 0,
            depth: 0,
            zero_copy: true,
            napi_us: 200,
            max_volumes_per_node: 32,
            attach_timeout: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct NodeConfig {
    pub max_volumes_per_node: i64,
    /// Milliseconds a stage waits for a disconnected session to clear; 0 takes 500.
    pub session_cleanup_delay: i64,
}

impl NodeConfig {
    pub fn session_cleanup_delay(&self) -> Duration {
        Duration::from_millis(
            u64::try_from(self.session_cleanup_delay)
                .ok()
                .filter(|v| *v > 0)
                .unwrap_or(500),
        )
    }
}

impl NvmeofConfig {
    /// The configured portal addresses when multipath is on (transportAddress
    /// first, trimmed, duplicates dropped); empty without multipath.
    pub fn multipath_addresses(&self) -> Vec<String> {
        if !self.multipath {
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::new();
        for address in std::iter::once(&self.transport_address).chain(&self.addresses) {
            let address = address.trim();
            if !address.is_empty() && !out.iter().any(|a| a == address) {
                out.push(address.to_string());
            }
        }
        out
    }

    pub fn device_wait_timeout(&self) -> Duration {
        Duration::from_secs(
            u64::try_from(self.device_wait_timeout)
                .ok()
                .filter(|v| *v > 0)
                .unwrap_or(60),
        )
    }

    /// The install-wide default data path; anything but "ublk" is the kernel's.
    pub fn default_data_path(&self) -> DataPath {
        if self.data_path.trim().eq_ignore_ascii_case("ublk") {
            DataPath::Ublk
        } else {
            DataPath::Kernel
        }
    }

    /// Whether volumes may be staged through nvmeublkd at all.
    pub fn ublk_available(&self) -> bool {
        self.ublk.enabled || self.default_data_path() == DataPath::Ublk
    }
}

impl Config {
    /// NodeGetInfo's max_volumes_per_node, 0 for none: `node.maxVolumesPerNode`,
    /// else the ublk budget when ublk is the default data path with zero copy
    /// (the daemon's buffer tables are what bound it).
    pub fn node_volume_limit(&self) -> i64 {
        if self.node.max_volumes_per_node > 0 {
            return self.node.max_volumes_per_node;
        }
        if self.nvmeof.enabled && self.nvmeof.default_data_path() == DataPath::Ublk && self.nvmeof.ublk.zero_copy {
            return i64::from(self.nvmeof.ublk.max_volumes_per_node);
        }
        0
    }
}

pub fn load(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    parse(&text, |name| std::env::var(name).ok())
}

pub fn parse(text: &str, env: impl Fn(&str) -> Option<String>) -> Result<Config> {
    let expanded = expand_env(text, env);
    let document: Value = serde_saphyr::from_str(&expanded).context("parse config YAML")?;
    let Value::Object(root) = &document else {
        bail!("config is not a mapping")
    };
    let section = |key: &str| {
        root.get(key)
            .cloned()
            .filter(|v| !v.is_null())
            .unwrap_or(Value::Object(Default::default()))
    };
    // Present without `enabled`: enabled (the blocks predate the key).
    let enabled = |key: &str| -> Result<bool> {
        match root.get(key) {
            None => Ok(false),
            Some(Value::Object(block)) => match block.get("enabled") {
                None => Ok(true),
                Some(v) => v.as_bool().with_context(|| format!("{key}.enabled must be a boolean")),
            },
            Some(Value::Null) => Ok(true),
            Some(_) => bail!("{key} must be a mapping"),
        }
    };
    let mut nvmeof: NvmeofConfig = serde_json::from_value(section("nvmeof")).context("config nvmeof")?;
    nvmeof.enabled = enabled("nvmeof")?;
    let nfs: NfsConfig = serde_json::from_value(section("nfs")).context("config nfs")?;
    let node: NodeConfig = serde_json::from_value(section("node")).context("config node")?;
    let command_timeouts: CommandTimeouts =
        serde_json::from_value(section("commandTimeouts")).context("config commandTimeouts")?;
    let session_gc: SessionGcConfig = serde_json::from_value(section("sessionGC")).context("config sessionGC")?;
    let driver = root
        .get("driver")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let config = Config {
        driver,
        nfs_enabled: enabled("nfs")?,
        nfs,
        iscsi_enabled: enabled("iscsi")?,
        nvmeof,
        node,
        command_timeouts,
        session_gc,
    };
    validate(&config)?;
    Ok(config)
}

fn validate(config: &Config) -> Result<()> {
    if let Some(n) = config.nfs.nconnect
        && !(1..=16).contains(&n)
    {
        bail!("nfs.nconnect must be between 1 and 16 (got {n})");
    }
    crate::discovery::parse_identity_networks(&config.nfs.node_identity_networks).map_err(anyhow::Error::msg)?;
    let n = &config.nvmeof;
    match n.data_path.trim().to_ascii_lowercase().as_str() {
        "" | "kernel" | "ublk" => {}
        other => bail!("nvmeof.dataPath {other:?} is not kernel or ublk"),
    }
    if n.ublk_available() {
        if !n.transport.trim().is_empty() && !n.transport.trim().eq_ignore_ascii_case("tcp") {
            bail!("the ublk data path needs nvmeof.transport tcp");
        }
        if !(1..=128).contains(&n.ublk.max_volumes_per_node) {
            bail!("nvmeof.ublk.maxVolumesPerNode must be 1..128");
        }
        if !Path::new(&n.ublk.socket_path).is_absolute() {
            bail!("nvmeof.ublk.socketPath must be absolute");
        }
        if n.ublk.queues > 4096 || n.ublk.depth > 4096 {
            bail!("nvmeof.ublk.queues and depth must be 0..4096");
        }
        if n.ublk.napi_us > 1_000_000 {
            bail!("nvmeof.ublk.napiUs must be 0..1000000");
        }
    }
    for (name, value) in [
        ("nvmeof.connect.nrIOQueues", n.connect.nr_io_queues),
        ("nvmeof.connect.nrWriteQueues", n.connect.nr_write_queues),
        ("nvmeof.connect.keepAliveTmo", n.connect.keep_alive_tmo),
    ] {
        if value.is_some_and(|v| v < 1) {
            bail!("{name} must be at least 1");
        }
    }
    if n.device_wait_timeout < 0 {
        bail!("nvmeof.deviceWaitTimeout must not be negative");
    }
    for (name, value) in [
        ("sessionGC.interval", config.session_gc.interval),
        ("sessionGC.gracePeriod", config.session_gc.grace_period),
    ] {
        if value < 0 {
            bail!("{name} must not be negative");
        }
    }
    if config.node.session_cleanup_delay < 0 {
        bail!("node.sessionCleanupDelay must not be negative");
    }
    let t = &config.command_timeouts;
    for (name, value) in [
        ("mount", t.mount),
        ("format", t.format),
        ("iscsi", t.iscsi),
        ("nvme", t.nvme),
    ] {
        if value < 0 {
            bail!("commandTimeouts.{name} must not be negative");
        }
    }
    if config.node.max_volumes_per_node < 0 {
        bail!("node.maxVolumesPerNode must not be negative");
    }
    Ok(())
}

/// Go's `os.ExpandEnv`: `${name}`, `$name` (letters, digits, `_`) and the
/// one-character shell specials; an unset variable expands to nothing; `${}` and
/// an unclosed `${` are dropped; a `$` followed by anything else stays.
pub fn expand_env(text: &str, env: impl Fn(&str) -> Option<String>) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let mut j = 0;
    while j < b.len() {
        if b[j] == b'$' && j + 1 < b.len() {
            out.push_str(&text[i..j]);
            let (name, width) = shell_name(&text[j + 1..]);
            if name.is_empty() && width > 0 {
                // invalid syntax: dropped
            } else if name.is_empty() {
                out.push('$');
            } else {
                out.push_str(&env(name).unwrap_or_default());
            }
            j += width;
            i = j + 1;
        }
        j += 1;
    }
    out.push_str(&text[i.min(text.len())..]);
    out
}

fn is_special(c: u8) -> bool {
    matches!(c, b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9')
}

fn shell_name(s: &str) -> (&str, usize) {
    let b = s.as_bytes();
    if b[0] == b'{' {
        if b.len() > 2 && is_special(b[1]) && b[2] == b'}' {
            return (&s[1..2], 3);
        }
        for (i, c) in b.iter().enumerate().skip(1) {
            if *c == b'}' {
                if i == 1 {
                    return ("", 2);
                }
                return (&s[1..i], i + 1);
            }
        }
        return ("", 1);
    }
    if is_special(b[0]) {
        return (&s[0..1], 1);
    }
    let n = b
        .iter()
        .take_while(|c| c.is_ascii_alphanumeric() || **c == b'_')
        .count();
    (&s[..n], n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        match name {
            "HOME" => Some("/root".into()),
            "A_1" => Some("x".into()),
            "1" => Some("one".into()),
            _ => None,
        }
    }

    #[test]
    fn env_expansion_follows_go() {
        // Go's os.Expand with the same mapping gives exactly these.
        let cases = [
            ("$HOME/x", "/root/x"),
            ("${HOME}/x", "/root/x"),
            ("${A_1}y", "xy"),
            ("$A_1-z", "x-z"),
            ("$UNSET.", "."),
            ("${UNSET}", ""),
            ("cost: 5$", "cost: 5$"),
            ("a $ b", "a $ b"),
            ("${}", ""),
            ("${HOME", "HOME"),
            ("$1st", "onest"),
            ("${1}", "one"),
            ("$$", ""),
            ("100%$", "100%$"),
            ("é$HOMEé", "é/rooté"),
            ("${é}", ""),
            ("$é", "$é"),
            ("${é", "é"),
            ("x${*}y", "xy"),
            ("$-", ""),
            ("${HOME}${A_1}$", "/rootx$"),
        ];
        for (input, want) in cases {
            assert_eq!(expand_env(input, env), want, "{input:?}");
        }
    }

    #[test]
    fn protocols_present_without_enabled_are_enabled() {
        let c = parse(
            "nfs: {shareHost: x}\niscsi:\n  enabled: false\nnvmeof:\n  transportAddress: 1.2.3.4\n",
            env,
        )
        .unwrap();
        assert!(c.nfs_enabled && !c.iscsi_enabled && c.nvmeof.enabled);
        let c = parse("driver: csi.scale.io\n", env).unwrap();
        assert!(!c.nfs_enabled && !c.iscsi_enabled && !c.nvmeof.enabled);
        let c = parse("nfs:\n", env).unwrap();
        assert!(c.nfs_enabled, "an empty block is present");
    }

    #[test]
    fn the_node_volume_limit() {
        let limit = |yaml: &str| parse(yaml, env).unwrap().node_volume_limit();
        assert_eq!(
            limit("nvmeof: {dataPath: kernel, ublk: {enabled: true, maxVolumesPerNode: 64}}"),
            0
        );
        assert_eq!(limit("nvmeof: {dataPath: ublk}"), 32);
        assert_eq!(limit("nvmeof: {dataPath: UBLK, ublk: {maxVolumesPerNode: 64}}"), 64);
        assert_eq!(
            limit("nvmeof: {dataPath: ublk, ublk: {zeroCopy: false}}"),
            0,
            "no buffer tables, no bound"
        );
        assert_eq!(limit("nvmeof: {dataPath: ublk}\nnode: {maxVolumesPerNode: 7}"), 7);
        assert_eq!(limit("nvmeof: {enabled: false, dataPath: ublk}"), 0);
    }

    #[test]
    fn the_production_shape_loads() {
        let yaml = "driver: csi.scale.io\ntruenas:\n  host: ${TRUENAS_HOST}\n  apiKey: ${TRUENAS_API_KEY}\nnvmeof:\n  enabled: true\n  transport: \"tcp\"\n  multipath: true\n  addresses: [192.168.201.10, 192.168.202.10]\n  connect: {nrIOQueues: 8}\n  dataPath: \"kernel\"\n  ublk: {enabled: true, queues: 0, depth: 0, zeroCopy: true, napiUs: 200, maxVolumesPerNode: 64, attachTimeout: 60}\nnode: {sessionCleanupDelay: 500, maxVolumesPerNode: 0}\n";
        let c = parse(yaml, env).unwrap();
        assert_eq!(c.driver, "csi.scale.io");
        assert!(c.nvmeof.enabled && c.nvmeof.ublk_available());
        assert_eq!(c.nvmeof.default_data_path(), DataPath::Kernel);
        assert_eq!(c.nvmeof.ublk.max_volumes_per_node, 64);
        assert_eq!(c.node_volume_limit(), 0);
    }

    #[test]
    fn keys_the_node_uses_are_validated() {
        for bad in [
            "nvmeof: {dataPath: spdk}",
            "nvmeof: {dataPath: ublk, transport: rdma}",
            "nvmeof: {dataPath: ublk, ublk: {maxVolumesPerNode: 129}}",
            "nvmeof: {ublk: {enabled: true, socketPath: relative.sock}}",
            "nvmeof: {enabled: maybe}",
            "node: {maxVolumesPerNode: -1}",
            "nvmeof: {ublk: {enabled: true, depth: 4097}}",
            "- a list",
        ] {
            assert!(parse(bad, env).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn the_nfs_keys() {
        let c = parse("nfs: {shareHost: x}\n", env).unwrap();
        assert_eq!(c.nfs.nconnect, None);
        assert!(c.nfs.node_identity_networks.is_empty());
        let c = parse(
            "nfs:\n  shareHost: x\n  nconnect: 4\n  nodeIdentityNetworks: [192.168.201.0/24, \"fd00:201::/64\"]\n",
            env,
        )
        .unwrap();
        assert_eq!(c.nfs.nconnect, Some(4));
        assert_eq!(c.nfs.node_identity_networks, ["192.168.201.0/24", "fd00:201::/64"]);
        for bad in [
            "nfs: {nconnect: 0}",
            "nfs: {nconnect: 17}",
            "nfs: {nodeIdentityNetworks: [192.168.201.0/33]}",
            "nfs: {nodeIdentityNetworks: [nas01]}",
        ] {
            assert!(parse(bad, env).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn command_timeouts() {
        let config = parse("nvmeof: {}\n", |_| None).unwrap();
        let t = &config.command_timeouts;
        assert_eq!(
            (t.mount(), t.format(), t.nvme()),
            (
                Duration::from_secs(30),
                Duration::from_secs(300),
                Duration::from_secs(30)
            )
        );
        let config = parse("commandTimeouts:\n  mount: 45\n  format: 600\n  nvme: 10\n", |_| None).unwrap();
        let t = &config.command_timeouts;
        assert_eq!(
            (t.mount(), t.format(), t.nvme()),
            (
                Duration::from_secs(45),
                Duration::from_secs(600),
                Duration::from_secs(10)
            )
        );
        let err = parse("commandTimeouts:\n  format: -1\n", |_| None).unwrap_err();
        assert!(
            format!("{err:#}").contains("commandTimeouts.format must not be negative"),
            "{err:#}"
        );
    }
}
