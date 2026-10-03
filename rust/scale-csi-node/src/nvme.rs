//! The kernel NVMe-oF initiator, through nvme-cli and sysfs, as the Go node
//! drives it (`pkg/util/nvme.go`):
//!
//! - `nvme connect` always asks the kernel to reconnect forever
//!   (`--reconnect-delay=10 --ctrl-loss-tmo=-1`), with `--fast_io_fail_tmo` and
//!   the queue and keep-alive knobs only when the caller sets them;
//! - output text ("already connected", "not found") is read only from a command
//!   that exited on its own: a wedged command's output is never trusted;
//! - a disconnect is retried on busy and timeout errors, three attempts;
//! - a subsystem's device is found through sysfs (namespaces directly under the
//!   subsystem or under a controller) or, after a fresh connect, through the
//!   controllers `nvme list-subsys` names. More than one namespace is an error:
//!   with no NSID to choose by, picking one could hand a pod another volume.
//!   (The Go node swallows that error inside its wait loop and times out after
//!   the full device timeout; here it fails at once.)
//! - a namespace's /dev node is taken only when it is a block device whose
//!   number matches the namespace's own sysfs `dev` file: right after a
//!   disconnect and a new connect (a handover between the plugins) /dev can
//!   still hold the previous namespace's node of that name, and the wait keeps
//!   polling until it is current.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use log::debug;
use serde_json::Value;

use crate::blockdev::{self, BlockDeviceNumber};
use crate::exec::{Limits, Output};
use crate::mount::Runner;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NvmePath {
    /// The controller, e.g. `nvme3`.
    pub name: String,
    pub transport: String,
    /// `traddr=192.0.2.10,trsvcid=4420,...`
    pub address: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Subsystem {
    pub nqn: String,
    /// `nvme-subsys3`.
    pub name: String,
    pub paths: Vec<NvmePath>,
}

/// A JSON object field, by exact key or (as Go's decoder does) any case.
fn field<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a Value> {
    object
        .get(key)
        .or_else(|| object.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v))
        .filter(|v| !v.is_null())
}

fn text(object: &serde_json::Map<String, Value>, key: &str) -> String {
    field(object, key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn subsystem_of(value: &Value) -> Option<Subsystem> {
    let object = value.as_object()?;
    let paths = field(object, "Paths")
        .and_then(Value::as_array)
        .map(|paths| {
            paths
                .iter()
                .filter_map(Value::as_object)
                .map(|p| NvmePath {
                    name: text(p, "Name"),
                    transport: text(p, "Transport"),
                    address: text(p, "Address"),
                    state: text(p, "State"),
                })
                .collect()
        })
        .unwrap_or_default();
    Some(Subsystem {
        nqn: text(object, "NQN"),
        name: text(object, "Name"),
        paths,
    })
}

fn all_named(subsystems: &[Subsystem]) -> bool {
    subsystems.iter().all(|s| !s.nqn.trim().is_empty())
}

/// `nvme list-subsys -o json`, in any of its shapes: nvme-cli 2.x's array of
/// hosts, 1.x's `{"Subsystems": [...]}`, or a bare array of subsystems. An entry
/// without an NQN means the shape was misread, not a real subsystem.
pub fn parse_list_subsys(output: &[u8]) -> Result<Vec<Subsystem>> {
    let value: Value = serde_json::from_slice(output).context("nvme list-subsys output is not JSON")?;
    let list = |v: &Value| -> Option<Vec<Subsystem>> { v.as_array()?.iter().map(subsystem_of).collect() };
    match &value {
        Value::Array(items) => {
            if items.is_empty() {
                return Ok(Vec::new());
            }
            let hosts: Vec<&Value> = items
                .iter()
                .filter_map(|item| item.as_object().and_then(|o| field(o, "Subsystems")))
                .collect();
            if !hosts.is_empty() {
                let mut subsystems = Vec::new();
                for host in hosts {
                    subsystems.extend(list(host).unwrap_or_default());
                }
                if all_named(&subsystems) {
                    return Ok(subsystems);
                }
            }
            if let Some(subsystems) = list(&value)
                && all_named(&subsystems)
            {
                return Ok(subsystems);
            }
        }
        Value::Object(object) => {
            if let Some(subsystems) = field(object, "Subsystems").and_then(list)
                && all_named(&subsystems)
            {
                return Ok(subsystems);
            }
        }
        _ => {}
    }
    bail!("unrecognized nvme list-subsys JSON shape")
}

/// One `key=value` field of a path's address (`traddr`, `trsvcid`, ...).
pub fn path_field(address: &str, wanted: &str) -> Option<String> {
    address
        .split([',', ' ', '\t', '\n', '\r'])
        .filter(|f| !f.is_empty())
        .find_map(|f| {
            f.split_once('=')
                .filter(|(k, _)| *k == wanted)
                .map(|(_, v)| v.to_string())
        })
}

/// The transport addresses of the subsystem's live controllers, deduplicated
/// in listing order.
pub fn live_addresses(nqn: &str, subsystems: &[Subsystem]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for subsystem in subsystems.iter().filter(|s| s.nqn == nqn) {
        for path in &subsystem.paths {
            if !path.state.trim().eq_ignore_ascii_case("live") {
                continue;
            }
            if let Some(address) = path_field(&path.address, "traddr")
                && !out.contains(&address)
            {
                out.push(address);
            }
        }
    }
    out
}

pub fn has_subsystem(nqn: &str, subsystems: &[Subsystem]) -> bool {
    subsystems.iter().any(|s| s.nqn == nqn)
}

/// `nvme connect`'s optional knobs. `None` omits a flag.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectOptions {
    /// Seconds; `None` omits `--fast_io_fail_tmo`.
    pub fast_io_fail_tmo: Option<u64>,
    pub nr_io_queues: Option<u32>,
    pub nr_write_queues: Option<u32>,
    /// Seconds.
    pub keep_alive_tmo: Option<u64>,
}

pub const TRANSPORTS: [&str; 3] = ["tcp", "rdma", "fc"];

/// One controller to connect: a subsystem through one portal.
#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub transport: &'a str,
    /// An address, IPv6 without brackets.
    pub host: &'a str,
    pub port: &'a str,
    pub nqn: &'a str,
}

/// The exact `nvme connect` argv the Go node runs.
pub fn connect_args(transport: &str, host: &str, port: &str, nqn: &str, options: &ConnectOptions) -> Vec<String> {
    let mut args: Vec<String> = [
        "connect",
        "-t",
        transport,
        "-n",
        nqn,
        "-a",
        host,
        "-s",
        port,
        "--reconnect-delay=10",
        "--ctrl-loss-tmo=-1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    if let Some(v) = options.fast_io_fail_tmo {
        args.push(format!("--fast_io_fail_tmo={v}"));
    }
    if let Some(v) = options.nr_io_queues {
        args.push(format!("--nr-io-queues={v}"));
    }
    if let Some(v) = options.nr_write_queues {
        args.push(format!("--nr-write-queues={v}"));
    }
    if let Some(v) = options.keep_alive_tmo {
        args.push(format!("--keep-alive-tmo={v}"));
    }
    args
}

/// `nvmeN` from a namespace device path `.../nvmeNnM` (not a partition).
pub fn controller_of(device: &str) -> Option<&str> {
    let name = Path::new(device).file_name()?.to_str()?;
    let rest = name.strip_prefix("nvme")?;
    let (ctrl, ns) = rest.split_once('n')?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    (digits(ctrl) && digits(ns)).then(|| &name[..4 + ctrl.len()])
}

/// A controller name, `nvmeN`.
fn is_controller_name(name: &str) -> bool {
    name.strip_prefix("nvme")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// A namespace device name, `nvmeNnM`.
pub fn is_namespace_name(name: &str) -> bool {
    controller_of(name).is_some() && !name.contains('/')
}

#[derive(Debug, PartialEq, Eq)]
pub enum Found {
    Device(String),
    None,
    /// More than one namespace: there is no NSID to choose by.
    Ambiguous(String),
}

fn glob_names(dir: &Path, keep: impl Fn(&str) -> bool) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| keep(n))
        .collect();
    names.sort();
    names
}

pub struct Nvme {
    pub runner: Arc<dyn Runner>,
    /// `commandTimeouts.nvme`.
    pub timeout: Duration,
    pub sysfs: PathBuf,
    pub dev: PathBuf,
    /// stat(2) of a block device's number (the tests supply their own).
    pub device_number: BlockDeviceNumber,
}

fn failure(what: &str, out: &Output) -> anyhow::Error {
    let status = match (out.code, out.wedged) {
        (_, true) => "wedged (output is unreliable)".to_string(),
        (Some(code), _) => format!("exit status {code}"),
        (None, _) => "killed by a signal".to_string(),
    };
    anyhow!("{what}: {status}, output: {}", out.combined().trim())
}

const DISCONNECT_RETRYABLE: [&str; 4] = [
    "device or resource busy",
    "session is busy",
    "connection timed out",
    "transport endpoint is not connected",
];

impl Nvme {
    fn limits(&self, deadline: Option<Instant>) -> Limits {
        Limits {
            timeout: self.timeout,
            rpc_deadline: deadline,
        }
    }

    pub async fn list_subsystems(&self, deadline: Option<Instant>) -> Result<Vec<Subsystem>> {
        let out = self
            .runner
            .run("nvme", &["list-subsys", "-o", "json"], self.limits(deadline))
            .await?;
        if !out.success() {
            return Err(failure("list-subsys failed", &out));
        }
        parse_list_subsys(&out.stdout).context("failed to parse subsystem list")
    }

    /// Runs `nvme connect`. "already connected" in the output of a command that
    /// exited on its own is success.
    pub async fn connect(
        &self,
        target: &Target<'_>,
        options: &ConnectOptions,
        budget: Duration,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let Target {
            transport,
            host,
            port,
            nqn,
        } = *target;
        if !TRANSPORTS.contains(&transport) {
            bail!("unsupported NVMe-oF transport: {transport} (supported: tcp, rdma, fc)");
        }
        let args = connect_args(transport, host, port, nqn, options);
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let limits = Limits {
            timeout: budget,
            rpc_deadline: deadline,
        };
        let out = self.runner.run("nvme", &argv, limits).await?;
        if out.success() {
            debug!("Connect output: {}", out.combined().trim());
            return Ok(());
        }
        if out.wedged {
            return Err(failure("connect failed", &out));
        }
        if out.combined().contains("already connected") {
            debug!("Subsystem already connected: {nqn}");
            return Ok(());
        }
        Err(failure("connect command failed", &out))
    }

    /// `nvme disconnect -n <nqn>`: three attempts on busy and timeout errors;
    /// "not found" or "No subsystems" from a command that exited on its own
    /// means it is already gone.
    pub async fn disconnect(&self, nqn: &str, deadline: Option<Instant>) -> Result<()> {
        let mut delay = Duration::from_millis(200);
        let mut attempt = 1;
        loop {
            let out = self
                .runner
                .run("nvme", &["disconnect", "-n", nqn], self.limits(deadline))
                .await?;
            if out.success() {
                return Ok(());
            }
            let error = if out.wedged {
                anyhow!("disconnect wedged (output is unreliable): {}", out.combined().trim())
            } else {
                let text = out.combined();
                if text.contains("not found") || text.contains("No subsystems") {
                    debug!("Subsystem already disconnected: {nqn}");
                    return Ok(());
                }
                failure("disconnect failed", &out)
            };
            let message = format!("{error:#}").to_lowercase();
            let retryable = DISCONNECT_RETRYABLE.iter().any(|r| message.contains(r));
            if !retryable || attempt >= 3 || deadline.is_some_and(|d| Instant::now() + delay >= d) {
                return Err(error);
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
            attempt += 1;
        }
    }

    /// The controller to address a namespace device through (Go
    /// nvmeNamespaceControllerAt). A native multipath head `nvmeXnY` is named
    /// after the subsystem instance, inherited from the founding controller;
    /// once that controller is gone, `/dev/nvmeX` no longer exists although
    /// other paths are live. `/sys/block/<dev>/device` is the subsystem for a
    /// head (it links each of its controllers by name) and the controller for
    /// a non-multipath namespace. A live controller is preferred, the one the
    /// name points at first; with no sysfs evidence, the name's controller.
    pub fn controller_for(&self, device: &str) -> Result<String> {
        let named = controller_of(device).with_context(|| format!("invalid NVMe namespace device: {device}"))?;
        // controller_of validated the file name as `nvmeNnM`.
        let name = Path::new(device).file_name().unwrap_or_default();
        let link = self.sysfs.join("block").join(name).join("device");
        let mut candidates = glob_names(&link, is_controller_name);
        if candidates.is_empty()
            && link.join("transport").exists()
            && let Some(own) = std::fs::canonicalize(&link)
                .ok()
                .and_then(|p| p.file_name().and_then(|n| n.to_str()).map(str::to_string))
                .filter(|n| is_controller_name(n))
        {
            candidates.push(own);
        }
        // Sorted by glob_names; the named controller first.
        candidates.sort_by_key(|c| c != named);
        let live = candidates.iter().find(|c| {
            std::fs::read_to_string(self.sysfs.join("class/nvme").join(c).join("state"))
                .is_ok_and(|s| s.trim() == "live")
        });
        Ok(live
            .or(candidates.first())
            .cloned()
            .unwrap_or_else(|| named.to_string()))
    }

    /// `nvme ns-rescan /dev/nvmeN` through a live controller of a namespace
    /// device's subsystem (see `controller_for`).
    pub async fn rescan(&self, device: &str, deadline: Option<Instant>) -> Result<()> {
        let controller = self.controller_for(device)?;
        let path = self.dev.join(controller);
        let path = path.to_string_lossy();
        let out = self
            .runner
            .run("nvme", &["ns-rescan", &path], self.limits(deadline))
            .await?;
        if !out.success() {
            return Err(failure("NVMe rescan failed", &out));
        }
        Ok(())
    }

    /// Whether `device` is the current node for the namespace whose sysfs
    /// directory is `sysfs_dir` (see `blockdev::is_current_node`).
    fn is_current_node(&self, device: &Path, sysfs_dir: &Path) -> bool {
        blockdev::is_current_node(&self.device_number, device, &sysfs_dir.join("dev"))
    }

    /// The subsystem's namespace device through sysfs: the native multipath
    /// head (`nvmeXnY`, X the subsystem instance) directly under the
    /// subsystem, or a namespace under one of its controllers.
    pub fn find_device_sysfs(&self, nqn: &str) -> Found {
        let root = self.sysfs.join("class/nvme-subsystem");
        for subsystem in glob_names(&root, |n| n.starts_with("nvme-subsys")) {
            let dir = root.join(&subsystem);
            let Ok(found) = std::fs::read_to_string(dir.join("subsysnqn")) else {
                continue;
            };
            if found.trim() != nqn {
                continue;
            }
            // (name, its sysfs directory), the direct entry first.
            let within = |parent: &Path| -> Vec<(String, PathBuf)> {
                glob_names(parent, is_namespace_name)
                    .into_iter()
                    .map(|name| {
                        let path = parent.join(&name);
                        (name, path)
                    })
                    .collect()
            };
            let mut namespaces = within(&dir);
            for controller in glob_names(&dir, |n| n.starts_with("nvme") && controller_of(n).is_none()) {
                namespaces.extend(within(&dir.join(controller)));
            }
            namespaces.sort_by(|a, b| a.0.cmp(&b.0));
            namespaces.dedup_by(|later, earlier| later.0 == earlier.0);
            if namespaces.len() > 1 {
                let names: Vec<&str> = namespaces.iter().map(|(name, _)| name.as_str()).collect();
                return Found::Ambiguous(format!(
                    "NVMe subsystem {nqn} exposes multiple namespaces ({})",
                    names.join(", ")
                ));
            }
            if let Some((name, sysfs_dir)) = namespaces.first() {
                let device = self.dev.join(name);
                if self.is_current_node(&device, sysfs_dir) {
                    return Found::Device(device.to_string_lossy().into_owned());
                }
            }
        }
        Found::None
    }

    /// The namespace behind a controller `nvme list-subsys` names, when its
    /// /dev node is current.
    pub fn find_device_for_controller(&self, controller: &str) -> Found {
        let digits = controller.strip_prefix("nvme").unwrap_or_default();
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Found::None;
        }
        let dir = self.sysfs.join("class/nvme").join(controller);
        let names = glob_names(&dir, |n| {
            n.starts_with(&format!("{controller}n")) && is_namespace_name(n)
        });
        if names.len() > 1 {
            return Found::Ambiguous(format!(
                "NVMe controller {controller} exposes multiple namespaces ({})",
                names.join(", ")
            ));
        }
        match names.first() {
            Some(name) if self.is_current_node(&self.dev.join(name), &dir.join(name)) => {
                Found::Device(self.dev.join(name).to_string_lossy().into_owned())
            }
            _ => Found::None,
        }
    }

    pub fn find_device_from_subsystems(&self, nqn: &str, subsystems: &[Subsystem]) -> Found {
        for subsystem in subsystems.iter().filter(|s| s.nqn == nqn) {
            for path in subsystem.paths.iter().filter(|p| !p.name.is_empty()) {
                match self.find_device_for_controller(&path.name) {
                    Found::None => {}
                    found => return found,
                }
            }
        }
        Found::None
    }

    /// Polls (50 ms, then 100 ms) until the subsystem's device appears, up to
    /// `timeout`. After a fresh connect the subsystem list is refreshed once.
    pub async fn wait_for_device(
        &self,
        nqn: &str,
        timeout: Duration,
        mut subsystems: Vec<Subsystem>,
        mut refresh: bool,
        deadline: Option<Instant>,
    ) -> Result<String> {
        let start = Instant::now();
        let mut interval = Duration::from_millis(50);
        loop {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                bail!("context canceled waiting for device (nqn={nqn})");
            }
            if start.elapsed() > timeout {
                bail!("timeout waiting for device (nqn={nqn})");
            }
            match self.find_device_sysfs(nqn) {
                Found::Device(device) => return Ok(device),
                Found::Ambiguous(why) => bail!("ambiguous NVMe namespace selection: {why}"),
                Found::None => {}
            }
            if refresh {
                if let Ok(fresh) = self.list_subsystems(deadline).await {
                    subsystems = fresh;
                }
                refresh = false;
            }
            match self.find_device_from_subsystems(nqn, &subsystems) {
                Found::Device(device) => return Ok(device),
                Found::Ambiguous(why) => bail!("ambiguous NVMe namespace selection: {why}"),
                Found::None => {}
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(Duration::from_millis(100));
        }
    }

    /// The subsystem NQN of a namespace device (Go GetNVMeInfoFromDevice).
    ///
    /// The block device is asked first: for a native multipath head
    /// (`nvmeXnY`), `/sys/block/nvmeXnY/device` is the subsystem
    /// (`nvme-subsysX`), which carries `subsysnqn` and lives as long as any
    /// path does. The name's `nvmeX` is only the subsystem instance, inherited
    /// from the founding controller: once that controller is gone,
    /// `/sys/class/nvme/nvmeX` no longer exists although the subsystem still
    /// has live paths. For a non-multipath namespace, `device` is the
    /// controller, which carries `subsysnqn` too. The controller named after
    /// the namespace stays as the fallback.
    pub fn nqn_of_device(&self, device: &str) -> Result<String> {
        let controller = controller_of(device).with_context(|| format!("invalid NVMe device name: {device}"))?;
        // controller_of validated the file name as `nvmeNnM`.
        let name = Path::new(device).file_name().unwrap_or_default();
        let block = self.sysfs.join("block").join(name).join("device/subsysnqn");
        if let Ok(nqn) = std::fs::read_to_string(&block)
            && !nqn.trim().is_empty()
        {
            return Ok(nqn.trim().to_string());
        }
        let class = self.sysfs.join("class/nvme").join(controller);
        for path in [class.join("subsysnqn"), class.join("subsystem/subsysnqn")] {
            if let Ok(nqn) = std::fs::read_to_string(&path) {
                return Ok(nqn.trim().to_string());
            }
        }
        bail!("could not find NQN for device {device}")
    }

    /// Writes a native multipath policy (`queue-depth`) for one subsystem.
    pub fn set_iopolicy(&self, subsystem: &str, policy: &str) -> Result<()> {
        if subsystem.is_empty() || subsystem == "." || subsystem == ".." || subsystem.contains('/') {
            bail!("invalid NVMe subsystem name {subsystem:?}");
        }
        let path = self.sysfs.join("class/nvme-subsystem").join(subsystem).join("iopolicy");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .context("open NVMe subsystem iopolicy")?;
        std::io::Write::write_all(&mut file, policy.as_bytes()).context("write NVMe subsystem iopolicy")
    }

    /// How many subsystem directories carry this NQN: more than one means
    /// `nvme_core.multipath=N` split the paths into separate devices.
    pub fn subsystem_dirs(&self, nqn: &str) -> Result<usize> {
        let root = self.sysfs.join("class/nvme-subsystem");
        let mut count = 0;
        for name in glob_names(&root, |n| n.starts_with("nvme-subsys")) {
            match std::fs::read_to_string(root.join(&name).join("subsysnqn")) {
                Ok(found) if found.trim() == nqn => count += 1,
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("read {name}/subsysnqn")),
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{fake_device_number, sysfs_dev};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Answers commands with scripted replies, in order; records the argv.
    /// (exit code, output, wedged)
    type Reply = (Option<i32>, &'static str, bool);

    #[derive(Default)]
    struct Script {
        replies: Mutex<Vec<Reply>>,
        calls: Mutex<Vec<String>>,
    }

    impl Script {
        fn new(replies: Vec<Reply>) -> Arc<Self> {
            Arc::new(Script {
                replies: Mutex::new(replies.into_iter().rev().collect()),
                calls: Mutex::default(),
            })
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl Runner for Script {
        async fn run(&self, program: &str, args: &[&str], _: Limits) -> std::io::Result<Output> {
            self.calls.lock().unwrap().push(format!("{program} {}", args.join(" ")));
            let (code, text, wedged) = self.replies.lock().unwrap().pop().expect("an unscripted command ran");
            Ok(Output {
                code,
                stdout: text.into(),
                stderr: Vec::new(),
                wedged,
                timed_out: false,
            })
        }
    }

    fn nvme(script: &Arc<Script>, root: &Path) -> Nvme {
        nvme_numbered(script, root, |name| Some(fake_device_number(name)))
    }

    /// An initiator whose stat(2) of an existing /dev node reports
    /// `number(name)` (None: not a block device). Fixtures cannot mknod.
    fn nvme_numbered(
        script: &Arc<Script>,
        root: &Path,
        number: impl Fn(&str) -> Option<u64> + Send + Sync + 'static,
    ) -> Nvme {
        Nvme {
            runner: script.clone(),
            timeout: Duration::from_secs(30),
            sysfs: root.join("sys"),
            dev: root.join("dev"),
            device_number: Arc::new(move |path: &str| {
                std::fs::metadata(path)?;
                Ok(number(&Path::new(path).file_name().unwrap().to_string_lossy()))
            }),
        }
    }

    #[test]
    fn list_subsys_shapes() {
        let nqns = |input: &str| -> Result<Vec<String>> {
            Ok(parse_list_subsys(input.as_bytes())?
                .into_iter()
                .map(|s| s.nqn)
                .collect())
        };
        // The Go test's vectors.
        assert_eq!(
            nqns(r#"{"Subsystems":[{"NQN":"nqn.1","Name":"nvme-subsys1"}]}"#).unwrap(),
            ["nqn.1"]
        );
        assert_eq!(
            nqns(r#"[{"HostNQN":"host-a","Subsystems":[{"NQN":"nqn.2a"}]},{"HostNQN":"host-b","Subsystems":[{"NQN":"nqn.2b"}]}]"#)
                .unwrap(),
            ["nqn.2a", "nqn.2b"]
        );
        assert_eq!(nqns(r#"[{"NQN":"nqn.3","Name":"nvme-subsys3"}]"#).unwrap(), ["nqn.3"]);
        assert!(nqns("[{").is_err(), "malformed");
        assert!(nqns(r#"[{"HostNQN":"host-only"}]"#).is_err(), "host-shaped phantom");
        assert!(nqns(r#"[{"NQN":""}]"#).is_err(), "empty NQN");
        assert!(nqns("{}").is_err());
        assert_eq!(nqns("[]").unwrap(), Vec::<String>::new());
        assert_eq!(nqns(r#"{"Subsystems":[]}"#).unwrap(), Vec::<String>::new());
        // Go's decoder matches keys in any case.
        assert_eq!(nqns(r#"[{"nqn":"nqn.4"}]"#).unwrap(), ["nqn.4"]);

        let full = parse_list_subsys(
            br#"[{"HostNQN":"h","Subsystems":[{"Name":"nvme-subsys2","NQN":"nqn.x","Paths":[
                {"Name":"nvme2","Transport":"tcp","Address":"traddr=192.0.2.10,trsvcid=4420","State":"live"}]}]}]"#,
        )
        .unwrap();
        assert_eq!(
            full,
            [Subsystem {
                nqn: "nqn.x".into(),
                name: "nvme-subsys2".into(),
                paths: vec![NvmePath {
                    name: "nvme2".into(),
                    transport: "tcp".into(),
                    address: "traddr=192.0.2.10,trsvcid=4420".into(),
                    state: "live".into(),
                }],
            }]
        );
    }

    #[test]
    fn live_addresses_of_a_subsystem() {
        let path = |address: &str, state: &str| NvmePath {
            address: address.into(),
            state: state.into(),
            ..Default::default()
        };
        let subsystems = vec![
            Subsystem {
                nqn: "nqn.test:volume-a".into(),
                paths: vec![
                    path("traddr=192.0.2.10,trsvcid=4420", "live"),
                    path("traddr=192.0.2.11 trsvcid=4420", "LIVE"),
                    path("traddr=192.0.2.12,trsvcid=4420", "connecting"),
                    path("traddr=192.0.2.10,trsvcid=4420", "live"),
                    path("trsvcid=4420", "live"),
                ],
                ..Default::default()
            },
            Subsystem {
                nqn: "nqn.test:volume-b".into(),
                paths: vec![path("traddr=198.51.100.20,trsvcid=4420", "live")],
                ..Default::default()
            },
        ];
        assert_eq!(
            live_addresses("nqn.test:volume-a", &subsystems),
            ["192.0.2.10", "192.0.2.11"]
        );
        assert!(live_addresses("nqn.test:none", &subsystems).is_empty());
        assert!(has_subsystem("nqn.test:volume-b", &subsystems));
        assert_eq!(
            path_field("traddr=2001:db8::1,trsvcid=4420", "trsvcid").as_deref(),
            Some("4420")
        );
        assert_eq!(path_field("a=1", "traddr"), None);
    }

    #[test]
    fn connect_argv() {
        assert_eq!(
            connect_args("tcp", "192.0.2.10", "4420", "nqn.x", &ConnectOptions::default()).join(" "),
            "connect -t tcp -n nqn.x -a 192.0.2.10 -s 4420 --reconnect-delay=10 --ctrl-loss-tmo=-1"
        );
        let all = ConnectOptions {
            fast_io_fail_tmo: Some(15),
            nr_io_queues: Some(4),
            nr_write_queues: Some(2),
            keep_alive_tmo: Some(5),
        };
        assert_eq!(
            connect_args("tcp", "2001:db8::1", "4420", "nqn.x", &all).join(" "),
            "connect -t tcp -n nqn.x -a 2001:db8::1 -s 4420 --reconnect-delay=10 --ctrl-loss-tmo=-1 \
             --fast_io_fail_tmo=15 --nr-io-queues=4 --nr-write-queues=2 --keep-alive-tmo=5"
        );
    }

    #[test]
    fn controllers_of_devices() {
        assert_eq!(controller_of("/dev/nvme0n1"), Some("nvme0"));
        assert_eq!(controller_of("nvme10n2"), Some("nvme10"));
        for no in [
            "nvme0n1p1",
            "nvme0c0n1",
            "nvme0",
            "sda",
            "nvmen1",
            "nvme1n",
            "/dev/ublkb0",
        ] {
            assert_eq!(controller_of(no), None, "{no}");
        }
    }

    fn touch(path: &Path, content: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn devices_through_sysfs() {
        let root = tempfile::tempdir().unwrap();
        let n = nvme(&Script::new(vec![]), root.path());
        let subsys = |name: &str| root.path().join("sys/class/nvme-subsystem").join(name);
        // Namespace directly under the subsystem (fabrics).
        touch(&subsys("nvme-subsys0").join("subsysnqn"), "nqn.a\n");
        touch(&subsys("nvme-subsys0").join("nvme0n1/size"), "8");
        touch(&subsys("nvme-subsys0").join("nvme0n1/dev"), &sysfs_dev("nvme0n1"));
        touch(&subsys("nvme-subsys0").join("nvme0c0n1/size"), "8");
        touch(&root.path().join("dev/nvme0n1"), "");
        // Under a controller directory (PCIe-style).
        touch(&subsys("nvme-subsys1").join("subsysnqn"), "nqn.b\n");
        touch(&subsys("nvme-subsys1").join("nvme1/nvme1n1/size"), "8");
        touch(&subsys("nvme-subsys1").join("nvme1/nvme1n1/dev"), &sysfs_dev("nvme1n1"));
        touch(&root.path().join("dev/nvme1n1"), "");
        // Two namespaces: no NSID to choose by.
        touch(&subsys("nvme-subsys2").join("subsysnqn"), "nqn.c\n");
        touch(&subsys("nvme-subsys2").join("nvme2n1/size"), "8");
        touch(&subsys("nvme-subsys2").join("nvme2n2/size"), "8");
        // Listed in sysfs but no device node yet.
        touch(&subsys("nvme-subsys3").join("subsysnqn"), "nqn.d\n");
        touch(&subsys("nvme-subsys3").join("nvme3n1/size"), "8");
        touch(&subsys("nvme-subsys3").join("nvme3n1/dev"), &sysfs_dev("nvme3n1"));

        let dev = |name: &str| root.path().join("dev").join(name).to_string_lossy().into_owned();
        assert_eq!(n.find_device_sysfs("nqn.a"), Found::Device(dev("nvme0n1")));
        assert_eq!(n.find_device_sysfs("nqn.b"), Found::Device(dev("nvme1n1")));
        assert!(matches!(n.find_device_sysfs("nqn.c"), Found::Ambiguous(why) if why.contains("nvme2n1, nvme2n2")));
        assert_eq!(n.find_device_sysfs("nqn.d"), Found::None);
        assert_eq!(n.find_device_sysfs("nqn.none"), Found::None);

        // Through the controller list-subsys names.
        touch(&root.path().join("sys/class/nvme/nvme5/nvme5n1/size"), "8");
        touch(
            &root.path().join("sys/class/nvme/nvme5/nvme5n1/dev"),
            &sysfs_dev("nvme5n1"),
        );
        touch(&root.path().join("dev/nvme5n1"), "");
        let listed = vec![Subsystem {
            nqn: "nqn.e".into(),
            paths: vec![NvmePath {
                name: "nvme5".into(),
                ..Default::default()
            }],
            ..Default::default()
        }];
        assert_eq!(
            n.find_device_from_subsystems("nqn.e", &listed),
            Found::Device(dev("nvme5n1"))
        );
        assert_eq!(n.find_device_from_subsystems("nqn.f", &listed), Found::None);
        assert_eq!(
            n.find_device_for_controller("../nvme5"),
            Found::None,
            "a controller name is validated"
        );

        // The NQN of a device, directly or through the subsystem link.
        touch(&root.path().join("sys/class/nvme/nvme0/subsysnqn"), "nqn.a\n");
        touch(&root.path().join("sys/class/nvme/nvme7/subsystem/subsysnqn"), "nqn.g\n");
        assert_eq!(n.nqn_of_device("/dev/nvme0n1").unwrap(), "nqn.a");
        assert_eq!(n.nqn_of_device("/dev/nvme7n3").unwrap(), "nqn.g");
        assert!(n.nqn_of_device("/dev/nvme9n1").is_err());
        assert!(n.nqn_of_device("/dev/sda").is_err());

        // Split multipath and the iopolicy.
        touch(&subsys("nvme-subsys9").join("subsysnqn"), "nqn.a\n");
        assert_eq!(n.subsystem_dirs("nqn.a").unwrap(), 2);
        assert_eq!(n.subsystem_dirs("nqn.b").unwrap(), 1);
        touch(&subsys("nvme-subsys0").join("iopolicy"), "numa");
        n.set_iopolicy("nvme-subsys0", "queue-depth").unwrap();
        assert_eq!(
            std::fs::read_to_string(subsys("nvme-subsys0").join("iopolicy")).unwrap(),
            "queue-depth"
        );
        for bad in ["", ".", "..", "../x"] {
            assert!(n.set_iopolicy(bad, "queue-depth").is_err(), "{bad:?}");
        }
    }

    /// A native multipath head is named after the subsystem instance, which
    /// the kernel takes from the founding controller. Once that controller is
    /// gone while other paths stay live, `/sys/class/nvme/nvmeX` disappears;
    /// the block device's own sysfs node still resolves to the subsystem.
    #[test]
    fn nqn_of_a_head_survives_loss_of_the_founding_controller() {
        let root = tempfile::tempdir().unwrap();
        let n = nvme(&Script::new(vec![]), root.path());
        let sys = root.path().join("sys");
        let subsys = sys.join("devices/virtual/nvme-subsystem/nvme-subsys0");
        touch(&subsys.join("subsysnqn"), "nqn.head\n");
        let head = subsys.join("nvme0n1");
        std::fs::create_dir_all(&head).unwrap();
        std::os::unix::fs::symlink(&subsys, head.join("device")).unwrap();
        std::fs::create_dir_all(sys.join("block")).unwrap();
        std::os::unix::fs::symlink(&head, sys.join("block/nvme0n1")).unwrap();
        // A surviving controller exists; the founding one (nvme0) does not.
        touch(&sys.join("class/nvme/nvme1/subsysnqn"), "nqn.head\n");
        assert_eq!(n.nqn_of_device("/dev/nvme0n1").unwrap(), "nqn.head");

        // The block node is read first.
        touch(&sys.join("class/nvme/nvme0/subsysnqn"), "nqn.stale\n");
        assert_eq!(n.nqn_of_device("/dev/nvme0n1").unwrap(), "nqn.head");

        // An empty block attribute falls back to the controller.
        touch(&sys.join("block/nvme4n1/device/subsysnqn"), "\n");
        touch(&sys.join("class/nvme/nvme4/subsysnqn"), "nqn.ctrl\n");
        assert_eq!(n.nqn_of_device("/dev/nvme4n1").unwrap(), "nqn.ctrl");

        // No block node and no controller is still a failed lookup; a
        // partition is not a namespace even when a block node exists.
        assert!(n.nqn_of_device("/dev/nvme9n1").is_err());
        touch(&sys.join("block/nvme0n1p1/device/subsysnqn"), "nqn.head\n");
        assert!(n.nqn_of_device("/dev/nvme0n1p1").is_err());
    }

    #[tokio::test]
    async fn connect_outcomes() {
        let target = Target {
            transport: "tcp",
            host: "192.0.2.10",
            port: "4420",
            nqn: "nqn.x",
        };
        let opts = ConnectOptions::default();
        let budget = Duration::from_secs(60);
        let root = tempfile::tempdir().unwrap();
        for (reply, ok) in [
            ((Some(0), "", false), true),
            (
                (
                    Some(1),
                    "Failed to write to /dev/nvme-fabrics: Operation already in progress; already connected",
                    false,
                ),
                true,
            ),
            ((None, "already connected", true), false),
            ((Some(1), "failed to connect: Connection refused", false), false),
        ] {
            let script = Script::new(vec![reply]);
            let got = nvme(&script, root.path()).connect(&target, &opts, budget, None).await;
            assert_eq!(got.is_ok(), ok, "{reply:?}: {got:?}");
        }
        let script = Script::new(vec![]);
        let bad = Target {
            transport: "loop",
            ..target
        };
        assert!(
            nvme(&script, root.path())
                .connect(&bad, &opts, budget, None)
                .await
                .is_err()
        );
        assert!(script.calls().is_empty(), "an unsupported transport runs nothing");
    }

    #[tokio::test]
    async fn disconnect_outcomes() {
        let root = tempfile::tempdir().unwrap();
        let busy = (Some(1), "Device or resource busy", false);
        let cases: Vec<(Vec<Reply>, bool, usize)> = vec![
            (vec![(Some(0), "", false)], true, 1),
            (vec![(Some(1), "nqn.x not found", false)], true, 1),
            (vec![(Some(1), "No subsystems", false)], true, 1),
            (vec![busy, (Some(0), "", false)], true, 2),
            (vec![busy, busy, busy], false, 3),
            (vec![(Some(1), "Invalid argument", false)], false, 1),
            (vec![(None, "not found", true)], false, 1),
        ];
        for (replies, ok, calls) in cases {
            let script = Script::new(replies.clone());
            let got = nvme(&script, root.path()).disconnect("nqn.x", None).await;
            assert_eq!(got.is_ok(), ok, "{replies:?}: {got:?}");
            assert_eq!(script.calls().len(), calls, "{replies:?}");
            assert!(script.calls().iter().all(|c| c == "nvme disconnect -n nqn.x"));
        }
    }

    #[tokio::test]
    async fn waiting_for_a_device() {
        let root = tempfile::tempdir().unwrap();
        // Not in sysfs; the refreshed list names its controller.
        touch(&root.path().join("sys/class/nvme/nvme4/nvme4n1/size"), "8");
        touch(
            &root.path().join("sys/class/nvme/nvme4/nvme4n1/dev"),
            &sysfs_dev("nvme4n1"),
        );
        touch(&root.path().join("dev/nvme4n1"), "");
        let script = Script::new(vec![(
            Some(0),
            r#"[{"Subsystems":[{"NQN":"nqn.w","Paths":[{"Name":"nvme4","State":"live"}]}]}]"#,
            false,
        )]);
        let n = nvme(&script, root.path());
        let got = n
            .wait_for_device("nqn.w", Duration::from_secs(2), Vec::new(), true, None)
            .await
            .unwrap();
        assert!(got.ends_with("dev/nvme4n1"), "{got}");
        assert_eq!(
            script.calls(),
            ["nvme list-subsys -o json"],
            "one refresh after a fresh connect"
        );

        // Two namespaces fail at once instead of waiting out the timeout.
        let subsys = root.path().join("sys/class/nvme-subsystem/nvme-subsys6");
        touch(&subsys.join("subsysnqn"), "nqn.amb\n");
        touch(&subsys.join("nvme6n1/size"), "8");
        touch(&subsys.join("nvme6n2/size"), "8");
        let started = Instant::now();
        let err = nvme(&Script::new(vec![]), root.path())
            .wait_for_device("nqn.amb", Duration::from_secs(30), Vec::new(), false, None)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("ambiguous"), "{err:#}");
        assert!(started.elapsed() < Duration::from_secs(1));

        let err = nvme(&Script::new(vec![]), root.path())
            .wait_for_device("nqn.never", Duration::from_millis(200), Vec::new(), false, None)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("timeout waiting for device"), "{err:#}");

        // The list is refreshed once, not on every poll.
        let script = Script::new(vec![(Some(0), "[]", false)]);
        let err = nvme(&script, root.path())
            .wait_for_device("nqn.late", Duration::from_millis(300), Vec::new(), true, None)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("timeout"), "{err:#}");
        assert_eq!(script.calls().len(), 1);
    }

    /// sysfs and /dev right after a connect: the namespace's sysfs dev file
    /// carries the new number and /dev/nvme3n1 exists. As the native multipath
    /// head directly under nvme-subsys3 (found through sysfs), or as a
    /// namespace of controller nvme3 with no subsystem directory (found
    /// through the controller list-subsys names). Returns the namespace's
    /// sysfs directory.
    fn connected(root: &Path, head: bool) -> PathBuf {
        let dir = if head {
            let subsys = root.join("sys/class/nvme-subsystem/nvme-subsys3");
            touch(&subsys.join("subsysnqn"), "nqn.h\n");
            subsys.join("nvme3n1")
        } else {
            root.join("sys/class/nvme/nvme3/nvme3n1")
        };
        touch(&dir.join("dev"), &sysfs_dev("nvme3n1"));
        touch(&root.join("dev/nvme3n1"), "");
        dir
    }

    fn listed() -> Vec<Subsystem> {
        vec![Subsystem {
            nqn: "nqn.h".into(),
            paths: vec![NvmePath {
                name: "nvme3".into(),
                state: "live".into(),
                ..Default::default()
            }],
            ..Default::default()
        }]
    }

    fn find(n: &Nvme, head: bool) -> Found {
        if head {
            n.find_device_sysfs("nqn.h")
        } else {
            n.find_device_from_subsystems("nqn.h", &listed())
        }
    }

    /// The /dev node the previous connect's namespace left behind, a node that
    /// is not a block device, or a namespace with no sysfs device number is
    /// never the device.
    #[test]
    fn only_a_current_node_is_the_device() {
        let fresh = fake_device_number("nvme3n1");
        for head in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let sysfs_dir = connected(root.path(), head);
            let script = Script::new(vec![]);

            let stale = nvme_numbered(&script, root.path(), move |_| Some(fresh ^ 0xff));
            assert_eq!(find(&stale, head), Found::None, "head={head}: a stale node");

            let not_block = nvme_numbered(&script, root.path(), |_| None);
            assert_eq!(find(&not_block, head), Found::None, "head={head}: not a block device");

            let n = nvme(&script, root.path());
            std::fs::remove_file(sysfs_dir.join("dev")).unwrap();
            assert_eq!(find(&n, head), Found::None, "head={head}: no sysfs dev file");
            touch(&sysfs_dir.join("dev"), "garbage\n");
            assert_eq!(find(&n, head), Found::None, "head={head}: an unreadable sysfs dev file");

            touch(&sysfs_dir.join("dev"), &sysfs_dev("nvme3n1"));
            let device = root.path().join("dev/nvme3n1").to_string_lossy().into_owned();
            assert_eq!(find(&n, head), Found::Device(device), "head={head}");
        }
    }

    /// stat(2) reports the stale number for the first `stale` stats, then the
    /// current one, counting every stat.
    fn stale_then_fresh(root: &Path, stale: u32) -> (Nvme, Arc<AtomicU32>) {
        let stats = Arc::new(AtomicU32::new(0));
        let counted = stats.clone();
        let n = nvme_numbered(&Script::new(vec![]), root, move |name| {
            let fresh = fake_device_number(name);
            Some(if counted.fetch_add(1, Ordering::SeqCst) < stale {
                fresh ^ 0xff
            } else {
                fresh
            })
        });
        (n, stats)
    }

    /// The handover race: the other plugin disconnected, this one connected,
    /// and /dev/nvme3n1 is still the previous namespace's node for a few
    /// polls. The wait returns only the current node.
    #[tokio::test]
    async fn the_device_wait_skips_a_stale_dev_node() {
        for head in [true, false] {
            let root = tempfile::tempdir().unwrap();
            connected(root.path(), head);
            let (n, stats) = stale_then_fresh(root.path(), 3);
            let subsystems = if head { Vec::new() } else { listed() };
            let got = n
                .wait_for_device("nqn.h", Duration::from_secs(5), subsystems, false, None)
                .await
                .unwrap();
            assert!(got.ends_with("dev/nvme3n1"), "head={head}: {got}");
            assert_eq!(
                stats.load(Ordering::SeqCst),
                4,
                "head={head}: polled past the stale node"
            );
        }
    }

    /// A node that never becomes current ends in the wait's own timeout.
    #[tokio::test]
    async fn the_device_wait_times_out_on_a_stale_dev_node() {
        for head in [true, false] {
            let root = tempfile::tempdir().unwrap();
            connected(root.path(), head);
            let (n, stats) = stale_then_fresh(root.path(), u32::MAX);
            let subsystems = if head { Vec::new() } else { listed() };
            let err = n
                .wait_for_device("nqn.h", Duration::from_millis(150), subsystems, false, None)
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), "timeout waiting for device (nqn=nqn.h)", "head={head}");
            assert!(
                stats.load(Ordering::SeqCst) > 1,
                "head={head}: re-checked until the timeout"
            );
        }
    }

    /// The founding controller (nvme0) of a multipath head is gone; nvme1
    /// and nvme2 are paths of the same subsystem. The rescan goes to a live
    /// one of them: /dev/nvme0 no longer exists.
    #[tokio::test]
    async fn rescan_survives_loss_of_the_founding_controller() {
        let root = tempfile::tempdir().unwrap();
        let sys = root.path().join("sys");
        let subsys = sys.join("devices/virtual/nvme-subsystem/nvme-subsys0");
        touch(&subsys.join("subsysnqn"), "nqn.head\n");
        let head = subsys.join("nvme0n1");
        std::fs::create_dir_all(&head).unwrap();
        std::os::unix::fs::symlink(&subsys, head.join("device")).unwrap();
        std::fs::create_dir_all(sys.join("block")).unwrap();
        std::os::unix::fs::symlink(&head, sys.join("block/nvme0n1")).unwrap();
        for (controller, state) in [("nvme1", "connecting"), ("nvme2", "live")] {
            let dir = sys.join("class/nvme").join(controller);
            touch(&dir.join("state"), &format!("{state}\n"));
            std::os::unix::fs::symlink(&dir, subsys.join(controller)).unwrap();
        }
        let script = Script::new(vec![(Some(0), "", false)]);
        nvme(&script, root.path()).rescan("/dev/nvme0n1", None).await.unwrap();
        assert_eq!(
            script.calls(),
            [format!("nvme ns-rescan {}", root.path().join("dev/nvme2").display())]
        );

        // With the founder back and live, it is used, as before.
        let founder = sys.join("class/nvme/nvme0");
        touch(&founder.join("state"), "live\n");
        std::os::unix::fs::symlink(&founder, subsys.join("nvme0")).unwrap();
        let n = nvme(&Script::new(vec![]), root.path());
        assert_eq!(n.controller_for("/dev/nvme0n1").unwrap(), "nvme0");

        // A non-multipath namespace: its device is its own controller.
        let ctrl = sys.join("class/nvme/nvme7");
        touch(&ctrl.join("transport"), "pcie\n");
        std::fs::create_dir_all(ctrl.join("nvme7n1")).unwrap();
        std::os::unix::fs::symlink(&ctrl, ctrl.join("nvme7n1/device")).unwrap();
        std::os::unix::fs::symlink(ctrl.join("nvme7n1"), sys.join("block/nvme7n1")).unwrap();
        assert_eq!(n.controller_for("/dev/nvme7n1").unwrap(), "nvme7");

        // No sysfs evidence: the name's controller; a partition is refused.
        assert_eq!(n.controller_for("/dev/nvme9n1").unwrap(), "nvme9");
        assert!(n.controller_for("/dev/nvme9n1p1").is_err());
    }

    #[tokio::test]
    async fn rescan_targets_the_controller() {
        let root = tempfile::tempdir().unwrap();
        let script = Script::new(vec![(Some(0), "", false)]);
        nvme(&script, root.path()).rescan("/dev/nvme3n1", None).await.unwrap();
        assert_eq!(
            script.calls(),
            [format!("nvme ns-rescan {}", root.path().join("dev/nvme3").display())]
        );
        assert!(
            nvme(&Script::new(vec![]), root.path())
                .rescan("/dev/sda", None)
                .await
                .is_err()
        );
    }
}
