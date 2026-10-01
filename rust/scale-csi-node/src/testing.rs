//! Fakes for node tests: a host whose mount table, filesystems and command log
//! live in memory, and an in-memory nvmeublkd whose devices are files in a
//! temporary directory (so a staging link resolves as a real /dev/ublkbN
//! would).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::config;
use crate::events::Recorded;
use crate::exec::{Limits, Output};
use crate::metrics::Metrics;
use crate::mount::{Mounter, Runner, Timeouts};
use crate::node_id::{self, NodeIdentity};
use crate::service::{Host, State};
use crate::ublk_client::{AttachRequest, Daemon, Device, DevicePath, Error};

#[derive(Default)]
pub struct HostState {
    /// target -> (source, fs type, options)
    pub mounts: BTreeMap<String, (String, String, String)>,
    /// Kept in step with `mounts` as /proc/self/mountinfo would be.
    pub mountinfo: Option<PathBuf>,
    /// device -> filesystem
    pub filesystems: HashMap<String, String>,
    pub calls: Vec<String>,
}

#[derive(Default)]
pub struct FakeHost(pub Mutex<HostState>);

fn output(code: i32, stdout: &str) -> Output {
    Output {
        code: Some(code),
        stdout: stdout.as_bytes().to_vec(),
        stderr: Vec::new(),
        wedged: false,
        timed_out: false,
    }
}

impl HostState {
    fn sync_mountinfo(&self) {
        let Some(path) = &self.mountinfo else { return };
        let text: String = self
            .mounts
            .iter()
            .enumerate()
            .map(|(i, (target, (source, fs, options)))| {
                format!(
                    "{} 1 0:{i} / {} {options} - {fs} {source} rw\n",
                    100 + i,
                    target.replace(' ', "\\040")
                )
            })
            .collect();
        std::fs::write(path, text).unwrap();
    }
}

impl FakeHost {
    pub fn mount(&self, target: &str, source: &str, fs: &str) {
        self.mount_with(target, source, fs, "rw");
    }

    pub fn mount_with(&self, target: &str, source: &str, fs: &str, options: &str) {
        let mut host = self.0.lock().unwrap();
        host.mounts
            .insert(target.into(), (source.into(), fs.into(), options.into()));
        host.sync_mountinfo();
    }

    pub fn calls(&self) -> Vec<String> {
        self.0.lock().unwrap().calls.clone()
    }

    pub fn is_mounted(&self, target: &str) -> bool {
        self.0.lock().unwrap().mounts.contains_key(target)
    }
}

#[tonic::async_trait]
impl Runner for FakeHost {
    async fn run(&self, program: &str, args: &[&str], _: Limits) -> std::io::Result<Output> {
        let mut host = self.0.lock().unwrap();
        host.calls.push(format!("{program} {}", args.join(" ")));
        let last = args.last().copied().unwrap_or_default();
        Ok(match (program, args) {
            ("findmnt", ["--mountpoint", target, "--noheadings"]) => match host.mounts.get(*target) {
                Some((source, fs, _)) => output(0, &format!("{target} {source} {fs} rw\n")),
                None => output(1, ""),
            },
            (
                "findmnt",
                [
                    "--first-only",
                    "--noheadings",
                    "--output",
                    "SOURCE,FSTYPE,OPTIONS",
                    "--mountpoint",
                    target,
                ],
            ) => match host.mounts.get(*target) {
                Some((source, fs, options)) => output(0, &format!("{source} {fs} {options}\n")),
                None => output(1, ""),
            },
            ("findmnt", ["--first-only", "-n", "-o", "SOURCE", path]) => match host.mounts.get(*path) {
                Some((source, _, _)) => output(0, &format!("{source}\n")),
                None => output(1, ""),
            },
            ("findmnt", ["-n", "-o", "FSTYPE", "--mountpoint", target]) => match host.mounts.get(*target) {
                Some((_, fs, _)) => output(0, &format!("{fs}\n")),
                None => output(1, ""),
            },
            ("blkid", _) => match host.filesystems.get(last) {
                Some(fs) => output(0, &format!("TYPE={fs}\n")),
                None => output(2, ""),
            },
            (mkfs, [_, device]) if mkfs.starts_with("mkfs.") => {
                let fs = mkfs.trim_start_matches("mkfs.").to_string();
                host.filesystems.insert((*device).into(), fs);
                output(0, "")
            }
            ("mount", ["-o", "remount,bind,ro", target]) => {
                if let Some(entry) = host.mounts.get_mut(*target) {
                    entry.2 = "ro".into();
                }
                host.sync_mountinfo();
                output(0, "")
            }
            ("mount", _) => {
                let fs = args
                    .iter()
                    .position(|a| *a == "-t")
                    .map(|i| args[i + 1].to_string())
                    .unwrap_or_default();
                let (source, target) = (args[args.len() - 2], last);
                // A bind mount shows the bound mount's source and filesystem;
                // a bound device node shows the device.
                let bound = args
                    .iter()
                    .position(|a| *a == "-o")
                    .is_some_and(|i| args[i + 1].split(',').any(|o| o == "bind"));
                let entry = match host.mounts.get(source) {
                    Some((src, fs, _)) if bound => (src.clone(), fs.clone(), "rw".to_string()),
                    _ if bound => (source.to_string(), "devtmpfs".to_string(), "rw".to_string()),
                    _ => (source.to_string(), fs, "rw".to_string()),
                };
                host.mounts.insert(target.into(), entry);
                host.sync_mountinfo();
                output(0, "")
            }
            ("umount", [target]) => {
                host.mounts.remove(*target);
                host.sync_mountinfo();
                output(0, "")
            }
            _ => output(127, ""),
        })
    }
}

#[derive(Default)]
pub struct DaemonState {
    pub next_id: i64,
    pub devices: BTreeMap<String, Device>,
    pub attaches: Vec<AttachRequest>,
    pub detaches: Vec<String>,
    pub lists: usize,
    pub attach_err: Option<Error>,
    pub detach_err: Option<Error>,
    pub list_err: Option<Error>,
    pub paths_down: bool,
    /// Any call fails the test.
    pub forbidden: bool,
}

pub struct FakeDaemon {
    pub dev_dir: tempfile::TempDir,
    pub state: Mutex<DaemonState>,
}

impl FakeDaemon {
    pub fn new() -> Arc<Self> {
        Arc::new(FakeDaemon {
            dev_dir: tempfile::tempdir().unwrap(),
            state: Mutex::new(DaemonState::default()),
        })
    }

    pub fn device_path(&self, id: i64) -> String {
        self.dev_dir
            .path()
            .join(format!("ublkb{id}"))
            .to_string_lossy()
            .into_owned()
    }

    /// Puts a device in place as if the daemon had attached it.
    pub fn insert(&self, volume: &str, subnqn: &str, id: i64, path: &str) {
        self.state.lock().unwrap().devices.insert(
            volume.into(),
            Device {
                volume: volume.into(),
                subnqn: subnqn.into(),
                dev_id: id,
                path: path.into(),
                paths: Vec::new(),
                existing: false,
            },
        );
    }

    pub fn attaches(&self) -> Vec<AttachRequest> {
        self.state.lock().unwrap().attaches.clone()
    }

    pub fn detaches(&self) -> Vec<String> {
        self.state.lock().unwrap().detaches.clone()
    }

    pub fn lists(&self) -> usize {
        self.state.lock().unwrap().lists
    }

    fn check(&self, state: &DaemonState, op: &str) {
        assert!(!state.forbidden, "nvmeublkd must not be contacted here ({op})");
    }
}

#[tonic::async_trait]
impl Daemon for FakeDaemon {
    async fn attach(&self, req: &AttachRequest, _: Instant) -> Result<Device, Error> {
        let mut state = self.state.lock().unwrap();
        self.check(&state, "attach");
        state.attaches.push(req.clone());
        if let Some(e) = state.attach_err.clone() {
            return Err(e);
        }
        if let Some(existing) = state.devices.get(&req.volume) {
            if existing.subnqn != req.subnqn {
                return Err(Error::Refused(format!(
                    "volume {} is already attached to a different subsystem ({})",
                    req.volume, existing.subnqn
                )));
            }
            let mut existing = existing.clone();
            existing.existing = true;
            return Ok(existing);
        }
        let id = state.next_id;
        state.next_id += 1;
        let path = self.device_path(id);
        std::fs::write(&path, b"").unwrap();
        let device = Device {
            volume: req.volume.clone(),
            subnqn: req.subnqn.clone(),
            dev_id: id,
            path,
            paths: req
                .addrs
                .iter()
                .map(|addr| DevicePath {
                    addr: addr.clone(),
                    up: !state.paths_down,
                })
                .collect(),
            existing: false,
        };
        state.devices.insert(req.volume.clone(), device.clone());
        Ok(device)
    }

    async fn detach(&self, volume: &str, _: Instant) -> Result<bool, Error> {
        let mut state = self.state.lock().unwrap();
        self.check(&state, "detach");
        state.detaches.push(volume.into());
        if let Some(e) = state.detach_err.clone() {
            return Err(e);
        }
        match state.devices.remove(volume) {
            Some(device) => {
                let _ = std::fs::remove_file(&device.path);
                Ok(false)
            }
            None => Ok(true),
        }
    }

    async fn list(&self, _: Instant) -> Result<Vec<Device>, Error> {
        let mut state = self.state.lock().unwrap();
        self.check(&state, "list");
        state.lists += 1;
        if let Some(e) = state.list_err.clone() {
            return Err(e);
        }
        let mut devices: Vec<Device> = state.devices.values().cloned().collect();
        devices.sort_by_key(|d| d.dev_id);
        Ok(devices)
    }
}

pub const HOST_NQN: &str = "nqn.2014-08.org.nvmexpress:uuid:0A1B2C3D-4E5F-6071-8293-A4B5C6D7E8F9";
pub const HOST_ID: &str = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";

pub struct Node {
    pub state: Arc<State>,
    pub host: Arc<FakeHost>,
    pub daemon: Arc<FakeDaemon>,
    pub events: Arc<Recorded>,
    pub dir: tempfile::TempDir,
}

impl Node {
    pub fn socket(&self) -> PathBuf {
        PathBuf::from(&self.state.config.nvmeof.ublk.socket_path)
    }

    pub fn path(&self, name: &str) -> String {
        self.dir.path().join(name).to_string_lossy().into_owned()
    }

    pub fn marked(&self, volume: &str) -> bool {
        crate::ublk_state::marker_exists(&self.socket(), &self.state.driver_name, volume).unwrap()
    }
}

/// A node with the given config (the ublk socket goes under a temporary
/// directory), a node id carrying a UUID host NQN, and no host ID file.
pub fn node(config_yaml: &str, host_nqn: &str, tweak: impl FnOnce(&mut State)) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("run").join("nvmeublkd.sock");
    let mut config = config::parse(config_yaml, |_| None).unwrap();
    config.nvmeof.ublk.socket_path = socket.to_string_lossy().into_owned();
    let node_id = node_id::encode(&NodeIdentity {
        name: "test-node-1".into(),
        nvme_nqn: host_nqn.into(),
        ..Default::default()
    })
    .unwrap();
    let host = Arc::new(FakeHost::default());
    let daemon = FakeDaemon::new();
    let events = Arc::new(Recorded::default());
    let mut state = State::new(
        config,
        "org.scale.csi.nvmeof".into(),
        "test-node-1".into(),
        node_id,
        Arc::new(Metrics::new()),
    );
    let runner: Arc<dyn Runner> = host.clone();
    state.mounter = Mounter {
        runner,
        timeouts: Timeouts::default(),
        proc_mounts: dir.path().join("no-proc-mounts"),
        mountinfo: dir.path().join("mountinfo"),
    };
    host.0.lock().unwrap().mountinfo = Some(dir.path().join("mountinfo"));
    state.ublk = daemon.clone();
    state.events = events.clone();
    state.host = Host {
        dev_dir: daemon.dev_dir.path().to_path_buf(),
        sysfs: dir.path().join("sys"),
        host_id_files: vec![dir.path().join("no-hostid")],
    };
    tweak(&mut state);
    Node {
        state: Arc::new(state),
        host,
        daemon,
        events,
        dir,
    }
}

pub fn assert_no_nvme_cli(host: &FakeHost) {
    for call in host.calls() {
        assert!(
            !call.starts_with("nvme "),
            "nvme-cli must not run on the ublk data path: {call}"
        );
    }
}

pub fn exists(path: &str) -> bool {
    Path::new(path).symlink_metadata().is_ok()
}
