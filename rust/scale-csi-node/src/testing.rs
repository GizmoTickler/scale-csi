//! Fakes for node tests: a host whose mount table, filesystems and command log
//! live in memory, and an in-memory nvmeublkd whose devices are files in a
//! temporary directory (so a staging link resolves as a real /dev/ublkbN
//! would).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::config;
use crate::csi::{self, volume_capability};
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
    /// Commands (by prefix of "program args") that fail with exit 32.
    pub failing: Vec<String>,
    /// Targets with a second mount stacked underneath: one `umount` lifts the
    /// top one and the target stays mounted.
    pub stacked: Vec<String>,
    /// A fake kernel NVMe initiator, when set.
    pub kernel: Option<FakeKernel>,
    /// A fake iSCSI initiator, when set.
    pub iscsi: Option<crate::iscsi_testing::FakeIscsi>,
    /// device -> filesystem
    pub filesystems: HashMap<String, String>,
    /// The NFS version an NFS mount negotiates (shown as `vers=`); 4.2 when
    /// unset. A version 3 mount shows as `nfs`, any other as `nfs4`.
    pub nfs_version: Option<String>,
    /// The RPC's deadline is spent once a command with this prefix has run:
    /// any later command bounded by the deadline is refused, as the real
    /// runner refuses one whose deadline has passed.
    pub deadline_spent_after: Option<String>,
    deadline_spent: bool,
    pub calls: Vec<String>,
    /// Paths looked at directly through `Host::lstat` and `Host::read_link`:
    /// "lstat <path>", "readlink <path>".
    pub path_probes: Vec<String>,
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

/// The fake device number of a device name.
pub fn fake_device_number(name: &str) -> u64 {
    name.bytes()
        .fold(7u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)))
}

/// The sysfs `dev` file ("MAJ:MIN") of a fake device (FakeHost::device_number).
pub fn sysfs_dev(name: &str) -> String {
    let number = fake_device_number(name);
    format!("{}:{}\n", libc::major(number), libc::minor(number))
}

impl FakeHost {
    /// A fake device number: a bind target reports its device's; a device
    /// path (symlinks resolved, as stat(2) does) reports one derived from its
    /// name, or a stale one while the fake iSCSI or kernel NVMe initiator
    /// still models the previous disk's node under that name.
    pub fn device_number(&self, path: &str) -> Option<u64> {
        let mut host = self.0.lock().unwrap();
        let device = match host.mounts.get(path) {
            // As a hung server would: the agent must never stat one.
            Some((_, fs, _)) if fs.starts_with("nfs") => panic!("stat of the network mount {path}"),
            Some((source, fs, _)) if fs == "devtmpfs" => source
                .strip_prefix("udev[")
                .and_then(|s| s.strip_suffix(']'))
                .unwrap_or(source)
                .to_string(),
            _ => std::fs::canonicalize(path).map_or_else(|_| path.to_string(), |p| p.to_string_lossy().into_owned()),
        };
        let name = std::path::Path::new(&device).file_name()?.to_str()?;
        let is_device =
            name.starts_with("ublkb") || name.starts_with("nvme") || name.starts_with("sd") || name.starts_with("dm-");
        if !is_device {
            return None;
        }
        let stale = match host.iscsi.as_mut().and_then(|f| f.stale.get_mut(name)) {
            Some(stale) => Some(stale),
            None => host.kernel.as_mut().and_then(|k| k.stale.get_mut(name)),
        };
        if let Some(stale) = stale
            && *stale > 0
        {
            *stale -= 1;
            return Some(fake_device_number(name) ^ 0xff);
        }
        Some(fake_device_number(name))
    }

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

    /// The path probes (`Host::lstat`, `Host::read_link`) that named `path`.
    pub fn probes_of(&self, path: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .path_probes
            .iter()
            .filter(|p| p.split_once(' ').is_some_and(|(_, probed)| probed == path))
            .cloned()
            .collect()
    }

    pub fn is_mounted(&self, target: &str) -> bool {
        self.0.lock().unwrap().mounts.contains_key(target)
    }
}

#[tonic::async_trait]
impl Runner for FakeHost {
    async fn run(&self, program: &str, args: &[&str], limits: Limits) -> std::io::Result<Output> {
        let mut host = self.0.lock().unwrap();
        let call = format!("{program} {}", args.join(" "));
        host.calls.push(call.clone());
        if host.deadline_spent && limits.rpc_deadline.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("deadline passed before running {program}"),
            ));
        }
        if host
            .deadline_spent_after
            .as_ref()
            .is_some_and(|prefix| call.starts_with(prefix.as_str()))
        {
            host.deadline_spent = true;
        }
        if host.failing.iter().any(|prefix| call.starts_with(prefix.as_str())) {
            return Ok(Output {
                code: Some(32),
                stdout: Vec::new(),
                stderr: b"mount: permission denied".to_vec(),
                wedged: false,
                timed_out: false,
            });
        }
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
            ("findmnt", ["-n", "-r", "-o", "SOURCE,TARGET", "-t", _]) => {
                let rows: String = host
                    .mounts
                    .iter()
                    .filter(|(_, (_, fs, _))| ["ext4", "ext3", "xfs", "btrfs"].contains(&fs.as_str()))
                    .map(|(target, (source, _, _))| format!("{source} {}\n", target.replace(' ', "\\x20")))
                    .collect();
                if rows.is_empty() {
                    output(1, "")
                } else {
                    output(0, &rows)
                }
            }
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
                    // As findmnt shows it: devtmpfs with the node's path as root.
                    _ if bound => (format!("udev[{source}]"), "devtmpfs".to_string(), "rw".to_string()),
                    // As the kernel shows an NFS mount: its negotiated version.
                    _ if fs == "nfs" => {
                        let version = host.nfs_version.clone().unwrap_or_else(|| "4.2".into());
                        let fs = if version.starts_with('3') { "nfs" } else { "nfs4" };
                        let ro = args
                            .iter()
                            .position(|a| *a == "-o")
                            .is_some_and(|i| args[i + 1].split(',').any(|o| o == "ro"));
                        let mode = if ro { "ro" } else { "rw" };
                        (source.to_string(), fs.to_string(), format!("{mode},vers={version}"))
                    }
                    _ => (source.to_string(), fs, "rw".to_string()),
                };
                host.mounts.insert(target.into(), entry);
                host.sync_mountinfo();
                output(0, "")
            }
            ("umount", [target]) => {
                if let Some(i) = host.stacked.iter().position(|t| t == target) {
                    host.stacked.remove(i);
                    return Ok(output(0, ""));
                }
                host.mounts.remove(*target);
                host.sync_mountinfo();
                output(0, "")
            }
            ("resize2fs" | "xfs_growfs" | "btrfs", _) => output(0, ""),
            ("nvme", _) if host.kernel.is_some() => host.kernel.as_mut().unwrap().run(args),
            ("iscsiadm", _) if host.iscsi.is_some() => host.iscsi.as_mut().unwrap().run(args),
            ("multipathd", _) if host.iscsi.is_some() => host.iscsi.as_mut().unwrap().run_multipathd(args),
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
        kubelet_dir: dir.path().join("kubelet"),
        sysfs: dir.path().join("sys"),
        host_id_files: vec![dir.path().join("no-hostid")],
        // A bind target reports its device's number (see FakeHost::device_number).
        device_number: {
            let numbers = host.clone();
            Arc::new(move |path: &str| Ok(numbers.device_number(path)))
        },
        // The real calls, logged in HostState::path_probes.
        lstat: {
            let probes = host.clone();
            Arc::new(move |path: &str| {
                probes.0.lock().unwrap().path_probes.push(format!("lstat {path}"));
                std::fs::symlink_metadata(path)
            })
        },
        read_link: {
            let probes = host.clone();
            Arc::new(move |path: &str| {
                probes.0.lock().unwrap().path_probes.push(format!("readlink {path}"));
                std::fs::read_link(path)
            })
        },
    };
    let nvme_runner: Arc<dyn Runner> = host.clone();
    state.nvme = crate::nvme::Nvme {
        runner: nvme_runner,
        timeout: std::time::Duration::from_secs(30),
        sysfs: dir.path().join("sys"),
        dev: daemon.dev_dir.path().to_path_buf(),
        // stat(2) fails on a missing node; the host's fake numbers the rest.
        device_number: {
            let numbers = host.clone();
            Arc::new(move |path: &str| {
                std::fs::metadata(path)?;
                Ok(numbers.device_number(path))
            })
        },
    };
    state.nvme_sessions =
        Some(crate::session_registry::SessionRegistry::at(dir.path().join("sessions/nvmeof")).unwrap());
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

pub const VOLUME: &str = "pvc-ublk-1";
pub const NQN: &str = "nqn.2011-06.com.example:pvc-ublk-1";
pub const UBLK_ON: &str = "nvmeof:\n  ublk:\n    enabled: true\n";

pub fn context(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut c: HashMap<String, String> = [
        ("node_attach_driver", "nvmeof"),
        ("nqn", NQN),
        ("transport", "tcp"),
        ("address", "192.0.2.20"),
        ("port", "4420"),
        ("nvmeof/dataPath", "ublk"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (k, v) in extra {
        c.insert(k.to_string(), v.to_string());
    }
    c
}

pub fn block() -> csi::VolumeCapability {
    csi::VolumeCapability {
        access_type: Some(volume_capability::AccessType::Block(Default::default())),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

pub fn filesystem() -> csi::VolumeCapability {
    csi::VolumeCapability {
        access_type: Some(volume_capability::AccessType::Mount(volume_capability::MountVolume {
            fs_type: "ext4".into(),
            ..Default::default()
        })),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

/// A kernel NVMe initiator as nvme-cli and sysfs show it: `connect` adds a
/// live controller (and, for a new subsystem, its sysfs entries and one
/// namespace device), `disconnect` removes the subsystem, `list-subsys` lists
/// them in nvme-cli 2.x's shape. Addresses in `unreachable` refuse connects.
/// (subsystem index, [(controller, address, state)])
pub type FakeSubsystem = (u32, Vec<(String, String, String)>);

pub struct FakeKernel {
    pub sys: PathBuf,
    pub dev: PathBuf,
    pub subsystems: BTreeMap<String, FakeSubsystem>,
    pub unreachable: Vec<String>,
    /// Disconnects fail (EINVAL), as for a controller the kernel will not drop.
    pub refuse_disconnect: bool,
    /// Written on `ns-rescan`: (file, content), e.g. a grown size.
    pub on_rescan: Option<(PathBuf, String)>,
    /// For this many stats after a connect, a new subsystem's namespace /dev
    /// node reports a stale device number: the node the previous namespace of
    /// that name left behind, before devtmpfs/udev catch up.
    pub stale_polls: u32,
    /// Device name -> stats left that report the stale number.
    pub stale: BTreeMap<String, u32>,
    next_subsystem: u32,
    next_controller: u32,
}

impl FakeKernel {
    pub fn new(sys: PathBuf, dev: PathBuf) -> Self {
        std::fs::create_dir_all(sys.join("class/nvme-subsystem")).unwrap();
        std::fs::create_dir_all(sys.join("class/nvme")).unwrap();
        std::fs::create_dir_all(&dev).unwrap();
        FakeKernel {
            sys,
            dev,
            subsystems: BTreeMap::new(),
            unreachable: Vec::new(),
            refuse_disconnect: false,
            on_rescan: None,
            stale_polls: 0,
            stale: BTreeMap::new(),
            next_subsystem: 0,
            next_controller: 0,
        }
    }

    /// The namespace device of a subsystem.
    pub fn device(&self, nqn: &str) -> Option<String> {
        let (index, _) = self.subsystems.get(nqn)?;
        Some(self.dev.join(format!("nvme{index}n1")).to_string_lossy().into_owned())
    }

    /// A live controller connected by someone else (or a previous stage).
    pub fn add_live(&mut self, nqn: &str, address: &str) {
        self.add_path(nqn, address, "live");
    }

    /// A subsystem whose controllers all lost their connection.
    pub fn add_dead(&mut self, nqn: &str, address: &str) {
        self.add_path(nqn, address, "connecting");
    }

    fn add_path(&mut self, nqn: &str, address: &str, state: &str) {
        self.add_controller(nqn, address, state, "off");
    }

    /// A controller with its sysfs attributes; returns its name.
    pub fn add_controller(&mut self, nqn: &str, address: &str, state: &str, fast_io_fail_tmo: &str) -> String {
        let controller = format!("nvme{}", self.next_controller);
        self.next_controller += 1;
        let dir = self.sys.join("class/nvme").join(&controller);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("subsysnqn"), format!("{nqn}\n")).unwrap();
        std::fs::write(dir.join("transport"), "tcp\n").unwrap();
        std::fs::write(dir.join("address"), format!("traddr={address},trsvcid=4420\n")).unwrap();
        std::fs::write(dir.join("fast_io_fail_tmo"), format!("{fast_io_fail_tmo}\n")).unwrap();
        if !self.subsystems.contains_key(nqn) {
            let index = self.next_subsystem;
            self.next_subsystem += 1;
            let dir = self.sys.join(format!("class/nvme-subsystem/nvme-subsys{index}"));
            // The native multipath head, nvme<subsystem instance>n1.
            let namespace = format!("nvme{index}n1");
            std::fs::create_dir_all(dir.join(&namespace)).unwrap();
            std::fs::write(dir.join(&namespace).join("dev"), sysfs_dev(&namespace)).unwrap();
            std::fs::write(dir.join("subsysnqn"), format!("{nqn}\n")).unwrap();
            std::fs::write(dir.join("iopolicy"), "numa").unwrap();
            std::fs::write(self.dev.join(&namespace), b"").unwrap();
            if self.stale_polls > 0 {
                self.stale.insert(namespace, self.stale_polls);
            }
            self.subsystems.insert(nqn.to_string(), (index, Vec::new()));
        }
        let entry = self.subsystems.get_mut(nqn).unwrap();
        entry
            .1
            .push((controller.clone(), address.to_string(), state.to_string()));
        controller
    }

    fn remove(&mut self, nqn: &str) -> bool {
        let Some((index, paths)) = self.subsystems.remove(nqn) else {
            return false;
        };
        let _ = std::fs::remove_dir_all(self.sys.join(format!("class/nvme-subsystem/nvme-subsys{index}")));
        let _ = std::fs::remove_file(self.dev.join(format!("nvme{index}n1")));
        for (controller, _, _) in paths {
            let _ = std::fs::remove_dir_all(self.sys.join("class/nvme").join(controller));
        }
        true
    }

    fn list_json(&self) -> String {
        let subsystems: Vec<serde_json::Value> = self
            .subsystems
            .iter()
            .map(|(nqn, (index, paths))| {
                serde_json::json!({
                    "Name": format!("nvme-subsys{index}"),
                    "NQN": nqn,
                    "Paths": paths.iter().map(|(name, address, state)| serde_json::json!({
                        "Name": name,
                        "Transport": "tcp",
                        "Address": format!("traddr={address},trsvcid=4420"),
                        "State": state,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::json!([{"HostNQN": "nqn.host", "Subsystems": subsystems}]).to_string()
    }

    fn run(&mut self, args: &[&str]) -> Output {
        let flag = |name: &str| args.iter().position(|a| *a == name).map(|i| args[i + 1].to_string());
        match args.first().copied() {
            Some("list-subsys") => output(0, &self.list_json()),
            Some("connect") => {
                let (nqn, address) = (flag("-n").unwrap(), flag("-a").unwrap());
                if self.unreachable.contains(&address) {
                    return Output {
                        code: Some(1),
                        stdout: Vec::new(),
                        stderr: b"could not add new controller: Connection refused".to_vec(),
                        wedged: false,
                        timed_out: false,
                    };
                }
                let live = self
                    .subsystems
                    .get(&nqn)
                    .is_some_and(|(_, paths)| paths.iter().any(|(_, a, s)| *a == address && s == "live"));
                if live {
                    return output(1, "already connected");
                }
                self.add_path(&nqn, &address, "live");
                output(0, "")
            }
            Some("disconnect") if self.refuse_disconnect => output(1, "Failed to disconnect: Invalid argument"),
            Some("disconnect") => {
                let nqn = flag("-n").unwrap();
                if self.remove(&nqn) {
                    output(0, &format!("NQN:{nqn} disconnected 1 controller(s)"))
                } else {
                    output(1, &format!("{nqn} not found"))
                }
            }
            Some("ns-rescan") => {
                if let Some((file, content)) = &self.on_rescan {
                    std::fs::write(file, content).unwrap();
                }
                output(0, "")
            }
            _ => output(127, ""),
        }
    }
}
