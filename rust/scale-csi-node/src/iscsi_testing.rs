//! A fake iSCSI initiator for node tests: iscsiadm's commands against an
//! in-memory set of targets, with the sysfs, /dev and node database layout the
//! kernel and open-iscsi produce, and a multipathd that builds a dm map once a
//! LUN has two paths.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::exec::Output;
use crate::iscsi::{Iscsi, same_portal, set_record_param, split_portal};
use crate::mount::Runner;
use crate::testing::{HOST_NQN, Node, node};

pub const PORTAL: &str = "192.0.2.30:3260";
pub const PORTAL_B: &str = "192.0.2.31:3260";
pub const PORTAL_C: &str = "192.0.2.32:3260";
pub const VOLUME: &str = "pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
pub const IQN: &str = "iqn.2005-10.org.freenas.ctl:pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
pub const WWID: &str = "6589cfc000000a1b2c3d4e5f60718293";
pub const CONFIG: &str = "iscsi:\n  targetPortal: 192.0.2.30:3260\n";
pub const MULTIPATH_CONFIG: &str =
    "iscsi:\n  targetPortal: 192.0.2.30:3260\n  multipath: true\n  portals: [192.0.2.31, 192.0.2.32]\n";
pub const HINT: &str = r#"["192.0.2.30:3260","192.0.2.31:3260","192.0.2.32:3260"]"#;
pub const USER: &str = "chap-user";
pub const PASSWORD: &str = "s3cret-Pass12";

#[derive(Debug, Clone, Default)]
pub struct FakeTarget {
    /// LUN -> NAA WWID (without the `naa.` designator).
    pub luns: BTreeMap<i64, String>,
    /// (user, password) the target requires.
    pub chap: Option<(String, String)>,
    /// Logins answer "no records found" until a discovery ran.
    pub hidden: bool,
}

#[derive(Debug, Clone)]
pub struct FakeSession {
    pub id: u32,
    pub host: u32,
    pub portal: String,
    pub iqn: String,
    /// LUN -> disk name (`sdb`).
    pub disks: BTreeMap<i64, String>,
}

pub struct FakeIscsi {
    pub sys: PathBuf,
    pub dev: PathBuf,
    /// The node database root records are written to.
    pub db: PathBuf,
    pub targets: BTreeMap<String, FakeTarget>,
    pub sessions: Vec<FakeSession>,
    pub unreachable: Vec<String>,
    /// Builds dm maps for LUNs with two or more paths.
    pub multipathd: bool,
    pub refuse_logout: bool,
    /// Logins fail with this exit code and output instead.
    pub login_failure: Option<(i32, String)>,
    /// Written on `--rescan`: (file, content).
    pub on_rescan: Option<(PathBuf, String)>,
    /// Written on `multipathd resize map <name>`, as multipathd grows a map
    /// only when told to: (file, content).
    pub on_multipath_resize: Option<(PathBuf, String)>,
    /// `multipathd resize map` answers this (exit code, output) instead.
    pub multipath_resize_reply: Option<(i32, String)>,
    /// `multipathd` argv, in order.
    pub multipathd_calls: Vec<String>,
    /// Another LUN identity answers on this portal (a misconfigured portal).
    pub foreign_wwid_on: Option<String>,
    pub discoveries: usize,
    next_session: u32,
    next_host: u32,
    next_disk: u32,
    next_dm: u32,
}

fn out(code: i32, text: &str) -> Output {
    Output {
        code: Some(code),
        stdout: text.as_bytes().to_vec(),
        stderr: Vec::new(),
        wedged: false,
        timed_out: false,
    }
}

fn disk_name(n: u32) -> String {
    let mut name = String::new();
    let mut n = n as i64;
    loop {
        name.insert(0, (b'a' + (n % 26) as u8) as char);
        n = n / 26 - 1;
        if n < 0 {
            break;
        }
    }
    format!("sd{name}")
}

fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

impl FakeIscsi {
    pub fn new(sys: PathBuf, dev: PathBuf, db: PathBuf) -> Self {
        let mut targets = BTreeMap::new();
        targets.insert(
            IQN.to_string(),
            FakeTarget {
                luns: [(0, WWID.to_string())].into(),
                ..Default::default()
            },
        );
        std::fs::create_dir_all(&dev).unwrap();
        FakeIscsi {
            sys,
            dev,
            db,
            targets,
            sessions: Vec::new(),
            unreachable: Vec::new(),
            multipathd: false,
            refuse_logout: false,
            login_failure: None,
            on_rescan: None,
            on_multipath_resize: None,
            multipath_resize_reply: None,
            multipathd_calls: Vec::new(),
            foreign_wwid_on: None,
            discoveries: 0,
            next_session: 1,
            next_host: 2,
            next_disk: 1,
            next_dm: 0,
        }
    }

    pub fn record_path(&self, iqn: &str, portal: &str) -> PathBuf {
        let (host, port) = split_portal(portal);
        self.db.join("nodes").join(iqn).join(format!("{host},{port}"))
    }

    pub fn record(&self, iqn: &str, portal: &str) -> Option<String> {
        std::fs::read_to_string(self.record_path(iqn, portal)).ok()
    }

    /// The device of a session's LUN 0.
    pub fn device_of(&self, portal: &str) -> Option<String> {
        let session = self.sessions.iter().find(|s| same_portal(&s.portal, portal))?;
        Some(self.dev.join(&session.disks[&0]).to_string_lossy().into_owned())
    }

    /// The dm map of a WWID: `/dev/mapper/<name>`.
    pub fn map_of(&self, wwid: &str) -> Option<String> {
        let block = self.sys.join("block");
        for entry in std::fs::read_dir(&block).ok()?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with("dm-") {
                continue;
            }
            let uuid = std::fs::read_to_string(entry.path().join("dm/uuid")).ok()?;
            if uuid.trim() == format!("mpath-3{wwid}") {
                let mapper = std::fs::read_to_string(entry.path().join("dm/name")).ok()?;
                return Some(
                    self.dev
                        .join("mapper")
                        .join(mapper.trim())
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        None
    }

    /// A session someone else (or an earlier stage) logged in.
    pub fn add_session(&mut self, portal: &str, iqn: &str) -> String {
        self.login_session(portal, iqn);
        self.device_of(portal).unwrap()
    }

    fn login_session(&mut self, portal: &str, iqn: &str) {
        let (id, host) = (self.next_session, self.next_host);
        self.next_session += 1;
        self.next_host += 1;
        let session = format!("session{id}");
        write(
            &self.sys.join("class/iscsi_session").join(&session).join("targetname"),
            &format!("{iqn}\n"),
        );
        std::fs::create_dir_all(self.sys.join(format!("class/iscsi_host/host{host}/device/{session}"))).unwrap();
        let mut disks = BTreeMap::new();
        let luns = self.targets.get(iqn).map(|t| t.luns.clone()).unwrap_or_default();
        for (lun, wwid) in luns {
            let disk = disk_name(self.next_disk);
            self.next_disk += 1;
            let wwid = match &self.foreign_wwid_on {
                Some(p) if same_portal(p, portal) => format!("{wwid}ff"),
                _ => wwid,
            };
            let scsi = self.sys.join(format!(
                "devices/platform/host{host}/{session}/target{host}:0:0/{host}:0:0:{lun}"
            ));
            write(&scsi.join("wwid"), &format!("naa.{wwid}\n"));
            std::fs::create_dir_all(
                self.sys
                    .join(format!("class/scsi_device/{host}:0:0:{lun}/device/block/{disk}")),
            )
            .unwrap();
            std::fs::create_dir_all(self.sys.join("block").join(&disk)).unwrap();
            std::os::unix::fs::symlink(&scsi, self.sys.join("block").join(&disk).join("device")).unwrap();
            write(&self.sys.join("class/block").join(&disk).join("size"), "2097152\n");
            write(&self.dev.join(&disk), "");
            disks.insert(lun, disk);
        }
        self.sessions.push(FakeSession {
            id,
            host,
            portal: portal.to_string(),
            iqn: iqn.to_string(),
            disks,
        });
        if self.multipathd {
            self.build_maps(iqn);
        }
    }

    /// A dm map per WWID with two or more paths (multipathd's find_multipaths).
    fn build_maps(&mut self, iqn: &str) {
        let mut paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for session in self.sessions.iter().filter(|s| s.iqn == iqn) {
            for disk in session.disks.values() {
                let wwid = std::fs::read_to_string(self.sys.join("block").join(disk).join("device/wwid")).unwrap();
                let wwid = wwid.trim().trim_start_matches("naa.").to_string();
                paths.entry(wwid).or_default().push(disk.clone());
            }
        }
        for (wwid, disks) in paths.into_iter().filter(|(_, d)| d.len() >= 2) {
            let dm = match self.find_dm(&wwid) {
                Some(dm) => dm,
                None => {
                    let dm = format!("dm-{}", self.next_dm);
                    let name = format!("mpath{}", (b'a' + self.next_dm as u8) as char);
                    self.next_dm += 1;
                    let dir = self.sys.join("block").join(&dm);
                    write(&dir.join("dm/uuid"), &format!("mpath-3{wwid}\n"));
                    write(&dir.join("dm/name"), &format!("{name}\n"));
                    write(&self.dev.join(&dm), "");
                    std::fs::create_dir_all(self.dev.join("mapper")).unwrap();
                    std::os::unix::fs::symlink(format!("../{dm}"), self.dev.join("mapper").join(&name)).unwrap();
                    dm
                }
            };
            for disk in disks {
                std::fs::create_dir_all(self.sys.join("block").join(&dm).join("slaves").join(&disk)).unwrap();
                std::fs::create_dir_all(self.sys.join("block").join(&disk).join("holders").join(&dm)).unwrap();
            }
        }
    }

    fn find_dm(&self, wwid: &str) -> Option<String> {
        for entry in std::fs::read_dir(self.sys.join("block")).ok()?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("dm-")
                && std::fs::read_to_string(entry.path().join("dm/uuid"))
                    .is_ok_and(|u| u.trim() == format!("mpath-3{wwid}"))
            {
                return Some(name);
            }
        }
        None
    }

    fn remove_session(&mut self, index: usize) {
        let session = self.sessions.remove(index);
        let name = format!("session{}", session.id);
        let _ = std::fs::remove_dir_all(self.sys.join("class/iscsi_session").join(&name));
        let _ = std::fs::remove_dir_all(self.sys.join(format!("class/iscsi_host/host{}", session.host)));
        let _ = std::fs::remove_dir_all(self.sys.join(format!("devices/platform/host{}", session.host)));
        for (lun, disk) in &session.disks {
            let _ = std::fs::remove_dir_all(self.sys.join(format!("class/scsi_device/{}:0:0:{lun}", session.host)));
            let _ = std::fs::remove_dir_all(self.sys.join("block").join(disk));
            let _ = std::fs::remove_dir_all(self.sys.join("class/block").join(disk));
            let _ = std::fs::remove_file(self.dev.join(disk));
            // The map loses the path; multipathd removes a map with none left.
            if let Ok(entries) = std::fs::read_dir(self.sys.join("block")) {
                for entry in entries.flatten() {
                    let slave = entry.path().join("slaves").join(disk);
                    if slave.exists() {
                        let _ = std::fs::remove_dir_all(&slave);
                        if std::fs::read_dir(entry.path().join("slaves")).map_or(0, |d| d.count()) == 0 {
                            let dm = entry.file_name().to_string_lossy().into_owned();
                            let name = std::fs::read_to_string(entry.path().join("dm/name")).unwrap_or_default();
                            let _ = std::fs::remove_file(self.dev.join("mapper").join(name.trim()));
                            let _ = std::fs::remove_file(self.dev.join(&dm));
                            let _ = std::fs::remove_dir_all(entry.path());
                        }
                    }
                }
            }
        }
    }

    fn list(&self) -> Output {
        if self.sessions.is_empty() {
            return out(21, "iscsiadm: No active sessions.\n");
        }
        let text: String = self
            .sessions
            .iter()
            .map(|s| format!("tcp: [{}] {},1 {} (non-flash)\n", s.id, s.portal, s.iqn))
            .collect();
        out(0, &text)
    }

    /// `multipathd <args>` (only `resize map <name>` is modelled).
    pub fn run_multipathd(&mut self, args: &[&str]) -> Output {
        self.multipathd_calls.push(args.join(" "));
        match args {
            ["resize", "map", _] if self.multipath_resize_reply.is_some() => {
                let (code, text) = self.multipath_resize_reply.clone().unwrap();
                out(code, &text)
            }
            ["resize", "map", _] if self.multipathd => {
                if let Some((file, content)) = &self.on_multipath_resize {
                    std::fs::write(file, content).unwrap();
                }
                out(0, "ok\n")
            }
            _ => out(1, "fail\n"),
        }
    }

    pub fn run(&mut self, args: &[&str]) -> Output {
        let flag = |name: &str| args.iter().position(|a| *a == name).map(|i| args[i + 1].to_string());
        let (iqn, portal) = (flag("-T").unwrap_or_default(), flag("-p").unwrap_or_default());
        match args {
            ["-m", "session"] => self.list(),
            ["-m", "discovery", "-t", "sendtargets", "-p", _] => {
                self.discoveries += 1;
                for target in self.targets.values_mut() {
                    target.hidden = false;
                }
                out(0, &format!("{portal},1 {IQN}\n"))
            }
            ["-m", "node", "-o", "new", ..] => {
                let path = self.record_path(&iqn, &portal);
                if !path.exists() {
                    write(
                        &path,
                        &format!(
                            "# BEGIN RECORD 2.1.11\nnode.name = {iqn}\nnode.session.auth.authmethod = None\n# END RECORD\n"
                        ),
                    );
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
                }
                out(0, "New iSCSI node added\n")
            }
            [.., "-o", "update", "-n", name, "-v", value] => {
                let path = self.record_path(&iqn, &portal);
                let Ok(text) = std::fs::read_to_string(&path) else {
                    return out(21, "iscsiadm: No records found\n");
                };
                let mode = std::fs::metadata(&path).unwrap().permissions();
                std::fs::write(&path, set_record_param(&text, name, value)).unwrap();
                std::fs::set_permissions(&path, mode).unwrap();
                out(0, "")
            }
            [.., "-o", "delete"] => match std::fs::remove_file(self.record_path(&iqn, &portal)) {
                Ok(()) => out(0, ""),
                Err(_) => out(21, "iscsiadm: No records found\n"),
            },
            [.., "--login"] => self.login(&iqn, &portal),
            [.., "--logout"] => {
                if self.refuse_logout {
                    return out(
                        1,
                        "iscsiadm: Could not logout of all requested sessions: encountered iSCSI driver error (9 - internal error)\n",
                    );
                }
                let mut found = false;
                while let Some(i) = self
                    .sessions
                    .iter()
                    .position(|s| s.iqn == iqn && same_portal(&s.portal, &portal))
                {
                    self.remove_session(i);
                    found = true;
                }
                if found {
                    out(0, "Logout of [...] successful.\n")
                } else {
                    out(21, "iscsiadm: No matching sessions found\n")
                }
            }
            [.., "--rescan"] => {
                if let Some((file, content)) = &self.on_rescan {
                    std::fs::write(file, content).unwrap();
                }
                out(0, "Rescanning session\n")
            }
            _ => out(7, "iscsiadm: unsupported in the fake\n"),
        }
    }

    fn login(&mut self, iqn: &str, portal: &str) -> Output {
        if self
            .sessions
            .iter()
            .any(|s| s.iqn == iqn && same_portal(&s.portal, portal))
        {
            return out(15, "iscsiadm: default: 1 session requested, but 1 already present.\n");
        }
        if let Some((code, text)) = &self.login_failure {
            return out(*code, text);
        }
        if self.unreachable.iter().any(|p| same_portal(p, portal)) {
            return out(
                8,
                "iscsiadm: Could not login to [iface: default]. iscsiadm: initiator reported error (8 - connection timed out)\n",
            );
        }
        let Some(record) = self.record(iqn, portal) else {
            return out(21, "iscsiadm: No records found\n");
        };
        let Some(target) = self.targets.get(iqn).cloned() else {
            return out(21, "iscsiadm: No records found\n");
        };
        if target.hidden {
            return out(21, "iscsiadm: No records found\n");
        }
        if let Some((user, password)) = &target.chap {
            let has = |line: &str| record.lines().any(|l| l.trim() == line);
            let ok = has("node.session.auth.authmethod = CHAP")
                && has(&format!("node.session.auth.username = {user}"))
                && has(&format!("node.session.auth.password = {password}"));
            if !ok {
                return out(
                    24,
                    "iscsiadm: Could not login to [iface: default]. iscsiadm: initiator reported error (24 - iSCSI login failed due to authorization failure)\n",
                );
            }
        }
        self.login_session(portal, iqn);
        out(0, "Login to [iface: default] successful.\n")
    }
}

/// A node with the fake initiator, nvmeublkd forbidden.
pub fn iscsi_node(config: &str) -> Node {
    iscsi_node_with(config, |_| {})
}

pub fn iscsi_node_with(config: &str, tweak: impl FnOnce(&mut FakeIscsi)) -> Node {
    let n = node(config, HOST_NQN, |state| {
        let dir = state.host.sysfs.parent().unwrap().to_path_buf();
        state.iscsi = fake_initiator(&dir, &state.host.dev_dir, state.mounter.runner.clone());
    });
    n.daemon.state.lock().unwrap().forbidden = true;
    let dev = n.daemon.dev_dir.path().to_path_buf();
    let mut fake = FakeIscsi::new(n.dir.path().join("sys"), dev, n.dir.path().join("etc-iscsi"));
    tweak(&mut fake);
    if fake.multipathd {
        write(&fake.dev.join("mapper/control"), "");
        write(&n.dir.path().join("run/multipathd.sock"), "");
    }
    n.host.0.lock().unwrap().iscsi = Some(fake);
    n
}

/// The agent's initiator pointed at the fake's sysfs, /dev and node database.
pub fn fake_initiator(n_dir: &Path, dev: &Path, runner: Arc<dyn Runner>) -> Iscsi {
    let mut iscsi = Iscsi::new(runner, Duration::from_secs(10));
    iscsi.sysfs = n_dir.join("sys");
    iscsi.dev = dev.to_path_buf();
    iscsi.node_db_roots = vec![n_dir.join("etc-iscsi"), n_dir.join("var-lib-iscsi")];
    iscsi.multipathd_sockets = vec![n_dir.join("run/multipathd.sock")];
    iscsi.discovery_retry_delay = Duration::from_millis(10);
    iscsi.session_refresh = Duration::from_millis(20);
    iscsi
}

pub fn fake<R>(n: &Node, f: impl FnOnce(&mut FakeIscsi) -> R) -> R {
    f(n.host.0.lock().unwrap().iscsi.as_mut().unwrap())
}

pub fn iscsi_calls(n: &Node) -> Vec<String> {
    n.host
        .calls()
        .into_iter()
        .filter(|c| c.starts_with("iscsiadm "))
        .collect()
}
