//! The NVMe-oF request-size clamp, `nvmeof.maxSectorsKB` (Go
//! `pkg/util/nvme_max_sectors.go`).
//!
//! TrueNAS 27 ships a Linux 6.18 target whose `nvmet_tcp_map_data()` rejects
//! any command carrying more than `NVMET_TCP_MAXH2CDATA` (4 MiB) of data with
//! "SGL invalid data" (sct 0x0 / sc 0xf, DNR), while Identify Controller still
//! reports MDTS=0 (no limit). The initiator trusts MDTS, leaves
//! `max_sectors_kb` at 32768 and issues commands of up to 32 MiB; every one
//! fails, ext4 aborts its journal and remounts read-only.
//!
//! The size that reaches the wire is bounded by the PATH devices
//! (`nvme<S>c<C>n<N>`, hidden gendisks under each controller), not only by the
//! multipath head (`nvme<S>n<N>`): the path request queue merges bios back up
//! to its own limit. Hidden gendisks have no /dev node and send no uevents, so
//! a udev rule cannot reach them. The clamp writes `queue/max_sectors_kb` on
//! the head AND every path device of the subsystem, after the paths are
//! connected and before anything is mounted or published, and only ever
//! lowers a value.
//!
//! - `auto` (default): NVMe/TCP subsystems are clamped to 4096 KiB when the
//!   controller reports MDTS=0, or when MDTS cannot be read (lowering is
//!   always safe, mounting unprotected is not). MDTS>0 is left to the kernel,
//!   which honors it. `nvme id-ctrl` runs only when some device is above the
//!   ceiling, so a converged subsystem costs sysfs reads only.
//! - a whole number: a fixed ceiling for every subsystem, any transport.
//! - `off` or `0`: disabled.
//!
//! A clamp that is required and cannot be proven in place is an error: the
//! stage fails rather than mount. Both node agents run the same vectors
//! (`pkg/util/testdata/nvme_max_sectors_vectors.json`), so either may stage a
//! subsystem the other connected.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::nvme::Nvme;

/// `NVMET_TCP_MAXH2CDATA` (0x400000 bytes) in KiB.
pub const AUTO_LIMIT_KB: u32 = 4096;
/// One 4 KiB page: the smallest value the block layer accepts anywhere.
const MIN_LIMIT_KB: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    #[default]
    Auto,
    Fixed(u32),
    Off,
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Policy::Auto => f.write_str("auto"),
            Policy::Off => f.write_str("off"),
            Policy::Fixed(kb) => write!(f, "{kb}"),
        }
    }
}

impl Policy {
    /// `""`/`auto`, `off`/`0`, or a whole number of KiB (at least 4).
    pub fn parse(raw: &str) -> Result<Policy, String> {
        let value = raw.trim().to_ascii_lowercase();
        match value.as_str() {
            "" | "auto" => return Ok(Policy::Auto),
            // `false`: YAML 1.1 readers (this one included) load a bare `off`
            // as a boolean.
            "off" | "0" | "false" => return Ok(Policy::Off),
            _ => {}
        }
        let kb: i64 = value
            .parse()
            .map_err(|_| format!("nvmeof.maxSectorsKB {raw:?} is not auto, off or a whole number of KiB"))?;
        if kb < i64::from(MIN_LIMIT_KB) {
            return Err(format!(
                "nvmeof.maxSectorsKB {kb} must be at least {MIN_LIMIT_KB} (or 0/off to disable)"
            ));
        }
        u32::try_from(kb)
            .map(Policy::Fixed)
            .map_err(|_| format!("nvmeof.maxSectorsKB {kb} is too large"))
    }

    /// The config value: absent or null is auto; a string or an integer.
    pub fn from_config(value: Option<&Value>) -> Result<Policy, String> {
        match value {
            None | Some(Value::Null) => Ok(Policy::Auto),
            Some(Value::String(s)) => Policy::parse(s),
            Some(Value::Bool(false)) => Ok(Policy::Off),
            Some(Value::Number(n)) => match n.as_i64() {
                Some(i) => Policy::parse(&i.to_string()),
                None => Err(format!("nvmeof.maxSectorsKB {n} is not a whole number of KiB")),
            },
            Some(other) => Err(format!(
                "nvmeof.maxSectorsKB {other} is not auto, off or a whole number of KiB"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceOutcome {
    pub name: String,
    pub head: bool,
    /// `None` when `queue/max_sectors_kb` could not be read.
    pub before: Option<u32>,
    pub after: Option<u32>,
}

/// One clamp pass, for the stage's log line.
#[derive(Debug, Clone, Default)]
pub struct Report {
    pub nqn: String,
    pub policy: Policy,
    pub transport: String,
    /// `Some(None)`: asked and the query failed.
    pub mdts: Option<Option<u32>>,
    /// The ceiling enforced; `None` when nothing is enforced.
    pub limit: Option<u32>,
    /// Why nothing is enforced.
    pub skipped: Option<String>,
    pub devices: Vec<DeviceOutcome>,
    /// Controllers that are not live and have no path device yet.
    pub waiting: Vec<String>,
}

impl Report {
    pub fn changed(&self) -> usize {
        self.devices.iter().filter(|d| d.before != d.after).count()
    }
}

fn kb_text(kb: Option<u32>, unknown: &str) -> String {
    kb.map_or_else(|| unknown.to_string(), |kb| kb.to_string())
}

/// The same line the Go plugin logs.
impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "policy={} transport={}", self.policy, self.transport)?;
        match self.mdts {
            Some(None) => f.write_str(" mdts=unknown")?,
            Some(Some(mdts)) => write!(f, " mdts={mdts}")?,
            None => {}
        }
        match (&self.skipped, self.limit) {
            (Some(why), _) => write!(f, " not enforced ({why})")?,
            (None, limit) => write!(f, " limit={}KiB changed={}", limit.unwrap_or(0), self.changed())?,
        }
        let devices: Vec<String> = self
            .devices
            .iter()
            .map(|d| format!("{} {}->{}", d.name, kb_text(d.before, "?"), kb_text(d.after, "-1")))
            .collect();
        write!(f, " devices=[{}]", devices.join(", "))?;
        if !self.waiting.is_empty() {
            write!(f, " waiting=[{}]", self.waiting.join(", "))?;
        }
        Ok(())
    }
}

/// A required clamp that is not in place.
#[derive(Debug)]
pub struct Failure {
    pub report: Box<Report>,
    pub reason: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NVMe max_sectors_kb clamp failed for {}: {} [{}]",
            self.report.nqn, self.reason, self.report
        )
    }
}

pub struct ClampOptions<'a> {
    pub policy: Policy,
    /// The volume's transport, used only when sysfs reports none.
    pub transport: &'a str,
    /// How long to wait for every live controller to expose its path device
    /// (the namespace scan runs after `nvme connect` returns).
    pub path_wait: Duration,
    pub deadline: Option<Instant>,
}

struct Device {
    name: String,
    queue: PathBuf,
    head: bool,
}

struct Controller {
    name: String,
    state: String,
    transport: String,
    has_path: bool,
}

#[derive(Default)]
struct Snapshot {
    found: bool,
    controllers: Vec<Controller>,
    devices: Vec<Device>,
}

impl Snapshot {
    fn pending_live(&self) -> Vec<String> {
        self.controllers
            .iter()
            .filter(|c| c.state == "live" && !c.has_path)
            .map(|c| c.name.clone())
            .collect()
    }
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// `nvmeN`.
fn is_controller(name: &str) -> bool {
    name.strip_prefix("nvme").is_some_and(digits)
}

/// `nvmeSnN` (a head, or a namespace without native multipath).
fn is_head(name: &str) -> bool {
    name.strip_prefix("nvme")
        .and_then(|rest| rest.split_once('n'))
        .is_some_and(|(s, n)| digits(s) && digits(n))
}

/// `nvmeScCnN` or `nvmeCnN`: a namespace device under a controller.
fn is_namespace(name: &str) -> bool {
    if is_head(name) {
        return true;
    }
    name.strip_prefix("nvme")
        .and_then(|rest| rest.split_once('c'))
        .and_then(|(s, rest)| rest.split_once('n').map(|(c, n)| (s, c, n)))
        .is_some_and(|(s, c, n)| digits(s) && digits(c) && digits(n))
}

fn names(dir: &Path, keep: impl Fn(&str) -> bool) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<String> = entries
        .flatten()
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| keep(n))
        .collect();
    out.sort();
    out
}

fn trimmed(path: &Path) -> String {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Every block device of every subsystem directory carrying `nqn` (more than
/// one with `nvme_core.multipath=N`): path devices first, heads last.
fn scan(subsystem_root: &Path, nqn: &str) -> Snapshot {
    let mut snap = Snapshot::default();
    let mut heads = Vec::new();
    for subsystem in names(subsystem_root, |n| n.starts_with("nvme-subsys")) {
        let dir = subsystem_root.join(&subsystem);
        if trimmed(&dir.join("subsysnqn")) != nqn {
            continue;
        }
        snap.found = true;
        for name in names(&dir, is_head) {
            heads.push(Device {
                queue: dir.join(&name).join("queue"),
                name,
                head: true,
            });
        }
        for controller in names(&dir, is_controller) {
            let cdir = dir.join(&controller);
            let paths = names(&cdir, is_namespace);
            snap.controllers.push(Controller {
                state: trimmed(&cdir.join("state")),
                transport: trimmed(&cdir.join("transport")).to_ascii_lowercase(),
                has_path: !paths.is_empty(),
                name: controller,
            });
            for name in paths {
                snap.devices.push(Device {
                    queue: cdir.join(&name).join("queue"),
                    name,
                    head: false,
                });
            }
        }
    }
    snap.devices.extend(heads);
    snap
}

fn read_kb(queue: &Path) -> Option<u32> {
    trimmed(&queue.join("max_sectors_kb")).parse().ok()
}

fn write_kb(queue: &Path, kb: u32) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(queue.join("max_sectors_kb"))?;
    std::io::Write::write_all(&mut file, kb.to_string().as_bytes())
}

/// `mdts` from `nvme id-ctrl -o json`.
pub fn parse_mdts(output: &[u8]) -> Result<u32, String> {
    #[derive(serde::Deserialize)]
    struct IdCtrl {
        mdts: Option<i64>,
    }
    let id: IdCtrl = serde_json::from_slice(output).map_err(|e| format!("parse id-ctrl output: {e}"))?;
    id.mdts
        .and_then(|m| u32::try_from(m).ok())
        .ok_or_else(|| "id-ctrl output has no mdts".to_string())
}

/// Lowers `queue/max_sectors_kb` on the head and every path device of the
/// subsystem carrying `nqn`, as the policy requires; never raises one.
pub async fn clamp(nvme: &Nvme, nqn: &str, options: &ClampOptions<'_>) -> Result<Report, Failure> {
    let mut report = Report {
        nqn: nqn.to_string(),
        policy: options.policy,
        transport: options.transport.trim().to_ascii_lowercase(),
        ..Default::default()
    };
    if options.policy == Policy::Off {
        report.skipped = Some("disabled".into());
        return Ok(report);
    }
    let root = nvme.sysfs.join("class/nvme-subsystem");
    let start = Instant::now();
    let mut interval = Duration::from_millis(50);
    let snap = loop {
        let snap = scan(&root, nqn);
        if snap.found && !snap.devices.is_empty() && snap.pending_live().is_empty() {
            break snap;
        }
        if start.elapsed() >= options.path_wait {
            break snap;
        }
        if options.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(Failure {
                report: Box::new(report),
                reason: "context canceled waiting for path devices".into(),
            });
        }
        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(Duration::from_millis(200));
    };
    report.waiting = snap
        .controllers
        .iter()
        .filter(|c| c.state != "live" && !c.has_path)
        .map(|c| c.name.clone())
        .collect();
    let fail = |report: Report, reason: String| {
        Err(Failure {
            report: Box::new(report),
            reason,
        })
    };
    if !snap.found {
        return fail(report, "subsystem is not in sysfs".into());
    }
    if snap.devices.is_empty() {
        return fail(report, "subsystem has no block devices in sysfs".into());
    }
    let pending = snap.pending_live();
    if !pending.is_empty() {
        return fail(
            report,
            format!(
                "live controller(s) {} have no namespace path device yet",
                pending.join(", ")
            ),
        );
    }
    let before: Vec<Option<u32>> = snap.devices.iter().map(|d| read_kb(&d.queue)).collect();
    report.devices = snap
        .devices
        .iter()
        .zip(&before)
        .map(|(d, kb)| DeviceOutcome {
            name: d.name.clone(),
            head: d.head,
            before: *kb,
            after: *kb,
        })
        .collect();

    let limit = match options.policy {
        Policy::Fixed(kb) => kb,
        Policy::Off => unreachable!("returned above"),
        Policy::Auto => {
            let transports: Vec<&str> = snap
                .controllers
                .iter()
                .map(|c| c.transport.as_str())
                .filter(|t| !t.is_empty())
                .collect();
            if let Some(first) = transports.first() {
                report.transport = first.to_string();
            }
            let tcp = if transports.is_empty() {
                report.transport.is_empty() || report.transport == "tcp"
            } else {
                transports.contains(&"tcp")
            };
            if !tcp {
                report.skipped = Some("auto applies to tcp only".into());
                return Ok(report);
            }
            if !before.iter().any(|kb| kb.is_none_or(|kb| kb > AUTO_LIMIT_KB)) {
                // Already within the ceiling: MDTS is irrelevant.
                report.limit = Some(AUTO_LIMIT_KB);
                return Ok(report);
            }
            // One live controller answers for the subsystem; none live, or a
            // failed query, leaves MDTS unknown and the clamp applies.
            let mut mdts = None;
            if let Some(live) = snap.controllers.iter().find(|c| c.state == "live") {
                mdts = nvme.id_ctrl_mdts(&live.name, options.deadline).await.ok();
            }
            report.mdts = Some(mdts);
            if let Some(m) = mdts.filter(|m| *m > 0) {
                report.skipped = Some(format!("controller reports MDTS={m}, which the kernel honors"));
                return Ok(report);
            }
            AUTO_LIMIT_KB
        }
    };
    report.limit = Some(limit);

    let mut failures = Vec::new();
    for (i, device) in snap.devices.iter().enumerate() {
        if before[i].is_some_and(|kb| kb <= limit) {
            continue;
        }
        if let Err(e) = write_kb(&device.queue, limit) {
            failures.push(format!("write {}: {e}", device.queue.join("max_sectors_kb").display()));
            continue;
        }
        let after = read_kb(&device.queue);
        report.devices[i].after = after;
        if after.is_none_or(|kb| kb > limit) {
            failures.push(format!(
                "{} reads back {} after writing {limit}",
                device.name,
                kb_text(after, "-1")
            ));
        }
    }
    if failures.is_empty() {
        Ok(report)
    } else {
        fail(report, failures.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use serde::Deserialize;

    use super::*;
    use crate::exec::{Limits, Output};
    use crate::mount::Runner;

    /// `nvme id-ctrl` answers with `mdts` (or fails, for `None`); every call
    /// is recorded.
    struct IdCtrl {
        mdts: Option<u32>,
        calls: Mutex<Vec<String>>,
    }

    #[tonic::async_trait]
    impl Runner for IdCtrl {
        async fn run(&self, program: &str, args: &[&str], _: Limits) -> std::io::Result<Output> {
            self.calls.lock().unwrap().push(format!("{program} {}", args.join(" ")));
            let (code, stdout) = match self.mdts {
                Some(m) => (0, format!("{{\"vid\":0,\"sn\":\"x\",\"mdts\":{m},\"subnqn\":\"nqn\"}}")),
                None => (1, String::new()),
            };
            Ok(Output {
                code: Some(code),
                stdout: stdout.into_bytes(),
                stderr: Vec::new(),
                wedged: false,
                timed_out: false,
            })
        }
    }

    struct Fake {
        _dir: tempfile::TempDir,
        nvme: Nvme,
        runner: Arc<IdCtrl>,
        queues: BTreeMap<String, PathBuf>,
    }

    impl Fake {
        fn new(mdts: Option<u32>) -> Fake {
            let dir = tempfile::tempdir().unwrap();
            let sys = dir.path().join("sys");
            std::fs::create_dir_all(sys.join("class/nvme-subsystem")).unwrap();
            std::fs::create_dir_all(sys.join("class/nvme")).unwrap();
            let runner = Arc::new(IdCtrl {
                mdts,
                calls: Mutex::new(Vec::new()),
            });
            let nvme = Nvme {
                runner: runner.clone(),
                timeout: Duration::from_secs(5),
                sysfs: sys,
                dev: dir.path().join("dev"),
                device_number: Arc::new(|_: &str| Ok(None)),
            };
            Fake {
                _dir: dir,
                nvme,
                runner,
                queues: BTreeMap::new(),
            }
        }

        fn subsystem(&self, name: &str, nqn: &str) -> PathBuf {
            let dir = self.nvme.sysfs.join("class/nvme-subsystem").join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("subsysnqn"), format!("{nqn}\n")).unwrap();
            dir
        }

        /// A controller under class/nvme, linked by name from its subsystem.
        fn controller(&self, subsystem: &Path, name: &str, state: &str, transport: &str) -> PathBuf {
            let dir = self.nvme.sysfs.join("class/nvme").join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("state"), format!("{state}\n")).unwrap();
            std::fs::write(dir.join("transport"), format!("{transport}\n")).unwrap();
            std::os::unix::fs::symlink(&dir, subsystem.join(name)).unwrap();
            dir
        }

        fn disk(&mut self, parent: &Path, name: &str, kb: u32, writable: bool) {
            let queue = parent.join(name).join("queue");
            std::fs::create_dir_all(&queue).unwrap();
            if writable {
                std::fs::write(queue.join("max_sectors_kb"), format!("{kb}\n")).unwrap();
            } else {
                // Neither readable as a value nor writable, even by root.
                std::fs::create_dir_all(queue.join("max_sectors_kb")).unwrap();
            }
            self.queues.insert(name.to_string(), queue);
        }

        fn value(&self, name: &str) -> i64 {
            read_kb(&self.queues[name]).map_or(-1, i64::from)
        }

        fn mdts_asked(&self) -> usize {
            self.runner.calls.lock().unwrap().len()
        }
    }

    fn options(policy: Policy, transport: &str, path_wait: Duration) -> ClampOptions<'_> {
        ClampOptions {
            policy,
            transport,
            path_wait,
            deadline: None,
        }
    }

    #[derive(Deserialize)]
    struct VectorController {
        name: String,
        state: String,
        transport: String,
        paths: BTreeMap<String, u32>,
    }

    #[derive(Deserialize)]
    struct VectorSubsystem {
        name: String,
        nqn: String,
        heads: BTreeMap<String, u32>,
        controllers: Vec<VectorController>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Expect {
        error: bool,
        mdts_queried: bool,
        #[serde(rename = "limitKB")]
        limit_kb: u32,
        skipped: bool,
        #[serde(default)]
        waiting: Vec<String>,
        after: BTreeMap<String, i64>,
    }

    #[derive(Deserialize)]
    struct Vector {
        name: String,
        policy: String,
        transport: String,
        mdts: Option<u32>,
        #[serde(default)]
        nqn: Option<String>,
        subsystems: Vec<VectorSubsystem>,
        #[serde(default)]
        unwritable: Vec<String>,
        expect: Expect,
    }

    #[derive(Deserialize)]
    struct Vectors {
        vectors: Vec<Vector>,
    }

    /// The vectors the Go plugin runs too (TestNVMeMaxSectorsVectors).
    #[tokio::test]
    async fn vectors_match_the_go_agent() {
        let vectors: Vectors =
            serde_json::from_str(include_str!("../../../pkg/util/testdata/nvme_max_sectors_vectors.json")).unwrap();
        assert!(!vectors.vectors.is_empty());
        for v in vectors.vectors {
            let mut fake = Fake::new(v.mdts);
            for s in &v.subsystems {
                let dir = fake.subsystem(&s.name, &s.nqn);
                for (name, kb) in &s.heads {
                    fake.disk(&dir.clone(), name, *kb, !v.unwritable.contains(name));
                }
                for c in &s.controllers {
                    let cdir = fake.controller(&dir, &c.name, &c.state, &c.transport);
                    for (name, kb) in &c.paths {
                        fake.disk(&cdir, name, *kb, !v.unwritable.contains(name));
                    }
                }
            }
            let policy = Policy::parse(&v.policy).unwrap();
            let nqn = v.nqn.as_deref().unwrap_or("nqn.test:vol");
            let result = clamp(&fake.nvme, nqn, &options(policy, &v.transport, Duration::ZERO)).await;
            match (&result, v.expect.error) {
                (Ok(report), false) => {
                    assert_eq!(report.limit.unwrap_or(0), v.expect.limit_kb, "{}: {report}", v.name);
                    assert_eq!(report.skipped.is_some(), v.expect.skipped, "{}: {report}", v.name);
                    assert_eq!(report.waiting, v.expect.waiting, "{}", v.name);
                }
                (Err(_), true) => {}
                (Ok(report), true) => panic!("{}: expected an error, got {report}", v.name),
                (Err(e), false) => panic!("{}: {e}", v.name),
            }
            assert_eq!(fake.mdts_asked() > 0, v.expect.mdts_queried, "{}: MDTS queried", v.name);
            assert!(fake.mdts_asked() <= 1, "{}: one controller answers", v.name);
            let after: BTreeMap<String, i64> = fake.queues.keys().map(|n| (n.clone(), fake.value(n))).collect();
            assert_eq!(after, v.expect.after, "{}", v.name);
        }
    }

    #[tokio::test]
    async fn waits_for_paths_that_appear_late() {
        let mut fake = Fake::new(Some(0));
        let dir = fake.subsystem("nvme-subsys3", "nqn.late");
        fake.disk(&dir, "nvme3n1", 32768, true);
        let first = fake.controller(&dir, "nvme3", "live", "tcp");
        fake.disk(&first, "nvme3c3n1", 32768, true);
        let late: Vec<PathBuf> = ["nvme4", "nvme5", "nvme6"]
            .iter()
            .map(|c| fake.controller(&dir, c, "live", "tcp"))
            .collect();
        let creator = {
            let late = late.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                for (i, cdir) in late.iter().enumerate() {
                    let queue = cdir.join(format!("nvme3c{}n1", 4 + i)).join("queue");
                    std::fs::create_dir_all(&queue).unwrap();
                    std::fs::write(queue.join("max_sectors_kb"), "32768\n").unwrap();
                }
            })
        };
        let report = clamp(
            &fake.nvme,
            "nqn.late",
            &options(Policy::Auto, "tcp", Duration::from_secs(5)),
        )
        .await
        .unwrap();
        creator.await.unwrap();
        assert_eq!(report.devices.len(), 5, "{report}");
        assert!(report.devices.iter().all(|d| d.after == Some(4096)), "{report}");
        for (i, cdir) in late.iter().enumerate() {
            assert_eq!(
                read_kb(&cdir.join(format!("nvme3c{}n1", 4 + i)).join("queue")),
                Some(4096)
            );
        }
    }

    #[tokio::test]
    async fn a_late_path_that_never_appears_fails_without_writing() {
        let mut fake = Fake::new(Some(0));
        let dir = fake.subsystem("nvme-subsys3", "nqn.late");
        fake.disk(&dir, "nvme3n1", 32768, true);
        let first = fake.controller(&dir, "nvme3", "live", "tcp");
        fake.disk(&first, "nvme3c3n1", 32768, true);
        fake.controller(&dir, "nvme4", "live", "tcp");
        let start = Instant::now();
        let err = clamp(
            &fake.nvme,
            "nqn.late",
            &options(Policy::Auto, "tcp", Duration::from_millis(200)),
        )
        .await
        .unwrap_err();
        assert!(err.reason.contains("nvme4"), "{err}");
        assert!(start.elapsed() >= Duration::from_millis(200), "waited the budget");
        assert_eq!(fake.value("nvme3n1"), 32768);
    }

    #[tokio::test]
    async fn a_spent_deadline_stops_the_wait() {
        let fake = Fake::new(Some(0));
        let mut opts = options(Policy::Auto, "tcp", Duration::from_secs(60));
        opts.deadline = Some(Instant::now());
        let err = clamp(&fake.nvme, "nqn.absent", &opts).await.unwrap_err();
        assert!(err.reason.contains("canceled"), "{err}");
    }

    #[tokio::test]
    async fn idempotent_and_free_once_converged() {
        let mut fake = Fake::new(Some(0));
        let dir = fake.subsystem("nvme-subsys0", "nqn.again");
        fake.disk(&dir, "nvme0n1", 32768, true);
        let c = fake.controller(&dir, "nvme0", "live", "tcp");
        fake.disk(&c, "nvme0c0n1", 32768, true);
        let opts = options(Policy::Auto, "tcp", Duration::ZERO);
        let first = clamp(&fake.nvme, "nqn.again", &opts).await.unwrap();
        assert_eq!(first.changed(), 2);
        let second = clamp(&fake.nvme, "nqn.again", &opts).await.unwrap();
        assert_eq!(second.changed(), 0);
        assert_eq!(fake.mdts_asked(), 1, "a converged subsystem costs no id-ctrl");
        assert_eq!(
            fake.runner.calls.lock().unwrap()[0],
            format!("nvme id-ctrl {} -o json", fake.nvme.dev.join("nvme0").display())
        );
    }

    #[tokio::test]
    async fn the_log_line_matches_the_go_agent() {
        let mut fake = Fake::new(Some(0));
        let dir = fake.subsystem("nvme-subsys0", "nqn.log");
        fake.disk(&dir, "nvme0n1", 32768, true);
        let c = fake.controller(&dir, "nvme0", "live", "tcp");
        fake.disk(&c, "nvme0c0n1", 32768, true);
        let report = clamp(&fake.nvme, "nqn.log", &options(Policy::Auto, "tcp", Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(
            report.to_string(),
            "policy=auto transport=tcp mdts=0 limit=4096KiB changed=2 devices=[nvme0c0n1 32768->4096, nvme0n1 32768->4096]"
        );
    }

    #[test]
    fn policies() {
        for (raw, want) in [
            ("", Policy::Auto),
            ("auto", Policy::Auto),
            (" Auto ", Policy::Auto),
            ("off", Policy::Off),
            ("OFF", Policy::Off),
            ("0", Policy::Off),
            ("false", Policy::Off),
            ("4", Policy::Fixed(4)),
            ("4096", Policy::Fixed(4096)),
        ] {
            assert_eq!(Policy::parse(raw), Ok(want), "{raw:?}");
        }
        for bad in ["-1", "3", "4k", "4096KiB", "on", "1.5", "true"] {
            assert!(Policy::parse(bad).is_err(), "{bad:?}");
        }
        assert_eq!(Policy::from_config(None), Ok(Policy::Auto));
        assert_eq!(Policy::from_config(Some(&Value::Null)), Ok(Policy::Auto));
        assert_eq!(
            Policy::from_config(Some(&serde_json::json!(4096))),
            Ok(Policy::Fixed(4096))
        );
        assert_eq!(Policy::from_config(Some(&serde_json::json!(0))), Ok(Policy::Off));
        assert_eq!(Policy::from_config(Some(&serde_json::json!("off"))), Ok(Policy::Off));
        assert!(Policy::from_config(Some(&serde_json::json!(1.5))).is_err());
        assert!(Policy::from_config(Some(&serde_json::json!(true))).is_err());
        assert_eq!(
            Policy::from_config(Some(&serde_json::json!(false))),
            Ok(Policy::Off),
            "a bare YAML off"
        );
    }

    #[test]
    fn mdts_from_id_ctrl() {
        assert_eq!(parse_mdts(br#"{"vid":0,"mdts":0,"subnqn":"x"}"#), Ok(0));
        assert_eq!(
            parse_mdts(br#"{"mdts":5,"tnvmcap":340282366920938463463374607431768211455}"#),
            Ok(5),
            "a 128-bit capacity elsewhere is skipped"
        );
        for bad in [&b"{}"[..], br#"{"mdts":-1}"#, b"not json", br#"{"mdts":"0"}"#] {
            assert!(parse_mdts(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn device_names() {
        assert!(is_head("nvme3n1") && !is_head("nvme3c4n1") && !is_head("nvme3") && !is_head("ng3n1"));
        assert!(is_namespace("nvme3c4n1") && is_namespace("nvme4n1"));
        assert!(!is_namespace("nvme3c4") && !is_namespace("ng3n1") && !is_namespace("nvme3cXn1"));
        assert!(is_controller("nvme12") && !is_controller("nvme-subsys1") && !is_controller("nvme1n1"));
    }
}
