//! Formatting, mounting and unmounting, with the Go node's safety rules
//! (`pkg/util/mount.go`):
//!
//! - only ext4, ext3, xfs and btrfs, refused before any command runs;
//! - a device is blank only when `blkid -p` (low-level probe, no cache that could
//!   describe a recycled device name) exits 2 with no output at all; exit 8
//!   (ambivalent signatures) and a partition table without a filesystem are
//!   refused, never formatted; a different filesystem is never reformatted;
//! - unmount retries busy targets with backoff, and only NFS may fall back to a
//!   lazy unmount: a device-backed or unclassifiable mount surfaces its error so
//!   the transport is not disconnected under a live mount.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use log::{debug, info, warn};

use crate::exec::{self, Limits, Output};

/// Runs host commands; the tests replace it.
#[tonic::async_trait]
pub trait Runner: Send + Sync {
    async fn run(&self, program: &str, args: &[&str], limits: Limits) -> std::io::Result<Output>;
}

pub struct HostRunner;

#[tonic::async_trait]
impl Runner for HostRunner {
    async fn run(&self, program: &str, args: &[&str], limits: Limits) -> std::io::Result<Output> {
        exec::run(program, args, limits, false).await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub mount: Duration,
    pub format: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            mount: Duration::from_secs(30),
            format: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountInfo {
    pub source: String,
    pub target: String,
    pub fs_type: String,
    pub options: Vec<String>,
    pub read_only: bool,
}

pub struct Mounter {
    pub runner: Arc<dyn Runner>,
    pub timeouts: Timeouts,
    /// `/proc/self/mounts`.
    pub proc_mounts: PathBuf,
}

const BLOCK_FILESYSTEMS: [&str; 4] = ["ext4", "ext3", "xfs", "btrfs"];
const UNMOUNT_RETRYABLE: [&str; 3] = [
    "device or resource busy",
    "target is busy",
    "resource temporarily unavailable",
];
const UNMOUNT_ATTEMPTS: u32 = 7;

pub fn validate_block_fs(fs_type: &str) -> Result<()> {
    if BLOCK_FILESYSTEMS.contains(&fs_type) {
        return Ok(());
    }
    bail!("invalid argument: unsupported filesystem type {fs_type:?}; supported types are ext4, ext3, xfs, btrfs")
}

fn failure(what: &str, out: &Output) -> anyhow::Error {
    let status = match (out.code, out.timed_out) {
        (_, true) => "timed out".to_string(),
        (Some(code), _) => format!("exit status {code}"),
        (None, _) => "killed by a signal".to_string(),
    };
    anyhow!("{what}: {status}, output: {}", out.combined())
}

impl Mounter {
    pub fn host(timeouts: Timeouts) -> Self {
        Mounter {
            runner: Arc::new(HostRunner),
            timeouts,
            proc_mounts: PathBuf::from("/proc/self/mounts"),
        }
    }

    fn limits(&self, timeout: Duration, deadline: Option<Instant>) -> Limits {
        Limits {
            timeout,
            rpc_deadline: deadline,
        }
    }

    pub async fn is_mounted(&self, path: &str, deadline: Option<Instant>) -> Result<bool> {
        let out = self
            .runner
            .run(
                "findmnt",
                &["--mountpoint", path, "--noheadings"],
                self.limits(self.timeouts.mount, deadline),
            )
            .await?;
        if out.wedged {
            return Err(failure(&format!("findmnt {path}"), &out));
        }
        match out.code {
            Some(0) => Ok(!String::from_utf8_lossy(&out.stdout).trim().is_empty()),
            Some(1) => Ok(false),
            _ => Err(failure(&format!("findmnt {path}"), &out)),
        }
    }

    pub async fn mount_info(&self, target: &str, deadline: Option<Instant>) -> Result<MountInfo> {
        let args = [
            "--first-only",
            "--noheadings",
            "--output",
            "SOURCE,FSTYPE,OPTIONS",
            "--mountpoint",
            target,
        ];
        let out = self
            .runner
            .run("findmnt", &args, self.limits(self.timeouts.mount, deadline))
            .await?;
        if !out.success() {
            return Err(failure(&format!("failed to inspect mountpoint {target}"), &out));
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let fields: Vec<&str> = text.split_whitespace().collect();
        if fields.len() < 3 {
            bail!("unexpected findmnt output for {target}: {:?}", text.trim());
        }
        let options: Vec<String> = fields[2].split(',').map(str::to_string).collect();
        Ok(MountInfo {
            source: unescape_proc_field(fields[0]),
            target: target.to_string(),
            fs_type: fields[1].to_string(),
            read_only: options.iter().any(|o| o == "ro"),
            options,
        })
    }

    pub async fn mount(
        &self,
        source: &str,
        target: &str,
        fs_type: &str,
        options: &[String],
        deadline: Option<Instant>,
    ) -> Result<()> {
        if !fs_type.is_empty() && fs_type != "nfs" {
            validate_block_fs(fs_type)?;
        }
        let joined = options.join(",");
        let mut args: Vec<&str> = Vec::new();
        if !fs_type.is_empty() {
            args.extend(["-t", fs_type]);
        }
        if !options.is_empty() {
            args.extend(["-o", &joined]);
        }
        args.extend([source, target]);
        debug!("mount {args:?}");
        let out = self
            .runner
            .run("mount", &args, self.limits(self.timeouts.mount, deadline))
            .await?;
        if !out.success() {
            return Err(failure("mount failed", &out));
        }
        Ok(())
    }

    /// A bind mount; `ro` needs its own remount (the flag is ignored on the
    /// first bind), and a failed remount takes the fresh bind down again.
    pub async fn bind_mount(
        &self,
        source: &str,
        target: &str,
        options: &[String],
        deadline: Option<Instant>,
    ) -> Result<()> {
        let mut all = vec!["bind".to_string()];
        all.extend(options.iter().cloned());
        let joined = all.join(",");
        let limits = self.limits(self.timeouts.mount, deadline);
        let out = self
            .runner
            .run("mount", &["-o", &joined, source, target], limits)
            .await?;
        if !out.success() {
            return Err(failure("bind mount failed", &out));
        }
        if options.iter().any(|o| o == "ro") {
            let out = self
                .runner
                .run("mount", &["-o", "remount,bind,ro", target], limits)
                .await?;
            if !out.success() {
                let remount = failure("read-only bind remount failed", &out);
                if let Err(e) = self.unmount(target, deadline).await {
                    return Err(remount.context(format!("failed to clean up bind mount: {e:#}")));
                }
                return Err(remount);
            }
        }
        Ok(())
    }

    pub async fn unmount(&self, target: &str, deadline: Option<Instant>) -> Result<()> {
        if !self.is_mounted(target, deadline).await? {
            return Ok(());
        }
        let fs_type = match self.mount_fs_type(target, deadline).await {
            Ok(t) => t,
            Err(e) => {
                // Unknown is not NFS: a failed unmount is surfaced, never lazy.
                warn!("could not determine the filesystem type of {target}: {e:#}");
                String::new()
            }
        };
        let mut delay = Duration::from_millis(100);
        let mut last = None;
        for attempt in 1..=UNMOUNT_ATTEMPTS {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                bail!("unmount {target}: deadline passed");
            }
            let limits = Limits {
                timeout: self.timeouts.mount,
                rpc_deadline: None,
            };
            let out = self.runner.run("umount", &[target], limits).await?;
            if out.success() {
                return Ok(());
            }
            let error = failure("unmount failed", &out);
            let text = format!("{error:#}").to_lowercase();
            let retryable = !out.wedged && UNMOUNT_RETRYABLE.iter().any(|r| text.contains(r));
            if !retryable {
                if fs_type != "nfs" && fs_type != "nfs4" {
                    return Err(error);
                }
                warn!(
                    "regular unmount of network filesystem {target} ({fs_type}) failed, trying lazy unmount: {error:#}"
                );
                let lazy = self.runner.run("umount", &["-l", target], limits).await?;
                if !lazy.success() {
                    return Err(error.context(format!("{:#}", failure("lazy unmount failed", &lazy))));
                }
                return Ok(());
            }
            last = Some(error);
            if attempt < UNMOUNT_ATTEMPTS {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
        Err(last
            .expect("at least one attempt")
            .context(format!("unmount {target}: failed after {UNMOUNT_ATTEMPTS} attempts")))
    }

    /// The filesystem type of a mountpoint: /proc/self/mounts first, findmnt as
    /// the fallback. An NFS-looking mount that cannot be classified is an error.
    async fn mount_fs_type(&self, target: &str, deadline: Option<Instant>) -> Result<String> {
        let proc = std::fs::read_to_string(&self.proc_mounts)
            .map_err(|e| anyhow!("failed to open {}: {e}", self.proc_mounts.display()))
            .and_then(|text| parse_proc_mounts(&text, target));
        let entry = match proc {
            Ok(entry) => return Ok(entry.fs_type),
            Err(e) => e,
        };
        let out = self
            .runner
            .run(
                "findmnt",
                &["-n", "-o", "FSTYPE", "--mountpoint", target],
                self.limits(self.timeouts.mount, deadline),
            )
            .await?;
        let fs_type = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !out.success() || fs_type.is_empty() {
            bail!("{entry:#}; findmnt could not classify {target} either");
        }
        Ok(fs_type)
    }

    /// The whole-device filesystem type, "" for a blank device.
    pub async fn filesystem_type(&self, device: &str, deadline: Option<Instant>) -> Result<String> {
        let args = ["-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", device];
        let out = self
            .runner
            .run("blkid", &args, self.limits(self.timeouts.mount, deadline))
            .await?;
        if out.wedged {
            return Err(failure(&format!("blkid failed for {device}"), &out));
        }
        match out.code {
            Some(0) => {}
            Some(8) => bail!(
                "device {device} has ambivalent filesystem signatures (blkid -p exit 8); refusing to guess which is real — clear the stale signature with wipefs before staging"
            ),
            Some(2)
                if String::from_utf8_lossy(&out.stdout).trim().is_empty()
                    && String::from_utf8_lossy(&out.stderr).trim().is_empty() =>
            {
                return Ok(String::new());
            }
            _ => return Err(failure(&format!("blkid failed for {device}"), &out)),
        }
        let (fs, pt) = parse_blkid_export(&String::from_utf8_lossy(&out.stdout));
        if fs.is_empty() && !pt.is_empty() {
            bail!(
                "device {device} has a partition table (PTTYPE={pt}) but no whole-device filesystem signature; refusing to treat it as unformatted"
            );
        }
        Ok(fs)
    }

    pub async fn format(&self, device: &str, fs_type: &str, deadline: Option<Instant>) -> Result<()> {
        validate_block_fs(fs_type)?;
        info!("formatting device {device} with {fs_type}");
        let (program, force) = match fs_type {
            "ext4" => ("mkfs.ext4", "-F"),
            "ext3" => ("mkfs.ext3", "-F"),
            "xfs" => ("mkfs.xfs", "-f"),
            _ => ("mkfs.btrfs", "-f"),
        };
        let out = self
            .runner
            .run(program, &[force, device], self.limits(self.timeouts.format, deadline))
            .await?;
        if !out.success() {
            return Err(failure("format failed", &out));
        }
        Ok(())
    }

    pub async fn format_and_mount(
        &self,
        device: &str,
        target: &str,
        fs_type: &str,
        options: &[String],
        deadline: Option<Instant>,
    ) -> Result<()> {
        validate_block_fs(fs_type)?;
        let existing = self.filesystem_type(device, deadline).await?;
        if existing.is_empty() {
            self.format(device, fs_type, deadline).await?;
        } else if existing != fs_type {
            bail!("device {device} has filesystem {existing}, requested {fs_type}");
        }
        self.mount(device, target, fs_type, options, deadline).await
    }
}

/// `TYPE` and `PTTYPE` from `blkid -o export`.
pub fn parse_blkid_export(output: &str) -> (String, String) {
    let (mut fs, mut pt) = (String::new(), String::new());
    for line in output.split('\n') {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches('"').to_string();
        match key.trim() {
            "TYPE" => fs = value,
            "PTTYPE" => pt = value,
            _ => {}
        }
    }
    (fs, pt)
}

/// The mount table's octal escapes (`\040` for a space); anything that is not
/// exactly three octal digits after a backslash stays as it is.
pub fn unescape_proc_field(value: &str) -> String {
    let b = value.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 3 < b.len() {
            let digits = &b[i + 1..i + 4];
            if digits.iter().all(|d| (b'0'..=b'7').contains(d)) {
                let v =
                    u32::from(digits[0] - b'0') * 64 + u32::from(digits[1] - b'0') * 8 + u32::from(digits[2] - b'0');
                if let Ok(byte) = u8::try_from(v) {
                    out.push(byte);
                    i += 4;
                    continue;
                }
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcMountEntry {
    pub source: String,
    pub target: String,
    pub fs_type: String,
}

/// The first `/proc/self/mounts` entry for `target`.
pub fn parse_proc_mounts(text: &str, target: &str) -> Result<ProcMountEntry> {
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 2 {
            continue;
        }
        let source = unescape_proc_field(fields[0]);
        if unescape_proc_field(fields[1]) != target {
            continue;
        }
        let Some(fs) = fields.get(2) else {
            bail!("mount entry for {target} has no filesystem type");
        };
        let fs_type = unescape_proc_field(fs);
        if fs_type.is_empty() {
            bail!("mount entry for {target} has an empty filesystem type");
        }
        return Ok(ProcMountEntry {
            source,
            target: target.to_string(),
            fs_type,
        });
    }
    bail!("mountpoint {target} not found in proc mounts")
}

pub fn is_nfs_mount_source(source: &str) -> bool {
    source.rfind(":/").is_some_and(|i| i > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// (exit code, stdout, stderr, wedged)
    type Reply = (Option<i32>, &'static str, &'static str, bool);

    /// Answers each command with the next scripted reply and records the argv.
    struct Script {
        replies: Mutex<Vec<Reply>>,
        calls: Mutex<Vec<String>>,
    }

    impl Script {
        fn new(replies: Vec<Reply>) -> Arc<Self> {
            Arc::new(Script {
                replies: Mutex::new(replies.into_iter().rev().collect()),
                calls: Mutex::new(Vec::new()),
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
            let (code, stdout, stderr, wedged) = self.replies.lock().unwrap().pop().expect("an unscripted command ran");
            Ok(Output {
                code,
                stdout: stdout.into(),
                stderr: stderr.into(),
                wedged,
                timed_out: false,
            })
        }
    }

    fn mounter(script: &Arc<Script>, proc_mounts: &str) -> (Mounter, tempfile::NamedTempFile) {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), proc_mounts).unwrap();
        let runner: Arc<dyn Runner> = script.clone();
        (
            Mounter {
                runner,
                timeouts: Timeouts::default(),
                proc_mounts: file.path().to_path_buf(),
            },
            file,
        )
    }

    #[tokio::test]
    async fn blank_means_exit_2_with_no_output_only() {
        let s = Script::new(vec![(Some(2), "", "", false)]);
        let (m, _f) = mounter(&s, "");
        assert_eq!(m.filesystem_type("/dev/x", None).await.unwrap(), "");
        assert_eq!(s.calls(), ["blkid -p -s TYPE -s PTTYPE -o export /dev/x"]);
        for reply in [
            (Some(2), "", "some warning", false),
            (Some(8), "", "", false),
            (Some(0), "PTTYPE=\"gpt\"\n", "", false),
            (Some(4), "", "", false),
            (Some(2), "", "", true),
        ] {
            let s = Script::new(vec![reply]);
            let (m, _f) = mounter(&s, "");
            assert!(
                m.filesystem_type("/dev/x", None).await.is_err(),
                "{reply:?} was read as blank"
            );
        }
        let s = Script::new(vec![(Some(0), "DEVNAME=/dev/x\nTYPE=\"xfs\"\n", "", false)]);
        let (m, _f) = mounter(&s, "");
        assert_eq!(m.filesystem_type("/dev/x", None).await.unwrap(), "xfs");
    }

    #[tokio::test]
    async fn never_reformats_and_refuses_unsupported_types_before_running_anything() {
        let s = Script::new(vec![(Some(0), "TYPE=ext4\n", "", false)]);
        let (m, _f) = mounter(&s, "");
        let e = m
            .format_and_mount("/dev/x", "/stage", "xfs", &[], None)
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("has filesystem ext4, requested xfs"));
        assert_eq!(s.calls().len(), 1, "no mkfs, no mount");
        let s = Script::new(vec![]);
        let (m, _f) = mounter(&s, "");
        assert!(m.format_and_mount("/dev/x", "/stage", "ntfs", &[], None).await.is_err());
        assert!(s.calls().is_empty());
    }

    #[tokio::test]
    async fn formats_a_blank_device_then_mounts() {
        let s = Script::new(vec![
            (Some(2), "", "", false),
            (Some(0), "", "", false),
            (Some(0), "", "", false),
        ]);
        let (m, _f) = mounter(&s, "");
        m.format_and_mount("/dev/x", "/stage", "xfs", &["nouuid".into()], None)
            .await
            .unwrap();
        assert_eq!(
            s.calls()[1..],
            [
                "mkfs.xfs -f /dev/x".to_string(),
                "mount -t xfs -o nouuid /dev/x /stage".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn a_device_unmount_failure_is_surfaced_never_lazy() {
        let proc = "/dev/ublkb0 /stage ext4 rw 0 0\n";
        let s = Script::new(vec![
            (Some(0), "/stage /dev/ublkb0 ext4 rw", "", false),
            (Some(32), "", "umount: /stage: some I/O error", false),
        ]);
        let (m, _f) = mounter(&s, proc);
        assert!(m.unmount("/stage", None).await.is_err());
        assert!(!s.calls().iter().any(|c| c.contains("-l")), "{:?}", s.calls());
    }

    #[tokio::test]
    async fn nfs_falls_back_to_lazy_and_busy_is_retried() {
        let proc = "192.0.2.1:/mnt/s /stage nfs4 rw 0 0\n";
        let s = Script::new(vec![
            (Some(0), "/stage", "", false),
            (Some(32), "", "umount.nfs4: /stage: Stale file handle", false),
            (Some(0), "", "", false),
        ]);
        let (m, _f) = mounter(&s, proc);
        m.unmount("/stage", None).await.unwrap();
        assert_eq!(s.calls().last().unwrap(), "umount -l /stage");

        let s = Script::new(vec![
            (Some(0), "/stage", "", false),
            (Some(32), "", "umount: /stage: target is busy.", false),
            (Some(0), "", "", false),
        ]);
        let (m, _f) = mounter(&s, "/dev/sda /stage ext4 rw 0 0\n");
        m.unmount("/stage", None).await.unwrap();
        assert_eq!(s.calls().iter().filter(|c| c.starts_with("umount /stage")).count(), 2);
    }

    #[tokio::test]
    async fn not_mounted_is_success_and_unclassifiable_nfs_is_not_lazy() {
        let s = Script::new(vec![(Some(1), "", "", false)]);
        let (m, _f) = mounter(&s, "");
        m.unmount("/stage", None).await.unwrap();
        // Not in proc mounts, findmnt fails: unknown, so the failure surfaces.
        let s = Script::new(vec![
            (Some(0), "/stage", "", false),
            (Some(1), "", "", false),
            (Some(32), "", "stale", false),
        ]);
        let (m, _f) = mounter(&s, "");
        assert!(m.unmount("/stage", None).await.is_err());
        assert!(!s.calls().iter().any(|c| c.contains("-l")));
    }

    #[tokio::test]
    async fn read_only_bind_is_remounted_and_undone_on_failure() {
        let s = Script::new(vec![(Some(0), "", "", false), (Some(0), "", "", false)]);
        let (m, _f) = mounter(&s, "");
        m.bind_mount("/stage", "/target", &["ro".into()], None).await.unwrap();
        assert_eq!(
            s.calls(),
            ["mount -o bind,ro /stage /target", "mount -o remount,bind,ro /target"]
        );
        let s = Script::new(vec![
            (Some(0), "", "", false),
            (Some(32), "", "remount failed", false),
            (Some(0), "/target", "", false),
            (Some(0), "", "", false),
        ]);
        let (m, _f) = mounter(&s, "/stage /target ext4 rw 0 0\n");
        assert!(m.bind_mount("/stage", "/target", &["ro".into()], None).await.is_err());
        assert_eq!(s.calls().last().unwrap(), "umount /target");
    }

    #[test]
    fn parsers() {
        assert_eq!(
            parse_blkid_export("TYPE=\"xfs\"\nPTTYPE=\"dos\"\nX"),
            ("xfs".into(), "dos".into())
        );
        assert_eq!(unescape_proc_field(r"/mnt/a\040b"), "/mnt/a b");
        assert_eq!(
            unescape_proc_field(r"/mnt/a\400b"),
            r"/mnt/a\400b",
            "over a byte stays literal"
        );
        assert_eq!(unescape_proc_field(r"/mnt/a\+12"), r"/mnt/a\+12");
        assert_eq!(unescape_proc_field(r"a\04"), r"a\04", "too short stays literal");
        assert_eq!(unescape_proc_field(r"\134"), "\\");
        let proc = "a /x ext4 rw 0 0\nb /y nfs4 rw 0 0\nc /y ext4 rw 0 0\nd\n";
        assert_eq!(
            parse_proc_mounts(proc, "/y").unwrap().fs_type,
            "nfs4",
            "the first entry wins"
        );
        assert!(parse_proc_mounts("e /z\n", "/z").is_err());
        assert!(parse_proc_mounts(proc, "/nope").is_err());
        assert!(is_nfs_mount_source("192.0.2.1:/mnt/s") && is_nfs_mount_source("[2001:db8::1]:/s"));
        assert!(!is_nfs_mount_source("/dev/sda") && !is_nfs_mount_source(":/x"));
    }
}
