//! The iSCSI initiator, through iscsiadm and sysfs, as the Go node drives it
//! (`pkg/util/iscsi.go`):
//!
//! - iscsiadm runs through the same PATH wrapper the Go node uses (the image's
//!   `/usr/local/bin/iscsiadm`, which nsenters the host), with `LC_ALL=C`;
//! - exit codes classify first (15: the session exists, 21: nothing matched),
//!   message text second, and never the text of a wedged command;
//! - a login first ensures a static node record for exactly one target
//!   (`-o new`) and falls back to a SendTargets discovery (cached and serialized
//!   per portal) only when the target or portal is not found; a CHAP rejection
//!   is terminal and never retried;
//! - logins are limited per portal (`resilience.rateLimiting`);
//! - an existing session is reused only if its device shows up within 2 s, else
//!   it is logged out and logged in again;
//! - CHAP credentials: the method and the user names go to iscsiadm on argv, the
//!   two passwords are written straight into the node record files (0600,
//!   atomically), never onto any argv: a hostPID node plugin's argv is readable
//!   by every process on the host;
//! - a device is found through sysfs, by the exact session of the portal when
//!   the stage is multipath (never another portal's path), else by target name;
//! - a dm-multipath map is identified by its dm UUID (`mpath-<wwid>`).

use std::collections::HashMap;
use std::io::Write;
use std::net::IpAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use log::{debug, info, warn};

use crate::exec::{Limits, Output};
use crate::mount::Runner;
use crate::nvme_addresses::join_host_port;

/// `ISCSI_ERR_SESS_EXISTS`: a `--login` whose session already exists.
pub const EXIT_SESSION_EXISTS: i32 = 15;
/// `ISCSI_ERR_NO_OBJS_FOUND`: no record, session or portal matched.
pub const EXIT_NO_OBJECTS: i32 = 21;
pub const DEFAULT_PORT: &str = "3260";
/// An existing session is reused when its device shows up within this.
pub const STALE_SESSION_VALIDATION: Duration = Duration::from_secs(2);
const DISCOVERY_RETRIES: u32 = 5;
const NODE_RECORD_MODE: u32 = 0o600;

/// One `iscsiadm -m session` line.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Session {
    /// `host:port` as iscsiadm prints it (IPv6 bracketed), without the tag.
    pub portal: String,
    pub iqn: String,
    /// The number of `/sys/class/iscsi_session/session<N>`.
    pub id: String,
}

/// Session CHAP credentials for a node record. Never logged: Debug redacts.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    pub password: String,
    pub mutual_username: String,
    pub mutual_password: String,
    pub mutual: bool,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("mutual", &self.mutual)
            .finish_non_exhaustive()
    }
}

/// Why a connect failed. `Auth` and `ChapConfig` carry no credential and no
/// iscsiadm output.
#[derive(Debug)]
pub enum ConnectError {
    /// The target rejected the CHAP credentials (Go ErrISCSIAuthFailure).
    Auth(String),
    /// The credentials could not be applied to the node record (Go
    /// ErrISCSICHAPConfig): names the parameter, never its value.
    ChapConfig(String),
    Other(String),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Auth(iqn) => write!(f, "iSCSI CHAP authentication failed for {iqn}"),
            ConnectError::ChapConfig(message) | ConnectError::Other(message) => f.write_str(message),
        }
    }
}

/// What a connect is asked for.
#[derive(Debug, Clone, Default)]
pub struct ConnectOptions {
    /// 0 takes 60 s.
    pub device_timeout: Duration,
    /// 0 takes 500 ms.
    pub session_cleanup_delay: Duration,
    pub chap: Option<Credentials>,
    /// The device must belong to this portal's session (multipath only).
    pub portal_scoped: bool,
}

/// A device lookup that found nothing, or why it failed.
#[derive(Debug)]
pub enum InfoError {
    /// The device's sysfs ancestry was read and holds no iSCSI session: it is
    /// positively local (Go ErrNotISCSIDevice).
    NotIscsi(String),
    /// The lookup failed: the identity is unknown.
    Unknown(String),
}

impl std::fmt::Display for InfoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InfoError::NotIscsi(m) => write!(f, "not an iSCSI device: {m}"),
            InfoError::Unknown(m) => f.write_str(m),
        }
    }
}

#[derive(Debug)]
enum PortalLookup {
    /// No session of this portal and target.
    NoSession,
    /// The portal's session has no device for the LUN (yet).
    Failed,
}

#[derive(Default)]
struct PortalState {
    /// Node record creation and discovery, serialized per portal.
    locks: HashMap<String, Arc<tokio::sync::Mutex<()>>>,
    logins: HashMap<String, Arc<tokio::sync::Semaphore>>,
    discovered: HashMap<String, Instant>,
}

pub struct Iscsi {
    pub runner: Arc<dyn Runner>,
    /// `commandTimeouts.iscsi`.
    pub timeout: Duration,
    pub sysfs: PathBuf,
    pub dev: PathBuf,
    /// Where node records live (the DaemonSet mounts both).
    pub node_db_roots: Vec<PathBuf>,
    /// multipathd's control socket, as seen from the plugin.
    pub multipathd_sockets: Vec<PathBuf>,
    /// `resilience.rateLimiting.maxConcurrentLogins`.
    pub max_concurrent_logins: usize,
    /// `resilience.rateLimiting.discoveryCacheDuration`.
    pub discovery_cache: Duration,
    /// The first wait before a fresh discovery (then doubling, capped at 10 s).
    pub discovery_retry_delay: Duration,
    /// How often a device wait re-lists the sessions.
    pub session_refresh: Duration,
    portals: Mutex<PortalState>,
}

impl Iscsi {
    pub fn new(runner: Arc<dyn Runner>, timeout: Duration) -> Self {
        Iscsi {
            runner,
            timeout,
            sysfs: PathBuf::from("/sys"),
            dev: PathBuf::from("/dev"),
            node_db_roots: vec![PathBuf::from("/etc/iscsi"), PathBuf::from("/var/lib/iscsi")],
            multipathd_sockets: vec![
                PathBuf::from("/host/run/multipathd/multipathd.sock"),
                PathBuf::from("/run/multipathd/multipathd.sock"),
            ],
            max_concurrent_logins: 2,
            discovery_cache: Duration::from_secs(30),
            discovery_retry_delay: Duration::from_secs(2),
            session_refresh: Duration::from_secs(1),
            portals: Mutex::default(),
        }
    }
}

/// Go's `exec` error text for a command that did not succeed.
fn exit_text(out: &Output) -> String {
    match out.code {
        Some(code) if out.wedged && code == 0 => "exec: WaitDelay expired before I/O complete".to_string(),
        Some(code) => format!("exit status {code}"),
        None => "signal: killed".to_string(),
    }
}

/// An exec failure WITHOUT its output (Go sanitizedExecClass): a credential
/// that iscsiadm or a wrapper echoed cannot leak through it.
fn sanitized_class(out: &std::io::Result<Output>) -> String {
    match out {
        Ok(out) if out.wedged && out.code == Some(0) => "command execution error".into(),
        Ok(out) => format!("exit status {}", out.code.unwrap_or(-1)),
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => "timed out".into(),
        Err(_) => "command execution error".into(),
    }
}

/// Go's `time.Duration` text for the durations used here.
pub fn go_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms < 1000 {
        return format!("{ms}ms");
    }
    let (h, m) = (ms / 3_600_000, (ms / 60_000) % 60);
    let (s, frac) = ((ms / 1000) % 60, ms % 1000);
    let seconds = if frac == 0 {
        format!("{s}s")
    } else {
        format!("{s}.{}s", format!("{frac:03}").trim_end_matches('0'))
    };
    match (h, m) {
        (0, 0) => seconds,
        (0, m) => format!("{m}m{seconds}"),
        (h, m) => format!("{h}h{m}m{seconds}"),
    }
}

fn is_re_space(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | 0x0c | b'\r' | b' ')
}

/// One line of `iscsiadm -m session`, as Go's
/// `^tcp:\s+\[(\d+)\]\s+([^,]+),\d+\s+(iqn\.\S+)` reads it (on the trimmed line).
pub fn parse_session_line(line: &str) -> Option<Session> {
    let rest = line.trim().strip_prefix("tcp:")?;
    let b = rest.as_bytes();
    let spaces = |from: usize| b[from..].iter().take_while(|c| is_re_space(**c)).count();
    let mut i = spaces(0);
    if i == 0 || b.get(i) != Some(&b'[') {
        return None;
    }
    i += 1;
    let digits = b[i..].iter().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 || b.get(i + digits) != Some(&b']') {
        return None;
    }
    let id = &rest[i..i + digits];
    i += digits + 1;
    let ws = spaces(i);
    if ws == 0 {
        return None;
    }
    let after = &rest[i..];
    let comma = after.find(',')?;
    // `\s+` is greedy and the portal runs to the first comma; when the comma
    // follows the whitespace directly, the regex hands the portal one space.
    let start = ws.min(comma.checked_sub(1)?);
    if start < 1 {
        return None;
    }
    let portal = &after[start..comma];
    let tail = &after[comma + 1..];
    let t = tail.as_bytes();
    let tag = t.iter().take_while(|c| c.is_ascii_digit()).count();
    let gap = t[tag..].iter().take_while(|c| is_re_space(**c)).count();
    if tag == 0 || gap == 0 {
        return None;
    }
    let target = &tail[tag + gap..];
    if !target.starts_with("iqn.") {
        return None;
    }
    let len = target.bytes().take_while(|c| !is_re_space(*c)).count();
    if len <= 4 {
        return None;
    }
    Some(Session {
        portal: portal.to_string(),
        iqn: target[..len].to_string(),
        id: id.to_string(),
    })
}

pub fn parse_sessions(output: &str) -> Vec<Session> {
    output
        .split('\n')
        .filter(|l| !l.trim().is_empty())
        .filter_map(parse_session_line)
        .collect()
}

/// Go's `net.SplitHostPort`.
pub fn split_host_port(hostport: &str) -> Option<(&str, &str)> {
    let i = hostport.rfind(':')?;
    let (host, j, k) = if hostport.starts_with('[') {
        let end = hostport.find(']')?;
        if end + 1 != i {
            return None;
        }
        (&hostport[1..end], 1, end + 1)
    } else {
        let host = &hostport[..i];
        if host.contains(':') {
            return None;
        }
        (host, 0, 0)
    };
    if hostport[j..].contains('[') || hostport[k..].contains(']') {
        return None;
    }
    Some((host, &hostport[i + 1..]))
}

/// Go's `net.ParseIP`.
pub fn parse_ip(s: &str) -> Option<IpAddr> {
    s.parse().ok()
}

/// Go's `net.IP.String`: an IPv4-mapped address prints as IPv4.
pub fn ip_string(ip: IpAddr) -> String {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v6.to_string(),
        },
        v4 => v4.to_string(),
    }
}

/// A portal as compared with what iscsiadm reports (Go
/// canonicalISCSIPortalForComparison): `host:port`, IPs in their canonical
/// form, the default port when there is none. No DNS lookup.
pub fn canonical_portal(portal: &str) -> String {
    let portal = portal.trim();
    if let Some((host, port)) = split_host_port(portal) {
        let host = host.trim();
        let host = parse_ip(host).map_or_else(|| host.to_string(), ip_string);
        return join_host_port(&host.to_lowercase(), port.trim());
    }
    if let Some(ip) = parse_ip(portal.trim_matches(['[', ']'])) {
        return join_host_port(&ip_string(ip), DEFAULT_PORT);
    }
    let unbracketed = portal.replace(['[', ']'], "");
    let unbracketed = unbracketed.trim();
    if !unbracketed.contains(':') {
        return join_host_port(&unbracketed.to_lowercase(), DEFAULT_PORT);
    }
    portal.to_lowercase()
}

pub fn same_portal(left: &str, right: &str) -> bool {
    canonical_portal(left) == canonical_portal(right)
}

/// The host and port naming a node record directory (Go splitISCSIPortal).
pub fn split_portal(portal: &str) -> (String, String) {
    let portal = portal.trim();
    match split_host_port(portal) {
        Some((h, p)) => (h.trim().to_string(), p.trim().to_string()),
        None => (portal.trim_matches(['[', ']']).to_string(), DEFAULT_PORT.to_string()),
    }
}

/// The identifier dm-multipath uses (Go normalizeSCSIWWID): sysfs's `naa.`,
/// `eui.` and `t10.` designators become SCSI's type nibble.
pub fn normalize_scsi_wwid(wwid: &str) -> String {
    let wwid = wwid.trim();
    let lower = wwid.to_lowercase();
    for (prefix, designator) in [("naa.", "3"), ("eui.", "2"), ("t10.", "1")] {
        if lower.starts_with(prefix) && wwid.is_char_boundary(prefix.len()) {
            let normalized = format!("{designator}{}", &wwid[prefix.len()..]);
            return if prefix == "t10." {
                normalized.replace(' ', "_")
            } else {
                normalized
            };
        }
    }
    wwid.to_string()
}

fn base_name(device: &str) -> &str {
    Path::new(device).file_name().and_then(|n| n.to_str()).unwrap_or(device)
}

fn sorted_entries(dir: &Path) -> std::io::Result<Vec<std::fs::DirEntry>> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}

fn sorted_names(dir: &Path, keep: impl Fn(&str) -> bool) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().to_str().map(str::to_string))
                .filter(|n| keep(n))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Device-name classes the driver's iSCSI staging never produces (Go
/// IsPositivelyNotISCSIBackable): a failed identity lookup of one of these is
/// not a reason for session GC to hold off.
pub fn is_positively_not_iscsi_backable(device: &str) -> bool {
    let name = base_name(device);
    ["loop", "ram", "zram", "nbd", "sr", "nvme", "ublkb"]
        .iter()
        .any(|p| name.starts_with(p))
}

/// `name = value` set in a node record's text, every existing assignment
/// replaced, one appended when there is none (Go setISCSINodeRecordParamText).
pub fn set_record_param(text: &str, name: &str, value: &str) -> String {
    let line = format!("{name} = {value}");
    let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
    let mut replaced = false;
    for raw in lines.iter_mut() {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match trimmed.split_once('=') {
            Some((key, _)) if key.trim() == name => {
                *raw = line.clone();
                replaced = true;
            }
            _ => {}
        }
    }
    if !replaced {
        let mut body = text.trim_end_matches('\n').to_string();
        if !body.is_empty() {
            body.push('\n');
        }
        return format!("{body}{line}\n");
    }
    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// One on-disk node record and the database root it is under (the temporary
/// file of a rewrite goes in the root: same filesystem, never read as a record).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    pub root: PathBuf,
    pub path: PathBuf,
}

/// Every node record a `--login -T <iqn> -p <portal>` could read, in both of
/// open-iscsi's layouts (Go iscsiNodeRecordFiles): the flat file
/// `nodes/<iqn>/<host>,<port>` that `-o new` writes without a tag, and the
/// `nodes/<iqn>/<host>,<port>,<tpgt>/<iface>` files a discovery writes. All or
/// nothing: a directory that could hide a record and cannot be read fails the
/// whole lookup, except a root that does not carry the target at all.
pub fn node_record_files(roots: &[PathBuf], portal: &str, iqn: &str) -> Result<Vec<NodeRecord>> {
    if iqn.is_empty() || iqn.contains('/') || iqn.contains("..") {
        bail!("refusing to locate a node record for an IQN that is not a single path component");
    }
    let (host, port) = split_portal(portal);
    let mut hosts = vec![host.clone()];
    if let Some(ip) = parse_ip(&host) {
        hosts.push(ip_string(ip));
    }
    let mut files = Vec::new();
    for root in roots {
        let target_dir = root.join("nodes").join(iqn);
        let portal_dirs = match sorted_entries(&target_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => bail!(
                "failed to read iSCSI node database directory {}: {e}",
                target_dir.display()
            ),
        };
        for entry in portal_dirs {
            let name = entry.file_name().to_string_lossy().into_owned();
            let parts: Vec<&str> = name.split(',').collect();
            if parts.len() < 2 || parts[1] != port || !hosts.iter().any(|h| h == parts[0]) {
                continue;
            }
            let path = target_dir.join(&name);
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if !is_dir {
                files.push(NodeRecord {
                    root: root.clone(),
                    path,
                });
                continue;
            }
            let ifaces = sorted_entries(&path)
                .with_context(|| format!("failed to read iSCSI node record directory {}", path.display()))?;
            for iface in ifaces {
                if iface.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                files.push(NodeRecord {
                    root: root.clone(),
                    path: path.join(iface.file_name()),
                });
            }
        }
    }
    if files.is_empty() {
        let roots: Vec<String> = roots.iter().map(|r| r.display().to_string()).collect();
        bail!(
            "no iSCSI node record found for {iqn} at {portal} under {}",
            roots.join(", ")
        );
    }
    Ok(files)
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One parameter into one record file, replaced atomically at 0600.
fn rewrite_node_record(record: &NodeRecord, name: &str, value: &str) -> Result<()> {
    let existing = std::fs::read_to_string(&record.path)
        .with_context(|| format!("failed to read iSCSI node record for param {name}"))?;
    let updated = set_record_param(&existing, name, value);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let temp = record.root.join(format!(
        ".scale-csi-node-{}-{nanos}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(NODE_RECORD_MODE)
            .open(&temp)
            .with_context(|| format!("failed to stage iSCSI node record for param {name}"))?;
        file.set_permissions(std::fs::Permissions::from_mode(NODE_RECORD_MODE))
            .with_context(|| format!("failed to restrict iSCSI node record permissions for param {name}"))?;
        file.write_all(updated.as_bytes())
            .with_context(|| format!("failed to write iSCSI node record for param {name}"))?;
        drop(file);
        std::fs::rename(&temp, &record.path)
            .with_context(|| format!("failed to install iSCSI node record for param {name}"))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// A credential written into every node record of the target and portal, not
/// onto iscsiadm's argv (Go writeISCSINodeRecordSecret). A write that fails
/// after another succeeded says so; nothing is rolled back (a rollback would
/// reinstate a stale, world-readable record), the stage fails and is retried.
pub fn write_node_record_secret(roots: &[PathBuf], portal: &str, iqn: &str, name: &str, value: &str) -> Result<()> {
    if value != value.trim() {
        bail!("node param {name} value must not have leading or trailing whitespace");
    }
    if value.contains(['\r', '\n']) {
        bail!("node param {name} value must not contain a newline");
    }
    if value.contains('#') {
        bail!("node param {name} value must not contain '#'");
    }
    let files = node_record_files(roots, portal, iqn)?;
    for (i, record) in files.iter().enumerate() {
        if let Err(e) = rewrite_node_record(record, name, value) {
            if i == 0 {
                return Err(e);
            }
            bail!(
                "node param {name} was applied to {i} of {} node records before {} failed: iSCSI node database was left partially updated; the stage must fail and be retried: {e:#}",
                files.len(),
                record.path.display()
            );
        }
    }
    Ok(())
}

fn is_target_not_found(message: &str) -> bool {
    let m = message.to_lowercase();
    [
        "no records found",
        "could not find records for",
        "no record found",
        "does not exist",
        "no portals found",
    ]
    .iter()
    .any(|s| m.contains(s))
}

/// A login rejected for its credentials (Go isAuthFailure).
pub fn is_auth_failure(message: &str) -> bool {
    let m = message.to_lowercase();
    ["authorization failure", "authentication failed", "login failed"]
        .iter()
        .any(|s| m.contains(s))
}

const LOGOUT_RETRYABLE: [&str; 4] = [
    "device or resource busy",
    "session is busy",
    "connection timed out",
    "transport endpoint is not connected",
];

impl Iscsi {
    fn limits(&self, deadline: Option<Instant>) -> Limits {
        Limits {
            timeout: self.timeout,
            rpc_deadline: deadline,
        }
    }

    async fn iscsiadm(&self, args: &[&str], deadline: Option<Instant>) -> std::io::Result<Output> {
        self.runner.run("iscsiadm", args, self.limits(deadline)).await
    }

    fn portal_lock(&self, portal: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.portals
            .lock()
            .expect("portal state")
            .locks
            .entry(portal.to_string())
            .or_default()
            .clone()
    }

    fn login_slots(&self, portal: &str) -> Arc<tokio::sync::Semaphore> {
        let max = self.max_concurrent_logins.max(1);
        self.portals
            .lock()
            .expect("portal state")
            .logins
            .entry(portal.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(max)))
            .clone()
    }

    /// `iscsiadm -m session`; exit 21 is no sessions.
    pub async fn list_sessions(&self, deadline: Option<Instant>) -> Result<Vec<Session>> {
        let out = self.iscsiadm(&["-m", "session"], deadline).await?;
        if out.code == Some(EXIT_NO_OBJECTS) {
            return Ok(Vec::new());
        }
        if !out.success() {
            bail!(
                "failed to list iSCSI sessions: {}, stderr: {}",
                exit_text(&out),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(parse_sessions(&String::from_utf8_lossy(&out.stdout)))
    }

    /// A static node record for exactly this target (`-o new`); one that
    /// exists already is fine.
    pub async fn ensure_node_record(&self, portal: &str, iqn: &str, deadline: Option<Instant>) -> Result<()> {
        let lock = self.portal_lock(portal);
        let _held = lock.lock().await;
        let out = self
            .iscsiadm(&["-m", "node", "-o", "new", "-T", iqn, "-p", portal], deadline)
            .await?;
        if out.success() {
            debug!("Static iSCSI node record created for {iqn} at {portal}");
            return Ok(());
        }
        let detail = format!("{} {}", out.combined(), exit_text(&out)).to_lowercase();
        if detail.contains("already exists") || detail.contains("already present") {
            debug!("iSCSI node record already exists for {iqn} at {portal}");
            return Ok(());
        }
        bail!(
            "node record command failed: {}, output: {}",
            exit_text(&out),
            out.combined()
        )
    }

    /// A SendTargets discovery of the portal, reused for the cache duration
    /// and serialized with the portal's other node database updates.
    pub async fn discover(&self, portal: &str, deadline: Option<Instant>) -> Result<()> {
        let fresh = |s: &Self| {
            s.portals
                .lock()
                .expect("portal state")
                .discovered
                .get(portal)
                .is_some_and(|at| at.elapsed() < s.discovery_cache)
        };
        if fresh(self) {
            debug!("Using cached discovery for portal {portal}");
            return Ok(());
        }
        let lock = self.portal_lock(portal);
        let _held = lock.lock().await;
        if fresh(self) {
            return Ok(());
        }
        info!("Performing iSCSI discovery for portal {portal} (serialized)");
        let out = self
            .iscsiadm(&["-m", "discovery", "-t", "sendtargets", "-p", portal], deadline)
            .await?;
        if !out.success() {
            bail!(
                "discovery command failed: {}, output: {}",
                exit_text(&out),
                out.combined()
            );
        }
        self.portals
            .lock()
            .expect("portal state")
            .discovered
            .insert(portal.to_string(), Instant::now());
        Ok(())
    }

    fn invalidate_discovery(&self, portal: &str) {
        self.portals.lock().expect("portal state").discovered.remove(portal);
    }

    /// `--login`, at most `max_concurrent_logins` per portal at a time; a
    /// session the snapshot already shows, exit 15, or "already present" from
    /// a command that exited on its own is success.
    pub async fn login(&self, portal: &str, iqn: &str, sessions: &[Session], deadline: Option<Instant>) -> Result<()> {
        let slots = self.login_slots(portal);
        let acquire = slots.acquire_owned();
        let _slot = match deadline {
            Some(d) => tokio::time::timeout(d.saturating_duration_since(Instant::now()), acquire)
                .await
                .map_err(|_| anyhow!("context canceled waiting for login slot: context deadline exceeded"))?,
            None => acquire.await,
        }
        .map_err(|e| anyhow!("login slot: {e}"))?;
        if sessions.iter().any(|s| s.iqn == iqn && same_portal(&s.portal, portal)) {
            debug!("Already logged in to target {iqn} through {portal}");
            return Ok(());
        }
        let out = self
            .iscsiadm(&["-m", "node", "-T", iqn, "-p", portal, "--login"], deadline)
            .await?;
        if out.success() {
            return Ok(());
        }
        if out.code == Some(EXIT_SESSION_EXISTS) {
            debug!("Target already logged in (exit code {EXIT_SESSION_EXISTS}): {iqn}");
            return Ok(());
        }
        if out.wedged {
            bail!("login wedged (output is unreliable): {}", exit_text(&out));
        }
        if out.combined().contains("already present") {
            debug!("Target already logged in: {iqn}");
            return Ok(());
        }
        bail!("login command failed: {}, output: {}", exit_text(&out), out.combined())
    }

    async fn logout_once(&self, portal: &str, iqn: &str) -> Result<()> {
        let out = self
            .iscsiadm(&["-m", "node", "-T", iqn, "-p", portal, "--logout"], None)
            .await?;
        if out.success() {
            return Ok(());
        }
        if out.code == Some(EXIT_NO_OBJECTS) {
            debug!("Target already logged out (exit code {EXIT_NO_OBJECTS}): {iqn}");
            return Ok(());
        }
        if out.wedged {
            bail!("logout wedged (output is unreliable): {}", exit_text(&out));
        }
        let text = out.combined();
        if text.contains("No matching sessions") || text.contains("not logged in") {
            debug!("Target already logged out: {iqn}");
            return Ok(());
        }
        bail!("logout failed: {}, output: {text}", exit_text(&out))
    }

    /// Logs out of a target through a portal (three attempts on busy and
    /// timeout errors), then deletes its node record (best effort). Not bound
    /// to the caller's deadline, as in the Go node: a logout cut short would
    /// leave a session holding the LUN.
    pub async fn logout(&self, portal: &str, iqn: &str) -> Result<()> {
        debug!("ISCSIDisconnect: portal={portal}, iqn={iqn}");
        let mut delay = Duration::from_millis(200);
        let mut attempt = 1;
        loop {
            match self.logout_once(portal, iqn).await {
                Ok(()) => break,
                Err(e) => {
                    let text = format!("{e:#}").to_lowercase();
                    if !LOGOUT_RETRYABLE.iter().any(|r| text.contains(r)) {
                        return Err(e);
                    }
                    if attempt >= 3 {
                        return Err(e.context(format!("iSCSI logout {iqn}: failed after 3 attempts")));
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                    attempt += 1;
                }
            }
        }
        match self
            .iscsiadm(&["-m", "node", "-T", iqn, "-p", portal, "-o", "delete"], None)
            .await
        {
            Ok(out) if out.success() => {}
            Ok(out) if out.code == Some(EXIT_NO_OBJECTS) => {
                debug!("iSCSI node record already deleted for {iqn} at {portal}")
            }
            Ok(out) => warn!(
                "Failed to delete node record: {}, output: {}",
                exit_text(&out),
                out.combined()
            ),
            Err(e) => warn!("Failed to delete node record: {e}"),
        }
        Ok(())
    }

    /// `--rescan` of the target's session through the portal (expansion).
    pub async fn rescan(&self, portal: &str, iqn: &str, deadline: Option<Instant>) -> Result<()> {
        let out = self
            .iscsiadm(&["-m", "node", "-T", iqn, "-p", portal, "--rescan"], deadline)
            .await?;
        if !out.success() {
            bail!("rescan failed: {}, output: {}", exit_text(&out), out.combined());
        }
        Ok(())
    }

    /// The dm-multipath map `device` is (or resolves to) and the paths under
    /// it, or `None` for a plain disk and for any dm device whose dm UUID is
    /// not `mpath-<wwid>` (a kpartx partition, an LVM volume): (map name,
    /// slave device paths).
    pub fn multipath_paths(&self, device: &str) -> Result<Option<(String, Vec<String>)>> {
        let resolved =
            std::fs::canonicalize(device).map_or_else(|_| device.to_string(), |p| p.to_string_lossy().into_owned());
        let device = self.block_device_parent(&resolved);
        let name = base_name(&device);
        if !name.starts_with("dm-") {
            return Ok(None);
        }
        let dir = self.sysfs.join("block").join(name);
        if !std::fs::read_to_string(dir.join("dm/uuid")).is_ok_and(|u| u.trim().starts_with("mpath-")) {
            return Ok(None);
        }
        let map = std::fs::read_to_string(dir.join("dm/name"))
            .map_err(|e| anyhow!("failed to read the map name of {device}: {e}"))?
            .trim()
            .to_string();
        let slaves = sorted_entries(&dir.join("slaves"))
            .map_err(|e| anyhow!("failed to inspect dm-multipath slaves for {device}: {e}"))?
            .into_iter()
            .map(|e| self.dev.join(e.file_name()).to_string_lossy().into_owned())
            .collect();
        Ok(Some((map, slaves)))
    }

    /// `multipathd resize map <name>`: a map takes its paths' new size only
    /// when told to, after every path was rescanned.
    pub async fn resize_multipath_map(&self, map: &str, deadline: Option<Instant>) -> Result<()> {
        let out = self
            .runner
            .run("multipathd", &["resize", "map", map], self.limits(deadline))
            .await?;
        let text = out.combined();
        if !out.success() || text.trim().to_lowercase().starts_with("fail") {
            bail!(
                "multipathd resize map {map} failed: {}, output: {text}",
                exit_text(&out)
            );
        }
        Ok(())
    }

    /// Session CHAP on the node record before a login (Go
    /// ConfigureISCSICHAPWithContext): the method and user names through
    /// iscsiadm, the passwords last, into the record files. Errors name the
    /// parameter, never a value, and never carry iscsiadm's output.
    pub async fn configure_chap(
        &self,
        portal: &str,
        iqn: &str,
        creds: &Credentials,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let set = |name: &'static str, value: String| async move {
            let out = self
                .iscsiadm(
                    &[
                        "-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", name, "-v", &value,
                    ],
                    deadline,
                )
                .await;
            if out.as_ref().is_ok_and(Output::success) {
                return Ok(());
            }
            Err(anyhow!("failed to set node param {name} ({})", sanitized_class(&out)))
        };
        let secret = |name: &'static str, value: &str| -> Result<()> {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                bail!("failed to set node param {name} (timed out)");
            }
            write_node_record_secret(&self.node_db_roots, portal, iqn, name, value)
        };
        set("node.session.auth.authmethod", "CHAP".into()).await?;
        set("node.session.auth.username", creds.username.clone()).await?;
        if creds.mutual {
            set("node.session.auth.username_in", creds.mutual_username.clone()).await?;
        }
        secret("node.session.auth.password", &creds.password)?;
        if creds.mutual {
            secret("node.session.auth.password_in", &creds.mutual_password)?;
        }
        Ok(())
    }

    /// Connects to a target through one portal and returns its LUN's device
    /// (Go ISCSIConnectWithOptionsAndSessions).
    pub async fn connect(
        &self,
        portal: &str,
        iqn: &str,
        lun: i64,
        options: &ConnectOptions,
        sessions: &[Session],
        deadline: Option<Instant>,
    ) -> Result<String, ConnectError> {
        let started = Instant::now();
        info!("ISCSIConnect: portal={portal}, iqn={iqn}, lun={lun}");
        let timeout = if options.device_timeout.is_zero() {
            Duration::from_secs(60)
        } else {
            options.device_timeout
        };
        let cleanup_delay = if options.session_cleanup_delay.is_zero() {
            Duration::from_millis(500)
        } else {
            options.session_cleanup_delay
        };
        let mut sessions = sessions.to_vec();
        if sessions.iter().any(|s| s.iqn == iqn && same_portal(&s.portal, portal)) {
            info!("Found existing session for {iqn}, validating...");
            match self
                .wait_for_device(
                    portal,
                    iqn,
                    lun,
                    STALE_SESSION_VALIDATION,
                    options.portal_scoped,
                    deadline,
                )
                .await
            {
                Ok(device) => {
                    info!("ISCSIConnect completed (session reuse) in {:?}", started.elapsed());
                    return Ok(device);
                }
                Err(_) => {
                    warn!(
                        "Existing session for {iqn} appears stale (device not found in {}), disconnecting",
                        go_duration(STALE_SESSION_VALIDATION)
                    );
                    if let Err(e) = self.logout(portal, iqn).await {
                        warn!("Failed to disconnect stale session {iqn}: {e:#} (proceeding anyway)");
                    }
                    sessions.retain(|s| s.iqn != iqn || !same_portal(&s.portal, portal));
                    self.wait_for_session_cleanup(portal, iqn, cleanup_delay, deadline)
                        .await;
                }
            }
        }

        self.ensure_node_record(portal, iqn, deadline)
            .await
            .map_err(|e| ConnectError::Other(format!("node record creation failed: {e:#}")))?;
        info!("iSCSI fast-path node record ensured for {iqn}");
        if let Some(creds) = &options.chap {
            self.configure_chap(portal, iqn, creds, deadline)
                .await
                .map_err(|e| ConnectError::ChapConfig(format!("iSCSI CHAP configuration failed for {iqn}: {e:#}")))?;
        }

        let mut login = self.login(portal, iqn, &sessions, deadline).await;
        if let Err(e) = &login {
            let message = format!("{e:#}");
            if is_auth_failure(&message) {
                warn!("iSCSI CHAP authentication failed for {iqn}; not retrying discovery");
                return Err(ConnectError::Auth(iqn.to_string()));
            }
            if !is_target_not_found(&message) {
                return Err(ConnectError::Other(format!("iSCSI login failed for {iqn}: {message}")));
            }
            warn!(
                "iSCSI fast-path login failed for {iqn} (target not found/no portal record), falling back to SendTargets discovery: {message}"
            );
            let mut delay = self.discovery_retry_delay;
            for attempt in 1..=DISCOVERY_RETRIES {
                info!(
                    "iSCSI retry {attempt}/{DISCOVERY_RETRIES}: waiting {} before fresh discovery for {iqn}",
                    go_duration(delay)
                );
                if let Some(d) = deadline
                    && Instant::now() + delay >= d
                {
                    tokio::time::sleep(d.saturating_duration_since(Instant::now())).await;
                    return Err(ConnectError::Other(
                        "context canceled during retry backoff: context deadline exceeded".into(),
                    ));
                }
                tokio::time::sleep(delay).await;
                self.invalidate_discovery(portal);
                match self.discover(portal, deadline).await {
                    Err(e) => {
                        warn!("iSCSI retry {attempt}/{DISCOVERY_RETRIES}: discovery failed for portal {portal}: {e:#}")
                    }
                    Ok(()) => {
                        login = self.login(portal, iqn, &sessions, deadline).await;
                        match &login {
                            Ok(()) => {
                                info!("iSCSI login succeeded for {iqn} after {attempt} discovery retries");
                                break;
                            }
                            Err(e) => {
                                let message = format!("{e:#}");
                                if is_auth_failure(&message) {
                                    warn!(
                                        "iSCSI CHAP authentication failed for {iqn} on post-discovery retry; not retrying"
                                    );
                                    return Err(ConnectError::Auth(iqn.to_string()));
                                }
                                if !is_target_not_found(&message) {
                                    return Err(ConnectError::Other(format!(
                                        "login failed for {iqn} after discovery retry {attempt}: {message}"
                                    )));
                                }
                                warn!(
                                    "iSCSI retry {attempt}/{DISCOVERY_RETRIES}: login still failed for {iqn} (target not found): {message}"
                                );
                            }
                        }
                    }
                }
                // 2, 4, 8, 10, 10 s: doubling, capped at five first delays.
                delay = (delay * 2).min(self.discovery_retry_delay * 5);
            }
            if let Err(e) = &login {
                return Err(ConnectError::Other(format!(
                    "iSCSI login failed for {iqn} after {DISCOVERY_RETRIES} discovery retries (total elapsed: {:?}): {e:#}",
                    started.elapsed()
                )));
            }
        }
        info!("iSCSI login completed for {iqn}");

        let device = self
            .wait_for_device(portal, iqn, lun, timeout, options.portal_scoped, deadline)
            .await
            .map_err(|e| ConnectError::Other(format!("device not found after {}: {e:#}", go_duration(timeout))))?;
        info!("ISCSIConnect completed (full connect) in {:?}", started.elapsed());
        Ok(device)
    }

    /// Polls until no session of the target remains through the portal, up to
    /// `timeout` (an unlistable state counts as present).
    pub async fn wait_for_session_cleanup(
        &self,
        portal: &str,
        iqn: &str,
        timeout: Duration,
        deadline: Option<Instant>,
    ) {
        let until = Instant::now() + timeout;
        loop {
            match self.list_sessions(None).await {
                Ok(sessions) if !sessions.iter().any(|s| s.iqn == iqn && same_portal(&s.portal, portal)) => return,
                Ok(_) => {}
                Err(e) => debug!("iSCSI session cleanup poll for {iqn}: {e:#}"),
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() || deadline.is_some_and(|d| Instant::now() >= d) {
                debug!("iSCSI session cleanup poll for {iqn} ended with the session still present");
                return;
            }
            tokio::time::sleep(left.min(Duration::from_millis(100))).await;
        }
    }

    /// Polls (50 ms, then 100 ms) until the LUN's device appears, up to
    /// `timeout`. A portal-scoped wait takes only the device of that portal's
    /// session; otherwise, as when sessions cannot be listed, a device of any
    /// session of the target will do.
    pub async fn wait_for_device(
        &self,
        portal: &str,
        iqn: &str,
        lun: i64,
        timeout: Duration,
        portal_scoped: bool,
        deadline: Option<Instant>,
    ) -> Result<String> {
        let started = Instant::now();
        let mut interval = Duration::from_millis(50);
        let mut sessions: Result<Vec<Session>> = Ok(Vec::new());
        let mut next_refresh: Option<Instant> = None;
        loop {
            if next_refresh.is_none_or(|at| Instant::now() >= at) {
                sessions = self.list_sessions(None).await;
                next_refresh = Some(Instant::now() + self.session_refresh);
            }
            let found = match &sessions {
                Ok(list) => match self.find_device_for_portal(portal, iqn, lun, list) {
                    Ok(device) => Some(device),
                    Err(PortalLookup::NoSession) if !portal_scoped => self.find_device_by_iqn(iqn, lun).ok(),
                    Err(_) => None,
                },
                Err(_) => self.find_device_by_iqn(iqn, lun).ok(),
            };
            if let Some(device) = found.filter(|d| !d.is_empty()) {
                return Ok(device);
            }
            if started.elapsed() > timeout {
                bail!("timeout waiting for device (iqn={iqn}, lun={lun})");
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                bail!("context canceled waiting for device (iqn={iqn}, lun={lun})");
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(Duration::from_millis(100));
        }
    }

    fn find_device_for_portal(
        &self,
        portal: &str,
        iqn: &str,
        lun: i64,
        sessions: &[Session],
    ) -> Result<String, PortalLookup> {
        match sessions.iter().find(|s| same_portal(&s.portal, portal) && s.iqn == iqn) {
            Some(session) => self.find_device_for_session(&session.id, lun).map_err(|e| {
                debug!("iSCSI device of session {} not found yet: {e:#}", session.id);
                PortalLookup::Failed
            }),
            None => Err(PortalLookup::NoSession),
        }
    }

    /// The device of any session of the target (sysfs `targetname`).
    pub fn find_device_by_iqn(&self, iqn: &str, lun: i64) -> Result<String> {
        let root = self.sysfs.join("class/iscsi_session");
        for session in sorted_names(&root, |n| n.starts_with("session")) {
            let Ok(target) = std::fs::read_to_string(root.join(&session).join("targetname")) else {
                continue;
            };
            if target.trim() != iqn {
                continue;
            }
            if let Ok(device) = self.find_device_for_session(&session["session".len()..], lun)
                && !device.is_empty()
            {
                return Ok(device);
            }
        }
        bail!("device not found for iqn={iqn}, lun={lun}")
    }

    /// The LUN's block device on the SCSI host owning the session. Never a
    /// session-agnostic lookup: targets commonly share LUN numbers.
    pub fn find_device_for_session(&self, id: &str, lun: i64) -> Result<String> {
        let number: i64 = id.parse().map_err(|e| anyhow!("failed to parse session name: {e}"))?;
        let hosts_root = self.sysfs.join("class/iscsi_host");
        for host in sorted_names(&hosts_root, |n| n.starts_with("host")) {
            if std::fs::symlink_metadata(hosts_root.join(&host).join("device").join(format!("session{number}")))
                .is_err()
            {
                continue;
            }
            let digits: String = host["host".len()..].chars().take_while(char::is_ascii_digit).collect();
            let host_number: i64 = digits
                .parse()
                .map_err(|_| anyhow!("failed to parse host name: {host}"))?;
            let block = self
                .sysfs
                .join("class/scsi_device")
                .join(format!("{host_number}:0:0:{lun}"))
                .join("device/block");
            for name in sorted_names(&block, |_| true) {
                let device = self.dev.join(&name);
                if device.exists() {
                    return Ok(device.to_string_lossy().into_owned());
                }
            }
        }
        bail!("device for session {number} not found")
    }

    /// The SCSI identifier dm-multipath keys its map by.
    pub fn scsi_wwid(&self, device: &str) -> Result<String> {
        let dir = self.sysfs.join("block").join(base_name(device)).join("device");
        let raw = std::fs::read(dir.join("wwid"))
            .or_else(|_| std::fs::read(dir.join("vpd_pg83")))
            .map_err(|e| anyhow!("failed to read WWN: {e}"))?;
        Ok(normalize_scsi_wwid(&String::from_utf8_lossy(&raw)))
    }

    /// The WWID of a dm-multipath head; any other device (a component SCSI
    /// path) fails.
    pub fn multipath_wwid(&self, device: &str) -> Result<String> {
        let resolved =
            std::fs::canonicalize(device).map_or_else(|_| device.to_string(), |p| p.to_string_lossy().into_owned());
        let name = base_name(&resolved);
        if !name.starts_with("dm-") {
            bail!("device {resolved} is not a dm-multipath map");
        }
        let uuid = std::fs::read_to_string(self.sysfs.join("block").join(name).join("dm/uuid"))
            .map_err(|e| anyhow!("read dm UUID for {resolved}: {e}"))?;
        match uuid.trim().strip_prefix("mpath-") {
            Some(wwid) if !wwid.is_empty() => Ok(normalize_scsi_wwid(wwid)),
            _ => bail!("device {resolved} is not a dm-multipath map"),
        }
    }

    /// The dm-multipath map of a WWID, by its dm UUID: `/dev/mapper/<name>`,
    /// else `/dev/dm-N`.
    pub fn find_multipath_device(&self, wwid: &str) -> Result<String> {
        let block = self.sysfs.join("block");
        let want = format!("mpath-{}", normalize_scsi_wwid(wwid));
        for dm in sorted_names(&block, |n| n.starts_with("dm-")) {
            let dir = block.join(&dm);
            match std::fs::read_to_string(dir.join("dm/uuid")) {
                Ok(uuid) if uuid.trim() == want => {}
                _ => continue,
            }
            if let Ok(name) = std::fs::read_to_string(dir.join("dm/name")) {
                let name = name.trim();
                // A single path component only: never escape /dev/mapper.
                if !name.is_empty() && !name.contains('/') && name != "." && name != ".." {
                    let mapper = self.dev.join("mapper").join(name);
                    if mapper.exists() {
                        return Ok(mapper.to_string_lossy().into_owned());
                    }
                }
            }
            let device = self.dev.join(&dm);
            if device.exists() {
                return Ok(device.to_string_lossy().into_owned());
            }
        }
        bail!("dm-multipath map not found for WWID {wwid}")
    }

    /// Refuses a SCSI path dm-multipath holds: mounting the component would
    /// bypass the map.
    pub fn check_multipath_ownership(&self, device: &str) -> Result<()> {
        let holders = self.sysfs.join("block").join(base_name(device)).join("holders");
        let entries = match std::fs::read_dir(&holders) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => bail!("failed to inspect holders for iSCSI device {device}: {e}"),
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with("dm-") {
                bail!("iSCSI device {device} is claimed by dm-multipath; staging the raw component path is unsafe");
            }
        }
        Ok(())
    }

    /// device-mapper and multipathd, which multi-portal login needs.
    pub fn check_multipath_prerequisites(&self) -> Result<()> {
        if let Err(e) = std::fs::metadata(self.dev.join("mapper/control")) {
            bail!("device-mapper control is unavailable: {e}");
        }
        if self.multipathd_sockets.iter().any(|s| std::fs::metadata(s).is_ok()) {
            return Ok(());
        }
        bail!("multipathd control socket is unavailable")
    }

    /// A SCSI disk (`sd[a-z]+`) or a dm-multipath map (Go IsLikelyISCSIDevice).
    pub fn is_likely_iscsi_device(&self, device: &str) -> bool {
        let name = base_name(device);
        if name.starts_with("dm-") {
            return std::fs::read_to_string(self.sysfs.join("block").join(name).join("dm/uuid"))
                .is_ok_and(|u| u.trim().starts_with("mpath-"));
        }
        name.len() >= 3 && name.starts_with("sd") && name[2..].bytes().all(|b| b.is_ascii_lowercase())
    }

    /// The whole disk of a partition (`sda1` -> `sda`), else the device.
    pub fn block_device_parent(&self, device: &str) -> String {
        let entry = self.sysfs.join("class/block").join(base_name(device));
        if std::fs::metadata(entry.join("partition")).is_err() {
            return device.to_string();
        }
        let Ok(resolved) = std::fs::canonicalize(&entry) else {
            return device.to_string();
        };
        match (Path::new(device).parent(), resolved.parent().and_then(Path::file_name)) {
            (Some(dir), Some(parent)) => dir.join(parent).to_string_lossy().into_owned(),
            _ => device.to_string(),
        }
    }

    /// The portal and target of the session a device belongs to, through its
    /// sysfs ancestry (Go GetISCSIInfoFromDeviceWithSessions): a partition is
    /// its disk's, a dm map its first iSCSI slave's.
    pub fn info_from_device(&self, device: &str, sessions: &[Session]) -> Result<(String, String), InfoError> {
        let resolved =
            std::fs::canonicalize(device).map_or_else(|_| device.to_string(), |p| p.to_string_lossy().into_owned());
        let device = self.block_device_parent(&resolved);
        let name = base_name(&device).to_string();
        if name.starts_with("dm-") {
            let slaves = sorted_entries(&self.sysfs.join("block").join(&name).join("slaves"))
                .map_err(|e| InfoError::Unknown(format!("failed to inspect dm-multipath slaves for {device}: {e}")))?;
            let mut all_local = !slaves.is_empty();
            for slave in slaves {
                let path = self.dev.join(slave.file_name());
                match self.info_from_device(&path.to_string_lossy(), sessions) {
                    Ok(found) => return Ok(found),
                    Err(InfoError::NotIscsi(_)) => {}
                    Err(InfoError::Unknown(_)) => all_local = false,
                }
            }
            if all_local {
                return Err(InfoError::NotIscsi(format!("every slave of {device} is local")));
            }
            return Err(InfoError::Unknown(format!(
                "no iSCSI session found for dm-multipath device {device}"
            )));
        }
        let target = std::fs::canonicalize(self.sysfs.join("block").join(&name).join("device"))
            .map_err(|e| InfoError::Unknown(format!("failed to resolve sysfs path: {e}")))?;
        let mut session_dir: Option<PathBuf> = None;
        let mut reached_root = false;
        let mut current = target.as_path();
        for _ in 0..64 {
            if current
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("session"))
            {
                session_dir = Some(current.to_path_buf());
                break;
            }
            match current.parent() {
                Some(parent) => current = parent,
                None => {
                    reached_root = true;
                    break;
                }
            }
        }
        let Some(session_dir) = session_dir else {
            if !reached_root {
                return Err(InfoError::Unknown(format!(
                    "sysfs ancestry of {device} exceeded the walk bound"
                )));
            }
            return Err(InfoError::NotIscsi(device));
        };
        let session = session_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut iqn = std::fs::read_to_string(self.sysfs.join("class/iscsi_session").join(&session).join("targetname"))
            .map(|t| t.trim().to_string())
            .unwrap_or_default();
        if iqn.is_empty() {
            let nested = session_dir.join("iscsi_session");
            if let Some(first) = sorted_names(&nested, |n| n.starts_with("session")).first()
                && let Ok(text) = std::fs::read_to_string(nested.join(first).join("targetname"))
            {
                iqn = text.trim().to_string();
            }
        }
        if iqn.is_empty() {
            return Err(InfoError::Unknown(format!(
                "could not find targetname for session {}",
                session_dir.display()
            )));
        }
        let id = session.strip_prefix("session").unwrap_or_default();
        sessions
            .iter()
            .find(|s| s.iqn == iqn && s.id == id)
            .map(|s| (s.portal.clone(), s.iqn.clone()))
            .ok_or_else(|| InfoError::Unknown(format!("could not find portal for IQN {iqn}")))
    }

    /// `info_from_device` with a fresh session list.
    pub async fn info_from_device_listed(&self, device: &str) -> Result<(String, String), InfoError> {
        let sessions = self
            .list_sessions(None)
            .await
            .map_err(|e| InfoError::Unknown(format!("failed to get sessions: {e:#}")))?;
        self.info_from_device(device, &sessions)
    }
}
