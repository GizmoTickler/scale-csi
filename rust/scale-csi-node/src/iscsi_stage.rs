//! Staging and unstaging iSCSI volumes (`pkg/driver/node.go`: stageISCSIVolume,
//! convergeISCSIMultipathPaths, convergeExistingISCSIPaths,
//! cleanupOrphanedSessionByVolumeID; `iscsi_chap.go`: nodeISCSIChAPCredentials).
//!
//! - Single portal (no usable `portals` hint, or no multipathd on the node):
//!   an existing session of the target is logged out first unless the staged
//!   device is still live; the LUN's device must not be a path dm-multipath
//!   holds.
//! - Multipath (a hint of two or more IP portals and dm-multipath on the
//!   node): every session of the target is logged out first unless the staged
//!   device is live; the primary portal must log in, the others share 5 s; a
//!   secondary path whose SCSI WWID differs from the primary's is logged out
//!   again; the stage uses the dm map of the WWID, or, when none appears within
//!   5 s, logs the secondaries out and uses the primary path.
//! - CHAP: the volume context's `chap` mode (CHAP or CHAP_MUTUAL) asks for the
//!   node-stage secret; a missing or invalid secret fails before any login, a
//!   rejected one is Unauthenticated and never retried.
//! - Unstage logs out every session of the target (by the device read from the
//!   live mount, else by the target name derived from the volume ID); a found
//!   session that will not log out fails the unstage.
//! - Session GC collects only sessions to the configured portal whose target is
//!   one this driver names, unused by any staged volume for the grace period.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use log::{debug, info, warn};
use tonic::{Code, Status};

use crate::capability::{protocol_share_name, session_target_matches};
use crate::csi;
use crate::events::ObjectRef;
use crate::iscsi::{ConnectError, ConnectOptions, Credentials, InfoError, Session, parse_ip, split_host_port};
use crate::nvme_addresses::{join_host_port, normalize_address};
use crate::publish::REASON_MOUNT_FAILED;
use crate::service::State;
use crate::stage::{finalize_staged_device, staged_block_device_path};
use crate::ublk_stage::bounded;

/// `scale_csi_node_connect_total`'s transport label.
pub const TRANSPORT_LABEL: &str = "iscsi";
pub const REASON_LOGIN_FAILED: &str = "ISCSILoginFailed";
pub const REASON_PATH_DEGRADED: &str = "ISCSIPathDegraded";
pub const REASON_MULTIPATH_UNAVAILABLE: &str = "ISCSIMultipathUnavailable";
pub const REASON_CHAP_FAILED: &str = "ISCSICHAPFailed";
/// Secondary logins and the dm map share this budget.
pub const SECONDARY_PATH_BUDGET: Duration = Duration::from_secs(5);
const INVALID_HINT_LABEL: &str = "invalid-publish-context";

const CHAP_KEY: &str = "chap";
const CHAP_NONE: &str = "NONE";
const CHAP_MUTUAL: &str = "CHAP_MUTUAL";

fn warn_event(state: &State, event: Option<&ObjectRef>, reason: &str, message: &str) {
    if let Some(object) = event {
        state.events.warning(object, reason, message);
    }
}

/// The target name the controller gives a volume (Go iscsiShareName).
pub fn share_name(state: &State, volume_id: &str) -> String {
    protocol_share_name(&format!("{volume_id}{}", state.config.iscsi.name_suffix))
}

/// The first of the keys with a non-blank value, verbatim (Go firstSecretKey).
fn first_secret(secrets: &HashMap<String, String>, keys: &[&str]) -> String {
    keys.iter()
        .filter_map(|k| secrets.get(*k))
        .find(|v| !v.trim().is_empty())
        .cloned()
        .unwrap_or_default()
}

fn validate_secret_value(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "iSCSI CHAP {name} is required when CHAP is enabled"
        )));
    }
    if value != value.trim() {
        return Err(Status::invalid_argument(format!(
            "iSCSI CHAP {name} must not have leading or trailing whitespace"
        )));
    }
    if value.contains('#') {
        return Err(Status::invalid_argument(format!(
            "iSCSI CHAP {name} must not contain '#'"
        )));
    }
    if !(12..=16).contains(&value.len()) {
        return Err(Status::invalid_argument(format!(
            "iSCSI CHAP {name} must be 12-16 characters (got {})",
            value.len()
        )));
    }
    Ok(())
}

/// The node-stage CHAP credentials the volume asks for (Go
/// nodeISCSIChAPCredentials): none when its `chap` mode is absent or NONE; a
/// missing or invalid secret is InvalidArgument. Messages never carry a value.
pub fn chap_credentials(
    context: &HashMap<String, String>,
    secrets: &HashMap<String, String>,
) -> Result<Option<Credentials>, Status> {
    let mode = context.get(CHAP_KEY).map(String::as_str).unwrap_or_default();
    if mode.is_empty() || mode == CHAP_NONE {
        return Ok(None);
    }
    let username = first_secret(secrets, &["username", "node.session.auth.username"]);
    let password = first_secret(secrets, &["password", "node.session.auth.password"]);
    let mutual_username = first_secret(secrets, &["mutualUsername", "node.session.auth.username_in"]);
    let mutual_password = first_secret(secrets, &["mutualPassword", "node.session.auth.password_in"]);
    if username.is_empty() {
        return Err(Status::invalid_argument(
            "iSCSI CHAP username is required when CHAP is enabled",
        ));
    }
    if let Some(tag) = secrets.get("tag").filter(|t| !t.trim().is_empty())
        && !tag.trim().parse::<i64>().is_ok_and(|t| t > 0)
    {
        return Err(Status::invalid_argument(format!(
            "iSCSI CHAP tag {:?} must be a positive integer",
            tag.trim()
        )));
    }
    validate_secret_value("password", &password)?;
    if !mutual_username.is_empty() {
        validate_secret_value("mutualPassword", &mutual_password)?;
        if mutual_password == password {
            return Err(Status::invalid_argument(
                "iSCSI CHAP mutualPassword must differ from password",
            ));
        }
    } else if !mutual_password.is_empty() {
        return Err(Status::invalid_argument(
            "iSCSI CHAP mutualPassword requires mutualUsername",
        ));
    }
    let mut creds = Credentials {
        username,
        password,
        mutual_username: String::new(),
        mutual_password: String::new(),
        mutual: false,
    };
    if mode == CHAP_MUTUAL {
        if mutual_username.is_empty() || mutual_password.is_empty() {
            return Err(Status::invalid_argument(
                "iSCSI CHAP_MUTUAL volume requires mutualUsername and mutualPassword in the node-stage secret",
            ));
        }
        creds.mutual = true;
        creds.mutual_username = mutual_username;
        creds.mutual_password = mutual_password;
    }
    Ok(Some(creds))
}

/// A portal host: an IP (IPv6 unbracketed, as given) or a DNS name, lower-cased
/// and without a trailing dot (Go normalizeISCSIHost).
fn normalize_host(raw: &str) -> Result<String, String> {
    if let Ok(address) = normalize_address(raw) {
        return Ok(address);
    }
    if raw != raw.trim()
        || raw.is_empty()
        || raw.contains("://")
        || raw.contains(['[', ']', '/', '\\', ' ', '\t', '\r', '\n', ':'])
    {
        return Err(format!("invalid iSCSI host {raw:?}"));
    }
    let host = raw.strip_suffix('.').unwrap_or(raw).to_lowercase();
    let label_ok = |label: &str| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    };
    if host.is_empty() || host.len() > 253 || !host.split('.').all(label_ok) {
        return Err(format!("invalid iSCSI hostname {raw:?}"));
    }
    Ok(host)
}

/// A target portal as `host:port`, the port 3260 when it has none (Go
/// normalizeISCSITargetPortal).
pub fn normalize_target_portal(raw: &str) -> Result<String, String> {
    if raw != raw.trim() || raw.is_empty() {
        return Err(format!("invalid portal {raw:?}"));
    }
    let Some((host, port)) = split_host_port(raw) else {
        return Ok(join_host_port(&normalize_host(raw)?, "3260"));
    };
    let host = normalize_host(host)?;
    match port.parse::<i64>() {
        Ok(p) if (1..=65535).contains(&p) => Ok(join_host_port(&host, &p.to_string())),
        _ => Err(format!("invalid port {port:?}")),
    }
}

/// The `portals` hint (Go parseISCSIMultipathPortals): IP portals, normalized
/// and de-duplicated in order; whether the hint is present; and why it was
/// discarded, if it was (whole, never partly used).
pub fn parse_multipath_portals(context: &HashMap<String, String>) -> (Vec<String>, bool, Option<String>) {
    let Some(raw) = context.get("portals") else {
        return (Vec::new(), false, None);
    };
    let decoded: Option<Vec<String>> = match serde_json::from_str(raw) {
        Ok(decoded) => decoded,
        Err(e) => return (Vec::new(), true, Some(format!("decode portals: {e}"))),
    };
    let decoded = decoded.unwrap_or_default();
    if decoded.is_empty() {
        return (Vec::new(), true, Some("portals is empty".into()));
    }
    let mut out: Vec<String> = Vec::with_capacity(decoded.len());
    for raw_portal in &decoded {
        let portal = match normalize_target_portal(raw_portal) {
            Ok(portal) => portal,
            Err(e) => {
                return (
                    Vec::new(),
                    true,
                    Some(format!("portals contains invalid target portal {raw_portal:?}: {e}")),
                );
            }
        };
        let is_ip = split_host_port(&portal).is_some_and(|(host, _)| parse_ip(host).is_some());
        if !is_ip {
            return (
                Vec::new(),
                true,
                Some(format!("portals contains non-IP target portal {raw_portal:?}")),
            );
        }
        if !out.contains(&portal) {
            out.push(portal);
        }
    }
    (out, true, None)
}

/// What one iSCSI stage needs.
pub struct StageRequest<'a> {
    /// The volume context with the `portals` publish hint merged in.
    pub context: &'a HashMap<String, String>,
    pub secrets: &'a HashMap<String, String>,
    pub staging: &'a str,
    pub capability: &'a csi::VolumeCapability,
    pub event: Option<&'a ObjectRef>,
    pub deadline: Option<Instant>,
}

fn parse_lun(text: &str) -> Result<i64, Status> {
    if text.is_empty() {
        return Ok(0);
    }
    text.parse()
        .map_err(|_| Status::invalid_argument(format!("invalid LUN number: {text}")))
}

async fn list_or_empty(state: &State, why: &str, deadline: Option<Instant>) -> Vec<Session> {
    match state.iscsi.list_sessions(deadline).await {
        Ok(sessions) => sessions,
        Err(e) => {
            warn!("Failed to list iSCSI sessions {why}: {e:#}");
            Vec::new()
        }
    }
}

/// Polls until no session of the target remains (any portal), up to
/// `node.sessionCleanupDelay`; returns the latest list (empty when the last
/// listing failed, as in Go).
async fn wait_for_target_gone(state: &State, iqn: &str, deadline: Option<Instant>) -> Vec<Session> {
    let until = Instant::now() + state.config.node.session_cleanup_delay();
    loop {
        let latest = match state.iscsi.list_sessions(deadline).await {
            Ok(sessions) => {
                if !sessions.iter().any(|s| s.iqn == iqn) {
                    return sessions;
                }
                sessions
            }
            Err(e) => {
                debug!("iSCSI session cleanup poll for {iqn}: {e:#}");
                Vec::new()
            }
        };
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() || deadline.is_some_and(|d| Instant::now() >= d) {
            debug!("iSCSI session cleanup poll for {iqn} ended with the session still present");
            return latest;
        }
        tokio::time::sleep(left.min(Duration::from_millis(100))).await;
    }
}

/// Single portal: an existing session of the target is logged out through the
/// volume's portal before the login, unless the staged device is still live.
async fn preemptive_single(
    state: &State,
    portal: &str,
    iqn: &str,
    staging: &str,
    sessions: Vec<Session>,
    deadline: Option<Instant>,
) -> Vec<Session> {
    if !sessions.iter().any(|s| s.iqn == iqn) {
        return sessions;
    }
    if let Some(live) = crate::nvme_kernel::staged_device(state, staging, deadline).await {
        info!("Skipping pre-emptive iSCSI disconnect for {iqn}: staged device {live} is still live");
        return sessions;
    }
    info!("Found existing iSCSI session for {iqn}, disconnecting before reconnect");
    if let Err(e) = state.iscsi.logout(portal, iqn).await {
        warn!("Failed to disconnect existing session {iqn}: {e:#} (proceeding anyway)");
    }
    wait_for_target_gone(state, iqn, deadline).await
}

/// Multipath: every session of the target is logged out through its own
/// portal first, unless the staged device is still live.
async fn preemptive_multipath(
    state: &State,
    iqn: &str,
    staging: &str,
    sessions: Vec<Session>,
    deadline: Option<Instant>,
) -> Vec<Session> {
    if let Some(live) = crate::nvme_kernel::staged_device(state, staging, deadline).await {
        info!("Skipping pre-emptive iSCSI multipath disconnect for {iqn}: staged device {live} is still live");
        return sessions;
    }
    let mut found = false;
    for session in sessions.iter().filter(|s| s.iqn == iqn) {
        found = true;
        if let Err(e) = state.iscsi.logout(&session.portal, iqn).await {
            warn!(
                "Failed to disconnect existing iSCSI session {iqn} through {}: {e:#} (proceeding anyway)",
                session.portal
            );
        }
    }
    if !found {
        return sessions;
    }
    wait_for_target_gone(state, iqn, deadline).await
}

async fn wait_for_multipath_device(state: &State, wwid: &str, until: Instant) -> Result<String> {
    loop {
        if let Ok(device) = state.iscsi.find_multipath_device(wwid) {
            return Ok(device);
        }
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(anyhow!("context deadline exceeded"));
        }
        tokio::time::sleep(left.min(Duration::from_millis(100))).await;
    }
}

/// Logs in through every portal and resolves the dm-multipath map (Go
/// convergeISCSIMultipathPaths): the device and the per-path failures.
pub async fn converge_multipath_paths(
    state: &State,
    portals: &[String],
    iqn: &str,
    lun: i64,
    options: &ConnectOptions,
    sessions: &[Session],
    deadline: Option<Instant>,
) -> Result<(String, Vec<String>), ConnectError> {
    let mut scoped = options.clone();
    scoped.portal_scoped = true;
    let primary = &portals[0];
    let primary_device = match state
        .iscsi
        .connect(primary, iqn, lun, &scoped, sessions, deadline)
        .await
    {
        Ok(device) => device,
        Err(e) => {
            state.metrics.record_iscsi_path_connect(primary, "error");
            return Err(e);
        }
    };
    state.metrics.record_iscsi_path_connect(primary, "success");
    let wwid = state
        .iscsi
        .scsi_wwid(&primary_device)
        .map_err(|e| ConnectError::Other(format!("resolve SCSI WWID for primary path {primary_device}: {e:#}")))?;

    let mut failures: Vec<String> = Vec::new();
    let mut connected: Vec<&String> = Vec::new();
    let secondary_deadline = bounded(deadline, SECONDARY_PATH_BUDGET);
    let mut secondary = ConnectOptions {
        device_timeout: SECONDARY_PATH_BUDGET,
        session_cleanup_delay: scoped.session_cleanup_delay,
        chap: scoped.chap.clone(),
        portal_scoped: true,
    };
    if !scoped.device_timeout.is_zero() && scoped.device_timeout < secondary.device_timeout {
        secondary.device_timeout = scoped.device_timeout;
    }
    for portal in &portals[1..] {
        let device = match state
            .iscsi
            .connect(portal, iqn, lun, &secondary, sessions, Some(secondary_deadline))
            .await
        {
            Ok(device) => device,
            Err(e) => {
                state.metrics.record_iscsi_path_connect(portal, "error");
                failures.push(format!("{portal}: {e}"));
                continue;
            }
        };
        let other = state.iscsi.scsi_wwid(&device);
        if other.as_ref().ok() != Some(&wwid) {
            state.metrics.record_iscsi_path_connect(portal, "error");
            // A path to another LUN must never stay beside the map.
            if let Err(e) = state.iscsi.logout(portal, iqn).await {
                failures.push(format!("{portal} mismatched-path logout: {e:#}"));
            }
            match other {
                Err(e) => failures.push(format!("{portal}: resolve SCSI WWID: {e:#}")),
                Ok(other) => failures.push(format!("{portal}: SCSI WWID {other} differs from primary {wwid}")),
            }
            continue;
        }
        state.metrics.record_iscsi_path_connect(portal, "success");
        connected.push(portal);
    }

    match wait_for_multipath_device(state, &wwid, bounded(deadline, SECONDARY_PATH_BUDGET)).await {
        Ok(map) => return Ok((map, failures)),
        Err(e) => {
            // No map: the extra sessions go, or multipathd could claim the raw
            // primary path after it is mounted.
            for portal in connected {
                if let Err(e) = state.iscsi.logout(portal, iqn).await {
                    failures.push(format!("{portal} fallback logout: {e:#}"));
                }
            }
            failures.push(format!("dm map for WWID {wwid}: {e:#}"));
        }
    }
    if let Err(e) = state.iscsi.check_multipath_ownership(&primary_device) {
        return Err(ConnectError::Other(format!(
            "dm-multipath claimed primary path but its map could not be resolved: {e:#}"
        )));
    }
    Ok((primary_device, failures))
}

/// Stages a volume through the kernel iSCSI initiator.
pub async fn stage(state: &State, req: StageRequest<'_>) -> Result<(), Status> {
    let get = |k: &str| req.context.get(k).map(String::as_str).unwrap_or_default();
    let (portal, iqn, lun_text) = (get("portal"), get("iqn"), get("lun"));
    if portal.is_empty() || iqn.is_empty() {
        return Err(Status::invalid_argument(
            "iSCSI portal and IQN are required in volume context",
        ));
    }
    let lun = parse_lun(lun_text)?;
    let mounted = state
        .mounter
        .is_mounted(req.staging, req.deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    if mounted {
        info!("iSCSI volume already mounted at {}", req.staging);
        return Ok(());
    }
    let mut sessions = list_or_empty(state, &format!("before staging {iqn}"), req.deadline).await;
    if matches!(
        req.capability.access_type,
        Some(csi::volume_capability::AccessType::Block(_))
    ) && let Some(device) = staged_block_device_path(req.staging, &state.host.dev_dir)
        && state
            .iscsi
            .info_from_device(&device, &sessions)
            .is_ok_and(|(_, staged)| staged == iqn)
    {
        info!("iSCSI block volume already staged at {}", req.staging);
        return Ok(());
    }

    let (portals, present, discarded) = parse_multipath_portals(req.context);
    let mut multipath = false;
    if let Some(reason) = discarded {
        let message =
            format!("iSCSI multipath portal list for {iqn} was discarded; using the primary portal only: {reason}");
        warn!("{message}");
        state.metrics.record_iscsi_path_connect(INVALID_HINT_LABEL, "error");
        warn_event(state, req.event, REASON_PATH_DEGRADED, &message);
    } else if present && portals.len() < 2 {
        let message =
            format!("iSCSI multipath portal list for {iqn} has fewer than two portals; using the primary portal only");
        warn!("{message}");
        warn_event(state, req.event, REASON_MULTIPATH_UNAVAILABLE, &message);
    } else if portals.len() > 1 {
        match state.iscsi.check_multipath_prerequisites() {
            // iSCSI has no kernel path aggregation: without multipathd the node
            // stays single path, never with sessions that could race a mount.
            Err(e) => {
                let message =
                    format!("iSCSI multipath is unavailable for {iqn}; staging through the primary portal only: {e:#}");
                warn!("{message}");
                warn_event(state, req.event, REASON_MULTIPATH_UNAVAILABLE, &message);
            }
            Ok(()) => multipath = true,
        }
    }
    sessions = if multipath {
        preemptive_multipath(state, iqn, req.staging, sessions, req.deadline).await
    } else {
        preemptive_single(state, portal, iqn, req.staging, sessions, req.deadline).await
    };

    // A CHAP volume without a usable secret fails here, before any login.
    let chap = chap_credentials(req.context, req.secrets).inspect_err(|e| {
        let mut keys: Vec<&String> = req.secrets.keys().collect();
        keys.sort();
        debug!(
            "iSCSI CHAP credential validation failed for {iqn} (secret keys: {keys:?}): {}",
            e.message()
        );
    })?;
    let options = ConnectOptions {
        device_timeout: state.config.iscsi.device_wait_timeout(),
        session_cleanup_delay: state.config.node.session_cleanup_delay(),
        chap,
        portal_scoped: false,
    };
    let result = if multipath {
        converge_multipath_paths(state, &portals, iqn, lun, &options, &sessions, req.deadline).await
    } else {
        state
            .iscsi
            .connect(portal, iqn, lun, &options, &sessions, req.deadline)
            .await
            .map(|device| (device, Vec::new()))
    };
    let (device, failures) = match result {
        Ok(found) => found,
        Err(e) => {
            state.metrics.record_node_connect(TRANSPORT_LABEL, "error");
            let (status, reason, message) = match &e {
                ConnectError::Auth(_) => {
                    let status = Status::unauthenticated(format!("iSCSI CHAP authentication failed for {iqn}"));
                    let message = status.message().to_string();
                    (status, REASON_LOGIN_FAILED, message)
                }
                ConnectError::ChapConfig(detail) => (
                    Status::internal(format!("failed to configure iSCSI CHAP for {iqn}")),
                    REASON_CHAP_FAILED,
                    format!("CHAP configuration failed: {detail}"),
                ),
                ConnectError::Other(detail) => {
                    let status = Status::internal(format!("failed to connect iSCSI: {detail}"));
                    let message = status.message().to_string();
                    (status, REASON_LOGIN_FAILED, message)
                }
            };
            warn_event(state, req.event, reason, &message);
            return Err(status);
        }
    };
    state.metrics.record_node_connect(TRANSPORT_LABEL, "success");
    if !failures.is_empty() {
        warn_event(
            state,
            req.event,
            REASON_PATH_DEGRADED,
            &format!("iSCSI path convergence for {iqn} is degraded: {}", failures.join("\n")),
        );
    }
    if !multipath {
        // Never mount one component of a map an operator's multipathd built.
        state
            .iscsi
            .check_multipath_ownership(&device)
            .map_err(|e| Status::failed_precondition(format!("{e:#}")))?;
    }
    let block = matches!(
        req.capability.access_type,
        Some(csi::volume_capability::AccessType::Block(_))
    );
    finalize_staged_device(&state.mounter, &device, req.staging, req.capability, req.deadline)
        .await
        .inspect_err(|status| {
            if !block && status.code() == Code::Internal {
                warn_event(state, req.event, REASON_MOUNT_FAILED, status.message());
            }
        })
}

fn same_device(left: &str, right: &str) -> bool {
    if left.is_empty() || right.is_empty() {
        return false;
    }
    let resolve =
        |p: &str| std::fs::canonicalize(p).map_or_else(|_| p.to_string(), |r| r.to_string_lossy().into_owned());
    let (l, r) = (resolve(left), resolve(right));
    if crate::locks::go_clean(&l) == crate::locks::go_clean(&r) {
        return true;
    }
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(&l), std::fs::metadata(&r)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

fn convergence_skipped(state: &State, event: Option<&ObjectRef>, iqn: &str, detail: &str) {
    let message = format!(
        "iSCSI multipath convergence for already-staged volume {iqn} was skipped; keep the live path unchanged and perform a full unstage/restage to adopt dm-multipath: {detail}"
    );
    warn!("{message}");
    warn_event(state, event, REASON_MULTIPATH_UNAVAILABLE, &message);
}

/// Tops up the paths of an already staged multipath volume (a replay), only
/// when what is staged is the live dm map of the volume: best effort, the
/// stage stays good whatever happens (Go convergeExistingISCSIPaths).
pub async fn converge_existing(
    state: &State,
    context: &HashMap<String, String>,
    secrets: &HashMap<String, String>,
    staging: &str,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let (portals, _, discarded) = parse_multipath_portals(context);
    if discarded.is_some() || portals.len() < 2 {
        return;
    }
    let iqn = context.get("iqn").map(String::as_str).unwrap_or_default();
    let lun = match context.get("lun").map(String::as_str).unwrap_or_default() {
        "" => 0,
        text => match text.parse::<i64>() {
            Ok(lun) => lun,
            Err(_) => return,
        },
    };
    if iqn.is_empty() {
        return;
    }
    let Some(staged) = crate::nvme_kernel::staged_device(state, staging, deadline).await else {
        convergence_skipped(
            state,
            event,
            iqn,
            &format!("the live staging device at {staging} could not be resolved"),
        );
        return;
    };
    let wwid = match state.iscsi.multipath_wwid(&staged) {
        Ok(wwid) => wwid,
        Err(e) => {
            convergence_skipped(
                state,
                event,
                iqn,
                &format!("staged device {staged} is not an existing dm-multipath map: {e:#}"),
            );
            return;
        }
    };
    match state.iscsi.find_multipath_device(&wwid) {
        Ok(map) if same_device(&staged, &map) => {}
        found => {
            let mut detail = format!("staged device {staged} is not the live dm-multipath map for WWID {wwid}");
            if let Err(e) = found {
                detail = format!("{detail}: {e:#}");
            }
            convergence_skipped(state, event, iqn, &detail);
            return;
        }
    }
    if let Err(e) = state.iscsi.check_multipath_prerequisites() {
        convergence_skipped(
            state,
            event,
            iqn,
            &format!("dm-multipath prerequisites are unavailable: {e:#}"),
        );
        return;
    }
    let Ok(chap) = chap_credentials(context, secrets) else {
        return;
    };
    let sessions = match state.iscsi.list_sessions(deadline).await {
        Ok(sessions) => sessions,
        Err(e) => {
            debug!("Cannot converge existing iSCSI paths for {iqn}: {e:#}");
            Vec::new()
        }
    };
    let options = ConnectOptions {
        device_timeout: state.config.iscsi.device_wait_timeout(),
        session_cleanup_delay: state.config.node.session_cleanup_delay(),
        chap,
        portal_scoped: false,
    };
    let failures = match converge_multipath_paths(state, &portals, iqn, lun, &options, &sessions, deadline).await {
        Ok((_, failures)) => failures,
        Err(e) => vec![e.to_string()],
    };
    if !failures.is_empty() {
        warn_event(
            state,
            event,
            REASON_PATH_DEGRADED,
            &format!("iSCSI path convergence for {iqn} is degraded: {}", failures.join("\n")),
        );
    }
}

/// That a staged device belongs to the target the request names.
pub async fn verify_stage_source(state: &State, device: &str, context: &HashMap<String, String>) -> Result<(), Status> {
    let (_, actual) = state
        .iscsi
        .info_from_device_listed(device)
        .await
        .map_err(|e| Status::internal(format!("failed to identify staged iSCSI device {device}: {e}")))?;
    let want = context.get("iqn").map(String::as_str).unwrap_or_default();
    if actual != want {
        return Err(Status::already_exists(format!(
            "staging target is backed by iSCSI target {actual}, requested {want}"
        )));
    }
    Ok(())
}

/// That a raw-block device's target is the one named for the volume.
pub async fn validate_raw_block_ownership(state: &State, volume_id: &str, device: &str) -> Result<(), Status> {
    let (_, iqn) = state.iscsi.info_from_device_listed(device).await.map_err(|e| {
        Status::internal(format!(
            "failed to identify iSCSI session for raw block device {device}: {e}"
        ))
    })?;
    let expected = share_name(state, volume_id);
    if !session_target_matches(&iqn, &expected) {
        return Err(Status::failed_precondition(format!(
            "raw block staging device {device} belongs to iSCSI target {iqn}, expected volume target {expected}"
        )));
    }
    Ok(())
}

/// Logs out every session of the target in the list, once per portal; whether
/// at least one was found and all logged out.
async fn logout_all_listed(state: &State, iqn: &str, sessions: &[Session]) -> bool {
    let mut seen: Vec<&str> = Vec::new();
    let mut all_ok = true;
    for session in sessions.iter().filter(|s| s.iqn == iqn) {
        if seen.contains(&session.portal.as_str()) {
            continue;
        }
        seen.push(&session.portal);
        match state.iscsi.logout(&session.portal, iqn).await {
            Ok(()) => info!("Disconnected iSCSI session {iqn} through {}", session.portal),
            Err(e) => {
                all_ok = false;
                warn!(
                    "Failed to disconnect iSCSI session {iqn} through {}: {e:#}",
                    session.portal
                );
            }
        }
    }
    !seen.is_empty() && all_ok
}

/// Logs out every session of the target (a dm map has one per portal; the map
/// itself is multipathd's to remove).
pub async fn logout_all(state: &State, iqn: &str) -> bool {
    match state.iscsi.list_sessions(None).await {
        Ok(sessions) => logout_all_listed(state, iqn, &sessions).await,
        Err(e) => {
            warn!("Failed to list iSCSI sessions before logout of {iqn}: {e:#}");
            false
        }
    }
}

/// Logs out the sessions named for the volume (Go
/// cleanupOrphanedSessionByVolumeID): success when there is none and when the
/// sessions cannot be listed; an error only when a found one would not go.
pub async fn cleanup_by_volume(state: &State, volume_id: &str) -> Result<()> {
    let target = share_name(state, volume_id);
    let sessions = match state.iscsi.list_sessions(None).await {
        Ok(sessions) => sessions,
        Err(e) => {
            debug!("Cannot list active iSCSI sessions for volume {volume_id}: {e:#}");
            return Ok(());
        }
    };
    let suffix = format!(":{target}");
    let found: BTreeSet<&str> = sessions
        .iter()
        .filter(|s| s.iqn.ends_with(&suffix))
        .map(|s| s.iqn.as_str())
        .collect();
    if found.is_empty() {
        debug!("No active iSCSI session found for volume {volume_id} (target: {target})");
        return Ok(());
    }
    let mut failed = Vec::new();
    for iqn in found {
        if !logout_all_listed(state, iqn, &sessions).await {
            failed.push(format!("failed to disconnect iSCSI session {iqn}"));
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failed.join("\n")))
    }
}

/// The session cleanup of an unstage whose device is not NVMe-oF (Go
/// NodeUnstageVolume): a block link's literal device name may be stale after a
/// reboot, so a link is never trusted, the volume's target name is; a device
/// read from the live mount before the unmount is. Without any device, both
/// transports are cleaned by the volume's names.
pub async fn unstage_cleanup(
    state: &State,
    volume_id: &str,
    device: &str,
    symlink: bool,
    deadline: Option<Instant>,
) -> Result<()> {
    if device.is_empty() {
        let iscsi = cleanup_by_volume(state, volume_id).await;
        let nvme = crate::nvme_kernel::cleanup_by_volume(state, volume_id, deadline).await;
        return match (iscsi, nvme) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(a), Err(b)) => Err(anyhow!("{a:#}\n{b:#}")),
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
    }
    if symlink {
        return cleanup_by_volume(state, volume_id).await;
    }
    let primary = match state.iscsi.info_from_device_listed(device).await {
        Ok((_, iqn)) => logout_all(state, &iqn).await,
        Err(e) => {
            debug!("Could not get iSCSI info from device {device}: {e}");
            false
        }
    };
    if primary {
        return Ok(());
    }
    cleanup_by_volume(state, volume_id).await
}

/// Rescans the session behind a device, for an expansion.
pub async fn rescan_device(state: &State, device: &str, deadline: Option<Instant>) -> Result<(), Status> {
    let (portal, iqn) = state
        .iscsi
        .info_from_device_listed(device)
        .await
        .map_err(|e| Status::internal(format!("failed to identify iSCSI session for {device}: {e}")))?;
    state
        .iscsi
        .rescan(&portal, &iqn, deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to rescan iSCSI device {device}: {e:#}")))
}

/// Whether a target is one this driver would create for a Kubernetes volume
/// (Go isDriverISCSITarget): `iqn.*:<the name of a pvc-<UUID> volume>`.
pub fn is_driver_target(state: &State, iqn: &str) -> bool {
    if !iqn.to_lowercase().starts_with("iqn.") {
        return false;
    }
    let Some((_, target)) = iqn.split_once(':') else {
        return false;
    };
    let Some(volume_id) = pvc_volume_id_prefix(target) else {
        return false;
    };
    target == share_name(state, volume_id)
}

/// `^pvc-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`.
fn pvc_volume_id_prefix(target: &str) -> Option<&str> {
    let rest = target.strip_prefix("pvc-")?;
    let b = rest.as_bytes();
    let mut i = 0;
    for (n, group) in [8, 4, 4, 4, 12].iter().enumerate() {
        if n > 0 {
            if b.get(i) != Some(&b'-') {
                return None;
            }
            i += 1;
        }
        let hex = b
            .get(i..i + group)?
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c));
        if !hex {
            return None;
        }
        i += group;
    }
    Some(&target[..4 + i])
}

/// The IQNs of every staged iSCSI volume among the in-use devices, or `None`
/// when one device that may be iSCSI cannot be identified (Go
/// getExpectedISCSITargets).
pub fn expected_targets(state: &State, devices: &[String], sessions: &[Session]) -> Option<BTreeSet<String>> {
    let mut expected = BTreeSet::new();
    let mut failed = 0;
    for device in devices {
        match state.iscsi.info_from_device(device, sessions) {
            Ok((portal, iqn)) => {
                if !portal.is_empty() && !iqn.is_empty() {
                    expected.insert(iqn);
                }
            }
            Err(InfoError::NotIscsi(_)) => {}
            Err(InfoError::Unknown(e)) => {
                if !crate::iscsi::is_positively_not_iscsi_backable(device) {
                    debug!("Session GC: failed to get iSCSI info for {device} (may be race condition): {e}");
                    failed += 1;
                }
            }
        }
    }
    if failed > 0 {
        warn!("Session GC: {failed} iSCSI device lookups failed, skipping GC to avoid race condition");
        return None;
    }
    Some(expected)
}

/// Whether a GC-listed session is in scope: through the configured portal
/// (as iscsiadm prints it, with or without the tag) and to a target this driver
/// names.
pub fn gc_in_scope(state: &State, session: &Session) -> bool {
    let portal = &state.config.iscsi.target_portal;
    (session.portal == *portal || session.portal == format!("{portal},1")) && is_driver_target(state, &session.iqn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// Generated from Go normalizeISCSITargetPortal.
    #[test]
    fn target_portals_normalize_as_go_does() {
        let cases = [
            ("", None),
            (" x", None),
            ("-bad", None),
            ("192.0.2.10", Some("192.0.2.10:3260")),
            ("192.0.2.10:+80", Some("192.0.2.10:80")),
            ("192.0.2.10:0080", Some("192.0.2.10:80")),
            ("192.0.2.10:3260", Some("192.0.2.10:3260")),
            ("192.0.2.1:-1", None),
            ("192.0.2.1:3260:1", None),
            ("2001:DB8::1", Some("[2001:DB8::1]:3260")),
            ("2001:db8::1", Some("[2001:db8::1]:3260")),
            ("Host.Example", Some("host.example:3260")),
            ("[192.0.2.1]:3260", Some("192.0.2.1:3260")),
            ("[2001:DB8::1]:3260", Some("[2001:DB8::1]:3260")),
            ("[2001:db8::1]", Some("[2001:db8::1]:3260")),
            ("[2001:db8::1]:3260", Some("[2001:db8::1]:3260")),
            ("[2001:db8::1]:x", None),
            ("[host]:3260", Some("host:3260")),
            ("a..b", None),
            ("h.:1", Some("h:1")),
            ("host.example.:3260", Some("host.example:3260")),
            ("host:0", None),
            ("host:65536", None),
            ("host_name", Some("host_name:3260")),
            ("tcp://x", None),
        ];
        for (input, want) in cases {
            assert_eq!(normalize_target_portal(input).ok().as_deref(), want, "{input:?}");
        }
    }

    /// Generated from Go parseISCSIMultipathPortals.
    #[test]
    fn portal_hints_parse_as_go_does() {
        let parse = |raw: &str| parse_multipath_portals(&ctx(&[("portals", raw)]));
        let ok = |raw: &str, want: &[&str]| {
            let (portals, present, discarded) = parse(raw);
            assert!(present && discarded.is_none(), "{raw}: {discarded:?}");
            assert_eq!(portals, want, "{raw}");
        };
        ok(
            r#"["192.0.2.10:3260","192.0.2.11"]"#,
            &["192.0.2.10:3260", "192.0.2.11:3260"],
        );
        ok(r#"["192.0.2.10","192.0.2.10:3260"]"#, &["192.0.2.10:3260"]);
        ok(
            r#"["192.0.2.10:3261","192.0.2.10:3260"]"#,
            &["192.0.2.10:3261", "192.0.2.10:3260"],
        );
        ok(
            r#"["[2001:db8::1]:3260","2001:db8::2"]"#,
            &["[2001:db8::1]:3260", "[2001:db8::2]:3260"],
        );
        for bad in [
            r#"[""]"#,
            r#"["192.0.2.1", 5]"#,
            r#"["host:3260"]"#,
            "[]",
            "not json",
            "null",
        ] {
            let (portals, present, discarded) = parse(bad);
            assert!(present && discarded.is_some() && portals.is_empty(), "{bad}");
        }
        assert_eq!(parse_multipath_portals(&ctx(&[])), (Vec::new(), false, None));
    }

    #[test]
    fn chap_credentials_follow_the_go_rules() {
        let secrets = |pairs: &[(&str, &str)]| ctx(pairs);
        let chap = ctx(&[("chap", "CHAP")]);
        let mutual = ctx(&[("chap", "CHAP_MUTUAL")]);
        assert_eq!(chap_credentials(&ctx(&[]), &secrets(&[])).unwrap(), None);
        assert_eq!(
            chap_credentials(&ctx(&[("chap", "NONE")]), &secrets(&[("username", "u")])).unwrap(),
            None,
            "NONE ignores even a present secret"
        );
        let one = chap_credentials(&chap, &secrets(&[("username", "u"), ("password", "abcdefghijkl")]))
            .unwrap()
            .unwrap();
        assert_eq!((one.username.as_str(), one.mutual), ("u", false));
        // Legacy open-iscsi aliases; a blank canonical key falls through.
        let alias = chap_credentials(
            &chap,
            &secrets(&[
                ("username", "  "),
                ("node.session.auth.username", "legacy"),
                ("node.session.auth.password", "abcdefghijkl"),
            ]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(alias.username, "legacy");
        let both = chap_credentials(
            &mutual,
            &secrets(&[
                ("username", "u"),
                ("password", "abcdefghijkl"),
                ("mutualUsername", "m"),
                ("mutualPassword", "mnopqrstuvwx"),
            ]),
        )
        .unwrap()
        .unwrap();
        assert!(both.mutual && both.mutual_username == "m");
        assert!(
            !format!("{both:?}").contains("abcdefghijkl") && !format!("{both:?}").contains("mnopqrstuvwx"),
            "Debug never shows a password"
        );
        for (context, pairs, why) in [
            (&chap, vec![], "username is required"),
            (&chap, vec![("username", "u")], "password is required"),
            (
                &chap,
                vec![("username", "u"), ("password", "short")],
                "12-16 characters (got 5)",
            ),
            (
                &chap,
                vec![("username", "u"), ("password", "abcdefghijklmnopq")],
                "12-16",
            ),
            (
                &chap,
                vec![("username", "u"), ("password", " abcdefghijkl")],
                "whitespace",
            ),
            (&chap, vec![("username", "u"), ("password", "abcdefgh#jkl")], "'#'"),
            (
                &chap,
                vec![("username", "u"), ("password", "abcdefghijkl"), ("tag", "x")],
                "tag \"x\" must be a positive integer",
            ),
            (
                &chap,
                vec![("username", "u"), ("password", "abcdefghijkl"), ("tag", "0")],
                "positive integer",
            ),
            (
                &chap,
                vec![
                    ("username", "u"),
                    ("password", "abcdefghijkl"),
                    ("mutualPassword", "mnopqrstuvwx"),
                ],
                "requires mutualUsername",
            ),
            (
                &chap,
                vec![
                    ("username", "u"),
                    ("password", "abcdefghijkl"),
                    ("mutualUsername", "m"),
                    ("mutualPassword", "abcdefghijkl"),
                ],
                "must differ",
            ),
            (
                &mutual,
                vec![("username", "u"), ("password", "abcdefghijkl")],
                "CHAP_MUTUAL volume requires",
            ),
        ] {
            let err = chap_credentials(context, &secrets(&pairs)).unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{pairs:?}");
            assert!(err.message().contains(why), "{pairs:?}: {}", err.message());
            for (_, value) in &pairs {
                if value.len() >= 5 {
                    assert!(
                        !err.message().contains(value),
                        "a secret value reached the error: {}",
                        err.message()
                    );
                }
            }
        }
        // A valid explicit tag is accepted (it only matters to the controller).
        assert!(
            chap_credentials(
                &chap,
                &secrets(&[("username", "u"), ("password", "abcdefghijkl"), ("tag", " 7 ")])
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn pvc_prefixes() {
        assert_eq!(
            pvc_volume_id_prefix("pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-sfx"),
            Some("pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9")
        );
        assert_eq!(pvc_volume_id_prefix("pvc-0A1B2C3D-4e5f-6071-8293-a4b5c6d7e8f9"), None);
        assert_eq!(pvc_volume_id_prefix("pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f"), None);
        assert_eq!(pvc_volume_id_prefix("foreign"), None);
    }
}
