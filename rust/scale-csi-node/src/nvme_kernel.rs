//! Staging NVMe-oF through the kernel initiator (`pkg/driver/node.go`:
//! stageNVMeoFVolume, convergeNVMeoFPaths, convergeExistingNVMeoFPaths).
//!
//! - The session is recorded as this plugin's before it is connected, so GC
//!   can collect it if it is ever orphaned (a failed record only means it never
//!   will be).
//! - Single path (no usable multipath hint): any existing controller for the
//!   NQN is disconnected first unless the staged device is still live, and a
//!   connect is skipped when any subsystem entry for the NQN exists.
//! - Multipath: a subsystem with controllers but no live path is disconnected
//!   first; then every requested address is converged. Already-live addresses
//!   share one device-wait budget, the first missing one gets the full budget,
//!   the rest share 5 s. Success needs one connected path and a device; a path
//!   that failed is an `NVMePathDegraded` event, not a failure.
//! - After the stage, multipath subsystems get the `queue-depth` iopolicy, and
//!   paths split across subsystem directories (nvme_core.multipath=N) are an
//!   `NVMeMultipathUnaggregated` event.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use log::{debug, info, warn};
use tonic::Status;

use crate::csi;
use crate::events::{self, ObjectRef};
use crate::nvme::{self, ConnectOptions, Subsystem, Target};
use crate::nvme_addresses::parse_multipath_addresses;
use crate::service::State;
use crate::stage::{finalize_staged_device, staged_block_device_path};
use crate::ublk_stage::{DATA_PATH_KEY, StageRequest, bounded};

pub const TRANSPORT_LABEL: &str = "nvmeof";
pub const REASON_MULTIPATH_UNAGGREGATED: &str = "NVMeMultipathUnaggregated";

/// Secondary paths share this budget after the first missing path.
pub const SECONDARY_PATH_BUDGET: Duration = Duration::from_secs(5);

/// `nvme connect`'s knobs from the config (Go nvmeConnectOptions):
/// `fast_io_fail_tmo` 0 means 15 s with multipath configured and omitted
/// without (failing I/O fast on a single path turns a survivable NAS reboot
/// into an outage); negative omits it.
pub fn connect_options(state: &State) -> ConnectOptions {
    let n = &state.config.nvmeof;
    let c = &n.connect;
    let fast_io_fail_tmo = match c.fast_io_fail_tmo {
        0 if n.multipath_addresses().is_empty() => None,
        0 => Some(15),
        v if v < 0 => None,
        v => Some(v as u64),
    };
    let positive = |v: Option<i64>| v.and_then(|v| u64::try_from(v).ok());
    ConnectOptions {
        fast_io_fail_tmo,
        nr_io_queues: positive(c.nr_io_queues).map(|v| v as u32),
        nr_write_queues: positive(c.nr_write_queues).map(|v| v as u32),
        keep_alive_tmo: positive(c.keep_alive_tmo),
    }
}

fn warn_event(state: &State, event: Option<&ObjectRef>, reason: &str, message: &str) {
    if let Some(object) = event {
        state.events.warning(object, reason, message);
    }
}

/// The volume's multipath addresses from the publish hint; a malformed hint is
/// discarded (event and metric) for the single-address fallback.
pub fn multipath_addresses(
    state: &State,
    context: &HashMap<String, String>,
    nqn: &str,
    event: Option<&ObjectRef>,
) -> Vec<String> {
    match parse_multipath_addresses(context) {
        Ok(addresses) => addresses.unwrap_or_default(),
        Err(reason) => {
            let message = format!(
                "NVMe-oF multipath address list for {nqn} was discarded; using single-address fallback; published addresses={}: {reason}",
                context.get("addresses").map(String::as_str).unwrap_or_default()
            );
            warn!("{message}");
            state
                .metrics
                .record_nvme_path_connect("invalid-publish-context", "error");
            warn_event(state, event, events::REASON_NVME_PATH_DEGRADED, &message);
            Vec::new()
        }
    }
}

async fn list(state: &State, deadline: Option<Instant>, why: &str) -> Vec<Subsystem> {
    match state.nvme.list_subsystems(deadline).await {
        Ok(subsystems) => subsystems,
        Err(e) => {
            warn!("Failed to list NVMe subsystems {why}: {e:#}");
            Vec::new()
        }
    }
}

/// The staged device, if it is still live: a block link, or a mounted device.
async fn staged_device(state: &State, staging: &str, deadline: Option<Instant>) -> Option<String> {
    if let Some(device) = staged_block_device_path(staging, &state.host.dev_dir) {
        return Some(device);
    }
    let source = state.mounter.mount_source(staging, deadline).await.ok()?;
    let path = std::path::Path::new(&source);
    (path.starts_with(&state.host.dev_dir) && path.exists()).then_some(source)
}

/// Disconnects an existing controller for the NQN before a connect, unless the
/// staged device is still live, then waits (up to `node.sessionCleanupDelay`)
/// for it to clear. Returns the latest subsystem list.
pub(crate) async fn preemptive_disconnect(
    state: &State,
    nqn: &str,
    staging: &str,
    subsystems: Vec<Subsystem>,
    deadline: Option<Instant>,
) -> Vec<Subsystem> {
    if !nvme::has_subsystem(nqn, &subsystems) {
        return subsystems;
    }
    if let Some(live) = staged_device(state, staging, deadline).await {
        info!("Skipping pre-emptive NVMe-oF disconnect for {nqn}: staged device {live} is still live");
        return subsystems;
    }
    info!("Found existing NVMe-oF session for {nqn}, disconnecting before reconnect");
    if let Err(e) = state.nvme.disconnect(nqn, deadline).await {
        warn!("Failed to disconnect existing session {nqn}: {e:#} (proceeding anyway)");
    }
    let wait = state.config.node.session_cleanup_delay();
    let until = Instant::now() + wait;
    let mut latest = subsystems;
    loop {
        match state.nvme.list_subsystems(deadline).await {
            Ok(fresh) => {
                latest = fresh;
                if !nvme::has_subsystem(nqn, &latest) {
                    return latest;
                }
            }
            Err(e) => debug!("NVMe-oF session cleanup poll for {nqn}: {e:#}"),
        }
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() || deadline.is_some_and(|d| Instant::now() >= d) {
            debug!("NVMe-oF session cleanup poll for {nqn} ended with the session still present");
            return latest;
        }
        tokio::time::sleep(left.min(Duration::from_millis(100))).await;
    }
}

/// One path: connect it unless that address is already live, then find the
/// subsystem's device within `budget`.
async fn connect_path(
    state: &State,
    target: &Target<'_>,
    options: &ConnectOptions,
    subsystems: &[Subsystem],
    budget: Duration,
    deadline: Option<Instant>,
) -> Result<String> {
    let deadline = Some(bounded(deadline, budget + state.nvme.timeout));
    let was_connected = nvme::has_subsystem(target.nqn, subsystems);
    if !nvme::live_addresses(target.nqn, subsystems)
        .iter()
        .any(|a| a == target.host)
    {
        state
            .nvme
            .connect(target, options, state.nvme.timeout, deadline)
            .await
            .map_err(|e| anyhow!("connect failed: {e:#}"))?;
    } else {
        debug!("Already connected to subsystem {} at {}", target.nqn, target.host);
    }
    state
        .nvme
        .wait_for_device(target.nqn, budget, subsystems.to_vec(), !was_connected, deadline)
        .await
        .map_err(|e| anyhow!("device not found: {e:#}"))
}

/// The single-path connect: skipped when any controller for the NQN exists.
async fn connect_single(
    state: &State,
    target: &Target<'_>,
    options: &ConnectOptions,
    subsystems: &[Subsystem],
    budget: Duration,
    deadline: Option<Instant>,
) -> Result<String> {
    let deadline = Some(bounded(deadline, budget + state.nvme.timeout));
    let was_connected = nvme::has_subsystem(target.nqn, subsystems);
    if !was_connected {
        state
            .nvme
            .connect(target, options, state.nvme.timeout, deadline)
            .await
            .map_err(|e| anyhow!("connect failed: {e:#}"))?;
    }
    state
        .nvme
        .wait_for_device(target.nqn, budget, subsystems.to_vec(), !was_connected, deadline)
        .await
        .map_err(|e| anyhow!("device not found: {e:#}"))
}

pub struct Converged {
    pub device: Option<String>,
    /// Per-address failures, for the degraded-path event.
    pub failures: Vec<String>,
}

/// Brings every requested address up (Go convergeNVMeoFPaths). Failure
/// returns the message and the per-address failures.
#[allow(clippy::too_many_arguments)]
pub async fn converge_paths(
    state: &State,
    nqn: &str,
    transport: &str,
    port: &str,
    addresses: &[String],
    subsystems: &[Subsystem],
    need_device: bool,
    deadline: Option<Instant>,
) -> Result<Converged, (String, Vec<String>)> {
    let options = connect_options(state);
    let budget = state.config.nvmeof.device_wait_timeout();
    let live = nvme::live_addresses(nqn, subsystems);
    let mut requested_live: Vec<&str> = Vec::new();
    let mut missing: Vec<&str> = Vec::new();
    for address in addresses {
        if live.contains(address) {
            state.metrics.record_nvme_path_connect(address, "already_live");
            requested_live.push(address);
        } else {
            missing.push(address);
        }
    }
    let mut connected = !requested_live.is_empty();
    let mut device: Option<String> = None;
    let mut failures: Vec<String> = Vec::new();
    let target = |host| Target {
        transport,
        host,
        port,
        nqn,
    };

    // Live controllers: no connect, the device within one shared budget.
    if need_device && !requested_live.is_empty() {
        let shared = bounded(deadline, budget);
        for address in &requested_live {
            let left = shared.saturating_duration_since(Instant::now());
            match connect_path(state, &target(address), &options, subsystems, left, Some(shared)).await {
                Ok(path) => {
                    device = Some(path);
                    break;
                }
                Err(e) => failures.push(format!("{address}: {e:#}")),
            }
        }
    }
    // The first missing path gets the full budget; the rest share 5 s.
    let mut rest = missing.as_slice();
    if need_device
        && device.is_none()
        && let Some((first, tail)) = rest.split_first()
    {
        let result = connect_path(state, &target(first), &options, subsystems, budget, deadline).await;
        record_outcome(state, first, result, &mut connected, &mut device, &mut failures);
        rest = tail;
    }
    if !rest.is_empty() {
        let short = bounded(deadline, SECONDARY_PATH_BUDGET);
        for address in rest {
            let left = short
                .saturating_duration_since(Instant::now())
                .min(SECONDARY_PATH_BUDGET.min(budget));
            let result = connect_path(state, &target(address), &options, subsystems, left, Some(short)).await;
            record_outcome(state, address, result, &mut connected, &mut device, &mut failures);
        }
    }

    if !connected {
        return Err((
            format!("no requested NVMe-oF path connected for {nqn}: {}", failures.join("\n")),
            failures,
        ));
    }
    if need_device && device.is_none() {
        return Err((
            format!(
                "no device path available from a requested NVMe-oF path for {nqn}: {}",
                failures.join("\n")
            ),
            failures,
        ));
    }
    Ok(Converged { device, failures })
}

fn record_outcome(
    state: &State,
    address: &str,
    result: Result<String>,
    connected: &mut bool,
    device: &mut Option<String>,
    failures: &mut Vec<String>,
) {
    match result {
        Ok(path) => {
            state.metrics.record_nvme_path_connect(address, "success");
            *connected = true;
            if device.is_none() {
                *device = Some(path);
            }
        }
        Err(e) => {
            warn!("Failed to connect NVMe-oF path {address}: {e:#}");
            state.metrics.record_nvme_path_connect(address, "error");
            failures.push(format!("{address}: {e:#}"));
        }
    }
}

fn record_path_failures(state: &State, event: Option<&ObjectRef>, nqn: &str, failures: &[String]) {
    if failures.is_empty() {
        return;
    }
    let message = format!(
        "NVMe-oF path convergence for {nqn} is degraded: {}",
        failures.join("\n")
    );
    warn_event(state, event, events::REASON_NVME_PATH_DEGRADED, &message);
}

/// `queue-depth` on every subsystem entry of the NQN (Go
/// setNVMeoFQueueDepthPolicy), then the split-multipath check.
async fn tune_multipath(
    state: &State,
    nqn: &str,
    subsystems: Vec<Subsystem>,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let named = |s: &[Subsystem]| s.iter().any(|s| s.nqn == nqn && !s.name.is_empty());
    let subsystems = if named(&subsystems) {
        subsystems
    } else {
        match state.nvme.list_subsystems(deadline).await {
            Ok(fresh) => fresh,
            Err(e) => {
                debug!("NVMe-oF queue-depth iopolicy listing unavailable for {nqn}: {e:#}");
                subsystems
            }
        }
    };
    let mut stamped = 0;
    for subsystem in subsystems.iter().filter(|s| s.nqn == nqn && !s.name.is_empty()) {
        match state.nvme.set_iopolicy(&subsystem.name, "queue-depth") {
            Ok(()) => stamped += 1,
            Err(e) => debug!(
                "NVMe-oF queue-depth iopolicy unsupported for {nqn} ({}): {e:#}",
                subsystem.name
            ),
        }
    }
    if stamped > 1 {
        info!(
            "NVMe-oF queue-depth iopolicy stamped on {stamped} subsystem directories for {nqn}; enable nvme_core.multipath=Y so the kernel aggregates all paths into one subsystem"
        );
    }
    match state.nvme.subsystem_dirs(nqn) {
        Ok(count) if count > 1 => {
            let message = format!(
                "NVMe-oF multipath for {nqn} is split across {count} subsystem directories; enable nvme_core.multipath=Y so the kernel aggregates the paths into one subsystem"
            );
            warn!("{message}");
            warn_event(state, event, REASON_MULTIPATH_UNAGGREGATED, &message);
        }
        Ok(_) => {}
        Err(e) => debug!("NVMe-oF multipath aggregation check unavailable for {nqn}: {e:#}"),
    }
}

fn transport_and_port(context: &HashMap<String, String>) -> (String, String) {
    let get = |k: &str| context.get(k).map(String::as_str).unwrap_or_default();
    let transport = if get("transport").is_empty() {
        "tcp"
    } else {
        get("transport")
    };
    let port = if get("port").is_empty() { "4420" } else { get("port") };
    (transport.to_string(), port.to_string())
}

/// Tops up the paths of a volume that is already staged (a replay): bounded,
/// best effort; the stage stays good whatever happens here.
pub async fn converge_existing(
    state: &State,
    context: &HashMap<String, String>,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let nqn = context.get("nqn").map(String::as_str).unwrap_or_default();
    let addresses = multipath_addresses(state, context, nqn, event);
    if addresses.is_empty() {
        return;
    }
    let (transport, port) = transport_and_port(context);
    let subsystems = list(state, deadline, &format!("before converging staged volume {nqn}")).await;
    let failures = match converge_paths(state, nqn, &transport, &port, &addresses, &subsystems, false, deadline).await {
        Ok(converged) => converged.failures,
        Err((_, failures)) => failures,
    };
    record_path_failures(state, event, nqn, &failures);
    tune_multipath(state, nqn, subsystems, event, deadline).await;
}

/// Stages a volume through the kernel initiator.
pub async fn stage(state: &State, req: StageRequest<'_>) -> Result<(), Status> {
    let get = |k: &str| req.context.get(k).map(String::as_str).unwrap_or_default();
    if get(DATA_PATH_KEY).trim().eq_ignore_ascii_case("ublk") {
        return Err(Status::internal(
            "NVMe-oF volume selects the ublk data path; refusing to connect it with the kernel initiator",
        ));
    }
    let (nqn, address) = (get("nqn"), get("address"));
    if nqn.is_empty() || address.is_empty() {
        return Err(Status::invalid_argument(
            "NVMe-oF NQN and address are required in volume context",
        ));
    }
    // Recorded BEFORE the connect: a crash in between would otherwise leave a
    // session GC could never collect.
    match &state.nvme_sessions {
        Some(registry) => {
            if let Err(e) = registry.record(nqn) {
                warn!("NVMe-oF stage {nqn}: {e:#}; session GC will not collect this session");
            }
        }
        None => warn!("NVMe-oF stage {nqn}: no session registry; session GC will not collect this session"),
    }
    let (transport, port) = transport_and_port(req.context);
    let addresses = multipath_addresses(state, req.context, nqn, req.event);

    let mounted = state
        .mounter
        .is_mounted(req.staging, req.deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    if mounted {
        if !addresses.is_empty() {
            converge_existing(state, req.context, req.event, req.deadline).await;
        }
        info!("NVMe-oF volume already mounted at {}", req.staging);
        return Ok(());
    }
    let mut subsystems = list(state, req.deadline, &format!("before staging {nqn}")).await;
    if matches!(
        req.capability.access_type,
        Some(csi::volume_capability::AccessType::Block(_))
    ) && let Some(device) = staged_block_device_path(req.staging, &state.host.dev_dir)
        && state.nvme.nqn_of_device(&device).is_ok_and(|found| found == nqn)
        && nvme::has_subsystem(nqn, &subsystems)
    {
        if !addresses.is_empty() {
            converge_existing(state, req.context, req.event, req.deadline).await;
        }
        info!("NVMe-oF block volume already staged at {}", req.staging);
        return Ok(());
    }

    let budget = state.config.nvmeof.device_wait_timeout();
    let result: Result<(String, Vec<String>), String> = if !addresses.is_empty() {
        if nvme::has_subsystem(nqn, &subsystems) && nvme::live_addresses(nqn, &subsystems).is_empty() {
            subsystems = preemptive_disconnect(state, nqn, req.staging, subsystems, req.deadline).await;
        }
        match converge_paths(
            state,
            nqn,
            &transport,
            &port,
            &addresses,
            &subsystems,
            true,
            req.deadline,
        )
        .await
        {
            Ok(converged) => Ok((converged.device.unwrap_or_default(), converged.failures)),
            Err((message, _)) => Err(message),
        }
    } else {
        subsystems = preemptive_disconnect(state, nqn, req.staging, subsystems, req.deadline).await;
        let host = address
            .strip_prefix('[')
            .and_then(|a| a.strip_suffix(']'))
            .unwrap_or(address);
        let target = Target {
            transport: &transport,
            host,
            port: &port,
            nqn,
        };
        connect_single(
            state,
            &target,
            &connect_options(state),
            &subsystems,
            budget,
            req.deadline,
        )
        .await
        .map(|device| (device, Vec::new()))
        .map_err(|e| format!("{e:#}"))
    };
    let (device, failures) = match result {
        Ok(found) => found,
        Err(message) => {
            state.metrics.record_node_connect(TRANSPORT_LABEL, "error");
            let status = Status::internal(format!("failed to connect NVMe-oF: {message}"));
            warn_event(state, req.event, events::REASON_NVME_CONNECT_FAILED, status.message());
            return Err(status);
        }
    };
    state.metrics.record_node_connect(TRANSPORT_LABEL, "success");
    record_path_failures(state, req.event, nqn, &failures);

    finalize_staged_device(&state.mounter, &device, req.staging, req.capability, req.deadline).await?;
    if !addresses.is_empty() {
        tune_multipath(state, nqn, subsystems, req.event, req.deadline).await;
    }
    Ok(())
}

/// That a kernel NVMe device is the subsystem the request names.
pub fn verify_stage_source(state: &State, device: &str, context: &HashMap<String, String>) -> Result<(), Status> {
    let nqn = state
        .nvme
        .nqn_of_device(device)
        .map_err(|e| Status::internal(format!("failed to identify staged NVMe-oF device {device}: {e:#}")))?;
    let want = context.get("nqn").map(String::as_str).unwrap_or_default();
    if nqn != want {
        return Err(Status::already_exists(format!(
            "staging target is backed by NVMe-oF subsystem {nqn}, requested {want}"
        )));
    }
    Ok(())
}
