//! Staging and unstaging through nvmeublkd, the userspace NVMe/TCP data path
//! (`pkg/driver/node_nvmeublk.go`). A volume staged this way has no kernel NVMe
//! controller: the daemon holds the connections and its own multipath and
//! serves the namespace as /dev/ublkbN. So:
//!
//! - staging never runs nvme-cli; it asks the daemon to attach, then shares the
//!   kernel path's format/mount/symlink tail;
//! - which volume and subsystem a /dev/ublkbN serves comes from the daemon,
//!   never from sysfs;
//! - unstage detaches by volume ID, and a detach failure is never skipped;
//! - a marker per volume (written before the attach) tells unstage to detach
//!   even when the staging path no longer names the device.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use tonic::Status;

use crate::capability::{protocol_share_name, session_target_matches};
use crate::config::DataPath;
use crate::csi;
use crate::events::{self, ObjectRef};
use crate::locks::go_clean;
use crate::nvme_addresses::ublk_portals;
use crate::service::State;
use crate::stage::{finalize_staged_device, staged_block_device_path};
use crate::ublk_client::{self, AttachRequest, Device, is_ublk_device};
use crate::ublk_state;

/// Bounds list and detach, which tear down at most one device; attach has its
/// own configurable budget.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// `scale_csi_node_connect_total`'s transport label for this path, distinct
/// from the kernel initiator's "nvmeof".
pub const TRANSPORT_LABEL: &str = "nvmeof-ublk";

/// The NVMe-oF volume context key that pins a volume's data path.
pub const DATA_PATH_KEY: &str = "nvmeof/dataPath";

/// The deadline of a call that may take `budget`, within the RPC's own.
pub fn bounded(rpc_deadline: Option<Instant>, budget: Duration) -> Instant {
    let own = Instant::now() + budget;
    rpc_deadline.map_or(own, |d| d.min(own))
}

fn daemon_status(e: &ublk_client::Error, message: String) -> Status {
    Status::new(e.code(), message)
}

/// The data path a volume stages through: its own pinned choice, else the
/// install's default. A malformed pin is an error, never a fallback.
pub fn data_path_for_volume(state: &State, context: &HashMap<String, String>) -> Result<DataPath, Status> {
    let Some(raw) = context.get(DATA_PATH_KEY) else {
        return Ok(state.config.nvmeof.default_data_path());
    };
    match raw.trim().to_lowercase().as_str() {
        "kernel" => Ok(DataPath::Kernel),
        "ublk" => Ok(DataPath::Ublk),
        _ => Err(Status::invalid_argument(format!(
            "volume context {DATA_PATH_KEY} must be \"kernel\" or \"ublk\", got {raw:?}"
        ))),
    }
}

fn socket(state: &State) -> &Path {
    Path::new(&state.config.nvmeof.ublk.socket_path)
}

/// The device the daemon serves at `device_path`.
pub async fn device_at(
    state: &State,
    device_path: &str,
    deadline: Option<Instant>,
) -> Result<Device, ublk_client::Error> {
    let devices = state.ublk.list(bounded(deadline, CONTROL_TIMEOUT)).await?;
    let want = go_clean(device_path);
    devices
        .into_iter()
        .find(|d| go_clean(&d.path) == want)
        .ok_or_else(|| ublk_client::Error::Other(format!("nvmeublkd serves no device at {device_path}")))
}

/// That a staged /dev/ublkbN serves this volume's subsystem, for this volume.
pub async fn verify_stage_source(
    state: &State,
    volume_id: &str,
    device_path: &str,
    context: &HashMap<String, String>,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let device = device_at(state, device_path, deadline).await.map_err(|e| {
        daemon_status(
            &e,
            format!("failed to identify staged NVMe-oF device {device_path} through nvmeublkd: {e}"),
        )
    })?;
    let nqn = context.get("nqn").map(String::as_str).unwrap_or_default();
    if device.subnqn != nqn {
        return Err(Status::already_exists(format!(
            "staging target is backed by NVMe-oF subsystem {}, requested {nqn}",
            device.subnqn
        )));
    }
    if device.volume != volume_id {
        return Err(Status::already_exists(format!(
            "staging target is backed by nvmeublkd volume {}, requested {volume_id}",
            device.volume
        )));
    }
    Ok(())
}

/// That a raw-block /dev/ublkbN belongs to this volume: the subsystem named
/// for it, and the daemon's own volume key.
pub async fn validate_raw_block_ownership(
    state: &State,
    volume_id: &str,
    device_path: &str,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let device = device_at(state, device_path, deadline).await.map_err(|e| {
        daemon_status(
            &e,
            format!("failed to identify nvmeublkd device for raw block device {device_path}: {e}"),
        )
    })?;
    let n = &state.config.nvmeof;
    let expected = format!("{}{}{}", n.name_prefix, protocol_share_name(volume_id), n.name_suffix);
    if !session_target_matches(&device.subnqn, &expected) {
        return Err(Status::failed_precondition(format!(
            "raw block staging device {device_path} belongs to NVMe-oF subsystem {}, expected volume subsystem {expected}",
            device.subnqn
        )));
    }
    if device.volume != volume_id {
        return Err(Status::failed_precondition(format!(
            "raw block staging device {device_path} is served for nvmeublkd volume {}, expected {volume_id}",
            device.volume
        )));
    }
    Ok(())
}

/// Refuses a device path that is not exactly the ublk block device the daemon
/// says it created: it is about to be formatted, mounted or handed to a pod.
pub fn validate_attached_device(dev_dir: &Path, device: &Device) -> Result<(), String> {
    let want = dev_dir.join(format!("ublkb{}", device.dev_id));
    if device.dev_id < 0 || Path::new(&device.path) != want {
        return Err(format!(
            "nvmeublkd returned device {:?} for dev_id {}, want {}",
            device.path,
            device.dev_id,
            want.display()
        ));
    }
    if let Err(e) = std::fs::metadata(&device.path) {
        return Err(format!(
            "nvmeublkd reported {} but it is not present on this node: {e}",
            device.path
        ));
    }
    Ok(())
}

pub struct StageRequest<'a> {
    pub volume_id: &'a str,
    /// The volume context with the publish context's path hint applied.
    pub context: &'a HashMap<String, String>,
    pub staging: &'a str,
    pub capability: &'a csi::VolumeCapability,
    pub event: Option<&'a ObjectRef>,
    pub deadline: Option<Instant>,
}

fn warn_event(state: &State, event: Option<&ObjectRef>, reason: &str, message: &str) {
    if let Some(object) = event {
        state.events.warning(object, reason, message);
    }
}

/// Stages a volume through nvmeublkd. The daemon's attach is idempotent per
/// volume, so a retry after any partial failure gets the same device back.
pub async fn stage(state: &State, req: StageRequest<'_>) -> Result<(), Status> {
    let get = |k: &str| req.context.get(k).map(String::as_str).unwrap_or_default();
    let (nqn, address) = (get("nqn"), get("address"));
    if nqn.is_empty() || address.is_empty() {
        return Err(Status::invalid_argument(
            "NVMe-oF NQN and address are required in volume context",
        ));
    }
    if req.volume_id.is_empty() {
        return Err(Status::invalid_argument(
            "volume ID is required for the ublk NVMe-oF data path",
        ));
    }
    let transport = if get("transport").is_empty() {
        "tcp"
    } else {
        get("transport")
    };
    if !transport.eq_ignore_ascii_case("tcp") {
        return Err(Status::invalid_argument(format!(
            "the ublk NVMe-oF data path supports only the tcp transport, volume uses {transport}"
        )));
    }
    let port = if get("port").is_empty() { "4420" } else { get("port") };
    if !state.config.nvmeof.ublk_available() {
        return Err(Status::failed_precondition(
            "volume selects the ublk NVMe-oF data path, which is not enabled on this node (nvmeof.ublk.enabled)",
        ));
    }
    let (addrs, discarded) = ublk_portals(req.context, address, port);
    if let Some(reason) = discarded {
        let message = format!(
            "NVMe-oF multipath address list for {nqn} was discarded; using single-address fallback; published addresses={}: {reason}",
            get("addresses")
        );
        warn!("{message}");
        state
            .metrics
            .record_nvme_path_connect("invalid-publish-context", "error");
        warn_event(state, req.event, events::REASON_NVME_PATH_DEGRADED, &message);
    }

    // Idempotent replays: a mounted staging path was identified through the
    // daemon by the caller's existing-stage check; a block link is checked
    // here, and one that does not verify falls through to an attach that
    // returns this volume's device and an atomic replacement of the link.
    let mounted = state
        .mounter
        .is_mounted(req.staging, req.deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    if mounted {
        info!(
            "NVMe-oF volume {} already mounted at {} (ublk data path)",
            req.volume_id, req.staging
        );
        return Ok(());
    }
    if matches!(
        req.capability.access_type,
        Some(csi::volume_capability::AccessType::Block(_))
    ) && let Some(device) = staged_block_device_path(req.staging, &state.host.dev_dir)
        && is_ublk_device(&device)
    {
        match verify_stage_source(state, req.volume_id, &device, req.context, req.deadline).await {
            Ok(()) => {
                info!(
                    "NVMe-oF block volume {} already staged at {} (ublk data path)",
                    req.volume_id, req.staging
                );
                return Ok(());
            }
            Err(e) => info!(
                "Re-attaching NVMe-oF block volume {}: staged device {device} did not verify: {}",
                req.volume_id,
                e.message()
            ),
        }
    }

    let (host_nqn, host_id) = match ublk_state::host_identity(&state.node_id, &state.host.host_id_files) {
        Ok(identity) => identity,
        Err(e) => {
            let status = Status::failed_precondition(format!(
                "cannot stage NVMe-oF volume {} through nvmeublkd: {e:#}",
                req.volume_id
            ));
            warn_event(state, req.event, events::REASON_NVME_CONNECT_FAILED, status.message());
            return Err(status);
        }
    };
    // The marker goes down BEFORE the attach: an attach that times out may
    // still complete in the daemon, and unstage must then know to detach.
    ublk_state::write_marker(socket(state), &state.driver_name, req.volume_id).map_err(|e| {
        Status::internal(format!(
            "failed to record the ublk data path for volume {}: {e:#}",
            req.volume_id
        ))
    })?;

    let ublk = &state.config.nvmeof.ublk;
    let attach = AttachRequest {
        volume: req.volume_id.to_string(),
        subnqn: nqn.to_string(),
        addrs: addrs.clone(),
        hostnqn: host_nqn,
        hostid: host_id,
        queues: ublk.queues,
        depth: ublk.depth,
        zero_copy: ublk.zero_copy,
        napi_us: ublk.napi_us,
    };
    let attached = state
        .ublk
        .attach(&attach, bounded(req.deadline, Duration::from_secs(ublk.attach_timeout)))
        .await
        .and_then(|device| {
            validate_attached_device(&state.host.dev_dir, &device)
                .map(|()| device)
                .map_err(ublk_client::Error::Other)
        });
    let device = match attached {
        Ok(device) => device,
        Err(e) => {
            state.metrics.record_node_connect(TRANSPORT_LABEL, "error");
            let status = daemon_status(
                &e,
                format!(
                    "failed to attach NVMe-oF volume {} through nvmeublkd: {e}",
                    req.volume_id
                ),
            );
            warn_event(state, req.event, events::REASON_NVME_CONNECT_FAILED, status.message());
            return Err(status);
        }
    };
    state.metrics.record_node_connect(TRANSPORT_LABEL, "success");
    info!(
        "nvmeublkd attached NVMe-oF volume {} ({nqn}) at {} (existing={}, paths={addrs:?})",
        req.volume_id, device.path, device.existing
    );
    record_path_health(state, req.volume_id, nqn, req.event, req.deadline).await;

    finalize_staged_device(&state.mounter, &device.path, req.staging, req.capability, req.deadline).await
}

/// Paths the daemon reports down right after an attach become the kernel
/// path's degraded-path event. Best effort: the volume works while any path is
/// up.
async fn record_path_health(
    state: &State,
    volume_id: &str,
    nqn: &str,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let devices = match state.ublk.list(bounded(deadline, CONTROL_TIMEOUT)).await {
        Ok(devices) => devices,
        Err(e) => {
            debug!("nvmeublkd path health for {volume_id} unavailable: {e}");
            return;
        }
    };
    let Some(device) = devices.into_iter().find(|d| d.volume == volume_id) else {
        return;
    };
    let down: Vec<String> = device
        .paths
        .iter()
        .filter(|p| !p.up)
        .map(|p| format!("{}: path is down in nvmeublkd", p.addr))
        .collect();
    if !down.is_empty() {
        let message = format!("NVMe-oF path convergence for {nqn} is degraded: {}", down.join("\n"));
        warn_event(state, event, events::REASON_NVME_PATH_DEGRADED, &message);
    }
}

/// What unstage found out about a volume's userspace data path.
#[derive(Debug, PartialEq, Eq)]
pub enum Unstaged {
    /// No ublk evidence: the kernel cleanup is all there is.
    NotUblk,
    /// Detached from the daemon; nothing else to do.
    Done,
    /// Detached a stale attachment beside a live kernel device, whose session
    /// still needs the kernel cleanup.
    DetachedKernelRemains,
}

/// Detaches a volume from nvmeublkd when anything shows it used the userspace
/// data path: the staged device itself, or its marker. A detach failure is
/// never skipped: the daemon would keep serving the volume.
pub async fn unstage(
    state: &State,
    volume_id: &str,
    device_path: &str,
    deadline: Option<Instant>,
) -> Result<Unstaged, Status> {
    let ublk_device = is_ublk_device(device_path);
    // The marker directory exists only where the ublk path is enabled; a
    // kernel-only node never looks for it.
    let marked = if state.config.nvmeof.ublk_available() {
        ublk_state::marker_exists(socket(state), &state.driver_name, volume_id).map_err(|e| {
            Status::internal(format!(
                "cannot tell whether volume {volume_id} used the ublk data path: {e:#}"
            ))
        })?
    } else {
        false
    };
    if !ublk_device && !marked {
        return Ok(Unstaged::NotUblk);
    }

    let absent = state
        .ublk
        .detach(volume_id, bounded(deadline, CONTROL_TIMEOUT))
        .await
        .map_err(|e| daemon_status(&e, format!("failed to detach volume {volume_id} from nvmeublkd: {e}")))?;
    if absent {
        debug!("nvmeublkd had no attachment for volume {volume_id}");
    } else {
        info!("Detached NVMe-oF volume {volume_id} from nvmeublkd");
    }
    ublk_state::remove_marker(socket(state), &state.driver_name, volume_id)
        .map_err(|e| Status::internal(format!("detached volume {volume_id} from nvmeublkd but {e:#}")))?;
    if !ublk_device && !device_path.is_empty() {
        return Ok(Unstaged::DetachedKernelRemains);
    }
    Ok(Unstaged::Done)
}
