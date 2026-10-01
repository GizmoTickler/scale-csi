//! NodePublishVolume and NodeUnpublishVolume (`pkg/driver/node.go`): a bind
//! mount from the staging path (a filesystem volume) or of the staged device
//! (a raw-block volume) onto the pod's target.
//!
//! A repeated publish with the same volume, source, capability and read-only
//! state succeeds without another mount; a different one at the same target is
//! AlreadyExists. A single-writer volume is refused at a second target while
//! the first is still mounted (the previous pod may still be tearing down).

use std::os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt};
use std::path::Path;
use std::time::Instant;

use log::{debug, info, warn};
use tonic::Status;

use crate::capability::{self, AccessType, ShareType, Signature, mount_sources_equal, normalize_mount_source};
use crate::csi;
use crate::csi::volume_capability::access_mode::Mode;
use crate::events::node_volume_ref;
use crate::locks::{go_clean, node_target_key, node_volume_key};
use crate::records::MountRecord;
use crate::service::State;
use crate::stage::remove_all;
use crate::ublk_client::is_ublk_device;
use crate::ublk_stage;

pub const REASON_MOUNT_FAILED: &str = "MountFailed";

/// What is at a publish target: a directory is a filesystem publication; a
/// regular file (the placeholder) or a block device (the bind-mounted device
/// node seen through the mount) is a raw-block one. The Go node accepts only
/// the regular file, so its raw-block replay of a live publication fails.
pub(crate) fn access_type_at_path(path: &str) -> Result<AccessType, String> {
    let meta = std::fs::metadata(path).map_err(|e| e.to_string())?;
    let kind = meta.file_type();
    if kind.is_dir() {
        Ok(AccessType::Mount)
    } else if kind.is_file() || kind.is_block_device() {
        Ok(AccessType::Block)
    } else {
        Err(format!("path {path} has unsupported type {kind:?}"))
    }
}

fn allows_multiple_targets(mode: i32) -> bool {
    [
        Mode::SingleNodeMultiWriter,
        Mode::MultiNodeReaderOnly,
        Mode::MultiNodeSingleWriter,
        Mode::MultiNodeMultiWriter,
    ]
    .iter()
    .any(|m| *m as i32 == mode)
}

fn likely_csi_publication_target(target: &str) -> bool {
    let target = go_clean(target);
    target.contains("/pods/") || target.contains("/volumeDevices/publish/")
}

/// The source a publication must show: the staged device (raw block), the
/// staging mount's source (filesystem). Without a staging path only a legacy
/// NFS direct mount publishes, which this agent does not serve.
async fn expected_source(
    state: &State,
    req: &csi::NodePublishVolumeRequest,
    capability: &Signature,
    deadline: Option<Instant>,
) -> Result<String, Status> {
    let staging = req.staging_target_path.as_str();
    if capability.access_type == AccessType::Block {
        if staging.is_empty() {
            return Err(Status::failed_precondition(
                "staging path is required for block volumes",
            ));
        }
        let device = std::fs::canonicalize(staging)
            .map_err(|e| Status::failed_precondition(format!("failed to resolve staged block device: {e}")))?
            .to_string_lossy()
            .into_owned();
        if !device.starts_with(&format!("{}/", state.host.dev_dir.display())) {
            return Err(Status::failed_precondition(format!(
                "staging path did not resolve to a block device: {device}"
            )));
        }
        return Ok(normalize_mount_source(&device));
    }
    if !staging.is_empty() {
        let info = state.mounter.mount_info(staging, deadline).await.map_err(|e| {
            Status::failed_precondition(format!("staging target {staging} is not a readable mount: {e:#}"))
        })?;
        return Ok(normalize_mount_source(&info.source));
    }
    match capability::attach_driver(&req.volume_context, &state.driver_name) {
        ShareType::Nfs => Err(Status::failed_precondition(
            "an NFS volume published without a staging path, which the Rust node agent does not serve yet",
        )),
        _ => Err(Status::failed_precondition("staging path required for block volumes")),
    }
}

async fn validate_existing(
    state: &State,
    req: &csi::NodePublishVolumeRequest,
    capability: &Signature,
    expected: &str,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let target = req.target_path.as_str();
    let actual = access_type_at_path(target)
        .map_err(|e| Status::internal(format!("failed to inspect existing target path: {e}")))?;
    if actual != capability.access_type {
        let name = |a: AccessType| if a == AccessType::Block { "block" } else { "mount" };
        return Err(Status::already_exists(format!(
            "target path {target} already contains access type {}, requested {}",
            name(actual),
            name(capability.access_type)
        )));
    }
    let info = state
        .mounter
        .mount_info(target, deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to inspect existing publication mount: {e:#}")))?;
    let mut live = normalize_mount_source(&info.source);
    if capability.access_type == AccessType::Block {
        // The mount table shows a bound device node's source as devtmpfs
        // ("udev[/nvme0n1]"), not the device: compare device numbers.
        let number = |path: &str| {
            (state.host.device_number)(path)
                .map_err(|e| Status::internal(format!("failed to inspect existing raw block publication: {e}")))
        };
        let (bound, staged) = (number(target)?, number(expected)?);
        if bound.is_none() || bound != staged {
            return Err(Status::already_exists(format!(
                "target path {target} is not bound to the staged device {expected}"
            )));
        }
        live = expected.to_string();
    }
    if info.read_only != req.readonly {
        return Err(Status::already_exists(format!(
            "target path {target} readonly state is {}, requested {}",
            info.read_only, req.readonly
        )));
    }
    match state.records.publication(target) {
        Some(record) => {
            if record.volume_id != req.volume_id
                || record.capability != *capability
                || record.readonly != req.readonly
                || !mount_sources_equal(&record.expected_source, expected)
                || !mount_sources_equal(&record.live_source, &live)
            {
                return Err(Status::already_exists(format!(
                    "target path {target} already contains an incompatible publication"
                )));
            }
        }
        None if !mount_sources_equal(&live, expected) => {
            return Err(Status::already_exists(format!(
                "target path {target} is backed by {live}, requested {expected}"
            )));
        }
        None => {}
    }
    state.records.store_publication(MountRecord {
        volume_id: req.volume_id.clone(),
        target_path: target.to_string(),
        expected_source: expected.to_string(),
        live_source: live,
        capability: capability.clone(),
        readonly: req.readonly,
    });
    Ok(())
}

fn blocked_by(req: &csi::NodePublishVolumeRequest, capability: &Signature, other: &str) -> Status {
    if capability.access_mode == Mode::SingleNodeSingleWriter as i32 {
        info!(
            "NodePublishVolume: single-writer migration for volume {} is blocked by the still-mounted target {other}; kubelet may still be tearing down the prior pod",
            req.volume_id
        );
    }
    Status::failed_precondition(format!(
        "volume {} is already published at different target path {other}",
        req.volume_id
    ))
}

/// Refuses a second target for a volume whose access mode allows one, while
/// another is mounted; the mount table catches publications this process has
/// no record of (it restarted).
async fn ensure_target_allowed(
    state: &State,
    req: &csi::NodePublishVolumeRequest,
    capability: &Signature,
    expected: &str,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let multiple = allows_multiple_targets(capability.access_mode);
    for record in state.records.publications() {
        if record.volume_id != req.volume_id || record.target_path == req.target_path {
            continue;
        }
        let mounted = state
            .mounter
            .is_mounted(&record.target_path, deadline)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "failed to verify existing publication at {}: {e:#}",
                    record.target_path
                ))
            })?;
        if !mounted {
            state.records.delete_publication(&record.target_path);
            continue;
        }
        if record.capability != *capability
            || record.readonly != req.readonly
            || !mount_sources_equal(&record.expected_source, expected)
            || !multiple
        {
            return Err(blocked_by(req, capability, &record.target_path));
        }
    }
    if multiple {
        return Ok(());
    }
    let mounts = state
        .mounter
        .list_mounts()
        .map_err(|e| Status::internal(format!("failed to rebuild publication state from mount table: {e:#}")))?;
    for mount in mounts {
        if mount.target == req.target_path
            || mount.target == req.staging_target_path
            || state.records.is_stage_target(&mount.target)
            || !likely_csi_publication_target(&mount.target)
        {
            continue;
        }
        if mount_sources_equal(&mount.source, expected) {
            return Err(blocked_by(req, capability, &mount.target));
        }
    }
    Ok(())
}

/// That a raw-block device belongs to the volume being published.
async fn validate_raw_block_ownership(
    state: &State,
    volume_id: &str,
    device: &str,
    share: ShareType,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    match share {
        ShareType::Nvmeof if is_ublk_device(device) => {
            ublk_stage::validate_raw_block_ownership(state, volume_id, device, deadline).await
        }
        ShareType::Nvmeof => crate::nvme_kernel::validate_raw_block_ownership(state, volume_id, device),
        // NFS never publishes a raw block volume.
        ShareType::Nfs => Ok(()),
        _ => Err(Status::failed_precondition(format!(
            "raw block device {device} is a kernel device, which the Rust node agent does not serve yet"
        ))),
    }
}

async fn remember(
    state: &State,
    req: &csi::NodePublishVolumeRequest,
    capability: &Signature,
    expected: &str,
    deadline: Option<Instant>,
) {
    // A raw-block publication is the staged device itself (its mount source is
    // devtmpfs); validate_existing compares it the same way.
    let live = match state.mounter.mount_info(&req.target_path, deadline).await {
        Ok(info) if capability.access_type != AccessType::Block => normalize_mount_source(&info.source),
        _ => expected.to_string(),
    };
    state.records.store_publication(MountRecord {
        volume_id: req.volume_id.clone(),
        target_path: req.target_path.clone(),
        expected_source: expected.to_string(),
        live_source: live,
        capability: capability.clone(),
        readonly: req.readonly,
    });
}

pub async fn node_publish(
    state: &State,
    req: &csi::NodePublishVolumeRequest,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let (volume_id, target, staging) = (
        req.volume_id.as_str(),
        req.target_path.as_str(),
        req.staging_target_path.as_str(),
    );
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if target.is_empty() {
        return Err(Status::invalid_argument("target path is required"));
    }
    let Some(volume_capability) = req.volume_capability.as_ref() else {
        return Err(Status::invalid_argument("volume capability is required"));
    };
    let capability = capability::signature(Some(volume_capability))?;
    let event = node_volume_ref(&req.volume_context, volume_id, &state.node_name);
    info!("NodePublishVolume: volumeID={volume_id}, targetPath={target}, stagingPath={staging}");
    let _volume = state
        .locks
        .try_lock(node_volume_key(volume_id))
        .ok_or_else(|| Status::aborted("operation already in progress"))?;
    let _target = state
        .locks
        .try_lock(node_target_key(target))
        .ok_or_else(|| Status::aborted("target path operation already in progress"))?;

    let expected = expected_source(state, req, &capability, deadline).await?;
    if let Some(parent) = Path::new(target).parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(parent)
            .map_err(|e| Status::internal(format!("failed to create target directory: {e}")))?;
    }
    let mounted = state
        .mounter
        .is_mounted(target, deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    if mounted {
        validate_existing(state, req, &capability, &expected, deadline).await?;
        info!("Volume {volume_id} already mounted compatibly at {target}");
        return Ok(());
    }
    ensure_target_allowed(state, req, &capability, &expected, deadline).await?;

    let mount_failed = |status: Status| {
        if let Some(object) = &event {
            state.events.warning(object, REASON_MOUNT_FAILED, status.message());
        }
        status
    };
    if capability.access_type == AccessType::Block {
        // expected_source resolved the staged device; this is the same path.
        let device = expected.clone();
        let share = capability::attach_driver(&req.volume_context, &state.driver_name);
        validate_raw_block_ownership(state, volume_id, &device, share, deadline).await?;
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o640)
            .open(target)
            .map_err(|e| Status::internal(format!("failed to create block target file: {e}")))?;
        let options: Vec<String> = if req.readonly { vec!["ro".into()] } else { Vec::new() };
        state
            .mounter
            .bind_mount(&device, target, &options, deadline)
            .await
            .map_err(|e| mount_failed(Status::internal(format!("failed to bind mount block device: {e:#}"))))?;
    } else {
        // The request's flags as given (not de-duplicated), after "ro".
        let mut options: Vec<String> = if req.readonly { vec!["ro".into()] } else { Vec::new() };
        if let Some(csi::volume_capability::AccessType::Mount(m)) = &volume_capability.access_type {
            options.extend(m.mount_flags.iter().cloned());
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o750)
            .create(target)
            .map_err(|e| Status::internal(format!("failed to create target path: {e}")))?;
        state
            .mounter
            .bind_mount(staging, target, &options, deadline)
            .await
            .map_err(|e| mount_failed(Status::internal(format!("failed to bind mount: {e:#}"))))?;
    }
    remember(state, req, &capability, &expected, deadline).await;
    info!("Volume {volume_id} published successfully at {target}");
    Ok(())
}

pub async fn node_unpublish(
    state: &State,
    req: &csi::NodeUnpublishVolumeRequest,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let (volume_id, target) = (req.volume_id.as_str(), req.target_path.as_str());
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if target.is_empty() {
        return Err(Status::invalid_argument("target path is required"));
    }
    info!("NodeUnpublishVolume: volumeID={volume_id}, targetPath={target}");
    let _volume = state
        .locks
        .try_lock(node_volume_key(volume_id))
        .ok_or_else(|| Status::aborted("operation already in progress"))?;
    let _target = state
        .locks
        .try_lock(node_target_key(target))
        .ok_or_else(|| Status::aborted("target path operation already in progress"))?;

    if let Err(unmount) = state.mounter.unmount(target, deadline).await {
        warn!("Failed to unmount target path: {unmount:#}");
        match state.mounter.is_mounted(target, deadline).await {
            Err(check) => {
                warn!("Failed to check mount status after unmount failure: {check:#}");
                return Err(Status::internal(format!(
                    "failed to unmount target path and cannot verify mount status: {unmount:#}"
                )));
            }
            Ok(true) => {
                return Err(Status::internal(format!(
                    "failed to unmount target path (still mounted): {unmount:#}"
                )));
            }
            Ok(false) => info!("Target path {target} is not mounted, proceeding with cleanup"),
        }
    }
    if let Err(e) = remove_all(target) {
        warn!("Failed to remove target path: {e}");
    }
    state.records.delete_publication(target);
    debug!("Volume {volume_id} unpublished successfully");
    Ok(())
}
