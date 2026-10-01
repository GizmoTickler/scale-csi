//! NodeStageVolume and NodeUnstageVolume (`pkg/driver/node.go`).
//!
//! A repeated stage with the same volume, source and capability is the same
//! operation and succeeds without touching anything; a different volume or
//! capability at the same path is AlreadyExists. The live target is read again
//! after a stage, so an external mount that appeared meanwhile cannot slip
//! past the check. The shared tail (finalizeStagedDevice, createSymlinkAtomic):
//! a raw-block volume gets a symlink to its device at the staging path, a
//! filesystem volume is formatted if blank and mounted there.
//!
//! This agent serves NVMe-oF through nvmeublkd only, so far. A volume on any
//! other path is refused with FailedPrecondition before anything is changed.

use std::collections::HashMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use log::{debug, info, warn};
use tonic::{Code, Status};

use crate::capability::{
    self, AccessType, ShareType, Signature, mount_flags_for_fs, mount_sources_equal, normalize_mount_source,
    protocol_share_name,
};
use crate::config::DataPath;
use crate::csi;
use crate::events::node_volume_ref;
use crate::locks::node_volume_key;
use crate::mount::Mounter;
use crate::nvme_kernel;
use crate::records::MountRecord;
use crate::service::State;
use crate::ublk_client::is_ublk_device;
use crate::ublk_stage::{self, StageRequest, Unstaged};

fn access_name(access: AccessType) -> &'static str {
    match access {
        AccessType::Mount => "mount",
        AccessType::Block => "block",
    }
}

fn share_name(share: ShareType) -> &'static str {
    match share {
        ShareType::Nfs => "nfs",
        ShareType::Iscsi => "iscsi",
        ShareType::Nvmeof => "nvmeof",
    }
}

fn not_served(what: String) -> Status {
    Status::failed_precondition(format!("{what}, which the Rust node agent does not serve yet"))
}

/// The volume context with one attach-scoped hint (`addresses`, `portals`)
/// from the publish context; present wins, even empty or malformed, so the
/// protocol's parser makes its observable fallback.
pub fn with_publish_hint(
    volume: &HashMap<String, String>,
    publish: &HashMap<String, String>,
    key: &str,
) -> HashMap<String, String> {
    let mut merged = volume.clone();
    if let Some(value) = publish.get(key) {
        merged.insert(key.to_string(), value.clone());
    }
    merged
}

/// The device a block staging link resolves to, when that is a present node
/// under the device directory, `/dev` (Go stagedBlockDevicePath).
pub fn staged_block_device_path(staging: &str, dev_dir: &Path) -> Option<String> {
    let meta = std::fs::symlink_metadata(staging).ok()?;
    if !meta.file_type().is_symlink() {
        return None;
    }
    let device = std::fs::canonicalize(staging).ok()?.to_str()?.to_string();
    if !Path::new(&device).starts_with(dev_dir) {
        return None;
    }
    std::fs::metadata(&device).ok()?;
    Some(device)
}

/// That a staged device is the one the request names.
async fn verify_stage_device_source(
    state: &State,
    volume_id: &str,
    device: &str,
    share: ShareType,
    context: &HashMap<String, String>,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    match share {
        ShareType::Nfs => Err(Status::already_exists(
            "NFS staging target cannot contain a raw block device",
        )),
        ShareType::Nvmeof if is_ublk_device(device) => {
            ublk_stage::verify_stage_source(state, volume_id, device, context, deadline).await
        }
        ShareType::Nvmeof => nvme_kernel::verify_stage_source(state, device, context),
        _ => Err(not_served(format!(
            "staging target is backed by {device}, a kernel {} device",
            share_name(share)
        ))),
    }
}

/// What an existing stage is compared against.
pub struct Wanted<'a> {
    pub volume_id: &'a str,
    pub staging: &'a str,
    /// The volume context as the request carries it.
    pub context: &'a HashMap<String, String>,
    pub share: ShareType,
    pub capability: &'a Signature,
    pub expected_source: &'a str,
    pub deadline: Option<Instant>,
}

impl Wanted<'_> {
    fn record_compatible(&self, record: &MountRecord) -> bool {
        record.volume_id == self.volume_id
            && record.expected_source == self.expected_source
            && record.capability == *self.capability
    }
}

/// Whether the staging path already holds a stage: `Ok(true)` when it holds
/// this one (recorded), `Ok(false)` when it holds none, an error when it holds
/// something else (Go handleExistingStageContext).
pub async fn handle_existing_stage(state: &State, want: &Wanted<'_>) -> Result<bool, Status> {
    let staging = want.staging;
    let mounted = state
        .mounter
        .is_mounted(staging, want.deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    let symlink = std::fs::symlink_metadata(staging).is_ok_and(|m| m.file_type().is_symlink());
    if !mounted && !symlink {
        state.records.delete_stage(staging);
        return Ok(false);
    }
    let actual = if symlink { AccessType::Block } else { AccessType::Mount };
    if actual != want.capability.access_type {
        return Err(Status::already_exists(format!(
            "staging target {staging} already contains access type {}, requested {}",
            access_name(actual),
            access_name(want.capability.access_type)
        )));
    }

    let live_source = if symlink {
        let device = match std::fs::canonicalize(staging) {
            Ok(device) => device.to_string_lossy().into_owned(),
            // A dangling link: the device vanished (a reboot with a persisted
            // staging dir, a dropped session). Failing would wedge the volume;
            // "not staged" lets the stage reattach and replace the link,
            // unless the record says the path belongs to another volume.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(record) = state.records.stage(staging)
                    && !want.record_compatible(&record)
                {
                    return Err(Status::already_exists(format!(
                        "staging target {staging} is already occupied by an incompatible staged volume"
                    )));
                }
                state.records.delete_stage(staging);
                return Ok(false);
            }
            Err(e) => {
                return Err(Status::internal(format!("failed to resolve staged block device: {e}")));
            }
        };
        if let Err(source) =
            verify_stage_device_source(state, want.volume_id, &device, want.share, want.context, want.deadline).await
        {
            // ublk numbers restart at 0 on every boot and every /dev/ublkbN is a
            // CSI volume, so a link that survived a reboot routinely resolves to
            // ANOTHER volume's device. The daemon already proved it stale;
            // unless a record says the path is another volume's, re-stage (the
            // other device is never touched).
            if is_ublk_device(&device)
                && source.code() == Code::AlreadyExists
                && state
                    .records
                    .stage(staging)
                    .is_none_or(|record| want.record_compatible(&record))
            {
                info!(
                    "Staging link {staging} resolves to {device}, now serving another volume ({}); re-staging {}",
                    source.message(),
                    want.volume_id
                );
                state.records.delete_stage(staging);
                return Ok(false);
            }
            return Err(source);
        }
        normalize_mount_source(&device)
    } else {
        let info = state
            .mounter
            .mount_info(staging, want.deadline)
            .await
            .map_err(|e| Status::internal(format!("failed to inspect existing staging mount: {e:#}")))?;
        let live = normalize_mount_source(&info.source);
        if want.share == ShareType::Nfs {
            if !mount_sources_equal(&live, want.expected_source) {
                return Err(Status::already_exists(format!(
                    "staging target {staging} is backed by {live}, requested {}",
                    want.expected_source
                )));
            }
            if info.fs_type != "nfs" && info.fs_type != "nfs4" {
                return Err(Status::already_exists(format!(
                    "staging target {staging} has filesystem {}, requested NFS",
                    info.fs_type
                )));
            }
        } else {
            verify_stage_device_source(state, want.volume_id, &live, want.share, want.context, want.deadline).await?;
            let fs = &want.capability.fs_type;
            if !fs.is_empty() && !info.fs_type.eq_ignore_ascii_case(fs) {
                return Err(Status::already_exists(format!(
                    "staging target {staging} has filesystem {}, requested {fs}",
                    info.fs_type
                )));
            }
        }
        live
    };

    if let Some(record) = state.records.stage(staging)
        && (!want.record_compatible(&record) || !mount_sources_equal(&record.live_source, &live_source))
    {
        return Err(Status::already_exists(format!(
            "staging target {staging} is already occupied by an incompatible staged volume"
        )));
    }
    state.records.store_stage(MountRecord {
        volume_id: want.volume_id.to_string(),
        target_path: staging.to_string(),
        expected_source: want.expected_source.to_string(),
        live_source,
        capability: want.capability.clone(),
        readonly: false,
    });
    Ok(true)
}

/// A best-effort record for a stage the mount table does not show yet.
async fn remember_stage(state: &State, want: &Wanted<'_>) {
    let mut live_source = want.expected_source.to_string();
    if want.capability.access_type == AccessType::Block {
        if let Some(device) = staged_block_device_path(want.staging, &state.host.dev_dir) {
            live_source = normalize_mount_source(&device);
        }
    } else if let Ok(info) = state.mounter.mount_info(want.staging, want.deadline).await {
        live_source = normalize_mount_source(&info.source);
    }
    state.records.store_stage(MountRecord {
        volume_id: want.volume_id.to_string(),
        target_path: want.staging.to_string(),
        expected_source: want.expected_source.to_string(),
        live_source,
        capability: want.capability.clone(),
        readonly: false,
    });
}

pub async fn node_stage(
    state: &State,
    req: &csi::NodeStageVolumeRequest,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let (volume_id, staging) = (req.volume_id.as_str(), req.staging_target_path.as_str());
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if staging.is_empty() {
        return Err(Status::invalid_argument("staging target path is required"));
    }
    let Some(capability) = req.volume_capability.as_ref() else {
        return Err(Status::invalid_argument("volume capability is required"));
    };
    if req.volume_context.is_empty() {
        return Err(Status::invalid_argument("volume context is required"));
    }
    info!("NodeStageVolume: volumeID={volume_id}, stagingPath={staging}");
    let _held = state
        .locks
        .try_lock(node_volume_key(volume_id))
        .ok_or_else(|| Status::aborted("operation already in progress"))?;

    let share = capability::attach_driver(&req.volume_context, &state.driver_name);
    if share != ShareType::Nvmeof {
        return Err(not_served(format!(
            "volume {volume_id} is a {} volume",
            share_name(share)
        )));
    }
    let stage_context = with_publish_hint(&req.volume_context, &req.publish_context, "addresses");
    let signature = capability::signature(Some(capability))?;
    let expected = capability::stage_source_identity(share, &req.volume_context)?;
    let want = Wanted {
        volume_id,
        staging,
        context: &req.volume_context,
        share,
        capability: &signature,
        expected_source: &expected,
        deadline,
    };
    if handle_existing_stage(state, &want).await? {
        // Kernel path convergence is for kernel controllers only: a ublk
        // device has none, and the daemon runs its own multipath.
        let ublk = state
            .records
            .stage(staging)
            .is_some_and(|record| is_ublk_device(&record.live_source));
        if !ublk {
            let event = node_volume_ref(&req.volume_context, volume_id, &state.node_name);
            nvme_kernel::converge_existing(state, &stage_context, event.as_ref(), deadline).await;
        }
        info!("Volume {volume_id} is already staged compatibly at {staging}");
        return Ok(());
    }

    // A filesystem mounts on the staging path; a raw-block volume becomes a
    // symlink at that exact path, so only its parent is created.
    let directory = if signature.access_type == AccessType::Block {
        Path::new(staging).parent().unwrap_or(Path::new("/"))
    } else {
        Path::new(staging)
    };
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o750)
        .create(directory)
        .map_err(|e| Status::internal(format!("failed to create staging directory: {e}")))?;
    let event = node_volume_ref(&req.volume_context, volume_id, &state.node_name);

    match ublk_stage::data_path_for_volume(state, &stage_context)? {
        DataPath::Ublk => {
            ublk_stage::stage(
                state,
                StageRequest {
                    volume_id,
                    context: &stage_context,
                    staging,
                    capability,
                    event: event.as_ref(),
                    deadline,
                },
            )
            .await?
        }
        DataPath::Kernel => {
            nvme_kernel::stage(
                state,
                StageRequest {
                    volume_id,
                    context: &stage_context,
                    staging,
                    capability,
                    event: event.as_ref(),
                    deadline,
                },
            )
            .await?
        }
    }

    if !handle_existing_stage(state, &want).await? {
        remember_stage(state, &want).await;
    }
    info!("Volume {volume_id} staged successfully at {staging}");
    Ok(())
}

/// `os.RemoveAll`: absent is fine, a directory goes with its contents.
pub(crate) fn remove_all(path: &str) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
    }
}

/// A live kernel NVMe-oF subsystem for this volume, if one exists. A sysfs
/// that cannot be read shows none: an unknown state must not by itself fail
/// an unstage that may have nothing to clean up (as the Go node's lookup).
fn kernel_nvme_session(state: &State, volume_id: &str) -> Option<String> {
    let n = &state.config.nvmeof;
    let suffix = format!(":{}{}{}", n.name_prefix, protocol_share_name(volume_id), n.name_suffix);
    let entries = std::fs::read_dir(state.host.sysfs.join("class/nvme-subsystem")).ok()?;
    entries.flatten().find_map(|entry| {
        let nqn = std::fs::read_to_string(entry.path().join("subsysnqn")).ok()?;
        let nqn = nqn.trim();
        nqn.ends_with(&suffix).then(|| nqn.to_string())
    })
}

pub async fn node_unstage(
    state: &State,
    req: &csi::NodeUnstageVolumeRequest,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let (volume_id, staging) = (req.volume_id.as_str(), req.staging_target_path.as_str());
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if staging.is_empty() {
        return Err(Status::invalid_argument("staging target path is required"));
    }
    info!("NodeUnstageVolume: volumeID={volume_id}, stagingPath={staging}");
    let _held = state
        .locks
        .try_lock(node_volume_key(volume_id))
        .ok_or_else(|| Status::aborted("operation already in progress"))?;

    // The device, read before anything is unmounted; a block volume's staging
    // path is a symlink to it, not a mount.
    let device = match state.mounter.mount_source(staging, deadline).await {
        Ok(device) if !device.is_empty() => device,
        mounted => match std::fs::read_link(staging) {
            Ok(target) => target.to_string_lossy().into_owned(),
            Err(link) => {
                debug!(
                    "Could not get device from staging path {staging}: mount err={:?}, symlink err={link}",
                    mounted.err()
                );
                String::new()
            }
        },
    };
    // A kernel device needs the kernel session cleanup: refuse before
    // changing anything rather than do half of it.
    if !device.is_empty() && !is_ublk_device(&device) {
        return Err(not_served(format!(
            "volume {volume_id} is staged on {device}, a kernel device"
        )));
    }

    let symlink = std::fs::symlink_metadata(staging).is_ok_and(|m| m.file_type().is_symlink());
    if symlink {
        if let Err(e) = std::fs::remove_file(staging)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!("Failed to remove staging symlink: {e}");
        }
    } else {
        if let Err(unmount) = state.mounter.unmount(staging, deadline).await {
            warn!("Failed to unmount staging path: {unmount:#}");
            match state.mounter.is_mounted(staging, deadline).await {
                Err(check) => {
                    warn!("Failed to check mount status after unmount failure: {check:#}");
                    return Err(Status::internal(format!(
                        "failed to unmount staging path and cannot verify mount status: {unmount:#}"
                    )));
                }
                Ok(true) => {
                    return Err(Status::internal(format!(
                        "failed to unmount staging path (still mounted): {unmount:#}"
                    )));
                }
                Ok(false) => info!("Staging path {staging} is not mounted, proceeding with cleanup"),
            }
        }
        if let Err(e) = remove_all(staging) {
            warn!("Failed to remove staging directory: {e}");
        }
    }

    match ublk_stage::unstage(state, volume_id, &device, deadline).await? {
        Unstaged::Done => {}
        Unstaged::DetachedKernelRemains => {
            return Err(not_served(format!(
                "volume {volume_id} still has a kernel device {device}"
            )));
        }
        Unstaged::NotUblk => {
            if let Some(nqn) = kernel_nvme_session(state, volume_id) {
                return Err(not_served(format!(
                    "volume {volume_id} has a kernel NVMe-oF session {nqn}"
                )));
            }
        }
    }
    state.records.delete_stage(staging);
    info!("Volume {volume_id} unstaged successfully");
    Ok(())
}

/// Finishes a stage once `device` is attached.
pub async fn finalize_staged_device(
    mounter: &Mounter,
    device: &str,
    staging: &str,
    capability: &csi::VolumeCapability,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    use csi::volume_capability::AccessType;
    match &capability.access_type {
        Some(AccessType::Block(_)) => create_symlink_atomic(Path::new(device), Path::new(staging))
            .map_err(|e| Status::internal(format!("failed to create device symlink: {e:#}"))),
        access => {
            let fs_type = match access {
                Some(AccessType::Mount(m)) if !m.fs_type.is_empty() => m.fs_type.to_lowercase(),
                _ => "ext4".to_string(),
            };
            let flags = mount_flags_for_fs(capability, &fs_type);
            mounter
                .format_and_mount(device, staging, &fs_type, &flags, deadline)
                .await
                .map_err(|e| Status::internal(format!("failed to format and mount: {e:#}")))
        }
    }
}

/// A symlink at `link` to `target`, replacing what is there without ever
/// deleting data: a symlink to the same target is kept; another symlink, an
/// empty directory (kubelet pre-creates the raw-block staging path as one) or
/// a regular file is replaced through a temporary link and an atomic rename;
/// anything else (a non-empty directory, a device node) is refused.
pub fn create_symlink_atomic(target: &Path, link: &Path) -> Result<()> {
    match std::os::unix::fs::symlink(target, link) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e).context("failed to create symlink"),
    }
    let meta = std::fs::symlink_metadata(link).context("failed to inspect existing symlink path")?;
    let kind = meta.file_type();
    if kind.is_symlink() {
        let existing = std::fs::read_link(link).context("failed to read existing symlink")?;
        if existing == target {
            return Ok(());
        }
        warn!(
            "existing symlink {} points to {}, expected {}; recreating atomically",
            link.display(),
            existing.display(),
            target.display()
        );
    } else if kind.is_dir() {
        // Only the empty leaf; never recursive.
        std::fs::remove_dir(link).with_context(|| {
            format!(
                "failed to remove existing directory {} before creating symlink (directory must be empty)",
                link.display()
            )
        })?;
        warn!(
            "removed existing empty directory {} before creating device symlink",
            link.display()
        );
    } else if kind.is_file() {
        std::fs::remove_file(link).with_context(|| {
            format!(
                "failed to remove existing regular file {} before creating symlink",
                link.display()
            )
        })?;
        warn!(
            "removed existing regular file {} before creating device symlink",
            link.display()
        );
    } else {
        bail!(
            "cannot replace existing path {} with symlink: unsupported file type",
            link.display()
        );
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let temp = link.with_file_name(format!(
        "{}.tmp.{nanos}",
        link.file_name().and_then(|n| n.to_str()).unwrap_or("staging")
    ));
    std::os::unix::fs::symlink(target, &temp).context("failed to create temporary symlink")?;
    if let Err(e) = std::fs::rename(&temp, link) {
        let _ = std::fs::remove_file(&temp);
        return Err(e).context("failed to atomically replace symlink");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symlink_replacement_rules() {
        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("stage");
        let dev = Path::new("/dev/ublkb3");

        create_symlink_atomic(dev, &link).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), dev);
        create_symlink_atomic(dev, &link).unwrap(); // same target: kept

        let other = Path::new("/dev/ublkb7");
        create_symlink_atomic(other, &link).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), other, "a stale link is replaced");

        std::fs::remove_file(&link).unwrap();
        std::fs::create_dir(&link).unwrap();
        create_symlink_atomic(dev, &link).unwrap();
        assert_eq!(
            std::fs::read_link(&link).unwrap(),
            dev,
            "kubelet's empty directory is replaced"
        );

        std::fs::remove_file(&link).unwrap();
        std::fs::write(&link, b"x").unwrap();
        create_symlink_atomic(dev, &link).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), dev, "a regular file is replaced");

        std::fs::remove_file(&link).unwrap();
        std::fs::create_dir(&link).unwrap();
        std::fs::write(link.join("data"), b"keep").unwrap();
        assert!(
            create_symlink_atomic(dev, &link).is_err(),
            "a non-empty directory is never removed"
        );
        assert_eq!(std::fs::read(link.join("data")).unwrap(), b"keep");

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "no temporary links left behind");
    }
}
