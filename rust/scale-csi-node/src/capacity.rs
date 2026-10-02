//! NodeGetVolumeStats and NodeExpandVolume (`pkg/driver/node.go`,
//! `node_nvmeublk.go`).
//!
//! Stats never touch a volume path before the mount table says it answers: a
//! stat of a dead hard NFS mount blocks in D-state for good. A raw-block
//! volume reports its device's size, a filesystem its bytes and inodes.
//!
//! A ublk device is sized from the namespace when the daemon attaches it and
//! cannot grow while live, so expansion has no rescan: when the device already
//! covers the request (it was attached after the controller grew the zvol)
//! only the filesystem grows; otherwise the caller is told to re-stage.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use log::{debug, info};
use tonic::Status;

use crate::capability::ShareType;
use crate::csi;
use crate::locks::node_volume_key;
use crate::service::State;
use crate::ublk_client::is_ublk_device;
use crate::ublk_stage;

fn sectors_to_bytes(path: &Path) -> Result<i64> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("failed to read device size from {}", path.display()))?;
    let sectors: i64 = text
        .trim()
        .parse()
        .with_context(|| format!("failed to parse device size from {}", path.display()))?;
    if sectors < 0 {
        bail!("device size from {} is negative", path.display());
    }
    sectors
        .checked_mul(512)
        .with_context(|| format!("device size from {} exceeds int64 bytes", path.display()))
}

/// A block device's size in bytes, from sysfs by name. A link
/// (`/dev/mapper/<name>`) is resolved to the node it names first: sysfs knows
/// a multipath map as dm-N only.
pub fn device_size(state: &State, device: &str) -> Result<i64> {
    let resolved = std::fs::canonicalize(device).unwrap_or_else(|_| Path::new(device).to_path_buf());
    let name = resolved.file_name().context("a device path has a name")?;
    sectors_to_bytes(&state.host.sysfs.join("class/block").join(name).join("size"))
}

struct FsStats {
    total_bytes: i64,
    available_bytes: i64,
    used_bytes: i64,
    total_inodes: i64,
    available_inodes: i64,
    used_inodes: i64,
}

fn filesystem_stats(path: &str) -> Result<FsStats> {
    let c_path = CString::new(Path::new(path).as_os_str().as_bytes()).context("path contains a NUL")?;
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: c_path is a valid NUL-terminated string and st a writable statfs.
    if unsafe { libc::statfs(c_path.as_ptr(), &mut st) } != 0 {
        return Err(std::io::Error::last_os_error()).context("statfs failed");
    }
    let block = st.f_bsize as i64;
    let (blocks, bfree, bavail) = (st.f_blocks as i64, st.f_bfree as i64, st.f_bavail as i64);
    let (files, ffree) = (st.f_files as i64, st.f_ffree as i64);
    Ok(FsStats {
        total_bytes: blocks.saturating_mul(block),
        available_bytes: bavail.saturating_mul(block),
        used_bytes: (blocks - bfree).saturating_mul(block),
        total_inodes: files,
        available_inodes: ffree,
        used_inodes: files - ffree,
    })
}

pub async fn node_get_volume_stats(
    state: &State,
    req: &csi::NodeGetVolumeStatsRequest,
    deadline: Option<Instant>,
) -> Result<csi::NodeGetVolumeStatsResponse, Status> {
    use csi::volume_usage::Unit;
    let (volume_id, path) = (req.volume_id.as_str(), req.volume_path.as_str());
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if path.is_empty() {
        return Err(Status::invalid_argument("volume path is required"));
    }
    debug!("NodeGetVolumeStats: volumeID={volume_id}, volumePath={path}");
    // The mount table only, bounded: never the filesystem itself yet.
    state
        .mounter
        .is_mounted(path, deadline)
        .await
        .map_err(|e| Status::internal(format!("mount unresponsive for {path}: {e:#}")))?;
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found(format!("volume path {path} does not exist")));
        }
        Err(e) => {
            return Err(Status::internal(format!("failed to inspect volume path {path}: {e}")));
        }
    };
    if meta.file_type().is_block_device() {
        let rdev = meta.rdev();
        let number = format!("{}:{}", libc::major(rdev), libc::minor(rdev));
        let total = sectors_to_bytes(&state.host.sysfs.join("dev/block").join(&number).join("size"))
            .map_err(|e| Status::internal(format!("failed to get block device size for {number}: {e:#}")))?;
        return Ok(csi::NodeGetVolumeStatsResponse {
            usage: vec![csi::VolumeUsage {
                total,
                unit: Unit::Bytes as i32,
                ..Default::default()
            }],
        });
    }
    let stats = filesystem_stats(path)
        .map_err(|e| Status::internal(format!("failed to get filesystem stats for {path}: {e:#}")))?;
    Ok(csi::NodeGetVolumeStatsResponse {
        usage: vec![
            csi::VolumeUsage {
                available: stats.available_bytes,
                total: stats.total_bytes,
                used: stats.used_bytes,
                unit: Unit::Bytes as i32,
            },
            csi::VolumeUsage {
                available: stats.available_inodes,
                total: stats.total_inodes,
                used: stats.used_inodes,
                unit: Unit::Inodes as i32,
            },
        ],
    })
}

/// The device behind an expansion request: a staging or volume path that is a
/// symlink into the device directory (raw block), else what is mounted there.
async fn expansion_device(
    state: &State,
    req: &csi::NodeExpandVolumeRequest,
    deadline: Option<Instant>,
) -> Result<(String, bool), Status> {
    let mut raw_block = matches!(
        req.volume_capability.as_ref().and_then(|c| c.access_type.as_ref()),
        Some(csi::volume_capability::AccessType::Block(_))
    );
    let dev = format!("{}/", state.host.dev_dir.display());
    for path in [req.staging_target_path.as_str(), req.volume_path.as_str()] {
        if path.is_empty() {
            continue;
        }
        if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
            let resolved = std::fs::canonicalize(path)
                .map_err(|e| Status::internal(format!("failed to resolve expansion device: {e}")))?
                .to_string_lossy()
                .into_owned();
            if resolved.starts_with(&dev) {
                raw_block = true;
                return Ok((resolved, raw_block));
            }
        }
        if let Ok(source) = state.mounter.mount_source(path, deadline).await
            && source.starts_with(&dev)
        {
            return Ok((source, raw_block));
        }
    }
    Ok((String::new(), raw_block))
}

pub async fn node_expand_volume(
    state: &State,
    req: &csi::NodeExpandVolumeRequest,
    deadline: Option<Instant>,
) -> Result<csi::NodeExpandVolumeResponse, Status> {
    let (volume_id, path) = (req.volume_id.as_str(), req.volume_path.as_str());
    if volume_id.is_empty() {
        return Err(Status::invalid_argument("volume ID is required"));
    }
    if path.is_empty() {
        return Err(Status::invalid_argument("volume path is required"));
    }
    match std::fs::metadata(path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(Status::not_found(format!("volume path {path} does not exist")));
        }
        Err(e) => {
            return Err(Status::internal(format!("failed to inspect volume path {path}: {e}")));
        }
    }
    info!("NodeExpandVolume: volumeID={volume_id}, volumePath={path}");
    let _held = state
        .locks
        .try_lock(node_volume_key(volume_id))
        .ok_or_else(|| Status::aborted("operation already in progress"))?;
    let capacity = req.capacity_range.as_ref().map_or(0, |r| r.required_bytes);
    let (device, raw_block) = expansion_device(state, req, deadline).await?;

    if is_ublk_device(&device) {
        if raw_block {
            ublk_stage::validate_raw_block_ownership(state, volume_id, &device, deadline).await?;
        }
        let size = device_size(state, &device)
            .map_err(|e| Status::internal(format!("failed to read size of {device}: {e:#}")))?;
        if capacity > 0 && size < capacity {
            return Err(Status::failed_precondition(format!(
                "ublk device {device} is {size} bytes, below the requested {capacity}: nvmeublkd cannot grow a live device; it picks up the new size when the volume is next staged (restart the pod)"
            )));
        }
        if !raw_block {
            state
                .mounter
                .resize_filesystem(path, deadline)
                .await
                .map_err(|e| Status::internal(format!("failed to resize filesystem: {e:#}")))?;
        }
        info!("Volume {volume_id} expanded on ublk device {device} ({size} bytes)");
        return Ok(csi::NodeExpandVolumeResponse { capacity_bytes: size });
    }
    if device.is_empty() {
        // Nothing block-backed: an NFS install has nothing to grow on a node.
        if crate::capability::attach_driver(&Default::default(), &state.driver_name) == ShareType::Nfs {
            info!("Volume {volume_id} expanded successfully");
            return Ok(csi::NodeExpandVolumeResponse {
                capacity_bytes: capacity,
            });
        }
        return Err(Status::internal("failed to resolve block device for expansion"));
    }
    let transport = if crate::nvme::controller_of(&device).is_some() {
        Transport::Nvme
    } else if state.config.iscsi_enabled
        && (state.iscsi.is_likely_iscsi_device(
            &std::fs::canonicalize(&device).map_or_else(|_| device.clone(), |p| p.to_string_lossy().into_owned()),
        ) || crate::capability::attach_driver(&Default::default(), &state.driver_name) == ShareType::Iscsi)
    {
        // Go blockTransportForDevice: a SCSI disk or dm-multipath map, else
        // the driver's default protocol. A /dev/mapper name is resolved to its
        // dm-N first (the Go node classifies the link's own name, so a
        // multipath filesystem on a generic driver name was never rescanned).
        Transport::Iscsi
    } else {
        return Err(Status::failed_precondition(format!(
            "volume {volume_id} is on {device}, which is not an NVMe-oF or iSCSI device; the Rust node agent does not serve it yet"
        )));
    };
    expand_kernel(
        state, volume_id, path, &device, raw_block, capacity, transport, deadline,
    )
    .await
}

/// The kernel transport a block device is rescanned through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Nvme,
    Iscsi,
}

/// How long a rescanned device may take to show its new size.
const SIZE_SETTLE: Duration = Duration::from_secs(5);
const SIZE_POLL: Duration = Duration::from_millis(200);

/// Polls the device's size after a rescan (Go waitForDeviceSize): settled at
/// the requested capacity, or, with none requested, once it grew.
async fn wait_for_device_size(state: &State, device: &str, before: Option<i64>, capacity: i64) -> Result<i64> {
    let until = Instant::now() + SIZE_SETTLE;
    loop {
        let size = device_size(state, device);
        if let Ok(size) = size {
            let settled = if capacity > 0 {
                size >= capacity
            } else {
                before.is_none_or(|b| b <= 0 || size > b)
            };
            if settled {
                return Ok(size);
            }
        }
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            let size = size?;
            bail!(
                "capacity remained at {size} bytes (before={}, requested={capacity}) for {SIZE_SETTLE:?}",
                before.unwrap_or(0)
            );
        }
        tokio::time::sleep(left.min(SIZE_POLL)).await;
    }
}

/// Kernel NVMe-oF and iSCSI: rescan the namespace or the session, wait for
/// the size, then grow the filesystem (or, raw block, just check the size).
#[allow(clippy::too_many_arguments)]
async fn expand_kernel(
    state: &State,
    volume_id: &str,
    path: &str,
    device: &str,
    raw_block: bool,
    capacity: i64,
    transport: Transport,
    deadline: Option<Instant>,
) -> Result<csi::NodeExpandVolumeResponse, Status> {
    if raw_block {
        match transport {
            Transport::Nvme => crate::nvme_kernel::validate_raw_block_ownership(state, volume_id, device)?,
            Transport::Iscsi => crate::iscsi_stage::validate_raw_block_ownership(state, volume_id, device).await?,
        }
    }
    let before = match device_size(state, device) {
        Ok(size) => {
            info!("Device {device} size before rescan: {size} bytes");
            Some(size)
        }
        Err(e) => {
            log::warn!("Could not read device size before rescan for {device}: {e:#}");
            None
        }
    };
    match transport {
        Transport::Nvme => state
            .nvme
            .rescan(device, deadline)
            .await
            .map_err(|e| Status::internal(format!("failed to rescan NVMe-oF device {device}: {e:#}")))?,
        Transport::Iscsi => crate::iscsi_stage::rescan_device(state, device, deadline).await?,
    }
    let after = wait_for_device_size(state, device, before, capacity)
        .await
        .map_err(|e| Status::internal(format!("device size did not settle after rescan for {device}: {e:#}")))?;
    info!("Device {device} size after rescan: {after} bytes");
    if raw_block {
        if capacity > 0 && after < capacity {
            return Err(Status::internal(format!(
                "raw block device {device} capacity is {after} bytes after rescan, below requested {capacity} bytes"
            )));
        }
        info!("Raw block volume {volume_id} rescanned; skipping filesystem resize");
        return Ok(csi::NodeExpandVolumeResponse { capacity_bytes: after });
    }
    state
        .mounter
        .resize_filesystem(path, deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to resize filesystem: {e:#}")))?;
    if capacity > 0 && after < capacity {
        return Err(Status::internal(format!(
            "block device {device} capacity is {after} bytes after resize, below requested {capacity} bytes"
        )));
    }
    info!("Volume {volume_id} expanded successfully");
    Ok(csi::NodeExpandVolumeResponse { capacity_bytes: after })
}
