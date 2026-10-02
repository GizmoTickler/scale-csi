//! A block device's /dev node, checked against the kernel's number for it.
//! Shared by the iSCSI and kernel NVMe-oF device lookups.

use std::path::Path;
use std::sync::Arc;

/// A path's device number, when it is a block device.
pub type BlockDeviceNumber = Arc<dyn Fn(&str) -> std::io::Result<Option<u64>> + Send + Sync>;

/// stat(2) of a path (symlinks followed): its number when it is a block device.
pub(crate) fn block_device_number(path: &str) -> std::io::Result<Option<u64>> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let meta = std::fs::metadata(path)?;
    Ok(meta.file_type().is_block_device().then(|| meta.rdev()))
}

/// Whether `device` is a block device node with the number the kernel gives
/// the disk (`sysfs_dev` holds `MAJ:MIN`). Right after one session is torn
/// down and a new one is connected to the same target (an iSCSI logout and
/// login, an NVMe-oF disconnect and connect, or a handover between the Go and
/// Rust node plugins), sysfs already names the new disk while /dev can still
/// hold the previous disk's node of that name (devtmpfs and udev removal lag),
/// or a stale node with no fresh one yet; opening it fails with ENXIO/ENODEV,
/// so a device wait keeps polling until the node matches. One stat and one
/// small sysfs read.
pub(crate) fn is_current_node(device_number: &BlockDeviceNumber, device: &Path, sysfs_dev: &Path) -> bool {
    let Ok(Some(number)) = device_number(&device.to_string_lossy()) else {
        return false;
    };
    let Ok(want) = std::fs::read_to_string(sysfs_dev) else {
        return false;
    };
    let Some((major, minor)) = want.trim().split_once(':') else {
        return false;
    };
    match (major.parse::<u32>(), minor.parse::<u32>()) {
        (Ok(major), Ok(minor)) => libc::major(number) == major && libc::minor(number) == minor,
        _ => false,
    }
}
