//! The tail every block-protocol stage shares (`pkg/driver/node.go`
//! finalizeStagedDevice, createSymlinkAtomic): a raw-block volume gets a
//! symlink to its device at the staging path; a filesystem volume is formatted
//! if blank and mounted there.

use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use log::warn;
use tonic::Status;

use crate::capability::mount_flags_for_fs;
use crate::csi;
use crate::mount::Mounter;

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
