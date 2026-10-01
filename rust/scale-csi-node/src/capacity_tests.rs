//! NodeGetVolumeStats and NodeExpandVolume against the fakes.

use tonic::Code;

use crate::capacity::{node_expand_volume, node_get_volume_stats};
use crate::csi::{self, volume_usage::Unit};
use crate::stage::node_stage;
use crate::testing::{HOST_NQN, Node, UBLK_ON, VOLUME, block, context, filesystem, node};

fn stats_request(path: &str) -> csi::NodeGetVolumeStatsRequest {
    csi::NodeGetVolumeStatsRequest {
        volume_id: VOLUME.into(),
        volume_path: path.into(),
        ..Default::default()
    }
}

fn expand_request(
    path: &str,
    staging: &str,
    capacity: i64,
    capability: csi::VolumeCapability,
) -> csi::NodeExpandVolumeRequest {
    csi::NodeExpandVolumeRequest {
        volume_id: VOLUME.into(),
        volume_path: path.into(),
        staging_target_path: staging.into(),
        capacity_range: Some(csi::CapacityRange {
            required_bytes: capacity,
            limit_bytes: 0,
        }),
        volume_capability: Some(capability),
        ..Default::default()
    }
}

/// Sets the size sysfs reports for a device, by name.
fn set_size(n: &Node, name: &str, bytes: i64) {
    let dir = n.dir.path().join("sys/class/block").join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("size"), format!("{}\n", bytes / 512)).unwrap();
}

async fn staged(capability: csi::VolumeCapability) -> (Node, String) {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let staging = n.path("staging/globalmount");
    node_stage(
        &n.state,
        &csi::NodeStageVolumeRequest {
            volume_id: VOLUME.into(),
            staging_target_path: staging.clone(),
            volume_capability: Some(capability),
            volume_context: context(&[]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    (n, staging)
}

#[tokio::test]
async fn filesystem_stats() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let path = n.path("vol");
    std::fs::create_dir_all(&path).unwrap();
    let resp = node_get_volume_stats(&n.state, &stats_request(&path), None)
        .await
        .unwrap();
    assert_eq!(resp.usage.len(), 2);
    let (bytes, inodes) = (&resp.usage[0], &resp.usage[1]);
    assert_eq!(bytes.unit, Unit::Bytes as i32);
    assert_eq!(inodes.unit, Unit::Inodes as i32);
    assert!(
        bytes.total > 0 && bytes.used >= 0 && bytes.used <= bytes.total,
        "{bytes:?}"
    );
    assert!(bytes.available <= bytes.total);
    assert!(inodes.used <= inodes.total, "{inodes:?}");
}

#[tokio::test]
async fn stats_of_a_missing_path_is_not_found() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let err = node_get_volume_stats(&n.state, &stats_request(&n.path("gone")), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

/// A mount table that does not answer is reported before the path is
/// touched (a stat of a dead hard NFS mount would hang).
#[tokio::test]
async fn an_unresponsive_mount_is_reported_without_touching_the_path() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    n.host.0.lock().unwrap().failing.push("findmnt --mountpoint".into());
    // A path that does not exist: had it been stat'ed, the answer would be NotFound.
    let err = node_get_volume_stats(&n.state, &stats_request(&n.path("gone")), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(err.message().contains("mount unresponsive"), "{}", err.message());
}

/// A raw-block volume reports its device's size, by device number. Needs a
/// block device on the test host.
#[tokio::test]
async fn block_stats_come_from_the_device_number() {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let Some(device) = std::fs::read_dir("/dev").ok().and_then(|entries| {
        entries
            .flatten()
            .map(|e| e.path())
            .find(|p| std::fs::metadata(p).is_ok_and(|m| m.file_type().is_block_device()))
    }) else {
        eprintln!("no block device under /dev; skipping");
        return;
    };
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let rdev = std::fs::metadata(&device).unwrap().rdev();
    let dir = n
        .dir
        .path()
        .join(format!("sys/dev/block/{}:{}", libc::major(rdev), libc::minor(rdev)));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("size"), "2048\n").unwrap();
    let resp = node_get_volume_stats(&n.state, &stats_request(device.to_str().unwrap()), None)
        .await
        .unwrap();
    assert_eq!(resp.usage.len(), 1);
    assert_eq!(resp.usage[0].total, 1 << 20);
    assert_eq!(resp.usage[0].unit, Unit::Bytes as i32);
}

#[tokio::test]
async fn a_ublk_filesystem_grows_when_the_device_covers_the_request() {
    let (n, staging) = staged(filesystem()).await;
    let device = n.daemon.device_path(0);
    set_size(&n, "ublkb0", 20 << 30);
    let path = n.path("pods/p1/volumes/mount");
    std::fs::create_dir_all(&path).unwrap();
    n.host.mount(&path, &device, "ext4");

    let resp = node_expand_volume(&n.state, &expand_request(&path, &staging, 20 << 30, filesystem()), None)
        .await
        .unwrap();
    assert_eq!(resp.capacity_bytes, 20 << 30);
    assert!(
        n.host.calls().contains(&format!("resize2fs {device}")),
        "{:?}",
        n.host.calls()
    );
}

#[tokio::test]
async fn a_live_ublk_device_cannot_grow() {
    let (n, staging) = staged(filesystem()).await;
    let device = n.daemon.device_path(0);
    set_size(&n, "ublkb0", 10 << 30);
    let path = n.path("pods/p1/volumes/mount");
    std::fs::create_dir_all(&path).unwrap();
    n.host.mount(&path, &device, "ext4");

    let err = node_expand_volume(&n.state, &expand_request(&path, &staging, 20 << 30, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("next staged"), "{}", err.message());
    assert!(
        !n.host.calls().iter().any(|c| c.starts_with("resize2fs")),
        "no filesystem resize on a device that did not grow"
    );
}

#[tokio::test]
async fn raw_block_expansion_checks_ownership_and_never_resizes() {
    let (n, staging) = staged(block()).await;
    set_size(&n, "ublkb0", 20 << 30);
    std::fs::create_dir_all(n.path("pods/p1/volumeDevices")).unwrap();
    let path = n.path("pods/p1/volumeDevices/dev");
    std::fs::write(&path, b"").unwrap();
    let resp = node_expand_volume(&n.state, &expand_request(&path, &staging, 20 << 30, block()), None)
        .await
        .unwrap();
    assert_eq!(resp.capacity_bytes, 20 << 30);
    assert!(!n.host.calls().iter().any(|c| c.starts_with("resize2fs")));

    // The device now serves another volume.
    {
        let mut daemon = n.daemon.state.lock().unwrap();
        let mut device = daemon.devices.remove(VOLUME).unwrap();
        device.volume = "pvc-other".into();
        daemon.devices.insert("pvc-other".into(), device);
    }
    let err = node_expand_volume(&n.state, &expand_request(&path, &staging, 20 << 30, block()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn expansion_of_other_devices() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let path = n.path("vol");
    std::fs::create_dir_all(&path).unwrap();
    // No device at all on an NVMe-oF install.
    let err = node_expand_volume(&n.state, &expand_request(&path, "", 1 << 30, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(err.message().contains("failed to resolve block device"));

    // A SCSI disk (iSCSI): not served yet, and untouched.
    let kernel = n.daemon.dev_dir.path().join("sdb");
    std::fs::write(&kernel, b"").unwrap();
    n.host.mount(&path, kernel.to_str().unwrap(), "ext4");
    let err = node_expand_volume(&n.state, &expand_request(&path, "", 1 << 30, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("does not serve"), "{}", err.message());
    assert!(
        !n.host
            .calls()
            .iter()
            .any(|c| c.starts_with("resize2fs") || c.starts_with("nvme"))
    );

    // An NFS install has nothing to grow on the node.
    let nfs = node("nvmeof: {}\n", HOST_NQN, |state| {
        state.driver_name = "org.scale.csi.nfs".into()
    });
    let nfs_path = nfs.path("vol");
    std::fs::create_dir_all(&nfs_path).unwrap();
    let resp = node_expand_volume(&nfs.state, &expand_request(&nfs_path, "", 5 << 30, filesystem()), None)
        .await
        .unwrap();
    assert_eq!(resp.capacity_bytes, 5 << 30);
}

#[tokio::test]
async fn expansion_requests_are_validated() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let mut req = expand_request(&n.path("vol"), "", 0, filesystem());
    assert_eq!(
        node_expand_volume(&n.state, &req, None).await.unwrap_err().code(),
        Code::NotFound
    );
    req.volume_id.clear();
    assert_eq!(
        node_expand_volume(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = expand_request("", "", 0, filesystem());
    assert_eq!(
        node_expand_volume(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    req.volume_path = n.path("vol");
    let mut stats = stats_request("");
    assert_eq!(
        node_get_volume_stats(&n.state, &stats, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    stats.volume_id.clear();
    stats.volume_path = "/x".into();
    assert_eq!(
        node_get_volume_stats(&n.state, &stats, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
}
