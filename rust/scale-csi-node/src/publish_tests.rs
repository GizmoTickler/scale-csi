//! NodePublishVolume and NodeUnpublishVolume against the fakes, on volumes
//! staged through nvmeublkd.

use tonic::Code;

use crate::csi::{self, volume_capability};
use crate::publish::{REASON_MOUNT_FAILED, node_publish, node_unpublish};
use crate::stage::node_stage;
use crate::testing::{HOST_NQN, NQN, Node, UBLK_ON, VOLUME, block, context, exists, filesystem, node};

fn with_mode(
    mut capability: csi::VolumeCapability,
    mode: volume_capability::access_mode::Mode,
) -> csi::VolumeCapability {
    capability.access_mode = Some(volume_capability::AccessMode { mode: mode as i32 });
    capability
}

fn publish_request(
    n: &Node,
    staging: &str,
    pod: &str,
    capability: csi::VolumeCapability,
) -> csi::NodePublishVolumeRequest {
    csi::NodePublishVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.into(),
        target_path: n.path(&format!("pods/{pod}/volumes/kubernetes.io~csi/pv/mount")),
        volume_capability: Some(capability),
        volume_context: context(&[]),
        ..Default::default()
    }
}

/// A ublk node with the volume staged; returns the staging path.
async fn staged(capability: csi::VolumeCapability) -> (Node, String) {
    // Devices "appear" under the daemon's directory; the node's /dev is that.
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

fn mounts_of(n: &Node, target: &str) -> usize {
    n.host
        .calls()
        .iter()
        .filter(|c| c.starts_with("mount ") && c.ends_with(target))
        .count()
}

#[tokio::test]
async fn filesystem_publish_and_replay() {
    let (n, staging) = staged(filesystem()).await;
    let req = publish_request(&n, &staging, "p1", filesystem());
    node_publish(&n.state, &req, None).await.unwrap();
    assert!(
        n.host
            .calls()
            .contains(&format!("mount -o bind {staging} {}", req.target_path)),
        "{:?}",
        n.host.calls()
    );
    let record = n.state.records.publication(&req.target_path).unwrap();
    assert_eq!(record.live_source, n.daemon.device_path(0));

    // The same request again: no second mount.
    node_publish(&n.state, &req, None).await.unwrap();
    assert_eq!(mounts_of(&n, &req.target_path), 1);

    // The same target asked for read-only, or for another volume: refused.
    let mut readonly = req.clone();
    readonly.readonly = true;
    assert_eq!(
        node_publish(&n.state, &readonly, None).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    let mut other = req.clone();
    other.volume_id = "pvc-other".into();
    assert_eq!(
        node_publish(&n.state, &other, None).await.unwrap_err().code(),
        Code::AlreadyExists
    );

    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: req.target_path.clone(),
        },
        None,
    )
    .await
    .unwrap();
    assert!(!n.host.is_mounted(&req.target_path));
    assert!(!exists(&req.target_path), "unpublish removes the target");
    assert!(n.state.records.publication(&req.target_path).is_none());
}

#[tokio::test]
async fn read_only_publish_remounts_read_only() {
    let (n, staging) = staged(filesystem()).await;
    let mut req = publish_request(&n, &staging, "p1", filesystem());
    req.readonly = true;
    if let Some(volume_capability::AccessType::Mount(m)) = &mut req.volume_capability.as_mut().unwrap().access_type {
        m.mount_flags = vec!["noatime".into(), "noatime".into()];
    }
    node_publish(&n.state, &req, None).await.unwrap();
    let calls = n.host.calls();
    assert!(
        calls.contains(&format!(
            "mount -o bind,ro,noatime,noatime {staging} {}",
            req.target_path
        )),
        "the request's flags as given, after ro: {calls:?}"
    );
    assert!(calls.contains(&format!("mount -o remount,bind,ro {}", req.target_path)));
    node_publish(&n.state, &req, None)
        .await
        .expect("a read-only replay matches the read-only mount");
}

#[tokio::test]
async fn a_single_writer_volume_gets_one_target() {
    let mode = volume_capability::access_mode::Mode::SingleNodeSingleWriter;
    let (n, staging) = staged(with_mode(filesystem(), mode)).await;
    let first = publish_request(&n, &staging, "p1", with_mode(filesystem(), mode));
    node_publish(&n.state, &first, None).await.unwrap();
    let second = publish_request(&n, &staging, "p2", with_mode(filesystem(), mode));
    let err = node_publish(&n.state, &second, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("already published at different target path"),
        "{}",
        err.message()
    );

    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: first.target_path.clone(),
        },
        None,
    )
    .await
    .unwrap();
    node_publish(&n.state, &second, None)
        .await
        .expect("the second target is allowed once the first is gone");
}

#[tokio::test]
async fn a_multi_writer_volume_gets_several_targets() {
    let mode = volume_capability::access_mode::Mode::SingleNodeMultiWriter;
    let (n, staging) = staged(with_mode(filesystem(), mode)).await;
    for pod in ["p1", "p2"] {
        node_publish(
            &n.state,
            &publish_request(&n, &staging, pod, with_mode(filesystem(), mode)),
            None,
        )
        .await
        .unwrap();
    }
}

/// After a restart the records are gone; the mount table still shows the
/// first publication and blocks a second single-writer target.
#[tokio::test]
async fn the_mount_table_blocks_a_second_target_after_a_restart() {
    let (n, staging) = staged(filesystem()).await;
    let device = n.daemon.device_path(0);
    let old_pod = n.path("pods/old/volumes/kubernetes.io~csi/pv/mount");
    n.host.mount(&old_pod, &device, "ext4");
    let err = node_publish(&n.state, &publish_request(&n, &staging, "new", filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains(&old_pod), "{}", err.message());
}

#[tokio::test]
async fn raw_block_publish_checks_ownership_through_the_daemon() {
    let (n, staging) = staged(block()).await;
    let req = publish_request(&n, &staging, "p1", block());
    node_publish(&n.state, &req, None).await.unwrap();
    let device = n.daemon.device_path(0);
    assert!(
        n.host
            .calls()
            .contains(&format!("mount -o bind {device} {}", req.target_path)),
        "{:?}",
        n.host.calls()
    );
    assert!(
        std::fs::metadata(&req.target_path).unwrap().is_file(),
        "the placeholder the device is bound onto"
    );
    node_publish(&n.state, &req, None)
        .await
        .expect("a raw-block replay is idempotent");
    assert_eq!(mounts_of(&n, &req.target_path), 1);

    // The device now serves another volume (or subsystem): refused.
    for (volume, subnqn) in [("pvc-other", NQN), (VOLUME, "nqn.2011-06.com.example:pvc-other")] {
        let n2 = node(UBLK_ON, HOST_NQN, |_| {});
        let staging2 = n2.path("staging/dev");
        std::fs::create_dir_all(n2.path("staging")).unwrap();
        let device2 = n2.daemon.device_path(4);
        std::fs::write(&device2, b"").unwrap();
        n2.daemon.insert(volume, subnqn, 4, &device2);
        std::os::unix::fs::symlink(&device2, &staging2).unwrap();
        let err = node_publish(&n2.state, &publish_request(&n2, &staging2, "p1", block()), None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{volume} {subnqn}: {err:?}");
    }
}

#[tokio::test]
async fn a_raw_block_publish_needs_a_staged_device() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let mut req = publish_request(&n, "", "p1", block());
    assert_eq!(
        node_publish(&n.state, &req, None).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    req.staging_target_path = n.path("not-a-link");
    assert_eq!(
        node_publish(&n.state, &req, None).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    // Without a staging path only NFS publishes (nfs_tests.rs).
    let fs = publish_request(&n, "", "p1", filesystem());
    let err = node_publish(&n.state, &fs, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(err.message(), "staging path required for block volumes");
}

#[tokio::test]
async fn a_failed_bind_mount_is_reported() {
    let (n, staging) = staged(filesystem()).await;
    let mut req = publish_request(&n, &staging, "p1", filesystem());
    req.volume_context
        .insert("csi.storage.k8s.io/pod.namespace".into(), "apps".into());
    req.volume_context
        .insert("csi.storage.k8s.io/pod.name".into(), "web-0".into());
    n.host.0.lock().unwrap().failing.push("mount -o bind".into());
    let err = node_publish(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(err.message().contains("failed to bind mount"), "{}", err.message());
    let events = n.events.take();
    assert!(
        events.iter().any(|(_, reason, _)| reason == REASON_MOUNT_FAILED),
        "{events:?}"
    );
    assert!(n.state.records.publication(&req.target_path).is_none());
}

#[tokio::test]
async fn unpublish_of_nothing_succeeds() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: n.path("pods/gone/volumes/x/mount"),
        },
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_busy_target_is_aborted() {
    let (n, staging) = staged(filesystem()).await;
    let req = publish_request(&n, &staging, "p1", filesystem());
    let _held = n
        .state
        .locks
        .try_lock(crate::locks::node_target_key(&req.target_path))
        .unwrap();
    let err = node_publish(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert_eq!(err.message(), "target path operation already in progress");
}

#[tokio::test]
async fn requests_are_validated() {
    let n = node(UBLK_ON, HOST_NQN, |_| {});
    let base = publish_request(&n, "", "p1", filesystem());
    let mut req = base.clone();
    req.volume_id.clear();
    assert_eq!(
        node_publish(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = base.clone();
    req.target_path.clear();
    assert_eq!(
        node_publish(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = base;
    req.volume_capability = None;
    assert_eq!(
        node_publish(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    for (volume, target) in [("", "/t"), ("v", "")] {
        let err = node_unpublish(
            &n.state,
            &csi::NodeUnpublishVolumeRequest {
                volume_id: volume.into(),
                target_path: target.into(),
            },
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}

/// A live raw-block publication, seen through its bind mount, is a block
/// device: still a raw-block target. (The Go node accepts only a regular
/// file there.) Needs a block device on the test host.
#[test]
fn a_bound_device_node_is_a_raw_block_target() {
    let Some(device) = std::fs::read_dir("/dev").ok().and_then(|entries| {
        entries.flatten().map(|e| e.path()).find(|p| {
            use std::os::unix::fs::FileTypeExt;
            std::fs::metadata(p).is_ok_and(|m| m.file_type().is_block_device())
        })
    }) else {
        eprintln!("no block device under /dev; skipping");
        return;
    };
    assert_eq!(
        crate::publish::access_type_at_path(device.to_str().unwrap()),
        Ok(crate::capability::AccessType::Block)
    );
}

/// Without a record (the agent restarted), the live mount alone decides: a
/// read-only mismatch or another source is refused.
#[tokio::test]
async fn a_replay_without_a_record_is_checked_against_the_live_mount() {
    let (n, staging) = staged(filesystem()).await;
    let req = publish_request(&n, &staging, "p1", filesystem());
    node_publish(&n.state, &req, None).await.unwrap();
    n.state.records.delete_publication(&req.target_path);
    let mut readonly = req.clone();
    readonly.readonly = true;
    let err = node_publish(&n.state, &readonly, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(err.message().contains("readonly state"), "{}", err.message());

    n.state.records.delete_publication(&req.target_path);
    n.host.mount(&req.target_path, "/dev/ublkb9", "ext4");
    let err = node_publish(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(err.message().contains("is backed by /dev/ublkb9"), "{}", err.message());
}

/// Without its record (the agent restarted), a single-writer raw-block volume
/// still bound at another pod's target is found in the mount table by device
/// number: its source there is devtmpfs, never the device. A network mount is
/// never stat'ed on the way.
#[tokio::test]
async fn a_raw_block_volume_bound_elsewhere_blocks_a_second_target_after_a_restart() {
    let (n, staging) = staged(block()).await;
    n.host.mount(
        &n.path("pods/p9/volumes/kubernetes.io~csi/share/mount"),
        "nas:/export",
        "nfs4",
    );
    let first = publish_request(&n, &staging, "p1", block());
    node_publish(&n.state, &first, None).await.unwrap();
    n.state.records.delete_publication(&first.target_path);
    let second = publish_request(&n, &staging, "p2", block());
    let err = node_publish(&n.state, &second, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains(&first.target_path), "{}", err.message());
}

/// A recorded publication at a target the mount-table scan would not
/// recognise as a pod's still blocks a second single-writer target.
#[tokio::test]
async fn a_recorded_publication_blocks_a_second_target() {
    let (n, staging) = staged(filesystem()).await;
    let mut first = publish_request(&n, &staging, "p1", filesystem());
    first.target_path = n.path("elsewhere/one");
    node_publish(&n.state, &first, None).await.unwrap();
    let mut second = first.clone();
    second.target_path = n.path("elsewhere/two");
    let err = node_publish(&n.state, &second, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
}

/// After a restart there is no record, and the mount table shows the bound
/// device node's source as devtmpfs (`udev[/dev/...]`): the replay matches the
/// target to the staged device by device number.
#[tokio::test]
async fn a_raw_block_replay_after_a_restart_matches_by_device_number() {
    let (n, staging) = staged(block()).await;
    let req = publish_request(&n, &staging, "p1", block());
    node_publish(&n.state, &req, None).await.unwrap();
    n.state.records.delete_publication(&req.target_path);
    node_publish(&n.state, &req, None)
        .await
        .expect("the bound device is the staged one");
    assert_eq!(mounts_of(&n, &req.target_path), 1);

    // Bound to another device: refused.
    n.state.records.delete_publication(&req.target_path);
    n.host
        .mount_with(&req.target_path, "udev[/dev/ublkb9]", "devtmpfs", "rw");
    let err = node_publish(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err:?}");
}
