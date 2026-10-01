//! iSCSI through every node RPC, against the fake initiator: stage, replay,
//! publish, unpublish, expand, stats and unstage, for filesystem and raw-block
//! volumes, single portal and multipath, with and without CHAP; session reuse,
//! session GC, and volumes staged by the Go node.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use tokio::sync::watch;
use tonic::Code;

use crate::capacity::{node_expand_volume, node_get_volume_stats};
use crate::csi::{self, volume_usage::Unit};
use crate::iscsi_stage::{REASON_CHAP_FAILED, REASON_LOGIN_FAILED, REASON_MULTIPATH_UNAVAILABLE, REASON_PATH_DEGRADED};
use crate::iscsi_testing::{
    CONFIG, HINT, IQN, MULTIPATH_CONFIG, PASSWORD, PORTAL, PORTAL_B, PORTAL_C, USER, VOLUME, WWID, fake, iscsi_calls,
    iscsi_node, iscsi_node_with,
};
use crate::publish::{REASON_MOUNT_FAILED, node_publish, node_unpublish};
use crate::session_gc::{gc_iscsi, pass};
use crate::stage::{node_stage, node_unstage};
use crate::testing::{Node, block, exists, filesystem};

fn context(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut c: HashMap<String, String> = [
        ("node_attach_driver", "iscsi"),
        ("portal", PORTAL),
        ("iqn", IQN),
        ("lun", "0"),
        ("csi.storage.k8s.io/pvc/namespace", "apps"),
        ("csi.storage.k8s.io/pvc/name", "data"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (k, v) in extra {
        c.insert(k.to_string(), v.to_string());
    }
    c
}

fn stage_request(n: &Node, capability: csi::VolumeCapability) -> csi::NodeStageVolumeRequest {
    csi::NodeStageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: n.path("staging/globalmount"),
        volume_capability: Some(capability),
        volume_context: context(&[]),
        ..Default::default()
    }
}

fn with_hint(mut req: csi::NodeStageVolumeRequest, hint: &str) -> csi::NodeStageVolumeRequest {
    req.publish_context.insert("portals".into(), hint.into());
    req
}

fn with_chap(
    mut req: csi::NodeStageVolumeRequest,
    mode: &str,
    secrets: &[(&str, &str)],
) -> csi::NodeStageVolumeRequest {
    req.volume_context.insert("chap".into(), mode.into());
    req.secrets = secrets.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    req
}

fn unstage_request(n: &Node) -> csi::NodeUnstageVolumeRequest {
    csi::NodeUnstageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: n.path("staging/globalmount"),
    }
}

fn publish_request(n: &Node, capability: csi::VolumeCapability) -> csi::NodePublishVolumeRequest {
    csi::NodePublishVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: n.path("staging/globalmount"),
        target_path: n.path("pods/p1/volumes/kubernetes.io~csi/pv/mount"),
        volume_capability: Some(capability),
        volume_context: context(&[]),
        ..Default::default()
    }
}

fn logins(n: &Node) -> Vec<String> {
    iscsi_calls(n).into_iter().filter(|c| c.ends_with("--login")).collect()
}

fn logouts(n: &Node) -> Vec<String> {
    iscsi_calls(n).into_iter().filter(|c| c.ends_with("--logout")).collect()
}

fn sessions(n: &Node) -> usize {
    fake(n, |f| f.sessions.len())
}

fn events(n: &Node, reason: &str) -> Vec<String> {
    n.events
        .take()
        .into_iter()
        .filter(|(_, r, _)| r == reason)
        .map(|(_, _, m)| m)
        .collect()
}

#[tokio::test]
async fn filesystem_volume_through_every_rpc() {
    let n = iscsi_node(CONFIG);
    let req = stage_request(&n, filesystem());
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        iscsi_calls(&n),
        [
            "iscsiadm -m session".to_string(),
            format!("iscsiadm -m node -o new -T {IQN} -p {PORTAL}"),
            format!("iscsiadm -m node -T {IQN} -p {PORTAL} --login"),
            "iscsiadm -m session".to_string(),
            "iscsiadm -m session".to_string(),
        ],
        "the Go node's fast path: a static record, a login, the device by session, then the stage verified"
    );
    let device = fake(&n, |f| f.device_of(PORTAL).unwrap());
    assert!(n.host.calls().contains(&format!("mkfs.ext4 -F {device}")));
    let staging = req.staging_target_path.clone();
    assert!(n.host.is_mounted(&staging));
    assert_eq!(n.state.metrics.node_connects("iscsi", "success"), 1);
    assert_eq!(
        n.state.records.stage(&staging).unwrap().expected_source,
        format!("iscsi:{IQN}")
    );

    // A replay: identified through sysfs, nothing logged in.
    let before = logins(&n).len();
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(logins(&n).len(), before);

    // Publish, replay, stats, unpublish.
    let publish = publish_request(&n, filesystem());
    node_publish(&n.state, &publish, None).await.unwrap();
    node_publish(&n.state, &publish, None).await.unwrap();
    let target = publish.target_path.clone();
    let stats = node_get_volume_stats(
        &n.state,
        &csi::NodeGetVolumeStatsRequest {
            volume_id: VOLUME.into(),
            volume_path: target.clone(),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(stats.usage.len(), 2);
    assert_eq!(stats.usage[1].unit, Unit::Inodes as i32);
    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: target.clone(),
        },
        None,
    )
    .await
    .unwrap();
    assert!(!n.host.is_mounted(&target));

    // Unstage: unmounted, logged out by the device read from the mount, and
    // the record deleted; a replay finds nothing to do.
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert!(!n.host.is_mounted(&staging) && !exists(&staging));
    assert_eq!(logouts(&n), [format!("iscsiadm -m node -T {IQN} -p {PORTAL} --logout")]);
    assert!(iscsi_calls(&n).contains(&format!("iscsiadm -m node -T {IQN} -p {PORTAL} -o delete")));
    assert_eq!(sessions(&n), 0);
    assert!(fake(&n, |f| f.record(IQN, PORTAL).is_none()));
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(logouts(&n).len(), 1);
}

#[tokio::test]
async fn raw_block_volume_through_every_rpc() {
    let n = iscsi_node(CONFIG);
    let req = stage_request(&n, block());
    node_stage(&n.state, &req, None).await.unwrap();
    let device = fake(&n, |f| f.device_of(PORTAL).unwrap());
    let staging = req.staging_target_path.clone();
    assert_eq!(std::fs::read_link(&staging).unwrap().to_string_lossy(), device);
    // Replay: the link and the session are the volume's.
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(logins(&n).len(), 1);

    let publish = publish_request(&n, block());
    node_publish(&n.state, &publish, None).await.unwrap();
    assert!(
        n.host
            .calls()
            .contains(&format!("mount -o bind {device} {}", publish.target_path))
    );
    node_publish(&n.state, &publish, None).await.unwrap();

    // Expansion of a raw-block volume: rescan through the session's portal.
    let size = n
        .dir
        .path()
        .join(format!("sys/class/block/{}/size", device.rsplit('/').next().unwrap()));
    fake(&n, |f| f.on_rescan = Some((size.clone(), "4194304\n".into())));
    let expanded = node_expand_volume(
        &n.state,
        &csi::NodeExpandVolumeRequest {
            volume_id: VOLUME.into(),
            volume_path: publish.target_path.clone(),
            staging_target_path: staging.clone(),
            capacity_range: Some(csi::CapacityRange {
                required_bytes: 2 << 30,
                limit_bytes: 0,
            }),
            volume_capability: Some(block()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(expanded.capacity_bytes, 2 << 30);
    assert!(iscsi_calls(&n).contains(&format!("iscsiadm -m node -T {IQN} -p {PORTAL} --rescan")));
    assert!(
        !n.host.calls().iter().any(|c| c.starts_with("resize2fs")),
        "raw block never resizes"
    );

    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: publish.target_path.clone(),
        },
        None,
    )
    .await
    .unwrap();
    // Unstage of a link: the session is found by the volume's target name,
    // never the literal device of the link.
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert!(!exists(&staging));
    assert_eq!(logouts(&n).len(), 1);
    assert_eq!(sessions(&n), 0);
}

/// A raw-block device is published only for the volume its target is named for.
#[tokio::test]
async fn raw_block_ownership_is_checked() {
    let n = iscsi_node(CONFIG);
    node_stage(&n.state, &stage_request(&n, block()), None).await.unwrap();
    let mut publish = publish_request(&n, block());
    publish.volume_id = "pvc-other".into();
    let err = node_publish(&n.state, &publish, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("expected volume target pvc-other"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn filesystem_expansion_rescans_then_resizes() {
    let n = iscsi_node(CONFIG);
    let req = stage_request(&n, filesystem());
    node_stage(&n.state, &req, None).await.unwrap();
    let device = fake(&n, |f| f.device_of(PORTAL).unwrap());
    n.host
        .0
        .lock()
        .unwrap()
        .filesystems
        .insert(device.clone(), "ext4".into());
    let size = n
        .dir
        .path()
        .join(format!("sys/class/block/{}/size", device.rsplit('/').next().unwrap()));
    fake(&n, |f| f.on_rescan = Some((size, "4194304\n".into())));
    let resp = node_expand_volume(
        &n.state,
        &csi::NodeExpandVolumeRequest {
            volume_id: VOLUME.into(),
            volume_path: req.staging_target_path.clone(),
            staging_target_path: req.staging_target_path.clone(),
            capacity_range: Some(csi::CapacityRange {
                required_bytes: 2 << 30,
                limit_bytes: 0,
            }),
            volume_capability: Some(filesystem()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(resp.capacity_bytes, 2 << 30);
    let calls = n.host.calls();
    let rescan = calls.iter().position(|c| c.ends_with("--rescan")).unwrap();
    let resize = calls.iter().position(|c| *c == format!("resize2fs {device}")).unwrap();
    assert!(rescan < resize, "{calls:?}");
}

#[tokio::test]
async fn chap_credentials_go_to_the_record_never_to_argv_or_the_log() {
    let n = iscsi_node_with(CONFIG, |f| {
        f.targets.get_mut(IQN).unwrap().chap = Some((USER.into(), PASSWORD.into()));
    });
    let req = with_chap(
        stage_request(&n, filesystem()),
        "CHAP",
        &[("username", USER), ("password", PASSWORD)],
    );
    node_stage(&n.state, &req, None).await.unwrap();
    let record_path = fake(&n, |f| f.record_path(IQN, PORTAL));
    let record = std::fs::read_to_string(&record_path).unwrap();
    assert!(
        record.contains(&format!("node.session.auth.password = {PASSWORD}\n")),
        "{record}"
    );
    assert!(
        record.contains(&format!("node.session.auth.username = {USER}\n")),
        "{record}"
    );
    assert!(record.contains("node.session.auth.authmethod = CHAP\n"), "{record}");
    assert_eq!(
        std::fs::metadata(&record_path).unwrap().permissions().mode() & 0o777,
        0o600,
        "a record holding a password is 0600"
    );
    for call in n.host.calls() {
        assert!(!call.contains(PASSWORD), "a password reached argv: {call}");
    }
    // The order: the record, the method and user on argv, then the login.
    let calls = iscsi_calls(&n);
    let update = calls
        .iter()
        .position(|c| c.contains("-n node.session.auth.username -v"))
        .unwrap();
    let login = calls.iter().position(|c| c.ends_with("--login")).unwrap();
    assert!(update < login, "{calls:?}");
}

#[tokio::test]
async fn mutual_chap() {
    let n = iscsi_node(CONFIG);
    let req = with_chap(
        stage_request(&n, block()),
        "CHAP_MUTUAL",
        &[
            ("username", USER),
            ("password", PASSWORD),
            ("mutualUsername", "target-user"),
            ("mutualPassword", "s3cret-Mutual1"), // gitleaks:allow (test fixture)
        ],
    );
    node_stage(&n.state, &req, None).await.unwrap();
    let record = fake(&n, |f| f.record(IQN, PORTAL).unwrap());
    assert!(
        record.contains("node.session.auth.password_in = s3cret-Mutual1\n"), // gitleaks:allow (test fixture)
        "{record}"
    );
    assert!(
        record.contains("node.session.auth.username_in = target-user\n"),
        "{record}"
    );
    assert!(n.host.calls().iter().all(|c| !c.contains("s3cret")));
}

/// A rejected secret is Unauthenticated at once: no discovery storm.
#[tokio::test]
async fn a_rejected_chap_secret_is_unauthenticated_and_not_retried() {
    let n = iscsi_node_with(CONFIG, |f| {
        f.targets.get_mut(IQN).unwrap().chap = Some((USER.into(), "another-Pass12".into()));
    });
    let req = with_chap(
        stage_request(&n, filesystem()),
        "CHAP",
        &[("username", USER), ("password", PASSWORD)],
    );
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    assert_eq!(err.message(), format!("iSCSI CHAP authentication failed for {IQN}"));
    assert_eq!(fake(&n, |f| f.discoveries), 0, "no discovery after a CHAP rejection");
    assert_eq!(logins(&n).len(), 1);
    assert_eq!(n.state.metrics.node_connects("iscsi", "error"), 1);
    let failed = events(&n, REASON_LOGIN_FAILED);
    assert_eq!(failed, [format!("iSCSI CHAP authentication failed for {IQN}")]);
}

#[tokio::test]
async fn a_chap_volume_without_a_usable_secret_logs_into_nothing() {
    let n = iscsi_node(CONFIG);
    let req = with_chap(
        stage_request(&n, filesystem()),
        "CHAP",
        &[("username", USER), ("password", "short")],
    );
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("12-16 characters"), "{}", err.message());
    assert!(logins(&n).is_empty());
}

/// CHAP that cannot be written to a node record fails the stage with
/// ISCSICHAPFailed, naming the parameter, never the value.
#[tokio::test]
async fn a_chap_record_that_cannot_be_written_fails_closed() {
    let n = iscsi_node(CONFIG);
    // The record is created somewhere the agent does not look.
    fake(&n, |f| f.db = n.dir.path().join("elsewhere"));
    let req = with_chap(
        stage_request(&n, filesystem()),
        "CHAP",
        &[("username", USER), ("password", PASSWORD)],
    );
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert_eq!(err.message(), format!("failed to configure iSCSI CHAP for {IQN}"));
    let failed = events(&n, REASON_CHAP_FAILED);
    assert_eq!(failed.len(), 1);
    assert!(
        failed[0].starts_with("CHAP configuration failed: ") && failed[0].contains("no iSCSI node record found"),
        "{failed:?}"
    );
    assert!(!failed[0].contains(PASSWORD));
    assert!(logins(&n).is_empty(), "never logged in without the credential");
}

/// A target TrueNAS has not published yet: the fast path finds no record, a
/// SendTargets discovery does, the login is retried.
#[tokio::test]
async fn a_target_not_found_falls_back_to_discovery() {
    let n = iscsi_node_with(CONFIG, |f| f.targets.get_mut(IQN).unwrap().hidden = true);
    node_stage(&n.state, &stage_request(&n, block()), None).await.unwrap();
    assert_eq!(fake(&n, |f| f.discoveries), 1);
    assert_eq!(logins(&n).len(), 2);
    assert!(iscsi_calls(&n).contains(&format!("iscsiadm -m discovery -t sendtargets -p {PORTAL}")));
}

/// Single portal: a session of the target left behind (a crashed unstage, a
/// move between nodes) is logged out before the login, as in the Go node.
#[tokio::test]
async fn a_leftover_session_is_replaced() {
    let n = iscsi_node(CONFIG);
    fake(&n, |f| f.add_session(PORTAL, IQN));
    node_stage(&n.state, &stage_request(&n, filesystem()), None)
        .await
        .unwrap();
    let calls = iscsi_calls(&n);
    let logout = calls.iter().position(|c| c.ends_with("--logout")).unwrap();
    let login = calls.iter().position(|c| c.ends_with("--login")).unwrap();
    assert!(logout < login, "{calls:?}");
    assert_eq!(sessions(&n), 1);
}

/// A multipath replay reuses each healthy session (its device shows up) and
/// replaces a stale one (a session whose device never appears).
#[tokio::test]
async fn healthy_sessions_are_reused_and_a_stale_one_replaced() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| f.multipathd = true);
    let req = with_hint(stage_request(&n, block()), HINT);
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(logins(&n).len(), 3);
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(logins(&n).len(), 3, "every session reused: {:?}", iscsi_calls(&n));
    assert!(logouts(&n).is_empty());

    let stale = fake(&n, |f| f.device_of(PORTAL_B).unwrap());
    std::fs::remove_file(&stale).unwrap();
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        logouts(&n),
        [format!("iscsiadm -m node -T {IQN} -p {PORTAL_B} --logout")]
    );
    assert_eq!(logins(&n).len(), 4);
    assert_eq!(sessions(&n), 3);
}

/// The pre-emptive logout never takes the session of a staged device that is
/// still live (Go preemptiveSessionDisconnect's guard; node_stage's own
/// replay check normally answers first, so the transport stage is driven
/// directly here).
#[tokio::test]
async fn a_live_staged_device_keeps_its_session() {
    let n = iscsi_node(CONFIG);
    let device = fake(&n, |f| f.add_session(PORTAL, IQN));
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(n.path("staging")).unwrap();
    std::os::unix::fs::symlink(&device, &staging).unwrap();
    let context = context(&[]);
    crate::iscsi_stage::stage(
        &n.state,
        crate::iscsi_stage::StageRequest {
            context: &context,
            secrets: &HashMap::new(),
            staging: &staging,
            capability: &filesystem(),
            event: None,
            deadline: None,
        },
    )
    .await
    .unwrap();
    assert!(logouts(&n).is_empty(), "{:?}", iscsi_calls(&n));
    assert!(logins(&n).is_empty(), "the live session is reused");
}

#[tokio::test]
async fn multipath_logs_into_every_portal_and_stages_the_map() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| {
        f.multipathd = true;
        f.unreachable = vec![PORTAL_C.into()];
    });
    let req = with_hint(stage_request(&n, block()), HINT);
    node_stage(&n.state, &req, None).await.unwrap();
    let map = fake(&n, |f| f.map_of(WWID).unwrap());
    assert_eq!(
        std::fs::canonicalize(&req.staging_target_path).unwrap(),
        std::fs::canonicalize(&map).unwrap(),
        "the stage is the dm map, not a path"
    );
    assert_eq!(logins(&n).len(), 3);
    for (portal, result) in [(PORTAL, "success"), (PORTAL_B, "success"), (PORTAL_C, "error")] {
        assert_eq!(n.state.metrics.iscsi_path_connects(portal, result), 1, "{portal}");
    }
    let degraded = events(&n, REASON_PATH_DEGRADED);
    assert_eq!(degraded.len(), 1);
    assert!(degraded[0].contains(PORTAL_C), "{degraded:?}");

    // The replay tops up the path that came back, through the live map.
    fake(&n, |f| f.unreachable.clear());
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(n.state.metrics.iscsi_path_connects(PORTAL_C, "success"), 1);
    assert_eq!(sessions(&n), 3);

    // Unstage logs out every portal's session.
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(logouts(&n).len(), 3);
    assert_eq!(sessions(&n), 0);
}

/// A path that reaches another LUN is logged out again and reported.
#[tokio::test]
async fn a_path_to_another_lun_is_logged_out() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| {
        f.multipathd = true;
        f.foreign_wwid_on = Some(PORTAL_C.into());
    });
    let req = with_hint(stage_request(&n, filesystem()), HINT);
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        logouts(&n),
        [format!("iscsiadm -m node -T {IQN} -p {PORTAL_C} --logout")]
    );
    assert_eq!(n.state.metrics.iscsi_path_connects(PORTAL_C, "error"), 1);
    let degraded = events(&n, REASON_PATH_DEGRADED);
    assert!(degraded[0].contains("differs from primary"), "{degraded:?}");
    let map = fake(&n, |f| f.map_of(WWID).unwrap());
    assert!(n.host.calls().contains(&format!("mkfs.ext4 -F {map}")));
}

/// Without multipathd the node stays single path: no secondary sessions that
/// could race a raw-device mount.
#[tokio::test]
async fn multipath_without_multipathd_is_single_path() {
    let n = iscsi_node(MULTIPATH_CONFIG);
    node_stage(&n.state, &with_hint(stage_request(&n, block()), HINT), None)
        .await
        .unwrap();
    assert_eq!(logins(&n), [format!("iscsiadm -m node -T {IQN} -p {PORTAL} --login")]);
    let unavailable = events(&n, REASON_MULTIPATH_UNAVAILABLE);
    assert!(
        unavailable[0].contains("staging through the primary portal only"),
        "{unavailable:?}"
    );
}

/// No dm map appears: the secondaries go and the primary path is staged.
#[tokio::test]
async fn multipath_without_a_map_falls_back_to_the_primary_path() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| f.multipathd = true);
    fake(&n, |f| f.multipathd = false);
    let req = with_hint(stage_request(&n, block()), HINT);
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(logouts(&n).len(), 2, "both secondaries logged out");
    let device = fake(&n, |f| f.device_of(PORTAL).unwrap());
    assert_eq!(
        std::fs::read_link(&req.staging_target_path).unwrap().to_string_lossy(),
        device
    );
    let degraded = events(&n, REASON_PATH_DEGRADED);
    assert!(degraded[0].contains("dm map for WWID"), "{degraded:?}");
}

#[tokio::test]
async fn unusable_portal_hints_degrade_to_the_primary_portal() {
    for (hint, reason, label) in [
        ("not json", REASON_PATH_DEGRADED, true),
        (r#"["host.example:3260","192.0.2.31"]"#, REASON_PATH_DEGRADED, true),
        ("[]", REASON_PATH_DEGRADED, true),
        (r#"["192.0.2.30:3260"]"#, REASON_MULTIPATH_UNAVAILABLE, false),
    ] {
        let n = iscsi_node_with(MULTIPATH_CONFIG, |f| f.multipathd = true);
        node_stage(&n.state, &with_hint(stage_request(&n, block()), hint), None)
            .await
            .unwrap();
        assert_eq!(logins(&n).len(), 1, "{hint}");
        assert_eq!(events(&n, reason).len(), 1, "{hint}");
        assert_eq!(
            n.state.metrics.iscsi_path_connects("invalid-publish-context", "error"),
            u64::from(label),
            "{hint}"
        );
    }
}

/// Single portal: a path an operator's multipathd holds is never mounted.
#[tokio::test]
async fn a_path_held_by_dm_multipath_is_refused() {
    let n = iscsi_node_with(CONFIG, |f| f.multipathd = true);
    // Another session to the same LUN already exists: multipathd builds a map
    // over both paths once the stage logs in.
    fake(&n, |f| f.add_session(PORTAL_B, IQN));
    let err = node_stage(&n.state, &stage_request(&n, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("claimed by dm-multipath"), "{}", err.message());
    assert!(
        !n.host
            .calls()
            .iter()
            .any(|c| c.starts_with("mkfs") || c.starts_with("mount "))
    );
}

/// A replay of a single-path stage with a multipath hint never adds sessions
/// under a raw device: it reports and keeps the path.
#[tokio::test]
async fn a_raw_path_stage_is_not_converged() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| f.multipathd = true);
    let req = stage_request(&n, block());
    node_stage(&n.state, &req, None).await.unwrap();
    node_stage(&n.state, &with_hint(req, HINT), None).await.unwrap();
    assert_eq!(logins(&n).len(), 1);
    let skipped = events(&n, REASON_MULTIPATH_UNAVAILABLE);
    assert!(
        skipped[0].contains("is not an existing dm-multipath map"),
        "{skipped:?}"
    );
}

#[tokio::test]
async fn a_failed_login_is_reported() {
    let n = iscsi_node_with(CONFIG, |f| f.unreachable = vec![PORTAL.into()]);
    let err = node_stage(&n.state, &stage_request(&n, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message()
            .starts_with("failed to connect iSCSI: iSCSI login failed for"),
        "{}",
        err.message()
    );
    assert_eq!(events(&n, REASON_LOGIN_FAILED).len(), 1);
    assert_eq!(n.state.metrics.node_connects("iscsi", "error"), 1);
}

#[tokio::test]
async fn a_failed_format_is_mount_failed() {
    let n = iscsi_node(CONFIG);
    n.host.0.lock().unwrap().failing = vec!["mkfs.ext4".into()];
    let err = node_stage(&n.state, &stage_request(&n, filesystem()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert_eq!(events(&n, REASON_MOUNT_FAILED).len(), 1);
}

#[tokio::test]
async fn request_validation() {
    let n = iscsi_node(CONFIG);
    let mut req = stage_request(&n, block());
    req.volume_context.insert("lun".into(), "x".into());
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(
        (err.code(), err.message()),
        (Code::InvalidArgument, "invalid LUN number: x")
    );
    let mut req = stage_request(&n, block());
    req.volume_context.remove("portal");
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    let mut req = stage_request(&n, block());
    req.volume_context.remove("iqn");
    assert_eq!(
        node_stage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert!(logins(&n).is_empty());
}

/// Another volume's target at the staging path is AlreadyExists.
#[tokio::test]
async fn another_target_at_the_staging_path_is_refused() {
    let n = iscsi_node(CONFIG);
    node_stage(&n.state, &stage_request(&n, block()), None).await.unwrap();
    let mut other = stage_request(&n, block());
    other.volume_id = "pvc-other".into();
    other
        .volume_context
        .insert("iqn".into(), "iqn.2005-10.org.freenas.ctl:pvc-other".into());
    let err = node_stage(&n.state, &other, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(err.message().contains("backed by iSCSI target"), "{}", err.message());
}

/// A session that will not log out fails the unstage, so kubelet retries.
#[tokio::test]
async fn an_unstage_fails_closed() {
    let n = iscsi_node(CONFIG);
    node_stage(&n.state, &stage_request(&n, block()), None).await.unwrap();
    fake(&n, |f| f.refuse_logout = true);
    let err = node_unstage(&n.state, &unstage_request(&n), None).await.unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message().contains("failed to disconnect orphaned session"),
        "{}",
        err.message()
    );
    fake(&n, |f| f.refuse_logout = false);
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(sessions(&n), 0);
}

/// Without any staged device (a crash after the unmount), the sessions named
/// for the volume are logged out; the name carries iscsi.nameSuffix.
#[tokio::test]
async fn an_unstage_without_a_device_cleans_up_by_name() {
    let suffixed = "iqn.2005-10.org.freenas.ctl:pvc-0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9-sfx";
    let n = iscsi_node_with(&format!("{CONFIG}  nameSuffix: -sfx\n"), |f| {
        f.targets.insert(suffixed.into(), f.targets[IQN].clone());
    });
    fake(&n, |f| {
        f.add_session(PORTAL, suffixed);
        f.add_session(PORTAL_B, suffixed);
        f.add_session(PORTAL, IQN);
    });
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(logouts(&n).len(), 2, "{:?}", logouts(&n));
    assert_eq!(
        fake(&n, |f| f.sessions.iter().filter(|s| s.iqn == IQN).count()),
        1,
        "not this volume's"
    );
}

fn stop() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    std::mem::forget(tx);
    rx
}

const GC: &str = "iscsi:\n  targetPortal: 192.0.2.30:3260\nsessionGC:\n  enabled: true\n  gracePeriod: 1\n";

#[tokio::test]
async fn gc_logs_out_an_orphan_after_the_grace_period() {
    let n = iscsi_node(GC);
    fake(&n, |f| f.add_session(PORTAL, IQN));
    gc_iscsi(&n.state, &stop()).await;
    assert!(logouts(&n).is_empty(), "the first sighting starts the grace period");
    assert_eq!(n.state.metrics.iscsi_sessions(), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_iscsi(&n.state, &stop()).await;
    assert_eq!(logouts(&n), [format!("iscsiadm -m node -T {IQN} -p {PORTAL} --logout")]);
    assert_eq!(n.state.metrics.gc_disconnects("iscsi"), 1);
}

/// In use, through another portal, or a target this driver does not name:
/// kept. One in-use SCSI disk that cannot be identified vetoes the pass.
#[tokio::test]
async fn gc_keeps_what_is_not_an_orphan_of_ours() {
    let n = iscsi_node(GC);
    let staged = stage_request(&n, block());
    node_stage(&n.state, &staged, None).await.unwrap();
    // Link it where kubelet's staging directory is, as kubelet would.
    let dir = n
        .dir
        .path()
        .join("kubelet/plugins/kubernetes.io/csi")
        .join(&n.state.driver_name)
        .join("h/dev");
    std::fs::create_dir_all(&dir).unwrap();
    std::os::unix::fs::symlink(fake(&n, |f| f.device_of(PORTAL).unwrap()), dir.join("vol")).unwrap();
    let foreign = "iqn.2005-10.org.freenas.ctl:not-a-pvc";
    let elsewhere = "iqn.2005-10.org.freenas.ctl:pvc-1a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
    fake(&n, |f| {
        for iqn in [foreign, elsewhere] {
            f.targets.insert(iqn.into(), f.targets[IQN].clone());
        }
        f.add_session(PORTAL, foreign);
        f.add_session(PORTAL_B, elsewhere);
    });
    for _ in 0..2 {
        gc_iscsi(&n.state, &stop()).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }
    gc_iscsi(&n.state, &stop()).await;
    assert!(logouts(&n).is_empty(), "{:?}", logouts(&n));

    // An orphan of ours, and an in-use SCSI disk whose identity cannot be read.
    let orphan = "iqn.2005-10.org.freenas.ctl:pvc-2a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
    fake(&n, |f| {
        f.targets.insert(orphan.into(), f.targets[IQN].clone());
        f.add_session(PORTAL, orphan);
    });
    let unknown = n.daemon.dev_dir.path().join("sdzz");
    std::fs::write(&unknown, b"").unwrap();
    n.host.mount(&n.path("mnt"), unknown.to_str().unwrap(), "ext4");
    gc_iscsi(&n.state, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_iscsi(&n.state, &stop()).await;
    assert!(logouts(&n).is_empty(), "an unreadable in-use disk vetoes the pass");
    n.host.0.lock().unwrap().mounts.remove(&n.path("mnt"));
    gc_iscsi(&n.state, &stop()).await;
    assert!(logouts(&n).is_empty(), "a vetoed pass never started the grace period");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_iscsi(&n.state, &stop()).await;
    assert_eq!(
        logouts(&n),
        [format!("iscsiadm -m node -T {orphan} -p {PORTAL} --logout")]
    );
}

#[tokio::test]
async fn gc_dry_run_and_disabled() {
    let n = iscsi_node(&format!("{GC}  dryRun: true\n"));
    fake(&n, |f| f.add_session(PORTAL, IQN));
    gc_iscsi(&n.state, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_iscsi(&n.state, &stop()).await;
    assert!(logouts(&n).is_empty());

    let n = iscsi_node(&format!("{GC}  iscsiEnabled: false\n"));
    fake(&n, |f| f.add_session(PORTAL, IQN));
    pass(&n.state, true, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    pass(&n.state, true, &stop()).await;
    assert!(logouts(&n).is_empty());
    assert_eq!(n.state.metrics.iscsi_sessions(), 1, "the gauge is kept");
}

/// iSCSI and NVMe-oF keep separate first-seen state.
#[tokio::test]
async fn gc_orphan_state_is_per_protocol() {
    let n = iscsi_node(GC);
    n.state
        .orphans
        .0
        .lock()
        .unwrap()
        .insert("nqn.x".into(), std::time::Instant::now());
    fake(&n, |f| f.add_session(PORTAL, IQN));
    gc_iscsi(&n.state, &stop()).await;
    assert!(n.state.orphans.0.lock().unwrap().contains_key("nqn.x"));
    assert!(n.state.iscsi_orphans.0.lock().unwrap().contains_key(IQN));
}

/// A raw-block volume the Go node staged: a link to the session's disk. The
/// agent replays, publishes and unstages it; and the agent's own stage leaves
/// exactly that layout.
#[tokio::test]
async fn a_raw_block_stage_of_the_go_node_is_served() {
    let n = iscsi_node(CONFIG);
    let device = fake(&n, |f| f.add_session(PORTAL, IQN));
    let req = stage_request(&n, block());
    std::fs::create_dir_all(n.path("staging")).unwrap();
    std::os::unix::fs::symlink(&device, &req.staging_target_path).unwrap();
    node_stage(&n.state, &req, None).await.unwrap();
    assert!(logins(&n).is_empty() && logouts(&n).is_empty());
    node_publish(&n.state, &publish_request(&n, block()), None)
        .await
        .unwrap();
    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: publish_request(&n, block()).target_path,
        },
        None,
    )
    .await
    .unwrap();
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(sessions(&n), 0);

    // The agent's own: the link names the device path itself, as Go's does.
    node_stage(&n.state, &req, None).await.unwrap();
    let device = fake(&n, |f| f.device_of(PORTAL).unwrap());
    assert_eq!(
        std::fs::read_link(&req.staging_target_path).unwrap().to_string_lossy(),
        device
    );
}

/// A filesystem the Go node staged (the disk mounted at the staging path),
/// replayed and unstaged by the agent.
#[tokio::test]
async fn a_filesystem_stage_of_the_go_node_is_served() {
    let n = iscsi_node(CONFIG);
    let device = fake(&n, |f| f.add_session(PORTAL, IQN));
    let req = stage_request(&n, filesystem());
    std::fs::create_dir_all(&req.staging_target_path).unwrap();
    n.host.mount(&req.staging_target_path, &device, "ext4");
    node_stage(&n.state, &req, None).await.unwrap();
    assert!(logins(&n).is_empty());
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert_eq!(logouts(&n).len(), 1);
    assert_eq!(sessions(&n), 0);
}

/// An install without iSCSI keeps refusing iSCSI volumes untouched.
#[tokio::test]
async fn an_install_without_iscsi_refuses_it() {
    let n = iscsi_node("nvmeof:\n  transportAddress: 192.0.2.20\n");
    let err = node_stage(&n.state, &stage_request(&n, block()), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(iscsi_calls(&n).is_empty());
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host.mount(&staging, "/dev/sdb", "ext4");
    let err = node_unstage(&n.state, &unstage_request(&n), None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(n.host.is_mounted(&staging));
    // A raw-block publish of one is refused too.
    let device = n.daemon.dev_dir.path().join("sdc");
    std::fs::write(&device, b"").unwrap();
    let link = n.path("block-staging/dev");
    std::fs::create_dir_all(n.path("block-staging")).unwrap();
    std::os::unix::fs::symlink(&device, &link).unwrap();
    let mut publish = publish_request(&n, block());
    publish.staging_target_path = link;
    let err = node_publish(&n.state, &publish, None).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(iscsi_calls(&n).is_empty());
}

/// After a reboot a raw-block link's literal device name can belong to another
/// volume: the unstage logs out by the volume's target name, never the link's
/// device, so the other volume's session survives.
#[tokio::test]
async fn a_stale_block_link_never_logs_out_another_volume() {
    let other = "iqn.2005-10.org.freenas.ctl:pvc-9a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
    let n = iscsi_node(CONFIG);
    let req = stage_request(&n, block());
    node_stage(&n.state, &req, None).await.unwrap();
    // The node rebooted: this volume's session is gone, and the disk name the
    // link holds now belongs to another volume's session.
    fake(&n, |f| {
        let i = f.sessions.iter().position(|s| s.iqn == IQN).unwrap();
        let session = f.sessions.remove(i);
        f.targets.insert(other.into(), f.targets[IQN].clone());
        f.add_session(PORTAL, other);
        let theirs = f.device_of(PORTAL).unwrap();
        std::fs::remove_file(&req.staging_target_path).unwrap();
        std::os::unix::fs::symlink(&theirs, &req.staging_target_path).unwrap();
        drop(session);
    });
    n.state.records.delete_stage(&req.staging_target_path);
    node_unstage(&n.state, &unstage_request(&n), None).await.unwrap();
    assert!(logouts(&n).is_empty(), "{:?}", logouts(&n));
    assert_eq!(fake(&n, |f| f.sessions.iter().filter(|s| s.iqn == other).count()), 1);
}

/// A multipath filesystem is mounted from /dev/mapper/<name>: expansion
/// resolves it to its dm map, identifies the session through a slave, and
/// rescans it.
#[tokio::test]
async fn a_multipath_filesystem_is_rescanned() {
    let n = iscsi_node_with(MULTIPATH_CONFIG, |f| f.multipathd = true);
    let req = with_hint(stage_request(&n, filesystem()), HINT);
    node_stage(&n.state, &req, None).await.unwrap();
    let map = fake(&n, |f| f.map_of(WWID).unwrap());
    n.host.0.lock().unwrap().filesystems.insert(map.clone(), "ext4".into());
    let dm = std::fs::canonicalize(&map).unwrap();
    let size = n.dir.path().join(format!(
        "sys/class/block/{}/size",
        dm.file_name().unwrap().to_string_lossy()
    ));
    std::fs::create_dir_all(size.parent().unwrap()).unwrap();
    std::fs::write(&size, "2097152\n").unwrap();
    fake(&n, |f| f.on_rescan = Some((size, "4194304\n".into())));
    let resp = node_expand_volume(
        &n.state,
        &csi::NodeExpandVolumeRequest {
            volume_id: VOLUME.into(),
            volume_path: req.staging_target_path.clone(),
            staging_target_path: req.staging_target_path.clone(),
            capacity_range: Some(csi::CapacityRange {
                required_bytes: 2 << 30,
                limit_bytes: 0,
            }),
            volume_capability: Some(filesystem()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(resp.capacity_bytes, 2 << 30);
    assert!(iscsi_calls(&n).iter().any(|c| c.ends_with("--rescan")));
    assert!(n.host.calls().contains(&format!("resize2fs {map}")));
}
