//! The NFS node path against the fakes: every RPC, the Go node's exact mount
//! argv, trunking, and taking over a volume the Go node staged.

use std::collections::HashMap;

use tonic::Code;

use crate::capacity::{node_expand_volume, node_get_volume_stats};
use crate::csi::{self, volume_capability, volume_usage::Unit};
use crate::events::{
    ObjectRef, REASON_NFS_MOUNT_FAILED, REASON_NFS_TRUNKING_DEGRADED, REASON_NFS_TRUNKING_UNAVAILABLE,
};
use crate::publish::{node_publish, node_unpublish};
use crate::stage::{node_stage, node_unstage};
use crate::testing::{HOST_NQN, Node, exists, node};

const VOLUME: &str = "pvc-nfs-1";
const SERVER: &str = "192.0.2.20";
const SHARE: &str = "/mnt/tank/k8s/pvc-nfs-1";
const SOURCE: &str = "192.0.2.20:/mnt/tank/k8s/pvc-nfs-1";
const NFS_ON: &str = "nfs:\n  shareHost: 192.0.2.20\n";

fn nfs_node(config: &str) -> Node {
    node(config, HOST_NQN, |state| state.driver_name = "csi.scale.io".into())
}

fn context(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut c: HashMap<String, String> = [("node_attach_driver", "nfs"), ("server", SERVER), ("share", SHARE)]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    for (k, v) in extra {
        c.insert(k.to_string(), v.to_string());
    }
    c
}

fn nfs_capability(flags: &[&str]) -> csi::VolumeCapability {
    csi::VolumeCapability {
        access_type: Some(volume_capability::AccessType::Mount(volume_capability::MountVolume {
            mount_flags: flags.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::MultiNodeMultiWriter as i32,
        }),
    }
}

fn stage_request(staging: &str, flags: &[&str], ctx: HashMap<String, String>) -> csi::NodeStageVolumeRequest {
    csi::NodeStageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.into(),
        volume_capability: Some(nfs_capability(flags)),
        volume_context: ctx,
        ..Default::default()
    }
}

fn with_addresses(mut req: csi::NodeStageVolumeRequest, addresses: &str) -> csi::NodeStageVolumeRequest {
    req.publish_context.insert("addresses".into(), addresses.into());
    req
}

fn unstage_request(staging: &str) -> csi::NodeUnstageVolumeRequest {
    csi::NodeUnstageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.into(),
    }
}

fn nfs_mounts(n: &Node) -> Vec<String> {
    n.host
        .calls()
        .into_iter()
        .filter(|c| c.starts_with("mount -t nfs"))
        .collect()
}

fn pv() -> ObjectRef {
    ObjectRef::Pv { name: VOLUME.into() }
}

fn assert_no_session_commands(n: &Node) {
    for call in n.host.calls() {
        assert!(
            !call.starts_with("nvme ") && !call.starts_with("iscsiadm "),
            "an NFS volume has no session: {call}"
        );
    }
}

/// The Go node's argv (util.MountNFSWithContext): `nfsvers=4` first, the
/// StorageClass options as given, `nconnect=` from the config last.
#[tokio::test]
async fn stage_mounts_with_the_go_argv_and_is_idempotent() {
    let n = nfs_node("nfs:\n  shareHost: 192.0.2.20\n  nconnect: 4\n");
    let staging = n.path("staging/globalmount");
    let req = stage_request(&staging, &["nfsvers=4.2", "hard", "nconnect=2", "hard"], context(&[]));
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        nfs_mounts(&n),
        [format!(
            "mount -t nfs -o nfsvers=4,nfsvers=4.2,hard,nconnect=4 {SOURCE} {staging}"
        )]
    );
    let record = n.state.records.stage(&staging).expect("recorded");
    assert_eq!(record.live_source, SOURCE);
    assert_eq!(n.state.metrics.node_connects("nfs", "success"), 1);

    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(nfs_mounts(&n).len(), 1, "a replay mounts nothing");

    // The same path for another volume or another source: refused.
    let mut other = req.clone();
    other.volume_id = "pvc-other".into();
    assert_eq!(
        node_stage(&n.state, &other, None).await.unwrap_err().code(),
        Code::AlreadyExists
    );
    let moved = stage_request(&staging, &[], context(&[("server", "192.0.2.99")]));
    let err = node_stage(&n.state, &moved, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(err.message().contains("is backed by"), "{}", err.message());
    assert_no_session_commands(&n);
}

#[tokio::test]
async fn an_ipv6_server_is_bracketed_and_options_default_to_nfsvers_4() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(
        &n.state,
        &stage_request(&staging, &[], context(&[("server", "2001:db8::20")])),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        nfs_mounts(&n),
        [format!("mount -t nfs -o nfsvers=4 [2001:db8::20]:{SHARE} {staging}")]
    );
}

#[tokio::test]
async fn a_failed_mount_is_internal_with_an_event() {
    let n = nfs_node(NFS_ON);
    n.host.0.lock().unwrap().failing.push("mount -t nfs".into());
    let staging = n.path("staging/globalmount");
    let err = node_stage(&n.state, &stage_request(&staging, &[], context(&[])), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message()
            .starts_with("failed to mount NFS: mount failed: exit status 32, output: mount: permission denied"),
        "{}",
        err.message()
    );
    assert_eq!(
        n.events.take(),
        [(pv(), REASON_NFS_MOUNT_FAILED.into(), err.message().into())]
    );
    assert_eq!(n.state.metrics.node_connects("nfs", "error"), 1);
    assert!(n.state.records.stage(&staging).is_none());
}

#[tokio::test]
async fn missing_server_or_share_is_invalid() {
    let n = nfs_node(NFS_ON);
    for missing in ["server", "share"] {
        let mut ctx = context(&[]);
        ctx.remove(missing);
        let err = node_stage(&n.state, &stage_request(&n.path("s"), &[], ctx), None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{missing}");
        assert_eq!(err.message(), "NFS server and share are required in volume context");
    }
    assert!(nfs_mounts(&n).is_empty());
}

#[tokio::test]
async fn trunking_mounts_the_first_address_and_probes_the_others() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    let req = with_addresses(
        stage_request(&staging, &["hard", "max_connect=9"], context(&[])),
        r#"["192.0.2.20","192.0.2.22","192.0.2.20","[2001:db8::23]"]"#,
    );
    node_stage(&n.state, &req, None).await.unwrap();
    let calls = n.host.calls();
    let flags = "nfsvers=4,hard,max_connect=3";
    assert_eq!(
        nfs_mounts(&n),
        [
            format!("mount -t nfs -o {flags} {SOURCE} {staging}"),
            format!("mount -t nfs -o {flags} 192.0.2.22:{SHARE} {staging}.scale-csi-nfs-trunk-1"),
            format!("mount -t nfs -o {flags} [2001:db8::23]:{SHARE} {staging}.scale-csi-nfs-trunk-2"),
        ]
    );
    for probe in 1..=2 {
        let path = format!("{staging}.scale-csi-nfs-trunk-{probe}");
        assert!(calls.contains(&format!("umount {path}")), "{calls:?}");
        assert!(!exists(&path), "probe {probe} is removed");
    }
    assert!(n.events.take().is_empty());
    assert_eq!(n.state.metrics.nfs_trunk_connects("192.0.2.22", "success"), 1);
    assert_eq!(n.state.metrics.nfs_trunk_connects("2001:db8::23", "success"), 1);
    let record = n.state.records.stage(&staging).unwrap();
    assert_eq!(record.live_source, SOURCE);
}

/// The controller lists nfs.shareHost, the volume's server, first. A hint that
/// does not is mounted from its first address, which then is not the source
/// the volume asks for: the stage is refused, as the Go node refuses it.
#[tokio::test]
async fn a_hint_led_by_another_address_is_refused_like_go() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    let req = with_addresses(
        stage_request(&staging, &[], context(&[])),
        r#"["192.0.2.21","192.0.2.20"]"#,
    );
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert_eq!(
        err.message(),
        format!("staging target {staging} is backed by 192.0.2.21:{SHARE}, requested {SOURCE}")
    );
}

#[tokio::test]
async fn a_replayed_trunked_stage_converges_the_trunks_again() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    n.host.mount_with(&staging, SOURCE, "nfs4", "rw,vers=4.1");
    let req = with_addresses(
        stage_request(&staging, &[], context(&[])),
        r#"["192.0.2.20","192.0.2.22"]"#,
    );
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        nfs_mounts(&n),
        [format!(
            "mount -t nfs -o nfsvers=4,max_connect=2 192.0.2.22:{SHARE} {staging}.scale-csi-nfs-trunk-1"
        )],
        "only the probe: the primary is already mounted"
    );
}

#[tokio::test]
async fn a_kernel_without_max_connect_gets_the_plain_primary_mount() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    n.host
        .0
        .lock()
        .unwrap()
        .failing
        .push("mount -t nfs -o nfsvers=4,max_connect".into());
    let req = with_addresses(
        stage_request(&staging, &[], context(&[])),
        r#"["192.0.2.21","192.0.2.22"]"#,
    );
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        nfs_mounts(&n)[1],
        format!("mount -t nfs -o nfsvers=4 {SOURCE} {staging}"),
        "the fallback mounts the volume context's server without max_connect"
    );
    assert_eq!(nfs_mounts(&n).len(), 2, "no probes after the fallback");
    let events = n.events.take();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, REASON_NFS_TRUNKING_UNAVAILABLE);
    assert!(
        events[0].2.starts_with(&format!(
            "NFS trunking options are unavailable for {SHARE}; the primary mount succeeded without max_connect: mount failed"
        )),
        "{}",
        events[0].2
    );
    assert_eq!(n.state.metrics.node_connects("nfs", "success"), 1);
}

#[tokio::test]
async fn a_trunked_mount_whose_fallback_also_fails() {
    let n = nfs_node(NFS_ON);
    n.host.0.lock().unwrap().failing.push("mount -t nfs".into());
    let staging = n.path("staging/globalmount");
    let req = with_addresses(
        stage_request(&staging, &[], context(&[])),
        r#"["192.0.2.21","192.0.2.22"]"#,
    );
    let err = node_stage(&n.state, &req, None).await.unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message()
            .starts_with("failed to mount NFS: trunking mount failed: mount failed")
            && err
                .message()
                .contains("; untrunked primary fallback failed: mount failed"),
        "{}",
        err.message()
    );
    let events = n.events.take();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, REASON_NFS_MOUNT_FAILED);
    assert_eq!(n.state.metrics.node_connects("nfs", "error"), 1);
}

#[tokio::test]
async fn trunking_needs_nfs_4_1() {
    for (version, shown) in [("4.0", "NFS 4.0"), ("3", "NFS 3.0")] {
        let n = nfs_node(NFS_ON);
        n.host.0.lock().unwrap().nfs_version = Some(version.into());
        let staging = n.path("staging/globalmount");
        let req = with_addresses(
            stage_request(&staging, &[], context(&[])),
            r#"["192.0.2.20","192.0.2.22"]"#,
        );
        node_stage(&n.state, &req, None).await.unwrap();
        assert_eq!(nfs_mounts(&n).len(), 1, "{version}: no probe");
        let events = n.events.take();
        assert_eq!(events.len(), 1, "{version}");
        assert_eq!(events[0].1, REASON_NFS_TRUNKING_UNAVAILABLE);
        assert_eq!(
            events[0].2,
            format!(
                "NFS trunking requires a negotiated NFS version of at least 4.1; {staging} is mounted with {shown} and remains available through its primary server"
            )
        );
    }
}

#[tokio::test]
async fn a_failed_trunk_degrades_the_volume() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    n.host
        .0
        .lock()
        .unwrap()
        .failing
        .push("mount -t nfs -o nfsvers=4,max_connect=3 192.0.2.22".into());
    let req = with_addresses(
        stage_request(&staging, &[], context(&[])),
        r#"["192.0.2.20","192.0.2.22","192.0.2.23"]"#,
    );
    node_stage(&n.state, &req, None).await.unwrap();
    let events = n.events.take();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1, REASON_NFS_TRUNKING_DEGRADED);
    assert!(
        events[0].2.starts_with(&format!(
            "NFS trunk transport convergence for {SHARE} is degraded: 192.0.2.22: mount failed"
        )),
        "{}",
        events[0].2
    );
    assert_eq!(n.state.metrics.nfs_trunk_connects("192.0.2.22", "error"), 1);
    assert_eq!(n.state.metrics.nfs_trunk_connects("192.0.2.23", "success"), 1);
    assert!(!exists(&format!("{staging}.scale-csi-nfs-trunk-1")));
}

#[tokio::test]
async fn a_malformed_address_hint_falls_back_to_the_server() {
    for hint in ["[]", "not json", r#"["192.0.2.21:2049"]"#] {
        let n = nfs_node(NFS_ON);
        let staging = n.path("staging/globalmount");
        node_stage(
            &n.state,
            &with_addresses(stage_request(&staging, &[], context(&[])), hint),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            nfs_mounts(&n),
            [format!("mount -t nfs -o nfsvers=4 {SOURCE} {staging}")],
            "{hint}"
        );
        let events = n.events.take();
        assert_eq!(events.len(), 1, "{hint}");
        assert_eq!(events[0].1, REASON_NFS_TRUNKING_DEGRADED);
        assert!(
            events[0].2.starts_with(&format!(
                "NFS trunking address list for {SHARE} was discarded; using the primary server only: "
            )),
            "{}",
            events[0].2
        );
        assert_eq!(
            n.state.metrics.nfs_trunk_connects("invalid-publish-context", "error"),
            1
        );
    }
    // A single address is no trunking: the plain server mount, no event.
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(
        &n.state,
        &with_addresses(stage_request(&staging, &[], context(&[])), r#"["192.0.2.21"]"#),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        nfs_mounts(&n),
        [format!("mount -t nfs -o nfsvers=4 {SOURCE} {staging}")]
    );
    assert!(n.events.take().is_empty());
}

#[tokio::test]
async fn unstage_unmounts_cleans_probes_and_touches_no_session() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(&n.state, &stage_request(&staging, &[], context(&[])), None)
        .await
        .unwrap();
    // A probe a crashed stage left mounted, and one left unmounted.
    let probe1 = format!("{staging}.scale-csi-nfs-trunk-1");
    let probe2 = format!("{staging}.scale-csi-nfs-trunk-2");
    std::fs::create_dir_all(&probe1).unwrap();
    std::fs::create_dir_all(&probe2).unwrap();
    n.host.mount(&probe1, &format!("192.0.2.22:{SHARE}"), "nfs4");

    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert!(!n.host.is_mounted(&staging) && !n.host.is_mounted(&probe1));
    assert!(!exists(&staging) && !exists(&probe1) && !exists(&probe2));
    assert!(n.state.records.stage(&staging).is_none());
    assert_no_session_commands(&n);

    node_unstage(&n.state, &unstage_request(&staging), None)
        .await
        .expect("a replay succeeds");
}

#[tokio::test]
async fn unstage_of_a_hung_nfs_mount_falls_back_to_lazy() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host.mount(&staging, SOURCE, "nfs4");
    n.host.0.lock().unwrap().failing.push(format!("umount {staging}"));
    let err = node_unstage(&n.state, &unstage_request(&staging), None).await;
    let calls = n.host.calls();
    assert!(calls.contains(&format!("umount -l {staging}")), "{calls:?}");
    // The fake host has no lazy unmount either: the mount stays, so unstage
    // fails closed rather than removing a mounted path.
    assert_eq!(err.unwrap_err().code(), Code::Internal);
    assert!(exists(&staging));
}

#[tokio::test]
async fn publish_binds_the_staged_mount() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(&n.state, &stage_request(&staging, &[], context(&[])), None)
        .await
        .unwrap();
    let target = n.path("pods/p1/volumes/kubernetes.io~csi/pv/mount");
    let req = csi::NodePublishVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.clone(),
        target_path: target.clone(),
        volume_capability: Some(nfs_capability(&["noatime"])),
        volume_context: context(&[]),
        readonly: true,
        ..Default::default()
    };
    node_publish(&n.state, &req, None).await.unwrap();
    let calls = n.host.calls();
    assert!(
        calls.contains(&format!("mount -o bind,ro,noatime {staging} {target}")),
        "{calls:?}"
    );
    assert!(calls.contains(&format!("mount -o remount,bind,ro {target}")));
    assert_eq!(n.state.records.publication(&target).unwrap().live_source, SOURCE);
    node_publish(&n.state, &req, None).await.expect("replay");

    // A multi-writer volume publishes at a second target too.
    let mut second = req.clone();
    second.target_path = n.path("pods/p2/volumes/kubernetes.io~csi/pv/mount");
    node_publish(&n.state, &second, None).await.unwrap();

    for t in [&target, &second.target_path] {
        node_unpublish(
            &n.state,
            &csi::NodeUnpublishVolumeRequest {
                volume_id: VOLUME.into(),
                target_path: t.clone(),
            },
            None,
        )
        .await
        .unwrap();
        assert!(!n.host.is_mounted(t) && !exists(t));
    }
    assert!(n.host.is_mounted(&staging), "unpublish leaves the stage");
}

/// Without a staging path (a legacy direct publish) the volume is mounted at
/// the target, with "ro" and the request's flags as its NFS options.
#[tokio::test]
async fn a_direct_publish_mounts_nfs_at_the_target() {
    let n = nfs_node("nfs:\n  shareHost: 192.0.2.20\n  nconnect: 2\n");
    let target = n.path("pods/p1/volumes/kubernetes.io~csi/pv/mount");
    let mut req = csi::NodePublishVolumeRequest {
        volume_id: VOLUME.into(),
        target_path: target.clone(),
        volume_capability: Some(nfs_capability(&["hard", "hard"])),
        volume_context: context(&[]),
        readonly: true,
        ..Default::default()
    };
    req.publish_context
        .insert("addresses".into(), r#"["192.0.2.20","192.0.2.22"]"#.into());
    std::fs::create_dir_all(&target).unwrap();
    node_publish(&n.state, &req, None).await.unwrap();
    assert_eq!(
        nfs_mounts(&n)[0],
        format!("mount -t nfs -o nfsvers=4,ro,hard,nconnect=2,max_connect=2 {SOURCE} {target}")
    );
    assert_eq!(n.state.records.publication(&target).unwrap().live_source, SOURCE);
    let before = nfs_mounts(&n).len();
    node_publish(&n.state, &req, None).await.expect("replay");
    assert_eq!(nfs_mounts(&n).len(), before, "a replay mounts nothing");

    // Not NFS, or no server: refused before any mount.
    let mut block = req.clone();
    block.target_path = n.path("pods/p2/volumes/kubernetes.io~csi/pv/mount");
    block
        .volume_context
        .insert("node_attach_driver".into(), "nvmeof".into());
    assert_eq!(
        node_publish(&n.state, &block, None).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut serverless = block.clone();
    serverless.volume_context = context(&[]);
    serverless.volume_context.remove("server");
    assert_eq!(
        node_publish(&n.state, &serverless, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );

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
    assert!(!n.host.is_mounted(&target) && !exists(&target));
}

#[tokio::test]
async fn stats_report_bytes_and_inodes_after_the_mount_table_check() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(&n.state, &stage_request(&staging, &[], context(&[])), None)
        .await
        .unwrap();
    let stats = node_get_volume_stats(
        &n.state,
        &csi::NodeGetVolumeStatsRequest {
            volume_id: VOLUME.into(),
            volume_path: staging.clone(),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let units: Vec<i32> = stats.usage.iter().map(|u| u.unit).collect();
    assert_eq!(units, [Unit::Bytes as i32, Unit::Inodes as i32]);
    assert!(stats.usage[0].total > 0);
    let check = n
        .host
        .calls()
        .iter()
        .position(|c| *c == format!("findmnt --mountpoint {staging} --noheadings"));
    assert!(check.is_some(), "the mount table is read first");

    // A mount table that does not answer: never a stat of the path.
    n.host
        .0
        .lock()
        .unwrap()
        .failing
        .push(format!("findmnt --mountpoint {staging}"));
    let err = node_get_volume_stats(
        &n.state,
        &csi::NodeGetVolumeStatsRequest {
            volume_id: VOLUME.into(),
            volume_path: staging.clone(),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(err.message().starts_with("mount unresponsive for"), "{}", err.message());
}

/// An NFS volume grows on the NAS: node expansion has nothing to do and
/// reports the requested size, as the Go node does.
#[tokio::test]
async fn expand_has_nothing_to_grow() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    node_stage(&n.state, &stage_request(&staging, &[], context(&[])), None)
        .await
        .unwrap();
    let before = n.host.calls().len();
    let got = node_expand_volume(
        &n.state,
        &csi::NodeExpandVolumeRequest {
            volume_id: VOLUME.into(),
            volume_path: staging.clone(),
            staging_target_path: staging.clone(),
            capacity_range: Some(csi::CapacityRange {
                required_bytes: 5 << 30,
                limit_bytes: 0,
            }),
            volume_capability: Some(nfs_capability(&[])),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(got.capacity_bytes, 5 << 30);
    let after: Vec<String> = n.host.calls()[before..].to_vec();
    assert!(
        after.iter().all(|c| c.starts_with("findmnt ")),
        "no resize or rescan: {after:?}"
    );
}

/// A volume the Go node staged (the same mount at the same path) is taken
/// over: the Rust stage replays it without mounting, publish binds it, and
/// unstage unmounts it. The Go node takes over the Rust one the same way: the
/// mount argv is the Go node's (see the stage test above).
#[tokio::test]
async fn takes_over_a_go_staged_volume() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host
        .mount_with(&staging, SOURCE, "nfs4", "rw,relatime,vers=4.2,hard,proto=tcp");

    node_stage(&n.state, &stage_request(&staging, &["hard"], context(&[])), None)
        .await
        .unwrap();
    assert!(nfs_mounts(&n).is_empty(), "the Go node's mount is this stage");
    let target = n.path("pods/p1/volumes/kubernetes.io~csi/pv/mount");
    node_publish(
        &n.state,
        &csi::NodePublishVolumeRequest {
            volume_id: VOLUME.into(),
            staging_target_path: staging.clone(),
            target_path: target.clone(),
            volume_capability: Some(nfs_capability(&["hard"])),
            volume_context: context(&[]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    node_unpublish(
        &n.state,
        &csi::NodeUnpublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: target,
        },
        None,
    )
    .await
    .unwrap();
    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert!(!n.host.is_mounted(&staging) && !exists(&staging));
    assert_no_session_commands(&n);

    // A publication the Go node made without staging is replayed as well.
    let target = n.path("pods/p9/volumes/kubernetes.io~csi/pv/mount");
    std::fs::create_dir_all(&target).unwrap();
    n.host.mount_with(&target, SOURCE, "nfs4", "rw,vers=4.2");
    node_publish(
        &n.state,
        &csi::NodePublishVolumeRequest {
            volume_id: VOLUME.into(),
            target_path: target.clone(),
            volume_capability: Some(nfs_capability(&[])),
            volume_context: context(&[]),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert!(nfs_mounts(&n).is_empty());
}

#[tokio::test]
async fn an_iscsi_volume_is_still_refused() {
    let n = nfs_node(NFS_ON);
    let err = node_stage(
        &n.state,
        &stage_request(
            &n.path("s"),
            &[],
            context(&[("node_attach_driver", "iscsi"), ("iqn", "iqn.2005-10.org.example:x")]),
        ),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("does not serve yet"));
    assert!(n.host.calls().iter().all(|c| !c.starts_with("mount ")));
}

/// Two NFS mounts stacked on the staging path (a Go and a Rust node both
/// staging during a handover): one umount lifts only the top one. What is left
/// mounted is the live share, and nothing under it may be deleted.
#[tokio::test]
async fn unstage_never_deletes_through_a_mount_still_stacked_underneath() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host.mount(&staging, SOURCE, "nfs4");
    n.host.0.lock().unwrap().stacked.push(staging.clone());
    let file = format!("{staging}/data-on-the-share");
    std::fs::write(&file, b"keep").unwrap();

    let err = node_unstage(&n.state, &unstage_request(&staging), None).await;
    assert_eq!(err.unwrap_err().code(), Code::Internal);
    assert!(exists(&file), "a file on the still-mounted share was deleted");

    // The retry lifts the last mount and finishes.
    std::fs::remove_file(&file).unwrap();
    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert!(!n.host.is_mounted(&staging) && !exists(&staging));
}

/// The same on unpublish: the target keeps a stacked bind of the share.
#[tokio::test]
async fn unpublish_never_deletes_through_a_mount_still_stacked_underneath() {
    let n = nfs_node(NFS_ON);
    let target = n.path("pods/p1/volumes/kubernetes.io~csi/pv/mount");
    std::fs::create_dir_all(&target).unwrap();
    n.host.mount(&target, SOURCE, "nfs4");
    n.host.0.lock().unwrap().stacked.push(target.clone());
    let file = format!("{target}/data-on-the-share");
    std::fs::write(&file, b"keep").unwrap();
    let req = csi::NodeUnpublishVolumeRequest {
        volume_id: VOLUME.into(),
        target_path: target.clone(),
    };
    let err = node_unpublish(&n.state, &req, None).await;
    assert_eq!(err.unwrap_err().code(), Code::Internal);
    assert!(exists(&file), "a file on the still-mounted share was deleted");
}

/// A directory left with files in it after the unmount is not ours to empty.
#[tokio::test]
async fn unstage_leaves_a_non_empty_unmounted_directory_in_place() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host.mount(&staging, SOURCE, "nfs4");
    let file = format!("{staging}/left-behind");
    std::fs::write(&file, b"x").unwrap();
    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert!(exists(&file));
}

/// findmnt failing (it stats the path, and times out on a dead server) must
/// not turn an NFS staging mount into an unknown device: mountinfo names the
/// share without touching the path, so unstage stays on the NFS path and runs
/// no block-session cleanup.
#[tokio::test]
async fn unstage_finds_the_share_in_mountinfo_when_findmnt_fails() {
    let n = nfs_node(NFS_ON);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    n.host.mount(&staging, SOURCE, "nfs4");
    n.host
        .0
        .lock()
        .unwrap()
        .failing
        .push(format!("findmnt --first-only -n -o SOURCE {staging}"));
    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert!(!n.host.is_mounted(&staging));
    assert_no_session_commands(&n);
}
