//! NodeStageVolume and NodeUnstageVolume on the ublk data path: the Go node's
//! tests (`pkg/driver/node_nvmeublk_test.go`) against the fakes.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;

use tonic::Code;

use crate::csi::{self, volume_capability};
use crate::events::{self, ObjectRef};
use crate::stage::{node_stage, node_unstage};
use crate::testing::{HOST_ID, HOST_NQN, Node, assert_no_nvme_cli, exists, node};
use crate::ublk_client::{AttachRequest, Daemon, Error};
use crate::ublk_stage::{TRANSPORT_LABEL, validate_attached_device};

const VOLUME: &str = "pvc-ublk-1";
const NQN: &str = "nqn.2011-06.com.example:pvc-ublk-1";
const UBLK_ON: &str = "nvmeof:\n  ublk:\n    enabled: true\n";

fn context(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut c: HashMap<String, String> = [
        ("node_attach_driver", "nvmeof"),
        ("nqn", NQN),
        ("transport", "tcp"),
        ("address", "192.0.2.20"),
        ("port", "4420"),
        ("nvmeof/dataPath", "ublk"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    for (k, v) in extra {
        c.insert(k.to_string(), v.to_string());
    }
    c
}

fn block() -> csi::VolumeCapability {
    csi::VolumeCapability {
        access_type: Some(volume_capability::AccessType::Block(Default::default())),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

fn filesystem() -> csi::VolumeCapability {
    csi::VolumeCapability {
        access_type: Some(volume_capability::AccessType::Mount(volume_capability::MountVolume {
            fs_type: "ext4".into(),
            ..Default::default()
        })),
        access_mode: Some(volume_capability::AccessMode {
            mode: volume_capability::access_mode::Mode::SingleNodeWriter as i32,
        }),
    }
}

fn stage_request(
    staging: &str,
    capability: csi::VolumeCapability,
    ctx: HashMap<String, String>,
) -> csi::NodeStageVolumeRequest {
    csi::NodeStageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.into(),
        volume_capability: Some(capability),
        volume_context: ctx,
        ..Default::default()
    }
}

fn unstage_request(staging: &str) -> csi::NodeUnstageVolumeRequest {
    csi::NodeUnstageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: staging.into(),
    }
}

fn ublk_node() -> Node {
    node(UBLK_ON, HOST_NQN, |_| {})
}

#[tokio::test]
async fn block_round_trip() {
    let n = ublk_node();
    let staging = n.path("staging/volume-device");
    // kubelet pre-creates the staging leaf as a directory.
    std::fs::create_dir_all(&staging).unwrap();
    let mut req = stage_request(&staging, block(), context(&[]));
    // ControllerPublish's multipath hint: the daemon gets the same address set
    // the kernel path would connect.
    req.publish_context.insert(
        "addresses".into(),
        r#"["192.0.2.20","192.0.2.21","2001:db8::22"]"#.into(),
    );

    node_stage(&n.state, &req, None).await.unwrap();

    let attaches = n.daemon.attaches();
    assert_eq!(
        attaches,
        vec![AttachRequest {
            volume: VOLUME.into(),
            subnqn: NQN.into(),
            addrs: vec![
                "192.0.2.20:4420".into(),
                "192.0.2.21:4420".into(),
                "[2001:db8::22]:4420".into()
            ],
            hostnqn: HOST_NQN.into(),
            hostid: HOST_ID.into(),
            queues: 0,
            depth: 0,
            zero_copy: true,
            napi_us: 200,
        }],
        "an install that pins no layout leaves queues and depth to the daemon and busy-polls for 200 us"
    );
    assert!(n.daemon.detaches().is_empty());
    assert_eq!(
        std::fs::read_link(&staging).unwrap().to_string_lossy(),
        n.daemon.device_path(0)
    );
    assert_eq!(n.state.metrics.node_connects(TRANSPORT_LABEL, "success"), 1);
    assert_eq!(
        n.state.metrics.node_connects("nvmeof", "success"),
        0,
        "the kernel transport label must not count a ublk attach"
    );
    assert!(n.marked(VOLUME), "a ublk stage leaves evidence for unstage");

    // A compatible re-stage verifies through the daemon and changes nothing.
    let before = std::fs::symlink_metadata(&staging).unwrap();
    let lists = n.daemon.lists();
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(
        n.daemon.attaches().len(),
        1,
        "a compatible re-stage must not attach again"
    );
    assert!(n.daemon.lists() > lists, "re-stage identity comes from the daemon");
    let after = std::fs::symlink_metadata(&staging).unwrap();
    assert_eq!(
        (before.dev(), before.ino()),
        (after.dev(), after.ino()),
        "a correct staging symlink must not be replaced"
    );

    node_unstage(&n.state, &unstage_request(&staging), None).await.unwrap();
    assert_eq!(n.daemon.detaches(), vec![VOLUME.to_string()]);
    assert!(!exists(&staging), "unstage removes the staging symlink");
    assert!(!n.marked(VOLUME), "a completed detach clears the evidence");
    assert_no_nvme_cli(&n.host);
}

#[tokio::test]
async fn filesystem_formats_the_daemons_device() {
    // The install default selects ublk; the volume does not pin it.
    let config = "nvmeof:\n  dataPath: ublk\n  ublk:\n    queues: 4\n    depth: 128\n    zeroCopy: false\n";
    let n = node(config, HOST_NQN, |_| {});
    let mut ctx = context(&[]);
    ctx.remove("nvmeof/dataPath");
    let staging = n.path("stage");

    node_stage(&n.state, &stage_request(&staging, filesystem(), ctx), None)
        .await
        .unwrap();

    let device = n.daemon.device_path(0);
    let calls = n.host.calls();
    assert!(calls.contains(&format!("mkfs.ext4 -F {device}")), "{calls:?}");
    assert!(
        calls.contains(&format!("mount -t ext4 {device} {staging}")),
        "{calls:?}"
    );
    let attaches = n.daemon.attaches();
    assert_eq!(attaches.len(), 1);
    assert_eq!(
        attaches[0].addrs,
        vec!["192.0.2.20:4420".to_string()],
        "no multipath hint falls back to address:port"
    );
    assert_eq!((attaches[0].queues, attaches[0].depth), (4, 128));
    assert!(!attaches[0].zero_copy);
    assert_eq!(attaches[0].napi_us, 200);
    let record = n.state.records.stage(&staging).unwrap();
    assert_eq!(record.live_source, device, "the record holds the live device");
    assert_no_nvme_cli(&n.host);
}

/// A mounted ublk volume re-stages through the daemon's identity; a mount of
/// another subsystem or another daemon volume is refused.
#[tokio::test]
async fn mounted_replay_verifies_through_the_daemon() {
    for (name, volume, subnqn, want) in [
        ("same volume", VOLUME, NQN, Code::Ok),
        (
            "different subsystem",
            VOLUME,
            "nqn.2011-06.com.example:other",
            Code::AlreadyExists,
        ),
        ("different daemon volume", "pvc-other", NQN, Code::AlreadyExists),
    ] {
        let n = ublk_node();
        let device = n.daemon.device_path(5);
        n.daemon.insert(volume, subnqn, 5, &device);
        let staging = n.path("stage");
        std::fs::create_dir_all(&staging).unwrap();
        n.host.mount(&staging, &device, "ext4");

        let got = node_stage(&n.state, &stage_request(&staging, filesystem(), context(&[])), None).await;
        assert_eq!(got.err().map_or(Code::Ok, |e| e.code()), want, "{name}");
        assert!(n.daemon.attaches().is_empty(), "{name}");
        assert!(n.daemon.lists() > 0, "{name}");
        assert_no_nvme_cli(&n.host);
    }
}

#[tokio::test]
async fn stage_failures() {
    /// name, config, host NQN, context overrides, attach error, code, message, attached
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        Vec<(&'static str, &'static str)>,
        Option<Error>,
        Code,
        &'static str,
        bool,
    );
    let cases: Vec<Case> = vec![
        (
            "ublk not enabled",
            "nvmeof: {}\n",
            HOST_NQN,
            vec![],
            None,
            Code::FailedPrecondition,
            "not enabled on this node",
            false,
        ),
        (
            "no host NQN",
            UBLK_ON,
            "",
            vec![],
            None,
            Code::FailedPrecondition,
            "no NVMe host NQN",
            false,
        ),
        (
            "no host ID",
            UBLK_ON,
            "nqn.2014-08.com.example:node",
            vec![],
            None,
            Code::FailedPrecondition,
            "no NVMe host ID",
            false,
        ),
        (
            "rdma transport",
            UBLK_ON,
            HOST_NQN,
            vec![("transport", "rdma")],
            None,
            Code::InvalidArgument,
            "supports only the tcp transport",
            false,
        ),
        (
            "malformed pinned data path",
            UBLK_ON,
            HOST_NQN,
            vec![("nvmeof/dataPath", "spdk")],
            None,
            Code::InvalidArgument,
            "nvmeof/dataPath",
            false,
        ),
        (
            "daemon refuses",
            UBLK_ON,
            HOST_NQN,
            vec![],
            Some(Error::Refused("zero copy unsupported".into())),
            Code::Internal,
            "zero copy unsupported",
            true,
        ),
        (
            "daemon not running",
            UBLK_ON,
            HOST_NQN,
            vec![],
            Some(Error::Unavailable("connect: no such file".into())),
            Code::Unavailable,
            "unavailable",
            true,
        ),
        (
            "attach ran out of time",
            UBLK_ON,
            HOST_NQN,
            vec![],
            Some(Error::Deadline("attach pvc-ublk-1".into())),
            Code::DeadlineExceeded,
            "deadline exceeded",
            true,
        ),
    ];
    for (name, config, host_nqn, extra, attach_err, want, message, attached) in cases {
        let n = node(config, host_nqn, |_| {});
        n.daemon.state.lock().unwrap().attach_err = attach_err;
        let err = node_stage(
            &n.state,
            &stage_request(&n.path("stage"), block(), context(&extra)),
            None,
        )
        .await
        .expect_err(name);
        assert_eq!(err.code(), want, "{name}: {err:?}");
        assert!(err.message().contains(message), "{name}: {}", err.message());
        assert_eq!(!n.daemon.attaches().is_empty(), attached, "{name}");
        if attached {
            assert_eq!(n.state.metrics.node_connects(TRANSPORT_LABEL, "error"), 1, "{name}");
            assert!(
                n.marked(VOLUME),
                "{name}: the marker stays so the unstage kubelet still owes can detach a late attach"
            );
            let events = n.events.take();
            assert!(
                events
                    .iter()
                    .any(|(_, reason, _)| reason == events::REASON_NVME_CONNECT_FAILED),
                "{name}: {events:?}"
            );
        }
        assert_no_nvme_cli(&n.host);
    }
}

#[test]
fn a_foreign_device_path_is_refused() {
    let dev = tempfile::tempdir().unwrap();
    let present = dev.path().join("ublkb3");
    std::fs::write(&present, b"").unwrap();
    let device = |id: i64, path: &std::path::Path| crate::ublk_client::Device {
        volume: VOLUME.into(),
        subnqn: NQN.into(),
        dev_id: id,
        path: path.to_string_lossy().into_owned(),
        paths: vec![],
        existing: false,
    };
    assert!(validate_attached_device(dev.path(), &device(0, std::path::Path::new("/dev/sda"))).is_err());
    assert!(
        validate_attached_device(dev.path(), &device(1, &present)).is_err(),
        "a path for a different dev_id"
    );
    assert!(
        validate_attached_device(dev.path(), &device(2, &dev.path().join("ublkb2"))).is_err(),
        "the device node is missing"
    );
    assert!(validate_attached_device(dev.path(), &device(-1, &dev.path().join("ublkb-1"))).is_err());
    validate_attached_device(dev.path(), &device(3, &present)).unwrap();
}

#[tokio::test]
async fn down_paths_are_reported() {
    let n = ublk_node();
    n.daemon.state.lock().unwrap().paths_down = true;
    let mut ctx = context(&[]);
    ctx.insert("csi.storage.k8s.io/pvc/namespace".into(), "apps".into());
    ctx.insert("csi.storage.k8s.io/pvc/name".into(), "data".into());
    node_stage(&n.state, &stage_request(&n.path("stage"), block(), ctx), None)
        .await
        .expect("a volume with paths down still stages");
    let events = n.events.take();
    assert_eq!(events.len(), 1, "{events:?}");
    let (object, reason, message) = &events[0];
    assert_eq!(
        *object,
        ObjectRef::Pvc {
            namespace: "apps".into(),
            name: "data".into()
        }
    );
    assert_eq!(reason, events::REASON_NVME_PATH_DEGRADED);
    assert!(
        message.contains("192.0.2.20:4420: path is down in nvmeublkd"),
        "{message}"
    );
}

#[tokio::test]
async fn a_discarded_multipath_hint_is_reported_and_falls_back() {
    let n = ublk_node();
    let mut req = stage_request(&n.path("stage"), block(), context(&[]));
    req.publish_context.insert("addresses".into(), "not json".into());
    node_stage(&n.state, &req, None).await.unwrap();
    assert_eq!(n.daemon.attaches()[0].addrs, vec!["192.0.2.20:4420".to_string()]);
    assert_eq!(
        n.state.metrics.nvme_path_connects("invalid-publish-context", "error"),
        1
    );
    let events = n.events.take();
    assert!(
        events
            .iter()
            .any(|(_, reason, message)| reason == events::REASON_NVME_PATH_DEGRADED
                && message.contains("was discarded; using single-address fallback")),
        "{events:?}"
    );
}

/// After a reboot ublk numbering restarts at 0: a block link that survived
/// resolves to whichever volume attached first. Its re-stage attaches its own
/// device and replaces the link, never touching the other volume's device.
#[tokio::test]
async fn a_stale_link_to_a_reused_device_reattaches() {
    let n = ublk_node();
    let other = n
        .daemon
        .attach(
            &AttachRequest {
                volume: "pvc-other".into(),
                subnqn: "nqn.2011-06.com.example:pvc-other".into(),
                ..Default::default()
            },
            std::time::Instant::now(),
        )
        .await
        .unwrap();
    let staging = n.path("staging/volume-device");
    std::fs::create_dir_all(n.path("staging")).unwrap();
    std::os::unix::fs::symlink(&other.path, &staging).unwrap();

    node_stage(&n.state, &stage_request(&staging, block(), context(&[])), None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_link(&staging).unwrap().to_string_lossy(),
        n.daemon.device_path(1),
        "the link now points at this volume's own device"
    );
    let attaches = n.daemon.attaches();
    assert_eq!(attaches.len(), 2);
    assert_eq!(attaches[1].volume, VOLUME);
    assert!(
        n.daemon.detaches().is_empty(),
        "the other volume's device is never detached"
    );
    assert!(exists(&other.path));
    assert_no_nvme_cli(&n.host);
}

/// A stage record naming another volume at this path is a real collision.
#[tokio::test]
async fn a_link_recorded_for_another_volume_stays_already_exists() {
    let n = ublk_node();
    let other = n
        .daemon
        .attach(
            &AttachRequest {
                volume: "pvc-other".into(),
                subnqn: "nqn.2011-06.com.example:pvc-other".into(),
                ..Default::default()
            },
            std::time::Instant::now(),
        )
        .await
        .unwrap();
    let staging = n.path("staging/volume-device");
    std::fs::create_dir_all(n.path("staging")).unwrap();
    std::os::unix::fs::symlink(&other.path, &staging).unwrap();
    n.state.records.store_stage(crate::records::MountRecord {
        volume_id: "pvc-other".into(),
        target_path: staging.clone(),
        expected_source: String::new(),
        live_source: String::new(),
        capability: crate::capability::signature(Some(&block())).unwrap(),
        readonly: false,
    });

    let err = node_stage(&n.state, &stage_request(&staging, block(), context(&[])), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert_eq!(n.daemon.attaches().len(), 1, "no attach for the colliding volume");
}

/// A dangling block link (the device vanished) re-stages.
#[tokio::test]
async fn a_dangling_link_restages() {
    let n = ublk_node();
    let staging = n.path("staging/volume-device");
    std::fs::create_dir_all(n.path("staging")).unwrap();
    std::os::unix::fs::symlink(n.daemon.device_path(9), &staging).unwrap();
    node_stage(&n.state, &stage_request(&staging, block(), context(&[])), None)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_link(&staging).unwrap().to_string_lossy(),
        n.daemon.device_path(0)
    );
}

#[tokio::test]
async fn a_staged_filesystem_requested_as_block_is_already_exists() {
    let n = ublk_node();
    let staging = n.path("stage");
    node_stage(&n.state, &stage_request(&staging, filesystem(), context(&[])), None)
        .await
        .unwrap();
    let err = node_stage(&n.state, &stage_request(&staging, block(), context(&[])), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert!(
        err.message().contains("access type mount, requested block"),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn other_paths_are_refused_before_anything_changes() {
    for (name, extra) in [
        ("kernel data path", vec![("nvmeof/dataPath", "kernel")]),
        (
            "iscsi",
            vec![("node_attach_driver", "iscsi"), ("iqn", "iqn.2005-10.org.example:x")],
        ),
        (
            "nfs",
            vec![
                ("node_attach_driver", "nfs"),
                ("server", "192.0.2.1"),
                ("share", "/mnt/x"),
            ],
        ),
    ] {
        let n = ublk_node();
        n.daemon.state.lock().unwrap().forbidden = true;
        let err = node_stage(
            &n.state,
            &stage_request(&n.path("stage"), block(), context(&extra)),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{name}: {err:?}");
        assert!(err.message().contains("does not serve yet"), "{name}");
        assert!(!n.marked(VOLUME), "{name}");
    }
}

#[tokio::test]
async fn a_second_operation_on_the_volume_is_aborted() {
    let n = ublk_node();
    let _held = n.state.locks.try_lock(crate::locks::node_volume_key(VOLUME)).unwrap();
    let err = node_stage(&n.state, &stage_request(&n.path("stage"), block(), context(&[])), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    let err = node_unstage(&n.state, &unstage_request(&n.path("stage")), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
}

#[tokio::test]
async fn unstage() {
    #[derive(Clone, Copy)]
    enum Setup {
        MountedUblk,
        LinkToUblk,
        MarkerOnly,
        MountedKernel,
        MarkerAndKernel,
        Nothing,
        NothingButKernelSession,
    }
    let cases = [
        (
            "filesystem mount of a ublk device",
            Setup::MountedUblk,
            None,
            Code::Ok,
            true,
        ),
        (
            "block symlink to a ublk device",
            Setup::LinkToUblk,
            None,
            Code::Ok,
            true,
        ),
        (
            "only the marker survives (an earlier attempt unmounted, then failed)",
            Setup::MarkerOnly,
            None,
            Code::Ok,
            true,
        ),
        (
            "ublk device with the daemon down fails closed",
            Setup::MountedUblk,
            Some(Error::Unavailable("connect".into())),
            Code::Unavailable,
            true,
        ),
        (
            "marker only with the daemon down is never silently skipped",
            Setup::MarkerOnly,
            Some(Error::Unavailable("connect".into())),
            Code::Unavailable,
            true,
        ),
        (
            "daemon refuses the detach",
            Setup::LinkToUblk,
            Some(Error::Refused("stop ublk device 3: busy".into())),
            Code::Internal,
            true,
        ),
        // The Go node detaches the marker's attachment and then cleans up the
        // kernel session; until the kernel path is ported this agent refuses
        // a kernel device before changing anything.
        (
            "a kernel device is refused untouched",
            Setup::MountedKernel,
            None,
            Code::FailedPrecondition,
            false,
        ),
        (
            "a marker beside a kernel device is refused untouched",
            Setup::MarkerAndKernel,
            None,
            Code::FailedPrecondition,
            false,
        ),
        ("nothing left: an unstage replay", Setup::Nothing, None, Code::Ok, false),
        (
            "nothing staged but a live kernel session",
            Setup::NothingButKernelSession,
            None,
            Code::FailedPrecondition,
            false,
        ),
    ];
    for (name, setup, detach_err, want, detached) in cases {
        let n = ublk_node();
        n.daemon.state.lock().unwrap().detach_err = detach_err;
        let staging = n.path("staging");
        let mark = || crate::ublk_state::write_marker(&n.socket(), &n.state.driver_name, VOLUME).unwrap();
        match setup {
            Setup::MountedUblk => {
                std::fs::create_dir_all(&staging).unwrap();
                n.host.mount(&staging, "/dev/ublkb3", "ext4");
                mark();
            }
            Setup::LinkToUblk => {
                std::os::unix::fs::symlink("/dev/ublkb3", &staging).unwrap();
                mark();
            }
            Setup::MarkerOnly => mark(),
            Setup::MountedKernel => {
                std::fs::create_dir_all(&staging).unwrap();
                n.host.mount(&staging, "/dev/nvme7n1", "ext4");
            }
            Setup::MarkerAndKernel => {
                std::fs::create_dir_all(&staging).unwrap();
                n.host.mount(&staging, "/dev/nvme7n1", "ext4");
                mark();
            }
            Setup::Nothing => {}
            Setup::NothingButKernelSession => {
                let subsys = n.dir.path().join("sys/class/nvme-subsystem/nvme-subsys7");
                std::fs::create_dir_all(&subsys).unwrap();
                std::fs::write(subsys.join("subsysnqn"), format!("{NQN}\n")).unwrap();
            }
        }

        let got = node_unstage(&n.state, &unstage_request(&staging), None).await;
        assert_eq!(
            got.as_ref().err().map_or(Code::Ok, |e| e.code()),
            want,
            "{name}: {got:?}"
        );
        let detaches = n.daemon.detaches();
        if detached {
            assert_eq!(detaches, vec![VOLUME.to_string()], "{name}");
        } else {
            assert!(detaches.is_empty(), "{name}: {detaches:?}");
        }
        match (want, setup) {
            (Code::Ok, _) => assert!(!n.marked(VOLUME), "{name}: a successful unstage leaves no marker"),
            (_, Setup::MarkerOnly | Setup::MarkerAndKernel) => {
                assert!(
                    n.marked(VOLUME),
                    "{name}: a failed unstage keeps the evidence for the retry"
                )
            }
            _ => {}
        }
        if matches!(setup, Setup::MountedKernel | Setup::MarkerAndKernel) {
            assert!(
                n.host.is_mounted(&staging),
                "{name}: a refused unstage leaves the mount alone"
            );
        }
        assert_no_nvme_cli(&n.host);
    }
}

/// A kernel-only install never contacts the daemon or looks for markers.
#[tokio::test]
async fn a_kernel_only_install_never_contacts_the_daemon() {
    let n = node("nvmeof: {}\n", HOST_NQN, |state| {
        // A marker directory that cannot be inspected would fail closed if it
        // were consulted.
        state.config.nvmeof.ublk.socket_path = "/proc/self/fdinfo/nonexistent/d.sock".into();
    });
    n.daemon.state.lock().unwrap().forbidden = true;
    node_unstage(&n.state, &unstage_request(&n.path("gone")), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn requests_are_validated() {
    let n = ublk_node();
    let staging = n.path("stage");
    let mut req = stage_request(&staging, block(), context(&[]));
    req.volume_id.clear();
    assert_eq!(
        node_stage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = stage_request("", block(), context(&[]));
    req.volume_id = VOLUME.into();
    assert_eq!(
        node_stage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = stage_request(&staging, block(), context(&[]));
    req.volume_capability = None;
    assert_eq!(
        node_stage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let req = stage_request(&staging, block(), HashMap::new());
    assert_eq!(
        node_stage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    let mut req = unstage_request(&staging);
    req.volume_id.clear();
    assert_eq!(
        node_unstage(&n.state, &req, None).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        node_unstage(&n.state, &unstage_request(""), None)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}
