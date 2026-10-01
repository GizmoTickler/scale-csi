//! Staging NVMe-oF through the kernel initiator, against a fake kernel.

use tonic::Code;

use crate::csi;
use crate::events;
use crate::nvme_kernel::{self, REASON_MULTIPATH_UNAGGREGATED};
use crate::stage::node_stage;
use crate::testing::{FakeKernel, HOST_NQN, NQN, Node, VOLUME, block, context, filesystem, node};

const SINGLE: &str = "nvmeof:\n  transportAddress: 192.0.2.20\n";
const MULTI: &str =
    "nvmeof:\n  transportAddress: 192.0.2.20\n  multipath: true\n  addresses: [192.0.2.21, 192.0.2.22]\n";
const HINT: &str = r#"["192.0.2.20","192.0.2.21","192.0.2.22"]"#;

fn kernel_node(config: &str) -> Node {
    let n = node(config, HOST_NQN, |_| {});
    n.daemon.state.lock().unwrap().forbidden = true;
    n.host.0.lock().unwrap().kernel = Some(FakeKernel::new(
        n.dir.path().join("sys"),
        n.daemon.dev_dir.path().to_path_buf(),
    ));
    n
}

fn kernel_context() -> std::collections::HashMap<String, String> {
    let mut ctx = context(&[]);
    ctx.remove("nvmeof/dataPath");
    ctx
}

fn stage_request(n: &Node, capability: csi::VolumeCapability, hint: Option<&str>) -> csi::NodeStageVolumeRequest {
    let mut req = csi::NodeStageVolumeRequest {
        volume_id: VOLUME.into(),
        staging_target_path: n.path("staging/globalmount"),
        volume_capability: Some(capability),
        volume_context: kernel_context(),
        ..Default::default()
    };
    if let Some(hint) = hint {
        req.publish_context.insert("addresses".into(), hint.into());
    }
    req
}

fn nvme_calls(n: &Node) -> Vec<String> {
    n.host.calls().into_iter().filter(|c| c.starts_with("nvme ")).collect()
}

fn device(n: &Node) -> String {
    n.host.0.lock().unwrap().kernel.as_ref().unwrap().device(NQN).unwrap()
}

#[tokio::test]
async fn single_path_block_stage() {
    let n = kernel_node(SINGLE);
    node_stage(&n.state, &stage_request(&n, block(), None), None)
        .await
        .unwrap();
    let connects: Vec<String> = nvme_calls(&n)
        .into_iter()
        .filter(|c| c.starts_with("nvme connect"))
        .collect();
    assert_eq!(
        connects,
        [format!(
            "nvme connect -t tcp -n {NQN} -a 192.0.2.20 -s 4420 --reconnect-delay=10 --ctrl-loss-tmo=-1"
        )],
        "single path: no --fast_io_fail_tmo, so I/O queues through a NAS reboot"
    );
    assert_eq!(
        std::fs::read_link(n.path("staging/globalmount"))
            .unwrap()
            .to_string_lossy(),
        device(&n)
    );
    assert!(
        n.state.nvme_sessions.as_ref().unwrap().has(NQN),
        "recorded for session GC"
    );
    assert_eq!(n.state.metrics.node_connects("nvmeof", "success"), 1);
    assert_eq!(n.state.metrics.node_connects("nvmeof-ublk", "success"), 0);

    // A replay verifies the device's subsystem and connects nothing.
    let before = nvme_calls(&n).len();
    node_stage(&n.state, &stage_request(&n, block(), None), None)
        .await
        .unwrap();
    assert!(
        !nvme_calls(&n)[before..].iter().any(|c| c.starts_with("nvme connect")),
        "{:?}",
        nvme_calls(&n)
    );
}

#[tokio::test]
async fn multipath_stage_converges_every_path() {
    let n = kernel_node(MULTI);
    n.host.0.lock().unwrap().kernel.as_mut().unwrap().unreachable = vec!["192.0.2.22".into()];
    node_stage(&n.state, &stage_request(&n, filesystem(), Some(HINT)), None)
        .await
        .unwrap();
    let connects: Vec<String> = nvme_calls(&n)
        .into_iter()
        .filter(|c| c.starts_with("nvme connect"))
        .collect();
    assert_eq!(connects.len(), 3, "{connects:?}");
    assert!(
        connects.iter().all(|c| c.ends_with("--fast_io_fail_tmo=15")),
        "multipath: every path fails over after 15 s: {connects:?}"
    );
    let dev = device(&n);
    assert!(n.host.calls().contains(&format!("mkfs.ext4 -F {dev}")));
    for (address, result) in [
        ("192.0.2.20", "success"),
        ("192.0.2.21", "success"),
        ("192.0.2.22", "error"),
    ] {
        assert_eq!(n.state.metrics.nvme_path_connects(address, result), 1, "{address}");
    }
    let events = n.events.take();
    assert!(
        events
            .iter()
            .any(|(_, reason, message)| reason == events::REASON_NVME_PATH_DEGRADED && message.contains("192.0.2.22")),
        "{events:?}"
    );
    let iopolicy = n.dir.path().join("sys/class/nvme-subsystem/nvme-subsys0/iopolicy");
    assert_eq!(std::fs::read_to_string(iopolicy).unwrap(), "queue-depth");

    // A replay tops up the missing path once it is reachable.
    n.host.0.lock().unwrap().kernel.as_mut().unwrap().unreachable.clear();
    node_stage(&n.state, &stage_request(&n, filesystem(), Some(HINT)), None)
        .await
        .unwrap();
    assert_eq!(n.state.metrics.nvme_path_connects("192.0.2.22", "success"), 1);
    assert_eq!(n.state.metrics.nvme_path_connects("192.0.2.20", "already_live"), 1);
    let to = |address: &str| {
        nvme_calls(&n)
            .iter()
            .filter(|c| c.starts_with("nvme connect") && c.contains(&format!("-a {address} ")))
            .count()
    };
    assert_eq!(
        (to("192.0.2.20"), to("192.0.2.21"), to("192.0.2.22")),
        (1, 1, 2),
        "a live path is never reconnected"
    );
}

/// A fresh stage finds one path already live: its device is used and that
/// path is never reconnected; the others are connected.
#[tokio::test]
async fn an_already_live_path_is_not_reconnected() {
    let n = kernel_node(MULTI);
    n.host
        .0
        .lock()
        .unwrap()
        .kernel
        .as_mut()
        .unwrap()
        .add_live(NQN, "192.0.2.20");
    node_stage(&n.state, &stage_request(&n, block(), Some(HINT)), None)
        .await
        .unwrap();
    let connects: Vec<String> = nvme_calls(&n)
        .into_iter()
        .filter(|c| c.starts_with("nvme connect"))
        .collect();
    assert!(!connects.iter().any(|c| c.contains("-a 192.0.2.20 ")), "{connects:?}");
    assert_eq!(connects.len(), 2, "{connects:?}");
    assert_eq!(n.state.metrics.nvme_path_connects("192.0.2.20", "already_live"), 1);
    assert!(
        !nvme_calls(&n).iter().any(|c| c.starts_with("nvme disconnect")),
        "a live session is kept"
    );
}

#[tokio::test]
async fn a_dead_session_is_disconnected_before_the_connect() {
    let n = kernel_node(SINGLE);
    n.host
        .0
        .lock()
        .unwrap()
        .kernel
        .as_mut()
        .unwrap()
        .add_dead(NQN, "192.0.2.99");
    node_stage(&n.state, &stage_request(&n, block(), None), None)
        .await
        .unwrap();
    let calls = nvme_calls(&n);
    let disconnect = calls.iter().position(|c| *c == format!("nvme disconnect -n {NQN}"));
    let connect = calls.iter().position(|c| c.starts_with("nvme connect"));
    assert!(
        disconnect.is_some() && connect.is_some() && disconnect < connect,
        "{calls:?}"
    );

    // The same with multipath: a subsystem with no live path is disconnected.
    let n = kernel_node(MULTI);
    n.host
        .0
        .lock()
        .unwrap()
        .kernel
        .as_mut()
        .unwrap()
        .add_dead(NQN, "192.0.2.20");
    node_stage(&n.state, &stage_request(&n, block(), Some(HINT)), None)
        .await
        .unwrap();
    let calls = nvme_calls(&n);
    assert!(calls.contains(&format!("nvme disconnect -n {NQN}")), "{calls:?}");
}

/// The pre-emptive disconnect never tears down a session whose staged device
/// is still live (a race with another stage of the same volume).
#[tokio::test]
async fn a_live_staged_device_is_never_disconnected() {
    let n = kernel_node(SINGLE);
    node_stage(&n.state, &stage_request(&n, block(), None), None)
        .await
        .unwrap();
    let subsystems = n.state.nvme.list_subsystems(None).await.unwrap();
    let staging = n.path("staging/globalmount");
    let after = nvme_kernel::preemptive_disconnect(&n.state, NQN, &staging, subsystems, None).await;
    assert!(crate::nvme::has_subsystem(NQN, &after));
    assert!(
        !nvme_calls(&n).iter().any(|c| c.starts_with("nvme disconnect")),
        "{:?}",
        nvme_calls(&n)
    );

    // Once the link is gone, the same call disconnects.
    std::fs::remove_file(&staging).unwrap();
    let subsystems = n.state.nvme.list_subsystems(None).await.unwrap();
    let after = nvme_kernel::preemptive_disconnect(&n.state, NQN, &staging, subsystems, None).await;
    assert!(!crate::nvme::has_subsystem(NQN, &after));
}

/// Single path: any existing controller for the NQN suppresses another
/// connect (the Go node's historical rule), as when it cannot be dropped.
#[tokio::test]
async fn a_single_path_stage_never_connects_twice() {
    let n = kernel_node(SINGLE);
    {
        let mut host = n.host.0.lock().unwrap();
        let kernel = host.kernel.as_mut().unwrap();
        kernel.add_dead(NQN, "192.0.2.20");
        kernel.refuse_disconnect = true;
    }
    node_stage(&n.state, &stage_request(&n, block(), None), None)
        .await
        .unwrap();
    let calls = nvme_calls(&n);
    assert!(calls.contains(&format!("nvme disconnect -n {NQN}")), "{calls:?}");
    assert!(!calls.iter().any(|c| c.starts_with("nvme connect")), "{calls:?}");
}

#[tokio::test]
async fn every_path_failing_fails_the_stage() {
    let n = kernel_node(MULTI);
    n.host.0.lock().unwrap().kernel.as_mut().unwrap().unreachable =
        vec!["192.0.2.20".into(), "192.0.2.21".into(), "192.0.2.22".into()];
    let err = node_stage(&n.state, &stage_request(&n, block(), Some(HINT)), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message().contains("no requested NVMe-oF path connected"),
        "{}",
        err.message()
    );
    assert_eq!(n.state.metrics.node_connects("nvmeof", "error"), 1);
    assert!(
        n.events
            .take()
            .iter()
            .any(|(_, reason, _)| reason == events::REASON_NVME_CONNECT_FAILED)
    );
    assert!(
        n.state.nvme_sessions.as_ref().unwrap().has(NQN),
        "recorded even though the connect failed"
    );
}

#[tokio::test]
async fn a_mount_of_another_subsystem_is_already_exists() {
    let n = kernel_node(SINGLE);
    let staging = n.path("staging/globalmount");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::create_dir_all(n.dir.path().join("sys/class/nvme/nvme7")).unwrap();
    std::fs::write(n.dir.path().join("sys/class/nvme/nvme7/subsysnqn"), "nqn.other\n").unwrap();
    n.host.mount(&staging, "/dev/nvme7n1", "ext4");
    let err = node_stage(&n.state, &stage_request(&n, filesystem(), None), None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err:?}");
    assert!(!nvme_calls(&n).iter().any(|c| c.starts_with("nvme connect")));
}

#[tokio::test]
async fn split_multipath_is_reported() {
    let n = kernel_node(MULTI);
    let mut ctx = kernel_context();
    ctx.insert("csi.storage.k8s.io/pv/name".into(), VOLUME.into());
    let extra = n.dir.path().join("sys/class/nvme-subsystem/nvme-subsys9");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("subsysnqn"), format!("{NQN}\n")).unwrap();
    let mut req = stage_request(&n, block(), Some(HINT));
    req.volume_context = ctx;
    node_stage(&n.state, &req, None).await.unwrap();
    assert!(
        n.events
            .take()
            .iter()
            .any(|(_, reason, message)| reason == REASON_MULTIPATH_UNAGGREGATED && message.contains("split across 2")),
    );
}

#[tokio::test]
async fn the_kernel_stage_refuses_a_ublk_volume() {
    let n = kernel_node(SINGLE);
    let ctx = context(&[]);
    let capability = block();
    let err = nvme_kernel::stage(
        &n.state,
        crate::ublk_stage::StageRequest {
            volume_id: VOLUME,
            context: &ctx,
            staging: &n.path("stage"),
            capability: &capability,
            event: None,
            deadline: None,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert!(
        err.message()
            .contains("refusing to connect it with the kernel initiator")
    );
    assert!(nvme_calls(&n).is_empty());
}

#[test]
fn connect_options_follow_the_go_rule() {
    let options = |config: &str| nvme_kernel::connect_options(&node(config, HOST_NQN, |_| {}).state);
    assert_eq!(options(SINGLE).fast_io_fail_tmo, None, "single path: omitted");
    assert_eq!(options(MULTI).fast_io_fail_tmo, Some(15), "multipath: 15 s");
    assert_eq!(
        options("nvmeof:\n  transportAddress: 192.0.2.20\n  connect:\n    fastIOFailTmo: 5\n").fast_io_fail_tmo,
        Some(5),
        "an explicit value wins on a single path"
    );
    assert_eq!(
        options("nvmeof:\n  multipath: true\n  transportAddress: 192.0.2.20\n  connect:\n    fastIOFailTmo: -1\n")
            .fast_io_fail_tmo,
        None,
        "negative omits it"
    );
    let knobs = options(
        "nvmeof:\n  transportAddress: 192.0.2.20\n  connect:\n    nrIOQueues: 4\n    nrWriteQueues: 2\n    keepAliveTmo: 5\n",
    );
    assert_eq!(
        (knobs.nr_io_queues, knobs.nr_write_queues, knobs.keep_alive_tmo),
        (Some(4), Some(2), Some(5))
    );
}
