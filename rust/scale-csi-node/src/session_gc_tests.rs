//! Session GC and the fast_io_fail_tmo convergence, against a fake kernel.

use std::path::Path;
use std::time::Duration;

use tokio::sync::watch;

use crate::session_gc::{gc_nvmeof, kubelet_dir_of, pass, reconcile_tunables, staged_block_devices};
use crate::testing::{FakeKernel, HOST_NQN, NQN, Node, node};

const GC: &str = "nvmeof:\n  transportAddress: 192.0.2.20\nsessionGC:\n  enabled: true\n  gracePeriod: 1\n";

fn gc_node(config: &str) -> Node {
    let n = node(config, HOST_NQN, |_| {});
    n.daemon.state.lock().unwrap().forbidden = true;
    n.host.0.lock().unwrap().kernel = Some(FakeKernel::new(
        n.dir.path().join("sys"),
        n.daemon.dev_dir.path().to_path_buf(),
    ));
    n
}

fn kernel<R>(n: &Node, f: impl FnOnce(&mut FakeKernel) -> R) -> R {
    f(n.host.0.lock().unwrap().kernel.as_mut().unwrap())
}

fn stop() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    std::mem::forget(tx);
    rx
}

fn disconnects(n: &Node) -> Vec<String> {
    n.host
        .calls()
        .into_iter()
        .filter(|c| c.starts_with("nvme disconnect"))
        .collect()
}

/// A raw-block staging link to `device` under the driver's kubelet directory.
fn stage_link(n: &Node, device: &str) {
    let dir = n
        .dir
        .path()
        .join("kubelet/plugins/kubernetes.io/csi")
        .join(&n.state.driver_name)
        .join("hash/dev");
    std::fs::create_dir_all(&dir).unwrap();
    std::os::unix::fs::symlink(device, dir.join("vol")).unwrap();
}

#[tokio::test]
async fn an_orphan_is_disconnected_after_the_grace_period() {
    let n = gc_node(GC);
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    n.state.nvme_sessions.as_ref().unwrap().record(NQN).unwrap();

    gc_nvmeof(&n.state, &stop()).await;
    assert!(
        disconnects(&n).is_empty(),
        "the first sighting only starts the grace period"
    );
    assert_eq!(n.state.metrics.nvme_sessions(), 1);
    gc_nvmeof(&n.state, &stop()).await;
    assert!(disconnects(&n).is_empty(), "still within the grace period");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_nvmeof(&n.state, &stop()).await;
    assert_eq!(disconnects(&n), [format!("nvme disconnect -n {NQN}")]);
    assert_eq!(n.state.metrics.gc_disconnects("nvmeof"), 1);
    assert!(!n.state.nvme_sessions.as_ref().unwrap().has(NQN), "forgotten");
}

#[tokio::test]
async fn sessions_that_are_not_ours_or_in_use_are_kept() {
    let n = gc_node(GC);
    let staged = "nqn.2011-06.com.example:staged";
    kernel(&n, |k| {
        k.add_live(staged, "192.0.2.20"); // subsystem 0, controller nvme0: in use below
        k.add_live(NQN, "192.0.2.20"); // not recorded by this plugin
        k.add_live("nqn.2011-06.com.example:elsewhere", "198.51.100.9"); // another portal
    });
    let registry = n.state.nvme_sessions.as_ref().unwrap();
    registry.record(staged).unwrap();
    registry.record("nqn.2011-06.com.example:elsewhere").unwrap();
    let device = kernel(&n, |k| k.device(staged).unwrap());
    stage_link(&n, &device);

    for _ in 0..2 {
        gc_nvmeof(&n.state, &stop()).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }
    gc_nvmeof(&n.state, &stop()).await;
    assert!(disconnects(&n).is_empty(), "{:?}", disconnects(&n));
    assert_eq!(n.state.metrics.nvme_sessions(), 3);
}

/// A staged session is recorded as this plugin's, so it is collectable once
/// it is orphaned even if the stage predates the registry.
#[tokio::test]
async fn a_staged_session_is_adopted() {
    let n = gc_node(GC);
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    let device = kernel(&n, |k| k.device(NQN).unwrap());
    stage_link(&n, &device);
    assert!(!n.state.nvme_sessions.as_ref().unwrap().has(NQN));
    gc_nvmeof(&n.state, &stop()).await;
    assert!(n.state.nvme_sessions.as_ref().unwrap().has(NQN));
    assert!(disconnects(&n).is_empty());
}

/// One in-use NVMe device whose identity cannot be read makes the expected set
/// incomplete: the pass does nothing.
#[tokio::test]
async fn an_unreadable_device_skips_the_pass() {
    let n = gc_node(GC);
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    n.state.nvme_sessions.as_ref().unwrap().record(NQN).unwrap();
    let mounted = n.daemon.dev_dir.path().join("nvme9n1");
    std::fs::write(&mounted, b"").unwrap();
    n.host.mount(&n.path("mnt"), mounted.to_str().unwrap(), "ext4");
    gc_nvmeof(&n.state, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_nvmeof(&n.state, &stop()).await;
    assert!(disconnects(&n).is_empty());
}

#[tokio::test]
async fn a_dry_run_disconnects_nothing() {
    let n = gc_node(&format!("{GC}  dryRun: true\n"));
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    n.state.nvme_sessions.as_ref().unwrap().record(NQN).unwrap();
    gc_nvmeof(&n.state, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_nvmeof(&n.state, &stop()).await;
    assert!(disconnects(&n).is_empty());
}

/// A record without a session is forgotten once older than the grace period;
/// a younger one may be a stage in progress.
#[tokio::test]
async fn stale_records_are_pruned() {
    let n = gc_node(GC);
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    let registry = n.state.nvme_sessions.as_ref().unwrap();
    registry.record("nqn.gone").unwrap();
    gc_nvmeof(&n.state, &stop()).await;
    assert!(registry.has("nqn.gone"), "young: may be a stage in progress");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    gc_nvmeof(&n.state, &stop()).await;
    assert!(!registry.has("nqn.gone"));
}

#[tokio::test]
async fn a_pass_without_cleanup_only_counts() {
    let n = gc_node(GC);
    kernel(&n, |k| k.add_live(NQN, "192.0.2.20"));
    n.state.nvme_sessions.as_ref().unwrap().record(NQN).unwrap();
    pass(&n.state, false, &stop()).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    pass(&n.state, false, &stop()).await;
    assert!(disconnects(&n).is_empty());
    assert_eq!(n.state.metrics.nvme_sessions(), 1);
}

#[tokio::test]
async fn fast_io_fail_tmo_converges_on_this_drivers_controllers_only() {
    let n = gc_node("nvmeof:\n  transportAddress: 192.0.2.20\n  multipath: true\n  addresses: [192.0.2.21]\n");
    let (ours, foreign, other_portal) = kernel(&n, |k| {
        let ours = k.add_controller(NQN, "192.0.2.20", "live", "off");
        let foreign = k.add_controller("nqn.foreign", "192.0.2.21", "live", "off");
        let other = k.add_controller(NQN, "198.51.100.9", "live", "off");
        (ours, foreign, other)
    });
    let device = kernel(&n, |k| k.device(NQN).unwrap());
    stage_link(&n, &device);
    reconcile_tunables(&n.state).await;
    let value = |ctrl: &str| {
        std::fs::read_to_string(n.dir.path().join("sys/class/nvme").join(ctrl).join("fast_io_fail_tmo"))
            .unwrap()
            .trim()
            .to_string()
    };
    assert_eq!(value(&ours), "15", "multipath: what a fresh connect sets");
    assert_eq!(value(&foreign), "off", "another subsystem is not ours");
    assert_eq!(value(&other_portal), "off", "another portal is not ours");
    assert_eq!(n.state.metrics.tunable_corrections("fast_io_fail_tmo", "corrected"), 1);

    // Converged: nothing more to do.
    reconcile_tunables(&n.state).await;
    assert_eq!(n.state.metrics.tunable_corrections("fast_io_fail_tmo", "corrected"), 1);
}

#[test]
fn the_kubelet_directory_comes_from_the_socket() {
    assert_eq!(
        kubelet_dir_of(Path::new("/var/lib/k0s/kubelet/plugins/csi.scale.io/csi.sock")).as_deref(),
        Some(Path::new("/var/lib/k0s/kubelet"))
    );
    assert_eq!(kubelet_dir_of(Path::new("/tmp/csi.sock")), None);
    assert_eq!(kubelet_dir_of(Path::new("/csi/csi.sock")), None);
}

#[test]
fn staged_links_are_found_and_mounts_are_not_descended() {
    let dir = tempfile::tempdir().unwrap();
    let dev = dir.path().join("dev");
    std::fs::create_dir_all(&dev).unwrap();
    std::fs::write(dev.join("nvme0n1"), b"").unwrap();
    let root = dir.path().join("csi");
    std::fs::create_dir_all(root.join("drv/a/dev")).unwrap();
    std::os::unix::fs::symlink(dev.join("nvme0n1"), root.join("drv/a/dev/vol")).unwrap();
    // A mounted filesystem volume: never walked into.
    std::fs::create_dir_all(root.join("drv/b/globalmount/data")).unwrap();
    std::os::unix::fs::symlink("/nonexistent", root.join("drv/b/globalmount/data/link")).unwrap();
    let found = staged_block_devices(&root, &dev).unwrap();
    assert_eq!(found.len(), 1);
    assert!(found.contains_key(dev.join("nvme0n1").to_str().unwrap()));
    // An unresolvable staging link fails the scan.
    std::os::unix::fs::symlink("/nonexistent/nvme3n1", root.join("drv/a/dev/stale")).unwrap();
    assert!(staged_block_devices(&root, &dev).is_err());
    assert!(staged_block_devices(&dir.path().join("none"), &dev).unwrap().is_empty());
}
