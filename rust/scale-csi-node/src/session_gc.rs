//! Node session GC and NVMe-oF controller tunables (`pkg/driver/driver.go`
//! startSessionGC, gcSessions, gcISCSISessions, gcNVMeoFSessions,
//! pruneSessionRegistry; `nvme_tunables.go`). iSCSI's rules are in
//! [`gc_iscsi`].
//!
//! A kernel NVMe-oF session is disconnected only when every one of these holds:
//! it reaches one of this install's portals, this plugin recorded connecting it,
//! no staged volume on the node uses it, and it stayed that way for the grace
//! period. The expected set is built from every in-use block device (mounts and
//! raw-block links under kubelet's CSI staging directory); one NVMe device whose
//! identity cannot be read makes the set incomplete, and the pass is skipped.
//!
//! Every tick also converges `fast_io_fail_tmo` on controllers this driver
//! staged, to the value a fresh connect would set.
//!
//! The kubelet directory comes from the CSI socket path
//! (`<kubelet>/plugins/<driver>/csi.sock`); the Go node hard-codes
//! `/var/lib/kubelet`, so on another kubelet directory its GC cannot see the
//! raw-block volumes staged there.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Result};
use log::{debug, info, warn};
use tokio::sync::watch;

use crate::nvme::path_field;
use crate::nvme_kernel::connect_options;
use crate::service::State;

/// Orphaned sessions by NQN, with when each was first seen orphaned. In memory:
/// a restart restarts every grace period.
#[derive(Default)]
pub struct Orphans(pub(crate) Mutex<HashMap<String, Instant>>);

/// The kubelet directory from a socket at `<kubelet>/plugins/<name>/csi.sock`.
pub fn kubelet_dir_of(socket: &Path) -> Option<PathBuf> {
    let plugins = socket.parent()?.parent()?;
    (plugins.file_name()? == "plugins").then(|| plugins.parent().map(Path::to_path_buf))?
}

fn staging_root(state: &State) -> PathBuf {
    state.host.kubelet_dir.join("plugins/kubernetes.io/csi")
}

/// Raw-block staging links under the staging root, by device (Go
/// GetStagedBlockDevices). A missing root is empty; a link that cannot be
/// resolved fails the scan. `globalmount` directories are mounted volumes and
/// are not descended into.
pub fn staged_block_devices(root: &Path, dev_dir: &Path) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    if !root.exists() {
        return Ok(out);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).with_context(|| format!("scan {}", dir.display()))? {
            let entry = entry.with_context(|| format!("scan {}", dir.display()))?;
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                let device = std::fs::canonicalize(&path)
                    .with_context(|| format!("failed to resolve staged block device symlink {}", path.display()))?;
                if device.starts_with(dev_dir) {
                    out.insert(
                        device.to_string_lossy().into_owned(),
                        path.to_string_lossy().into_owned(),
                    );
                }
            } else if kind.is_dir() && entry.file_name() != "globalmount" {
                stack.push(path);
            }
        }
    }
    Ok(out)
}

/// The whole disk of a partition (`nvme0n1p2` -> `nvme0n1`), else the device.
fn whole_disk(state: &State, device: &str) -> String {
    let Some(name) = Path::new(device).file_name() else {
        return device.to_string();
    };
    let entry = state.host.sysfs.join("class/block").join(name);
    if !entry.join("partition").exists() {
        return device.to_string();
    }
    match std::fs::canonicalize(&entry) {
        Ok(resolved) => match (Path::new(device).parent(), resolved.parent().and_then(Path::file_name)) {
            (Some(dir), Some(parent)) => dir.join(parent).to_string_lossy().into_owned(),
            _ => device.to_string(),
        },
        Err(_) => device.to_string(),
    }
}

/// Every in-use block device: mounted, or linked under kubelet's staging
/// directory (Go getInUseBlockDevices); `None` when either scan failed.
async fn in_use_devices(state: &State) -> Option<HashSet<String>> {
    let mut devices: HashSet<String> = match state.mounter.block_device_mounts(&state.host.dev_dir, None).await {
        Ok(mounts) => mounts.into_keys().collect(),
        Err(e) => {
            warn!("Session GC: failed to get in-use block devices: {e:#}");
            return None;
        }
    };
    match staged_block_devices(&staging_root(state), &state.host.dev_dir) {
        Ok(staged) => devices.extend(staged.into_keys()),
        Err(e) => {
            warn!("Session GC: failed to get in-use block devices: scan staged block devices: {e:#}");
            return None;
        }
    }
    Some(devices)
}

/// The NQNs of every staged NVMe-oF volume, or `None` when the set cannot be
/// trusted (the scan failed, or an NVMe device's identity is unreadable).
async fn expected_nqns(state: &State) -> Option<HashSet<String>> {
    let devices = in_use_devices(state).await?;
    let mut expected = HashSet::new();
    let mut failed = 0;
    for device in devices {
        let disk = whole_disk(state, &device);
        match state.nvme.nqn_of_device(&disk) {
            Ok(nqn) if !nqn.is_empty() => {
                expected.insert(nqn);
            }
            Ok(_) => {}
            Err(e) => {
                let likely = Path::new(&disk)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("nvme"));
                if likely {
                    debug!("Session GC: failed to get NVMe info for {device} (may be race condition): {e:#}");
                    failed += 1;
                }
            }
        }
    }
    if failed > 0 {
        warn!("Session GC: {failed} NVMe device lookups failed, skipping GC to avoid race condition");
        return None;
    }
    Some(expected)
}

fn targets(state: &State) -> Vec<String> {
    let n = &state.config.nvmeof;
    let addresses = n.multipath_addresses();
    if addresses.is_empty() {
        vec![n.transport_address.clone()]
    } else {
        addresses
    }
}

/// One NVMe-oF GC pass (Go gcNVMeoFSessions + pruneSessionRegistry).
pub async fn gc_nvmeof(state: &State, stop: &watch::Receiver<bool>) {
    let Some(registry) = &state.nvme_sessions else {
        warn!("Session GC: skipping NVMe-oF GC: no session registry to prove which sessions this plugin connected");
        return;
    };
    let gc = &state.config.session_gc;
    let grace = gc.grace_period();
    let subsystems = match state.nvme.list_subsystems(None).await {
        Ok(subsystems) => subsystems,
        Err(e) => {
            warn!("Session metrics: failed to list NVMe-oF sessions: {e:#}");
            return;
        }
    };
    state.metrics.set_nvme_sessions(subsystems.len());
    if subsystems.is_empty() {
        return;
    }
    let Some(expected) = expected_nqns(state).await else {
        info!("Session GC: skipping NVMe-oF GC due to unreliable expected-session lookup");
        return;
    };
    // Staged volumes' sessions are this plugin's: recorded, so one staged
    // before the registry existed becomes collectable once orphaned.
    if !gc.dry_run {
        for nqn in &expected {
            if let Err(e) = registry.record(nqn) {
                warn!("Session GC: {e:#}");
            }
        }
    }
    let targets = targets(state);
    let now = Instant::now();
    let mut listed: HashSet<String> = HashSet::new();
    let mut orphaned: HashSet<String> = HashSet::new();
    for subsystem in &subsystems {
        if *stop.borrow() {
            debug!("Session GC: stopping NVMe-oF cleanup after cancellation");
            return;
        }
        let nqn = subsystem.nqn.as_str();
        listed.insert(nqn.to_string());
        // Any path to any configured portal puts it in scope; one with no path
        // at all is a leak candidate.
        let reaches_ours = subsystem.paths.is_empty()
            || subsystem
                .paths
                .iter()
                .any(|p| path_field(&p.address, "traddr").is_some_and(|a| targets.contains(&a)));
        if !reaches_ours || !registry.has(nqn) {
            continue;
        }
        if expected.contains(nqn) {
            state.orphans.0.lock().unwrap().remove(nqn);
            continue;
        }
        orphaned.insert(nqn.to_string());
        let first_seen = {
            let mut seen = state.orphans.0.lock().unwrap();
            match seen.get(nqn) {
                Some(first) => Some(*first),
                None => {
                    seen.insert(nqn.to_string(), now);
                    None
                }
            }
        };
        let Some(first_seen) = first_seen else {
            info!("Session GC: found newly orphaned NVMe-oF session: {nqn} (will disconnect after {grace:?})");
            continue;
        };
        let orphaned_for = now.duration_since(first_seen);
        if orphaned_for < grace {
            debug!("Session GC: orphaned session {nqn} within grace period ({orphaned_for:?} < {grace:?})");
            continue;
        }
        info!("Session GC: orphaned NVMe-oF session {nqn} exceeded grace period ({orphaned_for:?})");
        if gc.dry_run {
            info!("Session GC: [DRY RUN] would disconnect orphaned session: {nqn}");
            continue;
        }
        match state.nvme.disconnect(nqn, None).await {
            Ok(()) => {
                info!("Session GC: disconnected orphaned NVMe-oF session: {nqn}");
                state.metrics.record_gc_disconnect("nvmeof");
                state.orphans.0.lock().unwrap().remove(nqn);
                if let Err(e) = registry.forget(nqn) {
                    warn!("Session GC: {e:#}");
                }
            }
            Err(e) => warn!("Session GC: failed to disconnect orphaned session {nqn}: {e:#}"),
        }
    }
    state.orphans.0.lock().unwrap().retain(|nqn, _| orphaned.contains(nqn));

    // Forget records whose session is gone and not staged; a young record may
    // be a stage in progress (it is written before the connect).
    if !gc.dry_run {
        match registry.entries() {
            Ok(entries) => {
                for (nqn, recorded) in entries {
                    let young = SystemTime::now().duration_since(recorded).is_ok_and(|age| age < grace)
                        || SystemTime::now() < recorded;
                    if listed.contains(&nqn) || expected.contains(&nqn) || young {
                        continue;
                    }
                    if let Err(e) = registry.forget(&nqn) {
                        warn!("Session GC: {e:#}");
                    }
                }
            }
            Err(e) => warn!("Session GC: list session registry: {e:#}"),
        }
    }
}

/// The configured portal's iSCSI sessions, listed (and counted in the gauge).
async fn observe_iscsi(state: &State) -> Option<Vec<crate::iscsi::Session>> {
    match state.iscsi.list_sessions(None).await {
        Ok(sessions) => {
            state.metrics.set_iscsi_sessions(sessions.len());
            Some(sessions)
        }
        Err(e) => {
            warn!("Session metrics: failed to list iSCSI sessions: {e:#}");
            None
        }
    }
}

/// One iSCSI GC pass (Go gcISCSISessions): a session is logged out only when it
/// goes through the configured portal, its target is one this driver names, no
/// staged volume uses it, and that stayed so for the grace period. An in-use
/// device that may be iSCSI and cannot be identified skips the pass.
pub async fn gc_iscsi(state: &State, stop: &watch::Receiver<bool>) {
    let gc = &state.config.session_gc;
    let grace = gc.grace_period();
    let portal = state.config.iscsi.target_portal.clone();
    let Some(sessions) = observe_iscsi(state).await else {
        return;
    };
    if sessions.is_empty() {
        return;
    }
    let Some(devices) = in_use_devices(state).await else {
        info!("Session GC: skipping iSCSI GC due to unreliable expected-session lookup");
        return;
    };
    let mut devices: Vec<String> = devices.into_iter().collect();
    devices.sort();
    let Some(expected) = crate::iscsi_stage::expected_targets(state, &devices, &sessions) else {
        info!("Session GC: skipping iSCSI GC due to unreliable expected-session lookup");
        return;
    };
    let now = Instant::now();
    let mut orphaned: HashSet<String> = HashSet::new();
    for session in &sessions {
        if *stop.borrow() {
            debug!("Session GC: stopping iSCSI cleanup after cancellation");
            return;
        }
        if !crate::iscsi_stage::gc_in_scope(state, session) {
            continue;
        }
        let iqn = session.iqn.as_str();
        if expected.contains(iqn) {
            state.iscsi_orphans.0.lock().unwrap().remove(iqn);
            continue;
        }
        orphaned.insert(iqn.to_string());
        let first_seen = {
            let mut seen = state.iscsi_orphans.0.lock().unwrap();
            match seen.get(iqn) {
                Some(first) => Some(*first),
                None => {
                    seen.insert(iqn.to_string(), now);
                    None
                }
            }
        };
        let Some(first_seen) = first_seen else {
            info!("Session GC: found newly orphaned iSCSI session: {iqn} (will disconnect after {grace:?})");
            continue;
        };
        let orphaned_for = now.duration_since(first_seen);
        if orphaned_for < grace {
            debug!("Session GC: orphaned session {iqn} within grace period ({orphaned_for:?} < {grace:?})");
            continue;
        }
        info!("Session GC: orphaned iSCSI session {iqn} exceeded grace period ({orphaned_for:?})");
        if gc.dry_run {
            info!("Session GC: [DRY RUN] would disconnect orphaned session: {iqn}");
            continue;
        }
        match state.iscsi.logout(&portal, iqn).await {
            Ok(()) => {
                info!("Session GC: disconnected orphaned iSCSI session: {iqn}");
                state.metrics.record_gc_disconnect("iscsi");
                state.iscsi_orphans.0.lock().unwrap().remove(iqn);
            }
            Err(e) => warn!("Session GC: failed to disconnect orphaned session {iqn}: {e:#}"),
        }
    }
    state
        .iscsi_orphans
        .0
        .lock()
        .unwrap()
        .retain(|iqn, _| orphaned.contains(iqn));
}

/// Session gauges only: no cleanup.
async fn observe(state: &State) {
    match state.nvme.list_subsystems(None).await {
        Ok(subsystems) => state.metrics.set_nvme_sessions(subsystems.len()),
        Err(e) => warn!("Session metrics: failed to list NVMe-oF sessions: {e:#}"),
    }
}

struct Controller {
    name: String,
    transport: String,
    address: String,
    nqn: String,
    fast_io_fail_tmo: i64,
}

fn read_trimmed(path: &Path) -> String {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn fabrics_controllers(state: &State) -> Result<Vec<Controller>> {
    let root = state.nvme.sysfs.join("class/nvme");
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", root.display())),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with("nvme") {
            continue;
        }
        let dir = root.join(&name);
        let transport = read_trimmed(&dir.join("transport"));
        if transport.is_empty() || transport == "pcie" {
            continue;
        }
        let raw = read_trimmed(&dir.join("fast_io_fail_tmo"));
        let fast_io_fail_tmo = if raw == "off" {
            -1
        } else if let Ok(v) = raw.parse() {
            v
        } else {
            continue;
        };
        out.push(Controller {
            name,
            transport,
            address: read_trimmed(&dir.join("address")),
            nqn: read_trimmed(&dir.join("subsysnqn")),
            fast_io_fail_tmo,
        });
    }
    Ok(out)
}

/// NQNs of NVMe devices staged under THIS driver's kubelet staging directory:
/// positive proof the controller is ours.
async fn staged_by_this_driver(state: &State) -> Result<HashSet<String>> {
    let root = staging_root(state).join(&state.driver_name);
    let mut candidates: HashSet<String> = HashSet::new();
    for (device, mounts) in state.mounter.block_device_mounts(&state.host.dev_dir, None).await? {
        if mounts.iter().any(|m| Path::new(m).starts_with(&root)) {
            candidates.insert(device);
        }
    }
    for (device, link) in staged_block_devices(&staging_root(state), &state.host.dev_dir)? {
        if Path::new(&link).starts_with(&root) {
            candidates.insert(device);
        }
    }
    Ok(candidates
        .into_iter()
        .filter_map(|device| state.nvme.nqn_of_device(&device).ok())
        .filter(|nqn| !nqn.is_empty())
        .collect())
}

/// Converges `fast_io_fail_tmo` on this driver's live controllers to what a
/// fresh connect would set (Go reconcileNVMeoFControllerTunables).
pub async fn reconcile_tunables(state: &State) {
    let n = &state.config.nvmeof;
    if n.transport_address.is_empty() {
        return;
    }
    let desired = connect_options(state).fast_io_fail_tmo.map_or(-1, |v| v as i64);
    let controllers = match fabrics_controllers(state) {
        Ok(controllers) => controllers,
        Err(e) => {
            warn!("NVMe-oF tunables: failed to list controllers: {e:#}");
            return;
        }
    };
    let targets = targets(state);
    let transport = n.transport.trim().to_lowercase();
    let port = n.transport_service_id.to_string();
    let mut owned: Option<HashSet<String>> = None;
    for ctrl in controllers {
        if ctrl.fast_io_fail_tmo == desired
            || (!transport.is_empty() && !ctrl.transport.eq_ignore_ascii_case(&transport))
            || (n.transport_service_id > 0 && path_field(&ctrl.address, "trsvcid").as_deref() != Some(port.as_str()))
            || !path_field(&ctrl.address, "traddr").is_some_and(|a| targets.contains(&a))
        {
            continue;
        }
        if owned.is_none() {
            match staged_by_this_driver(state).await {
                Ok(set) => owned = Some(set),
                Err(e) => {
                    warn!("NVMe-oF tunables: cannot prove controller ownership, skipping this pass: {e:#}");
                    return;
                }
            }
        }
        if !owned.as_ref().is_some_and(|o| o.contains(&ctrl.nqn)) {
            continue;
        }
        if state.config.session_gc.dry_run {
            info!(
                "NVMe-oF tunables (dry run): would set {} ({}) fast_io_fail_tmo {} -> {desired}",
                ctrl.name, ctrl.nqn, ctrl.fast_io_fail_tmo
            );
            continue;
        }
        let attr = state
            .nvme
            .sysfs
            .join("class/nvme")
            .join(&ctrl.name)
            .join("fast_io_fail_tmo");
        let written = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&attr)
            .and_then(|mut f| std::io::Write::write_all(&mut f, desired.to_string().as_bytes()));
        match written {
            Ok(()) => {
                state.metrics.record_tunable_correction("fast_io_fail_tmo", "corrected");
                info!(
                    "NVMe-oF tunables: set {} ({}) fast_io_fail_tmo {} -> {desired}",
                    ctrl.name, ctrl.nqn, ctrl.fast_io_fail_tmo
                );
            }
            Err(e) => {
                state.metrics.record_tunable_correction("fast_io_fail_tmo", "error");
                warn!(
                    "NVMe-oF tunables: failed to set {} ({}) fast_io_fail_tmo {} -> {desired}: {e}",
                    ctrl.name, ctrl.nqn, ctrl.fast_io_fail_tmo
                );
            }
        }
    }
}

/// One tick: iSCSI GC, NVMe-oF GC (or only their gauges), then the tunables.
pub async fn pass(state: &State, cleanup: bool, stop: &watch::Receiver<bool>) {
    let gc = &state.config.session_gc;
    // iSCSI is listed only where it is enabled: an NVMe-oF-only node never
    // runs iscsiadm.
    if state.config.iscsi_enabled {
        let iscsi =
            cleanup && gc.enabled && gc.iscsi_enabled.unwrap_or(true) && !state.config.iscsi.target_portal.is_empty();
        if iscsi {
            gc_iscsi(state, stop).await;
        } else {
            observe_iscsi(state).await;
        }
    }
    if *stop.borrow() {
        return;
    }
    let nvmeof =
        cleanup && gc.enabled && gc.nvmeof_enabled.unwrap_or(true) && !state.config.nvmeof.transport_address.is_empty();
    if nvmeof {
        gc_nvmeof(state, stop).await;
    } else {
        observe(state).await;
    }
    if !*stop.borrow() {
        reconcile_tunables(state).await;
    }
}

/// The loop: a startup delay, a first pass (cleanup only with runOnStartup),
/// then one pass per interval until `stop`.
pub async fn run(state: Arc<State>, mut stop: watch::Receiver<bool>) {
    let gc = state.config.session_gc.clone();
    if !gc.enabled {
        info!("Session cleanup disabled by configuration; active-session metrics remain enabled");
    }
    info!(
        "Session monitor started: interval={:?}, cleanup={}, gracePeriod={:?}, dryRun={}",
        gc.interval(),
        gc.enabled,
        gc.grace_period(),
        gc.dry_run
    );
    tokio::select! {
        _ = tokio::time::sleep(gc.startup_delay()) => {}
        _ = stop.changed() => return,
    }
    pass(&state, gc.enabled && gc.run_on_startup(), &stop).await;
    let mut ticker = tokio::time::interval(gc.interval());
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => pass(&state, true, &stop).await,
            _ = stop.changed() => {
                info!("Session GC stopped");
                return;
            }
        }
    }
}
