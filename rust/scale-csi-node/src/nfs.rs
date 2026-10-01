//! NFS on the node (`pkg/driver/node.go` stageNFSVolume, convergeNFSTrunks,
//! cleanupNFSTrunkProbeMounts; `pkg/util/mount.go` MountNFS;
//! `pkg/driver/nfs_options.go` nfsMountFlags).
//!
//! A stage is one `mount -t nfs -o nfsvers=4,<flags> server:share <path>`,
//! exactly the Go node's argv, so a volume staged by either plugin is the same
//! mount and either can take it over. The flags are the StorageClass mount
//! options as given (trimmed, de-duplicated), with `nconnect=` from
//! `nfs.nconnect` and, for a trunked volume, `max_connect=` replacing theirs.
//!
//! Trunking (NFSv4.1+): when the publish context carries more than one server
//! address, the first is mounted with `max_connect=<n>`, then every other one
//! is mounted once at a probe path beside the staging path and unmounted again,
//! so the client opens a transport to it on the same session. A trunk that
//! fails only degrades the volume (NFSTrunkingDegraded); a kernel that rejects
//! `max_connect` gets the plain primary mount (NFSTrunkingUnavailable).

use std::collections::HashMap;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::time::Instant;

use log::{debug, info, warn};
use tonic::Status;

use crate::capability::{self, nfs_source};
use crate::csi;
use crate::events::{self, ObjectRef};
use crate::nvme_addresses::normalize_address;
use crate::service::State;

/// The metric label of a discarded address hint (Go
/// invalidNFSTrunkingAddressMetricLabel).
const INVALID_HINT_LABEL: &str = "invalid-publish-context";
/// A trunk probe mounts at `<staging path><suffix><n>`.
const PROBE_SUFFIX: &str = ".scale-csi-nfs-trunk-";

fn warn_event(state: &State, event: Option<&ObjectRef>, reason: &str, message: &str) {
    if let Some(object) = event {
        state.events.warning(object, reason, message);
    }
}

/// The trunking addresses from the (publish-hinted) volume context: `None`
/// when there is no `addresses` key; an error when it is present but not a
/// non-empty JSON list of bare IPs (Go parseNFSTrunkingAddresses). Duplicates
/// are dropped, first one kept.
pub fn trunking_addresses(context: &HashMap<String, String>) -> Option<Result<Vec<String>, String>> {
    let raw = context.get("addresses")?;
    let parsed = (|| {
        let decoded: Option<Vec<String>> = serde_json::from_str(raw).map_err(|e| format!("decode addresses: {e}"))?;
        let decoded = decoded.unwrap_or_default();
        if decoded.is_empty() {
            return Err("addresses is empty".to_string());
        }
        let mut out: Vec<String> = Vec::with_capacity(decoded.len());
        for raw in &decoded {
            let address = normalize_address(raw)
                .map_err(|e| format!("addresses contains invalid server address {raw:?}: {e}"))?;
            if !out.contains(&address) {
                out.push(address);
            }
        }
        Ok(out)
    })();
    Some(parsed)
}

/// The StorageClass mount options for an NFS mount (Go nfsMountFlags): the
/// trimmed, de-duplicated list, never rewritten; conflicting versions are only
/// logged, since the kernel applies the last one.
pub fn mount_flags(capability: Option<&csi::VolumeCapability>) -> Vec<String> {
    let flags = capability.map(capability::mount_flags).unwrap_or_default();
    let mut versions: Vec<&str> = Vec::new();
    for flag in &flags {
        if let Some((key, value)) = flag.trim().split_once('=')
            && matches!(key.trim().to_lowercase().as_str(), "vers" | "nfsvers")
            && !versions.contains(&value.trim())
        {
            versions.push(value.trim());
        }
    }
    if versions.len() > 1 {
        warn!(
            "NFS mountOptions request conflicting versions {versions:?}; the kernel applies the LAST one. Passing them through unchanged."
        );
    }
    flags
}

/// The mount options of an NFS stage (Go configuredNFSMountFlags):
/// `nfs.nconnect` replaces any `nconnect=` in the options, and with two or
/// more trunk addresses `max_connect=<count>` replaces any `max_connect=`;
/// both are appended in that order.
pub fn configured_mount_flags(
    nconnect: Option<i64>,
    trunk_addresses: usize,
    capability: Option<&csi::VolumeCapability>,
) -> Vec<String> {
    let flags = mount_flags(capability);
    if nconnect.is_none() && trunk_addresses < 2 {
        return flags;
    }
    let mut out: Vec<String> = flags
        .into_iter()
        .filter(|flag| {
            let key = flag.split('=').next().unwrap_or_default().trim().to_lowercase();
            !(nconnect.is_some() && key == "nconnect" || trunk_addresses > 1 && key == "max_connect")
        })
        .collect();
    if let Some(n) = nconnect {
        out.push(format!("nconnect={n}"));
    }
    if trunk_addresses > 1 {
        out.push(format!("max_connect={trunk_addresses}"));
    }
    out
}

/// The NFS version a mount negotiated, from its `vers=`/`nfsvers=` option.
pub fn effective_version(options: &[String]) -> Option<f64> {
    options.iter().find_map(|option| {
        let (key, value) = option.split_once('=')?;
        if !matches!(key.trim().to_lowercase().as_str(), "vers" | "nfsvers") {
            return None;
        }
        value.trim().parse::<f64>().ok()
    })
}

fn printable_version(version: Option<f64>) -> String {
    match version {
        Some(v) => format!("NFS {v:.1}"),
        None => "an unknown version".to_string(),
    }
}

/// Mounts an NFS volume at `target`: the staging path, or for a direct
/// (unstaged) publish the publish target (Go stageNFSVolume).
pub async fn stage(
    state: &State,
    context: &HashMap<String, String>,
    target: &str,
    capability: Option<&csi::VolumeCapability>,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) -> Result<(), Status> {
    let get = |k: &str| context.get(k).map(String::as_str).unwrap_or_default();
    let (server, share) = (get("server"), get("share"));
    if server.is_empty() || share.is_empty() {
        return Err(Status::invalid_argument(
            "NFS server and share are required in volume context",
        ));
    }
    let hint = trunking_addresses(context);
    let present = hint.is_some();
    let addresses = match hint {
        Some(Err(e)) => {
            let message =
                format!("NFS trunking address list for {share} was discarded; using the primary server only: {e}");
            warn!("{message}");
            state.metrics.record_nfs_trunk_connect(INVALID_HINT_LABEL, "error");
            warn_event(state, event, events::REASON_NFS_TRUNKING_DEGRADED, &message);
            Vec::new()
        }
        Some(Ok(addresses)) => addresses,
        None => Vec::new(),
    };
    let trunking = present && addresses.len() > 1;
    let source = nfs_source(if trunking { &addresses[0] } else { server }, share);
    let nconnect = state.config.nfs.nconnect;

    let mounted = state
        .mounter
        .is_mounted(target, deadline)
        .await
        .map_err(|e| Status::internal(format!("failed to check mount status: {e:#}")))?;
    if mounted {
        if trunking {
            let flags = configured_mount_flags(nconnect, addresses.len(), capability);
            converge_trunks(state, &addresses, share, target, &flags, event, deadline).await;
        }
        info!("NFS already mounted at {target}");
        return Ok(());
    }

    let flags = configured_mount_flags(nconnect, addresses.len(), capability);
    let mut result = state.mounter.mount_nfs(&source, target, &flags, deadline).await;
    if let Err(trunked) = &result
        && trunking
    {
        // A kernel without max_connect rejects it before any version is
        // negotiated: retry once without it, so an optional availability
        // feature cannot keep the primary mount from working.
        let fallback = configured_mount_flags(nconnect, 0, capability);
        match state
            .mounter
            .mount_nfs(&nfs_source(server, share), target, &fallback, deadline)
            .await
        {
            Ok(()) => {
                let message = format!(
                    "NFS trunking options are unavailable for {share}; the primary mount succeeded without max_connect: {trunked:#}"
                );
                warn!("{message}");
                warn_event(state, event, events::REASON_NFS_TRUNKING_UNAVAILABLE, &message);
                state.metrics.record_node_connect("nfs", "success");
                return Ok(());
            }
            Err(untrunked) => {
                result = Err(anyhow::anyhow!(
                    "trunking mount failed: {trunked:#}; untrunked primary fallback failed: {untrunked:#}"
                ));
            }
        }
    }
    if let Err(e) = result {
        state.metrics.record_node_connect("nfs", "error");
        let status = Status::internal(format!("failed to mount NFS: {e:#}"));
        warn_event(state, event, events::REASON_NFS_MOUNT_FAILED, status.message());
        return Err(status);
    }
    state.metrics.record_node_connect("nfs", "success");
    if trunking {
        converge_trunks(state, &addresses, share, target, &flags, event, deadline).await;
    }
    Ok(())
}

/// Trunk convergence for a volume that is already staged (Go
/// convergeExistingNFSTrunks): only a usable hint of two or more addresses.
pub async fn converge_existing(
    state: &State,
    context: &HashMap<String, String>,
    staging: &str,
    capability: Option<&csi::VolumeCapability>,
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let Some(Ok(addresses)) = trunking_addresses(context) else {
        return;
    };
    let share = context.get("share").map(String::as_str).unwrap_or_default();
    if addresses.len() < 2 || share.is_empty() {
        return;
    }
    let flags = configured_mount_flags(state.config.nfs.nconnect, addresses.len(), capability);
    converge_trunks(state, &addresses, share, staging, &flags, event, deadline).await;
}

/// Opens a transport to every secondary server address by mounting it once
/// at a probe path and unmounting it again (Go convergeNFSTrunks). Only a
/// mount that negotiated NFS 4.1 or later can trunk.
async fn converge_trunks(
    state: &State,
    addresses: &[String],
    share: &str,
    staging: &str,
    flags: &[String],
    event: Option<&ObjectRef>,
    deadline: Option<Instant>,
) {
    let info = match state.mounter.mount_info(staging, deadline).await {
        Ok(info) => info,
        Err(e) => {
            let message = format!("cannot verify negotiated NFS version for trunking at {staging}: {e:#}");
            warn!("{message}");
            warn_event(state, event, events::REASON_NFS_TRUNKING_UNAVAILABLE, &message);
            return;
        }
    };
    let version = effective_version(&info.options);
    if version.is_none_or(|v| v < 4.1) {
        let message = format!(
            "NFS trunking requires a negotiated NFS version of at least 4.1; {staging} is mounted with {} and remains available through its primary server",
            printable_version(version)
        );
        warn!("{message}");
        warn_event(state, event, events::REASON_NFS_TRUNKING_UNAVAILABLE, &message);
        return;
    }
    let mut failures: Vec<String> = Vec::new();
    for (index, address) in addresses.iter().enumerate().skip(1) {
        let probe = format!("{staging}{PROBE_SUFFIX}{index}");
        if let Err(e) = std::fs::DirBuilder::new().recursive(true).mode(0o750).create(&probe) {
            state.metrics.record_nfs_trunk_connect(address, "error");
            failures.push(format!("{address}: create probe mountpoint: {e}"));
            continue;
        }
        if let Err(e) = state
            .mounter
            .mount_nfs(&nfs_source(address, share), &probe, flags, deadline)
            .await
        {
            state.metrics.record_nfs_trunk_connect(address, "error");
            failures.push(format!("{address}: {e:#}"));
            let _ = std::fs::remove_dir(&probe);
            continue;
        }
        state.metrics.record_nfs_trunk_connect(address, "success");
        if let Err(e) = state.mounter.unmount(&probe, deadline).await {
            failures.push(format!("{address}: probe unmount: {e:#}"));
            continue;
        }
        let _ = std::fs::remove_dir(&probe);
    }
    if !failures.is_empty() {
        warn_event(
            state,
            event,
            events::REASON_NFS_TRUNKING_DEGRADED,
            &format!(
                "NFS trunk transport convergence for {share} is degraded: {}",
                failures.join("\n")
            ),
        );
    }
}

/// Unmounts and removes trunk probe mounts a stage left beside `staging`
/// (Go cleanupNFSTrunkProbeMounts); every unstage runs it first.
pub async fn cleanup_trunk_probes(state: &State, staging: &str, deadline: Option<Instant>) {
    let path = Path::new(staging);
    let (Some(parent), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
        return;
    };
    let prefix = format!("{name}{PROBE_SUFFIX}");
    let Ok(entries) = std::fs::read_dir(if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    }) else {
        return;
    };
    let mut probes: Vec<String> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| n.starts_with(&prefix))
        .map(|n| parent.join(n).to_string_lossy().into_owned())
        .collect();
    probes.sort();
    for probe in probes {
        if matches!(state.mounter.is_mounted(&probe, deadline).await, Ok(true))
            && let Err(e) = state.mounter.unmount(&probe, deadline).await
        {
            warn!("Failed to clean NFS trunk probe mount {probe}: {e:#}");
            continue;
        }
        let removed = match std::fs::symlink_metadata(&probe) {
            Ok(meta) if meta.is_dir() => std::fs::remove_dir(&probe),
            Ok(_) => std::fs::remove_file(&probe),
            Err(e) => Err(e),
        };
        if let Err(e) = removed
            && e.kind() != std::io::ErrorKind::NotFound
        {
            debug!("Failed to remove NFS trunk probe path {probe}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mount_capability(flags: &[&str]) -> csi::VolumeCapability {
        csi::VolumeCapability {
            access_type: Some(csi::volume_capability::AccessType::Mount(
                csi::volume_capability::MountVolume {
                    mount_flags: flags.iter().map(|s| s.to_string()).collect(),
                    ..Default::default()
                },
            )),
            access_mode: None,
        }
    }

    #[test]
    fn configured_flags_follow_go() {
        let cap = mount_capability(&[" nfsvers=4.2 ", "nconnect=2", "hard", "hard", "max_connect=9", ""]);
        assert_eq!(
            configured_mount_flags(None, 0, Some(&cap)),
            ["nfsvers=4.2", "nconnect=2", "hard", "max_connect=9"],
            "no nconnect and no trunking: the options as given, de-duplicated"
        );
        assert_eq!(
            configured_mount_flags(Some(8), 1, Some(&cap)),
            ["nfsvers=4.2", "hard", "max_connect=9", "nconnect=8"],
            "nfs.nconnect replaces the StorageClass's"
        );
        assert_eq!(
            configured_mount_flags(None, 3, Some(&cap)),
            ["nfsvers=4.2", "nconnect=2", "hard", "max_connect=3"],
            "trunking replaces max_connect"
        );
        assert_eq!(
            configured_mount_flags(Some(4), 2, Some(&cap)),
            ["nfsvers=4.2", "hard", "nconnect=4", "max_connect=2"]
        );
        assert!(configured_mount_flags(None, 0, None).is_empty());
        let upper = mount_capability(&["NConnect = 3", "Max_Connect=5"]);
        assert_eq!(
            configured_mount_flags(Some(1), 2, Some(&upper)),
            ["nconnect=1", "max_connect=2"],
            "keys compare trimmed and case-insensitively"
        );
    }

    #[test]
    fn trunking_hints() {
        let ctx = |v: &str| HashMap::from([("addresses".to_string(), v.to_string())]);
        assert!(trunking_addresses(&HashMap::new()).is_none());
        assert_eq!(
            trunking_addresses(&ctx(r#"["192.0.2.10","[2001:db8::10]","192.0.2.10"]"#)),
            Some(Ok(vec!["192.0.2.10".to_string(), "2001:db8::10".to_string()]))
        );
        for bad in ["[]", "null", "nope", r#"["192.0.2.10:2049"]"#, r#"[" 192.0.2.10"]"#] {
            assert!(
                matches!(trunking_addresses(&ctx(bad)), Some(Err(_))),
                "{bad} must be discarded"
            );
        }
    }

    #[test]
    fn versions() {
        let opts = |s: &str| s.split(',').map(str::to_string).collect::<Vec<_>>();
        assert_eq!(effective_version(&opts("rw,vers=4.2,proto=tcp")), Some(4.2));
        assert_eq!(effective_version(&opts("rw,NFSVERS = 3")), Some(3.0));
        assert_eq!(effective_version(&opts("rw,vers=x,vers=4.1")), Some(4.1));
        assert_eq!(effective_version(&opts("rw")), None);
        assert_eq!(printable_version(Some(4.0)), "NFS 4.0");
        assert_eq!(printable_version(None), "an unknown version");
    }
}
