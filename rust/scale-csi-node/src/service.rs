//! The CSI Identity and Node services. Volume RPCs arrive with their protocol
//! slices; until then they answer Unimplemented (or FailedPrecondition for a
//! volume on a path not ported yet), and the agent refuses to start on an
//! install that enables a protocol it does not serve.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use log::{debug, error, info, trace};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::config::Config;
use crate::csi::{self, identity_server::Identity, node_server::Node};
use crate::events::{Events, LogEvents};
use crate::iscsi::Iscsi;
use crate::locks::OperationLocks;
use crate::metrics::Metrics;
use crate::mount::{CLocaleRunner, Mounter, Timeouts};
use crate::nvme::Nvme;
use crate::records::Records;
use crate::session_registry::SessionRegistry;
use crate::ublk_client::{self, Daemon};

/// A path's device number, when it is a block device.
pub type BlockDeviceNumber = Arc<dyn Fn(&str) -> std::io::Result<Option<u64>> + Send + Sync>;

fn block_device_number(path: &str) -> std::io::Result<Option<u64>> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let meta = std::fs::metadata(path)?;
    Ok(meta.file_type().is_block_device().then(|| meta.rdev()))
}

/// lstat(2) of a path the node looks at directly (never one that may be a
/// network mount: on a dead server it blocks).
pub type PathProbe = Arc<dyn Fn(&str) -> std::io::Result<std::fs::Metadata> + Send + Sync>;

/// readlink(2) of a staging path.
pub type LinkReader = Arc<dyn Fn(&str) -> std::io::Result<PathBuf> + Send + Sync>;

/// Where the node looks at the host; the tests point it elsewhere.
pub struct Host {
    /// Where block devices appear.
    pub dev_dir: PathBuf,
    /// Kubelet's root directory (its CSI staging directory is under it).
    pub kubelet_dir: PathBuf,
    pub sysfs: PathBuf,
    pub host_id_files: Vec<PathBuf>,
    /// stat(2) of a block device's number (the tests supply their own).
    pub device_number: BlockDeviceNumber,
    /// lstat(2) and readlink(2) of a path (the tests watch which paths).
    pub lstat: PathProbe,
    pub read_link: LinkReader,
}

impl Default for Host {
    fn default() -> Self {
        Host {
            dev_dir: PathBuf::from("/dev"),
            kubelet_dir: PathBuf::from("/var/lib/kubelet"),
            sysfs: PathBuf::from("/sys"),
            host_id_files: crate::ublk_state::default_host_id_files(),
            device_number: Arc::new(block_device_number),
            lstat: Arc::new(|path: &str| std::fs::symlink_metadata(path)),
            read_link: Arc::new(|path: &str| std::fs::read_link(path)),
        }
    }
}

pub struct State {
    pub metrics: Arc<Metrics>,
    pub driver_name: String,
    pub version: String,
    /// The encoded node id NodeGetInfo reports.
    pub node_id: String,
    /// The Kubernetes node name.
    pub node_name: String,
    pub config: Config,
    pub ready: AtomicBool,
    pub mounter: Mounter,
    pub locks: OperationLocks,
    pub records: Records,
    pub ublk: Arc<dyn Daemon>,
    pub events: Arc<dyn Events>,
    pub host: Host,
    /// The kernel NVMe-oF initiator (nvme-cli and sysfs).
    pub nvme: Nvme,
    /// The NVMe-oF sessions this plugin connected, beside the CSI socket.
    pub nvme_sessions: Option<SessionRegistry>,
    /// Shared by each running volume operation; see run_to_completion.
    pub operations: Arc<RwLock<()>>,
    /// Session GC's orphaned sessions and when it first saw them.
    pub orphans: crate::session_gc::Orphans,
    /// The kernel iSCSI initiator (iscsiadm and sysfs).
    pub iscsi: Iscsi,
    /// Session GC's orphaned iSCSI sessions, kept apart from NVMe-oF's.
    pub iscsi_orphans: crate::session_gc::Orphans,
}

impl State {
    /// A node on this host: host commands, the configured daemon socket,
    /// events to the log (main sends them to the API in a cluster).
    pub fn new(config: Config, driver_name: String, node_name: String, node_id: String, metrics: Arc<Metrics>) -> Self {
        let ublk = Arc::new(ublk_client::Client::new(Path::new(&config.nvmeof.ublk.socket_path)));
        let timeouts = Timeouts {
            mount: config.command_timeouts.mount(),
            format: config.command_timeouts.format(),
        };
        let nvme_timeout = config.command_timeouts.nvme();
        let mut iscsi = Iscsi::new(Arc::new(CLocaleRunner), config.command_timeouts.iscsi());
        iscsi.max_concurrent_logins = config.rate_limiting.max_concurrent_logins();
        iscsi.discovery_cache = config.rate_limiting.discovery_cache_duration();
        State {
            metrics,
            driver_name,
            version: env!("CARGO_PKG_VERSION").to_string(),
            node_id,
            node_name,
            config,
            ready: AtomicBool::new(false),
            mounter: Mounter::host(timeouts),
            locks: OperationLocks::default(),
            records: Records::default(),
            ublk,
            events: Arc::new(LogEvents),
            nvme: Nvme {
                runner: Arc::new(CLocaleRunner),
                timeout: nvme_timeout,
                sysfs: PathBuf::from("/sys"),
                dev: PathBuf::from("/dev"),
            },
            nvme_sessions: None,
            host: Host::default(),
            operations: Arc::default(),
            orphans: Default::default(),
            iscsi,
            iscsi_orphans: Default::default(),
        }
    }
}

/// The caller's deadline from `grpc-timeout` (1-8 digits and a unit).
pub fn rpc_deadline<T>(request: &Request<T>) -> Option<Instant> {
    let value = request.metadata().get("grpc-timeout")?.to_str().ok()?;
    let split = value.len().checked_sub(1)?;
    let (digits, unit) = value.split_at(split);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    let timeout = match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    };
    Some(Instant::now() + timeout)
}

static REQUEST_IDS: AtomicU64 = AtomicU64::new(0);

/// One RPC's log lines, as the Go node's interceptor writes them: the
/// request's identifiers when it arrives (at `info`, where the Go node's V(0)
/// and V(2) lines both show at the chart's -v=2), the body with its secrets
/// removed at `trace`, the completion at `debug`, a failure always.
struct RpcLog {
    id: u64,
    method: &'static str,
    started: Instant,
}

impl RpcLog {
    fn begin(method: &'static str, identifiers: String, body: &dyn std::fmt::Debug) -> Self {
        let id = REQUEST_IDS.fetch_add(1, Ordering::Relaxed) + 1;
        info!("[req-{id}] /csi.v1.Node/{method} {identifiers}");
        trace!("[req-{id}] request: {body:?}");
        RpcLog {
            id,
            method,
            started: Instant::now(),
        }
    }

    fn end<T>(&self, result: &Result<T, Status>) {
        let elapsed = self.started.elapsed();
        match result {
            Ok(_) => debug!(
                "[req-{}] /csi.v1.Node/{} completed in {elapsed:?}",
                self.id, self.method
            ),
            Err(e) => error!(
                "[req-{}] /csi.v1.Node/{} failed after {elapsed:?}: rpc error: code = {} desc = {}",
                self.id,
                self.method,
                crate::metrics::go_code_name(e.code()),
                e.message()
            ),
        }
    }
}

/// Runs a volume operation in its own task. tonic drops an RPC whose caller's
/// deadline passes; the operation itself still runs to the end (its commands
/// and daemon calls are bounded by that deadline) and releases its lock then,
/// as the Go node's handler does, so nothing is abandoned halfway. Each
/// operation holds a share of `operations` until it ends, which is how
/// shutdown waits for them.
async fn run_to_completion<T: Send + 'static>(
    operations: &Arc<RwLock<()>>,
    operation: impl Future<Output = Result<T, Status>> + Send + 'static,
) -> Result<T, Status> {
    let running = operations.clone().read_owned().await;
    tokio::spawn(async move {
        let result = operation.await;
        drop(running);
        result
    })
    .await
    .map_err(|e| Status::internal(format!("operation failed: {e}")))?
}

impl State {
    /// Waits for every volume operation still running, including those whose
    /// RPC was dropped at the caller's deadline (the Go node's GracefulStop
    /// waits for its handlers the same way).
    pub async fn wait_for_operations(&self) {
        let _all = self.operations.write().await;
    }
}

#[derive(Clone)]
pub struct IdentityService(pub Arc<State>);

#[derive(Clone)]
pub struct NodeService(pub Arc<State>);

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _: Request<csi::GetPluginInfoRequest>,
    ) -> Result<Response<csi::GetPluginInfoResponse>, Status> {
        Ok(Response::new(csi::GetPluginInfoResponse {
            name: self.0.driver_name.clone(),
            vendor_version: self.0.version.clone(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _: Request<csi::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<csi::GetPluginCapabilitiesResponse>, Status> {
        use csi::plugin_capability::{Service, Type, VolumeExpansion, service, volume_expansion};
        // The Go node advertises the controller service in node mode too; kubelet
        // ignores it, and keeping it keeps the two interchangeable.
        let capabilities = vec![
            csi::PluginCapability {
                r#type: Some(Type::Service(Service {
                    r#type: service::Type::ControllerService as i32,
                })),
            },
            csi::PluginCapability {
                r#type: Some(Type::VolumeExpansion(VolumeExpansion {
                    r#type: volume_expansion::Type::Online as i32,
                })),
            },
        ];
        Ok(Response::new(csi::GetPluginCapabilitiesResponse { capabilities }))
    }

    async fn probe(&self, _: Request<csi::ProbeRequest>) -> Result<Response<csi::ProbeResponse>, Status> {
        // Backend state is never a liveness signal on a node.
        Ok(Response::new(csi::ProbeResponse { ready: Some(true) }))
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn node_get_capabilities(
        &self,
        _: Request<csi::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<csi::NodeGetCapabilitiesResponse>, Status> {
        use csi::node_service_capability::{Rpc, Type, rpc};
        let rpc = |t: rpc::Type| csi::NodeServiceCapability {
            r#type: Some(Type::Rpc(Rpc { r#type: t as i32 })),
        };
        Ok(Response::new(csi::NodeGetCapabilitiesResponse {
            capabilities: vec![
                rpc(rpc::Type::StageUnstageVolume),
                rpc(rpc::Type::GetVolumeStats),
                rpc(rpc::Type::ExpandVolume),
                rpc(rpc::Type::SingleNodeMultiWriter),
            ],
        }))
    }

    async fn node_get_info(
        &self,
        _: Request<csi::NodeGetInfoRequest>,
    ) -> Result<Response<csi::NodeGetInfoResponse>, Status> {
        Ok(Response::new(csi::NodeGetInfoResponse {
            node_id: self.0.node_id.clone(),
            max_volumes_per_node: self.0.config.node_volume_limit(),
            accessible_topology: None,
        }))
    }

    async fn node_stage_volume(
        &self,
        request: Request<csi::NodeStageVolumeRequest>,
    ) -> Result<Response<csi::NodeStageVolumeResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let mut body = req.clone();
        body.secrets.clear();
        let log = RpcLog::begin(
            "NodeStageVolume",
            format!("volumeID={} stagingPath={}", req.volume_id, req.staging_target_path),
            &body,
        );
        let result = run_to_completion(&self.0.operations, async move {
            crate::stage::node_stage(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        result?;
        Ok(Response::new(csi::NodeStageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<csi::NodePublishVolumeRequest>,
    ) -> Result<Response<csi::NodePublishVolumeResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let mut body = req.clone();
        body.secrets.clear();
        let log = RpcLog::begin(
            "NodePublishVolume",
            format!("volumeID={} targetPath={}", req.volume_id, req.target_path),
            &body,
        );
        let result = run_to_completion(&self.0.operations, async move {
            crate::publish::node_publish(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        result?;
        Ok(Response::new(csi::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<csi::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<csi::NodeUnpublishVolumeResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let body = req.clone();
        let log = RpcLog::begin("NodeUnpublishVolume", format!("volumeID={}", req.volume_id), &body);
        let result = run_to_completion(&self.0.operations, async move {
            crate::publish::node_unpublish(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        result?;
        Ok(Response::new(csi::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<csi::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<csi::NodeGetVolumeStatsResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let body = req.clone();
        let log = RpcLog::begin("NodeGetVolumeStats", format!("volumeID={}", req.volume_id), &body);
        let result = run_to_completion(&self.0.operations, async move {
            crate::capacity::node_get_volume_stats(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        Ok(Response::new(result?))
    }

    async fn node_expand_volume(
        &self,
        request: Request<csi::NodeExpandVolumeRequest>,
    ) -> Result<Response<csi::NodeExpandVolumeResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let mut body = req.clone();
        body.secrets.clear();
        let log = RpcLog::begin("NodeExpandVolume", format!("volumeID={}", req.volume_id), &body);
        let result = run_to_completion(&self.0.operations, async move {
            crate::capacity::node_expand_volume(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        Ok(Response::new(result?))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<csi::NodeUnstageVolumeRequest>,
    ) -> Result<Response<csi::NodeUnstageVolumeResponse>, Status> {
        let deadline = rpc_deadline(&request);
        let (state, req) = (self.0.clone(), request.into_inner());
        let body = req.clone();
        let log = RpcLog::begin("NodeUnstageVolume", format!("volumeID={}", req.volume_id), &body);
        let result = run_to_completion(&self.0.operations, async move {
            crate::stage::node_unstage(&state, &req, deadline).await
        })
        .await;
        log.end(&result);
        result?;
        Ok(Response::new(csi::NodeUnstageVolumeResponse {}))
    }
}

impl State {
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn with_timeout(value: &str) -> Request<()> {
        let mut request = Request::new(());
        request.metadata_mut().insert("grpc-timeout", value.parse().unwrap());
        request
    }

    #[test]
    fn the_callers_deadline() {
        let now = Instant::now();
        let left = |value: &str| rpc_deadline(&with_timeout(value)).map(|d| d.saturating_duration_since(now));
        assert!(left("120S").is_some_and(|d| d > Duration::from_secs(119) && d <= Duration::from_secs(121)));
        assert!(left("2M").is_some_and(|d| d > Duration::from_secs(119)));
        assert!(left("1H").is_some_and(|d| d > Duration::from_secs(3599)));
        assert!(left("1500m").is_some_and(|d| d > Duration::from_millis(1400)));
        for bad in ["", "S", "12", "123456789S", "1x", "-1S", "1.5S"] {
            assert_eq!(left(bad), None, "{bad:?}");
        }
        assert_eq!(rpc_deadline(&Request::new(())), None);
    }

    /// An RPC dropped at its deadline does not abandon its operation.
    #[tokio::test]
    async fn an_operation_outlives_its_dropped_rpc() {
        let finished = Arc::new(AtomicUsize::new(0));
        let flag = finished.clone();
        let operations = Arc::new(RwLock::new(()));
        let rpc = run_to_completion(&operations, async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            flag.store(1, Ordering::SeqCst);
            Ok::<(), Status>(())
        });
        assert!(tokio::time::timeout(Duration::from_millis(10), rpc).await.is_err());
        assert_eq!(finished.load(Ordering::SeqCst), 0, "still running");
        // Shutdown waits for it.
        let _all = operations.write().await;
        assert_eq!(finished.load(Ordering::SeqCst), 1);
    }
}
