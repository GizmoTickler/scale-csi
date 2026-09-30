//! The CSI Identity and Node services. Volume RPCs arrive with their protocol
//! slices; until then they answer Unimplemented, and the agent refuses to start
//! on an install that enables a protocol it does not serve yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tonic::{Request, Response, Status};

use crate::config::Config;
use crate::csi::{self, identity_server::Identity, node_server::Node};

pub struct State {
    pub metrics: Arc<crate::metrics::Metrics>,
    pub driver_name: String,
    pub version: String,
    pub node_id: String,
    pub config: Config,
    pub ready: AtomicBool,
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
}

impl State {
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}
