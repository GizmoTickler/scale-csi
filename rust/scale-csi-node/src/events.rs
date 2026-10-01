//! Kubernetes events the node emits, with the Go node's reasons and the object
//! it attaches them to (`pkg/driver/events.go`). The sink is a seam: in a
//! cluster the agent writes them to the API (`kube_events`), outside one it
//! logs them, the tests record them.

use std::collections::HashMap;
use std::sync::Mutex;

use log::warn;

pub const REASON_NVME_CONNECT_FAILED: &str = "NVMeConnectFailed";
pub const REASON_NVME_PATH_DEGRADED: &str = "NVMePathDegraded";

const POD_NAME: &str = "csi.storage.k8s.io/pod.name";
const POD_NAMESPACE: &str = "csi.storage.k8s.io/pod.namespace";
const PVC_NAME: &str = "csi.storage.k8s.io/pvc/name";
const PVC_NAMESPACE: &str = "csi.storage.k8s.io/pvc/namespace";
const PV_NAME: &str = "csi.storage.k8s.io/pv/name";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectRef {
    Pod { namespace: String, name: String },
    Pvc { namespace: String, name: String },
    Pv { name: String },
    Node { name: String },
}

impl ObjectRef {
    /// The involved object's kind, as Go's PodRef/PVCRef/PVRef/NodeRef set it.
    pub fn kind(&self) -> &'static str {
        match self {
            ObjectRef::Pod { .. } => "Pod",
            ObjectRef::Pvc { .. } => "PersistentVolumeClaim",
            ObjectRef::Pv { .. } => "PersistentVolume",
            ObjectRef::Node { .. } => "Node",
        }
    }

    /// Every object the node refers to is in the core group.
    pub fn api_version(&self) -> &'static str {
        "v1"
    }

    /// The object's namespace; empty for cluster-scoped objects.
    pub fn namespace(&self) -> &str {
        match self {
            ObjectRef::Pod { namespace, .. } | ObjectRef::Pvc { namespace, .. } => namespace,
            ObjectRef::Pv { .. } | ObjectRef::Node { .. } => "",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            ObjectRef::Pod { name, .. }
            | ObjectRef::Pvc { name, .. }
            | ObjectRef::Pv { name }
            | ObjectRef::Node { name } => name,
        }
    }
}

impl std::fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectRef::Pod { namespace, name } => write!(f, "Pod {namespace}/{name}"),
            ObjectRef::Pvc { namespace, name } => write!(f, "PersistentVolumeClaim {namespace}/{name}"),
            ObjectRef::Pv { name } => write!(f, "PersistentVolume {name}"),
            ObjectRef::Node { name } => write!(f, "Node {name}"),
        }
    }
}

/// The object a node volume event is about (Go nodeVolumeEventRef): the pod,
/// else the PVC, else the PV (by name, else by volume ID), else the node.
pub fn node_volume_ref(context: &HashMap<String, String>, volume_id: &str, node_name: &str) -> Option<ObjectRef> {
    let get = |k: &str| context.get(k).map(String::as_str).unwrap_or_default();
    if !get(POD_NAMESPACE).is_empty() && !get(POD_NAME).is_empty() {
        return Some(ObjectRef::Pod {
            namespace: get(POD_NAMESPACE).into(),
            name: get(POD_NAME).into(),
        });
    }
    if !get(PVC_NAMESPACE).is_empty() && !get(PVC_NAME).is_empty() {
        return Some(ObjectRef::Pvc {
            namespace: get(PVC_NAMESPACE).into(),
            name: get(PVC_NAME).into(),
        });
    }
    if !get(PV_NAME).is_empty() {
        return Some(ObjectRef::Pv {
            name: get(PV_NAME).into(),
        });
    }
    if !volume_id.is_empty() {
        return Some(ObjectRef::Pv { name: volume_id.into() });
    }
    if !node_name.is_empty() {
        return Some(ObjectRef::Node { name: node_name.into() });
    }
    None
}

pub trait Events: Send + Sync {
    fn warning(&self, object: &ObjectRef, reason: &str, message: &str);
}

/// Writes events to the log only.
pub struct LogEvents;

impl Events for LogEvents {
    fn warning(&self, object: &ObjectRef, reason: &str, message: &str) {
        warn!("event Warning {reason} on {object}: {message}");
    }
}

/// Keeps every event, for tests.
#[derive(Default)]
pub struct Recorded(pub Mutex<Vec<(ObjectRef, String, String)>>);

impl Events for Recorded {
    fn warning(&self, object: &ObjectRef, reason: &str, message: &str) {
        self.0
            .lock()
            .expect("events")
            .push((object.clone(), reason.into(), message.into()));
    }
}

impl Recorded {
    pub fn take(&self) -> Vec<(ObjectRef, String, String)> {
        std::mem::take(&mut *self.0.lock().expect("events"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_object_an_event_is_about() {
        let ctx = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
        };
        let full = ctx(&[
            (POD_NAMESPACE, "apps"),
            (POD_NAME, "web-0"),
            (PVC_NAMESPACE, "apps"),
            (PVC_NAME, "data"),
            (PV_NAME, "pvc-1"),
        ]);
        assert_eq!(
            node_volume_ref(&full, "vol", "node"),
            Some(ObjectRef::Pod {
                namespace: "apps".into(),
                name: "web-0".into()
            })
        );
        let pvc = ctx(&[(POD_NAME, "web-0"), (PVC_NAMESPACE, "apps"), (PVC_NAME, "data")]);
        assert_eq!(
            node_volume_ref(&pvc, "vol", "node"),
            Some(ObjectRef::Pvc {
                namespace: "apps".into(),
                name: "data".into()
            }),
            "a pod name without its namespace is not a pod"
        );
        assert_eq!(
            node_volume_ref(&ctx(&[(PV_NAME, "pvc-1")]), "vol", "node"),
            Some(ObjectRef::Pv { name: "pvc-1".into() })
        );
        assert_eq!(
            node_volume_ref(&ctx(&[]), "vol", "node"),
            Some(ObjectRef::Pv { name: "vol".into() })
        );
        assert_eq!(
            node_volume_ref(&ctx(&[]), "", "node"),
            Some(ObjectRef::Node { name: "node".into() })
        );
        assert_eq!(node_volume_ref(&ctx(&[]), "", ""), None);
    }
}
