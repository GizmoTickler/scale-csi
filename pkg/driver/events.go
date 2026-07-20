package driver

import (
	"context"
	"os"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// Event reasons
	EventReasonVolumeCreated        = "VolumeCreated"
	EventReasonVolumeCreateFailed   = "VolumeCreateFailed"
	EventReasonVolumeDeleted        = "VolumeDeleted"
	EventReasonVolumeDeleteFailed   = "VolumeDeleteFailed"
	EventReasonVolumeExpanded       = "VolumeExpanded"
	EventReasonVolumeExpandFailed   = "VolumeExpandFailed"
	EventReasonSnapshotCreated      = "SnapshotCreated"
	EventReasonSnapshotCreateFailed = "SnapshotCreateFailed"
	EventReasonSnapshotDeleted      = "SnapshotDeleted"
	EventReasonSnapshotDeleteFailed = "SnapshotDeleteFailed"
	EventReasonISCSILoginFailed     = "ISCSILoginFailed"
	EventReasonNVMeConnectFailed    = "NVMeConnectFailed"
	EventReasonMountFailed          = "MountFailed"
	EventReasonISCSILogoutFailed    = "ISCSILogoutFailed"
	EventReasonNFSMountFailed       = "NFSMountFailed"
	EventReasonNFSUnmountFailed     = "NFSUnmountFailed"
	EventReasonTrueNASError         = "TrueNASError"
	EventReasonTrueNASReconnected   = "TrueNASReconnected"
)

const (
	pvcNameKey                 = "csi.storage.k8s.io/pvc/name"
	pvcNamespaceKey            = "csi.storage.k8s.io/pvc/namespace"
	pvNameKey                  = "csi.storage.k8s.io/pv/name"
	podNameKey                 = "csi.storage.k8s.io/pod.name"
	podNamespaceKey            = "csi.storage.k8s.io/pod.namespace"
	volumeSnapshotNameKey      = "csi.storage.k8s.io/volumesnapshot/name"
	volumeSnapshotNamespaceKey = "csi.storage.k8s.io/volumesnapshot/namespace"
)

// EventRecorder wraps Kubernetes event recording functionality
type EventRecorder struct {
	recorder      record.EventRecorder
	clientset     kubernetes.Interface
	dynamicClient dynamic.Interface
	enabled       bool
}

// NewEventRecorder creates a new event recorder
// Returns nil if running outside of Kubernetes or if events are disabled
func NewEventRecorder(driverName string) *EventRecorder {
	// Check if we're running in a Kubernetes cluster
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.V(4).Infof("Not running in Kubernetes cluster, events disabled: %v", err)
		return &EventRecorder{enabled: false}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Warningf("Failed to create Kubernetes clientset, events disabled: %v", err)
		return &EventRecorder{enabled: false}
	}
	dynamicClient, dynamicErr := dynamic.NewForConfig(config)
	if dynamicErr != nil {
		klog.Warningf("Failed to create Kubernetes dynamic client; orphan reconcile disabled: %v", dynamicErr)
	}

	// Create event broadcaster
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(4)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: clientset.CoreV1().Events(""),
	})

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
		Component: driverName,
		Host:      getHostname(),
	})

	klog.Info("Kubernetes event recorder initialized")

	return &EventRecorder{
		recorder:      recorder,
		clientset:     clientset,
		dynamicClient: dynamicClient,
		enabled:       true,
	}
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// IsEnabled returns true if the event recorder is enabled
func (e *EventRecorder) IsEnabled() bool {
	return e != nil && e.enabled
}

// Event records an event for an object
func (e *EventRecorder) Event(object runtime.Object, eventType, reason, message string) {
	if !e.IsEnabled() {
		return
	}
	e.recorder.Event(object, eventType, reason, message)
}

// Eventf records a formatted event for an object
func (e *EventRecorder) Eventf(object runtime.Object, eventType, reason, messageFmt string, args ...interface{}) {
	if !e.IsEnabled() {
		return
	}
	e.recorder.Eventf(object, eventType, reason, messageFmt, args...)
}

func (d *Driver) recordOperationFailureEvent(object runtime.Object, reason, operation string, operationErr error) {
	if operationErr == nil || object == nil || d.eventRecorder == nil {
		return
	}
	d.eventRecorder.Eventf(object, corev1.EventTypeWarning, reason, "%s failed: %v", operation, operationErr)
}

func (d *Driver) recordWarningEvent(object runtime.Object, reason, message string) {
	if object == nil || d.eventRecorder == nil {
		return
	}
	d.eventRecorder.Event(object, corev1.EventTypeWarning, reason, message)
}

func firstEventObject(objects []runtime.Object) runtime.Object {
	if len(objects) == 0 {
		return nil
	}
	return objects[0]
}

func createVolumeEventRef(req *csi.CreateVolumeRequest) runtime.Object {
	if req == nil {
		return nil
	}
	params := req.GetParameters()
	if namespace, name := params[pvcNamespaceKey], params[pvcNameKey]; namespace != "" && name != "" {
		return PVCRef(namespace, name)
	}
	if name := params[pvNameKey]; name != "" {
		return PVRef(name)
	}
	if name := req.GetName(); name != "" {
		return PVRef(name)
	}
	return nil
}

func createSnapshotEventRef(req *csi.CreateSnapshotRequest) runtime.Object {
	if req == nil {
		return nil
	}
	params := req.GetParameters()
	if namespace, name := params[volumeSnapshotNamespaceKey], params[volumeSnapshotNameKey]; namespace != "" && name != "" {
		return VolumeSnapshotRef(namespace, name)
	}
	if name := req.GetName(); name != "" {
		return VolumeSnapshotRef("", name)
	}
	return nil
}

func volumeEventRef(volumeID string) runtime.Object {
	if volumeID == "" {
		return nil
	}
	return PVRef(volumeID)
}

func nodeVolumeEventRef(volumeContext map[string]string, volumeID, nodeID string) runtime.Object {
	if namespace, name := volumeContext[podNamespaceKey], volumeContext[podNameKey]; namespace != "" && name != "" {
		return PodRef(namespace, name)
	}
	if namespace, name := volumeContext[pvcNamespaceKey], volumeContext[pvcNameKey]; namespace != "" && name != "" {
		return PVCRef(namespace, name)
	}
	if name := volumeContext[pvNameKey]; name != "" {
		return PVRef(name)
	}
	if volumeID != "" {
		return PVRef(volumeID)
	}
	if nodeID != "" {
		return NodeRef(nodeID)
	}
	return nil
}

// PVCRef creates an ObjectReference for a PVC
func PVCRef(namespace, name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		Namespace:  namespace,
		Name:       name,
		APIVersion: "v1",
	}
}

// PVRef creates an ObjectReference for a PV
func PVRef(name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:       "PersistentVolume",
		Name:       name,
		APIVersion: "v1",
	}
}

// PodRef creates an ObjectReference for a Pod.
func PodRef(namespace, name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:       "Pod",
		Namespace:  namespace,
		Name:       name,
		APIVersion: "v1",
	}
}

// VolumeSnapshotRef creates an ObjectReference for a VolumeSnapshot
func VolumeSnapshotRef(namespace, name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:       "VolumeSnapshot",
		Namespace:  namespace,
		Name:       name,
		APIVersion: "snapshot.storage.k8s.io/v1",
	}
}

// NodeRef creates an ObjectReference for a Node
func NodeRef(name string) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		Kind:       "Node",
		Name:       name,
		APIVersion: "v1",
	}
}

// NodeTopology holds topology information for a node
type NodeTopology struct {
	Zone   string
	Region string
}

// GetNodeTopology fetches topology labels from a Kubernetes node
// Returns empty topology if not running in-cluster or on error
func GetNodeTopology(nodeName string) *NodeTopology {
	if nodeName == "" {
		return &NodeTopology{}
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.V(4).Infof("Not running in Kubernetes cluster, topology auto-detection disabled: %v", err)
		return &NodeTopology{}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Warningf("Failed to create Kubernetes clientset for topology detection: %v", err)
		return &NodeTopology{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		klog.Warningf("Failed to get node %s for topology detection: %v", nodeName, err)
		return &NodeTopology{}
	}

	topology := &NodeTopology{
		Zone:   node.Labels["topology.kubernetes.io/zone"],
		Region: node.Labels["topology.kubernetes.io/region"],
	}

	if topology.Zone != "" || topology.Region != "" {
		klog.Infof("Auto-detected node topology: zone=%q, region=%q", topology.Zone, topology.Region)
	}

	return topology
}
