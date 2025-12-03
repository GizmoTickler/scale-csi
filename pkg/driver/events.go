package driver

import (
	"context"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	// Event reasons
	EventReasonVolumeCreated       = "VolumeCreated"
	EventReasonVolumeCreateFailed  = "VolumeCreateFailed"
	EventReasonVolumeDeleted       = "VolumeDeleted"
	EventReasonVolumeDeleteFailed  = "VolumeDeleteFailed"
	EventReasonVolumeExpanded      = "VolumeExpanded"
	EventReasonVolumeExpandFailed  = "VolumeExpandFailed"
	EventReasonSnapshotCreated     = "SnapshotCreated"
	EventReasonSnapshotCreateFailed = "SnapshotCreateFailed"
	EventReasonSnapshotDeleted     = "SnapshotDeleted"
	EventReasonSnapshotDeleteFailed = "SnapshotDeleteFailed"
	EventReasonISCSILoginFailed    = "ISCSILoginFailed"
	EventReasonISCSILogoutFailed   = "ISCSILogoutFailed"
	EventReasonNFSMountFailed      = "NFSMountFailed"
	EventReasonNFSUnmountFailed    = "NFSUnmountFailed"
	EventReasonTrueNASError        = "TrueNASError"
	EventReasonTrueNASReconnected  = "TrueNASReconnected"
)

// EventRecorder wraps Kubernetes event recording functionality
type EventRecorder struct {
	recorder  record.EventRecorder
	clientset kubernetes.Interface
	enabled   bool
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
		recorder:  recorder,
		clientset: clientset,
		enabled:   true,
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
