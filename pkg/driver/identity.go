package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/klog/v2"
)

// GetPluginInfo returns metadata about the driver.
func (d *Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	klog.V(4).Info("GetPluginInfo called")

	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: d.version,
	}, nil
}

// GetPluginCapabilities returns the capabilities of the driver.
func (d *Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	klog.V(4).Info("GetPluginCapabilities called")

	caps := []*csi.PluginCapability{
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		},
		{
			Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
					Type: csi.PluginCapability_VolumeExpansion_ONLINE,
				},
			},
		},
	}

	// Only advertise topology constraints when topology is explicitly enabled
	// Without this check, the provisioner requires topology but nodes don't provide it
	if d.config != nil && d.config.Node.Topology.Enabled {
		caps = append(caps, &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		})
		klog.V(4).Info("Topology enabled: advertising VOLUME_ACCESSIBILITY_CONSTRAINTS capability")
	}

	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

// Probe checks if the driver is healthy and ready.
func (d *Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	klog.V(4).Info("Probe called")

	// Check if TrueNAS connection is healthy
	if !d.ready.Load() {
		return nil, status.Error(codes.FailedPrecondition, "driver not ready")
	}

	// CSI Probe is used by the liveness sidecar and therefore reports process
	// health only. Backend connectivity remains visible through /readyz and the
	// scale_csi_truenas_connection_status metric. The client reconnects on the
	// next API call; it has no separate non-blocking reconnect entrypoint.
	d.observeTrueNASConnection()

	return &csi.ProbeResponse{
		Ready: &wrapperspb.BoolValue{
			Value: d.ready.Load(),
		},
	}, nil
}

func (d *Driver) observeTrueNASConnection() bool {
	if d.truenasClient == nil {
		SetTrueNASActiveConnections(0)
		return false
	}

	connected := d.truenasClient.IsConnected()
	SetTrueNASConnectionStatus(connected)
	activeConnections := 0
	if counter, ok := d.truenasClient.(interface{ ActiveConnectionCount() int }); ok {
		activeConnections = counter.ActiveConnectionCount()
	} else if connected {
		// Third-party test clients may not expose their pool. Preserve a useful
		// connected value while the production client reports the exact count.
		activeConnections = 1
	}
	SetTrueNASActiveConnections(activeConnections)

	const (
		connectionConnected    int32 = 1
		connectionDisconnected int32 = 2
	)
	state := connectionDisconnected
	if connected {
		state = connectionConnected
	}
	previous := d.truenasConnectionState.Swap(state)
	if state == connectionDisconnected && previous != connectionDisconnected {
		klog.Warning("TrueNAS client disconnected; it will reconnect on the next API call")
	} else if state == connectionConnected && previous == connectionDisconnected {
		klog.Info("TrueNAS client reconnected")
	}

	return connected
}
