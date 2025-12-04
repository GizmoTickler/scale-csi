package driver

import (
	"context"
	"time"

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

	if !d.truenasClient.IsConnected() {
		// Actually trigger reconnection by making a lightweight API call
		klog.Warning("TrueNAS client disconnected, attempting reconnection")
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := d.truenasClient.Call(pingCtx, "core.ping")
		cancel()
		if err != nil {
			klog.Warningf("TrueNAS reconnection failed: %v", err)
			return nil, status.Error(codes.Unavailable, "TrueNAS connection lost")
		}
		klog.Info("TrueNAS reconnection successful")
	}

	return &csi.ProbeResponse{
		Ready: &wrapperspb.BoolValue{
			Value: d.ready.Load(),
		},
	}, nil
}
