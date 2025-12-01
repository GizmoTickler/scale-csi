package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// DriverConfig holds the driver initialization configuration.
type DriverConfig struct {
	Name          string
	Version       string
	NodeID        string
	Endpoint      string
	RunController bool
	RunNode       bool
	Config        *Config
	HealthPort    int // Port for health/metrics HTTP server (0 to disable)
}

// Driver is the TrueNAS Scale CSI driver.
type Driver struct {
	// Embed unimplemented servers for forward compatibility with CSI spec
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	name          string
	version       string
	nodeID        string
	endpoint      string
	runController bool
	runNode       bool
	config        *Config

	// TrueNAS API client
	truenasClient truenas.ClientInterface

	// gRPC server
	server *grpc.Server

	// Health and metrics server
	healthServer *HealthServer
	healthPort   int

	// Kubernetes event recorder
	eventRecorder *EventRecorder

	// Operation lock to prevent concurrent operations on same volume
	operationLock sync.Map

	// Ready flag
	ready bool

	// Request counter for generating unique request IDs
	requestCounter uint64
}

// NewDriver creates a new TrueNAS CSI driver instance.
func NewDriver(cfg *DriverConfig) (*Driver, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("driver name is required")
	}
	if cfg.Version == "" {
		cfg.Version = "unknown"
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("config is required")
	}

	// Create TrueNAS API client
	truenasClient, err := truenas.NewClient(&truenas.ClientConfig{
		Host:              cfg.Config.TrueNAS.Host,
		Port:              cfg.Config.TrueNAS.Port,
		Protocol:          cfg.Config.TrueNAS.Protocol,
		APIKey:            cfg.Config.TrueNAS.APIKey,
		AllowInsecure:     cfg.Config.TrueNAS.AllowInsecure,
		Timeout:           time.Duration(cfg.Config.TrueNAS.RequestTimeout) * time.Second,
		ConnectTimeout:    time.Duration(cfg.Config.TrueNAS.ConnectTimeout) * time.Second,
		MaxConcurrentReqs: cfg.Config.TrueNAS.MaxConcurrentRequests,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create TrueNAS client: %w", err)
	}

	// Initialize event recorder (will be nil if not running in k8s)
	eventRecorder := NewEventRecorder(cfg.Name)

	return &Driver{
		name:          cfg.Name,
		version:       cfg.Version,
		nodeID:        cfg.NodeID,
		endpoint:      cfg.Endpoint,
		runController: cfg.RunController,
		runNode:       cfg.RunNode,
		config:        cfg.Config,
		truenasClient: truenasClient,
		healthPort:    cfg.HealthPort,
		eventRecorder: eventRecorder,
	}, nil
}

// Run starts the CSI driver.
func (d *Driver) Run() error {
	// Parse endpoint
	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %w", err)
	}

	var addr string
	if u.Scheme == "unix" {
		addr = u.Path
		// Remove existing socket file
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove socket file: %w", err)
		}
	} else {
		addr = u.Host
	}

	// Create listener
	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	// Set socket permissions for unix sockets
	if u.Scheme == "unix" {
		if err := os.Chmod(addr, 0660); err != nil {
			return fmt.Errorf("failed to set socket permissions: %w", err)
		}
	}

	// Create gRPC server with interceptor for logging
	d.server = grpc.NewServer(
		grpc.UnaryInterceptor(d.logInterceptor),
	)

	// Register CSI services
	csi.RegisterIdentityServer(d.server, d)

	if d.runController {
		csi.RegisterControllerServer(d.server, d)
		klog.Info("Controller service registered")
	}

	if d.runNode {
		csi.RegisterNodeServer(d.server, d)
		klog.Info("Node service registered")
	}

	// Start health and metrics server if port is configured
	if d.healthPort > 0 {
		d.healthServer = NewHealthServer(d, d.healthPort)
		if err := d.healthServer.Start(); err != nil {
			return fmt.Errorf("failed to start health server: %w", err)
		}
	}

	d.ready = true
	klog.Infof("CSI driver listening on %s", d.endpoint)

	return d.server.Serve(listener)
}

// Stop gracefully stops the driver.
func (d *Driver) Stop() {
	klog.Info("Stopping CSI driver")
	d.ready = false

	// Stop health server first
	if d.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := d.healthServer.Stop(ctx); err != nil {
			klog.Warningf("Failed to stop health server: %v", err)
		}
	}

	if d.server != nil {
		d.server.GracefulStop()
	}
	if d.truenasClient != nil {
		if err := d.truenasClient.Close(); err != nil {
			klog.Warningf("Failed to close TrueNAS client: %v", err)
		}
	}
}

// logInterceptor is a gRPC interceptor for logging requests with request IDs and timing.
func (d *Driver) logInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// Generate a unique request ID for tracing
	requestID := atomic.AddUint64(&d.requestCounter, 1)
	startTime := time.Now()

	// Extract key identifiers from common request types for better logging
	switch r := req.(type) {
	case *csi.CreateVolumeRequest:
		klog.Infof("[req-%d] %s name=%s", requestID, info.FullMethod, r.GetName())
	case *csi.DeleteVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s", requestID, info.FullMethod, r.GetVolumeId())
	case *csi.NodeStageVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s stagingPath=%s", requestID, info.FullMethod, r.GetVolumeId(), r.GetStagingTargetPath())
	case *csi.NodeUnstageVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s", requestID, info.FullMethod, r.GetVolumeId())
	case *csi.NodePublishVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s targetPath=%s", requestID, info.FullMethod, r.GetVolumeId(), r.GetTargetPath())
	case *csi.NodeUnpublishVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s", requestID, info.FullMethod, r.GetVolumeId())
	case *csi.CreateSnapshotRequest:
		klog.Infof("[req-%d] %s name=%s sourceVolumeID=%s", requestID, info.FullMethod, r.GetName(), r.GetSourceVolumeId())
	case *csi.DeleteSnapshotRequest:
		klog.Infof("[req-%d] %s snapshotID=%s", requestID, info.FullMethod, r.GetSnapshotId())
	case *csi.ControllerExpandVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s", requestID, info.FullMethod, r.GetVolumeId())
	case *csi.NodeExpandVolumeRequest:
		klog.Infof("[req-%d] %s volumeID=%s", requestID, info.FullMethod, r.GetVolumeId())
	default:
		klog.V(4).Infof("[req-%d] %s", requestID, info.FullMethod)
	}

	// Log full request at higher verbosity
	klog.V(5).Infof("[req-%d] request: %+v", requestID, req)

	// Handle the request
	resp, err := handler(ctx, req)

	// Calculate duration
	duration := time.Since(startTime)

	// Record metrics
	RecordCSIOperation(info.FullMethod, duration.Seconds(), err)

	// Log result
	if err != nil {
		klog.Errorf("[req-%d] %s failed after %v: %v", requestID, info.FullMethod, duration, err)
	} else {
		klog.V(4).Infof("[req-%d] %s completed in %v", requestID, info.FullMethod, duration)
		klog.V(5).Infof("[req-%d] response: %+v", requestID, resp)
	}

	return resp, err
}

// acquireOperationLock acquires a lock for the given operation key.
// Returns false if the lock is already held.
func (d *Driver) acquireOperationLock(key string) bool {
	_, loaded := d.operationLock.LoadOrStore(key, struct{}{})
	return !loaded
}

// releaseOperationLock releases the lock for the given operation key.
func (d *Driver) releaseOperationLock(key string) {
	d.operationLock.Delete(key)
}

// GetTrueNASClient returns the TrueNAS API client.
func (d *Driver) GetTrueNASClient() truenas.ClientInterface {
	return d.truenasClient
}

// GetConfig returns the driver configuration.
func (d *Driver) GetConfig() *Config {
	return d.config
}
