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
	"github.com/GizmoTickler/scale-csi/pkg/util"
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

	// Session GC context and cancellation
	gcCancel context.CancelFunc
	gcWg     sync.WaitGroup
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

		// Start session garbage collection for node plugin
		d.startSessionGC()
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

	// Stop session GC goroutine first
	d.stopSessionGC()

	// Stop health server
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

// startSessionGC starts the periodic session garbage collection goroutine.
// This runs only on the node plugin and cleans up orphaned iSCSI/NVMe-oF sessions.
func (d *Driver) startSessionGC() {
	// Check if GC is enabled (default to true if Interval > 0)
	if d.config.SessionGC.Interval <= 0 {
		klog.Info("Session GC disabled (interval <= 0)")
		return
	}

	// Check if explicitly disabled
	// Note: We consider GC enabled by default unless the config explicitly has
	// Enabled: false with a non-zero interval (unusual config)
	interval := time.Duration(d.config.SessionGC.Interval) * time.Second
	gracePeriod := time.Duration(d.config.SessionGC.GracePeriod) * time.Second
	dryRun := d.config.SessionGC.DryRun

	ctx, cancel := context.WithCancel(context.Background())
	d.gcCancel = cancel

	d.gcWg.Add(1)
	go func() {
		defer d.gcWg.Done()
		klog.Infof("Session GC started: interval=%v, gracePeriod=%v, dryRun=%v", interval, gracePeriod, dryRun)

		// Initial delay to let the system stabilize after startup
		select {
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
			klog.Info("Session GC stopped during startup delay")
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.runSessionGC(ctx, gracePeriod, dryRun)
			case <-ctx.Done():
				klog.Info("Session GC stopped")
				return
			}
		}
	}()
}

// stopSessionGC stops the session garbage collection goroutine.
func (d *Driver) stopSessionGC() {
	if d.gcCancel != nil {
		d.gcCancel()
		d.gcWg.Wait()
	}
}

// runSessionGC performs one garbage collection cycle.
// It runs GC for all configured protocols (iSCSI and/or NVMe-oF), not just
// the default driver type. This handles multi-protocol deployments where
// the driver name is generic but multiple StorageClasses use different protocols.
func (d *Driver) runSessionGC(_ context.Context, _ time.Duration, dryRun bool) {
	klog.V(4).Info("Running session GC")

	// Run iSCSI GC if iSCSI is configured (has target portal)
	if d.config.ISCSI.TargetPortal != "" {
		d.gcISCSISessions(dryRun)
	}

	// Run NVMe-oF GC if NVMe-oF is configured (has transport address)
	if d.config.NVMeoF.TransportAddress != "" {
		d.gcNVMeoFSessions(dryRun)
	}
}

// gcISCSISessions garbage collects orphaned iSCSI sessions.
func (d *Driver) gcISCSISessions(dryRun bool) {
	// Get all active iSCSI sessions
	sessions, err := util.ListISCSISessions()
	if err != nil {
		klog.Warningf("Session GC: failed to list iSCSI sessions: %v", err)
		return
	}

	if len(sessions) == 0 {
		klog.V(5).Info("Session GC: no active iSCSI sessions")
		return
	}

	klog.V(4).Infof("Session GC: found %d active iSCSI sessions", len(sessions))

	// Get expected sessions from kubelet staging directory
	expectedTargets := d.getExpectedISCSITargets()
	klog.V(4).Infof("Session GC: found %d expected iSCSI targets from staged volumes", len(expectedTargets))

	// Find orphaned sessions
	portal := d.config.ISCSI.TargetPortal
	orphanedCount := 0

	for _, session := range sessions {
		// Only consider sessions for our portal
		if session.Portal != portal && session.Portal != portal+",1" {
			klog.V(5).Infof("Session GC: skipping session %s (different portal: %s)", session.IQN, session.Portal)
			continue
		}

		// Check if this session is expected
		if _, expected := expectedTargets[session.IQN]; expected {
			klog.V(5).Infof("Session GC: session %s is expected (has staged volume)", session.IQN)
			continue
		}

		// This session has no corresponding staged volume - it's orphaned
		orphanedCount++
		klog.Infof("Session GC: found orphaned iSCSI session: %s", session.IQN)

		if dryRun {
			klog.Infof("Session GC: [DRY RUN] would disconnect orphaned session: %s", session.IQN)
			continue
		}

		// Disconnect the orphaned session
		if err := util.ISCSIDisconnect(portal, session.IQN); err != nil {
			klog.Warningf("Session GC: failed to disconnect orphaned session %s: %v", session.IQN, err)
		} else {
			klog.Infof("Session GC: disconnected orphaned iSCSI session: %s", session.IQN)
		}
	}

	if orphanedCount > 0 {
		klog.Infof("Session GC: processed %d orphaned iSCSI sessions", orphanedCount)
	} else {
		klog.V(4).Info("Session GC: no orphaned iSCSI sessions found")
	}
}

// gcNVMeoFSessions garbage collects orphaned NVMe-oF sessions.
func (d *Driver) gcNVMeoFSessions(dryRun bool) {
	// Get all active NVMe-oF sessions
	sessions, err := util.ListNVMeoFSessions()
	if err != nil {
		klog.Warningf("Session GC: failed to list NVMe-oF sessions: %v", err)
		return
	}

	if len(sessions) == 0 {
		klog.V(5).Info("Session GC: no active NVMe-oF sessions")
		return
	}

	klog.V(4).Infof("Session GC: found %d active NVMe-oF sessions", len(sessions))

	// Get expected sessions from kubelet staging directory
	expectedNQNs := d.getExpectedNVMeoFNQNs()
	klog.V(4).Infof("Session GC: found %d expected NVMe-oF NQNs from staged volumes", len(expectedNQNs))

	// Find orphaned sessions
	orphanedCount := 0
	targetAddr := d.config.NVMeoF.TransportAddress

	for _, session := range sessions {
		// Only consider sessions for our target address
		if session.Address != targetAddr {
			klog.V(5).Infof("Session GC: skipping session %s (different address: %s)", session.NQN, session.Address)
			continue
		}

		// Check if this session is expected
		if _, expected := expectedNQNs[session.NQN]; expected {
			klog.V(5).Infof("Session GC: session %s is expected (has staged volume)", session.NQN)
			continue
		}

		// This session has no corresponding staged volume - it's orphaned
		orphanedCount++
		klog.Infof("Session GC: found orphaned NVMe-oF session: %s", session.NQN)

		if dryRun {
			klog.Infof("Session GC: [DRY RUN] would disconnect orphaned session: %s", session.NQN)
			continue
		}

		// Disconnect the orphaned session
		if err := util.NVMeoFDisconnect(session.NQN); err != nil {
			klog.Warningf("Session GC: failed to disconnect orphaned session %s: %v", session.NQN, err)
		} else {
			klog.Infof("Session GC: disconnected orphaned NVMe-oF session: %s", session.NQN)
		}
	}

	if orphanedCount > 0 {
		klog.Infof("Session GC: processed %d orphaned NVMe-oF sessions", orphanedCount)
	} else {
		klog.V(4).Info("Session GC: no orphaned NVMe-oF sessions found")
	}
}

// getExpectedISCSITargets returns a map of IQNs that have corresponding staged volumes.
// It scans the kubelet CSI staging directory to find which volumes are currently staged.
func (d *Driver) getExpectedISCSITargets() map[string]struct{} {
	expected := make(map[string]struct{})

	// Kubelet stores staging mounts in /var/lib/kubelet/plugins/kubernetes.io/csi/
	// Each volume has a directory: /var/lib/kubelet/plugins/kubernetes.io/csi/<driver>/globalmount
	// We need to check what's actually mounted

	// Alternative approach: scan for iSCSI devices that are in use (mounted)
	// This is more reliable than trying to parse kubelet directories
	mountedDevices, err := util.GetMountedBlockDevices()
	if err != nil {
		klog.Warningf("Session GC: failed to get mounted block devices: %v", err)
		return expected
	}

	// For each mounted device, get its iSCSI session if any
	for device := range mountedDevices {
		// Check if this device is an iSCSI device
		portal, iqn, err := util.GetISCSIInfoFromDevice(device)
		if err != nil {
			continue // Not an iSCSI device
		}
		if portal != "" && iqn != "" {
			expected[iqn] = struct{}{}
		}
	}

	return expected
}

// getExpectedNVMeoFNQNs returns a map of NQNs that have corresponding staged volumes.
func (d *Driver) getExpectedNVMeoFNQNs() map[string]struct{} {
	expected := make(map[string]struct{})

	// Get mounted block devices
	mountedDevices, err := util.GetMountedBlockDevices()
	if err != nil {
		klog.Warningf("Session GC: failed to get mounted block devices: %v", err)
		return expected
	}

	// For each mounted device, get its NVMe-oF session if any
	for device := range mountedDevices {
		// Check if this device is an NVMe device
		nqn, err := util.GetNVMeInfoFromDevice(device)
		if err != nil {
			continue // Not an NVMe device
		}
		if nqn != "" {
			expected[nqn] = struct{}{}
		}
	}

	return expected
}
