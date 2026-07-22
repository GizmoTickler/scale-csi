package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

const kubeletCSIStagingRoot = "/var/lib/kubelet/plugins/kubernetes.io/csi"

var (
	getMountedBlockDevices = util.GetMountedBlockDevices
	getStagedBlockDevices  = func() (map[string]string, error) {
		return util.GetStagedBlockDevices(kubeletCSIStagingRoot)
	}
	getISCSIInfoFromDevice             = util.GetISCSIInfoFromDevice
	getNVMeInfoFromDevice              = util.GetNVMeInfoFromDevice
	getISCSIInfoFromDeviceWithSessions = util.GetISCSIInfoFromDeviceWithSessions
	iscsiConnectWithSessions           = util.ISCSIConnectWithOptionsAndSessions
	nvmeConnectWithSubsystems          = util.NVMeoFConnectWithOptionsAndSubsystems
	resolveNodeStatsDevice             = nodeStatsDevice
	getNodeDeviceSize                  = nodeStatsDeviceSize
	getNodeFilesystemStats             = util.GetFilesystemStats
	gcListISCSISessions                = util.ListISCSISessions
	gcDisconnectISCSI                  = util.ISCSIDisconnect
	gcListNVMeoFSessions               = util.ListNVMeoFSessions
	gcDisconnectNVMeoF                 = util.NVMeoFDisconnect
)

const expectedDeleteSnapshotLogInterval = time.Minute

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
	encodedNodeID string
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

	// Node mount state supplements the live mount table with the CSI identity
	// and capability that cannot be reconstructed from kernel mount metadata.
	nodeMountStateMu sync.Mutex
	stagedTargets    map[string]nodeMountRecord
	publishedTargets map[string]nodeMountRecord

	// Ready flag (atomic for safe concurrent access)
	ready atomic.Bool

	// Last observed aggregate TrueNAS connection state. Zero is unknown, one is
	// connected, and two is disconnected.
	truenasConnectionState atomic.Int32

	// Request counter for generating unique request IDs
	requestCounter uint64

	// Session GC context and cancellation
	gcCancel context.CancelFunc
	gcWg     sync.WaitGroup

	// Controller-side orphan reconcile context and cancellation.
	reconcileCancel        context.CancelFunc
	reconcileWg            sync.WaitGroup
	startupReconcileCancel context.CancelFunc
	startupReconcileWg     sync.WaitGroup
	startupReconcileOnce   sync.Once
	startupReconcileSignal chan struct{}

	// Background fencing state. Missing-record observations are in-memory on
	// purpose: a controller restart restarts the full grace period rather than
	// revoking an old record immediately after a fresh VA disappearance.
	stalePublicationRecordsSeen sync.Map
	fencingDeferredLogs         sync.Map

	// Track when orphaned sessions were first seen (for grace period). The
	// protocol maps must remain independent: a cleanup pass may only retire
	// stale observations from its own enumeration.
	orphanedISCSISessionsSeen sync.Map // Key: IQN, Value: first seen time
	orphanedNVMeSessionsSeen  sync.Map // Key: NQN, Value: first seen time

	// Service reload debouncer (prevents reload storms during bulk provisioning)
	serviceReloadDebouncer *ServiceReloadDebouncer

	// Cached auto-resolved iSCSI target group (portal/initiator) used when
	// iscsi.targetGroups is not configured; see resolveISCSITargetGroup.
	iscsiGroupMu       sync.Mutex
	iscsiResolvedGroup *truenas.ISCSITargetGroup

	// Cached TrueNAS NVMe-oF host IDs keyed by initiator NQN. Resolution is
	// serialized so concurrent volume creates do not race to create one host.
	nvmeHostMu        sync.Mutex
	nvmeResolvedHosts map[string]int

	expectedDeleteLogMu   sync.Mutex
	expectedDeleteLogLast map[string]time.Time
}

// newTrueNASClient constructs the TrueNAS API client; tests override it to
// inject the stateful mock before any connection is attempted.
var newTrueNASClient = func(cfg *truenas.ClientConfig) (truenas.ClientInterface, error) {
	return truenas.NewClient(cfg)
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

	encodedNodeID := ""
	if cfg.RunNode {
		identity := discoverNodeIdentity(context.Background(), cfg.NodeID)
		identity = nodeIdentityForEnabledProtocols(identity, cfg.Config)
		var identityErr error
		encodedNodeID, identityErr = encodeNodeIdentity(identity)
		if identityErr != nil {
			return nil, fmt.Errorf("node startup identity cannot be encoded within CSI's %d-byte node_id limit: %w",
				maxCSINodeIDBytes, identityErr)
		}
	}

	// Build circuit breaker config if enabled
	var cbConfig *truenas.CircuitBreakerConfig
	if cfg.Config.Resilience.CircuitBreaker.Enabled {
		cbConfig = &truenas.CircuitBreakerConfig{
			Enabled:             true,
			FailureThreshold:    cfg.Config.Resilience.CircuitBreaker.FailureThreshold,
			SuccessThreshold:    cfg.Config.Resilience.CircuitBreaker.SuccessThreshold,
			Timeout:             time.Duration(cfg.Config.Resilience.CircuitBreaker.Timeout) * time.Second,
			HalfOpenMaxRequests: cfg.Config.Resilience.CircuitBreaker.HalfOpenMaxRequests,
		}
	}

	// Node RPCs use only local mount and transport tools. Construct the
	// management client exclusively for controller mode so node pods neither
	// need nor retain TrueNAS API credentials.
	var truenasClient truenas.ClientInterface
	if cfg.RunController {
		if strings.TrimSpace(cfg.Config.TrueNAS.APIKey) == "" {
			return nil, fmt.Errorf("truenas.apiKey is required in controller mode")
		}
		var err error
		truenasClient, err = newTrueNASClient(&truenas.ClientConfig{
			Host:                  cfg.Config.TrueNAS.Host,
			Port:                  cfg.Config.TrueNAS.Port,
			Protocol:              cfg.Config.TrueNAS.Protocol,
			APIKey:                cfg.Config.TrueNAS.APIKey,
			AllowInsecure:         cfg.Config.TrueNAS.AllowInsecure,
			CACert:                cfg.Config.TrueNAS.CACert,
			CACertFile:            cfg.Config.TrueNAS.CACertFile,
			Timeout:               time.Duration(cfg.Config.TrueNAS.RequestTimeout) * time.Second,
			ConnectTimeout:        time.Duration(cfg.Config.TrueNAS.ConnectTimeout) * time.Second,
			WriteTimeout:          time.Duration(cfg.Config.TrueNAS.WriteTimeout) * time.Second,
			MaxConcurrentReqs:     cfg.Config.TrueNAS.MaxConcurrentRequests,
			MetricsRecorder:       RecordTrueNASRequest,
			CircuitBreaker:        cbConfig,
			APIRetryMaxAttempts:   cfg.Config.Resilience.Retry.MaxAttempts,
			APIRetryInitialDelay:  time.Duration(cfg.Config.Resilience.Retry.InitialDelay) * time.Millisecond,
			APIRetryMaxDelay:      time.Duration(cfg.Config.Resilience.Retry.MaxDelay) * time.Millisecond,
			APIRetryBackoffFactor: cfg.Config.Resilience.Retry.BackoffMultiplier,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create TrueNAS client: %w", err)
		}
	}

	// Configure utility package timeouts and rate limiting
	util.SetConfig(&util.UtilConfig{
		MountTimeout:           time.Duration(cfg.Config.CommandTimeouts.Mount) * time.Second,
		FormatTimeout:          time.Duration(cfg.Config.CommandTimeouts.Format) * time.Second,
		ISCSITimeout:           time.Duration(cfg.Config.CommandTimeouts.ISCSI) * time.Second,
		NVMeTimeout:            time.Duration(cfg.Config.CommandTimeouts.NVMe) * time.Second,
		DiscoveryCacheDuration: time.Duration(cfg.Config.Resilience.RateLimiting.DiscoveryCacheDuration) * time.Second,
		MaxConcurrentLogins:    cfg.Config.Resilience.RateLimiting.MaxConcurrentLogins,
	})

	// Initialize event recorder (will be nil if not running in k8s)
	eventRecorder := NewEventRecorder(cfg.Name)

	// Auto-detect topology from node labels when running in node mode
	// This only runs if topology is not already explicitly configured
	if cfg.RunNode && !cfg.Config.Node.Topology.Enabled {
		topology := GetNodeTopology(cfg.NodeID)
		if topology.Zone != "" {
			cfg.Config.Node.Topology.Zone = topology.Zone
		}
		if topology.Region != "" {
			cfg.Config.Node.Topology.Region = topology.Region
		}
		if topology.Zone != "" || topology.Region != "" {
			cfg.Config.Node.Topology.Enabled = true
		}
	}

	// The debouncer invokes the management API and therefore exists only beside
	// the controller client.
	var serviceDebouncer *ServiceReloadDebouncer
	if truenasClient != nil {
		debounceDelay := time.Duration(cfg.Config.ISCSI.ServiceReloadDebounce) * time.Millisecond
		serviceDebouncer = NewServiceReloadDebouncer(debounceDelay, func(ctx context.Context, service string) error {
			return truenasClient.ServiceReload(ctx, service)
		})
	}

	driver := &Driver{
		name:                   cfg.Name,
		version:                cfg.Version,
		nodeID:                 cfg.NodeID,
		encodedNodeID:          encodedNodeID,
		endpoint:               cfg.Endpoint,
		runController:          cfg.RunController,
		runNode:                cfg.RunNode,
		config:                 cfg.Config,
		truenasClient:          truenasClient,
		healthPort:             cfg.HealthPort,
		eventRecorder:          eventRecorder,
		serviceReloadDebouncer: serviceDebouncer,
		nvmeResolvedHosts:      make(map[string]int),
		stagedTargets:          make(map[string]nodeMountRecord),
		publishedTargets:       make(map[string]nodeMountRecord),
		expectedDeleteLogLast:  make(map[string]time.Time),
	}
	driver.observeTrueNASConnection()
	return driver, nil
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
		if removeErr := os.Remove(addr); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("failed to remove socket file: %w", removeErr)
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
		if err := os.Chmod(addr, 0o660); err != nil {
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

	// Bind health/metrics before any reconciliation or background workers. This
	// makes an occupied port a synchronous startup error with no half-started
	// controller or node goroutines left behind.
	if d.healthPort > 0 {
		d.healthServer = NewHealthServer(d, d.healthPort)
		if err := d.healthServer.Start(); err != nil {
			_ = listener.Close()
			return fmt.Errorf("failed to start health server: %w", err)
		}
	}

	// Serve first. Startup fencing may require hundreds of serialized middleware
	// calls on a populated cluster and must never make the CSI endpoint crash-loop.
	// Only strict mode gates readiness until its background retry loop converges.
	d.ready.Store(!d.runController || d.config == nil || d.config.Fencing.Mode != FencingModeStrict)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- d.server.Serve(listener)
	}()
	klog.Infof("CSI driver listening on %s", d.endpoint)

	if d.runController {
		d.startStartupAttachmentReconcile()
		d.startOrphanReconcile()
	}
	if d.runNode {
		d.startSessionGC()
	}

	return <-serveErr
}

// Stop gracefully stops the driver.
func (d *Driver) Stop() {
	klog.Info("Stopping CSI driver")
	d.ready.Store(false)

	// Stop session GC goroutine first
	d.stopSessionGC()
	d.stopStartupAttachmentReconcile()
	d.stopOrphanReconcile()

	// Stop the service reload debouncer
	if d.serviceReloadDebouncer != nil {
		d.serviceReloadDebouncer.Stop()
	}

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

	// Log a cloned request with any CSI secrets structurally removed. The
	// original request is left untouched for the RPC handler.
	klog.V(5).Infof("[req-%d] request: %+v", requestID, requestWithoutSecrets(req))

	// Kubernetes Pod readiness does not stop CSI sidecars in the same Pod from
	// using the Unix socket. During strict startup convergence, enforce the gate
	// at the RPC boundary as well. Teardown and read-only controller calls remain
	// available so transient duplicate VolumeAttachments can drain and unblock
	// the retry loop; grant/provision mutations receive a retryable response.
	var resp interface{}
	var err error
	if d.strictStartupControllerRPCBlocked(info.FullMethod) {
		err = status.Error(codes.Unavailable, "strict fencing startup reconciliation has not converged; retry this controller operation")
	} else {
		resp, err = handler(ctx, req)
	}

	// Calculate duration
	duration := time.Since(startTime)

	// Record metrics
	RecordCSIOperation(info.FullMethod, duration.Seconds(), err)

	// Log result
	if err != nil {
		if volumeID, expected := expectedDeleteVolumeSnapshotDependency(req, err); expected {
			if d.shouldLogExpectedDeleteSnapshotDependency(volumeID, time.Now()) {
				klog.V(2).Infof("[DELETE_VOLUME_WAITING_FOR_DEPENDENT_SNAPSHOTS] [req-%d] %s deferred after %v: %v",
					requestID, info.FullMethod, duration, err)
			}
		} else {
			klog.Errorf("[req-%d] %s failed after %v: %v", requestID, info.FullMethod, duration, err)
		}
	} else {
		klog.V(4).Infof("[req-%d] %s completed in %v", requestID, info.FullMethod, duration)
		klog.V(5).Infof("[req-%d] response: %+v", requestID, resp)
	}

	return resp, err
}

func (d *Driver) strictStartupControllerRPCBlocked(fullMethod string) bool {
	if d.config == nil || d.config.Fencing.Mode != FencingModeStrict || !d.runController || d.ready.Load() {
		return false
	}
	if !strings.HasPrefix(fullMethod, "/csi.v1.Controller/") {
		return false
	}
	switch fullMethod {
	case "/csi.v1.Controller/ControllerGetCapabilities",
		"/csi.v1.Controller/ValidateVolumeCapabilities",
		"/csi.v1.Controller/GetCapacity",
		"/csi.v1.Controller/ListVolumes",
		"/csi.v1.Controller/ControllerGetVolume",
		"/csi.v1.Controller/ListSnapshots",
		"/csi.v1.Controller/ControllerUnpublishVolume",
		"/csi.v1.Controller/DeleteVolume",
		"/csi.v1.Controller/DeleteSnapshot":
		return false
	default:
		return true
	}
}

func expectedDeleteVolumeSnapshotDependency(req interface{}, err error) (string, bool) {
	request, ok := req.(*csi.DeleteVolumeRequest)
	if !ok || request.GetVolumeId() == "" || status.Code(err) != codes.FailedPrecondition {
		return "", false
	}
	expected := fmt.Sprintf("volume %s has dependent snapshots that must be deleted first", request.GetVolumeId())
	return request.GetVolumeId(), status.Convert(err).Message() == expected
}

func (d *Driver) shouldLogExpectedDeleteSnapshotDependency(volumeID string, now time.Time) bool {
	d.expectedDeleteLogMu.Lock()
	defer d.expectedDeleteLogMu.Unlock()
	if d.expectedDeleteLogLast == nil {
		d.expectedDeleteLogLast = make(map[string]time.Time)
	}
	if last := d.expectedDeleteLogLast[volumeID]; !last.IsZero() && now.Sub(last) < expectedDeleteSnapshotLogInterval {
		return false
	}
	d.expectedDeleteLogLast[volumeID] = now
	return true
}

func requestWithoutSecrets(req interface{}) interface{} {
	message, ok := req.(proto.Message)
	if !ok || message == nil {
		return req
	}
	cloned := proto.Clone(message)
	reflected := cloned.ProtoReflect()
	if secrets := reflected.Descriptor().Fields().ByName("secrets"); secrets != nil {
		reflected.Clear(secrets)
	}
	return cloned
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
	cleanupEnabled := d.config.SessionGC.Enabled
	if !cleanupEnabled {
		klog.Info("Session cleanup disabled by configuration; active-session metrics remain enabled")
	}
	intervalSeconds := d.config.SessionGC.Interval
	if intervalSeconds <= 0 {
		intervalSeconds = 300
	}

	interval := time.Duration(intervalSeconds) * time.Second
	gracePeriod := time.Duration(d.config.SessionGC.GracePeriod) * time.Second
	dryRun := d.config.SessionGC.DryRun
	startupDelay := time.Duration(d.config.SessionGC.StartupDelay) * time.Second

	// Per-protocol GC settings (default to enabled if protocol is configured)
	iscsiGCEnabled := cleanupEnabled && d.config.ISCSI.TargetPortal != ""
	if d.config.SessionGC.ISCSIEnabled != nil {
		iscsiGCEnabled = cleanupEnabled && *d.config.SessionGC.ISCSIEnabled && d.config.ISCSI.TargetPortal != ""
	}
	nvmeofGCEnabled := cleanupEnabled && d.config.NVMeoF.TransportAddress != ""
	if d.config.SessionGC.NVMeoFEnabled != nil {
		nvmeofGCEnabled = cleanupEnabled && *d.config.SessionGC.NVMeoFEnabled && d.config.NVMeoF.TransportAddress != ""
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.gcCancel = cancel

	d.gcWg.Add(1)
	go func() {
		defer d.gcWg.Done()
		klog.Infof("Session monitor started: interval=%v, cleanup=%v, gracePeriod=%v, dryRun=%v, iSCSI=%v, NVMeoF=%v",
			interval, cleanupEnabled, gracePeriod, dryRun, iscsiGCEnabled, nvmeofGCEnabled)

		// Short delay to let the system stabilize, then run startup GC
		select {
		case <-time.After(startupDelay):
		case <-ctx.Done():
			klog.Info("Session GC stopped during startup delay")
			return
		}

		// Run GC immediately on startup to clean up stale sessions from node crashes
		// This is proactive cleanup before the first interval tick
		// RunOnStartup defaults to true via config loading
		if cleanupEnabled && d.config.SessionGC.RunOnStartup != nil && *d.config.SessionGC.RunOnStartup {
			klog.Info("Session GC: running proactive cleanup on startup")
			d.runSessionGCWithProtocols(ctx, gracePeriod, dryRun, iscsiGCEnabled, nvmeofGCEnabled)
		} else {
			// Keep the session-count gauges current even when cleanup or startup
			// cleanup is disabled. This path only performs read-only enumeration.
			d.runSessionGCWithProtocols(ctx, gracePeriod, dryRun, false, false)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.runSessionGCWithProtocols(ctx, gracePeriod, dryRun, iscsiGCEnabled, nvmeofGCEnabled)
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

// runSessionGC performs one garbage collection cycle with explicit protocol control.
// It runs GC for the specified protocols, allowing per-protocol enable/disable via config.
// gracePeriod specifies how long a session must be orphaned before cleanup.
func (d *Driver) runSessionGCWithProtocols(ctx context.Context, gracePeriod time.Duration, dryRun, iscsiEnabled, nvmeofEnabled bool) {
	klog.V(4).Info("Running session GC")

	// Run iSCSI GC if enabled
	if iscsiEnabled && ctx.Err() == nil {
		d.gcISCSISessions(ctx, gracePeriod, dryRun)
	} else if ctx.Err() == nil {
		_, _ = d.observeISCSISessions()
	}

	// Run NVMe-oF GC if enabled
	if nvmeofEnabled && ctx.Err() == nil {
		d.gcNVMeoFSessions(ctx, gracePeriod, dryRun)
	} else if ctx.Err() == nil {
		_, _ = d.observeNVMeoFSessions()
	}
}

func (d *Driver) observeISCSISessions() ([]util.ISCSISessionInfo, error) {
	sessions, err := gcListISCSISessions()
	if err != nil {
		klog.Warningf("Session metrics: failed to list iSCSI sessions: %v", err)
		return nil, err
	}
	SetISCSISessions(len(sessions))
	return sessions, nil
}

func (d *Driver) observeNVMeoFSessions() ([]util.NVMeoFSessionInfo, error) {
	sessions, err := gcListNVMeoFSessions()
	if err != nil {
		klog.Warningf("Session metrics: failed to list NVMe-oF sessions: %v", err)
		return nil, err
	}
	SetNVMESessions(len(sessions))
	return sessions, nil
}

// gcISCSISessions garbage collects orphaned iSCSI sessions.
// Sessions must be orphaned for at least gracePeriod before being disconnected.
func (d *Driver) gcISCSISessions(ctx context.Context, gracePeriod time.Duration, dryRun bool) {
	// Get all active iSCSI sessions
	sessions, err := d.observeISCSISessions()
	if err != nil {
		return
	}

	if len(sessions) == 0 {
		klog.V(5).Info("Session GC: no active iSCSI sessions")
		return
	}

	klog.V(4).Infof("Session GC: found %d active iSCSI sessions", len(sessions))

	// Get expected sessions from kubelet staging directory
	// Returns nil if the lookup was unreliable (e.g., race condition during stage/unstage)
	expectedTargets := d.getExpectedISCSITargets()
	if expectedTargets == nil {
		klog.Info("Session GC: skipping iSCSI GC due to unreliable expected targets lookup")
		return
	}
	klog.V(4).Infof("Session GC: found %d expected iSCSI targets from staged volumes", len(expectedTargets))

	// Find orphaned sessions
	portal := d.config.ISCSI.TargetPortal
	orphanedCount := 0
	now := time.Now()

	// Track which owned sessions remain active so this protocol's stale
	// first-seen entries can be retired without touching NVMe state.
	activeOrphanedSessions := make(map[string]struct{})

	for _, session := range sessions {
		if ctx.Err() != nil {
			klog.V(4).Info("Session GC: stopping iSCSI cleanup after cancellation")
			return
		}
		// Only consider sessions for our portal
		if session.Portal != portal && session.Portal != portal+",1" {
			klog.V(5).Infof("Session GC: skipping session %s (different portal: %s)", session.IQN, session.Portal)
			continue
		}
		if !d.isDriverISCSITarget(session.IQN) {
			klog.V(5).Infof("Session GC: skipping session %s (target is not owned by this driver)", session.IQN)
			continue
		}

		// Check if this session is expected
		if _, expected := expectedTargets[session.IQN]; expected {
			klog.V(5).Infof("Session GC: session %s is expected (has staged volume)", session.IQN)
			// Remove from orphaned tracking if it was previously orphaned
			d.orphanedISCSISessionsSeen.Delete(session.IQN)
			continue
		}

		// This session has no corresponding staged volume - it's orphaned
		activeOrphanedSessions[session.IQN] = struct{}{}

		// Check when we first saw this orphaned session
		firstSeenVal, loaded := d.orphanedISCSISessionsSeen.LoadOrStore(session.IQN, now)
		firstSeen, ok := firstSeenVal.(time.Time)
		if !ok {
			klog.Warningf("Session GC: unexpected type in iSCSI first-seen state for %s, resetting", session.IQN)
			d.orphanedISCSISessionsSeen.Store(session.IQN, now)
			continue
		}

		orphanedDuration := now.Sub(firstSeen)
		if !loaded {
			klog.Infof("Session GC: found newly orphaned iSCSI session: %s (will disconnect after %v)", session.IQN, gracePeriod)
			continue
		}

		// Check if grace period has passed
		if orphanedDuration < gracePeriod {
			klog.V(4).Infof("Session GC: orphaned session %s within grace period (%v < %v)", session.IQN, orphanedDuration, gracePeriod)
			continue
		}

		orphanedCount++
		klog.Infof("Session GC: orphaned iSCSI session %s exceeded grace period (%v)", session.IQN, orphanedDuration)

		if dryRun {
			klog.Infof("Session GC: [DRY RUN] would disconnect orphaned session: %s", session.IQN)
			continue
		}

		// Disconnect the orphaned session
		if err := gcDisconnectISCSI(portal, session.IQN); err != nil {
			klog.Warningf("Session GC: failed to disconnect orphaned session %s: %v", session.IQN, err)
		} else {
			klog.Infof("Session GC: disconnected orphaned iSCSI session: %s", session.IQN)
			RecordGCSessionDisconnected("iscsi")
			d.orphanedISCSISessionsSeen.Delete(session.IQN)
		}
	}

	// Clean up stale iSCSI entries for sessions that are no longer active.
	d.orphanedISCSISessionsSeen.Range(func(key, _ interface{}) bool {
		iqn := key.(string)
		if _, active := activeOrphanedSessions[iqn]; !active {
			d.orphanedISCSISessionsSeen.Delete(iqn)
		}
		return true
	})

	if orphanedCount > 0 {
		klog.Infof("Session GC: processed %d orphaned iSCSI sessions", orphanedCount)
	} else {
		klog.V(4).Info("Session GC: no orphaned iSCSI sessions found")
	}
}

// gcNVMeoFSessions garbage collects orphaned NVMe-oF sessions.
// Sessions must be orphaned for at least gracePeriod before being disconnected.
func (d *Driver) gcNVMeoFSessions(ctx context.Context, gracePeriod time.Duration, dryRun bool) {
	// Get all active NVMe-oF sessions
	sessions, err := d.observeNVMeoFSessions()
	if err != nil {
		return
	}

	if len(sessions) == 0 {
		klog.V(5).Info("Session GC: no active NVMe-oF sessions")
		return
	}

	klog.V(4).Infof("Session GC: found %d active NVMe-oF sessions", len(sessions))

	// Get expected sessions from kubelet staging directory
	// Returns nil if the lookup was unreliable (e.g., race condition during stage/unstage)
	expectedNQNs := d.getExpectedNVMeoFNQNs()
	if expectedNQNs == nil {
		klog.Info("Session GC: skipping NVMe-oF GC due to unreliable expected NQNs lookup")
		return
	}
	klog.V(4).Infof("Session GC: found %d expected NVMe-oF NQNs from staged volumes", len(expectedNQNs))

	// Find orphaned sessions
	orphanedCount := 0
	targetAddr := d.config.NVMeoF.TransportAddress
	now := time.Now()

	// Track which sessions are still active so stale NVMe first-seen entries can
	// be retired without touching iSCSI state.
	activeOrphanedSessions := make(map[string]struct{})

	for _, session := range sessions {
		if ctx.Err() != nil {
			klog.V(4).Info("Session GC: stopping NVMe-oF cleanup after cancellation")
			return
		}
		// Only consider sessions for our target address
		// Session address format from nvme list-subsys: "traddr=192.0.2.10,trsvcid=4420,src_addr=203.0.113.10"
		// Config address format: "192.0.2.10"
		if !nvmeSessionMatchesTransportAddress(session.Address, targetAddr) {
			klog.V(5).Infof("Session GC: skipping session %s (different address: %s, target: %s)", session.NQN, session.Address, targetAddr)
			continue
		}

		// Check if this session is expected
		if _, expected := expectedNQNs[session.NQN]; expected {
			klog.V(5).Infof("Session GC: session %s is expected (has staged volume)", session.NQN)
			// Remove from orphaned tracking if it was previously orphaned
			d.orphanedNVMeSessionsSeen.Delete(session.NQN)
			continue
		}

		// This session has no corresponding staged volume - it's orphaned
		activeOrphanedSessions[session.NQN] = struct{}{}

		// Check when we first saw this orphaned session
		firstSeenVal, loaded := d.orphanedNVMeSessionsSeen.LoadOrStore(session.NQN, now)
		firstSeen, ok := firstSeenVal.(time.Time)
		if !ok {
			klog.Warningf("Session GC: unexpected type in NVMe first-seen state for %s, resetting", session.NQN)
			d.orphanedNVMeSessionsSeen.Store(session.NQN, now)
			continue
		}

		orphanedDuration := now.Sub(firstSeen)
		if !loaded {
			klog.Infof("Session GC: found newly orphaned NVMe-oF session: %s (will disconnect after %v)", session.NQN, gracePeriod)
			continue
		}

		// Check if grace period has passed
		if orphanedDuration < gracePeriod {
			klog.V(4).Infof("Session GC: orphaned session %s within grace period (%v < %v)", session.NQN, orphanedDuration, gracePeriod)
			continue
		}

		orphanedCount++
		klog.Infof("Session GC: orphaned NVMe-oF session %s exceeded grace period (%v)", session.NQN, orphanedDuration)

		if dryRun {
			klog.Infof("Session GC: [DRY RUN] would disconnect orphaned session: %s", session.NQN)
			continue
		}

		// Disconnect the orphaned session
		if err := gcDisconnectNVMeoF(session.NQN); err != nil {
			klog.Warningf("Session GC: failed to disconnect orphaned session %s: %v", session.NQN, err)
		} else {
			klog.Infof("Session GC: disconnected orphaned NVMe-oF session: %s", session.NQN)
			RecordGCSessionDisconnected("nvmeof")
			d.orphanedNVMeSessionsSeen.Delete(session.NQN)
		}
	}

	// Clean up stale entries from the NVMe-oF first-seen map.
	d.orphanedNVMeSessionsSeen.Range(func(key, _ interface{}) bool {
		nqn := key.(string)
		// Only clean up NVMe-oF entries (NQNs start with "nqn.")
		if strings.HasPrefix(nqn, "nqn.") {
			if _, active := activeOrphanedSessions[nqn]; !active {
				d.orphanedNVMeSessionsSeen.Delete(nqn)
			}
		}
		return true
	})

	if orphanedCount > 0 {
		klog.Infof("Session GC: processed %d orphaned NVMe-oF sessions", orphanedCount)
	} else {
		klog.V(4).Info("Session GC: no orphaned NVMe-oF sessions found")
	}
}

func nvmeSessionMatchesTransportAddress(sessionAddress, targetAddress string) bool {
	for _, field := range strings.Split(sessionAddress, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
		if ok && key == "traddr" && value == targetAddress {
			return true
		}
	}
	return false
}

// getExpectedISCSITargets returns a map of IQNs that have corresponding staged volumes.
// It scans the kubelet CSI staging directory to find which volumes are currently staged.
// Returns nil if the lookup was unreliable (too many failures), signaling GC should be skipped.
func (d *Driver) getExpectedISCSITargets() map[string]struct{} {
	expected := make(map[string]struct{})

	inUseDevices, err := getInUseBlockDevices()
	if err != nil {
		klog.Warningf("Session GC: failed to get in-use block devices: %v", err)
		return nil // Return nil to signal GC should be skipped
	}

	// Track failed lookups to detect race conditions or transient issues
	// If we fail to look up too many devices, skip GC entirely to avoid data loss
	failedLookups := 0
	const maxFailedLookups = 2 // Allow 1-2 failures for non-iSCSI devices, but more suggests a problem

	// For each mounted device, get its iSCSI session if any
	for device := range inUseDevices {
		// Check if this device is an iSCSI device
		portal, iqn, err := getISCSIInfoFromDevice(device)
		if err != nil {
			// Check if this is likely an iSCSI device by looking at the device name pattern
			// iSCSI devices are typically sd[a-z]+ (not nvme*, loop*, etc)
			if util.IsLikelyISCSIDevice(device) {
				failedLookups++
				klog.V(4).Infof("Session GC: failed to get iSCSI info for %s (may be race condition): %v", device, err)
			}
			continue
		}
		if portal != "" && iqn != "" {
			expected[iqn] = struct{}{}
		}
	}

	// If too many lookups failed, this might indicate a race condition
	// (concurrent stage/unstage operations) - skip GC to be safe
	if failedLookups > maxFailedLookups {
		klog.Warningf("Session GC: %d iSCSI device lookups failed, skipping GC to avoid race condition", failedLookups)
		return nil // Return nil to signal GC should be skipped
	}

	return expected
}

// getExpectedNVMeoFNQNs returns a map of NQNs that have corresponding staged volumes.
// Returns nil if the lookup was unreliable (e.g., error getting mounted devices),
// signaling GC should be skipped to prevent false positives.
func (d *Driver) getExpectedNVMeoFNQNs() map[string]struct{} {
	inUseDevices, err := getInUseBlockDevices()
	if err != nil {
		klog.Warningf("Session GC: failed to get in-use block devices: %v", err)
		return nil // Return nil to signal GC should be skipped
	}

	expected := make(map[string]struct{})

	// Track failed lookups to detect race conditions or transient issues
	// If we fail to look up too many devices, skip GC entirely to avoid data loss
	failedLookups := 0
	const maxFailedLookups = 2 // Allow 1-2 failures, but more suggests a problem

	// For each mounted device, get its NVMe-oF session if any
	for device := range inUseDevices {
		// Check if this device is an NVMe device
		nqn, err := getNVMeInfoFromDevice(device)
		if err != nil {
			// Check if this looks like an NVMe device (might be transient state)
			if util.IsLikelyNVMeDevice(device) {
				failedLookups++
				klog.V(4).Infof("Session GC: failed to get NVMe info for %s (may be race condition): %v", device, err)
			}
			continue
		}
		if nqn != "" {
			expected[nqn] = struct{}{}
		}
	}

	// If too many lookups failed, this might indicate a race condition
	// (concurrent connect/disconnect operations) - skip GC to be safe
	if failedLookups > maxFailedLookups {
		klog.Warningf("Session GC: %d NVMe device lookups failed, skipping GC to avoid race condition", failedLookups)
		return nil // Return nil to signal GC should be skipped
	}

	return expected
}

func getInUseBlockDevices() (map[string]string, error) {
	mountedDevices, err := getMountedBlockDevices()
	if err != nil {
		return nil, fmt.Errorf("get mounted block devices: %w", err)
	}
	if mountedDevices == nil {
		mountedDevices = make(map[string]string)
	}

	stagedDevices, err := getStagedBlockDevices()
	if err != nil {
		return nil, fmt.Errorf("scan staged block devices: %w", err)
	}

	for device, stagingPath := range stagedDevices {
		mountedDevices[device] = stagingPath
	}
	return mountedDevices, nil
}
