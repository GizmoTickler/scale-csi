// Package driver implements the CSI driver for TrueNAS Scale.
package driver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
)

var configWarningf = klog.Warningf

// Config holds the driver configuration loaded from YAML.
type Config struct {
	// DriverName is the CSI driver name for registration
	DriverName string `yaml:"driver"`

	// DriverInstanceID is stamped on every driver-created dataset. It is the
	// ownership boundary used before adopting a pre-existing dataset whose name
	// collides with a CreateVolume request. When omitted, LoadConfig derives a
	// stable value from DriverName and the configured parent dataset.
	DriverInstanceID string `yaml:"driverInstanceId"`

	// InstanceID is the deprecated pre-v1.2.23 spelling. It was never consumed
	// by the driver, but accepting it here avoids breaking otherwise valid old
	// configuration files. driverInstanceId wins when both are present.
	InstanceID string `yaml:"instance_id"`

	// Fencing controls whether ControllerPublish/Unpublish materialize CSI
	// publications in the backend transport allowlists.
	Fencing FencingConfig `yaml:"fencing"`

	// TrueNAS connection settings
	TrueNAS TrueNASConfig `yaml:"truenas"`

	// ZFS dataset configuration
	ZFS ZFSConfig `yaml:"zfs"`

	// NFS share configuration
	NFS NFSConfig `yaml:"nfs"`

	// iSCSI configuration
	ISCSI ISCSIConfig `yaml:"iscsi"`

	// NVMe-oF configuration
	NVMeoF NVMeoFConfig `yaml:"nvmeof"`

	// Session garbage collection configuration (node plugin only)
	SessionGC SessionGCConfig `yaml:"sessionGC"`

	// Reconcile configures controller-side orphan detection and gated cleanup.
	Reconcile ReconcileConfig `yaml:"reconcile"`

	// Node configuration (node plugin only)
	Node NodeConfig `yaml:"node"`

	// Resilience configuration for retry, circuit breaker, and rate limiting
	Resilience ResilienceConfig `yaml:"resilience"`

	// CommandTimeouts configures timeouts for various command types
	CommandTimeouts CommandTimeoutConfig `yaml:"commandTimeouts"`
}

// FencingMode controls backend publication enforcement during rolling upgrades.
type FencingMode string

const (
	// FencingModeOff preserves the pre-v1.2.23 static-allowlist behavior.
	FencingModeOff FencingMode = "off"
	// FencingModeAdditive adds per-node identities without removing configured
	// static identities. It is the explicit rolling-upgrade transition mode;
	// the default remains off until operators complete the node-first sequence.
	FencingModeAdditive FencingMode = "additive"
	// FencingModeStrict makes per-volume publication records the sole allowlist.
	FencingModeStrict FencingMode = "strict"
)

// FencingConfig configures backend-enforced ControllerPublish semantics.
type FencingConfig struct {
	Mode FencingMode `yaml:"mode"`
	// StartupReconcileTimeout bounds one background convergence attempt. Failed
	// attempts retry with backoff without taking down the CSI endpoint.
	StartupReconcileTimeout string `yaml:"startupReconcileTimeout"`
	// StaleRecordGracePeriod is the continuous absence required before a
	// publication record with no live VolumeAttachment may be revoked.
	StaleRecordGracePeriod string `yaml:"staleRecordGracePeriod"`
}

// Enabled reports whether backend publication fencing is active.
func (c FencingConfig) Enabled() bool {
	return c.Mode == FencingModeAdditive || c.Mode == FencingModeStrict
}

func (c FencingConfig) StartupReconcileTimeoutDuration() (time.Duration, error) {
	if strings.TrimSpace(c.StartupReconcileTimeout) == "" {
		return 10 * time.Minute, nil
	}
	return time.ParseDuration(c.StartupReconcileTimeout)
}

func (c FencingConfig) StaleRecordGracePeriodDuration() (time.Duration, error) {
	if strings.TrimSpace(c.StaleRecordGracePeriod) == "" {
		return 10 * time.Minute, nil
	}
	return time.ParseDuration(c.StaleRecordGracePeriod)
}

// TrueNASConfig holds TrueNAS connection settings.
type TrueNASConfig struct {
	// Host is the TrueNAS hostname or IP
	Host string `yaml:"host"`

	// Port is the API port (default: 443 for HTTPS, 80 for HTTP)
	Port int `yaml:"port"`

	// Protocol is http or https
	Protocol string `yaml:"protocol"`

	// APIKey is the TrueNAS API key for authentication
	APIKey string `yaml:"apiKey"`

	// AllowInsecure skips TLS verification
	AllowInsecure bool `yaml:"allowInsecure"`

	// CACert is an optional PEM-encoded CA certificate used instead of system roots.
	CACert string `yaml:"caCert"`

	// CACertFile is an optional path to a PEM-encoded CA certificate file.
	CACertFile string `yaml:"caCertFile"`

	// RequestTimeout is the timeout for API requests in seconds (default: 60)
	RequestTimeout int `yaml:"requestTimeout"`

	// ConnectTimeout is the timeout for establishing connections in seconds (default: 10)
	ConnectTimeout int `yaml:"connectTimeout"`

	// WriteTimeout is the timeout for each WebSocket write in seconds (default: 30)
	WriteTimeout int `yaml:"writeTimeout"`

	// MaxConcurrentRequests limits concurrent API requests to prevent overwhelming TrueNAS (default: 10)
	MaxConcurrentRequests int `yaml:"maxConcurrentRequests"`
}

// ZFSConfig holds ZFS dataset configuration.
type ZFSConfig struct {
	// DatasetParentName is the parent dataset for volumes (e.g., "tank/k8s/volumes")
	DatasetParentName string `yaml:"datasetParentName"`

	// DetachedVolumesFromSnapshots creates independent local send/receive copies
	// instead of ZFS clones when provisioning a volume from a snapshot.
	DetachedVolumesFromSnapshots bool `yaml:"detachedVolumesFromSnapshots"`

	// DatasetEnableQuotas enables quota support for NFS volumes
	DatasetEnableQuotas bool `yaml:"datasetEnableQuotas"`

	// DatasetEnableReservation enables reservation support
	DatasetEnableReservation bool `yaml:"datasetEnableReservation"`

	// DatasetProperties are additional ZFS properties to set on datasets
	DatasetProperties map[string]string `yaml:"datasetProperties"`

	// ZvolBlocksize is the block size for zvols (default: 16K)
	ZvolBlocksize string `yaml:"zvolBlocksize"`

	// ZvolEnableReservation thick-provisions zvols with a full refreservation.
	ZvolEnableReservation bool `yaml:"zvolEnableReservation"`

	// ZvolReadyTimeout is the timeout in seconds for waiting for a zvol to be ready
	// after cloning operations. Increase for slow systems or large clones (default: 60)
	ZvolReadyTimeout int `yaml:"zvolReadyTimeout"`

	// DestroyForeignSnapshotsOnDelete allows recursive dataset deletion to remove
	// snapshots that were not created by the CSI driver (default: false)
	DestroyForeignSnapshotsOnDelete bool `yaml:"destroyForeignSnapshotsOnDelete"`
}

// NFSConfig holds NFS share configuration.
type NFSConfig struct {
	// Enabled enables NFS provisioning for StorageClasses that select NFS.
	Enabled bool `yaml:"enabled"`

	// ShareHost is the NFS server hostname/IP for clients to connect
	ShareHost string `yaml:"shareHost"`

	// ShareAllowedNetworks is a list of allowed networks (CIDR notation)
	ShareAllowedNetworks []string `yaml:"shareAllowedNetworks"`

	// ShareAllowedHosts is a list of allowed hosts
	ShareAllowedHosts []string `yaml:"shareAllowedHosts"`

	// ShareMaprootUser maps root to this user
	ShareMaprootUser string `yaml:"shareMaprootUser"`

	// ShareMaprootGroup maps root to this group
	ShareMaprootGroup string `yaml:"shareMaprootGroup"`

	// ShareMapallUser maps all users to this user
	ShareMapallUser string `yaml:"shareMapallUser"`

	// ShareMapallGroup maps all users to this group
	ShareMapallGroup string `yaml:"shareMapallGroup"`

	// ShareCommentTemplate is a template for share comments
	ShareCommentTemplate string `yaml:"shareCommentTemplate"`
}

// ISCSIConfig holds iSCSI configuration.
type ISCSIConfig struct {
	// Enabled enables iSCSI provisioning for StorageClasses that select iSCSI.
	Enabled bool `yaml:"enabled"`

	// TargetPortal is the iSCSI target portal (host:port)
	TargetPortal string `yaml:"targetPortal"`

	// TargetPortals is a list of additional portals for multipath
	TargetPortals []string `yaml:"targetPortals"`

	// Interface is the iSCSI interface to use (default: "default")
	Interface string `yaml:"interface"`

	// NameSuffix is a suffix for iSCSI target/extent names
	NameSuffix string `yaml:"nameSuffix"`

	// TargetGroups is the list of portal/initiator groups
	TargetGroups []ISCSITargetGroup `yaml:"targetGroups"`

	// ExtentBlocksize is the block size for extents (default: 512)
	ExtentBlocksize int `yaml:"extentBlocksize"`

	// ExtentDisablePhysicalBlocksize disables reporting physical block size
	ExtentDisablePhysicalBlocksize bool `yaml:"extentDisablePhysicalBlocksize"`

	// ExtentRpm sets the RPM reported to initiators (default: "SSD")
	ExtentRpm string `yaml:"extentRpm"`

	// ExtentAvailThreshold is the threshold for space warnings (0-100)
	ExtentAvailThreshold int `yaml:"extentAvailThreshold"`

	// DeviceWaitTimeout is the timeout for waiting for iSCSI devices to appear in seconds (default: 60)
	DeviceWaitTimeout int `yaml:"deviceWaitTimeout"`

	// ServiceReloadDebounce is the debounce window in milliseconds for iSCSI service reloads.
	// Multiple share creations within this window will be coalesced into a single reload.
	// This prevents reload storms during bulk volume provisioning. (default: 2000ms)
	ServiceReloadDebounce int `yaml:"serviceReloadDebounce"`
}

// ISCSITargetGroup represents a portal/initiator group configuration.
type ISCSITargetGroup struct {
	// Portal is the portal group ID
	Portal int `yaml:"portal"`

	// Initiator is the initiator group ID
	Initiator int `yaml:"initiator"`

	// AuthMethod is the authentication method
	AuthMethod string `yaml:"authMethod"`

	// Auth is the auth group ID
	Auth *int `yaml:"auth"`
}

// SessionGCConfig holds session garbage collection configuration.
type SessionGCConfig struct {
	// Enabled enables periodic garbage collection of orphaned sessions (default: true)
	Enabled bool `yaml:"enabled"`

	// Interval is the interval between GC runs in seconds (default: 300 = 5 minutes)
	Interval int `yaml:"interval"`

	// GracePeriod is how long a session must be orphaned before cleanup in seconds (default: 60)
	// This prevents cleaning up sessions that are in the middle of being staged
	GracePeriod int `yaml:"gracePeriod"`

	// DryRun logs orphaned sessions without disconnecting them (default: false)
	DryRun bool `yaml:"dryRun"`

	// RunOnStartup runs GC immediately when driver starts (default: true)
	// This helps clean up stale sessions from previous node crashes
	// Use pointer to detect explicit false vs unset (which defaults to true)
	RunOnStartup *bool `yaml:"runOnStartup"`

	// StartupDelay is the delay in seconds before running startup GC (default: 5)
	// Allows the driver to stabilize before running GC
	StartupDelay int `yaml:"startupDelay"`

	// ISCSIEnabled enables GC for iSCSI sessions (default: true if iSCSI configured)
	ISCSIEnabled *bool `yaml:"iscsiEnabled"`

	// NVMeoFEnabled enables GC for NVMe-oF sessions (default: true if NVMe-oF configured)
	NVMeoFEnabled *bool `yaml:"nvmeofEnabled"`
}

// ReconcileConfig configures periodic orphan detection and the run-once GC mode.
type ReconcileConfig struct {
	// Enabled starts read-only orphan detection on the controller (default: true).
	Enabled bool `yaml:"enabled"`

	// Interval is the duration between read-only detection passes (default: 1h).
	Interval string `yaml:"interval"`

	// MinOrphanAge is the minimum backend object age before it can be orphaned (default: 24h).
	MinOrphanAge string `yaml:"minOrphanAge"`

	// Delete configures the separately gated run-once cleanup entrypoint.
	Delete ReconcileDeleteConfig `yaml:"delete"`
}

// ReconcileDeleteConfig configures the opt-in orphan cleanup CronJob.
type ReconcileDeleteConfig struct {
	// Enabled permits --mode=reconcile to perform guarded deletion (default: false).
	Enabled bool `yaml:"enabled"`

	// Schedule is the CronJob schedule used by the Helm chart (default: 0 4 * * *).
	Schedule string `yaml:"schedule"`

	// MaxPerRun limits successful deletions across all object types (default: 5).
	MaxPerRun int `yaml:"maxPerRun"`
}

// IntervalDuration parses the configured reconcile interval.
func (c ReconcileConfig) IntervalDuration() (time.Duration, error) {
	return time.ParseDuration(c.Interval)
}

// MinOrphanAgeDuration parses the configured minimum orphan age.
func (c ReconcileConfig) MinOrphanAgeDuration() (time.Duration, error) {
	return time.ParseDuration(c.MinOrphanAge)
}

// NVMeoFConfig holds NVMe-oF configuration.
type NVMeoFConfig struct {
	// Enabled enables NVMe-oF provisioning for StorageClasses that select NVMe-oF.
	Enabled bool `yaml:"enabled"`

	// Transport is the transport type (tcp, rdma)
	Transport string `yaml:"transport"`

	// TransportAddress is the target address
	TransportAddress string `yaml:"transportAddress"`

	// TransportServiceID is the port (default: 4420)
	TransportServiceID int `yaml:"transportServiceId"`

	// NamePrefix is a prefix for subsystem/namespace names
	NamePrefix string `yaml:"namePrefix"`

	// NameSuffix is a suffix for subsystem/namespace names
	NameSuffix string `yaml:"nameSuffix"`

	// NameTemplate is a template for generating names
	NameTemplate string `yaml:"nameTemplate"`

	// SubsystemAllowAnyHost allows any host to connect
	SubsystemAllowAnyHost bool `yaml:"subsystemAllowAnyHost"`

	// SubsystemHosts is a list of allowed host NQNs
	SubsystemHosts []string `yaml:"subsystemHosts"`

	// DeviceWaitTimeout is the timeout for waiting for NVMe-oF devices to appear in seconds (default: 60)
	DeviceWaitTimeout int `yaml:"deviceWaitTimeout"`

	// CommandTimeout is the timeout for nvme CLI commands in seconds (default: 30)
	CommandTimeout int `yaml:"commandTimeout"`
}

// NodeConfig holds node-side configuration options.
type NodeConfig struct {
	// Topology configures topology awareness for the CSI driver
	Topology TopologyConfig `yaml:"topology"`

	// SessionCleanupDelay is the delay in milliseconds before retrying after cleaning up
	// an existing session during staging (default: 500)
	SessionCleanupDelay int `yaml:"sessionCleanupDelay"`

	// MaxVolumesPerNode is the maximum number of volumes the node can publish.
	// Zero means unlimited and is not advertised (default: 0)
	MaxVolumesPerNode int64 `yaml:"maxVolumesPerNode"`
}

// TopologyConfig holds topology awareness configuration.
type TopologyConfig struct {
	// Enabled enables topology awareness (default: false)
	Enabled bool `yaml:"enabled"`

	// Zone is the availability zone for this node (e.g., "zone-a")
	Zone string `yaml:"zone"`

	// Region is the region for this node (e.g., "us-west-1")
	Region string `yaml:"region"`

	// CustomLabels are additional topology labels to report
	// Keys should follow the format "topology.kubernetes.io/custom-key"
	CustomLabels map[string]string `yaml:"customLabels"`
}

// ResilienceConfig holds configuration for retry, circuit breaker, and rate limiting.
type ResilienceConfig struct {
	// CircuitBreaker configures the circuit breaker for TrueNAS API calls
	CircuitBreaker CircuitBreakerConfig `yaml:"circuitBreaker"`

	// Retry configures retry behavior for API calls
	Retry RetryConfig `yaml:"retry"`

	// RateLimiting configures rate limiting for API calls
	RateLimiting RateLimitConfig `yaml:"rateLimiting"`
}

// CircuitBreakerConfig holds circuit breaker configuration.
type CircuitBreakerConfig struct {
	// Enabled enables the opt-in circuit breaker (default: false)
	Enabled bool `yaml:"enabled"`

	// FailureThreshold is the number of consecutive failures before opening the circuit (default: 5)
	FailureThreshold int `yaml:"failureThreshold"`

	// SuccessThreshold is the number of consecutive successes to close the circuit (default: 2)
	SuccessThreshold int `yaml:"successThreshold"`

	// Timeout is the time in seconds the circuit stays open before transitioning to half-open (default: 30)
	Timeout int `yaml:"timeout"`

	// HalfOpenMaxRequests is the max requests allowed in half-open state (default: 3)
	HalfOpenMaxRequests int `yaml:"halfOpenMaxRequests"`
}

// RetryConfig holds retry configuration.
type RetryConfig struct {
	// MaxAttempts is the maximum number of retry attempts (default: 3)
	MaxAttempts int `yaml:"maxAttempts"`

	// InitialDelay is the initial delay in milliseconds between retries (default: 500)
	InitialDelay int `yaml:"initialDelay"`

	// MaxDelay is the maximum delay in milliseconds between retries (default: 5000)
	MaxDelay int `yaml:"maxDelay"`

	// BackoffMultiplier is the multiplier for exponential backoff (default: 2.0)
	BackoffMultiplier float64 `yaml:"backoffMultiplier"`
}

// RateLimitConfig holds rate limiting configuration.
type RateLimitConfig struct {
	// MaxConcurrentRequests limits concurrent API requests (default: 10)
	MaxConcurrentRequests int `yaml:"maxConcurrentRequests"`

	// MaxConcurrentLogins limits concurrent iSCSI logins per portal (default: 2)
	MaxConcurrentLogins int `yaml:"maxConcurrentLogins"`

	// DiscoveryCacheDuration is how long discovery results are cached in seconds (default: 30)
	DiscoveryCacheDuration int `yaml:"discoveryCacheDuration"`
}

// CommandTimeoutConfig holds timeouts for various command types.
type CommandTimeoutConfig struct {
	// Mount is the timeout for mount commands in seconds (default: 30)
	Mount int `yaml:"mount"`

	// Format is the timeout for format commands in seconds (default: 300)
	Format int `yaml:"format"`

	// ISCSI is the timeout for iscsiadm commands in seconds (default: 10)
	ISCSI int `yaml:"iscsi"`

	// NVMe is the timeout for nvme CLI commands in seconds (default: 30)
	NVMe int `yaml:"nvme"`
}

func yamlPathExists(document *yaml.Node, path ...string) bool {
	if document == nil || len(path) == 0 {
		return false
	}
	node := document
	if node.Kind == yaml.DocumentNode {
		if len(node.Content) == 0 {
			return false
		}
		node = node.Content[0]
	}
	for _, key := range path {
		if node.Kind != yaml.MappingNode {
			return false
		}
		var next *yaml.Node
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == key {
				next = node.Content[i+1]
				break
			}
		}
		if next == nil {
			return false
		}
		node = next
	}
	return true
}

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables in the config
	data = []byte(os.ExpandEnv(string(data)))

	// Defaults must be applied before unmarshalling so an explicit YAML false
	// still overrides the default.
	cfg := &Config{
		Fencing: FencingConfig{
			Mode:                    FencingModeOff,
			StartupReconcileTimeout: "10m",
			StaleRecordGracePeriod:  "10m",
		},
		ZFS:       ZFSConfig{DatasetEnableQuotas: true},
		SessionGC: SessionGCConfig{Enabled: true},
		Reconcile: ReconcileConfig{
			Enabled:      true,
			Interval:     "1h",
			MinOrphanAge: "24h",
			Delete: ReconcileDeleteConfig{
				Schedule:  "0 4 * * *",
				MaxPerRun: 5,
			},
		},
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	var extraDocument yaml.Node
	if err := decoder.Decode(&extraDocument); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
		return nil, fmt.Errorf("failed to parse config file: multiple YAML documents are not supported")
	}
	if err := validateNonNegativeConfig(cfg); err != nil {
		return nil, err
	}

	// Protocol blocks historically did not contain an explicit enabled field.
	// Preserve those config files while allowing an explicitly disabled block.
	var configDocument yaml.Node
	if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&configDocument); err != nil {
		return nil, fmt.Errorf("failed to inspect protocol configuration: %w", err)
	}
	if yamlPathExists(&configDocument, "nfs") && !yamlPathExists(&configDocument, "nfs", "enabled") {
		cfg.NFS.Enabled = true
	}
	if yamlPathExists(&configDocument, "iscsi") && !yamlPathExists(&configDocument, "iscsi", "enabled") {
		cfg.ISCSI.Enabled = true
	}
	if yamlPathExists(&configDocument, "nvmeof") && !yamlPathExists(&configDocument, "nvmeof", "enabled") {
		cfg.NVMeoF.Enabled = true
	}
	if len(cfg.ISCSI.TargetPortals) > 0 {
		configWarningf("iscsi.targetPortals is configured with %d additional portal(s), but scale-csi does not currently support iSCSI multipath; only iscsi.targetPortal will be used", len(cfg.ISCSI.TargetPortals))
	}
	if strings.TrimSpace(cfg.DriverInstanceID) == "" {
		cfg.DriverInstanceID = strings.TrimSpace(cfg.InstanceID)
	}
	if strings.TrimSpace(cfg.DriverInstanceID) == "" && cfg.DriverName != "" && cfg.ZFS.DatasetParentName != "" {
		cfg.DriverInstanceID = strings.TrimSpace(cfg.DriverName) + "@" + strings.TrimSuffix(strings.TrimSpace(cfg.ZFS.DatasetParentName), "/")
	}
	cfg.DriverInstanceID = strings.TrimSpace(cfg.DriverInstanceID)
	if cfg.DriverInstanceID == "" {
		return nil, fmt.Errorf("driverInstanceId must not be empty")
	}
	switch cfg.Fencing.Mode {
	case FencingModeOff, FencingModeAdditive, FencingModeStrict:
	default:
		return nil, fmt.Errorf("fencing.mode must be one of off, additive, or strict")
	}
	if cfg.Fencing.StartupReconcileTimeout == "" {
		cfg.Fencing.StartupReconcileTimeout = "10m"
	}
	if timeout, parseErr := cfg.Fencing.StartupReconcileTimeoutDuration(); parseErr != nil || timeout <= 0 {
		if parseErr != nil {
			return nil, fmt.Errorf("fencing.startupReconcileTimeout must be a positive duration: %w", parseErr)
		}
		return nil, fmt.Errorf("fencing.startupReconcileTimeout must be a positive duration")
	}
	if cfg.Fencing.StaleRecordGracePeriod == "" {
		cfg.Fencing.StaleRecordGracePeriod = "10m"
	}
	if grace, parseErr := cfg.Fencing.StaleRecordGracePeriodDuration(); parseErr != nil || grace <= 0 {
		if parseErr != nil {
			return nil, fmt.Errorf("fencing.staleRecordGracePeriod must be a positive duration: %w", parseErr)
		}
		return nil, fmt.Errorf("fencing.staleRecordGracePeriod must be a positive duration")
	}

	// Set defaults
	if cfg.TrueNAS.Protocol == "" {
		cfg.TrueNAS.Protocol = "https"
	}
	if cfg.TrueNAS.Port == 0 {
		if cfg.TrueNAS.Protocol == "https" {
			cfg.TrueNAS.Port = 443
		} else {
			cfg.TrueNAS.Port = 80
		}
	}
	if cfg.TrueNAS.RequestTimeout == 0 {
		cfg.TrueNAS.RequestTimeout = 60
	}
	if cfg.TrueNAS.ConnectTimeout == 0 {
		cfg.TrueNAS.ConnectTimeout = 10
	}
	if cfg.TrueNAS.WriteTimeout == 0 {
		cfg.TrueNAS.WriteTimeout = 30
	}
	if cfg.TrueNAS.MaxConcurrentRequests == 0 {
		cfg.TrueNAS.MaxConcurrentRequests = 10
	}
	if cfg.ZFS.ZvolBlocksize == "" {
		cfg.ZFS.ZvolBlocksize = "16K"
	}
	if cfg.ZFS.ZvolReadyTimeout == 0 {
		cfg.ZFS.ZvolReadyTimeout = 60 // Default 60 seconds for zvol clone readiness
	}
	if cfg.ISCSI.Interface == "" {
		cfg.ISCSI.Interface = "default"
	}
	if cfg.ISCSI.ExtentBlocksize == 0 {
		cfg.ISCSI.ExtentBlocksize = 512
	}
	if cfg.ISCSI.ExtentRpm == "" {
		cfg.ISCSI.ExtentRpm = "SSD"
	}
	if cfg.ISCSI.DeviceWaitTimeout == 0 {
		cfg.ISCSI.DeviceWaitTimeout = 60 // Default 60 seconds
	}
	if cfg.ISCSI.ServiceReloadDebounce == 0 {
		cfg.ISCSI.ServiceReloadDebounce = 2000 // Default 2 seconds debounce window
	}
	if cfg.NVMeoF.Transport == "" {
		cfg.NVMeoF.Transport = "tcp"
	}
	if cfg.NVMeoF.TransportServiceID == 0 {
		cfg.NVMeoF.TransportServiceID = 4420
	}
	if cfg.NVMeoF.DeviceWaitTimeout == 0 {
		cfg.NVMeoF.DeviceWaitTimeout = 60 // Default 60 seconds
	}
	if cfg.Reconcile.Interval == "" {
		cfg.Reconcile.Interval = "1h"
	}
	if cfg.Reconcile.MinOrphanAge == "" {
		cfg.Reconcile.MinOrphanAge = "24h"
	}
	if cfg.Reconcile.Delete.Schedule == "" {
		cfg.Reconcile.Delete.Schedule = "0 4 * * *"
	}
	if cfg.Reconcile.Delete.MaxPerRun <= 0 {
		return nil, fmt.Errorf("reconcile.delete.maxPerRun must be positive")
	}
	if interval, parseErr := cfg.Reconcile.IntervalDuration(); parseErr != nil || interval <= 0 {
		if parseErr != nil {
			return nil, fmt.Errorf("reconcile.interval must be a positive duration: %w", parseErr)
		}
		return nil, fmt.Errorf("reconcile.interval must be a positive duration")
	}
	if minAge, parseErr := cfg.Reconcile.MinOrphanAgeDuration(); parseErr != nil || minAge <= 0 {
		if parseErr != nil {
			return nil, fmt.Errorf("reconcile.minOrphanAge must be a positive duration: %w", parseErr)
		}
		return nil, fmt.Errorf("reconcile.minOrphanAge must be a positive duration")
	}

	// Session GC defaults to enabled. An explicit enabled=false remains
	// authoritative regardless of interval; enabled=true with no interval uses
	// the five-minute default.
	if cfg.SessionGC.Enabled && cfg.SessionGC.Interval <= 0 {
		cfg.SessionGC.Interval = 300 // 5 minutes
	}
	if cfg.SessionGC.GracePeriod == 0 {
		cfg.SessionGC.GracePeriod = 60 // 1 minute grace period
	}
	if cfg.SessionGC.StartupDelay == 0 {
		cfg.SessionGC.StartupDelay = 5 // 5 seconds delay before startup GC
	}
	// RunOnStartup defaults to true (helps clean stale sessions from crashes)
	if cfg.SessionGC.RunOnStartup == nil {
		defaultTrue := true
		cfg.SessionGC.RunOnStartup = &defaultTrue
	}

	// Node defaults
	if cfg.Node.SessionCleanupDelay == 0 {
		cfg.Node.SessionCleanupDelay = 500 // 500ms default
	}

	// Note: Topology is auto-detected from Kubernetes node labels during driver initialization
	// See NewDriver() in driver.go which calls GetNodeTopology()
	// Manual config via node.topology.zone/region is still supported and takes precedence

	// Resilience defaults - Circuit Breaker
	if cfg.Resilience.CircuitBreaker.FailureThreshold == 0 {
		cfg.Resilience.CircuitBreaker.FailureThreshold = 5
	}
	if cfg.Resilience.CircuitBreaker.SuccessThreshold == 0 {
		cfg.Resilience.CircuitBreaker.SuccessThreshold = 2
	}
	if cfg.Resilience.CircuitBreaker.Timeout == 0 {
		cfg.Resilience.CircuitBreaker.Timeout = 30 // 30 seconds
	}
	if cfg.Resilience.CircuitBreaker.HalfOpenMaxRequests == 0 {
		cfg.Resilience.CircuitBreaker.HalfOpenMaxRequests = 3
	}

	// Resilience defaults - Retry
	if cfg.Resilience.Retry.MaxAttempts == 0 {
		cfg.Resilience.Retry.MaxAttempts = 3
	}
	if cfg.Resilience.Retry.InitialDelay == 0 {
		cfg.Resilience.Retry.InitialDelay = 500 // 500ms
	}
	if cfg.Resilience.Retry.MaxDelay == 0 {
		cfg.Resilience.Retry.MaxDelay = 5000 // 5 seconds
	}
	if cfg.Resilience.Retry.BackoffMultiplier == 0 {
		cfg.Resilience.Retry.BackoffMultiplier = 2.0
	}

	// Resilience defaults - Rate Limiting
	if cfg.Resilience.RateLimiting.MaxConcurrentRequests == 0 {
		cfg.Resilience.RateLimiting.MaxConcurrentRequests = 10
	}
	if cfg.Resilience.RateLimiting.MaxConcurrentLogins == 0 {
		cfg.Resilience.RateLimiting.MaxConcurrentLogins = 2
	}
	if cfg.Resilience.RateLimiting.DiscoveryCacheDuration == 0 {
		cfg.Resilience.RateLimiting.DiscoveryCacheDuration = 30 // 30 seconds
	}

	// Command timeout defaults
	if cfg.CommandTimeouts.Mount == 0 {
		cfg.CommandTimeouts.Mount = 30 // 30 seconds
	}
	if cfg.CommandTimeouts.Format == 0 {
		cfg.CommandTimeouts.Format = 300 // 5 minutes
	}
	if cfg.CommandTimeouts.ISCSI == 0 {
		cfg.CommandTimeouts.ISCSI = 10 // 10 seconds
	}
	if cfg.CommandTimeouts.NVMe == 0 {
		cfg.CommandTimeouts.NVMe = 30 // 30 seconds
	}

	// NVMe-oF command timeout default
	if cfg.NVMeoF.CommandTimeout == 0 {
		cfg.NVMeoF.CommandTimeout = 30 // 30 seconds
	}

	// Validate required fields
	if cfg.TrueNAS.Host == "" {
		return nil, fmt.Errorf("truenas.host is required")
	}
	if cfg.ZFS.DatasetParentName == "" {
		return nil, fmt.Errorf("zfs.datasetParentName is required")
	}

	// The unified driver selects a protocol per StorageClass. Validate only the
	// protocols that this deployment enabled, never the deprecated driver-name
	// fallback.
	if !cfg.NFS.Enabled && !cfg.ISCSI.Enabled && !cfg.NVMeoF.Enabled {
		return nil, fmt.Errorf("at least one storage protocol must be enabled (nfs, iscsi, or nvmeof)")
	}
	if cfg.NFS.Enabled && cfg.NFS.ShareHost == "" {
		return nil, fmt.Errorf("nfs.shareHost is required when NFS is enabled")
	}
	if cfg.ISCSI.Enabled && cfg.ISCSI.TargetPortal == "" {
		return nil, fmt.Errorf("iscsi.targetPortal is required when iSCSI is enabled")
	}
	if cfg.NVMeoF.Enabled {
		if cfg.NVMeoF.TransportAddress == "" {
			return nil, fmt.Errorf("nvmeof.transportAddress is required when NVMe-oF is enabled")
		}
		if cfg.Fencing.Mode != FencingModeStrict && !cfg.NVMeoF.SubsystemAllowAnyHost && len(cfg.NVMeoF.SubsystemHosts) == 0 {
			return nil, fmt.Errorf("nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty; no host could connect")
		}
	}

	return cfg, nil
}

func validateNonNegativeConfig(cfg *Config) error {
	fields := []struct {
		name  string
		value int
	}{
		{"truenas.port", cfg.TrueNAS.Port},
		{"truenas.requestTimeout", cfg.TrueNAS.RequestTimeout},
		{"truenas.connectTimeout", cfg.TrueNAS.ConnectTimeout},
		{"truenas.writeTimeout", cfg.TrueNAS.WriteTimeout},
		{"truenas.maxConcurrentRequests", cfg.TrueNAS.MaxConcurrentRequests},
		{"zfs.zvolReadyTimeout", cfg.ZFS.ZvolReadyTimeout},
		{"iscsi.extentBlocksize", cfg.ISCSI.ExtentBlocksize},
		{"iscsi.extentAvailThreshold", cfg.ISCSI.ExtentAvailThreshold},
		{"iscsi.deviceWaitTimeout", cfg.ISCSI.DeviceWaitTimeout},
		{"iscsi.serviceReloadDebounce", cfg.ISCSI.ServiceReloadDebounce},
		{"nvmeof.transportServiceId", cfg.NVMeoF.TransportServiceID},
		{"nvmeof.deviceWaitTimeout", cfg.NVMeoF.DeviceWaitTimeout},
		{"nvmeof.commandTimeout", cfg.NVMeoF.CommandTimeout},
		{"sessionGC.interval", cfg.SessionGC.Interval},
		{"sessionGC.gracePeriod", cfg.SessionGC.GracePeriod},
		{"sessionGC.startupDelay", cfg.SessionGC.StartupDelay},
		{"reconcile.delete.maxPerRun", cfg.Reconcile.Delete.MaxPerRun},
		{"node.sessionCleanupDelay", cfg.Node.SessionCleanupDelay},
		{"resilience.circuitBreaker.failureThreshold", cfg.Resilience.CircuitBreaker.FailureThreshold},
		{"resilience.circuitBreaker.successThreshold", cfg.Resilience.CircuitBreaker.SuccessThreshold},
		{"resilience.circuitBreaker.timeout", cfg.Resilience.CircuitBreaker.Timeout},
		{"resilience.circuitBreaker.halfOpenMaxRequests", cfg.Resilience.CircuitBreaker.HalfOpenMaxRequests},
		{"resilience.retry.maxAttempts", cfg.Resilience.Retry.MaxAttempts},
		{"resilience.retry.initialDelay", cfg.Resilience.Retry.InitialDelay},
		{"resilience.retry.maxDelay", cfg.Resilience.Retry.MaxDelay},
		{"resilience.rateLimiting.maxConcurrentRequests", cfg.Resilience.RateLimiting.MaxConcurrentRequests},
		{"resilience.rateLimiting.maxConcurrentLogins", cfg.Resilience.RateLimiting.MaxConcurrentLogins},
		{"resilience.rateLimiting.discoveryCacheDuration", cfg.Resilience.RateLimiting.DiscoveryCacheDuration},
		{"commandTimeouts.mount", cfg.CommandTimeouts.Mount},
		{"commandTimeouts.format", cfg.CommandTimeouts.Format},
		{"commandTimeouts.iscsi", cfg.CommandTimeouts.ISCSI},
		{"commandTimeouts.nvme", cfg.CommandTimeouts.NVMe},
	}
	for _, field := range fields {
		if field.value < 0 {
			return fmt.Errorf("%s must not be negative", field.name)
		}
	}
	if cfg.Node.MaxVolumesPerNode < 0 {
		return fmt.Errorf("node.maxVolumesPerNode must not be negative")
	}
	if cfg.Resilience.Retry.BackoffMultiplier < 0 {
		return fmt.Errorf("resilience.retry.backoffMultiplier must not be negative")
	}
	return nil
}

// GetDriverShareType returns the share type based on driver name.
//
// Deprecated: Use GetShareType with StorageClass parameters instead.
func (c *Config) GetDriverShareType() ShareType {
	switch c.DriverName {
	case "org.scale.csi.nfs", "truenas-nfs":
		return ShareTypeNFS
	case "org.scale.csi.iscsi", "truenas-iscsi":
		return ShareTypeISCSI
	case "org.scale.csi.nvmeof", "truenas-nvmeof":
		return ShareTypeNVMeoF
	default:
		// Default to NFS if not specified
		return ShareTypeNFS
	}
}

// GetShareType returns the share type from StorageClass parameters. A
// single-protocol instance falls back to its sole enabled protocol; legacy
// configurations without enabled markers fall back to the driver name.
// The "protocol" parameter in StorageClass takes precedence.
func (c *Config) GetShareType(params map[string]string) ShareType {
	// Check StorageClass parameter first
	if params != nil {
		if protocol, ok := params["protocol"]; ok {
			return ParseShareType(protocol)
		}
	}
	if c.enabledShareTypeCount() == 1 {
		switch {
		case c.NFS.Enabled:
			return ShareTypeNFS
		case c.ISCSI.Enabled:
			return ShareTypeISCSI
		case c.NVMeoF.Enabled:
			return ShareTypeNVMeoF
		}
	}
	// Fall back to driver-name detection for old configs without enabled fields.
	return c.GetDriverShareType()
}

// enabledShareTypeCount reports how many protocols this driver instance serves.
// A unified, multi-protocol instance cannot safely infer a StorageClass's intent.
func (c *Config) enabledShareTypeCount() int {
	count := 0
	if c.NFS.Enabled {
		count++
	}
	if c.ISCSI.Enabled {
		count++
	}
	if c.NVMeoF.Enabled {
		count++
	}
	return count
}

// isShareTypeEnabled reports whether the given protocol is enabled on this
// driver instance.
func (c *Config) isShareTypeEnabled(shareType ShareType) bool {
	switch shareType {
	case ShareTypeNFS:
		return c.NFS.Enabled
	case ShareTypeISCSI:
		return c.ISCSI.Enabled
	case ShareTypeNVMeoF:
		return c.NVMeoF.Enabled
	default:
		return false
	}
}

// enabledShareTypeStrings returns the enabled protocol names in stable order.
// An empty result means the configuration carries no enabled markers (legacy),
// so callers must not treat it as "nothing is allowed".
func (c *Config) enabledShareTypeStrings() []string {
	enabled := make([]string, 0, 3)
	if c.NFS.Enabled {
		enabled = append(enabled, string(ShareTypeNFS))
	}
	if c.ISCSI.Enabled {
		enabled = append(enabled, string(ShareTypeISCSI))
	}
	if c.NVMeoF.Enabled {
		enabled = append(enabled, string(ShareTypeNVMeoF))
	}
	return enabled
}

// GetZFSResourceType returns the ZFS resource type for this driver.
func (c *Config) GetZFSResourceType() string {
	return c.GetDriverShareType().ZFSResourceType()
}

// GetZFSResourceTypeForShare returns the ZFS resource type for a given share type.
func (c *Config) GetZFSResourceTypeForShare(shareType ShareType) string {
	return shareType.ZFSResourceType()
}
