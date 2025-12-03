// Package driver implements the CSI driver for TrueNAS Scale.
package driver

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the driver configuration loaded from YAML.
type Config struct {
	// DriverName is the CSI driver name for registration
	DriverName string `yaml:"driver"`

	// InstanceID is a unique identifier for this driver instance
	InstanceID string `yaml:"instance_id"`

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

	// Node configuration (node plugin only)
	Node NodeConfig `yaml:"node"`

	// Resilience configuration for retry, circuit breaker, and rate limiting
	Resilience ResilienceConfig `yaml:"resilience"`

	// CommandTimeouts configures timeouts for various command types
	CommandTimeouts CommandTimeoutConfig `yaml:"commandTimeouts"`
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

	// RequestTimeout is the timeout for API requests in seconds (default: 60)
	RequestTimeout int `yaml:"requestTimeout"`

	// ConnectTimeout is the timeout for establishing connections in seconds (default: 10)
	ConnectTimeout int `yaml:"connectTimeout"`

	// MaxConcurrentRequests limits concurrent API requests to prevent overwhelming TrueNAS (default: 10)
	MaxConcurrentRequests int `yaml:"maxConcurrentRequests"`
}

// ZFSConfig holds ZFS dataset configuration.
type ZFSConfig struct {
	// DatasetParentName is the parent dataset for volumes (e.g., "tank/k8s/volumes")
	DatasetParentName string `yaml:"datasetParentName"`

	// DetachedSnapshotsDatasetParentName is the parent for detached snapshots
	DetachedSnapshotsDatasetParentName string `yaml:"detachedSnapshotsDatasetParentName"`

	// DatasetEnableQuotas enables quota support for NFS volumes
	DatasetEnableQuotas bool `yaml:"datasetEnableQuotas"`

	// DatasetEnableReservation enables reservation support
	DatasetEnableReservation bool `yaml:"datasetEnableReservation"`

	// DatasetProperties are additional ZFS properties to set on datasets
	DatasetProperties map[string]string `yaml:"datasetProperties"`

	// ZvolBlocksize is the block size for zvols (default: 16K)
	ZvolBlocksize string `yaml:"zvolBlocksize"`

	// ZvolReadyTimeout is the timeout in seconds for waiting for a zvol to be ready
	// after cloning operations. Increase for slow systems or large clones (default: 60)
	ZvolReadyTimeout int `yaml:"zvolReadyTimeout"`
}

// NFSConfig holds NFS share configuration.
type NFSConfig struct {
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
	// TargetPortal is the iSCSI target portal (host:port)
	TargetPortal string `yaml:"targetPortal"`

	// TargetPortals is a list of additional portals for multipath
	TargetPortals []string `yaml:"targetPortals"`

	// Interface is the iSCSI interface to use (default: "default")
	Interface string `yaml:"interface"`

	// NamePrefix is a prefix for iSCSI target/extent names
	NamePrefix string `yaml:"namePrefix"`

	// NameSuffix is a suffix for iSCSI target/extent names
	NameSuffix string `yaml:"nameSuffix"`

	// NameTemplate is a template for generating names
	NameTemplate string `yaml:"nameTemplate"`

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

// NVMeoFConfig holds NVMe-oF configuration.
type NVMeoFConfig struct {
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
	// Enabled enables the circuit breaker (default: true)
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

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables in the config
	data = []byte(os.ExpandEnv(string(data)))

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
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

	// Session GC defaults - enabled by default with 5 minute interval
	// Note: Enabled defaults to false from YAML unmarshaling, we set true explicitly
	// Users can disable by setting sessionGC.enabled: false
	if cfg.SessionGC.Interval == 0 {
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
	if cfg.TrueNAS.APIKey == "" {
		return nil, fmt.Errorf("truenas.apiKey is required")
	}
	if cfg.ZFS.DatasetParentName == "" {
		return nil, fmt.Errorf("zfs.datasetParentName is required")
	}

	// Validate protocol-specific settings based on driver type
	shareType := cfg.GetDriverShareType()
	switch shareType {
	case "nfs":
		if cfg.NFS.ShareHost == "" {
			return nil, fmt.Errorf("nfs.shareHost is required for NFS driver")
		}
	case "iscsi":
		if cfg.ISCSI.TargetPortal == "" {
			return nil, fmt.Errorf("iscsi.targetPortal is required for iSCSI driver")
		}
	case "nvmeof":
		if cfg.NVMeoF.TransportAddress == "" {
			return nil, fmt.Errorf("nvmeof.transportAddress is required for NVMe-oF driver")
		}
	}

	return cfg, nil
}

// GetDriverShareType returns the share type based on driver name.
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

// GetShareType returns the share type from StorageClass parameters,
// falling back to driver name if not specified in parameters.
// The "protocol" parameter in StorageClass takes precedence.
func (c *Config) GetShareType(params map[string]string) ShareType {
	// Check StorageClass parameter first
	if params != nil {
		if protocol, ok := params["protocol"]; ok {
			return ParseShareType(protocol)
		}
	}
	// Fall back to driver name-based detection
	return c.GetDriverShareType()
}

// GetZFSResourceType returns the ZFS resource type for this driver.
func (c *Config) GetZFSResourceType() string {
	return c.GetDriverShareType().ZFSResourceType()
}

// GetZFSResourceTypeForShare returns the ZFS resource type for a given share type.
func (c *Config) GetZFSResourceTypeForShare(shareType ShareType) string {
	return shareType.ZFSResourceType()
}
