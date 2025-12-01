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
	// (OTHER-001 fix: make NVMe-oF timeout configurable like iSCSI)
	DeviceWaitTimeout int `yaml:"deviceWaitTimeout"`
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
	if cfg.NVMeoF.Transport == "" {
		cfg.NVMeoF.Transport = "tcp"
	}
	if cfg.NVMeoF.TransportServiceID == 0 {
		cfg.NVMeoF.TransportServiceID = 4420
	}
	if cfg.NVMeoF.DeviceWaitTimeout == 0 {
		cfg.NVMeoF.DeviceWaitTimeout = 60 // Default 60 seconds (OTHER-001 fix)
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
func (c *Config) GetDriverShareType() string {
	switch c.DriverName {
	case "org.truenas.csi.nfs", "truenas-nfs":
		return "nfs"
	case "org.truenas.csi.iscsi", "truenas-iscsi":
		return "iscsi"
	case "org.truenas.csi.nvmeof", "truenas-nvmeof":
		return "nvmeof"
	default:
		// Default to NFS if not specified
		return "nfs"
	}
}

// GetShareType returns the share type from StorageClass parameters,
// falling back to driver name if not specified in parameters.
// The "protocol" parameter in StorageClass takes precedence.
func (c *Config) GetShareType(params map[string]string) string {
	// Check StorageClass parameter first
	if params != nil {
		if protocol, ok := params["protocol"]; ok {
			switch protocol {
			case "nfs":
				return "nfs"
			case "iscsi":
				return "iscsi"
			case "nvmeof":
				return "nvmeof"
			}
		}
	}
	// Fall back to driver name-based detection
	return c.GetDriverShareType()
}

// GetZFSResourceType returns the ZFS resource type for this driver.
func (c *Config) GetZFSResourceType() string {
	return c.GetZFSResourceTypeForShare(c.GetDriverShareType())
}

// GetZFSResourceTypeForShare returns the ZFS resource type for a given share type.
func (c *Config) GetZFSResourceTypeForShare(shareType string) string {
	switch shareType {
	case "nfs":
		return "filesystem"
	case "iscsi", "nvmeof":
		return "volume"
	default:
		return "filesystem"
	}
}
