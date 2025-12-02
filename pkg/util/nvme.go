// Package util provides utility functions for NVMe-oF operations.
package util

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// nvmeCommandTimeout is the default timeout for nvme CLI commands.
const nvmeCommandTimeout = 30 * time.Second

// NVMeSubsystem represents an NVMe subsystem.
type NVMeSubsystem struct {
	NQN        string          `json:"NQN"`
	Name       string          `json:"Name"`
	Paths      []NVMePath      `json:"Paths"`
	Namespaces []NVMeNamespace `json:"Namespaces"`
}

// NVMePath represents a path to an NVMe subsystem.
type NVMePath struct {
	Name      string `json:"Name"`
	Transport string `json:"Transport"`
	Address   string `json:"Address"`
	State     string `json:"State"`
}

// NVMeNamespace represents an NVMe namespace.
type NVMeNamespace struct {
	NameSpace    int   `json:"NameSpace"`
	NSID         int   `json:"NSID"`
	UsedBytes    int64 `json:"UsedBytes"`
	MaximumLBA   int64 `json:"MaximumLBA"`
	PhysicalSize int64 `json:"PhysicalSize"`
	SectorSize   int   `json:"SectorSize"`
}

// DefaultNVMeoFDeviceTimeout is the default timeout for waiting for NVMe-oF devices to appear.
const DefaultNVMeoFDeviceTimeout = 60 * time.Second

// NVMeoFConnectOptions holds options for NVMe-oF connection.
type NVMeoFConnectOptions struct {
	DeviceTimeout time.Duration // Timeout for waiting for device to appear (default: 60s)
}

// NVMeoFConnect connects to an NVMe-oF target and returns the device path.
func NVMeoFConnect(nqn, transportURI string) (string, error) {
	return NVMeoFConnectWithOptions(nqn, transportURI, nil)
}

// NVMeoFConnectWithOptions connects to an NVMe-oF target with configurable options.
func NVMeoFConnectWithOptions(nqn, transportURI string, opts *NVMeoFConnectOptions) (string, error) {
	klog.V(4).Infof("NVMeoFConnect: nqn=%s, transportURI=%s", nqn, transportURI)

	// Apply defaults
	timeout := DefaultNVMeoFDeviceTimeout
	if opts != nil && opts.DeviceTimeout > 0 {
		timeout = opts.DeviceTimeout
	}

	// Create a context with overall timeout for the connect operation
	ctx, cancel := context.WithTimeout(context.Background(), timeout+nvmeCommandTimeout)
	defer cancel()

	// Parse the transport URI
	// Format: tcp://host:port or rdma://host:port
	transport, host, port, err := parseTransportURI(transportURI)
	if err != nil {
		return "", fmt.Errorf("invalid transport URI: %w", err)
	}

	// Connect to the subsystem
	if err := nvmeConnect(ctx, transport, host, port, nqn); err != nil {
		return "", fmt.Errorf("connect failed: %w", err)
	}

	// Wait for device to appear with configurable timeout
	devicePath, err := waitForNVMeDevice(ctx, nqn, timeout)
	if err != nil {
		return "", fmt.Errorf("device not found: %w", err)
	}

	return devicePath, nil
}

// NVMeoFDisconnect disconnects from an NVMe-oF target with retry for transient failures.
func NVMeoFDisconnect(nqn string) error {
	return NVMeoFDisconnectWithContext(context.Background(), nqn)
}

// NVMeoFDisconnectWithContext disconnects from an NVMe-oF target with context and retry.
func NVMeoFDisconnectWithContext(ctx context.Context, nqn string) error {
	klog.V(4).Infof("NVMeoFDisconnect: nqn=%s", nqn)

	retryCfg := DisconnectRetryConfig()

	return RetryWithBackoff(ctx, "NVMe-oF disconnect "+nqn, retryCfg, func() error {
		cmdCtx, cancel := context.WithTimeout(ctx, nvmeCommandTimeout)
		defer cancel()

		cmd := exec.CommandContext(cmdCtx, "nvme", "disconnect", "-n", nqn)
		output, err := cmd.CombinedOutput()
		if err != nil {
			// Check if already disconnected - treat as success
			if strings.Contains(string(output), "not found") ||
				strings.Contains(string(output), "No subsystems") {
				klog.V(4).Infof("Subsystem already disconnected: %s", nqn)
				return nil
			}
			// Return error to potentially trigger retry
			return fmt.Errorf("disconnect failed: %v, output: %s", err, string(output))
		}
		return nil
	})
}

// validNVMeoFTransports are the supported NVMe-oF transport types.
var validNVMeoFTransports = map[string]bool{
	"tcp":  true,
	"rdma": true,
	"fc":   true,
}

// parseTransportURI parses a transport URI into its components.
func parseTransportURI(transportURI string) (transport, host, port string, err error) {
	u, err := url.Parse(transportURI)
	if err != nil {
		return "", "", "", err
	}

	transport = u.Scheme
	if transport == "" {
		transport = "tcp"
	}

	// Validate transport type
	if !validNVMeoFTransports[transport] {
		return "", "", "", fmt.Errorf("unsupported NVMe-oF transport: %s (supported: tcp, rdma, fc)", transport)
	}

	host = u.Hostname()
	if host == "" {
		return "", "", "", fmt.Errorf("missing host in URI")
	}

	port = u.Port()
	if port == "" {
		port = "4420" // Default NVMe-oF port
	}

	return transport, host, port, nil
}

// nvmeConnect connects to an NVMe-oF subsystem.
func nvmeConnect(ctx context.Context, transport, host, port, nqn string) error {
	// Check if already connected
	subsystems, err := listNVMeSubsystems(ctx)
	if err != nil {
		klog.Warningf("Failed to list NVMe subsystems: %v", err)
	} else {
		for _, subsys := range subsystems {
			if subsys.NQN == nqn {
				klog.V(4).Infof("Already connected to subsystem: %s", nqn)
				return nil
			}
		}
	}

	// Build connect command
	args := []string{
		"connect",
		"-t", transport,
		"-n", nqn,
		"-a", host,
		"-s", port,
	}

	cmd := exec.CommandContext(ctx, "nvme", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already connected
		if strings.Contains(string(output), "already connected") {
			klog.V(4).Infof("Subsystem already connected: %s", nqn)
			return nil
		}
		return fmt.Errorf("connect command failed: %v, output: %s", err, string(output))
	}

	klog.V(4).Infof("Connect output: %s", string(output))
	return nil
}

// listNVMeSubsystems returns the list of connected NVMe subsystems.
func listNVMeSubsystems(ctx context.Context) ([]NVMeSubsystem, error) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list-subsys failed: %v", err)
	}

	// Parse JSON output
	var result struct {
		Subsystems []NVMeSubsystem `json:"Subsystems"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		// Try alternative format
		var subsystems []NVMeSubsystem
		if err := json.Unmarshal(output, &subsystems); err != nil {
			return nil, fmt.Errorf("failed to parse subsystem list: %v", err)
		}
		return subsystems, nil
	}

	return result.Subsystems, nil
}

// waitForNVMeDevice waits for the NVMe device to appear.
// Uses exponential backoff starting at 50ms, maxing at 500ms for faster detection.
func waitForNVMeDevice(ctx context.Context, nqn string, timeout time.Duration) (string, error) {
	start := time.Now()
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 500 * time.Millisecond

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context cancelled waiting for device (nqn=%s): %w", nqn, ctx.Err())
		default:
		}

		// Check timeout
		if time.Since(start) > timeout {
			return "", fmt.Errorf("timeout waiting for device (nqn=%s)", nqn)
		}

		devicePath, err := findNVMeDevice(nqn)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}

		time.Sleep(pollInterval)
		// Exponential backoff: 50ms -> 100ms -> 200ms -> 400ms -> 500ms (max)
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

// findNVMeDevice finds the device path for an NVMe subsystem.
// Variable for testability.
var findNVMeDevice = func(nqn string) (string, error) {
	// Look in /sys/class/nvme-subsystem
	subsysDirs, err := filepath.Glob("/sys/class/nvme-subsystem/nvme-subsys*")
	if err != nil {
		return "", err
	}

	for _, subsysDir := range subsysDirs {
		// Read the subsysnqn file
		nqnPath := filepath.Join(subsysDir, "subsysnqn")
		nqnBytes, err := os.ReadFile(nqnPath)
		if err != nil {
			continue
		}
		subsysNQN := strings.TrimSpace(string(nqnBytes))

		if subsysNQN != nqn {
			continue
		}

		// Found the subsystem, now find the namespace device
		// Look for nvmeXnY devices
		nvmeDevices, err := filepath.Glob(filepath.Join(subsysDir, "nvme*/nvme*n*"))
		if err != nil {
			continue
		}

		if len(nvmeDevices) > 0 {
			// Get the device name
			deviceName := filepath.Base(nvmeDevices[0])
			devicePath := "/dev/" + deviceName
			if _, err := os.Stat(devicePath); err == nil {
				return devicePath, nil
			}
		}
	}

	// Alternative: use nvme list
	return findNVMeDeviceFromList(nqn)
}

// findNVMeDeviceFromList finds NVMe device using nvme list command.
func findNVMeDeviceFromList(nqn string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "list", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("nvme list failed: %v", err)
	}

	var result struct {
		Devices []struct {
			DevicePath   string `json:"DevicePath"`
			SubsystemNQN string `json:"SubsystemNQN"`
		} `json:"Devices"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return "", fmt.Errorf("failed to parse nvme list: %v", err)
	}

	for _, device := range result.Devices {
		if device.SubsystemNQN == nqn {
			return device.DevicePath, nil
		}
	}

	return "", fmt.Errorf("device not found for nqn=%s", nqn)
}

// GetNVMeDevicePath returns the device path for an NVMe subsystem NQN.
func GetNVMeDevicePath(nqn string) (string, error) {
	return findNVMeDevice(nqn)
}

// NVMeRescan rescans for new NVMe namespaces.
func NVMeRescan() error {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "ns-rescan", "/dev/nvme0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Warningf("NVMe rescan failed: %v, output: %s", err, string(output))
		// Not critical, continue
	}
	return nil
}

// NVMeGetNamespaceInfo returns information about an NVMe namespace.
func NVMeGetNamespaceInfo(devicePath string) (*NVMeNamespace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "id-ns", devicePath, "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("id-ns failed: %v", err)
	}

	var ns NVMeNamespace
	if err := json.Unmarshal(output, &ns); err != nil {
		return nil, fmt.Errorf("failed to parse namespace info: %v", err)
	}

	return &ns, nil
}

// NVMeGetSubsystemInfo returns information about an NVMe subsystem.
func NVMeGetSubsystemInfo(nqn string) (*NVMeSubsystem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	subsystems, err := listNVMeSubsystems(ctx)
	if err != nil {
		return nil, err
	}

	for _, subsys := range subsystems {
		if subsys.NQN == nqn {
			return &subsys, nil
		}
	}

	return nil, fmt.Errorf("subsystem not found: %s", nqn)
}

// NVMeListNamespaces lists all namespaces for a device.
func NVMeListNamespaces(devicePath string) ([]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	// Remove namespace suffix if present (e.g., /dev/nvme0n1 -> /dev/nvme0)
	ctrlPath := devicePath
	if strings.Contains(filepath.Base(devicePath), "n") {
		// Extract controller path
		parts := strings.Split(filepath.Base(devicePath), "n")
		if len(parts) >= 2 {
			ctrlPath = filepath.Join(filepath.Dir(devicePath), parts[0])
		}
	}

	cmd := exec.CommandContext(ctx, "nvme", "list-ns", ctrlPath, "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list-ns failed: %v", err)
	}

	var nsids []int
	if err := json.Unmarshal(output, &nsids); err != nil {
		// Try alternative format
		var result struct {
			Namespaces []int `json:"namespaces"`
		}
		if err := json.Unmarshal(output, &result); err != nil {
			return nil, fmt.Errorf("failed to parse namespace list: %v", err)
		}
		return result.Namespaces, nil
	}

	return nsids, nil
}

// NVMeFlush flushes data to the NVMe device.
func NVMeFlush(devicePath string, nsid int) error {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "flush", devicePath, "-n", fmt.Sprintf("%d", nsid))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("flush failed: %v, output: %s", err, string(output))
	}
	return nil
}

// IsNVMeFabric checks if a device is an NVMe-oF (fabric) device.
func IsNVMeFabric(devicePath string) (bool, error) {
	// Get the device name without /dev/
	deviceName := filepath.Base(devicePath)

	// Check if transport is fabrics
	transportPath := fmt.Sprintf("/sys/block/%s/device/transport", deviceName)
	transport, err := os.ReadFile(transportPath)
	if err != nil {
		// Try alternative path
		parts := strings.Split(deviceName, "n")
		if len(parts) >= 2 {
			ctrlName := parts[0]
			transportPath = fmt.Sprintf("/sys/class/nvme/%s/transport", ctrlName)
			transport, err = os.ReadFile(transportPath)
			if err != nil {
				return false, fmt.Errorf("failed to read transport: %v", err)
			}
		} else {
			return false, fmt.Errorf("failed to read transport: %v", err)
		}
	}

	transportStr := strings.TrimSpace(string(transport))
	// Fabric transports: tcp, rdma, fc
	return transportStr == "tcp" || transportStr == "rdma" || transportStr == "fc", nil
}

// NVMeDiscovery performs NVMe-oF discovery.
func NVMeDiscovery(transport, host, port string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	args := []string{
		"discover",
		"-t", transport,
		"-a", host,
		"-s", port,
		"-o", "json",
	}

	cmd := exec.CommandContext(ctx, "nvme", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("discovery failed: %v", err)
	}

	var result struct {
		Records []struct {
			SubNQN string `json:"subnqn"`
		} `json:"records"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("failed to parse discovery response: %v", err)
	}

	var nqns []string
	for _, record := range result.Records {
		nqns = append(nqns, record.SubNQN)
	}

	return nqns, nil
}

// GetNVMeInfoFromDevice returns the NQN for a given device path.
func GetNVMeInfoFromDevice(devicePath string) (string, error) {
	deviceName := filepath.Base(devicePath)

	// Check if it's an NVMe device
	if !strings.HasPrefix(deviceName, "nvme") {
		return "", fmt.Errorf("not an NVMe device: %s", devicePath)
	}

	// Find subsystem NQN
	// nvme0n1 -> nvme0
	parts := strings.Split(deviceName, "n")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid NVMe device name: %s", deviceName)
	}
	ctrlName := parts[0]

	// Read subsysnqn from controller
	// /sys/class/nvme/nvme0/subsysnqn
	nqnPath := filepath.Join("/sys/class/nvme", ctrlName, "subsysnqn")
	content, err := os.ReadFile(nqnPath)
	if err == nil {
		return strings.TrimSpace(string(content)), nil
	}

	// Try via subsystem link
	// /sys/class/nvme/nvme0/subsystem/subsysnqn
	nqnPath = filepath.Join("/sys/class/nvme", ctrlName, "subsystem", "subsysnqn")
	content, err = os.ReadFile(nqnPath)
	if err == nil {
		return strings.TrimSpace(string(content)), nil
	}

	return "", fmt.Errorf("could not find NQN for device %s", devicePath)
}

// FindNVMeoFSessionBySubsysName searches connected NVMe subsystems for one matching the subsystem name.
// The subsystem name is the part that should appear in the NQN (with any prefix/suffix already applied).
// This is used for cleanup when the device path is unavailable (e.g., after node restart).
func FindNVMeoFSessionBySubsysName(subsysName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	subsystems, err := listNVMeSubsystems(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list NVMe subsystems: %w", err)
	}

	// Search for a subsystem whose NQN contains the subsystem name
	// NQN format can vary: nqn.2014-08.org.nvmexpress:uuid:xxx or containing the volume name
	for _, subsystem := range subsystems {
		if strings.Contains(subsystem.NQN, subsysName) {
			klog.V(4).Infof("Found NVMe-oF subsystem for name %s: NQN=%s", subsysName, subsystem.NQN)
			return subsystem.NQN, nil
		}
	}

	return "", fmt.Errorf("no NVMe-oF subsystem found for name %s", subsysName)
}

// FindNVMeoFSessionByVolumeID is a convenience wrapper that searches by volumeID.
// Deprecated: Use FindNVMeoFSessionBySubsysName instead, which handles NamePrefix/NameSuffix correctly.
func FindNVMeoFSessionByVolumeID(volumeID string) (string, error) {
	return FindNVMeoFSessionBySubsysName(volumeID)
}

// NVMeoFSessionInfo holds information about an active NVMe-oF session.
// Used by session GC to identify orphaned sessions.
type NVMeoFSessionInfo struct {
	NQN       string
	Address   string
	Transport string
}

// ListNVMeoFSessions returns all active NVMe-oF sessions.
// This is used by the session GC to identify orphaned sessions.
func ListNVMeoFSessions() ([]NVMeoFSessionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	subsystems, err := listNVMeSubsystems(ctx)
	if err != nil {
		return nil, err
	}

	var sessions []NVMeoFSessionInfo
	for _, subsys := range subsystems {
		// Get the address from the first path (if available)
		address := ""
		transport := ""
		if len(subsys.Paths) > 0 {
			address = subsys.Paths[0].Address
			transport = subsys.Paths[0].Transport
		}
		sessions = append(sessions, NVMeoFSessionInfo{
			NQN:       subsys.NQN,
			Address:   address,
			Transport: transport,
		})
	}

	return sessions, nil
}

// FindNVMeoFSessionByNQN searches connected NVMe subsystems for one matching the exact NQN.
// This is used for pre-emptive cleanup before staging a volume.
func FindNVMeoFSessionByNQN(nqn string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCommandTimeout)
	defer cancel()

	subsystems, err := listNVMeSubsystems(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list NVMe subsystems: %w", err)
	}

	for _, subsystem := range subsystems {
		if subsystem.NQN == nqn {
			klog.V(4).Infof("Found NVMe-oF session for NQN %s", nqn)
			return subsystem.NQN, nil
		}
	}

	return "", fmt.Errorf("no NVMe-oF subsystem found for NQN %s", nqn)
}
