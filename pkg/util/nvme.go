// Package util provides utility functions for NVMe-oF operations.
package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// nvmeDeviceRegex matches NVMe namespace device names and captures the controller name.
// Pattern: nvme<controller_number>n<namespace_number>
// Examples: nvme0n1 -> captures "nvme0", nvme10n2 -> captures "nvme10"
var (
	nvmeControllerRegex       = regexp.MustCompile(`^nvme\d+$`)
	nvmeDeviceRegex           = regexp.MustCompile(`^(nvme\d+)n\d+$`)
	errNVMeNamespaceAmbiguous = errors.New("ambiguous NVMe namespace selection")
)

// Note: getNVMeTimeout() is now configurable via SetConfig().NVMeTimeout
// Default: 30s

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
	return NVMeoFConnectWithOptionsContext(context.Background(), nqn, transportURI, opts)
}

// NVMeoFConnectWithOptionsContext is NVMeoFConnectWithOptions bounded by the
// inbound context's deadline as well as the configured NVMe timeout.
func NVMeoFConnectWithOptionsContext(ctx context.Context, nqn, transportURI string, opts *NVMeoFConnectOptions) (string, error) {
	listCtx, cancel, err := commandContext(ctx, getNVMeTimeout())
	if err != nil {
		return "", err
	}
	subsystems, listErr := ListNVMeSubsystems(listCtx)
	cancel()
	if listErr != nil {
		klog.Warningf("Failed to list NVMe subsystems: %v", listErr)
	}

	return NVMeoFConnectWithOptionsAndSubsystemsContext(ctx, nqn, transportURI, opts, subsystems)
}

// NVMeoFConnectWithOptionsAndSubsystems connects to an NVMe-oF target using a
// pre-fetched subsystem list.
func NVMeoFConnectWithOptionsAndSubsystems(nqn, transportURI string, opts *NVMeoFConnectOptions, subsystems []NVMeSubsystem) (string, error) {
	return NVMeoFConnectWithOptionsAndSubsystemsContext(context.Background(), nqn, transportURI, opts, subsystems)
}

// NVMeoFConnectWithOptionsAndSubsystemsContext connects to an NVMe-oF target
// using a pre-fetched subsystem list, bounded by the inbound context's deadline
// as well as the overall connect timeout.
func NVMeoFConnectWithOptionsAndSubsystemsContext(ctx context.Context, nqn, transportURI string, opts *NVMeoFConnectOptions, subsystems []NVMeSubsystem) (string, error) {
	klog.V(4).Infof("NVMeoFConnect: nqn=%s, transportURI=%s", nqn, transportURI)

	// Apply defaults
	timeout := DefaultNVMeoFDeviceTimeout
	if opts != nil && opts.DeviceTimeout > 0 {
		timeout = opts.DeviceTimeout
	}

	// Create a context with overall timeout for the connect operation
	ctx, cancel, err := commandContext(ctx, timeout+getNVMeTimeout())
	if err != nil {
		return "", err
	}
	defer cancel()

	// Parse the transport URI
	// Format: tcp://host:port or rdma://host:port
	transport, host, port, err := parseTransportURI(transportURI)
	if err != nil {
		return "", fmt.Errorf("invalid transport URI: %w", err)
	}

	wasConnected := hasNVMeSubsystem(nqn, subsystems)

	// Connect to the subsystem
	if connectErr := nvmeConnectWithSubsystems(ctx, transport, host, port, nqn, subsystems); connectErr != nil {
		return "", fmt.Errorf("connect failed: %w", connectErr)
	}

	// Wait for device to appear with configurable timeout
	devicePath, err := waitForNVMeDeviceWithSubsystems(ctx, nqn, timeout, subsystems, !wasConnected)
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
		cmdCtx, cancel := context.WithTimeout(ctx, getNVMeTimeout())
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
			return fmt.Errorf("disconnect failed: %w, output: %s", err, string(output))
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

func nvmeConnectWithSubsystems(ctx context.Context, transport, host, port, nqn string, subsystems []NVMeSubsystem) error {
	// Check if already connected
	if hasNVMeSubsystem(nqn, subsystems) {
		klog.V(4).Infof("Already connected to subsystem: %s", nqn)
		return nil
	}

	// Build connect command with reconnect options for resilience.
	// --reconnect-delay=10: retry connection every 10 seconds on failure
	// --ctrl-loss-tmo=-1: never give up on reconnecting (-1 means infinite)
	// These options ensure NVMe-oF sessions automatically recover from
	// transient network issues or TrueNAS service restarts.
	args := []string{
		"connect",
		"-t", transport,
		"-n", nqn,
		"-a", host,
		"-s", port,
		"--reconnect-delay=10",
		"--ctrl-loss-tmo=-1",
	}

	cmd := exec.CommandContext(ctx, "nvme", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already connected
		if strings.Contains(string(output), "already connected") {
			klog.V(4).Infof("Subsystem already connected: %s", nqn)
			return nil
		}
		return fmt.Errorf("connect command failed: %w, output: %s", err, string(output))
	}

	klog.V(4).Infof("Connect output: %s", string(output))
	return nil
}

func hasNVMeSubsystem(nqn string, subsystems []NVMeSubsystem) bool {
	for _, subsystem := range subsystems {
		if subsystem.NQN == nqn {
			return true
		}
	}
	return false
}

// listNVMeSubsystemsFunc is the function used to list NVMe subsystems.
// Variable for testability.
var listNVMeSubsystemsFunc = listNVMeSubsystems

// ListNVMeSubsystems returns the connected NVMe subsystems.
func ListNVMeSubsystems(ctx context.Context) ([]NVMeSubsystem, error) {
	return listNVMeSubsystemsFunc(ctx)
}

// listNVMeSubsystems returns the list of connected NVMe subsystems.
func listNVMeSubsystems(ctx context.Context) ([]NVMeSubsystem, error) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list-subsys failed: %w", err)
	}

	subsystems, err := parseSubsysJSON(output)
	if err != nil {
		return nil, fmt.Errorf("failed to parse subsystem list: %w", err)
	}
	return subsystems, nil
}

// parseSubsysJSON accepts the nvme-cli 1.x object, nvme-cli 2.x host array,
// and direct subsystem-array shapes. A successful decode that produces an
// empty-NQN entry is a shape mismatch, not a real subsystem.
func parseSubsysJSON(output []byte) ([]NVMeSubsystem, error) {
	var hosts []struct {
		Subsystems []NVMeSubsystem `json:"Subsystems"`
	}
	if err := json.Unmarshal(output, &hosts); err == nil && hosts != nil {
		subsystems := make([]NVMeSubsystem, 0)
		hostShape := len(hosts) == 0
		for _, host := range hosts {
			if host.Subsystems != nil {
				hostShape = true
				subsystems = append(subsystems, host.Subsystems...)
			}
		}
		if hostShape && nvmeSubsystemsHaveNQN(subsystems) {
			return subsystems, nil
		}
	}

	var object struct {
		Subsystems []NVMeSubsystem `json:"Subsystems"`
	}
	if err := json.Unmarshal(output, &object); err == nil && object.Subsystems != nil && nvmeSubsystemsHaveNQN(object.Subsystems) {
		return object.Subsystems, nil
	}

	var subsystems []NVMeSubsystem
	if err := json.Unmarshal(output, &subsystems); err == nil && subsystems != nil && nvmeSubsystemsHaveNQN(subsystems) {
		return subsystems, nil
	}

	return nil, fmt.Errorf("unrecognized nvme list-subsys JSON shape")
}

func nvmeSubsystemsHaveNQN(subsystems []NVMeSubsystem) bool {
	for _, subsystem := range subsystems {
		if strings.TrimSpace(subsystem.NQN) == "" {
			return false
		}
	}
	return true
}

// waitForNVMeDevice waits for the NVMe device to appear.
// Uses exponential backoff starting at 50ms, maxing at 100ms for faster detection.
func waitForNVMeDevice(ctx context.Context, nqn string, timeout time.Duration) (string, error) {
	start := time.Now()
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 100 * time.Millisecond

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context canceled waiting for device (nqn=%s): %w", nqn, ctx.Err())
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
		// Exponential backoff: 50ms -> 100ms (max)
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

func waitForNVMeDeviceWithSubsystems(ctx context.Context, nqn string, timeout time.Duration, subsystems []NVMeSubsystem, refreshAfterConnect bool) (string, error) {
	start := time.Now()
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 100 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context canceled waiting for device (nqn=%s): %w", nqn, ctx.Err())
		default:
		}

		if time.Since(start) > timeout {
			return "", fmt.Errorf("timeout waiting for device (nqn=%s)", nqn)
		}

		devicePath, err := findNVMeDeviceFromSysfs(nqn)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}
		if refreshAfterConnect {
			refreshed, listErr := ListNVMeSubsystems(ctx)
			if listErr == nil {
				subsystems = refreshed
			}
			refreshAfterConnect = false
		}
		devicePath, err = findNVMeDeviceFromSubsystems(nqn, subsystems)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}

		time.Sleep(pollInterval)
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

// findNVMeDevice finds the device path for an NVMe subsystem.
// Variable for testability.
var findNVMeDevice = findNVMeDeviceFresh

func findNVMeDeviceFresh(nqn string) (string, error) {
	devicePath, err := findNVMeDeviceFromSysfs(nqn)
	if err == nil && devicePath != "" {
		return devicePath, nil
	}
	if errors.Is(err, errNVMeNamespaceAmbiguous) {
		return "", err
	}

	// Alternative: use nvme list-subsys to derive device from controller name
	return findNVMeDeviceFromListSubsys(nqn)
}

func findNVMeDeviceFromSysfs(nqn string) (string, error) {
	return findNVMeDeviceFromSysfsInPaths(nqn, "/sys/class/nvme-subsystem", "/dev")
}

func findNVMeDeviceFromSysfsInPaths(nqn, subsystemClassRoot, devRoot string) (string, error) {
	// Look in /sys/class/nvme-subsystem
	subsysDirs, err := filepath.Glob(filepath.Join(subsystemClassRoot, "nvme-subsys*"))
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

		// A subsystem may expose namespaces directly (common for fabrics) or
		// below controller directories (common for PCIe). Without an expected
		// NSID, selecting among multiple namespaces is unsafe.
		namespaceNames := make(map[string]struct{})
		patterns := []string{
			filepath.Join(subsysDir, "nvme*n*"),
			filepath.Join(subsysDir, "nvme*", "nvme*n*"),
		}
		for _, pattern := range patterns {
			nvmeDevices, _ := filepath.Glob(pattern)
			for _, devPath := range nvmeDevices {
				deviceName := filepath.Base(devPath)
				if nvmeDeviceRegex.MatchString(deviceName) {
					namespaceNames[deviceName] = struct{}{}
				}
			}
		}

		names := make([]string, 0, len(namespaceNames))
		for name := range namespaceNames {
			names = append(names, name)
		}
		sort.Strings(names)
		if len(names) > 1 {
			return "", fmt.Errorf("%w: NVMe subsystem %s exposes multiple namespaces (%s)", errNVMeNamespaceAmbiguous, nqn, strings.Join(names, ", "))
		}
		if len(names) == 1 {
			devicePath := filepath.Join(devRoot, names[0])
			if _, err := os.Stat(devicePath); err == nil {
				return devicePath, nil
			}
		}
	}

	return "", fmt.Errorf("device not found for nqn=%s", nqn)
}

// findNVMeDeviceFromListSubsys finds NVMe device using nvme list-subsys command.
// This is more reliable for NVMe-oF as it provides the NQN directly.
func findNVMeDeviceFromListSubsys(nqn string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("nvme list-subsys failed: %w", err)
	}

	subsystems, err := parseSubsysJSON(output)
	if err != nil {
		return "", fmt.Errorf("failed to parse nvme list-subsys: %w", err)
	}

	return findNVMeDeviceFromSubsystems(nqn, subsystems)
}

func findNVMeDeviceFromSubsystems(nqn string, subsystems []NVMeSubsystem) (string, error) {
	for _, subsys := range subsystems {
		if subsys.NQN == nqn {
			// Paths[].Name identifies the controller. Enumerate its namespaces;
			// namespace IDs are not guaranteed to start at 1.
			for _, path := range subsys.Paths {
				if path.Name != "" {
					devicePath, findErr := findNVMeNamespaceForController(path.Name, "/sys/class/nvme", "/dev")
					if findErr == nil {
						return devicePath, nil
					}
					if errors.Is(findErr, errNVMeNamespaceAmbiguous) {
						return "", findErr
					}
				}
			}
		}
	}

	return "", fmt.Errorf("device not found for nqn=%s", nqn)
}

func findNVMeNamespaceForController(controller, nvmeClassRoot, devRoot string) (string, error) {
	if !nvmeControllerRegex.MatchString(controller) {
		return "", fmt.Errorf("invalid NVMe controller name: %s", controller)
	}

	pattern := filepath.Join(nvmeClassRoot, controller, controller+"n*")
	namespaces, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}
	deviceNames := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		deviceName := filepath.Base(namespace)
		if !nvmeDeviceRegex.MatchString(deviceName) {
			continue
		}
		deviceNames = append(deviceNames, deviceName)
	}
	sort.Strings(deviceNames)
	if len(deviceNames) > 1 {
		return "", fmt.Errorf("%w: NVMe controller %s exposes multiple namespaces (%s)", errNVMeNamespaceAmbiguous, controller, strings.Join(deviceNames, ", "))
	}
	if len(deviceNames) == 1 {
		devicePath := filepath.Join(devRoot, deviceNames[0])
		if _, statErr := os.Stat(devicePath); statErr == nil {
			return devicePath, nil
		}
	}

	return "", fmt.Errorf("namespace device not found for controller %s", controller)
}

// GetNVMeDevicePath returns the device path for an NVMe subsystem NQN.
func GetNVMeDevicePath(nqn string) (string, error) {
	return findNVMeDevice(nqn)
}

// NVMeRescan rescans the controller that owns a namespace device.
func NVMeRescan(devicePath string) error {
	return NVMeRescanWithContext(context.Background(), devicePath)
}

// NVMeRescanWithContext is NVMeRescan bounded by the inbound context's deadline
// as well as the configured NVMe timeout.
func NVMeRescanWithContext(ctx context.Context, devicePath string) error {
	deviceName := filepath.Base(devicePath)
	matches := nvmeDeviceRegex.FindStringSubmatch(deviceName)
	if len(matches) != 2 {
		return fmt.Errorf("invalid NVMe namespace device: %s", devicePath)
	}
	controllerPath := "/dev/" + matches[1]

	ctx, cancel, err := commandContext(ctx, getNVMeTimeout())
	if err != nil {
		return err
	}
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "ns-rescan", controllerPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("NVMe rescan failed: %w, output: %s", err, string(output))
	}
	return nil
}

// NVMeGetNamespaceInfo returns information about an NVMe namespace.
func NVMeGetNamespaceInfo(devicePath string) (*NVMeNamespace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "id-ns", devicePath, "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("id-ns failed: %w", err)
	}

	var ns NVMeNamespace
	if err := json.Unmarshal(output, &ns); err != nil {
		return nil, fmt.Errorf("failed to parse namespace info: %w", err)
	}

	return &ns, nil
}

// NVMeGetSubsystemInfo returns information about an NVMe subsystem.
func NVMeGetSubsystemInfo(nqn string) (*NVMeSubsystem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
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
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	// Remove namespace suffix if present (e.g., /dev/nvme0n1 -> /dev/nvme0)
	ctrlPath := devicePath
	deviceName := filepath.Base(devicePath)
	if matches := nvmeDeviceRegex.FindStringSubmatch(deviceName); len(matches) == 2 {
		// matches[0] is full match (e.g., "nvme0n1"), matches[1] is controller (e.g., "nvme0")
		ctrlPath = filepath.Join(filepath.Dir(devicePath), matches[1])
	}

	cmd := exec.CommandContext(ctx, "nvme", "list-ns", ctrlPath, "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list-ns failed: %w", err)
	}

	var nsids []int
	if err := json.Unmarshal(output, &nsids); err != nil {
		// Try alternative format
		var result struct {
			Namespaces []int `json:"namespaces"`
		}
		if err := json.Unmarshal(output, &result); err != nil {
			return nil, fmt.Errorf("failed to parse namespace list: %w", err)
		}
		return result.Namespaces, nil
	}

	return nsids, nil
}

// NVMeFlush flushes data to the NVMe device.
func NVMeFlush(devicePath string, nsid int) error {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvme", "flush", devicePath, "-n", fmt.Sprintf("%d", nsid))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("flush failed: %w, output: %s", err, string(output))
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
		// Try alternative path using controller name extracted from namespace device
		if matches := nvmeDeviceRegex.FindStringSubmatch(deviceName); len(matches) == 2 {
			ctrlName := matches[1]
			transportPath = fmt.Sprintf("/sys/class/nvme/%s/transport", ctrlName)
			transport, err = os.ReadFile(transportPath)
			if err != nil {
				return false, fmt.Errorf("failed to read transport: %w", err)
			}
		} else {
			return false, fmt.Errorf("failed to read transport: %w", err)
		}
	}

	transportStr := strings.TrimSpace(string(transport))
	// Fabric transports: tcp, rdma, fc
	return transportStr == "tcp" || transportStr == "rdma" || transportStr == "fc", nil
}

// NVMeDiscovery performs NVMe-oF discovery.
func NVMeDiscovery(transport, host, port string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
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
		return nil, fmt.Errorf("discovery failed: %w", err)
	}

	var result struct {
		Records []struct {
			SubNQN string `json:"subnqn"`
		} `json:"records"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("failed to parse discovery response: %w", err)
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

	// Find subsystem NQN by extracting controller name from namespace device
	// nvme0n1 -> nvme0
	matches := nvmeDeviceRegex.FindStringSubmatch(deviceName)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid NVMe device name: %s", deviceName)
	}
	ctrlName := matches[1]

	// Read subsysnqn from controller
	// /sys/class/nvme/nvme0/subsysnqn
	nqnPath := filepath.Join("/sys/class/nvme", ctrlName, "subsysnqn") //nolint:gocritic // absolute sysfs path is intentional
	content, err := os.ReadFile(nqnPath)
	if err == nil {
		return strings.TrimSpace(string(content)), nil
	}

	// Try via subsystem link
	// /sys/class/nvme/nvme0/subsystem/subsysnqn
	nqnPath = filepath.Join("/sys/class/nvme", ctrlName, "subsystem", "subsysnqn") //nolint:gocritic // absolute sysfs path is intentional
	content, err = os.ReadFile(nqnPath)
	if err == nil {
		return strings.TrimSpace(string(content)), nil
	}

	return "", fmt.Errorf("could not find NQN for device %s", devicePath)
}

// IsLikelyNVMeDevice checks if a device path looks like an NVMe namespace device.
// Returns true for namespace devices (nvme0n1, nvme10n2) that could have NQN info.
// Returns false for partitions (nvme0n1p1) and controller devices (nvme0).
// Used for race condition detection in session GC - if GetNVMeInfoFromDevice fails
// for a device that looks like an NVMe namespace, it might indicate a transient state.
func IsLikelyNVMeDevice(devicePath string) bool {
	deviceName := filepath.Base(devicePath)
	// Use the same regex as GetNVMeInfoFromDevice to ensure consistency.
	// This matches namespace devices (nvme0n1) but NOT partitions (nvme0n1p4).
	return nvmeDeviceRegex.MatchString(deviceName)
}

// FindNVMeoFSessionBySubsysName searches connected NVMe subsystems for one matching the subsystem name.
// The subsystem name is the part that should appear in the NQN (with any prefix/suffix already applied).
// This is used for cleanup when the device path is unavailable (e.g., after node restart).
func FindNVMeoFSessionBySubsysName(subsysName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	subsystems, err := listNVMeSubsystemsFunc(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list NVMe subsystems: %w", err)
	}

	// Search for a subsystem whose NQN ends with the subsystem name after a colon.
	// NQN format: nqn.YYYY-MM.reverse.domain:subsystem_name
	// Example: nqn.2024-01.io.truenas:vol-1
	//
	// We use strict suffix matching (":subsysName") to prevent partial matches.
	// For example, searching for "vol-1" should NOT match "nqn.2024-01.io.truenas:vol-10"
	// but SHOULD match "nqn.2024-01.io.truenas:vol-1".
	searchSuffix := ":" + subsysName
	for _, subsystem := range subsystems {
		if strings.HasSuffix(subsystem.NQN, searchSuffix) {
			klog.V(4).Infof("Found NVMe-oF subsystem for name %s: NQN=%s", subsysName, subsystem.NQN)
			return subsystem.NQN, nil
		}
	}

	return "", fmt.Errorf("no NVMe-oF subsystem found for name %s", subsysName)
}

// FindNVMeoFSessionByVolumeID is a convenience wrapper that searches by volumeID.
//
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
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
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
	ctx, cancel := context.WithTimeout(context.Background(), getNVMeTimeout())
	defer cancel()

	subsystems, err := ListNVMeSubsystems(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list NVMe subsystems: %w", err)
	}

	return FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems)
}

// FindNVMeoFSessionByNQNFromSubsystems searches a pre-fetched subsystem list
// for an exact NQN match.
func FindNVMeoFSessionByNQNFromSubsystems(nqn string, subsystems []NVMeSubsystem) (string, error) {
	for _, subsystem := range subsystems {
		if subsystem.NQN == nqn {
			klog.V(4).Infof("Found NVMe-oF session for NQN %s", nqn)
			return subsystem.NQN, nil
		}
	}

	return "", fmt.Errorf("no NVMe-oF subsystem found for NQN %s", nqn)
}
