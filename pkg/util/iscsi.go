// Package util provides utility functions for iSCSI operations.
package util

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// portalDiscoveryMutex serializes iSCSI discovery operations per portal.
// This prevents TrueNAS from being overwhelmed when multiple volumes
// try to discover targets simultaneously.
var portalDiscoveryMutex sync.Map // map[portal]*sync.Mutex

// getPortalMutex returns a mutex for the given portal, creating one if needed.
func getPortalMutex(portal string) *sync.Mutex {
	mutex, _ := portalDiscoveryMutex.LoadOrStore(portal, &sync.Mutex{})
	return mutex.(*sync.Mutex)
}

// discoveryCache caches discovery results to avoid repeated calls.
// Key is portal, value is timestamp of last successful discovery.
var discoveryCache sync.Map // map[portal]time.Time

// discoveryValidDuration is how long a cached discovery is considered valid.
const discoveryValidDuration = 30 * time.Second

// maxDiscoveryRetries is the maximum number of discovery retries when a target is not found.
// This handles the case where TrueNAS takes time to propagate new targets to the iSCSI daemon.
const maxDiscoveryRetries = 5

// initialDiscoveryRetryDelay is the initial delay before retrying discovery.
// We start with a longer delay to give TrueNAS time to propagate the target.
const initialDiscoveryRetryDelay = 2 * time.Second

// maxDiscoveryRetryDelay caps the exponential backoff.
const maxDiscoveryRetryDelay = 10 * time.Second

// iscsiCommandTimeout is the timeout for iscsiadm commands to prevent hangs.
const iscsiCommandTimeout = 10 * time.Second

// invalidateDiscoveryCache removes the cached discovery for a portal.
// This should be called when a login fails due to "target not found" to force
// a fresh discovery that includes newly created targets.
func invalidateDiscoveryCache(portal string) {
	discoveryCache.Delete(portal)
	klog.V(4).Infof("Invalidated discovery cache for portal %s", portal)
}

// isTargetNotFoundError checks if the error indicates the target was not found
// in the iscsiadm node database. This happens when the target was created after
// the last discovery, and the cached discovery doesn't include it.
func isTargetNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// iscsiadm returns these messages when the target node record doesn't exist
	return strings.Contains(errStr, "No records found") ||
		strings.Contains(errStr, "Could not find records for") ||
		strings.Contains(errStr, "no record found") ||
		strings.Contains(errStr, "does not exist")
}

// maxConcurrentLogins limits the number of concurrent iSCSI logins per portal.
// TrueNAS iSCSI service can handle a few concurrent logins but gets slow with many.
const maxConcurrentLogins = 2

// portalLoginSemaphore limits concurrent logins per portal.
var portalLoginSemaphore sync.Map // map[portal]chan struct{}

// ISCSISession represents an active iSCSI session.
type ISCSISession struct {
	TargetPortal string
	IQN          string
	SessionID    string
}

// DefaultISCSIDeviceTimeout is the default timeout for waiting for iSCSI devices to appear.
const DefaultISCSIDeviceTimeout = 60 * time.Second

// ISCSIConnectOptions holds options for iSCSI connection.
type ISCSIConnectOptions struct {
	DeviceTimeout time.Duration // Timeout for waiting for device to appear (default: 60s)
}

// ISCSIConnect connects to an iSCSI target and returns the device path.
func ISCSIConnect(portal, iqn string, lun int) (string, error) {
	return ISCSIConnectWithOptions(context.Background(), portal, iqn, lun, nil)
}

// ISCSIConnectWithOptions connects to an iSCSI target with configurable options.
func ISCSIConnectWithOptions(ctx context.Context, portal, iqn string, lun int, opts *ISCSIConnectOptions) (string, error) {
	start := time.Now()
	klog.Infof("ISCSIConnect: portal=%s, iqn=%s, lun=%d", portal, iqn, lun)

	// Apply defaults
	timeout := DefaultISCSIDeviceTimeout
	if opts != nil && opts.DeviceTimeout > 0 {
		timeout = opts.DeviceTimeout
	}

	// Check if already logged in - skip discovery if session exists
	sessions, err := getISCSISessions()
	if err != nil {
		klog.V(4).Infof("Failed to get sessions: %v, will proceed with discovery", err)
	} else {
		for _, session := range sessions {
			if session.IQN == iqn {
				klog.Infof("Session already exists for %s, skipping discovery (elapsed: %v)", iqn, time.Since(start))
				// Session exists, just wait for device
				devicePath, err := waitForISCSIDevice(portal, iqn, lun, timeout)
				if err != nil {
					return "", fmt.Errorf("device not found after %v: %w", timeout, err)
				}
				klog.Infof("ISCSIConnect completed (session reuse) in %v", time.Since(start))
				return devicePath, nil
			}
		}
	}

	// Serialized discovery with caching to prevent TrueNAS overload
	// when multiple volumes mount simultaneously
	discoveryStart := time.Now()
	if err := iscsiDiscoverySerialized(ctx, portal); err != nil {
		return "", fmt.Errorf("discovery failed: %w", err)
	}
	klog.Infof("iSCSI discovery completed in %v", time.Since(discoveryStart))

	// Login to target (also serialized per portal to prevent overload)
	// If login fails due to target not found, retry with exponential backoff.
	// TrueNAS may take time to propagate newly created targets to the iSCSI daemon.
	loginStart := time.Now()
	loginErr := iscsiLoginSerialized(ctx, portal, iqn)
	if loginErr != nil && isTargetNotFoundError(loginErr) {
		klog.Warningf("iSCSI login failed for %s (target not found in discovery), will retry with fresh discovery: %v", iqn, loginErr)

		retryDelay := initialDiscoveryRetryDelay
		for attempt := 1; attempt <= maxDiscoveryRetries; attempt++ {
			// Wait before retry to give TrueNAS time to propagate the target
			klog.Infof("iSCSI retry %d/%d: waiting %v before fresh discovery for %s (elapsed: %v)",
				attempt, maxDiscoveryRetries, retryDelay, iqn, time.Since(start))
			time.Sleep(retryDelay)

			// Invalidate cache and perform fresh discovery
			invalidateDiscoveryCache(portal)
			if discoverErr := iscsiDiscoverySerialized(ctx, portal); discoverErr != nil {
				klog.Warningf("iSCSI retry %d/%d: discovery failed for portal %s: %v", attempt, maxDiscoveryRetries, portal, discoverErr)
				// Continue to next retry
			} else {
				klog.Infof("iSCSI retry %d/%d: fresh discovery completed for portal %s, attempting login to %s",
					attempt, maxDiscoveryRetries, portal, iqn)

				// Retry login
				loginErr = iscsiLoginSerialized(ctx, portal, iqn)
				if loginErr == nil {
					klog.Infof("iSCSI login succeeded for %s after %d discovery retries (total elapsed: %v)",
						iqn, attempt, time.Since(start))
					break
				}

				if !isTargetNotFoundError(loginErr) {
					// Different error, don't retry
					return "", fmt.Errorf("login failed for %s after discovery retry %d: %w", iqn, attempt, loginErr)
				}
				klog.Warningf("iSCSI retry %d/%d: login still failed for %s (target not found): %v",
					attempt, maxDiscoveryRetries, iqn, loginErr)
			}

			// Exponential backoff with cap
			retryDelay *= 2
			if retryDelay > maxDiscoveryRetryDelay {
				retryDelay = maxDiscoveryRetryDelay
			}
		}

		if loginErr != nil {
			return "", fmt.Errorf("iSCSI login failed for %s after %d discovery retries (total elapsed: %v): %w",
				iqn, maxDiscoveryRetries, time.Since(start), loginErr)
		}
	} else if loginErr != nil {
		return "", fmt.Errorf("iSCSI login failed for %s: %w", iqn, loginErr)
	}
	klog.Infof("iSCSI login completed for %s in %v", iqn, time.Since(loginStart))

	// Wait for device to appear
	deviceStart := time.Now()
	devicePath, err := waitForISCSIDevice(portal, iqn, lun, timeout)
	if err != nil {
		return "", fmt.Errorf("device not found after %v: %w", timeout, err)
	}
	klog.Infof("iSCSI device appeared in %v", time.Since(deviceStart))

	klog.Infof("ISCSIConnect completed (full connect) in %v", time.Since(start))
	return devicePath, nil
}

// ISCSIDisconnect disconnects from an iSCSI target.
func ISCSIDisconnect(portal, iqn string) error {
	klog.V(4).Infof("ISCSIDisconnect: portal=%s, iqn=%s", portal, iqn)

	// Logout from target
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--logout")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already logged out
		if strings.Contains(string(output), "No matching sessions") ||
			strings.Contains(string(output), "not logged in") {
			klog.V(4).Infof("Target already logged out: %s", iqn)
			return nil
		}
		return fmt.Errorf("logout failed: %v, output: %s", err, string(output))
	}

	// Delete the node record
	cmd = exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "-o", "delete")
	output, err = cmd.CombinedOutput()
	if err != nil {
		// Not critical if delete fails
		klog.Warningf("Failed to delete node record: %v, output: %s", err, string(output))
	}

	return nil
}

// iscsiDiscoverySerialized performs iSCSI discovery with serialization and caching.
// This prevents TrueNAS from being overwhelmed when multiple volumes try to
// discover targets simultaneously. Discovery results are cached for 30 seconds.
func iscsiDiscoverySerialized(ctx context.Context, portal string) error {
	// Check cache first (outside of mutex for fast path)
	if lastDiscovery, ok := discoveryCache.Load(portal); ok {
		if time.Since(lastDiscovery.(time.Time)) < discoveryValidDuration {
			klog.V(4).Infof("Using cached discovery for portal %s (age: %v)", portal, time.Since(lastDiscovery.(time.Time)))
			return nil
		}
	}

	// Acquire portal-specific mutex to serialize discovery
	mutex := getPortalMutex(portal)
	mutex.Lock()
	defer mutex.Unlock()

	// Check cache again after acquiring lock (another goroutine may have done discovery)
	if lastDiscovery, ok := discoveryCache.Load(portal); ok {
		if time.Since(lastDiscovery.(time.Time)) < discoveryValidDuration {
			klog.V(4).Infof("Using cached discovery for portal %s after lock (age: %v)", portal, time.Since(lastDiscovery.(time.Time)))
			return nil
		}
	}

	// Perform actual discovery
	klog.Infof("Performing iSCSI discovery for portal %s (serialized)", portal)
	if err := iscsiDiscovery(ctx, portal); err != nil {
		return err
	}

	// Update cache
	discoveryCache.Store(portal, time.Now())
	return nil
}

// iscsiDiscovery performs iSCSI discovery on the target portal.
func iscsiDiscovery(ctx context.Context, portal string) error {
	// Add timeout to prevent hangs on unreachable portals
	ctx, cancel := context.WithTimeout(ctx, iscsiCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "discovery", "-t", "sendtargets", "-p", portal)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("discovery command failed: %v, output: %s", err, string(output))
	}
	klog.V(4).Infof("Discovery output: %s", string(output))
	return nil
}

// getLoginSemaphore returns a semaphore for the given portal, creating one if needed.
func getLoginSemaphore(portal string) chan struct{} {
	sem, _ := portalLoginSemaphore.LoadOrStore(portal, make(chan struct{}, maxConcurrentLogins))
	return sem.(chan struct{})
}

// iscsiLoginSerialized performs iSCSI login with limited concurrency.
// Allows up to maxConcurrentLogins (2) concurrent logins per portal to prevent
// overwhelming TrueNAS while still allowing some parallelism.
func iscsiLoginSerialized(ctx context.Context, portal, iqn string) error {
	// Acquire semaphore slot (blocks if maxConcurrentLogins already in progress)
	sem := getLoginSemaphore(portal)
	sem <- struct{}{}
	defer func() { <-sem }()

	return iscsiLogin(ctx, portal, iqn)
}

// iscsiLogin logs into an iSCSI target.
func iscsiLogin(ctx context.Context, portal, iqn string) error {
	// Check if already logged in
	sessions, err := getISCSISessions()
	if err != nil {
		klog.Warningf("Failed to get iSCSI sessions: %v", err)
	} else {
		for _, session := range sessions {
			if session.IQN == iqn {
				klog.V(4).Infof("Already logged in to target: %s", iqn)
				return nil
			}
		}
	}

	// Login to target
	// Add timeout to prevent hangs on unreachable portals
	ctx, cancel := context.WithTimeout(ctx, iscsiCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--login")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already logged in
		if strings.Contains(string(output), "already present") {
			klog.V(4).Infof("Target already logged in: %s", iqn)
			return nil
		}
		return fmt.Errorf("login command failed: %v, output: %s", err, string(output))
	}
	klog.V(4).Infof("Login output: %s", string(output))
	return nil
}

// getISCSISessions returns the list of active iSCSI sessions.
func getISCSISessions() ([]ISCSISession, error) {
	cmd := exec.Command("iscsiadm", "-m", "session")
	output, err := cmd.Output()
	if err != nil {
		// No sessions is not an error
		if strings.Contains(string(output), "No active sessions") {
			return nil, nil
		}
		return nil, err
	}

	var sessions []ISCSISession
	lines := strings.Split(string(output), "\n")
	// Format: tcp: [session_id] portal:port,target_portal_group_tag iqn
	re := regexp.MustCompile(`^tcp:\s+\[(\d+)\]\s+([^,]+),\d+\s+(.+)$`)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		matches := re.FindStringSubmatch(line)
		if len(matches) == 4 {
			sessions = append(sessions, ISCSISession{
				SessionID:    matches[1],
				TargetPortal: matches[2],
				IQN:          matches[3],
			})
		}
	}

	return sessions, nil
}

// waitForISCSIDevice waits for the iSCSI device to appear in /dev.
// Uses exponential backoff starting at 50ms, maxing at 500ms for faster detection.
func waitForISCSIDevice(portal, iqn string, lun int, timeout time.Duration) (string, error) {
	start := time.Now()
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 500 * time.Millisecond

	for {
		devicePath, err := findISCSIDevice(iqn, lun)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}

		if time.Since(start) > timeout {
			return "", fmt.Errorf("timeout waiting for device (iqn=%s, lun=%d)", iqn, lun)
		}

		time.Sleep(pollInterval)
		// Exponential backoff: 50ms -> 100ms -> 200ms -> 400ms -> 500ms (max)
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

// findISCSIDevice finds the device path for an iSCSI LUN.
func findISCSIDevice(iqn string, lun int) (string, error) {
	// Look in /sys/class/iscsi_session for the session
	sessionDirs, err := filepath.Glob("/sys/class/iscsi_session/session*")
	if err != nil {
		return "", err
	}

	for _, sessionDir := range sessionDirs {
		// Read the targetname file
		targetNamePath := filepath.Join(sessionDir, "targetname")
		targetNameBytes, err := os.ReadFile(targetNamePath)
		if err != nil {
			continue
		}
		targetName := strings.TrimSpace(string(targetNameBytes))

		if targetName != iqn {
			continue
		}

		// Found the session, now find the device
		// Session directory contains device subdirectory
		sessionName := filepath.Base(sessionDir)
		devicePath, err := findDeviceForSession(sessionName, lun)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}
	}

	return "", fmt.Errorf("device not found for iqn=%s, lun=%d", iqn, lun)
}

// findDeviceForSession finds the block device for a specific session and LUN.
func findDeviceForSession(sessionName string, lun int) (string, error) {
	// Extract session number
	var sessionNum int
	if _, err := fmt.Sscanf(sessionName, "session%d", &sessionNum); err != nil {
		return "", fmt.Errorf("failed to parse session name: %w", err)
	}

	// Look for the device in /sys/class/scsi_device
	// Format: host:bus:target:lun
	pattern := fmt.Sprintf("/sys/class/scsi_device/*:0:0:%d/device/block/*", lun)
	devices, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}

	// Also check specific host pattern based on session
	hostPattern := fmt.Sprintf("/sys/class/iscsi_host/host*/device/session%d", sessionNum)
	hostDirs, _ := filepath.Glob(hostPattern)
	if len(hostDirs) > 0 {
		// Extract host number
		hostDir := filepath.Dir(filepath.Dir(hostDirs[0]))
		hostName := filepath.Base(hostDir)
		var hostNum int
		if _, err := fmt.Sscanf(hostName, "host%d", &hostNum); err != nil {
			return "", fmt.Errorf("failed to parse host name: %w", err)
		}

		// Look for device with this host
		pattern = fmt.Sprintf("/sys/class/scsi_device/%d:0:0:%d/device/block/*", hostNum, lun)
		devices, err = filepath.Glob(pattern)
		if err == nil && len(devices) > 0 {
			deviceName := filepath.Base(devices[0])
			return "/dev/" + deviceName, nil
		}
	}

	// Fallback: look through all scsi devices
	for _, device := range devices {
		deviceName := filepath.Base(device)
		devicePath := "/dev/" + deviceName
		if _, err := os.Stat(devicePath); err == nil {
			return devicePath, nil
		}
	}

	return "", fmt.Errorf("device not found")
}

// GetISCSIDevicePath returns the device path for an iSCSI target/LUN combination.
func GetISCSIDevicePath(iqn string, lun int) (string, error) {
	return findISCSIDevice(iqn, lun)
}

// ISCSIRescanSession rescans an iSCSI session to detect new LUNs.
func ISCSIRescanSession(portal, iqn string) error {
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--rescan")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rescan failed: %v, output: %s", err, string(output))
	}
	return nil
}

// ISCSIGetSessionStats returns session statistics for an iSCSI target.
func ISCSIGetSessionStats(iqn string) (map[string]string, error) {
	cmd := exec.Command("iscsiadm", "-m", "session", "-s")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get session stats: %v", err)
	}

	stats := make(map[string]string)
	lines := strings.Split(string(output), "\n")
	inTargetSection := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, iqn) {
			inTargetSection = true
			continue
		}
		if inTargetSection {
			if strings.HasPrefix(line, "Target:") {
				break // Next target
			}
			if strings.Contains(line, ":") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					stats[key] = value
				}
			}
		}
	}

	return stats, nil
}

// SetISCSINodeParam sets a parameter on an iSCSI node.
func SetISCSINodeParam(portal, iqn, name, value string) error {
	cmd := exec.Command("iscsiadm", "-m", "node", "-T", iqn, "-p", portal,
		"-o", "update", "-n", name, "-v", value)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to set node param: %v, output: %s", err, string(output))
	}
	return nil
}

// ConfigureISCSICHAP configures CHAP authentication for an iSCSI target.
func ConfigureISCSICHAP(portal, iqn, username, password string) error {
	// Set auth method to CHAP
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.authmethod", "CHAP"); err != nil {
		return err
	}

	// Set username
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.username", username); err != nil {
		return err
	}

	// Set password
	if err := SetISCSINodeParam(portal, iqn, "node.session.auth.password", password); err != nil {
		return err
	}

	return nil
}

// GetDeviceWWN returns the WWN (World Wide Name) for a device.
func GetDeviceWWN(devicePath string) (string, error) {
	// Get the device name without /dev/
	deviceName := filepath.Base(devicePath)

	// Read the WWN from sysfs
	wwnPath := fmt.Sprintf("/sys/block/%s/device/wwid", deviceName)
	wwn, err := os.ReadFile(wwnPath)
	if err != nil {
		// Try alternative path
		wwnPath = fmt.Sprintf("/sys/block/%s/device/vpd_pg83", deviceName)
		wwn, err = os.ReadFile(wwnPath)
		if err != nil {
			return "", fmt.Errorf("failed to read WWN: %v", err)
		}
	}

	return strings.TrimSpace(string(wwn)), nil
}

// GetDeviceSize returns the size of a block device in bytes.
func GetDeviceSize(devicePath string) (int64, error) {
	deviceName := filepath.Base(devicePath)
	sizePath := fmt.Sprintf("/sys/block/%s/size", deviceName)

	sizeBytes, err := os.ReadFile(sizePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read device size: %v", err)
	}

	// Size is in 512-byte sectors
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(sizeBytes)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse device size: %v", err)
	}

	return sectors * 512, nil
}

// FlushDeviceBuffers flushes buffers for a block device.
func FlushDeviceBuffers(devicePath string) error {
	cmd := exec.Command("blockdev", "--flushbufs", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to flush buffers: %v, output: %s", err, string(output))
	}
	return nil
}

// GetISCSIInfoFromDevice returns the portal and IQN for a given device path.
func GetISCSIInfoFromDevice(devicePath string) (string, string, error) {
	deviceName := filepath.Base(devicePath)

	// Find session directory in sysfs
	// /sys/block/sdX/device points to the scsi device
	sysPath := filepath.Join("/sys/block", deviceName, "device")
	targetPath, err := filepath.EvalSymlinks(sysPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve sysfs path: %v", err)
	}

	// Walk up until we find "session*"
	sessionDir := ""
	curr := targetPath
	for i := 0; i < 10; i++ { // limit depth
		if strings.HasPrefix(filepath.Base(curr), "session") {
			sessionDir = curr
			break
		}
		parent := filepath.Dir(curr)
		if parent == curr {
			break
		}
		curr = parent
	}

	if sessionDir == "" {
		return "", "", fmt.Errorf("could not find session directory for %s", devicePath)
	}

	// Get IQN
	iqn := ""
	// Optimization (PERF-005): Construct path directly using session name instead of walking
	sessionName := filepath.Base(sessionDir)
	targetNamePath := filepath.Join("/sys/class/iscsi_session", sessionName, "targetname")
	content, err := os.ReadFile(targetNamePath)
	if err == nil {
		iqn = strings.TrimSpace(string(content))
	} else {
		// Fallback: Try the old glob pattern if the direct class path fails
		// (This handles cases where sessionDir might not be what we expect)
		globPattern := filepath.Join(sessionDir, "iscsi_session", "session*", "targetname")
		matches, _ := filepath.Glob(globPattern)
		if len(matches) > 0 {
			content, err := os.ReadFile(matches[0])
			if err == nil {
				iqn = strings.TrimSpace(string(content))
			}
		}
	}

	if iqn == "" {
		return "", "", fmt.Errorf("could not find targetname for session %s", sessionDir)
	}

	// Get Portal using iscsiadm
	sessions, err := getISCSISessions()
	if err != nil {
		return "", "", fmt.Errorf("failed to get sessions: %v", err)
	}

	for _, s := range sessions {
		if s.IQN == iqn {
			return s.TargetPortal, s.IQN, nil
		}
	}

	return "", "", fmt.Errorf("could not find portal for IQN %s", iqn)
}
