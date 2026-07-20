// Package util provides utility functions for iSCSI operations.
package util

import (
	"bytes"
	"context"
	"errors"
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

// portalDiscoveryMutex serializes iSCSI node-record and discovery operations
// per portal. This prevents concurrent node database updates and keeps
// SendTargets fallback traffic from overwhelming TrueNAS.
var portalDiscoveryMutex sync.Map // map[portal]*sync.Mutex

// iscsiAdmCombinedOutput and waitForISCSIDeviceFn provide narrow test seams for
// the connect path while preserving the production command and device-wait
// behavior.
var iscsiAdmCombinedOutput = func(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "iscsiadm", args...).CombinedOutput()
}

var waitForISCSIDeviceFn = waitForISCSIDevice

// getPortalMutex returns a mutex for the given portal, creating one if needed.
func getPortalMutex(portal string) *sync.Mutex {
	mutex, _ := portalDiscoveryMutex.LoadOrStore(portal, &sync.Mutex{})
	if m, ok := mutex.(*sync.Mutex); ok {
		return m
	}
	// Should never happen, but return new mutex as fallback
	klog.Warningf("Unexpected type in portalDiscoveryMutex for portal %s, creating new mutex", portal)
	newMutex := &sync.Mutex{}
	portalDiscoveryMutex.Store(portal, newMutex)
	return newMutex
}

// discoveryCache caches discovery results to avoid repeated calls.
// Key is portal, value is timestamp of last successful discovery.
var discoveryCache sync.Map // map[portal]time.Time

// Note: getDiscoveryCacheDuration() is now configurable via SetConfig().DiscoveryCacheDuration
// Default: 30s

// maxDiscoveryRetries is the maximum number of discovery retries when a target is not found.
// This handles the case where TrueNAS takes time to propagate new targets to the iSCSI daemon.
const maxDiscoveryRetries = 5

// initialDiscoveryRetryDelay is the initial delay before retrying discovery.
// We start with a longer delay to give TrueNAS time to propagate the target.
const initialDiscoveryRetryDelay = 2 * time.Second

// maxDiscoveryRetryDelay caps the exponential backoff.
const maxDiscoveryRetryDelay = 10 * time.Second

// Note: getISCSITimeout() is now configurable via SetConfig().ISCSITimeout
// Default: 10s

// invalidateDiscoveryCache removes the cached discovery for a portal.
// This should be called when a login fails due to "target not found" to force
// a fresh discovery that includes newly created targets.
func invalidateDiscoveryCache(portal string) {
	discoveryCache.Delete(portal)
	klog.V(4).Infof("Invalidated discovery cache for portal %s", portal)
}

// isTargetNotFoundError checks if the error indicates the target or portal was
// not found in the iscsiadm node database.
func isTargetNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	// iscsiadm returns these messages when the target node record doesn't exist
	return strings.Contains(errStr, "no records found") ||
		strings.Contains(errStr, "could not find records for") ||
		strings.Contains(errStr, "no record found") ||
		strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "no portals found")
}

// Note: getMaxConcurrentLogins() is now configurable via SetConfig().MaxConcurrentLogins
// Default: 2

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

// staleSessionValidationTimeout bounds validation of an existing session. A
// healthy session normally exposes its device in about 50ms, leaving a 40x
// margin while avoiding a long delay when the session is stale.
const staleSessionValidationTimeout = 2 * time.Second

// ISCSIConnectOptions holds options for iSCSI connection.
type ISCSIConnectOptions struct {
	DeviceTimeout       time.Duration // Timeout for waiting for device to appear (default: 60s)
	SessionCleanupDelay time.Duration // Delay after cleaning up stale session (default: 500ms)
}

// ISCSIConnect connects to an iSCSI target and returns the device path.
func ISCSIConnect(portal, iqn string, lun int) (string, error) {
	return ISCSIConnectWithOptions(context.Background(), portal, iqn, lun, nil)
}

// ISCSIConnectWithOptions connects to an iSCSI target with configurable options.
func ISCSIConnectWithOptions(ctx context.Context, portal, iqn string, lun int, opts *ISCSIConnectOptions) (string, error) {
	sessions, err := ListISCSISessions()
	if err != nil {
		klog.V(4).Infof("Failed to get sessions: %v, will proceed with connection setup", err)
	}

	return ISCSIConnectWithOptionsAndSessions(ctx, portal, iqn, lun, opts, sessions)
}

// ISCSIConnectWithOptionsAndSessions connects to an iSCSI target using a
// pre-fetched session list.
func ISCSIConnectWithOptionsAndSessions(ctx context.Context, portal, iqn string, lun int, opts *ISCSIConnectOptions, sessions []ISCSISessionInfo) (string, error) {
	start := time.Now()
	klog.Infof("ISCSIConnect: portal=%s, iqn=%s, lun=%d", portal, iqn, lun)

	// Apply defaults
	timeout := DefaultISCSIDeviceTimeout
	sessionCleanupDelay := 500 * time.Millisecond
	if opts != nil {
		if opts.DeviceTimeout > 0 {
			timeout = opts.DeviceTimeout
		}
		if opts.SessionCleanupDelay > 0 {
			sessionCleanupDelay = opts.SessionCleanupDelay
		}
	}

	// Check if already logged in - validate session before reuse
	for _, session := range sessions {
		if session.IQN != iqn {
			continue
		}
		klog.Infof("Found existing session for %s, validating... (elapsed: %v)", iqn, time.Since(start))
		// Try to get device with a short timeout to validate session is healthy
		// If device appears quickly, session is valid and can be reused
		devicePath, waitErr := waitForISCSIDevice(portal, iqn, lun, staleSessionValidationTimeout)
		if waitErr == nil {
			klog.Infof("ISCSIConnect completed (session reuse) in %v", time.Since(start))
			return devicePath, nil
		}
		// Session exists but device didn't appear - session is likely stale
		// Disconnect it and proceed with fresh discovery/login
		klog.Warningf("Existing session for %s appears stale (device not found in %v), disconnecting", iqn, staleSessionValidationTimeout)
		if disconnectErr := ISCSIDisconnect(portal, iqn); disconnectErr != nil {
			klog.Warningf("Failed to disconnect stale session %s: %v (proceeding anyway)", iqn, disconnectErr)
		}
		// Do not let the pre-disconnect snapshot suppress the login below.
		sessions = removeISCSISessionByIQN(sessions, iqn)
		if sessionCleanupDelay > 0 {
			if cleanupErr := waitForISCSISessionCleanup(ctx, iqn, sessionCleanupDelay); cleanupErr != nil {
				klog.V(4).Infof("iSCSI session cleanup poll for %s ended with: %v", iqn, cleanupErr)
			}
		}
		break
	}

	// Create only the node record needed for this target. This avoids network
	// discovery on the happy path while leaving SendTargets available below as
	// a fallback for portal configurations the static record cannot satisfy.
	nodeRecordStart := time.Now()
	if nodeRecordErr := iscsiEnsureNodeRecord(ctx, portal, iqn); nodeRecordErr != nil {
		return "", fmt.Errorf("node record creation failed: %w", nodeRecordErr)
	}
	klog.Infof("iSCSI fast-path node record ensured for %s in %v", iqn, time.Since(nodeRecordStart))

	// Login to target (also serialized per portal to prevent overload)
	// If login fails due to target not found, retry with exponential backoff.
	// TrueNAS may take time to propagate newly created targets to the iSCSI daemon.
	loginStart := time.Now()
	loginErr := iscsiLoginSerializedWithSessions(ctx, portal, iqn, sessions)
	if loginErr != nil && isTargetNotFoundError(loginErr) {
		klog.Warningf("iSCSI fast-path login failed for %s (target not found/no portal record), falling back to SendTargets discovery: %v", iqn, loginErr)

		retryDelay := initialDiscoveryRetryDelay
		for attempt := 1; attempt <= maxDiscoveryRetries; attempt++ {
			// Wait before retry to give TrueNAS time to propagate the target
			klog.Infof("iSCSI retry %d/%d: waiting %v before fresh discovery for %s (elapsed: %v)",
				attempt, maxDiscoveryRetries, retryDelay, iqn, time.Since(start))
			// Use context-aware sleep to allow cancellation during backoff
			select {
			case <-time.After(retryDelay):
				// Continue with retry
			case <-ctx.Done():
				return "", fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
			}

			// Invalidate cache and perform fresh discovery
			invalidateDiscoveryCache(portal)
			if discoverErr := iscsiDiscoverySerialized(ctx, portal); discoverErr != nil {
				klog.Warningf("iSCSI retry %d/%d: discovery failed for portal %s: %v", attempt, maxDiscoveryRetries, portal, discoverErr)
				// Continue to next retry
			} else {
				klog.Infof("iSCSI retry %d/%d: fresh discovery completed for portal %s, attempting login to %s",
					attempt, maxDiscoveryRetries, portal, iqn)

				// Retry login
				loginErr = iscsiLoginSerializedWithSessions(ctx, portal, iqn, sessions)
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
	devicePath, err := waitForISCSIDeviceFn(portal, iqn, lun, timeout)
	if err != nil {
		return "", fmt.Errorf("device not found after %v: %w", timeout, err)
	}
	klog.Infof("iSCSI device appeared in %v", time.Since(deviceStart))

	klog.Infof("ISCSIConnect completed (full connect) in %v", time.Since(start))
	return devicePath, nil
}

// iscsiEnsureNodeRecord creates a static node record for exactly one target.
// Node database updates are serialized per portal, but intentionally bypass
// the discovery cache so every fast-path login first ensures its record exists.
func iscsiEnsureNodeRecord(ctx context.Context, portal, iqn string) error {
	mutex := getPortalMutex(portal)
	mutex.Lock()
	defer mutex.Unlock()

	cmdCtx, cancel := context.WithTimeout(ctx, getISCSITimeout())
	defer cancel()

	args := []string{"-m", "node", "-o", "new", "-T", iqn, "-p", portal}
	output, err := iscsiAdmCombinedOutput(cmdCtx, args...)
	if err != nil {
		detail := strings.ToLower(string(output) + " " + err.Error())
		if strings.Contains(detail, "already exists") || strings.Contains(detail, "already present") {
			klog.V(4).Infof("iSCSI node record already exists for %s at %s", iqn, portal)
			return nil
		}
		return fmt.Errorf("node record command failed: %w, output: %s", err, string(output))
	}

	klog.V(4).Infof("Static iSCSI node record created for %s at %s", iqn, portal)
	return nil
}

// ISCSIDisconnect disconnects from an iSCSI target with retry for transient failures.
func ISCSIDisconnect(portal, iqn string) error {
	return ISCSIDisconnectWithContext(context.Background(), portal, iqn)
}

// ISCSIDisconnectWithContext disconnects from an iSCSI target with context and retry.
func ISCSIDisconnectWithContext(ctx context.Context, portal, iqn string) error {
	klog.V(4).Infof("ISCSIDisconnect: portal=%s, iqn=%s", portal, iqn)

	retryCfg := DisconnectRetryConfig()

	err := RetryWithBackoff(ctx, "iSCSI logout "+iqn, retryCfg, func() error {
		cmdCtx, cancel := context.WithTimeout(ctx, getISCSITimeout())
		defer cancel()

		// Logout from target
		cmd := exec.CommandContext(cmdCtx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--logout")
		output, err := cmd.CombinedOutput()
		if err != nil {
			// Check if already logged out - treat as success
			if strings.Contains(string(output), "No matching sessions") ||
				strings.Contains(string(output), "not logged in") {
				klog.V(4).Infof("Target already logged out: %s", iqn)
				return nil
			}
			// Return error to potentially trigger retry
			return fmt.Errorf("logout failed: %w, output: %s", err, string(output))
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Delete the node record (best effort, no retry needed)
	cmdCtx, cancel := context.WithTimeout(ctx, getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "-o", "delete")
	output, err := cmd.CombinedOutput()
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
		if ts, ok := lastDiscovery.(time.Time); ok {
			if time.Since(ts) < getDiscoveryCacheDuration() {
				klog.V(4).Infof("Using cached discovery for portal %s (age: %v)", portal, time.Since(ts))
				return nil
			}
		}
	}

	// Acquire portal-specific mutex to serialize discovery
	mutex := getPortalMutex(portal)
	mutex.Lock()
	defer mutex.Unlock()

	// Check cache again after acquiring lock (another goroutine may have done discovery)
	if lastDiscovery, ok := discoveryCache.Load(portal); ok {
		if ts, ok := lastDiscovery.(time.Time); ok {
			if time.Since(ts) < getDiscoveryCacheDuration() {
				klog.V(4).Infof("Using cached discovery for portal %s after lock (age: %v)", portal, time.Since(ts))
				return nil
			}
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
	ctx, cancel := context.WithTimeout(ctx, getISCSITimeout())
	defer cancel()

	output, err := iscsiAdmCombinedOutput(ctx, "-m", "discovery", "-t", "sendtargets", "-p", portal)
	if err != nil {
		return fmt.Errorf("discovery command failed: %w, output: %s", err, string(output))
	}
	klog.V(4).Infof("Discovery output: %s", string(output))
	return nil
}

// getLoginSemaphore returns a semaphore for the given portal, creating one if needed.
func getLoginSemaphore(portal string) chan struct{} {
	sem, _ := portalLoginSemaphore.LoadOrStore(portal, make(chan struct{}, getMaxConcurrentLogins()))
	if s, ok := sem.(chan struct{}); ok {
		return s
	}
	// Should never happen, but return new semaphore as fallback
	klog.Warningf("Unexpected type in portalLoginSemaphore for portal %s, creating new semaphore", portal)
	newSem := make(chan struct{}, getMaxConcurrentLogins())
	portalLoginSemaphore.Store(portal, newSem)
	return newSem
}

func iscsiLoginSerializedWithSessions(ctx context.Context, portal, iqn string, sessions []ISCSISessionInfo) error {
	// Acquire semaphore slot with context awareness
	sem := getLoginSemaphore(portal)
	select {
	case sem <- struct{}{}:
		// Got a slot, proceed with login
		defer func() { <-sem }()
	case <-ctx.Done():
		return fmt.Errorf("context canceled waiting for login slot: %w", ctx.Err())
	}

	return iscsiLoginWithSessions(ctx, portal, iqn, sessions)
}

func iscsiLoginWithSessions(ctx context.Context, portal, iqn string, sessions []ISCSISessionInfo) error {
	// Check if already logged in
	for _, session := range sessions {
		if session.IQN == iqn {
			klog.V(4).Infof("Already logged in to target: %s", iqn)
			return nil
		}
	}

	// Login to target
	// Add timeout to prevent hangs on unreachable portals
	ctx, cancel := context.WithTimeout(ctx, getISCSITimeout())
	defer cancel()

	output, err := iscsiAdmCombinedOutput(ctx, "-m", "node", "-T", iqn, "-p", portal, "--login")
	if err != nil {
		// Check if already logged in
		if strings.Contains(string(output), "already present") {
			klog.V(4).Infof("Target already logged in: %s", iqn)
			return nil
		}
		return fmt.Errorf("login command failed: %w, output: %s", err, string(output))
	}
	klog.V(4).Infof("Login output: %s", string(output))
	return nil
}

// getISCSISessions returns the list of active iSCSI sessions.
func getISCSISessions() ([]ISCSISession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "session")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 21 {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list iSCSI sessions: %w, stderr: %s", err, strings.TrimSpace(stderr.String()))
	}

	var sessions []ISCSISession
	lines := strings.Split(string(output), "\n")
	// Format: tcp: [session_id] portal:port,target_portal_group_tag iqn (mode)
	// The mode suffix (e.g., "(non-flash)") is NOT part of the IQN
	// IQN format: iqn.YYYY-MM.reversed.domain:target_name
	re := regexp.MustCompile(`^tcp:\s+\[(\d+)\]\s+([^,]+),\d+\s+(iqn\.\S+)`)

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
// Uses exponential backoff starting at 50ms, maxing at 100ms for faster detection.
func waitForISCSIDevice(portal, iqn string, lun int, timeout time.Duration) (string, error) {
	start := time.Now()
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 100 * time.Millisecond

	for {
		devicePath, err := findISCSIDevice(iqn, lun)
		if err == nil && devicePath != "" {
			return devicePath, nil
		}

		if time.Since(start) > timeout {
			return "", fmt.Errorf("timeout waiting for device (iqn=%s, lun=%d)", iqn, lun)
		}

		time.Sleep(pollInterval)
		// Exponential backoff: 50ms -> 100ms (max)
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

func waitForISCSISessionCleanup(ctx context.Context, iqn string, timeout time.Duration) error {
	return pollForISCSISessionCleanup(ctx, timeout, func() (bool, error) {
		sessions, err := ListISCSISessions()
		if err != nil {
			return true, err
		}
		_, err = FindISCSISessionByIQNFromSessions(iqn, sessions)
		return err == nil, nil
	})
}

func pollForISCSISessionCleanup(ctx context.Context, timeout time.Duration, sessionExists func() (bool, error)) error {
	exists, lastErr := sessionExists()
	if !exists && lastErr == nil {
		return nil
	}
	if timeout <= 0 {
		return lastErr
	}

	deadline := time.Now().Add(timeout)
	for {
		wait := 100 * time.Millisecond
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("session still present after %v", timeout)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
			exists, lastErr = sessionExists()
			if !exists && lastErr == nil {
				return nil
			}
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
	return findDeviceForSessionInPaths(sessionName, lun, "/sys/class", "/dev")
}

func findDeviceForSessionInPaths(sessionName string, lun int, sysClassRoot, devRoot string) (string, error) {
	// Extract session number
	var sessionNum int
	if _, err := fmt.Sscanf(sessionName, "session%d", &sessionNum); err != nil {
		return "", fmt.Errorf("failed to parse session name: %w", err)
	}

	// Resolve the SCSI host that owns this session. Never fall back to a
	// session-agnostic LUN lookup: different targets commonly expose the same
	// LUN number, and doing so can select another volume's device.
	hostPattern := filepath.Join(sysClassRoot, "iscsi_host", "host*", "device", fmt.Sprintf("session%d", sessionNum))
	hostDirs, err := filepath.Glob(hostPattern)
	if err != nil {
		return "", err
	}
	for _, sessionDir := range hostDirs {
		// Extract host number
		hostDir := filepath.Dir(filepath.Dir(sessionDir))
		hostName := filepath.Base(hostDir)
		var hostNum int
		if _, scanErr := fmt.Sscanf(hostName, "host%d", &hostNum); scanErr != nil {
			return "", fmt.Errorf("failed to parse host name: %w", scanErr)
		}

		// Look for device with this host
		pattern := filepath.Join(sysClassRoot, "scsi_device", fmt.Sprintf("%d:0:0:%d", hostNum, lun), "device", "block", "*")
		devices, globErr := filepath.Glob(pattern)
		if globErr != nil {
			return "", globErr
		}
		for _, device := range devices {
			devicePath := filepath.Join(devRoot, filepath.Base(device))
			if _, statErr := os.Stat(devicePath); statErr == nil {
				return devicePath, nil
			}
		}
	}

	return "", fmt.Errorf("device for session %d not found", sessionNum)
}

// CheckISCSIDeviceMultipathOwnership refuses raw SCSI devices that multipathd
// already owns. Using a component path directly would bypass the dm-multipath
// head device and can corrupt the LUN.
func CheckISCSIDeviceMultipathOwnership(devicePath string) error {
	return checkISCSIDeviceMultipathOwnership(devicePath, "/sys/block")
}

func checkISCSIDeviceMultipathOwnership(devicePath, sysBlockRoot string) error {
	holdersPath := filepath.Join(sysBlockRoot, filepath.Base(devicePath), "holders")
	holders, err := os.ReadDir(holdersPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to inspect holders for iSCSI device %s: %w", devicePath, err)
	}
	for _, holder := range holders {
		if strings.HasPrefix(holder.Name(), "dm-") {
			return fmt.Errorf("iSCSI device %s is claimed by dm-multipath; iSCSI multipath is unsupported", devicePath)
		}
	}
	return nil
}

// GetISCSIDevicePath returns the device path for an iSCSI target/LUN combination.
func GetISCSIDevicePath(iqn string, lun int) (string, error) {
	return findISCSIDevice(iqn, lun)
}

// ISCSIRescanSession rescans an iSCSI session to detect new LUNs.
func ISCSIRescanSession(portal, iqn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--rescan")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rescan failed: %w, output: %s", err, string(output))
	}
	return nil
}

// ISCSIGetSessionStats returns session statistics for an iSCSI target.
func ISCSIGetSessionStats(iqn string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "session", "-s")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get session stats: %w", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal,
		"-o", "update", "-n", name, "-v", value)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to set node param: %w, output: %s", err, string(output))
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
			return "", fmt.Errorf("failed to read WWN: %w", err)
		}
	}

	return strings.TrimSpace(string(wwn)), nil
}

// GetDeviceSize returns the size of a block device in bytes.
func GetDeviceSize(devicePath string) (int64, error) {
	deviceName := filepath.Base(devicePath)
	sizePath := fmt.Sprintf("/sys/class/block/%s/size", deviceName)

	sizeBytes, err := os.ReadFile(sizePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read device size: %w", err)
	}

	// Size is in 512-byte sectors
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(sizeBytes)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse device size: %w", err)
	}

	return sectors * 512, nil
}

// FlushDeviceBuffers flushes buffers for a block device.
func FlushDeviceBuffers(devicePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), getISCSITimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "blockdev", "--flushbufs", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to flush buffers: %w, output: %s", err, string(output))
	}
	return nil
}

// IsLikelyISCSIDevice returns true if the device path looks like it could be an iSCSI device.
// iSCSI devices typically appear as /dev/sd[a-z]+ (SCSI disk devices).
// This is used by Session GC to distinguish between actual iSCSI lookup failures
// (which might indicate a race condition) vs non-iSCSI devices (expected to fail).
func IsLikelyISCSIDevice(devicePath string) bool {
	deviceName := filepath.Base(devicePath)
	// iSCSI devices are SCSI disks: sd[a-z]+
	// Non-iSCSI devices include: nvme*, loop*, nbd*, dm-*, etc.
	if strings.HasPrefix(deviceName, "sd") && len(deviceName) >= 3 {
		// Verify the rest is letters (sda, sdb, ..., sdaa, sdab, etc.)
		for _, c := range deviceName[2:] {
			if c < 'a' || c > 'z' {
				return false
			}
		}
		return true
	}
	return false
}

// GetISCSIInfoFromDevice returns the portal and IQN for a given device path.
func GetISCSIInfoFromDevice(devicePath string) (portal, iqn string, err error) {
	sessions, err := ListISCSISessions()
	if err != nil {
		return "", "", fmt.Errorf("failed to get sessions: %w", err)
	}

	return GetISCSIInfoFromDeviceWithSessions(devicePath, sessions)
}

// GetISCSIInfoFromDeviceWithSessions returns the portal and IQN for a device
// using a pre-fetched session list.
func GetISCSIInfoFromDeviceWithSessions(devicePath string, sessions []ISCSISessionInfo) (portal, iqn string, err error) {
	deviceName := filepath.Base(devicePath)

	// Find session directory in sysfs
	// /sys/block/sdX/device points to the scsi device
	sysPath := filepath.Join("/sys/block", deviceName, "device") //nolint:gocritic // absolute sysfs path is intentional
	targetPath, err := filepath.EvalSymlinks(sysPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve sysfs path: %w", err)
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
	iqn = ""
	// Optimization (PERF-005): Construct path directly using session name instead of walking
	sessionName := filepath.Base(sessionDir)
	targetNamePath := filepath.Join("/sys/class/iscsi_session", sessionName, "targetname") //nolint:gocritic // absolute sysfs path is intentional
	content, err := os.ReadFile(targetNamePath)
	if err == nil {
		iqn = strings.TrimSpace(string(content))
	} else {
		// Fallback: Try the old glob pattern if the direct class path fails
		// (This handles cases where sessionDir might not be what we expect)
		globPattern := filepath.Join(sessionDir, "iscsi_session", "session*", "targetname")
		matches, _ := filepath.Glob(globPattern)
		if len(matches) > 0 {
			fallbackContent, readErr := os.ReadFile(matches[0])
			if readErr == nil {
				iqn = strings.TrimSpace(string(fallbackContent))
			}
		}
	}

	if iqn == "" {
		return "", "", fmt.Errorf("could not find targetname for session %s", sessionDir)
	}

	for _, s := range sessions {
		if s.IQN == iqn {
			return s.Portal, s.IQN, nil
		}
	}

	return "", "", fmt.Errorf("could not find portal for IQN %s", iqn)
}

// FindISCSISessionByTargetName searches active iSCSI sessions for one matching the target name.
// The target name is the part after the colon in the IQN (e.g., "pvc-xxx" in "iqn.2005-10.org.freenas.ctl:pvc-xxx").
// This is used for cleanup when the device path is unavailable (e.g., after node restart).
func FindISCSISessionByTargetName(targetName string) (string, error) {
	sessions, err := getISCSISessions()
	if err != nil {
		return "", fmt.Errorf("failed to get iSCSI sessions: %w", err)
	}

	// Search for a session whose IQN ends with :{targetName}
	// IQN format: iqn.2005-10.org.freenas.ctl:pvc-xxx[-suffix]
	expectedSuffix := ":" + targetName
	for _, session := range sessions {
		if strings.HasSuffix(session.IQN, expectedSuffix) {
			klog.V(4).Infof("Found iSCSI session for target %s: IQN=%s", targetName, session.IQN)
			return session.IQN, nil
		}
	}

	return "", fmt.Errorf("no iSCSI session found for target %s", targetName)
}

// FindISCSISessionByVolumeID is a convenience wrapper that searches by volumeID.
//
// Deprecated: Use FindISCSISessionByTargetName instead, which handles NameSuffix correctly.
func FindISCSISessionByVolumeID(volumeID string) (string, error) {
	return FindISCSISessionByTargetName(volumeID)
}

// ListISCSISessions returns all active iSCSI sessions.
// This is a public wrapper around getISCSISessions for use by the session GC.
func ListISCSISessions() ([]ISCSISessionInfo, error) {
	sessions, err := getISCSISessions()
	if err != nil {
		return nil, err
	}

	// Convert to public struct
	result := make([]ISCSISessionInfo, len(sessions))
	for i, s := range sessions {
		result[i] = ISCSISessionInfo{
			Portal:    s.TargetPortal,
			IQN:       s.IQN,
			SessionID: s.SessionID,
		}
	}
	return result, nil
}

// ISCSISessionInfo holds information about an active iSCSI session.
// Used by session GC to identify orphaned sessions.
type ISCSISessionInfo struct {
	Portal    string
	IQN       string
	SessionID string
}

// FindISCSISessionByIQN searches active iSCSI sessions for one matching the exact IQN.
// This is used for pre-emptive cleanup before staging a volume.
func FindISCSISessionByIQN(iqn string) (string, error) {
	sessions, err := ListISCSISessions()
	if err != nil {
		return "", fmt.Errorf("failed to get iSCSI sessions: %w", err)
	}

	return FindISCSISessionByIQNFromSessions(iqn, sessions)
}

// FindISCSISessionByIQNFromSessions searches a pre-fetched session list for an
// exact IQN match.
func FindISCSISessionByIQNFromSessions(iqn string, sessions []ISCSISessionInfo) (string, error) {
	for _, session := range sessions {
		if session.IQN == iqn {
			klog.V(4).Infof("Found iSCSI session for IQN %s", iqn)
			return session.IQN, nil
		}
	}

	return "", fmt.Errorf("no iSCSI session found for IQN %s", iqn)
}

func removeISCSISessionByIQN(sessions []ISCSISessionInfo, iqn string) []ISCSISessionInfo {
	filtered := make([]ISCSISessionInfo, 0, len(sessions))
	for _, session := range sessions {
		if session.IQN != iqn {
			filtered = append(filtered, session)
		}
	}
	return filtered
}
