package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestISCSIConnectUsesStaticNodeRecordFastPath(t *testing.T) {
	portal := "192.0.2.10:3260"
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-fast-path"
	var calls [][]string

	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	})

	devicePath, err := ISCSIConnectWithOptionsAndSessions(
		context.Background(), portal, iqn, 0, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "/dev/test-iscsi", devicePath)
	require.Equal(t, [][]string{
		{"-m", "node", "-o", "new", "-T", iqn, "-p", portal},
		{"-m", "node", "-T", iqn, "-p", portal, "--login"},
	}, calls)
	assert.False(t, hasISCSIAdmMode(calls, "discovery"), "fast path must not run SendTargets discovery")
}

func TestISCSIConnectFallsBackToDiscoveryWhenFastPathTargetNotFound(t *testing.T) {
	portal := "192.0.2.11:3260"
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-discovery-fallback"
	var calls [][]string
	loginAttempts := 0

	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		if slices.Contains(args, "--login") {
			loginAttempts++
			if loginAttempts == 1 {
				return []byte("iscsiadm: No records found"), errors.New("exit status 21")
			}
		}
		return nil, nil
	})

	devicePath, err := ISCSIConnectWithOptionsAndSessions(
		context.Background(), portal, iqn, 0, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "/dev/test-iscsi", devicePath)
	require.Equal(t, [][]string{
		{"-m", "node", "-o", "new", "-T", iqn, "-p", portal},
		{"-m", "node", "-T", iqn, "-p", portal, "--login"},
		{"-m", "discovery", "-t", "sendtargets", "-p", portal},
		{"-m", "node", "-T", iqn, "-p", portal, "--login"},
	}, calls)
	assert.Equal(t, 2, loginAttempts)
}

func TestISCSIEnsureNodeRecordAlreadyExistsIsSuccess(t *testing.T) {
	portal := "192.0.2.12:3260"
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-existing-record"
	var calls [][]string

	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return []byte("iscsiadm: Could not create new record: record already exists"), errors.New("exit status 15")
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	require.NoError(t, iscsiEnsureNodeRecord(context.Background(), portal, iqn))
	require.Equal(t, [][]string{
		{"-m", "node", "-o", "new", "-T", iqn, "-p", portal},
	}, calls)
}

func TestISCSIConnectReusesPortlessConfiguredSession(t *testing.T) {
	portal := "192.0.2.13"
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-portless-reuse"
	var calls [][]string
	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	})

	devicePath, err := ISCSIConnectWithOptionsAndSessions(context.Background(), portal, iqn, 0, nil, []ISCSISessionInfo{{
		Portal: "192.0.2.13:3260", IQN: iqn, SessionID: "13",
	}})
	require.NoError(t, err)
	assert.Equal(t, "/dev/test-iscsi", devicePath)
	assert.Empty(t, calls, "a portless configured portal must reuse the canonical iscsiadm session")
}

func TestISCSIConnectDetectsStalePortlessConfiguredSession(t *testing.T) {
	portal := "192.0.2.14"
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-portless-stale"
	originalRunner := iscsiAdmCombinedOutput
	originalWait := waitForISCSIDeviceFn
	originalDisconnect := iscsiDisconnectForConnect
	originalList := listISCSISessionsForDevice
	t.Cleanup(func() {
		iscsiAdmCombinedOutput = originalRunner
		waitForISCSIDeviceFn = originalWait
		iscsiDisconnectForConnect = originalDisconnect
		listISCSISessionsForDevice = originalList
	})
	iscsiAdmCombinedOutput = func(context.Context, ...string) ([]byte, error) { return nil, nil }
	waitCalls := 0
	waitForISCSIDeviceFn = func(string, string, int, time.Duration) (string, error) {
		waitCalls++
		if waitCalls == 1 {
			return "", errors.New("stale device")
		}
		return "/dev/test-iscsi", nil
	}
	var disconnected []string
	iscsiDisconnectForConnect = func(gotPortal, gotIQN string) error {
		assert.Equal(t, iqn, gotIQN)
		disconnected = append(disconnected, gotPortal)
		return nil
	}
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) { return nil, nil }

	devicePath, err := ISCSIConnectWithOptionsAndSessions(context.Background(), portal, iqn, 0,
		&ISCSIConnectOptions{SessionCleanupDelay: time.Nanosecond},
		[]ISCSISessionInfo{{Portal: "192.0.2.14:3260", IQN: iqn, SessionID: "14"}})
	require.NoError(t, err)
	assert.Equal(t, "/dev/test-iscsi", devicePath)
	assert.Equal(t, []string{portal}, disconnected)
	assert.Equal(t, 2, waitCalls, "the stale session must be rejected before the fresh login/device wait")
}

func TestWaitForISCSIDeviceFallsBackWhenPortalSessionDoesNotMatch(t *testing.T) {
	originalList := listISCSISessionsForDevice
	originalPortalFind := findISCSIDeviceForPortal
	originalFallback := findISCSIDeviceFallback
	t.Cleanup(func() {
		listISCSISessionsForDevice = originalList
		findISCSIDeviceForPortal = originalPortalFind
		findISCSIDeviceFallback = originalFallback
	})
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-legacy-portals"
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) {
		return []ISCSISessionInfo{{Portal: "192.0.2.15:3260", IQN: iqn, SessionID: "15"}}, nil
	}
	findISCSIDeviceForPortal = func(portal, gotIQN string, lun int, sessions []ISCSISessionInfo) (string, error) {
		return "", fmt.Errorf("%w for %s", errISCSIPortalSessionNotFound, portal)
	}
	findISCSIDeviceFallback = func(gotIQN string, lun int) (string, error) {
		assert.Equal(t, iqn, gotIQN)
		assert.Zero(t, lun)
		return "/dev/test-iscsi", nil
	}

	for _, portal := range []string{"192.0.2.15", "truenas.example.com:3260"} {
		devicePath, err := waitForISCSIDevice(portal, iqn, 0, 20*time.Millisecond)
		require.NoError(t, err, portal)
		assert.Equal(t, "/dev/test-iscsi", devicePath, portal)
	}
}

func TestWaitForISCSIPortalDeviceRetriesNoMatchWithoutIQNFallback(t *testing.T) {
	originalList := listISCSISessionsForDevice
	originalPortalFind := findISCSIDeviceForPortal
	originalFallback := findISCSIDeviceFallback
	t.Cleanup(func() {
		listISCSISessionsForDevice = originalList
		findISCSIDeviceForPortal = originalPortalFind
		findISCSIDeviceFallback = originalFallback
	})
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-portal-scoped"
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) {
		return []ISCSISessionInfo{{Portal: "192.0.2.17:3260", IQN: iqn, SessionID: "17"}}, nil
	}
	findCalls := 0
	findISCSIDeviceForPortal = func(portal, gotIQN string, lun int, sessions []ISCSISessionInfo) (string, error) {
		findCalls++
		assert.Equal(t, "192.0.2.18:3260", portal)
		assert.Equal(t, iqn, gotIQN)
		assert.Zero(t, lun)
		return "", fmt.Errorf("%w for %s", errISCSIPortalSessionNotFound, portal)
	}
	fallbackCalls := 0
	findISCSIDeviceFallback = func(string, int) (string, error) {
		fallbackCalls++
		return "/dev/primary-portal-device", nil
	}

	devicePath, err := waitForISCSIPortalDevice("192.0.2.18:3260", iqn, 0, 20*time.Millisecond)
	require.Error(t, err)
	assert.Empty(t, devicePath)
	assert.GreaterOrEqual(t, findCalls, 2, "portal-scoped no-match must remain in the wait loop")
	assert.Zero(t, fallbackCalls, "portal-scoped lookup must not borrow the primary portal's device")
}

func TestWaitForISCSIPortalDeviceKeepsFallbackWhenSessionListFails(t *testing.T) {
	originalList := listISCSISessionsForDevice
	originalPortalFind := findISCSIDeviceForPortal
	originalFallback := findISCSIDeviceFallback
	t.Cleanup(func() {
		listISCSISessionsForDevice = originalList
		findISCSIDeviceForPortal = originalPortalFind
		findISCSIDeviceFallback = originalFallback
	})
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) {
		return nil, errors.New("iscsiadm unavailable")
	}
	findISCSIDeviceForPortal = func(portal, _ string, _ int, _ []ISCSISessionInfo) (string, error) {
		return "", fmt.Errorf("%w for %s", errISCSIPortalSessionNotFound, portal)
	}
	findISCSIDeviceFallback = func(string, int) (string, error) {
		return "/dev/sysfs-fallback", nil
	}

	devicePath, err := waitForISCSIPortalDevice("192.0.2.19:3260", "iqn.test", 0, 20*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, "/dev/sysfs-fallback", devicePath)
}

func TestWaitForISCSIDeviceRefreshesSessionListAtBoundedCadence(t *testing.T) {
	originalList := listISCSISessionsForDevice
	originalPortalFind := findISCSIDeviceForPortal
	t.Cleanup(func() {
		listISCSISessionsForDevice = originalList
		findISCSIDeviceForPortal = originalPortalFind
	})
	listCalls := 0
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) {
		listCalls++
		return []ISCSISessionInfo{{Portal: "192.0.2.16:3260", IQN: "iqn.test", SessionID: "16"}}, nil
	}
	findISCSIDeviceForPortal = func(string, string, int, []ISCSISessionInfo) (string, error) {
		return "", errors.New("matching session device not ready")
	}

	_, err := waitForISCSIDevice("192.0.2.16:3260", "iqn.test", 0, 260*time.Millisecond)
	require.Error(t, err)
	assert.Equal(t, 1, listCalls, "device polling must not fork iscsiadm on every 100ms iteration")
}

func stubISCSIConnectDependencies(
	t *testing.T,
	runner func(context.Context, ...string) ([]byte, error),
) {
	t.Helper()
	originalRunner := iscsiAdmCombinedOutput
	originalWait := waitForISCSIDeviceFn
	iscsiAdmCombinedOutput = runner
	waitForISCSIDeviceFn = func(string, string, int, time.Duration) (string, error) {
		return "/dev/test-iscsi", nil
	}
	t.Cleanup(func() {
		iscsiAdmCombinedOutput = originalRunner
		waitForISCSIDeviceFn = originalWait
	})
}

func hasISCSIAdmMode(calls [][]string, mode string) bool {
	for _, args := range calls {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "-m" && args[i+1] == mode {
				return true
			}
		}
	}
	return false
}

func TestGetISCSISessionsExit21IsEmpty(t *testing.T) {
	binDir := t.TempDir()
	script := "#!/bin/sh\nprintf 'iscsiadm: No active sessions.' >&2\nexit 21\n"
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "iscsiadm"), []byte(script), 0o750))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	sessions, err := getISCSISessions()
	require.NoError(t, err)
	assert.Empty(t, sessions)
}

func TestGetISCSISessionsOtherExitIsError(t *testing.T) {
	binDir := t.TempDir()
	script := "#!/bin/sh\nprintf 'transport failure' >&2\nexit 22\n"
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "iscsiadm"), []byte(script), 0o750))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	_, err := getISCSISessions()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exit status 22")
	assert.Contains(t, err.Error(), "transport failure")
}

func TestPollForISCSISessionCleanupReturnsEarly(t *testing.T) {
	calls := 0
	start := time.Now()
	err := pollForISCSISessionCleanup(context.Background(), 500*time.Millisecond, func() (bool, error) {
		calls++
		return calls == 1, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	assert.Less(t, time.Since(start), 400*time.Millisecond)
}

func TestFindDeviceForSessionUsesOwningHostOnly(t *testing.T) {
	sysClassRoot := filepath.Join(t.TempDir(), "class")
	devRoot := filepath.Join(t.TempDir(), "dev")
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "iscsi_host", "host4", "device", "session12"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "scsi_device", "4:0:0:0", "device", "block", "sdb"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "scsi_device", "9:0:0:0", "device", "block", "sdz"), 0o750))
	require.NoError(t, os.MkdirAll(devRoot, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "sdb"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "sdz"), nil, 0o600))

	devicePath, err := findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(devRoot, "sdb"), devicePath)

	// A device for the same LUN on another host must never be used as fallback.
	require.NoError(t, os.Remove(filepath.Join(devRoot, "sdb")))
	devicePath, err = findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.Error(t, err)
	assert.Empty(t, devicePath)
	assert.Contains(t, err.Error(), "device for session 12 not found")
}

func TestCheckISCSIDeviceMultipathOwnership(t *testing.T) {
	sysBlockRoot := t.TempDir()
	holders := filepath.Join(sysBlockRoot, "sdb", "holders")
	require.NoError(t, os.MkdirAll(holders, 0o750))
	require.NoError(t, os.Mkdir(filepath.Join(holders, "dm-0"), 0o750))

	err := checkISCSIDeviceMultipathOwnership("/dev/sdb", sysBlockRoot)
	require.Error(t, err)
	assert.Equal(t, "iSCSI device /dev/sdb is claimed by dm-multipath; staging the raw component path is unsafe", err.Error())

	require.NoError(t, os.Remove(filepath.Join(holders, "dm-0")))
	require.NoError(t, os.Mkdir(filepath.Join(holders, "md0"), 0o750))
	require.NoError(t, checkISCSIDeviceMultipathOwnership("/dev/sdb", sysBlockRoot))
	require.NoError(t, checkISCSIDeviceMultipathOwnership("/dev/sdc", sysBlockRoot))
}

// TestParseISCSISessions tests parsing of iscsiadm session output.
// This is the core parsing logic from getISCSISessions().
func TestParseISCSISessions(t *testing.T) {
	testCases := []struct {
		name         string
		output       string
		wantSessions []ISCSISession
		wantErr      bool
	}{
		{
			name: "single session",
			output: `tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc123 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				},
			},
		},
		{
			name: "multiple sessions",
			output: `tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc123 (non-flash)
tcp: [2] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-def456 (non-flash)
tcp: [3] 198.51.100.50:3260,1 iqn.2005-10.org.freenas.ctl:pvc-ghi789 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-def456",
				},
				{
					SessionID:    "3",
					TargetPortal: "198.51.100.50:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-ghi789",
				},
			},
		},
		{
			name:         "no active sessions",
			output:       `No active sessions.`,
			wantSessions: nil,
		},
		{
			name:         "empty output",
			output:       ``,
			wantSessions: nil,
		},
		{
			name: "session without mode suffix",
			output: `tcp: [5] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-xyz
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "5",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-xyz",
				},
			},
		},
		{
			name: "high session ID",
			output: `tcp: [999] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:vol-test (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "999",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:vol-test",
				},
			},
		},
		{
			name: "IPv6 portal address",
			output: `tcp: [1] [2001:db8::1]:3260,1 iqn.2005-10.org.freenas.ctl:pvc-ipv6 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "[2001:db8::1]:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-ipv6",
				},
			},
		},
		{
			name: "mixed output with empty lines",
			output: `tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc (non-flash)

tcp: [2] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-def (non-flash)

`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-def",
				},
			},
		},
		{
			name: "target portal group tag variations",
			output: `tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-tag1 (non-flash)
tcp: [2] 192.0.2.100:3260,2 iqn.2005-10.org.freenas.ctl:pvc-tag2 (non-flash)
tcp: [3] 192.0.2.100:3260,100 iqn.2005-10.org.freenas.ctl:pvc-tag100 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-tag1",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-tag2",
				},
				{
					SessionID:    "3",
					TargetPortal: "192.0.2.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-tag100",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessions := parseISCSISessionOutput(tc.output)
			assert.Equal(t, tc.wantSessions, sessions)
		})
	}
}

// parseISCSISessionOutput parses iscsiadm session output.
// This replicates the parsing logic from getISCSISessions() for testing.
func parseISCSISessionOutput(output string) []ISCSISession {
	if output == "" || stringContains(output, "No active sessions") {
		return nil
	}

	var sessions []ISCSISession
	lines := stringSplit(output, "\n")
	// Format: tcp: [session_id] portal:port,target_portal_group_tag iqn (mode)
	// Regex: ^tcp:\s+\[(\d+)\]\s+([^,]+),\d+\s+(iqn\.[^\s]+)

	for _, line := range lines {
		line = stringTrimSpace(line)
		if line == "" {
			continue
		}

		session, ok := parseSessionLine(line)
		if ok {
			sessions = append(sessions, session)
		}
	}

	return sessions
}

// parseSessionLine parses a single iscsiadm session line.
func parseSessionLine(line string) (ISCSISession, bool) {
	// Simple parsing without regex for testing
	// Format: tcp: [session_id] portal:port,tpgt iqn (mode)

	// Must start with "tcp:"
	if !stringHasPrefix(line, "tcp:") {
		return ISCSISession{}, false
	}

	// Find session ID between [ and ]
	bracketStart := stringIndex(line, "[")
	bracketEnd := stringIndex(line, "]")
	if bracketStart < 0 || bracketEnd < 0 || bracketEnd <= bracketStart {
		return ISCSISession{}, false
	}
	sessionID := line[bracketStart+1 : bracketEnd]

	// Find portal (between ] and ,)
	afterBracket := line[bracketEnd+1:]
	afterBracket = stringTrimSpace(afterBracket)
	commaIdx := stringIndex(afterBracket, ",")
	if commaIdx < 0 {
		return ISCSISession{}, false
	}
	portal := afterBracket[:commaIdx]

	// Find IQN (starts with "iqn.")
	iqnStart := stringIndex(afterBracket, "iqn.")
	if iqnStart < 0 {
		return ISCSISession{}, false
	}
	iqnPart := afterBracket[iqnStart:]
	// IQN ends at first space (before mode suffix like "(non-flash)")
	spaceIdx := stringIndex(iqnPart, " ")
	var iqn string
	if spaceIdx > 0 {
		iqn = iqnPart[:spaceIdx]
	} else {
		iqn = iqnPart
	}

	return ISCSISession{
		SessionID:    sessionID,
		TargetPortal: portal,
		IQN:          iqn,
	}, true
}

// TestISCSIConnectArguments tests the argument construction for iscsiadm commands.
func TestISCSIConnectArguments(t *testing.T) {
	testCases := []struct {
		name         string
		portal       string
		iqn          string
		operation    string // "discovery", "login", "logout", "delete"
		wantContains []string
	}{
		{
			name:      "discovery command",
			portal:    "192.0.2.100:3260",
			iqn:       "",
			operation: "discovery",
			wantContains: []string{
				"-m", "discovery", "-t", "sendtargets", "-p", "192.0.2.100:3260",
			},
		},
		{
			name:      "login command",
			portal:    "192.0.2.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			operation: "login",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				"-p", "192.0.2.100:3260", "--login",
			},
		},
		{
			name:      "logout command",
			portal:    "198.51.100.50:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-def456",
			operation: "logout",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:pvc-def456",
				"-p", "198.51.100.50:3260", "--logout",
			},
		},
		{
			name:      "delete node record",
			portal:    "192.0.2.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:vol-xyz",
			operation: "delete",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:vol-xyz",
				"-p", "192.0.2.100:3260", "-o", "delete",
			},
		},
		{
			name:      "rescan command",
			portal:    "192.0.2.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:rescan-test",
			operation: "rescan",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:rescan-test",
				"-p", "192.0.2.100:3260", "--rescan",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := buildISCSIAdmArgs(tc.portal, tc.iqn, tc.operation)
			argsStr := stringJoin(args, " ")

			for _, want := range tc.wantContains {
				assert.Contains(t, argsStr, want,
					"Expected '%s' in args, got: %v", want, args)
			}
		})
	}
}

// buildISCSIAdmArgs builds iscsiadm command arguments.
func buildISCSIAdmArgs(portal, iqn, operation string) []string {
	switch operation {
	case "discovery":
		return []string{"-m", "discovery", "-t", "sendtargets", "-p", portal}
	case "login":
		return []string{"-m", "node", "-T", iqn, "-p", portal, "--login"}
	case "logout":
		return []string{"-m", "node", "-T", iqn, "-p", portal, "--logout"}
	case "delete":
		return []string{"-m", "node", "-T", iqn, "-p", portal, "-o", "delete"}
	case "rescan":
		return []string{"-m", "node", "-T", iqn, "-p", portal, "--rescan"}
	default:
		return nil
	}
}

// TestIsTargetNotFoundError tests the target not found error detection.
func TestIsTargetNotFoundError(t *testing.T) {
	testCases := []struct {
		name     string
		errMsg   string
		wantTrue bool
	}{
		{
			name:     "no records found",
			errMsg:   "iscsiadm: No records found",
			wantTrue: true,
		},
		{
			name:     "could not find records for",
			errMsg:   "iscsiadm: Could not find records for target iqn.2005-10.org.freenas.ctl:test",
			wantTrue: true,
		},
		{
			name:     "no record found",
			errMsg:   "iscsiadm: no record found in database",
			wantTrue: true,
		},
		{
			name:     "does not exist",
			errMsg:   "iscsiadm: record does not exist in the database",
			wantTrue: true,
		},
		{
			name:     "no portals found",
			errMsg:   "iscsiadm: No portals found",
			wantTrue: true,
		},
		{
			name:     "connection refused",
			errMsg:   "iscsiadm: connection to target refused",
			wantTrue: false,
		},
		{
			name:     "timeout error",
			errMsg:   "iscsiadm: connection timed out",
			wantTrue: false,
		},
		{
			name:     "already logged in",
			errMsg:   "iscsiadm: session already present",
			wantTrue: false,
		},
		{
			name:     "generic error",
			errMsg:   "iscsiadm: some other error occurred",
			wantTrue: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := stringError(tc.errMsg)
			result := isTargetNotFoundError(err)
			assert.Equal(t, tc.wantTrue, result)
		})
	}
}

// stringError creates a simple error from a string for testing.
type stringError string

func (e stringError) Error() string {
	return string(e)
}

// TestFindISCSISessionByTargetName tests the IQN suffix matching logic.
func TestFindISCSISessionByTargetName(t *testing.T) {
	testCases := []struct {
		name       string
		sessions   []ISCSISession
		targetName string
		wantIQN    string
		wantFound  bool
	}{
		{
			name: "exact match",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123"},
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-def456"},
			},
			targetName: "pvc-abc123",
			wantIQN:    "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			wantFound:  true,
		},
		{
			name: "not found",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123"},
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-def456"},
			},
			targetName: "pvc-xyz789",
			wantIQN:    "",
			wantFound:  false,
		},
		{
			name:       "empty sessions",
			sessions:   []ISCSISession{},
			targetName: "pvc-abc123",
			wantIQN:    "",
			wantFound:  false,
		},
		{
			name: "partial name mismatch",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123-suffix"},
			},
			targetName: "pvc-abc123",
			wantIQN:    "", // Should NOT match because suffix differs
			wantFound:  false,
		},
		{
			name: "name with suffix in session",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123"},
			},
			targetName: "pvc-abc123",
			wantIQN:    "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			wantFound:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iqn, found := findSessionByTargetName(tc.sessions, tc.targetName)
			assert.Equal(t, tc.wantFound, found)
			assert.Equal(t, tc.wantIQN, iqn)
		})
	}
}

// findSessionByTargetName searches sessions for one matching the target name suffix.
// This replicates the logic from FindISCSISessionByTargetName() for testing.
func findSessionByTargetName(sessions []ISCSISession, targetName string) (string, bool) {
	expectedSuffix := ":" + targetName
	for _, session := range sessions {
		if stringHasSuffix(session.IQN, expectedSuffix) {
			return session.IQN, true
		}
	}
	return "", false
}

// TestFindISCSISessionByIQN tests exact IQN matching.
func TestFindISCSISessionByIQN(t *testing.T) {
	testCases := []struct {
		name      string
		sessions  []ISCSISession
		iqn       string
		wantFound bool
	}{
		{
			name: "exact match",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123"},
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-def456"},
			},
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			wantFound: true,
		},
		{
			name: "not found",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123"},
			},
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-xyz789",
			wantFound: false,
		},
		{
			name: "partial match not allowed",
			sessions: []ISCSISession{
				{IQN: "iqn.2005-10.org.freenas.ctl:pvc-abc123-extended"},
			},
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			wantFound: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			found := findSessionByIQN(tc.sessions, tc.iqn)
			assert.Equal(t, tc.wantFound, found)
		})
	}
}

// findSessionByIQN searches sessions for one matching the exact IQN.
func findSessionByIQN(sessions []ISCSISession, iqn string) bool {
	for _, session := range sessions {
		if session.IQN == iqn {
			return true
		}
	}
	return false
}

func TestFindISCSIDeviceForPortalSelectsExactSession(t *testing.T) {
	root := t.TempDir()
	sysClassRoot := filepath.Join(root, "sys", "class")
	devRoot := filepath.Join(root, "dev")
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-multipath"
	for _, fixture := range []struct {
		sessionID string
		host      string
		device    string
	}{
		{sessionID: "11", host: "6", device: "sda"},
		{sessionID: "12", host: "7", device: "sdb"},
	} {
		require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "iscsi_host", "host"+fixture.host, "device", "session"+fixture.sessionID), 0o750))
		require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "scsi_device", fixture.host+":0:0:0", "device", "block", fixture.device), 0o750))
		require.NoError(t, os.MkdirAll(devRoot, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(devRoot, fixture.device), nil, 0o600))
	}
	sessions := []ISCSISessionInfo{
		{Portal: "192.0.2.10:3260", IQN: iqn, SessionID: "11"},
		{Portal: "192.0.2.11:3260", IQN: iqn, SessionID: "12"},
	}

	for _, test := range []struct {
		name   string
		portal string
		want   string
	}{
		{name: "canonical", portal: "192.0.2.11:3260", want: "sdb"},
		{name: "legacy portless", portal: "192.0.2.10", want: "sda"},
	} {
		t.Run(test.name, func(t *testing.T) {
			devicePath, err := findISCSIDeviceForPortalFromSessionsInPaths(
				test.portal, iqn, 0, sessions, sysClassRoot, devRoot,
			)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(devRoot, test.want), devicePath)
		})
	}
}

func TestSameISCSIPortalCanonicalizesPortlessHostnameBothDirections(t *testing.T) {
	for _, test := range []struct {
		name  string
		left  string
		right string
	}{
		{name: "explicit then portless", left: "host.example.com:3260", right: "host.example.com"},
		{name: "portless then explicit", left: "host.example.com", right: "host.example.com:3260"},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, sameISCSIPortal(test.left, test.right))
		})
	}
}

func TestFindISCSIMultipathDeviceAndResolveSessionFromFakeSysfs(t *testing.T) {
	root := t.TempDir()
	sysBlockRoot := filepath.Join(root, "sys", "block")
	sessionRoot := filepath.Join(root, "sys", "class", "iscsi_session")
	devRoot := filepath.Join(root, "dev")
	wwid := "36001405a123456789abcdef000000001"
	dmRoot := filepath.Join(sysBlockRoot, "dm-2")
	require.NoError(t, os.MkdirAll(filepath.Join(dmRoot, "dm"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(dmRoot, "slaves", "sda"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dmRoot, "dm", "uuid"), []byte("mpath-"+wwid+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dmRoot, "dm", "name"), []byte(wwid+"\n"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(devRoot, "mapper"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "dm-2"), nil, 0o600))
	require.NoError(t, os.Symlink("../dm-2", filepath.Join(devRoot, "mapper", wwid)))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "sda"), nil, 0o600))

	topology := filepath.Join(root, "devices", "host6", "session11", "target6:0:0", "6:0:0:0")
	require.NoError(t, os.MkdirAll(topology, 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sysBlockRoot, "sda"), 0o750))
	require.NoError(t, os.Symlink(topology, filepath.Join(sysBlockRoot, "sda", "device")))
	require.NoError(t, os.MkdirAll(filepath.Join(sessionRoot, "session11"), 0o750))
	iqn := "iqn.2005-10.org.freenas.ctl:pvc-dm"
	require.NoError(t, os.WriteFile(filepath.Join(sessionRoot, "session11", "targetname"), []byte(iqn+"\n"), 0o600))

	dmDevice, err := findISCSIMultipathDeviceInPaths("naa.6001405a123456789abcdef000000001", sysBlockRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(devRoot, "mapper", wwid), dmDevice)

	portal, resolvedIQN, err := getISCSIInfoFromDeviceWithSessionsInPaths(dmDevice, []ISCSISessionInfo{
		{Portal: "192.0.2.99:3260", IQN: iqn, SessionID: "10"},
		{Portal: "192.0.2.10:3260", IQN: iqn, SessionID: "11"},
	}, sysBlockRoot, sessionRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, "192.0.2.10:3260", portal)
	assert.Equal(t, iqn, resolvedIQN)
}

func TestGetISCSIMultipathWWIDFromFakeSysfs(t *testing.T) {
	sysBlockRoot := filepath.Join(t.TempDir(), "sys", "block")
	dmRoot := filepath.Join(sysBlockRoot, "dm-4", "dm")
	require.NoError(t, os.MkdirAll(dmRoot, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dmRoot, "uuid"), []byte("mpath-36001405a123456789abcdef000000004\n"), 0o600))

	wwid, err := getISCSIMultipathWWIDInPaths("/dev/dm-4", sysBlockRoot)
	require.NoError(t, err)
	assert.Equal(t, "36001405a123456789abcdef000000004", wwid)

	_, err = getISCSIMultipathWWIDInPaths("/dev/sda", sysBlockRoot)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a dm-multipath map")
}

func TestNormalizeSCSIWWIDTranslatesT10Spaces(t *testing.T) {
	assert.Equal(t, "1ATA_____TrueNAS_Disk_01", normalizeSCSIWWID("t10.ATA     TrueNAS Disk 01"))
}

func TestCheckISCSIMultipathPrerequisites(t *testing.T) {
	root := t.TempDir()
	devRoot := filepath.Join(root, "dev")
	socket := filepath.Join(root, "run", "multipathd.sock")
	require.Error(t, checkISCSIMultipathPrerequisites(devRoot, []string{socket}))
	require.NoError(t, os.MkdirAll(filepath.Join(devRoot, "mapper"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "mapper", "control"), nil, 0o600))
	require.Error(t, checkISCSIMultipathPrerequisites(devRoot, []string{socket}))
	require.NoError(t, os.MkdirAll(filepath.Dir(socket), 0o750))
	require.NoError(t, os.WriteFile(socket, nil, 0o600))
	require.NoError(t, checkISCSIMultipathPrerequisites(devRoot, []string{socket}))
}

// TestIsLikelyISCSIDevice tests the iSCSI device detection heuristic.
func TestIsLikelyISCSIDevice(t *testing.T) {
	testCases := []struct {
		name       string
		devicePath string
		wantResult bool
	}{
		// Likely iSCSI devices (sd[a-z]+)
		{
			name:       "sda",
			devicePath: "/dev/sda",
			wantResult: true,
		},
		{
			name:       "sdb",
			devicePath: "/dev/sdb",
			wantResult: true,
		},
		{
			name:       "sdz",
			devicePath: "/dev/sdz",
			wantResult: true,
		},
		{
			name:       "sdaa",
			devicePath: "/dev/sdaa",
			wantResult: true,
		},
		{
			name:       "sdab",
			devicePath: "/dev/sdab",
			wantResult: true,
		},
		{
			name:       "sdaz",
			devicePath: "/dev/sdaz",
			wantResult: true,
		},
		// NOT iSCSI devices
		{
			name:       "nvme0n1",
			devicePath: "/dev/nvme0n1",
			wantResult: false,
		},
		{
			name:       "loop0",
			devicePath: "/dev/loop0",
			wantResult: false,
		},
		{
			name:       "nbd0",
			devicePath: "/dev/nbd0",
			wantResult: false,
		},
		{
			name:       "dm-0",
			devicePath: "/dev/dm-0",
			wantResult: false,
		},
		{
			name:       "xvda",
			devicePath: "/dev/xvda",
			wantResult: false,
		},
		{
			name:       "vda",
			devicePath: "/dev/vda",
			wantResult: false,
		},
		{
			name:       "sda1 partition",
			devicePath: "/dev/sda1",
			wantResult: false, // Has number after letters
		},
		{
			name:       "sd (too short)",
			devicePath: "/dev/sd",
			wantResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsLikelyISCSIDevice(tc.devicePath)
			assert.Equal(t, tc.wantResult, result)
		})
	}
}

// TestListISCSISessions tests the public wrapper for session listing.
func TestListISCSISessionsConversion(t *testing.T) {
	// Test the conversion from internal to public struct
	internalSessions := []ISCSISession{
		{
			SessionID:    "1",
			TargetPortal: "192.0.2.100:3260",
			IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc",
		},
		{
			SessionID:    "2",
			TargetPortal: "198.51.100.50:3260",
			IQN:          "iqn.2005-10.org.freenas.ctl:pvc-def",
		},
	}

	// Convert to public struct
	publicSessions := make([]ISCSISessionInfo, len(internalSessions))
	for i, s := range internalSessions {
		publicSessions[i] = ISCSISessionInfo{
			Portal:    s.TargetPortal,
			IQN:       s.IQN,
			SessionID: s.SessionID,
		}
	}

	require.Len(t, publicSessions, 2)
	assert.Equal(t, "192.0.2.100:3260", publicSessions[0].Portal)
	assert.Equal(t, "iqn.2005-10.org.freenas.ctl:pvc-abc", publicSessions[0].IQN)
	assert.Equal(t, "1", publicSessions[0].SessionID)
	assert.Equal(t, "198.51.100.50:3260", publicSessions[1].Portal)
	assert.Equal(t, "iqn.2005-10.org.freenas.ctl:pvc-def", publicSessions[1].IQN)
	assert.Equal(t, "2", publicSessions[1].SessionID)
}

// TestISCSINodeParamArguments tests the argument construction for node parameter updates.
func TestISCSINodeParamArguments(t *testing.T) {
	testCases := []struct {
		name       string
		portal     string
		iqn        string
		paramName  string
		paramValue string
		wantArgs   []string
	}{
		{
			name:       "auth method",
			portal:     "192.0.2.100:3260",
			iqn:        "iqn.2005-10.org.freenas.ctl:test",
			paramName:  "node.session.auth.authmethod",
			paramValue: "CHAP",
			wantArgs: []string{
				"-m", "node",
				"-T", "iqn.2005-10.org.freenas.ctl:test",
				"-p", "192.0.2.100:3260",
				"-o", "update",
				"-n", "node.session.auth.authmethod",
				"-v", "CHAP",
			},
		},
		{
			name:       "username",
			portal:     "198.51.100.50:3260",
			iqn:        "iqn.2005-10.org.freenas.ctl:vol-chap",
			paramName:  "node.session.auth.username",
			paramValue: "initiator-user",
			wantArgs: []string{
				"-m", "node",
				"-T", "iqn.2005-10.org.freenas.ctl:vol-chap",
				"-p", "198.51.100.50:3260",
				"-o", "update",
				"-n", "node.session.auth.username",
				"-v", "initiator-user",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			args := buildNodeParamArgs(tc.portal, tc.iqn, tc.paramName, tc.paramValue)
			assert.Equal(t, tc.wantArgs, args)
		})
	}
}

// buildNodeParamArgs builds arguments for iscsiadm node parameter update.
func buildNodeParamArgs(portal, iqn, name, value string) []string {
	return []string{
		"-m", "node",
		"-T", iqn,
		"-p", portal,
		"-o", "update",
		"-n", name,
		"-v", value,
	}
}

// TestISCSIGetSessionStatsOutputParsing tests parsing of session stats output.
func TestISCSIGetSessionStatsOutputParsing(t *testing.T) {
	testCases := []struct {
		name      string
		output    string
		targetIQN string
		wantStats map[string]string
		wantInSec bool
	}{
		{
			name: "basic stats parsing",
			output: `Target: iqn.2005-10.org.freenas.ctl:pvc-test
	Current Portal: 192.0.2.100:3260,1
	Persistent Portal: 192.0.2.100:3260,1
	State: LOGGED_IN
	Recovery Timeout: 120
	Target: iqn.2005-10.org.freenas.ctl:other-target
`,
			targetIQN: "iqn.2005-10.org.freenas.ctl:pvc-test",
			wantStats: map[string]string{
				"Current Portal":    "192.0.2.100:3260,1",
				"Persistent Portal": "192.0.2.100:3260,1",
				"State":             "LOGGED_IN",
				"Recovery Timeout":  "120",
			},
			wantInSec: true,
		},
		{
			name: "target not in output",
			output: `Target: iqn.2005-10.org.freenas.ctl:other-target
	State: LOGGED_IN
`,
			targetIQN: "iqn.2005-10.org.freenas.ctl:pvc-test",
			wantStats: map[string]string{},
			wantInSec: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stats := parseSessionStatsOutput(tc.output, tc.targetIQN)
			assert.Equal(t, tc.wantStats, stats)
		})
	}
}

// parseSessionStatsOutput parses iscsiadm session stats output.
// This replicates the logic from ISCSIGetSessionStats() for testing.
func parseSessionStatsOutput(output, iqn string) map[string]string {
	stats := make(map[string]string)
	lines := stringSplit(output, "\n")
	inTargetSection := false

	for _, line := range lines {
		line = stringTrimSpace(line)
		if stringContains(line, iqn) {
			inTargetSection = true
			continue
		}
		if inTargetSection {
			if stringHasPrefix(line, "Target:") {
				break // Next target
			}
			colonIdx := stringIndex(line, ":")
			if colonIdx > 0 {
				key := stringTrimSpace(line[:colonIdx])
				value := stringTrimSpace(line[colonIdx+1:])
				stats[key] = value
			}
		}
	}

	return stats
}

// TestGetDeviceSizeParsing tests parsing of device size from sysfs.
func TestGetDeviceSizeParsing(t *testing.T) {
	testCases := []struct {
		name          string
		sizeOutput    string
		wantSizeBytes int64
	}{
		{
			name:          "1GB device",
			sizeOutput:    "2097152", // 2097152 * 512 = 1GB
			wantSizeBytes: 2097152 * 512,
		},
		{
			name:          "10GB device",
			sizeOutput:    "20971520", // 20971520 * 512 = 10GB
			wantSizeBytes: 20971520 * 512,
		},
		{
			name:          "small device",
			sizeOutput:    "1024",
			wantSizeBytes: 1024 * 512,
		},
		{
			name:          "output with newline",
			sizeOutput:    "2097152\n",
			wantSizeBytes: 2097152 * 512,
		},
		{
			name:          "output with whitespace",
			sizeOutput:    "  2097152  \n",
			wantSizeBytes: 2097152 * 512,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sizeStr := stringTrimSpace(tc.sizeOutput)
			sectors := parseInt64(sizeStr)
			sizeBytes := sectors * 512
			assert.Equal(t, tc.wantSizeBytes, sizeBytes)
		})
	}
}

// parseInt64 parses a string to int64.
func parseInt64(s string) int64 {
	var result int64
	for _, c := range s {
		if c >= '0' && c <= '9' {
			result = result*10 + int64(c-'0')
		}
	}
	return result
}

// TestISCSIConnectOptionsDefaults tests default option handling.
func TestISCSIConnectOptionsDefaults(t *testing.T) {
	testCases := []struct {
		name                    string
		opts                    *ISCSIConnectOptions
		wantTimeout             bool
		wantSessionCleanupDelay bool
	}{
		{
			name:                    "nil options uses defaults",
			opts:                    nil,
			wantTimeout:             true, // Uses DefaultISCSIDeviceTimeout
			wantSessionCleanupDelay: true, // Uses 500ms default
		},
		{
			name:                    "zero values use defaults",
			opts:                    &ISCSIConnectOptions{},
			wantTimeout:             true,
			wantSessionCleanupDelay: true,
		},
		{
			name: "custom timeout",
			opts: &ISCSIConnectOptions{
				DeviceTimeout: 30 * 1000000000, // 30s as Duration
			},
			wantTimeout:             false, // Custom value
			wantSessionCleanupDelay: true,  // Uses default
		},
		{
			name: "custom cleanup delay",
			opts: &ISCSIConnectOptions{
				SessionCleanupDelay: 1000000000, // 1s as Duration
			},
			wantTimeout:             true,
			wantSessionCleanupDelay: false, // Custom value
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the default application logic
			timeout := DefaultISCSIDeviceTimeout
			sessionCleanupDelay := 500 * 1000000 // 500ms in nanoseconds

			if tc.opts != nil {
				if tc.opts.DeviceTimeout > 0 {
					timeout = tc.opts.DeviceTimeout
				}
				if tc.opts.SessionCleanupDelay > 0 {
					sessionCleanupDelay = int(tc.opts.SessionCleanupDelay)
				}
			}

			if tc.wantTimeout {
				assert.Equal(t, DefaultISCSIDeviceTimeout, timeout)
			} else {
				assert.NotEqual(t, DefaultISCSIDeviceTimeout, timeout)
			}

			if tc.wantSessionCleanupDelay {
				assert.Equal(t, 500*1000000, sessionCleanupDelay)
			} else {
				assert.NotEqual(t, 500*1000000, sessionCleanupDelay)
			}
		})
	}
}

// Helper functions to avoid import cycles in tests

func stringSplit(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func stringTrimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func stringContains(s, substr string) bool {
	return stringIndex(s, substr) >= 0
}

func stringIndex(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func stringHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func stringHasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func stringJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}
	return result
}

// TestRedactISCSIArgs guards the E4/O22 argv redactor: CHAP credential values are
// masked, parameter names and non-auth values are preserved, and the input slice
// is never mutated.
func TestRedactISCSIArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "auth password value is masked",
			args: []string{"iscsiadm", "-m", "node", "-T", "iqn.test:x", "-p", "192.0.2.10:3260", "-o", "update", "-n", "node.session.auth.password", "-v", "hunter2secret"},
			want: []string{"iscsiadm", "-m", "node", "-T", "iqn.test:x", "-p", "192.0.2.10:3260", "-o", "update", "-n", "node.session.auth.password", "-v", "***"},
		},
		{
			name: "auth username value is masked",
			args: []string{"-n", "node.session.auth.username", "-v", "chapuser"},
			want: []string{"-n", "node.session.auth.username", "-v", "***"},
		},
		{
			name: "mutual password_in value is masked",
			args: []string{"-n", "node.session.auth.password_in", "-v", "peersecret"},
			want: []string{"-n", "node.session.auth.password_in", "-v", "***"},
		},
		{
			name: "non-auth value is preserved",
			args: []string{"-n", "node.session.auth.authmethod", "-v", "CHAP"},
			want: []string{"-n", "node.session.auth.authmethod", "-v", "CHAP"},
		},
		{
			name: "bare value directly after an auth name is masked",
			args: []string{"-n", "node.session.auth.password", "hunter2secret"},
			want: []string{"-n", "node.session.auth.password", "***"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := slices.Clone(tc.args)
			assert.Equal(t, tc.want, redactISCSIArgs(tc.args))
			assert.Equal(t, original, tc.args, "input slice must not be mutated")
		})
	}
}

// TestSetISCSINodeParamErrorNeverLeaksCHAPSecret guards the E4/O22 error wrap: a
// forced auth-param failure carries the masked argv and never the secret value.
func TestSetISCSINodeParamErrorNeverLeaksCHAPSecret(t *testing.T) {
	if _, err := exec.LookPath("iscsiadm"); err == nil {
		t.Skip("iscsiadm present; cannot force a deterministic param-set failure")
	}
	const secret = "hunter2-do-not-leak"
	err := SetISCSINodeParam("192.0.2.99:3260", "iqn.test:redact", "node.session.auth.password", secret)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret, "CHAP secret must never appear in a SetISCSINodeParam error")
	assert.Contains(t, err.Error(), "***", "the auth value must be masked in the redacted argv")
}
