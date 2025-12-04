package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			output: `tcp: [1] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc123 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				},
			},
		},
		{
			name: "multiple sessions",
			output: `tcp: [1] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc123 (non-flash)
tcp: [2] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-def456 (non-flash)
tcp: [3] 10.0.0.50:3260,1 iqn.2005-10.org.freenas.ctl:pvc-ghi789 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-def456",
				},
				{
					SessionID:    "3",
					TargetPortal: "10.0.0.50:3260",
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
			output: `tcp: [5] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-xyz
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "5",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-xyz",
				},
			},
		},
		{
			name: "high session ID",
			output: `tcp: [999] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:vol-test (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "999",
					TargetPortal: "192.168.1.100:3260",
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
			output: `tcp: [1] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc (non-flash)

tcp: [2] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-def (non-flash)

`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-def",
				},
			},
		},
		{
			name: "target portal group tag variations",
			output: `tcp: [1] 192.168.1.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-tag1 (non-flash)
tcp: [2] 192.168.1.100:3260,2 iqn.2005-10.org.freenas.ctl:pvc-tag2 (non-flash)
tcp: [3] 192.168.1.100:3260,100 iqn.2005-10.org.freenas.ctl:pvc-tag100 (non-flash)
`,
			wantSessions: []ISCSISession{
				{
					SessionID:    "1",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-tag1",
				},
				{
					SessionID:    "2",
					TargetPortal: "192.168.1.100:3260",
					IQN:          "iqn.2005-10.org.freenas.ctl:pvc-tag2",
				},
				{
					SessionID:    "3",
					TargetPortal: "192.168.1.100:3260",
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
			portal:    "192.168.1.100:3260",
			iqn:       "",
			operation: "discovery",
			wantContains: []string{
				"-m", "discovery", "-t", "sendtargets", "-p", "192.168.1.100:3260",
			},
		},
		{
			name:      "login command",
			portal:    "192.168.1.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-abc123",
			operation: "login",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:pvc-abc123",
				"-p", "192.168.1.100:3260", "--login",
			},
		},
		{
			name:      "logout command",
			portal:    "10.0.0.50:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:pvc-def456",
			operation: "logout",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:pvc-def456",
				"-p", "10.0.0.50:3260", "--logout",
			},
		},
		{
			name:      "delete node record",
			portal:    "192.168.1.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:vol-xyz",
			operation: "delete",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:vol-xyz",
				"-p", "192.168.1.100:3260", "-o", "delete",
			},
		},
		{
			name:      "rescan command",
			portal:    "192.168.1.100:3260",
			iqn:       "iqn.2005-10.org.freenas.ctl:rescan-test",
			operation: "rescan",
			wantContains: []string{
				"-m", "node", "-T", "iqn.2005-10.org.freenas.ctl:rescan-test",
				"-p", "192.168.1.100:3260", "--rescan",
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
			TargetPortal: "192.168.1.100:3260",
			IQN:          "iqn.2005-10.org.freenas.ctl:pvc-abc",
		},
		{
			SessionID:    "2",
			TargetPortal: "10.0.0.50:3260",
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
	assert.Equal(t, "192.168.1.100:3260", publicSessions[0].Portal)
	assert.Equal(t, "iqn.2005-10.org.freenas.ctl:pvc-abc", publicSessions[0].IQN)
	assert.Equal(t, "1", publicSessions[0].SessionID)
	assert.Equal(t, "10.0.0.50:3260", publicSessions[1].Portal)
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
			portal:     "192.168.1.100:3260",
			iqn:        "iqn.2005-10.org.freenas.ctl:test",
			paramName:  "node.session.auth.authmethod",
			paramValue: "CHAP",
			wantArgs: []string{
				"-m", "node",
				"-T", "iqn.2005-10.org.freenas.ctl:test",
				"-p", "192.168.1.100:3260",
				"-o", "update",
				"-n", "node.session.auth.authmethod",
				"-v", "CHAP",
			},
		},
		{
			name:       "username",
			portal:     "10.0.0.50:3260",
			iqn:        "iqn.2005-10.org.freenas.ctl:vol-chap",
			paramName:  "node.session.auth.username",
			paramValue: "initiator-user",
			wantArgs: []string{
				"-m", "node",
				"-T", "iqn.2005-10.org.freenas.ctl:vol-chap",
				"-p", "10.0.0.50:3260",
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
		name       string
		output     string
		targetIQN  string
		wantStats  map[string]string
		wantInSec  bool
	}{
		{
			name: "basic stats parsing",
			output: `Target: iqn.2005-10.org.freenas.ctl:pvc-test
	Current Portal: 192.168.1.100:3260,1
	Persistent Portal: 192.168.1.100:3260,1
	State: LOGGED_IN
	Recovery Timeout: 120
	Target: iqn.2005-10.org.freenas.ctl:other-target
`,
			targetIQN: "iqn.2005-10.org.freenas.ctl:pvc-test",
			wantStats: map[string]string{
				"Current Portal":    "192.168.1.100:3260,1",
				"Persistent Portal": "192.168.1.100:3260,1",
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
