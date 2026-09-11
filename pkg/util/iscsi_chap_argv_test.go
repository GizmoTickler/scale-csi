package util

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigureISCSICHAPNeverPutsSecretOnARGV is the S-01 regression.
//
// It asserts on the ACTUAL argv handed to the exec seam — the same slice
// exec.CommandContext turns into /proc/<pid>/cmdline — rather than on any
// helper's return value. The node DaemonSet runs hostPID: true and the
// production iscsiadm is a bash wrapper that re-execs nsenter, so a CHAP
// credential on this argv is simultaneously readable from three host-namespace
// /proc entries by any pod admitted with hostPID. redactISCSIArgs and
// sanitizedExecClass only ever covered the log and error channels.
//
// The node database is deliberately NOT redirected here: this test is about
// what leaves the process on argv, so it tolerates the credential write itself
// failing for want of a node record (TestConfigureISCSICHAPWritesSecretToNodeRecord
// covers the landing side). That also keeps it meaningful against the pre-fix
// implementation, which reached argv before it could fail anywhere.
func TestConfigureISCSICHAPNeverPutsSecretOnARGV(t *testing.T) {
	const (
		portal       = "192.0.2.30:3260"
		iqn          = "iqn.2005-10.org.freenas.ctl:pvc-chap-argv-leak"
		password     = "chapsecret123"
		peerPassword = "peersecret456"
	)

	var calls [][]string
	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	creds := &ISCSICHAPCredentials{
		Username:       "chapuser",
		Password:       password,
		MutualUsername: "peeruser",
		MutualPassword: peerPassword,
		Mutual:         true,
	}
	// The returned error is intentionally not asserted on; see the doc comment.
	_ = ConfigureISCSICHAPWithContext(context.Background(), portal, iqn, creds)

	// Guard against a vacuous pass: the non-secret parameters must still be
	// applied through iscsiadm, so there IS an argv to inspect.
	require.NotEmpty(t, calls, "CHAP configuration must still drive iscsiadm for the non-secret params")
	assert.True(t, argvContains(calls, "node.session.auth.authmethod"), "authmethod must stay on argv")
	assert.True(t, argvContains(calls, "node.session.auth.username"), "username must stay on argv")

	for _, args := range calls {
		for _, arg := range args {
			assert.NotContains(t, arg, password,
				"CHAP password reached iscsiadm argv (world-readable via /proc on a hostPID node plugin): %v",
				redactISCSIArgs(args))
			assert.NotContains(t, arg, peerPassword,
				"mutual CHAP peer secret reached iscsiadm argv: %v", redactISCSIArgs(args))
		}
	}
}

func argvContains(calls [][]string, token string) bool {
	for _, args := range calls {
		if slices.Contains(args, token) {
			return true
		}
	}
	return false
}
