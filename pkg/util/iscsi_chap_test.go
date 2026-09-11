package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureISCSICHAPWithContextOneWay(t *testing.T) {
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	record := stubISCSINodeDBFastPath(t)
	var calls [][]string

	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	creds := &ISCSICHAPCredentials{Username: "chapuser", Password: "chapsecret123"}
	require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(), portal, iqn, creds))

	// The credential VALUE is deliberately absent from this argv: it goes into
	// the node record instead (S-01). Only the non-secret params are exec'd.
	require.Equal(t, [][]string{
		{"-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", "node.session.auth.authmethod", "-v", "CHAP"},
		{"-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", "node.session.auth.username", "-v", "chapuser"},
	}, calls)
	contents, err := os.ReadFile(record)
	require.NoError(t, err)
	assert.Contains(t, string(contents), "node.session.auth.password = chapsecret123")
}

func TestConfigureISCSICHAPWithContextMutual(t *testing.T) {
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	record := stubISCSINodeDBFastPath(t)
	var calls [][]string

	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	creds := &ISCSICHAPCredentials{
		Username:       "chapuser",
		Password:       "chapsecret123",
		MutualUsername: "peeruser",
		MutualPassword: "peersecret456",
		Mutual:         true,
	}
	require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(), portal, iqn, creds))

	// Both credential VALUES are absent from argv; both usernames remain on it.
	require.Equal(t, [][]string{
		{"-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", "node.session.auth.authmethod", "-v", "CHAP"},
		{"-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", "node.session.auth.username", "-v", "chapuser"},
		{"-m", "node", "-T", iqn, "-p", portal, "-o", "update", "-n", "node.session.auth.username_in", "-v", "peeruser"},
	}, calls)
	contents, err := os.ReadFile(record)
	require.NoError(t, err)
	assert.Contains(t, string(contents), "node.session.auth.password = chapsecret123")
	assert.Contains(t, string(contents), "node.session.auth.password_in = peersecret456")
}

func TestConfigureISCSICHAPWithContextNilIsNoop(t *testing.T) {
	called := false
	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, _ ...string) ([]byte, error) {
		called = true
		return nil, nil
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(), "192.0.2.22:3260", "iqn.test:x", nil))
	assert.False(t, called, "nil credentials must not touch iscsiadm")
}

func TestConfigureISCSICHAPWithContextErrorCarriesParamNameNotValue(t *testing.T) {
	const secret = "supersecretvalue"
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	stubISCSINodeDBFastPath(t)

	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
		// Fail on the username write — the first param whose value is credential
		// material and still travels through iscsiadm — and make BOTH the stdout
		// AND the error echo the submitted value, the worst case where iscsiadm or
		// a wrapper/exec-logger reflects the argv. The returned error must still be
		// clean (parameter name + exit class only).
		if slices.Contains(args, "node.session.auth.username") {
			return []byte("iscsiadm set -v " + secret + " failed"),
				fmt.Errorf("exec failed running -v %s", secret)
		}
		return nil, nil
	}
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	creds := &ISCSICHAPCredentials{Username: secret, Password: "chapsecret123"}
	err := ConfigureISCSICHAPWithContext(context.Background(), portal, iqn, creds)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node.session.auth.username", "the parameter NAME is safe to surface")
	assert.NotContains(t, err.Error(), secret, "credential value must never appear in errors (stdout or raw exec error)")
}

// TestISCSIConnectPostDiscoveryAuthFailureIsClassified is the F-07 regression:
// first login reports target-not-found, discovery succeeds, and the retry login
// fails authentication. The post-discovery auth failure must be classified as
// ErrISCSIAuthFailure (redacted), not returned as a generic login error.
func TestISCSIConnectPostDiscoveryAuthFailureIsClassified(t *testing.T) {
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	const secret = "wrongsecret12"
	loginAttempts := 0
	stubISCSINodeDBFastPath(t)

	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		if slices.Contains(args, "--login") {
			loginAttempts++
			if loginAttempts == 1 {
				// First login: target node record not yet propagated.
				return []byte("iscsiadm: No records found"), errors.New("exit status 21")
			}
			// Post-discovery retry: the credential is wrong. Echo the secret in the
			// raw output to prove it is not surfaced in the returned error.
			return []byte("Could not login: authentication failed (-v " + secret + ")"),
				fmt.Errorf("authentication failed: -v %s", secret)
		}
		return nil, nil
	})

	_, err := ISCSIConnectWithOptionsAndSessions(
		context.Background(), portal, iqn, 0,
		&ISCSIConnectOptions{CHAP: &ISCSICHAPCredentials{Username: "u", Password: secret}},
		nil,
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrISCSIAuthFailure), "post-discovery auth failure must be classified, got: %v", err)
	assert.NotContains(t, err.Error(), secret, "credential must never appear in the returned error")
	assert.GreaterOrEqual(t, loginAttempts, 2, "must have retried after discovery before classifying")
}

func TestISCSIConnectAuthFailureShortCircuitsDiscovery(t *testing.T) {
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	var calls [][]string
	loginAttempts := 0
	stubISCSINodeDBFastPath(t)

	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		if slices.Contains(args, "--login") {
			loginAttempts++
			return []byte("iscsiadm: Could not login: authentication failed"), errors.New("exit status 1")
		}
		return nil, nil
	})

	_, err := ISCSIConnectWithOptionsAndSessions(
		context.Background(), portal, iqn, 0,
		&ISCSIConnectOptions{CHAP: &ISCSICHAPCredentials{Username: "u", Password: "wrongsecret12"}},
		nil,
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrISCSIAuthFailure), "auth failure must be classified, got: %v", err)
	assert.Equal(t, 1, loginAttempts, "auth failure must not retry login")
	assert.False(t, hasISCSIAdmMode(calls, "discovery"), "auth failure must not enter SendTargets discovery")
}

func TestISCSIConnectAppliesCHAPBeforeLogin(t *testing.T) {
	portal := iscsiFixturePortal
	iqn := iscsiFixtureFastPathIQN
	var calls [][]string
	stubISCSINodeDBFastPath(t)

	stubISCSIConnectDependencies(t, func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, slices.Clone(args))
		return nil, nil
	})

	_, err := ISCSIConnectWithOptionsAndSessions(
		context.Background(), portal, iqn, 0,
		&ISCSIConnectOptions{CHAP: &ISCSICHAPCredentials{Username: "chapuser", Password: "chapsecret123"}},
		nil,
	)
	require.NoError(t, err)

	// The auth params must be written after the node record and before --login.
	loginIdx := -1
	authMethodIdx := -1
	for i, args := range calls {
		if slices.Contains(args, "--login") && loginIdx == -1 {
			loginIdx = i
		}
		if slices.Contains(args, "node.session.auth.authmethod") {
			authMethodIdx = i
		}
	}
	require.NotEqual(t, -1, authMethodIdx, "CHAP authmethod must be applied")
	require.NotEqual(t, -1, loginIdx, "login must happen")
	assert.Less(t, authMethodIdx, loginIdx, "CHAP params must be set before login")

	// S-01, at the production entry point: no argv the connect path hands to the
	// exec seam may carry the credential.
	for _, args := range calls {
		for _, arg := range args {
			assert.NotContains(t, arg, "chapsecret123", "CHAP password reached iscsiadm argv: %v", redactISCSIArgs(args))
		}
	}
}

func TestIsAuthFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"authorization failure", errors.New("iscsiadm: authorization failure"), true},
		{"authentication failed", errors.New("Could not login: authentication failed"), true},
		{"login failed", errors.New("login failed"), true},
		{"uppercased", errors.New("AUTHENTICATION FAILED"), true},
		{"target not found", errors.New("iscsiadm: No records found"), false},
		{"transport", errors.New("transport failure"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isAuthFailure(tc.err))
		})
	}
}
