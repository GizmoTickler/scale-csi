package util

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubISCSINodeDB builds the open-iscsi node database that iscsiEnsureNodeRecord
// would have left behind for (portal, iqn), points iscsiNodeDBRoots at it for
// the duration of the test, and returns the record path. The fixture record is
// created 0644 — the mode open-iscsi's own umask leaves — so tests can prove the
// credential write tightens it rather than inheriting an already-safe mode.
func stubISCSINodeDB(t *testing.T, portal, iqn string) string {
	t.Helper()
	root := t.TempDir()
	host, port := splitISCSIPortal(portal)
	recordDir := filepath.Join(root, "nodes", iqn, host+","+port+",1")
	require.NoError(t, os.MkdirAll(recordDir, 0o750))
	record := filepath.Join(recordDir, "default")
	require.NoError(t, os.WriteFile(record, []byte(
		"node.name = "+iqn+"\n"+
			"node.session.auth.authmethod = None\n"), 0o600))
	require.NoError(t, os.Chmod(record, 0o644)) //nolint:gosec // G302: deliberately reproduces the world-readable mode open-iscsi leaves, which the code under test must tighten

	original := iscsiNodeDBRoots
	iscsiNodeDBRoots = []string{root}
	t.Cleanup(func() { iscsiNodeDBRoots = original })
	return record
}

// TestConfigureISCSICHAPWritesSecretToNodeRecord is the landing half of the S-01
// regression: the credential the argv no longer carries must still reach the
// node record --login reads, and the record must not be world-readable.
func TestConfigureISCSICHAPWritesSecretToNodeRecord(t *testing.T) {
	const (
		portal       = "192.0.2.31:3260"
		iqn          = "iqn.2005-10.org.freenas.ctl:pvc-chap-record"
		password     = "chapsecret123"
		peerPassword = "peersecret456"
	)
	record := stubISCSINodeDB(t, portal, iqn)

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
	require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(), portal, iqn, creds))

	contents, err := os.ReadFile(record)
	require.NoError(t, err)
	assert.Contains(t, string(contents), "node.session.auth.password = "+password)
	assert.Contains(t, string(contents), "node.session.auth.password_in = "+peerPassword)
	// Pre-existing keys survive, and the stale authmethod is replaced not duplicated.
	assert.Contains(t, string(contents), "node.name = "+iqn)

	info, err := os.Stat(record)
	require.NoError(t, err)
	assert.Equal(t, fs.FileMode(0o600), info.Mode().Perm(),
		"the node record holds the CHAP secret in clear text and must not be world-readable")

	// No temp file may be left in the record directory: idbm reads every regular
	// file there as another iface record.
	entries, err := os.ReadDir(filepath.Dir(record))
	require.NoError(t, err)
	require.Len(t, entries, 1, "the record directory must contain only the iface record")
}

func TestConfigureISCSICHAPFailsClosedWithoutANodeRecord(t *testing.T) {
	originalRoots := iscsiNodeDBRoots
	iscsiNodeDBRoots = []string{t.TempDir()}
	t.Cleanup(func() { iscsiNodeDBRoots = originalRoots })

	originalRunner := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(context.Context, ...string) ([]byte, error) { return nil, nil }
	t.Cleanup(func() { iscsiAdmCombinedOutput = originalRunner })

	const password = "chapsecret123"
	err := ConfigureISCSICHAPWithContext(context.Background(), "192.0.2.32:3260", "iqn.test:norecord",
		&ISCSICHAPCredentials{Username: "chapuser", Password: password})
	require.Error(t, err, "a credential that could not be applied must never be reported as applied")
	assert.NotContains(t, err.Error(), password)
}

func TestWriteISCSINodeRecordSecretRejectsUnrepresentableValues(t *testing.T) {
	const (
		portal = "192.0.2.33:3260"
		iqn    = "iqn.2005-10.org.freenas.ctl:pvc-chap-badvalue"
	)
	record := stubISCSINodeDB(t, portal, iqn)
	before, err := os.ReadFile(record)
	require.NoError(t, err)

	for name, value := range map[string]string{
		"newline":            "secret\nnode.session.auth.username = evil",
		"comment":            "secret#comment",
		"leading whitespace": " secret1234567",
	} {
		t.Run(name, func(t *testing.T) {
			writeErr := writeISCSINodeRecordSecret(iscsiNodeDBRoots, portal, iqn, "node.session.auth.password", value)
			require.Error(t, writeErr)
			assert.NotContains(t, writeErr.Error(), value, "the rejected value must not be echoed back")
			after, readErr := os.ReadFile(record)
			require.NoError(t, readErr)
			assert.Equal(t, string(before), string(after), "a rejected value must not touch the record")
		})
	}
}

func TestSetISCSINodeRecordParamText(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "appends to a record without the key",
			input: "node.name = iqn.x\n",
			want:  "node.name = iqn.x\nnode.session.auth.password = s3cret\n",
		},
		{
			name:  "appends to an empty record",
			input: "",
			want:  "node.session.auth.password = s3cret\n",
		},
		{
			name:  "replaces an existing assignment in place",
			input: "node.name = iqn.x\nnode.session.auth.password = old\nnode.startup = manual\n",
			want:  "node.name = iqn.x\nnode.session.auth.password = s3cret\nnode.startup = manual\n",
		},
		{
			name:  "replaces every duplicate assignment",
			input: "node.session.auth.password = old1\nnode.startup = manual\nnode.session.auth.password=old2\n",
			want:  "node.session.auth.password = s3cret\nnode.startup = manual\nnode.session.auth.password = s3cret\n",
		},
		{
			name:  "leaves a commented assignment alone",
			input: "# node.session.auth.password = documented-default\n",
			want:  "# node.session.auth.password = documented-default\nnode.session.auth.password = s3cret\n",
		},
		{
			name:  "does not match a key that merely shares a prefix",
			input: "node.session.auth.password_in = peer\n",
			want:  "node.session.auth.password_in = peer\nnode.session.auth.password = s3cret\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setISCSINodeRecordParamText(tc.input, "node.session.auth.password", "s3cret"))
		})
	}
}

func TestISCSINodeRecordFiles(t *testing.T) {
	root := t.TempDir()
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-multi"
	// Two portal records for the same target: the tpgt -1 record `-o new` writes
	// and a real-tpgt record a SendTargets discovery leaves. --login may use
	// either, so both must be updated. A third record on a different portal must
	// not be touched.
	for _, dir := range []string{"192.0.2.40,3260,-1", "192.0.2.40,3260,1", "192.0.2.41,3260,1"} {
		full := filepath.Join(root, "nodes", iqn, dir)
		require.NoError(t, os.MkdirAll(full, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(full, "default"), []byte("node.name = "+iqn+"\n"), 0o600))
	}

	records, err := iscsiNodeRecordFiles([]string{root}, "192.0.2.40:3260", iqn)
	require.NoError(t, err)
	paths := make([]string, 0, len(records))
	for _, record := range records {
		assert.Equal(t, root, record.Root)
		paths = append(paths, record.Path)
	}
	slices.Sort(paths)
	assert.Equal(t, []string{
		filepath.Join(root, "nodes", iqn, "192.0.2.40,3260,-1", "default"),
		filepath.Join(root, "nodes", iqn, "192.0.2.40,3260,1", "default"),
	}, paths)

	_, err = iscsiNodeRecordFiles([]string{root}, "192.0.2.99:3260", iqn)
	require.Error(t, err)

	_, err = iscsiNodeRecordFiles([]string{root}, "192.0.2.40:3260", "../../etc")
	require.Error(t, err, "an IQN that is not a single path component must be refused")
}

func TestSplitISCSIPortal(t *testing.T) {
	cases := []struct {
		portal string
		host   string
		port   string
	}{
		{"192.0.2.40:3260", "192.0.2.40", "3260"},
		{"192.0.2.40", "192.0.2.40", "3260"},
		{"[2001:db8::1]:3260", "2001:db8::1", "3260"},
		{"[2001:db8::1]", "2001:db8::1", "3260"},
		{"nas.example.test:3261", "nas.example.test", "3261"},
	}
	for _, tc := range cases {
		t.Run(tc.portal, func(t *testing.T) {
			host, port := splitISCSIPortal(tc.portal)
			assert.Equal(t, tc.host, host)
			assert.Equal(t, tc.port, port)
		})
	}
}
