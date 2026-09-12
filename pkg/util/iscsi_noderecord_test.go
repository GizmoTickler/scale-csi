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

// The node database under testdata/iscsi-node-db was written by the real
// iscsiadm — the same version the cluster's nodes run — not drawn by hand. See
// its README.md for the capture method and hack/capture-iscsi-node-db-fixture.sh
// to regenerate it. These names identify what it contains.
const (
	// iscsiFixtureFastPathIQN carries OLD-STYLE records: plain files at
	// nodes/<iqn>/<host>,<port>. This is what iscsiEnsureNodeRecord's
	// `-o new -T <iqn> -p <portal>` (no tpgt) actually leaves behind.
	iscsiFixtureFastPathIQN = "iqn.2005-10.org.freenas.ctl:pvc-fastpath"
	// iscsiFixtureDiscoveredIQN carries NEW-STYLE records: a
	// <host>,<port>,<tpgt> directory per portal, holding one file per bound
	// iface. This is what a portal with a real tpgt (a SendTargets discovery)
	// produces.
	iscsiFixtureDiscoveredIQN = "iqn.2005-10.org.freenas.ctl:pvc-discovered"
	// iscsiFixturePortal is recorded for both IQNs above; iscsiFixtureOtherPortal
	// is recorded too and must never be touched by a write aimed at the first.
	iscsiFixturePortal      = "192.0.2.40:3260"
	iscsiFixtureOtherPortal = "192.0.2.41:3260"
	// iscsiFixtureIPv6Portal is recorded (fast-path IQN only) as
	// `2001:db8::31,3260` — iscsiadm strips the brackets.
	iscsiFixtureIPv6Portal = "[2001:db8::31]:3260"
	// iscsiNodeRecordTempPrefix is the temp-file prefix rewriteISCSINodeRecord
	// stages under; no test may leave one behind, because idbm would read a
	// stray file in a record directory as another record.
	iscsiNodeRecordTempPrefix = ".scale-csi-node-"
)

// stubISCSINodeDB materializes the captured node database into a temp directory,
// points iscsiNodeDBRoots at it for the duration of the test, and returns the
// root.
//
// The copy is made deliberately LOOSER than the capture: the real iscsiadm
// creates record files 0600 and record directories 0700 (at both umask 022 and
// umask 000 — see the fixture MANIFEST), so materializing them 0644/0755 is what
// makes "the credential write installs 0600" an assertion about the code under
// test rather than about an inherited default. Git could not have carried the
// captured modes anyway; it tracks only the exec bit.
func stubISCSINodeDB(t *testing.T) string {
	t.Helper()
	root := materializeISCSINodeDB(t)
	original := iscsiNodeDBRoots
	iscsiNodeDBRoots = []string{root}
	t.Cleanup(func() { iscsiNodeDBRoots = original })
	return root
}

// materializeISCSINodeDB copies the captured node database into a fresh temp
// directory and returns it, WITHOUT repointing iscsiNodeDBRoots. Production
// probes two roots, so a test that needs to model one root going bad while the
// other stays good needs two independent copies of the capture.
func materializeISCSINodeDB(t *testing.T) string {
	t.Helper()
	const src = "testdata/iscsi-node-db/tree"
	root := t.TempDir()
	require.NoError(t, filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(src, path)
		if relErr != nil {
			return relErr
		}
		dest := filepath.Join(root, rel)
		if entry.IsDir() {
			return os.MkdirAll(dest, 0o750)
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // G122: the walked tree is this repo's own committed testdata, read once, with no concurrent writer and no symlink in it to race against
		if readErr != nil {
			return readErr
		}
		if writeErr := os.WriteFile(dest, data, 0o600); writeErr != nil { //nolint:gosec // G703: dest is filepath.Join(t.TempDir(), <relative path from that same committed tree>) -- version-controlled input, not attacker input
			return writeErr
		}
		return os.Chmod(dest, 0o644) //nolint:gosec // G302: deliberately looser than the 0600 the real iscsiadm leaves, so the credential write's tightening is what the mode assertions observe
	}))
	return root
}

// stubISCSINodeDBFastPath materializes the captured database and returns the one
// record it holds for (iscsiFixturePortal, iscsiFixtureFastPathIQN): the flat
// file `iscsiadm -m node -o new -T <iqn> -p <portal>` writes, which is the only
// shape iscsiEnsureNodeRecord can produce and therefore the one every CHAP stage
// on a node that has not run a discovery actually meets.
func stubISCSINodeDBFastPath(t *testing.T) string {
	t.Helper()
	root := stubISCSINodeDB(t)
	records := iscsiFixtureRecords(t, root, iscsiFixtureFastPathIQN, iscsiFixturePortal)
	require.Len(t, records, 1)
	return records[0]
}

// iscsiFixtureRecords lists every record file the fixture holds for (iqn,
// portal), under root, whatever layout it is on.
func iscsiFixtureRecords(t *testing.T, root, iqn, portal string) []string {
	t.Helper()
	records, err := iscsiNodeRecordFiles([]string{root}, portal, iqn)
	require.NoError(t, err, "the captured node database must contain a record for %s at %s", iqn, portal)
	paths := make([]string, 0, len(records))
	for _, record := range records {
		assert.Equal(t, root, record.Root)
		paths = append(paths, record.Path)
	}
	slices.Sort(paths)
	return paths
}

// snapshotISCSINodeDB reads every regular file under root, keyed by path.
func snapshotISCSINodeDB(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // G122: path comes from walking a t.TempDir() this test populated itself and nothing else can write to
		if readErr != nil {
			return readErr
		}
		files[path] = string(data)
		return nil
	}))
	return files
}

// assertNoStagedISCSIRecords fails if an atomic-rewrite temp file survived
// anywhere under root: idbm reads every regular file in a record directory as
// another record, so a leak is a corrupt node database, not just litter.
func assertNoStagedISCSIRecords(t *testing.T, root string) {
	t.Helper()
	for path := range snapshotISCSINodeDB(t, root) {
		assert.NotContains(t, filepath.Base(path), iscsiNodeRecordTempPrefix,
			"an atomic-rewrite temp file was left in the node database")
	}
}

// TestConfigureISCSICHAPWritesSecretToNodeRecord is the landing half of the S-01
// regression: the credential the argv no longer carries must still reach the
// node record --login reads, and the record must not be world-readable.
//
// It runs against BOTH on-disk layouts of a node database written by the real
// iscsiadm, because which one a node is on depends only on whether it has ever
// run a SendTargets discovery for the target. The fast-path case is not a
// variation on a theme: it is the ONLY layout iscsiEnsureNodeRecord produces, so
// a version of this code that handles just the directory layout fails 100% of
// CHAP-enabled NodeStageVolume calls on a freshly-provisioned node.
func TestConfigureISCSICHAPWritesSecretToNodeRecord(t *testing.T) {
	const (
		password     = "chapsecret123"
		peerPassword = "peersecret456"
	)
	cases := []struct {
		name string
		iqn  string
		// want is the number of record files the fixture holds for the portal:
		// one flat file, or one file per bound iface in the directory.
		want int
	}{
		{name: "flat record the fast path creates", iqn: iscsiFixtureFastPathIQN, want: 1},
		{name: "directory record a discovery creates", iqn: iscsiFixtureDiscoveredIQN, want: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := stubISCSINodeDB(t)
			records := iscsiFixtureRecords(t, root, tc.iqn, iscsiFixturePortal)
			require.Len(t, records, tc.want)
			untouched := map[string]string{}
			for path, contents := range snapshotISCSINodeDB(t, root) {
				if !slices.Contains(records, path) {
					untouched[path] = contents
				}
			}
			require.NotEmpty(t, untouched, "the fixture must hold records the write must not reach")

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
			require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(), iscsiFixturePortal, tc.iqn, creds))

			for _, record := range records {
				contents, err := os.ReadFile(record)
				require.NoError(t, err)
				assert.Contains(t, string(contents), "node.session.auth.password = "+password)
				assert.Contains(t, string(contents), "node.session.auth.password_in = "+peerPassword)
				// The record idbm wrote survives intact around the appended
				// credentials, including its trailing "# END RECORD" marker —
				// idbm parses assignments after it, and a later `-o update`
				// re-serializes the whole record and preserves them.
				assert.Contains(t, string(contents), "node.name = "+tc.iqn)
				assert.Contains(t, string(contents), "# END RECORD")

				info, err := os.Stat(record)
				require.NoError(t, err)
				assert.Equal(t, fs.FileMode(0o600), info.Mode().Perm(),
					"the node record holds the CHAP secret in clear text and must not be world-readable")
			}

			// Records for other portals — and for the other target — keep the
			// bytes iscsiadm wrote.
			for path, want := range untouched {
				got, err := os.ReadFile(path)
				require.NoError(t, err)
				assert.Equal(t, want, string(got), "a record outside the target portal was rewritten: %s", path)
			}
			assertNoStagedISCSIRecords(t, root)
			assert.NotEmpty(t, calls, "the non-secret parameters still go through iscsiadm")
		})
	}
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
	root := stubISCSINodeDB(t)
	before := snapshotISCSINodeDB(t, root)

	for name, value := range map[string]string{
		"newline":            "secret\nnode.session.auth.username = evil",
		"comment":            "secret#comment",
		"leading whitespace": " secret1234567",
	} {
		t.Run(name, func(t *testing.T) {
			writeErr := writeISCSINodeRecordSecret(iscsiNodeDBRoots, iscsiFixturePortal, iscsiFixtureFastPathIQN,
				"node.session.auth.password", value)
			require.Error(t, writeErr)
			assert.NotContains(t, writeErr.Error(), value, "the rejected value must not be echoed back")
			assert.Equal(t, before, snapshotISCSINodeDB(t, root), "a rejected value must not touch the node database")
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

// TestISCSINodeRecordFiles pins the predicate against a node database the real
// iscsiadm wrote. Every expected path below is a path that exists in the
// captured tree, not one this test created to match its own expectations.
func TestISCSINodeRecordFiles(t *testing.T) {
	root := stubISCSINodeDB(t)

	cases := []struct {
		name   string
		iqn    string
		portal string
		want   []string
	}{
		{
			// The layout `iscsiadm -m node -o new -T <iqn> -p <host>:<port>`
			// leaves: a plain FILE, because no tpgt was given. Requiring a
			// directory here found nothing and failed every CHAP-enabled stage.
			name:   "old-style flat record",
			iqn:    iscsiFixtureFastPathIQN,
			portal: iscsiFixturePortal,
			want:   []string{"nodes/" + iscsiFixtureFastPathIQN + "/192.0.2.40,3260"},
		},
		{
			// iscsiadm records an IPv6 portal without its brackets.
			name:   "old-style flat record on an IPv6 portal",
			iqn:    iscsiFixtureFastPathIQN,
			portal: iscsiFixtureIPv6Portal,
			want:   []string{"nodes/" + iscsiFixtureFastPathIQN + "/2001:db8::31,3260"},
		},
		{
			// A portal with a real tpgt gets a directory holding one file per
			// bound iface. --login may use any of them, so all must be returned.
			name:   "new-style directory record, every bound iface",
			iqn:    iscsiFixtureDiscoveredIQN,
			portal: iscsiFixturePortal,
			want: []string{
				"nodes/" + iscsiFixtureDiscoveredIQN + "/192.0.2.40,3260,1/default",
				"nodes/" + iscsiFixtureDiscoveredIQN + "/192.0.2.40,3260,1/iface1",
			},
		},
		{
			name:   "a different portal on the same target is not collateral",
			iqn:    iscsiFixtureDiscoveredIQN,
			portal: iscsiFixtureOtherPortal,
			want:   []string{"nodes/" + iscsiFixtureDiscoveredIQN + "/192.0.2.41,3260,1/default"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := make([]string, 0, len(tc.want))
			for _, rel := range tc.want {
				full := filepath.Join(root, filepath.FromSlash(rel))
				require.FileExists(t, full)
				want = append(want, full)
			}
			slices.Sort(want)
			assert.Equal(t, want, iscsiFixtureRecords(t, root, tc.iqn, tc.portal))
		})
	}

	t.Run("a portal with no record fails closed", func(t *testing.T) {
		_, err := iscsiNodeRecordFiles([]string{root}, "192.0.2.99:3260", iscsiFixtureFastPathIQN)
		require.Error(t, err)
	})

	t.Run("a port that only prefixes a recorded one does not match", func(t *testing.T) {
		// "326" must not match the recorded "3260": the portal component is
		// compared whole, not by prefix.
		_, err := iscsiNodeRecordFiles([]string{root}, "192.0.2.40:326", iscsiFixtureFastPathIQN)
		require.Error(t, err)
	})

	t.Run("an IQN that is not a single path component is refused", func(t *testing.T) {
		_, err := iscsiNodeRecordFiles([]string{root}, iscsiFixturePortal, "../../etc")
		require.Error(t, err)
	})
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
