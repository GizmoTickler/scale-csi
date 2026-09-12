package util

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These names are the sentinels every assertion in this file searches error
// text for. They are deliberately high-entropy and contain no path-shaped
// substring, so "the error does not contain this" cannot pass by accident.
//
// They are also representable in a node record (no whitespace, no '#', no
// newline), so writeISCSINodeRecordSecret's own value guards do not fire first
// and mask the failure under test.
const (
	fixtureCHAPPassword       = "chappw-never-log-me-4f21b9c7"
	fixtureCHAPMutualPassword = "peerpw-never-log-me-8d0e3a15"
	fixtureCHAPUsername       = "chapuser-never-log-me-6b2c"
	fixtureCHAPMutualUsername = "peeruser-never-log-me-1e9d"
)

// fixtureCHAPCredentials is one set of mutual CHAP credentials built entirely
// from the sentinels above, so any of the four leaking into any error string is
// caught by assertNoCredentialInError.
func fixtureCHAPCredentials() *ISCSICHAPCredentials {
	return &ISCSICHAPCredentials{
		Username:       fixtureCHAPUsername,
		Password:       fixtureCHAPPassword,
		MutualUsername: fixtureCHAPMutualUsername,
		MutualPassword: fixtureCHAPMutualPassword,
		Mutual:         true,
	}
}

// assertNoCredentialInError is the proof obligation for every error path added
// or touched by the fail-closed change: an unwritable record must be NAMED, and
// the credential that could not be written must not travel with the complaint.
//
// It checks the fully formatted chain (%v walks every wrapped error, including
// the *fs.PathError the OS returns), because a credential leaking through a
// wrapped error is exactly as readable in a gRPC status or a klog line as one
// leaking through the outermost Errorf.
func assertNoCredentialInError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	text := err.Error()
	for _, secret := range []string{
		fixtureCHAPPassword, fixtureCHAPMutualPassword,
		fixtureCHAPUsername, fixtureCHAPMutualUsername,
	} {
		assert.NotContains(t, text, secret,
			"a CHAP credential reached an error string; the whole point of the node-record write is that it never does")
	}
}

// makeUnreadable strips every permission bit from path and returns a restore
// func. Callers MUST restore before snapshotting or before the test ends:
// t.TempDir's cleanup cannot remove a 0000 directory.
//
// Running as root defeats the setup entirely (root is not subject to file
// permissions), so the removal is verified to have actually taken effect and
// the test is skipped rather than asserted against a precondition that never
// fired. Under the uid this repo's gate runs as, it always fires.
func makeUnreadable(t *testing.T, path string) func() {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "the fixture must contain %s before a test can make it unreadable", path)
	original := info.Mode().Perm()
	require.NoError(t, os.Chmod(path, 0o000))
	restore := func() { _ = os.Chmod(path, original) }
	t.Cleanup(restore)

	if info.IsDir() {
		if _, readErr := os.ReadDir(path); readErr == nil {
			restore()
			t.Skipf("this test needs a directory the process cannot read; running as uid %d defeats that", os.Getuid())
		}
	} else if _, readErr := os.ReadFile(path); readErr == nil {
		restore()
		t.Skipf("this test needs a file the process cannot read; running as uid %d defeats that", os.Getuid())
	}
	return restore
}

// stubISCSINodeDBTwoRoots materializes the captured node database TWICE and
// points iscsiNodeDBRoots at both copies, in order.
//
// This is production's shape, not a contrivance: iscsiNodeDBRoots defaults to
// {"/etc/iscsi", "/var/lib/iscsi"} and the node DaemonSet mounts both, so a
// record for one (target, portal) can be visible under one root, the other, or
// both. It is the configuration in which a per-record skip is silently
// survivable -- and therefore the one in which the fail-open bug does damage
// instead of tripping the "no node record found" guard.
func stubISCSINodeDBTwoRoots(t *testing.T) (first, second string) {
	t.Helper()
	first = materializeISCSINodeDB(t)
	second = materializeISCSINodeDB(t)
	original := iscsiNodeDBRoots
	iscsiNodeDBRoots = []string{first, second}
	t.Cleanup(func() { iscsiNodeDBRoots = original })
	return first, second
}

// stubISCSIAdmNoop silences the non-secret iscsiadm parameter writes so a test
// exercises only the node-record path.
func stubISCSIAdmNoop(t *testing.T) {
	t.Helper()
	original := iscsiAdmCombinedOutput
	iscsiAdmCombinedOutput = func(context.Context, ...string) ([]byte, error) { return nil, nil }
	t.Cleanup(func() { iscsiAdmCombinedOutput = original })
}

// TestISCSINodeRecordFilesFailsClosedOnAnUnreadableRecord is the regression for
// the fail-open skip in the record walk.
//
// Both cases stage the same thing: two node database roots, a record for the
// target portal under each, and one of them made unreadable. Before the fix the
// walk skipped the unreadable one and returned the other, so
// writeISCSINodeRecordSecret wrote the credential into a strict SUBSET of the
// portals a --login can use and returned nil. The stage then reported success
// while one portal had no credential at all, which surfaces later as a
// path-specific authentication failure rather than as a configuration error.
//
// A credential write has no business reporting success over a subset.
func TestISCSINodeRecordFilesFailsClosedOnAnUnreadableRecord(t *testing.T) {
	cases := []struct {
		name string
		iqn  string
		// unreadable returns the path under the first root to strip: the
		// per-portal record DIRECTORY for the new-style layout, the whole
		// per-target directory for the old-style flat one.
		unreadable func(root, iqn string) string
	}{
		{
			// The <host>,<port>,<tpgt> directory a SendTargets discovery
			// produces. It was listed, it matched the portal, and it is a
			// directory -- there is no benign reading of a failure to
			// enumerate it.
			name: "new-style record directory cannot be enumerated",
			iqn:  iscsiFixtureDiscoveredIQN,
			unreadable: func(root, iqn string) string {
				return filepath.Join(root, "nodes", iqn, "192.0.2.40,3260,1")
			},
		},
		{
			// The per-target directory holding the flat records the fast path
			// writes. EACCES here is NOT the benign "this root does not carry
			// this target" case -- that one is ENOENT and is still skipped,
			// which the sibling test below pins.
			name: "per-target directory cannot be enumerated",
			iqn:  iscsiFixtureFastPathIQN,
			unreadable: func(root, iqn string) string {
				return filepath.Join(root, "nodes", iqn)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, second := stubISCSINodeDBTwoRoots(t)
			stubISCSIAdmNoop(t)

			// The SECOND root stays perfectly readable. That is what made the
			// bug fail open instead of tripping the "no node record found"
			// guard: there was always something to return.
			readable := iscsiFixtureRecords(t, second, tc.iqn, iscsiFixturePortal)
			require.NotEmpty(t, readable)
			before := snapshotISCSINodeDB(t, second)

			hidden := tc.unreadable(first, tc.iqn)
			restore := makeUnreadable(t, hidden)

			records, err := iscsiNodeRecordFiles(iscsiNodeDBRoots, iscsiFixturePortal, tc.iqn)
			require.Error(t, err, "a record that cannot be read must not be silently dropped from the set")
			assert.Nil(t, records, "a partial record set is the one return value this function must never produce")
			assert.Contains(t, err.Error(), hidden, "the error must name the record that could not be read")

			// The same failure through the real entry point, with a real
			// credential, is what proves the stage fails rather than reporting
			// a partial write as success.
			configureErr := ConfigureISCSICHAPWithContext(context.Background(),
				iscsiFixturePortal, tc.iqn, fixtureCHAPCredentials())
			assertNoCredentialInError(t, configureErr)
			assert.Contains(t, configureErr.Error(), hidden)

			restore()
			assert.Equal(t, before, snapshotISCSINodeDB(t, second),
				"discovery failed closed, so the readable root must not have been written at all")
			assertNoStagedISCSIRecords(t, second)
		})
	}
}

// TestISCSINodeRecordFilesStillSkipsARootWithoutTheTarget is the other half of
// the fail-closed change, and the reason the skip is narrowed to ENOENT rather
// than deleted.
//
// The two roots are ALTERNATIVES: production probes /etc/iscsi and
// /var/lib/iscsi and a node keeps its database under one of them, so "this root
// has no nodes/<iqn>" is the ordinary case on every healthy node. Turning that
// into an error would fail 100% of CHAP-enabled stages -- the same shape of
// mistake round seven found in this function's directory predicate.
func TestISCSINodeRecordFilesStillSkipsARootWithoutTheTarget(t *testing.T) {
	stubISCSIAdmNoop(t)
	populated := materializeISCSINodeDB(t)
	empty := t.TempDir()

	originalRoots := iscsiNodeDBRoots
	iscsiNodeDBRoots = []string{empty, populated}
	t.Cleanup(func() { iscsiNodeDBRoots = originalRoots })

	records, err := iscsiNodeRecordFiles(iscsiNodeDBRoots, iscsiFixturePortal, iscsiFixtureFastPathIQN)
	require.NoError(t, err, "a root that simply does not carry this target is not an error")
	require.Len(t, records, 1)
	assert.Equal(t, populated, records[0].Root)

	require.NoError(t, ConfigureISCSICHAPWithContext(context.Background(),
		iscsiFixturePortal, iscsiFixtureFastPathIQN, fixtureCHAPCredentials()))
	contents, err := os.ReadFile(records[0].Path)
	require.NoError(t, err)
	assert.Contains(t, string(contents), "node.session.auth.password = "+fixtureCHAPPassword)
}

// TestWriteISCSINodeRecordSecretAbortsAndReportsAPartialUpdate pins the chosen
// answer to "what happens to the records already written when one of them
// cannot be".
//
// The decision: ABORT, LEAVE THEM, REPORT IT. No rollback.
//
// Aborting is what makes it safe. Nothing logs in until a stage SUCCEEDS, so a
// half-updated database never reaches --login on its own; the error fails the
// stage, kubelet retries, and a later successful attempt rewrites every record.
// Rolling back would be strictly worse: the pre-image of an already-rewritten
// record may carry a STALE credential and the world-readable mode idbm created
// it with, it would race the concurrent sibling stages this function documents
// (clobbering a good write with a stale pre-image), and it is itself a write --
// the operation that just proved it can fail.
//
// So the guarantee is not "all or nothing on disk", which is unachievable here.
// It is "never silently partial": the failure is loud, it names the record, it
// says the database is partially updated, and it carries no credential.
func TestWriteISCSINodeRecordSecretAbortsAndReportsAPartialUpdate(t *testing.T) {
	// The discovered target's portal holds TWO iface records in one directory
	// ("default" and "iface1", in os.ReadDir's sorted order), so there is a
	// first record to succeed and a second to fail.
	const (
		firstRecord  = "default"
		secondRecord = "iface1"
	)

	t.Run("a failure after the first record reports the partial update", func(t *testing.T) {
		root := stubISCSINodeDB(t)
		records := iscsiFixtureRecords(t, root, iscsiFixtureDiscoveredIQN, iscsiFixturePortal)
		require.Len(t, records, 2)
		require.Equal(t, firstRecord, filepath.Base(records[0]))
		require.Equal(t, secondRecord, filepath.Base(records[1]))

		restore := makeUnreadable(t, records[1])
		err := writeISCSINodeRecordSecret(iscsiNodeDBRoots, iscsiFixturePortal, iscsiFixtureDiscoveredIQN,
			"node.session.auth.password", fixtureCHAPPassword)

		assertNoCredentialInError(t, err)
		require.ErrorIs(t, err, errISCSINodeDBPartiallyUpdated,
			"a credential write that stopped halfway must say so, not just fail")
		assert.Contains(t, err.Error(), records[1], "the error must name the record that could not be written")
		assert.Contains(t, err.Error(), "node.session.auth.password",
			"the parameter NAME is not secret and is what makes the error actionable")

		// Documented consequence of choosing abort over rollback: what was
		// written stays written, and stays tightened to 0600.
		written, readErr := os.ReadFile(records[0])
		require.NoError(t, readErr)
		assert.Contains(t, string(written), "node.session.auth.password = "+fixtureCHAPPassword,
			"the record written before the abort is deliberately NOT rolled back")
		info, statErr := os.Stat(records[0])
		require.NoError(t, statErr)
		assert.Equal(t, fs.FileMode(0o600), info.Mode().Perm(),
			"a rollback would have undone this one-way tightening on a file holding a clear-text password")

		restore()
		assertNoStagedISCSIRecords(t, root)
	})

	t.Run("a failure on the first record claims no partial update", func(t *testing.T) {
		// The sentinel must mean something. A write that failed before touching
		// anything left no partial state, so claiming one would send an
		// operator looking for a half-updated database that does not exist.
		root := stubISCSINodeDB(t)
		records := iscsiFixtureRecords(t, root, iscsiFixtureDiscoveredIQN, iscsiFixturePortal)
		require.Len(t, records, 2)

		untouched, readErr := os.ReadFile(records[1])
		require.NoError(t, readErr)

		restore := makeUnreadable(t, records[0])
		err := writeISCSINodeRecordSecret(iscsiNodeDBRoots, iscsiFixturePortal, iscsiFixtureDiscoveredIQN,
			"node.session.auth.password", fixtureCHAPPassword)

		assertNoCredentialInError(t, err)
		assert.NotErrorIs(t, err, errISCSINodeDBPartiallyUpdated,
			"nothing was written, so nothing may be reported as partially written")

		restore()
		after, readErr := os.ReadFile(records[1])
		require.NoError(t, readErr)
		assert.Equal(t, string(untouched), string(after),
			"the abort must stop at the first failure, not carry on into the rest of the set")
		assertNoStagedISCSIRecords(t, root)
	})
}

// TestConfigureISCSICHAPKeepsTheCredentialOutOfEveryErrorPath sweeps every
// failure this function can return and asserts the credential is in none of
// them.
//
// This is the assertion the argv change exists for. Moving the password off
// /proc/<pid>/cmdline buys nothing if the first permission error puts it into a
// gRPC status, a klog line or a Kubernetes Event instead -- and the new
// fail-closed arms are precisely the paths that now produce errors where they
// previously produced a silent skip.
func TestConfigureISCSICHAPKeepsTheCredentialOutOfEveryErrorPath(t *testing.T) {
	t.Run("no node record anywhere", func(t *testing.T) {
		stubISCSIAdmNoop(t)
		originalRoots := iscsiNodeDBRoots
		iscsiNodeDBRoots = []string{t.TempDir()}
		t.Cleanup(func() { iscsiNodeDBRoots = originalRoots })

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(context.Background(),
			iscsiFixturePortal, iscsiFixtureFastPathIQN, fixtureCHAPCredentials()))
	})

	t.Run("a record directory that cannot be enumerated", func(t *testing.T) {
		stubISCSIAdmNoop(t)
		first, _ := stubISCSINodeDBTwoRoots(t)
		makeUnreadable(t, filepath.Join(first, "nodes", iscsiFixtureDiscoveredIQN, "192.0.2.40,3260,1"))

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(context.Background(),
			iscsiFixturePortal, iscsiFixtureDiscoveredIQN, fixtureCHAPCredentials()))
	})

	t.Run("a record that cannot be written, after another already was", func(t *testing.T) {
		stubISCSIAdmNoop(t)
		root := stubISCSINodeDB(t)
		records := iscsiFixtureRecords(t, root, iscsiFixtureDiscoveredIQN, iscsiFixturePortal)
		require.Len(t, records, 2)
		makeUnreadable(t, records[1])

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(context.Background(),
			iscsiFixturePortal, iscsiFixtureDiscoveredIQN, fixtureCHAPCredentials()))
	})

	t.Run("an IQN that is not a single path component", func(t *testing.T) {
		stubISCSIAdmNoop(t)
		stubISCSINodeDB(t)

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(context.Background(),
			iscsiFixturePortal, "../../etc", fixtureCHAPCredentials()))
	})

	t.Run("a canceled context", func(t *testing.T) {
		stubISCSIAdmNoop(t)
		stubISCSINodeDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(ctx,
			iscsiFixturePortal, iscsiFixtureFastPathIQN, fixtureCHAPCredentials()))
	})

	t.Run("an iscsiadm failure on a non-secret parameter", func(t *testing.T) {
		// The usernames are half the credential and still travel on argv, so
		// the exec error must not be echoed either.
		original := iscsiAdmCombinedOutput
		iscsiAdmCombinedOutput = func(_ context.Context, args ...string) ([]byte, error) {
			return []byte("iscsiadm: could not run " + strings.Join(args, " ")), assert.AnError
		}
		t.Cleanup(func() { iscsiAdmCombinedOutput = original })
		stubISCSINodeDB(t)

		assertNoCredentialInError(t, ConfigureISCSICHAPWithContext(context.Background(),
			iscsiFixturePortal, iscsiFixtureFastPathIQN, fixtureCHAPCredentials()))
	})

	t.Run("a value the record format cannot represent", func(t *testing.T) {
		stubISCSINodeDB(t)
		for name, value := range map[string]string{
			"newline": fixtureCHAPPassword + "\nnode.session.auth.username = evil",
			"comment": fixtureCHAPPassword + "#comment",
			"space":   " " + fixtureCHAPPassword,
		} {
			t.Run(name, func(t *testing.T) {
				err := writeISCSINodeRecordSecret(iscsiNodeDBRoots, iscsiFixturePortal,
					iscsiFixtureFastPathIQN, "node.session.auth.password", value)
				assertNoCredentialInError(t, err)
				assert.NotContains(t, err.Error(), value)
			})
		}
	})
}

// TestISCSINodeRecordFilesReturnsEveryMatchingRootsRecords guards the
// assumption the fail-closed tests above rest on: with a record under BOTH
// roots, the walk returns both, so a skipped one really is a portal that loses
// its credential rather than a duplicate that some other root covers.
func TestISCSINodeRecordFilesReturnsEveryMatchingRootsRecords(t *testing.T) {
	first, second := stubISCSINodeDBTwoRoots(t)

	records, err := iscsiNodeRecordFiles(iscsiNodeDBRoots, iscsiFixturePortal, iscsiFixtureDiscoveredIQN)
	require.NoError(t, err)

	roots := make([]string, 0, len(records))
	for _, record := range records {
		roots = append(roots, record.Root)
	}
	assert.Equal(t, 2, slices.Index(roots, second)-slices.Index(roots, first),
		"roots must be walked in the order given, which is what makes an abort's stopping point reproducible")
	assert.Len(t, records, 4, "two iface records under each of the two roots")
}
