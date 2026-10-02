package driver

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// The rightful host's association is replaced by a foreign one: the allowlist
// keeps its size, so only the per-host check sees it. The volume is not
// skipped: it takes the per-volume path, which refuses to clear an intruder
// it cannot prove it granted (FailedPrecondition, the volume stays held) and
// leaves the allowlist as it found it.
func TestStartupDiffForeignHostReplacingTheRightfulHostIsNotSkipped(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	subsystem := f.subsystemOf(t, "diff-1")
	associations, err := f.client.MockClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
	require.NoError(t, err)
	require.Len(t, associations, 1)
	require.NoError(t, f.client.MockClient.NVMeoFHostSubsysDelete(ctx, associations[0].ID))
	host, err := f.client.MockClient.NVMeoFHostCreate(ctx, "nqn.2014-08.org.nvmexpress:uuid:intruder")
	require.NoError(t, err)
	_, err = f.client.MockClient.NVMeoFHostSubsysCreate(ctx, host.ID, subsystem.ID)
	require.NoError(t, err)
	f.perVolume()

	_, err = f.d.reconcilePublishedAttachmentsFor(ctx, nil)
	assert.Equal(t, []string{"diff-1"}, f.perVolume(), "a swapped allowlist must take the per-volume path")
	require.Error(t, err, "the per-volume path refuses to converge over an intruder")
	assert.Contains(t, err.Error(), "intruder")
	assert.True(t, f.d.startupGateStillPending("diff-1"), "the blocked volume stays held at publish")
	assert.Equal(t, []string{"nqn.2014-08.org.nvmexpress:uuid:intruder"}, f.allowedNQNs(t, "diff-1"),
		"nothing is granted to or revoked from a volume the per-volume path refused")
}

// The stored share IDs are judged against the backend: a stored ID naming
// another volume's namespace or subsystem, a namespace that serves another
// zvol, or a subsystem not named for the volume sends it down the per-volume
// path, which repairs it. The other volumes stay untouched.
func TestStartupDiffSendsAVolumeWithForeignShareIDsToThePerVolumePath(t *testing.T) {
	ctx := context.Background()
	property := func(t *testing.T, f *diffFixture, volumeID, key string) string {
		t.Helper()
		ds, err := f.client.MockClient.DatasetGet(ctx, "pool/parent/"+volumeID)
		require.NoError(t, err)
		return datasetUserProperty(ds, key)
	}
	namespaceOf := func(t *testing.T, f *diffFixture, volumeID string) *truenas.NVMeoFNamespace {
		t.Helper()
		id, err := strconv.Atoi(property(t, f, volumeID, PropNVMeoFNamespaceID))
		require.NoError(t, err)
		namespace := f.client.MockClient.NVMeNamespaces[id]
		require.NotNil(t, namespace)
		return namespace
	}
	cases := map[string]func(t *testing.T, f *diffFixture){
		"namespace ID names another volume's namespace": func(t *testing.T, f *diffFixture) {
			t.Helper()
			require.NoError(t, f.client.MockClient.DatasetSetUserProperty(ctx, "pool/parent/diff-1",
				PropNVMeoFNamespaceID, property(t, f, "diff-2", PropNVMeoFNamespaceID)))
		},
		"namespace and subsystem IDs name another volume's": func(t *testing.T, f *diffFixture) {
			t.Helper()
			for _, key := range []string{PropNVMeoFNamespaceID, PropNVMeoFSubsystemID} {
				require.NoError(t, f.client.MockClient.DatasetSetUserProperty(ctx, "pool/parent/diff-1", key, property(t, f, "diff-2", key)))
			}
		},
		"subsystem ID names another volume's subsystem": func(t *testing.T, f *diffFixture) {
			t.Helper()
			require.NoError(t, f.client.MockClient.DatasetSetUserProperty(ctx, "pool/parent/diff-1",
				PropNVMeoFSubsystemID, property(t, f, "diff-2", PropNVMeoFSubsystemID)))
		},
		"namespace serves another zvol": func(t *testing.T, f *diffFixture) {
			t.Helper()
			namespaceOf(t, f, "diff-1").DevicePath = "zvol/pool/parent/diff-2"
		},
		"subsystem not named for the volume": func(t *testing.T, f *diffFixture) {
			t.Helper()
			f.client.MockClient.NVMeSubsystems[f.subsystemOf(t, "diff-1").ID].Name = "renamed"
		},
	}
	for name, diverge := range cases {
		t.Run(name, func(t *testing.T) {
			f := newDiffFixture(t, 3)
			before := map[string][]string{"diff-0": f.allowedNQNs(t, "diff-0"), "diff-2": f.allowedNQNs(t, "diff-2")}
			diverge(t, f)
			f.perVolume()

			_, err := f.d.reconcilePublishedAttachmentsFor(ctx, nil)
			require.NoError(t, err)
			assert.Equal(t, []string{"diff-1"}, f.perVolume(), "only the volume with foreign share IDs takes the per-volume path")
			namespace := namespaceOf(t, f, "diff-1")
			assert.Equal(t, "zvol/pool/parent/diff-1", namespace.DevicePath, "the stored namespace serves this zvol again")
			assert.Equal(t, strconv.Itoa(namespace.SubsystemID), property(t, f, "diff-1", PropNVMeoFSubsystemID))
			subsystem := f.client.MockClient.NVMeSubsystems[namespace.SubsystemID]
			require.NotNil(t, subsystem)
			assert.False(t, subsystem.AllowAnyHost)
			associations, err := f.client.MockClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
			require.NoError(t, err)
			require.Len(t, associations, 1)
			assert.Equal(t, "nqn.2014-08.org.nvmexpress:uuid:k8s-1", associations[0].HostNQN)
			for volumeID, allowed := range before {
				assert.Equal(t, allowed, f.allowedNQNs(t, volumeID), "%s is untouched", volumeID)
			}
		})
	}
}

// A stored record that differs from the one the per-volume path would write
// only in the CO node ID (which no fence reads) is not skipped: ListVolumes
// reports that ID as the published node, so the per-volume path rewrites it.
// Skipping would keep reporting a node ID the CO no longer uses.
func TestStartupDiffSendsARecordWithAStaleCONodeIDToThePerVolumePath(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	ds, err := f.client.MockClient.DatasetGet(ctx, "pool/parent/diff-1")
	require.NoError(t, err)
	key := publicationPropertyKey("k8s-1")
	records, err := f.d.publications().records(ctx, ds.Name, ds)
	require.NoError(t, err)
	current := records[key]
	require.NotEmpty(t, current.EncodedID)
	// The same node, encoded as it was before it gained an address.
	older := diffNodeIdentity("k8s-1")
	older.IPs = nil
	stale, err := encodeNodeIdentity(older)
	require.NoError(t, err)
	require.NotEqual(t, current.EncodedID, stale)
	changed := current
	changed.EncodedID = stale
	require.NoError(t, f.d.publications().store(ctx, ds.Name, ds, key, changed))
	f.perVolume()

	_, err = f.d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"diff-1"}, f.perVolume(), "a record differing only in its CO node ID takes the per-volume path")
	ds, err = f.client.MockClient.DatasetGet(ctx, "pool/parent/diff-1")
	require.NoError(t, err)
	records, err = f.d.publications().records(ctx, ds.Name, ds)
	require.NoError(t, err)
	assert.Equal(t, current.EncodedID, records[key].EncodedID, "the per-volume path rewrote the CO node ID")
	assert.Equal(t, []string{"nqn.2014-08.org.nvmexpress:uuid:k8s-1"}, f.allowedNQNs(t, "diff-1"), "the fence is unchanged")
}
