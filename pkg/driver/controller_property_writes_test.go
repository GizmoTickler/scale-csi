package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// propertyWriteRecorder records every dataset user-property write: each one is
// a pool.dataset.update of roughly half a second on TrueNAS, the cost the
// control plane is measured in.
type propertyWriteRecorder struct {
	*truenas.MockClient
	writes []map[string]string
}

func (r *propertyWriteRecorder) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	copied := make(map[string]string, len(properties))
	for key, value := range properties {
		copied[key] = value
	}
	r.writes = append(r.writes, copied)
	return r.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

func (r *propertyWriteRecorder) DatasetUpdate(ctx context.Context, name string, params *truenas.DatasetUpdateParams) (*truenas.Dataset, error) {
	if params != nil && len(params.UserPropertiesUpdate) > 0 {
		written := make(map[string]string, len(params.UserPropertiesUpdate))
		for _, update := range params.UserPropertiesUpdate {
			written[update.Key] = update.Value
		}
		r.writes = append(r.writes, written)
	}
	return r.MockClient.DatasetUpdate(ctx, name, params)
}

// A fresh NVMe-oF volume costs two property writes: the ownership stamp right
// after the dataset exists, and one final update carrying the share's resource
// IDs with the managed/provision/name stamps. The IDs used to have a
// warning-only write of their own.
func TestCreateVolumeFoldsNVMeoFResourceIDsIntoTheFinalWrite(t *testing.T) {
	ctx := context.Background()
	recorder := &propertyWriteRecorder{MockClient: truenas.NewMockClient()}
	d := newMultipathAPICallCountDriver(t, newAPICallCountingClient(), nil)
	d.truenasClient = recorder
	mustCreateParentDataset(t, recorder.MockClient)

	resp, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("folded", "nvmeof"))
	require.NoError(t, err)
	require.Len(t, recorder.writes, 2, "ownership stamp + final update: %v", recorder.writes)
	final := recorder.writes[1]
	for _, key := range []string{PropNVMeoFSubsystemID, PropNVMeoFPortSubsysID, PropNVMeoFNamespaceID, PropManagedResource, PropProvisionSuccess} {
		assert.NotEmpty(t, final[key], "the final update carries %s", key)
	}
	assert.NotEmpty(t, resp.GetVolume().GetVolumeContext()["nqn"], "the volume context still resolves from the stored IDs")

	// The repair path (ensureShareExists, no final update of its own) still
	// stamps the IDs itself when the dataset lost them.
	datasetName := "pool/parent/" + resp.GetVolume().GetVolumeId()
	require.NoError(t, recorder.DatasetRemoveUserProperties(ctx, datasetName,
		[]string{PropNVMeoFSubsystemID, PropNVMeoFPortSubsysID, PropNVMeoFNamespaceID}))
	recorder.writes = nil
	require.NoError(t, d.createNVMeoFShareForDataset(ctx, nil, datasetName, resp.GetVolume().GetVolumeId(), false, true, nil))
	require.NotEmpty(t, recorder.writes)
	assert.NotEmpty(t, recorder.writes[len(recorder.writes)-1][PropNVMeoFSubsystemID])
}

// A publish that would store exactly the record already stored writes nothing,
// unless the stale-record sweep is watching that record: then it is rewritten
// with a new UpdatedAt, so a revoke that detected the old generation backs off.
func TestRepublishOfAnUnchangedRecordWritesNothing(t *testing.T) {
	ctx := context.Background()
	h := newFencingTestHarness(t, FencingModeOff, ShareTypeNVMeoF, withNVMeAllowAnyHost())
	recorder := &propertyWriteRecorder{MockClient: h.client}
	h.d.truenasClient = recorder
	datasetName := "pool/parent/republished"
	ds, err := h.client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)
	require.NoError(t, h.d.createNVMeoFShareForDataset(ctx, ds, datasetName, "republished", true, true, nil))
	nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
	require.NoError(t, err)
	publish := func(mode csi.VolumeCapability_AccessMode_Mode) {
		t.Helper()
		_, err := h.d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: "republished", NodeId: nodeID,
			VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode}},
			VolumeContext:    map[string]string{"node_attach_driver": "nvmeof"},
		})
		require.NoError(t, err)
	}
	stored := func() publicationRecord {
		t.Helper()
		fresh, err := h.client.DatasetGet(ctx, datasetName)
		require.NoError(t, err)
		records, err := publicationRecordsFromDataset(fresh)
		require.NoError(t, err)
		return records[publicationPropertyKey("worker-a")]
	}

	publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	first := stored()
	recorder.writes = nil
	time.Sleep(2 * time.Millisecond)
	publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	assert.Empty(t, recorder.writes, "an unchanged record is not rewritten")
	assert.Equal(t, first.UpdatedAt, stored().UpdatedAt)

	// Watched by the stale-record sweep: rewritten, new generation.
	key := stalePublicationObservationKey(datasetName, publicationPropertyKey("worker-a"))
	h.d.stalePublicationRecordsSeen.Store(key, newStalePublicationObservation(time.Now(), first))
	publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	require.Len(t, recorder.writes, 1)
	assert.False(t, samePublicationRecordGeneration(first, stored()), "a new generation")
	_, stillWatched := h.d.stalePublicationRecordsSeen.Load(key)
	assert.False(t, stillWatched)

	// A different publication (another access mode) is written.
	recorder.writes = nil
	publish(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER)
	require.Len(t, recorder.writes, 1)
	assert.Equal(t, int32(csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER), stored().AccessMode)
}

// With fencing off, unpublish has no backend access to remove, so it removes
// the record without first writing an "unpublishing" tombstone. With fencing
// on, the tombstone still precedes the revocation.
func TestUnpublishWritesATombstoneOnlyWhenFencingRemovesAccess(t *testing.T) {
	for _, tc := range []struct {
		mode          FencingMode
		wantTombstone bool
	}{
		{FencingModeOff, false},
		{FencingModeStrict, true},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			ctx := context.Background()
			h := newFencingTestHarness(t, tc.mode, ShareTypeNVMeoF, withNVMeAllowAnyHost())
			recorder := &propertyWriteRecorder{MockClient: h.client}
			h.d.truenasClient = recorder
			datasetName := "pool/parent/unpublished"
			ds, err := h.client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
			require.NoError(t, err)
			require.NoError(t, h.d.createNVMeoFShareForDataset(ctx, ds, datasetName, "unpublished", true, true, nil))
			nodeID, err := encodeNodeIdentity(NodeIdentity{Name: "worker-a", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:worker-a"})
			require.NoError(t, err)
			_, err = h.d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
				VolumeId: "unpublished", NodeId: nodeID,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}},
				VolumeContext:    map[string]string{"node_attach_driver": "nvmeof"},
			})
			require.NoError(t, err)
			recorder.writes = nil
			_, err = h.d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: "unpublished", NodeId: nodeID})
			require.NoError(t, err)
			tombstones := 0
			for _, write := range recorder.writes {
				if value, ok := write[publicationPropertyKey("worker-a")]; ok && value != "" {
					tombstones++
				}
			}
			assert.Equal(t, tc.wantTombstone, tombstones == 1, "tombstone writes: %d", tombstones)
			fresh, err := h.client.DatasetGet(ctx, datasetName)
			require.NoError(t, err)
			_, retained := fresh.UserProperties[publicationPropertyKey("worker-a")]
			assert.False(t, retained, "the record is gone either way")
		})
	}
}

// failingWriteClient fails the dataset user-property writes that match.
type failingWriteClient struct {
	*truenas.MockClient
	fail func(properties map[string]string) bool
}

func (c *failingWriteClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if c.fail != nil && c.fail(properties) {
		return errors.New("simulated property write failure")
	}
	return c.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

func nvmeObjectCounts(t *testing.T, client *truenas.MockClient) (subsystems, namespaces, portAssociations int) {
	t.Helper()
	ctx := context.Background()
	s, err := client.NVMeoFSubsystemList(ctx)
	require.NoError(t, err)
	n, err := client.NVMeoFNamespaceList(ctx)
	require.NoError(t, err)
	p, err := client.NVMeoFPortSubsysList(ctx)
	require.NoError(t, err)
	return len(s), len(n), len(p)
}

// A crash between building an NVMe-oF share and CreateVolume's final update
// leaves the share with no IDs stored and no managed/provision stamps. The
// retry finds the share by name, stores the IDs and finishes, without a second
// share; and it does not report success while the repair write fails.
func TestNVMeoFCreateRetryAfterACrashBeforeTheFinalWrite(t *testing.T) {
	ctx := context.Background()
	for _, repairFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "repair succeeds", true: "repair write fails"}[repairFails], func(t *testing.T) {
			client := &failingWriteClient{MockClient: truenas.NewMockClient()}
			d := newMultipathAPICallCountDriver(t, newAPICallCountingClient(), nil)
			d.truenasClient = client
			mustCreateParentDataset(t, client.MockClient)
			req := apiCallCountVolumeRequest("crashed", "nvmeof")
			datasetName := "pool/parent/crashed"

			// The state the crash leaves: dataset, ownership stamp, share objects;
			// the IDs only in the final map that was never written.
			ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
			require.NoError(t, err)
			stampDriverOwnership(t, client.MockClient, d, datasetName)
			require.NoError(t, d.createNVMeoFShare(ctx, ds, datasetName, "crashed", true, true, nil, map[string]string{}))
			fresh, err := client.DatasetGet(ctx, datasetName)
			require.NoError(t, err)
			require.Empty(t, datasetUserProperty(fresh, PropNVMeoFSubsystemID), "no ID was written before the crash")

			if repairFails {
				client.fail = func(properties map[string]string) bool {
					_, repair := properties[PropNVMeoFSubsystemID]
					return repair
				}
				_, err = d.CreateVolume(ctx, req)
				require.Error(t, err, "the retry must not succeed without the IDs stored")
				client.fail = nil
			}
			resp, err := d.CreateVolume(ctx, req)
			require.NoError(t, err)
			assert.NotEmpty(t, resp.GetVolume().GetVolumeContext()["nqn"])
			fresh, err = client.DatasetGet(ctx, datasetName)
			require.NoError(t, err)
			assert.NotEmpty(t, datasetUserProperty(fresh, PropNVMeoFSubsystemID))
			assert.NotEmpty(t, datasetUserProperty(fresh, PropNVMeoFNamespaceID))
			assert.Equal(t, "true", datasetUserProperty(fresh, PropProvisionSuccess))
			subsystems, namespaces, _ := nvmeObjectCounts(t, client.MockClient)
			assert.Equal(t, 1, subsystems, "the retry reused the crashed create's subsystem")
			assert.Equal(t, 1, namespaces)
		})
	}
}

// When CreateVolume's final update fails, the NVMe-oF share it just built is
// rolled back with the dataset, although the IDs were never stored on it.
func TestNVMeoFCreateRollsBackTheShareWhenTheFinalWriteFails(t *testing.T) {
	ctx := context.Background()
	client := &failingWriteClient{MockClient: truenas.NewMockClient(), fail: func(properties map[string]string) bool {
		_, final := properties[PropProvisionSuccess]
		return final
	}}
	d := newMultipathAPICallCountDriver(t, newAPICallCountingClient(), nil)
	d.truenasClient = client
	mustCreateParentDataset(t, client.MockClient)

	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("rolled-back", "nvmeof"))
	require.Error(t, err)
	subsystems, namespaces, portAssociations := nvmeObjectCounts(t, client.MockClient)
	assert.Zero(t, subsystems)
	assert.Zero(t, namespaces)
	assert.Zero(t, portAssociations)
	_, err = client.DatasetGet(ctx, "pool/parent/rolled-back")
	assert.Error(t, err, "the dataset is deleted too")
}

// deleteCountingClient counts the NVMe-oF delete calls of a share delete.
type deleteCountingClient struct {
	*truenas.MockClient
	calls map[string]int
}

func (c *deleteCountingClient) NVMeoFSubsystemDeleteCascade(ctx context.Context, id int) error {
	c.calls["subsys.delete(force)"]++
	return c.MockClient.NVMeoFSubsystemDeleteCascade(ctx, id)
}

func (c *deleteCountingClient) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	c.calls["subsys.delete"]++
	return c.MockClient.NVMeoFSubsystemDelete(ctx, id)
}

func (c *deleteCountingClient) NVMeoFNamespaceDelete(ctx context.Context, id int) error {
	c.calls["namespace.delete"]++
	return c.MockClient.NVMeoFNamespaceDelete(ctx, id)
}

func (c *deleteCountingClient) NVMeoFPortSubsysDelete(ctx context.Context, id int) error {
	c.calls["port_subsys.delete"]++
	return c.MockClient.NVMeoFPortSubsysDelete(ctx, id)
}

// A volume's NVMe-oF share is deleted with one forced subsystem delete (TrueNAS
// removes the namespace and the port and host associations with it), unless the
// subsystem also serves another namespace: then nothing is forced and the other
// namespace survives.
func TestNVMeoFShareDeleteIsOneForcedSubsystemDeleteWhenTheSubsystemIsTheVolumes(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*Driver, *deleteCountingClient, string) {
		t.Helper()
		client := &deleteCountingClient{MockClient: truenas.NewMockClient(), calls: map[string]int{}}
		d := newMultipathAPICallCountDriver(t, newAPICallCountingClient(), []string{"192.0.2.21", "192.0.2.22", "192.0.2.23"})
		d.truenasClient = client
		mustCreateParentDataset(t, client.MockClient)
		resp, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("deleted", "nvmeof"))
		require.NoError(t, err)
		return d, client, "pool/parent/" + resp.GetVolume().GetVolumeId()
	}

	t.Run("the volume's own subsystem", func(t *testing.T) {
		d, client, datasetName := setup(t)
		require.NoError(t, d.deleteNVMeoFShareForDataset(ctx, nil, datasetName))
		assert.Equal(t, map[string]int{"subsys.delete(force)": 1}, client.calls)
		subsystems, namespaces, _ := nvmeObjectCounts(t, client.MockClient)
		assert.Zero(t, subsystems)
		assert.Zero(t, namespaces)
	})

	t.Run("a subsystem that also serves another namespace", func(t *testing.T) {
		d, client, datasetName := setup(t)
		subsystems, err := client.NVMeoFSubsystemList(ctx)
		require.NoError(t, err)
		require.Len(t, subsystems, 1)
		other, err := client.NVMeoFNamespaceCreate(ctx, subsystems[0].ID, "zvol/pool/parent/another", "ZVOL")
		require.NoError(t, err)
		require.NoError(t, d.deleteNVMeoFShareForDataset(ctx, nil, datasetName))
		assert.Equal(t, map[string]int{"namespace.delete": 1}, client.calls, "only this volume's namespace; never forced, no port association touched")
		_, err = client.NVMeoFNamespaceGet(ctx, other.ID)
		assert.NoError(t, err, "the other namespace survives")
		_, err = client.NVMeoFSubsystemGet(ctx, subsystems[0].ID)
		assert.NoError(t, err, "and so does the subsystem")
	})

	t.Run("the listing fails", func(t *testing.T) {
		d, client, datasetName := setup(t)
		failing := &namespaceListFailingClient{deleteCountingClient: client}
		d.truenasClient = failing
		require.Error(t, d.deleteNVMeoFShareForDataset(ctx, nil, datasetName), "no listing, no forced delete")
		assert.Empty(t, client.calls, "nothing is deleted")
	})

	t.Run("this volume's namespace is not among the subsystem's", func(t *testing.T) {
		d, client, datasetName := setup(t)
		// A namespace whose subsystem the backend did not report: the volume's
		// subsystem is resolved from the stored ID and does not list it, so the
		// cascade cannot reach it.
		namespaces, err := client.NVMeoFNamespaceList(ctx)
		require.NoError(t, err)
		require.Len(t, namespaces, 1)
		namespaces[0].SubsystemID = 0
		require.NoError(t, d.deleteNVMeoFShareForDataset(ctx, nil, datasetName))
		assert.Equal(t, 1, client.calls["subsys.delete(force)"])
		assert.Equal(t, 1, client.calls["namespace.delete"], "the namespace the cascade did not cover is deleted on its own")
		_, err = client.NVMeoFNamespaceGet(ctx, namespaces[0].ID)
		assert.Error(t, err)
	})
}

type namespaceListFailingClient struct {
	*deleteCountingClient
}

func (c *namespaceListFailingClient) NVMeoFNamespaceListBySubsystem(context.Context, int) ([]*truenas.NVMeoFNamespace, error) {
	return nil, errors.New("simulated listing failure")
}

// zfs.observeBusyBeforeDelete=false skips the two observation-only scans that
// otherwise precede every dataset delete; absent, they run.
func TestBusyObservationBeforeDeleteCanBeTurnedOff(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		setting *bool
		want    int
	}{
		{"default", nil, 1},
		{"on", ptrTo(true), 1},
		{"off", ptrTo(false), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newAPICallCountingClient()
			d := newMultipathAPICallCountDriver(t, client, nil)
			d.config.ZFS.ObserveBusyBeforeDelete = tc.setting
			resp, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("observed", "nvmeof"))
			require.NoError(t, err)
			client.resetCalls()
			_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: resp.GetVolume().GetVolumeId()})
			require.NoError(t, err)
			_, methods := client.callSnapshot()
			assert.Equal(t, tc.want, methods["DatasetAttachments"])
			assert.Equal(t, tc.want, methods["DatasetProcesses"])
		})
	}
}
