package driver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Batch 4.3: iSCSI writes and reloads only when something changed. An
// unchanged republish makes no write and no reload; a strict move keeps only
// the initiator-allowlist writes, the publication records and one reload per
// half; a change whose reload failed is still reloaded later.

const (
	iscsiMoveVolume = "move-iscsi"
	iscsiNodeAIQN   = "iqn.2018-01.org.scale:worker-a"
	iscsiNodeBIQN   = "iqn.2018-01.org.scale:worker-b"
)

func iscsiTestNode(t *testing.T, name, iqn string) string {
	t.Helper()
	id, err := encodeNodeIdentity(NodeIdentity{Name: name, ISCSIIQN: iqn})
	require.NoError(t, err)
	return id
}

// newPublishedStrictISCSIVolume creates a strict-fenced iSCSI volume and
// publishes it to node A, then clears the call counts.
func newPublishedStrictISCSIVolume(t *testing.T) (*Driver, *apiCallCountingClient, string, string) {
	t.Helper()
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newFencedAPICallCountDriver(t, client, "iscsi", FencingModeStrict)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest(iscsiMoveVolume, "iscsi"))
	require.NoError(t, err)
	nodeA := iscsiTestNode(t, "worker-a", iscsiNodeAIQN)
	nodeB := iscsiTestNode(t, "worker-b", iscsiNodeBIQN)
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest(iscsiMoveVolume, nodeA))
	require.NoError(t, err)
	client.resetCalls()
	return d, client, nodeA, nodeB
}

func iscsiWrites(methods map[string]int) map[string]int {
	writes := map[string]int{}
	for _, method := range []string{
		"DatasetSetUserProperties", "DatasetRemoveUserProperties", "ISCSITargetUpdate", "ISCSITargetCreate",
		"ISCSIInitiatorUpdate", "ISCSIInitiatorCreateWithInitiators", "ISCSIExtentCreate",
		"ISCSITargetExtentCreate", "ServiceReload",
	} {
		if methods[method] > 0 {
			writes[method] = methods[method]
		}
	}
	return writes
}

// The strict iSCSI move golden: unpublish from A, publish to B.
//
// Before Batch 4.3, 25 calls: DatasetGet 2, DatasetRemoveUserProperties 1,
// DatasetSetUserProperties 5, ISCSIExtentGet 1, ISCSIInitiatorGet 3,
// ISCSIInitiatorUpdate 2, ISCSIPortalList 1, ISCSITargetExtentGet 1,
// ISCSITargetGet 3, ISCSITargetUpdate 2, ServiceReload 3, WaitForZvolReady 1.
//
// After, 19: -3 DatasetSetUserProperties (ensureShare's ID re-stamp, and the
// fence's initiator-group ID stamp on each half), -2 ISCSITargetUpdate (the
// target already references the per-volume group on every portal), -1
// ServiceReload (ensureShare changed nothing; each fence still reloads once,
// because the allowlist changed).
func TestStrictISCSIMoveGoldenAPICallCounts(t *testing.T) {
	ctx := context.Background()
	d, client, nodeA, nodeB := newPublishedStrictISCSIVolume(t)
	_, err := d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: iscsiMoveVolume, NodeId: nodeA})
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest(iscsiMoveVolume, nodeB))
	require.NoError(t, err)

	want := map[string]int{
		"DatasetGet":                  2,
		"DatasetRemoveUserProperties": 1,
		"DatasetSetUserProperties":    2,
		"ISCSIExtentGet":              1,
		"ISCSIInitiatorGet":           3,
		"ISCSIInitiatorUpdate":        2,
		"ISCSIPortalList":             1,
		"ISCSITargetExtentGet":        1,
		"ISCSITargetGet":              3,
		"ServiceReload":               2,
		"WaitForZvolReady":            1,
	}
	total := 19
	if recordsInKubernetes() {
		// The three remaining dataset property calls are the publication
		// records ("unpublishing", removal, new record); with records kept as
		// VolumePublications they are Kubernetes requests instead.
		delete(want, "DatasetRemoveUserProperties")
		delete(want, "DatasetSetUserProperties")
		total = 16
	}
	assertAPICallCount(t, "strict iSCSI move", client, total)
	assertAPICallMethodMap(t, "strict iSCSI move", client, want)

	// The move really moved: only B is allowed now.
	target, err := client.MockClient.ISCSITargetFindByName(ctx, d.iscsiShareName(iscsiMoveVolume))
	require.NoError(t, err)
	require.NotEmpty(t, target.Groups)
	group, err := client.MockClient.ISCSIInitiatorGet(ctx, target.Groups[0].Initiator)
	require.NoError(t, err)
	assert.Equal(t, []string{iscsiNodeBIQN}, group.Initiators)
}

// Before Batch 4.3 an unchanged republish to the same node made 2 dataset
// updates, 1 initiator update, 1 target update and 2 reloads. Now none.
func TestStrictISCSIUnchangedRepublishMakesNoWrites(t *testing.T) {
	d, client, nodeA, _ := newPublishedStrictISCSIVolume(t)
	_, err := d.ControllerPublishVolume(context.Background(), iscsiPublishRequest(iscsiMoveVolume, nodeA))
	require.NoError(t, err)
	_, methods := client.callSnapshot()
	assert.Empty(t, iscsiWrites(methods), "an unchanged republish writes and reloads nothing")
}

// A target that drifted (its per-volume group detached from the portal out of
// band) is still converged and reloaded: the skip is for an exact match only.
func TestStrictISCSIRepublishRebindsADriftedTarget(t *testing.T) {
	ctx := context.Background()
	d, client, nodeA, _ := newPublishedStrictISCSIVolume(t)
	target, err := client.MockClient.ISCSITargetFindByName(ctx, d.iscsiShareName(iscsiMoveVolume))
	require.NoError(t, err)
	client.MockClient.ISCSITargets[target.ID].Groups = []truenas.ISCSITargetGroup{{Portal: target.Groups[0].Portal, AuthMethod: "NONE"}}

	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest(iscsiMoveVolume, nodeA))
	require.NoError(t, err)
	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["ISCSITargetUpdate"], "a drifted target is rebound")
	assert.Equal(t, 1, methods["ServiceReload"], "and the change is reloaded")
	assert.Equal(t, 0, methods["ISCSIInitiatorUpdate"], "the allowlist itself was already right")
}

// A stored ID that is not set locally with the expected value (here: removed
// out of band) is re-stamped.
func TestISCSIRepublishRestampsAMissingResourceID(t *testing.T) {
	ctx := context.Background()
	d, client, nodeA, _ := newPublishedStrictISCSIVolume(t)
	require.NoError(t, client.MockClient.DatasetRemoveUserProperties(ctx, "pool/parent/"+iscsiMoveVolume, []string{PropISCSIExtentID}))

	_, err := d.ControllerPublishVolume(ctx, iscsiPublishRequest(iscsiMoveVolume, nodeA))
	require.NoError(t, err)
	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetSetUserProperties"])
	ds, err := client.MockClient.DatasetGet(ctx, "pool/parent/"+iscsiMoveVolume)
	require.NoError(t, err)
	assert.NotEmpty(t, datasetLocalUserProperty(ds, PropISCSIExtentID))
}

// A change whose reload failed is reloaded by the next pass even if that pass
// changes nothing itself; and a fresh controller (nothing known about what the
// service loaded) reloads once on its first pass.
func TestISCSIReloadOwedSurvivesAFailedReloadAndARestart(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newFencedAPICallCountDriver(t, client, "iscsi", FencingModeOff)
	var failNext atomic.Bool
	failNext.Store(true)
	d.serviceReloadDebouncer.Stop()
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		if failNext.Swap(false) {
			client.record("ServiceReload")
			return errors.New("simulated reload failure")
		}
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)

	// The create's reload fails (single path: not fatal).
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("owed-iscsi", "iscsi"))
	require.NoError(t, err)
	nodeA := iscsiTestNode(t, "worker-a", iscsiNodeAIQN)

	client.resetCalls()
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest("owed-iscsi", nodeA))
	require.NoError(t, err)
	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["ServiceReload"], "the publish must reload what the failed reload left unloaded")

	client.resetCalls()
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest("owed-iscsi", nodeA))
	require.NoError(t, err)
	_, methods = client.callSnapshot()
	assert.Equal(t, 0, methods["ServiceReload"], "once loaded, an unchanged republish does not reload")

	// A restart: a new debouncer knows nothing about what the service loaded.
	d.serviceReloadDebouncer.Stop()
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	client.resetCalls()
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest("owed-iscsi", nodeA))
	require.NoError(t, err)
	_, methods = client.callSnapshot()
	assert.Equal(t, 1, methods["ServiceReload"], "the first pass after a restart reloads once")
}

// A change marked while a reload is already running is not covered by it.
func TestServiceReloadDebouncerChangeDuringReloadStaysOwed(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	debouncer := NewServiceReloadDebouncer(0, func(context.Context, string) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	})
	defer debouncer.Stop()
	assert.True(t, debouncer.ReloadOwed("iscsitarget"), "a new debouncer starts owed")

	done := make(chan error, 1)
	go func() { done <- debouncer.RequestReload(context.Background(), "iscsitarget") }()
	<-started
	debouncer.MarkChanged("iscsitarget") // written after the running reload began
	close(release)
	require.NoError(t, <-done)
	assert.True(t, debouncer.ReloadOwed("iscsitarget"), "a change after the reload started is still owed")

	require.NoError(t, debouncer.RequestReloadIfOwed(context.Background(), "iscsitarget"))
	assert.False(t, debouncer.ReloadOwed("iscsitarget"))
	assert.Equal(t, int32(2), calls.Load())
	require.NoError(t, debouncer.RequestReloadIfOwed(context.Background(), "iscsitarget"))
	assert.Equal(t, int32(2), calls.Load(), "nothing owed: no reload")

	// A failed reload leaves the change owed.
	failing := NewServiceReloadDebouncer(time.Millisecond, func(context.Context, string) error { return errors.New("boom") })
	defer failing.Stop()
	require.Error(t, failing.RequestReload(context.Background(), "iscsitarget"))
	assert.True(t, failing.ReloadOwed("iscsitarget"))
}
