package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Sprint 6 (H1) regression tests: a CHAP-enabled CLONE must stamp its durable
// CHAP policy (PropISCSIAuthTag + PropISCSIAuthMode) in the SAME atomic
// pool.dataset.update that stamps ownership + content-source (the Sprint-3 clone
// fold). Before the fix the CHAP policy landed only in the LATE fatal
// managed-property update, after the whole iSCSI share build; a controller crash
// in that window left an owned dataset with stored CHAP=NONE, and
// guardExistingISCSICHAPPolicy then rejected every retry forever (stored NONE vs
// request CHAP => FailedPrecondition), permanently wedging the PVC. Fresh
// (non-clone) CHAP volumes were always atomic and are unaffected.

// errSprint6SimulatedCrash is the panic sentinel a fault client raises to model a
// controller restart/eviction/OOM. A panic (not a returned error) is used so the
// driver runs NO cleanup — exactly like a real crash — leaving the dataset in the
// precise post-merged-update state a retry must recover from.
var errSprint6SimulatedCrash = errors.New("simulated controller crash after the merged clone fold update")

// sprint6IsCloneFoldUpdate reports whether a pool.dataset.update is one of the
// Sprint-3 clone ownership folds (snapshot or volume). Both folds carry
// PropVolumeContentSourceType; the in-flight marker write (a hashed bookkeeping
// key) and the capacity/legacy stamps do not, so this matches only the fold.
func sprint6IsCloneFoldUpdate(params *truenas.DatasetUpdateParams) bool {
	if params == nil {
		return false
	}
	for _, update := range params.UserPropertiesUpdate {
		if update.Key == PropVolumeContentSourceType {
			return true
		}
	}
	return false
}

// chapCloneCrashAfterFoldClient simulates a controller crash in the H1 window: it
// lets the merged clone fold update become durable, then panics BEFORE the late
// fatal managed-property stamp (which is where the CHAP policy used to land).
type chapCloneCrashAfterFoldClient struct {
	*truenas.MockClient
	crash      bool
	foldLanded bool
}

func (c *chapCloneCrashAfterFoldClient) DatasetUpdate(
	ctx context.Context,
	name string,
	params *truenas.DatasetUpdateParams,
) (*truenas.Dataset, error) {
	resp, err := c.MockClient.DatasetUpdate(ctx, name, params)
	if err != nil {
		return resp, err
	}
	if c.crash && sprint6IsCloneFoldUpdate(params) {
		c.foldLanded = true
		panic(errSprint6SimulatedCrash)
	}
	return resp, nil
}

// chapModeDropFoldResponseClient models the backend acknowledging the merged fold
// write but silently dropping ONLY PropISCSIAuthMode: it applies every fold key
// except the CHAP mode, so the response the driver verifies against is missing
// exactly that one key (ownership, content-source, and even the CHAP tag persist).
type chapModeDropFoldResponseClient struct {
	*truenas.MockClient
	dropped bool
}

func (c *chapModeDropFoldResponseClient) DatasetUpdate(
	ctx context.Context,
	name string,
	params *truenas.DatasetUpdateParams,
) (*truenas.Dataset, error) {
	if c.dropped || !sprint6IsCloneFoldUpdate(params) {
		return c.MockClient.DatasetUpdate(ctx, name, params)
	}
	c.dropped = true
	partial := *params
	filtered := make([]truenas.UserPropertyUpdate, 0, len(params.UserPropertiesUpdate))
	for _, update := range params.UserPropertiesUpdate {
		if update.Key != PropISCSIAuthMode {
			filtered = append(filtered, update)
		}
	}
	partial.UserPropertiesUpdate = filtered
	return c.MockClient.DatasetUpdate(ctx, name, &partial)
}

// chapSnapshotCloneRequest builds an iSCSI snapshot-clone CreateVolume request
// that opts into CHAP (StorageClass param + per-StorageClass secret).
func chapSnapshotCloneRequest(name, snapshotID string, mutual bool) *csi.CreateVolumeRequest {
	req := snapshotCloneRequest(name, snapshotID, "iscsi", testGiB)
	req.Parameters[paramISCSIChAPSecret] = "true"
	req.Secrets = map[string]string{"username": "chapuser", "password": "chapsecret123"}
	if mutual {
		req.Secrets["mutualUsername"] = "peeruser"
		req.Secrets["mutualPassword"] = "peersecret456"
	}
	return req
}

// chapVolumeCloneRequest builds an iSCSI volume-clone CreateVolume request that
// opts into CHAP.
func chapVolumeCloneRequest(name, sourceVolumeID string) *csi.CreateVolumeRequest {
	req := apiCallCountVolumeRequest(name, "iscsi")
	req.Parameters[paramISCSIChAPSecret] = "true"
	req.Secrets = map[string]string{"username": "chapuser", "password": "chapsecret123"}
	req.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceVolumeID},
	}}
	return req
}

func seedISCSIVolumeCloneSource(t *testing.T, client *truenas.MockClient, volumeID string, capacity int64) {
	t.Helper()
	mustCreateParentDataset(t, client)
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volumeID, Type: "VOLUME", Volsize: capacity,
	})
	require.NoError(t, err)
}

// assertCHAPCloneRecoversAfterFoldCrash drives the exact H1 scenario: the first
// CreateVolume crashes right after the merged fold update lands; the retry must
// SUCCEED because the fold stamped the CHAP policy durably with ownership, so the
// retry's guardExistingISCSICHAPPolicy sees stored CHAP == request CHAP instead of
// wedging on stored NONE.
func assertCHAPCloneRecoversAfterFoldCrash(
	t *testing.T,
	d *Driver,
	client *chapCloneCrashAfterFoldClient,
	req *csi.CreateVolumeRequest,
	datasetName, volumeID string,
) {
	t.Helper()
	ctx := context.Background()

	// First attempt: crash after the merged update, before the fatal stamp.
	func() {
		defer func() {
			r := recover()
			require.Equal(t, errSprint6SimulatedCrash, r, "the first attempt must crash after the merged update")
		}()
		_, _ = d.CreateVolume(ctx, req)
		t.Fatal("CreateVolume returned without crashing; the crash simulation is invalid")
	}()
	require.True(t, client.foldLanded, "the merged fold update must be durable before the crash")

	// The merged update is durable: ownership + content-source + CHAP policy, all
	// LOCAL. The CHAP policy landing HERE (atomically with ownership) is the H1
	// fix — it is exactly what the retry's guard reads. Without it the stored mode
	// would be NONE and the guard would return FailedPrecondition forever.
	postCrash, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	require.True(t, datasetHasLocalOwnershipStamp(postCrash), "ownership must be durable before the crash")
	require.Equal(t, "CHAP", datasetLocalUserProperty(postCrash, PropISCSIAuthMode),
		"the clone fold must stamp the CHAP mode durably with ownership")
	require.NotEmpty(t, datasetLocalUserProperty(postCrash, PropISCSIAuthTag),
		"the clone fold must stamp the CHAP tag durably with ownership")

	// The controller "restarts" (driverInstanceID is stable across restarts) and
	// replays CreateVolume. The guard sees matching stored CHAP and the volume
	// completes instead of wedging.
	client.crash = false
	resp, err := d.CreateVolume(ctx, req)
	require.NoError(t, err, "retry must recover; without the H1 fold this is FailedPrecondition forever")
	require.NotNil(t, resp.GetVolume())
	assert.Equal(t, "CHAP", resp.GetVolume().GetVolumeContext()[volumeContextCHAPKey],
		"the recovered volume must publish the CHAP volume context")

	// Fully provisioned: managed/provision stamps durable and the marker retired.
	done, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(done, PropManagedResource, "true"))
	assert.True(t, datasetHasLocalUserProperty(done, PropProvisionSuccess, "true"))
	mode, tag := d.storedISCSICHAPPolicy(done)
	assert.Equal(t, "CHAP", mode)
	assert.Positive(t, tag)
	marker, markerErr := d.readInflightMarker(ctx, volumeID)
	require.NoError(t, markerErr)
	assert.Nil(t, marker, "a recovered clone must retire its in-flight marker")
}

// TestSprint6CHAPSnapshotCloneCrashAfterMergedUpdateRecovers is the exact wedge
// from the integrated review: a CHAP snapshot clone crashes between the early
// ownership fold and the late fatal CHAP stamp, then recovers on retry.
func TestSprint6CHAPSnapshotCloneCrashAfterMergedUpdateRecovers(t *testing.T) {
	client := &chapCloneCrashAfterFoldClient{MockClient: truenas.NewMockClient(), crash: true}
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)
	seedSnapshotCloneSource(t, client.MockClient, "VOLUME", testGiB)

	req := chapSnapshotCloneRequest("chap-snap-clone", "snap-1", false)
	assertCHAPCloneRecoversAfterFoldCrash(t, d, client, req, "pool/parent/chap-snap-clone", "chap-snap-clone")
}

// TestSprint6CHAPVolumeCloneCrashAfterMergedUpdateRecovers is the volume-clone
// variant: its fold (setAndVerifyDatasetUserProperties) must also carry the CHAP
// policy so a crash before the fatal stamp recovers on retry.
func TestSprint6CHAPVolumeCloneCrashAfterMergedUpdateRecovers(t *testing.T) {
	client := &chapCloneCrashAfterFoldClient{MockClient: truenas.NewMockClient(), crash: true}
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)
	seedISCSIVolumeCloneSource(t, client.MockClient, "chap-vol-source", testGiB)

	req := chapVolumeCloneRequest("chap-vol-clone", "chap-vol-source")
	assertCHAPCloneRecoversAfterFoldCrash(t, d, client, req, "pool/parent/chap-vol-clone", "chap-vol-clone")
}

// TestSprint6NonCHAPCloneFoldOmitsCHAPPolicy proves the fix is a strict no-op for
// non-CHAP clones: even with the CHAP feature enabled on the controller, a clone
// request that does not opt in carries no CHAP policy in its merged fold update.
func TestSprint6NonCHAPCloneFoldOmitsCHAPPolicy(t *testing.T) {
	ctx := context.Background()
	client := &cloneFoldCaptureClient{MockClient: truenas.NewMockClient()}
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)
	seedSnapshotCloneSource(t, client.MockClient, "VOLUME", testGiB)

	// CHAP is enabled globally, but this request carries no chapSecret param and no
	// username secret, so chapEnabledForCreate is false and no resolution is threaded.
	req := snapshotCloneRequest("plain-clone", "snap-1", "iscsi", testGiB)
	_, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)

	require.Len(t, client.foldUpdates, 1, "the clone must fold content-source+ownership in exactly one update")
	props := make(map[string]string)
	for _, update := range client.foldUpdates[0].UserPropertiesUpdate {
		props[update.Key] = update.Value
	}
	_, hasTag := props[PropISCSIAuthTag]
	_, hasMode := props[PropISCSIAuthMode]
	assert.False(t, hasTag, "a non-CHAP clone fold must not carry the CHAP tag")
	assert.False(t, hasMode, "a non-CHAP clone fold must not carry the CHAP mode")
	// The fold carries exactly the three Sprint-3 keys — byte-for-byte unchanged.
	assert.Equal(t, map[string]string{
		PropVolumeContentSourceType: "snapshot",
		PropVolumeContentSourceID:   "snap-1",
		PropDriverInstanceID:        d.driverInstanceID(),
	}, props)

	// The provisioned volume resolves stored CHAP = NONE.
	ds, err := client.DatasetGet(ctx, "pool/parent/plain-clone")
	require.NoError(t, err)
	mode, tag := d.storedISCSICHAPPolicy(ds)
	assert.Equal(t, "NONE", mode)
	assert.Equal(t, 0, tag)
}

// TestSprint6CHAPCloneFoldModeDropFailsBeforeMarkerRetirement extends the Sprint-3
// silent-drop lesson to the CHAP props the H1 fix folded in: a merged-update
// response that drops ONLY PropISCSIAuthMode must fail response verification and
// fail CreateVolume BEFORE the in-flight marker is retired — never stranding a
// markerless remnant.
func TestSprint6CHAPCloneFoldModeDropFailsBeforeMarkerRetirement(t *testing.T) {
	ctx := context.Background()
	client := &chapModeDropFoldResponseClient{MockClient: truenas.NewMockClient()}
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)
	seedSnapshotCloneSource(t, client.MockClient, "VOLUME", testGiB)

	req := chapSnapshotCloneRequest("chap-drop", "snap-1", false)
	_, err := d.CreateVolume(ctx, req)
	require.Error(t, err, "a merged update whose response drops the CHAP mode must fail verification")
	require.True(t, client.dropped, "the test must have intercepted and truncated the CHAP clone fold")

	// The failure happened BEFORE marker retirement: the marker is still present so
	// a retry can still prove provenance and recover the remnant.
	marker, markerErr := d.readInflightMarker(ctx, "chap-drop")
	require.NoError(t, markerErr)
	require.NotNil(t, marker, "verification failure must fail BEFORE the in-flight marker is retired")

	// The fold wrote ownership (only the CHAP mode was dropped), so guarded cleanup
	// correctly refused to delete a now-owned dataset: it is preserved, still
	// owned, and recoverable by a retry — no destructive cleanup, no markerless leak.
	assert.Equal(t, codes.Aborted, status.Code(err),
		"an owned-but-verification-failed clone must be preserved (lost-race precaution), not deleted")
	ds, getErr := client.DatasetGet(ctx, "pool/parent/chap-drop")
	require.NoError(t, getErr, "the owned dataset must be preserved for retry recovery")
	assert.True(t, datasetHasLocalOwnershipStamp(ds))
}
