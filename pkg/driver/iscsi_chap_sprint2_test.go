package driver

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newSprint2CHAPDriver builds a controller Driver wired to the given client with
// CHAP enabled and the requested fencing mode. It mirrors newAPICallCountDriver
// but accepts any ClientInterface (so fault-injection wrappers can be used) and
// turns on the CHAP feature gate.
func newSprint2CHAPDriver(t *testing.T, client truenas.ClientInterface, mode FencingMode) *Driver {
	t.Helper()
	d := &Driver{
		name: "org.scale.csi.iscsi",
		config: &Config{
			DriverName: "org.scale.csi.iscsi",
			Fencing:    FencingConfig{Mode: mode},
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			ISCSI: ISCSIConfig{
				TargetPortal:    "192.0.2.10:3260",
				ExtentBlocksize: 512,
				ExtentRpm:       "SSD",
				CHAP:            ISCSICHAPSettings{Enabled: true},
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d
}

func sprint2CHAPRequest(name string, mutual bool) *csi.CreateVolumeRequest {
	req := apiCallCountVolumeRequest(name, "iscsi")
	req.Parameters[paramISCSIChAPSecret] = "true"
	req.Secrets = map[string]string{"username": "chapuser", "password": "chapsecret123"}
	if mutual {
		req.Secrets["mutualUsername"] = "peeruser"
		req.Secrets["mutualPassword"] = "peersecret456"
	}
	return req
}

// mockClientOf extracts the underlying MockClient regardless of whether the
// driver was given a bare MockClient or the counting wrapper.
func mockClientOf(client truenas.ClientInterface) *truenas.MockClient {
	switch c := client.(type) {
	case *truenas.MockClient:
		return c
	case *apiCallCountingClient:
		return c.MockClient
	case *chapStampFailClient:
		return c.MockClient
	default:
		return nil
	}
}

func iscsiTargetForVolume(t *testing.T, client truenas.ClientInterface, volumeName string) *truenas.ISCSITarget {
	t.Helper()
	mock := mockClientOf(client)
	ds, err := mock.DatasetGet(context.Background(), "pool/parent/"+volumeName)
	require.NoError(t, err)
	targetID, err := strconv.Atoi(datasetUserProperty(ds, PropISCSITargetID))
	require.NoError(t, err)
	target, err := mock.ISCSITargetGet(context.Background(), targetID)
	require.NoError(t, err)
	return target
}

// ---------------------------------------------------------------------------
// X8 + X1 + X2 — stateful fence tests over a real applyISCSIFence path.
// ---------------------------------------------------------------------------

func TestSprint2FenceRetainsCHAPAcrossModes(t *testing.T) {
	for _, mode := range []FencingMode{FencingModeStrict, FencingModeAdditive} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			client := newAPICallCountingClient()
			d := newSprint2CHAPDriver(t, client, mode)

			volumeName := "fence-chap-" + string(mode)
			_, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
			require.NoError(t, err)

			datasetName := "pool/parent/" + volumeName
			ds, err := client.MockClient.DatasetGet(ctx, datasetName)
			require.NoError(t, err)
			// A properly provisioned CHAP volume carries the durable LOCAL linkage.
			wantTag, err := strconv.Atoi(datasetLocalUserProperty(ds, PropISCSIAuthTag))
			require.NoError(t, err)
			require.Equal(t, "CHAP", datasetLocalUserProperty(ds, PropISCSIAuthMode))

			// Publish/fence with an active initiator: every rebuilt group MUST keep
			// authmethod=CHAP + auth=<tag>. A fence that stripped CHAP would emit
			// authmethod=NONE here (the R1/X1 downgrade).
			active := []NodeIdentity{{Name: "worker-a", ISCSIIQN: "iqn.1993-08.org.debian:01:worker-a"}}
			require.NoError(t, d.applyISCSIFence(ctx, ds, datasetName, active, false, nil))

			target := iscsiTargetForVolume(t, client, volumeName)
			require.NotEmpty(t, target.Groups)
			for _, g := range target.Groups {
				assert.Equal(t, "CHAP", g.AuthMethod, "fence must retain CHAP authmethod")
				require.NotNil(t, g.Auth, "fence must retain the auth ref")
				assert.Equal(t, wantTag, *g.Auth, "auth ref must be the stored tag")
			}

			// Unpublish (empty active set) must also retain CHAP on the preserved
			// portal relationships rather than downgrading them.
			ds, err = client.MockClient.DatasetGet(ctx, datasetName)
			require.NoError(t, err)
			require.NoError(t, d.applyISCSIFence(ctx, ds, datasetName, nil, false, nil))
			target = iscsiTargetForVolume(t, client, volumeName)
			for _, g := range target.Groups {
				assert.Equal(t, "CHAP", g.AuthMethod, "unpublish must retain CHAP authmethod")
				require.NotNil(t, g.Auth)
				assert.Equal(t, wantTag, *g.Auth)
			}
		})
	}
}

// TestSprint2FenceUsesStoredModeNotGlobalFlag proves the fence reconstructs the
// mode from the immutable stored PropISCSIAuthMode (X2): a one-way volume stays
// one-way even after the controller's global mutual flag is flipped, and a
// mutual volume stays mutual even when the global flag is false.
func TestSprint2FenceUsesStoredModeNotGlobalFlag(t *testing.T) {
	t.Run("one-way volume survives global mutual=true flip", func(t *testing.T) {
		ctx := context.Background()
		client := newAPICallCountingClient()
		d := newSprint2CHAPDriver(t, client, FencingModeStrict)
		d.config.ISCSI.CHAP.Mutual = false

		volumeName := "flip-oneway"
		_, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
		require.NoError(t, err)

		// Operator flips the controller-wide flag and the controller restarts. The
		// existing volume must NOT be re-derived as mutual.
		d.config.ISCSI.CHAP.Mutual = true

		datasetName := "pool/parent/" + volumeName
		ds, err := client.MockClient.DatasetGet(ctx, datasetName)
		require.NoError(t, err)
		require.NoError(t, d.applyISCSIFence(ctx, ds, datasetName,
			[]NodeIdentity{{Name: "worker-a", ISCSIIQN: "iqn.1993-08.org.debian:01:worker-a"}}, false, nil))

		target := iscsiTargetForVolume(t, client, volumeName)
		require.NotEmpty(t, target.Groups)
		for _, g := range target.Groups {
			assert.Equal(t, "CHAP", g.AuthMethod, "stored one-way mode must win over the flipped global flag")
		}
	})

	t.Run("mutual volume survives global mutual=false", func(t *testing.T) {
		ctx := context.Background()
		client := newAPICallCountingClient()
		d := newSprint2CHAPDriver(t, client, FencingModeStrict)
		d.config.ISCSI.CHAP.Mutual = true

		volumeName := "flip-mutual"
		_, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, true))
		require.NoError(t, err)

		d.config.ISCSI.CHAP.Mutual = false

		datasetName := "pool/parent/" + volumeName
		ds, err := client.MockClient.DatasetGet(ctx, datasetName)
		require.NoError(t, err)
		require.Equal(t, "CHAP_MUTUAL", datasetLocalUserProperty(ds, PropISCSIAuthMode))
		require.NoError(t, d.applyISCSIFence(ctx, ds, datasetName,
			[]NodeIdentity{{Name: "worker-a", ISCSIIQN: "iqn.1993-08.org.debian:01:worker-a"}}, false, nil))

		target := iscsiTargetForVolume(t, client, volumeName)
		require.NotEmpty(t, target.Groups)
		for _, g := range target.Groups {
			assert.Equal(t, "CHAP_MUTUAL", g.AuthMethod, "stored mutual mode must win over the global flag")
		}
	})
}

// ---------------------------------------------------------------------------
// X1 — the CHAP linkage stamp is durable-or-fail.
// ---------------------------------------------------------------------------

// chapStampFailClient fails exactly the DatasetSetUserProperties write that
// carries the CHAP linkage (PropISCSIAuthMode), simulating a transient failure
// of the fatal managed-property update. All other writes succeed.
type chapStampFailClient struct {
	*truenas.MockClient
	fail bool
}

func (c *chapStampFailClient) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if c.fail {
		if _, ok := properties[PropISCSIAuthMode]; ok {
			return fmt.Errorf("injected CHAP stamp failure")
		}
	}
	return c.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

func TestSprint2CreateVolumeCHAPStampIsFatalThenRepairs(t *testing.T) {
	ctx := context.Background()
	client := &chapStampFailClient{MockClient: truenas.NewMockClient(), fail: true}
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	volumeName := "fatal-stamp"
	// The CHAP linkage cannot be durably stamped: CreateVolume MUST fail rather
	// than return success with a chap volumeContext but no stored linkage.
	_, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))

	// No success-with-missing-stamp state: the dataset was rolled back, so a fence
	// can never read a half-provisioned CHAP target.
	_, getErr := client.DatasetGet(ctx, "pool/parent/"+volumeName)
	assert.True(t, truenas.IsNotFoundError(getErr), "failed CHAP provision must clean up the dataset")

	// Retry succeeds and durably stamps the linkage.
	client.fail = false
	_, err = d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
	require.NoError(t, err)
	ds, err := client.DatasetGet(ctx, "pool/parent/"+volumeName)
	require.NoError(t, err)
	assert.Equal(t, "CHAP", datasetLocalUserProperty(ds, PropISCSIAuthMode))
	assert.NotEmpty(t, datasetLocalUserProperty(ds, PropISCSIAuthTag))
}

// ---------------------------------------------------------------------------
// X3 — clones must NOT inherit CHAP identity.
// ---------------------------------------------------------------------------

func TestSprint2CloneScrubDropsInheritedCHAP(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	// A clone whose ZFS user properties were inherited from a CHAP source (source
	// is the origin snapshot name, not "local"). The scrub for an iSCSI (non-CHAP
	// request) destination must remove the inherited CHAP tag/mode so it cannot be
	// honored as this volume's policy.
	datasetName := "pool/parent/cloned-vol"
	ds := &truenas.Dataset{
		Name: datasetName,
		UserProperties: map[string]truenas.UserProperty{
			PropISCSIAuthTag:  {Value: "5000", Source: "pool/src@snap"},
			PropISCSIAuthMode: {Value: "CHAP", Source: "pool/src@snap"},
			PropISCSITargetID: {Value: "9", Source: "pool/src@snap"},
		},
	}
	mock := client.MockClient
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
	require.NoError(t, err)

	d.scrubInheritedCloneProperties(ctx, ds, datasetName, ShareTypeISCSI)

	_, tagPresent := ds.UserProperties[PropISCSIAuthTag]
	_, modePresent := ds.UserProperties[PropISCSIAuthMode]
	assert.False(t, tagPresent, "inherited CHAP tag must be scrubbed from an iSCSI clone")
	assert.False(t, modePresent, "inherited CHAP mode must be scrubbed from an iSCSI clone")

	// With the inherited props gone and no request resolution, the clone resolves
	// to authmethod=NONE — it cannot honor the source volume's credentials.
	method, ref := d.iscsiGroupCHAP(ctx, ds)
	assert.Equal(t, "NONE", method)
	assert.Equal(t, 0, ref)
}

// TestSprint2CloneWithCHAPRequestStampsOwnPolicy proves a CHAP-A -> CHAP-B style
// clone (the current request resolves its OWN CHAP peer) yields the request's
// tag/mode on the destination, not the inherited source's — the source==local
// read guard ignores the inherited props and the request resolution wins.
func TestSprint2CloneWithCHAPRequestStampsOwnPolicy(t *testing.T) {
	d := &Driver{config: &Config{ISCSI: ISCSIConfig{CHAP: ISCSICHAPSettings{Enabled: true}}}}
	// Destination dataset still carries the source's inherited (non-local) CHAP
	// props; the current request resolved peer B (tag 8888).
	ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
		PropISCSIAuthTag:  {Value: "5000", Source: "pool/src@snap"},
		PropISCSIAuthMode: {Value: "CHAP", Source: "pool/src@snap"},
	}}
	ctx := withISCSIChAPResolution(context.Background(), &iscsiCHAPResolution{
		Peer:   &truenas.ISCSIAuth{ID: 2, Tag: 8888, User: "chapuser-b"},
		Mutual: true,
	})
	method, ref := d.iscsiGroupCHAP(ctx, ds)
	assert.Equal(t, "CHAP_MUTUAL", method, "request resolution mode (B) wins")
	assert.Equal(t, 8888, ref, "request resolution tag (B) wins, not the inherited source tag")
}

// ---------------------------------------------------------------------------
// X4 — existing-volume replay: stored auth policy is authoritative.
// ---------------------------------------------------------------------------

func TestSprint2ExistingVolumeCHAPPolicyGuard(t *testing.T) {
	chapCtx := func() context.Context {
		return withISCSIChAPResolution(context.Background(), &iscsiCHAPResolution{
			Peer:   &truenas.ISCSIAuth{ID: 1, Tag: 5000, User: "chapuser"},
			Mutual: false,
		})
	}
	chapCtxTag := func(tag int, mutual bool) context.Context {
		return withISCSIChAPResolution(context.Background(), &iscsiCHAPResolution{
			Peer:   &truenas.ISCSIAuth{ID: 1, Tag: tag, User: "chapuser"},
			Mutual: mutual,
		})
	}
	storedCHAP := func(tag int, mode string) *truenas.Dataset {
		up := map[string]truenas.UserProperty{}
		if mode != "" {
			up[PropISCSIAuthTag] = truenas.UserProperty{Value: strconv.Itoa(tag), Source: "local"}
			up[PropISCSIAuthMode] = truenas.UserProperty{Value: mode, Source: "local"}
		}
		return &truenas.Dataset{UserProperties: up}
	}

	d := &Driver{config: &Config{}}

	t.Run("none stored + none request is allowed", func(t *testing.T) {
		require.NoError(t, d.guardExistingISCSICHAPPolicy(context.Background(), storedCHAP(0, "")))
	})
	t.Run("none stored + CHAP request is FailedPrecondition (no retro-convert)", func(t *testing.T) {
		err := d.guardExistingISCSICHAPPolicy(chapCtx(), storedCHAP(0, ""))
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("CHAP stored + none request is FailedPrecondition", func(t *testing.T) {
		err := d.guardExistingISCSICHAPPolicy(context.Background(), storedCHAP(5000, "CHAP"))
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("CHAP stored + same-policy request is allowed", func(t *testing.T) {
		require.NoError(t, d.guardExistingISCSICHAPPolicy(chapCtx(), storedCHAP(5000, "CHAP")))
	})
	t.Run("CHAP stored + different-tag request is FailedPrecondition", func(t *testing.T) {
		err := d.guardExistingISCSICHAPPolicy(chapCtxTag(6000, false), storedCHAP(5000, "CHAP"))
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	t.Run("CHAP stored + different-mode request is FailedPrecondition", func(t *testing.T) {
		err := d.guardExistingISCSICHAPPolicy(chapCtxTag(5000, true), storedCHAP(5000, "CHAP"))
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

// TestSprint2ExistingVolumeReplayReturnsStableContext proves an idempotent replay
// of a CHAP volume returns the same immutable volume context and never re-stamps.
func TestSprint2ExistingVolumeReplayReturnsStableContext(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	volumeName := "replay-chap"
	first, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
	require.NoError(t, err)
	firstCtx := first.GetVolume().GetVolumeContext()

	second, err := d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
	require.NoError(t, err)
	secondCtx := second.GetVolume().GetVolumeContext()

	assert.Equal(t, firstCtx[volumeContextCHAPKey], secondCtx[volumeContextCHAPKey])
	assert.Equal(t, "CHAP", secondCtx[volumeContextCHAPKey])
}

// TestSprint2ExistingNonCHAPVolumeRejectsCHAPReplay is the F-06 regression: a
// legacy non-CHAP volume replayed through a CHAP StorageClass must fail rather
// than silently converting to CHAP.
func TestSprint2ExistingNonCHAPVolumeRejectsCHAPReplay(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	// Provision a plain (non-CHAP) iSCSI volume first.
	volumeName := "legacy-plain"
	plainReq := apiCallCountVolumeRequest(volumeName, "iscsi")
	_, err := d.CreateVolume(ctx, plainReq)
	require.NoError(t, err)

	// Replay the SAME volume id through a CHAP request: retro-conversion is denied.
	_, err = d.CreateVolume(ctx, sprint2CHAPRequest(volumeName, false))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// ---------------------------------------------------------------------------
// X5 — peer identity, rotation, collision coherence.
// ---------------------------------------------------------------------------

func TestSprint2HotCacheCollisionRejectedWithoutClearingCache(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	_, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "7000"})
	require.NoError(t, err)

	// Do NOT clear the cache: a different username on the same tag must still be a
	// FailedPrecondition on the hot-cache path (kills the collision bypass).
	_, err = d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "otheruser", "password": "othersecret12", "tag": "7000"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestSprint2RotationCallsAuthUpdate(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	base := map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "7100"}
	first, err := d.EnsureISCSIAuthPeer(ctx, base)
	require.NoError(t, err)
	require.False(t, first.Rotated)
	firstFP := first.Peer.CredentialFingerprint

	// Same user + tag, changed secret => rotation via iscsi.auth.update. Clear the
	// cache to also cover the cold-path rotation branch; the tag stays stable.
	d.iscsiResolvedAuth = nil
	rotated, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "rotatedpw1234", "tag": "7100"})
	require.NoError(t, err)
	assert.True(t, rotated.Rotated, "a changed secret for the same user/tag must rotate")
	assert.Equal(t, first.Peer.Tag, rotated.Peer.Tag, "rotation keeps the tag stable")
	assert.Equal(t, first.Peer.ID, rotated.Peer.ID, "rotation updates in place (same id)")
	assert.NotEqual(t, firstFP, rotated.Peer.CredentialFingerprint, "the fingerprint must change on rotation")
	assert.Len(t, client.ISCSIAuths, 1, "rotation must not create a second peer")
}

func TestSprint2HotCacheRotationFallsThrough(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	base := map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "7150"}
	_, err := d.EnsureISCSIAuthPeer(ctx, base)
	require.NoError(t, err)

	// A changed secret with a WARM cache must fall through to the cold rotation
	// path (not silently return the stale cached peer).
	rotated, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "rotatedpw1234", "tag": "7150"})
	require.NoError(t, err)
	assert.True(t, rotated.Rotated)
	assert.Len(t, client.ISCSIAuths, 1)
}

func TestSprint2PostCreateDuplicateTagReconciles(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	// Pre-seed a peer created by a "concurrent controller" with the SAME tag and
	// identity but a lower id, so our create loses the deterministic race. The
	// post-create re-query must delete ours and adopt the lower-id winner.
	winner, err := client.MockClient.ISCSIAuthCreate(ctx, 7200, "chapuser", "chapsecret123", "", "")
	require.NoError(t, err)

	res, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "7200"})
	require.NoError(t, err)
	assert.Equal(t, winner.ID, res.Peer.ID, "the lowest-id winner must be kept")
	// Exactly one peer survives for the tag (ours was deleted).
	peers, err := client.MockClient.ISCSIAuthQueryByTag(ctx, 7200)
	require.NoError(t, err)
	assert.Len(t, peers, 1)
}

func TestSprint2PostCreateDuplicateTagDifferentIdentityFails(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newSprint2CHAPDriver(t, client, FencingModeStrict)

	// A lower-id winner with a DIFFERENT username races us. After deleting ours we
	// cannot adopt an incompatible peer, so the create fails FailedPrecondition.
	_, err := client.MockClient.ISCSIAuthCreate(ctx, 7300, "otheruser", "othersecret12", "", "")
	require.NoError(t, err)

	_, err = d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "7300"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
