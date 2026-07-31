package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

func TestNodeISCSIChAPCredentials(t *testing.T) {
	cases := []struct {
		name       string
		volumeCtx  map[string]string
		secrets    map[string]string
		wantNil    bool
		wantMutual bool
		wantCode   codes.Code
		wantErrSub string
	}{
		{
			name:      "no chap flag means off",
			volumeCtx: map[string]string{"portal": "p", "iqn": "i"},
			secrets:   map[string]string{"username": "chapuser", "password": "chapsecret123"},
			wantNil:   true,
		},
		{
			name:      "explicit NONE means off",
			volumeCtx: map[string]string{"chap": "NONE"},
			secrets:   map[string]string{"username": "chapuser", "password": "chapsecret123"},
			wantNil:   true,
		},
		{
			name:      "chap mode builds one-way creds",
			volumeCtx: map[string]string{"chap": "CHAP"},
			secrets:   map[string]string{"username": "chapuser", "password": "chapsecret123"},
		},
		{
			name:       "mutual mode builds mutual creds",
			volumeCtx:  map[string]string{"chap": "CHAP_MUTUAL"},
			secrets:    map[string]string{"username": "chapuser", "password": "chapsecret123", "mutualUsername": "peeruser", "mutualPassword": "peersecret456"},
			wantMutual: true,
		},
		{
			name:      "chap mode accepts legacy aliases",
			volumeCtx: map[string]string{"chap": "CHAP"},
			secrets:   map[string]string{"node.session.auth.username": "chapuser", "node.session.auth.password": "chapsecret123"},
		},
		{
			name:       "missing secret fails fast",
			volumeCtx:  map[string]string{"chap": "CHAP"},
			secrets:    nil,
			wantCode:   codes.InvalidArgument,
			wantErrSub: "username is required",
		},
		{
			name:       "short password rejected",
			volumeCtx:  map[string]string{"chap": "CHAP"},
			secrets:    map[string]string{"username": "chapuser", "password": "short"},
			wantCode:   codes.InvalidArgument,
			wantErrSub: "12-16 characters",
		},
		{
			name:       "mutual mode without mutual secret rejected",
			volumeCtx:  map[string]string{"chap": "CHAP_MUTUAL"},
			secrets:    map[string]string{"username": "chapuser", "password": "chapsecret123"},
			wantCode:   codes.InvalidArgument,
			wantErrSub: "mutualUsername and mutualPassword",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			creds, err := nodeISCSIChAPCredentials(tc.volumeCtx, tc.secrets)
			if tc.wantNil {
				require.NoError(t, err)
				assert.Nil(t, creds)
				return
			}
			if tc.wantErrSub != "" {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				assert.Equal(t, tc.wantCode, st.Code())
				assert.Contains(t, st.Message(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, creds)
			assert.Equal(t, "chapuser", creds.Username)
			assert.Equal(t, "chapsecret123", creds.Password)
			assert.Equal(t, tc.wantMutual, creds.Mutual)
			if tc.wantMutual {
				assert.Equal(t, "peeruser", creds.MutualUsername)
				assert.Equal(t, "peersecret456", creds.MutualPassword)
			}
		})
	}
}

func TestNodeStageVolumeISCSIAuthFailureReturnsUnauthenticated(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	t.Cleanup(func() { iscsiConnectWithSessions = originalConnect })

	const secretPassword = "supersecretpw1"
	var capturedOpts *util.ISCSIConnectOptions
	iscsiConnectWithSessions = func(_ context.Context, _, _ string, _ int, opts *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		capturedOpts = opts
		return "", fmt.Errorf("%w for iqn.test:chap-volume", util.ErrISCSIAuthFailure)
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "chap-volume",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		Secrets: map[string]string{
			"username": "chapuser",
			"password": secretPassword,
		},
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.30:3260",
			"iqn":                "iqn.test:chap-volume",
			"lun":                "0",
			"chap":               "CHAP",
		},
	}

	_, err := d.NodeStageVolume(context.Background(), req)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Contains(t, st.Message(), "iqn.test:chap-volume")
	assert.NotContains(t, st.Message(), secretPassword, "secret must never appear in the status message")

	require.NotNil(t, capturedOpts, "connect must receive CHAP options")
	require.NotNil(t, capturedOpts.CHAP, "CHAP credentials must be threaded into connect options")
	assert.Equal(t, "chapuser", capturedOpts.CHAP.Username)
	assert.Equal(t, secretPassword, capturedOpts.CHAP.Password)
}

// TestNodeStageVolumeISCSICHAPConfigFailureEmitsEvent guards O15: when CHAP
// credentials cannot be applied to the node record (util.ErrISCSICHAPConfig), the
// node emits an ISCSICHAPFailed Warning Event whose message — like the returned
// status — NEVER contains the secret value.
func TestNodeStageVolumeISCSICHAPConfigFailureEmitsEvent(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	t.Cleanup(func() { iscsiConnectWithSessions = originalConnect })

	// 12-16 chars, no '#', no surrounding whitespace (TrueNAS auth.py rules).
	const secretPassword = "DONOTLEAKpw123"
	// Simulate the util connect path: a redacted CHAP-config failure carrying only
	// the parameter name and an exit-class summary — never the credential value.
	iscsiConnectWithSessions = func(_ context.Context, _, _ string, _ int, _ *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		return "", fmt.Errorf("%w for iqn.test:chap-config: failed to set node param node.session.auth.password (exit status 1)", util.ErrISCSICHAPConfig)
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	fakeRecorder := record.NewFakeRecorder(8)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}

	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "chap-config",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		Secrets: map[string]string{
			"username": "chapuser",
			"password": secretPassword,
		},
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.40:3260",
			"iqn":                "iqn.test:chap-config",
			"lun":                "0",
			"chap":               "CHAP",
		},
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
	assert.NotContains(t, st.Message(), secretPassword, "secret must never appear in the status message")

	sawCHAPFailed := false
drain:
	for {
		select {
		case event := <-fakeRecorder.Events:
			assert.NotContains(t, event, secretPassword, "secret must never appear in any event")
			assert.NotContains(t, event, "chapuser", "username must never appear in any event")
			if strings.Contains(event, EventReasonISCSICHAPFailed) {
				sawCHAPFailed = true
			}
		default:
			break drain
		}
	}
	assert.True(t, sawCHAPFailed, "CHAP config failure must emit an ISCSICHAPFailed event")
}

func TestNodeStageVolumeISCSIBuildsMutualCHAPCreds(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	t.Cleanup(func() { iscsiConnectWithSessions = originalConnect })

	var capturedOpts *util.ISCSIConnectOptions
	iscsiConnectWithSessions = func(_ context.Context, _, _ string, _ int, opts *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		capturedOpts = opts
		// Return an auth failure so the test stays on the error path and never
		// reaches device finalization; the captured opts are what we assert on.
		return "", fmt.Errorf("%w for iqn.test:chap-mutual", util.ErrISCSIAuthFailure)
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "chap-mutual",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		Secrets: map[string]string{
			"username":       "chapuser",
			"password":       "chapsecret123",
			"mutualUsername": "peeruser",
			"mutualPassword": "peersecret456",
		},
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.31:3260",
			"iqn":                "iqn.test:chap-mutual",
			"lun":                "0",
			"chap":               "CHAP_MUTUAL",
		},
	})
	require.Error(t, err)
	require.NotNil(t, capturedOpts)
	require.NotNil(t, capturedOpts.CHAP)
	assert.True(t, capturedOpts.CHAP.Mutual)
	assert.Equal(t, "peeruser", capturedOpts.CHAP.MutualUsername)
	assert.Equal(t, "peersecret456", capturedOpts.CHAP.MutualPassword)
}

func TestNodeStageVolumeISCSIWithoutCHAPLeavesCredsNil(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "iscsiadm")
	originalConnect := iscsiConnectWithSessions
	t.Cleanup(func() { iscsiConnectWithSessions = originalConnect })

	var capturedOpts *util.ISCSIConnectOptions
	iscsiConnectWithSessions = func(_ context.Context, _, _ string, _ int, opts *util.ISCSIConnectOptions, _ []util.ISCSISessionInfo) (string, error) {
		capturedOpts = opts
		return "", fmt.Errorf("some non-auth failure")
	}

	d := newTestNodeDriver(ShareTypeISCSI)
	_, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "plain-volume",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		VolumeContext: map[string]string{
			"node_attach_driver": "iscsi",
			"portal":             "192.0.2.32:3260",
			"iqn":                "iqn.test:plain-volume",
			"lun":                "0",
		},
	})
	require.Error(t, err)
	// A non-CHAP volume must not be classified as an auth failure.
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Internal, st.Code())
	require.NotNil(t, capturedOpts)
	assert.Nil(t, capturedOpts.CHAP, "non-CHAP volume must not carry credentials")
}

func TestRedactCHAP(t *testing.T) {
	redacted := redactCHAP(map[string]string{
		"username":       "chapuser",
		"password":       "chapsecret123",
		"mutualUsername": "peeruser",
		"mutualPassword": "peersecret456",
		"tag":            "1234",
	})
	assert.Equal(t, "chapuser", redacted["username"])
	assert.Equal(t, "***", redacted["password"])
	assert.Equal(t, "peeruser", redacted["mutualUsername"])
	assert.Equal(t, "***", redacted["mutualPassword"])
	assert.Equal(t, "1234", redacted["tag"])
	assert.Nil(t, redactCHAP(nil))
}

func TestValidateISCSIChAPSecret(t *testing.T) {
	cases := []struct {
		name    string
		secret  iscsiCHAPSecret
		wantSub string
	}{
		{"missing username", iscsiCHAPSecret{Password: "chapsecret123"}, "username is required"},
		{"missing password", iscsiCHAPSecret{Username: "chapuser"}, "password is required"},
		{"short password", iscsiCHAPSecret{Username: "chapuser", Password: "short"}, "12-16 characters"},
		{"long password", iscsiCHAPSecret{Username: "chapuser", Password: "thispasswordiswaytoolong"}, "12-16 characters"},
		{"mutual password without username", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", MutualPassword: "peersecret456"}, "mutualPassword requires mutualUsername"},
		{"mutual username without password", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", MutualUsername: "peeruser"}, "mutualPassword is required"},
		{"mutual password equals password", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", MutualUsername: "peeruser", MutualPassword: "chapsecret123"}, "must differ"},
		// X7: TrueNAS auth.py parity — exact unmodified secret.
		{"password with leading whitespace", iscsiCHAPSecret{Username: "chapuser", Password: " chapsecret12"}, "leading or trailing whitespace"},
		{"password with trailing whitespace", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret12 "}, "leading or trailing whitespace"},
		{"password with hash", iscsiCHAPSecret{Username: "chapuser", Password: "chap#secret12"}, "must not contain '#'"},
		{"malformed explicit tag", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", tagRaw: "abc", tagPresent: true}, "must be a positive integer"},
		{"zero explicit tag", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", tagRaw: "0", tagPresent: true}, "must be a positive integer"},
		{"valid one-way", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123"}, ""},
		{"valid mutual", iscsiCHAPSecret{Username: "chapuser", Password: "chapsecret123", MutualUsername: "peeruser", MutualPassword: "peersecret456"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateISCSIChAPSecret(tc.secret)
			if tc.wantSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tc.wantSub)
		})
	}
}

func TestApplyISCSIGroupCHAP(t *testing.T) {
	t.Run("stamps when active", func(t *testing.T) {
		group := &truenas.ISCSITargetGroup{Portal: 1, Initiator: 2, AuthMethod: "NONE"}
		applyISCSIGroupCHAP(group, iscsiCHAPModeCHAP, 42)
		assert.Equal(t, "CHAP", group.AuthMethod)
		require.NotNil(t, group.Auth)
		assert.Equal(t, 42, *group.Auth)
	})

	t.Run("no-op when auth ref is zero", func(t *testing.T) {
		group := &truenas.ISCSITargetGroup{Portal: 1, Initiator: 2, AuthMethod: "NONE"}
		applyISCSIGroupCHAP(group, iscsiCHAPModeCHAP, 0)
		assert.Equal(t, "NONE", group.AuthMethod)
		assert.Nil(t, group.Auth)
	})

	t.Run("no-op when mode is NONE", func(t *testing.T) {
		group := &truenas.ISCSITargetGroup{Portal: 1, Initiator: 2, AuthMethod: "NONE"}
		applyISCSIGroupCHAP(group, iscsiCHAPModeNone, 42)
		assert.Equal(t, "NONE", group.AuthMethod)
		assert.Nil(t, group.Auth)
	})
}

// TestISCSIGroupCHAPFromDatasetProperty guards the fence/rebuild linkage (R1/X2):
// a dataset that carries the LOCAL PropISCSIAuthTag + PropISCSIAuthMode must
// resolve back to a CHAP group so a fence pass rebuilds the group WITH its auth
// ref and stored mode instead of stripping it to authmethod=NONE or re-deriving
// the mode from the mutable global flag. A dataset without the properties — or
// one whose properties are clone-inherited (non-local) — stays NONE (X3).
func TestISCSIGroupCHAPFromDatasetProperty(t *testing.T) {
	d := &Driver{config: &Config{}}

	t.Run("local one-way property resolves CHAP", func(t *testing.T) {
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
			PropISCSIAuthTag:  {Value: "42", Source: "local"},
			PropISCSIAuthMode: {Value: "CHAP", Source: "local"},
		}}
		method, authRef := d.iscsiGroupCHAP(context.Background(), ds)
		assert.Equal(t, "CHAP", method)
		assert.Equal(t, 42, authRef)
	})

	t.Run("stored mode drives CHAP_MUTUAL regardless of global flag", func(t *testing.T) {
		// The controller's global mutual flag is false, but the stored per-volume
		// mode is CHAP_MUTUAL: the stored mode wins (never the global flag).
		dm := &Driver{config: &Config{ISCSI: ISCSIConfig{CHAP: ISCSICHAPSettings{Mutual: false}}}}
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
			PropISCSIAuthTag:  {Value: "7", Source: "local"},
			PropISCSIAuthMode: {Value: "CHAP_MUTUAL", Source: "local"},
		}}
		method, authRef := dm.iscsiGroupCHAP(context.Background(), ds)
		assert.Equal(t, "CHAP_MUTUAL", method)
		assert.Equal(t, 7, authRef)
	})

	t.Run("absent property stays NONE", func(t *testing.T) {
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}
		method, authRef := d.iscsiGroupCHAP(context.Background(), ds)
		assert.Equal(t, "NONE", method)
		assert.Equal(t, 0, authRef)
	})

	t.Run("clone-inherited property is ignored (source not local)", func(t *testing.T) {
		// A clone's inherited CHAP props carry the origin snapshot name as source,
		// not "local". They must never be honored — that would couple the clone to
		// the source volume's credentials (X3 / opus#2).
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{
			PropISCSIAuthTag:  {Value: "42", Source: "pool/src@snap"},
			PropISCSIAuthMode: {Value: "CHAP", Source: "pool/src@snap"},
		}}
		method, authRef := d.iscsiGroupCHAP(context.Background(), ds)
		assert.Equal(t, "NONE", method)
		assert.Equal(t, 0, authRef)
	})

	t.Run("request-scoped resolution wins", func(t *testing.T) {
		ctx := withISCSIChAPResolution(context.Background(), &iscsiCHAPResolution{
			Peer:   &truenas.ISCSIAuth{ID: 99, Tag: 5000, User: "chapuser"},
			Mutual: false,
		})
		method, authRef := d.iscsiGroupCHAP(ctx, &truenas.Dataset{})
		assert.Equal(t, "CHAP", method)
		// G1 live drill: the group auth ref is the peer TAG (SCST only emits
		// IncomingUser for tag-keyed refs).
		assert.Equal(t, 5000, authRef)
	})
}

func TestEnsureISCSIAuthPeerReuseAndCollision(t *testing.T) {
	ctx := context.Background()

	t.Run("reuse by tag for same username", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newAPICallCountDriver(t, client, "iscsi")
		d.config.ISCSI.CHAP.Enabled = true
		secrets := map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "5000"}

		first, err := d.EnsureISCSIAuthPeer(ctx, secrets)
		require.NoError(t, err)
		require.NotNil(t, first.Peer)

		// Drop the in-driver cache to prove reuse comes from the backend query,
		// not the cache. Same tag+user adopts the existing peer (no new create).
		d.iscsiResolvedAuth = nil
		second, err := d.EnsureISCSIAuthPeer(ctx, secrets)
		require.NoError(t, err)
		assert.Equal(t, first.Peer.ID, second.Peer.ID)
		assert.Len(t, client.ISCSIAuths, 1)
	})

	t.Run("tag collision with different username is FailedPrecondition", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newAPICallCountDriver(t, client, "iscsi")
		d.config.ISCSI.CHAP.Enabled = true

		_, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "chapsecret123", "tag": "6000"})
		require.NoError(t, err)

		d.iscsiResolvedAuth = nil
		_, err = d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "otheruser", "password": "othersecret12", "tag": "6000"})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("invalid secret fails fast", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newAPICallCountDriver(t, client, "iscsi")
		d.config.ISCSI.CHAP.Enabled = true
		_, err := d.EnsureISCSIAuthPeer(ctx, map[string]string{"username": "chapuser", "password": "short"})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Empty(t, client.ISCSIAuths, "no peer may be created for an invalid secret")
	})
}

// TestCreateVolumeISCSICHAPStampsGroupsAndProps is the end-to-end controller
// guard: a CHAP StorageClass provisions a target whose groups carry the CHAP
// authmethod + auth ref, persists the auth linkage as dataset properties (so
// fence/rebuild paths retain it), and advertises only a non-secret mode flag in
// the volume context.
func TestCreateVolumeISCSICHAPStampsGroupsAndProps(t *testing.T) {
	cases := []struct {
		name       string
		mutual     bool
		secrets    map[string]string
		wantMethod string
	}{
		{
			name:       "one-way",
			secrets:    map[string]string{"username": "chapuser", "password": "chapsecret123"},
			wantMethod: "CHAP",
		},
		{
			name:       "mutual",
			mutual:     true,
			secrets:    map[string]string{"username": "chapuser", "password": "chapsecret123", "mutualUsername": "peeruser", "mutualPassword": "peersecret456"},
			wantMethod: "CHAP_MUTUAL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := newAPICallCountingClient()
			d := newAPICallCountDriver(t, client, "iscsi")
			d.config.ISCSI.CHAP.Enabled = true
			d.config.ISCSI.CHAP.Mutual = tc.mutual

			volumeName := "chap-" + tc.name
			req := apiCallCountVolumeRequest(volumeName, "iscsi")
			req.Parameters[paramISCSIChAPSecret] = "true"
			req.Secrets = tc.secrets
			resp, err := d.CreateVolume(context.Background(), req)
			require.NoError(t, err)

			// Exactly one shared auth peer was created for the credential.
			require.Len(t, client.ISCSIAuths, 1)
			var peer *truenas.ISCSIAuth
			for _, p := range client.ISCSIAuths {
				peer = p
			}
			require.NotNil(t, peer)

			// Every created target group carries the CHAP authmethod + auth ref.
			ds, err := client.MockClient.DatasetGet(context.Background(), "pool/parent/"+volumeName)
			require.NoError(t, err)
			targetID, err := strconv.Atoi(datasetUserProperty(ds, PropISCSITargetID))
			require.NoError(t, err)
			target, err := client.MockClient.ISCSITargetGet(context.Background(), targetID)
			require.NoError(t, err)
			require.NotEmpty(t, target.Groups)
			for _, group := range target.Groups {
				assert.Equal(t, tc.wantMethod, group.AuthMethod)
				require.NotNil(t, group.Auth, "CHAP group must reference the auth peer")
				// G1 live drill: the auth ref must be the peer TAG — an ID-keyed
				// ref renders a CHAP target with no IncomingUser in SCST.
				assert.Equal(t, peer.Tag, *group.Auth)
			}

			// The auth linkage is persisted for fence/idempotent rebuilds: the
			// tag-keyed auth ref plus the immutable per-volume mode (X2). Both are
			// LOCAL so a clone cannot inherit them (X3).
			assert.Equal(t, strconv.Itoa(peer.Tag), datasetLocalUserProperty(ds, PropISCSIAuthTag))
			assert.Equal(t, tc.wantMethod, datasetLocalUserProperty(ds, PropISCSIAuthMode))

			// The volume context advertises only the mode flag, never a credential.
			volumeCtx := resp.GetVolume().GetVolumeContext()
			assert.Equal(t, tc.wantMethod, volumeCtx[volumeContextCHAPKey])
			for _, value := range volumeCtx {
				assert.NotEqual(t, "chapsecret123", value, "secret must never reach volumeContext")
				assert.NotEqual(t, "peersecret456", value, "mutual secret must never reach volumeContext")
			}
		})
	}
}
