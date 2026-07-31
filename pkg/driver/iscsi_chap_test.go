package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
