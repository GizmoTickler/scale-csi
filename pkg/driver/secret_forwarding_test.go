package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// datasetCreateFaultClient fails pool.dataset.create with a caller-supplied
// error. The mock backend cannot produce a middleware traceback, which is the
// only thing that leaks here.
type datasetCreateFaultClient struct {
	truenas.ClientInterface
	err error
}

func (c *datasetCreateFaultClient) DatasetCreate(context.Context, *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	return nil, c.err
}

// TestCreateVolumeEncryptionCreateErrorIsRedacted is the S-02 regression.
//
// pool.dataset.create takes the passphrase as a CALL ARGUMENT, exactly like the
// pool.dataset.unlock / pool.dataset.change_key calls redactEncryptionError
// already covers, but its error was returned raw. The chain is
// DatasetCreate -> createDataset -> createVolume -> CreateVolume's deferred
// recordOperationFailureEvent -> a Warning Event on the requesting PVC. A
// namespace user needs no extra permission to induce the failure and read the
// Event on their own claim, so this asserts on BOTH the gRPC status and the
// recorded Event.
func TestCreateVolumeEncryptionCreateErrorIsRedacted(t *testing.T) {
	const passphrase = "longenough1-do-not-leak"
	fakeRecorder := record.NewFakeRecorder(8)
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
			Encryption: EncryptionConfig{Enabled: true},
		},
		truenasClient: &datasetCreateFaultClient{
			ClientInterface: truenas.NewMockClient(),
			// The shape a middleware traceback that echoes call arguments produces.
			err: fmt.Errorf("failed to create dataset: [EFAULT] pool.dataset.create: "+
				"CallError: create({'name': 'pool/parent/enc-leak', 'encryption_options': "+
				"{'passphrase': %q}})", passphrase),
		},
		eventRecorder: &EventRecorder{recorder: fakeRecorder, enabled: true},
	}

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "enc-create-leak",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters: map[string]string{
			"encryption":    "true",
			pvcNamespaceKey: "tenant",
			pvcNameKey:      "claim-one",
		},
		Secrets: map[string]string{"passphrase": passphrase},
	})
	require.Error(t, err)
	assert.NotContains(t, status.Convert(err).Message(), passphrase,
		"the passphrase must not survive into the gRPC status")
	assert.Contains(t, status.Convert(err).Message(), "***",
		"the forwarded backend text is masked, not dropped silently")

	events := drainEvents(fakeRecorder)
	require.NotEmpty(t, events, "the failure must still reach the PVC as a Warning Event")
	sawCreateFailed := false
	for _, event := range events {
		assert.NotContains(t, event, passphrase, "the passphrase must not reach the tenant's PVC Event")
		if strings.Contains(event, EventReasonVolumeCreateFailed) {
			sawCreateFailed = true
		}
	}
	assert.True(t, sawCreateFailed, "guard against a vacuous pass: the Event under test must have been recorded")
}

// TestCreateVolumePlaintextCreateErrorIsUnchanged pins the no-op half: a create
// that carries no encryption resolution has nothing to redact, and its error
// must reach the caller byte-identical to before.
func TestCreateVolumePlaintextCreateErrorIsUnchanged(t *testing.T) {
	const backendText = "failed to create dataset: [EFAULT] pool is full"
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
		},
		truenasClient: &datasetCreateFaultClient{
			ClientInterface: truenas.NewMockClient(),
			err:             fmt.Errorf("%s", backendText),
		},
	}

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "plain-create-fail",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), backendText)
}

// iscsiAuthFaultClient fails iscsi.auth.create / iscsi.auth.update with a
// caller-supplied error.
type iscsiAuthFaultClient struct {
	truenas.ClientInterface
	createErr error
	updateErr error
	peers     []*truenas.ISCSIAuth
}

func (c *iscsiAuthFaultClient) ISCSIAuthQueryByTag(context.Context, int) ([]*truenas.ISCSIAuth, error) {
	return c.peers, nil
}

func (c *iscsiAuthFaultClient) ISCSIAuthCreate(context.Context, int, string, string, string, string) (*truenas.ISCSIAuth, error) {
	return nil, c.createErr
}

func (c *iscsiAuthFaultClient) ISCSIAuthUpdate(context.Context, int, string, string, string, string) (*truenas.ISCSIAuth, error) {
	return nil, c.updateErr
}

// TestEnsureISCSIAuthPeerErrorsAreRedacted is the S-03 regression.
//
// iscsi.auth.create and iscsi.auth.update take secret/peersecret as call
// arguments, and their errors were interpolated raw into a gRPC status that
// CreateVolume's deferred recordOperationFailureEvent writes onto the tenant's
// PVC. redactCHAP only ever handled secret MAPS, never forwarded backend text.
func TestEnsureISCSIAuthPeerErrorsAreRedacted(t *testing.T) {
	// 12-16 chars, no '#', no surrounding whitespace (TrueNAS auth.py rules), so
	// validateISCSIChAPSecret cannot reject them before the call.
	const (
		password     = "DONOTLEAKpw12"
		peerPassword = "DONOTLEAKpr34"
	)
	secrets := map[string]string{
		"username":       "chapuser",
		"password":       password,
		"mutualUsername": "peeruser",
		"mutualPassword": peerPassword,
	}

	newDriver := func(client truenas.ClientInterface) *Driver {
		return &Driver{
			name:          "org.scale.csi.iscsi",
			config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
			truenasClient: client,
		}
	}

	t.Run("create failure", func(t *testing.T) {
		d := newDriver(&iscsiAuthFaultClient{
			ClientInterface: truenas.NewMockClient(),
			createErr: fmt.Errorf("failed to create iSCSI auth peer: [EINVAL] iscsi.auth.create: "+
				"CallError: create({'user': 'chapuser', 'secret': %q, 'peersecret': %q})", password, peerPassword),
		})
		_, err := d.EnsureISCSIAuthPeer(context.Background(), secrets)
		require.Error(t, err)
		message := status.Convert(err).Message()
		assert.NotContains(t, message, password, "the CHAP secret must not survive into the gRPC status")
		assert.NotContains(t, message, peerPassword, "the mutual peer secret must not survive either")
		assert.Contains(t, message, "***", "the forwarded backend text is masked, not dropped silently")
	})

	t.Run("rotation failure", func(t *testing.T) {
		// A peer already owns the tag under the same username with a DIFFERENT
		// credential, which is what drives the iscsi.auth.update rotation branch.
		existing := &truenas.ISCSIAuth{
			ID: 7, Tag: deriveISCSIAuthTag("chapuser"), User: "chapuser", PeerUser: "peeruser",
			CredentialFingerprint: truenas.ISCSIAuthCredentialFingerprint("chapuser", "stale-secret1", "peeruser", "stale-peer12"),
		}
		d := newDriver(&iscsiAuthFaultClient{
			ClientInterface: truenas.NewMockClient(),
			peers:           []*truenas.ISCSIAuth{existing},
			updateErr: fmt.Errorf("failed to update iSCSI auth peer: [EINVAL] iscsi.auth.update: "+
				"CallError: update(7, {'secret': %q, 'peersecret': %q})", password, peerPassword),
		})
		_, err := d.EnsureISCSIAuthPeer(context.Background(), secrets)
		require.Error(t, err)
		message := status.Convert(err).Message()
		assert.Contains(t, message, "failed to rotate", "guard against a vacuous pass: the rotation branch must be the one that failed")
		assert.NotContains(t, message, password)
		assert.NotContains(t, message, peerPassword)
		assert.Contains(t, message, "***")
	})
}
