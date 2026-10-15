package driver

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// createPayloadRecordingClient records the wire payload of every
// pool.dataset.create, marshaled exactly as the real client would send it.
type createPayloadRecordingClient struct {
	*truenas.MockClient

	mu       sync.Mutex
	payloads []map[string]json.RawMessage
}

func (c *createPayloadRecordingClient) DatasetCreate(ctx context.Context, params *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	encoded, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.payloads = append(c.payloads, payload)
	c.mu.Unlock()
	return c.MockClient.DatasetCreate(ctx, params)
}

func (c *createPayloadRecordingClient) createPayloads() []map[string]json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]json.RawMessage(nil), c.payloads...)
}

// encryptionOptionsOf decodes the encryption_options object of one recorded
// create payload into its raw keys.
func encryptionOptionsOf(t *testing.T, payload map[string]json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	raw, ok := payload["encryption_options"]
	require.True(t, ok, "an encrypted create carries encryption_options")
	var opts map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &opts))
	return opts
}

func newAlgorithmTestDriver(version string) (*Driver, *createPayloadRecordingClient) {
	mock := truenas.NewMockClient()
	mock.SystemVersion = version
	client := &createPayloadRecordingClient{MockClient: mock}
	d := &Driver{
		name: "org.scale.csi.nfs",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
			Encryption: EncryptionConfig{Enabled: true},
		},
		truenasClient: client,
	}
	return d, client
}

func encryptedCreateRequest(name string, secrets map[string]string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
		Parameters:         map[string]string{"encryption": "true"},
		Secrets:            secrets,
	}
}

const (
	truenas2510 = "TrueNAS-SCALE-25.10.0"
	truenas26   = "TrueNAS-26.0.0"
	truenas27   = "TrueNAS-27.0.0-RC.1"
)

// TestCreateVolumeEncryptedSendsNoDefaultAlgorithm is the TrueNAS 27 break:
// pool.dataset.create no longer has encryption_options.algorithm and rejects
// unknown keys, so the default suite — absent or named explicitly — must never
// be sent, on any version. The stamp still records AES-256-GCM, which is what
// every version creates without the key, and the default path reads no version.
//
// PRE-FIX PROOF: the driver always sent "algorithm":"AES-256-GCM", so on 27 the
// mock rejects the create, and on 25.10/26 the payload check fails.
func TestCreateVolumeEncryptedSendsNoDefaultAlgorithm(t *testing.T) {
	for _, version := range []string{truenas2510, truenas26, truenas27} {
		for _, tc := range []struct {
			name    string
			secrets map[string]string
		}{
			{"algorithm unset", map[string]string{"passphrase": "longenough1"}},
			{"algorithm AES-256-GCM", map[string]string{"passphrase": "longenough1", "algorithm": "AES-256-GCM"}},
		} {
			t.Run(version+"/"+tc.name, func(t *testing.T) {
				d, client := newAlgorithmTestDriver(version)
				ctx := context.Background()
				resp, err := d.CreateVolume(ctx, encryptedCreateRequest("enc-default", tc.secrets))
				require.NoError(t, err)

				payloads := client.createPayloads()
				require.Len(t, payloads, 1)
				opts := encryptionOptionsOf(t, payloads[0])
				assert.NotContains(t, opts, "algorithm", "the default suite must not be on the wire")
				assert.Contains(t, opts, "passphrase")

				datasetName, err := d.datasetForID(resp.GetVolume().GetVolumeId())
				require.NoError(t, err)
				ds, err := client.DatasetGet(ctx, datasetName)
				require.NoError(t, err)
				assert.True(t, ds.Encrypted)
				assert.Equal(t, "AES-256-GCM", ds.EncryptionAlgorithm, "the backend created its default suite")
				assert.Equal(t, "AES-256-GCM", datasetLocalUserProperty(ds, PropEncryption),
					"the stamp names what the backend created")
				assert.Equal(t, "AES-256-GCM", resp.GetVolume().GetVolumeContext()[volumeContextEncryptionKey])
				assert.Zero(t, client.GetSystemInfoCalls, "the default suite costs no version read")
			})
		}
	}
}

// TestCreateVolumeNonDefaultAlgorithmRefusedOnTrueNAS26Plus: TrueNAS 26 creates
// AES-256-GCM whatever is requested (the stamp would lie) and 27 rejects the
// option, so a non-default suite is InvalidArgument before any create.
//
// PRE-FIX PROOF: on 26 the create succeeded (stamped AES-128-GCM over an
// AES-256-GCM dataset); on 27 it reached pool.dataset.create and failed there.
func TestCreateVolumeNonDefaultAlgorithmRefusedOnTrueNAS26Plus(t *testing.T) {
	for _, version := range []string{truenas26, truenas27} {
		t.Run(version, func(t *testing.T) {
			d, client := newAlgorithmTestDriver(version)
			_, err := d.CreateVolume(context.Background(), encryptedCreateRequest("enc-128",
				map[string]string{"passphrase": "longenough1", "algorithm": "AES-128-GCM"}))
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), "AES-128-GCM")
			assert.Contains(t, err.Error(), `"algorithm"`, "the message names the Secret key to remove")
			assert.NotContains(t, err.Error(), "longenough1")
			assert.Empty(t, client.createPayloads(), "no pool.dataset.create is issued")
			assert.Empty(t, client.Datasets, "nothing is created")
		})
	}
}

// TestCreateVolumeNonDefaultAlgorithmFailsClosedOnUnknownVersion: a version that
// does not parse is treated as 26+, and an unreadable one is Unavailable; the
// non-default suite is never sent blind.
func TestCreateVolumeNonDefaultAlgorithmFailsClosedOnUnknownVersion(t *testing.T) {
	secrets := map[string]string{"passphrase": "longenough1", "algorithm": "AES-128-GCM"}
	t.Run("unparseable version", func(t *testing.T) {
		d, client := newAlgorithmTestDriver("TrueNAS-MASTER")
		_, err := d.CreateVolume(context.Background(), encryptedCreateRequest("enc-unknown", secrets))
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Empty(t, client.createPayloads())
	})
	t.Run("unreadable version", func(t *testing.T) {
		d, client := newAlgorithmTestDriver(truenas2510)
		client.SystemInfoErr = errors.New("connection reset")
		_, err := d.CreateVolume(context.Background(), encryptedCreateRequest("enc-unreadable", secrets))
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
		assert.Empty(t, client.createPayloads())
	})
}

// TestCreateVolumeNonDefaultAlgorithmHonoredOnTrueNAS2510: 25.10 still honors a
// requested suite, so it is sent and stamped.
func TestCreateVolumeNonDefaultAlgorithmHonoredOnTrueNAS2510(t *testing.T) {
	d, client := newAlgorithmTestDriver(truenas2510)
	ctx := context.Background()
	resp, err := d.CreateVolume(ctx, encryptedCreateRequest("enc-128",
		map[string]string{"passphrase": "longenough1", "algorithm": "AES-128-GCM"}))
	require.NoError(t, err)

	payloads := client.createPayloads()
	require.Len(t, payloads, 1)
	assert.JSONEq(t, `"AES-128-GCM"`, string(encryptionOptionsOf(t, payloads[0])["algorithm"]))

	datasetName, err := d.datasetForID(resp.GetVolume().GetVolumeId())
	require.NoError(t, err)
	ds, err := client.DatasetGet(ctx, datasetName)
	require.NoError(t, err)
	assert.Equal(t, "AES-128-GCM", ds.EncryptionAlgorithm)
	assert.Equal(t, "AES-128-GCM", datasetLocalUserProperty(ds, PropEncryption))
	assert.Equal(t, 1, client.GetSystemInfoCalls)
}

// TestCreateVolumeReplayIgnoresBackendAlgorithmSupport: the version check guards
// only a create that is about to be sent. A replay of a volume that already
// exists (created on 25.10 with AES-128-GCM, the appliance since upgraded to 27)
// sends no create and must not be refused for the Secret it was created with.
func TestCreateVolumeReplayIgnoresBackendAlgorithmSupport(t *testing.T) {
	d, client := newAlgorithmTestDriver(truenas2510)
	ctx := context.Background()
	req := encryptedCreateRequest("enc-replay", map[string]string{"passphrase": "longenough1", "algorithm": "AES-128-GCM"})
	first, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)

	client.SystemVersion = truenas27
	replay, err := d.CreateVolume(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, first.GetVolume().GetVolumeId(), replay.GetVolume().GetVolumeId())
	assert.Len(t, client.createPayloads(), 1, "the replay sends no create")
	assert.Equal(t, "AES-128-GCM", replay.GetVolume().GetVolumeContext()[volumeContextEncryptionKey])
}
