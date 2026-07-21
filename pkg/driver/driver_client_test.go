package driver

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestNewDriverNodeOnlyDoesNotCreateTrueNASClient(t *testing.T) {
	original := newTrueNASClient
	t.Cleanup(func() { newTrueNASClient = original })
	clientCreated := false
	newTrueNASClient = func(*truenas.ClientConfig) (truenas.ClientInterface, error) {
		clientCreated = true
		return truenas.NewMockClient(), nil
	}

	drv, err := NewDriver(&DriverConfig{
		Name:     "csi.scale.io",
		Version:  "test",
		NodeID:   "node-a",
		Endpoint: "unix:///tmp/scale-csi-node-test.sock",
		RunNode:  true,
		Config: &Config{
			TrueNAS: TrueNASConfig{
				Host:                  "unreachable.example.test",
				RequestTimeout:        1,
				ConnectTimeout:        1,
				WriteTimeout:          1,
				MaxConcurrentRequests: 1,
			},
			ZFS:  ZFSConfig{DatasetParentName: "tank/csi"},
			Node: NodeConfig{Topology: TopologyConfig{Enabled: true}},
		},
	})
	require.NoError(t, err)
	assert.False(t, clientCreated)
	assert.Nil(t, drv.truenasClient)
	assert.Nil(t, drv.serviceReloadDebouncer)
	drv.Stop()
}

func TestNewDriverControllerCreatesTrueNASClient(t *testing.T) {
	original := newTrueNASClient
	t.Cleanup(func() { newTrueNASClient = original })
	clientCreated := false
	newTrueNASClient = func(*truenas.ClientConfig) (truenas.ClientInterface, error) {
		clientCreated = true
		return truenas.NewMockClient(), nil
	}

	drv, err := NewDriver(&DriverConfig{
		Name:          "csi.scale.io",
		Version:       "test",
		Endpoint:      "unix:///tmp/scale-csi-controller-test.sock",
		RunController: true,
		Config: &Config{
			TrueNAS: TrueNASConfig{Host: "truenas.example.test", APIKey: "test-key"},
			ZFS:     ZFSConfig{DatasetParentName: "tank/csi"},
		},
	})
	require.NoError(t, err)
	assert.True(t, clientCreated)
	assert.NotNil(t, drv.truenasClient)
	assert.NotNil(t, drv.serviceReloadDebouncer)
	drv.Stop()
}

func TestNewDriverControllerRequiresTrueNASAPIKey(t *testing.T) {
	original := newTrueNASClient
	t.Cleanup(func() { newTrueNASClient = original })
	clientCreated := false
	newTrueNASClient = func(*truenas.ClientConfig) (truenas.ClientInterface, error) {
		clientCreated = true
		return truenas.NewMockClient(), nil
	}

	drv, err := NewDriver(&DriverConfig{
		Name:          "csi.scale.io",
		Version:       "test",
		Endpoint:      "unix:///tmp/scale-csi-controller-no-key-test.sock",
		RunController: true,
		Config: &Config{
			TrueNAS: TrueNASConfig{Host: "truenas.example.test"},
			ZFS:     ZFSConfig{DatasetParentName: "tank/csi"},
		},
	})
	require.Error(t, err)
	assert.Nil(t, drv)
	assert.Contains(t, err.Error(), "truenas.apiKey")
	assert.False(t, clientCreated)
}

func TestRequestWithoutSecretsClonesAndStripsSecrets(t *testing.T) {
	original := &csi.NodePublishVolumeRequest{
		VolumeId: "volume-1",
		Secrets:  map[string]string{"api-key": "super-secret"},
	}

	redacted, ok := requestWithoutSecrets(original).(*csi.NodePublishVolumeRequest)
	require.True(t, ok)
	assert.NotSame(t, original, redacted)
	assert.Empty(t, redacted.GetSecrets())
	assert.Equal(t, "super-secret", original.GetSecrets()["api-key"], "logging redaction must not mutate the handler request")
}
