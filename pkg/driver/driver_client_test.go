package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestNewDriverNodeOnlyEnablesLazyConnect(t *testing.T) {
	original := newTrueNASClient
	t.Cleanup(func() { newTrueNASClient = original })
	var captured *truenas.ClientConfig
	newTrueNASClient = func(cfg *truenas.ClientConfig) (truenas.ClientInterface, error) {
		captured = cfg
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
				APIKey:                "test-key",
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
	require.NotNil(t, captured)
	assert.True(t, captured.LazyConnect)
	drv.Stop()
}
