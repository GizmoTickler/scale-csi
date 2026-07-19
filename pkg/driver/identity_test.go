package driver

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type disconnectedProbeClient struct {
	*truenas.MockClient
	callCount atomic.Int32
}

func (c *disconnectedProbeClient) IsConnected() bool {
	return false
}

func (c *disconnectedProbeClient) ActiveConnectionCount() int {
	return 0
}

func (c *disconnectedProbeClient) Call(context.Context, string, ...interface{}) (interface{}, error) {
	c.callCount.Add(1)
	return nil, assert.AnError
}

func TestProbeReadyWhenTrueNASDisconnected(t *testing.T) {
	client := &disconnectedProbeClient{MockClient: truenas.NewMockClient()}
	driver := &Driver{truenasClient: client}
	driver.ready.Store(true)

	response, err := driver.Probe(context.Background(), &csi.ProbeRequest{})

	require.NoError(t, err)
	require.NotNil(t, response.GetReady())
	assert.True(t, response.GetReady().GetValue())
	assert.Zero(t, client.callCount.Load(), "Probe must not make a blocking backend call")
	assert.Zero(t, testutil.ToFloat64(truenasConnectionStatus))
	assert.Zero(t, testutil.ToFloat64(truenasConnectionsActive))
}

type pooledProbeClient struct {
	*truenas.MockClient
	activeConnections int
}

func (c *pooledProbeClient) ActiveConnectionCount() int {
	return c.activeConnections
}

func TestObserveTrueNASConnectionReportsActivePoolSize(t *testing.T) {
	driver := &Driver{truenasClient: &pooledProbeClient{
		MockClient:        truenas.NewMockClient(),
		activeConnections: 3,
	}}

	assert.True(t, driver.observeTrueNASConnection())
	assert.Equal(t, float64(3), testutil.ToFloat64(truenasConnectionsActive))
}
