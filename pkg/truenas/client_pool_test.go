package truenas

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientPoolHonorsMaxConnections proves the WebSocket connection pool is sized
// exactly by ClientConfig.MaxConnections. The driver wires truenas.maxConnections
// into this field; lazy-connect mode builds the pool without dialing, so the pool
// length can be asserted without a server.
func TestClientPoolHonorsMaxConnections(t *testing.T) {
	for _, n := range []int{1, 5, 9, 16} {
		client, err := NewClient(&ClientConfig{
			Host:           "truenas.example.test",
			Port:           443,
			Protocol:       "https",
			APIKey:         "test-api-key",
			MaxConnections: n,
			LazyConnect:    true,
		})
		require.NoError(t, err)
		assert.Len(t, client.pool, n, "pool must be sized exactly by MaxConnections")
		_ = client.Close()
	}
}

// TestClientPoolDefaultsToFive proves an unset MaxConnections keeps the historical
// five-connection pool, so wiring truenas.maxConnections is byte-identical when the
// key is absent.
func TestClientPoolDefaultsToFive(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		Host:        "truenas.example.test",
		Port:        443,
		Protocol:    "https",
		APIKey:      "test-api-key",
		LazyConnect: true,
	})
	require.NoError(t, err)
	assert.Len(t, client.pool, 5, "an unset MaxConnections must default to the historical pool of 5")
	_ = client.Close()
}
