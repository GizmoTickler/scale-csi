package driver

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestCapacityGaugeIntervalDuration(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "empty defaults to 60s", raw: "", want: 60 * time.Second},
		{name: "whitespace defaults to 60s", raw: "   ", want: 60 * time.Second},
		{name: "below floor clamps to 30s", raw: "10s", want: 30 * time.Second},
		{name: "at floor stays", raw: "30s", want: 30 * time.Second},
		{name: "above floor stays", raw: "90s", want: 90 * time.Second},
		{name: "invalid errors", raw: "soon", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (CapacityConfig{GaugeInterval: tc.raw}).GaugeIntervalDuration()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCapacityGaugeLoopPublishesMetrics proves the opt-in poll loop (E4/K13)
// samples the parent dataset once and publishes both gauges (capacity = used +
// available), and that stop joins the goroutine.
func TestCapacityGaugeLoopPublishesMetrics(t *testing.T) {
	const (
		parent    = "pool/parent"
		available = float64(1000)
		used      = float64(4000)
	)
	client := truenas.NewMockClient()
	client.Datasets[parent] = &truenas.Dataset{
		ID:        parent,
		Name:      parent,
		Pool:      "pool",
		Type:      "FILESYSTEM",
		Available: truenas.DatasetProperty{Parsed: available},
		Used:      truenas.DatasetProperty{Parsed: used},
	}
	d := &Driver{
		config: &Config{
			ZFS:      ZFSConfig{DatasetParentName: parent},
			Capacity: CapacityConfig{GaugeEnabled: true, GaugeInterval: "30s"},
		},
		truenasClient: client,
	}

	d.startCapacityGauges()
	require.NotNil(t, d.capacityCancel, "gauge loop must start when gaugeEnabled")

	assert.Eventually(t, func() bool {
		return testutil.ToFloat64(poolAvailableBytes.WithLabelValues("pool", parent)) == available &&
			testutil.ToFloat64(poolCapacityBytes.WithLabelValues("pool", parent)) == used+available
	}, 2*time.Second, 10*time.Millisecond, "gauges were not published from the parent dataset sample")

	d.stopCapacityGauges()
}

// TestCapacityGaugeLoopDisabledByDefault proves the loop is a no-op unless
// capacity.gaugeEnabled is set (zero new API calls by default).
func TestCapacityGaugeLoopDisabledByDefault(t *testing.T) {
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: truenas.NewMockClient(),
	}
	d.startCapacityGauges()
	assert.Nil(t, d.capacityCancel, "gauge loop must not start when gaugeEnabled is false")
	d.stopCapacityGauges() // safe no-op
}
