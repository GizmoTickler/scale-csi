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

// TestStopCapacityGaugesBeforeStartPreventsLoopFromEverRunning is the
// regression test for the R1 shutdown race on capacityCancel: startCapacityGauges
// has the identical shape as startOrphanReconcile's C7 defect (a plain nil check
// on the CancelFunc field). startCapacityGauges is called from Run() after
// ensureNFSProtocols (a real TrueNAS network call) and startStartupAttachmentReconcile,
// so a Stop() landing while either is in flight must be observed here and
// prevent the poll loop from EVER launching, not merely fail to cancel a loop
// that started anyway. Before the fix, stopCapacityGauges only canceled
// whatever capacityCancel happened to already be assigned, with no memory that
// a stop was ever requested, so a Stop() that raced ahead of the assignment was
// silently lost and the subsequent Start() launched a poll loop that calls
// d.truenasClient.DatasetGet against an already-closed client, with no
// goroutine ever joined by Stop().
func TestStopCapacityGaugesBeforeStartPreventsLoopFromEverRunning(t *testing.T) {
	const (
		parent    = "pool/parent-race"
		available = float64(999)
		used      = float64(111)
	)
	client := truenas.NewMockClient()
	client.Datasets[parent] = &truenas.Dataset{
		ID:        parent,
		Name:      parent,
		Pool:      "pool-race",
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

	// Stop BEFORE Start ever runs — the observable analogue of a Stop() that
	// wins the race against capacityCancel's assignment (e.g. landing while
	// ensureNFSProtocols or the startup fencing reconcile is still in flight in
	// Run()).
	d.stopCapacityGauges()
	d.startCapacityGauges()
	t.Cleanup(d.stopCapacityGauges)

	assert.Nil(t, d.capacityCancel, "a poll loop must never launch once Stop() has already been observed")
	require.Never(t, func() bool {
		return testutil.ToFloat64(poolAvailableBytes.WithLabelValues("pool-race", parent)) == available
	}, 300*time.Millisecond, 10*time.Millisecond,
		"a capacity gauge poll loop must never launch once Stop() has already been observed")
}
