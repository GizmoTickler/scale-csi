package driver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newMultipathAPICallCountDriver builds an NVMe-oF driver over the counting
// client. addresses non-empty enables E-6 multipath.
func newMultipathAPICallCountDriver(t *testing.T, client *apiCallCountingClient, addresses []string) *Driver {
	t.Helper()
	d := &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi.nvmeof",
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			NVMeoF: NVMeoFConfig{
				Enabled:               true,
				Transport:             "TCP",
				TransportAddress:      "192.0.2.20",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
				Multipath:             len(addresses) > 0,
				Addresses:             addresses,
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d
}

// TestNVMeoFMultipathAPICallGolden is the F-9 golden the design asked for and
// the sprint shipped without: it pins the EXTRA per-volume API cost of enabling
// E-6 multipath, so a future change that turns a bounded N-port fan-out into an
// unbounded one fails here instead of in production.
//
// The multipath cost is exactly (N-1) additional NVMeoFGetOrCreatePort +
// NVMeoFPortSubsysCreate pairs, i.e. 2*(N-1) calls, where N is the number of
// distinct storage addresses. Nothing else changes.
func TestNVMeoFMultipathAPICallGolden(t *testing.T) {
	const (
		singlePathCalls = 12
		addressCount    = 4
	)
	// 4 addresses => 3 extra ports beyond the single-path baseline, each costing
	// one get-or-create plus one port_subsys.create.
	multipathCalls := singlePathCalls + 2*(addressCount-1)

	t.Run("single path baseline", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newMultipathAPICallCountDriver(t, client, nil)
		_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("mp-single", "nvmeof"))
		require.NoError(t, err)
		assertAPICallCount(t, "CreateVolume fresh NVMe-oF single path", client, singlePathCalls)
	})

	t.Run("multipath fan-out is bounded at 2*(N-1) extra calls", func(t *testing.T) {
		client := newAPICallCountingClient()
		d := newMultipathAPICallCountDriver(t, client, []string{"192.0.2.21", "192.0.2.22", "192.0.2.23"})
		_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("mp-multi", "nvmeof"))
		require.NoError(t, err)
		assertAPICallCount(t, "CreateVolume fresh NVMe-oF multipath", client, multipathCalls)
	})

	t.Run("duplicate addresses do not multiply the fan-out", func(t *testing.T) {
		client := newAPICallCountingClient()
		// TransportAddress repeated plus a duplicate: de-duplication must collapse
		// these to 2 distinct addresses.
		d := newMultipathAPICallCountDriver(t, client, []string{"192.0.2.20", "192.0.2.21", "192.0.2.21"})
		_, err := d.CreateVolume(context.Background(), apiCallCountVolumeRequest("mp-dup", "nvmeof"))
		require.NoError(t, err)
		assertAPICallCount(t, "CreateVolume fresh NVMe-oF multipath deduped", client, singlePathCalls+2)
	})
}

// TestMultipathConvergesExistingVolumes is the F-4 regression test. The
// already-exists fast path used to return BEFORE the port-association loop, so
// flipping nvmeof.multipath=true on a live install added the extra port
// associations for NEW volumes only — while the publish context advertised all
// addresses for EVERY volume. Existing volumes therefore advertised paths they
// had no association for: a guaranteed partial-path/connect failure when a node
// converged the advertised address set.
func TestMultipathConvergesExistingVolumes(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()

	// Provision with multipath OFF (the pre-flip state).
	d := newMultipathAPICallCountDriver(t, client, nil)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("mp-existing", "nvmeof"))
	require.NoError(t, err)

	// Operator flips nvmeof.multipath=true and restarts the controller.
	d.config.NVMeoF.Multipath = true
	d.config.NVMeoF.Addresses = []string{"192.0.2.21", "192.0.2.22", "192.0.2.23"}
	client.resetCalls()

	// A publish takes the already-exists fast path.
	require.NoError(t, nvmeoFShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/mp-existing", "mp-existing", &fenceResolution{}))

	_, counts := client.callSnapshot()
	assert.Equal(t, 4, counts["NVMeoFGetOrCreatePort"],
		"an existing volume must get a port association per advertised address, not zero")
	assert.Equal(t, 4, counts["NVMeoFPortSubsysCreate"],
		"an existing volume must get a port_subsys association per advertised address")

	// The publish context advertises exactly the addresses that were associated.
	volumeContext := map[string]string{}
	require.NoError(t, d.nvmeofVolumeContext(ctx, nil, "pool/parent/mp-existing", volumeContext))
	var advertised []string
	require.NoError(t, json.Unmarshal([]byte(volumeContext["addresses"]), &advertised))
	assert.Equal(t, []string{"192.0.2.20", "192.0.2.21", "192.0.2.22", "192.0.2.23"}, advertised)
}

// TestMultipathDisabledAddsNoCallsOnEnsure proves the convergence hook is free
// when multipath is off (the default): the already-exists fast path must issue
// exactly the same calls it did before.
func TestMultipathDisabledAddsNoCallsOnEnsure(t *testing.T) {
	ctx := context.Background()
	client := newAPICallCountingClient()
	d := newMultipathAPICallCountDriver(t, client, nil)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("mp-off", "nvmeof"))
	require.NoError(t, err)

	client.resetCalls()
	require.NoError(t, nvmeoFShareBackend{d}.EnsureShare(ctx, nil, "pool/parent/mp-off", "mp-off", &fenceResolution{}))

	_, counts := client.callSnapshot()
	assert.Zero(t, counts["NVMeoFGetOrCreatePort"], "multipath-off ensure must not touch ports")
	assert.Zero(t, counts["NVMeoFPortSubsysCreate"], "multipath-off ensure must not create associations")

	// And no addresses key is advertised at all.
	volumeContext := map[string]string{}
	require.NoError(t, d.nvmeofVolumeContext(ctx, nil, "pool/parent/mp-off", volumeContext))
	_, hasAddresses := volumeContext["addresses"]
	assert.False(t, hasAddresses, "the default publish context must carry no multipath addresses key")
}

// TestNVMeoFPortPerfDriftIsReported is the F-7 regression test: nvmeof.portPerf
// fields are applied at port CREATE only, so a changed value on an install whose
// ports already exist is a silent no-op. The drift must be detectable (and is
// surfaced as an operator warning by NVMeoFGetOrCreatePort).
func TestNVMeoFPortPerfDriftIsReported(t *testing.T) {
	live := &truenas.NVMeoFPort{
		ID: 1, Transport: "TCP", Address: "192.0.2.20", Port: 4420,
		InlineDataSize: intPtr(16384),
	}

	// No configured fields: nothing to report.
	assert.Empty(t, truenas.NVMeoFPortCreateOptions{}.Drift(live))

	// Matching value: nothing to report.
	assert.Empty(t, truenas.NVMeoFPortCreateOptions{InlineDataSize: intPtr(16384)}.Drift(live))

	// Changed value, and a field the live port does not carry at all.
	drift := truenas.NVMeoFPortCreateOptions{
		InlineDataSize: intPtr(32768),
		MaxQueueSize:   intPtr(256),
		PiEnable:       boolPtr(true),
	}.Drift(live)
	require.Len(t, drift, 3)
	assert.Contains(t, drift[0], "inline_data_size")
	assert.Contains(t, drift[0], "live=16384")
	assert.Contains(t, drift[0], "configured=32768")
	assert.Contains(t, drift[1], "max_queue_size")
	assert.Contains(t, drift[1], "live=unset")
	assert.Contains(t, drift[2], "pi_enable")

	// A nil port is not a drift report.
	assert.Empty(t, truenas.NVMeoFPortCreateOptions{PiEnable: boolPtr(true)}.Drift(nil))
}
