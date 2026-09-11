package driver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// TestConvergeNVMeoFPathsTopUpInheritsConnectKnobs is the load-bearing
// regression test for the N4 driver-side wiring: it FAILS on the pre-fix code,
// where convergeNVMeoFPaths built its secondary-path budget as a bare
// &util.NVMeoFConnectOptions{DeviceTimeout: ...} and silently dropped every
// other CLI knob. That made the top-up paths — the extra multipath paths whose
// whole purpose is to survive a failure — the only ones connected WITHOUT
// --fast_io_fail_tmo, so I/O on a dead controller would still block there
// rather than fail over. A partial application of N4 is worse than none,
// because it is invisible until the failure it was meant to handle.
func TestConvergeNVMeoFPathsTopUpInheritsConnectKnobs(t *testing.T) {
	original := nvmeConnectPathWithSubsystems
	t.Cleanup(func() { nvmeConnectPathWithSubsystems = original })

	queues := 6
	writeQueues := 2
	keepAlive := 7 * time.Second
	connectOpts := &util.NVMeoFConnectOptions{
		DeviceTimeout: 90 * time.Second,
		FastIOFailTmo: 20 * time.Second,
		NrIOQueues:    &queues,
		NrWriteQueues: &writeQueues,
		KeepAliveTmo:  &keepAlive,
	}

	seen := make(map[string]*util.NVMeoFConnectOptions)
	nvmeConnectPathWithSubsystems = func(_ context.Context, _ string, uri string, opts *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		seen[uri] = opts
		return "", nil
	}

	// needDevice=false routes EVERY address through the short top-up budget,
	// which is exactly the path that dropped the knobs before the fix.
	_, connectErrs, err := convergeNVMeoFPaths(
		context.Background(),
		"nqn.n4:topup", "tcp", "4420",
		[]string{"10.0.0.1", "10.0.0.2"},
		connectOpts,
		nil,
		false,
	)
	require.NoError(t, err)
	require.Empty(t, connectErrs)
	require.Len(t, seen, 2, "both addresses must be attempted through the top-up budget")

	for uri, opts := range seen {
		require.NotNil(t, opts, "top-up connect for %s got nil options", uri)
		assert.Equal(t, 20*time.Second, opts.FastIOFailTmo,
			"top-up path %s must inherit --fast_io_fail_tmo", uri)
		if assert.NotNil(t, opts.NrIOQueues, "top-up path %s dropped --nr-io-queues", uri) {
			assert.Equal(t, 6, *opts.NrIOQueues)
		}
		if assert.NotNil(t, opts.NrWriteQueues, "top-up path %s dropped --nr-write-queues", uri) {
			assert.Equal(t, 2, *opts.NrWriteQueues)
		}
		if assert.NotNil(t, opts.KeepAliveTmo, "top-up path %s dropped --keep-alive-tmo", uri) {
			assert.Equal(t, 7*time.Second, *opts.KeepAliveTmo)
		}
		// The budget IS deliberately overridden; only the knobs are inherited.
		assert.Equal(t, nvmeSecondaryPathConvergeBudget, opts.DeviceTimeout,
			"top-up path %s must use the short converge budget", uri)
	}

	// Aliasing guard: the inherited copy must not be the caller's struct, or a
	// later budget override would mutate the caller's options in place.
	assert.Equal(t, 90*time.Second, connectOpts.DeviceTimeout,
		"caller's options were mutated by the top-up copy")
}

// TestNVMeConnectOptionsMapsConfiguredKnobs covers the config->util mapping.
// NOTE (tautology disclosure): unlike the test above, this one CANNOT fail on
// the pre-fix tree, because nvmeConnectOptions did not exist there. It is kept
// as a semantics lock on the two non-obvious mappings — a zero FastIOFailTmo
// must stay zero so the util layer applies its own default, and only an
// explicit negative may disable the flag — not as proof of the defect.
func TestNVMeConnectOptionsMapsConfiguredKnobs(t *testing.T) {
	queues := 4
	keepAlive := 5
	d := &Driver{config: &Config{}}
	d.config.NVMeoF.DeviceWaitTimeout = 60
	d.config.NVMeoF.Connect = NVMeoFConnectConfig{
		FastIOFailTmo: 25,
		NrIOQueues:    &queues,
		KeepAliveTmo:  &keepAlive,
	}

	opts := d.nvmeConnectOptions(30 * time.Second)
	require.NotNil(t, opts)
	assert.Equal(t, 30*time.Second, opts.DeviceTimeout)
	assert.Equal(t, 25*time.Second, opts.FastIOFailTmo)
	require.NotNil(t, opts.NrIOQueues)
	assert.Equal(t, 4, *opts.NrIOQueues)
	assert.Nil(t, opts.NrWriteQueues, "unset knob must stay omitted")
	require.NotNil(t, opts.KeepAliveTmo)
	assert.Equal(t, 5*time.Second, *opts.KeepAliveTmo)

	// Copied, not aliased: mutating the returned options must not reach config.
	*opts.NrIOQueues = 99
	assert.Equal(t, 4, *d.config.NVMeoF.Connect.NrIOQueues, "config pointer was aliased")

	// Zero stays zero (util applies its own 15s default); negative is preserved
	// verbatim so only an explicit opt-out disables the flag.
	d.config.NVMeoF.Connect = NVMeoFConnectConfig{}
	assert.Zero(t, d.nvmeConnectOptions(time.Second).FastIOFailTmo)
	d.config.NVMeoF.Connect = NVMeoFConnectConfig{FastIOFailTmo: -1}
	assert.Equal(t, -1*time.Second, d.nvmeConnectOptions(time.Second).FastIOFailTmo)
}
