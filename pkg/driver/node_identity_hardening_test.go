package driver

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// TestNodeIdentityCommandHardensAgainstWedgedDescendant is the regression test
// for the first half of R3: nodeIdentityCommand's default implementation ran
// exec.CommandContext(...).Output() with no hardening at all — the one exec
// site the earlier host-command hardening sweep missed. It runs
// "nvme show-hostnqn", which resolves to the same bash-wrapper-into-
// nsenter-into-PID-1 shape util.HardenCmd exists for: killing the direct
// bash child on cancellation leaves the nsenter'd grandchild holding the
// inherited output pipe, so Output() blocks in Wait forever regardless of the
// command's own context deadline.
//
// `sh -c "sleep N & wait $!"` reproduces the shape exactly (mirrors
// pkg/util/iscsi_test.go's TestHardenCmdBoundsWaitOnWedgedDescendant): sleep
// is forked, not exec'd, by sh, so it independently holds the inherited pipe
// fds regardless of sh's own lifetime. This exercises the production
// nodeIdentityCommand closure directly (not a test double), so it proves the
// actual wiring rather than merely that util.HardenCmd works in isolation.
//
// The sleep duration (30s) is deliberately far longer than the observation
// budget below (4s): without hardening, Wait only unblocks when the
// grandchild process exits ON ITS OWN, so the test must not let that happen
// by accident within the budget, or it would pass regardless of whether
// hardening is wired up.
func TestNodeIdentityCommandHardensAgainstWedgedDescendant(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := nodeIdentityCommand(ctx, "sh", "-c", "sleep 30 & wait $!")
		done <- err
	}()

	select {
	case err := <-done:
		t.Logf("nodeIdentityCommand returned (process-group kill and/or WaitDelay unblocked Wait): %v", err)
	case <-time.After(4 * time.Second):
		t.Fatal("nodeIdentityCommand hung well past its 100ms context deadline; the process-group kill " +
			"should have reaped the wedging descendant almost immediately, so it is not hardened " +
			"against a wedged host-tool descendant")
	}
}

// TestNewDriverNodeIdentityDiscoveryIsBounded is the regression test for the
// second half of R3: NewDriver called discoverNodeIdentity with
// context.Background() — no deadline whatsoever — during driver construction,
// BEFORE the gRPC listener is created. A wedged host nvme-cli (or, as here, a
// stand-in that blocks exactly like one) therefore hung node startup
// indefinitely and the pod never became serviceable. NewDriver must instead
// derive a bounded context from commandTimeouts.nvme (via util.GetConfig(),
// populated from cfg.Config.CommandTimeouts.NVMe), so a wedged discovery call
// fails driver construction within that budget instead of hanging forever.
func TestNewDriverNodeIdentityDiscoveryIsBounded(t *testing.T) {
	originalCommand := nodeIdentityCommand
	t.Cleanup(func() { nodeIdentityCommand = originalCommand })
	// Simulates a wedged "nvme show-hostnqn": it never returns on its own and
	// only unblocks if the context passed to it is actually canceled.
	nodeIdentityCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = NewDriver(&DriverConfig{
			Name: "csi.scale.io", Version: "test", NodeID: "worker-a",
			Endpoint: "unix:///tmp/scale-csi-bounded-identity-test.sock", RunNode: true,
			Config: &Config{
				ZFS:             ZFSConfig{DatasetParentName: "tank/csi"},
				NFS:             NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
				CommandTimeouts: CommandTimeoutConfig{NVMe: 1},
			},
		})
	}()

	select {
	case <-done:
		// Expected: the 1s commandTimeouts.nvme budget bounds the wedged
		// discovery call and NewDriver returns instead of hanging.
	case <-time.After(10 * time.Second):
		t.Fatal("NewDriver hung on node identity discovery instead of bounding it to commandTimeouts.nvme; " +
			"discoverNodeIdentity must not be called with context.Background()")
	}
}
