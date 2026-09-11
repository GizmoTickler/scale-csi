package driver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// newLifecycleDriver builds the smallest Driver that Run() will actually serve:
// no controller, no node, no health port, no debug endpoint, so Run() reaches
// Serve() with no background workers to unwind. The endpoint is a unix socket
// under t.TempDir() so concurrent tests cannot collide.
func newLifecycleDriver(t *testing.T) *Driver {
	t.Helper()
	return &Driver{
		name:          "org.scale.csi.test",
		version:       "test",
		nodeID:        "node-0",
		endpoint:      "unix://" + filepath.Join(t.TempDir(), "csi.sock"),
		truenasClient: truenas.NewMockClient(),
	}
}

// TestStopBeforeRunPublishesServersDoesNotStrandRun proves the OUTCOME of the
// unsynchronized server fields, not merely that they were unsynchronized.
//
// cmd/scale-csi/main.go calls Stop() from the signal goroutine while Run() is
// still on the main goroutine. Pre-fix, a Stop() that landed before Run()
// assigned d.server observed nil, skipped GracefulStop(), closed the TrueNAS
// client anyway and returned — and Run() then built its own gRPC server and
// served forever against a dead client, with nothing left that could ever stop
// it. The pod hung until SIGKILL.
//
// Sequencing Stop() strictly before Run() (the goroutine start orders them, so
// this is deterministic and reports no data race either way) reproduces exactly
// that interleaving: post-fix Run() must observe the recorded stop and return
// instead of serving.
func TestStopBeforeRunPublishesServersDoesNotStrandRun(t *testing.T) {
	d := newLifecycleDriver(t)

	// The shutdown signal arrives while Run() has not started yet.
	d.Stop()

	runErr := make(chan error, 1)
	go func() {
		runErr <- d.Run()
	}()

	select {
	case err := <-runErr:
		require.NoError(t, err, "Run() must return cleanly once Stop() has already been recorded")
	case <-time.After(10 * time.Second):
		// Unblock the stranded Serve() so the rest of the package can still run,
		// then fail: this is the SIGKILL-until-hang outcome.
		d.Stop()
		t.Fatal("Run() kept serving gRPC after Stop() had already closed the TrueNAS client: the pod is stranded until SIGKILL")
	}
}

// TestRunAndStopServerFieldsAreSynchronized is the race half of the same
// defect: Run() writes d.server/d.healthServer/d.debugServer while Stop() reads
// them from another goroutine with no happens-before edge between the two.
// Under -race the pre-fix code reports a DATA RACE on d.server here regardless
// of which side wins the wall-clock, because the goroutine-start edge orders
// the test goroutine BEFORE Run() while Stop()'s accesses happen after it.
func TestRunAndStopServerFieldsAreSynchronized(t *testing.T) {
	for i := 0; i < 5; i++ {
		d := newLifecycleDriver(t)

		runErr := make(chan error, 1)
		go func() {
			runErr <- d.Run()
		}()

		// Concurrent with Run()'s startup: no synchronization between this
		// call's reads and Run()'s writes before the fix.
		d.Stop()

		select {
		case err := <-runErr:
			require.NoError(t, err, "Run() must return cleanly whichever side of the race wins")
		case <-time.After(10 * time.Second):
			d.Stop()
			t.Fatal("Run() never returned after a concurrent Stop()")
		}
	}
}
