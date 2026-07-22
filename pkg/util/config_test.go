package util

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommandContextBoundsCommandByInboundDeadline proves the FIX 2 contract: a
// CO-supplied deadline shorter than the configured timeout must bound the actual
// command, instead of being dropped by a fresh context.Background() timer.
func TestCommandContextBoundsCommandByInboundDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// Configured timeout (30s) far exceeds the inbound deadline; the floor would
	// raise the computed timeout to 5s, but the parent deadline must still cap the
	// derived context at ~250ms.
	derived, cancelCmd, err := commandContext(ctx, 30*time.Second)
	require.NoError(t, err)
	defer cancelCmd()

	start := time.Now()
	cmd := exec.CommandContext(derived, "sleep", "5")
	runErr := cmd.Run()
	elapsed := time.Since(start)

	require.Error(t, runErr, "the sleep command must be killed by the inbound deadline")
	assert.Less(t, elapsed, 2*time.Second,
		"command must be bounded by the 250ms inbound deadline, not the 30s configured timeout")
}

// TestCommandContextExpiredReturnsImmediatelyWithoutExec proves an already-expired
// inbound context fails fast so a doomed command is never launched.
func TestCommandContextExpiredReturnsImmediatelyWithoutExec(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, _, err := commandContext(ctx, 30*time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestCommandContextAppliesFloorWithoutDeadline proves that with no inbound
// deadline the configured timeout is used, raised to the small floor.
func TestCommandContextAppliesFloorWithoutDeadline(t *testing.T) {
	derived, cancel, err := commandContext(context.Background(), 100*time.Millisecond)
	require.NoError(t, err)
	defer cancel()

	deadline, ok := derived.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(minCommandTimeout), deadline, time.Second,
		"a sub-floor configured timeout must be raised to the floor when no deadline is present")
}

// TestCommandContextUsesShorterOfDeadlineAndConfigured proves the effective
// timeout is min(configured, remaining deadline) when the deadline is the larger
// of the two but still shorter than a big configured timeout.
func TestCommandContextUsesShorterOfDeadlineAndConfigured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	derived, cancelCmd, err := commandContext(ctx, 30*time.Second)
	require.NoError(t, err)
	defer cancelCmd()

	deadline, ok := derived.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(3*time.Second), deadline, time.Second,
		"the inbound deadline (3s) must win over the configured timeout (30s)")
}
