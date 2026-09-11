package util

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// longOptionPattern matches a long option as it appears both in an argv element
// we emit ("--nr-io-queues=4") and in nvme-cli's help text ("--nr-io-queues=<NUM>").
var longOptionPattern = regexp.MustCompile(`--[a-zA-Z0-9][a-zA-Z0-9_-]*`)

// TestRunNVMeConnectEmitsOnlyOptionsNVMeCLIAccepts is the guard for a class of
// defect that NO mock-based test in this repo can catch, and that shipped
// undetected: the driver emitted "--fast-io-fail-tmo" while nvme-cli spells that
// one option with UNDERSCORES. A fake exec stub validates our spelling against
// our own expectation, so the pre-existing unit test asserted the broken argv and
// passed. On real hardware every connect failed with
// "connect: unrecognized option", which would have failed NodeStageVolume for
// every NVMe-oF volume on any re-stage -- node reboot, drain, or rollout.
//
// testdata/nvme_connect_help.txt is REAL `nvme connect --help` output captured
// from a production cluster node (nvme-cli 2.15). Note what it proves: nvme-cli
// is inconsistent on purpose-built ground -- --ctrl-loss-tmo, --reconnect-delay,
// --nr-io-queues and --keep-alive-tmo are hyphenated, while --fast_io_fail_tmo
// and --tls_key are not. There is no rule to infer; the only defense is checking
// against what the binary actually accepts.
//
// Refresh the fixture with:
//
//	ssh core@<node> 'sudo nvme connect --help' > pkg/util/testdata/nvme_connect_help.txt
func TestRunNVMeConnectEmitsOnlyOptionsNVMeCLIAccepts(t *testing.T) {
	helpBytes, err := os.ReadFile("testdata/nvme_connect_help.txt")
	require.NoError(t, err, "real nvme connect --help fixture must be present")
	help := string(helpBytes)

	accepted := make(map[string]struct{})
	for _, opt := range longOptionPattern.FindAllString(help, -1) {
		accepted[opt] = struct{}{}
	}
	require.Contains(t, accepted, "--nqn", "fixture does not look like nvme connect --help")

	original := nvmeConnectCommand
	t.Cleanup(func() { nvmeConnectCommand = original })
	var got []string
	nvmeConnectCommand = func(_ context.Context, args ...string) ([]byte, error) {
		got = append([]string(nil), args...)
		return []byte("connected"), nil
	}

	// Set every optional knob so the argv covers the full surface, not just the
	// default path -- a flag only emitted when configured is exactly where a
	// spelling error hides longest.
	queues, writeQueues := 4, 2
	keepAlive := 7 * time.Second
	require.NoError(t, runNVMeConnect(context.Background(), "tcp", "192.0.2.10", "4420", "nqn.test:vol", &NVMeoFConnectOptions{
		FastIOFailTmo: 15 * time.Second,
		NrIOQueues:    &queues,
		NrWriteQueues: &writeQueues,
		KeepAliveTmo:  &keepAlive,
	}))
	require.NotEmpty(t, got)

	checked := 0
	for _, arg := range got {
		if !strings.HasPrefix(arg, "--") {
			continue
		}
		name := longOptionPattern.FindString(arg)
		require.NotEmpty(t, name, "could not parse option out of %q", arg)
		checked++
		if _, ok := accepted[name]; !ok {
			t.Errorf("runNVMeConnect emits %q, which real nvme-cli does not accept (from %q); "+
				"nvme connect --help lists no such option, so every connect would fail with "+
				"\"unrecognized option\" on a real node", name, arg)
		}
	}
	require.GreaterOrEqual(t, checked, 6, "expected the full option surface to be exercised")
}
