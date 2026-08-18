package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsecureFlagDefaultsToFalse(t *testing.T) {
	assert.False(t, *insecure)
}

func TestMaxOutputFlagDefaultsToTwoMiB(t *testing.T) {
	assert.Equal(t, int64(2*1024*1024), *maxOut)

	f := flag.Lookup("max-output")
	require.NotNil(t, f, "-max-output flag must be registered")
	assert.Equal(t, fmt.Sprintf("%d", defaultMaxOutput), f.DefValue)
}

func TestFormatCallOutputUnderCapIsUnchanged(t *testing.T) {
	payload := []byte(`{"key": "value"}`)
	assert.Equal(t, string(payload), formatCallOutput(payload, int64(len(payload))))
	assert.Equal(t, string(payload), formatCallOutput(payload, defaultMaxOutput))
}

func TestFormatCallOutputTruncatesOverCap(t *testing.T) {
	payload := []byte(strings.Repeat("x", 100))
	out := formatCallOutput(payload, 10)

	// The first max-output bytes are preserved verbatim...
	require.True(t, strings.HasPrefix(out, strings.Repeat("x", 10)))
	assert.NotContains(t, out, strings.Repeat("x", 11), "must print exactly max-output payload bytes")
	// ...followed by a notice with the shown/total sizes and the flag hint.
	assert.Contains(t, out, "output truncated")
	assert.Contains(t, out, "first 10 of 100 bytes")
	assert.Contains(t, out, "-max-output 100")
}

func TestFormatCallOutputNonPositiveCapMeansUnlimited(t *testing.T) {
	payload := []byte(strings.Repeat("y", 64))
	assert.Equal(t, string(payload), formatCallOutput(payload, 0))
	assert.Equal(t, string(payload), formatCallOutput(payload, -1))
}

// TestCleanupFlagUsageNamesTheOnlyHonoringCommand pins the H-4 doc fix: the
// -cleanup flag is implemented only for nvmeof-audit, and both the flag usage
// string and the printHelp text must say so instead of the old ambiguous
// "for audit commands" wording.
func TestCleanupFlagUsageNamesTheOnlyHonoringCommand(t *testing.T) {
	f := flag.Lookup("cleanup")
	require.NotNil(t, f, "-cleanup flag must be registered")
	assert.Contains(t, f.Usage, "nvmeof-audit")
	assert.Contains(t, f.Usage, "iscsi-audit is report-only")
	assert.NotContains(t, f.Usage, "for audit commands", "old ambiguous wording must not return")
}

// TestPrintHelpDocumentsCleanupScopeAndOutputCap asserts the printed help
// names nvmeof-audit as the only -cleanup command, marks iscsi-audit as
// report-only, and mentions the -max-output cap on raw call output.
func TestPrintHelpDocumentsCleanupScopeAndOutputCap(t *testing.T) {
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	printHelp()
	require.NoError(t, w.Close())
	captured, err := io.ReadAll(r)
	require.NoError(t, err)
	help := string(captured)

	assert.Contains(t, help, "nvmeof-audit")
	assert.Contains(t, help, "ONLY command")
	assert.Contains(t, help, "that honors -cleanup")
	assert.Contains(t, help, "report-only; does NOT honor -cleanup")
	assert.Contains(t, help, "-max-output")
}
