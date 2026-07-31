// Package chart holds helm-template assertion tests that guard the chart-plumbing
// DoD: every driver config key must be plumbed through values.yaml,
// values.schema.json, and templates/configmap.yaml so it actually renders into the
// driver config. Batch 14 shipped a driver config key without chart plumbing and
// flux-local caught it; these tests catch the same class of regression in `go test`.
package chart

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func chartDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "charts", "scale-csi")
}

func helmTemplate(t *testing.T, extraArgs ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart template assertion")
	}
	args := append([]string{"template", "scale-csi", chartDir(t)}, extraArgs...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v failed: %v\n%s", args, err, out)
	}
	return string(out)
}

// TestChartTombstoneReaperScanFallbackPlumbing proves the new
// reconcile.tombstoneReaper.scanFallback.enabled key renders end-to-end into the
// driver configmap: false by default, true when set. A driver config key that is
// not plumbed through the chart fails here.
func TestChartTombstoneReaperScanFallbackPlumbing(t *testing.T) {
	const defaultBlock = "      tombstoneReaper:\n        scanFallback:\n          enabled: false\n"
	const enabledBlock = "      tombstoneReaper:\n        scanFallback:\n          enabled: true\n"

	if out := helmTemplate(t); !strings.Contains(out, defaultBlock) {
		t.Errorf("default render does not carry scanFallback.enabled=false in the configmap")
	}
	if out := helmTemplate(t, "--set", "reconcile.tombstoneReaper.scanFallback.enabled=true"); !strings.Contains(out, enabledBlock) {
		t.Errorf("--set scanFallback.enabled=true did not propagate into the rendered configmap")
	}
}

// TestChartDeprecatedKeysNotRendered proves the retired iscsi.extentAvailThreshold
// and nvmeof.commandTimeout keys are no longer rendered into the driver configmap.
// Both were parsed but consumed by nothing (the nvme CLI timeout is
// commandTimeouts.nvme), so rendering them would suggest a wiring that does not
// exist. The plural commandTimeouts block must remain.
func TestChartDeprecatedKeysNotRendered(t *testing.T) {
	out := helmTemplate(t, "--set", "iscsi.enabled=true", "--set", "nvmeof.enabled=true", "--set", "nvmeof.subsystemAllowAnyHost=true")
	if strings.Contains(out, "extentAvailThreshold:") {
		t.Errorf("rendered configmap still carries the deprecated iscsi.extentAvailThreshold key")
	}
	if strings.Contains(out, "commandTimeout:") {
		t.Errorf("rendered configmap still carries the deprecated nvmeof.commandTimeout key")
	}
	if !strings.Contains(out, "    commandTimeouts:") {
		t.Errorf("rendered configmap dropped the live commandTimeouts block")
	}
}

// TestChartTrueNASMaxConnectionsPlumbing proves the truenas.maxConnections key
// renders end-to-end into the driver configmap: the pool-size default of 5 when
// unset, and the overridden value when set. A regression that drops the plumbing
// (reverting the pool to a hardcoded, unconfigurable size) fails here.
func TestChartTrueNASMaxConnectionsPlumbing(t *testing.T) {
	if out := helmTemplate(t); !strings.Contains(out, "      maxConnections: 5\n") {
		t.Errorf("default render does not carry truenas.maxConnections=5 in the configmap")
	}
	if out := helmTemplate(t, "--set", "truenas.maxConnections=9"); !strings.Contains(out, "      maxConnections: 9\n") {
		t.Errorf("--set truenas.maxConnections=9 did not propagate into the rendered configmap")
	}
}

// TestChartRateLimitingDeprecation proves the retired
// resilience.rateLimiting.maxConcurrentRequests key is no longer rendered into the
// driver configmap, while the still-wired maxConcurrentLogins key keeps rendering.
// The driver ignores maxConcurrentRequests (the API concurrency limit is
// truenas.maxConcurrentRequests), so rendering it would suggest a wiring that does
// not exist.
func TestChartRateLimitingDeprecation(t *testing.T) {
	out := helmTemplate(t)
	if strings.Contains(out, "      rateLimiting:\n        maxConcurrentRequests:") {
		t.Errorf("rendered configmap still carries the deprecated resilience.rateLimiting.maxConcurrentRequests key")
	}
	if !strings.Contains(out, "      rateLimiting:\n        maxConcurrentLogins: 2\n") {
		t.Errorf("rendered configmap dropped the still-wired resilience.rateLimiting.maxConcurrentLogins key")
	}
}

// TestChartSidecarTimeouts pins the CSI sidecar --timeout flags: the attacher and
// resizer run with a 120s deadline (bounding publish/unpublish/expand), while the
// provisioner and snapshotter keep 300s. A regression that reverts the
// attacher/resizer timeout fails here.
func TestChartSidecarTimeouts(t *testing.T) {
	out := helmTemplate(t)
	if got := strings.Count(out, `"--timeout=120s"`); got != 2 {
		t.Errorf("expected exactly 2 sidecars (attacher, resizer) with --timeout=120s, got %d", got)
	}
	if got := strings.Count(out, `"--timeout=300s"`); got != 2 {
		t.Errorf("expected exactly 2 sidecars (provisioner, snapshotter) with --timeout=300s, got %d", got)
	}
}
