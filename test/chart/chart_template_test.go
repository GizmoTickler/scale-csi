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
