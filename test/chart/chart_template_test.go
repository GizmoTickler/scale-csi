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

	"gopkg.in/yaml.v3"
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

// v130TrueNASConfig is a replica of the TrueNASConfig struct shipped in v1.3.0,
// BEFORE sprint1 added maxConnections. It is the strict-parse rollback contract
// for the default rendered truenas: block: a v1.3.0 binary decodes config with
// KnownFields(true) and has no maxConnections field, so any key the DEFAULT
// render emits that is absent here crash-loops an image-only rollback or mixed
// rollout at config parse. If you add a new truenas key to the default render,
// this replica makes the regression test fail until you consciously add the field
// here WITH a rollback note explaining why an old binary can still parse the new
// ConfigMap (usually it cannot — so render the key only on explicit override).
type v130TrueNASConfig struct {
	Host                  string `yaml:"host"`
	Port                  int    `yaml:"port"`
	Protocol              string `yaml:"protocol"`
	APIKey                string `yaml:"apiKey"`
	AllowInsecure         bool   `yaml:"allowInsecure"`
	CACert                string `yaml:"caCert"`
	CACertFile            string `yaml:"caCertFile"`
	RequestTimeout        int    `yaml:"requestTimeout"`
	ConnectTimeout        int    `yaml:"connectTimeout"`
	WriteTimeout          int    `yaml:"writeTimeout"`
	MaxConcurrentRequests int    `yaml:"maxConcurrentRequests"`
}

// renderedTrueNASBlock extracts the truenas: mapping from the rendered driver
// ConfigMap so it can be strict-parsed against v130TrueNASConfig.
func renderedTrueNASBlock(t *testing.T, rendered string) []byte {
	t.Helper()
	var manifest struct {
		Kind string            `yaml:"kind"`
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal([]byte(rendered), &manifest); err != nil {
		t.Fatalf("decode rendered ConfigMap manifest: %v", err)
	}
	if manifest.Kind != "ConfigMap" {
		t.Fatalf("expected a ConfigMap manifest, got kind %q", manifest.Kind)
	}
	configYAML, ok := manifest.Data["config.yaml"]
	if !ok {
		t.Fatalf("rendered ConfigMap has no config.yaml data key")
	}
	// Isolate the truenas: subtree (non-strict, so the other top-level config
	// keys are ignored) and re-encode it for a strict decode against the replica.
	var cfg struct {
		TrueNAS yaml.Node `yaml:"truenas"`
	}
	if err := yaml.Unmarshal([]byte(configYAML), &cfg); err != nil {
		t.Fatalf("decode rendered config.yaml: %v", err)
	}
	if cfg.TrueNAS.Kind != yaml.MappingNode {
		t.Fatalf("rendered config.yaml has no truenas: mapping")
	}
	out, err := yaml.Marshal(&cfg.TrueNAS)
	if err != nil {
		t.Fatalf("re-encode truenas block: %v", err)
	}
	return out
}

// TestChartTrueNASMaxConnectionsPlumbing guards the truenas.maxConnections
// removal-only render invariant. The driver-side default of 5 is the source of
// truth; the chart renders the key ONLY on explicit override so the default
// ConfigMap stays parseable by the v1.3.0 strict loader (which has no
// maxConnections field). A regression that re-adds the key to the default render
// (breaking image-only rollback / mixed rollouts) fails here.
func TestChartTrueNASMaxConnectionsPlumbing(t *testing.T) {
	t.Run("default render omits maxConnections", func(t *testing.T) {
		if out := helmTemplate(t); strings.Contains(out, "maxConnections:") {
			t.Errorf("default render must not emit truenas.maxConnections; a v1.3.0 strict parser has no such field and would crash-loop")
		}
	})

	t.Run("override renders the key", func(t *testing.T) {
		if out := helmTemplate(t, "--set", "truenas.maxConnections=8"); !strings.Contains(out, "      maxConnections: 8\n") {
			t.Errorf("--set truenas.maxConnections=8 did not propagate into the rendered configmap")
		}
	})

	t.Run("default truenas block parses on the v1.3.0 strict loader", func(t *testing.T) {
		block := renderedTrueNASBlock(t, helmTemplate(t, "--show-only", "templates/configmap.yaml"))
		dec := yaml.NewDecoder(strings.NewReader(string(block)))
		dec.KnownFields(true)
		var replica v130TrueNASConfig
		if err := dec.Decode(&replica); err != nil {
			t.Errorf("default rendered truenas: block is not parseable by the v1.3.0 strict loader; a new key was added without a rollback note: %v", err)
		}
	})
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
