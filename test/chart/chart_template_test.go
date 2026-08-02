// Package chart holds helm-template assertion tests that guard the chart-plumbing
// DoD: every driver config key must be plumbed through values.yaml,
// values.schema.json, and templates/configmap.yaml so it actually renders into the
// driver config. Batch 14 shipped a driver config key without chart plumbing and
// flux-local caught it; these tests catch the same class of regression in `go test`.
package chart

import (
	"errors"
	"io"
	"os"
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

// helmTemplateExpectError runs helm template expecting it to FAIL (e.g. a schema
// validation rejection) and returns the combined output so callers can assert on
// the failure reason. It fails the test if helm unexpectedly succeeds.
func helmTemplateExpectError(t *testing.T, extraArgs ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart template assertion")
	}
	args := append([]string{"template", "scale-csi", chartDir(t)}, extraArgs...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err == nil {
		t.Fatalf("helm template %v unexpectedly succeeded; expected a validation error\n%s", args, out)
	}
	return string(out)
}

// manifest is a loosely-typed decoded Kubernetes object from a helm render.
type manifest map[string]any

// decodeManifests splits a multi-document helm render into decoded manifests,
// skipping empty documents (the `---` separators helm emits). Parsing
// per-resource — rather than substring-matching the whole render — is what lets
// the negative RBAC invariants below actually catch an accidentally unconditional
// rule (codex L2).
func decodeManifests(t *testing.T, rendered string) []manifest {
	t.Helper()
	var out []manifest
	dec := yaml.NewDecoder(strings.NewReader(rendered))
	for {
		var m manifest
		err := dec.Decode(&m)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode rendered manifest: %v", err)
		}
		if len(m) == 0 {
			continue
		}
		out = append(out, m)
	}
	return out
}

// findManifest returns the first manifest of kind whose metadata.name contains
// nameSubstr, failing the test if none matches.
func findManifest(t *testing.T, manifests []manifest, kind, nameSubstr string) manifest {
	t.Helper()
	for _, m := range manifests {
		if m["kind"] != kind {
			continue
		}
		meta, _ := asManifest(m["metadata"])
		name, _ := meta["name"].(string)
		if strings.Contains(name, nameSubstr) {
			return m
		}
	}
	t.Fatalf("no %s manifest with name containing %q", kind, nameSubstr)
	return nil
}

// asManifest normalizes a decoded YAML mapping to manifest. yaml.v3 decodes
// nested mappings into the named manifest type (not a bare map[string]any), so a
// plain type assertion to map[string]any would fail; accept both forms.
func asManifest(v any) (manifest, bool) {
	switch m := v.(type) {
	case manifest:
		return m, true
	case map[string]any:
		return manifest(m), true
	default:
		return nil, false
	}
}

func asStringSlice(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// roleHasRule reports whether a ClusterRole/Role manifest carries a rule with
// exactly these resources and exactly these verbs (order-sensitive, matching the
// rendered template).
func roleHasRule(role manifest, resources, verbs []string) bool {
	rules, _ := role["rules"].([]any)
	for _, r := range rules {
		rule, ok := asManifest(r)
		if !ok {
			continue
		}
		if equalStrings(asStringSlice(rule["resources"]), resources) && equalStrings(asStringSlice(rule["verbs"]), verbs) {
			return true
		}
	}
	return false
}

// roleTouchesResource reports whether any rule in a ClusterRole/Role manifest
// lists resource.
func roleTouchesResource(role manifest, resource string) bool {
	rules, _ := role["rules"].([]any)
	for _, r := range rules {
		rule, ok := asManifest(r)
		if !ok {
			continue
		}
		for _, res := range asStringSlice(rule["resources"]) {
			if res == resource {
				return true
			}
		}
	}
	return false
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

// TestChartHoldCSISnapshotsPlumbing proves the GF2/E1 zfs.holdCsiSnapshots key is
// removal-only rendered: ABSENT from the default configmap (so the default render
// stays byte-identical to v1.4.1 and a rolled-back binary with no holdCsiSnapshots
// field still strict-parses it) and present only when explicitly enabled.
func TestChartHoldCSISnapshotsPlumbing(t *testing.T) {
	t.Run("default render omits holdCsiSnapshots", func(t *testing.T) {
		if out := helmTemplate(t, "--show-only", "templates/configmap.yaml"); strings.Contains(out, "holdCsiSnapshots:") {
			t.Errorf("default configmap must not emit zfs.holdCsiSnapshots; the feature is opt-in and the default render must stay byte-identical")
		}
	})

	t.Run("enabled renders the key", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "zfs.holdCsiSnapshots=true")
		if !strings.Contains(out, "      holdCsiSnapshots: true\n") {
			t.Errorf("--set zfs.holdCsiSnapshots=true did not propagate into the rendered configmap; got:\n%s", out)
		}
	})
}

// TestChartSnapshotSchedulePlumbing proves the GF2/E2 driver-managed periodic
// snapshot keys (zfs.snapshotSchedule, zfs.snapshotRetention) are removal-only
// rendered: ABSENT from the default configmap (empty default => byte-identical
// render) and present only when set to a non-empty value.
func TestChartSnapshotSchedulePlumbing(t *testing.T) {
	t.Run("default render omits the schedule keys", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "snapshotSchedule:") {
			t.Errorf("default configmap must not emit zfs.snapshotSchedule; empty default must stay byte-identical")
		}
		if strings.Contains(out, "snapshotRetention:") {
			t.Errorf("default configmap must not emit zfs.snapshotRetention; empty default must stay byte-identical")
		}
	})

	t.Run("set renders both keys", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml",
			"--set", "zfs.snapshotSchedule=0 0 * * *", "--set", "zfs.snapshotRetention=30d")
		if !strings.Contains(out, "      snapshotSchedule: \"0 0 * * *\"\n") {
			t.Errorf("--set zfs.snapshotSchedule did not render quoted into the configmap; got:\n%s", out)
		}
		if !strings.Contains(out, "      snapshotRetention: \"30d\"\n") {
			t.Errorf("--set zfs.snapshotRetention did not render quoted into the configmap; got:\n%s", out)
		}
	})
}

// TestChartPromoteRestoredClonesPlumbing proves the GF2/E3 zfs.promoteRestoredClones
// key is removal-only rendered: ABSENT from the default configmap (default false =>
// byte-identical render) and present only when enabled.
func TestChartPromoteRestoredClonesPlumbing(t *testing.T) {
	t.Run("default render omits promoteRestoredClones", func(t *testing.T) {
		if out := helmTemplate(t, "--show-only", "templates/configmap.yaml"); strings.Contains(out, "promoteRestoredClones:") {
			t.Errorf("default configmap must not emit zfs.promoteRestoredClones; the feature is opt-in")
		}
	})

	t.Run("enabled renders the key", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "zfs.promoteRestoredClones=true")
		if !strings.Contains(out, "      promoteRestoredClones: true\n") {
			t.Errorf("--set zfs.promoteRestoredClones=true did not propagate into the rendered configmap; got:\n%s", out)
		}
	})
}

// TestChartReportVolumeUsagePlumbing proves the GF2/E4 zfs.reportVolumeUsage key
// and its ScaleCSIVolumeNearQuota alert are removal-only rendered: ABSENT from the
// default configmap and prometheusrule (default false => byte-identical render)
// and present only when enabled.
func TestChartReportVolumeUsagePlumbing(t *testing.T) {
	t.Run("default render omits the key and the alert", func(t *testing.T) {
		if out := helmTemplate(t, "--show-only", "templates/configmap.yaml"); strings.Contains(out, "reportVolumeUsage:") {
			t.Errorf("default configmap must not emit zfs.reportVolumeUsage; the feature is opt-in")
		}
		if out := helmTemplate(t, "--set", "metrics.prometheusRule.enabled=true"); strings.Contains(out, "ScaleCSIVolumeNearQuota") {
			t.Errorf("ScaleCSIVolumeNearQuota must stay absent unless zfs.reportVolumeUsage is also true")
		}
	})

	t.Run("enabled renders the key and the alert", func(t *testing.T) {
		if out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "zfs.reportVolumeUsage=true"); !strings.Contains(out, "      reportVolumeUsage: true\n") {
			t.Errorf("--set zfs.reportVolumeUsage=true did not render into the configmap; got:\n%s", out)
		}
		out := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
			"--set", "metrics.prometheusRule.enabled=true", "--set", "zfs.reportVolumeUsage=true")
		if !strings.Contains(out, "- alert: ScaleCSIVolumeNearQuota") {
			t.Errorf("both toggles on did not render ScaleCSIVolumeNearQuota; got:\n%s", out)
		}
	})
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

// TestChartCSIStorageCapacityPlumbing guards the CSIStorageCapacity render
// invariant (E1). Capacity tracking is strictly opt-in: the default render must
// keep storageCapacity=false and emit NO provisioner --enable-capacity flag, NO
// csistoragecapacities RBAC, and NO provisioner NAMESPACE/POD_NAME env, so the
// default manifest stays byte-identical and creates no cluster objects. Enabling
// capacity.enabled flips the CSIDriver field and wires the provisioner + RBAC.
func TestChartCSIStorageCapacityPlumbing(t *testing.T) {
	t.Run("default render keeps capacity off", func(t *testing.T) {
		out := helmTemplate(t)
		if !strings.Contains(out, "storageCapacity: false") {
			t.Errorf("default render must keep CSIDriver storageCapacity: false")
		}
		// NAMESPACE is asserted alongside POD_NAME (codex L2): both Downward API
		// env vars are capacity-gated, so neither may appear in the default render.
		for _, absent := range []string{"--enable-capacity", "csistoragecapacities", "POD_NAME", "NAMESPACE"} {
			if strings.Contains(out, absent) {
				t.Errorf("default render must not emit %q; capacity tracking is opt-in", absent)
			}
		}
	})

	t.Run("enabled wires CSIDriver, provisioner, and RBAC", func(t *testing.T) {
		out := helmTemplate(t, "--set", "capacity.enabled=true")
		for _, want := range []string{
			"storageCapacity: true",
			`"--enable-capacity"`,
			`"--capacity-ownerref-level=2"`,
			`resources: ["csistoragecapacities"]`,
			`resources: ["replicasets"]`,
			"name: POD_NAME",
			"name: NAMESPACE",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("--set capacity.enabled=true did not render %q", want)
			}
		}
		// The immediate-binding flag is opt-in and must stay absent by default.
		if strings.Contains(out, "--capacity-for-immediate-binding") {
			t.Errorf("capacity.enabled=true must not render --capacity-for-immediate-binding unless forImmediateBinding is set")
		}
	})

	// codex M2: external-provisioner ignores Immediate-binding StorageClasses by
	// default (the scheduler ignores capacity for immediate binding), so the flag
	// is a deliberate opt-in for non-scheduler capacity consumers.
	t.Run("forImmediateBinding renders the opt-in flag both states", func(t *testing.T) {
		on := helmTemplate(t, "--set", "capacity.enabled=true", "--set", "capacity.forImmediateBinding=true")
		if !strings.Contains(on, `"--capacity-for-immediate-binding"`) {
			t.Errorf("capacity.forImmediateBinding=true did not render --capacity-for-immediate-binding")
		}

		off := helmTemplate(t, "--set", "capacity.enabled=true", "--set", "capacity.forImmediateBinding=false")
		if strings.Contains(off, "--capacity-for-immediate-binding") {
			t.Errorf("capacity.forImmediateBinding=false must not render --capacity-for-immediate-binding")
		}

		// The flag is nested under capacity.enabled: setting it alone renders nothing.
		disabled := helmTemplate(t, "--set", "capacity.forImmediateBinding=true")
		if strings.Contains(disabled, "--capacity-for-immediate-binding") {
			t.Errorf("forImmediateBinding must have no effect while capacity.enabled is false")
		}
	})
}

// TestChartCapacityConfigPlumbing guards the driver-config capacity: block render
// invariant (E2/K8). The block must be ABSENT from the default ConfigMap so a
// rolled-back binary whose Config struct has no capacity field still strict-parses
// it (F1 removal-only invariant); it renders only when a capacity value is
// non-default. If you add a capacity key to the DEFAULT render, a v1.3.0-style
// strict loader crash-loops — keep the block opt-in.
func TestChartCapacityConfigPlumbing(t *testing.T) {
	t.Run("default configmap omits the capacity block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "    capacity:") {
			t.Errorf("default configmap must not emit a capacity: block; a rolled-back strict parser has no such field")
		}
	})

	t.Run("reportMaximumVolumeSize renders the block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "capacity.reportMaximumVolumeSize=true")
		const block = "    capacity:\n      reportMaximumVolumeSize: true\n"
		if !strings.Contains(out, block) {
			t.Errorf("--set capacity.reportMaximumVolumeSize=true did not render the capacity block; got:\n%s", out)
		}
	})

	t.Run("gaugeEnabled renders the gauge block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "capacity.gaugeEnabled=true")
		const block = "    capacity:\n      gaugeEnabled: true\n      gaugeInterval: \"60s\"\n"
		if !strings.Contains(out, block) {
			t.Errorf("--set capacity.gaugeEnabled=true did not render the gauge block with default interval; got:\n%s", out)
		}
	})
}

// TestChartPoolCapacityAlert guards the ScaleCSIPoolNearFull PrometheusRule alert
// (E4/K15). The alert depends on the capacity gauges, so it renders ONLY when both
// metrics.prometheusRule.enabled and capacity.gaugeEnabled are true; with either
// off (the default) it is absent, keeping the default render byte-identical.
func TestChartPoolCapacityAlert(t *testing.T) {
	t.Run("absent by default and with only prometheusRule", func(t *testing.T) {
		if out := helmTemplate(t); strings.Contains(out, "ScaleCSIPoolNearFull") {
			t.Errorf("default render must not emit ScaleCSIPoolNearFull")
		}
		out := helmTemplate(t, "--set", "metrics.prometheusRule.enabled=true")
		if strings.Contains(out, "ScaleCSIPoolNearFull") {
			t.Errorf("ScaleCSIPoolNearFull must stay absent unless capacity.gaugeEnabled is also true")
		}
	})

	t.Run("present with both toggles and honors the threshold", func(t *testing.T) {
		// A values file (not --set) supplies the numeric threshold: helm --set
		// delivers decimals as strings, which the schema's type:number rejects.
		valuesPath := filepath.Join(t.TempDir(), "pool-alert-values.yaml")
		const values = `metrics:
  prometheusRule:
    enabled: true
    poolUsageThreshold: 0.9
capacity:
  gaugeEnabled: true
`
		if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
			t.Fatalf("write override values: %v", err)
		}
		out := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml", "-f", valuesPath)
		if !strings.Contains(out, "- alert: ScaleCSIPoolNearFull") {
			t.Errorf("both toggles on did not render ScaleCSIPoolNearFull; got:\n%s", out)
		}
		if !strings.Contains(out, ") > 0.9") {
			t.Errorf("poolUsageThreshold=0.9 did not propagate into the alert expr; got:\n%s", out)
		}
	})

	// codex M4: the threshold renders directly (no Sprig `default`), so a
	// legitimate numeric 0 must propagate as `> 0`, not be silently rewritten to
	// the 0.85 default. Cover the lower bound, the default, a fractional value,
	// and the upper bound.
	t.Run("threshold boundary values propagate exactly", func(t *testing.T) {
		render := func(t *testing.T, thresholdLine string) string {
			t.Helper()
			valuesPath := filepath.Join(t.TempDir(), "threshold-values.yaml")
			values := "metrics:\n  prometheusRule:\n    enabled: true\n" + thresholdLine + "capacity:\n  gaugeEnabled: true\n"
			if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
				t.Fatalf("write override values: %v", err)
			}
			return helmTemplate(t, "--show-only", "templates/prometheusrule.yaml", "-f", valuesPath)
		}

		// wantExpr is newline-anchored: a bare Contains(") > 0") would also match
		// ") > 0.85", making the zero-lower-bound case vacuous (it would pass even
		// if a Sprig `default` swallowed the 0 back to 0.85).
		cases := []struct {
			name      string
			threshold string // empty => use the values.yaml default
			wantExpr  string
		}{
			{"zero lower bound", "    poolUsageThreshold: 0\n", ") > 0\n"},
			{"default", "", ") > 0.85\n"},
			{"fractional", "    poolUsageThreshold: 0.5\n", ") > 0.5\n"},
			{"one upper bound", "    poolUsageThreshold: 1\n", ") > 1\n"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				out := render(t, tc.threshold)
				if !strings.Contains(out, tc.wantExpr) {
					t.Errorf("threshold case %q did not render %q; got:\n%s", tc.name, tc.wantExpr, out)
				}
			})
		}
	})
}

// TestChartReconcileStalledThresholdDerivation guards the codex M3 fix: the
// ScaleCSIReconcileStalled age threshold is DERIVED from reconcile.interval
// (3x the cadence, rendered to seconds) rather than a hard-coded three hours, so
// an operator who lengthens the interval lengthens the threshold in lockstep and
// no longer gets a false critical before the next scheduled pass.
func TestChartReconcileStalledThresholdDerivation(t *testing.T) {
	for _, tc := range []struct {
		interval    string // empty => values.yaml default (1h)
		wantSeconds string
	}{
		{"", "10800"},      // default 1h -> 3 * 3600
		{"2h", "21600"},    // 3 * 7200
		{"2h30m", "27000"}, // 3 * (2*3600 + 30*60)
		{"45m", "8100"},    // 3 * 2700
		{"90s", "270"},     // 3 * 90
	} {
		name := tc.interval
		if name == "" {
			name = "default-1h"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{"--show-only", "templates/prometheusrule.yaml", "--set", "metrics.prometheusRule.enabled=true"}
			if tc.interval != "" {
				args = append(args, "--set", "reconcile.interval="+tc.interval)
			}
			out := helmTemplate(t, args...)

			// Find the reconcile-stalled expr line and assert its threshold.
			var exprLine string
			for _, line := range strings.Split(out, "\n") {
				if strings.Contains(line, "scale_csi_reconcile_last_success_timestamp_seconds") {
					exprLine = strings.TrimSpace(line)
					break
				}
			}
			if exprLine == "" {
				t.Fatalf("rendered prometheusrule has no reconcile-stalled expr; got:\n%s", out)
			}
			// HasSuffix anchors the exact threshold so "> 270" cannot silently
			// match a "> 2700" render.
			if want := "> " + tc.wantSeconds; !strings.HasSuffix(exprLine, want) {
				t.Errorf("interval %q: expected expr to end with %q, got: %s", tc.interval, want, exprLine)
			}
		})
	}
}

// TestChartHealthMonitorSidecar guards the external-health-monitor sidecar render
// invariant (E3/K11b). The sidecar and its extra RBAC are strictly opt-in: the
// default render carries no csi-external-health-monitor container and no health
// pods watch rule, keeping the default manifest byte-identical. Enabling
// sidecars.healthMonitor renders the pinned-image container, the ACTIVE
// --list-volumes-interval cadence (this driver advertises LIST_VOLUMES; codex M1)
// plus the --monitor-interval fallback, and the pods get/list/watch + events get
// RBAC delta. The RBAC assertions parse the controller ClusterRole per-resource
// (codex L2) so an accidentally unconditional pods rule cannot slip past a
// whole-render substring match.
func TestChartHealthMonitorSidecar(t *testing.T) {
	t.Run("default render omits the sidecar and its RBAC", func(t *testing.T) {
		out := helmTemplate(t)
		if strings.Contains(out, "csi-external-health-monitor") {
			t.Errorf("default render must not emit the external-health-monitor sidecar; it is opt-in")
		}
		// Parse the controller ClusterRole and assert it carries NO pods rule at
		// all (capacity and health-monitor are both off). A substring match on the
		// whole render could not distinguish an unconditional pods rule from the
		// gated one; per-resource parsing can.
		role := findManifest(t, decodeManifests(t, out), "ClusterRole", "scale-csi-controller")
		if roleTouchesResource(role, "pods") {
			t.Errorf("default controller ClusterRole must not grant any pods rule; health-monitor RBAC is opt-in")
		}
	})

	t.Run("enabled renders the sidecar and RBAC", func(t *testing.T) {
		out := helmTemplate(t, "--set", "sidecars.healthMonitor.enabled=true")
		for _, want := range []string{
			"- name: csi-external-health-monitor",
			"image: registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.18.0",
			// codex M1: LIST_VOLUMES is advertised, so --list-volumes-interval is
			// the active cadence; --monitor-interval is retained as the fallback.
			`"--list-volumes-interval=60s"`,
			`"--monitor-interval=60s"`,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("--set sidecars.healthMonitor.enabled=true did not render %q", want)
			}
		}

		// Per-resource RBAC assertions on the controller ClusterRole.
		role := findManifest(t, decodeManifests(t, out), "ClusterRole", "scale-csi-controller")
		if !roleHasRule(role, []string{"pods"}, []string{"get", "list", "watch"}) {
			t.Errorf("health-monitor RBAC must grant pods get/list/watch")
		}
		if !roleHasRule(role, []string{"events"}, []string{"get"}) {
			t.Errorf("health-monitor RBAC must grant events get (codex L1 upstream parity)")
		}
		// Leader election runs in the release namespace via a Lease; the sidecar
		// relies on the existing leases rule, so confirm it covers create/update.
		if !roleHasRule(role, []string{"leases"}, []string{"get", "watch", "list", "create", "update", "delete"}) {
			t.Errorf("controller ClusterRole must keep the leases rule that backs health-monitor leader election")
		}
	})
}

// TestChartDurationValidation guards the two opt-in duration strings (codex M3):
// sidecars.healthMonitor.interval and capacity.gaugeInterval. Both must be
// positive Go durations. The schema rejects malformed strings — which previously
// passed validation and then crash-looped the health-monitor's Go duration flag
// parser (or silently disabled the opted-in gauges) — and zero durations, which
// would make the interval meaningless. Each case asserts helm fails validation
// and names the offending field by its schema JSON-pointer.
func TestChartDurationValidation(t *testing.T) {
	cases := []struct {
		name    string
		setKey  string
		bad     string
		pointer string
		extra   []string
	}{
		{
			name:    "healthMonitor.interval malformed",
			setKey:  "sidecars.healthMonitor.interval",
			bad:     "bogus",
			pointer: "/sidecars/healthMonitor/interval",
			extra:   []string{"--set", "sidecars.healthMonitor.enabled=true"},
		},
		{
			name:    "healthMonitor.interval zero",
			setKey:  "sidecars.healthMonitor.interval",
			bad:     "0s",
			pointer: "/sidecars/healthMonitor/interval",
			extra:   []string{"--set", "sidecars.healthMonitor.enabled=true"},
		},
		{
			name:    "capacity.gaugeInterval malformed",
			setKey:  "capacity.gaugeInterval",
			bad:     "bogus",
			pointer: "/capacity/gaugeInterval",
		},
		{
			name:    "capacity.gaugeInterval zero",
			setKey:  "capacity.gaugeInterval",
			bad:     "0s",
			pointer: "/capacity/gaugeInterval",
		},
		// reconcile.interval feeds the ScaleCSIReconcileStalled threshold helper
		// (codex M3), which parses whole h/m/s segments; the schema rejects
		// anything it cannot parse (malformed, zero, and fractional durations).
		{
			name:    "reconcile.interval malformed",
			setKey:  "reconcile.interval",
			bad:     "bogus",
			pointer: "/reconcile/interval",
		},
		{
			name:    "reconcile.interval zero",
			setKey:  "reconcile.interval",
			bad:     "0s",
			pointer: "/reconcile/interval",
		},
		{
			name:    "reconcile.interval fractional",
			setKey:  "reconcile.interval",
			bad:     "1.5h",
			pointer: "/reconcile/interval",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(tc.extra, "--set", tc.setKey+"="+tc.bad)
			out := helmTemplateExpectError(t, args...)
			if !strings.Contains(out, tc.pointer) {
				t.Errorf("expected a schema validation error at %q; got:\n%s", tc.pointer, out)
			}
		})
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

// TestChartISCSIChAPPlumbing guards the iSCSI CHAP render invariant. CHAP is
// strictly opt-in: the default render must emit NO chap: block (zero behavior
// change for existing installs), and the block renders only when
// iscsi.chap.enabled=true. Every new chap key (enabled/tag/mutual) is asserted
// so a key shipped without chart plumbing fails here.
func TestChartISCSIChAPPlumbing(t *testing.T) {
	t.Run("default render omits the chap block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "      chap:") {
			t.Errorf("default render must not emit iscsi.chap; CHAP is opt-in and default-off")
		}
	})

	t.Run("enabled renders the chap block with defaults", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "iscsi.chap.enabled=true")
		const block = "      chap:\n        enabled: true\n        tag: 0\n        mutual: false\n"
		if !strings.Contains(out, block) {
			t.Errorf("--set iscsi.chap.enabled=true did not render the default chap block; got:\n%s", out)
		}
	})

	t.Run("tag and mutual propagate", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml",
			"--set", "iscsi.chap.enabled=true",
			"--set", "iscsi.chap.tag=1234",
			"--set", "iscsi.chap.mutual=true")
		const block = "      chap:\n        enabled: true\n        tag: 1234\n        mutual: true\n"
		if !strings.Contains(out, block) {
			t.Errorf("iscsi.chap.tag/mutual did not propagate into the rendered configmap; got:\n%s", out)
		}
	})
}

// TestChartISCSIChAPStorageClassSecretRefs guards the per-StorageClass CHAP
// secret-ref plumbing. The four CSI secret-ref parameters render ONLY when a
// class sets chapSecretName (omit-when-unset), reference the Secret by
// name/namespace only (never credential values), and default the namespace to
// the release namespace.
func TestChartISCSIChAPStorageClassSecretRefs(t *testing.T) {
	t.Run("default render emits no secret refs", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml")
		if strings.Contains(out, "csi.storage.k8s.io/provisioner-secret-name") {
			t.Errorf("default render must not emit CHAP secret-ref parameters")
		}
	})

	t.Run("chapSecretName renders the four secret refs", func(t *testing.T) {
		valuesPath := filepath.Join(t.TempDir(), "chap-values.yaml")
		const values = `storageClasses:
  - name: scale-iscsi-chap
    enabled: true
    protocol: iscsi
    chapSecretName: scale-iscsi-chap
    chapSecretNamespace: ""
`
		if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
			t.Fatalf("write override values: %v", err)
		}
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		for _, want := range []string{
			"csi.storage.k8s.io/provisioner-secret-name: scale-iscsi-chap",
			"csi.storage.k8s.io/provisioner-secret-namespace: default",
			"csi.storage.k8s.io/node-stage-secret-name: scale-iscsi-chap",
			"csi.storage.k8s.io/node-stage-secret-namespace: default",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("rendered CHAP StorageClass missing %q; got:\n%s", want, out)
			}
		}
	})

	t.Run("explicit chapSecretNamespace is honored", func(t *testing.T) {
		valuesPath := filepath.Join(t.TempDir(), "chap-values-ns.yaml")
		const values = `storageClasses:
  - name: scale-iscsi-chap
    enabled: true
    protocol: iscsi
    chapSecretName: scale-iscsi-chap
    chapSecretNamespace: kube-system
`
		if err := os.WriteFile(valuesPath, []byte(values), 0o600); err != nil {
			t.Fatalf("write override values: %v", err)
		}
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		if !strings.Contains(out, "csi.storage.k8s.io/provisioner-secret-namespace: kube-system") {
			t.Errorf("explicit chapSecretNamespace was not honored; got:\n%s", out)
		}
	})
}
