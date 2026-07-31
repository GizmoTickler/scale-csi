// Package chart holds helm-template assertion tests that guard the chart-plumbing
// DoD: every driver config key must be plumbed through values.yaml,
// values.schema.json, and templates/configmap.yaml so it actually renders into the
// driver config. Batch 14 shipped a driver config key without chart plumbing and
// flux-local caught it; these tests catch the same class of regression in `go test`.
package chart

import (
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
		for _, absent := range []string{"--enable-capacity", "csistoragecapacities", "POD_NAME"} {
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
}

// TestChartHealthMonitorSidecar guards the external-health-monitor sidecar render
// invariant (E3/K11b). The sidecar and its extra RBAC are strictly opt-in: the
// default render carries no csi-external-health-monitor container and no health
// pods watch rule, keeping the default manifest byte-identical. Enabling
// sidecars.healthMonitor renders the pinned-image container, its monitor-interval,
// and the pods get/list/watch RBAC delta.
func TestChartHealthMonitorSidecar(t *testing.T) {
	t.Run("default render omits the sidecar and its RBAC", func(t *testing.T) {
		out := helmTemplate(t)
		if strings.Contains(out, "csi-external-health-monitor") {
			t.Errorf("default render must not emit the external-health-monitor sidecar; it is opt-in")
		}
		if strings.Contains(out, `verbs: ["get", "list", "watch"]`) && strings.Contains(out, "external-health-monitor") {
			t.Errorf("default render must not emit health-monitor RBAC")
		}
	})

	t.Run("enabled renders the sidecar and RBAC", func(t *testing.T) {
		out := helmTemplate(t, "--set", "sidecars.healthMonitor.enabled=true")
		for _, want := range []string{
			"- name: csi-external-health-monitor",
			"image: registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.18.0",
			`"--monitor-interval=60s"`,
			`resources: ["pods"]`,
		} {
			if !strings.Contains(out, want) {
				t.Errorf("--set sidecars.healthMonitor.enabled=true did not render %q", want)
			}
		}
	})
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
