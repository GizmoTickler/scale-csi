package chart

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeValues writes a temporary values override file and returns its path.
func writeValues(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write override values: %v", err)
	}
	return path
}

// TestChartGF5NFSKeysDefaultOff is the byte-identity guard for GF-Sprint 5's NFS
// epic: NONE of the new nfs.* configmap keys may appear in a default render, so
// an existing install upgrading to this chart gets the exact same ConfigMap it
// had before (and a rolled-back binary can still parse it).
func TestChartGF5NFSKeysDefaultOff(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
	for _, key := range []string{
		"shareSecurity",
		"shareExposeSnapshots",
		"krbEnabled",
		"versionPreflight",
		"ensureProtocols",
	} {
		if strings.Contains(out, key) {
			t.Errorf("default configmap render must not contain the GF5 NFS key %q; got:\n%s", key, out)
		}
	}
}

// TestChartGF5NFSSecurityPlumbing proves nfs.shareSecurity is plumbed
// values.yaml -> values.schema.json -> configmap, and that an invalid enum is
// rejected by the schema rather than reaching the driver.
func TestChartGF5NFSSecurityPlumbing(t *testing.T) {
	t.Run("renders when set", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "nfs.shareSecurity={SYS}")
		if !strings.Contains(out, "      shareSecurity:\n        - SYS\n") {
			t.Errorf("nfs.shareSecurity did not render into the configmap; got:\n%s", out)
		}
	})

	t.Run("schema rejects an unknown mode", func(t *testing.T) {
		out := helmTemplateExpectError(t, "--set", "nfs.shareSecurity={KRB9}")
		if !strings.Contains(out, "shareSecurity") {
			t.Errorf("schema rejection did not mention shareSecurity; got:\n%s", out)
		}
	})
}

// TestChartGF5NFSBooleanPlumbing covers the remaining scalar NFS keys.
func TestChartGF5NFSBooleanPlumbing(t *testing.T) {
	cases := []struct {
		set  string
		want string
	}{
		{"nfs.shareExposeSnapshots=true", "      shareExposeSnapshots: true\n"},
		{"nfs.krbEnabled=true", "      krbEnabled: true\n"},
		{"nfs.versionPreflight=true", "      versionPreflight: true\n"},
	}
	for _, tc := range cases {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", tc.set)
		if !strings.Contains(out, tc.want) {
			t.Errorf("--set %s did not render %q; got:\n%s", tc.set, tc.want, out)
		}
	}
}

// TestChartGF5NFSEnsureProtocolsPlumbing proves the hard-gated global-service
// key renders only when set, and carries its HARD RULE banner.
func TestChartGF5NFSEnsureProtocolsPlumbing(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "nfs.ensureProtocols={NFSV3,NFSV4}")
	if !strings.Contains(out, "      ensureProtocols:\n        - NFSV3\n        - NFSV4\n") {
		t.Errorf("nfs.ensureProtocols did not render; got:\n%s", out)
	}
	if !strings.Contains(out, "HARD RULE") {
		t.Errorf("nfs.ensureProtocols render is missing its HARD RULE banner; got:\n%s", out)
	}
	if out := helmTemplateExpectError(t, "--set", "nfs.ensureProtocols={NFSV2}"); !strings.Contains(out, "ensureProtocols") {
		t.Errorf("schema did not reject an invalid ensureProtocols entry; got:\n%s", out)
	}
}

// TestChartGF5NFSStorageClassParameters proves the per-class NFS export
// overrides are emitted ONLY when the class sets them, so an untouched class
// renders the identical parameter map it did before GF5.
func TestChartGF5NFSStorageClassParameters(t *testing.T) {
	t.Run("default class emits no NFS override parameters", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml")
		for _, key := range []string{"nfsSecurity", "nfsExposeSnapshots", "nfsReadOnly"} {
			if strings.Contains(out, key) {
				t.Errorf("default StorageClass render must not emit %q; got:\n%s", key, out)
			}
		}
	})

	t.Run("overrides render as CSI parameters", func(t *testing.T) {
		valuesPath := writeValues(t, "gf5-nfs-sc.yaml", `storageClasses:
  - name: scale-nfs-secure
    protocol: nfs
    nfsSecurity:
      - SYS
    nfsExposeSnapshots: true
    nfsReadOnly: false
    mountOptions:
      - nfsvers=4.1
      - nconnect=8
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		for _, want := range []string{
			"nfsSecurity: SYS",
			"nfsExposeSnapshots: \"true\"",
			"nfsReadOnly: \"false\"",
			"- nfsvers=4.1",
			"- nconnect=8",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("rendered StorageClass missing %q; got:\n%s", want, out)
			}
		}
	})
}

// TestChartGF5FsGroupPolicy proves the CSIDriver fsGroupPolicy is now a chart
// value whose DEFAULT render is byte-identical (`File`), and that an operator
// committing to ACL-managed volumes can select `None`. Flipping the shipped
// default would change fsGroup semantics for every existing volume, so the
// default is asserted explicitly.
func TestChartGF5FsGroupPolicy(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/csidriver.yaml")
	if !strings.Contains(out, "  fsGroupPolicy: File\n") {
		t.Errorf("default CSIDriver render must keep fsGroupPolicy: File; got:\n%s", out)
	}

	out = helmTemplate(t, "--show-only", "templates/csidriver.yaml", "--set", "csidriver.fsGroupPolicy=None")
	if !strings.Contains(out, "  fsGroupPolicy: None\n") {
		t.Errorf("csidriver.fsGroupPolicy=None did not render; got:\n%s", out)
	}

	if out := helmTemplateExpectError(t, "--set", "csidriver.fsGroupPolicy=Nonsense"); !strings.Contains(out, "fsGroupPolicy") {
		t.Errorf("schema did not reject an invalid fsGroupPolicy; got:\n%s", out)
	}
}

// TestChartGF5NFSACLStorageClassParameters proves the ACL parameters are emitted
// only when a class sets them, and that a template and an inline ACL both round
// trip into CSI parameters.
func TestChartGF5NFSACLStorageClassParameters(t *testing.T) {
	t.Run("default class emits no ACL parameters", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml")
		for _, key := range []string{"nfsACLTemplate", "nfsACL:"} {
			if strings.Contains(out, key) {
				t.Errorf("default StorageClass render must not emit %q; got:\n%s", key, out)
			}
		}
	})

	t.Run("template renders", func(t *testing.T) {
		valuesPath := writeValues(t, "gf5-acl-template.yaml", `storageClasses:
  - name: scale-nfs-acl
    protocol: nfs
    nfsACLTemplate: NFS4_RESTRICTED
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		if !strings.Contains(out, "nfsACLTemplate: NFS4_RESTRICTED") {
			t.Errorf("nfsACLTemplate did not render; got:\n%s", out)
		}
	})

	t.Run("inline acl renders as JSON", func(t *testing.T) {
		valuesPath := writeValues(t, "gf5-acl-inline.yaml", `storageClasses:
  - name: scale-nfs-acl
    protocol: nfs
    nfsACL:
      - tag: owner@
        type: ALLOW
        perms:
          BASIC: FULL_CONTROL
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		if !strings.Contains(out, `"tag":"owner@"`) {
			t.Errorf("nfsACL did not render as JSON; got:\n%s", out)
		}
	})

	t.Run("schema rejects an unknown template", func(t *testing.T) {
		valuesPath := writeValues(t, "gf5-acl-bad.yaml", `storageClasses:
  - name: scale-nfs-acl
    protocol: nfs
    nfsACLTemplate: NFS4_WIDE_OPEN
`)
		if out := helmTemplateExpectError(t, "-f", valuesPath); !strings.Contains(out, "nfsACLTemplate") {
			t.Errorf("schema did not reject an unknown ACL template; got:\n%s", out)
		}
	})
}

// TestChartGF5ZFSPerformanceClass proves the curated-class parameter is emitted
// only when a class sets it, that the schema pins the five documented classes,
// and that the bundled opt-in example stays disabled by default.
func TestChartGF5ZFSPerformanceClass(t *testing.T) {
	t.Run("default render carries no performance class", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml")
		if strings.Contains(out, "zfsPerformanceClass") {
			t.Errorf("default StorageClass render must not emit zfsPerformanceClass; got:\n%s", out)
		}
		if strings.Contains(out, "scale-nfs-media") {
			t.Errorf("the opt-in curated-class example must stay disabled by default; got:\n%s", out)
		}
	})

	t.Run("the bundled example renders when enabled", func(t *testing.T) {
		// Mirrors the values.yaml `scale-nfs-media` entry verbatim; enabling an
		// array element in place requires re-supplying the entry.
		valuesPath := writeValues(t, "gf5-perf-example.yaml", `storageClasses:
  - name: scale-nfs-media
    enabled: true
    protocol: nfs
    zfsPerformanceClass: media
    mountOptions:
      - nfsvers=4.1
      - nconnect=8
      - hard
      - noatime
      - rsize=1048576
      - wsize=1048576
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		if !strings.Contains(out, "zfsPerformanceClass: media") {
			t.Errorf("enabling the curated-class example did not emit the parameter; got:\n%s", out)
		}
		for _, want := range []string{"- nfsvers=4.1", "- nconnect=8", "- rsize=1048576"} {
			if !strings.Contains(out, want) {
				t.Errorf("curated-class example is missing the matching mount profile option %q; got:\n%s", want, out)
			}
		}
	})

	t.Run("every documented class is accepted", func(t *testing.T) {
		for _, class := range []string{"database", "media", "vm", "backup", "general"} {
			valuesPath := writeValues(t, "gf5-perf-"+class+".yaml", `storageClasses:
  - name: scale-perf
    protocol: nfs
    zfsPerformanceClass: `+class+`
`)
			out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
			if !strings.Contains(out, "zfsPerformanceClass: "+class) {
				t.Errorf("class %q did not render; got:\n%s", class, out)
			}
		}
	})

	t.Run("schema rejects an unknown class", func(t *testing.T) {
		valuesPath := writeValues(t, "gf5-perf-bad.yaml", `storageClasses:
  - name: scale-perf
    protocol: nfs
    zfsPerformanceClass: ludicrous
`)
		if out := helmTemplateExpectError(t, "-f", valuesPath); !strings.Contains(out, "zfsPerformanceClass") {
			t.Errorf("schema did not reject an unknown performance class; got:\n%s", out)
		}
	})
}

// TestChartGF5BackendHealthPlumbing proves the backend-health block renders only
// when enabled (byte-identical default), that the gated alerts appear with it,
// and that the interval is plumbed.
func TestChartGF5BackendHealthPlumbing(t *testing.T) {
	t.Run("default configmap carries no backendHealth block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "backendHealth") {
			t.Errorf("default configmap render must not contain backendHealth; got:\n%s", out)
		}
	})

	t.Run("enabled renders the block with its interval", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "backendHealth.enabled=true")
		if !strings.Contains(out, "    backendHealth:\n      enabled: true\n      interval: \"60s\"\n") {
			t.Errorf("backendHealth block did not render; got:\n%s", out)
		}
		out = helmTemplate(t, "--show-only", "templates/configmap.yaml",
			"--set", "backendHealth.enabled=true", "--set", "backendHealth.interval=5m")
		if !strings.Contains(out, "interval: \"5m\"") {
			t.Errorf("backendHealth.interval did not propagate; got:\n%s", out)
		}
	})

	t.Run("health alerts are gated on the poller", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
			"--set", "metrics.prometheusRule.enabled=true")
		gated := []string{
			"ScaleCSIPoolDegraded", "ScaleCSIPoolScanErrors", "ScaleCSIPoolDiskTemperatureAlert",
			"ScaleCSIPoolHealthStale", "ScaleCSIPoolConditionFlipPending",
		}
		for _, alert := range gated {
			if strings.Contains(out, alert) {
				t.Errorf("%s must not render without backendHealth.enabled; got:\n%s", alert, out)
			}
		}

		out = helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
			"--set", "metrics.prometheusRule.enabled=true", "--set", "backendHealth.enabled=true")
		for _, alert := range gated {
			if !strings.Contains(out, alert) {
				t.Errorf("%s did not render with backendHealth.enabled; got:\n%s", alert, out)
			}
		}
		// The alert uses the SAME severity split as the VolumeCondition path: the
		// two agree on WHICH states are abnormal. Assert the rendered selector as
		// a complete object, not as a phrase that a decoy comment can satisfy.
		degraded := renderedAlert(t, "ScaleCSIPoolDegraded")
		degradedExpr, _ := degraded["expr"].(string)
		statusSelector := regexp.MustCompile(`scale_csi_pool_status\{([^}]*)\}`).FindStringSubmatch(degradedExpr)
		if len(statusSelector) != 2 || statusSelector[1] != `status=~"DEGRADED|FAULTED|UNAVAIL"` {
			t.Errorf("ScaleCSIPoolDegraded must render the exact VolumeCondition severity selector; got %q", degradedExpr)
		}
		// M6 round 3: they do NOT agree on WHEN. The rendered rules must never
		// promise otherwise, and must point at the two gauges that expose the
		// disagreement windows (confirmation lag, recovery, poll stall).
		if strings.Contains(out, "can never disagree") {
			t.Errorf("the rules must not claim the alert and the PVC condition can never disagree; got:\n%s", out)
		}
		for _, honest := range []string{
			"once the two-sample hysteresis confirms the transition",
			"scale_csi_pool_health_flip_pending",
			"max(scale_csi_pool_health_flip_pending) by (pool) == 1",
		} {
			if !strings.Contains(out, honest) {
				t.Errorf("the rendered rules are missing the honest timing wording %q; got:\n%s", honest, out)
			}
		}
	})

	t.Run("schema rejects an unknown backendHealth key", func(t *testing.T) {
		if out := helmTemplateExpectError(t, "--set", "backendHealth.bogus=true"); !strings.Contains(out, "backendHealth") {
			t.Errorf("schema did not reject an unknown backendHealth key; got:\n%s", out)
		}
	})
}

// TestChartGF5BackendHealthRejectsMultipleControllerReplicas is the chart-side
// enforcement for NF2. The poller has no leader-election gate; a second steady
// replica is therefore a configuration error, while termination/drain/eviction
// overlap remains a documented Deployment residual detected by the skew alert.
func TestChartGF5BackendHealthRejectsMultipleControllerReplicas(t *testing.T) {
	schemaOut := helmTemplateExpectError(t, "--set", "backendHealth.enabled=true", "--set", "controller.replicas=2")
	if !strings.Contains(schemaOut, "value must be 1") {
		t.Errorf("the values schema must reject multiple backend-health controller replicas; got:\n%s", schemaOut)
	}
	templateOut := helmTemplateExpectError(t, "--skip-schema-validation", "--set", "backendHealth.enabled=true", "--set", "controller.replicas=2")
	if !strings.Contains(templateOut, "backendHealth.enabled requires controller.replicas=1") {
		t.Errorf("the template must fail with the actionable replica constraint; got:\n%s", templateOut)
	}
}

// TestChartGF5BackendHealthDashboardPanel proves the health panel ships with the
// dashboard and references only registered metrics (the drift test covers the
// name check; this covers presence).
func TestChartGF5BackendHealthDashboardPanel(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/grafana-dashboard.yaml", "--set", "metrics.dashboards.enabled=true")
	for _, want := range []string{
		"Backend Pool Health",
		"scale_csi_pool_healthy",
		"scale_csi_pool_scan_state",
		"scale_csi_pool_disk_temp_alerts",
		"scale_csi_pool_disk_temp_alerts_age_seconds",
		// M6 round 3: an operator reading the panel must be able to tell a held or
		// stale condition from a healthy one without leaving the dashboard.
		"scale_csi_pool_health_flip_pending",
		"scale_csi_pool_health_stale",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dashboard is missing %q; got a %d-byte render", want, len(out))
		}
	}
}

// repoFile reads a file relative to the repository root.
func repoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := filepath.Join(append([]string{chartDir(t), "..", ".."}, parts...)...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

// TestChartGF5SchemaCouplesKRB5ToKrbEnabled is the chart-side half of the KRB5
// fail-closed gate (H2). The schema previously enumerated KRB5* independently of
// nfs.krbEnabled, so `--set nfs.shareSecurity={KRB5}` with krbEnabled=false
// rendered a ConfigMap that stamped KRB5 on EVERY newly created export.
//
// The driver enforces the same rule at config load, because a hand-written
// ConfigMap bypasses this schema entirely; both halves are required.
func TestChartGF5SchemaCouplesKRB5ToKrbEnabled(t *testing.T) {
	for _, mode := range []string{"KRB5", "KRB5I", "KRB5P"} {
		t.Run(mode+" without krbEnabled is rejected", func(t *testing.T) {
			out := helmTemplateExpectError(t, "--set", "nfs.shareSecurity={"+mode+"}")
			if !strings.Contains(out, "shareSecurity") {
				t.Errorf("schema rejection did not mention shareSecurity; got:\n%s", out)
			}
		})
		t.Run(mode+" with krbEnabled renders", func(t *testing.T) {
			out := helmTemplate(t, "--show-only", "templates/configmap.yaml",
				"--set", "nfs.shareSecurity={"+mode+"}", "--set", "nfs.krbEnabled=true")
			if !strings.Contains(out, "        - "+mode+"\n") {
				t.Errorf("acknowledged %s did not render; got:\n%s", mode, out)
			}
		})
	}

	t.Run("SYS never needs the acknowledgement", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "nfs.shareSecurity={SYS}")
		if !strings.Contains(out, "        - SYS\n") {
			t.Errorf("SYS did not render without krbEnabled; got:\n%s", out)
		}
	})

	t.Run("a mixed list is rejected on its Kerberos member", func(t *testing.T) {
		helmTemplateExpectError(t, "--set", "nfs.shareSecurity={SYS,KRB5P}")
	})
}

// TestChartGF5SchemaRejectsMaprootMapallCombo is the chart-side half of M2:
// TrueNAS rejects a sharing.nfs.create payload carrying both squash mappings,
// and the shipped defaults set maproot, so setting mapall alone is a trap.
func TestChartGF5SchemaRejectsMaprootMapallCombo(t *testing.T) {
	out := helmTemplateExpectError(t, "--set", "nfs.shareMapallUser=nobody")
	if !strings.Contains(out, "shareMaproot") {
		t.Errorf("schema rejection did not name the conflicting maproot keys; got:\n%s", out)
	}

	// Clearing maproot resolves it.
	helmTemplate(t, "--show-only", "templates/configmap.yaml",
		"--set", "nfs.shareMapallUser=nobody",
		"--set", "nfs.shareMaprootUser=",
		"--set", "nfs.shareMaprootGroup=")
}

// TestGF5DocsDoNotOverclaimACLProtected is the H3 revert-proof on the shipped
// documentation. `nfs41_flags.protected` is NFSv4.1 ACL4_PROTECTED — automatic
// INHERITANCE suppression — not a chmod guard. The property that governs what a
// chmod does to a non-trivial ACL is `aclmode`, and the driver sets PASSTHROUGH,
// which regenerates the mode-bearing ACEs on every chmod.
func TestGF5DocsDoNotOverclaimACLProtected(t *testing.T) {
	doc := repoFile(t, "docs", "reference", "storageclass.md")

	for _, wrong := range []string{
		"so a\nchmod cannot make the server recompute the ACL from the mode",
		"chmod cannot make the server recompute the ACL from the mode",
	} {
		if strings.Contains(doc, wrong) {
			t.Errorf("storageclass.md still claims protected=true blocks a chmod recompute: %q", wrong)
		}
	}

	for _, required := range []string{
		"inheritance",
		"`aclmode`",
		"nfsACLMode: RESTRICTED",
		"It is **not** a `chmod` guard",
	} {
		if !strings.Contains(doc, required) {
			t.Errorf("storageclass.md is missing the corrected ACL wording %q", required)
		}
	}
}

// TestGF5DocsRecordThePerformanceClassCloneLimit is the H1 revert-proof on the
// documentation: a clone/restore inherits the origin's geometry, so the curated
// class is neither applied nor stamped, and that must be written down.
func TestGF5DocsRecordThePerformanceClassCloneLimit(t *testing.T) {
	doc := repoFile(t, "docs", "reference", "storageclass.md")
	for _, required := range []string{
		"Performance classes do not apply to clones or snapshot restores",
		"ZFSPerformanceClassIgnored",
	} {
		if !strings.Contains(doc, required) {
			t.Errorf("storageclass.md is missing %q", required)
		}
	}
}

// TestGF5Fix2DocsRecordTheContentSourceSemantics is the round-2 documentation
// revert-proof. Both H1 and H3 resolve to ONE semantic — a volume materialized
// from a content source inherits the origin dataset's properties, and the driver
// neither applies nor CLAIMS the dataset-level properties it could not set — and
// the shipped docs have to say so for each of them.
func TestGF5Fix2DocsRecordTheContentSourceSemantics(t *testing.T) {
	doc := repoFile(t, "docs", "reference", "storageclass.md")
	for _, required := range []string{
		// H1: the inherited stamp, the scrub, and the guard that ignores it.
		"inherited from its source",
		"never treats a content-source volume's class\nstamp as authoritative**",
		"CSI idempotency violation",
		// H3: nfsACLMode refused before mutation, ACL itself still applied.
		"`nfsACLMode` is **rejected with `InvalidArgument` before anything is created**",
		"filesystem.setacl` acts on the\n  materialized path and genuinely applies",
	} {
		if !strings.Contains(doc, required) {
			t.Errorf("storageclass.md is missing the round-2 content-source semantics %q", required)
		}
	}

	production := repoFile(t, "docs", "production.md")
	for _, required := range []string{
		// M4: idle is a representable state and unknown cells are retired.
		`Idle is `,
		"retired (zeroed) on the next sample",
		// M6 timing caveat: the interval ceiling that BOUNDS the confirmation lag.
		"clamped to **30s–2m**",
	} {
		if !strings.Contains(production, required) {
			t.Errorf("production.md is missing %q", required)
		}
	}
}

// TestGF5Fix3DocsAreHonestAboutSignalTiming pins the M6 round-3 correction: the
// raw gauges (what the alerts read) and the debounced per-PVC VolumeCondition
// share a severity split, NOT a timeline. The absolute "can never disagree"
// promise was false — the ceiling bounds the confirmation lag but cannot cover a
// confirming sample that never arrives, and cannot remove the deliberate
// one-sample recovery window. Documentation and values must state that plainly
// and name the telemetry that exposes each window.
func TestGF5Fix3DocsAreHonestAboutSignalTiming(t *testing.T) {
	for _, doc := range []struct {
		name     string
		body     string
		required []string
	}{
		{
			name: "docs/production.md",
			body: repoFile(t, "docs", "production.md"),
			required: []string{
				"Signal timing",
				"Confirmation lag",
				"Recovery",
				"Poll stall",
				"scale_csi_pool_health_flip_pending",
				"the condition **holds** its last value",
				"**not** a guarantee that the alert and the",
			},
		},
		{
			name: "charts/scale-csi/values.yaml",
			body: repoFile(t, "charts", "scale-csi", "values.yaml"),
			required: []string{
				"DO differ transiently",
				"scale_csi_pool_health_flip_pending",
				"clamped to 30s-2m",
				"three times the EFFECTIVE (clamped) value",
			},
		},
		{
			name: "charts/scale-csi/values.schema.json",
			body: repoFile(t, "charts", "scale-csi", "values.schema.json"),
			required: []string{
				"clamped to 30s-2m",
				"does not make the raw gauges, the debounced condition and the alert agree at every instant",
			},
		},
		{
			name: "charts/scale-csi/templates/prometheusrule.yaml",
			body: repoFile(t, "charts", "scale-csi", "templates", "prometheusrule.yaml"),
			required: []string{
				"do not claim they",
				"always agree",
				"ScaleCSIPoolConditionFlipPending",
			},
		},
	} {
		t.Run(doc.name, func(t *testing.T) {
			if strings.Contains(doc.body, "can never disagree") {
				t.Errorf("%s still promises the two signals can never disagree; the design does not provide that", doc.name)
			}
			for _, required := range doc.required {
				if !strings.Contains(doc.body, required) {
					t.Errorf("%s is missing the honest timing wording %q", doc.name, required)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GF5 fix-5 — the taxonomy guards assert STRUCTURE, not word presence
// ---------------------------------------------------------------------------
//
// The round-4 guards searched for class names anywhere in a file and checked the
// "bounded poll stall" defect on a single line. Four semantically identical
// regressions walked straight through them: naming the poll stall on one line
// and calling it bounded on the next; renaming the canonical numbered item while
// leaving the phrase elsewhere in the file; paraphrasing the unsound triage
// conclusion; and adding a numbered class while the hard-coded names still all
// matched. The guards below therefore parse the ENUMERATION BLOCK itself — a
// numbered list, in canonical order, count-complete, with each item's own
// boundedness qualifier inside that item's own text.

// ADJUDICATED BOUNDARY: these guards defend against accidental drift; adversarial editing of prose is a non-goal.

// signalTimingClasses is the CANONICAL taxonomy of ways the raw gauges, the
// debounced VolumeCondition, the alert (raw gauge + its own `for` hold, observed
// on Prometheus's cadence) and the external-health-monitor PVC refresh can
// disagree. The canonical prose lives on backendHealthFlipSamples in
// pkg/driver/backend_health.go; every enumerating surface must number the SAME
// classes in the SAME order.
var signalTimingClasses = []string{
	"confirmation lag",
	"alert hold",
	"recovery",
	"poll stall",
	"observer lag",
	"cold start",
	"replica skew",
}

// signalTimingClassRequires pins vocabulary that must appear INSIDE a specific
// item's own text. Scoping matters: round 5 required "successful usable sample"
// anywhere in the file, so moving it into a neighboring item while item 4 said
// "until the backend responds" passed the guard while saying the wrong thing.
var signalTimingClassRequires = map[string][]string{
	"poll stall": {"successful usable sample"},
}

// signalTimingClassBounded records which qualifier each class must carry IN ITS
// OWN item text. A class whose duration has an upper bound must say "bounded"
// and must not say "unbounded"; the three that have none must say the opposite.
// This is what makes "Poll stall" on one line and "BOUNDED by the TTL" on the
// next a failure rather than a pass.
var signalTimingClassBounded = map[string]bool{
	"confirmation lag": true,
	"alert hold":       true,
	"recovery":         true,
	"poll stall":       false,
	"observer lag":     false,
	"cold start":       false,
	"replica skew":     false,
}

var (
	// forbiddenWindowCountRe catches any surface that re-asserts a divergence
	// count smaller than the real one ("two windows", "exactly three bounded
	// ways", "four classes", ...). A count is a promise: if it is asserted
	// anywhere it has to be right everywhere.
	forbiddenWindowCountRe = regexp.MustCompile(`(?i)\b(two|three|four|five|six|2|3|4|5|6)\b[^.\n]{0,40}?\b(windows?|ways?|classes|divergences?)\b`)
	// exactlyCountRe catches the "exactly N" phrasing even when the noun is far
	// away or on the next line.
	exactlyCountRe = regexp.MustCompile(`(?i)\bexactly (two|three|four|five|six)\b`)
	// declaredClassCountRe is the positive form: an enumerating surface must
	// state the real count.
	declaredClassCountRe = regexp.MustCompile(`(?i)\b(seven|7)\b[^.\n]{0,40}?\b(classes|windows?|ways?|divergences?)\b`)
	// boundedWordRe matches only the standalone word. "unbounded" is one word, so
	// the leading \b cannot match inside it.
	boundedWordRe   = regexp.MustCompile(`\bbounded\b`)
	unboundedWordRe = regexp.MustCompile(`\bunbounded\b`)
	pollStallRe     = regexp.MustCompile(`poll stall`)
	// backendAnswersRe is the WRONG termination condition for the poll stall: a
	// valid pool.query that does not list the pool answers and is still a failed
	// sample (pkg/truenas/pool_health.go). Round 5 matched only the exact phrase
	// "until a/the backend answers", so "until the backend responds" — the same
	// false claim — walked straight through. Any verb of ANSWERING is wrong here;
	// only a successful usable SAMPLE ends the stall.
	backendAnswersRe = regexp.MustCompile(
		`until (?:a|the) (?:backend|appliance|truenas)[^.\n]{0,30}?\b(answers?|answered|responds?|responded|replies|replied|returns?|returned|is reachable|comes back|recovers)\b`)
	// pollStallTerminationRe is the termination statement itself. A neighboring
	// sentence mentioning a successful sample is not enough: item 4 must say
	// that the stall lasts until that sample arrives.
	pollStallTerminationRe = regexp.MustCompile(`(?i)\blast(?:s|ing)?\s+until\s+(?:a|the)\s+successful\s+usable\s+sample\s+arrives\b`)
	// numberedItemRe finds an enumeration entry marker. The leading group keeps
	// it from matching inside a version or a decimal.
	numberedItemRe = regexp.MustCompile(`(^|[^\w.])(\d{1,2})\.[ \t]+`)
)

// signalTimingVocabularyRe scopes the count check to sentences that are actually
// about this contract. Repos contain unrelated prose about "windows" (e.g. the
// write/verify race analysis), and flagging that would make the guard useless.
// Word boundaries matter: "unconditional" is not a mention of a condition.
var signalTimingVocabularyRe = regexp.MustCompile(
	`\b(diverg\w*|differs?|differing|gauges?|alerts?|volumecondition|conditions?|hysteresis|debounced?|taxonom\w*|classes|confirmation lag|poll stall|observer lag|cold start|signal timing)\b`)

// normalizeTimingProse lowercases and folds hyphens so "Alert hold",
// "alert-hold" and "poll-stall" all compare equal.
func normalizeTimingProse(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "-", " ")
}

// enclosingSentence returns the text around [start,end) delimited by sentence
// stops, so a match can be judged in context.
func enclosingSentence(body string, start, end int) string {
	from := strings.LastIndexAny(body[:start], ".!?")
	to := strings.IndexAny(body[end:], ".!?")
	if to < 0 {
		to = len(body)
	} else {
		to += end
	}
	return body[from+1 : to]
}

// aboutSignalTiming reports whether a sentence is discussing the raw-versus-
// condition-versus-alert contract at all.
func aboutSignalTiming(sentence string) bool {
	return signalTimingVocabularyRe.MatchString(sentence)
}

// findTimingCountClaim returns the first count claim made INSIDE a signal-timing
// sentence, or "".
func findTimingCountClaim(re *regexp.Regexp, body string) string {
	// Newlines are folded so a claim wrapped across comment lines is still seen
	// as one sentence; the regexes themselves stop at sentence stops.
	flat := strings.ReplaceAll(body, "\n", " ")
	for _, loc := range re.FindAllStringIndex(flat, -1) {
		if aboutSignalTiming(enclosingSentence(flat, loc[0], loc[1])) {
			return strings.TrimSpace(flat[loc[0]:loc[1]])
		}
	}
	return ""
}

// taxonomyItem is one numbered entry of the canonical enumeration together with
// the text that belongs to IT and to no other entry.
type taxonomyItem struct {
	number int
	name   string
	text   string
}

// lineMarker returns the comment/table marker a line carries ("//", "#", "|" or
// ""), which is how the enumeration block is delimited without hard-coding a
// span: the block is the run of lines that share the anchor's marker.
func lineMarker(line string) string {
	trimmed := strings.TrimLeft(line, " \t")
	for _, marker := range []string{"//", "#", "|", "*"} {
		if strings.HasPrefix(trimmed, marker) {
			return marker
		}
	}
	return ""
}

// markerContent strips the marker so an "empty" comment line can be recognized.
func markerContent(line, marker string) string {
	trimmed := strings.TrimSpace(line)
	if marker != "" {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, marker))
	}
	return trimmed
}

// extractSignalTimingTaxonomy parses the canonical enumeration out of one
// surface and returns its items in file order.
//
// The block is located structurally, never by a fixed window: it starts at
// "1. <first class>" and runs to the end of the marker run that the anchor line
// belongs to (or to the end of the anchor line when the whole enumeration is
// written inline, as it is in a JSON description or a table cell). The last
// item additionally stops at the first blank comment line, so trailing prose in
// the same comment paragraph cannot be read as part of it.
func extractSignalTimingTaxonomy(t *testing.T, name, body string) ([]taxonomyItem, int, int) {
	t.Helper()
	body = visibleTaxonomyBody(body)

	anchorRe := regexp.MustCompile(`(^|[^\w.])1\.[ \t]+` + regexp.QuoteMeta(signalTimingClasses[0]))
	all := anchorRe.FindAllStringIndex(body, -1)
	if len(all) == 0 {
		t.Fatalf("%s: no canonical enumeration found. It must start with a numbered item %q — "+
			"every enumerating surface carries the SAME numbered classes in the SAME order: %s",
			name, "1. "+signalTimingClasses[0], strings.Join(signalTimingClasses, ", "))
	}
	if len(all) > 1 {
		// The anchor MUST be unique. Taking the first match let a compliant DECOY
		// list be inserted ahead of the shipped enumeration: the guard validated
		// the decoy and never read the list the operator actually sees.
		lines := make([]int, 0, len(all))
		for _, loc := range all {
			lines = append(lines, strings.Count(body[:loc[0]], "\n")+1)
		}
		t.Fatalf("%s: %d enumerations start with %q (around lines %v). The canonical list must appear EXACTLY ONCE "+
			"per surface — with more than one, a guard that parses the first cannot tell which list ships.",
			name, len(all), "1. "+signalTimingClasses[0], lines)
	}
	loc := all[0]
	start := loc[0]
	if body[start] != '1' {
		start++
	}

	lines := strings.Split(body, "\n")
	anchorLine, offset := 0, 0
	for i, line := range lines {
		if offset+len(line) >= start {
			anchorLine = i
			break
		}
		offset += len(line) + 1
	}
	marker := lineMarker(lines[anchorLine])

	// Inline enumeration (JSON description, markdown table cell): the whole list
	// lives on the anchor line, so the block must not swallow the lines after it.
	inline := true
	for _, class := range signalTimingClasses[1:] {
		if !strings.Contains(lines[anchorLine], class) {
			inline = false
			break
		}
	}

	blockLines := []string{lines[anchorLine]}
	if !inline {
		for i := anchorLine + 1; i < len(lines); i++ {
			if lineMarker(lines[i]) != marker || (marker == "" && strings.TrimSpace(lines[i]) == "") {
				break
			}
			blockLines = append(blockLines, lines[i])
		}
	}
	block := strings.Join(blockLines, "\n")
	block = block[start-offset:]

	// The last item ends at the first blank comment line, so a following
	// paragraph in the same comment run is not attributed to it.
	limit := len(block)
	for i, line := range strings.Split(block, "\n") {
		if i == 0 {
			continue
		}
		if markerContent(line, marker) == "" {
			limit = len(strings.Join(strings.Split(block, "\n")[:i], "\n"))
			break
		}
	}

	matches := numberedItemRe.FindAllStringSubmatchIndex(block, -1)
	items := make([]taxonomyItem, 0, len(matches))
	for i, m := range matches {
		numStart := m[4]
		number := 0
		for _, c := range block[m[4]:m[5]] {
			number = number*10 + int(c-'0')
		}
		end := len(block)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		if end > limit {
			end = limit
		}
		if numStart >= limit {
			// A numbered item after the enumeration paragraph is not part of it.
			continue
		}
		text := block[m[1]:end]
		items = append(items, taxonomyItem{number: number, text: text, name: strings.TrimSpace(text)})
	}
	return items, anchorLine, anchorLine + len(blockLines) - 1
}

// timingSurface records what a file is REQUIRED to do with the taxonomy. Being
// honest about this matters: only six surfaces carry the enumeration. metrics.go
// and the Grafana dashboard reference the canonical list and must merely never
// contradict it — claiming they "name the same classes" was itself false.
type timingSurface struct {
	name string
	path []string
	// enumerates surfaces carry the whole numbered taxonomy.
	// The rest must never contradict it and must point at the canonical copy.
	enumerates bool
}

func signalTimingSurfaces() []timingSurface {
	return []timingSurface{
		{name: "pkg/driver/backend_health.go", path: []string{"pkg", "driver", "backend_health.go"}, enumerates: true},
		{name: "charts/scale-csi/templates/prometheusrule.yaml", path: []string{"charts", "scale-csi", "templates", "prometheusrule.yaml"}, enumerates: true},
		{name: "charts/scale-csi/values.yaml", path: []string{"charts", "scale-csi", "values.yaml"}, enumerates: true},
		{name: "charts/scale-csi/values.schema.json", path: []string{"charts", "scale-csi", "values.schema.json"}, enumerates: true},
		{name: "docs/production.md", path: []string{"docs", "production.md"}, enumerates: true},
		{name: "docs/deployment.md", path: []string{"docs", "deployment.md"}, enumerates: true},
		{name: "pkg/driver/metrics.go", path: []string{"pkg", "driver", "metrics.go"}},
		{name: "charts/scale-csi/templates/grafana-dashboard.yaml", path: []string{"charts", "scale-csi", "templates", "grafana-dashboard.yaml"}},
	}
}

// TestGF5Fix5SignalTimingTaxonomyIsStructurallyComplete is the mechanical guard
// for M6, and it exists because the same overclaim came back four times in
// slightly different words.
//
// Round 3 replaced "the signals can never disagree" with "exactly three bounded
// windows". Round 4 made it four. Both were incomplete: Prometheus observes the
// gauges on its own scrape and rule-evaluation cadence (observer lag), and a
// process whose FIRST sample fails publishes no raw series at all (cold start).
//
// This test fails if ANY surface:
//   - asserts a divergence count below seven, or fails to state seven;
//   - writes the enumeration as anything other than the canonical numbered
//     classes, in canonical order, with no item missing and no item added;
//   - puts the wrong boundedness qualifier inside an item's own text;
//   - calls the poll stall bounded on the line that names it;
//   - still says the poll stall ends when "the backend answers".
func TestGF5Fix5SignalTimingTaxonomyIsStructurallyComplete(t *testing.T) {
	for _, surface := range signalTimingSurfaces() {
		t.Run(surface.name, func(t *testing.T) {
			body := normalizeTimingProse(visibleTaxonomyBody(repoFile(t, surface.path...)))

			if m := findTimingCountClaim(forbiddenWindowCountRe, body); m != "" {
				t.Errorf("%s re-asserts an incomplete divergence count (%q). The taxonomy has SEVEN numbered classes — %s —"+
					" and a count stated on one surface is a promise on all of them.",
					surface.name, m, strings.Join(signalTimingClasses, ", "))
			}
			if m := findTimingCountClaim(exactlyCountRe, body); m != "" {
				t.Errorf("%s re-asserts an incomplete divergence count (%q); the taxonomy has SEVEN classes", surface.name, m)
			}
			if m := backendAnswersRe.FindString(body); m != "" {
				t.Errorf("%s still ends the poll stall when %q. The stall ends at the next SUCCESSFUL USABLE sample:"+
					" a valid pool.query that does not list the pool answers and still takes the failed-sample path.", surface.name, m)
			}
			blockFrom, blockTo := -1, -1
			var items []taxonomyItem
			if surface.enumerates {
				items, blockFrom, blockTo = extractSignalTimingTaxonomy(t, surface.name, body)
			}
			// Outside the enumeration block nothing may call the poll stall
			// bounded. Inside it, the per-ITEM rules below are stricter: they read
			// each item's own text, which is what an inline enumeration needs.
			for i, line := range strings.Split(body, "\n") {
				if i >= blockFrom && i <= blockTo {
					continue
				}
				if pollStallRe.MatchString(line) && boundedWordRe.MatchString(line) {
					t.Errorf("%s:%d describes the poll-stall class as \"bounded\": %q. It lasts until a successful usable"+
						" sample arrives; say so plainly instead.", surface.name, i+1, strings.TrimSpace(line))
				}
			}

			if !surface.enumerates {
				// A non-enumerating surface must still send the reader to the one
				// canonical copy rather than growing a divergent summary.
				if !strings.Contains(body, "backendhealthflipsamples") {
					t.Errorf("%s summarizes the timing contract without pointing at the canonical list"+
						" (backendHealthFlipSamples in pkg/driver/backend_health.go)", surface.name)
				}
				return
			}

			if m := findTimingCountClaim(declaredClassCountRe, body); m == "" {
				t.Errorf("%s enumerates the divergence classes but never states the count (seven)", surface.name)
			}

			if len(items) != len(signalTimingClasses) {
				got := make([]string, 0, len(items))
				for _, item := range items {
					got = append(got, strings.SplitN(item.name, "\n", 2)[0])
				}
				t.Fatalf("%s enumerates %d numbered divergence classes, want exactly %d (%s). Parsed items: %q."+
					" Adding, dropping or renumbering a class here silently breaks every other surface.",
					surface.name, len(items), len(signalTimingClasses), strings.Join(signalTimingClasses, ", "), got)
			}
			for i, item := range items {
				class := signalTimingClasses[i]
				if item.number != i+1 {
					t.Errorf("%s: enumeration item %d is numbered %d; the canonical order is fixed", surface.name, i+1, item.number)
				}
				if !strings.HasPrefix(strings.TrimLeft(item.name, "*_`| "), class) {
					t.Errorf("%s: numbered item %d must be %q, got %q. Renaming the canonical item while the phrase"+
						" survives somewhere else in the file is exactly the regression this guard exists for.",
						surface.name, i+1, class, firstLine(item.name))
				}
				bounded := signalTimingClassBounded[class]
				hasBounded := boundedWordRe.MatchString(item.text)
				hasUnbounded := unboundedWordRe.MatchString(item.text)
				if bounded && (!hasBounded || hasUnbounded) {
					t.Errorf("%s: item %d (%s) must call itself BOUNDED, and only bounded, inside its own text; got %q",
						surface.name, i+1, class, collapse(item.text))
				}
				if !bounded && (!hasUnbounded || hasBounded) {
					t.Errorf("%s: item %d (%s) must call itself UNBOUNDED, and must not claim a bound, inside its own"+
						" text; got %q", surface.name, i+1, class, collapse(item.text))
				}
				itemText := collapseTaxonomyText(item.text)
				// Item-SCOPED vocabulary. A required phrase sitting in a NEIGHBORING
				// item is not this item saying it.
				for _, required := range signalTimingClassRequires[class] {
					if !strings.Contains(itemText, required) {
						t.Errorf("%s: item %d (%s) must state %q in ITS OWN text; got %q. Moving the phrase into another"+
							" item leaves this one free to state the wrong termination condition.",
							surface.name, i+1, class, required, itemText)
					}
				}
				if class == "poll stall" && !pollStallTerminationRe.MatchString(itemText) {
					t.Errorf("%s: item %d (%s) must make its OWN termination statement that the stall lasts until a successful"+
						" usable sample arrives; got %q", surface.name, i+1, class, itemText)
				}
				if m := backendAnswersRe.FindString(itemText); m != "" {
					t.Errorf("%s: item %d (%s) ends the window at %q. Only a SUCCESSFUL USABLE sample ends it — a valid"+
						" pool.query that does not list the pool answers and is still a failed sample.",
						surface.name, i+1, class, m)
				}
			}
		})
	}
}

func firstLine(s string) string {
	return strings.TrimSpace(strings.SplitN(s, "\n", 2)[0])
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// collapseTaxonomyText folds a numbered item while removing the line markers
// used by Go/YAML/Markdown comment blocks. This lets the structural guards
// assert a sentence that crosses a wrapped comment line without treating the
// marker itself as prose.
func collapseTaxonomyText(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		for _, marker := range []string{"//", "#", "|", "*"} {
			if strings.HasPrefix(trimmed, marker) {
				trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, marker))
				break
			}
		}
		trimmed = strings.NewReplacer("**", "", "`", "").Replace(trimmed)
		lines[i] = trimmed
	}
	return collapse(strings.Join(lines, " "))
}

var nonVisibleHTMLBlockRe = regexp.MustCompile(`(?is)<!--.*?-->|<script\b[^>]*>.*?</script\s*>|<style\b[^>]*>.*?</style\s*>|<template\b[^>]*>.*?</template\s*>|<noscript\b[^>]*>.*?</noscript\s*>|<(?:div|span|section|article|p|li|ol|ul|table|thead|tbody|tr|td|th|pre)\b[^>]*(?:\bhidden\b|aria-hidden\s*=\s*["']?true|display\s*:\s*none|visibility\s*:\s*hidden)[^>]*>.*?</(?:div|span|section|article|p|li|ol|ul|table|thead|tbody|tr|td|th|pre)\s*>`)

// visibleTaxonomyBody removes HTML comments and blocks that cannot be seen by
// an operator before locating the canonical anchor. Newlines are retained so
// diagnostics still point at useful source lines, and a visible duplicate
// remains an ambiguity that must fail the guard.
func visibleTaxonomyBody(body string) string {
	return nonVisibleHTMLBlockRe.ReplaceAllStringFunc(body, func(hidden string) string {
		return strings.Map(func(r rune) rune {
			if r == '\n' {
				return r
			}
			return ' '
		}, hidden)
	})
}

// collapseProse folds a wrapped comment block into a single line so a sentence
// that spans lines can be matched: leading // and # markers are dropped first.
func collapseProse(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		trimmed = strings.TrimPrefix(trimmed, "//")
		trimmed = strings.TrimPrefix(trimmed, "#")
		lines[i] = trimmed
	}
	return collapse(strings.Join(lines, " "))
}

// triageRuleBlock returns the operator-facing triage procedure in
// docs/production.md: the "Triage rule" paragraph plus the numbered steps that
// belong to it, and nothing after them.
func triageRuleBlock(t *testing.T, production string) string {
	t.Helper()
	idx := strings.Index(production, "**Triage rule.**")
	if idx < 0 {
		t.Fatal("docs/production.md no longer contains a `**Triage rule.**` block")
	}
	lines := strings.Split(production[idx:], "\n")
	listish := regexp.MustCompile(`^(\s+\S|\s*(\d+\.|[-*])\s)`)
	end := len(lines)
	for i, line := range lines {
		if strings.TrimSpace(line) != "" {
			continue
		}
		if i+1 < len(lines) && listish.MatchString(lines[i+1]) {
			continue
		}
		end = i
		break
	}
	return strings.Join(lines[:end], "\n")
}

// TestGF5Fix5TriageRuleRequiresSynchronizedObservations pins the operator-facing
// consequence of classes 2, 5 and 6: two diagnostic gauges reading 0 prove
// nothing on their own.
//
// During the alert hold both gauges read 0 while the condition and the alert
// genuinely differ. After a recovery both gauges read 0 while the alert is still
// firing simply because Prometheus has not scraped or re-evaluated yet. So the
// triage procedure has to (a) check the gauges, (b) check the alert's own state,
// (c) check that the three observations are SYNCHRONIZED — sample/scrape
// freshness and, during a rollout, controller identity — and (d) stop short of
// declaring a defect. The order matters, so it is asserted.
// conclusionVocabRe is the vocabulary of DECLARING a defect. Round 5 forbade
// only the literal word "real" in the triage block, so "treat the mismatch as a
// defect" — the same unsound conclusion — passed while every required substring
// survived in order.
var conclusionVocabRe = regexp.MustCompile(`(?i)\b(real|defect|defects|bug|bugs|genuine|broken)\b`)

// structuredConclusionNegationRe is deliberately narrow. A generic negation
// token anywhere in the sentence lets "A mismatch is a defect, not a bug"
// satisfy the guard for "defect" even though that conclusion is positive.
var structuredConclusionNegationRe = regexp.MustCompile(`(?i)(?:\bnot\s+(?:a|an)?\s*(?:real\s+)?(?:defect|defects|bug|bugs|genuine|broken)|\bnever\s+conclude\s+(?:a|an)?\s*(?:real\s+)?(?:defect|defects|bug|bugs|genuine|broken))\s*$`)

var (
	triageConclusionStructureRe = regexp.MustCompile(`(?i)\bonly when\b[^.?!]*\bremaining difference\b[^.?!]*\bunexplained by the documented classes\b`)
	triageExplicitNegationRe    = regexp.MustCompile(`(?i)\bnever conclude a defect from two diagnostic gauges reading 0\b`)
)

func conclusionWordIsNegated(flat string, loc []int) bool {
	sentenceStart := strings.LastIndexAny(flat[:loc[0]], ".?!") + 1
	return structuredConclusionNegationRe.MatchString(flat[sentenceStart:loc[1]])
}

func TestGF5Fix5TriageRuleRequiresSynchronizedObservations(t *testing.T) {
	production := repoFile(t, "docs", "production.md")
	collapsed := collapse(production)

	// Whole-file: the unsound conclusion must not exist anywhere, in any wording.
	unsoundConclusionRe := regexp.MustCompile(`(?i)both[^.\n]{0,80}gauges[^.]{0,240}\b(real|defect|bug|genuine|broken)\b`)
	if m := unsoundConclusionRe.FindString(collapsed); m != "" {
		t.Errorf("docs/production.md still concludes from two zero gauges that a difference is a defect (%q)."+
			" Both gauges read 0 throughout the alert-hold, observer-lag, cold-start and replica-skew classes.", m)
	}

	block := triageRuleBlock(t, production)
	// Every defect-declaring word in the procedure must sit inside a NEGATED
	// sentence. This is the semantic form of the round-5 rule: it no longer
	// matters which noun the wording picks.
	flat := collapse(block)
	structureFlat := strings.NewReplacer("**", "", "`", "").Replace(flat)
	if !triageConclusionStructureRe.MatchString(structureFlat) || !triageExplicitNegationRe.MatchString(structureFlat) {
		t.Errorf("the triage conclusion no longer has the required structure: one fresh pod observation must establish an"+
			" unexplained difference before the explicit two-gauge conclusion is negated; got %q", flat)
	}
	for _, loc := range conclusionVocabRe.FindAllStringIndex(flat, -1) {
		if !conclusionWordIsNegated(flat, loc) {
			sentence := enclosingSentence(flat, loc[0], loc[1])
			t.Errorf("the triage procedure declares a mismatch %q without negating it: %q. A remaining difference can"+
				" only be called UNEXPLAINED, and only after sample age and producer identity have been checked.",
				flat[loc[0]:loc[1]], strings.TrimSpace(sentence))
		}
	}

	// The freshness step must use the DRIVER-OWNED timestamp. PromQL timestamp()
	// returns the scrape time, so it cannot answer "when did the driver last
	// sample?" — wherever it still appears, it must be labeled as scrape age.
	if !strings.Contains(block, "scale_csi_pool_health_last_success_timestamp_seconds") {
		t.Error("the triage block does not use the driver-owned scale_csi_pool_health_last_success_timestamp_seconds" +
			" for sample age; timestamp() on any other gauge returns the SCRAPE time, so a frozen driver that keeps" +
			" answering scrapes reads as fresh")
	}
	for _, loc := range regexp.MustCompile(`timestamp\(scale_csi\w*`).FindAllStringIndex(flat, -1) {
		window := flat[loc[0]:min(len(flat), loc[1]+200)]
		if !strings.Contains(strings.ToLower(window), "scrape") {
			t.Errorf("the triage block uses %q without saying it is the SCRAPE time: %q", flat[loc[0]:loc[1]], collapse(window))
		}
	}

	// The procedure, in order. Each step must come after the previous one.
	cursor := 0
	for _, required := range []string{
		"scale_csi_pool_health_flip_pending",
		"scale_csi_pool_health_stale",
		`alertstate="pending"`,
		"neither gauge",
		"synchronized observations",
		"sample age",
		"scale_csi_pool_health_last_success_timestamp_seconds",
		"ontroller identity",
		"pod",
		"unexplained by the documented classes",
	} {
		at := strings.Index(block[cursor:], required)
		if at < 0 {
			t.Errorf("the triage block is missing %q, or states it out of order (gauges -> alert state ->"+
				" synchronized observations -> conclusion)", required)
			continue
		}
		cursor += at + len(required)
	}
}

// TestGF5Fix5TimeBoundsIncludeTheCallTimeout pins finding 4: nothing is
// published until the backend call RETURNS, and that call is bounded at 30s by
// backendHealthCallTimeout. A confirmed condition therefore trails a state
// change by up to 2 x interval PLUS one call timeout — 4m30s at the ceiling, not
// the 4m that 2 x interval alone suggests (still inside the 5m alert hold).
// driverConstRe extracts a Go constant declaration from the driver source. The
// guard reads the CONSTANTS rather than the prose, because round 5 asserted the
// literal strings "2m30s"/"4m30s" and would therefore have passed with
// backendHealthCallTimeout changed to 45s and the docs untouched.
func driverConstRe(name string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^const ` + regexp.QuoteMeta(name) + ` = (\d+)(?: \* time\.(Second|Minute))?$`)
}

func driverDurationConst(t *testing.T, src, name string) time.Duration {
	t.Helper()
	m := driverConstRe(name).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("pkg/driver/backend_health.go no longer declares `const %s = N * time.Unit`; the documented bounds are"+
			" computed from it, so the guard cannot verify them", name)
	}
	n, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	switch m[2] {
	case "Second":
		return time.Duration(n) * time.Second
	case "Minute":
		return time.Duration(n) * time.Minute
	}
	t.Fatalf("const %s is not a duration", name)
	return 0
}

func driverIntConst(t *testing.T, src, name string) int {
	t.Helper()
	m := driverConstRe(name).FindStringSubmatch(src)
	if m == nil || m[2] != "" {
		t.Fatalf("pkg/driver/backend_health.go no longer declares `const %s = N`", name)
	}
	n, err := strconv.Atoi(m[1])
	require.NoError(t, err)
	return n
}

func TestGF5Fix5TimeBoundsIncludeTheCallTimeout(t *testing.T) {
	// COMPUTE the bounds from the shipped constants. Changing a constant while
	// leaving the old number in the docs now fails here.
	driverSrc := repoFile(t, "pkg", "driver", "backend_health.go")
	callTimeout := driverDurationConst(t, driverSrc, "backendHealthCallTimeout")
	maxInterval := driverDurationConst(t, driverSrc, "maxBackendHealthInterval")
	flipSamples := driverIntConst(t, driverSrc, "backendHealthFlipSamples")

	perStep := (maxInterval + callTimeout).String()                              // one confirmation step
	confirmed := (time.Duration(flipSamples)*maxInterval + callTimeout).String() // to a confirmed condition
	if perStep == confirmed {
		t.Fatalf("the two computed bounds collapsed to %q; the guard would assert nothing", perStep)
	}
	// The bound that OMITS the call timeout is the regression this pins.
	omitted := (time.Duration(flipSamples) * maxInterval).String()
	staleBoundRe := regexp.MustCompile(`(?i)at most ` + regexp.QuoteMeta(omitted) + `\b|(<=|≤) ` + regexp.QuoteMeta(maxInterval.String()) + `\b`)

	for _, surface := range []struct {
		name string
		path []string
	}{
		{name: "pkg/driver/backend_health.go", path: []string{"pkg", "driver", "backend_health.go"}},
		{name: "docs/production.md", path: []string{"docs", "production.md"}},
		{name: "charts/scale-csi/templates/prometheusrule.yaml", path: []string{"charts", "scale-csi", "templates", "prometheusrule.yaml"}},
		{name: "charts/scale-csi/values.yaml", path: []string{"charts", "scale-csi", "values.yaml"}},
		{name: "charts/scale-csi/values.schema.json", path: []string{"charts", "scale-csi", "values.schema.json"}},
	} {
		t.Run(surface.name, func(t *testing.T) {
			body := repoFile(t, surface.path...)
			if m := staleBoundRe.FindString(body); m != "" {
				t.Errorf("%s states a driver-side bound of %q, which omits the bounded backend call time:"+
					" a poll may take the full %v backendHealthCallTimeout before anything is published",
					surface.name, m, callTimeout)
			}
			lower := normalizeTimingProse(body)
			if !strings.Contains(lower, "call timeout") && !strings.Contains(lower, "backendhealthcalltimeout") {
				t.Errorf("%s states the confirmation bound without naming the backend call timeout that is part of it", surface.name)
			}
			if !strings.Contains(lower, confirmed) {
				t.Errorf("%s does not state the AGGREGATE confirmed bound COMPUTED from the shipped constants (maxBackendHealthInterval=%v,"+
					" backendHealthCallTimeout=%v, backendHealthFlipSamples=%d): %s to a confirmed condition at the ceiling."+
					" A per-step bound (%s) is not sufficient; changing a constant without updating the aggregate text is"+
					" exactly the drift this asserts.", surface.name, maxInterval, callTimeout, flipSamples, confirmed, perStep)
			}
			// The number is a DRIVER-SIDE publication bound. Presenting it as an
			// end-to-end PVC/alert deadline is the round-5 overclaim.
			if strings.Contains(lower, confirmed) &&
				!strings.Contains(lower, "driver-side") && !strings.Contains(lower, "driver side") {
				t.Errorf("%s states %s without scoping it to DRIVER-SIDE publication. The CSI read,"+
					" external-health-monitor refresh, scrape and rule evaluation are all downstream of it and"+
					" unbounded, so it is not a deadline for a PVC becoming abnormal.", surface.name, confirmed)
			}
		})
	}
}

// TestGF5Fix5PollStallEndsAtAUsableSample pins finding (c): "the backend
// answered" is not the poll stall's termination condition. PoolHealth can get a
// perfectly valid pool.query response that simply does not contain the pool and
// return "pool ... not found" (pkg/truenas/pool_health.go), which takes the same
// failed-sample path as an unreachable appliance.
func TestGF5Fix5PollStallEndsAtAUsableSample(t *testing.T) {
	for _, surface := range []struct {
		name string
		path []string
	}{
		{name: "pkg/driver/backend_health.go", path: []string{"pkg", "driver", "backend_health.go"}},
		{name: "docs/production.md", path: []string{"docs", "production.md"}},
		{name: "charts/scale-csi/templates/prometheusrule.yaml", path: []string{"charts", "scale-csi", "templates", "prometheusrule.yaml"}},
		{name: "charts/scale-csi/values.yaml", path: []string{"charts", "scale-csi", "values.yaml"}},
		{name: "charts/scale-csi/values.schema.json", path: []string{"charts", "scale-csi", "values.schema.json"}},
	} {
		t.Run(surface.name, func(t *testing.T) {
			body := normalizeTimingProse(visibleTaxonomyBody(repoFile(t, surface.path...)))
			items, _, _ := extractSignalTimingTaxonomy(t, surface.name, body)
			if len(items) <= 3 || !pollStallTerminationRe.MatchString(collapseTaxonomyText(items[3].text)) {
				t.Errorf("%s does not make the poll stall's OWN item say it lasts until a SUCCESSFUL USABLE sample arrives", surface.name)
			}
		})
	}
}

// renderedAlert renders the bundled PrometheusRule with every gated block on and
// returns one alert rule as decoded YAML. Comparing against the RENDERED object
// is what makes the assertions below semantic: an equivalent re-formatting of
// the template cannot skip them, and a changed expression cannot hide behind a
// substring that is no longer there.
func renderedAlert(t *testing.T, name string) manifest {
	t.Helper()
	rendered := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "capacity.gaugeEnabled=true",
		"--set", "backendHealth.enabled=true")
	rule := findManifest(t, decodeManifests(t, rendered), "PrometheusRule", "scale-csi")
	spec, ok := asManifest(rule["spec"])
	if !ok {
		t.Fatal("PrometheusRule has no decodable spec")
	}
	groups, _ := spec["groups"].([]any)
	for _, groupAny := range groups {
		group, ok := asManifest(groupAny)
		if !ok {
			continue
		}
		rules, _ := group["rules"].([]any)
		for _, ruleAny := range rules {
			alert, ok := asManifest(ruleAny)
			if !ok {
				continue
			}
			if got, _ := alert["alert"].(string); got == name {
				return alert
			}
		}
	}
	t.Fatalf("the rendered PrometheusRule contains no alert named %q", name)
	return nil
}

// TestGF5Fix5ReplicaSkewIsDetectableNotInferred pins divergence class 7: the
// producer-identity class needs its own DETECTION, not a "pin a pod" hint. The
// driver-owned last-success timestamp is one series per controller process, so
// counting it counts producers.
func TestGF5Fix5ReplicaSkewIsDetectable(t *testing.T) {
	skew := renderedAlert(t, "ScaleCSIPoolHealthProducerSkew")
	expr, _ := skew["expr"].(string)
	if !regexp.MustCompile(`^\s*count\s+by\s+\(\s*pool\s*\)\s*\(\s*scale_csi_pool_health_last_success_timestamp_seconds\s*\)\s*>\s*1\s*$`).MatchString(expr) {
		t.Errorf("ScaleCSIPoolHealthProducerSkew must render count by (pool) of the driver-owned timestamp > 1; got %q", expr)
	}

	// The chart must also render a NON-OVERLAPPING rollout when the poller is on,
	// or the invariant the taxonomy claims does not exist.
	deployment := helmTemplate(t, "--show-only", "templates/controller-deployment.yaml", "--set", "backendHealth.enabled=true")
	controller := findManifest(t, decodeManifests(t, deployment), "Deployment", "controller")
	spec, ok := asManifest(controller["spec"])
	if !ok {
		t.Fatal("controller Deployment has no decodable spec")
	}
	strategy, ok := asManifest(spec["strategy"])
	if !ok {
		t.Fatal("controller Deployment renders no strategy")
	}
	switch strategy["type"] {
	case "Recreate":
		// Non-overlapping by construction.
	case "RollingUpdate":
		rolling, ok := asManifest(strategy["rollingUpdate"])
		if !ok {
			t.Fatal("RollingUpdate strategy with no rollingUpdate block")
		}
		if fmt.Sprint(rolling["maxSurge"]) != "0" {
			t.Errorf("with backendHealth.enabled the controller rollout must not overlap producers"+
				" (maxSurge must be 0); got maxSurge=%v. Two overlapping processes publish independently sampled"+
				" scale_csi_pool_* series that every `max by (pool)` rule merges.", rolling["maxSurge"])
		}
	default:
		t.Errorf("unexpected controller strategy type %v", strategy["type"])
	}
}

// TestGF5Fix5MetricsCommentsAgreeWithTheBundledAlerts pins finding 3: two
// comments in pkg/driver/metrics.go contradicted the alerts they describe.
// ScaleCSIPoolDegraded has NO stale gate, so a frozen DEGRADED sample keeps it
// firing after a real recovery; and flip_pending = 1 does not imply the served
// condition is the previous verdict, because past the TTL it is dataset-only.
func TestGF5Fix5MetricsCommentsAgreeWithTheBundledAlerts(t *testing.T) {
	metrics := repoFile(t, "pkg", "driver", "metrics.go")

	// Assert against the RENDERED alert, not the template text. Round 5 read raw
	// bytes and only entered this branch for ONE exact formatting of the
	// expression and hold, so re-formatting the YAML — or actually gating the
	// alert on the stale gauge — skipped the semantic check entirely.
	degraded := renderedAlert(t, "ScaleCSIPoolDegraded")
	expr, _ := degraded["expr"].(string)
	if !strings.Contains(expr, "scale_csi_pool_status") {
		t.Fatalf("ScaleCSIPoolDegraded no longer reads the raw pool status gauge: %q", expr)
	}
	if strings.Contains(expr, "scale_csi_pool_health_stale") {
		t.Errorf("ScaleCSIPoolDegraded is now GATED on scale_csi_pool_health_stale (%q), but pkg/driver/metrics.go"+
			" documents it as deliberately ungated. Change both or neither.", expr)
	} else if strings.Contains(metrics, "must not keep alerting after a real recovery") {
		t.Error("pkg/driver/metrics.go still claims the staleness TTL stops a stale DEGRADED from alerting;" +
			" ScaleCSIPoolDegraded has no stale gate, so the frozen sample keeps it firing")
	}
	// The alert-hold class is BOUNDED BY THIS HOLD, so the canonical taxonomy has
	// to name the value the chart actually renders.
	hold, _ := degraded["for"].(string)
	if hold == "" {
		t.Fatal("ScaleCSIPoolDegraded renders no `for` hold; the alert-hold class has nothing to be bounded by")
	}
	canonical := repoFile(t, "pkg", "driver", "backend_health.go")
	if !strings.Contains(canonical, "`for: "+hold+"`") {
		t.Errorf("ScaleCSIPoolDegraded renders `for: %s`, but the canonical taxonomy in"+
			" pkg/driver/backend_health.go does not bound the alert-hold class by that value", hold)
	}

	collapsedMetrics := collapseProse(metrics)
	for _, required := range []string{
		"deliberately not gated on this one",
		"FROZEN DEGRADED sample keeps that alert firing",
		"NOT a complete disagreement detector",
	} {
		if !strings.Contains(collapsedMetrics, collapse(required)) {
			t.Errorf("pkg/driver/metrics.go is missing the corrected wording %q", required)
		}
	}

	health := collapseProse(repoFile(t, "pkg", "driver", "backend_health_test.go"))
	if strings.Contains(health, "a stale DEGRADED keeps alerting after a real recovery") {
		t.Error("pkg/driver/backend_health_test.go repeats the same false claim in a comment: the TTL bounds" +
			" CONDITIONS, not alerting")
	}
}
