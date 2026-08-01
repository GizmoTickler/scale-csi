package chart

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
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
		// two agree on WHICH states are abnormal.
		if !strings.Contains(out, `status=~"DEGRADED|FAULTED|UNAVAIL"`) {
			t.Errorf("ScaleCSIPoolDegraded must match the VolumeCondition severity split; got:\n%s", out)
		}
		if strings.Contains(out, `status=~"DEGRADED|FAULTED|UNAVAIL|OFFLINE`) {
			t.Errorf("OFFLINE/REMOVED must not raise a critical alert; got:\n%s", out)
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

// signalTimingClasses is the CANONICAL taxonomy of ways the raw gauges, the
// debounced VolumeCondition and the alert (raw gauge + its own `for` hold) can
// disagree. The canonical prose lives on backendHealthFlipSamples in
// pkg/driver/backend_health.go; every other surface must name the SAME classes.
var signalTimingClasses = []string{
	"confirmation lag",
	"alert hold",
	"recovery",
	"poll stall",
}

var (
	// forbiddenWindowCountRe catches any surface that re-asserts a divergence
	// count smaller than the real one ("two windows", "exactly three bounded,
	// intended ways", "the three bounded windows", ...). A count is a promise: if
	// it is asserted anywhere it has to be right everywhere, so the only safe
	// numbers here are none at all or "four".
	forbiddenWindowCountRe = regexp.MustCompile(`(?i)\b(two|three|2|3)\b[^.\n]{0,40}?\b(windows?|ways?|classes|divergences?)\b`)
	// exactlyCountRe catches the "exactly N" phrasing even when the noun is far
	// away or on the next line.
	exactlyCountRe = regexp.MustCompile(`(?i)\bexactly (two|three)\b`)
	// boundedWordRe matches only the standalone word. "unbounded" is one word, so
	// the leading \b cannot match inside it.
	boundedWordRe = regexp.MustCompile(`\bbounded\b`)
	pollStallRe   = regexp.MustCompile(`poll stall`)
)

// signalTimingVocabularyRe scopes the count check to sentences that are actually
// about this contract. Repos contain unrelated prose about "windows" (e.g. the
// write/verify race analysis), and flagging that would make the guard useless.
// Word boundaries matter: "unconditional" is not a mention of a condition.
var signalTimingVocabularyRe = regexp.MustCompile(
	`\b(diverg\w*|differs?|differing|gauges?|alerts?|volumecondition|conditions?|hysteresis|debounced?|confirmation lag|poll stall|signal timing)\b`)

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

// TestGF5Fix4SignalTimingTaxonomyIsCompleteEverywhere is the mechanical guard
// for the M6 round-4 correction, and it exists because the same overclaim came
// back three times in slightly different words.
//
// Round 3 replaced "the signals can never disagree" with "exactly three bounded
// windows". That was still false twice over: the PrometheusRule's own `for: 5m`
// creates a FOURTH window in which the PVC condition has already confirmed while
// the alert is merely PENDING (and BOTH diagnostic gauges read 0), and the
// poll-stall window is not bounded at all — its own stated duration is "until
// the backend answers".
//
// So this test fails if ANY surface:
//   - asserts a divergence count below four (in any of the usual phrasings), or
//   - calls the poll-stall class "bounded" on the same line it names it, or
//   - drops one of the four canonical class names, or
//   - stops saying, somewhere, that something here is unbounded.
func TestGF5Fix4SignalTimingTaxonomyIsCompleteEverywhere(t *testing.T) {
	for _, surface := range []struct {
		name string
		path []string
		// enumerates surfaces carry the whole taxonomy and must name every class.
		// The rest must merely never contradict it.
		enumerates bool
	}{
		{name: "pkg/driver/backend_health.go", path: []string{"pkg", "driver", "backend_health.go"}, enumerates: true},
		{name: "charts/scale-csi/templates/prometheusrule.yaml", path: []string{"charts", "scale-csi", "templates", "prometheusrule.yaml"}, enumerates: true},
		{name: "charts/scale-csi/values.yaml", path: []string{"charts", "scale-csi", "values.yaml"}, enumerates: true},
		{name: "charts/scale-csi/values.schema.json", path: []string{"charts", "scale-csi", "values.schema.json"}, enumerates: true},
		{name: "docs/production.md", path: []string{"docs", "production.md"}, enumerates: true},
		{name: "docs/deployment.md", path: []string{"docs", "deployment.md"}, enumerates: true},
		{name: "pkg/driver/metrics.go", path: []string{"pkg", "driver", "metrics.go"}},
		{name: "charts/scale-csi/templates/grafana-dashboard.yaml", path: []string{"charts", "scale-csi", "templates", "grafana-dashboard.yaml"}},
	} {
		t.Run(surface.name, func(t *testing.T) {
			body := normalizeTimingProse(repoFile(t, surface.path...))

			if m := findTimingCountClaim(forbiddenWindowCountRe, body); m != "" {
				t.Errorf("%s re-asserts an incomplete divergence count (%q). The taxonomy has FOUR classes — %s —"+
					" and a count stated on one surface is a promise on all of them.",
					surface.name, m, strings.Join(signalTimingClasses, ", "))
			}
			if m := findTimingCountClaim(exactlyCountRe, body); m != "" {
				t.Errorf("%s re-asserts an incomplete divergence count (%q); the taxonomy has FOUR classes", surface.name, m)
			}

			// The poll stall lasts "until the backend answers" — there is no bound
			// on that. Naming it and calling it bounded in the same breath is the
			// exact round-3 defect.
			for i, line := range strings.Split(body, "\n") {
				if pollStallRe.MatchString(line) && boundedWordRe.MatchString(line) {
					t.Errorf("%s:%d describes the poll-stall class as \"bounded\": %q. It lasts until the backend answers;"+
						" say so plainly instead.", surface.name, i+1, strings.TrimSpace(line))
				}
			}

			if !surface.enumerates {
				return
			}
			for _, class := range signalTimingClasses {
				if !strings.Contains(body, class) {
					t.Errorf("%s enumerates the divergence classes but omits %q; every enumeration must be complete and identical", surface.name, class)
				}
			}
			if !strings.Contains(body, "unbounded") {
				t.Errorf("%s enumerates the divergence classes without ever saying the poll stall is unbounded", surface.name)
			}
		})
	}
}

// TestGF5Fix4TriageRuleAccountsForTheAlertHold pins the operator-facing
// consequence of the fourth window: during the alert hold BOTH diagnostic gauges
// read 0 while the PVC condition and the alert genuinely disagree, so
// "both gauges are 0 therefore any difference is real" is not a sound triage
// rule and must not come back.
func TestGF5Fix4TriageRuleAccountsForTheAlertHold(t *testing.T) {
	production := repoFile(t, "docs", "production.md")
	collapsed := strings.Join(strings.Fields(production), " ")

	if strings.Contains(collapsed, "signals are describing the same confirmed state and any difference is real") {
		t.Error("docs/production.md still tells operators that both gauges reading 0 proves a difference is real;" +
			" during the alert hold both gauges read 0 while the condition and the alert legitimately differ")
	}
	for _, required := range []string{
		// The window itself, and the ONLY signal that can distinguish it.
		`alertstate="pending"`,
		"neither gauge",
		"check the alert's own state",
	} {
		if !strings.Contains(collapsed, strings.Join(strings.Fields(required), " ")) {
			t.Errorf("docs/production.md triage guidance is missing %q", required)
		}
	}
}
