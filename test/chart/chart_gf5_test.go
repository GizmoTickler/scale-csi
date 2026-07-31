package chart

import (
	"os"
	"path/filepath"
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
		for _, alert := range []string{"ScaleCSIPoolDegraded", "ScaleCSIPoolScanErrors", "ScaleCSIPoolDiskTemperatureAlert"} {
			if strings.Contains(out, alert) {
				t.Errorf("%s must not render without backendHealth.enabled; got:\n%s", alert, out)
			}
		}

		out = helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
			"--set", "metrics.prometheusRule.enabled=true", "--set", "backendHealth.enabled=true")
		for _, alert := range []string{"ScaleCSIPoolDegraded", "ScaleCSIPoolScanErrors", "ScaleCSIPoolDiskTemperatureAlert"} {
			if !strings.Contains(out, alert) {
				t.Errorf("%s did not render with backendHealth.enabled; got:\n%s", alert, out)
			}
		}
		// The alert must use the SAME severity split as the VolumeCondition path,
		// so an alert and a PVC event can never disagree.
		if !strings.Contains(out, `status=~"DEGRADED|FAULTED|UNAVAIL"`) {
			t.Errorf("ScaleCSIPoolDegraded must match the VolumeCondition severity split; got:\n%s", out)
		}
		if strings.Contains(out, `status=~"DEGRADED|FAULTED|UNAVAIL|OFFLINE`) {
			t.Errorf("OFFLINE/REMOVED must not raise a critical alert; got:\n%s", out)
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
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dashboard is missing %q; got a %d-byte render", want, len(out))
		}
	}
}
