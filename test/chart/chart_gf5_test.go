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
