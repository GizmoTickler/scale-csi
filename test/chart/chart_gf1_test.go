package chart

// GF-Sprint 1 (encryption at rest) chart assertions. Encryption ships
// default-off and opt-in per StorageClass, so the load-bearing invariant is that
// a deployment which never touches encryption emits NO encryption surface at
// all: any stray encryption key in the default render would crash-loop a
// rolled-back binary whose Config has no encryption field.
//
// This was originally expressed as a byte-identity comparison against
// `git archive main -- charts`. That formulation decayed the moment GF1 merged:
// once the encryption work was on main, the guard compared the chart to itself
// and passed vacuously, and it had been doing so since v1.6.0. Pinning it to a
// fixed pre-encryption ref was no better — by v1.6.1 every manifest differed for
// unrelated reasons, so the exempt list would have had to swallow the whole
// chart. The invariant is now asserted directly against the render, which cannot
// decay with branch state, plus a second check proving the token list is live.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

// gf1EncryptionSurface is every token the chart emits when encryption is turned
// on, across ALL manifests. The default render must contain none of them.
//
// The csi.storage.k8s.io secret refs are NOT encryption-exclusive — iSCSI CHAP
// emits the same keys — so their absence is only meaningful against DEFAULT
// values, which is exactly the render this guard checks.
var gf1EncryptionSurface = []string{
	"encryption:",
	"csi.storage.k8s.io/provisioner-secret-name",
	"csi.storage.k8s.io/provisioner-secret-namespace",
	"csi.storage.k8s.io/controller-publish-secret-name",
	"csi.storage.k8s.io/controller-publish-secret-namespace",
	"csi.storage.k8s.io/node-stage-secret-name",
	"csi.storage.k8s.io/node-stage-secret-namespace",
}

// renderChart runs `helm template scale-csi <dir>` with optional extra args and
// returns the full multi-document render. It skips the test when helm is absent.
func renderChart(t *testing.T, dir string, extraArgs ...string) []byte {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart template assertion")
	}
	args := append([]string{"template", "scale-csi", dir}, extraArgs...)
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %v failed: %v\n%s", args, err, out)
	}
	return out
}

// TestChartGF1DefaultOffCarriesNoEncryptionSurface is the default-off guard. It
// renders the WHOLE chart with default values and asserts no encryption token
// appears in any manifest — the breadth the old byte comparison provided, since
// a stray key could land in the bundled StorageClass, RBAC, or Secret just as
// easily as in the ConfigMap. The sibling tests below cover the ConfigMap and
// StorageClass enabled-paths in detail; this one covers everything at once.
func TestChartGF1DefaultOffCarriesNoEncryptionSurface(t *testing.T) {
	t.Run("default render emits no encryption surface in any manifest", func(t *testing.T) {
		out := string(renderChart(t, chartDir(t)))
		for _, token := range gf1EncryptionSurface {
			if strings.Contains(out, token) {
				t.Errorf("default render must not emit %q anywhere; a rolled-back binary "+
					"whose Config has no encryption field would crash-loop on it", token)
			}
		}
	})

	// Teeth. Without this, gf1EncryptionSurface could silently go stale — a key
	// renamed in the templates would leave the absence check above passing
	// against tokens the chart no longer emits, i.e. guarding nothing. Every
	// token must be reachable by turning encryption on.
	t.Run("every listed token actually renders when encryption is enabled", func(t *testing.T) {
		valuesPath := writeValues(t, "gf1-enc-surface.yaml", `encryption:
  enabled: true
storageClasses:
  - name: scale-nfs-encrypted
    enabled: true
    protocol: nfs
    encryptionSecretName: scale-nfs-encrypted
    encryptionSecretNamespace: ""
`)
		out := string(renderChart(t, chartDir(t), "-f", valuesPath))
		for _, token := range gf1EncryptionSurface {
			if !strings.Contains(out, token) {
				t.Errorf("gf1EncryptionSurface lists %q but the enabled render never emits it; "+
					"the token is stale and the default-off check above is guarding nothing", token)
			}
		}
	})
}

// TestChartGF1EncryptionConfigPlumbing guards the controller-wide gate render
// invariant: the default configmap carries NO encryption: block (byte-identical
// default, rolled-back strict parser stays happy) and the block renders only when
// encryption.enabled=true.
func TestChartGF1EncryptionConfigPlumbing(t *testing.T) {
	t.Run("default render omits the encryption block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "    encryption:") {
			t.Errorf("default configmap must not emit an encryption: block; encryption is opt-in and default-off")
		}
	})

	t.Run("enabled renders the block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "encryption.enabled=true")
		if !strings.Contains(out, "    encryption:\n      enabled: true\n") {
			t.Errorf("--set encryption.enabled=true did not render the encryption block; got:\n%s", out)
		}
	})

	t.Run("schema rejects an unknown encryption key", func(t *testing.T) {
		if out := helmTemplateExpectError(t, "--set", "encryption.bogus=true"); !strings.Contains(out, "encryption") {
			t.Errorf("schema did not reject an unknown encryption key; got:\n%s", out)
		}
	})
}

// TestChartGF1EncryptionStorageClassSecretRefs guards the per-StorageClass
// encryption plumbing. When a class sets encryptionSecretName the render emits
// the encryption parameter AND all three CSI secret refs (provisioner,
// controller-publish, node-stage), references the Secret by name/namespace only,
// and defaults the namespace to the release namespace. The controller-publish
// ref is the load-bearing one: the locked-volume reconciler resolves the
// passphrase through it.
func TestChartGF1EncryptionStorageClassSecretRefs(t *testing.T) {
	t.Run("default render emits no encryption parameter or secret refs", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml")
		for _, absent := range []string{
			"encryption:",
			"csi.storage.k8s.io/controller-publish-secret-name",
		} {
			if strings.Contains(out, absent) {
				t.Errorf("default StorageClass render must not emit %q; got:\n%s", absent, out)
			}
		}
	})

	t.Run("encryptionSecretName renders the parameter and all three secret refs", func(t *testing.T) {
		valuesPath := writeValues(t, "gf1-enc-sc.yaml", `storageClasses:
  - name: scale-nfs-encrypted
    enabled: true
    protocol: nfs
    encryptionSecretName: scale-nfs-encrypted
    encryptionSecretNamespace: ""
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		for _, want := range []string{
			`encryption: "true"`,
			"csi.storage.k8s.io/provisioner-secret-name: scale-nfs-encrypted",
			"csi.storage.k8s.io/provisioner-secret-namespace: default",
			"csi.storage.k8s.io/controller-publish-secret-name: scale-nfs-encrypted",
			"csi.storage.k8s.io/controller-publish-secret-namespace: default",
			"csi.storage.k8s.io/node-stage-secret-name: scale-nfs-encrypted",
			"csi.storage.k8s.io/node-stage-secret-namespace: default",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("rendered encrypted StorageClass missing %q; got:\n%s", want, out)
			}
		}
	})

	t.Run("explicit encryptionSecretNamespace is honored", func(t *testing.T) {
		valuesPath := writeValues(t, "gf1-enc-sc-ns.yaml", `storageClasses:
  - name: scale-nfs-encrypted
    enabled: true
    protocol: nfs
    encryptionSecretName: scale-nfs-encrypted
    encryptionSecretNamespace: kube-system
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		if !strings.Contains(out, "csi.storage.k8s.io/controller-publish-secret-namespace: kube-system") {
			t.Errorf("explicit encryptionSecretNamespace was not honored; got:\n%s", out)
		}
	})

	t.Run("encrypted render carries no credential material", func(t *testing.T) {
		valuesPath := writeValues(t, "gf1-enc-sc-creds.yaml", `storageClasses:
  - name: scale-nfs-encrypted
    enabled: true
    protocol: nfs
    encryptionSecretName: scale-nfs-encrypted
`)
		out := helmTemplate(t, "--show-only", "templates/storageclass.yaml", "-f", valuesPath)
		// The chart references the Secret by name/namespace only. None of the
		// Secret's data keys (the passphrase and its rotation/algorithm overrides)
		// may appear as rendered StorageClass parameters.
		for _, leaked := range []string{
			"passphrase",
			"passphrasePrevious",
			"pbkdf2iters",
			"AES-256-GCM",
		} {
			if strings.Contains(out, leaked) {
				t.Errorf("encrypted StorageClass render must not carry credential material %q; got:\n%s", leaked, out)
			}
		}
	})
}

// renderedConfigYAML extracts the driver config.yaml payload from a rendered
// ConfigMap manifest so it can be handed to the driver's strict loader.
func renderedConfigYAML(t *testing.T, rendered string) string {
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
	return configYAML
}

// loadRenderedConfig writes the rendered driver config.yaml to a temp file and
// runs it through the driver's real LoadConfig, which decodes with
// KnownFields(true). It is the strict-parse contract: a configmap key the driver
// Config struct does not have crash-loops the controller at startup, so any
// encryption key the chart renders must be a field the driver understands.
func loadRenderedConfig(t *testing.T, configYAML string) *driver.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(configYAML), 0o600); err != nil {
		t.Fatalf("write rendered config.yaml: %v", err)
	}
	cfg, err := driver.LoadConfig(path)
	if err != nil {
		t.Fatalf("driver.LoadConfig rejected the rendered config.yaml: %v\n--- config ---\n%s", err, configYAML)
	}
	return cfg
}

// requiredConfigArgs supplies the two values the driver validates as required
// (truenas.host, zfs.parentDataset) so LoadConfig reaches field validation
// rather than failing on a missing required field unrelated to encryption.
var requiredConfigArgs = []string{
	"--set", "truenas.host=nas01.example.invalid",
	"--set", "zfs.parentDataset=flashstor/csi",
}

// TestChartGF1RenderedConfigStrictParses extends the chart suite's config-parse
// guard to encryption: the rendered configmap strict-parses into driver.Config
// BOTH with encryption off (the default — the encryption field must stay absent
// so a rolled-back binary still parses it) and with encryption.enabled=true (the
// rendered encryption: block must be a field the current driver understands).
func TestChartGF1RenderedConfigStrictParses(t *testing.T) {
	t.Run("default render strict-parses with encryption off", func(t *testing.T) {
		rendered := helmTemplate(t, append([]string{"--show-only", "templates/configmap.yaml"}, requiredConfigArgs...)...)
		cfg := loadRenderedConfig(t, renderedConfigYAML(t, rendered))
		if cfg.Encryption.Enabled {
			t.Errorf("default render must leave Encryption.Enabled false; got true")
		}
	})

	t.Run("encryption.enabled=true strict-parses with encryption on", func(t *testing.T) {
		args := append(append([]string{"--show-only", "templates/configmap.yaml"}, requiredConfigArgs...),
			"--set", "encryption.enabled=true")
		rendered := helmTemplate(t, args...)
		cfg := loadRenderedConfig(t, renderedConfigYAML(t, rendered))
		if !cfg.Encryption.Enabled {
			t.Errorf("encryption.enabled=true did not survive the strict parse into driver.Config; got Enabled=false")
		}
	})
}
