package chart

// GF-Sprint 1 (encryption at rest) chart assertions. Encryption ships
// default-off and opt-in per StorageClass, so the load-bearing invariant is that
// a deployment which never touches encryption renders BYTE-IDENTICALLY to the
// pre-encryption chart: any stray encryption key in the default render would
// change the manifest and crash-loop a rolled-back binary whose Config has no
// encryption field. The byte-identity test below asserts against the actual main
// render (built with `git archive main -- charts`), not a frozen copy, so it
// tracks whatever the pre-encryption chart renders.

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

// repoRoot resolves the repository root (two levels above test/chart).
func repoRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(chartDir(t), "..", "..")
}

// gf1ExemptManifests are the only manifests the default-off byte-identity guard
// is allowed to skip: the two workloads GF2 hardened. Encryption configuration
// never renders into either, so exempting them costs the guard nothing, while
// exempting anything else would.
var gf1ExemptManifests = []string{
	"templates/controller-deployment.yaml",
	"templates/node-daemonset.yaml",
}

// dropExemptManifests splits a multi-document helm render on its `# Source:`
// markers and removes the exempt manifests, leaving every other document — and
// their ordering — byte-comparable.
func dropExemptManifests(render []byte) []byte {
	docs := strings.Split(string(render), "---\n")
	kept := make([]string, 0, len(docs))
	for _, doc := range docs {
		exempt := false
		for _, name := range gf1ExemptManifests {
			if strings.Contains(doc, "# Source: scale-csi/"+name) {
				exempt = true
				break
			}
		}
		if !exempt {
			kept = append(kept, doc)
		}
	}
	return []byte(strings.Join(kept, "---\n"))
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

// TestChartGF1DefaultOffByteIdenticalToMain is the default-off guard. It builds
// the pre-encryption chart from the main branch (`git archive main -- charts`)
// into a temp dir, renders BOTH charts with default values, and asserts the
// renders are byte-identical.
//
// GF2 exempted exactly two manifests: controller-deployment.yaml and
// node-daemonset.yaml, whose sidecar securityContext and registrar probe are
// deliberate workload changes. Everything else — configmap.yaml, the bundled
// storageclass.yaml, volumesnapshotclass.yaml, secret.yaml, RBAC — is still
// compared in full, because those are where a stray encryption key would land.
// An earlier revision of this guard narrowed the comparison to configmap.yaml
// alone, which silently dropped the bundled StorageClass from coverage while the
// comment still claimed a secret ref on that class would be caught. Exempt by
// NAME, never by narrowing to a single file.
func TestChartGF1DefaultOffByteIdenticalToMain(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart template assertion")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH; skipping main-render byte-identity assertion")
	}

	root := repoRoot(t)
	// Confirm the main branch exists before shelling out, so a checkout without
	// it skips rather than fails opaquely.
	if out, err := exec.Command("git", "-C", root, "rev-parse", "--verify", "main").CombinedOutput(); err != nil {
		t.Skipf("main branch not resolvable; skipping byte-identity assertion: %v\n%s", err, out)
	}

	tmp := t.TempDir()
	archive, err := exec.Command("git", "-C", root, "archive", "main", "--", "charts").Output()
	if err != nil {
		t.Fatalf("git archive main -- charts failed: %v", err)
	}
	tarCmd := exec.Command("tar", "-x")
	tarCmd.Dir = tmp
	tarCmd.Stdin = bytes.NewReader(archive)
	if out, err := tarCmd.CombinedOutput(); err != nil {
		t.Fatalf("extract main charts archive: %v\n%s", err, out)
	}

	mainRender := dropExemptManifests(renderChart(t, filepath.Join(tmp, "charts", "scale-csi")))
	newRender := dropExemptManifests(renderChart(t, chartDir(t)))

	if !bytes.Equal(mainRender, newRender) {
		t.Errorf("default-off render is NOT byte-identical to the pre-encryption chart on main;\n"+
			"a stray encryption key renders by default. main=%d bytes, new=%d bytes",
			len(mainRender), len(newRender))
	}
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
