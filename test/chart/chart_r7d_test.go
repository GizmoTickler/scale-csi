package chart

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// controllerSecretRefParams are the CSI secret-ref StorageClass parameters that
// are resolved with the CONTROLLER ServiceAccount's credentials:
// provisioner-secret by external-provisioner (CreateVolume/DeleteVolume),
// controller-publish-secret by external-attacher, controller-expand-secret by
// external-resizer. node-stage / node-publish / node-expand secrets are
// resolved by the kubelet and arrive in the RPC already populated, so they need
// no rule in the controller ClusterRole.
var controllerSecretRefParams = regexp.MustCompile(`^csi\.storage\.k8s\.io/(provisioner|controller-publish|controller-expand)-secret-name$`)

// renderedClassesNeedControllerSecretGet walks every StorageClass the render
// actually produced and reports whether any of them names a controller-side CSI
// secret ref. This is deliberately computed FROM THE RENDER rather than
// restated as a fixture: a hand-written expectation is exactly what let the
// RBAC template and the StorageClass template disagree in the first place.
func renderedClassesNeedControllerSecretGet(t *testing.T, manifests []manifest) bool {
	t.Helper()
	for _, m := range manifests {
		if m["kind"] != "StorageClass" {
			continue
		}
		params, ok := asManifest(m["parameters"])
		if !ok {
			continue
		}
		for key := range params {
			if controllerSecretRefParams.MatchString(key) {
				return true
			}
		}
	}
	return false
}

// TestControllerSecretRBACMatchesRenderedStorageClasses renders BOTH templates
// from the same values and compares the derived sets, which is the only
// assertion that can catch the class of defect that put it here: the RBAC gate
// re-implemented "which StorageClasses exist and what do they reference" rather
// than deriving it, and its copy was wrong in both directions.
//
//   - Over-grant. storageclass.yaml REPLACES the class list when the legacy
//     singular .Values.storageClass is set; the RBAC copy ADDED the legacy entry
//     to the plural count. A plural class with chapSecretName that the legacy
//     form had already displaced still turned on cluster-wide `get` on every
//     Secret in the cluster, mounted into five sidecar containers, for a release
//     that renders no class needing it.
//
//   - Under-grant. The RBAC copy only knew chapSecretName/encryptionSecretName.
//     extraParameters passes through verbatim, so a class can name
//     csi.storage.k8s.io/provisioner-secret-name directly — and did, with no
//     secrets rule granted at all. Every PVC on that class stays Pending, and
//     the only evidence is a Forbidden line in the external-provisioner log.
func TestControllerSecretRBACMatchesRenderedStorageClasses(t *testing.T) {
	cases := []struct {
		name   string
		values string
		// want is the expectation a human can check by eye; the test ALSO
		// cross-checks it against what the StorageClass render actually
		// contains, so a wrong expectation here cannot make the test pass.
		want bool
	}{
		{
			name:   "default render needs nothing",
			values: "storageClasses: []\n",
			want:   false,
		},
		{
			name: "legacy singular form displaces a secret-bearing plural class",
			values: `storageClass:
  name: legacy-plain
  protocol: nfs
storageClasses:
  - name: plural-chap
    protocol: iscsi
    chapSecretName: chap-plural
`,
			want: false,
		},
		{
			name: "legacy create:false renders no class at all",
			values: `storageClass:
  create: false
storageClasses:
  - name: plural-enc
    protocol: iscsi
    encryptionSecretName: enc-plural
`,
			want: false,
		},
		{
			name: "a secret ref carried through extraParameters still needs the rule",
			values: `storageClasses:
  - name: sc-extra
    protocol: nfs
    extraParameters:
      csi.storage.k8s.io/provisioner-secret-name: byo-secret
      csi.storage.k8s.io/provisioner-secret-namespace: kube-system
`,
			want: true,
		},
		{
			name: "an enabled chapSecretName class still needs the rule",
			values: `storageClasses:
  - name: sc-chap
    protocol: iscsi
    chapSecretName: chap-a
`,
			want: true,
		},
		{
			name: "a DISABLED secret-bearing class needs nothing",
			values: `storageClasses:
  - name: sc-chap
    enabled: false
    protocol: iscsi
    chapSecretName: chap-a
`,
			want: false,
		},
		{
			name: "the legacy singular form with its own secret needs the rule",
			values: `storageClass:
  name: legacy-chap
  protocol: iscsi
  chapSecretName: chap-legacy
`,
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valuesPath := writeValues(t, "secret-rbac.yaml", tc.values)
			manifests := decodeManifests(t, helmTemplate(t, "-f", valuesPath))

			derived := renderedClassesNeedControllerSecretGet(t, manifests)
			if derived != tc.want {
				t.Fatalf("the rendered StorageClasses %s a controller-side secret ref, but the case "+
					"expects %v — fix the expectation, not the assertion below",
					map[bool]string{true: "DO carry", false: "carry NO"}[derived], tc.want)
			}

			role := findManifest(t, manifests, "ClusterRole", "scale-csi-controller")
			granted := roleTouchesResource(role, "secrets")
			if granted != derived {
				t.Errorf("controller ClusterRole grants secrets=%v but the rendered StorageClasses need it=%v; "+
					"the RBAC gate and the StorageClass template have drifted", granted, derived)
			}
		})
	}
}

// TestControllerSecretRBACStillHonorsEncryptionEnabled keeps the second OR
// alive: pkg/driver/reconcile_encryption.go's locked-volume reconciler reads a
// Secret named by LIVE StorageClass parameters whenever encryption is enabled,
// independent of chart values, so a GitOps split that manages StorageClasses
// out-of-band still needs the rule.
func TestControllerSecretRBACStillHonorsEncryptionEnabled(t *testing.T) {
	manifests := decodeManifests(t, helmTemplate(t, "--set", "encryption.enabled=true"))
	if renderedClassesNeedControllerSecretGet(t, manifests) {
		t.Fatal("this case is only meaningful when NO rendered class carries a secret ref")
	}
	role := findManifest(t, manifests, "ClusterRole", "scale-csi-controller")
	if !roleHasRule(role, []string{"secrets"}, []string{"get"}) {
		t.Error("encryption.enabled must turn the secrets get rule on even when no StorageClass declares a secret ref")
	}
}

// TestNVMeoFSubtreeDeletionStillRenders is the regression test for a
// render-time crash on an UPGRADE path. Setting a values subtree to null is
// Helm's documented way to delete it, and the schema accepts the deletion
// because none of these keys is "required" — but Helm's coalesce then removes
// the key outright, so a bare .Values.nvmeof.connect.fastIOFailTmo in the
// template is a nil-pointer dereference, not a validation error. The operator
// sees `helm upgrade` abort with a template stack trace.
func TestNVMeoFSubtreeDeletionStillRenders(t *testing.T) {
	cases := []struct {
		name   string
		values string
	}{
		{name: "connect deleted", values: "nvmeof:\n  enabled: true\n  connect: null\n"},
		{name: "portPerf deleted", values: "nvmeof:\n  enabled: true\n  portPerf: null\n"},
		{name: "both deleted", values: "nvmeof:\n  enabled: true\n  connect: null\n  portPerf: null\n"},
		{name: "the whole nvmeof subtree deleted", values: "nvmeof: null\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valuesPath := writeValues(t, "nvmeof-null.yaml", tc.values)
			// helmTemplate fails the test on a non-zero exit, which is the
			// assertion: a deleted subtree must render, not crash.
			out := helmTemplate(t, "-f", valuesPath, "--show-only", "templates/configmap.yaml")
			if !strings.Contains(out, "kind: ConfigMap") {
				t.Errorf("configmap did not render:\n%s", out)
			}
			// A deleted subtree must behave exactly like an unset one: the
			// connect block is emitted only for non-default values, so it must
			// be absent here.
			if strings.Contains(out, "fastIOFailTmo:") || strings.Contains(out, "keepAliveTmo:") {
				t.Errorf("a deleted nvmeof.connect subtree must render no connect keys:\n%s", out)
			}
		})
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test file path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// wsURLHost matches the host of a ws:// or wss:// URL.
var wsURLHost = regexp.MustCompile(`wss?://([A-Za-z0-9._-]+)`)

// documentationHost reports whether a hostname is a reserved documentation or
// loopback name rather than somebody's real appliance. RFC 2606 reserves
// .example/.invalid/.test and example.com/net/org for exactly this.
func documentationHost(host string) bool {
	host = strings.ToLower(host)
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	for _, suffix := range []string{".example.com", ".example.net", ".example.org", ".example", ".invalid", ".test", ".localhost"} {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return host == "example.com" || host == "example.net" || host == "example.org"
}

// TestDocsCarryNoRealApplianceIdentity guards a standing rule: no plaintext
// internal hostname may be committed. docs/reference/truenas-api-map.md and
// docs/reference/truenas-api-methods.json are GENERATED from a live appliance,
// and the generator wrote the real websocket URL and hostname straight into
// both. Anything regenerated from a live NAS has to be scrubbed on the way in,
// so this test exists to fail the next time it is not.
//
// The test names no real domain — encoding one here would simply move the leak.
// It asserts the positive form instead: every ws/wss host under docs/ must be
// an RFC 2606 documentation name.
func TestDocsCarryNoRealApplianceIdentity(t *testing.T) {
	docsDir := filepath.Join(repoRoot(t), "docs")

	t.Run("no websocket URL names a real host", func(t *testing.T) {
		err := filepath.Walk(docsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			content, readErr := os.ReadFile(path) //nolint:gosec // path comes from walking the repo's own docs tree
			if readErr != nil {
				return readErr
			}
			for _, match := range wsURLHost.FindAllStringSubmatch(string(content), -1) {
				if !documentationHost(match[1]) {
					t.Errorf("%s carries a websocket URL for a non-documentation host (%q); "+
						"generated API references must be scrubbed to an RFC 2606 name", path, match[1])
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk docs: %v", err)
		}
	})

	// The generated pair also records the appliance's own short hostname, which
	// no RFC 2606 rule can catch. Pin the scrubbed value directly.
	t.Run("the generated API reference records a placeholder hostname", func(t *testing.T) {
		const wantHostname = "truenas"

		jsonPath := filepath.Join(docsDir, "reference", "truenas-api-methods.json")
		raw, err := os.ReadFile(jsonPath)
		if err != nil {
			t.Fatalf("read %s: %v", jsonPath, err)
		}
		var doc struct {
			Source     string `json:"source"`
			SystemInfo struct {
				Hostname string `json:"hostname"`
			} `json:"system_info"`
		}
		// Also proves the programmatic scrub left the 9MB file valid JSON.
		if jsonErr := json.Unmarshal(raw, &doc); jsonErr != nil {
			t.Fatalf("%s is not valid JSON: %v", jsonPath, jsonErr)
		}
		if doc.SystemInfo.Hostname != wantHostname {
			t.Errorf("%s system_info.hostname = %q, want the %q placeholder", jsonPath, doc.SystemInfo.Hostname, wantHostname)
		}

		mdPath := filepath.Join(docsDir, "reference", "truenas-api-map.md")
		md, err := os.ReadFile(mdPath)
		if err != nil {
			t.Fatalf("read %s: %v", mdPath, err)
		}
		if !strings.Contains(string(md), "**Hostname:** `"+wantHostname+"`") {
			t.Errorf("%s does not record the %q hostname placeholder", mdPath, wantHostname)
		}
	})
}

// TestReleaseNotesNextTitlesThisRelease catches the stalest kind of release
// note: the "(next)" file still titled with the version that already shipped.
// v1.10.6, v1.11.0, v1.11.1 and v1.11.2 are tagged and have their own sections further
// down the same file.
func TestReleaseNotesNextTitlesThisRelease(t *testing.T) {
	path := filepath.Join(repoRoot(t), "docs", "release-notes-next.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	content := string(raw)
	lines := strings.SplitN(content, "\n", 2)
	title := lines[0]

	const wantVersion = "v1.12.0"
	if !strings.Contains(title, wantVersion) {
		t.Errorf("release notes title %q does not name this release (%s)", title, wantVersion)
	}
	// A version that already has a released section below cannot also be the
	// "next" one.
	for _, shipped := range []string{"v1.10.6", "v1.11.0", "v1.11.1", "v1.11.2"} {
		if strings.Contains(title, shipped) {
			t.Errorf("release notes title %q names %s, which is tagged and has its own section in this file", title, shipped)
		}
	}
	if !strings.Contains(content, "## "+wantVersion) {
		t.Errorf("release notes carry no %q section describing this release", "## "+wantVersion)
	}
}
