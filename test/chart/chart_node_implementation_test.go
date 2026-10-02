package chart

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// node.implementation selects the node plugin binary in the one image. The
// default render is unchanged; rust swaps the container command and is refused
// where the install enables no protocol the Rust agent serves.

var rustNodeArgs = []string{
	"--set", "nfs.enabled=false", "--set", "iscsi.enabled=false",
	"--set", "nvmeof.enabled=true", "--set", "nvmeof.subsystemAllowAnyHost=true",
	"--set", "nvmeof.dataPath=ublk",
}

func nodePluginContainer(t *testing.T, rendered string) manifest {
	t.Helper()
	for _, m := range decodeManifests(t, rendered) {
		meta, _ := asManifest(m["metadata"])
		if name, _ := meta["name"].(string); m["kind"] != "DaemonSet" || !strings.HasSuffix(name, "-node") {
			continue
		}
		container, ok := namedEntry(t, podSpecOf(t, m, "node DaemonSet")["containers"], "scale-csi")
		if !ok {
			t.Fatal("the node DaemonSet has no scale-csi container")
		}
		return container
	}
	t.Fatal("no node DaemonSet rendered")
	return nil
}

// The Rust node agent is the default since v1.21.0; node.implementation=go
// still selects the Go node plugin.
func TestChartNodeImplementationDefaultIsRust(t *testing.T) {
	base := withArgs(rustNodeArgs[:4], nvmeofOnArgs...)
	if got, want := helmTemplate(t, withArgs(base, "--set", "node.implementation=rust")...), helmTemplate(t, base...); got != want {
		t.Error("node.implementation=rust must render exactly the default")
	}
	if got := renderJSON(t, nodePluginContainer(t, helmTemplate(t))["command"]); got != `["/usr/local/bin/scale-csi-node"]` {
		t.Errorf("the default node plugin is the Rust agent, got command %s", got)
	}
	if _, set := nodePluginContainer(t, helmTemplate(t, "--set", "node.implementation=go"))["command"]; set {
		t.Error("the Go node plugin runs the image's entrypoint")
	}
}

func TestChartNodeImplementationRustRunsTheAgent(t *testing.T) {
	rendered := helmTemplate(t, withArgs(rustNodeArgs, "--set", "node.implementation=rust")...)
	container := nodePluginContainer(t, rendered)
	if got, want := container["command"], []any{"/usr/local/bin/scale-csi-node"}; !reflect.DeepEqual(got, want) {
		t.Errorf("command = %v, want %v", got, want)
	}
	goContainer := nodePluginContainer(t, helmTemplate(t, rustNodeArgs...))
	if !reflect.DeepEqual(container["args"], goContainer["args"]) {
		t.Errorf("the agent takes the Go node's flags unchanged:\nrust %v\ngo   %v", container["args"], goContainer["args"])
	}
}

// NFS is served, alone or beside NVMe-oF, for every node or as a canary.
func TestChartNodeImplementationRustServesNFS(t *testing.T) {
	nfsOnly := []string{"--set", "nfs.enabled=true", "--set", "iscsi.enabled=false", "--set", "nvmeof.enabled=false"}
	for name, args := range map[string][]string{
		"nfs only":           withArgs(nfsOnly, "--set", "node.implementation=rust"),
		"nfs and nvmeof":     withArgs(rustNodeArgs, "--set", "nfs.enabled=true", "--set", "node.implementation=rust"),
		"nfs only, a canary": withArgs(nfsOnly, "--set", "node.implementation=go", "--set", "node.rustNodes={k8s-2}"),
	} {
		t.Run(name, func(t *testing.T) {
			rendered := helmTemplate(t, args...)
			for setName, set := range nodeDaemonSets(t, rendered) {
				if strings.HasSuffix(setName, "-node-rust") || !strings.Contains(name, "canary") {
					container, _ := namedEntry(t, podSpecOf(t, set, setName)["containers"], "scale-csi")
					if got, want := container["command"], []any{"/usr/local/bin/scale-csi-node"}; !reflect.DeepEqual(got, want) {
						t.Errorf("%s: command = %v, want %v", setName, got, want)
					}
				}
			}
		})
	}
}

// The kernel initiator as the default data path is served.
func TestChartNodeImplementationRustServesTheKernelPath(t *testing.T) {
	helmTemplate(t, withArgs(rustNodeArgs, "--set", "nvmeof.dataPath=kernel", "--set", "node.implementation=rust")...)
}

func TestChartNodeImplementationRustRefusesWhatItDoesNotServe(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		reason string
	}{
		{"every protocol off", withArgs(rustNodeArgs, "--set", "nvmeof.enabled=false"), "needs a protocol"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := helmTemplateExpectError(t, withArgs(tc.args, "--set", "node.implementation=rust")...)
			if !strings.Contains(out, tc.reason) {
				t.Errorf("refusal does not say why (%q):\n%s", tc.reason, out)
			}
		})
	}
	if out := helmTemplateExpectError(t, "--set", "node.implementation=python"); !strings.Contains(out, "implementation") {
		t.Errorf("the schema must refuse an unknown implementation:\n%s", out)
	}
}

// iSCSI is served: with NVMe-oF, alone, and on canary nodes. Only NFS is
// still refused.
func TestChartNodeImplementationRustServesISCSI(t *testing.T) {
	for name, args := range map[string][]string{
		"with NVMe-oF": withArgs(rustNodeArgs, "--set", "iscsi.enabled=true", "--set", "node.implementation=rust"),
		"alone": withArgs(rustNodeArgs, "--set", "iscsi.enabled=true", "--set", "nvmeof.enabled=false",
			"--set", "node.implementation=rust"),
		"on canary nodes": withArgs(rustNodeArgs, "--set", "iscsi.enabled=true", "--set", "node.implementation=go", "--set", "node.rustNodes={k8s-2}"),
	} {
		t.Run(name, func(t *testing.T) {
			rendered := helmTemplate(t, args...)
			found := false
			for _, set := range nodeDaemonSets(t, rendered) {
				container, _ := namedEntry(t, podSpecOf(t, set, "node DaemonSet")["containers"], "scale-csi")
				if reflect.DeepEqual(container["command"], []any{"/usr/local/bin/scale-csi-node"}) {
					found = true
					mounts := renderJSON(t, container["volumeMounts"])
					for _, path := range []string{`"/etc/iscsi"`, `"/var/lib/iscsi"`, `"/host"`} {
						if !strings.Contains(mounts, path) {
							t.Errorf("the agent needs %s for iSCSI: %s", path, mounts)
						}
					}
				}
			}
			if !found {
				t.Error("no DaemonSet runs the Rust agent")
			}
		})
	}
	// Every protocol at once is served too.
	helmTemplate(t, withArgs(rustNodeArgs, "--set", "iscsi.enabled=true", "--set", "nfs.enabled=true",
		"--set", "node.implementation=rust")...)
}

// nodeDaemonSets returns the rendered node plugin DaemonSets by name.
func nodeDaemonSets(t *testing.T, rendered string) map[string]manifest {
	t.Helper()
	out := map[string]manifest{}
	for _, m := range decodeManifests(t, rendered) {
		meta, _ := asManifest(m["metadata"])
		name, _ := meta["name"].(string)
		if m["kind"] == "DaemonSet" && (strings.HasSuffix(name, "-node") || strings.HasSuffix(name, "-node-rust")) {
			out[name] = m
		}
	}
	return out
}

// node.rustNodes canaries the Rust agent: a second DaemonSet on just those
// nodes, which the Go DaemonSet avoids; the two never select each other's
// pods' scheduling, and nvmeublkd keeps running everywhere.
func TestChartNodeRustNodesCanariesTheAgent(t *testing.T) {
	rendered := helmTemplate(t, withArgs(rustNodeArgs, "--set", "node.implementation=go", "--set", "node.rustNodes={k8s-2}")...)
	sets := nodeDaemonSets(t, rendered)
	goSet, rustSet := sets["scale-csi-node"], sets["scale-csi-node-rust"]
	if goSet == nil || rustSet == nil || len(sets) != 2 {
		t.Fatalf("want the Go and the Rust node DaemonSets, got %v", reflect.ValueOf(sets).MapKeys())
	}
	affinity := func(m manifest) string {
		spec := podSpecOf(t, m, "node DaemonSet")
		node, _ := asManifest(spec["affinity"])
		return renderJSON(t, node)
	}
	if got := affinity(goSet); !strings.Contains(got, `"NotIn"`) || !strings.Contains(got, `"k8s-2"`) {
		t.Errorf("the Go DaemonSet must avoid the canary node: %s", got)
	}
	if got := affinity(rustSet); !strings.Contains(got, `"In"`) || !strings.Contains(got, `"k8s-2"`) {
		t.Errorf("the Rust DaemonSet must run only on the canary node: %s", got)
	}
	rustContainer, _ := namedEntry(t, podSpecOf(t, rustSet, "rust node DaemonSet")["containers"], "scale-csi")
	if got, want := rustContainer["command"], []any{"/usr/local/bin/scale-csi-node"}; !reflect.DeepEqual(got, want) {
		t.Errorf("rust command = %v, want %v", got, want)
	}
	goContainer, _ := namedEntry(t, podSpecOf(t, goSet, "node DaemonSet")["containers"], "scale-csi")
	if _, set := goContainer["command"]; set {
		t.Error("the Go DaemonSet keeps the image's entrypoint")
	}
	if !strings.Contains(renderJSON(t, rustSet["spec"]), `"scale-csi.io/node-implementation":"rust"`) {
		t.Error("the Rust DaemonSet selects its pods by the implementation label")
	}
	if strings.Contains(renderJSON(t, goSet["spec"]), "node-implementation") {
		t.Error("the Go DaemonSet's selector is unchanged (it is immutable)")
	}
	for _, m := range decodeManifests(t, rendered) {
		meta, _ := asManifest(m["metadata"])
		if name, _ := meta["name"].(string); m["kind"] == "DaemonSet" && strings.HasSuffix(name, "-nvmeublkd") {
			if _, set := podSpecOf(t, m, "nvmeublkd")["affinity"]; set {
				t.Error("nvmeublkd keeps running on every node, the canary node included")
			}
		}
	}

	for name, args := range map[string][]string{
		"with node.implementation=rust": withArgs(rustNodeArgs, "--set", "node.rustNodes={k8s-2}", "--set", "node.implementation=rust"),
		"with node.affinity":            withArgs(rustNodeArgs, "--set", "node.implementation=go", "--set", "node.rustNodes={k8s-2}", "--set", "node.affinity.podAntiAffinity.x=y"),
	} {
		t.Run("refused "+name, func(t *testing.T) { helmTemplateExpectError(t, args...) })
	}
}

func renderJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The Rust agent records Kubernetes Events with the node ServiceAccount: it
// creates them and patches a repeat's count, so the node ClusterRole must
// grant both, and the pod must get its ServiceAccount token.
func TestChartRustNodeMayRecordEvents(t *testing.T) {
	rendered := helmTemplate(t, withArgs(rustNodeArgs, "--set", "node.implementation=rust")...)
	manifests := decodeManifests(t, rendered)
	role := findManifest(t, manifests, "ClusterRole", "-node")
	granted := map[string]bool{}
	rules, _ := role["rules"].([]any)
	for _, r := range rules {
		rule, ok := asManifest(r)
		if !ok || !equalStrings(asStringSlice(rule["apiGroups"]), []string{""}) {
			continue
		}
		for _, res := range asStringSlice(rule["resources"]) {
			if res != "events" {
				continue
			}
			for _, verb := range asStringSlice(rule["verbs"]) {
				granted[verb] = true
			}
		}
	}
	for _, verb := range []string{"create", "patch"} {
		if !granted[verb] {
			t.Errorf("the node ClusterRole must grant %s on core events (granted: %v)", verb, granted)
		}
	}
	daemonSet := findManifest(t, manifests, "DaemonSet", "-node")
	if automount, set := podSpecOf(t, daemonSet, "node DaemonSet")["automountServiceAccountToken"]; set && automount != true {
		t.Errorf("the node pod must mount its ServiceAccount token, got automountServiceAccountToken=%v", automount)
	}
}
