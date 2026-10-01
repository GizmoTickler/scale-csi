package chart

import (
	"reflect"
	"strings"
	"testing"
)

// node.implementation selects the node plugin binary in the one image. The
// default render is unchanged; rust swaps the container command and is refused
// where the Rust agent does not serve the install's protocols yet.

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

func TestChartNodeImplementationDefaultIsUnchanged(t *testing.T) {
	base := withArgs(rustNodeArgs[:4], nvmeofOnArgs...)
	if got, want := helmTemplate(t, withArgs(base, "--set", "node.implementation=go")...), helmTemplate(t, base...); got != want {
		t.Error("node.implementation=go must render exactly the default")
	}
	if _, set := nodePluginContainer(t, helmTemplate(t))["command"]; set {
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

func TestChartNodeImplementationRustRefusesWhatItDoesNotServe(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		reason string
	}{
		{"NFS on", withArgs(rustNodeArgs, "--set", "nfs.enabled=true"), "serves NVMe-oF only"},
		{"iSCSI on", withArgs(rustNodeArgs, "--set", "iscsi.enabled=true"), "serves NVMe-oF only"},
		{"kernel default data path", withArgs(rustNodeArgs, "--set", "nvmeof.dataPath=kernel"), "nvmeof.dataPath=ublk"},
		{"NVMe-oF off", withArgs(rustNodeArgs, "--set", "nvmeof.enabled=false"), "nvmeof.enabled=true"},
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
