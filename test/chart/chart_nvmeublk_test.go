package chart

import (
	"reflect"
	"strings"
	"testing"
)

// The userspace NVMe/TCP data path (nvmeof.dataPath / nvmeof.ublk.*) is
// strictly opt-in. These tests pin that a render which does not use it is
// byte-identical to one that never heard of it, and that using it plumbs the
// driver config, the node plugin's /run/nvmeublk mount and, separately, the
// optional nvmeublkd DaemonSet.

var nvmeofOnArgs = []string{"--set", "nvmeof.enabled=true", "--set", "nvmeof.subsystemAllowAnyHost=true"}

func withArgs(base []string, extra ...string) []string {
	return append(append([]string(nil), base...), extra...)
}

func podSpecOf(t *testing.T, workload manifest, name string) manifest {
	t.Helper()
	spec, ok := asManifest(workload["spec"])
	if !ok {
		t.Fatalf("%s has no spec", name)
	}
	template, ok := asManifest(spec["template"])
	if !ok {
		t.Fatalf("%s has no pod template", name)
	}
	podSpec, ok := asManifest(template["spec"])
	if !ok {
		t.Fatalf("%s pod has no spec", name)
	}
	return podSpec
}

func namedEntry(t *testing.T, list any, name string) (manifest, bool) {
	t.Helper()
	items, _ := list.([]any)
	for _, item := range items {
		entry, ok := asManifest(item)
		if ok && entry["name"] == name {
			return entry, true
		}
	}
	return nil, false
}

func hasManifest(manifests []manifest, kind, nameSubstr string) bool {
	for _, m := range manifests {
		meta, _ := asManifest(m["metadata"])
		name, _ := meta["name"].(string)
		if m["kind"] == kind && strings.Contains(name, nameSubstr) {
			return true
		}
	}
	return false
}

func TestChartNVMeUblkOffRendersNothing(t *testing.T) {
	cases := []struct {
		name string
		base []string
	}{
		{name: "default install", base: nil},
		{name: "NVMe-oF enabled", base: nvmeofOnArgs},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := helmTemplate(t, tc.base...)
			for _, absent := range []string{"nvmeublk", "dataPath", "ublk"} {
				if strings.Contains(baseline, absent) {
					t.Errorf("a render without the ublk data path must not mention %q", absent)
				}
			}
			// Every explicit "off" spelling, and ublk tunables set while ublk
			// is not in use, must render byte-for-byte the same manifests.
			explicitOff := helmTemplate(t, withArgs(tc.base,
				"--set", "nvmeof.dataPath=kernel",
				"--set", "nvmeof.ublk.enabled=false",
				"--set", "nvmeof.ublk.queues=4",
				"--set", "nvmeof.ublk.zeroCopy=false",
				"--set", "nvmeof.ublk.daemon.enabled=false",
				"--set", "nvmeof.ublk.daemon.image.tag=v1",
			)...)
			if explicitOff != baseline {
				t.Errorf("explicitly disabled ublk settings changed the render")
			}
		})
	}

	t.Run("ublk selected while NVMe-oF is disabled", func(t *testing.T) {
		if helmTemplate(t, "--set", "nvmeof.ublk.enabled=true", "--set", "nvmeof.dataPath=ublk") != helmTemplate(t) {
			t.Errorf("ublk settings must render nothing while nvmeof.enabled=false")
		}
	})
}

func TestChartNVMeUblkInUsePlumbsConfigAndNodeMount(t *testing.T) {
	cases := []struct {
		name         string
		args         []string
		wantDataPath string
		wantEnabled  bool
	}{
		{name: "StorageClass opt-in", args: []string{"--set", "nvmeof.ublk.enabled=true"}, wantDataPath: "kernel", wantEnabled: true},
		{name: "install default", args: []string{"--set", "nvmeof.dataPath=ublk"}, wantDataPath: "ublk", wantEnabled: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := withArgs(withArgs(nvmeofOnArgs, requiredConfigArgs...), tc.args...)
			args = withArgs(args, "--set", "nvmeof.ublk.queues=4", "--set", "nvmeof.ublk.depth=128",
				"--set", "nvmeof.ublk.zeroCopy=false", "--set", "nvmeof.ublk.napiUs=200", "--set", "nvmeof.ublk.attachTimeout=90")
			cfg := loadRenderedConfig(t, renderedConfigYAML(t, helmTemplate(t, withArgs(args, "--show-only", "templates/configmap.yaml")...)))
			ublk := cfg.NVMeoF.Ublk
			if cfg.NVMeoF.DataPath != tc.wantDataPath || ublk.Enabled != tc.wantEnabled {
				t.Errorf("dataPath=%q enabled=%t, want %q %t", cfg.NVMeoF.DataPath, ublk.Enabled, tc.wantDataPath, tc.wantEnabled)
			}
			if ublk.Queues != 4 || ublk.Depth != 128 || ublk.ZeroCopy == nil || *ublk.ZeroCopy || ublk.NapiUs != 200 || ublk.AttachTimeout != 90 {
				t.Errorf("ublk tunables did not reach the driver config: %+v", ublk)
			}
			if ublk.SocketPath != "/run/nvmeublk/nvmeublkd.sock" {
				t.Errorf("socket path = %q; the chart mounts /run/nvmeublk, so the driver default must stand", ublk.SocketPath)
			}

			manifests := decodeManifests(t, helmTemplate(t, args...))
			podSpec := podSpecOf(t, findManifest(t, manifests, "DaemonSet", "-node"), "node DaemonSet")
			volume, ok := namedEntry(t, podSpec["volumes"], "nvmeublk-run")
			if !ok {
				t.Fatal("node DaemonSet has no nvmeublk-run volume")
			}
			hostPath, _ := asManifest(volume["hostPath"])
			if hostPath["path"] != "/run/nvmeublk" || hostPath["type"] != "DirectoryOrCreate" {
				t.Errorf("nvmeublk-run hostPath = %v, want /run/nvmeublk DirectoryOrCreate", hostPath)
			}
			driver, ok := namedEntry(t, podSpec["containers"], "scale-csi")
			if !ok {
				t.Fatal("node DaemonSet has no scale-csi container")
			}
			mount, ok := namedEntry(t, driver["volumeMounts"], "nvmeublk-run")
			if !ok || mount["mountPath"] != "/run/nvmeublk" {
				t.Errorf("node plugin must mount nvmeublk-run at /run/nvmeublk; got %v", mount)
			}
			if hasManifest(manifests, "DaemonSet", "-nvmeublkd") {
				t.Errorf("the nvmeublkd DaemonSet is a separate opt-in and must not render here")
			}
		})
	}

	t.Run("defaults reach the driver config", func(t *testing.T) {
		args := withArgs(withArgs(nvmeofOnArgs, requiredConfigArgs...), "--set", "nvmeof.ublk.enabled=true", "--show-only", "templates/configmap.yaml")
		cfg := loadRenderedConfig(t, renderedConfigYAML(t, helmTemplate(t, args...)))
		ublk := cfg.NVMeoF.Ublk
		if ublk.Queues != 2 || ublk.Depth != 64 || ublk.ZeroCopy == nil || !*ublk.ZeroCopy || ublk.NapiUs != 0 || ublk.AttachTimeout != 60 {
			t.Errorf("ublk defaults changed on the way to the driver: %+v", ublk)
		}
	})
}

func TestChartNVMeUblkDaemonSet(t *testing.T) {
	args := withArgs(nvmeofOnArgs,
		"--set", "nvmeof.ublk.enabled=true",
		"--set", "nvmeof.ublk.daemon.enabled=true",
		"--set", "nvmeof.ublk.daemon.image.tag=v0.1.0",
		"--set", "node.tolerations[0].key=storage-only",
		"--set", "node.tolerations[0].operator=Exists",
		"--set", "node.tolerations[0].effect=NoSchedule",
	)
	manifests := decodeManifests(t, helmTemplate(t, args...))
	daemon := findManifest(t, manifests, "DaemonSet", "-nvmeublkd")
	node := findManifest(t, manifests, "DaemonSet", "-node")
	podSpec := podSpecOf(t, daemon, "nvmeublkd DaemonSet")
	nodePodSpec := podSpecOf(t, node, "node DaemonSet")

	if podSpec["hostNetwork"] != true {
		t.Errorf("nvmeublkd must run with hostNetwork")
	}
	if podSpec["priorityClassName"] != "system-node-critical" {
		t.Errorf("priorityClassName = %v, want system-node-critical", podSpec["priorityClassName"])
	}
	if grace, _ := podSpec["terminationGracePeriodSeconds"].(int); grace <= 5 {
		t.Errorf("terminationGracePeriodSeconds = %v, must exceed the daemon's 5 s drain", podSpec["terminationGracePeriodSeconds"])
	}
	for _, key := range []string{"tolerations", "nodeSelector", "affinity"} {
		if !reflect.DeepEqual(podSpec[key], nodePodSpec[key]) {
			t.Errorf("nvmeublkd %s = %v, want the node plugin's %v so every staging node has a daemon", key, podSpec[key], nodePodSpec[key])
		}
	}

	spec, _ := asManifest(daemon["spec"])
	strategy, _ := asManifest(spec["updateStrategy"])
	rolling, _ := asManifest(strategy["rollingUpdate"])
	if strategy["type"] != "RollingUpdate" || rolling["maxUnavailable"] != 1 || rolling["maxSurge"] != 0 {
		t.Errorf("updateStrategy = %v, want RollingUpdate maxUnavailable 1 maxSurge 0", strategy)
	}
	selector, _ := asManifest(spec["selector"])
	nodeSpec, _ := asManifest(node["spec"])
	nodeSelector, _ := asManifest(nodeSpec["selector"])
	if reflect.DeepEqual(selector, nodeSelector) {
		t.Errorf("nvmeublkd and the node plugin must not share a pod selector")
	}

	container, ok := namedEntry(t, podSpec["containers"], "nvmeublkd")
	if !ok {
		t.Fatal("nvmeublkd DaemonSet has no nvmeublkd container")
	}
	if got := asStringSlice(container["command"]); !reflect.DeepEqual(got, []string{"nvmeublk", "daemon"}) {
		t.Errorf("command = %v, want [nvmeublk daemon]", got)
	}
	if container["image"] != "registry.example.invalid/nvmeublk:v0.1.0" {
		t.Errorf("image = %v", container["image"])
	}
	securityContext, _ := asManifest(container["securityContext"])
	if securityContext["privileged"] != true {
		t.Errorf("nvmeublkd must be privileged (ublk control, io_uring, mlockall)")
	}
	for name, want := range map[string]string{"host-dev": "/dev", "nvmeublk-run": "/run/nvmeublk"} {
		mount, ok := namedEntry(t, container["volumeMounts"], name)
		if !ok || mount["mountPath"] != want {
			t.Errorf("nvmeublkd mount %s = %v, want %s", name, mount, want)
		}
		volume, ok := namedEntry(t, podSpec["volumes"], name)
		hostPath, _ := asManifest(volume["hostPath"])
		if !ok || hostPath["path"] != want {
			t.Errorf("nvmeublkd volume %s = %v, want hostPath %s", name, volume, want)
		}
	}
	runVolume, _ := namedEntry(t, podSpec["volumes"], "nvmeublk-run")
	if hostPath, _ := asManifest(runVolume["hostPath"]); hostPath["type"] != "DirectoryOrCreate" {
		t.Errorf("/run/nvmeublk must be DirectoryOrCreate; got %v", hostPath["type"])
	}

	t.Run("digest wins over tag", func(t *testing.T) {
		const digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
		out := helmTemplate(t, withArgs(args, "--set", "nvmeof.ublk.daemon.image.digest="+digest)...)
		if !strings.Contains(out, `image: "registry.example.invalid/nvmeublk@`+digest+`"`) {
			t.Errorf("digest did not take precedence over tag")
		}
	})

	t.Run("an image tag or digest is required", func(t *testing.T) {
		out := helmTemplateExpectError(t, withArgs(nvmeofOnArgs, "--set", "nvmeof.ublk.enabled=true", "--set", "nvmeof.ublk.daemon.enabled=true")...)
		if !strings.Contains(out, "nvmeof.ublk.daemon.image.tag (or digest) is required") {
			t.Errorf("missing image tag must fail with a clear message; got:\n%s", out)
		}
	})
}

// The daemon only serves the ublk data path; enabling it without that path
// (or without NVMe-oF) must fail the render rather than run an unreachable
// privileged host-network DaemonSet.
func TestChartNVMeUblkDaemonRequiresUblkInUse(t *testing.T) {
	for name, args := range map[string][]string{
		"ublk not in use": withArgs(nvmeofOnArgs, "--set", "nvmeof.ublk.daemon.enabled=true", "--set", "nvmeof.ublk.daemon.image.tag=v0.1.0"),
		"nvmeof disabled": {"--set", "nvmeof.ublk.enabled=true", "--set", "nvmeof.ublk.daemon.enabled=true", "--set", "nvmeof.ublk.daemon.image.tag=v0.1.0"},
	} {
		t.Run(name, func(t *testing.T) {
			out := helmTemplateExpectError(t, args...)
			if !strings.Contains(out, "nvmeof.ublk.daemon.enabled requires the ublk data path in use") {
				t.Errorf("expected a clear render failure; got:\n%s", out)
			}
		})
	}
}

func TestChartNVMeUblkSchemaRejectsInvalidValues(t *testing.T) {
	cases := [][]string{
		{"--set", "nvmeof.dataPath=spdk"},
		{"--set", "nvmeof.ublk.queues=0"},
		{"--set", "nvmeof.ublk.depth=8192"},
		{"--set", "nvmeof.ublk.napiUs=-1"},
		{"--set", "nvmeof.ublk.attachTimeout=0"},
		{"--set", "nvmeof.ublk.daemon.terminationGracePeriodSeconds=1"},
		{"--set", "nvmeof.ublk.bogus=true"},
	}
	for _, args := range cases {
		out := helmTemplateExpectError(t, args...)
		if !strings.Contains(out, "values don't meet the specifications of the schema") && !strings.Contains(out, "schema") {
			t.Errorf("%v must be rejected by the values schema; got:\n%s", args, out)
		}
	}
}

// Deleting a subtree with null is Helm's documented way to drop it; the
// templates must treat it as unset rather than crash.
func TestChartNVMeUblkSubtreeDeletionStillRenders(t *testing.T) {
	cases := []string{
		"nvmeof:\n  enabled: true\n  subsystemAllowAnyHost: true\n  ublk: null\n",
		"nvmeof:\n  enabled: true\n  subsystemAllowAnyHost: true\n  ublk:\n    enabled: true\n    daemon: null\n",
		"nvmeof:\n  enabled: true\n  subsystemAllowAnyHost: true\n  dataPath: ublk\n  ublk:\n    zeroCopy: null\n",
	}
	for i, values := range cases {
		out := helmTemplate(t, "-f", writeValues(t, "ublk-null.yaml", values))
		if !strings.Contains(out, "kind: ConfigMap") {
			t.Errorf("case %d did not render", i)
		}
		if i == 2 && !strings.Contains(out, "zeroCopy: true") {
			t.Errorf("a deleted zeroCopy must render its default (true)")
		}
	}
}
