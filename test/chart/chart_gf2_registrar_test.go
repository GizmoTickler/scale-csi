package chart

import (
	"strings"
	"testing"
)

func registrarProbe(t *testing.T, container manifest, name string) manifest {
	t.Helper()
	value, ok := asManifest(container[name])
	if !ok {
		t.Fatalf("container %q has no %s", container["name"], name)
	}
	return value
}

func registrarProbePort(t *testing.T, container manifest, probeName string) int {
	t.Helper()
	probeSpec := registrarProbe(t, container, probeName)
	httpGet, ok := asManifest(probeSpec["httpGet"])
	if !ok {
		t.Fatalf("container %q %s has no httpGet", container["name"], probeName)
	}
	port, ok := httpGet["port"].(int)
	if !ok {
		t.Fatalf("container %q %s port is not an integer: %#v", container["name"], probeName, httpGet["port"])
	}
	if path, ok := httpGet["path"].(string); !ok || path != "/healthz" {
		t.Errorf("container %q %s must probe /healthz; got %#v", container["name"], probeName, httpGet["path"])
	}
	return port
}

// TestChartGF2RegistrarHealthProbe guards the hostNetwork-safe registrar probe.
// The endpoint is enabled by default with an explicit, values-driven port, and
// disabling healthProbe removes the flag and both probes rather than leaving a
// node-wide listener behind.
func TestChartGF2RegistrarHealthProbe(t *testing.T) {
	t.Run("default endpoint and probes render on the registrar", func(t *testing.T) {
		manifests := decodeManifests(t, helmTemplate(t, "--show-only", "templates/node-daemonset.yaml"))
		registrar := workloadContainer(t, findManifest(t, manifests, "DaemonSet", "-node"), "csi-node-driver-registrar")
		args := asStringSlice(registrar["args"])
		if !strings.Contains(strings.Join(args, "\n"), "--http-endpoint=:9810") {
			t.Errorf("default registrar endpoint did not render: %#v", args)
		}
		if got := registrarProbePort(t, registrar, "livenessProbe"); got != 9810 {
			t.Errorf("default registrar liveness port = %d, want 9810", got)
		}
		if got := registrarProbePort(t, registrar, "readinessProbe"); got != 9810 {
			t.Errorf("default registrar readiness port = %d, want 9810", got)
		}
	})

	t.Run("custom port is used by endpoint and probes", func(t *testing.T) {
		valuesPath := writeValues(t, "gf2-registrar-health.yaml", `sidecars:
  nodeDriverRegistrar:
    healthProbe:
      enabled: true
      port: 19991
`)
		manifests := decodeManifests(t, helmTemplate(t, "--show-only", "templates/node-daemonset.yaml", "-f", valuesPath))
		registrar := workloadContainer(t, findManifest(t, manifests, "DaemonSet", "-node"), "csi-node-driver-registrar")
		args := asStringSlice(registrar["args"])
		if !strings.Contains(strings.Join(args, "\n"), "--http-endpoint=:19991") {
			t.Errorf("custom registrar endpoint did not render: %#v", args)
		}
		if got := registrarProbePort(t, registrar, "livenessProbe"); got != 19991 {
			t.Errorf("custom registrar liveness port = %d, want 19991", got)
		}
		if got := registrarProbePort(t, registrar, "readinessProbe"); got != 19991 {
			t.Errorf("custom registrar readiness port = %d, want 19991", got)
		}
	})

	t.Run("disabled endpoint removes listener and probes", func(t *testing.T) {
		manifests := decodeManifests(t, helmTemplate(t, "--show-only", "templates/node-daemonset.yaml", "--set", "sidecars.nodeDriverRegistrar.healthProbe.enabled=false"))
		registrar := workloadContainer(t, findManifest(t, manifests, "DaemonSet", "-node"), "csi-node-driver-registrar")
		if args := strings.Join(asStringSlice(registrar["args"]), "\n"); strings.Contains(args, "--http-endpoint") {
			t.Errorf("disabled registrar health endpoint still rendered: %s", args)
		}
		for _, name := range []string{"livenessProbe", "readinessProbe"} {
			if _, present := registrar[name]; present {
				t.Errorf("disabled registrar health endpoint still rendered %s", name)
			}
		}
	})

	t.Run("schema rejects an impossible host port", func(t *testing.T) {
		out := helmTemplateExpectError(t, "--set", "sidecars.nodeDriverRegistrar.healthProbe.port=65536")
		if !strings.Contains(out, "port") {
			t.Errorf("schema rejection did not mention registrar health port: %s", out)
		}
	})
}
