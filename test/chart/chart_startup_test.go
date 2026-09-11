package chart

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// probeEndpoint is an httpGet probe target reduced to the pair that decides
// WHAT a probe proves: which port it talks to and which path on it.
type probeEndpoint struct {
	path string
	port string
}

func (p probeEndpoint) String() string { return p.port + p.path }

// containerProbe extracts an httpGet probe from a container spec.
func containerProbe(t *testing.T, container manifest, name string) (manifest, probeEndpoint) {
	t.Helper()
	probe, ok := asManifest(container[name])
	if !ok {
		t.Fatalf("container has no %s", name)
	}
	httpGet, ok := asManifest(probe["httpGet"])
	if !ok {
		t.Fatalf("%s is not an httpGet probe: %#v", name, probe)
	}
	path, _ := httpGet["path"].(string)
	return probe, probeEndpoint{path: path, port: fmt.Sprint(httpGet["port"])}
}

func probeInt(t *testing.T, probe manifest, key string) int {
	t.Helper()
	v, ok := probe[key].(int)
	if !ok {
		t.Fatalf("probe field %s is not an integer: %#v", key, probe[key])
	}
	return v
}

// TestDriverStartupConnectionWindowAndProbe pins the startup-probe design for
// both workloads.
//
// The two workloads deliberately probe DIFFERENT endpoints, and the difference
// is load-bearing rather than an oversight. The controller's isReady() requires
// a live TrueNAS connection and, in strict fencing mode, stays false until
// startup fencing converges, so pointing its startup probe at /readyz turns
// "unready but still serving" into CrashLoopBackOff. The node plugin has no
// such dependency (its isReady() is true immediately), so /readyz there is
// correct and stays.
//
// The controller instead probes port 9808 — the liveness-probe sidecar — which
// is the only endpoint in that pod that answers 200 ONLY once the CSI socket is
// serving. See TestControllerStartupProbeGatesTheLivenessProbe for why the
// obvious-looking /healthz on the metrics port is not usable.
func TestDriverStartupConnectionWindowAndProbe(t *testing.T) {
	tests := []struct {
		template string
		kind     string
		name     string
		want     probeEndpoint
	}{
		{template: "templates/controller-deployment.yaml", kind: "Deployment", name: "controller", want: probeEndpoint{path: "/healthz", port: "9808"}},
		{template: "templates/node-daemonset.yaml", kind: "DaemonSet", name: "-node", want: probeEndpoint{path: "/readyz", port: "9809"}},
	}

	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			manifests := decodeManifests(t, helmTemplate(t, "--show-only", test.template))
			workload := findManifest(t, manifests, test.kind, test.name)
			container := workloadContainer(t, workload, "scale-csi")
			args := strings.Join(asStringSlice(container["args"]), "\n")
			if !strings.Contains(args, "-startup-connect-timeout=5m") {
				t.Errorf("default startup connection window did not render: %s", args)
			}

			probe, got := containerProbe(t, container, "startupProbe")
			if got != test.want {
				t.Errorf("startupProbe must hit %s, got %s: %#v", test.want, got, probe)
			}
			if probeInt(t, probe, "periodSeconds") != 10 || probeInt(t, probe, "failureThreshold") != 30 {
				t.Errorf("startupProbe window must match the default five-minute retry window: %#v", probe)
			}
		})
	}
}

// TestControllerStartupProbeGatesTheLivenessProbe is the regression test for
// the defect that neutered commit 86574e8's startup-retry feature outright.
//
// 86574e8 made the driver RETRY its initial TrueNAS connection for
// startupConnectTimeout (5m) instead of fataling, and cmd/scale-csi/startup.go
// stands up a temporary health server during that window so a startup probe has
// something to talk to. But that server maps /healthz to an UNCONDITIONAL 200,
// exactly as the driver's real health server does (pkg/driver/health.go), so
// /healthz on the metrics port answers from the moment the process binds the
// port — before the backend has been retried even once.
//
// The controller's startup probe used to point there. The consequences:
//
//  1. It passed at ~t=10s regardless of whether TrueNAS was reachable, so the
//     310s startup budget the manifest computed was never actually consumed.
//  2. Passing the startup probe ARMS the livenessProbe, which points at port
//     9808 — the liveness-probe sidecar, which needs /csi/csi.sock, which
//     Driver.Run() has not created yet. Five 10s failures later: restart at
//     ~70s.
//
// So any backend outage longer than ~70s still produced CrashLoopBackOff, which
// is the exact failure 86574e8 claims to fix.
//
// The invariant this test encodes: whatever the startup probe proves must be at
// least as strong as what the liveness probe demands. Probing the same endpoint
// is the simplest way to satisfy that, and it is what the chart does — passing
// startup means the CSI socket answered a Probe RPC, which is precisely what
// liveness goes on to watch.
func TestControllerStartupProbeGatesTheLivenessProbe(t *testing.T) {
	manifests := decodeManifests(t, helmTemplate(t, "--show-only", "templates/controller-deployment.yaml"))
	deployment := findManifest(t, manifests, "Deployment", "controller")
	container := workloadContainer(t, deployment, "scale-csi")

	_, startup := containerProbe(t, container, "startupProbe")
	_, liveness := containerProbe(t, container, "livenessProbe")

	if startup != liveness {
		t.Errorf("startupProbe (%s) and livenessProbe (%s) target different endpoints: "+
			"passing startup would arm a liveness probe whose condition startup never proved. "+
			"That is how the startup-retry window produced a restart at ~70s.", startup, liveness)
	}

	// The endpoint must be the one that means "the driver is SERVING", not the
	// one that means "a process is bound to the port". 9808 is the
	// liveness-probe sidecar: it refuses the connection until the CSI socket
	// exists and answers 200 only when a CSI Probe RPC succeeds.
	if startup.port == "9809" {
		t.Errorf("startupProbe targets the driver's own health port (%s); both the startup health "+
			"server and the driver health server return an unconditional 200 on /healthz there, so "+
			"the probe passes during the TrueNAS retry window and proves nothing", startup)
	}
	if startup.port != "9808" || startup.path != "/healthz" {
		t.Errorf("startupProbe = %s, want 9808/healthz (the liveness-probe sidecar)", startup)
	}
}

// TestControllerStartupProbeBudgetCoversStartupConnectTimeout proves the probe
// budget is DERIVED from startupConnectTimeout rather than hard-coded to a
// number that happens to work at the default.
//
// Now that the startup probe actually fails for the whole time the driver is
// retrying, the budget is load-bearing for the first time:
// initialDelaySeconds + failureThreshold*periodSeconds must exceed
// startupConnectTimeout, or the kubelet kills the container at exactly the
// moment its own retry path was about to succeed. A hard-coded 30 satisfies
// that at the 5m default (10 + 300 = 310 > 300) and silently fails at any
// larger configured window.
func TestControllerStartupProbeBudgetCoversStartupConnectTimeout(t *testing.T) {
	for _, timeout := range []string{"5m", "90s", "10m", "30m", "1h", "1h30m", "0s"} {
		t.Run(timeout, func(t *testing.T) {
			manifests := decodeManifests(t, helmTemplate(t,
				"--set", "startupConnectTimeout="+timeout,
				"--show-only", "templates/controller-deployment.yaml"))
			deployment := findManifest(t, manifests, "Deployment", "controller")
			container := workloadContainer(t, deployment, "scale-csi")

			args := strings.Join(asStringSlice(container["args"]), "\n")
			if !strings.Contains(args, "-startup-connect-timeout="+timeout) {
				t.Fatalf("startup connection window %q did not render: %s", timeout, args)
			}

			probe, _ := containerProbe(t, container, "startupProbe")
			initialDelay := probeInt(t, probe, "initialDelaySeconds")
			period := probeInt(t, probe, "periodSeconds")
			threshold := probeInt(t, probe, "failureThreshold")

			window, err := time.ParseDuration(timeout)
			if err != nil {
				t.Fatalf("bad test input %q: %v", timeout, err)
			}
			budget := time.Duration(initialDelay+threshold*period) * time.Second
			if budget <= window {
				t.Errorf("startupProbe budget %s (initialDelay %ds + %d*%ds) does not outlast "+
					"startupConnectTimeout %s; the kubelet restarts the container mid-retry",
					budget, initialDelay, threshold, period, window)
			}
		})
	}
}
