package chart

import (
	"fmt"
	"math"
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
		// gatedOnBackend is true where the startup probe cannot pass until the
		// TrueNAS connection succeeds, i.e. where the probe budget has to
		// outlast startupConnectTimeout. Only the controller is: cmd/scale-csi
		// computes needsTrueNAS = runController || reconcileOnce, and
		// driver.NewDriver builds the management client only when RunController
		// is set, so a node-mode process never attempts a backend connection,
		// never enters the startup-retry loop, never stands up the temporary
		// startup health server, and answers /readyz as soon as the driver's
		// own health server binds. Its failureThreshold is therefore an
		// ordinary generous constant and is deliberately NOT derived.
		gatedOnBackend bool
	}{
		{template: "templates/controller-deployment.yaml", kind: "Deployment", name: "controller", want: probeEndpoint{path: "/healthz", port: "9808"}, gatedOnBackend: true},
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
			// Deliberately NOT a literal failureThreshold. The controller's is
			// derived from startupConnectTimeout, so pinning the number here
			// would just re-encode whatever the template happens to emit and
			// would pass on a wrong formula. Assert the property instead: the
			// probe must still be able to observe a success after the whole
			// retry window has elapsed. See
			// TestControllerStartupProbeBudgetCoversStartupConnectTimeout for
			// the full sweep and for why the kubelet's Nth failure lands at
			// initialDelay + (N-1)*period.
			initialDelay := probeInt(t, probe, "initialDelaySeconds")
			period := probeInt(t, probe, "periodSeconds")
			threshold := probeInt(t, probe, "failureThreshold")
			if period != 10 {
				t.Errorf("startupProbe periodSeconds must stay 10s: %#v", probe)
			}
			lastAttempt := time.Duration(initialDelay+(threshold-1)*period) * time.Second
			if test.gatedOnBackend && lastAttempt < 5*time.Minute+time.Duration(period)*time.Second {
				t.Errorf("startupProbe last attempt at %s does not clear the default five-minute "+
					"retry window plus a period of slack: %#v", lastAttempt, probe)
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
// number that happens to work at the default, AND that it is derived with the
// kubelet's real probe schedule rather than a convenient approximation of it.
//
// The approximation is the whole defect. The obvious model is
//
//	budget = initialDelay + failureThreshold*period
//
// and it is wrong by one period. probeWorker calls doProbe BEFORE its first
// ticker wait, and a tick that lands inside initialDelaySeconds returns "keep
// going" WITHOUT counting a failure, so attempts sit on the period grid and the
// Nth consecutive failure — the one that kills the container — happens at
//
//	lastAttempt = initialDelay + (failureThreshold-1)*period
//
// At the 5m default the historical failureThreshold 30 puts that at
// 10 + 29*10 = 300s: EXACTLY startupConnectTimeout, not the 310s the old
// comment asserted. Zero slack, and the slack is load-bearing — the CSI socket
// only appears AFTER the connect window closes, and the probe target still owes
// a grpc reconnect backoff, a driver-name round trip and a listener bind before
// it answers 200. A backend that recovered in the last second of its permitted
// window was killed anyway.
//
// The formula also floor-divided, so any window that was not a whole number of
// periods ("305s") got the same threshold as the period below it and the last
// attempt landed BACK INSIDE the window.
//
// Both properties are asserted below against the whole legal value space of
// startupConnectTimeout (values.schema.json allows the full Go duration
// grammar, including fractional segments and windows past 1e6 seconds), never
// against a literal threshold.
func TestControllerStartupProbeBudgetCoversStartupConnectTimeout(t *testing.T) {
	// postConnectTail is the work that must still complete AFTER the driver's
	// own connect window closes before the startup probe's target can answer
	// 200: one grpc reconnect backoff inside the liveness-probe sidecar
	// (upstream pins a one second maximum reconnect delay), the driver-name
	// round trip that its Connect performs, and the bind of its HTTP listener.
	// Three seconds is a generous ceiling on that.
	const postConnectTail = 3 * time.Second

	// The sweep spans the schema: sub-second units, the default, windows just
	// off a period boundary (which floor division truncated), fractional
	// segments, and windows at and beyond 1e6 seconds — where
	// durationToSecondsCeil used to render a float64 in exponent form, sprig's
	// int parsed it as 0, and the threshold silently collapsed to its floor.
	timeouts := []string{
		"0s", "1ns", "1us", "1µs", "500ms", "1s", "0.5s", "11s", "90s",
		"5m", "300s", "301s", "305s", "309s", "310s", "10m", "30m",
		"1h", "1h30m", "1.5h", "1h30.5m", "3599s", "3601s", "2h", "24h",
		"277h", "278h", "300h", "1000h",
		"999999s", "1000000s", "1000001s", "2000000s",
	}

	prevSeconds, prevThreshold := -1.0, 0
	for _, timeout := range timeouts {
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
			if period <= 0 || threshold <= 0 {
				t.Fatalf("nonsensical startupProbe schedule: %#v", probe)
			}

			parsed, err := time.ParseDuration(timeout)
			if err != nil {
				t.Fatalf("bad test input %q: %v", timeout, err)
			}
			window := time.Duration(math.Ceil(parsed.Seconds())) * time.Second

			// The kubelet's real schedule: the last attempt the container ever
			// gets, not a period later.
			lastAttempt := time.Duration(initialDelay+(threshold-1)*period) * time.Second

			// Primary property. The driver may connect at any instant up to
			// and including the end of the window, and probe attempts are
			// period-spaced, so an attempt must exist a full period past the
			// window for one to be guaranteed to land after the socket is
			// serving no matter where the window boundary falls on the grid.
			if want := window + time.Duration(period)*time.Second; lastAttempt < want {
				t.Errorf("last startup probe attempt is at %s (initialDelay %ds + (%d-1)*%ds); "+
					"startupConnectTimeout %s needs an attempt at or after %s, else the kubelet "+
					"kills the container at exactly the moment its own retry path was about to "+
					"succeed", lastAttempt, initialDelay, threshold, period, window, want)
			}

			// Physical restatement, independent of the period: whatever the
			// grid is, the budget has to cover the window plus the work the
			// probe target still owes after the connection succeeds.
			if want := window + postConnectTail; lastAttempt < want {
				t.Errorf("last startup probe attempt at %s leaves no room for the post-connect "+
					"tail (grpc reconnect backoff + driver-name round trip + listener bind) "+
					"after startupConnectTimeout %s; need at least %s", lastAttempt, window, want)
			}

			// Derived, not constant: a longer window must never yield a smaller
			// budget. The budget is quantized to whole periods, so equality is
			// legitimate for two windows inside the same period; growth across
			// a wider gap is asserted by
			// TestControllerStartupProbeBudgetScalesWithTheWindow.
			seconds := math.Ceil(parsed.Seconds())
			if seconds > prevSeconds && threshold < prevThreshold {
				t.Errorf("threshold %d for %s is smaller than %d for the shorter preceding "+
					"window; the budget is not derived from startupConnectTimeout",
					threshold, timeout, prevThreshold)
			}
			prevSeconds, prevThreshold = seconds, threshold
		})
	}
}

// TestControllerStartupProbeBudgetScalesWithTheWindow kills the cheat that
// TestControllerStartupProbeBudgetCoversStartupConnectTimeout cannot see on its
// own: a single enormous constant satisfies "the budget outlasts the window"
// for every value in a bounded sweep while being derived from nothing.
//
// The budget is a LINEAR function of startupConnectTimeout with slope 1, so
// doubling the window must push the last probe attempt out by the window's own
// length, give or take the period the threshold is quantized to.
func TestControllerStartupProbeBudgetScalesWithTheWindow(t *testing.T) {
	lastAttempt := func(timeout string) (time.Duration, int) {
		t.Helper()
		manifests := decodeManifests(t, helmTemplate(t,
			"--set", "startupConnectTimeout="+timeout,
			"--show-only", "templates/controller-deployment.yaml"))
		deployment := findManifest(t, manifests, "Deployment", "controller")
		container := workloadContainer(t, deployment, "scale-csi")
		probe, _ := containerProbe(t, container, "startupProbe")
		initialDelay := probeInt(t, probe, "initialDelaySeconds")
		period := probeInt(t, probe, "periodSeconds")
		threshold := probeInt(t, probe, "failureThreshold")
		return time.Duration(initialDelay+(threshold-1)*period) * time.Second, period
	}

	// Both windows are well past the `max 30` floor, so the floor cannot mask
	// a constant.
	for _, pair := range []struct{ small, large string }{
		{"10m", "20m"},
		{"1h", "2h"},
		{"300h", "600h"}, // past 1e6 seconds, where the helper used to collapse
	} {
		t.Run(pair.small+"->"+pair.large, func(t *testing.T) {
			small, period := lastAttempt(pair.small)
			large, _ := lastAttempt(pair.large)
			smallWindow, err := time.ParseDuration(pair.small)
			if err != nil {
				t.Fatalf("bad test input %q: %v", pair.small, err)
			}
			grew := large - small
			slack := time.Duration(period) * time.Second
			if grew < smallWindow-slack || grew > smallWindow+slack {
				t.Errorf("doubling startupConnectTimeout from %s to %s moved the last probe "+
					"attempt by %s (%s -> %s); a budget derived from the window would move it "+
					"by %s +/- one %s period. A constant moves it by 0.",
					pair.small, pair.large, grew, small, large, smallWindow, slack)
			}
		})
	}
}
