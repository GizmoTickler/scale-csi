package chart

import (
	"fmt"
	"strings"
	"testing"
)

func TestDriverStartupConnectionWindowAndProbe(t *testing.T) {
	// The two workloads deliberately probe DIFFERENT endpoints, and the
	// difference is load-bearing rather than an oversight.
	//
	// The controller's isReady() requires a live TrueNAS connection and, in
	// strict fencing mode, stays false until startup fencing converges. Pointing
	// its STARTUP probe at /readyz turned "unready but still serving" into
	// CrashLoopBackOff on a 10 + 30*10 = 310s budget — effectively the same
	// number as startupConnectTimeout (5m) — so a NAS maintenance window longer
	// than five minutes killed the container at exactly the moment its own retry
	// path was about to succeed. Readiness is already covered by the separate
	// readinessProbe; the startup probe must only prove the process is alive.
	//
	// The node plugin has no such dependency (its isReady() is true
	// immediately), so /readyz there is correct and stays.
	tests := []struct {
		template  string
		kind      string
		name      string
		probePath string
	}{
		{template: "templates/controller-deployment.yaml", kind: "Deployment", name: "controller", probePath: "/healthz"},
		{template: "templates/node-daemonset.yaml", kind: "DaemonSet", name: "-node", probePath: "/readyz"},
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

			probe, ok := asManifest(container["startupProbe"])
			if !ok {
				t.Fatal("scale-csi container has no startupProbe")
			}
			httpGet, ok := asManifest(probe["httpGet"])
			if !ok || httpGet["path"] != test.probePath || fmt.Sprint(httpGet["port"]) != "9809" {
				t.Errorf("startupProbe must hit %s on port 9809: %#v", test.probePath, probe)
			}
			if fmt.Sprint(probe["periodSeconds"]) != "10" || fmt.Sprint(probe["failureThreshold"]) != "30" {
				t.Errorf("startupProbe window must match the default five-minute retry window: %#v", probe)
			}
		})
	}
}
