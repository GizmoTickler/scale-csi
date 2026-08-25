package chart

import (
	"fmt"
	"strings"
	"testing"
)

func TestDriverStartupConnectionWindowAndProbe(t *testing.T) {
	tests := []struct {
		template string
		kind     string
		name     string
	}{
		{template: "templates/controller-deployment.yaml", kind: "Deployment", name: "controller"},
		{template: "templates/node-daemonset.yaml", kind: "DaemonSet", name: "-node"},
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
			if !ok || httpGet["path"] != "/readyz" || fmt.Sprint(httpGet["port"]) != "9809" {
				t.Errorf("startupProbe must use driver readiness on port 9809: %#v", probe)
			}
			if fmt.Sprint(probe["periodSeconds"]) != "10" || fmt.Sprint(probe["failureThreshold"]) != "30" {
				t.Errorf("startupProbe window must match the default five-minute retry window: %#v", probe)
			}
		})
	}
}
