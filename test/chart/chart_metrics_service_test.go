package chart

import (
	"testing"
)

// TestChartMetricsServicesPublishNotReadyAddresses is the regression test for
// C6: /metrics is served off the SAME mux/port as /readyz (pkg/driver/health.go),
// and the controller's readiness deliberately fails closed during a TrueNAS
// outage (see isReady). Without publishNotReadyAddresses: true on the metrics
// Service, the Endpoints controller drops the pod the instant that happens,
// taking the Prometheus scrape target down with the backend:
// scale_csi_truenas_connection_status then goes MISSING instead of reading 0,
// so ScaleCSITrueNASConnectionDown (5m) can never fire and the slower
// ScaleCSIControllerMetricsAbsent/ScaleCSIControllerDown (10m) mislabels a
// backend outage as a controller outage.
func TestChartMetricsServicesPublishNotReadyAddresses(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/metrics-service.yaml")
	manifests := decodeManifests(t, out)

	for _, name := range []string{"controller-metrics", "node-metrics"} {
		svc := findManifest(t, manifests, "Service", name)
		spec, ok := asManifest(svc["spec"])
		if !ok {
			t.Fatalf("%s Service has no spec", name)
		}
		publish, ok := spec["publishNotReadyAddresses"].(bool)
		if !ok || !publish {
			t.Errorf("%s Service must set publishNotReadyAddresses: true so /metrics stays scraped when the pod's readiness gate (TrueNAS connectivity) trips; got spec: %#v", name, spec)
		}
	}
}
