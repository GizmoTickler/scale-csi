package chart

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	// Blank import triggers the driver's metric registration so MetricNames()
	// returns the complete registered set. The drift comparison below is only
	// meaningful if registration has run.
	_ "github.com/GizmoTickler/scale-csi/pkg/driver"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

// scaleCSIMetricToken matches a fully-qualified scale_csi metric name as it
// appears in a PromQL expression, including any histogram _bucket/_sum/_count
// suffix (the suffix is stripped before lookup).
var scaleCSIMetricToken = regexp.MustCompile(`scale_csi_[a-z0-9_]+`)

// metricSuffixes are the Prometheus-generated suffixes a histogram/summary
// exposes on top of the registered base name. A panel naming
// scale_csi_operations_duration_seconds_bucket references the registered
// scale_csi_operations_duration_seconds.
var metricSuffixes = []string{"_bucket", "_sum", "_count"}

// TestChartMetricDrift forbids metric-name drift between the driver and the
// chart: every scale_csi_* metric a dashboard panel or PrometheusRule alert
// references MUST be registered by the driver (driver.MetricNames()). Deleting
// a metric still named by a panel, or adding a panel that names a typo'd /
// removed metric, fails here. The reverse is deliberately NOT enforced — a
// metric may be defined but not yet paneled.
func TestChartMetricDrift(t *testing.T) {
	registered := map[string]bool{}
	for _, name := range driver.MetricNames() {
		registered[name] = true
	}
	if len(registered) == 0 {
		t.Fatal("driver.MetricNames() returned no metrics; registration did not run")
	}

	lookup := func(token string) bool {
		if registered[token] {
			return true
		}
		for _, suffix := range metricSuffixes {
			if strings.HasSuffix(token, suffix) && registered[strings.TrimSuffix(token, suffix)] {
				return true
			}
		}
		return false
	}

	for _, template := range []string{
		"templates/grafana-dashboard.yaml",
		"templates/prometheusrule.yaml",
	} {
		t.Run(template, func(t *testing.T) {
			path := filepath.Join(chartDir(t), template)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read chart template %s: %v", template, err)
			}
			seen := map[string]bool{}
			for _, token := range scaleCSIMetricToken.FindAllString(string(raw), -1) {
				if seen[token] {
					continue
				}
				seen[token] = true
				if !lookup(token) {
					t.Errorf("%s references unknown metric %q (not in driver.MetricNames()); a panel/alert names a metric the driver does not register", template, token)
				}
			}
		})
	}
}

// TestChartDashboardJSONParses renders the Grafana dashboard ConfigMap (it is
// gated behind metrics.dashboards.enabled) and proves the appended panels did
// not break the inline JSON: the scale-csi.json block must json.Unmarshal.
func TestChartDashboardJSONParses(t *testing.T) {
	rendered := helmTemplate(t, "--show-only", "templates/grafana-dashboard.yaml", "--set", "metrics.dashboards.enabled=true")
	cm := findManifest(t, decodeManifests(t, rendered), "ConfigMap", "grafana-dashboard")

	data, ok := asManifest(cm["data"])
	if !ok {
		t.Fatalf("dashboard ConfigMap has no decodable data mapping")
	}
	jsonStr, ok := data["scale-csi.json"].(string)
	if !ok || jsonStr == "" {
		t.Fatalf("dashboard ConfigMap has no scale-csi.json data key")
	}

	var dashboard struct {
		Panels []struct {
			ID    int    `json:"id"`
			Title string `json:"title"`
		} `json:"panels"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &dashboard); err != nil {
		t.Fatalf("rendered scale-csi.json is not valid JSON: %v", err)
	}
	if len(dashboard.Panels) == 0 {
		t.Fatal("rendered dashboard has no panels")
	}

	// Panel ids must be unique or Grafana rejects the dashboard.
	ids := map[int]string{}
	for _, p := range dashboard.Panels {
		if prev, dup := ids[p.ID]; dup {
			t.Errorf("duplicate panel id %d (%q and %q)", p.ID, prev, p.Title)
		}
		ids[p.ID] = p.Title
	}
}

// TestChartPrometheusRuleRenders proves the PrometheusRule (gated behind
// metrics.prometheusRule.enabled) renders with the new alerts and that each new
// alert carries a runbook_url annotation (O18).
func TestChartPrometheusRuleRenders(t *testing.T) {
	rendered := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml", "--set", "metrics.prometheusRule.enabled=true")
	for _, alert := range []string{
		"ScaleCSIManualRecoveryTombstones",
		"ScaleCSIRemnantVolumesDetected",
		"ScaleCSITombstoneBacklog",
		"ScaleCSIFencingTakeoverSpike",
		"ScaleCSIFencingProvenanceOverflow",
		"ScaleCSIReconcileStalled",
		"ScaleCSIJobDispatcherUnsubscribed",
		"ScaleCSIDeleteResidualCleanupFailing",
	} {
		if !strings.Contains(rendered, "- alert: "+alert) {
			t.Errorf("prometheusrule render missing alert %q", alert)
		}
	}
	if got := strings.Count(rendered, "runbook_url:"); got < 8 {
		t.Errorf("expected at least 8 runbook_url annotations on the new alerts, got %d", got)
	}
}
