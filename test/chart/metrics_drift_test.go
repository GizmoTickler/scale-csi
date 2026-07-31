package chart

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	// Blank import triggers the driver's metric registration so MetricNames()
	// returns the complete registered set. The drift comparison below is only
	// meaningful if registration has run.
	_ "github.com/GizmoTickler/scale-csi/pkg/driver"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

// scaleCSIMetricRef matches a fully-qualified scale_csi metric OR recording-rule
// name as it appears in a PromQL expression. Prometheus metric names allow
// [a-zA-Z_:][a-zA-Z0-9_:]*; anchoring on the driver's scale_csi prefix means
// PromQL function names (rate, sum, histogram_quantile, ...) and label names are
// never mistaken for metrics, while colon-bearing recording-rule names are
// captured WHOLE rather than truncated at the first colon (codex M1).
var scaleCSIMetricRef = regexp.MustCompile(`scale_csi[a-zA-Z0-9_:]*`)

// generatedSuffixes are the series Prometheus exposes on top of a
// histogram/summary base. They are legitimate ONLY when the base is a
// histogram/summary — a gauge or counter named `..._count` does not exist.
var generatedSuffixes = []string{"_bucket", "_sum", "_count"}

// validateMetricRef checks a single scale_csi token extracted from an
// expression. registered is the driver's registered base names; histograms is
// the subset that are histograms/summaries (and so legitimately expose
// _bucket/_sum/_count); recordingRules is the set of chart-defined
// recording-rule output names. It returns a descriptive error when the token
// names something that does not exist.
func validateMetricRef(token string, registered, histograms, recordingRules map[string]bool) error {
	// A chart-defined recording-rule output is valid whether or not it follows
	// the colon naming convention.
	if recordingRules[token] {
		return nil
	}
	// Any other colon-bearing name is a recording rule the chart does not
	// define; the driver never registers colon names.
	if strings.Contains(token, ":") {
		return fmt.Errorf("references recording rule %q that the chart does not define", token)
	}
	if registered[token] {
		return nil
	}
	for _, suffix := range generatedSuffixes {
		if base, ok := strings.CutSuffix(token, suffix); ok {
			if histograms[base] {
				return nil
			}
			return fmt.Errorf("uses generated suffix %q on %q, but base %q is not a histogram/summary", suffix, token, base)
		}
	}
	return fmt.Errorf("references unknown metric %q (not registered by the driver)", token)
}

func metricSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

// TestChartMetricDrift forbids metric-name drift between the driver and the
// chart: every scale_csi_* metric a dashboard panel or PrometheusRule alert
// EXPRESSION references MUST be registered by the driver (or be a chart-defined
// recording-rule output). Deleting a metric still named by a panel, or adding a
// panel that names a typo'd / removed metric, fails here. The reverse is
// deliberately NOT enforced — a metric may be defined but not yet paneled.
//
// Unlike a raw byte scan, this renders both templates and extracts the actual
// expr fields, so comments, descriptions, and recording-rule output names are
// not mistaken for driver metric references; it retains full colon-bearing
// names; and it permits _bucket/_sum/_count only on histogram bases (codex M1).
// A full PromQL parser is deliberately NOT used — it would require pulling in
// the Prometheus server module; the namespace-anchored token scan covers every
// driver metric reference without that dependency.
func TestChartMetricDrift(t *testing.T) {
	registered := metricSet(driver.MetricNames())
	histograms := metricSet(driver.HistogramMetricNames())
	if len(registered) == 0 {
		t.Fatal("driver.MetricNames() returned no metrics; registration did not run")
	}

	ruleExprs, recordingRules := ruleExpressions(t)
	for _, template := range []string{
		"templates/grafana-dashboard.yaml",
		"templates/prometheusrule.yaml",
	} {
		t.Run(template, func(t *testing.T) {
			var exprs []string
			switch template {
			case "templates/grafana-dashboard.yaml":
				exprs = dashboardExpressions(t)
			case "templates/prometheusrule.yaml":
				exprs = ruleExprs
			}
			if len(exprs) == 0 {
				t.Fatalf("no expressions extracted from %s; render/decode broke", template)
			}
			seen := map[string]bool{}
			for _, expr := range exprs {
				for _, token := range scaleCSIMetricRef.FindAllString(expr, -1) {
					if seen[token] {
						continue
					}
					seen[token] = true
					if err := validateMetricRef(token, registered, histograms, recordingRules); err != nil {
						t.Errorf("%s: %v", template, err)
					}
				}
			}
		})
	}
}

// TestMetricDriftValidation pins the token validator against the exact false
// negatives the previous raw-scan test missed (codex M1): a fake generated
// suffix on a non-histogram, colon-style recording-rule references, and a
// chart-defined recording-rule output name.
func TestMetricDriftValidation(t *testing.T) {
	registered := metricSet(driver.MetricNames())
	histograms := metricSet(driver.HistogramMetricNames())
	// A chart-defined recording-rule output (no colon) to prove record outputs
	// are accepted, not treated as unknown driver metrics.
	recordingRules := map[string]bool{"scale_csi_operations_rate5m": true}

	for _, tc := range []struct {
		name    string
		token   string
		wantErr bool
	}{
		{name: "registered counter", token: "scale_csi_operations_total", wantErr: false},
		{name: "registered gauge", token: "scale_csi_job_dispatcher_subscribed", wantErr: false},
		{name: "histogram bucket suffix", token: "scale_csi_operations_duration_seconds_bucket", wantErr: false},
		{name: "histogram sum suffix", token: "scale_csi_operations_duration_seconds_sum", wantErr: false},
		{name: "histogram count suffix", token: "scale_csi_operations_duration_seconds_count", wantErr: false},
		{name: "dashboard typo", token: "scale_csi_no_such_metric", wantErr: true},
		{name: "rule typo total", token: "scale_csi_no_such_total", wantErr: true},
		{name: "fake count on a gauge", token: "scale_csi_job_dispatcher_subscribed_count", wantErr: true},
		{name: "fake bucket on a counter", token: "scale_csi_operations_total_bucket", wantErr: true},
		{name: "unknown colon recording rule", token: "scale_csi_operations_total:rate5m", wantErr: true},
		{name: "unknown colon-prefix recording rule", token: "scale_csi:operations:rate5m", wantErr: true},
		{name: "chart-defined recording rule output", token: "scale_csi_operations_rate5m", wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMetricRef(tc.token, registered, histograms, recordingRules)
			if tc.wantErr {
				if err == nil {
					t.Errorf("token %q must be rejected, but validated clean", tc.token)
				}
			} else if err != nil {
				t.Errorf("token %q must be accepted, got: %v", tc.token, err)
			}
		})
	}
}

// dashboardExpressions renders the Grafana dashboard ConfigMap and returns every
// panel target expr string.
func dashboardExpressions(t *testing.T) []string {
	t.Helper()
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
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &dashboard); err != nil {
		t.Fatalf("rendered scale-csi.json is not valid JSON: %v", err)
	}

	var exprs []string
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			if target.Expr != "" {
				exprs = append(exprs, target.Expr)
			}
		}
	}
	return exprs
}

// ruleExpressions renders the PrometheusRule (all-on so every gated rule is
// present) and returns the alert/recording-rule expr strings plus the set of
// chart-defined recording-rule output names.
func ruleExpressions(t *testing.T) (exprs []string, recordingRules map[string]bool) {
	t.Helper()
	recordingRules = map[string]bool{}
	rendered := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "capacity.gaugeEnabled=true")
	ruleManifest := findManifest(t, decodeManifests(t, rendered), "PrometheusRule", "scale-csi")

	spec, ok := asManifest(ruleManifest["spec"])
	if !ok {
		t.Fatalf("PrometheusRule has no decodable spec")
	}
	groups, _ := spec["groups"].([]any)
	for _, groupAny := range groups {
		group, ok := asManifest(groupAny)
		if !ok {
			continue
		}
		rules, _ := group["rules"].([]any)
		for _, ruleAny := range rules {
			rule, ok := asManifest(ruleAny)
			if !ok {
				continue
			}
			if record, ok := rule["record"].(string); ok && record != "" {
				recordingRules[record] = true
			}
			if expr, ok := rule["expr"].(string); ok && expr != "" {
				exprs = append(exprs, expr)
			}
		}
	}
	return exprs, recordingRules
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
		"ScaleCSISustainedLockContention",
	} {
		if !strings.Contains(rendered, "- alert: "+alert) {
			t.Errorf("prometheusrule render missing alert %q", alert)
		}
	}
	if got := strings.Count(rendered, "runbook_url:"); got < 9 {
		t.Errorf("expected at least 9 runbook_url annotations on the new alerts, got %d", got)
	}
}
