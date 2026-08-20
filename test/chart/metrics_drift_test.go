package chart

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	// Blank import triggers the driver's metric registration so MetricNames()
	// returns the complete registered set. The drift comparison below is only
	// meaningful if registration has run.
	"github.com/GizmoTickler/scale-csi/pkg/driver"
	_ "github.com/GizmoTickler/scale-csi/pkg/driver"
)

// grafanaDurationVars matches Grafana duration template variables such as
// $__rate_interval / ${__interval}. They are not valid PromQL; they occupy
// range-duration slots, so substituting a literal duration lets ParseExpr
// succeed without inventing metric names.
var grafanaDurationVars = regexp.MustCompile(`\$\{?__[a-zA-Z][a-zA-Z0-9_]*\}?`)

var promqlParser = parser.NewParser(parser.Options{})

// extractScaleCSIMetricRefs returns every scale_csi metric or recording-rule
// name that appears as a PromQL vector selector. Grouping labels, label
// matcher names, string literals (including PromQL raw/backtick strings), and
// comments cannot satisfy coverage. A substring regex has no left boundary
// (fake_scale_csi_foo credits scale_csi_foo) and a token lexer credits every
// identifier, including `sum by (scale_csi_foo) (vector(0))`.
func extractScaleCSIMetricRefs(expr string) []string {
	refs, err := parseScaleCSIMetricRefs(expr)
	if err != nil {
		return nil
	}
	return refs
}

func parseScaleCSIMetricRefs(expr string) ([]string, error) {
	rewritten := grafanaDurationVars.ReplaceAllString(expr, "1m")
	parsed, err := promqlParser.ParseExpr(rewritten)
	if err != nil {
		return nil, fmt.Errorf("parse PromQL %q: %w", expr, err)
	}
	if parsed == nil {
		return nil, fmt.Errorf("parse PromQL %q: empty AST", expr)
	}
	var refs []string
	parser.Inspect(parsed, func(node parser.Node, _ []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		name := vectorSelectorMetricName(vs)
		if isScaleCSIMetricIdent(name) {
			refs = append(refs, name)
		}
		return nil
	})
	return refs, nil
}

func vectorSelectorMetricName(vs *parser.VectorSelector) string {
	if vs.Name != "" {
		return vs.Name
	}
	for _, m := range vs.LabelMatchers {
		if m.Name == "__name__" && m.Type == labels.MatchEqual {
			return m.Value
		}
	}
	return ""
}

func isScaleCSIMetricIdent(ident string) bool {
	return ident == "scale_csi" || strings.HasPrefix(ident, "scale_csi_") || strings.HasPrefix(ident, "scale_csi:")
}

// generatedSuffixes are the series Prometheus exposes on top of a
// histogram/summary base. They are legitimate ONLY when the base is a
// histogram/summary — a gauge or counter named `..._count` does not exist.
var generatedSuffixes = []string{"_bucket", "_sum", "_count"}

// unobservedMetrics is the explicit allowlist of registered driver metrics that
// no dashboard panel and no PrometheusRule expression is required to name.
// Each entry MUST carry a one-line reason: an empty map is the goal (every
// metric is observed); a catch-all or unexplained entry defeats the reverse
// drift check. Prefer paneling a metric that carries operator signal over
// adding it here.
var unobservedMetrics = map[string]string{
	// Intentionally empty. A newly registered metric that is referenced by
	// neither a panel nor a PrometheusRule fails TestChartMetricDrift until it
	// is added to a panel/alert or given a reason here.
}

// unpaneledMetrics is the explicit allowlist of registered driver metrics that
// no dashboard panel is required to name. Distinct from unobservedMetrics: a
// PrometheusRule reference satisfies dashboard-OR-rule coverage but does NOT
// satisfy this dashboard-only invariant. Each entry MUST carry a one-line
// reason. Prefer paneling a metric that carries operator signal over adding
// it here — this is the check that would have caught a missing Guarded GC
// panel while the alert still named the gauge.
var unpaneledMetrics = map[string]string{
	// Intentionally empty. All registered metrics are currently paneled.
}

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

// canonicalObservedName maps a scale_csi token from a panel or rule expression
// back to the registered driver metric it covers. Histogram/summary derived
// series (_bucket/_sum/_count) map to their base so a panel that charts
// operations_duration_seconds_bucket covers operations_duration_seconds and
// does not demand a separate panel for the derived name. Tokens that are not
// a registered name and not a legal histogram derivative return ""
// (recording-rule outputs, typos, fake suffixes on gauges).
func canonicalObservedName(token string, registered, histograms map[string]bool) string {
	if registered[token] {
		return token
	}
	for _, suffix := range generatedSuffixes {
		if base, ok := strings.CutSuffix(token, suffix); ok && histograms[base] {
			return base
		}
	}
	return ""
}

func metricSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

// uncoveredRegistered returns registered metrics that are not in covered and
// not in allowlist. Used by both reverse invariants so a pin test can prove
// dashboard-only fails when dashboard-OR-rule passes.
func uncoveredRegistered(registered []string, covered map[string]bool, allowlist map[string]string) []string {
	var missing []string
	for _, name := range registered {
		if _, ok := allowlist[name]; ok {
			continue
		}
		if !covered[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

func assertAllowlist(t *testing.T, label, coveredDesc string, allowlist map[string]string, registered, covered map[string]bool) {
	t.Helper()
	for name, reason := range allowlist {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s[%q] has no reason; each allowlisted metric needs a one-line reason", label, name)
		}
		if !registered[name] {
			t.Errorf("%s[%q] is not in driver.MetricNames(); remove the stale allowlist entry", label, name)
		}
		if covered[name] {
			t.Errorf("%s[%q] is already referenced by %s; remove it from the allowlist", label, name, coveredDesc)
		}
	}
}

// TestChartMetricDrift forbids metric-name drift between the driver and the
// chart in both directions:
//
//  1. Forward: every scale_csi_* metric a dashboard panel or PrometheusRule
//     alert EXPRESSION references MUST be registered by the driver (or be a
//     chart-defined recording-rule output). Deleting a metric still named by a
//     panel, or adding a panel that names a typo'd / removed metric, fails here.
//  2. Reverse (dashboard-OR-rule): every metric in driver.MetricNames() MUST
//     be referenced by a dashboard panel or a chart PrometheusRule expression,
//     unless it is in unobservedMetrics with a one-line reason.
//  3. Reverse (dashboard-only): every metric in driver.MetricNames() MUST be
//     referenced by a dashboard panel expression, unless it is in
//     unpaneledMetrics with a one-line reason. A PrometheusRule reference
//     does not satisfy this invariant — that is the check that would have
//     caught a missing Guarded GC panel while the alert still named the gauge.
//
// Histogram/summary derived series (_bucket/_sum/_count) cover the registered
// base — a panel that charts the _bucket does not leave the base unobserved,
// and neither reverse check demands a panel for a derived name. Node-plugin-only
// metrics still count as registered; they are not silently exempt.
//
// Unlike a raw byte scan, this renders both templates and extracts the actual
// expr fields, so titles, descriptions, annotations, and YAML comments are
// not mistaken for driver metric references; it retains full colon-bearing
// names; and it permits _bucket/_sum/_count only on histogram bases (codex M1).
// Metric names come from PromQL vector selectors (string literals, comments,
// grouping labels, and matcher names do not count); hidden Grafana targets
// are ignored; nested panels inside collapsed rows are traversed.
func TestChartMetricDrift(t *testing.T) {
	registered := metricSet(driver.MetricNames())
	histograms := metricSet(driver.HistogramMetricNames())
	if len(registered) == 0 {
		t.Fatal("driver.MetricNames() returned no metrics; registration did not run")
	}

	ruleExprs, recordingRules := ruleExpressions(t)
	observed := map[string]bool{}
	paneled := map[string]bool{}
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
				tokens, err := parseScaleCSIMetricRefs(expr)
				if err != nil {
					t.Errorf("%s: %v", template, err)
					continue
				}
				for _, token := range tokens {
					if seen[token] {
						continue
					}
					seen[token] = true
					if err := validateMetricRef(token, registered, histograms, recordingRules); err != nil {
						t.Errorf("%s: %v", template, err)
					}
					if base := canonicalObservedName(token, registered, histograms); base != "" {
						observed[base] = true
						if template == "templates/grafana-dashboard.yaml" {
							paneled[base] = true
						}
					}
				}
			}
		})
	}

	t.Run("every registered metric is observed", func(t *testing.T) {
		for _, name := range uncoveredRegistered(driver.MetricNames(), observed, unobservedMetrics) {
			t.Errorf("registered metric %q is not referenced by any dashboard panel or PrometheusRule expression; add a panel (or alert) for it, or add it to unobservedMetrics with a one-line reason", name)
		}
		assertAllowlist(t, "unobservedMetrics", "a panel or rule", unobservedMetrics, registered, observed)
	})

	t.Run("every registered metric is paneled", func(t *testing.T) {
		for _, name := range uncoveredRegistered(driver.MetricNames(), paneled, unpaneledMetrics) {
			t.Errorf("registered metric %q is not referenced by any dashboard panel expression; a PrometheusRule reference does not satisfy this invariant (that is the dashboard-OR-rule check / unobservedMetrics). Add a panel for it, or add it to unpaneledMetrics with a one-line reason", name)
		}
		assertAllowlist(t, "unpaneledMetrics", "a dashboard panel", unpaneledMetrics, registered, paneled)
	})
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

	// Reverse direction: a token in a panel/rule covers its registered base,
	// and histogram derived series cover the base rather than demanding a
	// panel of their own. Typos and fake suffixes cover nothing.
	for _, tc := range []struct {
		name        string
		token       string
		wantCovered string
	}{
		{name: "histogram bucket covers base", token: "scale_csi_operations_duration_seconds_bucket", wantCovered: "scale_csi_operations_duration_seconds"},
		{name: "histogram sum covers base", token: "scale_csi_operations_duration_seconds_sum", wantCovered: "scale_csi_operations_duration_seconds"},
		{name: "histogram count covers base", token: "scale_csi_operations_duration_seconds_count", wantCovered: "scale_csi_operations_duration_seconds"},
		{name: "histogram base covers itself", token: "scale_csi_operations_duration_seconds", wantCovered: "scale_csi_operations_duration_seconds"},
		{name: "registered gauge covers itself", token: "scale_csi_job_dispatcher_subscribed", wantCovered: "scale_csi_job_dispatcher_subscribed"},
		{name: "dashboard typo covers nothing", token: "scale_csi_no_such_metric", wantCovered: ""},
		{name: "fake count on a gauge covers nothing", token: "scale_csi_job_dispatcher_subscribed_count", wantCovered: ""},
		{name: "recording-rule token covers nothing", token: "scale_csi_operations_rate5m", wantCovered: ""},
	} {
		t.Run("reverse "+tc.name, func(t *testing.T) {
			got := canonicalObservedName(tc.token, registered, histograms)
			if got != tc.wantCovered {
				t.Errorf("canonicalObservedName(%q) = %q, want %q", tc.token, got, tc.wantCovered)
			}
		})
	}
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// TestDashboardTargetExprsIgnoresHiddenTargets pins that a target with
// hide:true must not satisfy reverse coverage.
func TestDashboardTargetExprsIgnoresHiddenTargets(t *testing.T) {
	const dash = `{"panels":[{"targets":[
		{"expr":"scale_csi_tombstone_unknown_age","hide":true},
		{"expr":"scale_csi_operations_total"}
	]}]}`
	exprs, err := dashboardTargetExprs([]byte(dash))
	if err != nil {
		t.Fatalf("dashboardTargetExprs: %v", err)
	}
	joined := strings.Join(exprs, "\n")
	if strings.Contains(joined, "scale_csi_tombstone_unknown_age") {
		t.Fatalf("hidden target still extracted: %v", exprs)
	}
	if !strings.Contains(joined, "scale_csi_operations_total") {
		t.Fatalf("visible target dropped: %v", exprs)
	}
}

// TestDashboardTargetExprsTraversesNestedPanels pins that a panel inside a
// collapsed Grafana row counts as coverage, while hidden nested targets still
// do not.
func TestDashboardTargetExprsTraversesNestedPanels(t *testing.T) {
	const dash = `{
		"panels":[{
			"type":"row",
			"collapsed":true,
			"panels":[{
				"targets":[
					{"expr":"scale_csi_tombstone_unknown_age"},
					{"expr":"scale_csi_operations_total","hide":true}
				]
			}]
		}]
	}`
	exprs, err := dashboardTargetExprs([]byte(dash))
	if err != nil {
		t.Fatalf("dashboardTargetExprs: %v", err)
	}
	joined := strings.Join(exprs, "\n")
	if !strings.Contains(joined, "scale_csi_tombstone_unknown_age") {
		t.Fatalf("nested panel in collapsed row dropped: %v", exprs)
	}
	if strings.Contains(joined, "scale_csi_operations_total") {
		t.Fatalf("hidden nested target still extracted: %v", exprs)
	}
}

// TestExtractScaleCSIMetricRefsIdentifierBoundary pins that a non-scale
// identifier with an embedded scale_csi_ prefix is ignored as a selector
// (not tokenized as the real metric). The fake name is therefore not a
// forward-check hit; reverse coverage of the real metric requires a real
// selector.
func TestExtractScaleCSIMetricRefsIdentifierBoundary(t *testing.T) {
	const realMetric = "scale_csi_tombstone_unknown_age"
	fake := "fake_scale_csi_tombstone_unknown_age"
	refs, err := parseScaleCSIMetricRefs(fake)
	if err != nil {
		t.Fatalf("parse %q: %v", fake, err)
	}
	if containsStr(refs, realMetric) {
		t.Fatalf("left-boundary miss: %q extracted %v (credits the real metric)", fake, refs)
	}
	if len(refs) != 0 {
		t.Fatalf("fake identifier %q must not yield a scale_csi token, got %v", fake, refs)
	}

	covered := map[string]bool{}
	registered := metricSet([]string{realMetric})
	histograms := map[string]bool{}
	for _, token := range refs {
		if base := canonicalObservedName(token, registered, histograms); base != "" {
			covered[base] = true
		}
	}
	gaps := uncoveredRegistered([]string{realMetric}, covered, nil)
	if len(gaps) != 1 || gaps[0] != realMetric {
		t.Fatalf("panel whose only expr is the fake identifier must leave %q uncovered; gaps=%v", realMetric, gaps)
	}

	real, err := parseScaleCSIMetricRefs("rate(scale_csi_tombstone_unknown_age[5m])")
	if err != nil {
		t.Fatalf("parse real selector: %v", err)
	}
	if !containsStr(real, realMetric) {
		t.Fatalf("real selector must still extract, got %v", real)
	}
}

// TestExtractScaleCSIMetricRefsStringLiterals pins that string contents are
// not vector selectors, so a label_replace replacement (any quote form)
// cannot satisfy coverage. PromQL raw strings use backticks and do not treat
// backslash as an escape.
func TestExtractScaleCSIMetricRefsStringLiterals(t *testing.T) {
	const metric = "scale_csi_tombstone_unknown_age"
	for _, tc := range []struct {
		name string
		expr string
	}{
		{name: "double-quoted string", expr: `label_replace(vector(0), "x", "scale_csi_tombstone_unknown_age", "", "")`},
		{name: "single-quoted string", expr: `label_replace(vector(0), "x", 'scale_csi_tombstone_unknown_age', "", "")`},
		{name: "raw backtick string", expr: "label_replace(vector(0), \"x\", `scale_csi_tombstone_unknown_age`, \"\", \"\")"},
		{name: "escaped quote in double-quoted string", expr: `label_replace(vector(0), "x", "foo\"scale_csi_tombstone_unknown_age", "", "")`},
		{name: "escaped quote in single-quoted string", expr: `label_replace(vector(0), "x", 'foo\'scale_csi_tombstone_unknown_age', "", "")`},
		{name: "backslash-quote in raw backtick string", expr: "label_replace(vector(0), \"x\", `foo\\\"scale_csi_tombstone_unknown_age`, \"\", \"\")"},
		{name: "hash comment", expr: "vector(0) # scale_csi_tombstone_unknown_age"},
		{name: "label value", expr: `up{job="scale_csi_tombstone_unknown_age"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs, err := parseScaleCSIMetricRefs(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			if containsStr(refs, metric) {
				t.Fatalf("non-selector extracted as a metric: %v from %s", refs, tc.expr)
			}
		})
	}

	mixed := `label_replace(scale_csi_operations_total, "x", "scale_csi_tombstone_unknown_age", "", "")`
	mixedRefs, err := parseScaleCSIMetricRefs(mixed)
	if err != nil {
		t.Fatalf("parse mixed: %v", err)
	}
	if !containsStr(mixedRefs, "scale_csi_operations_total") {
		t.Fatalf("real selector next to a string must still extract, got %v", mixedRefs)
	}
	if containsStr(mixedRefs, metric) {
		t.Fatalf("string replacement must not extract, got %v", mixedRefs)
	}
}

// TestExtractScaleCSIMetricRefsSelectorPosition pins that only vector-selector
// metric names count: grouping labels and label matcher names do not.
func TestExtractScaleCSIMetricRefsSelectorPosition(t *testing.T) {
	const decoy = "scale_csi_tombstone_unknown_age"
	const real = "scale_csi_operations_total"
	for _, tc := range []struct {
		name    string
		expr    string
		want    []string
		notWant string
	}{
		{name: "by grouping label", expr: `sum by (scale_csi_tombstone_unknown_age) (vector(0))`},
		{name: "without grouping label", expr: `sum without (scale_csi_tombstone_unknown_age) (vector(0))`},
		{name: "label matcher name", expr: `up{scale_csi_tombstone_unknown_age="foo"}`},
		{name: "selector with grouping decoy", expr: `sum by (scale_csi_tombstone_unknown_age) (scale_csi_operations_total)`, want: []string{real}, notWant: decoy},
		{name: "selector with matcher-name decoy", expr: `scale_csi_operations_total{scale_csi_tombstone_unknown_age="foo"}`, want: []string{real}, notWant: decoy},
		{name: "range selector", expr: `rate(scale_csi_operations_total[5m])`, want: []string{real}},
		{name: "grafana rate interval", expr: `rate(scale_csi_operations_total[$__rate_interval])`, want: []string{real}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refs, err := parseScaleCSIMetricRefs(tc.expr)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.expr, err)
			}
			for _, w := range tc.want {
				if !containsStr(refs, w) {
					t.Fatalf("missing selector %q in %v from %s", w, refs, tc.expr)
				}
			}
			notWant := tc.notWant
			if notWant == "" {
				notWant = decoy
			}
			if containsStr(refs, notWant) && !containsStr(tc.want, notWant) {
				t.Fatalf("non-selector %q extracted as a metric: %v from %s", notWant, refs, tc.expr)
			}
		})
	}
}

// TestDashboardOnlyReverseFailsWhenMetricIsAlertReferencedButUnpaneled proves
// the dashboard-only reverse invariant is distinct from dashboard-OR-rule: a
// metric named by a PrometheusRule but by no panel fails dashboard-only and
// passes dashboard-OR-rule. That is the gap that unreferencing
// scale_csi_reconcile_delete_enabled from the dashboard would have slipped
// through.
func TestDashboardOnlyReverseFailsWhenMetricIsAlertReferencedButUnpaneled(t *testing.T) {
	const metric = "scale_csi_reconcile_delete_enabled"
	registered := []string{metric}
	histograms := map[string]bool{}
	registeredSet := metricSet(registered)

	paneled := map[string]bool{}
	for _, token := range extractScaleCSIMetricRefs(`max(scale_csi_operations_total) or vector(0)`) {
		if base := canonicalObservedName(token, registeredSet, histograms); base != "" {
			paneled[base] = true
		}
	}
	observed := map[string]bool{}
	for k, v := range paneled {
		observed[k] = v
	}
	ruleExpr := `scale_csi_reconcile_delete_enabled == 1 and absent(max(scale_csi_tombstone_reap_last_success_timestamp_seconds))`
	for _, token := range extractScaleCSIMetricRefs(ruleExpr) {
		if base := canonicalObservedName(token, registeredSet, histograms); base != "" {
			observed[base] = true
		}
	}

	if paneled[metric] {
		t.Fatal("setup: panel expr must not cover " + metric)
	}
	if !observed[metric] {
		t.Fatal("setup: rule expr must cover " + metric)
	}

	if gaps := uncoveredRegistered(registered, observed, unobservedMetrics); len(gaps) != 0 {
		t.Fatalf("dashboard-OR-rule reverse must pass when the metric is alert-referenced; gaps=%v", gaps)
	}
	gaps := uncoveredRegistered(registered, paneled, unpaneledMetrics)
	if len(gaps) != 1 || gaps[0] != metric {
		t.Fatalf("dashboard-only reverse must fail when the metric is panel-less but alert-referenced; gaps=%v", gaps)
	}
}

// dashboardExpressions renders the Grafana dashboard ConfigMap and returns every
// non-hidden panel target expr string.
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

	exprs, err := dashboardTargetExprs([]byte(jsonStr))
	if err != nil {
		t.Fatalf("rendered scale-csi.json is not valid JSON: %v", err)
	}
	return exprs
}

// grafanaPanel is a dashboard panel that may itself contain nested panels
// (Grafana collapsed rows). Row collapse is presentation state; nested
// panels are real coverage.
type grafanaPanel struct {
	Panels  []grafanaPanel `json:"panels"`
	Targets []struct {
		Expr string `json:"expr"`
		Hide bool   `json:"hide"`
	} `json:"targets"`
}

// dashboardTargetExprs returns every non-hidden panel target expr, including
// targets on panels nested inside collapsed rows. Hidden targets (Grafana
// `"hide": true`) are ignored so they cannot satisfy reverse coverage.
func dashboardTargetExprs(raw []byte) ([]string, error) {
	var dashboard struct {
		Panels []grafanaPanel `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dashboard); err != nil {
		return nil, err
	}
	var exprs []string
	collectPanelExprs(dashboard.Panels, &exprs)
	return exprs, nil
}

func collectPanelExprs(panels []grafanaPanel, exprs *[]string) {
	for _, panel := range panels {
		for _, target := range panel.Targets {
			if target.Hide {
				continue
			}
			if target.Expr != "" {
				*exprs = append(*exprs, target.Expr)
			}
		}
		if len(panel.Panels) > 0 {
			collectPanelExprs(panel.Panels, exprs)
		}
	}
}

// ruleExpressions renders the PrometheusRule (all-on so every gated rule is
// present) and returns the alert/recording-rule expr strings plus the set of
// chart-defined recording-rule output names.
func ruleExpressions(t *testing.T) (exprs []string, recordingRules map[string]bool) {
	t.Helper()
	recordingRules = map[string]bool{}
	// Every VALUE-GATED alert block must be enabled here, or a metric typo inside
	// one would never be seen by the drift check.
	rendered := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "capacity.gaugeEnabled=true",
		"--set", "backendHealth.enabled=true",
		"--set", "zfs.reportVolumeUsage=true",
		"--set", "zfs.snapshotSchedule=0 0 * * *",
		"--set", "zfs.holdCsiSnapshots=true",
		"--set", "zfs.promoteRestoredClones=true",
		"--set", "reconcile.delete.enabled=true")
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
		"ScaleCSITombstoneOldestStuck",
		"ScaleCSIFencingTakeoverSpike",
		"ScaleCSIFencingProvenanceOverflow",
		"ScaleCSIReconcileStalled",
		"ScaleCSIJobDispatcherUnsubscribed",
		"ScaleCSIDeleteResidualCleanupFailing",
		"ScaleCSISustainedLockContention",
		"ScaleCSIOperationFailedPreconditionStuck",
	} {
		if !strings.Contains(rendered, "- alert: "+alert) {
			t.Errorf("prometheusrule render missing alert %q", alert)
		}
	}
	if got := strings.Count(rendered, "runbook_url:"); got < 9 {
		t.Errorf("expected at least 9 runbook_url annotations on the new alerts, got %d", got)
	}
	for _, alert := range []string{
		"ScaleCSITombstoneReapCapped",
		"ScaleCSITombstoneReapStale",
		"ScaleCSITombstoneReapNeverRan",
	} {
		if strings.Contains(rendered, "- alert: "+alert) {
			t.Errorf("delete-pass alert %q must not render when reconcile.delete.enabled is false", alert)
		}
	}
}

func TestChartPrometheusRuleDeletePassAlerts(t *testing.T) {
	rendered := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "reconcile.delete.enabled=true")
	for _, alert := range []string{
		"ScaleCSITombstoneReapCapped",
		"ScaleCSITombstoneReapStale",
		"ScaleCSITombstoneReapNeverRan",
	} {
		if !strings.Contains(rendered, "- alert: "+alert) {
			t.Errorf("prometheusrule render missing delete-pass alert %q", alert)
		}
	}
	if !strings.Contains(rendered, "absent(max(scale_csi_tombstone_reap_last_success_timestamp_seconds") {
		t.Error("ScaleCSITombstoneReapNeverRan must wrap absent() around max() so both sides share an empty label set")
	}
	if strings.Contains(rendered, "and absent(scale_csi_tombstone_reap_last_success_timestamp_seconds") &&
		!strings.Contains(rendered, "and absent(max(scale_csi_tombstone_reap_last_success_timestamp_seconds") {
		t.Error("ScaleCSITombstoneReapNeverRan still uses the dead absent(series) form")
	}

	gatedOff := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "reconcile.enabled=false",
		"--set", "reconcile.delete.enabled=true")
	if !strings.Contains(gatedOff, "- alert: ScaleCSITombstoneReapNeverRan") {
		t.Error("delete-pass alerts must render when reconcile.delete.enabled=true even if reconcile.enabled=false")
	}
	if strings.Contains(gatedOff, "- alert: ScaleCSIOrphanVolumesDetected") {
		t.Error("orphan-detection alerts must not render when reconcile.enabled=false")
	}
}

func TestChartPrometheusRuleDefaultAlertCounts(t *testing.T) {
	defaultRender := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true")
	defaultAlerts := strings.Count(defaultRender, "- alert:")
	defaultRunbooks := strings.Count(defaultRender, "runbook_url:")
	if defaultAlerts != 20 {
		t.Errorf("default PrometheusRule render (prometheusRule.enabled, other defaults) must have 20 alerts, got %d", defaultAlerts)
	}
	if defaultRunbooks != 11 {
		t.Errorf("default PrometheusRule render must have 11 runbook_url annotations, got %d", defaultRunbooks)
	}

	withDelete := helmTemplate(t, "--show-only", "templates/prometheusrule.yaml",
		"--set", "metrics.prometheusRule.enabled=true",
		"--set", "reconcile.delete.enabled=true")
	if got := strings.Count(withDelete, "- alert:"); got != 23 {
		t.Errorf("prometheusRule + delete.enabled render must have 23 alerts, got %d", got)
	}
	if got := strings.Count(withDelete, "runbook_url:"); got != 14 {
		t.Errorf("prometheusRule + delete.enabled render must have 14 runbook_url annotations, got %d", got)
	}
}
