package main

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// MetricCardinality bans volume IDs, PVC names, and snapshot IDs as
// Prometheus label VALUES.
//
// Why: those identifiers are unbounded over a cluster's lifetime. A counter
// or gauge vec labeled by one of them accumulates one time series per
// volume/PVC/snapshot EVER seen, forever — a slow, silent cardinality leak
// that first shows up as Prometheus/Victoria Metrics memory pressure, not as
// an obviously-wrong metric. The one legitimate exception in this codebase is
// the per-volume usage/quota/fencing gauges, which are explicitly designed
// around this: they are Reset() and fully republished from a live-volume walk
// every pass (see ResetVolumeUsageMetrics and
// ResetStartupFencingUnconvergedVolumes in pkg/driver/metrics.go), so their
// cardinality is bounded by the CURRENT live volume count, not by history.
// Those are allowlisted by variable name below; a new per-identity vec must
// either follow that same reset-and-republish discipline and be added to the
// allowlist with the same justification, or use a bounded label instead.
var MetricCardinality = &analysis.Analyzer{
	Name: "metriccardinality",
	Doc:  "flags .WithLabelValues()/.DeleteLabelValues() calls passing an apparent volume/PVC/snapshot identifier to a Prometheus vec not on the reviewed allowlist",
	Run:  runMetricCardinality,
}

// reviewedPerVolumeVecs are the Reset-and-republish-per-pass vecs reviewed and
// accepted as bounded despite being keyed by volume identity. See the doc
// comment above.
var reviewedPerVolumeVecs = map[string]bool{
	"volumeUsedBytes":                  true,
	"volumeQuotaBytes":                 true,
	"volumeNearQuota":                  true,
	"startupFencingUnconvergedVolumes": true,
}

var suspiciousLabelIdentifiers = []string{
	"volumeid", "volume_id",
	"pvcname", "pvc_name", "pvname", "pv_name",
	"snapshotid", "snapshot_id",
	"snapshotname", "snapshot_name",
}

func runMetricCardinality(pass *analysis.Pass) (any, error) {
	if !isTargetPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	for _, file := range pass.Files {
		if isGeneratedOrTestFile(pass.Fset, file) {
			continue
		}
		lc := newLineComments(pass.Fset, file)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if sel.Sel.Name != "WithLabelValues" && sel.Sel.Name != "DeleteLabelValues" && sel.Sel.Name != "GetMetricWithLabelValues" {
				return true
			}
			recvName, ok := vecReceiverName(sel.X)
			if !ok || reviewedPerVolumeVecs[recvName] {
				return true
			}
			if !isPrometheusVecType(pass, sel.X) {
				return true
			}
			for _, arg := range call.Args {
				if name, ok := suspiciousArgName(arg); ok {
					if lc.suppressed(pass.Fset, int(call.Pos()), "RG-METRIC-CARDINALITY") {
						continue
					}
					pass.Reportf(call.Pos(),
						"RG-METRIC-CARDINALITY: %s.%s(%s, ...) passes what looks like a volume/PVC/snapshot identifier as a Prometheus label value — this vec is not on the reviewed reset-and-republish allowlist (see MetricCardinality's doc comment), so its series count would grow without bound over the cluster's lifetime instead of tracking only live volumes",
						recvName, sel.Sel.Name, name)
				}
			}
			return true
		})
	}
	return nil, nil
}

// vecReceiverName returns the identifier name of the vec being called on,
// e.g. "volumeUsedBytes" in volumeUsedBytes.WithLabelValues(...).
func vecReceiverName(x ast.Expr) (string, bool) {
	id, ok := x.(*ast.Ident)
	if !ok {
		return "", false
	}
	return id.Name, true
}

func isPrometheusVecType(pass *analysis.Pass, x ast.Expr) bool {
	t := pass.TypesInfo.TypeOf(x)
	if t == nil {
		return false
	}
	named, ok := unwrapPointer(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	if !strings.HasSuffix(obj.Pkg().Path(), "prometheus/client_golang/prometheus") {
		return false
	}
	return strings.HasSuffix(obj.Name(), "Vec")
}

func unwrapPointer(t types.Type) types.Type {
	if ptr, ok := t.(*types.Pointer); ok {
		return ptr.Elem()
	}
	return t
}

// suspiciousArgName reports whether arg's textual identifier (its own name,
// or the final selector segment for something like usage.VolumeID) matches a
// known high-cardinality identity field.
func suspiciousArgName(arg ast.Expr) (string, bool) {
	var name string
	switch e := arg.(type) {
	case *ast.Ident:
		name = e.Name
	case *ast.SelectorExpr:
		name = e.Sel.Name
	default:
		return "", false
	}
	lower := strings.ToLower(name)
	for _, needle := range suspiciousLabelIdentifiers {
		if strings.Contains(lower, needle) {
			return name, true
		}
	}
	return "", false
}
