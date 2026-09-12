package chart

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// renderDurationHelper renders scale-csi.durationToSecondsCeil directly, for a
// batch of inputs, and reports both the raw string the helper emits and the
// value a caller gets after the sprig `int` conversion every call site applies.
//
// The helper is only reachable from one template in the chart
// (controller-deployment.yaml), and that template folds its result into a
// failureThreshold that is clamped by a `max 30` floor — which is exactly what
// hid this defect. So the helper is exercised here on its own, by rendering a
// throwaway copy of the chart in a temp dir with one extra template. The copy
// is a plain directory copy of charts/scale-csi (no VCS metadata lives there);
// the chart under test is never written to.
func renderDurationHelper(t *testing.T, inputs []string) (raw, asInt []string) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart template assertion")
	}

	dst := filepath.Join(t.TempDir(), "scale-csi")
	if err := os.CopyFS(dst, os.DirFS(chartDir(t))); err != nil {
		t.Fatalf("copy chart for helper probe: %v", err)
	}

	// The inputs are baked into the generated template rather than passed as
	// values: values.schema.json sets additionalProperties=false at the root,
	// so an ad-hoc value would be rejected before any template ran.
	var body strings.Builder
	body.WriteString("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: duration-probe\ndata:\n")
	for i, in := range inputs {
		if strings.ContainsAny(in, `"\`) {
			t.Fatalf("input %q would need escaping in the generated probe template", in)
		}
		fmt.Fprintf(&body, "  raw-%d: {{ include \"scale-csi.durationToSecondsCeil\" %q | quote }}\n", i, in)
		fmt.Fprintf(&body, "  int-%d: {{ include \"scale-csi.durationToSecondsCeil\" %q | int | quote }}\n", i, in)
	}
	probe := filepath.Join(dst, "templates", "zz-duration-probe.yaml")
	if err := os.WriteFile(probe, []byte(body.String()), 0o600); err != nil {
		t.Fatalf("write helper probe template: %v", err)
	}

	out, err := exec.Command("helm", "template", "scale-csi", dst,
		"--show-only", "templates/zz-duration-probe.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("helm template helper probe failed: %v\n%s", err, out)
	}

	manifests := decodeManifests(t, string(out))
	cm := findManifest(t, manifests, "ConfigMap", "duration-probe")
	data, ok := asManifest(cm["data"])
	if !ok {
		t.Fatalf("helper probe rendered no data: %#v", cm)
	}
	raw = make([]string, len(inputs))
	asInt = make([]string, len(inputs))
	for i := range inputs {
		raw[i], _ = data[fmt.Sprintf("raw-%d", i)].(string)
		asInt[i], _ = data[fmt.Sprintf("int-%d", i)].(string)
	}
	return raw, asInt
}

var wholeNumber = regexp.MustCompile(`^[0-9]+$`)

// TestDurationToSecondsCeilAlwaysRendersWholeSeconds is the regression test for
// the helper silently collapsing to its caller's floor above a million seconds.
//
// sprig's `ceil` returns a float64. text/template prints a float64 with %v,
// which is strconv 'g' at shortest precision, and 'g' switches to exponent form
// once the decimal exponent reaches 6. So every result at or above 1e6 seconds
// rendered as text like "1.0008e+06" instead of digits. Every call site then
// pipes the helper through sprig's `int`, which is a string parse and returns 0
// on failure — so the controller's startup budget did not merely lose
// precision, it became 0 and fell back to its `max 30` floor: a 300h window got
// the same 300s probe budget as no window at all.
//
// The property asserted here is the helper's actual contract, stated without
// reference to any particular value: it emits WHOLE SECONDS, in decimal, equal
// to the ceiling of the duration, and a caller's `int` conversion round-trips
// it unchanged. A test that only checked a handful of small durations would
// have passed throughout the defect's life, because the defect lives entirely
// above the 1e6 boundary.
func TestDurationToSecondsCeilAlwaysRendersWholeSeconds(t *testing.T) {
	// The full legal space of startupConnectTimeout per values.schema.json
	// (^(0|0s|([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+)$): every unit, integer
	// and fractional segments, concatenated segments, and — the part that
	// matters — durations either side of and far beyond 1e6 seconds.
	inputs := []string{
		"0", "0s", "1ns", "999999999ns", "1us", "1µs", "1ms", "500ms", "999ms",
		"1s", "0.5s", "1.5s", "90s", "1m", "1.5m", "5m", "30m",
		"1h", "1.5h", "1h30m", "1h30.5m", "2h30m15s", "24h",
		"277h", "277.7h",
		// At and above the 1e6-second boundary where 'g' flips to exponent form.
		"999999s", "1000000s", "1000001s", "1080000s", "2000000s",
		"278h", "300h", "1000h", "10000h", "100000h",
		"1000000000s",
	}

	raw, asInt := renderDurationHelper(t, inputs)

	for i, in := range inputs {
		parsed, err := time.ParseDuration(in)
		if err != nil {
			// "0" is legal per the schema but not a Go duration literal.
			if in != "0" {
				t.Fatalf("bad test input %q: %v", in, err)
			}
			parsed = 0
		}
		want := int64(math.Ceil(parsed.Seconds()))

		if !wholeNumber.MatchString(raw[i]) {
			t.Errorf("durationToSecondsCeil(%q) = %q, which is not a whole number of seconds. "+
				"A float64 rendered in exponent form is not parseable by any caller.", in, raw[i])
			continue
		}
		got, err := strconv.ParseInt(raw[i], 10, 64)
		if err != nil {
			t.Errorf("durationToSecondsCeil(%q) = %q: %v", in, raw[i], err)
			continue
		}
		if got != want {
			t.Errorf("durationToSecondsCeil(%q) = %d seconds, want ceil(%s) = %d", in, got, parsed, want)
		}
		// What every call site actually observes. This is where the defect
		// turned a wrong-looking string into a silent 0.
		if asInt[i] != raw[i] {
			t.Errorf("durationToSecondsCeil(%q) renders %q but a caller's `int` conversion sees %q; "+
				"the helper's output is not consumable by the templates that use it", in, raw[i], asInt[i])
		}
	}
}

// TestStartupProbeBudgetSurvivesLongConnectWindows is the end-to-end half of
// the same defect: it asserts the collapse is not reachable from values.yaml.
//
// startupConnectTimeout windows at or above 1e6 seconds are absurd in practice
// but legal per values.schema.json, and for every one of them the controller
// rendered failureThreshold 30 — the `max 30` floor — i.e. a 300s probe budget
// against a multi-day connect window. The pod was guaranteed to be killed
// mid-retry, and nothing in the render looked wrong.
//
// Asserted as a property: a window past the boundary must produce a budget that
// tracks it, not the same budget as the shortest possible window.
func TestStartupProbeBudgetSurvivesLongConnectWindows(t *testing.T) {
	floorProbe := func(timeout string) int {
		t.Helper()
		manifests := decodeManifests(t, helmTemplate(t,
			"--set", "startupConnectTimeout="+timeout,
			"--show-only", "templates/controller-deployment.yaml"))
		deployment := findManifest(t, manifests, "Deployment", "controller")
		container := workloadContainer(t, deployment, "scale-csi")
		probe, _ := containerProbe(t, container, "startupProbe")
		return probeInt(t, probe, "failureThreshold")
	}

	// The floor, as rendered by the smallest legal window. Read rather than
	// hard-coded so the test states the relation, not the number.
	floor := floorProbe("0s")

	for _, timeout := range []string{"1000000s", "1000001s", "2000000s", "278h", "300h", "1000h"} {
		t.Run(timeout, func(t *testing.T) {
			if got := floorProbe(timeout); got == floor {
				t.Errorf("startupConnectTimeout=%s renders failureThreshold %d, identical to the "+
					"floor rendered for a zero window. The derived budget collapsed: the duration "+
					"helper handed the template a value it could not parse.", timeout, got)
			}
		})
	}
}
