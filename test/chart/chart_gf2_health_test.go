package chart

import (
	"strings"
	"testing"
)

// TestChartGF2HealthCacheTTL guards the health.cacheTTL chart seam: the default
// remains a driver-side five-second default and only an explicit non-default
// value adds a new ConfigMap block.
func TestChartGF2HealthCacheTTL(t *testing.T) {
	t.Run("default omits the compatibility-preserving block", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
		if strings.Contains(out, "    health:\n") {
			t.Errorf("default ConfigMap must omit health.cacheTTL so the historical render stays unchanged")
		}
	})

	t.Run("custom TTL renders into the driver config", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "health.cacheTTL=15s")
		if !strings.Contains(out, "    health:\n      cacheTTL: \"15s\"\n") {
			t.Errorf("health.cacheTTL=15s did not render into the ConfigMap; got:\n%s", out)
		}
	})

	t.Run("zero disables cache reuse and still renders explicitly", func(t *testing.T) {
		out := helmTemplate(t, "--show-only", "templates/configmap.yaml", "--set", "health.cacheTTL=0s")
		if !strings.Contains(out, "    health:\n      cacheTTL: \"0s\"\n") {
			t.Errorf("health.cacheTTL=0s did not render into the ConfigMap; got:\n%s", out)
		}
	})

	t.Run("schema rejects a negative TTL", func(t *testing.T) {
		out := helmTemplateExpectError(t, "--set-string", "health.cacheTTL=-1s")
		if !strings.Contains(out, "cacheTTL") {
			t.Errorf("schema rejection did not mention health.cacheTTL: %s", out)
		}
	})
}
