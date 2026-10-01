package chart

import (
	"reflect"
	"strings"
	"testing"
)

// nfs.nodeIdentityNetworks changes node IDs, so it must render only when set:
// the default ConfigMap (and with it every node ID) stays as before.
func TestChartNFSNodeIdentityNetworksDefaultOff(t *testing.T) {
	out := helmTemplate(t, "--show-only", "templates/configmap.yaml")
	if strings.Contains(out, "nodeIdentityNetworks") {
		t.Errorf("the default configmap must not carry nodeIdentityNetworks:\n%s", out)
	}
}

func TestChartNFSNodeIdentityNetworksPlumbing(t *testing.T) {
	args := append([]string{"--show-only", "templates/configmap.yaml"}, requiredConfigArgs...)
	args = append(args, "--set", "nfs.nodeIdentityNetworks={192.168.201.0/24,fd00:201::/64}")
	rendered := helmTemplate(t, args...)
	if !strings.Contains(rendered, "      nodeIdentityNetworks:\n        - 192.168.201.0/24\n        - fd00:201::/64\n") {
		t.Errorf("nfs.nodeIdentityNetworks did not render under nfs:\n%s", rendered)
	}
	cfg := loadRenderedConfig(t, renderedConfigYAML(t, rendered))
	if want := []string{"192.168.201.0/24", "fd00:201::/64"}; !reflect.DeepEqual(cfg.NFS.NodeIdentityNetworks, want) {
		t.Errorf("driver config nfs.nodeIdentityNetworks = %v, want %v", cfg.NFS.NodeIdentityNetworks, want)
	}

	tooMany := make([]string, 17)
	for i := range tooMany {
		tooMany[i] = "10.0.0.0/8"
	}
	if out := helmTemplateExpectError(t, "--set", "nfs.nodeIdentityNetworks={"+strings.Join(tooMany, ",")+"}"); !strings.Contains(out, "nodeIdentityNetworks") {
		t.Errorf("the schema must cap nodeIdentityNetworks at 16:\n%s", out)
	}
}
