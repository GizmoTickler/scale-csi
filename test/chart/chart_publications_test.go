package chart

import (
	"strings"
	"testing"
)

// The controller keeps publication records as VolumePublications: the chart
// ships the CRD, a Role for them in the release namespace only, and tells the
// controller to use them.
func TestChartKeepsPublicationRecordsInKubernetes(t *testing.T) {
	out := helmTemplate(t, "--include-crds")
	for _, want := range []string{
		"name: volumepublications.scale-csi.io",
		"scope: Namespaced",
		`resources: ["volumepublications"]`,
		"name: SCALE_CSI_PUBLICATION_STORE",
		"value: kubernetes",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("default render must contain %q", want)
		}
	}
	for _, doc := range strings.Split(out, "\n---\n") {
		if strings.Contains(doc, "kind: ClusterRole\n") && strings.Contains(doc, "volumepublications") {
			t.Errorf("volumepublications must be granted in the namespace only, not by a ClusterRole")
		}
	}
}
