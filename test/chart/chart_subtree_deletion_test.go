package chart

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestChartDeletedSubtreeFailsAsSchemaErrorNotTemplatePanic pins the deferred
// v1.11.0 item "deleting most chart configuration subtrees aborts the render".
// `--set <subtree>=null` removes a shipped default. Templates dereference those
// defaults, so deletion used to abort mid-render with a nil-pointer template
// error (45 of 56 object subtrees). Every object subtree the chart ships is now
// required by the schema: deleting one must either still render or fail
// validation naming the missing property, never reach a template.
func TestChartDeletedSubtreeFailsAsSchemaErrorNotTemplatePanic(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH")
	}
	raw, err := os.ReadFile(filepath.Join(chartDir(t), "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values map[string]interface{}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	var paths []string
	var walk func(node map[string]interface{}, prefix string, depth int)
	walk = func(node map[string]interface{}, prefix string, depth int) {
		for key, child := range node {
			childMap, ok := child.(map[string]interface{})
			if !ok || len(childMap) == 0 {
				continue
			}
			path := prefix + key
			paths = append(paths, path)
			if depth < 2 {
				walk(childMap, path+".", depth+1)
			}
		}
	}
	walk(values, "", 0)
	if len(paths) < 40 {
		t.Fatalf("expected the chart to ship dozens of object subtrees, found %d", len(paths))
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, path := range paths {
		wg.Add(1)
		sem <- struct{}{}
		go func(path string) {
			defer wg.Done()
			defer func() { <-sem }()
			out, err := exec.Command("helm", "template", "scale-csi", chartDir(t), "--set", path+"=null").CombinedOutput()
			if err == nil {
				return
			}
			if !strings.Contains(string(out), "missing property") {
				t.Errorf("--set %s=null aborted the render outside schema validation:\n%s", path, firstLines(string(out), 4))
			}
		}(path)
	}
	wg.Wait()
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
