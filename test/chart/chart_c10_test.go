package chart

import (
	"strings"
	"testing"
)

// TestChartNodeDaemonSetUpdateStrategyIsExplicit is the regression test for
// the first half of C10: the node DaemonSet used to render no updateStrategy
// at all, so the API server defaulted it (RollingUpdate, maxSurge: 0,
// maxUnavailable: 1) and Helm's three-way merge never owned the field — the
// identical defect the controller Deployment's strategy block already fixed
// (see controller-deployment.yaml). Render it explicitly and pin maxSurge: 0,
// which is the only safe value here: hostNetwork: true means a surge pod's
// registrar (:9810) and liveness (:9808) HOST ports would collide with the
// outgoing pod's.
func TestChartNodeDaemonSetUpdateStrategyIsExplicit(t *testing.T) {
	out := helmTemplate(t)
	node := findManifest(t, decodeManifests(t, out), "DaemonSet", "-node")
	spec, ok := asManifest(node["spec"])
	if !ok {
		t.Fatal("node DaemonSet has no spec")
	}
	strategy, ok := asManifest(spec["updateStrategy"])
	if !ok {
		t.Fatal("node DaemonSet renders no updateStrategy; the field must be explicit and Helm-owned")
	}
	if got, _ := strategy["type"].(string); got != "RollingUpdate" {
		t.Errorf("node DaemonSet updateStrategy.type = %v, want RollingUpdate", strategy["type"])
	}
	rollingUpdate, ok := asManifest(strategy["rollingUpdate"])
	if !ok {
		t.Fatal("node DaemonSet updateStrategy has no rollingUpdate block")
	}
	if got, ok := rollingUpdate["maxSurge"].(int); !ok || got != 0 {
		t.Errorf("node DaemonSet rollingUpdate.maxSurge = %v, want 0 (hostNetwork ports would collide on a surge pod)", rollingUpdate["maxSurge"])
	}
}

// TestChartNodeDaemonSetHasTerminationGracePeriod is the regression test for
// the second half of C10: with no explicit terminationGracePeriodSeconds the
// DaemonSet got the Pod API's 30s default, while commandTimeouts.nvme and
// commandTimeouts.mount (each 30s by default) could each legitimately run
// close to that long, and GracefulStop() blocks on in-flight RPCs — a
// node-plugin pod killed mid-mount/mid-connect can strand a mount or a
// half-torn-down session.
func TestChartNodeDaemonSetHasTerminationGracePeriod(t *testing.T) {
	out := helmTemplate(t)
	node := findManifest(t, decodeManifests(t, out), "DaemonSet", "-node")
	spec, ok := asManifest(node["spec"])
	if !ok {
		t.Fatal("node DaemonSet has no spec")
	}
	template, ok := asManifest(spec["template"])
	if !ok {
		t.Fatal("node DaemonSet has no pod template")
	}
	podSpec, ok := asManifest(template["spec"])
	if !ok {
		t.Fatal("node DaemonSet pod has no spec")
	}
	seconds, ok := podSpec["terminationGracePeriodSeconds"].(int)
	if !ok {
		t.Fatalf("node DaemonSet renders no terminationGracePeriodSeconds; got %#v", podSpec["terminationGracePeriodSeconds"])
	}
	const commandTimeoutNVMe, commandTimeoutMount = 30, 30
	if seconds <= commandTimeoutNVMe || seconds <= commandTimeoutMount {
		t.Errorf("node DaemonSet terminationGracePeriodSeconds = %d, want > both commandTimeouts.nvme (%d) and commandTimeouts.mount (%d)",
			seconds, commandTimeoutNVMe, commandTimeoutMount)
	}
}

// TestChartReconcileCronJobHasDeadlines is the regression test for the
// concurrencyPolicy: Forbid gap in C10: with no activeDeadlineSeconds, one
// wedged Job is never superseded (Forbid means the next scheduled fire is
// simply skipped) and silently stops all future reconciliation forever; with
// no startingDeadlineSeconds a missed schedule fire is never retried either.
func TestChartReconcileCronJobHasDeadlines(t *testing.T) {
	out := helmTemplate(t, "--set", "reconcile.delete.enabled=true")
	cronJob := findManifest(t, decodeManifests(t, out), "CronJob", "-reconcile")
	spec, ok := asManifest(cronJob["spec"])
	if !ok {
		t.Fatal("reconcile CronJob has no spec")
	}
	if _, ok := spec["activeDeadlineSeconds"].(int); !ok {
		t.Errorf("reconcile CronJob renders no activeDeadlineSeconds; got %#v", spec["activeDeadlineSeconds"])
	}
	if _, ok := spec["startingDeadlineSeconds"].(int); !ok {
		t.Errorf("reconcile CronJob renders no startingDeadlineSeconds; got %#v", spec["startingDeadlineSeconds"])
	}
}

// TestChartControllerSecretsRBACGatedOnStorageClassSecretRefs is the
// regression test for C10's secrets RBAC gap: cluster-wide get on ALL Secrets
// was granted unconditionally even though every bundled example StorageClass
// that would need one (CHAP, encryption) ships disabled by default, making it
// dead privilege mounted into five upstream sidecar containers.
func TestChartControllerSecretsRBACGatedOnStorageClassSecretRefs(t *testing.T) {
	t.Run("default render grants no secrets rule", func(t *testing.T) {
		out := helmTemplate(t)
		role := findManifest(t, decodeManifests(t, out), "ClusterRole", "scale-csi-controller")
		if roleTouchesResource(role, "secrets") {
			t.Error("default controller ClusterRole must not grant any secrets rule; no enabled StorageClass declares one")
		}
	})

	t.Run("an enabled CHAP StorageClass turns the rule on", func(t *testing.T) {
		valuesPath := writeValues(t, "chap-secret-rbac.yaml", `iscsi:
  enabled: true
  chap:
    enabled: true
storageClasses:
  - name: scale-nfs
    protocol: nfs
    isDefault: false
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: []
    extraParameters: {}
  - name: scale-iscsi-chap
    enabled: true
    protocol: iscsi
    chapSecretName: scale-iscsi-chap
    chapSecretNamespace: ""
    isDefault: false
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: []
    extraParameters: {}
`)
		out := helmTemplate(t, "-f", valuesPath)
		role := findManifest(t, decodeManifests(t, out), "ClusterRole", "scale-csi-controller")
		if !roleHasRule(role, []string{"secrets"}, []string{"get"}) {
			t.Error("an enabled StorageClass with chapSecretName must turn the secrets get rule on")
		}
	})
}

// TestChartSidecarsHaveRetryIntervalMax is the regression test for C10's last
// item: without --retry-interval-max, the provisioner/attacher/resizer
// sidecars inherit upstream's 5-minute default, so after a TrueNAS blip a
// failed ControllerUnpublishVolume backs off to once every 5 minutes — the
// single largest avoidable contributor to a node reboot stranding volumes.
func TestChartSidecarsHaveRetryIntervalMax(t *testing.T) {
	out := helmTemplate(t)
	got := strings.Count(out, `"--retry-interval-max=30s"`)
	if got != 3 {
		t.Errorf(`expected exactly 3 "--retry-interval-max=30s" occurrences (provisioner, attacher, resizer), got %d`, got)
	}
}
