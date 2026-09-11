package chart

import "testing"

// podSecurityContextOf returns the pod-level securityContext of a rendered
// Deployment/DaemonSet manifest, or nil when the pod spec renders none.
func podSecurityContextOf(t *testing.T, workload manifest, name string) manifest {
	t.Helper()
	spec, ok := asManifest(workload["spec"])
	if !ok {
		t.Fatalf("%s workload has no spec", name)
	}
	template, ok := asManifest(spec["template"])
	if !ok {
		t.Fatalf("%s workload has no pod template", name)
	}
	podSpec, ok := asManifest(template["spec"])
	if !ok {
		t.Fatalf("%s workload pod has no spec", name)
	}
	context, _ := asManifest(podSpec["securityContext"])
	return context
}

// cronJobPodSecurityContextOf is podSecurityContextOf for a CronJob, whose pod
// template sits one level deeper under spec.jobTemplate.spec.template.spec.
func cronJobPodSecurityContextOf(t *testing.T, workload manifest, name string) manifest {
	t.Helper()
	spec, ok := asManifest(workload["spec"])
	if !ok {
		t.Fatalf("%s workload has no spec", name)
	}
	jobTemplate, ok := asManifest(spec["jobTemplate"])
	if !ok {
		t.Fatalf("%s workload has no jobTemplate", name)
	}
	return podSecurityContextOf(t, jobTemplate, name)
}

func seccompProfileType(context manifest) (string, bool) {
	profile, ok := asManifest(context["seccompProfile"])
	if !ok {
		return "", false
	}
	profileType, ok := profile["type"].(string)
	return profileType, ok
}

// TestChartControllerPodSeccompDefault guards the H-3 hardening boundary: the
// controller pod (no privileged containers) defaults to the RuntimeDefault
// seccomp profile, deep-merged over the shared podSecurityContext without
// losing its keys — while the node DaemonSet pod deliberately renders NO
// pod-level seccompProfile, because its driver container is privileged
// (privileged implies unconfined) and a pod-level profile there risks breaking
// the iSCSI/NVMe host tooling.
func TestChartControllerPodSeccompDefault(t *testing.T) {
	rendered := helmTemplate(t)
	manifests := decodeManifests(t, rendered)

	controller := findManifest(t, manifests, "Deployment", "-controller")
	controllerContext := podSecurityContextOf(t, controller, "controller")
	if controllerContext == nil {
		t.Fatal("controller pod rendered no pod-level securityContext")
	}
	if got, ok := seccompProfileType(controllerContext); !ok || got != "RuntimeDefault" {
		t.Errorf("controller pod must default to seccompProfile RuntimeDefault; got %#v", controllerContext["seccompProfile"])
	}
	// The controller-only seccomp default must MERGE with (not replace) the
	// shared podSecurityContext defaults.
	if got, ok := controllerContext["runAsNonRoot"].(bool); !ok || got {
		t.Errorf("controller pod lost the shared runAsNonRoot: false default; got %#v", controllerContext["runAsNonRoot"])
	}
	if got, ok := controllerContext["fsGroup"].(int); !ok || got != 0 {
		t.Errorf("controller pod lost the shared fsGroup: 0 default; got %#v", controllerContext["fsGroup"])
	}

	node := findManifest(t, manifests, "DaemonSet", "-node")
	nodeContext := podSecurityContextOf(t, node, "node")
	if nodeContext == nil {
		t.Fatal("node pod rendered no pod-level securityContext")
	}
	if _, present := nodeContext["seccompProfile"]; present {
		t.Errorf("node DaemonSet pod must NOT gain a pod-level seccompProfile (privileged driver + iSCSI/NVMe tooling); got %#v", nodeContext["seccompProfile"])
	}
}

// TestChartReconcileCronJobPodSeccompDefault is the regression test for C10:
// the reconcile CronJob runs the CONTROLLER image and is authorized to
// destroy backend objects (its very existence is gated on
// reconcile.delete.enabled), so it must get the same controller-only seccomp
// hardening as the controller Deployment — before the fix it applied only the
// bare shared podSecurityContext, making it the one controller-image pod left
// running seccomp-unconfined.
func TestChartReconcileCronJobPodSeccompDefault(t *testing.T) {
	rendered := helmTemplate(t, "--set", "reconcile.delete.enabled=true")
	manifests := decodeManifests(t, rendered)

	cronJob := findManifest(t, manifests, "CronJob", "-reconcile")
	context := cronJobPodSecurityContextOf(t, cronJob, "reconcile")
	if context == nil {
		t.Fatal("reconcile CronJob pod rendered no pod-level securityContext")
	}
	if got, ok := seccompProfileType(context); !ok || got != "RuntimeDefault" {
		t.Errorf("reconcile CronJob pod must default to seccompProfile RuntimeDefault (same as the controller Deployment); got %#v", context["seccompProfile"])
	}
	// The controller-only seccomp default must MERGE with (not replace) the
	// shared podSecurityContext defaults, exactly like the controller pod.
	if got, ok := context["runAsNonRoot"].(bool); !ok || got {
		t.Errorf("reconcile CronJob pod lost the shared runAsNonRoot: false default; got %#v", context["runAsNonRoot"])
	}
	if got, ok := context["fsGroup"].(int); !ok || got != 0 {
		t.Errorf("reconcile CronJob pod lost the shared fsGroup: 0 default; got %#v", context["fsGroup"])
	}
}

// TestChartControllerPodSeccompOverride proves the controller seccomp default
// is operator-overridable like the other securityContext values: a
// controller.podSecurityContext override wins over the default, and a shared
// podSecurityContext override still merges through underneath it (and reaches
// the node pod untouched).
func TestChartControllerPodSeccompOverride(t *testing.T) {
	valuesPath := writeValues(t, "seccomp-override.yaml", `podSecurityContext:
  runAsNonRoot: false
  fsGroup: 1000
controller:
  podSecurityContext:
    seccompProfile:
      type: Unconfined
`)
	rendered := helmTemplate(t, "-f", valuesPath)
	manifests := decodeManifests(t, rendered)

	controller := findManifest(t, manifests, "Deployment", "-controller")
	controllerContext := podSecurityContextOf(t, controller, "controller")
	if controllerContext == nil {
		t.Fatal("controller pod rendered no pod-level securityContext")
	}
	if got, ok := seccompProfileType(controllerContext); !ok || got != "Unconfined" {
		t.Errorf("controller.podSecurityContext.seccompProfile override did not render; got %#v", controllerContext["seccompProfile"])
	}
	if got, ok := controllerContext["fsGroup"].(int); !ok || got != 1000 {
		t.Errorf("shared podSecurityContext override did not merge under the controller override; got %#v", controllerContext["fsGroup"])
	}

	node := findManifest(t, manifests, "DaemonSet", "-node")
	nodeContext := podSecurityContextOf(t, node, "node")
	if nodeContext == nil {
		t.Fatal("node pod rendered no pod-level securityContext")
	}
	if got, ok := nodeContext["fsGroup"].(int); !ok || got != 1000 {
		t.Errorf("shared podSecurityContext override did not reach the node pod; got %#v", nodeContext["fsGroup"])
	}
	if _, present := nodeContext["seccompProfile"]; present {
		t.Errorf("controller-only seccomp override leaked into the node pod: %#v", nodeContext["seccompProfile"])
	}
}
