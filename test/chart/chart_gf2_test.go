package chart

import "testing"

func workloadContainer(t *testing.T, workload manifest, name string) manifest {
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
	containers, ok := podSpec["containers"].([]any)
	if !ok {
		t.Fatalf("%s workload pod has no containers", name)
	}
	for _, item := range containers {
		container, ok := asManifest(item)
		if !ok {
			continue
		}
		if container["name"] == name {
			return container
		}
	}
	t.Fatalf("workload does not contain container %q", name)
	return nil
}

func securityContext(t *testing.T, container manifest) manifest {
	t.Helper()
	context, ok := asManifest(container["securityContext"])
	if !ok {
		t.Fatalf("container %q has no securityContext", container["name"])
	}
	return context
}

func assertHardenedSidecar(t *testing.T, container manifest, expectNonRoot bool) {
	t.Helper()
	context := securityContext(t, container)
	if got, ok := context["allowPrivilegeEscalation"].(bool); !ok || got {
		t.Errorf("container %q must disable privilege escalation; got %#v", container["name"], context["allowPrivilegeEscalation"])
	}
	if got, ok := context["readOnlyRootFilesystem"].(bool); !ok || !got {
		t.Errorf("container %q must use a read-only root filesystem; got %#v", container["name"], context["readOnlyRootFilesystem"])
	}
	capabilities, ok := asManifest(context["capabilities"])
	if !ok || !equalStrings(asStringSlice(capabilities["drop"]), []string{"ALL"}) {
		t.Errorf("container %q must drop ALL capabilities; got %#v", container["name"], context["capabilities"])
	}
	if expectNonRoot {
		if got, ok := context["runAsNonRoot"].(bool); !ok || !got {
			t.Errorf("container %q must run as non-root; got %#v", container["name"], context["runAsNonRoot"])
		}
		if got, ok := context["runAsUser"].(int); !ok || got != 65532 {
			t.Errorf("container %q must use the verified non-root UID 65532; got %#v", container["name"], context["runAsUser"])
		}
	}
}

// TestChartGF2SidecarSecurityContexts guards the per-container boundary: the
// hardened baseline belongs to sidecars only, while the node driver's existing
// privileged/SYS_ADMIN context remains untouched for mount and block-device work.
func TestChartGF2SidecarSecurityContexts(t *testing.T) {
	rendered := helmTemplate(t, "--set", "sidecars.healthMonitor.enabled=true")
	manifests := decodeManifests(t, rendered)
	controller := findManifest(t, manifests, "Deployment", "-controller")
	node := findManifest(t, manifests, "DaemonSet", "-node")

	for _, name := range []string{
		"csi-provisioner",
		"csi-attacher",
		"csi-resizer",
		"csi-snapshotter",
		"liveness-probe",
		"csi-external-health-monitor",
	} {
		assertHardenedSidecar(t, workloadContainer(t, controller, name), true)
	}
	assertHardenedSidecar(t, workloadContainer(t, node, "liveness-probe"), true)
	// The registrar writes its registration socket into the mounted, root-owned
	// hostPath. Its image and deployment therefore deliberately do not claim
	// runAsNonRoot, but the common filesystem/capability hardening still applies.
	registrar := workloadContainer(t, node, "csi-node-driver-registrar")
	assertHardenedSidecar(t, registrar, false)
	registrarContext := securityContext(t, registrar)
	if _, present := registrarContext["runAsNonRoot"]; present {
		t.Errorf("registrar must not claim runAsNonRoot with the root-owned registration hostPath")
	}

	controllerDriver := securityContext(t, workloadContainer(t, controller, "scale-csi"))
	if got, ok := controllerDriver["privileged"].(bool); ok && got {
		t.Errorf("controller scale-csi container unexpectedly became privileged")
	}
	nodeDriver := securityContext(t, workloadContainer(t, node, "scale-csi"))
	if got, ok := nodeDriver["privileged"].(bool); !ok || !got {
		t.Errorf("node scale-csi container lost its intentional privileged context; got %#v", nodeDriver["privileged"])
	}
	nodeCapabilities, ok := asManifest(nodeDriver["capabilities"])
	if !ok || !equalStrings(asStringSlice(nodeCapabilities["add"]), []string{"SYS_ADMIN"}) {
		t.Errorf("node scale-csi container lost SYS_ADMIN; got %#v", nodeDriver["capabilities"])
	}
}

// TestChartGF2SidecarSecurityContextOverrides proves operators can loosen the
// common policy and then override just one sidecar without repeating the whole
// baseline. Nested capabilities are checked too, because a shallow merge would
// silently discard the common map.
func TestChartGF2SidecarSecurityContextOverrides(t *testing.T) {
	valuesPath := writeValues(t, "gf2-sidecar-security.yaml", `sidecars:
  securityContext:
    allowPrivilegeEscalation: true
    readOnlyRootFilesystem: false
    capabilities:
      drop:
        - NET_RAW
  provisioner:
    securityContext:
      readOnlyRootFilesystem: true
      runAsNonRoot: false
`)
	rendered := helmTemplate(t, "-f", valuesPath)
	manifests := decodeManifests(t, rendered)
	controller := findManifest(t, manifests, "Deployment", "-controller")

	attacher := securityContext(t, workloadContainer(t, controller, "csi-attacher"))
	if got := attacher["allowPrivilegeEscalation"]; got != true {
		t.Errorf("common allowPrivilegeEscalation override did not render: %#v", got)
	}
	if got := attacher["readOnlyRootFilesystem"]; got != false {
		t.Errorf("common readOnlyRootFilesystem override did not render: %#v", got)
	}
	attacherCapabilities, ok := asManifest(attacher["capabilities"])
	if !ok || !equalStrings(asStringSlice(attacherCapabilities["drop"]), []string{"NET_RAW"}) {
		t.Errorf("common nested capabilities override did not render: %#v", attacher["capabilities"])
	}

	provisioner := securityContext(t, workloadContainer(t, controller, "csi-provisioner"))
	if got := provisioner["allowPrivilegeEscalation"]; got != true {
		t.Errorf("provisioner did not inherit the common override: %#v", got)
	}
	if got := provisioner["readOnlyRootFilesystem"]; got != true {
		t.Errorf("provisioner-specific readOnlyRootFilesystem override did not render: %#v", got)
	}
	if got := provisioner["runAsNonRoot"]; got != false {
		t.Errorf("provisioner-specific runAsNonRoot override did not render: %#v", got)
	}
}
