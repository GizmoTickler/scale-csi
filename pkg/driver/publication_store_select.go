package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	// publicationStoreEnv names the store; the chart sets it on the controller.
	// An environment variable, not a config key: the chart's default ConfigMap
	// stays parseable by a rolled-back binary, which ignores the variable.
	publicationStoreEnv        = "SCALE_CSI_PUBLICATION_STORE"
	publicationStoreKubernetes = "kubernetes"
	publicationStoreZFS        = "zfs"
)

var (
	publicationStoreSetting = func() string { return strings.TrimSpace(os.Getenv(publicationStoreEnv)) }
	// podNamespace is the driver's own namespace: POD_NAMESPACE when set, else
	// the service account's.
	podNamespace = func() (string, error) {
		if namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); namespace != "" {
			return namespace, nil
		}
		raw, err := os.ReadFile(serviceAccountNamespaceFile)
		if err != nil {
			return "", fmt.Errorf("read the pod's namespace: %w", err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	publicationStoreProbeAttempts = 5
	publicationStoreProbeInterval = 2 * time.Second
)

// selectPublicationStore decides where the controller keeps publication
// records. With SCALE_CSI_PUBLICATION_STORE=kubernetes (the chart's setting)
// they are VolumePublications in the driver's namespace, and records an older
// release left on ZFS are imported; a missing CRD or Role then stops the
// controller rather than writing records where the next start would not look.
// Unset, or "zfs" (tests, a development run), they stay on ZFS.
func (d *Driver) selectPublicationStore(ctx context.Context) error {
	if !d.runController {
		return nil
	}
	switch setting := publicationStoreSetting(); setting {
	case "", publicationStoreZFS:
		klog.Info("Publication records are kept on ZFS")
		return nil
	case publicationStoreKubernetes:
	default:
		return fmt.Errorf("%s=%q: want %q or %q", publicationStoreEnv, setting, publicationStoreKubernetes, publicationStoreZFS)
	}
	if d.eventRecorder == nil || d.eventRecorder.dynamicClient == nil {
		return errors.New("publication records: no Kubernetes client, so VolumePublications cannot be kept")
	}
	namespace, err := podNamespace()
	if err != nil {
		return fmt.Errorf("publication records: the driver's namespace is unknown: %w", err)
	}
	if namespace == "" {
		return errors.New("publication records: the driver's namespace is empty")
	}
	store := kubernetesPublicationStore{client: d.eventRecorder.dynamicClient, namespace: namespace, instance: d.config.DriverInstanceID}
	for attempt := 1; ; attempt++ {
		_, err = store.resource().List(ctx, metav1.ListOptions{Limit: 1})
		switch {
		case err == nil:
			publicationCache, cacheErr := newPublicationCache(store)
			if cacheErr != nil {
				return fmt.Errorf("publication records: %w", cacheErr)
			}
			// Stop() sets serverStopped before it loads the reference: one of
			// the two sees the other and the watch is ended.
			d.publicationCacheRef.Store(publicationCache)
			d.serverStateMu.Lock()
			stopped := d.serverStopped
			d.serverStateMu.Unlock()
			if stopped {
				publicationCache.close()
			} else {
				publicationCache.start(ctx)
			}
			d.publicationStore = importingPublicationStore{kube: store, legacy: zfsPublicationStore{client: d.truenasClient}, cache: publicationCache}
			klog.Infof("Publication records are kept as VolumePublications in namespace %s", namespace)
			return nil
		case apierrors.IsNotFound(err):
			return fmt.Errorf("publication records: the VolumePublication CRD (scale-csi.io/v1alpha1) is not installed; "+
				"apply the chart's crds/ (with Flux, set install.crds and upgrade.crds to CreateReplace): %w", err)
		case apierrors.IsForbidden(err):
			return fmt.Errorf("publication records: the controller may not list VolumePublications in namespace %s; "+
				"the chart's controller-publications Role is missing: %w", namespace, err)
		case attempt >= publicationStoreProbeAttempts:
			return fmt.Errorf("publication records: VolumePublications could not be listed after %d attempts: %w", attempt, err)
		}
		klog.Warningf("Publication records: listing VolumePublications failed (attempt %d of %d): %v",
			attempt, publicationStoreProbeAttempts, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(publicationStoreProbeInterval):
		}
	}
}
