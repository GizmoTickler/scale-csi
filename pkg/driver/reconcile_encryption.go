package driver

import (
	"context"
	"fmt"
	"time"

	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Locked-volume unlock reconciler (GF-Sprint 1, E-2 §4).
//
// THE HONEST AVAILABILITY STORY. Because TrueNAS does not persist a passphrase
// (P-3: key_present_in_database:false), a nas01 reboot brings EVERY encrypted
// volume up LOCKED, and CSI does NOT re-issue ControllerPublish for
// already-attached volumes — so running pods see EIO until something re-unlocks
// the dataset. This reconciler is that something: on startup and on every
// reconcile cadence it re-unlocks locked encrypted managed datasets.
//
// This is a BEST-EFFORT RECONVERGENCE, NOT A GUARANTEE. Recovery latency is the
// reconcile interval; during the window, encrypted-volume pods take I/O errors.
// Pair encrypted StorageClasses with pod liveness/restart so they re-stage after
// unlock. An operator who cannot tolerate that window should not encrypt.

// CSI StorageClass parameters that name the controller-publish secret (the same
// secret ControllerPublishVolume unlocks with). The chart renders these from a
// StorageClass's encryptionSecretName/encryptionSecretNamespace. The reconciler
// reads the passphrase from this secret; it never logs it.
const (
	csiControllerPublishSecretNameParam      = "csi.storage.k8s.io/controller-publish-secret-name"
	csiControllerPublishSecretNamespaceParam = "csi.storage.k8s.io/controller-publish-secret-namespace"
)

// encryptionUnlockEventThreshold is how many consecutive failed re-unlock passes
// a dataset must accumulate before the reconciler raises a K8s Event. A single
// transient backend blip stays a log line; only a PERSISTENT failure (no key,
// wrong key, backend down) becomes an operator-visible Event.
const encryptionUnlockEventThreshold = 2

// encryptionUnlockCallTimeout bounds one pass's backend reads so a hung call
// cannot stall the reconcile loop beyond a fixed window. Mirrors the
// backend-health and capacity loops' guard.
const encryptionUnlockCallTimeout = 30 * time.Second

// reconcileEncryptedUnlocks re-unlocks every locked encrypted managed dataset.
// It is gated on the controller-wide encryption feature and on in-cluster client
// access (it must read K8s Secrets); without either it is a no-op. It is
// idempotent (gated on the summary's locked==true, P-8), bounded (one managed-
// dataset listing plus one PV/SC listing per pass), and redacted (no passphrase
// reaches a log or Event). Called from the controller reconcile loop at startup
// and on every cadence.
func (d *Driver) reconcileEncryptedUnlocks(ctx context.Context) {
	if d.config == nil || !d.config.Encryption.Enabled {
		return
	}
	clientset, _, err := d.kubernetesReconcileClients()
	if err != nil {
		// No in-cluster client => cannot read Secrets => cannot unlock. This is
		// expected outside a cluster (tests, the node-only plugin); stay silent.
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	defer cancel()

	datasets, err := d.listAllManagedDatasets(callCtx)
	if err != nil {
		if callCtx.Err() == nil {
			klog.Warningf("Encryption unlock reconcile could not list managed datasets: %v", err)
		}
		return
	}

	// Resolve the key-lookup state once per pass (bounded: one PV list, SCs and
	// Secrets fetched lazily per distinct StorageClass/secret).
	resolver := &encryptionSecretResolver{d: d, clientset: clientset, storageClasses: map[string]*storagev1.StorageClass{}}

	unlocked, failed := 0, 0
	for _, ds := range datasets {
		if !isEncryptedDataset(ds) {
			continue
		}
		datasetName := ds.Name
		volumeID := d.volumeIDForDataset(datasetName)

		summary, summaryErr := d.truenasClient.DatasetEncryptionSummary(callCtx, datasetName)
		if summaryErr != nil {
			if callCtx.Err() == nil {
				klog.Warningf("Encryption unlock reconcile: could not read encryption summary for %s: %v", volumeID, summaryErr)
			}
			d.noteEncryptionUnlockFailure(volumeID, "read encryption summary")
			failed++
			continue
		}
		if !encryptionSummaryLocked(summary, datasetName) {
			// Healthy (unlocked). Clear any failure streak so a recovered volume
			// stops alerting.
			d.clearEncryptionUnlockFailure(volumeID)
			continue
		}

		passphrase, resolveErr := resolver.resolvePassphrase(callCtx, volumeID)
		if resolveErr != nil || passphrase == "" {
			// Redacted: name the volume and the reason, never a credential.
			klog.Warningf("Encryption unlock reconcile: volume %s is locked but no unlock passphrase could be resolved: %v", volumeID, resolveErr)
			d.noteEncryptionUnlockFailure(volumeID, "no resolvable unlock passphrase")
			failed++
			continue
		}

		if unlockErr := d.truenasClient.DatasetUnlock(callCtx, datasetName, passphrase); unlockErr != nil {
			klog.Warningf("Encryption unlock reconcile: failed to unlock locked volume %s (wrong key or backend error): %v", volumeID, unlockErr)
			d.noteEncryptionUnlockFailure(volumeID, "unlock failed")
			failed++
			continue
		}
		d.clearEncryptionUnlockFailure(volumeID)
		unlocked++
		klog.Infof("Encryption unlock reconcile: re-unlocked locked encrypted volume %s", volumeID)
	}
	if unlocked > 0 || failed > 0 {
		klog.Infof("Encryption unlock reconcile complete: re-unlocked=%d stillLockedOrFailed=%d", unlocked, failed)
	}
}

// volumeIDForDataset is the inverse of datasetForID for reconcile reporting: it
// returns the CSI volume handle a dataset was provisioned from. The dataset name
// is the parent-joined sanitized id, so stripping the parent prefix recovers it.
// It is used only for log/Event identity, never for backend addressing.
func (d *Driver) volumeIDForDataset(datasetName string) string {
	parent := d.parentDatasetName()
	if parent != "" {
		if prefix := parent + "/"; len(datasetName) > len(prefix) && datasetName[:len(prefix)] == prefix {
			return datasetName[len(prefix):]
		}
	}
	return datasetName
}

// encryptionSecretResolver resolves a locked volume's unlock passphrase from its
// owning StorageClass's controller-publish secret. StorageClasses are cached per
// pass; PVs are listed once and indexed by volume handle.
type encryptionSecretResolver struct {
	d              *Driver
	clientset      kubernetes.Interface
	storageClasses map[string]*storagev1.StorageClass
	pvByHandle     map[string]string // volume handle -> PV name
	pvsListed      bool
}

// resolvePassphrase returns the unlock passphrase for a locked volume: PV (by
// volume handle) -> StorageClass -> controller-publish-secret ref -> Secret ->
// "passphrase". Any missing link is an error (logged redacted by the caller).
func (r *encryptionSecretResolver) resolvePassphrase(ctx context.Context, volumeID string) (string, error) {
	pvName, err := r.pvNameForHandle(ctx, volumeID)
	if err != nil {
		return "", err
	}
	if pvName == "" {
		return "", fmt.Errorf("no PersistentVolume has volume handle %q", volumeID)
	}
	pv, err := r.clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get PersistentVolume %s: %w", pvName, err)
	}
	scName := pv.Spec.StorageClassName
	if scName == "" {
		return "", fmt.Errorf("PersistentVolume %s has no StorageClass", pvName)
	}
	sc, err := r.storageClass(ctx, scName)
	if err != nil {
		return "", err
	}
	secretName := sc.Parameters[csiControllerPublishSecretNameParam]
	secretNamespace := sc.Parameters[csiControllerPublishSecretNamespaceParam]
	if secretName == "" || secretNamespace == "" {
		return "", fmt.Errorf("StorageClass %s does not name a controller-publish secret for unlock", scName)
	}
	secret, err := r.clientset.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("unlock secret %s/%s not found (lost key = unrecoverable data, R2)", secretNamespace, secretName)
		}
		return "", fmt.Errorf("get unlock secret %s/%s: %w", secretNamespace, secretName, err)
	}
	if raw, ok := secret.Data[encryptionSecretKeyPassphrase]; ok && len(raw) > 0 {
		return string(raw), nil
	}
	return "", fmt.Errorf("unlock secret %s/%s has no %q key", secretNamespace, secretName, encryptionSecretKeyPassphrase)
}

func (r *encryptionSecretResolver) pvNameForHandle(ctx context.Context, volumeID string) (string, error) {
	if !r.pvsListed {
		r.pvByHandle = map[string]string{}
		pvs, err := r.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return "", fmt.Errorf("list PersistentVolumes: %w", err)
		}
		for i := range pvs.Items {
			pv := &pvs.Items[i]
			if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == r.d.name && pv.Spec.CSI.VolumeHandle != "" {
				r.pvByHandle[pv.Spec.CSI.VolumeHandle] = pv.Name
			}
		}
		r.pvsListed = true
	}
	return r.pvByHandle[volumeID], nil
}

func (r *encryptionSecretResolver) storageClass(ctx context.Context, name string) (*storagev1.StorageClass, error) {
	if sc, ok := r.storageClasses[name]; ok {
		return sc, nil
	}
	sc, err := r.clientset.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get StorageClass %s: %w", name, err)
	}
	r.storageClasses[name] = sc
	return sc, nil
}

// noteEncryptionUnlockFailure records a consecutive re-unlock failure for a
// volume and raises a redacted K8s Event once the failure becomes persistent
// (encryptionUnlockEventThreshold consecutive passes). The message names the
// volume and the failure class; it never carries key material.
func (d *Driver) noteEncryptionUnlockFailure(volumeID, reason string) {
	d.encryptionUnlockFailMu.Lock()
	if d.encryptionUnlockFailures == nil {
		d.encryptionUnlockFailures = make(map[string]int)
	}
	d.encryptionUnlockFailures[volumeID]++
	count := d.encryptionUnlockFailures[volumeID]
	d.encryptionUnlockFailMu.Unlock()
	if count != encryptionUnlockEventThreshold {
		return
	}
	d.recordWarningEvent(volumeEventRef(volumeID), EventReasonEncryptionUnlockFailed,
		fmt.Sprintf("Encrypted volume %s has stayed locked across %d reconcile passes (%s); its pods see I/O errors until it is re-unlocked",
			volumeID, count, reason))
}

// clearEncryptionUnlockFailure resets a volume's consecutive-failure streak once
// it is observed unlocked (or successfully re-unlocked), so a recovered volume
// stops alerting.
func (d *Driver) clearEncryptionUnlockFailure(volumeID string) {
	d.encryptionUnlockFailMu.Lock()
	delete(d.encryptionUnlockFailures, volumeID)
	d.encryptionUnlockFailMu.Unlock()
}
