package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Locked-volume unlock reconciler (GF-Sprint 1, E-2 §4).
//
// THE HONEST AVAILABILITY STORY. Because TrueNAS does not persist a passphrase
// (P-3: key_present_in_database:false), a nas01 reboot brings EVERY encrypted
// volume up LOCKED, and CSI does NOT re-issue ControllerPublish for
// already-attached volumes — so running pods see EIO until something re-unlocks
// the dataset. This reconciler is that something: on startup and on every
// reconcile cadence it re-unlocks locked encrypted managed datasets, and it
// honors the same two-key rotation window the publish path does (a reboot
// inside an open window must not need a human).
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

// encryptionUnlockCallTimeout bounds ONE backend call, not the whole pass. A
// pass-wide deadline shared by every call made every later volume's call fail
// instantly once it expired, silently, which is exactly how a partially
// converged pass looked identical to a complete one.
const encryptionUnlockCallTimeout = 30 * time.Second

// encryptionUnlockPassDeadline bounds the whole pass so a large or slow fleet
// cannot hold the reconcile loop indefinitely. Hitting it TRUNCATES the pass,
// which is logged with the carry-over count — never silently.
const encryptionUnlockPassDeadline = 5 * time.Minute

// encryptionUnlockMaxVolumesPerPass caps how many volumes one pass will act on
// (an unlock or a rotation completion — skips and healthy volumes are free).
// Each action is a @job on the appliance; unbounded fan-out after a reboot of a
// large fleet would flood the middleware. The remainder carries over to the next
// pass and the carry-over is logged. A var so tests can shrink it.
var encryptionUnlockMaxVolumesPerPass = 25

// encryptionUnlockPerVolumeDelay paces the acting volumes so a pass cannot
// dispatch its whole budget of unlock jobs in a burst. It is a var so tests can
// zero it (the same pattern as startupReconcileInitialBackoff).
var encryptionUnlockPerVolumeDelay = 250 * time.Millisecond

// encryptionUnlockOutcome classifies what one candidate did in one pass, so the
// pass summary can distinguish "nothing to do" from "skipped everything".
type encryptionUnlockOutcome int

const (
	// encryptionOutcomeHealthy: the volume is unlocked and needs nothing.
	encryptionOutcomeHealthy encryptionUnlockOutcome = iota
	// encryptionOutcomeForeign: the candidate carries no LOCAL encryption stamp on
	// a source-bearing re-read — a clone-inherited marker or a foreign dataset.
	// Never unlocked: it is not this volume's own encryption policy (P-7).
	encryptionOutcomeForeign
	// encryptionOutcomeBusy: a publish holds the per-volume operation lock and is
	// already handling this volume.
	encryptionOutcomeBusy
	// encryptionOutcomeRaced: the volume was unlocked by someone else between the
	// summary read and the action. Benign, never a failure (R4).
	encryptionOutcomeRaced
	// encryptionOutcomeUnlocked: this pass re-unlocked a locked volume.
	encryptionOutcomeUnlocked
	// encryptionOutcomeRotated: this pass completed an open rotation window.
	encryptionOutcomeRotated
	// encryptionOutcomeFailed: the volume is still locked after this pass.
	encryptionOutcomeFailed
)

// acts reports whether an outcome consumed a slot of the per-pass action budget
// (i.e. issued a backend @job that changes key state).
func (o encryptionUnlockOutcome) acts() bool {
	return o == encryptionOutcomeUnlocked || o == encryptionOutcomeRotated || o == encryptionOutcomeFailed
}

// reconcileEncryptedUnlocks re-unlocks every locked encrypted managed dataset
// and completes any interrupted key rotation. It is gated on the controller-wide
// encryption feature and on in-cluster clientset access (it must read K8s
// Secrets). It is idempotent (gated on the summary's locked==true, P-8),
// BOUNDED (per-pass action budget + per-call timeouts + a pass deadline, all of
// which log their carry-over rather than truncating silently), and redacted (no
// passphrase reaches a log or Event). Called from the controller reconcile loop
// at startup and on every cadence, and once before the startup share-ensure pass
// so shares are never rebuilt over a locked, device-less zvol.
func (d *Driver) reconcileEncryptedUnlocks(ctx context.Context) {
	if d.config == nil || !d.config.Encryption.Enabled {
		return
	}
	// The reconciler needs the CLIENTSET only (PVs, StorageClasses, Secrets); it
	// never touches the dynamic client. Gating on both would silently disable
	// R1's only mitigation on a cluster where the dynamic client failed to build.
	clientset, err := d.encryptionReconcileClient()
	if err != nil {
		d.warnEncryptionReconcilerInert(err)
		return
	}

	passCtx, cancel := context.WithTimeout(ctx, encryptionUnlockPassDeadline)
	defer cancel()

	listCtx, listCancel := context.WithTimeout(passCtx, encryptionUnlockCallTimeout)
	datasets, err := d.listAllManagedDatasets(listCtx)
	listCancel()
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Encryption unlock reconcile could not list managed datasets: %v", err)
		}
		return
	}

	// CANDIDATE SELECTION IS SOURCE-BLIND ON PURPOSE. listAllManagedDatasets
	// prefers zfs.resource.query, which returns user_properties as a FLAT map
	// with NO per-property source, so every property on that path parses with
	// Source=="" and any source=="local" test rejects 100% of datasets. Selecting
	// candidates with a source-gated read here made the entire reconciler a silent
	// no-op. The local-source OWNERSHIP requirement is not dropped — it moves to a
	// per-candidate, source-bearing pool.dataset.query confirmation
	// (confirmEncryptionOwner) taken only for candidates that actually need an
	// action, which is rare (locked, or an open rotation window).
	candidates := make([]string, 0, len(datasets))
	for _, ds := range datasets {
		if encryptionUnlockCandidate(ds) {
			candidates = append(candidates, ds.Name)
		}
	}

	resolver := &encryptionSecretResolver{
		d:              d,
		clientset:      clientset,
		storageClasses: map[string]*storagev1.StorageClass{},
		secrets:        map[string]*corev1.Secret{},
	}

	counts := map[encryptionUnlockOutcome]int{}
	acted, processed := 0, 0
	for _, datasetName := range candidates {
		if passCtx.Err() != nil || acted >= encryptionUnlockMaxVolumesPerPass {
			break
		}
		if acted > 0 && encryptionUnlockPerVolumeDelay > 0 {
			select {
			case <-passCtx.Done():
			case <-time.After(encryptionUnlockPerVolumeDelay):
			}
		}
		outcome := d.reconcileEncryptedUnlockOne(passCtx, resolver, datasetName)
		counts[outcome]++
		processed++
		if outcome.acts() {
			acted++
		}
	}

	remaining := len(candidates) - processed
	if remaining > 0 && ctx.Err() == nil {
		reason := "per-pass action budget"
		if passCtx.Err() != nil {
			reason = "pass deadline"
		}
		klog.Warningf("Encryption unlock reconcile truncated after %d/%d candidate volumes (%s, budget=%d, deadline=%v); "+
			"the remaining %d carry over to the next pass",
			processed, len(candidates), reason, encryptionUnlockMaxVolumesPerPass, encryptionUnlockPassDeadline, remaining)
	}
	if counts[encryptionOutcomeUnlocked] > 0 || counts[encryptionOutcomeFailed] > 0 ||
		counts[encryptionOutcomeRotated] > 0 || counts[encryptionOutcomeForeign] > 0 {
		klog.Infof("Encryption unlock reconcile complete: candidates=%d reUnlocked=%d rotationsCompleted=%d "+
			"stillLockedOrFailed=%d skippedForeign=%d skippedBusy=%d benignRaces=%d truncated=%d",
			len(candidates), counts[encryptionOutcomeUnlocked], counts[encryptionOutcomeRotated],
			counts[encryptionOutcomeFailed], counts[encryptionOutcomeForeign], counts[encryptionOutcomeBusy],
			counts[encryptionOutcomeRaced], remaining)
	} else {
		klog.V(4).Infof("Encryption unlock reconcile complete: candidates=%d, nothing to do", len(candidates))
	}
}

// encryptionUnlockCandidate is the cheap, SOURCE-BLIND pre-filter applied to the
// bulk managed-dataset listing. It must not use any source-gated accessor: the
// zfs.resource.query path strips user-property sources (see mock_client.go's
// probe-confirmed note), so a source==local test here rejects everything. A
// candidate is anything that carries the encryption stamp at all, or that the
// backend itself reports encrypted when the listing carries that field.
// Ownership is confirmed later, per candidate, with a source-bearing read.
func encryptionUnlockCandidate(ds *truenas.Dataset) bool {
	return datasetUserPropertyHasValue(ds, PropEncryption) || datasetEncryptedOnWire(ds)
}

// reconcileEncryptedUnlockOne converges ONE candidate volume. It takes the same
// per-volume operation lock ControllerPublishVolume holds, so a publish and this
// reconciler can never issue overlapping unlock/change_key jobs on one dataset
// (which, on an already-unlocked dataset, is a FAILED job by P-8 and used to be
// reported as an unhealthy volume — R4).
func (d *Driver) reconcileEncryptedUnlockOne(
	ctx context.Context,
	resolver *encryptionSecretResolver,
	datasetName string,
) encryptionUnlockOutcome {
	volumeID := d.volumeIDForDataset(datasetName)

	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		// A publish (or another controller operation) owns this volume right now
		// and is handling the unlock itself. Not a failure.
		klog.V(4).Infof("Encryption unlock reconcile: volume %s is busy with another operation; skipping this pass", volumeID)
		return encryptionOutcomeBusy
	}
	defer d.releaseOperationLock(lockKey)

	summaryCtx, cancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	summary, summaryErr := d.truenasClient.DatasetEncryptionSummary(summaryCtx, datasetName)
	cancel()
	if summaryErr != nil {
		if ctx.Err() == nil {
			klog.Warningf("Encryption unlock reconcile: could not read encryption summary for %s: %v", volumeID, summaryErr)
		}
		d.noteEncryptionUnlockFailure(volumeID, "read encryption summary")
		return encryptionOutcomeFailed
	}
	locked, lockedErr := encryptionSummaryLocked(summary, datasetName)
	if lockedErr != nil {
		// Fail closed (F17): an unreadable lock state is not evidence of health.
		klog.Warningf("Encryption unlock reconcile: cannot determine the lock state of %s: %v", volumeID, lockedErr)
		d.noteEncryptionUnlockFailure(volumeID, "unreadable encryption summary")
		return encryptionOutcomeFailed
	}

	if !locked {
		d.clearEncryptionUnlockFailure(volumeID)
		return d.completeInterruptedRotation(ctx, resolver, datasetName, volumeID)
	}

	// Source-bearing OWNERSHIP confirmation before touching a locked dataset. The
	// listing that produced this candidate has no property sources, so this is the
	// read that proves the encryption stamp is LOCAL to this dataset — i.e. that
	// the volume's encryption is its own and not inherited from a clone origin
	// (P-7). Paid only for candidates that need an action.
	fresh, confirmed := d.confirmEncryptionOwner(ctx, datasetName, volumeID)
	if !confirmed {
		return encryptionOutcomeForeign
	}
	if fresh != nil && !fresh.Locked {
		// Raced: a publish unlocked it between the summary read and this read.
		d.clearEncryptionUnlockFailure(volumeID)
		return encryptionOutcomeRaced
	}

	keys, resolveErr := resolver.keysFor(ctx, volumeID)
	if resolveErr != nil || keys.Passphrase == "" {
		// Redacted: name the volume and the reason, never a credential.
		klog.Warningf("Encryption unlock reconcile: volume %s is locked but no unlock passphrase could be resolved: %v",
			volumeID, resolveErr)
		d.noteEncryptionUnlockFailure(volumeID, "no resolvable unlock passphrase")
		return encryptionOutcomeFailed
	}

	callCtx, callCancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	convergeErr := d.convergeEncryptedDatasetKey(callCtx, datasetName, volumeID, true, keys)
	callCancel()
	if convergeErr != nil {
		// R4/TOCTOU: a lost race with a publish looks exactly like a failed unlock
		// (P-8 turns an unlock of an already-unlocked dataset into a FAILED job).
		// Re-read before calling a healthy, serving volume dead.
		if d.datasetObservedUnlocked(ctx, datasetName) {
			d.clearEncryptionUnlockFailure(volumeID)
			klog.V(4).Infof("Encryption unlock reconcile: volume %s was unlocked concurrently; treating the failed "+
				"unlock as benign", volumeID)
			return encryptionOutcomeRaced
		}
		klog.Warningf("Encryption unlock reconcile: failed to unlock locked volume %s (wrong key or backend error): %s",
			volumeID, redactEncryptionError(convergeErr, keys.Passphrase, keys.Previous))
		d.noteEncryptionUnlockFailure(volumeID, "unlock failed")
		return encryptionOutcomeFailed
	}
	d.clearEncryptionUnlockFailure(volumeID)
	if keys.rotationIntent() {
		d.markEncryptionRotationConverged(volumeID, keys)
	}
	klog.Infof("Encryption unlock reconcile: re-unlocked locked encrypted volume %s", volumeID)
	return encryptionOutcomeUnlocked
}

// completeInterruptedRotation handles the UNLOCKED arm. An unlocked dataset is
// serving I/O, so this is not an availability path — it is the R2 (permanent key
// loss) path. A controller killed between unlock(previous) and change_key leaves
// the volume unlocked but still keyed to the PREVIOUS passphrase while the
// operator believes rotation completed; when they then drop passphrasePrevious,
// the next lock is terminal. So while a rotation window is open, this converges
// the key with change_key(current) — a no-op by outcome when the volume is
// already on the current key (probed live 2026-08-02, see
// convergeEncryptedDatasetKey) — exactly ONCE per window per volume, tracked by
// a passphrase fingerprint so a later window converges again but a steady-state
// open window does not re-key on every pass.
func (d *Driver) completeInterruptedRotation(
	ctx context.Context,
	resolver *encryptionSecretResolver,
	datasetName, volumeID string,
) encryptionUnlockOutcome {
	keys, err := resolver.keysFor(ctx, volumeID)
	if err != nil || !keys.rotationIntent() {
		// A healthy unlocked volume whose key cannot be resolved is NOT a failure:
		// nothing is broken, and there is nothing to do.
		return encryptionOutcomeHealthy
	}
	if d.encryptionRotationConvergedFor(volumeID, keys) {
		return encryptionOutcomeHealthy
	}
	fresh, confirmed := d.confirmEncryptionOwner(ctx, datasetName, volumeID)
	if !confirmed {
		return encryptionOutcomeForeign
	}
	if fresh != nil && fresh.Locked {
		// It locked between the summary read and now; the next pass takes the
		// locked arm.
		return encryptionOutcomeRaced
	}
	callCtx, cancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	convergeErr := d.convergeEncryptedDatasetKey(callCtx, datasetName, volumeID, false, keys)
	cancel()
	if convergeErr != nil {
		klog.Warningf("Encryption unlock reconcile: could not converge the open rotation window for volume %s: %s",
			volumeID, redactEncryptionError(convergeErr, keys.Passphrase, keys.Previous))
		return encryptionOutcomeFailed
	}
	d.markEncryptionRotationConverged(volumeID, keys)
	klog.Infof("Encryption unlock reconcile: completed the open key rotation for volume %s", volumeID)
	return encryptionOutcomeRotated
}

// confirmEncryptionOwner re-reads a candidate through pool.dataset.query (the
// SOURCE-BEARING read path) and reports whether its encryption stamp is LOCAL to
// this dataset. A clone-inherited stamp reports the ORIGIN SNAPSHOT as its
// source, never "local", so a clone can never be unlocked as if it owned its
// key. A read error is treated as unconfirmed: never act on a dataset whose
// ownership could not be proven.
func (d *Driver) confirmEncryptionOwner(ctx context.Context, datasetName, volumeID string) (*truenas.Dataset, bool) {
	callCtx, cancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	defer cancel()
	fresh, err := d.truenasClient.DatasetGet(callCtx, datasetName)
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Encryption unlock reconcile: could not confirm encryption ownership of %s: %v", volumeID, err)
		}
		return nil, false
	}
	if !isEncryptedDataset(fresh) {
		klog.V(4).Infof("Encryption unlock reconcile: %s carries no LOCAL encryption stamp (inherited or foreign); "+
			"it is not this driver's key to load", volumeID)
		return fresh, false
	}
	return fresh, true
}

// datasetObservedUnlocked re-reads the encryption summary and reports a
// definitive unlocked observation. Anything else (error, ambiguity, locked) is
// false, so a failed unlock is only ever excused by positive evidence.
func (d *Driver) datasetObservedUnlocked(ctx context.Context, datasetName string) bool {
	callCtx, cancel := context.WithTimeout(ctx, encryptionUnlockCallTimeout)
	defer cancel()
	summary, err := d.truenasClient.DatasetEncryptionSummary(callCtx, datasetName)
	if err != nil {
		return false
	}
	locked, lockedErr := encryptionSummaryLocked(summary, datasetName)
	return lockedErr == nil && !locked
}

// encryptionReconcileClient returns the Kubernetes clientset the unlock
// reconciler needs. Unlike kubernetesReconcileClients it does NOT require the
// dynamic client: this reconciler reads PVs, StorageClasses and Secrets only,
// and gating it on an unrelated client is how R1's mitigation gets silently
// disabled in a cluster.
func (d *Driver) encryptionReconcileClient() (kubernetes.Interface, error) {
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil {
		return nil, fmt.Errorf("kubernetes client access is unavailable; the unlock reconciler must read Secrets")
	}
	return d.eventRecorder.clientset, nil
}

// warnEncryptionReconcilerInert logs ONCE that encryption is enabled but the
// unlock reconciler cannot run. Outside a cluster (unit tests, the node-only
// plugin) this is expected and stays a single line; in a controller it is the
// difference between "R1 is mitigated" and "R1 is not mitigated", which must
// never be silent.
func (d *Driver) warnEncryptionReconcilerInert(cause error) {
	d.encryptionUnlockFailMu.Lock()
	warned := d.encryptionReconcilerInertWarned
	d.encryptionReconcilerInertWarned = true
	d.encryptionUnlockFailMu.Unlock()
	if warned {
		return
	}
	klog.Warningf("Encryption is enabled but the locked-volume unlock reconciler cannot run (%v). Encrypted volumes "+
		"will NOT be re-unlocked automatically after a TrueNAS reboot; their pods stay in I/O error until an operator "+
		"unlocks them.", cause)
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

// encryptionSecretResolver resolves a locked volume's unlock keys from its
// owning StorageClass's controller-publish secret. StorageClasses, Secrets and
// the PV index are cached per pass, so a fleet-wide reboot costs one PV list plus
// one Get per distinct StorageClass and Secret — not per volume.
type encryptionSecretResolver struct {
	d              *Driver
	clientset      kubernetes.Interface
	storageClasses map[string]*storagev1.StorageClass
	secrets        map[string]*corev1.Secret
	pvByHandle     map[string]string // volume handle -> PV name
	pvsListed      bool
}

// keysFor returns the unlock keys for a volume: PV (by volume handle) ->
// StorageClass -> controller-publish-secret ref -> Secret -> passphrase (+
// passphrasePrevious, the open rotation window). Any missing link is an error
// (logged redacted by the caller). Both keys come from the SAME Secret the
// publish path uses, so the two callers can never disagree about the window.
func (r *encryptionSecretResolver) keysFor(ctx context.Context, volumeID string) (encryptionKeys, error) {
	secret, err := r.secretFor(ctx, volumeID)
	if err != nil {
		return encryptionKeys{}, err
	}
	keys := encryptionKeys{
		Passphrase: string(secret.Data[encryptionSecretKeyPassphrase]),
		Previous:   string(secret.Data[encryptionSecretKeyPassphrasePrevious]),
	}
	if keys.Passphrase == "" {
		return encryptionKeys{}, fmt.Errorf("unlock secret %s/%s has no %q key",
			secret.Namespace, secret.Name, encryptionSecretKeyPassphrase)
	}
	return keys, nil
}

func (r *encryptionSecretResolver) secretFor(ctx context.Context, volumeID string) (*corev1.Secret, error) {
	pvName, err := r.pvNameForHandle(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if pvName == "" {
		return nil, fmt.Errorf("no PersistentVolume has volume handle %q", volumeID)
	}
	pv, err := r.clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get PersistentVolume %s: %w", pvName, err)
	}
	scName := pv.Spec.StorageClassName
	if scName == "" {
		return nil, fmt.Errorf("PersistentVolume %s has no StorageClass", pvName)
	}
	sc, err := r.storageClass(ctx, scName)
	if err != nil {
		return nil, err
	}
	secretName := sc.Parameters[csiControllerPublishSecretNameParam]
	secretNamespace := sc.Parameters[csiControllerPublishSecretNamespaceParam]
	if secretName == "" || secretNamespace == "" {
		return nil, fmt.Errorf("StorageClass %s does not name a controller-publish secret for unlock", scName)
	}
	cacheKey := secretNamespace + "/" + secretName
	if cached, ok := r.secrets[cacheKey]; ok {
		return cached, nil
	}
	secret, err := r.clientset.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("unlock secret %s not found (lost key = unrecoverable data, R2)", cacheKey)
		}
		return nil, fmt.Errorf("get unlock secret %s: %w", cacheKey, err)
	}
	r.secrets[cacheKey] = secret
	return secret, nil
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

// encryptionRotationFingerprint identifies a rotation WINDOW without holding key
// material: a salted SHA-256 over the two passphrases. It is never logged, never
// stamped and never leaves the process; it exists so a converged window is not
// re-converged on every reconcile pass while a NEW window (different keys) still
// is. Same construction as the CHAP credential fingerprint.
func encryptionRotationFingerprint(keys encryptionKeys) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{"gf1-rotation", keys.Passphrase, keys.Previous}, "\x00")))
	return hex.EncodeToString(sum[:])
}

// encryptionRotationConvergedFor reports whether THIS rotation window has already
// been converged for this volume in this process.
func (d *Driver) encryptionRotationConvergedFor(volumeID string, keys encryptionKeys) bool {
	fingerprint := encryptionRotationFingerprint(keys)
	d.encryptionUnlockFailMu.Lock()
	defer d.encryptionUnlockFailMu.Unlock()
	return d.encryptionRotationConverged[volumeID] == fingerprint
}

func (d *Driver) markEncryptionRotationConverged(volumeID string, keys encryptionKeys) {
	fingerprint := encryptionRotationFingerprint(keys)
	d.encryptionUnlockFailMu.Lock()
	defer d.encryptionUnlockFailMu.Unlock()
	if d.encryptionRotationConverged == nil {
		d.encryptionRotationConverged = make(map[string]string)
	}
	d.encryptionRotationConverged[volumeID] = fingerprint
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
