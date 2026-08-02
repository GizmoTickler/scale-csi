package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// The unlock reconciler is the ONLY mitigation for R1 (a TrueNAS reboot leaves
// every encrypted volume locked and serving zero I/O), and it shipped with no
// tests at all — which is how it shipped as a total, silent no-op. These tests
// drive it end to end: through listAllManagedDatasets (the real enumeration, on
// the source-stripped zfs.resource.query path), through the real PV -> SC ->
// Secret resolution, and against the mock's probe-faithful lock model.

const (
	encReconcileDriverName = "csi.scale.io"
	encReconcileParent     = "pool/parent"
	encReconcileSCName     = "encrypted-sc"
	encReconcileSecretNS   = "storage"
	encReconcileSecretName = "encryption-key"
)

// encryptionReconcileDriver builds a controller with in-cluster clientset access
// and NO dynamic client — the shape a controller has when the dynamic client
// failed to build. The unlock reconciler reads PVs, StorageClasses and Secrets
// only, so it must still run (F9).
func encryptionReconcileDriver(t *testing.T, coreObjects ...runtime.Object) (*Driver, *apiCallCountingClient, *record.FakeRecorder) {
	t.Helper()
	// Tests must not pay the production pacing delay.
	previousDelay := encryptionUnlockPerVolumeDelay
	encryptionUnlockPerVolumeDelay = 0
	t.Cleanup(func() { encryptionUnlockPerVolumeDelay = previousDelay })

	client := newAPICallCountingClient()
	recorder := record.NewFakeRecorder(64)
	d := &Driver{
		name: encReconcileDriverName,
		config: &Config{
			DriverName: encReconcileDriverName,
			ZFS:        ZFSConfig{DatasetParentName: encReconcileParent},
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
			Encryption: EncryptionConfig{Enabled: true},
		},
		truenasClient: client,
		eventRecorder: &EventRecorder{
			clientset: kubernetesfake.NewSimpleClientset(coreObjects...),
			recorder:  recorder,
			enabled:   true,
		},
	}
	return d, client, recorder
}

func encryptionReconcilePV(volumeID string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-" + volumeID},
		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: encReconcileSCName,
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       encReconcileDriverName,
					VolumeHandle: volumeID,
				},
			},
		},
	}
}

func encryptionReconcileSC() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: encReconcileSCName},
		Parameters: map[string]string{
			paramEncryption:                          "true",
			csiControllerPublishSecretNameParam:      encReconcileSecretName,
			csiControllerPublishSecretNamespaceParam: encReconcileSecretNS,
		},
	}
}

func encryptionReconcileSecret(passphrase, previous string) *corev1.Secret {
	data := map[string][]byte{encryptionSecretKeyPassphrase: []byte(passphrase)}
	if previous != "" {
		data[encryptionSecretKeyPassphrasePrevious] = []byte(previous)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: encReconcileSecretName, Namespace: encReconcileSecretNS},
		Data:       data,
	}
}

// addManagedEncryptedVolume creates an encrypted, CSI-managed, correctly stamped
// dataset (the state a real provisioned encrypted volume is in) and returns its
// dataset name.
func addManagedEncryptedVolume(t *testing.T, client *apiCallCountingClient, volumeID, passphrase string) string {
	t.Helper()
	name := encReconcileParent + "/" + volumeID
	createEncryptedDataset(t, client, name, passphrase)
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), name, PropManagedResource, "true"))
	require.NoError(t, client.DatasetSetUserProperty(context.Background(), name, PropCSIVolumeName, volumeID))
	return name
}

func eventsContainingReason(events []string, reason string) int {
	count := 0
	for _, event := range events {
		if strings.Contains(event, reason) {
			count++
		}
	}
	return count
}

// TestReconcileEncryptedUnlocksLockedVolume is the F1 regression: the reconciler
// must actually unlock. It goes through listAllManagedDatasets (whose primary
// path, zfs.resource.query, returns user properties with NO source), so a
// candidate filter that demands source=="local" on that listing skips 100% of
// volumes — which is exactly what shipped, silently.
//
// PRE-FIX PROOF: restoring the old candidate filter (`if !isEncryptedDataset(ds)
// { continue }` over the listing) makes this FAIL with DatasetUnlock count 0 and
// the dataset still locked.
func TestReconcileEncryptedUnlocksLockedVolume(t *testing.T) {
	const volumeID = "enc-vol-1"
	const passphrase = "unlock-me-123"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetUnlock"], "the reconciler must attempt exactly one unlock")
	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked, "the locked volume must be unlocked after one pass")
	assert.Empty(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed))
}

// TestReconcileEncryptedUnlockConfirmsLocalOwnership proves the fix did NOT drop
// the source==local ownership requirement while making enumeration work. A clone
// of an encrypted volume inherits BOTH the encryption (P-7: encrypted:true,
// encryption_root == the origin) and the user properties (source == the origin
// snapshot, never "local"). It is enumerated as a candidate — it must never be
// unlocked, because its key is not this volume's to load.
func TestReconcileEncryptedUnlockConfirmsLocalOwnership(t *testing.T) {
	const volumeID = "enc-origin"
	const cloneID = "enc-clone"
	const passphrase = "unlock-me-123"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(cloneID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	origin := addManagedEncryptedVolume(t, client, volumeID, passphrase)

	// Clone it (P-7 inheritance) and lock the origin, which locks the clone too.
	ctx := context.Background()
	_, err := client.SnapshotCreate(ctx, origin, "snap", nil)
	require.NoError(t, err)
	require.NoError(t, client.SnapshotClone(ctx, origin+"@snap", encReconcileParent+"/"+cloneID))
	clone, err := client.DatasetGet(ctx, encReconcileParent+"/"+cloneID)
	require.NoError(t, err)
	require.True(t, clone.Encrypted, "P-7: a clone of an encrypted dataset is encrypted")
	require.Equal(t, origin, clone.EncryptionRoot, "P-7: encryption_root is the ORIGIN, not the clone")
	require.Equal(t, "", datasetLocalUserProperty(clone, PropEncryption),
		"a clone-inherited stamp is not local and must not read as the clone's own policy")

	// Remove the origin's own PV so only the clone is resolvable, then lock.
	require.NoError(t, client.DatasetLock(ctx, origin))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(ctx)

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetUnlock"],
		"neither the clone (inherited key) nor an origin with no resolvable PV may be unlocked")
	assert.GreaterOrEqual(t, methods["DatasetGet"], 1,
		"ownership is confirmed with a source-bearing pool.dataset.query read")
	assert.Empty(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed),
		"a foreign/inherited dataset is a skip, never a failure")
}

// TestReconcileEncryptedUnlockHonorsRotationWindow is the F3 regression: a
// reboot INSIDE the rotation window. The dataset is still keyed to the previous
// passphrase; the Secret carries both. The reconciler is the only actor that
// runs for already-attached volumes, so it must use the same two-key window the
// publish path does — and then complete the rotation.
//
// PRE-FIX PROOF: with resolvePassphrase reading only secret.Data["passphrase"]
// and no change_key, this FAILS — one unlock attempted, dataset still locked,
// DatasetChangeKey count 0.
func TestReconcileEncryptedUnlockHonorsRotationWindow(t *testing.T) {
	const volumeID = "enc-rotating"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(newPass, oldPass))
	name := addManagedEncryptedVolume(t, client, volumeID, oldPass)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())

	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetChangeKey"], "the reconciler completes the rotation it opened")
	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked)

	// The volume now holds the NEW key and the OLD one is dead (P-6).
	require.NoError(t, client.DatasetLock(context.Background(), name))
	require.Error(t, client.DatasetUnlock(context.Background(), name, oldPass))
	require.NoError(t, client.DatasetUnlock(context.Background(), name, newPass))

	assert.Equal(t, 1, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionRotated))
}

// TestReconcileCompletesInterruptedRotation is the reconciler half of the F2
// regression. The controller died between unlock(previous) and change_key: the
// dataset is UNLOCKED and serving I/O, but still keyed to the PREVIOUS
// passphrase while the operator believes rotation completed. Nothing else in the
// system will ever notice — CSI does not re-publish an attached volume.
//
// The completion is safe because change_key to the SAME passphrase succeeds and
// leaves that key valid (probed live 2026-08-02), so this arm converges an
// interrupted rotation and is a no-op by outcome otherwise. It must also not
// re-key on every pass.
func TestReconcileCompletesInterruptedRotation(t *testing.T) {
	const volumeID = "enc-half-rotated"
	const oldPass = "old-pass-123"
	const newPass = "new-pass-456"

	d, client, _ := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(newPass, oldPass))
	name := addManagedEncryptedVolume(t, client, volumeID, oldPass)
	// Unlocked, still on the OLD key: exactly the abandoned-rotation state.

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())
	_, methods := client.callSnapshot()
	require.Equal(t, 1, methods["DatasetChangeKey"], "an open rotation window on an unlocked volume is converged")

	// The key really moved: OLD is dead, NEW works.
	require.NoError(t, client.DatasetLock(context.Background(), name))
	require.Error(t, client.DatasetUnlock(context.Background(), name, oldPass))
	require.NoError(t, client.DatasetUnlock(context.Background(), name, newPass))

	// A second pass over the SAME window must not re-key again.
	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())
	_, methods = client.callSnapshot()
	assert.Zero(t, methods["DatasetChangeKey"], "a converged window is not re-keyed on every pass")
}

// TestReconcileEncryptedUnlockSkipsVolumeHeldByPublish is half of the F7
// regression: ControllerPublishVolume serializes per volume; the reconciler used
// to take no lock at all, so it could fire an unlock at a dataset a publish had
// just unlocked — a FAILED job by P-8 — and count it as a volume failure.
func TestReconcileEncryptedUnlockSkipsVolumeHeldByPublish(t *testing.T) {
	const volumeID = "enc-busy"
	const passphrase = "unlock-me-123"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	// Simulate an in-flight ControllerPublishVolume holding the per-volume lock.
	require.True(t, d.acquireOperationLock(volumeLockKey(volumeID)))
	defer d.releaseOperationLock(volumeLockKey(volumeID))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())

	total, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetUnlock"], "a volume a publish is handling is skipped, not raced")
	assert.Zero(t, methods["DatasetEncryptionSummary"], "the busy check precedes any backend call for that volume")
	assert.Equal(t, 1, total, "only the managed-dataset listing is issued")
	assert.Empty(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed))
}

// encryptionRaceClient loses the unlock race the way production does: the
// dataset is unlocked by someone else (a publish) in the instant before
// DatasetUnlock lands, so the call returns the P-8 already-unlocked FAILED job.
type encryptionRaceClient struct {
	*apiCallCountingClient
	raceDataset string
}

func (c *encryptionRaceClient) DatasetUnlock(ctx context.Context, name, passphrase string) error {
	if name == c.raceDataset {
		// The publish wins the race and unlocks it first...
		if err := c.apiCallCountingClient.DatasetUnlock(ctx, name, passphrase); err != nil {
			return err
		}
		// ...and now OUR unlock lands on an already-unlocked dataset (P-8).
		return fmt.Errorf("pool.dataset.unlock job failed: dataset is already unlocked")
	}
	return c.apiCallCountingClient.DatasetUnlock(ctx, name, passphrase)
}

// TestReconcileEncryptedUnlockLostRaceIsBenign is the other half of F7: losing
// the race to a publish must NEVER raise "this volume is dead" on a volume that
// is healthy and serving I/O (R4). Two passes at the event threshold must
// produce no Warning Event.
func TestReconcileEncryptedUnlockLostRaceIsBenign(t *testing.T) {
	const volumeID = "enc-raced"
	const passphrase = "unlock-me-123"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	d.truenasClient = &encryptionRaceClient{apiCallCountingClient: client, raceDataset: name}

	for pass := 0; pass < encryptionUnlockEventThreshold; pass++ {
		require.NoError(t, client.DatasetLock(context.Background(), name))
		d.reconcileEncryptedUnlocks(context.Background())
	}

	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked, "the volume is unlocked and healthy")
	assert.Zero(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed),
		"a lost unlock race is benign: never a volume-unhealthy Event (R4)")
}

// TestReconcileEncryptedUnlockEventOnlyOnPersistentFailure pins the Event
// gating: one bad pass is a log line, a PERSISTENT failure is an Event, and a
// recovery clears the streak so the next failure starts counting from zero.
func TestReconcileEncryptedUnlockEventOnlyOnPersistentFailure(t *testing.T) {
	const volumeID = "enc-wrong-key"

	d, client, recorder := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret("wrong-pass-999", ""))
	name := addManagedEncryptedVolume(t, client, volumeID, "right-pass-123")
	require.NoError(t, client.DatasetLock(context.Background(), name))

	d.reconcileEncryptedUnlocks(context.Background())
	assert.Zero(t, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed),
		"a single failed pass stays a log line")

	d.reconcileEncryptedUnlocks(context.Background())
	assert.Equal(t, 1, eventsContainingReason(drainEvents(recorder), EventReasonEncryptionUnlockFailed),
		"a persistent failure becomes exactly one operator-visible Event")

	stillLocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.True(t, stillLocked.Locked, "P-5: a wrong passphrase leaves the dataset locked")

	// Recovery clears the streak.
	require.NoError(t, client.DatasetUnlock(context.Background(), name, "right-pass-123"))
	d.reconcileEncryptedUnlocks(context.Background())
	d.encryptionUnlockFailMu.Lock()
	_, tracked := d.encryptionUnlockFailures[volumeID]
	d.encryptionUnlockFailMu.Unlock()
	assert.False(t, tracked, "an observed-healthy volume stops alerting")
}

// TestReconcileEncryptedUnlockBudgetCarriesOver is the F8 regression: the pass
// is really bounded, and what it does not reach carries over to the next pass
// instead of being silently dropped.
func TestReconcileEncryptedUnlockBudgetCarriesOver(t *testing.T) {
	const passphrase = "unlock-me-123"
	volumeIDs := []string{"enc-a", "enc-b", "enc-c"}

	objects := []runtime.Object{encryptionReconcileSC(), encryptionReconcileSecret(passphrase, "")}
	for _, volumeID := range volumeIDs {
		objects = append(objects, encryptionReconcilePV(volumeID))
	}
	d, client, _ := encryptionReconcileDriver(t, objects...)

	previousBudget := encryptionUnlockMaxVolumesPerPass
	encryptionUnlockMaxVolumesPerPass = 1
	t.Cleanup(func() { encryptionUnlockMaxVolumesPerPass = previousBudget })

	names := make([]string, 0, len(volumeIDs))
	for _, volumeID := range volumeIDs {
		name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
		require.NoError(t, client.DatasetLock(context.Background(), name))
		names = append(names, name)
	}

	lockedCount := func() int {
		locked := 0
		for _, name := range names {
			ds, err := client.DatasetGet(context.Background(), name)
			require.NoError(t, err)
			if ds.Locked {
				locked++
			}
		}
		return locked
	}

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())
	_, methods := client.callSnapshot()
	assert.Equal(t, 1, methods["DatasetUnlock"], "one pass never exceeds the per-pass action budget")
	assert.Equal(t, 2, lockedCount(), "the rest stay locked, awaiting the next pass")

	d.reconcileEncryptedUnlocks(context.Background())
	d.reconcileEncryptedUnlocks(context.Background())
	assert.Zero(t, lockedCount(), "the carry-over is worked off on later passes, never dropped")
}

// TestReconcileEncryptedUnlockRunsWithoutDynamicClient is the F9 regression: the
// reconciler needs the clientset only. Gating it on the dynamic client as well
// silently turned R1's mitigation off wherever that client was unavailable.
func TestReconcileEncryptedUnlockRunsWithoutDynamicClient(t *testing.T) {
	const volumeID = "enc-no-dynamic"
	const passphrase = "unlock-me-123"

	d, client, _ := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	require.Nil(t, d.eventRecorder.dynamicClient, "this driver deliberately has no dynamic client")
	_, _, err := d.kubernetesReconcileClients()
	require.Error(t, err, "the orphan-reconcile gate would refuse to run here")

	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	d.reconcileEncryptedUnlocks(context.Background())

	unlocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.False(t, unlocked.Locked, "the unlock reconciler runs on the clientset alone")
}

// TestReconcileEncryptedUnlockNoopWhenDisabled proves the default-off posture:
// a controller with encryption disabled issues not one backend call.
func TestReconcileEncryptedUnlockNoopWhenDisabled(t *testing.T) {
	const volumeID = "enc-disabled"
	const passphrase = "unlock-me-123"

	d, client, _ := encryptionReconcileDriver(t,
		encryptionReconcilePV(volumeID), encryptionReconcileSC(), encryptionReconcileSecret(passphrase, ""))
	d.config.Encryption.Enabled = false
	name := addManagedEncryptedVolume(t, client, volumeID, passphrase)
	require.NoError(t, client.DatasetLock(context.Background(), name))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(context.Background())

	total, _ := client.callSnapshot()
	assert.Zero(t, total, "encryption off: the reconciler makes no backend call at all")
}

// TestReconcileEncryptedUnlockIgnoresPlaintextVolumes proves the enumeration is
// still a filter, not a sweep: a plaintext managed volume is never a candidate,
// so no encryption_summary job is issued for it.
func TestReconcileEncryptedUnlockIgnoresPlaintextVolumes(t *testing.T) {
	d, client, _ := encryptionReconcileDriver(t, encryptionReconcileSC(), encryptionReconcileSecret("unlock-me-123", ""))
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: encReconcileParent + "/plain", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, encReconcileParent+"/plain", PropManagedResource, "true"))

	client.resetCalls()
	d.reconcileEncryptedUnlocks(ctx)

	_, methods := client.callSnapshot()
	assert.Zero(t, methods["DatasetEncryptionSummary"], "a plaintext volume is not an unlock candidate")
	assert.Zero(t, methods["DatasetUnlock"])
}

// TestReconcileEncryptedUnlockMissingSecretIsActionable proves the R2 path: when
// the Secret that holds the only copy of the key is gone, the volume cannot be
// unlocked and the operator must be told — persistently, and without the driver
// ever pretending the volume is fine.
func TestReconcileEncryptedUnlockMissingSecretIsActionable(t *testing.T) {
	const volumeID = "enc-lost-key"

	// SC + PV exist; the Secret does not.
	d, client, recorder := encryptionReconcileDriver(t, encryptionReconcilePV(volumeID), encryptionReconcileSC())
	name := addManagedEncryptedVolume(t, client, volumeID, "unlock-me-123")
	require.NoError(t, client.DatasetLock(context.Background(), name))

	for pass := 0; pass < encryptionUnlockEventThreshold; pass++ {
		d.reconcileEncryptedUnlocks(context.Background())
	}

	events := drainEvents(recorder)
	assert.Equal(t, 1, eventsContainingReason(events, EventReasonEncryptionUnlockFailed))
	for _, event := range events {
		assert.NotContains(t, event, "unlock-me-123", "no Event may carry key material")
	}
	stillLocked, err := client.DatasetGet(context.Background(), name)
	require.NoError(t, err)
	assert.True(t, stillLocked.Locked)
}

// TestEncryptionUnlockCandidateIsSourceBlind pins the enumeration contract at
// the unit level: candidate selection must work on a dataset whose user
// properties carry NO source, which is every dataset on the zfs.resource.query
// path. This is the assertion that fails first if anyone re-introduces a
// source-gated read into the candidate filter.
func TestEncryptionUnlockCandidateIsSourceBlind(t *testing.T) {
	sourceless := &truenas.Dataset{
		ResourceQuery: true,
		UserProperties: map[string]truenas.UserProperty{
			PropEncryption: {Value: "AES-256-GCM"}, // no Source: the resource path strips it
		},
	}
	assert.True(t, encryptionUnlockCandidate(sourceless),
		"a sourceless encryption stamp must still select the volume as a candidate")
	assert.False(t, isEncryptedDataset(sourceless),
		"...while the OWNERSHIP predicate still (correctly) refuses to call it ours")

	assert.True(t, encryptionUnlockCandidate(&truenas.Dataset{Encrypted: true}),
		"the backend's own encrypted flag is also a candidate signal")
	assert.False(t, encryptionUnlockCandidate(&truenas.Dataset{
		UserProperties: map[string]truenas.UserProperty{PropEncryption: {Value: "-"}},
	}), "the ZFS sentinel is not a stamp")
	assert.False(t, encryptionUnlockCandidate(&truenas.Dataset{}))
}

// TestVolumeIDForDataset pins the reporting identity used in logs and Events.
func TestVolumeIDForDataset(t *testing.T) {
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: encReconcileParent}}}
	assert.Equal(t, "vol-1", d.volumeIDForDataset(encReconcileParent+"/vol-1"))
	assert.Equal(t, "other/vol-1", d.volumeIDForDataset("other/vol-1"))
}
