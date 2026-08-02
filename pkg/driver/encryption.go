package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ZFS-native encryption at rest (GF-Sprint 1). This file mirrors iscsi_chap.go:
// per-StorageClass Secret parsing, validation BEFORE any API call, redaction,
// request-scoped context plumbing (no ShareBackend widening), a durable LOCAL
// user-property stamp, and an idempotent-replay immutability guard.
//
// Doctrine: encryption is an AVAILABILITY HAZARD — a locked dataset serves zero
// I/O (P-4). It ships default-off, opt-in per StorageClass. The passphrase is
// radioactive: it exists only in the K8s Secret, the short-lived parse struct,
// and request-scoped context. It must NEVER reach a log, gRPC status, K8s Event,
// volumeContext, or dataset user-property.

// paramEncryption is the StorageClass parameter that explicitly opts a class
// into encryption (encryption: "true"). Encryption is also implied when the
// provisioner secret carries a passphrase.
const paramEncryption = "encryption"

// volumeContextEncryptionKey is the ONLY encryption-related volume-context key.
// It carries the algorithm marker (e.g. "AES-256-GCM") so consumers can tell a
// volume is encrypted; it holds NO key material. volumeContext is persisted in
// the PV, so the passphrase must never be written there.
const volumeContextEncryptionKey = "encryption"

// Encryption secret keys accepted in the per-StorageClass Kubernetes Secret.
// passphrase is canonical and required; passphrasePrevious opens the two-key
// rotation window (E-3); algorithm and pbkdf2iters are optional overrides.
const (
	encryptionSecretKeyPassphrase         = "passphrase"
	encryptionSecretKeyPassphrasePrevious = "passphrasePrevious"
	encryptionSecretKeyAlgorithm          = "algorithm"
	encryptionSecretKeyPbkdf2Iters        = "pbkdf2iters"
)

// encryptionDefaultAlgorithm is the default create algorithm. P-0 probed the
// full choice set {AES-128-CCM, AES-192-CCM, AES-256-CCM, AES-128-GCM,
// AES-192-GCM, AES-256-GCM}; AES-256-GCM is present, so it is the default.
const encryptionDefaultAlgorithm = "AES-256-GCM"

// encryptionMinPassphraseLen is the ZFS minimum passphrase length. ZFS rejects a
// passphrase shorter than 8 characters; the driver enforces it before any API
// call so a bad secret fails fast with InvalidArgument.
const encryptionMinPassphraseLen = 8

// encryptionAlgorithms is the P-0 probed set pool.dataset.encryption_algorithm_choices
// returns on nas01 26.0.0-BETA.1. A requested algorithm outside this set is
// rejected with InvalidArgument before any API call. Do not "correct" this set
// from ZFS documentation — it is pinned to the probe.
var encryptionAlgorithms = map[string]struct{}{
	"AES-128-CCM": {},
	"AES-192-CCM": {},
	"AES-256-CCM": {},
	"AES-128-GCM": {},
	"AES-192-GCM": {},
	"AES-256-GCM": {},
}

// encryptionSecret is the driver-side parse of a per-StorageClass encryption
// Secret. It is short-lived: validated, folded into the create call or the
// unlock call, and dropped. Passphrase holds the EXACT unmodified secret (never
// trimmed) so validation rejects a value ZFS would reject rather than silently
// mutating it.
type encryptionSecret struct {
	Passphrase         string
	PassphrasePrevious string
	Algorithm          string
	Pbkdf2Iters        int
}

// encryptionResolution is the validated result threaded from CreateVolume down to
// the dataset-param builder via request context. It carries the key material for
// the single pool.dataset.create call; it is never persisted.
type encryptionResolution struct {
	Algorithm   string
	Passphrase  string
	Pbkdf2Iters int
}

// encryptionContextKey carries the request-scoped encryption resolution from
// CreateVolume down to the dataset-param builder without widening the
// ShareBackend interface (which is shared by NFS/iSCSI/NVMe-oF and carries no
// secrets).
type encryptionContextKey struct{}

func withEncryptionResolution(ctx context.Context, res *encryptionResolution) context.Context {
	return context.WithValue(ctx, encryptionContextKey{}, res)
}

func encryptionResolutionFromContext(ctx context.Context) *encryptionResolution {
	if res, ok := ctx.Value(encryptionContextKey{}).(*encryptionResolution); ok {
		return res
	}
	return nil
}

// parseEncryptionSecret reads the encryption Secret keys. Values are taken
// VERBATIM (untrimmed) so validation can reject, not silently repair, a
// malformed secret.
func parseEncryptionSecret(secrets map[string]string) encryptionSecret {
	parsed := encryptionSecret{
		Passphrase:         firstSecretKey(secrets, encryptionSecretKeyPassphrase),
		PassphrasePrevious: firstSecretKey(secrets, encryptionSecretKeyPassphrasePrevious),
		Algorithm:          strings.TrimSpace(secrets[encryptionSecretKeyAlgorithm]),
	}
	if raw := strings.TrimSpace(secrets[encryptionSecretKeyPbkdf2Iters]); raw != "" {
		if iters, err := strconv.Atoi(raw); err == nil && iters > 0 {
			parsed.Pbkdf2Iters = iters
		}
	}
	return parsed
}

// validateEncryptionSecret enforces the ZFS encryption constraints on the EXACT
// unmodified secret before any API call, so a bad secret fails fast with
// InvalidArgument instead of a backend error. It returns the resolved algorithm
// (default applied) for the caller to stamp and fold.
func validateEncryptionSecret(secret encryptionSecret) (string, error) {
	if secret.Passphrase == "" {
		return "", status.Error(codes.InvalidArgument, "encryption passphrase is required when encryption is enabled")
	}
	if len(secret.Passphrase) < encryptionMinPassphraseLen {
		return "", status.Errorf(codes.InvalidArgument,
			"encryption passphrase must be at least %d characters (ZFS minimum)", encryptionMinPassphraseLen)
	}
	algorithm := secret.Algorithm
	if algorithm == "" {
		algorithm = encryptionDefaultAlgorithm
	}
	if _, ok := encryptionAlgorithms[algorithm]; !ok {
		return "", status.Errorf(codes.InvalidArgument,
			"encryption algorithm %q is not supported by the backend (supported: AES-128-CCM, AES-192-CCM, AES-256-CCM, AES-128-GCM, AES-192-GCM, AES-256-GCM)",
			algorithm)
	}
	return algorithm, nil
}

// encryptionEnabledForCreate reports whether a CreateVolume request opts into
// encryption: the controller-wide feature must be on, and either the
// StorageClass sets encryption=true or the provisioner secret carries a
// passphrase. Zero-value config (Enabled=false) is OFF.
func (d *Driver) encryptionEnabledForCreate(params, secrets map[string]string) bool {
	if d.config == nil || !d.config.Encryption.Enabled {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(params[paramEncryption]), "true") {
		return true
	}
	return firstSecretKey(secrets, encryptionSecretKeyPassphrase) != ""
}

// storedEncryptionAlgorithm reads the durable per-volume encryption marker from
// the dataset's LOCAL properties: the algorithm the volume was created with. It
// returns "" when the volume is not encrypted (property absent, non-local, or
// the ZFS sentinel). This is the single reader every unlock/reconcile/health
// path consults to decide whether a volume is encrypted, and it holds NO key.
func storedEncryptionAlgorithm(ds *truenas.Dataset) string {
	return datasetLocalUserProperty(ds, PropEncryption)
}

// isEncryptedDataset reports whether a dataset carries the LOCAL encryption
// marker — i.e. whether THIS driver stamped THIS dataset as its own encrypted
// volume. A clone-inherited marker (source != local) does not count: a clone
// shares its origin's key (P-7) and is handled through the origin, never stamped
// as independently encrypted.
//
// This is an OWNERSHIP predicate, not an "is it encrypted" predicate. The stamp
// is written after pool.dataset.create returns, so an encrypted dataset can
// legitimately exist without it (a controller killed inside that window). Use
// datasetEncryptedOnWire / datasetNeedsEncryptionHandling wherever the question
// is "does this dataset hold ciphertext", and reserve this one for "is this
// volume's encryption policy ours".
func isEncryptedDataset(ds *truenas.Dataset) bool {
	return storedEncryptionAlgorithm(ds) != ""
}

// datasetEncryptedOnWire is the BACKEND's own answer, independent of any stamp:
// pool.dataset.query returns encrypted:true for an encrypted dataset, including
// a locked one (P-1/P-2 create result, P-4 locked row). It is the only truth
// available for a dataset whose stamp write never landed, and for a clone, whose
// encryption is inherited from its origin (P-7).
func datasetEncryptedOnWire(ds *truenas.Dataset) bool {
	return ds != nil && ds.Encrypted
}

// datasetNeedsEncryptionHandling reports whether a dataset must go through the
// unlock/rotation machinery at publish time: either the driver stamped it, or
// the backend says it holds ciphertext. An encrypted-but-unstamped dataset MUST
// take this path — treating it as plaintext skips the unlock and then fails
// later, opaquely, in WaitForZvolReady/mount with no mention of encryption.
func datasetNeedsEncryptionHandling(ds *truenas.Dataset) bool {
	return isEncryptedDataset(ds) || datasetEncryptedOnWire(ds)
}

// encryptionKeys is the pair of passphrases a publish/reconcile pass may use:
// the desired (current) key and, while the rotation window is deliberately open,
// the previous one. It is short-lived and never persisted or logged.
type encryptionKeys struct {
	Passphrase string
	Previous   string
}

// rotationIntent reports that the operator has deliberately opened a rotation
// window: a usable current passphrase AND a different previous one. A previous
// key equal to the current one (or either being empty) is not an intent.
func (k encryptionKeys) rotationIntent() bool {
	return k.Passphrase != "" && k.Previous != "" && k.Previous != k.Passphrase
}

// encryptionKeysFromSecrets reads the two rotation-window keys out of a CSI
// secret map (the controller-publish secret at publish time, the same Secret's
// data at reconcile time).
func encryptionKeysFromSecrets(secrets map[string]string) encryptionKeys {
	return encryptionKeys{
		Passphrase: firstSecretKey(secrets, encryptionSecretKeyPassphrase),
		Previous:   firstSecretKey(secrets, encryptionSecretKeyPassphrasePrevious),
	}
}

// encryptionRedactionMask replaces any passphrase substring found in text.
const encryptionRedactionMask = "***"

// redactEncryptionSecrets scrubs every supplied passphrase out of arbitrary
// text. It exists because the driver FORWARDS backend text it did not compose:
// pool.dataset.unlock / pool.dataset.change_key take the passphrase as a call
// ARGUMENT, and a middleware traceback on a 26.0.0-BETA backend is free to echo
// its arguments into job["error"]/job["exception"], which pollJobOnce carries
// verbatim into jobTerminalError. Everything the driver composes itself is
// already passphrase-free; this closes the forwarding channel (R6).
func redactEncryptionSecrets(text string, secrets ...string) string {
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		text = strings.ReplaceAll(text, secret, encryptionRedactionMask)
	}
	return text
}

// redactEncryptionError renders a backend error with every supplied passphrase
// masked. Callers interpolate the RESULT (a string), never the error, so no
// unredacted text can reach a gRPC status, a klog line or a K8s Event.
func redactEncryptionError(err error, secrets ...string) string {
	if err == nil {
		return ""
	}
	return redactEncryptionSecrets(err.Error(), secrets...)
}

// encryptionProps returns the durable per-volume encryption stamp
// (PropEncryption = <algorithm>) for an encryption-resolved create, or nil when
// encryption is not active. It is folded into the single fatal managed-property
// update so the marker is durable-or-rolled-back with the rest of provisioning.
// It stamps ONLY the algorithm — NEVER the passphrase.
func encryptionProps(ctx context.Context) map[string]string {
	res := encryptionResolutionFromContext(ctx)
	if res == nil {
		return nil
	}
	return map[string]string{PropEncryption: res.Algorithm}
}

// guardExistingEncryptionPolicy enforces that an idempotent CreateVolume replay
// against an already-provisioned volume cannot flip its encryption posture, and
// repairs the one state where the stamp is missing but the truth is knowable.
//
// The comparand is WIRE TRUTH, not the stamp alone: pool.dataset.query reports
// encrypted:true for an encrypted dataset (P-1/P-2/P-4), and the stamp is only
// written after the create returns. A controller killed inside that window
// leaves an ENCRYPTED dataset with no stamp; comparing stamps alone would then
// tell the operator their encrypted volume "already exists as plaintext" and
// wedge the PVC forever behind a statement that is the exact opposite of the
// truth. So:
//
//   - encrypted (wire or stamp) + encrypted request: allowed; when the stamp is
//     missing this returns the repair props so the caller re-stamps the algorithm
//     it is provisioning with (the request IS this volume's create request — the
//     replay of the very create whose stamp write was lost). A dataset that
//     carries a content-source record is never healed: its encryption may be
//     INHERITED from its origin (P-7) and is not this volume's own policy.
//   - encrypted + plaintext request: FailedPrecondition, with a message that
//     names the real state (never the word plaintext about a dataset the backend
//     says is encrypted).
//   - plaintext + encrypted request: FailedPrecondition — encryption is
//     create-time only, ZFS cannot encrypt an existing dataset in place.
//
// It is READ-ONLY apart from the returned repair map, so callers must run it
// BEFORE they write any property (the ordering the comment at its call site
// asserts, and what keeps a conflicting replay reclaimable).
func (d *Driver) guardExistingEncryptionPolicy(ctx context.Context, ds *truenas.Dataset) (map[string]string, error) {
	storedAlgorithm := storedEncryptionAlgorithm(ds)
	stamped := storedAlgorithm != ""
	onWire := datasetEncryptedOnWire(ds)
	storedEncrypted := stamped || onWire

	resolution := encryptionResolutionFromContext(ctx)
	requestEncrypted := resolution != nil

	switch {
	case storedEncrypted && requestEncrypted:
		if stamped {
			return nil, nil
		}
		if datasetUserProperty(ds, PropVolumeContentSourceType) != "" {
			return nil, status.Error(codes.FailedPrecondition,
				"volume already exists and the backend reports it encrypted, but it was provisioned from a content "+
					"source, so its encryption is inherited from its origin (P-7) and is not this volume's own policy. "+
					"It cannot be adopted as an independently encrypted volume — provision a fresh encrypted volume "+
					"and copy the data in")
		}
		// Repair, do not re-create: this is a replay of the create whose stamp
		// write was lost, and the algorithm is known from the request.
		return map[string]string{PropEncryption: resolution.Algorithm}, nil
	case !storedEncrypted && !requestEncrypted:
		return nil, nil
	case storedEncrypted && !requestEncrypted:
		if !stamped {
			return nil, status.Error(codes.FailedPrecondition,
				"volume already exists ENCRYPTED on the backend (pool.dataset.query reports encrypted:true) but carries "+
					"no local encryption stamp — most likely a create interrupted between pool.dataset.create and the "+
					"stamp write. It cannot be reprovisioned as plaintext: replay the SAME encrypted StorageClass to "+
					"repair the stamp, or delete the volume")
		}
		return nil, status.Errorf(codes.FailedPrecondition,
			"volume already exists encrypted (%s); it cannot be reprovisioned as plaintext. "+
				"Encryption is immutable for the life of a volume — provision a new volume to change it",
			storedAlgorithm)
	default:
		return nil, status.Error(codes.FailedPrecondition,
			"volume already exists as plaintext; it cannot be retro-encrypted. "+
				"Encryption is create-time only — provision a new volume with encryption enabled")
	}
}

// guardEncryptedContentSource refuses to provision a volume FROM an encrypted
// content source. It guards the SOURCE side; the destination side (an encrypted
// StorageClass combined with any content source) is refused in CreateVolume.
//
// Why refuse rather than carry it:
//
//   - clone restore: P-7 is unambiguous — the clone comes out encrypted:true
//     with encryption_root == the ORIGIN. It shares the origin's key, cannot be
//     re-keyed independently, and gets no LOCAL stamp of its own, so the driver
//     would treat it as plaintext: publish would never unlock it and the unlock
//     reconciler would never consider it (its stamp is inherited, not local).
//     The first appliance reboot leaves that volume dead — locked, with no
//     recovery path in the driver at all.
//   - detached (send/recv) restore: UNPROBED whether TrueNAS 26.0 sends raw
//     (encrypted target, same wedge as above) or plain (a SILENT decryption of
//     the operator's data into a plaintext dataset). Both outcomes are wrong to
//     produce without an explicit decision, so this is fail-closed until the
//     drill settles it (see scripts/gf1-encryption-drill.sh step 6b).
//
// It costs one extra pool.dataset.query ONLY when encryption is enabled
// controller-wide AND the caller has no already-read source dataset to hand; a
// deployment with encryption off pays nothing and its clone/restore call counts
// are byte-identical to pre-encryption.
func (d *Driver) guardEncryptedContentSource(
	ctx context.Context,
	sourceDataset string,
	sourceDS *truenas.Dataset,
	sourceDescription string,
) error {
	if d.config == nil || !d.config.Encryption.Enabled {
		return nil
	}
	if sourceDS == nil {
		var err error
		sourceDS, err = d.truenasClient.DatasetGet(ctx, sourceDataset)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				return status.Errorf(codes.NotFound, "content source not found: %s", sourceDescription)
			}
			return status.Errorf(codes.Internal,
				"failed to check whether content source %s is encrypted: %v", sourceDescription, err)
		}
	}
	if !datasetNeedsEncryptionHandling(sourceDS) {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition,
		"content source %s is an ENCRYPTED volume and cannot be cloned or restored from. A ZFS clone inherits the "+
			"ORIGIN's key and encryption_root (P-7) and carries no encryption policy of its own, so the driver could "+
			"never unlock it — the volume would be dead I/O after the first appliance reboot. Provision a fresh "+
			"encrypted volume from the encrypted StorageClass and copy the data in",
		sourceDescription)
}

// applyEncryptionToCreateParams folds the request-scoped encryption resolution
// into a dataset-create payload (P-1/P-2 shape): encryption:true,
// inherit_encryption:false, and the encryption_options. It is a no-op when no
// resolution is present, so a plaintext create payload is byte-identical to
// pre-encryption (+0 RTT). The passphrase rides in this single create call and
// is never stamped as a property.
func applyEncryptionToCreateParams(ctx context.Context, params *truenas.DatasetCreateParams) {
	res := encryptionResolutionFromContext(ctx)
	if res == nil {
		return
	}
	encrypted := true
	inherit := false
	params.Encryption = &encrypted
	params.InheritEncryption = &inherit
	params.EncryptionOptions = &truenas.EncryptionOptions{
		Algorithm:   res.Algorithm,
		Passphrase:  res.Passphrase,
		Pbkdf2Iters: res.Pbkdf2Iters,
	}
}

// encryptionSummaryLocked reads the P-3 summary for datasetName and reports
// whether THAT dataset is locked.
//
// It fails CLOSED on any ambiguity. pool.dataset.encryption_summary <id> returns
// rows for the subtree, so "no row named datasetName" means either an empty
// result or a set of rows describing CHILDREN — and reading a child's lock state
// (or defaulting an empty result to "not locked") would silently skip the unlock
// of a locked volume, which is the one failure this whole feature exists to
// prevent. Both cases are errors; the caller surfaces them rather than guessing.
// Backend id/path normalisation drift on a BETA is exactly the kind of thing
// that would otherwise be masked here, so the drill asserts the row name matches
// the dataset id.
func encryptionSummaryLocked(summary []truenas.EncryptionSummaryEntry, datasetName string) (bool, error) {
	for _, entry := range summary {
		if entry.Name == datasetName {
			return entry.Locked, nil
		}
	}
	if len(summary) == 0 {
		return false, fmt.Errorf("encryption_summary for %s returned no rows", datasetName)
	}
	names := make([]string, 0, len(summary))
	for _, entry := range summary {
		names = append(names, entry.Name)
	}
	return false, fmt.Errorf("encryption_summary for %s named no matching row (rows: %s)",
		datasetName, strings.Join(names, ", "))
}

// unlockEncryptedDatasetForPublish is the controller-side unlock that runs in
// ControllerPublishVolume BEFORE ensureShareExists (E-2 §2). The node has no
// TrueNAS client, so unlock cannot happen node-side; and the share/extent build
// on a locked zvol has no backing device (P-4) and would fail. It is a strict
// no-op for a plaintext volume and for an encrypted volume that is already
// unlocked with no rotation window open (the P-8 gate: unlock is NOT
// idempotent). It fails CLOSED: no secret or a wrong passphrase is a
// FailedPrecondition, never a silent skip. The passphrase comes from the
// controller-publish-secret and never leaves this request scope; every log,
// status and Event message here is either composed by the driver or routed
// through redactEncryptionError.
//
// The unlock/rotation decision itself lives in convergeEncryptedDatasetKey,
// which the locked-volume reconciler calls with the same inputs — one
// implementation, two callers, so a reboot inside the rotation window recovers
// through either path.
func (d *Driver) unlockEncryptedDatasetForPublish(ctx context.Context, ds *truenas.Dataset, datasetName, volumeID string, secrets map[string]string) error {
	// Wire truth, not the stamp alone: an encrypted dataset whose stamp write was
	// lost to a controller kill must still be unlocked here. Skipping it would
	// hand ensureShareExists a locked zvol with no backing device and surface as
	// an unexplained device-wait timeout (F4).
	if !datasetNeedsEncryptionHandling(ds) {
		return nil
	}
	keys := encryptionKeysFromSecrets(secrets)

	summary, err := d.truenasClient.DatasetEncryptionSummary(ctx, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read encryption summary for volume %s: %s",
			volumeID, redactEncryptionError(err, keys.Passphrase, keys.Previous))
	}
	locked, lockedErr := encryptionSummaryLocked(summary, datasetName)
	if lockedErr != nil {
		// Fail closed: an unreadable lock state is not evidence of an unlocked
		// volume (F17).
		return status.Errorf(codes.Internal, "cannot determine the lock state of encrypted volume %s: %v",
			volumeID, lockedErr)
	}
	if !locked && !keys.rotationIntent() {
		// P-8: already unlocked and no rotation window is open. Do NOT call unlock
		// — it would fail and mis-report a healthy volume.
		return nil
	}
	if locked && keys.Passphrase == "" {
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and requires a controller-publish secret with a %q key to unlock it",
			volumeID, encryptionSecretKeyPassphrase)
	}
	if locked && !isEncryptedDataset(ds) {
		klog.Warningf("Volume %s is locked and the backend reports it encrypted, but it carries no local encryption "+
			"stamp (an interrupted create, or encryption inherited from a content source). Unlocking it with the "+
			"supplied publish secret; replay CreateVolume against the same encrypted StorageClass to repair the stamp.",
			volumeID)
	}
	return d.convergeEncryptedDatasetKey(ctx, datasetName, volumeID, locked, keys)
}

// convergeEncryptedDatasetKey drives a dataset to the state the Secret says it
// should be in: unlocked, and keyed with the CURRENT passphrase. It is the
// single implementation shared by ControllerPublishVolume and the locked-volume
// reconciler.
//
// The four states, all probe-grounded:
//
//   - unlocked, no rotation window: nothing to do. Calling unlock here would
//     return a FAILED job (P-8) and mis-report a healthy volume.
//   - unlocked, rotation window OPEN (passphrasePrevious present and different):
//     call change_key(current) unconditionally. change_key requires the dataset
//     unlocked (P-6), which it is. Either the dataset is still on the OLD key —
//     an interrupted rotation, and this completes it — or it is already on the
//     current key, and re-keying to the IDENTICAL passphrase is a success whose
//     key stays valid (probed live on nas01 26.0.0-BETA.1, 2026-08-02:
//     change_key with the same passphrase returns job SUCCESS, and a subsequent
//     lock -> unlock with that same passphrase succeeds). Without this arm an
//     interrupted rotation reports SUCCESS forever on the old key, and the
//     operator drops passphrasePrevious believing rotation finished — the R2
//     permanent-data-loss trap.
//   - locked: unlock(current). If it succeeds the dataset is BY DEFINITION keyed
//     with the current passphrase, so no re-key is needed even mid-window.
//   - locked and current fails, window open: unlock(previous) then
//     change_key(current) (P-5 fail-closed on the first, P-6 on the re-key).
//
// A failed re-key NEVER returns success: it emits a persistent, actionable
// Warning Event telling the operator to keep passphrasePrevious, and returns an
// error, so the next publish or reconcile pass retries the completion.
func (d *Driver) convergeEncryptedDatasetKey(
	ctx context.Context,
	datasetName, volumeID string,
	locked bool,
	keys encryptionKeys,
) error {
	rotating := keys.rotationIntent()

	if !locked {
		if !rotating {
			return nil
		}
		if changeErr := d.truenasClient.DatasetChangeKey(ctx, datasetName, keys.Passphrase); changeErr != nil {
			return d.noteEncryptionRotationIncomplete(volumeID, keys, changeErr)
		}
		d.recordEncryptionRotated(volumeID)
		return nil
	}

	if keys.Passphrase == "" {
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and no unlock passphrase is available", volumeID)
	}
	if unlockErr := d.truenasClient.DatasetUnlock(ctx, datasetName, keys.Passphrase); unlockErr == nil {
		// Unlocked with the current passphrase: the dataset already holds that key,
		// so a supplied previous key is stale and there is nothing to re-key.
		return nil
	}
	if !rotating {
		// P-5 fail-closed: the passphrase did not unlock and there is no rotation
		// window. The dataset stays locked; surface the failure.
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and the supplied passphrase did not unlock it", volumeID)
	}
	if prevErr := d.truenasClient.DatasetUnlock(ctx, datasetName, keys.Previous); prevErr != nil {
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and neither the current nor the previous passphrase unlocked it", volumeID)
	}
	if changeErr := d.truenasClient.DatasetChangeKey(ctx, datasetName, keys.Passphrase); changeErr != nil {
		return d.noteEncryptionRotationIncomplete(volumeID, keys, changeErr)
	}
	d.recordEncryptionRotated(volumeID)
	return nil
}

// recordEncryptionRotated emits the redacted rotation Event: it names the volume
// and nothing else.
func (d *Driver) recordEncryptionRotated(volumeID string) {
	d.recordNormalEvent(volumeEventRef(volumeID), EventReasonEncryptionRotated,
		fmt.Sprintf("Rotated encryption passphrase for volume %s", volumeID))
}

// noteEncryptionRotationIncomplete is the durable, actionable trace an abandoned
// rotation must leave behind (R2). The dataset is unlocked but may still be
// keyed to the PREVIOUS passphrase, so the one thing the operator must not do is
// drop passphrasePrevious. It emits a Warning Event saying exactly that, logs it
// redacted, and returns an error — never success — so the next publish or
// reconcile pass re-attempts the completion.
func (d *Driver) noteEncryptionRotationIncomplete(volumeID string, keys encryptionKeys, cause error) error {
	detail := redactEncryptionError(cause, keys.Passphrase, keys.Previous)
	d.recordWarningEvent(volumeEventRef(volumeID), EventReasonEncryptionRotationIncomplete,
		fmt.Sprintf("Encryption passphrase rotation for volume %s did NOT complete: the volume may still be keyed to "+
			"the PREVIOUS passphrase. KEEP passphrasePrevious in the unlock Secret until an EncryptionRotated event is "+
			"observed for this volume — removing it now can make the data permanently unrecoverable", volumeID))
	klog.Warningf("Encryption rotation for volume %s did not complete (re-key failed, rotation window must stay open): %s",
		volumeID, detail)
	return status.Errorf(codes.Internal,
		"encrypted volume %s: re-keying to the current passphrase failed, so the rotation window must stay open "+
			"(keep passphrasePrevious in the Secret): %s", volumeID, detail)
}
