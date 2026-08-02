package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
// marker. A clone-inherited marker (source != local) does not count: a clone
// shares its origin's key (P-7) and is handled through the origin, never stamped
// as independently encrypted.
func isEncryptedDataset(ds *truenas.Dataset) bool {
	return storedEncryptionAlgorithm(ds) != ""
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
// against an already-provisioned volume cannot flip its encryption posture. The
// dataset's stored, LOCAL marker is authoritative and immutable: encryption is
// create-time only (ZFS cannot encrypt an existing dataset in place). A conflict
// — encrypted-vs-plaintext in either direction — is a FailedPrecondition, never a
// silent re-stamp. This blocks both retro-encrypting a plaintext volume and
// replaying an encrypted volume as plaintext.
func (d *Driver) guardExistingEncryptionPolicy(ctx context.Context, ds *truenas.Dataset) error {
	storedAlgorithm := storedEncryptionAlgorithm(ds)
	storedEncrypted := storedAlgorithm != ""

	requestEncrypted := encryptionResolutionFromContext(ctx) != nil

	if storedEncrypted == requestEncrypted {
		return nil
	}
	if storedEncrypted {
		return status.Errorf(codes.FailedPrecondition,
			"volume already exists encrypted (%s); it cannot be reprovisioned as plaintext. "+
				"Encryption is immutable for the life of a volume — provision a new volume to change it",
			storedAlgorithm)
	}
	return status.Error(codes.FailedPrecondition,
		"volume already exists as plaintext; it cannot be retro-encrypted. "+
			"Encryption is create-time only — provision a new volume with encryption enabled")
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
// whether it is locked. A summary that does not name the dataset falls back to
// the first entry (encryption_summary for a single id returns that dataset's
// row); an empty summary reports false so the gate never calls unlock
// speculatively (P-8 — unlock on an unlocked dataset is a FAILED job).
func encryptionSummaryLocked(summary []truenas.EncryptionSummaryEntry, datasetName string) bool {
	for _, entry := range summary {
		if entry.Name == datasetName {
			return entry.Locked
		}
	}
	if len(summary) > 0 {
		return summary[0].Locked
	}
	return false
}

// unlockEncryptedDatasetForPublish is the controller-side unlock that runs in
// ControllerPublishVolume BEFORE ensureShareExists (E-2 §2). The node has no
// TrueNAS client, so unlock cannot happen node-side; and the share/extent build
// on a locked zvol has no backing device (P-4) and would fail. It is a strict
// no-op for a plaintext volume and for an encrypted volume that is already
// unlocked (the P-8 gate: unlock is NOT idempotent). It fails CLOSED: no secret
// or a wrong passphrase is a FailedPrecondition, never a silent skip. The
// passphrase comes from the controller-publish-secret and never leaves this
// request scope; log and Event messages are redacted.
//
// Rotation (E-3 §1): when the current passphrase fails but passphrasePrevious is
// supplied, it unlocks with the previous key and then re-keys the dataset to the
// current one (change_key requires unlocked, P-6). This is idempotent by outcome
// — once rotated, the old key is dead (P-6) and a replay lands on the
// current-passphrase-succeeds arm.
func (d *Driver) unlockEncryptedDatasetForPublish(ctx context.Context, ds *truenas.Dataset, datasetName, volumeID string, secrets map[string]string) error {
	if !isEncryptedDataset(ds) {
		return nil
	}
	summary, err := d.truenasClient.DatasetEncryptionSummary(ctx, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read encryption summary for volume %s: %v", volumeID, err)
	}
	if !encryptionSummaryLocked(summary, datasetName) {
		// P-8: already unlocked. Do NOT call unlock — it would fail and mis-report
		// a healthy volume.
		return nil
	}

	passphrase := firstSecretKey(secrets, encryptionSecretKeyPassphrase)
	if passphrase == "" {
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and requires a controller-publish secret with a %q key to unlock it",
			volumeID, encryptionSecretKeyPassphrase)
	}
	previous := firstSecretKey(secrets, encryptionSecretKeyPassphrasePrevious)

	if unlockErr := d.truenasClient.DatasetUnlock(ctx, datasetName, passphrase); unlockErr == nil {
		// Unlocked with the current passphrase. A supplied previous key is already
		// stale (rotation completed on an earlier publish) — nothing to do.
		return nil
	}
	if previous == "" || previous == passphrase {
		// P-5 fail-closed: the passphrase did not unlock and there is no rotation
		// window. The dataset stays locked; surface the failure.
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and the supplied passphrase did not unlock it", volumeID)
	}

	// Rotation window: unlock with the previous key, then re-key to the current.
	if prevErr := d.truenasClient.DatasetUnlock(ctx, datasetName, previous); prevErr != nil {
		return status.Errorf(codes.FailedPrecondition,
			"encrypted volume %s is locked and neither the current nor the previous passphrase unlocked it", volumeID)
	}
	if changeErr := d.truenasClient.DatasetChangeKey(ctx, datasetName, passphrase); changeErr != nil {
		return status.Errorf(codes.Internal,
			"encrypted volume %s was unlocked with the previous passphrase but re-keying to the current one failed: %v",
			volumeID, changeErr)
	}
	// Redacted: names the volume, never the passphrase.
	d.recordNormalEvent(volumeEventRef(volumeID), EventReasonEncryptionRotated,
		fmt.Sprintf("Rotated encryption passphrase for volume %s", volumeID))
	return nil
}
