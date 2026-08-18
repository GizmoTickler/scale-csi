package driver

// CSI MODIFY_VOLUME (Kubernetes VolumeAttributesClass).
//
// ControllerModifyVolume retunes a LIVE volume from mutable_parameters, mapping
// them onto pool.dataset.update. The vocabulary is deliberately small and safe:
// only ZFS properties that are live-tunable (zfsLiveTunableProperties) AND that
// this driver already speaks at create time (applyDatasetProperties /
// DatasetCreateParams) are accepted. Everything else — block geometry, capacity,
// protocol/share identity, encryption, and any key the driver does not
// recognize — is a hard InvalidArgument that NAMES the offending key, because a
// silently ignored mutable parameter is a correctness lie: Kubernetes would
// record the PVC as carrying attributes the volume does not have.
//
// The same vocabulary is enforced at CreateVolume, which may carry
// mutable_parameters from the PVC's VolumeAttributesClass at provision time
// (resolved once, threaded to createDataset through the request context, exactly
// like the curated performance class).

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// mutableSyncValues / mutableAtimeValues are the accepted value sets for the two
// tunables the backend publishes no *_choices list for (ZFSPropertyChoices
// carries recordsize/compression/checksum only). They are the OpenZFS enums
// themselves, spelled upper-case exactly as the 26.0 schema and
// applyDatasetProperties' normalization emit them. INHERIT is deliberately not
// accepted: a VolumeAttributesClass declares a concrete attribute, and "go back
// to whatever the parent says" is not a declarable volume attribute.
var mutableSyncValues = map[string]struct{}{
	"STANDARD": {},
	"ALWAYS":   {},
	"DISABLED": {},
}

var mutableAtimeValues = map[string]struct{}{
	"ON":  {},
	"OFF": {},
}

// mutableParameterRejections names the parameter families ControllerModifyVolume
// refuses ON PURPOSE, each with the reason an operator needs to fix their
// VolumeAttributesClass. Keys are compared lower-cased. These exist so a
// dangerous key gets a diagnosis instead of the generic unknown-parameter
// message — the refusal class (InvalidArgument) is the same either way.
var mutableParameterRejections = map[string]string{
	// Block/extent geometry is IMMUTABLE. volblocksize is fixed in ZFS itself
	// (zfsImmutableProperties), and the extent block sizes address bytes already
	// on disk — the entire block_opts immutability discipline exists to refuse
	// exactly this change (a retuned logical block size over existing data is
	// silent corruption, not a retune).
	"volblocksize":       "zvol block geometry is fixed when the dataset is created and cannot be modified afterwards",
	"blocksize":          "block geometry is fixed when the volume is created and cannot be modified afterwards",
	"pblocksize":         "physical block size reporting is fixed when the volume is created and cannot be modified afterwards",
	paramISCSIBlocksize:  "extent block geometry is fixed when the volume is created and cannot be modified afterwards",
	paramISCSIPblocksize: "extent physical-block reporting is fixed when the volume is created and cannot be modified afterwards",
	// Capacity-ish keys: MODIFY_VOLUME must never change capacity (CSI spec);
	// ControllerExpandVolume owns every one of these.
	"volsize":        "capacity is changed through volume expansion (ControllerExpandVolume), never through mutable parameters",
	"quota":          "capacity is changed through volume expansion (ControllerExpandVolume), never through mutable parameters",
	"refquota":       "capacity is changed through volume expansion (ControllerExpandVolume), never through mutable parameters",
	"reservation":    "capacity reservations are managed by the driver's create/expand paths, never through mutable parameters",
	"refreservation": "capacity reservations are managed by the driver's create/expand paths, never through mutable parameters",
	// Protocol/share identity is fixed at creation; every share object (NFS
	// export, iSCSI target/extent, NVMe-oF subsystem) was built for it.
	"protocol": "a volume's protocol and share objects are fixed when the volume is created",
	// Encryption is create-time only: ZFS cannot encrypt (or re-key to a
	// different algorithm) existing data in place — the same reason CreateVolume
	// refuses encryption on a content-source volume.
	paramEncryption: "encryption is create-time only; ZFS cannot encrypt existing data in place",
}

// resolveMutableTunables validates CSI mutable_parameters and resolves them to
// a ZFS-property map (zfsProp* key -> normalized upper-case value). It is the
// SINGLE vocabulary gate for both call sites: ControllerModifyVolume (with the
// live dataset's type) and CreateVolume (with the type the share type implies).
//
// zfsPerformanceClass is rejected here rather than re-applied: the
// PropZFSPerformanceClass stamp asserts "this dataset was CREATED with the
// curated class's properties" and is the anchor of the create-only guard
// (guardPerformanceClassChange). Re-applying a class to an existing dataset
// could honor only its live-tunable subset — volblocksize, the property that
// most distinguishes the presets on a zvol, is immutable — so the volume would
// carry the class name without the class's geometry, and re-stamping would turn
// the guard's anchor into exactly the false-accept/false-reject lie the stamp
// exists to rule out (clones are deliberately never stamped for the same
// reason). Operators who want a preset's tunable subset can spell those
// properties out in the VolumeAttributesClass directly.
func (d *Driver) resolveMutableTunables(ctx context.Context, parameters map[string]string, datasetType string) (map[string]string, error) {
	if len(parameters) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	resolved := make(map[string]string, len(parameters))
	for _, rawKey := range keys {
		normalizedKey := strings.ToLower(strings.TrimSpace(rawKey))
		value := strings.ToUpper(strings.TrimSpace(parameters[rawKey]))
		if strings.EqualFold(strings.TrimSpace(rawKey), zfsPerformanceClassParam) {
			return nil, status.Errorf(codes.InvalidArgument,
				"mutable parameter %q is not supported: a curated ZFS performance class is applied when a volume is created "+
					"and cannot be re-applied to an existing volume (its zvol geometry is immutable, so only part of the class "+
					"could take effect). Set the individual tunables (%s) in the VolumeAttributesClass instead",
				rawKey, strings.Join(supportedMutableParameters(), ", "))
		}
		if reason, rejected := mutableParameterRejections[normalizedKey]; rejected {
			return nil, status.Errorf(codes.InvalidArgument, "mutable parameter %q is not modifiable: %s", rawKey, reason)
		}
		switch normalizedKey {
		case zfsPropCompression, zfsPropSync, zfsPropAtime, zfsPropRecordsize:
			// recognized; validated below
		default:
			return nil, status.Errorf(codes.InvalidArgument,
				"unsupported mutable parameter %q; supported parameters are: %s",
				rawKey, strings.Join(supportedMutableParameters(), ", "))
		}
		if value == "" {
			return nil, status.Errorf(codes.InvalidArgument,
				"mutable parameter %q is set but empty; omit it or give it a value", rawKey)
		}
		switch normalizedKey {
		case zfsPropRecordsize, zfsPropAtime:
			// Filesystem-only at the ZFS layer: a zvol has no mounted POSIX
			// namespace. Same split applyDatasetProperties warn-and-drops at create
			// — but a MODIFY must refuse loudly rather than drop, or Kubernetes
			// records an attribute the zvol cannot carry.
			if datasetType == "VOLUME" {
				return nil, status.Errorf(codes.InvalidArgument,
					"mutable parameter %q applies to FILESYSTEM (NFS) volumes only; this volume is a zvol", rawKey)
			}
		}
		switch normalizedKey {
		case zfsPropSync:
			if _, ok := mutableSyncValues[value]; !ok {
				return nil, status.Errorf(codes.InvalidArgument,
					"invalid mutable parameter %s value %q; valid options are: STANDARD, ALWAYS, DISABLED", rawKey, parameters[rawKey])
			}
		case zfsPropAtime:
			if _, ok := mutableAtimeValues[value]; !ok {
				return nil, status.Errorf(codes.InvalidArgument,
					"invalid mutable parameter %s value %q; valid options are: ON, OFF", rawKey, parameters[rawKey])
			}
		case zfsPropCompression, zfsPropRecordsize:
			if err := d.validateMutableChoiceValue(ctx, normalizedKey, value); err != nil {
				return nil, err
			}
		}
		resolved[normalizedKey] = value
	}
	return resolved, nil
}

// supportedMutableParameters lists the accepted vocabulary for error messages,
// derived from the same constants the resolver switches on so the message can
// never drift from the code.
func supportedMutableParameters() []string {
	return []string{zfsPropCompression, zfsPropSync, zfsPropAtime + " (NFS only)", zfsPropRecordsize + " (NFS only)"}
}

// validateMutableChoiceValue checks a compression/recordsize value against the
// backend's own choice lists, with the same fail-open discipline as
// validatePerformanceClassValues: when the backend did not report a list, the
// driver must not invent a restriction — a truly bad value still fails the
// pool.dataset.update loudly.
func (d *Driver) validateMutableChoiceValue(ctx context.Context, key, value string) error {
	choices, err := d.zfsPropertyChoices(ctx)
	if err != nil {
		klog.Warningf("ZFS property choice validation skipped for mutable parameter %s=%s: %v", key, value, err)
		return nil
	}
	var allow func(string) (allowed, known bool)
	switch key {
	case zfsPropCompression:
		allow = choices.AllowsCompression
	case zfsPropRecordsize:
		allow = choices.AllowsRecordsize
	default:
		return nil
	}
	if allowed, known := allow(value); known && !allowed {
		return status.Errorf(codes.InvalidArgument,
			"invalid mutable parameter %s value %q: this TrueNAS does not accept it", key, value)
	}
	return nil
}

// datasetTypeForShareType maps a share type to the dataset type createDataset
// will provision, so CreateVolume can run the mutable-parameter vocabulary gate
// before anything exists.
func datasetTypeForShareType(shareType ShareType) string {
	if shareType == ShareTypeNFS {
		return "FILESYSTEM"
	}
	return "VOLUME"
}

// --- CreateVolume plumbing (mirrors the zfsPerformanceClass context pattern) ---

type mutableTunablesContextKey struct{}

func withMutableTunables(ctx context.Context, tunables map[string]string) context.Context {
	if len(tunables) == 0 {
		return ctx
	}
	return context.WithValue(ctx, mutableTunablesContextKey{}, tunables)
}

func mutableTunablesFromContext(ctx context.Context) map[string]string {
	tunables, _ := ctx.Value(mutableTunablesContextKey{}).(map[string]string)
	return tunables
}

// applyMutableTunablesToCreate folds resolved mutable parameters into a dataset
// create payload (+0 RTT). It runs AFTER applyPerformanceClassProperties and
// applyDatasetProperties on purpose: the curated class and the controller-wide
// zfs.datasetProperties are deployment-scoped policy, while mutable_parameters
// are this volume's own declared attributes — the ones ControllerModifyVolume
// will keep in sync afterwards — so they win. Geometry cannot collide here:
// resolveMutableTunables rejects volblocksize and friends outright.
func applyMutableTunablesToCreate(params *truenas.DatasetCreateParams, tunables map[string]string) {
	if params == nil || len(tunables) == 0 {
		return
	}
	keys := make([]string, 0, len(tunables))
	for key := range tunables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := tunables[key]
		switch key {
		case zfsPropCompression:
			params.Compression = value
		case zfsPropSync:
			params.Sync = value
		case zfsPropAtime:
			params.Atime = value
		case zfsPropRecordsize:
			params.Recordsize = value
		}
		klog.V(2).Infof("Applying mutable volume attribute %s=%q to %s", key, value, params.Name)
	}
}

// datasetTunableValue returns the dataset's CURRENT value for a live-tunable
// property as pool.dataset.query reported it, or "" when the read did not carry
// one (e.g. a value the projection never delivered). Callers must treat "" as
// "unknown, assume different" — issuing a redundant pool.dataset.update is
// harmless, skipping a needed one on a guess is not.
func datasetTunableValue(ds *truenas.Dataset, prop string) string {
	if ds == nil {
		return ""
	}
	var property truenas.DatasetProperty
	switch prop {
	case zfsPropCompression:
		property = ds.Compression
	case zfsPropSync:
		property = ds.Sync
	case zfsPropAtime:
		property = ds.Atime
	case zfsPropRecordsize:
		property = ds.Recordsize
	}
	if value, ok := property.Value.(string); ok && value != "" {
		return value
	}
	return property.Rawvalue
}

// buildMutableTunableUpdate diffs the resolved tunables against the dataset's
// current values and returns ONE pool.dataset.update payload carrying only the
// properties that would actually change, plus the sorted "prop=value" list of
// those changes. An empty list means the volume already matches and NO update
// call may be made — CSI MODIFY_VOLUME is idempotent and a byte-identical
// pool.dataset.update is still a wasted round trip.
func buildMutableTunableUpdate(ds *truenas.Dataset, tunables map[string]string) (params *truenas.DatasetUpdateParams, changed []string) {
	params = &truenas.DatasetUpdateParams{}
	keys := make([]string, 0, len(tunables))
	for key := range tunables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		want := tunables[key]
		current := strings.TrimSpace(datasetTunableValue(ds, key))
		if current != "" && strings.EqualFold(current, want) {
			continue
		}
		switch key {
		case zfsPropCompression:
			params.Compression = want
		case zfsPropSync:
			params.Sync = want
		case zfsPropAtime:
			params.Atime = want
		case zfsPropRecordsize:
			params.Recordsize = want
		}
		changed = append(changed, key+"="+want)
	}
	return params, changed
}

// applyMutableTunables issues the single diff-derived pool.dataset.update for a
// volume, returning the list of properties it changed (empty for a no-op, in
// which case no API call was made). Shared by ControllerModifyVolume, the
// content-source arm of CreateVolume (a clone/restore accepts no create-time
// property payload, but every mutable tunable is live-tunable, so one update
// after materialization honors the VolumeAttributesClass there too), and
// createVolumeExisting (a retry that resumes after the post-materialization
// apply failed must re-apply, or the PVC would bind with recorded attributes
// the dataset does not carry; the diff makes an already-converged retry free).
func (d *Driver) applyMutableTunables(ctx context.Context, datasetName string, ds *truenas.Dataset, tunables map[string]string) ([]string, error) {
	if len(tunables) == 0 {
		return nil, nil
	}
	params, changed := buildMutableTunableUpdate(ds, tunables)
	if len(changed) == 0 {
		return nil, nil
	}
	if _, err := d.truenasClient.DatasetUpdate(ctx, datasetName, params); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply mutable parameters (%s): %v",
			strings.Join(changed, ", "), err)
	}
	return changed, nil
}

// ControllerModifyVolume modifies a volume's mutable attributes (CSI
// MODIFY_VOLUME, Kubernetes VolumeAttributesClass mutable_parameters). Flow and
// error classes mirror ControllerExpandVolume, its closest structural sibling:
// per-volume operation lock, datasetForID, DatasetGet-then-classify, and full
// idempotency (already-matching values return success without touching the
// backend). Capacity is untouchable by construction — every capacity-ish key is
// rejected in resolveMutableTunables.
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (_ *csi.ControllerModifyVolumeResponse, operationErr error) {
	volumeID := req.GetVolumeId()
	defer func() {
		d.recordOperationFailureEvent(volumeEventRef(volumeID), EventReasonVolumeModifyFailed, "ControllerModifyVolume", operationErr)
	}()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	klog.Infof("ControllerModifyVolume: volumeID=%s, mutableParameters=%d", volumeID, len(req.GetMutableParameters()))

	// Lock on volume ID so a modify never interleaves with a concurrent
	// create/delete/expand of the same volume (same key space they use).
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName, err := d.datasetForID(volumeID)
	if err != nil {
		return nil, err
	}

	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume details: %v", err)
	}

	// Only retune datasets this driver manages, per the local-stamp discipline
	// the ownership model uses everywhere (a clone-inherited stamp is not local
	// and does not count). A foreign dataset that merely lives under the parent
	// is not the driver's to modify — FailedPrecondition, the same class the
	// performance-class guard uses for "this volume cannot satisfy the request".
	if !datasetHasLocalUserProperty(ds, PropManagedResource, "true") {
		return nil, status.Errorf(codes.FailedPrecondition,
			"dataset %s is not managed by this driver (no local %s stamp); refusing to modify a dataset the driver does not own",
			datasetName, PropManagedResource)
	}

	// Validate against the dataset's REAL type (a multi-protocol driver cannot
	// infer it from config — see ControllerExpandVolume's mixed-protocol lesson).
	tunables, err := d.resolveMutableTunables(ctx, req.GetMutableParameters(), ds.Type)
	if err != nil {
		return nil, err
	}

	changed, err := d.applyMutableTunables(ctx, datasetName, ds, tunables)
	if err != nil {
		return nil, err
	}
	if len(changed) == 0 {
		klog.V(4).Infof("ControllerModifyVolume: volume %s already matches the requested mutable parameters; no-op", volumeID)
		return &csi.ControllerModifyVolumeResponse{}, nil
	}

	message := fmt.Sprintf("Modified volume %s: %s", volumeID, strings.Join(changed, ", "))
	klog.Info(message)
	d.recordNormalEvent(volumeEventRef(volumeID), EventReasonVolumeModified, message)
	return &csi.ControllerModifyVolumeResponse{}, nil
}
