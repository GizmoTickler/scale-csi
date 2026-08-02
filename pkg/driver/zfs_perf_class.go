package driver

import (
	"context"
	"fmt"
	"sort"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// zfsPerformanceClassParam is the StorageClass parameter that selects a curated,
// vetted ZFS property set instead of hand-tuned zfs.datasetProperties.
const zfsPerformanceClassParam = "zfsPerformanceClass"

// Canonical ZFS property keys the curated classes emit. They are the exact
// spellings pool.dataset.create accepts — in particular
// `special_small_block_size`, NOT the commonly mis-typed `special_small_blocks`,
// which the API rejects.
const (
	zfsPropRecordsize            = "recordsize"
	zfsPropVolblocksize          = "volblocksize"
	zfsPropSync                  = "sync"
	zfsPropLogbias               = "logbias"
	zfsPropCompression           = "compression"
	zfsPropChecksum              = "checksum"
	zfsPropPrimarycache          = "primarycache"
	zfsPropSecondarycache        = "secondarycache"
	zfsPropSpecialSmallBlockSize = "special_small_block_size"
	zfsPropAtime                 = "atime"
	zfsPropCopies                = "copies"
	zfsPropReadonly              = "readonly"
)

// zfsImmutableProperties are fixed at dataset CREATE and cannot be changed
// afterwards. Verified live against pool.dataset.update's accepted-key schema on
// TrueNAS 26.0:
//   - volblocksize — zvol geometry, immutable in ZFS itself
//   - logbias, primarycache, secondarycache — rejected by pool.dataset.update
//     ("Extra inputs are not permitted"), i.e. create-only through this API
//
// Attempting to change any of them on an existing volume is a hard error rather
// than a silently ignored request (risk R1).
var zfsImmutableProperties = map[string]struct{}{
	zfsPropVolblocksize:   {},
	zfsPropLogbias:        {},
	zfsPropPrimarycache:   {},
	zfsPropSecondarycache: {},
}

// zfsLiveTunableProperties can be changed on an existing dataset via
// pool.dataset.update. recordsize/compression/checksum apply to NEW writes only:
// blocks already on disk keep the geometry they were written with.
var zfsLiveTunableProperties = map[string]struct{}{
	zfsPropRecordsize:            {},
	zfsPropSync:                  {},
	zfsPropCompression:           {},
	zfsPropChecksum:              {},
	zfsPropAtime:                 {},
	zfsPropSpecialSmallBlockSize: {},
	zfsPropCopies:                {},
	zfsPropReadonly:              {},
}

// zfsPerformanceClasses are the curated presets. Values were chosen against the
// live choice lists (recordsize/compression/checksum) and the backend's
// recommended zvol blocksize, and are validated again at resolve time.
//
// `special_small_block_size` is emitted ONLY where it is non-zero, and only when
// the pool actually has a `special` allocation-class vdev (risk R8); a zero
// value is the ZFS default and is left unset so the create payload stays minimal.
var zfsPerformanceClasses = map[string]map[string]string{
	// Small random I/O: match the typical 16K page, keep the ZIL on the low
	// latency path, and route small blocks/metadata to the special vdev.
	"database": {
		zfsPropRecordsize:            "16K",
		zfsPropVolblocksize:          "16K",
		zfsPropSync:                  "STANDARD",
		zfsPropLogbias:               "LATENCY",
		zfsPropCompression:           "LZ4",
		zfsPropPrimarycache:          "ALL",
		zfsPropSpecialSmallBlockSize: "16K",
		zfsPropAtime:                 "OFF",
	},
	// Large sequential I/O: big records, throughput-biased ZIL, denser codec.
	"media": {
		zfsPropRecordsize:   "1M",
		zfsPropVolblocksize: "64K",
		zfsPropSync:         "STANDARD",
		zfsPropLogbias:      "THROUGHPUT",
		zfsPropCompression:  "ZSTD",
		zfsPropPrimarycache: "ALL",
		zfsPropAtime:        "OFF",
	},
	// Mixed guest-filesystem I/O behind a zvol.
	"vm": {
		zfsPropRecordsize:   "64K",
		zfsPropVolblocksize: "16K",
		zfsPropSync:         "STANDARD",
		zfsPropLogbias:      "LATENCY",
		zfsPropCompression:  "LZ4",
		zfsPropPrimarycache: "ALL",
		zfsPropAtime:        "OFF",
	},
	// Write-once/read-rarely: do not pollute ARC with data pages.
	"backup": {
		zfsPropRecordsize:   "1M",
		zfsPropVolblocksize: "128K",
		zfsPropSync:         "STANDARD",
		zfsPropLogbias:      "THROUGHPUT",
		zfsPropCompression:  "ZSTD",
		zfsPropPrimarycache: "METADATA",
		zfsPropAtime:        "OFF",
	},
	// The balanced default, close to ZFS's own defaults.
	"general": {
		zfsPropRecordsize:   "128K",
		zfsPropVolblocksize: "16K",
		zfsPropSync:         "STANDARD",
		zfsPropLogbias:      "LATENCY",
		zfsPropCompression:  "LZ4",
		zfsPropPrimarycache: "ALL",
		zfsPropAtime:        "OFF",
	},
}

// zfsFilesystemOnlyProperties / zfsVolumeOnlyProperties mirror the split
// applyDatasetProperties already enforces for operator-supplied properties.
var zfsFilesystemOnlyProperties = map[string]struct{}{
	zfsPropRecordsize: {},
	zfsPropAtime:      {},
}

var zfsVolumeOnlyProperties = map[string]struct{}{
	zfsPropVolblocksize: {},
}

func sortedPerformanceClasses() []string {
	classes := make([]string, 0, len(zfsPerformanceClasses))
	for class := range zfsPerformanceClasses {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	return classes
}

// zfsPerformanceClassFromParams validates and normalizes the StorageClass
// parameter. An absent parameter returns "" — the default — which leaves the
// dataset create payload exactly as it has always been (properties inherited
// from the parent, plus whatever zfs.datasetProperties configures).
func zfsPerformanceClassFromParams(params map[string]string) (string, error) {
	raw, ok := params[zfsPerformanceClassParam]
	if !ok {
		return "", nil
	}
	class := strings.ToLower(strings.TrimSpace(raw))
	if class == "" {
		return "", status.Errorf(codes.InvalidArgument,
			"StorageClass parameter %s is set but empty; omit it to inherit the parent dataset's properties",
			zfsPerformanceClassParam)
	}
	if _, ok := zfsPerformanceClasses[class]; !ok {
		return "", status.Errorf(codes.InvalidArgument,
			"invalid StorageClass parameter %s value %q; valid options are: %s",
			zfsPerformanceClassParam, raw, strings.Join(sortedPerformanceClasses(), ", "))
	}
	return class, nil
}

// contentSourceKind names a CreateVolume content source for operator-facing
// messages.
func contentSourceKind(source *csi.VolumeContentSource) string {
	switch {
	case source.GetSnapshot() != nil:
		return "snapshot"
	case source.GetVolume() != nil:
		return "volume"
	default:
		return "volume content"
	}
}

type zfsPerformanceClassContextKey struct{}

func withZFSPerformanceClass(ctx context.Context, class string) context.Context {
	if class == "" {
		return ctx
	}
	return context.WithValue(ctx, zfsPerformanceClassContextKey{}, class)
}

func zfsPerformanceClassFromContext(ctx context.Context) string {
	class, _ := ctx.Value(zfsPerformanceClassContextKey{}).(string)
	return class
}

// resolvePerformanceClassProperties returns the property map a class contributes
// to a dataset of the given type, after dropping the properties that do not
// apply to that type and any the backend does not support.
func (d *Driver) resolvePerformanceClassProperties(ctx context.Context, class, datasetType string) (map[string]string, error) {
	preset, ok := zfsPerformanceClasses[class]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument,
			"invalid ZFS performance class %q; valid options are: %s", class, strings.Join(sortedPerformanceClasses(), ", "))
	}

	resolved := make(map[string]string, len(preset))
	for key, value := range preset {
		if datasetType == "VOLUME" {
			if _, filesystemOnly := zfsFilesystemOnlyProperties[key]; filesystemOnly {
				continue
			}
		} else if _, volumeOnly := zfsVolumeOnlyProperties[key]; volumeOnly {
			continue
		}
		resolved[key] = value
	}

	if err := d.validatePerformanceClassValues(ctx, class, resolved); err != nil {
		return nil, err
	}

	// R8: special_small_block_size without a `special` allocation-class vdev is
	// meaningless. Drop it with a warning rather than failing provisioning.
	if size, ok := resolved[zfsPropSpecialSmallBlockSize]; ok {
		hasSpecial, err := d.poolHasSpecialVdev(ctx)
		switch {
		case err != nil:
			klog.Warningf("Could not verify a special vdev for %s=%s (class %s); applying it anyway: %v",
				zfsPropSpecialSmallBlockSize, size, class, err)
		case !hasSpecial:
			klog.Warningf("Dropping %s=%s from ZFS performance class %s: pool %s has no special allocation-class vdev",
				zfsPropSpecialSmallBlockSize, size, class, d.parentPoolName())
			delete(resolved, zfsPropSpecialSmallBlockSize)
		}
	}
	return resolved, nil
}

// validatePerformanceClassValues checks every curated value against the live
// backend choice lists so an unsupported value becomes an InvalidArgument at
// CreateVolume instead of an opaque pool.dataset.create rejection. When the
// backend does not report a list, validation is skipped (fail-open) rather than
// inventing a restriction.
func (d *Driver) validatePerformanceClassValues(ctx context.Context, class string, properties map[string]string) error {
	choices, err := d.zfsPropertyChoices(ctx)
	if err != nil {
		klog.Warningf("ZFS property choice validation skipped for class %s: %v", class, err)
		return nil
	}
	checks := []struct {
		key   string
		allow func(string) (allowed, known bool)
	}{
		{zfsPropRecordsize, choices.AllowsRecordsize},
		{zfsPropCompression, choices.AllowsCompression},
		{zfsPropChecksum, choices.AllowsChecksum},
	}
	for _, check := range checks {
		value, ok := properties[check.key]
		if !ok {
			continue
		}
		allowed, known := check.allow(value)
		if known && !allowed {
			return status.Errorf(codes.InvalidArgument,
				"ZFS performance class %s sets %s=%s which this TrueNAS does not accept", class, check.key, value)
		}
	}
	return nil
}

// applyPerformanceClassProperties layers the curated preset onto a dataset
// create payload. It runs BEFORE applyDatasetProperties, so an explicit
// zfs.datasetProperties key always wins — the class is the floor, not a ceiling.
//
// EXCEPTION, matching pre-existing behavior: volblocksize set here (like the
// zfs.zvolBlocksize config default) makes applyDatasetProperties warn-and-skip a
// datasetProperties volblocksize. Zvol geometry has exactly one owner.
func applyPerformanceClassProperties(params *truenas.DatasetCreateParams, properties map[string]string) {
	if params == nil || len(properties) == 0 {
		return
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := properties[key]
		switch key {
		case zfsPropRecordsize:
			params.Recordsize = value
		case zfsPropVolblocksize:
			params.Volblocksize = value
		case zfsPropSync:
			params.Sync = value
		case zfsPropLogbias:
			params.Logbias = value
		case zfsPropCompression:
			params.Compression = value
		case zfsPropChecksum:
			params.Checksum = value
		case zfsPropPrimarycache:
			params.Primarycache = value
		case zfsPropSecondarycache:
			params.Secondarycache = value
		case zfsPropSpecialSmallBlockSize:
			params.SpecialSmallBlockSize = value
		case zfsPropAtime:
			params.Atime = value
		case zfsPropReadonly:
			params.Readonly = value
		}
		klog.V(2).Infof("Applying curated ZFS performance property %s=%q to %s", key, value, params.Name)
	}
}

// ---------------------------------------------------------------------------
// Immutability guard (risk R1)
// ---------------------------------------------------------------------------

// zfsPropertyMutationError describes a rejected attempt to change create-only
// ZFS properties on an existing volume.
type zfsPropertyMutationError struct {
	Volume     string
	Properties []string
	From, To   string
}

func (e *zfsPropertyMutationError) Error() string {
	return fmt.Sprintf(
		"volume %s cannot change ZFS performance class %s -> %s: %s are fixed when the dataset is created and cannot be modified afterwards; provision a new volume with the desired class and migrate the data",
		e.Volume, e.From, e.To, strings.Join(e.Properties, ", "))
}

// guardImmutableZFSProperties is THE single gate every property-changing path
// must pass. It returns the create-only properties whose value would change
// between two resolved property maps.
//
// This is not advisory: volblocksize is immutable in ZFS itself, and
// logbias/primarycache/secondarycache are rejected outright by
// pool.dataset.update. Letting a user believe a StorageClass edit re-tuned an
// existing volume's zvol geometry would be a silent correctness lie.
func guardImmutableZFSProperties(current, desired map[string]string) []string {
	var changed []string
	for key := range zfsImmutableProperties {
		currentValue, hasCurrent := current[key]
		desiredValue, hasDesired := desired[key]
		if !hasDesired {
			continue
		}
		if !hasCurrent || !strings.EqualFold(strings.TrimSpace(currentValue), strings.TrimSpace(desiredValue)) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// liveTunableZFSPropertyDiff returns the live-tunable properties whose value
// would change. They CAN be re-applied via pool.dataset.update, with the caveat
// that recordsize/compression/checksum affect NEW writes only.
func liveTunableZFSPropertyDiff(current, desired map[string]string) []string {
	var changed []string
	for key := range zfsLiveTunableProperties {
		desiredValue, hasDesired := desired[key]
		if !hasDesired {
			continue
		}
		if currentValue, hasCurrent := current[key]; !hasCurrent ||
			!strings.EqualFold(strings.TrimSpace(currentValue), strings.TrimSpace(desiredValue)) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

// guardPerformanceClassChange enforces the create-only rule when an existing
// volume's StorageClass now asks for a DIFFERENT curated class.
//
//   - Immutable properties would change  -> hard FailedPrecondition. The request
//     is impossible to satisfy in place, and pretending otherwise would leave a
//     volume whose real geometry silently disagrees with its class.
//   - Only live-tunable properties differ -> allowed, with a loud warning that
//     the driver does NOT retroactively rewrite an existing volume's properties
//     and that recordsize-style changes would only affect new writes anyway.
//   - The volume carries no stamp -> warn only; there is nothing to compare
//     against and an unstamped healthy volume must not be wedged. Two distinct
//     populations land here, and BOTH are honest: volumes that predate the
//     feature, and clone/restore volumes, which are deliberately never stamped
//     because the curated properties were never applied to them (a ZFS clone
//     inherits the origin's geometry). Stamping a clone would make this guard
//     compare a declared class against geometry that does not exist — the exact
//     false-accept / false-reject pair the stamp is supposed to rule out.
func (d *Driver) guardPerformanceClassChange(ctx context.Context, volumeID, storedClass, requestedClass, datasetType string) error {
	if requestedClass == "" || requestedClass == storedClass {
		return nil
	}
	if storedClass == "" {
		klog.Warningf("Volume %s has no recorded ZFS performance class; StorageClass now requests %q. "+
			"Existing datasets are NOT retuned; the class applies to newly provisioned volumes only.", volumeID, requestedClass)
		return nil
	}

	current, err := d.resolvePerformanceClassProperties(ctx, storedClass, datasetType)
	if err != nil {
		// An unknown stored class cannot prove the change safe; refuse.
		return status.Errorf(codes.FailedPrecondition,
			"volume %s records ZFS performance class %q which this driver cannot resolve; refusing to reinterpret it as %q",
			volumeID, storedClass, requestedClass)
	}
	desired, err := d.resolvePerformanceClassProperties(ctx, requestedClass, datasetType)
	if err != nil {
		return err
	}

	if immutable := guardImmutableZFSProperties(current, desired); len(immutable) > 0 {
		mutationErr := &zfsPropertyMutationError{
			Volume: volumeID, Properties: immutable, From: storedClass, To: requestedClass,
		}
		return status.Error(codes.FailedPrecondition, mutationErr.Error())
	}

	if tunable := liveTunableZFSPropertyDiff(current, desired); len(tunable) > 0 {
		klog.Warningf("Volume %s keeps ZFS performance class %s: only live-tunable properties (%s) differ from the requested %s, "+
			"and the driver does not retune existing datasets. Note that recordsize/compression/checksum changes affect NEW writes only.",
			volumeID, storedClass, strings.Join(tunable, ", "), requestedClass)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Cached backend lookups
// ---------------------------------------------------------------------------

// zfsPropertyChoices reads the backend choice lists at most once per controller
// lifetime, and only when a curated class is actually requested.
func (d *Driver) zfsPropertyChoices(ctx context.Context) (*truenas.ZFSPropertyChoices, error) {
	d.zfsChoicesMu.Lock()
	defer d.zfsChoicesMu.Unlock()
	if d.zfsChoices != nil {
		return d.zfsChoices, nil
	}
	if d.zfsChoicesErr != nil {
		// Do not re-hammer a backend that already refused; validation is fail-open.
		return nil, d.zfsChoicesErr
	}
	choices, err := d.truenasClient.ZFSPropertyChoices(ctx)
	if err != nil {
		d.zfsChoicesErr = err
		return nil, err
	}
	d.zfsChoices = choices
	return choices, nil
}

// poolHasSpecialVdev caches whether the parent dataset's pool has a special
// allocation-class vdev.
func (d *Driver) poolHasSpecialVdev(ctx context.Context) (bool, error) {
	pool := d.parentPoolName()
	if pool == "" {
		return false, fmt.Errorf("no parent dataset configured")
	}
	d.zfsChoicesMu.Lock()
	defer d.zfsChoicesMu.Unlock()
	if d.specialVdevChecked {
		return d.specialVdevPresent, nil
	}
	present, err := d.truenasClient.PoolHasSpecialVdev(ctx, pool)
	if err != nil {
		return false, err
	}
	d.specialVdevChecked = true
	d.specialVdevPresent = present
	return present, nil
}

// parentPoolName is the pool component of zfs.parentDataset.
func (d *Driver) parentPoolName() string {
	parent := d.parentDatasetName()
	if index := strings.Index(parent, "/"); index > 0 {
		return parent[:index]
	}
	return parent
}
