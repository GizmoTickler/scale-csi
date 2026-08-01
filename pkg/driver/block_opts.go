package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// StorageClass parameter keys for block-protocol tuning. Every key is optional;
// an absent key falls back to the controller-wide default (or the backend
// default when there is none), so a StorageClass that sets none of these
// provisions exactly as it did before these knobs existed.
const (
	paramISCSIBlocksize      = "iscsi/blocksize"
	paramISCSIPblocksize     = "iscsi/pblocksize"
	paramISCSIQueuedCommands = "iscsi/queuedCommands"
	paramISCSIInsecureTpc    = "iscsi/insecureTpc"
	paramISCSIReadOnly       = "iscsi/readOnly"
	paramISCSIAvailThreshold = "iscsi/availThreshold"
	paramISCSIStableSerial   = "iscsi/stableSerial"
	paramISCSIAuthNetworks   = "iscsi/authNetworks"
	paramNVMeoFQidMax        = "nvmeof/qidMax"
	paramNVMeoFPiEnable      = "nvmeof/piEnable"
)

// Bounds for nvmeof/qidMax. An NVMe queue identifier is a 16-bit field, so
// 65535 is the hard ceiling the backend can accept (F-8).
const (
	nvmeoFQidMaxMin = 1
	nvmeoFQidMaxMax = 65535
)

// blockOpts carries the resolved per-volume block-protocol tuning. Pointer
// fields are nil when the StorageClass did not opt in, which means "omit the
// API parameter / keep the historical behavior". This is what preserves the
// byte-identical contract for volumes whose StorageClass sets nothing.
type blockOpts struct {
	iscsiBlocksize      *int
	iscsiPblocksize     *bool
	iscsiQueuedCommands *int
	iscsiInsecureTpc    *bool
	iscsiReadOnly       *bool
	iscsiAvailThreshold *int
	iscsiSerial         string
	iscsiAuthNetworks   []string
	nvmeofQidMax        *int
	nvmeofPiEnable      *bool
}

// blockOptsContextKey carries the request-scoped resolution from CreateVolume
// down to the share builders without widening the ShareBackend interface (which
// is shared by NFS and carries no block-protocol tuning). Mirrors the CHAP
// resolution threading.
//
// The context is ONLY a CreateVolume-request carrier. It is never the source of
// truth: every path that rebuilds or re-ensures a share for an EXISTING volume
// (ControllerPublishVolume, the startup attachment reconcile, a DR/restore
// rebuild) has no request context, so resolution MUST fall through to the
// volume's stored dataset properties. See effectiveBlockOpts.
type blockOptsContextKey struct{}

func withBlockOpts(ctx context.Context, opts *blockOpts) context.Context {
	if opts == nil {
		return ctx
	}
	return context.WithValue(ctx, blockOptsContextKey{}, opts)
}

func blockOptsFromContext(ctx context.Context) *blockOpts {
	if opts, ok := ctx.Value(blockOptsContextKey{}).(*blockOpts); ok {
		return opts
	}
	return nil
}

// resolveBlockOpts parses and validates the block-protocol StorageClass
// parameters. volumeName seeds the deterministic stable serial. It returns a
// non-nil *blockOpts only when at least one knob is set; a nil result means
// "provision exactly as before".
func resolveBlockOpts(params map[string]string, volumeName string) (*blockOpts, error) {
	if len(params) == 0 {
		return nil, nil
	}
	opts := &blockOpts{}
	set := false

	if raw, ok := params[paramISCSIBlocksize]; ok {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be an integer, got %q", paramISCSIBlocksize, raw)
		}
		switch value {
		case 512, 1024, 2048, 4096:
		default:
			return nil, status.Errorf(codes.InvalidArgument, "%s must be one of 512, 1024, 2048, 4096; got %d", paramISCSIBlocksize, value)
		}
		opts.iscsiBlocksize = &value
		set = true
	}

	if raw, ok := params[paramISCSIPblocksize]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a boolean, got %q", paramISCSIPblocksize, raw)
		}
		opts.iscsiPblocksize = &value
		set = true
	}

	if raw, ok := params[paramISCSIQueuedCommands]; ok {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be an integer, got %q", paramISCSIQueuedCommands, raw)
		}
		switch value {
		case 32, 128:
		default:
			return nil, status.Errorf(codes.InvalidArgument, "%s must be one of 32, 128; got %d", paramISCSIQueuedCommands, value)
		}
		opts.iscsiQueuedCommands = &value
		set = true
	}

	if raw, ok := params[paramISCSIInsecureTpc]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a boolean, got %q", paramISCSIInsecureTpc, raw)
		}
		opts.iscsiInsecureTpc = &value
		set = true
	}

	if raw, ok := params[paramISCSIReadOnly]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a boolean, got %q", paramISCSIReadOnly, raw)
		}
		opts.iscsiReadOnly = &value
		set = true
	}

	if raw, ok := params[paramISCSIAvailThreshold]; ok {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be an integer, got %q", paramISCSIAvailThreshold, raw)
		}
		if value < 1 || value > 99 {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be between 1 and 99; got %d", paramISCSIAvailThreshold, value)
		}
		opts.iscsiAvailThreshold = &value
		set = true
	}

	if raw, ok := params[paramISCSIStableSerial]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a boolean, got %q", paramISCSIStableSerial, raw)
		}
		if value {
			opts.iscsiSerial = stableISCSISerial(volumeName)
			set = true
		}
	}

	if raw, ok := params[paramISCSIAuthNetworks]; ok {
		networks, err := parseAuthNetworks(raw)
		if err != nil {
			return nil, err
		}
		opts.iscsiAuthNetworks = networks
		set = true
	}

	if raw, ok := params[paramNVMeoFQidMax]; ok {
		value, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be an integer, got %q", paramNVMeoFQidMax, raw)
		}
		// Bounded on BOTH sides (F-8). An NVMe queue identifier is 16 bits, so
		// qid_max cannot exceed 65535; an unbounded value used to reach
		// nvmet.subsys.create and surface as an opaque Internal error instead of
		// being rejected as InvalidArgument at admission.
		if value < nvmeoFQidMaxMin || value > nvmeoFQidMaxMax {
			return nil, status.Errorf(codes.InvalidArgument,
				"%s must be between %d and %d (an NVMe queue identifier is 16-bit); got %d",
				paramNVMeoFQidMax, nvmeoFQidMaxMin, nvmeoFQidMaxMax, value)
		}
		opts.nvmeofQidMax = &value
		set = true
	}

	if raw, ok := params[paramNVMeoFPiEnable]; ok {
		value, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a boolean, got %q", paramNVMeoFPiEnable, raw)
		}
		opts.nvmeofPiEnable = &value
		set = true
	}

	if !set {
		return nil, nil
	}
	return opts, nil
}

// storedProperties renders the resolved options as dataset user properties.
// ONLY options that were actually SET produce a key: an absent key means "use
// the controller default", so a StorageClass that opts into nothing yields nil
// and the dataset write is byte-identical to pre-GF4. Returns nil when there is
// nothing to persist.
func (o *blockOpts) storedProperties() map[string]string {
	if o == nil {
		return nil
	}
	props := make(map[string]string, 10)
	if o.iscsiBlocksize != nil {
		props[PropBlockISCSIBlocksize] = strconv.Itoa(*o.iscsiBlocksize)
	}
	if o.iscsiPblocksize != nil {
		props[PropBlockISCSIPblocksize] = strconv.FormatBool(*o.iscsiPblocksize)
	}
	if o.iscsiQueuedCommands != nil {
		props[PropBlockISCSIQueuedCommands] = strconv.Itoa(*o.iscsiQueuedCommands)
	}
	if o.iscsiInsecureTpc != nil {
		props[PropBlockISCSIInsecureTpc] = strconv.FormatBool(*o.iscsiInsecureTpc)
	}
	if o.iscsiReadOnly != nil {
		props[PropBlockISCSIReadOnly] = strconv.FormatBool(*o.iscsiReadOnly)
	}
	if o.iscsiAvailThreshold != nil {
		props[PropBlockISCSIAvailThreshold] = strconv.Itoa(*o.iscsiAvailThreshold)
	}
	if o.iscsiSerial != "" {
		props[PropBlockISCSISerial] = o.iscsiSerial
	}
	if len(o.iscsiAuthNetworks) > 0 {
		props[PropBlockISCSIAuthNetworks] = strings.Join(o.iscsiAuthNetworks, ",")
	}
	if o.nvmeofQidMax != nil {
		props[PropBlockNVMeoFQidMax] = strconv.Itoa(*o.nvmeofQidMax)
	}
	if o.nvmeofPiEnable != nil {
		props[PropBlockNVMeoFPiEnable] = strconv.FormatBool(*o.nvmeofPiEnable)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

// blockOptsProps returns the durable per-volume block-tuning properties for a
// CreateVolume request, or nil when the request opts into nothing / is not a
// block protocol. It is the single source of truth for folding the tuning into
// a dataset write, mirroring iscsiCHAPPolicyProps: the fresh-create path folds
// it into the FATAL managed-property update and the clone paths fold it into
// their atomic ownership+content-source write, so the stamp is durable-or-
// rolled-back with the rest of provisioning rather than best-effort.
func blockOptsProps(ctx context.Context, shareType ShareType) map[string]string {
	if !shareType.IsBlockProtocol() {
		return nil
	}
	return blockOptsFromContext(ctx).storedProperties()
}

// blockOptsFromDataset reconstructs the per-volume block tuning from the
// dataset's stored user properties. This is what makes rebuild/publish/reconcile
// paths — which carry no request context — resolve the volume's OWN options.
//
// Parsing is deliberately tolerant: a malformed property is logged and skipped
// rather than failing an attach, because a publish must never be wedged by a
// corrupt advisory stamp. A missing key simply falls through to the controller
// default, which is the pre-GF4 behavior.
func blockOptsFromDataset(ds *truenas.Dataset) *blockOpts {
	if ds == nil {
		return nil
	}
	opts := &blockOpts{}
	set := false

	if value, ok := storedBlockInt(ds, PropBlockISCSIBlocksize); ok {
		opts.iscsiBlocksize = &value
		set = true
	}
	if value, ok := storedBlockBool(ds, PropBlockISCSIPblocksize); ok {
		opts.iscsiPblocksize = &value
		set = true
	}
	if value, ok := storedBlockInt(ds, PropBlockISCSIQueuedCommands); ok {
		opts.iscsiQueuedCommands = &value
		set = true
	}
	if value, ok := storedBlockBool(ds, PropBlockISCSIInsecureTpc); ok {
		opts.iscsiInsecureTpc = &value
		set = true
	}
	if value, ok := storedBlockBool(ds, PropBlockISCSIReadOnly); ok {
		opts.iscsiReadOnly = &value
		set = true
	}
	if value, ok := storedBlockInt(ds, PropBlockISCSIAvailThreshold); ok {
		opts.iscsiAvailThreshold = &value
		set = true
	}
	if raw := storedBlockRaw(ds, PropBlockISCSISerial); raw != "" {
		opts.iscsiSerial = raw
		set = true
	}
	if raw := storedBlockRaw(ds, PropBlockISCSIAuthNetworks); raw != "" {
		networks, err := parseAuthNetworks(raw)
		if err != nil {
			klog.Warningf("Ignoring malformed stored block property %s=%q: %v", PropBlockISCSIAuthNetworks, raw, err)
		} else if len(networks) > 0 {
			opts.iscsiAuthNetworks = networks
			set = true
		}
	}
	if value, ok := storedBlockInt(ds, PropBlockNVMeoFQidMax); ok {
		opts.nvmeofQidMax = &value
		set = true
	}
	if value, ok := storedBlockBool(ds, PropBlockNVMeoFPiEnable); ok {
		opts.nvmeofPiEnable = &value
		set = true
	}

	if !set {
		return nil
	}
	return opts
}

// storedBlockRaw reads a stored block-tuning property. Unlike the CHAP reader it
// does NOT require source=="local": geometry describes the on-disk data layout,
// which a ZFS clone shares byte-for-byte with its source, so an inherited value
// is the correct one for the clone (and the conservative direction — it keeps a
// 4096 clone at 4096 rather than silently reverting it to the 512 default).
func storedBlockRaw(ds *truenas.Dataset, key string) string {
	value := strings.TrimSpace(datasetUserProperty(ds, key))
	if value == "-" {
		return ""
	}
	return value
}

func storedBlockInt(ds *truenas.Dataset, key string) (int, bool) {
	raw := storedBlockRaw(ds, key)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		klog.Warningf("Ignoring malformed stored block property %s=%q: %v", key, raw, err)
		return 0, false
	}
	return value, true
}

func storedBlockBool(ds *truenas.Dataset, key string) (value, ok bool) {
	raw := storedBlockRaw(ds, key)
	if raw == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		klog.Warningf("Ignoring malformed stored block property %s=%q: %v", key, raw, err)
		return false, false
	}
	return parsed, true
}

// mergeBlockOpts overlays the request-scoped resolution onto the stored
// per-volume resolution, PER KEY. A key the request does not set keeps the
// stored value, so a StorageClass that changes only (say) queuedCommands cannot
// silently reset a volume's geometry to the controller default.
func mergeBlockOpts(request, stored *blockOpts) *blockOpts {
	if request == nil {
		return stored
	}
	if stored == nil {
		return request
	}
	merged := *request
	if merged.iscsiBlocksize == nil {
		merged.iscsiBlocksize = stored.iscsiBlocksize
	}
	if merged.iscsiPblocksize == nil {
		merged.iscsiPblocksize = stored.iscsiPblocksize
	}
	if merged.iscsiQueuedCommands == nil {
		merged.iscsiQueuedCommands = stored.iscsiQueuedCommands
	}
	if merged.iscsiInsecureTpc == nil {
		merged.iscsiInsecureTpc = stored.iscsiInsecureTpc
	}
	if merged.iscsiReadOnly == nil {
		merged.iscsiReadOnly = stored.iscsiReadOnly
	}
	if merged.iscsiAvailThreshold == nil {
		merged.iscsiAvailThreshold = stored.iscsiAvailThreshold
	}
	if merged.iscsiSerial == "" {
		merged.iscsiSerial = stored.iscsiSerial
	}
	if len(merged.iscsiAuthNetworks) == 0 {
		merged.iscsiAuthNetworks = stored.iscsiAuthNetworks
	}
	if merged.nvmeofQidMax == nil {
		merged.nvmeofQidMax = stored.nvmeofQidMax
	}
	if merged.nvmeofPiEnable == nil {
		merged.nvmeofPiEnable = stored.nvmeofPiEnable
	}
	return &merged
}

// effectiveBlockOpts is THE resolver. Every call site that builds or rebuilds a
// block share uses it, so there is exactly one resolution order in the tree:
//
//	request-scoped StorageClass opts (CreateVolume only)
//	  -> the volume's STORED dataset properties (rebuild / publish / reconcile)
//	    -> the controller-wide default (the resolved* helpers below)
//
// A nil result means "nothing was ever opted into", which is the pre-GF4 path.
func effectiveBlockOpts(ctx context.Context, ds *truenas.Dataset) *blockOpts {
	return mergeBlockOpts(blockOptsFromContext(ctx), blockOptsFromDataset(ds))
}

// parseAuthNetworks splits a comma-separated list of CIDRs and validates each
// entry. An empty/whitespace value yields an empty list (treated as unset).
func parseAuthNetworks(raw string) ([]string, error) {
	fields := strings.Split(raw, ",")
	networks := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, _, err := net.ParseCIDR(field); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s entry %q is not a valid CIDR: %v", paramISCSIAuthNetworks, field, err)
		}
		networks = append(networks, field)
	}
	return networks, nil
}

// stableISCSISerial derives a deterministic 16-char SCSI serial from the volume
// name so the identity survives extent delete+recreate. It is stable for the
// life of the volume and unique per distinct volume name.
func stableISCSISerial(volumeName string) string {
	sum := sha256.Sum256([]byte("scale-csi-iscsi-serial:" + volumeName))
	return hex.EncodeToString(sum[:8])
}

// iscsiExtentCreateOpts builds the truenas extent options from the resolved
// per-volume tuning. The returned options only carry fields the StorageClass
// opted into; everything else stays at the historical create default.
func (o *blockOpts) iscsiExtentCreateOpts() truenas.ISCSIExtentCreateOptions {
	var opts truenas.ISCSIExtentCreateOptions
	if o == nil {
		return opts
	}
	opts.InsecureTpc = o.iscsiInsecureTpc
	opts.ReadOnly = o.iscsiReadOnly
	opts.AvailThreshold = o.iscsiAvailThreshold
	opts.Serial = o.iscsiSerial
	return opts
}

// iscsiTargetCreateOpts builds the truenas target options from the resolved
// per-volume tuning.
func (o *blockOpts) iscsiTargetCreateOpts() truenas.ISCSITargetCreateOptions {
	var opts truenas.ISCSITargetCreateOptions
	if o == nil {
		return opts
	}
	opts.QueuedCommands = o.iscsiQueuedCommands
	opts.AuthNetworks = o.iscsiAuthNetworks
	return opts
}

// nvmeofSubsystemCreateOpts builds the truenas subsystem options from the
// resolved per-volume tuning.
func (o *blockOpts) nvmeofSubsystemCreateOpts() truenas.NVMeoFSubsystemCreateOptions {
	var opts truenas.NVMeoFSubsystemCreateOptions
	if o == nil {
		return opts
	}
	opts.QidMax = o.nvmeofQidMax
	opts.PiEnable = o.nvmeofPiEnable
	return opts
}

// resolvedISCSIBlocksize returns the blocksize to create an extent with: the
// per-SC override when set, else the controller-wide default.
func (o *blockOpts) resolvedISCSIBlocksize(controllerDefault int) int {
	if o != nil && o.iscsiBlocksize != nil {
		return *o.iscsiBlocksize
	}
	return controllerDefault
}

// resolvedISCSIPblocksize returns the physical-blocksize reporting flag: the
// per-SC override when set, else !extentDisablePhysicalBlocksize.
func (o *blockOpts) resolvedISCSIPblocksize(disablePhysicalBlocksize bool) bool {
	if o != nil && o.iscsiPblocksize != nil {
		return *o.iscsiPblocksize
	}
	return !disablePhysicalBlocksize
}

// hasGeometryOpinion reports whether the caller explicitly asked for a logical
// data layout (blocksize / pblocksize). Only these two describe how bytes are
// addressed on the media, so only these two have to be reconciled against a
// clone SOURCE — the rest describe the volume's own share objects, which a clone
// gets fresh. Used to keep the clone-source geometry probe off the default path
// entirely (zero extra API round trips when no class opts into geometry).
func (o *blockOpts) hasGeometryOpinion() bool {
	return o != nil && (o.iscsiBlocksize != nil || o.iscsiPblocksize != nil)
}

// requestedISCSIBlocksize returns the blocksize the caller has an OPINION about
// — the per-SC override or the volume's stored geometry — and nil when neither
// exists. It is deliberately NOT defaulted to the controller-wide blocksize:
// the immutability guard must only fire on a genuine, explicit divergence. A
// publish/reconcile that supplies no opts against a volume with no stamp has no
// opinion at all and must never be rejected (that false positive made every
// 4096 volume permanently unattachable).
func (o *blockOpts) requestedISCSIBlocksize() *int {
	if o == nil {
		return nil
	}
	return o.iscsiBlocksize
}

// guardISCSIBlocksizeImmutability enforces R-1 against the LIVE extent: an
// extent's blocksize is fixed for the life of the volume, so a request that
// resolves a different blocksize is rejected rather than silently keeping a
// divergent geometry (which would desync the StorageClass contract from the
// backend). requested is nil when the caller has no geometry opinion (a no-opts
// publish on an unstamped volume), in which case the guard is a no-op. A zero
// stored blocksize (legacy extent predating blocksize reporting) is not proof of
// a mismatch and is left alone.
func guardISCSIBlocksizeImmutability(existing *truenas.ISCSIExtent, requested *int, datasetName string) error {
	if existing == nil || existing.Blocksize == 0 || requested == nil || *requested == 0 || existing.Blocksize == *requested {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition,
		"iSCSI extent for %s already exists with immutable blocksize %d; requested %d. "+
			"%s is fixed for the life of the volume and cannot be changed on an extent that holds data",
		datasetName, existing.Blocksize, *requested, paramISCSIBlocksize)
}

// guardStoredBlockGeometry enforces R-1 against the volume's STORED geometry,
// which is the half the live-extent guard structurally cannot cover: on a
// DR/restore rebuild the extent is ABSENT, so there is nothing to compare and
// the create used to fall through to the controller default. Comparing the
// request against the persisted geometry fails closed on a genuine StorageClass
// change even when no extent exists.
//
// It fires ONLY when both sides are explicitly set. A rebuild with no request
// opts (stored-only) is the normal healing path and must succeed — it re-creates
// with the stored geometry. A fresh volume with no stamp (request-only) is a
// create and must succeed.
func guardStoredBlockGeometry(stored, request *blockOpts, datasetName string) error {
	if stored == nil || request == nil {
		return nil
	}
	if stored.iscsiBlocksize != nil && request.iscsiBlocksize != nil && *stored.iscsiBlocksize != *request.iscsiBlocksize {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s was provisioned with immutable iSCSI extent blocksize %d; the StorageClass now resolves %d. "+
				"%s is fixed for the life of the volume (its filesystem and partition table are laid out against "+
				"that logical block size) — provision a new volume to change it",
			datasetName, *stored.iscsiBlocksize, *request.iscsiBlocksize, paramISCSIBlocksize)
	}
	if stored.iscsiPblocksize != nil && request.iscsiPblocksize != nil && *stored.iscsiPblocksize != *request.iscsiPblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s was provisioned with immutable iSCSI physical-blocksize reporting %t; the StorageClass now resolves %t. "+
				"%s is fixed at extent create and changes the alignment the initiator optimizes for — "+
				"provision a new volume to change it",
			datasetName, *stored.iscsiPblocksize, *request.iscsiPblocksize, paramISCSIPblocksize)
	}
	return nil
}

// guardCloneSourceGeometry enforces R-1 on the CLONE / SNAPSHOT-RESTORE path,
// which guardStoredBlockGeometry structurally cannot cover (N-1).
//
// A clone's own stamp is written by the clone fold from the REQUEST's options
// BEFORE the share builder runs, so by the time guardStoredBlockGeometry reads
// the destination it compares the request against itself and always agrees — the
// inherited source geometry has already been overwritten. The only honest
// comparison is against the SOURCE dataset's stored geometry, taken before any
// destination write.
//
// This matters because a ZFS clone shares its source's data byte-for-byte: the
// filesystem and partition table on it were laid out against the SOURCE's
// logical block size. Kubernetes restricts PVC-to-PVC cloning to one
// StorageClass but places NO such restriction on restoring a VolumeSnapshot into
// a different class, so "restore a 4096 volume into a 512 class" is reachable in
// exactly the deployment two differently-tuned classes invite.
//
// It fires ONLY when both sides are explicit. A restore into a class that opts
// into nothing inherits the source geometry (correct, and verified) and is left
// alone; a source with no stamp has no recorded geometry to contradict.
func guardCloneSourceGeometry(sourceOpts, request *blockOpts, sourceRef, datasetName string) error {
	if sourceOpts == nil || request == nil {
		return nil
	}
	if sourceOpts.iscsiBlocksize != nil && request.iscsiBlocksize != nil && *sourceOpts.iscsiBlocksize != *request.iscsiBlocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create %s from %s: the source volume was provisioned with iSCSI extent blocksize %d and a ZFS clone "+
				"shares its data layout byte-for-byte, but the StorageClass resolves %d. Exposing %d-byte logical blocks over "+
				"data whose filesystem and partition table were written against %d-byte blocks corrupts it — restore into a "+
				"StorageClass whose %s matches the source",
			datasetName, sourceRef, *sourceOpts.iscsiBlocksize, *request.iscsiBlocksize,
			*request.iscsiBlocksize, *sourceOpts.iscsiBlocksize, paramISCSIBlocksize)
	}
	if sourceOpts.iscsiPblocksize != nil && request.iscsiPblocksize != nil && *sourceOpts.iscsiPblocksize != *request.iscsiPblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create %s from %s: the source volume was provisioned with iSCSI physical-blocksize reporting %t and a "+
				"ZFS clone shares its data layout byte-for-byte, but the StorageClass resolves %t. pblocksize changes the "+
				"alignment the initiator optimizes for against data already laid out for the source's value — restore into a "+
				"StorageClass whose %s matches the source",
			datasetName, sourceRef, *sourceOpts.iscsiPblocksize, *request.iscsiPblocksize, paramISCSIPblocksize)
	}
	return nil
}

// guardCloneSourceBlockGeometry resolves the clone/restore source's stored
// geometry and runs guardCloneSourceGeometry against the request. sourceDS may
// be supplied by a caller that already queried the source (the volume-clone path
// does, so the guard is free there); otherwise it is fetched, but ONLY when the
// StorageClass actually opts into a geometry — a default class, and every NFS
// request, short-circuits before any API call, which is what keeps the default
// provisioning path's round-trip count unchanged.
func (d *Driver) guardCloneSourceBlockGeometry(
	ctx context.Context,
	sourceDataset string,
	sourceDS *truenas.Dataset,
	sourceRef, datasetName string,
	shareType ShareType,
) error {
	if !shareType.IsBlockProtocol() {
		return nil
	}
	request := blockOptsFromContext(ctx)
	if !request.hasGeometryOpinion() {
		return nil
	}
	if sourceDS == nil {
		var err error
		sourceDS, err = d.truenasClient.DatasetGet(ctx, sourceDataset)
		if err != nil {
			// Fail closed: with an explicit geometry in the request and no way to
			// read what the source actually holds, proceeding is the corruption we
			// are guarding against.
			return status.Errorf(codes.Internal,
				"failed to read the stored block geometry of clone source %s: %v", sourceDataset, err)
		}
	}
	return guardCloneSourceGeometry(blockOptsFromDataset(sourceDS), request, sourceRef, datasetName)
}

// ---------------------------------------------------------------------------
// Existing-volume mutability policy (codex gate #1)
//
// EVERY per-volume block-protocol knob is IMMUTABLE for the life of the volume.
// A CreateVolume replay whose StorageClass resolves a value that is not already
// in effect fails closed with FailedPrecondition naming the parameter; nothing
// is ever accepted-and-ignored.
//
// Several of these fields ARE mutable at the TrueNAS 26.0 API level
// (iscsi.target.update accepts iscsi_parameters.QueuedCommands and
// auth_networks; iscsi.extent.update accepts avail_threshold, insecure_tpc, ro
// and serial; nvmet.subsys.update accepts qid_max and pi_enable). The driver
// deliberately does NOT reconcile them onto a live object, for three reasons:
//
//  1. The volume's stored stamp — not the backend object — is what every
//     publish / startup-reconcile / DR rebuild replays (F-2/F-3). Pushing a new
//     value to the backend without also re-stamping produces stamp-vs-backend
//     drift that the next rebuild silently reverts; re-stamping on the
//     existing-volume arm would add a new fatal write and a new crash window to
//     a path whose whole job is to be idempotent.
//  2. Kubernetes treats StorageClass `parameters` as immutable, so a changed
//     value never arrives as an in-place operator edit — it can only come from a
//     deleted-and-recreated class or a different class colliding on the same
//     volume name. Neither is an intent-to-mutate signal for a volume that
//     already holds data.
//  3. ro / insecure_tpc / auth_networks / pi_enable are enforced by the target
//     while an initiator is connected. Silently retargeting a live, mounted
//     volume's safety posture mid-flight is strictly worse than refusing and
//     saying why.
//
// blocksize / pblocksize are additionally immutable at the DATA level and keep
// their own dedicated guards and messages.
// ---------------------------------------------------------------------------

// blockOptConflict renders the FailedPrecondition for an immutable knob.
func blockOptConflict(datasetName, param, current, requested string) error {
	return status.Errorf(codes.FailedPrecondition,
		"volume %s already exists with %s=%s; the StorageClass resolves %s. Per-volume block-protocol tuning is fixed at "+
			"volume create — the driver never reconciles it onto a live share object, because the volume's stored stamp is "+
			"what every publish and DR rebuild replays, so a backend-only change would be silently reverted. Provision a new "+
			"volume, or restore %s on the StorageClass",
		datasetName, param, current, requested, param)
}

// blockOptIntConflict decides whether an explicitly requested integer is already
// in effect. live is what the backend reports for the existing object (nil when
// the backend does not report the field at all); stored is the volume's stamp.
// The backend is authoritative when it reports the field; the stamp is the
// fallback ONLY when it does not, so a same-value replay of a tuned volume can
// never be rejected just because TrueNAS omits a field from its query response.
func blockOptIntConflict(requested, live, stored *int) (conflict bool, current string) {
	if requested == nil {
		return false, ""
	}
	if live != nil {
		if *live == *requested {
			return false, ""
		}
		return true, strconv.Itoa(*live)
	}
	if stored != nil {
		if *stored == *requested {
			return false, ""
		}
		return true, strconv.Itoa(*stored)
	}
	return true, "unset"
}

// blockOptBoolConflict is blockOptIntConflict for booleans.
func blockOptBoolConflict(requested, live, stored *bool) (conflict bool, current string) {
	if requested == nil {
		return false, ""
	}
	if live != nil {
		if *live == *requested {
			return false, ""
		}
		return true, strconv.FormatBool(*live)
	}
	if stored != nil {
		if *stored == *requested {
			return false, ""
		}
		return true, strconv.FormatBool(*stored)
	}
	return true, "unset"
}

// blockOptStringConflict is blockOptIntConflict for strings; "" means the
// backend did not report the field.
func blockOptStringConflict(requested, live, stored string) (conflict bool, current string) {
	if requested == "" {
		return false, ""
	}
	if live != "" {
		if live == requested {
			return false, ""
		}
		return true, live
	}
	if stored != "" {
		if stored == requested {
			return false, ""
		}
		return true, stored
	}
	return true, "unset"
}

// blockOptNetworksConflict is blockOptIntConflict for the CIDR list. Comparison
// is order-insensitive so a backend that reorders the ACL is not a conflict.
func blockOptNetworksConflict(requested, live, stored []string) (conflict bool, current string) {
	if len(requested) == 0 {
		return false, ""
	}
	if len(live) > 0 {
		if sameNetworkSet(live, requested) {
			return false, ""
		}
		return true, strings.Join(live, ",")
	}
	if len(stored) > 0 {
		if sameNetworkSet(stored, requested) {
			return false, ""
		}
		return true, strings.Join(stored, ",")
	}
	return true, "unset"
}

func sameNetworkSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	left := append([]string(nil), a...)
	right := append([]string(nil), b...)
	sort.Strings(left)
	sort.Strings(right)
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// guardStoredBlockTuning is guardStoredBlockGeometry for the eight NON-geometry
// knobs, on the path where the backend object is ABSENT (a DR/restore rebuild).
// There is nothing live to compare against, so the stamp is the only record of
// what the volume was provisioned with.
func guardStoredBlockTuning(stored, request *blockOpts, datasetName string) error {
	if stored == nil || request == nil {
		return nil
	}
	if conflict, current := blockOptIntConflict(request.iscsiQueuedCommands, nil, stored.iscsiQueuedCommands); conflict {
		return blockOptConflict(datasetName, paramISCSIQueuedCommands, current, strconv.Itoa(*request.iscsiQueuedCommands))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiInsecureTpc, nil, stored.iscsiInsecureTpc); conflict {
		return blockOptConflict(datasetName, paramISCSIInsecureTpc, current, strconv.FormatBool(*request.iscsiInsecureTpc))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiReadOnly, nil, stored.iscsiReadOnly); conflict {
		return blockOptConflict(datasetName, paramISCSIReadOnly, current, strconv.FormatBool(*request.iscsiReadOnly))
	}
	if conflict, current := blockOptIntConflict(request.iscsiAvailThreshold, nil, stored.iscsiAvailThreshold); conflict {
		return blockOptConflict(datasetName, paramISCSIAvailThreshold, current, strconv.Itoa(*request.iscsiAvailThreshold))
	}
	if conflict, current := blockOptStringConflict(request.iscsiSerial, "", stored.iscsiSerial); conflict {
		return blockOptConflict(datasetName, paramISCSIStableSerial, current, request.iscsiSerial)
	}
	if conflict, current := blockOptNetworksConflict(request.iscsiAuthNetworks, nil, stored.iscsiAuthNetworks); conflict {
		return blockOptConflict(datasetName, paramISCSIAuthNetworks, current, strings.Join(request.iscsiAuthNetworks, ","))
	}
	if conflict, current := blockOptIntConflict(request.nvmeofQidMax, nil, stored.nvmeofQidMax); conflict {
		return blockOptConflict(datasetName, paramNVMeoFQidMax, current, strconv.Itoa(*request.nvmeofQidMax))
	}
	if conflict, current := blockOptBoolConflict(request.nvmeofPiEnable, nil, stored.nvmeofPiEnable); conflict {
		return blockOptConflict(datasetName, paramNVMeoFPiEnable, current, strconv.FormatBool(*request.nvmeofPiEnable))
	}
	return nil
}

// guardExistingISCSIExtentOpts fails a replay closed when the StorageClass asks
// for an extent value the EXISTING extent does not already carry. Blocksize has
// its own dedicated data-corruption message (guardISCSIBlocksizeImmutability)
// and is not repeated here.
func guardExistingISCSIExtentOpts(extent *truenas.ISCSIExtent, request, stored *blockOpts, datasetName string) error {
	if extent == nil || request == nil {
		return nil
	}
	if stored == nil {
		stored = &blockOpts{}
	}
	// pblocksize / insecure_tpc / ro are plain booleans that TrueNAS always
	// reports on iscsi.extent.query, so the live object is authoritative.
	livePblocksize, liveInsecureTpc, liveReadOnly := extent.Pblocksize, extent.InsecureTpc, extent.Ro
	if conflict, current := blockOptBoolConflict(request.iscsiPblocksize, &livePblocksize, stored.iscsiPblocksize); conflict {
		return blockOptConflict(datasetName, paramISCSIPblocksize, current, strconv.FormatBool(*request.iscsiPblocksize))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiInsecureTpc, &liveInsecureTpc, stored.iscsiInsecureTpc); conflict {
		return blockOptConflict(datasetName, paramISCSIInsecureTpc, current, strconv.FormatBool(*request.iscsiInsecureTpc))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiReadOnly, &liveReadOnly, stored.iscsiReadOnly); conflict {
		return blockOptConflict(datasetName, paramISCSIReadOnly, current, strconv.FormatBool(*request.iscsiReadOnly))
	}
	if conflict, current := blockOptIntConflict(request.iscsiAvailThreshold, extent.AvailThreshold, stored.iscsiAvailThreshold); conflict {
		return blockOptConflict(datasetName, paramISCSIAvailThreshold, current, strconv.Itoa(*request.iscsiAvailThreshold))
	}
	if conflict, current := blockOptStringConflict(request.iscsiSerial, extent.Serial, stored.iscsiSerial); conflict {
		return blockOptConflict(datasetName, paramISCSIStableSerial, current, request.iscsiSerial)
	}
	return nil
}

// guardExistingISCSITargetOpts is guardExistingISCSIExtentOpts for the target
// half (queue depth and the network ACL).
func guardExistingISCSITargetOpts(target *truenas.ISCSITarget, request, stored *blockOpts, datasetName string) error {
	if target == nil || request == nil {
		return nil
	}
	if stored == nil {
		stored = &blockOpts{}
	}
	if conflict, current := blockOptIntConflict(request.iscsiQueuedCommands, target.QueuedCommands, stored.iscsiQueuedCommands); conflict {
		return blockOptConflict(datasetName, paramISCSIQueuedCommands, current, strconv.Itoa(*request.iscsiQueuedCommands))
	}
	if conflict, current := blockOptNetworksConflict(request.iscsiAuthNetworks, target.AuthNetworks, stored.iscsiAuthNetworks); conflict {
		return blockOptConflict(datasetName, paramISCSIAuthNetworks, current, strings.Join(request.iscsiAuthNetworks, ","))
	}
	return nil
}

// guardExistingNVMeoFSubsystemOpts is guardExistingISCSIExtentOpts for the NVMe
// subsystem. Both fields are nullable on nvmet.subsys.query, so both fall back
// to the stamp when the backend omits them.
func guardExistingNVMeoFSubsystemOpts(subsys *truenas.NVMeoFSubsystem, request, stored *blockOpts, datasetName string) error {
	if subsys == nil || request == nil {
		return nil
	}
	if stored == nil {
		stored = &blockOpts{}
	}
	if conflict, current := blockOptIntConflict(request.nvmeofQidMax, subsys.QidMax, stored.nvmeofQidMax); conflict {
		return blockOptConflict(datasetName, paramNVMeoFQidMax, current, strconv.Itoa(*request.nvmeofQidMax))
	}
	if conflict, current := blockOptBoolConflict(request.nvmeofPiEnable, subsys.PiEnable, stored.nvmeofPiEnable); conflict {
		return blockOptConflict(datasetName, paramNVMeoFPiEnable, current, strconv.FormatBool(*request.nvmeofPiEnable))
	}
	return nil
}

// validateNoNVMeoFPortParams rejects NVMe-oF port performance fields supplied as
// StorageClass parameters. Those fields live on a port that is SHARED across
// volumes, so a per-SC value would mutate a shared object under other volumes
// (R-4). They are install-wide only.
func validateNoNVMeoFPortParams(params map[string]string) error {
	for _, key := range []string{"nvmeof/inlineDataSize", "nvmeof/maxQueueSize", "nvmeof/portPiEnable"} {
		if _, ok := params[key]; ok {
			return status.Errorf(codes.InvalidArgument,
				"%s is not a per-StorageClass parameter: NVMe-oF port performance fields are install-wide (nvmeof.port.*) because the port is shared across volumes", key)
		}
	}
	return nil
}
