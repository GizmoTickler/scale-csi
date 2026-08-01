package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"path"
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

	// iscsiStableSerial and iscsiAuthNetworksSet record that the StorageClass
	// EXPLICITLY set those two keys, including to their "off" value
	// (stableSerial: "false", authNetworks: ""). Both knobs degrade to an empty
	// value — "" and an empty CIDR list — which is indistinguishable from "the
	// class said nothing" in iscsiSerial / iscsiAuthNetworks alone. Without these
	// two flags, turning either knob OFF on an existing volume would be the one
	// direction that is silently accepted-and-ignored, contradicting the single
	// rule that all ten knobs are immutable. They are set ONLY by
	// resolveBlockOpts (a real CreateVolume request); the stored stamp never
	// carries them, because a stamp records what a volume HAS, not what a class
	// asked for.
	iscsiStableSerial    *bool
	iscsiAuthNetworksSet bool
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
		// Record the explicit request even when it is false, so "the class turned
		// stableSerial off" stays distinguishable from "the class said nothing".
		opts.iscsiStableSerial = &value
		set = true
		if value {
			opts.iscsiSerial = stableISCSISerial(volumeName)
		}
	}

	if raw, ok := params[paramISCSIAuthNetworks]; ok {
		networks, err := parseAuthNetworks(raw)
		if err != nil {
			return nil, err
		}
		opts.iscsiAuthNetworks = networks
		opts.iscsiAuthNetworksSet = true
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

// NOTE: there is deliberately no "does this caller have a geometry opinion"
// predicate any more. hasGeometryOpinion() used to exist and to mean "the
// StorageClass set blocksize or pblocksize", and gating the clone-source
// resolution on it is precisely how the controller-wide default became invisible
// to every geometry guard (N-1e).
//
// The reasoning it encoded is false: a request that names neither key still
// creates an extent, and that extent still gets a logical block size — the
// controller-wide default (helm `iscsi.extentBlocksize`), which is every bit as
// much an opinion about how the data is addressed, just one nobody recorded.
// Round 4 therefore handles the default where it would actually be APPLIED
// (resolveExtentCreateBlocksize for an absent extent on an existing volume,
// resolveCloneSourceBlockGeometry for cloned data) instead of letting any guard
// short-circuit on "the caller said nothing". Re-introducing such a predicate as
// a guard gate would re-open the class. See "the geometry invariant" below.

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
// into nothing has nothing to contradict and is left alone HERE — but it does
// not therefore inherit the controller default: its destination is stamped with
// the source geometry resolved by resolveCloneSourceBlockGeometry, which runs
// for every block clone whether or not the class said anything.
//
// A source with no STAMP is not "no geometry" either: its geometry is resolved
// from its live iSCSI extent before this runs, because every volume provisioned
// before these knobs existed is unstamped and is exactly the installed base a
// newly-knobbed StorageClass gets pointed at.
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

// ---------------------------------------------------------------------------
// THE GEOMETRY INVARIANT (GF-4 round 4)
//
//	The driver never creates an iSCSI extent at a geometry it has not resolved
//	from a durable or observable record of THAT VOLUME'S ACTUAL LAYOUT. The
//	controller-wide default is itself a geometry opinion and may only be applied
//	to storage that provably holds no data yet.
//
// Five forms of the same corruption were found across four rounds — a rebuild
// with no opts; a publish/startup-reconcile; a restore from a STAMPED source; a
// restore from an UNSTAMPED source; and finally the controller-wide default
// (helm `iscsi.extentBlocksize`) reaching both a no-opts restore and a plain
// rebuild. Each round closed the trigger in front of it. They are all the same
// bug: an unstamped volume's true geometry was recorded NOWHERE the driver
// consults before creating an extent, so something had to supply it, and that
// something was a mutable install-wide setting no guard could see.
//
// Four mechanisms enforce the invariant. None adds a round trip to a path that
// did not already need one:
//
//  1. RECORD WHAT WE CREATED, AND BACK-STAMP WHAT WE SEE. Every time the iSCSI
//     share builder creates an extent, or resolves a live one for a volume whose
//     dataset carries no geometry stamp, the extent's ACTUAL geometry is folded
//     into the resource-ID dataset update that path already performs
//     (observedGeometryProps). Volumes this driver creates are stamped at
//     create; the entire pre-GF4 fleet back-stamps itself on its first publish /
//     reconcile / replay. +0 API calls, and it is what makes (2) safe.
//
//  2. NEVER DEFAULT OVER EXISTING DATA. When an ABSENT extent must be
//     re-created, the blocksize comes from the stamp; there is no live extent to
//     read, so if the dataset is an existing CSI block volume with no stamp the
//     request is REFUSED (resolveExtentCreateBlocksize) rather than guessed. The
//     controller default survives only where it cannot lie: a zvol this very
//     call created, or a dataset carrying no evidence of ever having held a CSI
//     block volume. This closes the plain-rebuild form (N-1e(b)).
//
//  3. A CLONE INHERITS ITS SOURCE'S REAL GEOMETRY, ALWAYS. Cloning or restoring
//     a BLOCK volume resolves the SOURCE's geometry unconditionally — stamp and
//     live extent, before the first destination mutation — rejects a request
//     that conflicts with it, and stamps the answer onto the destination
//     (resolveCloneSourceBlockGeometry). A no-opts restore can therefore never
//     fall through to whatever the controller default happens to be today
//     (N-1e(a)), and a no-opts hop can no longer LAUNDER a wrong geometry into
//     the next restore's ground truth, because the destination now records the
//     source's real layout instead of the default it was created with.
//
//  4. PRECEDENCE, AND REFUSE ON DISAGREEMENT. The LIVE extent is authoritative
//     for what the data actually IS; the STAMP records the intent the volume was
//     provisioned with. Where only one exists, it answers. Where both exist and
//     DISAGREE, the driver refuses and names both values
//     (guardStampedVsLiveGeometry on a volume, reconcileSourceGeometry on a
//     clone source) instead of silently picking a side — a drifted volume is a
//     fact an operator has to see, not one to resolve by coin flip.
//
// Cost, stated honestly: a BLOCK clone/restore now pays one source DatasetGet
// (skipped when the caller already has the source dataset, i.e. the volume-clone
// path) plus one source ISCSIExtentFindByDisk, ALWAYS rather than only when a
// StorageClass opted into a geometry. Fresh provisioning, publish, unpublish,
// reconcile and every NFS path pay nothing extra; their golden counts are
// unchanged.
// ---------------------------------------------------------------------------

// geometryProps renders ONLY the two geometry keys of a resolved blockOpts.
// Unlike storedProperties (which records what a StorageClass asked for) this
// records what a volume's data IS, which is why it is written for volumes whose
// class opted into nothing at all.
func geometryProps(o *blockOpts) map[string]string {
	if o == nil {
		return nil
	}
	props := make(map[string]string, 2)
	if o.iscsiBlocksize != nil {
		props[PropBlockISCSIBlocksize] = strconv.Itoa(*o.iscsiBlocksize)
	}
	if o.iscsiPblocksize != nil {
		props[PropBlockISCSIPblocksize] = strconv.FormatBool(*o.iscsiPblocksize)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

// observedGeometryProps is mechanism (1): back-stamp the geometry of an extent
// the driver just created or just resolved onto a dataset that does not already
// record it.
//
// It returns only the keys the dataset is MISSING, so it is idempotent and a
// fully stamped volume yields nil — the caller folds the result into a dataset
// update it was going to issue anyway, so the cost is zero round trips whether
// or not there is anything to stamp. A backend that does not report a field
// (blocksize 0, pblocksize null) contributes nothing: an unknown value is never
// invented.
func observedGeometryProps(ds *truenas.Dataset, extent *truenas.ISCSIExtent) map[string]string {
	if extent == nil {
		return nil
	}
	props := make(map[string]string, 2)
	if extent.Blocksize != 0 && !datasetUserPropertyHasValue(ds, PropBlockISCSIBlocksize) {
		props[PropBlockISCSIBlocksize] = strconv.Itoa(extent.Blocksize)
	}
	if extent.Pblocksize != nil && !datasetUserPropertyHasValue(ds, PropBlockISCSIPblocksize) {
		props[PropBlockISCSIPblocksize] = strconv.FormatBool(*extent.Pblocksize)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

// guardStampedVsLiveGeometry is mechanism (4) on the volume itself: the stamp
// records intent, the live extent records what the data is, and a disagreement
// between them is a fact the driver must surface rather than resolve silently.
//
// Before this, the stamp beat the live extent unconditionally (the stamp was
// consulted first and the live read only happened when the stamp was silent), so
// a volume whose extent had been re-created out of band — or laundered through a
// no-opts hop by the very bug this round fixes — would keep certifying its own
// wrong geometry to every downstream guard.
func guardStampedVsLiveGeometry(stored *blockOpts, extent *truenas.ISCSIExtent, datasetName string) error {
	if stored == nil || extent == nil {
		return nil
	}
	if stored.iscsiBlocksize != nil && extent.Blocksize != 0 && *stored.iscsiBlocksize != extent.Blocksize {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s records iSCSI extent blocksize %d but its live extent reports %d. The live extent is what the data is "+
				"actually addressed through and the stamp is what the volume was provisioned with, so this is real drift "+
				"(an out-of-band extent edit, or an extent re-created at a different geometry). The driver refuses to pick a "+
				"side: reconcile the extent's blocksize with %s=%d, or correct the stamp to match the extent",
			datasetName, *stored.iscsiBlocksize, extent.Blocksize, PropBlockISCSIBlocksize, *stored.iscsiBlocksize)
	}
	if stored.iscsiPblocksize != nil && extent.Pblocksize != nil && *stored.iscsiPblocksize != *extent.Pblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s records iSCSI physical-blocksize reporting %t but its live extent reports %t. The driver refuses to "+
				"pick a side: reconcile the extent with %s=%t, or correct the stamp to match the extent",
			datasetName, *stored.iscsiPblocksize, *extent.Pblocksize, PropBlockISCSIPblocksize, *stored.iscsiPblocksize)
	}
	return nil
}

// datasetRecordsAPriorISCSIExtent reports whether the driver's own bookkeeping
// says this dataset HAS had an iSCSI extent — and therefore that its bytes may
// already be addressed through a logical block size the driver must not guess.
//
// PropISCSIExtentID is the right and only witness: it is written by the share
// builder in the same dataset update as the geometry stamp (see
// observedGeometryProps), so "the driver recorded an extent for this dataset"
// and "the driver recorded that extent's geometry" are durable together. Its
// presence WITHOUT a geometry stamp is precisely the pre-GF4 legacy volume whose
// extent has since gone missing — a DR restore, an orphan reconcile, a share
// teardown — which is the shape that silently re-created the extent at whatever
// `iscsi.extentBlocksize` happened to be that day. A ZFS clone inherits its
// source's user properties, so a clone of a real volume carries the witness too;
// that is correct, and its geometry is supplied by mechanism (3) long before
// this is consulted.
//
// A dataset without it has no extent history the driver could contradict: a zvol
// nothing has ever exported cannot have a filesystem laid out against a logical
// block size, so the controller default is the honest answer there, not a guess.
func datasetRecordsAPriorISCSIExtent(ds *truenas.Dataset) bool {
	return datasetUserPropertyHasValue(ds, PropISCSIExtentID)
}

// resolveExtentCreateBlocksize is mechanism (2): the ONE place the blocksize an
// extent is created with is decided.
//
// Resolution order is stamp (per-SC override or the volume's own record) ->
// controller default, and the controller default is available ONLY when it
// cannot lie:
//
//   - freshlyCreated: DatasetCreate produced this zvol during this very call, so
//     there is no data on it and no layout to contradict; or
//   - the driver's bookkeeping shows the dataset never had an iSCSI extent, so
//     nothing has ever addressed its bytes through a logical block size.
//
// Otherwise the volume exists, its extent is absent (so there is no live
// geometry to read), and nothing records what its data was written against —
// the state in which every previous round silently wrote the current helm
// default over old data. It fails closed instead, naming the property an
// operator can set to recover.
func (d *Driver) resolveExtentCreateBlocksize(opts *blockOpts, ds *truenas.Dataset, datasetName string, freshlyCreated bool) (int, error) {
	if opts != nil && opts.iscsiBlocksize != nil {
		return *opts.iscsiBlocksize, nil
	}
	if freshlyCreated || !datasetRecordsAPriorISCSIExtent(ds) {
		return d.config.ISCSI.ExtentBlocksize, nil
	}
	return 0, status.Errorf(codes.FailedPrecondition,
		"cannot re-create the iSCSI extent for %s: the volume already exists, its extent is absent (so there is no live "+
			"geometry to read), and it carries no %s record of the logical block size its data was written against. "+
			"Falling back to the controller-wide default (iscsi.extentBlocksize=%d) would lay a guessed geometry over data "+
			"that may have been written against a different one, which corrupts it — the driver refuses instead. Recover by "+
			"restoring the volume's original extent, or by recording its real blocksize on the dataset "+
			"(zfs set %s=<512|1024|2048|4096> %s) and retrying",
		datasetName, PropBlockISCSIBlocksize, d.config.ISCSI.ExtentBlocksize, PropBlockISCSIBlocksize, datasetName)
}

// resolveCloneSourceBlockGeometry is mechanism (3). It answers "what geometry is
// the data we are about to clone actually addressed through", rejects a request
// that contradicts that answer, and returns the properties that record it on the
// DESTINATION so no later path has to ask again.
//
// It runs for EVERY block clone/restore, not only for a class that opts into a
// geometry. That is the round-4 correction: a class with no geometry parameter
// still produces an extent, and before this the extent it produced was created
// at the current controller default — over data cloned byte-for-byte from a
// source that may have been written against something else entirely. Gating this
// on hasExplicitGeometry made the default invisible to the guard; the guard has
// to see it precisely because nobody else does.
//
// sourceDS may be supplied by a caller that already queried the source (the
// volume-clone path does), which saves the DatasetGet there. NFS short-circuits
// before any API call, so no filesystem path pays for this.
//
// A source with neither a stamp nor a live extent yields no properties and no
// error: nothing has ever exported that data, so there is no layout to preserve
// and nothing to contradict.
func (d *Driver) resolveCloneSourceBlockGeometry(
	ctx context.Context,
	sourceDataset string,
	sourceDS *truenas.Dataset,
	sourceRef, datasetName string,
	shareType ShareType,
) (map[string]string, error) {
	if !shareType.IsBlockProtocol() {
		return nil, nil
	}
	if sourceDS == nil {
		var err error
		sourceDS, err = d.truenasClient.DatasetGet(ctx, sourceDataset)
		if err != nil {
			// Fail closed: about to lay a geometry over this source's data with no
			// way to read what that data is, which is the corruption itself.
			return nil, status.Errorf(codes.Internal,
				"failed to read the stored block geometry of clone source %s: %v", sourceDataset, err)
		}
	}
	extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, "zvol/"+sourceDataset)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to read the live block geometry of clone source %s: %v", sourceDataset, err)
	}
	sourceGeometry, err := reconcileSourceGeometry(blockOptsFromDataset(sourceDS), extent, sourceRef)
	if err != nil {
		return nil, err
	}
	if guardErr := guardCloneSourceGeometry(sourceGeometry, blockOptsFromContext(ctx), sourceRef, datasetName); guardErr != nil {
		return nil, guardErr
	}
	return geometryProps(sourceGeometry), nil
}

// reconcileSourceGeometry applies the precedence rule of mechanism (4) to a
// clone source: the live extent is authoritative for what the data is, the stamp
// answers where there is no extent, and a disagreement is refused rather than
// silently resolved. A drifted source is exactly the state in which "which of
// these two numbers is the truth" cannot be answered from inside the driver.
func reconcileSourceGeometry(stamped *blockOpts, extent *truenas.ISCSIExtent, sourceRef string) (*blockOpts, error) {
	if extent == nil {
		return stamped, nil
	}
	resolved := &blockOpts{}
	if stamped != nil {
		*resolved = *stamped
	}
	if extent.Blocksize != 0 {
		if resolved.iscsiBlocksize != nil && *resolved.iscsiBlocksize != extent.Blocksize {
			return nil, status.Errorf(codes.FailedPrecondition,
				"clone source %s records iSCSI extent blocksize %d but its live extent reports %d. The driver will not clone "+
					"data whose real geometry it cannot establish: reconcile the source's extent and its %s stamp, then retry",
				sourceRef, *resolved.iscsiBlocksize, extent.Blocksize, PropBlockISCSIBlocksize)
		}
		blocksize := extent.Blocksize
		resolved.iscsiBlocksize = &blocksize
	}
	if extent.Pblocksize != nil {
		if resolved.iscsiPblocksize != nil && *resolved.iscsiPblocksize != *extent.Pblocksize {
			return nil, status.Errorf(codes.FailedPrecondition,
				"clone source %s records iSCSI physical-blocksize reporting %t but its live extent reports %t. The driver will "+
					"not clone data whose real geometry it cannot establish: reconcile the source's extent and its %s stamp, "+
					"then retry",
				sourceRef, *resolved.iscsiPblocksize, *extent.Pblocksize, PropBlockISCSIPblocksize)
		}
		pblocksize := *extent.Pblocksize
		resolved.iscsiPblocksize = &pblocksize
	}
	return resolved, nil
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

// blockOptStableSerialOffConflict decides the OFF direction of
// iscsi/stableSerial — the direction the value-based conflict helpers
// structurally cannot see, because "no stable serial" and "the class said
// nothing about stableSerial" are the same empty string in blockOpts.iscsiSerial.
//
// The live serial alone cannot decide it: TrueNAS auto-generates a serial for
// every extent, so a non-empty live serial is not evidence that the volume was
// pinned. The two things that ARE evidence are the volume's stamp (only written
// when stableSerial was on) and a live serial that equals the deterministic
// serial this volume's name derives — which a random auto-generated one never
// does. Together they make a same-value replay of a stableSerial: "false"
// volume succeed while a genuine on -> off change fails closed.
func blockOptStableSerialOffConflict(request *blockOpts, live, stored, volumeName string) (conflict bool, current string) {
	if request == nil || request.iscsiStableSerial == nil || *request.iscsiStableSerial {
		return false, ""
	}
	if stored != "" {
		return true, "true"
	}
	if live != "" && volumeName != "" && live == stableISCSISerial(volumeName) {
		return true, "true"
	}
	return false, ""
}

// blockOptNetworksOffConflict is blockOptStableSerialOffConflict for
// iscsi/authNetworks: an explicitly EMPTY list ("remove the target ACL") on a
// volume whose target carries one is a change, not a no-opinion, and dropping a
// network ACL silently is exactly the accepted-and-ignored shape the immutability
// policy exists to prevent.
func blockOptNetworksOffConflict(request *blockOpts, live, stored []string) (conflict bool, current string) {
	if request == nil || !request.iscsiAuthNetworksSet || len(request.iscsiAuthNetworks) > 0 {
		return false, ""
	}
	if len(live) > 0 {
		return true, strings.Join(live, ",")
	}
	if len(stored) > 0 {
		return true, strings.Join(stored, ",")
	}
	return false, ""
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
	// Off-direction, absent-object flavor: the stamp is the only record here, so
	// the live-serial half of the comparison is deliberately empty.
	if conflict, current := blockOptStableSerialOffConflict(request, "", stored.iscsiSerial, ""); conflict {
		return blockOptConflict(datasetName, paramISCSIStableSerial, current, "false")
	}
	if conflict, current := blockOptNetworksOffConflict(request, nil, stored.iscsiAuthNetworks); conflict {
		return blockOptConflict(datasetName, paramISCSIAuthNetworks, current, "none")
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
	// pblocksize / insecure_tpc / ro are nullable in the response model, so the
	// SAME rule applies to all ten knobs without exception: the live object is
	// authoritative when it reports the field, and the stamp is the fallback when
	// it does not. (A response that omits one used to parse as false and reject a
	// same-value replay as a conflict.)
	if conflict, current := blockOptBoolConflict(request.iscsiPblocksize, extent.Pblocksize, stored.iscsiPblocksize); conflict {
		return blockOptConflict(datasetName, paramISCSIPblocksize, current, strconv.FormatBool(*request.iscsiPblocksize))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiInsecureTpc, extent.InsecureTpc, stored.iscsiInsecureTpc); conflict {
		return blockOptConflict(datasetName, paramISCSIInsecureTpc, current, strconv.FormatBool(*request.iscsiInsecureTpc))
	}
	if conflict, current := blockOptBoolConflict(request.iscsiReadOnly, extent.Ro, stored.iscsiReadOnly); conflict {
		return blockOptConflict(datasetName, paramISCSIReadOnly, current, strconv.FormatBool(*request.iscsiReadOnly))
	}
	if conflict, current := blockOptIntConflict(request.iscsiAvailThreshold, extent.AvailThreshold, stored.iscsiAvailThreshold); conflict {
		return blockOptConflict(datasetName, paramISCSIAvailThreshold, current, strconv.Itoa(*request.iscsiAvailThreshold))
	}
	if conflict, current := blockOptStringConflict(request.iscsiSerial, extent.Serial, stored.iscsiSerial); conflict {
		return blockOptConflict(datasetName, paramISCSIStableSerial, current, request.iscsiSerial)
	}
	// The OFF direction of iscsi/stableSerial: turning a knob off is a change like
	// any other and must not be accepted-and-ignored either (see
	// blockOptStableSerialOffConflict for why the live serial alone cannot decide
	// it). path.Base(datasetName) is the volume ID the serial was derived from —
	// datasetForID builds every dataset name as <parent>/<volumeID>.
	if conflict, current := blockOptStableSerialOffConflict(request, extent.Serial, stored.iscsiSerial, path.Base(datasetName)); conflict {
		return blockOptConflict(datasetName, paramISCSIStableSerial, current, "false")
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
	if conflict, current := blockOptNetworksOffConflict(request, target.AuthNetworks, stored.iscsiAuthNetworks); conflict {
		return blockOptConflict(datasetName, paramISCSIAuthNetworks, current, "none")
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
