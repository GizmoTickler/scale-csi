package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

// effectiveBlockOpts is the request-plus-stamp merge for the EIGHT NON-GEOMETRY
// knobs:
//
//	request-scoped StorageClass opts (CreateVolume only)
//	  -> the volume's STORED dataset properties (rebuild / publish / reconcile)
//	    -> the controller-wide default (the resolved* helpers below)
//
// A nil result means "nothing was ever opted into", which is the pre-GF4 path.
//
// It is deliberately NOT the geometry resolver, and the round-4 comment claiming
// it was "THE resolver" was false: production never called it, and blocksize /
// pblocksize were decided by four other pieces of code. Geometry has one choke
// point of its own — see resolveExtentGeometry — because it is the only tuning
// whose wrong value corrupts data rather than mis-tuning a share.
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
func guardCloneSourceGeometry(sourceOpts blockGeometry, request *blockOpts, sourceRef, datasetName string) error {
	if request == nil {
		return nil
	}
	if sourceOpts.blocksize != nil && request.iscsiBlocksize != nil && *sourceOpts.blocksize != *request.iscsiBlocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create %s from %s: the source volume was provisioned with iSCSI extent blocksize %d and a ZFS clone "+
				"shares its data layout byte-for-byte, but the StorageClass resolves %d. Exposing %d-byte logical blocks over "+
				"data whose filesystem and partition table were written against %d-byte blocks corrupts it — restore into a "+
				"StorageClass whose %s matches the source",
			datasetName, sourceRef, *sourceOpts.blocksize, *request.iscsiBlocksize,
			*request.iscsiBlocksize, *sourceOpts.blocksize, paramISCSIBlocksize)
	}
	if sourceOpts.pblocksize != nil && request.iscsiPblocksize != nil && *sourceOpts.pblocksize != *request.iscsiPblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create %s from %s: the source volume was provisioned with iSCSI physical-blocksize reporting %t and a "+
				"ZFS clone shares its data layout byte-for-byte, but the StorageClass resolves %t. pblocksize changes the "+
				"alignment the initiator optimizes for against data already laid out for the source's value — restore into a "+
				"StorageClass whose %s matches the source",
			datasetName, sourceRef, *sourceOpts.pblocksize, *request.iscsiPblocksize, paramISCSIPblocksize)
	}
	return nil
}

// ---------------------------------------------------------------------------
// THE GEOMETRY INVARIANT (GF-4, mechanism built in round 5)
//
//	The driver never creates an iSCSI extent at a geometry it has not resolved
//	from a record of THAT DATA'S ACTUAL LAYOUT. The controller-wide default is
//	itself a geometry opinion, and so is a StorageClass parameter; both may only
//	be applied to storage that provably holds no block-addressed data.
//
// Six forms of the same corruption were found across five rounds. They are all
// the same bug: the true geometry of some bytes was recorded NOWHERE the driver
// consults before creating an extent, so something had to supply it, and that
// something was either a mutable install-wide setting or a StorageClass
// parameter — neither of which knows anything about bytes that already exist.
//
// The mechanism, and where to read it:
//
//  1. ONE CHOKE POINT. There is exactly one production extent-create call
//     (iscsi_share.go). It takes an extentGeometry — a COMPLETE record, logical
//     and physical resolved together — produced by exactly one function,
//     (*Driver).resolveExtentGeometry, and every retry and create-error recovery
//     arm re-validates the object it ends up with against that same record
//     (validateExtentAgainstGeometry).
//
//  2. UNKNOWN IS A STATE. geometryKnowledge distinguishes "provably never
//     block-addressed" from "may hold data, layout unestablishable" from "no
//     opinion". The second fails closed.
//
//  3. A REQUEST IS INTENT. An explicit iscsi/blocksize or iscsi/pblocksize never
//     supplies a value over storage that may hold data; it may only agree with
//     the evidence (guardRequestAgainstEvidence).
//
//  4. HISTORY IS THE WHOLE WITNESS SET. blockDataHistoryWitnesses, counting the
//     ZFS "-" sentinel as presence — because a detached copy writes exactly that
//     sentinel BECAUSE the dataset holds somebody else's bytes.
//
//  5. SNAPSHOT PROVENANCE IS THE SNAPSHOT'S. A restore resolves geometry from
//     the stamp the SNAPSHOT captured, never from the source's current state,
//     which describes the source now. CreateSnapshot stamps every snapshot it
//     takes; a snapshot that captured nothing and whose source has block-data
//     history fails closed.
//
//  6. RECORD WHAT WE CREATED. The share builder folds the extent's actual
//     geometry AND the extent-ID witness into the caller's FATAL property update
//     when there is one, so both are durable-or-rolled-back with the rest of
//     provisioning rather than left to a warning-only write; it also back-stamps
//     a live extent it merely resolved. +0 API calls either way.
//
//  7. PRECEDENCE, AND REFUSE ON DISAGREEMENT. Where a stamp and a live extent
//     both exist and disagree, the driver refuses and names both values
//     (guardStampedVsLiveGeometry, reconcileSourceGeometry) instead of picking a
//     side.
//
// What back-stamping DOES and DOES NOT claim (stated because the docs previously
// overclaimed): observedGeometryProps records what the extent reports NOW. That
// is proof of how the data is addressed today and it stops any future rebuild or
// helm-default change from reaching the volume. It is NOT proof of historical
// truth: a volume corrupted before this mechanism existed — an unstamped live
// 512 extent laid over 4096-layout bytes by an old defaulted rebuild — is
// observationally indistinguishable from a correct 512 volume, and back-stamping
// freezes the observable state rather than repairing the history. Only an
// operator who knows the original geometry can correct such a volume.
//
// Cost, stated honestly: a PVC-to-PVC BLOCK clone pays one source DatasetGet
// (skipped when the caller already has the source dataset) plus one source
// ISCSIExtentFindByDisk. A BLOCK snapshot restore pays NOTHING when the snapshot
// carries its own geometry stamp (round 4 paid two calls here) and one
// DatasetGet when it does not. CreateSnapshot pays one ISCSIExtentFindByDisk for
// a zvol whose dataset is not yet stamped, and nothing once it is. Fresh
// provisioning, publish, unpublish, reconcile and every NFS path pay nothing
// extra.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// THE GEOMETRY CHOKE POINT (GF-4 round 5)
//
// Round 4 asserted an invariant in a comment and left FIVE independent geometry
// semantics in production (blockOptsFromContext+blockOptsFromDataset+
// mergeBlockOpts in the share builder; blockOptsFromDataset+reconcileSourceGeometry
// in the clone resolver; resolveExtentCreateBlocksize for LOGICAL only;
// resolvedISCSIPblocksize for PHYSICAL independently at the create call; and
// observedGeometryProps as a post-hoc stamp). A comment asserting an invariant is
// not an invariant.
//
// Round 5 builds the mechanism instead. Everything below funnels into exactly two
// functions:
//
//   - resolveCloneSourceBlockGeometry answers "what are the bytes we are about to
//     clone addressed through", as a TRI-STATE (see geometryKnowledge).
//   - (*Driver).resolveExtentGeometry answers "what geometry may this extent be
//     created at", as ONE COMPLETE record (logical AND physical together, never
//     independently), and every creation / retry / recovery arm carries it.
//
// Three semantic rules the round-4 code broke:
//
//	(a) UNKNOWN IS A STATE. "no extent and no properties" used to be a successful
//	    nil that the resolver read as "no geometry". It now resolves to
//	    geometryUnknown, which is distinct from geometryNoHistory ("this storage
//	    provably never held block-addressed data") and from "no opinion".
//	(b) A REQUEST IS INTENT, NOT EVIDENCE. An explicit iscsi/blocksize never
//	    supplies a value for storage that may already hold data; it may only AGREE
//	    with the evidence, and a disagreement fails closed.
//	(c) HISTORY IS NOT ONE PROPERTY. "Has this storage ever been block-addressed"
//	    is answered by the whole witness set (blockDataHistoryWitnesses), counting
//	    the ZFS "-" sentinel as presence, because a detached copy resets
//	    PropISCSIExtentID to "-" precisely BECAUSE it carries somebody else's data.
// ---------------------------------------------------------------------------

// geometryKnowledge is what the driver knows about the layout of the bytes that
// are already on a piece of storage. It is deliberately four-valued: the round-4
// bug was that "unknown" and "nothing was ever written here" were the same nil.
type geometryKnowledge int

const (
	// geometryUnexamined: the question was not asked (NFS, or no content source).
	geometryUnexamined geometryKnowledge = iota
	// geometryNoHistory: the storage provably holds no block-addressed data, so
	// the controller-wide default cannot lie and an explicit request may win.
	geometryNoHistory
	// geometryUnknown: the storage MAY hold block-addressed data and the driver
	// cannot establish its layout. Fail closed; never guess.
	geometryUnknown
	// geometryKnown: blocksize AND pblocksize are both resolved from evidence
	// about the bytes themselves.
	geometryKnown
)

func (k geometryKnowledge) String() string {
	switch k {
	case geometryNoHistory:
		return "no-history"
	case geometryUnknown:
		return "unknown"
	case geometryKnown:
		return "known"
	default:
		return "unexamined"
	}
}

// blockGeometry is ONE record of what a piece of storage's bytes are addressed
// through. blocksize and pblocksize travel together and are only ever produced
// together; provenance names the evidence, and appears verbatim in the
// fail-closed messages so an operator can see WHY the driver believes what it
// believes.
type blockGeometry struct {
	knowledge  geometryKnowledge
	blocksize  *int
	pblocksize *bool
	provenance string
}

// complete reports whether BOTH geometry fields are resolved. A half-resolved
// record is never usable for a create: that is the hole that let pblocksize come
// from the mutable controller default while logical came from the stamp.
func (g blockGeometry) complete() bool {
	return g.blocksize != nil && g.pblocksize != nil
}

// props renders the geometry keys this record carries, for stamping onto a
// destination dataset. A record that knows nothing yields nil, so a write that
// would have happened anyway stays byte-identical.
func (g blockGeometry) props() map[string]string {
	props := make(map[string]string, 2)
	if g.blocksize != nil {
		props[PropBlockISCSIBlocksize] = strconv.Itoa(*g.blocksize)
	}
	if g.pblocksize != nil {
		props[PropBlockISCSIPblocksize] = strconv.FormatBool(*g.pblocksize)
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

// stampGeometry lifts the two geometry fields out of a parsed stamp. The result
// is evidence FRAGMENTS: knowledge is only geometryKnown when both are present.
func stampGeometry(o *blockOpts, provenance string) blockGeometry {
	g := blockGeometry{knowledge: geometryUnexamined, provenance: provenance}
	if o != nil {
		g.blocksize = o.iscsiBlocksize
		g.pblocksize = o.iscsiPblocksize
	}
	if g.complete() {
		g.knowledge = geometryKnown
	}
	return g
}

// liveGeometry lifts the geometry out of a live extent. A backend that omits a
// field contributes nothing for it — an unknown value is never invented.
func liveGeometry(extent *truenas.ISCSIExtent, provenance string) blockGeometry {
	g := blockGeometry{knowledge: geometryUnexamined, provenance: provenance}
	if extent == nil {
		return g
	}
	if extent.Blocksize != 0 {
		blocksize := extent.Blocksize
		g.blocksize = &blocksize
	}
	if extent.Pblocksize != nil {
		pblocksize := *extent.Pblocksize
		g.pblocksize = &pblocksize
	}
	if g.complete() {
		g.knowledge = geometryKnown
	}
	return g
}

// snapshotGeometry reads the geometry a SNAPSHOT captured. A ZFS snapshot holds
// the dataset's user properties as of the instant it was taken, so this — and
// only this — is a record of the layout of the bytes IN THE SNAPSHOT. The source
// dataset's current stamp and current live extent describe the source NOW, which
// is a different question (see the snapshot arm of
// resolveCloneSourceBlockGeometry).
func snapshotGeometry(snap *truenas.Snapshot) blockGeometry {
	g := blockGeometry{knowledge: geometryUnexamined, provenance: "the geometry the snapshot captured"}
	if snap == nil {
		return g
	}
	read := func(key string) string {
		prop, ok := snap.UserProperties[key]
		if !ok {
			return ""
		}
		value := strings.TrimSpace(prop.Value)
		if value == "-" {
			return ""
		}
		return value
	}
	if raw := read(PropBlockISCSIBlocksize); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil {
			g.blocksize = &value
		} else {
			klog.Warningf("Ignoring malformed snapshot geometry property %s=%q on %s: %v", PropBlockISCSIBlocksize, raw, snap.ID, err)
		}
	}
	if raw := read(PropBlockISCSIPblocksize); raw != "" {
		if value, err := strconv.ParseBool(raw); err == nil {
			g.pblocksize = &value
		} else {
			klog.Warningf("Ignoring malformed snapshot geometry property %s=%q on %s: %v", PropBlockISCSIPblocksize, raw, snap.ID, err)
		}
	}
	if g.complete() {
		g.knowledge = geometryKnown
	}
	return g
}

// mergeGeometry fills the gaps in primary from fill without ever overriding a
// value primary already holds, and recomputes knowledge from the result.
func mergeGeometry(primary, fill blockGeometry) blockGeometry {
	merged := primary
	if merged.blocksize == nil && fill.blocksize != nil {
		merged.blocksize = fill.blocksize
		if merged.provenance == "" {
			merged.provenance = fill.provenance
		} else if fill.provenance != "" && fill.provenance != merged.provenance {
			merged.provenance = merged.provenance + " and " + fill.provenance
		}
	}
	if merged.pblocksize == nil && fill.pblocksize != nil {
		merged.pblocksize = fill.pblocksize
		if merged.provenance == "" {
			merged.provenance = fill.provenance
		} else if fill.provenance != "" && !strings.Contains(merged.provenance, fill.provenance) {
			merged.provenance = merged.provenance + " and " + fill.provenance
		}
	}
	if merged.complete() {
		merged.knowledge = geometryKnown
	} else if merged.knowledge == geometryKnown {
		merged.knowledge = geometryUnexamined
	}
	return merged
}

// blockDataHistoryWitnesses is the FULL witness set for "this storage may
// already hold block-addressed data".
//
// Round 4 asked one property (PropISCSIExtentID, via datasetUserPropertyHasValue)
// and therefore read both "absent" and the "-" sentinel as "no history" — while
// a detached copy sets exactly that sentinel BECAUSE the dataset carries somebody
// else's bytes (provenance.go), the warning-only resource-ID update can simply
// lose the witness, and a content-source stamp proves the data came from
// elsewhere. Every one of those is now a witness, and PRESENCE (including "-")
// counts, not "has a meaningful value".
var blockDataHistoryWitnesses = []string{
	PropISCSIExtentID,
	PropISCSITargetID,
	PropISCSITargetExtentID,
	PropNVMeoFNamespaceID,
	PropNVMeoFSubsystemID,
	PropNVMeoFPortSubsysID,
	PropBlockISCSIBlocksize,
	PropBlockISCSIPblocksize,
	PropVolumeContentSourceType,
	PropVolumeContentSourceID,
	PropVolumeOriginSnapshot,
}

// datasetMayHoldBlockData reports whether anything the driver can see says this
// dataset's bytes may already be addressed through a logical block size, and
// names the witness. A dataset the driver could not read is treated as
// data-bearing: absence of evidence is not evidence of absence.
func datasetMayHoldBlockData(ds *truenas.Dataset) (mayHoldData bool, witness string) {
	if ds == nil {
		return true, "the dataset could not be read"
	}
	for _, key := range blockDataHistoryWitnesses {
		if _, ok := ds.UserProperties[key]; ok {
			return true, key
		}
	}
	return false, ""
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
//
// WHAT THIS CLAIMS, EXACTLY. It records what the extent reports NOW. That is a
// fact about how the data is addressed today, and recording it stops every later
// rebuild — and every later change to the helm default — from reaching this
// volume. It is NOT a claim about history. A volume whose extent was already
// re-created at the wrong geometry by a pre-fix defaulted rebuild presents an
// unstamped live 512 extent over 4096-layout bytes, which is observationally
// identical to a correct 512 volume; back-stamping freezes that observable state
// as the record and cannot repair it. Correcting such a volume requires an
// operator who knows the original geometry. Do not read this function as
// certification of truth; read it as "the driver stops guessing from here on".
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

// extentGeometry is the ONE complete, validated record an iSCSI extent is
// created from. Both fields are concrete; there is no way to construct one where
// only half the geometry was resolved, which is what makes "logical from the
// stamp, physical from today's helm value" unrepresentable.
type extentGeometry struct {
	blocksize  int
	pblocksize bool
	// provenance names the evidence, and is logged with every create so the
	// answer to "why is this extent 4096" is in the operator's log, not in a
	// reviewer's reconstruction.
	provenance string
}

// resolvedGeometryContextKey carries the CreateVolume-scoped geometry DECISION
// (the clone/restore source resolution) to the share builder. It is the same
// request-scoped threading the CHAP resolution and the block opts use, and it is
// what stops the share builder from having to RE-derive a clone destination's
// geometry from properties — the round-4 shape in which a destination whose
// source provably had no history was indistinguishable from one whose source's
// geometry had simply been lost.
type resolvedGeometryContextKey struct{}

func withResolvedGeometry(ctx context.Context, g blockGeometry) context.Context {
	if g.knowledge == geometryUnexamined {
		return ctx
	}
	return context.WithValue(ctx, resolvedGeometryContextKey{}, g)
}

func resolvedGeometryFromContext(ctx context.Context) (blockGeometry, bool) {
	g, ok := ctx.Value(resolvedGeometryContextKey{}).(blockGeometry)
	return g, ok
}

// resolveExtentGeometry is THE choke point: the ONE function that decides the
// geometry an extent is created at, resolving logical and physical TOGETHER from
// a single body of evidence.
//
// The evidence ladder, strongest first — note that an explicit StorageClass
// request appears NOWHERE in it, because a request is intent about the future,
// not evidence about bytes already on disk:
//
//  1. freshlyCreated — DatasetCreate produced this zvol during this very call.
//     There is nothing on it, so nothing can be contradicted: the request (else
//     the controller-wide default) supplies BOTH fields.
//  2. The CreateVolume-scoped clone/restore decision (resolvedGeometryFromContext).
//     geometryUnknown fails closed here rather than after a destination has been
//     mutated; geometryNoHistory means the source provably held no
//     block-addressed data, so the destination holds none either.
//  3. The volume's own durable stamp, merged with (2).
//  4. No witness in blockDataHistoryWitnesses — this storage has never been
//     block-addressed by anything the driver can see, so the default cannot lie.
//  5. Otherwise: UNKNOWN. Fail closed, naming BOTH properties an operator can
//     record to recover. This is the state in which every previous round quietly
//     wrote the current helm default over old data.
//
// An explicit request is finally checked for AGREEMENT with the resolved record
// (rule (b)); a disagreement is a data-corruption refusal, not a preference.
func (d *Driver) resolveExtentGeometry(
	ctx context.Context,
	requestOpts, storedOpts *blockOpts,
	ds *truenas.Dataset,
	datasetName string,
	freshlyCreated bool,
) (extentGeometry, error) {
	merged := mergeBlockOpts(requestOpts, storedOpts)

	// (1) A zvol this very call created.
	if freshlyCreated {
		return d.dataFreeGeometry(merged, "the zvol was created by this call and holds no data"), nil
	}

	evidence := stampGeometry(storedOpts, "the volume's recorded geometry stamp")

	// (2) The clone/restore decision made before the first destination mutation.
	if carried, ok := resolvedGeometryFromContext(ctx); ok {
		switch carried.knowledge {
		case geometryUnknown:
			return extentGeometry{}, d.unknownGeometryError(datasetName, carried.provenance)
		case geometryNoHistory:
			if !evidence.complete() {
				return d.dataFreeGeometry(merged, carried.provenance), nil
			}
		case geometryKnown:
			evidence = mergeGeometry(evidence, carried)
		case geometryUnexamined:
		}
	}

	// (3) A complete record from evidence about the bytes.
	if evidence.complete() {
		if err := guardRequestAgainstEvidence(requestOpts, evidence, datasetName); err != nil {
			return extentGeometry{}, err
		}
		return extentGeometry{
			blocksize:  *evidence.blocksize,
			pblocksize: *evidence.pblocksize,
			provenance: evidence.provenance,
		}, nil
	}

	// (4) Nothing the driver can see says this storage was ever block-addressed.
	if mayHoldData, witness := datasetMayHoldBlockData(ds); !mayHoldData {
		return d.dataFreeGeometry(merged, "no witness of this dataset ever having been block-addressed"), nil
	} else if !evidence.complete() {
		// (5) It may hold data and the record is absent or half-present.
		reason := fmt.Sprintf("its extent is absent, %s records that it has been block-addressed", witness)
		switch {
		case evidence.blocksize != nil:
			reason += fmt.Sprintf(", and it records %s=%d but no %s", PropBlockISCSIBlocksize, *evidence.blocksize, PropBlockISCSIPblocksize)
		case evidence.pblocksize != nil:
			reason += fmt.Sprintf(", and it records %s=%t but no %s", PropBlockISCSIPblocksize, *evidence.pblocksize, PropBlockISCSIBlocksize)
		default:
			reason += ", and it records neither geometry property"
		}
		return extentGeometry{}, d.unknownGeometryError(datasetName, reason)
	}
	return extentGeometry{}, d.unknownGeometryError(datasetName, "its geometry could not be established")
}

// dataFreeGeometry is the ONLY place the controller-wide defaults may be
// applied, and it resolves both fields in one step so physical can never come
// from a different generation of config than logical.
func (d *Driver) dataFreeGeometry(opts *blockOpts, provenance string) extentGeometry {
	return extentGeometry{
		blocksize:  opts.resolvedISCSIBlocksize(d.config.ISCSI.ExtentBlocksize),
		pblocksize: opts.resolvedISCSIPblocksize(d.config.ISCSI.ExtentDisablePhysicalBlocksize),
		provenance: provenance,
	}
}

// guardRequestAgainstEvidence enforces rule (b) at the choke point: a request may
// AGREE with what the data is, never define it.
func guardRequestAgainstEvidence(request *blockOpts, evidence blockGeometry, datasetName string) error {
	if request == nil {
		return nil
	}
	if request.iscsiBlocksize != nil && evidence.blocksize != nil && *request.iscsiBlocksize != *evidence.blocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create the iSCSI extent for %s at %s=%d: %s says its data is addressed through %d-byte logical blocks. "+
				"A StorageClass parameter is a statement of intent, not evidence about bytes that already exist — provision a "+
				"new volume to change %s",
			datasetName, paramISCSIBlocksize, *request.iscsiBlocksize, evidence.provenance, *evidence.blocksize, paramISCSIBlocksize)
	}
	if request.iscsiPblocksize != nil && evidence.pblocksize != nil && *request.iscsiPblocksize != *evidence.pblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"cannot create the iSCSI extent for %s at %s=%t: %s says its data was laid out for %t. "+
				"A StorageClass parameter is a statement of intent, not evidence about bytes that already exist — provision a "+
				"new volume to change %s",
			datasetName, paramISCSIPblocksize, *request.iscsiPblocksize, evidence.provenance, *evidence.pblocksize, paramISCSIPblocksize)
	}
	return nil
}

// unknownGeometryError is the single fail-closed message for rule (a). It names
// BOTH properties, because a record that resolves only logical is exactly the
// half-resolved state that let physical come from the mutable controller default.
func (d *Driver) unknownGeometryError(datasetName, reason string) error {
	return status.Errorf(codes.FailedPrecondition,
		"cannot create the iSCSI extent for %s: %s, so the driver cannot establish the geometry its data is addressed "+
			"through. Falling back to the controller-wide defaults (iscsi.extentBlocksize=%d, pblocksize=%t) would lay a "+
			"GUESSED geometry over data that may have been written against a different one, which corrupts it — the driver "+
			"refuses instead. Recover by restoring the original extent, or by recording the real geometry "+
			"(zfs set %s=<512|1024|2048|4096> %s=<true|false> %s) and retrying",
		datasetName, reason, d.config.ISCSI.ExtentBlocksize, !d.config.ISCSI.ExtentDisablePhysicalBlocksize,
		PropBlockISCSIBlocksize, PropBlockISCSIPblocksize, datasetName)
}

// validateExtentAgainstGeometry is the retry/idempotency half of the choke point.
//
// Round 4 accepted whatever object came back from the create-error recovery arms
// — ISCSIExtentFindByDisk after an ambiguous error (iscsi_share.go), and the
// client's own find-by-name fallback on "already exists"/"invalid params"
// (truenas/iscsi.go) — with no geometry check at all. A concurrent controller or
// a stale same-name object could therefore win the race with a DIFFERENT
// geometry, and the next resource update would back-stamp it as this volume's
// truth. Every arm that yields an extent now proves it matches the record the
// create was authorized against.
func validateExtentAgainstGeometry(extent *truenas.ISCSIExtent, geometry extentGeometry, datasetName string) error {
	if extent == nil {
		return nil
	}
	if extent.Blocksize != 0 && extent.Blocksize != geometry.blocksize {
		return status.Errorf(codes.FailedPrecondition,
			"the iSCSI extent now present for %s reports blocksize %d but this volume's data is addressed through %d (%s). "+
				"The driver did not create this object — a concurrent controller or a stale same-name extent did — and it "+
				"refuses to adopt it or to back-stamp its geometry as this volume's truth. Remove the conflicting extent and retry",
			datasetName, extent.Blocksize, geometry.blocksize, geometry.provenance)
	}
	if extent.Pblocksize != nil && *extent.Pblocksize != geometry.pblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"the iSCSI extent now present for %s reports physical-blocksize reporting %t but this volume was resolved to %t (%s). "+
				"The driver did not create this object and refuses to adopt it. Remove the conflicting extent and retry",
			datasetName, *extent.Pblocksize, geometry.pblocksize, geometry.provenance)
	}
	return nil
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
// Round 5 makes the answer a TRI-STATE and splits the two questions round 4
// conflated:
//
//   - A SNAPSHOT restore asks about the bytes IN THE SNAPSHOT. The source
//     dataset's CURRENT stamp and CURRENT live extent answer a different
//     question — what the source is addressed through NOW — and round 4 used
//     them anyway, so a pre-GF4 source that wrote through 4096, was snapshotted,
//     and later had its extent re-created at 512 would restore the OLD snapshot
//     at 512 over 4096-layout bytes. Provenance is therefore tied to the snapshot
//     itself: a ZFS snapshot holds the dataset's user properties as of the
//     instant it was taken, so the geometry stamp the snapshot CAPTURED is the
//     only record of the snapshot's own layout. Where the snapshot captured none
//     and the source has block-data history, the answer is geometryUnknown and
//     the restore fails closed. (CreateSnapshot now stamps every snapshot it
//     takes, so this window closes for everything created from here on.)
//   - A PVC-to-PVC clone asks about the bytes as they are NOW: its temporary
//     snapshot is taken from the source's current state moments later, so the
//     source's live extent IS the layout, and reconcileSourceGeometry answers.
//
// sourceDS may be supplied by a caller that already queried the source (the
// volume-clone path does), which saves the DatasetGet there. NFS short-circuits
// before any API call, so no filesystem path pays for this.
func (d *Driver) resolveCloneSourceBlockGeometry(
	ctx context.Context,
	sourceDataset string,
	sourceDS *truenas.Dataset,
	snap *truenas.Snapshot,
	sourceRef, datasetName string,
	shareType ShareType,
) (blockGeometry, error) {
	if !shareType.IsBlockProtocol() {
		return blockGeometry{knowledge: geometryUnexamined}, nil
	}

	// A snapshot that captured its own geometry answers for its own bytes, with
	// no read of the source's current state at all.
	if snap != nil {
		if captured := snapshotGeometry(snap); captured.knowledge == geometryKnown {
			if guardErr := guardCloneSourceGeometry(captured, blockOptsFromContext(ctx), sourceRef, datasetName); guardErr != nil {
				return blockGeometry{}, guardErr
			}
			return captured, nil
		}
	}

	if sourceDS == nil {
		var err error
		sourceDS, err = d.truenasClient.DatasetGet(ctx, sourceDataset)
		if err != nil {
			// Fail closed: about to lay a geometry over this source's data with no
			// way to read what that data is, which is the corruption itself.
			return blockGeometry{}, status.Errorf(codes.Internal,
				"failed to read the stored block geometry of clone source %s: %v", sourceDataset, err)
		}
	}

	// The snapshot captured nothing. The source's CURRENT state cannot establish
	// the snapshot's layout, so the only honest answers are "there was never any
	// block-addressed data to preserve" or "unknown".
	if snap != nil {
		if mayHoldData, witness := datasetMayHoldBlockData(sourceDS); !mayHoldData {
			return blockGeometry{
				knowledge:  geometryNoHistory,
				provenance: fmt.Sprintf("clone source %s has never been block-addressed", sourceRef),
			}, nil
		} else {
			return blockGeometry{}, status.Errorf(codes.FailedPrecondition,
				"cannot restore %s from %s: the snapshot records no %s/%s geometry of its own, and %s shows its source has "+
					"held block-addressed data — so the source's CURRENT extent describes the source now, not the layout of "+
					"the bytes inside this snapshot. Creating the destination from it would lay a guessed geometry over the "+
					"snapshot's data. Record the snapshot's real geometry and retry "+
					"(zfs set %s=<512|1024|2048|4096> %s=<true|false> %s); snapshots taken by this driver carry it "+
					"automatically",
				datasetName, sourceRef, PropBlockISCSIBlocksize, PropBlockISCSIPblocksize, witness,
				PropBlockISCSIBlocksize, PropBlockISCSIPblocksize, snap.ID)
		}
	}

	extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, "zvol/"+sourceDataset)
	if err != nil {
		return blockGeometry{}, status.Errorf(codes.Internal,
			"failed to read the live block geometry of clone source %s: %v", sourceDataset, err)
	}
	sourceGeometry, err := reconcileSourceGeometry(blockOptsFromDataset(sourceDS), extent, sourceRef)
	if err != nil {
		return blockGeometry{}, err
	}
	if guardErr := guardCloneSourceGeometry(sourceGeometry, blockOptsFromContext(ctx), sourceRef, datasetName); guardErr != nil {
		return blockGeometry{}, guardErr
	}
	if sourceGeometry.knowledge == geometryKnown {
		return sourceGeometry, nil
	}
	if mayHoldData, _ := datasetMayHoldBlockData(sourceDS); !mayHoldData {
		return blockGeometry{
			knowledge:  geometryNoHistory,
			provenance: fmt.Sprintf("clone source %s has never been block-addressed", sourceRef),
		}, nil
	}
	return blockGeometry{
		knowledge: geometryUnknown,
		provenance: fmt.Sprintf(
			"clone source %s may hold block-addressed data but neither a complete geometry stamp nor a live extent could "+
				"establish its layout", sourceRef),
	}, nil
}

// snapshotGeometryProps renders the geometry a new snapshot of datasetName must
// CAPTURE so that a later restore has provenance tied to the snapshot rather
// than to whatever the source looks like at restore time.
//
// It is best-effort by construction, and that is safe in exactly one direction:
// a snapshot whose geometry could not be captured is still taken, and restoring
// it later fails CLOSED (resolveCloneSourceBlockGeometry) instead of guessing.
// A missed capture therefore costs availability, never integrity.
//
// Cost: nil (no call) for a filesystem dataset, for a zvol nothing has ever
// exported, and for any volume the driver has already stamped — which is every
// volume it has created or published since the geometry stamp existed. One
// ISCSIExtentFindByDisk for an unstamped iSCSI zvol, i.e. the pre-GF4 fleet.
func (d *Driver) snapshotGeometryProps(ctx context.Context, ds *truenas.Dataset, datasetName string) map[string]string {
	if ds == nil || !strings.EqualFold(ds.Type, "VOLUME") {
		return nil
	}
	stamped := stampGeometry(blockOptsFromDataset(ds), "")
	if stamped.complete() {
		return stamped.props()
	}
	if !storedBlockProtocol(ds, ShareTypeISCSI) {
		return stamped.props()
	}
	extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, "zvol/"+datasetName)
	if err != nil {
		klog.Warningf("Could not capture the live iSCSI geometry of %s onto its snapshot (a later restore of it will "+
			"fail closed rather than guess): %v", datasetName, err)
		return stamped.props()
	}
	return mergeGeometry(stamped, liveGeometry(extent, "")).props()
}

// reconcileSourceGeometry applies the precedence rule of mechanism (4) to a
// clone source: the live extent is authoritative for what the data is, the stamp
// answers where there is no extent, and a disagreement is refused rather than
// silently resolved. A drifted source is exactly the state in which "which of
// these two numbers is the truth" cannot be answered from inside the driver.
func reconcileSourceGeometry(stamped *blockOpts, extent *truenas.ISCSIExtent, sourceRef string) (blockGeometry, error) {
	resolved := stampGeometry(stamped, fmt.Sprintf("clone source %s's recorded geometry stamp", sourceRef))
	if extent == nil {
		return resolved, nil
	}
	if extent.Blocksize != 0 {
		if resolved.blocksize != nil && *resolved.blocksize != extent.Blocksize {
			return blockGeometry{}, status.Errorf(codes.FailedPrecondition,
				"clone source %s records iSCSI extent blocksize %d but its live extent reports %d. The driver will not clone "+
					"data whose real geometry it cannot establish: reconcile the source's extent and its %s stamp, then retry",
				sourceRef, *resolved.blocksize, extent.Blocksize, PropBlockISCSIBlocksize)
		}
		blocksize := extent.Blocksize
		resolved.blocksize = &blocksize
	}
	if extent.Pblocksize != nil {
		if resolved.pblocksize != nil && *resolved.pblocksize != *extent.Pblocksize {
			return blockGeometry{}, status.Errorf(codes.FailedPrecondition,
				"clone source %s records iSCSI physical-blocksize reporting %t but its live extent reports %t. The driver will "+
					"not clone data whose real geometry it cannot establish: reconcile the source's extent and its %s stamp, "+
					"then retry",
				sourceRef, *resolved.pblocksize, *extent.Pblocksize, PropBlockISCSIPblocksize)
		}
		pblocksize := *extent.Pblocksize
		resolved.pblocksize = &pblocksize
	}
	if resolved.complete() {
		resolved.knowledge = geometryKnown
		resolved.provenance = fmt.Sprintf("clone source %s's live extent", sourceRef)
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
