package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
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
			"blocksize is fixed for the life of the volume and cannot be changed on an extent that holds data",
		datasetName, existing.Blocksize, *requested)
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
				"blocksize is fixed for the life of the volume (its filesystem and partition table are laid out against "+
				"that logical block size) — provision a new volume to change it",
			datasetName, *stored.iscsiBlocksize, *request.iscsiBlocksize)
	}
	if stored.iscsiPblocksize != nil && request.iscsiPblocksize != nil && *stored.iscsiPblocksize != *request.iscsiPblocksize {
		return status.Errorf(codes.FailedPrecondition,
			"volume %s was provisioned with immutable iSCSI physical-blocksize reporting %t; the StorageClass now resolves %t. "+
				"pblocksize is fixed at extent create and changes the alignment the initiator optimizes for — "+
				"provision a new volume to change it",
			datasetName, *stored.iscsiPblocksize, *request.iscsiPblocksize)
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
