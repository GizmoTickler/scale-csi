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
		if value < 1 {
			return nil, status.Errorf(codes.InvalidArgument, "%s must be a positive integer; got %d", paramNVMeoFQidMax, value)
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

// guardISCSIBlocksizeImmutability enforces R-1: an extent's blocksize is fixed
// for the life of the volume. When an existing extent is found whose blocksize
// differs from the resolved request, the request is rejected rather than
// silently keeping a divergent geometry (which would desync the StorageClass
// contract from the backend). A zero stored blocksize (legacy extent predating
// blocksize reporting) is not proof of a mismatch and is left alone.
func guardISCSIBlocksizeImmutability(existing *truenas.ISCSIExtent, resolved int, datasetName string) error {
	if existing == nil || existing.Blocksize == 0 || resolved == 0 || existing.Blocksize == resolved {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition,
		"iSCSI extent for %s already exists with immutable blocksize %d; requested %d. "+
			"blocksize is fixed for the life of the volume and cannot be changed on an extent that holds data",
		datasetName, existing.Blocksize, resolved)
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
