package driver

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// nvmeoFDataPathKey is both the StorageClass parameter that pins an NVMe-oF
// volume's node data path and the volume-context key that carries the choice
// from CreateVolume to NodeStage. Kubernetes persists the volume context in
// the PV, so the choice survives driver restarts and later changes to the
// install-wide nvmeof.dataPath default.
const nvmeoFDataPathKey = "nvmeof/dataPath"

type nvmeoFDataPathContextKey struct{}

// withNVMeoFDataPath threads the StorageClass's data-path choice to
// getVolumeContext, mirroring the other per-StorageClass resolutions. An empty
// value (the StorageClass did not choose) is not stored.
func withNVMeoFDataPath(ctx context.Context, dataPath string) context.Context {
	if dataPath == "" {
		return ctx
	}
	return context.WithValue(ctx, nvmeoFDataPathContextKey{}, dataPath)
}

func nvmeoFDataPathFromContext(ctx context.Context) string {
	dataPath, _ := ctx.Value(nvmeoFDataPathContextKey{}).(string)
	return dataPath
}

// nvmeoFDataPathForCreate validates the StorageClass's nvmeof/dataPath
// parameter. It returns "" when the parameter is absent, so a StorageClass that
// does not set it provisions a volume context identical to before this
// parameter existed; that volume follows the install-wide default at stage
// time. Validation is pure (no backend I/O), so a typo is InvalidArgument
// before anything is created.
func (d *Driver) nvmeoFDataPathForCreate(params map[string]string, shareType ShareType) (string, error) {
	raw, present := params[nvmeoFDataPathKey]
	if !present {
		return "", nil
	}
	if shareType != ShareTypeNVMeoF {
		return "", status.Errorf(codes.InvalidArgument,
			"StorageClass parameter %s applies only to protocol nvmeof, not %s", nvmeoFDataPathKey, shareType)
	}
	dataPath, ok := normalizeNVMeoFDataPath(raw)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", status.Errorf(codes.InvalidArgument,
			"StorageClass parameter %s must be %q or %q, got %q", nvmeoFDataPathKey, NVMeoFDataPathKernel, NVMeoFDataPathUblk, raw)
	}
	if dataPath == NVMeoFDataPathUblk && !d.config.NVMeoF.ublkAvailable() {
		return "", status.Errorf(codes.InvalidArgument,
			"StorageClass parameter %s=%s requires the ublk data path to be enabled on this driver (nvmeof.ublk.enabled or nvmeof.dataPath: ublk)",
			nvmeoFDataPathKey, NVMeoFDataPathUblk)
	}
	return dataPath, nil
}

// nvmeoFDataPathForVolume is the data path NodeStage uses: the volume's own
// pinned choice first, then the install-wide default. A malformed pinned value
// is an error rather than a silent fallback, because falling back could stage
// a volume that asked for one data path through the other.
func (d *Driver) nvmeoFDataPathForVolume(volumeContext map[string]string) (string, error) {
	raw, present := volumeContext[nvmeoFDataPathKey]
	if !present {
		return d.config.NVMeoF.defaultDataPath(), nil
	}
	dataPath, ok := normalizeNVMeoFDataPath(raw)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", status.Errorf(codes.InvalidArgument,
			"volume context %s must be %q or %q, got %q", nvmeoFDataPathKey, NVMeoFDataPathKernel, NVMeoFDataPathUblk, raw)
	}
	return dataPath, nil
}
