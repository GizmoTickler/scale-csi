package driver

import (
	"context"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ShareBackend is the per-protocol share implementation. It collapses the
// `switch shareType` dispatch that was previously scattered across
// ensureShareExists, createShareWithOptions, and deleteShare into a single
// backendForShareType selector, so adding a protocol means adding one backend
// rather than touching every dispatch site.
//
// Each backend is a thin view over the Driver: the per-protocol bodies remain
// the existing Driver methods (byte-identical), and the backend methods route
// to them. This keeps the deeply Driver-coupled create/delete logic in place
// while giving callers one polymorphic entry point per operation.
type ShareBackend interface {
	// EnsureShare makes the share exist for an already-provisioned dataset
	// (idempotent create used on the retry path). res, when non-nil, receives the
	// backend objects the ensure resolved so the subsequent fence phases reuse
	// them instead of re-resolving (see fenceResolution).
	EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, res *fenceResolution) error
	// CreateShare creates the share for a dataset. freshlyCreated skips
	// guaranteed-miss idempotency lookups; zvolReady indicates the zvol was
	// returned by DatasetCreate or the clone readiness wait completed;
	// finalProperties carries stamps NFS folds into its share-ID write (block
	// protocols ignore them and are stamped separately).
	CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error
	// DeleteShare removes the share for a dataset.
	DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error
	// ApplyFence converges the backend's host allowlist to the published set.
	// applyBackendFence computes the shared inputs (enforceable/removing
	// identities and the additive-ownership/protected grant sets) and each
	// backend applies the subset its protocol uses, preserving the former
	// per-protocol switch exactly. res carries the request's memoized backend
	// objects (see fenceResolution); backends reuse them instead of re-resolving.
	// hasDeferredActiveISCSI reports that at least one ACTIVE iSCSI identity was
	// deferred (live publication whose IQN is momentarily unresolvable) rather
	// than removed. Only the iSCSI backend consumes it: when the real-IQN
	// allowlist is empty AND a deferred-active identity exists, the empty set is
	// NOT a genuine last-unpublish, so iSCSI skips the mutation instead of
	// writing the deny-all sentinel (which would self-fence the live node).
	ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string, hasDeferredActiveISCSI bool, res *fenceResolution) error
	// VolumeContext populates the protocol-specific publish context keys onto
	// volumeContext (which already carries node_attach_driver).
	VolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error
}

// backendForShareType returns the ShareBackend for shareType, or nil for an
// unrecognized type. Callers decide how to treat nil (the create path maps it
// to InvalidArgument; the ensure/delete paths treat it as a no-op), preserving
// each former switch's default branch exactly.
func backendForShareType(d *Driver, shareType ShareType) ShareBackend {
	switch shareType {
	case ShareTypeNFS:
		return nfsShareBackend{d: d}
	case ShareTypeISCSI:
		return iscsiShareBackend{d: d}
	case ShareTypeNVMeoF:
		return nvmeoFShareBackend{d: d}
	default:
		return nil
	}
}
