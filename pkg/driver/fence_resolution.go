package driver

import (
	"context"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// fenceResolution is a per-request memo of backend share objects resolved during
// a single per-volume-locked fence pass (ControllerPublishVolume,
// ControllerUnpublishVolume, or one startup-fencing volume). It is threaded
// through the ensure-share, validate, classify, and apply phases so each reuses
// one backend read instead of re-resolving the same immutable objects — the
// duplicate namespace/subsystem resolution that made a strict NVMe-oF publish
// cost ~13 round trips.
//
// It is strictly a per-request memo and is NEVER cached across requests: a fresh
// instance is created per request and lives only as long as that request's call
// stack. A nil *fenceResolution is valid everywhere and means "no memo; resolve
// directly", which preserves the historical behavior exactly for callers that do
// not opt in (CreateVolume's ensure-share, delete, and unit tests).
//
// The namespace/subsystem/share/target identities are immutable within a request
// and are safe to resolve once, including before the stale-record takeover. The
// NVMe-oF host-subsystem association list is different: a takeover revokes an
// association and applyNVMeFence creates/removes associations, so callers load
// it only AFTER any takeover and treat it as a snapshot of the fence phase's
// starting state. The deliberate post-takeover dataset re-read (which refreshes
// publication records) is unaffected by this memo.
type fenceResolution struct {
	nvmeNamespace *truenas.NVMeoFNamespace
	nvmeSubsystem *truenas.NVMeoFSubsystem
	nvmeNSLoaded  bool

	nvmeAssociations []*truenas.NVMeoFHostSubsys
	nvmeAssocLoaded  bool

	nfsShare  *truenas.NFSShare
	nfsLoaded bool

	iscsiTarget *truenas.ISCSITarget
	iscsiLoaded bool
}

// resolvedNVMeObjects returns the volume's NVMe-oF namespace and subsystem,
// resolving them once per request and memoizing the pair. The subsystem is
// resolved from the namespace, matching resolveNVMeSubsystem's contract.
func (d *Driver) resolvedNVMeObjects(ctx context.Context, res *fenceResolution, ds *truenas.Dataset, datasetName string) (*truenas.NVMeoFNamespace, *truenas.NVMeoFSubsystem, error) {
	if res != nil && res.nvmeNSLoaded {
		return res.nvmeNamespace, res.nvmeSubsystem, nil
	}
	namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
	if err != nil {
		return nil, nil, err
	}
	subsystem, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
	if err != nil {
		return nil, nil, err
	}
	if res != nil {
		res.nvmeNamespace = namespace
		res.nvmeSubsystem = subsystem
		res.nvmeNSLoaded = true
	}
	return namespace, subsystem, nil
}

// resolvedNVMeAssociations returns the subsystem's host-subsystem association
// list, reading it once per request and memoizing it. Callers must invoke this
// only after any stale-record takeover so the snapshot reflects post-revocation
// state.
func (d *Driver) resolvedNVMeAssociations(ctx context.Context, res *fenceResolution, subsysID int) ([]*truenas.NVMeoFHostSubsys, error) {
	if res != nil && res.nvmeAssocLoaded {
		return res.nvmeAssociations, nil
	}
	associations, err := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsysID)
	if err != nil {
		return nil, err
	}
	if res != nil {
		res.nvmeAssociations = associations
		res.nvmeAssocLoaded = true
	}
	return associations, nil
}

// refreshNVMeAssociations always reads the backend and replaces the memo. Fence
// enforcement uses this at mutation boundaries because compatibility reads are
// classification evidence, not a lock against another controller or operator.
func (d *Driver) refreshNVMeAssociations(ctx context.Context, res *fenceResolution, subsysID int) ([]*truenas.NVMeoFHostSubsys, error) {
	associations, err := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsysID)
	if err != nil {
		return nil, err
	}
	if res != nil {
		res.nvmeAssociations = associations
		res.nvmeAssocLoaded = true
	}
	return associations, nil
}

func (res *fenceResolution) invalidateNVMeAssociations() {
	if res == nil {
		return
	}
	res.nvmeAssociations = nil
	res.nvmeAssocLoaded = false
}

func (res *fenceResolution) storeNVMeObjects(namespace *truenas.NVMeoFNamespace, subsystem *truenas.NVMeoFSubsystem) {
	if res == nil {
		return
	}
	res.nvmeNamespace = namespace
	res.nvmeSubsystem = subsystem
	res.nvmeNSLoaded = true
	res.invalidateNVMeAssociations()
}

// resolvedNFSShare returns the volume's NFS share, resolving it once per request
// and memoizing it.
func (d *Driver) resolvedNFSShare(ctx context.Context, res *fenceResolution, ds *truenas.Dataset, datasetName string) (*truenas.NFSShare, error) {
	if res != nil && res.nfsLoaded {
		return res.nfsShare, nil
	}
	share, err := d.resolveNFSShare(ctx, ds, datasetName)
	if err != nil {
		return nil, err
	}
	if res != nil {
		res.nfsShare = share
		res.nfsLoaded = true
	}
	return share, nil
}

// resolvedISCSITarget returns the volume's iSCSI target, resolving it once per
// request and memoizing it.
func (d *Driver) resolvedISCSITarget(ctx context.Context, res *fenceResolution, ds *truenas.Dataset, datasetName string) (*truenas.ISCSITarget, error) {
	if res != nil && res.iscsiLoaded {
		return res.iscsiTarget, nil
	}
	target, err := d.resolveISCSITarget(ctx, ds, datasetName)
	if err != nil {
		return nil, err
	}
	if res != nil {
		res.iscsiTarget = target
		res.iscsiLoaded = true
	}
	return target, nil
}
