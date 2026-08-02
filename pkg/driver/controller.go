package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Import truenas package for error helper functions (aliased for clarity in error checks)

// ZFS property names for tracking CSI resources
const (
	PropManagedResource  = "truenas-csi:managed_resource"
	PropDriverInstanceID = "truenas-csi:driver_instance_id"
	// PropDriverInstanceIDAdopted marks an ownership stamp written by legacy
	// reconciliation rather than by createDataset. It keeps adoption useful for
	// cleanup provenance without making pre-existing bytes eligible for a
	// create-time data-free proof.
	PropDriverInstanceIDAdopted = "truenas-csi:driver_instance_id_adopted"
	PropProvisionSuccess        = "truenas-csi:provision_success"
	PropCSIVolumeName           = "truenas-csi:csi_volume_name"
	PropShareVolumeContext      = "truenas-csi:csi_share_volume_context"
	PropVolumeContentSourceType = "truenas-csi:csi_volume_content_source_type"
	PropVolumeContentSourceID   = "truenas-csi:csi_volume_content_source_id"
	PropVolumeOriginSnapshot    = "truenas-csi:csi_volume_origin_snapshot" // temp snapshot created during volume-to-volume cloning
	PropInternalResource        = "truenas-csi:internal_resource"          // internal snapshots that must not be exposed through ListSnapshots
	PropRequestedSizeBytes      = "truenas-csi:requested_size_bytes"       // requested capacity for quota-less filesystem volumes
	// PropZFSPerformanceClass records the curated ZFS performance class a volume
	// was CREATED with. It is the anchor for the create-only property guard: a
	// later StorageClass edit is compared against this stamp, never re-derived
	// from the live dataset (whose immutable geometry could not be changed anyway).
	// Only stamped when a class was requested, so it never appears on volumes
	// that do not use the feature.
	PropZFSPerformanceClass       = "truenas-csi:zfs_performance_class"
	PropCSISnapshotName           = "truenas-csi:csi_snapshot_name"
	PropCSISnapshotSourceVolumeID = "truenas-csi:csi_snapshot_source_volume_id"
	snapshotTombstoneMarker       = "-csi-deleted-"
	PropNFSShareID                = "truenas-csi:truenas_nfs_share_id"
	PropISCSITargetID             = "truenas-csi:truenas_iscsi_target_id"
	PropISCSIExtentID             = "truenas-csi:truenas_iscsi_extent_id"
	PropISCSITargetExtentID       = "truenas-csi:truenas_iscsi_targetextent_id"
	PropISCSIInitiatorID          = "truenas-csi:truenas_iscsi_initiator_id"
	// PropISCSIAuthTag stores the iscsi.auth TAG that the target group's auth ref
	// points at (G1: SCST only emits IncomingUser for tag-keyed refs). It is the
	// durable link fence/rebuild passes use to re-stamp CHAP without the secret.
	PropISCSIAuthTag = "truenas-csi:truenas_iscsi_auth_tag"
	// PropISCSIAuthMode stores the immutable per-volume auth mode (CHAP or
	// CHAP_MUTUAL) stamped at CreateVolume. Every later path (fence, volume
	// context, idempotent replay) reads THIS, never the mutable controller-wide
	// iscsi.chap.mutual flag, so a global-flag flip cannot downgrade or upgrade an
	// existing volume and mixed one-way/mutual StorageClasses coexist correctly.
	PropISCSIAuthMode = "truenas-csi:truenas_iscsi_auth_mode"

	// Block-protocol tuning (GF-Sprint 4) persisted per volume. The resolved
	// per-StorageClass options are stamped here at CreateVolume so EVERY later
	// path that rebuilds or re-ensures the share for an EXISTING volume
	// (ControllerPublishVolume -> ensureShareExists, the startup attachment
	// reconcile, a DR/restore rebuild) resolves the volume's OWN geometry and
	// safety knobs instead of collapsing to the controller default.
	//
	// This mirrors PropISCSIAuthMode/PropISCSIAuthTag, which exist for exactly
	// this reason. Without it: a rebuild whose extent is ABSENT silently
	// re-created the extent at the 512 controller default over data laid down
	// against 4096-byte logical blocks (silent corruption); a rebuild whose
	// extent EXISTS was rejected forever by the immutability guard (stored 4096
	// vs default 512 => permanently unattachable); and every other
	// create-time-only option (stable serial, read-only, insecure_tpc, target
	// auth_networks, avail_threshold, qid_max, pi_enable) was silently dropped.
	//
	// ONLY keys whose option was actually SET are stamped: an absent key means
	// "use the controller default", so a StorageClass that opts into nothing
	// stamps nothing and provisions byte-identically to pre-GF4.
	//
	// Unlike the CHAP props these are read WITHOUT the source==local guard. CHAP
	// is a credential policy a clone must never inherit; block geometry
	// describes the DATA layout, which a ZFS clone shares byte-for-byte with its
	// source, so inheriting it is both correct and the safe direction.
	PropBlockISCSIBlocksize      = "truenas-csi:block_blocksize"
	PropBlockISCSIPblocksize     = "truenas-csi:block_pblocksize"
	PropBlockISCSIQueuedCommands = "truenas-csi:block_queued_commands"
	PropBlockISCSIInsecureTpc    = "truenas-csi:block_insecure_tpc"
	PropBlockISCSIReadOnly       = "truenas-csi:block_readonly"
	PropBlockISCSIAvailThreshold = "truenas-csi:block_avail_threshold"
	PropBlockISCSISerial         = "truenas-csi:block_serial"
	PropBlockISCSIAuthNetworks   = "truenas-csi:block_auth_networks"
	PropBlockNVMeoFQidMax        = "truenas-csi:nvme_qid_max"
	PropBlockNVMeoFPiEnable      = "truenas-csi:nvme_pi_enable"

	PropNVMeoFSubsystemID  = "truenas-csi:truenas_nvmeof_subsystem_id"
	PropNVMeoFNamespaceID  = "truenas-csi:truenas_nvmeof_namespace_id"
	PropNVMeoFPortSubsysID = "truenas-csi:truenas_nvmeof_portsubsys_id"

	// NOTE (GF2-fix4/L1): there is deliberately NO snapshot-task-id property.
	// One existed, written on every scheduled CreateVolume and read by nothing:
	// deleteVolumeSnapshotTask resolves the task through SnapshotTaskListByDataset
	// plus the schema proof (which it must do anyway — a stamped id that pointed at
	// a pre-existing FOREIGN task could never be allowed to authorize deleting it),
	// so the id could only ever have saved a query the delete path still makes. It
	// cost an extra DatasetSetUserProperties round trip per scheduled create and
	// left a stale, meaningless value on every clone of a scheduled volume. Do not
	// reintroduce it without a reader that is gated behind the same schema proof.

	// PropInflightMarkerPrefix namespaces per-volume in-flight content-source
	// creation markers written on the PARENT dataset (the only object proven to
	// accept durable user-property writes on 26.0 while the destination does not
	// exist yet). A marker is POSITIVE durable proof that this driver instance
	// started a clone/copy toward that destination; crash recovery is gated on it.
	PropInflightMarkerPrefix = "truenas-csi:inflight_"
	// PropTombstoneLedgerPrefix namespaces the parent-dataset ledger of
	// deferred-delete tombstone snapshots. Written BEFORE the tombstone rename so
	// the reaper only ever destroys snapshots this driver provably tombstoned; a
	// crash between ledger write and rename leaves a ledger entry without a
	// tombstone, which the reconciler sweeps.
	PropTombstoneLedgerPrefix = "truenas-csi:tombstone_"
	// PropRecoveryNonce is a per-attempt compare-and-swap value included in the
	// remnant recovery ownership stamp. operationLock is per-process, so two
	// overlapping controllers (upgrade window) can both attempt recovery; the
	// post-write re-read proves whose stamp won and the loser returns Aborted.
	PropRecoveryNonce = "truenas-csi:recovery_nonce"

	inflightMarkerVersion  = 1
	tombstoneLedgerVersion = 2
	inflightModeClone      = "clone"
	inflightModeCopy       = "copy"
)

const (
	originSnapshotDeleteAttempts           = 3
	originSnapshotDeleteBackoff            = 500 * time.Millisecond
	originSnapshotDeleteMaxBackoff         = 2 * time.Second
	detachedCopyJobAbortTimeout            = 10 * time.Second
	replicationJobReasonCreateVolumeFailed = "create_volume_failed"

	// truenasMinRefquotaBytes is the smallest refquota TrueNAS accepts on a
	// dataset. Below it, pool.dataset.create fails with the unqualified -32602
	// "Invalid params" that every other schema/validation error also produces
	// (observed live with a 64 MiB request, 2026-08-02).
	truenasMinRefquotaBytes = 1024 * 1024 * 1024
)

func isDatasetDependencyOrBusyError(err error) bool {
	if err == nil {
		return false
	}
	// TrueNAS surfaces the real reason (e.g. ENOTEMPTY "has snapshots") in the
	// API error's Data field, not its top-level message — over the WebSocket
	// API a has-snapshots delete arrives as a generic -32602 "Invalid params".
	// Inspect FullError() so the dependency markers below can match.
	message := strings.ToLower(err.Error())
	var apiErr *truenas.APIError
	if errors.As(err, &apiErr) {
		message = strings.ToLower(apiErr.FullError())
	}
	for _, marker := range []string{"busy", "dependent", "snapshot", "has children", "method call error", "enotempty"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func snapshotBlocksVolumeDeletion(snap *truenas.Snapshot) bool {
	if snap == nil || isSnapshotTombstone(snap) {
		return false
	}
	// Internal-resource is safe to inspect on the 26.0 flat read path: datasets
	// never carry this snapshot-only property, so it cannot be inherited.
	if prop, ok := snap.UserProperties[PropInternalResource]; ok && prop.Value == "true" {
		return true
	}
	return isCSISnapshot(snap)
}

func isInternalCloneSourceSnapshot(snap *truenas.Snapshot) bool {
	if snap == nil || isSnapshotTombstone(snap) || !strings.HasPrefix(snap.Name, "clone-source-") {
		return false
	}
	prop, ok := snap.UserProperties[PropInternalResource]
	return ok && prop.Value == "true"
}

// deleteOrphanedInternalCloneSourceSnapshots removes driver-owned snapshots
// after DatasetHasDependentClones has authoritatively confirmed that no clone
// still references any snapshot of the source dataset.
func (d *Driver) deleteOrphanedInternalCloneSourceSnapshots(ctx context.Context, snapshots []*truenas.Snapshot) ([]*truenas.Snapshot, error) {
	remaining := make([]*truenas.Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if !isInternalCloneSourceSnapshot(snap) {
			remaining = append(remaining, snap)
			continue
		}
		if err := d.truenasClient.SnapshotDelete(ctx, snap.ID, true, false); err != nil && !truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.Internal, "failed to delete orphaned internal snapshot %s: %v", snap.ID, err)
		}
		klog.Infof("Deleted orphaned internal clone-source snapshot %s", snap.ID)
	}
	return remaining, nil
}

func isCSISnapshot(snap *truenas.Snapshot) bool {
	if snap == nil || isSnapshotTombstone(snap) {
		return false
	}
	_, hasCSIName := snap.UserProperties[PropCSISnapshotName]
	if snap.ResourceQuery {
		// The 26.0 API cannot distinguish local from inherited values.
		// csi_snapshot_name is snapshot-only, while managed_resource inherits
		// from CSI volume datasets into manual snapshots.
		return hasCSIName
	}
	// Legacy read path: managed_resource inherits from CSI volume datasets into
	// every snapshot of them — task-created scheduled snapshots (GF2/E2) and
	// manual ones alike — and here the API DOES report the source. An inherited
	// or foreign source proves the value was not written on this snapshot at
	// CreateSnapshot, so it must not classify the snapshot as CSI-created (the
	// rule snapshotMatchesRetainedTombstoneIdentity already applies). Trusting
	// it wedged DeleteVolume behind the dependent-snapshot guard for every
	// scheduled snapshot on a legacy read.
	managedProp := snap.UserProperties[PropManagedResource]
	managed := managedProp.Value == "true" &&
		(managedProp.Source == "" || isLocalUserPropertySource(managedProp.Source))
	return managed || hasCSIName
}

// snapshotShortName returns the snapshot's short name, falling back to the
// name encoded in its full ID.
func snapshotShortName(snap *truenas.Snapshot) string {
	if snap == nil {
		return ""
	}
	if snap.Name != "" {
		return snap.Name
	}
	if extracted, ok := extractSnapshotName(snap.ID); ok {
		return extracted
	}
	return ""
}

// snapshotCarriesLiveCSIIdentity reports that a snapshot's recorded CSI name
// sanitizes to its own CURRENT short name — i.e. it is a live CSI snapshot
// under exactly that name (created as such), not a renamed driver tombstone.
// Driver tombstones fail this: their retained csi_snapshot_name (the 26.0
// property strip is a silent no-op) records the PRE-rename name, which no
// longer matches the tombstone-shaped current name.
func snapshotCarriesLiveCSIIdentity(snap *truenas.Snapshot) bool {
	if snap == nil {
		return false
	}
	property, ok := snap.UserProperties[PropCSISnapshotName]
	if !ok || property.Value == "" || property.Value == "-" {
		return false
	}
	return sanitizeVolumeID(property.Value) == snapshotShortName(snap)
}

func isSnapshotTombstone(snap *truenas.Snapshot) bool {
	if snap == nil {
		return false
	}
	name := snapshotShortName(snap)
	marker := strings.LastIndex(name, snapshotTombstoneMarker)
	if marker <= 0 {
		return false
	}
	if _, err := strconv.ParseUint(name[marker+len(snapshotTombstoneMarker):], 10, 64); err != nil {
		return false
	}
	// Identity beats name shape: a legitimate CSI snapshot whose requested name
	// merely ends in -csi-deleted-<n> is a CSI snapshot with a full lifecycle
	// (deletable, listable, blocking), never a tombstone. Real driver tombstones
	// released their CSI name at rename, so their retained identity (if any) no
	// longer matches and they still classify as tombstones.
	return !snapshotCarriesLiveCSIIdentity(snap)
}

// ControllerGetCapabilities returns the capabilities of the controller.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Info("ControllerGetCapabilities called")

	caps := []*csi.ControllerServiceCapability{
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_GET_VOLUME,
				},
			},
		},
		{
			// Meaningful only alongside GET_VOLUME: ControllerGetVolume populates
			// Volume.Status.VolumeCondition from the dataset's already-returned
			// user properties (no extra API call).
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
				},
			},
		},
		{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			},
		},
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

// CreateVolume creates a new volume.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (_ *csi.CreateVolumeResponse, operationErr error) {
	detachedCopyJobID := truenas.UnknownReplicationJobID
	defer func() {
		if operationErr != nil && detachedCopyJobID != truenas.UnknownReplicationJobID {
			d.abortReplicationJobBestEffort(ctx, detachedCopyJobID, replicationJobReasonCreateVolumeFailed)
		}
		d.recordOperationFailureEvent(createVolumeEventRef(req), EventReasonVolumeCreateFailed, "CreateVolume", operationErr)
	}()
	start := time.Now()
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	volumeID := sanitizeVolumeID(name)
	datasetName, err := d.datasetForID(volumeID)
	if err != nil {
		return nil, err
	}
	if source := req.GetVolumeContentSource(); source != nil {
		if snapshot := source.GetSnapshot(); snapshot != nil {
			if _, validationErr := d.datasetForID(snapshot.GetSnapshotId()); validationErr != nil {
				return nil, validationErr
			}
		} else if volume := source.GetVolume(); volume != nil {
			if _, validationErr := d.datasetForID(volume.GetVolumeId()); validationErr != nil {
				return nil, validationErr
			}
		}
	}

	// Enhanced logging for debugging volsync and backup scenarios
	contentSourceInfo := "none"
	if src := req.GetVolumeContentSource(); src != nil {
		if snap := src.GetSnapshot(); snap != nil {
			contentSourceInfo = fmt.Sprintf("snapshot:%s", snap.GetSnapshotId())
		} else if vol := src.GetVolume(); vol != nil {
			contentSourceInfo = fmt.Sprintf("volume:%s", vol.GetVolumeId())
		}
	}
	klog.Infof("CreateVolume: name=%s, contentSource=%s", name, contentSourceInfo)

	// Log accessibility requirements for debugging (topology awareness)
	if reqs := req.GetAccessibilityRequirements(); reqs != nil {
		reqTopologies := make([]string, 0)
		for _, topo := range reqs.GetRequisite() {
			reqTopologies = append(reqTopologies, fmt.Sprintf("%v", topo.GetSegments()))
		}
		prefTopologies := make([]string, 0)
		for _, topo := range reqs.GetPreferred() {
			prefTopologies = append(prefTopologies, fmt.Sprintf("%v", topo.GetSegments()))
		}
		klog.V(4).Infof("CreateVolume: accessibility_requirements requisite=%v preferred=%v",
			reqTopologies, prefTopologies)
	}

	// Lock on the sanitized volume ID so all operations use the same key space.
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	vp, err := d.validateCreateVolumeRequest(req, volumeID)
	if err != nil {
		return nil, err
	}
	capacityBytes := vp.capacityBytes
	shareType, detached := vp.shareType, vp.detached

	// iSCSI CHAP is strictly opt-in. When this StorageClass opts in, ensure the
	// shared backend auth peer once (cached per tag) and thread the resolution to
	// the share builder and volume-context builder via the request context. A
	// deployment that never enables CHAP skips this entirely, so the non-CHAP
	// golden RTT counts are unaffected.
	if shareType == ShareTypeISCSI && d.chapEnabledForCreate(req.GetParameters(), req.GetSecrets()) {
		chapResolution, chapErr := d.EnsureISCSIAuthPeer(ctx, req.GetSecrets())
		if chapErr != nil {
			return nil, chapErr
		}
		if chapResolution.Rotated {
			// The backend peer's secret was re-keyed in place. Surface a redacted
			// Event (no credential) so operators can confirm the rotation applied.
			d.recordNormalEvent(createVolumeEventRef(req), EventReasonISCSICHAPRotated,
				fmt.Sprintf("Rotated iSCSI CHAP credential for auth tag %d", chapResolution.Peer.Tag))
		}
		ctx = withISCSIChAPResolution(ctx, chapResolution)
	}

	// Block-protocol tuning (GF-Sprint 4) is strictly opt-in per StorageClass.
	// Resolve and validate the knobs once and thread them to the share builder
	// via the request context, mirroring the CHAP resolution. A StorageClass that
	// sets none of these resolves to nil and provisions byte-identically to
	// pre-GF4. NVMe-oF port performance fields are install-wide (shared port) and
	// are rejected here so a per-SC value cannot mutate a shared object (R-4).
	if shareType == ShareTypeISCSI || shareType == ShareTypeNVMeoF {
		if portErr := validateNoNVMeoFPortParams(req.GetParameters()); portErr != nil {
			return nil, portErr
		}
		opts, optsErr := resolveBlockOpts(req.GetParameters(), volumeID)
		if optsErr != nil {
			return nil, optsErr
		}
		ctx = withBlockOpts(ctx, opts)
	}

	// Resolve the per-StorageClass NFS export overrides. A strict no-op for a
	// class that sets none of the new parameters. The class's PINNED NFS VERSION
	// is preflighted further down, on the create arm only — see there.
	if shareType == ShareTypeNFS {
		nfsOptions, nfsErr := d.parseNFSShareOptions(req.GetParameters())
		if nfsErr != nil {
			return nil, nfsErr
		}
		ctx = withNFSShareOptions(ctx, nfsOptions)

		aclOptions, aclErr := parseNFSACLOptions(req.GetParameters())
		if aclErr != nil {
			return nil, aclErr
		}
		// H3: aclmode/acltype are stamped in the pool.dataset.create payload, which
		// a content-source volume never issues. Refuse an explicit nfsACLMode here,
		// BEFORE any mutation, rather than materializing a volume whose chmod
		// behavior is its origin's while the events claim the requested mode.
		if aclErr := validateNFSACLContentSource(aclOptions, req.GetVolumeContentSource()); aclErr != nil {
			return nil, aclErr
		}
		ctx = withNFSACLOptions(ctx, aclOptions)
	}

	// Curated ZFS performance class. Validated here (pure, no backend I/O) so a
	// typo is InvalidArgument before anything is created; the preset itself is
	// resolved and validated against the live choice lists inside createDataset.
	performanceClass, performanceErr := zfsPerformanceClassFromParams(req.GetParameters())
	if performanceErr != nil {
		return nil, performanceErr
	}
	ctx = withZFSPerformanceClass(ctx, performanceClass)

	// Check if volume already exists
	existingDS, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err == nil && existingDS != nil {
		return d.createVolumeExisting(ctx, req, existingDS, datasetName, name, volumeID, vp)
	}
	if err != nil && !truenas.IsNotFoundError(err) {
		return nil, status.Errorf(codes.Internal, "failed to check whether volume exists: %v", err)
	}

	// Opt-in NFS version preflight, deliberately BELOW the already-exists check.
	// CSI requires CreateVolume to be idempotent for an identical request, and a
	// replay for a volume this driver already provisioned must not be turned into
	// a hard FailedPrecondition by a GLOBAL, server-side protocol setting that
	// says nothing about that existing dataset. Above the check it gated replays
	// too, so an appliance missing NFSV4 failed every retry of an
	// already-successful volume. createVolumeExisting has returned by here, so
	// reaching this point means the dataset does not exist yet — a fresh create
	// or content-source materialization, exactly the case the preflight is meant
	// to stop before anything is provisioned. Known scope limit: a create whose
	// dataset landed but which crashed before finishing resumes through
	// createVolumeExisting and therefore skips the preflight; that volume was
	// already materialized, so refusing it would strand storage, not save it.
	if shareType == ShareTypeNFS {
		if preflightErr := d.preflightNFSVersion(ctx, mountFlagsFromCapabilities(req.GetVolumeCapabilities())); preflightErr != nil {
			return nil, preflightErr
		}
	}
	freshlyCreated := false

	// Handle volume content source (clone from snapshot or volume)
	var contentSource *csi.VolumeContentSource
	var createdDS *truenas.Dataset
	// contentSourceGeometry is the SOURCE's resolved block geometry (nil for NFS
	// and for a fresh, source-less create). Each clone branch already folds it
	// into its own atomic content-source write; it is also folded into the FATAL
	// managed-property update below so the record of what the cloned data is
	// addressed through is durable-or-rolled-back with the rest of provisioning,
	// exactly like the CHAP linkage. Adding keys to a map that is already written
	// costs no extra round trip.
	var contentSourceGeometry map[string]string
	zvolReady := false
	// performanceClassApplied is the ONLY authority for stamping
	// PropZFSPerformanceClass. The stamp asserts "this dataset was CREATED with
	// the curated class's properties", and createDataset is the only place they
	// are ever applied. A clone / snapshot restore inherits the ORIGIN dataset's
	// geometry and accepts no property payload, so stamping there would be a
	// silent correctness lie in both directions: a later replay would be
	// false-accepted against a class the volume does not carry, or false-rejected
	// with "volblocksize is fixed when the dataset is created" for a property the
	// driver never set on this dataset.
	performanceClassApplied := false
	if req.GetVolumeContentSource() != nil {
		contentSource = req.GetVolumeContentSource()
		clonedDS, resolvedGeometry, srcErr := d.handleVolumeContentSource(
			ctx, datasetName, name, contentSource, capacityBytes, shareType, detached, &detachedCopyJobID,
		)
		if srcErr != nil {
			return nil, srcErr
		}
		// Carry the ONE resolved record to the share builder. The destination is
		// also stamped with it below, but the stamp cannot express the difference
		// between "the source provably held no block-addressed data" and "the
		// source's geometry was lost" — and that difference is exactly what decides
		// whether the controller default may be applied. The record can.
		ctx = withResolvedGeometry(ctx, resolvedGeometry)
		sourceGeometry := resolvedGeometry.props()
		contentSourceGeometry = sourceGeometry
		if detached {
			// Detached snapshot copies do NOT fold the ownership stamp into their
			// content-source write, so stamp it here before any share object exists.
			// Clone/replication APIs cannot stamp properties atomically; the initial
			// absence check plus a successful (not AlreadyExists) copy response is the
			// creation proof.
			//
			// GF-4 round 4, mechanism (3): the SOURCE's real geometry rides along in
			// this same write. A detached copy carries the source's byte layout just
			// like a clone does, so its extent must be created from the source's
			// geometry rather than the controller default. Adding keys to a map that
			// is already written costs no extra round trip.
			detachedStamp := map[string]string{
				PropDriverInstanceID: d.driverInstanceID(),
			}
			for key, value := range sourceGeometry {
				detachedStamp[key] = value
			}
			verifiedClone, ownerErr := d.setAndVerifyDatasetUserProperties(ctx, datasetName, detachedStamp)
			if ownerErr != nil {
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, status.Errorf(codes.Internal, "failed to stamp and verify cloned volume ownership: %v", ownerErr)
			}
			createdDS = verifiedClone
		} else {
			// Sprint 3 (L2a): the snapshot-clone and volume-clone paths folded the
			// ownership stamp into their atomic content-source write, so clonedDS
			// already carries durable ownership and no separate stamp is needed.
			createdDS = clonedDS
		}
		// Ownership is durable; the in-flight marker has served its purpose.
		// Best-effort removal — the reconciler sweep retires leftovers.
		d.deleteInflightMarker(ctx, volumeID)
		zvolReady = true
		// Scrub the user properties this volume inherited from its content source
		// (ZFS copies the source's user properties into a clone, and a detached
		// replication copy carries them over as LOCAL values). Two families:
		// backend share-object IDs belonging to foreign protocols — a stale
		// inherited ID would make ensureShareExists validate this volume against
		// the SOURCE volume's share objects — and the curated performance-class
		// stamp, which would otherwise assert curated geometry that was never
		// applied here. Best-effort and a SEPARATE pool.dataset.update from the
		// authoritative ownership stamp above (cleanup, not provenance).
		d.scrubInheritedCloneProperties(ctx, createdDS, datasetName, shareType)
		if performanceClass != "" {
			// H1: say so out loud. A clone silently carrying its origin's geometry
			// under a different class name is exactly the correctness lie the
			// immutability guard exists to prevent.
			message := fmt.Sprintf(
				"StorageClass parameter %s=%q was IGNORED for volume %s: the volume is provisioned from a %s content source, "+
					"and a ZFS clone/restore inherits the origin dataset's geometry (recordsize, volblocksize, compression, ...) — "+
					"the curated properties cannot be applied and the volume is NOT stamped with the class "+
					"(any class stamp copied from the source is scrubbed, and the class guard never treats a content-source "+
					"volume's stamp as authoritative). "+
					"Provision an empty volume with this class and copy the data in if the curated geometry is required.",
				zfsPerformanceClassParam, performanceClass, volumeID, contentSourceKind(contentSource))
			klog.Warning(message)
			d.recordWarningEvent(createVolumeEventRef(req), EventReasonZFSPerformanceClassIgnored, message)
		}
	} else {
		// Create new dataset
		var createErr error
		createdDS, createErr = d.createDataset(ctx, datasetName, capacityBytes, shareType)
		if createErr != nil {
			return nil, createErr
		}
		freshlyCreated = createdDS.CreatedByCall
		zvolReady = freshlyCreated
		// createDataset is the one and only place applyPerformanceClassProperties
		// runs, so this is the one and only place the stamp becomes truthful.
		performanceClassApplied = performanceClass != ""
	}

	// Mark as managed and successful. NFS folds these stamps into the share-ID
	// update inside createShareWithOptions (one pool.dataset.update on the same
	// side of the NFSShareCreate boundary); block protocols stamp them in a
	// separate update below because their in-share ID write is non-fatal.
	volumeProperties := map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropProvisionSuccess: "true",
		PropCSIVolumeName:    name,
	}
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		volumeProperties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
	// Record the curated class this volume was created with so a later
	// StorageClass edit can be checked against the create-only property rules
	// instead of silently pretending the volume was retuned. Folded into the
	// existing property update, so it costs no extra round trip; absent unless
	// the class was requested AND actually applied (never on clone/restore —
	// see performanceClassApplied above).
	if performanceClassApplied {
		volumeProperties[PropZFSPerformanceClass] = performanceClass
	}
	// Fold the durable CHAP auth linkage into the FATAL managed-property update
	// below so it is stamped-or-rolled-back with the rest of provisioning (X1): a
	// fence pass reconstructs authmethod+auth purely from PropISCSIAuthTag +
	// PropISCSIAuthMode, so a missing stamp would silently downgrade the target to
	// authmethod=NONE. The immutable mode is stored here (never re-derived from the
	// mutable global flag). Only the create path reaches this; existing volumes are
	// policy-guarded in createVolumeExisting and never re-stamped. For clones this
	// re-writes the CHAP policy the clone fold already stamped atomically with
	// ownership (Sprint 6 H1) — idempotent, same values via iscsiCHAPPolicyProps.
	for key, value := range iscsiCHAPPolicyProps(ctx, shareType) {
		volumeProperties[key] = value
	}
	// Fold the resolved block-protocol tuning into the SAME fatal update, for the
	// same reason: every rebuild path reconstructs the volume's geometry and
	// safety knobs purely from these properties, so a missing stamp silently
	// re-creates the extent at the controller-default blocksize over data laid
	// out for a different one, and drops the stable serial / read-only /
	// insecure_tpc / target auth_networks / avail_threshold / qid_max /
	// pi_enable. Adding keys to a map that is already written costs NO extra
	// round trip. nil (adds nothing, byte-identical write) for NFS and for any
	// StorageClass that opts into no block tuning.
	for key, value := range blockOptsProps(ctx, shareType) {
		volumeProperties[key] = value
	}
	// GF-4 round 4: and the geometry the CLONED DATA is actually addressed
	// through, for the same reason (see contentSourceGeometry above). nil for a
	// fresh create — a zvol this call just made holds nothing, so the controller
	// default is the honest answer there and the share builder stamps whatever the
	// extent really came out at.
	for key, value := range contentSourceGeometry {
		volumeProperties[key] = value
	}

	// Create share (NFS, iSCSI, or NVMe-oF). A definitely fresh DatasetCreate
	// result and the clone readiness path do not need another zvol poll.
	if shareErr := d.createShareWithOptions(ctx, createdDS, datasetName, name, shareType, freshlyCreated, zvolReady, volumeProperties); shareErr != nil {
		// Cleanup on failure
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after share creation failure: %v", delErr)
		}
		return nil, shareErr
	}

	// Block protocols (iSCSI/NVMe-oF) stamp their resource IDs non-fatally inside
	// createShareWithOptions, so the managed/ownership/provision/name stamps still
	// happen here as a separate, fatal update. NFS already stamped them together
	// with the share ID and skips this round trip.
	if shareType != ShareTypeNFS {
		if waitErr := d.setDatasetUserProperties(ctx, createdDS, datasetName, volumeProperties); waitErr != nil {
			// Property setting failed - clean up the share and dataset to avoid orphaned resources
			// The next CreateVolume call will start fresh
			klog.Errorf("Failed to set properties for volume %s: %v - cleaning up orphaned resources", volumeID, waitErr)
			if delErr := d.deleteShare(ctx, createdDS, datasetName, shareType); delErr != nil {
				klog.Warningf("Failed to cleanup share after property failure: %v", delErr)
			}
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); delErr != nil {
				klog.Warningf("Failed to cleanup dataset after property failure: %v", delErr)
			}
			return nil, status.Errorf(codes.Internal, "failed to set volume properties: %v", waitErr)
		}
	}

	// Create the driver-owned periodic-snapshot task (GF2/E2) now that the dataset
	// exists and is owned/stamped for every path (fresh, clone, detached; NFS and
	// block). A nil spec (the default) is a no-op; a task failure is non-fatal.
	d.ensureSnapshotTask(ctx, createdDS, datasetName, volumeID, vp.snapshotTask, req)

	// Apply the requested NFSv4 ACL after the dataset, its ownership stamps and
	// its export all exist. Strict no-op unless a StorageClass asked for one, and
	// best-effort by design: it never blocks a Bound PVC (risk R7).
	if shareType == ShareTypeNFS {
		// contentSource != nil means the dataset was materialized by a clone /
		// replication copy, which accepts no property payload: acltype and aclmode
		// are the ORIGIN's, not the ones this request asked for. Tell
		// applyNFSVolumeACL so its log/event report what was actually applied (H3).
		d.applyNFSVolumeACL(ctx, createdDS, datasetName, createVolumeEventRef(req), contentSource)
	}

	// Get volume context for response
	volumeContext, err := d.getVolumeContext(ctx, createdDS, datasetName, shareType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
	}

	klog.Infof("CreateVolume completed: volume=%s, shareType=%s, contentSource=%s, elapsed=%v",
		volumeID, shareType, contentSourceInfo, time.Since(start))

	if contentSource != nil {
		if actualCapacity := d.getDatasetCapacity(createdDS); actualCapacity > 0 {
			capacityBytes = actualCapacity
		}
	}

	volume := &csi.Volume{
		VolumeId:      volumeID,
		CapacityBytes: capacityBytes,
		VolumeContext: volumeContext,
		ContentSource: contentSource,
	}

	// Add accessible topology if topology awareness is enabled
	// For a single TrueNAS backend, volumes are accessible from nodes in any configured topology
	if d.config.Node.Topology.Enabled {
		accessibleTopo := d.getAccessibleTopology()
		if accessibleTopo != nil {
			volume.AccessibleTopology = accessibleTopo
		}
	}

	return &csi.CreateVolumeResponse{Volume: volume}, nil
}

func (d *Driver) parentDatasetName() string {
	return strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/")
}

// createVolumeParams holds the validated, request-derived values CreateVolume
// needs after the pure (no backend I/O) validation phase.
type createVolumeParams struct {
	capacityBytes int64
	requiredBytes int64
	limitBytes    int64
	shareType     ShareType
	detached      bool
	// snapshotTask is the resolved, validated driver-managed periodic-snapshot
	// configuration (GF2/E2), or nil when the volume is not scheduled.
	snapshotTask *snapshotTaskSpec
}

// validateCreateVolumeRequest performs the pure (no backend I/O) validation of a
// CreateVolume request: capacity-range clamping and bounds, protocol-parameter
// validation against the enabled protocol set, NFS/raw-block and access-mode
// compatibility, and clone-vs-detached resolution. Extracted verbatim from
// CreateVolume (Batch 18 R7).
func (d *Driver) validateCreateVolumeRequest(req *csi.CreateVolumeRequest, volumeID string) (createVolumeParams, error) {
	// Calculate and validate capacity
	capacityBytes := int64(0)
	requiredBytes := int64(0)
	limitBytes := int64(0)
	if req.GetCapacityRange() != nil {
		requiredBytes = req.GetCapacityRange().GetRequiredBytes()
		limitBytes = req.GetCapacityRange().GetLimitBytes()
		capacityBytes = requiredBytes
	}
	if capacityBytes == 0 {
		capacityBytes = 1024 * 1024 * 1024 // Default 1GiB
	}
	// Validate the applied capacity against the limit. This covers both an
	// explicit required_bytes that exceeds limit_bytes and the case where
	// required_bytes is omitted (0): the 1GiB default is then applied and must
	// itself respect a caller-supplied limit below that default.
	if limitBytes > 0 && capacityBytes > limitBytes {
		return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
			"required capacity (%d bytes) exceeds limit (%d bytes)", capacityBytes, limitBytes)
	}

	// Minimum capacity validation (at least 1MiB to avoid edge cases). The
	// protocol-specific refquota floor is applied below, once the share type is
	// known.
	const minCapacity = 1024 * 1024 // 1 MiB
	if capacityBytes < minCapacity {
		return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
			"requested capacity (%d bytes) is below minimum (%d bytes)", capacityBytes, minCapacity)
	}

	// Maximum capacity sanity check (1PiB should be more than enough)
	const maxCapacity = 1024 * 1024 * 1024 * 1024 * 1024 // 1 PiB
	if capacityBytes > maxCapacity {
		return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
			"requested capacity (%d bytes) exceeds maximum (%d bytes)", capacityBytes, maxCapacity)
	}

	// A unified multi-protocol deployment cannot infer whether an omitted
	// StorageClass parameter meant NFS, iSCSI, or NVMe-oF. Keep the historical
	// driver-name fallback only for instances that serve at most one protocol.
	params := req.GetParameters()
	protocol, hasProtocol := params["protocol"]
	if !hasProtocol {
		if d.config.enabledShareTypeCount() > 1 {
			return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %q is required when multiple storage protocols are enabled; valid options are: %s",
				"protocol", strings.Join(ValidShareTypeStrings(), ", "))
		}
	} else {
		explicitShareType := ShareType(strings.ToLower(strings.TrimSpace(protocol)))
		if !explicitShareType.IsValid() {
			return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
				"invalid protocol %q; valid options are: %s",
				protocol, strings.Join(ValidShareTypeStrings(), ", "))
		}
		// Reject a syntactically valid protocol that this instance does not
		// serve. Without this, an nfs+iscsi install accepts nvmeof here and
		// only fails deep in the share-creation path. Legacy configs with no
		// enabled markers (enabledShareTypeStrings empty) keep the historical
		// driver-name fallback and are not gated here.
		if enabled := d.config.enabledShareTypeStrings(); len(enabled) > 0 && !d.config.isShareTypeEnabled(explicitShareType) {
			return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %q value %q is not enabled on this driver; enabled options are: %s",
				"protocol", protocol, strings.Join(enabled, ", "))
		}
	}
	shareType := d.config.GetShareType(params)
	klog.Infof("CreateVolume: using share type %s for volume %s", shareType, volumeID)
	if shareType == ShareTypeNFS {
		for _, capability := range req.GetVolumeCapabilities() {
			if capability.GetBlock() != nil {
				return createVolumeParams{}, status.Error(codes.InvalidArgument, "raw block volume capability is incompatible with NFS protocol")
			}
		}
	}

	// TrueNAS floors `refquota` at 1 GiB and reports a violation as the same
	// unqualified -32602 "Invalid params" as everything else, so a 64 MiB PVC
	// failed with nothing to act on (live drill, 2026-08-02). Only a volume whose
	// size is expressed AS a refquota is affected: NFS filesystems with
	// zfs.datasetEnableQuotas. A zvol is sized by volsize and a quota-less NFS
	// volume writes no refquota at all; neither is gated here.
	//
	// This runs before the already-exists check, which is safe precisely because
	// the backend enforcing the floor is the same one that would have had to
	// accept such a volume: a sub-1GiB refquota volume cannot exist to have its
	// CreateVolume replayed.
	if shareType == ShareTypeNFS && d.config.ZFS.DatasetEnableQuotas && capacityBytes < truenasMinRefquotaBytes {
		return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
			"requested capacity (%d bytes) is below the 1 GiB minimum TrueNAS enforces for a dataset refquota, which is how this "+
				"volume's size is applied (protocol nfs with zfs.datasetEnableQuotas). Request at least %d bytes, or disable "+
				"zfs.datasetEnableQuotas to provision quota-less NFS volumes",
			capacityBytes, int64(truenasMinRefquotaBytes))
	}

	// Validate access mode against protocol
	// RWX (ReadWriteMany) is only supported for NFS volumes
	if mode, ok := multiNodeAccessMode(req.GetVolumeCapabilities()); ok && !shareType.SupportsMultiNode() {
		return createVolumeParams{}, status.Errorf(codes.InvalidArgument,
			"access mode %s requires NFS protocol, but %s was requested",
			mode.String(), shareType)
	}

	// Resolve clone-vs-detached for a snapshot content source. The StorageClass
	// parameter opts a class in or out; otherwise the global default applies. This
	// single resolved value drives every content-source decision below (existing
	// remnant recovery, in-flight marker mode, and the clone/copy branch) so a
	// retry stays consistent with the class that made the request.
	detached, err := d.snapshotRestoreDetached(params)
	if err != nil {
		return createVolumeParams{}, err
	}
	// Resolve the driver-managed periodic-snapshot configuration (GF2/E2). A nil
	// spec means the volume is not scheduled (the default-off case); a malformed
	// schedule/retention is an InvalidArgument so a bad StorageClass parameter
	// fails fast rather than provisioning without PITR.
	snapshotTask, err := d.resolveSnapshotTaskSpec(params, volumeID)
	if err != nil {
		return createVolumeParams{}, err
	}
	return createVolumeParams{
		capacityBytes: capacityBytes,
		requiredBytes: requiredBytes,
		limitBytes:    limitBytes,
		shareType:     shareType,
		detached:      detached,
		snapshotTask:  snapshotTask,
	}, nil
}

// createVolumeExisting handles the already-exists arm of CreateVolume: the
// self-contained path taken when the target dataset already exists. It
// validates protocol/content-source/ownership compatibility, self-heals an
// interrupted content-source remnant, ensures properties and the share exist,
// and returns the idempotent CreateVolume response. Extracted verbatim from
// CreateVolume (Batch 18 R7).
func (d *Driver) createVolumeExisting(ctx context.Context, req *csi.CreateVolumeRequest, existingDS *truenas.Dataset, datasetName, name, volumeID string, vp createVolumeParams) (*csi.CreateVolumeResponse, error) {
	capacityBytes, requiredBytes, limitBytes := vp.capacityBytes, vp.requiredBytes, vp.limitBytes
	shareType, detached := vp.shareType, vp.detached
	var err error
	// Volume exists - check and ensure properties are set
	klog.Infof("Volume %s already exists", volumeID)
	if shareType.IsBlockProtocol() && existingDS.Type == "FILESYSTEM" {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %s already exists as a filesystem, incompatible with requested %s protocol",
			volumeID, shareType)
	}
	if shareType == ShareTypeNFS && existingDS.Type == "VOLUME" {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %s already exists as a block volume, incompatible with requested NFS protocol",
			volumeID)
	}
	if storedBlockProtocol(existingDS, ShareTypeISCSI) && shareType != ShareTypeISCSI {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %s exists with protocol %s, requested %s", volumeID, ShareTypeISCSI, shareType)
	}
	if storedBlockProtocol(existingDS, ShareTypeNVMeoF) && shareType != ShareTypeNVMeoF {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %s exists with protocol %s, requested %s", volumeID, ShareTypeNVMeoF, shareType)
	}
	// Crash self-healing for a content-source create that built the destination
	// (clone or detached copy) but crashed before ownership was stamped. Such a
	// remnant otherwise wedges the PVC permanently (terminal AlreadyExists below)
	// and leaks invisibly (no managed_resource for the orphan reconciler). This
	// runs before the content-source and ownership gates because an unstamped
	// clone has no local content-source properties yet and would trip those
	// gates first; recovery itself validates source/protocol/capacity
	// compatibility against the marker and the remnant before any mutation. It
	// is a strict no-op for any dataset without a matching in-flight marker.
	if source := req.GetVolumeContentSource(); source != nil {
		recovered, action, recoverErr := d.recoverInFlightContentSourceRemnant(
			ctx, existingDS, datasetName, name, source, capacityBytes, limitBytes, shareType, detached,
		)
		if recoverErr != nil {
			return nil, recoverErr
		}
		switch action {
		case remnantActionResume:
			// The remnant is now stamped and its content-source flow completed;
			// fall through so the normal existing-dataset tail (capacity checks,
			// idempotent share creation, response) finishes the volume.
			existingDS = recovered
		case remnantActionDestroy:
			return nil, status.Errorf(codes.Aborted,
				"destroyed unstamped interrupted detached-copy remnant %s; retry CreateVolume to recreate it cleanly", datasetName)
		}
	}
	storedContentSource := volumeContentSourceFromDataset(existingDS)
	requestedContentSource := req.GetVolumeContentSource()
	storedSourceIsDurable := datasetHasDurableContentSource(existingDS)
	if (storedSourceIsDurable && storedContentSource == nil) ||
		(storedContentSource == nil) != (requestedContentSource == nil) ||
		(storedContentSource != nil && !volumeContentSourcesEqual(storedContentSource, requestedContentSource)) {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %s already exists with content source %s, incompatible with requested %s",
			volumeID, describeDatasetContentSource(existingDS), describeVolumeContentSource(requestedContentSource))
	}
	// A present owner is authoritative and must match locally. The v1.2.22
	// installed base predates this stamp, so an actually absent owner may be
	// backfilled only when both older local managed markers identify the same
	// CSI volume. Empty, inherited, or different owner values are present-and-
	// different and are never auto-adopted.
	owner, ownerPresent := datasetUserPropertyProjection(existingDS, PropDriverInstanceID)
	switch {
	case ownerPresent:
		if !datasetHasLocalUserProperty(existingDS, PropDriverInstanceID, d.driverInstanceID()) {
			return nil, status.Errorf(codes.AlreadyExists,
				"dataset %s already exists but ownership property %s is %q, expected a local value of %q",
				datasetName, PropDriverInstanceID, owner.Value, d.driverInstanceID())
		}
	case datasetHasLocalUserProperty(existingDS, PropManagedResource, "true") &&
		datasetHasLocalUserProperty(existingDS, PropCSIVolumeName, name):
		verified, stampErr := d.setAndVerifyDatasetUserProperties(ctx, datasetName, map[string]string{
			PropDriverInstanceID: d.driverInstanceID(),
		})
		if stampErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to backfill legacy volume ownership: %v", stampErr)
		}
		existingDS = verified
		klog.Infof("Backfilled ownership stamp on legacy managed dataset %s", datasetName)
	default:
		return nil, status.Errorf(codes.AlreadyExists,
			"dataset %s already exists without ownership property %s and does not have matching local legacy CSI markers",
			datasetName, PropDriverInstanceID)
	}
	if snapshot := req.GetVolumeContentSource().GetSnapshot(); detached && snapshot != nil {
		existingDS, err = d.prepareDetachedSnapshotCopy(
			ctx, datasetName, existingDS, name, snapshot.GetSnapshotId(), snapshot.GetSnapshotId(), capacityBytes, shareType,
		)
		if err != nil {
			return nil, err
		}
	}

	// IMMUTABILITY GUARD (risk R1). volblocksize is immutable in ZFS itself, so a
	// StorageClass that now names a curated class with a different zvol geometry
	// CANNOT be satisfied in place. Refuse loudly rather than let an operator
	// believe an existing volume was retuned. Every other curated property is
	// live-tunable, and for those the guard warns instead of refusing (the driver
	// still does not retune existing datasets).
	if requestedClass := zfsPerformanceClassFromContext(ctx); requestedClass != "" {
		storedClass := datasetUserProperty(existingDS, PropZFSPerformanceClass)
		if storedClass == "-" {
			storedClass = ""
		}
		// H1: a content-source volume's class stamp is NEVER authoritative. The
		// curated properties are applied exactly once, inside createDataset, and a
		// clone/restore does not go through it — so any class property such a
		// volume carries was COPIED from its origin (a ZFS clone inherits the
		// source's user properties with the origin snapshot as their source; a
		// detached replication copy reproduces them as local values). Feeding a
		// copied stamp to the guard produces both failure directions: a false
		// accept against geometry the volume does not have, and — because an
		// identical replay of a SUCCESSFUL CreateVolume would be compared against
		// the origin's class instead of the requested one — a FailedPrecondition
		// on an exact request replay, i.e. a CSI idempotency violation.
		//
		// So: treat the volume as unstamped (which is what it honestly is) and
		// scrub the copied stamp so the on-disk record stops asserting it. The
		// scrub is best-effort; the ignore above is what makes behavior correct.
		if datasetHasDurableContentSource(existingDS) {
			if storedClass != "" {
				klog.Warningf("Volume %s carries ZFS performance class stamp %q inherited from its %s content source; "+
					"the curated properties were never applied to this volume, so the stamp is ignored and scrubbed.",
					volumeID, storedClass, describeDatasetContentSource(existingDS))
				d.scrubInheritedCloneProperties(ctx, existingDS, datasetName, shareType)
			}
			klog.Warningf("Volume %s was provisioned from a content source, so ZFS performance class %q was never applied "+
				"(a clone/restore inherits the origin dataset's geometry). The replay is accepted and the volume is left unchanged.",
				volumeID, requestedClass)
			storedClass = ""
		}
		if guardErr := d.guardPerformanceClassChange(ctx, volumeID, storedClass, requestedClass, existingDS.Type); guardErr != nil {
			return nil, guardErr
		}
	}

	existingCapacity := d.getDatasetCapacity(existingDS)
	if existingCapacity > 0 {
		if existingCapacity < requiredBytes {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %s already exists with capacity %d bytes, less than required capacity %d bytes",
				volumeID, existingCapacity, requiredBytes)
		}
		if limitBytes > 0 && existingCapacity > limitBytes {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %s already exists with capacity %d bytes, greater than capacity limit %d bytes",
				volumeID, existingCapacity, limitBytes)
		}
	}

	// Ensure properties are set (idempotent) in one API update.
	propertyUpdates := make(map[string]string, 3)
	if datasetUserProperty(existingDS, PropManagedResource) != "true" {
		propertyUpdates[PropManagedResource] = "true"
	}
	if datasetUserProperty(existingDS, PropProvisionSuccess) != "true" {
		propertyUpdates[PropProvisionSuccess] = "true"
	}
	if datasetUserProperty(existingDS, PropCSIVolumeName) != name {
		propertyUpdates[PropCSIVolumeName] = name
	}
	if detached &&
		req.GetVolumeContentSource().GetSnapshot() != nil &&
		shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		requestedSize := strconv.FormatInt(capacityBytes, 10)
		if datasetUserProperty(existingDS, PropRequestedSizeBytes) != requestedSize {
			propertyUpdates[PropRequestedSizeBytes] = requestedSize
		}
	}
	if waitErr := d.setDatasetUserProperties(ctx, existingDS, datasetName, propertyUpdates); waitErr != nil {
		klog.Errorf("Failed to ensure properties for existing volume %s: %v", volumeID, waitErr)
		return nil, status.Errorf(codes.Internal, "failed to ensure volume properties: %v", waitErr)
	}

	// The stored per-volume CHAP policy is authoritative: an idempotent replay may
	// not convert this volume to/from CHAP or change its tag/mode (X4). Guard
	// BEFORE ensureShareExists so a conflicting replay fails fast and never
	// rebuilds the target's auth groups.
	if shareType == ShareTypeISCSI {
		if guardErr := d.guardExistingISCSICHAPPolicy(ctx, existingDS); guardErr != nil {
			return nil, guardErr
		}
	}

	// GEOMETRY (round 5). A replay whose destination is a clone/restore that
	// carries no geometry record of its own must RE-RESOLVE the source, exactly as
	// the un-replayed path did. Without this, a destination cloned from a source
	// that provably held no block-addressed data is indistinguishable from one
	// whose geometry record was simply lost — the first may take the controller
	// default, the second must never — and the replay wedges on the second reading.
	// Costs nothing for a stamped destination, which is every destination the
	// driver has finished provisioning.
	if shareType.IsBlockProtocol() && req.GetVolumeContentSource() != nil &&
		!stampGeometry(blockOptsFromDataset(existingDS), "").complete() {
		resolved, geometryErr := d.contentSourceBlockGeometry(ctx, datasetName, req.GetVolumeContentSource(), shareType)
		if geometryErr != nil {
			return nil, geometryErr
		}
		ctx = withResolvedGeometry(ctx, resolved)
	}

	// CRITICAL: Ensure share exists for existing volumes (fixes missing iSCSI targets after retries)
	// This handles the case where a previous CreateVolume created the dataset but failed
	// to create the share (e.g., due to timeout, TrueNAS API error, etc.)
	if shareErr := d.ensureShareExists(ctx, existingDS, datasetName, name, shareType, nil); shareErr != nil {
		return nil, shareErr
	}

	// Idempotently re-ensure the driver-owned periodic-snapshot task (GF2/E2) so a
	// retry whose first attempt failed to create the task still converges. A nil
	// spec (the default) is a no-op; FindByDataset adopts an existing task rather
	// than duplicating it.
	d.ensureSnapshotTask(ctx, existingDS, datasetName, volumeID, vp.snapshotTask, req)

	volumeContext, ctxErr := d.getVolumeContext(ctx, existingDS, datasetName, shareType)
	if ctxErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", ctxErr)
	}
	volume := &csi.Volume{
		VolumeId:           volumeID,
		CapacityBytes:      d.getDatasetCapacity(existingDS),
		VolumeContext:      volumeContext,
		ContentSource:      storedContentSource,
		AccessibleTopology: d.getAccessibleTopology(),
	}
	return &csi.CreateVolumeResponse{Volume: volume}, nil
}

func storedBlockProtocol(ds *truenas.Dataset, shareType ShareType) bool {
	var properties []string
	switch shareType {
	case ShareTypeISCSI:
		properties = []string{PropISCSITargetID, PropISCSIExtentID, PropISCSITargetExtentID}
	case ShareTypeNVMeoF:
		properties = []string{PropNVMeoFSubsystemID, PropNVMeoFNamespaceID}
	default:
		return false
	}
	for _, property := range properties {
		if datasetUserPropertyHasValue(ds, property) {
			return true
		}
	}
	return false
}

func (d *Driver) driverInstanceID() string {
	if configured := strings.TrimSpace(d.config.DriverInstanceID); configured != "" {
		return configured
	}
	driverName := strings.TrimSpace(d.name)
	if driverName == "" {
		driverName = strings.TrimSpace(d.config.DriverName)
	}
	return driverName + "@" + strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/")
}

func volumeContentSourceFromDataset(ds *truenas.Dataset) *csi.VolumeContentSource {
	sourceID := datasetUserProperty(ds, PropVolumeContentSourceID)
	if sourceID == "" || sourceID == "-" {
		return nil
	}

	switch datasetUserProperty(ds, PropVolumeContentSourceType) {
	case "snapshot":
		return &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: sourceID},
		}}
	case "volume":
		return &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: sourceID},
		}}
	default:
		return nil
	}
}

func datasetHasDurableContentSource(ds *truenas.Dataset) bool {
	sourceType := datasetUserProperty(ds, PropVolumeContentSourceType)
	sourceID := datasetUserProperty(ds, PropVolumeContentSourceID)
	return (sourceType != "" && sourceType != "-") || (sourceID != "" && sourceID != "-")
}

func describeDatasetContentSource(ds *truenas.Dataset) string {
	if source := volumeContentSourceFromDataset(ds); source != nil {
		return describeVolumeContentSource(source)
	}
	if !datasetHasDurableContentSource(ds) {
		return "none"
	}
	return fmt.Sprintf("invalid(type=%q,id=%q)",
		datasetUserProperty(ds, PropVolumeContentSourceType),
		datasetUserProperty(ds, PropVolumeContentSourceID))
}

func volumeContentSourcesEqual(left, right *csi.VolumeContentSource) bool {
	leftType, leftID, leftOK := volumeContentSourceIdentity(left)
	rightType, rightID, rightOK := volumeContentSourceIdentity(right)
	return leftOK && rightOK && leftType == rightType && leftID == rightID
}

func volumeContentSourceIdentity(source *csi.VolumeContentSource) (sourceType, sourceID string, valid bool) {
	if source == nil {
		return "", "", false
	}
	if snapshot := source.GetSnapshot(); snapshot != nil && snapshot.GetSnapshotId() != "" {
		return "snapshot", snapshot.GetSnapshotId(), true
	}
	if volume := source.GetVolume(); volume != nil && volume.GetVolumeId() != "" {
		return "volume", volume.GetVolumeId(), true
	}
	return "", "", false
}

func describeVolumeContentSource(source *csi.VolumeContentSource) string {
	sourceType, sourceID, ok := volumeContentSourceIdentity(source)
	if !ok {
		return "none"
	}
	return sourceType + ":" + sourceID
}

func datasetOriginSnapshotID(ds *truenas.Dataset) string {
	if ds == nil {
		return ""
	}
	if parsed, ok := ds.Origin.Parsed.(string); ok && parsed != "" && parsed != "-" {
		return parsed
	}
	if ds.Origin.Rawvalue != "" && ds.Origin.Rawvalue != "-" {
		return ds.Origin.Rawvalue
	}
	if value, ok := ds.Origin.Value.(string); ok && value != "" && value != "-" {
		return value
	}
	return ""
}

// DeleteVolume deletes a volume.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (_ *csi.DeleteVolumeResponse, operationErr error) {
	volumeID := req.GetVolumeId()
	defer func() {
		d.recordOperationFailureEvent(volumeEventRef(volumeID), EventReasonVolumeDeleteFailed, "DeleteVolume", operationErr)
	}()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Benign teardown entry: demoted to V(2) so a steady-state VolSync delete hour
	// emits ~0 V(0) controller lines (E4/O21). Delete failures still log at Error.
	klog.V(2).Infof("DeleteVolume: volumeID=%s", volumeID)

	// Lock on volume ID
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName, err := d.datasetForID(volumeID)
	if err != nil {
		return nil, err
	}

	// Check if volume exists (idempotency - return success if already deleted)
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			klog.Infof("Volume %s dataset not found, attempting orphaned resource cleanup", volumeID)
			// Dataset is gone but there may be orphaned NVMe-oF/iSCSI resources.
			// Since we can't check dataset properties, try both protocols using
			// fallback logic that finds resources by name. CSI DeleteVolume must be
			// idempotent — a volume whose dataset is already absent is a successful
			// delete — so a best-effort cleanup failure must not fail the RPC: log
			// it, emit a metric, and let the orphan reconcile sweep the residue.
			if cleanupErr := d.deleteShare(ctx, nil, datasetName, ShareTypeNVMeoF); cleanupErr != nil {
				RecordDeleteVolumeOrphanCleanupFailure(string(ShareTypeNVMeoF))
				klog.Warningf("Failed to cleanup orphaned NVMe-oF resources for %s (orphan reconcile will retry): %v", volumeID, cleanupErr)
			} else {
				klog.Infof("Cleaned up orphaned NVMe-oF resources for %s", volumeID)
			}
			if cleanupErr := d.deleteShare(ctx, nil, datasetName, ShareTypeISCSI); cleanupErr != nil {
				RecordDeleteVolumeOrphanCleanupFailure(string(ShareTypeISCSI))
				klog.Warningf("Failed to cleanup orphaned iSCSI resources for %s (orphan reconcile will retry): %v", volumeID, cleanupErr)
			} else {
				klog.Infof("Cleaned up orphaned iSCSI resources for %s", volumeID)
			}
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to verify volume %s: %v", volumeID, err)
	}

	// Determine share type from stored ZFS properties (most reliable)
	// This handles the case where a single driver handles multiple protocols
	shareType := d.config.GetDriverShareType() // fallback to driver name
	shareTypeKnown := false
	if ds != nil {
		// Check stored properties to determine share type - these were set during CreateVolume
		switch {
		case datasetUserPropertyHasValue(ds, PropNVMeoFSubsystemID):
			shareType = ShareTypeNVMeoF
			shareTypeKnown = true
			klog.V(4).Infof("Detected NVMe-oF volume from stored subsystem ID property")
		case datasetUserPropertyHasValue(ds, PropISCSITargetID):
			shareType = ShareTypeISCSI
			shareTypeKnown = true
			klog.V(4).Infof("Detected iSCSI volume from stored target ID property")
		case datasetUserPropertyHasValue(ds, PropNFSShareID):
			shareType = ShareTypeNFS
			shareTypeKnown = true
			klog.V(4).Infof("Detected NFS volume from stored share ID property")
		default:
			// Fallback to dataset type-based detection
			switch ds.Type {
			case "FILESYSTEM":
				shareType = ShareTypeNFS
				shareTypeKnown = true
			case "VOLUME":
				// For zvol without stored properties, we need to try BOTH protocols
				// to avoid orphaning resources if the driver config doesn't match
				klog.Warningf("Volume %s has no stored protocol properties, will try cleanup for both iSCSI and NVMe-oF", volumeID)
			}
		}
	}

	// IMPORTANT: Check for snapshots that can block dataset deletion BEFORE deleting the
	// share. The share must be deleted before the dataset (extents block zvol
	// deletion), so bailing after share deletion would leave a volume that
	// still exists but is inaccessible, with no path that re-creates its share.
	// The snapshot and dataset-origin checks each cost one query on every delete;
	// the dependency-error fallback after DatasetDelete stays as a second line
	// of defense.
	hasDependentClones, cloneErr := d.truenasClient.DatasetHasDependentClones(ctx, datasetName)
	if cloneErr != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to verify clone dependencies for volume %s before share deletion: %v", volumeID, cloneErr)
	}
	if hasDependentClones {
		klog.Infof("Volume %s has a dependent clone, cannot delete", volumeID)
		return nil, status.Errorf(codes.FailedPrecondition,
			"volume %s has dependent clones that must be deleted first", volumeID)
	}
	snapshots, snapErr := d.truenasClient.SnapshotList(ctx, datasetName)
	if snapErr != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to verify snapshot dependencies for volume %s before share deletion: %v", volumeID, snapErr)
	}
	snapshots, snapErr = d.deleteOrphanedInternalCloneSourceSnapshots(ctx, snapshots)
	if snapErr != nil {
		return nil, snapErr
	}
	for _, snap := range snapshots {
		if snapshotBlocksVolumeDeletion(snap) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"volume %s has dependent snapshots that must be deleted first", volumeID)
		}
	}
	// Foreign (non-CSI) snapshots — e.g. from a TrueNAS periodic-snapshot or
	// replication task on the parent dataset — do not "block" in the CSI sense,
	// but any snapshot still prevents a non-recursive dataset delete. Refuse
	// BEFORE deleting the share (default policy) so we never strand a shareless
	// volume; the post-share-delete fallback stays as a second line of defense
	// for snapshots that appear after this check. Driver-owned scheduled
	// snapshots (GF2/E2) are recognized by their task naming-schema prefix and
	// excluded here: they are deleted WITH the volume, never treated as foreign
	// (R4).
	// Delete the driver-owned periodic-snapshot task (GF2/E2) BEFORE the foreign
	// guard runs (GF2-fix/H2): if the guard refuses, the task must already be gone
	// so it cannot keep minting snapshots that make the next attempt refuse again
	// — a self-sustaining wedge. Best-effort and never fatal; a repeated
	// DeleteVolume simply finds no task.
	//
	// It returns the CORROBORATING task schema (GF2-fix2/B1): the ownership
	// predicate requires a live driver-minted task on this exact dataset, and
	// this is the last moment such a task can be observed. An empty value makes
	// every unlabeled snapshot foreign, so a repeated DeleteVolume that finds no
	// task refuses rather than destroying — the safe direction. A volume whose
	// snapshots the driver really did schedule is deleted on the FIRST attempt,
	// while the task is still there.
	scheduledTaskSchema := d.deleteVolumeSnapshotTask(ctx, ds, datasetName, volumeID)

	// The NAS's civil timezone is the clock a periodic-snapshot task renders its
	// names from, so proving those names needs it (GF2-fix2/B1-a). Resolved ONLY
	// for a volume that actually carries a task binding — an unscheduled volume,
	// and therefore the default path, never asks. It is the zone RECORDED when the
	// task was created, confirmed to still be the NAS's live zone
	// (GF2-fix3/B1-d): a missing record, an unreadable live zone, or any
	// difference between the two returns nil and fails closed.
	var scheduledZone *time.Location
	if scheduledTaskSchema != "" {
		scheduledZone = d.scheduledSnapshotZone(ctx, ds, datasetName)
	}

	foreignSnapshots, unprovenSnapshots := d.foreignSnapshotsOnly(snapshots, ds, scheduledTaskSchema, scheduledZone, true)
	if !d.config.ZFS.DestroyForeignSnapshotsOnDelete && len(foreignSnapshots) > 0 {
		// The DECISION is unchanged — preserve-until-reaped is deliberate. Only the
		// DIAGNOSIS is refined: a blocking snapshot this driver provably tombstoned
		// itself must not be blamed on a foreign task.
		tombstones := d.countProvenDriverTombstones(ctx, foreignSnapshots)
		klog.Infof("Volume %s has %d snapshots blocking deletion (%d of them this driver's own deferred-deletion tombstones) and destroyForeignSnapshotsOnDelete is disabled; refusing before share deletion",
			volumeID, len(foreignSnapshots), tombstones)
		return nil, status.Error(codes.FailedPrecondition,
			foreignSnapshotRefusalMessage(volumeID, len(foreignSnapshots), unprovenSnapshots, tombstones))
	}

	// Delete share first (errors are fatal to prevent orphaned targets)
	switch {
	case shareTypeKnown:
		// We know the share type, delete just that one
		if err := d.deleteShare(ctx, ds, datasetName, shareType); err != nil {
			klog.Errorf("Failed to delete share for volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete share: %v", err)
		}
	case ds != nil && ds.Type == "VOLUME":
		// Unknown zvol - try both iSCSI and NVMe-oF to avoid orphaned resources
		// One will normally prove absence and return nil. Any other error is a
		// cleanup failure and must stop dataset deletion.
		var cleanupErrors []string
		if err := d.deleteShare(ctx, ds, datasetName, ShareTypeISCSI); err != nil {
			cleanupErrors = append(cleanupErrors, "iSCSI: "+err.Error())
		}
		if err := d.deleteShare(ctx, ds, datasetName, ShareTypeNVMeoF); err != nil {
			cleanupErrors = append(cleanupErrors, "NVMe-oF: "+err.Error())
		}
		if len(cleanupErrors) > 0 {
			return nil, status.Errorf(codes.Internal, "protocol cleanup failed for %s: %s", volumeID, strings.Join(cleanupErrors, "; "))
		}
	default:
		// Default fallback for filesystem or unknown types
		if err := d.deleteShare(ctx, ds, datasetName, shareType); err != nil {
			klog.Errorf("Failed to delete share for volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete share: %v", err)
		}
	}

	// Get origin snapshot property before deletion (for volume-to-volume clones)
	// This snapshot was created during cloning and should be cleaned up after the clone is deleted
	var originSnapshotID string
	if ds != nil {
		if prop, ok := ds.UserProperties[PropVolumeOriginSnapshot]; ok && prop.Value != "" && prop.Value != "-" {
			originSnapshotID = prop.Value
		}
	}

	// Try to delete dataset without recursive first to preserve snapshots
	// This follows CSI spec: snapshots should survive after source volume deletion
	if err := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); err != nil {
		// DatasetDelete already handles "not found" errors, so this is a real error
		if !isDatasetDependencyOrBusyError(err) {
			klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
		}
		// Re-check dataset origins for a clone created after the up-front guard,
		// then classify snapshots. This remains authoritative on TrueNAS 26.0,
		// where snapshot clone projections are empty.
		hasDependentClones, cloneErr = d.truenasClient.DatasetHasDependentClones(ctx, datasetName)
		if cloneErr != nil {
			return nil, status.Errorf(codes.Internal,
				"failed to verify clone dependencies for volume %s: %v", volumeID, cloneErr)
		}
		if hasDependentClones {
			return nil, status.Errorf(codes.FailedPrecondition,
				"volume %s has dependent clones that must be deleted first", volumeID)
		}

		// Check if there are CSI-managed snapshots that are blocking deletion
		// TrueNAS returns various error messages: "Method call error", "has dependent clones", etc.
		snapshots, snapErr := d.truenasClient.SnapshotList(ctx, datasetName)
		hadSnapshotsBeforeInternalCleanup := len(snapshots) > 0
		if snapErr == nil {
			snapshots, snapErr = d.deleteOrphanedInternalCloneSourceSnapshots(ctx, snapshots)
			if snapErr != nil {
				return nil, snapErr
			}
		}
		switch {
		case snapErr == nil && len(snapshots) > 0:
			// Found snapshots - check if any are managed or internal.
			for _, snap := range snapshots {
				if snapshotBlocksVolumeDeletion(snap) {
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s has dependent snapshots that must be deleted first", volumeID)
				}
			}
			// Non-CSI-managed snapshots exist. Driver-owned scheduled snapshots
			// (GF2/E2) are deleted WITH the volume via the recursive destroy below
			// and never respect the foreign-preserve policy (R4); only genuinely
			// foreign snapshots are preserved by default (recursive deletion of
			// them is an explicit operator opt-in).
			// meter=false: the pre-share-delete pass above already counted and
			// logged these; this fresh list exists to catch snapshots that
			// appeared after that check, not to re-report the same ones.
			foreignSnapshots, unprovenSnapshots := d.foreignSnapshotsOnly(snapshots, ds, scheduledTaskSchema, scheduledZone, false)
			if len(foreignSnapshots) > 0 && !d.config.ZFS.DestroyForeignSnapshotsOnDelete {
				return nil, status.Error(codes.FailedPrecondition, foreignSnapshotRefusalMessage(
					volumeID, len(foreignSnapshots), unprovenSnapshots, d.countProvenDriverTombstones(ctx, foreignSnapshots)))
			}
			klog.V(4).Infof("Volume %s has non-managed snapshots, deleting recursively", volumeID)
			if delErr := d.recursiveDatasetDeleteWithHoldRecovery(ctx, datasetName); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, delErr)
				if isDatasetDependencyOrBusyError(delErr) {
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s has dependent snapshot clones that must be deleted first: %v", volumeID, delErr)
				}
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
		case snapErr != nil:
			// UNCONDITIONALLY fail closed (GF2-fix3/B1-f). An error is not evidence
			// of absence, and destroyForeignSnapshotsOnDelete does not authorize a
			// BLIND recursive destroy: the opt-in means "you may destroy the foreign
			// snapshots I have seen and classified", not "destroy whatever is there
			// when the listing fails". Round 2 still recursed here for opted-in
			// operators, which could take out snapshots nothing ever classified.
			// A retry once the backend answers again is always available.
			return nil, status.Errorf(codes.FailedPrecondition,
				"cannot verify snapshots for volume %s; refusing recursive delete (this refusal is not affected by zfs.destroyForeignSnapshotsOnDelete — an unreadable snapshot list is not evidence that there is nothing to destroy): %v", volumeID, snapErr)
		default:
			if !hadSnapshotsBeforeInternalCleanup {
				// No snapshots, but non-recursive delete still failed - preserve the
				// existing error classification for an unrelated backend failure.
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, err)
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
			}
			// The only snapshots may have been unreferenced internal clone-source
			// snapshots. Retry now that those driver-owned blockers are gone.
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s after internal snapshot cleanup: %v", volumeID, delErr)
				if isDatasetDependencyOrBusyError(delErr) {
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s acquired new snapshot dependencies during deletion: %v", volumeID, delErr)
				}
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
		}
	}

	// Clean up origin snapshot if this was a volume-to-volume clone
	// The clone's dependency on the snapshot is now broken, so we can delete it
	// NOTE (GF2-fix/F5): GF2 briefly added an ungated "origin promoted away" skip
	// here. It was removed: deleteCloneOriginSnapshot already treats NotFound as
	// success, so chasing a migrated origin snapshot is harmless, while the guard
	// was the sprint's ONLY default-path behavior change (it fired for every
	// volume-to-volume clone with all four GF2 flags off) and it failed OPEN —
	// an absent origin property would have silently orphaned the temp
	// clone-source snapshot with no tombstone and no ledger entry.
	if originSnapshotID != "" {
		klog.Infof("Cleaning up origin snapshot %s for deleted volume clone %s", originSnapshotID, volumeID)
		if err := d.deleteCloneOriginSnapshot(ctx, originSnapshotID); err != nil {
			klog.Errorf("Failed to delete origin snapshot %s: %v", originSnapshotID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete clone origin snapshot %s: %v", originSnapshotID, err)
		}
	}

	// Drop the volume's per-volume usage series so a deleted volume cannot leave
	// a latched near-quota gauge (and unbounded label cardinality) behind
	// (GF2-fix/F6). A no-op when the feature never published one.
	DeleteVolumeUsageMetrics(volumeID)

	klog.Infof("Volume %s deleted successfully", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
}

// deleteCloneOriginSnapshot retries the post-clone cleanup within the original
// DeleteVolume call. A later CO retry cannot recover the snapshot ID after the
// clone dataset has already disappeared.
func (d *Driver) deleteCloneOriginSnapshot(ctx context.Context, snapshotID string) error {
	backoff := originSnapshotDeleteBackoff
	var lastErr error
	for attempt := 1; attempt <= originSnapshotDeleteAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = d.truenasClient.SnapshotDelete(ctx, snapshotID, true, false)
		if lastErr == nil || truenas.IsNotFoundError(lastErr) {
			return nil
		}
		if attempt == originSnapshotDeleteAttempts {
			break
		}

		klog.Warningf("Failed to delete clone origin snapshot %s (attempt %d/%d): %v; retrying in %v",
			snapshotID, attempt, originSnapshotDeleteAttempts, lastErr, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
			backoff *= 2
			if backoff > originSnapshotDeleteMaxBackoff {
				backoff = originSnapshotDeleteMaxBackoff
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}

	return lastErr
}

// ControllerPublishVolume attaches a volume to a node.
// For iSCSI/NVMe-oF volumes, this ensures the target/subsystem exists on TrueNAS.
// This is critical for volumes restored from backups (e.g., VolSync) where the
// underlying zvol exists but the export configuration was not restored.
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	nodeID := req.GetNodeId()
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	identity, err := d.resolveControllerNodeIdentity(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	// Best-effort node validation: when this process also runs the node service
	// (combined/all mode) it knows its own node ID, so a request for a different
	// node is a NotFound. In the split deployment runNode is false and this is
	// inert (the CO's attach-detach controller owns node targeting). This also
	// satisfies the csi-sanity "publish should fail when the node does not exist"
	// conformance case. (Do not remove — it is conditionally load-bearing.)
	if d.runNode && identity.Name != d.nodeID {
		return nil, status.Errorf(codes.NotFound, "node not found: %s", nodeID)
	}
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
		return nil, status.Errorf(codes.Internal, "failed to get volume: %v", err)
	}

	shareType := shareTypeForPublishedVolume(ds, req.GetVolumeContext())

	klog.Infof("ControllerPublishVolume: volumeID=%s, nodeID=%s, shareType=%s", volumeID, nodeID, shareType)

	// Per-request memo so the ensure-share step and the fence phases resolve the
	// backend share objects once and reuse them (see fenceResolution).
	res := &fenceResolution{}
	// Ensure the share exists (critical for restored volumes)
	// This recreates missing iSCSI targets or NVMe-oF subsystems
	if err := d.ensureShareExists(ctx, ds, datasetName, volumeID, shareType, res); err != nil {
		return nil, err
	}
	// Publication records (CSI single-node exclusivity, idempotency, takeover) are
	// maintained unconditionally; fencing.mode only governs backend allowlist
	// enforcement inside publishFencedVolume.
	if err := d.publishFencedVolume(ctx, ds, datasetName, shareType, identity, req.GetVolumeCapability(), req.GetReadonly(), res); err != nil {
		return nil, err
	}

	klog.Infof("ControllerPublishVolume: volume %s published successfully to node %s", volumeID, nodeID)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume detaches a volume from a node (not used for NFS).
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
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
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume: %v", err)
	}
	// Publication records are cleared unconditionally so single-node bookkeeping
	// stays correct in every fencing mode; backend allowlist revocation inside
	// unpublishFencedVolume remains governed by fencing.mode.
	shareType := shareTypeForPublishedVolume(ds, nil)
	if err := d.unpublishFencedVolume(ctx, ds, datasetName, shareType, req.GetNodeId(), &fenceResolution{}); err != nil {
		return nil, err
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities validates volume capabilities.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	// Check volume exists and use its actual type when validating capabilities.
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

	if mode, ok := multiNodeAccessMode(caps); ok && ds.Type == "VOLUME" {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Message: fmt.Sprintf("access mode %s requires NFS protocol; volume %s is a block volume", mode.String(), volumeID),
		}, nil
	}
	for _, capability := range caps {
		if capability.GetBlock() != nil && ds.Type == "FILESYSTEM" {
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: fmt.Sprintf("block access type is incompatible with filesystem volume %s", volumeID),
			}, nil
		}
	}

	// Validate capabilities
	confirmed := &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
		VolumeCapabilities: caps,
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

// ListVolumes lists all volumes.
// Note: Pagination is based on the offset of server-filtered CSI datasets.
// The client-side managed-resource check remains as a compatibility safeguard.
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Info("ListVolumes called")

	// Parse starting token as offset
	offset := 0
	if req.GetStartingToken() != "" {
		var err error
		offset, err = strconv.Atoi(req.GetStartingToken())
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token: %v", err)
		}
	}

	// Use max entries as limit (default to 100 if not specified or 0).
	requestedLimit := int(req.GetMaxEntries())
	if requestedLimit == 0 {
		requestedLimit = 100
	}
	// Fetch one lookahead row to determine whether another page exists.
	fetchLimit := requestedLimit + 1

	datasets, err := d.truenasClient.DatasetList(ctx, d.config.ZFS.DatasetParentName, fetchLimit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	pageSize := len(datasets)
	if pageSize > requestedLimit {
		pageSize = requestedLimit
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, pageSize)
	for _, ds := range datasets[:pageSize] {

		// Skip if not managed by CSI
		if prop, ok := ds.UserProperties[PropManagedResource]; !ok || prop.Value != "true" {
			continue
		}

		volumeID := path.Base(ds.Name)
		capacity := d.getDatasetCapacity(ds)

		entries = append(entries, &csi.ListVolumesResponse_Entry{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: capacity,
			},
			// Populate the entry's VolumeCondition from the dataset+pool
			// composition ControllerGetVolume also builds on. external-health-monitor
			// v0.18.0 prefers ListVolumes whenever LIST_VOLUMES is advertised and
			// reads Entry.Status.VolumeCondition; leaving it nil made its nil-safe
			// getters report every listed volume as normal (codex H1). The opt-in
			// quota upgrade (GF2/E4) is deliberately NOT applied here — one quota
			// query per listed volume — so near-quota surfaces via the reconcile
			// sweep's gauge and alert instead; see volumeConditionFromDataset.
			Status: &csi.ListVolumesResponse_VolumeStatus{
				VolumeCondition: d.volumeCondition(ds),
			},
		})
	}

	// Advance by server-filtered rows consumed; compatibility filtering above does
	// not affect page math.
	nextToken := ""
	if len(datasets) > requestedLimit {
		nextToken = strconv.Itoa(offset + pageSize)
	}

	return &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

// GetCapacity returns the available capacity.
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Info("GetCapacity called")

	// Report the parent dataset's ZFS-computed `available` bytes rather than a
	// raw vdev free-space sum. `available` natively nets out RAIDZ parity
	// overhead, ancestor quota/refquota, and existing refreservations, so it is
	// the honest "how much can I still provision here" number (G1 probe confirmed
	// the parsed value is bytes on 26.0). req.Parameters is deliberately ignored:
	// the driver honors no per-StorageClass parent/pool override (only `protocol`
	// is consumed at CreateVolume), and datasetForID always derives
	// path.Join(DatasetParentName, id), so every StorageClass of this
	// single-backend driver shares the one parent dataset reported here.
	parent := strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/")
	ds, err := d.truenasClient.DatasetGet(ctx, parent)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get capacity: %v", err)
	}

	avail := int64(0)
	if v, ok := ds.Available.Parsed.(float64); ok {
		avail = int64(v)
	} else {
		// A scheduler reads AvailableCapacity=0 as "backend full" and can halt
		// provisioning cluster-wide, so a missing/unparseable `available` must not
		// degrade silently. Keep returning 0 (honest "nothing confirmed free")
		// but surface why, so an operator can tell a full pool from a parse miss.
		klog.Warningf("GetCapacity: dataset %s has absent/unparseable `available` (%v); reporting 0", parent, ds.Available.Parsed)
	}

	resp := &csi.GetCapacityResponse{
		AvailableCapacity: avail,
	}
	// maximum_volume_size is opt-in (capacity.reportMaximumVolumeSize). Under the
	// default thin/sparse provisioning `available` is a soft estimate and a hard
	// maximum would make the scheduler wrongly reject legitimate overcommit; only
	// thick deployments (zvolEnableReservation) should advertise it, where
	// `available` already nets out refreservations and is a true remaining ceiling.
	// TrueNAS 26.0 exposes no dedicated max-size API, so the ceiling is `available`.
	if d.config.Capacity.ReportMaximumVolumeSize {
		resp.MaximumVolumeSize = wrapperspb.Int64(avail)
	}
	return resp, nil
}

// CreateSnapshot creates a snapshot.
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (_ *csi.CreateSnapshotResponse, operationErr error) {
	defer func() {
		d.recordOperationFailureEvent(createSnapshotEventRef(req), EventReasonSnapshotCreateFailed, "CreateSnapshot", operationErr)
	}()
	start := time.Now()
	sourceVolumeID := req.GetSourceVolumeId()
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}

	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	klog.Infof("CreateSnapshot: name=%s, sourceVolumeID=%s", name, sourceVolumeID)

	// Always acquire the source-volume lock before the snapshot lock. This
	// serializes snapshot creation with DeleteVolume and gives all creators a
	// fixed lock order.
	sourceVolumeLockKey := volumeLockKey(sourceVolumeID)
	if !d.acquireOperationLock(sourceVolumeLockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for the source volume")
	}
	defer d.releaseOperationLock(sourceVolumeLockKey)

	snapshotID := sanitizeVolumeID(name)
	if _, err := d.datasetForID(snapshotID); err != nil {
		return nil, err
	}
	snapshotLockKey := "snapshot:" + snapshotID
	if !d.acquireOperationLock(snapshotLockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(snapshotLockKey)

	datasetName, err := d.datasetForID(sourceVolumeID)
	if err != nil {
		return nil, err
	}
	sourceDataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.NotFound, "source volume not found: %s", sourceVolumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get source volume: %v", err)
	}

	// Snapshot names are global CSI identifiers even though ZFS only requires
	// uniqueness within a dataset. Resolve the short name before creation so a
	// request cannot silently create the same CSI snapshot ID for another source.
	existing, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to find existing snapshot: %v", err)
	}
	if existing != nil {
		// Identity properties are only compared when present: snapshots created
		// before they were introduced lack them, and dataset+name equality
		// already establishes same-source for those.
		originalName, hasName := existing.UserProperties[PropCSISnapshotName]
		originalSource, hasSource := existing.UserProperties[PropCSISnapshotSourceVolumeID]
		if !isCSISnapshot(existing) || existing.Dataset != datasetName ||
			(hasName && originalName.Value != name) ||
			(hasSource && originalSource.Value != sourceVolumeID) {
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot name %q is already associated with another snapshot", name)
		}
		d.holdCSISnapshotIfEnabled(ctx, existing.ID, req)
		return d.createSnapshotResponse(existing, sourceDataset, snapshotID, sourceVolumeID, start), nil
	}

	// Create the snapshot and its identity atomically. TrueNAS 26.0 silently
	// ignores post-create pool.snapshot.update property writes.
	snapshotProperties := map[string]string{
		PropManagedResource:           "true",
		PropDriverInstanceID:          d.driverInstanceID(),
		PropCSISnapshotName:           name,
		PropCSISnapshotSourceVolumeID: sourceVolumeID,
	}
	// GEOMETRY PROVENANCE (round 5). A restore has to know the layout of the bytes
	// IN THIS SNAPSHOT, and the source's state at RESTORE time cannot answer that
	// — the extent can be re-created at a different geometry in between. For an
	// iSCSI source, capture it HERE, where "now" IS the snapshot's content, and
	// fold it into the create's own property write. NVMe-oF has no iSCSI extent
	// geometry to capture, so it follows its namespace path without this probe.
	// ROUND 6: this returns an ERROR when the volume's stamp and its live extent
	// disagree. Capturing either one would be a guess about which describes the
	// bytes in this snapshot, and the restore that reads it back would act on that
	// guess — so the snapshot is refused instead. The volume is already
	// unpublishable in that state (guardStampedVsLiveGeometry), so this costs no
	// availability that was not already lost.
	// The geometry capture is iSCSI-specific. NVMe-oF's namespace is the
	// protocol object and this client exposes no namespace block-size setting to
	// compare; its namespace witness must not route the snapshot through an
	// iSCSI extent lookup.
	sourceShareType := shareTypeForPublishedVolume(sourceDataset, nil)
	geometryProps, geometryErr := d.snapshotGeometryProps(ctx, sourceDataset, datasetName, sourceShareType)
	if geometryErr != nil {
		return nil, geometryErr
	}
	for key, value := range geometryProps {
		snapshotProperties[key] = value
	}
	snap, err := d.truenasClient.SnapshotCreate(ctx, datasetName, snapshotID, snapshotProperties)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	// Place the deletion-proof hold now that the snapshot and its identity exist.
	d.holdCSISnapshotIfEnabled(ctx, snap.ID, req)

	// The source dataset fetched for the existence check above still reflects
	// the volume size for restoreSize — no need to re-query it.
	return d.createSnapshotResponse(snap, sourceDataset, snapshotID, sourceVolumeID, start), nil
}

// holdCSISnapshotIfEnabled places a deletion-proof hold on a CSI snapshot when
// zfs.holdCsiSnapshots is set (GF2/E1). A hold is a hardening layer, not a
// correctness precondition: failure is non-fatal — logged, metered, and surfaced
// as a warning event — so the snapshot still becomes/stays ReadyToUse and simply
// degrades to pre-GF2 (unprotected) behavior. The idempotent client treats an
// already-held snapshot as success, so the idempotent-create retry re-holds
// safely.
func (d *Driver) holdCSISnapshotIfEnabled(ctx context.Context, snapshotID string, req *csi.CreateSnapshotRequest) {
	if !d.config.ZFS.HoldCSISnapshots {
		return
	}
	if err := d.truenasClient.SnapshotHold(ctx, snapshotID); err != nil {
		RecordSnapshotHold(snapshotHoldOpHold, err)
		klog.Warningf("Failed to place deletion-proof hold on snapshot %s (continuing unprotected): %v", snapshotID, err)
		d.recordWarningEvent(createSnapshotEventRef(req), EventReasonSnapshotHoldFailed,
			fmt.Sprintf("Snapshot %s was created but its deletion-proof hold failed; it is not protected from foreign deletion: %v", snapshotID, err))
		return
	}
	RecordSnapshotHold(snapshotHoldOpHold, nil)
	klog.V(4).Infof("Placed deletion-proof hold on snapshot %s", snapshotID)
}

// releaseCSISnapshotHoldIfEnabled removes a deletion-proof hold before the
// driver's own destroy paths run (GF2/E1, R1). It is called by DeleteSnapshot,
// handleSnapshotClones, and reapTombstoneSnapshot — the only three driver sites
// that destroy a snapshot — each of which has already provenance-proven the
// snapshot is a driver CSI snapshot/tombstone, so releasing never strips a hold
// the driver does not own (R2). The release is idempotent and best-effort: a
// failure is logged and metered, and the subsequent destroy surfaces a retryable
// EBUSY if the hold is genuinely still present. Gated on zfs.holdCsiSnapshots so
// the default path makes no extra call.
func (d *Driver) releaseCSISnapshotHoldIfEnabled(ctx context.Context, snapshotID string) {
	if !d.config.ZFS.HoldCSISnapshots {
		return
	}
	d.releaseCSISnapshotHold(ctx, snapshotID)
}

// releaseCSISnapshotHold releases UNCONDITIONALLY — regardless of
// zfs.holdCsiSnapshots (GF2-fix/H4). Holds are backend state that outlives the
// flag: flipping the flag back off (an ordinary Helm rollback) must never leave
// previously-held CSI snapshots and tombstones undeletable by the driver, which
// is exactly what a flag-gated release produced (EBUSY -> codes.Internal ->
// infinite external-snapshotter retry, with the source volume's DeleteVolume
// blocked behind it). Every caller has already provenance-proven the snapshot is
// a driver CSI snapshot or tombstone, so this never strips a hold the driver
// does not own (R2).
func (d *Driver) releaseCSISnapshotHold(ctx context.Context, snapshotID string) {
	if err := d.truenasClient.SnapshotRelease(ctx, snapshotID); err != nil {
		RecordSnapshotHold(snapshotHoldOpRelease, err)
		klog.Warningf("Failed to release deletion-proof hold on snapshot %s before destroy: %v", snapshotID, err)
		return
	}
	RecordSnapshotHold(snapshotHoldOpRelease, nil)
	klog.V(4).Infof("Released deletion-proof hold on snapshot %s before destroy", snapshotID)
}

// destroyDriverSnapshot destroys a snapshot the caller has already
// provenance-proven the driver owns, recovering from a hold the CURRENT
// configuration did not place (GF2-fix/H4).
//
// The flag-gated pre-release above saves a round trip while the feature is on.
// This is the fail-safe underneath it: if the destroy still returns EBUSY "has
// the following holds", the hold is released UNCONDITIONALLY and the destroy is
// retried exactly once. On the default path (no hold was ever placed) that EBUSY
// never occurs, so this adds zero API calls — the default-off invariant survives
// while "turn the flag back off" stops being a permanent wedge.
func (d *Driver) destroyDriverSnapshot(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	err := d.truenasClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
	if err == nil || !truenas.IsSnapshotHeldError(err) {
		return err
	}
	klog.Warningf("Destroy of driver-owned snapshot %s was refused by a ZFS hold; releasing unconditionally and retrying once", snapshotID)
	RecordSnapshotHoldRecovery()
	d.releaseCSISnapshotHold(ctx, snapshotID)
	return d.truenasClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

// releaseHeldDriverSnapshotsUnder releases the hold on every snapshot beneath a
// volume dataset that this driver can PROVE it owns: a live CSI snapshot
// carrying this instance's identity, or a tombstone whose retained identity
// re-derives through the production rename algorithm. Foreign snapshots are
// never released (R2).
//
// It exists for the one destroy path that cannot release per snapshot — the
// recursive DatasetDelete in DeleteVolume, where ZFS refuses the WHOLE operation
// with EBUSY if any snapshot beneath the dataset is held. Without it a held
// driver tombstone made its volume permanently undeletable. This is also the
// only production wiring of SnapshotIsHeld, which was otherwise dead code
// carried on ClientInterface.
// lazyTombstoneLedger returns an accessor that reads the tombstone ledger at
// most once, on first use. The ledger is the driver's AUTHORITATIVE proof that
// it tombstoned a snapshot, and the retained-identity chain alone is not
// sufficient because handleSnapshotClones attempts to strip those identity
// properties at rename. Callers are on already-exceptional paths (a hold
// recovery, a delete refusal), so the read costs nothing on the hot path.
func (d *Driver) lazyTombstoneLedger(ctx context.Context) func() map[string]tombstoneLedgerEntry {
	var ledger map[string]tombstoneLedgerEntry
	return func() map[string]tombstoneLedgerEntry {
		if ledger != nil {
			return ledger
		}
		ledger = make(map[string]tombstoneLedgerEntry)
		reads := d.readBookkeepingDatasets(ctx, d.bookkeepingEnabled())
		for key, entry := range tombstoneLedgerFromDataset(reads.parent) {
			ledger[key] = entry
		}
		for key, entry := range tombstoneLedgerFromDataset(reads.child) {
			ledger[key] = entry
		}
		return ledger
	}
}

// snapshotIsProvenTombstone reports whether a snapshot is one this driver
// provably tombstoned: tombstone name SHAPE plus either a retained identity that
// re-derives through the production rename algorithm, or a matching ledger
// entry. Shape alone is never enough — a manual lookalike must not be claimed.
func (d *Driver) snapshotIsProvenTombstone(snap *truenas.Snapshot, ledgerFor func() map[string]tombstoneLedgerEntry) bool {
	if !isSnapshotTombstone(snap) {
		return false
	}
	if snapshotMatchesRetainedTombstoneIdentity(snap, d.driverInstanceID()) {
		return true
	}
	entry, recorded := ledgerFor()[tombstoneLedgerKey(snap.ID)]
	return recorded && tombstoneLedgerEntryMatchesSnapshot(entry, snap)
}

// countProvenDriverTombstones reports how many of the snapshots blocking a
// DeleteVolume are this driver's OWN deferred-deletion tombstones. It is a pure
// DIAGNOSTIC for the refusal message: it changes no decision, and the
// preserve-until-reaped behavior is deliberate (the reconcile reaper clears them
// and the retry then succeeds).
//
// The ledger is read only when at least one blocking snapshot is
// tombstone-shaped, so an ordinary foreign-snapshot refusal costs no extra call.
func (d *Driver) countProvenDriverTombstones(ctx context.Context, snapshots []*truenas.Snapshot) int {
	shaped := false
	for _, snap := range snapshots {
		if isSnapshotTombstone(snap) {
			shaped = true
			break
		}
	}
	if !shaped {
		return 0
	}
	ledgerFor := d.lazyTombstoneLedger(ctx)
	count := 0
	for _, snap := range snapshots {
		if d.snapshotIsProvenTombstone(snap, ledgerFor) {
			count++
		}
	}
	return count
}

func (d *Driver) releaseHeldDriverSnapshotsUnder(ctx context.Context, datasetName string) int {
	snapshots, err := d.truenasClient.SnapshotList(ctx, datasetName)
	if err != nil {
		klog.Warningf("Could not list snapshots of %s to clear driver-owned holds: %v", datasetName, err)
		return 0
	}
	ledgerFor := d.lazyTombstoneLedger(ctx)

	released := 0
	for _, snap := range snapshots {
		if snap == nil {
			continue
		}
		owned := isCSISnapshot(snap) && snapshotCarriesInstanceIdentity(snap, d.driverInstanceID())
		if !owned {
			owned = d.snapshotIsProvenTombstone(snap, ledgerFor)
		}
		if !owned {
			continue
		}
		held, heldErr := d.truenasClient.SnapshotIsHeld(ctx, snap.ID)
		if heldErr != nil {
			klog.Warningf("Could not read holds on driver-owned snapshot %s: %v", snap.ID, heldErr)
			continue
		}
		if !held {
			continue
		}
		RecordSnapshotHoldRecovery()
		d.releaseCSISnapshotHold(ctx, snap.ID)
		released++
	}
	return released
}

// recursiveDatasetDeleteWithHoldRecovery performs DeleteVolume's recursive
// dataset destroy, recovering once from a driver-owned ZFS hold beneath it
// (GF2-fix/H4). ZFS refuses the entire recursive destroy with EBUSY when any
// snapshot under the dataset is held, and the per-snapshot release sites cannot
// see those snapshots — so with holds enabled (or previously enabled and since
// disabled) a held driver tombstone made its volume undeletable forever. Only
// driver-proven snapshots are released; a hold on a foreign snapshot correctly
// keeps the destroy refused. Zero extra calls unless the EBUSY actually happens.
func (d *Driver) recursiveDatasetDeleteWithHoldRecovery(ctx context.Context, datasetName string) error {
	err := d.truenasClient.DatasetDelete(ctx, datasetName, true, true)
	if err == nil || !truenas.IsSnapshotHeldError(err) {
		return err
	}
	if released := d.releaseHeldDriverSnapshotsUnder(ctx, datasetName); released == 0 {
		return err
	}
	klog.Warningf("Recursive delete of %s was refused by ZFS holds on driver-owned snapshots; released them and retrying once", datasetName)
	return d.truenasClient.DatasetDelete(ctx, datasetName, true, true)
}

// snapshotCarriesInstanceIdentity reports whether a snapshot's retained identity
// names THIS driver instance.
func snapshotCarriesInstanceIdentity(snap *truenas.Snapshot, instanceID string) bool {
	if snap == nil || instanceID == "" {
		return false
	}
	property, ok := snap.UserProperties[PropDriverInstanceID]
	return ok && property.Value == instanceID
}

func (d *Driver) createSnapshotResponse(
	snap *truenas.Snapshot,
	sourceDataset *truenas.Dataset,
	snapshotID string,
	sourceVolumeID string,
	start time.Time,
) *csi.CreateSnapshotResponse {
	var snapshotSize int64
	if sourceDataset != nil {
		snapshotSize = d.getDatasetCapacity(sourceDataset)
	}
	if snapshotSize <= 0 {
		snapshotSize = snap.GetSnapshotSize()
	}
	klog.Infof("CreateSnapshot completed: snapshot=%s, sourceVolume=%s, size=%d, elapsed=%v",
		snapshotID, sourceVolumeID, snapshotSize, time.Since(start))

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: sourceVolumeID,
			SizeBytes:      snapshotSize,
			CreationTime:   timestampProto(snap.GetCreationTime()),
			ReadyToUse:     true,
		},
	}
}

// DeleteSnapshot deletes a snapshot.
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	klog.Infof("DeleteSnapshot: snapshotID=%s", snapshotID)

	// The CSI snapshot ID does not encode its source volume, so a read-only
	// lookup is required before the locks can be ordered. Once resolved, acquire
	// the source-volume lock before the snapshot lock to match CreateSnapshot.
	snap, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
	if err != nil {
		// If parent dataset doesn't exist, the snapshot is effectively deleted
		if truenas.IsNotFoundError(err) {
			klog.Infof("Snapshot %s parent not found, treating as deleted", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
	}

	if snap == nil {
		klog.Infof("Snapshot %s not found, treating as already deleted", snapshotID)
		return &csi.DeleteSnapshotResponse{}, nil
	}
	if !isCSISnapshot(snap) {
		klog.Warningf("Snapshot %s resolves to non-CSI snapshot %s; refusing to delete it", snapshotID, snap.ID)
		return &csi.DeleteSnapshotResponse{}, nil
	}

	// If the dataset is outside the configured CSI parent, its source is unknown
	// and the non-blocking snapshot lock remains the only available guard.
	parentPrefix := strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/") + "/"
	if strings.HasPrefix(snap.Dataset, parentPrefix) {
		sourceVolumeID := path.Base(snap.Dataset)
		sourceVolumeLockKey := volumeLockKey(sourceVolumeID)
		if !d.acquireOperationLock(sourceVolumeLockKey) {
			return nil, status.Error(codes.Aborted, "operation already in progress for the source volume")
		}
		defer d.releaseOperationLock(sourceVolumeLockKey)
	}
	snapshotLockKey := "snapshot:" + snapshotID
	if !d.acquireOperationLock(snapshotLockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(snapshotLockKey)

	// Release the deletion-proof hold BEFORE any destroy/tombstone (GF2/E1, R1).
	// The hold survives the tombstone rename, so releasing here — under the same
	// source-volume+snapshot locks — guarantees the snapshot the reaper may later
	// destroy is already unheld. Idempotent: a snapshot that was never held (the
	// feature defaults off) releases to a no-op success.
	d.releaseCSISnapshotHoldIfEnabled(ctx, snap.ID)

	if err := d.destroyDriverSnapshot(ctx, snap.ID, false, false); err != nil {
		// Handle "not found" as success (idempotency)
		if truenas.IsNotFoundError(err) {
			klog.Infof("Snapshot %s already deleted", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}

		// A restored volume is a ZFS clone of its source snapshot. Defer-destroy
		// the snapshot so the CSI snapshot and restored-volume lifecycles remain
		// independent while ZFS keeps the dependency alive internally.
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			if handleErr := d.handleSnapshotClones(ctx, snap); handleErr != nil {
				return nil, handleErr
			}
			klog.Infof("Snapshot %s scheduled for deferred deletion", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}

		klog.Errorf("Failed to delete snapshot %s: %v", snapshotID, err)
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
	}
	klog.Infof("Snapshot %s deleted successfully", snapshotID)

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots lists snapshots.
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Info("ListSnapshots called")

	// A snapshot ID uniquely identifies at most one snapshot, so bypass the
	// paginated list API when it is provided.
	if snapshotID := req.GetSnapshotId(); snapshotID != "" {
		snap, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
		}
		if snap == nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		entry, entryErr := d.snapshotListEntry(ctx, snap, req.GetSourceVolumeId())
		if entryErr != nil {
			return nil, entryErr
		}
		if entry == nil || entry.Snapshot.GetSnapshotId() != snapshotID {
			return &csi.ListSnapshotsResponse{}, nil
		}
		return &csi.ListSnapshotsResponse{Entries: []*csi.ListSnapshotsResponse_Entry{entry}}, nil
	}

	// Parse starting token as offset
	offset := 0
	if req.GetStartingToken() != "" {
		var err error
		offset, err = strconv.Atoi(req.GetStartingToken())
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token: %v", err)
		}
	}

	// Use max entries as limit (default to 100 if not specified or 0)
	limit := int(req.GetMaxEntries())
	if limit == 0 {
		limit = 100
	}

	snapshots, err := d.truenasClient.SnapshotListAll(ctx, d.config.ZFS.DatasetParentName, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, 0)
	for _, snap := range snapshots {
		entry, entryErr := d.snapshotListEntry(ctx, snap, req.GetSourceVolumeId())
		if entryErr != nil {
			return nil, entryErr
		}
		if entry != nil {
			entries = append(entries, entry)
		}
	}

	// Generate next token if we got a full page
	nextToken := ""
	if len(snapshots) == limit {
		nextToken = strconv.Itoa(offset + limit)
	}

	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

// ControllerExpandVolume expands a volume.
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (_ *csi.ControllerExpandVolumeResponse, operationErr error) {
	volumeID := req.GetVolumeId()
	defer func() {
		d.recordOperationFailureEvent(volumeEventRef(volumeID), EventReasonVolumeExpandFailed, "ControllerExpandVolume", operationErr)
	}()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity range is required")
	}
	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	if capacityBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity is required")
	}
	limitBytes := req.GetCapacityRange().GetLimitBytes()
	if limitBytes > 0 && capacityBytes > limitBytes {
		return nil, status.Errorf(codes.InvalidArgument,
			"required capacity (%d bytes) exceeds limit (%d bytes)", capacityBytes, limitBytes)
	}

	klog.Infof("ControllerExpandVolume: volumeID=%s, capacity=%d", volumeID, capacityBytes)

	// Lock on volume ID to prevent concurrent expansions of same volume
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName, err := d.datasetForID(volumeID)
	if err != nil {
		return nil, err
	}

	// Get dataset to determine type and current state
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume details: %v", err)
	}

	currentCapacity := d.getDatasetCapacity(ds)
	hasRefquota := false
	if parsed, ok := ds.Refquota.Parsed.(float64); ok {
		hasRefquota = parsed > 0
	}
	quotaLessNFS := ds.Type == "FILESYSTEM" && !d.config.ZFS.DatasetEnableQuotas && !hasRefquota
	if quotaLessNFS && capacityBytes >= currentCapacity {
		if err := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
			PropRequestedSizeBytes: strconv.FormatInt(capacityBytes, 10),
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to record expanded volume capacity: %v", err)
		}
	}
	if capacityBytes <= currentCapacity {
		// Still request node expansion for zvols: a retry can land here after the
		// controller-side expand succeeded but before the node resized the filesystem.
		klog.Infof("Volume %s already has capacity %d bytes; expansion is a no-op", volumeID, currentCapacity)
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         currentCapacity,
			NodeExpansionRequired: ds.Type == "VOLUME",
		}, nil
	}

	// For zvols (iSCSI/NVMe-oF), expand the volsize
	if ds.Type == "VOLUME" {
		if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
		}
	}

	// For filesystems (NFS), update quota
	if ds.Type == "FILESYSTEM" {
		if d.config.ZFS.DatasetEnableQuotas {
			// Quotas are enabled - update the refquota
			params := &truenas.DatasetUpdateParams{
				Refquota: capacityBytes,
			}
			if _, err := d.truenasClient.DatasetUpdate(ctx, datasetName, params); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to update quota: %v", err)
			}
		} else {
			// Quotas are disabled - check if dataset has a quota set and update it,
			// otherwise the filesystem already has unlimited space from the pool
			if parsed, ok := ds.Refquota.Parsed.(float64); ok && parsed > 0 {
				// Dataset has an existing quota, update it
				params := &truenas.DatasetUpdateParams{
					Refquota: capacityBytes,
				}
				if _, err := d.truenasClient.DatasetUpdate(ctx, datasetName, params); err != nil {
					return nil, status.Errorf(codes.Internal, "failed to update quota: %v", err)
				}
			}
			// If no quota exists and quotas are disabled, the filesystem can already
			// use all available pool space - expansion is a no-op
			klog.V(4).Infof("NFS volume %s has no quota set, expansion is a no-op", volumeID)
		}
	}

	// Node expansion is required for zvols (iSCSI/NVMe-oF) to resize the filesystem
	// Use the actual dataset type, not the driver's default config
	nodeExpansionRequired := ds.Type == "VOLUME"

	klog.Infof("Volume %s expanded successfully", volumeID)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         capacityBytes,
		NodeExpansionRequired: nodeExpansionRequired,
	}, nil
}

// ControllerGetVolume gets information about a volume.
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

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

	// VolumeCondition is derived from the dataset's ALREADY-returned user
	// properties (no extra API call) via the same helper ListVolumes uses. A
	// dataset-gone case returns NotFound above, so reaching here means the
	// backend object exists; abnormal is reserved for a definitive negative
	// marker (see volumeConditionFromDataset).
	condition := volumeConditionFromDataset(ds)

	// GF2/E4 quota/usage reporting is strictly opt-in: when enabled, one extra
	// pool.dataset.query feeds the per-volume usage metrics and upgrades the
	// condition to abnormal once the volume crosses 95% of its effective quota.
	// When disabled (the default) no extra call is made and the condition is the
	// stamp-derived one above, exactly as before.
	if d.config.ZFS.ReportVolumeUsage {
		usage, usageErr := d.truenasClient.DatasetGetQuotaUsage(ctx, datasetName)
		if usageErr != nil {
			klog.Warningf("ControllerGetVolume: failed to read quota/usage for volume %s: %v", volumeID, usageErr)
		} else {
			RecordVolumeUsage(volumeID, usage)
			if volumeUsageNearQuota(usage) {
				// UPGRADE, never REPLACE (GF2-fix4/L2). The stamp-derived condition
				// can already be the definitive negative "provisioning is explicitly
				// marked failed", and overwriting the whole struct lost that stronger
				// reason exactly when both were true. Only Abnormal false->true and
				// an appended message.
				condition = upgradeConditionNearQuota(condition, usage)
			}
		}
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: d.getDatasetCapacity(ds),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			// Compose the stamp+quota condition (GF2/E4) with the pool-level
			// backend-health snapshot (GF5): a dataset-specific abnormal —
			// including the >95% quota upgrade above — wins over a pool-level
			// one, exactly as composeVolumeCondition orders it.
			VolumeCondition: composeVolumeCondition(condition, d.poolHealthSnapshot()),
		},
	}, nil
}

// volumeConditionFromDataset derives a CSI VolumeCondition from a fetched
// dataset's user properties without any further API call. It is the shared BASE
// for ControllerGetVolume and ListVolumes (both then compose the pool-level
// backend-health snapshot on top). The two RPCs are NOT guaranteed identical:
// ControllerGetVolume alone upgrades on the opt-in quota signal (GF2/E4,
// zfs.reportVolumeUsage) — doing that in ListVolumes would cost one quota query
// per listed volume. The external-health-monitor prefers ListVolumes when
// LIST_VOLUMES is advertised, so the near-quota signal reaches operators
// through the reconcile sweep's scale_csi_volume_near_quota gauge and its
// alert, not necessarily through the PVC's VolumeCondition.
//
// The semantics are deliberately conservative about declaring ill health. A
// volume is abnormal ONLY on a definitive negative marker: an explicit
// provision_success="false". A dataset-gone condition never reaches here (both
// callers return NotFound first). Missing managed/provision stamps are NOT
// evidence of ill health: the always-on adoption reconcile backfills only
// driver_instance_id, and a long-Bound legacy volume never re-runs CreateVolume
// (the sole path that writes both stamps), so an unstamped dataset can be
// perfectly healthy. Those are reported normal with a message noting the health
// is unverified, rather than flagged abnormal and raising spurious volume-health
// events on clusters with pre-stamp legacy PVs.
func volumeConditionFromDataset(ds *truenas.Dataset) *csi.VolumeCondition {
	if datasetUserProperty(ds, PropProvisionSuccess) == "false" {
		return &csi.VolumeCondition{
			Abnormal: true,
			Message:  "dataset provisioning is explicitly marked failed",
		}
	}
	managed := datasetUserProperty(ds, PropManagedResource) == "true"
	provisioned := datasetUserProperty(ds, PropProvisionSuccess) == "true"
	if managed && provisioned {
		return &csi.VolumeCondition{Abnormal: false}
	}
	return &csi.VolumeCondition{
		Abnormal: false,
		Message:  "volume health unverified: managed/provision stamps absent (legacy or adoption-pending dataset)",
	}
}

// upgradeConditionNearQuota folds the >95% quota finding into an existing
// VolumeCondition (GF2-fix4/L2): Abnormal is only ever raised false->true and
// the quota text is APPENDED, so a definitive-negative message the stamp check
// already produced survives instead of being overwritten by the quota one.
func upgradeConditionNearQuota(condition *csi.VolumeCondition, usage *truenas.DatasetQuotaUsage) *csi.VolumeCondition {
	message := volumeNearQuotaMessage(usage)
	if condition == nil {
		return &csi.VolumeCondition{Abnormal: true, Message: message}
	}
	condition.Abnormal = true
	if condition.Message == "" {
		condition.Message = message
	} else {
		condition.Message += "; " + message
	}
	return condition
}

// volumeNearQuotaMessage reports the REAL numbers behind the near-quota finding:
// which ZFS property binds the volume and the usage measurement that property
// actually governs (GF2-fix4/H1). When snapshots hold space that `refquota` does
// NOT count, that is called out too — it is the number an operator otherwise
// spends an afternoon reconciling against `zfs list`.
func volumeNearQuotaMessage(usage *truenas.DatasetQuotaUsage) string {
	used, quota, limit := volumeUsageBasis(usage)
	message := fmt.Sprintf("volume uses %d of %d bytes (>95%% of its %s)", used, quota, limit)
	if limit == volumeLimitRefquota && usage.UsedBySnapshots > 0 {
		message += fmt.Sprintf("; a further %d bytes are held by snapshots and do not count against refquota", usage.UsedBySnapshots)
	}
	return message
}

// ControllerModifyVolume modifies a volume (not implemented).
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not implemented")
}

// Helper functions

func sanitizeVolumeID(name string) string {
	// Rebuild through a rune range so arbitrary invalid UTF-8 input is repaired
	// while replacing the path and space characters disallowed by this scheme.
	var sanitized strings.Builder
	for _, r := range name {
		switch r {
		case '/', ' ':
			sanitized.WriteByte('-')
		default:
			sanitized.WriteRune(r)
		}
	}
	name = sanitized.String()
	if name != "" && !isLowerAlphanumeric(name[0]) {
		name = "v" + name
	}
	for len(name) > 128 {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	return name
}

func (d *Driver) datasetForID(id string) (string, error) {
	if id == "" || strings.ContainsAny(id, "/") || id == "." || id == ".." {
		return "", status.Errorf(codes.InvalidArgument, "invalid volume/snapshot id %q", id)
	}
	// The bookkeeping child dataset holds the driver's tombstone ledger and
	// in-flight markers; a crafted volumeHandle must never be able to target it
	// for delete/expand/clone. Its leaf is the only ID that resolves to it
	// (datasetForID rejects any ID containing a path separator).
	if id == bookkeepingDatasetLeaf {
		return "", status.Errorf(codes.InvalidArgument, "invalid volume/snapshot id %q: reserved bookkeeping dataset", id)
	}
	name := path.Join(d.config.ZFS.DatasetParentName, id)
	parent := strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/") + "/"
	if !strings.HasPrefix(name+"/", parent) {
		return "", status.Errorf(codes.InvalidArgument, "id %q escapes parent dataset", id)
	}
	return name, nil
}

func isLowerAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func multiNodeAccessMode(caps []*csi.VolumeCapability) (csi.VolumeCapability_AccessMode_Mode, bool) {
	for _, capability := range caps {
		mode := capability.GetAccessMode().GetMode()
		switch mode {
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER:
			return mode, true
		}
	}
	return csi.VolumeCapability_AccessMode_UNKNOWN, false
}

func buildSnapshotListEntry(snap *truenas.Snapshot, sourceVolumeFilter string) *csi.ListSnapshotsResponse_Entry {
	if !isCSISnapshot(snap) {
		return nil
	}

	snapshotID, ok := extractSnapshotName(snap.ID)
	if !ok {
		klog.V(4).Infof("Skipping snapshot with invalid ID format: %s", snap.ID)
		return nil
	}

	sourceVolumeID := ""
	if prop, ok := snap.UserProperties[PropCSISnapshotSourceVolumeID]; ok {
		sourceVolumeID = prop.Value
	} else if snap.Dataset != "" {
		// Legacy snapshots predate the source property, but the source volume is
		// still unambiguous from the snapshot dataset.
		sourceVolumeID = path.Base(snap.Dataset)
	}
	if sourceVolumeFilter != "" && sourceVolumeID != sourceVolumeFilter {
		return nil
	}

	return &csi.ListSnapshotsResponse_Entry{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: sourceVolumeID,
			SizeBytes:      snap.GetSnapshotSize(),
			CreationTime:   timestampProto(snap.GetCreationTime()),
			ReadyToUse:     true,
		},
	}
}

func (d *Driver) snapshotListEntry(ctx context.Context, snap *truenas.Snapshot, sourceVolumeFilter string) (*csi.ListSnapshotsResponse_Entry, error) {
	entry := buildSnapshotListEntry(snap, sourceVolumeFilter)
	if entry == nil {
		return nil, nil
	}
	sourceDataset, err := d.truenasClient.DatasetGet(ctx, snap.Dataset)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return entry, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get source dataset %s for snapshot %s restore size: %v", snap.Dataset, snap.ID, err)
	}
	if restoreSize := d.getDatasetCapacity(sourceDataset); restoreSize > 0 {
		entry.Snapshot.SizeBytes = restoreSize
	}
	return entry, nil
}

func (d *Driver) getDatasetCapacity(ds *truenas.Dataset) int64 {
	// For zvols, use volsize
	if ds.Type == "VOLUME" {
		if parsed, ok := ds.Volsize.Parsed.(float64); ok {
			return int64(parsed)
		}
	}
	// For filesystems, use quota or the requested size recorded at creation.
	if parsed, ok := ds.Refquota.Parsed.(float64); ok && parsed > 0 {
		return int64(parsed)
	}
	if requestedSize := datasetUserProperty(ds, PropRequestedSizeBytes); requestedSize != "" && requestedSize != "-" {
		if parsed, err := strconv.ParseInt(requestedSize, 10, 64); err == nil && parsed > 0 {
			return parsed
		}
	}
	// Legacy quota-less filesystem volumes predate the requested-size property.
	if parsed, ok := ds.Available.Parsed.(float64); ok {
		return int64(parsed)
	}
	return 0
}

// createDataset creates a new ZFS dataset or zvol and returns the API result.
func (d *Driver) createDataset(ctx context.Context, datasetName string, capacityBytes int64, shareType ShareType) (*truenas.Dataset, error) {
	params := &truenas.DatasetCreateParams{
		Name: datasetName,
	}

	if shareType == ShareTypeNFS {
		// Create filesystem for NFS
		params.Type = "FILESYSTEM"
		if d.config.ZFS.DatasetEnableQuotas {
			params.Refquota = capacityBytes
		}
		if d.config.ZFS.DatasetEnableReservation {
			params.Refreservation = capacityBytes
		}
	} else {
		// Create zvol for iSCSI/NVMe-oF
		params.Type = "VOLUME"
		params.Volsize = capacityBytes
		params.Volblocksize = d.config.ZFS.ZvolBlocksize
		params.Sparse = !d.config.ZFS.ZvolEnableReservation
		if d.config.ZFS.ZvolEnableReservation {
			params.Refreservation = capacityBytes
		}
	}
	// Curated ZFS performance class, layered UNDER zfs.datasetProperties so an
	// explicit operator key always wins. Absent parameter = zero properties and
	// the historical create payload.
	if class := zfsPerformanceClassFromContext(ctx); class != "" {
		curated, resolveErr := d.resolvePerformanceClassProperties(ctx, class, params.Type)
		if resolveErr != nil {
			return nil, resolveErr
		}
		applyPerformanceClassProperties(params, curated)
	}
	d.applyDatasetProperties(params)
	// An NFSv4 dacl can only be applied to an acltype=NFSV4 dataset. Stamp it
	// (plus aclmode=PASSTHROUGH) ONLY when this volume actually requested an ACL;
	// otherwise both stay inherited from the parent, exactly as before.
	applyDatasetACLParams(params, nfsACLOptionsFromContext(ctx))
	postCreateProperties := make(map[string]string, len(params.UserProperties)+1)
	for _, property := range params.UserProperties {
		postCreateProperties[property.Key] = property.Value
	}
	postCreateProperties[PropDriverInstanceID] = d.driverInstanceID()
	// Live TrueNAS 26.0 accepts inline pool.dataset.create user_properties but
	// silently writes none of them. Keep all standard dataset fields on create,
	// then publish ownership and custom user properties through the proven
	// pool.dataset.update user_properties_update path.
	params.UserProperties = nil

	ds, err := d.truenasClient.DatasetCreate(ctx, params)
	if err != nil {
		return nil, err
	}
	if !ds.CreatedByCall {
		if datasetHasLocalUserProperty(ds, PropDriverInstanceID, d.driverInstanceID()) {
			// Another controller instance won the create race. Re-enter through the
			// normal existing-volume path on retry so protocol, source, capacity and
			// name compatibility are all checked, and so this caller can never clean
			// up an object it did not create.
			return nil, status.Errorf(codes.Aborted,
				"dataset %s was concurrently created by this driver instance; retry CreateVolume", datasetName)
		}
		return nil, status.Errorf(codes.AlreadyExists,
			"dataset %s appeared during creation without matching local ownership; refusing to adopt a raced object",
			datasetName)
	}
	verified, stampErr := d.setAndVerifyDatasetUserProperties(ctx, datasetName, postCreateProperties)
	if stampErr != nil {
		if cleanupErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); cleanupErr != nil {
			klog.Warningf("Failed to cleanup newly created dataset %s after ownership stamp failure: %v", datasetName, cleanupErr)
		}
		return nil, status.Errorf(codes.Internal, "failed to stamp and verify dataset ownership after creation: %v", stampErr)
	}
	verified.CreatedByCall = true
	return verified, nil
}

// errDatasetPropertyVerification marks a write that was acknowledged but whose
// re-read did not show our exact values as local. Callers that race concurrent
// writers (the recovery nonce CAS) distinguish this lost-race shape from a
// backend failure.
var errDatasetPropertyVerification = errors.New("dataset user property verification failed")

// mirrorUserProperties records the written properties on the local dataset
// mirror as source=local, matching what pool.dataset.update's
// user_properties_update persists on TrueNAS 26.0. It mirrors the values we
// wrote (which the update response reflects) without a re-read. It is the single
// canonical mirror shared by every property-writing path so the local cache
// cannot drift between them.
func mirrorUserProperties(ds *truenas.Dataset, properties map[string]string) {
	if ds == nil || len(properties) == 0 {
		return
	}
	if ds.UserProperties == nil {
		ds.UserProperties = make(map[string]truenas.UserProperty, len(properties))
	}
	for key, value := range properties {
		ds.UserProperties[key] = truenas.UserProperty{Value: value, Source: "local"}
	}
}

// stampAndMirror is the canonical write-and-cache path for the non-verifying
// property stamps. It persists properties through pool.dataset.update's
// user_properties_update (the batch user-property setter, which the TrueNAS
// 26.0 write path persists with source=local) and mirrors the written keys into
// ds.UserProperties via mirrorUserProperties — trusting the write without a
// re-read. It takes the client explicitly so both Driver methods and the
// package-level publication-record helper share one implementation.
//
// The response-verifying stamps (recovery nonce CAS, ownership stamps) do NOT
// use this helper: they go through setAndVerifyDatasetUserProperties, which
// needs pool.dataset.update's returned dataset to verify against the response
// plus the one-time post-connect paranoia re-read.
func stampAndMirror(ctx context.Context, client truenas.ClientInterface, ds *truenas.Dataset, datasetName string, properties map[string]string) error {
	if err := client.DatasetSetUserProperties(ctx, datasetName, properties); err != nil {
		return err
	}
	mirrorUserProperties(ds, properties)
	return nil
}

// setAndVerifyDatasetProps writes an optional filesystem refquota together with
// user properties through one pool.dataset.update and verifies that every
// requested value persisted with source=local. The widened shape is used by the
// snapshot-clone fold: refquota, content-source identity, and (since Sprint 3
// L2a) the ownership stamp all become durable in one response-verifying update,
// so there is no intermediate state where content-source exists without
// ownership.
func (d *Driver) setAndVerifyDatasetProps(
	ctx context.Context,
	datasetName string,
	refquota interface{},
	properties map[string]string,
) (*truenas.Dataset, error) {
	// Build sorted user-property updates mirroring DatasetSetUserProperties, then
	// call pool.dataset.update directly. Live TrueNAS 26.0 persists these with
	// source=local AND reflects the post-write state in the response, so the
	// returned dataset is authoritative for verification — no fresh re-read on
	// the hot path.
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	updates := make([]truenas.UserPropertyUpdate, 0, len(keys))
	for _, key := range keys {
		updates = append(updates, truenas.UserPropertyUpdate{Key: key, Value: properties[key]})
	}
	updated, err := d.truenasClient.DatasetUpdate(ctx, datasetName, &truenas.DatasetUpdateParams{
		Refquota:             refquota,
		UserPropertiesUpdate: updates,
	})
	if err != nil {
		return nil, err
	}
	if verifyErr := verifyLocalUserProperties(updated, properties); verifyErr != nil {
		return nil, verifyErr
	}
	if verifyErr := verifyLocalRefquota(updated, refquota); verifyErr != nil {
		return nil, verifyErr
	}
	// Belt-and-braces: the very first write in this Driver's lifetime also
	// re-reads the dataset and re-verifies, preserving the paranoia that
	// originally caught the inline-create property drop. Every later write
	// trusts the update response alone, amortizing the extra round trip away.
	if !d.datasetUpdateVerifiedOnce.Load() {
		reread, getErr := d.truenasClient.DatasetGet(ctx, datasetName)
		if getErr != nil {
			return nil, fmt.Errorf("re-read dataset after user-property update: %w", getErr)
		}
		if verifyErr := verifyLocalUserProperties(reread, properties); verifyErr != nil {
			return nil, verifyErr
		}
		if verifyErr := verifyLocalRefquota(reread, refquota); verifyErr != nil {
			return nil, verifyErr
		}
		d.datasetUpdateVerifiedOnce.Store(true)
	}
	return updated, nil
}

// setAndVerifyDatasetUserProperties preserves the property-only call surface
// for ownership, bookkeeping, and recovery stamps.
func (d *Driver) setAndVerifyDatasetUserProperties(ctx context.Context, datasetName string, properties map[string]string) (*truenas.Dataset, error) {
	return d.setAndVerifyDatasetProps(ctx, datasetName, nil, properties)
}

// verifyLocalUserProperties returns an errDatasetPropertyVerification-wrapped
// error unless every property is present on ds with source=local and the exact
// expected value. Callers that race concurrent writers distinguish this shape.
func verifyLocalUserProperties(ds *truenas.Dataset, properties map[string]string) error {
	for key, expected := range properties {
		if !datasetHasLocalUserProperty(ds, key, expected) {
			return fmt.Errorf("%w: property %s did not persist locally with the expected value", errDatasetPropertyVerification, key)
		}
	}
	return nil
}

func verifyLocalRefquota(ds *truenas.Dataset, expected interface{}) error {
	if expected == nil {
		return nil
	}
	expectedBytes, ok := expected.(int64)
	if !ok {
		return fmt.Errorf("%w: unsupported refquota verification type %T", errDatasetPropertyVerification, expected)
	}
	if ds == nil || !isLocalUserPropertySource(ds.Refquota.Source) ||
		datasetPropertyBytes(ds.Refquota) != expectedBytes {
		return fmt.Errorf("%w: refquota did not persist locally with the expected value", errDatasetPropertyVerification)
	}
	return nil
}

// warnDatasetPropertyUnsupportedOn26 names the one failure an operator cannot
// diagnose from the wire error: a zfs.datasetProperties key TrueNAS 26.0 dropped
// from its dataset schema.
func warnDatasetPropertyUnsupportedOn26(key, datasetName string) {
	klog.Warningf("zfs.datasetProperties %q is not in the TrueNAS 26.0 pool.dataset.create schema; on a 26.0 appliance this create "+
		"(%s) fails with an opaque \"Invalid params\". Remove the key and set the property out of band (zfs set %s=... on the parent "+
		"dataset, which new volumes inherit), or keep it only on a pre-26.0 appliance.", key, datasetName, key)
}

func (d *Driver) applyDatasetProperties(params *truenas.DatasetCreateParams) {
	properties := d.config.ZFS.DatasetProperties
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, rawKey := range keys {
		key := strings.TrimSpace(rawKey)
		value := strings.TrimSpace(properties[rawKey])
		normalizedKey := strings.ToLower(key)
		if params.Type == "VOLUME" && (normalizedKey == "atime" || normalizedKey == "recordsize") {
			klog.Warningf("Ignoring filesystem-only zfs.datasetProperties key %q for VOLUME dataset %s", rawKey, params.Name)
			continue
		}
		if params.Type == "FILESYSTEM" && normalizedKey == "volblocksize" {
			klog.Warningf("Ignoring volume-only zfs.datasetProperties key %q for FILESYSTEM dataset %s", rawKey, params.Name)
			continue
		}

		switch normalizedKey {
		case "compression":
			params.Compression = strings.ToUpper(value)
		case "sync":
			params.Sync = strings.ToUpper(value)
		case "atime":
			params.Atime = strings.ToUpper(value)
		case "recordsize":
			params.Recordsize = strings.ToUpper(value)
		case "checksum":
			params.Checksum = strings.ToUpper(value)
		// logbias/primarycache/secondarycache are NOT in the TrueNAS 26.0
		// pool.dataset.create or .update schema (probed live 2026-08-02: "[EINVAL]
		// data.<TYPE>.<key>: Extra inputs are not permitted"), so on 26.0 every
		// create carrying one fails with an opaque "Invalid params". They are still
		// emitted because this driver's documented floor is 25.04 and only 26.0 was
		// probed; the warning is what turns that failure from a mystery into a
		// diagnosis. The curated zfsPerformanceClass presets emit none of them.
		case "logbias":
			params.Logbias = strings.ToUpper(value)
			warnDatasetPropertyUnsupportedOn26(normalizedKey, params.Name)
		case "primarycache":
			params.Primarycache = strings.ToUpper(value)
			warnDatasetPropertyUnsupportedOn26(normalizedKey, params.Name)
		case "secondarycache":
			params.Secondarycache = strings.ToUpper(value)
			warnDatasetPropertyUnsupportedOn26(normalizedKey, params.Name)
		case "snapdir":
			params.Snapdir = strings.ToUpper(value)
		case "special_small_block_size":
			// The correct key is special_small_block_size; pool.dataset.* rejects
			// the commonly mis-typed special_small_blocks. Previously this key fell
			// through to the unknown-key warning and was silently dropped.
			params.SpecialSmallBlockSize = strings.ToUpper(value)
		case "dedup":
			params.Deduplication = strings.ToUpper(value)
		case "readonly":
			params.Readonly = strings.ToUpper(value)
		case "volblocksize":
			if params.Volblocksize != "" {
				klog.Warningf("Ignoring zfs.datasetProperties volblocksize value %q for %s because zfs.zvolBlocksize is %q", value, params.Name, params.Volblocksize)
				continue
			}
			params.Volblocksize = strings.ToUpper(value)
		case "copies":
			copies, err := strconv.Atoi(value)
			if err != nil || copies < 1 {
				klog.Warningf("Ignoring invalid zfs.datasetProperties copies value %q", value)
				continue
			}
			params.Copies = copies
		case "":
			klog.Warning("Ignoring empty zfs.datasetProperties key")
			continue
		default:
			if strings.Contains(key, ":") {
				params.UserProperties = append(params.UserProperties, truenas.UserPropertyUpdate{Key: key, Value: value})
			} else {
				klog.Warningf("Ignoring unknown zfs.datasetProperties key %q", rawKey)
				continue
			}
		}
		klog.V(2).Infof("Applying zfs.datasetProperties %s=%q to %s", key, value, params.Name)
	}
}

// snapshotRestoreModeParam is the StorageClass parameter that selects how a
// volume is provisioned from a snapshot content source.
const snapshotRestoreModeParam = "snapshotRestoreMode"

// snapshotRestoreDetached resolves whether a snapshot-sourced CreateVolume
// builds an independent detached copy (true) or a cheap ZFS clone (false).
// Resolution order: the StorageClass `snapshotRestoreMode` parameter when set
// (`clone`|`detached`, plus legacy boolean spellings), else the global
// zfs.detachedVolumesFromSnapshots default. An unrecognized value is rejected so
// a misconfigured StorageClass surfaces instead of silently picking a path. This
// lets DR-restore classes opt into independent copies while the dominant hourly
// VolSync source-backup mounts stay cheap clones.
func (d *Driver) snapshotRestoreDetached(params map[string]string) (bool, error) {
	if params != nil {
		if raw, ok := params[snapshotRestoreModeParam]; ok {
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "detached", "copy", "true", "enabled":
				return true, nil
			case "clone", "false", "disabled":
				return false, nil
			default:
				return false, status.Errorf(codes.InvalidArgument,
					"invalid StorageClass parameter %q value %q; valid options are: clone, detached",
					snapshotRestoreModeParam, raw)
			}
		}
	}
	return d.config.ZFS.DetachedVolumesFromSnapshots, nil
}

// inheritedProtocolPropertyKeys are the per-volume backend share-object IDs that
// ZFS may inherit from a clone's source dataset. The scrub removes only
// source-proven inherited IDs belonging to OTHER protocols: local values are
// authoritative, and the selected protocol's resolver owns repair of its own
// stale backreferences. The scrub runs best-effort after the ownership stamp.
var inheritedProtocolPropertyKeys = []string{
	PropNFSShareID,
	PropISCSITargetID,
	PropISCSIExtentID,
	PropISCSITargetExtentID,
	PropISCSIInitiatorID,
	PropISCSIAuthTag,
	PropISCSIAuthMode,
	PropNVMeoFSubsystemID,
	PropNVMeoFNamespaceID,
	PropNVMeoFPortSubsysID,
}

// scrubInheritedCloneProperties removes the user properties a freshly
// materialized content-source volume must not keep, in ONE pool.dataset.update:
//
//   - provably inherited backend share-object IDs from protocols foreign to
//     shareType. Local properties and all current-protocol properties survive;
//     the protocol-specific backreference resolver repairs same-protocol stale
//     IDs.
//   - PropZFSPerformanceClass, UNCONDITIONALLY when present (H1). This volume
//     was materialized from a content source in this very call, so the driver
//     never applied a curated class to it: any class stamp it carries was copied
//     from the origin and asserts geometry that was never applied here. The
//     source qualifier the protocol keys use does NOT apply — a detached
//     replication copy reproduces the source's properties as LOCAL values, so a
//     source-based filter would let exactly that path keep the lie.
//
// It is idempotent and best-effort. Best-effort is safe for the class stamp
// because the immutability guard independently refuses to treat a content-source
// volume's stamp as authoritative (see createVolumeExisting): the scrub keeps
// the on-disk record honest, the guard keeps the BEHAVIOR honest even if the
// scrub could not run.
func (d *Driver) scrubInheritedCloneProperties(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType) {
	if ds == nil {
		return
	}
	currentProtocol := map[string]struct{}{}
	knownProtocol := true
	switch shareType {
	case ShareTypeNFS:
		currentProtocol[PropNFSShareID] = struct{}{}
	case ShareTypeISCSI:
		currentProtocol[PropISCSITargetID] = struct{}{}
		currentProtocol[PropISCSIExtentID] = struct{}{}
		currentProtocol[PropISCSITargetExtentID] = struct{}{}
		currentProtocol[PropISCSIInitiatorID] = struct{}{}
		// PropISCSIAuthTag/PropISCSIAuthMode are deliberately NOT current-protocol:
		// CHAP identity is POLICY, not a same-protocol share-object back-reference.
		// An iSCSI->iSCSI clone must NOT inherit the source's CHAP tag/mode, so the
		// scrub removes any inherited CHAP props here. When the CURRENT CreateVolume
		// request itself resolves CHAP, createISCSIShareForDataset re-stamps the
		// request's own tag/mode as a local property afterward.
	case ShareTypeNVMeoF:
		currentProtocol[PropNVMeoFSubsystemID] = struct{}{}
		currentProtocol[PropNVMeoFNamespaceID] = struct{}{}
		currentProtocol[PropNVMeoFPortSubsysID] = struct{}{}
	default:
		// An unrecognized share type cannot decide which backend IDs are foreign,
		// but the class stamp below is protocol-independent and still has to go.
		knownProtocol = false
	}
	present := make([]string, 0, len(inheritedProtocolPropertyKeys)+1)
	if knownProtocol {
		for _, key := range inheritedProtocolPropertyKeys {
			property, ok := ds.UserProperties[key]
			if !ok {
				continue
			}
			if _, ownProtocol := currentProtocol[key]; ownProtocol {
				continue
			}
			source := strings.TrimSpace(property.Source)
			if source == "" || isLocalUserPropertySource(source) {
				continue
			}
			present = append(present, key)
		}
	}
	// H1: unconditional, source-independent. See the doc comment.
	if _, stamped := ds.UserProperties[PropZFSPerformanceClass]; stamped {
		present = append(present, PropZFSPerformanceClass)
	}
	if len(present) == 0 {
		return
	}
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, datasetName, present); err != nil {
		klog.Warningf("Failed to scrub inherited properties %v from content-source volume %s "+
			"(reconcile will reconcile the backreference; the performance-class guard independently ignores an inherited class stamp): %v",
			present, datasetName, err)
		return
	}
	for _, key := range present {
		delete(ds.UserProperties, key)
	}
}

// cloneReadyRetryDelay is the single bounded retry gap used when confirming a
// freshly cloned dataset is queryable (L1b). The live probe showed clones are
// immediately queryable, so this guards transient load only — it is deliberately
// NOT an exponential poll.
const cloneReadyRetryDelay = 250 * time.Millisecond

// confirmCloneReady confirms a freshly cloned dataset is queryable. TrueNAS 26.0
// returns pool.snapshot.clone synchronously queryable (probe-verified on nas01:
// 10/10 scratch clones immediately queryable with a populated volsize/type and
// zero retries), so a single DatasetGet suffices; one bounded retry guards the
// load conditions the probe did not model. When requireVolsize is set (zvol
// clones) the get must also show type=VOLUME with a populated volsize. Filesystem
// clones pass requireVolsize=false and return on the first successful get.
func (d *Driver) confirmCloneReady(ctx context.Context, datasetName string, timeout time.Duration, requireVolsize bool) (*truenas.Dataset, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			delay := cloneReadyRetryDelay
			if remaining := time.Until(deadline); remaining < delay {
				if remaining <= 0 {
					break
				}
				delay = remaining
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context canceled confirming clone %s readiness: %w", datasetName, ctx.Err())
			case <-time.After(delay):
			}
		}
		ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
		if err != nil {
			lastErr = err
			continue
		}
		if requireVolsize {
			if ds.Type != "VOLUME" {
				lastErr = fmt.Errorf("clone %s is type %s, expected VOLUME", datasetName, ds.Type)
				continue
			}
			if volsize, ok := ds.Volsize.Parsed.(float64); !ok || volsize <= 0 {
				lastErr = fmt.Errorf("clone %s zvol has no populated volsize yet", datasetName)
				continue
			}
		}
		return ds, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("clone %s was not queryable before the readiness deadline", datasetName)
	}
	return nil, fmt.Errorf("confirming clone %s readiness: %w", datasetName, lastErr)
}

// cloneReadinessExhaustedStatus maps a confirmCloneReady exhaustion to the gRPC
// code the external-provisioner retries in background. A genuine readiness miss
// is codes.Unavailable: external-provisioner v6.3.0 checkError maps Internal to
// ProvisioningFinished (terminal), which would abandon a clone that simply needs
// a moment to become queryable, whereas Unavailable maps to
// ProvisioningInBackground. A context cancellation or deadline is preserved as the
// actual cause so the CO sees the real reason rather than a generic Unavailable.
func cloneReadinessExhaustedStatus(ctx context.Context, datasetName string, err error) error {
	code := codes.Unavailable
	switch {
	case ctx.Err() == context.Canceled || errors.Is(err, context.Canceled):
		code = codes.Canceled
	case ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	}
	return status.Errorf(code, "failed waiting for cloned volume %s to become ready: %v", datasetName, err)
}

// handleVolumeContentSource clones/copies the requested content source into
// datasetName.
//
// It returns, alongside the created dataset, the SOURCE's resolved block
// geometry as ONE tri-state record (geometryUnexamined for NFS). Cloned data is
// addressed through the geometry it was WRITTEN against, so the destination must
// record that geometry rather than inherit whatever the controller-wide default
// happens to be when its extent is created. Each branch folds the rendered
// properties into the atomic write it already performs; the detached branch also
// hands them back because its ownership stamp is issued by the caller. The
// record itself (not just its properties) travels to the share builder, because
// "no history" and "unknown" render identically as no properties and mean
// opposite things.
func (d *Driver) handleVolumeContentSource(
	ctx context.Context,
	datasetName, volumeName string,
	source *csi.VolumeContentSource,
	capacityBytes int64,
	shareType ShareType,
	detached bool,
	detachedCopyJobID *int64,
) (*truenas.Dataset, blockGeometry, error) {
	// Timeout for waiting for cloned dataset to be ready (configurable via zfs.zvolReadyTimeout)
	cloneReadyTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
	var createdDS *truenas.Dataset
	var resolvedGeometry blockGeometry
	var sourceGeometry map[string]string

	if snapshot := source.GetSnapshot(); snapshot != nil {
		// Create from snapshot using either the legacy clone or the gated
		// independent local send/receive path.
		snapshotID := snapshot.GetSnapshotId()
		if _, err := d.datasetForID(snapshotID); err != nil {
			return nil, blockGeometry{}, err
		}
		klog.Infof("Creating volume from snapshot: %s -> %s", snapshotID, datasetName)

		// Find the snapshot using efficient query (PERF-001 fix)
		snap, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
		if err != nil {
			return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
		}

		if snap == nil {
			return nil, blockGeometry{}, status.Errorf(codes.NotFound, "snapshot not found: %s", snapshotID)
		}

		sourceSnapshot := snap.ID
		// N-1: fail a geometry-changing restore closed BEFORE the first
		// destination mutation. The clone fold below stamps the REQUEST's own
		// block options onto the destination, so by the time the share builder
		// runs, guardStoredBlockGeometry compares the request against itself and
		// always agrees — the inherited source geometry has already been
		// overwritten. The SOURCE's real geometry is the only honest comparand and
		// it has to be read here, before anything is written.
		//
		// Round 4: for an iSCSI restore this runs whether or not the class
		// opts into a geometry. A no-opts restore still creates an extent, and
		// before this that extent was created at whatever the controller-wide
		// default (`iscsi.extentBlocksize`) happened to be — laid over data cloned
		// byte-for-byte from a source that may have been written against something
		// else entirely. The resolved source geometry is stamped onto the
		// destination in the folds below, so no later rebuild consults the default
		// either. NVMe-oF and NFS pay nothing: the resolver short-circuits on
		// share type.
		//
		// Round 5: for iSCSI, the SNAPSHOT is passed, not just its dataset name.
		// The bytes being restored are the snapshot's, and the source dataset's
		// current stamp and current live extent describe the source NOW — a source whose extent
		// was re-created at a different geometry after this snapshot was taken
		// would otherwise hand the restore a geometry its data was never written
		// against. Provenance now comes from the stamp the snapshot itself
		// captured.
		var geometryErr error
		resolvedGeometry, geometryErr = d.resolveCloneSourceBlockGeometry(ctx, snap.Dataset, nil, snap, snapshotID, datasetName, shareType)
		if geometryErr != nil {
			return nil, blockGeometry{}, geometryErr
		}
		if resolvedGeometry.knowledge == geometryUnknown {
			// Fail closed BEFORE the first destination mutation rather than letting
			// the share builder discover it after a dataset exists.
			return nil, blockGeometry{}, d.unknownGeometryError(datasetName, resolvedGeometry.provenance)
		}
		sourceGeometry = resolvedGeometry.props()
		// Durable in-flight provenance BEFORE the first destination mutation. A
		// crash between clone/copy and the ownership stamp leaves a dataset with
		// no local identity; only this marker lets a retry prove the remnant is
		// ours and recover it instead of wedging on terminal AlreadyExists.
		marker, markerErr := d.newInflightMarker(datasetName, source, shareType)
		if markerErr != nil {
			return nil, blockGeometry{}, markerErr
		}
		if detached {
			marker.Mode = inflightModeCopy
		} else {
			marker.Origin = snap.ID
		}
		if markerWriteErr := d.writeInflightMarker(ctx, marker); markerWriteErr != nil {
			return nil, blockGeometry{}, markerWriteErr
		}
		if detached {
			klog.V(4).Infof("Found snapshot %s for independent local copy", sourceSnapshot)
			jobID, copyErr := d.truenasClient.CopyDatasetFromSnapshotLocal(ctx, snap.Dataset, snap.Name, datasetName)
			if copyErr != nil {
				if truenas.IsDatasetDestinationExistsError(copyErr) {
					// Deliberately KEEP the shared per-volume marker: the concurrent
					// winner is the same driver instance mid-flight on the same
					// name+source, and if it crashes before its ownership stamp the
					// surviving marker is what lets a retry recover its remnant. The
					// winner's post-stamp delete and the reconciler sweep retire it.
					return nil, blockGeometry{}, status.Errorf(codes.Aborted,
						"detached snapshot copy destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
				}
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to copy snapshot into an independent volume: %v", copyErr)
			}
			// The low-level copy owns abort-on-error while it runs. Once it
			// succeeds, publish the ID immediately so CreateVolume's fail-path
			// defer covers every subsequent readiness/share/property failure.
			if detachedCopyJobID != nil {
				*detachedCopyJobID = jobID
			}
			klog.Infof("Independent snapshot copy created: %s -> %s", sourceSnapshot, datasetName)
		} else {
			klog.V(4).Infof("Found snapshot %s for cloning", sourceSnapshot)

			if cloneErr := d.truenasClient.SnapshotClone(ctx, sourceSnapshot, datasetName); cloneErr != nil {
				if truenas.IsDatasetDestinationExistsError(cloneErr) {
					// KEEP the shared marker: the same-instance winner may still need
					// it for crash recovery (see the detached branch above).
					return nil, blockGeometry{}, status.Errorf(codes.Aborted,
						"snapshot clone destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
				}
				d.deleteInflightMarker(ctx, path.Base(datasetName))
				return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to clone snapshot: %v", cloneErr)
			}
			klog.Infof("Snapshot clone created: %s -> %s", sourceSnapshot, datasetName)
		}

		if detached {
			// Detached snapshot copies use the replication path, which the L1b
			// synchronous-clone probe did NOT cover; keep the (L1a-tuned) readiness
			// poll. prepareDetachedSnapshotCopy needs the dataset.
			createdDS, err = d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
			if err != nil {
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed waiting for detached snapshot copy to become ready: %v", err)
			}
			createdDS, err = d.prepareDetachedSnapshotCopy(
				ctx, datasetName, createdDS, volumeName, snapshotID, snap.Name, capacityBytes, shareType,
			)
			if err != nil {
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, blockGeometry{}, err
			}
		} else {
			// Sprint 3 (L1b): TrueNAS 26.0 pool.snapshot.clone is synchronously
			// queryable (probe-verified: 10/10 clones immediately queryable, zero
			// retries). Zvol clones confirm volsize with a single bounded get; this is
			// critical for iSCSI/NVMe-oF where extent creation needs the volsize.
			// Filesystem clones trust the clone response and take their dataset from
			// the merged property update below — no readiness round trip at all.
			if shareType.IsBlockProtocol() {
				createdDS, err = d.confirmCloneReady(ctx, datasetName, cloneReadyTimeout, true)
				if err != nil {
					return nil, blockGeometry{}, d.guardedCleanupFailedSnapshotClone(ctx, datasetName, &marker,
						cloneReadinessExhaustedStatus(ctx, datasetName, err))
				}
				if err := d.ensureCloneCapacity(ctx, datasetName, createdDS, capacityBytes); err != nil {
					return nil, blockGeometry{}, d.guardedCleanupFailedSnapshotClone(ctx, datasetName, &marker, err)
				}
			}
			// The clone's dataset type is fixed by the share type (NFS->filesystem,
			// block->zvol), so the filesystem refquota decision no longer needs a
			// readiness read of createdDS.Type.
			var refquota interface{}
			if !shareType.IsBlockProtocol() && d.config.ZFS.DatasetEnableQuotas && capacityBytes > 0 {
				refquota = capacityBytes
			}
			// Sprint 3 (L2a): refquota + content-source identity + the ownership
			// stamp persist in ONE atomic pool.dataset.update (single ZFS txg;
			// DatasetUpdate sets every user property atomically). This REMOVES the
			// old content-source-vs-ownership crash window by making the two durable
			// simultaneously rather than weakening it: a crash before this write
			// leaves marker+clone with no ownership (recoverable, identical to the
			// old "crash before #7"); a crash after it leaves a complete owned volume
			// with the marker still present, which the reconciler sweep retires
			// (identical to the old "crash after #8, before #9"). The marker is still
			// written before the clone and retired only after this write is durable.
			// For filesystem clones this update also yields the createdDS the caller
			// publishes (L1b removed their separate readiness read).
			//
			// Sprint 6 (H1): a CHAP-resolved iSCSI clone folds its durable CHAP
			// policy (PropISCSIAuthTag + PropISCSIAuthMode) into this SAME atomic
			// write, so ownership + content-source + CHAP become durable in one txg
			// — matching the fresh path, which stamps CHAP atomically with
			// ownership. Without this, a crash between the early ownership fold and
			// the late fatal managed-property stamp left an owned dataset with
			// stored CHAP=NONE; guardExistingISCSICHAPPolicy then rejected every
			// retry forever (stored NONE vs request CHAP), wedging the PVC. nil for
			// non-CHAP requests, so non-CHAP clones are byte-for-byte unchanged.
			foldProps := map[string]string{
				PropVolumeContentSourceType: "snapshot",
				PropVolumeContentSourceID:   snapshotID,
				PropDriverInstanceID:        d.driverInstanceID(),
			}
			for key, value := range iscsiCHAPPolicyProps(ctx, shareType) {
				foldProps[key] = value
			}
			// GF-4: the resolved block tuning folds into this SAME atomic write for
			// the CHAP rationale above — the clone's share is built from these
			// properties on every later rebuild, so they must be durable with
			// ownership rather than only at the late fatal stamp. nil for requests
			// that opt into nothing, so default clones are byte-for-byte unchanged.
			for key, value := range blockOptsProps(ctx, shareType) {
				foldProps[key] = value
			}
			// GF-4 round 4, mechanism (3): the SOURCE's real geometry, recorded on
			// the destination in the same atomic write. This is what a no-opts
			// restore of an unstamped source now inherits instead of the controller
			// default, and it is also what stops a no-opts hop from LAUNDERING a
			// wrong geometry into the next restore's ground truth. Written BEFORE the
			// share builder runs, so the extent below is created from it. Where the
			// request also sets a geometry the two agree by construction — the guard
			// above rejected every case where they would not.
			for key, value := range sourceGeometry {
				foldProps[key] = value
			}
			verified, updateErr := d.setAndVerifyDatasetProps(ctx, datasetName, refquota, foldProps)
			if updateErr != nil {
				return nil, blockGeometry{}, d.guardedCleanupFailedSnapshotClone(ctx, datasetName, &marker, updateErr)
			}
			createdDS = verified
		}

	} else if volume := source.GetVolume(); volume != nil {
		// Clone from volume
		sourceVolumeID := volume.GetVolumeId()
		sourceDataset, err := d.datasetForID(sourceVolumeID)
		if err != nil {
			return nil, blockGeometry{}, err
		}
		klog.Infof("Creating volume from volume: %s -> %s", sourceVolumeID, datasetName)

		sourceDS, getErr := d.truenasClient.DatasetGet(ctx, sourceDataset)
		if getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				return nil, blockGeometry{}, status.Errorf(codes.NotFound, "source volume not found: %s", sourceVolumeID)
			}
			return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to get source volume: %v", getErr)
		}
		// N-1 (volume-clone flavor). This existence probe was already querying the
		// source, so reusing its result saves the DatasetGet: the only extra call
		// here is the source's live-extent read, which is what tells the driver what
		// the cloned data is actually addressed through.
		//
		// Round 5: no snapshot is passed because the temporary source snapshot is
		// taken from the source's CURRENT state moments below, so "what the source
		// is addressed through now" is exactly the right question here.
		var geometryErr error
		resolvedGeometry, geometryErr = d.resolveCloneSourceBlockGeometry(ctx, sourceDataset, sourceDS, nil, sourceVolumeID, datasetName, shareType)
		if geometryErr != nil {
			return nil, blockGeometry{}, geometryErr
		}
		if resolvedGeometry.knowledge == geometryUnknown {
			return nil, blockGeometry{}, d.unknownGeometryError(datasetName, resolvedGeometry.provenance)
		}
		sourceGeometry = resolvedGeometry.props()

		// Create a snapshot of source volume, then clone it
		tempSnapshotName := fmt.Sprintf("clone-source-%s", sanitizeVolumeID(path.Base(datasetName)))
		// Durable in-flight provenance BEFORE any mutation; the recorded origin is
		// the deterministic internal snapshot the clone must descend from.
		marker, markerErr := d.newInflightMarker(datasetName, source, shareType)
		if markerErr != nil {
			return nil, blockGeometry{}, markerErr
		}
		marker.Origin = sourceDataset + "@" + tempSnapshotName
		if markerWriteErr := d.writeInflightMarker(ctx, marker); markerWriteErr != nil {
			return nil, blockGeometry{}, markerWriteErr
		}
		snap, err := d.truenasClient.SnapshotCreate(ctx, sourceDataset, tempSnapshotName, map[string]string{
			PropInternalResource: "true",
		})
		if err != nil {
			d.deleteInflightMarker(ctx, path.Base(datasetName))
			return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to create source snapshot: %v", err)
		}
		klog.V(4).Infof("Created temporary snapshot %s for volume clone", snap.ID)
		// Internal snapshots are deliberately not marked as CSI-managed. Their
		// snapshot-only marker is written atomically at creation and lets
		// DeleteVolume reject source deletion before its share is touched.

		if cloneErr := d.truenasClient.SnapshotClone(ctx, snap.ID, datasetName); cloneErr != nil {
			if truenas.IsDatasetDestinationExistsError(cloneErr) {
				// The winning clone may depend on the same deterministic temporary
				// snapshot. Do not delete either object; its CreateVolume path owns
				// completion and the retry will pass through the full ownership gate.
				// KEEP the shared marker too: the same-instance winner may still
				// need it for crash recovery if it dies before its ownership stamp.
				return nil, blockGeometry{}, status.Errorf(codes.Aborted,
					"volume clone destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
			}
			d.deleteInflightMarker(ctx, path.Base(datasetName))
			if delErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); delErr != nil {
				klog.Warningf("Failed to cleanup snapshot after clone failure: %v", delErr)
			}
			return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to clone volume: %v", cloneErr)
		}
		klog.Infof("Volume clone created: %s -> %s", sourceVolumeID, datasetName)

		// Sprint 3 (L1b): the volume clone descends from pool.snapshot.clone (via the
		// temporary source snapshot), which the probe verified is synchronously
		// queryable. Confirm with a single bounded get instead of an exponential poll;
		// the dataset is still fetched here because ensureCloneCapacity needs it.
		createdDS, err = d.confirmCloneReady(ctx, datasetName, cloneReadyTimeout, shareType.IsBlockProtocol())
		if err != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, blockGeometry{}, cloneReadinessExhaustedStatus(ctx, datasetName, err)
		}
		if err := d.ensureCloneCapacity(ctx, datasetName, createdDS, capacityBytes); err != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, blockGeometry{}, err
		}

		// Include origin snapshot so it can be cleaned up when the clone is deleted.
		// Sprint 3 (L2a): the ownership stamp folds into this same content-source
		// write so content-source identity and ownership become durable in one atomic
		// pool.dataset.update (single txg), removing the intermediate state where one
		// existed without the other. The marker is still retired only after this write
		// succeeds (see the shared ownership gate in CreateVolume).
		//
		// Sprint 3 fix: this fold MUST be response-verifying. The ownership stamp is
		// the only thing that makes the clone recoverable, so an acknowledged update
		// that silently dropped PropDriverInstanceID would otherwise let CreateVolume
		// retire the in-flight marker and strand a markerless/ownerless dataset that
		// is invisible to marker recovery AND remnant GC AND the managed-keyed list —
		// a permanent invisible leak. setAndVerifyDatasetUserProperties checks every
		// key (ownership included) against the update RESPONSE with no extra round
		// trip, matching the snapshot-clone fold and the stampAndMirror contract that
		// ownership stamps never use the non-verifying path. A verification failure
		// returns before CreateVolume retires the marker, and cleanupFailedClone keeps
		// the marker unless the destination is verifiably gone, so no markerless
		// remnant is ever left behind. The verified dataset is the one published.
		//
		// Sprint 6 (H1): a CHAP-resolved iSCSI volume clone folds its durable CHAP
		// policy into this same atomic write (see the snapshot-clone fold above for
		// the full crash-window rationale), so ownership + content-source + CHAP are
		// durable in one txg and a crash before the late fatal stamp can no longer
		// wedge guardExistingISCSICHAPPolicy. nil for non-CHAP requests.
		foldProps := map[string]string{
			PropVolumeContentSourceType: "volume",
			PropVolumeContentSourceID:   sourceVolumeID,
			PropVolumeOriginSnapshot:    snap.ID,
			PropDriverInstanceID:        d.driverInstanceID(),
		}
		for key, value := range iscsiCHAPPolicyProps(ctx, shareType) {
			foldProps[key] = value
		}
		// GF-4: same atomic write for the resolved block tuning (see the
		// snapshot-clone fold above). nil for requests that opt into nothing.
		for key, value := range blockOptsProps(ctx, shareType) {
			foldProps[key] = value
		}
		// GF-4 round 4, mechanism (3): the SOURCE's real geometry, recorded on the
		// destination in the same atomic write (see the snapshot-clone fold above).
		for key, value := range sourceGeometry {
			foldProps[key] = value
		}
		verified, updateErr := d.setAndVerifyDatasetUserProperties(ctx, datasetName, foldProps)
		if updateErr != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, blockGeometry{}, status.Errorf(codes.Internal, "failed to set content source properties for volume clone: %v", updateErr)
		}
		createdDS = verified
	}

	return createdDS, resolvedGeometry, nil
}

func (d *Driver) abortReplicationJobBestEffort(ctx context.Context, jobID int64, reason string) {
	if d.truenasClient == nil || jobID == truenas.UnknownReplicationJobID {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedCopyJobAbortTimeout)
	defer cancel()
	if err := d.truenasClient.ReplicationJobAbort(abortCtx, jobID, reason); err != nil {
		klog.Warningf("Failed to abort one-time replication job %d after CreateVolume failure: %v", jobID, err)
		return
	}
	klog.Infof("Aborted one-time replication job %d after CreateVolume failure", jobID)
}

func (d *Driver) cleanupFailedClone(ctx context.Context, datasetName, tempSnapshotID string) {
	destroyErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true)
	if destroyErr != nil {
		klog.Warningf("Failed to cleanup clone dataset %s: %v", datasetName, destroyErr)
	}
	if tempSnapshotID != "" {
		if err := d.truenasClient.SnapshotDelete(ctx, tempSnapshotID, false, false); err != nil {
			klog.Warningf("Failed to cleanup temporary clone-source snapshot %s: %v", tempSnapshotID, err)
		}
	}
	// Retire the in-flight marker ONLY when the destination is verifiably gone.
	// A failed cleanup destroy (e.g. a partial detached copy still holding its
	// transferred snapshot blocks the non-recursive delete) must KEEP the marker:
	// it is the retry's only proof of provenance for recovering the remnant, and
	// deleting it here would leave an unrecoverable, unmarked leak.
	if destroyErr == nil {
		d.deleteInflightMarker(ctx, path.Base(datasetName))
		return
	}
	if _, getErr := d.truenasClient.DatasetGet(ctx, datasetName); truenas.IsNotFoundError(getErr) {
		d.deleteInflightMarker(ctx, path.Base(datasetName))
		return
	}
	klog.Warningf("Keeping in-flight marker for %s: cleanup destroy failed and the remnant may still exist; a retry will recover it", datasetName)
}

// guardedCleanupFailedSnapshotClone destroys a failed snapshot-clone attempt
// only while the destination and its durable marker still prove they belong to
// this exact attempt. operationLock is process-local, so a peer controller may
// have recovered and stamped the clone while this process was handling an
// error. Any local ownership stamp, missing marker, changed nonce/identity, or
// failed guard read is a lost race: refuse deletion and return retryable
// Aborted. Ownership is intentionally checked before marker identity because a
// peer removes the marker only after its ownership stamp is durable.
func (d *Driver) guardedCleanupFailedSnapshotClone(
	ctx context.Context,
	datasetName string,
	expected *inflightMarker,
	cause error,
) error {
	dataset, getErr := d.truenasClient.DatasetGet(ctx, datasetName)
	if getErr != nil {
		if truenas.IsNotFoundError(getErr) {
			d.cleanupFailedClone(ctx, datasetName, "")
			return snapshotCloneFailureStatus(datasetName, cause)
		}
		return status.Errorf(codes.Aborted,
			"cannot verify failed snapshot clone %s before cleanup; refusing delete and retry CreateVolume: %v",
			datasetName, cause)
	}
	if datasetHasLocalOwnershipStamp(dataset) {
		return status.Errorf(codes.Aborted,
			"lost snapshot-clone cleanup race for %s (now owned); refusing delete and retry CreateVolume: %v",
			datasetName, cause)
	}
	marker, markerErr := d.readInflightMarker(ctx, path.Base(datasetName))
	if markerErr != nil || marker == nil || expected == nil ||
		marker.Version != expected.Version ||
		marker.Instance != expected.Instance ||
		marker.Dataset != expected.Dataset ||
		marker.Mode != expected.Mode ||
		marker.SourceType != expected.SourceType ||
		marker.SourceID != expected.SourceID ||
		marker.Origin != expected.Origin ||
		marker.Protocol != expected.Protocol ||
		marker.Nonce != expected.Nonce ||
		dataset.Name != expected.Dataset ||
		datasetOriginSnapshotID(dataset) != expected.Origin {
		return status.Errorf(codes.Aborted,
			"snapshot-clone cleanup identity changed for %s; refusing delete and retry CreateVolume: %v",
			datasetName, cause)
	}
	d.cleanupFailedClone(ctx, datasetName, "")
	return snapshotCloneFailureStatus(datasetName, cause)
}

// snapshotCloneFailureStatus builds the terminal status for a failed snapshot
// clone that guardedCleanupFailedSnapshotClone has proven safe to clean up. It
// preserves the cause's gRPC code when the cause already carries one — so a
// readiness exhaustion stays codes.Unavailable, which external-provisioner v6.3.0
// retries in background — and defaults to codes.Internal for raw backend errors
// (the merged-update and capacity failures). The Aborted lost-race returns above
// never route through here.
func snapshotCloneFailureStatus(datasetName string, cause error) error {
	code := codes.Internal
	if st, ok := status.FromError(cause); ok && st.Code() != codes.Unknown {
		code = st.Code()
	}
	return status.Errorf(code, "failed snapshot clone %s: %v", datasetName, cause)
}

func datasetPropertyBytes(property truenas.DatasetProperty) int64 {
	switch value := property.Parsed.(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	case uint64:
		if value <= ^uint64(0)>>1 {
			return int64(value)
		}
	}
	return 0
}

func (d *Driver) ensureCloneCapacity(ctx context.Context, datasetName string, ds *truenas.Dataset, capacityBytes int64) error {
	if ds == nil {
		return status.Error(codes.Internal, "cloned volume became ready without dataset details")
	}

	switch ds.Type {
	case "VOLUME":
		currentSize := d.getDatasetCapacity(ds)
		if capacityBytes > currentSize {
			klog.Infof("Expanding cloned zvol from %d to %d bytes", currentSize, capacityBytes)
			if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
				return status.Errorf(codes.Internal, "failed to expand cloned volume: %v", err)
			}
		}
	case "FILESYSTEM":
		if d.config.ZFS.DatasetEnableQuotas {
			params := &truenas.DatasetUpdateParams{Refquota: capacityBytes}
			if _, err := d.truenasClient.DatasetUpdate(ctx, datasetName, params); err != nil {
				return status.Errorf(codes.Internal, "failed to set cloned volume quota: %v", err)
			}
		}
	}

	return nil
}

func (d *Driver) getVolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType) (map[string]string, error) {
	volumeContext := map[string]string{
		"node_attach_driver": shareType.String(),
	}

	if ds == nil {
		var err error
		ds, err = d.truenasClient.DatasetGet(ctx, datasetName)
		if err != nil {
			return nil, err
		}
	}

	if backend := backendForShareType(d, shareType); backend != nil {
		if err := backend.VolumeContext(ctx, ds, datasetName, volumeContext); err != nil {
			return nil, err
		}
	}

	return volumeContext, nil
}

func timestampProto(unixSeconds int64) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: unixSeconds,
	}
}

// extractSnapshotName safely extracts the snapshot name from a ZFS snapshot ID.
// ZFS snapshot IDs are in format "dataset@snapshotname".
// Returns the snapshot name and true if valid, empty string and false if invalid.
func extractSnapshotName(snapshotID string) (string, bool) {
	parts := strings.Split(snapshotID, "@")
	if len(parts) != 2 {
		return "", false
	}
	return path.Base(parts[1]), true
}

// getAccessibleTopology returns the topology segments where volumes are accessible.
// For a single TrueNAS backend, all volumes are accessible from any node that can
// reach TrueNAS over the network. The topology returned matches the node's configured
// topology, indicating the volume is accessible from that topology segment.
func (d *Driver) getAccessibleTopology() []*csi.Topology {
	if !d.config.Node.Topology.Enabled {
		return nil
	}

	segments := make(map[string]string)

	if d.config.Node.Topology.Zone != "" {
		segments["topology.kubernetes.io/zone"] = d.config.Node.Topology.Zone
	}
	if d.config.Node.Topology.Region != "" {
		segments["topology.kubernetes.io/region"] = d.config.Node.Topology.Region
	}
	for k, v := range d.config.Node.Topology.CustomLabels {
		segments[k] = v
	}

	if len(segments) == 0 {
		return nil
	}

	return []*csi.Topology{{Segments: segments}}
}
