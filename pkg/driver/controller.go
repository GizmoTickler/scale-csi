package driver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Import truenas package for error helper functions (aliased for clarity in error checks)

// ZFS property names for tracking CSI resources
const (
	PropManagedResource           = "truenas-csi:managed_resource"
	PropDriverInstanceID          = "truenas-csi:driver_instance_id"
	PropProvisionSuccess          = "truenas-csi:provision_success"
	PropCSIVolumeName             = "truenas-csi:csi_volume_name"
	PropShareVolumeContext        = "truenas-csi:csi_share_volume_context"
	PropVolumeContentSourceType   = "truenas-csi:csi_volume_content_source_type"
	PropVolumeContentSourceID     = "truenas-csi:csi_volume_content_source_id"
	PropVolumeOriginSnapshot      = "truenas-csi:csi_volume_origin_snapshot" // temp snapshot created during volume-to-volume cloning
	PropInternalResource          = "truenas-csi:internal_resource"          // internal snapshots that must not be exposed through ListSnapshots
	PropRequestedSizeBytes        = "truenas-csi:requested_size_bytes"       // requested capacity for quota-less filesystem volumes
	PropCSISnapshotName           = "truenas-csi:csi_snapshot_name"
	PropCSISnapshotSourceVolumeID = "truenas-csi:csi_snapshot_source_volume_id"
	snapshotTombstoneMarker       = "-csi-deleted-"
	PropNFSShareID                = "truenas-csi:truenas_nfs_share_id"
	PropISCSITargetID             = "truenas-csi:truenas_iscsi_target_id"
	PropISCSIExtentID             = "truenas-csi:truenas_iscsi_extent_id"
	PropISCSITargetExtentID       = "truenas-csi:truenas_iscsi_targetextent_id"
	PropISCSIInitiatorID          = "truenas-csi:truenas_iscsi_initiator_id"
	PropNVMeoFSubsystemID         = "truenas-csi:truenas_nvmeof_subsystem_id"
	PropNVMeoFNamespaceID         = "truenas-csi:truenas_nvmeof_namespace_id"
	PropNVMeoFPortSubsysID        = "truenas-csi:truenas_nvmeof_portsubsys_id"

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
	tombstoneLedgerVersion = 1
	inflightModeClone      = "clone"
	inflightModeCopy       = "copy"
)

const (
	originSnapshotDeleteAttempts   = 3
	originSnapshotDeleteBackoff    = 500 * time.Millisecond
	originSnapshotDeleteMaxBackoff = 2 * time.Second
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
	managed := snap.UserProperties[PropManagedResource].Value == "true"
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
	defer func() {
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
	volumeID := d.sanitizeVolumeID(name)
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
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	// Calculate and validate capacity
	capacityBytes := int64(0)
	requiredBytes := int64(0)
	limitBytes := int64(0)
	if req.GetCapacityRange() != nil {
		requiredBytes = req.GetCapacityRange().GetRequiredBytes()
		limitBytes = req.GetCapacityRange().GetLimitBytes()
		capacityBytes = requiredBytes

		// Validate limit vs required
		if limitBytes > 0 && capacityBytes > limitBytes {
			return nil, status.Errorf(codes.InvalidArgument,
				"required capacity (%d bytes) exceeds limit (%d bytes)", capacityBytes, limitBytes)
		}
	}
	if capacityBytes == 0 {
		capacityBytes = 1024 * 1024 * 1024 // Default 1GiB
	}

	// Minimum capacity validation (at least 1MiB to avoid edge cases)
	const minCapacity = 1024 * 1024 // 1 MiB
	if capacityBytes < minCapacity {
		return nil, status.Errorf(codes.InvalidArgument,
			"requested capacity (%d bytes) is below minimum (%d bytes)", capacityBytes, minCapacity)
	}

	// Maximum capacity sanity check (1PiB should be more than enough)
	const maxCapacity = 1024 * 1024 * 1024 * 1024 * 1024 // 1 PiB
	if capacityBytes > maxCapacity {
		return nil, status.Errorf(codes.InvalidArgument,
			"requested capacity (%d bytes) exceeds maximum (%d bytes)", capacityBytes, maxCapacity)
	}

	// A unified multi-protocol deployment cannot infer whether an omitted
	// StorageClass parameter meant NFS, iSCSI, or NVMe-oF. Keep the historical
	// driver-name fallback only for instances that serve at most one protocol.
	params := req.GetParameters()
	protocol, hasProtocol := params["protocol"]
	if !hasProtocol {
		if d.config.enabledShareTypeCount() > 1 {
			return nil, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %q is required when multiple storage protocols are enabled; valid options are: %s",
				"protocol", strings.Join(ValidShareTypeStrings(), ", "))
		}
	} else {
		explicitShareType := ShareType(strings.ToLower(strings.TrimSpace(protocol)))
		if !explicitShareType.IsValid() {
			return nil, status.Errorf(codes.InvalidArgument,
				"invalid protocol %q; valid options are: %s",
				protocol, strings.Join(ValidShareTypeStrings(), ", "))
		}
	}
	shareType := d.config.GetShareType(params)
	klog.Infof("CreateVolume: using share type %s for volume %s", shareType, volumeID)
	if shareType == ShareTypeNFS {
		for _, capability := range req.GetVolumeCapabilities() {
			if capability.GetBlock() != nil {
				return nil, status.Error(codes.InvalidArgument, "raw block volume capability is incompatible with NFS protocol")
			}
		}
	}

	// Validate access mode against protocol
	// RWX (ReadWriteMany) is only supported for NFS volumes
	if mode, ok := multiNodeAccessMode(req.GetVolumeCapabilities()); ok && !shareType.SupportsMultiNode() {
		return nil, status.Errorf(codes.InvalidArgument,
			"access mode %s requires NFS protocol, but %s was requested",
			mode.String(), shareType)
	}

	// Check if volume already exists
	existingDS, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err == nil && existingDS != nil {
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
				ctx, existingDS, datasetName, name, source, capacityBytes, limitBytes, shareType,
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
		if snapshot := req.GetVolumeContentSource().GetSnapshot(); d.config.ZFS.DetachedVolumesFromSnapshots && snapshot != nil {
			existingDS, err = d.prepareDetachedSnapshotCopy(
				ctx, datasetName, existingDS, name, snapshot.GetSnapshotId(), snapshot.GetSnapshotId(), capacityBytes, shareType,
			)
			if err != nil {
				return nil, err
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
		if d.config.ZFS.DetachedVolumesFromSnapshots &&
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

		// CRITICAL: Ensure share exists for existing volumes (fixes missing iSCSI targets after retries)
		// This handles the case where a previous CreateVolume created the dataset but failed
		// to create the share (e.g., due to timeout, TrueNAS API error, etc.)
		if shareErr := d.ensureShareExists(ctx, existingDS, datasetName, name, shareType); shareErr != nil {
			return nil, shareErr
		}

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
	if err != nil && !truenas.IsNotFoundError(err) {
		return nil, status.Errorf(codes.Internal, "failed to check whether volume exists: %v", err)
	}
	freshlyCreated := false

	// Handle volume content source (clone from snapshot or volume)
	var contentSource *csi.VolumeContentSource
	var createdDS *truenas.Dataset
	zvolReady := false
	if req.GetVolumeContentSource() != nil {
		contentSource = req.GetVolumeContentSource()
		_, srcErr := d.handleVolumeContentSource(ctx, datasetName, name, contentSource, capacityBytes, shareType)
		if srcErr != nil {
			return nil, srcErr
		}
		// Clone/replication APIs cannot stamp properties atomically. The initial
		// absence check plus a successful (not AlreadyExists) clone/copy response is
		// the creation proof; stamp ownership before creating any share object.
		verifiedClone, ownerErr := d.setAndVerifyDatasetUserProperties(ctx, datasetName, map[string]string{
			PropDriverInstanceID: d.driverInstanceID(),
		})
		if ownerErr != nil {
			d.cleanupFailedClone(ctx, datasetName, "")
			return nil, status.Errorf(codes.Internal, "failed to stamp and verify cloned volume ownership: %v", ownerErr)
		}
		// Ownership is durable; the in-flight marker has served its purpose.
		// Best-effort removal — the reconciler sweep retires leftovers.
		d.deleteInflightMarker(ctx, volumeID)
		createdDS = verifiedClone
		zvolReady = true
	} else {
		// Create new dataset
		var createErr error
		createdDS, createErr = d.createDataset(ctx, datasetName, capacityBytes, shareType)
		if createErr != nil {
			return nil, createErr
		}
		freshlyCreated = createdDS.CreatedByCall
		zvolReady = freshlyCreated
	}

	// Create share (NFS, iSCSI, or NVMe-oF). A definitely fresh DatasetCreate
	// result and the clone readiness path do not need another zvol poll.
	if shareErr := d.createShareWithOptions(ctx, createdDS, datasetName, name, shareType, freshlyCreated, zvolReady); shareErr != nil {
		// Cleanup on failure
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, true); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after share creation failure: %v", delErr)
		}
		return nil, shareErr
	}

	// Mark as managed and successful in one API update.
	volumeProperties := map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropProvisionSuccess: "true",
		PropCSIVolumeName:    name,
	}
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		volumeProperties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
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

type inFlightRemnantAction int

const (
	remnantActionNone inFlightRemnantAction = iota
	remnantActionResume
	remnantActionDestroy
)

// inflightMarker is the durable record, written on the PARENT dataset via the
// proven pool.dataset.update path BEFORE a content-source clone/copy starts,
// that this driver instance owns an in-flight creation of Dataset from the
// recorded source. Crash recovery requires it as POSITIVE proof: absence of
// local properties on a dataset proves nothing (publication records,
// content-source props, operator or configured custom properties, and the
// share-created-before-property-stored window all leave datasets that must
// never be adopted or destroyed).
type inflightMarker struct {
	Version    int    `json:"v"`
	Instance   string `json:"instance"`
	Dataset    string `json:"dataset"`
	Mode       string `json:"mode"`        // inflightModeClone | inflightModeCopy
	SourceType string `json:"source_type"` // "snapshot" | "volume"
	SourceID   string `json:"source_id"`
	// Origin is the exact ZFS origin the destination must report in clone mode
	// (the resolved source snapshot ID, or the deterministic internal
	// clone-source snapshot for volume sources). Empty for detached copies.
	Origin    string `json:"origin,omitempty"`
	Protocol  string `json:"protocol"`
	Nonce     string `json:"nonce"`
	StartedAt string `json:"started_at"`
}

func hashedPropertyKey(prefix, identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return prefix + hex.EncodeToString(sum[:8])
}

func inflightMarkerKey(volumeID string) string {
	return hashedPropertyKey(PropInflightMarkerPrefix, volumeID)
}

func (d *Driver) parentDatasetName() string {
	return strings.TrimSuffix(d.config.ZFS.DatasetParentName, "/")
}

func newRecoveryNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// writeInflightMarker persists and verifies the marker before any backend
// mutation for the destination starts. Failure is fatal to the create by
// design: without durable provenance a crash would leave an unrecoverable,
// invisible remnant, so "no marker, no mutation".
func (d *Driver) writeInflightMarker(ctx context.Context, marker inflightMarker) error {
	encoded, err := json.Marshal(marker)
	if err != nil {
		return status.Errorf(codes.Internal, "encode in-flight creation marker: %v", err)
	}
	key := inflightMarkerKey(path.Base(marker.Dataset))
	if _, err := d.setAndVerifyDatasetUserProperties(ctx, d.parentDatasetName(), map[string]string{key: string(encoded)}); err != nil {
		return status.Errorf(codes.Internal,
			"record in-flight creation marker for %s on parent dataset: %v", marker.Dataset, err)
	}
	return nil
}

// readInflightMarker returns this driver's marker for volumeID, or nil when no
// valid local marker exists. Non-local values and unparseable payloads are
// treated as absent for recovery (never a license to act) and left for the
// reconciler sweep.
func (d *Driver) readInflightMarker(ctx context.Context, volumeID string) (*inflightMarker, error) {
	parent, err := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if err != nil {
		return nil, fmt.Errorf("read parent dataset for in-flight markers: %w", err)
	}
	property, ok := datasetUserPropertyProjection(parent, inflightMarkerKey(volumeID))
	if !ok || !isLocalUserPropertySource(property.Source) {
		return nil, nil
	}
	var marker inflightMarker
	if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
		klog.Warningf("Ignoring unparseable in-flight marker for %s: %v", volumeID, err)
		return nil, nil
	}
	if marker.Version != inflightMarkerVersion {
		return nil, nil
	}
	return &marker, nil
}

// newInflightMarker builds the base marker (clone mode by default; the caller
// switches to copy mode and/or records the expected origin).
func (d *Driver) newInflightMarker(datasetName string, source *csi.VolumeContentSource, shareType ShareType) (inflightMarker, error) {
	sourceType, sourceID, ok := volumeContentSourceIdentity(source)
	if !ok {
		return inflightMarker{}, status.Error(codes.InvalidArgument, "volume content source is missing an identity")
	}
	nonce, err := newRecoveryNonce()
	if err != nil {
		return inflightMarker{}, status.Errorf(codes.Internal, "generate in-flight marker nonce: %v", err)
	}
	return inflightMarker{
		Version:    inflightMarkerVersion,
		Instance:   d.driverInstanceID(),
		Dataset:    datasetName,
		Mode:       inflightModeClone,
		SourceType: sourceType,
		SourceID:   sourceID,
		Protocol:   string(shareType),
		Nonce:      nonce,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

// deleteInflightMarker is best-effort: a leftover marker is retired by the
// reconciler sweep once its dataset is stamped or gone.
func (d *Driver) deleteInflightMarker(ctx context.Context, volumeID string) {
	key := inflightMarkerKey(volumeID)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), []string{key}); err != nil {
		klog.Warningf("Failed to remove in-flight creation marker for %s (reconciler sweep will retire it): %v", volumeID, err)
	}
}

// recoverInFlightContentSourceRemnant restores crash self-healing for a
// content-source CreateVolume whose destination was created but crashed before
// ownership was stamped. Recovery is gated on POSITIVE durable proof — a
// matching parent-dataset in-flight marker written by this driver instance
// before the clone/copy started — never on absence of evidence. Without a
// matching marker every existing dataset stays on the terminal AlreadyExists
// path, which protects foreign datasets, operator/DR datasets at colliding
// names, datasets whose backend share exists but whose share-ID property write
// failed, and datasets carrying only publication-record or custom local
// properties.
//
// Compatibility with the CURRENT request is validated BEFORE any mutation:
// source identity (marker source vs the request), protocol (marker protocol vs
// the requested share type; dataset-type/protocol mismatches were already
// rejected by the checks that run before this call), and capacity (remnant
// capacity vs the request's limit). An incompatible request keeps AlreadyExists
// exactly as CSI requires and never receives a resumed OK.
//
//   - CLONE mode: the remnant's ZFS origin must equal the origin recorded in
//     the marker (double provenance proof) and the remnant must have no
//     snapshots of its own. RESUME: normalize capacity, then stamp the full
//     local identity set through the update+verify path with a per-attempt
//     nonce CAS — two overlapping controller processes can both reach this
//     point, and only the process whose nonce survives the re-read proceeds;
//     the loser returns Aborted.
//   - COPY mode (detached): completeness of an interrupted send/receive cannot
//     be proven, so DESTROY and return Aborted for a clean recreate. The real
//     crash shape leaves the transferred replication snapshot on the target
//     (replication creates target+snapshot; snapshot cleanup runs only after
//     the job completes), so the destroy is recursive — sanctioned exclusively
//     by the marker's proof of ownership.
func (d *Driver) recoverInFlightContentSourceRemnant(
	ctx context.Context,
	existingDS *truenas.Dataset,
	datasetName, name string,
	source *csi.VolumeContentSource,
	capacityBytes, limitBytes int64,
	shareType ShareType,
) (*truenas.Dataset, inFlightRemnantAction, error) {
	if source == nil || existingDS == nil || existingDS.Name != datasetName {
		return existingDS, remnantActionNone, nil
	}
	volumeID := path.Base(datasetName)
	// A dataset with a LOCAL ownership stamp is not an in-flight remnant: ours
	// continues through the normal existing-volume path (retiring a stale marker
	// left by a crash between stamp and marker delete); foreign is never touched.
	if owner, ok := datasetUserPropertyProjection(existingDS, PropDriverInstanceID); ok && isLocalUserPropertySource(owner.Source) {
		if owner.Value == d.driverInstanceID() {
			d.deleteInflightMarker(ctx, volumeID)
		}
		return existingDS, remnantActionNone, nil
	}
	marker, err := d.readInflightMarker(ctx, volumeID)
	if err != nil {
		return existingDS, remnantActionNone, status.Errorf(codes.Internal,
			"inspect in-flight creation marker for %s: %v", datasetName, err)
	}
	if marker == nil || marker.Instance != d.driverInstanceID() || marker.Dataset != datasetName {
		return existingDS, remnantActionNone, nil
	}
	// Content-source compatibility: the marker must describe exactly the source
	// this request asks for. A different source is an incompatible request and
	// must keep the terminal AlreadyExists outcome.
	requestType, requestID, ok := volumeContentSourceIdentity(source)
	if !ok || marker.SourceType != requestType || marker.SourceID != requestID {
		return existingDS, remnantActionNone, nil
	}

	switch marker.Mode {
	case inflightModeCopy:
		if !d.config.ZFS.DetachedVolumesFromSnapshots || source.GetSnapshot() == nil {
			return existingDS, remnantActionNone, nil
		}
		// The same request-compatibility validation as the clone branch runs
		// BEFORE the destroy: an incompatible request must keep the terminal
		// AlreadyExists outcome with the remnant untouched, never trigger a
		// destroy-and-recreate on behalf of a differently-shaped request.
		if marker.Protocol != string(shareType) {
			return existingDS, remnantActionNone, nil
		}
		if existingCapacity := d.getDatasetCapacity(existingDS); limitBytes > 0 && existingCapacity > limitBytes {
			return existingDS, remnantActionNone, status.Errorf(codes.AlreadyExists,
				"in-flight remnant for volume %s has capacity %d bytes, greater than requested capacity limit %d bytes",
				volumeID, existingCapacity, limitBytes)
		}
		// Marker-proven interrupted copy: the recursive destroy also removes the
		// transferred replication snapshot the crash left on the target.
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
			return existingDS, remnantActionNone, status.Errorf(codes.Internal,
				"destroy marker-proven interrupted detached-copy remnant %s: %v", datasetName, delErr)
		}
		d.deleteInflightMarker(ctx, volumeID)
		klog.Warningf("Destroyed marker-proven interrupted detached-copy remnant %s; retry will recreate it cleanly", datasetName)
		return nil, remnantActionDestroy, nil

	case inflightModeClone:
		// The current configuration must still take the clone path for this
		// source; after a config flip the remnant is left for the operator.
		if d.config.ZFS.DetachedVolumesFromSnapshots && source.GetSnapshot() != nil {
			return existingDS, remnantActionNone, nil
		}
		// Protocol compatibility before any mutation.
		if marker.Protocol != string(shareType) {
			return existingDS, remnantActionNone, nil
		}
		origin := datasetOriginSnapshotID(existingDS)
		if origin == "" || marker.Origin == "" || origin != marker.Origin {
			return existingDS, remnantActionNone, nil
		}
		// A just-cloned remnant owns no snapshots; anything else is not a
		// half-object this recovery understands.
		snapshots, listErr := d.truenasClient.SnapshotList(ctx, datasetName)
		if listErr != nil {
			return existingDS, remnantActionNone, status.Errorf(codes.Internal,
				"inspect in-flight clone remnant %s for snapshots: %v", datasetName, listErr)
		}
		if len(snapshots) > 0 {
			return existingDS, remnantActionNone, nil
		}
		// Capacity compatibility BEFORE any mutation: a request whose limit the
		// remnant already exceeds is incompatible per CSI.
		if existingCapacity := d.getDatasetCapacity(existingDS); limitBytes > 0 && existingCapacity > limitBytes {
			return existingDS, remnantActionNone, status.Errorf(codes.AlreadyExists,
				"in-flight remnant for volume %s has capacity %d bytes, greater than requested capacity limit %d bytes",
				volumeID, existingCapacity, limitBytes)
		}
		completed, resumeErr := d.completeResumedCloneRemnant(ctx, existingDS, datasetName, name, source, capacityBytes, shareType)
		if resumeErr != nil {
			return existingDS, remnantActionNone, resumeErr
		}
		d.deleteInflightMarker(ctx, volumeID)
		klog.Infof("Resumed marker-proven in-flight clone remnant %s (origin %s) after a crash before ownership was stamped", datasetName, origin)
		return completed, remnantActionResume, nil
	}
	return existingDS, remnantActionNone, nil
}

// completeResumedCloneRemnant finishes a proven clone remnant: it normalizes the
// clone's capacity and stamps the full local identity set (ownership, managed
// markers, content source, and for volume clones the origin snapshot) through the
// proven update+verify path, so the volume no longer depends on any
// clone-inherited (non-local) value. The stamp carries a per-attempt nonce:
// operationLock is per-process, so two overlapping controllers can both attempt
// this stamp; each writes its full payload in a single update and the re-read
// decides whose write won. The loser maps the verification failure to Aborted —
// its retry re-enters through the normal existing-volume path, where the
// winner's identical-instance stamp is simply idempotent. The caller then
// finishes through the normal existing-dataset tail (idempotent share creation
// and the response).
func (d *Driver) completeResumedCloneRemnant(
	ctx context.Context,
	existingDS *truenas.Dataset,
	datasetName, name string,
	source *csi.VolumeContentSource,
	capacityBytes int64,
	shareType ShareType,
) (*truenas.Dataset, error) {
	if err := d.ensureCloneCapacity(ctx, datasetName, existingDS, capacityBytes); err != nil {
		return nil, err
	}
	nonce, nonceErr := newRecoveryNonce()
	if nonceErr != nil {
		return nil, status.Errorf(codes.Internal, "generate recovery nonce: %v", nonceErr)
	}
	properties := map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropProvisionSuccess: "true",
		PropCSIVolumeName:    name,
		PropRecoveryNonce:    nonce,
	}
	if snapshot := source.GetSnapshot(); snapshot != nil {
		properties[PropVolumeContentSourceType] = "snapshot"
		properties[PropVolumeContentSourceID] = snapshot.GetSnapshotId()
	} else if volume := source.GetVolume(); volume != nil {
		sourceDataset, err := d.datasetForID(volume.GetVolumeId())
		if err != nil {
			return nil, err
		}
		tempSnapshotName := fmt.Sprintf("clone-source-%s", d.sanitizeVolumeID(path.Base(datasetName)))
		properties[PropVolumeContentSourceType] = "volume"
		properties[PropVolumeContentSourceID] = volume.GetVolumeId()
		properties[PropVolumeOriginSnapshot] = sourceDataset + "@" + tempSnapshotName
	}
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		properties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
	verified, err := d.setAndVerifyDatasetUserProperties(ctx, datasetName, properties)
	if err != nil {
		if errors.Is(err, errDatasetPropertyVerification) {
			// Another controller's recovery stamp overwrote ours between our write
			// and re-read. Its (same-instance) stamp is authoritative; retry through
			// the normal existing-volume path.
			return nil, status.Errorf(codes.Aborted,
				"lost the in-flight remnant recovery race for %s; retry CreateVolume", datasetName)
		}
		return nil, status.Errorf(codes.Internal, "stamp and verify resumed clone remnant ownership: %v", err)
	}
	if removeErr := d.truenasClient.DatasetRemoveUserProperties(ctx, datasetName, []string{PropRecoveryNonce}); removeErr != nil {
		klog.Warningf("Failed to remove recovery nonce from %s (inert leftover): %v", datasetName, removeErr)
	}
	delete(verified.UserProperties, PropRecoveryNonce)
	return verified, nil
}

func storedBlockProtocol(ds *truenas.Dataset, shareType ShareType) bool {
	var properties []string
	switch shareType {
	case ShareTypeISCSI:
		properties = []string{PropISCSITargetID, PropISCSITargetExtentID}
	case ShareTypeNVMeoF:
		properties = []string{PropNVMeoFSubsystemID, PropNVMeoFNamespaceID}
	default:
		return false
	}
	for _, property := range properties {
		if value := datasetUserProperty(ds, property); value != "" && value != "-" {
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

	klog.Infof("DeleteVolume: volumeID=%s", volumeID)

	// Lock on volume ID
	lockKey := "volume:" + volumeID
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
			// fallback logic that finds resources by name.
			var cleanupErrors []string
			if cleanupErr := d.deleteShare(ctx, nil, datasetName, ShareTypeNVMeoF); cleanupErr != nil {
				klog.Warningf("Failed to cleanup orphaned NVMe-oF resources for %s: %v", volumeID, cleanupErr)
				cleanupErrors = append(cleanupErrors, "NVMe-oF: "+cleanupErr.Error())
			} else {
				klog.Infof("Cleaned up orphaned NVMe-oF resources for %s", volumeID)
			}
			if cleanupErr := d.deleteShare(ctx, nil, datasetName, ShareTypeISCSI); cleanupErr != nil {
				klog.Warningf("Failed to cleanup orphaned iSCSI resources for %s: %v", volumeID, cleanupErr)
				cleanupErrors = append(cleanupErrors, "iSCSI: "+cleanupErr.Error())
			} else {
				klog.Infof("Cleaned up orphaned iSCSI resources for %s", volumeID)
			}
			if len(cleanupErrors) > 0 {
				return nil, status.Errorf(codes.Internal, "orphaned protocol cleanup failed for %s: %s", volumeID, strings.Join(cleanupErrors, "; "))
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
		if prop, ok := ds.UserProperties[PropNVMeoFSubsystemID]; ok && prop.Value != "" && prop.Value != "-" {
			shareType = ShareTypeNVMeoF
			shareTypeKnown = true
			klog.V(4).Infof("Detected NVMe-oF volume from stored subsystem ID property")
		} else if prop, ok := ds.UserProperties[PropISCSITargetID]; ok && prop.Value != "" && prop.Value != "-" {
			shareType = ShareTypeISCSI
			shareTypeKnown = true
			klog.V(4).Infof("Detected iSCSI volume from stored target ID property")
		} else if prop, ok := ds.UserProperties[PropNFSShareID]; ok && prop.Value != "" && prop.Value != "-" {
			shareType = ShareTypeNFS
			shareTypeKnown = true
			klog.V(4).Infof("Detected NFS volume from stored share ID property")
		} else {
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
	// for snapshots that appear after this check.
	if !d.config.ZFS.DestroyForeignSnapshotsOnDelete && len(snapshots) > 0 {
		klog.Infof("Volume %s has non-CSI snapshots and destroyForeignSnapshotsOnDelete is disabled; refusing before share deletion", volumeID)
		return nil, status.Errorf(codes.FailedPrecondition,
			"volume %s has non-CSI snapshots (likely from a TrueNAS periodic-snapshot or replication task on the parent dataset); delete them, or exclude the CSI parent dataset from snapshot tasks, or set zfs.destroyForeignSnapshotsOnDelete=true to allow the driver to remove them", volumeID)
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
			// Non-CSI-managed snapshots exist. Preserve them by default; recursive
			// deletion is an explicit operator opt-in because it destroys snapshots
			// with an independent lifecycle from the CSI volume.
			if !d.config.ZFS.DestroyForeignSnapshotsOnDelete {
				return nil, status.Errorf(codes.FailedPrecondition,
					"volume %s has non-CSI snapshots (likely from a TrueNAS periodic-snapshot or replication task on the parent dataset); delete them, or exclude the CSI parent dataset from snapshot tasks, or set zfs.destroyForeignSnapshotsOnDelete=true to allow the driver to remove them", volumeID)
			}
			klog.V(4).Infof("Volume %s has non-managed snapshots, deleting recursively", volumeID)
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, delErr)
				if isDatasetDependencyOrBusyError(delErr) {
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s has dependent snapshot clones that must be deleted first: %v", volumeID, delErr)
				}
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
		case snapErr != nil:
			// When snapshot state cannot be verified, fail safe unless the operator
			// explicitly allowed destructive cleanup of foreign snapshots.
			if !d.config.ZFS.DestroyForeignSnapshotsOnDelete {
				return nil, status.Errorf(codes.FailedPrecondition,
					"cannot verify snapshots for volume %s; refusing recursive delete: %v", volumeID, snapErr)
			}
			klog.V(4).Infof("Could not list snapshots for %s (%v), trying recursive delete", volumeID, snapErr)
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, delErr)
				if isDatasetDependencyOrBusyError(delErr) {
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s has dependent snapshot clones that must be deleted first: %v", volumeID, delErr)
				}
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
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
	if originSnapshotID != "" {
		klog.Infof("Cleaning up origin snapshot %s for deleted volume clone %s", originSnapshotID, volumeID)
		if err := d.deleteCloneOriginSnapshot(ctx, originSnapshotID); err != nil {
			klog.Errorf("Failed to delete origin snapshot %s: %v", originSnapshotID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete clone origin snapshot %s: %v", originSnapshotID, err)
		}
	}

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
	lockKey := "volume:" + volumeID
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

	// Ensure the share exists (critical for restored volumes)
	// This recreates missing iSCSI targets or NVMe-oF subsystems
	if err := d.ensureShareExists(ctx, ds, datasetName, volumeID, shareType); err != nil {
		return nil, err
	}
	if d.config.Fencing.Enabled() {
		if err := d.publishFencedVolume(ctx, ds, datasetName, shareType, identity, req.GetVolumeCapability(), req.GetReadonly()); err != nil {
			return nil, err
		}
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
	if !d.config.Fencing.Enabled() {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	lockKey := "volume:" + volumeID
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
	shareType := shareTypeForPublishedVolume(ds, nil)
	if err := d.unpublishFencedVolume(ctx, ds, datasetName, shareType, req.GetNodeId()); err != nil {
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

	available, err := d.truenasClient.GetPoolAvailable(ctx, d.config.ZFS.DatasetParentName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get capacity: %v", err)
	}

	return &csi.GetCapacityResponse{
		AvailableCapacity: available,
	}, nil
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
	volumeLockKey := "volume:" + sourceVolumeID
	if !d.acquireOperationLock(volumeLockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for the source volume")
	}
	defer d.releaseOperationLock(volumeLockKey)

	snapshotID := d.sanitizeVolumeID(name)
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
		return d.createSnapshotResponse(existing, sourceDataset, snapshotID, sourceVolumeID, start), nil
	}

	// Create the snapshot and its identity atomically. TrueNAS 26.0 silently
	// ignores post-create pool.snapshot.update property writes.
	snap, err := d.truenasClient.SnapshotCreate(ctx, datasetName, snapshotID, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           name,
		PropCSISnapshotSourceVolumeID: sourceVolumeID,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	// The source dataset fetched for the existence check above still reflects
	// the volume size for restoreSize — no need to re-query it.
	return d.createSnapshotResponse(snap, sourceDataset, snapshotID, sourceVolumeID, start), nil
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
		volumeLockKey := "volume:" + sourceVolumeID
		if !d.acquireOperationLock(volumeLockKey) {
			return nil, status.Error(codes.Aborted, "operation already in progress for the source volume")
		}
		defer d.releaseOperationLock(volumeLockKey)
	}
	snapshotLockKey := "snapshot:" + snapshotID
	if !d.acquireOperationLock(snapshotLockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(snapshotLockKey)

	if err := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); err != nil {
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

// tombstoneLedgerEntry is the parent-dataset record proving that THIS driver
// tombstoned the named snapshot. It is written BEFORE the rename so the orphan
// reaper only ever destroys snapshots with proven driver provenance; a
// name-shaped lookalike (e.g. a user snapshot literally named *-csi-deleted-N)
// has no ledger entry and is never touched. A crash between ledger write and
// rename leaves an entry without a tombstone, which the reconciler sweeps.
type tombstoneLedgerEntry struct {
	Version  int    `json:"v"`
	Snapshot string `json:"snapshot"` // full tombstone ID: dataset@tombstone-name
	Dataset  string `json:"dataset"`
	// CreatedAt is the tombstoned snapshot's immutable ZFS creation time (unix
	// seconds), captured at ledger-write time. The reaper requires the observed
	// snapshot's creation time to MATCH, so a stale ledger entry can never
	// authorize reaping a different snapshot later recreated at the same full ID.
	CreatedAt int64  `json:"created_at,omitempty"`
	RenamedAt string `json:"renamed_at"`
}

func tombstoneLedgerKey(fullSnapshotID string) string {
	return hashedPropertyKey(PropTombstoneLedgerPrefix, fullSnapshotID)
}

func (d *Driver) writeTombstoneLedgerEntry(ctx context.Context, entry tombstoneLedgerEntry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode tombstone ledger entry: %w", err)
	}
	if _, err := d.setAndVerifyDatasetUserProperties(ctx, d.parentDatasetName(), map[string]string{
		tombstoneLedgerKey(entry.Snapshot): string(encoded),
	}); err != nil {
		return fmt.Errorf("record tombstone ledger entry for %s: %w", entry.Snapshot, err)
	}
	return nil
}

// removeTombstoneLedgerEntry is best-effort; an orphaned entry (its snapshot no
// longer exists) is retired by the reconciler sweep.
func (d *Driver) removeTombstoneLedgerEntry(ctx context.Context, fullSnapshotID string) {
	key := tombstoneLedgerKey(fullSnapshotID)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), []string{key}); err != nil {
		klog.Warningf("Failed to remove tombstone ledger entry for %s (reconciler sweep will retire it): %v", fullSnapshotID, err)
	}
}

// tombstoneLedgerFromDataset extracts the local tombstone ledger entries from
// the parent dataset, keyed by property key. Non-local and unparseable values
// are ignored (never grounds to reap anything).
func tombstoneLedgerFromDataset(parent *truenas.Dataset) map[string]tombstoneLedgerEntry {
	entries := make(map[string]tombstoneLedgerEntry)
	if parent == nil {
		return entries
	}
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropTombstoneLedgerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var entry tombstoneLedgerEntry
		if err := json.Unmarshal([]byte(property.Value), &entry); err != nil {
			klog.Warningf("Ignoring unparseable tombstone ledger entry %s: %v", key, err)
			continue
		}
		if entry.Version != tombstoneLedgerVersion || entry.Snapshot == "" || key != tombstoneLedgerKey(entry.Snapshot) {
			continue
		}
		entries[key] = entry
	}
	return entries
}

// handleSnapshotClones tombstones a snapshot and asks ZFS to destroy it once
// its last clone releases the dependency.
func (d *Driver) handleSnapshotClones(ctx context.Context, snap *truenas.Snapshot) error {
	tombstoneName := snapshotTombstoneName(snap.Dataset, snap.Name, time.Now().UnixNano())
	deleteID := snap.Dataset + "@" + tombstoneName
	// Durable provenance BEFORE the rename: the reaper may only ever destroy
	// snapshots this ledger proves the driver tombstoned. The immutable creation
	// time binds the entry to THIS snapshot, not any later object at the same ID.
	if err := d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  deleteID,
		Dataset:   snap.Dataset,
		CreatedAt: snap.GetCreationTime(),
		RenamedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to record tombstone provenance for snapshot %s: %v", snap.ID, err)
	}
	if renameErr := d.truenasClient.SnapshotRename(ctx, snap.ID, tombstoneName); renameErr != nil {
		// The error may be a timeout AFTER the rename actually landed. Retire the
		// ledger entry ONLY when the original name is observably still present
		// (the rename provably did not happen); when the tombstone name exists,
		// the rename succeeded and the entry must survive — it is the reaper's
		// only provenance for the now-unnamed tombstone. When neither is
		// observable, keep the entry: the age-gated sweep retires a false one.
		if _, tombErr := d.truenasClient.SnapshotGet(ctx, deleteID); tombErr == nil {
			klog.Warningf("Snapshot rename to %s reported %v but the tombstone exists; keeping its ledger entry", deleteID, renameErr)
		} else if _, origErr := d.truenasClient.SnapshotGet(ctx, snap.ID); origErr == nil {
			d.removeTombstoneLedgerEntry(ctx, deleteID)
		}
		return status.Errorf(codes.Internal, "failed to tombstone snapshot %s before deferred deletion: %v", snap.ID, renameErr)
	}

	properties := []string{PropManagedResource, PropCSISnapshotName, PropCSISnapshotSourceVolumeID}
	if err := d.truenasClient.SnapshotRemoveUserProperties(ctx, deleteID, properties); err != nil {
		klog.Warningf("Failed to strip CSI properties from deferred snapshot %s: %v", deleteID, err)
	}
	if err := d.truenasClient.SnapshotDelete(ctx, deleteID, true, false); err != nil {
		if truenas.IsNotFoundError(err) {
			d.removeTombstoneLedgerEntry(ctx, deleteID)
			return nil
		}
		// TrueNAS 26.0's zfs.resource.snapshot.destroy has no deferred-destroy
		// mode, so a tombstone that still has a live restored clone cannot be
		// removed yet. The tombstone rename already released the CSI snapshot name,
		// which is DeleteSnapshot's entire contract, so this is a success — leave
		// the tombstone (and its ledger entry) for the orphan reconciler to reap
		// once its last clone is gone. Returning an error here would surface a
		// spurious Internal and (once the CO's retry sees the renamed-away CSI
		// name as absent) leak the tombstone forever.
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			klog.V(4).Infof("Tombstoned snapshot %s still has dependent clones; deferring reclamation to orphan reconcile", deleteID)
			return nil
		}
		return status.Errorf(codes.Internal, "failed to defer snapshot deletion: %v", err)
	}
	// The backend accepted the (deferred or immediate) destroy; the ledger entry
	// has served its purpose.
	d.removeTombstoneLedgerEntry(ctx, deleteID)
	return nil
}

func snapshotTombstoneName(dataset, name string, nonce int64) string {
	const maxZFSSnapshotNameLength = 255
	suffix := snapshotTombstoneMarker + strconv.FormatInt(nonce, 10)
	maxShortNameLength := maxZFSSnapshotNameLength - len(dataset) - 1
	maxOriginalNameLength := maxShortNameLength - len(suffix)
	if maxOriginalNameLength < 0 {
		maxOriginalNameLength = 0
	}
	if len(name) > maxOriginalNameLength {
		name = name[:maxOriginalNameLength]
	}
	return name + suffix
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
	lockKey := "volume:" + volumeID
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

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: d.getDatasetCapacity(ds),
		},
	}, nil
}

// ControllerModifyVolume modifies a volume (not implemented).
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not implemented")
}

// Helper functions

func (d *Driver) sanitizeVolumeID(name string) string {
	return sanitizeVolumeID(name)
}

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

func snapshotListEntry(snap *truenas.Snapshot, sourceVolumeFilter string) *csi.ListSnapshotsResponse_Entry {
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
	entry := snapshotListEntry(snap, sourceVolumeFilter)
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
	d.applyDatasetProperties(params)
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

func (d *Driver) setAndVerifyDatasetUserProperties(ctx context.Context, datasetName string, properties map[string]string) (*truenas.Dataset, error) {
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, properties); err != nil {
		return nil, err
	}
	verified, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, fmt.Errorf("re-read dataset after user-property update: %w", err)
	}
	for key, expected := range properties {
		if !datasetHasLocalUserProperty(verified, key, expected) {
			return nil, fmt.Errorf("%w: property %s did not persist locally with the expected value", errDatasetPropertyVerification, key)
		}
	}
	return verified, nil
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
		case "logbias":
			params.Logbias = strings.ToUpper(value)
		case "primarycache":
			params.Primarycache = strings.ToUpper(value)
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

func (d *Driver) handleVolumeContentSource(ctx context.Context, datasetName, volumeName string, source *csi.VolumeContentSource, capacityBytes int64, shareType ShareType) (*truenas.Dataset, error) {
	// Timeout for waiting for cloned dataset to be ready (configurable via zfs.zvolReadyTimeout)
	cloneReadyTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
	var createdDS *truenas.Dataset

	if snapshot := source.GetSnapshot(); snapshot != nil {
		// Create from snapshot using either the legacy clone or the gated
		// independent local send/receive path.
		snapshotID := snapshot.GetSnapshotId()
		if _, err := d.datasetForID(snapshotID); err != nil {
			return nil, err
		}
		klog.Infof("Creating volume from snapshot: %s -> %s", snapshotID, datasetName)

		// Find the snapshot using efficient query (PERF-001 fix)
		snap, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
		}

		if snap == nil {
			return nil, status.Errorf(codes.NotFound, "snapshot not found: %s", snapshotID)
		}

		sourceSnapshot := snap.ID
		// Durable in-flight provenance BEFORE the first destination mutation. A
		// crash between clone/copy and the ownership stamp leaves a dataset with
		// no local identity; only this marker lets a retry prove the remnant is
		// ours and recover it instead of wedging on terminal AlreadyExists.
		marker, markerErr := d.newInflightMarker(datasetName, source, shareType)
		if markerErr != nil {
			return nil, markerErr
		}
		if d.config.ZFS.DetachedVolumesFromSnapshots {
			marker.Mode = inflightModeCopy
		} else {
			marker.Origin = snap.ID
		}
		if markerWriteErr := d.writeInflightMarker(ctx, marker); markerWriteErr != nil {
			return nil, markerWriteErr
		}
		if d.config.ZFS.DetachedVolumesFromSnapshots {
			klog.V(4).Infof("Found snapshot %s for independent local copy", sourceSnapshot)
			if copyErr := d.truenasClient.CopyDatasetFromSnapshotLocal(ctx, snap.Dataset, snap.Name, datasetName); copyErr != nil {
				if truenas.IsDatasetDestinationExistsError(copyErr) {
					// Deliberately KEEP the shared per-volume marker: the concurrent
					// winner is the same driver instance mid-flight on the same
					// name+source, and if it crashes before its ownership stamp the
					// surviving marker is what lets a retry recover its remnant. The
					// winner's post-stamp delete and the reconciler sweep retire it.
					return nil, status.Errorf(codes.Aborted,
						"detached snapshot copy destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
				}
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, status.Errorf(codes.Internal, "failed to copy snapshot into an independent volume: %v", copyErr)
			}
			klog.Infof("Independent snapshot copy created: %s -> %s", sourceSnapshot, datasetName)
		} else {
			klog.V(4).Infof("Found snapshot %s for cloning", sourceSnapshot)

			if cloneErr := d.truenasClient.SnapshotClone(ctx, sourceSnapshot, datasetName); cloneErr != nil {
				if truenas.IsDatasetDestinationExistsError(cloneErr) {
					// KEEP the shared marker: the same-instance winner may still need
					// it for crash recovery (see the detached branch above).
					return nil, status.Errorf(codes.Aborted,
						"snapshot clone destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
				}
				d.deleteInflightMarker(ctx, path.Base(datasetName))
				return nil, status.Errorf(codes.Internal, "failed to clone snapshot: %v", cloneErr)
			}
			klog.Infof("Snapshot clone created: %s -> %s", sourceSnapshot, datasetName)
		}

		// Wait for the new dataset to be ready before proceeding.
		// This is critical for iSCSI/NVMe-oF where extent creation needs the zvol
		createdDS, err = d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			d.cleanupFailedClone(ctx, datasetName, "")
			if d.config.ZFS.DetachedVolumesFromSnapshots {
				return nil, status.Errorf(codes.Internal, "failed waiting for detached snapshot copy to become ready: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "failed waiting for cloned volume to become ready: %v", err)
		}
		if d.config.ZFS.DetachedVolumesFromSnapshots {
			createdDS, err = d.prepareDetachedSnapshotCopy(
				ctx, datasetName, createdDS, volumeName, snapshotID, snap.Name, capacityBytes, shareType,
			)
			if err != nil {
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, err
			}
		} else {
			if err := d.ensureCloneCapacity(ctx, datasetName, createdDS, capacityBytes); err != nil {
				d.cleanupFailedClone(ctx, datasetName, "")
				return nil, err
			}

			if err := d.setDatasetUserProperties(ctx, createdDS, datasetName, map[string]string{
				PropVolumeContentSourceType: "snapshot",
				PropVolumeContentSourceID:   snapshotID,
			}); err != nil {
				klog.Warningf("Failed to set content source properties for snapshot clone: %v", err)
			}
		}

	} else if volume := source.GetVolume(); volume != nil {
		// Clone from volume
		sourceVolumeID := volume.GetVolumeId()
		sourceDataset, err := d.datasetForID(sourceVolumeID)
		if err != nil {
			return nil, err
		}
		klog.Infof("Creating volume from volume: %s -> %s", sourceVolumeID, datasetName)

		if _, getErr := d.truenasClient.DatasetGet(ctx, sourceDataset); getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				return nil, status.Errorf(codes.NotFound, "source volume not found: %s", sourceVolumeID)
			}
			return nil, status.Errorf(codes.Internal, "failed to get source volume: %v", getErr)
		}

		// Create a snapshot of source volume, then clone it
		tempSnapshotName := fmt.Sprintf("clone-source-%s", d.sanitizeVolumeID(path.Base(datasetName)))
		// Durable in-flight provenance BEFORE any mutation; the recorded origin is
		// the deterministic internal snapshot the clone must descend from.
		marker, markerErr := d.newInflightMarker(datasetName, source, shareType)
		if markerErr != nil {
			return nil, markerErr
		}
		marker.Origin = sourceDataset + "@" + tempSnapshotName
		if markerWriteErr := d.writeInflightMarker(ctx, marker); markerWriteErr != nil {
			return nil, markerWriteErr
		}
		snap, err := d.truenasClient.SnapshotCreate(ctx, sourceDataset, tempSnapshotName, map[string]string{
			PropInternalResource: "true",
		})
		if err != nil {
			d.deleteInflightMarker(ctx, path.Base(datasetName))
			return nil, status.Errorf(codes.Internal, "failed to create source snapshot: %v", err)
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
				return nil, status.Errorf(codes.Aborted,
					"volume clone destination %s appeared concurrently; retry CreateVolume through the ownership gate", datasetName)
			}
			d.deleteInflightMarker(ctx, path.Base(datasetName))
			if delErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); delErr != nil {
				klog.Warningf("Failed to cleanup snapshot after clone failure: %v", delErr)
			}
			return nil, status.Errorf(codes.Internal, "failed to clone volume: %v", cloneErr)
		}
		klog.Infof("Volume clone created: %s -> %s", sourceVolumeID, datasetName)

		// Wait for cloned dataset to be ready
		createdDS, err = d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, status.Errorf(codes.Internal, "failed waiting for cloned volume to become ready: %v", err)
		}
		if err := d.ensureCloneCapacity(ctx, datasetName, createdDS, capacityBytes); err != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, err
		}

		// Include origin snapshot so it can be cleaned up when the clone is deleted.
		if err := d.setDatasetUserProperties(ctx, createdDS, datasetName, map[string]string{
			PropVolumeContentSourceType: "volume",
			PropVolumeContentSourceID:   sourceVolumeID,
			PropVolumeOriginSnapshot:    snap.ID,
		}); err != nil {
			d.cleanupFailedClone(ctx, datasetName, snap.ID)
			return nil, status.Errorf(codes.Internal, "failed to set content source properties for volume clone: %v", err)
		}
	}

	return createdDS, nil
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

func (d *Driver) prepareDetachedSnapshotCopy(
	ctx context.Context,
	datasetName string,
	ds *truenas.Dataset,
	volumeName string,
	snapshotID string,
	snapshotShortName string,
	capacityBytes int64,
	shareType ShareType,
) (*truenas.Dataset, error) {
	if err := d.truenasClient.DestroyReplicatedTargetSnapshot(ctx, datasetName, snapshotShortName); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove transferred snapshot from detached copy: %v", err)
	}

	if err := d.ensureCloneCapacity(ctx, datasetName, ds, capacityBytes); err != nil {
		return nil, err
	}

	// Refresh after a possible expansion and use the target's actual properties,
	// not the source values returned by the replication receive.
	refreshed, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to verify detached snapshot copy: %v", err)
	}

	updates := &truenas.DatasetUpdateParams{}
	needsUpdate := false
	switch refreshed.Type {
	case "VOLUME":
		volsize := datasetPropertyBytes(refreshed.Volsize)
		if volsize < capacityBytes {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy has volsize %d bytes, less than requested %d bytes",
				volsize, capacityBytes)
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.ZvolEnableReservation {
			desiredRefreservation = volsize
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			updates.Refreservation = desiredRefreservation
			needsUpdate = true
		}
	case "FILESYSTEM":
		desiredRefquota := int64(0)
		if d.config.ZFS.DatasetEnableQuotas {
			desiredRefquota = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refquota) != desiredRefquota {
			updates.Refquota = desiredRefquota
			needsUpdate = true
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.DatasetEnableReservation {
			desiredRefreservation = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			updates.Refreservation = desiredRefreservation
			needsUpdate = true
		}
	default:
		return nil, status.Errorf(codes.Internal,
			"detached snapshot copy %s has unsupported dataset type %q", datasetName, refreshed.Type)
	}

	if needsUpdate {
		if _, updateErr := d.truenasClient.DatasetUpdate(ctx, datasetName, updates); updateErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to apply detached snapshot copy capacity properties: %v", updateErr)
		}
		refreshed, err = d.truenasClient.DatasetGet(ctx, datasetName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to verify detached snapshot copy properties: %v", err)
		}
	}

	switch refreshed.Type {
	case "VOLUME":
		volsize := datasetPropertyBytes(refreshed.Volsize)
		desiredRefreservation := int64(0)
		if d.config.ZFS.ZvolEnableReservation {
			desiredRefreservation = volsize
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy has refreservation %d bytes, expected %d bytes",
				datasetPropertyBytes(refreshed.Refreservation), desiredRefreservation)
		}
	case "FILESYSTEM":
		desiredRefquota := int64(0)
		if d.config.ZFS.DatasetEnableQuotas {
			desiredRefquota = capacityBytes
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.DatasetEnableReservation {
			desiredRefreservation = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refquota) != desiredRefquota ||
			datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy quota properties do not match the requested configuration")
		}
	}

	// A replication receive inherits source user properties. Apply configured
	// user properties first, then overwrite every volume identity field that
	// belongs to the new CSI volume.
	identityProperties := make(map[string]string)
	for rawKey, rawValue := range d.config.ZFS.DatasetProperties {
		key := strings.TrimSpace(rawKey)
		if strings.Contains(key, ":") {
			identityProperties[key] = strings.TrimSpace(rawValue)
		}
	}
	identityProperties[PropCSIVolumeName] = volumeName
	identityProperties[PropDriverInstanceID] = d.driverInstanceID()
	identityProperties[PropVolumeContentSourceType] = "snapshot"
	identityProperties[PropVolumeContentSourceID] = snapshotID
	identityProperties[PropVolumeOriginSnapshot] = "-"
	// Share identifiers belong to the source dataset's TrueNAS database
	// objects and must never drive the target's create/retry path.
	identityProperties[PropNFSShareID] = "-"
	identityProperties[PropISCSITargetID] = "-"
	identityProperties[PropISCSIExtentID] = "-"
	identityProperties[PropISCSITargetExtentID] = "-"
	identityProperties[PropNVMeoFSubsystemID] = "-"
	identityProperties[PropNVMeoFNamespaceID] = "-"
	identityProperties[PropNVMeoFPortSubsysID] = "-"
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		identityProperties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
	if err := d.setDatasetUserProperties(ctx, refreshed, datasetName, identityProperties); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to overwrite detached snapshot copy identity properties: %v", err)
	}
	return refreshed, nil
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

	switch shareType {
	case ShareTypeNFS:
		volumeContext["server"] = d.config.NFS.ShareHost
		volumeContext["share"] = ds.Mountpoint

	case ShareTypeISCSI:
		target, err := d.resolveISCSITarget(ctx, ds, datasetName)
		if err != nil || target == nil {
			return nil, status.Errorf(codes.Internal, "failed to resolve iSCSI target for %s: %v", datasetName, err)
		}
		globalCfg, err := d.truenasClient.ISCSIGlobalConfigGet(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get iSCSI global config: %v", err)
		}
		volumeContext["iqn"] = fmt.Sprintf("%s:%s", globalCfg.Basename, target.Name)
		volumeContext["portal"] = d.config.ISCSI.TargetPortal
		volumeContext["lun"] = "0"
		volumeContext["interface"] = d.config.ISCSI.Interface

	case ShareTypeNVMeoF:
		namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resolve NVMe-oF namespace: %v", err)
		}
		subsys, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
		if err != nil || subsys == nil {
			return nil, status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem for %s: %v", datasetName, err)
		}
		if namespace == nil || namespace.SubsystemID != subsys.ID {
			return nil, status.Errorf(codes.Internal, "NVMe-oF namespace for %s is missing or references a different subsystem", datasetName)
		}
		volumeContext["nqn"] = subsys.NQN
		volumeContext["transport"] = d.config.NVMeoF.Transport
		volumeContext["address"] = d.config.NVMeoF.TransportAddress
		volumeContext["port"] = strconv.Itoa(d.config.NVMeoF.TransportServiceID)
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
