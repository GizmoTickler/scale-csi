package driver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sync/errgroup"
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
	PropProvisionSuccess          = "truenas-csi:provision_success"
	PropCSIVolumeName             = "truenas-csi:csi_volume_name"
	PropShareVolumeContext        = "truenas-csi:csi_share_volume_context"
	PropVolumeContentSourceType   = "truenas-csi:csi_volume_content_source_type"
	PropVolumeContentSourceID     = "truenas-csi:csi_volume_content_source_id"
	PropVolumeOriginSnapshot      = "truenas-csi:csi_volume_origin_snapshot" // temp snapshot created during volume-to-volume cloning
	PropCSISnapshotName           = "truenas-csi:csi_snapshot_name"
	PropCSISnapshotSourceVolumeID = "truenas-csi:csi_snapshot_source_volume_id"
	PropNFSShareID                = "truenas-csi:truenas_nfs_share_id"
	PropISCSITargetID             = "truenas-csi:truenas_iscsi_target_id"
	PropISCSIExtentID             = "truenas-csi:truenas_iscsi_extent_id"
	PropISCSITargetExtentID       = "truenas-csi:truenas_iscsi_targetextent_id"
	PropNVMeoFSubsystemID         = "truenas-csi:truenas_nvmeof_subsystem_id"
	PropNVMeoFNamespaceID         = "truenas-csi:truenas_nvmeof_namespace_id"
	PropNVMeoFPortSubsysID        = "truenas-csi:truenas_nvmeof_portsubsys_id"
)

// ControllerGetCapabilities returns the capabilities of the controller.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Info("ControllerGetCapabilities called")

	caps := []*csi.ControllerServiceCapability{
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
					Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
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
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	start := time.Now()
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
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

	// Lock on volume name to prevent concurrent creates
	lockKey := "volume:" + name
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	// Calculate and validate capacity
	capacityBytes := int64(0)
	if req.GetCapacityRange() != nil {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
		limitBytes := req.GetCapacityRange().GetLimitBytes()

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

	// Get volume ID from name
	volumeID := d.sanitizeVolumeID(name)
	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Get share type from StorageClass parameters (with fallback to driver name)
	params := req.GetParameters()
	shareType := d.config.GetShareType(params)
	klog.Infof("CreateVolume: using share type %s for volume %s", shareType, volumeID)

	// Validate access mode against protocol
	// RWX (ReadWriteMany) is only supported for NFS volumes
	for _, cap := range req.GetVolumeCapabilities() {
		if accessMode := cap.GetAccessMode(); accessMode != nil {
			mode := accessMode.GetMode()
			if mode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER ||
				mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
				mode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER {
				if !shareType.SupportsMultiNode() {
					return nil, status.Errorf(codes.InvalidArgument,
						"access mode %s requires NFS protocol, but %s was requested",
						mode.String(), shareType)
				}
			}
		}
	}

	// Check if volume already exists
	existingDS, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err == nil && existingDS != nil {
		// Volume exists - check and ensure properties are set
		klog.Infof("Volume %s already exists", volumeID)

		// Ensure properties are set (idempotent)
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			if prop, ok := existingDS.UserProperties[PropManagedResource]; ok && prop.Value == "true" {
				return nil
			}
			if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropManagedResource, "true"); setErr != nil {
				return fmt.Errorf("failed to set managed resource property: %w", setErr)
			}
			return nil
		})
		g.Go(func() error {
			if prop, ok := existingDS.UserProperties[PropProvisionSuccess]; ok && prop.Value == "true" {
				return nil
			}
			if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropProvisionSuccess, "true"); setErr != nil {
				return fmt.Errorf("failed to set provision success property: %w", setErr)
			}
			return nil
		})
		g.Go(func() error {
			if prop, ok := existingDS.UserProperties[PropCSIVolumeName]; ok && prop.Value == name {
				return nil
			}
			if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropCSIVolumeName, name); setErr != nil {
				return fmt.Errorf("failed to set CSI volume name property: %w", setErr)
			}
			return nil
		})
		// Wait for all property sets to complete
		if waitErr := g.Wait(); waitErr != nil {
			klog.Errorf("Failed to ensure properties for existing volume %s: %v", volumeID, waitErr)
			return nil, status.Errorf(codes.Internal, "failed to ensure volume properties: %v", waitErr)
		}

		// CRITICAL: Ensure share exists for existing volumes (fixes missing iSCSI targets after retries)
		// This handles the case where a previous CreateVolume created the dataset but failed
		// to create the share (e.g., due to timeout, TrueNAS API error, etc.)
		if shareErr := d.ensureShareExists(ctx, existingDS, datasetName, name, shareType); shareErr != nil {
			return nil, shareErr
		}

		volumeContext, ctxErr := d.getVolumeContext(ctx, datasetName, shareType)
		if ctxErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", ctxErr)
		}
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      volumeID,
				CapacityBytes: d.getDatasetCapacity(existingDS),
				VolumeContext: volumeContext,
			},
		}, nil
	}

	// Handle volume content source (clone from snapshot or volume)
	var contentSource *csi.VolumeContentSource
	var mountpoint string // For NFS shares, used to avoid extra DatasetGet call
	isClonedVolume := false
	if req.GetVolumeContentSource() != nil {
		contentSource = req.GetVolumeContentSource()
		if srcErr := d.handleVolumeContentSource(ctx, datasetName, contentSource, capacityBytes); srcErr != nil {
			return nil, srcErr
		}
		isClonedVolume = true // handleVolumeContentSource already called WaitForZvolReady
	} else {
		// Create new dataset
		var createErr error
		mountpoint, createErr = d.createDataset(ctx, datasetName, capacityBytes, shareType)
		if createErr != nil {
			return nil, createErr
		}
	}

	// Create share (NFS, iSCSI, or NVMe-oF)
	// For cloned volumes, skip zvol wait since handleVolumeContentSource already waited
	if shareErr := d.createShareWithOptions(ctx, datasetName, name, shareType, isClonedVolume, mountpoint); shareErr != nil {
		// Cleanup on failure
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, false); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after share creation failure: %v", delErr)
		}
		return nil, shareErr
	}

	// Mark as managed and successful - run property sets in parallel
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropManagedResource, "true"); setErr != nil {
			return fmt.Errorf("failed to set managed resource property: %w", setErr)
		}
		return nil
	})
	g.Go(func() error {
		if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropProvisionSuccess, "true"); setErr != nil {
			return fmt.Errorf("failed to set provision success property: %w", setErr)
		}
		return nil
	})
	g.Go(func() error {
		if setErr := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropCSIVolumeName, name); setErr != nil {
			return fmt.Errorf("failed to set CSI volume name property: %w", setErr)
		}
		return nil
	})
	// Wait for all property sets to complete
	if waitErr := g.Wait(); waitErr != nil {
		// Property setting failed - clean up the share and dataset to avoid orphaned resources
		// The next CreateVolume call will start fresh
		klog.Errorf("Failed to set properties for volume %s: %v - cleaning up orphaned resources", volumeID, waitErr)
		if delErr := d.deleteShare(ctx, datasetName, shareType); delErr != nil {
			klog.Warningf("Failed to cleanup share after property failure: %v", delErr)
		}
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, false); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after property failure: %v", delErr)
		}
		return nil, status.Errorf(codes.Internal, "failed to set volume properties: %v", waitErr)
	}

	// Get volume context for response
	volumeContext, err := d.getVolumeContext(ctx, datasetName, shareType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
	}

	klog.Infof("CreateVolume completed: volume=%s, shareType=%s, contentSource=%s, elapsed=%v",
		volumeID, shareType, contentSourceInfo, time.Since(start))

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

// DeleteVolume deletes a volume.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
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

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Check if volume exists (idempotency - return success if already deleted)
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			klog.Infof("Volume %s dataset not found, attempting orphaned resource cleanup", volumeID)
			// Dataset is gone but there may be orphaned NVMe-oF/iSCSI resources.
			// Since we can't check dataset properties, try both protocols using
			// fallback logic that finds resources by name.
			var cleanupErrors []string
			if cleanupErr := d.deleteShare(ctx, datasetName, ShareTypeNVMeoF); cleanupErr != nil {
				// Only log as warning if it's not a "not found" type error
				errStr := cleanupErr.Error()
				if strings.Contains(errStr, "cleanup errors") {
					klog.Warningf("Failed to cleanup orphaned NVMe-oF resources for %s: %v", volumeID, cleanupErr)
					cleanupErrors = append(cleanupErrors, "NVMe-oF: "+errStr)
				} else {
					klog.V(4).Infof("No orphaned NVMe-oF resources for %s", volumeID)
				}
			} else {
				klog.Infof("Cleaned up orphaned NVMe-oF resources for %s", volumeID)
			}
			if cleanupErr := d.deleteShare(ctx, datasetName, ShareTypeISCSI); cleanupErr != nil {
				errStr := cleanupErr.Error()
				if strings.Contains(errStr, "cleanup errors") {
					klog.Warningf("Failed to cleanup orphaned iSCSI resources for %s: %v", volumeID, cleanupErr)
					cleanupErrors = append(cleanupErrors, "iSCSI: "+errStr)
				} else {
					klog.V(4).Infof("No orphaned iSCSI resources for %s", volumeID)
				}
			} else {
				klog.Infof("Cleaned up orphaned iSCSI resources for %s", volumeID)
			}
			if len(cleanupErrors) > 0 {
				klog.Warningf("Some orphaned resources could not be cleaned up for %s: %v", volumeID, cleanupErrors)
			}
			return &csi.DeleteVolumeResponse{}, nil
		}
		// Log but don't fail - try to proceed with deletion anyway
		klog.V(4).Infof("Could not verify volume existence: %v", err)
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

	// IMPORTANT: Check for dependent CSI-managed snapshots BEFORE deleting the share.
	// If we delete the share first and then discover snapshots exist, the share is already
	// gone and the volume becomes inaccessible while still existing.
	snapshots, snapErr := d.truenasClient.SnapshotList(ctx, datasetName)
	if snapErr == nil && len(snapshots) > 0 {
		for _, snap := range snapshots {
			if prop, ok := snap.UserProperties[PropManagedResource]; ok && prop.Value == "true" {
				klog.Infof("Volume %s has dependent CSI-managed snapshots, cannot delete", volumeID)
				return nil, status.Errorf(codes.FailedPrecondition,
					"volume %s has dependent snapshots that must be deleted first", volumeID)
			}
		}
	}

	// Delete share first (errors are fatal to prevent orphaned targets)
	switch {
	case shareTypeKnown:
		// We know the share type, delete just that one
		if err := d.deleteShare(ctx, datasetName, shareType); err != nil {
			klog.Errorf("Failed to delete share for volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete share: %v", err)
		}
	case ds != nil && ds.Type == "VOLUME":
		// Unknown zvol - try both iSCSI and NVMe-oF to avoid orphaned resources
		// One will likely return "not found" which is fine
		var lastErr error
		if err := d.deleteShare(ctx, datasetName, ShareTypeISCSI); err != nil {
			klog.V(4).Infof("iSCSI cleanup for %s: %v (may be expected if not iSCSI)", volumeID, err)
			lastErr = err
		}
		if err := d.deleteShare(ctx, datasetName, ShareTypeNVMeoF); err != nil {
			klog.V(4).Infof("NVMe-oF cleanup for %s: %v (may be expected if not NVMe-oF)", volumeID, err)
			lastErr = err
		}
		// Only fail if BOTH cleanup attempts had unexpected errors
		// "not found" type errors are expected for the wrong protocol
		if lastErr != nil && !strings.Contains(lastErr.Error(), "not found") && !strings.Contains(lastErr.Error(), "cleanup errors") {
			klog.Warningf("Share cleanup had errors for %s: %v", volumeID, lastErr)
		}
	default:
		// Default fallback for filesystem or unknown types
		if err := d.deleteShare(ctx, datasetName, shareType); err != nil {
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
		// Check if there are CSI-managed snapshots that are blocking deletion
		// TrueNAS returns various error messages: "Method call error", "has dependent clones", etc.
		snapshots, snapErr := d.truenasClient.SnapshotList(ctx, datasetName)
		switch {
		case snapErr == nil && len(snapshots) > 0:
			// Found snapshots - check if any are CSI-managed
			for _, snap := range snapshots {
				if prop, ok := snap.UserProperties[PropManagedResource]; ok && prop.Value == "true" {
					klog.Infof("Volume %s has dependent CSI-managed snapshots, cannot delete", volumeID)
					return nil, status.Errorf(codes.FailedPrecondition,
						"volume %s has dependent snapshots that must be deleted first", volumeID)
				}
			}
			// Non-CSI-managed snapshots exist, retry with recursive deletion
			klog.V(4).Infof("Volume %s has non-managed snapshots, deleting recursively", volumeID)
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, delErr)
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
		case snapErr != nil:
			// Could not check snapshots, log and try recursive delete
			klog.V(4).Infof("Could not list snapshots for %s (%v), trying recursive delete", volumeID, snapErr)
			if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
				klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, delErr)
				return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", delErr)
			}
		default:
			// No snapshots, but non-recursive delete still failed - just fail
			klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
		}
	}

	// Clean up origin snapshot if this was a volume-to-volume clone
	// The clone's dependency on the snapshot is now broken, so we can delete it
	if originSnapshotID != "" {
		klog.Infof("Cleaning up origin snapshot %s for deleted volume clone %s", originSnapshotID, volumeID)
		if err := d.truenasClient.SnapshotDelete(ctx, originSnapshotID, false, false); err != nil {
			// Log but don't fail - the snapshot might have been deleted already or might have other clones
			if !truenas.IsNotFoundError(err) {
				klog.Warningf("Failed to delete origin snapshot %s: %v", originSnapshotID, err)
			}
		}
	}

	klog.Infof("Volume %s deleted successfully", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
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

	// Determine share type from volume context
	volumeContext := req.GetVolumeContext()
	shareTypeStr := volumeContext["node_attach_driver"]
	if shareTypeStr == "" {
		// NFS volumes don't require controller publish
		klog.V(4).Infof("ControllerPublishVolume: volume %s has no node_attach_driver, assuming NFS (no-op)", volumeID)
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	shareType := ParseShareType(shareTypeStr)
	if shareType == ShareTypeNFS {
		// NFS doesn't require controller publish - shares are always accessible
		return &csi.ControllerPublishVolumeResponse{}, nil
	}

	klog.Infof("ControllerPublishVolume: volumeID=%s, nodeID=%s, shareType=%s", volumeID, nodeID, shareType)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Get the dataset to verify it exists and check share configuration
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume: %v", err)
	}

	// Ensure the share exists (critical for restored volumes)
	// This recreates missing iSCSI targets or NVMe-oF subsystems
	if err := d.ensureShareExists(ctx, ds, datasetName, volumeID, shareType); err != nil {
		return nil, err
	}

	klog.Infof("ControllerPublishVolume: volume %s published successfully to node %s", volumeID, nodeID)
	return &csi.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume detaches a volume from a node (not used for NFS).
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Not implemented - NFS doesn't require controller unpublish
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

	// Check volume exists
	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)
	_, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
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
// Note: Pagination is based on raw dataset offset, not filtered volume count.
// This is necessary because TrueNAS API pagination is offset-based, and we filter
// client-side for CSI-managed volumes. The trade-off is that pages may have fewer
// entries than requested if there are non-CSI datasets interspersed.
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

	// Use max entries as limit (default to 100 if not specified or 0)
	// Request more from API to account for filtered non-CSI datasets
	requestedLimit := int(req.GetMaxEntries())
	if requestedLimit == 0 {
		requestedLimit = 100
	}
	// Fetch extra to increase chance of filling the page after filtering
	fetchLimit := requestedLimit * 2
	if fetchLimit < 50 {
		fetchLimit = 50
	}

	datasets, err := d.truenasClient.DatasetList(ctx, d.config.ZFS.DatasetParentName, fetchLimit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0, requestedLimit)
	datasetsProcessed := 0
	for _, ds := range datasets {
		datasetsProcessed++

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

		// Stop if we have enough entries
		if len(entries) >= requestedLimit {
			break
		}
	}

	// Generate next token based on datasets actually processed
	// This ensures we don't skip any datasets on the next page
	nextToken := ""
	if datasetsProcessed > 0 && len(datasets) >= datasetsProcessed {
		// More datasets may exist if we got a full fetch or filled our requested limit
		if len(datasets) == fetchLimit || len(entries) == requestedLimit {
			nextToken = strconv.Itoa(offset + datasetsProcessed)
		}
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
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
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

	// Lock on snapshot name
	lockKey := "snapshot:" + name
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, sourceVolumeID)
	snapshotID := d.sanitizeVolumeID(name)

	// Create snapshot
	snap, err := d.truenasClient.SnapshotCreate(ctx, datasetName, snapshotID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	// Set snapshot properties in parallel
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if setErr := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropManagedResource, "true"); setErr != nil {
			return fmt.Errorf("failed to set managed resource property on snapshot: %w", setErr)
		}
		return nil
	})
	g.Go(func() error {
		if setErr := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropCSISnapshotName, name); setErr != nil {
			return fmt.Errorf("failed to set CSI snapshot name property: %w", setErr)
		}
		return nil
	})
	g.Go(func() error {
		if setErr := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropCSISnapshotSourceVolumeID, sourceVolumeID); setErr != nil {
			return fmt.Errorf("failed to set CSI snapshot source volume ID property: %w", setErr)
		}
		return nil
	})
	if waitErr := g.Wait(); waitErr != nil {
		// Property setting failed - delete the snapshot to avoid orphaned/invisible snapshots
		// Without PropManagedResource=true, the snapshot won't appear in ListSnapshots
		klog.Errorf("Failed to set properties for snapshot %s: %v - deleting orphaned snapshot", snapshotID, waitErr)
		if delErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); delErr != nil {
			klog.Warningf("Failed to cleanup snapshot after property failure: %v", delErr)
		}
		return nil, status.Errorf(codes.Internal, "failed to set snapshot properties: %v", waitErr)
	}

	// Get source volume size for restoreSize (CSI spec requires minimum volume size to restore)
	// snap.GetSnapshotSize() returns "used" bytes which may be tiny for near-empty volumes
	var snapshotSize int64
	sourceDataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		klog.Warningf("Failed to get source dataset size, using snapshot used bytes: %v", err)
		snapshotSize = snap.GetSnapshotSize()
	} else if volsize, ok := sourceDataset.Volsize.Parsed.(float64); ok && volsize > 0 {
		// For zvols (iSCSI/NVMe-oF), use volsize
		snapshotSize = int64(volsize)
	} else if refquota, ok := sourceDataset.Refquota.Parsed.(float64); ok && refquota > 0 {
		// For filesystems (NFS), use refquota as the volume size
		snapshotSize = int64(refquota)
	} else {
		// Fallback to snapshot used bytes if volume size not available
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
	}, nil
}

// DeleteSnapshot deletes a snapshot.
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	klog.Infof("DeleteSnapshot: snapshotID=%s", snapshotID)

	// Lock on snapshot ID
	lockKey := "snapshot:" + snapshotID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this snapshot")
	}
	defer d.releaseOperationLock(lockKey)

	// Find and delete the snapshot using efficient query (PERF-001 fix)
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

	if err := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); err != nil {
		// Handle "not found" as success (idempotency)
		if truenas.IsNotFoundError(err) {
			klog.Infof("Snapshot %s already deleted", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}

		// Handle snapshot with clones - verify and cleanup orphans only
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			if handleErr := d.handleSnapshotClones(ctx, snap.ID, cloneErr.Clones); handleErr != nil {
				return nil, handleErr
			}
			// Retry snapshot deletion after handling clones
			if retryErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); retryErr != nil {
				if truenas.IsNotFoundError(retryErr) {
					klog.Infof("Snapshot %s already deleted after clone cleanup", snapshotID)
					return &csi.DeleteSnapshotResponse{}, nil
				}
				klog.Errorf("Failed to delete snapshot %s after clone cleanup: %v", snapshotID, retryErr)
				return nil, status.Errorf(codes.Internal, "failed to delete snapshot after clone cleanup: %v", retryErr)
			}
			klog.Infof("Snapshot %s deleted successfully after orphan cleanup", snapshotID)
			return &csi.DeleteSnapshotResponse{}, nil
		}

		klog.Errorf("Failed to delete snapshot %s: %v", snapshotID, err)
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
	}
	klog.Infof("Snapshot %s deleted successfully", snapshotID)

	return &csi.DeleteSnapshotResponse{}, nil
}

// handleSnapshotClones verifies and handles clones before snapshot deletion.
// It classifies each clone as:
// - Non-CSI: Not managed by this driver (never touch)
// - Active: CSI-managed with an active export (NFS share, iSCSI extent, NVMe namespace)
// - Orphaned: CSI-managed without any export (safe to delete)
// Only orphaned clones are deleted. Active and non-CSI clones cause an error.
func (d *Driver) handleSnapshotClones(ctx context.Context, snapshotID string, clones []string) error {
	var activeClones []string
	var orphanedClones []string
	var nonCSIClones []string

	for _, cloneDataset := range clones {
		// Get the dataset to check properties
		ds, err := d.truenasClient.DatasetGet(ctx, cloneDataset)
		if err != nil {
			// If we can't get the dataset, treat it as non-CSI to be safe
			klog.Warningf("Failed to get clone dataset %s: %v, treating as non-CSI", cloneDataset, err)
			nonCSIClones = append(nonCSIClones, cloneDataset)
			continue
		}

		// Check if CSI-managed
		prop, isCSIManaged := ds.UserProperties[PropManagedResource]
		if !isCSIManaged || prop.Value != "true" {
			nonCSIClones = append(nonCSIClones, cloneDataset)
			continue
		}

		// Check if provisioning is complete - if not, the clone is still being set up
		// and we should treat it as active to avoid race conditions with CreateVolume
		provisionProp, hasProvisionProp := ds.UserProperties[PropProvisionSuccess]
		if !hasProvisionProp || provisionProp.Value != "true" {
			// Clone is still being provisioned - treat as active
			klog.V(4).Infof("Clone %s is still being provisioned (PropProvisionSuccess=%v), treating as active", cloneDataset, provisionProp.Value)
			activeClones = append(activeClones, cloneDataset)
			continue
		}

		// CSI-managed and fully provisioned - check for active exports
		hasExport, err := d.cloneHasActiveExport(ctx, cloneDataset, ds.Type)
		if err != nil {
			// If we can't determine, assume active to be safe
			klog.Warningf("Failed to check exports for clone %s: %v, assuming active", cloneDataset, err)
			activeClones = append(activeClones, cloneDataset)
			continue
		}

		if hasExport {
			activeClones = append(activeClones, cloneDataset)
		} else {
			orphanedClones = append(orphanedClones, cloneDataset)
		}
	}

	// Fail if there are non-CSI clones
	if len(nonCSIClones) > 0 {
		return status.Errorf(codes.FailedPrecondition,
			"cannot delete snapshot: has non-CSI dependent datasets %v. These must be manually removed.",
			nonCSIClones)
	}

	// Fail if there are active clones
	if len(activeClones) > 0 {
		return status.Errorf(codes.FailedPrecondition,
			"cannot delete snapshot: has active volumes %v. Delete these volumes first using 'kubectl delete pvc'.",
			activeClones)
	}

	// Delete orphaned clones (safe - no exports means not in use)
	if len(orphanedClones) > 0 {
		klog.Warningf("Found %d orphaned clones from failed volume deletions, cleaning up: %v", len(orphanedClones), orphanedClones)
		for _, orphan := range orphanedClones {
			klog.Infof("Deleting orphaned clone: %s", orphan)
			if err := d.truenasClient.DatasetDelete(ctx, orphan, false, true); err != nil {
				// Log but don't fail - the clone might have been deleted by another operation
				if !truenas.IsNotFoundError(err) {
					klog.Warningf("Failed to delete orphaned clone %s: %v", orphan, err)
				}
			}
		}
	}

	return nil
}

// cloneHasActiveExport checks if a clone dataset has an active NFS share, iSCSI extent, or NVMe namespace.
func (d *Driver) cloneHasActiveExport(ctx context.Context, datasetPath, datasetType string) (bool, error) {
	// For filesystem datasets, check NFS shares
	if datasetType == "FILESYSTEM" {
		// NFS shares use the mount path which is typically /mnt/{pool}/{dataset}
		mountPath := "/mnt/" + datasetPath
		share, err := d.truenasClient.NFSShareFindByPath(ctx, mountPath)
		if err != nil {
			return false, fmt.Errorf("failed to check NFS share: %w", err)
		}
		if share != nil {
			return true, nil
		}
		return false, nil
	}

	// For zvols, check iSCSI extents and NVMe namespaces
	if datasetType == "VOLUME" {
		// iSCSI extents use zvol path like "zvol/{pool}/{dataset}"
		zvolPath := "zvol/" + datasetPath
		extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, zvolPath)
		if err != nil {
			return false, fmt.Errorf("failed to check iSCSI extent: %w", err)
		}
		if extent != nil {
			return true, nil
		}

		// NVMe namespaces also use zvol path
		namespace, err := d.truenasClient.NVMeoFNamespaceFindByDevicePath(ctx, zvolPath)
		if err != nil {
			return false, fmt.Errorf("failed to check NVMe namespace: %w", err)
		}
		if namespace != nil {
			return true, nil
		}

		return false, nil
	}

	// Unknown type - assume has export to be safe
	klog.Warningf("Unknown dataset type %s for %s, assuming has active export", datasetType, datasetPath)
	return true, nil
}

// ListSnapshots lists snapshots.
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Info("ListSnapshots called")

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
		// Skip if not managed by CSI
		if prop, ok := snap.UserProperties[PropManagedResource]; !ok || prop.Value != "true" {
			continue
		}

		// Extract snapshot name safely
		snapshotID, ok := extractSnapshotName(snap.ID)
		if !ok {
			klog.V(4).Infof("Skipping snapshot with invalid ID format: %s", snap.ID)
			continue
		}

		// Filter by snapshot ID if specified
		if req.GetSnapshotId() != "" {
			if snapshotID != req.GetSnapshotId() {
				continue
			}
		}

		// Filter by source volume if specified
		sourceVolumeID := ""
		if prop, ok := snap.UserProperties[PropCSISnapshotSourceVolumeID]; ok {
			sourceVolumeID = prop.Value
		}
		if req.GetSourceVolumeId() != "" && sourceVolumeID != req.GetSourceVolumeId() {
			continue
		}

		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snapshotID,
				SourceVolumeId: sourceVolumeID,
				SizeBytes:      snap.GetSnapshotSize(),
				CreationTime:   timestampProto(snap.GetCreationTime()),
				ReadyToUse:     true,
			},
		})
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
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
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

	klog.Infof("ControllerExpandVolume: volumeID=%s, capacity=%d", volumeID, capacityBytes)

	// Lock on volume ID to prevent concurrent expansions of same volume
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)

	// Get dataset to determine type and current state
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume details: %v", err)
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

	datasetName := path.Join(d.config.ZFS.DatasetParentName, volumeID)
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume not found: %s", volumeID)
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: d.getDatasetCapacity(ds),
		},
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: false,
				Message:  "volume is healthy",
			},
		},
	}, nil
}

// ControllerModifyVolume modifies a volume (not implemented).
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerModifyVolume not implemented")
}

// Helper functions

func (d *Driver) sanitizeVolumeID(name string) string {
	// Remove invalid characters for ZFS dataset names
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, " ", "-")
	// Ensure it starts with alphanumeric
	if name != "" && !isAlphanumeric(name[0]) {
		name = "v" + name
	}
	// Limit length (ZFS has a 256 char limit, CSI has 128)
	if len(name) > 128 {
		name = name[:128]
	}
	return name
}

func isAlphanumeric(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func (d *Driver) getDatasetCapacity(ds *truenas.Dataset) int64 {
	// For zvols, use volsize
	if ds.Type == "VOLUME" {
		if parsed, ok := ds.Volsize.Parsed.(float64); ok {
			return int64(parsed)
		}
	}
	// For filesystems, use quota or available
	if parsed, ok := ds.Refquota.Parsed.(float64); ok && parsed > 0 {
		return int64(parsed)
	}
	if parsed, ok := ds.Available.Parsed.(float64); ok {
		return int64(parsed)
	}
	return 0
}

// createDataset creates a new ZFS dataset or zvol.
// Returns the mountpoint for NFS datasets (empty string for zvols).
func (d *Driver) createDataset(ctx context.Context, datasetName string, capacityBytes int64, shareType ShareType) (string, error) {
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
		params.Sparse = true
	}

	ds, err := d.truenasClient.DatasetCreate(ctx, params)
	if err != nil {
		return "", err
	}
	// Return mountpoint for NFS shares (avoid extra DatasetGet call in createNFSShare)
	return ds.Mountpoint, nil
}

func (d *Driver) handleVolumeContentSource(ctx context.Context, datasetName string, source *csi.VolumeContentSource, capacityBytes int64) error {
	// Timeout for waiting for cloned dataset to be ready (configurable via zfs.zvolReadyTimeout)
	cloneReadyTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second

	if snapshot := source.GetSnapshot(); snapshot != nil {
		// Clone from snapshot
		snapshotID := snapshot.GetSnapshotId()
		klog.Infof("Creating volume from snapshot: %s -> %s", snapshotID, datasetName)

		// Find the snapshot using efficient query (PERF-001 fix)
		snap, err := d.truenasClient.SnapshotFindByName(ctx, d.config.ZFS.DatasetParentName, snapshotID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to find snapshot: %v", err)
		}

		if snap == nil {
			return status.Errorf(codes.NotFound, "snapshot not found: %s", snapshotID)
		}

		sourceSnapshot := snap.ID
		klog.V(4).Infof("Found snapshot %s for cloning", sourceSnapshot)

		if cloneErr := d.truenasClient.SnapshotClone(ctx, sourceSnapshot, datasetName); cloneErr != nil {
			return status.Errorf(codes.Internal, "failed to clone snapshot: %v", cloneErr)
		}
		klog.Infof("Snapshot clone created: %s -> %s", sourceSnapshot, datasetName)

		// Wait for cloned dataset to be ready before proceeding
		// This is critical for iSCSI/NVMe-oF where extent creation needs the zvol
		ds, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			klog.Warningf("Clone readiness check failed (will continue): %v", err)
		} else if ds.Type == "VOLUME" && capacityBytes > 0 {
			// Check if we need to expand the cloned volume to match requested capacity
			if currentSize, ok := ds.Volsize.Parsed.(float64); ok {
				if capacityBytes > int64(currentSize) {
					klog.Infof("Expanding cloned zvol from %d to %d bytes", int64(currentSize), capacityBytes)
					if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
						klog.Warningf("Failed to expand cloned zvol (will continue with original size): %v", err)
					}
				}
			}
		}

		// Set content source properties in parallel
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropVolumeContentSourceType, "snapshot"); err != nil {
				klog.Warningf("Failed to set volume content source type property: %v", err)
			}
			return nil
		})
		g.Go(func() error {
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropVolumeContentSourceID, snapshotID); err != nil {
				klog.Warningf("Failed to set volume content source ID property: %v", err)
			}
			return nil
		})
		if err := g.Wait(); err != nil {
			klog.Warningf("Error setting content source properties for snapshot clone: %v", err)
		}

	} else if volume := source.GetVolume(); volume != nil {
		// Clone from volume
		sourceVolumeID := volume.GetVolumeId()
		sourceDataset := path.Join(d.config.ZFS.DatasetParentName, sourceVolumeID)
		klog.Infof("Creating volume from volume: %s -> %s", sourceVolumeID, datasetName)

		// Create a snapshot of source volume, then clone it
		tempSnapshotName := fmt.Sprintf("clone-source-%s", d.sanitizeVolumeID(path.Base(datasetName)))
		snap, err := d.truenasClient.SnapshotCreate(ctx, sourceDataset, tempSnapshotName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create source snapshot: %v", err)
		}
		klog.V(4).Infof("Created temporary snapshot %s for volume clone", snap.ID)

		if cloneErr := d.truenasClient.SnapshotClone(ctx, snap.ID, datasetName); cloneErr != nil {
			if delErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); delErr != nil {
				klog.Warningf("Failed to cleanup snapshot after clone failure: %v", delErr)
			}
			return status.Errorf(codes.Internal, "failed to clone volume: %v", cloneErr)
		}
		klog.Infof("Volume clone created: %s -> %s", sourceVolumeID, datasetName)

		// Wait for cloned dataset to be ready
		ds, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			klog.Warningf("Clone readiness check failed (will continue): %v", err)
		} else if ds.Type == "VOLUME" && capacityBytes > 0 {
			// Check if we need to expand the cloned volume
			if currentSize, ok := ds.Volsize.Parsed.(float64); ok {
				if capacityBytes > int64(currentSize) {
					klog.Infof("Expanding cloned zvol from %d to %d bytes", int64(currentSize), capacityBytes)
					if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
						klog.Warningf("Failed to expand cloned zvol (will continue with original size): %v", err)
					}
				}
			}
		}

		// Set content source properties in parallel
		// Include origin snapshot so it can be cleaned up when the cloned volume is deleted
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropVolumeContentSourceType, "volume"); err != nil {
				klog.Warningf("Failed to set volume content source type property: %v", err)
			}
			return nil
		})
		g.Go(func() error {
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropVolumeContentSourceID, sourceVolumeID); err != nil {
				klog.Warningf("Failed to set volume content source ID property: %v", err)
			}
			return nil
		})
		g.Go(func() error {
			// Store the origin snapshot ID for cleanup when the clone is deleted
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropVolumeOriginSnapshot, snap.ID); err != nil {
				klog.Warningf("Failed to set volume origin snapshot property: %v", err)
			}
			return nil
		})
		if err := g.Wait(); err != nil {
			klog.Warningf("Error setting content source properties for volume clone: %v", err)
		}
	}

	return nil
}

func (d *Driver) getVolumeContext(ctx context.Context, datasetName string, shareType ShareType) (map[string]string, error) {
	volumeContext := map[string]string{
		"node_attach_driver": shareType.String(),
	}

	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, err
	}

	switch shareType {
	case ShareTypeNFS:
		volumeContext["server"] = d.config.NFS.ShareHost
		volumeContext["share"] = ds.Mountpoint

	case ShareTypeISCSI:
		// Get target info from dataset properties or fallback to lookup by name
		var target *truenas.ISCSITarget
		var globalCfg *truenas.ISCSIGlobalConfig

		if prop, ok := ds.UserProperties[PropISCSITargetID]; ok && prop.Value != "" && prop.Value != "-" {
			targetID, err := strconv.Atoi(prop.Value)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "invalid target ID: %v", err)
			}

			// Fetch target and global config in parallel for better performance
			g, gCtx := errgroup.WithContext(ctx)
			g.Go(func() error {
				var err error
				target, err = d.truenasClient.ISCSITargetGet(gCtx, targetID)
				if err != nil {
					return status.Errorf(codes.Internal, "failed to get iSCSI target: %v", err)
				}
				return nil
			})
			g.Go(func() error {
				var err error
				globalCfg, err = d.truenasClient.ISCSIGlobalConfigGet(gCtx)
				if err != nil {
					return status.Errorf(codes.Internal, "failed to get iSCSI global config: %v", err)
				}
				return nil
			})
			if err := g.Wait(); err != nil {
				return nil, err
			}
		} else {
			// Fallback: lookup target by name (for volumes created before property tracking)
			iscsiName := path.Base(datasetName)
			if d.config.ISCSI.NameSuffix != "" {
				iscsiName += d.config.ISCSI.NameSuffix
			}
			klog.V(4).Infof("Target ID property missing for %s, falling back to name lookup: %s", datasetName, iscsiName)

			var err error
			target, err = d.truenasClient.ISCSITargetFindByName(ctx, iscsiName)
			if err != nil || target == nil {
				return nil, status.Errorf(codes.Internal, "failed to find iSCSI target by name %s: %v", iscsiName, err)
			}
			globalCfg, err = d.truenasClient.ISCSIGlobalConfigGet(ctx)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get iSCSI global config: %v", err)
			}
		}
		volumeContext["iqn"] = fmt.Sprintf("%s:%s", globalCfg.Basename, target.Name)
		volumeContext["portal"] = d.config.ISCSI.TargetPortal
		volumeContext["lun"] = "0"
		volumeContext["interface"] = d.config.ISCSI.Interface

	case ShareTypeNVMeoF:
		// Get subsystem info from dataset properties or fallback to lookup by name
		var subsys *truenas.NVMeoFSubsystem

		if prop, ok := ds.UserProperties[PropNVMeoFSubsystemID]; ok && prop.Value != "" && prop.Value != "-" {
			subsysID, err := strconv.Atoi(prop.Value)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "invalid subsystem ID: %v", err)
			}
			subsys, err = d.truenasClient.NVMeoFSubsystemGet(ctx, subsysID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get NVMe-oF subsystem: %v", err)
			}
		} else {
			// Fallback: lookup subsystem by name (for volumes created before property tracking)
			subsysName := path.Base(datasetName)
			if d.config.NVMeoF.NamePrefix != "" {
				subsysName = d.config.NVMeoF.NamePrefix + subsysName
			}
			if d.config.NVMeoF.NameSuffix != "" {
				subsysName += d.config.NVMeoF.NameSuffix
			}
			klog.V(4).Infof("Subsystem ID property missing for %s, falling back to name lookup: %s", datasetName, subsysName)

			var err error
			subsys, err = d.truenasClient.NVMeoFSubsystemFindByName(ctx, subsysName)
			if err != nil || subsys == nil {
				return nil, status.Errorf(codes.Internal, "failed to find NVMe-oF subsystem by name %s: %v", subsysName, err)
			}
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
