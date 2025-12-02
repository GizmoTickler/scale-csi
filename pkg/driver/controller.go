package driver

import (
	"context"
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
	PropCSISnapshotName           = "truenas-csi:csi_snapshot_name"
	PropCSISnapshotSourceVolumeID = "truenas-csi:csi_snapshot_source_volume_id"
	PropNFSShareID                = "truenas-csi:truenas_nfs_share_id"
	PropISCSITargetID             = "truenas-csi:truenas_iscsi_target_id"
	PropISCSIExtentID             = "truenas-csi:truenas_iscsi_extent_id"
	PropISCSITargetExtentID       = "truenas-csi:truenas_iscsi_targetextent_id"
	PropNVMeoFSubsystemID         = "truenas-csi:truenas_nvmeof_subsystem_id"
	PropNVMeoFNamespaceID         = "truenas-csi:truenas_nvmeof_namespace_id"
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

	// Lock on volume name to prevent concurrent creates
	lockKey := "volume:" + name
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress for this volume")
	}
	defer d.releaseOperationLock(lockKey)

	// Calculate capacity
	capacityBytes := int64(0)
	if req.GetCapacityRange() != nil {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
	}
	if capacityBytes == 0 {
		capacityBytes = 1024 * 1024 * 1024 // Default 1GiB
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
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropManagedResource, "true"); err != nil {
				return fmt.Errorf("failed to set managed resource property: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			if prop, ok := existingDS.UserProperties[PropProvisionSuccess]; ok && prop.Value == "true" {
				return nil
			}
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropProvisionSuccess, "true"); err != nil {
				return fmt.Errorf("failed to set provision success property: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			if prop, ok := existingDS.UserProperties[PropCSIVolumeName]; ok && prop.Value == name {
				return nil
			}
			if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropCSIVolumeName, name); err != nil {
				return fmt.Errorf("failed to set CSI volume name property: %w", err)
			}
			return nil
		})
		// Wait for all property sets to complete
		if err := g.Wait(); err != nil {
			klog.Errorf("Failed to ensure properties for existing volume %s: %v", volumeID, err)
			return nil, status.Errorf(codes.Internal, "failed to ensure volume properties: %v", err)
		}

		// CRITICAL: Ensure share exists for existing volumes (fixes missing iSCSI targets after retries)
		// This handles the case where a previous CreateVolume created the dataset but failed
		// to create the share (e.g., due to timeout, TrueNAS API error, etc.)
		if err := d.ensureShareExists(ctx, existingDS, datasetName, name, shareType); err != nil {
			return nil, err
		}

		volumeContext, err := d.getVolumeContext(ctx, datasetName, shareType)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
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
	if req.GetVolumeContentSource() != nil {
		contentSource = req.GetVolumeContentSource()
		if err := d.handleVolumeContentSource(ctx, datasetName, contentSource, capacityBytes); err != nil {
			return nil, err
		}
	} else {
		// Create new dataset
		if err := d.createDataset(ctx, datasetName, capacityBytes, shareType); err != nil {
			return nil, err
		}
	}

	// Create share (NFS, iSCSI, or NVMe-oF)
	if err := d.createShare(ctx, datasetName, name, shareType); err != nil {
		// Cleanup on failure
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, false, false); delErr != nil {
			klog.Warningf("Failed to cleanup dataset after share creation failure: %v", delErr)
		}
		return nil, err
	}

	// Mark as managed and successful - run property sets in parallel
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropManagedResource, "true"); err != nil {
			return fmt.Errorf("failed to set managed resource property: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropProvisionSuccess, "true"); err != nil {
			return fmt.Errorf("failed to set provision success property: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := d.truenasClient.DatasetSetUserProperty(gCtx, datasetName, PropCSIVolumeName, name); err != nil {
			return fmt.Errorf("failed to set CSI volume name property: %w", err)
		}
		return nil
	})
	// Wait for all property sets to complete
	if err := g.Wait(); err != nil {
		// If property setting fails, return error so it retries
		klog.Errorf("Failed to set properties for volume %s: %v", volumeID, err)
		return nil, status.Errorf(codes.Internal, "failed to set volume properties: %v", err)
	}

	// Get volume context for response
	volumeContext, err := d.getVolumeContext(ctx, datasetName, shareType)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume context: %v", err)
	}

	klog.Infof("CreateVolume completed: volume=%s, shareType=%s, contentSource=%s, elapsed=%v",
		volumeID, shareType, contentSourceInfo, time.Since(start))

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
			VolumeContext: volumeContext,
			ContentSource: contentSource,
		},
	}, nil
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
			klog.Infof("Volume %s already deleted or does not exist", volumeID)
			return &csi.DeleteVolumeResponse{}, nil
		}
		// Log but don't fail - try to proceed with deletion anyway
		klog.V(4).Infof("Could not verify volume existence: %v", err)
	}

	// Determine share type from dataset type
	// Filesystem = NFS, Volume (zvol) = iSCSI or NVMe-oF
	shareType := d.config.GetDriverShareType() // fallback to driver name
	if ds != nil {
		switch ds.Type {
		case "FILESYSTEM":
			shareType = ShareTypeNFS
		case "VOLUME":
			// For zvol, prefer iSCSI unless driver specifically configured for NVMe-oF
			if d.config.GetDriverShareType() == ShareTypeNVMeoF {
				shareType = ShareTypeNVMeoF
			} else {
				shareType = ShareTypeISCSI
			}
		}
	}

	// Delete share first (errors are fatal to prevent orphaned targets)
	if err := d.deleteShare(ctx, datasetName, shareType); err != nil {
		klog.Errorf("Failed to delete share for volume %s: %v", volumeID, err)
		return nil, status.Errorf(codes.Internal, "failed to delete share: %v", err)
	}

	// Delete dataset (recursive to handle snapshots, force to ignore busy state)
	if err := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); err != nil {
		// DatasetDelete already handles "not found" errors, so this is a real error
		klog.Errorf("Failed to delete dataset for volume %s: %v", volumeID, err)
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}

	klog.Infof("Volume %s deleted successfully", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to a node (not used for NFS).
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// Not implemented - NFS doesn't require controller publish
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
	limit := int(req.GetMaxEntries())
	if limit == 0 {
		limit = 100
	}

	datasets, err := d.truenasClient.DatasetList(ctx, d.config.ZFS.DatasetParentName, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list volumes: %v", err)
	}

	entries := make([]*csi.ListVolumesResponse_Entry, 0)
	for _, ds := range datasets {
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

	// Generate next token if we got a full page
	nextToken := ""
	if len(datasets) == limit {
		nextToken = strconv.Itoa(offset + limit)
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
	// Set snapshot properties in parallel
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropManagedResource, "true"); err != nil {
			return fmt.Errorf("failed to set managed resource property on snapshot: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropCSISnapshotName, name); err != nil {
			return fmt.Errorf("failed to set CSI snapshot name property: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := d.truenasClient.SnapshotSetUserProperty(gCtx, snap.ID, PropCSISnapshotSourceVolumeID, sourceVolumeID); err != nil {
			return fmt.Errorf("failed to set CSI snapshot source volume ID property: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		klog.Errorf("Failed to set properties for snapshot %s: %v", snapshotID, err)
		return nil, status.Errorf(codes.Internal, "failed to set snapshot properties: %v", err)
	}

	snapshotSize := snap.GetSnapshotSize()
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
		klog.Errorf("Failed to delete snapshot %s: %v", snapshotID, err)
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
	}
	klog.Infof("Snapshot %s deleted successfully", snapshotID)

	return &csi.DeleteSnapshotResponse{}, nil
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

	// For zvols (iSCSI/NVMe-oF), expand the volsize
	if d.config.GetZFSResourceType() == "volume" {
		if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to expand volume: %v", err)
		}
	}

	// For filesystems (NFS), update quota if enabled
	if d.config.GetZFSResourceType() == "filesystem" && d.config.ZFS.DatasetEnableQuotas {
		params := &truenas.DatasetUpdateParams{
			Refquota: capacityBytes,
		}
		if _, err := d.truenasClient.DatasetUpdate(ctx, datasetName, params); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to update quota: %v", err)
		}
	}

	// Node expansion may be required for filesystems
	nodeExpansionRequired := d.config.GetZFSResourceType() == "volume"

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
	if len(name) > 0 && !isAlphanumeric(name[0]) {
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

func (d *Driver) createDataset(ctx context.Context, datasetName string, capacityBytes int64, shareType ShareType) error {
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

	_, err := d.truenasClient.DatasetCreate(ctx, params)
	return err
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

		if err := d.truenasClient.SnapshotClone(ctx, sourceSnapshot, datasetName); err != nil {
			return status.Errorf(codes.Internal, "failed to clone snapshot: %v", err)
		}
		klog.Infof("Snapshot clone created: %s -> %s", sourceSnapshot, datasetName)

		// Wait for cloned dataset to be ready before proceeding
		// This is critical for iSCSI/NVMe-oF where extent creation needs the zvol
		ds, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			klog.Warningf("Clone readiness check failed (will continue): %v", err)
		} else {
			// Check if we need to expand the cloned volume to match requested capacity
			if ds.Type == "VOLUME" && capacityBytes > 0 {
				if currentSize, ok := ds.Volsize.Parsed.(float64); ok {
					if capacityBytes > int64(currentSize) {
						klog.Infof("Expanding cloned zvol from %d to %d bytes", int64(currentSize), capacityBytes)
						if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
							klog.Warningf("Failed to expand cloned zvol (will continue with original size): %v", err)
						}
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

		if err := d.truenasClient.SnapshotClone(ctx, snap.ID, datasetName); err != nil {
			if delErr := d.truenasClient.SnapshotDelete(ctx, snap.ID, false, false); delErr != nil {
				klog.Warningf("Failed to cleanup snapshot after clone failure: %v", delErr)
			}
			return status.Errorf(codes.Internal, "failed to clone volume: %v", err)
		}
		klog.Infof("Volume clone created: %s -> %s", sourceVolumeID, datasetName)

		// Wait for cloned dataset to be ready
		ds, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, cloneReadyTimeout)
		if err != nil {
			klog.Warningf("Clone readiness check failed (will continue): %v", err)
		} else {
			// Check if we need to expand the cloned volume
			if ds.Type == "VOLUME" && capacityBytes > 0 {
				if currentSize, ok := ds.Volsize.Parsed.(float64); ok {
					if capacityBytes > int64(currentSize) {
						klog.Infof("Expanding cloned zvol from %d to %d bytes", int64(currentSize), capacityBytes)
						if err := d.truenasClient.DatasetExpand(ctx, datasetName, capacityBytes); err != nil {
							klog.Warningf("Failed to expand cloned zvol (will continue with original size): %v", err)
						}
					}
				}
			}
		}

		// Set content source properties in parallel
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
		if err := g.Wait(); err != nil {
			klog.Warningf("Error setting content source properties for volume clone: %v", err)
		}
	}

	return nil
}

func (d *Driver) getVolumeContext(ctx context.Context, datasetName string, shareType ShareType) (map[string]string, error) {
	context := map[string]string{
		"node_attach_driver": shareType.String(),
	}

	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, err
	}

	switch shareType {
	case ShareTypeNFS:
		context["server"] = d.config.NFS.ShareHost
		context["share"] = ds.Mountpoint

	case ShareTypeISCSI:
		// Get target info from dataset properties
		if prop, ok := ds.UserProperties[PropISCSITargetID]; ok {
			targetID, err := strconv.Atoi(prop.Value)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "invalid target ID: %v", err)
			}
			target, err := d.truenasClient.ISCSITargetGet(ctx, targetID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get iSCSI target: %v", err)
			}
			globalCfg, err := d.truenasClient.ISCSIGlobalConfigGet(ctx)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get iSCSI global config: %v", err)
			}
			context["iqn"] = fmt.Sprintf("%s:%s", globalCfg.Basename, target.Name)
		}
		context["portal"] = d.config.ISCSI.TargetPortal
		context["lun"] = "0"
		context["interface"] = d.config.ISCSI.Interface

	case ShareTypeNVMeoF:
		// Get subsystem info from dataset properties
		if prop, ok := ds.UserProperties[PropNVMeoFSubsystemID]; ok {
			subsysID, err := strconv.Atoi(prop.Value)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "invalid subsystem ID: %v", err)
			}
			subsys, err := d.truenasClient.NVMeoFSubsystemGet(ctx, subsysID)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get NVMe-oF subsystem: %v", err)
			}
			context["nqn"] = subsys.NQN
		}
		context["transport"] = d.config.NVMeoF.Transport
		context["address"] = d.config.NVMeoF.TransportAddress
		context["port"] = strconv.Itoa(d.config.NVMeoF.TransportServiceID)
	}

	return context, nil
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
