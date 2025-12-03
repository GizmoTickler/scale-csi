package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// NodeGetCapabilities returns the capabilities of the node service.
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Info("NodeGetCapabilities called")

	caps := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
				},
			},
		},
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

// NodeGetInfo returns information about the node.
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(4).Info("NodeGetInfo called")

	resp := &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}

	// Add topology information if enabled
	if d.config.Node.Topology.Enabled {
		topology := make(map[string]string)

		// Add standard topology keys
		if d.config.Node.Topology.Zone != "" {
			topology["topology.kubernetes.io/zone"] = d.config.Node.Topology.Zone
		}
		if d.config.Node.Topology.Region != "" {
			topology["topology.kubernetes.io/region"] = d.config.Node.Topology.Region
		}

		// Add custom labels
		for k, v := range d.config.Node.Topology.CustomLabels {
			topology[k] = v
		}

		if len(topology) > 0 {
			resp.AccessibleTopology = &csi.Topology{
				Segments: topology,
			}
			klog.V(4).Infof("NodeGetInfo: returning topology %v", topology)
		}
	}

	return resp, nil
}

// NodeStageVolume mounts a volume to a staging path.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()
	volumeContext := req.GetVolumeContext()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if volumeContext == nil {
		return nil, status.Error(codes.InvalidArgument, "volume context is required")
	}

	klog.Infof("NodeStageVolume: volumeID=%s, stagingPath=%s", volumeID, stagingPath)

	// Lock on volume ID
	lockKey := "node-stage:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Get attach driver from volume context and normalize
	attachDriver := ParseShareType(volumeContext["node_attach_driver"])
	if volumeContext["node_attach_driver"] == "" {
		attachDriver = d.config.GetDriverShareType()
	}

	// Ensure staging directory exists
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging directory: %v", err)
	}

	switch attachDriver {
	case ShareTypeNFS:
		if err := d.stageNFSVolume(ctx, volumeContext, stagingPath); err != nil {
			return nil, err
		}
	case ShareTypeISCSI:
		if err := d.stageISCSIVolume(ctx, volumeContext, stagingPath, req.GetVolumeCapability()); err != nil {
			return nil, err
		}
	case ShareTypeNVMeoF:
		if err := d.stageNVMeoFVolume(ctx, volumeContext, stagingPath, req.GetVolumeCapability()); err != nil {
			return nil, err
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported attach driver: %s (supported: %v)", attachDriver, ValidShareTypeStrings())
	}

	klog.Infof("Volume %s staged successfully at %s", volumeID, stagingPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts a volume from the staging path.
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	stagingPath := req.GetStagingTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	klog.Infof("NodeUnstageVolume: volumeID=%s, stagingPath=%s", volumeID, stagingPath)

	// Lock on volume ID
	lockKey := "node-unstage:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Get device path before unmounting (for session cleanup)
	devicePath, err := util.GetDeviceFromMountPoint(stagingPath)
	if err != nil {
		// If not mounted, we can't get the device path, so we can't cleanup session
		// This is expected if already unstaged
		klog.V(4).Infof("Could not get device from mount point %s: %v", stagingPath, err)
	}

	// Unmount staging path
	if err := util.Unmount(stagingPath); err != nil {
		klog.Warningf("Failed to unmount staging path: %v", err)
		// Check if still mounted before attempting removal to prevent data corruption
		mounted, checkErr := util.IsMounted(stagingPath)
		if checkErr != nil {
			klog.Warningf("Failed to check mount status after unmount failure: %v", checkErr)
			// If we can't verify mount status, don't risk removing a mounted path
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path and cannot verify mount status: %v", err)
		}
		if mounted {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging path (still mounted): %v", err)
		}
		// Not mounted, safe to continue with cleanup
		klog.Infof("Staging path %s is not mounted, proceeding with cleanup", stagingPath)
	}

	// Clean up staging directory (only reached if unmount succeeded or path was not mounted)
	if err := os.RemoveAll(stagingPath); err != nil {
		klog.Warningf("Failed to remove staging directory: %v", err)
	}

	// Disconnect session if we found a device
	if devicePath != "" {
		if strings.Contains(devicePath, "nvme") {
			// NVMe-oF cleanup
			nqn, err := util.GetNVMeInfoFromDevice(devicePath)
			if err == nil {
				if err := util.NVMeoFDisconnect(nqn); err != nil {
					klog.Warningf("Failed to disconnect NVMe-oF session %s: %v", nqn, err)
				} else {
					klog.Infof("Disconnected NVMe-oF session %s", nqn)
				}
			} else {
				klog.V(4).Infof("Could not get NVMe info from device %s: %v", devicePath, err)
			}
		} else {
			// Try iSCSI cleanup
			portal, iqn, err := util.GetISCSIInfoFromDevice(devicePath)
			if err == nil {
				if err := util.ISCSIDisconnect(portal, iqn); err != nil {
					klog.Warningf("Failed to disconnect iSCSI session %s: %v", iqn, err)
				} else {
					klog.Infof("Disconnected iSCSI session %s", iqn)
				}
			} else {
				klog.V(4).Infof("Could not get iSCSI info from device %s: %v", devicePath, err)
			}
		}
	}

	// Always attempt cleanup by volumeID as a safety net.
	// This catches sessions that weren't disconnected because devicePath was empty,
	// device info extraction failed, remain from previous failed unstage attempts,
	// or some edge case left a duplicate session.
	// The cleanup function is idempotent - it will simply log "no session found"
	// if no session remains after the disconnect above.
	d.cleanupOrphanedSessionByVolumeID(ctx, volumeID)

	klog.Infof("Volume %s unstaged successfully", volumeID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// cleanupOrphanedSessionByVolumeID attempts to clean up iSCSI/NVMe-oF sessions
// when the device path is unavailable (e.g., after node restart or force unmount).
// This prevents session leaks that accumulate over time.
func (d *Driver) cleanupOrphanedSessionByVolumeID(ctx context.Context, volumeID string) {
	shareType := d.config.GetDriverShareType()

	switch shareType {
	case ShareTypeISCSI:
		// Try to find and disconnect any iSCSI session for this volume
		portal := d.config.ISCSI.TargetPortal

		// Construct the expected target name (same logic as createISCSIShare)
		// Target name = volumeID + optional NameSuffix
		targetName := volumeID
		if d.config.ISCSI.NameSuffix != "" {
			targetName = targetName + d.config.ISCSI.NameSuffix
		}

		// Check if there's an active session for this target
		iqn, err := util.FindISCSISessionByTargetName(targetName)
		if err != nil {
			klog.V(4).Infof("No active iSCSI session found for volume %s (target: %s): %v", volumeID, targetName, err)
			return
		}

		if iqn != "" {
			klog.Infof("Found orphaned iSCSI session for volume %s: %s", volumeID, iqn)
			if err := util.ISCSIDisconnect(portal, iqn); err != nil {
				klog.Warningf("Failed to disconnect orphaned iSCSI session %s: %v", iqn, err)
			} else {
				klog.Infof("Successfully cleaned up orphaned iSCSI session %s", iqn)
			}
		}

	case ShareTypeNVMeoF:
		// Construct the expected NQN (same logic as createNVMeoFShare)
		nqnName := volumeID
		if d.config.NVMeoF.NamePrefix != "" {
			nqnName = d.config.NVMeoF.NamePrefix + nqnName
		}
		if d.config.NVMeoF.NameSuffix != "" {
			nqnName = nqnName + d.config.NVMeoF.NameSuffix
		}

		// Try to find and disconnect any NVMe-oF session for this volume
		nqn, err := util.FindNVMeoFSessionBySubsysName(nqnName)
		if err != nil {
			klog.V(4).Infof("No active NVMe-oF session found for volume %s (nqn: %s): %v", volumeID, nqnName, err)
			return
		}

		if nqn != "" {
			klog.Infof("Found orphaned NVMe-oF session for volume %s: %s", volumeID, nqn)
			if err := util.NVMeoFDisconnect(nqn); err != nil {
				klog.Warningf("Failed to disconnect orphaned NVMe-oF session %s: %v", nqn, err)
			} else {
				klog.Infof("Successfully cleaned up orphaned NVMe-oF session %s", nqn)
			}
		}
	}
}

// NodePublishVolume mounts a volume to a target path.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	klog.Infof("NodePublishVolume: volumeID=%s, targetPath=%s, stagingPath=%s", volumeID, targetPath, stagingPath)

	// Lock on volume ID
	lockKey := "node-publish:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Ensure target directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target directory: %v", err)
	}

	// Check if already mounted
	mounted, err := util.IsMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		klog.Infof("Volume %s already mounted at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Determine mount options
	readonly := req.GetReadonly()
	mountOptions := []string{}
	if readonly {
		mountOptions = append(mountOptions, "ro")
	}

	// Add volume capability mount flags
	if req.GetVolumeCapability() != nil {
		if mount := req.GetVolumeCapability().GetMount(); mount != nil {
			mountOptions = append(mountOptions, mount.GetMountFlags()...)
		}
	}

	// Bind mount from staging path to target path
	if stagingPath != "" {
		if err := os.MkdirAll(targetPath, 0750); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create target path: %v", err)
		}
		if err := util.BindMount(stagingPath, targetPath, mountOptions); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to bind mount: %v", err)
		}
	} else {
		// Direct mount (legacy mode without staging)
		volumeContext := req.GetVolumeContext()
		attachDriver := ParseShareType(volumeContext["node_attach_driver"])
		if volumeContext["node_attach_driver"] == "" {
			attachDriver = d.config.GetDriverShareType()
		}

		switch attachDriver {
		case ShareTypeNFS:
			server := volumeContext["server"]
			share := volumeContext["share"]
			source := fmt.Sprintf("%s:%s", server, share)
			if err := util.MountNFS(source, targetPath, mountOptions); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to mount NFS: %v", err)
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "staging path required for block volumes")
		}
	}

	klog.Infof("Volume %s published successfully at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts a volume from the target path.
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	klog.Infof("NodeUnpublishVolume: volumeID=%s, targetPath=%s", volumeID, targetPath)

	// Lock on volume ID
	lockKey := "node-unpublish:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Unmount target path
	if err := util.Unmount(targetPath); err != nil {
		klog.Warningf("Failed to unmount target path: %v", err)
		// Check if still mounted before attempting removal
		mounted, checkErr := util.IsMounted(targetPath)
		if checkErr != nil {
			klog.Warningf("Failed to check mount status after unmount failure: %v", checkErr)
			return nil, status.Errorf(codes.Internal, "failed to unmount target path and cannot verify mount status: %v", err)
		}
		if mounted {
			return nil, status.Errorf(codes.Internal, "failed to unmount target path (still mounted): %v", err)
		}
		klog.Infof("Target path %s is not mounted, proceeding with cleanup", targetPath)
	}

	// Remove target path (only reached if unmount succeeded or path was not mounted)
	if err := os.RemoveAll(targetPath); err != nil {
		klog.Warningf("Failed to remove target path: %v", err)
	}

	klog.Infof("Volume %s unpublished successfully", volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats returns statistics for a volume.
func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}

	klog.V(4).Infof("NodeGetVolumeStats: volumeID=%s, volumePath=%s", volumeID, volumePath)

	// Check if path exists
	if _, err := os.Stat(volumePath); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "volume path not found: %s", volumePath)
	}

	// Get filesystem stats
	stats, err := util.GetFilesystemStats(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume stats: %v", err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: stats.AvailableBytes,
				Total:     stats.TotalBytes,
				Used:      stats.UsedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: stats.AvailableInodes,
				Total:     stats.TotalInodes,
				Used:      stats.UsedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

// NodeExpandVolume expands a volume on the node.
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	klog.Infof("NodeExpandVolume: volumeID=%s, volumePath=%s", volumeID, volumePath)

	// For block volumes (iSCSI/NVMe-oF), resize the filesystem
	shareType := d.config.GetDriverShareType()
	if shareType.IsBlockProtocol() {
		// Find the device and resize filesystem
		if volumePath != "" {
			if err := util.ResizeFilesystem(volumePath); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", err)
			}
		}
	}

	capacityBytes := int64(0)
	if req.GetCapacityRange() != nil {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	klog.Infof("Volume %s expanded successfully", volumeID)
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: capacityBytes,
	}, nil
}

// stageNFSVolume mounts an NFS volume to the staging path.
func (d *Driver) stageNFSVolume(ctx context.Context, volumeContext map[string]string, stagingPath string) error {
	if volumeContext == nil {
		return status.Error(codes.InvalidArgument, "volume context is required for NFS staging")
	}
	server := volumeContext["server"]
	share := volumeContext["share"]

	if server == "" || share == "" {
		return status.Error(codes.InvalidArgument, "NFS server and share are required in volume context")
	}

	source := fmt.Sprintf("%s:%s", server, share)

	// Check if already mounted
	mounted, err := util.IsMounted(stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		klog.Infof("NFS already mounted at %s", stagingPath)
		return nil
	}

	// Mount NFS
	if err := util.MountNFS(source, stagingPath, nil); err != nil {
		return status.Errorf(codes.Internal, "failed to mount NFS: %v", err)
	}

	return nil
}

// stageISCSIVolume connects and mounts an iSCSI volume to the staging path.
func (d *Driver) stageISCSIVolume(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability) error {
	if volumeContext == nil {
		return status.Error(codes.InvalidArgument, "volume context is required for iSCSI staging")
	}
	portal := volumeContext["portal"]
	iqn := volumeContext["iqn"]
	lunStr := volumeContext["lun"]

	if portal == "" || iqn == "" {
		return status.Error(codes.InvalidArgument, "iSCSI portal and IQN are required in volume context")
	}

	// Parse LUN number
	lun := 0
	if lunStr != "" {
		var err error
		lun, err = strconv.Atoi(lunStr)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid LUN number: %s", lunStr)
		}
	}

	// Pre-emptive cleanup: disconnect any existing session for this target
	// This prevents duplicate sessions when a volume moves between nodes
	// or when a previous unstage failed to clean up properly.
	existingIQN, err := util.FindISCSISessionByIQN(iqn)
	if err == nil && existingIQN != "" {
		klog.Infof("Found existing iSCSI session for %s, disconnecting before reconnect", iqn)
		if disconnectErr := util.ISCSIDisconnect(portal, existingIQN); disconnectErr != nil {
			klog.Warningf("Failed to disconnect existing session %s: %v (proceeding anyway)", existingIQN, disconnectErr)
		}
		// Brief pause to allow session cleanup to complete (configurable)
		time.Sleep(time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond)
	}

	// Connect to iSCSI target with configurable timeout
	connectOpts := &util.ISCSIConnectOptions{
		DeviceTimeout:       time.Duration(d.config.ISCSI.DeviceWaitTimeout) * time.Second,
		SessionCleanupDelay: time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond,
	}
	devicePath, err := util.ISCSIConnectWithOptions(ctx, portal, iqn, lun, connectOpts)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to connect iSCSI: %v", err)
	}

	// Check if block mode
	if volCap != nil && volCap.GetBlock() != nil {
		// For block mode, create a symlink to the device
		if err := os.Symlink(devicePath, stagingPath); err != nil && !os.IsExist(err) {
			return status.Errorf(codes.Internal, "failed to create device symlink: %v", err)
		}
		return nil
	}

	// For filesystem mode, format and mount
	fsType := "ext4"
	if volCap != nil && volCap.GetMount() != nil && volCap.GetMount().GetFsType() != "" {
		fsType = volCap.GetMount().GetFsType()
	}

	if err := util.FormatAndMount(devicePath, stagingPath, fsType, nil); err != nil {
		return status.Errorf(codes.Internal, "failed to format and mount: %v", err)
	}

	return nil
}

// stageNVMeoFVolume connects and mounts an NVMe-oF volume to the staging path.
func (d *Driver) stageNVMeoFVolume(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability) error {
	if volumeContext == nil {
		return status.Error(codes.InvalidArgument, "volume context is required for NVMe-oF staging")
	}
	nqn := volumeContext["nqn"]
	transport := volumeContext["transport"]
	address := volumeContext["address"]
	port := volumeContext["port"]

	if nqn == "" || address == "" {
		return status.Error(codes.InvalidArgument, "NVMe-oF NQN and address are required in volume context")
	}

	if transport == "" {
		transport = "tcp"
	}
	if port == "" {
		port = "4420"
	}

	// Pre-emptive cleanup: disconnect any existing session for this NQN
	// This prevents duplicate sessions when a volume moves between nodes
	// or when a previous unstage failed to clean up properly.
	existingNQN, err := util.FindNVMeoFSessionByNQN(nqn)
	if err == nil && existingNQN != "" {
		klog.Infof("Found existing NVMe-oF session for %s, disconnecting before reconnect", nqn)
		if disconnectErr := util.NVMeoFDisconnect(existingNQN); disconnectErr != nil {
			klog.Warningf("Failed to disconnect existing session %s: %v (proceeding anyway)", existingNQN, disconnectErr)
		}
		// Brief pause to allow session cleanup to complete (configurable)
		time.Sleep(time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond)
	}

	// Connect to NVMe-oF subsystem with configurable timeout
	transportURI := fmt.Sprintf("%s://%s:%s", transport, address, port)
	connectOpts := &util.NVMeoFConnectOptions{
		DeviceTimeout: time.Duration(d.config.NVMeoF.DeviceWaitTimeout) * time.Second,
	}
	devicePath, err := util.NVMeoFConnectWithOptions(nqn, transportURI, connectOpts)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to connect NVMe-oF: %v", err)
	}

	// Check if block mode
	if volCap != nil && volCap.GetBlock() != nil {
		// For block mode, create a symlink to the device
		if err := os.Symlink(devicePath, stagingPath); err != nil && !os.IsExist(err) {
			return status.Errorf(codes.Internal, "failed to create device symlink: %v", err)
		}
		return nil
	}

	// For filesystem mode, format and mount
	fsType := "ext4"
	if volCap != nil && volCap.GetMount() != nil && volCap.GetMount().GetFsType() != "" {
		fsType = strings.ToLower(volCap.GetMount().GetFsType())
	}

	if err := util.FormatAndMount(devicePath, stagingPath, fsType, nil); err != nil {
		return status.Errorf(codes.Internal, "failed to format and mount: %v", err)
	}

	return nil
}
