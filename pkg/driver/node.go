package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

var (
	nodeStatsSysfsRoot = "/sys"
	nodeStatsStat      = func(path string) (uint32, uint64, error) {
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			return 0, 0, err
		}
		return uint32(stat.Mode), uint64(stat.Rdev), nil //nolint:unconvert // Stat_t field widths differ per platform (darwin: Mode uint16, Rdev int32)
	}
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
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_VOLUME_CONDITION,
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
	lockKey := nodeVolumeLockKey(volumeID)
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
	if err := os.MkdirAll(stagingPath, 0o750); err != nil {
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
	lockKey := nodeVolumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Get device path before unmounting (for session cleanup)
	// For block mode volumes, stagingPath is a symlink to the device, not a mount point
	devicePath, err := util.GetDeviceFromMountPoint(stagingPath)
	if err != nil || devicePath == "" {
		// Check if it's a symlink (block mode volumes use symlinks)
		if target, readErr := os.Readlink(stagingPath); readErr == nil {
			devicePath = target
			klog.V(4).Infof("Staging path %s is a symlink to device %s", stagingPath, devicePath)
		} else {
			// If not mounted and not a symlink, we can't get the device path
			// This is expected if already unstaged
			klog.V(4).Infof("Could not get device from staging path %s: mount err=%v, symlink err=%v", stagingPath, err, readErr)
		}
	}

	// Check if staging path is a symlink (block mode) or a mount point (filesystem mode)
	fileInfo, statErr := os.Lstat(stagingPath)
	isSymlink := statErr == nil && fileInfo.Mode()&os.ModeSymlink != 0

	if isSymlink {
		// For block mode volumes, just remove the symlink
		klog.V(4).Infof("Staging path %s is a symlink, removing", stagingPath)
		if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
			klog.Warningf("Failed to remove staging symlink: %v", err)
		}
	} else {
		// For filesystem mode, unmount and remove directory
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
	}

	// Disconnect session if we found a device
	if devicePath != "" {
		if strings.Contains(devicePath, "nvme") {
			// NVMe-oF cleanup
			nqn, nvmeErr := util.GetNVMeInfoFromDevice(devicePath)
			if nvmeErr == nil {
				if discErr := util.NVMeoFDisconnect(nqn); discErr != nil {
					klog.Warningf("Failed to disconnect NVMe-oF session %s: %v", nqn, discErr)
				} else {
					klog.Infof("Disconnected NVMe-oF session %s", nqn)
				}
			} else {
				klog.V(4).Infof("Could not get NVMe info from device %s: %v", devicePath, nvmeErr)
			}
		} else {
			// Try iSCSI cleanup
			portal, iqn, iscsiErr := util.GetISCSIInfoFromDevice(devicePath)
			if iscsiErr == nil {
				if discErr := util.ISCSIDisconnect(portal, iqn); discErr != nil {
					klog.Warningf("Failed to disconnect iSCSI session %s: %v", iqn, discErr)
				} else {
					klog.Infof("Disconnected iSCSI session %s", iqn)
				}
			} else {
				klog.V(4).Infof("Could not get iSCSI info from device %s: %v", devicePath, iscsiErr)
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
	_ = ctx

	// Mixed-protocol deployments cannot infer the volume's transport from the
	// driver's global default. Both lookups are exact and no-op when absent.
	portal := d.config.ISCSI.TargetPortal
	targetName := volumeID + d.config.ISCSI.NameSuffix
	iqn, err := util.FindISCSISessionByTargetName(targetName)
	if err != nil {
		klog.V(4).Infof("No active iSCSI session found for volume %s (target: %s): %v", volumeID, targetName, err)
	} else if iqn != "" {
		klog.Infof("Found orphaned iSCSI session for volume %s: %s", volumeID, iqn)
		if disconnectErr := util.ISCSIDisconnect(portal, iqn); disconnectErr != nil {
			klog.Warningf("Failed to disconnect orphaned iSCSI session %s: %v", iqn, disconnectErr)
		} else {
			klog.Infof("Successfully cleaned up orphaned iSCSI session %s", iqn)
		}
	}

	nqnName := d.config.NVMeoF.NamePrefix + volumeID + d.config.NVMeoF.NameSuffix
	nqn, err := util.FindNVMeoFSessionBySubsysName(nqnName)
	if err != nil {
		klog.V(4).Infof("No active NVMe-oF session found for volume %s (nqn: %s): %v", volumeID, nqnName, err)
	} else if nqn != "" {
		klog.Infof("Found orphaned NVMe-oF session for volume %s: %s", volumeID, nqn)
		if err := util.NVMeoFDisconnect(nqn); err != nil {
			klog.Warningf("Failed to disconnect orphaned NVMe-oF session %s: %v", nqn, err)
		} else {
			klog.Infof("Successfully cleaned up orphaned NVMe-oF session %s", nqn)
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
	lockKey := nodeVolumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	// Ensure target directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o750); err != nil {
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
		if req.GetVolumeCapability() != nil && req.GetVolumeCapability().GetBlock() != nil {
			devicePath, resolveErr := filepath.EvalSymlinks(stagingPath)
			if resolveErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to resolve staged block device: %v", resolveErr)
			}
			if !strings.HasPrefix(devicePath, "/dev/") {
				return nil, status.Errorf(codes.Internal, "staging path did not resolve to a block device: %s", devicePath)
			}

			target, openErr := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, 0o640)
			if openErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to create block target file: %v", openErr)
			}
			if closeErr := target.Close(); closeErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to close block target file: %v", closeErr)
			}

			blockMountOptions := []string{}
			if readonly {
				blockMountOptions = append(blockMountOptions, "ro")
			}
			if err := util.BindMount(devicePath, targetPath, blockMountOptions); err != nil {
				return nil, status.Errorf(codes.Internal, "failed to bind mount block device: %v", err)
			}
			klog.Infof("Block volume %s published successfully at %s", volumeID, targetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}

		if err := os.MkdirAll(targetPath, 0o750); err != nil {
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
	lockKey := nodeVolumeLockKey(volumeID)
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

	devicePath, blockMode, err := resolveNodeStatsDevice(volumePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", volumePath)
		}
		return abnormalVolumeStatsResponse(fmt.Sprintf("failed to inspect volume path %s: %v", volumePath, err)), nil
	}
	if blockMode {
		totalBytes, sizeErr := getNodeDeviceSize(devicePath)
		if sizeErr != nil {
			return abnormalVolumeStatsResponse(fmt.Sprintf("failed to get block device size for %s: %v", devicePath, sizeErr)), nil
		}
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{{
				Total: totalBytes,
				Unit:  csi.VolumeUsage_BYTES,
			}},
			VolumeCondition: &csi.VolumeCondition{Abnormal: false},
		}, nil
	}

	// Get filesystem stats
	stats, err := getNodeFilesystemStats(volumePath)
	if err != nil {
		return abnormalVolumeStatsResponse(fmt.Sprintf("failed to get filesystem stats for %s: %v", volumePath, err)), nil
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
		VolumeCondition: &csi.VolumeCondition{Abnormal: false},
	}, nil
}

func abnormalVolumeStatsResponse(message string) *csi.NodeGetVolumeStatsResponse {
	return &csi.NodeGetVolumeStatsResponse{
		VolumeCondition: &csi.VolumeCondition{
			Abnormal: true,
			Message:  message,
		},
	}
}

func nodeStatsDevice(volumePath string) (devicePath string, blockMode bool, err error) {
	mode, rdev, err := nodeStatsStat(volumePath)
	if err != nil {
		return "", false, err
	}
	if mode&unix.S_IFMT == unix.S_IFBLK {
		return fmt.Sprintf("%d:%d", unix.Major(rdev), unix.Minor(rdev)), true, nil
	}

	return "", false, nil
}

func nodeStatsDeviceSize(deviceNumber string) (int64, error) {
	sizePath := filepath.Join(nodeStatsSysfsRoot, "dev", "block", deviceNumber, "size")
	data, err := os.ReadFile(sizePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read block device size from %s: %w", sizePath, err)
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block device size from %s: %w", sizePath, err)
	}
	if sectors < 0 {
		return 0, fmt.Errorf("block device size from %s is negative", sizePath)
	}
	return sectors * 512, nil
}

// NodeExpandVolume expands a volume on the node.
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	klog.Infof("NodeExpandVolume: volumeID=%s, volumePath=%s", volumeID, volumePath)

	lockKey := nodeVolumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	capacityBytes := int64(0)
	if req.GetCapacityRange() != nil {
		capacityBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	devicePath, rawBlock, err := resolveNodeExpansionDevice(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resolve expansion device: %v", err)
	}

	shareType := blockTransportForDevice(devicePath, d.config.GetDriverShareType())
	if shareType.IsBlockProtocol() {
		if devicePath == "" {
			return nil, status.Error(codes.Internal, "failed to resolve block device for expansion")
		}

		currentFSBytes := int64(0)
		if !rawBlock && volumePath != "" {
			if stats, statsErr := util.GetFilesystemStats(volumePath); statsErr != nil {
				klog.Warningf("Could not read current filesystem size for %s: %v", volumePath, statsErr)
			} else {
				currentFSBytes = stats.TotalBytes
			}
		}

		beforeBytes, beforeErr := util.GetDeviceSize(devicePath)
		if beforeErr != nil {
			klog.Warningf("Could not read device size before rescan for %s: %v", devicePath, beforeErr)
		} else {
			klog.Infof("Device %s size before rescan: %d bytes", devicePath, beforeBytes)
		}

		switch shareType {
		case ShareTypeISCSI:
			portal, iqn, infoErr := util.GetISCSIInfoFromDevice(devicePath)
			if infoErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to identify iSCSI session for %s: %v", devicePath, infoErr)
			}
			if rescanErr := util.ISCSIRescanSession(portal, iqn); rescanErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to rescan iSCSI device %s: %v", devicePath, rescanErr)
			}
		case ShareTypeNVMeoF:
			if rescanErr := util.NVMeRescan(devicePath); rescanErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to rescan NVMe-oF device %s: %v", devicePath, rescanErr)
			}
		}

		afterBytes, afterErr := util.GetDeviceSize(devicePath)
		if afterErr != nil {
			klog.Warningf("Could not read device size after rescan for %s: %v", devicePath, afterErr)
		} else {
			klog.Infof("Device %s size after rescan: %d bytes", devicePath, afterBytes)
			if beforeErr == nil && afterBytes <= beforeBytes {
				if rawBlock && capacityBytes > beforeBytes {
					klog.Warningf("Raw block device %s did not grow after rescan (before=%d, after=%d, requested=%d)", devicePath, beforeBytes, afterBytes, capacityBytes)
				} else if !rawBlock && capacityBytes > currentFSBytes {
					klog.Warningf("Device %s did not grow after rescan (before=%d, after=%d, requested=%d, filesystem=%d); attempting filesystem resize", devicePath, beforeBytes, afterBytes, capacityBytes, currentFSBytes)
				}
			}
		}

		// Node expansion for raw block volumes only needs the transport rescan.
		if rawBlock {
			klog.Infof("Raw block volume %s rescanned; skipping filesystem resize", volumeID)
			return &csi.NodeExpandVolumeResponse{CapacityBytes: capacityBytes}, nil
		}

		if volumePath != "" {
			if resizeErr := util.ResizeFilesystem(volumePath); resizeErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", resizeErr)
			}
		}
	}

	klog.Infof("Volume %s expanded successfully", volumeID)
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: capacityBytes,
	}, nil
}

func nodeVolumeLockKey(volumeID string) string {
	return "node:" + volumeID
}

func resolveNodeExpansionDevice(req *csi.NodeExpandVolumeRequest) (devicePath string, rawBlock bool, err error) {
	rawBlock = req.GetVolumeCapability() != nil && req.GetVolumeCapability().GetBlock() != nil

	paths := []string{req.GetStagingTargetPath(), req.GetVolumePath()}
	for _, path := range paths {
		if path == "" {
			continue
		}

		info, err := os.Lstat(path)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			resolvedPath, evalErr := filepath.EvalSymlinks(path)
			if evalErr != nil {
				return "", rawBlock, evalErr
			}
			if strings.HasPrefix(resolvedPath, "/dev/") {
				return resolvedPath, true, nil
			}
		}

		mountedDevice, mountErr := util.GetDeviceFromMountPoint(path)
		if mountErr == nil && strings.HasPrefix(mountedDevice, "/dev/") {
			return mountedDevice, rawBlock, nil
		}
	}

	return "", rawBlock, nil
}

func blockTransportForDevice(devicePath string, fallback ShareType) ShareType {
	if util.IsLikelyNVMeDevice(devicePath) {
		return ShareTypeNVMeoF
	}
	if util.IsLikelyISCSIDevice(devicePath) {
		return ShareTypeISCSI
	}
	if fallback.IsBlockProtocol() {
		return fallback
	}
	return ShareTypeNFS
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

	mounted, err := util.IsMounted(stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		klog.Infof("iSCSI volume already mounted at %s", stagingPath)
		return nil
	}

	sessions, listErr := util.ListISCSISessions()
	if listErr != nil {
		klog.Warningf("Failed to list iSCSI sessions before staging %s: %v", iqn, listErr)
	}

	if volCap != nil && volCap.GetBlock() != nil {
		if devicePath, ok := stagedBlockDevicePath(stagingPath); ok {
			_, stagedIQN, infoErr := getISCSIInfoFromDeviceWithSessions(devicePath, sessions)
			if infoErr == nil && stagedIQN == iqn {
				klog.Infof("iSCSI block volume already staged at %s", stagingPath)
				return nil
			}
		}
	}

	// Pre-emptive cleanup: disconnect any existing session for this target
	// This prevents duplicate sessions when a volume moves between nodes
	// or when a previous unstage failed to clean up properly.
	existingIQN, err := util.FindISCSISessionByIQNFromSessions(iqn, sessions)
	if err == nil && existingIQN != "" {
		if liveDevicePath, live := stagedDevicePath(stagingPath); live {
			klog.Infof("Skipping pre-emptive iSCSI disconnect for %s: staged device %s is still live", iqn, liveDevicePath)
		} else {
			klog.Infof("Found existing iSCSI session for %s, disconnecting before reconnect", iqn)
			if disconnectErr := util.ISCSIDisconnect(portal, existingIQN); disconnectErr != nil {
				klog.Warningf("Failed to disconnect existing session %s: %v (proceeding anyway)", existingIQN, disconnectErr)
			}
			cleanupDelay := time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond
			if pollErr := waitForSessionCleanup(ctx, cleanupDelay, func() (bool, error) {
				sessions, listErr = util.ListISCSISessions()
				if listErr != nil {
					return true, listErr
				}
				_, findErr := util.FindISCSISessionByIQNFromSessions(iqn, sessions)
				return findErr == nil, nil
			}); pollErr != nil {
				klog.V(4).Infof("iSCSI session cleanup poll for %s ended with: %v", iqn, pollErr)
			}
		}
	}

	// Connect to iSCSI target with configurable timeout
	connectOpts := &util.ISCSIConnectOptions{
		DeviceTimeout:       time.Duration(d.config.ISCSI.DeviceWaitTimeout) * time.Second,
		SessionCleanupDelay: time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond,
	}
	devicePath, err := iscsiConnectWithSessions(ctx, portal, iqn, lun, connectOpts, sessions)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to connect iSCSI: %v", err)
	}

	// Check if block mode
	if volCap != nil && volCap.GetBlock() != nil {
		// For block mode, create a symlink to the device
		// Use atomic rename to avoid race conditions when recreating symlinks
		if err := createSymlinkAtomic(devicePath, stagingPath); err != nil {
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

	mounted, err := util.IsMounted(stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		klog.Infof("NVMe-oF volume already mounted at %s", stagingPath)
		return nil
	}

	subsystems, listErr := util.ListNVMeSubsystems(ctx)
	if listErr != nil {
		klog.Warningf("Failed to list NVMe subsystems before staging %s: %v", nqn, listErr)
	}

	if volCap != nil && volCap.GetBlock() != nil {
		if devicePath, ok := stagedBlockDevicePath(stagingPath); ok {
			stagedNQN, infoErr := getNVMeInfoFromDevice(devicePath)
			_, findErr := util.FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems)
			if infoErr == nil && findErr == nil && stagedNQN == nqn {
				klog.Infof("NVMe-oF block volume already staged at %s", stagingPath)
				return nil
			}
		}
	}

	// Pre-emptive cleanup: disconnect any existing session for this NQN
	// This prevents duplicate sessions when a volume moves between nodes
	// or when a previous unstage failed to clean up properly.
	existingNQN, err := util.FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems)
	if err == nil && existingNQN != "" {
		if liveDevicePath, live := stagedDevicePath(stagingPath); live {
			klog.Infof("Skipping pre-emptive NVMe-oF disconnect for %s: staged device %s is still live", nqn, liveDevicePath)
		} else {
			klog.Infof("Found existing NVMe-oF session for %s, disconnecting before reconnect", nqn)
			if disconnectErr := util.NVMeoFDisconnect(existingNQN); disconnectErr != nil {
				klog.Warningf("Failed to disconnect existing session %s: %v (proceeding anyway)", existingNQN, disconnectErr)
			}
			cleanupDelay := time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond
			if pollErr := waitForSessionCleanup(ctx, cleanupDelay, func() (bool, error) {
				subsystems, listErr = util.ListNVMeSubsystems(ctx)
				if listErr != nil {
					return true, listErr
				}
				_, findErr := util.FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems)
				return findErr == nil, nil
			}); pollErr != nil {
				klog.V(4).Infof("NVMe-oF session cleanup poll for %s ended with: %v", nqn, pollErr)
			}
		}
	}

	// Connect to NVMe-oF subsystem with configurable timeout
	transportURI := fmt.Sprintf("%s://%s:%s", transport, address, port)
	connectOpts := &util.NVMeoFConnectOptions{
		DeviceTimeout: time.Duration(d.config.NVMeoF.DeviceWaitTimeout) * time.Second,
	}
	devicePath, err := nvmeConnectWithSubsystems(nqn, transportURI, connectOpts, subsystems)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to connect NVMe-oF: %v", err)
	}

	// Check if block mode
	if volCap != nil && volCap.GetBlock() != nil {
		// For block mode, create a symlink to the device
		// Use atomic rename to avoid race conditions when recreating symlinks
		if err := createSymlinkAtomic(devicePath, stagingPath); err != nil {
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

func stagedBlockDevicePath(stagingPath string) (string, bool) {
	info, err := os.Lstat(stagingPath)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return "", false
	}

	devicePath, err := filepath.EvalSymlinks(stagingPath)
	if err != nil || !strings.HasPrefix(devicePath, "/dev/") {
		return "", false
	}
	if _, err := os.Stat(devicePath); err != nil {
		return "", false
	}

	return devicePath, true
}

func stagedDevicePath(stagingPath string) (string, bool) {
	if devicePath, ok := stagedBlockDevicePath(stagingPath); ok {
		return devicePath, true
	}

	devicePath, err := util.GetDeviceFromMountPoint(stagingPath)
	if err != nil || !strings.HasPrefix(devicePath, "/dev/") {
		return "", false
	}
	if _, err := os.Stat(devicePath); err != nil {
		return "", false
	}
	return devicePath, true
}

func waitForSessionCleanup(ctx context.Context, timeout time.Duration, sessionExists func() (bool, error)) error {
	exists, lastErr := sessionExists()
	if !exists && lastErr == nil {
		return nil
	}
	if timeout <= 0 {
		return lastErr
	}

	deadline := time.Now().Add(timeout)

	for {
		wait := 100 * time.Millisecond
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("session still present after %v", timeout)
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
			exists, lastErr = sessionExists()
			if !exists && lastErr == nil {
				return nil
			}
		}
	}
}

// createSymlinkAtomic creates a symlink atomically using rename to avoid race conditions.
// If the symlink already exists and points to the correct target, it's left as-is.
// If it points to a different target, it's atomically replaced.
func createSymlinkAtomic(target, linkPath string) error {
	// First try to create the symlink directly
	if err := os.Symlink(target, linkPath); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	// Symlink exists - check if it already points to the correct target
	existingTarget, err := os.Readlink(linkPath)
	if err != nil {
		return fmt.Errorf("failed to read existing symlink: %w", err)
	}
	if existingTarget == target {
		// Already correct, nothing to do
		return nil
	}

	// Need to replace the symlink - use atomic rename to avoid race conditions
	klog.Warningf("Existing symlink %s points to %s, expected %s - recreating atomically", linkPath, existingTarget, target)

	// Create a temporary symlink with unique name to avoid races from concurrent calls
	tempLink := fmt.Sprintf("%s.tmp.%d", linkPath, time.Now().UnixNano())

	if err := os.Symlink(target, tempLink); err != nil {
		return fmt.Errorf("failed to create temporary symlink: %w", err)
	}

	// Atomic rename to replace the old symlink
	if err := os.Rename(tempLink, linkPath); err != nil {
		_ = os.Remove(tempLink) // Clean up temp link on failure
		return fmt.Errorf("failed to atomically replace symlink: %w", err)
	}

	return nil
}
