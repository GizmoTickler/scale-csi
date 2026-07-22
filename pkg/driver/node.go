package driver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

var (
	nodeStatsSysfsRoot         = "/sys"
	nodeGetNVMeInfo            = util.GetNVMeInfoFromDevice
	nodeNVMeDisconnect         = util.NVMeoFDisconnect
	nodeGetISCSIInfo           = util.GetISCSIInfoFromDevice
	nodeISCSIDisconnect        = util.ISCSIDisconnect
	nodeGetDeviceSize          = util.GetDeviceSize
	nodeResizeFilesystem       = util.ResizeFilesystem
	nodeFormatAndMount         = util.FormatAndMount
	nodeIsMounted              = util.IsMounted
	nodeGetMountInfo           = util.GetMountInfo
	nodeListMountInfo          = util.ListMountInfo
	nodeCheckISCSIMultipath    = util.CheckISCSIDeviceMultipathOwnership
	nodeISCSIRescan            = util.ISCSIRescanSession
	nodeNVMeRescan             = util.NVMeRescan
	nodeDeviceSizePollTimeout  = 5 * time.Second
	nodeDeviceSizePollInterval = 200 * time.Millisecond
	nodeStatsStat              = func(path string) (uint32, uint64, error) {
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			return 0, 0, err
		}
		return uint32(stat.Mode), uint64(stat.Rdev), nil //nolint:unconvert // Stat_t field widths differ per platform (darwin: Mode uint16, Rdev int32)
	}
)

type nodeAccessType string

const (
	nodeAccessMount nodeAccessType = "mount"
	nodeAccessBlock nodeAccessType = "block"
)

type nodeCapabilitySignature struct {
	AccessType nodeAccessType
	AccessMode csi.VolumeCapability_AccessMode_Mode
	FSType     string
	MountFlags string
}

type nodeMountRecord struct {
	VolumeID       string
	TargetPath     string
	ExpectedSource string
	LiveSource     string
	Capability     nodeCapabilitySignature
	Readonly       bool
}

func nodeCapabilityForRequest(capability *csi.VolumeCapability) (nodeCapabilitySignature, error) {
	if capability == nil {
		return nodeCapabilitySignature{}, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	var accessType nodeAccessType
	switch {
	case capability.GetBlock() != nil:
		accessType = nodeAccessBlock
	case capability.GetMount() != nil:
		accessType = nodeAccessMount
	default:
		// Preserve compatibility with older COs and existing tests that omitted
		// the mount oneof while providing an access mode; the historical driver
		// treated such capabilities as filesystem mounts.
		accessType = nodeAccessMount
	}
	flags := append([]string(nil), volumeMountFlags(capability)...)
	sort.Strings(flags)
	accessMode := csi.VolumeCapability_AccessMode_UNKNOWN
	if capability.GetAccessMode() != nil {
		accessMode = capability.GetAccessMode().GetMode()
	}
	return nodeCapabilitySignature{
		AccessType: accessType,
		AccessMode: accessMode,
		FSType:     strings.ToLower(capability.GetMount().GetFsType()),
		MountFlags: strings.Join(flags, ","),
	}, nil
}

func normalizeMountSource(source string) string {
	source = strings.TrimSpace(source)
	if bracket := strings.IndexByte(source, '['); bracket > 0 && strings.HasSuffix(source, "]") {
		source = source[:bracket]
	}
	if strings.HasPrefix(source, "/") {
		source = filepath.Clean(source)
	}
	return source
}

func mountSourcesEqual(left, right string) bool {
	return normalizeMountSource(left) == normalizeMountSource(right)
}

func accessTypeAtPath(path string) (nodeAccessType, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Mode().IsRegular() {
		return nodeAccessBlock, nil
	}
	if info.IsDir() {
		return nodeAccessMount, nil
	}
	return "", fmt.Errorf("path %s has unsupported type %s", path, info.Mode())
}

func (d *Driver) stageRecord(target string) (nodeMountRecord, bool) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	record, ok := d.stagedTargets[target]
	return record, ok
}

func (d *Driver) storeStageRecord(record nodeMountRecord) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	if d.stagedTargets == nil {
		d.stagedTargets = make(map[string]nodeMountRecord)
	}
	d.stagedTargets[record.TargetPath] = record
}

func (d *Driver) deleteStageRecord(target string) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	delete(d.stagedTargets, target)
}

func (d *Driver) publicationRecord(target string) (nodeMountRecord, bool) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	record, ok := d.publishedTargets[target]
	return record, ok
}

func (d *Driver) publicationRecords() []nodeMountRecord {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	records := make([]nodeMountRecord, 0, len(d.publishedTargets))
	for target := range d.publishedTargets {
		records = append(records, d.publishedTargets[target])
	}
	return records
}

func (d *Driver) storePublicationRecord(record nodeMountRecord) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	if d.publishedTargets == nil {
		d.publishedTargets = make(map[string]nodeMountRecord)
	}
	d.publishedTargets[record.TargetPath] = record
}

func (d *Driver) deletePublicationRecord(target string) {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	delete(d.publishedTargets, target)
}

func stageSourceIdentity(shareType ShareType, volumeContext map[string]string) (string, error) {
	switch shareType {
	case ShareTypeNFS:
		server, share := volumeContext["server"], volumeContext["share"]
		if server == "" || share == "" {
			return "", status.Error(codes.InvalidArgument, "NFS server and share are required in volume context")
		}
		return normalizeMountSource(fmt.Sprintf("%s:%s", server, share)), nil
	case ShareTypeISCSI:
		if volumeContext["iqn"] == "" {
			return "", status.Error(codes.InvalidArgument, "iSCSI IQN is required in volume context")
		}
		return "iscsi:" + volumeContext["iqn"], nil
	case ShareTypeNVMeoF:
		if volumeContext["nqn"] == "" {
			return "", status.Error(codes.InvalidArgument, "NVMe-oF NQN is required in volume context")
		}
		return "nvmeof:" + volumeContext["nqn"], nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "unsupported attach driver: %s", shareType)
	}
}

func verifyStageDeviceSource(devicePath string, shareType ShareType, volumeContext map[string]string) error {
	switch shareType {
	case ShareTypeNFS:
		return status.Error(codes.AlreadyExists, "NFS staging target cannot contain a raw block device")
	case ShareTypeISCSI:
		_, actualIQN, err := nodeGetISCSIInfo(devicePath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to identify staged iSCSI device %s: %v", devicePath, err)
		}
		if actualIQN != volumeContext["iqn"] {
			return status.Errorf(codes.AlreadyExists, "staging target is backed by iSCSI target %s, requested %s", actualIQN, volumeContext["iqn"])
		}
	case ShareTypeNVMeoF:
		actualNQN, err := nodeGetNVMeInfo(devicePath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to identify staged NVMe-oF device %s: %v", devicePath, err)
		}
		if actualNQN != volumeContext["nqn"] {
			return status.Errorf(codes.AlreadyExists, "staging target is backed by NVMe-oF subsystem %s, requested %s", actualNQN, volumeContext["nqn"])
		}
	}
	return nil
}

func (d *Driver) handleExistingStage(req *csi.NodeStageVolumeRequest, shareType ShareType, capability nodeCapabilitySignature, expectedSource string) (bool, error) {
	stagingPath := req.GetStagingTargetPath()
	mounted, err := nodeIsMounted(stagingPath)
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	info, statErr := os.Lstat(stagingPath)
	symlink := statErr == nil && info.Mode()&os.ModeSymlink != 0
	if !mounted && !symlink {
		d.deleteStageRecord(stagingPath)
		return false, nil
	}
	actualAccess := nodeAccessMount
	if symlink {
		actualAccess = nodeAccessBlock
	}
	if actualAccess != capability.AccessType {
		return true, status.Errorf(codes.AlreadyExists, "staging target %s already contains access type %s, requested %s", stagingPath, actualAccess, capability.AccessType)
	}

	liveSource := ""
	if symlink {
		devicePath, resolveErr := filepath.EvalSymlinks(stagingPath)
		if resolveErr != nil {
			return true, status.Errorf(codes.Internal, "failed to resolve staged block device: %v", resolveErr)
		}
		if sourceErr := verifyStageDeviceSource(devicePath, shareType, req.GetVolumeContext()); sourceErr != nil {
			return true, sourceErr
		}
		liveSource = normalizeMountSource(devicePath)
	} else {
		mountInfo, infoErr := nodeGetMountInfo(stagingPath)
		if infoErr != nil {
			return true, status.Errorf(codes.Internal, "failed to inspect existing staging mount: %v", infoErr)
		}
		liveSource = normalizeMountSource(mountInfo.Source)
		switch shareType {
		case ShareTypeNFS:
			if !mountSourcesEqual(liveSource, expectedSource) {
				return true, status.Errorf(codes.AlreadyExists, "staging target %s is backed by %s, requested %s", stagingPath, liveSource, expectedSource)
			}
			if mountInfo.FSType != "nfs" && mountInfo.FSType != "nfs4" {
				return true, status.Errorf(codes.AlreadyExists, "staging target %s has filesystem %s, requested NFS", stagingPath, mountInfo.FSType)
			}
		default:
			if sourceErr := verifyStageDeviceSource(liveSource, shareType, req.GetVolumeContext()); sourceErr != nil {
				return true, sourceErr
			}
			if capability.FSType != "" && !strings.EqualFold(mountInfo.FSType, capability.FSType) {
				return true, status.Errorf(codes.AlreadyExists, "staging target %s has filesystem %s, requested %s", stagingPath, mountInfo.FSType, capability.FSType)
			}
		}
	}

	if record, ok := d.stageRecord(stagingPath); ok {
		if record.VolumeID != req.GetVolumeId() || record.ExpectedSource != expectedSource || record.Capability != capability || !mountSourcesEqual(record.LiveSource, liveSource) {
			return true, status.Errorf(codes.AlreadyExists, "staging target %s is already occupied by an incompatible staged volume", stagingPath)
		}
	}
	d.storeStageRecord(nodeMountRecord{
		VolumeID:       req.GetVolumeId(),
		TargetPath:     stagingPath,
		ExpectedSource: expectedSource,
		LiveSource:     liveSource,
		Capability:     capability,
	})
	return true, nil
}

func (d *Driver) rememberStage(req *csi.NodeStageVolumeRequest, shareType ShareType, capability nodeCapabilitySignature, expectedSource string) {
	liveSource := expectedSource
	if capability.AccessType == nodeAccessBlock {
		if devicePath, ok := stagedBlockDevicePath(req.GetStagingTargetPath()); ok {
			liveSource = normalizeMountSource(devicePath)
		}
	} else if mountInfo, err := nodeGetMountInfo(req.GetStagingTargetPath()); err == nil {
		liveSource = normalizeMountSource(mountInfo.Source)
	}
	d.storeStageRecord(nodeMountRecord{
		VolumeID:       req.GetVolumeId(),
		TargetPath:     req.GetStagingTargetPath(),
		ExpectedSource: expectedSource,
		LiveSource:     liveSource,
		Capability:     capability,
	})
}

func (d *Driver) expectedPublicationSource(req *csi.NodePublishVolumeRequest, capability nodeCapabilitySignature) (string, error) {
	if capability.AccessType == nodeAccessBlock {
		if req.GetStagingTargetPath() == "" {
			return "", status.Error(codes.FailedPrecondition, "staging path is required for block volumes")
		}
		devicePath, err := filepath.EvalSymlinks(req.GetStagingTargetPath())
		if err != nil {
			return "", status.Errorf(codes.FailedPrecondition, "failed to resolve staged block device: %v", err)
		}
		if !strings.HasPrefix(devicePath, "/dev/") {
			return "", status.Errorf(codes.FailedPrecondition, "staging path did not resolve to a block device: %s", devicePath)
		}
		return normalizeMountSource(devicePath), nil
	}
	if req.GetStagingTargetPath() != "" {
		mountInfo, err := nodeGetMountInfo(req.GetStagingTargetPath())
		if err != nil {
			return "", status.Errorf(codes.FailedPrecondition, "staging target %s is not a readable mount: %v", req.GetStagingTargetPath(), err)
		}
		return normalizeMountSource(mountInfo.Source), nil
	}
	volumeContext := req.GetVolumeContext()
	if d.nodeAttachDriver(volumeContext) != ShareTypeNFS {
		return "", status.Error(codes.FailedPrecondition, "staging path required for block volumes")
	}
	server, share := volumeContext["server"], volumeContext["share"]
	if server == "" || share == "" {
		return "", status.Error(codes.InvalidArgument, "NFS server and share are required in volume context")
	}
	return normalizeMountSource(fmt.Sprintf("%s:%s", server, share)), nil
}

func (d *Driver) validateExistingPublication(req *csi.NodePublishVolumeRequest, capability nodeCapabilitySignature, expectedSource string) error {
	actualAccess, err := accessTypeAtPath(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to inspect existing target path: %v", err)
	}
	if actualAccess != capability.AccessType {
		return status.Errorf(codes.AlreadyExists, "target path %s already contains access type %s, requested %s", req.GetTargetPath(), actualAccess, capability.AccessType)
	}
	mountInfo, err := nodeGetMountInfo(req.GetTargetPath())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to inspect existing publication mount: %v", err)
	}
	actualSource := normalizeMountSource(mountInfo.Source)
	if mountInfo.ReadOnly != req.GetReadonly() {
		return status.Errorf(codes.AlreadyExists, "target path %s readonly state is %t, requested %t", req.GetTargetPath(), mountInfo.ReadOnly, req.GetReadonly())
	}
	if record, ok := d.publicationRecord(req.GetTargetPath()); ok {
		if record.VolumeID != req.GetVolumeId() || record.Capability != capability || record.Readonly != req.GetReadonly() || !mountSourcesEqual(record.ExpectedSource, expectedSource) || !mountSourcesEqual(record.LiveSource, actualSource) {
			return status.Errorf(codes.AlreadyExists, "target path %s already contains an incompatible publication", req.GetTargetPath())
		}
	} else if !mountSourcesEqual(actualSource, expectedSource) {
		return status.Errorf(codes.AlreadyExists, "target path %s is backed by %s, requested %s", req.GetTargetPath(), actualSource, expectedSource)
	}
	d.storePublicationRecord(nodeMountRecord{
		VolumeID:       req.GetVolumeId(),
		TargetPath:     req.GetTargetPath(),
		ExpectedSource: expectedSource,
		LiveSource:     actualSource,
		Capability:     capability,
		Readonly:       req.GetReadonly(),
	})
	return nil
}

func allowsMultiplePublicationTargets(mode csi.VolumeCapability_AccessMode_Mode) bool {
	switch mode {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return true
	default:
		return false
	}
}

func likelyCSIPublicationTarget(target string) bool {
	normalized := filepath.ToSlash(filepath.Clean(target))
	return strings.Contains(normalized, "/pods/") || strings.Contains(normalized, "/volumeDevices/publish/")
}

func (d *Driver) isKnownStageTarget(target string) bool {
	d.nodeMountStateMu.Lock()
	defer d.nodeMountStateMu.Unlock()
	_, ok := d.stagedTargets[target]
	return ok
}

func (d *Driver) ensurePublicationTargetAllowed(req *csi.NodePublishVolumeRequest, capability nodeCapabilitySignature, expectedSource string) error {
	records := d.publicationRecords()
	for i := range records {
		record := &records[i]
		if record.VolumeID != req.GetVolumeId() || record.TargetPath == req.GetTargetPath() {
			continue
		}
		mounted, err := nodeIsMounted(record.TargetPath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to verify existing publication at %s: %v", record.TargetPath, err)
		}
		if !mounted {
			d.deletePublicationRecord(record.TargetPath)
			continue
		}
		if record.Capability != capability || record.Readonly != req.GetReadonly() || !mountSourcesEqual(record.ExpectedSource, expectedSource) || !allowsMultiplePublicationTargets(capability.AccessMode) {
			if capability.AccessMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				klog.Infof("NodePublishVolume: single-writer migration for volume %s is blocked by the still-mounted target %s; kubelet may still be tearing down the prior pod", req.GetVolumeId(), record.TargetPath)
			}
			return status.Errorf(codes.FailedPrecondition, "volume %s is already published at different target path %s", req.GetVolumeId(), record.TargetPath)
		}
	}
	if allowsMultiplePublicationTargets(capability.AccessMode) {
		return nil
	}

	mounts, err := nodeListMountInfo()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to rebuild publication state from mount table: %v", err)
	}
	for _, mount := range mounts {
		if mount.Target == req.GetTargetPath() || mount.Target == req.GetStagingTargetPath() || d.isKnownStageTarget(mount.Target) || !likelyCSIPublicationTarget(mount.Target) {
			continue
		}
		if mountSourcesEqual(mount.Source, expectedSource) {
			if capability.AccessMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
				klog.Infof("NodePublishVolume: single-writer migration for volume %s is blocked by the still-mounted target %s; kubelet may still be tearing down the prior pod", req.GetVolumeId(), mount.Target)
			}
			return status.Errorf(codes.FailedPrecondition, "volume %s is already published at different target path %s", req.GetVolumeId(), mount.Target)
		}
	}
	return nil
}

func (d *Driver) rememberPublication(req *csi.NodePublishVolumeRequest, capability nodeCapabilitySignature, expectedSource string) {
	liveSource := expectedSource
	if mountInfo, err := nodeGetMountInfo(req.GetTargetPath()); err == nil {
		liveSource = normalizeMountSource(mountInfo.Source)
	}
	d.storePublicationRecord(nodeMountRecord{
		VolumeID:       req.GetVolumeId(),
		TargetPath:     req.GetTargetPath(),
		ExpectedSource: expectedSource,
		LiveSource:     liveSource,
		Capability:     capability,
		Readonly:       req.GetReadonly(),
	})
}

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
		NodeId: d.encodedNodeID,
	}
	if d.config.Node.MaxVolumesPerNode > 0 {
		resp.MaxVolumesPerNode = d.config.Node.MaxVolumesPerNode
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
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
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

	// Get attach driver from volume context and normalize.
	attachDriver := d.nodeAttachDriver(volumeContext)
	capability, err := nodeCapabilityForRequest(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	expectedSource, err := stageSourceIdentity(attachDriver, volumeContext)
	if err != nil {
		return nil, err
	}
	if handled, existingErr := d.handleExistingStage(req, attachDriver, capability, expectedSource); handled {
		if existingErr != nil {
			return nil, existingErr
		}
		klog.Infof("Volume %s is already staged compatibly at %s", volumeID, stagingPath)
		return &csi.NodeStageVolumeResponse{}, nil
	} else if existingErr != nil {
		return nil, existingErr
	}

	// Filesystem volumes mount on the staging path, while raw-block volumes
	// create a symlink at that exact path. Creating the leaf for block mode
	// makes the later symlink deterministically fail with EEXIST.
	directoryToCreate := stagingPath
	if req.GetVolumeCapability().GetBlock() != nil {
		directoryToCreate = filepath.Dir(stagingPath)
	}
	if err := os.MkdirAll(directoryToCreate, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging directory: %v", err)
	}
	eventObject := nodeVolumeEventRef(volumeContext, volumeID, d.nodeID)

	switch attachDriver {
	case ShareTypeNFS:
		if err := d.stageNFSVolume(ctx, volumeContext, stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
			return nil, err
		}
	case ShareTypeISCSI:
		if err := d.stageISCSIVolume(ctx, volumeContext, stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
			return nil, err
		}
	case ShareTypeNVMeoF:
		if err := d.stageNVMeoFVolume(ctx, volumeContext, stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
			return nil, err
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported attach driver: %s (supported: %v)", attachDriver, ValidShareTypeStrings())
	}

	// Re-read the live target after the transport helper succeeds. Besides
	// recording the kernel-resolved backing device, this closes the window in
	// which an external mount could appear after the initial idempotency check;
	// the protocol helpers' legacy mounted fast paths cannot bypass compatibility
	// validation.
	if handled, existingErr := d.handleExistingStage(req, attachDriver, capability, expectedSource); handled {
		if existingErr != nil {
			return nil, existingErr
		}
		klog.Infof("Volume %s staged successfully at %s", volumeID, stagingPath)
		return &csi.NodeStageVolumeResponse{}, nil
	} else if existingErr != nil {
		return nil, existingErr
	}

	// Some unit-test transports do not populate a real mount table. Preserve a
	// best-effort record for those and for unusual mount helpers whose successful
	// result is not immediately visible to findmnt.
	d.rememberStage(req, attachDriver, capability, expectedSource)
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

	// A raw-block staging symlink contains a literal /dev name that can become
	// stale after reboot. Never derive the session to disconnect from it; use
	// the volume's expected target name instead.
	if isSymlink {
		if strings.Contains(devicePath, "nvme") {
			d.cleanupOrphanedSessionByVolumeID(ctx, volumeID, ShareTypeNVMeoF)
		} else {
			d.cleanupOrphanedSessionByVolumeID(ctx, volumeID, ShareTypeISCSI)
		}
		d.deleteStageRecord(stagingPath)
		klog.Infof("Volume %s unstaged successfully", volumeID)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Filesystem-mode device paths were read from the live mount before it was
	// unmounted, so they remain safe for direct session cleanup.
	primaryDisconnectSucceeded := false
	if devicePath != "" {
		if strings.Contains(devicePath, "nvme") {
			// NVMe-oF cleanup
			nqn, nvmeErr := nodeGetNVMeInfo(devicePath)
			if nvmeErr == nil {
				if discErr := nodeNVMeDisconnect(nqn); discErr != nil {
					klog.Warningf("Failed to disconnect NVMe-oF session %s: %v", nqn, discErr)
				} else {
					klog.Infof("Disconnected NVMe-oF session %s", nqn)
					primaryDisconnectSucceeded = true
				}
			} else {
				klog.V(4).Infof("Could not get NVMe info from device %s: %v", devicePath, nvmeErr)
			}
		} else {
			// Try iSCSI cleanup
			portal, iqn, iscsiErr := nodeGetISCSIInfo(devicePath)
			if iscsiErr == nil {
				if discErr := nodeISCSIDisconnect(portal, iqn); discErr != nil {
					klog.Warningf("Failed to disconnect iSCSI session %s: %v", iqn, discErr)
				} else {
					klog.Infof("Disconnected iSCSI session %s", iqn)
					primaryDisconnectSucceeded = true
				}
			} else {
				klog.V(4).Infof("Could not get iSCSI info from device %s: %v", devicePath, iscsiErr)
			}
		}
	}

	// The scan is only needed when no device was available or the primary
	// device-based disconnect could not be completed successfully. A known device
	// identifies its transport; without one, probe both exact session lookups.
	if devicePath == "" {
		d.cleanupOrphanedSessionByVolumeID(ctx, volumeID, ShareTypeISCSI)
		d.cleanupOrphanedSessionByVolumeID(ctx, volumeID, ShareTypeNVMeoF)
	} else if !primaryDisconnectSucceeded {
		attachDriver := ShareTypeISCSI
		if strings.Contains(devicePath, "nvme") {
			attachDriver = ShareTypeNVMeoF
		}
		d.cleanupOrphanedSessionByVolumeID(ctx, volumeID, attachDriver)
	}

	d.deleteStageRecord(stagingPath)
	klog.Infof("Volume %s unstaged successfully", volumeID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// cleanupOrphanedSessionByVolumeID attempts to clean up iSCSI/NVMe-oF sessions
// when the device path is unavailable (e.g., after node restart or force unmount).
// This prevents session leaks that accumulate over time.
func (d *Driver) cleanupOrphanedSessionByVolumeID(ctx context.Context, volumeID string, attachDriver ShareType) {
	_ = ctx

	switch attachDriver {
	case ShareTypeISCSI:
		portal := d.config.ISCSI.TargetPortal
		targetName := d.iscsiShareName(volumeID)
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

	case ShareTypeNVMeoF:
		nqnName := d.config.NVMeoF.NamePrefix + protocolShareName(volumeID) + d.config.NVMeoF.NameSuffix
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
}

func (d *Driver) nodeAttachDriver(volumeContext map[string]string) ShareType {
	if volumeContext != nil && volumeContext["node_attach_driver"] != "" {
		return ParseShareType(volumeContext["node_attach_driver"])
	}
	return d.config.GetDriverShareType()
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
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	capability, err := nodeCapabilityForRequest(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	eventObject := nodeVolumeEventRef(req.GetVolumeContext(), volumeID, d.nodeID)

	klog.Infof("NodePublishVolume: volumeID=%s, targetPath=%s, stagingPath=%s", volumeID, targetPath, stagingPath)

	// Lock on volume ID
	lockKey := nodeVolumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)
	targetLockKey := nodeTargetLockKey(targetPath)
	if !d.acquireOperationLock(targetLockKey) {
		return nil, status.Error(codes.Aborted, "target path operation already in progress")
	}
	defer d.releaseOperationLock(targetLockKey)

	expectedSource, err := d.expectedPublicationSource(req, capability)
	if err != nil {
		return nil, err
	}

	// Ensure target directory exists
	if mkdirErr := os.MkdirAll(filepath.Dir(targetPath), 0o750); mkdirErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to create target directory: %v", mkdirErr)
	}

	// Check if already mounted
	mounted, err := nodeIsMounted(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		if err := d.validateExistingPublication(req, capability, expectedSource); err != nil {
			return nil, err
		}
		klog.Infof("Volume %s already mounted compatibly at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	if err := d.ensurePublicationTargetAllowed(req, capability, expectedSource); err != nil {
		return nil, err
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
			shareType := d.nodeAttachDriver(req.GetVolumeContext())
			if ownershipErr := d.validateRawBlockDeviceOwnership(volumeID, devicePath, shareType); ownershipErr != nil {
				return nil, ownershipErr
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
				operationErr := status.Errorf(codes.Internal, "failed to bind mount block device: %v", err)
				d.recordWarningEvent(eventObject, EventReasonMountFailed, operationErr.Error())
				return nil, operationErr
			}
			d.rememberPublication(req, capability, expectedSource)
			klog.Infof("Block volume %s published successfully at %s", volumeID, targetPath)
			return &csi.NodePublishVolumeResponse{}, nil
		}

		if err := os.MkdirAll(targetPath, 0o750); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create target path: %v", err)
		}
		if err := util.BindMount(stagingPath, targetPath, mountOptions); err != nil {
			operationErr := status.Errorf(codes.Internal, "failed to bind mount: %v", err)
			d.recordWarningEvent(eventObject, EventReasonMountFailed, operationErr.Error())
			return nil, operationErr
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
				operationErr := status.Errorf(codes.Internal, "failed to mount NFS: %v", err)
				d.recordWarningEvent(eventObject, EventReasonNFSMountFailed, operationErr.Error())
				return nil, operationErr
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "staging path required for block volumes")
		}
	}

	d.rememberPublication(req, capability, expectedSource)
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
	targetLockKey := nodeTargetLockKey(targetPath)
	if !d.acquireOperationLock(targetLockKey) {
		return nil, status.Error(codes.Aborted, "target path operation already in progress")
	}
	defer d.releaseOperationLock(targetLockKey)

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
	d.deletePublicationRecord(targetPath)

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
	if sectors > math.MaxInt64/512 {
		return 0, fmt.Errorf("block device size from %s exceeds int64 bytes", sizePath)
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
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "volume path is required")
	}
	if _, err := os.Stat(volumePath); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "volume path %s does not exist", volumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to inspect volume path %s: %v", volumePath, err)
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

		if rawBlock {
			if ownershipErr := d.validateRawBlockDeviceOwnership(volumeID, devicePath, shareType); ownershipErr != nil {
				return nil, ownershipErr
			}
		}

		beforeBytes, beforeErr := nodeGetDeviceSize(devicePath)
		if beforeErr != nil {
			klog.Warningf("Could not read device size before rescan for %s: %v", devicePath, beforeErr)
		} else {
			klog.Infof("Device %s size before rescan: %d bytes", devicePath, beforeBytes)
		}

		switch shareType {
		case ShareTypeISCSI:
			portal, iqn, infoErr := nodeGetISCSIInfo(devicePath)
			if infoErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to identify iSCSI session for %s: %v", devicePath, infoErr)
			}
			if rescanErr := nodeISCSIRescan(portal, iqn); rescanErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to rescan iSCSI device %s: %v", devicePath, rescanErr)
			}
		case ShareTypeNVMeoF:
			if rescanErr := nodeNVMeRescan(devicePath); rescanErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to rescan NVMe-oF device %s: %v", devicePath, rescanErr)
			}
		}

		afterBytes, afterErr := waitForDeviceSize(ctx, devicePath, beforeBytes, capacityBytes)
		if afterErr != nil {
			return nil, status.Errorf(codes.Internal, "device size did not settle after rescan for %s: %v", devicePath, afterErr)
		}
		klog.Infof("Device %s size after rescan: %d bytes", devicePath, afterBytes)

		// Node expansion for raw block volumes only needs the transport rescan.
		if rawBlock {
			if capacityBytes > 0 && afterBytes < capacityBytes {
				return nil, status.Errorf(codes.Internal,
					"raw block device %s capacity is %d bytes after rescan, below requested %d bytes",
					devicePath, afterBytes, capacityBytes)
			}
			klog.Infof("Raw block volume %s rescanned; skipping filesystem resize", volumeID)
			return &csi.NodeExpandVolumeResponse{CapacityBytes: afterBytes}, nil
		}

		if volumePath != "" {
			if resizeErr := nodeResizeFilesystem(volumePath); resizeErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", resizeErr)
			}
		}
		if capacityBytes > 0 && afterBytes < capacityBytes {
			return nil, status.Errorf(codes.Internal,
				"block device %s capacity is %d bytes after resize, below requested %d bytes",
				devicePath, afterBytes, capacityBytes)
		}
		capacityBytes = afterBytes
	}

	klog.Infof("Volume %s expanded successfully", volumeID)
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: capacityBytes,
	}, nil
}

func (d *Driver) validateRawBlockDeviceOwnership(volumeID, devicePath string, shareType ShareType) error {
	switch shareType {
	case ShareTypeISCSI:
		_, iqn, err := nodeGetISCSIInfo(devicePath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to identify iSCSI session for raw block device %s: %v", devicePath, err)
		}
		expected := d.iscsiShareName(volumeID)
		if !sessionTargetMatchesExpected(iqn, expected) {
			return status.Errorf(codes.FailedPrecondition,
				"raw block staging device %s belongs to iSCSI target %s, expected volume target %s",
				devicePath, iqn, expected)
		}
	case ShareTypeNVMeoF:
		nqn, err := nodeGetNVMeInfo(devicePath)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to identify NVMe-oF session for raw block device %s: %v", devicePath, err)
		}
		expected := d.config.NVMeoF.NamePrefix + protocolShareName(volumeID) + d.config.NVMeoF.NameSuffix
		if !sessionTargetMatchesExpected(nqn, expected) {
			return status.Errorf(codes.FailedPrecondition,
				"raw block staging device %s belongs to NVMe-oF subsystem %s, expected volume subsystem %s",
				devicePath, nqn, expected)
		}
	}
	return nil
}

func sessionTargetMatchesExpected(actual, expected string) bool {
	return actual == expected || strings.HasSuffix(actual, ":"+expected)
}

func waitForDeviceSize(ctx context.Context, devicePath string, beforeBytes, capacityBytes int64) (int64, error) {
	deadline := time.Now().Add(nodeDeviceSizePollTimeout)
	var lastSize int64
	var lastErr error

	for {
		lastSize, lastErr = nodeGetDeviceSize(devicePath)
		if lastErr == nil {
			settled := capacityBytes > 0 && lastSize >= capacityBytes
			if capacityBytes <= 0 {
				settled = beforeBytes <= 0 || lastSize > beforeBytes
			}
			if settled {
				return lastSize, nil
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr != nil {
				return 0, lastErr
			}
			return lastSize, fmt.Errorf("capacity remained at %d bytes (before=%d, requested=%d) for %v",
				lastSize, beforeBytes, capacityBytes, nodeDeviceSizePollTimeout)
		}

		wait := nodeDeviceSizePollInterval
		if remaining < wait {
			wait = remaining
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
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

func nodeVolumeLockKey(volumeID string) string {
	return "node:" + volumeID
}

func nodeTargetLockKey(targetPath string) string {
	return "node-target:" + filepath.Clean(targetPath)
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
func (d *Driver) stageNFSVolume(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
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
	if err := util.MountNFS(source, stagingPath, volumeMountFlags(volCap)); err != nil {
		RecordNodeConnect("nfs", "error")
		operationErr := status.Errorf(codes.Internal, "failed to mount NFS: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSMountFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("nfs", "success")

	return nil
}

// stageISCSIVolume connects and mounts an iSCSI volume to the staging path.
func (d *Driver) stageISCSIVolume(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
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
		RecordNodeConnect("iscsi", "error")
		operationErr := status.Errorf(codes.Internal, "failed to connect iSCSI: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSILoginFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("iscsi", "success")
	if err := nodeCheckISCSIMultipath(devicePath); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
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
	mountFlags := volumeMountFlagsForFS(volCap, fsType)

	if err := nodeFormatAndMount(devicePath, stagingPath, fsType, mountFlags); err != nil {
		operationErr := status.Errorf(codes.Internal, "failed to format and mount: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonMountFailed, operationErr.Error())
		return operationErr
	}

	return nil
}

// stageNVMeoFVolume connects and mounts an NVMe-oF volume to the staging path.
func (d *Driver) stageNVMeoFVolume(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
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
		RecordNodeConnect("nvmeof", "error")
		operationErr := status.Errorf(codes.Internal, "failed to connect NVMe-oF: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMeConnectFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("nvmeof", "success")

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
	mountFlags := volumeMountFlagsForFS(volCap, fsType)

	if err := nodeFormatAndMount(devicePath, stagingPath, fsType, mountFlags); err != nil {
		operationErr := status.Errorf(codes.Internal, "failed to format and mount: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonMountFailed, operationErr.Error())
		return operationErr
	}

	return nil
}

func volumeMountFlags(volCap *csi.VolumeCapability) []string {
	if volCap == nil || volCap.GetMount() == nil {
		return nil
	}
	flags := volCap.GetMount().GetMountFlags()
	result := make([]string, 0, len(flags))
	seen := make(map[string]struct{}, len(flags))
	for _, flag := range flags {
		flag = strings.TrimSpace(flag)
		if flag == "" {
			continue
		}
		if _, duplicate := seen[flag]; duplicate {
			continue
		}
		seen[flag] = struct{}{}
		result = append(result, flag)
	}
	return result
}

func volumeMountFlagsForFS(volCap *csi.VolumeCapability, fsType string) []string {
	flags := volumeMountFlags(volCap)
	if !strings.EqualFold(fsType, "xfs") {
		return flags
	}
	for _, flag := range flags {
		if strings.EqualFold(flag, "nouuid") {
			return flags
		}
	}
	return append(flags, "nouuid")
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
// If the path is already the correct symlink, it's left as-is. Other symlinks,
// empty directories, and regular files are replaced without recursively deleting data.
func createSymlinkAtomic(target, linkPath string) error {
	// First try to create the symlink directly
	if err := os.Symlink(target, linkPath); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	existingInfo, err := os.Lstat(linkPath)
	if err != nil {
		return fmt.Errorf("failed to inspect existing symlink path: %w", err)
	}

	if existingInfo.Mode()&os.ModeSymlink != 0 {
		// Symlink exists - check if it already points to the correct target.
		existingTarget, readErr := os.Readlink(linkPath)
		if readErr != nil {
			return fmt.Errorf("failed to read existing symlink: %w", readErr)
		}
		if existingTarget == target {
			// Already correct, nothing to do.
			return nil
		}

		klog.Warningf("Existing symlink %s points to %s, expected %s - recreating atomically", linkPath, existingTarget, target)
	} else {
		switch {
		case existingInfo.IsDir():
			// Kubelet pre-creates the raw-block staging path as an empty
			// directory. Remove only that leaf; never recursively delete it.
			if removeErr := os.Remove(linkPath); removeErr != nil {
				return fmt.Errorf("failed to remove existing directory %s before creating symlink (directory must be empty): %w", linkPath, removeErr)
			}
			klog.Warningf("Removed existing empty directory %s before creating device symlink", linkPath)
		case existingInfo.Mode().IsRegular():
			if removeErr := os.Remove(linkPath); removeErr != nil {
				return fmt.Errorf("failed to remove existing regular file %s before creating symlink: %w", linkPath, removeErr)
			}
			klog.Warningf("Removed existing regular file %s before creating device symlink", linkPath)
		default:
			return fmt.Errorf("cannot replace existing path %s with symlink: unsupported file type %s", linkPath, existingInfo.Mode())
		}
	}

	// Create a temporary symlink with unique name to avoid races from concurrent calls.
	// Rename atomically replaces a stale symlink, or installs the link after an
	// empty directory or regular file was safely removed above.
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
