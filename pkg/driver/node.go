package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
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
	nodeStatsSysfsRoot          = "/sys"
	nodeGetNVMeInfo             = util.GetNVMeInfoFromDevice
	nodeNVMeDisconnect          = util.NVMeoFDisconnectWithContext
	nodeListNVMeSubsystems      = util.ListNVMeSubsystems
	nodeSetNVMeIOPolicy         = util.SetNVMeSubsystemIOPolicy
	nodeNVMeSubsystemSysfsRoot  = "/sys/class/nvme-subsystem"
	nodeGetISCSIInfo            = util.GetISCSIInfoFromDevice
	nodeISCSIDisconnect         = util.ISCSIDisconnect
	nodeGetSCSIWWID             = util.GetSCSIWWID
	nodeFindISCSIMultipath      = util.FindISCSIMultipathDevice
	nodeCheckISCSIMultipathHost = util.CheckISCSIMultipathPrerequisites
	nodeGetDeviceSize           = util.GetDeviceSize
	nodeResizeFilesystem        = util.ResizeFilesystemWithContext
	nodeFormatAndMount          = util.FormatAndMountWithContext
	nodeIsMounted               = util.IsMounted
	nodeGetMountInfo            = util.GetMountInfo
	nodeListMountInfo           = util.ListMountInfo
	nodeMountNFS                = util.MountNFSWithContext
	nodeUnmount                 = util.UnmountWithContext
	nodeCheckISCSIMultipath     = util.CheckISCSIDeviceMultipathOwnership
	nodeISCSIRescan             = util.ISCSIRescanSessionWithContext
	nodeNVMeRescan              = util.NVMeRescanWithContext
	nodeDeviceSizePollTimeout   = 5 * time.Second
	nodeDeviceSizePollInterval  = 200 * time.Millisecond
	nodeStatsStat               = func(path string) (uint32, uint64, error) {
		var stat unix.Stat_t
		if err := unix.Stat(path, &stat); err != nil {
			return 0, 0, err
		}
		return uint32(stat.Mode), uint64(stat.Rdev), nil //nolint:unconvert // Stat_t field widths differ per platform (darwin: Mode uint16, Rdev int32)
	}
)

// Secondary paths are availability improvements, not a reason to hold a CSI
// NodeStage RPC for N times the full device and nvme-cli timeouts. After the
// already-live device wait and mandatory first missing-path attempt, every
// remaining top-up shares this total budget.
const nvmeSecondaryPathConvergeBudget = 5 * time.Second

// dm-multipath creates one map after at least two SCSI paths become visible.
// Secondary iSCSI logins share this budget so an unavailable storage VLAN does
// not multiply the full configured device timeout.
const iscsiSecondaryPathConvergeBudget = 5 * time.Second

const invalidNVMeMultipathAddressMetricLabel = "invalid-publish-context"
const invalidISCSIMultipathPortalMetricLabel = "invalid-publish-context"
const invalidNFSTrunkingAddressMetricLabel = "invalid-publish-context"

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
		return normalizeMountSource(nfsSource(server, share)), nil
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
			// A DANGLING staging symlink — the backing device vanished (node reboot
			// with a persisted staging dir, a dropped iSCSI/NVMe-oF session) — makes
			// EvalSymlinks fail with a not-exist-class error. Returning Internal here
			// wedges the volume forever: every kubelet retry fails identically and the
			// stage never reaches the reconnect path built to repair exactly this.
			// Drop the stale stage record and report "not staged" so the normal stage
			// path disconnects the stale session, reconnects, and atomically replaces
			// the link. A resolvable device that fails for any other reason stays
			// fail-closed with Internal.
			if errors.Is(resolveErr, os.ErrNotExist) {
				// The live device is gone so its identity cannot be re-derived; guard
				// the self-heal with the persisted stage record. If a record exists and
				// belongs to a DIFFERENT volume/source/capability (a malformed request
				// or kubelet path confusion — and distinct volume IDs take distinct node
				// locks, so nothing else serializes this collision), fail closed with
				// AlreadyExists and leave volume A's record and link intact rather than
				// erasing them. Only an absent or compatible record is cleared to let the
				// requesting volume reconnect. LiveSource is intentionally not compared:
				// the device vanished, so the record's last-known source cannot match.
				if record, ok := d.stageRecord(stagingPath); ok &&
					(record.VolumeID != req.GetVolumeId() || record.ExpectedSource != expectedSource || record.Capability != capability) {
					return true, status.Errorf(codes.AlreadyExists, "staging target %s is already occupied by an incompatible staged volume", stagingPath)
				}
				d.deleteStageRecord(stagingPath)
				return false, nil
			}
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
	return normalizeMountSource(nfsSource(server, share)), nil
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
	stageContext := volumeContext
	switch attachDriver {
	case ShareTypeNFS:
		stageContext = publishHintStageContext(volumeContext, req.GetPublishContext(), "addresses")
	case ShareTypeISCSI:
		stageContext = publishHintStageContext(volumeContext, req.GetPublishContext(), "portals")
	case ShareTypeNVMeoF:
		stageContext = publishHintStageContext(volumeContext, req.GetPublishContext(), "addresses")
	}
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
		eventObject := nodeVolumeEventRef(volumeContext, volumeID, d.nodeID)
		switch attachDriver {
		case ShareTypeNFS:
			d.convergeExistingNFSTrunks(ctx, stageContext, stagingPath, req.GetVolumeCapability(), eventObject)
		case ShareTypeISCSI:
			d.convergeExistingISCSIPaths(ctx, stageContext, req.GetSecrets(), eventObject)
		case ShareTypeNVMeoF:
			d.convergeExistingNVMeoFPaths(ctx, stageContext, eventObject)
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
		if err := d.stageNFSVolume(ctx, stageContext, stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
			return nil, err
		}
	case ShareTypeISCSI:
		if err := d.stageISCSIVolume(ctx, stageContext, req.GetSecrets(), stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
			return nil, err
		}
	case ShareTypeNVMeoF:
		if err := d.stageNVMeoFVolume(ctx, stageContext, stagingPath, req.GetVolumeCapability(), eventObject); err != nil {
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

// publishHintStageContext overlays one attach-scoped path hint onto the
// immutable PV context. Presence in PublishContext wins even for malformed or
// empty values so the protocol parser can make its observable safe fallback.
// Returning the original map when absent preserves older-controller behavior.
func publishHintStageContext(volumeContext, publishContext map[string]string, key string) map[string]string {
	rawHint, present := publishContext[key]
	if !present {
		return volumeContext
	}
	merged := make(map[string]string, len(volumeContext)+1)
	for key, value := range volumeContext {
		merged[key] = value
	}
	merged[key] = rawHint
	return merged
}

func nvmeoFStageContext(volumeContext, publishContext map[string]string) map[string]string {
	return publishHintStageContext(volumeContext, publishContext, "addresses")
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

	klog.V(2).Infof("NodeUnstageVolume: volumeID=%s, stagingPath=%s", volumeID, stagingPath)

	// Lock on volume ID
	lockKey := nodeVolumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return nil, status.Error(codes.Aborted, "operation already in progress")
	}
	defer d.releaseOperationLock(lockKey)

	cleanupNFSTrunkProbeMounts(ctx, stagingPath)

	// Get device path before unmounting (for session cleanup)
	// For block mode volumes, stagingPath is a symlink to the device, not a mount point
	devicePath, err := util.GetDeviceFromMountPointWithContext(ctx, stagingPath)
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
		if err := util.UnmountWithContext(ctx, stagingPath); err != nil {
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
		klog.V(2).Infof("Volume %s unstaged successfully", volumeID)
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
				if discErr := nodeNVMeDisconnect(ctx, nqn); discErr != nil {
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
			_, iqn, iscsiErr := nodeGetISCSIInfo(devicePath)
			if iscsiErr == nil {
				// A dm-multipath device can have several sessions for one IQN.
				// Logout every session, but never flush the dm map here: another
				// staged consumer may still hold it and multipathd owns map lifetime.
				primaryDisconnectSucceeded = disconnectAllISCSISessionsForIQN(iqn)
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
	klog.V(2).Infof("Volume %s unstaged successfully", volumeID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// cleanupOrphanedSessionByVolumeID attempts to clean up iSCSI/NVMe-oF sessions
// when the device path is unavailable (e.g., after node restart or force unmount).
// This prevents session leaks that accumulate over time.
func (d *Driver) cleanupOrphanedSessionByVolumeID(ctx context.Context, volumeID string, attachDriver ShareType) {
	switch attachDriver {
	case ShareTypeISCSI:
		targetName := d.iscsiShareName(volumeID)
		sessions, listErr := listISCSISessions()
		if listErr != nil {
			klog.V(4).Infof("Cannot list active iSCSI sessions for volume %s: %v", volumeID, listErr)
			return
		}
		expectedSuffix := ":" + targetName
		foundIQNs := make(map[string]struct{})
		for _, session := range sessions {
			if strings.HasSuffix(session.IQN, expectedSuffix) {
				foundIQNs[session.IQN] = struct{}{}
			}
		}
		if len(foundIQNs) == 0 {
			klog.V(4).Infof("No active iSCSI session found for volume %s (target: %s)", volumeID, targetName)
		}
		for iqn := range foundIQNs {
			disconnectAllISCSISessionsForIQNWithSnapshot(iqn, sessions)
		}

	case ShareTypeNVMeoF:
		nqnName := d.config.NVMeoF.NamePrefix + protocolShareName(volumeID) + d.config.NVMeoF.NameSuffix
		nqn, err := util.FindNVMeoFSessionBySubsysName(nqnName)
		if err != nil {
			klog.V(4).Infof("No active NVMe-oF session found for volume %s (nqn: %s): %v", volumeID, nqnName, err)
		} else if nqn != "" {
			klog.Infof("Found orphaned NVMe-oF session for volume %s: %s", volumeID, nqn)
			if err := nodeNVMeDisconnect(ctx, nqn); err != nil {
				klog.Warningf("Failed to disconnect orphaned NVMe-oF session %s: %v", nqn, err)
			} else {
				klog.Infof("Successfully cleaned up orphaned NVMe-oF session %s", nqn)
			}
		}
	}
}

func disconnectAllISCSISessionsForIQN(iqn string) bool {
	sessions, listErr := listISCSISessions()
	if listErr != nil {
		klog.Warningf("Failed to list iSCSI sessions before logout of %s: %v", iqn, listErr)
		return false
	}
	return disconnectAllISCSISessionsForIQNWithSnapshot(iqn, sessions)
}

func disconnectAllISCSISessionsForIQNWithSnapshot(iqn string, sessions []util.ISCSISessionInfo) bool {
	found := false
	allSucceeded := true
	seenPortals := make(map[string]struct{})
	for _, session := range sessions {
		if session.IQN != iqn {
			continue
		}
		if _, duplicate := seenPortals[session.Portal]; duplicate {
			continue
		}
		seenPortals[session.Portal] = struct{}{}
		found = true
		if disconnectErr := nodeISCSIDisconnect(session.Portal, iqn); disconnectErr != nil {
			allSucceeded = false
			klog.Warningf("Failed to disconnect iSCSI session %s through %s: %v", iqn, session.Portal, disconnectErr)
		} else {
			klog.Infof("Disconnected iSCSI session %s through %s", iqn, session.Portal)
		}
	}
	return found && allSucceeded
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
			if err := util.BindMountWithContext(ctx, devicePath, targetPath, blockMountOptions); err != nil {
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
		if err := util.BindMountWithContext(ctx, stagingPath, targetPath, mountOptions); err != nil {
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
			nfsContext := publishHintStageContext(volumeContext, req.GetPublishContext(), "addresses")
			directCapability := req.GetVolumeCapability()
			if directCapability != nil && directCapability.GetMount() != nil {
				mount := directCapability.GetMount()
				directCapability = &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{
						FsType:     mount.GetFsType(),
						MountFlags: mountOptions,
					}},
					AccessMode: directCapability.GetAccessMode(),
				}
			}
			if mountErr := d.stageNFSVolume(ctx, nfsContext, targetPath, directCapability, eventObject); mountErr != nil {
				return nil, mountErr
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "staging path required for block volumes")
		}
	}

	d.rememberPublication(req, capability, expectedSource)
	klog.Infof("Volume %s published successfully at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func cleanupNFSTrunkProbeMounts(ctx context.Context, stagingPath string) {
	probePaths, globErr := filepath.Glob(stagingPath + ".scale-csi-nfs-trunk-*")
	if globErr != nil {
		return
	}
	for _, probePath := range probePaths {
		mounted, mountErr := nodeIsMounted(probePath)
		if mountErr == nil && mounted {
			if unmountErr := nodeUnmount(ctx, probePath); unmountErr != nil {
				klog.Warningf("Failed to clean NFS trunk probe mount %s: %v", probePath, unmountErr)
				continue
			}
		}
		if removeErr := os.Remove(probePath); removeErr != nil && !os.IsNotExist(removeErr) {
			klog.V(4).Infof("Failed to remove NFS trunk probe path %s: %v", probePath, removeErr)
		}
	}
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

	klog.V(2).Infof("NodeUnpublishVolume: volumeID=%s, targetPath=%s", volumeID, targetPath)

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
	if err := util.UnmountWithContext(ctx, targetPath); err != nil {
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

	klog.V(2).Infof("Volume %s unpublished successfully", volumeID)
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

	// Bounded liveness pre-gate, run BEFORE any unix.Stat on volumePath. For a
	// filesystem (NFS/remote) volume volumePath is the mountpoint, and unix.Stat
	// on the root of a dead hard mount issues a GETATTR that blocks in
	// uninterruptible D-state indefinitely — the exact hazard this gate closes.
	// The findmnt-backed check reads the kernel mount table (never the filesystem
	// itself) and is bounded by both the configured mount timeout and the kubelet's
	// inbound RPC deadline, so it cannot itself hang; if it cannot confirm the path
	// promptly we report an abnormal condition instead of touching it. Ordering it
	// ahead of device resolution means a dead mount can hang NEITHER stat (in the
	// resolver) NOR the statfs below. Block-device paths are not mountpoints, so
	// the check returns not-mounted with no error and falls through to the fast
	// local device stat. No new TrueNAS API calls are involved.
	if _, mountErr := nodeStatsMountCheck(ctx, volumePath); mountErr != nil {
		return abnormalVolumeStatsResponse(fmt.Sprintf("mount unresponsive for %s: %v", volumePath, mountErr)), nil
	}

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

	// Get filesystem stats (mount liveness was already gated above, before stat).
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
			if rescanErr := nodeISCSIRescan(ctx, portal, iqn); rescanErr != nil {
				return nil, status.Errorf(codes.Internal, "failed to rescan iSCSI device %s: %v", devicePath, rescanErr)
			}
		case ShareTypeNVMeoF:
			if rescanErr := nodeNVMeRescan(ctx, devicePath); rescanErr != nil {
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
			if resizeErr := nodeResizeFilesystem(ctx, volumePath); resizeErr != nil {
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

// volumeLockKey is the controller-plane per-volume operation-lock key. It is
// deliberately in a different keyspace from nodeVolumeLockKey ("node:") so a
// node-plane stage/unstage and a controller-plane create/delete/expand for the
// same volume ID never contend on the same lock.
func volumeLockKey(volumeID string) string {
	return "volume:" + volumeID
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

	addresses, hintPresent, addressErr := parseNFSTrunkingAddresses(volumeContext)
	if addressErr != nil {
		message := fmt.Sprintf("NFS trunking address list for %s was discarded; using the primary server only: %v", share, addressErr)
		klog.Warning(message)
		RecordNFSTrunkConnect(invalidNFSTrunkingAddressMetricLabel, "error")
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSTrunkingDegraded, message)
		addresses = nil
	}
	trunkingActive := hintPresent && len(addresses) > 1
	source := fmt.Sprintf("%s:%s", server, share)
	if trunkingActive {
		source = nfsSource(addresses[0], share)
	}

	// Check if already mounted
	mounted, err := util.IsMounted(stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		if trunkingActive {
			d.convergeNFSTrunks(ctx, addresses, share, stagingPath, configuredNFSMountFlags(d.config.NFS.NConnect, len(addresses), volCap), eventObjects...)
		}
		klog.Infof("NFS already mounted at %s", stagingPath)
		return nil
	}

	// Mount NFS
	mountFlags := configuredNFSMountFlags(d.config.NFS.NConnect, len(addresses), volCap)
	mountErr := nodeMountNFS(ctx, source, stagingPath, mountFlags)
	if mountErr != nil && trunkingActive {
		// Linux before max_connect support rejects the option before any NFS
		// version is negotiated. Retry once without the trunking-only option so
		// an optional availability feature cannot brick the primary mount.
		fallbackFlags := configuredNFSMountFlags(d.config.NFS.NConnect, 0, volCap)
		fallbackErr := nodeMountNFS(ctx, nfsSource(server, share), stagingPath, fallbackFlags)
		if fallbackErr == nil {
			message := fmt.Sprintf("NFS trunking options are unavailable for %s; the primary mount succeeded without max_connect: %v", share, mountErr)
			klog.Warning(message)
			d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSTrunkingUnavailable, message)
			RecordNodeConnect("nfs", "success")
			return nil
		}
		mountErr = fmt.Errorf("trunking mount failed: %w; untrunked primary fallback failed: %w", mountErr, fallbackErr)
	}
	if mountErr != nil {
		RecordNodeConnect("nfs", "error")
		operationErr := status.Errorf(codes.Internal, "failed to mount NFS: %v", mountErr)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSMountFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("nfs", "success")
	if trunkingActive {
		d.convergeNFSTrunks(ctx, addresses, share, stagingPath, mountFlags, eventObjects...)
	}

	return nil
}

func configuredNFSMountFlags(nconnect *int, trunkAddressCount int, volCap *csi.VolumeCapability) []string {
	flags := nfsMountFlags(volCap)
	if nconnect == nil && trunkAddressCount < 2 {
		return flags
	}
	configured := make([]string, 0, len(flags)+2)
	for _, flag := range flags {
		key := strings.ToLower(strings.TrimSpace(strings.SplitN(flag, "=", 2)[0]))
		if nconnect != nil && key == "nconnect" {
			continue
		}
		if trunkAddressCount > 1 && key == "max_connect" {
			continue
		}
		configured = append(configured, flag)
	}
	if nconnect != nil {
		configured = append(configured, fmt.Sprintf("nconnect=%d", *nconnect))
	}
	if trunkAddressCount > 1 {
		configured = append(configured, fmt.Sprintf("max_connect=%d", trunkAddressCount))
	}
	return configured
}

func parseNFSTrunkingAddresses(volumeContext map[string]string) (normalized []string, present bool, err error) {
	raw, present := volumeContext["addresses"]
	if !present {
		return nil, false, nil
	}
	var decoded []string
	if decodeErr := json.Unmarshal([]byte(raw), &decoded); decodeErr != nil {
		return nil, true, fmt.Errorf("decode addresses: %w", decodeErr)
	}
	if len(decoded) == 0 {
		return nil, true, errors.New("addresses is empty")
	}
	normalized = make([]string, 0, len(decoded))
	seen := make(map[string]struct{}, len(decoded))
	for _, rawAddress := range decoded {
		address, normalizeErr := normalizeNVMeoFAddress(rawAddress)
		if normalizeErr != nil {
			return nil, true, fmt.Errorf("addresses contains invalid server address %q: %w", rawAddress, normalizeErr)
		}
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		normalized = append(normalized, address)
	}
	return normalized, true, nil
}

func nfsSource(address, share string) string {
	if strings.Contains(address, ":") {
		return fmt.Sprintf("[%s]:%s", address, share)
	}
	return fmt.Sprintf("%s:%s", address, share)
}

func (d *Driver) convergeExistingNFSTrunks(ctx context.Context, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) {
	addresses, _, addressErr := parseNFSTrunkingAddresses(volumeContext)
	if addressErr != nil || len(addresses) < 2 {
		return
	}
	share := volumeContext["share"]
	if share == "" {
		return
	}
	d.convergeNFSTrunks(ctx, addresses, share, stagingPath, configuredNFSMountFlags(d.config.NFS.NConnect, len(addresses), volCap), eventObjects...)
}

func (d *Driver) convergeNFSTrunks(ctx context.Context, addresses []string, share, stagingPath string, mountFlags []string, eventObjects ...runtime.Object) {
	mountInfo, infoErr := nodeGetMountInfo(stagingPath)
	if infoErr != nil {
		message := fmt.Sprintf("cannot verify negotiated NFS version for trunking at %s: %v", stagingPath, infoErr)
		klog.Warning(message)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSTrunkingUnavailable, message)
		return
	}
	version, versionKnown := effectiveNFSVersion(mountInfo.Options)
	if !versionKnown || version < 4.1 {
		message := fmt.Sprintf("NFS trunking requires a negotiated NFS version of at least 4.1; %s is mounted with %s and remains available through its primary server", stagingPath, printableNFSVersion(version, versionKnown))
		klog.Warning(message)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSTrunkingUnavailable, message)
		return
	}

	failures := make([]error, 0)
	for index, address := range addresses[1:] {
		probePath := fmt.Sprintf("%s.scale-csi-nfs-trunk-%d", stagingPath, index+1)
		if mkdirErr := os.MkdirAll(probePath, 0o750); mkdirErr != nil {
			RecordNFSTrunkConnect(address, "error")
			failures = append(failures, fmt.Errorf("%s: create probe mountpoint: %w", address, mkdirErr))
			continue
		}
		mountErr := nodeMountNFS(ctx, nfsSource(address, share), probePath, mountFlags)
		if mountErr != nil {
			RecordNFSTrunkConnect(address, "error")
			failures = append(failures, fmt.Errorf("%s: %w", address, mountErr))
			_ = os.Remove(probePath)
			continue
		}
		RecordNFSTrunkConnect(address, "success")
		if unmountErr := nodeUnmount(ctx, probePath); unmountErr != nil {
			failures = append(failures, fmt.Errorf("%s: probe unmount: %w", address, unmountErr))
			continue
		}
		_ = os.Remove(probePath)
	}
	if len(failures) > 0 {
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNFSTrunkingDegraded,
			fmt.Sprintf("NFS trunk transport convergence for %s is degraded: %v", share, errors.Join(failures...)))
	}
}

func effectiveNFSVersion(options []string) (float64, bool) {
	for _, option := range options {
		parts := strings.SplitN(option, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		if key != "vers" && key != "nfsvers" {
			continue
		}
		version, parseErr := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if parseErr == nil {
			return version, true
		}
	}
	return 0, false
}

func printableNFSVersion(version float64, known bool) string {
	if !known {
		return "an unknown version"
	}
	return fmt.Sprintf("NFS %.1f", version)
}

// nodeStageTransport abstracts the protocol-specific session operations the
// shared pre-emptive-disconnect helper needs. S is the transport's session-list
// type (iSCSI []util.ISCSISessionInfo / NVMe-oF []util.NVMeSubsystem).
type nodeStageTransport[S any] struct {
	name        string                           // human label, "iSCSI" / "NVMe-oF"
	findSession func(sessions S) (string, error) // existing session id ("" if none)
	disconnect  func(existing string) error
	relist      func() (S, error)
}

// preemptiveSessionDisconnect disconnects any pre-existing session for a target
// before staging, preventing duplicate sessions when a volume moves between
// nodes or a previous unstage failed to clean up. It is skipped when the staged
// device is still live. After disconnecting it polls until the session clears,
// re-listing sessions each iteration; the refreshed list is returned so the
// caller feeds the post-cleanup state into connect (the original inline blocks
// re-assigned the captured `sessions`/`subsystems` variable — preserved here by
// returning it, including the assign-before-error-check ordering).
func preemptiveSessionDisconnect[S any](ctx context.Context, d *Driver, t nodeStageTransport[S], id, stagingPath string, sessions S) S {
	existing, err := t.findSession(sessions)
	if err != nil || existing == "" {
		return sessions
	}
	if liveDevicePath, live := stagedDevicePath(stagingPath); live {
		klog.Infof("Skipping pre-emptive %s disconnect for %s: staged device %s is still live", t.name, id, liveDevicePath)
		return sessions
	}
	klog.Infof("Found existing %s session for %s, disconnecting before reconnect", t.name, id)
	if disconnectErr := t.disconnect(existing); disconnectErr != nil {
		klog.Warningf("Failed to disconnect existing session %s: %v (proceeding anyway)", existing, disconnectErr)
	}
	cleanupDelay := time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond
	if pollErr := waitForSessionCleanup(ctx, cleanupDelay, func() (bool, error) {
		refreshed, listErr := t.relist()
		sessions = refreshed
		if listErr != nil {
			return true, listErr
		}
		_, findErr := t.findSession(sessions)
		return findErr == nil, nil
	}); pollErr != nil {
		klog.V(4).Infof("%s session cleanup poll for %s ended with: %v", t.name, id, pollErr)
	}
	return sessions
}

// finalizeStagedDevice completes a stage once the backing device is connected:
// for block volumes it atomically (re)creates the staging symlink; for
// filesystem volumes it formats and mounts. This tail is identical across the
// iSCSI and NVMe-oF stage paths.
func (d *Driver) finalizeStagedDevice(ctx context.Context, devicePath, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
	// For block mode, create a symlink to the device. Use atomic rename to avoid
	// race conditions when recreating symlinks.
	if volCap != nil && volCap.GetBlock() != nil {
		if err := createSymlinkAtomic(devicePath, stagingPath); err != nil {
			return status.Errorf(codes.Internal, "failed to create device symlink: %v", err)
		}
		return nil
	}

	// For filesystem mode, format and mount.
	fsType := "ext4"
	if volCap != nil && volCap.GetMount() != nil && volCap.GetMount().GetFsType() != "" {
		fsType = strings.ToLower(volCap.GetMount().GetFsType())
	}
	mountFlags := volumeMountFlagsForFS(volCap, fsType)

	if err := nodeFormatAndMount(ctx, devicePath, stagingPath, fsType, mountFlags); err != nil {
		operationErr := status.Errorf(codes.Internal, "failed to format and mount: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonMountFailed, operationErr.Error())
		return operationErr
	}

	return nil
}

// stageISCSIVolume connects and mounts an iSCSI volume to the staging path.
func (d *Driver) stageISCSIVolume(ctx context.Context, volumeContext, secrets map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
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

	sessions, listErr := listISCSISessions()
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

	portals, hintPresent, portalErr := parseISCSIMultipathPortals(volumeContext)
	multipathActive := false
	switch {
	case portalErr != nil:
		message := fmt.Sprintf("iSCSI multipath portal list for %s was discarded; using the primary portal only: %v", iqn, portalErr)
		klog.Warning(message)
		RecordISCSIPathConnect(invalidISCSIMultipathPortalMetricLabel, "error")
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSIPathDegraded, message)
	case hintPresent && len(portals) < 2:
		message := fmt.Sprintf("iSCSI multipath portal list for %s has fewer than two portals; using the primary portal only", iqn)
		klog.Warning(message)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSIMultipathUnavailable, message)
	case len(portals) > 1:
		prerequisiteErr := nodeCheckISCSIMultipathHost()
		if prerequisiteErr != nil {
			// Unlike NVMe-oF, iSCSI has no native kernel path aggregation. A node
			// without multipathd must remain usable, so do not create secondary
			// sessions that could later race a raw-device mount.
			message := fmt.Sprintf("iSCSI multipath is unavailable for %s; staging through the primary portal only: %v", iqn, prerequisiteErr)
			klog.Warning(message)
			d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSIMultipathUnavailable, message)
		} else {
			multipathActive = true
		}
	}

	if multipathActive {
		sessions = preemptiveISCSIMultipathDisconnect(ctx, d, iqn, stagingPath, sessions)
	} else {
		// Preserve the historical single-portal cleanup path exactly when no
		// usable attach hint exists.
		sessions = preemptiveSessionDisconnect(ctx, d, nodeStageTransport[[]util.ISCSISessionInfo]{
			name:        "iSCSI",
			findSession: func(s []util.ISCSISessionInfo) (string, error) { return util.FindISCSISessionByIQNFromSessions(iqn, s) },
			disconnect:  func(existing string) error { return nodeISCSIDisconnect(portal, existing) },
			relist:      listISCSISessions,
		}, iqn, stagingPath, sessions)
	}

	// Build node-side CHAP credentials from the volume context mode flag and the
	// node-stage secret. nil means CHAP is off for this volume and the connect
	// path applies no auth params (zero behavior change). A CHAP volume with a
	// missing/invalid secret fails fast here with InvalidArgument rather than
	// letting iscsiadm enter a login retry storm.
	chapCreds, err := nodeISCSIChAPCredentials(volumeContext, secrets)
	if err != nil {
		// Aid debugging without leaking: log the REDACTED secret key set (values
		// masked by redactCHAP) alongside the sanitized validation error.
		klog.V(4).Infof("iSCSI CHAP credential validation failed for %s (secret keys: %v): %v",
			iqn, redactCHAP(secrets), err)
		return err
	}

	// Connect to iSCSI target with configurable timeout
	connectOpts := &util.ISCSIConnectOptions{
		DeviceTimeout:       time.Duration(d.config.ISCSI.DeviceWaitTimeout) * time.Second,
		SessionCleanupDelay: time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond,
		CHAP:                chapCreds,
	}
	var devicePath string
	var pathFailures []error
	if multipathActive {
		devicePath, pathFailures, err = convergeISCSIMultipathPaths(ctx, portals, iqn, lun, connectOpts, sessions)
	} else {
		devicePath, err = iscsiConnectWithSessions(ctx, portal, iqn, lun, connectOpts, sessions)
	}
	if err != nil {
		RecordNodeConnect("iscsi", "error")
		if errors.Is(err, util.ErrISCSIAuthFailure) {
			// A wrong CHAP secret is terminal and must not retry. Return
			// Unauthenticated with a redacted message (no credential, no raw
			// iscsiadm output) so kubelet surfaces a clean failure.
			operationErr := status.Errorf(codes.Unauthenticated, "iSCSI CHAP authentication failed for %s", iqn)
			d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSILoginFailed, operationErr.Error())
			return operationErr
		}
		if errors.Is(err, util.ErrISCSICHAPConfig) {
			// CHAP credentials could not be applied to the node record before login.
			// The wrapped error is already redacted (parameter name + exit class only,
			// never the secret value), so it is safe to surface on the PVC/PV (E3/O15).
			operationErr := status.Errorf(codes.Internal, "failed to configure iSCSI CHAP for %s", iqn)
			d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSICHAPFailed, "CHAP configuration failed: "+err.Error())
			return operationErr
		}
		operationErr := status.Errorf(codes.Internal, "failed to connect iSCSI: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSILoginFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("iscsi", "success")
	if len(pathFailures) > 0 {
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSIPathDegraded,
			fmt.Sprintf("iSCSI path convergence for %s is degraded: %v", iqn, errors.Join(pathFailures...)))
	}
	if !multipathActive {
		// Legacy single-path safety check: if an operator independently enabled
		// multipathd, never bypass a dm map by mounting one component path.
		if ownershipErr := nodeCheckISCSIMultipath(devicePath); ownershipErr != nil {
			return status.Error(codes.FailedPrecondition, ownershipErr.Error())
		}
	}

	return d.finalizeStagedDevice(ctx, devicePath, stagingPath, volCap, eventObjects...)
}

func parseISCSIMultipathPortals(volumeContext map[string]string) (normalized []string, present bool, err error) {
	raw, present := volumeContext["portals"]
	if !present {
		return nil, false, nil
	}
	var decoded []string
	if decodeErr := json.Unmarshal([]byte(raw), &decoded); decodeErr != nil {
		return nil, true, fmt.Errorf("decode portals: %w", decodeErr)
	}
	if len(decoded) == 0 {
		return nil, true, errors.New("portals is empty")
	}
	normalized = make([]string, 0, len(decoded))
	seen := make(map[string]struct{}, len(decoded))
	for _, rawPortal := range decoded {
		portal, normalizeErr := normalizeISCSITargetPortal(rawPortal)
		if normalizeErr != nil {
			return nil, true, fmt.Errorf("portals contains invalid target portal %q: %w", rawPortal, normalizeErr)
		}
		if _, duplicate := seen[portal]; duplicate {
			continue
		}
		seen[portal] = struct{}{}
		normalized = append(normalized, portal)
	}
	return normalized, true, nil
}

func preemptiveISCSIMultipathDisconnect(ctx context.Context, d *Driver, iqn, stagingPath string, sessions []util.ISCSISessionInfo) []util.ISCSISessionInfo {
	if liveDevicePath, live := stagedDevicePath(stagingPath); live {
		klog.Infof("Skipping pre-emptive iSCSI multipath disconnect for %s: staged device %s is still live", iqn, liveDevicePath)
		return sessions
	}
	found := false
	for _, session := range sessions {
		if session.IQN != iqn {
			continue
		}
		found = true
		if disconnectErr := nodeISCSIDisconnect(session.Portal, iqn); disconnectErr != nil {
			klog.Warningf("Failed to disconnect existing iSCSI session %s through %s: %v (proceeding anyway)", iqn, session.Portal, disconnectErr)
		}
	}
	if !found {
		return sessions
	}
	cleanupDelay := time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond
	if pollErr := waitForSessionCleanup(ctx, cleanupDelay, func() (bool, error) {
		refreshed, refreshErr := listISCSISessions()
		sessions = refreshed
		if refreshErr != nil {
			return true, refreshErr
		}
		for _, session := range sessions {
			if session.IQN == iqn {
				return true, nil
			}
		}
		return false, nil
	}); pollErr != nil {
		klog.V(4).Infof("iSCSI multipath session cleanup poll for %s ended with: %v", iqn, pollErr)
	}
	return sessions
}

func convergeISCSIMultipathPaths(
	ctx context.Context,
	portals []string,
	iqn string,
	lun int,
	connectOpts *util.ISCSIConnectOptions,
	sessions []util.ISCSISessionInfo,
) (string, []error, error) {
	primaryPortal := portals[0]
	primaryDevice, primaryErr := iscsiConnectWithSessions(ctx, primaryPortal, iqn, lun, connectOpts, sessions)
	if primaryErr != nil {
		RecordISCSIPathConnect(primaryPortal, "error")
		return "", nil, primaryErr
	}
	RecordISCSIPathConnect(primaryPortal, "success")

	wwid, wwidErr := nodeGetSCSIWWID(primaryDevice)
	if wwidErr != nil {
		return "", nil, fmt.Errorf("resolve SCSI WWID for primary path %s: %w", primaryDevice, wwidErr)
	}

	pathFailures := make([]error, 0)
	connectedSecondaries := make([]string, 0, len(portals)-1)
	secondaryCtx, cancel := context.WithTimeout(ctx, iscsiSecondaryPathConvergeBudget)
	defer cancel()
	secondaryOpts := &util.ISCSIConnectOptions{
		DeviceTimeout:       iscsiSecondaryPathConvergeBudget,
		SessionCleanupDelay: connectOpts.SessionCleanupDelay,
		CHAP:                connectOpts.CHAP,
	}
	if connectOpts.DeviceTimeout > 0 && connectOpts.DeviceTimeout < secondaryOpts.DeviceTimeout {
		secondaryOpts.DeviceTimeout = connectOpts.DeviceTimeout
	}
	for _, secondaryPortal := range portals[1:] {
		secondaryDevice, connectErr := iscsiConnectWithSessions(secondaryCtx, secondaryPortal, iqn, lun, secondaryOpts, sessions)
		if connectErr != nil {
			RecordISCSIPathConnect(secondaryPortal, "error")
			pathFailures = append(pathFailures, fmt.Errorf("%s: %w", secondaryPortal, connectErr))
			continue
		}
		secondaryWWID, secondaryWWIDErr := nodeGetSCSIWWID(secondaryDevice)
		if secondaryWWIDErr != nil || secondaryWWID != wwid {
			RecordISCSIPathConnect(secondaryPortal, "error")
			// A session that resolves to a different LUN identity must never remain
			// beside the staged map. It cannot be a path for this volume, and keeping
			// it would make later map discovery and teardown ambiguous.
			if disconnectErr := nodeISCSIDisconnect(secondaryPortal, iqn); disconnectErr != nil {
				pathFailures = append(pathFailures, fmt.Errorf("%s mismatched-path logout: %w", secondaryPortal, disconnectErr))
			}
			if secondaryWWIDErr != nil {
				pathFailures = append(pathFailures, fmt.Errorf("%s: resolve SCSI WWID: %w", secondaryPortal, secondaryWWIDErr))
			} else {
				pathFailures = append(pathFailures, fmt.Errorf("%s: SCSI WWID %s differs from primary %s", secondaryPortal, secondaryWWID, wwid))
			}
			continue
		}
		RecordISCSIPathConnect(secondaryPortal, "success")
		connectedSecondaries = append(connectedSecondaries, secondaryPortal)
	}

	mapCtx, mapCancel := context.WithTimeout(ctx, iscsiSecondaryPathConvergeBudget)
	defer mapCancel()
	dmDevice, dmErr := waitForISCSIMultipathDevice(mapCtx, wwid)
	if dmErr == nil {
		return dmDevice, pathFailures, nil
	}

	// If no map materialized, remove the extra sessions before falling back to
	// the raw primary device. Leaving multiple live paths would let multipathd
	// claim that raw device after it had been mounted.
	for _, secondaryPortal := range connectedSecondaries {
		if disconnectErr := nodeISCSIDisconnect(secondaryPortal, iqn); disconnectErr != nil {
			pathFailures = append(pathFailures, fmt.Errorf("%s fallback logout: %w", secondaryPortal, disconnectErr))
		}
	}
	pathFailures = append(pathFailures, fmt.Errorf("dm map for WWID %s: %w", wwid, dmErr))
	if ownershipErr := nodeCheckISCSIMultipath(primaryDevice); ownershipErr != nil {
		return "", pathFailures, fmt.Errorf("dm-multipath claimed primary path but its map could not be resolved: %w", ownershipErr)
	}
	return primaryDevice, pathFailures, nil
}

func waitForISCSIMultipathDevice(ctx context.Context, wwid string) (string, error) {
	for {
		devicePath, findErr := nodeFindISCSIMultipath(wwid)
		if findErr == nil {
			return devicePath, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (d *Driver) convergeExistingISCSIPaths(ctx context.Context, volumeContext, secrets map[string]string, eventObjects ...runtime.Object) {
	portals, _, portalErr := parseISCSIMultipathPortals(volumeContext)
	if portalErr != nil || len(portals) < 2 {
		return
	}
	if prerequisiteErr := nodeCheckISCSIMultipathHost(); prerequisiteErr != nil {
		return
	}
	iqn := volumeContext["iqn"]
	lun, lunErr := strconv.Atoi(volumeContext["lun"])
	if volumeContext["lun"] == "" {
		lun = 0
		lunErr = nil
	}
	if iqn == "" || lunErr != nil {
		return
	}
	chapCreds, chapErr := nodeISCSIChAPCredentials(volumeContext, secrets)
	if chapErr != nil {
		return
	}
	sessions, listErr := listISCSISessions()
	if listErr != nil {
		klog.V(4).Infof("Cannot converge existing iSCSI paths for %s: %v", iqn, listErr)
	}
	connectOpts := &util.ISCSIConnectOptions{
		DeviceTimeout:       time.Duration(d.config.ISCSI.DeviceWaitTimeout) * time.Second,
		SessionCleanupDelay: time.Duration(d.config.Node.SessionCleanupDelay) * time.Millisecond,
		CHAP:                chapCreds,
	}
	_, failures, convergeErr := convergeISCSIMultipathPaths(ctx, portals, iqn, lun, connectOpts, sessions)
	if convergeErr != nil {
		failures = append(failures, convergeErr)
	}
	if len(failures) > 0 {
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonISCSIPathDegraded,
			fmt.Sprintf("iSCSI path convergence for %s is degraded: %v", iqn, errors.Join(failures...)))
	}
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
	multipathAddresses := d.nodeNVMeMultipathAddresses(volumeContext, nqn, eventObjects...)

	mounted, err := util.IsMounted(stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		if len(multipathAddresses) > 0 {
			d.convergeExistingNVMeoFPaths(ctx, volumeContext, eventObjects...)
		}
		klog.Infof("NVMe-oF volume already mounted at %s", stagingPath)
		return nil
	}

	subsystems, listErr := nodeListNVMeSubsystems(ctx)
	if listErr != nil {
		klog.Warningf("Failed to list NVMe subsystems before staging %s: %v", nqn, listErr)
	}

	if volCap != nil && volCap.GetBlock() != nil {
		if devicePath, ok := stagedBlockDevicePath(stagingPath); ok {
			stagedNQN, infoErr := getNVMeInfoFromDevice(devicePath)
			_, findErr := util.FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems)
			if infoErr == nil && findErr == nil && stagedNQN == nqn {
				if len(multipathAddresses) > 0 {
					d.convergeExistingNVMeoFPaths(ctx, volumeContext, eventObjects...)
				}
				klog.Infof("NVMe-oF block volume already staged at %s", stagingPath)
				return nil
			}
		}
	}

	// Connect to NVMe-oF subsystem with configurable timeout
	connectOpts := &util.NVMeoFConnectOptions{
		DeviceTimeout: time.Duration(d.config.NVMeoF.DeviceWaitTimeout) * time.Second,
	}
	var devicePath string
	var pathFailures []error
	if len(multipathAddresses) > 0 {
		// A fresh stage with controllers but no live path is the same wedged
		// session state the historical single-path flow repairs pre-emptively.
		if _, findErr := util.FindNVMeoFSessionByNQNFromSubsystems(nqn, subsystems); findErr == nil &&
			len(util.LiveNVMeoFAddresses(nqn, subsystems)) == 0 {
			subsystems = preemptiveSessionDisconnect(ctx, d, nodeStageTransport[[]util.NVMeSubsystem]{
				name:        "NVMe-oF",
				findSession: func(s []util.NVMeSubsystem) (string, error) { return util.FindNVMeoFSessionByNQNFromSubsystems(nqn, s) },
				disconnect:  func(existing string) error { return nodeNVMeDisconnect(ctx, existing) },
				relist:      func() ([]util.NVMeSubsystem, error) { return nodeListNVMeSubsystems(ctx) },
			}, nqn, stagingPath, subsystems)
		}
		devicePath, pathFailures, err = convergeNVMeoFPaths(ctx, nqn, transport, port, multipathAddresses, connectOpts, subsystems, true)
	} else {
		// Preserve the historical single-path flow byte-for-byte when an older
		// controller supplies no usable multipath hint.
		subsystems = preemptiveSessionDisconnect(ctx, d, nodeStageTransport[[]util.NVMeSubsystem]{
			name:        "NVMe-oF",
			findSession: func(s []util.NVMeSubsystem) (string, error) { return util.FindNVMeoFSessionByNQNFromSubsystems(nqn, s) },
			disconnect:  func(existing string) error { return nodeNVMeDisconnect(ctx, existing) },
			relist:      func() ([]util.NVMeSubsystem, error) { return nodeListNVMeSubsystems(ctx) },
		}, nqn, stagingPath, subsystems)

		transportURI := fmt.Sprintf("%s://%s:%s", transport, address, port)
		devicePath, err = nvmeConnectWithSubsystems(ctx, nqn, transportURI, connectOpts, subsystems)
	}
	if err != nil {
		RecordNodeConnect("nvmeof", "error")
		operationErr := status.Errorf(codes.Internal, "failed to connect NVMe-oF: %v", err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMeConnectFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect("nvmeof", "success")
	if len(pathFailures) > 0 {
		d.recordNVMePathFailures(firstEventObject(eventObjects), nqn, pathFailures)
	}

	if err := d.finalizeStagedDevice(ctx, devicePath, stagingPath, volCap, eventObjects...); err != nil {
		return err
	}
	if len(multipathAddresses) > 0 {
		policySubsystems := subsystems
		if !hasNamedNVMeSubsystem(nqn, policySubsystems) {
			if refreshed, listErr := nodeListNVMeSubsystems(ctx); listErr == nil {
				policySubsystems = refreshed
			} else {
				klog.V(4).Infof("NVMe-oF queue-depth iopolicy listing unavailable for %s: %v", nqn, listErr)
			}
		}
		d.setNVMeoFQueueDepthPolicy(nqn, policySubsystems)
		d.recordNVMeMultipathAggregation(firstEventObject(eventObjects), nqn)
	}
	return nil
}

func parseNVMeoFMultipathAddresses(volumeContext map[string]string) ([]string, error) {
	raw, present := volumeContext["addresses"]
	if !present {
		return nil, nil
	}
	var decoded []string
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("decode addresses: %w", err)
	}
	if len(decoded) == 0 {
		return nil, nil
	}
	addresses := make([]string, 0, len(decoded))
	seen := make(map[string]struct{}, len(decoded))
	for _, rawAddress := range decoded {
		address, err := normalizeNVMeoFAddress(rawAddress)
		if err != nil {
			return nil, fmt.Errorf("addresses contains invalid transport address %q: %w", rawAddress, err)
		}
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func normalizeNVMeoFAddress(rawAddress string) (string, error) {
	if rawAddress != strings.TrimSpace(rawAddress) {
		return "", errors.New("leading or trailing whitespace is not allowed")
	}
	address := rawAddress
	if address == "" {
		return "", errors.New("empty address")
	}
	if strings.Contains(address, "://") {
		return "", errors.New("URI schemes are not allowed")
	}
	if strings.HasPrefix(address, "[") {
		closing := strings.IndexByte(address, ']')
		if closing != len(address)-1 {
			return "", errors.New("bracketed addresses must not include a port or suffix")
		}
		address = address[1:closing]
		if ip := net.ParseIP(address); ip == nil || !strings.Contains(address, ":") {
			return "", errors.New("brackets are valid only around an IPv6 address")
		}
		return address, nil
	}
	if strings.ContainsAny(address, "[] \t\r\n") {
		return "", errors.New("address must be a bare IP")
	}
	if net.ParseIP(address) != nil {
		return address, nil
	}
	return "", errors.New("address must be a bare IP without a port")
}

func (d *Driver) nodeNVMeMultipathAddresses(volumeContext map[string]string, nqn string, eventObjects ...runtime.Object) []string {
	addresses, err := parseNVMeoFMultipathAddresses(volumeContext)
	if err != nil {
		rawAddresses := volumeContext["addresses"]
		message := fmt.Sprintf("NVMe-oF multipath address list for %s was discarded; using single-address fallback; published addresses=%s: %v", nqn, rawAddresses, err)
		klog.Warning(message)
		RecordNVMePathConnect(invalidNVMeMultipathAddressMetricLabel, "error")
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMePathDegraded, message)
		return nil
	}
	return addresses
}

func (d *Driver) convergeExistingNVMeoFPaths(ctx context.Context, volumeContext map[string]string, eventObjects ...runtime.Object) {
	nqn := volumeContext["nqn"]
	addresses := d.nodeNVMeMultipathAddresses(volumeContext, nqn, eventObjects...)
	if len(addresses) == 0 {
		return
	}
	transport := volumeContext["transport"]
	if transport == "" {
		transport = "tcp"
	}
	port := volumeContext["port"]
	if port == "" {
		port = "4420"
	}
	subsystems, err := nodeListNVMeSubsystems(ctx)
	if err != nil {
		klog.Warningf("Failed to list NVMe subsystems before converging staged volume %s: %v", nqn, err)
	}
	connectOpts := &util.NVMeoFConnectOptions{
		DeviceTimeout: time.Duration(d.config.NVMeoF.DeviceWaitTimeout) * time.Second,
	}
	// A compatible staging target remains usable even if every top-up fails.
	// Missing paths are bounded best-effort work on this idempotent replay.
	_, pathFailures, _ := convergeNVMeoFPaths(ctx, nqn, transport, port, addresses, connectOpts, subsystems, false)
	if len(pathFailures) > 0 {
		d.recordNVMePathFailures(firstEventObject(eventObjects), nqn, pathFailures)
	}
	policySubsystems := subsystems
	if !hasNamedNVMeSubsystem(nqn, policySubsystems) {
		if refreshed, listErr := nodeListNVMeSubsystems(ctx); listErr == nil {
			policySubsystems = refreshed
		} else {
			klog.V(4).Infof("NVMe-oF queue-depth iopolicy relist unavailable for staged volume %s: %v", nqn, listErr)
		}
	}
	d.setNVMeoFQueueDepthPolicy(nqn, policySubsystems)
	d.recordNVMeMultipathAggregation(firstEventObject(eventObjects), nqn)
}

func convergeNVMeoFPaths(
	ctx context.Context,
	nqn, transport, port string,
	addresses []string,
	connectOpts *util.NVMeoFConnectOptions,
	subsystems []util.NVMeSubsystem,
	needDevice bool,
) (string, []error, error) {
	liveAddresses := util.LiveNVMeoFAddresses(nqn, subsystems)
	live := make(map[string]struct{}, len(liveAddresses))
	for _, address := range liveAddresses {
		live[address] = struct{}{}
	}
	requestedLive := make([]string, 0, len(addresses))
	missing := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if _, alreadyLive := live[address]; alreadyLive {
			requestedLive = append(requestedLive, address)
			RecordNVMePathConnect(address, "already_live")
			continue
		}
		missing = append(missing, address)
	}
	connected := len(requestedLive) > 0
	devicePath := ""
	connectErrors := make([]error, 0)
	transportURI := func(address string) string {
		return fmt.Sprintf("%s://%s", transport, net.JoinHostPort(address, port))
	}
	connectPath := func(connectCtx context.Context, address string, opts *util.NVMeoFConnectOptions, requireDevice bool) {
		path, err := nvmeConnectPathWithSubsystems(connectCtx, nqn, transportURI(address), opts, subsystems)
		if err == nil && requireDevice && path == "" {
			err = errors.New("connect returned an empty device path")
		}
		if err != nil {
			klog.Warningf("Failed to connect NVMe-oF path %s for %s: %v", address, nqn, err)
			connectErrors = append(connectErrors, fmt.Errorf("%s: %w", address, err))
			RecordNVMePathConnect(address, "error")
			return
		}
		RecordNVMePathConnect(address, "success")
		connected = true
		if path != "" && devicePath == "" {
			devicePath = path
		}
	}

	// If requested controllers are already live, try each one's device-wait
	// behavior within one shared configured-timeout budget. No connect command is
	// issued because the exact-path utility sees each live controller.
	if needDevice && len(requestedLive) > 0 {
		deviceWaitTimeout := util.DefaultNVMeoFDeviceTimeout
		if connectOpts != nil && connectOpts.DeviceTimeout > 0 {
			deviceWaitTimeout = connectOpts.DeviceTimeout
		}
		liveWaitCtx, cancel := context.WithTimeout(ctx, deviceWaitTimeout)
		defer cancel()
		for _, address := range requestedLive {
			path, err := nvmeConnectPathWithSubsystems(liveWaitCtx, nqn, transportURI(address), connectOpts, subsystems)
			if err != nil || path == "" {
				if err == nil {
					err = errors.New("connect returned an empty device path")
				}
				connectErrors = append(connectErrors, fmt.Errorf("%s: %w", address, err))
				continue
			}
			devicePath = path
			break
		}
	}

	// A fresh stage can spend at most two full configured budgets: one shared by
	// already-live device waits above, then one for missing[0]. Later candidates
	// and all already-staged top-ups share one short budget.
	if needDevice && devicePath == "" && len(missing) > 0 {
		connectPath(ctx, missing[0], connectOpts, true)
		missing = missing[1:]
	}
	if len(missing) > 0 {
		topUpCtx, cancel := context.WithTimeout(ctx, nvmeSecondaryPathConvergeBudget)
		defer cancel()
		shortOpts := &util.NVMeoFConnectOptions{DeviceTimeout: nvmeSecondaryPathConvergeBudget}
		if connectOpts != nil && connectOpts.DeviceTimeout > 0 && connectOpts.DeviceTimeout < shortOpts.DeviceTimeout {
			shortOpts.DeviceTimeout = connectOpts.DeviceTimeout
		}
		for _, address := range missing {
			connectPath(topUpCtx, address, shortOpts, needDevice && devicePath == "")
		}
	}

	if !connected {
		return "", connectErrors, fmt.Errorf("no requested NVMe-oF path connected for %s: %w", nqn, errors.Join(connectErrors...))
	}
	if needDevice && devicePath == "" {
		return "", connectErrors, fmt.Errorf("no device path available from a requested NVMe-oF path for %s: %w", nqn, errors.Join(connectErrors...))
	}
	return devicePath, connectErrors, nil
}

func (d *Driver) setNVMeoFQueueDepthPolicy(nqn string, subsystems []util.NVMeSubsystem) {
	found := false
	stamped := 0
	for _, subsystem := range subsystems {
		if subsystem.NQN != nqn || subsystem.Name == "" {
			continue
		}
		found = true
		if err := nodeSetNVMeIOPolicy(subsystem.Name, "queue-depth"); err != nil {
			klog.V(4).Infof("NVMe-oF queue-depth iopolicy unsupported for %s (%s): %v", nqn, subsystem.Name, err)
		} else {
			stamped++
		}
	}
	if !found {
		klog.V(4).Infof("NVMe-oF subsystem not visible while setting queue-depth iopolicy for %s", nqn)
	}
	if stamped > 1 {
		klog.V(2).Infof("NVMe-oF queue-depth iopolicy stamped on %d subsystem directories for %s; enable nvme_core.multipath=Y so the kernel aggregates all paths into one subsystem", stamped, nqn)
	}
}

func (d *Driver) recordNVMeMultipathAggregation(eventObject runtime.Object, nqn string) {
	count, err := countNVMeSubsystemDirsByNQN(nodeNVMeSubsystemSysfsRoot, nqn)
	if err != nil {
		klog.V(4).Infof("NVMe-oF multipath aggregation check unavailable for %s: %v", nqn, err)
		return
	}
	if count <= 1 {
		return
	}
	message := fmt.Sprintf("NVMe-oF multipath for %s is split across %d subsystem directories; enable nvme_core.multipath=Y so the kernel aggregates the paths into one subsystem", nqn, count)
	klog.Warning(message)
	d.recordWarningEvent(eventObject, EventReasonNVMeMultipathUnaggregated, message)
}

func countNVMeSubsystemDirsByNQN(root, nqn string) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, fmt.Errorf("read NVMe subsystem sysfs root: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "nvme-subsys") {
			continue
		}
		subsysNQN, readErr := os.ReadFile(filepath.Join(root, entry.Name(), "subsysnqn"))
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return 0, fmt.Errorf("read NVMe subsystem %s NQN: %w", entry.Name(), readErr)
		}
		if strings.TrimSpace(string(subsysNQN)) == nqn {
			count++
		}
	}
	return count, nil
}

func hasNamedNVMeSubsystem(nqn string, subsystems []util.NVMeSubsystem) bool {
	for _, subsystem := range subsystems {
		if subsystem.NQN == nqn && subsystem.Name != "" {
			return true
		}
	}
	return false
}

func (d *Driver) recordNVMePathFailures(eventObject runtime.Object, nqn string, failures []error) {
	d.recordWarningEvent(eventObject, EventReasonNVMePathDegraded,
		fmt.Sprintf("NVMe-oF path convergence for %s is degraded: %v", nqn, errors.Join(failures...)))
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
