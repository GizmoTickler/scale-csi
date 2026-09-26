package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// The userspace NVMe/TCP data path.
//
// A volume staged this way has NO kernel NVMe controller: nvmeublkd, a
// per-node daemon, holds the NVMe/TCP connections (and its own multipath) and
// serves the namespace as /dev/ublkbN. So nothing on this path may consult
// /sys/class/nvme or run nvme-cli, and nothing on the kernel path may mistake a
// ublk device for its own:
//   - staging never connects, disconnects, repairs, re-tunes or converges a
//     kernel session; it asks the daemon to attach and then reuses the kernel
//     path's format/mount/symlink tail;
//   - the identity of a staged /dev/ublkbN (which subsystem, which volume)
//     comes from the daemon, never from sysfs;
//   - unstage detaches the volume from the daemon by volume ID and returns
//     before the kernel session cleanup;
//   - session GC and fast_io_fail_tmo convergence see only kernel
//     controllers, and a ublk device resolves to no NQN in their staged-device
//     scans, so they neither count nor touch it.
//
// Evidence that a volume used this path outlives the mount: a marker file per
// volume under the daemon's runtime directory (tmpfs, so it vanishes with the
// devices on reboot). Unstage uses it to refuse to skip a detach when the
// staging path no longer names the device.

// nvmeUblkDaemon is the subset of the daemon client the node uses.
type nvmeUblkDaemon interface {
	Attach(ctx context.Context, req util.NVMeUblkAttachRequest) (util.NVMeUblkDevice, error)
	Detach(ctx context.Context, volume string) (absent bool, err error)
	List(ctx context.Context) ([]util.NVMeUblkDevice, error)
}

// Seams for tests.
var (
	newNVMeUblkDaemon = func(socketPath string) nvmeUblkDaemon { return util.NewNVMeUblkClient(socketPath) }
	nodeNVMeUblkStat  = func(path string) error {
		_, err := os.Stat(path)
		return err
	}
	// nodeNVMeUblkDevDir is where ublk block devices appear.
	nodeNVMeUblkDevDir = "/dev"
)

// nvmeUblkControlTimeout bounds list and detach. They do no network I/O to
// the target beyond tearing down one device, unlike attach, which connects
// every path and has its own configurable budget.
const nvmeUblkControlTimeout = 30 * time.Second

// nvmeUblkTransportLabel is the transport label on scale_csi_node_connect_total
// for this data path, distinct from the kernel initiator's "nvmeof".
const nvmeUblkTransportLabel = "nvmeof-ublk"

// nvmeUblkMarkerDirName is this driver's directory under the daemon's runtime
// directory.
const nvmeUblkMarkerDirName = "scale-csi"

func (d *Driver) nvmeUblkConfig() NVMeoFUblkConfig {
	return d.config.NVMeoF.Ublk.withDefaults()
}

func (d *Driver) nvmeUblkDaemon() nvmeUblkDaemon {
	return newNVMeUblkDaemon(d.nvmeUblkConfig().SocketPath)
}

// nvmeUblkStatusCode maps a daemon error: Unavailable when nothing listens on
// the socket (the daemon is not running on this node yet), DeadlineExceeded
// when the call ran out of time, Internal otherwise. Both non-Internal codes
// are ones kubelet treats as an uncertain outcome, so it keeps the volume's
// NodeUnstage obligation: an attach that timed out here may still complete in
// the daemon, and only an unstage detaches it.
func nvmeUblkStatusCode(err error) codes.Code {
	switch {
	case errors.Is(err, util.ErrNVMeUblkDaemonUnavailable):
		return codes.Unavailable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return codes.DeadlineExceeded
	}
	return codes.Internal
}

// nvmeUblkHostIdentity returns the identity the daemon must connect with: the
// node's own host NQN (the one NodeGetInfo reported, and so the one strict
// fencing allowlists for this node) and the matching host ID.
func (d *Driver) nvmeUblkHostIdentity() (hostNQN, hostID string, err error) {
	identity, err := parseNodeIdentity(d.encodedNodeID)
	if err != nil {
		return "", "", fmt.Errorf("decode this node's identity: %w", err)
	}
	hostNQN = strings.TrimSpace(identity.NVMeNQN)
	if hostNQN == "" {
		return "", "", errors.New("this node reported no NVMe host NQN at startup (nvme show-hostnqn); " +
			"the daemon must connect with the node's own NQN, which is what publication fencing admits")
	}
	hostID, err = nodeNVMeHostID(hostNQN)
	if err != nil {
		return "", "", err
	}
	return hostNQN, hostID, nil
}

// nvmeUblkAddresses builds the daemon's portal list from the same address set
// the kernel path connects: every multipath address when the volume carries a
// usable hint, otherwise the single address. The kernel path's degraded-hint
// event and metric are shared through nodeNVMeMultipathAddresses.
func (d *Driver) nvmeUblkAddresses(volumeContext map[string]string, nqn, address, port string, eventObjects ...runtime.Object) []string {
	addresses := d.nodeNVMeMultipathAddresses(volumeContext, nqn, eventObjects...)
	if len(addresses) == 0 {
		addresses = []string{strings.TrimSuffix(strings.TrimPrefix(address, "["), "]")}
	}
	portals := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		portals = append(portals, net.JoinHostPort(addr, port))
	}
	return portals
}

// nvmeUblkMarkerPath is this volume's marker: one file per (driver, volume)
// under <socket dir>/scale-csi. The name is a hash because volume IDs contain
// path separators.
func (d *Driver) nvmeUblkMarkerPath(volumeID string) string {
	sum := sha256.Sum256([]byte(d.name + "\x00" + volumeID))
	return filepath.Join(filepath.Dir(d.nvmeUblkConfig().SocketPath), nvmeUblkMarkerDirName, hex.EncodeToString(sum[:16]))
}

func (d *Driver) writeNVMeUblkMarker(volumeID string) error {
	path := d.nvmeUblkMarkerPath(volumeID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create ublk marker directory: %w", err)
	}
	if err := os.WriteFile(path, []byte(d.name+"\n"+volumeID+"\n"), 0o600); err != nil {
		return fmt.Errorf("write ublk marker: %w", err)
	}
	return nil
}

func (d *Driver) nvmeUblkMarkerExists(volumeID string) (bool, error) {
	_, err := os.Stat(d.nvmeUblkMarkerPath(volumeID))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("check ublk marker: %w", err)
	}
}

func (d *Driver) removeNVMeUblkMarker(volumeID string) error {
	if err := os.Remove(d.nvmeUblkMarkerPath(volumeID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove ublk marker: %w", err)
	}
	return nil
}

// nvmeUblkDeviceAt asks the daemon which volume it serves at devicePath.
func (d *Driver) nvmeUblkDeviceAt(ctx context.Context, devicePath string) (util.NVMeUblkDevice, error) {
	listCtx, cancel := context.WithTimeout(ctx, nvmeUblkControlTimeout)
	defer cancel()
	devices, err := d.nvmeUblkDaemon().List(listCtx)
	if err != nil {
		return util.NVMeUblkDevice{}, err
	}
	want := filepath.Clean(devicePath)
	for _, device := range devices {
		if filepath.Clean(device.Path) == want {
			return device, nil
		}
	}
	return util.NVMeUblkDevice{}, fmt.Errorf("nvmeublkd serves no device at %s", devicePath)
}

// verifyNVMeUblkStageSource is verifyStageDeviceSource for a /dev/ublkbN: the
// daemon, not sysfs, says which subsystem and which volume the device serves.
func (d *Driver) verifyNVMeUblkStageSource(ctx context.Context, volumeID, devicePath string, volumeContext map[string]string) error {
	device, err := d.nvmeUblkDeviceAt(ctx, devicePath)
	if err != nil {
		return status.Errorf(nvmeUblkStatusCode(err), "failed to identify staged NVMe-oF device %s through nvmeublkd: %v", devicePath, err)
	}
	if device.SubNQN != volumeContext["nqn"] {
		return status.Errorf(codes.AlreadyExists, "staging target is backed by NVMe-oF subsystem %s, requested %s", device.SubNQN, volumeContext["nqn"])
	}
	if device.Volume != volumeID {
		return status.Errorf(codes.AlreadyExists, "staging target is backed by nvmeublkd volume %s, requested %s", device.Volume, volumeID)
	}
	return nil
}

// validateNVMeUblkRawBlockOwnership is validateRawBlockDeviceOwnership for a
// /dev/ublkbN, with the same expected-subsystem rule as the kernel path plus
// the daemon's own volume key.
func (d *Driver) validateNVMeUblkRawBlockOwnership(ctx context.Context, volumeID, devicePath string) error {
	device, err := d.nvmeUblkDeviceAt(ctx, devicePath)
	if err != nil {
		return status.Errorf(nvmeUblkStatusCode(err), "failed to identify nvmeublkd device for raw block device %s: %v", devicePath, err)
	}
	expected := d.config.NVMeoF.NamePrefix + protocolShareName(volumeID) + d.config.NVMeoF.NameSuffix
	if !sessionTargetMatchesExpected(device.SubNQN, expected) {
		return status.Errorf(codes.FailedPrecondition,
			"raw block staging device %s belongs to NVMe-oF subsystem %s, expected volume subsystem %s",
			devicePath, device.SubNQN, expected)
	}
	if device.Volume != volumeID {
		return status.Errorf(codes.FailedPrecondition,
			"raw block staging device %s is served for nvmeublkd volume %s, expected %s", devicePath, device.Volume, volumeID)
	}
	return nil
}

// isNVMeUblkStaged reports whether the recorded live source of a compatible
// existing stage is a ublk device. handleExistingStage records it just before
// NodeStageVolume consults this.
func (d *Driver) isNVMeUblkStaged(stagingPath string) bool {
	record, ok := d.stageRecord(stagingPath)
	return ok && util.IsNVMeUblkDevice(record.LiveSource)
}

// stageNVMeoFUblkVolume stages volumeID through nvmeublkd. It never runs
// nvme-cli: no connect, no pre-emptive disconnect of a wedged session, no
// iopolicy and no path convergence. The daemon's attach is idempotent per
// volume, so a retry after any partial failure gets the same device back.
func (d *Driver) stageNVMeoFUblkVolume(ctx context.Context, volumeID string, volumeContext map[string]string, stagingPath string, volCap *csi.VolumeCapability, eventObjects ...runtime.Object) error {
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
	if volumeID == "" {
		return status.Error(codes.InvalidArgument, "volume ID is required for the ublk NVMe-oF data path")
	}
	if transport == "" {
		transport = "tcp"
	}
	if !strings.EqualFold(transport, "tcp") {
		return status.Errorf(codes.InvalidArgument, "the ublk NVMe-oF data path supports only the tcp transport, volume uses %s", transport)
	}
	if port == "" {
		port = "4420"
	}
	if !d.config.NVMeoF.ublkAvailable() {
		return status.Error(codes.FailedPrecondition,
			"volume selects the ublk NVMe-oF data path, which is not enabled on this node (nvmeof.ublk.enabled)")
	}
	addrs := d.nvmeUblkAddresses(volumeContext, nqn, address, port, eventObjects...)

	// Idempotent replays. A mounted staging path was already identified
	// through the daemon by handleExistingStage; a block symlink is checked
	// here, and anything that does not verify falls through to an attach that
	// returns the volume's device and an atomic replacement of the link.
	mounted, err := util.IsMountedWithContext(ctx, stagingPath)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check mount status: %v", err)
	}
	if mounted {
		klog.Infof("NVMe-oF volume %s already mounted at %s (ublk data path)", volumeID, stagingPath)
		return nil
	}
	if volCap != nil && volCap.GetBlock() != nil {
		if devicePath, ok := stagedBlockDevicePath(stagingPath); ok && util.IsNVMeUblkDevice(devicePath) {
			verifyErr := d.verifyNVMeUblkStageSource(ctx, volumeID, devicePath, volumeContext)
			if verifyErr == nil {
				klog.Infof("NVMe-oF block volume %s already staged at %s (ublk data path)", volumeID, stagingPath)
				return nil
			}
			klog.Infof("Re-attaching NVMe-oF block volume %s: staged device %s did not verify: %v", volumeID, devicePath, verifyErr)
		}
	}

	hostNQN, hostID, err := d.nvmeUblkHostIdentity()
	if err != nil {
		operationErr := status.Errorf(codes.FailedPrecondition, "cannot stage NVMe-oF volume %s through nvmeublkd: %v", volumeID, err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMeConnectFailed, operationErr.Error())
		return operationErr
	}
	// The marker goes down BEFORE the attach: an attach that times out here
	// may still complete in the daemon, and unstage must then know to detach.
	if markerErr := d.writeNVMeUblkMarker(volumeID); markerErr != nil {
		return status.Errorf(codes.Internal, "failed to record the ublk data path for volume %s: %v", volumeID, markerErr)
	}

	ublk := d.nvmeUblkConfig()
	attachCtx, cancel := context.WithTimeout(ctx, time.Duration(ublk.AttachTimeout)*time.Second)
	device, err := d.nvmeUblkDaemon().Attach(attachCtx, util.NVMeUblkAttachRequest{
		Volume:   volumeID,
		SubNQN:   nqn,
		Addrs:    addrs,
		HostNQN:  hostNQN,
		HostID:   hostID,
		Queues:   ublk.Queues,
		Depth:    ublk.Depth,
		ZeroCopy: *ublk.ZeroCopy,
		NapiUs:   ublk.NapiUs,
	})
	cancel()
	if err == nil {
		err = validateNVMeUblkAttachedDevice(device)
	}
	if err != nil {
		RecordNodeConnect(nvmeUblkTransportLabel, "error")
		operationErr := status.Errorf(nvmeUblkStatusCode(err), "failed to attach NVMe-oF volume %s through nvmeublkd: %v", volumeID, err)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMeConnectFailed, operationErr.Error())
		return operationErr
	}
	RecordNodeConnect(nvmeUblkTransportLabel, "success")
	klog.Infof("nvmeublkd attached NVMe-oF volume %s (%s) at %s (existing=%t, paths=%v)", volumeID, nqn, device.Path, device.Existing, addrs)
	d.recordNVMeUblkPathHealth(ctx, volumeID, nqn, firstEventObject(eventObjects))

	return d.finalizeStagedDevice(ctx, device.Path, stagingPath, volCap, eventObjects...)
}

// validateNVMeUblkAttachedDevice refuses a device path that is not exactly
// the ublk block device the daemon says it created: the path is about to be
// formatted, mounted or handed to a pod.
func validateNVMeUblkAttachedDevice(device util.NVMeUblkDevice) error {
	want := filepath.Join(nodeNVMeUblkDevDir, fmt.Sprintf("ublkb%d", device.DevID))
	if device.DevID < 0 || device.Path != want {
		return fmt.Errorf("nvmeublkd returned device %q for dev_id %d, want %s", device.Path, device.DevID, want)
	}
	if err := nodeNVMeUblkStat(device.Path); err != nil {
		return fmt.Errorf("nvmeublkd reported %s but it is not present on this node: %w", device.Path, err)
	}
	return nil
}

// recordNVMeUblkPathHealth surfaces paths the daemon reports down right after
// attach, as the kernel path does for paths that failed to connect. It is
// best-effort: the volume is usable while any path is up.
func (d *Driver) recordNVMeUblkPathHealth(ctx context.Context, volumeID, nqn string, eventObject runtime.Object) {
	listCtx, cancel := context.WithTimeout(ctx, nvmeUblkControlTimeout)
	defer cancel()
	devices, err := d.nvmeUblkDaemon().List(listCtx)
	if err != nil {
		klog.V(4).Infof("nvmeublkd path health for %s unavailable: %v", volumeID, err)
		return
	}
	for _, device := range devices {
		if device.Volume != volumeID {
			continue
		}
		var down []error
		for _, path := range device.Paths {
			if !path.Up {
				down = append(down, fmt.Errorf("%s: path is down in nvmeublkd", path.Addr))
			}
		}
		if len(down) > 0 {
			d.recordNVMePathFailures(eventObject, nqn, down)
		}
		return
	}
}

// unstageNVMeoFUblkVolume detaches volumeID from nvmeublkd when anything shows
// it used the userspace data path: the staged device itself, or the volume's
// marker. handled reports that NodeUnstageVolume must return now because the
// kernel session cleanup has nothing to do for this volume. A detach failure
// is never skipped: the daemon would keep serving the volume.
//
// The only case that detaches AND falls through to the kernel cleanup is a
// marker left beside a positively kernel device (a volume re-staged through
// the kernel path after a ublk attach that never completed): the stale daemon
// attachment is removed and the live kernel session still gets its cleanup.
func (d *Driver) unstageNVMeoFUblkVolume(ctx context.Context, volumeID, devicePath string) (handled bool, err error) {
	ublkDevice := util.IsNVMeUblkDevice(devicePath)
	marked := false
	// The marker directory exists only where the ublk path is enabled; a
	// kernel-only node never looks for it.
	if d.config.NVMeoF.ublkAvailable() {
		if marked, err = d.nvmeUblkMarkerExists(volumeID); err != nil {
			return true, status.Errorf(codes.Internal, "cannot tell whether volume %s used the ublk data path: %v", volumeID, err)
		}
	}
	if !ublkDevice && !marked {
		return false, nil
	}

	detachCtx, cancel := context.WithTimeout(ctx, nvmeUblkControlTimeout)
	absent, detachErr := d.nvmeUblkDaemon().Detach(detachCtx, volumeID)
	cancel()
	if detachErr != nil {
		return true, status.Errorf(nvmeUblkStatusCode(detachErr), "failed to detach volume %s from nvmeublkd: %v", volumeID, detachErr)
	}
	if absent {
		klog.V(4).Infof("nvmeublkd had no attachment for volume %s", volumeID)
	} else {
		klog.Infof("Detached NVMe-oF volume %s from nvmeublkd", volumeID)
	}
	if err := d.removeNVMeUblkMarker(volumeID); err != nil {
		return true, status.Errorf(codes.Internal, "detached volume %s from nvmeublkd but %v", volumeID, err)
	}
	if !ublkDevice && devicePath != "" {
		return false, nil
	}
	return true, nil
}

// expandNVMeUblkVolume is NodeExpandVolume for a ublk-staged volume. The
// daemon sizes a device from the namespace when it attaches and cannot grow
// a live one, so there is no rescan: if the device already covers the
// request (it was attached after the controller expanded the zvol, the usual
// case for an offline expansion) only the filesystem grows; otherwise the
// caller is told to re-stage.
func (d *Driver) expandNVMeUblkVolume(ctx context.Context, volumeID, volumePath, devicePath string, rawBlock bool, capacityBytes int64) (*csi.NodeExpandVolumeResponse, error) {
	if rawBlock {
		if err := d.validateNVMeUblkRawBlockOwnership(ctx, volumeID, devicePath); err != nil {
			return nil, err
		}
	}
	sizeBytes, err := nodeGetDeviceSize(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read size of %s: %v", devicePath, err)
	}
	if capacityBytes > 0 && sizeBytes < capacityBytes {
		return nil, status.Errorf(codes.FailedPrecondition,
			"ublk device %s is %d bytes, below the requested %d: nvmeublkd cannot grow a live device; "+
				"it picks up the new size when the volume is next staged (restart the pod)",
			devicePath, sizeBytes, capacityBytes)
	}
	if !rawBlock {
		if err := nodeResizeFilesystem(ctx, volumePath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resize filesystem: %v", err)
		}
	}
	klog.Infof("Volume %s expanded on ublk device %s (%d bytes)", volumeID, devicePath, sizeBytes)
	return &csi.NodeExpandVolumeResponse{CapacityBytes: sizeBytes}, nil
}
