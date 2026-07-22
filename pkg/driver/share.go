package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

var pvcVolumeIDPrefix = regexp.MustCompile(`^pvc-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// protocolShareName converts a dataset base name into a name legal for iSCSI
// targets/extents and NVMe-oF subsystems: TrueNAS requires lowercase
// alphanumerics plus dot, dash, and colon (validated live on 26.0; violations
// surface as a bare -32602 over the WebSocket API). The 64-char cap is the
// iSCSI extent limit — the tightest of the objects sharing this name (targets
// allow 120). When sanitization changes the name, a short hash of the
// original is appended so distinct originals ("Vol-A" vs "vol-a") cannot
// collide. Already-legal names pass through unchanged, and no deployment can
// have a working share >64 chars (extent creation always rejected them).
func protocolShareName(base string) string {
	const maxLen = 64
	var b strings.Builder
	for _, r := range strings.ToLower(base) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == ':', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	sanitized := b.String()
	if sanitized == "" || strings.Trim(sanitized, ".") == "" || !isLowerAlphanumeric(sanitized[0]) {
		sanitized = "x" + sanitized
	}
	if sanitized == base && len(sanitized) <= maxLen {
		return sanitized
	}
	sum := sha256.Sum256([]byte(base))
	suffix := "-" + hex.EncodeToString(sum[:4])
	if len(sanitized) > maxLen-len(suffix) {
		sanitized = sanitized[:maxLen-len(suffix)]
	}
	return sanitized + suffix
}

// iscsiShareName is the single target/extent naming path used by provisioning,
// node cleanup, and session-GC ownership checks.
func (d *Driver) iscsiShareName(volumeID string) string {
	return protocolShareName(volumeID + d.config.ISCSI.NameSuffix)
}

// isDriverISCSITarget reports whether an IQN target identifier is exactly one
// this driver would create for a Kubernetes pvc-<UUID> volume. Recomputing the
// full name through iscsiShareName keeps suffix sanitization and truncation in
// lockstep with provisioning while excluding foreign targets on the same portal.
func (d *Driver) isDriverISCSITarget(iqn string) bool {
	if !strings.HasPrefix(strings.ToLower(iqn), "iqn.") {
		return false
	}
	_, target, ok := strings.Cut(iqn, ":")
	if !ok || target == "" {
		return false
	}
	volumeID := pvcVolumeIDPrefix.FindString(target)
	return volumeID != "" && target == d.iscsiShareName(volumeID)
}

// resolveISCSITargetGroup derives a usable portal/initiator group when
// iscsi.targetGroups is not configured. A target created with no groups is
// accepted by TrueNAS but can never be discovered by any initiator, so share
// creation must fail loudly rather than produce one. The result is cached;
// failures are retried on the next attempt.
func (d *Driver) resolveISCSITargetGroup(ctx context.Context) (*truenas.ISCSITargetGroup, error) {
	d.iscsiGroupMu.Lock()
	defer d.iscsiGroupMu.Unlock()
	if d.iscsiResolvedGroup != nil {
		return d.iscsiResolvedGroup, nil
	}

	host, portStr, err := net.SplitHostPort(d.config.ISCSI.TargetPortal)
	if err != nil {
		host = d.config.ISCSI.TargetPortal
		portStr = "3260"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid iscsi.targetPortal %q: %w", d.config.ISCSI.TargetPortal, err)
	}

	portals, err := d.truenasClient.ISCSIPortalList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list iSCSI portals: %w", err)
	}
	portalID := 0
	for _, p := range portals {
		for _, l := range p.Listen {
			if l.IP == host && l.Port == port {
				portalID = p.ID
				break
			}
		}
		if portalID != 0 {
			break
		}
	}
	if portalID == 0 {
		return nil, fmt.Errorf("no TrueNAS iSCSI portal listens on configured targetPortal %q — create one in TrueNAS or set iscsi.targetGroups explicitly", d.config.ISCSI.TargetPortal)
	}

	initiators, err := d.truenasClient.ISCSIInitiatorList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list iSCSI initiator groups: %w", err)
	}
	initiatorID := 0
	for _, g := range initiators {
		if g.Initiators == nil && !strings.HasPrefix(strings.TrimSpace(g.Comment), "scale-csi fencing:") {
			initiatorID = g.ID
			break
		}
	}
	if initiatorID == 0 {
		created, err := d.truenasClient.ISCSIInitiatorCreate(ctx, "scale-csi allow-all")
		if err != nil {
			return nil, fmt.Errorf("failed to create allow-all iSCSI initiator group: %w", err)
		}
		initiatorID = created.ID
	}

	group := &truenas.ISCSITargetGroup{
		Portal:     portalID,
		Initiator:  initiatorID,
		AuthMethod: "NONE",
	}
	d.iscsiResolvedGroup = group
	klog.Infof("Resolved iSCSI target group automatically: portal=%d (matches %s), initiator=%d", portalID, d.config.ISCSI.TargetPortal, initiatorID)
	return group, nil
}

func (d *Driver) invalidateISCSITargetGroup() {
	d.iscsiGroupMu.Lock()
	defer d.iscsiGroupMu.Unlock()
	d.iscsiResolvedGroup = nil
}

// resolveNVMeoFHostIDs resolves configured initiator NQNs to TrueNAS host IDs.
// The mutex makes resolution single-flight within a driver instance; only
// successful resolutions are cached so API failures are retried.
func (d *Driver) resolveNVMeoFHostIDs(ctx context.Context, nqns []string) ([]int, error) {
	d.nvmeHostMu.Lock()
	defer d.nvmeHostMu.Unlock()
	if d.nvmeResolvedHosts == nil {
		d.nvmeResolvedHosts = make(map[string]int)
	}

	hostIDs := make([]int, 0, len(nqns))
	seen := make(map[string]struct{}, len(nqns))
	for _, nqn := range nqns {
		if _, duplicate := seen[nqn]; duplicate {
			continue
		}
		seen[nqn] = struct{}{}

		if hostID, ok := d.nvmeResolvedHosts[nqn]; ok {
			hostIDs = append(hostIDs, hostID)
			continue
		}

		host, err := d.truenasClient.NVMeoFHostFindByNQN(ctx, nqn)
		if err != nil {
			delete(d.nvmeResolvedHosts, nqn)
			return nil, fmt.Errorf("failed to find NVMe-oF host %q: %w", nqn, err)
		}
		if host == nil {
			host, err = d.truenasClient.NVMeoFHostCreate(ctx, nqn)
			if err != nil {
				delete(d.nvmeResolvedHosts, nqn)
				return nil, fmt.Errorf("failed to create NVMe-oF host %q: %w", nqn, err)
			}
		}
		if host.ID <= 0 {
			delete(d.nvmeResolvedHosts, nqn)
			return nil, fmt.Errorf("resolved NVMe-oF host %q has invalid ID %d", nqn, host.ID)
		}

		d.nvmeResolvedHosts[nqn] = host.ID
		hostIDs = append(hostIDs, host.ID)
	}

	return hostIDs, nil
}

func (d *Driver) invalidateNVMeoFHostIDs(nqns []string) {
	d.nvmeHostMu.Lock()
	defer d.nvmeHostMu.Unlock()
	for _, nqn := range nqns {
		delete(d.nvmeResolvedHosts, nqn)
	}
}

func isNVMeoFHostNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if _, structured := truenas.APIErrno(err); structured {
		return truenas.IsNotFoundError(err)
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "host") &&
		(strings.Contains(message, "not found") ||
			strings.Contains(message, "does not exist") ||
			strings.Contains(message, "no matching"))
}

func (d *Driver) reconcileNVMeoFHostAssociations(ctx context.Context, subsysID int) error {
	if d.config.Fencing.Mode != FencingModeStrict && d.config.NVMeoF.SubsystemAllowAnyHost {
		return nil
	}
	if d.config.Fencing.Mode != FencingModeStrict && len(d.config.NVMeoF.SubsystemHosts) == 0 {
		return status.Error(codes.FailedPrecondition, "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty — no host could connect; set allow-any-host or provide at least one host NQN")
	}
	if d.config.Fencing.Mode == FencingModeStrict {
		if _, err := d.truenasClient.NVMeoFSubsystemUpdateAllowAnyHost(ctx, subsysID, false); err != nil {
			return err
		}
	}

	staticHosts := d.config.NVMeoF.SubsystemHosts
	if d.config.Fencing.Mode == FencingModeStrict {
		staticHosts = nil
	}
	seen := make(map[string]struct{}, len(staticHosts))
	for _, nqn := range staticHosts {
		if _, duplicate := seen[nqn]; duplicate {
			continue
		}
		seen[nqn] = struct{}{}
		if err := d.ensureNVMeoFHostAssociation(ctx, nqn, subsysID); err != nil {
			return err
		}
	}
	if d.config.Fencing.Mode == FencingModeStrict {
		associations, err := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsysID)
		if err != nil {
			return err
		}
		for _, association := range associations {
			if err := d.truenasClient.NVMeoFHostSubsysDelete(ctx, association.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Driver) ensureNVMeoFHostAssociation(ctx context.Context, nqn string, subsysID int) error {
	resolveHostID := func() (int, error) {
		hostIDs, err := d.resolveNVMeoFHostIDs(ctx, []string{nqn})
		if err != nil {
			return 0, err
		}
		if len(hostIDs) != 1 {
			return 0, fmt.Errorf("resolved NVMe-oF host %q to %d IDs", nqn, len(hostIDs))
		}
		return hostIDs[0], nil
	}

	hostID, err := resolveHostID()
	if err != nil {
		return fmt.Errorf("failed to resolve NVMe-oF subsystem host %q: %w", nqn, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		association, findErr := d.truenasClient.NVMeoFHostSubsysFind(ctx, hostID, subsysID)
		if findErr != nil {
			return fmt.Errorf("failed to find NVMe-oF host %q association with subsystem %d: %w", nqn, subsysID, findErr)
		}
		if association != nil {
			return nil
		}

		if _, createErr := d.truenasClient.NVMeoFHostSubsysCreate(ctx, hostID, subsysID); createErr == nil {
			return nil
		} else if attempt == 0 && isNVMeoFHostNotFoundError(createErr) {
			d.invalidateNVMeoFHostIDs([]string{nqn})
			hostID, err = resolveHostID()
			if err != nil {
				return fmt.Errorf("failed to re-resolve NVMe-oF subsystem host %q: %w", nqn, err)
			}
			continue
		} else {
			return fmt.Errorf("failed to associate NVMe-oF host %q with subsystem %d: %w", nqn, subsysID, createErr)
		}
	}
	return nil
}

func datasetUserProperty(ds *truenas.Dataset, key string) string {
	if ds == nil {
		return ""
	}
	if prop, ok := ds.UserProperties[key]; ok {
		return prop.Value
	}
	return ""
}

func datasetUserPropertyProjection(ds *truenas.Dataset, key string) (truenas.UserProperty, bool) {
	if ds == nil {
		return truenas.UserProperty{}, false
	}
	property, ok := ds.UserProperties[key]
	return property, ok
}

func datasetHasLocalUserProperty(ds *truenas.Dataset, key, expected string) bool {
	// An inherited value is not proof that this specific dataset was created or
	// explicitly adopted by this driver instance.
	property, ok := datasetUserPropertyProjection(ds, key)
	return ok && property.Value == expected && strings.EqualFold(strings.TrimSpace(property.Source), "local")
}

func expectedNFSMountpoint(ds *truenas.Dataset, datasetName string) string {
	if ds != nil && ds.Mountpoint != "" {
		return path.Clean(ds.Mountpoint)
	}
	return path.Join("/mnt", datasetName)
}

func nfsShareReferencesPath(share *truenas.NFSShare, expectedPath string) bool {
	if share == nil {
		return false
	}
	expectedPath = path.Clean(expectedPath)
	if share.Path != "" && path.Clean(share.Path) == expectedPath {
		return true
	}
	for _, candidate := range share.Paths {
		if path.Clean(candidate) == expectedPath {
			return true
		}
	}
	return false
}

// resolveNFSShare treats a stored database ID as a cache, never as ownership
// proof. A mismatched ID is ignored and the relationship (export path) is used
// to resolve the correct object.
func (d *Driver) resolveNFSShare(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.NFSShare, error) {
	expectedPath := expectedNFSMountpoint(ds, datasetName)
	if rawID := datasetUserProperty(ds, PropNFSShareID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid NFS share ID %q", rawID)
		}
		share, getErr := d.truenasClient.NFSShareGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify NFS share %d: %w", id, getErr)
		}
		if getErr == nil && nfsShareReferencesPath(share, expectedPath) {
			return share, nil
		}
		if share != nil {
			klog.Warningf("Ignoring stored NFS share ID %d for %s: backend path does not match %s", id, datasetName, expectedPath)
		}
	}
	share, err := d.truenasClient.NFSShareFindByPath(ctx, expectedPath)
	if err != nil {
		return nil, fmt.Errorf("resolve NFS share by path %s: %w", expectedPath, err)
	}
	return share, nil
}

func normalizedZvolReference(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "/dev/")
	return strings.TrimPrefix(value, "/")
}

func (d *Driver) resolveISCSITarget(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.ISCSITarget, error) {
	expectedName := d.iscsiShareName(path.Base(datasetName))
	if rawID := datasetUserProperty(ds, PropISCSITargetID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid iSCSI target ID %q", rawID)
		}
		target, getErr := d.truenasClient.ISCSITargetGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify iSCSI target %d: %w", id, getErr)
		}
		if getErr == nil && target != nil && target.Name == expectedName {
			return target, nil
		}
		if target != nil {
			klog.Warningf("Ignoring stored iSCSI target ID %d for %s: backend name %q does not match %q", id, datasetName, target.Name, expectedName)
		}
	}
	target, err := d.truenasClient.ISCSITargetFindByName(ctx, expectedName)
	if err != nil {
		return nil, fmt.Errorf("resolve iSCSI target by name %s: %w", expectedName, err)
	}
	return target, nil
}

func (d *Driver) resolveISCSIExtent(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.ISCSIExtent, error) {
	expectedDisk := normalizedZvolReference("zvol/" + datasetName)
	if rawID := datasetUserProperty(ds, PropISCSIExtentID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid iSCSI extent ID %q", rawID)
		}
		extent, getErr := d.truenasClient.ISCSIExtentGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify iSCSI extent %d: %w", id, getErr)
		}
		if getErr == nil && extent != nil && normalizedZvolReference(extent.Disk) == expectedDisk {
			return extent, nil
		}
		if extent != nil {
			klog.Warningf("Ignoring stored iSCSI extent ID %d for %s: backend disk %q does not match %q", id, datasetName, extent.Disk, expectedDisk)
		}
	}
	extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, expectedDisk)
	if err != nil {
		return nil, fmt.Errorf("resolve iSCSI extent by disk %s: %w", expectedDisk, err)
	}
	return extent, nil
}

func (d *Driver) resolveISCSITargetExtent(ctx context.Context, ds *truenas.Dataset, targetID, extentID int) (*truenas.ISCSITargetExtent, error) {
	if targetID <= 0 || extentID <= 0 {
		return nil, nil
	}
	if rawID := datasetUserProperty(ds, PropISCSITargetExtentID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid iSCSI target-extent ID %q", rawID)
		}
		association, getErr := d.truenasClient.ISCSITargetExtentGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify iSCSI target-extent %d: %w", id, getErr)
		}
		if getErr == nil && association != nil && association.Target == targetID && association.Extent == extentID {
			return association, nil
		}
		if association != nil {
			klog.Warningf("Ignoring stored iSCSI target-extent ID %d: backend relationship %d/%d does not match %d/%d", id, association.Target, association.Extent, targetID, extentID)
		}
	}
	association, err := d.truenasClient.ISCSITargetExtentFind(ctx, targetID, extentID)
	if err != nil {
		return nil, fmt.Errorf("resolve iSCSI target-extent relationship: %w", err)
	}
	return association, nil
}

func (d *Driver) nvmeSubsystemName(datasetName string) string {
	name := protocolShareName(path.Base(datasetName))
	if d.config.NVMeoF.NamePrefix != "" {
		name = d.config.NVMeoF.NamePrefix + name
	}
	return name + d.config.NVMeoF.NameSuffix
}

func (d *Driver) resolveNVMeNamespace(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.NVMeoFNamespace, error) {
	expectedDevice := normalizedZvolReference("zvol/" + datasetName)
	if rawID := datasetUserProperty(ds, PropNVMeoFNamespaceID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid NVMe-oF namespace ID %q", rawID)
		}
		namespace, getErr := d.truenasClient.NVMeoFNamespaceGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify NVMe-oF namespace %d: %w", id, getErr)
		}
		if getErr == nil && namespace != nil && normalizedZvolReference(namespace.DevicePath) == expectedDevice {
			return namespace, nil
		}
		if namespace != nil {
			klog.Warningf("Ignoring stored NVMe-oF namespace ID %d for %s: backend device %q does not match %q", id, datasetName, namespace.DevicePath, expectedDevice)
		}
	}
	namespace, err := d.truenasClient.NVMeoFNamespaceFindByDevicePath(ctx, expectedDevice)
	if err != nil {
		return nil, fmt.Errorf("resolve NVMe-oF namespace by device %s: %w", expectedDevice, err)
	}
	return namespace, nil
}

func (d *Driver) resolveNVMeSubsystem(ctx context.Context, ds *truenas.Dataset, datasetName string, namespace *truenas.NVMeoFNamespace) (*truenas.NVMeoFSubsystem, error) {
	if namespace != nil && namespace.SubsystemID > 0 {
		subsystem, err := d.truenasClient.NVMeoFSubsystemGet(ctx, namespace.SubsystemID)
		if err != nil && !truenas.IsNotFoundError(err) {
			return nil, fmt.Errorf("resolve namespace subsystem %d: %w", namespace.SubsystemID, err)
		}
		if err == nil && subsystem != nil {
			return subsystem, nil
		}
	}
	expectedName := d.nvmeSubsystemName(datasetName)
	if rawID := datasetUserProperty(ds, PropNVMeoFSubsystemID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid NVMe-oF subsystem ID %q", rawID)
		}
		subsystem, getErr := d.truenasClient.NVMeoFSubsystemGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, fmt.Errorf("verify NVMe-oF subsystem %d: %w", id, getErr)
		}
		if getErr == nil && subsystem != nil && subsystem.Name == expectedName {
			return subsystem, nil
		}
		if subsystem != nil {
			klog.Warningf("Ignoring stored NVMe-oF subsystem ID %d for %s: backend name %q does not match %q", id, datasetName, subsystem.Name, expectedName)
		}
	}
	subsystem, err := d.truenasClient.NVMeoFSubsystemFindByName(ctx, expectedName)
	if err != nil {
		return nil, fmt.Errorf("resolve NVMe-oF subsystem by name %s: %w", expectedName, err)
	}
	return subsystem, nil
}

func (d *Driver) datasetForProperties(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.Dataset, error) {
	if ds != nil {
		return ds, nil
	}
	return d.truenasClient.DatasetGet(ctx, datasetName)
}

func (d *Driver) setDatasetUserProperties(ctx context.Context, ds *truenas.Dataset, datasetName string, properties map[string]string) error {
	if len(properties) == 0 {
		return nil
	}
	return stampAndMirror(ctx, d.truenasClient, ds, datasetName, properties)
}

const (
	// defaultShareRetryAttempts is the number of times to retry share creation
	defaultShareRetryAttempts = 3
	// defaultShareRetryDelay is the initial delay between retry attempts
	defaultShareRetryDelay = 2 * time.Second
)

// ensureShareExists checks if a share exists for the dataset and creates it if missing.
// This is critical for idempotency when a volume was created but share creation failed.
func (d *Driver) ensureShareExists(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, shareType ShareType) error {
	backend := backendForShareType(d, shareType)
	if backend == nil {
		return nil
	}
	return backend.EnsureShare(ctx, ds, datasetName, volumeName)
}

// createShareWithOptions creates a share with additional options.
// shareType should be obtained from config.GetShareType(params) to support StorageClass parameters.
// freshlyCreated skips guaranteed-miss idempotency lookups. zvolReady indicates
// that DatasetCreate returned the zvol or the clone readiness wait completed.
// finalProperties carries the managed/ownership/provision/name stamps that
// CreateVolume would otherwise write in a separate post-share update. NFS folds
// them into the share-ID stamp (one pool.dataset.update on the same side of the
// NFSShareCreate boundary); callers that must not change the idempotent-retry
// path (ensureShareExists) pass nil. Block protocols ignore them and let
// CreateVolume stamp them separately, because their in-share ID stamp is a
// non-fatal best-effort write.
func (d *Driver) createShareWithOptions(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, shareType ShareType, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	klog.Infof("Creating %s share for dataset: %s (freshlyCreated=%v, zvolReady=%v)", shareType, datasetName, freshlyCreated, zvolReady)

	backend := backendForShareType(d, shareType)
	if backend == nil {
		return status.Errorf(codes.InvalidArgument, "unsupported share type: %s", shareType)
	}
	return backend.CreateShare(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady, finalProperties)
}

// deleteShare deletes the share for a dataset.
// shareType should be obtained from config.GetShareType(params) or stored metadata.
func (d *Driver) deleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType) error {
	klog.Infof("Deleting %s share for dataset: %s", shareType, datasetName)

	backend := backendForShareType(d, shareType)
	if backend == nil {
		return nil
	}
	return backend.DeleteShare(ctx, ds, datasetName)
}
