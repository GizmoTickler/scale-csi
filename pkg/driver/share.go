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
		if len(g.Initiators) == 0 {
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
	if !d.config.Fencing.Enabled() && d.config.NVMeoF.SubsystemAllowAnyHost {
		return nil
	}
	if !d.config.Fencing.Enabled() && len(d.config.NVMeoF.SubsystemHosts) == 0 {
		return status.Error(codes.FailedPrecondition, "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty — no host could connect; set allow-any-host or provide at least one host NQN")
	}
	if d.config.Fencing.Enabled() {
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

// datasetLocalUserProperty is intentionally stricter than the general property
// reader. An ownership value inherited from a parent dataset is not proof that
// this specific dataset was created or explicitly adopted by this instance.
func datasetLocalUserProperty(ds *truenas.Dataset, key string) string {
	if ds == nil {
		return ""
	}
	property, ok := ds.UserProperties[key]
	if !ok || strings.Contains(strings.ToLower(property.Source), "inherit") {
		return ""
	}
	return property.Value
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
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, properties); err != nil {
		return err
	}
	if ds != nil {
		if ds.UserProperties == nil {
			ds.UserProperties = make(map[string]truenas.UserProperty, len(properties))
		}
		for key, value := range properties {
			ds.UserProperties[key] = truenas.UserProperty{Value: value}
		}
	}
	return nil
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
	switch shareType {
	case ShareTypeNFS:
		// A replicated dataset can retain the user property while the TrueNAS
		// configuration database no longer contains the referenced share.
		if prop, ok := ds.UserProperties[PropNFSShareID]; ok && prop.Value != "" && prop.Value != "-" {
			shareID, err := strconv.Atoi(prop.Value)
			if err != nil || shareID <= 0 {
				return status.Errorf(codes.Internal, "invalid NFS share ID %q for %s", prop.Value, datasetName)
			}
			share, getErr := d.truenasClient.NFSShareGet(ctx, shareID)
			switch {
			case getErr == nil && nfsShareReferencesPath(share, expectedNFSMountpoint(ds, datasetName)):
				klog.V(4).Infof("NFS share already exists for %s (ID: %s)", datasetName, prop.Value)
				return nil
			case getErr != nil && !truenas.IsNotFoundError(getErr):
				return status.Errorf(codes.Internal, "failed to verify NFS share %d for %s: %v", shareID, datasetName, getErr)
			default:
				delete(ds.UserProperties, PropNFSShareID)
			}
		}
		klog.Infof("NFS share missing for existing volume %s, creating...", datasetName)
		return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, false)

	case ShareTypeISCSI:
		// The create path validates every cached ID against its target/extent
		// relationship before taking the idempotent fast path.
		return d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, false, false)

	case ShareTypeNVMeoF:
		// The create path validates the namespace's device backreference and
		// repairs cached IDs before returning an idempotent success.
		return d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, false, false)

	default:
		return nil
	}
}

// createShareWithOptions creates a share with additional options.
// shareType should be obtained from config.GetShareType(params) to support StorageClass parameters.
// freshlyCreated skips guaranteed-miss idempotency lookups. zvolReady indicates
// that DatasetCreate returned the zvol or the clone readiness wait completed.
func (d *Driver) createShareWithOptions(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, shareType ShareType, freshlyCreated, zvolReady bool) error {
	klog.Infof("Creating %s share for dataset: %s (freshlyCreated=%v, zvolReady=%v)", shareType, datasetName, freshlyCreated, zvolReady)

	switch shareType {
	case ShareTypeNFS:
		return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated)
	case ShareTypeISCSI:
		return d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady)
	case ShareTypeNVMeoF:
		return d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported share type: %s", shareType)
	}
}

// deleteShare deletes the share for a dataset.
// shareType should be obtained from config.GetShareType(params) or stored metadata.
func (d *Driver) deleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType) error {
	klog.Infof("Deleting %s share for dataset: %s", shareType, datasetName)

	switch shareType {
	case ShareTypeNFS:
		return d.deleteNFSShareForDataset(ctx, ds, datasetName)
	case ShareTypeISCSI:
		return d.deleteISCSIShareForDataset(ctx, ds, datasetName)
	case ShareTypeNVMeoF:
		return d.deleteNVMeoFShareForDataset(ctx, ds, datasetName)
	default:
		return nil
	}
}

// createNFSShare creates an NFS share for a dataset.
// mountpoint can be provided to avoid an extra DatasetGet call (empty string triggers lookup).
func (d *Driver) createNFSShare(ctx context.Context, datasetName, volumeName, mountpoint string) error {
	ds, err := d.datasetForProperties(ctx, nil, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}
	if mountpoint != "" {
		ds.Mountpoint = mountpoint
	}
	return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, false)
}

func (d *Driver) createNFSShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated bool) error {
	var err error
	ds, err = d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	if !freshlyCreated {
		existing, resolveErr := d.resolveNFSShare(ctx, ds, datasetName)
		if resolveErr != nil {
			return status.Errorf(codes.Internal, "failed to resolve NFS share: %v", resolveErr)
		}
		if existing != nil {
			if propertyErr := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{PropNFSShareID: strconv.Itoa(existing.ID)}); propertyErr != nil {
				return status.Errorf(codes.Internal, "failed to repair NFS share ID: %v", propertyErr)
			}
			klog.Infof("NFS share already exists for %s (ID %d)", datasetName, existing.ID)
			return nil
		}
	}

	// Create NFS share
	comment := fmt.Sprintf("truenas-csi (%s): %s", d.name, datasetName)

	params := &truenas.NFSShareCreateParams{
		Path:         ds.Mountpoint,
		Comment:      comment,
		Networks:     d.config.NFS.ShareAllowedNetworks,
		Hosts:        d.config.NFS.ShareAllowedHosts,
		Ro:           false,
		MaprootUser:  d.config.NFS.ShareMaprootUser,
		MaprootGroup: d.config.NFS.ShareMaprootGroup,
		MapallUser:   d.config.NFS.ShareMapallUser,
		MapallGroup:  d.config.NFS.ShareMapallGroup,
		Enabled:      !d.config.Fencing.Enabled(),
	}
	if d.config.Fencing.Enabled() {
		// An enabled export with both lists empty means allow-all in TrueNAS. New
		// fenced volumes therefore start disabled until ControllerPublish writes
		// at least one node identity.
		params.Networks = []string{}
		params.Hosts = []string{}
	}

	share, err := d.truenasClient.NFSShareCreate(ctx, params)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NFS share: %v", err)
	}

	// Store share ID in dataset property
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{PropNFSShareID: strconv.Itoa(share.ID)}); err != nil {
		return status.Errorf(codes.Internal, "failed to store NFS share ID: %v", err)
	}

	klog.Infof("Created NFS share ID %d for %s", share.ID, datasetName)
	return nil
}

// deleteNFSShare deletes the NFS share for a dataset.
func (d *Driver) deleteNFSShare(ctx context.Context, datasetName string) error {
	return d.deleteNFSShareForDataset(ctx, nil, datasetName)
}

func (d *Driver) deleteNFSShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	if fetched, err := d.datasetForProperties(ctx, ds, datasetName); err == nil {
		ds = fetched
	} else if !truenas.IsNotFoundError(err) {
		return fmt.Errorf("failed to read dataset before NFS cleanup: %w", err)
	}
	share, err := d.resolveNFSShare(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	if share == nil {
		return nil
	}
	if err := d.truenasClient.NFSShareDelete(ctx, share.ID); err != nil {
		if truenas.IsNotFoundError(err) {
			return nil
		}
		// Return error so caller can retry - don't silently swallow
		return fmt.Errorf("failed to delete NFS share %d: %w", share.ID, err)
	}

	klog.Infof("Deleted NFS share ID %d", share.ID)
	return nil
}

// createISCSIShare creates iSCSI target, extent, and target-extent association.
// This function is idempotent and includes retry logic for robustness during
// high-load scenarios (e.g., volsync backup bursts).
func (d *Driver) createISCSIShare(ctx context.Context, datasetName, volumeName string) error {
	return d.createISCSIShareForDataset(ctx, nil, datasetName, volumeName, false, false)
}

func (d *Driver) createISCSIShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool) error {
	start := time.Now()
	klog.Infof("createISCSIShare: starting for dataset %s", datasetName)
	var err error
	ds, err = d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Generate iSCSI name and disk path upfront
	iscsiName := d.iscsiShareName(path.Base(datasetName))
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	// Step 2: Find or create target (idempotent)
	var target *truenas.ISCSITarget
	var targetID int

	if !freshlyCreated {
		target, err = d.resolveISCSITarget(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve iSCSI target: %v", err)
		}
		if target != nil {
			targetID = target.ID
			klog.V(4).Infof("Resolved existing target %s (ID %d)", iscsiName, targetID)
		}
	}

	// Create target if needed
	if target == nil {
		targetGroups := []truenas.ISCSITargetGroup{}
		usedResolvedGroup := false
		configuredGroups := d.config.ISCSI.TargetGroups
		if d.config.Fencing.Mode == FencingModeStrict {
			configuredGroups = nil
		}
		for _, tg := range configuredGroups {
			if d.config.Fencing.Enabled() {
				initiator, verifyErr := d.truenasClient.ISCSIInitiatorGet(ctx, tg.Initiator)
				if verifyErr != nil {
					return status.Errorf(codes.Internal, "failed to verify configured iSCSI initiator group %d: %v", tg.Initiator, verifyErr)
				}
				if initiator == nil || len(initiator.Initiators) == 0 {
					return status.Errorf(codes.FailedPrecondition,
						"iscsi.targetGroups initiator %d is missing or allow-all; fenced targets require a non-empty initiator allowlist", tg.Initiator)
				}
			}
			var auth *int
			if tg.Auth != nil && *tg.Auth > 0 {
				auth = tg.Auth
			}
			targetGroups = append(targetGroups, truenas.ISCSITargetGroup{
				Portal:     tg.Portal,
				Initiator:  tg.Initiator,
				AuthMethod: tg.AuthMethod,
				Auth:       auth,
			})
		}
		if len(targetGroups) == 0 && !d.config.Fencing.Enabled() {
			resolved, resolveErr := d.resolveISCSITargetGroup(ctx)
			if resolveErr != nil {
				return status.Errorf(codes.Internal, "cannot create iSCSI target for %s: %v", datasetName, resolveErr)
			}
			targetGroups = append(targetGroups, *resolved)
			usedResolvedGroup = true
		}

		target, err = d.truenasClient.ISCSITargetCreate(ctx, iscsiName, "", "ISCSI", targetGroups)
		if err != nil {
			if usedResolvedGroup {
				d.invalidateISCSITargetGroup()
			}
			if freshlyCreated && truenas.IsAlreadyExistsError(err) {
				target, _ = d.truenasClient.ISCSITargetFindByName(ctx, iscsiName)
			}
		}
		if target == nil {
			return status.Errorf(codes.Internal, "failed to create iSCSI target: %v", err)
		}
		targetID = target.ID
		klog.Infof("Created iSCSI target %s (ID %d)", iscsiName, targetID)
	}

	// Step 3: Wait for zvol to be ready before creating extent
	// This is critical for cloned volumes which may not be immediately available
	// Skip if caller already verified zvol readiness (e.g., after cloning)
	if !zvolReady {
		zvolTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
		klog.V(4).Infof("Waiting for zvol %s to be ready before creating extent (timeout: %v)", datasetName, zvolTimeout)
		if _, waitErr := d.truenasClient.WaitForZvolReady(ctx, datasetName, zvolTimeout); waitErr != nil {
			klog.Warningf("Zvol readiness check failed (will attempt extent creation anyway): %v", waitErr)
		}
	} else {
		klog.V(4).Infof("Skipping zvol wait for %s (already verified ready)", datasetName)
	}

	// Step 4: Find or create extent with retry (idempotent)
	var extent *truenas.ISCSIExtent
	var extentID int

	if !freshlyCreated {
		extent, err = d.resolveISCSIExtent(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve iSCSI extent: %v", err)
		}
		if extent != nil {
			extentID = extent.ID
			klog.V(4).Infof("Resolved existing extent by disk path %s (ID %d)", diskPath, extentID)
		}
	}

	// Create extent with retry logic
	if extent == nil {
		comment := fmt.Sprintf("truenas-csi: %s", datasetName)
		var lastErr error

		for attempt := 0; attempt < defaultShareRetryAttempts; attempt++ {
			if attempt > 0 {
				delay := defaultShareRetryDelay * time.Duration(1<<uint(attempt-1))
				klog.V(4).Infof("Retrying extent creation for %s (attempt %d/%d, delay %v)", datasetName, attempt+1, defaultShareRetryAttempts, delay)
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return status.Errorf(codes.DeadlineExceeded, "context canceled during extent creation retry")
				}
			}

			var createErr error
			extent, createErr = d.truenasClient.ISCSIExtentCreate(
				ctx,
				iscsiName,
				diskPath,
				comment,
				d.config.ISCSI.ExtentBlocksize,
				!d.config.ISCSI.ExtentDisablePhysicalBlocksize,
				d.config.ISCSI.ExtentRpm,
			)
			if createErr == nil {
				extentID = extent.ID
				klog.Infof("Created iSCSI extent %s (ID %d) on attempt %d", iscsiName, extentID, attempt+1)
				break
			}
			lastErr = createErr
			klog.Warningf("Extent creation attempt %d failed for %s: %v", attempt+1, datasetName, createErr)

			// Fresh creates only fall back on a definite already-exists result.
			// Existing-volume retries retain the broader ambiguity check.
			if !freshlyCreated || truenas.IsAlreadyExistsError(createErr) {
				e, findErr := d.truenasClient.ISCSIExtentFindByDisk(ctx, diskPath)
				if findErr == nil && e != nil {
					extent = e
					extentID = e.ID
					klog.Infof("Extent found after error (ID %d), continuing", extentID)
					break
				}
			}
		}

		if extent == nil {
			// Cleanup target on failure
			if delErr := d.truenasClient.ISCSITargetDelete(ctx, targetID, true); delErr != nil {
				klog.Warningf("Failed to cleanup iSCSI target after extent creation failure: %v", delErr)
			}
			return status.Errorf(codes.Internal, "failed to create iSCSI extent after %d attempts: %v", defaultShareRetryAttempts, lastErr)
		}
	}

	// Step 5: Find or create target-extent association (idempotent)
	var targetExtent *truenas.ISCSITargetExtent

	// Check if association already exists
	if !freshlyCreated {
		targetExtent, err = d.resolveISCSITargetExtent(ctx, ds, targetID, extentID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve iSCSI target-extent: %v", err)
		}
		if targetExtent != nil {
			klog.V(4).Infof("Using existing target-extent association (ID %d)", targetExtent.ID)
		}
	}

	// Create association if needed
	if targetExtent == nil {
		var err error
		targetExtent, err = d.truenasClient.ISCSITargetExtentCreate(ctx, targetID, extentID, 0)
		if err != nil {
			if freshlyCreated && truenas.IsAlreadyExistsError(err) {
				targetExtent, _ = d.truenasClient.ISCSITargetExtentFind(ctx, targetID, extentID)
			}
		}
		if targetExtent == nil {
			// Cleanup orphaned target and extent on association failure
			// These resources are useless without the association and will block future provisioning
			klog.Errorf("Failed to create target-extent association, cleaning up orphaned resources: %v", err)
			if delErr := d.truenasClient.ISCSIExtentDelete(ctx, extentID, false, true); delErr != nil {
				klog.Warningf("Failed to cleanup orphaned iSCSI extent %d: %v", extentID, delErr)
			}
			if delErr := d.truenasClient.ISCSITargetDelete(ctx, targetID, true); delErr != nil {
				klog.Warningf("Failed to cleanup orphaned iSCSI target %d: %v", targetID, delErr)
			}
			return status.Errorf(codes.Internal, "failed to create target-extent association: %v", err)
		}
		klog.Infof("Created target-extent association (ID %d)", targetExtent.ID)
	}

	// Step 6: Store all property IDs in one dataset update.
	// These properties are used for idempotency on retry and cleanup during deletion.
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
		PropISCSITargetID:       strconv.Itoa(targetID),
		PropISCSIExtentID:       strconv.Itoa(extentID),
		PropISCSITargetExtentID: strconv.Itoa(targetExtent.ID),
	}); err != nil {
		klog.Warningf("Failed to store iSCSI resource IDs: %v", err)
	}

	// Request iSCSI service reload using debouncer to prevent reload storms
	// during bulk volume provisioning. Multiple requests within the debounce
	// window will be coalesced into a single reload operation.
	klog.V(4).Infof("Requesting debounced iSCSI service reload to ensure target is discoverable")
	if d.serviceReloadDebouncer != nil {
		if err := d.serviceReloadDebouncer.RequestReload(ctx, "iscsitarget"); err != nil {
			// Non-fatal: the service might auto-reload, and node has retry logic.
			// Log at WARNING level for operator visibility (not V(4) debug level).
			klog.Warningf("iSCSI service reload failed (non-fatal, will retry on node): %v", err)
		}
	}

	klog.Infof("iSCSI share setup complete for %s: target=%d, extent=%d, targetextent=%d (took %v)",
		datasetName, targetID, extentID, targetExtent.ID, time.Since(start))
	return nil
}

// deleteISCSIShare deletes iSCSI resources for a dataset.
// It tries to delete by stored property IDs first, then falls back to lookup by name
// to handle cases where properties were never stored (e.g., failed volume creation).
// Returns an error if any cleanup fails so the caller can retry.
func (d *Driver) deleteISCSIShare(ctx context.Context, datasetName string) error {
	return d.deleteISCSIShareForDataset(ctx, nil, datasetName)
}

func (d *Driver) deleteISCSIShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	if fetched, err := d.datasetForProperties(ctx, ds, datasetName); err == nil {
		ds = fetched
	} else if !truenas.IsNotFoundError(err) {
		return fmt.Errorf("failed to read dataset before iSCSI cleanup: %w", err)
	}

	target, err := d.resolveISCSITarget(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	extent, err := d.resolveISCSIExtent(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	var initiatorGroup *truenas.ISCSIInitiator
	if d.config.Fencing.Enabled() || datasetUserProperty(ds, PropISCSIInitiatorID) != "" {
		initiatorGroup, err = d.resolveFencingInitiatorGroup(ctx, ds, datasetName)
		if err != nil {
			return fmt.Errorf("failed to resolve per-volume iSCSI initiator group: %w", err)
		}
	}

	var associations []*truenas.ISCSITargetExtent
	switch {
	case target != nil && extent != nil:
		association, resolveErr := d.resolveISCSITargetExtent(ctx, ds, target.ID, extent.ID)
		if resolveErr != nil {
			return resolveErr
		}
		if association != nil {
			associations = append(associations, association)
		}
	case target != nil:
		associations, err = d.truenasClient.ISCSITargetExtentFindByTarget(ctx, target.ID)
	case extent != nil:
		associations, err = d.truenasClient.ISCSITargetExtentFindByExtent(ctx, extent.ID)
	}
	if err != nil {
		return fmt.Errorf("failed to resolve iSCSI target-extent associations: %w", err)
	}

	var errs []error
	for _, association := range associations {
		if deleteErr := d.truenasClient.ISCSITargetExtentDelete(ctx, association.ID, true); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("target-extent %d: %w", association.ID, deleteErr))
		}
	}
	if extent != nil {
		if deleteErr := d.truenasClient.ISCSIExtentDelete(ctx, extent.ID, false, true); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("extent %d: %w", extent.ID, deleteErr))
		}
	}
	if target != nil {
		if deleteErr := d.truenasClient.ISCSITargetDelete(ctx, target.ID, true); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("target %d: %w", target.ID, deleteErr))
		}
	}
	if initiatorGroup != nil {
		if deleteErr := d.truenasClient.ISCSIInitiatorDelete(ctx, initiatorGroup.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("initiator group %d: %w", initiatorGroup.ID, deleteErr))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("iSCSI cleanup errors for %s: %v", datasetName, errs)
	}

	klog.Infof("Deleted iSCSI resources for %s", datasetName)
	return nil
}

func (d *Driver) createNVMeoFShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool) error {
	if !d.config.Fencing.Enabled() && !d.config.NVMeoF.SubsystemAllowAnyHost && len(d.config.NVMeoF.SubsystemHosts) == 0 {
		return status.Error(codes.FailedPrecondition, "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty — no host could connect; set allow-any-host or provide at least one host NQN")
	}

	var err error
	ds, err = d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Generate NVMe-oF subsystem name (TrueNAS 25.10+ auto-generates NQN from name)
	subsysName := d.nvmeSubsystemName(datasetName)
	var subsys *truenas.NVMeoFSubsystem
	if !freshlyCreated {
		namespace, resolveErr := d.resolveNVMeNamespace(ctx, ds, datasetName)
		if resolveErr != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF namespace: %v", resolveErr)
		}
		subsys, resolveErr = d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
		if resolveErr != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem: %v", resolveErr)
		}
		if namespace != nil {
			if subsys == nil || namespace.SubsystemID != subsys.ID {
				return status.Errorf(codes.Internal, "NVMe-oF namespace %d for %s has no matching subsystem", namespace.ID, datasetName)
			}
			if propertyErr := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
				PropNVMeoFSubsystemID: strconv.Itoa(subsys.ID),
				PropNVMeoFNamespaceID: strconv.Itoa(namespace.ID),
			}); propertyErr != nil {
				return status.Errorf(codes.Internal, "failed to repair NVMe-oF object IDs: %v", propertyErr)
			}
			// Fenced allowlists are changed only after ControllerPublishVolume has
			// durably stored the requested node identity. CreateVolume retries and
			// ensure-share checks must not transiently clear a strict subsystem.
			if !d.config.Fencing.Enabled() {
				if reconcileErr := d.reconcileNVMeoFHostAssociations(ctx, subsys.ID); reconcileErr != nil {
					return status.Errorf(codes.Internal, "failed to reconcile NVMe-oF subsystem hosts: %v", reconcileErr)
				}
			}
			klog.Infof("NVMe-oF share already exists for %s (namespace=%d, subsystem=%d)", datasetName, namespace.ID, subsys.ID)
			return nil
		}
	}

	// Wait for zvol to be ready before creating subsystem/namespace
	// This is critical for cloned volumes which may not be immediately available
	// Skip if caller already verified zvol readiness (e.g., after cloning)
	if !zvolReady {
		zvolTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
		klog.V(4).Infof("Waiting for zvol %s to be ready before creating NVMe-oF share (timeout: %v)", datasetName, zvolTimeout)
		if _, waitErr := d.truenasClient.WaitForZvolReady(ctx, datasetName, zvolTimeout); waitErr != nil {
			klog.Warningf("Zvol readiness check failed (will attempt share creation anyway): %v", waitErr)
		}
	} else {
		klog.V(4).Infof("Skipping zvol wait for %s (already verified ready)", datasetName)
	}

	allowAnyHost := d.config.NVMeoF.SubsystemAllowAnyHost && !d.config.Fencing.Enabled()
	staticHosts := d.config.NVMeoF.SubsystemHosts
	if d.config.Fencing.Mode == FencingModeStrict {
		staticHosts = nil
	}
	var hostIDs []int
	if !allowAnyHost && len(staticHosts) > 0 {
		hostIDs, err = d.resolveNVMeoFHostIDs(ctx, staticHosts)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem hosts: %v", err)
		}
	}

	// Create subsystem (TrueNAS 25.10+: serial is auto-generated, hosts are IDs not NQNs).
	subsysWasExisting := subsys != nil
	if subsys == nil {
		subsys, err = d.truenasClient.NVMeoFSubsystemCreate(ctx, subsysName, allowAnyHost, hostIDs)
	}
	if err != nil && len(hostIDs) > 0 && isNVMeoFHostNotFoundError(err) {
		d.invalidateNVMeoFHostIDs(staticHosts)
		hostIDs, err = d.resolveNVMeoFHostIDs(ctx, staticHosts)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to re-resolve NVMe-oF subsystem hosts: %v", err)
		}
		subsys, err = d.truenasClient.NVMeoFSubsystemCreate(
			ctx,
			subsysName,
			allowAnyHost,
			hostIDs,
		)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NVMe-oF subsystem: %v", err)
	}
	if err = d.reconcileNVMeoFHostAssociations(ctx, subsys.ID); err != nil {
		if !subsysWasExisting {
			if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF subsystem after host reconciliation failure: %v", delErr)
			}
		}
		return status.Errorf(codes.Internal, "failed to reconcile NVMe-oF subsystem hosts: %v", err)
	}

	// Get or create the NVMe-oF TCP port BEFORE creating namespace
	// TrueNAS 25.10+: Subsystems must be associated with a port to be accessible over the network
	port, err := d.truenasClient.NVMeoFGetOrCreatePort(
		ctx,
		d.config.NVMeoF.Transport,
		d.config.NVMeoF.TransportAddress,
		d.config.NVMeoF.TransportServiceID,
	)
	if err != nil {
		// Cleanup subsystem on port failure - volume would be unusable without a port
		if !subsysWasExisting {
			if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF subsystem after port failure: %v", delErr)
			}
		}
		return status.Errorf(codes.Internal, "failed to get/create NVMe-oF port: %v", err)
	}

	// Associate subsystem with port (required for network accessibility)
	portSubsys, err := d.truenasClient.NVMeoFPortSubsysCreate(ctx, port.ID, subsys.ID)
	if err != nil {
		d.truenasClient.InvalidateNVMeoFPort(
			d.config.NVMeoF.Transport,
			d.config.NVMeoF.TransportAddress,
			d.config.NVMeoF.TransportServiceID,
		)
		// Cleanup subsystem on association failure - volume would be unusable
		if !subsysWasExisting {
			if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF subsystem after port association failure: %v", delErr)
			}
		}
		return status.Errorf(codes.Internal, "failed to associate subsystem with port: %v", err)
	}
	klog.V(4).Infof("Associated NVMe-oF subsystem %d with port %d (association ID %d)", subsys.ID, port.ID, portSubsys.ID)

	// Create namespace (TrueNAS 25.10+: device_path format is "zvol/pool/vol", device_type is required)
	devicePath := fmt.Sprintf("zvol/%s", datasetName)
	namespace, err := d.truenasClient.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")
	if err != nil {
		// Cleanup port-subsystem association and subsystem on namespace failure
		if delErr := d.truenasClient.NVMeoFPortSubsysDelete(ctx, portSubsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF port-subsystem association: %v", delErr)
		}
		if !subsysWasExisting {
			if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF subsystem: %v", delErr)
			}
		}
		return status.Errorf(codes.Internal, "failed to create NVMe-oF namespace: %v", err)
	}

	// Store all property IDs in one dataset update.
	// These properties are used for idempotency on retry and cleanup during deletion.
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
		PropNVMeoFSubsystemID:  strconv.Itoa(subsys.ID),
		PropNVMeoFPortSubsysID: strconv.Itoa(portSubsys.ID),
		PropNVMeoFNamespaceID:  strconv.Itoa(namespace.ID),
	}); err != nil {
		klog.Warningf("Failed to store NVMe-oF resource IDs: %v", err)
	}

	klog.Infof("Created NVMe-oF subsystem=%d, namespace=%d, port-assoc=%d for %s", subsys.ID, namespace.ID, portSubsys.ID, datasetName)
	return nil
}

// deleteNVMeoFShare deletes NVMe-oF resources for a dataset.
// It tries to delete by stored property IDs first, then falls back to lookup by name/path
// to handle cases where properties were never stored (e.g., failed volume creation).
// Returns an error if any cleanup fails so the caller can retry.
func (d *Driver) deleteNVMeoFShare(ctx context.Context, datasetName string) error {
	return d.deleteNVMeoFShareForDataset(ctx, nil, datasetName)
}

func (d *Driver) deleteNVMeoFShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	if fetched, err := d.datasetForProperties(ctx, ds, datasetName); err == nil {
		ds = fetched
	} else if !truenas.IsNotFoundError(err) {
		return fmt.Errorf("failed to read dataset before NVMe-oF cleanup: %w", err)
	}
	namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	subsystem, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
	if err != nil {
		return err
	}
	var portAssociations []*truenas.NVMeoFPortSubsys
	if subsystem != nil {
		allAssociations, listErr := d.truenasClient.NVMeoFPortSubsysList(ctx)
		if listErr != nil {
			return fmt.Errorf("failed to list NVMe-oF port-subsystem associations: %w", listErr)
		}
		portAssociations = truenas.NVMeoFPortSubsysFilterBySubsystem(allAssociations, subsystem.ID)
	}

	var errs []error
	if namespace != nil {
		if deleteErr := d.truenasClient.NVMeoFNamespaceDelete(ctx, namespace.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("namespace %d: %w", namespace.ID, deleteErr))
		}
	}
	for _, association := range portAssociations {
		if deleteErr := d.truenasClient.NVMeoFPortSubsysDelete(ctx, association.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("port-subsystem %d: %w", association.ID, deleteErr))
		}
	}
	if subsystem != nil {
		if deleteErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsystem.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			errs = append(errs, fmt.Errorf("subsystem %d: %w", subsystem.ID, deleteErr))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("NVMe-oF cleanup errors for %s: %v", datasetName, errs)
	}

	klog.Infof("Deleted NVMe-oF resources for %s", datasetName)
	return nil
}
