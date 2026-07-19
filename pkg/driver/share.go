package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

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
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "host") &&
		(strings.Contains(message, "not found") ||
			strings.Contains(message, "does not exist") ||
			strings.Contains(message, "no matching"))
}

func (d *Driver) reconcileNVMeoFHostAssociations(ctx context.Context, subsysID int) error {
	if d.config.NVMeoF.SubsystemAllowAnyHost {
		return nil
	}
	if len(d.config.NVMeoF.SubsystemHosts) == 0 {
		return status.Error(codes.FailedPrecondition, "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty — no host could connect; set allow-any-host or provide at least one host NQN")
	}

	seen := make(map[string]struct{}, len(d.config.NVMeoF.SubsystemHosts))
	for _, nqn := range d.config.NVMeoF.SubsystemHosts {
		if _, duplicate := seen[nqn]; duplicate {
			continue
		}
		seen[nqn] = struct{}{}
		if err := d.ensureNVMeoFHostAssociation(ctx, nqn, subsysID); err != nil {
			return err
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
		// Check if NFS share ID is stored
		if prop, ok := ds.UserProperties[PropNFSShareID]; ok && prop.Value != "" && prop.Value != "-" {
			klog.V(4).Infof("NFS share already exists for %s (ID: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("NFS share missing for existing volume %s, creating...", datasetName)
		return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, false)

	case ShareTypeISCSI:
		// Check if iSCSI target-extent association exists (this means full setup is complete)
		if prop, ok := ds.UserProperties[PropISCSITargetExtentID]; ok && prop.Value != "" && prop.Value != "-" {
			klog.V(4).Infof("iSCSI share already exists for %s (targetextent: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("iSCSI share missing for existing volume %s, creating...", datasetName)
		return d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, false, false)

	case ShareTypeNVMeoF:
		// Check if NVMe-oF namespace ID is stored
		if prop, ok := ds.UserProperties[PropNVMeoFNamespaceID]; ok && prop.Value != "" && prop.Value != "-" {
			if !d.config.NVMeoF.SubsystemAllowAnyHost {
				subsysID, err := strconv.Atoi(datasetUserProperty(ds, PropNVMeoFSubsystemID))
				if err != nil || subsysID <= 0 {
					return status.Errorf(codes.Internal, "invalid NVMe-oF subsystem ID for %s", datasetName)
				}
				if err := d.reconcileNVMeoFHostAssociations(ctx, subsysID); err != nil {
					return status.Errorf(codes.Internal, "failed to reconcile NVMe-oF subsystem hosts for %s: %v", datasetName, err)
				}
			}
			klog.V(4).Infof("NVMe-oF share already exists for %s (namespace: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("NVMe-oF share missing for existing volume %s, creating...", datasetName)
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

	// Check if share already exists
	if existingProp := datasetUserProperty(ds, PropNFSShareID); !freshlyCreated && existingProp != "" && existingProp != "-" {
		klog.Infof("NFS share already exists for %s", datasetName)
		return nil
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
	// Get share ID from dataset property
	ds, err := d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return nil
	}
	shareIDStr := datasetUserProperty(ds, PropNFSShareID)
	if shareIDStr == "" || shareIDStr == "-" {
		return nil // No share to delete
	}

	shareID, err := strconv.Atoi(shareIDStr)
	if err != nil {
		return nil
	}

	if err := d.truenasClient.NFSShareDelete(ctx, shareID); err != nil {
		// Return error so caller can retry - don't silently swallow
		return fmt.Errorf("failed to delete NFS share %d: %w", shareID, err)
	}

	klog.Infof("Deleted NFS share ID %d", shareID)
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
	iscsiName := protocolShareName(path.Base(datasetName) + d.config.ISCSI.NameSuffix)
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	// Step 1: Check if already fully configured (idempotency fast-path)
	existingTE := datasetUserProperty(ds, PropISCSITargetExtentID)
	if !freshlyCreated && existingTE != "" && existingTE != "-" {
		// Verify the target-extent still exists by looking it up by ID
		teID, atoiErr := strconv.Atoi(existingTE)
		if atoiErr == nil {
			if te, findErr := d.truenasClient.ISCSITargetExtentGet(ctx, teID); findErr == nil && te != nil {
				klog.Infof("iSCSI share already fully configured for %s (targetextent=%d)", datasetName, teID)
				return nil
			}
		}
		klog.V(4).Infof("Stored target-extent ID %s invalid or not found, will recreate", existingTE)
	}

	// Step 2: Find or create target (idempotent)
	var target *truenas.ISCSITarget
	var targetID int

	// Check if we have a stored target ID
	existingTargetID := datasetUserProperty(ds, PropISCSITargetID)
	if !freshlyCreated && existingTargetID != "" && existingTargetID != "-" {
		if id, atoiErr := strconv.Atoi(existingTargetID); atoiErr == nil {
			if t, findErr := d.truenasClient.ISCSITargetGet(ctx, id); findErr == nil {
				target = t
				targetID = t.ID
				klog.V(4).Infof("Using existing target ID %d for %s", targetID, datasetName)
			}
		}
	}

	// If no stored target, check by name
	if !freshlyCreated && target == nil {
		if t, findErr := d.truenasClient.ISCSITargetFindByName(ctx, iscsiName); findErr == nil && t != nil {
			target = t
			targetID = t.ID
			klog.V(4).Infof("Found existing target by name %s (ID %d)", iscsiName, targetID)
		}
	}

	// Create target if needed
	if target == nil {
		targetGroups := []truenas.ISCSITargetGroup{}
		usedResolvedGroup := false
		for _, tg := range d.config.ISCSI.TargetGroups {
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
		if len(targetGroups) == 0 {
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

	// Check if we have a stored extent ID
	existingExtentID := datasetUserProperty(ds, PropISCSIExtentID)
	if !freshlyCreated && existingExtentID != "" && existingExtentID != "-" {
		if id, err := strconv.Atoi(existingExtentID); err == nil {
			if e, err := d.truenasClient.ISCSIExtentGet(ctx, id); err == nil {
				extent = e
				extentID = e.ID
				klog.V(4).Infof("Using existing extent ID %d for %s", extentID, datasetName)
			}
		}
	}

	// If no stored extent, check by disk path (more reliable than name for clones)
	if !freshlyCreated && extent == nil {
		if e, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, diskPath); err == nil && e != nil {
			extent = e
			extentID = e.ID
			klog.V(4).Infof("Found existing extent by disk path %s (ID %d)", diskPath, extentID)
		}
	}

	// If still no extent, check by name
	if !freshlyCreated && extent == nil {
		if e, err := d.truenasClient.ISCSIExtentFindByName(ctx, iscsiName); err == nil && e != nil {
			extent = e
			extentID = e.ID
			klog.V(4).Infof("Found existing extent by name %s (ID %d)", iscsiName, extentID)
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

			var err error
			extent, err = d.truenasClient.ISCSIExtentCreate(
				ctx,
				iscsiName,
				diskPath,
				comment,
				d.config.ISCSI.ExtentBlocksize,
				d.config.ISCSI.ExtentRpm,
			)
			if err == nil {
				extentID = extent.ID
				klog.Infof("Created iSCSI extent %s (ID %d) on attempt %d", iscsiName, extentID, attempt+1)
				break
			}
			lastErr = err
			klog.Warningf("Extent creation attempt %d failed for %s: %v", attempt+1, datasetName, err)

			// Fresh creates only fall back on a definite already-exists result.
			// Existing-volume retries retain the broader ambiguity check.
			if !freshlyCreated || truenas.IsAlreadyExistsError(err) {
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
		if te, err := d.truenasClient.ISCSITargetExtentFind(ctx, targetID, extentID); err == nil && te != nil {
			targetExtent = te
			klog.V(4).Infof("Using existing target-extent association (ID %d)", te.ID)
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
	if err := d.serviceReloadDebouncer.RequestReload(ctx, "iscsitarget"); err != nil {
		// Non-fatal: the service might auto-reload, and node has retry logic.
		// Log at WARNING level for operator visibility (not V(4) debug level).
		klog.Warningf("iSCSI service reload failed (non-fatal, will retry on node): %v", err)
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
	}

	// Generate the expected iSCSI name (same logic as createISCSIShare)
	iscsiName := protocolShareName(path.Base(datasetName) + d.config.ISCSI.NameSuffix)
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	var extDeleted, tgtDeleted bool
	var errs []error

	// Try to delete target-extent association by stored ID
	if teIDStr := datasetUserProperty(ds, PropISCSITargetExtentID); teIDStr != "" && teIDStr != "-" {
		if teID, err := strconv.Atoi(teIDStr); err == nil {
			if err := d.truenasClient.ISCSITargetExtentDelete(ctx, teID, true); err != nil {
				klog.Warningf("Failed to delete iSCSI target-extent %d: %v", teID, err)
				errs = append(errs, fmt.Errorf("target-extent %d: %w", teID, err))
			}
		}
	}

	// Try to delete extent by stored ID
	if extIDStr := datasetUserProperty(ds, PropISCSIExtentID); extIDStr != "" && extIDStr != "-" {
		if extID, err := strconv.Atoi(extIDStr); err == nil {
			if err := d.truenasClient.ISCSIExtentDelete(ctx, extID, false, true); err != nil {
				klog.Warningf("Failed to delete iSCSI extent %d: %v", extID, err)
				errs = append(errs, fmt.Errorf("extent %d: %w", extID, err))
			} else {
				extDeleted = true
			}
		}
	}

	// Try to delete target by stored ID
	if tgtIDStr := datasetUserProperty(ds, PropISCSITargetID); tgtIDStr != "" && tgtIDStr != "-" {
		if tgtID, err := strconv.Atoi(tgtIDStr); err == nil {
			if err := d.truenasClient.ISCSITargetDelete(ctx, tgtID, true); err != nil {
				klog.Warningf("Failed to delete iSCSI target %d: %v", tgtID, err)
				errs = append(errs, fmt.Errorf("target %d: %w", tgtID, err))
			} else {
				tgtDeleted = true
			}
		}
	}

	// Fallback: If extent was not deleted by ID, try to find and delete by disk path
	// This handles cases where the dataset properties were never stored
	if !extDeleted {
		if extent, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, diskPath); err == nil && extent != nil {
			klog.V(4).Infof("Found orphaned extent by disk path %s (ID %d), deleting", diskPath, extent.ID)
			// First delete any target-extent associations for this extent
			assocs, assocErr := d.truenasClient.ISCSITargetExtentFindByExtent(ctx, extent.ID)
			if assocErr != nil {
				klog.Warningf("Failed to query target-extent associations for extent %d: %v", extent.ID, assocErr)
				// Continue anyway - extent deletion with force=true may still work
			}
			for _, assoc := range assocs {
				if err := d.truenasClient.ISCSITargetExtentDelete(ctx, assoc.ID, true); err != nil {
					klog.Warningf("Failed to delete orphaned target-extent %d: %v", assoc.ID, err)
					errs = append(errs, fmt.Errorf("orphaned target-extent %d: %w", assoc.ID, err))
				}
			}
			if err := d.truenasClient.ISCSIExtentDelete(ctx, extent.ID, false, true); err != nil {
				klog.Warningf("Failed to delete orphaned extent %d: %v", extent.ID, err)
				errs = append(errs, fmt.Errorf("orphaned extent %d: %w", extent.ID, err))
			}
		}
	}

	// Fallback: If target was not deleted by ID, try to find and delete by name
	// This handles cases where target was created but property was never stored
	if !tgtDeleted {
		if target, err := d.truenasClient.ISCSITargetFindByName(ctx, iscsiName); err == nil && target != nil {
			klog.V(4).Infof("Found orphaned target by name %s (ID %d), deleting", iscsiName, target.ID)
			// First delete any target-extent associations for this target
			assocs, assocErr := d.truenasClient.ISCSITargetExtentFindByTarget(ctx, target.ID)
			if assocErr != nil {
				klog.Warningf("Failed to query target-extent associations for target %d: %v", target.ID, assocErr)
				// Continue anyway - target deletion with force=true may still work
			}
			for _, assoc := range assocs {
				if err := d.truenasClient.ISCSITargetExtentDelete(ctx, assoc.ID, true); err != nil {
					klog.Warningf("Failed to delete orphaned target-extent %d: %v", assoc.ID, err)
					errs = append(errs, fmt.Errorf("orphaned target-extent %d: %w", assoc.ID, err))
				}
			}
			if err := d.truenasClient.ISCSITargetDelete(ctx, target.ID, true); err != nil {
				klog.Warningf("Failed to delete orphaned target %d: %v", target.ID, err)
				errs = append(errs, fmt.Errorf("orphaned target %d: %w", target.ID, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("iSCSI cleanup errors for %s: %v", datasetName, errs)
	}

	klog.Infof("Deleted iSCSI resources for %s", datasetName)
	return nil
}

func (d *Driver) createNVMeoFShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool) error {
	if !d.config.NVMeoF.SubsystemAllowAnyHost && len(d.config.NVMeoF.SubsystemHosts) == 0 {
		return status.Error(codes.FailedPrecondition, "nvmeof.subsystemAllowAnyHost is false but nvmeof.subsystemHosts is empty — no host could connect; set allow-any-host or provide at least one host NQN")
	}

	var err error
	ds, err = d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Check if already configured
	existingProp := datasetUserProperty(ds, PropNVMeoFNamespaceID)
	if !freshlyCreated && existingProp != "" && existingProp != "-" {
		klog.Infof("NVMe-oF share already exists for %s", datasetName)
		return nil
	}

	// Generate NVMe-oF subsystem name (TrueNAS 25.10+ auto-generates NQN from name)
	subsysName := protocolShareName(path.Base(datasetName))
	if d.config.NVMeoF.NamePrefix != "" {
		subsysName = d.config.NVMeoF.NamePrefix + subsysName
	}
	if d.config.NVMeoF.NameSuffix != "" {
		subsysName += d.config.NVMeoF.NameSuffix
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

	var hostIDs []int
	if !d.config.NVMeoF.SubsystemAllowAnyHost {
		hostIDs, err = d.resolveNVMeoFHostIDs(ctx, d.config.NVMeoF.SubsystemHosts)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem hosts: %v", err)
		}
	}

	// Create subsystem (TrueNAS 25.10+: serial is auto-generated, hosts are IDs not NQNs).
	subsys, err := d.truenasClient.NVMeoFSubsystemCreate(
		ctx,
		subsysName,
		d.config.NVMeoF.SubsystemAllowAnyHost,
		hostIDs,
	)
	if err != nil && len(hostIDs) > 0 && isNVMeoFHostNotFoundError(err) {
		d.invalidateNVMeoFHostIDs(d.config.NVMeoF.SubsystemHosts)
		hostIDs, err = d.resolveNVMeoFHostIDs(ctx, d.config.NVMeoF.SubsystemHosts)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to re-resolve NVMe-oF subsystem hosts: %v", err)
		}
		subsys, err = d.truenasClient.NVMeoFSubsystemCreate(
			ctx,
			subsysName,
			d.config.NVMeoF.SubsystemAllowAnyHost,
			hostIDs,
		)
	}
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NVMe-oF subsystem: %v", err)
	}
	if err = d.reconcileNVMeoFHostAssociations(ctx, subsys.ID); err != nil {
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
		if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF subsystem after port failure: %v", delErr)
		}
		return status.Errorf(codes.Internal, "failed to get/create NVMe-oF port: %v", err)
	}

	// Associate subsystem with port (required for network accessibility)
	portSubsys, err := d.truenasClient.NVMeoFPortSubsysCreate(ctx, port.ID, subsys.ID)
	if err != nil {
		// Cleanup subsystem on association failure - volume would be unusable
		if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF subsystem after port association failure: %v", delErr)
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
		if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF subsystem: %v", delErr)
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
	}

	// Generate the expected NVMe-oF subsystem name (same logic as createNVMeoFShareForDataset)
	subsysName := protocolShareName(path.Base(datasetName))
	if d.config.NVMeoF.NamePrefix != "" {
		subsysName = d.config.NVMeoF.NamePrefix + subsysName
	}
	if d.config.NVMeoF.NameSuffix != "" {
		subsysName += d.config.NVMeoF.NameSuffix
	}
	devicePath := fmt.Sprintf("zvol/%s", datasetName)

	var nsDeleted, ssDeleted bool
	var errs []error
	var portSubsysAssocs []*truenas.NVMeoFPortSubsys
	var portSubsysAssocsErr error
	portSubsysAssocsFetched := false
	portSubsysAssocsFor := func(subsysID int) ([]*truenas.NVMeoFPortSubsys, error) {
		if !portSubsysAssocsFetched {
			portSubsysAssocs, portSubsysAssocsErr = d.truenasClient.NVMeoFPortSubsysList(ctx)
			portSubsysAssocsFetched = true
		}
		if portSubsysAssocsErr != nil {
			return nil, portSubsysAssocsErr
		}
		return truenas.NVMeoFPortSubsysFilterBySubsystem(portSubsysAssocs, subsysID), nil
	}

	// Step 1: Try to delete port-subsystem association by stored ID
	// (fallback cleanup is handled in subsystem deletion steps below)
	if psIDStr := datasetUserProperty(ds, PropNVMeoFPortSubsysID); psIDStr != "" && psIDStr != "-" {
		if psID, err := strconv.Atoi(psIDStr); err == nil {
			if err := d.truenasClient.NVMeoFPortSubsysDelete(ctx, psID); err != nil {
				klog.Warningf("Failed to delete NVMe-oF port-subsystem %d: %v", psID, err)
				// Don't add to errs - fallback will clean up via subsystem listing
			}
		}
	}

	// Step 2: Try to delete namespace by stored ID
	if nsIDStr := datasetUserProperty(ds, PropNVMeoFNamespaceID); nsIDStr != "" && nsIDStr != "-" {
		if nsID, err := strconv.Atoi(nsIDStr); err == nil {
			if err := d.truenasClient.NVMeoFNamespaceDelete(ctx, nsID); err != nil {
				klog.Warningf("Failed to delete NVMe-oF namespace %d: %v", nsID, err)
				errs = append(errs, fmt.Errorf("namespace %d: %w", nsID, err))
			} else {
				nsDeleted = true
			}
		}
	}

	// Step 2: Try to delete subsystem by stored ID (including port-subsys associations)
	if ssIDStr := datasetUserProperty(ds, PropNVMeoFSubsystemID); ssIDStr != "" && ssIDStr != "-" {
		if ssID, err := strconv.Atoi(ssIDStr); err == nil {
			// First delete any port-subsystem associations for this subsystem
			if assocs, err := portSubsysAssocsFor(ssID); err == nil {
				for _, assoc := range assocs {
					if err := d.truenasClient.NVMeoFPortSubsysDelete(ctx, assoc.ID); err != nil {
						klog.Warningf("Failed to delete port-subsystem association %d: %v", assoc.ID, err)
					}
				}
			}
			if err := d.truenasClient.NVMeoFSubsystemDelete(ctx, ssID); err != nil {
				klog.Warningf("Failed to delete NVMe-oF subsystem %d: %v", ssID, err)
				errs = append(errs, fmt.Errorf("subsystem %d: %w", ssID, err))
			} else {
				ssDeleted = true
			}
		}
	}

	// Step 3: Fallback - If namespace was not deleted by ID, try to find by device path
	// This handles cases where the dataset properties were never stored
	if !nsDeleted {
		if ns, err := d.truenasClient.NVMeoFNamespaceFindByDevicePath(ctx, devicePath); err == nil && ns != nil {
			klog.V(4).Infof("Found orphaned namespace by device path %s (ID %d), deleting", devicePath, ns.ID)
			if err := d.truenasClient.NVMeoFNamespaceDelete(ctx, ns.ID); err != nil {
				klog.Warningf("Failed to delete orphaned NVMe-oF namespace %d: %v", ns.ID, err)
				errs = append(errs, fmt.Errorf("orphaned namespace %d: %w", ns.ID, err))
			}
		}
	}

	// Step 4: Fallback - If subsystem was not deleted by ID, try to find by name
	// This handles cases where subsystem was created but property was never stored
	if !ssDeleted {
		if subsys, err := d.truenasClient.NVMeoFSubsystemFindByName(ctx, subsysName); err == nil && subsys != nil {
			klog.V(4).Infof("Found orphaned subsystem by name %s (ID %d), deleting", subsysName, subsys.ID)
			// First delete any port-subsystem associations for this subsystem
			if assocs, err := portSubsysAssocsFor(subsys.ID); err == nil {
				for _, assoc := range assocs {
					if err := d.truenasClient.NVMeoFPortSubsysDelete(ctx, assoc.ID); err != nil {
						klog.Warningf("Failed to delete orphaned port-subsystem association %d: %v", assoc.ID, err)
					}
				}
			}
			if err := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); err != nil {
				klog.Warningf("Failed to delete orphaned NVMe-oF subsystem %d: %v", subsys.ID, err)
				errs = append(errs, fmt.Errorf("orphaned subsystem %d: %w", subsys.ID, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("NVMe-oF cleanup errors for %s: %v", datasetName, errs)
	}

	klog.Infof("Deleted NVMe-oF resources for %s", datasetName)
	return nil
}
