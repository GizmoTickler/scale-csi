package driver

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	// defaultShareRetryAttempts is the number of times to retry share creation
	defaultShareRetryAttempts = 3
	// defaultShareRetryDelay is the initial delay between retry attempts
	defaultShareRetryDelay = 2 * time.Second
)

// ensureShareExists checks if a share exists for the dataset and creates it if missing.
// This is critical for idempotency when a volume was created but share creation failed.
func (d *Driver) ensureShareExists(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeName string, shareType ShareType) error {
	switch shareType {
	case ShareTypeNFS:
		// Check if NFS share ID is stored
		if prop, ok := ds.UserProperties[PropNFSShareID]; ok && prop.Value != "" && prop.Value != "-" {
			klog.V(4).Infof("NFS share already exists for %s (ID: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("NFS share missing for existing volume %s, creating...", datasetName)
		return d.createNFSShare(ctx, datasetName, volumeName)

	case ShareTypeISCSI:
		// Check if iSCSI target-extent association exists (this means full setup is complete)
		if prop, ok := ds.UserProperties[PropISCSITargetExtentID]; ok && prop.Value != "" && prop.Value != "-" {
			klog.V(4).Infof("iSCSI share already exists for %s (targetextent: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("iSCSI share missing for existing volume %s, creating...", datasetName)
		return d.createISCSIShare(ctx, datasetName, volumeName)

	case ShareTypeNVMeoF:
		// Check if NVMe-oF namespace ID is stored
		if prop, ok := ds.UserProperties[PropNVMeoFNamespaceID]; ok && prop.Value != "" && prop.Value != "-" {
			klog.V(4).Infof("NVMe-oF share already exists for %s (namespace: %s)", datasetName, prop.Value)
			return nil
		}
		klog.Infof("NVMe-oF share missing for existing volume %s, creating...", datasetName)
		return d.createNVMeoFShare(ctx, datasetName, volumeName)

	default:
		return nil
	}
}

// createShareWithOptions creates a share with additional options.
// shareType should be obtained from config.GetShareType(params) to support StorageClass parameters.
// skipZvolWait skips the WaitForZvolReady call in iSCSI/NVMe-oF share creation (used after cloning).
func (d *Driver) createShareWithOptions(ctx context.Context, datasetName string, volumeName string, shareType ShareType, skipZvolWait bool) error {
	klog.Infof("Creating %s share for dataset: %s (skipZvolWait=%v)", shareType, datasetName, skipZvolWait)

	switch shareType {
	case ShareTypeNFS:
		return d.createNFSShare(ctx, datasetName, volumeName)
	case ShareTypeISCSI:
		return d.createISCSIShareWithOptions(ctx, datasetName, volumeName, skipZvolWait)
	case ShareTypeNVMeoF:
		return d.createNVMeoFShareWithOptions(ctx, datasetName, volumeName, skipZvolWait)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported share type: %s", shareType)
	}
}

// deleteShare deletes the share for a dataset.
// shareType should be obtained from config.GetShareType(params) or stored metadata.
func (d *Driver) deleteShare(ctx context.Context, datasetName string, shareType ShareType) error {
	klog.Infof("Deleting %s share for dataset: %s", shareType, datasetName)

	switch shareType {
	case ShareTypeNFS:
		return d.deleteNFSShare(ctx, datasetName)
	case ShareTypeISCSI:
		return d.deleteISCSIShare(ctx, datasetName)
	case ShareTypeNVMeoF:
		return d.deleteNVMeoFShare(ctx, datasetName)
	default:
		return nil
	}
}

// createNFSShare creates an NFS share for a dataset.
func (d *Driver) createNFSShare(ctx context.Context, datasetName string, volumeName string) error {
	// Get dataset to find mountpoint
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Check if share already exists
	existingProp, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropNFSShareID)
	if existingProp != "" && existingProp != "-" {
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
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropNFSShareID, strconv.Itoa(share.ID)); err != nil {
		return status.Errorf(codes.Internal, "failed to store NFS share ID: %v", err)
	}

	klog.Infof("Created NFS share ID %d for %s", share.ID, datasetName)
	return nil
}

// deleteNFSShare deletes the NFS share for a dataset.
func (d *Driver) deleteNFSShare(ctx context.Context, datasetName string) error {
	// Get share ID from dataset property
	shareIDStr, err := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropNFSShareID)
	if err != nil || shareIDStr == "" || shareIDStr == "-" {
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
func (d *Driver) createISCSIShare(ctx context.Context, datasetName string, volumeName string) error {
	return d.createISCSIShareWithOptions(ctx, datasetName, volumeName, false)
}

// createISCSIShareWithOptions creates iSCSI share with additional options.
// skipZvolWait skips the WaitForZvolReady call (used when zvol is freshly cloned).
func (d *Driver) createISCSIShareWithOptions(ctx context.Context, datasetName string, volumeName string, skipZvolWait bool) error {
	start := time.Now()
	klog.Infof("createISCSIShare: starting for dataset %s", datasetName)

	// Generate iSCSI name and disk path upfront
	iscsiName := path.Base(datasetName)
	if d.config.ISCSI.NameSuffix != "" {
		iscsiName = iscsiName + d.config.ISCSI.NameSuffix
	}
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	// Step 1: Check if already fully configured (idempotency fast-path)
	existingTE, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSITargetExtentID)
	if existingTE != "" && existingTE != "-" {
		// Verify the target-extent still exists by looking it up by ID
		teID, err := strconv.Atoi(existingTE)
		if err == nil {
			if te, err := d.truenasClient.ISCSITargetExtentGet(ctx, teID); err == nil && te != nil {
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
	existingTargetID, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSITargetID)
	if existingTargetID != "" && existingTargetID != "-" {
		if id, err := strconv.Atoi(existingTargetID); err == nil {
			if t, err := d.truenasClient.ISCSITargetGet(ctx, id); err == nil {
				target = t
				targetID = t.ID
				klog.V(4).Infof("Using existing target ID %d for %s", targetID, datasetName)
			}
		}
	}

	// If no stored target, check by name
	if target == nil {
		if t, err := d.truenasClient.ISCSITargetFindByName(ctx, iscsiName); err == nil && t != nil {
			target = t
			targetID = t.ID
			klog.V(4).Infof("Found existing target by name %s (ID %d)", iscsiName, targetID)
		}
	}

	// Create target if needed
	if target == nil {
		targetGroups := []truenas.ISCSITargetGroup{}
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

		var err error
		target, err = d.truenasClient.ISCSITargetCreate(ctx, iscsiName, "", "ISCSI", targetGroups)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to create iSCSI target: %v", err)
		}
		targetID = target.ID
		klog.Infof("Created iSCSI target %s (ID %d)", iscsiName, targetID)
	}

	// Store target ID
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetID, strconv.Itoa(targetID)); err != nil {
		klog.Warningf("Failed to store iSCSI target ID: %v", err)
	}

	// Step 3: Wait for zvol to be ready before creating extent
	// This is critical for cloned volumes which may not be immediately available
	// Skip if caller already verified zvol readiness (e.g., after cloning)
	if !skipZvolWait {
		zvolTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
		klog.V(4).Infof("Waiting for zvol %s to be ready before creating extent (timeout: %v)", datasetName, zvolTimeout)
		if _, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, zvolTimeout); err != nil {
			klog.Warningf("Zvol readiness check failed (will attempt extent creation anyway): %v", err)
		}
	} else {
		klog.V(4).Infof("Skipping zvol wait for %s (already verified ready)", datasetName)
	}

	// Step 4: Find or create extent with retry (idempotent)
	var extent *truenas.ISCSIExtent
	var extentID int

	// Check if we have a stored extent ID
	existingExtentID, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSIExtentID)
	if existingExtentID != "" && existingExtentID != "-" {
		if id, err := strconv.Atoi(existingExtentID); err == nil {
			if e, err := d.truenasClient.ISCSIExtentGet(ctx, id); err == nil {
				extent = e
				extentID = e.ID
				klog.V(4).Infof("Using existing extent ID %d for %s", extentID, datasetName)
			}
		}
	}

	// If no stored extent, check by disk path (more reliable than name for clones)
	if extent == nil {
		if e, err := d.truenasClient.ISCSIExtentFindByDisk(ctx, diskPath); err == nil && e != nil {
			extent = e
			extentID = e.ID
			klog.V(4).Infof("Found existing extent by disk path %s (ID %d)", diskPath, extentID)
		}
	}

	// If still no extent, check by name
	if extent == nil {
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
					return status.Errorf(codes.DeadlineExceeded, "context cancelled during extent creation retry")
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

			// Check if extent was actually created despite error
			if e, findErr := d.truenasClient.ISCSIExtentFindByDisk(ctx, diskPath); findErr == nil && e != nil {
				extent = e
				extentID = e.ID
				klog.Infof("Extent found after error (ID %d), continuing", extentID)
				break
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

	// Store extent ID
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropISCSIExtentID, strconv.Itoa(extentID)); err != nil {
		klog.Warningf("Failed to store iSCSI extent ID: %v", err)
	}

	// Step 5: Find or create target-extent association (idempotent)
	var targetExtent *truenas.ISCSITargetExtent

	// Check if association already exists
	if te, err := d.truenasClient.ISCSITargetExtentFind(ctx, targetID, extentID); err == nil && te != nil {
		targetExtent = te
		klog.V(4).Infof("Using existing target-extent association (ID %d)", te.ID)
	}

	// Create association if needed
	if targetExtent == nil {
		var err error
		targetExtent, err = d.truenasClient.ISCSITargetExtentCreate(ctx, targetID, extentID, 0)
		if err != nil {
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

	// Store target-extent ID
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropISCSITargetExtentID, strconv.Itoa(targetExtent.ID)); err != nil {
		klog.Warningf("Failed to store iSCSI target-extent ID: %v", err)
	}

	// Request iSCSI service reload using debouncer to prevent reload storms
	// during bulk volume provisioning. Multiple requests within the debounce
	// window will be coalesced into a single reload operation.
	klog.V(4).Infof("Requesting debounced iSCSI service reload to ensure target is discoverable")
	if err := d.serviceReloadDebouncer.RequestReload(ctx, "iscsitarget"); err != nil {
		// Non-fatal: the service might auto-reload, and node has retry logic
		klog.V(4).Infof("iSCSI service reload returned (may be normal): %v", err)
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
	// Generate the expected iSCSI name (same logic as createISCSIShare)
	iscsiName := path.Base(datasetName)
	if d.config.ISCSI.NameSuffix != "" {
		iscsiName = iscsiName + d.config.ISCSI.NameSuffix
	}
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	var extDeleted, tgtDeleted bool
	var errs []error

	// Try to delete target-extent association by stored ID
	if teIDStr, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSITargetExtentID); teIDStr != "" && teIDStr != "-" {
		if teID, err := strconv.Atoi(teIDStr); err == nil {
			if err := d.truenasClient.ISCSITargetExtentDelete(ctx, teID, true); err != nil {
				klog.Warningf("Failed to delete iSCSI target-extent %d: %v", teID, err)
				errs = append(errs, fmt.Errorf("target-extent %d: %w", teID, err))
			}
		}
	}

	// Try to delete extent by stored ID
	if extIDStr, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSIExtentID); extIDStr != "" && extIDStr != "-" {
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
	if tgtIDStr, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropISCSITargetID); tgtIDStr != "" && tgtIDStr != "-" {
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
			if assocs, err := d.truenasClient.ISCSITargetExtentFindByExtent(ctx, extent.ID); err == nil {
				for _, assoc := range assocs {
					if err := d.truenasClient.ISCSITargetExtentDelete(ctx, assoc.ID, true); err != nil {
						klog.Warningf("Failed to delete orphaned target-extent %d: %v", assoc.ID, err)
					}
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
			if assocs, err := d.truenasClient.ISCSITargetExtentFindByTarget(ctx, target.ID); err == nil {
				for _, assoc := range assocs {
					if err := d.truenasClient.ISCSITargetExtentDelete(ctx, assoc.ID, true); err != nil {
						klog.Warningf("Failed to delete orphaned target-extent %d: %v", assoc.ID, err)
					}
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

// createNVMeoFShare creates NVMe-oF subsystem and namespace.
func (d *Driver) createNVMeoFShare(ctx context.Context, datasetName string, volumeName string) error {
	return d.createNVMeoFShareWithOptions(ctx, datasetName, volumeName, false)
}

// createNVMeoFShareWithOptions creates NVMe-oF share with additional options.
// skipZvolWait skips the WaitForZvolReady call (used when zvol is freshly cloned).
func (d *Driver) createNVMeoFShareWithOptions(ctx context.Context, datasetName string, volumeName string, skipZvolWait bool) error {
	// Check if already configured
	existingProp, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropNVMeoFNamespaceID)
	if existingProp != "" && existingProp != "-" {
		klog.Infof("NVMe-oF share already exists for %s", datasetName)
		return nil
	}

	// Generate NVMe-oF subsystem name (TrueNAS 25.10+ auto-generates NQN from name)
	subsysName := path.Base(datasetName)
	if d.config.NVMeoF.NamePrefix != "" {
		subsysName = d.config.NVMeoF.NamePrefix + subsysName
	}
	if d.config.NVMeoF.NameSuffix != "" {
		subsysName = subsysName + d.config.NVMeoF.NameSuffix
	}

	// Wait for zvol to be ready before creating subsystem/namespace
	// This is critical for cloned volumes which may not be immediately available
	// Skip if caller already verified zvol readiness (e.g., after cloning)
	if !skipZvolWait {
		zvolTimeout := time.Duration(d.config.ZFS.ZvolReadyTimeout) * time.Second
		klog.V(4).Infof("Waiting for zvol %s to be ready before creating NVMe-oF share (timeout: %v)", datasetName, zvolTimeout)
		if _, err := d.truenasClient.WaitForZvolReady(ctx, datasetName, zvolTimeout); err != nil {
			klog.Warningf("Zvol readiness check failed (will attempt share creation anyway): %v", err)
		}
	} else {
		klog.V(4).Infof("Skipping zvol wait for %s (already verified ready)", datasetName)
	}

	// Create subsystem (TrueNAS 25.10+: serial is auto-generated, hosts are IDs not NQNs)
	// Note: SubsystemHosts config is ignored in 25.10+ - use allow_any_host or configure hosts separately
	subsys, err := d.truenasClient.NVMeoFSubsystemCreate(
		ctx,
		subsysName,
		d.config.NVMeoF.SubsystemAllowAnyHost,
		nil, // Host IDs - not used when allow_any_host is true
	)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NVMe-oF subsystem: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropNVMeoFSubsystemID, strconv.Itoa(subsys.ID)); err != nil {
		return status.Errorf(codes.Internal, "failed to store NVMe-oF subsystem ID: %v", err)
	}

	// Create namespace (TrueNAS 25.10+: device_path format is "zvol/pool/vol", device_type is required)
	devicePath := fmt.Sprintf("zvol/%s", datasetName)
	namespace, err := d.truenasClient.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")
	if err != nil {
		if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
			klog.Warningf("Failed to cleanup NVMe-oF subsystem: %v", delErr)
		}
		return status.Errorf(codes.Internal, "failed to create NVMe-oF namespace: %v", err)
	}
	if err := d.truenasClient.DatasetSetUserProperty(ctx, datasetName, PropNVMeoFNamespaceID, strconv.Itoa(namespace.ID)); err != nil {
		return status.Errorf(codes.Internal, "failed to store NVMe-oF namespace ID: %v", err)
	}

	// Get or create the NVMe-oF TCP port and associate subsystem with it
	// TrueNAS 25.10+: Subsystems must be associated with a port to be accessible over the network
	port, err := d.truenasClient.NVMeoFGetOrCreatePort(
		ctx,
		d.config.NVMeoF.Transport,
		d.config.NVMeoF.TransportAddress,
		d.config.NVMeoF.TransportServiceID,
	)
	if err != nil {
		klog.Warningf("Failed to get/create NVMe-oF port (subsystem may not be accessible): %v", err)
	} else {
		// Associate subsystem with port
		if err := d.truenasClient.NVMeoFPortSubsysCreate(ctx, port.ID, subsys.ID); err != nil {
			klog.Warningf("Failed to associate subsystem with port (subsystem may not be accessible): %v", err)
		} else {
			klog.V(4).Infof("Associated NVMe-oF subsystem %d with port %d", subsys.ID, port.ID)
		}
	}

	klog.Infof("Created NVMe-oF subsystem=%d, namespace=%d for %s", subsys.ID, namespace.ID, datasetName)
	return nil
}

// deleteNVMeoFShare deletes NVMe-oF resources for a dataset.
// It tries to delete by stored property IDs first, then falls back to lookup by name/path
// to handle cases where properties were never stored (e.g., failed volume creation).
// Returns an error if any cleanup fails so the caller can retry.
func (d *Driver) deleteNVMeoFShare(ctx context.Context, datasetName string) error {
	// Generate the expected NVMe-oF subsystem name (same logic as createNVMeoFShare)
	subsysName := path.Base(datasetName)
	if d.config.NVMeoF.NamePrefix != "" {
		subsysName = d.config.NVMeoF.NamePrefix + subsysName
	}
	if d.config.NVMeoF.NameSuffix != "" {
		subsysName = subsysName + d.config.NVMeoF.NameSuffix
	}
	devicePath := fmt.Sprintf("zvol/%s", datasetName)

	var nsDeleted, ssDeleted bool
	var errs []error

	// Step 1: Try to delete namespace by stored ID
	if nsIDStr, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropNVMeoFNamespaceID); nsIDStr != "" && nsIDStr != "-" {
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
	if ssIDStr, _ := d.truenasClient.DatasetGetUserProperty(ctx, datasetName, PropNVMeoFSubsystemID); ssIDStr != "" && ssIDStr != "-" {
		if ssID, err := strconv.Atoi(ssIDStr); err == nil {
			// First delete any port-subsystem associations for this subsystem
			if assocs, err := d.truenasClient.NVMeoFPortSubsysListBySubsystem(ctx, ssID); err == nil {
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
			if assocs, err := d.truenasClient.NVMeoFPortSubsysListBySubsystem(ctx, subsys.ID); err == nil {
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
