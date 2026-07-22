package driver

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// iscsiShareBackend implements ShareBackend for iSCSI.
type iscsiShareBackend struct{ d *Driver }

func (b iscsiShareBackend) EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string) error {
	// The create path validates every cached ID against its target/extent
	// relationship before taking the idempotent fast path.
	return b.d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, false, false)
}

func (b iscsiShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	return b.d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady)
}

func (b iscsiShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteISCSIShareForDataset(ctx, ds, datasetName)
}

func (b iscsiShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string) error {
	return b.d.applyISCSIFence(ctx, ds, datasetName, enforceable)
}

func (b iscsiShareBackend) VolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error {
	return b.d.iscsiVolumeContext(ctx, ds, datasetName, volumeContext)
}

// iscsiVolumeContext resolves the iSCSI target and populates the publish
// context keys.
func (d *Driver) iscsiVolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error {
	target, err := d.resolveISCSITarget(ctx, ds, datasetName)
	if err != nil || target == nil {
		return status.Errorf(codes.Internal, "failed to resolve iSCSI target for %s: %v", datasetName, err)
	}
	globalCfg, err := d.truenasClient.ISCSIGlobalConfigGet(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get iSCSI global config: %v", err)
	}
	volumeContext["iqn"] = fmt.Sprintf("%s:%s", globalCfg.Basename, target.Name)
	volumeContext["portal"] = d.config.ISCSI.TargetPortal
	volumeContext["lun"] = "0"
	volumeContext["interface"] = d.config.ISCSI.Interface
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
		if len(targetGroups) == 0 && d.config.Fencing.Mode != FencingModeStrict {
			resolved, resolveErr := d.resolveISCSITargetGroup(ctx)
			if resolveErr != nil {
				return status.Errorf(codes.Internal, "cannot create iSCSI target for %s: %v", datasetName, resolveErr)
			}
			targetGroups = append(targetGroups, *resolved)
			usedResolvedGroup = true
		}
		if len(targetGroups) == 0 {
			// TrueNAS may reject a target with no portal groups. Strict mode starts
			// with a CSI-owned, non-nil-empty initiator allowlist attached to the
			// resolved portals: the target is valid but authorizes no initiator.
			dynamicGroup, groupErr := d.resolveFencingInitiatorGroup(ctx, ds, datasetName)
			if groupErr != nil {
				return status.Errorf(codes.Internal, "failed to resolve strict iSCSI initiator group: %v", groupErr)
			}
			if dynamicGroup == nil {
				dynamicGroup, groupErr = d.truenasClient.ISCSIInitiatorCreateWithInitiators(
					ctx, []string{}, "scale-csi fencing: "+datasetName,
				)
			}
			if groupErr != nil {
				return status.Errorf(codes.Internal, "failed to create strict iSCSI initiator group: %v", groupErr)
			}
			if propertyErr := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
				PropISCSIInitiatorID: strconv.Itoa(dynamicGroup.ID),
			}); propertyErr != nil {
				return status.Errorf(codes.Internal, "failed to store strict iSCSI initiator group: %v", propertyErr)
			}
			portals, portalErr := d.resolveISCSIPortalIDs(ctx)
			if portalErr != nil {
				return status.Errorf(codes.Internal, "failed to resolve strict iSCSI portal groups: %v", portalErr)
			}
			for _, portalID := range portals {
				targetGroups = append(targetGroups, truenas.ISCSITargetGroup{
					Portal: portalID, Initiator: dynamicGroup.ID, AuthMethod: "NONE",
				})
			}
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
