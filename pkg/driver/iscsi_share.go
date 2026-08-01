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

func (b iscsiShareBackend) EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, res *fenceResolution) error {
	// The create path validates every cached ID against its target/extent
	// relationship before taking the idempotent fast path.
	return b.d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, false, false, nil)
}

func (b iscsiShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	return b.d.createISCSIShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady, finalProperties)
}

func (b iscsiShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteISCSIShareForDataset(ctx, ds, datasetName)
}

func (b iscsiShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string, hasDeferredActiveISCSI bool, res *fenceResolution) error {
	return b.d.applyISCSIFence(ctx, ds, datasetName, enforceable, hasDeferredActiveISCSI, res)
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
	// Advertise the CHAP mode (never credentials) so the node knows to expect a
	// node-stage secret and which direction to configure. Prefer the request
	// resolution on create; otherwise read the immutable stored per-volume mode
	// (source==local) — never the mutable global iscsi.chap.mutual flag.
	if res := iscsiCHAPResolutionFromContext(ctx); res != nil && res.Peer != nil {
		volumeContext[volumeContextCHAPKey] = res.authMethod()
	} else if mode, _ := d.storedISCSICHAPPolicy(ds); mode != iscsiCHAPModeNone {
		volumeContext[volumeContextCHAPKey] = mode
	}
	return nil
}

// createISCSIShare creates iSCSI target, extent, and target-extent association.
// This function is idempotent and includes retry logic for robustness during
// high-load scenarios (e.g., volsync backup bursts).
func (d *Driver) createISCSIShare(ctx context.Context, datasetName, volumeName string) error {
	return d.createISCSIShareForDataset(ctx, nil, datasetName, volumeName, false, false, nil)
}

// finalProperties, when non-nil, is the caller's FATAL managed-property update
// (CreateVolume's). The share builder folds the extent-ID witness and the
// extent's ACTUAL geometry into it so both become durable-or-rolled-back with
// the rest of provisioning instead of depending on the warning-only resource-ID
// write below. That closes the "the witness can simply be lost" hole without a
// single extra round trip: the map is written either way.
func (d *Driver) createISCSIShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	start := time.Now()
	klog.Infof("createISCSIShare: starting for dataset %s", datasetName)
	var err error
	ds, err = d.datasetForProperties(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get dataset: %v", err)
	}

	// Resolve the CHAP posture for this dataset: the request-scoped resolution on
	// a fresh CreateVolume, else the stored (local) dataset properties on
	// idempotent rebuilds. authRef==0 means CHAP is off and groups stay
	// authmethod=NONE.
	chapMethod, chapAuthRef := d.iscsiGroupCHAP(ctx, ds)

	// Generate iSCSI name and disk path upfront
	iscsiName := d.iscsiShareName(path.Base(datasetName))
	diskPath := fmt.Sprintf("zvol/%s", datasetName)

	// Resolved per-volume block-protocol tuning (GF-Sprint 4), through the ONE
	// resolver: request-scoped StorageClass opts (CreateVolume only) -> the
	// volume's STORED dataset properties -> the controller default. This function
	// is reached by four callers and only ONE of them (a fresh/replayed
	// CreateVolume) carries request opts; ControllerPublishVolume, the startup
	// attachment reconcile and DR/restore rebuilds do not. Reading the stored
	// properties is what stops those paths from re-creating an absent extent at
	// the 512 default over 4096-geometry data, and from dropping the stable
	// serial / read-only / insecure_tpc / auth_networks / avail_threshold /
	// qid_max / pi_enable the volume was provisioned with.
	//
	// Nil on both sides means nothing was ever opted into: byte-identical to
	// pre-GF4.
	requestOpts := blockOptsFromContext(ctx)
	storedOpts := blockOptsFromDataset(ds)
	// R-1, absent-extent half: a genuine StorageClass geometry change must fail
	// closed BEFORE the extent is (re-)created, because on a rebuild there is no
	// live extent left to compare against.
	if guardErr := guardStoredBlockGeometry(storedOpts, requestOpts, datasetName); guardErr != nil {
		return guardErr
	}
	// Same fail-closed treatment for the eight non-geometry knobs on the
	// absent-object rebuild path (codex gate #1). Per-volume block tuning is
	// immutable; a rebuild that re-creates the objects must not quietly adopt a
	// value the volume was never provisioned with.
	if guardErr := guardStoredBlockTuning(storedOpts, requestOpts, datasetName); guardErr != nil {
		return guardErr
	}
	opts := mergeBlockOpts(requestOpts, storedOpts)

	// Step 2: Find or create target (idempotent)
	var target *truenas.ISCSITarget
	var targetID int

	if !freshlyCreated {
		target, err = d.resolveISCSITarget(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve iSCSI target: %v", err)
		}
		if target != nil {
			// codex gate #1, target half: queuedCommands / authNetworks are
			// create-time-only for this driver, so a replay that resolves a value
			// the LIVE target does not already carry must fail closed instead of
			// returning success over an unchanged backend. The comparison prefers
			// the live object and falls back to the volume's stamp only for a field
			// TrueNAS does not report, so a same-value replay is never rejected.
			if guardErr := guardExistingISCSITargetOpts(target, requestOpts, storedOpts, datasetName); guardErr != nil {
				return guardErr
			}
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
			group := truenas.ISCSITargetGroup{
				Portal:     tg.Portal,
				Initiator:  tg.Initiator,
				AuthMethod: tg.AuthMethod,
				Auth:       auth,
			}
			applyISCSIGroupCHAP(&group, chapMethod, chapAuthRef)
			targetGroups = append(targetGroups, group)
		}
		if len(targetGroups) == 0 && d.config.Fencing.Mode != FencingModeStrict {
			resolved, resolveErr := d.resolveISCSITargetGroup(ctx)
			if resolveErr != nil {
				return status.Errorf(codes.Internal, "cannot create iSCSI target for %s: %v", datasetName, resolveErr)
			}
			applyISCSIGroupCHAP(resolved, chapMethod, chapAuthRef)
			targetGroups = append(targetGroups, *resolved)
			usedResolvedGroup = true
		}
		if len(targetGroups) == 0 {
			// TrueNAS may reject a target with no portal groups. Strict mode starts
			// with a CSI-owned deny-all initiator group attached to the resolved
			// portals: the target is valid but authorizes no initiator. The group
			// carries the non-matchable sentinel because an EMPTY allowlist renders
			// allow-all (INITIATOR *) on TrueNAS 26.0 SCST, not deny-all.
			dynamicGroup, groupErr := d.resolveFencingInitiatorGroup(ctx, ds, datasetName)
			if groupErr != nil {
				return status.Errorf(codes.Internal, "failed to resolve strict iSCSI initiator group: %v", groupErr)
			}
			if dynamicGroup == nil {
				dynamicGroup, groupErr = d.truenasClient.ISCSIInitiatorCreateWithInitiators(
					ctx, iscsiDenyAllInitiators(), "scale-csi fencing: "+datasetName,
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
				group := truenas.ISCSITargetGroup{
					Portal: portalID, Initiator: dynamicGroup.ID, AuthMethod: "NONE",
				}
				applyISCSIGroupCHAP(&group, chapMethod, chapAuthRef)
				targetGroups = append(targetGroups, group)
			}
		}

		target, err = d.truenasClient.ISCSITargetCreate(ctx, iscsiName, "", "ISCSI", targetGroups, opts.iscsiTargetCreateOpts())
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
			// Precedence, and the one case the driver refuses to decide: the LIVE
			// extent says what the data is addressed through, the STAMP says what the
			// volume was provisioned with, and a disagreement between them is real
			// drift (an out-of-band edit, or a geometry laundered onto this volume by
			// an earlier defaulted rebuild). Surfacing it beats silently preferring
			// either side, which is what let a wrong geometry certify itself to every
			// downstream guard.
			if guardErr := guardStampedVsLiveGeometry(storedOpts, extent, datasetName); guardErr != nil {
				return guardErr
			}
			// R-1, existing-extent half: blocksize is immutable once an extent
			// holds data. Reject a request whose resolved blocksize diverges from
			// the existing extent rather than silently keeping a geometry that
			// desyncs the StorageClass contract from the backend.
			//
			// The comparison uses the OPINION (per-SC override, else the stored
			// stamp), never the controller default. Defaulting here is what made a
			// no-opts publish of a 4096 volume compare 4096 vs 512 and return
			// FailedPrecondition forever.
			if guardErr := guardISCSIBlocksizeImmutability(extent, opts.requestedISCSIBlocksize(), datasetName); guardErr != nil {
				return guardErr
			}
			// codex gate #1, extent half: pblocksize / insecureTpc / readOnly /
			// availThreshold / stableSerial are create-time-only for this driver.
			// Silently returning success while the extent stays permissive
			// (insecure_tpc) or writable (ro) was a safety-contract violation, not
			// just a docs gap.
			if guardErr := guardExistingISCSIExtentOpts(extent, requestOpts, storedOpts, datasetName); guardErr != nil {
				return guardErr
			}
			extentID = extent.ID
			klog.V(4).Infof("Resolved existing extent by disk path %s (ID %d)", diskPath, extentID)
		}
	}

	// Create extent with retry logic
	if extent == nil {
		comment := fmt.Sprintf("truenas-csi: %s", datasetName)
		var lastErr error

		// THE GEOMETRY CHOKE POINT. One record, logical AND physical resolved
		// together by one function from evidence about the bytes that already
		// exist — never from a StorageClass parameter or a helm default over
		// storage that may hold data. There is no live extent here to read (that is
		// why we are creating one). See "the geometry invariant" in block_opts.go.
		geometry, geometryErr := d.resolveExtentGeometry(ctx, requestOpts, storedOpts, ds, datasetName, freshlyCreated)
		if geometryErr != nil {
			return geometryErr
		}
		klog.V(4).Infof("Resolved extent geometry for %s: blocksize=%d pblocksize=%t (%s)",
			datasetName, geometry.blocksize, geometry.pblocksize, geometry.provenance)

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
				geometry.blocksize,
				geometry.pblocksize,
				d.config.ISCSI.ExtentRpm,
				opts.iscsiExtentCreateOpts(),
			)
			if createErr == nil {
				// ISCSIExtentCreate itself falls back to find-by-name on an ambiguous
				// "already exists"/"invalid params", so even a nil error can hand back
				// an object this call did not create. Validate before adopting it: an
				// unvalidated adoption is what let a stale or concurrently-created
				// extent at a different geometry become this volume's back-stamped truth.
				if mismatch := validateExtentAgainstGeometry(extent, geometry, datasetName); mismatch != nil {
					return mismatch
				}
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
					// Same rule for the idempotency arm. The object is NOT deleted on a
					// mismatch: the driver did not create it and must not destroy an
					// extent another controller may be mid-flight on. Refuse and say so.
					if mismatch := validateExtentAgainstGeometry(e, geometry, datasetName); mismatch != nil {
						return mismatch
					}
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
	resourceProps := map[string]string{
		PropISCSITargetID:       strconv.Itoa(targetID),
		PropISCSIExtentID:       strconv.Itoa(extentID),
		PropISCSITargetExtentID: strconv.Itoa(targetExtent.ID),
	}
	// Mechanism (1) of the geometry invariant: record the geometry of the extent
	// we just created, or back-stamp the one we just resolved, onto a dataset that
	// does not already carry it. Folded into a dataset update that happens anyway,
	// so it costs ZERO extra round trips and adds nothing to the hot path.
	//
	// This is what makes the rest of the invariant safe: it stamps volumes at
	// create (including the default path — the geometry a volume HAS is not the
	// same fact as what its StorageClass asked for), and it stamps the entire
	// pre-GF4 fleet on its first publish / reconcile / replay. Once a volume is
	// stamped, no later rebuild has to consult the controller default, and no
	// later change to the helm value can reach its data.
	for key, value := range observedGeometryProps(ds, extent) {
		resourceProps[key] = value
	}
	// ...and, when the caller has a FATAL property update of its own (CreateVolume
	// does), the SAME keys ride in it. The write below is warning-only, which is
	// exactly how a volume could end up with data, no extent-ID witness and no
	// geometry stamp — the state in which a later rebuild has nothing to resolve
	// from. Folding into a map the caller writes anyway costs zero round trips and
	// makes the witness and the geometry durable-or-rolled-back with provisioning.
	if finalProperties != nil {
		for key, value := range resourceProps {
			finalProperties[key] = value
		}
	}
	// NOTE: the CHAP auth linkage (PropISCSIAuthTag + PropISCSIAuthMode) is NOT
	// written here. It is a security control that a fence pass depends on, so it
	// must be durable-or-fail: a warn-only write could return CreateVolume success
	// with a chap volumeContext but no stored linkage, and the next fence would
	// rebuild the target as authmethod=NONE (a strict-mode auth downgrade). The
	// linkage is instead folded into CreateVolume's FATAL managed-property update
	// (controller.go), which rolls back the share+dataset on failure. The group
	// authmethod+auth are still applied to the live target groups above.
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, resourceProps); err != nil {
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
