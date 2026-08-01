package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// nvmeoFShareBackend implements ShareBackend for NVMe-oF.
type nvmeoFShareBackend struct{ d *Driver }

func (b nvmeoFShareBackend) EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, res *fenceResolution) error {
	// The create path validates the namespace's device backreference and
	// repairs cached IDs before returning an idempotent success.
	return b.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, false, false, res)
}

func (b nvmeoFShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	return b.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady, nil)
}

func (b nvmeoFShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteNVMeoFShareForDataset(ctx, ds, datasetName)
}

func (b nvmeoFShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string, hasDeferredActiveISCSI bool, res *fenceResolution) error {
	return b.d.applyNVMeFence(ctx, ds, datasetName, enforceable, removing, ownedNVMeNQNs, uniqueSortedStrings(protectedNVMeNQNs), res)
}

func (b nvmeoFShareBackend) VolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error {
	return b.d.nvmeofVolumeContext(ctx, ds, datasetName, volumeContext)
}

// nvmeofVolumeContext resolves the NVMe-oF namespace/subsystem and populates
// the publish context keys.
func (d *Driver) nvmeofVolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error {
	namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to resolve NVMe-oF namespace: %v", err)
	}
	subsys, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
	if err != nil || subsys == nil {
		return status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem for %s: %v", datasetName, err)
	}
	if namespace == nil || namespace.SubsystemID != subsys.ID {
		return status.Errorf(codes.Internal, "NVMe-oF namespace for %s is missing or references a different subsystem", datasetName)
	}
	volumeContext["nqn"] = subsys.NQN
	volumeContext["transport"] = d.config.NVMeoF.Transport
	volumeContext["address"] = d.config.NVMeoF.TransportAddress
	volumeContext["port"] = strconv.Itoa(d.config.NVMeoF.TransportServiceID)
	// E-6 multipath: advertise every storage address so a multipath-aware node can
	// connect each to the same NQN. The single "address" key above is retained for
	// back-compat with nodes that connect one path. Emitted only when multipath is
	// enabled so the default publish context is unchanged.
	if addresses := d.config.NVMeoF.multipathAddresses(); len(addresses) > 0 {
		encoded, err := json.Marshal(addresses)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to encode NVMe-oF multipath addresses: %v", err)
		}
		volumeContext["addresses"] = string(encoded)
	}
	return nil
}

// nvmeofPortCreateOpts builds the install-wide NVMe-oF port performance options
// (E-4) from config. An all-nil config yields a zero options value, which omits
// every field and reproduces the historical port create exactly.
func (d *Driver) nvmeofPortCreateOpts() truenas.NVMeoFPortCreateOptions {
	return truenas.NVMeoFPortCreateOptions{
		InlineDataSize: d.config.NVMeoF.PortPerf.InlineDataSize,
		MaxQueueSize:   d.config.NVMeoF.PortPerf.MaxQueueSize,
		PiEnable:       d.config.NVMeoF.PortPerf.PiEnable,
	}
}

// associateNVMeoFPorts ensures the subsystem is reachable on one port per
// address and returns the association IDs in address order. It is a no-op
// returning (nil, nil) for an empty address list, which is what makes the
// multipath convergence call on the already-exists path cost zero API round
// trips when multipath is disabled.
//
// Both the port get-or-create and the association create are already-exists
// tolerant, so calling this repeatedly for the same subsystem converges rather
// than duplicating objects.
func (d *Driver) associateNVMeoFPorts(ctx context.Context, subsysID int, addresses []string) ([]int, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	portOpts := d.nvmeofPortCreateOpts()
	portSubsysIDs := make([]int, 0, len(addresses))
	for _, addr := range addresses {
		port, portErr := d.truenasClient.NVMeoFGetOrCreatePort(
			ctx,
			d.config.NVMeoF.Transport,
			addr,
			d.config.NVMeoF.TransportServiceID,
			portOpts,
		)
		if portErr != nil {
			return portSubsysIDs, fmt.Errorf("failed to get/create NVMe-oF port for %s: %w", addr, portErr)
		}
		assoc, assocErr := d.truenasClient.NVMeoFPortSubsysCreate(ctx, port.ID, subsysID)
		if assocErr != nil {
			d.truenasClient.InvalidateNVMeoFPort(
				d.config.NVMeoF.Transport,
				addr,
				d.config.NVMeoF.TransportServiceID,
			)
			return portSubsysIDs, fmt.Errorf("failed to associate subsystem with port %s: %w", addr, assocErr)
		}
		portSubsysIDs = append(portSubsysIDs, assoc.ID)
		klog.V(4).Infof("Associated NVMe-oF subsystem %d with port %d (association ID %d)", subsysID, port.ID, assoc.ID)
	}
	return portSubsysIDs, nil
}

func (d *Driver) createNVMeoFShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, res *fenceResolution) error {
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
	// Resolved per-volume block-protocol tuning (GF-Sprint 4) through the ONE
	// resolver: request-scoped StorageClass opts (CreateVolume only) -> the
	// volume's STORED dataset properties -> the controller default. A rebuild
	// reached from ControllerPublishVolume / the startup reconcile carries no
	// request opts, so without the stored half a re-created subsystem silently
	// dropped qid_max and pi_enable (disabling T10-PI under a connected
	// initiator).
	requestOpts := blockOptsFromContext(ctx)
	storedOpts := blockOptsFromDataset(ds)
	// codex gate #1, absent-object half: qidMax / piEnable are create-time-only
	// for this driver, so a rebuild that re-creates the subsystem must not adopt a
	// value the volume was never provisioned with.
	if guardErr := guardStoredBlockTuning(storedOpts, requestOpts, datasetName); guardErr != nil {
		return guardErr
	}
	opts := mergeBlockOpts(requestOpts, storedOpts)
	var subsys *truenas.NVMeoFSubsystem
	if !freshlyCreated {
		namespace, resolvedSubsys, resolveErr := d.resolvedNVMeObjects(ctx, res, ds, datasetName)
		if resolveErr != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF namespace/subsystem: %v", resolveErr)
		}
		subsys = resolvedSubsys
		if namespace != nil {
			if subsys == nil || namespace.SubsystemID != subsys.ID {
				return status.Errorf(codes.Internal, "NVMe-oF namespace %d for %s has no matching subsystem", namespace.ID, datasetName)
			}
			// codex gate #1, live-object half: this fast path used to return
			// success BEFORE the requested subsystem options were even looked at,
			// so a changed qid_max or pi_enable (a data-integrity control) was
			// always ignored. Fail closed instead.
			if guardErr := guardExistingNVMeoFSubsystemOpts(subsys, requestOpts, storedOpts, datasetName); guardErr != nil {
				return guardErr
			}
			// The repair-stamp write heals missing/stale cached object IDs. When the
			// dataset already carries the resolved IDs it is a no-op, so re-issuing
			// it on every publish is a wasted pool.dataset.update — skip it. The
			// write still runs (with the same values) whenever the props are absent
			// or diverge, so the self-healing contract is unchanged.
			if datasetUserProperty(ds, PropNVMeoFSubsystemID) != strconv.Itoa(subsys.ID) ||
				datasetUserProperty(ds, PropNVMeoFNamespaceID) != strconv.Itoa(namespace.ID) {
				if propertyErr := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{
					PropNVMeoFSubsystemID: strconv.Itoa(subsys.ID),
					PropNVMeoFNamespaceID: strconv.Itoa(namespace.ID),
				}); propertyErr != nil {
					return status.Errorf(codes.Internal, "failed to repair NVMe-oF object IDs: %v", propertyErr)
				}
			}
			// Fenced allowlists are changed only after ControllerPublishVolume has
			// durably stored the requested node identity. CreateVolume retries and
			// ensure-share checks must not transiently clear a strict subsystem.
			if !d.config.Fencing.Enabled() {
				if reconcileErr := d.reconcileNVMeoFHostAssociations(ctx, subsys.ID); reconcileErr != nil {
					return status.Errorf(codes.Internal, "failed to reconcile NVMe-oF subsystem hosts: %v", reconcileErr)
				}
			}
			// E-6 convergence (F-4): this early return used to skip the port
			// association loop entirely, so flipping nvmeof.multipath=true on a
			// live install added the extra ports for NEW volumes only — while the
			// publish context advertised all addresses for EVERY volume. An
			// existing volume therefore advertised paths it had no port_subsys
			// association for. Converge here so the advertisement is true.
			// No-op (zero extra API calls) when multipath is off, which is the
			// default, so the single-port path is unchanged.
			if _, assocErr := d.associateNVMeoFPorts(ctx, subsys.ID, d.config.NVMeoF.multipathAddresses()); assocErr != nil {
				return status.Errorf(codes.Internal, "failed to converge NVMe-oF multipath port associations: %v", assocErr)
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

	allowAnyHost := d.config.NVMeoF.SubsystemAllowAnyHost && d.config.Fencing.Mode != FencingModeStrict
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
		subsys, err = d.truenasClient.NVMeoFSubsystemCreate(ctx, subsysName, allowAnyHost, hostIDs, opts.nvmeofSubsystemCreateOpts())
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
			opts.nvmeofSubsystemCreateOpts(),
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

	// Get or create the NVMe-oF TCP port(s) BEFORE creating namespace.
	// TrueNAS 25.10+: Subsystems must be associated with a port to be accessible
	// over the network. When multipath is enabled the subsystem is associated with
	// one port per configured storage address (E-6); otherwise a single port on
	// TransportAddress is used (byte-identical to pre-GF4). Install-wide port
	// performance fields (E-4) apply to every created port.
	addresses := d.config.NVMeoF.multipathAddresses()
	if len(addresses) == 0 {
		addresses = []string{d.config.NVMeoF.TransportAddress}
	}
	portSubsysIDs, assocErr := d.associateNVMeoFPorts(ctx, subsys.ID, addresses)
	if assocErr != nil {
		// Cleanup subsystem on port/association failure - the volume would be
		// unusable without a port. Deleting the subsystem also reaps every
		// association already created for it, which is why no explicit
		// association rollback runs here: a partial loop leaves associations
		// only when the subsystem PRE-EXISTED, and in that case some of the
		// collected IDs may be associations this call merely adopted (the
		// create is already-exists-tolerant), so deleting them would tear down
		// working paths. The loop is idempotent, so a retry converges.
		if !subsysWasExisting {
			if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF subsystem after port association failure: %v", delErr)
			}
		}
		return status.Errorf(codes.Internal, "%v", assocErr)
	}
	// The first association is the canonical one recorded in the dataset property
	// (back-compat with the single-port path). The delete path lists and removes
	// ALL associations for the subsystem, so the extra multipath associations are
	// reaped on volume delete.
	portSubsys := &truenas.NVMeoFPortSubsys{ID: portSubsysIDs[0]}

	// Create namespace (TrueNAS 25.10+: device_path format is "zvol/pool/vol", device_type is required)
	devicePath := fmt.Sprintf("zvol/%s", datasetName)
	namespace, err := d.truenasClient.NVMeoFNamespaceCreate(ctx, subsys.ID, devicePath, "ZVOL")
	if err != nil {
		// Cleanup port-subsystem association(s) and subsystem on namespace failure
		for _, assocID := range portSubsysIDs {
			if delErr := d.truenasClient.NVMeoFPortSubsysDelete(ctx, assocID); delErr != nil {
				klog.Warningf("Failed to cleanup NVMe-oF port-subsystem association %d: %v", assocID, delErr)
			}
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

	// ensureShareExists may have memoized a complete miss before recreating this
	// share. Replace that stale (nil, nil) resolution with the objects just
	// created and clear associations after all share/association mutations so
	// the fenced publish classifies and enforces against current backend state.
	res.storeNVMeObjects(namespace, subsys)
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
