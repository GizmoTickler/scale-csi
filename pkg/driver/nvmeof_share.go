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

func (b nvmeoFShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string, res *fenceResolution) error {
	return b.d.createNVMeoFShare(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady, res, finalProperties)
}

func (b nvmeoFShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteNVMeoFShareForDataset(ctx, ds, datasetName)
}

func (b nvmeoFShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string, hasDeferredActiveISCSI bool, res *fenceResolution) error {
	return b.d.applyNVMeFence(ctx, ds, datasetName, enforceable, removing, ownedNVMeNQNs, uniqueSortedStrings(protectedNVMeNQNs), res)
}

func (b nvmeoFShareBackend) VolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string, res *fenceResolution) error {
	return b.d.nvmeofVolumeContext(ctx, ds, datasetName, volumeContext, res)
}

// nvmeofVolumeContext resolves the NVMe-oF namespace/subsystem and populates
// the publish context keys. When res already holds a consistent pair (this
// request created or resolved them) they are used as they are; anything less
// is resolved from the backend exactly as before.
func (d *Driver) nvmeofVolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string, res *fenceResolution) error {
	var namespace *truenas.NVMeoFNamespace
	var subsys *truenas.NVMeoFSubsystem
	// An empty NQN in the memo (a create reply without subnqn) is never
	// trusted: the volume context is immutable.
	if res != nil && res.nvmeNSLoaded && res.nvmeNamespace != nil && res.nvmeSubsystem != nil &&
		res.nvmeSubsystem.NQN != "" && res.nvmeNamespace.SubsystemID == res.nvmeSubsystem.ID {
		namespace, subsys = res.nvmeNamespace, res.nvmeSubsystem
	} else {
		var err error
		namespace, err = d.resolveNVMeNamespace(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF namespace: %v", err)
		}
		subsys, err = d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
		if err != nil || subsys == nil {
			return status.Errorf(codes.Internal, "failed to resolve NVMe-oF subsystem for %s: %v", datasetName, err)
		}
	}
	if namespace == nil || namespace.SubsystemID != subsys.ID {
		return status.Errorf(codes.Internal, "NVMe-oF namespace for %s is missing or references a different subsystem", datasetName)
	}
	volumeContext["nqn"] = subsys.NQN
	volumeContext["transport"] = d.config.NVMeoF.Transport
	volumeContext["address"] = d.config.NVMeoF.TransportAddress
	volumeContext["port"] = strconv.Itoa(d.config.NVMeoF.TransportServiceID)
	// Keep the create-time volume-context hint for backward compatibility with
	// older nodes. ControllerPublishVolume also returns the same encoding in its
	// mutable publish context so pre-existing PVs can converge without PV edits.
	publishContext, err := d.nvmeofPublishContext()
	if err != nil {
		return err
	}
	for key, value := range publishContext {
		volumeContext[key] = value
	}
	return nil
}

// nvmeofPublishContext returns the mutable, attach-scoped NVMe-oF hints that a
// node needs in addition to the immutable PV volume context. It is intentionally
// empty when multipath is disabled so the historical single-path response is
// unchanged. Keeping the encoder here also guarantees CreateVolume and
// ControllerPublishVolume use exactly the same addresses JSON format.
func (d *Driver) nvmeofPublishContext() (map[string]string, error) {
	addresses := d.config.NVMeoF.multipathAddresses()
	if len(addresses) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(addresses)
	if err != nil {
		// []string contains no values that encoding/json can reject. Keep this
		// defensive branch because the helper's error contract is used by both
		// CreateVolume and ControllerPublishVolume.
		return nil, status.Errorf(codes.Internal, "failed to encode NVMe-oF multipath addresses: %v", err)
	}
	return map[string]string{"addresses": string(encoded)}, nil
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
//
// checkExisting is false on the brand-new-subsystem path (a subsystem that was
// just created has, by construction, zero existing associations, so listing
// first would be a pure extra round trip with nothing to find — the original
// blind create is already optimal there and the CreateVolume API-call-count
// golden test pins that exact shape) and true on the "already exists" publish
// convergence path (C5): that path is reached on EVERY ControllerPublishVolume
// for an existing multipath volume (see the "E-6 convergence (F-4)" comment on
// that caller), so a blind create-per-address there means every publish,
// forever, re-attempts an association it almost always already has. Before
// this fix that blind create failed already-exists on every steady-state
// call, and NVMeoFPortSubsysCreate reacts to that failure with its own
// recovery query — live counters showed exactly 4 creates + 4 recovery
// queries per 4-address publish (~420ms of a 1.15s publish). ONE
// NVMeoFPortSubsysListBySubsystem query up front (already exported for this
// purpose) tells us what is already associated, so the loop below only calls
// NVMeoFPortSubsysCreate for an address that is actually missing — steady
// state becomes that one query and zero creates.
func (d *Driver) associateNVMeoFPorts(ctx context.Context, subsysID int, addresses []string, checkExisting bool) ([]int, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	portOpts := d.nvmeofPortCreateOpts()
	var existingByPortID map[int]*truenas.NVMeoFPortSubsys
	if checkExisting {
		existingAssociations, listErr := d.truenasClient.NVMeoFPortSubsysListBySubsystem(ctx, subsysID)
		if listErr != nil {
			return nil, fmt.Errorf("failed to list existing NVMe-oF port associations for subsystem %d: %w", subsysID, listErr)
		}
		existingByPortID = make(map[int]*truenas.NVMeoFPortSubsys, len(existingAssociations))
		for _, assoc := range existingAssociations {
			if assoc != nil {
				existingByPortID[assoc.PortID] = assoc
			}
		}
	}
	portSubsysIDs := make([]int, 0, len(addresses))
	for _, addr := range addresses {
		// Address -> port resolution is itself already-exists-tolerant AND
		// process-lifetime cached (NVMeoFGetOrCreatePort), so this is a free
		// cache hit in steady state; it is only ever a real API call the first
		// time this address is seen by this controller process.
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
		if assoc, ok := existingByPortID[port.ID]; ok {
			portSubsysIDs = append(portSubsysIDs, assoc.ID)
			continue
		}
		assoc, assocErr := d.truenasClient.NVMeoFPortSubsysCreate(ctx, port.ID, subsysID)
		if assocErr != nil {
			d.truenasClient.InvalidateNVMeoFPort(
				ctx,
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
	return d.createNVMeoFShare(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady, res, nil)
}

// createNVMeoFShare is createNVMeoFShareForDataset with CreateVolume's final
// property update: when finalProperties is non-nil the new share's resource IDs
// are folded into it instead of a separate warning-only write, as the NFS and
// iSCSI builders do. The caller writes that map fatally right after, still on
// the same side of the share-create boundary, so the IDs become
// durable-or-rolled-back with the rest of provisioning and a create costs one
// pool.dataset.update fewer. A crash in between (there is no wait in that
// window, unlike iSCSI's debounced reload) leaves the share without stored IDs:
// the CreateVolume retry finds the share by name and its repair stamp is
// fatal, so the retry does not succeed until the IDs are stored.
func (d *Driver) createNVMeoFShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, res *fenceResolution, finalProperties map[string]string) error { //nolint:unparam // volumeName is part of the ShareBackend.EnsureShare calling convention shared with NFS/iSCSI (see share_backend.go); this backend does not currently need it, but the signature stays symmetric across all three
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
			// default, so the single-port path is unchanged. checkExisting=true
			// (C5): this is the steady-state per-publish path, so diff against
			// what already exists instead of blindly re-creating every address.
			if _, assocErr := d.associateNVMeoFPorts(ctx, subsys.ID, d.config.NVMeoF.multipathAddresses(), true); assocErr != nil {
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
	// A subsystem nvmet.subsys.create really made starts with exactly the
	// allow_any_host value and host associations it was created with (strict:
	// closed, none; otherwise the configured static hosts, which the create
	// associated before returning), so reconciling it would rewrite
	// allow_any_host to the value it has and list associations it cannot have
	// yet. One that already existed, found above or adopted by name by the
	// create, may carry anything and is reconciled.
	if subsysWasExisting || subsys.Adopted {
		if err = d.reconcileNVMeoFHostAssociations(ctx, subsys.ID); err != nil {
			if !subsysWasExisting {
				if delErr := d.truenasClient.NVMeoFSubsystemDelete(ctx, subsys.ID); delErr != nil {
					klog.Warningf("Failed to cleanup NVMe-oF subsystem after host reconciliation failure: %v", delErr)
				}
			}
			return status.Errorf(codes.Internal, "failed to reconcile NVMe-oF subsystem hosts: %v", err)
		}
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
	// checkExisting=false (C5): a brand-new subsystem has zero associations by
	// construction (or, on the subsysWasExisting resume path, this call is rare
	// enough that the extra list round trip is not worth complicating the
	// fresh-create call-count contract for); the blind create-per-address here
	// is already optimal and is pinned by the CreateVolume API-call-count
	// golden test.
	portSubsysIDs, assocErr := d.associateNVMeoFPorts(ctx, subsys.ID, addresses, false)
	if assocErr != nil {
		// Cleanup subsystem on port/association failure - the volume would be
		// unusable without a port. A partial loop leaves the associations it
		// made, and TrueNAS refuses a plain delete of a subsystem still
		// visible on a port. No explicit association rollback runs for a
		// subsystem that PRE-EXISTED: some of the collected IDs may be
		// associations this call merely adopted (the create is
		// already-exists-tolerant), so deleting them would tear down working
		// paths. The loop is idempotent, so a retry converges.
		if !subsysWasExisting {
			d.rollBackNewNVMeoFSubsystem(ctx, subsys.ID)
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

	// Store all property IDs in one dataset update, or in the caller's.
	// These properties are used for idempotency on retry and cleanup during deletion.
	resourceIDs := map[string]string{
		PropNVMeoFSubsystemID:  strconv.Itoa(subsys.ID),
		PropNVMeoFPortSubsysID: strconv.Itoa(portSubsys.ID),
		PropNVMeoFNamespaceID:  strconv.Itoa(namespace.ID),
	}
	if finalProperties != nil {
		for key, value := range resourceIDs {
			finalProperties[key] = value
		}
	} else if err := d.setDatasetUserProperties(ctx, ds, datasetName, resourceIDs); err != nil {
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
	// One volume, one subsystem: when the subsystem holds no namespace but this
	// volume's, a single forced delete removes it with its namespace and its port
	// and host associations (one nvmet change instead of one per object, each
	// of which TrueNAS applies serially). The listing is what makes the forced
	// delete safe, so failing to read it fails the delete. Nothing locks across
	// volumes here: a second install on the same NAS with the same name prefix
	// and suffix could add a namespace to this subsystem between the listing and
	// the delete, the same name collision that would already let the two
	// installs adopt each other's subsystems.
	if subsystem != nil {
		held, listErr := d.truenasClient.NVMeoFNamespaceListBySubsystem(ctx, subsystem.ID)
		if listErr != nil {
			return fmt.Errorf("failed to list NVMe-oF namespaces of subsystem %d: %w", subsystem.ID, listErr)
		}
		ownDevice := "zvol/" + datasetName
		var own, others []*truenas.NVMeoFNamespace
		for _, listed := range held {
			if (namespace != nil && listed.ID == namespace.ID) || listed.DevicePath == ownDevice {
				own = append(own, listed)
			} else {
				others = append(others, listed)
			}
		}
		listedOwn := func(id int) bool {
			for _, n := range own {
				if n.ID == id {
					return true
				}
			}
			return false
		}
		if len(others) > 0 {
			// The subsystem also serves another volume (a name collision): stop
			// exporting this volume's zvol and leave the subsystem, its port
			// associations and the other namespaces alone, so the other volume
			// keeps working.
			toDelete := own
			if namespace != nil && !listedOwn(namespace.ID) {
				toDelete = append(toDelete, namespace)
			}
			for _, n := range toDelete {
				if deleteErr := d.truenasClient.NVMeoFNamespaceDelete(ctx, n.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
					return fmt.Errorf("NVMe-oF cleanup errors for %s: namespace %d: %w", datasetName, n.ID, deleteErr)
				}
			}
			// Success here lets the dataset delete run, so make sure nothing still
			// exports this zvol.
			if remaining, findErr := d.truenasClient.NVMeoFNamespaceFindByDevicePath(ctx, ownDevice); findErr != nil {
				return fmt.Errorf("NVMe-oF cleanup errors for %s: verify the zvol is no longer exported: %w", datasetName, findErr)
			} else if remaining != nil {
				return fmt.Errorf("NVMe-oF cleanup errors for %s: namespace %d still exports the zvol", datasetName, remaining.ID)
			}
			klog.Warningf("NVMe-oF subsystem %d (%s) also serves %d namespace(s) of another volume; deleted only %s's namespace and left the subsystem and its port associations in place",
				subsystem.ID, subsystem.Name, len(others), datasetName)
			return nil
		}
		if deleteErr := d.truenasClient.NVMeoFSubsystemDeleteCascade(ctx, subsystem.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			return fmt.Errorf("NVMe-oF cleanup errors for %s: subsystem %d: %w", datasetName, subsystem.ID, deleteErr)
		}
		// The cascade removed what the subsystem held; a namespace of this volume
		// that it did not hold is deleted on its own.
		if namespace != nil && !listedOwn(namespace.ID) {
			if deleteErr := d.truenasClient.NVMeoFNamespaceDelete(ctx, namespace.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
				return fmt.Errorf("NVMe-oF cleanup errors for %s: namespace %d: %w", datasetName, namespace.ID, deleteErr)
			}
		}
		klog.Infof("Deleted NVMe-oF resources for %s", datasetName)
		return nil
	}

	// No subsystem: only a namespace of this volume can be left.
	if namespace != nil {
		if deleteErr := d.truenasClient.NVMeoFNamespaceDelete(ctx, namespace.ID); deleteErr != nil && !truenas.IsNotFoundError(deleteErr) {
			return fmt.Errorf("NVMe-oF cleanup errors for %s: namespace %d: %w", datasetName, namespace.ID, deleteErr)
		}
	}

	klog.Infof("Deleted NVMe-oF resources for %s", datasetName)
	return nil
}

// rollBackNewNVMeoFSubsystem deletes a subsystem this create made, after a
// partial port association. "Made" is a belief, not a proof: the create adopts
// an existing subsystem of the same name (another install on the NAS with a
// colliding name), and a forced delete would take that install's namespace
// and paths with it. So the delete is forced, reaping the associations made
// here, only while the subsystem serves no namespace, as one this call created
// still does; otherwise it is the plain delete, which TrueNAS refuses while
// the subsystem is on a port, and the subsystem is left for the delete path.
func (d *Driver) rollBackNewNVMeoFSubsystem(ctx context.Context, subsysID int) {
	namespaces, err := d.truenasClient.NVMeoFNamespaceListBySubsystem(ctx, subsysID)
	if err == nil && len(namespaces) == 0 {
		err = d.truenasClient.NVMeoFSubsystemDeleteCascade(ctx, subsysID)
	} else {
		if err != nil {
			klog.Warningf("NVMe-oF subsystem %d rollback: cannot list its namespaces (%v); not forcing the delete", subsysID, err)
		}
		err = d.truenasClient.NVMeoFSubsystemDelete(ctx, subsysID)
	}
	if err != nil {
		klog.Warningf("Failed to cleanup NVMe-oF subsystem after port association failure: %v", err)
	}
}
