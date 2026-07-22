package driver

import (
	"context"
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

func (b nvmeoFShareBackend) EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string) error {
	// The create path validates the namespace's device backreference and
	// repairs cached IDs before returning an idempotent success.
	return b.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, false, false)
}

func (b nvmeoFShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	return b.d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, zvolReady)
}

func (b nvmeoFShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteNVMeoFShareForDataset(ctx, ds, datasetName)
}

func (b nvmeoFShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string) error {
	return b.d.applyNVMeFence(ctx, ds, datasetName, enforceable, removing, ownedNVMeNQNs, uniqueSortedStrings(protectedNVMeNQNs))
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
