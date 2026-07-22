package driver

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// nfsShareBackend implements ShareBackend for NFS.
type nfsShareBackend struct{ d *Driver }

func (b nfsShareBackend) EnsureShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string) error {
	return b.d.ensureNFSShareExists(ctx, ds, datasetName, volumeName)
}

func (b nfsShareBackend) CreateShare(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated, zvolReady bool, finalProperties map[string]string) error {
	return b.d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, freshlyCreated, finalProperties)
}

func (b nfsShareBackend) DeleteShare(ctx context.Context, ds *truenas.Dataset, datasetName string) error {
	return b.d.deleteNFSShareForDataset(ctx, ds, datasetName)
}

func (b nfsShareBackend) ApplyFence(ctx context.Context, ds *truenas.Dataset, datasetName string, enforceable, removing []NodeIdentity, ownedNFSHosts, ownedNVMeNQNs, protectedNFSHosts, protectedNVMeNQNs []string) error {
	return b.d.applyNFSFence(ctx, ds, datasetName, enforceable, ownedNFSHosts, uniqueSortedStrings(protectedNFSHosts))
}

func (b nfsShareBackend) VolumeContext(ctx context.Context, ds *truenas.Dataset, datasetName string, volumeContext map[string]string) error {
	return b.d.nfsVolumeContext(ds, volumeContext)
}

// nfsVolumeContext populates the NFS publish context keys.
func (d *Driver) nfsVolumeContext(ds *truenas.Dataset, volumeContext map[string]string) error {
	volumeContext["server"] = d.config.NFS.ShareHost
	volumeContext["share"] = ds.Mountpoint
	return nil
}

// ensureNFSShareExists is the NFS EnsureShare implementation.
func (d *Driver) ensureNFSShareExists(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string) error {
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
	return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, false, nil)
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
	return d.createNFSShareForDataset(ctx, ds, datasetName, volumeName, false, nil)
}

func (d *Driver) createNFSShareForDataset(ctx context.Context, ds *truenas.Dataset, datasetName, volumeName string, freshlyCreated bool, finalProperties map[string]string) error {
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
		Enabled:      d.config.Fencing.Mode != FencingModeStrict,
	}
	if d.config.Fencing.Mode == FencingModeStrict {
		// An enabled export with both lists empty means allow-all in TrueNAS. New
		// strict volumes therefore start disabled until ControllerPublish writes at
		// least one node identity. Additive retains the pre-upgrade static policy so
		// a deferred legacy node remains usable.
		params.Networks = []string{}
		params.Hosts = []string{}
	}

	share, err := d.truenasClient.NFSShareCreate(ctx, params)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create NFS share: %v", err)
	}

	// Store the share ID together with CreateVolume's final managed/ownership/
	// provision/name stamps in ONE pool.dataset.update. Both writes are on the
	// same side of the NFSShareCreate boundary, so folding them together removes
	// a round trip without crossing a crash boundary: the share ID is still
	// stamped immediately after the share object exists (so ensureShareExists can
	// find the share by ID), and finalProperties is empty on the idempotent
	// ensureShareExists/createNFSShare paths so re-stamping there is unchanged.
	shareProperties := make(map[string]string, len(finalProperties)+1)
	for key, value := range finalProperties {
		shareProperties[key] = value
	}
	shareProperties[PropNFSShareID] = strconv.Itoa(share.ID)
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, shareProperties); err != nil {
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
