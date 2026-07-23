package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestControllerInboundIDsRejectBookkeepingDatasetName proves a crafted
// volume/snapshot ID equal to the bookkeeping dataset leaf is rejected with
// InvalidArgument at the datasetForID validation layer — before any TrueNAS
// access — across every RPC entry class that resolves an inbound ID. This keeps
// the driver's bookkeeping child dataset (tombstone ledger + in-flight markers)
// from ever being targeted for delete/expand/clone. Mirrors the SEC-F1
// path-traversal guard test.
func TestControllerInboundIDsRejectBookkeepingDatasetName(t *testing.T) {
	capability := testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	bookkeepingID := bookkeepingDatasetLeaf
	tests := []struct {
		name string
		call func(*Driver, string) error
	}{
		{
			name: "DeleteVolume",
			call: func(d *Driver, id string) error {
				_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
				return err
			},
		},
		{
			name: "ControllerPublishVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
					VolumeId: id, NodeId: "node-1", VolumeCapability: capability,
				})
				return err
			},
		},
		{
			name: "ValidateVolumeCapabilities",
			call: func(d *Driver, id string) error {
				_, err := d.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: id, VolumeCapabilities: []*csi.VolumeCapability{capability},
				})
				return err
			},
		},
		{
			name: "CreateSnapshotSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
					SourceVolumeId: id, Name: "safe-snapshot",
				})
				return err
			},
		},
		{
			name: "ControllerExpandVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId: id, CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
				})
				return err
			},
		},
		{
			name: "ControllerGetVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: id})
				return err
			},
		},
		{
			name: "CreateVolumeSnapshotSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
					Name: "safe-target", VolumeCapabilities: []*csi.VolumeCapability{capability},
					VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: id},
					}},
				})
				return err
			},
		},
		{
			name: "CreateVolumeVolumeSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
					Name: "safe-target", VolumeCapabilities: []*csi.VolumeCapability{capability},
					VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: id},
					}},
				})
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newControllerCallCountingMock()
			d := &Driver{
				config: &Config{
					DriverName: "org.scale.csi.nfs",
					ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
					NFS:        NFSConfig{ShareHost: "192.0.2.10"},
				},
				truenasClient: client,
			}

			err := tc.call(d, bookkeepingID)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Zero(t, client.datasetGetCalls, "the bookkeeping ID must be rejected before TrueNAS access")
			assert.Empty(t, client.Datasets)
			assert.Empty(t, client.Snapshots)
			assert.Empty(t, client.NFSShares)
			assert.Empty(t, client.ISCSITargets)
			assert.Empty(t, client.NVMeSubsystems)
			assert.Empty(t, client.DatasetDeleteCalls)
		})
	}
}
