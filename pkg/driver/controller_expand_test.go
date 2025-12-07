package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestControllerExpandVolume_MixedProtocolBug(t *testing.T) {
	// Setup: Driver configured as NFS (filesystem)
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
			},
			DriverName: "org.scale.csi.nfs", // Implies "filesystem" resource type
		},
		truenasClient: mockClient,
	}

	// Pre-create a VOLUME (zvol) - simulating an iSCSI volume
	// This happens when a user uses the NFS driver but requests iSCSI protocol via StorageClass parameters
	volName := "vol-mixed-proto"
	initialSize := int64(1024 * 1024 * 1024) // 1GiB
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name:    "pool/parent/" + volName,
		Type:    "VOLUME",
		Volsize: initialSize,
	})
	assert.NoError(t, err)

	// Action: Expand volume to 2GiB
	newSize := int64(2 * 1024 * 1024 * 1024) // 2GiB
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: volName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: newSize,
		},
	}

	// Execute expansion
	_, err = d.ControllerExpandVolume(context.Background(), req)
	assert.NoError(t, err)

	// Verify: Check if the ZVOL size was actually updated
	ds, err := mockClient.DatasetGet(context.Background(), "pool/parent/"+volName)
	assert.NoError(t, err)

	// The bug: The driver thinks it's a filesystem (due to DriverName), so it updates Refquota instead of Volsize
	// We expect this assertion to FAIL if the bug is present
	currentVolSize := int64(ds.Volsize.Parsed.(float64))
	if currentVolSize != newSize {
		t.Logf("BUG REPRODUCED: Volume size is %d, expected %d", currentVolSize, newSize)
		t.Fail()
	}
}
