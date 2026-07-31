package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// capacityTestDriver builds a controller Driver whose parent dataset is seeded on
// the mock with a ZFS-computed `available` value, mirroring the single
// pool.dataset.query GetCapacity now issues (E2/K7).
func capacityTestDriver(client truenas.ClientInterface) *Driver {
	return &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}
}

func seedParentDataset(client *truenas.MockClient, available, used float64) {
	client.Datasets["pool/parent"] = &truenas.Dataset{
		ID:        "pool/parent",
		Name:      "pool/parent",
		Type:      "FILESYSTEM",
		Available: truenas.DatasetProperty{Parsed: available},
		Used:      truenas.DatasetProperty{Parsed: used},
	}
}

// TestGetCapacityReportsParentAvailable proves GetCapacity returns the parent
// dataset's ZFS `available` bytes (parity/quota/reservation-aware) instead of the
// old raw-vdev free-space sum, and leaves maximum_volume_size unset by default.
func TestGetCapacityReportsParentAvailable(t *testing.T) {
	client := truenas.NewMockClient()
	const available = float64(15461032397232) // G1 probe value (~14.06 TiB)
	seedParentDataset(client, available, 1234)
	d := capacityTestDriver(client)

	resp, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(available), resp.GetAvailableCapacity())
	assert.Nil(t, resp.GetMaximumVolumeSize(), "maximum_volume_size must stay unset unless reportMaximumVolumeSize is on")
}

// TestGetCapacityMaximumVolumeSize proves the opt-in reportMaximumVolumeSize flag
// sets maximum_volume_size to the same available ceiling, and that leaving it off
// keeps the field nil.
func TestGetCapacityMaximumVolumeSize(t *testing.T) {
	const available = float64(8 * testGiB)

	t.Run("off leaves maximum unset", func(t *testing.T) {
		client := truenas.NewMockClient()
		seedParentDataset(client, available, 0)
		d := capacityTestDriver(client)

		resp, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
		require.NoError(t, err)
		assert.Nil(t, resp.GetMaximumVolumeSize())
	})

	t.Run("on sets maximum to available", func(t *testing.T) {
		client := truenas.NewMockClient()
		seedParentDataset(client, available, 0)
		d := capacityTestDriver(client)
		d.config.Capacity.ReportMaximumVolumeSize = true

		resp, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp.GetMaximumVolumeSize())
		assert.Equal(t, int64(available), resp.GetMaximumVolumeSize().GetValue())
	})
}

// TestGetCapacityIgnoresRequestParameters proves per-StorageClass parameters do
// not change the reported parent-dataset capacity: the driver honors no per-SC
// parent/pool override, so every class shares the one backend number.
func TestGetCapacityIgnoresRequestParameters(t *testing.T) {
	client := truenas.NewMockClient()
	const available = float64(4 * testGiB)
	seedParentDataset(client, available, 0)
	d := capacityTestDriver(client)

	resp, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{
		Parameters: map[string]string{"protocol": "iscsi", "pool": "some/other"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(available), resp.GetAvailableCapacity())
}

// TestGetCapacityDatasetError proves a backend failure surfaces as codes.Internal
// and a missing parent dataset (parsed available absent) reports zero rather than
// panicking on the type assertion.
func TestGetCapacityDatasetError(t *testing.T) {
	t.Run("backend error maps to Internal", func(t *testing.T) {
		client := truenas.NewMockClient()
		client.InjectError = assert.AnError
		d := capacityTestDriver(client)

		_, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("absent available parses to zero", func(t *testing.T) {
		client := truenas.NewMockClient()
		// Parent dataset exists but carries no parsed available property.
		client.Datasets["pool/parent"] = &truenas.Dataset{ID: "pool/parent", Name: "pool/parent", Type: "FILESYSTEM"}
		d := capacityTestDriver(client)

		resp, err := d.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
		require.NoError(t, err)
		assert.Zero(t, resp.GetAvailableCapacity())
	})
}
