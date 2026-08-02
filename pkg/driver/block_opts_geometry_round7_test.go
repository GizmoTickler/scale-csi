package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestNVMeoFSnapshotRestoreUsesTheNamespacePath is the NVMe-oF snapshot
// restore regression. NVMe-oF namespaces have no iSCSI extent geometry in the
// TrueNAS surface used by this driver; the restore therefore follows the
// existing namespace path and completes.
func TestNVMeoFSnapshotRestoreUsesTheNamespacePath(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-nvme-snapshot-src", "nvmeof", nil))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-nvme-snapshot-src",
		Name:           "nvme-snapshot-point",
	})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-nvme-snapshot-dst", "nvmeof", nil),
		"nvme-snapshot-point",
	))
	require.NoError(t, err, "an NVMe-oF snapshot restore must not ask the iSCSI geometry resolver")

	namespace, err := client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/pool/parent/pvc-nvme-snapshot-dst")
	require.NoError(t, err)
	assert.NotNil(t, namespace, "the restored volume must have an NVMe-oF namespace")
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-nvme-snapshot-dst")
	require.NoError(t, err)
	assert.Nil(t, extent, "an NVMe-oF restore must not create an iSCSI extent")
}

// TestNVMeoFVolumeCloneUsesTheNamespacePath is the PVC-to-PVC half of the
// NVMe-oF availability regression. It uses the controller's volume-content
// source arm, rather than calling the geometry resolver directly.
func TestNVMeoFVolumeCloneUsesTheNamespacePath(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)

	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-nvme-clone-src", "nvmeof", nil))
	require.NoError(t, err)
	cloneRequest := blockTuningRequest("pvc-nvme-clone-dst", "nvmeof", nil)
	cloneRequest.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "pvc-nvme-clone-src"},
	}}
	_, err = d.CreateVolume(ctx, cloneRequest)
	require.NoError(t, err, "an NVMe-oF PVC clone must not ask the iSCSI geometry resolver")

	namespace, err := client.NVMeoFNamespaceFindByDevicePath(ctx, "zvol/pool/parent/pvc-nvme-clone-dst")
	require.NoError(t, err)
	assert.NotNil(t, namespace, "the cloned volume must have an NVMe-oF namespace")
	extent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-nvme-clone-dst")
	require.NoError(t, err)
	assert.Nil(t, extent, "an NVMe-oF clone must not create an iSCSI extent")
}

// nvmeISCSIProbeCountingClient makes an accidental iSCSI geometry question
// observable on an NVMe-oF snapshot. Returning an error models a backend with
// no iSCSI extent for the NVMe zvol; snapshot capture remains best-effort, so
// the call count is the discriminating assertion.
type nvmeISCSIProbeCountingClient struct {
	*truenas.MockClient
	iscsiExtentFindCalls int
}

func (c *nvmeISCSIProbeCountingClient) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*truenas.ISCSIExtent, error) {
	c.iscsiExtentFindCalls++
	return nil, fmt.Errorf("unexpected iSCSI extent probe for NVMe-oF disk %s", diskPath)
}

// TestNVMeoFZvolNeverEntersTheISCSIFailClosedPath pins the protocol boundary
// through CreateSnapshot. An NVMe namespace witness is not an iSCSI geometry
// record, and the snapshot must not invoke ISCSIExtentFindByDisk at all.
func TestNVMeoFZvolNeverEntersTheISCSIFailClosedPath(t *testing.T) {
	mock := truenas.NewMockClient()
	client := &nvmeISCSIProbeCountingClient{MockClient: mock}
	d, _ := newBlockImmutabilityDriver(t)
	d.truenasClient = client
	ctx := context.Background()
	_, err := mock.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-nvme-probe", "nvmeof", nil))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-nvme-probe",
		Name:           "nvme-probe-point",
	})
	require.NoError(t, err)
	assert.Zero(t, client.iscsiExtentFindCalls,
		"an NVMe-oF zvol must not enter the iSCSI extent-geometry probe")
}

// TestSnapshotCapturedOutOfDomainBlocksizeIsUntrusted drives the snapshot
// restore arm with an operator-mutated snapshot property. A captured value
// outside the iSCSI extent domain is treated as an incomplete record, so the
// restore refuses instead of creating an extent at that value.
func TestSnapshotCapturedOutOfDomainBlocksizeIsUntrusted(t *testing.T) {
	d, client := newBlockImmutabilityDriver(t)
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = d.CreateVolume(ctx, blockTuningRequest("pvc-invalid-snapshot-src", "iscsi", nil))
	require.NoError(t, err)
	_, err = d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: "pvc-invalid-snapshot-src",
		Name:           "invalid-snapshot-point",
	})
	require.NoError(t, err)

	// Model the documented operator recovery edit through the client API. The
	// value is not produced by the driver and is deliberately outside its valid
	// iSCSI extent domain.
	require.NoError(t, client.SnapshotSetUserProperty(ctx, "pool/parent/pvc-invalid-snapshot-src@invalid-snapshot-point",
		PropBlockISCSIBlocksize, "1234"))

	_, err = d.CreateVolume(ctx, restoreFromSnapshot(
		blockTuningRequest("pvc-invalid-snapshot-dst", "iscsi", nil),
		"invalid-snapshot-point",
	))
	require.Error(t, err, "an out-of-domain snapshot value must not drive a destination extent")
	assert.Contains(t, status.Convert(err).Message(), validISCSIBlocksizeList)
	destinationExtent, findErr := client.ISCSIExtentFindByDisk(ctx, "zvol/pool/parent/pvc-invalid-snapshot-dst")
	require.NoError(t, findErr)
	assert.Nil(t, destinationExtent, "the restore must fail before creating an extent at 1234")
}
