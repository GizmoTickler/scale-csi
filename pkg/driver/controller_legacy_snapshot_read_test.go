package driver

import (
	"context"
	"testing"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The 26.0 resource-query read path is sourceless and isCSISnapshot trusts only
// the snapshot-only csi_snapshot_name there. The LEGACY path reports property
// sources, and managed_resource inherits from every CSI volume dataset into
// every snapshot of it — so an inherited source must not classify the snapshot
// as CSI-created (the rule snapshotMatchesRetainedTombstoneIdentity already
// applies to tombstone identity). Trusting it classified every task-created
// scheduled snapshot (GF2/E2) as a blocking CSI snapshot on a legacy read.
func TestIsCSISnapshotLegacyReadRejectsInheritedManagedResource(t *testing.T) {
	legacySnap := func(props map[string]truenas.UserProperty) *truenas.Snapshot {
		return &truenas.Snapshot{
			ID:             "pool/parent/vol@snap",
			Name:           "snap",
			Dataset:        "pool/parent/vol",
			UserProperties: props,
			ResourceQuery:  false,
		}
	}
	for _, tc := range []struct {
		name  string
		props map[string]truenas.UserProperty
		want  bool
	}{
		{
			name: "inherited managed_resource is NOT a CSI snapshot",
			props: map[string]truenas.UserProperty{
				PropManagedResource: {Value: "true", Source: "inherited from pool/parent/vol"},
			},
			want: false,
		},
		{
			name: "locally sourced managed_resource is a CSI snapshot",
			props: map[string]truenas.UserProperty{
				PropManagedResource: {Value: "true", Source: "local"},
			},
			want: true,
		},
		{
			name: "sourceless managed_resource keeps the pre-existing trust",
			props: map[string]truenas.UserProperty{
				PropManagedResource: {Value: "true"},
			},
			want: true,
		},
		{
			name: "snapshot-only csi_snapshot_name classifies regardless of managed_resource",
			props: map[string]truenas.UserProperty{
				PropCSISnapshotName: {Value: "snap", Source: "local"},
				PropManagedResource: {Value: "true", Source: "inherited from pool/parent/vol"},
			},
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isCSISnapshot(legacySnap(tc.props)))
		})
	}
}

// The end-to-end form of the wedge: a task-created scheduled snapshot read back
// through the LEGACY path (sources populated, managed_resource inherited) must
// not trip the dependent-snapshot guard — DeleteVolume proves it through the
// scheduled-snapshot ownership chain and deletes it with the volume, exactly as
// it does on a 26.0 resource-query read.
func TestDeleteVolumeAcceptsTaskCreatedScheduledSnapshotOnLegacyRead(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.SystemTimezoneName = "UTC"
	d := newScheduleTestDriver(client)

	seedScheduledVolume(t, client, d, "sched-legacy")
	snap := fireScheduledSnapshot(t, client, time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC), "UTC")
	require.Equal(t, "pool/parent/sched-legacy", snap.Dataset)

	// Reshape the stored snapshot into the legacy read: sources reported, the
	// dataset-derived properties inherited. The dataset really does carry
	// managed_resource, so its snapshots inherit it there.
	stored := client.Snapshots[snap.ID]
	require.NotNil(t, stored)
	stored.ResourceQuery = false
	for key, prop := range stored.UserProperties {
		prop.Source = "inherited from pool/parent/sched-legacy"
		stored.UserProperties[key] = prop
	}

	_, err := d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "sched-legacy"})
	require.NoError(t, err,
		"an inherited managed_resource on a legacy read must not classify the scheduled snapshot as a blocking CSI snapshot")
}
