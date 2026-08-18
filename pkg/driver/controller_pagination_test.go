package driver

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// MockClientWithPagination extends MockClient to support pagination testing
type MockClientWithPagination struct {
	*truenas.MockClient
}

func (m *MockClientWithPagination) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	var allDatasets []*truenas.Dataset
	for _, ds := range m.Datasets {
		allDatasets = append(allDatasets, ds)
	}

	// Simple pagination simulation
	start := offset
	if start >= len(allDatasets) {
		return []*truenas.Dataset{}, nil
	}
	end := start + limit
	if end > len(allDatasets) {
		end = len(allDatasets)
	}
	if limit == 0 {
		end = len(allDatasets)
	}

	return allDatasets[start:end], nil
}

func (m *MockClientWithPagination) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*truenas.Snapshot, error) {
	var allSnapshots []*truenas.Snapshot
	for _, snap := range m.Snapshots {
		allSnapshots = append(allSnapshots, snap)
	}

	// Simple pagination simulation
	start := offset
	if start >= len(allSnapshots) {
		return []*truenas.Snapshot{}, nil
	}
	end := start + limit
	if end > len(allSnapshots) {
		end = len(allSnapshots)
	}
	if limit == 0 {
		end = len(allSnapshots)
	}

	return allSnapshots[start:end], nil
}

func TestListVolumes_Pagination(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	// Populate with 5 volumes
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("vol-%d", i)
		mockClient.Datasets["pool/parent/"+name] = &truenas.Dataset{
			ID:   "pool/parent/" + name,
			Name: "pool/parent/" + name,
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource: {Value: "true"},
			},
		}
	}
	mockClient.Datasets["pool/parent/unmanaged"] = &truenas.Dataset{
		ID:             "pool/parent/unmanaged",
		Name:           "pool/parent/unmanaged",
		UserProperties: map[string]truenas.UserProperty{},
	}
	mockClient.Datasets["pool/other/managed"] = &truenas.Dataset{
		ID:   "pool/other/managed",
		Name: "pool/other/managed",
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource: {Value: "true"},
		},
	}

	paginatedClient := &PaginatedMockClient{MockClient: mockClient}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: paginatedClient,
	}

	var walkedIDs []string

	// Test Case 1: First Page (Limit 2)
	req := &csi.ListVolumesRequest{
		MaxEntries: 2,
	}
	resp, err := d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "2", resp.NextToken)
	for _, entry := range resp.Entries {
		walkedIDs = append(walkedIDs, entry.GetVolume().GetVolumeId())
	}

	// Test Case 2: Second Page (Limit 2, Offset 2)
	req = &csi.ListVolumesRequest{
		MaxEntries:    2,
		StartingToken: "2",
	}
	resp, err = d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "4", resp.NextToken)
	for _, entry := range resp.Entries {
		walkedIDs = append(walkedIDs, entry.GetVolume().GetVolumeId())
	}

	// Test Case 3: Last Page (Limit 2, Offset 4) - Should return 1 item, no next token
	req = &csi.ListVolumesRequest{
		MaxEntries:    2,
		StartingToken: "4",
	}
	resp, err = d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 1)
	assert.Empty(t, resp.NextToken)
	for _, entry := range resp.Entries {
		walkedIDs = append(walkedIDs, entry.GetVolume().GetVolumeId())
	}

	// The frozen view is name-sorted, so the walk covers every managed volume
	// exactly once, in order — no skips, no duplicates.
	assert.Equal(t, []string{"vol-0", "vol-1", "vol-2", "vol-3", "vol-4"}, walkedIDs)

	// P-2 API shape: one path-scoped listing per WALK (the fresh page), served
	// from the frozen view for both continuation pages; one id-filtered
	// hydration read per page. The old per-page pool.dataset.query lookahead
	// (DatasetList with limit+1) is gone entirely — DatasetList would only run
	// as the fallback when the resource listing errs.
	assert.Equal(t, []string{"pool/parent"}, paginatedClient.datasetQueryByParentParents,
		"exactly one listing call per walk, issued by the fresh (empty-token) page")
	assert.Empty(t, paginatedClient.datasetListLimits,
		"the pool.dataset.query fallback must not run when the path-scoped listing succeeds")
	assert.Equal(t, [][]string{
		{"pool/parent/vol-0", "pool/parent/vol-1"},
		{"pool/parent/vol-2", "pool/parent/vol-3"},
		{"pool/parent/vol-4"},
	}, paginatedClient.datasetGetByNamesCalls, "one page-scoped hydration read per returned page")
}

func TestListSnapshots_Pagination(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	// Populate with 5 snapshots
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("snap-%d", i)
		id := "pool/parent/vol-0@" + name
		mockClient.Snapshots[id] = &truenas.Snapshot{
			ID:      id,
			Name:    name,
			Dataset: "pool/parent/vol-0",
			UserProperties: map[string]truenas.UserProperty{
				PropManagedResource: {Value: "true"},
			},
		}
	}

	paginatedClient := &PaginatedMockClient{MockClient: mockClient}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: paginatedClient,
	}

	// Test Case 1: First Page (Limit 2)
	req := &csi.ListSnapshotsRequest{
		MaxEntries: 2,
	}
	resp, err := d.ListSnapshots(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "2", resp.NextToken)

	// Test Case 2: Second Page (Limit 2, Offset 2)
	req = &csi.ListSnapshotsRequest{
		MaxEntries:    2,
		StartingToken: "2",
	}
	resp, err = d.ListSnapshots(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "4", resp.NextToken)

	// Test Case 3: Last Page (Limit 2, Offset 4) - Should return 1 item, no next token
	req = &csi.ListSnapshotsRequest{
		MaxEntries:    2,
		StartingToken: "4",
	}
	resp, err = d.ListSnapshots(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 1)
	assert.Empty(t, resp.NextToken)
}

type PaginatedMockClient struct {
	*truenas.MockClient
	datasetListLimits           []int
	datasetListParents          []string
	datasetQueryByParentParents []string
	datasetGetByNamesCalls      [][]string
}

func (m *PaginatedMockClient) DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*truenas.Dataset, error) {
	m.datasetQueryByParentParents = append(m.datasetQueryByParentParents, parentDataset)
	return m.MockClient.DatasetQueryByParent(ctx, parentDataset)
}

func (m *PaginatedMockClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	m.datasetGetByNamesCalls = append(m.datasetGetByNamesCalls, append([]string(nil), names...))
	return m.MockClient.DatasetGetByNames(ctx, names)
}

func (m *PaginatedMockClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	m.datasetListLimits = append(m.datasetListLimits, limit)
	m.datasetListParents = append(m.datasetListParents, parentName)

	// Emulate the TrueNAS name-prefix and managed-resource filters before
	// applying offset/limit pagination.
	var allDatasets []*truenas.Dataset
	for _, ds := range m.Datasets {
		if !strings.HasPrefix(ds.Name, parentName+"/") {
			continue
		}
		if prop, ok := ds.UserProperties[PropManagedResource]; !ok || prop.Value != "true" {
			continue
		}
		allDatasets = append(allDatasets, ds)
	}
	sort.Slice(allDatasets, func(i, j int) bool { return allDatasets[i].Name < allDatasets[j].Name })

	start := offset
	if start >= len(allDatasets) {
		return []*truenas.Dataset{}, nil
	}
	end := start + limit
	if end > len(allDatasets) {
		end = len(allDatasets)
	}
	if limit == 0 {
		end = len(allDatasets)
	}

	return allDatasets[start:end], nil
}

func (m *PaginatedMockClient) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*truenas.Snapshot, error) {
	var allSnapshots []*truenas.Snapshot
	for _, snap := range m.Snapshots {
		allSnapshots = append(allSnapshots, snap)
	}

	start := offset
	if start >= len(allSnapshots) {
		return []*truenas.Snapshot{}, nil
	}
	end := start + limit
	if end > len(allSnapshots) {
		end = len(allSnapshots)
	}
	if limit == 0 {
		end = len(allSnapshots)
	}

	return allSnapshots[start:end], nil
}
