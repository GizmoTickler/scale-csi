package driver

import (
	"context"
	"fmt"
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
				"truenas-csi:managed_resource": {Value: "true"},
			},
		}
	}

	// We can't easily override methods on the struct without an interface wrapper or embedding.
	// Since Driver uses the interface, we can pass a wrapper.
	// However, MockClient methods are defined on *MockClient.
	// We need to create a wrapper that implements the interface and delegates to MockClient, overriding List methods.

	// Actually, since I modified MockClient in the codebase to accept limit/offset but ignore them,
	// I can't test pagination logic *inside* the driver unless the client returns paginated results.
	// The driver logic is:
	// 1. Parse token to offset
	// 2. Pass offset/limit to client
	// 3. Get results
	// 4. If result count == limit, set next token

	// If MockClient ignores limit/offset and returns EVERYTHING, then:
	// If I ask for limit=2, offset=0. Mock returns 5 items.
	// Driver sees 5 items. 5 != 2 (limit). Next token = "".
	// This is WRONG behavior for the test.

	// So I MUST modify MockClient in the test to behave correctly.
	// But MockClient is a concrete type in pkg/truenas.
	// I can't shadow methods of a type defined in another package easily unless I embed it and use the embedded type.

	// Let's create a local struct that embeds *truenas.MockClient and overrides the methods.
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
	req := &csi.ListVolumesRequest{
		MaxEntries: 2,
	}
	resp, err := d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "2", resp.NextToken)

	// Test Case 2: Second Page (Limit 2, Offset 2)
	req = &csi.ListVolumesRequest{
		MaxEntries:    2,
		StartingToken: "2",
	}
	resp, err = d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, "4", resp.NextToken)

	// Test Case 3: Last Page (Limit 2, Offset 4) - Should return 1 item, no next token
	req = &csi.ListVolumesRequest{
		MaxEntries:    2,
		StartingToken: "4",
	}
	resp, err = d.ListVolumes(context.Background(), req)
	assert.NoError(t, err)
	assert.Len(t, resp.Entries, 1)
	assert.Empty(t, resp.NextToken)
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
				"truenas-csi:managed_resource": {Value: "true"},
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
}

func (m *PaginatedMockClient) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*truenas.Dataset, error) {
	// Access internal map via public field (it is exported)
	var allDatasets []*truenas.Dataset
	// Iteration order of map is random, so we must sort or just accept random subset for generic count test.
	// For strict pagination test, we need stable order.
	// Let's just collect all and return a slice.
	for _, ds := range m.Datasets {
		allDatasets = append(allDatasets, ds)
	}

	// Since map order is random, let's not rely on specific items, just counts.

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
