package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type datasetGetByNamesChunkFailureClient struct {
	*truenas.MockClient
	calls  int
	chunks [][]string
}

func (c *datasetGetByNamesChunkFailureClient) DatasetGetByNames(_ context.Context, names []string) (map[string]*truenas.Dataset, error) {
	c.calls++
	c.chunks = append(c.chunks, append([]string(nil), names...))
	if c.calls == 2 {
		return nil, fmt.Errorf("injected chunk failure")
	}
	result := make(map[string]*truenas.Dataset, len(names))
	for _, name := range names {
		result[name] = &truenas.Dataset{Name: name}
	}
	return result, nil
}

func TestDatasetGetByNamesChunkingBoundsRequestsAndIsolatesFailures(t *testing.T) {
	names := make([]string, 0, 600)
	for i := 0; i < 600; i++ {
		names = append(names, fmt.Sprintf("pool/parent/%04d-%s", i, strings.Repeat("x", 180)))
	}
	chunks := chunkDatasetNames(names, datasetGetByNamesBatchBudget)
	require.Greater(t, len(chunks), 2)
	for _, chunk := range chunks {
		encoded, err := json.Marshal(chunk)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(encoded)+datasetGetByNamesEnvelopeHeadroom, datasetGetByNamesBatchBudget)
	}

	client := &datasetGetByNamesChunkFailureClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}
	result, failed := d.datasetGetByNamesChunked(context.Background(), names)
	require.Equal(t, len(chunks), client.calls, "a failed chunk must not stop later chunks")
	require.Len(t, failed, len(chunks[1]))
	for _, name := range chunks[1] {
		_, ok := failed[name]
		assert.True(t, ok)
		assert.NotContains(t, result, name)
	}
	for _, chunk := range append(chunks[:1], chunks[2:]...) {
		for _, name := range chunk {
			assert.Contains(t, result, name, "successful chunks are merged despite a peer failure")
		}
	}
}
