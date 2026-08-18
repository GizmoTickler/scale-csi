package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// countingOrphanClassifyClient wraps the mock so the batched orphan-classify
// path can be observed: it counts DatasetGetByNames round trips and any
// residual per-candidate DatasetGet reads (the N+1 shape the batching removed),
// and can fail exactly the chunk containing failChunkContaining to prove a
// chunk failure is isolated to its own names.
type countingOrphanClassifyClient struct {
	*truenas.MockClient
	datasetGetCalls        int
	datasetGetByNamesCalls int
	failChunkContaining    string
	failErr                error
}

func (c *countingOrphanClassifyClient) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	c.datasetGetCalls++
	return c.MockClient.DatasetGet(ctx, name)
}

func (c *countingOrphanClassifyClient) DatasetGetByNames(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	c.datasetGetByNamesCalls++
	if c.failChunkContaining != "" {
		for _, name := range names {
			if name == c.failChunkContaining {
				return nil, c.failErr
			}
		}
	}
	return c.MockClient.DatasetGetByNames(ctx, names)
}

// newOrphanClassifyFixture builds count age-eligible, non-live orphan
// candidates whose names are long enough that chunkDatasetNames must split the
// batched re-read into multiple DatasetGetByNames chunks. The datasets double
// as the listing view classifyOrphanVolumes receives.
func newOrphanClassifyFixture(t *testing.T, count int) (*countingOrphanClassifyClient, *Driver, []*truenas.Dataset, []string) {
	t.Helper()
	client := &countingOrphanClassifyClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}
	old := time.Now().Add(-48 * time.Hour)
	datasets := make([]*truenas.Dataset, 0, count)
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		volumeID := fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 180))
		ds := addReconcileDataset(client.MockClient, volumeID, old, true, 10)
		datasets = append(datasets, ds)
		names = append(names, ds.Name)
	}
	return client, d, datasets, names
}

// N candidates must cost ceil(N/chunk) DatasetGetByNames round trips, not N
// per-candidate DatasetGet reads, and report.OrphanVolumes must keep the input
// dataset order.
func TestClassifyOrphanVolumesBatchesSourceBearingReads(t *testing.T) {
	client, d, datasets, names := newOrphanClassifyFixture(t, 600)
	expectedChunks := len(chunkDatasetNames(names, datasetGetByNamesBatchBudget))
	require.Greater(t, expectedChunks, 1, "the fixture must be large enough to require multiple chunks")

	report := &ReconcileReport{}
	kubeState := &kubernetesReconcileState{volumeHandles: map[string]struct{}{}}
	managed := d.classifyOrphanVolumes(context.Background(), time.Now(), datasets, kubeState, time.Hour, report)

	assert.Equal(t, len(datasets), managed)
	assert.Equal(t, expectedChunks, client.datasetGetByNamesCalls, "candidates must be re-read in ceil(N/chunk) batches")
	assert.Zero(t, client.datasetGetCalls, "the per-candidate DatasetGet N+1 path must be gone")
	require.Len(t, report.OrphanVolumes, len(datasets))
	for i, orphan := range report.OrphanVolumes {
		assert.Equal(t, names[i], orphan.BackendID, "classification must preserve the input dataset order")
	}
}

// A failed chunk must record one orphan_volume_classify failure per affected
// candidate and skip exactly those names — never classifying them as orphans
// and never stopping the later chunks or failing the pass.
func TestClassifyOrphanVolumesChunkFailureSkipsOnlyAffectedCandidates(t *testing.T) {
	client, d, datasets, names := newOrphanClassifyFixture(t, 600)
	chunks := chunkDatasetNames(names, datasetGetByNamesBatchBudget)
	require.Greater(t, len(chunks), 2)
	client.failChunkContaining = chunks[1][0]
	client.failErr = fmt.Errorf("injected chunk failure")
	failureMetric := reconcileFailuresTotal.WithLabelValues("orphan_volume_classify")
	failuresBefore := testutil.ToFloat64(failureMetric)

	report := &ReconcileReport{}
	kubeState := &kubernetesReconcileState{volumeHandles: map[string]struct{}{}}
	managed := d.classifyOrphanVolumes(context.Background(), time.Now(), datasets, kubeState, time.Hour, report)

	assert.Equal(t, len(datasets), managed, "the managed count precedes classification and must survive a chunk failure")
	assert.Equal(t, len(chunks), client.datasetGetByNamesCalls, "a failed chunk must not stop later chunks")
	assert.Equal(t, failuresBefore+float64(len(chunks[1])), testutil.ToFloat64(failureMetric),
		"every candidate in the failed chunk records its own failure")
	failedNames := make(map[string]struct{}, len(chunks[1]))
	for _, name := range chunks[1] {
		failedNames[name] = struct{}{}
	}
	expected := make([]string, 0, len(names)-len(chunks[1]))
	for _, name := range names {
		if _, failed := failedNames[name]; !failed {
			expected = append(expected, name)
		}
	}
	actual := make([]string, 0, len(report.OrphanVolumes))
	for _, orphan := range report.OrphanVolumes {
		actual = append(actual, orphan.BackendID)
	}
	assert.Equal(t, expected, actual, "only the failed chunk's candidates are skipped, in input order")
}

// The batched re-read must keep the source gate: an inherited managed_resource
// is skipped silently, and a candidate that vanished between listing and
// re-read (absent from the DatasetGetByNames result) records a failure and is
// never classified as an orphan.
func TestClassifyOrphanVolumesSkipsInheritedAndVanishedCandidates(t *testing.T) {
	client := &countingOrphanClassifyClient{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}
	old := time.Now().Add(-48 * time.Hour)
	genuine := addReconcileDataset(client.MockClient, "genuine-orphan", old, true, 100)
	nested := addNestedForeignDataset(client.MockClient, "genuine-orphan", "nested", old)
	// vanished exists only in the listing view: the mock holds no dataset by
	// this name, so the batched re-read omits it (the DatasetGetByNames
	// contract for deleted names).
	vanished := &truenas.Dataset{
		ID:       "pool/parent/vanished",
		Name:     "pool/parent/vanished",
		Type:     "FILESYSTEM",
		Creation: truenas.DatasetProperty{Parsed: float64(old.Unix())},
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource: {Value: "true"},
		},
	}
	failureMetric := reconcileFailuresTotal.WithLabelValues("orphan_volume_classify")
	failuresBefore := testutil.ToFloat64(failureMetric)

	report := &ReconcileReport{}
	kubeState := &kubernetesReconcileState{volumeHandles: map[string]struct{}{}}
	managed := d.classifyOrphanVolumes(context.Background(), time.Now(),
		[]*truenas.Dataset{genuine, nested, vanished}, kubeState, time.Hour, report)

	assert.Equal(t, 3, managed, "every managed_resource=true listing entry is counted before filtering")
	require.Len(t, report.OrphanVolumes, 1)
	assert.Equal(t, "genuine-orphan", report.OrphanVolumes[0].ID)
	assert.Equal(t, 1, client.datasetGetByNamesCalls)
	assert.Zero(t, client.datasetGetCalls)
	assert.Equal(t, failuresBefore+1, testutil.ToFloat64(failureMetric),
		"only the vanished candidate records a classification failure")
}
