package truenas

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readTypedFixture(t testing.TB, name string) []byte {
	t.Helper()
	payload, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return payload
}

func interfaceSnapshots(t testing.TB, payload []byte, resourceQuery bool) []*Snapshot {
	t.Helper()
	var items []interface{}
	require.NoError(t, json.Unmarshal(payload, &items))
	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snapshot, err := parseSnapshot(item)
		require.NoError(t, err)
		snapshot.ResourceQuery = resourceQuery
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func typedSnapshots(t testing.TB, payload []byte, resourceQuery bool) []*Snapshot {
	t.Helper()
	var items []*rawSnapshot
	require.NoError(t, json.Unmarshal(payload, &items))
	return rawSnapshotsToSnapshots(items, resourceQuery)
}

func interfaceDatasets(t testing.TB, payload []byte, resourceQuery bool) []*Dataset {
	t.Helper()
	var items []interface{}
	require.NoError(t, json.Unmarshal(payload, &items))
	datasets := make([]*Dataset, 0, len(items))
	for _, item := range items {
		var (
			dataset *Dataset
			err     error
		)
		if resourceQuery {
			dataset, err = parseDatasetResource(item)
			if dataset != nil {
				dataset.ResourceQuery = true
			}
		} else {
			dataset, err = parseDataset(item)
		}
		require.NoError(t, err)
		datasets = append(datasets, dataset)
	}
	return datasets
}

func typedDatasets(t testing.TB, payload []byte, resourceQuery bool) []*Dataset {
	t.Helper()
	var items []*rawDataset
	require.NoError(t, json.Unmarshal(payload, &items))
	return rawDatasetsToDatasets(items, resourceQuery)
}

func TestTypedDecodeGoldenDeepEquality(t *testing.T) {
	t.Run("snapshot_resource", func(t *testing.T) {
		payload := readTypedFixture(t, "snapshot-resource-26.0.json")
		require.Equal(t, interfaceSnapshots(t, payload, true), typedSnapshots(t, payload, true))
	})
	t.Run("pool_snapshot", func(t *testing.T) {
		payload := readTypedFixture(t, "pool-snapshot-26.0.json")
		require.Equal(t, interfaceSnapshots(t, payload, false), typedSnapshots(t, payload, false))
	})
	t.Run("dataset_resource", func(t *testing.T) {
		payload := readTypedFixture(t, "dataset-resource-26.0.json")
		require.Equal(t, interfaceDatasets(t, payload, true), typedDatasets(t, payload, true))
	})
	t.Run("dataset_origins", func(t *testing.T) {
		payload := readTypedFixture(t, "dataset-origins-26.0.json")
		require.Equal(t, interfaceDatasets(t, payload, false), typedDatasets(t, payload, false))
	})
	// Full top-level property set (used/quota/refquota/volsize/creation with
	// value+rawvalue+parsed+source, string-or-number parsed, nested user
	// properties) — pins the pool.dataset.query path used by DatasetList.
	t.Run("dataset_list", func(t *testing.T) {
		payload := readTypedFixture(t, "dataset-list-26.0.json")
		require.Equal(t, interfaceDatasets(t, payload, false), typedDatasets(t, payload, false))
	})
}

func TestTypedSnapshotPreservesGenericPropertyShapeAndPrecedence(t *testing.T) {
	payload := readTypedFixture(t, "snapshot-resource-26.0.json")
	snapshots := typedSnapshots(t, payload, true)
	require.NotEmpty(t, snapshots)
	snapshot := snapshots[0]

	used, ok := snapshot.Properties["used"].(map[string]interface{})
	require.True(t, ok)
	assert.IsType(t, float64(0), used["value"])
	assert.Equal(t, "2723840", used["raw"])
	assert.NotContains(t, used, "rawvalue")
	assert.Equal(t, UserProperty{Value: "from-user-properties", Source: "INHERITED"},
		snapshot.UserProperties["truenas-csi:managed"])
	assert.Equal(t, UserProperty{Value: "flat-value"}, snapshot.UserProperties["truenas-csi:flat"])
	assert.Equal(t, int64(2723840), snapshot.GetSnapshotSize())
	assert.Equal(t, int64(1754693322), snapshot.GetCreationTime())
}

func TestRawResultToInterfacePreservesPublicJSONTypes(t *testing.T) {
	raw := json.RawMessage(`{"number":42,"array":[true,null,"value"]}`)
	got, err := rawResultToInterface(raw)
	require.NoError(t, err)

	var want interface{}
	require.NoError(t, json.Unmarshal(raw, &want))
	assert.True(t, reflect.DeepEqual(want, got))
	number := got.(map[string]interface{})["number"]
	assert.IsType(t, float64(0), number)
}

func TestCallRawIsSharedByTypedAndGenericDecoders(t *testing.T) {
	cfg := &ClientConfig{
		Timeout:               time.Second,
		APIRetryMaxAttempts:   1,
		APIRetryInitialDelay:  time.Millisecond,
		APIRetryMaxDelay:      time.Millisecond,
		APIRetryBackoffFactor: 1,
	}
	connection := NewConnection(0, cfg)
	connection.mu.Lock()
	connection.generation = 1
	connection.stopped = false
	connection.authenticated = true
	connection.writeCh = make(chan writeRequest)
	connection.conn.Store(&websocket.Conn{})
	connection.mu.Unlock()
	atomic.StoreInt32(&connection.connState, int32(stateConnected))

	var metrics atomic.Int32
	client := &Client{
		config:    cfg,
		pool:      []*Connection{connection},
		semaphore: make(chan struct{}, 1),
		metricsRecorder: func(string, float64, error) {
			metrics.Add(1)
		},
	}

	responses := []json.RawMessage{
		json.RawMessage(`{"name":"typed","count":42}`),
		json.RawMessage(`{"name":"generic","count":43}`),
	}
	go func() {
		for _, result := range responses {
			request := <-connection.writeCh
			request.resultCh <- nil
			connection.pendingMu.RLock()
			pending := connection.pending[request.id]
			connection.pendingMu.RUnlock()
			pending.responseCh <- &rpcResponse{ID: request.id, Result: result}
		}
	}()

	var typed struct {
		Name  string `json:"name"`
		Count int64  `json:"count"`
	}
	require.NoError(t, callTyped(context.Background(), client, &typed, "test.typed"))
	assert.Equal(t, "typed", typed.Name)
	assert.Equal(t, int64(42), typed.Count)

	generic, err := client.CallWithContext(context.Background(), "test.generic")
	require.NoError(t, err)
	assert.Equal(t, "generic", generic.(map[string]interface{})["name"])
	assert.Equal(t, float64(43), generic.(map[string]interface{})["count"])
	assert.Equal(t, int32(2), metrics.Load(), "both decoders must traverse the same metrics pipeline")
	assert.Empty(t, client.semaphore)
}

var (
	benchmarkSnapshots []*Snapshot
	benchmarkDatasets  []*Dataset
)

func repeatJSONArray(t testing.TB, payload []byte, copies int) []byte {
	t.Helper()
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &items))
	repeated := make([]json.RawMessage, 0, len(items)*copies)
	for range copies {
		repeated = append(repeated, items...)
	}
	result, err := json.Marshal(repeated)
	require.NoError(t, err)
	return result
}

func BenchmarkSnapshotDecode(b *testing.B) {
	payload := repeatJSONArray(b, readTypedFixture(b, "snapshot-resource-26.0.json"), 100)
	b.Run("Interface", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkSnapshots = interfaceSnapshots(b, payload, true)
		}
	})
	b.Run("Typed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkSnapshots = typedSnapshots(b, payload, true)
		}
	})
}

func BenchmarkDatasetDecode(b *testing.B) {
	payload := repeatJSONArray(b, readTypedFixture(b, "dataset-resource-26.0.json"), 100)
	b.Run("Interface", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkDatasets = interfaceDatasets(b, payload, true)
		}
	})
	b.Run("Typed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkDatasets = typedDatasets(b, payload, true)
		}
	})
}

func FuzzTypedSnapshotDecodeMatchesInterface(f *testing.F) {
	f.Add(readTypedFixture(f, "snapshot-resource-26.0.json"))
	f.Add(readTypedFixture(f, "pool-snapshot-26.0.json"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		var generic []interface{}
		if err := json.Unmarshal(payload, &generic); err != nil {
			return
		}
		var typed []*rawSnapshot
		if err := json.Unmarshal(payload, &typed); err != nil {
			return
		}
		got := rawSnapshotsToSnapshots(typed, true)
		want := make([]*Snapshot, 0, len(generic))
		for _, item := range generic {
			snapshot, err := parseSnapshot(item)
			if err != nil {
				continue
			}
			snapshot.ResourceQuery = true
			want = append(want, snapshot)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("typed snapshot decode diverged from interface decode")
		}
	})
}

func FuzzTypedDatasetDecodeMatchesInterface(f *testing.F) {
	f.Add(readTypedFixture(f, "dataset-resource-26.0.json"))
	f.Add(readTypedFixture(f, "dataset-origins-26.0.json"))
	f.Add(readTypedFixture(f, "dataset-list-26.0.json"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		var generic []interface{}
		if err := json.Unmarshal(payload, &generic); err != nil {
			return
		}
		var typed []*rawDataset
		if err := json.Unmarshal(payload, &typed); err != nil {
			return
		}
		got := rawDatasetsToDatasets(typed, false)
		want := make([]*Dataset, 0, len(generic))
		for _, item := range generic {
			dataset, err := parseDataset(item)
			if err != nil {
				continue
			}
			want = append(want, dataset)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("typed dataset decode diverged from interface decode")
		}
	})
}
