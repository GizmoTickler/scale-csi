package truenas

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
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

// TestTypedDecodeGoldenDeepEquality pins typed-vs-interface decode equality on
// wire-shaped fixtures. Snapshot fixture provenance (documented here, on the
// consuming test, exactly like snapshot-resource-stamped-live-26.0.json does):
//
//   - snapshot-resource-26.0.json: shape live-verified 2026-08-18 against
//     TrueNAS 26.0.0-BETA.2 (nas01) zfs.resource.snapshot.query. On the wire,
//     user_properties are BARE STRINGS (never {value,source} objects — no
//     source info exists for snapshot user properties on 26.0, even with
//     get_source:true), native properties come back as {"raw": "...",
//     "source": null, "value": <num>} entries, and `properties` is JSON null
//     when the requested properties list is empty. The truenas-csi:* key
//     spellings are realistic legacy-era stamps and exercise the decode-time
//     namespace fold (prop_ns.go).
//   - pool-snapshot-26.0.json: shape live-verified 2026-08-18 against TrueNAS
//     26.0.0-BETA.2 (nas01) pool.snapshot.query. On the wire, createtxg is a
//     STRING (e.g. "4523837"), there is NO user_properties key at all,
//     properties is an empty dict {}, holds is an empty dict {}, and BOTH `id`
//     and `name` carry the FULL dataset@snapshot path — the short name lives
//     only in `snapshot_name`.
func TestTypedDecodeGoldenDeepEquality(t *testing.T) {
	t.Run("snapshot_resource", func(t *testing.T) {
		payload := readTypedFixture(t, "snapshot-resource-26.0.json")
		require.Equal(t, interfaceSnapshots(t, payload, true), typedSnapshots(t, payload, true))

		// Pin the live 26.0 wire facts the fixture now carries.
		snapshots := typedSnapshots(t, payload, true)
		require.Len(t, snapshots, 2)
		assert.Equal(t, UserProperty{Value: "from-user-properties"},
			snapshots[0].UserProperties["scale-csi:managed"],
			"bare-string user property must decode with empty Source and fold to the canonical namespace")
		assert.NotContains(t, snapshots[0].UserProperties, "truenas-csi:managed")
		assert.Equal(t, int64(2723840), snapshots[0].GetSnapshotSize())
		assert.Equal(t, int64(1754693322), snapshots[0].GetCreationTime())
		assert.NotNil(t, snapshots[1].Properties, "wire `properties: null` must decode to an empty map, not nil")
		assert.Empty(t, snapshots[1].Properties)
	})
	t.Run("pool_snapshot", func(t *testing.T) {
		payload := readTypedFixture(t, "pool-snapshot-26.0.json")
		require.Equal(t, interfaceSnapshots(t, payload, false), typedSnapshots(t, payload, false))

		// Pin the live 26.0 wire facts the fixture now carries.
		snapshots := typedSnapshots(t, payload, false)
		require.Len(t, snapshots, 1)
		assert.Equal(t, uint64(987656), snapshots[0].CreateTXG, "string createtxg must parse")
		assert.Empty(t, snapshots[0].UserProperties, "pool.snapshot.query returns no user_properties key")
		assert.Empty(t, snapshots[0].Properties)
		// Wire `name` is the FULL dataset@snapshot path (identical to `id`);
		// the decoders must resolve ID from it and take the SHORT name from
		// `snapshot_name`, never surface the full path as the snapshot name.
		assert.Equal(t, "flashstor/csi/volumes/pvc-c@snapshot-c", snapshots[0].ID)
		assert.Equal(t, "snapshot-c", snapshots[0].Name)
		assert.Equal(t, "flashstor/csi/volumes/pvc-c", snapshots[0].Dataset)
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
	// DECODER-TOLERANCE INPUT, NOT A LIVE 26.0 SHAPE. Live probes (2026-08-18,
	// TrueNAS 26.0.0-BETA.2) proved zfs.resource.snapshot.query returns
	// user_properties as BARE STRINGS — {value,source} objects never appear on
	// the wire, so the wire fixture (snapshot-resource-26.0.json) no longer
	// carries them. This inline payload (the pre-2026-08-18 hand-built fixture
	// content) stays here purely to pin that the decoder TOLERATES the object
	// shape: {value,source} user_properties (with the flat-string form mixed
	// in), namespaced keys inside `properties`, user_properties precedence
	// over a same-key properties entry, and the legacy
	// value/rawvalue/parsed/{"$date"} property dicts must all keep decoding —
	// and namespace-folding — exactly as before.
	payload := []byte(`[
	  {
	    "name": "flashstor/csi/volumes/pvc-a@snapshot-a",
	    "snapshot_name": "snapshot-a",
	    "dataset": "flashstor/csi/volumes/pvc-a",
	    "pool": "flashstor",
	    "type": "SNAPSHOT",
	    "createtxg": 987654,
	    "properties": {
	      "used": {
	        "value": 2723840,
	        "raw": "2723840",
	        "source": "LOCAL"
	      },
	      "creation": {
	        "value": 1754693322,
	        "raw": "1754693322",
	        "source": "LOCAL"
	      },
	      "clones": {
	        "value": "flashstor/csi/volumes/clone-a",
	        "source": "LOCAL"
	      },
	      "truenas-csi:managed": {
	        "value": "from-properties",
	        "source": "LOCAL"
	      }
	    },
	    "user_properties": {
	      "truenas-csi:managed": {
	        "value": "from-user-properties",
	        "source": "INHERITED"
	      },
	      "truenas-csi:flat": "flat-value"
	    }
	  },
	  {
	    "id": "flashstor/csi/volumes/pvc-b@snapshot-b",
	    "name": "snapshot-b",
	    "dataset": "flashstor/csi/volumes/pvc-b",
	    "pool": "flashstor",
	    "type": "SNAPSHOT",
	    "createtxg": "987655",
	    "properties": {
	      "used": {
	        "value": 4096,
	        "rawvalue": "4096",
	        "parsed": 4096,
	        "source": "LOCAL"
	      },
	      "creation": {
	        "value": "1754693399",
	        "rawvalue": "1754693399",
	        "parsed": {
	          "$date": 1754693399000
	        },
	        "source": "LOCAL"
	      }
	    },
	    "user_properties": {
	      "truenas-csi:driver_instance": {
	        "value": "cluster-a",
	        "source": "LOCAL"
	      }
	    }
	  }
	]`)
	// The typed decoder must also stay deep-equal to the interface decoder on
	// this off-wire shape (coverage previously provided via the fixture).
	require.Equal(t, interfaceSnapshots(t, payload, true), typedSnapshots(t, payload, true))
	snapshots := typedSnapshots(t, payload, true)
	require.NotEmpty(t, snapshots)
	snapshot := snapshots[0]

	used, ok := snapshot.Properties["used"].(map[string]interface{})
	require.True(t, ok)
	assert.IsType(t, float64(0), used["value"])
	assert.Equal(t, "2723840", used["raw"])
	assert.NotContains(t, used, "rawvalue")
	// Legacy-spelled wire keys fold onto the canonical namespace with the
	// {value,source} shape (object or flat) preserved unchanged.
	assert.Equal(t, UserProperty{Value: "from-user-properties", Source: "INHERITED"},
		snapshot.UserProperties["scale-csi:managed"])
	assert.Equal(t, UserProperty{Value: "flat-value"}, snapshot.UserProperties["scale-csi:flat"])
	assert.NotContains(t, snapshot.UserProperties, "truenas-csi:managed", "legacy key must be folded out of UserProperties")
	assert.NotContains(t, snapshot.UserProperties, "truenas-csi:flat", "legacy key must be folded out of UserProperties")
	assert.Equal(t, int64(2723840), snapshot.GetSnapshotSize())
	assert.Equal(t, int64(1754693322), snapshot.GetCreationTime())

	// Second entry: object-shaped user property with a source, string
	// createtxg, and the legacy value/rawvalue/parsed/{"$date"} dicts.
	require.Len(t, snapshots, 2)
	legacy := snapshots[1]
	assert.Equal(t, UserProperty{Value: "cluster-a", Source: "LOCAL"},
		legacy.UserProperties["scale-csi:driver_instance"])
	assert.NotContains(t, legacy.UserProperties, "truenas-csi:driver_instance", "legacy key must be folded out of UserProperties")
	assert.Equal(t, uint64(987655), legacy.CreateTXG)
	assert.Equal(t, int64(4096), legacy.GetSnapshotSize())
	assert.Equal(t, int64(1754693399), legacy.GetCreationTime())
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

func TestPendingDepthRecorderTracksCallLifecycle(t *testing.T) {
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

	var callbackCount atomic.Int32
	var lastDepth atomic.Int32
	client := &Client{
		config:    cfg,
		pool:      []*Connection{connection},
		semaphore: make(chan struct{}, 1),
		pendingDepthRecorder: func(depth int) {
			callbackCount.Add(1)
			lastDepth.Store(int32(depth))
		},
	}
	connection.client = client

	go func() {
		request := <-connection.writeCh
		request.resultCh <- nil
		connection.pendingMu.RLock()
		pending := connection.pending[request.id]
		connection.pendingMu.RUnlock()
		pending.responseCh <- &rpcResponse{ID: request.id, Result: json.RawMessage(`true`)}
		connection.pendingMu.Lock()
		delete(connection.pending, request.id)
		connection.pendingMu.Unlock()
		connection.pendingDepthChanged()
	}()

	result, err := client.CallWithContext(context.Background(), "test.pending")
	require.NoError(t, err)
	assert.Equal(t, true, result)
	assert.GreaterOrEqual(t, callbackCount.Load(), int32(2), "depth recorder must observe admission and cleanup")
	assert.Equal(t, int32(0), lastDepth.Load())
	assert.Equal(t, 0, client.PendingDepth())
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

// decodedKeysLowercase walks a decoded JSON value and reports whether every
// object key is already lowercase. Go's encoding/json matches struct fields
// CASE-INSENSITIVELY (a "nAme" key fills the Name field), while the legacy
// interface{} parsers do exact map lookups — so mixed-case keys make the two
// paths diverge by stdlib design, not by a decoding bug. TrueNAS emits
// canonical lowercase keys on the wire (typed decode being more tolerant
// off-contract cannot lose data), so the differential property is only claimed
// for wire-shaped inputs. Found by the 2026-07-31 extended fuzz run; the
// mixed-case corpus entries are retained as regression seeds for this guard.
func decodedKeysLowercase(v interface{}) bool {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, child := range val {
			if k != strings.ToLower(k) {
				return false
			}
			if !decodedKeysLowercase(child) {
				return false
			}
		}
	case []interface{}:
		for _, child := range val {
			if !decodedKeysLowercase(child) {
				return false
			}
		}
	}
	return true
}

func FuzzTypedSnapshotDecodeMatchesInterface(f *testing.F) {
	f.Add(readTypedFixture(f, "snapshot-resource-26.0.json"))
	f.Add(readTypedFixture(f, "pool-snapshot-26.0.json"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		var generic []interface{}
		if err := json.Unmarshal(payload, &generic); err != nil {
			return
		}
		if !decodedKeysLowercase(generic) {
			t.Skip("off-wire-contract mixed-case key; typed/interface divergence is stdlib case-insensitivity, not a bug")
		}
		// Canonicalize through the decoded value so the differential property is
		// claimed for JSON VALUES, not raw byte-strings: duplicate object keys
		// (impossible from a JSON-RPC emitter) merge via struct-overlay in typed
		// decode but last-wins-replace in maps — a stdlib representation
		// divergence, not a decoding bug. Found by the 2026-07-31 fuzz run.
		canonical, err := json.Marshal(generic)
		if err != nil {
			return
		}
		var typed []*rawSnapshot
		if err := json.Unmarshal(canonical, &typed); err != nil {
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
		if !decodedKeysLowercase(generic) {
			t.Skip("off-wire-contract mixed-case key; typed/interface divergence is stdlib case-insensitivity, not a bug")
		}
		// Canonicalize through the decoded value (see snapshot target above):
		// claims the differential property for JSON VALUES, ruling out
		// duplicate-key overlay-vs-replace representation divergence.
		canonical, err := json.Marshal(generic)
		if err != nil {
			return
		}
		var typed []*rawDataset
		if err := json.Unmarshal(canonical, &typed); err != nil {
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

// TestLiveCapturedStampedSnapshotFixtureDecodes pins the decode behavior for a
// fixture captured VERBATIM from a live TrueNAS-26.0.0-BETA.2
// zfs.resource.snapshot.query response (2026-08-17, scoped write probe against
// nas01; see the P1 snapshot-handle work). It is the reference capture the
// hand-built snapshot-resource-26.0.json fixture was realigned to on
// 2026-08-18 (fixture-hygiene finding E-1). Two wire facts it proves:
//
//  1. Snapshot user_properties arrive as BARE STRINGS (no {value, source}
//     object and therefore NO source information), so any snapshot-side logic
//     must never rely on UserProperty.Source — the handle stamp trust rule
//     compares short names instead, and this fixture is the reason why.
//  2. An inline SnapshotCreate property map (including the
//     truenas-csi:csi_snapshot_handle stamp) persists and is returned by the
//     targeted, non-recursive read — the fast path the qualified snapshot
//     handles depend on.
func TestLiveCapturedStampedSnapshotFixtureDecodes(t *testing.T) {
	payload := readTypedFixture(t, "snapshot-resource-stamped-live-26.0.json")

	// Interface and typed decoders must agree on the live shape too.
	require.Equal(t, interfaceSnapshots(t, payload, true), typedSnapshots(t, payload, true))

	snapshots := typedSnapshots(t, payload, true)
	require.Len(t, snapshots, 1)
	snap := snapshots[0]

	assert.Equal(t, "flashstor/csi-live-test/pvc-live-a@livetest-1", snap.ID)
	assert.Equal(t, "livetest-1", snap.Name)
	assert.Equal(t, "flashstor/csi-live-test/pvc-live-a", snap.Dataset)

	// The live capture predates the namespace rename, so the wire carries
	// truenas-csi:* stamps; decode folds them onto the canonical namespace.
	handle, ok := snap.UserProperties["scale-csi:csi_snapshot_handle"]
	require.True(t, ok, "inline-created handle stamp must survive the targeted read (folded to canonical)")
	assert.Equal(t, "flashstor/csi-live-test/pvc-live-a@livetest-1", handle.Value)
	// Bare-string wire shape carries no source: it must decode empty, not fail.
	assert.Equal(t, "", handle.Source)
	assert.NotContains(t, snap.UserProperties, "truenas-csi:csi_snapshot_handle", "legacy key must be folded out of UserProperties")

	managed, ok := snap.UserProperties["scale-csi:managed_resource"]
	require.True(t, ok)
	assert.Equal(t, "true", managed.Value)
	assert.NotContains(t, snap.UserProperties, "truenas-csi:managed_resource", "legacy key must be folded out of UserProperties")
}
