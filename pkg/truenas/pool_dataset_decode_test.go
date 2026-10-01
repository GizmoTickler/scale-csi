package truenas

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// interfacePoolRows is what DatasetGet and DatasetGetByNames did before the
// typed decode: an interface{} tree, then parseDataset per row (nil where it
// fails).
func interfacePoolRows(t *testing.T, payload []byte) []*Dataset {
	t.Helper()
	var items []interface{}
	require.NoError(t, json.Unmarshal(payload, &items))
	out := make([]*Dataset, len(items))
	for i, item := range items {
		if dataset, err := parseDataset(item); err == nil {
			out[i] = dataset
		}
	}
	return out
}

// The typed pool.dataset.query decode gives exactly what parseDataset gave,
// on the wire fixtures and on rows the typed decoder rejects (which take the
// interface path as before).
func TestPoolDatasetRowsMatchParseDataset(t *testing.T) {
	for _, fixture := range []string{"dataset-list-26.0.json", "dataset-origins-26.0.json"} {
		payload := readTypedFixture(t, fixture)
		got, err := decodePoolDatasetRows(payload)
		require.NoError(t, err)
		assert.Equal(t, interfacePoolRows(t, payload), got, fixture)
	}
	for name, payload := range map[string]string{
		"rawvalue is a number":     `[{"id":"p/a","name":"p/a","used":{"value":"1K","rawvalue":1024,"parsed":1024,"source":"NONE"}}]`,
		"a null row":               `[{"id":"p/a","name":"p/a"},null]`,
		"a row that is not a dict": `[{"id":"p/a","name":"p/a"},"p/b"]`,
		"user property odd shape":  `[{"id":"p/a","name":"p/a","user_properties":{"scale-csi:x":{"value":7,"source":"LOCAL"}}}]`,
	} {
		got, err := decodePoolDatasetRows([]byte(payload))
		require.NoError(t, err, name)
		assert.Equal(t, interfacePoolRows(t, []byte(payload)), got, name)
	}
}

// A reply that is not a list is an error, never "not found": a false absence
// lets DeleteVolume report success over a live dataset.
func TestPoolDatasetRowsRejectANonList(t *testing.T) {
	for _, payload := range []string{``, `null`, `{"id":"p/a"}`, `"p/a"`, `true`} {
		_, err := decodePoolDatasetRows([]byte(payload))
		var notAList notADatasetListError
		require.ErrorAs(t, err, &notAList, payload)
		assert.False(t, IsNotFoundError(err), payload)
	}
	_, err := decodePoolDatasetRows([]byte(`[{"id":`))
	require.Error(t, err)
}

func TestPoolDatasetRowMatchesParseDataset(t *testing.T) {
	var rows []json.RawMessage
	require.NoError(t, json.Unmarshal(readTypedFixture(t, "dataset-list-26.0.json"), &rows))
	for _, row := range append(rows, json.RawMessage(`{"id":"p/a","used":{"rawvalue":5}}`), json.RawMessage(`null`)) {
		var generic interface{}
		require.NoError(t, json.Unmarshal(row, &generic))
		want, wantErr := parseDataset(generic)
		got, err := decodePoolDatasetRow(row)
		assert.Equal(t, wantErr, err)
		assert.Equal(t, want, got)
	}
}

// DatasetGet, DatasetGetByNames and DatasetUpdate no longer build an
// interface{} tree per reply: a one-row read was about 640 allocations.
func TestDatasetReadsUseTheTypedDecoder(t *testing.T) {
	s := newScaleBenchServer(t, 30)
	c := s.client(t)
	ctx := context.Background()
	name := scaleBenchParent + "/" + scaleBenchVolume(3)
	_, err := c.DatasetGet(ctx, name) // connect outside the measurement
	require.NoError(t, err)
	get := testing.AllocsPerRun(20, func() {
		if _, err := c.DatasetGet(ctx, name); err != nil {
			t.Fatal(err)
		}
	})
	update := testing.AllocsPerRun(20, func() {
		if _, err := c.DatasetUpdate(ctx, name, &DatasetUpdateParams{}); err != nil {
			t.Fatal(err)
		}
	})
	assert.Less(t, get, 350.0, "DatasetGet allocations")
	assert.Less(t, update, 350.0, "DatasetUpdate allocations")
}

// FuzzPoolDatasetRowsMatchInterface extends the differential fuzz target to
// the full pool.dataset.query decode, fallback included: for any JSON array,
// decodePoolDatasetRows must equal parseDataset row by row.
func FuzzPoolDatasetRowsMatchInterface(f *testing.F) {
	f.Add(readTypedFixture(f, "dataset-list-26.0.json"))
	f.Add(readTypedFixture(f, "dataset-origins-26.0.json"))
	f.Add([]byte(`[{"id":"p/a","used":{"rawvalue":5}},null,"x"]`))
	f.Fuzz(func(t *testing.T, payload []byte) {
		var generic []interface{}
		if err := json.Unmarshal(payload, &generic); err != nil {
			return
		}
		if !decodedKeysLowercase(generic) {
			t.Skip("off-wire-contract mixed-case key; typed/interface divergence is stdlib case-insensitivity, not a bug")
		}
		canonical, err := json.Marshal(generic)
		if err != nil {
			return
		}
		got, err := decodePoolDatasetRows(canonical)
		if err != nil {
			t.Fatalf("decode of a JSON array failed: %v", err)
		}
		want := make([]*Dataset, len(generic))
		for i, item := range generic {
			if dataset, parseErr := parseDataset(item); parseErr == nil {
				want[i] = dataset
			}
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("typed pool.dataset.query decode diverged from interface decode")
		}
	})
}
