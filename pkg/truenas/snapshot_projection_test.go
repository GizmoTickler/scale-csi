package truenas

import (
	"context"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// N-10: the same gate as dataset_projection_test.go, applied to the SNAPSHOT
// reads — which had a projection and no model at all.
//
//	layer 1  the projection the client SENDS is pinned (assertSnapshotQueryProjection)
//	layer 2  a fake appliance decides what to return FROM the request: projected
//	         properties appear, unprojected ones do not
//	layer 3  projectSnapshotLikeResourceQuery gives MockClient the same behavior
//
// The anchor is snapshotResourceQueryProjectionWant — a hardcoded literal, not
// derived from the code under test — so renaming the projected properties fails
// the wire assertion FIRST, before the appliance's decision is consulted.

// snapshotResourceQueryProjectionWant is the projection every
// zfs.resource.snapshot.query read must carry. Written out in full so a silent
// drop is a failing test rather than a self-consistent tautology.
var snapshotResourceQueryProjectionWant = []string{
	// used: GetSnapshotSize (CSI snapshot responses, reaper reclaimable bytes).
	"used",
	// creation: GetCreationTime (scheduled-snapshot ownership predicate, every
	// age gate). Each of those FAILS CLOSED without it.
	"creation",
}

func assertSnapshotQueryProjection(t *testing.T, got interface{}) {
	t.Helper()
	want := make([]interface{}, 0, len(snapshotResourceQueryProjectionWant))
	for _, property := range snapshotResourceQueryProjectionWant {
		want = append(want, property)
	}
	assert.Equal(t, want, got,
		"the zfs.resource.snapshot.query projection decides which PROPERTIES exist on the wire (N-10)")
}

// snapshotQueryAppliance is a fake TrueNAS 26.0 snapshot resource API. It
// returns the top-level core unconditionally (that is what the shape says: the
// projection selects PROPERTIES, not top-level fields) and a property only when
// the request projected it. createtxg is emitted unless the test asks for the
// UNPROBED "absent" shape.
//
// It is deliberately NOT told what the driver asks for — it reads the request.
func snapshotQueryAppliance(t *testing.T, seen chan<- []interface{}, withCreateTXG bool) func(*websocket.Conn) {
	t.Helper()
	return func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case snapshotResourceQueryMethod:
				options, _ := req.Params[0].(map[string]interface{})
				projected, _ := options["properties"].([]interface{})
				paths, _ := options["paths"].([]interface{})
				// The client's capability PROBE calls this method with no paths and
				// no properties; it is not a read and must not be mistaken for one.
				if len(paths) > 0 {
					select {
					case seen <- projected:
					default:
					}
				}
				requested := func(name string) bool {
					for _, item := range projected {
						if item == name {
							return true
						}
					}
					return false
				}
				properties := map[string]interface{}{}
				if requested("used") {
					properties["used"] = map[string]interface{}{"value": float64(4096), "raw": "4096"}
				}
				if requested("creation") {
					properties["creation"] = map[string]interface{}{"value": float64(1754150400), "raw": "1754150400"}
				}
				if requested("clones") {
					properties["clones"] = map[string]interface{}{"value": "flashstor/parent/clone-1"}
				}
				row := map[string]interface{}{
					"id":            "flashstor/parent/gf1v-src@snap-1",
					"name":          "flashstor/parent/gf1v-src@snap-1",
					"snapshot_name": "snap-1",
					"dataset":       "flashstor/parent/gf1v-src",
					"pool":          "flashstor",
					"type":          "SNAPSHOT",
					"properties":    properties,
					"user_properties": map[string]interface{}{
						"truenas-csi:managed_resource": "true",
					},
				}
				if withCreateTXG {
					// Top-level, NOT a property: unaffected by the projection.
					row["createtxg"] = "8412"
				}
				resp.Result = []interface{}{row}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	}
}

func snapshotProjectionClient(t *testing.T, withCreateTXG bool) (*Client, chan []interface{}) {
	t.Helper()
	mock := newMockWSServer()
	seen := make(chan []interface{}, 4)
	server := mock.start(snapshotQueryAppliance(t, seen, withCreateTXG))
	t.Cleanup(mock.close)
	client := newSnapshotTestClient(t, server.URL)
	// The client probes for zfs.resource.snapshot.query on first use; this
	// appliance answers it, so every read below takes the 26.0 resource path —
	// the only one that carries a projection at all.
	return client, seen
}

// TestSnapshotResourceQueryProjectionIsPinned is the wire-level anchor: every
// snapshot read must send the measured projection, and what the appliance
// returns follows from that request.
//
// PRE-FIX PROOF: rename the entries of snapshotResourceQueryProperties (e.g. to
// bogus1/bogus2) and this FAILS on the projection assertion, before any decoded
// value is examined — the same non-circularity the dataset gate has.
func TestSnapshotResourceQueryProjectionIsPinned(t *testing.T) {
	for _, tc := range []struct {
		name string
		read func(*Client) error
	}{
		{"SnapshotGet", func(c *Client) error {
			_, err := c.SnapshotGet(context.Background(), "flashstor/parent/gf1v-src@snap-1")
			return err
		}},
		{"SnapshotList", func(c *Client) error {
			_, err := c.SnapshotList(context.Background(), "flashstor/parent/gf1v-src")
			return err
		}},
		{"SnapshotListAll", func(c *Client) error {
			_, err := c.SnapshotListAll(context.Background(), "flashstor/parent", 0, 0)
			return err
		}},
		{"SnapshotFindByName", func(c *Client) error {
			_, err := c.SnapshotFindByName(context.Background(), "flashstor/parent", "snap-1")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, seen := snapshotProjectionClient(t, true)
			require.NoError(t, tc.read(client))
			assertSnapshotQueryProjection(t, <-seen)
		})
	}
}

// TestSnapshotProjectionCarriesTheFieldsGuardsRead is the substance: the fields
// the driver's guards read must survive a real projected read.
//
// createtxg is top-level, so it is NOT the projection's doing — that is exactly
// why it is called out: the assumption that the snapshot resource API returns it
// is UNPROBED (the enumeration that includes createtxg was measured on the
// DATASET resource API). Drill step 1d measures it; this test pins that the
// decoder reads it when it IS present.
func TestSnapshotProjectionCarriesTheFieldsGuardsRead(t *testing.T) {
	client, seen := snapshotProjectionClient(t, true)

	snap, err := client.SnapshotGet(context.Background(), "flashstor/parent/gf1v-src@snap-1")
	require.NoError(t, err)
	assertSnapshotQueryProjection(t, <-seen)

	assert.Equal(t, uint64(8412), snap.CreateTXG,
		"tombstone identity matching and promote refusal read THIS field")
	assert.Equal(t, int64(1754150400), snap.GetCreationTime(),
		"the scheduled-snapshot ownership predicate and every age gate read creation")
	assert.Equal(t, int64(4096), snap.GetSnapshotSize(), "used is projected and decodes")
	assert.Equal(t, "snap-1", snap.Name)
	assert.Equal(t, "flashstor/parent/gf1v-src", snap.Dataset)
	require.Contains(t, snap.UserProperties, "scale-csi:managed_resource",
		"get_user_properties is always true; user properties are not part of the projection")
	assert.NotContains(t, snap.UserProperties, "truenas-csi:managed_resource",
		"legacy-spelled wire key must be folded onto the canonical namespace")

	// And the negative half: a property the driver does NOT project is absent, so
	// nothing may quietly grow a dependency on it. GetClones degrades to the
	// authoritative dataset-origin scan by design.
	assert.Empty(t, snap.GetClones(),
		"clones is deliberately unprojected (26.0 does not deliver it on either snapshot read)")
}

// TestSnapshotGuardsFailClosedWithoutCreateTXG pins what happens if the UNPROBED
// assumption is wrong. If zfs.resource.snapshot.query omits createtxg it decodes
// to 0, and both readers must degrade CLOSED — promote refuses, tombstone
// identity declines to match — never the reverse.
func TestSnapshotGuardsFailClosedWithoutCreateTXG(t *testing.T) {
	client, seen := snapshotProjectionClient(t, false)

	snap, err := client.SnapshotGet(context.Background(), "flashstor/parent/gf1v-src@snap-1")
	require.NoError(t, err)
	assertSnapshotQueryProjection(t, <-seen)

	assert.Zero(t, snap.CreateTXG,
		"an absent top-level createtxg decodes to the zero value — the shape the guards must survive")
	assert.Equal(t, int64(1754150400), snap.GetCreationTime(),
		"the PROJECTED properties are unaffected: this is a top-level-field question, not a projection one")
}

// TestProjectSnapshotLikeResourceQueryModelsTheWire is the projection MODEL's own
// gate: given the current projection it preserves what the wire carries, and
// given a narrower one it strips exactly the properties that would then be
// absent — while leaving every top-level field alone.
func TestProjectSnapshotLikeResourceQueryModelsTheWire(t *testing.T) {
	full := &Snapshot{
		ID: "flashstor/parent/gf1v-src@snap-1", Name: "snap-1",
		Dataset: "flashstor/parent/gf1v-src", Pool: "flashstor", Type: "SNAPSHOT",
		CreateTXG: 8412,
		Properties: map[string]interface{}{
			"used":                         map[string]interface{}{"value": float64(4096)},
			"creation":                     map[string]interface{}{"value": float64(1754150400)},
			"clones":                       map[string]interface{}{"value": "flashstor/parent/clone-1"},
			"truenas-csi:managed_resource": map[string]interface{}{"value": "true", "source": "LOCAL"},
		},
		UserProperties: map[string]UserProperty{"truenas-csi:managed_resource": {Value: "true"}},
	}

	current := projectSnapshotLikeResourceQuery(full, snapshotResourceQueryProperties)
	assert.Equal(t, int64(4096), current.GetSnapshotSize(), "used is projected")
	assert.Equal(t, int64(1754150400), current.GetCreationTime(), "creation is projected")
	assert.Empty(t, current.GetClones(), "clones is not projected: the model must not deliver it")
	assert.Equal(t, uint64(8412), current.CreateTXG, "top-level fields are not the projection's business")
	assert.Contains(t, current.Properties, "truenas-csi:managed_resource",
		"user properties ride on get_user_properties, not on the projection")
	assert.Contains(t, full.Properties, "clones", "the input must not be mutated")

	narrowed := projectSnapshotLikeResourceQuery(full, []string{"used"})
	assert.Equal(t, int64(4096), narrowed.GetSnapshotSize())
	assert.Zero(t, narrowed.GetCreationTime(),
		"drop creation from the projection and every age gate reads zero — the D-3 shape, on snapshots")
}

// TestMockSnapshotProjectionModeStripsUnprojectedProperties gives the driver
// suite the same teeth the dataset side got: with the mode on, a mock snapshot
// carries only what a real projected read delivers.
func TestMockSnapshotProjectionModeStripsUnprojectedProperties(t *testing.T) {
	ctx := context.Background()
	mock := NewMockClient()
	mock.ModelQueryProjection = true
	_, err := mock.DatasetCreate(ctx, &DatasetCreateParams{Name: "pool/parent/src", Type: "FILESYSTEM"})
	require.NoError(t, err)
	snap, err := mock.SnapshotCreate(ctx, "pool/parent/src", "snap-1", nil)
	require.NoError(t, err)
	mock.SetSnapshotCreationTime(snap.ID, 1754150400)
	mock.SetSnapshotUsedBytes(snap.ID, 4096)
	mock.Snapshots[snap.ID].Properties["clones"] = map[string]interface{}{"value": "pool/parent/clone-1"}

	read, err := mock.SnapshotGet(ctx, snap.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(1754150400), read.GetCreationTime(), "projected: survives")
	assert.Equal(t, int64(4096), read.GetSnapshotSize(), "projected: survives")
	assert.Empty(t, read.GetClones(), "unprojected: the mock must not deliver what the wire would not")
	assert.NotZero(t, read.CreateTXG, "top-level: unaffected by the projection")
	assert.Contains(t, mock.Snapshots[snap.ID].Properties, "clones", "the stored object is untouched")

	// The UNPROBED half, modeled on demand.
	mock.ModelSnapshotCreateTXGAbsent = true
	absent, err := mock.SnapshotGet(ctx, snap.ID)
	require.NoError(t, err)
	assert.Zero(t, absent.CreateTXG,
		"if zfs.resource.snapshot.query does not carry createtxg, this is what the driver sees")
}
