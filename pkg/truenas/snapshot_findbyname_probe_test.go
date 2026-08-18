package truenas

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P-1: SnapshotFindByName's 26.0 leg no longer pays a full recursive
// zfs.resource.snapshot.query scan (~650ms, >1MB at 576 snapshots, linear in
// snapshot count — live-measured on nas01, 26.0.0-BETA.2) on every call. It
// probes server-side with pool.snapshot.query first (~330ms flat, ~300 bytes,
// live-verified for reads on 26.0.0-BETA.2) and only then issues ONE targeted
// zfs.resource.snapshot.query (~2ms, accepts multiple paths) for the hits.
//
// These tests pin the three wire sequences:
//   - hit:   probe → one targeted (non-recursive) resource query, full decode
//   - miss:  probe → NOTHING (no resource scan at all)
//   - error: probe fails → the pre-P-1 recursive scan, unchanged behavior
//
// The capability probe (zfs.resource.snapshot.query with empty paths) is not a
// read and is excluded from the counts, matching snapshot_projection_test.go.

// findByNameProbeAppliance is a fake 26.0 appliance for the P-1 probe tests.
//
//   - zfs.resource.snapshot.query with empty paths (the capability probe)
//     answers [] so the client takes the 26.0 leg.
//   - pool.snapshot.query responses/errors are supplied per test; the probe's
//     params are captured for wire-shape assertions.
//   - Targeted/recursive zfs.resource.snapshot.query options are captured and
//     counted; the rows returned are supplied per test.
type findByNameProbeAppliance struct {
	probeParams     chan []interface{}
	resourceOptions chan map[string]interface{}
	resourceReads   atomic.Int32
	recursiveReads  atomic.Int32

	poolQueryResult interface{}
	poolQueryError  *rpcError
	resourceRows    []interface{}
}

func (a *findByNameProbeAppliance) handler() func(*websocket.Conn) {
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
			case "pool.snapshot.query":
				select {
				case a.probeParams <- req.Params:
				default:
				}
				if a.poolQueryError != nil {
					resp.Error = a.poolQueryError
				} else {
					resp.Result = a.poolQueryResult
				}
			case snapshotResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				paths := options["paths"].([]interface{})
				if len(paths) == 0 {
					// Capability probe: not a read.
					resp.Result = []interface{}{}
					break
				}
				a.resourceReads.Add(1)
				if options["recursive"] == true {
					a.recursiveReads.Add(1)
				}
				select {
				case a.resourceOptions <- options:
				default:
				}
				resp.Result = a.resourceRows
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	}
}

func newFindByNameProbeClient(t *testing.T, appliance *findByNameProbeAppliance) *Client {
	t.Helper()
	appliance.probeParams = make(chan []interface{}, 2)
	appliance.resourceOptions = make(chan map[string]interface{}, 2)
	mock := newMockWSServer()
	server := mock.start(appliance.handler())
	t.Cleanup(mock.close)
	return newSnapshotTestClient(t, server.URL)
}

// TestSnapshotFindByNameProbeHitWireShape pins the exact probe request
// (method, filters, options) and the single targeted follow-up read: the probe
// LOCATES (pool.snapshot.query on 26.0 carries no user_properties key, an empty
// properties dict, and createtxg as a STRING — live-verified), and one
// non-recursive zfs.resource.snapshot.query with exactly the hit paths supplies
// the identity properties, through the normal decode including the
// truenas-csi: → scale-csi: namespace fold (prop_ns.go).
func TestSnapshotFindByNameProbeHitWireShape(t *testing.T) {
	appliance := &findByNameProbeAppliance{
		// Live 26.0 probe row shape: no user_properties key at all, properties
		// an empty dict, createtxg a string. Deliberately unsorted so the
		// deterministic-order pick below is not an accident of server order.
		poolQueryResult: []interface{}{
			map[string]interface{}{
				"name":          "tank/k8s/volumes/pvc-b@target-snap",
				"dataset":       "tank/k8s/volumes/pvc-b",
				"snapshot_name": "target-snap",
				"createtxg":     "331922",
				"properties":    map[string]interface{}{},
			},
			map[string]interface{}{
				"name":          "tank/k8s/volumes/pvc-a@target-snap",
				"dataset":       "tank/k8s/volumes/pvc-a",
				"snapshot_name": "target-snap",
				"createtxg":     "331921",
				"properties":    map[string]interface{}{},
			},
		},
		resourceRows: []interface{}{
			map[string]interface{}{
				"name":          "tank/k8s/volumes/pvc-a@target-snap",
				"snapshot_name": "target-snap",
				"dataset":       "tank/k8s/volumes/pvc-a",
				"pool":          "tank",
				"type":          "SNAPSHOT",
				"createtxg":     float64(331921),
				"properties": map[string]interface{}{
					"used":     map[string]interface{}{"value": float64(4096), "raw": "4096"},
					"creation": map[string]interface{}{"value": float64(1754150400), "raw": "1754150400"},
				},
				"user_properties": map[string]interface{}{
					"truenas-csi:managed_resource": "true",
				},
			},
			map[string]interface{}{
				"name":            "tank/k8s/volumes/pvc-b@target-snap",
				"snapshot_name":   "target-snap",
				"dataset":         "tank/k8s/volumes/pvc-b",
				"properties":      map[string]interface{}{},
				"user_properties": map[string]interface{}{},
			},
			// A row whose short name does NOT match must be filtered client-side.
			map[string]interface{}{
				"name":            "tank/k8s/volumes/pvc-a@wrong-name",
				"snapshot_name":   "wrong-name",
				"dataset":         "tank/k8s/volumes/pvc-a",
				"properties":      map[string]interface{}{},
				"user_properties": map[string]interface{}{},
			},
		},
	}
	client := newFindByNameProbeClient(t, appliance)

	snap, err := client.SnapshotFindByName(context.Background(), "tank/k8s/volumes", "target-snap")
	require.NoError(t, err)
	require.NotNil(t, snap)

	// The probe's exact wire shape.
	probe := <-appliance.probeParams
	require.Len(t, probe, 2)
	assert.Equal(t, []interface{}{
		[]interface{}{"dataset", "^", "tank/k8s/volumes/"},
		[]interface{}{"snapshot_name", "=", "target-snap"},
	}, probe[0], "P-1 probe filters: parent-scoped dataset prefix plus EXACT snapshot_name equality (not the legacy id regex)")
	assert.Equal(t, map[string]interface{}{
		"select": []interface{}{"name", "dataset", "snapshot_name", "createtxg"},
	}, probe[1], "P-1 probe options: the live-verified ~300-byte select projection")

	// Exactly ONE targeted resource read, with exactly the hit paths.
	assert.Equal(t, int32(1), appliance.resourceReads.Load(), "probe hits cost exactly one targeted resource query")
	assert.Equal(t, int32(0), appliance.recursiveReads.Load(), "the recursive scan must not run on the probe path")
	options := <-appliance.resourceOptions
	assert.Equal(t, []interface{}{
		"tank/k8s/volumes/pvc-a@target-snap",
		"tank/k8s/volumes/pvc-b@target-snap",
	}, options["paths"], "all probe hits ride in ONE call, in deterministic order")
	assert.Equal(t, false, options["recursive"])
	assert.Equal(t, true, options["get_user_properties"])
	assert.Equal(t, []interface{}{"used", "creation"}, options["properties"],
		"the targeted read carries the pinned snapshot projection (N-10)")

	// First match in deterministic (ID) order, with the FULL snapshot object:
	// user properties decode through the normal path including the namespace fold.
	assert.Equal(t, "tank/k8s/volumes/pvc-a@target-snap", snap.ID)
	assert.Equal(t, "target-snap", snap.Name)
	assert.Equal(t, uint64(331921), snap.CreateTXG)
	assert.Equal(t, int64(4096), snap.GetSnapshotSize())
	assert.Equal(t, "true", snap.UserProperties["scale-csi:managed_resource"].Value,
		"callers read UserProperties for identity checks; the legacy wire key must fold onto the canonical namespace")
	assert.NotContains(t, snap.UserProperties, "truenas-csi:managed_resource",
		"legacy key must be folded out of UserProperties (prop_ns.go)")
}

// TestSnapshotFindByNameProbeMissSkipsResourceScan pins the common fresh-create
// case: zero probe hits mean nil, nil with NO zfs.resource.snapshot.query read
// at all — the whole point of P-1.
func TestSnapshotFindByNameProbeMissSkipsResourceScan(t *testing.T) {
	appliance := &findByNameProbeAppliance{
		poolQueryResult: []interface{}{},
	}
	client := newFindByNameProbeClient(t, appliance)

	snap, err := client.SnapshotFindByName(context.Background(), "tank/k8s/volumes", "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, snap)

	probe := <-appliance.probeParams
	assert.Equal(t, []interface{}{
		[]interface{}{"dataset", "^", "tank/k8s/volumes/"},
		[]interface{}{"snapshot_name", "=", "nonexistent"},
	}, probe[0])
	assert.Equal(t, int32(0), appliance.resourceReads.Load(),
		"a probe miss must not issue any zfs.resource.snapshot.query read")
}

// TestSnapshotFindByNameProbeErrorFallsBackToScan pins the safety net: if the
// probe call errors for ANY reason (here method-not-found, as some future build
// might return), the pre-P-1 recursive scan runs unchanged and produces the old
// behavior — including the parent-dataset exclusion and the namespace fold.
func TestSnapshotFindByNameProbeErrorFallsBackToScan(t *testing.T) {
	appliance := &findByNameProbeAppliance{
		poolQueryError: &rpcError{Code: -32601, Message: "Method not found"},
		resourceRows: []interface{}{
			// ON the parent itself: must stay excluded, exactly as before P-1.
			map[string]interface{}{"name": "tank/k8s/volumes@target-snap", "snapshot_name": "target-snap", "dataset": "tank/k8s/volumes", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{}},
			// Outside the parent: excluded.
			map[string]interface{}{"name": "tank/k8s/volumes-other@target-snap", "snapshot_name": "target-snap", "dataset": "tank/k8s/volumes-other", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{}},
			// The real match, with a legacy-namespaced identity stamp.
			map[string]interface{}{"name": "tank/k8s/volumes/pvc-a@target-snap", "snapshot_name": "target-snap", "dataset": "tank/k8s/volumes/pvc-a", "properties": map[string]interface{}{}, "user_properties": map[string]interface{}{"truenas-csi:managed_resource": "true"}},
		},
	}
	client := newFindByNameProbeClient(t, appliance)

	snap, err := client.SnapshotFindByName(context.Background(), "tank/k8s/volumes", "target-snap")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, "tank/k8s/volumes/pvc-a@target-snap", snap.ID)
	assert.Equal(t, "true", snap.UserProperties["scale-csi:managed_resource"].Value)
	assert.NotContains(t, snap.UserProperties, "truenas-csi:managed_resource")

	assert.Equal(t, int32(1), appliance.resourceReads.Load(), "fallback is exactly the old single recursive scan")
	assert.Equal(t, int32(1), appliance.recursiveReads.Load())
	options := <-appliance.resourceOptions
	assert.Equal(t, []interface{}{"tank/k8s/volumes"}, options["paths"])
	assert.Equal(t, true, options["recursive"])
	assert.Equal(t, true, options["get_user_properties"])
	assert.Equal(t, []interface{}{"used", "creation"}, options["properties"])
}
