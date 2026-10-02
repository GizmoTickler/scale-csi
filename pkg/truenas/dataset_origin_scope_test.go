package truenas

import (
	"context"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// originScopeServer answers like a TrueNAS 26.0 appliance whose only clone of
// tank/k8s/volumes/source's snapshot lives OUTSIDE the CSI parent
// (tank/backups/restore). pool.dataset.query, filtered to the parent, cannot
// see it; zfs.resource.query of the pool root (live shape: origin as
// {raw, source, value}, "none" for a non-clone) can.
func originScopeServer(t *testing.T, resourceFails bool, resourceParams chan<- map[string]interface{}) *Client {
	t.Helper()
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case datasetResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				if options["get_children"] != true {
					// The capability probe: the pool root alone.
					resp.Result = []interface{}{map[string]interface{}{"name": "tank", "pool": "tank", "type": "FILESYSTEM",
						"properties": map[string]interface{}{"origin": map[string]interface{}{"raw": "none", "source": nil, "value": nil}}}}
					break
				}
				resourceParams <- options
				if resourceFails {
					resp.Error = &rpcError{Code: -32001, Message: "Method call error"}
					break
				}
				resp.Result = []interface{}{
					map[string]interface{}{"name": "tank", "pool": "tank", "type": "FILESYSTEM",
						"properties": map[string]interface{}{"origin": map[string]interface{}{"raw": "none", "source": nil, "value": nil}}},
					map[string]interface{}{"name": "tank/k8s/volumes/source", "pool": "tank", "type": "VOLUME",
						"properties": map[string]interface{}{"origin": map[string]interface{}{"raw": "none", "source": nil, "value": nil}}},
					map[string]interface{}{"name": "tank/backups/restore", "pool": "tank", "type": "FILESYSTEM",
						"properties": map[string]interface{}{"origin": map[string]interface{}{
							"raw": "tank/k8s/volumes/source@snap-1", "source": nil, "value": "tank/k8s/volumes/source@snap-1"}}},
				}
			case "pool.dataset.query":
				resp.Result = []interface{}{map[string]interface{}{"id": "tank/k8s/volumes/source", "name": "tank/k8s/volumes/source",
					"origin": map[string]interface{}{"value": "", "parsed": "", "rawvalue": ""}}}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	t.Cleanup(mock.close)
	return newSnapshotTestClient(t, server.URL)
}

// A clone outside the CSI parent is a dependency: the origin scan covers the
// whole pool through zfs.resource.query, asking for the origin property only.
func TestDatasetHasDependentClonesSeesACloneOutsideTheParent(t *testing.T) {
	params := make(chan map[string]interface{}, 2)
	client := originScopeServer(t, false, params)

	hasClones, err := client.DatasetHasDependentClones(context.Background(), "tank/k8s/volumes/source")
	require.NoError(t, err)
	assert.True(t, hasClones, "a clone outside the parent must be seen")
	var options map[string]interface{}
	select {
	case options = <-params:
	default:
		t.Fatal("the origin scan did not use zfs.resource.query on the pool")
	}
	assert.Equal(t, []interface{}{"tank"}, options["paths"], "the scan is the origin's whole pool")
	assert.Equal(t, true, options["get_children"])
	assert.Equal(t, []interface{}{"origin"}, options["properties"])
	assert.Nil(t, options["get_user_properties"], "no user properties are materialised")

	clones, err := client.SnapshotDependentClones(context.Background(), "tank/k8s/volumes/source@snap-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"tank/backups/restore"}, clones)

	none, err := client.SnapshotDependentClones(context.Background(), "tank/k8s/volumes/source@snap-2")
	require.NoError(t, err)
	assert.Empty(t, none, "scope is still the exact snapshot")
}

// A failed pool-wide scan is an error, never a narrower answer.
func TestDatasetHasDependentClonesFailsClosedWhenThePoolScanFails(t *testing.T) {
	params := make(chan map[string]interface{}, 1)
	client := originScopeServer(t, true, params)
	_, err := client.DatasetHasDependentClones(context.Background(), "tank/k8s/volumes/source")
	require.Error(t, err)
}

// The mock's origin scan has the client's scope: the origin's whole pool, or
// only the CSI parent without the resource API; never another pool.
func TestMockOriginScanScope(t *testing.T) {
	ctx := context.Background()
	m := NewMockClient()
	for _, name := range []string{"tank/k8s/volumes/source", "tank/backups/restore", "other/restore"} {
		_, err := m.DatasetCreate(ctx, &DatasetCreateParams{Name: name, Type: "FILESYSTEM"})
		require.NoError(t, err)
	}
	m.Datasets["other/restore"].Origin = DatasetProperty{Value: "tank/k8s/volumes/source@snap-1", Parsed: "tank/k8s/volumes/source@snap-1"}

	has, err := m.DatasetHasDependentClones(ctx, "tank/k8s/volumes/source")
	require.NoError(t, err)
	assert.False(t, has, "a dataset in another pool cannot be a clone of this one")

	m.Datasets["tank/backups/restore"].Origin = DatasetProperty{Value: "tank/k8s/volumes/source@snap-1", Parsed: "tank/k8s/volumes/source@snap-1"}
	has, err = m.DatasetHasDependentClones(ctx, "tank/k8s/volumes/source")
	require.NoError(t, err)
	assert.True(t, has, "same pool, other parent: seen")
	clones, err := m.SnapshotDependentClones(ctx, "tank/k8s/volumes/source@snap-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"tank/backups/restore"}, clones)

	m.DisableResourceQuery = true
	has, err = m.DatasetHasDependentClones(ctx, "tank/k8s/volumes/source")
	require.NoError(t, err)
	assert.False(t, has, "without the resource API only the parent is scanned")
}
