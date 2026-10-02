package truenas

import (
	"context"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceProbePaths(t *testing.T) {
	for in, want := range map[string][]string{
		"tank/k8s/volumes":            {"tank"},
		"tank/k8s/volumes/pvc-1@snap": {"tank"},
		"tank@snap":                   {"tank"},
		"/tank/a":                     {"tank"},
		"tank":                        {"tank"},
		"":                            {},
	} {
		assert.Equal(t, want, resourceProbePaths(in), in)
	}
}

// The capability probes read the pool's root dataset (or its own snapshots)
// without user properties, not every dataset or snapshot on the appliance with
// all their properties: with "paths": [] that one probe is tens of megabytes on
// a large NAS, the shape of the 2026-07 out-of-memory incident.
func TestResourceAPIProbesAreScopedToThePoolRoot(t *testing.T) {
	var mu sync.Mutex
	probes := map[string][]map[string]interface{}{}
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
			case datasetResourceQueryMethod, snapshotResourceQueryMethod:
				options := req.Params[0].(map[string]interface{})
				if _, read := options["get_user_properties"]; !read {
					mu.Lock()
					probes[req.Method] = append(probes[req.Method], options)
					mu.Unlock()
				}
				resp.Result = []interface{}{}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	t.Cleanup(mock.close)
	client := newSnapshotTestClient(t, server.URL)
	ctx := context.Background()

	_, err := client.DatasetQueryByParent(ctx, "tank/k8s/volumes")
	require.NoError(t, err)
	_, err = client.DatasetQueryByParent(ctx, "tank/k8s/volumes")
	require.NoError(t, err)
	_, err = client.SnapshotList(ctx, "tank/k8s/volumes/pvc-1")
	require.NoError(t, err)
	_, err = client.SnapshotList(ctx, "tank/k8s/volumes/pvc-1")
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, probes[datasetResourceQueryMethod], 1, "detection is still cached")
	assert.Equal(t, map[string]interface{}{
		"paths": []interface{}{"tank"}, "get_children": false, "properties": toInterfaces(datasetResourceQueryProperties),
	}, probes[datasetResourceQueryMethod][0])
	require.Len(t, probes[snapshotResourceQueryMethod], 1, "detection is still cached")
	assert.Equal(t, map[string]interface{}{
		"paths": []interface{}{"tank"}, "recursive": false, "properties": nil,
	}, probes[snapshotResourceQueryMethod][0])
}

func toInterfaces(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
