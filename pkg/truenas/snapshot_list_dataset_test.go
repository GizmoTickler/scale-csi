package truenas

import (
	"context"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SnapshotList on the resource API keeps a row whose dataset field is empty
// when its ID names the listed dataset, and still drops a child's snapshot.
func TestSnapshotListDerivesTheDatasetFromTheID(t *testing.T) {
	resetSnapshotAPIPrefix()
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
			case snapshotResourceQueryMethod:
				resp.Result = []interface{}{
					map[string]interface{}{"name": "tank/k8s/volumes/pvc-123@snap1", "snapshot_name": "snap1", "type": "SNAPSHOT"},
					map[string]interface{}{"name": "tank/k8s/volumes/pvc-123/child@snap2", "snapshot_name": "snap2", "type": "SNAPSHOT"},
				}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	snapshots, err := client.SnapshotList(context.Background(), "tank/k8s/volumes/pvc-123")
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.Equal(t, "tank/k8s/volumes/pvc-123@snap1", snapshots[0].ID)
	assert.Equal(t, "tank/k8s/volumes/pvc-123", snapshots[0].Dataset)
}
