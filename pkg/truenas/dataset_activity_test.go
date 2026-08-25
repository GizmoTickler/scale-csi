package truenas

import (
	"context"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatasetActivityMethodsUseTrueNAS26Schemas(t *testing.T) {
	mock := newMockWSServer()
	paramsSeen := make(chan rpcTestRequest, 2)
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
			case "pool.dataset.attachments":
				paramsSeen <- req
				resp.Result = []interface{}{map[string]interface{}{
					"type": "NFS Share", "service": "nfs", "attachments": []string{"/mnt/tank/work"},
				}}
			case "pool.dataset.processes":
				paramsSeen <- req
				resp.Result = []interface{}{map[string]interface{}{
					"pid": float64(2520), "name": "smbd", "service": nil, "cmdline": "/usr/sbin/smbd --foreground",
				}}
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

	attachments, err := client.DatasetAttachments(context.Background(), "tank/work")
	require.NoError(t, err)
	require.Len(t, attachments, 1)
	assert.Equal(t, "NFS Share", attachments[0].Type)
	require.NotNil(t, attachments[0].Service)
	assert.Equal(t, "nfs", *attachments[0].Service)

	processes, err := client.DatasetProcesses(context.Background(), "tank/work")
	require.NoError(t, err)
	require.Len(t, processes, 1)
	assert.Equal(t, int64(2520), processes[0].PID)
	assert.Nil(t, processes[0].Service)
	require.NotNil(t, processes[0].Cmdline)
	assert.Equal(t, "/usr/sbin/smbd --foreground", *processes[0].Cmdline)

	for range 2 {
		req := <-paramsSeen
		assert.Equal(t, []interface{}{"tank/work"}, req.Params)
	}
}
