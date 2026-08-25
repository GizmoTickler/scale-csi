package truenas

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoreJobWaitAvailableAndUsed(t *testing.T) {
	mock := newMockWSServer()
	var jobWaitCalls atomic.Int32
	var getJobsCalls atomic.Int32
	jobWaitParams := make(chan []interface{}, 1)
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key", "core.subscribe":
				resp.Result = true
			case "core.job_wait":
				jobWaitCalls.Add(1)
				jobWaitParams <- req.Params
				resp.Result = float64(91)
			case "core.get_jobs":
				getJobsCalls.Add(1)
				resp.Result = []interface{}{map[string]interface{}{"id": float64(91), "state": "RUNNING"}}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
			if req.Method == "core.get_jobs" {
				notification := map[string]interface{}{
					"jsonrpc": "2.0",
					"method":  "collection_update",
					"params": map[string]interface{}{
						"collection": "core.get_jobs",
						"fields":     map[string]interface{}{"id": float64(91), "state": "SUCCESS"},
					},
				}
				if err := conn.WriteJSON(notification); err != nil {
					return
				}
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, server.URL)

	require.NoError(t, client.waitForJob(context.Background(), 41))
	assert.Equal(t, int32(1), jobWaitCalls.Load())
	assert.Equal(t, int32(1), getJobsCalls.Load(), "the core.job_wait job should complete from its event after the initial race-closing poll")
	assert.Equal(t, []interface{}{float64(41)}, <-jobWaitParams)
}

func TestCoreJobWaitMethodNotFoundFallsBackAndCaches(t *testing.T) {
	mock := newMockWSServer()
	var jobWaitCalls atomic.Int32
	var getJobsCalls atomic.Int32
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
			case "core.job_wait":
				jobWaitCalls.Add(1)
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			case "core.get_jobs":
				getJobsCalls.Add(1)
				resp.Result = []interface{}{map[string]interface{}{"id": req.Params[0].([]interface{})[0].([]interface{})[2], "state": "SUCCESS"}}
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

	require.NoError(t, client.waitForJob(context.Background(), 42))
	require.NoError(t, client.waitForJob(context.Background(), 43))
	assert.Equal(t, int32(1), jobWaitCalls.Load(), "method-not-found must be cached")
	assert.Equal(t, int32(2), getJobsCalls.Load())
}

func TestCoreJobWaitRespectsContextCancellation(t *testing.T) {
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
			case "core.job_wait":
				resp.Result = float64(93)
			case "core.get_jobs":
				resp.Result = []interface{}{map[string]interface{}{"id": float64(93), "state": "RUNNING"}}
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

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := client.waitForJob(ctx, 44)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), err)
}
