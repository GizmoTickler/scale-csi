package truenas

import (
	"context"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fleetListServer answers nvmet.host_subsys.query and nvmet.host.query with
// fixed rows, recording the parameters of each call.
type fleetListServer struct {
	mu     sync.Mutex
	params map[string][]interface{}
	rows   map[string][]interface{}
}

func (s *fleetListServer) start(t *testing.T) *Client {
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
			case "system.info":
				resp.Result = map[string]interface{}{"version": "TrueNAS-SCALE-26.0.0"}
			case "nvmet.host_subsys.query", "nvmet.host.query":
				s.mu.Lock()
				if s.params == nil {
					s.params = map[string][]interface{}{}
				}
				s.params[req.Method] = append(s.params[req.Method], req.Params[0])
				resp.Result = s.rows[req.Method]
				s.mu.Unlock()
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

// The startup diff reads every allowed-host association and every host in
// one unfiltered query each, with the nested host and subsystem expanded
// exactly as the per-subsystem listing parses them.
func TestNVMeoFFleetListsReadWholeTables(t *testing.T) {
	ctx := context.Background()
	server := &fleetListServer{rows: map[string][]interface{}{
		"nvmet.host_subsys.query": {
			map[string]interface{}{"id": float64(1), "host": map[string]interface{}{"id": float64(11), "hostnqn": "nqn.a"}, "subsys": map[string]interface{}{"id": float64(7)}},
			map[string]interface{}{"id": float64(2), "host": map[string]interface{}{"id": float64(12)}, "subsys": map[string]interface{}{"id": float64(8)}},
		},
		"nvmet.host.query": {
			map[string]interface{}{"id": float64(11), "hostnqn": "nqn.a"},
			map[string]interface{}{"id": float64(12), "hostnqn": "nqn.b"},
		},
	}}
	client := server.start(t)

	associations, err := client.NVMeoFHostSubsysList(ctx)
	require.NoError(t, err)
	assert.Equal(t, []*NVMeoFHostSubsys{
		{ID: 1, HostID: 11, HostNQN: "nqn.a", SubsysID: 7},
		{ID: 2, HostID: 12, SubsysID: 8},
	}, associations)

	hosts, err := client.NVMeoFHostList(ctx)
	require.NoError(t, err)
	assert.Equal(t, []*NVMeoFHost{{ID: 11, HostNQN: "nqn.a"}, {ID: 12, HostNQN: "nqn.b"}}, hosts)

	server.mu.Lock()
	defer server.mu.Unlock()
	assert.Equal(t, []interface{}{[]interface{}{}}, server.params["nvmet.host_subsys.query"], "unfiltered")
	assert.Equal(t, []interface{}{[]interface{}{}}, server.params["nvmet.host.query"], "unfiltered")
}

// A row the parser cannot read fails the listing instead of being dropped: a
// caller deciding a subsystem's allowlist is exactly what it wants must never
// see fewer associations than the backend holds.
func TestNVMeoFFleetListsFailOnAnUnparseableRow(t *testing.T) {
	ctx := context.Background()
	server := &fleetListServer{rows: map[string][]interface{}{
		"nvmet.host_subsys.query": {
			map[string]interface{}{"id": float64(1), "host": map[string]interface{}{"id": float64(11), "hostnqn": "nqn.a"}, "subsys": map[string]interface{}{"id": float64(7)}},
			"not-an-object",
		},
		"nvmet.host.query": {"not-an-object"},
	}}
	client := server.start(t)

	_, err := client.NVMeoFHostSubsysList(ctx)
	require.Error(t, err)
	_, err = client.NVMeoFHostList(ctx)
	require.Error(t, err)
}
