package truenas

import (
	"context"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subsysFilterServer answers nvmet.host_subsys.query and nvmet.port_subsys.query
// from a fixed two-subsystem table and records the filter each call sent.
// rejectFilter makes it fail any call that carries a filter (a backend that
// does not accept nested-field filters); ignoreFilter makes it return the
// whole table whatever the filter says (a backend that silently drops it).
type subsysFilterServer struct {
	mu           sync.Mutex
	filters      map[string][]interface{}
	rejectFilter bool
	// rejectCode is the error code of a rejection (default -32602, invalid
	// params); any other code models a transient middleware failure.
	rejectCode   int
	ignoreFilter bool
}

func (s *subsysFilterServer) calls(method string) []interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]interface{}(nil), s.filters[method]...)
}

func (s *subsysFilterServer) rows(method string) []map[string]interface{} {
	if method == "nvmet.host_subsys.query" {
		return []map[string]interface{}{
			{"id": float64(1), "host": map[string]interface{}{"id": float64(11), "hostnqn": "nqn.a"}, "subsys": map[string]interface{}{"id": float64(7)}},
			{"id": float64(2), "host": map[string]interface{}{"id": float64(12), "hostnqn": "nqn.b"}, "subsys": map[string]interface{}{"id": float64(8)}},
		}
	}
	return []map[string]interface{}{
		{"id": float64(21), "port": map[string]interface{}{"id": float64(1)}, "subsys": map[string]interface{}{"id": float64(7)}},
		{"id": float64(22), "port": map[string]interface{}{"id": float64(2)}, "subsys": map[string]interface{}{"id": float64(7)}},
		{"id": float64(23), "port": map[string]interface{}{"id": float64(1)}, "subsys": map[string]interface{}{"id": float64(8)}},
	}
}

func (s *subsysFilterServer) start(t *testing.T) *Client {
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
			case "nvmet.host_subsys.query", "nvmet.port_subsys.query":
				filter, _ := req.Params[0].([]interface{})
				s.mu.Lock()
				if s.filters == nil {
					s.filters = map[string][]interface{}{}
				}
				s.filters[req.Method] = append(s.filters[req.Method], filter)
				s.mu.Unlock()
				if len(filter) > 0 && s.rejectFilter {
					code := s.rejectCode
					if code == 0 {
						code = -32602
					}
					resp.Error = &rpcError{Code: code, Message: "Invalid params"}
					break
				}
				var want float64 = -1
				if len(filter) > 0 && !s.ignoreFilter {
					want = filter[0].([]interface{})[2].(float64)
				}
				out := make([]interface{}, 0)
				for _, row := range s.rows(req.Method) {
					if want >= 0 && row["subsys"].(map[string]interface{})["id"].(float64) != want {
						continue
					}
					out = append(out, row)
				}
				resp.Result = out
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

func subsysFilterFor(id int) []interface{} {
	return []interface{}{[]interface{}{"subsys.id", "=", float64(id)}}
}

// TestNVMeoFAssociationListsFilterBySubsystemOnServer pins the server-side
// subsys.id filter on both association listings, so a publish no longer reads
// every association on the NAS.
func TestNVMeoFAssociationListsFilterBySubsystemOnServer(t *testing.T) {
	ctx := context.Background()
	server := &subsysFilterServer{}
	client := server.start(t)

	hosts, err := client.NVMeoFHostSubsysListBySubsystem(ctx, 7)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	assert.Equal(t, 11, hosts[0].HostID)
	assert.Equal(t, []interface{}{subsysFilterFor(7)}, server.calls("nvmet.host_subsys.query"))

	ports, err := client.NVMeoFPortSubsysListBySubsystem(ctx, 7)
	require.NoError(t, err)
	require.Len(t, ports, 2)
	assert.Equal(t, []interface{}{subsysFilterFor(7)}, server.calls("nvmet.port_subsys.query"))

	// The whole-table listing used by the orphan sweep stays unfiltered.
	all, err := client.NVMeoFPortSubsysList(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

// TestNVMeoFAssociationListsRefilterWhenServerIgnoresFilter keeps the
// client-side filter authoritative: a backend that drops the filter must not
// leak another subsystem's associations into a fence decision.
func TestNVMeoFAssociationListsRefilterWhenServerIgnoresFilter(t *testing.T) {
	ctx := context.Background()
	server := &subsysFilterServer{ignoreFilter: true}
	client := server.start(t)

	hosts, err := client.NVMeoFHostSubsysListBySubsystem(ctx, 8)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	assert.Equal(t, 12, hosts[0].HostID)

	ports, err := client.NVMeoFPortSubsysListBySubsystem(ctx, 8)
	require.NoError(t, err)
	require.Len(t, ports, 1)
	assert.Equal(t, 23, ports[0].ID)
}

// TestNVMeoFAssociationListsFallBackWhenServerRejectsFilter covers a backend
// that rejects the nested filter: the call is retried unfiltered once, and
// later calls go straight to the unfiltered form.
func TestNVMeoFAssociationListsFallBackWhenServerRejectsFilter(t *testing.T) {
	ctx := context.Background()
	server := &subsysFilterServer{rejectFilter: true}
	client := server.start(t)

	for i := 0; i < 2; i++ {
		hosts, err := client.NVMeoFHostSubsysListBySubsystem(ctx, 7)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
		ports, err := client.NVMeoFPortSubsysListBySubsystem(ctx, 7)
		require.NoError(t, err)
		require.Len(t, ports, 2)
	}
	// filtered (rejected), unfiltered, then unfiltered only.
	empty := []interface{}{}
	assert.Equal(t, []interface{}{subsysFilterFor(7), empty, empty}, server.calls("nvmet.host_subsys.query"))
	assert.Equal(t, []interface{}{subsysFilterFor(7), empty, empty}, server.calls("nvmet.port_subsys.query"))
}

// A filtered call that fails with anything but invalid params (a transient
// middleware error) is answered unfiltered, but the filter is tried again next
// time: only a real rejection of the filter switches it off for good.
func TestNVMeoFAssociationListsKeepFilterAfterTransientError(t *testing.T) {
	ctx := context.Background()
	server := &subsysFilterServer{rejectFilter: true, rejectCode: -32001}
	client := server.start(t)

	for i := 0; i < 2; i++ {
		hosts, err := client.NVMeoFHostSubsysListBySubsystem(ctx, 7)
		require.NoError(t, err)
		require.Len(t, hosts, 1)
	}
	empty := []interface{}{}
	assert.Equal(t, []interface{}{subsysFilterFor(7), empty, subsysFilterFor(7), empty}, server.calls("nvmet.host_subsys.query"))
}
