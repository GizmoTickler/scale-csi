package truenas

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseISCSIAuth(t *testing.T) {
	auth, err := parseISCSIAuth(map[string]interface{}{
		"id":       float64(7),
		"tag":      float64(1000),
		"user":     "chapuser",
		"peeruser": "peeruser",
		// Secret material in the response must never be parsed into the struct.
		"secret":     "should-be-ignored",
		"peersecret": "should-be-ignored-too",
	})
	require.NoError(t, err)
	assert.Equal(t, 7, auth.ID)
	assert.Equal(t, 1000, auth.Tag)
	assert.Equal(t, "chapuser", auth.User)
	assert.Equal(t, "peeruser", auth.PeerUser)

	_, err = parseISCSIAuth("not a map")
	require.Error(t, err)
}

func TestISCSIAuthSecretParams(t *testing.T) {
	oneWay := iscsiAuthSecretParams("user", "secretsecret12", "", "")
	assert.Equal(t, "user", oneWay["user"])
	assert.Equal(t, "secretsecret12", oneWay["secret"])
	assert.NotContains(t, oneWay, "peeruser", "one-way CHAP must not carry peer fields")
	assert.NotContains(t, oneWay, "peersecret")

	mutual := iscsiAuthSecretParams("user", "secretsecret12", "peer", "peersecret123")
	assert.Equal(t, "peer", mutual["peeruser"])
	assert.Equal(t, "peersecret123", mutual["peersecret"])
}

// authWSServer spins up a mock TrueNAS that records iscsi.auth params and
// echoes a fixed auth object back from create/update.
func authWSServer(t *testing.T, captured *chan []interface{}, method string, result interface{}) *httptest.Server {
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
			case method:
				if captured != nil {
					*captured <- req.Params
				}
				resp.Result = result
			case "iscsi.auth.query":
				if captured != nil {
					*captured <- req.Params
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
	return server
}

func TestISCSIAuthCreateParams(t *testing.T) {
	captured := make(chan []interface{}, 1)
	server := authWSServer(t, &captured, "iscsi.auth.create", map[string]interface{}{
		"id": float64(11), "tag": float64(1000), "user": "chapuser",
	})
	client := newSnapshotTestClient(t, server.URL)

	auth, err := client.ISCSIAuthCreate(context.Background(), 1000, "chapuser", "secretsecret12", "", "")
	require.NoError(t, err)
	require.NotNil(t, auth)
	assert.Equal(t, 11, auth.ID)
	assert.Equal(t, 1000, auth.Tag)

	params := (<-captured)[0].(map[string]interface{})
	assert.Equal(t, float64(1000), params["tag"])
	assert.Equal(t, "chapuser", params["user"])
	assert.Equal(t, "secretsecret12", params["secret"])
	assert.NotContains(t, params, "peeruser")
	assert.NotContains(t, params, "peersecret")
}

func TestISCSIAuthCreateMutualParams(t *testing.T) {
	captured := make(chan []interface{}, 1)
	server := authWSServer(t, &captured, "iscsi.auth.create", map[string]interface{}{
		"id": float64(12), "tag": float64(1001), "user": "chapuser", "peeruser": "peer",
	})
	client := newSnapshotTestClient(t, server.URL)

	_, err := client.ISCSIAuthCreate(context.Background(), 1001, "chapuser", "secretsecret12", "peer", "peersecret123")
	require.NoError(t, err)

	params := (<-captured)[0].(map[string]interface{})
	assert.Equal(t, "peer", params["peeruser"])
	assert.Equal(t, "peersecret123", params["peersecret"])
}

func TestISCSIAuthQueryByTagParams(t *testing.T) {
	captured := make(chan []interface{}, 1)
	server := authWSServer(t, &captured, "iscsi.auth.query", []interface{}{
		map[string]interface{}{"id": float64(5), "tag": float64(1000), "user": "chapuser"},
	})
	client := newSnapshotTestClient(t, server.URL)

	peers, err := client.ISCSIAuthQueryByTag(context.Background(), 1000)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, 5, peers[0].ID)

	params := <-captured
	filters := params[0].([]interface{})
	assert.Equal(t, []interface{}{"tag", "=", float64(1000)}, filters[0])
}

func TestISCSIAuthUpdateParams(t *testing.T) {
	captured := make(chan []interface{}, 1)
	server := authWSServer(t, &captured, "iscsi.auth.update", map[string]interface{}{
		"id": float64(11), "tag": float64(1000), "user": "chapuser",
	})
	client := newSnapshotTestClient(t, server.URL)

	_, err := client.ISCSIAuthUpdate(context.Background(), 11, "chapuser", "newsecret12345", "", "")
	require.NoError(t, err)

	params := <-captured
	assert.Equal(t, float64(11), params[0])
	body := params[1].(map[string]interface{})
	assert.Equal(t, "chapuser", body["user"])
	assert.Equal(t, "newsecret12345", body["secret"])
	assert.NotContains(t, body, "peeruser")
}

func TestISCSIAuthDeleteNotFoundTolerant(t *testing.T) {
	mock := newMockWSServer()
	mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			switch req.Method {
			case "auth.login_with_api_key":
				resp.Result = true
			case "iscsi.auth.delete":
				resp.Error = &rpcError{Code: -1, Message: "not found", Data: map[string]interface{}{"errno": "ENOENT"}}
			case "iscsi.auth.query":
				resp.Result = []interface{}{}
			default:
				resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
			}
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	defer mock.close()
	client := newSnapshotTestClient(t, mock.server.URL)

	assert.NoError(t, client.ISCSIAuthDelete(context.Background(), 999),
		"deleting a vanished auth peer must be idempotent")
}

func TestMockClientISCSIAuthReuseByTag(t *testing.T) {
	m := NewMockClient()
	ctx := context.Background()

	created, err := m.ISCSIAuthCreate(ctx, 1000, "chapuser", "secretsecret12", "", "")
	require.NoError(t, err)

	peers, err := m.ISCSIAuthQueryByTag(ctx, 1000)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, created.ID, peers[0].ID)
	assert.Equal(t, "chapuser", peers[0].User)

	// A different tag yields no peers.
	empty, err := m.ISCSIAuthQueryByTag(ctx, 2000)
	require.NoError(t, err)
	assert.Empty(t, empty)

	require.NoError(t, m.ISCSIAuthDelete(ctx, created.ID))
	peers, err = m.ISCSIAuthQueryByTag(ctx, 1000)
	require.NoError(t, err)
	assert.Empty(t, peers)
}
