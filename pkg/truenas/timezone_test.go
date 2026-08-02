package truenas

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// F2 — the cache store happened AFTER the unlocked c.Call with no generation
// guard, so a zone read through a connection that died mid-call could land in
// the cache after invalidateSystemTimezone (driven by handleDisconnect) had
// already cleared it, and then be trusted for the full TTL. That is a
// pre-reconnect value authorizing scheduled-snapshot decisions after an HA
// failover or a middleware restart — precisely what the invalidation exists to
// prevent.
//
// The test drives that exact interleaving: the config read is held on the wire
// while the invalidation runs, and the post-invalidation cache must be EMPTY.
func TestSystemTimezoneDiscardsAStoreRacedByInvalidation(t *testing.T) {
	var configCalls atomic.Int32
	inFlight := make(chan struct{})
	release := make(chan struct{})

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
			case "system.general.config":
				// First read is the pre-reconnect one and is held on the wire; every
				// later read answers with the zone the "new" backend reports.
				if configCalls.Add(1) == 1 {
					close(inFlight)
					<-release
					resp.Result = map[string]interface{}{"timezone": "America/New_York"}
				} else {
					resp.Result = map[string]interface{}{"timezone": "UTC"}
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

	type result struct {
		zone string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		loc, err := client.SystemTimezone(context.Background())
		zone := ""
		if loc != nil {
			zone = loc.String()
		}
		done <- result{zone: zone, err: err}
	}()

	<-inFlight
	// The reconnect path's invalidation, racing the in-flight read.
	client.invalidateSystemTimezone()
	close(release)

	first := <-done
	require.NoError(t, first.err)
	assert.Equal(t, "America/New_York", first.zone,
		"the caller still gets the value it read; only the CACHE store is discarded")

	// Within the TTL: a cached pre-invalidation zone would be served from memory
	// with no second wire call. It must instead be re-read.
	loc, err := client.SystemTimezone(context.Background())
	require.NoError(t, err)
	require.NotNil(t, loc)
	assert.Equal(t, "UTC", loc.String(), "the invalidated cache must not serve the pre-reconnect zone")
	assert.Equal(t, int32(2), configCalls.Load(), "the second read must reach the backend")
}

// The generation guard must not defeat ordinary caching: an uncontended read is
// still served from memory for the TTL (one wire call, not two).
func TestSystemTimezoneStillCachesWithoutAnInvalidation(t *testing.T) {
	var configCalls atomic.Int32

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
			case "system.general.config":
				configCalls.Add(1)
				resp.Result = map[string]interface{}{"timezone": "America/New_York"}
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

	for i := 0; i < 3; i++ {
		loc, err := client.SystemTimezone(context.Background())
		require.NoError(t, err)
		require.NotNil(t, loc)
		assert.Equal(t, "America/New_York", loc.String())
	}
	assert.Equal(t, int32(1), configCalls.Load(), "repeat reads within the TTL are served from the cache")
}
