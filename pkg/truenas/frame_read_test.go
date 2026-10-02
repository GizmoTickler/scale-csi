package truenas

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The read loop reuses one frame buffer. A decoded response must not alias
// it, a large message's buffer is not kept, and an empty message is the
// io.ErrUnexpectedEOF conn.ReadJSON returned.
func TestReadFrameJSONReusesOneBufferSafely(t *testing.T) {
	big := `{"jsonrpc":"2.0","id":1,"result":"` + strings.Repeat("a", maxRetainedFrameBuffer+1) + `"}`
	messages := []string{
		`{"jsonrpc":"2.0","id":1,"result":["first"]}`,
		`{"jsonrpc":"2.0","id":2,"result":["second-and-longer"]}`,
		big,
		``,
	}
	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for _, message := range messages {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
				return
			}
		}
		_, _, _ = conn.ReadMessage()
	})
	defer mock.close()
	conn, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.Close()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	var frame bytes.Buffer
	var first, second, third rpcResponse
	require.NoError(t, readFrameJSON(conn, &frame, &first))
	require.NoError(t, readFrameJSON(conn, &frame, &second))
	assert.JSONEq(t, `["first"]`, string(first.Result), "the first result must not alias the reused buffer")
	assert.JSONEq(t, `["second-and-longer"]`, string(second.Result))
	assert.Equal(t, int64(2), second.ID)

	require.NoError(t, readFrameJSON(conn, &frame, &third))
	assert.Len(t, third.Result, maxRetainedFrameBuffer+3)
	assert.Zero(t, frame.Cap(), "a buffer grown past the bound is released, not pinned")

	var empty rpcResponse
	assert.ErrorIs(t, readFrameJSON(conn, &frame, &empty), io.ErrUnexpectedEOF)
}
