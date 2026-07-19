package truenas

import (
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

func TestClientRecordsAggregateConnectionTransitions(t *testing.T) {
	connection := NewConnection(0, &ClientConfig{})
	var transitions []bool
	client := &Client{
		pool: []*Connection{connection},
		connectionStatusRecorder: func(connected bool) {
			transitions = append(transitions, connected)
		},
	}
	connection.connectionStateChanged = client.recordConnectionStatus

	connection.notifyConnectionStateChanged()
	connection.notifyConnectionStateChanged()

	connection.mu.Lock()
	connection.conn.Store(&websocket.Conn{})
	connection.authenticated = true
	connection.stopped = false
	connection.mu.Unlock()
	atomic.StoreInt32(&connection.connState, int32(stateConnected))
	connection.notifyConnectionStateChanged()
	connection.notifyConnectionStateChanged()

	connection.mu.Lock()
	connection.authenticated = false
	connection.conn.Store(nil)
	connection.stopped = true
	connection.mu.Unlock()
	atomic.StoreInt32(&connection.connState, int32(stateDisconnected))
	connection.notifyConnectionStateChanged()

	want := []bool{false, true, false}
	if !reflect.DeepEqual(transitions, want) {
		t.Fatalf("connection transitions = %#v, want %#v", transitions, want)
	}
}
