package truenas

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
)

func TestIsNotFoundErrorGenericCodeDoesNotMaskBusyDataset(t *testing.T) {
	err := &APIError{Code: -1, Message: "dataset is busy"}
	if IsNotFoundError(err) {
		t.Fatal("generic TrueNAS code -1 with a busy message must not be classified as not found")
	}
}

func TestIsNotFoundErrorGenericCodeWithNotFoundMessage(t *testing.T) {
	err := &APIError{Code: -1, Message: "dataset not found"}
	if !IsNotFoundError(err) {
		t.Fatal("a realistic not-found message must still be classified as not found")
	}
}

func TestIsConnectionErrorUsesTypedTransportFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "transport sentinel", err: fmt.Errorf("write: %w", ErrTransportFailure), want: true},
		{name: "ambiguous sentinel", err: fmt.Errorf("read: %w", ErrAmbiguousResult), want: true},
		{name: "connection refused errno", err: fmt.Errorf("dial: %w", syscall.ECONNREFUSED), want: true},
		{name: "connection reset errno", err: fmt.Errorf("read: %w", syscall.ECONNRESET), want: true},
		{name: "url transport", err: &url.Error{Op: "dial", URL: "wss://truenas.example.test", Err: syscall.EHOSTUNREACH}, want: true},
		{name: "net error", err: &net.DNSError{Err: "timeout", IsTimeout: true}, want: true},
		{name: "websocket close", err: &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "lost"}, want: true},
		{name: "websocket handshake", err: websocket.ErrBadHandshake, want: true},
		{name: "transport eof", err: io.EOF, want: true},
		{name: "plain connection message", err: errors.New("connection refused while validating dataset"), want: false},
		{name: "plain timeout message", err: errors.New("timeout policy rejected"), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, IsConnectionError(test.err))
		})
	}
}

func TestIsConnectionErrorNeverRetriesStructuredAPIErrorsByMessage(t *testing.T) {
	for _, message := range []string{
		"connection refused by policy",
		"dataset timeout must be positive",
		"connection object already exists",
	} {
		apiErr := &APIError{Code: -32602, Message: message}
		assert.False(t, IsConnectionError(apiErr))
		assert.False(t, IsConnectionError(fmt.Errorf("API call failed: %w", apiErr)))
		assert.False(t, IsConnectionError(&url.Error{Op: "post", URL: "wss://truenas.example.test", Err: apiErr}))
	}
}
