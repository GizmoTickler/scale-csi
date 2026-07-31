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

// TestIsNotFoundErrorIgnoresNotFoundMentionsInDataBlob is the Batch 18 R8b
// regression: a -1 error whose Message is benign but whose Data blob merely
// MENTIONS "not found" about some nested object must NOT be classified as this
// object's authoritative absence. The fallback matches Message only, not
// FullError() (which embeds Data via %+v).
func TestIsNotFoundErrorIgnoresNotFoundMentionsInDataBlob(t *testing.T) {
	err := &APIError{
		Code:    -1,
		Message: "dataset is busy",
		Data: map[string]interface{}{
			"validation": []interface{}{
				map[string]interface{}{"detail": "referenced child dataset not found"},
			},
		},
	}
	// FullError() would contain "not found"; Message does not.
	assert.Contains(t, err.FullError(), "not found")
	assert.NotContains(t, err.Message, "not found")
	assert.False(t, IsNotFoundError(err),
		"a not-found mention buried in the Data blob must not be treated as this object's absence")
}

// TestIsLockContentionError guards the E1 benign_aborted classifier: only an
// EBUSY errno whose message signals a lock / in-progress operation is benign. A
// genuinely busy dataset (e.g. a mounted-dataset destroy) must stay a real error
// so it is never masked as retryable contention.
func TestIsLockContentionError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "ebusy already in progress is contention",
			err:  &APIError{Code: int(syscall.EBUSY), Message: "call already in progress"},
			want: true,
		},
		{
			name: "ebusy lock held is contention",
			err:  &APIError{Code: int(syscall.EBUSY), Message: "dataset lock held by another operation"},
			want: true,
		},
		{
			name: "ebusy genuinely busy dataset is real",
			err:  &APIError{Code: int(syscall.EBUSY), Message: "dataset is busy"},
			want: false,
		},
		{
			name: "enoent is not contention",
			err:  &APIError{Code: int(syscall.ENOENT), Message: "call already in progress"},
			want: false,
		},
		{name: "nil error", err: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsLockContentionError(tc.err))
		})
	}
}

func TestStructuredErrnoIsAuthoritativeOverMessageFallbacks(t *testing.T) {
	positivePermissionCode := &APIError{Code: int(syscall.EACCES), Message: "dataset not found and already exists"}
	assert.False(t, IsNotFoundError(positivePermissionCode))
	assert.False(t, IsAlreadyExistsError(positivePermissionCode))

	permissionWithMisleadingText := &APIError{
		Code:    -32602,
		Message: "dataset not found and already exists",
		Data: map[string]interface{}{
			"validation": []interface{}{map[string]interface{}{"errno": "EACCES"}},
		},
	}
	assert.False(t, IsNotFoundError(permissionWithMisleadingText))
	assert.False(t, IsAlreadyExistsError(permissionWithMisleadingText))
	assert.False(t, MessageFallbackContains(permissionWithMisleadingText, "not found", "already exists"))

	notFoundWithGenericText := &APIError{
		Code: -32602, Message: "Invalid params", Data: map[string]interface{}{"errno": "ENOENT"},
	}
	assert.True(t, IsNotFoundError(notFoundWithGenericText))
	assert.False(t, IsAlreadyExistsError(notFoundWithGenericText))

	existsWithGenericText := &APIError{
		Code: -32602, Message: "Invalid params", Data: map[string]interface{}{"errno": float64(syscall.EEXIST)},
	}
	assert.True(t, IsAlreadyExistsError(existsWithGenericText))
	assert.False(t, IsNotFoundError(existsWithGenericText))
}

func TestMethodNotFoundStructuredCodePrecedesMessageFallback(t *testing.T) {
	assert.True(t, isMethodNotFoundError(&APIError{Code: -32601, Message: "generic RPC failure"}))
	assert.False(t, isMethodNotFoundError(&APIError{Code: -32602, Message: "method not found"}))
	assert.True(t, isMethodNotFoundError(&APIError{Code: -1, Message: "method does not exist"}))
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
