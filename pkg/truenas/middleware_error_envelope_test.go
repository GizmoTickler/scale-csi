package truenas

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shapes below are not invented. They are what middlewared actually puts on
// the wire, read from the appliance's own source
// (/usr/lib/python3/dist-packages/middlewared/api/base/server/ws_handler/rpc.py
// on nas01, TrueNAS 26.0):
//
//	def format_truenas_error(self, errno_, reason, exc_info=None, extra=None):
//	    return {
//	        "error": errno_,                 # NUMERIC errno
//	        "errname": get_errname(errno_),  # symbolic name
//	        "reason": reason,
//	        "trace": ...,
//	        "extra": extra,
//	    }
//
//	def format_truenas_validation_error(self, exception, exc_info=None, errors=None):
//	    return self.format_truenas_error(errno.EINVAL, str(exception), exc_info, errors)
//
// Two envelopes therefore exist, and NEITHER carries an "errno" key:
//
//   - ValidationError(s) -> JSON-RPC -32602 "Invalid params". The top-level
//     errno is HARDCODED to EINVAL; the per-attribute errnos ride in "extra" as
//     [attribute, errmsg, errno] triples (process_method_call passes
//     [(e.attribute, e.errmsg, e.errno)]). pool.dataset.create reports an
//     existing dataset exactly this way: plugins/pool_/dataset.py does
//     `verrors.add('pool_dataset_create.name', f'Path {mountpoint} already
//     exists')`, and ValidationErrors.add defaults errno to EINVAL.
//
//   - CallException -> JSON-RPC -32001 "Method call error" (TRUENAS_CALL_ERROR).
//     Here the top-level errno IS the semantic one, e.g. EEXIST, and the message
//     is the constant literal "Method call error".

// liveDatasetExistsEnvelope is the -32602 envelope pool.dataset.create returns
// when the dataset is already there.
func liveDatasetExistsEnvelope(name string) *rpcError {
	reason := "[EINVAL] pool_dataset_create.name: Path /mnt/" + name + " already exists"
	return &rpcError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason":  reason,
			"trace":   nil,
			"extra": []interface{}{
				[]interface{}{"pool_dataset_create.name", "Path /mnt/" + name + " already exists", float64(syscall.EINVAL)},
			},
		},
	}
}

// TestDatasetCreateAdoptsExistingDatasetOnLiveInvalidParamsEnvelope is the
// regression for the idempotency blocker. A CreateVolume retry after an
// ambiguous write (controller restart, sidecar deadline) must adopt the dataset
// the first attempt already created. With IsAlreadyExistsError narrowed to the
// Message and no belt at this call site, the retry failed forever with
// "failed to create dataset: Invalid params" and the PVC stayed Pending.
func TestDatasetCreateAdoptsExistingDatasetOnLiveInvalidParamsEnvelope(t *testing.T) {
	const name = "tank/k8s/volumes/pvc-idempotent"

	client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
		switch req.Method {
		case "auth.login_with_api_key":
			resp.Result = true
		case "pool.dataset.create":
			resp.Error = liveDatasetExistsEnvelope(name)
		case "pool.dataset.query":
			resp.Result = []interface{}{existingDatasetRow(name)}
		default:
			resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
		}
	})

	dataset, err := client.DatasetCreate(context.Background(), &DatasetCreateParams{
		Name: name,
		Type: "FILESYSTEM",
	})
	require.NoError(t, err, "a create retry against an existing dataset must be idempotent")
	require.NotNil(t, dataset)
	assert.Equal(t, name, dataset.Name)
	assert.False(t, dataset.CreatedByCall, "an adopted dataset was not created by this call")
}

// TestIsAlreadyExistsErrorReadsTheCallErrorEnvelope covers the other live
// envelope. A CallException carrying EEXIST arrives as -32001 whose Message is
// the constant literal "Method call error": the semantic errno is only in Data,
// under "error"/"errname", neither of which findErrno's "errno"/"*_errno" scan
// accepts. Matching Message alone therefore made this shape a hard false, and
// callers branch to "adopt the existing object" on it.
func TestIsAlreadyExistsErrorReadsTheCallErrorEnvelope(t *testing.T) {
	callError := &APIError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error":   float64(syscall.EEXIST),
			"errname": "EEXIST",
			"reason":  "[EEXIST] Dataset tank/k8s/volumes/pvc-1 already exists",
			"trace":   nil,
			"extra":   nil,
		},
	}
	assert.True(t, IsAlreadyExistsError(callError),
		"the structured EEXIST in the middleware error envelope is authoritative")

	// A validation error whose per-attribute errno is EEXIST must classify too:
	// the top-level errno of a -32602 is always the generic EINVAL.
	validationEEXIST := &APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason":  "[EEXIST] sharing.nfs.create.path: share already exists",
			"extra": []interface{}{
				[]interface{}{"sharing.nfs.create.path", "share already exists", float64(syscall.EEXIST)},
			},
		},
	}
	assert.True(t, IsAlreadyExistsError(validationEEXIST),
		"a per-attribute EEXIST in the validation extra is this object's errno")

	// The round-six narrowing must be preserved: an already-exists MENTION about
	// a nested object, with no structured errno anywhere, still must not classify.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -1,
		Message: "[EFAULT] validation failed",
		Data: map[string]interface{}{
			"reason": "the parent portal group already exists on another target",
		},
	}), "an unstructured mention about a nested object is not this object's existence")

	// And a genuinely-unrelated errno in the envelope must not be read as EEXIST.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error":   float64(syscall.EACCES),
			"errname": "EACCES",
			"reason":  "[EACCES] permission denied",
		},
	}))
}

// TestMessageFallbackBeltStillEngagesForLiveValidationEnvelope is a GUARD, not a
// fix test: it passes before and after this change, and exists to make the next
// edit to findErrno fail loudly.
//
// The obvious-looking repair for the blocker is to teach findErrno the
// "errname" key. That repair is self-defeating. middlewared stamps a generic
// EINVAL on EVERY validation error, so findErrno would report "errno present"
// for every -32602 — and MessageFallbackContains returns false the moment ANY
// structured errno is present. The `|| MessageFallbackContains(err, "invalid
// params")` belt that ten create sites rely on (iscsi.go, nvmeof.go, nfs.go and
// now dataset.go) would silently stop engaging, and the already-exists shape
// this test pins would go back to failing the create.
func TestMessageFallbackBeltStillEngagesForLiveValidationEnvelope(t *testing.T) {
	envelope := liveDatasetExistsEnvelope("tank/k8s/volumes/pvc-belt")
	apiErr := &APIError{Code: envelope.Code, Message: envelope.Message, Data: envelope.Data}

	_, found := APIErrno(apiErr)
	assert.False(t, found,
		"the generic EINVAL middlewared stamps on every validation error must NOT be treated "+
			"as an authoritative errno; doing so disables MessageFallbackContains everywhere")
	assert.True(t, MessageFallbackContains(apiErr, "invalid params"),
		"the belt every create site pairs with IsAlreadyExistsError must keep engaging")
}

// TestDatasetGetRejectsNonListQueryResult pins the data-loss direction. A
// pool.dataset.query answer that is not a list is a MALFORMED or unexpected
// response, not proof the dataset is absent — but it was collapsed into the
// plain error "dataset not found", which IsNotFoundError matches, which makes
// DatasetExists report a false absence, which makes DatasetDelete return nil
// and the PV disappear while the dataset lives on.
func TestDatasetGetRejectsNonListQueryResult(t *testing.T) {
	const name = "tank/k8s/volumes/pvc-malformed"

	for _, test := range []struct {
		name   string
		result interface{}
	}{
		{name: "object instead of list", result: map[string]interface{}{"id": name}},
		{name: "count projection", result: float64(1)},
		{name: "bare string", result: "tank/k8s/volumes/pvc-malformed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
				switch req.Method {
				case "auth.login_with_api_key":
					resp.Result = true
				case "pool.dataset.query":
					resp.Result = test.result
				default:
					resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
				}
			})

			_, err := client.DatasetGet(context.Background(), name)
			require.Error(t, err)
			assert.False(t, IsNotFoundError(err),
				"a malformed query answer must never be classified as the dataset's absence: %v", err)

			exists, existsErr := client.DatasetExists(context.Background(), name)
			require.Error(t, existsErr,
				"DatasetExists must surface the malformed answer instead of reporting a false absence")
			assert.False(t, exists)
		})
	}
}

// TestDatasetGetUserPropertiesRejectsNonListQueryResult is the sibling of the
// test above: the same `!ok || len == 0` conflation lived in the user-property
// read's pool.dataset.query fallback.
func TestDatasetGetUserPropertiesRejectsNonListQueryResult(t *testing.T) {
	const name = "tank/k8s/volumes/pvc-props"

	client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
		switch req.Method {
		case "auth.login_with_api_key":
			resp.Result = true
		case "pool.dataset.query":
			resp.Result = map[string]interface{}{"id": name}
		default:
			resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
		}
	})

	_, err := client.DatasetGetUserProperties(context.Background(), name)
	require.Error(t, err)
	assert.False(t, IsNotFoundError(err),
		"a malformed user-property answer must not be reported as the dataset's absence: %v", err)
}

// existingDatasetRow is a pool.dataset.query row for a dataset that exists.
func existingDatasetRow(name string) map[string]interface{} {
	return map[string]interface{}{
		"id":         name,
		"name":       name,
		"pool":       "tank",
		"type":       "FILESYSTEM",
		"mountpoint": "/mnt/" + name,
		"used": map[string]interface{}{
			"value": "0", "rawvalue": "0", "parsed": float64(0), "source": "LOCAL",
		},
		"available": map[string]interface{}{
			"value": "100G", "rawvalue": "107374182400", "parsed": float64(107374182400), "source": "LOCAL",
		},
		"user_properties": map[string]interface{}{},
	}
}

// newEnvelopeTestClient wires a client to a mock TrueNAS JSON-RPC WebSocket
// server driven by a per-request responder.
func newEnvelopeTestClient(t *testing.T, respond func(req rpcTestRequest, resp *rpcTestResponse)) *Client {
	t.Helper()

	mock := newMockWSServer()
	server := mock.start(func(conn *websocket.Conn) {
		for {
			var req rpcTestRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			resp := rpcTestResponse{JSONRPC: "2.0", ID: req.ID}
			respond(req, &resp)
			if err := conn.WriteJSON(resp); err != nil {
				return
			}
		}
	})
	t.Cleanup(mock.close)

	host, port := testServerAddress(t, server.URL)
	client, err := NewClient(&ClientConfig{
		Host:           host,
		Port:           port,
		Protocol:       "http",
		APIKey:         "test-api-key",
		Timeout:        5 * time.Second,
		ConnectTimeout: 5 * time.Second,
		MaxConnections: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}
