package truenas

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testdata/nvmet-host-subsys-duplicate-26.0.json is a CAPTURED wire payload, not
// a reconstruction. It was produced on 2026-09-12 by issuing
// nvmet.host_subsys.create for an association that already existed (host 5,
// subsystem 2543) against the live appliance nas01 (TrueNAS 26.0) through this
// package's own client, and marshaling the resulting APIError's Code, Message
// and Data verbatim. The association count was confirmed unchanged on both sides
// of the call — middlewared rejects the duplicate in validation, before
// datastore.insert, so nothing was mutated.
//
// The file is used as-is, including the full ten-frame "trace", precisely so
// that no field is an author's model of the wire. The chain that produces it,
// read off the appliance:
//
//	plugins/nvmet/host_subsys.py:143   verrors.add(f'{schema_name}.host_id',
//	                                     "This record already exists (Host ID: .../Subsystem ID: ...)")
//	service_exception.py:59            def add(self, attribute, errmsg, errno: int = errno.EINVAL)
//	ws_handler/rpc.py:399              except ValidationErrors: send_truenas_validation_error(..., list(e))
//	ws_handler/rpc.py:147              format_truenas_validation_error -> format_truenas_error(errno.EINVAL, str(exception), ...)
//	ws_handler/rpc.py:97               {"error": <errno>, "errname": ..., "reason": ..., "trace": ..., "extra": ...}
//	service_exception.py:77            ValidationErrors.__iter__ yields (attribute, errmsg, errno)
//
// so the duplicate report arrives as -32602 "Invalid params" whose ONLY
// already-exists evidence is TEXT carried on an entry stamped with the generic
// EINVAL that ValidationErrors.add defaults to.
const hostSubsysDuplicateFixture = "testdata/nvmet-host-subsys-duplicate-26.0.json"

// loadHostSubsysDuplicateError returns the captured duplicate as the *APIError
// the client hands to the classifiers.
func loadHostSubsysDuplicateError(t *testing.T) *APIError {
	t.Helper()
	raw, err := os.ReadFile(hostSubsysDuplicateFixture)
	require.NoError(t, err)
	var wire rpcError
	require.NoError(t, json.Unmarshal(raw, &wire))
	return &APIError{Code: wire.Code, Message: wire.Message, Data: wire.Data}
}

// loadHostSubsysDuplicateRPCError returns the captured duplicate in the shape a
// mock server writes back on the wire.
func loadHostSubsysDuplicateRPCError(t *testing.T) *rpcError {
	t.Helper()
	raw, err := os.ReadFile(hostSubsysDuplicateFixture)
	require.NoError(t, err)
	var wire rpcError
	require.NoError(t, json.Unmarshal(raw, &wire))
	return &wire
}

// TestIsAlreadyExistsErrorClassifiesTheLiveHostSubsysDuplicate is the regression
// for the v1.11.0 production failure.
//
// On startup, strict fencing re-issues nvmet.host_subsys.create for every
// desired host on every subsystem (pkg/driver/fencing.go) and relies on this
// classifier for idempotency. v1.11.0 discarded any validation entry whose errno
// was not EEXIST BEFORE reading that entry's message. middlewared stamps the
// generic EINVAL on every validation entry, so the words "This record already
// exists" were never read: all 47 existing associations classified as hard
// errors, the fence never converged, strict mode refused every controller RPC,
// snapshots failed within seconds and the controller never became ready. The
// release was rolled back after eight minutes.
func TestIsAlreadyExistsErrorClassifiesTheLiveHostSubsysDuplicate(t *testing.T) {
	apiErr := loadHostSubsysDuplicateError(t)

	// Pin the captured shape so a fixture edit cannot quietly turn this into a
	// test of something else.
	require.Equal(t, -32602, apiErr.Code)
	require.Equal(t, "Invalid params", apiErr.Message)
	data, ok := apiErr.Data.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, float64(syscall.EINVAL), data["error"],
		"format_truenas_validation_error hardcodes the top-level errno to EINVAL")
	assert.Equal(t, "EINVAL", data["errname"])
	assert.Equal(t,
		"[EINVAL] nvmet_host_subsys_create.host_id: "+
			"This record already exists (Host ID: 5/Subsystem ID: 2543)\n",
		data["reason"],
		"reason is str(ValidationErrors), which appends a newline per entry")
	require.NotNil(t, data["trace"], "the live envelope carries a trace; the classifier must not need to read it")
	extra, ok := data["extra"].([]interface{})
	require.True(t, ok)
	require.Len(t, extra, 1)
	entry, ok := extra[0].([]interface{})
	require.True(t, ok)
	require.Len(t, entry, 3, "ValidationErrors.__iter__ yields (attribute, errmsg, errno)")
	assert.Equal(t, "nvmet_host_subsys_create.host_id", entry[0])
	assert.Equal(t, "This record already exists (Host ID: 5/Subsystem ID: 2543)", entry[1])
	assert.Equal(t, float64(syscall.EINVAL), entry[2],
		"the emitter calls verrors.add() without an errno, so the entry carries EINVAL, NOT EEXIST")

	assert.True(t, IsAlreadyExistsError(apiErr),
		"the duplicate host_subsys association must classify as already-exists; "+
			"v1.11.0 returned false here and strict fencing never converged")
}

// TestHostSubsysDuplicateLeavesTheMessageFallbackBeltArmed is the companion
// guard. The central fix must not make findErrno/APIErrno see this envelope: the
// moment APIErrno reports an errno, MessageFallbackContains refuses to look at
// text at all, and the `|| MessageFallbackContains(err, "invalid params")` belt
// that every sibling create site pairs with IsAlreadyExistsError stops engaging.
func TestHostSubsysDuplicateLeavesTheMessageFallbackBeltArmed(t *testing.T) {
	apiErr := loadHostSubsysDuplicateError(t)

	_, found := APIErrno(apiErr)
	assert.False(t, found,
		"neither the envelope's own keys nor anything in the ten-frame trace may be "+
			"read as an authoritative errno; that would disable the belt everywhere")
	assert.True(t, MessageFallbackContains(apiErr, "invalid params"),
		"the belt must remain available as defense in depth")
}

// TestNVMeoFHostSubsysCreateAdoptsTheExistingAssociation drives the captured
// payload through the method fencing actually calls, end to end over a mock
// JSON-RPC WebSocket server. This is the behavior the fence depends on: a
// duplicate create returns the association that is already there, not an error.
func TestNVMeoFHostSubsysCreateAdoptsTheExistingAssociation(t *testing.T) {
	const (
		hostID     = 5
		subsysID   = 2543
		assocID    = 35886
		hostNQN    = "nqn.2014-08.org.nvmexpress:uuid:k8s-0"
		subsysName = "pvc-fenced"
	)

	var createCalls, queryCalls int
	client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
		switch req.Method {
		case "auth.login_with_api_key":
			resp.Result = true
		case "nvmet.host_subsys.create":
			createCalls++
			resp.Error = loadHostSubsysDuplicateRPCError(t)
		case "nvmet.host_subsys.query":
			queryCalls++
			resp.Result = []interface{}{map[string]interface{}{
				"id": float64(assocID),
				"host": map[string]interface{}{
					"id": float64(hostID), "hostnqn": hostNQN,
				},
				"subsys": map[string]interface{}{
					"id": float64(subsysID), "name": subsysName,
				},
			}}
		default:
			resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
		}
	})

	assoc, err := client.NVMeoFHostSubsysCreate(context.Background(), hostID, subsysID)
	require.NoError(t, err,
		"re-issuing the create for an association that already exists must be idempotent; "+
			"strict fencing does exactly this for every host on every subsystem at startup")
	require.NotNil(t, assoc)
	assert.Equal(t, assocID, assoc.ID, "the EXISTING association must be returned")
	assert.Equal(t, hostID, assoc.HostID)
	assert.Equal(t, subsysID, assoc.SubsysID)
	assert.Equal(t, hostNQN, assoc.HostNQN)
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, queryCalls, "the adoption must be confirmed by a read, never assumed")
}

// TestNVMeoFHostSubsysCreateStillFailsWhenNothingExists is the fail-closed
// direction. "Invalid params" with no association behind it is a genuine
// parameter error and must stay an error — the confirming read is what makes the
// tolerance safe.
func TestNVMeoFHostSubsysCreateStillFailsWhenNothingExists(t *testing.T) {
	client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
		switch req.Method {
		case "auth.login_with_api_key":
			resp.Result = true
		case "nvmet.host_subsys.create":
			resp.Error = loadHostSubsysDuplicateRPCError(t)
		case "nvmet.host_subsys.query":
			resp.Result = []interface{}{}
		default:
			resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
		}
	})

	assoc, err := client.NVMeoFHostSubsysCreate(context.Background(), 5, 2543)
	require.Error(t, err, "a tolerated duplicate that is not actually there must not be swallowed")
	assert.Nil(t, assoc)
	assert.Contains(t, err.Error(), "failed to create host_subsys association")
}

// TestEveryGenuineAlreadyExistsEmitterStillClassifies re-verifies the classifier
// against every already-exists report this driver can provoke, in the exact
// shape the emitter produces.
//
// The errno column is the load-bearing one. Every entry built with a bare
// verrors.add() carries EINVAL, because ValidationErrors.add declares
// `errno: int = errno.EINVAL`; only pool_/snapshot.py passes errno.EEXIST
// explicitly. A classifier that gates on EEXIST therefore sees exactly one of
// these, which is how v1.11.0 shipped.
//
// Emitters enumerated from the appliance source at
// /usr/lib/python3/dist-packages/middlewared/plugins.
func TestEveryGenuineAlreadyExistsEmitterStillClassifies(t *testing.T) {
	validationEnvelope := func(attribute, errmsg string, entryErrno syscall.Errno) *APIError {
		return &APIError{
			Code:    -32602,
			Message: "Invalid params",
			Data: map[string]interface{}{
				"error":   float64(syscall.EINVAL),
				"errname": "EINVAL",
				"reason":  "[" + errnoName(entryErrno) + "] " + attribute + ": " + errmsg + "\n",
				"trace":   map[string]interface{}{"class": "ValidationErrors"},
				"extra": []interface{}{
					[]interface{}{attribute, errmsg, float64(entryErrno)},
				},
			},
		}
	}

	for _, test := range []struct {
		name      string
		source    string
		method    string
		apiErr    *APIError
		driverUse string
	}{
		{
			name:      "nvmet host_subsys duplicate",
			source:    "plugins/nvmet/host_subsys.py:143",
			method:    "nvmet.host_subsys.create",
			driverUse: "strict fencing on startup; NVMeoFSubsystemCreate host association",
			apiErr: validationEnvelope("nvmet_host_subsys_create.host_id",
				"This record already exists (Host ID: 5/Subsystem ID: 2543)", syscall.EINVAL),
		},
		{
			name:      "nvmet port_subsys duplicate",
			source:    "plugins/nvmet/port_subsys.py:148",
			method:    "nvmet.port_subsys.create",
			driverUse: "NodePublish / share creation",
			apiErr: validationEnvelope("nvmet_port_subsys_create.port_id",
				"This record already exists (Host ID: 1/Subsystem ID: 2543)", syscall.EINVAL),
		},
		{
			name:      "nvmet namespace duplicate",
			source:    "plugins/nvmet/namespace.py:297",
			method:    "nvmet.namespace.create",
			driverUse: "CreateVolume / share creation",
			apiErr: validationEnvelope("nvmet_namespace_create.nsid",
				"This record already exists (Subsystem ID: 2543/NSID: 1)", syscall.EINVAL),
		},
		{
			name:      "pool dataset already exists",
			source:    "plugins/pool_/dataset.py:519",
			method:    "pool.dataset.create",
			driverUse: "CreateVolume retry after an ambiguous write",
			apiErr: validationEnvelope("pool_dataset_create.name",
				"Path /mnt/tank/k8s/volumes/pvc-1 already exists", syscall.EINVAL),
		},
		{
			name:      "iscsi target name taken",
			source:    "plugins/iscsi_/targets.py:315",
			method:    "iscsi.target.create",
			driverUse: "iSCSI share creation",
			apiErr: validationEnvelope("iscsi_target_create.name",
				"Target with this name already exists", syscall.EINVAL),
		},
		{
			name:      "iscsi target alias taken",
			source:    "plugins/iscsi_/targets.py:214",
			method:    "iscsi.target.create",
			driverUse: "iSCSI share creation",
			apiErr: validationEnvelope("iscsi_target_create.alias",
				"Alias already exists", syscall.EINVAL),
		},
		{
			name:      "snapshot already exists",
			source:    "plugins/pool_/snapshot.py:350 (the one emitter that passes errno.EEXIST)",
			method:    "pool.snapshot.create",
			driverUse: "CreateSnapshot retry",
			apiErr: validationEnvelope("pool.snapshot.create",
				"tank/k8s/volumes/pvc-1@snap-1 already exists.", syscall.EEXIST),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.True(t, IsAlreadyExistsError(test.apiErr),
				"%s (%s) reports an existing object that %s must adopt",
				test.method, test.source, test.driverUse)
		})
	}

	// The CallException envelope and the hold path are unchanged and still
	// classify on their own top-level errno.
	assert.True(t, IsAlreadyExistsError(&APIError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error": float64(syscall.EEXIST), "errname": "EEXIST",
			"reason": "[EEXIST] Dataset tank/k8s/volumes/pvc-1 already exists",
		},
	}), "a CallError's top-level errno describes the call itself")
}

// TestRelaxedValidationErrnoDoesNotReopenMisattribution pins the two narrowings
// the relaxation had to preserve.
func TestRelaxedValidationErrnoDoesNotReopenMisattribution(t *testing.T) {
	// Round six: a "reason" that MENTIONS another object already existing, with
	// no envelope errno anywhere, is not an envelope at all and must not be read.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -1,
		Message: "[EFAULT] validation failed",
		Data: map[string]interface{}{
			"reason": "the parent portal group already exists on another target",
		},
	}), "an unattributed mention about a nested object is not this object's existence")

	// Round eight: an entry that names a DIFFERENT attribute and whose own text
	// is not an already-exists report still must not speak for the call.
	// plugins/account.py does exactly this for a user that does NOT exist.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error": float64(syscall.EINVAL), "errname": "EINVAL",
			"reason": "[EINVAL] user_create.group: This field is required\n" +
				"[EEXIST] user_create.home: /mnt/tank/home/svc: homedir already used by backup.\n",
			"extra": []interface{}{
				[]interface{}{"user_create.group", "This field is required", float64(syscall.EINVAL)},
				[]interface{}{"user_create.home", "/mnt/tank/home/svc: homedir already used by backup.", float64(syscall.EEXIST)},
			},
		},
	}), "an EEXIST on an unrelated attribute with no corroborating message is not this object's existence")

	// A specific, contradictory errno on the entry is still disqualifying: only
	// the wanted errno and the generic EINVAL default are treated as "no opinion".
	//
	// This case also pins why the "reason" fallback may not run alongside the
	// entries. reason is ValidationErrors.__str__, the CONCATENATION of every
	// entry, so reading it here would fuse the envelope's generic EINVAL with the
	// EACCES entry's text and launder the disqualified entry back into a true.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error": float64(syscall.EINVAL), "errname": "EINVAL",
			"reason": "[EACCES] share_create.path: dataset already exists but is not accessible\n",
			"extra": []interface{}{
				[]interface{}{"share_create.path", "dataset already exists but is not accessible", float64(syscall.EACCES)},
			},
		},
	}), "an entry that asserts a different specific errno is not an already-exists report")

	// The reason fallback DOES run for an envelope that carried no readable
	// entries: there the reason is middlewared's text for the call itself.
	assert.True(t, IsAlreadyExistsError(&APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error": float64(syscall.EINVAL), "errname": "EINVAL",
			"reason": "[EINVAL] nvmet_host_subsys_create.host_id: " +
				"This record already exists (Host ID: 5/Subsystem ID: 2543)\n",
			"extra": nil,
		},
	}), "with no entries to attribute to, the envelope's own reason is the call's text")

	// An envelope errno that is neither the wanted one nor EINVAL blocks the
	// reason consult outright.
	assert.False(t, IsAlreadyExistsError(&APIError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error": float64(syscall.EACCES), "errname": "EACCES",
			"reason": "[EACCES] the target already exists but you may not read it",
		},
	}), "a contradictory envelope errno must not be talked out of by the reason text")
}

// errnoName renders the symbolic name middlewared's get_errname would produce
// for the errnos used in the fixtures above.
func errnoName(e syscall.Errno) string {
	switch e {
	case syscall.EEXIST:
		return "EEXIST"
	case syscall.EACCES:
		return "EACCES"
	default:
		return "EINVAL"
	}
}

// TestHostSubsysDuplicateFixtureCarriesNoErrnoShapedTraceKey documents WHY the
// captured trace is safe to keep in the fixture. findErrno descends the whole
// Data blob looking for a key spelled "errno" or "*_errno", and each trace frame
// carries a `locals` map whose keys are the frame's variable names. None of the
// ten frames on the real duplicate path has such a variable — but if middlewared
// ever gains one, APIErrno would short-circuit with whatever repr string it
// holds and silently re-break this classifier AND the message-fallback belt.
// This assertion makes that day loud.
func TestHostSubsysDuplicateFixtureCarriesNoErrnoShapedTraceKey(t *testing.T) {
	raw, err := os.ReadFile(hostSubsysDuplicateFixture)
	require.NoError(t, err)

	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &doc))

	var walk func(value interface{}) []string
	walk = func(value interface{}) []string {
		var hits []string
		switch typed := value.(type) {
		case map[string]interface{}:
			for key, child := range typed {
				if strings.EqualFold(key, "errno") || strings.HasSuffix(strings.ToLower(key), "_errno") {
					hits = append(hits, key)
				}
				hits = append(hits, walk(child)...)
			}
		case []interface{}:
			for _, child := range typed {
				hits = append(hits, walk(child)...)
			}
		}
		return hits
	}

	assert.Empty(t, walk(doc),
		"no key in the live duplicate envelope is errno-shaped; findErrno must find nothing here")
}
