package truenas

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The envelopes below are not invented. Every one of them is derived from source
// read off the appliance (nas01, TrueNAS 26.0) — see the per-fixture citations —
// and the libzfs_core rendering in liveAlreadyHeldHoldEnvelope was captured by
// executing the failing call against the live library.
//
// Note in particular that NONE of them carries a positive JSON-RPC code.
// middlewared only ever emits the reserved negative codes (-32602 for
// ValidationError(s), -32001 TRUENAS_CALL_ERROR for everything else); the
// semantic errno travels in Data, never in "code".

// TestIsAlreadyExistsErrorRequiresTheFailingAttributeToCorroborate is the
// regression for the attribution bug in the validation-envelope reader.
//
// For a ValidationError(s) the per-attribute errnos ride in "extra" as
// [attribute, errmsg, errno] triples. The reader took fields[2] — the errno —
// from EVERY triple and never looked at fields[0]/fields[1], so an EEXIST
// reported against an unrelated attribute made the whole error read as "the
// object you asked to create already exists".
//
// ValidationErrors is an accumulator (middlewared/service_exception.py: add()
// appends, __iter__ yields (attribute, errmsg, errno) for each), so multi-entry
// lists are ordinary, and an EEXIST in one of them routinely means "some OTHER
// object is taken". middlewared/plugins/account.py:524-530 is the live example:
//
//	verrors.add(f'{schema}.home',
//	            f'{data["home"]}: homedir already used by {in_use[0]["username"]}.',
//	            errno.EEXIST)
//
// The user being created does not exist. The home directory is taken.
func TestIsAlreadyExistsErrorRequiresTheFailingAttributeToCorroborate(t *testing.T) {
	// user.create: one plain EINVAL entry plus an EEXIST entry about an
	// unrelated attribute. Nothing here says the created object exists.
	unrelatedEEXIST := &APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason": "[EINVAL] user_create.group: This field is required\n" +
				"[EEXIST] user_create.home: /mnt/tank/home/svc: homedir already used by backup.\n",
			"trace": nil,
			"extra": []interface{}{
				[]interface{}{"user_create.group", "This field is required", float64(syscall.EINVAL)},
				[]interface{}{"user_create.home", "/mnt/tank/home/svc: homedir already used by backup.", float64(syscall.EEXIST)},
			},
		},
	}
	assert.False(t, IsAlreadyExistsError(unrelatedEEXIST),
		"an EEXIST attributed to an unrelated attribute is not this object's existence; "+
			"callers branch to 'adopt what is already there' on a true verdict")

	// The positive control, from middlewared/plugins/pool_/snapshot.py:347-352:
	//   raise ValidationError("pool.snapshot.create", f"{name} already exists.",
	//                         errno.EEXIST)
	// The entry corroborates its own errno, so idempotent create must still adopt.
	snapshotExists := &APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason":  "[EEXIST] pool.snapshot.create: snap-1 already exists.",
			"trace":   nil,
			"extra": []interface{}{
				[]interface{}{"pool.snapshot.create", "snap-1 already exists.", float64(syscall.EEXIST)},
			},
		},
	}
	assert.True(t, IsAlreadyExistsError(snapshotExists),
		"an EEXIST entry that corroborates itself is still this object's existence")

	// A triple whose message is not a string cannot corroborate anything, and a
	// bare errno is exactly what must no longer be enough.
	uncorroborated := &APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason":  "[EEXIST] some.method: failure",
			"extra": []interface{}{
				[]interface{}{"some.method", nil, float64(syscall.EEXIST)},
			},
		},
	}
	assert.False(t, IsAlreadyExistsError(uncorroborated),
		"an entry with no message of its own cannot corroborate its errno")

	// The CallException envelope is unchanged: its top-level errno is the one
	// middlewared assigned to the CALL, so it needs no attribute to corroborate.
	assert.True(t, IsAlreadyExistsError(&APIError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error":   float64(syscall.EEXIST),
			"errname": "EEXIST",
			"reason":  "[EEXIST] Application with name foo already exists",
			"trace":   nil,
			"extra":   nil,
		},
	}), "a top-level EEXIST describes the call itself, not one of its arguments")
}

// liveAlreadyHeldHoldEnvelope is what pool.snapshot.hold puts on the wire when
// the snapshot already carries the `truenas` hold.
//
// The chain, read off the appliance: pool.snapshot.hold (plugins/pool_/snapshot.py:69)
// calls the PRIVATE zfs.resource.snapshot.hold_impl, which calls hold_impl()
// (plugins/zfs/snapshot_hold_release_impl.py), which calls
// truenas_pylibzfs.lzc.create_holds() and re-raises its ZFSCoreException BARE.
// ZFSCoreException is not a CallException, so process_method_call takes the
// generic `except Exception` arm, adapt_exception() declines it, and the error
// goes out as -32001 "Method call error" with a HARDCODED top-level errno of
// EINVAL. The libzfs errno survives only inside "reason", which is
// str(ZFSCoreException). That rendering was captured from the live library:
//
//	>>> truenas_pylibzfs.lzc.create_holds(holds=[("no_such_pool_xyz/ds@nope", "truenas")])
//	ZFSCoreException: ('lzc_hold() failed', (('Operation failed', 2),))
//
// so a duplicate hold is the same shape with EEXIST's strerror and errno.
func liveAlreadyHeldHoldEnvelope() *rpcError {
	return &rpcError{
		Code:    -32001,
		Message: "Method call error",
		Data: map[string]interface{}{
			"error":   float64(syscall.EINVAL),
			"errname": "EINVAL",
			"reason":  "('lzc_hold() failed', (('File exists', 17),))",
			"trace": map[string]interface{}{
				"class":     "ZFSCoreException",
				"formatted": "Traceback (most recent call last):\n  File \"/usr/lib/python3/dist-packages/middlewared/plugins/zfs/snapshot_hold_release_impl.py\", line 117, in hold_impl\n    truenas_pylibzfs.lzc.create_holds(holds=holds)\n",
				"repr":      "ZFSCoreException('lzc_hold() failed', (('File exists', 17),))",
			},
			"extra": nil,
		},
	}
}

// TestSnapshotHoldIsIdempotentOnTheLiveCallErrorEnvelope is the regression for
// the hold call site.
//
// SnapshotHold documents a re-hold as idempotent success, and the driver relies
// on it: CreateSnapshot is retried by the external-snapshotter, and every retry
// re-holds. On TrueNAS 26.0 the already-held evidence reaches the client ONLY in
// the envelope's "reason" — the top-level errno is a hardcoded EINVAL and
// Message is the constant literal "Method call error". APIError.Error() renders
// Code and Message and never Data, so the lzc_hold branch that was supposed to
// catch this could not see its own evidence: every re-hold failed, and the
// driver logged and event-ed "it is not protected from foreign deletion" about a
// snapshot that was in fact protected.
func TestSnapshotHoldIsIdempotentOnTheLiveCallErrorEnvelope(t *testing.T) {
	client := newEnvelopeTestClient(t, func(req rpcTestRequest, resp *rpcTestResponse) {
		switch req.Method {
		case "auth.login_with_api_key":
			resp.Result = true
		case "pool.snapshot.hold":
			snapshotID, _ := req.Params[0].(string)
			switch snapshotID {
			case "tank/k8s/volumes/pvc-1@snap-held":
				resp.Error = liveAlreadyHeldHoldEnvelope()
			case "tank/k8s/volumes/pvc-1@snap-gone":
				// The same call against a snapshot that does not exist:
				// ZFSPathNotFoundException is also a plain Exception, so it takes
				// the same -32001/EINVAL arm with its own reason.
				resp.Error = &rpcError{
					Code:    -32001,
					Message: "Method call error",
					Data: map[string]interface{}{
						"error":   float64(syscall.EINVAL),
						"errname": "EINVAL",
						"reason":  "'tank/k8s/volumes/pvc-1@snap-gone' not found",
						"extra":   nil,
					},
				}
			case "tank/k8s/volumes/pvc-1@snap-protected":
				// hold_impl's own guard, plugins/zfs/snapshot_crud.py:575-577:
				// ValidationError(schema, f"{path!r} is a protected path.", errno.EACCES)
				resp.Error = &rpcError{
					Code:    -32602,
					Message: "Invalid params",
					Data: map[string]interface{}{
						"error":   float64(syscall.EINVAL),
						"errname": "EINVAL",
						"reason":  "[EACCES] zfs.resource.snapshot.hold: 'tank/k8s/volumes/pvc-1@snap-protected' is a protected path.",
						"extra": []interface{}{
							[]interface{}{
								"zfs.resource.snapshot.hold",
								"'tank/k8s/volumes/pvc-1@snap-protected' is a protected path.",
								float64(syscall.EACCES),
							},
						},
					},
				}
			default:
				resp.Result = nil
			}
		default:
			resp.Error = &rpcError{Code: -32601, Message: "Method not found"}
		}
	})

	ctx := context.Background()

	require.NoError(t, client.SnapshotHold(ctx, "tank/k8s/volumes/pvc-1@snap-fresh"),
		"a fresh hold succeeds")
	assert.NoError(t, client.SnapshotHold(ctx, "tank/k8s/volumes/pvc-1@snap-held"),
		"a duplicate lzc_hold() is the idempotent already-held outcome, and its errno "+
			"lives only in the envelope reason on TrueNAS 26.0")
	assert.Error(t, client.SnapshotHold(ctx, "tank/k8s/volumes/pvc-1@snap-gone"),
		"a missing snapshot arrives in the same -32001/EINVAL envelope and must NOT be "+
			"read as a hold that is already in place")
	assert.Error(t, client.SnapshotHold(ctx, "tank/k8s/volumes/pvc-1@snap-protected"),
		"a protected-path refusal must not be reported as a snapshot that is already held")
}
