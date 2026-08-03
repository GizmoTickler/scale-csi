package truenas

import (
	"context"
	"fmt"
)

// ZFS-native encryption at rest (GF-Sprint 1). Every method here is a TrueNAS
// @job: the call dispatches, returns a job id, and is awaited through the shared
// job-wait path (waitForJob), so a job that ends in a terminal failure state
// surfaces as an error.
//
// ★ THE JOB STATE IS NOT THE WHOLE ANSWER FOR unlock. ★ The design pinned P-5 as
// "a wrong passphrase is a FAILED job". The LIVE DRILL (nas01 26.0.0-BETA.1,
// 2026-08-02, /tmp/scale-csi-gf1-drill-report.md) proved that FALSE for this
// call shape: pool.dataset.unlock returns job state SUCCESS for a wrong key and
// reports the failure ONLY in the job RESULT payload
// ({"unlocked": [names], "failed": {name: {"error": "Invalid Key"}}}). Reading
// the state alone made a wrong-key unlock indistinguishable from a correct one —
// a fail-OPEN publish and a rotation arm that could never be reached. Unlock
// therefore asserts on the payload; see DatasetUnlock.
//
// All other shapes remain pinned to the nas01 probes (design §0 P-0..P-11, plus
// the drill's re-verification). Do not "correct" them from ZFS or TrueNAS
// documentation.
const (
	datasetLockMethod              = "pool.dataset.lock"
	datasetUnlockMethod            = "pool.dataset.unlock"
	datasetChangeKeyMethod         = "pool.dataset.change_key"
	datasetEncryptionSummaryMethod = "pool.dataset.encryption_summary"
)

// EncryptionSummaryEntry is one row of a pool.dataset.encryption_summary job
// result (P-3). For a passphrase dataset TrueNAS does NOT persist the key
// (key_present_in_database:false), so after a reboot every such dataset comes up
// locked:true with valid_key:false until something supplies the passphrase.
type EncryptionSummaryEntry struct {
	Name                 string `json:"name"`
	KeyFormat            string `json:"key_format"`
	KeyPresentInDatabase bool   `json:"key_present_in_database"`
	ValidKey             bool   `json:"valid_key"`
	Locked               bool   `json:"locked"`
	UnlockError          string `json:"unlock_error"`
	UnlockSuccessful     bool   `json:"unlock_successful"`
}

// dispatchDatasetJob dispatches a dataset @job method and awaits its terminal
// state, translating a FAILED/ABORTED/CANCELED job into an error. It is the
// shared body for lock/unlock/change_key, which differ only in method and args.
func (c *Client) dispatchDatasetJob(ctx context.Context, method string, args ...interface{}) error {
	result, err := c.Call(ctx, method, args...)
	if err != nil {
		return fmt.Errorf("failed to dispatch %s: %w", method, err)
	}
	jobID, err := replicationJobID(result)
	if err != nil {
		return fmt.Errorf("%s returned an unusable job id: %w", method, err)
	}
	if err := c.waitForJob(ctx, jobID); err != nil {
		return fmt.Errorf("%s job %d failed: %w", method, jobID, err)
	}
	return nil
}

// DatasetLock locks an encrypted dataset. The driver NEVER calls this in a live
// control path — locking is an operator/host action and a locked dataset serves
// zero I/O (P-4). It exists only so the drill and unit tests can model the
// locked state.
func (c *Client) DatasetLock(ctx context.Context, name string) error {
	return c.dispatchDatasetJob(ctx, datasetLockMethod, name)
}

// DatasetUnlock unlocks an encrypted dataset with a passphrase. The payload is
// the P-4 shape: {"datasets":[{"name":name,"passphrase":passphrase}],
// "toggle_attachments":true}. toggle_attachments makes TrueNAS re-run the
// iSCSI/NFS attachments on unlock so an extent over the zvol needs no recreation
// (the /dev/zvol/<name> path is stable across the unlock, P-4).
//
// ★ THE OUTCOME LIVES IN THE JOB RESULT, NOT THE JOB STATE. ★ Live drill,
// nas01 26.0.0-BETA.1, 2026-08-02 (/tmp/scale-csi-gf1-drill-report.md, D-1):
//
//	wrong key:   job SUCCESS, result {"unlocked": [],         "failed": {"<name>": {"error": "Invalid Key", "skipped": []}}}
//	correct key: job SUCCESS, result {"unlocked": ["<name>"], "failed": {}}
//
// Both are SUCCESSFUL jobs. Trusting the state alone (the design's P-5 claim,
// now known false for this call shape) made a wrong passphrase indistinguishable
// from a correct one: publish reported success on a still-locked volume, and the
// rotation arm that only runs after a failed unlock became unreachable dead
// code. So this method fetches the result and requires the dataset to be named
// in "unlocked" AND absent from "failed"; anything else is an error carrying the
// backend's own reason. Callers that log or surface that error MUST scrub it —
// it is backend text about a key operation (R6).
//
// Unlock is still NOT idempotent: unlocking an already-unlocked dataset is a
// hard call error ("[EINVAL] ... dataset is not locked", drill-confirmed, so P-8
// holds). Callers MUST gate on locked==true (read via DatasetEncryptionSummary)
// before calling.
func (c *Client) DatasetUnlock(ctx context.Context, name, passphrase string) error {
	options := map[string]interface{}{
		"datasets": []map[string]interface{}{
			{"name": name, "passphrase": passphrase},
		},
		"toggle_attachments": true,
	}
	result, err := c.Call(ctx, datasetUnlockMethod, name, options)
	if err != nil {
		return fmt.Errorf("failed to dispatch %s: %w", datasetUnlockMethod, err)
	}
	jobID, err := replicationJobID(result)
	if err != nil {
		return fmt.Errorf("%s returned an unusable job id: %w", datasetUnlockMethod, err)
	}
	if waitErr := c.waitForJob(ctx, jobID); waitErr != nil {
		return fmt.Errorf("%s job %d failed: %w", datasetUnlockMethod, jobID, waitErr)
	}
	raw, err := c.fetchJobResult(ctx, jobID)
	if err != nil {
		// Fail CLOSED: an unlock whose outcome cannot be read is not an unlock.
		return fmt.Errorf("%s job %d succeeded but its result could not be read: %w",
			datasetUnlockMethod, jobID, err)
	}
	return datasetUnlockOutcome(name, raw)
}

// datasetUnlockOutcome decides whether a pool.dataset.unlock job actually
// unlocked the dataset, from the job's RESULT payload. It is shared with the
// mock so both agree on the contract by construction.
//
// Success requires BOTH: the dataset named in "unlocked", and no entry for it in
// "failed". Anything else — an unreadable payload, an empty result, or a payload
// that mentions the dataset nowhere — is an error, because none of those is
// evidence that the key was loaded.
func datasetUnlockOutcome(name string, raw interface{}) error {
	payload, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unlock of %s returned an unreadable result payload (%T); "+
			"the dataset is not proven unlocked", name, raw)
	}
	if reason, failed := datasetUnlockFailureReason(payload, name); failed {
		return fmt.Errorf("unlock of %s failed: %s", name, reason)
	}
	for _, entry := range asInterfaceSlice(payload["unlocked"]) {
		if unlockedName, ok := entry.(string); ok && unlockedName == name {
			return nil
		}
	}
	return fmt.Errorf("unlock of %s reported neither success nor failure for it "+
		"(unlocked=%v failed=%v); the dataset is not proven unlocked",
		name, payload["unlocked"], payload["failed"])
}

// datasetUnlockFailureReason extracts the backend's reason for a per-dataset
// unlock failure. The probed shape is {"failed": {"<name>": {"error": "...",
// "skipped": [...]}}}; a non-conforming entry still counts as a failure with a
// generic reason, never as a success.
func datasetUnlockFailureReason(payload map[string]interface{}, name string) (string, bool) {
	failed, ok := payload["failed"].(map[string]interface{})
	if !ok || len(failed) == 0 {
		return "", false
	}
	entry, present := failed[name]
	if !present {
		return "", false
	}
	if detail, ok := entry.(map[string]interface{}); ok {
		if reason, ok := detail["error"].(string); ok && reason != "" {
			return reason, true
		}
	}
	return "backend reported the unlock as failed with no reason", true
}

func asInterfaceSlice(raw interface{}) []interface{} {
	if items, ok := raw.([]interface{}); ok {
		return items
	}
	return nil
}

// DatasetChangeKey re-keys an UNLOCKED dataset to a new passphrase (P-6). It
// requires the key already loaded (dataset unlocked); afterward the old
// passphrase is dead. The driver uses it to complete the two-key rotation window
// (E-3): unlock with passphrasePrevious, then change_key to passphrase.
func (c *Client) DatasetChangeKey(ctx context.Context, name, passphrase string) error {
	options := map[string]interface{}{
		"passphrase": passphrase,
	}
	return c.dispatchDatasetJob(ctx, datasetChangeKeyMethod, name, options)
}

// DatasetEncryptionSummary returns the per-dataset encryption summary (P-3). It
// is a job whose RESULT is a list; the call awaits the job (a FAILED job is an
// error) and then reads the result back through core.get_jobs. The driver reads
// Locked and ValidKey to gate unlock (P-8) and to report health (E-3 §2).
func (c *Client) DatasetEncryptionSummary(ctx context.Context, name string) ([]EncryptionSummaryEntry, error) {
	result, err := c.Call(ctx, datasetEncryptionSummaryMethod, name)
	if err != nil {
		return nil, fmt.Errorf("failed to dispatch %s: %w", datasetEncryptionSummaryMethod, err)
	}
	jobID, err := replicationJobID(result)
	if err != nil {
		return nil, fmt.Errorf("%s returned an unusable job id: %w", datasetEncryptionSummaryMethod, err)
	}
	if err = c.waitForJob(ctx, jobID); err != nil {
		return nil, fmt.Errorf("%s job %d failed: %w", datasetEncryptionSummaryMethod, jobID, err)
	}
	raw, err := c.fetchJobResult(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return parseEncryptionSummary(raw)
}

// fetchJobResult reads a completed job's result payload back through
// core.get_jobs. waitForJob only reports terminal state, not the result, so a
// result-bearing job (encryption_summary) re-reads the row once it is terminal.
func (c *Client) fetchJobResult(ctx context.Context, jobID int64) (interface{}, error) {
	filters := [][]interface{}{{"id", "=", jobID}}
	result, err := c.Call(ctx, "core.get_jobs", filters)
	if err != nil {
		return nil, fmt.Errorf("failed to query job %d result: %w", jobID, err)
	}
	jobs, ok := result.([]interface{})
	if !ok || len(jobs) == 0 {
		return nil, fmt.Errorf("job %d result not found", jobID)
	}
	job, ok := jobs[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected job response type %T", jobs[0])
	}
	return job["result"], nil
}

// parseEncryptionSummary decodes the P-3 result list into typed entries.
func parseEncryptionSummary(raw interface{}) ([]EncryptionSummaryEntry, error) {
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected encryption_summary result type %T", raw)
	}
	entries := make([]EncryptionSummaryEntry, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected encryption_summary entry type %T", item)
		}
		entry := EncryptionSummaryEntry{}
		if v, ok := m["name"].(string); ok {
			entry.Name = v
		}
		if v, ok := m["key_format"].(string); ok {
			entry.KeyFormat = v
		}
		if v, ok := m["key_present_in_database"].(bool); ok {
			entry.KeyPresentInDatabase = v
		}
		if v, ok := m["valid_key"].(bool); ok {
			entry.ValidKey = v
		}
		if v, ok := m["locked"].(bool); ok {
			entry.Locked = v
		}
		if v, ok := m["unlock_error"].(string); ok {
			entry.UnlockError = v
		}
		if v, ok := m["unlock_successful"].(bool); ok {
			entry.UnlockSuccessful = v
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
