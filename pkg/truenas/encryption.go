package truenas

import (
	"context"
	"fmt"
)

// ZFS-native encryption at rest (GF-Sprint 1). Every method here is a TrueNAS
// @job: the call dispatches, returns a job id, and is awaited through the shared
// job-wait path (waitForJob), so a job that ends in a terminal failure state —
// including the fail-closed wrong-passphrase unlock (P-5) — surfaces as an error.
//
// All shapes are pinned to the nas01 26.0.0-BETA.1 probes (design §0). Do not
// "correct" them from ZFS or TrueNAS documentation.
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
// Unlock is NOT idempotent: unlocking an already-unlocked dataset returns a
// FAILED job (P-8). Callers MUST gate on locked==true (read via
// DatasetEncryptionSummary) before calling. A wrong passphrase is a FAILED job
// (P-5) and surfaces here as an error — fail-closed is native.
func (c *Client) DatasetUnlock(ctx context.Context, name, passphrase string) error {
	options := map[string]interface{}{
		"datasets": []map[string]interface{}{
			{"name": name, "passphrase": passphrase},
		},
		"toggle_attachments": true,
	}
	return c.dispatchDatasetJob(ctx, datasetUnlockMethod, name, options)
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
