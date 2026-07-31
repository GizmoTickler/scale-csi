package truenas

import (
	"context"
	"fmt"
)

// SnapshotTask is a TrueNAS periodic-snapshot task (pool.snapshottask.*). The
// driver owns ONE non-recursive task per scheduled volume dataset (GF2/E2) so a
// PVC gets automatic PITR with bounded time-based retention without an external
// scheduler or a box-wide task covering the CSI parent.
type SnapshotTask struct {
	ID            int               `json:"id"`
	Dataset       string            `json:"dataset"`
	Recursive     bool              `json:"recursive"`
	NamingSchema  string            `json:"naming_schema"`
	Schedule      map[string]string `json:"schedule"`
	LifetimeValue int               `json:"lifetime_value"`
	LifetimeUnit  string            `json:"lifetime_unit"`
	Enabled       bool              `json:"enabled"`
	AllowEmpty    bool              `json:"allow_empty"`
	Exclude       []string          `json:"exclude"`
}

// SnapshotTaskCreateParams holds the parameters for creating a periodic-snapshot
// task. The live probe (P2) proved per-dataset, non-recursive scoping works and
// that retention is TIME-based only (lifetime_value + lifetime_unit); 26.0-BETA.1
// has NO max_count field, so count caps are a driver-side concern, not an API one.
type SnapshotTaskCreateParams struct {
	Dataset       string            `json:"dataset"`
	Recursive     bool              `json:"recursive"`
	NamingSchema  string            `json:"naming_schema"`
	Schedule      map[string]string `json:"schedule"`
	LifetimeValue int               `json:"lifetime_value"`
	LifetimeUnit  string            `json:"lifetime_unit"`
	Enabled       bool              `json:"enabled"`
	AllowEmpty    bool              `json:"allow_empty"`
}

// SnapshotTaskCreate creates a periodic-snapshot task scoped to a single dataset.
// The driver always creates non-recursive tasks (recursive:false) so a volume's
// task never snapshots the CSI parent or sibling volumes (P2).
func (c *Client) SnapshotTaskCreate(ctx context.Context, params *SnapshotTaskCreateParams) (*SnapshotTask, error) {
	var task SnapshotTask
	if err := callTyped(ctx, c, &task, "pool.snapshottask.create", params); err != nil {
		return nil, fmt.Errorf("failed to create snapshot task for dataset %s: %w", params.Dataset, err)
	}
	return &task, nil
}

// SnapshotTaskFindByDataset returns the periodic-snapshot task scoped to dataset,
// or nil when none exists. It is the idempotency probe CreateVolume uses before
// creating a task so a retry does not duplicate tasks.
func (c *Client) SnapshotTaskFindByDataset(ctx context.Context, dataset string) (*SnapshotTask, error) {
	filters := [][]interface{}{{"dataset", "=", dataset}}
	var tasks []SnapshotTask
	if err := callTyped(ctx, c, &tasks, "pool.snapshottask.query", filters, map[string]interface{}{}); err != nil {
		return nil, fmt.Errorf("failed to query snapshot tasks for dataset %s: %w", dataset, err)
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return &tasks[0], nil
}

// SnapshotTaskUpdate updates an existing periodic-snapshot task in place.
func (c *Client) SnapshotTaskUpdate(ctx context.Context, id int, params *SnapshotTaskCreateParams) error {
	if _, err := c.Call(ctx, "pool.snapshottask.update", id, params); err != nil {
		return fmt.Errorf("failed to update snapshot task %d: %w", id, err)
	}
	return nil
}

// SnapshotTaskDelete deletes a periodic-snapshot task by id. Deleting an
// already-absent task is success (idempotent) so DeleteVolume cleanup never fails
// on a task a peer or operator already removed.
func (c *Client) SnapshotTaskDelete(ctx context.Context, id int) error {
	if _, err := c.Call(ctx, "pool.snapshottask.delete", id); err != nil {
		if IsNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("failed to delete snapshot task %d: %w", id, err)
	}
	return nil
}
