package truenas

import (
	"context"
	"fmt"
	"strings"
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

// SnapshotTaskListByDataset returns EVERY periodic-snapshot task scoped to
// dataset. It deliberately returns the full list rather than "the first task":
// a dataset may legitimately carry a pre-existing FOREIGN task alongside the
// driver's own, and the caller must select its own by naming-schema provenance
// rather than adopting whichever task the backend happens to return first
// (GF2-fix/H2 — first-match adoption both mis-adopted foreign tasks and later
// authorized deleting them at DeleteVolume as if the driver owned them).
func (c *Client) SnapshotTaskListByDataset(ctx context.Context, dataset string) ([]*SnapshotTask, error) {
	filters := [][]interface{}{{"dataset", "=", dataset}}
	return c.snapshotTaskQuery(ctx, filters, "dataset "+dataset)
}

// SnapshotTaskListByParent returns every periodic-snapshot task whose dataset
// lives below parentDataset. It is the input to the stranded-task sweep
// (GF2-fix/H2): a task whose volume dataset is gone can only be found this way,
// because no dataset is left to carry its binding property.
func (c *Client) SnapshotTaskListByParent(ctx context.Context, parentDataset string) ([]*SnapshotTask, error) {
	prefix := strings.TrimSuffix(parentDataset, "/") + "/"
	filters := [][]interface{}{{"dataset", "^", prefix}}
	return c.snapshotTaskQuery(ctx, filters, "parent "+parentDataset)
}

func (c *Client) snapshotTaskQuery(ctx context.Context, filters [][]interface{}, scope string) ([]*SnapshotTask, error) {
	var tasks []SnapshotTask
	if err := callTyped(ctx, c, &tasks, "pool.snapshottask.query", filters, map[string]interface{}{}); err != nil {
		return nil, fmt.Errorf("failed to query snapshot tasks for %s: %w", scope, err)
	}
	out := make([]*SnapshotTask, 0, len(tasks))
	for i := range tasks {
		out = append(out, &tasks[i])
	}
	return out, nil
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
