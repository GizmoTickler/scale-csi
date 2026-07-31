package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// GF2/E2 StorageClass parameters for driver-managed periodic snapshots. Each is
// optional; an absent parameter falls back to the controller-wide zfs default,
// and an empty resolved schedule disables periodic snapshots for the volume.
const (
	paramSnapshotSchedule     = "snapshotSchedule"
	paramSnapshotRetention    = "snapshotRetention"
	paramSnapshotNamingSchema = "snapshotNamingSchema"
)

// PropSnapshotNamingSchema records the strftime naming schema a volume's
// driver-owned periodic-snapshot task uses, so DeleteVolume's foreign-snapshot
// scan can recognize task-created snapshots (which carry NO CSI user properties,
// P2) as driver-owned rather than foreign (GF2/E2, R4).
const PropSnapshotNamingSchema = "truenas-csi:snapshot_naming_schema"

// defaultSnapshotRetention bounds a scheduled task's snapshot lifetime when no
// retention is configured, so an enabled schedule can never grow unbounded
// snapshots (TrueNAS 26.0 retention is time-based only, P2/R6).
const (
	defaultSnapshotRetentionValue = 30
	defaultSnapshotRetentionUnit  = "DAY"
)

// snapshotTaskSpec is the resolved, validated periodic-snapshot configuration for
// a single CreateVolume request. A nil *snapshotTaskSpec means the volume is not
// scheduled (the common, default-off case).
type snapshotTaskSpec struct {
	schedule      map[string]string
	namingSchema  string
	lifetimeValue int
	lifetimeUnit  string
}

// resolveSnapshotTaskSpec resolves the per-StorageClass periodic-snapshot
// parameters against the controller-wide zfs defaults and validates them. It
// returns (nil, nil) when the resolved schedule is empty (periodic snapshots
// off). A present-but-empty StorageClass parameter opts a class out even when a
// global default is set, mirroring the snapshotRestoreMode resolution precedent.
func (d *Driver) resolveSnapshotTaskSpec(params map[string]string, volumeID string) (*snapshotTaskSpec, error) {
	schedule := d.config.ZFS.SnapshotSchedule
	if raw, ok := params[paramSnapshotSchedule]; ok {
		schedule = raw
	}
	schedule = strings.TrimSpace(schedule)
	if schedule == "" {
		return nil, nil
	}

	retention := d.config.ZFS.SnapshotRetention
	if raw, ok := params[paramSnapshotRetention]; ok {
		retention = raw
	}
	namingSchema := strings.TrimSpace(params[paramSnapshotNamingSchema])
	if namingSchema == "" {
		namingSchema = defaultSnapshotNamingSchema(volumeID)
	}

	parsedSchedule, err := parseSnapshotSchedule(schedule)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid %s %q: %v", paramSnapshotSchedule, schedule, err)
	}
	lifetimeValue, lifetimeUnit, err := parseSnapshotRetention(retention)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid %s %q: %v", paramSnapshotRetention, retention, err)
	}

	return &snapshotTaskSpec{
		schedule:      parsedSchedule,
		namingSchema:  namingSchema,
		lifetimeValue: lifetimeValue,
		lifetimeUnit:  lifetimeUnit,
	}, nil
}

// defaultSnapshotNamingSchema builds a per-volume-unique strftime naming schema so
// two volumes' scheduled snapshots never collide and the schema's literal prefix
// ("csi-") lets DeleteVolume recognize them (GF2/E2).
func defaultSnapshotNamingSchema(volumeID string) string {
	return "csi-%Y%m%d-%H%M%S-" + volumeID
}

// parseSnapshotSchedule parses a five-field cron string "minute hour dom month
// dow" into the TrueNAS pool.snapshottask schedule map.
func parseSnapshotSchedule(schedule string) (map[string]string, error) {
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return nil, fmt.Errorf("expected 5 cron fields (minute hour dom month dow), got %d", len(fields))
	}
	keys := []string{"minute", "hour", "dom", "month", "dow"}
	out := make(map[string]string, len(keys))
	for i, key := range keys {
		out[key] = fields[i]
	}
	return out, nil
}

// parseSnapshotRetention parses a bounded-retention duration like "24h", "30d",
// "2w", "6M"/"6mo", "1y" into a TrueNAS lifetime_value + lifetime_unit
// (HOUR|DAY|WEEK|MONTH|YEAR). An empty value resolves to the 30d safety bound.
// TrueNAS 26.0 exposes no count-based retention (P2), so this is time-based only.
func parseSnapshotRetention(retention string) (lifetimeValue int, lifetimeUnit string, err error) {
	retention = strings.TrimSpace(retention)
	if retention == "" {
		return defaultSnapshotRetentionValue, defaultSnapshotRetentionUnit, nil
	}
	lower := strings.ToLower(retention)
	var unit string
	var numStr string
	switch {
	case strings.HasSuffix(lower, "mo"):
		unit, numStr = "MONTH", retention[:len(retention)-2]
	case strings.HasSuffix(lower, "h"):
		unit, numStr = "HOUR", retention[:len(retention)-1]
	case strings.HasSuffix(lower, "d"):
		unit, numStr = "DAY", retention[:len(retention)-1]
	case strings.HasSuffix(lower, "w"):
		unit, numStr = "WEEK", retention[:len(retention)-1]
	case strings.HasSuffix(lower, "m"):
		unit, numStr = "MONTH", retention[:len(retention)-1]
	case strings.HasSuffix(lower, "y"):
		unit, numStr = "YEAR", retention[:len(retention)-1]
	default:
		return 0, "", fmt.Errorf("missing unit suffix; use one of h (hour), d (day), w (week), M/mo (month), y (year)")
	}
	value, err := strconv.Atoi(strings.TrimSpace(numStr))
	if err != nil || value < 1 {
		return 0, "", fmt.Errorf("retention quantity %q must be a positive integer", numStr)
	}
	return value, unit, nil
}

// ensureSnapshotTask creates (or adopts) the driver-owned periodic-snapshot task
// for a volume dataset and binds its id + naming schema to the dataset via user
// properties (GF2/E2). Idempotent: an existing task scoped to the dataset is
// adopted rather than duplicated. A task failure is non-fatal — logged, metered,
// and surfaced as a warning event — so a backend hiccup never blocks volume
// provisioning; the volume simply runs without automatic PITR until the next
// create retry.
func (d *Driver) ensureSnapshotTask(ctx context.Context, datasetName, volumeID string, spec *snapshotTaskSpec, req *csi.CreateVolumeRequest) {
	if spec == nil {
		return
	}
	task, err := d.truenasClient.SnapshotTaskFindByDataset(ctx, datasetName)
	if err != nil {
		d.recordSnapshotTaskWarning(req, volumeID, fmt.Sprintf("could not look up existing periodic-snapshot task: %v", err))
		return
	}
	if task == nil {
		task, err = d.truenasClient.SnapshotTaskCreate(ctx, &truenas.SnapshotTaskCreateParams{
			Dataset:       datasetName,
			Recursive:     false,
			NamingSchema:  spec.namingSchema,
			Schedule:      spec.schedule,
			LifetimeValue: spec.lifetimeValue,
			LifetimeUnit:  spec.lifetimeUnit,
			Enabled:       true,
			AllowEmpty:    true,
		})
		if err != nil {
			d.recordSnapshotTaskWarning(req, volumeID, fmt.Sprintf("failed to create periodic-snapshot task: %v", err))
			return
		}
		klog.Infof("Created driver-managed periodic-snapshot task %d for volume %s (schedule %v, retention %d%s)",
			task.ID, volumeID, spec.schedule, spec.lifetimeValue, spec.lifetimeUnit)
	}
	RecordScheduledSnapshotTaskEnsured()

	// Bind the task id and naming schema to the dataset for DeleteVolume cleanup
	// and foreign-snapshot recognition. Best-effort: a stamp failure leaves the
	// task discoverable via the dataset-scoped query fallback.
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropSnapshotTaskID:       strconv.Itoa(task.ID),
		PropSnapshotNamingSchema: spec.namingSchema,
	}); err != nil {
		klog.Warningf("Failed to stamp periodic-snapshot task binding on volume %s: %v", volumeID, err)
	}
}

// deleteVolumeSnapshotTask removes the driver-owned periodic-snapshot task bound
// to a volume dataset before the dataset is destroyed (GF2/E2). It prefers the
// stamped task id and falls back to a dataset-scoped query. Absent tasks are
// success. Best-effort: a failure is logged but never blocks DeleteVolume, since
// the recursive dataset delete removes the task-created snapshots regardless and
// the orphan reconcile sweeps a stranded task.
func (d *Driver) deleteVolumeSnapshotTask(ctx context.Context, dataset *truenas.Dataset, datasetName string) {
	taskIDStr := ""
	hasBinding := false
	if dataset != nil {
		if prop, ok := dataset.UserProperties[PropSnapshotTaskID]; ok && prop.Value != "" {
			taskIDStr = prop.Value
			hasBinding = true
		}
		if prop, ok := dataset.UserProperties[PropSnapshotNamingSchema]; ok && prop.Value != "" {
			hasBinding = true
		}
	}
	// A volume that was never scheduled carries no task-binding property: skip
	// entirely so the default DeleteVolume path makes zero extra calls (GF2/E2
	// default-off invariant). Only a proven scheduled volume attempts task removal.
	if !hasBinding {
		return
	}
	if taskIDStr != "" {
		if id, err := strconv.Atoi(taskIDStr); err == nil {
			if err := d.truenasClient.SnapshotTaskDelete(ctx, id); err != nil {
				klog.Warningf("Failed to delete periodic-snapshot task %d for volume dataset %s: %v", id, datasetName, err)
			}
			return
		}
		klog.Warningf("Volume dataset %s carries an unparseable %s=%q; falling back to task query", datasetName, PropSnapshotTaskID, taskIDStr)
	}
	task, err := d.truenasClient.SnapshotTaskFindByDataset(ctx, datasetName)
	if err != nil {
		klog.Warningf("Failed to look up periodic-snapshot task for volume dataset %s during delete: %v", datasetName, err)
		return
	}
	if task == nil {
		return
	}
	if err := d.truenasClient.SnapshotTaskDelete(ctx, task.ID); err != nil {
		klog.Warningf("Failed to delete periodic-snapshot task %d for volume dataset %s: %v", task.ID, datasetName, err)
	}
}

// scheduledSnapshotLiteralPrefix returns the literal (non-strftime) prefix of a
// naming schema: everything before the first '%' directive. Task-created
// snapshots carry no CSI props (P2), so this prefix plus the dataset's ownership
// stamp is how the driver recognizes its own scheduled snapshots (GF2/E2, R4).
func scheduledSnapshotLiteralPrefix(namingSchema string) string {
	if idx := strings.Index(namingSchema, "%"); idx >= 0 {
		return namingSchema[:idx]
	}
	return namingSchema
}

// isDriverScheduledSnapshot reports whether a snapshot under a driver-owned
// volume dataset was created by the driver's periodic-snapshot task rather than
// by an external actor. Such snapshots carry NO CSI user properties (P2), so they
// are recognized by the task naming-schema's literal prefix on a dataset that
// carries this driver instance's ownership stamp. They are driver-owned (deleted
// with the volume), NOT foreign, and never a reaper/orphan candidate (R4).
func isDriverScheduledSnapshot(snap *truenas.Snapshot, dataset *truenas.Dataset, driverInstanceID string) bool {
	if snap == nil || dataset == nil || isCSISnapshot(snap) || isSnapshotTombstone(snap) {
		return false
	}
	if !datasetHasLocalUserProperty(dataset, PropDriverInstanceID, driverInstanceID) {
		return false
	}
	prop, ok := dataset.UserProperties[PropSnapshotNamingSchema]
	if !ok || prop.Value == "" {
		return false
	}
	prefix := scheduledSnapshotLiteralPrefix(prop.Value)
	return prefix != "" && strings.HasPrefix(snap.Name, prefix)
}

// foreignSnapshotsOnly filters out driver-owned scheduled snapshots (GF2/E2, R4)
// from a volume's snapshot list, returning only the genuinely foreign ones that
// the foreign-snapshot guard must police. Driver-scheduled snapshots carry no CSI
// props (P2) and would otherwise trip DeleteVolume's foreign guard; they are
// owned by the driver and deleted with the volume (the recursive dataset delete
// removes them once the task is gone), so they are never "foreign".
func (d *Driver) foreignSnapshotsOnly(snapshots []*truenas.Snapshot, dataset *truenas.Dataset) []*truenas.Snapshot {
	foreign := make([]*truenas.Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if isDriverScheduledSnapshot(snap, dataset, d.driverInstanceID()) {
			continue
		}
		foreign = append(foreign, snap)
	}
	return foreign
}

func (d *Driver) recordSnapshotTaskWarning(req *csi.CreateVolumeRequest, volumeID, message string) {
	RecordScheduledSnapshotTaskEnsureFailed()
	klog.Warningf("Periodic-snapshot task not ensured for volume %s (continuing without PITR): %s", volumeID, message)
	d.recordWarningEvent(createVolumeEventRef(req), EventReasonSnapshotTaskFailed,
		fmt.Sprintf("Volume %s was provisioned but its driver-managed periodic-snapshot task was not created: %s", volumeID, message))
}
