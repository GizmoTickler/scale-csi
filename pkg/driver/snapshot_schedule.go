package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
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
//
// NOTE (GF2-fix/B1): there is deliberately NO snapshotNamingSchema parameter.
// The naming schema is the driver's ONLY provenance for a task-created snapshot
// (those snapshots carry no CSI user properties — P2 — and TrueNAS 26.0 cannot
// add properties to an existing snapshot), so the driver must mint it itself,
// with an unguessable per-volume nonce. A caller-chosen schema would let any
// snapshot whose name an outsider can predict be classified as driver-owned.
const (
	paramSnapshotSchedule  = "snapshotSchedule"
	paramSnapshotRetention = "snapshotRetention"
)

// PropSnapshotNamingSchema records the strftime naming schema a volume's
// driver-owned periodic-snapshot task uses. It is the ownership provenance for
// task-created snapshots (which carry NO CSI user properties, P2): its embedded
// per-volume nonce is driver-minted and never published anywhere but this
// property and the task object itself (GF2/E2, R4).
const PropSnapshotNamingSchema = "truenas-csi:snapshot_naming_schema"

// defaultSnapshotRetention bounds a scheduled task's snapshot lifetime when no
// retention is configured, so an enabled schedule can never grow unbounded
// snapshots (TrueNAS 26.0 retention is time-based only, P2/R6).
const (
	defaultSnapshotRetentionValue = 30
	defaultSnapshotRetentionUnit  = "DAY"
)

// scheduledSnapshotSchemaPrefix and scheduledSnapshotTimestampSuffix bracket the
// driver-minted naming schema:
//
//	csi-<sanitized volume id>-<nonce>-%Y%m%d-%H%M%S
//
// Both the volume id and the 16-hex-character nonce are re-derived from the
// stamped value before it is trusted, exactly as the tombstone reaper re-derives
// a tombstone name through its production rename algorithm before authorizing a
// destroy (snapshotMatchesRetainedTombstoneIdentity).
const (
	scheduledSnapshotSchemaPrefix    = "csi-"
	scheduledSnapshotTimestampSuffix = "-%Y%m%d-%H%M%S"
	scheduledSnapshotNonceBytes      = 8
)

// scheduledSnapshotNoncePattern is the exact shape of a driver-minted nonce.
var scheduledSnapshotNoncePattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// scheduledSnapshotNamePattern matches the rendered output of the driver-minted
// schema: the literal prefix, the volume id, the nonce, then the FULL strftime
// timestamp expansion (8 digits, '-', 6 digits) — anchored at both ends. This is
// the structural half of the ownership proof; the nonce is the secret half.
var scheduledSnapshotNamePattern = regexp.MustCompile(`^csi-(.+)-([0-9a-f]{16})-\d{8}-\d{6}$`)

// snapshotTaskSpec is the resolved, validated periodic-snapshot configuration for
// a single CreateVolume request. A nil *snapshotTaskSpec means the volume is not
// scheduled (the common, default-off case).
type snapshotTaskSpec struct {
	schedule      map[string]string
	namingSchema  string
	lifetimeValue int
	lifetimeUnit  string
}

func (s *snapshotTaskSpec) createParams(datasetName string) *truenas.SnapshotTaskCreateParams {
	return &truenas.SnapshotTaskCreateParams{
		Dataset:       datasetName,
		Recursive:     false,
		NamingSchema:  s.namingSchema,
		Schedule:      s.schedule,
		LifetimeValue: s.lifetimeValue,
		LifetimeUnit:  s.lifetimeUnit,
		Enabled:       true,
		AllowEmpty:    true,
	}
}

// matchesTask reports whether an existing backend task already carries exactly
// this spec, so the adopt path can converge a drifted task instead of silently
// leaving the StorageClass's schedule/retention unapplied (GF2-fix/H2).
func (s *snapshotTaskSpec) matchesTask(task *truenas.SnapshotTask) bool {
	if task == nil || task.Recursive || !task.Enabled ||
		task.NamingSchema != s.namingSchema ||
		task.LifetimeValue != s.lifetimeValue ||
		task.LifetimeUnit != s.lifetimeUnit ||
		len(task.Schedule) != len(s.schedule) {
		return false
	}
	for key, want := range s.schedule {
		if task.Schedule[key] != want {
			return false
		}
	}
	return true
}

// resolveSnapshotTaskSpec resolves the per-StorageClass periodic-snapshot
// parameters against the controller-wide zfs defaults and validates them. It
// returns (nil, nil) when the resolved schedule is empty (periodic snapshots
// off). A present-but-empty StorageClass parameter opts a class out even when a
// global default is set, mirroring the snapshotRestoreMode resolution precedent.
//
// The minted naming schema carries a fresh per-volume nonce; ensureSnapshotTask
// substitutes an already-stamped schema for the same volume so a CreateVolume
// retry never re-mints (and therefore never orphans) the ownership proof of the
// snapshots an existing task already produced.
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

	parsedSchedule, err := parseSnapshotSchedule(schedule)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid %s %q: %v", paramSnapshotSchedule, schedule, err)
	}
	lifetimeValue, lifetimeUnit, err := parseSnapshotRetention(retention)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid %s %q: %v", paramSnapshotRetention, retention, err)
	}

	namingSchema, err := newDriverScheduledNamingSchema(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mint periodic-snapshot naming schema: %v", err)
	}

	return &snapshotTaskSpec{
		schedule:      parsedSchedule,
		namingSchema:  namingSchema,
		lifetimeValue: lifetimeValue,
		lifetimeUnit:  lifetimeUnit,
	}, nil
}

// newDriverScheduledNamingSchema mints a per-volume-unique strftime naming
// schema carrying an unguessable driver-minted nonce. The nonce is what turns
// the schema from a *name convention* (which any actor can imitate — the B1
// data-destruction hazard) into positive ownership provenance for snapshots that
// can carry no properties of their own.
func newDriverScheduledNamingSchema(volumeID string) (string, error) {
	buf := make([]byte, scheduledSnapshotNonceBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return driverScheduledNamingSchema(volumeID, hex.EncodeToString(buf)), nil
}

// driverScheduledNamingSchema is the single production algorithm that renders a
// schema from (volumeID, nonce). Verification re-runs it and demands exact
// equality — the same discipline as snapshotTombstoneName.
func driverScheduledNamingSchema(volumeID, nonce string) string {
	return scheduledSnapshotSchemaPrefix + sanitizeVolumeID(volumeID) + "-" + nonce + scheduledSnapshotTimestampSuffix
}

// schemaProvesVolumeOwnership re-derives (volumeID, nonce) from a stamped schema
// and requires that the production algorithm reproduces the stamped value
// byte-for-byte for THIS volume. A schema for a different volume, a
// hand-written one, or one with a malformed nonce proves nothing and is
// rejected.
func schemaProvesVolumeOwnership(schema, volumeID string) bool {
	if schema == "" || volumeID == "" {
		return false
	}
	nonce, ok := scheduledSchemaNonce(schema)
	if !ok {
		return false
	}
	return schema == driverScheduledNamingSchema(volumeID, nonce)
}

// scheduledSchemaNonce extracts the nonce from a driver-minted schema.
func scheduledSchemaNonce(schema string) (string, bool) {
	trimmed, ok := strings.CutSuffix(schema, scheduledSnapshotTimestampSuffix)
	if !ok || !strings.HasPrefix(trimmed, scheduledSnapshotSchemaPrefix) {
		return "", false
	}
	idx := strings.LastIndex(trimmed, "-")
	if idx < 0 {
		return "", false
	}
	nonce := trimmed[idx+1:]
	if !scheduledSnapshotNoncePattern.MatchString(nonce) {
		return "", false
	}
	return nonce, true
}

// scheduledSnapshotNameMatchesSchema reports whether a snapshot's short name is
// EXACTLY an output the given driver-minted schema could have produced: the
// literal prefix, the same volume id, the same nonce, and a fully-formed
// strftime timestamp. Nothing about this is a prefix test — the previous
// `strings.HasPrefix(name, "csi-")` classifier authorized destroying any
// operator snapshot beginning with "csi-" (GF2-fix/B1).
func scheduledSnapshotNameMatchesSchema(name, schema string) bool {
	match := scheduledSnapshotNamePattern.FindStringSubmatch(name)
	if match == nil {
		return false
	}
	// Re-render through the production algorithm from the parsed parts and
	// demand exact equality with the stamped schema.
	return driverScheduledNamingSchema(match[1], match[2]) == schema
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

// ensureSnapshotTask creates (or converges) the driver-owned periodic-snapshot
// task for a volume dataset (GF2/E2).
//
// ORDERING (GF2-fix/H2): the naming-schema binding is stamped on the dataset
// BEFORE pool.snapshottask.create, and the stamp failure is fatal to the ensure.
// The previous create-then-best-effort-stamp order could strand a live task
// forever behind a lost binding — the invisible-leak class this codebase keeps
// hitting — because DeleteVolume's cleanup short-circuits on a missing binding.
// With stamp-first the binding is always at least as durable as the task: a
// crash after the stamp but before the create leaves only a harmless property
// (the next create converges it, and the stranded-task sweep needs nothing);
// a crash after the create is covered because the schema stamp is already there.
//
// A task failure is non-fatal — logged, metered, and surfaced as a warning event
// — so a backend hiccup never blocks volume provisioning.
func (d *Driver) ensureSnapshotTask(ctx context.Context, dataset *truenas.Dataset, datasetName, volumeID string, spec *snapshotTaskSpec, req *csi.CreateVolumeRequest) {
	if spec == nil {
		return
	}
	// Keep an already-minted nonce across retries: re-minting would orphan the
	// ownership proof for every snapshot the existing task already created.
	if existing := datasetLocalUserProperty(dataset, PropSnapshotNamingSchema); schemaProvesVolumeOwnership(existing, volumeID) {
		spec = &snapshotTaskSpec{
			schedule:      spec.schedule,
			namingSchema:  existing,
			lifetimeValue: spec.lifetimeValue,
			lifetimeUnit:  spec.lifetimeUnit,
		}
	}
	// 1. Durable binding FIRST. Without it the driver could not later prove it
	//    owns the task, nor prove the task's snapshots are its own.
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropSnapshotNamingSchema: spec.namingSchema,
	}); err != nil {
		d.recordSnapshotTaskWarning(req, volumeID,
			fmt.Sprintf("could not stamp the periodic-snapshot binding (no task was created): %v", err))
		return
	}

	tasks, err := d.truenasClient.SnapshotTaskListByDataset(ctx, datasetName)
	if err != nil {
		d.recordSnapshotTaskWarning(req, volumeID, fmt.Sprintf("could not look up existing periodic-snapshot tasks: %v", err))
		return
	}
	// 2. Adopt only a task this driver PROVABLY minted for this volume. A
	//    pre-existing foreign task on the same dataset is left strictly alone:
	//    adopting it would stamp its id as driver-owned and authorize deleting it
	//    at DeleteVolume, and its snapshots would never match our schema anyway.
	task := driverOwnedTask(tasks, spec.namingSchema)
	switch {
	case task == nil:
		task, err = d.truenasClient.SnapshotTaskCreate(ctx, spec.createParams(datasetName))
		if err != nil {
			d.recordSnapshotTaskWarning(req, volumeID, fmt.Sprintf("failed to create periodic-snapshot task: %v", err))
			return
		}
		klog.Infof("Created driver-managed periodic-snapshot task %d for volume %s (schedule %v, retention %d%s)",
			task.ID, volumeID, spec.schedule, spec.lifetimeValue, spec.lifetimeUnit)
	case !spec.matchesTask(task):
		// Converge a drifted task to the requested spec rather than leaving the
		// StorageClass's schedule/retention silently unapplied (GF2-fix/H2).
		if err := d.truenasClient.SnapshotTaskUpdate(ctx, task.ID, spec.createParams(datasetName)); err != nil {
			d.recordSnapshotTaskWarning(req, volumeID, fmt.Sprintf("failed to converge periodic-snapshot task %d: %v", task.ID, err))
			return
		}
		klog.Infof("Converged driver-managed periodic-snapshot task %d for volume %s to the requested schedule/retention", task.ID, volumeID)
	}
	RecordScheduledSnapshotTaskEnsured()

	// 3. Record the exact id last. Its absence is recoverable (the delete path
	//    re-queries by dataset and re-proves ownership by schema); its presence
	//    just saves that query.
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropSnapshotTaskID: strconv.Itoa(task.ID),
	}); err != nil {
		klog.Warningf("Failed to stamp periodic-snapshot task id on volume %s (the schema binding still resolves it): %v", volumeID, err)
	}
}

// driverOwnedTask picks the task whose naming schema is EXACTLY the schema this
// driver minted for this volume. Any other task on the dataset is foreign.
func driverOwnedTask(tasks []*truenas.SnapshotTask, namingSchema string) *truenas.SnapshotTask {
	for _, task := range tasks {
		if task != nil && task.NamingSchema == namingSchema {
			return task
		}
	}
	return nil
}

// deleteVolumeSnapshotTask removes the driver-owned periodic-snapshot task bound
// to a volume dataset before the dataset is destroyed (GF2/E2).
//
// It NEVER blocks DeleteVolume (GF2-fix/H2): every failure is logged and
// metered, and the caller proceeds. It also never deletes a task it cannot prove
// it owns — the stamped id is trusted only after the dataset's schema binding
// has been re-derived for this volume, so a stamped id that points at a
// pre-existing foreign task (the old first-match adoption bug) cannot authorize
// its deletion.
func (d *Driver) deleteVolumeSnapshotTask(ctx context.Context, dataset *truenas.Dataset, datasetName, volumeID string) {
	schema := datasetLocalUserProperty(dataset, PropSnapshotNamingSchema)
	if !schemaProvesVolumeOwnership(schema, volumeID) {
		// A volume that was never scheduled carries no binding: skip entirely so
		// the default DeleteVolume path makes zero extra calls (GF2/E2
		// default-off invariant).
		if schema != "" {
			klog.Warningf("Volume dataset %s carries an unprovable %s=%q; refusing to delete any periodic-snapshot task",
				datasetName, PropSnapshotNamingSchema, schema)
		}
		return
	}

	tasks, err := d.truenasClient.SnapshotTaskListByDataset(ctx, datasetName)
	if err != nil {
		klog.Warningf("Failed to look up periodic-snapshot tasks for volume dataset %s during delete (continuing; the sweep will retire a stranded task): %v", datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
		return
	}
	task := driverOwnedTask(tasks, schema)
	if task == nil {
		return
	}
	if err := d.truenasClient.SnapshotTaskDelete(ctx, task.ID); err != nil {
		klog.Warningf("Failed to delete periodic-snapshot task %d for volume dataset %s (continuing; the sweep will retire it): %v", task.ID, datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
	}
}

// driverScheduledSnapshotProvenance is the SINGLE ownership predicate for
// task-created snapshots (GF2-fix/B1).
//
// Task-created snapshots carry NO CSI user properties (P2) and TrueNAS 26.0
// cannot add properties to an existing snapshot, so per-snapshot provenance must
// come from something the driver alone controls. That is the naming schema's
// per-volume NONCE. A snapshot is proven driver-owned only when ALL of:
//
//  1. it is not a CSI snapshot or tombstone (those have their own provenance);
//  2. it lives on EXACTLY the volume's own dataset;
//  3. that dataset carries THIS driver instance's ownership stamp;
//  4. that dataset carries a naming-schema binding that the driver's own
//     production algorithm reproduces byte-for-byte for THIS volume id (so the
//     nonce is one the driver minted, not one an outsider chose);
//  5. the snapshot's short name is exactly a rendering of that schema — same
//     volume id, same nonce, full strftime timestamp expansion.
//
// If any link is missing the answer is NO and the snapshot stays FOREIGN, which
// means the default policy refuses to destroy it. Unprovable never means
// deletable.
//
// requireLocalSource distinguishes the two call sites. Delete-authorizing callers
// (DeleteVolume's foreign guard) pass true and read the dataset from
// pool.dataset.query, which carries property sources, so a value INHERITED from a
// clone origin can never masquerade as a local ownership stamp. The metrics-only
// reconcile caller passes false because TrueNAS 26.0's zfs.resource.query
// projection strips per-property source entirely; that path only increments a
// gauge and can never delete anything.
func driverScheduledSnapshotProvenance(
	snap *truenas.Snapshot,
	dataset *truenas.Dataset,
	driverInstanceID string,
	requireLocalSource bool,
) bool {
	if snap == nil || dataset == nil || isCSISnapshot(snap) || isSnapshotTombstone(snap) {
		return false
	}
	if snap.Dataset != dataset.Name {
		return false
	}
	if requireLocalSource {
		if !datasetHasLocalUserProperty(dataset, PropDriverInstanceID, driverInstanceID) {
			return false
		}
	} else if datasetUserProperty(dataset, PropDriverInstanceID) != driverInstanceID || driverInstanceID == "" {
		return false
	}

	schema := ""
	if requireLocalSource {
		schema = datasetLocalUserProperty(dataset, PropSnapshotNamingSchema)
	} else {
		schema = datasetUserProperty(dataset, PropSnapshotNamingSchema)
	}
	// The volume id is the dataset's own base name; the schema must prove out
	// against it, which also rejects a schema inherited from another volume.
	if !schemaProvesVolumeOwnership(schema, datasetVolumeID(dataset.Name)) {
		return false
	}
	return scheduledSnapshotNameMatchesSchema(snapshotShortName(snap), schema)
}

// isDriverScheduledSnapshot is the delete-authorizing form of the provenance
// predicate: strict property sources required.
func isDriverScheduledSnapshot(snap *truenas.Snapshot, dataset *truenas.Dataset, driverInstanceID string) bool {
	return driverScheduledSnapshotProvenance(snap, dataset, driverInstanceID, true)
}

// datasetVolumeID derives a volume id from a dataset path.
func datasetVolumeID(datasetName string) string {
	if idx := strings.LastIndex(datasetName, "/"); idx >= 0 {
		return datasetName[idx+1:]
	}
	return datasetName
}

// foreignSnapshotsOnly filters out PROVEN driver-owned scheduled snapshots
// (GF2/E2, R4) from a volume's snapshot list, returning every snapshot the
// foreign-snapshot guard must still police. Anything the ownership chain above
// cannot prove stays foreign and is therefore preserved by the default policy.
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

// scheduledSnapshotsConfigured reports whether this controller has any
// driver-managed periodic-snapshot configuration at all. It gates the
// stranded-task sweep so a deployment that never enabled scheduling issues zero
// pool.snapshottask.* calls (the GF2 default-off invariant).
func (d *Driver) scheduledSnapshotsConfigured() bool {
	return d.config != nil && strings.TrimSpace(d.config.ZFS.SnapshotSchedule) != ""
}

// sweepStrandedSnapshotTasks deletes periodic-snapshot tasks that the driver
// provably minted but whose volume dataset no longer exists (GF2-fix/H2).
//
// This is the sweep the old code's comment falsely claimed already existed. A
// task is deleted only when BOTH:
//   - its dataset lives below this driver's parent dataset AND is absent from
//     the pass's managed-dataset listing and from a fresh existence check, and
//   - its naming schema is exactly one this driver's production algorithm mints
//     for that dataset's volume id (nonce and all) — foreign tasks, box-wide
//     tasks, and other instances' tasks can never match.
//
// It runs only when this deployment uses scheduling at all: either the
// controller-wide schedule is configured, or the pass observed at least one
// dataset carrying a schema binding. A deployment that never scheduled anything
// therefore makes zero extra API calls. (Documented limitation: an
// SC-only-scheduled deployment whose LAST scheduled volume was already deleted
// has no observable signal left, so the sweep does not run for it; the
// stamp-before-create ordering is what actually prevents that leak, the sweep is
// the belt.)
func (d *Driver) sweepStrandedSnapshotTasks(ctx context.Context, datasets []*truenas.Dataset, report *ReconcileReport, deleteEnabled bool) {
	scheduledObserved := d.scheduledSnapshotsConfigured()
	live := make(map[string]struct{}, len(datasets))
	for _, ds := range datasets {
		if ds == nil {
			continue
		}
		live[ds.Name] = struct{}{}
		if datasetUserProperty(ds, PropSnapshotNamingSchema) != "" {
			scheduledObserved = true
		}
	}
	if !scheduledObserved {
		return
	}

	tasks, err := d.truenasClient.SnapshotTaskListByParent(ctx, d.parentDatasetName())
	if err != nil {
		d.recordReconcileObjectFailure("snapshot_task_sweep", d.parentDatasetName(), err)
		return
	}
	for _, task := range tasks {
		if ctx.Err() != nil {
			return
		}
		if task == nil || task.Recursive {
			continue
		}
		if !schemaProvesVolumeOwnership(task.NamingSchema, datasetVolumeID(task.Dataset)) {
			continue // foreign task: never touched
		}
		if _, alive := live[task.Dataset]; alive {
			continue
		}
		// Confirm absence authoritatively before deleting: the pass's listing may
		// simply not have covered it (unstamped, mid-create, transient).
		exists, existsErr := d.truenasClient.DatasetExists(ctx, task.Dataset)
		if existsErr != nil {
			d.recordReconcileObjectFailure("snapshot_task_sweep", task.Dataset, existsErr)
			continue
		}
		if exists {
			continue
		}
		report.StrandedSnapshotTasks = append(report.StrandedSnapshotTasks, task.Dataset)
		if !deleteEnabled {
			continue
		}
		if err := d.truenasClient.SnapshotTaskDelete(ctx, task.ID); err != nil {
			d.recordReconcileObjectFailure("snapshot_task_sweep", task.Dataset, err)
			continue
		}
		RecordStrandedSnapshotTaskReaped()
		report.DeletedSnapshotTasks = append(report.DeletedSnapshotTasks, task.Dataset)
		klog.Infof("Orphan reconcile: deleted stranded periodic-snapshot task %d for destroyed volume dataset %s", task.ID, task.Dataset)
	}
}

func (d *Driver) recordSnapshotTaskWarning(req *csi.CreateVolumeRequest, volumeID, message string) {
	RecordScheduledSnapshotTaskEnsureFailed()
	klog.Warningf("Periodic-snapshot task not ensured for volume %s (continuing without PITR): %s", volumeID, message)
	d.recordWarningEvent(createVolumeEventRef(req), EventReasonSnapshotTaskFailed,
		fmt.Sprintf("Volume %s was provisioned but its driver-managed periodic-snapshot task was not created: %s", volumeID, message))
}
