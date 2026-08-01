package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

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

// PropSnapshotTaskCorroboration records that THIS driver instance observed its
// OWN live, non-recursive, dataset-scoped periodic-snapshot task carrying this
// exact naming schema on this dataset (GF2-fix2/B1-b).
//
// It exists because the delete path deletes that task before the foreign-snapshot
// guard runs (GF2-fix/H2), so on a RETRY of a failed DeleteVolume the task is
// gone and the live-task requirement could never be met again — the volume's own
// scheduled snapshots would be reclassified foreign and the delete would be
// wedged forever. Writing the observation down before deleting the task keeps a
// retry decidable without weakening the first attempt. It is written only on the
// delete path, only for a volume that already proves out a schema binding, and
// only once.
const PropSnapshotTaskCorroboration = "truenas-csi:snapshot_task_corroboration"

// PropSnapshotTaskTimezone records the IANA timezone name the NAS was configured
// with at the moment this volume's periodic-snapshot task was created
// (GF2-fix3/B1-d).
//
// A task renders %Y%m%d-%H%M%S from the NAS's civil clock, so proving a
// task-created snapshot's name requires knowing WHICH clock. Reading only the
// CURRENT zone cannot detect every reconfiguration — New_York -> Toronto, or a
// switch to a fixed -05:00 evaluated against a winter-created snapshot, leaves
// the civil fields identical — so the zone in force at task-creation time is
// written down here and compared against the live value at delete time. Any
// difference is then detectable regardless of whether the offsets coincide, and
// the delete path fails CLOSED on a mismatch.
//
// It is WRITE-ONCE. A CreateVolume retry after a zone change must NOT re-stamp
// it: overwriting would launder exactly the reconfiguration this property
// exists to expose. It is read only through datasetLocalUserProperty, so a
// clone, a replication-received dataset, or a detached copy that INHERITS it
// proves nothing (the standing content-source rule).
const PropSnapshotTaskTimezone = "truenas-csi:snapshot_task_timezone"

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

// scheduledSnapshotTimestampLayout is the Go layout equivalent of the schema's
// strftime tail (%Y%m%d-%H%M%S). Parsing through it — rather than matching eight
// digits and six digits — is what makes a name with an impossible calendar or
// clock value (20260230-250000) unprovable (GF2-fix2/B1-c).
const scheduledSnapshotTimestampLayout = "20060102-150405"

// scheduledSnapshotCreationSkew is the ONLY tolerance in the name-vs-creation
// agreement check (GF2-fix2/B1-a), and it exists solely for CLOCK SKEW between
// the moment the middleware renders the name and the moment ZFS stamps
// `creation` (which has whole-second granularity). It is deliberately expressed
// in SECONDS: it is NOT an allowance for timezone or UTC-offset ambiguity —
// there is none left, because the NAS's civil zone is now read from the backend
// and the comparison is exact. Do not widen this to absorb an offset problem;
// a zone the driver cannot resolve must fail CLOSED instead.
const scheduledSnapshotCreationSkew = 2 * time.Second

// scheduledSnapshotNoncePattern is the exact shape of a driver-minted nonce.
var scheduledSnapshotNoncePattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// scheduledSnapshotNamePattern matches the rendered output of the driver-minted
// schema: the literal prefix, the volume id, the nonce, then the strftime
// timestamp expansion — anchored at both ends. The digit classes are only the
// lexer; parseScheduledSnapshotName is what enforces the canonical volume
// segment and the valid calendar/clock domain.
var scheduledSnapshotNamePattern = regexp.MustCompile(`^csi-(.+)-([0-9a-f]{16})-(\d{8}-\d{6})$`)

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

// parseScheduledSnapshotName reports whether a snapshot's short name is EXACTLY
// an output the given driver-minted schema could have produced, and returns the
// wall-clock instant the name encodes.
//
// Three things are required, and each closed a real hole (GF2-fix2/B1-c):
//
//  1. The captured volume segment must be its own CANONICAL rendering. The
//     previous version pushed the segment through sanitizeVolumeID before
//     re-rendering, so any non-canonical spelling that merely SANITIZES to the
//     dataset leaf (e.g. "Abc" for leaf "vAbc") re-rendered to the stamped schema
//     and was accepted as driver-owned.
//  2. Re-rendering through the production algorithm must reproduce the stamped
//     schema byte-for-byte — same volume id, same nonce.
//  3. The timestamp must be a REAL calendar instant, not merely eight digits and
//     six digits. 20260230-250000 is now rejected.
//
// Nothing about this is a prefix test — the original `strings.HasPrefix(name,
// "csi-")` classifier authorized destroying any operator snapshot beginning with
// "csi-" (GF2-fix/B1).
func parseScheduledSnapshotName(name, schema string) (encoded time.Time, ok bool) {
	match := scheduledSnapshotNamePattern.FindStringSubmatch(name)
	if match == nil {
		return time.Time{}, false
	}
	volumeSegment, nonce, timestamp := match[1], match[2], match[3]
	if sanitizeVolumeID(volumeSegment) != volumeSegment {
		return time.Time{}, false
	}
	if driverScheduledNamingSchema(volumeSegment, nonce) != schema {
		return time.Time{}, false
	}
	// Parsed as a bare civil wall clock. The zone is supplied separately by the
	// caller (the NAS's own, read from system.general.config), so UTC is only the
	// carrier here. The round-trip re-render is belt-and-braces against any
	// layout element the parser would normalize rather than reject.
	encoded, err := time.ParseInLocation(scheduledSnapshotTimestampLayout, timestamp, time.UTC)
	if err != nil || encoded.Format(scheduledSnapshotTimestampLayout) != timestamp {
		return time.Time{}, false
	}
	return encoded, true
}

// scheduledSnapshotCreationAgrees reports whether the civil instant a scheduled
// snapshot's NAME encodes is EXACTLY when the snapshot was actually created
// (GF2-fix2/B1-a).
//
// HOW IT WORKS. A TrueNAS periodic-snapshot task renders %Y%m%d-%H%M%S from the
// NAS's LOCAL civil clock at the moment it takes the snapshot, while the
// snapshot's `creation` property is UTC epoch seconds (verified against the
// captured 26.0 payloads in pkg/truenas/testdata: zfs.resource.snapshot.query
// returns `{"value":1754693322,"raw":"1754693322"}` and pool.snapshot.query
// returns `{"value":"1754693450","parsed":{"$date":1754693450000}}`). The NAS's
// zone is read from `system.general.config` -> `timezone` (live-verified on
// TrueNAS 26.0.0-BETA.1: "America/New_York") and cached, so the driver can
// convert `creation` INTO that civil clock and demand exact agreement.
//
// DIRECTION MATTERS. The conversion is epoch -> civil, which is total and
// unambiguous. Converting the other way (civil -> epoch) would be ambiguous
// during a DST fall-back hour and undefined during a spring-forward gap; doing
// it this way removes DST as a source of slack entirely, and tzdata gives the
// correct historical offset for any past instant.
//
// TOLERANCE. Exactly ±scheduledSnapshotCreationSkew (2 SECONDS), and only for
// clock skew between name render and creation stamp. There is no offset
// allowance, because there is no longer any offset uncertainty.
//
// CHANCE-PASS RATE, RE-DERIVED. The predecessor of this check accepted any name
// whose UTC delta fell within ±2min of a 15-minute quantum: 241 seconds in every
// 900, i.e. 26.8%, regardless of how far the forged timestamp strayed. This
// version accepts a fixed 5-second window (±2s inclusive) around one specific
// instant. For a timestamp chosen anywhere within a day that is 5/86400 =
// 5.8e-5; over a week, 8.3e-6; it keeps shrinking with the range, whereas the
// old rate did not shrink at all. So roughly a 4,600x improvement at
// day-scale and unbounded improvement beyond.
//
// It is NOT literally zero, and should not be described as such. An actor who
// creates the snapshot at the second its name encodes still passes — but that is
// the documented storage-administrator spoof case, which this change does not
// close and does not claim to. What it removes is the accidental-collision
// slack: a name that merely happens to look plausible no longer passes.
//
// FAIL CLOSED. A zero/absent `creation`, or a nil zone (the NAS timezone could
// not be read), means UNVERIFIABLE, which means NOT PROVEN, which means the
// snapshot stays FOREIGN and is preserved. The residual case this cannot detect
// is a NAS whose timezone SETTING was changed between snapshot creation and
// evaluation: the old snapshots' names then disagree under the new zone and are
// reclassified foreign. That is deliberate — the window is NOT widened to absorb
// it, because a false-foreign is a preserved snapshot while a false-owned is
// deleted data.
func scheduledSnapshotCreationAgrees(encoded time.Time, creationUnix int64, zone *time.Location) bool {
	if creationUnix <= 0 || zone == nil {
		return false
	}
	// creation (UTC epoch) rendered in the NAS's civil clock, then compared with
	// the civil clock the name encodes.
	civil := time.Unix(creationUnix, 0).In(zone)
	actual := time.Date(civil.Year(), civil.Month(), civil.Day(),
		civil.Hour(), civil.Minute(), civil.Second(), 0, time.UTC)
	delta := encoded.Unix() - actual.Unix()
	if delta < 0 {
		delta = -delta
	}
	return delta <= int64(scheduledSnapshotCreationSkew/time.Second)
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
	//
	//    The binding is (schema, timezone) — see PropSnapshotTaskTimezone. The
	//    zone the task renders its names from is as much a part of the ownership
	//    proof as the nonce, so a task is NEVER created without it: an unreadable
	//    zone here means the driver could never prove the resulting snapshots and
	//    would wedge its own DeleteVolume behind the foreign guard. Failing the
	//    ensure (never the volume) is the fail-closed direction.
	binding := map[string]string{PropSnapshotNamingSchema: spec.namingSchema}
	// WRITE-ONCE: only stamp the zone when this dataset does not already carry a
	// locally-sourced one. Re-stamping on a CreateVolume retry that happens to
	// follow a NAS timezone change would overwrite the very evidence the delete
	// path uses to detect that change.
	if recorded := datasetLocalUserProperty(dataset, PropSnapshotTaskTimezone); recorded == "" {
		zone := d.nasCivilZone(ctx)
		if zone == nil {
			d.recordSnapshotTaskWarning(req, volumeID,
				"could not read the NAS timezone (system.general.config); no periodic-snapshot task was created, because its snapshots could not later be proven")
			return
		}
		binding[PropSnapshotTaskTimezone] = zone.String()
	}
	// An already-recorded zone is left ALONE and costs zero extra calls: the
	// delete path is where stored-vs-current is compared and where a
	// reconfiguration must fail closed.
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, binding); err != nil {
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
//
// It returns the CORROBORATING TASK SCHEMA (GF2-fix2/B1-b): the naming schema of
// a driver-minted, non-recursive task that this call observed alive on EXACTLY
// this dataset immediately before deleting it. That observation is the delete
// path's positive evidence that a task really was minting snapshots with that
// name shape here, and it must be captured HERE because the task is deleted
// before the foreign guard runs. An empty return means NO corroboration — no
// task, an unreadable task list, or a task whose schema does not prove out — and
// the foreign guard then treats every unlabeled snapshot on the dataset as
// foreign. Unprovable is never deletable.
func (d *Driver) deleteVolumeSnapshotTask(ctx context.Context, dataset *truenas.Dataset, datasetName, volumeID string) (corroboratingTaskSchema string) {
	schema := datasetLocalUserProperty(dataset, PropSnapshotNamingSchema)
	if !schemaProvesVolumeOwnership(schema, volumeID) {
		// A volume that was never scheduled carries no binding: skip entirely so
		// the default DeleteVolume path makes zero extra calls (GF2/E2
		// default-off invariant).
		if schema != "" {
			klog.Warningf("Volume dataset %s carries an unprovable %s=%q; refusing to delete any periodic-snapshot task",
				datasetName, PropSnapshotNamingSchema, schema)
		}
		return ""
	}

	tasks, err := d.truenasClient.SnapshotTaskListByDataset(ctx, datasetName)
	if err != nil {
		klog.Warningf("Failed to look up periodic-snapshot tasks for volume dataset %s during delete (continuing; the sweep will retire a stranded task): %v", datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
		return ""
	}
	task := driverOwnedTask(tasks, schema)
	// Corroboration requires the task to be scoped to EXACTLY this dataset and
	// non-recursive, which is the only shape ensureSnapshotTask ever creates.
	if task == nil || task.Dataset != datasetName || task.Recursive {
		// No live task. Honor a corroboration THIS driver durably recorded on an
		// earlier attempt of this same delete (see PropSnapshotTaskCorroboration);
		// anything else is uncorroborated and every snapshot stays foreign.
		if datasetLocalUserProperty(dataset, PropSnapshotTaskCorroboration) == schema {
			return schema
		}
		return ""
	}
	corroboratingTaskSchema = task.NamingSchema
	// DURABLE RECORD BEFORE THE EVIDENCE IS DESTROYED (GF2-fix3/B1-e).
	//
	// The task is the only live proof that something on this dataset was minting
	// snapshots under this schema, and this call is about to delete it. If the
	// record does not land, a LATER failure in this same DeleteVolume (share or
	// dataset delete) leaves the next attempt with neither a task nor a
	// corroboration: it would classify the driver's OWN snapshots as foreign and
	// return FailedPrecondition forever. Round 2 logged that write failure and
	// deleted the task anyway, which defeats the entire reason the property
	// exists.
	//
	// So: write it, VERIFY it with a source-bearing re-read (an ambiguous
	// "succeeded remotely but returned an error" is exactly the case an assumed
	// write gets wrong, in both directions), and delete the task ONLY when the
	// record is provably durable. When it is not, the task SURVIVES — which is
	// what keeps a retry decidable, because the retry will observe it alive
	// again. This attempt still proceeds on its own first-hand observation.
	if !d.recordSnapshotTaskCorroboration(ctx, dataset, datasetName, schema) {
		return corroboratingTaskSchema
	}
	if err := d.truenasClient.SnapshotTaskDelete(ctx, task.ID); err != nil {
		klog.Warningf("Failed to delete periodic-snapshot task %d for volume dataset %s (continuing; the sweep will retire it): %v", task.ID, datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
	}
	return corroboratingTaskSchema
}

// recordSnapshotTaskCorroboration durably records, and then VERIFIES, that this
// driver observed its own live task with this schema on this dataset. It reports
// whether the record is provably in place; false means the caller must NOT
// destroy the task it just observed.
func (d *Driver) recordSnapshotTaskCorroboration(ctx context.Context, dataset *truenas.Dataset, datasetName, schema string) bool {
	if datasetLocalUserProperty(dataset, PropSnapshotTaskCorroboration) == schema {
		return true
	}
	if err := d.truenasClient.DatasetSetUserProperties(ctx, datasetName, map[string]string{
		PropSnapshotTaskCorroboration: schema,
	}); err != nil {
		klog.Warningf("Failed to record the periodic-snapshot task corroboration for volume dataset %s; KEEPING the task so a retry of this delete can still observe it: %v", datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
		return false
	}
	// Verify rather than assume. DatasetGet is the source-bearing read, so this
	// also proves the value landed LOCALLY (an inherited value would prove
	// nothing on a retry, which reads it through datasetLocalUserProperty).
	verified, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		klog.Warningf("Could not verify the periodic-snapshot task corroboration for volume dataset %s; KEEPING the task so a retry of this delete can still observe it: %v", datasetName, err)
		RecordScheduledSnapshotTaskDeleteFailed()
		return false
	}
	if datasetLocalUserProperty(verified, PropSnapshotTaskCorroboration) != schema {
		klog.Warningf("The periodic-snapshot task corroboration for volume dataset %s did not read back as a local %s=%q; KEEPING the task so a retry of this delete can still observe it",
			datasetName, PropSnapshotTaskCorroboration, schema)
		RecordScheduledSnapshotTaskDeleteFailed()
		return false
	}
	return true
}

// scheduledProvenanceOptions selects how strict the provenance predicate is for
// a particular call site.
type scheduledProvenanceOptions struct {
	// requireLocalSource demands source=="local" on the dataset's ownership and
	// schema properties. Delete-authorizing callers set it; the metrics-only
	// reconcile caller cannot, because TrueNAS 26.0's zfs.resource.query
	// projection strips per-property source entirely.
	requireLocalSource bool
	// requireTaskCorroboration demands that corroboratingTaskSchema equals the
	// dataset's stamped schema — i.e. that a driver-minted periodic-snapshot task
	// with exactly this schema was observed ALIVE on exactly this dataset. Only
	// the delete path sets it; it is what makes the predicate FAIL CLOSED when
	// the owning task is absent, unreadable, or mismatched.
	requireTaskCorroboration bool
	// corroboratingTaskSchema is that observation (empty = none obtained).
	corroboratingTaskSchema string
	// nasZone is the NAS's civil timezone, in which a periodic-snapshot task
	// renders its %Y%m%d-%H%M%S name. NIL means the driver could not read it, and
	// every candidate then fails closed — exactly like a missing task.
	nasZone *time.Location
}

// driverScheduledSnapshotProvenance is the SINGLE ownership predicate for
// task-created snapshots (GF2-fix/B1, tightened in GF2-fix2/B1).
//
// Task-created snapshots carry NO CSI user properties (P2) and TrueNAS 26.0
// cannot add properties to an existing snapshot, so per-snapshot provenance must
// be assembled from durable state the driver controls. A snapshot is treated as
// driver-created only when ALL of:
//
//  1. it is not a CSI snapshot or tombstone (those have their own provenance);
//  2. it lives on EXACTLY the volume's own dataset;
//  3. that dataset carries THIS driver instance's ownership stamp;
//  4. that dataset carries a naming-schema binding that the driver's own
//     production algorithm reproduces byte-for-byte for THIS volume id (so the
//     nonce is one the driver minted, not one an outsider chose);
//  5. a driver-minted, non-recursive periodic-snapshot task carrying EXACTLY
//     that schema was observed alive on exactly this dataset (delete path only);
//  6. the snapshot's short name is exactly a rendering of that schema — same
//     volume id in canonical form, same nonce, a REAL calendar/clock instant;
//  7. that encoded instant is EXACTLY (±2s clock skew) when the snapshot was
//     created, comparing against the snapshot's own creation property rendered
//     in the NAS's own civil timezone, which the driver reads from the backend.
//
// If any link is missing the answer is NO and the snapshot stays FOREIGN, which
// means the default policy refuses to destroy it. Unprovable never means
// deletable.
//
// # DOCUMENTED TRUST BOUNDARY — read this before strengthening any claim
//
// This predicate does NOT establish "a snapshot this driver did not create
// cannot be deleted". It establishes: "a snapshot that does not sit on this
// volume's own locally-stamped dataset, or whose name does not reproduce the
// driver-minted per-volume nonce, or that is not corroborated by a live
// driver-minted task on that dataset, or whose name does not encode a real
// instant agreeing with its creation time, is never deleted as driver-owned."
//
// The residue is real and is accepted deliberately: an actor with pool-write
// access on the NAS can READ the naming schema (it is stored on the dataset and
// on the task, and pool.snapshottask.query exposes it) and can create a snapshot
// on the volume dataset with that exact name at the matching second. Such a
// snapshot is INDISTINGUISHABLE from a task-created one, because TrueNAS 26.0
// offers no way to stamp a user property on an existing snapshot and no way to
// attribute a snapshot to the task that made it. Closing this would require
// per-snapshot provenance the platform does not provide. Storage-administrator
// access to the CSI parent dataset is therefore a TRUSTED boundary for this
// feature. Do not let a doc, comment or test name claim otherwise.
//
// Reading the NAS timezone (GF2-fix2 round 2) does NOT change that boundary. It
// removes the accidental-collision slack in link 7 only — see
// scheduledSnapshotCreationAgrees for the re-derived numbers.
func driverScheduledSnapshotProvenance(
	snap *truenas.Snapshot,
	dataset *truenas.Dataset,
	driverInstanceID string,
	opts scheduledProvenanceOptions,
) bool {
	if snap == nil || dataset == nil || isCSISnapshot(snap) || isSnapshotTombstone(snap) {
		return false
	}
	if snap.Dataset != dataset.Name {
		return false
	}
	if opts.requireLocalSource {
		if !datasetHasLocalUserProperty(dataset, PropDriverInstanceID, driverInstanceID) {
			return false
		}
	} else if datasetUserProperty(dataset, PropDriverInstanceID) != driverInstanceID || driverInstanceID == "" {
		return false
	}

	schema := ""
	if opts.requireLocalSource {
		schema = datasetLocalUserProperty(dataset, PropSnapshotNamingSchema)
	} else {
		schema = datasetUserProperty(dataset, PropSnapshotNamingSchema)
	}
	// The volume id is the dataset's own base name; the schema must prove out
	// against it, which also rejects a schema inherited from another volume.
	if !schemaProvesVolumeOwnership(schema, datasetVolumeID(dataset.Name)) {
		return false
	}
	// FAIL CLOSED without a corroborating live task: no task means nothing on
	// this dataset was minting snapshots under this schema, so an unlabeled
	// snapshot bearing it has no claim to driver authorship at all.
	if opts.requireTaskCorroboration && (opts.corroboratingTaskSchema == "" || opts.corroboratingTaskSchema != schema) {
		return false
	}
	encoded, ok := parseScheduledSnapshotName(snapshotShortName(snap), schema)
	if !ok {
		return false
	}
	return scheduledSnapshotCreationAgrees(encoded, snap.GetCreationTime(), opts.nasZone)
}

// isDriverScheduledSnapshot is the delete-authorizing form of the provenance
// predicate: strict property sources, a live corroborating task, and the NAS's
// own civil timezone all required.
func isDriverScheduledSnapshot(snap *truenas.Snapshot, dataset *truenas.Dataset, driverInstanceID, corroboratingTaskSchema string, nasZone *time.Location) bool {
	return driverScheduledSnapshotProvenance(snap, dataset, driverInstanceID, scheduledProvenanceOptions{
		requireLocalSource:       true,
		requireTaskCorroboration: true,
		corroboratingTaskSchema:  corroboratingTaskSchema,
		nasZone:                  nasZone,
	})
}

// datasetVolumeID derives a volume id from a dataset path.
func datasetVolumeID(datasetName string) string {
	if idx := strings.LastIndex(datasetName, "/"); idx >= 0 {
		return datasetName[idx+1:]
	}
	return datasetName
}

// foreignSnapshotsOnly filters out driver-scheduled snapshots the ownership
// chain proves out (GF2/E2, R4) from a volume's snapshot list, returning every
// snapshot the foreign-snapshot guard must still police. Anything the chain
// cannot prove stays foreign and is therefore preserved by the default policy.
//
// corroboratingTaskSchema comes from deleteVolumeSnapshotTask, which ran just
// before this call and is the only place the owning task can still be observed.
// Passing "" (no task seen) makes every snapshot foreign — the safe direction.
// nasZone is likewise nil when the NAS timezone could not be read, and that too
// makes every snapshot foreign.
func (d *Driver) foreignSnapshotsOnly(snapshots []*truenas.Snapshot, dataset *truenas.Dataset, corroboratingTaskSchema string, nasZone *time.Location) []*truenas.Snapshot {
	foreign := make([]*truenas.Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if isDriverScheduledSnapshot(snap, dataset, d.driverInstanceID(), corroboratingTaskSchema, nasZone) {
			continue
		}
		foreign = append(foreign, snap)
	}
	return foreign
}

// nasCivilZone returns the NAS's CURRENT civil timezone — the clock a
// periodic-snapshot task renders its %Y%m%d-%H%M%S names from (GF2-fix2/B1-a).
//
// NO DRIVER-LEVEL CACHE (GF2-fix3/B1-a). Round 2 memoized this on the Driver
// behind a one-hour TTL that nothing could invalidate: a reconnect dropped only
// the truenas.Client's copy, so a zone reconfiguration — or a lookup that would
// now FAIL — was bypassed for the rest of the TTL while the stale zone kept
// authorizing deletes. Every call now goes to truenas.Client.SystemTimezone,
// whose cache IS dropped on reconnect, never caches an error, and is bounded by
// the deliberately short systemTimezoneTTL.
//
// CALL BUDGET. This is resolved ONLY for a volume that actually carries a
// periodic-snapshot binding, so an unscheduled deployment — the default — never
// calls it at all. A scheduled volume pays at most one system.general.config
// round trip per client-cache expiry, on a path that already makes ~9 calls.
//
// FAIL CLOSED. A nil return (with the error logged) means every candidate
// scheduled snapshot is treated as FOREIGN and preserved — the same direction as
// a missing corroborating task. Guessing UTC here would silently misclassify
// every snapshot on a non-UTC NAS, in the deleting direction.
func (d *Driver) nasCivilZone(ctx context.Context) *time.Location {
	zone, err := d.truenasClient.SystemTimezone(ctx)
	if err != nil || zone == nil {
		klog.Warningf("Could not read the NAS timezone (system.general.config); driver-scheduled snapshots cannot be proven and will be preserved as foreign: %v", err)
		RecordNASTimezoneUnresolved()
		return nil
	}
	return zone
}

// scheduledSnapshotZone resolves the zone a volume's scheduled snapshot names
// must be proven in, and is the ONLY zone source the delete path may use
// (GF2-fix3/B1-d).
//
// WHY A STORED ZONE. Reading only the CURRENT zone cannot detect every timezone
// reconfiguration: America/New_York -> America/Toronto, or a switch to a fixed
// -05:00 for a winter-created snapshot, leaves the civil fields identical, so a
// name minted under the old configuration still "agrees" and stays deletable.
// ensureSnapshotTask therefore records the IANA zone that was in force when the
// TASK was created, durably, on the volume's own dataset, WRITE-ONCE. Comparing
// stored-vs-current makes the FACT of a reconfiguration detectable whether or
// not the offsets happen to coincide.
//
// FAIL CLOSED on: no locally-sourced stored zone (a clone/received/detached copy
// inherits the property non-locally and is rejected by datasetLocalUserProperty,
// exactly like the corroboration and ownership stamps); an unreadable current
// zone; or any difference between the two. A nil return makes every candidate
// snapshot FOREIGN, i.e. preserved.
func (d *Driver) scheduledSnapshotZone(ctx context.Context, dataset *truenas.Dataset, datasetName string) *time.Location {
	stored := datasetLocalUserProperty(dataset, PropSnapshotTaskTimezone)
	if stored == "" {
		klog.Warningf("Volume dataset %s carries no locally-recorded %s; its scheduled snapshots cannot be proven and will be preserved as foreign",
			datasetName, PropSnapshotTaskTimezone)
		RecordNASTimezoneUnresolved()
		return nil
	}
	current := d.nasCivilZone(ctx)
	if current == nil {
		return nil
	}
	if current.String() != stored {
		klog.Warningf("Volume dataset %s recorded NAS timezone %q when its periodic-snapshot task was created but the NAS now reports %q; the snapshot names cannot be proven under a reconfigured clock and will be preserved as foreign",
			datasetName, stored, current.String())
		RecordNASTimezoneUnresolved()
		return nil
	}
	return current
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

	// NOTE (GF2-fix3/B1-a): round 2 warmed the driver's NAS-timezone cache here.
	// There is no driver cache any more, and the client's is deliberately far
	// shorter than the reconcile interval, so a "warm" call from this pass could
	// never actually serve a later CSI RPC — it was an API call that bought
	// nothing and a comment that overstated what the hot path pays. DeleteVolume
	// resolves the zone itself, for scheduled volumes only.

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
