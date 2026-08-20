package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	// defaultReapRecordPollInterval is the conservative controller poll of the
	// durable last-reap record. 5m is 288 DatasetGetUserProperties calls/day
	// (576 if the child is absent and the parent is consulted) per replica,
	// versus 2,880/5,760 at a 30s tick.
	defaultReapRecordPollInterval = 5 * time.Minute

	// reapRecordCallTimeout bounds each property-only read so a hung backend
	// cannot stall the poller. Kept well below the default interval so an
	// outage cannot turn the tick into back-to-back circuit-breaker probes.
	reapRecordCallTimeout = 30 * time.Second

	// reapRecordFutureSkew is clock-skew tolerance for completion/oldest
	// timestamps on read. A record further in the future is invalid: it would
	// suppress staleness alerts and force overlays of current inventory.
	reapRecordFutureSkew = 5 * time.Minute
)

// tombstoneReapRecord is the singleton durable outcome of one completed
// delete-capable reconcile pass. It lives on the .csi-bookkeeping child
// dataset (created lazily on first write) under PropTombstoneReapLast so the
// payload cannot inherit onto snapshots. Writes are last-writer-wins, tolerant
// of a missing record, and never fail the pass that produced them.
type tombstoneReapRecord struct {
	Version int `json:"v"`
	// DriverInstanceID binds the record to the CSI instance that wrote it.
	// Required on v2. The sourceless TrueNAS 26.0 read (zfs.resource.query)
	// cannot report per-property Source, so this exact-match is the
	// anti-inheritance guard: a record inherited from an ancestor owned by
	// a different instance must not export as ours.
	DriverInstanceID string `json:"driver_instance_id"`
	CompletedAt      int64  `json:"completed_at"`
	// CompletedAtNano is the same instant as CompletedAt at nanosecond
	// resolution so a record written in the same unix second as a detection
	// observation still overlays (unix-second equality would leave the
	// pre-reap backlog latched until the next interval).
	CompletedAtNano   int64 `json:"completed_at_nano,omitempty"`
	Reaped            int   `json:"reaped"`
	SkippedOnCap      int   `json:"skipped_on_cap"`
	SkippedRefused    int   `json:"skipped_refused"`
	RemainingEligible int   `json:"remaining_eligible"`
	RemainingBytes    int64 `json:"remaining_bytes"`
	// OldestCreatedAt is the creation unix timestamp of the oldest remaining
	// driver-owned tombstone (including age-gated). 0 means none remain, which
	// publishes oldest-age as 0 rather than now-0 (a 1970-01-01 tombstone
	// would look like "age unknown"/never-ran). Tombstones whose creation
	// time is unavailable are counted by scale_csi_tombstone_unknown_age, not
	// represented here.
	OldestCreatedAt int64 `json:"oldest_created_at,omitempty"`
}

type reapRecordStatus int

const (
	reapRecordAbsent reapRecordStatus = iota
	reapRecordInvalid
	reapRecordOK
	// reapRecordLegacy is a schema-valid v1 record seen on the sourceless
	// production read. It is not authentic until a source-bearing
	// pool.dataset.query confirms Source=="local"; the next delete-capable
	// pass rewrites it as v2.
	reapRecordLegacy
)

func (r *ReconcileReport) tombstoneOldestAge() time.Duration {
	var max time.Duration
	for i := range r.TombstonePending {
		if r.TombstonePending[i].Age > max {
			max = r.TombstonePending[i].Age
		}
	}
	return max
}

func (r *ReconcileReport) tombstoneOldestCreatedAt() time.Time {
	var oldest time.Time
	for i := range r.TombstonePending {
		created := r.TombstonePending[i].CreatedAt
		if created.IsZero() {
			continue
		}
		if oldest.IsZero() || created.Before(oldest) {
			oldest = created
		}
	}
	return oldest
}

func (r *ReconcileReport) reapRecordAt(at time.Time) tombstoneReapRecord {
	deleted := make(map[string]struct{}, len(r.DeletedTombstones))
	for _, id := range r.DeletedTombstones {
		deleted[id] = struct{}{}
	}
	remainingEligible := 0
	var remainingBytes int64
	for i := range r.TombstoneSnapshots {
		item := &r.TombstoneSnapshots[i]
		if _, gone := deleted[item.ID]; gone {
			continue
		}
		remainingEligible++
		remainingBytes += item.Bytes
	}
	var oldestCreated time.Time
	for i := range r.TombstonePending {
		item := &r.TombstonePending[i]
		if _, gone := deleted[item.ID]; gone {
			continue
		}
		if item.CreatedAt.IsZero() {
			continue
		}
		if oldestCreated.IsZero() || item.CreatedAt.Before(oldestCreated) {
			oldestCreated = item.CreatedAt
		}
	}
	capSkips := r.CapSkippedDeletes()
	rec := tombstoneReapRecord{
		Version:           tombstoneReapRecordVersion,
		CompletedAt:       at.Unix(),
		CompletedAtNano:   at.UnixNano(),
		Reaped:            len(r.DeletedTombstones),
		SkippedOnCap:      capSkips,
		SkippedRefused:    len(r.SkippedDeletes) - capSkips,
		RemainingEligible: remainingEligible,
		RemainingBytes:    remainingBytes,
	}
	if !oldestCreated.IsZero() {
		rec.OldestCreatedAt = oldestCreated.Unix()
	}
	return rec
}

func reapRecordInstant(rec *tombstoneReapRecord) int64 {
	if rec == nil {
		return 0
	}
	if rec.CompletedAtNano > 0 {
		return rec.CompletedAtNano
	}
	return rec.CompletedAt * int64(time.Second)
}

func (rec *tombstoneReapRecord) validate(now time.Time) error {
	if rec == nil {
		return fmt.Errorf("nil record")
	}
	if rec.Version != tombstoneReapRecordVersion && rec.Version != tombstoneReapRecordVersionV1 {
		return fmt.Errorf("unsupported version %d", rec.Version)
	}
	if rec.Version == tombstoneReapRecordVersion && strings.TrimSpace(rec.DriverInstanceID) == "" {
		return fmt.Errorf("driver_instance_id is required")
	}
	if rec.CompletedAt <= 0 {
		return fmt.Errorf("completed_at must be positive")
	}
	if rec.Reaped < 0 || rec.SkippedOnCap < 0 || rec.SkippedRefused < 0 ||
		rec.RemainingEligible < 0 || rec.RemainingBytes < 0 {
		return fmt.Errorf("negative counts or bytes")
	}
	if rec.CompletedAtNano < 0 {
		return fmt.Errorf("completed_at_nano must not be negative")
	}
	if rec.CompletedAtNano != 0 && rec.CompletedAtNano/int64(time.Second) != rec.CompletedAt {
		return fmt.Errorf("completed_at_nano is inconsistent with completed_at")
	}
	nowUnix := now.Unix()
	skew := int64(reapRecordFutureSkew.Seconds())
	if rec.CompletedAt > nowUnix+skew {
		return fmt.Errorf("completed_at is in the future")
	}
	if rec.OldestCreatedAt < 0 {
		return fmt.Errorf("oldest_created_at must not be negative")
	}
	if rec.OldestCreatedAt > 0 {
		if rec.OldestCreatedAt > nowUnix+skew {
			return fmt.Errorf("oldest_created_at is in the future")
		}
		if rec.OldestCreatedAt > rec.CompletedAt {
			return fmt.Errorf("oldest_created_at is after completed_at")
		}
	}
	return nil
}

func inspectTombstoneReapRecord(ds *truenas.Dataset, instanceID string) (*tombstoneReapRecord, reapRecordStatus) {
	if ds == nil {
		return nil, reapRecordAbsent
	}
	property, ok := ds.UserProperties[PropTombstoneReapLast]
	if !ok || !reapRecordPropertyAuthentic(property.Source) {
		return nil, reapRecordAbsent
	}
	var rec tombstoneReapRecord
	if err := json.Unmarshal([]byte(property.Value), &rec); err != nil {
		klog.Warningf("Ignoring unparseable tombstone reap record on %s: %v", ds.Name, err)
		return nil, reapRecordInvalid
	}
	if err := rec.validate(time.Now()); err != nil {
		klog.Warningf("Ignoring invalid tombstone reap record on %s: %v", ds.Name, err)
		return nil, reapRecordInvalid
	}
	sourceless := strings.TrimSpace(property.Source) == ""
	switch rec.Version {
	case tombstoneReapRecordVersion:
		// NEVER accept a missing instance ID from the sourceless path —
		// Source was the anti-inheritance guard and v2 replaces it with
		// an exact driver_instance_id match. A foreign or empty ID is
		// unauthentic (absent), not garbage: it must not block a valid
		// record at the other dual-read location.
		if rec.DriverInstanceID == "" || instanceID == "" || rec.DriverInstanceID != instanceID {
			klog.V(4).Infof("Ignoring tombstone reap record on %s: driver_instance_id %q does not match this instance", ds.Name, rec.DriverInstanceID)
			return nil, reapRecordAbsent
		}
		return &rec, reapRecordOK
	case tombstoneReapRecordVersionV1:
		if sourceless {
			return &rec, reapRecordLegacy
		}
		return &rec, reapRecordOK
	default:
		return nil, reapRecordInvalid
	}
}

// reapRecordPropertyAuthentic is the anti-inheritance guard for
// PropTombstoneReapLast. When Source is populated (pool.dataset.query), it
// requires source=="local" — the same exact-match used for ownership stamps
// and publication records. Clone-inherited user properties report the origin
// snapshot name, not "inherited", so a substring filter would let a clone
// parse someone else's record as its own.
//
// When Source is empty the field is unavailable: TrueNAS 26.0
// zfs.resource.query (DatasetGetUserProperties, the production poller path)
// returns user_properties as a flat map with no per-property source. The
// mock strips Source to "" for the same reason. v2 authenticity then rests
// on an exact driver_instance_id match — Source cannot be the
// anti-inheritance guard on this path, and a record inherited from an
// ancestor of a different instance would otherwise export as ours.
//
// v1 records have no instance binding. They are never accepted from the
// sourceless path; a one-time source-bearing DatasetGet accepts them only
// when Source=="local", and the next delete-capable pass rewrites v2.
func reapRecordPropertyAuthentic(source string) bool {
	if strings.TrimSpace(source) == "" {
		return true
	}
	return isLocalUserPropertySource(source)
}

func parseTombstoneReapRecord(ds *truenas.Dataset, instanceID string) *tombstoneReapRecord {
	rec, status := inspectTombstoneReapRecord(ds, instanceID)
	if status != reapRecordOK {
		return nil
	}
	return rec
}

func newerReapRecord(a, b *tombstoneReapRecord) *tombstoneReapRecord {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if reapRecordInstant(b) > reapRecordInstant(a) {
		return b
	}
	return a
}

func tombstoneReapRecordFromDatasets(parent, child *truenas.Dataset, instanceID string) *tombstoneReapRecord {
	childRec, childStatus := inspectTombstoneReapRecord(child, instanceID)
	if childStatus == reapRecordInvalid {
		// A present-but-invalid child must not be overwritten by a stale
		// parent record: the child is the write target, so garbage there
		// means the canonical location is unusable.
		return nil
	}
	if childStatus != reapRecordOK {
		childRec = nil
	}
	parentRec, parentStatus := inspectTombstoneReapRecord(parent, instanceID)
	if parentStatus != reapRecordOK {
		parentRec = nil
	}
	// newerReapRecord(child, parent): parent wins only when strictly
	// newer; the child wins ties because it is the write target.
	return newerReapRecord(childRec, parentRec)
}

// writeTombstoneReapRecord persists rec on the dedicated bookkeeping child
// dataset (created lazily). The child is used even when reconcile.bookkeeping
// relocation is off: this singleton must not inherit onto every snapshot the
// way parent-dataset user properties do. A write failure is returned to the
// caller, which must treat it as non-fatal to the reap.
func (d *Driver) bindTombstoneReapRecord(rec *tombstoneReapRecord) {
	if rec == nil {
		return
	}
	rec.Version = tombstoneReapRecordVersion
	rec.DriverInstanceID = d.driverInstanceID()
}

func (d *Driver) writeTombstoneReapRecord(ctx context.Context, rec tombstoneReapRecord) error {
	d.bindTombstoneReapRecord(&rec)
	encoded, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("encode tombstone reap record: %w", err)
	}
	if err := d.ensureBookkeepingDataset(ctx); err != nil {
		return fmt.Errorf("resolve bookkeeping dataset for tombstone reap record: %w", err)
	}
	target := d.bookkeepingDatasetName()
	if _, err := d.setAndVerifyDatasetUserProperties(ctx, target, map[string]string{
		PropTombstoneReapLast: string(encoded),
	}); err != nil {
		d.noteBookkeepingWriteFailure(target, err)
		return fmt.Errorf("record tombstone reap outcome on %s: %w", target, err)
	}
	return nil
}

// getDatasetUserProperties is a property-only read for the reap-record poller
// and dual-location record load. The real TrueNAS client uses a path-scoped
// zfs.resource.query (get_children=false) or a pool.dataset.query with an
// empty native-property projection. *MockClient uses the matching sourceless
// resource-query shape; other wrappers keep using DatasetGet so injected
// DatasetGet faults still apply.
func (d *Driver) getDatasetUserProperties(ctx context.Context, name string) (*truenas.Dataset, error) {
	switch c := d.truenasClient.(type) {
	case *truenas.Client:
		return c.DatasetGetUserProperties(ctx, name)
	case *truenas.MockClient:
		return c.DatasetGetUserProperties(ctx, name)
	default:
		// Wrappers that inject DatasetGet faults still apply.
		return d.truenasClient.DatasetGet(ctx, name)
	}
}

// inspectTombstoneReapRecordMaybeLegacy is the v1 compatibility slow path.
// Production reads stay on sourceless DatasetGetUserProperties. Only a
// schema-valid v1 payload (no instance id) pays one named DatasetGet
// (pool.dataset.query) so Source=="local" can be proven; that is not a
// switch of the poller onto full-system user-property materialization.
func (d *Driver) inspectTombstoneReapRecordMaybeLegacy(ctx context.Context, ds *truenas.Dataset, name string) (*tombstoneReapRecord, reapRecordStatus, error) {
	rec, status := inspectTombstoneReapRecord(ds, d.driverInstanceID())
	if status != reapRecordLegacy {
		return rec, status, nil
	}
	sourced, err := d.truenasClient.DatasetGet(ctx, name)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			return nil, reapRecordAbsent, nil
		}
		return nil, reapRecordAbsent, err
	}
	rec, status = inspectTombstoneReapRecord(sourced, d.driverInstanceID())
	if status == reapRecordLegacy {
		// DatasetGet was still sourceless: never accept v1 without a
		// local source.
		return nil, reapRecordAbsent, nil
	}
	return rec, status, nil
}

func (d *Driver) readTombstoneReapRecord(ctx context.Context) (*tombstoneReapRecord, error) {
	childName := d.bookkeepingDatasetName()
	child, err := d.getDatasetUserProperties(ctx, childName)
	if err != nil && !truenas.IsNotFoundError(err) {
		return nil, err
	}
	childRec, childStatus, err := d.inspectTombstoneReapRecordMaybeLegacy(ctx, child, childName)
	if err != nil {
		return nil, err
	}
	if childStatus == reapRecordInvalid {
		return nil, nil
	}
	if childStatus != reapRecordOK {
		childRec = nil
	}
	parentName := d.parentDatasetName()
	parent, err := d.getDatasetUserProperties(ctx, parentName)
	if err != nil && !truenas.IsNotFoundError(err) {
		return nil, err
	}
	parentRec, parentStatus, err := d.inspectTombstoneReapRecordMaybeLegacy(ctx, parent, parentName)
	if err != nil {
		return nil, err
	}
	if parentStatus != reapRecordOK {
		parentRec = nil
	}
	return newerReapRecord(childRec, parentRec), nil
}

func (d *Driver) noteOrphanMetricsObservation(at time.Time) {
	if at.IsZero() {
		return
	}
	nano := at.UnixNano()
	for {
		last := d.orphanMetricsObservedAt.Load()
		if nano <= last {
			return
		}
		if d.orphanMetricsObservedAt.CompareAndSwap(last, nano) {
			return
		}
	}
}

func reapRecordShouldOverlay(rec *tombstoneReapRecord, observedNano int64) bool {
	if rec == nil {
		return false
	}
	if rec.CompletedAtNano > observedNano {
		return true
	}
	if rec.CompletedAtNano == 0 && rec.CompletedAt > observedNano/int64(time.Second) {
		// Records written before CompletedAtNano existed: fall back to
		// whole-second comparison.
		return true
	}
	return false
}

// retainReapRecordLocked keeps the newest record PAYLOAD, not just its
// timestamp. Caller holds reapRecordPublishMu.
func (d *Driver) retainReapRecordLocked(rec *tombstoneReapRecord) bool {
	if rec == nil {
		return false
	}
	instant := reapRecordInstant(rec)
	if d.latestReapRecord != nil && instant < reapRecordInstant(d.latestReapRecord) {
		return false
	}
	cp := *rec
	d.latestReapRecord = &cp
	d.reapRecordPublishedAt.Store(instant)
	return true
}

func (d *Driver) publishLastTombstoneReapRecord(rec *tombstoneReapRecord, forceOverlay bool) {
	if rec == nil {
		return
	}
	d.reapRecordPublishMu.Lock()
	defer d.reapRecordPublishMu.Unlock()
	d.publishLastTombstoneReapRecordLocked(rec, forceOverlay)
}

func (d *Driver) publishLastTombstoneReapRecordLocked(rec *tombstoneReapRecord, forceOverlay bool) {
	if !d.retainReapRecordLocked(rec) {
		// A reconcile holding an older record must not overwrite a
		// newer poll result (or a newer pass).
		return
	}
	overlay := forceOverlay || reapRecordShouldOverlay(d.latestReapRecord, d.orphanMetricsObservedAt.Load())
	publishLastTombstoneReapRecord(d.latestReapRecord, time.Now(), overlay)
}

// publishOrphanAndReapMetrics publishes the detection inventory and selects
// the latest reap-record overlay under one driver-scoped lock. A poll that
// already published a newer payload is re-applied after the detection
// gauges so a finishing pass cannot restore an older backlog.
func (d *Driver) publishOrphanAndReapMetrics(report ReconcileReport, rec *tombstoneReapRecord, forceOverlay bool) {
	d.reapRecordPublishMu.Lock()
	defer d.reapRecordPublishMu.Unlock()
	d.retainReapRecordLocked(rec)
	SetOrphanReconcileMetrics(report)
	if d.latestReapRecord == nil {
		return
	}
	overlay := forceOverlay || reapRecordShouldOverlay(d.latestReapRecord, d.orphanMetricsObservedAt.Load())
	publishLastTombstoneReapRecord(d.latestReapRecord, time.Now(), overlay)
}

func (d *Driver) refreshTombstoneReapMetrics(ctx context.Context) {
	if d.truenasClient == nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, reapRecordCallTimeout)
	defer cancel()
	rec, err := d.readTombstoneReapRecord(callCtx)
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Tombstone reap-record refresh failed: %v", err)
		}
		return
	}
	d.publishLastTombstoneReapRecord(rec, false)
}

func (d *Driver) configuredReapRecordPollInterval() time.Duration {
	if d.config == nil {
		return defaultReapRecordPollInterval
	}
	raw := strings.TrimSpace(d.config.Reconcile.Delete.ReapRecordPollInterval)
	if raw == "" {
		return defaultReapRecordPollInterval
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval <= 0 {
		klog.Errorf("Tombstone reap-record poll disabled due to invalid interval %q: %v", raw, err)
		return 0
	}
	return interval
}

func (d *Driver) startTombstoneReapRecordPoll() {
	d.reapRecordStateMu.Lock()
	defer d.reapRecordStateMu.Unlock()
	if d.reapRecordStopped || d.reapRecordCancel != nil {
		return
	}
	if d.config == nil || !d.config.Reconcile.Delete.Enabled {
		return
	}
	interval := d.configuredReapRecordPollInterval()
	if interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.reapRecordCancel = cancel
	d.reapRecordWg.Add(1)
	go func() {
		defer d.reapRecordWg.Done()
		klog.Infof("Tombstone reap-record poll started: interval=%v", interval)
		d.refreshTombstoneReapMetrics(ctx)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.refreshTombstoneReapMetrics(ctx)
			case <-ctx.Done():
				klog.Info("Tombstone reap-record poll stopped")
				return
			}
		}
	}()
}

func (d *Driver) stopTombstoneReapRecordPoll() {
	d.reapRecordStateMu.Lock()
	d.reapRecordStopped = true
	cancel := d.reapRecordCancel
	d.reapRecordCancel = nil
	d.reapRecordStateMu.Unlock()
	if cancel != nil {
		cancel()
	}
	d.reapRecordWg.Wait()
}
