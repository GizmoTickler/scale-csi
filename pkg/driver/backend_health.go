package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// backendHealthCallTimeout bounds each per-tick backend read so a hung call can
// never stall the poller for longer than a fixed window, independent of the
// configured interval. Mirrors the capacity gauge loop's guard.
const backendHealthCallTimeout = 30 * time.Second

// minBackendHealthInterval clamps the poll cadence. pool.query is cheap but not
// free, and health is not a sub-30s signal.
const minBackendHealthInterval = 30 * time.Second

// maxBackendHealthInterval is the CEILING, and it exists to bound how far the
// debounced VolumeCondition may trail the raw gauges rather than to save API
// calls.
//
// The fan-out hysteresis (backendHealthFlipSamples) needs two consecutive
// samples to flip a PVC's VolumeCondition, and NOTHING is published until each
// of those samples' backend calls RETURNS — backendHealthCallTimeout bounds that
// at 30s. So a degradation that keeps being observed reaches conditions within
// at most 2 × interval + backendHealthCallTimeout: 4m30s at the ceiling, NOT the
// 4m that 2 × interval alone suggests. The ScaleCSIPoolDegraded alert fires off
// the UNDAMPED gauge after a 5m hold, so the ceiling still keeps a confirmed
// flip inside that hold (4m30s < 5m) WHEN SAMPLES KEEP ARRIVING.
//
// This is a DRIVER-SIDE bound. It says nothing about when Prometheus SCRAPES the
// change or when the rule is EVALUATED, and the chart puts no upper bound on the
// ServiceMonitor interval — that is the observer-lag class, and it is outside
// every number here.
//
// This is a bound, NOT a guarantee that the two signals always agree, and it
// does not reduce the number of ways they can differ — it CREATES one of them:
// ordering the confirmed condition ahead of the 5m hold means the PVC already
// reads Abnormal while ScaleCSIPoolDegraded is still PENDING (the "alert hold"
// class).
//
// backendHealthFlipSamples carries the SINGLE canonical enumeration: SIX
// NUMBERED classes of divergence — confirmation lag, alert hold, recovery, poll
// stall, observer lag and cold start. The first three have an upper bound; the
// last three do not. Every other copy of that list (prometheusrule.yaml,
// values.yaml, values.schema.json, docs/production.md, docs/deployment.md)
// numbers the SAME classes in the SAME order. A count asserted in one place is a
// promise in all of them.
//
// A larger configured value is clamped with a loud warning rather than rejected:
// failing the controller over an observability cadence would be a worse outcome
// than honoring the ceiling.
const maxBackendHealthInterval = 2 * time.Minute

// configuredBackendHealthInterval parses the cadence exactly as configured, with
// NO clamping, so the poller can report a clamp against the operator's own
// value. An empty/absent setting is the 60s default.
func (d *Driver) configuredBackendHealthInterval() (time.Duration, error) {
	if d.config == nil {
		return 2 * minBackendHealthInterval, nil
	}
	raw := strings.TrimSpace(d.config.BackendHealth.Interval)
	if raw == "" {
		return 2 * minBackendHealthInterval, nil
	}
	interval, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	return interval, nil
}

// clampBackendHealthInterval applies [minBackendHealthInterval,
// maxBackendHealthInterval].
func clampBackendHealthInterval(interval time.Duration) time.Duration {
	switch {
	case interval < minBackendHealthInterval:
		return minBackendHealthInterval
	case interval > maxBackendHealthInterval:
		return maxBackendHealthInterval
	}
	return interval
}

// resolveBackendHealthInterval resolves the configured cadence and clamps it to
// [minBackendHealthInterval, maxBackendHealthInterval]. It is the ONE place the
// effective interval is derived, so the poll loop and the staleness TTL can
// never disagree about it.
//
// It is deliberately SILENT. backendHealthTTL calls it, and the TTL is
// recomputed for every composed VolumeCondition — ListVolumes composes one per
// volume — so warning from here would turn a supported configuration into a
// per-volume log storm. The clamp is reported ONCE, from startBackendHealth.
func (d *Driver) resolveBackendHealthInterval() (time.Duration, error) {
	interval, err := d.configuredBackendHealthInterval()
	if err != nil {
		return 0, err
	}
	return clampBackendHealthInterval(interval), nil
}

// backendHealthStaleIntervals is how many consecutive missed samples make the
// cached snapshot untrustworthy. Keeping the last snapshot across a blip is
// correct; keeping it across an outage is not — a stale DEGRADED keeps
// ScaleCSIPoolDegraded firing after the pool has recovered, and a stale ONLINE
// masks a real degradation. Past the TTL the driver falls back to the
// dataset-only condition (exactly the pre-GF5 semantics) and raises
// scale_csi_pool_health_stale.
const backendHealthStaleIntervals = 3

// backendHealthFlipSamples is the hysteresis depth for the condition fan-out: a
// pool-health transition must be confirmed by this many CONSECUTIVE samples
// before it flips every managed PVC's VolumeCondition.
//
// The fan-out is fleet-wide by construction — one pool backs every volume — so
// an unfiltered DEGRADED<->ONLINE flap would rewrite N conditions and churn N
// PVC events on every tick. Metrics are deliberately NOT damped: SetPoolHealthMetrics
// always publishes the raw sample, so a flap stays fully visible to Prometheus
// while the per-PVC condition stays stable.
//
// The FIRST observation is never damped: with no previous snapshot there is
// nothing to flap against, and delaying the initial signal would only blind the
// first interval after startup.
//
// THE HONEST CONTRACT (do not restate this as "the alert and the PVC condition
// can never disagree" — that claim is false). FOUR observers are involved, not
// two: the RAW gauges, the DEBOUNCED per-PVC condition, the ALERT (the raw gauge
// plus its own `for` hold, which Prometheus evaluates on ITS OWN scrape and
// rule-evaluation cadence, not on the driver's), and the PVC condition/Event
// refresh that external-health-monitor drives on a third cadence. They share one
// SEVERITY SPLIT — the same states are abnormal in all of them — but they are
// not the same signal in TIME. This is the CANONICAL list and it is complete:
// SIX NUMBERED classes of divergence; the first three have an upper bound, the
// last three do not. Do not restate it with a smaller count, and do not rename
// or reorder the items — every other copy is checked against this one.
//
//  1. Confirmation lag (BOUNDED: one successful poll interval plus one
//     backendHealthCallTimeout, so ≤ 2m30s at the interval ceiling). An
//     established-state transition is withheld until the second consecutive
//     sample, so the condition trails the gauges. maxBackendHealthInterval keeps
//     2 × interval + one call timeout under ScaleCSIPoolDegraded's 5m hold, so a
//     degradation that keeps being observed reaches conditions before the alert
//     fires. Observable via scale_csi_pool_health_flip_pending = 1.
//  2. Alert hold (BOUNDED: the remainder of the rule's own `for: 5m`). Once the
//     second sample confirms, PVCs read Abnormal while ScaleCSIPoolDegraded is
//     still PENDING and therefore NOT firing. The ceiling in
//     maxBackendHealthInterval deliberately produces this ordering; it does not
//     remove the window. NOT observable via the two diagnostic gauges — both
//     read 0 here. Distinguish it with the alert's own pending state.
//  3. Recovery (BOUNDED: one sample, deliberately). The raw degraded series
//     drops to 0 on the FIRST healthy sample while the condition stays Abnormal
//     until the second: the alert has cleared and PVCs still read abnormal.
//     Nothing about the interval can remove this; it is the point of the damper.
//     Observable via scale_csi_pool_health_flip_pending = 1.
//  4. Poll stall (UNBOUNDED — it lasts until a SUCCESSFUL USABLE sample arrives,
//     and nothing here limits that). "The backend answered" is NOT the end of
//     it: a valid pool.query that simply does not contain the pool comes back as
//     `pool ... not found` (pkg/truenas/pool_health.go) and takes this same
//     failed-sample path. If usable samples stop arriving the condition HOLDS
//     its last value and the gauges FREEZE at theirs, so a single unconfirmed
//     degraded sample can keep the raw alert expression true while the condition
//     still reads normal. It is observable, not silent: scale_csi_pool_health_stale
//     goes to 1 on the first failed sample that finds an unconfirmed flip, and
//     the moment the TTL expires — at the sample attempt or at the CSI read that
//     first refuses to serve the snapshot, whichever comes first, NOT only when
//     a hung call finally returns. Past the TTL the condition falls back to
//     dataset-only, at which point flip_pending may still read 1 even though the
//     served condition no longer carries the held verdict; the next successful
//     sample clears it.
//  5. Observer lag (UNBOUNDED — the driver does not control it and cannot see
//     it). Prometheus sees these gauges only on its next successful SCRAPE and
//     changes alert state only on its next rule EVALUATION; `for: 5m` starts
//     when the EXPRESSION first evaluates true, not when the driver sampled. The
//     chart places no upper limit on the ServiceMonitor interval and a
//     scrape/evaluation outage is not limited at all. Two consequences, both
//     EXPECTED: after a recovery both diagnostic gauges can read 0 while the
//     alert is still firing, and before the first true evaluation the condition
//     can already be abnormal with no ALERTS{alertstate="pending"} series yet.
//     During a RollingUpdate two controller processes publish independently
//     sampled series that the alert's `max by (pool)` merges. Diagnose it by
//     comparing sample/scrape freshness and pinning the pod, never by reading
//     the two diagnostic gauges.
//  6. Cold start (UNBOUNDED — until the FIRST successful sample of this
//     process). All of this state is process-local and the CSI and metrics
//     servers are already serving before startBackendHealth produces anything
//     (see driver.go): conditions are dataset-only and the raw
//     scale_csi_pool_status/_healthy series DO NOT EXIST yet, so
//     ScaleCSIPoolDegraded cannot fire whatever the pool is doing. That is not
//     the poll-stall shape — there is nothing frozen to see. The first failing
//     sample therefore publishes scale_csi_pool_health_stale = 1 (and
//     flip_pending = 0) for the configured pool so the blind window is visible
//     and ScaleCSIPoolHealthStale can fire; the raw gauges stay ABSENT rather
//     than being invented.
const backendHealthFlipSamples = 2

// startBackendHealth launches the controller-only backend-health poll loop when
// backendHealth.enabled is set. Each tick is at most TWO bounded READ calls
// (pool.query + disk.temperature_alerts) — the loop never writes anything.
//
// DEFAULT OFF: an un-opted-in deployment issues zero additional API calls and
// its VolumeConditions keep the exact dataset-only semantics they had before.
func (d *Driver) startBackendHealth() {
	if d.config == nil || !d.config.BackendHealth.Enabled {
		return
	}
	configured, err := d.configuredBackendHealthInterval()
	if err != nil {
		klog.Errorf("Backend health polling disabled due to invalid interval %q: %v", d.config.BackendHealth.Interval, err)
		return
	}
	// Report the clamp exactly once, here, against the operator's own value.
	// resolveBackendHealthInterval stays silent because the condition path calls
	// it per volume.
	interval := clampBackendHealthInterval(configured)
	switch {
	case configured < minBackendHealthInterval:
		klog.Warningf("backendHealth.interval %v is below the %v floor; using %v", configured, minBackendHealthInterval, interval)
	case configured > maxBackendHealthInterval:
		klog.Warningf("backendHealth.interval %v exceeds the %v ceiling; using %v so a hysteresis-CONFIRMED VolumeCondition "+
			"flip (at most 2 x interval) still lands inside the ScaleCSIPoolDegraded alert's 5m hold while samples keep arriving",
			configured, maxBackendHealthInterval, interval)
	}
	pool := d.parentPoolName()
	if pool == "" {
		klog.Error("Backend health polling disabled: no zfs.parentDataset configured")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.backendHealthCancel = cancel
	d.backendHealthWg.Add(1)
	go func() {
		defer d.backendHealthWg.Done()
		klog.Infof("Backend health polling started: interval=%v pool=%s", interval, pool)
		run := func() {
			// Publish the staleness verdict BEFORE the call, not only after it: a
			// hung poll may burn the whole backendHealthCallTimeout, and until it
			// returns the error branch in sampleBackendHealth cannot run. Without
			// this, a snapshot that expires during a stalled call leaves a frozen
			// DEGRADED gauge alerting with BOTH diagnostics reading 0.
			d.markPoolHealthStaleIfExpired()
			callCtx, callCancel := context.WithTimeout(ctx, backendHealthCallTimeout)
			defer callCancel()
			d.sampleBackendHealth(callCtx, pool)
		}
		// Populate immediately so the first ControllerGetVolume after startup is
		// already pool-aware rather than falling back for a whole interval.
		run()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				klog.Info("Backend health polling stopped")
				return
			}
		}
	}()
}

func (d *Driver) stopBackendHealth() {
	if d.backendHealthCancel != nil {
		d.backendHealthCancel()
		d.backendHealthWg.Wait()
	}
}

// backendHealthTTL is how long a cached snapshot may drive VolumeConditions
// before it is considered stale. It is derived from the configured poll cadence
// so it self-tunes, and never falls below the interval floor.
func (d *Driver) backendHealthTTL() time.Duration {
	interval, err := d.resolveBackendHealthInterval()
	if err != nil || interval <= 0 {
		interval = 2 * minBackendHealthInterval
	}
	return time.Duration(backendHealthStaleIntervals) * interval
}

// sampleBackendHealth takes one health sample and publishes it to the cache and
// the Prometheus gauges. A failed sample leaves the PREVIOUS snapshot in place:
// a transient backend blip must not flip every PVC's condition, and a recent
// snapshot is strictly better information than none. Past backendHealthTTL the
// snapshot stops being served at all (see poolHealthSnapshot).
//
// PUBLICATION ORDER IS PART OF THE CONTRACT. The pool sample is published the
// INSTANT it is in hand, BEFORE the second backend read. Doing the
// disk.temperature_alerts call first — as this used to — opened a
// publication/commit gap: a VALID pool sample existed, timestamped, while the
// old raw gauges, the old condition, flip_pending and stale were all still
// exposed for however long that second call took (up to the remainder of
// backendHealthCallTimeout). That window fitted NONE of the named divergence
// classes: the raw sample had not changed (not confirmation lag), the sample had
// succeeded (not a poll stall), the exporter had not published it (not observer
// lag) and a previous sample existed (not cold start). It is closed here by
// construction — no backend I/O sits between acquiring a sample and publishing
// it — rather than being documented as a seventh class.
//
// The temperature count is therefore a FOLLOW-UP refresh of an already published
// sample. It cannot hold the health verdict back, and it cannot fabricate: until
// it lands, the pool carries the LAST KNOWN count (0 on a first sample), which
// the refresh then corrects.
func (d *Driver) sampleBackendHealth(ctx context.Context, pool string) {
	snapshot, err := d.truenasClient.PoolHealth(ctx, pool)
	if err != nil {
		if ctx.Err() == nil {
			klog.Warningf("Backend health sample failed for pool %s: %v", pool, err)
		}
		d.publishFailedSampleStaleness(pool)
		return
	}
	d.publishSample(snapshot)
	if snapshot.Degraded() {
		klog.Warningf("Pool %s is %s (healthy=%t detail=%q)", snapshot.Pool, snapshot.Status, snapshot.Healthy, snapshot.StatusDetail)
	}

	// Disk temperature alerts are a per-DISK signal; fan them out with the pool.
	alerts, alertErr := d.truenasClient.DiskTemperatureAlerts(ctx, snapshot.Disks)
	if alertErr != nil {
		if ctx.Err() == nil {
			klog.Warningf("Disk temperature alert sample failed for pool %s: %v; scale_csi_pool_disk_temp_alerts keeps its "+
				"last known value rather than being reset to 0", pool, alertErr)
		}
		return
	}
	d.publishTemperatureAlerts(snapshot.Pool, len(alerts))
}

// publishSample publishes ONE successful sample: the raw gauges, the hysteresis
// decision, the cached snapshot pointer, the staleness verdict and the
// last-success timestamp. All of it happens inside ONE critical section, so the
// (pointer, stale gauge) pair can never be observed half-updated and a
// concurrent read-path staleness decision can never survive across it.
//
// Metrics get the RAW sample, always: Prometheus must see a flap as a flap.
func (d *Driver) publishSample(snapshot *truenas.PoolHealthSnapshot) {
	d.backendHealthPublishMu.Lock()
	defer d.backendHealthPublishMu.Unlock()

	// Carry the last known temperature count forward instead of publishing a
	// fabricated 0 for the length of the disk.temperature_alerts call.
	if previous := d.backendHealth.Load(); previous != nil && previous.Pool == snapshot.Pool {
		snapshot.TemperatureAlerts = previous.TemperatureAlerts
	}
	SetPoolHealthMetrics(snapshot)
	d.publishBackendHealthLocked(snapshot)
	SetPoolHealthStale(snapshot.Pool, false)
	SetPoolHealthLastSuccess(snapshot.Pool, snapshot.SampledAt)
}

// publishTemperatureAlerts refreshes the per-DISK temperature count on an
// ALREADY PUBLISHED sample. It deliberately does NOT run the hysteresis: the
// count plays no part in PoolHealthSnapshot.Degraded(), so re-publishing the
// sample through publishBackendHealthLocked would count one backend sample twice and
// confirm a held flip with a single observation.
//
// The cached snapshot is replaced by an updated COPY: the stored pointer is read
// concurrently by every CSI read, so mutating it in place would be a genuine
// data race.
func (d *Driver) publishTemperatureAlerts(pool string, alerts int) {
	if pool == "" {
		return
	}
	d.backendHealthPublishMu.Lock()
	defer d.backendHealthPublishMu.Unlock()
	current := d.backendHealth.Load()
	if current == nil || current.Pool != pool {
		return
	}
	if current.TemperatureAlerts != alerts {
		updated := *current
		updated.TemperatureAlerts = alerts
		d.backendHealth.Store(&updated)
	}
	SetPoolDiskTemperatureAlerts(pool, alerts)
}

// publishFailedSampleStaleness publishes the staleness verdict from inside the
// poll loop so it keeps updating even while the backend is unreachable.
//
// The verdict answers ONE question: is the condition the driver is currently
// serving backed by a fresh sample? It is not in any of the cases below, and all
// of them must be visible or classes 4 and 6 in backendHealthFlipSamples would
// be silent:
//   - no successful sample has landed since this process started (COLD START);
//   - the snapshot aged past its TTL, so conditions have fallen back to
//     dataset-only; or
//   - a flip is pending and its CONFIRMING sample is exactly the one that just
//     failed to arrive, so the served verdict is one the latest raw sample
//     already contradicts.
//
// It runs inside the same critical section as publishSample so its verdict and
// a successful sample's verdict can never interleave into a lost update.
func (d *Driver) publishFailedSampleStaleness(pool string) {
	d.backendHealthPublishMu.Lock()
	previous := d.backendHealth.Load()
	if previous == nil {
		// COLD START (class 6). There is no previous snapshot to freeze, so the raw
		// scale_csi_pool_* series do not exist at all and ScaleCSIPoolDegraded
		// cannot fire whatever the pool is doing — an unbounded blind window that
		// used to be COMPLETELY silent because the staleness verdict was published
		// only when a previous snapshot existed.
		//
		// The honest publication is: stale = 1 (nothing served is sample-backed)
		// and flip_pending = 0 (there is no held transition). The raw health gauges
		// are deliberately NOT invented — an absent scale_csi_pool_status is the
		// truth, and ScaleCSIPoolHealthStale is the alert that covers it.
		//
		// stale = 1 does NOT identify WHY: a misspelled, renamed or deleted pool
		// produces exactly this state, because the label is the CONFIGURED pool
		// string and nothing has verified it against the appliance. Read it as "no
		// fresh sample for this configured identity", never as "an existing pool is
		// stale".
		if pool == "" {
			d.backendHealthPublishMu.Unlock()
			return
		}
		SetPoolHealthStale(pool, true)
		SetPoolHealthFlipPending(pool, false)
		d.backendHealthPublishMu.Unlock()
		klog.Warningf("Backend health has no successful sample yet for pool %s: VolumeConditions are dataset-only and the raw "+
			"scale_csi_pool_* series stay ABSENT until a successful usable sample arrives, so ScaleCSIPoolDegraded cannot fire. "+
			"scale_csi_pool_health_stale is 1 so this window is not silent. This does not say the pool is unhealthy or even that "+
			"it EXISTS — %q is the configured name, and a missing or renamed pool reaches this same state", pool, pool)
		return
	}
	age := time.Since(previous.SampledAt)
	ttl := d.backendHealthTTL()
	expired := age > ttl
	unconfirmedFlip := d.backendHealthPendingFlips.Load() > 0
	SetPoolHealthStale(previous.Pool, expired || unconfirmedFlip)
	d.backendHealthPublishMu.Unlock()
	switch {
	case expired:
		klog.Warningf("Backend health snapshot for pool %s is stale (last successful sample %v ago, TTL %v); "+
			"VolumeConditions fall back to dataset-only until a successful usable sample arrives",
			previous.Pool, age.Truncate(time.Second), ttl)
	case unconfirmedFlip:
		klog.Warningf("Backend health snapshot for pool %s is unconfirmed: a held health transition is still waiting for a "+
			"confirming sample (last successful sample %v ago, TTL %v). VolumeConditions keep the previous verdict, "+
			"which the raw scale_csi_pool_* gauges already contradict",
			previous.Pool, age.Truncate(time.Second), ttl)
	}
}

// markPoolHealthStaleIfExpired raises the staleness verdict for an already
// expired snapshot without waiting for an in-flight backend call to return. The
// poll loop calls it before each sample so a stall that outlives the TTL cannot
// leave a frozen gauge alerting with both diagnostics at 0 for a whole
// backendHealthCallTimeout.
func (d *Driver) markPoolHealthStaleIfExpired() {
	snapshot := d.backendHealth.Load()
	if snapshot == nil || snapshot.SampledAt.IsZero() {
		return
	}
	if time.Since(snapshot.SampledAt) > d.backendHealthTTL() {
		d.markPoolHealthStale(snapshot)
	}
}

// markPoolHealthStale publishes stale = 1 for a snapshot the driver has just
// decided it can no longer serve.
//
// It is deliberately SILENT (no logging): poolHealthSnapshot calls it, and
// ListVolumes composes one condition per volume, so logging here would be a
// per-volume storm. The poll loop does the logging.
//
// This runs on the CSI read path, CONCURRENTLY WITH THE POLLER, and the decision
// it publishes is about ONE snapshot: "the pointer I read is expired". Both the
// re-check of that pointer and the gauge write therefore happen inside the SAME
// critical section publishSample uses.
//
// Writing the gauge first and re-reading the pointer afterwards — which is what
// this did — is NOT synchronization, and the failure is a lost update rather
// than a data race, so `go test -race` cannot see it:
//
//	 1. the reader marks expired snapshot S stale;
//	 2. the poller clears staleness for a successful sample it has NOT yet
//	    stored;
//	 3. the reader re-reads, still sees S, and leaves stale = 1;
//	 4. the poller stores the fresh pointer.
//
// End state: a fresh cached sample with stale = 1 until the NEXT successful
// sample. Reversing the poller's two steps fails the same way with the roles
// swapped. Under one mutex neither interleaving exists: if the pointer still is
// S the poller has not published (and its later write, ordered after this one,
// is the newer truth); if it is not, the decision is already superseded and
// nothing is written at all.
func (d *Driver) markPoolHealthStale(snapshot *truenas.PoolHealthSnapshot) {
	if snapshot == nil || snapshot.Pool == "" {
		return
	}
	d.backendHealthPublishMu.Lock()
	defer d.backendHealthPublishMu.Unlock()
	if d.backendHealth.Load() != snapshot {
		// A successful sample superseded this snapshot; ITS verdict is authoritative
		// and must not be overwritten by a decision made against an older pointer.
		return
	}
	SetPoolHealthStale(snapshot.Pool, true)
}

// publishBackendHealthLocked applies the fan-out hysteresis and updates the cache
// that drives every managed volume's VolumeCondition. The caller MUST hold
// backendHealthPublishMu: the pending-flip counter, the flip_pending gauge, the
// snapshot pointer and the staleness gauge are one observable state, and a
// reader deciding staleness against the pointer must not see them half-written.
//
// scale_csi_pool_health_flip_pending tracks the HELD-FLIP window exactly: 1 from
// the unconfirmed sample until a successful sample resolves it. That is not the
// same thing as "the raw gauges and the condition disagree" — it reads 0 during
// the alert-hold class, and past the staleness TTL it can still read 1 after the
// condition has fallen back to dataset-only. See backendHealthFlipSamples for
// the canonical numbered list of divergence classes.
func (d *Driver) publishBackendHealthLocked(snapshot *truenas.PoolHealthSnapshot) {
	previous := d.backendHealth.Load()
	if previous == nil || previous.Degraded() == snapshot.Degraded() {
		// No transition to confirm (or nothing to compare against yet).
		d.backendHealthPendingFlips.Store(0)
		SetPoolHealthFlipPending(snapshot.Pool, false)
		d.backendHealth.Store(snapshot)
		return
	}

	pending := d.backendHealthPendingFlips.Add(1)
	if pending < backendHealthFlipSamples {
		klog.V(2).Infof("Pool %s health transition (%s -> %s) held for confirmation (%d/%d consecutive samples); "+
			"per-PVC VolumeConditions are unchanged for now and deliberately disagree with the raw gauges until it confirms",
			snapshot.Pool, previous.Status, snapshot.Status, pending, backendHealthFlipSamples)
		SetPoolHealthFlipPending(snapshot.Pool, true)
		// Keep serving the previous verdict, but carry the fresh sample time so the
		// staleness TTL measures backend liveness, not the age of the verdict.
		held := *previous
		held.SampledAt = snapshot.SampledAt
		d.backendHealth.Store(&held)
		return
	}
	klog.Infof("Pool %s health transition (%s -> %s) confirmed by %d consecutive samples; updating every managed volume's condition",
		snapshot.Pool, previous.Status, snapshot.Status, pending)
	d.backendHealthPendingFlips.Store(0)
	SetPoolHealthFlipPending(snapshot.Pool, false)
	d.backendHealth.Store(snapshot)
}

// poolHealthSnapshot returns the most recent sample, or nil when the poller is
// disabled, has not yet produced one, or the cached one has aged past its TTL.
//
// Returning nil past the TTL is what makes composeVolumeCondition fall back to
// the pre-GF5 dataset-only condition instead of asserting hours-old pool state
// as current fact.
//
// The TTL expires HERE, on the read path — not when a stalled poll finally
// returns — so the staleness verdict is published at the same instant the
// snapshot stops being served. Otherwise a call burning the full
// backendHealthCallTimeout would leave the served condition already fallen back
// while both diagnostic gauges still read 0.
func (d *Driver) poolHealthSnapshot() *truenas.PoolHealthSnapshot {
	snapshot := d.backendHealth.Load()
	if snapshot == nil {
		return nil
	}
	if !snapshot.SampledAt.IsZero() && time.Since(snapshot.SampledAt) > d.backendHealthTTL() {
		d.markPoolHealthStale(snapshot)
		return nil
	}
	return snapshot
}

// volumeCondition composes the dataset-level condition with the pool-level
// backend health snapshot. It is the single helper ControllerGetVolume and
// ListVolumes share, so both RPCs — and whichever the external-health-monitor
// picks — report an identical condition.
//
// Attribution rationale: ZFS exposes no per-dataset health. Every managed volume
// lives on ONE pool, so a pool condition applies to all of them; this is a
// deliberate fan-out, not an approximation.
//
// Severity is deliberately conservative:
//   - a definitive dataset-level negative marker still wins (unchanged);
//   - DEGRADED/FAULTED/UNAVAIL -> Abnormal, because the data path is at risk;
//   - an in-progress scrub/resilver or a disk temperature alert -> NOT abnormal,
//     just a descriptive message. A routine monthly scrub must never mark every
//     PVC in the cluster unhealthy.
func (d *Driver) volumeCondition(ds *truenas.Dataset) *csi.VolumeCondition {
	return composeVolumeCondition(volumeConditionFromDataset(ds), d.poolHealthSnapshot())
}

func composeVolumeCondition(base *csi.VolumeCondition, snapshot *truenas.PoolHealthSnapshot) *csi.VolumeCondition {
	if snapshot == nil {
		return base
	}
	if base != nil && base.GetAbnormal() {
		// A dataset-level failure is more specific than a pool-level one; keep it.
		return base
	}

	if snapshot.Degraded() {
		message := fmt.Sprintf("pool %s is %s", snapshot.Pool, snapshot.Status)
		if snapshot.StatusDetail != "" {
			message += ": " + snapshot.StatusDetail
		}
		return &csi.VolumeCondition{Abnormal: true, Message: message}
	}

	warnings := make([]string, 0, 2)
	if snapshot.Scanning() {
		function := snapshot.ScanFunction
		if function == "" {
			function = "scan"
		}
		warnings = append(warnings, fmt.Sprintf("pool %s %s in progress (%.1f%%)", snapshot.Pool, function, snapshot.ScanPercentage))
	}
	if snapshot.ScanErrors > 0 {
		warnings = append(warnings, fmt.Sprintf("pool %s last scan reported %d errors", snapshot.Pool, snapshot.ScanErrors))
	}
	if snapshot.TemperatureAlerts > 0 {
		warnings = append(warnings, fmt.Sprintf("pool %s has %d disk temperature alert(s)", snapshot.Pool, snapshot.TemperatureAlerts))
	}
	if len(warnings) == 0 {
		return base
	}

	message := joinNonEmpty(warnings, "; ")
	if base != nil && base.GetMessage() != "" {
		message = base.GetMessage() + "; " + message
	}
	return &csi.VolumeCondition{Abnormal: false, Message: message}
}

func joinNonEmpty(values []string, sep string) string {
	out := ""
	for _, value := range values {
		if value == "" {
			continue
		}
		if out != "" {
			out += sep
		}
		out += value
	}
	return out
}
