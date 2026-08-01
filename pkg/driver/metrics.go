package driver

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	metricsNamespace = "scale_csi"
)

// metricNames accumulates the fully-qualified name of every metric as it is
// registered by the reg* helpers below. It is the single source of truth that
// MetricNames() exposes and that the chart drift test compares dashboard/alert
// expressions against — registering a metric and naming it can never diverge.
var metricNames []string

// histogramNames accumulates the fully-qualified base name of every histogram
// (and summary) metric. The chart drift test uses it to decide whether a
// generated _bucket/_sum/_count suffix is legal: those series exist ONLY for
// histogram/summary bases, so `scale_csi_<gauge>_count` must be rejected.
var histogramNames []string

// recordName appends the metric's fully-qualified name (namespace + name) to
// metricNames. The name is derived from the SAME opts the collector is built
// from, so there is no second copy of the name to drift.
func recordName(namespace, subsystem, name string) {
	metricNames = append(metricNames, prometheus.BuildFQName(namespace, subsystem, name))
}

// The reg* helpers replace promauto: they build the collector, register it on
// the default registerer (identical to promauto), and record its name. A metric
// added through them is automatically visible to MetricNames().

func regCounter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	prometheus.MustRegister(c)
	recordName(opts.Namespace, opts.Subsystem, opts.Name)
	return c
}

func regCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	prometheus.MustRegister(c)
	recordName(opts.Namespace, opts.Subsystem, opts.Name)
	return c
}

func regGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	prometheus.MustRegister(g)
	recordName(opts.Namespace, opts.Subsystem, opts.Name)
	return g
}

func regGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(opts, labels)
	prometheus.MustRegister(g)
	recordName(opts.Namespace, opts.Subsystem, opts.Name)
	return g
}

func regHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	prometheus.MustRegister(h)
	recordName(opts.Namespace, opts.Subsystem, opts.Name)
	histogramNames = append(histogramNames, prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name))
	return h
}

// MetricNames returns the fully-qualified names of every metric the driver
// registers, in registration order. The returned slice is a copy. Adding a
// metric through the reg* helpers is the ONLY way to make it appear here; the
// chart drift test fails if a dashboard/alert names anything not in this set.
func MetricNames() []string {
	return append([]string(nil), metricNames...)
}

// HistogramMetricNames returns the fully-qualified base names of every
// histogram/summary the driver registers, in registration order. The chart
// drift test uses this set to permit the Prometheus-generated
// _bucket/_sum/_count suffixes ONLY on histogram bases. The returned slice is a
// copy.
func HistogramMetricNames() []string {
	return append([]string(nil), histogramNames...)
}

var (
	// CSI operation metrics
	csiOperationsTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "operations_total",
			Help:      "Total number of CSI operations",
		},
		[]string{"operation", "status", "code"},
	)

	csiOperationsDuration = regHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "operations_duration_seconds",
			Help:      "Duration of CSI operations in seconds",
			Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"operation"},
	)

	// TrueNAS API metrics
	truenasRequestsTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_requests_total",
			Help:      "Total number of TrueNAS API requests",
		},
		[]string{"method", "status"},
	)

	truenasRequestsDuration = regHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_requests_duration_seconds",
			Help:      "Duration of TrueNAS API requests in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method"},
	)

	// Connection metrics
	truenasConnectionStatus = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_connection_status",
			Help:      "TrueNAS connection status (1 = connected, 0 = disconnected)",
		},
	)

	truenasConnectionsActive = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_connections_active",
			Help:      "Number of active TrueNAS WebSocket connections",
		},
	)

	// jobDispatcherSubscribed is 1 when at least one pooled connection holds a
	// live core.get_jobs subscription and 0 when the driver has degraded to the
	// pure-poll fallback (higher API load + latency). Published from the health
	// tick off Client.AnyConnectionJobSubscribed (E2/O7).
	//
	// It is a label-less GaugeVec (not a plain Gauge) on purpose, and is touched
	// ONLY by the controller health tick (which runs only with a live TrueNAS
	// client). A label-less vec exports NO series until its single child is
	// created, so a node-mode process — which builds no client and never calls
	// SetJobDispatcherSubscribed — exports nothing for it. This is the same
	// mechanism as the controller-only pool_*_bytes GaugeVecs and is what keeps
	// an unscoped min()/==0 from ever observing a phantom node-mode 0 (codex H1).
	jobDispatcherSubscribed = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "job_dispatcher_subscribed",
			Help:      "1 when a pooled connection holds a live core.get_jobs subscription; 0 = pure-poll fallback",
		},
		nil,
	)

	// iSCSI metrics
	iscsiSessionsTotal = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "iscsi_sessions_total",
			Help:      "Total number of active iSCSI sessions on this node",
		},
	)

	nvmeSessionsTotal = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "nvme_sessions_total",
			Help:      "Total number of active NVMe-oF sessions on this node",
		},
	)

	nodeConnectTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "node_connect_total",
			Help:      "Total number of node transport connection attempts",
		},
		[]string{"transport", "result"},
	)

	gcSessionsDisconnectedTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "gc_sessions_disconnected_total",
			Help:      "Total number of orphaned sessions disconnected by session garbage collection",
		},
		[]string{"transport"},
	)

	// Controller orphan reconcile metrics.
	orphanVolumes = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_volumes",
			Help:      "Number of CSI-managed TrueNAS volumes without a live Kubernetes PV handle",
		},
	)

	orphanSnapshots = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_snapshots",
			Help:      "Number of CSI-managed TrueNAS snapshots without a live Kubernetes VolumeSnapshotContent handle",
		},
	)

	spentRestoreSnapshots = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spent_restore_snapshots",
			Help:      "Number of spent VolSync restore-destination snapshots whose source PVC is no longer Bound",
		},
	)

	orphanVolumesBytes = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_volumes_bytes",
			Help:      "Reported used bytes held by detected orphan CSI-managed TrueNAS volumes",
		},
	)

	orphanSnapshotsBytes = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_snapshots_bytes",
			Help:      "Reported used bytes held by detected orphan CSI-managed TrueNAS snapshots",
		},
	)

	// Deferred-delete tombstones are retained by design on backends without ZFS
	// deferred destroy until their last restored clone disappears. Detection is
	// always on; guarded reaping only runs where reconcile deletion is enabled,
	// so these gauges are how default installs see the reapable backlog.
	tombstoneSnapshots = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "tombstone_snapshots",
			Help:      "Number of driver-tombstoned deferred-delete snapshots awaiting reap (ledger-proven, age-eligible)",
		},
	)

	tombstoneSnapshotsBytes = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "tombstone_snapshots_bytes",
			Help:      "Reported used bytes held by driver-tombstoned deferred-delete snapshots awaiting reap",
		},
	)

	remnantVolumes = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "remnant_volumes",
			Help:      "Number of marker-proven unstamped remnant datasets awaiting guarded reap (interrupted creates with no possible retry)",
		},
	)

	// manualRecoveryTombstones are driver-tombstoned deferred-delete snapshots
	// the guarded reaper REFUSES to reap because creation-time snapshot identity
	// is unproven. They never drain on their own — each is an operator-attention
	// item, so the gauge is alertable rather than merely informational. Populated
	// from ReconcileReport.ManualRecoveryTombstoneCount (E2/O5), which was
	// previously computed but dropped on the floor.
	manualRecoveryTombstones = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "manual_recovery_tombstones",
			Help:      "Driver-tombstoned deferred-delete snapshots the guarded reaper refuses to reap (unproven creation-time identity); operator attention required",
		},
	)

	// tombstoneReapedTotal counts successful tombstone reaps labeled by discovery
	// path: "ledger" (the strict ledger-driven reaper) or "scan_fallback"
	// (reconcile.tombstoneReaper.scanFallback). The fixed 2-value enum gives
	// scan-fallback coverage visibility without an unbounded label (E2/O6).
	tombstoneReapedTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "tombstone_reaped_total",
			Help:      "Total driver-tombstoned deferred-delete snapshots successfully reaped, by discovery path",
		},
		[]string{"path"},
	)

	// Pool-capacity gauges (E4). Populated only when capacity.gaugeEnabled by a
	// controller-only poll loop; cardinality is fixed at one series per gauge for
	// this single-backend driver, labeled {pool,dataset}. pool_capacity_bytes is
	// used+available from the same pool.dataset.query row that feeds
	// pool_available_bytes, so the near-full ratio is internally consistent.
	poolAvailableBytes = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_available_bytes",
			Help:      "ZFS-computed available bytes on the parent dataset (parity/quota/reservation-aware)",
		},
		[]string{"pool", "dataset"},
	)

	poolCapacityBytes = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_capacity_bytes",
			Help:      "Total usable bytes (used + available) on the parent dataset",
		},
		[]string{"pool", "dataset"},
	)

	// Backend-health gauges (GF5 E4). Published only when backendHealth.enabled
	// by a controller-only READ-ONLY poll loop. ZFS has no per-dataset health, so
	// these are per-POOL signals that the VolumeCondition path fans out to every
	// managed volume on that pool.
	//
	// pool_status is a one-hot series over the {pool,status} label pair: exactly
	// one status label is 1 at a time, the rest are 0, so `max by (pool)` style
	// queries and label-based alerting both work.
	poolStatus = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_status",
			Help:      "ZFS pool status, one-hot over the status label (1 = current status)",
		},
		[]string{"pool", "status"},
	)

	poolHealthy = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_healthy",
			Help:      "1 when TrueNAS reports the pool healthy, 0 otherwise",
		},
		[]string{"pool"},
	)

	// poolScanState is one-hot over {pool,function,state}: it reports whether a
	// scrub or resilver is SCANNING/FINISHED/CANCELED. A running scrub is normal
	// maintenance, not ill health, which is why it is a separate series from
	// pool_healthy rather than folded into it.
	poolScanState = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_scan_state",
			Help:      "ZFS pool scrub/resilver state, one-hot over the function and state labels (1 = current)",
		},
		[]string{"pool", "function", "state"},
	)

	poolScanErrors = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_scan_errors",
			Help:      "Errors reported by the pool's most recent scrub/resilver",
		},
		[]string{"pool"},
	)

	poolDiskTempAlerts = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_disk_temp_alerts",
			Help:      "Number of the pool's member disks currently raising a temperature alert",
		},
		[]string{"pool"},
	)

	// poolHealthStale is 1 when the VolumeCondition the driver is serving is NOT
	// backed by a fresh sample: either the cached snapshot aged past its TTL (so
	// conditions have fallen back to dataset-only), or a pending condition flip
	// never received the confirming sample it is waiting on because the backend
	// stopped answering. Every other scale_csi_pool_* gauge is only current while
	// this is 0.
	poolHealthStale = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_health_stale",
			Help:      "1 when the served VolumeCondition is not backed by a fresh sample (snapshot past its TTL, or a pending flip whose confirming sample never arrived)",
		},
		[]string{"pool"},
	)

	// poolHealthFlipPending is 1 while a health transition is waiting for its
	// confirming sample. It makes the confirmation-lag and recovery classes of
	// raw-vs-condition divergence observable instead of implicit. It is NOT a
	// complete disagreement detector: it reads 0 during the alert-hold class, and
	// past the staleness TTL it can still read 1 after the condition has fallen
	// back to dataset-only. See backendHealthFlipSamples for the canonical four
	// classes.
	poolHealthFlipPending = regGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_health_flip_pending",
			Help:      "1 while a pool-health transition awaits its confirming sample; until the staleness TTL expires the per-PVC VolumeCondition still carries the previous verdict",
		},
		[]string{"pool"},
	)

	reconcileLastSuccessTimestamp = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_last_success_timestamp_seconds",
			Help:      "Unix timestamp of the most recent completed controller reconcile pass",
		},
	)

	reconcileFailuresTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_failures_total",
			Help:      "Total reconcile failures and isolated object skips by phase",
		},
		[]string{"phase"},
	)

	replicationJobsAbortedTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "replication_jobs_aborted_total",
			Help:      "Total driver-owned one-time replication jobs successfully aborted",
		},
		[]string{"reason"},
	)

	fencingDeferredTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fencing_deferred_total",
			Help:      "Total backend fencing operations deferred to preserve upgrade compatibility",
		},
		[]string{"reason", "protocol"},
	)

	fencingStaleDeferredTotal = regCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fencing_stale_deferred_total",
			Help:      "Total stale publication cleanup passes deferred by the empty-VolumeAttachment safety brake",
		},
	)

	// fencingTakeoverTotal counts successful synchronous stale-publication
	// takeovers — the single most dangerous operation on a live strict cluster
	// (it revokes one node's grant to hand the volume to another). It is labeled
	// by reason so alerting can watch the stale_record path specifically.
	fencingTakeoverTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fencing_takeover_total",
			Help:      "Total successful synchronous fencing takeovers, labeled by reason",
		},
		[]string{"reason"},
	)

	// fencingProvenanceOverflowTotal counts publishes refused because a node's
	// additive CSI-added grant provenance list exceeded the hard cap even after
	// compaction — i.e. it consists entirely of backend-live entries. Provenance
	// is never silently evicted (that would turn revocable grants into permanent
	// static policy); the publish fails closed with ResourceExhausted instead.
	fencingProvenanceOverflowTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fencing_provenance_overflow_total",
			Help:      "Total publishes refused because backend-live additive fencing provenance exceeded the per-node cap",
		},
		[]string{"protocol"},
	)

	// deleteVolumeOrphanCleanupFailuresTotal counts DeleteVolume calls whose
	// dataset was already gone but whose best-effort residual share cleanup failed.
	// The delete still succeeds (CSI DeleteVolume is idempotent: volume-not-found is
	// success) and the orphan reconcile sweeps the residue, so this is observable
	// rather than fatal. Labeled by protocol so a stuck backend is identifiable.
	deleteVolumeOrphanCleanupFailuresTotal = regCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "delete_volume_orphan_cleanup_failures_total",
			Help:      "Total DeleteVolume calls with an absent dataset whose residual share cleanup failed best-effort",
		},
		[]string{"protocol"},
	)

	// Circuit breaker metrics
	circuitBreakerState = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0 = closed, 1 = open, 2 = half-open)",
		},
	)

	circuitBreakerFailuresTotal = regCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_failures_total",
			Help:      "Total number of failures recorded by the circuit breaker",
		},
	)

	circuitBreakerSuccessesTotal = regCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_successes_total",
			Help:      "Total number of successes recorded by the circuit breaker",
		},
	)

	circuitBreakerOpensTotal = regCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_opens_total",
			Help:      "Total number of times the circuit breaker has opened",
		},
	)

	circuitBreakerCurrentFailures = regGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_current_failures",
			Help:      "Current number of consecutive failures in the circuit breaker",
		},
	)
)

func init() {
	// The transport counter's benign_* status values (E1) are intentionally NOT
	// pre-touched here. The method label set is large (every TrueNAS API method),
	// so pre-creating every method×benign-value series would be pure cardinality
	// bloat for series that may never appear. Prometheus rate() tolerates a
	// series' first appearance, so lazy creation on the first benign event is
	// correct and cheaper. Only the small, fixed enums below are pre-touched.
	for _, reason := range []string{
		truenas.ReplicationJobAbortReasonContextEnded,
		truenas.ReplicationJobAbortReasonCopyFailed,
		replicationJobReasonCreateVolumeFailed,
		replicationJobReasonMissingMarker,
		replicationJobReasonMissingSourceDataset,
	} {
		replicationJobsAbortedTotal.WithLabelValues(reason).Add(0)
	}
	for _, protocol := range []string{"nfs", "iscsi", "nvmeof"} {
		fencingDeferredTotal.WithLabelValues("missing_identity", protocol).Add(0)
	}
	fencingDeferredTotal.WithLabelValues("outside_allowed_network", "nfs").Add(0)
	fencingTakeoverTotal.WithLabelValues(fencingTakeoverReasonStaleRecord).Add(0)
	for _, protocol := range []string{"nfs", "nvmeof"} {
		fencingProvenanceOverflowTotal.WithLabelValues(protocol).Add(0)
	}
	tombstoneReapedTotal.WithLabelValues(tombstoneReapedPathLedger).Add(0)
	tombstoneReapedTotal.WithLabelValues(tombstoneReapedPathScanFallback).Add(0)
}

// RecordCSIOperation records metrics for a CSI operation
func RecordCSIOperation(operation string, duration float64, err error) {
	operationStatus := "success"
	code := codes.OK
	if err != nil {
		code = grpcstatus.Code(err)
		switch code {
		case codes.Aborted, codes.NotFound, codes.AlreadyExists:
			operationStatus = "benign"
		default:
			operationStatus = "error"
		}
	}
	csiOperationsTotal.WithLabelValues(operation, operationStatus, code.String()).Inc()
	csiOperationsDuration.WithLabelValues(operation).Observe(duration)
}

// RecordTrueNASRequest records metrics for a TrueNAS API request.
//
// The status label carries a 5-value outcome taxonomy (E1): success,
// benign_exists, benign_notfound, benign_aborted, and error. Benign idempotent
// outcomes (AlreadyExists/NotFound) and lock-contention retries MOVE OUT of
// "error" so this per-method transport counter tells the same truth as the
// RPC-level operations_total. Existing status="error" selectors keep working and
// become honest for free; see docs/release-notes-next.md for the migration table.
//
// The classifier is method-agnostic, so the iscsi.auth.* peer-CRUD calls added
// for CHAP are classified benign_exists on their idempotent enforcement-boundary
// creates automatically — no per-method special-casing.
//
// Latency is observed for every outcome (the histogram has no status label).
func RecordTrueNASRequest(method string, duration float64, err error) {
	truenasRequestsTotal.WithLabelValues(method, trueNASRequestStatus(err)).Inc()
	truenasRequestsDuration.WithLabelValues(method).Observe(duration)
}

// trueNASRequestStatus classifies a TrueNAS API outcome for the transport
// counter. Order matters: the idempotency classifiers are checked before the
// generic error fallthrough.
func trueNASRequestStatus(err error) string {
	switch {
	case err == nil:
		return "success"
	case truenas.IsAlreadyExistsError(err):
		return "benign_exists"
	case truenas.IsNotFoundError(err):
		return "benign_notfound"
	case truenas.IsLockContentionError(err):
		return "benign_aborted"
	default:
		return "error"
	}
}

// SetTrueNASConnectionStatus sets the connection status metric
func SetTrueNASConnectionStatus(connected bool) {
	if connected {
		truenasConnectionStatus.Set(1)
	} else {
		truenasConnectionStatus.Set(0)
	}
}

// SetTrueNASActiveConnections sets the number of active connections
func SetTrueNASActiveConnections(count int) {
	truenasConnectionsActive.Set(float64(count))
}

// SetJobDispatcherSubscribed publishes whether any pooled connection holds a
// live core.get_jobs subscription (true → 1). False means the driver has
// degraded to the pure-poll fallback. Calling it creates the label-less vec's
// single child, which is what makes the series exportable — so it must only be
// called in controller mode (see the gauge's declaration comment, codex H1).
func SetJobDispatcherSubscribed(subscribed bool) {
	if subscribed {
		jobDispatcherSubscribed.WithLabelValues().Set(1)
	} else {
		jobDispatcherSubscribed.WithLabelValues().Set(0)
	}
}

// SetISCSISessions sets the number of active iSCSI sessions
func SetISCSISessions(count int) {
	iscsiSessionsTotal.Set(float64(count))
}

// SetNVMESessions sets the number of active NVMe-oF sessions.
func SetNVMESessions(count int) {
	nvmeSessionsTotal.Set(float64(count))
}

// RecordNodeConnect records a node transport connection attempt.
func RecordNodeConnect(transport, result string) {
	nodeConnectTotal.WithLabelValues(transport, result).Inc()
}

// RecordGCSessionDisconnected records a successful orphan session disconnect.
func RecordGCSessionDisconnected(transport string) {
	gcSessionsDisconnectedTotal.WithLabelValues(transport).Inc()
}

// Tombstone reap discovery paths for tombstoneReapedTotal (E2/O6). The label is
// a fixed 2-value enum, never a path or ID.
const (
	tombstoneReapedPathLedger       = "ledger"
	tombstoneReapedPathScanFallback = "scan_fallback"
)

// RecordTombstoneReaped counts a successful tombstone reap by discovery path
// (tombstoneReapedPathLedger or tombstoneReapedPathScanFallback).
func RecordTombstoneReaped(path string) {
	tombstoneReapedTotal.WithLabelValues(path).Inc()
}

// SetOrphanReconcileMetrics publishes the latest detection report, including a
// partial report from a failed pass so gauges never silently freeze.
func SetOrphanReconcileMetrics(report ReconcileReport) {
	orphanVolumes.Set(float64(report.OrphanVolumeCount))
	orphanSnapshots.Set(float64(report.OrphanSnapshotCount))
	spentRestoreSnapshots.Set(float64(report.SpentRestoreSnapshotCount))
	orphanVolumesBytes.Set(float64(report.OrphanVolumeBytes))
	orphanSnapshotsBytes.Set(float64(report.OrphanSnapshotBytes))
	tombstoneSnapshots.Set(float64(report.TombstoneSnapshotCount))
	tombstoneSnapshotsBytes.Set(float64(report.TombstoneSnapshotBytes))
	remnantVolumes.Set(float64(report.RemnantVolumeCount))
	manualRecoveryTombstones.Set(float64(report.ManualRecoveryTombstoneCount))
}

func RecordReconcileSuccess(at time.Time) {
	reconcileLastSuccessTimestamp.Set(float64(at.Unix()))
}

// poolStatusLabels is the fixed status label set the one-hot pool_status series
// covers. Keeping it fixed bounds cardinality and means a status that goes away
// is explicitly zeroed instead of leaving a stale 1 behind.
var poolStatusLabels = []string{
	truenas.PoolStatusOnline,
	truenas.PoolStatusDegraded,
	truenas.PoolStatusFaulted,
	truenas.PoolStatusOffline,
	truenas.PoolStatusUnavail,
	truenas.PoolStatusRemoved,
}

// poolScanNone is the label used on BOTH the `function` and the `state`
// dimension when the pool reports no scan at all. Giving idle a REPRESENTABLE
// state is what makes the cross-product genuinely one-hot: without it an idle
// pool zeroed every fixed cell and set none, silently degrading the documented
// "exactly one cell is 1" contract to "at most one".
const poolScanNone = "NONE"

// poolScanStateLabels is the fixed scan-state label set, zeroed the same way.
var poolScanStateLabels = []string{
	truenas.PoolScanStateScanning,
	truenas.PoolScanStateFinished,
	truenas.PoolScanStateCanceled,
	poolScanNone,
}

// poolScanFunctionLabels is the fixed scan-FUNCTION label set. One-hot means
// one-hot across the whole {function} × {state} cross-product, not merely within
// the current function: a SCRUB that FINISHED followed by a RESILVER that is
// SCANNING would otherwise leave BOTH
// {function="SCRUB",state="FINISHED"}=1 and {function="RESILVER",state="SCANNING"}=1
// exported simultaneously, which breaks every query written against the
// documented contract.
var poolScanFunctionLabels = []string{
	truenas.PoolScanFunctionScrub,
	truenas.PoolScanFunctionResilver,
	poolScanNone,
}

// poolDynamicStatuses remembers the UNRECOGNIZED pool_status labels this process
// has ever exported, per pool, so they can be zeroed when the pool moves on.
// Without it an unknown status stays pinned at 1 forever — the exact
// stale-alerting failure the one-hot design exists to prevent.
//
// poolDynamicScanCells does the same job for pool_scan_state, keyed by the
// {function,state} PAIR. Both dimensions can carry a value outside the fixed
// sets (an unknown scan function, an unknown scan state, or both), and such a
// cell lives outside the zeroing cross-product below — so without this registry
// it would stay pinned at 1 forever while a later known sample lit a second
// cell, breaking one-hot exactly where an operator is least likely to notice.
var (
	poolDynamicStatusMu sync.Mutex
	poolDynamicStatuses = map[string]map[string]struct{}{}

	poolDynamicScanMu    sync.Mutex
	poolDynamicScanCells = map[string]map[[2]string]struct{}{}
)

// SetPoolHealthMetrics publishes one backend-health sample. Every one-hot series
// is rewritten on each sample (current label 1, all others 0) so a recovered
// pool never leaves a stale DEGRADED series firing an alert forever.
func SetPoolHealthMetrics(snapshot *truenas.PoolHealthSnapshot) {
	if snapshot == nil || snapshot.Pool == "" {
		return
	}
	current := strings.ToUpper(strings.TrimSpace(snapshot.Status))
	matched := false
	for _, label := range poolStatusLabels {
		value := 0.0
		if label == current {
			value, matched = 1.0, true
		}
		poolStatus.WithLabelValues(snapshot.Pool, label).Set(value)
	}
	setDynamicPoolStatus(snapshot.Pool, current, matched)

	healthy := 0.0
	if snapshot.Healthy {
		healthy = 1
	}
	poolHealthy.WithLabelValues(snapshot.Pool).Set(healthy)

	currentFunction := strings.ToUpper(strings.TrimSpace(snapshot.ScanFunction))
	if currentFunction == "" {
		currentFunction = poolScanNone
	}
	currentScan := strings.ToUpper(strings.TrimSpace(snapshot.ScanState))
	if currentScan == "" {
		// Idle is a STATE, not the absence of one. Mapping it onto the NONE label
		// keeps the cross-product one-hot for a pool that has never been scanned.
		currentScan = poolScanNone
	}
	scanMatched := false
	for _, function := range poolScanFunctionLabels {
		for _, label := range poolScanStateLabels {
			value := 0.0
			if function == currentFunction && label == currentScan {
				value, scanMatched = 1.0, true
			}
			poolScanState.WithLabelValues(snapshot.Pool, function, label).Set(value)
		}
	}
	// A scan function OR state the driver does not know about still has to be
	// visible, and still has to be retired on the next sample, so route it through
	// the same bookkeeping as the fixed set.
	setDynamicPoolScanState(snapshot.Pool, currentFunction, currentScan, scanMatched)

	poolScanErrors.WithLabelValues(snapshot.Pool).Set(float64(snapshot.ScanErrors))
	poolDiskTempAlerts.WithLabelValues(snapshot.Pool).Set(float64(snapshot.TemperatureAlerts))
}

// setDynamicPoolStatus exports an unrecognized status as 1 and zeroes every
// previously-exported unrecognized status for that pool.
func setDynamicPoolStatus(pool, current string, matched bool) {
	poolDynamicStatusMu.Lock()
	defer poolDynamicStatusMu.Unlock()
	seen := poolDynamicStatuses[pool]
	for label := range seen {
		if label == current && !matched {
			continue
		}
		poolStatus.WithLabelValues(pool, label).Set(0)
	}
	if matched || current == "" {
		return
	}
	poolStatus.WithLabelValues(pool, current).Set(1)
	if seen == nil {
		seen = map[string]struct{}{}
		poolDynamicStatuses[pool] = seen
	}
	seen[current] = struct{}{}
}

// setDynamicPoolScanState exports a {function,state} cell that falls outside the
// fixed cross-product as 1, and RETIRES every such cell this process previously
// exported for the pool by zeroing it.
//
// Retiring matters more here than the initial export does: the fixed
// cross-product loop above cannot zero a cell whose labels it does not enumerate,
// so an unknown function (or unknown state) observed once would otherwise stay at
// 1 for the process's lifetime and sit alongside whatever cell the current sample
// lights — two series at 1, which is precisely the one-hot contract violation the
// design is supposed to rule out.
func setDynamicPoolScanState(pool, function, state string, matched bool) {
	poolDynamicScanMu.Lock()
	defer poolDynamicScanMu.Unlock()
	current := [2]string{function, state}
	seen := poolDynamicScanCells[pool]
	for cell := range seen {
		if cell == current && !matched {
			continue
		}
		poolScanState.WithLabelValues(pool, cell[0], cell[1]).Set(0)
		delete(seen, cell)
	}
	if matched || function == "" || state == "" {
		if len(seen) == 0 {
			delete(poolDynamicScanCells, pool)
		}
		return
	}
	poolScanState.WithLabelValues(pool, function, state).Set(1)
	if seen == nil {
		seen = map[[2]string]struct{}{}
		poolDynamicScanCells[pool] = seen
	}
	seen[current] = struct{}{}
}

// SetPoolHealthStale publishes whether the VolumeCondition currently being
// served is backed by a fresh sample. 1 means it is NOT: either the snapshot
// aged past its TTL (conditions have fallen back to dataset-only — a stale
// DEGRADED must not keep alerting after a real recovery, and a stale ONLINE must
// not mask a real degradation), or a pending flip's confirming sample never
// arrived, so the served verdict is one the driver's own latest raw sample
// already contradicts.
func SetPoolHealthStale(pool string, stale bool) {
	if pool == "" {
		return
	}
	value := 0.0
	if stale {
		value = 1
	}
	poolHealthStale.WithLabelValues(pool).Set(value)
}

// SetPoolHealthFlipPending publishes whether a pool-health transition is waiting
// for its confirming sample. While it is 1 the raw gauges and the per-PVC
// VolumeCondition deliberately disagree: the condition is still the previous
// verdict. This is the operator-visible form of the fan-out hysteresis.
func SetPoolHealthFlipPending(pool string, pending bool) {
	if pool == "" {
		return
	}
	value := 0.0
	if pending {
		value = 1
	}
	poolHealthFlipPending.WithLabelValues(pool).Set(value)
}

// SetPoolCapacityMetrics publishes the latest parent-dataset capacity sample.
// pool is the ZFS pool name and dataset the parent dataset path; available and
// capacity are bytes (capacity = used + available from one pool.dataset.query).
func SetPoolCapacityMetrics(pool, dataset string, available, capacity float64) {
	poolAvailableBytes.WithLabelValues(pool, dataset).Set(available)
	poolCapacityBytes.WithLabelValues(pool, dataset).Set(capacity)
}

func RecordReconcileFailure(phase string) {
	reconcileFailuresTotal.WithLabelValues(phase).Inc()
}

func RecordReplicationJobAborted(reason string) {
	replicationJobsAbortedTotal.WithLabelValues(reason).Inc()
}

func RecordFencingDeferred(reason, protocol string) {
	fencingDeferredTotal.WithLabelValues(reason, protocol).Inc()
}

func RecordFencingStaleDeferred() {
	fencingStaleDeferredTotal.Inc()
}

func RecordFencingTakeover(reason string) {
	fencingTakeoverTotal.WithLabelValues(reason).Inc()
}

func RecordFencingProvenanceOverflow(protocol string) {
	fencingProvenanceOverflowTotal.WithLabelValues(protocol).Inc()
}

func RecordDeleteVolumeOrphanCleanupFailure(protocol string) {
	deleteVolumeOrphanCleanupFailuresTotal.WithLabelValues(protocol).Inc()
}

// Circuit breaker metrics tracking
var (
	cbMetricsMu             sync.Mutex
	lastCBTotalFailures     int64
	lastCBTotalSuccesses    int64
	lastCBTotalCircuitOpens int64
)

// UpdateCircuitBreakerMetrics updates Prometheus metrics from circuit breaker stats.
// This should be called periodically (e.g., during health checks).
func UpdateCircuitBreakerMetrics(stats *truenas.CircuitBreakerStats) {
	if stats == nil {
		return
	}

	cbMetricsMu.Lock()
	defer cbMetricsMu.Unlock()

	// Update state gauge (0 = closed, 1 = open, 2 = half-open)
	circuitBreakerState.Set(float64(stats.State))

	// Update current failures gauge
	circuitBreakerCurrentFailures.Set(float64(stats.Failures))

	// Increment counters by delta (counters can only go up)
	if stats.TotalFailures > lastCBTotalFailures {
		circuitBreakerFailuresTotal.Add(float64(stats.TotalFailures - lastCBTotalFailures))
		lastCBTotalFailures = stats.TotalFailures
	}

	if stats.TotalSuccesses > lastCBTotalSuccesses {
		circuitBreakerSuccessesTotal.Add(float64(stats.TotalSuccesses - lastCBTotalSuccesses))
		lastCBTotalSuccesses = stats.TotalSuccesses
	}

	if stats.TotalCircuitOpens > lastCBTotalCircuitOpens {
		circuitBreakerOpensTotal.Add(float64(stats.TotalCircuitOpens - lastCBTotalCircuitOpens))
		lastCBTotalCircuitOpens = stats.TotalCircuitOpens
	}
}
