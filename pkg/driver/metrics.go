package driver

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	metricsNamespace = "scale_csi"
)

var (
	// CSI operation metrics
	csiOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "operations_total",
			Help:      "Total number of CSI operations",
		},
		[]string{"operation", "status", "code"},
	)

	csiOperationsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "operations_duration_seconds",
			Help:      "Duration of CSI operations in seconds",
			Buckets:   []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{"operation"},
	)

	// TrueNAS API metrics
	truenasRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_requests_total",
			Help:      "Total number of TrueNAS API requests",
		},
		[]string{"method", "status"},
	)

	truenasRequestsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_requests_duration_seconds",
			Help:      "Duration of TrueNAS API requests in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"method"},
	)

	// Connection metrics
	truenasConnectionStatus = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_connection_status",
			Help:      "TrueNAS connection status (1 = connected, 0 = disconnected)",
		},
	)

	truenasConnectionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "truenas_connections_active",
			Help:      "Number of active TrueNAS WebSocket connections",
		},
	)

	// iSCSI metrics
	iscsiSessionsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "iscsi_sessions_total",
			Help:      "Total number of active iSCSI sessions on this node",
		},
	)

	nvmeSessionsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "nvme_sessions_total",
			Help:      "Total number of active NVMe-oF sessions on this node",
		},
	)

	nodeConnectTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "node_connect_total",
			Help:      "Total number of node transport connection attempts",
		},
		[]string{"transport", "result"},
	)

	gcSessionsDisconnectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "gc_sessions_disconnected_total",
			Help:      "Total number of orphaned sessions disconnected by session garbage collection",
		},
		[]string{"transport"},
	)

	// Controller orphan reconcile metrics.
	orphanVolumes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_volumes",
			Help:      "Number of CSI-managed TrueNAS volumes without a live Kubernetes PV handle",
		},
	)

	orphanSnapshots = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_snapshots",
			Help:      "Number of CSI-managed TrueNAS snapshots without a live Kubernetes VolumeSnapshotContent handle",
		},
	)

	spentRestoreSnapshots = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "spent_restore_snapshots",
			Help:      "Number of spent VolSync restore-destination snapshots whose source PVC is no longer Bound",
		},
	)

	orphanVolumesBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "orphan_volumes_bytes",
			Help:      "Reported used bytes held by detected orphan CSI-managed TrueNAS volumes",
		},
	)

	orphanSnapshotsBytes = promauto.NewGauge(
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
	tombstoneSnapshots = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "tombstone_snapshots",
			Help:      "Number of driver-tombstoned deferred-delete snapshots awaiting reap (ledger-proven, age-eligible)",
		},
	)

	tombstoneSnapshotsBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "tombstone_snapshots_bytes",
			Help:      "Reported used bytes held by driver-tombstoned deferred-delete snapshots awaiting reap",
		},
	)

	remnantVolumes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "remnant_volumes",
			Help:      "Number of marker-proven unstamped remnant datasets awaiting guarded reap (interrupted creates with no possible retry)",
		},
	)

	// Pool-capacity gauges (E4). Populated only when capacity.gaugeEnabled by a
	// controller-only poll loop; cardinality is fixed at one series per gauge for
	// this single-backend driver, labeled {pool,dataset}. pool_capacity_bytes is
	// used+available from the same pool.dataset.query row that feeds
	// pool_available_bytes, so the near-full ratio is internally consistent.
	poolAvailableBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_available_bytes",
			Help:      "ZFS-computed available bytes on the parent dataset (parity/quota/reservation-aware)",
		},
		[]string{"pool", "dataset"},
	)

	poolCapacityBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "pool_capacity_bytes",
			Help:      "Total usable bytes (used + available) on the parent dataset",
		},
		[]string{"pool", "dataset"},
	)

	reconcileLastSuccessTimestamp = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_last_success_timestamp_seconds",
			Help:      "Unix timestamp of the most recent completed controller reconcile pass",
		},
	)

	reconcileFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "reconcile_failures_total",
			Help:      "Total reconcile failures and isolated object skips by phase",
		},
		[]string{"phase"},
	)

	replicationJobsAbortedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "replication_jobs_aborted_total",
			Help:      "Total driver-owned one-time replication jobs successfully aborted",
		},
		[]string{"reason"},
	)

	fencingDeferredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "fencing_deferred_total",
			Help:      "Total backend fencing operations deferred to preserve upgrade compatibility",
		},
		[]string{"reason", "protocol"},
	)

	fencingStaleDeferredTotal = promauto.NewCounter(
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
	fencingTakeoverTotal = promauto.NewCounterVec(
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
	fencingProvenanceOverflowTotal = promauto.NewCounterVec(
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
	deleteVolumeOrphanCleanupFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "delete_volume_orphan_cleanup_failures_total",
			Help:      "Total DeleteVolume calls with an absent dataset whose residual share cleanup failed best-effort",
		},
		[]string{"protocol"},
	)

	// Circuit breaker metrics
	circuitBreakerState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0 = closed, 1 = open, 2 = half-open)",
		},
	)

	circuitBreakerFailuresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_failures_total",
			Help:      "Total number of failures recorded by the circuit breaker",
		},
	)

	circuitBreakerSuccessesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_successes_total",
			Help:      "Total number of successes recorded by the circuit breaker",
		},
	)

	circuitBreakerOpensTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "circuit_breaker_opens_total",
			Help:      "Total number of times the circuit breaker has opened",
		},
	)

	circuitBreakerCurrentFailures = promauto.NewGauge(
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
}

func RecordReconcileSuccess(at time.Time) {
	reconcileLastSuccessTimestamp.Set(float64(at.Unix()))
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
