package driver

import (
	"sync"

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

// RecordTrueNASRequest records metrics for a TrueNAS API request
func RecordTrueNASRequest(method string, duration float64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	truenasRequestsTotal.WithLabelValues(method, status).Inc()
	truenasRequestsDuration.WithLabelValues(method).Observe(duration)
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
