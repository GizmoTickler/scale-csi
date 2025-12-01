package driver

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
		[]string{"operation", "status"},
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

	// Volume metrics
	volumesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "volumes_total",
			Help:      "Total number of volumes by type",
		},
		[]string{"share_type"},
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

	// Snapshot metrics
	snapshotsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "snapshots_total",
			Help:      "Total number of snapshots",
		},
	)
)

// RecordCSIOperation records metrics for a CSI operation
func RecordCSIOperation(operation string, duration float64, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	csiOperationsTotal.WithLabelValues(operation, status).Inc()
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

// SetVolumesTotal sets the total number of volumes for a share type
func SetVolumesTotal(shareType string, count int) {
	volumesTotal.WithLabelValues(shareType).Set(float64(count))
}

// SetSnapshotsTotal sets the total number of snapshots
func SetSnapshotsTotal(count int) {
	snapshotsTotal.Set(float64(count))
}
