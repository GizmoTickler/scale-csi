package driver

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// backendHealthSnapshot is the one immutable generation visible to both CSI
// reads and Prometheus. The metric portion is deep-copied before each swap and
// the CSI portion is never mutated after publication.
type backendHealthSnapshot struct {
	CSISnapshot *truenas.PoolHealthSnapshot
	Metrics     *backendHealthMetricsSnapshot
}

// Backend-health metrics are deliberately exposed by one collector rather than
// by GaugeVec children. A GaugeVec can be updated one child at a time, while a
// Prometheus scrape gathers those children independently. The collector takes
// one pointer to the immutable backendHealthSnapshot and emits every
// backend-health family from that generation.
type backendHealthMetricsSnapshot struct {
	Pools map[string]*backendHealthMetricPool
}

type backendHealthMetricPool struct {
	RawPublished         bool
	Statuses             map[string]float64
	Healthy              float64
	ScanStates           map[[2]string]float64
	ScanErrors           float64
	TemperatureAlerts    float64
	TemperatureSampledAt time.Time
	Stale                float64
	StaleSet             bool
	FlipPending          float64
	FlipPendingSet       bool
	LastSuccess          float64
	LastSuccessSet       bool
}

const (
	backendHealthMetricPoolStatus     = "pool_status"
	backendHealthMetricPoolHealthy    = "pool_healthy"
	backendHealthMetricPoolScanState  = "pool_scan_state"
	backendHealthMetricPoolScanErrors = "pool_scan_errors"
	backendHealthMetricPoolTempAlerts = "pool_disk_temp_alerts"
	backendHealthMetricTempAge        = "pool_disk_temp_alerts_age_seconds"
	backendHealthMetricStale          = "pool_health_stale"
	backendHealthMetricLastSuccess    = "pool_health_last_success_timestamp_seconds"
	backendHealthMetricFlipPending    = "pool_health_flip_pending"
)

var (
	backendHealthPoolStatusDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricPoolStatus),
		"ZFS pool status, one-hot over the status label (1 = current status)",
		[]string{"pool", "status"}, nil,
	)
	backendHealthPoolHealthyDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricPoolHealthy),
		"1 when TrueNAS reports the pool healthy, 0 otherwise", []string{"pool"}, nil,
	)
	backendHealthPoolScanStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricPoolScanState),
		"ZFS pool scrub/resilver state, one-hot over the function and state labels (1 = current)",
		[]string{"pool", "function", "state"}, nil,
	)
	backendHealthPoolScanErrorsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricPoolScanErrors),
		"Errors reported by the pool's most recent scrub/resilver", []string{"pool"}, nil,
	)
	backendHealthPoolTempAlertsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricPoolTempAlerts),
		"Number of the pool's member disks currently raising a temperature alert", []string{"pool"}, nil,
	)
	backendHealthTempAgeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricTempAge),
		"Age in seconds of the most recent successful disk temperature alert sample; absent before the first sample", []string{"pool"}, nil,
	)
	backendHealthStaleDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricStale),
		"1 when the served VolumeCondition is not backed by a fresh sample (snapshot past its TTL, a pending flip whose confirming sample never arrived, or no successful sample since startup)",
		[]string{"pool"}, nil,
	)
	backendHealthLastSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricLastSuccess),
		"Unix timestamp of the most recent successful usable pool health sample (driver-owned; not the scrape time)",
		[]string{"pool"}, nil,
	)
	backendHealthFlipPendingDesc = prometheus.NewDesc(
		prometheus.BuildFQName(metricsNamespace, "", backendHealthMetricFlipPending),
		"1 while a pool-health transition awaits its confirming sample; until the staleness TTL expires the per-PVC VolumeCondition still carries the previous verdict",
		[]string{"pool"}, nil,
	)

	backendHealthState     atomic.Pointer[backendHealthSnapshot]
	backendHealthMetricMu  sync.Mutex
	backendHealthCollector = &backendHealthMetricCollector{}

	// These read-only vector-shaped handles preserve the existing in-package test
	// probes while making accidental per-gauge writes impossible. They are not
	// registered; backendHealthCollector is the sole registered collector.
	poolStatus            = newBackendHealthMetricVec(backendHealthMetricPoolStatus, backendHealthPoolStatusDesc, 2)
	poolHealthy           = newBackendHealthMetricVec(backendHealthMetricPoolHealthy, backendHealthPoolHealthyDesc, 1)
	poolScanState         = newBackendHealthMetricVec(backendHealthMetricPoolScanState, backendHealthPoolScanStateDesc, 3)
	poolScanErrors        = newBackendHealthMetricVec(backendHealthMetricPoolScanErrors, backendHealthPoolScanErrorsDesc, 1)
	poolDiskTempAlerts    = newBackendHealthMetricVec(backendHealthMetricPoolTempAlerts, backendHealthPoolTempAlertsDesc, 1)
	poolHealthStale       = newBackendHealthMetricVec(backendHealthMetricStale, backendHealthStaleDesc, 1)
	poolHealthLastSuccess = newBackendHealthMetricVec(backendHealthMetricLastSuccess, backendHealthLastSuccessDesc, 1)
	poolHealthFlipPending = newBackendHealthMetricVec(backendHealthMetricFlipPending, backendHealthFlipPendingDesc, 1)
)

func init() {
	backendHealthState.Store(&backendHealthSnapshot{
		Metrics: &backendHealthMetricsSnapshot{Pools: map[string]*backendHealthMetricPool{}},
	})
	prometheus.MustRegister(backendHealthCollector)
	for _, name := range []string{
		backendHealthMetricPoolStatus,
		backendHealthMetricPoolHealthy,
		backendHealthMetricPoolScanState,
		backendHealthMetricPoolScanErrors,
		backendHealthMetricPoolTempAlerts,
		backendHealthMetricTempAge,
		backendHealthMetricStale,
		backendHealthMetricLastSuccess,
		backendHealthMetricFlipPending,
	} {
		recordName(metricsNamespace, "", name)
	}
}

type backendHealthMetricCollector struct{}

func (c *backendHealthMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range backendHealthMetricDescs() {
		ch <- desc
	}
}

func (c *backendHealthMetricCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot := backendHealthState.Load()
	if snapshot == nil || snapshot.Metrics == nil {
		return
	}
	c.collectSnapshot(ch, snapshot.Metrics, "")
}

func (c *backendHealthMetricCollector) collectSnapshot(ch chan<- prometheus.Metric, snapshot *backendHealthMetricsSnapshot, family string) {
	pools := make([]string, 0, len(snapshot.Pools))
	for pool := range snapshot.Pools {
		pools = append(pools, pool)
	}
	sort.Strings(pools)
	for _, pool := range pools {
		metricPool := snapshot.Pools[pool]
		if metricPool != nil {
			c.collectPool(ch, pool, *metricPool, family)
		}
	}
}

func (c *backendHealthMetricCollector) collectPool(ch chan<- prometheus.Metric, pool string, metricPool backendHealthMetricPool, family string) {
	if metricPool.RawPublished {
		if family == "" || family == backendHealthMetricPoolStatus {
			for status, value := range metricPool.Statuses {
				ch <- prometheus.MustNewConstMetric(backendHealthPoolStatusDesc, prometheus.GaugeValue, value, pool, status)
			}
		}
		if family == "" || family == backendHealthMetricPoolHealthy {
			ch <- prometheus.MustNewConstMetric(backendHealthPoolHealthyDesc, prometheus.GaugeValue, metricPool.Healthy, pool)
		}
		if family == "" || family == backendHealthMetricPoolScanState {
			for cell, value := range metricPool.ScanStates {
				ch <- prometheus.MustNewConstMetric(backendHealthPoolScanStateDesc, prometheus.GaugeValue, value, pool, cell[0], cell[1])
			}
		}
		if family == "" || family == backendHealthMetricPoolScanErrors {
			ch <- prometheus.MustNewConstMetric(backendHealthPoolScanErrorsDesc, prometheus.GaugeValue, metricPool.ScanErrors, pool)
		}
		if family == "" || family == backendHealthMetricPoolTempAlerts {
			ch <- prometheus.MustNewConstMetric(backendHealthPoolTempAlertsDesc, prometheus.GaugeValue, metricPool.TemperatureAlerts, pool)
		}
		if (family == "" || family == backendHealthMetricTempAge) && !metricPool.TemperatureSampledAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(backendHealthTempAgeDesc, prometheus.GaugeValue, backendHealthObservationAge(metricPool.TemperatureSampledAt), pool)
		}
	}
	if metricPool.StaleSet && (family == "" || family == backendHealthMetricStale) {
		ch <- prometheus.MustNewConstMetric(backendHealthStaleDesc, prometheus.GaugeValue, metricPool.Stale, pool)
	}
	if metricPool.LastSuccessSet && (family == "" || family == backendHealthMetricLastSuccess) {
		ch <- prometheus.MustNewConstMetric(backendHealthLastSuccessDesc, prometheus.GaugeValue, metricPool.LastSuccess, pool)
	}
	if metricPool.FlipPendingSet && (family == "" || family == backendHealthMetricFlipPending) {
		ch <- prometheus.MustNewConstMetric(backendHealthFlipPendingDesc, prometheus.GaugeValue, metricPool.FlipPending, pool)
	}
}

func backendHealthMetricDescs() []*prometheus.Desc {
	return []*prometheus.Desc{
		backendHealthPoolStatusDesc,
		backendHealthPoolHealthyDesc,
		backendHealthPoolScanStateDesc,
		backendHealthPoolScanErrorsDesc,
		backendHealthPoolTempAlertsDesc,
		backendHealthTempAgeDesc,
		backendHealthStaleDesc,
		backendHealthLastSuccessDesc,
		backendHealthFlipPendingDesc,
	}
}

func backendHealthObservationAge(observedAt time.Time) float64 {
	age := time.Since(observedAt).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func cloneBackendHealthMetricsSnapshot(previous *backendHealthMetricsSnapshot) *backendHealthMetricsSnapshot {
	next := &backendHealthMetricsSnapshot{Pools: make(map[string]*backendHealthMetricPool)}
	if previous == nil {
		return next
	}
	for pool, previousPool := range previous.Pools {
		if previousPool == nil {
			continue
		}
		metricPool := *previousPool
		metricPool.Statuses = cloneFloatMap(metricPool.Statuses)
		metricPool.ScanStates = cloneScanStateMap(metricPool.ScanStates)
		next.Pools[pool] = &metricPool
	}
	return next
}

func cloneBackendHealthSnapshot(previous *backendHealthSnapshot) *backendHealthSnapshot {
	next := &backendHealthSnapshot{Metrics: cloneBackendHealthMetricsSnapshot(nil)}
	if previous == nil {
		return next
	}
	next.CSISnapshot = previous.CSISnapshot
	next.Metrics = cloneBackendHealthMetricsSnapshot(previous.Metrics)
	return next
}

func cloneFloatMap(values map[string]float64) map[string]float64 {
	if values == nil {
		return nil
	}
	cloned := make(map[string]float64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneScanStateMap(values map[[2]string]float64) map[[2]string]float64 {
	if values == nil {
		return nil
	}
	cloned := make(map[[2]string]float64, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func updateBackendHealthState(mutator func(*backendHealthSnapshot)) {
	backendHealthMetricMu.Lock()
	defer backendHealthMetricMu.Unlock()
	next := cloneBackendHealthSnapshot(backendHealthState.Load())
	mutator(next)
	backendHealthState.Store(next)
}

func updateBackendHealthMetrics(mutator func(*backendHealthMetricsSnapshot)) {
	updateBackendHealthState(func(state *backendHealthSnapshot) {
		if state.Metrics == nil {
			state.Metrics = cloneBackendHealthMetricsSnapshot(nil)
		}
		mutator(state.Metrics)
	})
}

func ensureBackendHealthMetricPool(snapshot *backendHealthMetricsSnapshot, pool string) *backendHealthMetricPool {
	metricPool, ok := snapshot.Pools[pool]
	if !ok {
		metricPool = &backendHealthMetricPool{}
	}
	if metricPool.Statuses == nil {
		metricPool.Statuses = make(map[string]float64, len(poolStatusLabels))
		for _, label := range poolStatusLabels {
			metricPool.Statuses[label] = 0
		}
	}
	if metricPool.ScanStates == nil {
		metricPool.ScanStates = make(map[[2]string]float64, len(poolScanFunctionLabels)*len(poolScanStateLabels))
		for _, function := range poolScanFunctionLabels {
			for _, state := range poolScanStateLabels {
				metricPool.ScanStates[[2]string{function, state}] = 0
			}
		}
	}
	snapshot.Pools[pool] = metricPool
	return metricPool
}

func applyBackendHealthRawMetric(metricPool *backendHealthMetricPool, snapshot *truenas.PoolHealthSnapshot) {
	metricPool.RawPublished = true
	for status := range metricPool.Statuses {
		metricPool.Statuses[status] = 0
	}
	currentStatus := strings.ToUpper(strings.TrimSpace(snapshot.Status))
	if currentStatus != "" {
		metricPool.Statuses[currentStatus] = 1
	}
	if snapshot.Healthy {
		metricPool.Healthy = 1
	} else {
		metricPool.Healthy = 0
	}
	for cell := range metricPool.ScanStates {
		metricPool.ScanStates[cell] = 0
	}
	currentFunction := strings.ToUpper(strings.TrimSpace(snapshot.ScanFunction))
	if currentFunction == "" {
		currentFunction = poolScanNone
	}
	currentState := strings.ToUpper(strings.TrimSpace(snapshot.ScanState))
	if currentState == "" {
		currentState = poolScanNone
	}
	metricPool.ScanStates[[2]string{currentFunction, currentState}] = 1
	metricPool.ScanErrors = float64(snapshot.ScanErrors)
	metricPool.TemperatureAlerts = float64(snapshot.TemperatureAlerts)
	metricPool.TemperatureSampledAt = snapshot.TemperatureSampledAt
}

// publishBackendHealthSampleState publishes the CSI-facing sample and the
// complete metric state through one immutable generation. The metric snapshot
// is the raw sample; csiSnapshot is the hysteresis-selected sample served by
// CSI.
func publishBackendHealthSampleState(csiSnapshot, metricSnapshot *truenas.PoolHealthSnapshot, pending bool) {
	if csiSnapshot == nil || metricSnapshot == nil || metricSnapshot.Pool == "" {
		return
	}
	updateBackendHealthState(func(state *backendHealthSnapshot) {
		state.CSISnapshot = csiSnapshot
		metricPool := ensureBackendHealthMetricPool(state.Metrics, metricSnapshot.Pool)
		applyBackendHealthRawMetric(metricPool, metricSnapshot)
		metricPool.Stale = 0
		metricPool.StaleSet = true
		if pending {
			metricPool.FlipPending = 1
		} else {
			metricPool.FlipPending = 0
		}
		metricPool.FlipPendingSet = true
		if !metricSnapshot.SampledAt.IsZero() {
			metricPool.LastSuccess = float64(metricSnapshot.SampledAt.UnixNano()) / 1e9
			metricPool.LastSuccessSet = true
		}
	})
}

func publishBackendHealthTemperatureState(snapshot *truenas.PoolHealthSnapshot) {
	if snapshot == nil || snapshot.Pool == "" {
		return
	}
	updateBackendHealthState(func(state *backendHealthSnapshot) {
		state.CSISnapshot = snapshot
		metricPool := ensureBackendHealthMetricPool(state.Metrics, snapshot.Pool)
		if !metricPool.RawPublished {
			return
		}
		metricPool.TemperatureAlerts = float64(snapshot.TemperatureAlerts)
		metricPool.TemperatureSampledAt = snapshot.TemperatureSampledAt
	})
}

func publishBackendHealthColdStartMetrics(pool string) {
	if pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		metricPool.Stale = 1
		metricPool.StaleSet = true
		metricPool.FlipPending = 0
		metricPool.FlipPendingSet = true
	})
}

func publishBackendHealthStaleMetrics(pool string, stale, pending bool) {
	if pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		if metricPool.RawPublished {
			metricPool.Stale = boolMetric(stale)
			metricPool.StaleSet = true
		}
		metricPool.FlipPending = boolMetric(pending)
		metricPool.FlipPendingSet = true
	})
}

func publishBackendHealthStaleOnlyMetrics(pool string, stale bool) {
	if pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		metricPool.Stale = boolMetric(stale)
		metricPool.StaleSet = true
	})
}

func publishBackendHealthTemperatureMetrics(pool string, alerts int, observedAt time.Time) {
	if pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		if !metricPool.RawPublished {
			return
		}
		metricPool.TemperatureAlerts = float64(alerts)
		metricPool.TemperatureSampledAt = observedAt
	})
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// The following compatibility helpers are used by package tests that exercise
// the metric publication contract directly. They still build and atomically
// swap a complete immutable snapshot; they never mutate a Prometheus child.
func SetPoolHealthMetrics(snapshot *truenas.PoolHealthSnapshot) {
	if snapshot == nil || snapshot.Pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, snapshot.Pool)
		applyBackendHealthRawMetric(metricPool, snapshot)
	})
}

func SetPoolHealthStale(pool string, stale bool) {
	publishBackendHealthStaleOnlyMetrics(pool, stale)
}

func SetPoolHealthLastSuccess(pool string, at time.Time) {
	if pool == "" || at.IsZero() {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		metricPool.LastSuccess = float64(at.UnixNano()) / 1e9
		metricPool.LastSuccessSet = true
	})
}

func SetPoolDiskTemperatureAlerts(pool string, alerts int) {
	publishBackendHealthTemperatureMetrics(pool, alerts, time.Now())
}

func SetPoolHealthFlipPending(pool string, pending bool) {
	if pool == "" {
		return
	}
	updateBackendHealthMetrics(func(metrics *backendHealthMetricsSnapshot) {
		metricPool := ensureBackendHealthMetricPool(metrics, pool)
		metricPool.FlipPending = boolMetric(pending)
		metricPool.FlipPendingSet = true
	})
}

type backendHealthMetricVec struct {
	family     string
	desc       *prometheus.Desc
	labelCount int
}

func newBackendHealthMetricVec(family string, desc *prometheus.Desc, labelCount int) *backendHealthMetricVec {
	return &backendHealthMetricVec{family: family, desc: desc, labelCount: labelCount}
}

func (v *backendHealthMetricVec) WithLabelValues(values ...string) prometheus.Gauge {
	if len(values) != v.labelCount {
		panic("wrong number of backend-health metric labels")
	}
	return &backendHealthMetricGauge{vec: v, labels: append([]string(nil), values...)}
}

func (v *backendHealthMetricVec) Describe(ch chan<- *prometheus.Desc) {
	ch <- v.desc
}

func (v *backendHealthMetricVec) Collect(ch chan<- prometheus.Metric) {
	snapshot := backendHealthState.Load()
	if snapshot == nil || snapshot.Metrics == nil {
		return
	}
	backendHealthCollector.collectSnapshot(ch, snapshot.Metrics, v.family)
}

type backendHealthMetricGauge struct {
	vec    *backendHealthMetricVec
	labels []string
}

func (g *backendHealthMetricGauge) Desc() *prometheus.Desc { return g.vec.desc }

func (g *backendHealthMetricGauge) Write(out *dto.Metric) error {
	snapshot := backendHealthState.Load()
	var metrics *backendHealthMetricsSnapshot
	if snapshot != nil {
		metrics = snapshot.Metrics
	}
	value := backendHealthMetricValue(metrics, g.vec.family, g.labels)
	return prometheus.MustNewConstMetric(g.vec.desc, prometheus.GaugeValue, value, g.labels...).Write(out)
}

func (g *backendHealthMetricGauge) Describe(ch chan<- *prometheus.Desc) { ch <- g.vec.desc }

func (g *backendHealthMetricGauge) Collect(ch chan<- prometheus.Metric) {
	snapshot := backendHealthState.Load()
	var metrics *backendHealthMetricsSnapshot
	if snapshot != nil {
		metrics = snapshot.Metrics
	}
	value := backendHealthMetricValue(metrics, g.vec.family, g.labels)
	ch <- prometheus.MustNewConstMetric(g.vec.desc, prometheus.GaugeValue, value, g.labels...)
}

func (g *backendHealthMetricGauge) Set(float64) { panic("backend-health metrics are collector-owned") }
func (g *backendHealthMetricGauge) Inc()        { panic("backend-health metrics are collector-owned") }
func (g *backendHealthMetricGauge) Dec()        { panic("backend-health metrics are collector-owned") }
func (g *backendHealthMetricGauge) Add(float64) { panic("backend-health metrics are collector-owned") }
func (g *backendHealthMetricGauge) Sub(float64) { panic("backend-health metrics are collector-owned") }
func (g *backendHealthMetricGauge) SetToCurrentTime() {
	panic("backend-health metrics are collector-owned")
}

func backendHealthMetricValue(snapshot *backendHealthMetricsSnapshot, family string, labels []string) float64 {
	if snapshot == nil || len(labels) == 0 {
		return 0
	}
	metricPool, ok := snapshot.Pools[labels[0]]
	if !ok {
		return 0
	}
	switch family {
	case backendHealthMetricPoolStatus:
		if len(labels) == 2 {
			return metricPool.Statuses[labels[1]]
		}
	case backendHealthMetricPoolHealthy:
		if metricPool.RawPublished {
			return metricPool.Healthy
		}
	case backendHealthMetricPoolScanState:
		if len(labels) == 3 {
			return metricPool.ScanStates[[2]string{labels[1], labels[2]}]
		}
	case backendHealthMetricPoolScanErrors:
		if metricPool.RawPublished {
			return metricPool.ScanErrors
		}
	case backendHealthMetricPoolTempAlerts:
		if metricPool.RawPublished {
			return metricPool.TemperatureAlerts
		}
	case backendHealthMetricTempAge:
		if metricPool.RawPublished && !metricPool.TemperatureSampledAt.IsZero() {
			return backendHealthObservationAge(metricPool.TemperatureSampledAt)
		}
	case backendHealthMetricStale:
		if metricPool.StaleSet {
			return metricPool.Stale
		}
	case backendHealthMetricLastSuccess:
		if metricPool.LastSuccessSet {
			return metricPool.LastSuccess
		}
	case backendHealthMetricFlipPending:
		if metricPool.FlipPendingSet {
			return metricPool.FlipPending
		}
	}
	return 0
}
