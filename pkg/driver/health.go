package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

// HealthServer provides HTTP endpoints for health checks and metrics
type HealthServer struct {
	driver     *Driver
	server     *http.Server
	port       int
	mu         sync.RWMutex
	lastCheck  time.Time
	lastStatus HealthStatus
}

// HealthStatus represents the current health status of the driver
type HealthStatus struct {
	Ready              bool                  `json:"ready"`
	TrueNASConnected   bool                  `json:"truenas_connected"`
	ControllerRunning  bool                  `json:"controller_running"`
	NodeRunning        bool                  `json:"node_running"`
	LastTrueNASCheck   string                `json:"last_truenas_check,omitempty"`
	Error              string                `json:"error,omitempty"`
	CircuitBreaker     *CircuitBreakerHealth `json:"circuit_breaker,omitempty"`
}

// CircuitBreakerHealth represents circuit breaker health information
type CircuitBreakerHealth struct {
	State             string `json:"state"`
	CurrentFailures   int    `json:"current_failures"`
	TotalFailures     int64  `json:"total_failures"`
	TotalSuccesses    int64  `json:"total_successes"`
	TotalCircuitOpens int64  `json:"total_circuit_opens"`
}

// NewHealthServer creates a new health server
func NewHealthServer(driver *Driver, port int) *HealthServer {
	return &HealthServer{
		driver: driver,
		port:   port,
	}
}

// Start starts the health server
func (h *HealthServer) Start() error {
	mux := http.NewServeMux()

	// Liveness probe - checks if the process is alive
	mux.HandleFunc("/healthz", h.handleLiveness)
	mux.HandleFunc("/livez", h.handleLiveness)

	// Readiness probe - checks if the driver is ready to serve traffic
	mux.HandleFunc("/readyz", h.handleReadiness)

	// Detailed health status
	mux.HandleFunc("/health", h.handleHealth)

	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	h.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", h.port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	klog.Infof("Starting health server on port %d", h.port)

	go func() {
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Health server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the health server
func (h *HealthServer) Stop(ctx context.Context) error {
	if h.server != nil {
		return h.server.Shutdown(ctx)
	}
	return nil
}

// handleLiveness handles liveness probe requests
// Returns 200 if the process is alive (even if not fully ready)
func (h *HealthServer) handleLiveness(w http.ResponseWriter, r *http.Request) {
	// Liveness just checks if the process is running
	// We're alive if we can respond to this request
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// handleReadiness handles readiness probe requests
// Returns 200 only if the driver is ready to serve traffic
func (h *HealthServer) handleReadiness(w http.ResponseWriter, r *http.Request) {
	status := h.checkHealth()

	w.Header().Set("Content-Type", "text/plain")

	if status.Ready && status.TrueNASConnected {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		msg := "Not Ready"
		if !status.TrueNASConnected {
			msg = "TrueNAS disconnected"
		}
		_, _ = w.Write([]byte(msg))
	}
}

// handleHealth handles detailed health status requests
func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := h.checkHealth()

	w.Header().Set("Content-Type", "application/json")

	if status.Ready && status.TrueNASConnected {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	_ = json.NewEncoder(w).Encode(status)
}

// checkHealth checks the current health status
func (h *HealthServer) checkHealth() HealthStatus {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Cache health check results for 5 seconds to avoid hammering TrueNAS
	if time.Since(h.lastCheck) < 5*time.Second {
		return h.lastStatus
	}

	status := HealthStatus{
		Ready:             h.driver.ready.Load(),
		ControllerRunning: h.driver.runController,
		NodeRunning:       h.driver.runNode,
	}

	// Check TrueNAS connection
	if h.driver.truenasClient != nil {
		connected := h.driver.truenasClient.IsConnected()
		status.TrueNASConnected = connected
		status.LastTrueNASCheck = time.Now().Format(time.RFC3339)

		// Update metrics
		SetTrueNASConnectionStatus(connected)

		// Update circuit breaker metrics and health status
		if cbStats := h.driver.truenasClient.CircuitBreakerStats(); cbStats != nil {
			UpdateCircuitBreakerMetrics(cbStats)
			status.CircuitBreaker = &CircuitBreakerHealth{
				State:             cbStats.State.String(),
				CurrentFailures:   cbStats.Failures,
				TotalFailures:     cbStats.TotalFailures,
				TotalSuccesses:    cbStats.TotalSuccesses,
				TotalCircuitOpens: cbStats.TotalCircuitOpens,
			}
		}
	}

	h.lastCheck = time.Now()
	h.lastStatus = status

	return status
}
