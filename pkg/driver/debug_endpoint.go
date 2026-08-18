package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	// Importing net/http/pprof registers its handlers on http.DefaultServeMux
	// as an import side effect. That is harmless here — NOTHING in this binary
	// serves the default mux (the health/metrics server in health.go and the
	// debug server below each build their own http.NewServeMux) — but it is the
	// reason the pprof handler FUNCTIONS are wired explicitly onto a dedicated
	// mux in Start instead of relying on those implicit registrations: the
	// profiling surface must exist only behind the opt-in debug listener, never
	// leak onto whatever mux a future change might serve by default.
	"net/http/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// DebugServer serves the opt-in debug HTTP endpoint: the net/http/pprof
// profiles under /debug/pprof/ and a JSON runtime-state dump at /debug/state.
// It exists for 3am incident triage — "which volume operation is wedged, is
// the backend connected, what does this node think it has mounted" — without
// attaching a debugger to a running CSI pod.
//
// It is DISABLED unless debug.listenAddress is configured (see DebugConfig in
// config.go for the security posture: unauthenticated by design, loopback or a
// guarded port only). It deliberately mirrors HealthServer's lifecycle: bind
// synchronously at startup so an occupied address fails fast, serve on a
// dedicated goroutine, and shut down gracefully from Driver.Stop.
type DebugServer struct {
	driver        *Driver
	server        *http.Server
	listenAddress string

	// startedAt anchors the uptime reported by /debug/state. It is recorded at
	// construction, which Driver.Run performs during startup, so it tracks the
	// driver process closely enough for triage purposes.
	startedAt time.Time

	// mu guards boundAddr, which Start writes and Addr reads. A ":0" listen
	// address only resolves to a concrete port at bind time, so tests (and log
	// readers) need the post-bind address rather than the configured one.
	mu        sync.RWMutex
	boundAddr string
}

// DebugState is the response body of GET /debug/state.
//
// SECURITY INVARIANT: every field below is an EXPLICIT non-secret allowlist.
// No config struct is ever marshalled wholesale, because Config carries the
// TrueNAS API key (and future config may carry more credential material).
// When extending this struct, copy individual fields — never embed Config,
// TrueNASConfig, or any other yaml-decoded struct. TestDebugStateNeverLeaksSecrets
// guards this invariant with a sentinel API key.
type DebugState struct {
	Driver         DebugDriverInfo    `json:"driver"`
	OperationLocks []string           `json:"operation_locks"`
	TrueNAS        DebugTrueNASState  `json:"truenas"`
	Config         DebugConfigSummary `json:"config"`
	NodeMounts     DebugNodeMounts    `json:"node_mounts"`
}

// DebugDriverInfo identifies the process serving the dump.
type DebugDriverInfo struct {
	Name              string  `json:"name"`
	Version           string  `json:"version"`
	NodeID            string  `json:"node_id,omitempty"`
	ControllerRunning bool    `json:"controller_running"`
	NodeRunning       bool    `json:"node_running"`
	Ready             bool    `json:"ready"`
	StartedAt         string  `json:"started_at"`
	UptimeSeconds     float64 `json:"uptime_seconds"`
}

// DebugTrueNASState is the cheap client-side view of the backend connection.
// It reuses CircuitBreakerHealth from health.go so both surfaces report the
// breaker identically.
type DebugTrueNASState struct {
	// ClientConfigured is false on node-only plugins, which never construct a
	// management client (see NewDriver); the remaining fields are then zero.
	ClientConfigured bool                  `json:"client_configured"`
	Connected        bool                  `json:"connected"`
	JobSubscribed    bool                  `json:"job_subscribed"`
	CircuitBreaker   *CircuitBreakerHealth `json:"circuit_breaker,omitempty"`
}

// DebugConfigSummary is the NON-SECRET subset of Config worth seeing during an
// incident. The TrueNAS API key, CHAP secrets, and encryption passphrases are
// deliberately absent — the first lives in Config and is excluded by this
// allowlist; the latter two never enter Config at all (they arrive per-volume
// via CSI secret refs).
type DebugConfigSummary struct {
	DriverName        string   `json:"driver_name"`
	DriverInstanceID  string   `json:"driver_instance_id"`
	TrueNASHost       string   `json:"truenas_host"`
	TrueNASPort       int      `json:"truenas_port"`
	TrueNASProtocol   string   `json:"truenas_protocol"`
	DatasetParentName string   `json:"dataset_parent_name"`
	EnabledProtocols  []string `json:"enabled_protocols"`
	FencingMode       string   `json:"fencing_mode"`
	ReconcileEnabled  bool     `json:"reconcile_enabled"`
	EncryptionEnabled bool     `json:"encryption_enabled"`
}

// DebugMountRecord is the triage view of one node mount-state record: the CSI
// identity and location, without the capability minutiae.
type DebugMountRecord struct {
	VolumeID   string `json:"volume_id"`
	TargetPath string `json:"target_path"`
	Readonly   bool   `json:"readonly"`
}

// DebugNodeMounts summarizes the node plugin's in-memory mount bookkeeping
// (see nodeMountRecord in node.go). Empty on controller-only deployments.
type DebugNodeMounts struct {
	StagedCount    int                `json:"staged_count"`
	PublishedCount int                `json:"published_count"`
	Staged         []DebugMountRecord `json:"staged"`
	Published      []DebugMountRecord `json:"published"`
}

// NewDebugServer creates a debug server, or returns nil when listenAddress is
// empty. Returning nil (rather than a disabled server) makes "no listener at
// all" the enforced default: callers cannot accidentally Start a server that
// was never configured, and Driver.Run's nil check is the entire enablement
// logic.
func NewDebugServer(driver *Driver, listenAddress string) *DebugServer {
	listenAddress = strings.TrimSpace(listenAddress)
	if listenAddress == "" {
		return nil
	}
	return &DebugServer{
		driver:        driver,
		listenAddress: listenAddress,
		startedAt:     time.Now(),
	}
}

// Start starts the debug server.
func (s *DebugServer) Start() error {
	// A DEDICATED mux, never http.DefaultServeMux and never the health/metrics
	// mux: profiling and state dumps must be reachable ONLY through the opt-in
	// debug listener, not through the always-on metrics port that Prometheus
	// (and anything else on the pod network) can already reach.
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/state", s.handleState)

	s.server = &http.Server{
		Addr:    s.listenAddress,
		Handler: mux,
		// ReadHeaderTimeout keeps the health server's defensive posture against
		// slow-header clients, but there is deliberately NO WriteTimeout:
		// /debug/pprof/profile?seconds=N and /debug/pprof/trace stream their
		// response for the requested duration, and the health server's 10s write
		// timeout would silently truncate any longer capture.
		ReadHeaderTimeout: 10 * time.Second,
	}

	klog.Infof("Starting debug server on %s (pprof + /debug/state; unauthenticated, keep loopback or guarded)", s.listenAddress)
	// Bind before returning so an occupied address is a startup failure, not an
	// asynchronous log line after the CSI endpoint has already become ready.
	// This mirrors HealthServer.Start.
	listener, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return fmt.Errorf("bind debug listener %s: %w", s.server.Addr, err)
	}
	s.mu.Lock()
	s.boundAddr = listener.Addr().String()
	s.mu.Unlock()

	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Debug server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully stops the debug server. It mirrors HealthServer.Stop so both
// auxiliary HTTP servers share one lifecycle idiom.
func (s *DebugServer) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Addr returns the bound listen address, which differs from the configured one
// when the config used port 0 (the kernel picks a free port at bind time).
// Empty until Start has bound the listener.
func (s *DebugServer) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.boundAddr
}

// handleState serves GET /debug/state.
func (s *DebugServer) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed; use GET", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	// Indented output: this endpoint is read by humans over kubectl
	// port-forward at 3am, not scraped by machines.
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(s.collectState())
}

// collectState assembles the allowlisted runtime snapshot. Every read here is
// cheap and in-memory: no TrueNAS API call is made, so hitting /debug/state
// during an outage can never add backend load or block on a wedged connection.
func (s *DebugServer) collectState() DebugState {
	d := s.driver

	state := DebugState{
		Driver: DebugDriverInfo{
			Name:              d.name,
			Version:           d.version,
			NodeID:            d.nodeID,
			ControllerRunning: d.runController,
			NodeRunning:       d.runNode,
			Ready:             d.ready.Load(),
			StartedAt:         s.startedAt.UTC().Format(time.RFC3339),
			UptimeSeconds:     time.Since(s.startedAt).Seconds(),
		},
		// Non-nil so an idle driver renders "operation_locks": [] rather than
		// null, keeping the JSON shape stable for jq one-liners.
		OperationLocks: []string{},
	}

	// Currently-held per-volume operation locks (acquireOperationLock in
	// driver.go). A key that stays here across repeated fetches is the
	// signature of a wedged operation — exactly what this endpoint is for.
	d.operationLock.Range(func(key, _ interface{}) bool {
		if lockKey, ok := key.(string); ok {
			state.OperationLocks = append(state.OperationLocks, lockKey)
		}
		return true
	})
	// sync.Map iteration order is unspecified; sort for a diff-stable dump.
	sort.Strings(state.OperationLocks)

	// Backend connection view. IsConnected is read directly instead of via
	// observeTrueNASConnection so a debug fetch never mutates metrics or emits
	// the reconnect/disconnect transition logs that the health path owns.
	state.TrueNAS.ClientConfigured = d.truenasClient != nil
	if d.truenasClient != nil {
		state.TrueNAS.Connected = d.truenasClient.IsConnected()
		state.TrueNAS.JobSubscribed = d.truenasClient.AnyConnectionJobSubscribed()
		if cbStats := d.truenasClient.CircuitBreakerStats(); cbStats != nil {
			state.TrueNAS.CircuitBreaker = &CircuitBreakerHealth{
				State:             cbStats.State.String(),
				CurrentFailures:   cbStats.Failures,
				TotalFailures:     cbStats.TotalFailures,
				TotalSuccesses:    cbStats.TotalSuccesses,
				TotalCircuitOpens: cbStats.TotalCircuitOpens,
			}
		}
	}

	// Field-by-field copy of the non-secret allowlist; see DebugConfigSummary.
	if cfg := d.config; cfg != nil {
		state.Config = DebugConfigSummary{
			DriverName:        cfg.DriverName,
			DriverInstanceID:  cfg.DriverInstanceID,
			TrueNASHost:       cfg.TrueNAS.Host,
			TrueNASPort:       cfg.TrueNAS.Port,
			TrueNASProtocol:   cfg.TrueNAS.Protocol,
			DatasetParentName: cfg.ZFS.DatasetParentName,
			EnabledProtocols:  cfg.enabledShareTypeStrings(),
			FencingMode:       string(cfg.Fencing.Mode),
			ReconcileEnabled:  cfg.Reconcile.Enabled,
			EncryptionEnabled: cfg.Encryption.Enabled,
		}
	}

	state.NodeMounts = s.collectNodeMounts()
	return state
}

// collectNodeMounts snapshots the node plugin's staged/published bookkeeping
// under its own mutex, then sorts outside the lock.
func (s *DebugServer) collectNodeMounts() DebugNodeMounts {
	d := s.driver

	d.nodeMountStateMu.Lock()
	staged := make([]DebugMountRecord, 0, len(d.stagedTargets))
	for _, record := range d.stagedTargets {
		staged = append(staged, DebugMountRecord{
			VolumeID:   record.VolumeID,
			TargetPath: record.TargetPath,
			Readonly:   record.Readonly,
		})
	}
	published := make([]DebugMountRecord, 0, len(d.publishedTargets))
	for _, record := range d.publishedTargets {
		published = append(published, DebugMountRecord{
			VolumeID:   record.VolumeID,
			TargetPath: record.TargetPath,
			Readonly:   record.Readonly,
		})
	}
	d.nodeMountStateMu.Unlock()

	sort.Slice(staged, func(i, j int) bool { return staged[i].TargetPath < staged[j].TargetPath })
	sort.Slice(published, func(i, j int) bool { return published[i].TargetPath < published[j].TargetPath })

	return DebugNodeMounts{
		StagedCount:    len(staged),
		PublishedCount: len(published),
		Staged:         staged,
		Published:      published,
	}
}
