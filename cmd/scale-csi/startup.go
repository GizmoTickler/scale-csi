package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

const (
	startupConnectInitialBackoff = time.Second
	startupConnectMaxBackoff     = 30 * time.Second
)

type startupHealthServer struct {
	server *http.Server
}

func startStartupHealthServer(port int, runController, runNode bool) (*startupHealthServer, error) {
	mux := newStartupHealthHandler(runController, runNode)
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, fmt.Errorf("bind startup health listener %s: %w", server.Addr, err)
	}

	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
			klog.Errorf("Startup health server error: %v", serveErr)
		}
	}()
	klog.Infof("Serving startup health checks on port %d while connecting to TrueNAS", port)
	return &startupHealthServer{server: server}, nil
}

func newStartupHealthHandler(runController, runNode bool) http.Handler {
	mux := http.NewServeMux()
	liveness := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
	notReady := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("TrueNAS connecting"))
	}
	mux.HandleFunc("/healthz", liveness)
	mux.HandleFunc("/livez", liveness)
	mux.HandleFunc("/readyz", notReady)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"ready":false,"truenas_connected":false,"controller_running":%t,"node_running":%t,"error":"connecting to TrueNAS"}`+"\n", runController, runNode)
	})
	mux.HandleFunc("/metrics", notReady)
	return mux
}

func (s *startupHealthServer) Stop(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func createDriverWithStartupRetry(
	timeout time.Duration,
	create func() (*driver.Driver, error),
	shouldRetry func(error) bool,
) (*driver.Driver, error) {
	return createDriverWithStartupRetryClock(timeout, create, shouldRetry, time.Now, time.Sleep)
}

func createDriverWithStartupRetryClock(
	timeout time.Duration,
	create func() (*driver.Driver, error),
	shouldRetry func(error) bool,
	now func() time.Time,
	sleep func(time.Duration),
) (*driver.Driver, error) {
	if timeout == 0 {
		return create()
	}

	deadline := now().Add(timeout)
	backoff := startupConnectInitialBackoff
	attempt := 0
	for {
		attempt++
		drv, err := create()
		if err == nil {
			if attempt > 1 {
				klog.Infof("Connected to TrueNAS after %d startup attempts", attempt)
			}
			return drv, nil
		}
		if !shouldRetry(err) {
			return nil, err
		}

		remaining := deadline.Sub(now())
		if remaining <= 0 {
			return nil, err
		}
		delay := min(backoff, remaining)
		klog.Warningf("Failed to create driver (attempt %d); retrying in %s: %v", attempt, delay, err)
		sleep(delay)
		if !now().Before(deadline) {
			return nil, err
		}
		backoff = min(backoff*2, startupConnectMaxBackoff)
	}
}
