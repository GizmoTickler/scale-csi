package main

import (
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

func TestCreateDriverWithStartupRetryUsesExponentialBackoff(t *testing.T) {
	now := time.Unix(0, 0)
	var delays []time.Duration
	attempts := 0
	wantDriver := &driver.Driver{}

	gotDriver, err := createDriverWithStartupRetryClock(
		time.Minute,
		func() (*driver.Driver, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("temporary DNS failure")
			}
			return wantDriver, nil
		},
		func(error) bool { return true },
		func() time.Time { return now },
		func(delay time.Duration) {
			delays = append(delays, delay)
			now = now.Add(delay)
		},
	)

	if err != nil {
		t.Fatalf("create driver: %v", err)
	}
	if gotDriver != wantDriver {
		t.Fatalf("driver = %p, want %p", gotDriver, wantDriver)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	wantDelays := []time.Duration{time.Second, 2 * time.Second}
	if len(delays) != len(wantDelays) {
		t.Fatalf("delays = %v, want %v", delays, wantDelays)
	}
	for i := range wantDelays {
		if delays[i] != wantDelays[i] {
			t.Fatalf("delay %d = %s, want %s", i, delays[i], wantDelays[i])
		}
	}
}

func TestCreateDriverWithStartupRetryStopsAtWindow(t *testing.T) {
	now := time.Unix(0, 0)
	var delays []time.Duration
	attempts := 0
	wantErr := errors.New("connection refused")

	drv, err := createDriverWithStartupRetryClock(
		2500*time.Millisecond,
		func() (*driver.Driver, error) {
			attempts++
			return nil, wantErr
		},
		func(error) bool { return true },
		func() time.Time { return now },
		func(delay time.Duration) {
			delays = append(delays, delay)
			now = now.Add(delay)
		},
	)

	if drv != nil || !errors.Is(err, wantErr) {
		t.Fatalf("driver=%v error=%v, want nil and %v", drv, err, wantErr)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	wantDelays := []time.Duration{time.Second, 1500 * time.Millisecond}
	if len(delays) != len(wantDelays) || delays[0] != wantDelays[0] || delays[1] != wantDelays[1] {
		t.Fatalf("delays = %v, want %v", delays, wantDelays)
	}
}

func TestCreateDriverWithStartupRetryZeroFailsFast(t *testing.T) {
	attempts := 0
	wantErr := errors.New("lookup failed")
	drv, err := createDriverWithStartupRetryClock(
		0,
		func() (*driver.Driver, error) {
			attempts++
			return nil, wantErr
		},
		func(error) bool { t.Fatal("retry classifier called in fail-fast mode"); return false },
		func() time.Time { t.Fatal("clock called in fail-fast mode"); return time.Time{} },
		func(time.Duration) { t.Fatal("sleep called in fail-fast mode") },
	)

	if drv != nil || !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("driver=%v error=%v attempts=%d, want nil, %v, 1", drv, err, attempts, wantErr)
	}
}

func TestCreateDriverWithStartupRetryDoesNotRetryPermanentError(t *testing.T) {
	attempts := 0
	wantErr := errors.New("truenas.apiKey is required")
	drv, err := createDriverWithStartupRetryClock(
		time.Minute,
		func() (*driver.Driver, error) {
			attempts++
			return nil, wantErr
		},
		func(error) bool { return false },
		func() time.Time { return time.Unix(0, 0) },
		func(time.Duration) { t.Fatal("sleep called for permanent startup error") },
	)

	if drv != nil || !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("driver=%v error=%v attempts=%d, want nil, %v, 1", drv, err, attempts, wantErr)
	}
}

func TestStartupHealthReportsLiveButNotReady(t *testing.T) {
	handler := newStartupHealthHandler(true, false)

	for _, path := range []string{"/healthz", "/livez"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Errorf("%s status = %d, want %d", path, recorder.Code, http.StatusOK)
		}
	}

	readiness := httptest.NewRecorder()
	handler.ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readiness.Code != http.StatusServiceUnavailable || readiness.Body.String() != "TrueNAS connecting" {
		t.Errorf("readyz = (%d, %q), want (%d, %q)", readiness.Code, readiness.Body.String(), http.StatusServiceUnavailable, "TrueNAS connecting")
	}

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, want %d", health.Code, http.StatusServiceUnavailable)
	}
	if got := health.Body.String(); got != "{\"ready\":false,\"truenas_connected\":false,\"controller_running\":true,\"node_running\":false,\"error\":\"connecting to TrueNAS\"}\n" {
		t.Fatalf("health body = %q", got)
	}
}

func TestSetMemoryLimitFromCgroupV2(t *testing.T) {
	limitPath := writeMemoryLimitFile(t, "104857600")
	var got int64

	limit, configured, err := setMemoryLimitFromCgroup(
		limitPath,
		filepath.Join(t.TempDir(), "missing-v1"),
		unsetEnvironment,
		func(value int64) int64 {
			got = value
			return 0
		},
	)

	if err != nil {
		t.Fatalf("set memory limit: %v", err)
	}
	if !configured {
		t.Fatal("memory limit was not configured")
	}
	const want = int64(90 * 1024 * 1024)
	if limit != want || got != want {
		t.Fatalf("memory limit = %d, setter received %d, want %d", limit, got, want)
	}
}

func TestSetMemoryLimitFallsBackToCgroupV1(t *testing.T) {
	v1Path := writeMemoryLimitFile(t, "200")
	var got int64

	limit, configured, err := setMemoryLimitFromCgroup(
		filepath.Join(t.TempDir(), "missing-v2"),
		v1Path,
		unsetEnvironment,
		func(value int64) int64 {
			got = value
			return 0
		},
	)

	if err != nil {
		t.Fatalf("set memory limit: %v", err)
	}
	if !configured || limit != 180 || got != 180 {
		t.Fatalf("configured=%v, limit=%d, setter received %d; want true, 180, 180", configured, limit, got)
	}
}

func TestSetMemoryLimitHonorsExplicitEnvironment(t *testing.T) {
	limitPath := writeMemoryLimitFile(t, "104857600")
	setterCalled := false

	_, configured, err := setMemoryLimitFromCgroup(
		limitPath,
		filepath.Join(t.TempDir(), "missing-v1"),
		func(key string) (string, bool) {
			if key != "GOMEMLIMIT" {
				t.Fatalf("lookup key = %q, want GOMEMLIMIT", key)
			}
			return "64MiB", true
		},
		func(int64) int64 {
			setterCalled = true
			return 0
		},
	)

	if err != nil {
		t.Fatalf("set memory limit: %v", err)
	}
	if configured || setterCalled {
		t.Fatalf("configured=%v, setterCalled=%v; explicit GOMEMLIMIT must win", configured, setterCalled)
	}
}

func TestSetMemoryLimitIgnoresUnlimitedValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
		useV1 bool
	}{
		{name: "cgroup v2 max", value: "max"},
		{name: "cgroup v1 sentinel", value: "9223372036854771712", useV1: true},
		{name: "cgroup v1 max uint", value: "18446744073709551615", useV1: true},
		{name: "zero", value: "0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limitPath := writeMemoryLimitFile(t, test.value)
			v2Path, v1Path := limitPath, filepath.Join(t.TempDir(), "missing-v1")
			if test.useV1 {
				v2Path, v1Path = filepath.Join(t.TempDir(), "missing-v2"), limitPath
			}
			setterCalled := false

			_, configured, err := setMemoryLimitFromCgroup(
				v2Path,
				v1Path,
				unsetEnvironment,
				func(int64) int64 {
					setterCalled = true
					return math.MinInt64
				},
			)

			if err != nil {
				t.Fatalf("set memory limit: %v", err)
			}
			if configured || setterCalled {
				t.Fatalf("configured=%v, setterCalled=%v; unlimited value must be ignored", configured, setterCalled)
			}
		})
	}
}

func writeMemoryLimitFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.limit")
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("write fake cgroup file: %v", err)
	}
	return path
}

func unsetEnvironment(string) (string, bool) {
	return "", false
}
