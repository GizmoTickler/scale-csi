package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

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
