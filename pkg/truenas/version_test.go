package truenas

import (
	"context"
	"testing"
)

func TestParseTrueNASVersion(t *testing.T) {
	tests := []struct {
		name          string
		version       string
		expectMajor   int
		expectMinor   int
		expectPatch   int
	}{
		{
			name:        "Full TrueNAS SCALE version",
			version:     "TrueNAS-SCALE-25.10.0",
			expectMajor: 25,
			expectMinor: 10,
			expectPatch: 0,
		},
		{
			name:        "TrueNAS prefix without SCALE",
			version:     "TrueNAS-25.10.0",
			expectMajor: 25,
			expectMinor: 10,
			expectPatch: 0,
		},
		{
			name:        "Version only",
			version:     "25.10.0",
			expectMajor: 25,
			expectMinor: 10,
			expectPatch: 0,
		},
		{
			name:        "Version without patch",
			version:     "25.10",
			expectMajor: 25,
			expectMinor: 10,
			expectPatch: 0,
		},
		{
			name:        "Version with suffix",
			version:     "TrueNAS-SCALE-25.10.0-MASTER",
			expectMajor: 25,
			expectMinor: 10,
			expectPatch: 0,
		},
		{
			name:        "Older version",
			version:     "TrueNAS-SCALE-24.04.0",
			expectMajor: 24,
			expectMinor: 4,
			expectPatch: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &SystemInfo{}
			parseTrueNASVersion(tt.version, info)

			if info.VersionMajor != tt.expectMajor {
				t.Errorf("VersionMajor = %d, want %d", info.VersionMajor, tt.expectMajor)
			}
			if info.VersionMinor != tt.expectMinor {
				t.Errorf("VersionMinor = %d, want %d", info.VersionMinor, tt.expectMinor)
			}
			if info.VersionPatch != tt.expectPatch {
				t.Errorf("VersionPatch = %d, want %d", info.VersionPatch, tt.expectPatch)
			}
		})
	}
}

func TestParseSystemInfo(t *testing.T) {
	data := map[string]interface{}{
		"version":        "TrueNAS-SCALE-25.10.0",
		"hostname":       "truenas-test",
		"uptime_seconds": float64(123456),
		"system_product": "TrueNAS",
		"system_serial":  "test-serial",
	}

	info, err := parseSystemInfo(data)
	if err != nil {
		t.Fatalf("parseSystemInfo() error = %v", err)
	}

	if info.Version != "TrueNAS-SCALE-25.10.0" {
		t.Errorf("Version = %s, want TrueNAS-SCALE-25.10.0", info.Version)
	}
	if info.VersionMajor != 25 {
		t.Errorf("VersionMajor = %d, want 25", info.VersionMajor)
	}
	if info.VersionMinor != 10 {
		t.Errorf("VersionMinor = %d, want 10", info.VersionMinor)
	}
	if info.Hostname != "truenas-test" {
		t.Errorf("Hostname = %s, want truenas-test", info.Hostname)
	}
	if info.UptimeSeconds != 123456 {
		t.Errorf("UptimeSeconds = %d, want 123456", info.UptimeSeconds)
	}
}

func TestCheckNVMeoFSupport(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		expectError bool
	}{
		{
			name:        "Supported version 25.10",
			version:     "TrueNAS-SCALE-25.10.0",
			expectError: false,
		},
		{
			name:        "Supported version 26.0",
			version:     "TrueNAS-SCALE-26.0.0",
			expectError: false,
		},
		{
			name:        "Unsupported version 24.10",
			version:     "TrueNAS-SCALE-24.10.0",
			expectError: true,
		},
		{
			name:        "Unsupported version 25.04",
			version:     "TrueNAS-SCALE-25.04.0",
			expectError: true,
		},
		{
			name:        "Unsupported version 23.10",
			version:     "TrueNAS-SCALE-23.10.0",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &SystemInfo{Version: tt.version}
			parseTrueNASVersion(tt.version, info)

			// Create a mock client with the specific version
			client := &Client{
				versionCache: info,
			}

			err := client.CheckNVMeoFSupport(context.Background())
			if tt.expectError && err == nil {
				t.Errorf("CheckNVMeoFSupport() expected error for version %s, got nil", tt.version)
			}
			if !tt.expectError && err != nil {
				t.Errorf("CheckNVMeoFSupport() unexpected error for version %s: %v", tt.version, err)
			}
		})
	}
}
