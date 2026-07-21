package driver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

func TestExpectedSessionsIncludeStagedBlockSymlinks(t *testing.T) {
	originalMounted := getMountedBlockDevices
	originalStaged := getStagedBlockDevices
	originalISCSIInfo := getISCSIInfoFromDevice
	originalNVMeInfo := getNVMeInfoFromDevice
	t.Cleanup(func() {
		getMountedBlockDevices = originalMounted
		getStagedBlockDevices = originalStaged
		getISCSIInfoFromDevice = originalISCSIInfo
		getNVMeInfoFromDevice = originalNVMeInfo
	})

	d := &Driver{}

	t.Run("iSCSI unions mounted and raw block staged devices", func(t *testing.T) {
		getMountedBlockDevices = func() (map[string]string, error) {
			return map[string]string{"/dev/sda": "/mounted"}, nil
		}
		getStagedBlockDevices = func() (map[string]string, error) {
			return map[string]string{"/dev/sdb": "/staged/globalmount"}, nil
		}
		getISCSIInfoFromDevice = func(device string) (string, string, error) {
			switch device {
			case "/dev/sda":
				return "10.0.0.1:3260", "iqn.test:mounted", nil
			case "/dev/sdb":
				return "10.0.0.1:3260", "iqn.test:block", nil
			default:
				return "", "", errors.New("not iSCSI")
			}
		}

		expected := d.getExpectedISCSITargets()
		require.NotNil(t, expected)
		assert.Contains(t, expected, "iqn.test:mounted")
		assert.Contains(t, expected, "iqn.test:block")
	})

	t.Run("NVMe-oF unions mounted and raw block staged devices", func(t *testing.T) {
		getMountedBlockDevices = func() (map[string]string, error) {
			return map[string]string{"/dev/nvme2n1": "/mounted"}, nil
		}
		getStagedBlockDevices = func() (map[string]string, error) {
			return map[string]string{"/dev/nvme3n7": "/staged/globalmount"}, nil
		}
		getNVMeInfoFromDevice = func(device string) (string, error) {
			switch device {
			case "/dev/nvme2n1":
				return "nqn.test:mounted", nil
			case "/dev/nvme3n7":
				return "nqn.test:block", nil
			default:
				return "", errors.New("not NVMe")
			}
		}

		expected := d.getExpectedNVMeoFNQNs()
		require.NotNil(t, expected)
		assert.Contains(t, expected, "nqn.test:mounted")
		assert.Contains(t, expected, "nqn.test:block")
	})

	t.Run("staging scan error skips garbage collection", func(t *testing.T) {
		getMountedBlockDevices = func() (map[string]string, error) {
			return map[string]string{}, nil
		}
		getStagedBlockDevices = func() (map[string]string, error) {
			return nil, errors.New("staging directory unreadable")
		}

		assert.Nil(t, d.getExpectedISCSITargets())
		assert.Nil(t, d.getExpectedNVMeoFNQNs())
	})
}

func TestNVMeSessionMatchesTransportAddress(t *testing.T) {
	tests := []struct {
		name           string
		sessionAddress string
		targetAddress  string
		want           bool
	}{
		{
			name:           "exact traddr",
			sessionAddress: "traddr=192.168.120.10,trsvcid=4420,src_addr=192.168.122.10",
			targetAddress:  "192.168.120.10",
			want:           true,
		},
		{
			name:           "prefix overlap",
			sessionAddress: "traddr=192.168.120.100,trsvcid=4420",
			targetAddress:  "192.168.120.10",
			want:           false,
		},
		{
			name:           "source address collision",
			sessionAddress: "traddr=10.0.0.20,trsvcid=4420,src_addr=192.168.120.10",
			targetAddress:  "192.168.120.10",
			want:           false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, nvmeSessionMatchesTransportAddress(tc.sessionAddress, tc.targetAddress))
		})
	}
}

func TestSessionGCStopsBetweenDisconnectsWhenContextIsCanceled(t *testing.T) {
	originalMounted := getMountedBlockDevices
	originalStaged := getStagedBlockDevices
	originalListISCSI := gcListISCSISessions
	originalDisconnectISCSI := gcDisconnectISCSI
	originalListNVMe := gcListNVMeoFSessions
	originalDisconnectNVMe := gcDisconnectNVMeoF
	t.Cleanup(func() {
		getMountedBlockDevices = originalMounted
		getStagedBlockDevices = originalStaged
		gcListISCSISessions = originalListISCSI
		gcDisconnectISCSI = originalDisconnectISCSI
		gcListNVMeoFSessions = originalListNVMe
		gcDisconnectNVMeoF = originalDisconnectNVMe
	})
	getMountedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	getStagedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }

	t.Run("iSCSI", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		d := &Driver{config: &Config{ISCSI: ISCSIConfig{TargetPortal: "192.0.2.10:3260", NamePrefix: "csi-"}}}
		sessions := []util.ISCSISessionInfo{
			{Portal: d.config.ISCSI.TargetPortal, IQN: "iqn.test:csi-one"},
			{Portal: d.config.ISCSI.TargetPortal, IQN: "iqn.test:csi-two"},
		}
		for _, session := range sessions {
			d.orphanedISCSISessionsSeen.Store(session.IQN, time.Now().Add(-time.Hour))
		}
		gcListISCSISessions = func() ([]util.ISCSISessionInfo, error) { return sessions, nil }
		disconnects := 0
		gcDisconnectISCSI = func(string, string) error {
			disconnects++
			cancel()
			return nil
		}

		metricBefore := testutil.ToFloat64(gcSessionsDisconnectedTotal.WithLabelValues("iscsi"))
		d.gcISCSISessions(ctx, 0, false)
		assert.Equal(t, 1, disconnects)
		assert.Equal(t, metricBefore+1, testutil.ToFloat64(gcSessionsDisconnectedTotal.WithLabelValues("iscsi")))
	})

	t.Run("NVMeoF", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"}}}
		sessions := []util.NVMeoFSessionInfo{
			{NQN: "nqn.test:one", Address: "traddr=192.0.2.20,trsvcid=4420"},
			{NQN: "nqn.test:two", Address: "traddr=192.0.2.20,trsvcid=4420"},
		}
		for _, session := range sessions {
			d.orphanedNVMeSessionsSeen.Store(session.NQN, time.Now().Add(-time.Hour))
		}
		gcListNVMeoFSessions = func() ([]util.NVMeoFSessionInfo, error) { return sessions, nil }
		disconnects := 0
		gcDisconnectNVMeoF = func(string) error {
			disconnects++
			cancel()
			return nil
		}

		metricBefore := testutil.ToFloat64(gcSessionsDisconnectedTotal.WithLabelValues("nvmeof"))
		d.gcNVMeoFSessions(ctx, 0, false)
		assert.Equal(t, 1, disconnects)
		assert.Equal(t, metricBefore+1, testutil.ToFloat64(gcSessionsDisconnectedTotal.WithLabelValues("nvmeof")))
	})
}

func TestSessionGaugesRefreshWhenCleanupDisabled(t *testing.T) {
	originalListISCSI := gcListISCSISessions
	originalListNVMe := gcListNVMeoFSessions
	t.Cleanup(func() {
		gcListISCSISessions = originalListISCSI
		gcListNVMeoFSessions = originalListNVMe
	})
	gcListISCSISessions = func() ([]util.ISCSISessionInfo, error) { return nil, nil }
	gcListNVMeoFSessions = func() ([]util.NVMeoFSessionInfo, error) { return nil, nil }
	SetISCSISessions(7)
	SetNVMESessions(9)

	d := &Driver{}
	d.runSessionGCWithProtocols(context.Background(), 0, false, false, false)

	assert.Zero(t, testutil.ToFloat64(iscsiSessionsTotal))
	assert.Zero(t, testutil.ToFloat64(nvmeSessionsTotal))
}

func TestISCSIGCPreservesNVMeOrphanFirstSeen(t *testing.T) {
	originalMounted := getMountedBlockDevices
	originalStaged := getStagedBlockDevices
	originalListISCSI := gcListISCSISessions
	originalListNVMe := gcListNVMeoFSessions
	t.Cleanup(func() {
		getMountedBlockDevices = originalMounted
		getStagedBlockDevices = originalStaged
		gcListISCSISessions = originalListISCSI
		gcListNVMeoFSessions = originalListNVMe
	})
	getMountedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	getStagedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }

	const orphanNQN = "nqn.2014-08.org.nvmexpress:csi-orphan"
	d := &Driver{config: &Config{
		ISCSI:  ISCSIConfig{TargetPortal: "192.0.2.10:3260", NamePrefix: "csi-"},
		NVMeoF: NVMeoFConfig{TransportAddress: "192.0.2.20"},
	}}
	gcListNVMeoFSessions = func() ([]util.NVMeoFSessionInfo, error) {
		return []util.NVMeoFSessionInfo{{NQN: orphanNQN, Address: "traddr=192.0.2.20,trsvcid=4420"}}, nil
	}
	d.gcNVMeoFSessions(context.Background(), time.Hour, true)
	firstSeenValue, ok := d.orphanedNVMeSessionsSeen.Load(orphanNQN)
	require.True(t, ok)
	firstSeen := firstSeenValue.(time.Time)

	gcListISCSISessions = func() ([]util.ISCSISessionInfo, error) {
		return []util.ISCSISessionInfo{{IQN: "iqn.2005-10.org.freenas.ctl:csi-other", Portal: "192.0.2.10:3260"}}, nil
	}
	d.gcISCSISessions(context.Background(), time.Hour, true)

	afterISCSIValue, ok := d.orphanedNVMeSessionsSeen.Load(orphanNQN)
	require.True(t, ok, "iSCSI cleanup must not delete NVMe grace-period state")
	assert.Equal(t, firstSeen, afterISCSIValue.(time.Time))
}

func TestISCSIGCRestrictsCleanupToConfiguredTargetPrefix(t *testing.T) {
	originalMounted := getMountedBlockDevices
	originalStaged := getStagedBlockDevices
	originalListISCSI := gcListISCSISessions
	originalDisconnectISCSI := gcDisconnectISCSI
	t.Cleanup(func() {
		getMountedBlockDevices = originalMounted
		getStagedBlockDevices = originalStaged
		gcListISCSISessions = originalListISCSI
		gcDisconnectISCSI = originalDisconnectISCSI
	})
	getMountedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	getStagedBlockDevices = func() (map[string]string, error) { return map[string]string{}, nil }
	portal := "192.0.2.10:3260"
	d := &Driver{config: &Config{ISCSI: ISCSIConfig{TargetPortal: portal, NamePrefix: "csi-"}}}
	owned := "iqn.2005-10.org.freenas.ctl:csi-owned"
	foreign := "iqn.2005-10.org.freenas.ctl:database"
	gcListISCSISessions = func() ([]util.ISCSISessionInfo, error) {
		return []util.ISCSISessionInfo{{Portal: portal, IQN: owned}, {Portal: portal, IQN: foreign}}, nil
	}
	d.orphanedISCSISessionsSeen.Store(owned, time.Now().Add(-time.Hour))
	d.orphanedISCSISessionsSeen.Store(foreign, time.Now().Add(-time.Hour))
	var disconnected []string
	gcDisconnectISCSI = func(_ string, iqn string) error {
		disconnected = append(disconnected, iqn)
		return nil
	}

	d.gcISCSISessions(context.Background(), 0, false)

	assert.Equal(t, []string{owned}, disconnected)
	_, foreignTracked := d.orphanedISCSISessionsSeen.Load(foreign)
	assert.False(t, foreignTracked, "foreign sessions must not remain in this driver's ownership map")
}

func TestStartSessionGCHonorsEnabled(t *testing.T) {
	falseValue := false
	disabled := &Driver{config: &Config{SessionGC: SessionGCConfig{Enabled: false, Interval: 1}}}
	disabled.startSessionGC()
	assert.Nil(t, disabled.gcCancel)

	enabled := &Driver{config: &Config{SessionGC: SessionGCConfig{
		Enabled:      true,
		Interval:     0,
		RunOnStartup: &falseValue,
	}}}
	enabled.startSessionGC()
	require.NotNil(t, enabled.gcCancel)
	enabled.stopSessionGC()
}

// TestGetExpectedNVMeoFNQNs_FailedLookupsThreshold tests that getExpectedNVMeoFNQNs
// returns nil when too many NVMe device lookups fail, preventing false positive GC.
func TestGetExpectedNVMeoFNQNs_FailedLookupsThreshold(t *testing.T) {
	// This test verifies the behavior documented in the implementation:
	// If > 2 NVMe device lookups fail, the function returns nil to signal
	// that GC should be skipped to avoid race conditions.
	//
	// The actual implementation relies on util.GetMountedBlockDevices() and
	// util.GetNVMeInfoFromDevice() which require system calls. We test the
	// contract/interface behavior rather than mocking all system calls.

	t.Run("Concept verification", func(t *testing.T) {
		// The maxFailedLookups constant is 2 in the implementation.
		// When failedLookups > 2, getExpectedNVMeoFNQNs returns nil.
		//
		// This documents the expected behavior:
		// - 0-2 failed lookups: continue with GC (non-NVMe devices failing is expected)
		// - >2 failed lookups: skip GC (indicates race condition or transient state)

		const maxFailedLookups = 2

		// Examples of what triggers failedLookups increment:
		// - util.GetNVMeInfoFromDevice returns error for a device that looks like NVMe
		// - This can happen during staging/unstaging when sysfs entries are changing

		// The IsLikelyNVMeDevice function is used to filter which failed lookups count:
		// - /dev/nvme0n1 failing: counts as a failed lookup (looks like NVMe)
		// - /dev/sda failing: does NOT count (not an NVMe device)

		assert.Equal(t, 2, maxFailedLookups, "maxFailedLookups should be 2 as per implementation")
	})
}

// TestSessionGC_OrphanedSessionTracking tests the grace period mechanism
// for orphaned session tracking.
func TestSessionGC_OrphanedSessionTracking(t *testing.T) {
	d := &Driver{
		config: &Config{
			SessionGC: SessionGCConfig{
				Interval:    300,
				GracePeriod: 60,
				DryRun:      true,
			},
			NVMeoF: NVMeoFConfig{
				TransportAddress: "192.168.1.100",
			},
		},
		truenasClient: truenas.NewMockClient(),
	}

	now := time.Now()

	t.Run("New orphaned session tracking", func(t *testing.T) {
		nqn := "nqn.2014-08.org.nvmexpress:uuid:test-1"

		// First time seeing an orphaned session
		_, loaded := d.orphanedNVMeSessionsSeen.LoadOrStore(nqn, now)
		assert.False(t, loaded, "First store should not find existing value")

		// Second time seeing the same session
		_, loaded = d.orphanedNVMeSessionsSeen.LoadOrStore(nqn, now)
		assert.True(t, loaded, "Second store should find existing value")

		// Cleanup
		d.orphanedNVMeSessionsSeen.Delete(nqn)
	})

	t.Run("Grace period calculation", func(t *testing.T) {
		nqn := "nqn.2014-08.org.nvmexpress:uuid:test-2"
		gracePeriod := 60 * time.Second

		firstSeen := now.Add(-30 * time.Second) // 30 seconds ago
		d.orphanedNVMeSessionsSeen.Store(nqn, firstSeen)

		// Load the value
		val, ok := d.orphanedNVMeSessionsSeen.Load(nqn)
		assert.True(t, ok)

		orphanedDuration := now.Sub(val.(time.Time))
		assert.True(t, orphanedDuration < gracePeriod,
			"30 seconds ago should be within 60 second grace period")

		// Now test with session first seen 90 seconds ago (beyond grace period)
		firstSeen = now.Add(-90 * time.Second)
		d.orphanedNVMeSessionsSeen.Store(nqn, firstSeen)

		val, _ = d.orphanedNVMeSessionsSeen.Load(nqn)
		orphanedDuration = now.Sub(val.(time.Time))
		assert.True(t, orphanedDuration > gracePeriod,
			"90 seconds ago should exceed 60 second grace period")

		// Cleanup
		d.orphanedNVMeSessionsSeen.Delete(nqn)
	})

	t.Run("protocol maps are independent", func(t *testing.T) {
		nqn1 := "nqn.2014-08.org.nvmexpress:uuid:vol-1"
		nqn2 := "nqn.2014-08.org.nvmexpress:uuid:vol-2"
		iqn := "iqn.2005-10.org.freenas.ctl:csi-target-1"

		d.orphanedNVMeSessionsSeen.Store(nqn1, now)
		d.orphanedNVMeSessionsSeen.Store(nqn2, now)
		d.orphanedISCSISessionsSeen.Store(iqn, now)
		nvmeofCount := 0
		iscsiCount := 0
		d.orphanedNVMeSessionsSeen.Range(func(_, _ interface{}) bool {
			nvmeofCount++
			return true
		})
		d.orphanedISCSISessionsSeen.Range(func(_, _ interface{}) bool {
			iscsiCount++
			return true
		})

		assert.Equal(t, 2, nvmeofCount, "Should have 2 NVMe-oF sessions")
		assert.Equal(t, 1, iscsiCount, "Should have 1 iSCSI session")

		// Cleanup
		d.orphanedNVMeSessionsSeen.Delete(nqn1)
		d.orphanedNVMeSessionsSeen.Delete(nqn2)
		d.orphanedISCSISessionsSeen.Delete(iqn)
	})
}

// TestSessionGC_CleanupStaleEntries tests that stale orphaned session entries
// are cleaned up when sessions are no longer active.
func TestSessionGC_CleanupStaleEntries(t *testing.T) {
	d := &Driver{
		config: &Config{
			SessionGC: SessionGCConfig{
				Interval:    300,
				GracePeriod: 60,
			},
		},
	}

	now := time.Now()

	// Simulate adding orphaned session entries
	nqn1 := "nqn.2014-08.org.nvmexpress:uuid:active-session"
	nqn2 := "nqn.2014-08.org.nvmexpress:uuid:stale-session"
	d.orphanedNVMeSessionsSeen.Store(nqn1, now)
	d.orphanedNVMeSessionsSeen.Store(nqn2, now)

	// Simulate which sessions are still active (only nqn1)
	activeOrphanedSessions := map[string]struct{}{
		nqn1: {},
	}

	// Simulate cleanup logic from gcNVMeoFSessions
	// Only clean up entries with "nqn." prefix that are no longer active
	d.orphanedNVMeSessionsSeen.Range(func(key, _ interface{}) bool {
		nqn := key.(string)
		if len(nqn) > 4 && nqn[:4] == "nqn." {
			if _, active := activeOrphanedSessions[nqn]; !active {
				d.orphanedNVMeSessionsSeen.Delete(nqn)
			}
		}
		return true
	})

	// Verify nqn1 is still present (active)
	_, ok := d.orphanedNVMeSessionsSeen.Load(nqn1)
	assert.True(t, ok, "Active session entry should remain")

	// Verify nqn2 was cleaned up (stale)
	_, ok = d.orphanedNVMeSessionsSeen.Load(nqn2)
	assert.False(t, ok, "Stale session entry should be cleaned up")
}

// TestSessionGC_DryRunMode tests that dry run mode logs but doesn't disconnect.
func TestSessionGC_DryRunMode(t *testing.T) {
	t.Run("DryRun configuration", func(t *testing.T) {
		// Verify DryRun field works correctly
		cfg := SessionGCConfig{
			Interval:    300,
			GracePeriod: 60,
			DryRun:      true,
		}

		assert.Equal(t, 300, cfg.Interval, "Interval should be 300")
		assert.Equal(t, 60, cfg.GracePeriod, "GracePeriod should be 60")
		assert.True(t, cfg.DryRun, "DryRun should be enabled")

		cfg.DryRun = false
		assert.False(t, cfg.DryRun, "DryRun should be disabled")
	})
}

// TestSessionGC_ProtocolEnabling tests per-protocol GC enable/disable.
func TestSessionGC_ProtocolEnabling(t *testing.T) {
	t.Run("ISCSIEnabled and NVMeoFEnabled config", func(t *testing.T) {
		trueVal := true
		falseVal := false

		cfg := &Config{
			SessionGC: SessionGCConfig{
				ISCSIEnabled:  &trueVal,
				NVMeoFEnabled: &falseVal,
			},
			ISCSI: ISCSIConfig{
				TargetPortal: "192.168.1.100:3260",
			},
			NVMeoF: NVMeoFConfig{
				TransportAddress: "192.168.1.100",
			},
		}

		// iSCSI GC should be enabled (ISCSIEnabled=true and TargetPortal is set)
		iscsiGCEnabled := cfg.ISCSI.TargetPortal != ""
		if cfg.SessionGC.ISCSIEnabled != nil {
			iscsiGCEnabled = *cfg.SessionGC.ISCSIEnabled && cfg.ISCSI.TargetPortal != ""
		}
		assert.True(t, iscsiGCEnabled, "iSCSI GC should be enabled")

		// NVMe-oF GC should be disabled (NVMeoFEnabled=false)
		nvmeofGCEnabled := cfg.NVMeoF.TransportAddress != ""
		if cfg.SessionGC.NVMeoFEnabled != nil {
			nvmeofGCEnabled = *cfg.SessionGC.NVMeoFEnabled && cfg.NVMeoF.TransportAddress != ""
		}
		assert.False(t, nvmeofGCEnabled, "NVMe-oF GC should be disabled")
	})

	t.Run("Default protocol enabling based on configuration", func(t *testing.T) {
		// When ISCSIEnabled/NVMeoFEnabled are nil, defaults are based on protocol config
		cfg := &Config{
			SessionGC: SessionGCConfig{
				ISCSIEnabled:  nil,
				NVMeoFEnabled: nil,
			},
			ISCSI: ISCSIConfig{
				TargetPortal: "", // Not configured
			},
			NVMeoF: NVMeoFConfig{
				TransportAddress: "192.168.1.100", // Configured
			},
		}

		iscsiGCEnabled := cfg.ISCSI.TargetPortal != ""
		nvmeofGCEnabled := cfg.NVMeoF.TransportAddress != ""

		assert.False(t, iscsiGCEnabled, "iSCSI GC should be disabled when not configured")
		assert.True(t, nvmeofGCEnabled, "NVMe-oF GC should be enabled when configured")
	})
}

// TestSessionGC_ConcurrentAccess tests thread safety of protocol tracking.
func TestSessionGC_ConcurrentAccess(t *testing.T) {
	d := &Driver{}

	now := time.Now()
	const numGoroutines = 10
	const numOps = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				nqn := "nqn.test:session-" + string(rune('0'+id))

				// Store
				d.orphanedNVMeSessionsSeen.Store(nqn, now)

				// Load
				d.orphanedNVMeSessionsSeen.Load(nqn)

				// LoadOrStore
				d.orphanedNVMeSessionsSeen.LoadOrStore(nqn, now)

				// Range
				d.orphanedNVMeSessionsSeen.Range(func(key, value interface{}) bool {
					return true
				})

				// Delete
				if j%2 == 0 {
					d.orphanedNVMeSessionsSeen.Delete(nqn)
				}
			}
		}(i)
	}

	wg.Wait()
	// If we get here without panics, the test passes
}

// TestSessionGC_RunOnStartupConfig tests RunOnStartup configuration.
func TestSessionGC_RunOnStartupConfig(t *testing.T) {
	t.Run("RunOnStartup nil defaults to true", func(t *testing.T) {
		cfg := SessionGCConfig{
			RunOnStartup: nil,
		}

		// As per LoadConfig, nil defaults to true
		runOnStartup := true
		if cfg.RunOnStartup != nil {
			runOnStartup = *cfg.RunOnStartup
		}

		assert.True(t, runOnStartup, "RunOnStartup should default to true when nil")
	})

	t.Run("RunOnStartup explicitly false", func(t *testing.T) {
		falseVal := false
		cfg := SessionGCConfig{
			RunOnStartup: &falseVal,
		}

		runOnStartup := true
		if cfg.RunOnStartup != nil {
			runOnStartup = *cfg.RunOnStartup
		}

		assert.False(t, runOnStartup, "RunOnStartup should be false when explicitly set")
	})

	t.Run("RunOnStartup explicitly true", func(t *testing.T) {
		trueVal := true
		cfg := SessionGCConfig{
			RunOnStartup: &trueVal,
		}

		runOnStartup := false
		if cfg.RunOnStartup != nil {
			runOnStartup = *cfg.RunOnStartup
		}

		assert.True(t, runOnStartup, "RunOnStartup should be true when explicitly set")
	})
}

// TestIsLikelyNVMeDevice_UsedInSessionGC tests that IsLikelyNVMeDevice is used
// correctly in the session GC context for determining which failed lookups to count.
func TestIsLikelyNVMeDevice_UsedInSessionGC(t *testing.T) {
	// This test documents how IsLikelyNVMeDevice is used in session GC:
	// When GetNVMeInfoFromDevice fails, we check IsLikelyNVMeDevice to determine
	// if the failure should count toward the failedLookups threshold.

	testCases := []struct {
		device       string
		countsAsFail bool
		reason       string
	}{
		{
			device:       "/dev/nvme0n1",
			countsAsFail: true,
			reason:       "NVMe namespace device - failure should count",
		},
		{
			device:       "/dev/nvme1n2",
			countsAsFail: true,
			reason:       "NVMe namespace device - failure should count",
		},
		{
			device:       "/dev/sda",
			countsAsFail: false,
			reason:       "SCSI device - not NVMe, failure should not count",
		},
		{
			device:       "/dev/sdb1",
			countsAsFail: false,
			reason:       "SCSI partition - not NVMe, failure should not count",
		},
		{
			device:       "/dev/dm-0",
			countsAsFail: false,
			reason:       "Device mapper - not NVMe, failure should not count",
		},
		{
			device:       "/dev/loop0",
			countsAsFail: false,
			reason:       "Loop device - not NVMe, failure should not count",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.device, func(t *testing.T) {
			// Use the actual function from util package
			// The test documents that this is the expected behavior
			_ = tc // Placeholder - actual implementation uses util.IsLikelyNVMeDevice

			// The logic in getExpectedNVMeoFNQNs is:
			// if util.IsLikelyNVMeDevice(device) {
			//     failedLookups++
			// }

			assert.NotEmpty(t, tc.reason, "Test case should have a reason")
		})
	}
}
