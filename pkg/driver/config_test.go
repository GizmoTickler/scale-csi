package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadTestConfig(t *testing.T, body string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return LoadConfig(path)
}

const requiredTestConfig = `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
zfs:
  datasetParentName: tank/csi
`

func TestLoadConfigValidatesOnlyEnabledProtocols(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: false
iscsi:
  enabled: true
  targetPortal: 192.0.2.10:3260
nvmeof:
  enabled: false
`)
	require.NoError(t, err)
	assert.False(t, cfg.NFS.Enabled)
	assert.True(t, cfg.ISCSI.Enabled)
	assert.Empty(t, cfg.NFS.ShareHost)
}

func TestLoadConfigAllowsMissingAPIKeyForNodeMode(t *testing.T) {
	cfg, err := loadTestConfig(t, `
driver: csi.scale.io
truenas:
  host: truenas.example.test
zfs:
  datasetParentName: tank/csi
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Empty(t, cfg.TrueNAS.APIKey)
}

func TestLoadConfigRequiresAtLeastOneProtocol(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one storage protocol")
}

func TestLoadConfigValidatesEnabledProtocolFields(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nfs.shareHost")
}

func TestLoadConfigRejectsRestrictedNVMeWithoutHosts(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
fencing:
  mode: off
nvmeof:
  enabled: true
  transportAddress: 192.0.2.20
  subsystemAllowAnyHost: false
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subsystemHosts is empty")
	assert.Contains(t, err.Error(), "no host could connect")
}

func TestLoadConfigDefaultsHealthCacheTTL(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Equal(t, "5s", cfg.Health.CacheTTL)

	cfg, err = loadTestConfig(t, requiredTestConfig+`
health:
  cacheTTL: 15s
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Equal(t, "15s", cfg.Health.CacheTTL)
}

func TestLoadConfigRejectsNegativeHealthCacheTTL(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
health:
  cacheTTL: -1s
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "health.cacheTTL")
}

func TestLoadConfigDefaultsFencingAndOwnershipIdentity(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Equal(t, FencingModeOff, cfg.Fencing.Mode)
	assert.Equal(t, "10m", cfg.Fencing.StartupReconcileTimeout)
	assert.Equal(t, "10m", cfg.Fencing.StaleRecordGracePeriod)
	assert.Equal(t, "csi.scale.io@tank/csi", cfg.DriverInstanceID)
}

func TestLoadConfigAcceptsAllFencingModesAndRejectsUnknownMode(t *testing.T) {
	for _, mode := range []FencingMode{FencingModeOff, FencingModeAdditive, FencingModeStrict} {
		t.Run(string(mode), func(t *testing.T) {
			cfg, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
fencing:
  mode: %s
nfs:
  enabled: true
  shareHost: 192.0.2.10
`, mode))
			require.NoError(t, err)
			assert.Equal(t, mode, cfg.Fencing.Mode)
		})
	}

	_, err := loadTestConfig(t, requiredTestConfig+`
fencing:
  mode: permissive
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fencing.mode")
}

func TestLoadConfigRejectsInvalidFencingReconcileDurations(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		value string
	}{
		{name: "startup timeout", field: "startupReconcileTimeout", value: "not-a-duration"},
		{name: "stale grace", field: "staleRecordGracePeriod", value: "0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
fencing:
  mode: additive
  %s: %s
`, tc.field, tc.value))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "fencing."+tc.field)
		})
	}
}

func TestLoadConfigDefaultsDatasetQuotasToTrue(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.True(t, cfg.NFS.Enabled, "a legacy protocol block remains enabled")
	assert.True(t, cfg.ZFS.DatasetEnableQuotas)

	cfg, err = loadTestConfig(t, `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
zfs:
  datasetParentName: tank/csi
  datasetEnableQuotas: false
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.False(t, cfg.ZFS.DatasetEnableQuotas, "an explicit false must override the default")
}

func TestLoadConfigDetachedVolumesFromSnapshotsDefaultsOff(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.False(t, cfg.ZFS.DetachedVolumesFromSnapshots)

	cfg, err = loadTestConfig(t, `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
zfs:
  datasetParentName: tank/csi
  detachedVolumesFromSnapshots: true
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.True(t, cfg.ZFS.DetachedVolumesFromSnapshots)
}

func TestLoadConfigCustomCATrust(t *testing.T) {
	cfg, err := loadTestConfig(t, `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
  caCert: |
    -----BEGIN CERTIFICATE-----
    test-ca
    -----END CERTIFICATE-----
  caCertFile: /etc/scale-csi/truenas-ca.pem
zfs:
  datasetParentName: tank/csi
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Contains(t, cfg.TrueNAS.CACert, "BEGIN CERTIFICATE")
	assert.Equal(t, "/etc/scale-csi/truenas-ca.pem", cfg.TrueNAS.CACertFile)
}

func TestLoadConfigZvolReservationAndISCSIPhysicalBlocksize(t *testing.T) {
	cfg, err := loadTestConfig(t, `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
zfs:
  datasetParentName: tank/csi
  zvolEnableReservation: true
iscsi:
  enabled: true
  targetPortal: 192.0.2.10:3260
  extentDisablePhysicalBlocksize: true
`)
	require.NoError(t, err)
	assert.True(t, cfg.ZFS.ZvolEnableReservation)
	assert.True(t, cfg.ISCSI.ExtentDisablePhysicalBlocksize)
}

func TestLoadConfigRejectsNegativeNumericSettings(t *testing.T) {
	config := func(truenasExtra, zfsExtra, rootExtra string) string {
		return fmt.Sprintf(`
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
%s
zfs:
  datasetParentName: tank/csi
%s
nfs:
  enabled: true
  shareHost: 192.0.2.10
%s
`, truenasExtra, zfsExtra, rootExtra)
	}
	tests := []struct {
		name      string
		body      string
		wantField string
	}{
		{
			name:      "API concurrency",
			body:      config("  maxConcurrentRequests: -1", "", ""),
			wantField: "truenas.maxConcurrentRequests",
		},
		{
			name:      "API request timeout",
			body:      config("  requestTimeout: -1", "", ""),
			wantField: "truenas.requestTimeout",
		},
		{
			name:      "zvol readiness timeout",
			body:      config("", "  zvolReadyTimeout: -1", ""),
			wantField: "zfs.zvolReadyTimeout",
		},
		{
			name:      "command timeout",
			body:      config("", "", "commandTimeouts:\n  mount: -1"),
			wantField: "commandTimeouts.mount",
		},
		{
			name:      "retry multiplier",
			body:      config("", "", "resilience:\n  retry:\n    backoffMultiplier: -1"),
			wantField: "resilience.retry.backoffMultiplier",
		},
		{
			name:      "max volumes per node",
			body:      config("", "", "node:\n  maxVolumesPerNode: -1"),
			wantField: "node.maxVolumesPerNode",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestConfig(t, test.body)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantField)
		})
	}
}

func TestLoadConfigDefaultsTrueNASConcurrency(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.TrueNAS.MaxConcurrentRequests)
}

func TestLoadConfigTrueNASMaxConnections(t *testing.T) {
	withMaxConnections := func(extra string) string {
		return `
driver: csi.scale.io
truenas:
  host: truenas.example.test
  apiKey: test-key
` + extra + `
zfs:
  datasetParentName: tank/csi
nfs:
  enabled: true
  shareHost: 192.0.2.10
`
	}

	t.Run("defaults to five when unset", func(t *testing.T) {
		cfg, err := loadTestConfig(t, withMaxConnections(""))
		require.NoError(t, err)
		assert.Equal(t, 5, cfg.TrueNAS.MaxConnections)
	})

	t.Run("accepts the full clamp range", func(t *testing.T) {
		for _, n := range []int{1, 8, 16} {
			cfg, err := loadTestConfig(t, withMaxConnections(fmt.Sprintf("  maxConnections: %d\n", n)))
			require.NoError(t, err)
			assert.Equal(t, n, cfg.TrueNAS.MaxConnections)
		}
	})

	t.Run("rejects values outside the clamp", func(t *testing.T) {
		for _, n := range []int{-1, 17, 100} {
			_, err := loadTestConfig(t, withMaxConnections(fmt.Sprintf("  maxConnections: %d\n", n)))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "truenas.maxConnections")
		}
	})

	// An explicit zero must be rejected, not coerced to the default of 5: the
	// default is seeded pre-decode so a hand-written `maxConnections: 0` survives
	// decoding and trips the 1..16 validation, matching the chart schema (which
	// also rejects 0). Coercing it would silently accept an operator typo.
	t.Run("rejects an explicit zero", func(t *testing.T) {
		_, err := loadTestConfig(t, withMaxConnections("  maxConnections: 0\n"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "truenas.maxConnections")
	})
}

func TestLoadConfigReconcileDefaultsAreSafe(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.True(t, cfg.Reconcile.Enabled)
	assert.Equal(t, "1h", cfg.Reconcile.Interval)
	assert.Equal(t, "24h", cfg.Reconcile.MinOrphanAge)
	assert.False(t, cfg.Reconcile.Delete.Enabled)
	assert.Equal(t, "0 4 * * *", cfg.Reconcile.Delete.Schedule)
	assert.Equal(t, 5, cfg.Reconcile.Delete.MaxPerRun)
}

func TestLoadConfigReconcileExplicitDisableAndDeleteGate(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
reconcile:
  enabled: false
  interval: 2h
  minOrphanAge: 48h
  delete:
    enabled: true
    schedule: "30 3 * * 0"
    maxPerRun: 11
`)
	require.NoError(t, err)
	assert.False(t, cfg.Reconcile.Enabled)
	assert.Equal(t, "2h", cfg.Reconcile.Interval)
	assert.Equal(t, "48h", cfg.Reconcile.MinOrphanAge)
	assert.True(t, cfg.Reconcile.Delete.Enabled)
	assert.Equal(t, "30 3 * * 0", cfg.Reconcile.Delete.Schedule)
	assert.Equal(t, 11, cfg.Reconcile.Delete.MaxPerRun)
}

func TestLoadConfigRejectsUnsafeReconcileDeleteCap(t *testing.T) {
	for _, maxPerRun := range []int{-1, 0} {
		t.Run(fmt.Sprintf("maxPerRun=%d", maxPerRun), func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: 192.0.2.10
reconcile:
  delete:
    maxPerRun: %d
`, maxPerRun))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "reconcile.delete.maxPerRun")
		})
	}
}

func TestLoadConfigRejectsUnsafeReconcileDurations(t *testing.T) {
	for _, test := range []struct {
		name  string
		field string
		value string
	}{
		{name: "invalid interval", field: "interval", value: "hourly"},
		{name: "zero interval", field: "interval", value: "0s"},
		{name: "invalid minimum age", field: "minOrphanAge", value: "tomorrow"},
		{name: "zero minimum age", field: "minOrphanAge", value: "0s"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
reconcile:
  `+test.field+`: `+test.value+`
`)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "reconcile.")
		})
	}
}

func TestLoadConfigRejectsUnknownKeys(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
  shareHosst: typo.example.test
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shareHosst")
	assert.Contains(t, err.Error(), "field")
}

func TestLoadConfigSessionGCEnabledNormalization(t *testing.T) {
	protocol := `
nfs:
  enabled: true
  shareHost: 192.0.2.10
`
	t.Run("defaults enabled with default interval", func(t *testing.T) {
		cfg, err := loadTestConfig(t, requiredTestConfig+protocol)
		require.NoError(t, err)
		assert.True(t, cfg.SessionGC.Enabled)
		assert.Equal(t, 300, cfg.SessionGC.Interval)
	})
	t.Run("explicit disable keeps zero interval", func(t *testing.T) {
		cfg, err := loadTestConfig(t, requiredTestConfig+protocol+`
sessionGC:
  enabled: false
  interval: 0
`)
		require.NoError(t, err)
		assert.False(t, cfg.SessionGC.Enabled)
		assert.Zero(t, cfg.SessionGC.Interval)
	})
	t.Run("enabled zero interval gets default", func(t *testing.T) {
		cfg, err := loadTestConfig(t, requiredTestConfig+protocol+`
sessionGC:
  enabled: true
  interval: 0
`)
		require.NoError(t, err)
		assert.True(t, cfg.SessionGC.Enabled)
		assert.Equal(t, 300, cfg.SessionGC.Interval)
	})
}

func TestLoadConfigWarnsForInertISCSITargetPortals(t *testing.T) {
	originalWarningf := configWarningf
	t.Cleanup(func() { configWarningf = originalWarningf })
	var warning string
	configWarningf = func(format string, args ...interface{}) {
		warning = fmt.Sprintf(format, args...)
	}

	_, err := loadTestConfig(t, requiredTestConfig+`
iscsi:
  enabled: true
  targetPortal: 192.0.2.10:3260
  targetPortals:
    - 192.0.2.11:3260
`)
	require.NoError(t, err)
	assert.Contains(t, warning, "iscsi.targetPortals")
	assert.Contains(t, warning, "iscsi.multipath=true and iscsi.portals")
}

func TestLoadConfigISCSIMultipathNormalizesAndDeduplicatesPortals(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
iscsi:
  enabled: true
  targetPortal: "[2001:db8::10]:3261"
  multipath: true
  portals:
    - "[2001:db8::11]"
    - "2001:db8::10"
    - "2001:db8::12"
`)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::10]:3261", cfg.ISCSI.TargetPortal)
	assert.Equal(t, []string{
		"[2001:db8::10]:3261",
		"[2001:db8::11]:3261",
		"[2001:db8::12]:3261",
	}, cfg.ISCSI.multipathPortals())
}

func TestLoadConfigNormalizesLegacyISCSIPortalUnconditionally(t *testing.T) {
	tests := []struct {
		name   string
		portal string
		want   string
	}{
		{name: "portless IPv4", portal: "192.0.2.10", want: "192.0.2.10:3260"},
		{name: "portless hostname", portal: "truenas.example.com", want: "truenas.example.com:3260"},
		{name: "hostname with explicit port", portal: "TRUENAS.example.com:3261", want: "truenas.example.com:3261"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
iscsi:
  enabled: true
  targetPortal: %q
`, test.portal))
			require.NoError(t, err)
			assert.Equal(t, test.want, cfg.ISCSI.TargetPortal)
		})
	}
}

func TestLoadConfigRejectsInvalidISCSIMultipathPortals(t *testing.T) {
	tests := []struct {
		name       string
		primary    string
		additional string
		wantField  string
	}{
		{name: "hostname primary", primary: "storage.invalid:3260", additional: "192.0.2.11", wantField: "iscsi.targetPortal"},
		{name: "port in additional", primary: "192.0.2.10:3260", additional: "192.0.2.11:3260", wantField: "iscsi.portals[0]"},
		{name: "hostname additional", primary: "192.0.2.10:3260", additional: "storage.invalid", wantField: "iscsi.portals[0]"},
		{name: "whitespace", primary: "192.0.2.10:3260", additional: " 192.0.2.11", wantField: "iscsi.portals[0]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
iscsi:
  enabled: true
  targetPortal: %q
  multipath: true
  portals: [%q]
`, test.primary, test.additional))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantField)
		})
	}
}

func TestLoadConfigRejectsISCSIMultipathWithoutAdditionalPortal(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
iscsi:
  enabled: true
  targetPortal: 192.0.2.10:3260
  multipath: true
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iscsi.portals")
}

func TestLoadConfigBoundsNFSAddressesAndISCSIPortals(t *testing.T) {
	nfsAddresses := make([]string, 17)
	iscsiPortals := make([]string, 17)
	for i := range nfsAddresses {
		nfsAddresses[i] = fmt.Sprintf("192.0.2.%d", i+1)
		iscsiPortals[i] = fmt.Sprintf("198.51.100.%d", i+1)
	}

	_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: 203.0.113.1
  addresses: [%s]
`, strings.Join(nfsAddresses, ", ")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nfs.addresses must contain at most 16 entries")

	_, err = loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
iscsi:
  enabled: true
  targetPortal: 203.0.113.2
  portals: [%s]
`, strings.Join(iscsiPortals, ", ")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iscsi.portals must contain at most 16 entries")
}

func TestLoadConfigBoundsEffectiveNFSTrunkTransports(t *testing.T) {
	addresses := make([]string, 16)
	for i := range addresses {
		addresses[i] = fmt.Sprintf("192.0.2.%d", i+1)
	}
	_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: 203.0.113.1
  trunking: true
  addresses: [%s]
`, strings.Join(addresses, ", ")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at most 16 distinct transports")
}

func TestLoadConfigNFSTransportKnobs(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: "[2001:db8::20]"
  nconnect: 8
  trunking: true
  addresses:
    - "[2001:db8::21]"
    - "2001:db8::20"
    - "2001:db8::22"
`)
	require.NoError(t, err)
	require.NotNil(t, cfg.NFS.NConnect)
	assert.Equal(t, 8, *cfg.NFS.NConnect)
	assert.Equal(t, []string{"2001:db8::20", "2001:db8::21", "2001:db8::22"}, cfg.NFS.trunkingAddresses())
}

func TestLoadConfigRejectsInvalidNFSTransportKnobs(t *testing.T) {
	for _, nconnect := range []int{0, 17} {
		t.Run(fmt.Sprintf("nconnect-%d", nconnect), func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: 192.0.2.20
  nconnect: %d
`, nconnect))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "nfs.nconnect")
		})
	}
	for _, test := range []struct {
		name      string
		shareHost string
		address   string
		wantField string
	}{
		{name: "hostname primary", shareHost: "storage.invalid", address: "192.0.2.21", wantField: "nfs.shareHost"},
		{name: "port", shareHost: "192.0.2.20", address: "192.0.2.21:2049", wantField: "nfs.addresses[0]"},
		{name: "hostname", shareHost: "192.0.2.20", address: "storage.invalid", wantField: "nfs.addresses[0]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: %q
  trunking: true
  addresses: [%q]
`, test.shareHost, test.address))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantField)
		})
	}
}

func TestLoadConfigRejectsNFSTrunkingWithoutAddresses(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.20
  trunking: true
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nfs.addresses")
}

func TestTransportMultipathDisabledPreservesLegacyValues(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: storage.invalid
  addresses: [also.invalid]
iscsi:
  enabled: true
  targetPortal: storage.invalid:3260
  portals: [also.invalid]
`)
	require.NoError(t, err)
	assert.Nil(t, cfg.NFS.NConnect)
	assert.Nil(t, cfg.NFS.trunkingAddresses())
	assert.Nil(t, cfg.ISCSI.multipathPortals())
	assert.Equal(t, "storage.invalid", cfg.NFS.ShareHost)
	assert.Equal(t, "storage.invalid:3260", cfg.ISCSI.TargetPortal)
}

// TestLoadConfigWarnsForOrphanedGlobalSquashGroup covers the config-load half of
// the squash preflight: a globally orphaned group passes startup (a StorageClass
// can still supply the missing user, and failing here would take block
// provisioning down with it) but no longer does so silently.
func TestLoadConfigWarnsForOrphanedGlobalSquashGroup(t *testing.T) {
	captureWarnings := func(t *testing.T) *[]string {
		t.Helper()
		originalWarningf := configWarningf
		t.Cleanup(func() { configWarningf = originalWarningf })
		warnings := &[]string{}
		configWarningf = func(format string, args ...interface{}) {
			*warnings = append(*warnings, fmt.Sprintf(format, args...))
		}
		return warnings
	}

	for _, tc := range []struct {
		name     string
		yaml     string
		expected string
	}{
		{"maproot", "  shareMaprootUser: \"\"\n  shareMaprootGroup: wheel\n", "nfs.shareMaprootUser"},
		{"mapall", "  shareMapallUser: \"\"\n  shareMapallGroup: nogroup\n", "nfs.shareMapallUser"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings := captureWarnings(t)

			_, err := loadTestConfig(t, requiredTestConfig+"nfs:\n  enabled: true\n  shareHost: 192.0.2.10\n"+tc.yaml)
			require.NoError(t, err, "an orphaned squash group must not be startup-fatal")

			var matched int
			for _, warning := range *warnings {
				if strings.Contains(warning, tc.expected) && strings.Contains(warning, "without its user") {
					matched++
				}
			}
			assert.Equal(t, 1, matched, "expected exactly one orphaned-group warning, got %v", *warnings)
		})
	}

	t.Run("a complete pair warns about nothing", func(t *testing.T) {
		warnings := captureWarnings(t)

		_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
  shareMaprootUser: root
  shareMaprootGroup: wheel
`)
		require.NoError(t, err)
		for _, warning := range *warnings {
			assert.NotContains(t, warning, "without its user")
		}
	})
}

func TestLoadConfigWarnsWhenDeprecatedRateLimitingConcurrencySet(t *testing.T) {
	originalWarningf := configWarningf
	t.Cleanup(func() { configWarningf = originalWarningf })
	var warnings []string
	configWarningf = func(format string, args ...interface{}) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
resilience:
  rateLimiting:
    maxConcurrentRequests: 25
`)
	require.NoError(t, err, "the deprecated key must still parse for backward compatibility")
	assert.Equal(t, 25, cfg.Resilience.RateLimiting.MaxConcurrentRequests, "the key still parses for backward compatibility but is consumed by nothing")

	var deprecations int
	for _, warning := range warnings {
		if strings.Contains(warning, "resilience.rateLimiting.maxConcurrentRequests is deprecated") {
			deprecations++
			assert.Contains(t, warning, "truenas.maxConcurrentRequests")
		}
	}
	assert.Equal(t, 1, deprecations, "exactly one deprecation warning must fire when the key is set")
}

func TestLoadConfigWarnsForOtherDeprecatedKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "nfs.shareCommentTemplate",
			body: "nfs:\n  enabled: true\n  shareHost: 192.0.2.10\n  shareCommentTemplate: csi\n",
			want: "nfs.shareCommentTemplate is deprecated",
		},
		{
			name: "iscsi.extentAvailThreshold",
			body: "iscsi:\n  enabled: true\n  targetPortal: 192.0.2.10:3260\n  extentAvailThreshold: 50\n",
			want: "iscsi.extentAvailThreshold is deprecated",
		},
		{
			name: "nvmeof.nameTemplate",
			body: "nvmeof:\n  enabled: true\n  transportAddress: 192.0.2.20\n  subsystemAllowAnyHost: true\n  nameTemplate: csi\n",
			want: "nvmeof.nameTemplate is deprecated",
		},
		{
			name: "nvmeof.commandTimeout",
			body: "nvmeof:\n  enabled: true\n  transportAddress: 192.0.2.20\n  subsystemAllowAnyHost: true\n  commandTimeout: 30\n",
			want: "nvmeof.commandTimeout is deprecated",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originalWarningf := configWarningf
			t.Cleanup(func() { configWarningf = originalWarningf })
			var warnings []string
			configWarningf = func(format string, args ...interface{}) {
				warnings = append(warnings, fmt.Sprintf(format, args...))
			}

			_, err := loadTestConfig(t, requiredTestConfig+test.body)
			require.NoError(t, err, "deprecated keys must still parse for backward compatibility")

			var hits int
			for _, warning := range warnings {
				if strings.Contains(warning, test.want) {
					hits++
				}
			}
			assert.Equal(t, 1, hits, "exactly one deprecation warning must fire for %s", test.name)
		})
	}
}

func TestLoadConfigNoDeprecationWarningWhenRateLimitingConcurrencyUnset(t *testing.T) {
	originalWarningf := configWarningf
	t.Cleanup(func() { configWarningf = originalWarningf })
	var warnings []string
	configWarningf = func(format string, args ...interface{}) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
resilience:
  rateLimiting:
    maxConcurrentLogins: 4
`)
	require.NoError(t, err)
	assert.Zero(t, cfg.Resilience.RateLimiting.MaxConcurrentRequests, "the deprecated key carries no default once unset")
	assert.Equal(t, 4, cfg.Resilience.RateLimiting.MaxConcurrentLogins, "the still-wired login limit keeps its explicit value")
	for _, warning := range warnings {
		assert.NotContains(t, warning, "rateLimiting.maxConcurrentRequests is deprecated")
	}
}

func TestRepositoryExampleConfigsParseStrictly(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	require.NoError(t, err)
	require.Len(t, paths, 3, "examples must contain only the three complete protocol configs")

	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			cfg, loadErr := LoadConfig(path)
			require.NoError(t, loadErr)
			assert.Equal(t, 1, cfg.enabledShareTypeCount())
		})
	}
}

func TestLoadConfigNVMeoFBlockKnobsDefaultOmit(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nvmeof:
  enabled: true
  transportAddress: 192.0.2.20
  subsystemAllowAnyHost: true
`)
	require.NoError(t, err)
	// GF-Sprint 4 knobs default to omit so an unchanged config is byte-identical.
	assert.Nil(t, cfg.NVMeoF.PortPerf.InlineDataSize)
	assert.Nil(t, cfg.NVMeoF.PortPerf.MaxQueueSize)
	assert.Nil(t, cfg.NVMeoF.PortPerf.PiEnable)
	assert.False(t, cfg.NVMeoF.Multipath)
	assert.Empty(t, cfg.NVMeoF.Addresses)
	assert.Nil(t, cfg.NVMeoF.multipathAddresses(), "multipath off yields no address list")
}

func TestLoadConfigNVMeoFPortPerfAndMultipath(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nvmeof:
  enabled: true
  transportAddress: 192.168.120.10
  subsystemAllowAnyHost: true
  portPerf:
    inlineDataSize: 8192
    maxQueueSize: 128
    piEnable: true
  multipath: true
  addresses:
    - 192.168.202.10
    - 192.168.203.10
`)
	require.NoError(t, err)
	require.NotNil(t, cfg.NVMeoF.PortPerf.InlineDataSize)
	assert.Equal(t, 8192, *cfg.NVMeoF.PortPerf.InlineDataSize)
	require.NotNil(t, cfg.NVMeoF.PortPerf.MaxQueueSize)
	assert.Equal(t, 128, *cfg.NVMeoF.PortPerf.MaxQueueSize)
	require.NotNil(t, cfg.NVMeoF.PortPerf.PiEnable)
	assert.True(t, *cfg.NVMeoF.PortPerf.PiEnable)
	assert.True(t, cfg.NVMeoF.Multipath)
	assert.Equal(t, []string{"192.168.120.10", "192.168.202.10", "192.168.203.10"}, cfg.NVMeoF.multipathAddresses())
}

func TestLoadConfigRejectsMultipathWithoutAddresses(t *testing.T) {
	_, err := loadTestConfig(t, requiredTestConfig+`
nvmeof:
  enabled: true
  transportAddress: 192.0.2.20
  subsystemAllowAnyHost: true
  multipath: true
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nvmeof.addresses")
}

func TestLoadConfigRejectsInvalidNVMeoFMultipathAddresses(t *testing.T) {
	tests := []struct {
		name             string
		transportAddress string
		additional       string
		wantField        string
	}{
		{name: "port", transportAddress: "192.0.2.20", additional: "192.0.2.21:4420", wantField: "nvmeof.addresses[0]"},
		{name: "URI scheme", transportAddress: "192.0.2.20", additional: "tcp://192.0.2.21", wantField: "nvmeof.addresses[0]"},
		{name: "malformed bracket", transportAddress: "192.0.2.20", additional: "[2001:db8::21", wantField: "nvmeof.addresses[0]"},
		{name: "whitespace", transportAddress: "192.0.2.20", additional: " 192.0.2.21", wantField: "nvmeof.addresses[0]"},
		{name: "hostname", transportAddress: "192.0.2.20", additional: "storage.invalid", wantField: "nvmeof.addresses[0]"},
		{name: "primary hostname", transportAddress: "storage.invalid", additional: "192.0.2.21", wantField: "nvmeof.transportAddress"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nvmeof:
  enabled: true
  transportAddress: %q
  subsystemAllowAnyHost: true
  multipath: true
  addresses:
    - %q
`, test.transportAddress, test.additional))
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantField)
		})
	}
}

func TestLoadConfigNormalizesBracketedNVMeoFMultipathIPv6(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nvmeof:
  enabled: true
  transportAddress: "[2001:db8::20]"
  subsystemAllowAnyHost: true
  multipath: true
  addresses:
    - "[2001:db8::21]"
`)
	require.NoError(t, err)
	assert.Equal(t, "2001:db8::20", cfg.NVMeoF.TransportAddress)
	assert.Equal(t, []string{"2001:db8::21"}, cfg.NVMeoF.Addresses)
	assert.Equal(t, []string{"2001:db8::20", "2001:db8::21"}, cfg.NVMeoF.multipathAddresses())
}

func TestNVMeoFMultipathAddresses(t *testing.T) {
	off := NVMeoFConfig{TransportAddress: "192.168.120.10"}
	assert.Nil(t, off.multipathAddresses(), "multipath off must yield nil")

	on := NVMeoFConfig{
		TransportAddress: "192.168.120.10",
		Multipath:        true,
		Addresses:        []string{"192.168.202.10", "192.168.120.10", "192.168.203.10", ""},
	}
	// TransportAddress first, de-duplicated, blanks dropped.
	assert.Equal(t, []string{"192.168.120.10", "192.168.202.10", "192.168.203.10"}, on.multipathAddresses())
}
