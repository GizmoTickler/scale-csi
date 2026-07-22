package driver

import (
	"fmt"
	"os"
	"path/filepath"
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
	assert.Contains(t, warning, "does not currently support iSCSI multipath")
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
