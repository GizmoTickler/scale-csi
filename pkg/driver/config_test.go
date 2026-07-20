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
nvmeof:
  enabled: true
  transportAddress: 192.0.2.20
  subsystemAllowAnyHost: false
`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "subsystemHosts is empty")
	assert.Contains(t, err.Error(), "no host could connect")
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
  namePrefix: csi-
  extentDisablePhysicalBlocksize: true
`)
	require.NoError(t, err)
	assert.True(t, cfg.ZFS.ZvolEnableReservation)
	assert.Equal(t, "csi-", cfg.ISCSI.NamePrefix)
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
