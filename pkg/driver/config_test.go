package driver

import (
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
