package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

const nvmeofTestConfigBlock = `
nvmeof:
  enabled: true
  transportAddress: 192.0.2.20
  subsystemAllowAnyHost: true
`

// An install that never mentions the data path loads exactly as before: the
// kernel path, and ublk settings only at their defaults.
func TestLoadConfigNVMeoFDataPathDefaults(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock)
	require.NoError(t, err)
	assert.Equal(t, NVMeoFDataPathKernel, cfg.NVMeoF.DataPath)
	assert.False(t, cfg.NVMeoF.ublkAvailable())
	assert.Equal(t, util.DefaultNVMeUblkSocket, cfg.NVMeoF.Ublk.SocketPath)
	assert.Equal(t, 8, cfg.NVMeoF.Ublk.Queues)
	assert.Equal(t, 64, cfg.NVMeoF.Ublk.Depth)
	require.NotNil(t, cfg.NVMeoF.Ublk.ZeroCopy)
	assert.True(t, *cfg.NVMeoF.Ublk.ZeroCopy)
	assert.Equal(t, 0, cfg.NVMeoF.Ublk.NapiUs)
	assert.Equal(t, 60, cfg.NVMeoF.Ublk.AttachTimeout)
}

func TestLoadConfigNVMeoFDataPathParses(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock+`  dataPath: " UBLK "
  ublk:
    socketPath: /run/custom/d.sock
    queues: 4
    depth: 128
    zeroCopy: false
    napiUs: 200
    attachTimeout: 90
`)
	require.NoError(t, err)
	assert.Equal(t, NVMeoFDataPathUblk, cfg.NVMeoF.DataPath, "dataPath is canonicalized")
	assert.True(t, cfg.NVMeoF.ublkAvailable(), "dataPath: ublk implies the ublk data path is available")
	assert.Equal(t, "/run/custom/d.sock", cfg.NVMeoF.Ublk.SocketPath)
	assert.Equal(t, 4, cfg.NVMeoF.Ublk.Queues)
	assert.Equal(t, 128, cfg.NVMeoF.Ublk.Depth)
	require.NotNil(t, cfg.NVMeoF.Ublk.ZeroCopy)
	assert.False(t, *cfg.NVMeoF.Ublk.ZeroCopy, "an explicit false must survive defaulting")
	assert.Equal(t, 200, cfg.NVMeoF.Ublk.NapiUs)
	assert.Equal(t, 90, cfg.NVMeoF.Ublk.AttachTimeout)

	optIn, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock+`  ublk:
    enabled: true
`)
	require.NoError(t, err)
	assert.Equal(t, NVMeoFDataPathKernel, optIn.NVMeoF.DataPath, "ublk.enabled alone keeps the kernel default")
	assert.True(t, optIn.NVMeoF.ublkAvailable())
}

func TestLoadConfigNVMeoFDataPathRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"unknown data path", "  dataPath: spdk\n", "nvmeof.dataPath"},
		{"relative socket", "  ublk:\n    socketPath: run/d.sock\n", "nvmeof.ublk.socketPath"},
		{"negative queues", "  ublk:\n    queues: -1\n", "nvmeof.ublk.queues"},
		{"too many queues", "  ublk:\n    queues: 4097\n", "nvmeof.ublk.queues"},
		{"negative depth", "  ublk:\n    depth: -8\n", "nvmeof.ublk.depth"},
		{"too deep", "  ublk:\n    depth: 8192\n", "nvmeof.ublk.depth"},
		{"negative napi", "  ublk:\n    napiUs: -1\n", "nvmeof.ublk.napiUs"},
		{"napi typo", "  ublk:\n    napiUs: 2000000\n", "nvmeof.ublk.napiUs"},
		{"negative attach timeout", "  ublk:\n    attachTimeout: -5\n", "nvmeof.ublk.attachTimeout"},
		{"rdma with ublk", "  transport: rdma\n  dataPath: ublk\n", "supports only nvmeof.transport=tcp"},
		{"rdma with ublk opt-in", "  transport: rdma\n  ublk:\n    enabled: true\n", "supports only nvmeof.transport=tcp"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock+tc.yaml)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// rdma stays valid for installs that never enable ublk.
func TestLoadConfigNVMeoFRDMAWithoutUblkStillLoads(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock+"  transport: rdma\n")
	require.NoError(t, err)
	assert.Equal(t, "rdma", cfg.NVMeoF.Transport)
}

func TestNormalizeNVMeoFDataPath(t *testing.T) {
	tests := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"", NVMeoFDataPathKernel, true},
		{"kernel", NVMeoFDataPathKernel, true},
		{" Kernel ", NVMeoFDataPathKernel, true},
		{"ublk", NVMeoFDataPathUblk, true},
		{"UBLK", NVMeoFDataPathUblk, true},
		{"userspace", "", false},
	}
	for _, tc := range tests {
		got, ok := normalizeNVMeoFDataPath(tc.in)
		assert.Equal(t, tc.wantOK, ok, tc.in)
		assert.Equal(t, tc.want, got, tc.in)
	}
}

// A Config built in code (tests, or anything that bypasses LoadConfig) reads
// its ublk settings through withDefaults and so behaves like a loaded one.
func TestNVMeoFUblkConfigWithDefaults(t *testing.T) {
	got := NVMeoFUblkConfig{}.withDefaults()
	require.NotNil(t, got.ZeroCopy)
	assert.Equal(t, NVMeoFUblkConfig{
		SocketPath:    util.DefaultNVMeUblkSocket,
		Queues:        8,
		Depth:         64,
		ZeroCopy:      got.ZeroCopy,
		AttachTimeout: 60,
	}, got)
	assert.True(t, *got.ZeroCopy)

	off := false
	kept := NVMeoFUblkConfig{SocketPath: "/x.sock", Queues: 2, Depth: 16, ZeroCopy: &off, NapiUs: 50, AttachTimeout: 5}.withDefaults()
	assert.Equal(t, "/x.sock", kept.SocketPath)
	assert.Equal(t, 2, kept.Queues)
	assert.Equal(t, 16, kept.Depth)
	assert.False(t, *kept.ZeroCopy)
	assert.Equal(t, 50, kept.NapiUs)
	assert.Equal(t, 5, kept.AttachTimeout)

	assert.Equal(t, NVMeoFDataPathKernel, NVMeoFConfig{DataPath: "bogus"}.defaultDataPath(),
		"an invalid value never selects the userspace path")
}
