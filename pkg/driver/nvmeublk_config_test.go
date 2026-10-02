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
	assert.Zero(t, cfg.NVMeoF.Ublk.Queues, "unset queues stay 0: nvmeublkd sizes the device")
	assert.Zero(t, cfg.NVMeoF.Ublk.Depth, "unset depth stays 0: nvmeublkd sizes the device")
	require.NotNil(t, cfg.NVMeoF.Ublk.ZeroCopy)
	assert.True(t, *cfg.NVMeoF.Ublk.ZeroCopy)
	require.NotNil(t, cfg.NVMeoF.Ublk.NapiUs)
	assert.Equal(t, 200, *cfg.NVMeoF.Ublk.NapiUs, "the measured busy-poll budget is the default")
	assert.Equal(t, 32, cfg.NVMeoF.Ublk.MaxVolumesPerNode)
	assert.Equal(t, 60, cfg.NVMeoF.Ublk.AttachTimeout)
	assert.Zero(t, cfg.nodeVolumeLimit(), "a kernel-default install advertises no volume limit")
}

func TestLoadConfigNVMeoFDataPathParses(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+nvmeofTestConfigBlock+`  dataPath: " UBLK "
  ublk:
    socketPath: /run/custom/d.sock
    queues: 4
    depth: 128
    zeroCopy: false
    napiUs: 0
    maxVolumesPerNode: 64
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
	require.NotNil(t, cfg.NVMeoF.Ublk.NapiUs)
	assert.Zero(t, *cfg.NVMeoF.Ublk.NapiUs, "an explicit 0 (busy polling off) must survive defaulting")
	assert.Equal(t, 64, cfg.NVMeoF.Ublk.MaxVolumesPerNode)
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
		{"negative volume budget", "  ublk:\n    maxVolumesPerNode: -1\n", "nvmeof.ublk.maxVolumesPerNode"},
		{"volume budget past the smallest layout", "  ublk:\n    maxVolumesPerNode: 129\n", "nvmeof.ublk.maxVolumesPerNode"},
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
	require.NotNil(t, got.NapiUs)
	assert.Equal(t, NVMeoFUblkConfig{
		SocketPath:        util.DefaultNVMeUblkSocket,
		ZeroCopy:          got.ZeroCopy,
		NapiUs:            got.NapiUs,
		MaxVolumesPerNode: 32,
		AttachTimeout:     60,
	}, got, "queues and depth stay 0: the daemon sizes the device")
	assert.True(t, *got.ZeroCopy)
	assert.Equal(t, 200, *got.NapiUs)

	off, napi := false, 50
	kept := NVMeoFUblkConfig{SocketPath: "/x.sock", Queues: 2, Depth: 16, ZeroCopy: &off, NapiUs: &napi, MaxVolumesPerNode: 8, AttachTimeout: 5}.withDefaults()
	assert.Equal(t, "/x.sock", kept.SocketPath)
	assert.Equal(t, 2, kept.Queues)
	assert.Equal(t, 16, kept.Depth)
	assert.False(t, *kept.ZeroCopy)
	assert.Equal(t, 50, *kept.NapiUs)
	assert.Equal(t, 8, kept.MaxVolumesPerNode)
	assert.Equal(t, 5, kept.AttachTimeout)

	assert.Equal(t, NVMeoFDataPathKernel, NVMeoFConfig{DataPath: "bogus"}.defaultDataPath(),
		"an invalid value never selects the userspace path")
}

// The CSI volume limit a node advertises: the explicit node setting first;
// otherwise the ublk volume budget, but only when ublk is what NVMe-oF volumes
// use by default (past that budget nvmeublkd refuses the attach).
func TestNodeVolumeLimit(t *testing.T) {
	ublkDefault := func(budget int) *Config {
		cfg := &Config{}
		cfg.NVMeoF.Enabled = true
		cfg.NVMeoF.DataPath = NVMeoFDataPathUblk
		cfg.NVMeoF.Ublk.MaxVolumesPerNode = budget
		return cfg
	}
	assert.Zero(t, (&Config{}).nodeVolumeLimit(), "nothing configured: unlimited")
	assert.Equal(t, int64(32), ublkDefault(0).nodeVolumeLimit(), "ublk by default: the default budget")
	assert.Equal(t, int64(64), ublkDefault(64).nodeVolumeLimit(), "ublk by default: the configured budget")

	explicit := ublkDefault(64)
	explicit.Node.MaxVolumesPerNode = 20
	assert.Equal(t, int64(20), explicit.nodeVolumeLimit(), "node.maxVolumesPerNode wins")

	optIn := &Config{}
	optIn.NVMeoF.Enabled = true
	optIn.NVMeoF.Ublk.Enabled = true
	optIn.NVMeoF.Ublk.MaxVolumesPerNode = 16
	assert.Zero(t, optIn.nodeVolumeLimit(), "opt-in classes do not cap the node: most volumes stay on the kernel path")

	copying := ublkDefault(16)
	off := false
	copying.NVMeoF.Ublk.ZeroCopy = &off
	assert.Zero(t, copying.nodeVolumeLimit(), "without zero copy the daemon's tables bound nothing")

	disabled := ublkDefault(16)
	disabled.NVMeoF.Enabled = false
	assert.Zero(t, disabled.nodeVolumeLimit(), "NVMe-oF off: the data path setting is not in use")
}
