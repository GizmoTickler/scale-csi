package util

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var (
	// nvme3n1 as the kernel numbers the namespace of the NEW connect...
	freshNVMe3n1 = unix.Mkdev(259, 4)
	// ...and the node the previous connect's nvme3n1 left behind.
	staleNVMe3n1 = unix.Mkdev(259, 1)
)

// nvmeFixture lays out a fake sysfs and /dev as they look right after a
// connect: the namespace's sysfs dev file carries the new number and
// /dev/nvme3n1 exists. With head, nvme3n1 is the native multipath head
// directly under nvme-subsys3; otherwise it is a namespace of controller
// nvme5 and the subsystem directory is absent (found through list-subsys).
func nvmeFixture(t *testing.T, nqn string, head bool) (subsystemRoot, controllerRoot, devRoot string) {
	t.Helper()
	root := t.TempDir()
	subsystemRoot = filepath.Join(root, "sys", "class", "nvme-subsystem")
	controllerRoot = filepath.Join(root, "sys", "class", "nvme")
	devRoot = filepath.Join(root, "dev")
	require.NoError(t, os.MkdirAll(subsystemRoot, 0o750))
	if head {
		subsystem := filepath.Join(subsystemRoot, "nvme-subsys3")
		writeSysfsDev(t, filepath.Join(subsystem, "nvme3n1"), freshNVMe3n1)
		require.NoError(t, os.WriteFile(filepath.Join(subsystem, "subsysnqn"), []byte(nqn+"\n"), 0o600))
	} else {
		require.NoError(t, os.MkdirAll(controllerRoot, 0o750))
		// A controller's namespace takes the controller's instance.
		writeSysfsDev(t, filepath.Join(controllerRoot, "nvme3", "nvme3n1"), freshNVMe3n1)
	}
	require.NoError(t, os.MkdirAll(devRoot, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "nvme3n1"), nil, 0o600))
	return subsystemRoot, controllerRoot, devRoot
}

// stubNVMePaths points the device wait's lookups at a fake sysfs and /dev.
func stubNVMePaths(t *testing.T, subsystemRoot, controllerRoot, devRoot string) {
	t.Helper()
	originalSubsystem, originalController, originalDev := nvmeSubsystemClassRoot, nvmeControllerClassRoot, nvmeDevRoot
	t.Cleanup(func() {
		nvmeSubsystemClassRoot, nvmeControllerClassRoot, nvmeDevRoot = originalSubsystem, originalController, originalDev
	})
	nvmeSubsystemClassRoot, nvmeControllerClassRoot, nvmeDevRoot = subsystemRoot, controllerRoot, devRoot
}

// nvmeStaleThenFresh reports the stale number for the first `stale` stats of
// nvme3n1, then the current one, counting every stat.
func nvmeStaleThenFresh(stale int32, stats *atomic.Int32) func(string) (uint64, bool) {
	return func(name string) (uint64, bool) {
		if name != "nvme3n1" {
			return 0, false
		}
		if stats.Add(1) <= stale {
			return staleNVMe3n1, true
		}
		return freshNVMe3n1, true
	}
}

func nvmeControllerSubsystems(nqn string) []NVMeSubsystem {
	return []NVMeSubsystem{{NQN: nqn, Name: "nvme-subsys3", Paths: []NVMePath{{Name: "nvme3", State: "live"}}}}
}

func TestFindNVMeDeviceRejectsStaleDevNode(t *testing.T) {
	const nqn = "nqn.2011-06.com.example:pvc-stale"
	for _, test := range []struct {
		name string
		head bool
		find func(subsystemRoot, controllerRoot, devRoot string) (string, error)
		dev  func(subsystemRoot, controllerRoot string) string
	}{
		{
			name: "subsystem multipath head",
			head: true,
			find: func(subsystemRoot, _, devRoot string) (string, error) {
				return findNVMeDeviceFromSysfsInPaths(nqn, subsystemRoot, devRoot)
			},
			dev: func(subsystemRoot, _ string) string {
				return filepath.Join(subsystemRoot, "nvme-subsys3", "nvme3n1")
			},
		},
		{
			name: "controller namespace",
			find: func(_, controllerRoot, devRoot string) (string, error) {
				return findNVMeNamespaceForController("nvme3", controllerRoot, devRoot)
			},
			dev: func(_, controllerRoot string) string {
				return filepath.Join(controllerRoot, "nvme3", "nvme3n1")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			subsystemRoot, controllerRoot, devRoot := nvmeFixture(t, nqn, test.head)
			sysDir := test.dev(subsystemRoot, controllerRoot)

			fixedBlockDeviceNumbers(t, map[string]uint64{"nvme3n1": staleNVMe3n1})
			devicePath, err := test.find(subsystemRoot, controllerRoot, devRoot)
			require.Error(t, err, "a /dev node left by the previous connect's namespace must not be used")
			assert.Empty(t, devicePath)

			fixedBlockDeviceNumbers(t, map[string]uint64{})
			_, err = test.find(subsystemRoot, controllerRoot, devRoot)
			require.Error(t, err, "a node that is not a block device must not be used")

			fixedBlockDeviceNumbers(t, map[string]uint64{"nvme3n1": freshNVMe3n1})
			require.NoError(t, os.Remove(filepath.Join(sysDir, "dev")))
			_, err = test.find(subsystemRoot, controllerRoot, devRoot)
			require.Error(t, err, "a namespace with no sysfs device number must not match")

			writeSysfsDev(t, sysDir, freshNVMe3n1)
			devicePath, err = test.find(subsystemRoot, controllerRoot, devRoot)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(devRoot, "nvme3n1"), devicePath)
		})
	}
}

// The handover race: the other plugin disconnected, this one connected, and
// /dev/nvme3n1 is still the previous namespace's node for a few polls. The
// wait must not hand that node to blkid or mkfs (ENXIO), but wait for the
// current one.
func TestWaitForNVMeDeviceWaitsForCurrentDevNode(t *testing.T) {
	const nqn = "nqn.2011-06.com.example:pvc-handover"
	for _, test := range []struct {
		name       string
		head       bool
		subsystems []NVMeSubsystem
	}{
		{name: "subsystem multipath head", head: true},
		{name: "controller from list-subsys", subsystems: nvmeControllerSubsystems(nqn)},
	} {
		t.Run(test.name, func(t *testing.T) {
			subsystemRoot, controllerRoot, devRoot := nvmeFixture(t, nqn, test.head)
			stubNVMePaths(t, subsystemRoot, controllerRoot, devRoot)
			var stats atomic.Int32
			stubBlockDeviceNumber(t, nvmeStaleThenFresh(3, &stats))

			devicePath, err := waitForNVMeDeviceWithSubsystems(context.Background(), nqn, 5*time.Second, test.subsystems, false)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(devRoot, "nvme3n1"), devicePath)
			assert.Equal(t, int32(4), stats.Load(), "the wait must poll past the stale node to the current one")
		})
	}
}

func TestWaitForNVMeDeviceTimesOutOnAStaleDevNode(t *testing.T) {
	const nqn = "nqn.2011-06.com.example:pvc-stale-forever"
	for _, test := range []struct {
		name       string
		head       bool
		subsystems []NVMeSubsystem
	}{
		{name: "subsystem multipath head", head: true},
		{name: "controller from list-subsys", subsystems: nvmeControllerSubsystems(nqn)},
	} {
		t.Run(test.name, func(t *testing.T) {
			subsystemRoot, controllerRoot, devRoot := nvmeFixture(t, nqn, test.head)
			stubNVMePaths(t, subsystemRoot, controllerRoot, devRoot)
			var stats atomic.Int32
			stubBlockDeviceNumber(t, nvmeStaleThenFresh(1<<30, &stats))

			devicePath, err := waitForNVMeDeviceWithSubsystems(context.Background(), nqn, 150*time.Millisecond, test.subsystems, false)
			require.Error(t, err)
			assert.Empty(t, devicePath)
			assert.EqualError(t, err, "timeout waiting for device (nqn="+nqn+")")
			assert.Greater(t, stats.Load(), int32(1), "the stale node must be re-checked until the timeout")
		})
	}
}
