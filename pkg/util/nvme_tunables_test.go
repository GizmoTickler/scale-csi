package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFakeNVMeController(t *testing.T, root, name string, attrs map[string]string) {
	t.Helper()
	dir := filepath.Join(root, name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	for key, value := range attrs {
		require.NoError(t, os.WriteFile(filepath.Join(dir, key), []byte(value+"\n"), 0o600))
	}
}

func TestListNVMeFabricsControllersAt(t *testing.T) {
	root := t.TempDir()
	writeFakeNVMeController(t, root, "nvme0", map[string]string{"transport": "pcie", "fast_io_fail_tmo": "off"})
	writeFakeNVMeController(t, root, "nvme1", map[string]string{
		"transport": "tcp", "address": "traddr=192.0.2.10,trsvcid=4420", "subsysnqn": "nqn.a", "fast_io_fail_tmo": "off",
	})
	writeFakeNVMeController(t, root, "nvme2", map[string]string{
		"transport": "tcp", "address": "traddr=192.0.2.11,trsvcid=4420", "subsysnqn": "nqn.a", "fast_io_fail_tmo": "15",
	})
	// No fast_io_fail_tmo attribute: an old kernel cannot be reconciled.
	writeFakeNVMeController(t, root, "nvme3", map[string]string{"transport": "tcp", "subsysnqn": "nqn.b"})
	// Not a controller entry.
	writeFakeNVMeController(t, root, "other", map[string]string{"transport": "tcp", "fast_io_fail_tmo": "off"})

	controllers, err := listNVMeFabricsControllersAt(root)
	require.NoError(t, err)
	require.Len(t, controllers, 2)
	assert.Equal(t, NVMeFabricsController{Name: "nvme1", Transport: "tcp", Address: "traddr=192.0.2.10,trsvcid=4420", SubsysNQN: "nqn.a", FastIOFailTmo: -1}, controllers[0])
	assert.Equal(t, 15, controllers[1].FastIOFailTmo)
}

func TestListNVMeFabricsControllersMissingRootIsEmpty(t *testing.T) {
	controllers, err := listNVMeFabricsControllersAt(filepath.Join(t.TempDir(), "absent"))
	require.NoError(t, err)
	assert.Empty(t, controllers)
}

func TestSetNVMeControllerFastIOFailTmoAt(t *testing.T) {
	root := t.TempDir()
	writeFakeNVMeController(t, root, "nvme4", map[string]string{"fast_io_fail_tmo": "off"})

	require.NoError(t, setNVMeControllerFastIOFailTmoAt(root, "nvme4", 15))
	data, err := os.ReadFile(filepath.Join(root, "nvme4", "fast_io_fail_tmo"))
	require.NoError(t, err)
	assert.Equal(t, "15", string(data))

	require.NoError(t, setNVMeControllerFastIOFailTmoAt(root, "nvme4", -7))
	data, err = os.ReadFile(filepath.Join(root, "nvme4", "fast_io_fail_tmo"))
	require.NoError(t, err)
	assert.Equal(t, "-1", string(data), "any negative value disables the timeout")

	for _, bad := range []string{"", ".", "..", "../nvme4", "sda", "nvme4/../x"} {
		assert.Error(t, setNVMeControllerFastIOFailTmoAt(root, bad, 15), bad)
	}
	assert.Error(t, setNVMeControllerFastIOFailTmoAt(root, "nvme9", 15), "a missing controller is an error, never a create")
}

// GetBlockDeviceMounts must keep every mount point of a device: a published
// filesystem volume appears at its kubelet staging path AND its pod bind mount,
// and an ownership check keyed on the staging path must still see it when the
// pod mount is listed second.
func TestGetBlockDeviceMountsKeepsEveryMountPoint(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' " +
		"'/dev/nvme0n1 /var/lib/kubelet/plugins/kubernetes.io/csi/csi.scale.io/h/globalmount' " +
		"'/dev/nvme0n1 /var/lib/kubelet/pods/p/volumes/kubernetes.io~csi/pvc/mount' " +
		"'tmpfs /run'\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "findmnt"), []byte(script), 0o700)) //nolint:gosec // test-only executable stub
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	mounts, err := GetBlockDeviceMounts()
	require.NoError(t, err)
	assert.Equal(t, map[string][]string{"/dev/nvme0n1": {
		"/var/lib/kubelet/plugins/kubernetes.io/csi/csi.scale.io/h/globalmount",
		"/var/lib/kubelet/pods/p/volumes/kubernetes.io~csi/pvc/mount",
	}}, mounts)

	single, err := GetMountedBlockDevices()
	require.NoError(t, err)
	assert.Len(t, single, 1, "the single-target inventory keeps its historical shape")
}
