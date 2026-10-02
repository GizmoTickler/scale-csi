package util

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// writeFakeMultipathd installs a fake multipathd built from script at the
// front of PATH, under t.TempDir() (see writeFakeISCSIAdm).
func writeFakeMultipathd(t *testing.T, script string) {
	t.Helper()
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "multipathd"), []byte(script), 0o750)) //nolint:gosec // must be executable to stand in as a fake multipathd this test execs
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestMultipathResizeMapWithContext(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		argsFile := filepath.Join(t.TempDir(), "args")
		writeFakeMultipathd(t, "#!/bin/sh\nprintf '%s' \"$*\" > '"+argsFile+"'\necho ok\nexit 0\n")
		require.NoError(t, MultipathResizeMapWithContext(context.Background(), "mpatha"))
		args, err := os.ReadFile(argsFile)
		require.NoError(t, err)
		assert.Equal(t, "resize map mpatha", string(args))
	})

	t.Run("non-zero exit", func(t *testing.T) {
		writeFakeMultipathd(t, "#!/bin/sh\necho 'map not found'\nexit 1\n")
		err := MultipathResizeMapWithContext(context.Background(), "mpatha")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multipathd resize map mpatha failed")
		assert.Contains(t, err.Error(), "exit status 1")
		assert.Contains(t, err.Error(), "map not found")
	})

	t.Run("exit 0 printing fail", func(t *testing.T) {
		// multipathd reports a refused resize on stdout with exit status 0.
		writeFakeMultipathd(t, "#!/bin/sh\necho 'FAIL'\nexit 0\n")
		err := MultipathResizeMapWithContext(context.Background(), "mpatha")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "multipathd resize map mpatha failed, output: FAIL")
	})

	t.Run("canceled context", func(t *testing.T) {
		writeFakeMultipathd(t, "#!/bin/sh\nexit 0\n")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, MultipathResizeMapWithContext(ctx, "mpatha"), context.Canceled)
	})
}

// The real stat seam rejects anything that is not a block device node and a
// path that does not exist.
func TestBlockDeviceNumberDefault(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "sdb")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	_, err := blockDeviceNumber(file)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a block device")

	_, err = blockDeviceNumber(filepath.Join(dir, "absent"))
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err), "a missing node is ENOENT: %v", err)

	// A real block device node, when the host has one, reports the kernel's
	// number for it (a stat only; nothing is opened).
	for _, sysDev := range globSysBlockDevs() {
		want, readErr := os.ReadFile(sysDev)
		if readErr != nil {
			continue
		}
		node := filepath.Join("/dev", filepath.Base(filepath.Dir(sysDev)))
		if _, statErr := os.Stat(node); statErr != nil {
			continue
		}
		number, err := blockDeviceNumber(node)
		require.NoError(t, err, node)
		assert.Equal(t, strings.TrimSpace(string(want)), fmt.Sprintf("%d:%d", unix.Major(number), unix.Minor(number)), node)
		assert.True(t, isCurrentBlockDeviceNode(node, sysDev), node)
		return
	}
	t.Log("no block device node on this host; the success path is covered by the stubbed seam tests")
}

func globSysBlockDevs() []string {
	matches, _ := filepath.Glob("/sys/class/block/*/dev")
	return matches
}

// A sysfs dev file that is not "MAJ:MIN" never matches.
func TestIsCurrentBlockDeviceNodeRejectsAMalformedSysfsDev(t *testing.T) {
	fixedBlockDeviceNumbers(t, map[string]uint64{"sdb": freshSDB})
	dir := t.TempDir()
	node := filepath.Join(dir, "sdb")
	require.NoError(t, os.WriteFile(node, nil, 0o600))
	sysDev := filepath.Join(dir, "dev")
	require.NoError(t, os.WriteFile(sysDev, []byte("garbage\n"), 0o600))
	assert.False(t, isCurrentBlockDeviceNode(node, sysDev))
	assert.False(t, isCurrentBlockDeviceNode(node, filepath.Join(dir, "absent")))
}

func TestFindISCSIDeviceWithoutASessionIsNotFound(t *testing.T) {
	if matches, _ := filepath.Glob("/sys/class/iscsi_session/session*"); len(matches) > 0 {
		t.Skip("host has iSCSI sessions")
	}
	_, err := findISCSIDevice("iqn.2005-10.org.freenas.ctl:no-such-target", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "device not found")
}

// A plain disk name is never a map, whatever the host's sysfs holds.
func TestMultipathPathsPlainDiskIsNotAMap(t *testing.T) {
	name, paths, isMap, err := MultipathPaths(filepath.Join(t.TempDir(), "sdzz"))
	require.NoError(t, err)
	assert.False(t, isMap)
	assert.Empty(t, name)
	assert.Empty(t, paths)
}

func TestMultipathPathsErrors(t *testing.T) {
	sys := t.TempDir()
	block := filepath.Join(sys, "block")
	devDir := filepath.Join(sys, "dev")
	require.NoError(t, os.MkdirAll(devDir, 0o750))

	// A symlink (a /dev/disk/by-id link) is resolved to the dm node first.
	node := filepath.Join(devDir, "dm-7")
	require.NoError(t, os.WriteFile(node, nil, 0o600))
	link := filepath.Join(sys, "by-id-link")
	require.NoError(t, os.Symlink(node, link))

	// An unreadable dm UUID (here a directory) is an error, not "not a map".
	require.NoError(t, os.MkdirAll(filepath.Join(block, "dm-7", "dm", "uuid"), 0o750))
	_, _, isMap, err := multipathPathsIn(link, block, devDir)
	require.Error(t, err)
	assert.False(t, isMap)
	assert.Contains(t, err.Error(), "failed to read the dm UUID of "+node)

	// A map with no name.
	require.NoError(t, os.Remove(filepath.Join(block, "dm-7", "dm", "uuid")))
	require.NoError(t, os.WriteFile(filepath.Join(block, "dm-7", "dm", "uuid"), []byte("mpath-36589cfc000000a1b\n"), 0o600))
	_, _, isMap, err = multipathPathsIn(link, block, devDir)
	require.Error(t, err)
	assert.True(t, isMap)
	assert.Contains(t, err.Error(), "failed to read the map name")

	// A map whose slaves cannot be listed.
	require.NoError(t, os.WriteFile(filepath.Join(block, "dm-7", "dm", "name"), []byte("mpathb\n"), 0o600))
	_, _, isMap, err = multipathPathsIn(link, block, devDir)
	require.Error(t, err)
	assert.True(t, isMap)
	assert.True(t, strings.Contains(err.Error(), "failed to inspect dm-multipath slaves"), err.Error())
}
