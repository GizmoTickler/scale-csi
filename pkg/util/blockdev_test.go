package util

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// stubBlockDeviceNumber replaces the stat seam: a path reports number(name),
// where name is the base name of the path with symlinks resolved, or is not a
// block device when number reports false. Fixtures cannot mknod.
func stubBlockDeviceNumber(t *testing.T, number func(name string) (uint64, bool)) {
	t.Helper()
	original := blockDeviceNumber
	t.Cleanup(func() { blockDeviceNumber = original })
	blockDeviceNumber = func(path string) (uint64, error) {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return 0, err
		}
		n, ok := number(filepath.Base(resolved))
		if !ok {
			return 0, fmt.Errorf("%s is not a block device", path)
		}
		return n, nil
	}
}

// fixedBlockDeviceNumbers is the stat seam for fixtures whose nodes are all
// current: each named node reports its number.
func fixedBlockDeviceNumbers(t *testing.T, numbers map[string]uint64) {
	t.Helper()
	stubBlockDeviceNumber(t, func(name string) (uint64, bool) {
		n, ok := numbers[name]
		return n, ok
	})
}

// writeSysfsDev writes the kernel's "MAJ:MIN\n" dev file for a disk.
func writeSysfsDev(t *testing.T, dir string, number uint64) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dev"),
		fmt.Appendf(nil, "%d:%d\n", unix.Major(number), unix.Minor(number)), 0o600))
}
