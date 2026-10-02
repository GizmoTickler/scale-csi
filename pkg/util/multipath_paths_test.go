package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipathPathsReadsTheMapNameAndItsPaths(t *testing.T) {
	sys := t.TempDir()
	block := filepath.Join(sys, "block")
	for _, dir := range []string{"dm-3/dm", "dm-3/slaves/sdb", "dm-3/slaves/sdc", "sdb", "sdc"} {
		require.NoError(t, os.MkdirAll(filepath.Join(block, dir), 0o750))
	}
	require.NoError(t, os.WriteFile(filepath.Join(block, "dm-3/dm/name"), []byte("mpatha\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(block, "dm-3/dm/uuid"), []byte("mpath-36589cfc000000a1b\n"), 0o600))
	name, paths, isMap, err := multipathPathsIn("/dev/dm-3", block, "/dev")
	require.NoError(t, err)
	assert.True(t, isMap)
	assert.Equal(t, "mpatha", name)
	assert.Equal(t, []string{"/dev/sdb", "/dev/sdc"}, paths)

	_, _, isMap, err = multipathPathsIn("/dev/sdb", block, "/dev")
	require.NoError(t, err)
	assert.False(t, isMap)
}

// Only a dm-multipath map counts: a kpartx partition of a map or an LVM volume
// is a dm device too, with slaves, and takes the single-device rescan path.
func TestMultipathPathsOnlyCountsADMMultipathMap(t *testing.T) {
	sys := t.TempDir()
	block := filepath.Join(sys, "block")
	for dm, uuid := range map[string]string{
		"dm-4": "part1-mpath-36589cfc000000a1b\n",
		"dm-5": "LVM-aBcDeF0123456789\n",
		"dm-6": "",
	} {
		for _, dir := range []string{dm + "/dm", dm + "/slaves/dm-3"} {
			require.NoError(t, os.MkdirAll(filepath.Join(block, dir), 0o750))
		}
		require.NoError(t, os.WriteFile(filepath.Join(block, dm, "dm/name"), []byte("other\n"), 0o600))
		if uuid != "" {
			require.NoError(t, os.WriteFile(filepath.Join(block, dm, "dm/uuid"), []byte(uuid), 0o600))
		}
		name, paths, isMap, err := multipathPathsIn("/dev/"+dm, block, "/dev")
		require.NoError(t, err, dm)
		assert.False(t, isMap, dm)
		assert.Empty(t, name, dm)
		assert.Empty(t, paths, dm)
	}
}
