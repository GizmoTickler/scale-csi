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
	name, paths, isMap, err := multipathPathsIn("/dev/dm-3", block, "/dev")
	require.NoError(t, err)
	assert.True(t, isMap)
	assert.Equal(t, "mpatha", name)
	assert.Equal(t, []string{"/dev/sdb", "/dev/sdc"}, paths)

	_, _, isMap, err = multipathPathsIn("/dev/sdb", block, "/dev")
	require.NoError(t, err)
	assert.False(t, isMap)
}
