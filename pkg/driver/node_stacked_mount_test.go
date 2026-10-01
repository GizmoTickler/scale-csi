package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A second mount stacked under the staging or target path (two node plugins
// staging during a handover) survives one umount. What is left mounted is a
// live share or filesystem: nothing under it may be deleted.
func TestNodeUnstageNeverDeletesThroughAStackedMount(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount", "umount")
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "mounted")
	t.Setenv("FAKE_NODE_STACKED_MOUNT", "1") // still mounted after every umount
	stagingPath := filepath.Join(t.TempDir(), "globalmount")
	require.NoError(t, os.MkdirAll(stagingPath, 0o750))
	file := filepath.Join(stagingPath, "data-on-the-share")
	require.NoError(t, os.WriteFile(file, []byte("keep"), 0o600))

	d := newTestNodeDriver(ShareTypeNFS)
	_, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "pvc-nfs-1",
		StagingTargetPath: stagingPath,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.FileExists(t, file, "a file on the still-mounted share was deleted")
}

func TestNodeUnpublishNeverDeletesThroughAStackedMount(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount", "umount")
	t.Setenv("FAKE_NODE_FINDMNT_OUTPUT", "mounted")
	t.Setenv("FAKE_NODE_STACKED_MOUNT", "1")
	targetPath := filepath.Join(t.TempDir(), "pod", "mount")
	require.NoError(t, os.MkdirAll(targetPath, 0o750))
	file := filepath.Join(targetPath, "data-on-the-share")
	require.NoError(t, os.WriteFile(file, []byte("keep"), 0o600))

	d := newTestNodeDriver(ShareTypeNFS)
	_, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "pvc-nfs-1",
		TargetPath: targetPath,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.FileExists(t, file, "a file on the still-mounted share was deleted")
}

// A directory left with files in it after the unmount is not ours to empty.
func TestNodeUnpublishLeavesANonEmptyUnmountedDirectory(t *testing.T) {
	installFakeNodeCommands(t, "findmnt", "mount", "umount")
	targetPath := filepath.Join(t.TempDir(), "pod", "mount")
	require.NoError(t, os.MkdirAll(targetPath, 0o750))
	file := filepath.Join(targetPath, "left-behind")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	d := newTestNodeDriver(ShareTypeNFS)
	_, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "pvc-nfs-1",
		TargetPath: targetPath,
	})
	require.NoError(t, err)
	assert.FileExists(t, file)
}
