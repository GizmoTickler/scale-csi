package driver

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A live raw-block publication is the device node bind mounted over kubelet's
// placeholder file; stat through the mount reports a block device. Before this
// was accepted, every replay of a live raw-block NodePublishVolume returned
// Internal ("unsupported type Drw-rw----"), seen on a live node.
func TestAccessTypeForMode(t *testing.T) {
	cases := []struct {
		name string
		mode os.FileMode
		want nodeAccessType
	}{
		{"placeholder file", 0o640, nodeAccessBlock},
		{"bound block device", os.ModeDevice | 0o660, nodeAccessBlock},
		{"directory", os.ModeDir | 0o750, nodeAccessMount},
	}
	for _, tc := range cases {
		got, err := accessTypeForMode("/target", tc.mode)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}
	for _, mode := range []os.FileMode{os.ModeDevice | os.ModeCharDevice | 0o666, os.ModeSymlink | 0o777, os.ModeNamedPipe | 0o600} {
		_, err := accessTypeForMode("/target", mode)
		assert.Error(t, err, "%s is not a publish target", mode)
	}
}
