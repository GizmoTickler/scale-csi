package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The StorageClass parameter lands in the volume context verbatim (after
// canonicalization), costs no API call, and is replayed identically on the
// already-exists path.
func TestCreateVolumeRecordsNVMeoFDataPath(t *testing.T) {
	const singlePathCalls = 12
	tests := []struct {
		name      string
		param     *string
		ublkOptIn bool
		want      string
		wantKey   bool
	}{
		{name: "absent parameter leaves the volume context unchanged", wantKey: false},
		{name: "kernel is pinned explicitly", param: ptrTo("kernel"), want: NVMeoFDataPathKernel, wantKey: true},
		{name: "ublk is pinned when enabled", param: ptrTo(" UBLK "), ublkOptIn: true, want: NVMeoFDataPathUblk, wantKey: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newAPICallCountingClient()
			d := newMultipathAPICallCountDriver(t, client, nil)
			d.config.NVMeoF.Ublk.Enabled = tc.ublkOptIn
			req := apiCallCountVolumeRequest("dp-volume", "nvmeof")
			if tc.param != nil {
				req.Parameters[nvmeoFDataPathKey] = *tc.param
			}
			resp, err := d.CreateVolume(context.Background(), req)
			require.NoError(t, err)
			assertAPICallCount(t, "CreateVolume fresh NVMe-oF with data path", client, singlePathCalls)
			got, ok := resp.GetVolume().GetVolumeContext()[nvmeoFDataPathKey]
			assert.Equal(t, tc.wantKey, ok)
			assert.Equal(t, tc.want, got)

			replay, err := d.CreateVolume(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, resp.GetVolume().GetVolumeContext(), replay.GetVolume().GetVolumeContext(),
				"an idempotent replay must return the same volume context")
		})
	}
}

func TestCreateVolumeRejectsInvalidNVMeoFDataPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{"unknown value", "spdk", "must be \"kernel\" or \"ublk\""},
		{"empty value", "  ", "must be \"kernel\" or \"ublk\""},
		{"ublk without the feature enabled", "ublk", "requires the ublk data path to be enabled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newAPICallCountingClient()
			d := newMultipathAPICallCountDriver(t, client, nil)
			req := apiCallCountVolumeRequest("dp-bad", "nvmeof")
			req.Parameters[nvmeoFDataPathKey] = tc.value
			_, err := d.CreateVolume(context.Background(), req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, err.Error(), tc.want)
			assertAPICallCount(t, "rejected data path", client, 0)
		})
	}
}

func TestNVMeoFDataPathForCreateRejectsOtherProtocols(t *testing.T) {
	d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{Ublk: NVMeoFUblkConfig{Enabled: true}}}}
	for _, shareType := range []ShareType{ShareTypeNFS, ShareTypeISCSI} {
		_, err := d.nvmeoFDataPathForCreate(map[string]string{nvmeoFDataPathKey: "ublk"}, shareType)
		require.Error(t, err, shareType)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "applies only to protocol nvmeof")

		dataPath, err := d.nvmeoFDataPathForCreate(map[string]string{"protocol": string(shareType)}, shareType)
		require.NoError(t, err, "an unset parameter is a no-op for every protocol")
		assert.Empty(t, dataPath)
	}
}

func TestNVMeoFDataPathForVolume(t *testing.T) {
	tests := []struct {
		name          string
		defaultPath   string
		volumeContext map[string]string
		want          string
		wantErr       bool
	}{
		{name: "no pin, empty default is kernel", volumeContext: map[string]string{}, want: NVMeoFDataPathKernel},
		{name: "no pin follows the ublk default", defaultPath: "ublk", volumeContext: map[string]string{}, want: NVMeoFDataPathUblk},
		{name: "kernel pin beats a ublk default", defaultPath: "ublk", volumeContext: map[string]string{nvmeoFDataPathKey: "kernel"}, want: NVMeoFDataPathKernel},
		{name: "ublk pin beats a kernel default", defaultPath: "kernel", volumeContext: map[string]string{nvmeoFDataPathKey: "ublk"}, want: NVMeoFDataPathUblk},
		{name: "malformed pin fails closed", volumeContext: map[string]string{nvmeoFDataPathKey: "fast"}, wantErr: true},
		{name: "empty pin fails closed", volumeContext: map[string]string{nvmeoFDataPathKey: ""}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &Driver{config: &Config{NVMeoF: NVMeoFConfig{DataPath: tc.defaultPath}}}
			got, err := d.nvmeoFDataPathForVolume(tc.volumeContext)
			if tc.wantErr {
				require.Error(t, err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func ptrTo[T any](v T) *T { return &v }
