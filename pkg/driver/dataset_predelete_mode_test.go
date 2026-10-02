package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// failingDeleteClient fails every dataset delete with a fixed error.
type failingDeleteClient struct {
	*apiCallCountingClient
	deleteErr error
}

func (c *failingDeleteClient) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	c.record("DatasetDelete")
	return c.deleteErr
}

// Batch 4.2: in on-failure mode (the default) the busy scans run after a
// delete fails with anything but a plain snapshot/children dependency, and not
// otherwise; in always mode they run before the delete as before.
func TestBusyObservationOnFailureRunsOnlyWhenTheDeleteFails(t *testing.T) {
	ctx := context.Background()
	busy := &truenas.APIError{Code: -32001, Message: "Method call error", Data: map[string]interface{}{"reason": "[EBUSY] cannot destroy 'pool/parent/x': dataset is busy"}}
	for _, tc := range []struct {
		name string
		mode BusyObservationMode
		err  error
		want int
	}{
		{"default mode, busy failure", "", busy, 1},
		{"on-failure, busy failure", BusyObservationOnFailure, busy, 1},
		{"on-failure, unclassified failure", BusyObservationOnFailure, errors.New("connection reset"), 1},
		{"on-failure, snapshot dependency", BusyObservationOnFailure, errors.New("dataset has dependent snapshots"), 0},
		{"on-failure, already gone", BusyObservationOnFailure, &truenas.APIError{Code: 2, Message: "dataset does not exist (ENOENT)"}, 0},
		{"on-failure, success", BusyObservationOnFailure, nil, 0},
		{"never, busy failure", BusyObservationNever, busy, 0},
		{"always, busy failure", BusyObservationAlways, busy, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counting := newAPICallCountingClient()
			client := &failingDeleteClient{apiCallCountingClient: counting, deleteErr: tc.err}
			d := &Driver{config: &Config{ZFS: ZFSConfig{ObserveBusyBeforeDelete: tc.mode}}, truenasClient: client}
			_, err := counting.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/x", Type: "VOLUME"})
			require.NoError(t, err)
			counting.resetCalls()
			gotErr := d.deleteDatasetWithBusyObservation(ctx, "pool/parent/x", false, true, "DeleteVolume")
			assert.Equal(t, tc.err, gotErr, "the delete's own result is returned unchanged")
			_, methods := counting.callSnapshot()
			assert.Equal(t, tc.want, methods["DatasetAttachments"])
			assert.Equal(t, tc.want, methods["DatasetProcesses"])
			assert.Equal(t, 1, methods["DatasetDelete"])
		})
	}
}

// A caller that has given up is not observed after the failure.
func TestBusyObservationOnFailureSkipsACanceledCaller(t *testing.T) {
	counting := newAPICallCountingClient()
	client := &failingDeleteClient{apiCallCountingClient: counting, deleteErr: errors.New("dataset is busy")}
	d := &Driver{config: &Config{}, truenasClient: client}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = d.deleteDatasetWithBusyObservation(ctx, "pool/parent/x", false, true, "DeleteVolume")
	_, methods := counting.callSnapshot()
	assert.Zero(t, methods["DatasetAttachments"])
}

// End to end: a DeleteVolume whose dataset delete fails busy is observed once,
// after the failure, in the default mode.
func TestDeleteVolumeBusyFailureIsObservedByDefault(t *testing.T) {
	ctx := context.Background()
	counting := newAPICallCountingClient()
	d := newMultipathAPICallCountDriver(t, counting, nil)
	resp, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("observed-busy", "nvmeof"))
	require.NoError(t, err)
	d.truenasClient = &failingDeleteClient{apiCallCountingClient: counting, deleteErr: errors.New("cannot destroy: dataset is busy")}
	counting.resetCalls()
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: resp.GetVolume().GetVolumeId()})
	require.Error(t, err)
	_, methods := counting.callSnapshot()
	assert.Equal(t, 1, methods["DatasetAttachments"])
	assert.Equal(t, 1, methods["DatasetProcesses"])
}

const busyModeTestProtocol = `nfs:
  enabled: true
  shareHost: 192.0.2.10
`

func TestObserveBusyBeforeDeleteConfigForms(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  BusyObservationMode
	}{
		{"", BusyObservationOnFailure},
		{"true", BusyObservationAlways},
		{"false", BusyObservationNever},
		{"always", BusyObservationAlways},
		{"on-failure", BusyObservationOnFailure},
		{`"on-failure"`, BusyObservationOnFailure},
		{"never", BusyObservationNever},
	} {
		t.Run(tc.value, func(t *testing.T) {
			body := requiredTestConfig
			if tc.value != "" {
				body += "  observeBusyBeforeDelete: " + tc.value + "\n"
			}
			body += busyModeTestProtocol
			cfg, err := loadTestConfig(t, body)
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.busyObservationMode())
		})
	}
	for _, bad := range []string{"sometimes", "1", "[always]", `"true"`} {
		t.Run("rejects "+bad, func(t *testing.T) {
			_, err := loadTestConfig(t, requiredTestConfig+"  observeBusyBeforeDelete: "+bad+"\n"+busyModeTestProtocol)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "observeBusyBeforeDelete")
		})
	}
}
