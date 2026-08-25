package driver

import (
	"bytes"
	"context"
	"flag"
	"io"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func TestDeleteVolumeLogsDatasetAttachmentsAndProcessesAndProceeds(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	const datasetName = "pool/parent/busy-volume"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	nfs := "nfs"
	cmdline := "/usr/sbin/smbd --foreground"
	client.DatasetAttachmentValues[datasetName] = []truenas.DatasetAttachment{{
		Type: "NFS Share", Service: &nfs, Attachments: []string{"/mnt/pool/parent/busy-volume"},
	}}
	client.DatasetProcessValues[datasetName] = []truenas.DatasetProcess{{
		PID: 2520, Name: "smbd", Cmdline: &cmdline,
	}}
	attachmentBefore := testutil.ToFloat64(datasetBusyObservationsTotal.WithLabelValues("attachment"))
	processBefore := testutil.ToFloat64(datasetBusyObservationsTotal.WithLabelValues("process"))

	logged := captureV2Klog(t, func() {
		_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "busy-volume"})
	})
	require.NoError(t, err)
	_, err = client.DatasetGet(ctx, datasetName)
	assert.True(t, truenas.IsNotFoundError(err), "the observability check must not block deletion")
	assert.Contains(t, logged, "Dataset pool/parent/busy-volume is busy before DeleteVolume delete")
	assert.Contains(t, logged, `attachment type="NFS Share" service="nfs"`)
	assert.Contains(t, logged, "/mnt/pool/parent/busy-volume")
	assert.Contains(t, logged, `process pid=2520 name="smbd"`)
	assert.Contains(t, logged, cmdline)
	assert.Equal(t, attachmentBefore+1, testutil.ToFloat64(datasetBusyObservationsTotal.WithLabelValues("attachment")))
	assert.Equal(t, processBefore+1, testutil.ToFloat64(datasetBusyObservationsTotal.WithLabelValues("process")))
}

func TestDeleteVolumeDatasetActivityQueryErrorsDoNotFailDelete(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	const datasetName = "pool/parent/query-error-volume"
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	client.DatasetAttachmentsErr = assert.AnError
	client.DatasetProcessesErr = assert.AnError

	logged := captureV2Klog(t, func() {
		_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "query-error-volume"})
	})
	require.NoError(t, err)
	_, err = client.DatasetGet(ctx, datasetName)
	assert.True(t, truenas.IsNotFoundError(err), "query failures must remain best-effort")
	assert.Equal(t, []string{datasetName}, client.DatasetAttachmentCalls)
	assert.Equal(t, []string{datasetName}, client.DatasetProcessCalls)
	assert.Contains(t, logged, "Could not inspect dataset pool/parent/query-error-volume attachments before DeleteVolume delete")
	assert.Contains(t, logged, "Could not inspect dataset pool/parent/query-error-volume processes before DeleteVolume delete")
}

func TestReconcileVolumeReapLogsDatasetActivityAndProceeds(t *testing.T) {
	ctx := context.Background()
	d, client := newReconcileTestDriver(t, false,
		[]runtime.Object{reconcilePV("live-volume", "csi.scale.io")}, nil,
	)
	old := time.Now().Add(-48 * time.Hour)
	addReconcileDataset(client, "live-volume", old, true, 100)
	addReconcileDataset(client, "busy-orphan", old, true, 100)
	smb := "cifs"
	cmdline := "/usr/sbin/smbd --foreground"
	client.DatasetProcessValues["pool/parent/busy-orphan"] = []truenas.DatasetProcess{{
		PID: 97778, Name: "smbd", Service: &smb, Cmdline: &cmdline,
	}}

	var report ReconcileReport
	var err error
	logged := captureV2Klog(t, func() {
		report, err = d.ReconcileOrphans(ctx, ReconcileOptions{Delete: true, MinOrphanAge: time.Hour})
	})
	require.NoError(t, err)
	assert.Contains(t, report.DeletedVolumes, "busy-orphan")
	assert.Contains(t, client.DatasetProcessCalls, "pool/parent/busy-orphan")
	assert.Contains(t, logged, `process pid=97778 name="smbd" service="cifs"`)
}

func captureV2Klog(t *testing.T, fn func()) string {
	t.Helper()
	if flag.Lookup("v") == nil {
		klog.InitFlags(nil)
	}
	verbosity := flag.Lookup("v")
	originalVerbosity := verbosity.Value.String()
	require.NoError(t, verbosity.Value.Set("2"))

	var buf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(io.Discard)
	klog.SetOutputBySeverity("INFO", &buf)
	t.Cleanup(func() {
		klog.Flush()
		_ = verbosity.Value.Set(originalVerbosity)
		klog.SetOutputBySeverity("INFO", io.Discard)
		klog.LogToStderr(true)
	})
	fn()
	klog.Flush()
	return buf.String()
}
