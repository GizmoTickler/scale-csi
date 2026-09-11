package driver

import (
	"bytes"
	"context"
	"flag"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// TestDeleteOrphanedSharesHonorsTheRelistedKubernetesState is the regression for
// the one delete path that was never given the re-listed currentState that
// runReconcileDeletePhase exists to produce. Volume orphans get three gates
// against it; shares got none, so a PersistentVolume that appeared between
// detection and mutation could not stop the sweep. An absent dataset under a
// live PV is the anomaly shareOrphanLivePV exists to SURFACE, not to "fix" by
// deleting the share.
func TestDeleteOrphanedSharesHonorsTheRelistedKubernetesState(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	_, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)

	// Detection sees no live PV for the volume.
	detectionState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, detectionState, &report)
	require.Len(t, report.OrphanShares, 1)

	// The re-list immediately before mutation DOES see one.
	currentState := &kubernetesReconcileState{
		volumeHandles: map[string]struct{}{"gone-volume": {}},
	}
	d.deleteOrphanedShares(ctx, &report, currentState, 0, 5)

	assert.Empty(t, report.DeletedShares, "a share whose volume regained a live PV must not be swept")
	require.Len(t, report.SkippedDeletes, 1)
	assert.Equal(t, "share", report.SkippedDeletes[0].Kind)
	assert.Contains(t, report.SkippedDeletes[0].Reason, "live PersistentVolume")

	shares, err := client.NFSShareList(ctx)
	require.NoError(t, err)
	assert.Len(t, shares, 1, "the backend share must survive")
}

// TestOrphanShareSweepISCSIUsesClassifiedBackendIDNotDerivedName is the
// regression for the false-"already absent" leak. The deleter re-derived the
// target and extent from iscsiShareName(SourceVolumeID), which is a function of
// iscsi.nameSuffix — so a suffix change between the sweep that classified the
// orphan and the one that deletes it resolved nothing, and the deleter logged
// "already absent", counted the orphan in DeletedShares as a success, and
// recorded no failure metric while the real orphan leaked forever.
func TestOrphanShareSweepISCSIUsesClassifiedBackendIDNotDerivedName(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	createISCSIShareFixture(t, ctx, client, d, "gone-volume", "pool/parent/gone-volume")
	originalName := d.iscsiShareName("gone-volume")

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)
	require.NotEmpty(t, report.OrphanShares[0].BackendID)

	// An operator changes iscsi.nameSuffix between passes. Every object on the
	// appliance keeps the name it was created with.
	d.config.ISCSI.NameSuffix = "-rev2"
	require.NotEqual(t, originalName, d.iscsiShareName("gone-volume"))

	failuresBefore := testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share"))
	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)

	assert.Equal(t, failuresBefore, testutil.ToFloat64(reconcileFailuresTotal.WithLabelValues("share")))
	require.Len(t, report.DeletedShares, 1)

	extents, err := client.ISCSIExtentList(ctx)
	require.NoError(t, err)
	assert.Empty(t, extents, "the classified extent must be deleted by ID, not left behind by a stale name lookup")

	target, err := client.ISCSITargetFindByName(ctx, originalName)
	require.NoError(t, err)
	assert.Nil(t, target, "the target must be reached through the target-extent association, not the derived name")
}

// TestOrphanShareSweepNVMeoFUsesClassifiedBackendIDNotDerivedName is the
// NVMe-oF half of the same defect: nvmeSubsystemName depends on both
// nvmeof.namePrefix and nvmeof.nameSuffix.
func TestOrphanShareSweepNVMeoFUsesClassifiedBackendIDNotDerivedName(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	subsys, err := client.NVMeoFSubsystemCreate(ctx, d.nvmeSubsystemName("pool/parent/gone-volume"), true, nil)
	require.NoError(t, err)
	_, err = client.NVMeoFNamespaceCreate(ctx, subsys.ID, "zvol/pool/parent/gone-volume", "ZVOL")
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)
	require.Len(t, report.OrphanShares, 1)

	d.config.NVMeoF.NameSuffix = "-rev2"

	d.deleteOrphanedShares(ctx, &report, kubeState, 0, 5)
	require.Len(t, report.DeletedShares, 1)

	remaining, err := client.NVMeoFSubsystemGet(ctx, subsys.ID)
	require.Error(t, err, "the classified subsystem must be deleted by ID, not left behind by a stale name lookup")
	assert.True(t, truenas.IsNotFoundError(err))
	assert.Nil(t, remaining)
}

// TestDetectOrphanedNFSSharesScopesToTheParentDataset pins the scoping guard NFS
// was the only protocol to omit. iSCSI and NVMe-oF both refuse to classify a
// backreference pointing outside the configured parent dataset; NFS trusted the
// share comment alone, which is operator-writable free text on an appliance
// shared with other CSI instances and with hand-made shares.
func TestDetectOrphanedNFSSharesScopesToTheParentDataset(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newOrphanShareSweepDriver(client)

	// A comment carrying THIS driver's name but a dataset outside its parent.
	_, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/tank/other/foreign-volume",
		Comment: "scale-csi (org.scale.csi.test): tank/other/foreign-volume",
		Enabled: true,
	})
	require.NoError(t, err)
	// A genuine orphan under the parent, to prove the guard is not a blanket
	// disable.
	_, err = client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{
		Path:    "/mnt/pool/parent/gone-volume",
		Comment: "scale-csi (org.scale.csi.test): pool/parent/gone-volume",
		Enabled: true,
	})
	require.NoError(t, err)

	kubeState := &kubernetesReconcileState{volumeHandles: make(map[string]struct{})}
	report := ReconcileReport{}
	d.detectOrphanedShares(ctx, kubeState, &report)

	require.Len(t, report.OrphanShares, 1, "only the share under the configured parent may be classified")
	assert.Equal(t, "pool/parent/gone-volume", report.OrphanShares[0].ID)
}

// TestDatasetBusyObservationFailureIsVisible pins the observability fix. The
// pre-delete busy probe is observation-only and correctly does NOT gate the
// delete — the defect is that its failure path was invisible. The error logged
// at klog.V(2), silent at the default verbosity the driver runs at, and no
// metric moved at all, so "the probe failed" and "nothing was busy" were
// byte-identical in both logs and metrics.
func TestDatasetBusyObservationFailureIsVisible(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}
	client.DatasetAttachmentsErr = assert.AnError
	client.DatasetProcessesErr = assert.AnError

	attachErrBefore := testutil.ToFloat64(datasetBusyObservationErrorsTotal.WithLabelValues("attachment"))
	processErrBefore := testutil.ToFloat64(datasetBusyObservationErrorsTotal.WithLabelValues("process"))

	logged := captureDefaultVerbosityKlog(t, func() {
		d.observeDatasetBusyBeforeDelete(ctx, "pool/parent/probe-volume", "DeleteVolume")
	})

	assert.Contains(t, logged, "Could not inspect dataset pool/parent/probe-volume attachments",
		"a failed probe must be visible at the DEFAULT verbosity, not only at -v=2")
	assert.Contains(t, logged, "Could not inspect dataset pool/parent/probe-volume processes")
	assert.Equal(t, attachErrBefore+1, testutil.ToFloat64(datasetBusyObservationErrorsTotal.WithLabelValues("attachment")))
	assert.Equal(t, processErrBefore+1, testutil.ToFloat64(datasetBusyObservationErrorsTotal.WithLabelValues("process")))
}

// TestDatasetBusyObservationQuietProbeMaterializesItsSeries is the other half:
// a probe that ran and found nothing must still produce an observation series,
// or a fleet where every probe fails looks exactly like a fleet where nothing is
// ever busy.
func TestDatasetBusyObservationQuietProbeMaterializesItsSeries(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			DriverName: "org.scale.csi.nfs",
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: client,
	}

	datasetBusyObservationsTotal.Reset()
	d.observeDatasetBusyBeforeDelete(ctx, "pool/parent/quiet-volume", "DeleteVolume")

	assert.Equal(t, 2, testutil.CollectAndCount(datasetBusyObservationsTotal, "scale_csi_dataset_busy_observations_total"),
		"a successful, quiet probe must still materialize an observation series for BOTH kinds")
}

// captureDefaultVerbosityKlog captures klog output WITHOUT raising -v, so a
// message only emitted at V(2) is invisible to it — which is the whole point.
func captureDefaultVerbosityKlog(t *testing.T, fn func()) string {
	t.Helper()
	if flag.Lookup("v") == nil {
		klog.InitFlags(nil)
	}
	verbosity := flag.Lookup("v")
	originalVerbosity := verbosity.Value.String()
	require.NoError(t, verbosity.Value.Set("0"))

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
