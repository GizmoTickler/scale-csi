package driver

import (
	"context"
	"errors"
	"strings"
	"sync"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

func (d *Driver) deleteDatasetWithBusyObservation(
	ctx context.Context,
	datasetName string,
	recursive, force bool,
	operation string,
) error {
	mode := d.config.busyObservationMode()
	if mode == BusyObservationAlways {
		d.observeDatasetBusy(ctx, datasetName, operation, "before")
	}
	err := d.truenasClient.DatasetDelete(ctx, datasetName, recursive, force)
	if mode == BusyObservationOnFailure && busyObservationExplainsDeleteFailure(ctx, err) {
		d.observeDatasetBusy(ctx, datasetName, operation, "after a failed")
	}
	return err
}

// busyObservationExplainsDeleteFailure reports whether a failed delete is one
// the busy scans could explain. They are skipped for a delete that succeeded,
// for a dataset that is already gone, for a caller that has given up, and for
// a plain snapshot/children dependency refusal: that is the normal first
// attempt for a volume with snapshots, and the scans cannot add to it. Every
// other failure, a busy one or an unclassified one, is observed.
func busyObservationExplainsDeleteFailure(ctx context.Context, err error) bool {
	if err == nil || truenas.IsNotFoundError(err) || ctx.Err() != nil {
		return false
	}
	message := strings.ToLower(err.Error())
	var apiErr *truenas.APIError
	if errors.As(err, &apiErr) {
		message = strings.ToLower(apiErr.FullError())
	}
	if strings.Contains(message, "busy") || strings.Contains(message, "in use") {
		return true
	}
	for _, marker := range []string{"dependent", "snapshot", "has children", "enotempty"} {
		if strings.Contains(message, marker) {
			return false
		}
	}
	return true
}

// observeDatasetBusy is observation-only by design: it never gates
// the delete, and a probe failure is therefore not fatal. What it must NOT be is
// invisible. Both arms previously logged at klog.V(2) — silent at the default
// verbosity the driver actually runs at — and the metric only moved when
// something was found, so "the query failed" and "nothing was busy" produced
// byte-identical (empty) logs and metrics. Errors now log at Warning and
// increment their own counter, while the observation counter is recorded even
// at zero so the quiet case has a series of its own.
//
// The two reads are independent and observation-only, so they run
// concurrently: pool.dataset.attachments (~570ms on nas01) and
// pool.dataset.processes (~210ms) used to add up on every DeleteVolume.
//
// phase is "before" (zfs.observeBusyBeforeDelete: always) or "after a failed"
// (on-failure). After a failure what was found is logged at the default
// verbosity: it is the likely explanation of that failure.
func (d *Driver) observeDatasetBusy(ctx context.Context, datasetName, operation, phase string) {
	verbosity := klog.Level(2)
	if phase != "before" {
		verbosity = 0
	}
	var (
		wg           sync.WaitGroup
		attachments  []truenas.DatasetAttachment
		attachErr    error
		processes    []truenas.DatasetProcess
		processesErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		attachments, attachErr = d.truenasClient.DatasetAttachments(ctx, datasetName)
	}()
	go func() {
		defer wg.Done()
		processes, processesErr = d.truenasClient.DatasetProcesses(ctx, datasetName)
	}()
	wg.Wait()

	if attachErr != nil {
		RecordDatasetBusyObservationError("attachment")
		klog.Warningf("Could not inspect dataset %s attachments %s %s delete: %v", datasetName, phase, operation, attachErr)
	} else {
		RecordDatasetBusyObservations("attachment", len(attachments))
		for _, attachment := range attachments {
			klog.V(verbosity).Infof("Dataset %s is busy %s %s delete: attachment type=%q service=%q names=%v",
				datasetName, phase, operation, attachment.Type, optionalDatasetActivityValue(attachment.Service), attachment.Attachments)
		}
	}

	if processesErr != nil {
		RecordDatasetBusyObservationError("process")
		klog.Warningf("Could not inspect dataset %s processes %s %s delete: %v", datasetName, phase, operation, processesErr)
		return
	}
	RecordDatasetBusyObservations("process", len(processes))
	for _, process := range processes {
		klog.V(verbosity).Infof("Dataset %s is busy %s %s delete: process pid=%d name=%q service=%q cmdline=%q",
			datasetName, phase, operation, process.PID, process.Name,
			optionalDatasetActivityValue(process.Service), optionalDatasetActivityValue(process.Cmdline))
	}
}

func optionalDatasetActivityValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// The busy series exist at zero from start-up: in the default on-failure mode
// the scans run rarely, and "no series" must not read as "never measured".
func init() {
	for _, kind := range []string{"attachment", "process"} {
		datasetBusyObservationsTotal.WithLabelValues(kind)
		datasetBusyObservationErrorsTotal.WithLabelValues(kind)
	}
}
