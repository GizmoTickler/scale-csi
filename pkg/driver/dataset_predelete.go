package driver

import (
	"context"
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
	d.observeDatasetBusyBeforeDelete(ctx, datasetName, operation)
	return d.truenasClient.DatasetDelete(ctx, datasetName, recursive, force)
}

// observeDatasetBusyBeforeDelete is observation-only by design: it never gates
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
func (d *Driver) observeDatasetBusyBeforeDelete(ctx context.Context, datasetName, operation string) {
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
		klog.Warningf("Could not inspect dataset %s attachments before %s delete: %v", datasetName, operation, attachErr)
	} else {
		RecordDatasetBusyObservations("attachment", len(attachments))
		for _, attachment := range attachments {
			klog.V(2).Infof("Dataset %s is busy before %s delete: attachment type=%q service=%q names=%v",
				datasetName, operation, attachment.Type, optionalDatasetActivityValue(attachment.Service), attachment.Attachments)
		}
	}

	if processesErr != nil {
		RecordDatasetBusyObservationError("process")
		klog.Warningf("Could not inspect dataset %s processes before %s delete: %v", datasetName, operation, processesErr)
		return
	}
	RecordDatasetBusyObservations("process", len(processes))
	for _, process := range processes {
		klog.V(2).Infof("Dataset %s is busy before %s delete: process pid=%d name=%q service=%q cmdline=%q",
			datasetName, operation, process.PID, process.Name,
			optionalDatasetActivityValue(process.Service), optionalDatasetActivityValue(process.Cmdline))
	}
}

func optionalDatasetActivityValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
