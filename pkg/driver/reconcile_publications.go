package driver

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type stalePublicationObservation struct {
	FirstMissing time.Time
	UpdatedAt    string
	State        string
	EncodedID    string
}

func newStalePublicationObservation(now time.Time, record publicationRecord) stalePublicationObservation {
	return stalePublicationObservation{
		FirstMissing: now,
		UpdatedAt:    record.UpdatedAt,
		State:        record.State,
		EncodedID:    record.EncodedID,
	}
}

func (observation stalePublicationObservation) matches(record publicationRecord) bool {
	return observation.UpdatedAt == record.UpdatedAt &&
		observation.State == record.State &&
		observation.EncodedID == record.EncodedID
}

func stalePublicationObservationKey(datasetName, propertyKey string) string {
	return datasetName + "\x00" + propertyKey
}

// datasetHasPublicationRecordKeys reports whether the dataset carries at least one
// publication_* user-property KEY. The TrueNAS 26.0 zfs.resource.query listing
// exposes these keys but NOT their source (user_properties come back as a flat
// string map), so key presence is the cheap, source-independent pre-filter used to
// decide which datasets need a source-bearing re-fetch before their records can be
// classified. It is deliberately over-inclusive — a clone inherits the source
// volume's publication_* keys — but the source-authoritative parse that follows the
// re-fetch narrows back to only this dataset's own (local) records.
func datasetHasPublicationRecordKeys(dataset *truenas.Dataset) bool {
	if dataset == nil {
		return false
	}
	for key := range dataset.UserProperties {
		if strings.HasPrefix(key, publicationPropertyPrefix) {
			return true
		}
	}
	return false
}

// publicationPropertyCount counts publication_* user-property KEYS across the
// listing. It deliberately ignores property source: the zfs.resource.query listing
// is flat/sourceless on TrueNAS 26.0, so source is unavailable here. The count only
// feeds the mass-absence brake — a heuristic that defers all revocation when the
// VolumeAttachment list looks empty while several records exist — where counting
// clone-inherited keys as well is safe: it biases the brake toward deferral
// (inaction) rather than mass revocation. Source-authoritative classification
// happens later, per candidate dataset, after a source-bearing re-fetch.
func publicationPropertyCount(datasets []*truenas.Dataset) int {
	count := 0
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		for key := range dataset.UserProperties {
			if strings.HasPrefix(key, publicationPropertyPrefix) {
				count++
			}
		}
	}
	return count
}

// reconcileStalePublicationRecords repairs the operator force-finalizer escape
// hatch. A finalizer-removed VolumeAttachment never reaches external-attacher's
// normal ControllerUnpublishVolume call, so its durable record otherwise blocks
// SINGLE_NODE volumes forever. Absence must be continuous for the configured
// grace period and is proved again under the same per-volume lock used by CSI.
func (d *Driver) reconcileStalePublicationRecords(
	ctx context.Context,
	datasets []*truenas.Dataset,
	state *kubernetesReconcileState,
	now time.Time,
) {
	if state == nil {
		return
	}
	recordCount := publicationPropertyCount(datasets)
	if state.volumeAttachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		// A zero-result VA list while several backend records exist is the shape of
		// an etcd restore or informer/API discontinuity, not evidence for mass
		// revocation. Restart every observation's grace window after recovery.
		d.stalePublicationRecordsSeen.Range(func(key, _ interface{}) bool {
			d.stalePublicationRecordsSeen.Delete(key)
			return true
		})
		RecordFencingStaleDeferred()
		klog.Warningf("Stale fencing record reconcile deferred: VolumeAttachment list is empty while %d records exist (brake threshold=%d)",
			recordCount, staleRecordMassAbsenceThreshold)
		return
	}
	grace, err := d.config.Fencing.StaleRecordGracePeriodDuration()
	if err != nil || grace <= 0 {
		d.recordReconcileObjectFailure("stale_publication_configuration", "fencing.staleRecordGracePeriod", err)
		return
	}
	// The zfs.resource.query listing returns user_properties as a flat, SOURCELESS
	// map on TrueNAS 26.0, but publicationRecordsFromDataset must distinguish a
	// dataset's own (source=="local") records from clone-inherited ones by source:
	// run against the listing directly, it would skip every record and silently
	// disable this repair. Pre-filter cheaply on publication_* KEY presence (the
	// flat read still exposes keys) and re-fetch ONLY those candidates through a
	// source-bearing pool.dataset.query read. The re-fetches are batched into ONE
	// DatasetGetByNames (["id","in",names]) instead of one DatasetGet per dataset
	// — with fencing on, every attached volume carries a record, so this collapses
	// ~N source-bearing GETs per pass into a single round trip. A source-bearing
	// listing (the pool.dataset.query fallback) is already authoritative and is
	// used as-is. The read stays source-bearing (same DatasetGet projection);
	// zfs.resource.query is never used here because it loses user-property source.
	sourcelessNames := make([]string, 0)
	for _, dataset := range datasets {
		if dataset != nil && dataset.ResourceQuery && datasetHasPublicationRecordKeys(dataset) {
			sourcelessNames = append(sourcelessNames, dataset.Name)
		}
	}
	var sourceBearing map[string]*truenas.Dataset
	var failedSourceBearing map[string]struct{}
	if len(sourcelessNames) > 0 {
		sourceBearing, failedSourceBearing = d.datasetGetByNamesChunked(ctx, sourcelessNames)
	}
	for _, dataset := range datasets {
		if dataset == nil {
			continue
		}
		recordSource := dataset
		if dataset.ResourceQuery && datasetHasPublicationRecordKeys(dataset) {
			sourceBearingDataset, ok := sourceBearing[dataset.Name]
			if !ok {
				// A failed chunk is already recorded once and affects only its own
				// names. A successful chunk that omitted this dataset means it
				// vanished between listing and re-read; record that separately.
				if _, failed := failedSourceBearing[dataset.Name]; !failed {
					d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name,
						fmt.Errorf("source-bearing re-read returned no dataset"))
				}
				continue
			}
			recordSource = sourceBearingDataset
		}
		records, parseErr := publicationRecordsFromDataset(recordSource)
		if parseErr != nil {
			d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name, parseErr)
			continue
		}
		volumeID := path.Base(dataset.Name)
		for propertyKey := range records {
			record := records[propertyKey]
			observationKey := stalePublicationObservationKey(dataset.Name, propertyKey)
			if _, live := state.liveVolumeAttachments[volumeAttachmentKey(volumeID, record.Node)]; live {
				d.stalePublicationRecordsSeen.Delete(observationKey)
				continue
			}
			firstMissing := now
			if record.State != publicationStateRemoving {
				candidate := newStalePublicationObservation(now, record)
				actual, loaded := d.stalePublicationRecordsSeen.LoadOrStore(observationKey, candidate)
				observation, valid := actual.(stalePublicationObservation)
				if !loaded || !valid || !observation.matches(record) {
					d.stalePublicationRecordsSeen.Store(observationKey, candidate)
					observation = candidate
				}
				firstMissing = observation.FirstMissing
				if now.Sub(firstMissing) < grace {
					continue
				}
			}
			revoked, err := d.revokeStalePublicationRecord(ctx, dataset.Name, volumeID, propertyKey, record, recordCount)
			if err != nil {
				d.recordReconcileObjectFailure("stale_publication_cleanup", volumeID+"/"+record.Node, err)
				continue
			}
			d.stalePublicationRecordsSeen.Delete(observationKey)
			if !revoked {
				continue
			}
			klog.Infof("Stale fencing record reconcile revoked volume=%s node=%s after continuous absence since %s",
				volumeID, record.Node, firstMissing.UTC().Format(time.RFC3339))
		}
	}
}

func (d *Driver) revokeStalePublicationRecord(
	ctx context.Context,
	datasetName, volumeID, propertyKey string,
	detected publicationRecord,
	recordCount int,
) (bool, error) {
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation is in progress")
	}
	defer d.releaseOperationLock(lockKey)

	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("fresh dataset read: %w", err)
	}
	records, err := publicationRecordsFromDataset(dataset)
	if err != nil {
		return false, fmt.Errorf("fresh publication record read: %w", err)
	}
	current, exists := records[propertyKey]
	if !exists || !samePublicationRecordGeneration(current, detected) {
		// The grant was removed or republished while the outer pass waited for
		// the volume lock. Never apply an old grace decision to a new generation.
		return false, nil
	}
	live, attachmentCount, err := d.liveVolumeAttachmentExists(ctx, volumeID, current.Node)
	if err != nil {
		return false, err
	}
	if live {
		return false, nil
	}
	if attachmentCount == 0 && recordCount >= staleRecordMassAbsenceThreshold {
		RecordFencingStaleDeferred()
		return false, fmt.Errorf("mass-absence brake engaged during final VolumeAttachment recheck")
	}
	nodeID := current.EncodedID
	if nodeID == "" {
		nodeID = current.Node
	}
	shareType := shareTypeForPublishedVolume(dataset, nil)
	if err := d.unpublishFencedVolume(ctx, dataset, datasetName, shareType, nodeID, nil); err != nil {
		return false, fmt.Errorf("revoke backend grant and publication record: %w", err)
	}
	return true, nil
}
