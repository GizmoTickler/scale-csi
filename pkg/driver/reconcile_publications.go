package driver

import (
	"context"
	"fmt"
	"path"
	"sort"
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

// staleSweepCandidate is one dataset's records for the stale-record sweep.
type staleSweepCandidate struct {
	datasetName string
	records     map[string]publicationRecord
}

// staleSweep is what one sweep pass classifies.
type staleSweep struct {
	candidates []staleSweepCandidate
	// recordCount is what the mass-absence brake weighs; negative means the
	// records could not be listed and the pass does nothing.
	recordCount int
}

// staleSweepCandidates reads the records the sweep classifies.
func (d *Driver) staleSweepCandidates(ctx context.Context, datasets []*truenas.Dataset) staleSweep {
	switch store := d.publications().(type) {
	case kubernetesPublicationStore:
		return d.kubernetesStaleSweepCandidates(ctx, store, datasets)
	case importingPublicationStore:
		// One list of the VolumePublications, and the records not yet imported
		// from the datasets' own properties; Kubernetes wins per key, as reads do.
		current := d.kubernetesStaleSweepCandidates(ctx, store.kube, datasets)
		if current.recordCount < 0 {
			return current
		}
		return mergeStaleSweeps(d.zfsStaleSweepCandidates(ctx, store.legacy, datasets), current)
	default:
		return d.zfsStaleSweepCandidates(ctx, store, datasets)
	}
}

// mergeStaleSweeps resolves each dataset's ZFS and Kubernetes records as every
// read does (resolvePublicationRecords), so the record a revoke re-reads under
// the lock is the one the sweep classified. The brake weighs both counts: a
// record caught in both stores by a crash counts twice, which only makes the
// brake engage sooner.
func mergeStaleSweeps(legacy, current staleSweep) staleSweep {
	legacyBy := make(map[string]map[string]publicationRecord, len(legacy.candidates))
	currentBy := make(map[string]map[string]publicationRecord, len(current.candidates))
	order := make([]string, 0, len(legacy.candidates))
	for _, sweep := range []struct {
		candidates []staleSweepCandidate
		into       map[string]map[string]publicationRecord
	}{{legacy.candidates, legacyBy}, {current.candidates, currentBy}} {
		for _, candidate := range sweep.candidates {
			if _, seen := legacyBy[candidate.datasetName]; !seen {
				if _, seen := currentBy[candidate.datasetName]; !seen {
					order = append(order, candidate.datasetName)
				}
			}
			sweep.into[candidate.datasetName] = candidate.records
		}
	}
	merged := staleSweep{candidates: make([]staleSweepCandidate, 0, len(order)), recordCount: legacy.recordCount + current.recordCount}
	for _, name := range order {
		records := resolvePublicationRecords(legacyBy[name], currentBy[name])
		if records == nil {
			records = map[string]publicationRecord{}
		}
		merged.candidates = append(merged.candidates, staleSweepCandidate{datasetName: name, records: records})
	}
	return merged
}

// zfsStaleSweepCandidates reads the records the datasets carry as properties.
func (d *Driver) zfsStaleSweepCandidates(ctx context.Context, store publicationStore, datasets []*truenas.Dataset) staleSweep {
	recordCount := publicationPropertyCount(datasets)
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
	candidates := make([]staleSweepCandidate, 0)
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
		records, parseErr := store.records(ctx, recordSource.Name, recordSource)
		if parseErr != nil {
			d.recordReconcileObjectFailure("stale_publication_classification", dataset.Name, parseErr)
			continue
		}
		candidates = append(candidates, staleSweepCandidate{datasetName: dataset.Name, records: records})
	}
	return staleSweep{candidates: candidates, recordCount: recordCount}
}

// kubernetesStaleSweepCandidates lists this instance's VolumePublications. A
// record whose dataset is not in the listing is removed once a fresh read
// under the volume lock shows the dataset is gone (its volume was deleted and
// the delete's own removal did not happen); anything else about it is left.
func (d *Driver) kubernetesStaleSweepCandidates(ctx context.Context, store kubernetesPublicationStore, datasets []*truenas.Dataset) staleSweep {
	listing, err := store.all(ctx)
	if err != nil {
		d.recordReconcileObjectFailure("stale_publication_classification", "volumepublications", err)
		return staleSweep{recordCount: -1}
	}
	all := listing.byDataset
	for _, badErr := range listing.unreadable {
		d.recordReconcileObjectFailure("stale_publication_classification", "volumepublications", badErr)
	}
	listed := make(map[string]struct{}, len(datasets))
	for _, dataset := range datasets {
		if dataset != nil {
			listed[dataset.Name] = struct{}{}
		}
	}
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)
	candidates := make([]staleSweepCandidate, 0, len(names))
	missing := make([]string, 0)
	for _, name := range names {
		if _, ok := listed[name]; ok {
			candidates = append(candidates, staleSweepCandidate{datasetName: name, records: all[name]})
			continue
		}
		missing = append(missing, name)
	}
	// DeleteVolume removes a volume's records itself, so a dataset missing
	// here is a leak of one interrupted delete. Many at once, or an empty
	// listing, is the shape of a pool not yet imported (each dataset then
	// reads NotFound too), not of deletes: leave them for a later pass.
	if len(missing) > 0 && (len(listed) == 0 || len(missing) > max(staleRecordMassAbsenceThreshold, len(listed)/10)) {
		RecordFencingStaleDeferred()
		klog.Warningf("Stale fencing record reconcile: %d datasets with publication records are missing from a listing of %d; not forgetting them this pass",
			len(missing), len(listed))
	} else {
		for _, name := range missing {
			d.forgetPublicationsOfDeletedDataset(ctx, store, name)
		}
	}
	return staleSweep{candidates: candidates, recordCount: listing.objects}
}

func (d *Driver) forgetPublicationsOfDeletedDataset(ctx context.Context, store kubernetesPublicationStore, datasetName string) {
	lockKey := volumeLockKey(path.Base(datasetName))
	if !d.acquireOperationLock(lockKey) {
		return
	}
	defer d.releaseOperationLock(lockKey)
	if _, err := d.truenasClient.DatasetGet(ctx, datasetName); !truenas.IsNotFoundError(err) {
		return // present, or unknown: leave its records alone
	}
	if err := store.forget(ctx, datasetName); err != nil {
		d.recordReconcileObjectFailure("stale_publication_cleanup", datasetName, err)
		return
	}
	klog.Infof("Stale fencing record reconcile removed the publication records of deleted dataset %s", datasetName)
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
	sweep := d.staleSweepCandidates(ctx, datasets)
	candidates, recordCount := sweep.candidates, sweep.recordCount
	if recordCount < 0 {
		return
	}
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
	for _, candidate := range candidates {
		records := candidate.records
		volumeID := path.Base(candidate.datasetName)
		for propertyKey := range records {
			record := records[propertyKey]
			observationKey := stalePublicationObservationKey(candidate.datasetName, propertyKey)
			if _, live := state.liveVolumeAttachments[volumeAttachmentKey(volumeID, record.Node)]; live {
				d.stalePublicationRecordsSeen.Delete(observationKey)
				continue
			}
			firstMissing := now
			if record.State != publicationStateRemoving {
				candidateObservation := newStalePublicationObservation(now, record)
				actual, loaded := d.stalePublicationRecordsSeen.LoadOrStore(observationKey, candidateObservation)
				observation, valid := actual.(stalePublicationObservation)
				if !loaded || !valid || !observation.matches(record) {
					d.stalePublicationRecordsSeen.Store(observationKey, candidateObservation)
					observation = candidateObservation
				}
				firstMissing = observation.FirstMissing
				if now.Sub(firstMissing) < grace {
					continue
				}
			}
			revoked, err := d.revokeStalePublicationRecord(ctx, candidate.datasetName, volumeID, propertyKey, record, recordCount)
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

// runStalePublicationRecordsPass loads the minimal state
// reconcileStalePublicationRecords needs — one backend dataset listing plus
// the Kubernetes reconcile state — and invokes it. It exists so the fencing
// stale-record grace-period cadence (see startOrphanReconcile, C1 fix) can run
// ONLY this repair instead of the full orphan-detection pass: unlike that pass
// it issues no snapshot listing and performs no bookkeeping sweeps or
// adoption/migration writes, so running it every grace period (routinely far
// shorter than reconcile.interval) does not multiply the heavy pass's cost.
// A caller must have already confirmed fencing is enabled; this is a no-op
// otherwise since reconcileStalePublicationRecords has nothing to repair.
func (d *Driver) runStalePublicationRecordsPass(ctx context.Context, minOrphanAge time.Duration) {
	if d.config == nil || d.truenasClient == nil || !d.config.Fencing.Enabled() {
		return
	}
	datasets, err := d.listAllManagedDatasets(ctx)
	if err != nil {
		d.recordReconcileObjectFailure("stale_publication_list_backend_volumes", d.config.ZFS.DatasetParentName, err)
		return
	}
	kubeState, err := d.loadKubernetesReconcileState(ctx, minOrphanAge)
	if err != nil {
		d.recordReconcileObjectFailure("stale_publication_load_kubernetes_state", "kubernetes", err)
		return
	}
	d.reconcileStalePublicationRecords(ctx, datasets, kubeState, time.Now())
}

func (d *Driver) revokeStalePublicationRecord(
	ctx context.Context,
	datasetName, volumeID, propertyKey string,
	detected publicationRecord,
	recordCount int,
) (bool, error) {
	revoked, err := d.revokeStalePublicationRecordLocked(ctx, datasetName, volumeID, propertyKey, detected, recordCount)
	if revoked {
		// (C11 fix) This revoke may be exactly what a quarantined startup fencing
		// volume (quarantineStaleStartupFencingVolume) was waiting on: that
		// carve-out exists BECAUSE this same stale record made the volume's own
		// publication fail validatePublicationCompatibility. Revoking it removes
		// that block, but nothing else re-evaluates the volume: the startup
		// reconcile goroutine (if still running) is idling on its signal, not
		// polling. The signal names this volume, and only this volume is re-run.
		// It is sent after the volume lock is released, so the re-run never
		// finds the lock still held by this revoke.
		d.requestStartupAttachmentReconcile(datasetName)
	}
	return revoked, err
}

func (d *Driver) revokeStalePublicationRecordLocked(
	ctx context.Context,
	datasetName, volumeID, propertyKey string,
	detected publicationRecord,
	recordCount int,
) (bool, error) {
	lockKey := volumeLockKey(volumeID)
	if !d.acquireOperationLock(lockKey) {
		return false, fmt.Errorf("volume operation is in progress")
	}
	defer d.releaseOperationLock(lockKey)

	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return false, fmt.Errorf("fresh dataset read: %w", err)
	}
	records, err := d.publications().records(ctx, datasetName, dataset)
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
