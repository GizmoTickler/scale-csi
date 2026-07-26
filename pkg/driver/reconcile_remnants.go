package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// sweepStaleInflightMarkers retires in-flight creation markers that can no
// longer gate a recovery: their dataset is gone, or it now carries a local
// ownership stamp (creation completed; only the marker delete was lost). Both
// conditions are proven live and only after a generous age bound so an active
// multi-minute detached copy is never raced. Markers from other driver
// instances, other versions, or with unusable timestamps are left alone;
// corrupt (unparseable) payloads in our namespace are retired because nothing
// can ever act on them.
func (d *Driver) sweepStaleInflightMarkers(ctx context.Context, parent *truenas.Dataset, now time.Time, minAge time.Duration) {
	if parent == nil {
		return
	}
	staleKeys := make([]string, 0)
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			klog.Warningf("Retiring corrupt in-flight marker %s: %v", key, err)
			staleKeys = append(staleKeys, key)
			continue
		}
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() || marker.Dataset == "" {
			continue
		}
		startedAt, parseErr := time.Parse(time.RFC3339Nano, marker.StartedAt)
		if parseErr != nil || now.Sub(startedAt) <= minAge {
			continue
		}
		dataset, err := d.truenasClient.DatasetGet(ctx, marker.Dataset)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				staleKeys = append(staleKeys, key)
			} else {
				d.recordReconcileObjectFailure("inflight_marker_sweep", marker.Dataset, err)
			}
			continue
		}
		if owner, ok := datasetUserPropertyProjection(dataset, PropDriverInstanceID); ok && isLocalUserPropertySource(owner.Source) {
			staleKeys = append(staleKeys, key)
		}
		// Dataset exists but is still unstamped: the marker remains the only proof
		// that lets a retry recover the remnant — keep it.
	}
	if len(staleKeys) == 0 {
		return
	}
	sort.Strings(staleKeys)
	if err := d.removeBookkeepingProperties(ctx, staleKeys); err != nil {
		d.recordReconcileObjectFailure("inflight_marker_sweep", d.parentDatasetName(), err)
		return
	}
	klog.Infof("Orphan reconcile: retired %d stale in-flight creation markers", len(staleKeys))
}

// localInflightMarkers extracts every parseable LOCAL in-flight creation marker
// from a dataset, keyed by property key. Version and instance filtering is left
// to the caller (matching sweepStaleInflightMarkers), so foreign or newer-version
// markers are visible to the same guards that ignore them.
func localInflightMarkers(ds *truenas.Dataset) map[string]*inflightMarker {
	markers := make(map[string]*inflightMarker)
	if ds == nil {
		return markers
	}
	for key, property := range ds.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			klog.Warningf("Ignoring unparseable in-flight marker %s: %v", key, err)
			continue
		}
		markers[key] = &marker
	}
	return markers
}

// classifyRemnantOrphans detects remnant orphans: unstamped datasets whose
// in-flight creation marker survived a controller crash and whose same-name
// CreateVolume retry is never coming (VolSync mints a NEW PVC UID on failure, so
// the marker — not a retry — is the only thing that can ever act on the remnant).
// Each candidate must satisfy, all proven live: a valid local marker for THIS
// instance whose dataset sits strictly under the CSI parent (datasetForID-style
// validation), marker age beyond minOrphanAge, an existing UNSTAMPED dataset, and
// no referencing Kubernetes object. Detection is read-only; the guarded destroy
// runs in deleteDetectedOrphans under opts.Delete. A stamped dataset is left to
// the stale-marker sweep (marker retirement) and the orphan-volume pass (dataset
// reclamation) — this phase never touches it.
func (d *Driver) classifyRemnantOrphans(ctx context.Context, now time.Time, minOrphanAge time.Duration, parentDataset, bookkeeping *truenas.Dataset, report *ReconcileReport) {
	// Reuse the parent/bookkeeping datasets the reconcile pass already read instead
	// of re-fetching them here (N+1 elimination). A nil argument means the pass did
	// not have a successful read (e.g. bookkeeping disabled, NotFound, or a transient
	// error), so fall back to a direct read to preserve the historical behavior —
	// including distinguishing NotFound from a real error for the bookkeeping dataset.
	if parentDataset == nil {
		var err error
		parentDataset, err = d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
		if err != nil {
			d.recordReconcileObjectFailure("remnant_orphan_classify", d.parentDatasetName(), err)
			return
		}
	}
	markers := localInflightMarkers(parentDataset)
	if d.bookkeepingEnabled() {
		bookkeepingDataset := bookkeeping
		if bookkeepingDataset == nil {
			var bkErr error
			bookkeepingDataset, bkErr = d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
			if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
				d.recordReconcileObjectFailure("remnant_orphan_classify", d.bookkeepingDatasetName(), bkErr)
				return
			}
		}
		// Dual-read merge: the same marker carries the same content-hashed key in
		// both locations, so the union is lossless and a migrated marker is seen
		// regardless of which dataset still holds it.
		for key, marker := range localInflightMarkers(bookkeepingDataset) {
			markers[key] = marker
		}
	}
	parentName := d.parentDatasetName()
	for key, marker := range markers {
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() || marker.Dataset == "" {
			continue
		}
		volumeID := path.Base(marker.Dataset)
		if !datasetStrictlyBelowParent(marker.Dataset, parentName) || !validVolumeIDLeaf(volumeID) || key != inflightMarkerKey(volumeID) {
			continue
		}
		if volumeID == bookkeepingDatasetLeaf {
			// Belt-and-suspenders: the bookkeeping dataset is never a volume and
			// must never be classified, whatever properties it carries.
			continue
		}
		startedAt, parseErr := time.Parse(time.RFC3339Nano, marker.StartedAt)
		if parseErr != nil || now.Sub(startedAt) <= minOrphanAge {
			continue
		}
		dataset, getErr := d.truenasClient.DatasetGet(ctx, marker.Dataset)
		if getErr != nil {
			if truenas.IsNotFoundError(getErr) {
				// Dataset gone: nothing to destroy; the stale-marker sweep retires
				// the marker. Not a remnant orphan.
				continue
			}
			d.recordReconcileObjectFailure("remnant_orphan_classify", marker.Dataset, getErr)
			continue
		}
		if datasetHasLocalOwnershipStamp(dataset) {
			klog.V(4).Infof("Orphan reconcile: skipping remnant candidate %s because it carries a local ownership stamp", marker.Dataset)
			continue
		}
		if safe, k8sReason := d.remnantHasNoKubernetesReference(ctx, volumeID); !safe {
			klog.V(2).Infof("Orphan reconcile: skipping remnant candidate %s: %s", marker.Dataset, k8sReason)
			continue
		}
		report.RemnantVolumes = append(report.RemnantVolumes, ReconcileObject{
			ID:           volumeID,
			BackendID:    marker.Dataset,
			PVName:       volumeID,
			CreatedAt:    startedAt,
			Age:          now.Sub(startedAt),
			Bytes:        dataset.GetUsedBytes(),
			remnantNonce: marker.Nonce,
		})
	}
}

// destroyRemnantOrphan removes a classified remnant orphan under opts.Delete.
// Immediately before the non-recursive destroy it re-proves, live: the marker is
// still present with an identical nonce (a rewritten marker means a fresh create
// owns the dataset now), the dataset still exists and is unstamped, the dataset's
// actual ZFS origin matches the marker (clone mode) or is empty (detached copy),
// and no Kubernetes object references the volume. The destroy is NON-recursive
// with force=false so any child dataset or snapshot under the remnant FAILS the
// delete (fail-safe) and is surfaced as a skip reason rather than destroyed
// silently. On success the marker is retired from both bookkeeping locations and
// a Warning event is recorded so operators see the reap.
func (d *Driver) destroyRemnantOrphan(ctx context.Context, remnant ReconcileObject) (destroyed bool, reason string) {
	volumeID := remnant.ID
	// Serialize against any concurrent CSI operation on the same volume name —
	// in particular a same-name CreateVolume RESUME, whose marker keeps the
	// original StartedAt (aged past the gate) and nonce, defeating the age and
	// nonce guards; the per-volume lock is the discriminator the sibling
	// reconcile delete paths rely on.
	lockKey := "volume:" + volumeID
	if !d.acquireOperationLock(lockKey) {
		return false, "volume operation is in progress"
	}
	defer d.releaseOperationLock(lockKey)
	marker, err := d.readInflightMarker(ctx, volumeID)
	if err != nil {
		return false, fmt.Sprintf("remnant marker revalidation failed: %v", err)
	}
	if marker == nil || marker.Instance != d.driverInstanceID() || marker.Dataset != remnant.BackendID {
		return false, "in-flight marker is no longer present for this remnant"
	}
	if marker.Nonce != remnant.remnantNonce {
		return false, "in-flight marker nonce changed (a new create attempt owns the remnant)"
	}
	dataset, err := d.truenasClient.DatasetGet(ctx, marker.Dataset)
	if err != nil {
		if truenas.IsNotFoundError(err) {
			// Already gone: the goal is met; retire the marker and report success.
			d.deleteInflightMarker(ctx, volumeID)
			return true, ""
		}
		return false, fmt.Sprintf("remnant dataset revalidation failed: %v", err)
	}
	if datasetHasLocalOwnershipStamp(dataset) {
		return false, "remnant dataset became stamped (creation completed)"
	}
	actualOrigin := datasetOriginSnapshotID(dataset)
	switch marker.Mode {
	case inflightModeClone:
		if marker.Origin == "" || actualOrigin != marker.Origin {
			return false, fmt.Sprintf("remnant origin %q does not match marker origin %q", actualOrigin, marker.Origin)
		}
	case inflightModeCopy:
		if actualOrigin != "" {
			return false, fmt.Sprintf("detached-copy remnant has unexpected origin %q", actualOrigin)
		}
	default:
		return false, fmt.Sprintf("remnant marker has unrecognized mode %q", marker.Mode)
	}
	if safe, k8sReason := d.remnantHasNoKubernetesReference(ctx, volumeID); !safe {
		return false, k8sReason
	}
	if delErr := d.truenasClient.DatasetDelete(ctx, marker.Dataset, false, false); delErr != nil {
		if truenas.IsNotFoundError(delErr) {
			d.deleteInflightMarker(ctx, volumeID)
			return true, ""
		}
		return false, fmt.Sprintf("guarded remnant destroy refused: %v", delErr)
	}
	d.deleteInflightMarker(ctx, volumeID)
	d.recordWarningEvent(volumeEventRef(volumeID), EventReasonRemnantOrphanReaped,
		fmt.Sprintf("Reaped remnant orphan dataset %s (origin %q, mode %s) left unstamped after a crashed create",
			marker.Dataset, actualOrigin, marker.Mode))
	klog.Infof("Orphan reconcile: destroyed remnant orphan volume %s (backend=%s origin=%q)", volumeID, marker.Dataset, actualOrigin)
	return true, ""
}

// sweepOrphanedReplicationJobs aborts only active replication.run_onetime jobs
// whose target is strictly below this driver's configured parent dataset. A
// matching copy marker protects legitimate in-flight work unless a live backend
// read proves that its source dataset is gone. The target dataset is never used
// as an abort trigger: a live detached copy (replication.run_onetime with
// only_from_scratch) deliberately has no target until 'zfs receive' materializes
// it, so target absence is expected mid-copy. The source, by contrast, is present
// throughout a legitimate copy, so its absence unambiguously marks an abandoned
// job. Marker reads happen after the job list, preserving the marker-before-launch
// ordering used by CreateVolume and avoiding a stale-parent-read race with a newly
// launched job.
func (d *Driver) sweepOrphanedReplicationJobs(ctx context.Context) error {
	if d.config == nil || d.truenasClient == nil {
		return fmt.Errorf("driver configuration and TrueNAS client are required")
	}
	jobs, err := d.truenasClient.ReplicationJobList(ctx)
	if err != nil {
		return err
	}
	parentName := d.parentDatasetName()
	inScope := make([]*truenas.ReplicationJob, 0, len(jobs))
	for _, job := range jobs {
		if job != nil && datasetStrictlyBelowParent(job.TargetDataset, parentName) {
			inScope = append(inScope, job)
		}
	}
	if len(inScope) == 0 {
		return nil
	}

	parent, err := d.truenasClient.DatasetGet(ctx, parentName)
	if err != nil {
		// Without the parent read, marker absence is not proven. Fail closed and
		// leave every job untouched.
		return fmt.Errorf("read parent dataset before replication job sweep: %w", err)
	}
	markers := d.liveCopyMarkers(parent)
	if d.bookkeepingEnabled() {
		// Dual-read: copy markers may live on the dedicated bookkeeping dataset.
		// A marker in either location protects its in-flight job from being aborted.
		bookkeeping, bkErr := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if bkErr != nil && !truenas.IsNotFoundError(bkErr) {
			return fmt.Errorf("read bookkeeping dataset before replication job sweep: %w", bkErr)
		}
		for dataset := range d.liveCopyMarkers(bookkeeping) {
			markers[dataset] = struct{}{}
		}
	}

	for _, job := range inScope {
		reason := ""
		if _, marked := markers[job.TargetDataset]; !marked {
			reason = replicationJobReasonMissingMarker
		} else {
			// The target dataset is deliberately absent until 'zfs receive'
			// materializes it, so only a missing SOURCE proves the job is abandoned.
			for _, sourceDataset := range job.SourceDatasets {
				missing, conclusive := d.replicationJobDatasetMissing(ctx, sourceDataset, job.ID, "source")
				if !conclusive {
					reason = ""
					break
				}
				if missing {
					reason = replicationJobReasonMissingSourceDataset
					break
				}
			}
		}
		if reason == "" {
			continue
		}
		if err := d.truenasClient.ReplicationJobAbort(ctx, job.ID, reason); err != nil {
			d.recordReconcileObjectFailure("replication_job_abort", fmt.Sprint(job.ID), err)
			continue
		}
		klog.Warningf("Replication job sweep aborted driver-owned job id=%d target=%s reason=%s", job.ID, job.TargetDataset, reason)
	}
	return nil
}

func (d *Driver) liveCopyMarkers(parent *truenas.Dataset) map[string]struct{} {
	markers := make(map[string]struct{})
	if parent == nil {
		return markers
	}
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropInflightMarkerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var marker inflightMarker
		if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
			continue
		}
		if marker.Version != inflightMarkerVersion || marker.Instance != d.driverInstanceID() ||
			marker.Mode != inflightModeCopy || !datasetStrictlyBelowParent(marker.Dataset, d.parentDatasetName()) ||
			key != inflightMarkerKey(path.Base(marker.Dataset)) {
			continue
		}
		markers[marker.Dataset] = struct{}{}
	}
	return markers
}

func (d *Driver) replicationJobDatasetMissing(ctx context.Context, dataset string, jobID int64, role string) (missing, conclusive bool) {
	if strings.TrimSpace(dataset) == "" {
		return false, false
	}
	if _, err := d.truenasClient.DatasetGet(ctx, dataset); err == nil {
		return false, true
	} else if truenas.IsNotFoundError(err) {
		return true, true
	} else {
		d.recordReconcileObjectFailure("replication_job_dataset_check", fmt.Sprintf("%d/%s/%s", jobID, role, dataset), err)
		return false, false
	}
}

func (d *Driver) runReplicationJobSweep(ctx context.Context) {
	if err := d.sweepOrphanedReplicationJobs(ctx); err != nil && ctx.Err() == nil {
		RecordReconcileFailure("replication_job_sweep")
		klog.Errorf("Replication job sweep failed: %v", err)
	}
}
