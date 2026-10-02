package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const startupReconcileWorkers = 4

var (
	startupReconcileInitialBackoff = 5 * time.Second
	startupReconcileMaxBackoff     = time.Minute
)

// startupReconcileRequestedHook, when set (tests only), sees every re-run
// request as it is made.
var startupReconcileRequestedHook func(datasetName string)

// errStartupVolumeBusy is a volume a pass skipped because a live CSI operation
// held its lock. The pass's targets are kept, and the retry runs it.
var errStartupVolumeBusy = errors.New("live CSI operation is in progress")

// startupErrOnlyBusy is whether every error in a pass's (joined, wrapped)
// error is errStartupVolumeBusy.
func startupErrOnlyBusy(err error) bool {
	for err != nil {
		if err == errStartupVolumeBusy { //nolint:errorlint // the leaf itself, not a wrapper of it
			return true
		}
		switch e := err.(type) { //nolint:errorlint // walks the tree errors.Is would, but needs every leaf
		case interface{ Unwrap() []error }:
			errs := e.Unwrap()
			if len(errs) == 0 {
				return false
			}
			for _, leaf := range errs {
				if !startupErrOnlyBusy(leaf) {
					return false
				}
			}
			return true
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		default:
			return false
		}
	}
	return false
}

// startupQuarantine is a volume quarantineStaleStartupFencingVolume carved
// out: its dataset and the key of the stale record blocking it.
type startupQuarantine struct {
	datasetName string
	staleKey    string
}

// healStartupQuarantines asks the startup loop to re-run each quarantined
// volume whose blocking stale record is gone. The stale-record revoke signals
// its own volume, but the record can also go another way (an operator, or a
// revoke that found it already gone); the periodic stale-record sweep calls
// this with the datasets it just listed, so no quarantine outlives its cause
// by more than one sweep. Quarantines are rare: this reads only theirs.
func (d *Driver) healStartupQuarantines(ctx context.Context, datasets []*truenas.Dataset) {
	d.startupReconcileTargetsMu.Lock()
	quarantined := make([]startupQuarantine, 0, len(d.startupQuarantined))
	for _, q := range d.startupQuarantined {
		quarantined = append(quarantined, q)
	}
	d.startupReconcileTargetsMu.Unlock()
	if len(quarantined) == 0 {
		return
	}
	byName := make(map[string]*truenas.Dataset, len(datasets))
	for _, dataset := range datasets {
		if dataset != nil {
			byName[dataset.Name] = dataset
		}
	}
	for _, q := range quarantined {
		dataset := byName[q.datasetName]
		if dataset == nil {
			continue
		}
		records, err := d.publications().records(ctx, q.datasetName, dataset)
		if err != nil {
			klog.V(2).Infof("Quarantined volume %s: publication records unreadable, not re-run yet: %v", q.datasetName, err)
			continue
		}
		if _, blocked := records[q.staleKey]; !blocked {
			d.requestStartupAttachmentReconcile(q.datasetName)
		}
	}
}

// clearStartupQuarantineVolume forgets one volume's quarantine. A pass calls
// it from the volume's own worker once the volume lock is held, so a volume
// the pass could not take (busy) keeps its quarantine and its gauge.
func (d *Driver) clearStartupQuarantineVolume(volumeID string) {
	d.startupReconcileTargetsMu.Lock()
	_, was := d.startupQuarantined[volumeID]
	delete(d.startupQuarantined, volumeID)
	d.startupReconcileTargetsMu.Unlock()
	if was {
		ClearStartupFencingUnconverged(volumeID)
	}
}

type startupPublication struct {
	identity NodeIdentity
	nodeID   string // the node's id as its CSINode advertises it; "" if it advertises none
	mode     csi.VolumeCapability_AccessMode_Mode
	readonly bool
}

type startupFencingVolume struct {
	volumeID         string
	volumeAttributes map[string]string
	publications     []startupPublication
	// claimedNodes is every node with a VolumeAttachment for this volume,
	// INCLUDING ones not yet or no longer Attached. Deliberately wider than
	// publications, and must match the predicate reconcileStalePublicationRecords
	// uses, or a quarantine can outlive any revoke that could clear it.
	claimedNodes map[string]struct{}
	// pv carries one PersistentVolume referencing this volume so per-volume
	// operator-attention conditions can be surfaced as Events, not just klog.
	pv *corev1.PersistentVolume
	// attachments is every VolumeAttachment of this volume in the pass's
	// snapshot, Attached or not, with the publication it would converge. The
	// per-volume refresh re-reads exactly these by name under the lock.
	attachments []startupAttachment
}

// startupAttachment is one VolumeAttachment from the snapshot. VolumeAttachment
// names are derived from (attacher, PV, node) and their spec is immutable, so
// a GET by name under the volume lock answers "is this attachment still there,
// still Attached, not being deleted" without listing the cluster.
type startupAttachment struct {
	name        string
	nodeName    string
	pvName      string
	publication startupPublication
}

// reconcilePublishedAttachments is the rolling-upgrade bridge from static
// transport authorization to durable per-volume publication records. It first
// takes a Kubernetes-only snapshot, then reconciles independent volumes with a
// four-worker bound. The TrueNAS client has ten request slots, so this leaves
// capacity for live CSI calls while startup convergence runs in the background.
func (d *Driver) reconcilePublishedAttachments(ctx context.Context) error {
	return d.reconcilePublishedAttachmentsFor(ctx, nil)
}

// reconcilePublishedAttachmentsFor is one pass over every attached volume
// (targets nil), or over only the volumes whose dataset is in targets: a
// re-run signal (a deferred fence, a revoked stale record) names the volumes
// it concerns, and the rest of the cluster is left alone.
func (d *Driver) reconcilePublishedAttachmentsFor(ctx context.Context, targets map[string]struct{}) error {
	if d.config == nil || !d.config.Fencing.Enabled() {
		return nil
	}
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil {
		return fmt.Errorf("fencing startup reconciliation requires Kubernetes client access")
	}
	clientset := d.eventRecorder.clientset

	pvList, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list PersistentVolumes for startup fencing reconciliation: %w", err)
	}
	attachmentList, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list VolumeAttachments for startup fencing reconciliation: %w", err)
	}
	csiNodeList, err := clientset.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list CSINodes for startup fencing reconciliation: %w", err)
	}
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list Nodes for startup fencing reconciliation: %w", err)
	}

	pvs := make(map[string]*corev1.PersistentVolume, len(pvList.Items))
	for i := range pvList.Items {
		pvs[pvList.Items[i].Name] = &pvList.Items[i]
	}
	csiNodes := make(map[string]*storagev1.CSINode, len(csiNodeList.Items))
	for i := range csiNodeList.Items {
		csiNodes[csiNodeList.Items[i].Name] = &csiNodeList.Items[i]
	}
	nodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes[nodeList.Items[i].Name] = &nodeList.Items[i]
	}

	volumes := make(map[string]*startupFencingVolume)
	var collectionErrors []error
	attachmentCount := 0
	for i := range attachmentList.Items {
		attachment := &attachmentList.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		// A VolumeAttachment that EXISTS but is not yet Attached, or is being
		// deleted, is still a live CLAIM for deciding whether a published record
		// is a stale leftover. reconcileStalePublicationRecords -- the only thing
		// that can clear a quarantine -- uses exactly this wider rule, and the two
		// predicates disagreeing made quarantine PERMANENT: a node mid-drain read
		// as STALE here (so the volume was quarantined) and LIVE there (so no
		// revoke fired and nothing ever signaled the loop). Strict mode then
		// latched ready with that volume's record and backend fence never written.
		attachedNow := attachment.Status.Attached && attachment.DeletionTimestamp.IsZero()
		pvName := *attachment.Spec.Source.PersistentVolumeName
		pv := pvs[pvName]
		if pv == nil {
			collectionErrors = append(collectionErrors, fmt.Errorf(
				"volume attachment %s references missing PersistentVolume %s", attachment.Name, pvName))
			continue
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != d.name || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}

		identity := startupNodeIdentity(d.name, attachment.Spec.NodeName, csiNodes[attachment.Spec.NodeName], nodes[attachment.Spec.NodeName])
		mode, readonly := accessModeForPersistentVolume(pv)
		publication := startupPublication{
			identity: identity,
			nodeID:   csiNodeID(d.name, csiNodes[attachment.Spec.NodeName]),
			mode:     mode,
			readonly: readonly,
		}
		volumeID := pv.Spec.CSI.VolumeHandle
		volume := volumes[volumeID]
		if volume == nil {
			volume = &startupFencingVolume{
				volumeID:         volumeID,
				volumeAttributes: pv.Spec.CSI.VolumeAttributes,
				pv:               pv,
			}
			volumes[volumeID] = volume
		}
		if volume.claimedNodes == nil {
			volume.claimedNodes = make(map[string]struct{})
		}
		volume.claimedNodes[attachment.Spec.NodeName] = struct{}{}
		volume.attachments = append(volume.attachments, startupAttachment{
			name: attachment.Name, nodeName: attachment.Spec.NodeName, pvName: pvName, publication: publication,
		})
		if !attachedNow {
			// A live claim, but nothing to converge a fence for on this pass.
			continue
		}
		volume.publications = append(volume.publications, publication)
		attachmentCount++
	}

	volumeIDs := make([]string, 0, len(volumes))
	for volumeID := range volumes {
		if targets != nil {
			datasetName, err := d.datasetForID(volumeID)
			if err != nil {
				continue
			}
			if _, targeted := targets[datasetName]; !targeted {
				continue
			}
		}
		volumeIDs = append(volumeIDs, volumeID)
	}
	sort.Strings(volumeIDs)
	if targets != nil {
		// Collection errors about volumes this pass does not touch are not
		// this pass's to report; the next full pass still sees them.
		collectionErrors = nil
		attachmentCount = 0
		for _, volumeID := range volumeIDs {
			attachmentCount += len(volumes[volumeID].publications)
		}
	}
	// (C11) Reset ONCE, before any worker can call RecordStartupFencingUnconverged,
	// so a volume that converges on THIS pass drops out of the gauge instead of
	// latching a stale 1 forever (mirrors ResetVolumeUsageMetrics). Must not run
	// concurrently with the per-volume Set calls below, which is why it happens
	// here rather than inside a worker. The quarantine set (startupQuarantined)
	// is reset the same way and for the same reason, so it never carries a
	// verdict a later pass over the same volume has replaced (see
	// quarantineStaleStartupFencingVolume and the field's doc comment in
	// driver.go).
	//
	// A targeted pass clears only its own volumes' quarantine verdicts, each
	// in the volume's worker once its lock is held: a volume it does not touch,
	// or cannot take, keeps its quarantine (and its gauge).
	if targets == nil {
		ResetStartupFencingUnconvergedVolumes()
		d.resetStartupQuarantine()
	}
	jobs := make(chan *startupFencingVolume)
	results := make(chan error, len(volumeIDs))
	workerCount := startupReconcileWorkers
	if len(volumeIDs) < workerCount {
		workerCount = len(volumeIDs)
	}
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for volume := range jobs {
				results <- d.reconcileStartupFencingVolume(ctx, volume)
			}
		}()
	}
	for _, volumeID := range volumeIDs {
		jobs <- volumes[volumeID]
	}
	close(jobs)
	workers.Wait()
	close(results)
	for result := range results {
		if result != nil {
			collectionErrors = append(collectionErrors, result)
		}
	}
	if len(collectionErrors) > 0 {
		return errors.Join(collectionErrors...)
	}
	klog.Infof("Startup fencing reconciliation converged: %d attached publication(s) across %d volume(s)",
		attachmentCount, len(volumeIDs))
	return nil
}

// csiNodeID is the id the CSINode advertises for driverName, "" if none.
func csiNodeID(driverName string, csiNode *storagev1.CSINode) string {
	if csiNode == nil {
		return ""
	}
	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == driverName {
			return driver.NodeID
		}
	}
	return ""
}

func startupNodeIdentity(
	driverName, nodeName string,
	csiNode *storagev1.CSINode,
	node *corev1.Node,
) NodeIdentity {
	identity := NodeIdentity{Name: nodeName, Legacy: true}
	if csiNode != nil {
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name != driverName {
				continue
			}
			if parsed, err := parseNodeIdentity(driver.NodeID); err == nil {
				identity = mergeNodeIdentity(identity, parsed)
			}
			break
		}
	}
	// VolumeAttachment.spec.nodeName is the durable Kubernetes identity even if
	// a stale or malformed CSINode advertises another display name.
	identity.Name = nodeName
	if node != nil {
		for _, address := range node.Status.Addresses {
			if ip := net.ParseIP(address.Address); ip != nil {
				identity.IPs = append(identity.IPs, ip)
			}
		}
		identity.IPs = canonicalNodeIPs(identity.IPs)
	}
	return identity
}

// quarantineStaleStartupFencingVolume records the C11 carve-out for ONE
// volume whose startup fencing convergence is blocked ONLY by a stale
// publication record (proven by stalePublishedRecordNode: staleNode has no
// live VolumeAttachment in this pass's snapshot). It logs, raises a Warning
// Event against the volume's PV, and marks the visibility gauge — then
// returns nil so this volume never joins reconcilePublishedAttachments'
// errors.Join and never holds strict-mode readiness down for every OTHER
// volume. Mirrors the errGeometryUnestablishable carve-out immediately below
// in this file, which uses the same log/event/return-nil shape, but for a
// different distinction: that carve-out is PERMANENT (no retry clears it,
// there is nothing to fence), while this one is a DEFERRAL. This volume's own
// publication record and backend fence are not written on this pass — the
// caller returns here before ever reaching that code — so the volume is
// recorded in startupQuarantined below: it is how
// startStartupAttachmentReconcile's strict branch knows to keep its reconcile
// goroutine alive on an otherwise-nil-error pass instead of exiting, so that a
// later requestStartupAttachmentReconcile signal — fired by
// revokeStalePublicationRecord once the stale record named above is actually
// revoked — has a live goroutine to wake and retry this volume. That signal
// names this volume's dataset, and the retry re-runs only it.
func (d *Driver) quarantineStaleStartupFencingVolume(volume *startupFencingVolume, datasetName, staleNode string, cause error) error {
	klog.Warningf("Startup fencing for volume %s is blocked by a stale publication record for node %s "+
		"(no live VolumeAttachment); not holding cluster-wide readiness on it. This volume's own publication "+
		"record and backend fence are deferred, not abandoned: convergence is retried automatically once the "+
		"stale record above is revoked (normally after fencing.staleRecordGracePeriod of continuous absence): %v",
		volume.volumeID, staleNode, cause)
	d.recordWarningEvent(volume.pv, "StartupFencingStaleRecordConflict",
		fmt.Sprintf("startup fencing quarantined (stale publication record for node %s, no live VolumeAttachment): %v",
			staleNode, cause))
	RecordStartupFencingUnconverged(volume.volumeID)
	d.startupReconcileTargetsMu.Lock()
	if d.startupQuarantined == nil {
		d.startupQuarantined = make(map[string]startupQuarantine)
	}
	d.startupQuarantined[volume.volumeID] = startupQuarantine{datasetName: datasetName, staleKey: publicationPropertyKey(staleNode)}
	d.startupReconcileTargetsMu.Unlock()
	return nil
}

// resetStartupQuarantine forgets every quarantine before a full pass.
func (d *Driver) resetStartupQuarantine() {
	d.startupReconcileTargetsMu.Lock()
	d.startupQuarantined = nil
	d.startupReconcileTargetsMu.Unlock()
}

// startupQuarantineCount is the number of volumes currently quarantined.
func (d *Driver) startupQuarantineCount() int {
	d.startupReconcileTargetsMu.Lock()
	defer d.startupReconcileTargetsMu.Unlock()
	return len(d.startupQuarantined)
}

// takeStartupReconcileTargets returns and forgets the datasets re-run signals
// have named since the last call.
func (d *Driver) takeStartupReconcileTargets() map[string]struct{} {
	d.startupReconcileTargetsMu.Lock()
	defer d.startupReconcileTargetsMu.Unlock()
	pending := d.startupReconcilePending
	d.startupReconcilePending = nil
	return pending
}

func (d *Driver) reconcileStartupFencingVolume(ctx context.Context, volume *startupFencingVolume) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lockKey := volumeLockKey(volume.volumeID)
	if !d.acquireOperationLock(lockKey) {
		return fmt.Errorf("startup reconcile volume %s: %w", volume.volumeID, errStartupVolumeBusy)
	}
	defer d.releaseOperationLock(lockKey)
	d.clearStartupQuarantineVolume(volume.volumeID)

	// The initial list only schedules work. Rebuild the current attachment set
	// after taking the same per-volume lock as ControllerPublish/Unpublish. A VA
	// with a deletion timestamp is already in the unpublish path and must never be
	// re-granted from the stale startup snapshot. The snapshot's own attachments
	// are re-read by name (one GET each) instead of listing every PV, VA,
	// CSINode and Node in the cluster per volume; a VA created after the
	// snapshot is published by its own ControllerPublishVolume.
	currentVolume, err := d.refreshStartupFencingVolume(ctx, volume)
	if err != nil {
		return fmt.Errorf("refresh attached volume %s: %w", volume.volumeID, err)
	}
	if len(currentVolume.publications) == 0 {
		return nil
	}
	volume = currentVolume
	datasetName, err := d.datasetForID(volume.volumeID)
	if err != nil {
		return fmt.Errorf("resolve attached volume %s: %w", volume.volumeID, err)
	}

	// The dataset and its publication properties must be read only after the
	// volume lock is held; startup now runs concurrently with the served CSI API.
	dataset, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return fmt.Errorf("read attached volume %s: %w", volume.volumeID, err)
	}
	records, err := d.publications().records(ctx, datasetName, dataset)
	if err != nil {
		return fmt.Errorf("read publication records for attached volume %s: %w", volume.volumeID, err)
	}
	if err := d.refreshStartupIdentities(ctx, volume, records); err != nil {
		return fmt.Errorf("refresh node identity for attached volume %s: %w", volume.volumeID, err)
	}
	shareType := shareTypeForPublishedVolume(dataset, volume.volumeAttributes)
	compatibilityRecords := make(map[string]publicationRecord, len(records)+len(volume.publications))
	for key := range records {
		compatibilityRecords[key] = records[key]
	}
	desired := make(map[string]publicationRecord)
	deferred := false
	// (C11) liveNodes is the set of node names this pass actually observed a
	// live VolumeAttachment for. A conflicting published record whose node is
	// NOT in this set cannot be a genuine concurrent claim — reconcileStale
	// PublicationRecords is the only thing that could still be racing it, and
	// that mechanism only ever REVOKES, never re-publishes. Used below to tell
	// that shape apart from a real (or transient dual-VA migration) conflict,
	// which must keep blocking exactly as before.
	// From claimedNodes, not publications: a node mid-drain must count as live
	// here or it is quarantined by a predicate the revoke path disagrees with,
	// and the quarantine never clears.
	liveNodes := make(map[string]struct{}, len(volume.claimedNodes)+len(volume.publications))
	for node := range volume.claimedNodes {
		liveNodes[node] = struct{}{}
	}
	for _, publication := range volume.publications {
		liveNodes[publication.identity.Name] = struct{}{}
	}
	// Per-volume memo so the validate/classify/ensure/apply phases resolve the
	// backend share objects once and reuse them across this startup pass (see
	// fenceResolution). The per-volume lock is held for the whole pass.
	res := &fenceResolution{}
	// stored is each key's record as it was read, before this pass touched it,
	// so an unchanged record is recognized and not rewritten.
	stored := make(map[string]publicationRecord, len(volume.publications))
	for _, publication := range volume.publications {
		isDeferred, identityErr := d.validateOrDeferFencingIdentity(datasetName, publication.identity, shareType)
		if identityErr != nil {
			return fmt.Errorf("cannot reconcile attached volume %s on node %s: %w",
				volume.volumeID, publication.identity.Name, identityErr)
		}
		record, recordErr := newPublicationRecord(publication.identity, publication.mode, publication.readonly)
		if recordErr != nil {
			return fmt.Errorf("encode attached node %s identity: %w", publication.identity.Name, recordErr)
		}
		record.keepCONodeID(publication.nodeID)
		if compatibilityErr := validatePublicationCompatibility(compatibilityRecords, record); compatibilityErr != nil {
			// (C11) A conflict whose ONLY blocking record has no live
			// VolumeAttachment at all is a stale record left by a force-removed
			// VA finalizer, not a real concurrent claim. Quarantine this ONE
			// volume — like the errGeometryUnestablishable carve-out below does
			// for a permanent geometry refusal — instead of holding cluster-wide
			// strict readiness down for up to fencing.staleRecordGracePeriod
			// (observed live: 10-20 minutes of no provisioning/attaches after
			// every controller restart, for a single stale volume). The
			// periodic reconcileStalePublicationRecords sweep is what actually
			// revokes the stale record; this only stops it from also blocking
			// every OTHER volume in the meantime.
			if staleNode, found, staleErr := d.confirmedStaleStartupRecord(ctx, volume.volumeID, compatibilityRecords, liveNodes); staleErr != nil {
				return staleErr
			} else if found {
				return d.quarantineStaleStartupFencingVolume(volume, datasetName, staleNode, compatibilityErr)
			}
			// Transient dual-VA states are normal during migration. Both modes retry
			// this volume; strict readiness remains false, but the process stays up.
			return fmt.Errorf("startup fencing for volume %s has not converged: %w", volume.volumeID, compatibilityErr)
		}
		if compatibilityErr := d.validateBackendSingleNodeCompatibility(
			ctx, dataset, datasetName, shareType, publication.identity, compatibilityRecords, publication.mode, res,
		); compatibilityErr != nil {
			// (C11) Same carve-out for the backend-allowlist half of the check.
			if staleNode, found, staleErr := d.confirmedStaleStartupRecord(ctx, volume.volumeID, compatibilityRecords, liveNodes); staleErr != nil {
				return staleErr
			} else if found {
				return d.quarantineStaleStartupFencingVolume(volume, datasetName, staleNode, compatibilityErr)
			}
			return fmt.Errorf("startup fencing for volume %s has not converged: %w", volume.volumeID, compatibilityErr)
		}
		key := publicationPropertyKey(publication.identity.Name)
		previous, hasPrevious := records[key]
		if hasPrevious {
			if _, seen := stored[key]; !seen {
				stored[key] = previous
			}
		}
		if ownershipErr := d.populateAdditiveGrantOwnership(
			ctx, dataset, datasetName, shareType, publication.identity,
			previous, hasPrevious, isDeferred, &record, res,
		); ownershipErr != nil {
			return fmt.Errorf("classify startup grant ownership for volume %s on node %s: %w",
				volume.volumeID, publication.identity.Name, ownershipErr)
		}
		compatibilityRecords[key] = record
		records[key] = record
		desired[key] = record
		if isDeferred {
			// Persist publication ownership for every captured VA, including legacy
			// identities. applyBackendFence skips only the deferred identity in
			// additive mode, so enforceable peers can still converge while the
			// preserved static policy carries the legacy node.
			deferred = true
			continue
		}
	}
	for key := range desired {
		record := desired[key]
		observationKey := stalePublicationObservationKey(datasetName, key)
		_, staleObserved := d.stalePublicationRecordsSeen.Load(observationKey)
		if previous, hasPrevious := stored[key]; hasPrevious && !staleObserved && samePublicationRecordExceptTime(previous, record) {
			// The same rule as a repeated ControllerPublishVolume: the stored
			// record already says exactly this, so a restart rewrites nothing.
			// A record the stale-record sweep is watching is rewritten, so a
			// revoke that detected it sees a new generation and backs off.
			records[key] = previous
		} else if err := d.publications().store(ctx, datasetName, dataset, key, record); err != nil {
			return fmt.Errorf("persist startup attachment for volume %s: %w", volume.volumeID, err)
		}
		d.stalePublicationRecordsSeen.Delete(observationKey)
	}
	if len(desired) > 0 {
		if err := d.ensureShareExists(ctx, dataset, datasetName, volume.volumeID, shareType, res); err != nil {
			// The PERMANENT geometry refusal (extent absent, layout unestablishable)
			// is a per-volume operator condition, not a convergence failure: no
			// retry clears it, the absent extent exposes no data path so there is
			// nothing to fence on this volume, and letting it join the error set
			// would hold strict-mode readiness — and with it CreateVolume /
			// ControllerPublishVolume / ControllerExpandVolume for EVERY volume —
			// down forever. Surface it loudly and let the rest of the pass converge.
			var permanent errGeometryUnestablishable
			if errors.As(err, &permanent) {
				klog.Warningf("Startup fencing for volume %s cannot rebuild its share until an operator records its "+
					"geometry or restores its extent; not blocking controller readiness on it: %v", volume.volumeID, err)
				d.recordWarningEvent(volume.pv, "StartupShareGeometryUnestablishable",
					fmt.Sprintf("startup share rebuild refused: %v", err))
				return nil
			}
			return fmt.Errorf("ensure share for startup attachment %s: %w", volume.volumeID, err)
		}
		if err := d.applyBackendFence(ctx, dataset, datasetName, shareType, records, res); err != nil {
			return fmt.Errorf("enforce startup attachment fence for volume %s: %w", volume.volumeID, err)
		}
	}
	if deferred {
		return fmt.Errorf("%w: startup fencing for volume %s is waiting for node identity re-registration",
			errFenceDeferred, volume.volumeID)
	}
	return nil
}

// refreshStartupFencingVolume re-reads, under the volume lock, each
// VolumeAttachment the snapshot saw for this volume: one GET by name each. A
// VA that is gone, or replaced by an object for another PV or node, no longer
// claims anything; one being deleted or no longer Attached still claims its
// node but is never (re)granted. Node identities still come from the
// snapshot here; reconcileStartupFencingVolume re-reads one wherever it would
// change the stored record (refreshStartupIdentities).
func (d *Driver) refreshStartupFencingVolume(ctx context.Context, snapshot *startupFencingVolume) (*startupFencingVolume, error) {
	result := &startupFencingVolume{
		volumeID:         snapshot.volumeID,
		volumeAttributes: snapshot.volumeAttributes,
		pv:               snapshot.pv,
		attachments:      snapshot.attachments,
	}
	attachments := d.eventRecorder.clientset.StorageV1().VolumeAttachments()
	for i := range snapshot.attachments {
		attachment := &snapshot.attachments[i]
		current, err := attachments.Get(ctx, attachment.name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get VolumeAttachment %s: %w", attachment.name, err)
		}
		if current.Spec.Attacher != d.name || current.Spec.NodeName != attachment.nodeName ||
			current.Spec.Source.PersistentVolumeName == nil || *current.Spec.Source.PersistentVolumeName != attachment.pvName {
			continue
		}
		if result.claimedNodes == nil {
			result.claimedNodes = make(map[string]struct{})
		}
		result.claimedNodes[attachment.nodeName] = struct{}{}
		if !current.Status.Attached || !current.DeletionTimestamp.IsZero() {
			continue
		}
		result.publications = append(result.publications, attachment.publication)
	}
	return result, nil
}

// refreshStartupIdentities re-reads, under the volume lock, the node identity
// of each grant whose snapshot identity would change the stored record. The
// snapshot can be a whole pass old: a node that re-registered since (a new
// address, NQN or IQN), and that a live publish already granted, would
// otherwise have its current identity revoked and the old one granted. A grant
// the stored record already matches needs no read, so a steady restart costs
// no CSINode or Node request at all.
func (d *Driver) refreshStartupIdentities(
	ctx context.Context,
	volume *startupFencingVolume,
	records map[string]publicationRecord,
) error {
	identities := make(map[string]startupNodeIdentityRead)
	for i := range volume.publications {
		publication := &volume.publications[i]
		if previous, ok := records[publicationPropertyKey(publication.identity.Name)]; ok {
			candidate, err := newPublicationRecord(publication.identity, publication.mode, publication.readonly)
			if err == nil {
				candidate.keepCONodeID(publication.nodeID)
				if samePublicationRecordExceptTime(previous, candidate) {
					continue
				}
			}
		}
		identity, nodeID, err := d.currentStartupNodeIdentity(ctx, publication.identity.Name, identities)
		if err != nil {
			return err
		}
		publication.identity, publication.nodeID = identity, nodeID
	}
	return nil
}

type startupNodeIdentityRead struct {
	identity NodeIdentity
	nodeID   string
}

// currentStartupNodeIdentity is startupNodeIdentity and csiNodeID over the
// node's CSINode and Node as they are now (absent is nil, as in a listing),
// read once per node per refresh.
func (d *Driver) currentStartupNodeIdentity(
	ctx context.Context,
	nodeName string,
	seen map[string]startupNodeIdentityRead,
) (NodeIdentity, string, error) {
	if read, ok := seen[nodeName]; ok {
		return read.identity, read.nodeID, nil
	}
	clientset := d.eventRecorder.clientset
	csiNode, err := clientset.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		csiNode, err = nil, nil
	}
	if err != nil {
		return NodeIdentity{}, "", fmt.Errorf("get CSINode %s: %w", nodeName, err)
	}
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		node, err = nil, nil
	}
	if err != nil {
		return NodeIdentity{}, "", fmt.Errorf("get Node %s: %w", nodeName, err)
	}
	read := startupNodeIdentityRead{
		identity: startupNodeIdentity(d.name, nodeName, csiNode, node),
		nodeID:   csiNodeID(d.name, csiNode),
	}
	seen[nodeName] = read
	return read.identity, read.nodeID, nil
}

// confirmedStaleStartupRecord is stalePublishedRecordNode over a liveNodes set
// re-read from a full VolumeAttachment listing. The per-volume refresh only
// re-reads the snapshot's own attachments, so it cannot see a VA created
// since; quarantining on that narrower view could defer a volume whose
// conflicting record belongs to a node that is in fact attached, which the
// stale-record sweep would then never revoke. Quarantine is rare, so it pays
// for the listing the common path no longer does.
func (d *Driver) confirmedStaleStartupRecord(ctx context.Context, volumeID string, records map[string]publicationRecord, liveNodes map[string]struct{}) (staleNode string, found bool, err error) {
	if _, found = stalePublishedRecordNode(records, liveNodes); !found {
		return "", false, nil
	}
	current, err := d.currentStartupFencingVolume(ctx, volumeID)
	if err != nil {
		return "", false, fmt.Errorf("confirm stale publication record for volume %s: %w", volumeID, err)
	}
	confirmed := make(map[string]struct{}, len(liveNodes)+len(current.claimedNodes))
	for node := range liveNodes {
		confirmed[node] = struct{}{}
	}
	for node := range current.claimedNodes {
		confirmed[node] = struct{}{}
	}
	staleNode, found = stalePublishedRecordNode(records, confirmed)
	return staleNode, found, nil
}

// currentStartupFencingVolume rebuilds a volume's attachment set from full
// listings. The per-volume pass uses refreshStartupFencingVolume; this one
// backs the rare quarantine decision (confirmedStaleStartupRecord).
func (d *Driver) currentStartupFencingVolume(ctx context.Context, volumeID string) (*startupFencingVolume, error) {
	result := &startupFencingVolume{volumeID: volumeID}
	clientset := d.eventRecorder.clientset
	pvList, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list PersistentVolumes: %w", err)
	}
	pvs := make(map[string]*corev1.PersistentVolume)
	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle == volumeID {
			pvs[pv.Name] = pv
		}
	}
	attachmentList, err := clientset.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list VolumeAttachments: %w", err)
	}
	type currentAttachment struct {
		nodeName string
		pv       *corev1.PersistentVolume
	}
	current := make([]currentAttachment, 0)
	for i := range attachmentList.Items {
		attachment := &attachmentList.Items[i]
		if attachment.Spec.Attacher != d.name || attachment.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pv := pvs[*attachment.Spec.Source.PersistentVolumeName]
		if pv == nil {
			continue
		}
		// claimedNodes must be populated HERE, not only in the collection pass.
		// reconcileStartupFencingVolume overwrites its snapshot with this
		// freshly-read volume, so a claimedNodes set built anywhere else is
		// discarded before liveNodes is derived from it — which made the
		// draining-node fix a silent no-op. Widen before the narrow filter, so
		// a VolumeAttachment that exists but is not yet or no longer Attached
		// still counts as a live CLAIM, matching the predicate
		// reconcileStalePublicationRecords uses. The two disagreeing is what
		// made a quarantine permanent: mid-drain read STALE here and LIVE
		// there, so no revoke ever fired to wake the loop.
		if result.claimedNodes == nil {
			result.claimedNodes = make(map[string]struct{})
		}
		result.claimedNodes[attachment.Spec.NodeName] = struct{}{}
		if !attachment.Status.Attached || !attachment.DeletionTimestamp.IsZero() {
			continue
		}
		current = append(current, currentAttachment{nodeName: attachment.Spec.NodeName, pv: pv})
	}
	if len(current) == 0 {
		return result, nil
	}
	csiNodeList, err := clientset.StorageV1().CSINodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list CSINodes: %w", err)
	}
	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Nodes: %w", err)
	}
	csiNodes := make(map[string]*storagev1.CSINode, len(csiNodeList.Items))
	for i := range csiNodeList.Items {
		csiNodes[csiNodeList.Items[i].Name] = &csiNodeList.Items[i]
	}
	nodes := make(map[string]*corev1.Node, len(nodeList.Items))
	for i := range nodeList.Items {
		nodes[nodeList.Items[i].Name] = &nodeList.Items[i]
	}
	for _, attachment := range current {
		identity := startupNodeIdentity(d.name, attachment.nodeName, csiNodes[attachment.nodeName], nodes[attachment.nodeName])
		mode, readonly := accessModeForPersistentVolume(attachment.pv)
		result.volumeAttributes = attachment.pv.Spec.CSI.VolumeAttributes
		result.pv = attachment.pv
		result.publications = append(result.publications, startupPublication{
			identity: identity, nodeID: csiNodeID(d.name, csiNodes[attachment.nodeName]), mode: mode, readonly: readonly,
		})
	}
	return result, nil
}

func accessModeForPersistentVolume(pv *corev1.PersistentVolume) (csi.VolumeCapability_AccessMode_Mode, bool) {
	if pv == nil {
		return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadWriteMany {
			return csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, false
		}
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadOnlyMany {
			return csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY, true
		}
	}
	for _, mode := range pv.Spec.AccessModes {
		if mode == corev1.ReadWriteOncePod {
			return csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER, false
		}
	}
	return csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, false
}

func (d *Driver) runStartupAttachmentReconcile(parent context.Context, targets map[string]struct{}) error {
	timeout, err := d.config.Fencing.StartupReconcileTimeoutDuration()
	if err != nil {
		return fmt.Errorf("invalid fencing.startupReconcileTimeout: %w", err)
	}
	if timeout <= 0 {
		return fmt.Errorf("fencing.startupReconcileTimeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	// Encryption at rest (GF-Sprint 1, E-2 §2 ordering): UNLOCK BEFORE the share
	// ensure pass. On the exact scenario the feature exists for — an appliance
	// reboot, which brings every encrypted volume up LOCKED (P-3/P-4) — this pass
	// would otherwise rebuild shares over locked zvols that have no backing device
	// (P-4) and fail each one in WaitForZvolReady before a single unlock had been
	// attempted. The publish path already honors this ordering; the startup path
	// must too. Internally gated on encryption.enabled and in-cluster client
	// access, so it is a strict no-op for every deployment that does not encrypt.
	d.reconcileEncryptedUnlocks(ctx)
	return d.reconcilePublishedAttachmentsFor(ctx, targets)
}

func (d *Driver) startStartupAttachmentReconcile() {
	if d.config == nil || !d.config.Fencing.Enabled() {
		return
	}
	signal := d.startupAttachmentReconcileSignal()
	ctx, cancel := context.WithCancel(context.Background())
	// (C7) startupReconcileStateMu + startupReconcileStopped close the race where
	// a Stop() landing between this function's entry and the plain assignment
	// below used to observe a nil startupReconcileCancel, skip cancellation, and
	// let this loop launch anyway — including reconcileEncryptedUnlocks and
	// reconcilePublishedAttachments, which WRITE backend fencing state — running
	// concurrently with GracefulStop()/truenasClient.Close(). A Stop() that
	// already won the race is visible here under the same lock, so this call
	// exits before ever launching the loop.
	d.startupReconcileStateMu.Lock()
	if d.startupReconcileStopped || d.startupReconcileCancel != nil {
		d.startupReconcileStateMu.Unlock()
		cancel()
		return
	}
	d.startupReconcileCancel = cancel
	// Add BEFORE releasing the lock that guards the cancel handle. Stop()
	// takes that same lock, then Wait()s; if Add lands after the unlock, a
	// Stop() in that window sees a counter of 0, Wait() returns
	// immediately, and the just-launched goroutine races Close(). That is
	// also the documented `sync: WaitGroup misuse: Add called concurrently
	// with Wait` panic shape.
	d.startupReconcileWg.Add(1)
	d.startupReconcileStateMu.Unlock()
	go func() {
		defer d.startupReconcileWg.Done()
		defer func() {
			d.startupReconcileTargetsMu.Lock()
			d.startupReconcileExited = true
			d.startupReconcilePending = nil
			d.startupReconcileTargetsMu.Unlock()
		}()
		backoff := startupReconcileInitialBackoff
		waitForSignal := false
		// full is true until a full pass has converged; after that, each
		// signal re-runs only the volumes it named (targets), and a failed
		// targeted pass retries those same volumes.
		full := true
		var targets map[string]struct{}
		for {
			if waitForSignal {
				// A converged additive controller, or a strict controller with
				// nothing left but quarantined volumes (C11), stays idle here. A
				// later publish that must defer fencing (recordFencingDeferred) or
				// a stale-record revoke that just unblocked a quarantined volume
				// (revokeStalePublicationRecord) signals this channel, allowing
				// retry without a permanent cluster-wide polling and backend-write
				// loop.
				select {
				case <-signal:
					backoff = startupReconcileInitialBackoff
					waitForSignal = false
				case <-ctx.Done():
					return
				}
			}
			pending := d.takeStartupReconcileTargets()
			if !full {
				if targets == nil {
					targets = make(map[string]struct{}, len(pending))
				}
				for datasetName := range pending {
					targets[datasetName] = struct{}{}
				}
				if len(targets) == 0 {
					// The volumes this signal named were already taken by an
					// earlier pass; nothing is left to re-run.
					targets = nil
					waitForSignal = true
					continue
				}
			}
			var passTargets map[string]struct{}
			if !full {
				passTargets = targets
			}
			err := d.runStartupAttachmentReconcile(ctx, passTargets)
			if err == nil {
				full = false
				targets = nil
				if d.config.Fencing.Mode == FencingModeStrict {
					d.ready.Store(true)
					if d.startupQuarantineCount() == 0 {
						// Every volume genuinely converged on this pass: nothing is
						// deferred, so there is nothing left for this goroutine to
						// retry. Exiting here (rather than idling forever) is the
						// original, intended behavior for a fully converged strict
						// controller.
						return
					}
					// (C11 fix) At least one volume is QUARANTINED rather than
					// genuinely converged (quarantineStaleStartupFencingVolume
					// recorded it in startupQuarantined instead of joining a
					// pass's errors). Readiness still latches true — that is the
					// whole point of the carve-out, and must be preserved — but this
					// goroutine must NOT exit: it is the only thing that will ever
					// write the quarantined volume's own publication record and
					// backend fence. Fall through to the shared waitForSignal path
					// below so the next reconcilePublishedAttachments pass runs when
					// revokeStalePublicationRecord signals that the stale record
					// blocking it has been revoked, instead of busy-polling.
				}
				waitForSignal = true
				continue
			}
			if ctx.Err() != nil {
				return
			}
			// A targeted pass runs only after a full pass converged, on volumes
			// that pass left converged or quarantined. One that only met busy
			// volumes keeps readiness (their quarantine and gauge stand, and
			// the targets are kept for the retry below): dropping it would gate
			// every CSI call in the cluster on a lock some other operation holds.
			if d.config.Fencing.Mode == FencingModeStrict && (full || !startupErrOnlyBusy(err)) {
				d.ready.Store(false)
			}
			klog.Warningf("Background startup fencing reconciliation incomplete; retrying in %v: %v", backoff, err)
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			backoff *= 2
			if backoff > startupReconcileMaxBackoff {
				backoff = startupReconcileMaxBackoff
			}
		}
	}()
}

func (d *Driver) startupAttachmentReconcileSignal() <-chan struct{} {
	d.startupReconcileOnce.Do(func() {
		d.startupReconcileSignal = make(chan struct{}, 1)
	})
	return d.startupReconcileSignal
}

// requestStartupAttachmentReconcile asks the startup reconcile loop to re-run
// the volume whose dataset is datasetName, and only that volume. The request
// is dropped harmlessly if the loop has already exited.
func (d *Driver) requestStartupAttachmentReconcile(datasetName string) {
	if startupReconcileRequestedHook != nil {
		startupReconcileRequestedHook(datasetName)
	}
	d.startupReconcileTargetsMu.Lock()
	if d.startupReconcileExited {
		d.startupReconcileTargetsMu.Unlock()
		return
	}
	if d.startupReconcilePending == nil {
		d.startupReconcilePending = make(map[string]struct{})
	}
	d.startupReconcilePending[datasetName] = struct{}{}
	d.startupReconcileTargetsMu.Unlock()
	d.startupAttachmentReconcileSignal()
	select {
	case d.startupReconcileSignal <- struct{}{}:
	default:
		// A pending signal already covers this deferral.
	}
}

func (d *Driver) stopStartupAttachmentReconcile() {
	// (C7) Terminal: startupReconcileStopped=true is recorded under the same
	// lock startStartupAttachmentReconcile checks before assigning
	// startupReconcileCancel, so a Stop() that wins the race prevents the loop
	// from EVER starting instead of the two racing on a plain nil check.
	d.startupReconcileStateMu.Lock()
	d.startupReconcileStopped = true
	cancel := d.startupReconcileCancel
	d.startupReconcileCancel = nil
	d.startupReconcileStateMu.Unlock()
	if cancel != nil {
		cancel()
	}
	d.startupReconcileWg.Wait()
}
