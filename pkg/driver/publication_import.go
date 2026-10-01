package driver

import (
	"context"
	"path"
	"time"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

var (
	// publicationImportDelay lets the startup reconcile go first.
	publicationImportDelay = time.Minute
	// publicationImportRetry is the wait before a pass that left records.
	publicationImportRetry = 10 * time.Minute
	// publicationImportPace spaces the volumes of a pass.
	publicationImportPace = 500 * time.Millisecond
)

// startPublicationImport moves the publication records an older release left
// on ZFS into Kubernetes, for the volumes no write touches: a pass after the
// start, then again until a pass leaves none. It runs at delete class, one
// volume at a time.
func (d *Driver) startPublicationImport() {
	store, ok := d.publications().(importingPublicationStore)
	if !ok {
		return
	}
	// The pass is a background writer, so it needs the single controller that
	// fencing guarantees (one replica, Recreate). With fencing off the chart
	// allows two replicas and rolling updates, and a second process's pass,
	// working from a dataset read before another process unpublished a node,
	// would bring that node's record back. There, records move on each
	// volume's next publish or unpublish, which only the attacher's leader
	// receives.
	if d.config == nil || d.config.Fencing.Mode == FencingModeOff || d.config.Fencing.Mode == "" {
		klog.Info("Publication records: no background import with fencing off; records move on each volume's next write")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Stop() takes the same lock and sets the terminal flag first, so a Stop()
	// that wins the race keeps the loop from ever starting (the C7 pattern of
	// startCapacityGauges): a pass calls TrueNAS and must not outlive Close().
	d.publicationImportStateMu.Lock()
	if d.publicationImportStopped || d.publicationImportCancel != nil {
		d.publicationImportStateMu.Unlock()
		cancel()
		return
	}
	d.publicationImportCancel = cancel
	d.publicationImportWg.Add(1)
	d.publicationImportStateMu.Unlock()
	go func() {
		defer d.publicationImportWg.Done()
		wait := publicationImportDelay
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			remaining, err := d.importPublicationRecordsPass(ctx, store)
			switch {
			case ctx.Err() != nil:
				return
			case err != nil:
				d.recordReconcileObjectFailure("publication_import", "listing", err)
			case remaining == 0:
				klog.Info("Publication records: none left on ZFS; the import is complete")
				return
			default:
				klog.Infof("Publication records: %d volumes still hold records on ZFS; next import pass in %v", remaining, publicationImportRetry)
			}
			wait = publicationImportRetry
		}
	}()
}

func (d *Driver) stopPublicationImport() {
	d.publicationImportStateMu.Lock()
	d.publicationImportStopped = true
	cancel := d.publicationImportCancel
	d.publicationImportCancel = nil
	d.publicationImportStateMu.Unlock()
	if cancel != nil {
		cancel()
	}
	d.publicationImportWg.Wait()
}

// importPublicationRecordsPass imports the ZFS records of every volume whose
// listing shows any, and returns how many it could not (busy or failed). Each
// volume is read again under its lock: a listing taken before a concurrent
// unpublish would bring back the records it removed.
func (d *Driver) importPublicationRecordsPass(ctx context.Context, store importingPublicationStore) (remaining int, err error) {
	ctx = truenas.WithPriority(ctx, truenas.PriorityDelete)
	datasets, err := d.listAllManagedDatasets(ctx)
	if err != nil {
		return 0, err
	}
	first := true
	for _, listed := range datasets {
		if listed == nil || !datasetHasPublicationRecordKeys(listed) {
			continue
		}
		if !first {
			select {
			case <-ctx.Done():
				return remaining, ctx.Err()
			case <-time.After(publicationImportPace):
			}
		}
		first = false
		if !d.importPublicationRecordsOf(ctx, store, listed.Name) {
			remaining++
		}
	}
	return remaining, nil
}

// importPublicationRecordsOf imports one volume's ZFS records under its lock;
// false means they are still there.
func (d *Driver) importPublicationRecordsOf(ctx context.Context, store importingPublicationStore, datasetName string) bool {
	lockKey := volumeLockKey(path.Base(datasetName))
	if !d.acquireOperationLock(lockKey) {
		return false
	}
	defer d.releaseOperationLock(lockKey)
	ds, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if truenas.IsNotFoundError(err) {
		return true
	}
	if err == nil {
		err = store.importLegacy(ctx, datasetName, ds, nil)
	}
	if err != nil {
		d.recordReconcileObjectFailure("publication_import", datasetName, err)
		return false
	}
	return true
}
