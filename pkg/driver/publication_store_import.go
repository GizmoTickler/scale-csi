package driver

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// importingPublicationStore keeps records in Kubernetes and moves the ones an
// older release left on ZFS there. It reads both and resolves them (see
// resolvePublicationRecords); a write of a volume's records stores the
// resolved set in Kubernetes and removes the ZFS ones from the dataset (one
// dataset update, once). A volume no write touches keeps its ZFS records
// until the background import moves them.
//
// The order of every write keeps a crash harmless: a record is copied to
// Kubernetes before it leaves ZFS, and a removal takes the ZFS copy away
// before the Kubernetes one, so no crash brings a removed record back.
type importingPublicationStore struct {
	kube   kubernetesPublicationStore
	legacy zfsPublicationStore
	// cache answers the reads that only report records; nil reads the API.
	cache *publicationCache
}

// cachedRecords is records with the Kubernetes side read from the cache,
// once it has synced.
func (s importingPublicationStore) cachedRecords(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	if s.cache == nil {
		return s.records(ctx, datasetName, ds)
	}
	current, synced, err := s.cache.records(datasetName)
	if err != nil {
		return nil, err
	}
	if !synced {
		return s.records(ctx, datasetName, ds)
	}
	legacy, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	return resolvePublicationRecords(legacy, current), nil
}

func (s importingPublicationStore) records(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	legacy, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	current, err := s.kube.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	return resolvePublicationRecords(legacy, current), nil
}

func (s importingPublicationStore) lockedRecords(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	legacy, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	current, err := s.kube.lockedRecords(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	return resolvePublicationRecords(legacy, current), nil
}

// resolvePublicationRecords decides a volume's records from its ZFS records
// (legacy) and its VolumePublications (current).
//
//   - When the newest ZFS record is newer than every VolumePublication, a
//     release that keeps records on ZFS has run since this one last wrote the
//     volume (a rollback, then this release again): ZFS decides. That release
//     removed from ZFS whatever it unpublished, so a published
//     VolumePublication it never saw must not survive; an "unpublishing" one
//     for a node ZFS does not name is kept, since it can only revoke.
//   - Otherwise the two merge per key, the newer copy winning and Kubernetes
//     on a tie: ZFS records not yet imported, and after a crash between a
//     Kubernetes write and the ZFS removal, the Kubernetes copy.
func resolvePublicationRecords(legacy, current map[string]publicationRecord) map[string]publicationRecord {
	if len(legacy) == 0 {
		return current
	}
	if len(current) == 0 {
		return legacy
	}
	if newestPublicationRecord(legacy).After(newestPublicationRecord(current)) {
		// A tombstone this release wrote is kept unless ZFS names the node:
		// it can only lead to a revoke, and it holds the identity a pending
		// revoke needs.
		out := make(map[string]publicationRecord, len(legacy))
		for key := range legacy {
			out[key] = legacy[key]
		}
		for key := range current {
			if _, named := out[key]; !named && current[key].State == publicationStateRemoving {
				out[key] = current[key]
			}
		}
		return out
	}
	out := make(map[string]publicationRecord, len(legacy))
	for key := range legacy {
		out[key] = legacy[key]
	}
	for key := range current {
		if old, ok := out[key]; ok && publicationRecordTime(old).After(publicationRecordTime(current[key])) {
			continue
		}
		out[key] = current[key]
	}
	return out
}

func publicationRecordTime(record publicationRecord) time.Time {
	at, err := time.Parse(time.RFC3339Nano, record.UpdatedAt)
	if err != nil {
		return time.Time{}
	}
	return at
}

func newestPublicationRecord(records map[string]publicationRecord) time.Time {
	var newest time.Time
	for key := range records {
		if at := publicationRecordTime(records[key]); at.After(newest) {
			newest = at
		}
	}
	return newest
}

// store resolves and imports the volume's ZFS records before writing: the
// write's own fresh time must not decide that resolution.
func (s importingPublicationStore) store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error {
	if err := s.importLegacy(ctx, datasetName, ds, nil); err != nil {
		return err
	}
	return s.kube.store(ctx, datasetName, ds, key, record)
}

func (s importingPublicationStore) remove(ctx context.Context, datasetName string, ds *truenas.Dataset, keys []string) error {
	removing := make(map[string]bool, len(keys))
	for _, key := range keys {
		removing[key] = true
	}
	if err := s.importLegacy(ctx, datasetName, ds, removing); err != nil {
		return err
	}
	return s.kube.remove(ctx, datasetName, ds, keys)
}

func (s importingPublicationStore) forget(ctx context.Context, datasetName string) error {
	return s.kube.forget(ctx, datasetName)
}

// importLegacy stores the dataset's resolved records in Kubernetes (except
// those being removed), deletes the VolumePublications the resolution
// dropped, then removes every ZFS record from the dataset. Callers hold the
// volume lock.
func (s importingPublicationStore) importLegacy(ctx context.Context, datasetName string, ds *truenas.Dataset, removing map[string]bool) error {
	legacy, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil || len(legacy) == 0 {
		return err
	}
	current, err := s.kube.lockedRecords(ctx, datasetName, ds)
	if err != nil {
		return err
	}
	resolved := resolvePublicationRecords(legacy, current)
	keys := make([]string, 0, len(legacy))
	for key := range legacy {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if removing[key] {
			continue
		}
		record, kept := resolved[key]
		if existing, ok := current[key]; !kept || (ok && reflect.DeepEqual(existing, record)) {
			continue
		}
		if err := s.kube.store(ctx, datasetName, ds, key, record); err != nil {
			return err
		}
	}
	dropped := make([]string, 0)
	for key := range current {
		if _, kept := resolved[key]; !kept {
			dropped = append(dropped, key)
		}
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		if err := s.kube.remove(ctx, datasetName, ds, dropped); err != nil {
			return err
		}
	}
	return s.legacy.remove(ctx, datasetName, ds, keys)
}
