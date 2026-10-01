package driver

import (
	"context"
	"sort"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// importingPublicationStore keeps records in Kubernetes and moves the ones an
// older release left on ZFS there. It reads both, Kubernetes winning per key,
// and on a write of a volume's records it imports that volume's ZFS records
// and removes them from the dataset (one dataset update, once). A volume no
// write touches keeps its ZFS records until the background import moves them.
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
	out, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	for key := range current {
		out[key] = current[key]
	}
	return out, nil
}

func (s importingPublicationStore) records(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	out, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	current, err := s.kube.records(ctx, datasetName, ds)
	if err != nil {
		return nil, err
	}
	for key := range current {
		out[key] = current[key]
	}
	return out, nil
}

func (s importingPublicationStore) store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error {
	if err := s.kube.store(ctx, datasetName, ds, key, record); err != nil {
		return err
	}
	return s.importLegacy(ctx, datasetName, ds, nil)
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

// importLegacy copies the dataset's ZFS records that Kubernetes lacks (except
// those being removed) to Kubernetes, then removes every ZFS record from the
// dataset. Callers hold the volume lock.
func (s importingPublicationStore) importLegacy(ctx context.Context, datasetName string, ds *truenas.Dataset, removing map[string]bool) error {
	legacy, err := s.legacy.records(ctx, datasetName, ds)
	if err != nil || len(legacy) == 0 {
		return err
	}
	current, err := s.kube.records(ctx, datasetName, ds)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(legacy))
	for key := range legacy {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, imported := current[key]; imported || removing[key] {
			continue
		}
		if err := s.kube.store(ctx, datasetName, ds, key, legacy[key]); err != nil {
			return err
		}
	}
	return s.legacy.remove(ctx, datasetName, ds, keys)
}
