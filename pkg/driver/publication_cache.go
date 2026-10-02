package driver

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const volumePublicationDatasetIndex = "dataset"

// publicationCacheSyncTimeout bounds the wait for the first listing at
// startup; until the cache syncs, readers ask the API instead.
var publicationCacheSyncTimeout = 30 * time.Second

// publicationCache is a watch-fed copy of the instance's VolumePublications,
// indexed by dataset. ListVolumes and ControllerGetVolume read it: they answer
// the external-attacher's resync every minute, which must not cost an API
// call per volume. A copy lagging by milliseconds is harmless there, since the
// attacher only asks again later; every write path reads the API.
type publicationCache struct {
	store    kubernetesPublicationStore
	informer cache.SharedIndexInformer
	stop     chan struct{}
	once     sync.Once
}

func newPublicationCache(store kubernetesPublicationStore) (*publicationCache, error) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(store.client, 0, store.namespace,
		func(options *metav1.ListOptions) { options.LabelSelector = store.instanceSelector() })
	informer := factory.ForResource(volumePublicationGVR).Informer()
	err := informer.AddIndexers(cache.Indexers{volumePublicationDatasetIndex: func(obj interface{}) ([]string, error) {
		object, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return nil, nil
		}
		return []string{object.GetLabels()[labelVolumePublicationDS]}, nil
	}})
	if err != nil {
		return nil, fmt.Errorf("index the publication cache: %w", err)
	}
	return &publicationCache{store: store, informer: informer, stop: make(chan struct{})}, nil
}

// start runs the watch and waits, up to the sync timeout, for the first
// listing. A cache that has not synced is not an error: readers use the API.
func (c *publicationCache) start(ctx context.Context) {
	go c.informer.Run(c.stop)
	syncCtx, cancel := context.WithTimeout(ctx, publicationCacheSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), c.informer.HasSynced) {
		klog.Warningf("Publication cache not synced after %v; ListVolumes reads the API until it is", publicationCacheSyncTimeout)
	}
}

func (c *publicationCache) close() {
	c.once.Do(func() { close(c.stop) })
}

// records answers from the cache; ok is false until it has synced.
func (c *publicationCache) records(datasetName string) (records map[string]publicationRecord, ok bool, err error) {
	if !c.informer.HasSynced() {
		return nil, false, nil
	}
	items, err := c.informer.GetIndexer().ByIndex(volumePublicationDatasetIndex, shortHash(datasetName))
	if err != nil {
		return nil, false, err
	}
	objects := make([]*unstructured.Unstructured, 0, len(items))
	for _, item := range items {
		if object, isObject := item.(*unstructured.Unstructured); isObject {
			objects = append(objects, object)
		}
	}
	records, err = c.store.recordsOf(datasetName, objects)
	return records, err == nil, err
}

// cachedPublicationReader is a store with a read that costs no API call.
type cachedPublicationReader interface {
	cachedRecords(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error)
}

// readPublicationRecordsCached reads a volume's records for a caller that only
// reports them (ListVolumes, ControllerGetVolume): from the cache where the
// store has one.
func readPublicationRecordsCached(ctx context.Context, store publicationStore, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	if reader, ok := store.(cachedPublicationReader); ok {
		return reader.cachedRecords(ctx, datasetName, ds)
	}
	return store.records(ctx, datasetName, ds)
}
