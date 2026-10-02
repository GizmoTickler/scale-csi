package driver

import (
	"context"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// publicationStore holds the driver's publication records: one per (volume,
// node), addressed by the volume's dataset name and the record key
// (publicationPropertyKey of the node). Callers hold the volume's lock.
//
// ds is the dataset the caller already read. The ZFS store reads the records
// from it and mirrors its writes into it, so a caller's later fence
// computation sees them without another read.
type publicationStore interface {
	// records reads the records for a caller that only reports or judges
	// them. It never sets what a later write is compared against.
	records(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error)
	// lockedRecords is the read a write is decided on. The caller holds the
	// volume lock, and the store's next store or remove of these records is
	// a compare-and-set against what this read saw.
	lockedRecords(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error)
	store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error
	remove(ctx context.Context, datasetName string, ds *truenas.Dataset, keys []string) error
	// forget drops every record of a deleted volume's dataset.
	forget(ctx context.Context, datasetName string) error
}

// zfsPublicationStore keeps each record as a user property on the volume's
// dataset (scale-csi:publication_<hash(node)>).
type zfsPublicationStore struct {
	client truenas.ClientInterface
}

func (s zfsPublicationStore) records(_ context.Context, _ string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	return publicationRecordsFromDataset(ds)
}

func (s zfsPublicationStore) lockedRecords(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	return s.records(ctx, datasetName, ds)
}

func (s zfsPublicationStore) store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error {
	return storePublicationRecord(ctx, s.client, ds, datasetName, key, record)
}

func (s zfsPublicationStore) remove(ctx context.Context, datasetName string, ds *truenas.Dataset, keys []string) error {
	return removePublicationRecords(ctx, s.client, ds, datasetName, keys)
}

// forget is a no-op: the records went with the dataset.
func (s zfsPublicationStore) forget(context.Context, string) error { return nil }

// publications is the driver's record store: the configured one, else the
// ZFS store on the driver's TrueNAS client.
// testPublicationStore is nil outside tests. The test suite sets it to run
// the record tests against VolumePublications as well as ZFS.
var testPublicationStore func(*Driver) publicationStore

func (d *Driver) publications() publicationStore {
	if d.publicationStore != nil {
		return d.publicationStore
	}
	if testPublicationStore != nil {
		return testPublicationStore(d)
	}
	return zfsPublicationStore{client: d.truenasClient}
}
