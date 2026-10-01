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
	records(ctx context.Context, datasetName string, ds *truenas.Dataset) (map[string]publicationRecord, error)
	store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error
	remove(ctx context.Context, datasetName string, ds *truenas.Dataset, keys []string) error
}

// zfsPublicationStore keeps each record as a user property on the volume's
// dataset (scale-csi:publication_<hash(node)>).
type zfsPublicationStore struct {
	client truenas.ClientInterface
}

func (s zfsPublicationStore) records(_ context.Context, _ string, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	return publicationRecordsFromDataset(ds)
}

func (s zfsPublicationStore) store(ctx context.Context, datasetName string, ds *truenas.Dataset, key string, record publicationRecord) error {
	return storePublicationRecord(ctx, s.client, ds, datasetName, key, record)
}

func (s zfsPublicationStore) remove(ctx context.Context, datasetName string, ds *truenas.Dataset, keys []string) error {
	return removePublicationRecords(ctx, s.client, ds, datasetName, keys)
}

// publications is the driver's record store: the configured one, else the
// ZFS store on the driver's TrueNAS client.
func (d *Driver) publications() publicationStore {
	if d.publicationStore != nil {
		return d.publicationStore
	}
	return zfsPublicationStore{client: d.truenasClient}
}
