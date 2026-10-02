package driver

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// SCALE_CSI_TEST_PUBLICATION_STORE=kubernetes runs the suite with every
// driver that has no store of its own keeping records as VolumePublications
// (with the import from ZFS), one fake API per driver.
const testPublicationStoreEnv = "SCALE_CSI_TEST_PUBLICATION_STORE"

var testStores sync.Map // *Driver -> publicationStore

func init() {
	if os.Getenv(testPublicationStoreEnv) != publicationStoreKubernetes {
		return
	}
	testPublicationStore = func(d *Driver) publicationStore {
		if store, ok := testStores.Load(d); ok {
			return store.(publicationStore)
		}
		store, _ := testStores.LoadOrStore(d, importingPublicationStore{
			kube:   newKubernetesPublicationStore(newFakeVolumePublicationClient(), "scale-csi", "suite"),
			legacy: zfsPublicationStore{client: d.truenasClient},
		})
		return store.(publicationStore)
	}
}

// recordsInKubernetes reports a run with records kept as VolumePublications.
func recordsInKubernetes() bool { return testPublicationStore != nil }

// skipWithRecordsInKubernetes skips a test of the ZFS store itself (its
// property writes or call counts) in the run against VolumePublications.
func skipWithRecordsInKubernetes(t *testing.T, why string) {
	t.Helper()
	if recordsInKubernetes() {
		t.Skipf("records on ZFS only: %s", why)
	}
}

// mustStoredRecords is storedPublicationRecords for a test that expects
// the read to succeed.
func mustStoredRecords(t *testing.T, d *Driver, ds *truenas.Dataset) map[string]publicationRecord {
	t.Helper()
	records, err := storedPublicationRecords(d, ds)
	if err != nil {
		t.Fatalf("read the publication records of %s: %v", ds.Name, err)
	}
	return records
}

// storedPublicationRecords reads a volume's records the way the driver does,
// from whichever store it keeps them in.
func storedPublicationRecords(d *Driver, ds *truenas.Dataset) (map[string]publicationRecord, error) {
	return d.publications().records(context.Background(), ds.Name, ds)
}
