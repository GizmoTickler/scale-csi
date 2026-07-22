package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// mustCreateParentDataset materializes the CSI parent dataset in the mock. On a
// real backend it always exists; the driver now also stores durable bookkeeping
// (in-flight creation markers, the tombstone ledger) on it.
func mustCreateParentDataset(t *testing.T, client truenas.ClientInterface) {
	t.Helper()
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
}

type controllerCallCountingMock struct {
	*truenas.MockClient
	datasetGetCalls       int
	snapshotListCalls     int
	dependentCloneQueries int
}

type busyDatasetDeleteMock struct {
	*controllerCallCountingMock
}

type clonePropertyFailureMock struct {
	*truenas.MockClient
	err error
}

type datasetCreateCaptureMock struct {
	*truenas.MockClient
	params []*truenas.DatasetCreateParams
}

type racedDatasetCreateMock struct {
	*truenas.MockClient
	owner string
}

type racedSnapshotCloneDestinationMock struct {
	*truenas.MockClient
}

func (m *racedSnapshotCloneDestinationMock) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	// The competing request wins between CreateVolume's initial DatasetGet and
	// this request's clone call. The second low-level call must report that this
	// caller did not create the destination, even though the origin matches.
	if err := m.MockClient.SnapshotClone(ctx, snapshotID, newDatasetName); err != nil {
		return err
	}
	return &truenas.ErrDatasetDestinationExists{
		Destination: newDatasetName, ExpectedOrigin: snapshotID,
		VerificationErr: errors.New("transient origin verification failure"),
	}
}

type racedDetachedCopyDestinationMock struct {
	*truenas.MockClient
}

func (m *racedDetachedCopyDestinationMock) CopyDatasetFromSnapshotLocal(
	ctx context.Context,
	sourceDataset, snapshotShortName, targetDataset string,
) error {
	if err := m.MockClient.CopyDatasetFromSnapshotLocal(ctx, sourceDataset, snapshotShortName, targetDataset); err != nil {
		return err
	}
	return m.MockClient.CopyDatasetFromSnapshotLocal(ctx, sourceDataset, snapshotShortName, targetDataset)
}

func (m *racedDatasetCreateMock) DatasetCreate(ctx context.Context, params *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	// Simulate another controller winning after this caller's initial NotFound
	// read but before pool.dataset.create. The real client converts EEXIST to an
	// existing Dataset response with CreatedByCall=false.
	if _, err := m.MockClient.DatasetCreate(ctx, params); err != nil {
		return nil, err
	}
	if err := m.DatasetSetUserProperties(ctx, params.Name, map[string]string{
		PropDriverInstanceID: m.owner,
		PropManagedResource:  "true",
		PropCSIVolumeName:    strings.TrimPrefix(params.Name, "pool/parent/"),
	}); err != nil {
		return nil, err
	}
	return m.MockClient.DatasetCreate(ctx, params)
}

type silentCloneOwnerUpdateMock struct {
	*truenas.MockClient
}

func (m *silentCloneOwnerUpdateMock) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if len(properties) == 1 && properties[PropDriverInstanceID] != "" {
		// Model an acknowledged update that did not persist. Clone ownership is
		// not safe to trust until the authoritative re-read verifies source=local.
		return nil
	}
	return m.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

type silentDatasetPropertyUpdateMock struct {
	*truenas.MockClient
}

func (m *silentDatasetPropertyUpdateMock) DatasetSetUserProperties(context.Context, string, map[string]string) error {
	return nil
}

func (m *datasetCreateCaptureMock) DatasetCreate(ctx context.Context, params *truenas.DatasetCreateParams) (*truenas.Dataset, error) {
	copyParams := *params
	copyParams.UserProperties = append([]truenas.UserPropertyUpdate(nil), params.UserProperties...)
	m.params = append(m.params, &copyParams)
	return m.MockClient.DatasetCreate(ctx, params)
}

func (m *clonePropertyFailureMock) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if _, authoritative := properties[PropVolumeOriginSnapshot]; authoritative {
		return m.err
	}
	return m.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

type originSnapshotDeleteFailureMock struct {
	*truenas.MockClient
	originSnapshotID string
	err              error
}

func (m *originSnapshotDeleteFailureMock) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	if snapshotID == m.originSnapshotID {
		return m.err
	}
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

type transientOriginSnapshotDeleteFailureMock struct {
	*truenas.MockClient
	originSnapshotID string
	failuresLeft     int
	deleteCalls      int
}

func (m *transientOriginSnapshotDeleteFailureMock) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	if snapshotID == m.originSnapshotID {
		m.deleteCalls++
		if m.failuresLeft > 0 {
			m.failuresLeft--
			return fmt.Errorf("temporary snapshot delete failure")
		}
	}
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

type nfsShareCreateFailureMock struct {
	*truenas.MockClient
	err error
}

type missingDatasetCleanupFailureMock struct {
	*truenas.MockClient
}

func (m *missingDatasetCleanupFailureMock) DatasetGet(context.Context, string) (*truenas.Dataset, error) {
	return nil, &truenas.APIError{Code: -1, Message: "dataset not found"}
}

func (m *missingDatasetCleanupFailureMock) NVMeoFNamespaceFindByDevicePath(context.Context, string) (*truenas.NVMeoFNamespace, error) {
	return nil, fmt.Errorf("backend query unavailable")
}

func (m *nfsShareCreateFailureMock) NFSShareCreate(context.Context, *truenas.NFSShareCreateParams) (*truenas.NFSShare, error) {
	return nil, m.err
}

type cloneDependencyMock struct {
	*truenas.MockClient
	shareDeleteAttempted bool
}

func (m *cloneDependencyMock) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	if err := m.MockClient.SnapshotClone(ctx, snapshotID, newDatasetName); err != nil {
		return err
	}
	snap, err := m.SnapshotGet(ctx, snapshotID)
	if err != nil {
		return err
	}
	snap.Properties["clones"] = map[string]interface{}{"value": newDatasetName}
	return nil
}

func (m *cloneDependencyMock) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	snapshots, err := m.SnapshotList(ctx, name)
	if err != nil {
		return err
	}
	for _, snap := range snapshots {
		if len(snap.GetClones()) > 0 {
			return &truenas.APIError{Code: -1, Message: "snapshot has dependent clones"}
		}
	}
	if err := m.MockClient.DatasetDelete(ctx, name, recursive, force); err != nil {
		return err
	}
	for _, snap := range m.Snapshots {
		clones := snap.GetClones()
		remaining := clones[:0]
		for _, clone := range clones {
			if clone != name {
				remaining = append(remaining, clone)
			}
		}
		snap.Properties["clones"] = map[string]interface{}{"value": strings.Join(remaining, ",")}
	}
	return nil
}

func (m *cloneDependencyMock) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	snap, err := m.SnapshotGet(ctx, snapshotID)
	if !defer_ && err == nil && len(snap.GetClones()) > 0 {
		return &truenas.ErrSnapshotHasClones{SnapshotID: snapshotID, Clones: snap.GetClones()}
	}
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

func (m *cloneDependencyMock) NFSShareDelete(ctx context.Context, id int) error {
	m.shareDeleteAttempted = true
	return m.MockClient.NFSShareDelete(ctx, id)
}

type recursiveCloneDependencyMock struct {
	*truenas.MockClient
}

type snapshotListErrorMock struct {
	*cloneDependencyMock
}

type fallbackSnapshotListErrorMock struct {
	*truenas.MockClient
	snapshotListCalls  int
	datasetDeleteCalls int
}

// DatasetDelete simulates the race the fallback defends against: the non-
// recursive delete fails with an ENOTEMPTY dependency error (a snapshot that
// appeared after the up-front guard's clean check).
func (m *fallbackSnapshotListErrorMock) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	m.datasetDeleteCalls++
	if !recursive {
		return &truenas.APIError{
			Code:    -32602,
			Message: "Invalid params",
			Data:    map[string]interface{}{"reason": "[ENOTEMPTY] zfs.resource.destroy: has snapshots. Set recursive=True to remove them."},
		}
	}
	return m.MockClient.DatasetDelete(ctx, name, recursive, force)
}

type snapshotRenameErrorMock struct {
	*truenas.MockClient
	deleteDefers []bool
}

func (m *snapshotRenameErrorMock) SnapshotRename(context.Context, string, string) error {
	return &truenas.APIError{Code: -32601, Message: "Method not found"}
}

func (m *snapshotRenameErrorMock) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	m.deleteDefers = append(m.deleteDefers, defer_)
	return m.MockClient.SnapshotDelete(ctx, snapshotID, defer_, recursive)
}

func (m *snapshotListErrorMock) SnapshotList(context.Context, string) ([]*truenas.Snapshot, error) {
	return nil, fmt.Errorf("injected snapshot query failure")
}

func (m *fallbackSnapshotListErrorMock) SnapshotList(ctx context.Context, dataset string) ([]*truenas.Snapshot, error) {
	m.snapshotListCalls++
	if m.snapshotListCalls == 1 {
		// Up-front guard sees no snapshots (the race: one appears afterward).
		return []*truenas.Snapshot{}, nil
	}
	// Fallback re-check cannot verify snapshot state.
	return nil, fmt.Errorf("injected fallback snapshot query failure")
}

type blockingSnapshotCreateMock struct {
	*truenas.MockClient
	entered chan struct{}
	release chan struct{}
}

func (m *blockingSnapshotCreateMock) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*truenas.Snapshot, error) {
	close(m.entered)
	select {
	case <-m.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return m.MockClient.SnapshotCreate(ctx, dataset, name, userProperties)
}

func (m *recursiveCloneDependencyMock) DatasetDelete(context.Context, string, bool, bool) error {
	return fmt.Errorf("snapshot has dependent clones")
}

func (m *busyDatasetDeleteMock) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	if !recursive {
		return &truenas.APIError{Code: -1, Message: "dataset is busy"}
	}
	return m.MockClient.DatasetDelete(ctx, name, recursive, force)
}

func newControllerCallCountingMock() *controllerCallCountingMock {
	return &controllerCallCountingMock{MockClient: truenas.NewMockClient()}
}

func (m *controllerCallCountingMock) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	m.datasetGetCalls++
	return m.MockClient.DatasetGet(ctx, name)
}

func (m *controllerCallCountingMock) SnapshotList(ctx context.Context, dataset string) ([]*truenas.Snapshot, error) {
	m.snapshotListCalls++
	return m.MockClient.SnapshotList(ctx, dataset)
}

func (m *controllerCallCountingMock) DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error) {
	m.dependentCloneQueries++
	return m.MockClient.DatasetHasDependentClones(ctx, datasetName)
}

func TestCreateVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
			DriverName: "org.scale.csi.nfs",
			NFS: NFSConfig{
				ShareHost: "1.2.3.4",
			},
		},
		truenasClient: mockClient,
	}

	// Test Case 1: Success
	req := &csi.CreateVolumeRequest{
		Name:               "vol-01",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024 * 1024 * 1024,
		},
	}
	resp, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "vol-01", resp.Volume.VolumeId)
	assert.Equal(t, int64(1024*1024*1024), resp.Volume.CapacityBytes)

	// Verify dataset created
	ds, err := mockClient.DatasetGet(context.Background(), "pool/parent/vol-01")
	assert.NoError(t, err)
	assert.Equal(t, "pool/parent/vol-01", ds.ID)

	// Test Case 2: Idempotency (Same request)
	resp2, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, resp.Volume.VolumeId, resp2.Volume.VolumeId)

	// Test Case 3: Missing Name
	_, err = d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{})
	assert.Error(t, err)
}

func TestCreateVolumeFailureRecordsWarningEvent(t *testing.T) {
	mockClient := truenas.NewMockClient()
	mockClient.InjectError = errors.New("simulated TrueNAS failure")
	fakeRecorder := record.NewFakeRecorder(1)
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: mockClient,
		eventRecorder: &EventRecorder{
			recorder: fakeRecorder,
			enabled:  true,
		},
	}

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "event-failure-volume",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		Parameters: map[string]string{
			pvcNamespaceKey: "storage",
			pvcNameKey:      "claim-one",
		},
	})
	require.Error(t, err)

	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonVolumeCreateFailed)
		assert.Contains(t, event, "CreateVolume failed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for CreateVolume failure event")
	}
}

func TestCreateVolumeExistingReturnsContentSource(t *testing.T) {
	for _, tc := range []struct {
		name       string
		storedType string
		storedID   string
		request    *csi.VolumeContentSource
		wantType   string
		wantID     string
		wantCode   codes.Code
	}{
		{
			name:       "stored source and different requested source are incompatible",
			storedType: "snapshot",
			storedID:   "stored-snapshot",
			request: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "request-snapshot"},
			}},
			wantCode: codes.AlreadyExists,
		},
		{
			name:       "identical source retry succeeds",
			storedType: "snapshot",
			storedID:   "stored-snapshot",
			request: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "stored-snapshot"},
			}},
			wantType: "snapshot",
			wantID:   "stored-snapshot",
		},
		{
			name:       "malformed durable source is not inferred",
			storedType: "unexpected",
			storedID:   "stored-source",
			request: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "request-volume"},
			}},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "stored none and requested source are incompatible",
			request: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "request-volume"},
			}},
			wantCode: codes.AlreadyExists,
		},
		{
			name:       "stored source and request none are incompatible",
			storedType: "volume",
			storedID:   "stored-volume",
			wantCode:   codes.AlreadyExists,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := truenas.NewMockClient()
			d := &Driver{
				config: &Config{
					ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
					DriverName: "org.scale.csi.nfs",
					NFS:        NFSConfig{ShareHost: "1.2.3.4"},
				},
				truenasClient: mockClient,
			}
			ctx := context.Background()
			_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
				Name: "pool/parent/existing-source", Type: "FILESYSTEM", Refquota: 1024 * 1024 * 1024,
			})
			require.NoError(t, err)
			require.NoError(t, mockClient.DatasetSetUserProperty(ctx, "pool/parent/existing-source", PropDriverInstanceID, d.driverInstanceID()))
			require.NoError(t, mockClient.DatasetSetUserProperty(ctx, "pool/parent/existing-source", PropNFSShareID, "1"))
			if tc.storedType != "" {
				require.NoError(t, mockClient.DatasetSetUserProperty(ctx, "pool/parent/existing-source", PropVolumeContentSourceType, tc.storedType))
				require.NoError(t, mockClient.DatasetSetUserProperty(ctx, "pool/parent/existing-source", PropVolumeContentSourceID, tc.storedID))
			}

			resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
				Name:                "existing-source",
				VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
				VolumeContentSource: tc.request,
			})
			if tc.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tc.wantCode, status.Code(err))
				assert.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp.GetVolume().GetContentSource())
			if tc.wantType == "snapshot" {
				assert.Equal(t, tc.wantID, resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())
			} else {
				assert.Equal(t, tc.wantID, resp.GetVolume().GetContentSource().GetVolume().GetVolumeId())
			}
		})
	}
}

func TestCreateVolumeRejectsNFSRawBlockBeforeMutation(t *testing.T) {
	client := &datasetCreateCaptureMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
			NFS: NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}

	resp, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:       "nfs-block-must-fail-early",
		Parameters: map[string]string{"protocol": "nfs"},
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		}},
	})

	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Nil(t, resp)
	assert.Empty(t, client.params, "NFS raw-block rejection must precede dataset creation")
	_, lookupErr := client.DatasetGet(context.Background(), "pool/parent/nfs-block-must-fail-early")
	require.Error(t, lookupErr)
}

func TestDeleteVolumeMissingDatasetFailsClosedOnProtocolCleanupError(t *testing.T) {
	client := &missingDatasetCleanupFailureMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}

	response, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "missing-volume"})
	require.Error(t, err)
	assert.Nil(t, response)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "orphaned protocol cleanup failed")
	assert.Contains(t, err.Error(), "backend query unavailable")
}

func TestCreateDatasetAppliesConfiguredProperties(t *testing.T) {
	client := &datasetCreateCaptureMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{DriverName: "org.scale.csi.test", ZFS: ZFSConfig{
			DatasetParentName:        "pool/parent",
			DatasetEnableQuotas:      true,
			DatasetEnableReservation: true,
			DatasetProperties: map[string]string{
				"compression":       "zstd",
				"sync":              "always",
				"atime":             "off",
				"recordsize":        "128k",
				"logbias":           "throughput",
				"primarycache":      "metadata",
				"copies":            "2",
				"dedup":             "verify",
				"readonly":          "off",
				"org.truenas:owner": "storage-team",
				"unknown":           "ignored",
			},
		}},
		truenasClient: client,
	}

	_, err := d.createDataset(context.Background(), "pool/parent/volume", 2*testGiB, ShareTypeNFS)
	require.NoError(t, err)
	require.Len(t, client.params, 1)
	params := client.params[0]
	assert.Equal(t, "ZSTD", params.Compression)
	assert.Equal(t, "ALWAYS", params.Sync)
	assert.Equal(t, "OFF", params.Atime)
	assert.Equal(t, "128K", params.Recordsize)
	assert.Equal(t, "THROUGHPUT", params.Logbias)
	assert.Equal(t, "METADATA", params.Primarycache)
	assert.Equal(t, 2, params.Copies)
	assert.Equal(t, "VERIFY", params.Deduplication)
	assert.Equal(t, "OFF", params.Readonly)
	assert.Equal(t, 2*testGiB, params.Refquota)
	assert.Equal(t, 2*testGiB, params.Refreservation)
	assert.Empty(t, params.UserProperties,
		"TrueNAS 26.0 silently drops inline dataset-create user properties")
	dataset, getErr := client.DatasetGet(context.Background(), "pool/parent/volume")
	require.NoError(t, getErr)
	assert.True(t, datasetHasLocalUserProperty(dataset, "org.truenas:owner", "storage-team"))
	assert.True(t, datasetHasLocalUserProperty(dataset, PropDriverInstanceID, "org.scale.csi.test@pool/parent"))
}

func TestCreateDatasetZvolSkipsFilesystemOnlyProperties(t *testing.T) {
	client := &datasetCreateCaptureMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{ZFS: ZFSConfig{
			DatasetProperties: map[string]string{
				"compression":  "zstd",
				"recordsize":   "128k",
				"atime":        "off",
				"readonly":     "on",
				"volblocksize": "64k",
			},
		}},
		truenasClient: client,
	}

	_, err := d.createDataset(context.Background(), "pool/parent/zvol", 2*testGiB, ShareTypeISCSI)
	require.NoError(t, err)
	require.Len(t, client.params, 1)
	params := client.params[0]
	assert.Equal(t, "VOLUME", params.Type)
	assert.Equal(t, "ZSTD", params.Compression)
	assert.Equal(t, "ON", params.Readonly)
	assert.Equal(t, "64K", params.Volblocksize)
	assert.Empty(t, params.Recordsize)
	assert.Empty(t, params.Atime)
}

func TestCreateDatasetZvolReservation(t *testing.T) {
	for _, test := range []struct {
		name               string
		enabled            bool
		wantSparse         bool
		wantRefreservation int64
	}{
		{name: "thin by default", wantSparse: true},
		{name: "thick opt in", enabled: true, wantRefreservation: 4 * testGiB},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &datasetCreateCaptureMock{MockClient: truenas.NewMockClient()}
			d := &Driver{
				config: &Config{ZFS: ZFSConfig{
					ZvolBlocksize:         "16K",
					ZvolEnableReservation: test.enabled,
					DatasetProperties: map[string]string{
						"volblocksize": "64K",
						"sparse":       "false",
						"refquota":     "1",
					},
				}},
				truenasClient: client,
			}

			_, err := d.createDataset(context.Background(), "pool/parent/zvol", 4*testGiB, ShareTypeISCSI)
			require.NoError(t, err)
			require.Len(t, client.params, 1)
			params := client.params[0]
			assert.Equal(t, "VOLUME", params.Type)
			assert.Equal(t, 4*testGiB, params.Volsize)
			assert.Equal(t, "16K", params.Volblocksize, "explicit zvol block size must win")
			assert.Equal(t, test.wantSparse, params.Sparse)
			assert.Equal(t, test.wantRefreservation, params.Refreservation)
			assert.Zero(t, params.Refquota, "filesystem-only refquota must not leak into zvol creation")
		})
	}
}

func TestCreateVolumeLocksOnSanitizedVolumeID(t *testing.T) {
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: truenas.NewMockClient(),
	}
	name := "volume/name with spaces"
	lockKey := "volume:" + d.sanitizeVolumeID(name)
	require.True(t, d.acquireOperationLock(lockKey))
	defer d.releaseOperationLock(lockKey)

	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               name,
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
}

func TestDeleteVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create volume
	volName := "vol-delete"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volName,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.DeleteVolumeRequest{
		VolumeId: volName,
	}
	_, err = d.DeleteVolume(context.Background(), req)
	assert.NoError(t, err)

	// Verify dataset deleted
	_, err = mockClient.DatasetGet(context.Background(), "pool/parent/"+volName)
	assert.Error(t, err)

	// Test Case 2: Idempotency (Already deleted)
	_, err = d.DeleteVolume(context.Background(), req)
	assert.NoError(t, err)

	// Test Case 3: Missing ID
	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})
	assert.Error(t, err)
}

func TestGetVolumeContextUsesProvidedDatasetWithoutQuery(t *testing.T) {
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config:        &Config{NFS: NFSConfig{ShareHost: "198.51.100.10"}},
		truenasClient: mockClient,
	}
	ds := &truenas.Dataset{
		Name:           "pool/parent/vol-context",
		Type:           "FILESYSTEM",
		Mountpoint:     "/mnt/pool/parent/vol-context",
		UserProperties: map[string]truenas.UserProperty{},
	}

	volumeContext, err := d.getVolumeContext(context.Background(), ds, ds.Name, ShareTypeNFS)
	assert.NoError(t, err)
	assert.Equal(t, "/mnt/pool/parent/vol-context", volumeContext["share"])
	assert.Zero(t, mockClient.datasetGetCalls)
}

func TestDeleteVolumeHappyPathListsSnapshotsOnce(t *testing.T) {
	// The up-front dependent-snapshot check is deliberate: the share is deleted
	// before the dataset, so discovering snapshots only after DatasetDelete
	// fails would leave an existing volume with no share. One SnapshotList per
	// delete is the accepted cost.
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-fast-delete", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-fast-delete"})
	assert.NoError(t, err)
	assert.Equal(t, 1, mockClient.datasetGetCalls)
	assert.Equal(t, 1, mockClient.snapshotListCalls)
	assert.Equal(t, 1, mockClient.dependentCloneQueries)
}

func TestDeleteVolumeWithManagedSnapshotFailsBeforeShareDeletion(t *testing.T) {
	mockClient := newControllerCallCountingMock()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: mockClient,
	}
	ctx := context.Background()
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-snap-guard", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	share, err := mockClient.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: "/mnt/pool/parent/vol-snap-guard"})
	assert.NoError(t, err)
	_, err = mockClient.SnapshotCreate(ctx, "pool/parent/vol-snap-guard", "snap-1", map[string]string{
		PropManagedResource: "true",
	})
	assert.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vol-snap-guard"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	// The guard must fire before share deletion: the share still exists.
	remaining, listErr := mockClient.NFSShareGet(ctx, share.ID)
	assert.NoError(t, listErr)
	assert.NotNil(t, remaining)
}

func TestDeleteVolumeWithDatasetOriginCloneFailsBeforeShareDeletion(t *testing.T) {
	client := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source-origin", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source-origin"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: source.Mountpoint})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, fmt.Sprint(share.ID)))
	clone, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/external/clone", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	client.Datasets[clone.Name].Origin = truenas.DatasetProperty{
		Value:  "pool/parent/source-origin@external-snapshot",
		Parsed: "pool/parent/source-origin@external-snapshot",
	}

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source-origin"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remaining, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remaining)
}

func TestDeleteVolumeCloneSourceFailsBeforeShareDeletionThenSucceeds(t *testing.T) {
	client := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source"
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: source.Mountpoint})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, source.Name, PropNFSShareID, fmt.Sprint(share.ID)))

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "clone",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
		}},
	})
	assert.NoError(t, err)
	clone, err := client.DatasetGet(ctx, "pool/parent/clone")
	assert.NoError(t, err)
	originID := clone.UserProperties[PropVolumeOriginSnapshot].Value
	assert.NotEmpty(t, originID)
	origin, err := client.SnapshotGet(ctx, originID)
	assert.NoError(t, err)
	assert.Equal(t, "true", origin.UserProperties[PropInternalResource].Value)
	assert.NotEmpty(t, origin.GetClones())

	client.shareDeleteAttempted = false
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remainingShare, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remainingShare)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "clone"})
	assert.NoError(t, err)
	_, err = client.SnapshotGet(ctx, originID)
	assert.Error(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.NoError(t, err)
	_, err = client.DatasetGet(ctx, source.Name)
	assert.Error(t, err)
}

func TestDeleteVolumeRemovesOrphanedInternalCloneSourceSnapshot(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "source",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.NoError(t, err)
	orphan, err := client.SnapshotCreate(ctx, "pool/parent/source", "clone-source-deleted-clone", map[string]string{
		PropInternalResource: "true",
	})
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	require.NoError(t, err)
	_, err = client.SnapshotGet(ctx, orphan.ID)
	require.Error(t, err)
	assert.True(t, truenas.IsNotFoundError(err))
	_, err = client.DatasetGet(ctx, "pool/parent/source")
	require.Error(t, err)
	assert.True(t, truenas.IsNotFoundError(err))
}

func TestDeleteVolumeForeignSnapshotStillBlocks(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	_, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "source",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.NoError(t, err)
	foreign, err := client.SnapshotCreate(ctx, "pool/parent/source", "periodic-backup", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = client.SnapshotGet(ctx, foreign.ID)
	require.NoError(t, err)
	_, err = client.DatasetGet(ctx, "pool/parent/source")
	require.NoError(t, err)
}

func TestVolumeClonePropertyWriteFailureRollsBackCloneAndOriginSnapshot(t *testing.T) {
	ctx := context.Background()
	client := &clonePropertyFailureMock{MockClient: truenas.NewMockClient(), err: fmt.Errorf("injected property write failure")}
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NFS: NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client.MockClient)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 8 * testGiB})
	require.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source"

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "clone",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to set content source properties")
	_, err = client.DatasetGet(ctx, "pool/parent/clone")
	require.Error(t, err)
	_, err = client.SnapshotGet(ctx, "pool/parent/source@clone-source-clone")
	require.Error(t, err)
	require.NotEmpty(t, client.DatasetDeleteCalls)
	assert.True(t, client.DatasetDeleteCalls[len(client.DatasetDeleteCalls)-1].Force)
}

// A crash between the volume-source clone and its ownership stamp leaves an
// unstamped remnant plus the durable in-flight marker written before the clone
// started. The marker (instance + source + recorded origin) combined with the
// remnant's matching ZFS origin PROVES provenance, so the retry resumes it into
// a healthy, correctly-stamped volume instead of wedging on terminal
// AlreadyExists. Provenance is proven, never inferred from the request.
func TestCreateVolumeResumesProvenInFlightVolumeCloneRemnant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source"
	snap, err := client.SnapshotCreate(ctx, source.Name, "clone-source-clone", map[string]string{PropInternalResource: "true"})
	require.NoError(t, err)
	// Simulate the crash exactly as production leaves it: marker written (same
	// code path as handleVolumeContentSource), clone created, stamp never ran.
	contentSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
	}}
	marker, err := d.newInflightMarker("pool/parent/clone", contentSource, ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = snap.ID
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, client.SnapshotClone(ctx, snap.ID, "pool/parent/clone"))

	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "clone",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: contentSource,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "source", resp.GetVolume().GetContentSource().GetVolume().GetVolumeId())

	clone, err := client.DatasetGet(ctx, "pool/parent/clone")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(clone, PropManagedResource, "true"))
	assert.True(t, datasetHasLocalUserProperty(clone, PropDriverInstanceID, d.driverInstanceID()))
	assert.True(t, datasetHasLocalUserProperty(clone, PropCSIVolumeName, "clone"))
	assert.Equal(t, "volume", datasetUserProperty(clone, PropVolumeContentSourceType))
	assert.Equal(t, "source", datasetUserProperty(clone, PropVolumeContentSourceID))
	assert.Equal(t, snap.ID, datasetUserProperty(clone, PropVolumeOriginSnapshot))
	assert.Equal(t, snap.ID, datasetOriginSnapshotID(clone))
	assert.NotContains(t, clone.UserProperties, PropRecoveryNonce, "the CAS nonce must be removed after a won recovery")
	// The consumed marker is gone from the parent.
	consumed, err := d.readInflightMarker(ctx, "clone")
	require.NoError(t, err)
	assert.Nil(t, consumed)
}

// A request that reaches a marker-proven remnant with an INCOMPATIBLE shape
// (different source, or a capacity limit the remnant exceeds) must keep the
// terminal AlreadyExists outcome — recovery may never resume it into an OK.
func TestCreateVolumeRemnantRecoveryRejectsIncompatibleRequests(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	snapA, err := client.SnapshotCreate(ctx, source.Name, "snap-a", map[string]string{
		PropManagedResource: "true", PropCSISnapshotName: "snap-a", PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, source.Name, "snap-b", map[string]string{
		PropManagedResource: "true", PropCSISnapshotName: "snap-b", PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	sourceFor := func(id string) *csi.VolumeContentSource {
		return &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: id},
		}}
	}
	marker, err := d.newInflightMarker("pool/parent/restored", sourceFor("snap-a"), ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = snapA.ID
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, client.SnapshotClone(ctx, snapA.ID, "pool/parent/restored"))

	// Different source than the marker records: no recovery, terminal outcome.
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "restored",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: sourceFor("snap-b"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// Matching source but a limit below the remnant's capacity: incompatible.
	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "restored",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: testGiB, LimitBytes: 2 * testGiB},
		VolumeContentSource: sourceFor("snap-a"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// The remnant must be untouched: still unstamped, still origin-linked.
	remnant, err := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	assert.False(t, datasetHasLocalUserProperty(remnant, PropDriverInstanceID, d.driverInstanceID()))
	assert.Equal(t, snapA.ID, datasetOriginSnapshotID(remnant))
}

// Two overlapping controller processes (per-process operation locks cannot
// exclude each other) can both attempt remnant recovery. The per-attempt nonce
// in the ownership stamp is the CAS: the process whose write is overwritten
// before its verifying re-read loses and returns Aborted without touching the
// winner's stamp.
func TestCreateVolumeRemnantRecoveryNonceLoserAborts(t *testing.T) {
	ctx := context.Background()
	client := &recoveryRaceLoserMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	snap, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropManagedResource: "true", PropCSISnapshotName: "snap-1", PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	contentSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	marker, err := d.newInflightMarker("pool/parent/restored", contentSource, ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = snap.ID
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, client.SnapshotClone(ctx, snap.ID, "pool/parent/restored"))

	_, err = d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "restored",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: contentSource,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err), "the nonce loser must return retryable Aborted")
	assert.True(t, client.raced, "the race injection must have fired")
	// The winner's stamp (same instance, different nonce payload) is intact.
	remnant, err := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(remnant, PropDriverInstanceID, d.driverInstanceID()))
}

// recoveryRaceLoserMock simulates a concurrent controller process winning the
// recovery stamp race: the first stamp carrying a recovery nonce is immediately
// overwritten by a competing same-instance payload with a different nonce, so
// the caller's verifying re-read must fail on its nonce.
type recoveryRaceLoserMock struct {
	*truenas.MockClient
	raced bool
}

func (m *recoveryRaceLoserMock) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	if _, hasNonce := properties[PropRecoveryNonce]; hasNonce && !m.raced {
		m.raced = true
		if err := m.MockClient.DatasetSetUserProperties(ctx, name, properties); err != nil {
			return err
		}
		competing := make(map[string]string, len(properties))
		for key, value := range properties {
			competing[key] = value
		}
		competing[PropRecoveryNonce] = "competing-controller-nonce"
		return m.MockClient.DatasetSetUserProperties(ctx, name, competing)
	}
	return m.MockClient.DatasetSetUserProperties(ctx, name, properties)
}

// A remnant carrying a LOCAL ownership stamp from another driver instance is
// never adopted — even when our own in-flight marker matches it. The stamped
// owner always wins; recovery stays terminal AlreadyExists (the clone-race
// guarantee).
func TestCreateVolumeRejectsForeignOwnedCloneRemnant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	source.Mountpoint = "/mnt/pool/parent/source"
	snap, err := client.SnapshotCreate(ctx, source.Name, "clone-source-clone", map[string]string{PropInternalResource: "true"})
	require.NoError(t, err)
	contentSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
		Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
	}}
	marker, err := d.newInflightMarker("pool/parent/clone", contentSource, ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = snap.ID
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, client.SnapshotClone(ctx, snap.ID, "pool/parent/clone"))
	// A different driver instance's LOCAL ownership stamp must veto adoption even
	// though our marker and the origin both match.
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/clone", PropDriverInstanceID, "org.other.csi@pool/parent"))

	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "clone",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: contentSource,
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
	assert.Nil(t, resp)
	clone, err := client.DatasetGet(ctx, "pool/parent/clone")
	require.NoError(t, err)
	assert.False(t, datasetHasLocalUserProperty(clone, PropDriverInstanceID, d.driverInstanceID()),
		"a foreign-owned remnant must never be re-stamped by this instance")
}

// Absence of local properties is NOT recovery license. Without a matching
// in-flight marker, every unowned-looking dataset stays terminal AlreadyExists:
// a dataset carrying only a local publication record, a dataset whose backend
// share exists but whose share-ID property write failed, and even an unstamped
// clone whose origin happens to match the requested source (pre-marker-era or
// foreign remnant).
func TestCreateVolumeRefusesUnmarkedDatasetsWithContentSource(t *testing.T) {
	newDriver := func(client truenas.ClientInterface) *Driver {
		return &Driver{
			config: &Config{
				ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
				DriverName: "org.scale.csi.nfs",
				NFS:        NFSConfig{ShareHost: "192.0.2.10"},
			},
			truenasClient: client,
		}
	}
	snapshotSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	request := func() *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:                "target",
			VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:       &csi.CapacityRange{RequiredBytes: testGiB},
			VolumeContentSource: snapshotSource,
		}
	}

	t.Run("publication-record-only local properties do not permit recovery", func(t *testing.T) {
		ctx := context.Background()
		client := truenas.NewMockClient()
		d := newDriver(client)
		mustCreateParentDataset(t, client)
		source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB})
		require.NoError(t, err)
		_, err = client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{PropManagedResource: "true", PropCSISnapshotName: "snap-1"})
		require.NoError(t, err)
		foreign, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/target", Type: "FILESYSTEM", Refquota: testGiB})
		require.NoError(t, err)
		require.NoError(t, client.DatasetSetUserProperty(ctx, foreign.Name, publicationPropertyKey("some-node"), `{"v":1,"node":"some-node","state":"published","access_mode":1,"updated_at":"2026-01-01T00:00:00Z"}`))

		_, err = d.CreateVolume(ctx, request())
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
		_, err = client.DatasetGet(ctx, "pool/parent/target")
		require.NoError(t, err, "the unmarked dataset must not be destroyed")
	})

	t.Run("share without stored share-ID property does not permit recovery", func(t *testing.T) {
		ctx := context.Background()
		client := truenas.NewMockClient()
		d := newDriver(client)
		mustCreateParentDataset(t, client)
		source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB})
		require.NoError(t, err)
		_, err = client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{PropManagedResource: "true", PropCSISnapshotName: "snap-1"})
		require.NoError(t, err)
		// The ensureShareExists-before-property-store window: a live backend share
		// exists for the dataset but no local property records it.
		orphanShareDS, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/target", Type: "FILESYSTEM", Refquota: testGiB})
		require.NoError(t, err)
		share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: orphanShareDS.Mountpoint, Enabled: true})
		require.NoError(t, err)

		_, err = d.CreateVolume(ctx, request())
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
		_, err = client.DatasetGet(ctx, "pool/parent/target")
		require.NoError(t, err, "the dataset backing a live share must not be destroyed")
		remaining, err := client.NFSShareGet(ctx, share.ID)
		require.NoError(t, err)
		require.NotNil(t, remaining, "the backend share must remain untouched")
	})

	t.Run("matching origin without a marker does not permit recovery", func(t *testing.T) {
		ctx := context.Background()
		client := truenas.NewMockClient()
		d := newDriver(client)
		mustCreateParentDataset(t, client)
		source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB})
		require.NoError(t, err)
		snap, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{PropManagedResource: "true", PropCSISnapshotName: "snap-1"})
		require.NoError(t, err)
		require.NoError(t, client.SnapshotClone(ctx, snap.ID, "pool/parent/target"))

		_, err = d.CreateVolume(ctx, request())
		require.Error(t, err)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))
		remnant, err := client.DatasetGet(ctx, "pool/parent/target")
		require.NoError(t, err)
		assert.False(t, datasetHasLocalUserProperty(remnant, PropDriverInstanceID, d.driverInstanceID()),
			"an unmarked origin-matching clone must not be adopted")
	})
}

// A crash between the snapshot clone and its ownership stamp (the narrow ZFS
// clone window) leaves an unstamped remnant whose ZFS origin is the requested
// source snapshot. The retry resumes it into a healthy, correctly-stamped volume
// and round-trips through delete with no leaked dataset.
func TestCreateVolumeResumesProvenInFlightSnapshotCloneRemnant(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, source.Name, map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropCSIVolumeName:    "source",
	}))
	snapshot, err := client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "snap-1",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	// Simulate the crash: the in-flight marker was written, the clone exists
	// (origin = source snapshot) and inherits the source's identity markers as
	// NON-local (origin-name source), but the ownership stamp never ran.
	contentSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	marker, err := d.newInflightMarker("pool/parent/restored", contentSource, ShareTypeNFS)
	require.NoError(t, err)
	marker.Origin = snapshot.ID
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, "pool/parent/restored"))
	remnant, err := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	require.False(t, datasetHasLocalUserProperty(remnant, PropDriverInstanceID, d.driverInstanceID()),
		"precondition: the crash remnant carries no local ownership")

	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:                "restored",
		VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: contentSource,
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-1", resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())

	restored, err := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(restored, PropManagedResource, "true"))
	assert.True(t, datasetHasLocalUserProperty(restored, PropDriverInstanceID, d.driverInstanceID()))
	assert.True(t, datasetHasLocalUserProperty(restored, PropCSIVolumeName, "restored"))
	assert.Equal(t, "snapshot", datasetUserProperty(restored, PropVolumeContentSourceType))
	assert.Equal(t, "snap-1", datasetUserProperty(restored, PropVolumeContentSourceID))
	// No leaked dataset: only the parent, the source, and the resumed volume.
	assert.Len(t, client.Datasets, 3)

	// The resumed volume deletes cleanly, leaving the independent CSI snapshot.
	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "restored"})
	require.NoError(t, err)
	_, err = client.DatasetGet(ctx, "pool/parent/restored")
	assert.True(t, truenas.IsNotFoundError(err))
	remainingSnap, err := client.SnapshotFindByName(ctx, "pool/parent", "snap-1")
	require.NoError(t, err)
	require.NotNil(t, remainingSnap)
}

// The REAL detached-copy crash shape: replication created the target dataset
// AND left the transferred snapshot on it (snapshot cleanup runs only after the
// job completes), and the in-flight marker written before the copy is still on
// the parent. The marker proves ownership, so the retry destroys the remnant
// recursively (transferred snapshot included), returns Aborted, and the next
// retry recreates the copy cleanly and converges. Without a marker the same
// dataset shape is refused (covered separately).
func TestCreateVolumeDestroysInterruptedDetachedCopyRemnantThenConverges(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", DetachedVolumesFromSnapshots: true, DatasetEnableQuotas: true},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "192.0.2.10"},
		},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client)
	source, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: 4 * testGiB})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperties(ctx, source.Name, map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropCSIVolumeName:    "source",
	}))
	_, err = client.SnapshotCreate(ctx, source.Name, "snap-1", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "snap-1",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	contentSource := &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
		Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "snap-1"},
	}}
	// Simulate the interrupted local send/receive exactly: marker written before
	// the copy, an independent target dataset (no ZFS origin) whose received user
	// properties are NOT local, and the transferred replication snapshot still on
	// the target. The ownership stamp never ran.
	marker, err := d.newInflightMarker("pool/parent/restored", contentSource, ShareTypeNFS)
	require.NoError(t, err)
	marker.Mode = inflightModeCopy
	require.NoError(t, d.writeInflightMarker(ctx, marker))
	client.Datasets["pool/parent/restored"] = &truenas.Dataset{
		ID: "pool/parent/restored", Name: "pool/parent/restored", Type: "FILESYSTEM",
		Mountpoint: "/mnt/pool/parent/restored",
		Refquota:   truenas.DatasetProperty{Parsed: float64(4 * testGiB)},
		Creation:   truenas.DatasetProperty{Parsed: float64(time.Now().Unix())},
		UserProperties: map[string]truenas.UserProperty{
			PropManagedResource: {Value: "true", Source: "received"},
			PropCSIVolumeName:   {Value: "source", Source: "received"},
		},
	}
	transferred, err := client.SnapshotCreate(ctx, "pool/parent/restored", "snap-1", nil)
	require.NoError(t, err)

	restoreRequest := func() *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name:                "restored",
			Parameters:          map[string]string{"protocol": "nfs"},
			VolumeCapabilities:  []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
			CapacityRange:       &csi.CapacityRange{RequiredBytes: 2 * testGiB},
			VolumeContentSource: contentSource,
		}
	}

	_, err = d.CreateVolume(ctx, restoreRequest())
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	_, getErr := client.DatasetGet(ctx, "pool/parent/restored")
	assert.True(t, truenas.IsNotFoundError(getErr), "the marker-proven detached-copy remnant must be destroyed")
	_, snapErr := client.SnapshotGet(ctx, transferred.ID)
	require.Error(t, snapErr, "the transferred replication snapshot must be destroyed with the remnant")

	resp, err := d.CreateVolume(ctx, restoreRequest())
	require.NoError(t, err)
	assert.Equal(t, "snap-1", resp.GetVolume().GetContentSource().GetSnapshot().GetSnapshotId())
	restored, err := client.DatasetGet(ctx, "pool/parent/restored")
	require.NoError(t, err)
	assert.True(t, datasetHasLocalUserProperty(restored, PropDriverInstanceID, d.driverInstanceID()))
	assert.True(t, datasetHasLocalUserProperty(restored, PropCSIVolumeName, "restored"))
	assert.Equal(t, "snap-1", datasetUserProperty(restored, PropVolumeContentSourceID))
	// The consumed marker is gone after convergence.
	remaining, err := d.readInflightMarker(ctx, "restored")
	require.NoError(t, err)
	assert.Nil(t, remaining)
}

func TestCreateVolumeCloneReportsInheritedActualCapacity(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:   ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			ISCSI: ISCSIConfig{TargetPortal: "192.0.2.10:3260"},
		},
		truenasClient: client,
		serviceReloadDebouncer: NewServiceReloadDebouncer(0, func(context.Context, string) error {
			return nil
		}),
	}
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	mustCreateParentDataset(t, client)
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "VOLUME", Volsize: 8 * testGiB})
	require.NoError(t, err)

	resp, err := d.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "clone",
		Parameters:         map[string]string{"protocol": "iscsi"},
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * testGiB},
		VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
			Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: "source"},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, 8*testGiB, resp.GetVolume().GetCapacityBytes())
}

func TestCreateVolumeShareFailureCleanupUsesForceDelete(t *testing.T) {
	client := &nfsShareCreateFailureMock{MockClient: truenas.NewMockClient(), err: fmt.Errorf("injected share failure")}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, NFS: NFSConfig{ShareHost: "192.0.2.10"}},
		truenasClient: client,
	}
	_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "failed-volume",
		VolumeCapabilities: []*csi.VolumeCapability{testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)},
		CapacityRange:      &csi.CapacityRange{RequiredBytes: testGiB},
	})
	require.Error(t, err)
	require.NotEmpty(t, client.DatasetDeleteCalls)
	cleanup := client.DatasetDeleteCalls[len(client.DatasetDeleteCalls)-1]
	assert.Equal(t, "pool/parent/failed-volume", cleanup.Name)
	assert.False(t, cleanup.Recursive)
	assert.True(t, cleanup.Force)
}

func TestDeleteVolumeReturnsOriginSnapshotDeleteFailure(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()
	originID := "pool/parent/source@clone-source-clone"
	client := &originSnapshotDeleteFailureMock{
		MockClient:       base,
		originSnapshotID: originID,
		err:              fmt.Errorf("temporary snapshot delete failure"),
	}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, NFS: NFSConfig{ShareHost: "192.0.2.10"}},
		truenasClient: client,
	}
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/clone", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/clone", PropVolumeOriginSnapshot, originID))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "clone"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "temporary snapshot delete failure")
}

func TestDeleteVolumeRetriesOriginSnapshotDelete(t *testing.T) {
	ctx := context.Background()
	base := truenas.NewMockClient()
	originID := "pool/parent/source@clone-source-clone"
	client := &transientOriginSnapshotDeleteFailureMock{
		MockClient:       base,
		originSnapshotID: originID,
		failuresLeft:     2,
	}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, NFS: NFSConfig{ShareHost: "192.0.2.10"}},
		truenasClient: client,
	}

	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "clone-source-clone", map[string]string{PropInternalResource: "true"})
	require.NoError(t, err)
	_, err = client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/clone", Type: "FILESYSTEM"})
	require.NoError(t, err)
	require.NoError(t, client.DatasetSetUserProperty(ctx, "pool/parent/clone", PropVolumeOriginSnapshot, originID))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "clone"})
	require.NoError(t, err)
	assert.Equal(t, 3, client.deleteCalls)
	_, err = client.SnapshotGet(ctx, originID)
	require.Error(t, err)
	assert.True(t, truenas.IsNotFoundError(err), "origin snapshot must be removed after the transient failures clear")
}

func TestDeleteCloneOriginSnapshotHonorsContext(t *testing.T) {
	client := &transientOriginSnapshotDeleteFailureMock{
		MockClient:       truenas.NewMockClient(),
		originSnapshotID: "pool/parent/source@clone-source-clone",
		failuresLeft:     1,
	}
	d := &Driver{truenasClient: client}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.deleteCloneOriginSnapshot(ctx, client.originSnapshotID)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, client.deleteCalls, "snapshot delete must not run after context cancellation")
}

func TestDeleteVolumeRecursiveCloneDependencyIsFailedPrecondition(t *testing.T) {
	client := &recursiveCloneDependencyMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, DriverName: "org.scale.csi.nfs"},
		truenasClient: client,
	}
	ctx := context.Background()
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	assert.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "external", nil)
	assert.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestDeleteVolumeSnapshotQueryFailureDoesNotDeleteShare(t *testing.T) {
	base := &cloneDependencyMock{MockClient: truenas.NewMockClient()}
	client := &snapshotListErrorMock{cloneDependencyMock: base}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
			NFS:        NFSConfig{ShareHost: "1.2.3.4"},
		},
		truenasClient: client,
	}
	ctx := context.Background()
	ds, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	assert.NoError(t, err)
	share, err := client.NFSShareCreate(ctx, &truenas.NFSShareCreateParams{Path: "/mnt/pool/parent/source"})
	assert.NoError(t, err)
	assert.NoError(t, client.DatasetSetUserProperty(ctx, ds.Name, PropNFSShareID, fmt.Sprint(share.ID)))

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.False(t, client.shareDeleteAttempted)
	remainingShare, shareErr := client.NFSShareGet(ctx, share.ID)
	assert.NoError(t, shareErr)
	assert.NotNil(t, remainingShare)
}

func TestDeleteVolumeBusyDatasetChecksManagedSnapshots(t *testing.T) {
	countingMock := newControllerCallCountingMock()
	mockClient := &busyDatasetDeleteMock{controllerCallCountingMock: countingMock}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	datasetName := "pool/parent/vol-with-snapshot"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: datasetName, Type: "FILESYSTEM",
	})
	assert.NoError(t, err)
	snapshot, err := mockClient.SnapshotCreate(context.Background(), datasetName, "managed", nil)
	assert.NoError(t, err)
	snapshot.UserProperties[PropManagedResource] = truenas.UserProperty{Value: "true"}

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "vol-with-snapshot"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Equal(t, 1, countingMock.snapshotListCalls)
}

func TestDeleteVolumeForeignSnapshotsAreFailSafeByDefault(t *testing.T) {
	ctx := context.Background()
	datasetName := "pool/parent/vol-with-foreign-snapshot"
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = mockClient.SnapshotCreate(ctx, datasetName, "periodic-2026-07-18", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vol-with-foreign-snapshot"})

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "zfs.destroyForeignSnapshotsOnDelete=true")
	_, getErr := mockClient.DatasetGet(ctx, datasetName)
	assert.NoError(t, getErr, "the dataset must remain when recursive deletion is disabled")
	// The refusal must happen in the up-front guard, BEFORE the share is
	// deleted — so no dataset-delete is even attempted. This prevents stranding
	// a shareless volume (the invariant guarded since the clone-source rounds).
	assert.Empty(t, mockClient.DatasetDeleteCalls,
		"foreign snapshots must be refused before any share/dataset deletion is attempted")
}

func TestDeleteVolumeForeignSnapshotsCanBeDestroyedWithExplicitOptIn(t *testing.T) {
	ctx := context.Background()
	datasetName := "pool/parent/vol-with-opt-in"
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName:               "pool/parent",
				DestroyForeignSnapshotsOnDelete: true,
			},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	snapshot, err := mockClient.SnapshotCreate(ctx, datasetName, "periodic-2026-07-18", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vol-with-opt-in"})

	require.NoError(t, err)
	_, getErr := mockClient.DatasetGet(ctx, datasetName)
	assert.Error(t, getErr)
	_, getSnapshotErr := mockClient.SnapshotGet(ctx, snapshot.ID)
	assert.Error(t, getSnapshotErr)
	require.Len(t, mockClient.DatasetDeleteCalls, 2)
	assert.False(t, mockClient.DatasetDeleteCalls[0].Recursive)
	assert.True(t, mockClient.DatasetDeleteCalls[1].Recursive,
		"the opt-in path must retry with recursive deletion")
}

func TestDeleteVolumeFallbackSnapshotListErrorRefusesRecursiveDeleteByDefault(t *testing.T) {
	ctx := context.Background()
	datasetName := "pool/parent/vol-with-query-error"
	mockClient := &fallbackSnapshotListErrorMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = mockClient.SnapshotCreate(ctx, datasetName, "periodic-2026-07-18", nil)
	require.NoError(t, err)

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "vol-with-query-error"})

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "cannot verify snapshots")
	_, getErr := mockClient.DatasetGet(ctx, datasetName)
	assert.NoError(t, getErr)
	// Only the non-recursive first attempt ran; the fallback refused rather
	// than escalating to a recursive delete when it could not verify state.
	assert.Equal(t, 1, mockClient.datasetDeleteCalls,
		"an unverifiable snapshot state must never trigger recursive deletion by default")
}

func TestCreateSnapshot(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create source volume
	volName := "vol-snap"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/" + volName,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: volName,
		Name:           "snap-01",
	}
	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "snap-01", resp.Snapshot.SnapshotId)
	assert.Equal(t, volName, resp.Snapshot.SourceVolumeId)

	// The same name and source are idempotent.
	idempotentResp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, resp.Snapshot.SnapshotId, idempotentResp.Snapshot.SnapshotId)

	// Verify snapshot created
	snapID := "pool/parent/" + volName + "@snap-01"
	snap, err := mockClient.SnapshotGet(context.Background(), snapID)
	assert.NoError(t, err)
	assert.Equal(t, snapID, snap.ID)

	// The same global CSI snapshot name cannot refer to another source volume.
	_, err = mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/vol-snap-other",
	})
	assert.NoError(t, err)
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		SourceVolumeId: "vol-snap-other",
		Name:           "snap-01",
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// Test Case 2: Missing Source
	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{Name: "snap-02"})
	assert.Error(t, err)
}

func TestControllerPublishVolumeValidation(t *testing.T) {
	mockClient := truenas.NewMockClient()
	d := &Driver{
		nodeID:  "known-node",
		runNode: true,
		config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "pool/parent"},
		},
		truenasClient: mockClient,
	}
	capability := testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)

	_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "missing-volume",
		NodeId:           "known-node",
		VolumeCapability: capability,
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// In combined/all mode (runNode=true) the process knows its own node ID, so a
	// request for a different node is NotFound (best-effort check; also required by
	// the csi-sanity "publish should fail when the node does not exist" conformance case).
	_, err = d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "missing-volume",
		NodeId:           "unknown-node",
		VolumeCapability: capability,
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Contains(t, err.Error(), "node not found")

	_, err = mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/existing-volume", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	_, err = d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         "existing-volume",
		NodeId:           "known-node",
		VolumeCapability: capability,
	})
	assert.NoError(t, err, "publish must succeed for an existing volume on the known node")

	_, err = d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "missing-volume",
		NodeId:   "known-node",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestControllerInboundIDsRejectPathTraversalWithoutMutation(t *testing.T) {
	capability := testVolumeCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	tests := []struct {
		name string
		call func(*Driver, string) error
	}{
		{
			name: "DeleteVolume",
			call: func(d *Driver, id string) error {
				_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: id})
				return err
			},
		},
		{
			name: "ControllerPublishVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
					VolumeId: id, NodeId: "node-1", VolumeCapability: capability,
				})
				return err
			},
		},
		{
			name: "ValidateVolumeCapabilities",
			call: func(d *Driver, id string) error {
				_, err := d.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: id, VolumeCapabilities: []*csi.VolumeCapability{capability},
				})
				return err
			},
		},
		{
			name: "CreateSnapshotSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
					SourceVolumeId: id, Name: "safe-snapshot",
				})
				return err
			},
		},
		{
			name: "ControllerExpandVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId: id, CapacityRange: &csi.CapacityRange{RequiredBytes: testGiB},
				})
				return err
			},
		},
		{
			name: "ControllerGetVolume",
			call: func(d *Driver, id string) error {
				_, err := d.ControllerGetVolume(context.Background(), &csi.ControllerGetVolumeRequest{VolumeId: id})
				return err
			},
		},
		{
			name: "CreateVolumeSnapshotSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
					Name: "safe-target", VolumeCapabilities: []*csi.VolumeCapability{capability},
					VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: id},
					}},
				})
				return err
			},
		},
		{
			name: "CreateVolumeVolumeSource",
			call: func(d *Driver, id string) error {
				_, err := d.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
					Name: "safe-target", VolumeCapabilities: []*csi.VolumeCapability{capability},
					VolumeContentSource: &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: id},
					}},
				})
				return err
			},
		},
	}

	for _, tc := range tests {
		for _, id := range []string{"../x", "..", "a/b"} {
			t.Run(tc.name+"/"+strings.ReplaceAll(id, "/", "_"), func(t *testing.T) {
				client := newControllerCallCountingMock()
				d := &Driver{
					config: &Config{
						DriverName: "org.scale.csi.nfs",
						ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
						NFS:        NFSConfig{ShareHost: "192.0.2.10"},
					},
					truenasClient: client,
				}

				err := tc.call(d, id)
				require.Error(t, err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				assert.Zero(t, client.datasetGetCalls, "invalid IDs must be rejected before TrueNAS access")
				assert.Empty(t, client.Datasets)
				assert.Empty(t, client.Snapshots)
				assert.Empty(t, client.NFSShares)
				assert.Empty(t, client.ISCSITargets)
				assert.Empty(t, client.NVMeSubsystems)
				assert.Empty(t, client.DatasetDeleteCalls)
			})
		}
	}
}

func TestCreateSnapshotSerializesWithDeleteVolume(t *testing.T) {
	client := &blockingSnapshotCreateMock{
		MockClient: truenas.NewMockClient(),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}, DriverName: "org.scale.csi.nfs"},
		truenasClient: client,
	}
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	createErr := make(chan error, 1)
	go func() {
		_, createSnapshotErr := d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Name: "snapshot", SourceVolumeId: "source",
		})
		createErr <- createSnapshotErr
	}()
	<-client.entered

	_, err = d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "source"})
	assert.Equal(t, codes.Aborted, status.Code(err))
	close(client.release)
	assert.NoError(t, <-createErr)
}

func TestDeleteSnapshot(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	// Pre-create snapshot
	snapName := "snap-delete"
	volName := "vol-snap-del"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{Name: "pool/parent/" + volName})
	assert.NoError(t, err)
	_, err = mockClient.SnapshotCreate(context.Background(), "pool/parent/"+volName, snapName, map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           snapName,
		PropCSISnapshotSourceVolumeID: volName,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.DeleteSnapshotRequest{
		SnapshotId: snapName,
	}
	_, err = d.DeleteSnapshot(context.Background(), req)
	assert.NoError(t, err)

	_, err = mockClient.SnapshotGet(context.Background(), "pool/parent/"+volName+"@"+snapName)
	assert.Error(t, err)
}

func TestDeleteSnapshotAcquiresSourceVolumeLockBeforeSnapshotLock(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}, truenasClient: client}
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "snapshot", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "snapshot",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	require.True(t, d.acquireOperationLock("volume:source"))
	require.True(t, d.acquireOperationLock("snapshot:snapshot"))
	t.Cleanup(func() {
		d.releaseOperationLock("snapshot:snapshot")
		d.releaseOperationLock("volume:source")
	})

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "snapshot"})

	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.Contains(t, err.Error(), "source volume")
}

func TestDeleteSnapshotRefusesForeignShortNameCollision(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}, truenasClient: client}
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	foreign, err := client.SnapshotCreate(ctx, "pool/parent/source", "daily-backup", nil)
	require.NoError(t, err)

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "daily-backup"})
	require.NoError(t, err)
	remaining, err := client.SnapshotGet(ctx, foreign.ID)
	require.NoError(t, err)
	assert.Equal(t, foreign.ID, remaining.ID)
}

func TestListSnapshotsUsesSourceDatasetCapacityForRestoreSize(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}}, truenasClient: client}
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "VOLUME", Volsize: 12 * testGiB})
	require.NoError(t, err)
	_, err = client.SnapshotCreate(ctx, "pool/parent/source", "restore-point", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "restore-point",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)

	resp, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SnapshotId: "restore-point"})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, 12*testGiB, resp.GetEntries()[0].GetSnapshot().GetSizeBytes())
}

func TestDeleteSnapshotWithRestoredVolumeDefersAndReleasesSnapshot(t *testing.T) {
	ctx := context.Background()
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent"},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}

	mustCreateParentDataset(t, mockClient)
	_, err := mockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM", Refquota: testGiB,
	})
	require.NoError(t, err)
	created, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "restore-point", SourceVolumeId: "source",
	})
	require.NoError(t, err)
	originalSnapshotID := "pool/parent/source@" + created.GetSnapshot().GetSnapshotId()
	require.NoError(t, mockClient.SnapshotClone(ctx, originalSnapshotID, "pool/parent/restored"))

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "restore-point"})
	require.NoError(t, err)
	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "restore-point"})
	require.NoError(t, err, "retry after tombstone rename must be idempotent")

	listed, err := d.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	require.NoError(t, err)
	assert.Empty(t, listed.GetEntries(), "the tombstone must not be exposed as a CSI snapshot")

	allSnapshots, err := mockClient.SnapshotList(ctx, "pool/parent/source")
	require.NoError(t, err)
	var tombstoneID string
	for _, snap := range allSnapshots {
		if strings.HasPrefix(snap.Name, "restore-point-csi-deleted-") {
			tombstoneID = snap.ID
			assert.NotContains(t, snap.UserProperties, PropManagedResource)
			assert.NotContains(t, snap.UserProperties, PropCSISnapshotName)
			assert.NotContains(t, snap.UserProperties, PropCSISnapshotSourceVolumeID)
		}
	}
	require.NotEmpty(t, tombstoneID)

	recreated, err := d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "restore-point", SourceVolumeId: "source",
	})
	require.NoError(t, err)
	assert.Equal(t, "restore-point", recreated.GetSnapshot().GetSnapshotId())

	_, err = d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "restored"})
	require.NoError(t, err)
	_, err = mockClient.SnapshotGet(ctx, tombstoneID)
	assert.Error(t, err, "ZFS should reclaim a deferred snapshot after its final clone is deleted")
}

func TestDeleteSnapshotRenameFailureReturnsError(t *testing.T) {
	ctx := context.Background()
	client := &snapshotRenameErrorMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}
	mustCreateParentDataset(t, client.MockClient)
	_, err := client.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: "pool/parent/source", Type: "FILESYSTEM"})
	require.NoError(t, err)
	snapshot, err := client.SnapshotCreate(ctx, "pool/parent/source", "restore-point", map[string]string{
		PropManagedResource:           "true",
		PropCSISnapshotName:           "restore-point",
		PropCSISnapshotSourceVolumeID: "source",
	})
	require.NoError(t, err)
	require.NoError(t, client.SnapshotClone(ctx, snapshot.ID, "pool/parent/restored"))

	_, err = d.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: "restore-point"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "failed to tombstone snapshot")
	assert.Equal(t, []bool{false}, client.deleteDefers)
	remaining, err := client.SnapshotGet(ctx, snapshot.ID)
	require.NoError(t, err)
	assert.Equal(t, "restore-point", remaining.Name)
	assert.NotEmpty(t, remaining.UserProperties)
	// The rename never happened, so its pre-written ledger entry must be retired.
	parent, err := client.DatasetGet(ctx, "pool/parent")
	require.NoError(t, err)
	assert.Empty(t, tombstoneLedgerFromDataset(parent),
		"a failed tombstone rename must not leave a ledger entry behind")
}

func TestSnapshotTombstoneNameCapsFullZFSSnapshotName(t *testing.T) {
	const maxZFSSnapshotNameLength = 255
	dataset := "pool/" + strings.Repeat("deeply-nested/", 15)
	name := strings.Repeat("snapshot-name-", 20)
	nonce := int64(9223372036854775807)

	tombstone := snapshotTombstoneName(dataset, name, nonce)
	fullName := dataset + "@" + tombstone

	assert.LessOrEqual(t, len(fullName), maxZFSSnapshotNameLength)
	assert.True(t, strings.HasSuffix(tombstone, "-csi-deleted-9223372036854775807"), "the unique tombstone nonce must survive truncation")
	assert.Less(t, len(tombstone), len(name)+len("-csi-deleted-9223372036854775807"))
}

func TestSnapshotListEntryClassification(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   *truenas.Snapshot
		filter     string
		wantSource string
		wantEntry  bool
		wantBlocks bool
	}{
		{
			name: "26.0 inherited managed property is not CSI identity",
			snapshot: &truenas.Snapshot{
				ID: "pool/parent/manual-source@manual", Name: "manual", Dataset: "pool/parent/manual-source",
				ResourceQuery: true,
				UserProperties: map[string]truenas.UserProperty{
					PropManagedResource: {Value: "true", Source: "inherited from pool/parent/manual-source"},
				},
			},
		},
		{
			name: "legacy managed snapshot derives source from dataset",
			snapshot: &truenas.Snapshot{
				ID: "pool/parent/legacy-source@legacy", Name: "legacy", Dataset: "pool/parent/legacy-source",
				UserProperties: map[string]truenas.UserProperty{
					PropManagedResource: {Value: "true"},
				},
			},
			wantEntry:  true,
			wantSource: "legacy-source",
			filter:     "legacy-source",
			wantBlocks: true,
		},
		{
			name: "tombstone is excluded even when identity properties survive",
			snapshot: &truenas.Snapshot{
				ID: "pool/parent/source@restore-point-csi-deleted-42", Name: "restore-point-csi-deleted-42", Dataset: "pool/parent/source",
				ResourceQuery: true,
				UserProperties: map[string]truenas.UserProperty{
					PropManagedResource:           {Value: "true"},
					PropCSISnapshotName:           {Value: "restore-point"},
					PropCSISnapshotSourceVolumeID: {Value: "source"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := snapshotListEntry(tc.snapshot, tc.filter)
			assert.Equal(t, tc.wantBlocks, snapshotBlocksVolumeDeletion(tc.snapshot))
			if !tc.wantEntry {
				assert.Nil(t, entry)
				return
			}
			require.NotNil(t, entry)
			assert.Equal(t, tc.wantSource, entry.GetSnapshot().GetSourceVolumeId())
		})
	}
}

func TestCreateSnapshotSurvivesTrueNAS26UpdateNoOp(t *testing.T) {
	client := truenas.NewMockClient()
	client.SimulateUpdateNoOp = true
	_, err := client.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: "pool/parent/source", Type: "FILESYSTEM",
	})
	require.NoError(t, err)
	d := &Driver{
		config:        &Config{ZFS: ZFSConfig{DatasetParentName: "pool/parent"}},
		truenasClient: client,
	}

	_, err = d.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "inline-properties", SourceVolumeId: "source",
	})
	require.NoError(t, err)
	snap, err := client.SnapshotGet(context.Background(), "pool/parent/source@inline-properties")
	require.NoError(t, err)
	assert.Equal(t, "true", snap.UserProperties[PropManagedResource].Value)
	assert.Equal(t, "inline-properties", snap.UserProperties[PropCSISnapshotName].Value)
	assert.Equal(t, "source", snap.UserProperties[PropCSISnapshotSourceVolumeID].Value)
}

func TestSanitizeVolumeIDRuneSafe(t *testing.T) {
	input := strings.Repeat("a", 127) + "é"
	got := sanitizeVolumeID(input)
	assert.True(t, utf8.ValidString(got))
	assert.LessOrEqual(t, len(got), 128)
	assert.Equal(t, strings.Repeat("a", 127), got)
	assert.Equal(t, "vUpper", sanitizeVolumeID("Upper"))
	assert.Equal(t, "v🔥-name", sanitizeVolumeID("🔥/name"))
}

// TestCreateSnapshot_RestoreSize verifies that CreateSnapshot returns the correct
// SizeBytes value for restore operations. This is critical for CSI volume restore
// where the PVC must have a size >= snapshot.restoreSize.
//
// The fix ensures we use the source volume's volsize (for zvols) instead of the
// snapshot's "used" bytes, which can be tiny for near-empty volumes.
func TestCreateSnapshot_RestoreSize(t *testing.T) {
	// Test cases for the restoreSize fix
	tests := []struct {
		name         string
		datasetType  string
		volsize      int64 // Source volume size (for zvols)
		refquota     int64 // Source refquota (for filesystems)
		snapshotUsed int64 // Snapshot "used" bytes to set on mock
		expectedSize int64 // Expected SizeBytes in response
		description  string
	}{
		{
			name:         "zvol_uses_volsize_not_used_bytes",
			datasetType:  "VOLUME",
			volsize:      10 * 1024 * 1024 * 1024, // 10 GiB
			snapshotUsed: 102400,                  // 100 KiB (near-empty volume)
			expectedSize: 10 * 1024 * 1024 * 1024, // Should use volsize, NOT used bytes
			description:  "For zvols with valid volsize, SizeBytes should be volsize (not snapshot used bytes)",
		},
		{
			name:         "zvol_empty_volume",
			datasetType:  "VOLUME",
			volsize:      5 * 1024 * 1024 * 1024, // 5 GiB
			snapshotUsed: 0,                      // Completely empty
			expectedSize: 5 * 1024 * 1024 * 1024, // Should use volsize
			description:  "Empty zvol should still use volsize for restoreSize",
		},
		{
			name:         "zvol_1gib_volume",
			datasetType:  "VOLUME",
			volsize:      1 * 1024 * 1024 * 1024, // 1 GiB
			snapshotUsed: 512,                    // Tiny used bytes
			expectedSize: 1 * 1024 * 1024 * 1024, // Should use volsize
			description:  "1 GiB zvol should return 1 GiB restoreSize even with tiny used bytes",
		},
		{
			name:         "zvol_zero_volsize_fallback",
			datasetType:  "VOLUME",
			volsize:      0,       // Invalid/missing volsize
			snapshotUsed: 2097152, // 2 MiB (not used in this test - mock returns 0)
			expectedSize: 0,       // Falls back to snapshot used bytes (mock returns 0)
			description:  "When volsize is 0 and refquota is 0, fall back to snapshot used bytes",
		},
		{
			name:         "filesystem_uses_refquota",
			datasetType:  "FILESYSTEM",
			refquota:     20 * 1024 * 1024 * 1024, // 20 GiB refquota
			snapshotUsed: 51200,                   // 50 KiB used
			expectedSize: 20 * 1024 * 1024 * 1024, // For filesystems (NFS), use refquota
			description:  "Filesystems use refquota as volume size",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mock client
			mockClient := truenas.NewMockClient()
			d := &Driver{
				config: &Config{
					ZFS: ZFSConfig{
						DatasetParentName: "pool/parent",
					},
				},
				truenasClient: mockClient,
			}

			volName := "vol-" + tc.name
			datasetName := "pool/parent/" + volName

			// Create source volume with appropriate type and size
			_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
				Name:     datasetName,
				Type:     tc.datasetType,
				Volsize:  tc.volsize,
				Refquota: tc.refquota,
			})
			assert.NoError(t, err, "Failed to create source dataset")

			// Create the snapshot request
			snapName := "snap-" + tc.name
			req := &csi.CreateSnapshotRequest{
				SourceVolumeId: volName,
				Name:           snapName,
			}

			// Execute CreateSnapshot
			resp, err := d.CreateSnapshot(context.Background(), req)
			assert.NoError(t, err, "CreateSnapshot should succeed: %s", tc.description)
			assert.NotNil(t, resp, "Response should not be nil")
			assert.NotNil(t, resp.Snapshot, "Snapshot should not be nil")

			// Now set the snapshot used bytes on the mock (for verification purposes)
			// Note: This happens after snapshot creation, but we use it to verify
			// that the code path is using volsize, not GetSnapshotSize()
			snapID := datasetName + "@" + snapName
			mockClient.SetSnapshotUsedBytes(snapID, tc.snapshotUsed)

			// Verify the snapshot size matches expected (volsize for zvols, refquota for filesystems)
			assert.Equal(t, tc.expectedSize, resp.Snapshot.SizeBytes,
				"%s: expected %d, got %d", tc.description, tc.expectedSize, resp.Snapshot.SizeBytes)

			// Verify other snapshot properties
			assert.Equal(t, snapName, resp.Snapshot.SnapshotId)
			assert.Equal(t, volName, resp.Snapshot.SourceVolumeId)
			assert.True(t, resp.Snapshot.ReadyToUse)
		})
	}
}

// TestCreateSnapshot_RestoreSizeFallbackWithUsedBytes tests the fallback path
// when volsize is not available but snapshot has used bytes set.
func TestCreateSnapshot_RestoreSizeFallbackWithUsedBytes(t *testing.T) {
	// This test uses a custom mock that sets used bytes on snapshot creation
	mockClient := &snapshotWithUsedBytesMock{
		MockClient: truenas.NewMockClient(),
		usedBytes:  5 * 1024 * 1024, // 5 MiB
	}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	volName := "vol-fallback"
	datasetName := "pool/parent/" + volName

	// Create source volume as FILESYSTEM (no volsize)
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: datasetName,
		Type: "FILESYSTEM",
	})
	assert.NoError(t, err)

	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: volName,
		Name:           "snap-fallback",
	}

	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.NoError(t, err, "CreateSnapshot should succeed")
	assert.NotNil(t, resp)

	// Should fall back to snapshot used bytes (5 MiB)
	assert.Equal(t, int64(5*1024*1024), resp.Snapshot.SizeBytes,
		"Should use snapshot used bytes when volsize is not available")
}

// snapshotWithUsedBytesMock sets the used property on snapshots at creation time
type snapshotWithUsedBytesMock struct {
	*truenas.MockClient
	usedBytes int64
}

func (m *snapshotWithUsedBytesMock) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*truenas.Snapshot, error) {
	snap, err := m.MockClient.SnapshotCreate(ctx, dataset, name, userProperties)
	if err != nil {
		return nil, err
	}
	// Set the used bytes property
	snap.Properties["used"] = map[string]interface{}{
		"parsed": float64(m.usedBytes),
	}
	return snap, nil
}

// TestCreateSnapshot_SourceDatasetGetFailure verifies that a failing source
// DatasetGet fails the RPC up front — before any snapshot is created — so a
// retry can succeed cleanly with no orphaned snapshot. (The former post-create
// size re-query and its used-bytes fallback were removed: the up-front fetch
// is reused for restoreSize.)
func TestCreateSnapshot_SourceDatasetGetFailure(t *testing.T) {
	mockClient := &datasetGetFailMock{
		MockClient:     truenas.NewMockClient(),
		failDatasetGet: true,
	}

	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
		},
		truenasClient: mockClient,
	}

	req := &csi.CreateSnapshotRequest{
		SourceVolumeId: "vol-fail-test",
		Name:           "snap-fail",
	}

	resp, err := d.CreateSnapshot(context.Background(), req)
	assert.Error(t, err, "CreateSnapshot must fail when the source dataset cannot be read")
	assert.Nil(t, resp)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.False(t, mockClient.snapCreated, "no snapshot may be created when the source lookup fails")
}

// datasetGetFailMock wraps MockClient to fail DatasetGet and record whether a
// snapshot was ever created.
type datasetGetFailMock struct {
	*truenas.MockClient
	failDatasetGet bool
	snapCreated    bool
}

func (m *datasetGetFailMock) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*truenas.Snapshot, error) {
	snap, err := m.MockClient.SnapshotCreate(ctx, dataset, name, userProperties)
	if err == nil {
		m.snapCreated = true
	}
	return snap, err
}

func (m *datasetGetFailMock) DatasetGet(ctx context.Context, name string) (*truenas.Dataset, error) {
	if m.failDatasetGet {
		return nil, &truenas.APIError{Code: -1, Message: "dataset not found (simulated)"}
	}
	return m.MockClient.DatasetGet(ctx, name)
}

func TestControllerExpandVolume(t *testing.T) {
	// Setup
	mockClient := truenas.NewMockClient()
	d := &Driver{
		config: &Config{
			ZFS: ZFSConfig{
				DatasetParentName: "pool/parent",
			},
			DriverName: "org.scale.csi.nfs",
		},
		truenasClient: mockClient,
	}

	// Pre-create volume
	volName := "vol-expand"
	_, err := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name:    "pool/parent/" + volName,
		Volsize: 1024,
	})
	assert.NoError(t, err)

	// Test Case 1: Success
	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: volName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2048,
		},
	}
	resp, err := d.ControllerExpandVolume(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, int64(2048), resp.CapacityBytes)
	assert.False(t, resp.NodeExpansionRequired) // NFS doesn't require node expansion usually, but code says depends on resource type

	// Verify expansion
	ds, _ := mockClient.DatasetGet(context.Background(), "pool/parent/"+volName)
	// Note: Mock implementation updates Volsize for Expand, but driver might update Refquota for NFS
	// Let's check what the driver does.
	// Driver checks config.GetZFSResourceType().
	// Default config implies filesystem for NFS.
	// Driver updates Refquota for filesystem.
	// Mock DatasetUpdate handles Volsize, let's ensure it handles Refquota too if we want to test that path.
	// For now, basic check is fine.
	_ = ds
}

// TestIsDatasetDependencyOrBusyError_InspectsAPIErrorData pins that the
// has-snapshots (ENOTEMPTY) reason — which TrueNAS puts in the API error's
// Data field, not the top-level "Invalid params" message — is recognized as a
// dependency error so DeleteVolume runs its snapshot handling instead of
// returning Internal.
func TestIsDatasetDependencyOrBusyError_InspectsAPIErrorData(t *testing.T) {
	enotempty := fmt.Errorf("failed to delete dataset: %w", &truenas.APIError{
		Code:    -32602,
		Message: "Invalid params",
		Data:    map[string]interface{}{"reason": "[ENOTEMPTY] zfs.resource.destroy: 'tank/vol' has snapshots. Set recursive=True to remove them."},
	})
	assert.True(t, isDatasetDependencyOrBusyError(enotempty),
		"has-snapshots reason in APIError.Data must classify as a dependency error")

	plain := fmt.Errorf("failed to delete dataset: %w", &truenas.APIError{Code: -32602, Message: "Invalid params"})
	assert.False(t, isDatasetDependencyOrBusyError(plain),
		"a bare Invalid params with no dependency reason must not classify as a dependency error")

	assert.False(t, isDatasetDependencyOrBusyError(nil))
}
