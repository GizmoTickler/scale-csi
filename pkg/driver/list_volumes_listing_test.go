package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// listingShapeAppliance is an in-process appliance with production-shaped
// rows (zfs.resource.query: {value, raw} properties and flat user
// properties; pool.dataset.query: {value, rawvalue, parsed, source}) that
// records every name pool.dataset.query is asked for.
type listingShapeAppliance struct {
	mu        sync.Mutex
	poolReads [][]string
}

// listingShapeRows returns the resource listing and the pool rows for n
// volumes: every third one a filesystem with a refquota, volume legacy (if
// >= 0) carrying a ZFS publication record (flat in the listing, LOCAL in the
// pool row).
func listingShapeRows(t *testing.T, n, legacy int, mutate func(i int, resource, pool map[string]interface{})) ([]byte, map[string][]byte) {
	t.Helper()
	rows := make([]map[string]interface{}, n)
	pool := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		resource := scaleDriverResourceRow(i)
		poolRow := scaleDriverPoolRow(i)
		name := poolRow["name"].(string)
		if i%3 == 2 {
			resource["type"], poolRow["type"] = "FILESYSTEM", "FILESYSTEM"
			resource["properties"].(map[string]interface{})["refquota"] = map[string]interface{}{
				"value": int64(5368709120), "raw": "5368709120", "source": map[string]interface{}{"type": "LOCAL", "value": nil}}
			poolRow["refquota"] = map[string]interface{}{"value": "5G", "rawvalue": "5368709120", "parsed": int64(5368709120), "source": "LOCAL"}
		}
		if i == legacy {
			record, err := json.Marshal(publicationRecord{Version: publicationRecordVersion, Node: "k8s-9", EncodedID: "sc1.id-k8s-9",
				NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:k8s-9", State: publicationStatePublished, AccessMode: 1, UpdatedAt: "2026-01-01T00:00:00Z"})
			require.NoError(t, err)
			key := publicationPropertyKey("k8s-9")
			resource["user_properties"].(map[string]interface{})[key] = string(record)
			poolRow["user_properties"].(map[string]interface{})[key] = map[string]interface{}{
				"value": string(record), "rawvalue": string(record), "parsed": string(record), "source": "LOCAL"}
		}
		if mutate != nil {
			mutate(i, resource, poolRow)
		}
		rows[i] = resource
		encoded, err := json.Marshal(poolRow)
		require.NoError(t, err)
		pool[name] = encoded
	}
	listing, err := json.Marshal(rows)
	require.NoError(t, err)
	return listing, pool
}

func newListingShapeDriver(t *testing.T, n, legacy int) (*Driver, *listingShapeAppliance) {
	t.Helper()
	return newListingShapeDriverWith(t, n, legacy, nil)
}

// newListingShapeDriverWith is newListingShapeDriver with each row passed
// through mutate before it is served.
func newListingShapeDriverWith(t *testing.T, n, legacy int, mutate func(i int, resource, pool map[string]interface{})) (*Driver, *listingShapeAppliance) {
	t.Helper()
	listing, pool := listingShapeRows(t, n, legacy, mutate)
	appliance := &listingShapeAppliance{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     int64             `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.Unmarshal(msg, &req)
			var out bytes.Buffer
			fmt.Fprintf(&out, `{"jsonrpc":"2.0","id":%d,"result":`, req.ID)
			switch req.Method {
			case "zfs.resource.query":
				if bytes.Contains(req.Params[0], []byte(`"get_user_properties"`)) {
					out.Write(listing)
				} else {
					out.WriteString("[]")
				}
			case "pool.dataset.query":
				var filters [][]interface{}
				_ = json.Unmarshal(req.Params[0], &filters)
				var names []string
				out.WriteString("[")
				if len(filters) == 1 && len(filters[0]) == 3 {
					if requested, ok := filters[0][2].([]interface{}); ok {
						for _, name := range requested {
							if row, found := pool[name.(string)]; found {
								if len(names) > 0 {
									out.WriteString(",")
								}
								out.Write(row)
								names = append(names, name.(string))
							}
						}
					}
				}
				out.WriteString("]")
				appliance.mu.Lock()
				appliance.poolReads = append(appliance.poolReads, names)
				appliance.mu.Unlock()
			default:
				out.WriteString("true")
			}
			out.WriteString("}")
			if err := conn.WriteMessage(websocket.TextMessage, out.Bytes()); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	host, portText, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	var port int
	_, _ = fmt.Sscanf(portText, "%d", &port)
	client, err := truenas.NewClient(&truenas.ClientConfig{Host: host, Port: port, Protocol: "http", APIKey: "k",
		Timeout: 30 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	kube := kubernetesPublicationStore{client: newFakeVolumePublicationClient(), namespace: "scale-csi", instance: "listing"}
	ctx := context.Background()
	for i := 0; i < n; i++ {
		node := fmt.Sprintf("k8s-%d", i%3)
		record := publicationRecord{Version: publicationRecordVersion, Node: node, EncodedID: "sc1.id-" + node,
			NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:" + node, State: publicationStatePublished, AccessMode: 1, UpdatedAt: "2026-10-01T00:00:00Z"}
		require.NoError(t, kube.store(ctx, scaleDriverParent+"/"+scaleDriverVolume(i), nil, publicationPropertyKey(node), record))
	}
	cache, err := newPublicationCache(kube)
	require.NoError(t, err)
	cache.start(ctx)
	t.Cleanup(cache.close)
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: scaleDriverParent}}, truenasClient: client}
	d.publicationStore = importingPublicationStore{kube: kube, legacy: zfsPublicationStore{client: client}, cache: cache}
	return d, appliance
}

func walkListVolumes(t *testing.T, d *Driver, pageSize int32) []*csi.ListVolumesResponse_Entry {
	t.Helper()
	var entries []*csi.ListVolumesResponse_Entry
	token := ""
	for {
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: pageSize, StartingToken: token})
		require.NoError(t, err)
		entries = append(entries, resp.Entries...)
		if resp.NextToken == "" {
			return entries
		}
		token = resp.NextToken
	}
}

// The ListVolumes output contract on the real row shapes: each entry's
// capacity (a zvol's volsize, a filesystem's refquota) comes from the
// listing's {value, raw} properties and equals what the pool.dataset.query
// row (parsed) gives; its published nodes come from the VolumePublication
// cache. No page is re-read from TrueNAS.
func TestListVolumesFromTheListingKeepsTheOutputContract(t *testing.T) {
	const n = 30
	d, appliance := newListingShapeDriver(t, n, -1)
	entries := walkListVolumes(t, d, 7)
	require.Len(t, entries, n)
	byID := make(map[string]*csi.ListVolumesResponse_Entry, n)
	for _, entry := range entries {
		byID[entry.GetVolume().GetVolumeId()] = entry
	}
	for i := 0; i < n; i++ {
		entry := byID[scaleDriverVolume(i)]
		require.NotNil(t, entry, "volume %d listed", i)
		want := int64(10737418240)
		if i%3 == 2 {
			want = 5368709120
		}
		assert.Equal(t, want, entry.GetVolume().GetCapacityBytes(), "volume %d capacity", i)
		assert.Equal(t, []string{fmt.Sprintf("sc1.id-k8s-%d", i%3)}, entry.GetStatus().GetPublishedNodeIds(), "volume %d published nodes", i)
	}
	appliance.mu.Lock()
	defer appliance.mu.Unlock()
	assert.Empty(t, appliance.poolReads, "no page is re-read")
}

// A dataset that still carries a ZFS publication record (not yet imported)
// is the one re-read by name, so its LOCAL record is seen; its entry then
// reports both its Kubernetes and its ZFS record, as before.
func TestListVolumesReReadsOnlyDatasetsWithZFSRecords(t *testing.T) {
	const n = 30
	d, appliance := newListingShapeDriver(t, n, 4)
	entries := walkListVolumes(t, d, 100)
	require.Len(t, entries, n)
	for _, entry := range entries {
		if entry.GetVolume().GetVolumeId() != scaleDriverVolume(4) {
			continue
		}
		assert.Equal(t, []string{"sc1.id-k8s-1", "sc1.id-k8s-9"}, entry.GetStatus().GetPublishedNodeIds())
		assert.Equal(t, int64(10737418240), entry.GetVolume().GetCapacityBytes())
	}
	appliance.mu.Lock()
	defer appliance.mu.Unlock()
	assert.Equal(t, [][]string{{scaleDriverParent + "/" + scaleDriverVolume(4)}}, appliance.poolReads)
}

// listedPropertyBytes reads every numeric shape a listing can carry.
func TestListedPropertyBytesShapes(t *testing.T) {
	for name, tc := range map[string]struct {
		property truenas.DatasetProperty
		want     int64
		ok       bool
	}{
		"pool parsed":           {truenas.DatasetProperty{Value: "10G", Rawvalue: "10737418240", Parsed: float64(10737418240)}, 10737418240, true},
		"resource value":        {truenas.DatasetProperty{Value: float64(10737418240)}, 10737418240, true},
		"resource value text":   {truenas.DatasetProperty{Value: "10737418240"}, 10737418240, true},
		"human value, rawvalue": {truenas.DatasetProperty{Value: "10G", Rawvalue: "10737418240"}, 10737418240, true},
		"none":                  {truenas.DatasetProperty{Value: "-"}, 0, false},
	} {
		got, ok := listedPropertyBytes(tc.property)
		assert.Equal(t, tc.ok, ok, name)
		assert.Equal(t, tc.want, got, name)
	}
}

// zfs.resource.query sends {"raw": "<digits>", "value": <number>, "source":
// {...}} (the shape captured live). raw is read when value carries no number.
func TestListedPropertyBytesReadsTheResourceQueryRaw(t *testing.T) {
	production := truenas.DatasetProperty{Value: float64(21474836480), Raw: "21474836480"}
	got, ok := listedPropertyBytes(production)
	assert.True(t, ok)
	assert.Equal(t, int64(21474836480), got)

	rawOnly := truenas.DatasetProperty{Value: nil, Raw: "21474836480"}
	got, ok = listedPropertyBytes(rawOnly)
	assert.True(t, ok, "raw is the fallback when value is null")
	assert.Equal(t, int64(21474836480), got)

	none := truenas.DatasetProperty{Value: nil, Raw: "none"}
	_, ok = listedPropertyBytes(none)
	assert.False(t, ok)
}

// A zvol whose volsize is unreadable has an unknown capacity (0), never the
// pool's free space from available.
func TestListedDatasetCapacityOfAZvolWithoutVolsizeIsUnknown(t *testing.T) {
	available := truenas.DatasetProperty{Value: float64(15051461210048), Raw: "15051461210048"}
	for name, volsize := range map[string]truenas.DatasetProperty{
		"absent":     {},
		"null value": {Value: nil, Raw: "-"},
	} {
		capacity, known := listedDatasetCapacity(&truenas.Dataset{Name: "pool/parent/zvol", Type: "VOLUME", Volsize: volsize, Available: available})
		assert.False(t, known, name)
		assert.Zero(t, capacity, name)
	}
	// A filesystem with no quota and no requested size still reports
	// available, as before.
	capacity, known := listedDatasetCapacity(&truenas.Dataset{Name: "pool/parent/fs", Type: "FILESYSTEM", Available: available})
	assert.True(t, known)
	assert.Equal(t, int64(15051461210048), capacity)
}

// End to end on the live row shape: a zvol whose volsize comes as raw only
// is listed with that size, and one whose volsize is null is listed with
// capacity 0, not the pool's available bytes.
func TestListVolumesZvolCapacityFromTheResourceQueryRow(t *testing.T) {
	d, _ := newListingShapeDriverWith(t, 3, -1, func(i int, resource, _ map[string]interface{}) {
		properties := resource["properties"].(map[string]interface{})
		switch i {
		case 0:
			properties["volsize"] = map[string]interface{}{"raw": "21474836480", "value": nil,
				"source": map[string]interface{}{"type": "LOCAL", "value": nil}}
		case 1:
			properties["volsize"] = nil
		}
	})
	entries := walkListVolumes(t, d, 100)
	require.Len(t, entries, 3)
	capacities := map[string]int64{}
	for _, entry := range entries {
		capacities[entry.GetVolume().GetVolumeId()] = entry.GetVolume().GetCapacityBytes()
	}
	assert.Equal(t, int64(21474836480), capacities[scaleDriverVolume(0)], "volsize read from raw")
	assert.Zero(t, capacities[scaleDriverVolume(1)], "a null volsize is unknown, not the pool's free space")
	_, logged := d.unknownVolsizeLogged.Load(scaleDriverParent + "/" + scaleDriverVolume(1))
	assert.True(t, logged)
}
