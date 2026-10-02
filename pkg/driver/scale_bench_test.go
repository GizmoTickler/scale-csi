package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// ListVolumes benchmarks at 30 and 1,000 volumes: the real websocket client
// against an in-process appliance with production-shaped rows, records in
// Kubernetes behind the informer cache, every volume attached, as the
// external-attacher walks it every minute.

const scaleDriverParent = "flashstor/k8s"

func scaleDriverVolume(i int) string { return fmt.Sprintf("pvc-%08x-1c2d-4e5f-8a9b-%012x", i*7919, i) }

func scaleDriverUserProps(i int) map[string]string {
	name := scaleDriverVolume(i)
	return map[string]string{
		"scale-csi:managed_resource":             "true",
		"scale-csi:csi_volume_name":              name,
		"scale-csi:driver_instance_id":           "org.scale-csi.nvmeof-prod",
		"scale-csi:provision_success":            "true",
		"scale-csi:requested_size_bytes":         "10737418240",
		"scale-csi:truenas_nvmeof_subsystem_id":  fmt.Sprint(100 + i),
		"scale-csi:truenas_nvmeof_namespace_id":  fmt.Sprint(200 + i),
		"scale-csi:truenas_nvmeof_portsubsys_id": fmt.Sprint(300 + i),
		"scale-csi:zfs_performance_class":        "general",
		"org.freenas:description":                "",
	}
}

func scaleDriverPoolRow(i int) map[string]interface{} {
	name := scaleDriverParent + "/" + scaleDriverVolume(i)
	up := map[string]interface{}{}
	for k, v := range scaleDriverUserProps(i) {
		up[k] = map[string]interface{}{"value": v, "rawvalue": v, "parsed": v, "source": "LOCAL"}
	}
	p := func(value interface{}, raw string, parsed interface{}, source string) map[string]interface{} {
		return map[string]interface{}{"value": value, "rawvalue": raw, "parsed": parsed, "source": source}
	}
	return map[string]interface{}{
		"id": name, "name": name, "pool": "flashstor", "type": "VOLUME", "mountpoint": nil,
		"encrypted": false, "encryption_root": nil, "key_loaded": false, "locked": false,
		"key_format": p(nil, "none", nil, "DEFAULT"), "children": []interface{}{},
		"used": p("1.02G", "1092616192", 1092616192, "NONE"), "available": p("1.75T", "1924145348608", 1924145348608, "NONE"),
		"quota": p(nil, "0", nil, "DEFAULT"), "refquota": p(nil, "0", nil, "DEFAULT"),
		"reservation": p(nil, "0", nil, "DEFAULT"), "refreservation": p(nil, "0", nil, "DEFAULT"),
		"volsize": p("10G", "10737418240", 10737418240, "LOCAL"), "volblocksize": p("16K", "16384", 16384, "DEFAULT"),
		"compression": p("LZ4", "lz4", "lz4", "INHERITED"), "sync": p("STANDARD", "standard", "standard", "DEFAULT"),
		"origin":          p("", "", "", "NONE"),
		"creation":        p("Wed Jul 23 14:02 2026", "1784988120", map[string]interface{}{"$date": 1784988120000}, "NONE"),
		"user_properties": up,
	}
}

func scaleDriverResourceRow(i int) map[string]interface{} {
	name := scaleDriverParent + "/" + scaleDriverVolume(i)
	up := map[string]interface{}{}
	for k, v := range scaleDriverUserProps(i) {
		up[k] = v
	}
	r := func(value interface{}, raw string) map[string]interface{} {
		return map[string]interface{}{"value": value, "raw": raw, "source": map[string]interface{}{"type": "NONE", "value": nil}}
	}
	return map[string]interface{}{
		"name": name, "pool": "flashstor", "type": "VOLUME", "createtxg": 4532290 + i,
		"properties": map[string]interface{}{
			"used": r(1092616192, "1092616192"), "available": r(int64(1924145348608), "1924145348608"),
			"quota": r(0, "0"), "refquota": r(0, "0"), "reservation": r(0, "0"), "refreservation": r(0, "0"),
			"volsize": r(int64(10737418240), "10737418240"), "volblocksize": r(16384, "16384"),
			"creation": r(1784988120, "1784988120"), "origin": r(nil, "-"),
		},
		"user_properties": up, "children": []interface{}{},
	}
}

func newScaleDriverServer(tb testing.TB, n int) *httptest.Server {
	tb.Helper()
	rows := make([]map[string]interface{}, n)
	poolRows := make(map[string][]byte, n)
	for i := range rows {
		rows[i] = scaleDriverResourceRow(i)
		row, err := json.Marshal(scaleDriverPoolRow(i))
		if err != nil {
			tb.Fatal(err)
		}
		poolRows[scaleDriverParent+"/"+scaleDriverVolume(i)] = row
	}
	listing, err := json.Marshal(rows)
	if err != nil {
		tb.Fatal(err)
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }, WriteBufferSize: 64 << 10}
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
			wr, err := conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			fmt.Fprintf(wr, `{"jsonrpc":"2.0","id":%d,"result":`, req.ID)
			switch req.Method {
			case "zfs.resource.query":
				if bytes.Contains(req.Params[0], []byte(`"get_user_properties"`)) {
					_, _ = wr.Write(listing)
				} else {
					_, _ = wr.Write([]byte("[]")) // capability probe
				}
			case "pool.dataset.query":
				var filters [][]interface{}
				_ = json.Unmarshal(req.Params[0], &filters)
				_, _ = wr.Write([]byte("["))
				if len(filters) == 1 && len(filters[0]) == 3 {
					if names, ok := filters[0][2].([]interface{}); ok {
						for i, name := range names {
							if i > 0 {
								_, _ = wr.Write([]byte(","))
							}
							_, _ = wr.Write(poolRows[name.(string)])
						}
					}
				}
				_, _ = wr.Write([]byte("]"))
			default:
				_, _ = wr.Write([]byte("true"))
			}
			_, _ = wr.Write([]byte("}"))
			_ = wr.Close()
		}
	}))
	tb.Cleanup(srv.Close)
	return srv
}

// newScaleDriver wires a controller to that appliance with one
// VolumePublication per volume behind the informer cache.
func newScaleDriver(tb testing.TB, n int) *Driver {
	tb.Helper()
	srv := newScaleDriverServer(tb, n)
	host, portText, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	var port int
	_, _ = fmt.Sscanf(portText, "%d", &port)
	client, err := truenas.NewClient(&truenas.ClientConfig{Host: host, Port: port, Protocol: "http", APIKey: "k",
		Timeout: 60 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = client.Close() })
	kube := newKubernetesPublicationStore(newFakeVolumePublicationClient(), "scale-csi", "bench")
	ctx := context.Background()
	for i := 0; i < n; i++ {
		node := fmt.Sprintf("k8s-%d", i%3)
		record := publicationRecord{Version: publicationRecordVersion, Node: node, EncodedID: "sc1.id-" + node,
			NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:" + node, IPs: []string{"192.168.122.10"},
			State: publicationStatePublished, AccessMode: 1, UpdatedAt: "2026-10-01T00:00:00Z"}
		if storeErr := kube.store(ctx, scaleDriverParent+"/"+scaleDriverVolume(i), nil, publicationPropertyKey(node), record); storeErr != nil {
			tb.Fatal(storeErr)
		}
	}
	cache, err := newPublicationCache(kube)
	if err != nil {
		tb.Fatal(err)
	}
	cache.start(ctx)
	tb.Cleanup(cache.close)
	d := &Driver{config: &Config{ZFS: ZFSConfig{DatasetParentName: scaleDriverParent}}, truenasClient: client}
	d.publicationStore = importingPublicationStore{kube: kube, legacy: zfsPublicationStore{client: client}, cache: cache}
	return d
}

// scaleListVolumesWalk is one external-attacher walk (100-entry pages).
func scaleListVolumesWalk(tb testing.TB, d *Driver) int {
	tb.Helper()
	token, total := "", 0
	for {
		resp, err := d.ListVolumes(context.Background(), &csi.ListVolumesRequest{StartingToken: token})
		if err != nil {
			tb.Fatal(err)
		}
		total += len(resp.Entries)
		if resp.NextToken == "" {
			return total
		}
		token = resp.NextToken
	}
}

func scaleHeapAlloc() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// BenchmarkScaleListVolumesWalk times one full walk, and reports as
// pinned-B/op the heap the walk's page cache keeps alive between walks (for
// its 30-second TTL), measured by dropping that cache after the walk.
func BenchmarkScaleListVolumesWalk(b *testing.B) {
	for _, n := range []int{30, 1000} {
		d := newScaleDriver(b, n)
		b.Run(fmt.Sprintf("volumes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if got := scaleListVolumesWalk(b, d); got != n {
					b.Fatalf("walk returned %d of %d", got, n)
				}
			}
			b.StopTimer()
			withCache := scaleHeapAlloc()
			d.volumePageCacheMu.Lock()
			d.volumePageCache = nil
			d.volumePageCacheMu.Unlock()
			withoutCache := scaleHeapAlloc()
			pinned := float64(0)
			if withCache > withoutCache {
				pinned = float64(withCache - withoutCache)
			}
			b.ReportMetric(pinned, "pinned-B/op")
		})
	}
}

// BenchmarkScaleRequestLogging is the RPC logging interceptor at the default
// verbosity, where the V(5) request dump is off.
func BenchmarkScaleRequestLogging(b *testing.B) {
	d := &Driver{config: &Config{}}
	req := &csi.ControllerPublishVolumeRequest{
		VolumeId: scaleDriverVolume(1), NodeId: "sc1.id-k8s-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		Secrets:       map[string]string{"a": "b"},
		VolumeContext: scaleDriverUserProps(1),
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ControllerPublishVolume"}
	handler := func(context.Context, interface{}) (interface{}, error) {
		return &csi.ControllerPublishVolumeResponse{}, nil
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := d.logInterceptor(context.Background(), req, info, handler); err != nil {
			b.Fatal(err)
		}
	}
}
