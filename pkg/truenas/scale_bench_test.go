package truenas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Benchmarks for the dataset read paths at 30 and 1,000 volumes, end to end
// through the real websocket client against an in-process appliance that
// answers with production-shaped rows (an NVMe-oF zvol with a dozen
// scale-csi user properties, under the full pool.dataset.query projection).

const scaleBenchParent = "flashstor/k8s"

var scaleBenchVolumes = []int{30, 1000}

func scaleBenchVolume(i int) string {
	return fmt.Sprintf("pvc-%08x-1c2d-4e5f-8a9b-%012x", i*7919, i)
}

func scaleBenchUserProps(i int) map[string]string {
	name := scaleBenchVolume(i)
	return map[string]string{
		"scale-csi:managed_resource":               "true",
		"scale-csi:csi_volume_name":                name,
		"scale-csi:driver_instance_id":             "org.scale-csi.nvmeof-prod",
		"scale-csi:provision_success":              "true",
		"scale-csi:requested_size_bytes":           "10737418240",
		"scale-csi:truenas_nvmeof_subsystem_id":    fmt.Sprint(100 + i),
		"scale-csi:truenas_nvmeof_namespace_id":    fmt.Sprint(200 + i),
		"scale-csi:truenas_nvmeof_portsubsys_id":   fmt.Sprint(300 + i),
		"scale-csi:zfs_performance_class":          "general",
		"scale-csi:csi_volume_content_source_type": "",
		"scale-csi:csi_share_volume_context":       `{"nqn":"nqn.2011-06.com.truenas:uuid:6a1f:` + name + `","transport":"tcp"}`,
		"org.freenas:description":                  "",
	}
}

func scaleBenchPoolProp(value interface{}, raw string, parsed interface{}, source string) map[string]interface{} {
	return map[string]interface{}{"value": value, "rawvalue": raw, "parsed": parsed, "source": source}
}

// scaleBenchPoolRow is one pool.dataset.query row.
func scaleBenchPoolRow(i int) map[string]interface{} {
	name := scaleBenchParent + "/" + scaleBenchVolume(i)
	up := map[string]interface{}{}
	for k, v := range scaleBenchUserProps(i) {
		up[k] = map[string]interface{}{"value": v, "rawvalue": v, "parsed": v, "source": "LOCAL"}
	}
	p := scaleBenchPoolProp
	return map[string]interface{}{
		"id": name, "name": name, "pool": "flashstor", "type": "VOLUME", "mountpoint": nil,
		"encrypted": false, "encryption_root": nil, "key_loaded": false, "locked": false,
		"key_format": p(nil, "none", nil, "DEFAULT"), "encryption": p("off", "off", "off", "DEFAULT"),
		"keyformat": p("none", "none", "none", "DEFAULT"), "encryptionroot": p("", "", "", "NONE"),
		"keystatus": p("", "-", nil, "NONE"), "children": []interface{}{},
		"used": p("1.02G", "1092616192", 1092616192, "NONE"), "available": p("1.75T", "1924145348608", 1924145348608, "NONE"),
		"quota": p(nil, "0", nil, "DEFAULT"), "refquota": p(nil, "0", nil, "DEFAULT"),
		"referenced": p("1.02G", "1092616192", 1092616192, "NONE"), "usedbysnapshots": p("0B", "0", 0, "NONE"),
		"reservation": p(nil, "0", nil, "DEFAULT"), "refreservation": p(nil, "0", nil, "DEFAULT"),
		"volsize": p("10G", "10737418240", 10737418240, "LOCAL"), "volblocksize": p("16K", "16384", 16384, "DEFAULT"),
		"compression": p("LZ4", "lz4", "lz4", "INHERITED"), "sync": p("STANDARD", "standard", "standard", "DEFAULT"),
		"atime": p(nil, "off", nil, "DEFAULT"), "recordsize": p(nil, "-", nil, "NONE"), "origin": p("", "", "", "NONE"),
		"creation":        p("Wed Jul 23 14:02 2026", "1784988120", map[string]interface{}{"$date": 1784988120000}, "NONE"),
		"user_properties": up,
	}
}

func scaleBenchResourceProp(value interface{}, raw string) map[string]interface{} {
	return map[string]interface{}{"value": value, "raw": raw, "source": map[string]interface{}{"type": "NONE", "value": nil}}
}

// scaleBenchResourceRow is one zfs.resource.query row.
func scaleBenchResourceRow(i int) map[string]interface{} {
	name := scaleBenchParent + "/" + scaleBenchVolume(i)
	up := map[string]interface{}{}
	for k, v := range scaleBenchUserProps(i) {
		up[k] = v
	}
	r := scaleBenchResourceProp
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

// scaleBenchServer answers pool.dataset.query (by "id" = or in), pool.dataset.update
// and zfs.resource.query for n volumes.
type scaleBenchServer struct {
	srv      *httptest.Server
	listing  []byte
	poolRows map[string][]byte
}

func newScaleBenchServer(tb testing.TB, n int) *scaleBenchServer {
	tb.Helper()
	s := &scaleBenchServer{poolRows: make(map[string][]byte, n)}
	rows := make([]map[string]interface{}, n)
	for i := range rows {
		rows[i] = scaleBenchResourceRow(i)
		row, err := json.Marshal(scaleBenchPoolRow(i))
		if err != nil {
			tb.Fatal(err)
		}
		s.poolRows[scaleBenchParent+"/"+scaleBenchVolume(i)] = row
	}
	var err error
	if s.listing, err = json.Marshal(rows); err != nil {
		tb.Fatal(err)
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }, WriteBufferSize: 64 << 10}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			s.answer(wr, req.Method, req.Params)
			_, _ = wr.Write([]byte("}"))
			_ = wr.Close()
		}
	}))
	tb.Cleanup(s.srv.Close)
	return s
}

func (s *scaleBenchServer) answer(wr interface{ Write([]byte) (int, error) }, method string, params []json.RawMessage) {
	switch method {
	case datasetResourceQueryMethod:
		if !bytes.Contains(params[0], []byte(`"get_user_properties"`)) {
			_, _ = wr.Write([]byte("[]")) // capability probe
			return
		}
		_, _ = wr.Write(s.listing)
	case "pool.dataset.update":
		var name string
		_ = json.Unmarshal(params[0], &name)
		_, _ = wr.Write(s.poolRows[name])
	case "pool.dataset.query":
		var filters [][]interface{}
		_ = json.Unmarshal(params[0], &filters)
		_, _ = wr.Write([]byte("["))
		if len(filters) == 1 && len(filters[0]) == 3 {
			switch names := filters[0][2].(type) {
			case []interface{}:
				for i, name := range names {
					if i > 0 {
						_, _ = wr.Write([]byte(","))
					}
					_, _ = wr.Write(s.poolRows[name.(string)])
				}
			case string:
				_, _ = wr.Write(s.poolRows[names])
			}
		}
		_, _ = wr.Write([]byte("]"))
	default:
		_, _ = wr.Write([]byte("true"))
	}
}

func (s *scaleBenchServer) client(tb testing.TB) *Client {
	tb.Helper()
	hostport := strings.TrimPrefix(s.srv.URL, "http://")
	host, portText, _ := strings.Cut(hostport, ":")
	var port int
	_, _ = fmt.Sscanf(portText, "%d", &port)
	c, err := NewClient(&ClientConfig{Host: host, Port: port, Protocol: "http", APIKey: "k",
		Timeout: 60 * time.Second, ConnectTimeout: 5 * time.Second, MaxConnections: 1})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

var scaleBenchSink interface{}

// BenchmarkScaleDatasetReads: DatasetGet and DatasetUpdate return one row;
// DatasetGetByNames hydrates every volume in one call (a ListVolumes page is
// at most 100, so 1,000 is the startup and reconcile shape); and
// DatasetQueryByParent is the full managed listing a ListVolumes walk starts with.
func BenchmarkScaleDatasetReads(b *testing.B) {
	ctx := context.Background()
	for _, n := range scaleBenchVolumes {
		s := newScaleBenchServer(b, n)
		c := s.client(b)
		names := make([]string, n)
		for i := range names {
			names[i] = scaleBenchParent + "/" + scaleBenchVolume(i)
		}
		b.Run(fmt.Sprintf("volumes=%d/DatasetGet", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ds, err := c.DatasetGet(ctx, names[n/2])
				if err != nil || ds.Name != names[n/2] {
					b.Fatal(err)
				}
				scaleBenchSink = ds
			}
		})
		b.Run(fmt.Sprintf("volumes=%d/DatasetUpdate", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				ds, err := c.DatasetUpdate(ctx, names[n/2], &DatasetUpdateParams{})
				if err != nil || ds.Name != names[n/2] {
					b.Fatal(err)
				}
				scaleBenchSink = ds
			}
		})
		b.Run(fmt.Sprintf("volumes=%d/DatasetGetByNames", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := c.DatasetGetByNames(ctx, names)
				if err != nil || len(out) != n {
					b.Fatal(err, len(out))
				}
				scaleBenchSink = out
			}
		})
		b.Run(fmt.Sprintf("volumes=%d/DatasetQueryByParent", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := c.DatasetQueryByParent(ctx, scaleBenchParent)
				if err != nil || len(out) != n {
					b.Fatal(err, len(out))
				}
				scaleBenchSink = out
			}
		})
	}
}
