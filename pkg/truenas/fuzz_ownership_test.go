package truenas

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzResponseResultAPIErrorFidelity targets the JSON-RPC 2.0 envelope decode
// in client.go: every reply from the TrueNAS WebSocket is unmarshalled into
// rpcResponse and reduced to (json.RawMessage, error) by responseResult. The
// threat model calls out that TrueNAS collapses many unrelated failures onto a
// bare "-32602 Invalid params" and that a decode which mis-handles the
// envelope does not merely panic -- it can silently turn a real backend error
// into a "success" (nil error, garbage result) or vice versa. The invariant
// pinned here is envelope fidelity: an "error" field must always surface as a
// non-nil *APIError with the exact code/message carried over, and an envelope
// with no "error" field must never manufacture one.
func FuzzResponseResultAPIErrorFidelity(f *testing.F) {
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params"}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found","data":{"class":"ValidationError","errname":"EINVAL"}}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":null,"error":null}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":true,"error":{"code":0,"message":""}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var resp rpcResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			return
		}
		result, err := responseResult(&resp)
		if resp.Error != nil {
			if err == nil {
				t.Fatalf("JSON-RPC error field %+v was silently swallowed (result=%s)", resp.Error, result)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("error field was not surfaced as *APIError: %v", err)
			}
			if apiErr.Code != resp.Error.Code {
				t.Fatalf("APIError.Code %d != response error code %d", apiErr.Code, resp.Error.Code)
			}
			if apiErr.Message != resp.Error.Message {
				t.Fatalf("APIError.Message %q != response error message %q", apiErr.Message, resp.Error.Message)
			}
			if result != nil {
				t.Fatalf("result must be nil when an error field is present, got %s", result)
			}
			return
		}
		if err != nil {
			t.Fatalf("responseResult returned error %v for a response with no error field", err)
		}
	})
}

// FuzzTypedDatasetDecodeMatchesInterfaceResourceQuery is the resourceQuery=true
// twin of FuzzTypedDatasetDecodeMatchesInterface (typed_decode_test.go), which
// only ever exercises the resourceQuery=false (pool.dataset.query) path even
// though its own seed corpus includes zfs.resource.query-shaped fixtures
// (dataset-resource-26.0.json, dataset-origins-26.0.json). The typed and
// legacy interface{} decoders must stay deep-equal on THIS path too --
// zfs.resource.query is the read the orphan/reconcile scan and fencing use to
// decide dataset ownership, so a divergence here is exactly the class of bug
// the fuzz-expansion brief is about.
func FuzzTypedDatasetDecodeMatchesInterfaceResourceQuery(f *testing.F) {
	f.Add(readTypedFixture(f, "dataset-resource-26.0.json"))
	f.Add(readTypedFixture(f, "dataset-origins-26.0.json"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		var generic []interface{}
		if err := json.Unmarshal(payload, &generic); err != nil {
			return
		}
		if !decodedKeysLowercase(generic) {
			t.Skip("off-wire-contract mixed-case key; typed/interface divergence is stdlib case-insensitivity, not a bug")
		}
		canonical, err := json.Marshal(generic)
		if err != nil {
			return
		}
		var typed []*rawDataset
		if err := json.Unmarshal(canonical, &typed); err != nil {
			return
		}
		got := rawDatasetsToDatasets(typed, true)
		want := make([]*Dataset, 0, len(generic))
		for _, item := range generic {
			dataset, err := parseDatasetResource(item)
			if err != nil {
				continue
			}
			dataset.ResourceQuery = true
			want = append(want, dataset)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("typed dataset decode (resourceQuery=true) diverged from interface decode")
		}
	})
}

// FuzzNormalizeCSIUserPropertiesFold pins the collision rule documented on
// normalizeCSIUserProperties (prop_ns.go): a LOCAL value beats a non-local one
// regardless of which namespace spelling carries it, a tie goes to the
// canonical spelling, and -- the safety-critical part -- the legacy
// truenas-csi:* key must NEVER survive the fold. A driver that still reads a
// legacy key after this call would see stale/duplicate ownership data.
func FuzzNormalizeCSIUserPropertiesFold(f *testing.F) {
	f.Add("managed_resource", true, "true", "local", true, "true", "local")
	f.Add("managed_resource", true, "true", "local", true, "false", "inherited")
	f.Add("managed_resource", false, "", "", true, "true", "local")
	f.Add("managed_resource", true, "true", "local", false, "", "")
	f.Add("driver_instance_id", true, "instance-a", "LOCAL", true, "instance-b", "tank/src@snap")
	f.Fuzz(func(t *testing.T, suffix string,
		haveCanonical bool, canonicalValue, canonicalSource string,
		haveLegacy bool, legacyValue, legacySource string,
	) {
		canonicalKey := CSIPropertyNamespace + suffix
		legacyKey := LegacyCSIPropertyNamespace + suffix
		props := map[string]UserProperty{}
		if haveCanonical {
			props[canonicalKey] = UserProperty{Value: canonicalValue, Source: canonicalSource}
		}
		if haveLegacy {
			props[legacyKey] = UserProperty{Value: legacyValue, Source: legacySource}
		}

		legacyReturned := normalizeCSIUserProperties(props)

		if _, present := props[legacyKey]; present {
			t.Fatalf("legacy key %q survived normalizeCSIUserProperties", legacyKey)
		}

		if !haveLegacy {
			if legacyReturned != nil {
				t.Fatalf("no legacy entry was present but normalizeCSIUserProperties returned %#v", legacyReturned)
			}
			if haveCanonical {
				got, ok := props[canonicalKey]
				if !ok || got.Value != canonicalValue || got.Source != canonicalSource {
					t.Fatalf("canonical-only entry mutated: got %#v", got)
				}
			}
			return
		}

		got, ok := legacyReturned[legacyKey]
		if !ok || got.Value != legacyValue || got.Source != legacySource {
			t.Fatalf("returned legacy map does not carry the raw legacy entry: %#v", legacyReturned)
		}

		canonicalIsLocalExisting := haveCanonical && isLocalCSIPropertySource(canonicalSource)
		legacyIsLocal := isLocalCSIPropertySource(legacySource)
		var wantValue, wantSource string
		if haveCanonical && (canonicalIsLocalExisting || !legacyIsLocal) {
			wantValue, wantSource = canonicalValue, canonicalSource
		} else {
			wantValue, wantSource = legacyValue, legacySource
		}
		resolved, ok := props[canonicalKey]
		if !ok {
			t.Fatalf("canonical key %q missing after fold", canonicalKey)
		}
		if resolved.Value != wantValue || resolved.Source != wantSource {
			t.Fatalf("canonical fold resolved to %#v, want {Value:%q Source:%q}", resolved, wantValue, wantSource)
		}

		// Idempotence: a second pass over the already-folded map must be a
		// pure no-op (no legacy keys remain to fold).
		again := normalizeCSIUserProperties(props)
		if again != nil {
			t.Fatalf("second normalizeCSIUserProperties pass was not a no-op: %#v", again)
		}
	})
}

// FuzzIsLocalCSIPropertySource is the pkg/truenas twin of the driver-package
// isLocalUserPropertySource fuzz target: it pins the exact-match discipline
// (case-insensitive, trimmed "local" only) and specifically that an
// origin-snapshot-shaped source (ZFS clone inheritance, e.g. "tank/src@snap")
// is never classified as local.
func FuzzIsLocalCSIPropertySource(f *testing.F) {
	f.Add("local")
	f.Add("LOCAL")
	f.Add(" Local\t")
	f.Add("inherited")
	f.Add("INHERITED")
	f.Add("tank/src@snap")
	f.Add("localhost")
	f.Add("")
	f.Fuzz(func(t *testing.T, source string) {
		got := isLocalCSIPropertySource(source)
		want := strings.EqualFold(strings.TrimSpace(source), "local")
		if got != want {
			t.Fatalf("isLocalCSIPropertySource(%q) = %v, want %v", source, got, want)
		}
		if strings.Contains(source, "@") && got {
			t.Fatalf("isLocalCSIPropertySource(%q) = true for an origin-snapshot-shaped source", source)
		}
	})
}

// FuzzParseDatasetResourceOwnershipSafety fuzzes parseDatasetResource's
// per-entry user-property decode, which is explicitly documented (dataset.go)
// to degrade a flat string value to Source="" ("unknown") specifically so
// datasetHasLocalUserProperty callers "fail safe rather than misreporting
// local". This pins that contract end to end: the decoded Source is either an
// exact passthrough of the wire's "source" field, or "" for the flat-string
// shape -- and in neither case does isLocalCSIPropertySource read the result
// as local unless the wire literally said "local".
func FuzzParseDatasetResourceOwnershipSafety(f *testing.F) {
	f.Add("scale-csi:managed_resource", "true", "local", true)
	f.Add("scale-csi:managed_resource", "true", "local", false)
	f.Add("scale-csi:managed_resource", "true", "tank/src@snap", false)
	f.Add("scale-csi:managed_resource", "true", "", true)
	f.Add("scale-csi:driver_instance_id", "instance-a", "LOCAL", false)
	f.Fuzz(func(t *testing.T, key, value, source string, flat bool) {
		if !utf8.ValidString(key) || !utf8.ValidString(value) || !utf8.ValidString(source) {
			return
		}
		if key == "" || strings.HasPrefix(key, LegacyCSIPropertyNamespace) {
			// Empty keys are a degenerate JSON-object edge case unrelated to this
			// property; legacy-namespace folding is covered by
			// FuzzNormalizeCSIUserPropertiesFold and would otherwise obscure the
			// per-entry decode invariant this target pins.
			return
		}

		m := map[string]interface{}{"name": "tank/x", "id": "tank/x"}
		userProps := map[string]interface{}{}
		if flat {
			userProps[key] = value
		} else {
			userProps[key] = map[string]interface{}{"value": value, "source": source}
		}
		m["user_properties"] = userProps

		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("failed to marshal synthetic payload: %v", err)
		}
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatalf("failed to round-trip synthetic payload: %v", err)
		}

		ds, err := parseDatasetResource(data)
		if err != nil {
			t.Fatalf("parseDatasetResource rejected a well-formed object payload: %v", err)
		}
		prop, ok := ds.UserProperties[key]
		if !ok {
			t.Fatalf("user property %q vanished during decode", key)
		}

		if flat {
			if prop.Source != "" {
				t.Fatalf("flat-string user property must decode with an EMPTY source, got %q", prop.Source)
			}
		} else {
			if prop.Value != value {
				t.Fatalf("user property value corrupted in decode: got %q want %q", prop.Value, value)
			}
			if prop.Source != source {
				t.Fatalf("user property source corrupted in decode: got %q want %q", prop.Source, source)
			}
		}

		wireSaidLocal := !flat && strings.EqualFold(strings.TrimSpace(source), "local")
		if isLocalCSIPropertySource(prop.Source) && !wireSaidLocal {
			t.Fatalf("decoded source %q reads as local but the wire source was %q (flat=%v): fail-closed violation", prop.Source, source, flat)
		}
	})
}

// FuzzParseISCSIInitiatorAllowlistShape pins the nil-vs-empty-slice contract
// documented directly on parseISCSITarget's initiator decode: an absent or
// JSON-null "initiators" key must decode to a nil slice (unknown), while a
// present array -- even an empty one -- must decode to a non-nil slice,
// because TrueNAS 26.0's SCST backend treats an empty initiator list as
// allow-all. Collapsing "unknown" and "explicit empty" into the same Go zero
// value would misroute a fencing allow/deny decision.
func FuzzParseISCSIInitiatorAllowlistShape(f *testing.F) {
	f.Add([]byte(`{"id":1,"comment":"c"}`))
	f.Add([]byte(`{"id":1,"initiators":null}`))
	f.Add([]byte(`{"id":1,"initiators":[]}`))
	f.Add([]byte(`{"id":1,"initiators":["iqn.a","iqn.b"]}`))
	f.Add([]byte(`{"id":1,"initiators":[1,2,"iqn.c"]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			return
		}
		m, ok := data.(map[string]interface{})
		if !ok {
			return
		}
		group, err := parseISCSIInitiator(data)
		if err != nil {
			return
		}

		rawInitiators, present := m["initiators"]
		if !present || rawInitiators == nil {
			if group.Initiators != nil {
				t.Fatalf("absent/null initiators must decode to nil (unknown), got %#v", group.Initiators)
			}
			return
		}
		arr, ok := rawInitiators.([]interface{})
		if !ok {
			return
		}
		if group.Initiators == nil {
			t.Fatalf("a present initiators array (even empty) must decode to a non-nil slice")
		}
		var want []string
		for _, item := range arr {
			if s, ok := item.(string); ok {
				want = append(want, s)
			}
		}
		if want == nil {
			want = []string{}
		}
		if !reflect.DeepEqual(group.Initiators, want) {
			t.Fatalf("parseISCSIInitiator altered the initiator list: got %#v want %#v", group.Initiators, want)
		}
	})
}

// FuzzParseReplicationJobNeverPanics targets parseReplicationJob, which
// decodes core.get_jobs entries the driver treats as reap candidates
// (ReplicationJobList/ReplicationJobAbort). replicationJobID rejects negative
// and non-integral IDs, and UnknownReplicationJobID (-1) is a reserved
// sentinel meaning "no proven job" -- so a successfully parsed job must never
// carry a negative ID, or reap logic could confuse a malformed response with
// the sentinel.
func FuzzParseReplicationJobNeverPanics(f *testing.F) {
	f.Add([]byte(`{"id":42,"method":"replication.run_onetime","state":"RUNNING","arguments":[{"target_dataset":"tank/x","source_datasets":["tank/y"]}]}`))
	f.Add([]byte(`{"id":42,"method":"replication.run_onetime","state":"success","arguments":"[{\"target_dataset\":\"tank/x\"}]"}`))
	f.Add([]byte(`{"id":"not-a-number","state":"RUNNING"}`))
	f.Add([]byte(`{"id":-1,"state":"RUNNING"}`))
	f.Add([]byte(`{"id":1.5,"state":"RUNNING"}`))
	f.Add([]byte(`{"id":42,"state":"RUNNING","args":[{"target_dataset":"tank/x"}]}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			return
		}
		job, err := parseReplicationJob(data)
		if err != nil {
			return
		}
		if job.ID < 0 {
			t.Fatalf("replication job ID must never be negative (collides with UnknownReplicationJobID sentinel): %d", job.ID)
		}
		if job.State != strings.ToUpper(strings.TrimSpace(job.State)) {
			t.Fatalf("replication job state %q is not upper-trimmed", job.State)
		}
	})
}

// FuzzJobDispatcherOfferNeverPanics targets jobDispatcher.offer, the WebSocket
// read loop's entry point for core.get_jobs push notifications (job.go /
// job_dispatcher.go). It is documented to perform "one typed parse and one
// non-blocking send" and must never panic or block regardless of malformed
// notification content, since it runs directly on the connection read loop.
func FuzzJobDispatcherOfferNeverPanics(f *testing.F) {
	f.Add([]byte(`{"collection":"core.get_jobs","fields":{"id":5,"state":"SUCCESS"}}`))
	f.Add([]byte(`{"collection":"core.get_jobs","fields":{"id":5,"state":"FAILED","error":"boom"}}`))
	f.Add([]byte(`{"collection":"core.get_jobs","fields":{"id":-1,"state":"RUNNING"}}`))
	f.Add([]byte(`{"collection":"other.thing","fields":{}}`))
	f.Add([]byte(`{"collection":"core.get_jobs","fields":{"id":"not-a-number","state":"RUNNING"}}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))

	d := newJobDispatcher()
	f.Fuzz(func(t *testing.T, raw []byte) {
		d.offer(raw)
	})
}

// FuzzParseShareAndBlockResponses sweeps every remaining legacy
// interface{}-based response parser named in the fuzz-expansion threat model
// (NFS share, iSCSI target/extent/target-extent/global-config, and the
// NVMe-oF subsystem/host/namespace/port/association parsers) over the same
// arbitrary decoded JSON value. These parsers are simple type-assertion
// walks with no deeper cross-field invariant, so the primary oracle here is
// panic-freedom; parseNVMeoFSubsystem additionally gets a fabrication check
// on its Hosts slice since that field gates NVMe-oF host allowlisting.
func FuzzParseShareAndBlockResponses(f *testing.F) {
	f.Add([]byte(`{"id":1,"path":"/mnt/tank/x","paths":["/mnt/tank/x"],"hosts":["10.0.0.1"],"networks":["10.0.0.0/24"],"ro":false,"enabled":true}`))
	f.Add([]byte(`{"id":1,"name":"iqn.2005-10.org.freenas.ctl:target","mode":"ISCSI","groups":[{"portal":1,"initiator":2,"authmethod":"CHAP","auth":3,"auth_networks":["10.0.0.0/24"]}]}`))
	f.Add([]byte(`{"id":1,"name":"extent1","type":"DISK","disk":"zvol/tank/x","blocksize":512,"enabled":true}`))
	f.Add([]byte(`{"id":1,"target":2,"extent":3,"lunid":0}`))
	f.Add([]byte(`{"id":1,"initiators":[],"comment":"deny-list"}`))
	f.Add([]byte(`{"id":1,"subnqn":"nqn.2011-06.com.truenas:sub1","allow_any_host":false,"hosts":[1,2],"namespaces":[3],"ports":[4]}`))
	f.Add([]byte(`{"id":1,"hostnqn":"nqn.2014-08.org.nvmexpress:uuid:abc"}`))
	f.Add([]byte(`{"id":1,"nsid":1,"device_type":"ZVOL","device_path":"zvol/tank/x","enabled":true}`))
	f.Add([]byte(`{"id":1,"index":0,"addr_trtype":"tcp","addr_traddr":"192.0.2.1","addr_trsvcid":4420,"addr_adrfam":"ipv4","enabled":true,"subsystems":[1]}`))
	f.Add([]byte(`{"id":1,"host":{"id":2,"hostnqn":"nqn.x"},"subsys":{"id":3}}`))
	f.Add([]byte(`{"id":1,"port":{"id":2},"subsys":{"id":3}}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`"just a string"`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		var data interface{}
		if err := json.Unmarshal(raw, &data); err != nil {
			return
		}

		_, _ = parseNFSShare(data)
		_, _ = parseISCSITarget(data)
		_, _ = parseISCSIExtent(data)
		_, _ = parseISCSITargetExtent(data)
		_, _ = parseISCSIGlobalConfig(data)
		_, _ = parseISCSIInitiator(data)
		_, _ = parseNVMeoFHost(data)
		_, _ = parseNVMeoFNamespace(data)
		_, _ = parseNVMeoFPort(data)
		_, _ = parseNVMeoFHostSubsys(data)
		_, _ = parseNVMeoFPortSubsys(data)

		if subsys, err := parseNVMeoFSubsystem(data); err == nil {
			if m, ok := data.(map[string]interface{}); ok {
				if hosts, ok := m["hosts"].([]interface{}); ok && len(subsys.Hosts) > len(hosts) {
					t.Fatalf("parseNVMeoFSubsystem fabricated host IDs: got %d entries, wire had %d", len(subsys.Hosts), len(hosts))
				}
			}
		}
	})
}
