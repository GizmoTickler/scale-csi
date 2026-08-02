package truenas

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// datasetResourceQueryMethod is the TrueNAS 26.0 path-scoped dataset read API.
// It returns properties + user-properties + source + origin in one call and is
// scoped to explicit paths, avoiding the full-system user-property
// materialization that pool.dataset.query forces.
const datasetResourceQueryMethod = "zfs.resource.query"

// Dataset represents a ZFS dataset from the TrueNAS API.
type Dataset struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Pool       string          `json:"pool"`
	Type       string          `json:"type"`
	Mountpoint string          `json:"mountpoint"`
	Used       DatasetProperty `json:"used"`
	Available  DatasetProperty `json:"available"`
	Quota      DatasetProperty `json:"quota"`
	Refquota   DatasetProperty `json:"refquota"`
	// Referenced and Usedbysnapshots are what let usage reporting compare LIKE
	// WITH LIKE (GF2-fix4/H1): in ZFS `refquota` bounds `referenced`, while
	// `quota` bounds `used` (= referenced + usedbysnapshots + usedbychildren +
	// usedbyrefreservation). Comparing `used` against `refquota` reports every
	// volume that merely HAS snapshots as near-quota.
	Referenced      DatasetProperty         `json:"referenced"`
	Usedbysnapshots DatasetProperty         `json:"usedbysnapshots"`
	Reservation     DatasetProperty         `json:"reservation"`
	Refreservation  DatasetProperty         `json:"refreservation"`
	Volsize         DatasetProperty         `json:"volsize"`
	Volblocksize    DatasetProperty         `json:"volblocksize"`
	Origin          DatasetProperty         `json:"origin"`
	Creation        DatasetProperty         `json:"creation"`
	UserProperties  map[string]UserProperty `json:"user_properties"`
	// CreatedByCall is true only when pool.dataset.create itself created this
	// object. The idempotent AlreadyExists fallback leaves it false so callers
	// never post-stamp a raced foreign dataset.
	CreatedByCall bool `json:"-"`
	// ResourceQuery is true when the dataset came from the TrueNAS 26.0
	// zfs.resource.query read path rather than pool.dataset.query.
	ResourceQuery bool `json:"-"`
}

// DatasetProperty represents a ZFS property with parsed and raw values.
type DatasetProperty struct {
	Value    interface{} `json:"value"`
	Rawvalue string      `json:"rawvalue"`
	Parsed   interface{} `json:"parsed"`
	Source   string      `json:"source"`
}

// UserProperty represents a user-defined ZFS property.
type UserProperty struct {
	Value  string `json:"value"`
	Source string `json:"source"`
}

const datasetManagedResourceProperty = "truenas-csi:managed_resource"

var datasetQueryProperties = []string{
	"used",
	"available",
	"quota",
	"refquota",
	"referenced",
	"usedbysnapshots",
	"reservation",
	"refreservation",
	"volsize",
	"volblocksize",
	"creation",
}

// DatasetCreateParams holds parameters for creating a dataset.
type DatasetCreateParams struct {
	Name           string `json:"name"`
	Type           string `json:"type,omitempty"`         // FILESYSTEM or VOLUME
	Volsize        int64  `json:"volsize,omitempty"`      // For volumes
	Volblocksize   string `json:"volblocksize,omitempty"` // For volumes
	Sparse         bool   `json:"sparse,omitempty"`       // For volumes
	Quota          int64  `json:"quota,omitempty"`        // For filesystems
	Refquota       int64  `json:"refquota,omitempty"`     // For filesystems
	Reservation    int64  `json:"reservation,omitempty"`
	Refreservation int64  `json:"refreservation,omitempty"`
	Comments       string `json:"comments,omitempty"`
	Readonly       string `json:"readonly,omitempty"` // ON, OFF, INHERIT
	Atime          string `json:"atime,omitempty"`
	Exec           string `json:"exec,omitempty"`
	Sync           string `json:"sync,omitempty"`
	Compression    string `json:"compression,omitempty"`
	Deduplication  string `json:"deduplication,omitempty"`
	Logbias        string `json:"logbias,omitempty"`
	Primarycache   string `json:"primarycache,omitempty"`
	Secondarycache string `json:"secondarycache,omitempty"`
	Checksum       string `json:"checksum,omitempty"`
	Snapdir        string `json:"snapdir,omitempty"`
	Copies         int    `json:"copies,omitempty"`
	Recordsize     string `json:"recordsize,omitempty"`
	// SpecialSmallBlockSize is the `special_small_block_size` property. The key
	// is NOT `special_small_blocks`: pool.dataset.create/update reject that
	// spelling. It requires the pool to have a `special` allocation-class vdev.
	SpecialSmallBlockSize string               `json:"special_small_block_size,omitempty"`
	Casesensitivity       string               `json:"casesensitivity,omitempty"`
	Aclmode               string               `json:"aclmode,omitempty"`
	Acltype               string               `json:"acltype,omitempty"`
	ShareType             string               `json:"share_type,omitempty"`
	Xattr                 string               `json:"xattr,omitempty"`
	UserProperties        []UserPropertyUpdate `json:"user_properties,omitempty"`
}

// DatasetUpdateParams holds parameters for updating a dataset.
type DatasetUpdateParams struct {
	Volsize              int64                `json:"volsize,omitempty"`
	Quota                interface{}          `json:"quota,omitempty"`
	Refquota             interface{}          `json:"refquota,omitempty"`
	Reservation          interface{}          `json:"reservation,omitempty"`
	Refreservation       interface{}          `json:"refreservation,omitempty"`
	Comments             string               `json:"comments,omitempty"`
	Readonly             string               `json:"readonly,omitempty"`
	UserPropertiesUpdate []UserPropertyUpdate `json:"user_properties_update,omitempty"`
}

// ---------------------------------------------------------------------------
// pool.dataset.create / pool.dataset.update accepted-key schema (TrueNAS 26.0)
// ---------------------------------------------------------------------------
//
// TrueNAS 26.0's middleware is SCHEMA-STRICT and its models are PER DATASET
// TYPE: a payload key outside the model for the type being created is rejected
// with "[EINVAL] data.<TYPE>.<key>: Extra inputs are not permitted", which the
// WebSocket transport collapses to the JSON-RPC -32602 "Invalid params" an
// operator actually sees. The classification below is what MockClient enforces,
// so "the driver emits a key 26.0 does not accept for this dataset type" fails a
// unit test instead of every CreateVolume on a real appliance (v1.5.0's
// zfsPerformanceClass blocker).
//
// The classification is a HAND-MAINTAINED literal, not a derivation from the
// params structs. That is the whole point: a set derived from the structs would
// auto-classify every new field as accepted and could only ever catch keys
// already known to be bad. TestDatasetParamsSchemaCoverage diffs the structs'
// JSON tags against these maps in both directions, so a new field with no
// classification entry — and a stale entry for a removed field — is a test
// failure rather than a live surprise.
//
// SCOPE OF THE CLAIM: every entry below is annotated with the evidence behind
// it. Only the datasetKeyRejectedBy26 entries and the FILESYSTEM/VOLUME split
// the driver itself already enforces are probe- or code-backed; the rest are ZFS
// semantics, marked as such. Keys whose type split is genuinely unknown are
// classified datasetKeyBothTypes (permissive) and say so, so this is a real
// per-type gate for what has been established and does not pretend to be a
// complete model of 26.0.

// datasetKeyScope classifies one payload key against TrueNAS 26.0's per-type
// pool.dataset.create / pool.dataset.update models.
type datasetKeyScope uint8

const (
	// datasetKeyRejectedBy26 — absent from BOTH type models on 26.0.
	datasetKeyRejectedBy26 datasetKeyScope = iota
	// datasetKeyBothTypes — accepted for FILESYSTEM and VOLUME alike.
	datasetKeyBothTypes
	// datasetKeyFilesystemOnly — accepted for FILESYSTEM, rejected for VOLUME.
	datasetKeyFilesystemOnly
	// datasetKeyVolumeOnly — accepted for VOLUME, rejected for FILESYSTEM.
	datasetKeyVolumeOnly
)

// datasetCreateKeyScopes classifies every JSON tag DatasetCreateParams can put
// on the wire.
//
// PROBED (26.0.0-BETA.1, nas01, pool flashstor, 2026-08-02, one property per
// call, against BOTH pool.dataset.create and pool.dataset.update, for BOTH
// dataset types):
//
//	logbias        => [EINVAL] data.FILESYSTEM.logbias: Extra inputs are not permitted
//	primarycache   => [EINVAL] data.FILESYSTEM.primarycache: Extra inputs are not permitted
//	secondarycache => [EINVAL] data.VOLUME.secondarycache: Extra inputs are not permitted
//
// Those three are absent from the 26.0 API schema, and an audit of
// core.get_methods found no alternative setter (filesystem.set_zfs_attributes is
// POSIX attributes, not ZFS properties). The struct fields are retained because
// applyDatasetProperties still honors them for operators on the 25.04-25.10
// floor this driver also supports; on 26.0 they cannot be set through the API at
// all, only out of band with `zfs set` (e.g. on the parent dataset, so new
// volumes inherit).
var datasetCreateKeyScopes = map[string]datasetKeyScope{
	// Structural: present in both models.
	"name": datasetKeyBothTypes,
	"type": datasetKeyBothTypes,

	// VOLUME-only. DRIVER-BACKED: createDataset (controller.go) sets all three
	// on the zvol branch only, and ZFS itself has no such properties on a
	// filesystem.
	"volsize":      datasetKeyVolumeOnly,
	"volblocksize": datasetKeyVolumeOnly,
	"sparse":       datasetKeyVolumeOnly,

	// FILESYSTEM-only, DRIVER-BACKED: createDataset writes refquota only on the
	// NFS filesystem branch, applyDatasetProperties warn-and-drops recordsize and
	// atime for a VOLUME, and zfsFilesystemOnlyProperties encodes the same split
	// for the curated presets.
	"refquota":   datasetKeyFilesystemOnly,
	"recordsize": datasetKeyFilesystemOnly,
	"atime":      datasetKeyFilesystemOnly,
	// quota is ZFS SEMANTICS, not driver-backed — the driver never sets it. It is
	// refquota's dataset-level sibling and ZFS has no zvol form of it.
	"quota": datasetKeyFilesystemOnly,

	// FILESYSTEM-only by ZFS SEMANTICS — these describe a mounted POSIX
	// namespace, which a zvol does not have. The driver only ever sets acltype
	// and aclmode, and applyDatasetACLParams already returns early for a VOLUME.
	// NOT independently probed against 26.0's VOLUME model; if one of these turns
	// out to be accepted there, this is the line to correct.
	"exec":            datasetKeyFilesystemOnly,
	"snapdir":         datasetKeyFilesystemOnly,
	"casesensitivity": datasetKeyFilesystemOnly,
	"aclmode":         datasetKeyFilesystemOnly,
	"acltype":         datasetKeyFilesystemOnly,
	"share_type":      datasetKeyFilesystemOnly,
	"xattr":           datasetKeyFilesystemOnly,

	// Both types. refreservation is DRIVER-BACKED (createDataset writes it on
	// both branches) and special_small_block_size is PRESET-BACKED (it is in the
	// `database` preset and is absent from zfsFilesystemOnlyProperties, so it is
	// emitted for zvols too). The rest are ZFS properties defined for filesystems
	// and volumes alike; their type split was not separately probed, and BOTH is
	// the permissive classification.
	"reservation":              datasetKeyBothTypes,
	"refreservation":           datasetKeyBothTypes,
	"comments":                 datasetKeyBothTypes,
	"readonly":                 datasetKeyBothTypes,
	"sync":                     datasetKeyBothTypes,
	"compression":              datasetKeyBothTypes,
	"deduplication":            datasetKeyBothTypes,
	"checksum":                 datasetKeyBothTypes,
	"copies":                   datasetKeyBothTypes,
	"special_small_block_size": datasetKeyBothTypes,
	"user_properties":          datasetKeyBothTypes,

	// Rejected by 26.0 outright — see the probe transcript above.
	"logbias":        datasetKeyRejectedBy26,
	"primarycache":   datasetKeyRejectedBy26,
	"secondarycache": datasetKeyRejectedBy26,
}

// datasetUpdateKeyScopes classifies every JSON tag DatasetUpdateParams can put
// on the wire. Same evidence rules as datasetCreateKeyScopes; the type split is
// the one ZFS enforces (a zvol is sized by volsize and carries no dataset
// quota, a filesystem is the reverse).
var datasetUpdateKeyScopes = map[string]datasetKeyScope{
	"volsize":                datasetKeyVolumeOnly,
	"quota":                  datasetKeyFilesystemOnly,
	"refquota":               datasetKeyFilesystemOnly,
	"reservation":            datasetKeyBothTypes,
	"refreservation":         datasetKeyBothTypes,
	"comments":               datasetKeyBothTypes,
	"readonly":               datasetKeyBothTypes,
	"user_properties_update": datasetKeyBothTypes,
}

// datasetKeyAccepted reports whether TrueNAS 26.0 accepts key for datasetType,
// per the classification map. An unclassified key is treated as rejected: a
// field added to a params struct without a classification entry must fail
// closed, and TestDatasetParamsSchemaCoverage names it explicitly.
//
// An EMPTY datasetType means "the caller does not know the type" (the mock's
// update path when the dataset is not in its store). It resolves permissively:
// only the keys neither model accepts are rejected.
func datasetKeyAccepted(scopes map[string]datasetKeyScope, key, datasetType string) bool {
	scope, classified := scopes[key]
	if !classified {
		return false
	}
	switch scope {
	case datasetKeyRejectedBy26:
		return false
	case datasetKeyBothTypes:
		return true
	case datasetKeyFilesystemOnly:
		return datasetType != "VOLUME"
	case datasetKeyVolumeOnly:
		return datasetType == "VOLUME" || datasetType == ""
	default:
		return false
	}
}

// datasetParamsJSONKeys lists the JSON tag names of a params struct. A non-struct
// (a pointer, say) returns nothing rather than panicking in reflect: the caller
// is TestDatasetParamsSchemaCoverage, whose require.NotEmpty then fails with the
// callsite instead of a stack trace.
func datasetParamsJSONKeys(params interface{}) []string {
	structType := reflect.TypeOf(params)
	if structType == nil || structType.Kind() != reflect.Struct {
		return nil
	}
	keys := make([]string, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i++ {
		tag := structType.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name != "" {
			keys = append(keys, name)
		}
	}
	return keys
}

// UnsupportedDatasetPropertyError builds the rejection TrueNAS 26.0 produces for
// a payload key outside the method's schema.
//
// datasetType, when set, reproduces the live error's dataset-type path segment
// ("data.FILESYSTEM.logbias"). The middleware detail is put in Message rather
// than Data on purpose: the WebSocket transport shows an operator only
// "Invalid params", and reproducing THAT in a unit-test failure would recreate
// the exact diagnosis problem this check exists to prevent. The JSON-RPC code is
// the real one.
func UnsupportedDatasetPropertyError(datasetType, property string) *APIError {
	keyPath := "data." + property
	if datasetType != "" {
		keyPath = "data." + datasetType + "." + property
	}
	return &APIError{
		Code:    -32602,
		Message: fmt.Sprintf("[EINVAL] %s: Extra inputs are not permitted", keyPath),
	}
}

// validateDatasetPayloadKeys reports the first payload key that TrueNAS 26.0
// would reject for datasetType, marshaling the params exactly as the real client
// would so `omitempty` decides what is actually on the wire.
func validateDatasetPayloadKeys(params interface{}, scopes map[string]datasetKeyScope, datasetType string) error {
	encoded, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode dataset params: %w", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return fmt.Errorf("decode dataset params: %w", err)
	}
	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	// Deterministic reporting when a payload carries more than one bad key.
	sort.Strings(keys)
	for _, key := range keys {
		if !datasetKeyAccepted(scopes, key, datasetType) {
			return UnsupportedDatasetPropertyError(datasetType, key)
		}
	}
	return nil
}

// UserPropertyUpdate represents an update to a user property.
type UserPropertyUpdate struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Remove bool   `json:"remove,omitempty"`
}

// DatasetCreate creates a new ZFS dataset.
func (c *Client) DatasetCreate(ctx context.Context, params *DatasetCreateParams) (*Dataset, error) {
	result, err := c.Call(ctx, "pool.dataset.create", params)
	if err != nil {
		// Handle "already exists" errors by returning existing dataset (idempotency)
		if IsAlreadyExistsError(err) {
			return c.DatasetGet(ctx, params.Name)
		}
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	dataset, err := parseDataset(result)
	if err != nil {
		return nil, err
	}
	dataset.CreatedByCall = true
	return dataset, nil
}

// DatasetDelete deletes a ZFS dataset.
func (c *Client) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	options := map[string]interface{}{
		"recursive": recursive,
		"force":     force,
	}

	// TrueNAS 26.0 takes the dataset id as a bare positional string. This is
	// deliberately different from zfs.resource.snapshot.destroy, whose sole
	// argument is an object containing {"path": ...}.
	_, err := c.Call(ctx, "pool.dataset.delete", name, options)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous errors)
		LogAPIError(err, "DatasetDelete error")

		// Handle "not found" errors as success (idempotency)
		if IsNotFoundError(err) {
			return nil
		}
		// TrueNAS collapses BOTH "dataset does not exist" AND "dataset has
		// snapshots" (ENOTEMPTY: "Set recursive=True to remove them") to the
		// same JSON-RPC -32602 "Invalid params" over the WebSocket API
		// (validated live on 26.0). The old blanket "-32602 => nil" swallowed
		// the has-snapshots case, making a non-recursive delete falsely report
		// success and orphan the dataset. Disambiguate by existence: only treat
		// it as done when the dataset is actually gone; otherwise surface the
		// error so callers (DeleteVolume) run their snapshot-dependency
		// handling instead of assuming deletion.
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Code == -32602 {
			if exists, existErr := c.DatasetExists(ctx, name); existErr == nil && !exists {
				return nil
			}
		}
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

// DatasetGet retrieves a dataset by name.
func (c *Client) DatasetGet(ctx context.Context, name string) (*Dataset, error) {
	filters := [][]interface{}{{"id", "=", name}}
	options := map[string]interface{}{
		"extra": map[string]interface{}{
			"properties": datasetQueryProperties,
		},
	}

	result, err := c.Call(ctx, "pool.dataset.query", filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataset: %w", err)
	}

	datasets, ok := result.([]interface{})
	if !ok || len(datasets) == 0 {
		return nil, fmt.Errorf("dataset not found: %s", name)
	}

	return parseDataset(datasets[0])
}

// DatasetGetByNames retrieves multiple datasets by name in a single
// source-bearing pool.dataset.query (["id","in",[names]]) using the exact same
// projection/options as DatasetGet, so user-property SOURCE is preserved (the
// zfs.resource.query listing strips it). It returns a map keyed by dataset name;
// names with no dataset are simply absent. Callers batch many per-name reads
// (e.g. the stale-publication reconcile) into one round trip without losing the
// source information publicationRecordsFromDataset depends on.
func (c *Client) DatasetGetByNames(ctx context.Context, names []string) (map[string]*Dataset, error) {
	result := make(map[string]*Dataset, len(names))
	if len(names) == 0 {
		return result, nil
	}
	filters := [][]interface{}{{"id", "in", names}}
	options := map[string]interface{}{
		"extra": map[string]interface{}{
			"properties": datasetQueryProperties,
		},
	}

	response, err := c.Call(ctx, "pool.dataset.query", filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to get datasets: %w", err)
	}

	items, ok := response.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	for _, item := range items {
		dataset, parseErr := parseDataset(item)
		if parseErr != nil || dataset == nil {
			continue
		}
		result[dataset.Name] = dataset
	}
	return result, nil
}

// DatasetUpdate updates a dataset's properties.
func (c *Client) DatasetUpdate(ctx context.Context, name string, params *DatasetUpdateParams) (*Dataset, error) {
	result, err := c.Call(ctx, "pool.dataset.update", name, params)
	if err != nil {
		return nil, fmt.Errorf("failed to update dataset: %w", err)
	}

	return parseDataset(result)
}

// DatasetList lists CSI-managed datasets below the given parent.
func (c *Client) DatasetList(ctx context.Context, parentName string, limit, offset int) ([]*Dataset, error) {
	filters := make([][]interface{}, 0)
	if parentName != "" {
		filters = append(filters, []interface{}{"name", "^", parentName + "/"})
	}
	filters = append(filters, []interface{}{"user_properties." + datasetManagedResourceProperty + ".value", "=", "true"})

	options := map[string]interface{}{
		"extra": map[string]interface{}{
			"flat":       true,
			"properties": datasetQueryProperties,
		},
	}

	if limit > 0 {
		options["limit"] = limit
	}
	if offset > 0 {
		options["offset"] = offset
	}

	var raw []*rawDataset
	if err := callTyped(ctx, c, &raw, "pool.dataset.query", filters, options); err != nil {
		return nil, fmt.Errorf("failed to list datasets: %w", err)
	}
	return rawDatasetsToDatasets(raw, false), nil
}

// datasetResourceQueryStatus detects the TrueNAS 26.0 dataset resource API
// (zfs.resource.query). It mirrors snapshotResourceQueryStatus: successful and
// method-not-found probes are cached, transient failures are retried on the
// next call, and concurrent callers share one probe.
func (c *Client) datasetResourceQueryStatus(ctx context.Context) (available, detected bool) {
	c.datasetResourceMu.Lock()
	if c.datasetResourceDetected {
		cachedAvailable := c.datasetResourceAvailable
		c.datasetResourceMu.Unlock()
		return cachedAvailable, true
	}
	if probeDone := c.datasetResourceProbeDone; probeDone != nil {
		c.datasetResourceMu.Unlock()
		select {
		case <-probeDone:
			c.datasetResourceMu.Lock()
			cachedDetected := c.datasetResourceDetected
			cachedAvailable := cachedDetected && c.datasetResourceAvailable
			c.datasetResourceMu.Unlock()
			return cachedAvailable, cachedDetected
		case <-ctx.Done():
			return false, false
		}
	}

	probeDone := make(chan struct{})
	c.datasetResourceProbeDone = probeDone
	c.datasetResourceMu.Unlock()

	_, err := c.Call(ctx, datasetResourceQueryMethod, datasetResourceQueryOptions(nil, false))
	detected = err == nil || isMethodNotFoundError(err)
	available = err == nil

	c.datasetResourceMu.Lock()
	if detected && !c.datasetResourceDetected {
		c.datasetResourceDetected = true
		c.datasetResourceAvailable = available
	}
	c.datasetResourceProbeDone = nil
	close(probeDone)
	detected = c.datasetResourceDetected
	available = detected && c.datasetResourceAvailable
	c.datasetResourceMu.Unlock()

	if available {
		klog.V(2).Infof("Detected TrueNAS 26.0 dataset resource API")
	} else if err != nil && !detected {
		klog.Warningf("Could not detect dataset resource API; managed-dataset listing will retry through pool.dataset.query: %v", err)
	}
	return available, detected
}

func (c *Client) hasDatasetResourceQuery(ctx context.Context) bool {
	available, _ := c.datasetResourceQueryStatus(ctx)
	return available
}

// datasetResourceQueryOptions builds the zfs.resource.query options object.
// get_children recurses below the given paths; get_user_properties and
// get_source surface user-property values with their source so the source=='local'
// discipline survives the read.
func datasetResourceQueryOptions(paths []string, getChildren bool) map[string]interface{} {
	if paths == nil {
		paths = []string{}
	}
	return map[string]interface{}{
		"paths":               paths,
		"get_children":        getChildren,
		"properties":          datasetQueryProperties,
		"get_user_properties": true,
		"get_source":          true,
	}
}

// LIVE-PROBE GATE: The request options and response shape for the dataset
// zfs.resource.query read below are MODELED on the snapshot resource API
// (zfs.resource.snapshot.query) and the architecture review — they are NOT yet
// confirmed by a live TrueNAS 26.0 probe. The safe fallback to pool.dataset.query
// in listAllManagedDatasets protects reconciliation if this shape is rejected or
// parses empty. A live probe against TrueNAS 26.0 MUST confirm the shape (field
// names, user_properties source presence, get_children/get_source behavior) before
// this path is relied upon in production.
//
// DatasetQueryByParent returns every dataset below parentDataset (path-scoped,
// never a full-system scan) using the TrueNAS 26.0 zfs.resource.query API. It
// returns ALL datasets under the parent; callers filter for the managed_resource
// user property. Detection gates the call: if the resource API is not detected
// available, it returns an error so callers fall back to pool.dataset.query.
func (c *Client) DatasetQueryByParent(ctx context.Context, parentDataset string) ([]*Dataset, error) {
	if !c.hasDatasetResourceQuery(ctx) {
		return nil, fmt.Errorf("dataset resource API (zfs.resource.query) is not available")
	}
	parent := strings.TrimSuffix(parentDataset, "/")
	var raw []*rawDataset
	if err := callTyped(ctx, c, &raw, datasetResourceQueryMethod, datasetResourceQueryOptions([]string{parent}, true)); err != nil {
		return nil, fmt.Errorf("failed to query datasets by parent: %w", err)
	}
	return rawDatasetsToDatasets(raw, true), nil
}

// LIVE-PROBE GATE: parseDatasetResource parses the MODELED (not live-verified)
// zfs.resource.query dataset item shape. See the gate comment on
// DatasetQueryByParent: a live TrueNAS 26.0 probe must confirm field names,
// user_properties source presence, and get_children/get_source behavior before
// this parser is relied upon in production. The parser is deliberately defensive:
// it reuses parseDataset/parseProperty where the shape matches and degrades flat
// user-property values to source="" (unknown) so datasetHasLocalUserProperty
// callers fail safe rather than misreporting local.
func parseDatasetResource(data interface{}) (*Dataset, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected dataset resource format")
	}

	ds := &Dataset{
		UserProperties: make(map[string]UserProperty),
	}

	// The resource API may key the dataset by "name" (the dataset path) and/or
	// "path"; pool.dataset.query uses "id"/"name". Accept both.
	if v, ok := m["name"].(string); ok {
		ds.Name = v
	}
	if v, ok := m["path"].(string); ok && ds.Name == "" {
		ds.Name = v
	}
	if v, ok := m["id"].(string); ok {
		ds.ID = v
	}
	if ds.ID == "" {
		ds.ID = ds.Name
	}
	if v, ok := m["pool"].(string); ok {
		ds.Pool = v
	}
	if v, ok := m["type"].(string); ok {
		ds.Type = v
	}
	if v, ok := m["mountpoint"].(string); ok {
		ds.Mountpoint = v
	}

	// The resource API nests ZFS properties under a "properties" map keyed by
	// property name, each entry shaped like parseProperty's input
	// ({value, rawvalue, parsed, source}). Fall back to top-level keys when a
	// "properties" map is absent so a shape closer to pool.dataset.query still
	// parses.
	props := m
	if nested, ok := m["properties"].(map[string]interface{}); ok {
		props = nested
	}
	ds.Used = parseProperty(props["used"])
	ds.Available = parseProperty(props["available"])
	ds.Quota = parseProperty(props["quota"])
	ds.Refquota = parseProperty(props["refquota"])
	ds.Referenced = parseProperty(props["referenced"])
	ds.Usedbysnapshots = parseProperty(props["usedbysnapshots"])
	ds.Reservation = parseProperty(props["reservation"])
	ds.Refreservation = parseProperty(props["refreservation"])
	ds.Volsize = parseProperty(props["volsize"])
	ds.Volblocksize = parseProperty(props["volblocksize"])
	ds.Origin = parseProperty(props["origin"])
	ds.Creation = parseProperty(props["creation"])

	// User properties: prefer the nested {value, source} shape (source must
	// survive so the source=='local' discipline holds). Defensively also accept
	// a flat key -> "value" string form (as the snapshot resource API flattens);
	// flat values get source="" (unknown) so datasetHasLocalUserProperty degrades
	// safely rather than misreporting local.
	if userProps, ok := m["user_properties"].(map[string]interface{}); ok {
		for key, val := range userProps {
			switch typed := val.(type) {
			case map[string]interface{}:
				prop := UserProperty{}
				if v, ok := typed["value"].(string); ok {
					prop.Value = v
				}
				if v, ok := typed["source"].(string); ok {
					prop.Source = v
				}
				ds.UserProperties[key] = prop
			case string:
				ds.UserProperties[key] = UserProperty{Value: typed}
			}
		}
	}

	return ds, nil
}

// DatasetHasDependentClones reports whether any dataset in the same CSI parent was
// cloned from a snapshot of datasetName. Snapshot clone projections are absent
// on TrueNAS 26.0, but pool.dataset.query still exposes the origin property.
func (c *Client) DatasetHasDependentClones(ctx context.Context, datasetName string) (bool, error) {
	parent := path.Dir(datasetName)
	if parent == "." || parent == "/" {
		return false, fmt.Errorf("invalid dataset name %q", datasetName)
	}
	origins, err := c.queryDatasetOrigins(ctx, parent)
	if err != nil {
		return false, err
	}
	originPrefix := datasetName + "@"
	for _, origin := range origins {
		if strings.HasPrefix(origin, originPrefix) {
			return true, nil
		}
	}
	return false, nil
}

// DatasetPromote promotes a clone so it no longer depends on its origin
// snapshot (GF2/E3). pool.dataset.promote takes a single dataset-name string,
// returns null, and works on both filesystems and zvols (P3). Promote MOVES the
// origin snapshot (and every snapshot older-or-equal) onto the promoted clone
// and re-parents the source + sibling clones onto it — so callers must gate on
// sole-dependency before promoting (R3). Promoting an already-independent
// dataset (empty origin) is a no-op the caller gates on; the backend error from
// a redundant promote is surfaced for the caller to treat as benign.
func (c *Client) DatasetPromote(ctx context.Context, datasetName string) error {
	if _, err := c.Call(ctx, "pool.dataset.promote", datasetName); err != nil {
		return fmt.Errorf("failed to promote dataset %s: %w", datasetName, err)
	}
	return nil
}

// DatasetQuotaUsage reports a dataset's space accounting for quota/usage
// reporting (GF2/E4). Quota, Refquota and Volsize are 0 when unset (no limit).
//
// It carries BOTH usage numerators on purpose (GF2-fix4/H1). ZFS bounds
// `referenced` with `refquota` and `used` with `quota`, and the difference
// between the two numerators is exactly the space a volume's snapshots hold —
// which the GF2 scheduled-snapshot feature is designed to accumulate. A consumer
// must therefore pick the numerator that belongs to the limit it evaluates;
// mixing them reports a perfectly healthy volume as near-quota forever.
//
// Type is the dataset type ("FILESYSTEM" or "VOLUME"). A zvol carries neither
// quota nor refquota — `volsize` IS its capacity — so block volumes are covered
// through Volsize rather than being silently skipped (GF2-fix4/M1).
type DatasetQuotaUsage struct {
	Type            string
	Used            int64
	Referenced      int64
	UsedBySnapshots int64
	Quota           int64
	Refquota        int64
	Volsize         int64
	Available       int64
}

// DatasetGetQuotaUsage returns a dataset's space accounting.
// It reuses the dataset query path (which already projects these properties) so
// it is one pool.dataset.query, not a separate endpoint (P-usage).
func (c *Client) DatasetGetQuotaUsage(ctx context.Context, datasetName string) (*DatasetQuotaUsage, error) {
	ds, err := c.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, err
	}
	return ds.QuotaUsage(), nil
}

// QuotaUsage projects an ALREADY-FETCHED dataset's space accounting, with no
// additional API call. It is what lets the reconcile dataset walk publish the
// per-volume usage gauges for every managed volume (GF2-fix/F6) — the shipped
// health-monitor sidecar drives ListVolumes, not ControllerGetVolume, so gauges
// populated only from ControllerGetVolume left the near-quota alert unable to
// fire in the shipped topology.
func (d *Dataset) QuotaUsage() *DatasetQuotaUsage {
	if d == nil {
		return nil
	}
	return &DatasetQuotaUsage{
		Type:            d.Type,
		Used:            datasetPropertyInt64(d.Used),
		Referenced:      datasetPropertyInt64(d.Referenced),
		UsedBySnapshots: datasetPropertyInt64(d.Usedbysnapshots),
		Quota:           datasetPropertyInt64(d.Quota),
		Refquota:        datasetPropertyInt64(d.Refquota),
		Volsize:         datasetPropertyInt64(d.Volsize),
		Available:       datasetPropertyInt64(d.Available),
	}
}

// datasetPropertyInt64 extracts a non-negative int64 from a dataset property's
// parsed value, returning 0 when absent or out of range (e.g. an unset quota).
func datasetPropertyInt64(property DatasetProperty) int64 {
	if value, ok := property.Parsed.(float64); ok {
		if result, valid := nonNegativeInt64FromFloat(value); valid {
			return result
		}
	}
	return 0
}

// SnapshotDependentClones is the exported, authoritative dependent-clone query
// for ONE exact snapshot. It answers "which datasets are clones of this
// snapshot" from the backend's own origin projection rather than from any
// caller-side slice, so a clone the caller's managed-dataset listing never saw
// (an admin's `zfs clone`, a replication/VolSync target, another driver
// instance's volume) is still counted (GF2-fix/H3).
//
// Scope note, documented honestly: TrueNAS 26.0 exposes no `clones` property on
// snapshots, so the only available authority is a dataset-origin query. It
// covers the whole CSI PARENT subtree — every clone that can exist under the
// driver's parent, managed or not. A clone living OUTSIDE the parent subtree is
// invisible to every 26.0 API, so callers that mutate dependency structure
// (promote) must treat that as a residual documented risk, not proven absence.
func (c *Client) SnapshotDependentClones(ctx context.Context, snapshotID string) ([]string, error) {
	return c.snapshotDependentClones(ctx, snapshotID)
}

// snapshotDependentClones returns the datasets cloned from one exact snapshot —
// the authoritative dependent-clone check on TrueNAS 26.0, where the snapshot
// query APIs no longer expose the ZFS clones property.
func (c *Client) snapshotDependentClones(ctx context.Context, snapshotID string) ([]string, error) {
	datasetName, _, found := strings.Cut(snapshotID, "@")
	parent := path.Dir(datasetName)
	if !found || parent == "." || parent == "/" {
		return nil, fmt.Errorf("invalid snapshot id %q", snapshotID)
	}
	origins, err := c.queryDatasetOrigins(ctx, parent)
	if err != nil {
		return nil, err
	}
	var clones []string
	for name, origin := range origins {
		if origin == snapshotID {
			clones = append(clones, name)
		}
	}
	return clones, nil
}

// queryDatasetOrigins returns dataset name → origin (empty for non-clones) for
// datasets below the configured CSI parent, using the projected origin property.
func (c *Client) queryDatasetOrigins(ctx context.Context, parent string) (map[string]string, error) {
	filters := [][]interface{}{{"name", "^", strings.TrimSuffix(parent, "/") + "/"}}
	options := map[string]interface{}{
		"extra": map[string]interface{}{
			"properties": []string{"origin"},
		},
	}
	var raw []*rawDataset
	if err := callTyped(ctx, c, &raw, "pool.dataset.query", filters, options); err != nil {
		return nil, fmt.Errorf("failed to query dataset origins: %w", err)
	}
	origins := make(map[string]string, len(raw))
	for _, item := range raw {
		dataset := item.toDataset(false)
		origins[dataset.Name] = datasetPropertyString(dataset.Origin)
	}
	return origins, nil
}

func datasetPropertyString(property DatasetProperty) string {
	// Prefer parsed/rawvalue: TrueNAS 26.0 UPPERCASES the display-oriented
	// "value" field for some properties (observed live on origin:
	// value=FLASHSTOR/...@SNAPSHOT-... while parsed/rawvalue keep true case),
	// which breaks identity comparisons against real ZFS names.
	if parsed, ok := property.Parsed.(string); ok && parsed != "" {
		return parsed
	}
	if property.Rawvalue != "" {
		return property.Rawvalue
	}
	if value, ok := property.Value.(string); ok && value != "" {
		return value
	}
	return ""
}

// DatasetSetUserProperty sets a user property on a dataset.
func (c *Client) DatasetSetUserProperty(ctx context.Context, name, key, value string) error {
	return c.DatasetSetUserProperties(ctx, name, map[string]string{key: value})
}

// DatasetSetUserProperties sets multiple user properties on a dataset in one
// update. Live TrueNAS 26.0 probes confirm pool.dataset.update's
// user_properties_update persists these values with source=local, unlike the
// silently dropped inline pool.dataset.create shape.
func (c *Client) DatasetSetUserProperties(ctx context.Context, name string, properties map[string]string) error {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	updates := make([]UserPropertyUpdate, 0, len(keys))
	for _, key := range keys {
		updates = append(updates, UserPropertyUpdate{Key: key, Value: properties[key]})
	}

	params := &DatasetUpdateParams{
		UserPropertiesUpdate: updates,
	}

	_, err := c.DatasetUpdate(ctx, name, params)
	return err
}

// DatasetRemoveUserProperties removes local user properties in one update.
// Publication records use the live-verified {key, remove:true} update rather
// than an empty value so a retry can distinguish "never published" from an
// interrupted unpublish tombstone.
func (c *Client) DatasetRemoveUserProperties(ctx context.Context, name string, keys []string) error {
	keys = append([]string(nil), keys...)
	sort.Strings(keys)
	updates := make([]UserPropertyUpdate, 0, len(keys))
	for _, key := range keys {
		if key != "" {
			updates = append(updates, UserPropertyUpdate{Key: key, Remove: true})
		}
	}
	if len(updates) == 0 {
		return nil
	}
	_, err := c.DatasetUpdate(ctx, name, &DatasetUpdateParams{UserPropertiesUpdate: updates})
	return err
}

// DatasetGetUserProperty gets a user property from a dataset.
func (c *Client) DatasetGetUserProperty(ctx context.Context, name, key string) (string, error) {
	ds, err := c.DatasetGet(ctx, name)
	if err != nil {
		return "", err
	}

	if prop, ok := ds.UserProperties[key]; ok {
		return prop.Value, nil
	}

	return "", nil
}

// DatasetExpand expands a zvol to the specified size.
func (c *Client) DatasetExpand(ctx context.Context, name string, newSize int64) error {
	params := &DatasetUpdateParams{
		Volsize: newSize,
	}

	_, err := c.DatasetUpdate(ctx, name, params)
	return err
}

// GetPoolAvailable returns the available space in a pool.
func (c *Client) GetPoolAvailable(ctx context.Context, poolName string) (int64, error) {
	// Extract pool name from dataset path
	parts := strings.Split(poolName, "/")
	pool := parts[0]

	result, err := c.Call(ctx, "pool.query", [][]interface{}{{"name", "=", pool}}, map[string]interface{}{})
	if err != nil {
		return 0, fmt.Errorf("failed to query pool: %w", err)
	}

	pools, ok := result.([]interface{})
	if !ok || len(pools) == 0 {
		return 0, fmt.Errorf("pool not found: %s", pool)
	}

	poolData, ok := pools[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected pool data format")
	}

	// Get the topology.data free space
	if topology, ok := poolData["topology"].(map[string]interface{}); ok {
		if data, ok := topology["data"].([]interface{}); ok && len(data) > 0 {
			// Sum up available space from all vdevs
			var totalAvail int64
			for _, vdev := range data {
				if vdevMap, ok := vdev.(map[string]interface{}); ok {
					if stats, ok := vdevMap["stats"].(map[string]interface{}); ok {
						if free, ok := stats["free"].(float64); ok {
							freeBytes, valid := nonNegativeInt64FromFloat(free)
							if !valid || freeBytes > math.MaxInt64-totalAvail {
								return 0, fmt.Errorf("pool available space is outside the int64 range")
							}
							totalAvail += freeBytes
						}
					}
				}
			}
			return totalAvail, nil
		}
	}

	// Fallback: use dataset query on pool root
	ds, err := c.DatasetGet(ctx, pool)
	if err != nil {
		return 0, err
	}

	if avail, ok := ds.Available.Parsed.(float64); ok {
		if availableBytes, valid := nonNegativeInt64FromFloat(avail); valid {
			return availableBytes, nil
		}
		return 0, fmt.Errorf("pool available space is outside the int64 range")
	}

	return 0, fmt.Errorf("unable to determine pool available space")
}

// parseDataset converts a raw API response to a Dataset struct.
func parseDataset(data interface{}) (*Dataset, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected dataset format")
	}

	ds := &Dataset{
		UserProperties: make(map[string]UserProperty),
	}

	if v, ok := m["id"].(string); ok {
		ds.ID = v
	}
	if v, ok := m["name"].(string); ok {
		ds.Name = v
	}
	if v, ok := m["pool"].(string); ok {
		ds.Pool = v
	}
	if v, ok := m["type"].(string); ok {
		ds.Type = v
	}
	if v, ok := m["mountpoint"].(string); ok {
		ds.Mountpoint = v
	}

	// Parse properties
	ds.Used = parseProperty(m["used"])
	ds.Available = parseProperty(m["available"])
	ds.Quota = parseProperty(m["quota"])
	ds.Refquota = parseProperty(m["refquota"])
	ds.Referenced = parseProperty(m["referenced"])
	ds.Usedbysnapshots = parseProperty(m["usedbysnapshots"])
	ds.Reservation = parseProperty(m["reservation"])
	ds.Refreservation = parseProperty(m["refreservation"])
	ds.Volsize = parseProperty(m["volsize"])
	ds.Volblocksize = parseProperty(m["volblocksize"])
	ds.Origin = parseProperty(m["origin"])
	ds.Creation = parseProperty(m["creation"])

	// Parse user properties
	if userProps, ok := m["user_properties"].(map[string]interface{}); ok {
		for key, val := range userProps {
			if propMap, ok := val.(map[string]interface{}); ok {
				prop := UserProperty{}
				if v, ok := propMap["value"].(string); ok {
					prop.Value = v
				}
				if v, ok := propMap["source"].(string); ok {
					prop.Source = v
				}
				ds.UserProperties[key] = prop
			}
		}
	}

	return ds, nil
}

// parseProperty converts a raw property to DatasetProperty.
func parseProperty(data interface{}) DatasetProperty {
	prop := DatasetProperty{}
	if data == nil {
		return prop
	}

	if m, ok := data.(map[string]interface{}); ok {
		prop.Value = m["value"]
		if v, ok := m["rawvalue"].(string); ok {
			prop.Rawvalue = v
		}
		prop.Parsed = m["parsed"]
		if v, ok := m["source"].(string); ok {
			prop.Source = v
		}
	}

	return prop
}

// GetCreationTime returns the dataset creation timestamp in Unix seconds.
func (ds *Dataset) GetCreationTime() int64 {
	if ds == nil {
		return 0
	}
	if parsedMap, ok := ds.Creation.Parsed.(map[string]interface{}); ok {
		if dateMs, ok := parsedMap["$date"].(float64); ok {
			if timestamp, valid := nonNegativeInt64FromFloat(dateMs / 1000); valid {
				return timestamp
			}
		}
	}
	for _, value := range []interface{}{ds.Creation.Parsed, ds.Creation.Value, ds.Creation.Rawvalue} {
		switch typed := value.(type) {
		case float64:
			if timestamp, valid := nonNegativeInt64FromFloat(typed); valid {
				return timestamp
			}
		case int64:
			if typed >= 0 {
				return typed
			}
		case string:
			if timestamp, err := strconv.ParseInt(typed, 10, 64); err == nil && timestamp >= 0 {
				return timestamp
			}
		}
	}
	return 0
}

// GetUsedBytes returns the dataset's reported used space in bytes.
func (ds *Dataset) GetUsedBytes() int64 {
	if ds == nil {
		return 0
	}
	for _, value := range []interface{}{ds.Used.Parsed, ds.Used.Value, ds.Used.Rawvalue} {
		switch typed := value.(type) {
		case float64:
			if used, valid := nonNegativeInt64FromFloat(typed); valid {
				return used
			}
		case int64:
			if typed >= 0 {
				return typed
			}
		case string:
			if used, err := strconv.ParseInt(typed, 10, 64); err == nil && used >= 0 {
				return used
			}
		}
	}
	return 0
}

// WaitForDatasetReady waits for a dataset to be available and queryable.
// This is important after clone operations where the dataset may not be
// immediately available for subsequent operations.
func (c *Client) WaitForDatasetReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error) {
	start := time.Now()
	// Sprint 3 (L1a): the first re-poll starts at 50ms (was 100ms) so the common
	// fast-ready clone pays less than one 100ms tick; doubling still caps at 2s
	// (50,100,200,400,800,1600,2000...).
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 2 * time.Second

	klog.V(4).Infof("Waiting for dataset %s to be ready (timeout: %v)", name, timeout)

	var lastErr error
	for {
		ds, err := c.DatasetGet(ctx, name)
		if err == nil && ds != nil {
			klog.V(4).Infof("Dataset %s is ready (took %v)", name, time.Since(start))
			return ds, nil
		}
		lastErr = err

		if time.Since(start) > timeout {
			return nil, fmt.Errorf("timeout waiting for dataset %s to be ready after %v: %w", name, timeout, lastErr)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled waiting for dataset: %w", ctx.Err())
		case <-time.After(pollInterval):
		}

		// Exponential backoff
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

// WaitForZvolReady waits for a zvol to be ready with a valid volsize.
// After cloning, the zvol may not immediately have all properties available.
func (c *Client) WaitForZvolReady(ctx context.Context, name string, timeout time.Duration) (*Dataset, error) {
	start := time.Now()
	// Sprint 3 (L1a): the first re-poll starts at 50ms (was 100ms) so the common
	// fast-ready clone pays less than one 100ms tick; doubling still caps at 2s
	// (50,100,200,400,800,1600,2000...).
	pollInterval := 50 * time.Millisecond
	maxPollInterval := 2 * time.Second

	klog.V(4).Infof("Waiting for zvol %s to be ready (timeout: %v)", name, timeout)

	var lastErr error
	for {
		ds, err := c.DatasetGet(ctx, name)
		if err == nil && ds != nil {
			// Verify it's a VOLUME type and has a valid volsize
			if ds.Type == "VOLUME" {
				if volsize, ok := ds.Volsize.Parsed.(float64); ok && volsize > 0 {
					klog.V(4).Infof("Zvol %s is ready with volsize %d (took %v)", name, int64(volsize), time.Since(start))
					return ds, nil
				}
				klog.V(4).Infof("Zvol %s exists but volsize not ready yet", name)
			} else {
				// It's a filesystem, which is also valid for NFS
				klog.V(4).Infof("Dataset %s is ready as type %s (took %v)", name, ds.Type, time.Since(start))
				return ds, nil
			}
		}
		if err != nil {
			lastErr = err
		}

		if time.Since(start) > timeout {
			if lastErr != nil {
				return nil, fmt.Errorf("timeout waiting for zvol %s to be ready after %v: %w", name, timeout, lastErr)
			}
			return nil, fmt.Errorf("timeout waiting for zvol %s to be ready after %v", name, timeout)
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled waiting for zvol: %w", ctx.Err())
		case <-time.After(pollInterval):
		}

		// Exponential backoff
		pollInterval *= 2
		if pollInterval > maxPollInterval {
			pollInterval = maxPollInterval
		}
	}
}

// DatasetExists checks if a dataset exists without returning an error for not found.
func (c *Client) DatasetExists(ctx context.Context, name string) (bool, error) {
	_, err := c.DatasetGet(ctx, name)
	if err != nil {
		if IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
