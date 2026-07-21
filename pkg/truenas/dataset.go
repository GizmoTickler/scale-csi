package truenas

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// Dataset represents a ZFS dataset from the TrueNAS API.
type Dataset struct {
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	Pool           string                  `json:"pool"`
	Type           string                  `json:"type"`
	Mountpoint     string                  `json:"mountpoint"`
	Used           DatasetProperty         `json:"used"`
	Available      DatasetProperty         `json:"available"`
	Quota          DatasetProperty         `json:"quota"`
	Refquota       DatasetProperty         `json:"refquota"`
	Reservation    DatasetProperty         `json:"reservation"`
	Refreservation DatasetProperty         `json:"refreservation"`
	Volsize        DatasetProperty         `json:"volsize"`
	Volblocksize   DatasetProperty         `json:"volblocksize"`
	Origin         DatasetProperty         `json:"origin"`
	Creation       DatasetProperty         `json:"creation"`
	UserProperties map[string]UserProperty `json:"user_properties"`
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
	"reservation",
	"refreservation",
	"volsize",
	"volblocksize",
	"creation",
}

// DatasetCreateParams holds parameters for creating a dataset.
type DatasetCreateParams struct {
	Name            string               `json:"name"`
	Type            string               `json:"type,omitempty"`         // FILESYSTEM or VOLUME
	Volsize         int64                `json:"volsize,omitempty"`      // For volumes
	Volblocksize    string               `json:"volblocksize,omitempty"` // For volumes
	Sparse          bool                 `json:"sparse,omitempty"`       // For volumes
	Quota           int64                `json:"quota,omitempty"`        // For filesystems
	Refquota        int64                `json:"refquota,omitempty"`     // For filesystems
	Reservation     int64                `json:"reservation,omitempty"`
	Refreservation  int64                `json:"refreservation,omitempty"`
	Comments        string               `json:"comments,omitempty"`
	Readonly        string               `json:"readonly,omitempty"` // ON, OFF, INHERIT
	Atime           string               `json:"atime,omitempty"`
	Exec            string               `json:"exec,omitempty"`
	Sync            string               `json:"sync,omitempty"`
	Compression     string               `json:"compression,omitempty"`
	Deduplication   string               `json:"deduplication,omitempty"`
	Logbias         string               `json:"logbias,omitempty"`
	Primarycache    string               `json:"primarycache,omitempty"`
	Copies          int                  `json:"copies,omitempty"`
	Recordsize      string               `json:"recordsize,omitempty"`
	Casesensitivity string               `json:"casesensitivity,omitempty"`
	Aclmode         string               `json:"aclmode,omitempty"`
	Acltype         string               `json:"acltype,omitempty"`
	ShareType       string               `json:"share_type,omitempty"`
	Xattr           string               `json:"xattr,omitempty"`
	UserProperties  []UserPropertyUpdate `json:"user_properties,omitempty"`
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

	return parseDataset(result)
}

// DatasetDelete deletes a ZFS dataset.
func (c *Client) DatasetDelete(ctx context.Context, name string, recursive, force bool) error {
	options := map[string]interface{}{
		"recursive": recursive,
		"force":     force,
	}

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

	result, err := c.Call(ctx, "pool.dataset.query", filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to list datasets: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	datasets := make([]*Dataset, 0, len(items))
	for _, item := range items {
		ds, err := parseDataset(item)
		if err != nil {
			continue
		}
		datasets = append(datasets, ds)
	}

	return datasets, nil
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
	result, err := c.Call(ctx, "pool.dataset.query", filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query dataset origins: %w", err)
	}
	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	origins := make(map[string]string, len(items))
	for _, item := range items {
		dataset, parseErr := parseDataset(item)
		if parseErr != nil {
			continue
		}
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

// DatasetSetUserProperties sets multiple user properties on a dataset in one update.
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
// Publication records use removal rather than an empty value so a retry can
// distinguish "never published" from an interrupted unpublish tombstone.
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
	pollInterval := 100 * time.Millisecond
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
	pollInterval := 100 * time.Millisecond
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
