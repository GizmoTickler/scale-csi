package truenas

import (
	"context"
	"fmt"
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

// DatasetCreateParams holds parameters for creating a dataset.
type DatasetCreateParams struct {
	Name            string `json:"name"`
	Type            string `json:"type,omitempty"`         // FILESYSTEM or VOLUME
	Volsize         int64  `json:"volsize,omitempty"`      // For volumes
	Volblocksize    string `json:"volblocksize,omitempty"` // For volumes
	Sparse          bool   `json:"sparse,omitempty"`       // For volumes
	Quota           int64  `json:"quota,omitempty"`        // For filesystems
	Refquota        int64  `json:"refquota,omitempty"`     // For filesystems
	Reservation     int64  `json:"reservation,omitempty"`
	Refreservation  int64  `json:"refreservation,omitempty"`
	Comments        string `json:"comments,omitempty"`
	Readonly        string `json:"readonly,omitempty"` // ON, OFF, INHERIT
	Atime           string `json:"atime,omitempty"`
	Exec            string `json:"exec,omitempty"`
	Sync            string `json:"sync,omitempty"`
	Compression     string `json:"compression,omitempty"`
	Deduplication   string `json:"deduplication,omitempty"`
	Copies          int    `json:"copies,omitempty"`
	Recordsize      string `json:"recordsize,omitempty"`
	Casesensitivity string `json:"casesensitivity,omitempty"`
	Aclmode         string `json:"aclmode,omitempty"`
	Acltype         string `json:"acltype,omitempty"`
	ShareType       string `json:"share_type,omitempty"`
	Xattr           string `json:"xattr,omitempty"`
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
func (c *Client) DatasetDelete(ctx context.Context, name string, recursive bool, force bool) error {
	options := map[string]interface{}{
		"recursive": recursive,
		"force":     force,
	}

	_, err := c.Call(ctx, "pool.dataset.delete", name, options)
	if err != nil {
		// Handle "not found" errors as success (idempotency)
		if IsNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("failed to delete dataset: %w", err)
	}

	return nil
}

// DatasetGet retrieves a dataset by name.
func (c *Client) DatasetGet(ctx context.Context, name string) (*Dataset, error) {
	filters := [][]interface{}{{"id", "=", name}}

	result, err := c.Call(ctx, "pool.dataset.query", filters, map[string]interface{}{})
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

// DatasetList lists datasets matching the given filters.
func (c *Client) DatasetList(ctx context.Context, parentName string, limit int, offset int) ([]*Dataset, error) {
	filters := make([][]interface{}, 0)
	if parentName != "" {
		filters = append(filters, []interface{}{"id", "^", parentName + "/"})
	}

	options := map[string]interface{}{
		"extra": map[string]interface{}{
			"flat": true,
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

// DatasetSetUserProperty sets a user property on a dataset.
func (c *Client) DatasetSetUserProperty(ctx context.Context, name string, key string, value string) error {
	params := &DatasetUpdateParams{
		UserPropertiesUpdate: []UserPropertyUpdate{
			{Key: key, Value: value},
		},
	}

	_, err := c.DatasetUpdate(ctx, name, params)
	return err
}

// DatasetGetUserProperty gets a user property from a dataset.
func (c *Client) DatasetGetUserProperty(ctx context.Context, name string, key string) (string, error) {
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
							totalAvail += int64(free)
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
		return int64(avail), nil
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
			return nil, fmt.Errorf("context cancelled waiting for dataset: %w", ctx.Err())
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
			return nil, fmt.Errorf("context cancelled waiting for zvol: %w", ctx.Err())
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
		if IsNotFoundError(err) || strings.Contains(err.Error(), "not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
