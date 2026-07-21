package truenas

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const snapshotResourceQueryMethod = "zfs.resource.snapshot.query"

// hasSnapshotResourceQuery detects the TrueNAS 26.0 snapshot resource API.
// Successful and method-not-found probes are cached; transient failures are
// deliberately retried on the next read. Concurrent callers share one probe.
func (c *Client) hasSnapshotResourceQuery(ctx context.Context) bool {
	c.snapshotResourceMu.Lock()
	if c.snapshotResourceDetected {
		available := c.snapshotResourceAvailable
		c.snapshotResourceMu.Unlock()
		return available
	}
	if probeDone := c.snapshotResourceProbeDone; probeDone != nil {
		c.snapshotResourceMu.Unlock()
		select {
		case <-probeDone:
			c.snapshotResourceMu.Lock()
			available := c.snapshotResourceDetected && c.snapshotResourceAvailable
			c.snapshotResourceMu.Unlock()
			return available
		case <-ctx.Done():
			return false
		}
	}

	probeDone := make(chan struct{})
	c.snapshotResourceProbeDone = probeDone
	c.snapshotResourceMu.Unlock()

	_, err := c.Call(ctx, snapshotResourceQueryMethod, snapshotResourceQueryOptions(nil, false, nil))
	detected := err == nil || isMethodNotFoundError(err)
	available := err == nil

	c.snapshotResourceMu.Lock()
	if detected && !c.snapshotResourceDetected {
		c.snapshotResourceDetected = true
		c.snapshotResourceAvailable = available
	}
	c.snapshotResourceProbeDone = nil
	close(probeDone)
	available = c.snapshotResourceDetected && c.snapshotResourceAvailable
	c.snapshotResourceMu.Unlock()

	if available {
		klog.V(2).Infof("Detected TrueNAS 26.0 snapshot resource API")
	} else if err != nil && !detected {
		klog.Warningf("Could not detect snapshot resource API, using legacy snapshot reads: %v", err)
	}
	return available
}

func isMethodNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.Code == -32601 {
			return true
		}
		// -1 is TrueNAS's unstructured application-error bucket; known JSON-RPC
		// codes are authoritative and must not be overridden by message text.
		if apiErr.Code != -1 {
			return false
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "method not found") || strings.Contains(message, "method does not exist")
}

func snapshotResourceQueryOptions(paths []string, recursive bool, properties []string) map[string]interface{} {
	if paths == nil {
		paths = []string{}
	}
	return map[string]interface{}{
		"paths":               paths,
		"recursive":           recursive,
		"properties":          properties,
		"get_user_properties": true,
	}
}

// ErrSnapshotHasClones is returned when a snapshot cannot be deleted because it has dependent clones.
// The caller should inspect the Clones field to determine how to proceed.
type ErrSnapshotHasClones struct {
	SnapshotID string
	Clones     []string
}

func (e *ErrSnapshotHasClones) Error() string {
	return fmt.Sprintf("snapshot %s has dependent clones: %v", e.SnapshotID, e.Clones)
}

// detectSnapshotAPIPrefix detects which API prefix to use for snapshot methods.
// This provides compatibility between TrueNAS 24.x (zfs.snapshot.*) and 25.04+ (pool.snapshot.*).
func (c *Client) detectSnapshotAPIPrefix(ctx context.Context) string {
	c.snapshotPrefixMu.Lock()
	if c.snapshotAPIPrefix != "" {
		prefix := c.snapshotAPIPrefix
		c.snapshotPrefixMu.Unlock()
		return prefix
	}
	if probeDone := c.snapshotPrefixProbeDone; probeDone != nil {
		c.snapshotPrefixMu.Unlock()
		select {
		case <-probeDone:
			c.snapshotPrefixMu.Lock()
			prefix := c.snapshotAPIPrefix
			c.snapshotPrefixMu.Unlock()
			if prefix != "" {
				return prefix
			}
		case <-ctx.Done():
		}
		return "zfs.snapshot"
	}
	probeDone := make(chan struct{})
	c.snapshotPrefixProbeDone = probeDone
	c.snapshotPrefixMu.Unlock()

	detectedPrefix := ""

	// Try pool.snapshot.query first (TrueNAS 25.04+)
	_, err := c.Call(ctx, "pool.snapshot.query", [][]interface{}{}, map[string]interface{}{"limit": 1})
	if err == nil {
		detectedPrefix = "pool.snapshot"
		klog.V(2).Infof("Detected TrueNAS 25.04+ API (pool.snapshot.*)")
	} else {
		// Fall back to zfs.snapshot.query (TrueNAS 24.x)
		_, err = c.Call(ctx, "zfs.snapshot.query", [][]interface{}{}, map[string]interface{}{"limit": 1})
		if err == nil {
			detectedPrefix = "zfs.snapshot"
			klog.V(2).Infof("Detected TrueNAS 24.x API (zfs.snapshot.*)")
		}
	}

	c.snapshotPrefixMu.Lock()
	if detectedPrefix != "" && c.snapshotAPIPrefix == "" {
		c.snapshotAPIPrefix = detectedPrefix
	}
	prefix := c.snapshotAPIPrefix
	c.snapshotPrefixProbeDone = nil
	close(probeDone)
	c.snapshotPrefixMu.Unlock()
	if prefix != "" {
		return prefix
	}

	// Do not cache the fallback: a transient outage must be re-probed later.
	klog.Warningf("Could not detect snapshot API prefix, defaulting to zfs.snapshot.*")
	return "zfs.snapshot"
}

// snapshotMethod returns the full API method name for a snapshot operation.
func (c *Client) snapshotMethod(ctx context.Context, operation string) string {
	prefix := c.detectSnapshotAPIPrefix(ctx)
	return prefix + "." + operation
}

// Snapshot represents a ZFS snapshot from the TrueNAS API.
type Snapshot struct {
	ID             string                  `json:"id"`
	Name           string                  `json:"name"`
	CreateTXG      uint64                  `json:"createtxg"`
	Dataset        string                  `json:"dataset"`
	Pool           string                  `json:"pool"`
	Type           string                  `json:"type"`
	Properties     map[string]interface{}  `json:"properties"`
	UserProperties map[string]UserProperty `json:"user_properties"`
	// ResourceQuery is true when the snapshot came from the TrueNAS 26.0
	// zfs.resource.snapshot.query read path. That API flattens user properties
	// and does not expose whether a value is local or inherited.
	ResourceQuery bool `json:"-"`
}

// SnapshotCreateParams holds parameters for creating a snapshot.
type SnapshotCreateParams struct {
	Dataset    string            `json:"dataset"`
	Name       string            `json:"name"`
	Recursive  bool              `json:"recursive,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// SnapshotCreate creates a new ZFS snapshot with user properties applied
// atomically by the create operation when the API generation supports it.
func (c *Client) SnapshotCreate(ctx context.Context, dataset, name string, userProperties map[string]string) (*Snapshot, error) {
	prefix := c.detectSnapshotAPIPrefix(ctx)
	params := &SnapshotCreateParams{
		Dataset:    dataset,
		Name:       name,
		Properties: userProperties,
	}

	if len(userProperties) == 0 {
		return c.snapshotCreateCall(ctx, prefix, params)
	}

	c.snapshotCreatePropertiesMu.Lock()
	supported, probed := c.snapshotCreatePropertiesSupport[prefix]
	if probed {
		c.snapshotCreatePropertiesMu.Unlock()
		if supported {
			return c.snapshotCreateCall(ctx, prefix, params)
		}
		return c.snapshotCreateThenSetProperties(ctx, prefix, dataset, name, userProperties)
	}

	// Keep the first probe single-flight. A successful create proves support;
	// a field-validation failure is cached and retried without properties.
	snap, err := c.snapshotCreateCall(ctx, prefix, params)
	if err == nil {
		if c.snapshotCreatePropertiesSupport == nil {
			c.snapshotCreatePropertiesSupport = make(map[string]bool)
		}
		c.snapshotCreatePropertiesSupport[prefix] = true
		c.snapshotCreatePropertiesMu.Unlock()
		return snap, nil
	}
	if !isSnapshotCreatePropertiesValidationError(err) {
		c.snapshotCreatePropertiesMu.Unlock()
		return nil, err
	}
	if c.snapshotCreatePropertiesSupport == nil {
		c.snapshotCreatePropertiesSupport = make(map[string]bool)
	}
	c.snapshotCreatePropertiesSupport[prefix] = false
	c.snapshotCreatePropertiesMu.Unlock()

	klog.Warningf("Snapshot create properties are unsupported by %s; falling back to post-create updates", prefix)
	return c.snapshotCreateThenSetProperties(ctx, prefix, dataset, name, userProperties)
}

func (c *Client) snapshotCreateCall(ctx context.Context, prefix string, params *SnapshotCreateParams) (*Snapshot, error) {
	result, err := c.Call(ctx, prefix+".create", params)
	if err != nil {
		// Ignore "already exists" errors
		if IsAlreadyExistsError(err) {
			return c.SnapshotGet(ctx, params.Dataset+"@"+params.Name)
		}
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	return parseSnapshot(result)
}

func (c *Client) snapshotCreateThenSetProperties(
	ctx context.Context,
	prefix, dataset, name string,
	userProperties map[string]string,
) (*Snapshot, error) {
	snap, err := c.snapshotCreateCall(ctx, prefix, &SnapshotCreateParams{Dataset: dataset, Name: name})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(userProperties))
	for key := range userProperties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := c.Call(ctx, prefix+".update", snap.ID, map[string]interface{}{
			"user_properties_update": []map[string]interface{}{{"key": key, "value": userProperties[key]}},
		}); err != nil {
			return nil, fmt.Errorf("failed to set snapshot property %q after create: %w", key, err)
		}
		if snap.UserProperties == nil {
			snap.UserProperties = make(map[string]UserProperty)
		}
		snap.UserProperties[key] = UserProperty{Value: userProperties[key], Source: "local"}
	}
	return snap, nil
}

func isSnapshotCreatePropertiesValidationError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Code == -32602 {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "propert") &&
		(strings.Contains(message, "validation") || strings.Contains(message, "invalid param") ||
			strings.Contains(message, "unexpected") || strings.Contains(message, "not permitted"))
}

// SnapshotDelete deletes a ZFS snapshot.
// If the snapshot has clones, it returns ErrSnapshotHasClones with the list of clones.
// The caller can then retry with defer=true to let ZFS reclaim the snapshot after
// its final clone releases the dependency.
func (c *Client) SnapshotDelete(ctx context.Context, snapshotID string, defer_, recursive bool) error {
	options := map[string]interface{}{
		"defer":     defer_,
		"recursive": recursive,
	}

	_, err := c.Call(ctx, c.snapshotMethod(ctx, "delete"), snapshotID, options)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous errors)
		LogAPIError(err, "SnapshotDelete error")

		// Ignore "does not exist" errors
		if IsNotFoundError(err) {
			return nil
		}

		// Any other failure is ambiguous (TrueNAS reports has-clones, races,
		// and bad ids with varying messages across versions — e.g. bare
		// "Invalid params"). Distinguish by observation, not message text:
		// a snapshot that no longer exists means the delete goal is met, and
		// dependent clones are detected via the dataset origin projection,
		// which stays authoritative on TrueNAS 26.0 where snapshot queries
		// no longer expose the ZFS clones property.
		snap, getErr := c.SnapshotGet(ctx, snapshotID)
		if getErr != nil {
			if IsNotFoundError(getErr) {
				return nil
			}
			// Liveness unknown — surface the original delete error.
			return fmt.Errorf("failed to delete snapshot: %w", err)
		}

		// Pre-25.04 fast path: clones projected on the snapshot itself.
		if clones := snap.GetClones(); len(clones) > 0 {
			return &ErrSnapshotHasClones{SnapshotID: snapshotID, Clones: clones}
		}
		if clones, cloneErr := c.snapshotDependentClones(ctx, snapshotID); cloneErr == nil && len(clones) > 0 {
			return &ErrSnapshotHasClones{SnapshotID: snapshotID, Clones: clones}
		}

		// Snapshot exists but can't be deleted for unknown reason
		return fmt.Errorf("failed to delete snapshot (unknown reason): %w", err)
	}

	return nil
}

// SnapshotRename renames a snapshot within its current dataset. TrueNAS 26.0
// exposes the operation through zfs.resource.snapshot.rename; older releases
// use the version-detected pool.snapshot.* or zfs.snapshot.* mutation API.
func (c *Client) SnapshotRename(ctx context.Context, snapshotID, newName string) error {
	dataset, _, ok := strings.Cut(snapshotID, "@")
	if !ok || dataset == "" || newName == "" {
		return fmt.Errorf("invalid snapshot rename %q -> %q", snapshotID, newName)
	}

	newSnapshotID := dataset + "@" + newName
	if c.hasSnapshotResourceQuery(ctx) {
		params := map[string]interface{}{
			"current_name": snapshotID,
			"new_name":     newSnapshotID,
			"recursive":    false,
		}
		if _, err := c.Call(ctx, "zfs.resource.snapshot.rename", params); err != nil {
			return fmt.Errorf("failed to rename snapshot: %w", err)
		}
		return nil
	}

	options := map[string]interface{}{
		"new_name":  newName,
		"force":     false,
		"recursive": false,
	}
	if _, err := c.Call(ctx, c.snapshotMethod(ctx, "rename"), snapshotID, options); err != nil {
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}
	return nil
}

// SnapshotGet retrieves a snapshot by ID (dataset@snapshot format).
func (c *Client) SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		dataset, _, ok := strings.Cut(snapshotID, "@")
		if !ok || dataset == "" {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		snapshots, err := c.querySnapshotResources(ctx, []string{dataset}, false, []string{"used", "creation"})
		if err != nil {
			return nil, fmt.Errorf("failed to get snapshot: %w", err)
		}
		for _, snap := range snapshots {
			if snap.ID == snapshotID {
				return snap, nil
			}
		}
		return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
	}

	result, err := c.Call(ctx, c.snapshotMethod(ctx, "get_instance"), snapshotID)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous errors)
		LogAPIError(err, "SnapshotGet error")

		if IsNotFoundError(err) {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		// Older middleware reports a missing get_instance as bare -32602. A
		// structured errno, when present, remains authoritative and must not be
		// replaced by this compatibility fallback.
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Code == -32602 {
			if _, structured := APIErrno(apiErr); structured {
				return nil, fmt.Errorf("failed to get snapshot: %w", err)
			}
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return parseSnapshot(result)
}

// SnapshotList lists snapshots for a dataset.
func (c *Client) SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		snapshots, err := c.querySnapshotResources(ctx, []string{dataset}, false, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshots: %w", err)
		}
		filtered := snapshots[:0]
		for _, snap := range snapshots {
			if snap.Dataset == dataset {
				filtered = append(filtered, snap)
			}
		}
		return filtered, nil
	}

	filters := [][]interface{}{{"dataset", "=", dataset}}

	result, err := c.Call(ctx, c.snapshotMethod(ctx, "query"), filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snap, err := parseSnapshot(item)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// SnapshotListAll lists all snapshots under a parent dataset (recursive).
func (c *Client) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		parentDataset = strings.TrimSuffix(parentDataset, "/")
		snapshots, err := c.querySnapshotResources(ctx, []string{parentDataset}, true, []string{"used", "creation"})
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshots: %w", err)
		}
		filtered := snapshots[:0]
		prefix := parentDataset + "/"
		for _, snap := range snapshots {
			if snap.Dataset != parentDataset && strings.HasPrefix(snap.Dataset, prefix) {
				filtered = append(filtered, snap)
			}
		}
		sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })
		return paginateSnapshots(filtered, limit, offset), nil
	}

	filters := [][]interface{}{{"dataset", "^", strings.TrimSuffix(parentDataset, "/") + "/"}}

	options := map[string]interface{}{}
	if limit > 0 {
		options["limit"] = limit
	}
	if offset > 0 {
		options["offset"] = offset
	}

	result, err := c.Call(ctx, c.snapshotMethod(ctx, "query"), filters, options)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snap, err := parseSnapshot(item)
		if err != nil {
			continue
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// SnapshotFindByName finds a snapshot by its short name under a parent dataset.
// This is more efficient than SnapshotListAll + iteration (PERF-001 fix).
// The name parameter is the snapshot name without the dataset prefix (e.g., "my-snapshot" not "pool/dataset@my-snapshot").
func (c *Client) SnapshotFindByName(ctx context.Context, parentDataset, name string) (*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		parentDataset = strings.TrimSuffix(parentDataset, "/")
		snapshots, err := c.querySnapshotResources(ctx, []string{parentDataset}, true, []string{"used", "creation"})
		if err != nil {
			return nil, fmt.Errorf("failed to query snapshots: %w", err)
		}
		prefix := parentDataset + "/"
		for _, snap := range snapshots {
			if snap.Dataset != parentDataset && strings.HasPrefix(snap.Dataset, prefix) && snap.Name == name {
				return snap, nil
			}
		}
		return nil, nil
	}

	// Build the full snapshot ID pattern to match: any dataset under parentDataset + @ + name
	// We use "id" filter with regex match to find the snapshot regardless of its parent dataset
	// The pattern matches any string ending with "@" + name
	filters := legacySnapshotNameFilters(parentDataset, name)

	result, err := c.Call(ctx, c.snapshotMethod(ctx, "query"), filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	if len(items) == 0 {
		return nil, nil // Not found, not an error
	}

	return parseSnapshot(items[0])
}

func legacySnapshotNameFilters(parentDataset, name string) [][]interface{} {
	return [][]interface{}{
		{"dataset", "^", strings.TrimSuffix(parentDataset, "/") + "/"},
		{"id", "~", fmt.Sprintf(".*@%s$", regexp.QuoteMeta(name))},
	}
}

func (c *Client) querySnapshotResources(ctx context.Context, paths []string, recursive bool, properties []string) ([]*Snapshot, error) {
	result, err := c.Call(ctx, snapshotResourceQueryMethod, snapshotResourceQueryOptions(paths, recursive, properties))
	if err != nil {
		return nil, err
	}
	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	snapshots := make([]*Snapshot, 0, len(items))
	for _, item := range items {
		snap, parseErr := parseSnapshot(item)
		if parseErr != nil {
			continue
		}
		snap.ResourceQuery = true
		snapshots = append(snapshots, snap)
	}
	return snapshots, nil
}

func paginateSnapshots(snapshots []*Snapshot, limit, offset int) []*Snapshot {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(snapshots) {
		return []*Snapshot{}
	}
	end := len(snapshots)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return snapshots[offset:end]
}

// SnapshotSetUserProperty sets a user property on a snapshot.
//
// TrueNAS 26.0 middleware currently returns success for pool.snapshot.update
// user_properties_update while silently applying nothing. Keep this method for
// legacy compatibility, but do not rely on it for correctness on that release.
func (c *Client) SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error {
	params := map[string]interface{}{
		"user_properties_update": []map[string]interface{}{
			{"key": key, "value": value},
		},
	}

	_, err := c.Call(ctx, c.snapshotMethod(ctx, "update"), snapshotID, params)
	return err
}

// SnapshotRemoveUserProperties removes user properties from a snapshot.
// TrueNAS 26.0 has the same silent-no-op middleware bug for property removal;
// callers must use tombstone identity as the correctness boundary there.
func (c *Client) SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	params := map[string]interface{}{
		"user_properties_remove": keys,
	}

	_, err := c.Call(ctx, c.snapshotMethod(ctx, "update"), snapshotID, params)
	return err
}

// SnapshotClone clones a snapshot to create a new dataset.
func (c *Client) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	params := map[string]interface{}{
		"snapshot":    snapshotID,
		"dataset_dst": newDatasetName,
	}

	_, err := c.Call(ctx, c.snapshotMethod(ctx, "clone"), params)
	if err != nil {
		if IsAlreadyExistsError(err) {
			existing, getErr := c.DatasetGet(ctx, newDatasetName)
			if getErr != nil {
				return fmt.Errorf("clone destination %s already exists but its origin could not be verified: %w", newDatasetName, getErr)
			}
			existingOrigin := datasetPropertyString(existing.Origin)
			if existingOrigin != snapshotID {
				return fmt.Errorf("clone destination %s already exists with origin %q, requested origin %q", newDatasetName, existingOrigin, snapshotID)
			}
			return nil
		}
		return fmt.Errorf("failed to clone snapshot: %w", err)
	}

	return nil
}

const replicationJobPollInterval = 500 * time.Millisecond

// CopyDatasetFromSnapshotLocal creates an independent dataset with a local ZFS
// send/receive, then removes the replicated snapshot from the destination.
func (c *Client) CopyDatasetFromSnapshotLocal(ctx context.Context, sourceDataset, snapshotShortName, targetDataset string) error {
	params := map[string]interface{}{
		"direction":         "PUSH",
		"transport":         "LOCAL",
		"source_datasets":   []string{sourceDataset},
		"target_dataset":    targetDataset,
		"recursive":         false,
		"replicate":         false,
		"name_regex":        "^" + regexp.QuoteMeta(snapshotShortName) + "$",
		"retention_policy":  "NONE",
		"readonly":          "IGNORE",
		"only_from_scratch": true,
	}

	result, err := c.Call(ctx, "replication.run_onetime", params)
	if err != nil {
		if IsAlreadyExistsError(err) && c.localCopyTargetExists(ctx, targetDataset) {
			klog.Infof("Local snapshot copy target %s already exists; continuing idempotently", targetDataset)
			return c.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName)
		}
		return fmt.Errorf("failed to start local snapshot copy: %w", err)
	}

	jobID, err := replicationJobID(result)
	if err != nil {
		return fmt.Errorf("failed to start local snapshot copy: %w", err)
	}
	if err := c.waitForJob(ctx, jobID); err != nil {
		// only_from_scratch reports an existing target through the asynchronous
		// job. A previous successful request may have completed before its caller
		// observed the response, so existence is the idempotency boundary.
		var terminalErr *jobTerminalError
		if errors.As(err, &terminalErr) && IsAlreadyExistsError(err) && c.localCopyTargetExists(ctx, targetDataset) {
			klog.Infof("Local snapshot copy target %s already exists after job %d; continuing idempotently", targetDataset, jobID)
			return c.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName)
		}
		return fmt.Errorf("local snapshot copy job %d failed: %w", jobID, err)
	}

	return c.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName)
}

type jobTerminalError struct {
	state  string
	detail string
}

func (e *jobTerminalError) Error() string {
	return fmt.Sprintf("job entered state %s: %s", e.state, e.detail)
}

func replicationJobID(result interface{}) (int64, error) {
	switch value := result.(type) {
	case float64:
		jobID, valid := nonNegativeInt64FromFloat(value)
		if !valid || math.Trunc(value) != value {
			return 0, fmt.Errorf("unexpected replication job id %v", value)
		}
		return jobID, nil
	case int:
		if value < 0 {
			return 0, fmt.Errorf("unexpected replication job id %d", value)
		}
		return int64(value), nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("unexpected replication job id %d", value)
		}
		return value, nil
	default:
		return 0, fmt.Errorf("unexpected replication job id type %T", result)
	}
}

func (c *Client) waitForJob(ctx context.Context, jobID int64) error {
	filters := [][]interface{}{{"id", "=", jobID}}
	for {
		result, err := c.Call(ctx, "core.get_jobs", filters)
		if err != nil {
			return fmt.Errorf("failed to query job: %w", err)
		}
		jobs, ok := result.([]interface{})
		if !ok {
			return fmt.Errorf("unexpected core.get_jobs response type %T", result)
		}
		if len(jobs) > 0 {
			job, ok := jobs[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("unexpected job response type %T", jobs[0])
			}
			state, _ := job["state"].(string)
			switch strings.ToUpper(state) {
			case "SUCCESS":
				return nil
			case "FAILED", "ABORTED", "CANCELED":
				detail := "no error detail"
				if message, ok := job["error"].(string); ok && message != "" {
					detail = message
				} else if exception, ok := job["exception"].(string); ok && exception != "" {
					detail = exception
				}
				return &jobTerminalError{state: state, detail: detail}
			}
		}

		timer := time.NewTimer(replicationJobPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("context ended while waiting for job: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (c *Client) localCopyTargetExists(ctx context.Context, targetDataset string) bool {
	exists, err := c.DatasetExists(ctx, targetDataset)
	return err == nil && exists
}

// DestroyReplicatedTargetSnapshot removes the snapshot transferred to the
// destination by a local replication copy. The operation is idempotent.
func (c *Client) DestroyReplicatedTargetSnapshot(ctx context.Context, targetDataset, snapshotShortName string) error {
	targetSnapshot := targetDataset + "@" + snapshotShortName
	if err := c.SnapshotDelete(ctx, targetSnapshot, false, false); err != nil {
		if IsNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("failed to remove replicated target snapshot %s: %w", targetSnapshot, err)
	}
	return nil
}

// SnapshotRollback rolls back a dataset to a snapshot.
func (c *Client) SnapshotRollback(ctx context.Context, snapshotID string, force, recursive, recursiveClones bool) error {
	options := map[string]interface{}{
		"force":            force,
		"recursive":        recursive,
		"recursive_clones": recursiveClones,
	}

	_, err := c.Call(ctx, c.snapshotMethod(ctx, "rollback"), snapshotID, options)
	if err != nil {
		return fmt.Errorf("failed to rollback snapshot: %w", err)
	}

	return nil
}

// parseSnapshot converts a raw API response to a Snapshot struct.
func parseSnapshot(data interface{}) (*Snapshot, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected snapshot format")
	}

	snap := &Snapshot{
		Properties:     make(map[string]interface{}),
		UserProperties: make(map[string]UserProperty),
	}

	if v, ok := m["id"].(string); ok {
		snap.ID = v
	}
	if v, ok := m["name"].(string); ok {
		snap.Name = v
		if snap.ID == "" && strings.Contains(v, "@") {
			snap.ID = v
		}
	}
	if v, ok := m["snapshot_name"].(string); ok {
		snap.Name = v
	}
	if v, ok := unsignedInteger(m["createtxg"]); ok {
		snap.CreateTXG = v
	}
	if v, ok := m["dataset"].(string); ok {
		snap.Dataset = v
	}
	if v, ok := m["pool"].(string); ok {
		snap.Pool = v
	}
	if v, ok := m["type"].(string); ok {
		snap.Type = v
	}
	if snap.ID == "" && snap.Dataset != "" && snap.Name != "" {
		snap.ID = snap.Dataset + "@" + snap.Name
	}

	// Parse properties
	if props, ok := m["properties"].(map[string]interface{}); ok {
		snap.Properties = props
		// Also look for user properties in properties map (keys with :)
		for key, val := range props {
			if strings.Contains(key, ":") {
				if propMap, ok := val.(map[string]interface{}); ok {
					prop := UserProperty{}
					if v, ok := propMap["value"].(string); ok {
						prop.Value = v
					}
					if v, ok := propMap["source"].(string); ok {
						prop.Source = v
					}
					snap.UserProperties[key] = prop
				}
			}
		}
	}

	// Parse user properties (if explicitly returned in separate field)
	if userProps, ok := m["user_properties"].(map[string]interface{}); ok {
		for key, val := range userProps {
			switch propValue := val.(type) {
			case string:
				snap.UserProperties[key] = UserProperty{Value: propValue}
			case map[string]interface{}:
				prop := UserProperty{}
				if v, ok := propValue["value"].(string); ok {
					prop.Value = v
				}
				if v, ok := propValue["source"].(string); ok {
					prop.Source = v
				}
				snap.UserProperties[key] = prop
			}
		}
	}

	return snap, nil
}

func unsignedInteger(value interface{}) (uint64, bool) {
	switch v := value.(type) {
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint64:
		return v, true
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

// GetSnapshotSize returns the size of a snapshot in bytes.
func (snap *Snapshot) GetSnapshotSize() int64 {
	if used, ok := snap.Properties["used"]; ok {
		if usedMap, ok := used.(map[string]interface{}); ok {
			if parsed, ok := usedMap["parsed"].(float64); ok {
				if size, valid := nonNegativeInt64FromFloat(parsed); valid {
					return size
				}
			}
			// TrueNAS 26.0 zfs.resource.snapshot.query shape: {"value": <number>, "raw": "<string>"}
			if value, ok := usedMap["value"].(float64); ok {
				if size, valid := nonNegativeInt64FromFloat(value); valid {
					return size
				}
			}
			if raw, ok := usedMap["raw"].(string); ok {
				if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
					return v
				}
			}
		}
	}
	return 0
}

// GetCreationTime returns the creation timestamp of a snapshot (unix seconds).
func (snap *Snapshot) GetCreationTime() int64 {
	if creation, ok := snap.Properties["creation"]; ok {
		if creationMap, ok := creation.(map[string]interface{}); ok {
			// Try parsed as float64 first (some versions may return this)
			if parsed, ok := creationMap["parsed"].(float64); ok {
				if timestamp, valid := nonNegativeInt64FromFloat(parsed); valid {
					return timestamp
				}
			}
			// TrueNAS returns parsed as {"$date": milliseconds}
			if parsedMap, ok := creationMap["parsed"].(map[string]interface{}); ok {
				if dateMs, ok := parsedMap["$date"].(float64); ok {
					if timestamp, valid := nonNegativeInt64FromFloat(dateMs / 1000); valid {
						return timestamp
					}
				}
			}
			// Fallback: parse rawvalue as string
			if rawvalue, ok := creationMap["rawvalue"].(string); ok {
				if ts, err := strconv.ParseInt(rawvalue, 10, 64); err == nil {
					return ts
				}
			}
			// TrueNAS 26.0 zfs.resource.snapshot.query shape: {"value": <number>, "raw": "<string>"}
			if value, ok := creationMap["value"].(float64); ok {
				if timestamp, valid := nonNegativeInt64FromFloat(value); valid {
					return timestamp
				}
			}
			if raw, ok := creationMap["raw"].(string); ok {
				if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
					return ts
				}
			}
		}
	}
	return 0
}

// GetClones returns a list of clone dataset names that were created from this snapshot.
// TrueNAS 26.0 no longer projects clones through either snapshot read API; callers
// that require an authoritative dependency check must use DatasetHasDependentClones.
func (snap *Snapshot) GetClones() []string {
	if clones, ok := snap.Properties["clones"]; ok {
		if clonesMap, ok := clones.(map[string]interface{}); ok {
			if value, ok := clonesMap["value"].(string); ok && value != "" {
				// Clones are comma-separated
				return strings.Split(value, ",")
			}
		}
	}
	return nil
}
