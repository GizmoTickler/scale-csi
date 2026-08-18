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
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

const snapshotResourceQueryMethod = "zfs.resource.snapshot.query"

// snapshotResourceQueryStatus detects the TrueNAS 26.0 snapshot resource API.
// Successful and method-not-found probes are cached; transient failures are
// deliberately retried on the next call. The second result distinguishes a
// proved legacy backend from an unknown/transient probe result so mutation
// callers never fall through to pool.snapshot.update on 26.0. Concurrent
// callers share one probe.
func (c *Client) snapshotResourceQueryStatus(ctx context.Context) (available, detected bool) {
	c.snapshotResourceMu.Lock()
	if c.snapshotResourceDetected {
		cachedAvailable := c.snapshotResourceAvailable
		c.snapshotResourceMu.Unlock()
		return cachedAvailable, true
	}
	if probeDone := c.snapshotResourceProbeDone; probeDone != nil {
		c.snapshotResourceMu.Unlock()
		select {
		case <-probeDone:
			c.snapshotResourceMu.Lock()
			cachedDetected := c.snapshotResourceDetected
			cachedAvailable := cachedDetected && c.snapshotResourceAvailable
			c.snapshotResourceMu.Unlock()
			return cachedAvailable, cachedDetected
		case <-ctx.Done():
			return false, false
		}
	}

	probeDone := make(chan struct{})
	c.snapshotResourceProbeDone = probeDone
	c.snapshotResourceMu.Unlock()

	_, err := c.Call(ctx, snapshotResourceQueryMethod, snapshotResourceQueryOptions(nil, false, nil))
	detected = err == nil || isMethodNotFoundError(err)
	available = err == nil

	c.snapshotResourceMu.Lock()
	if detected && !c.snapshotResourceDetected {
		c.snapshotResourceDetected = true
		c.snapshotResourceAvailable = available
	}
	c.snapshotResourceProbeDone = nil
	close(probeDone)
	detected = c.snapshotResourceDetected
	available = detected && c.snapshotResourceAvailable
	c.snapshotResourceMu.Unlock()

	if available {
		klog.V(2).Infof("Detected TrueNAS 26.0 snapshot resource API")
	} else if err != nil && !detected {
		klog.Warningf("Could not detect snapshot resource API; mutation callers will fail closed and reads may retry through the legacy path: %v", err)
	}
	return available, detected
}

func (c *Client) hasSnapshotResourceQuery(ctx context.Context) bool {
	available, _ := c.snapshotResourceQueryStatus(ctx)
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

// ErrDatasetDestinationExists reports that a clone/copy request lost the race
// to an already-existing destination. Even a matching origin is not creation
// proof: callers must retry through their normal ownership/idempotency gate and
// must not stamp, mutate, or clean up this object.
type ErrDatasetDestinationExists struct {
	Destination     string
	ExpectedOrigin  string
	ActualOrigin    string
	VerificationErr error
}

func (e *ErrDatasetDestinationExists) Error() string {
	if e.VerificationErr != nil {
		return fmt.Sprintf("clone destination %s already exists but its origin could not be verified: %v",
			e.Destination, e.VerificationErr)
	}
	if e.ExpectedOrigin != "" || e.ActualOrigin != "" {
		return fmt.Sprintf("clone destination %s already exists with origin %q, requested origin %q",
			e.Destination, e.ActualOrigin, e.ExpectedOrigin)
	}
	return fmt.Sprintf("dataset destination %s already exists", e.Destination)
}

func IsDatasetDestinationExistsError(err error) bool {
	var destinationErr *ErrDatasetDestinationExists
	return errors.As(err, &destinationErr)
}

// snapshotAPIPrefix is the unconditional mutation API prefix for snapshots.
//
// TrueNAS 25.04 is the documented floor for this driver, and 25.04 moved the
// snapshot mutation API from zfs.snapshot.* to pool.snapshot.*. The 24.x
// zfs.snapshot.* leg (and its runtime probe/fallback) is removed: it does not
// exist on 26.0, so the old default was actively wrong there. Reads needing
// user properties, plus rename and destroy, still use the separately-detected
// TrueNAS 26.0 zfs.resource.snapshot.* path (see snapshotResourceQueryStatus).
const snapshotAPIPrefix = "pool.snapshot"

// snapshotMethod returns the full API method name for a snapshot operation.
func (c *Client) snapshotMethod(operation string) string {
	return snapshotAPIPrefix + "." + operation
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
	prefix := snapshotAPIPrefix
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

	// Keep the first probe single-flight. Live TrueNAS 26.0 probes prove that
	// inline snapshot-create properties persist even though no working API can
	// add properties to an existing snapshot. A successful create proves support;
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
	var err error
	if c.hasSnapshotResourceQuery(ctx) {
		// TrueNAS 26.0's live wire contract is a single object argument containing
		// path; zfs.resource.snapshot.destroy does not accept the legacy positional
		// snapshot id and options pair.
		_, err = c.Call(ctx, "zfs.resource.snapshot.destroy", map[string]interface{}{"path": snapshotID})
	} else {
		options := map[string]interface{}{
			"defer":     defer_,
			"recursive": recursive,
		}
		_, err = c.Call(ctx, c.snapshotMethod("delete"), snapshotID, options)
	}
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous errors)
		LogAPIError(err, "SnapshotDelete error")

		// Ignore "does not exist" errors
		if IsNotFoundError(err) && !isMethodNotFoundError(err) {
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
// use the pool.snapshot.* mutation API.
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
	if _, err := c.Call(ctx, c.snapshotMethod("rename"), snapshotID, options); err != nil {
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}
	return nil
}

// The hold tag applied by pool.snapshot.hold is hard-coded to "truenas" on
// TrueNAS 26.0: the live probe (P1) proved neither pool.snapshot.hold nor
// zfs.resource.snapshot.hold accepts a custom tag — a hold is a single global
// boolean per snapshot, so the driver never names a tag and treats a hold as
// "ours" only after it has provenance-proven the snapshot is a driver CSI
// snapshot/tombstone (R2).

// SnapshotHold places a deletion-proof hold on a snapshot so foreign actors
// (box-wide periodic-task pruning, admin, replication retention) hit EBUSY on
// destroy. It is idempotent: an already-held snapshot (EEXIST/errno 17, surfaced
// by TrueNAS as a wrapped lzc_hold() failure) is treated as success. The hold is
// the fixed `truenas` tag; the API accepts no custom tag (P1).
func (c *Client) SnapshotHold(ctx context.Context, snapshotID string) error {
	_, err := c.Call(ctx, c.snapshotMethod("hold"), snapshotID)
	if err == nil || isSnapshotAlreadyHeldError(err) {
		return nil
	}
	return fmt.Errorf("failed to hold snapshot: %w", err)
}

// SnapshotRelease removes the hold placed by SnapshotHold. It is idempotent:
// releasing a snapshot that is not held (ENOENT / no-such-hold) is success.
func (c *Client) SnapshotRelease(ctx context.Context, snapshotID string) error {
	_, err := c.Call(ctx, c.snapshotMethod("release"), snapshotID)
	if err == nil || isSnapshotNotHeldError(err) {
		return nil
	}
	return fmt.Errorf("failed to release snapshot: %w", err)
}

// SnapshotIsHeld reports whether the snapshot carries any hold. It reads the
// resource path (zfs.resource.snapshot.holds) rather than the extra:{holds}
// query so it works on the same API generation as destroy/rename. On a backend
// without the method (pre-26.0) it fails closed to "not held" so a hold-less
// older backend never blocks the driver's own lifecycle.
func (c *Client) SnapshotIsHeld(ctx context.Context, snapshotID string) (bool, error) {
	var holds []string
	if err := callTyped(ctx, c, &holds, "zfs.resource.snapshot.holds", map[string]interface{}{"path": snapshotID}); err != nil {
		if isMethodNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to query snapshot holds: %w", err)
	}
	return len(holds) > 0, nil
}

// isSnapshotAlreadyHeldError treats the idempotent already-held outcomes as
// success. TrueNAS reports a re-hold as EEXIST/errno 17, sometimes wrapped as a
// bare lzc_hold() failure whose only structured signal is the errno; both the
// errno path and the ZFS wrapper message are accepted.
func isSnapshotAlreadyHeldError(err error) bool {
	if err == nil || IsAlreadyExistsError(err) {
		return true
	}
	if errno, ok := APIErrno(err); ok {
		return errno == syscall.EEXIST
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "lzc_hold") && lzcErrnoMatches(message, 17)
}

// lzcErrnoTuplePattern matches the errno POSITION of a libzfs_core traceback,
// e.g. `('lzc_hold() failed', (…, 17))` or `lzc_hold() failed: errno 17`. The
// errno must sit in a tuple-tail or explicit `errno N` position, so a digit run
// that merely appears inside the snapshot path can never be mistaken for it —
// PVC names are UUIDs and routinely contain "17" (e.g. `pvc-17a3…`), which the
// previous bare substring match would have reported as "already held" for ANY
// lzc_hold failure on such a snapshot (GF2-fix/F9).
var lzcErrnoTuplePattern = regexp.MustCompile(`errno\D{0,3}(\d+)|,\s*(\d+)\s*\)`)

// lzcErrnoMatches reports whether an lzc_* traceback message carries exactly
// the given errno in an errno-shaped position.
func lzcErrnoMatches(message string, want int) bool {
	for _, match := range lzcErrnoTuplePattern.FindAllStringSubmatch(message, -1) {
		for _, group := range match[1:] {
			if group == "" {
				continue
			}
			if value, err := strconv.Atoi(group); err == nil && value == want {
				return true
			}
		}
	}
	return false
}

// IsSnapshotHeldError reports whether a destroy failed BECAUSE the snapshot (or
// one under a recursively destroyed dataset) still carries a ZFS hold. TrueNAS
// surfaces this as EBUSY with a "has the following holds: truenas" message (P1).
//
// This is what makes hold/release fail-safe against configuration history
// (GF2-fix/H4): holds are backend state that outlives the zfs.holdCsiSnapshots
// flag, so a driver whose flag was turned back OFF must still be able to destroy
// its OWN previously-held snapshots. Callers use it to trigger an unconditional
// release-and-retry — which costs zero extra API calls on the default path,
// because a deployment that never held anything never sees this error.
func IsSnapshotHeldError(err error) bool {
	if err == nil {
		return false
	}
	if errno, ok := APIErrno(err); ok && errno == syscall.EBUSY {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "has the following holds")
}

// isSnapshotNotHeldError treats releasing a non-held snapshot as success: an
// authoritative ENOENT, or (on releases that surface no errno) a message that
// names the missing hold.
func isSnapshotNotHeldError(err error) bool {
	if err == nil || IsNotFoundError(err) {
		return true
	}
	if _, ok := APIErrno(err); ok {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "hold") &&
		(strings.Contains(message, "not") || strings.Contains(message, "no such"))
}

// SnapshotGet retrieves a snapshot by ID (dataset@snapshot format).
func (c *Client) SnapshotGet(ctx context.Context, snapshotID string) (*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		dataset, _, ok := strings.Cut(snapshotID, "@")
		if !ok || dataset == "" {
			return nil, fmt.Errorf("snapshot not found: %s", snapshotID)
		}
		snapshots, err := c.querySnapshotResources(ctx, []string{dataset}, false, snapshotResourceQueryProperties)
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

	var raw *rawSnapshot
	if err := callTyped(ctx, c, &raw, c.snapshotMethod("get_instance"), snapshotID); err != nil {
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
	if raw == nil {
		return nil, fmt.Errorf("unexpected snapshot format")
	}

	return raw.toSnapshot(), nil
}

// SnapshotList lists snapshots for a dataset.
//
// It requests the SAME explicit property projection as every other read path
// (used + creation) rather than leaving `properties` unset. DeleteVolume's
// scheduled-snapshot ownership predicate requires the `creation` property and
// fails CLOSED without it (GF2-fix2/B1-a), so this call must not depend on the
// server's default projection for a property the driver's safety decision reads.
// No extra round trip: same call, narrower payload.
func (c *Client) SnapshotList(ctx context.Context, dataset string) ([]*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		snapshots, err := c.querySnapshotResources(ctx, []string{dataset}, false, snapshotResourceQueryProperties)
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

	var raw []*rawSnapshot
	if err := callTyped(ctx, c, &raw, c.snapshotMethod("query"), filters, map[string]interface{}{}); err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	return rawSnapshotsToSnapshots(raw, false), nil
}

// SnapshotListAll lists all snapshots under a parent dataset (recursive).
func (c *Client) SnapshotListAll(ctx context.Context, parentDataset string, limit, offset int) ([]*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		parentDataset = strings.TrimSuffix(parentDataset, "/")
		snapshots, err := c.querySnapshotResources(ctx, []string{parentDataset}, true, snapshotResourceQueryProperties)
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

	var raw []*rawSnapshot
	if err := callTyped(ctx, c, &raw, c.snapshotMethod("query"), filters, options); err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	return rawSnapshotsToSnapshots(raw, false), nil
}

// SnapshotFindByName finds a snapshot by its short name under a parent dataset.
// This is more efficient than SnapshotListAll + iteration (PERF-001 fix).
// The name parameter is the snapshot name without the dataset prefix (e.g., "my-snapshot" not "pool/dataset@my-snapshot").
//
// P-1: the 26.0 leg used to fetch the ENTIRE recursive snapshot set under the
// parent via zfs.resource.snapshot.query with get_user_properties (live-measured
// on nas01, TrueNAS 26.0.0-BETA.2: ~650ms and >1MB at 576 snapshots, linear in
// snapshot count) and filter client-side. It now probes server-side first:
//
//  1. pool.snapshot.query with parent-scoped filters
//     [["dataset","^",parent+"/"],["snapshot_name","=",name]] and
//     {"select":["name","dataset","snapshot_name","createtxg"]} — live-verified
//     on 26.0.0-BETA.2 at ~330ms flat and ~300 bytes for both positive and
//     negative matches. pool.snapshot.query on 26.0 returns NO user_properties
//     key at all, `properties` as an empty dict, and createtxg as a STRING, so
//     the probe can only LOCATE, never classify.
//  2. Zero probe hits → nil, nil with no scan at all (the common fresh-create
//     case in CreateSnapshot's uniqueness check).
//  3. Probe hits → ONE targeted zfs.resource.snapshot.query with all hit paths
//     (non-recursive, get_user_properties:true — live-measured ~2ms per call,
//     accepts multiple paths in one call) to supply the identity properties
//     callers read (UserProperties), exactly as the old scan did.
//  4. Any probe error (including method-not-found on some future build) falls
//     back to the recursive scan below, unchanged.
func (c *Client) SnapshotFindByName(ctx context.Context, parentDataset, name string) (*Snapshot, error) {
	if c.hasSnapshotResourceQuery(ctx) {
		parentDataset = strings.TrimSuffix(parentDataset, "/")

		paths, probeErr := c.probeSnapshotPathsByName(ctx, parentDataset, name)
		if probeErr == nil {
			if len(paths) == 0 {
				return nil, nil // Not found, not an error — and no recursive scan paid.
			}
			// One targeted read supplies what the probe cannot: the full snapshot
			// object with user properties for the callers' identity checks.
			snapshots, err := c.querySnapshotResources(ctx, paths, false, snapshotResourceQueryProperties)
			if err != nil {
				return nil, fmt.Errorf("failed to query snapshots: %w", err)
			}
			// Re-check the short name client-side and pick the first match in
			// deterministic (ID) order.
			var match *Snapshot
			for _, snap := range snapshots {
				if snap.Name != name {
					continue
				}
				if match == nil || snap.ID < match.ID {
					match = snap
				}
			}
			if match == nil {
				return nil, nil
			}
			return match, nil
		}
		klog.V(2).Infof("Snapshot name probe via %s failed; falling back to recursive snapshot scan under %q: %v",
			c.snapshotMethod("query"), parentDataset, probeErr)

		// FALLBACK: the pre-P-1 recursive scan, unchanged.
		snapshots, err := c.querySnapshotResources(ctx, []string{parentDataset}, true, snapshotResourceQueryProperties)
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

	var raw []*rawSnapshot
	if err := callTyped(ctx, c, &raw, c.snapshotMethod("query"), filters, map[string]interface{}{}); err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil // Not found, not an error
	}
	return raw[0].toSnapshot(), nil
}

func legacySnapshotNameFilters(parentDataset, name string) [][]interface{} {
	return [][]interface{}{
		{"dataset", "^", strings.TrimSuffix(parentDataset, "/") + "/"},
		{"id", "~", fmt.Sprintf(".*@%s$", regexp.QuoteMeta(name))},
	}
}

// snapshotNameProbeFilters is the P-1 server-side name probe's filter set.
// Unlike legacySnapshotNameFilters it uses an EQUALITY snapshot_name filter,
// not a regex on id — equality is cheaper and exact. The `"dataset","^",parent+"/"`
// filter also preserves the recursive scan's semantics that a snapshot ON the
// parent dataset itself is excluded (the scan requires
// snap.Dataset != parentDataset && strings.HasPrefix(snap.Dataset, parent+"/")).
func snapshotNameProbeFilters(parentDataset, name string) [][]interface{} {
	return [][]interface{}{
		{"dataset", "^", strings.TrimSuffix(parentDataset, "/") + "/"},
		{"snapshot_name", "=", name},
	}
}

// snapshotNameProbeSelect keeps the probe response tiny (~300 bytes live-measured
// on 26.0.0-BETA.2, vs >1MB for the recursive scan it replaces). createtxg comes
// back as a STRING on 26.0's pool.snapshot.query and is not read here; the shape
// is kept exactly as live-verified.
var snapshotNameProbeSelect = []string{"name", "dataset", "snapshot_name", "createtxg"}

// probeSnapshotPathsByName runs the P-1 pool.snapshot.query name probe
// (live-verified to work for READS on TrueNAS 26.0.0-BETA.2, ~330ms flat
// regardless of snapshot count) and returns the matching <dataset@name> paths,
// deduplicated and in deterministic (sorted) order. It locates only; the caller
// classifies via a targeted zfs.resource.snapshot.query, because on 26.0 this
// method returns no user_properties key and an empty properties dict.
func (c *Client) probeSnapshotPathsByName(ctx context.Context, parentDataset, name string) ([]string, error) {
	var rows []struct {
		Name         string `json:"name"`
		Dataset      string `json:"dataset"`
		SnapshotName string `json:"snapshot_name"`
	}
	options := map[string]interface{}{"select": snapshotNameProbeSelect}
	if err := callTyped(ctx, c, &rows, c.snapshotMethod("query"), snapshotNameProbeFilters(parentDataset, name), options); err != nil {
		return nil, err
	}
	parent := strings.TrimSuffix(parentDataset, "/")
	prefix := parent + "/"
	paths := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		// Defensive re-check of the server-side dataset scoping: strictly below
		// the parent, never the parent itself.
		if row.Dataset == "" || row.Dataset == parent || !strings.HasPrefix(row.Dataset, prefix) {
			continue
		}
		path := row.Name
		if !strings.Contains(path, "@") {
			path = row.Dataset + "@" + row.SnapshotName
		}
		if _, dup := seen[path]; dup {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func (c *Client) querySnapshotResources(ctx context.Context, paths []string, recursive bool, properties []string) ([]*Snapshot, error) {
	var raw []*rawSnapshot
	if err := callTyped(ctx, c, &raw, snapshotResourceQueryMethod, snapshotResourceQueryOptions(paths, recursive, properties)); err != nil {
		return nil, err
	}
	return rawSnapshotsToSnapshots(raw, true), nil
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
// TrueNAS 26.0 has no working mutation path for properties on an existing
// snapshot: zfs.resource.snapshot.update does not exist and pool.snapshot.update
// acknowledges the request while silently dropping it. Keep this method only
// for older backends; 26.0 callers get an explicit unsupported error.
func (c *Client) SnapshotSetUserProperty(ctx context.Context, snapshotID, key, value string) error {
	resourceAPI, detected := c.snapshotResourceQueryStatus(ctx)
	if !detected {
		return fmt.Errorf("snapshot API generation could not be determined; refusing a potentially silent existing-snapshot property update")
	}
	if resourceAPI {
		return fmt.Errorf("existing snapshot user-property updates are unsupported by the TrueNAS 26.0 resource API")
	}
	params := map[string]interface{}{
		"user_properties_update": []map[string]interface{}{
			{"key": key, "value": value},
		},
	}

	_, err := c.Call(ctx, c.snapshotMethod("update"), snapshotID, params)
	return err
}

// SnapshotRemoveUserProperties removes user properties from a snapshot.
// TrueNAS 26.0 has the same silent-no-op behavior for property removal. Removal
// is best-effort cleanup after a durable rename tombstone, so skip the
// nonexistent mutation rather than pretending pool.snapshot.update persisted it.
func (c *Client) SnapshotRemoveUserProperties(ctx context.Context, snapshotID string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	resourceAPI, detected := c.snapshotResourceQueryStatus(ctx)
	if !detected {
		return fmt.Errorf("snapshot API generation could not be determined; refusing a potentially silent existing-snapshot property removal")
	}
	if resourceAPI {
		return nil
	}
	// Legacy backends can really remove: widen canonical scale-csi:* keys to
	// their truenas-csi:* twins so pre-rename snapshot stamps go too.
	params := map[string]interface{}{
		"user_properties_remove": expandCSIPropertyRemovalKeys(keys),
	}

	_, err := c.Call(ctx, c.snapshotMethod("update"), snapshotID, params)
	return err
}

// SnapshotClone clones a snapshot to create a new dataset.
func (c *Client) SnapshotClone(ctx context.Context, snapshotID, newDatasetName string) error {
	params := map[string]interface{}{
		"snapshot":    snapshotID,
		"dataset_dst": newDatasetName,
	}

	_, err := c.Call(ctx, c.snapshotMethod("clone"), params)
	if err != nil {
		if IsAlreadyExistsError(err) {
			existing, getErr := c.DatasetGet(ctx, newDatasetName)
			if getErr != nil {
				return &ErrDatasetDestinationExists{
					Destination: newDatasetName, ExpectedOrigin: snapshotID, VerificationErr: getErr,
				}
			}
			existingOrigin := datasetPropertyString(existing.Origin)
			return &ErrDatasetDestinationExists{
				Destination: newDatasetName, ExpectedOrigin: snapshotID, ActualOrigin: existingOrigin,
			}
		}
		return fmt.Errorf("failed to clone snapshot: %w", err)
	}

	return nil
}

const (
	replicationJobPollInterval = 500 * time.Millisecond
	replicationJobAbortTimeout = 10 * time.Second
	slowSafetyNetInterval      = 10 * time.Second
)

// CopyDatasetFromSnapshotLocal creates an independent dataset with a local ZFS
// send/receive, then removes the replicated snapshot from the destination.
func (c *Client) CopyDatasetFromSnapshotLocal(
	ctx context.Context,
	sourceDataset, snapshotShortName, targetDataset string,
) (jobID int64, retErr error) {
	jobID = UnknownReplicationJobID
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
	// Every error after a proven launch attempts to abort the middleware job. The
	// cleanup context deliberately survives a canceled/deadline-exceeded CSI
	// request; using ctx here is the original leak.
	defer func() {
		if retErr == nil || jobID == UnknownReplicationJobID {
			return
		}
		reason := ReplicationJobAbortReasonCopyFailed
		if errors.Is(retErr, context.Canceled) || errors.Is(retErr, context.DeadlineExceeded) {
			reason = ReplicationJobAbortReasonContextEnded
		}
		abortCtx, cancel := replicationJobCleanupContext(ctx)
		defer cancel()
		if abortErr := c.ReplicationJobAbort(abortCtx, jobID, reason); abortErr != nil {
			klog.Warningf("Failed to abort one-time replication job %d after detached-copy failure: %v", jobID, abortErr)
			return
		}
		klog.Infof("Aborted one-time replication job %d after detached-copy failure", jobID)
	}()

	result, err := c.Call(ctx, ReplicationRunOnetimeMethod, params)
	if err != nil {
		// Cancellation may arrive after middleware accepted the mutation but before
		// its response reached us. Recover only by the complete method+arguments
		// signature, never by a broad dataset prefix.
		if ctx.Err() != nil || errors.Is(err, ErrAmbiguousResult) {
			recoveryCtx, cancel := replicationJobCleanupContext(ctx)
			recoveredID, recoveryErr := c.recoverReplicationJobID(recoveryCtx, params)
			cancel()
			if recoveryErr != nil {
				klog.Warningf("Could not recover an ambiguously launched one-time replication job for target %s: %v", targetDataset, recoveryErr)
			} else {
				jobID = recoveredID
			}
		}
		if IsAlreadyExistsError(err) {
			return jobID, &ErrDatasetDestinationExists{Destination: targetDataset}
		}
		return jobID, fmt.Errorf("failed to start local snapshot copy: %w", err)
	}

	// Capture the job ID immediately after the launch returns. There is no
	// cancellation point between the response and this parse.
	jobID, err = replicationJobID(result)
	if err != nil {
		recoveryCtx, cancel := replicationJobCleanupContext(ctx)
		recoveredID, recoveryErr := c.recoverReplicationJobID(recoveryCtx, params)
		cancel()
		if recoveryErr != nil {
			klog.Warningf("Could not recover one-time replication job after an invalid launch response for target %s: %v", targetDataset, recoveryErr)
		} else {
			jobID = recoveredID
		}
		return jobID, fmt.Errorf("failed to start local snapshot copy: %w", err)
	}
	if err := c.waitForJob(ctx, jobID); err != nil {
		// only_from_scratch reports an existing target through the asynchronous
		// job. AlreadyExists is itself proof that this caller does not own the
		// destination; no follow-up read may downgrade that safety decision.
		var terminalErr *jobTerminalError
		if errors.As(err, &terminalErr) && IsAlreadyExistsError(err) {
			return jobID, &ErrDatasetDestinationExists{Destination: targetDataset}
		}
		return jobID, fmt.Errorf("local snapshot copy job %d failed: %w", jobID, err)
	}

	if err := c.DestroyReplicatedTargetSnapshot(ctx, targetDataset, snapshotShortName); err != nil {
		return jobID, err
	}
	return jobID, nil
}

func replicationJobCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), replicationJobAbortTimeout)
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
	if c.dispatcher == nil {
		return clientClosedJobWaitError(jobID)
	}
	evCh := c.dispatcher.register(jobID)
	defer c.dispatcher.unregister(jobID, evCh)

	// Snapshot this before the initial poll. If a connection subscribes while
	// the poll is in flight, its pulse triggers the post-subscribe poll for that
	// generation and closes the subscribe-after-terminal gap.
	subscriptionChanged := c.jobSubscriptionSignal()
	if done, err := c.pollJob(ctx, jobID); done || err != nil {
		return err
	}

	pollInterval := replicationJobPollInterval
	if c.jobWaitPollInterval > 0 {
		pollInterval = c.jobWaitPollInterval
	}
	safetyInterval := slowSafetyNetInterval
	if c.jobWaitSafetyInterval > 0 {
		safetyInterval = c.jobWaitSafetyInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	lastPoll := time.Now()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context ended while waiting for job: %w", ctx.Err())
		case ev, ok := <-evCh:
			if !ok {
				return clientClosedJobWaitError(jobID)
			}
			if terminalJobState(ev.state) {
				return terminalJobResult(ev)
			}
		case <-subscriptionChanged:
			if done, err := c.pollJob(ctx, jobID); done || err != nil {
				return err
			}
			lastPoll = time.Now()
			subscriptionChanged = c.jobSubscriptionSignal()
		case <-ticker.C:
			if !c.anyConnectionJobSubscribed() || time.Since(lastPoll) >= safetyInterval {
				if done, err := c.pollJob(ctx, jobID); done || err != nil {
					return err
				}
				lastPoll = time.Now()
			}
		}
	}
}

func (c *Client) pollJob(ctx context.Context, jobID int64) (bool, error) {
	if c.jobPollOnceOverride != nil {
		return c.jobPollOnceOverride(ctx, jobID)
	}
	return c.pollJobOnce(ctx, jobID)
}

func (c *Client) pollJobOnce(ctx context.Context, jobID int64) (bool, error) {
	filters := [][]interface{}{{"id", "=", jobID}}
	result, err := c.Call(ctx, "core.get_jobs", filters)
	if err != nil {
		return false, fmt.Errorf("failed to query job: %w", err)
	}
	jobs, ok := result.([]interface{})
	if !ok {
		return false, fmt.Errorf("unexpected core.get_jobs response type %T", result)
	}
	if len(jobs) == 0 {
		return false, nil
	}
	job, ok := jobs[0].(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("unexpected job response type %T", jobs[0])
	}
	state, _ := job["state"].(string)
	if !terminalJobState(state) {
		return false, nil
	}
	detail := ""
	if message, ok := job["error"].(string); ok && message != "" {
		detail = message
	} else if exception, ok := job["exception"].(string); ok && exception != "" {
		detail = exception
	}
	return true, terminalJobResult(jobEvent{jobID: jobID, state: state, detail: detail})
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

	_, err := c.Call(ctx, c.snapshotMethod("rollback"), snapshotID, options)
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
	normalizeCSIUserProperties(snap.UserProperties)

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
