package driver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type inFlightRemnantAction int

const (
	remnantActionNone inFlightRemnantAction = iota
	remnantActionResume
	remnantActionDestroy
)

// inflightMarker is the durable record, written on the PARENT dataset via the
// proven pool.dataset.update path BEFORE a content-source clone/copy starts,
// that this driver instance owns an in-flight creation of Dataset from the
// recorded source. Crash recovery requires it as POSITIVE proof: absence of
// local properties on a dataset proves nothing (publication records,
// content-source props, operator or configured custom properties, and the
// share-created-before-property-stored window all leave datasets that must
// never be adopted or destroyed).
type inflightMarker struct {
	Version    int    `json:"v"`
	Instance   string `json:"instance"`
	Dataset    string `json:"dataset"`
	Mode       string `json:"mode"`        // inflightModeClone | inflightModeCopy
	SourceType string `json:"source_type"` // "snapshot" | "volume"
	SourceID   string `json:"source_id"`
	// Origin is the exact ZFS origin the destination must report in clone mode
	// (the resolved source snapshot ID, or the deterministic internal
	// clone-source snapshot for volume sources). Empty for detached copies.
	Origin    string `json:"origin,omitempty"`
	Protocol  string `json:"protocol"`
	Nonce     string `json:"nonce"`
	StartedAt string `json:"started_at"`
}

func hashedPropertyKey(prefix, identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return prefix + hex.EncodeToString(sum[:8])
}

// bookkeepingDatasetLeaf is the path leaf of the dedicated bookkeeping child
// dataset. datasetForID rejects it as an inbound volume/snapshot ID so a crafted
// volumeHandle can never target the bookkeeping dataset for delete/expand/clone.
const bookkeepingDatasetLeaf = ".csi-bookkeeping"

// bookkeepingDatasetSuffix names the dedicated child dataset that holds the
// driver's bookkeeping (tombstone ledger + in-flight markers) when the Fix 4b
// relocation is enabled. It is a child of the CSI parent but is NEVER used as a
// volume parent, so its local user properties inherit to nothing — unlike the
// parent, whose properties ZFS copies into every descendant dataset and snapshot
// (the payload bloat measured at 29 MB per snapshot query on production 26.0).
const bookkeepingDatasetSuffix = "/" + bookkeepingDatasetLeaf

func (d *Driver) bookkeepingDatasetName() string {
	return d.parentDatasetName() + bookkeepingDatasetSuffix
}

func (d *Driver) bookkeepingEnabled() bool {
	return d.config != nil && d.config.Reconcile.Bookkeeping.EnabledOrDefault()
}

// ensureBookkeepingDataset lazily and idempotently creates the dedicated
// bookkeeping child dataset on first write.
func (d *Driver) ensureBookkeepingDataset(ctx context.Context) error {
	name := d.bookkeepingDatasetName()
	if _, err := d.truenasClient.DatasetGet(ctx, name); err == nil {
		return nil
	} else if !truenas.IsNotFoundError(err) {
		return err
	}
	if _, err := d.truenasClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: name, Type: "FILESYSTEM"}); err != nil && !truenas.IsAlreadyExistsError(err) {
		return err
	}
	return nil
}

// bookkeepingWriteTarget returns the dataset new bookkeeping entries are written
// to: the dedicated child dataset when the relocation is enabled (created
// lazily), otherwise the parent dataset (v1.2.28 behavior).
func (d *Driver) bookkeepingWriteTarget(ctx context.Context) (string, error) {
	if !d.bookkeepingEnabled() {
		return d.parentDatasetName(), nil
	}
	if err := d.ensureBookkeepingDataset(ctx); err != nil {
		return "", err
	}
	return d.bookkeepingDatasetName(), nil
}

// removeBookkeepingProperties removes bookkeeping property keys from every
// location they may live (parent and, when the relocation is enabled, the
// dedicated child). Removal is idempotent and tolerant of an absent bookkeeping
// dataset, so deleting an entry never fails merely because it predates the
// relocation. The parent is always cleaned so a stale entry cannot linger in a
// location a later read still consults. It returns the first backend error so
// the reconciler sweeps can preserve their confirm-before-forget semantics.
func (d *Driver) removeBookkeepingProperties(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	var firstErr error
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), keys); err != nil {
		firstErr = err
	}
	if d.bookkeepingEnabled() {
		if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.bookkeepingDatasetName(), keys); err != nil && !truenas.IsNotFoundError(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func inflightMarkerKey(volumeID string) string {
	return hashedPropertyKey(PropInflightMarkerPrefix, volumeID)
}

func newRecoveryNonce() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// writeInflightMarker persists and verifies the marker before any backend
// mutation for the destination starts. Failure is fatal to the create by
// design: without durable provenance a crash would leave an unrecoverable,
// invisible remnant, so "no marker, no mutation".
func (d *Driver) writeInflightMarker(ctx context.Context, marker inflightMarker) error {
	encoded, err := json.Marshal(marker)
	if err != nil {
		return status.Errorf(codes.Internal, "encode in-flight creation marker: %v", err)
	}
	key := inflightMarkerKey(path.Base(marker.Dataset))
	target, err := d.bookkeepingWriteTarget(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "resolve bookkeeping dataset for in-flight marker: %v", err)
	}
	if _, err := d.setAndVerifyDatasetUserProperties(ctx, target, map[string]string{key: string(encoded)}); err != nil {
		return status.Errorf(codes.Internal,
			"record in-flight creation marker for %s on %s: %v", marker.Dataset, target, err)
	}
	return nil
}

// readInflightMarker returns this driver's marker for volumeID, or nil when no
// valid local marker exists. Non-local values and unparseable payloads are
// treated as absent for recovery (never a license to act) and left for the
// reconciler sweep.
func (d *Driver) readInflightMarker(ctx context.Context, volumeID string) (*inflightMarker, error) {
	key := inflightMarkerKey(volumeID)
	// Dual-read: new markers live on the dedicated bookkeeping dataset; legacy
	// markers remain on the parent until cleaned up. Consult the bookkeeping
	// dataset first, then fall back to the parent so a marker written before (or
	// during) the relocation is found regardless of location.
	if d.bookkeepingEnabled() {
		bookkeeping, err := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
		if err != nil && !truenas.IsNotFoundError(err) {
			return nil, fmt.Errorf("read bookkeeping dataset for in-flight markers: %w", err)
		}
		if marker := parseInflightMarker(bookkeeping, key, volumeID); marker != nil {
			return marker, nil
		}
	}
	parent, err := d.truenasClient.DatasetGet(ctx, d.parentDatasetName())
	if err != nil {
		return nil, fmt.Errorf("read parent dataset for in-flight markers: %w", err)
	}
	return parseInflightMarker(parent, key, volumeID), nil
}

// parseInflightMarker extracts a valid local in-flight marker for key from a
// dataset, or nil when absent, non-local, unparseable, or the wrong version.
func parseInflightMarker(ds *truenas.Dataset, key, volumeID string) *inflightMarker {
	property, ok := datasetUserPropertyProjection(ds, key)
	if !ok || !isLocalUserPropertySource(property.Source) {
		return nil
	}
	var marker inflightMarker
	if err := json.Unmarshal([]byte(property.Value), &marker); err != nil {
		klog.Warningf("Ignoring unparseable in-flight marker for %s: %v", volumeID, err)
		return nil
	}
	if marker.Version != inflightMarkerVersion {
		return nil
	}
	return &marker
}

// newInflightMarker builds the base marker (clone mode by default; the caller
// switches to copy mode and/or records the expected origin).
func (d *Driver) newInflightMarker(datasetName string, source *csi.VolumeContentSource, shareType ShareType) (inflightMarker, error) {
	sourceType, sourceID, ok := volumeContentSourceIdentity(source)
	if !ok {
		return inflightMarker{}, status.Error(codes.InvalidArgument, "volume content source is missing an identity")
	}
	nonce, err := newRecoveryNonce()
	if err != nil {
		return inflightMarker{}, status.Errorf(codes.Internal, "generate in-flight marker nonce: %v", err)
	}
	return inflightMarker{
		Version:    inflightMarkerVersion,
		Instance:   d.driverInstanceID(),
		Dataset:    datasetName,
		Mode:       inflightModeClone,
		SourceType: sourceType,
		SourceID:   sourceID,
		Protocol:   string(shareType),
		Nonce:      nonce,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

// deleteInflightMarker is best-effort: a leftover marker is retired by the
// reconciler sweep once its dataset is stamped or gone.
func (d *Driver) deleteInflightMarker(ctx context.Context, volumeID string) {
	if err := d.removeBookkeepingProperties(ctx, []string{inflightMarkerKey(volumeID)}); err != nil {
		klog.Warningf("Failed to remove in-flight creation marker for %s (reconciler sweep will retire it): %v", volumeID, err)
	}
}

// recoverInFlightContentSourceRemnant restores crash self-healing for a
// content-source CreateVolume whose destination was created but crashed before
// ownership was stamped. Recovery is gated on POSITIVE durable proof — a
// matching parent-dataset in-flight marker written by this driver instance
// before the clone/copy started — never on absence of evidence. Without a
// matching marker every existing dataset stays on the terminal AlreadyExists
// path, which protects foreign datasets, operator/DR datasets at colliding
// names, datasets whose backend share exists but whose share-ID property write
// failed, and datasets carrying only publication-record or custom local
// properties.
//
// Compatibility with the CURRENT request is validated BEFORE any mutation:
// source identity (marker source vs the request), protocol (marker protocol vs
// the requested share type; dataset-type/protocol mismatches were already
// rejected by the checks that run before this call), and capacity (remnant
// capacity vs the request's limit). An incompatible request keeps AlreadyExists
// exactly as CSI requires and never receives a resumed OK.
//
//   - CLONE mode: the remnant's ZFS origin must equal the origin recorded in
//     the marker (double provenance proof) and the remnant must have no
//     snapshots of its own. RESUME: normalize capacity, then stamp the full
//     local identity set through the update+verify path with a per-attempt
//     nonce CAS — two overlapping controller processes can both reach this
//     point, and only the process whose nonce survives the re-read proceeds;
//     the loser returns Aborted.
//   - COPY mode (detached): completeness of an interrupted send/receive cannot
//     be proven, so DESTROY and return Aborted for a clean recreate. The real
//     crash shape leaves the transferred replication snapshot on the target
//     (replication creates target+snapshot; snapshot cleanup runs only after
//     the job completes), so the destroy is recursive — sanctioned exclusively
//     by the marker's proof of ownership.
func (d *Driver) recoverInFlightContentSourceRemnant(
	ctx context.Context,
	existingDS *truenas.Dataset,
	datasetName, name string,
	source *csi.VolumeContentSource,
	capacityBytes, limitBytes int64,
	shareType ShareType,
	detached bool,
) (*truenas.Dataset, inFlightRemnantAction, error) {
	if source == nil || existingDS == nil || existingDS.Name != datasetName {
		return existingDS, remnantActionNone, nil
	}
	volumeID := path.Base(datasetName)
	// A dataset with a LOCAL ownership stamp is not an in-flight remnant: ours
	// continues through the normal existing-volume path (retiring a stale marker
	// left by a crash between stamp and marker delete); foreign is never touched.
	if owner, ok := datasetUserPropertyProjection(existingDS, PropDriverInstanceID); ok && isLocalUserPropertySource(owner.Source) {
		if owner.Value == d.driverInstanceID() {
			d.deleteInflightMarker(ctx, volumeID)
		}
		return existingDS, remnantActionNone, nil
	}
	marker, err := d.readInflightMarker(ctx, volumeID)
	if err != nil {
		return existingDS, remnantActionNone, status.Errorf(codes.Internal,
			"inspect in-flight creation marker for %s: %v", datasetName, err)
	}
	if marker == nil || marker.Instance != d.driverInstanceID() || marker.Dataset != datasetName {
		return existingDS, remnantActionNone, nil
	}
	// Content-source compatibility: the marker must describe exactly the source
	// this request asks for. A different source is an incompatible request and
	// must keep the terminal AlreadyExists outcome.
	requestType, requestID, ok := volumeContentSourceIdentity(source)
	if !ok || marker.SourceType != requestType || marker.SourceID != requestID {
		return existingDS, remnantActionNone, nil
	}

	switch marker.Mode {
	case inflightModeCopy:
		if !detached || source.GetSnapshot() == nil {
			return existingDS, remnantActionNone, nil
		}
		// The same request-compatibility validation as the clone branch runs
		// BEFORE the destroy: an incompatible request must keep the terminal
		// AlreadyExists outcome with the remnant untouched, never trigger a
		// destroy-and-recreate on behalf of a differently-shaped request.
		if marker.Protocol != string(shareType) {
			return existingDS, remnantActionNone, nil
		}
		if existingCapacity := d.getDatasetCapacity(existingDS); limitBytes > 0 && existingCapacity > limitBytes {
			return existingDS, remnantActionNone, status.Errorf(codes.AlreadyExists,
				"in-flight remnant for volume %s has capacity %d bytes, greater than requested capacity limit %d bytes",
				volumeID, existingCapacity, limitBytes)
		}
		// Marker-proven interrupted copy: the recursive destroy also removes the
		// transferred replication snapshot the crash left on the target.
		if delErr := d.truenasClient.DatasetDelete(ctx, datasetName, true, true); delErr != nil {
			return existingDS, remnantActionNone, status.Errorf(codes.Internal,
				"destroy marker-proven interrupted detached-copy remnant %s: %v", datasetName, delErr)
		}
		d.deleteInflightMarker(ctx, volumeID)
		klog.Warningf("Destroyed marker-proven interrupted detached-copy remnant %s; retry will recreate it cleanly", datasetName)
		return nil, remnantActionDestroy, nil

	case inflightModeClone:
		// The current resolution must still take the clone path for this source;
		// after a StorageClass/class default flip the remnant is left for the
		// operator.
		if detached && source.GetSnapshot() != nil {
			return existingDS, remnantActionNone, nil
		}
		// Protocol compatibility before any mutation.
		if marker.Protocol != string(shareType) {
			return existingDS, remnantActionNone, nil
		}
		origin := datasetOriginSnapshotID(existingDS)
		if origin == "" || marker.Origin == "" || origin != marker.Origin {
			return existingDS, remnantActionNone, nil
		}
		// A just-cloned remnant owns no snapshots; anything else is not a
		// half-object this recovery understands.
		snapshots, listErr := d.truenasClient.SnapshotList(ctx, datasetName)
		if listErr != nil {
			return existingDS, remnantActionNone, status.Errorf(codes.Internal,
				"inspect in-flight clone remnant %s for snapshots: %v", datasetName, listErr)
		}
		if len(snapshots) > 0 {
			return existingDS, remnantActionNone, nil
		}
		// Capacity compatibility BEFORE any mutation: a request whose limit the
		// remnant already exceeds is incompatible per CSI.
		if existingCapacity := d.getDatasetCapacity(existingDS); limitBytes > 0 && existingCapacity > limitBytes {
			return existingDS, remnantActionNone, status.Errorf(codes.AlreadyExists,
				"in-flight remnant for volume %s has capacity %d bytes, greater than requested capacity limit %d bytes",
				volumeID, existingCapacity, limitBytes)
		}
		completed, resumeErr := d.completeResumedCloneRemnant(ctx, existingDS, datasetName, name, source, capacityBytes, shareType)
		if resumeErr != nil {
			return existingDS, remnantActionNone, resumeErr
		}
		d.deleteInflightMarker(ctx, volumeID)
		klog.Infof("Resumed marker-proven in-flight clone remnant %s (origin %s) after a crash before ownership was stamped", datasetName, origin)
		return completed, remnantActionResume, nil
	}
	return existingDS, remnantActionNone, nil
}

// completeResumedCloneRemnant finishes a proven clone remnant: it normalizes the
// clone's capacity and stamps the full local identity set (ownership, managed
// markers, content source, and for volume clones the origin snapshot) through the
// proven update+verify path, so the volume no longer depends on any
// clone-inherited (non-local) value. The stamp carries a per-attempt nonce:
// operationLock is per-process, so two overlapping controllers can both attempt
// this stamp; each writes its full payload in a single update and the re-read
// decides whose write won. The loser maps the verification failure to Aborted —
// its retry re-enters through the normal existing-volume path, where the
// winner's identical-instance stamp is simply idempotent. The caller then
// finishes through the normal existing-dataset tail (idempotent share creation
// and the response).
func (d *Driver) completeResumedCloneRemnant(
	ctx context.Context,
	existingDS *truenas.Dataset,
	datasetName, name string,
	source *csi.VolumeContentSource,
	capacityBytes int64,
	shareType ShareType,
) (*truenas.Dataset, error) {
	if err := d.ensureCloneCapacity(ctx, datasetName, existingDS, capacityBytes); err != nil {
		return nil, err
	}
	nonce, nonceErr := newRecoveryNonce()
	if nonceErr != nil {
		return nil, status.Errorf(codes.Internal, "generate recovery nonce: %v", nonceErr)
	}
	properties := map[string]string{
		PropManagedResource:  "true",
		PropDriverInstanceID: d.driverInstanceID(),
		PropProvisionSuccess: "true",
		PropCSIVolumeName:    name,
		PropRecoveryNonce:    nonce,
	}
	if snapshot := source.GetSnapshot(); snapshot != nil {
		properties[PropVolumeContentSourceType] = "snapshot"
		properties[PropVolumeContentSourceID] = snapshot.GetSnapshotId()
	} else if volume := source.GetVolume(); volume != nil {
		sourceDataset, err := d.datasetForID(volume.GetVolumeId())
		if err != nil {
			return nil, err
		}
		tempSnapshotName := fmt.Sprintf("clone-source-%s", d.sanitizeVolumeID(path.Base(datasetName)))
		properties[PropVolumeContentSourceType] = "volume"
		properties[PropVolumeContentSourceID] = volume.GetVolumeId()
		properties[PropVolumeOriginSnapshot] = sourceDataset + "@" + tempSnapshotName
	}
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		properties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
	verified, err := d.setAndVerifyDatasetUserProperties(ctx, datasetName, properties)
	if err != nil {
		if errors.Is(err, errDatasetPropertyVerification) {
			// Another controller's recovery stamp overwrote ours between our write
			// and re-read. Its (same-instance) stamp is authoritative; retry through
			// the normal existing-volume path.
			return nil, status.Errorf(codes.Aborted,
				"lost the in-flight remnant recovery race for %s; retry CreateVolume", datasetName)
		}
		return nil, status.Errorf(codes.Internal, "stamp and verify resumed clone remnant ownership: %v", err)
	}
	if removeErr := d.truenasClient.DatasetRemoveUserProperties(ctx, datasetName, []string{PropRecoveryNonce}); removeErr != nil {
		klog.Warningf("Failed to remove recovery nonce from %s (inert leftover): %v", datasetName, removeErr)
	}
	delete(verified.UserProperties, PropRecoveryNonce)
	return verified, nil
}

// tombstoneLedgerEntry is the parent-dataset record proving that THIS driver
// tombstoned the named snapshot. It is written BEFORE the rename so the orphan
// reaper only ever destroys snapshots with proven driver provenance; a
// name-shaped lookalike (e.g. a user snapshot literally named *-csi-deleted-N)
// has no ledger entry and is never touched. A crash between ledger write and
// rename leaves an entry without a tombstone, which the reconciler sweeps.
type tombstoneLedgerEntry struct {
	Version  int    `json:"v"`
	Snapshot string `json:"snapshot"` // full tombstone ID: dataset@tombstone-name
	Dataset  string `json:"dataset"`
	// CreatedAt is the tombstoned snapshot's immutable ZFS creation time (unix
	// seconds), captured at ledger-write time. The reaper requires the observed
	// snapshot's creation time to MATCH, so a stale ledger entry can never
	// authorize reaping a different snapshot later recreated at the same full ID.
	CreatedAt int64  `json:"created_at,omitempty"`
	RenamedAt string `json:"renamed_at"`
}

func tombstoneLedgerKey(fullSnapshotID string) string {
	return hashedPropertyKey(PropTombstoneLedgerPrefix, fullSnapshotID)
}

func (d *Driver) writeTombstoneLedgerEntry(ctx context.Context, entry tombstoneLedgerEntry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode tombstone ledger entry: %w", err)
	}
	target, err := d.bookkeepingWriteTarget(ctx)
	if err != nil {
		return fmt.Errorf("resolve bookkeeping dataset for tombstone ledger: %w", err)
	}
	if _, err := d.setAndVerifyDatasetUserProperties(ctx, target, map[string]string{
		tombstoneLedgerKey(entry.Snapshot): string(encoded),
	}); err != nil {
		return fmt.Errorf("record tombstone ledger entry for %s on %s: %w", entry.Snapshot, target, err)
	}
	return nil
}

// removeTombstoneLedgerEntry is best-effort; an orphaned entry (its snapshot no
// longer exists) is retired by the reconciler sweep.
func (d *Driver) removeTombstoneLedgerEntry(ctx context.Context, fullSnapshotID string) {
	if err := d.removeBookkeepingProperties(ctx, []string{tombstoneLedgerKey(fullSnapshotID)}); err != nil {
		klog.Warningf("Failed to remove tombstone ledger entry for %s (reconciler sweep will retire it): %v", fullSnapshotID, err)
	}
}

// migrateParentBookkeeping copies the parent dataset's local bookkeeping
// properties (tombstone ledger + in-flight markers) to the dedicated bookkeeping
// child dataset. It is idempotent and re-runnable: a copy that already landed is
// simply overwritten with an identical value. When CleanupParent is enabled it
// then removes the just-copied entries from the parent — strictly AFTER a
// confirmed copy, so the migration is lossless (a parent entry is never deleted
// unless its copy succeeded). Until CleanupParent is enabled the parent entries
// stay in place and the dual-read serves them from there, so enabling the
// relocation is safe even mid-rollout with older controllers reading the parent.
func (d *Driver) migrateParentBookkeeping(ctx context.Context, parent *truenas.Dataset) {
	if parent == nil || !d.bookkeepingEnabled() {
		return
	}
	toCopy := make(map[string]string)
	for key, property := range parent.UserProperties {
		if !isLocalUserPropertySource(property.Source) {
			continue
		}
		if strings.HasPrefix(key, PropTombstoneLedgerPrefix) || strings.HasPrefix(key, PropInflightMarkerPrefix) {
			toCopy[key] = property.Value
		}
	}
	if len(toCopy) == 0 {
		return
	}
	if err := d.ensureBookkeepingDataset(ctx); err != nil {
		d.recordReconcileObjectFailure("bookkeeping_migration", d.bookkeepingDatasetName(), err)
		return
	}
	// Skip entries whose identical copy already landed on the bookkeeping
	// dataset (a prior pass, possibly one that ended ambiguous). Without this
	// the steady-state cleanupParent=false configuration would re-copy the
	// whole backlog every pass.
	bookkeeping, err := d.truenasClient.DatasetGet(ctx, d.bookkeepingDatasetName())
	if err != nil && !truenas.IsNotFoundError(err) {
		d.recordReconcileObjectFailure("bookkeeping_migration", d.bookkeepingDatasetName(), err)
		return
	}
	pending := make(map[string]string, len(toCopy))
	for key, value := range toCopy {
		if bookkeeping != nil {
			if property, ok := bookkeeping.UserProperties[key]; ok &&
				isLocalUserPropertySource(property.Source) && property.Value == value {
				continue
			}
		}
		pending[key] = value
	}
	// Copy in batches bounded well under TrueNAS's 64 kB WebSocket inbound
	// message limit (server closes 1009 on oversized requests — observed live
	// on 26.0 with a ~300-entry single-call migration).
	for _, batch := range chunkUserProperties(pending, bookkeepingMigrationBatchBudget) {
		if err := d.truenasClient.DatasetSetUserProperties(ctx, d.bookkeepingDatasetName(), batch); err != nil {
			d.recordReconcileObjectFailure("bookkeeping_migration", d.bookkeepingDatasetName(), err)
			return
		}
	}
	if !d.config.Reconcile.Bookkeeping.CleanupParent {
		return
	}
	keys := make([]string, 0, len(toCopy))
	for key := range toCopy {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if err := d.truenasClient.DatasetRemoveUserProperties(ctx, d.parentDatasetName(), keys); err != nil {
		d.recordReconcileObjectFailure("bookkeeping_migration", d.parentDatasetName(), err)
		return
	}
	klog.Infof("Orphan reconcile: migrated %d bookkeeping entries off the parent dataset", len(keys))
}

// tombstoneLedgerFromDataset extracts the local tombstone ledger entries from
// the parent dataset, keyed by property key. Non-local and unparseable values
// are ignored (never grounds to reap anything).
func tombstoneLedgerFromDataset(parent *truenas.Dataset) map[string]tombstoneLedgerEntry {
	entries := make(map[string]tombstoneLedgerEntry)
	if parent == nil {
		return entries
	}
	for key, property := range parent.UserProperties {
		if !strings.HasPrefix(key, PropTombstoneLedgerPrefix) || !isLocalUserPropertySource(property.Source) {
			continue
		}
		var entry tombstoneLedgerEntry
		if err := json.Unmarshal([]byte(property.Value), &entry); err != nil {
			klog.Warningf("Ignoring unparseable tombstone ledger entry %s: %v", key, err)
			continue
		}
		if entry.Version != tombstoneLedgerVersion || entry.Snapshot == "" || key != tombstoneLedgerKey(entry.Snapshot) {
			continue
		}
		entries[key] = entry
	}
	return entries
}

// handleSnapshotClones tombstones a snapshot and asks ZFS to destroy it once
// its last clone releases the dependency.
func (d *Driver) handleSnapshotClones(ctx context.Context, snap *truenas.Snapshot) error {
	tombstoneName := snapshotTombstoneName(snap.Dataset, snap.Name, time.Now().UnixNano())
	deleteID := snap.Dataset + "@" + tombstoneName
	// Durable provenance BEFORE the rename: the reaper may only ever destroy
	// snapshots this ledger proves the driver tombstoned. The immutable creation
	// time binds the entry to THIS snapshot, not any later object at the same ID.
	if err := d.writeTombstoneLedgerEntry(ctx, tombstoneLedgerEntry{
		Version:   tombstoneLedgerVersion,
		Snapshot:  deleteID,
		Dataset:   snap.Dataset,
		CreatedAt: snap.GetCreationTime(),
		RenamedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to record tombstone provenance for snapshot %s: %v", snap.ID, err)
	}
	if renameErr := d.truenasClient.SnapshotRename(ctx, snap.ID, tombstoneName); renameErr != nil {
		// The error may be a timeout AFTER the rename actually landed. Retire the
		// ledger entry ONLY when the original name is observably still present
		// (the rename provably did not happen); when the tombstone name exists,
		// the rename succeeded and the entry must survive — it is the reaper's
		// only provenance for the now-unnamed tombstone. When neither is
		// observable, keep the entry: the age-gated sweep retires a false one.
		if _, tombErr := d.truenasClient.SnapshotGet(ctx, deleteID); tombErr == nil {
			klog.Warningf("Snapshot rename to %s reported %v but the tombstone exists; keeping its ledger entry", deleteID, renameErr)
		} else if _, origErr := d.truenasClient.SnapshotGet(ctx, snap.ID); origErr == nil {
			d.removeTombstoneLedgerEntry(ctx, deleteID)
		}
		return status.Errorf(codes.Internal, "failed to tombstone snapshot %s before deferred deletion: %v", snap.ID, renameErr)
	}

	properties := []string{PropManagedResource, PropCSISnapshotName, PropCSISnapshotSourceVolumeID}
	if err := d.truenasClient.SnapshotRemoveUserProperties(ctx, deleteID, properties); err != nil {
		klog.Warningf("Failed to strip CSI properties from deferred snapshot %s: %v", deleteID, err)
	}
	if err := d.truenasClient.SnapshotDelete(ctx, deleteID, true, false); err != nil {
		if truenas.IsNotFoundError(err) {
			d.removeTombstoneLedgerEntry(ctx, deleteID)
			return nil
		}
		// TrueNAS 26.0's zfs.resource.snapshot.destroy has no deferred-destroy
		// mode, so a tombstone that still has a live restored clone cannot be
		// removed yet. The tombstone rename already released the CSI snapshot name,
		// which is DeleteSnapshot's entire contract, so this is a success — leave
		// the tombstone (and its ledger entry) for the orphan reconciler to reap
		// once its last clone is gone. Returning an error here would surface a
		// spurious Internal and (once the CO's retry sees the renamed-away CSI
		// name as absent) leak the tombstone forever.
		var cloneErr *truenas.ErrSnapshotHasClones
		if errors.As(err, &cloneErr) {
			klog.V(4).Infof("Tombstoned snapshot %s still has dependent clones; deferring reclamation to orphan reconcile", deleteID)
			return nil
		}
		return status.Errorf(codes.Internal, "failed to defer snapshot deletion: %v", err)
	}
	// The backend accepted the (deferred or immediate) destroy; the ledger entry
	// has served its purpose.
	d.removeTombstoneLedgerEntry(ctx, deleteID)
	return nil
}

func snapshotTombstoneName(dataset, name string, nonce int64) string {
	const maxZFSSnapshotNameLength = 255
	suffix := snapshotTombstoneMarker + strconv.FormatInt(nonce, 10)
	maxShortNameLength := maxZFSSnapshotNameLength - len(dataset) - 1
	maxOriginalNameLength := maxShortNameLength - len(suffix)
	if maxOriginalNameLength < 0 {
		maxOriginalNameLength = 0
	}
	if len(name) > maxOriginalNameLength {
		name = name[:maxOriginalNameLength]
	}
	return name + suffix
}

func (d *Driver) prepareDetachedSnapshotCopy(
	ctx context.Context,
	datasetName string,
	ds *truenas.Dataset,
	volumeName string,
	snapshotID string,
	snapshotShortName string,
	capacityBytes int64,
	shareType ShareType,
) (*truenas.Dataset, error) {
	if err := d.truenasClient.DestroyReplicatedTargetSnapshot(ctx, datasetName, snapshotShortName); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove transferred snapshot from detached copy: %v", err)
	}

	if err := d.ensureCloneCapacity(ctx, datasetName, ds, capacityBytes); err != nil {
		return nil, err
	}

	// Refresh after a possible expansion and use the target's actual properties,
	// not the source values returned by the replication receive.
	refreshed, err := d.truenasClient.DatasetGet(ctx, datasetName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to verify detached snapshot copy: %v", err)
	}

	updates := &truenas.DatasetUpdateParams{}
	needsUpdate := false
	switch refreshed.Type {
	case "VOLUME":
		volsize := datasetPropertyBytes(refreshed.Volsize)
		if volsize < capacityBytes {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy has volsize %d bytes, less than requested %d bytes",
				volsize, capacityBytes)
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.ZvolEnableReservation {
			desiredRefreservation = volsize
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			updates.Refreservation = desiredRefreservation
			needsUpdate = true
		}
	case "FILESYSTEM":
		desiredRefquota := int64(0)
		if d.config.ZFS.DatasetEnableQuotas {
			desiredRefquota = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refquota) != desiredRefquota {
			updates.Refquota = desiredRefquota
			needsUpdate = true
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.DatasetEnableReservation {
			desiredRefreservation = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			updates.Refreservation = desiredRefreservation
			needsUpdate = true
		}
	default:
		return nil, status.Errorf(codes.Internal,
			"detached snapshot copy %s has unsupported dataset type %q", datasetName, refreshed.Type)
	}

	if needsUpdate {
		if _, updateErr := d.truenasClient.DatasetUpdate(ctx, datasetName, updates); updateErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to apply detached snapshot copy capacity properties: %v", updateErr)
		}
		refreshed, err = d.truenasClient.DatasetGet(ctx, datasetName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to verify detached snapshot copy properties: %v", err)
		}
	}

	switch refreshed.Type {
	case "VOLUME":
		volsize := datasetPropertyBytes(refreshed.Volsize)
		desiredRefreservation := int64(0)
		if d.config.ZFS.ZvolEnableReservation {
			desiredRefreservation = volsize
		}
		if datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy has refreservation %d bytes, expected %d bytes",
				datasetPropertyBytes(refreshed.Refreservation), desiredRefreservation)
		}
	case "FILESYSTEM":
		desiredRefquota := int64(0)
		if d.config.ZFS.DatasetEnableQuotas {
			desiredRefquota = capacityBytes
		}
		desiredRefreservation := int64(0)
		if d.config.ZFS.DatasetEnableReservation {
			desiredRefreservation = capacityBytes
		}
		if datasetPropertyBytes(refreshed.Refquota) != desiredRefquota ||
			datasetPropertyBytes(refreshed.Refreservation) != desiredRefreservation {
			return nil, status.Errorf(codes.Internal,
				"detached snapshot copy quota properties do not match the requested configuration")
		}
	}

	// A replication receive inherits source user properties. Apply configured
	// user properties first, then overwrite every volume identity field that
	// belongs to the new CSI volume.
	identityProperties := make(map[string]string)
	for rawKey, rawValue := range d.config.ZFS.DatasetProperties {
		key := strings.TrimSpace(rawKey)
		if strings.Contains(key, ":") {
			identityProperties[key] = strings.TrimSpace(rawValue)
		}
	}
	identityProperties[PropCSIVolumeName] = volumeName
	identityProperties[PropDriverInstanceID] = d.driverInstanceID()
	identityProperties[PropVolumeContentSourceType] = "snapshot"
	identityProperties[PropVolumeContentSourceID] = snapshotID
	identityProperties[PropVolumeOriginSnapshot] = "-"
	// Share identifiers belong to the source dataset's TrueNAS database
	// objects and must never drive the target's create/retry path.
	identityProperties[PropNFSShareID] = "-"
	identityProperties[PropISCSITargetID] = "-"
	identityProperties[PropISCSIExtentID] = "-"
	identityProperties[PropISCSITargetExtentID] = "-"
	identityProperties[PropNVMeoFSubsystemID] = "-"
	identityProperties[PropNVMeoFNamespaceID] = "-"
	identityProperties[PropNVMeoFPortSubsysID] = "-"
	if shareType == ShareTypeNFS && !d.config.ZFS.DatasetEnableQuotas {
		identityProperties[PropRequestedSizeBytes] = strconv.FormatInt(capacityBytes, 10)
	}
	if err := d.setDatasetUserProperties(ctx, refreshed, datasetName, identityProperties); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to overwrite detached snapshot copy identity properties: %v", err)
	}
	return refreshed, nil
}

// bookkeepingMigrationBatchBudget bounds the approximate JSON payload of a
// single migration DatasetSetUserProperties call. TrueNAS's WebSocket server
// rejects inbound messages over 64 kB (close 1009); half that leaves headroom
// for the RPC envelope and dataset name.
const bookkeepingMigrationBatchBudget = 32 * 1024

// chunkUserProperties splits properties into deterministic (sorted-key)
// batches whose approximate encoded size stays within budget. A single
// oversized entry still gets its own batch — the caller surfaces the backend
// error rather than silently dropping the entry.
func chunkUserProperties(properties map[string]string, budget int) []map[string]string {
	if len(properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var batches []map[string]string
	current := make(map[string]string)
	size := 0
	for _, key := range keys {
		entrySize := len(key) + len(properties[key]) + 64
		if len(current) > 0 && size+entrySize > budget {
			batches = append(batches, current)
			current = make(map[string]string)
			size = 0
		}
		current[key] = properties[key]
		size += entrySize
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches
}
