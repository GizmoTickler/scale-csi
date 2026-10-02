package driver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Diff-first startup. A full startup pass used to take every attached volume's
// lock and re-read and re-check it on its own: about seven TrueNAS calls per
// volume even when nothing had changed while the controller was down. The diff
// reads the fleet once instead (the attached volumes' datasets, by name, and
// the nvmet subsystem, namespace, port_subsys and host_subsys tables) and
// leaves out of
// the per-volume path every volume whose stored records and backend are
// already exactly what that path would make them. Everything else (a
// difference, a doubt, a read that failed, a volume a live CSI operation
// touched while the diff read) takes the per-volume path as before.
//
// The diff only ever skips work; it never writes, grants or revokes. A
// skipped volume is one whose observed state is a fixed point of the
// per-volume path, and any change after that is made by a CSI operation under
// the volume lock, which converges its own volume. It covers strict NVMe-oF
// volumes, the shape of a large fleet; additive mode, NFS and iSCSI take the
// per-volume path.

// startupDiffReadConcurrency bounds the diff's concurrent TrueNAS reads, like
// the per-volume workers, so live CSI calls keep request slots.
const startupDiffReadConcurrency = startupReconcileWorkers

// startupDiffMinNamesPerRead keeps a small re-read in one request: splitting
// it across slots would cost more round trips than it saves.
const startupDiffMinNamesPerRead = 100

// startupLockWatch records every volume lock held or taken while the startup
// diff reads. A volume a live operation touched in that window is not judged
// from the diff's reads: it takes the per-volume path.
type startupLockWatch struct {
	mu      sync.Mutex
	touched map[string]struct{}
}

func (w *startupLockWatch) touch(key string) {
	w.mu.Lock()
	w.touched[key] = struct{}{}
	w.mu.Unlock()
}

func (w *startupLockWatch) wasTouched(key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, touched := w.touched[key]
	return touched
}

// beginStartupLockWatch starts recording lock acquisitions, then records the
// locks already held. A lock taken in between is recorded by its taker.
func (d *Driver) beginStartupLockWatch() *startupLockWatch {
	watch := &startupLockWatch{touched: make(map[string]struct{})}
	d.startupLockWatch.Store(watch)
	d.operationLock.Range(func(key, _ interface{}) bool {
		if name, ok := key.(string); ok {
			watch.touch(name)
		}
		return true
	})
	return watch
}

func (d *Driver) endStartupLockWatch(watch *startupLockWatch) {
	d.startupLockWatch.CompareAndSwap(watch, nil)
}

// startupDiffFleet is the fleet-wide state the diff judges volumes against.
type startupDiffFleet struct {
	datasets       map[string]*truenas.Dataset
	subsystems     map[int]*truenas.NVMeoFSubsystem
	namespaces     map[int]*truenas.NVMeoFNamespace
	portSubsystems map[int]map[int]struct{} // subsystem ID -> port IDs
	hostSubsystems map[int][]*truenas.NVMeoFHostSubsys
	hostNQNs       map[int]string // host ID -> NQN, for associations the backend did not expand
	portIDs        []int          // the configured multipath addresses' ports
}

// startupDiffConverged returns the volumes among volumeIDs that need no
// per-volume convergence. It returns nil (every volume takes the per-volume
// path) when it does not apply or any fleet read fails.
func (d *Driver) startupDiffConverged(
	ctx context.Context,
	volumes map[string]*startupFencingVolume,
	volumeIDs []string,
) map[string]struct{} {
	if d.config == nil || d.config.Fencing.Mode != FencingModeStrict || !d.config.NVMeoF.Enabled {
		return nil
	}
	candidates := make([]string, 0, len(volumeIDs))
	for _, volumeID := range volumeIDs {
		if startupDiffShapeEligible(volumes[volumeID]) {
			candidates = append(candidates, volumeID)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	watch := d.beginStartupLockWatch()
	defer d.endStartupLockWatch(watch)
	fleet, err := d.readStartupDiffFleet(ctx, candidates)
	if err != nil {
		klog.Warningf("Startup fencing diff unavailable, every attached volume is converged on its own: %v", err)
		return nil
	}
	converged := make(map[string]struct{}, len(candidates))
	for _, volumeID := range candidates {
		if watch.wasTouched(volumeLockKey(volumeID)) {
			continue
		}
		if reason := d.startupDiffVolume(ctx, fleet, volumes[volumeID]); reason != "" {
			klog.V(4).Infof("Startup fencing diff: volume %s takes the per-volume path: %s", volumeID, reason)
			continue
		}
		// Judged on reads a live operation may have overlapped: drop it if
		// its lock was taken at any point up to here.
		if watch.wasTouched(volumeLockKey(volumeID)) {
			continue
		}
		converged[volumeID] = struct{}{}
	}
	klog.Infof("Startup fencing diff: %d of %d attached volume(s) already converged", len(converged), len(volumeIDs))
	return converged
}

// startupDiffShapeEligible is whether the diff can judge a volume at all:
// it has an Attached VolumeAttachment, and every node claiming it is Attached
// (one mid-attach or mid-detach is the per-volume path's to settle).
func startupDiffShapeEligible(volume *startupFencingVolume) bool {
	if volume == nil || len(volume.publications) == 0 {
		return false
	}
	if value := volume.volumeAttributes["node_attach_driver"]; value != "" && ParseShareType(value) != ShareTypeNVMeoF {
		return false
	}
	published := make(map[string]struct{}, len(volume.publications))
	for _, publication := range volume.publications {
		published[publication.identity.Name] = struct{}{}
	}
	if len(published) != len(volume.publications) {
		return false
	}
	for node := range volume.claimedNodes {
		if _, ok := published[node]; !ok {
			return false
		}
	}
	return true
}

// readStartupDiffFleet reads the fleet: the candidates' datasets (by name, in
// up to startupDiffReadConcurrency requests; pool.dataset.query carries the
// property sources that tell a volume's own publication records from inherited
// ones), the four nvmet tables, and the configured ports. All reads start
// after the lock watch, so a volume a live operation changes under them is
// recorded as touched.
func (d *Driver) readStartupDiffFleet(ctx context.Context, candidates []string) (*startupDiffFleet, error) {
	names := make([]string, 0, len(candidates))
	for _, volumeID := range candidates {
		if datasetName, err := d.datasetForID(volumeID); err == nil {
			names = append(names, datasetName)
		}
	}
	parts := splitNames(names, startupDiffReadConcurrency, startupDiffMinNamesPerRead)
	datasetParts := make([]map[string]*truenas.Dataset, len(parts))
	var (
		subsystems     []*truenas.NVMeoFSubsystem
		namespaces     []*truenas.NVMeoFNamespace
		portSubsystems []*truenas.NVMeoFPortSubsys
		hostSubsystems []*truenas.NVMeoFHostSubsys
	)
	reads := make([]func() error, 0, 4+len(parts))
	reads = append(reads,
		func() (err error) { namespaces, err = d.truenasClient.NVMeoFNamespaceList(ctx); return err },
		func() (err error) { portSubsystems, err = d.truenasClient.NVMeoFPortSubsysList(ctx); return err },
		func() (err error) { subsystems, err = d.truenasClient.NVMeoFSubsystemList(ctx); return err },
		func() (err error) { hostSubsystems, err = d.truenasClient.NVMeoFHostSubsysList(ctx); return err },
	)
	for i := range parts {
		reads = append(reads, func() (err error) {
			datasetParts[i], err = d.datasetGetByNamesWithin(ctx, parts[i])
			return err
		})
	}
	if err := runBounded(reads, startupDiffReadConcurrency); err != nil {
		return nil, err
	}
	fleet := &startupDiffFleet{
		datasets:       make(map[string]*truenas.Dataset, len(names)),
		subsystems:     make(map[int]*truenas.NVMeoFSubsystem, len(subsystems)),
		namespaces:     make(map[int]*truenas.NVMeoFNamespace, len(namespaces)),
		portSubsystems: make(map[int]map[int]struct{}),
		hostSubsystems: make(map[int][]*truenas.NVMeoFHostSubsys),
	}
	for _, part := range datasetParts {
		for name, dataset := range part {
			if dataset != nil {
				fleet.datasets[name] = dataset
			}
		}
	}
	for _, subsystem := range subsystems {
		if subsystem != nil {
			fleet.subsystems[subsystem.ID] = subsystem
		}
	}
	for _, namespace := range namespaces {
		if namespace != nil {
			fleet.namespaces[namespace.ID] = namespace
		}
	}
	for _, association := range portSubsystems {
		if association == nil {
			continue
		}
		if fleet.portSubsystems[association.SubsysID] == nil {
			fleet.portSubsystems[association.SubsysID] = make(map[int]struct{})
		}
		fleet.portSubsystems[association.SubsysID][association.PortID] = struct{}{}
	}
	unexpanded := false
	for _, association := range hostSubsystems {
		if association == nil {
			continue
		}
		if association.SubsysID <= 0 {
			// An association the diff cannot place could be on any
			// subsystem: no subsystem's allowlist can be judged exact.
			return nil, fmt.Errorf("host_subsys association %d names no subsystem", association.ID)
		}
		if association.HostNQN == "" {
			unexpanded = true
		}
		fleet.hostSubsystems[association.SubsysID] = append(fleet.hostSubsystems[association.SubsysID], association)
	}
	if unexpanded {
		hosts, err := d.truenasClient.NVMeoFHostList(ctx)
		if err != nil {
			return nil, err
		}
		fleet.hostNQNs = make(map[int]string, len(hosts))
		for _, host := range hosts {
			if host != nil {
				fleet.hostNQNs[host.ID] = host.HostNQN
			}
		}
	}
	for _, address := range d.config.NVMeoF.multipathAddresses() {
		// The per-volume path resolves the same ports; the client caches them
		// for the process, so this is a read at most once per address.
		port, err := d.truenasClient.NVMeoFGetOrCreatePort(ctx, d.config.NVMeoF.Transport, address,
			d.config.NVMeoF.TransportServiceID, d.nvmeofPortCreateOpts())
		if err != nil {
			return nil, fmt.Errorf("resolve NVMe-oF port for %s: %w", address, err)
		}
		fleet.portIDs = append(fleet.portIDs, port.ID)
	}
	return fleet, nil
}

// startupDiffVolume returns "" when the per-volume path would change nothing
// for this volume, else why it would.
func (d *Driver) startupDiffVolume(ctx context.Context, fleet *startupDiffFleet, volume *startupFencingVolume) string {
	datasetName, err := d.datasetForID(volume.volumeID)
	if err != nil {
		return "no dataset name"
	}
	dataset := fleet.datasets[datasetName]
	if dataset == nil {
		return "dataset not read"
	}
	if shareTypeForPublishedVolume(dataset, volume.volumeAttributes) != ShareTypeNVMeoF {
		return "not NVMe-oF"
	}

	// Records: exactly one per attached node, each exactly the record the
	// per-volume path would store, none under the stale-record sweep's watch.
	records, err := readPublicationRecordsCached(ctx, d.publications(), datasetName, dataset)
	if err != nil {
		return "publication records unreadable"
	}
	if len(records) != len(volume.publications) {
		return "record set differs from the attached nodes"
	}
	desired := make(map[string]publicationRecord, len(volume.publications))
	desiredNQNs := make(map[string]struct{}, len(volume.publications))
	for _, publication := range volume.publications {
		if deferred, identityErr := d.validateOrDeferFencingIdentity(datasetName, publication.identity, ShareTypeNVMeoF); identityErr != nil || deferred {
			return "node identity not enforceable"
		}
		candidate, recordErr := newPublicationRecord(publication.identity, publication.mode, publication.readonly)
		if recordErr != nil {
			return "node identity not encodable"
		}
		candidate.keepCONodeID(publication.nodeID)
		key := publicationPropertyKey(publication.identity.Name)
		stored, ok := records[key]
		if !ok || !samePublicationRecordExceptTime(stored, candidate) {
			return "stored record differs"
		}
		if _, watched := d.stalePublicationRecordsSeen.Load(stalePublicationObservationKey(datasetName, key)); watched {
			return "record under the stale-record sweep"
		}
		if compatibilityErr := validatePublicationCompatibility(desired, candidate); compatibilityErr != nil {
			return "attached nodes conflict"
		}
		desired[key] = candidate
		desiredNQNs[candidate.NVMeNQN] = struct{}{}
	}

	// Share: the stored IDs name a namespace on this zvol and its subsystem,
	// linked to every configured port.
	namespaceID, err := strconv.Atoi(datasetUserProperty(dataset, PropNVMeoFNamespaceID))
	if err != nil || namespaceID <= 0 {
		return "no stored namespace ID"
	}
	namespace := fleet.namespaces[namespaceID]
	if namespace == nil || normalizedZvolReference(namespace.DevicePath) != normalizedZvolReference("zvol/"+datasetName) {
		return "stored namespace is not this zvol's"
	}
	if datasetUserProperty(dataset, PropNVMeoFSubsystemID) != strconv.Itoa(namespace.SubsystemID) {
		return "stored subsystem ID differs from the namespace's"
	}
	subsystem := fleet.subsystems[namespace.SubsystemID]
	if subsystem == nil || subsystem.Name != d.nvmeSubsystemName(datasetName) {
		return "subsystem missing or misnamed"
	}
	for _, portID := range fleet.portIDs {
		if _, linked := fleet.portSubsystems[subsystem.ID][portID]; !linked {
			return "a configured port is not linked"
		}
	}

	// Fence: closed, and allowing exactly the attached nodes.
	if subsystem.AllowAnyHost {
		return "subsystem allows any host"
	}
	allowed := make(map[string]struct{}, len(desiredNQNs))
	for _, association := range fleet.hostSubsystems[subsystem.ID] {
		nqn := association.HostNQN
		if nqn == "" {
			nqn = fleet.hostNQNs[association.HostID]
		}
		if nqn == "" {
			return "an allowed host cannot be identified"
		}
		if _, want := desiredNQNs[nqn]; !want {
			return "a host outside the attached nodes is allowed"
		}
		allowed[nqn] = struct{}{}
	}
	if len(allowed) != len(desiredNQNs) {
		return "an attached node is not allowed"
	}
	return ""
}

// runBounded runs fns with at most limit at a time and returns their errors
// joined.
func runBounded(fns []func() error, limit int) error {
	if limit < 1 {
		limit = 1
	}
	slots := make(chan struct{}, limit)
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, fn func() error) {
			defer wg.Done()
			defer func() { <-slots }()
			errs[i] = fn()
		}(i, fn)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// splitNames splits names into at most parts slices of at least minPer names
// each (fewer when there are fewer names).
func splitNames(names []string, parts, minPer int) [][]string {
	if len(names) == 0 {
		return nil
	}
	if parts < 1 {
		parts = 1
	}
	per := (len(names) + parts - 1) / parts
	if per < minPer {
		per = minPer
	}
	var out [][]string
	for start := 0; start < len(names); start += per {
		end := start + per
		if end > len(names) {
			end = len(names)
		}
		out = append(out, names[start:end])
	}
	return out
}

// datasetGetByNamesWithin reads names by DatasetGetByNames, in as many
// requests as the request budget needs, one after another. Any failed
// request fails the read.
func (d *Driver) datasetGetByNamesWithin(ctx context.Context, names []string) (map[string]*truenas.Dataset, error) {
	out := make(map[string]*truenas.Dataset, len(names))
	for _, chunk := range chunkDatasetNames(names, datasetGetByNamesBatchBudget) {
		datasets, err := d.truenasClient.DatasetGetByNames(ctx, chunk)
		if err != nil {
			return nil, err
		}
		for name, dataset := range datasets {
			out[name] = dataset
		}
	}
	return out, nil
}
