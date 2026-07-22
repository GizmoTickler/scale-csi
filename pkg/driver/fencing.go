package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

const (
	publicationPropertyPrefix = "truenas-csi:publication_"
	publicationRecordVersion  = 1
	publicationStatePublished = "published"
	publicationStateRemoving  = "unpublishing"

	// additiveGrantHardCap bounds a single node's CSI-added grant provenance list
	// so identity churn (per-boot NQNs, DHCP-rotated IPs) can never grow one ZFS
	// user property past the backend's value limit and break every publish for
	// the volume. Compaction (dropping entries no longer on the backend) keeps
	// the list small in practice; a list that still exceeds the cap after
	// compaction consists entirely of backend-live grants, and dropping any of
	// those would permanently orphan a revocable grant — so the publish FAILS
	// (ResourceExhausted) instead of silently evicting.
	additiveGrantHardCap = 32
)

// isLocalUserPropertySource reports whether a ZFS user-property source marks the
// value as set directly on this dataset. This must be an exact "local" match:
// ZFS clone-inherited user properties do NOT report "inherited"; their source is
// the ORIGIN SNAPSHOT NAME (e.g. "tank/src@snap"), so a substring "inherit"
// filter lets a clone parse the source volume's publication records as its own.
func isLocalUserPropertySource(source string) bool {
	return strings.EqualFold(strings.TrimSpace(source), "local")
}

// compactAdditiveGrants bounds a per-node additive provenance list against
// unbounded growth. It keeps exactly the entries that are still meaningful — the
// node's current-identity grants (currentGrants) or entries still present on the
// backend allowlist read at write time (backendAllowlist) — and drops stale
// entries a prior convergence already removed from the backend. It NEVER drops a
// backend-live entry: every retained entry is a grant scale-csi may still need
// to revoke, and evicting one would turn it into unrevocable "static policy".
// Entries keep stable first-seen order (previous list order first, then new
// current grants); dedup is uniform across both inputs. overCap reports that
// even the fully compacted list exceeds additiveGrantHardCap — the caller must
// fail the operation rather than lose provenance.
func compactAdditiveGrants(previous, currentGrants, backendAllowlist []string) (compacted []string, overCap bool) {
	keep := make(map[string]struct{}, len(currentGrants)+len(backendAllowlist))
	for _, group := range [][]string{currentGrants, backendAllowlist} {
		for _, value := range group {
			if value = strings.TrimSpace(value); value != "" {
				keep[value] = struct{}{}
			}
		}
	}
	ordered := make([]string, 0, len(previous)+len(currentGrants))
	seen := make(map[string]struct{}, len(previous)+len(currentGrants))
	for _, value := range previous {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, dup := seen[value]; dup {
			continue
		}
		if _, live := keep[value]; !live {
			continue
		}
		seen[value] = struct{}{}
		ordered = append(ordered, value)
	}
	for _, value := range currentGrants {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		ordered = append(ordered, value)
	}
	return ordered, len(ordered) > additiveGrantHardCap
}

var (
	errFenceBackendAbsent = errors.New("fencing backend object is absent")
	errFenceDeferred      = errors.New("backend fencing is deferred")
)

// publicationRecord is recovery metadata, not a second source of publication
// truth: the transport allowlist is authoritative. Its purpose is to make an
// interrupted unpublish retryable after the Kubernetes Node and its encoded
// node_id have disappeared. State is written to "unpublishing" before access is
// removed, so a controller restart can never accidentally re-add that node.
type publicationRecord struct {
	Version    int      `json:"v"`
	Node       string   `json:"node"`
	EncodedID  string   `json:"node_id,omitempty"`
	NVMeNQN    string   `json:"nvme_nqn,omitempty"`
	ISCSIIQN   string   `json:"iscsi_iqn,omitempty"`
	IPs        []string `json:"ips,omitempty"`
	State      string   `json:"state"`
	AccessMode int32    `json:"access_mode"`
	Readonly   bool     `json:"readonly,omitempty"`
	UpdatedAt  string   `json:"updated_at"`

	// Additive mode may revoke only grants that scale-csi can prove it added.
	// These optional fields make version-1 records backward compatible: an old
	// record without provenance is treated conservatively as static ownership.
	CSIAddedNFSHosts []string `json:"csi_added_nfs_hosts,omitempty"`
	CSIAddedNVMeNQNs []string `json:"csi_added_nvme_nqns,omitempty"`
}

func publicationPropertyKey(nodeName string) string {
	sum := sha256.Sum256([]byte(nodeName))
	return publicationPropertyPrefix + hex.EncodeToString(sum[:8])
}

func newPublicationRecord(identity NodeIdentity, mode csi.VolumeCapability_AccessMode_Mode, readonly bool) (publicationRecord, error) {
	encoded, err := encodeNodeIdentity(identity)
	if err != nil {
		return publicationRecord{}, err
	}
	record := publicationRecord{
		Version:    publicationRecordVersion,
		Node:       identity.Name,
		EncodedID:  encoded,
		NVMeNQN:    identity.NVMeNQN,
		ISCSIIQN:   identity.ISCSIIQN,
		State:      publicationStatePublished,
		AccessMode: int32(mode),
		Readonly:   readonly,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	for _, ip := range canonicalNodeIPs(identity.IPs) {
		record.IPs = append(record.IPs, ip.String())
	}
	return record, nil
}

func (r publicationRecord) identity() NodeIdentity {
	identity := NodeIdentity{Name: r.Node, NVMeNQN: r.NVMeNQN, ISCSIIQN: r.ISCSIIQN}
	for _, value := range r.IPs {
		if ip := net.ParseIP(value); ip != nil {
			identity.IPs = append(identity.IPs, ip)
		}
	}
	identity.IPs = canonicalNodeIPs(identity.IPs)
	return identity
}

func samePublicationRecordGeneration(left, right publicationRecord) bool {
	return left.Version == right.Version && left.Node == right.Node &&
		left.EncodedID == right.EncodedID && left.NVMeNQN == right.NVMeNQN &&
		left.ISCSIIQN == right.ISCSIIQN && slices.Equal(left.IPs, right.IPs) &&
		left.State == right.State && left.AccessMode == right.AccessMode &&
		left.Readonly == right.Readonly && left.UpdatedAt == right.UpdatedAt &&
		slices.Equal(left.CSIAddedNFSHosts, right.CSIAddedNFSHosts) &&
		slices.Equal(left.CSIAddedNVMeNQNs, right.CSIAddedNVMeNQNs)
}

func publicationRecordsFromDataset(ds *truenas.Dataset) (map[string]publicationRecord, error) {
	records := make(map[string]publicationRecord)
	if ds == nil {
		return records, nil
	}
	for key, property := range ds.UserProperties {
		if !strings.HasPrefix(key, publicationPropertyPrefix) {
			continue
		}
		// Only a value set locally on THIS dataset is one of its own publication
		// records. A ZFS clone inherits the source volume's publication_* user
		// properties with the origin snapshot name as their source, so a substring
		// "inherit" test would misclassify them as local and make a freshly cloned
		// volume look "published elsewhere" on its first publish.
		if !isLocalUserPropertySource(property.Source) {
			continue
		}
		var record publicationRecord
		if err := json.Unmarshal([]byte(property.Value), &record); err != nil {
			return nil, fmt.Errorf("invalid publication record %s: %w", key, err)
		}
		if record.Version != publicationRecordVersion || record.Node == "" ||
			(record.State != publicationStatePublished && record.State != publicationStateRemoving) {
			return nil, fmt.Errorf("invalid publication record %s contents", key)
		}
		if key != publicationPropertyKey(record.Node) {
			return nil, fmt.Errorf("publication record %s does not match node %q", key, record.Node)
		}
		records[key] = record
	}
	return records, nil
}

func storePublicationRecord(ctx context.Context, client truenas.ClientInterface, ds *truenas.Dataset, datasetName, key string, record publicationRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := client.DatasetSetUserProperty(ctx, datasetName, key, string(encoded)); err != nil {
		return err
	}
	if ds.UserProperties == nil {
		ds.UserProperties = make(map[string]truenas.UserProperty)
	}
	ds.UserProperties[key] = truenas.UserProperty{Value: string(encoded), Source: "local"}
	return nil
}

func removePublicationRecords(ctx context.Context, client truenas.ClientInterface, ds *truenas.Dataset, datasetName string, keys []string) error {
	if err := client.DatasetRemoveUserProperties(ctx, datasetName, keys); err != nil {
		return err
	}
	for _, key := range keys {
		delete(ds.UserProperties, key)
	}
	return nil
}

func isMultiNodeMode(mode csi.VolumeCapability_AccessMode_Mode) bool {
	switch mode {
	case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return true
	default:
		return false
	}
}

func accessModeFromCapability(capability *csi.VolumeCapability) csi.VolumeCapability_AccessMode_Mode {
	if capability == nil || capability.GetAccessMode() == nil {
		return csi.VolumeCapability_AccessMode_UNKNOWN
	}
	return capability.GetAccessMode().GetMode()
}

func shareTypeForPublishedVolume(ds *truenas.Dataset, volumeContext map[string]string) ShareType {
	if value := volumeContext["node_attach_driver"]; value != "" {
		if shareType := ParseShareType(value); shareType.IsValid() {
			return shareType
		}
	}
	switch {
	case storedBlockProtocol(ds, ShareTypeNVMeoF):
		return ShareTypeNVMeoF
	case storedBlockProtocol(ds, ShareTypeISCSI):
		return ShareTypeISCSI
	default:
		return ShareTypeNFS
	}
}

func mergeNodeIdentity(base, additional NodeIdentity) NodeIdentity {
	if base.Name == "" {
		base.Name = additional.Name
	}
	if base.NVMeNQN == "" {
		base.NVMeNQN = additional.NVMeNQN
	}
	if base.ISCSIIQN == "" {
		base.ISCSIIQN = additional.ISCSIIQN
	}
	base.IPs = canonicalNodeIPs(append(base.IPs, additional.IPs...))
	return base
}

// resolveControllerNodeIdentity augments a legacy plain node_id from live
// CSINode/Node state when available. Missing cluster state is not itself an
// error; protocol-specific validation below decides whether enough identity is
// present to enforce the requested fence.
func (d *Driver) resolveControllerNodeIdentity(ctx context.Context, nodeID string) (NodeIdentity, error) {
	identity, err := parseNodeIdentity(nodeID)
	if err != nil {
		return NodeIdentity{}, status.Errorf(codes.InvalidArgument, "invalid node ID: %v", err)
	}
	if d.eventRecorder == nil || d.eventRecorder.clientset == nil || identity.Name == "" {
		return identity, nil
	}
	if csiNode, getErr := d.eventRecorder.clientset.StorageV1().CSINodes().Get(ctx, identity.Name, metav1.GetOptions{}); getErr == nil {
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name != d.name {
				continue
			}
			if current, parseErr := parseNodeIdentity(driver.NodeID); parseErr == nil && !current.Legacy {
				identity = mergeNodeIdentity(identity, current)
			}
			break
		}
	}
	if node, getErr := d.eventRecorder.clientset.CoreV1().Nodes().Get(ctx, identity.Name, metav1.GetOptions{}); getErr == nil {
		for _, address := range node.Status.Addresses {
			if ip := net.ParseIP(address.Address); ip != nil {
				identity.IPs = append(identity.IPs, ip)
			}
		}
		identity.IPs = canonicalNodeIPs(identity.IPs)
	}
	return identity, nil
}

func validateIdentityForProtocol(identity NodeIdentity, shareType ShareType) error {
	switch shareType {
	case ShareTypeNVMeoF:
		if identity.NVMeNQN == "" {
			return status.Errorf(codes.FailedPrecondition, "node %s did not report an NVMe host NQN; upgrade/restart its node plugin before publishing this fenced volume", identity.Name)
		}
	case ShareTypeISCSI:
		if identity.ISCSIIQN == "" {
			return status.Errorf(codes.FailedPrecondition, "node %s did not report an iSCSI initiator IQN; upgrade/restart its node plugin before publishing this fenced volume", identity.Name)
		}
	case ShareTypeNFS:
		if len(identity.IPs) == 0 {
			return status.Errorf(codes.FailedPrecondition, "node %s did not report an IP address; upgrade/restart its node plugin before publishing this fenced volume", identity.Name)
		}
	}
	return nil
}

func (d *Driver) recordFencingDeferred(identity NodeIdentity, shareType ShareType, reason, detail string) {
	RecordFencingDeferred(reason, string(shareType))
	d.requestStartupAttachmentReconcile()
	key := reason + "\x00" + string(shareType) + "\x00" + identity.Name
	if _, loaded := d.fencingDeferredLogs.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	klog.Warningf("Fencing deferred for node %s protocol=%s reason=%s: %s",
		identity.Name, shareType, reason, detail)
}

// validateOrDeferFencingIdentity keeps additive mode rolling-upgrade safe. A
// node that has not re-registered its transport identity continues using the
// static allowlist. The publish still writes a durable ownership record so a
// second legacy node cannot bypass SINGLE_NODE semantics, but skips only that
// node's backend grant. Strict mode preserves the fail-closed contract.
func (d *Driver) validateOrDeferFencingIdentity(identity NodeIdentity, shareType ShareType) (bool, error) {
	if err := validateIdentityForProtocol(identity, shareType); err != nil {
		if d.config.Fencing.Mode == FencingModeAdditive {
			d.recordFencingDeferred(identity, shareType, "missing_identity", err.Error())
			return true, nil
		}
		return false, err
	}
	if shareType != ShareTypeNFS {
		return false, nil
	}
	for _, ip := range identity.IPs {
		allowed, err := ipWithinConfiguredNetworks(ip, d.config.NFS.ShareAllowedNetworks)
		if err != nil {
			return false, err
		}
		if allowed {
			return false, nil
		}
	}
	err := status.Errorf(codes.FailedPrecondition,
		"node %s has no IP inside nfs.shareAllowedNetworks", identity.Name)
	if d.config.Fencing.Mode == FencingModeAdditive {
		d.recordFencingDeferred(identity, shareType, "outside_allowed_network", err.Error())
		return true, nil
	}
	return false, err
}

func validatePublicationCompatibility(records map[string]publicationRecord, requested publicationRecord) error {
	requestedMode := csi.VolumeCapability_AccessMode_Mode(requested.AccessMode)
	for key := range records {
		existing := records[key]
		if existing.State != publicationStatePublished {
			continue
		}
		if existing.Node == requested.Node {
			if existing.AccessMode != requested.AccessMode || existing.Readonly != requested.Readonly {
				return status.Errorf(codes.AlreadyExists,
					"volume is already published to node %s with different access parameters", requested.Node)
			}
			continue
		}
		existingMode := csi.VolumeCapability_AccessMode_Mode(existing.AccessMode)
		if !isMultiNodeMode(requestedMode) || !isMultiNodeMode(existingMode) {
			// CSI requires the blocking node to be identified for this case.
			return status.Errorf(codes.FailedPrecondition,
				"volume is already published to node %s and cannot be published to node %s with access mode %s",
				existing.Node, requested.Node, requestedMode.String())
		}
	}
	return nil
}

func identityForSameNode(records map[string]publicationRecord, requested NodeIdentity) NodeIdentity {
	identity := requested
	for key := range records {
		record := records[key]
		if record.Node == requested.Name {
			identity = mergeNodeIdentity(identity, record.identity())
		}
	}
	return identity
}

func stringSet(values ...[]string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, group := range values {
		for _, value := range group {
			if value = strings.TrimSpace(value); value != "" {
				result[value] = struct{}{}
			}
		}
	}
	return result
}

// validateBackendSingleNodeCompatibility makes the transport allowlist, not
// just the recovery properties, participate in CSI's "published elsewhere"
// decision. Additive mode deliberately exempts configured static entries: they
// are upgrade compatibility grants rather than live CSI publications. Strict
// mode exempts those same known entries only long enough for applyBackendFence
// to remove them, as required by the transition contract.
func (d *Driver) validateBackendSingleNodeCompatibility(
	ctx context.Context,
	ds *truenas.Dataset,
	datasetName string,
	shareType ShareType,
	requested NodeIdentity,
	records map[string]publicationRecord,
	mode csi.VolumeCapability_AccessMode_Mode,
) error {
	if isMultiNodeMode(mode) {
		return nil
	}
	sameNode := identityForSameNode(records, requested)
	failedPrecondition := func(kind, identity string) error {
		return status.Errorf(codes.FailedPrecondition,
			"volume is already published elsewhere: backend %s allowlist contains %s", kind, identity)
	}

	switch shareType {
	case ShareTypeNFS:
		share, err := d.resolveNFSShare(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "verify NFS publication allowlist: %v", err)
		}
		if share == nil {
			return status.Errorf(codes.Internal, "verify NFS publication allowlist: %v", errFenceBackendAbsent)
		}
		sameIPs := make([]string, 0, len(sameNode.IPs))
		for _, ip := range sameNode.IPs {
			sameIPs = append(sameIPs, ip.String())
		}
		exempt := stringSet(sameIPs, d.config.NFS.ShareAllowedHosts)
		for _, host := range share.Hosts {
			host = strings.TrimSpace(host)
			if _, allowed := exempt[host]; host != "" && !allowed {
				return failedPrecondition("NFS host", host)
			}
		}

	case ShareTypeISCSI:
		target, err := d.resolveISCSITarget(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "verify iSCSI publication allowlist: %v", err)
		}
		if target == nil {
			return status.Errorf(codes.Internal, "verify iSCSI publication allowlist: %v", errFenceBackendAbsent)
		}
		staticGroups := make(map[int]struct{}, len(d.config.ISCSI.TargetGroups))
		for _, group := range d.config.ISCSI.TargetGroups {
			staticGroups[group.Initiator] = struct{}{}
		}
		exempt := stringSet([]string{sameNode.ISCSIIQN})
		for _, group := range target.Groups {
			if group.Initiator <= 0 {
				continue // allow-all has no node identity and is replaced below.
			}
			if _, static := staticGroups[group.Initiator]; static {
				continue
			}
			initiator, getErr := d.truenasClient.ISCSIInitiatorGet(ctx, group.Initiator)
			if getErr != nil {
				return status.Errorf(codes.Internal, "verify iSCSI initiator group %d: %v", group.Initiator, getErr)
			}
			if initiator == nil {
				continue
			}
			for _, iqn := range initiator.Initiators {
				if _, allowed := exempt[iqn]; iqn != "" && !allowed {
					return failedPrecondition("iSCSI initiator", iqn)
				}
			}
		}

	case ShareTypeNVMeoF:
		namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
		if err != nil {
			return status.Errorf(codes.Internal, "verify NVMe-oF publication allowlist: %v", err)
		}
		subsystem, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
		if err != nil {
			return status.Errorf(codes.Internal, "verify NVMe-oF publication allowlist: %v", err)
		}
		if subsystem == nil {
			return status.Errorf(codes.Internal, "verify NVMe-oF publication allowlist: %v", errFenceBackendAbsent)
		}
		sameNodeNQNs := []string{sameNode.NVMeNQN}
		for key := range records {
			record := records[key]
			if record.Node == requested.Name {
				sameNodeNQNs = append(sameNodeNQNs, record.NVMeNQN)
			}
		}
		exemptNQNs := stringSet(sameNodeNQNs, d.config.NVMeoF.SubsystemHosts)
		exemptHostIDs := make(map[int]struct{}, len(exemptNQNs))
		for nqn := range exemptNQNs {
			host, findErr := d.truenasClient.NVMeoFHostFindByNQN(ctx, nqn)
			if findErr != nil {
				return status.Errorf(codes.Internal, "verify NVMe-oF host %q: %v", nqn, findErr)
			}
			if host != nil {
				exemptHostIDs[host.ID] = struct{}{}
			}
		}
		associations, listErr := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
		if listErr != nil {
			return status.Errorf(codes.Internal, "verify NVMe-oF subsystem allowlist: %v", listErr)
		}
		for _, association := range associations {
			if _, allowed := exemptHostIDs[association.HostID]; allowed {
				continue
			}
			if _, allowed := exemptNQNs[association.HostNQN]; association.HostNQN != "" && allowed {
				continue
			}
			identity := association.HostNQN
			if identity == "" {
				identity = fmt.Sprintf("host ID %d", association.HostID)
			}
			return failedPrecondition("NVMe host", identity)
		}
	}
	return nil
}

func normalizedNFSHost(value string) string {
	value = strings.TrimSpace(value)
	if ip := net.ParseIP(value); ip != nil {
		return ip.String()
	}
	return value
}

// populateAdditiveGrantOwnership classifies transport grants before the
// publication record is stored. The record is therefore durable before the
// corresponding grant is applied, while retries retain the original ownership
// decision instead of mistaking a previously added grant for static policy.
func (d *Driver) populateAdditiveGrantOwnership(
	ctx context.Context,
	ds *truenas.Dataset,
	datasetName string,
	shareType ShareType,
	identity NodeIdentity,
	previous publicationRecord,
	hasPrevious bool,
	deferred bool,
	record *publicationRecord,
) error {
	if d.config.Fencing.Mode != FencingModeAdditive || record == nil {
		return nil
	}
	if hasPrevious {
		record.CSIAddedNFSHosts = append([]string(nil), previous.CSIAddedNFSHosts...)
		record.CSIAddedNVMeNQNs = append([]string(nil), previous.CSIAddedNVMeNQNs...)
	}
	if deferred {
		return nil
	}

	switch shareType {
	case ShareTypeNFS:
		share, err := d.resolveNFSShare(ctx, ds, datasetName)
		if err != nil {
			return err
		}
		if share != nil && share.Enabled && len(share.Hosts) == 0 && len(share.Networks) == 0 {
			// A legacy allow-all share needs no dynamic host grant. Additive mode
			// deliberately leaves that policy untouched until strict cutover.
			return nil
		}
		existing := make(map[string]struct{})
		backendHosts := make([]string, 0)
		if share != nil {
			for _, host := range share.Hosts {
				normalized := normalizedNFSHost(host)
				existing[normalized] = struct{}{}
				backendHosts = append(backendHosts, normalized)
			}
		}
		configured := make(map[string]struct{}, len(d.config.NFS.ShareAllowedHosts))
		for _, host := range d.config.NFS.ShareAllowedHosts {
			configured[normalizedNFSHost(host)] = struct{}{}
		}
		newlyAdded := make([]string, 0)
		for _, ip := range canonicalNodeIPs(identity.IPs) {
			allowed, boundErr := ipWithinConfiguredNetworks(ip, d.config.NFS.ShareAllowedNetworks)
			if boundErr != nil {
				return boundErr
			}
			if !allowed {
				continue
			}
			host := ip.String()
			if _, static := configured[host]; static {
				continue
			}
			if _, alreadyPresent := existing[host]; alreadyPresent {
				continue
			}
			newlyAdded = append(newlyAdded, host)
		}
		// Compact against the backend allowlist read here so entries a prior fence
		// already removed from the share drop off, bounding the list under churn.
		// A list still over the cap after compaction is all backend-live; dropping
		// any entry would orphan a revocable grant, so the publish fails instead.
		compacted, overCap := compactAdditiveGrants(record.CSIAddedNFSHosts, newlyAdded, backendHosts)
		if overCap {
			RecordFencingProvenanceOverflow("nfs")
			klog.Errorf("Additive NFS grant provenance for %s holds %d backend-live entries (cap %d); refusing to publish rather than dropping revocation provenance",
				datasetName, len(compacted), additiveGrantHardCap)
			return status.Errorf(codes.ResourceExhausted,
				"additive NFS grant provenance for %s holds %d backend-live entries, exceeding the %d-entry cap; investigate node identity churn or clean stale hosts from the share allowlist",
				datasetName, len(compacted), additiveGrantHardCap)
		}
		record.CSIAddedNFSHosts = compacted
	case ShareTypeNVMeoF:
		nqn := strings.TrimSpace(identity.NVMeNQN)
		if nqn == "" {
			return nil
		}
		if _, static := stringSet(d.config.NVMeoF.SubsystemHosts)[nqn]; static {
			return nil
		}
		namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
		if err != nil {
			return err
		}
		subsystem, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
		if err != nil {
			return err
		}
		associationExists := false
		backendNQNs := make([]string, 0)
		associationHostIDs := make(map[int]struct{})
		if subsystem != nil {
			host, findErr := d.truenasClient.NVMeoFHostFindByNQN(ctx, nqn)
			if findErr != nil {
				return findErr
			}
			associations, listErr := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
			if listErr != nil {
				return listErr
			}
			for _, association := range associations {
				associationHostIDs[association.HostID] = struct{}{}
				if association.HostNQN != "" {
					backendNQNs = append(backendNQNs, association.HostNQN)
				}
				// TrueNAS 26.0 returns nested host/subsys objects and normally
				// expands hostnqn. HostID remains the defensive fallback.
				if (host != nil && association.HostID == host.ID) || association.HostNQN == nqn {
					associationExists = true
				}
			}
			if associationExists {
				// The CURRENT NQN's association is live even when the backend hides
				// hostnqn (matched by HostID above). It must count as backend-live,
				// or a republish would compact the live entry out of provenance and
				// the final unpublish could never revoke the association.
				backendNQNs = append(backendNQNs, nqn)
			}
			// Defensive completeness for backends that omit the expanded hostnqn
			// field: a provenance entry whose association is live but only visible
			// by HostID must count as backend-live, or compaction would drop an
			// entry that additive teardown still needs for revocation.
			backendVisible := stringSet(backendNQNs)
			for _, previousNQN := range record.CSIAddedNVMeNQNs {
				previousNQN = strings.TrimSpace(previousNQN)
				if previousNQN == "" || previousNQN == nqn {
					continue
				}
				if _, visible := backendVisible[previousNQN]; visible {
					continue
				}
				previousHost, resolveErr := d.truenasClient.NVMeoFHostFindByNQN(ctx, previousNQN)
				if resolveErr != nil {
					return resolveErr
				}
				if previousHost == nil {
					continue
				}
				if _, associated := associationHostIDs[previousHost.ID]; associated {
					backendNQNs = append(backendNQNs, previousNQN)
				}
			}
		}
		var newlyAdded []string
		if !associationExists {
			newlyAdded = []string{nqn}
		}
		// Compact against the subsystem's live associations so NQNs a prior fence
		// already detached drop off, bounding the list under per-boot NQN churn.
		// A list still over the cap after compaction is all backend-live; dropping
		// any entry would orphan a revocable grant, so the publish fails instead.
		compacted, overCap := compactAdditiveGrants(record.CSIAddedNVMeNQNs, newlyAdded, backendNQNs)
		if overCap {
			RecordFencingProvenanceOverflow("nvmeof")
			klog.Errorf("Additive NVMe-oF grant provenance for %s holds %d backend-live entries (cap %d); refusing to publish rather than dropping revocation provenance",
				datasetName, len(compacted), additiveGrantHardCap)
			return status.Errorf(codes.ResourceExhausted,
				"additive NVMe-oF grant provenance for %s holds %d backend-live entries, exceeding the %d-entry cap; investigate node identity churn or clean stale hosts from the subsystem",
				datasetName, len(compacted), additiveGrantHardCap)
		}
		record.CSIAddedNVMeNQNs = compacted
	}
	return nil
}

func (d *Driver) publishFencedVolume(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType, identity NodeIdentity, capability *csi.VolumeCapability, readonly bool) error {
	deferred, err := d.validateOrDeferFencingIdentity(identity, shareType)
	if err != nil {
		return err
	}
	records, err := publicationRecordsFromDataset(ds)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read durable publication records: %v", err)
	}
	record, err := newPublicationRecord(identity, accessModeFromCapability(capability), readonly)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to persist node identity: %v", err)
	}
	if err := validatePublicationCompatibility(records, record); err != nil {
		return err
	}
	if err := d.validateBackendSingleNodeCompatibility(
		ctx, ds, datasetName, shareType, identity, records,
		csi.VolumeCapability_AccessMode_Mode(record.AccessMode),
	); err != nil {
		return err
	}
	key := publicationPropertyKey(identity.Name)
	previous, hasPrevious := records[key]
	if err := d.populateAdditiveGrantOwnership(
		ctx, ds, datasetName, shareType, identity, previous, hasPrevious, deferred, &record,
	); err != nil {
		// The provenance-overflow refusal is a deliberate, actionable status;
		// preserve its code instead of masking it as Internal.
		if status.Code(err) == codes.ResourceExhausted {
			return err
		}
		return status.Errorf(codes.Internal, "failed to classify additive grant ownership: %v", err)
	}
	if err := storePublicationRecord(ctx, d.truenasClient, ds, datasetName, key, record); err != nil {
		return status.Errorf(codes.Internal, "failed to store publication identity: %v", err)
	}
	d.stalePublicationRecordsSeen.Delete(stalePublicationObservationKey(datasetName, key))
	records[key] = record
	if deferred {
		// Persist publication ownership even though the transport-specific grant is
		// deferred. This prevents two legacy nodes from both receiving a successful
		// SINGLE_NODE publish, while leaving the static backend policy untouched.
		return nil
	}
	if err := d.applyBackendFence(ctx, ds, datasetName, shareType, records); err != nil {
		// Keep the durable record. A retry or startup reconciliation will converge
		// the allowlist without needing the node to report its identity again.
		return status.Errorf(codes.Internal, "failed to enforce backend publication fence: %v", err)
	}
	return nil
}

func (d *Driver) unpublishFencedVolume(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType, nodeID string) error {
	records, err := publicationRecordsFromDataset(ds)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read durable publication records: %v", err)
	}
	keys := make([]string, 0)
	if nodeID == "" {
		for key := range records {
			keys = append(keys, key)
		}
	} else {
		identity, parseErr := parseNodeIdentity(nodeID)
		if parseErr != nil {
			return status.Errorf(codes.InvalidArgument, "invalid node ID: %v", parseErr)
		}
		key := publicationPropertyKey(identity.Name)
		if _, exists := records[key]; exists {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	for _, key := range keys {
		record := records[key]
		record.State = publicationStateRemoving
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := storePublicationRecord(ctx, d.truenasClient, ds, datasetName, key, record); err != nil {
			return status.Errorf(codes.Internal, "failed to store unpublish tombstone: %v", err)
		}
		records[key] = record
	}
	if err := d.applyBackendFence(ctx, ds, datasetName, shareType, records); err != nil && !errors.Is(err, errFenceBackendAbsent) {
		return status.Errorf(codes.Internal, "failed to remove backend publication fence: %v", err)
	}
	if err := removePublicationRecords(ctx, d.truenasClient, ds, datasetName, keys); err != nil {
		return status.Errorf(codes.Internal, "backend access was removed but durable publication cleanup failed: %v", err)
	}
	return nil
}

func activeAndRemovingIdentities(records map[string]publicationRecord) (active, removing []NodeIdentity) {
	for key := range records {
		record := records[key]
		switch record.State {
		case publicationStatePublished:
			active = append(active, record.identity())
		case publicationStateRemoving:
			removing = append(removing, record.identity())
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Name < active[j].Name })
	sort.Slice(removing, func(i, j int) bool { return removing[i].Name < removing[j].Name })
	return active, removing
}

func additiveGrantOwnership(records map[string]publicationRecord) (nfsHosts, nvmeNQNs []string) {
	for key := range records {
		record := records[key]
		nfsHosts = append(nfsHosts, record.CSIAddedNFSHosts...)
		nvmeNQNs = append(nvmeNQNs, record.CSIAddedNVMeNQNs...)
	}
	return uniqueSortedStrings(nfsHosts), uniqueSortedStrings(nvmeNQNs)
}

func (d *Driver) applyBackendFence(ctx context.Context, ds *truenas.Dataset, datasetName string, shareType ShareType, records map[string]publicationRecord) error {
	active, removing := activeAndRemovingIdentities(records)
	// Provenance is cumulative across same-node identity rotations. Supplying it
	// for published as well as removing records lets each convergence remove old
	// CSI-added grants immediately; current identities and configured static
	// entries remain protected by each backend's desired set.
	ownedNFSHosts, ownedNVMeNQNs := additiveGrantOwnership(records)
	enforceable := make([]NodeIdentity, 0, len(active))
	protectedNFSHosts := make([]string, 0)
	protectedNVMeNQNs := make([]string, 0)
	for _, identity := range active {
		deferred, err := d.validateOrDeferFencingIdentity(identity, shareType)
		if err != nil {
			return fmt.Errorf("publication record for node %s is not enforceable yet: %w", identity.Name, err)
		}
		if deferred {
			// Additive mode keeps a durable ownership record for legacy nodes so
			// SINGLE_NODE compatibility still works, but leaves their access under
			// the preserved static policy until they re-register a transport
			// identity. Preserve any earlier CSI-added grant without recreating it;
			// enforceable peers must still converge independently.
			record := records[publicationPropertyKey(identity.Name)]
			protectedNFSHosts = append(protectedNFSHosts, record.CSIAddedNFSHosts...)
			protectedNVMeNQNs = append(protectedNVMeNQNs, record.CSIAddedNVMeNQNs...)
			continue
		}
		enforceable = append(enforceable, identity)
	}
	switch shareType {
	case ShareTypeNFS:
		return d.applyNFSFence(ctx, ds, datasetName, enforceable, ownedNFSHosts, uniqueSortedStrings(protectedNFSHosts))
	case ShareTypeISCSI:
		return d.applyISCSIFence(ctx, ds, datasetName, enforceable)
	case ShareTypeNVMeoF:
		return d.applyNVMeFence(
			ctx, ds, datasetName, enforceable, removing, ownedNVMeNQNs, uniqueSortedStrings(protectedNVMeNQNs),
		)
	default:
		return fmt.Errorf("unsupported share type %q", shareType)
	}
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func ipWithinConfiguredNetworks(ip net.IP, networks []string) (bool, error) {
	if len(networks) == 0 {
		return true, nil
	}
	for _, value := range networks {
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			if configuredIP := net.ParseIP(value); configuredIP != nil && configuredIP.Equal(ip) {
				return true, nil
			}
			return false, fmt.Errorf("invalid nfs.shareAllowedNetworks entry %q", value)
		}
		if network.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}

func (d *Driver) applyNFSFence(
	ctx context.Context,
	ds *truenas.Dataset,
	datasetName string,
	active []NodeIdentity,
	additiveRemovingHosts, additiveProtectedHosts []string,
) error {
	share, err := d.resolveNFSShare(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	if share == nil {
		return fmt.Errorf("%w: NFS share for %s", errFenceBackendAbsent, datasetName)
	}
	if d.config.Fencing.Mode == FencingModeAdditive && share.Enabled && len(share.Hosts) == 0 && len(share.Networks) == 0 {
		// An enabled share with no host/network restrictions is the legacy
		// allow-all policy. Adding a per-node host would narrow it and could evict
		// nodes whose identities have not re-registered yet. Additive mode must
		// preserve that broad static policy; strict mode performs the cutover.
		return nil
	}
	activeHosts := make([]string, 0)
	for _, identity := range active {
		accepted := false
		for _, ip := range identity.IPs {
			allowed, boundErr := ipWithinConfiguredNetworks(ip, d.config.NFS.ShareAllowedNetworks)
			if boundErr != nil {
				return boundErr
			}
			if allowed {
				activeHosts = append(activeHosts, ip.String())
				accepted = true
			}
		}
		if !accepted {
			if d.config.Fencing.Mode == FencingModeAdditive {
				d.recordFencingDeferred(identity, ShareTypeNFS, "outside_allowed_network",
					"no node IP is inside nfs.shareAllowedNetworks")
				return fmt.Errorf("%w: node %s has no IP inside nfs.shareAllowedNetworks", errFenceDeferred, identity.Name)
			}
			return status.Errorf(codes.FailedPrecondition, "node %s has no IP inside nfs.shareAllowedNetworks", identity.Name)
		}
	}
	hosts := append([]string(nil), activeHosts...)
	if d.config.Fencing.Mode == FencingModeAdditive {
		hosts = append(hosts, d.config.NFS.ShareAllowedHosts...)
		// Preserve unknown pre-upgrade host entries. Only durable grants that
		// scale-csi classified before adding may be removed by additive teardown.
		removeSet := make(map[string]struct{}, len(additiveRemovingHosts))
		for _, host := range additiveRemovingHosts {
			removeSet[normalizedNFSHost(host)] = struct{}{}
		}
		activeSet := make(map[string]struct{})
		protectedHosts := append(append([]string(nil), activeHosts...), d.config.NFS.ShareAllowedHosts...)
		protectedHosts = append(protectedHosts, additiveProtectedHosts...)
		for _, host := range protectedHosts {
			activeSet[normalizedNFSHost(host)] = struct{}{}
		}
		for _, host := range share.Hosts {
			hostKey := normalizedNFSHost(host)
			if _, removingHost := removeSet[hostKey]; removingHost {
				if _, stillNeeded := activeSet[hostKey]; !stillNeeded {
					continue
				}
			}
			hosts = append(hosts, host)
		}
	}
	hosts = uniqueSortedStrings(hosts)
	networks := []string{}
	if d.config.Fencing.Mode == FencingModeAdditive {
		// Additive is a migration mode: existing static network grants are
		// preserved bit-for-bit alongside dynamic host entries.
		networks = append(networks, share.Networks...)
	}
	_, err = d.truenasClient.NFSShareUpdate(ctx, share.ID, map[string]interface{}{
		"hosts":    hosts,
		"networks": networks,
		"enabled":  len(hosts) > 0 || len(networks) > 0,
	})
	return err
}

func (d *Driver) resolveISCSIPortalIDs(ctx context.Context) ([]int, error) {
	if len(d.config.ISCSI.TargetGroups) > 0 {
		ids := make([]string, 0, len(d.config.ISCSI.TargetGroups))
		for _, group := range d.config.ISCSI.TargetGroups {
			if group.Portal <= 0 {
				return nil, fmt.Errorf("iscsi.targetGroups contains invalid portal ID %d", group.Portal)
			}
			ids = append(ids, strconv.Itoa(group.Portal))
		}
		unique := uniqueSortedStrings(ids)
		result := make([]int, 0, len(unique))
		for _, value := range unique {
			id, _ := strconv.Atoi(value)
			result = append(result, id)
		}
		return result, nil
	}
	host, portText, err := net.SplitHostPort(d.config.ISCSI.TargetPortal)
	if err != nil {
		host, portText = d.config.ISCSI.TargetPortal, "3260"
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, fmt.Errorf("invalid iscsi.targetPortal %q: %w", d.config.ISCSI.TargetPortal, err)
	}
	portals, err := d.truenasClient.ISCSIPortalList(ctx)
	if err != nil {
		return nil, err
	}
	for _, portal := range portals {
		for _, listen := range portal.Listen {
			if listen.IP == host && listen.Port == port {
				return []int{portal.ID}, nil
			}
		}
	}
	return nil, fmt.Errorf("no TrueNAS iSCSI portal listens on %q", d.config.ISCSI.TargetPortal)
}

func (d *Driver) resolveFencingInitiatorGroup(ctx context.Context, ds *truenas.Dataset, datasetName string) (*truenas.ISCSIInitiator, error) {
	comment := "scale-csi fencing: " + datasetName
	if rawID := datasetUserProperty(ds, PropISCSIInitiatorID); rawID != "" && rawID != "-" {
		id, err := strconv.Atoi(rawID)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid iSCSI fencing initiator ID %q", rawID)
		}
		group, getErr := d.truenasClient.ISCSIInitiatorGet(ctx, id)
		if getErr != nil && !truenas.IsNotFoundError(getErr) {
			return nil, getErr
		}
		if getErr == nil && group != nil && group.Comment == comment {
			return group, nil
		}
		if group != nil {
			klog.Warningf("Ignoring stored iSCSI initiator group ID %d for %s: comment %q does not prove ownership", id, datasetName, group.Comment)
		}
	}
	groups, err := d.truenasClient.ISCSIInitiatorList(ctx)
	if err != nil {
		return nil, err
	}
	for _, group := range groups {
		if group.Comment == comment {
			return group, nil
		}
	}
	return nil, nil
}

func (d *Driver) safeAdditiveISCSIGroups(ctx context.Context, target *truenas.ISCSITarget, dynamicID int) ([]truenas.ISCSITargetGroup, error) {
	if d.config.Fencing.Mode != FencingModeAdditive {
		return nil, nil
	}
	result := make([]truenas.ISCSITargetGroup, 0)
	for _, group := range target.Groups {
		if dynamicID > 0 && group.Initiator == dynamicID {
			continue
		}
		if group.Initiator <= 0 {
			// A null initiator on a TrueNAS target group is a legacy allow-all
			// relationship. It remains load-bearing throughout additive migration.
			result = append(result, group)
			continue
		}
		initiator, err := d.truenasClient.ISCSIInitiatorGet(ctx, group.Initiator)
		if err != nil {
			if truenas.IsNotFoundError(err) {
				// A stale relationship cannot provide an access restriction and is
				// safe to omit from the replacement group list.
				continue
			}
			return nil, fmt.Errorf("verify static initiator group %d: %w", group.Initiator, err)
		}
		if initiator == nil {
			// A dangling relationship cannot authorize a known static initiator.
			continue
		}
		if initiator.Initiators == nil {
			// TrueNAS null is the legacy allow-all shape. Preserve it in additive
			// mode; a non-nil empty list is intentionally different (deny-all) and
			// is also a valid static relationship.
			result = append(result, group)
			continue
		}
		result = append(result, group)
	}
	return result, nil
}

func (d *Driver) applyISCSIFence(ctx context.Context, ds *truenas.Dataset, datasetName string, active []NodeIdentity) error {
	target, err := d.resolveISCSITarget(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("%w: iSCSI target for %s", errFenceBackendAbsent, datasetName)
	}
	iqns := make([]string, 0, len(active))
	for _, identity := range active {
		if identity.ISCSIIQN != "" {
			iqns = append(iqns, identity.ISCSIIQN)
		}
	}
	iqns = uniqueSortedStrings(iqns)
	dynamicGroup, err := d.resolveFencingInitiatorGroup(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	dynamicID := 0
	if dynamicGroup != nil {
		dynamicID = dynamicGroup.ID
	}
	groups, err := d.safeAdditiveISCSIGroups(ctx, target, dynamicID)
	if err != nil {
		return err
	}
	if dynamicGroup == nil {
		// A non-nil empty list is an exact deny-all allowlist. Keeping this
		// CSI-owned group attached to the target preserves portal validity on the
		// last unpublish without granting an initiator access.
		dynamicGroup, err = d.truenasClient.ISCSIInitiatorCreateWithInitiators(ctx, iqns, "scale-csi fencing: "+datasetName)
	} else {
		dynamicGroup, err = d.truenasClient.ISCSIInitiatorUpdate(ctx, dynamicGroup.ID, iqns, dynamicGroup.Comment)
	}
	if err != nil {
		return err
	}
	dynamicID = dynamicGroup.ID
	if err := d.setDatasetUserProperties(ctx, ds, datasetName, map[string]string{PropISCSIInitiatorID: strconv.Itoa(dynamicID)}); err != nil {
		return err
	}
	if len(iqns) > 0 {
		portals, portalErr := d.resolveISCSIPortalIDs(ctx)
		if portalErr != nil {
			return portalErr
		}
		for _, portalID := range portals {
			groups = append(groups, truenas.ISCSITargetGroup{Portal: portalID, Initiator: dynamicID, AuthMethod: "NONE"})
		}
	} else {
		// Retain the exact existing portal relationships on last unpublish. Some
		// TrueNAS releases reject a target update with zero portal groups; access is
		// fenced by the now-empty initiator allowlist, not by removing the portal.
		for _, group := range target.Groups {
			if group.Initiator == dynamicID {
				groups = append(groups, group)
			}
		}
		if len(groups) == 0 {
			// An interrupted publish may have created the CSI initiator group but
			// never attached it. Reuse the target's actual portal IDs first; config
			// lookup is only a fallback when the target truly has no relationships.
			portalSet := make(map[int]struct{})
			for _, group := range target.Groups {
				if group.Portal > 0 {
					portalSet[group.Portal] = struct{}{}
				}
			}
			portals := make([]int, 0, len(portalSet))
			for portalID := range portalSet {
				portals = append(portals, portalID)
			}
			sort.Ints(portals)
			if len(portals) == 0 {
				var portalErr error
				portals, portalErr = d.resolveISCSIPortalIDs(ctx)
				if portalErr != nil {
					return portalErr
				}
			}
			for _, portalID := range portals {
				groups = append(groups, truenas.ISCSITargetGroup{Portal: portalID, Initiator: dynamicID, AuthMethod: "NONE"})
			}
		}
	}
	if _, err := d.truenasClient.ISCSITargetUpdate(ctx, target.ID, groups); err != nil {
		return err
	}
	if d.serviceReloadDebouncer != nil {
		if err := d.serviceReloadDebouncer.RequestReload(ctx, "iscsitarget"); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) applyNVMeFence(
	ctx context.Context,
	ds *truenas.Dataset,
	datasetName string,
	active, removing []NodeIdentity,
	additiveRemovingNQNs, additiveProtectedNQNs []string,
) error {
	namespace, err := d.resolveNVMeNamespace(ctx, ds, datasetName)
	if err != nil {
		return err
	}
	subsystem, err := d.resolveNVMeSubsystem(ctx, ds, datasetName, namespace)
	if err != nil {
		return err
	}
	if subsystem == nil {
		return fmt.Errorf("%w: NVMe-oF subsystem for %s", errFenceBackendAbsent, datasetName)
	}
	if subsystem.AllowAnyHost && d.config.Fencing.Mode == FencingModeStrict {
		if _, updateErr := d.truenasClient.NVMeoFSubsystemUpdateAllowAnyHost(ctx, subsystem.ID, false); updateErr != nil {
			return updateErr
		}
	}
	desiredNQNs := make([]string, 0, len(active)+len(d.config.NVMeoF.SubsystemHosts))
	for _, identity := range active {
		if identity.NVMeNQN != "" {
			desiredNQNs = append(desiredNQNs, identity.NVMeNQN)
		}
	}
	if d.config.Fencing.Mode == FencingModeAdditive {
		desiredNQNs = append(desiredNQNs, d.config.NVMeoF.SubsystemHosts...)
	}
	desiredNQNs = uniqueSortedStrings(desiredNQNs)
	desiredIDs, err := d.resolveNVMeoFHostIDs(ctx, desiredNQNs)
	if err != nil {
		return err
	}
	desiredByID := make(map[int]struct{}, len(desiredIDs))
	for _, hostID := range desiredIDs {
		desiredByID[hostID] = struct{}{}
		if _, createErr := d.truenasClient.NVMeoFHostSubsysCreate(ctx, hostID, subsystem.ID); createErr != nil {
			return createErr
		}
	}
	associations, err := d.truenasClient.NVMeoFHostSubsysListBySubsystem(ctx, subsystem.ID)
	if err != nil {
		return err
	}
	removeNQNs := append([]string(nil), additiveRemovingNQNs...)
	if d.config.Fencing.Mode == FencingModeStrict {
		removeNQNs = make([]string, 0, len(removing))
		for _, identity := range removing {
			if identity.NVMeNQN != "" {
				removeNQNs = append(removeNQNs, identity.NVMeNQN)
			}
		}
	}
	removeIDs := make([]int, 0, len(removeNQNs))
	for _, nqn := range uniqueSortedStrings(removeNQNs) {
		host, findErr := d.truenasClient.NVMeoFHostFindByNQN(ctx, nqn)
		if findErr != nil {
			return fmt.Errorf("resolve NVMe-oF host %q for unpublish: %w", nqn, findErr)
		}
		if host != nil {
			removeIDs = append(removeIDs, host.ID)
		}
	}
	removeByID := make(map[int]struct{}, len(removeIDs))
	for _, hostID := range removeIDs {
		removeByID[hostID] = struct{}{}
	}
	protectedNQNs := stringSet(additiveProtectedNQNs)
	protectedByID := make(map[int]struct{}, len(protectedNQNs))
	for nqn := range protectedNQNs {
		host, findErr := d.truenasClient.NVMeoFHostFindByNQN(ctx, nqn)
		if findErr != nil {
			return fmt.Errorf("resolve deferred NVMe-oF host %q: %w", nqn, findErr)
		}
		if host != nil {
			protectedByID[host.ID] = struct{}{}
		}
	}
	for _, association := range associations {
		if _, keep := desiredByID[association.HostID]; keep {
			continue
		}
		if _, keep := protectedByID[association.HostID]; keep {
			continue
		}
		if _, keep := protectedNQNs[association.HostNQN]; association.HostNQN != "" && keep {
			continue
		}
		remove := d.config.Fencing.Mode == FencingModeStrict
		if d.config.Fencing.Mode == FencingModeAdditive {
			// TrueNAS 26.0 expands hostnqn in the nested host object. Resolve the
			// durable CSI-added NQN to HostID as a defensive fallback and remove
			// only associations carrying explicit scale-csi provenance.
			_, remove = removeByID[association.HostID]
		}
		if remove {
			if err := d.truenasClient.NVMeoFHostSubsysDelete(ctx, association.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
