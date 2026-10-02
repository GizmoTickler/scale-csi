package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// VolumePublication objects hold publication records in Kubernetes: one per
// (volume dataset, node), in the driver's namespace. A record write is an API
// write of a few milliseconds instead of a ZFS property update of 0.2-0.4 s
// that TrueNAS serializes, and a copied or replicated dataset carries no
// records with it.
var volumePublicationGVR = schema.GroupVersionResource{
	Group:    "scale-csi.io",
	Version:  "v1alpha1",
	Resource: "volumepublications",
}

const (
	volumePublicationKind      = "VolumePublication"
	volumePublicationListKind  = "VolumePublicationList"
	labelVolumePublicationInst = "scale-csi.io/instance"
	labelVolumePublicationDS   = "scale-csi.io/dataset"
	labelVolumePublicationNode = "scale-csi.io/node"
)

var labelValuePattern = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`)

// volumePublicationSpec is a publication record as the object stores it.
type volumePublicationSpec struct {
	Version          int      `json:"version"`
	Dataset          string   `json:"dataset"`
	Node             string   `json:"node"`
	NodeID           string   `json:"nodeID,omitempty"`
	NVMeNQN          string   `json:"nvmeNQN,omitempty"`
	ISCSIIQN         string   `json:"iscsiIQN,omitempty"`
	IPs              []string `json:"ips,omitempty"`
	State            string   `json:"state"`
	AccessMode       int32    `json:"accessMode"`
	Readonly         bool     `json:"readonly,omitempty"`
	UpdatedAt        string   `json:"updatedAt"`
	CSIAddedNFSHosts []string `json:"csiAddedNFSHosts,omitempty"`
	CSIAddedNVMeNQNs []string `json:"csiAddedNVMeNQNs,omitempty"`
}

func specFromRecord(datasetName string, record publicationRecord) volumePublicationSpec {
	return volumePublicationSpec{
		Version:          record.Version,
		Dataset:          datasetName,
		Node:             record.Node,
		NodeID:           record.EncodedID,
		NVMeNQN:          record.NVMeNQN,
		ISCSIIQN:         record.ISCSIIQN,
		IPs:              record.IPs,
		State:            record.State,
		AccessMode:       record.AccessMode,
		Readonly:         record.Readonly,
		UpdatedAt:        record.UpdatedAt,
		CSIAddedNFSHosts: record.CSIAddedNFSHosts,
		CSIAddedNVMeNQNs: record.CSIAddedNVMeNQNs,
	}
}

func (s volumePublicationSpec) record() publicationRecord {
	return publicationRecord{
		Version:          s.Version,
		Node:             s.Node,
		EncodedID:        s.NodeID,
		NVMeNQN:          s.NVMeNQN,
		ISCSIIQN:         s.ISCSIIQN,
		IPs:              s.IPs,
		State:            s.State,
		AccessMode:       s.AccessMode,
		Readonly:         s.Readonly,
		UpdatedAt:        s.UpdatedAt,
		CSIAddedNFSHosts: s.CSIAddedNFSHosts,
		CSIAddedNVMeNQNs: s.CSIAddedNVMeNQNs,
	}
}

func shortHash(parts ...string) string {
	h := sha256.New()
	for i, part := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil)[:20])
}

// kubernetesPublicationStore keeps records as VolumePublication objects.
type kubernetesPublicationStore struct {
	client    dynamic.Interface
	namespace string
	instance  string
	// versions carries each object's resourceVersion from the read under the
	// volume lock to the write that follows it. Nil (a store built without
	// newKubernetesPublicationStore) reads the object before each write.
	versions *publicationVersions
}

func newKubernetesPublicationStore(client dynamic.Interface, namespace, instance string) kubernetesPublicationStore {
	return kubernetesPublicationStore{client: client, namespace: namespace, instance: instance, versions: newPublicationVersions()}
}

// errPublicationRecordConflict reports a record write that found the object
// changed since the locked read it was decided on: another process wrote or
// removed it. The write is not made; the caller fails and is retried, and the
// retry decides again from a fresh read.
var errPublicationRecordConflict = errors.New("publication record changed since it was read")

// publicationVersions remembers, per dataset, the resourceVersion of each of
// its VolumePublications as this process last read or wrote them. A dataset
// present here was read: an object name absent from its entry was observed
// absent, and its write is a create.
type publicationVersions struct {
	mu        sync.Mutex
	byDataset map[string]map[string]string
}

func newPublicationVersions() *publicationVersions {
	return &publicationVersions{byDataset: make(map[string]map[string]string)}
}

// observe replaces datasetName's versions with a read of its objects.
func (v *publicationVersions) observe(datasetName string, objects []*unstructured.Unstructured) {
	if v == nil {
		return
	}
	versions := make(map[string]string, len(objects))
	for _, object := range objects {
		versions[object.GetName()] = object.GetResourceVersion()
	}
	v.mu.Lock()
	v.byDataset[datasetName] = versions
	v.mu.Unlock()
}

// lookup is name's resourceVersion: observed is false if datasetName was
// never read, exists false if name was observed absent.
func (v *publicationVersions) lookup(datasetName, name string) (resourceVersion string, observed, exists bool) {
	if v == nil {
		return "", false, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	versions, observed := v.byDataset[datasetName]
	if !observed {
		return "", false, false
	}
	resourceVersion, exists = versions[name]
	return resourceVersion, true, exists
}

func (v *publicationVersions) set(datasetName, name, resourceVersion string) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if versions, observed := v.byDataset[datasetName]; observed {
		versions[name] = resourceVersion
	}
}

func (v *publicationVersions) drop(datasetName, name string) {
	if v == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if versions, observed := v.byDataset[datasetName]; observed {
		delete(versions, name)
	}
}

// observed is whether datasetName has been read.
func (v *publicationVersions) observed(datasetName string) bool {
	if v == nil {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	_, observed := v.byDataset[datasetName]
	return observed
}

// forget drops datasetName: its next write reads the object first.
func (v *publicationVersions) forget(datasetName string) {
	if v == nil {
		return
	}
	v.mu.Lock()
	delete(v.byDataset, datasetName)
	v.mu.Unlock()
}

func (s kubernetesPublicationStore) objectName(datasetName, key string) string {
	return "vp-" + shortHash(s.instance, datasetName, key)
}

func (s kubernetesPublicationStore) resource() dynamic.ResourceInterface {
	return s.client.Resource(volumePublicationGVR).Namespace(s.namespace)
}

// records reads the dataset's records for a caller that only reports or
// judges them. It leaves the versions alone: a read made without the volume
// lock (a ListVolumes page, the startup diff, a quarantine check) could
// otherwise record a foreign write as seen, which the next locked write
// would then overwrite, or record a version older than one this process has
// since written, which the next locked write would then fail on.
func (s kubernetesPublicationStore) records(ctx context.Context, datasetName string, _ *truenas.Dataset) (map[string]publicationRecord, error) {
	records, _, err := s.list(ctx, datasetName)
	return records, err
}

// lockedRecords is records for a caller holding the volume lock, deciding a
// write: the versions it reads are what the write is compared against.
func (s kubernetesPublicationStore) lockedRecords(ctx context.Context, datasetName string, _ *truenas.Dataset) (map[string]publicationRecord, error) {
	records, objects, err := s.list(ctx, datasetName)
	if err != nil {
		return nil, err
	}
	s.versions.observe(datasetName, objects)
	return records, nil
}

func (s kubernetesPublicationStore) list(ctx context.Context, datasetName string) (map[string]publicationRecord, []*unstructured.Unstructured, error) {
	list, err := s.resource().List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
		labelVolumePublicationInst, shortHash(s.instance), labelVolumePublicationDS, shortHash(datasetName))})
	if err != nil {
		return nil, nil, fmt.Errorf("list publication records for %s: %w", datasetName, err)
	}
	objects := make([]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		objects[i] = &list.Items[i]
	}
	records, err := s.recordsOf(datasetName, objects)
	if err != nil {
		return nil, nil, err
	}
	return records, objects, nil
}

// recordsOf validates the dataset's objects, from the API or the cache, as
// records keyed by node.
func (s kubernetesPublicationStore) recordsOf(datasetName string, objects []*unstructured.Unstructured) (map[string]publicationRecord, error) {
	records := make(map[string]publicationRecord, len(objects))
	for _, object := range objects {
		spec, err := decodeVolumePublicationSpec(object)
		if err != nil {
			return nil, err
		}
		if spec.Dataset != datasetName {
			continue // a label hash collision, not this dataset's record
		}
		record := spec.record()
		if record.Version != publicationRecordVersion || record.Node == "" ||
			(record.State != publicationStatePublished && record.State != publicationStateRemoving) {
			return nil, fmt.Errorf("invalid publication record %s contents", object.GetName())
		}
		key := publicationPropertyKey(record.Node)
		if object.GetName() != s.objectName(datasetName, key) {
			return nil, fmt.Errorf("publication record %s does not match node %q", object.GetName(), record.Node)
		}
		records[key] = record
	}
	return records, nil
}

func decodeVolumePublicationSpec(object *unstructured.Unstructured) (volumePublicationSpec, error) {
	raw, found, err := unstructured.NestedMap(object.Object, "spec")
	if err != nil || !found {
		return volumePublicationSpec{}, fmt.Errorf("publication record %s has no spec", object.GetName())
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return volumePublicationSpec{}, err
	}
	var spec volumePublicationSpec
	if err := json.Unmarshal(encoded, &spec); err != nil {
		return volumePublicationSpec{}, fmt.Errorf("invalid publication record %s: %w", object.GetName(), err)
	}
	return spec, nil
}

func (s kubernetesPublicationStore) object(datasetName, key string, record publicationRecord) (*unstructured.Unstructured, error) {
	encoded, err := json.Marshal(specFromRecord(datasetName, record))
	if err != nil {
		return nil, err
	}
	var spec map[string]interface{}
	if err := json.Unmarshal(encoded, &spec); err != nil {
		return nil, err
	}
	labels := map[string]string{
		labelVolumePublicationInst: shortHash(s.instance),
		labelVolumePublicationDS:   shortHash(datasetName),
	}
	if labelValuePattern.MatchString(record.Node) {
		labels[labelVolumePublicationNode] = record.Node
	}
	object := &unstructured.Unstructured{Object: map[string]interface{}{"spec": spec}}
	object.SetAPIVersion(volumePublicationGVR.GroupVersion().String())
	object.SetKind(volumePublicationKind)
	object.SetName(s.objectName(datasetName, key))
	object.SetNamespace(s.namespace)
	object.SetLabels(labels)
	return object, nil
}

// store creates or replaces the record as a compare-and-set against the read
// the caller made under the volume lock (lockedRecords): an object that read saw is
// updated at the resourceVersion it saw, one it saw absent is created. That is
// one request per write. A dataset this process has not read is read first.
// A conflict (the object changed, appeared or went since the read) is
// reported as errPublicationRecordConflict and never overwritten.
func (s kubernetesPublicationStore) store(ctx context.Context, datasetName string, _ *truenas.Dataset, key string, record publicationRecord) error {
	desired, err := s.object(datasetName, key, record)
	if err != nil {
		return err
	}
	name := desired.GetName()
	resourceVersion, observed, exists := s.versions.lookup(datasetName, name)
	if !observed {
		current, getErr := s.resource().Get(ctx, name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
		case getErr != nil:
			return fmt.Errorf("store publication record %s for %s: %w", name, datasetName, getErr)
		default:
			resourceVersion, exists = current.GetResourceVersion(), true
		}
	}
	var stored *unstructured.Unstructured
	if exists {
		desired.SetResourceVersion(resourceVersion)
		stored, err = s.resource().Update(ctx, desired, metav1.UpdateOptions{})
	} else {
		stored, err = s.resource().Create(ctx, desired, metav1.CreateOptions{})
	}
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) || (exists && apierrors.IsNotFound(err)) {
			s.versions.forget(datasetName)
			return fmt.Errorf("store publication record %s for %s: %w: %w", name, datasetName, errPublicationRecordConflict, err)
		}
		return fmt.Errorf("store publication record %s for %s: %w", name, datasetName, err)
	}
	s.versions.set(datasetName, name, stored.GetResourceVersion())
	return nil
}

// remove deletes the records. An object whose version the locked read (or
// this process's last write) saw is deleted only at that version: one
// changed since is reported as errPublicationRecordConflict and kept. One
// the read saw absent is not deleted: if it exists now it was created since,
// and that is reported the same way. One of a dataset never read is deleted
// as it is.
func (s kubernetesPublicationStore) remove(ctx context.Context, datasetName string, _ *truenas.Dataset, keys []string) error {
	for _, key := range keys {
		name := s.objectName(datasetName, key)
		options := metav1.DeleteOptions{}
		resourceVersion, observed, exists := s.versions.lookup(datasetName, name)
		switch {
		case exists && resourceVersion != "":
			options.Preconditions = &metav1.Preconditions{ResourceVersion: &resourceVersion}
		case observed && !exists:
			_, err := s.resource().Get(ctx, name, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				continue
			case err != nil:
				return fmt.Errorf("remove publication record %s for %s: %w", name, datasetName, err)
			}
			s.versions.forget(datasetName)
			return fmt.Errorf("remove publication record %s for %s: %w: created since it was read", name, datasetName, errPublicationRecordConflict)
		}
		if err := s.resource().Delete(ctx, name, options); err != nil && !apierrors.IsNotFound(err) {
			if apierrors.IsConflict(err) {
				s.versions.forget(datasetName)
				return fmt.Errorf("remove publication record %s for %s: %w: %w", name, datasetName, errPublicationRecordConflict, err)
			}
			return fmt.Errorf("remove publication record %s for %s: %w", name, datasetName, err)
		}
		s.versions.drop(datasetName, name)
	}
	return nil
}

func (s kubernetesPublicationStore) instanceSelector() string {
	return fmt.Sprintf("%s=%s", labelVolumePublicationInst, shortHash(s.instance))
}

// forget deletes every record of the dataset.
func (s kubernetesPublicationStore) forget(ctx context.Context, datasetName string) error {
	defer s.versions.forget(datasetName)
	list, err := s.resource().List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s,%s=%s",
		s.instanceSelector(), labelVolumePublicationDS, shortHash(datasetName))})
	if err != nil {
		return fmt.Errorf("list publication records for %s: %w", datasetName, err)
	}
	for i := range list.Items {
		object := &list.Items[i]
		if spec, err := decodeVolumePublicationSpec(object); err == nil && spec.Dataset != datasetName {
			continue
		}
		if err := s.resource().Delete(ctx, object.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("remove publication record %s for %s: %w", object.GetName(), datasetName, err)
		}
	}
	return nil
}

// publicationListing is every record of a driver instance.
type publicationListing struct {
	// byDataset holds the readable records, by dataset and key.
	byDataset map[string]map[string]publicationRecord
	// objects counts every object, unreadable ones included (the sweep's
	// mass-absence brake weighs them).
	objects int
	// unreadable reports the objects that could not be read.
	unreadable []error
}

// all lists every record of this driver instance.
func (s kubernetesPublicationStore) all(ctx context.Context) (publicationListing, error) {
	list, err := s.resource().List(ctx, metav1.ListOptions{LabelSelector: s.instanceSelector()})
	if err != nil {
		return publicationListing{}, fmt.Errorf("list publication records: %w", err)
	}
	out := make(map[string]map[string]publicationRecord)
	var bad []error
	for i := range list.Items {
		object := &list.Items[i]
		spec, err := decodeVolumePublicationSpec(object)
		if err != nil {
			bad = append(bad, err)
			continue
		}
		record := spec.record()
		key := publicationPropertyKey(record.Node)
		if record.Version != publicationRecordVersion || record.Node == "" ||
			(record.State != publicationStatePublished && record.State != publicationStateRemoving) ||
			object.GetName() != s.objectName(spec.Dataset, key) {
			bad = append(bad, fmt.Errorf("invalid publication record %s contents", object.GetName()))
			continue
		}
		if out[spec.Dataset] == nil {
			out[spec.Dataset] = make(map[string]publicationRecord)
		}
		out[spec.Dataset][key] = record
	}
	return publicationListing{byDataset: out, objects: len(list.Items), unreadable: bad}, nil
}
