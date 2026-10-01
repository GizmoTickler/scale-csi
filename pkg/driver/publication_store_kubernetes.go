package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

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
	volumePublicationConflicts = 3
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
}

func (s kubernetesPublicationStore) objectName(datasetName, key string) string {
	return "vp-" + shortHash(s.instance, datasetName, key)
}

func (s kubernetesPublicationStore) resource() dynamic.ResourceInterface {
	return s.client.Resource(volumePublicationGVR).Namespace(s.namespace)
}

func (s kubernetesPublicationStore) records(ctx context.Context, datasetName string, _ *truenas.Dataset) (map[string]publicationRecord, error) {
	list, err := s.resource().List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
		labelVolumePublicationInst, shortHash(s.instance), labelVolumePublicationDS, shortHash(datasetName))})
	if err != nil {
		return nil, fmt.Errorf("list publication records for %s: %w", datasetName, err)
	}
	records := make(map[string]publicationRecord, len(list.Items))
	for i := range list.Items {
		object := &list.Items[i]
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

// store creates or replaces the record. Callers hold the volume lock; a
// conflict means another process wrote the same object, and the caller's
// record (decided under the lock) replaces it, as a ZFS property write did.
func (s kubernetesPublicationStore) store(ctx context.Context, datasetName string, _ *truenas.Dataset, key string, record publicationRecord) error {
	desired, err := s.object(datasetName, key, record)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		current, err := s.resource().Get(ctx, desired.GetName(), metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			_, err = s.resource().Create(ctx, desired, metav1.CreateOptions{})
		case err == nil:
			desired.SetResourceVersion(current.GetResourceVersion())
			_, err = s.resource().Update(ctx, desired, metav1.UpdateOptions{})
		}
		if err == nil {
			return nil
		}
		if (!apierrors.IsConflict(err) && !apierrors.IsAlreadyExists(err)) || attempt+1 >= volumePublicationConflicts {
			return fmt.Errorf("store publication record %s for %s: %w", desired.GetName(), datasetName, err)
		}
	}
}

func (s kubernetesPublicationStore) remove(ctx context.Context, datasetName string, _ *truenas.Dataset, keys []string) error {
	for _, key := range keys {
		name := s.objectName(datasetName, key)
		if err := s.resource().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("remove publication record %s for %s: %w", name, datasetName, err)
		}
	}
	return nil
}

func (s kubernetesPublicationStore) instanceSelector() string {
	return fmt.Sprintf("%s=%s", labelVolumePublicationInst, shortHash(s.instance))
}

// forget deletes every record of the dataset.
func (s kubernetesPublicationStore) forget(ctx context.Context, datasetName string) error {
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

// all returns every record of this driver instance, by dataset and key, and
// how many objects there are (unreadable ones included, for the sweep's
// mass-absence brake). An unreadable object is reported, not skipped silently.
func (s kubernetesPublicationStore) all(ctx context.Context) (map[string]map[string]publicationRecord, int, []error, error) {
	list, err := s.resource().List(ctx, metav1.ListOptions{LabelSelector: s.instanceSelector()})
	if err != nil {
		return nil, 0, nil, fmt.Errorf("list publication records: %w", err)
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
	return out, len(list.Items), bad, nil
}

