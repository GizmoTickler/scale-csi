package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

// A publish whose record write finds the record written by another process
// since its locked read fails Aborted, which the attacher retries at once,
// not Internal.
func TestPublishRecordConflictIsAborted(t *testing.T) {
	d, gated := newLockTestVolume(t, "record-conflict")
	fake := newFakeVolumePublicationClient()
	d.publicationStore = importingPublicationStore{
		kube:   newKubernetesPublicationStore(fake, "scale-csi", "conflict"),
		legacy: zfsPublicationStore{client: gated},
	}
	fake.PrependReactor("create", "volumepublications", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(volumePublicationGVR.GroupResource(), "vp")
	})
	_, err := d.ControllerPublishVolume(context.Background(), lockTestPublishRequest(t, "record-conflict", "worker-a",
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER))
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err), "%v", err)
}
