package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// Each controller RPC's TrueNAS requests carry its admission class: attach and
// detach first, deletes last, everything else in between.
func TestTruenasAdmissionClassPerRPC(t *testing.T) {
	start := time.Now()
	for method, want := range map[string]truenas.Priority{
		csi.Controller_ControllerPublishVolume_FullMethodName:   truenas.PriorityAttach,
		csi.Controller_ControllerUnpublishVolume_FullMethodName: truenas.PriorityAttach,
		csi.Controller_CreateVolume_FullMethodName:              truenas.PriorityDefault,
		csi.Controller_ControllerExpandVolume_FullMethodName:    truenas.PriorityDefault,
		csi.Controller_CreateSnapshot_FullMethodName:            truenas.PriorityDefault,
		csi.Controller_DeleteVolume_FullMethodName:              truenas.PriorityDelete,
		csi.Controller_DeleteSnapshot_FullMethodName:            truenas.PriorityDelete,
	} {
		ctx := truenasAdmissionContext(context.Background(), method, start)
		assert.Equal(t, want, truenas.PriorityOf(ctx), method)
	}
	assert.Equal(t, truenas.PriorityDefault, truenas.PriorityOf(context.Background()), "unmarked work is the default class")
}
