package driver

import (
	"context"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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

// The interceptor hands the handler a context carrying the RPC's class and
// start, so the TrueNAS calls the handler makes are admitted accordingly.
func TestLogInterceptorPassesTheAdmissionContext(t *testing.T) {
	d := &Driver{config: &Config{}}
	for method, want := range map[string]truenas.Priority{
		csi.Controller_ControllerPublishVolume_FullMethodName: truenas.PriorityAttach,
		csi.Controller_DeleteVolume_FullMethodName:            truenas.PriorityDelete,
		csi.Controller_CreateVolume_FullMethodName:            truenas.PriorityDefault,
	} {
		var seen context.Context
		_, err := d.logInterceptor(context.Background(), &csi.ControllerPublishVolumeRequest{}, &grpc.UnaryServerInfo{FullMethod: method},
			func(ctx context.Context, _ interface{}) (interface{}, error) {
				seen = ctx
				return nil, nil
			})
		require.NoError(t, err)
		require.NotNil(t, seen)
		assert.Equal(t, want, truenas.PriorityOf(seen), method)
		start, ok := truenas.OperationStartOf(seen)
		assert.True(t, ok && !start.IsZero(), "%s carries its start", method)
	}
}
