package driver

import (
	"context"
	"testing"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// rendezvousBusyClient makes each busy-observation read wait for the other to
// start. Run sequentially, the first read never sees the second and the probe
// stalls until the test times out.
type rendezvousBusyClient struct {
	*truenas.MockClient
	attachEntered  chan struct{}
	processEntered chan struct{}
}

func (c *rendezvousBusyClient) DatasetAttachments(ctx context.Context, name string) ([]truenas.DatasetAttachment, error) {
	close(c.attachEntered)
	select {
	case <-c.processEntered:
	case <-time.After(2 * time.Second):
	}
	return c.MockClient.DatasetAttachments(ctx, name)
}

func (c *rendezvousBusyClient) DatasetProcesses(ctx context.Context, name string) ([]truenas.DatasetProcess, error) {
	close(c.processEntered)
	select {
	case <-c.attachEntered:
	case <-time.After(2 * time.Second):
	}
	return c.MockClient.DatasetProcesses(ctx, name)
}

func TestObserveDatasetBusyBeforeDeleteRunsReadsConcurrently(t *testing.T) {
	client := &rendezvousBusyClient{
		MockClient:     truenas.NewMockClient(),
		attachEntered:  make(chan struct{}),
		processEntered: make(chan struct{}),
	}
	d := &Driver{truenasClient: client}

	start := time.Now()
	d.observeDatasetBusyBeforeDelete(context.Background(), "pool/parent/vol", "DeleteVolume")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("busy-observation reads ran sequentially (%v); they are independent and must overlap", elapsed)
	}
}
