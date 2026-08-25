package truenas

import (
	"context"
	"fmt"
)

type DatasetAttachment struct {
	Type        string   `json:"type"`
	Service     *string  `json:"service"`
	Attachments []string `json:"attachments"`
}

type DatasetProcess struct {
	PID     int64   `json:"pid"`
	Name    string  `json:"name"`
	Service *string `json:"service"`
	Cmdline *string `json:"cmdline"`
}

func (c *Client) DatasetAttachments(ctx context.Context, name string) ([]DatasetAttachment, error) {
	var attachments []DatasetAttachment
	if err := callTyped(ctx, c, &attachments, "pool.dataset.attachments", name); err != nil {
		return nil, fmt.Errorf("query dataset attachments for %s: %w", name, err)
	}
	return attachments, nil
}

func (c *Client) DatasetProcesses(ctx context.Context, name string) ([]DatasetProcess, error) {
	var processes []DatasetProcess
	if err := callTyped(ctx, c, &processes, "pool.dataset.processes", name); err != nil {
		return nil, fmt.Errorf("query dataset processes for %s: %w", name, err)
	}
	return processes, nil
}
