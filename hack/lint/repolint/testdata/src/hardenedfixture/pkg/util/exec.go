// Package util is analysistest fixture data for RG-EXEC-UNHARDENED.
package util

import (
	"context"
	"os/exec"
)

func hardenCmd(cmd *exec.Cmd) {}

// HardenCmd is the exported spelling other packages use.
func HardenCmd(cmd *exec.Cmd) {}

func unhardened(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys") // want `RG-EXEC-UNHARDENED`
	_, _ = cmd.Output()
}

func hardenedLocal(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys")
	hardenCmd(cmd)
	_, _ = cmd.Output()
}

// The exported spelling must be accepted too: rejecting it produced false
// positives on correct code, including a site that had just been fixed.
func hardenedExported(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys")
	HardenCmd(cmd)
	_, _ = cmd.Output()
}
