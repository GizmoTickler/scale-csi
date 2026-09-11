// Package util is analysistest fixture data for RG-BLKID-NO-P. The import path
// must end in /pkg/util because the rule is deliberately scoped to the packages
// that shell out to host tools.
package util

import (
	"context"
	"os/exec"
)

// The shape that actually ships: flags are literals, the device path is a
// VARIABLE. This is what the rule silently failed to catch.
func missingDashP(ctx context.Context, devicePath string) {
	cmd := exec.CommandContext(ctx, "blkid", "-s", "TYPE", "-o", "export", devicePath) // want `RG-BLKID-NO-P`
	_, _ = cmd.Output()
}

func hasDashP(ctx context.Context, devicePath string) {
	cmd := exec.CommandContext(ctx, "blkid", "-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", devicePath)
	_, _ = cmd.Output()
}

// A genuine variadic spread: argv is not statically visible, so stay silent.
func variadicSpread(ctx context.Context, args []string) {
	cmd := exec.CommandContext(ctx, "blkid", args...)
	_, _ = cmd.Output()
}

func notBlkid(ctx context.Context, devicePath string) {
	cmd := exec.CommandContext(ctx, "lsblk", "-o", "NAME", devicePath)
	_, _ = cmd.Output()
}
