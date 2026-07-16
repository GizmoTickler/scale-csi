// Package util provides utility functions for filesystem and mount operations.
package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"k8s.io/klog/v2"
)

// FilesystemStats holds filesystem statistics.
type FilesystemStats struct {
	TotalBytes      int64
	AvailableBytes  int64
	UsedBytes       int64
	TotalInodes     int64
	AvailableInodes int64
	UsedInodes      int64
}

// IsMounted checks if a path is currently mounted.
func IsMounted(path string) (bool, error) {
	// Check if path exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	// Use findmnt to check mount status
	cmd := exec.CommandContext(ctx, "findmnt", "--mountpoint", path, "--noheadings")
	output, err := cmd.Output()
	if err != nil {
		// Exit code 1 means not mounted
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, err
	}

	return strings.TrimSpace(string(output)) != "", nil
}

// Mount mounts a source to a target with the given filesystem type and options.
func Mount(source, target, fsType string, options []string) error {
	klog.V(4).Infof("Mounting %s to %s (fsType=%s, options=%v)", source, target, fsType, options)

	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	args := []string{}
	if fsType != "" {
		args = append(args, "-t", fsType)
	}
	if len(options) > 0 {
		args = append(args, "-o", strings.Join(options, ","))
	}
	args = append(args, source, target)

	cmd := exec.CommandContext(ctx, "mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount failed: %w, output: %s", err, string(output))
	}

	return nil
}

// MountNFS mounts an NFS share.
func MountNFS(source, target string, options []string) error {
	// Add NFS-specific default options
	nfsOptions := []string{"nfsvers=4"}
	nfsOptions = append(nfsOptions, options...)

	return Mount(source, target, "nfs", nfsOptions)
}

// BindMount creates a bind mount.
func BindMount(source, target string, options []string) error {
	klog.V(4).Infof("Bind mounting %s to %s", source, target)

	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	// Use "-o bind" instead of "--bind" for BusyBox compatibility
	// BusyBox mount doesn't support GNU-style long options
	mountOptions := []string{"bind"}
	mountOptions = append(mountOptions, options...)
	args := []string{"-o", strings.Join(mountOptions, ",")}
	args = append(args, source, target)

	cmd := exec.CommandContext(ctx, "mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bind mount failed: %w, output: %s", err, string(output))
	}

	// A read-only bind mount requires a separate remount on BusyBox and older
	// util-linux versions. The ro flag on the initial bind is otherwise ignored.
	if containsMountOption(options, "ro") {
		cmd = exec.CommandContext(ctx, "mount", "-o", "remount,bind,ro", target)
		output, err = cmd.CombinedOutput()
		if err != nil {
			remountErr := fmt.Errorf("read-only bind remount failed: %w, output: %s", err, string(output))
			if unmountErr := Unmount(target); unmountErr != nil {
				return errors.Join(remountErr, fmt.Errorf("failed to clean up bind mount: %w", unmountErr))
			}
			return remountErr
		}
	}

	return nil
}

func containsMountOption(options []string, wanted string) bool {
	for _, option := range options {
		if option == wanted {
			return true
		}
	}
	return false
}

// Unmount unmounts a target path with retry for transient failures.
func Unmount(target string) error {
	return UnmountWithContext(context.Background(), target)
}

// UnmountWithContext unmounts a target path with context and retry for transient failures.
func UnmountWithContext(ctx context.Context, target string) error {
	klog.V(4).Infof("Unmounting %s", target)

	// Check if mounted
	mounted, err := IsMounted(target)
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}

	retryCfg := UnmountRetryConfig()

	return RetryWithBackoff(ctx, "unmount "+target, retryCfg, func() error {
		cmdCtx, cancel := context.WithTimeout(ctx, getMountTimeout())
		defer cancel()

		cmd := exec.CommandContext(cmdCtx, "umount", target)
		output, err := cmd.CombinedOutput()
		if err == nil {
			return nil
		}

		// Check if it was a transient "busy" error before trying lazy unmount
		if !IsRetryableError(err, retryCfg.RetryableErrors) {
			// Not retryable with regular unmount, try lazy unmount as last resort
			cmd = exec.CommandContext(cmdCtx, "umount", "-l", target)
			if lazyOutput, lazyErr := cmd.CombinedOutput(); lazyErr != nil {
				return errors.Join(
					fmt.Errorf("unmount failed: %w, output: %s", err, string(output)),
					fmt.Errorf("lazy unmount failed: %w, output: %s", lazyErr, string(lazyOutput)),
				)
			}
			return nil // Lazy unmount succeeded
		}

		// Return the error to trigger retry
		return fmt.Errorf("unmount failed: %w, output: %s", err, string(output))
	})
}

// FormatAndMount formats a device and mounts it.
func FormatAndMount(devicePath, target, fsType string, options []string) error {
	klog.V(4).Infof("FormatAndMount: device=%s, target=%s, fsType=%s", devicePath, target, fsType)

	// Check if already formatted
	existingFS, err := GetFilesystemType(devicePath)
	if err != nil {
		return fmt.Errorf("failed to get filesystem type for %s: %w", devicePath, err)
	}

	if existingFS == "" {
		// Format the device
		if err := FormatDevice(devicePath, fsType); err != nil {
			return err
		}
	} else if existingFS != fsType {
		klog.Warningf("Device %s already formatted with %s, expected %s", devicePath, existingFS, fsType)
	}

	// Mount the device
	return Mount(devicePath, target, fsType, options)
}

// FormatDevice formats a block device with the given filesystem type.
func FormatDevice(devicePath, fsType string) error {
	klog.Infof("Formatting device %s with %s", devicePath, fsType)

	ctx, cancel := context.WithTimeout(context.Background(), getFormatTimeout())
	defer cancel()

	var cmd *exec.Cmd
	switch fsType {
	case "ext4":
		cmd = exec.CommandContext(ctx, "mkfs.ext4", "-F", devicePath)
	case "ext3":
		cmd = exec.CommandContext(ctx, "mkfs.ext3", "-F", devicePath)
	case "xfs":
		cmd = exec.CommandContext(ctx, "mkfs.xfs", "-f", devicePath)
	case "btrfs":
		cmd = exec.CommandContext(ctx, "mkfs.btrfs", "-f", devicePath)
	default:
		return fmt.Errorf("unsupported filesystem type: %s", fsType)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("format failed: %w, output: %s", err, string(output))
	}

	return nil
}

// GetFilesystemType returns the filesystem type of a device.
func GetFilesystemType(devicePath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	// Keep stdout separate from stderr: the success path returns stdout as the
	// filesystem type, and exit code 2 only means "no filesystem" when blkid
	// produced no output at all.
	cmd := exec.CommandContext(ctx, "blkid", "-o", "value", "-s", "TYPE", devicePath)
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 &&
			strings.TrimSpace(string(output)) == "" && strings.TrimSpace(string(exitErr.Stderr)) == "" {
			return "", nil
		}
		var stderr []byte
		if exitErr != nil {
			stderr = exitErr.Stderr
		}
		return "", fmt.Errorf("blkid failed for %s: %w, output: %s, stderr: %s", devicePath, err, string(output), string(stderr))
	}
	return strings.TrimSpace(string(output)), nil
}

// GetFilesystemStats returns filesystem statistics for a path.
func GetFilesystemStats(path string) (*FilesystemStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, fmt.Errorf("statfs failed: %w", err)
	}

	// Bsize is int64 on linux but uint32 on darwin; the conversion is required
	// for portable builds even though it is a no-op on linux.
	blockSize := int64(stat.Bsize) //nolint:unconvert

	return &FilesystemStats{
		TotalBytes:      int64(stat.Blocks) * blockSize,
		AvailableBytes:  int64(stat.Bavail) * blockSize,
		UsedBytes:       (int64(stat.Blocks) - int64(stat.Bfree)) * blockSize,
		TotalInodes:     int64(stat.Files),
		AvailableInodes: int64(stat.Ffree),
		UsedInodes:      int64(stat.Files) - int64(stat.Ffree),
	}, nil
}

// ResizeFilesystem resizes the filesystem on a mounted path.
func ResizeFilesystem(mountPath string) error {
	klog.Infof("Resizing filesystem at %s", mountPath)

	// Get the device path
	devicePath, err := GetDeviceFromMountPoint(mountPath)
	if err != nil {
		return fmt.Errorf("failed to get device from mount point: %w", err)
	}

	// Get filesystem type
	fsType, err := GetFilesystemType(devicePath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), getFormatTimeout())
	defer cancel()

	// Resize based on filesystem type
	var cmd *exec.Cmd
	switch fsType {
	case "ext4", "ext3", "ext2":
		cmd = exec.CommandContext(ctx, "resize2fs", devicePath)
	case "xfs":
		cmd = exec.CommandContext(ctx, "xfs_growfs", mountPath)
	case "btrfs":
		cmd = exec.CommandContext(ctx, "btrfs", "filesystem", "resize", "max", mountPath)
	default:
		return fmt.Errorf("resize not supported for filesystem type: %s", fsType)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("resize failed: %w, output: %s", err, string(output))
	}

	return nil
}

// GetDeviceFromMountPoint returns the device path for a mount point.
func GetDeviceFromMountPoint(mountPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, "findmnt", "-n", "-o", "SOURCE", mountPath)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to find device: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

// GetMountedBlockDevices returns a map of all mounted block devices.
// The keys are device paths (e.g., "/dev/sda1"), values are mount points.
// This is used by session GC to determine which iSCSI/NVMe devices are in use.
func GetMountedBlockDevices() (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	// Use findmnt to list all block device mounts
	// -n: no headers, -l: list format, -o: output columns
	cmd := exec.CommandContext(ctx, "findmnt", "-n", "-l", "-o", "SOURCE,TARGET", "-t", "ext4,ext3,xfs,btrfs")
	output, err := cmd.Output()
	if err != nil {
		// Exit code 1 with empty output means no mounts found (not an error)
		// Exit code 1 with non-empty stderr indicates an actual error
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			// Only treat as "no mounts" if there's no output
			if len(output) == 0 || strings.TrimSpace(string(output)) == "" {
				return make(map[string]string), nil
			}
			// Non-empty output with exit code 1 is unexpected, log and continue parsing
			klog.V(4).Infof("findmnt returned exit code 1 with output, continuing: %s", string(output))
		} else {
			return nil, fmt.Errorf("findmnt failed: %w", err)
		}
	}

	devices := make(map[string]string)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: SOURCE TARGET (space-separated)
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			device := fields[0]
			target := fields[1]
			// Only include actual block devices (skip things like tmpfs, overlay, etc.)
			if strings.HasPrefix(device, "/dev/") {
				devices[device] = target
			}
		}
	}

	return devices, nil
}

// GetStagedBlockDevices returns block devices referenced by symlinks below a
// kubelet CSI staging directory. Raw block volumes are staged as symlinks, so
// they do not appear in GetMountedBlockDevices.
func GetStagedBlockDevices(stagingRoot string) (map[string]string, error) {
	devices := make(map[string]string)

	// A missing staging root just means no CSI volume was ever staged on this
	// node; treat it as empty rather than an error so session GC still runs.
	if _, err := os.Stat(stagingRoot); os.IsNotExist(err) {
		return devices, nil
	}

	err := filepath.WalkDir(stagingRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink == 0 {
			// Filesystem-mode staging targets are mounted directories. Do not
			// descend into volume data while looking for raw-block symlinks.
			if entry.IsDir() && path != stagingRoot && filepath.Base(path) == "globalmount" {
				return filepath.SkipDir
			}
			return nil
		}

		devicePath, err := filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("failed to resolve staged block device symlink %s: %w", path, err)
		}
		if !strings.HasPrefix(devicePath, "/dev/") {
			return nil
		}

		devices[devicePath] = path
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan CSI staging directory %s: %w", stagingRoot, err)
	}

	return devices, nil
}
