// Package util provides utility functions for filesystem and mount operations.
package util

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

// MountInfo is the live kernel view of one mount.
type MountInfo struct {
	Source   string
	Target   string
	FSType   string
	Options  []string
	ReadOnly bool
}

// GetMountInfo returns the backing source, filesystem type, and effective
// options for an exact mountpoint.
func GetMountInfo(target string) (MountInfo, error) {
	return GetMountInfoWithContext(context.Background(), target)
}

// GetMountInfoWithContext is GetMountInfo bounded by the inbound context's
// deadline as well as the configured mount timeout.
func GetMountInfoWithContext(ctx context.Context, target string) (MountInfo, error) {
	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return MountInfo{}, err
	}
	defer cancel()

	output, err := exec.CommandContext(
		ctx,
		"findmnt",
		"--first-only",
		"--noheadings",
		"--output",
		"SOURCE,FSTYPE,OPTIONS",
		"--mountpoint",
		target,
	).Output()
	if err != nil {
		return MountInfo{}, fmt.Errorf("failed to inspect mountpoint %s: %w", target, err)
	}
	fields := strings.Fields(strings.TrimSpace(string(output)))
	if len(fields) < 3 {
		return MountInfo{}, fmt.Errorf("unexpected findmnt output for %s: %q", target, strings.TrimSpace(string(output)))
	}
	options := strings.Split(fields[2], ",")
	return MountInfo{
		Source:   unescapeProcMountField(fields[0]),
		Target:   target,
		FSType:   fields[1],
		Options:  options,
		ReadOnly: containsMountOption(options, "ro"),
	}, nil
}

// ListMountInfo returns the process mount namespace from Linux mountinfo. On
// non-Linux development hosts where procfs is absent, it returns an empty
// result so callers can fall back to their in-memory state.
func ListMountInfo() ([]MountInfo, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open mountinfo: %w", err)
	}
	defer func() { _ = file.Close() }()
	return parseMountInfo(file)
}

func parseMountInfo(reader io.Reader) ([]MountInfo, error) {
	var mounts []MountInfo
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if len(fields) < 6 || separator < 0 || separator+3 >= len(fields) {
			continue
		}
		options := append(strings.Split(fields[5], ","), strings.Split(fields[separator+3], ",")...)
		mounts = append(mounts, MountInfo{
			Source:   unescapeProcMountField(fields[separator+2]),
			Target:   unescapeProcMountField(fields[4]),
			FSType:   fields[separator+1],
			Options:  options,
			ReadOnly: containsMountOption(options, "ro"),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read mountinfo: %w", err)
	}
	return mounts, nil
}

// IsMounted checks if a path is currently mounted.
func IsMounted(path string) (bool, error) {
	return IsMountedWithContext(context.Background(), path)
}

// IsMountedWithContext is IsMounted bounded by the inbound context's deadline as
// well as the configured mount timeout.
func IsMountedWithContext(ctx context.Context, path string) (bool, error) {
	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return false, err
	}
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
	return MountWithContext(context.Background(), source, target, fsType, options)
}

// MountWithContext is Mount bounded by the inbound context's deadline as well as
// the configured mount timeout.
func MountWithContext(ctx context.Context, source, target, fsType string, options []string) error {
	klog.V(4).Infof("Mounting %s to %s (fsType=%s, options=%v)", source, target, fsType, options)
	if fsType != "" && fsType != "nfs" {
		if err := validateBlockFilesystemType(fsType); err != nil {
			return err
		}
	}

	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return err
	}
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
	return MountNFSWithContext(context.Background(), source, target, options)
}

// MountNFSWithContext is MountNFS bounded by the inbound context's deadline.
func MountNFSWithContext(ctx context.Context, source, target string, options []string) error {
	// Add NFS-specific default options
	nfsOptions := []string{"nfsvers=4"}
	nfsOptions = append(nfsOptions, options...)

	return MountWithContext(ctx, source, target, "nfs", nfsOptions)
}

// BindMount creates a bind mount.
func BindMount(source, target string, options []string) error {
	return BindMountWithContext(context.Background(), source, target, options)
}

// BindMountWithContext is BindMount bounded by the inbound context's deadline as
// well as the configured mount timeout.
func BindMountWithContext(ctx context.Context, source, target string, options []string) error {
	klog.V(4).Infof("Bind mounting %s to %s", source, target)

	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return err
	}
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
	fsType, fsTypeErr := getMountFilesystemType(target)
	if fsTypeErr != nil {
		// Unknown filesystem types are treated conservatively: a failed regular
		// unmount will be surfaced rather than lazily detaching a device mount.
		klog.Warningf("Could not determine filesystem type for %s: %v", target, fsTypeErr)
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
		unmountErr := fmt.Errorf("unmount failed: %w, output: %s", err, string(output))

		// Busy errors are retried by RetryWithBackoff. Lazy unmount is reserved
		// for unreachable network filesystems; device-backed and unknown mounts
		// must surface the original error before callers disconnect the session.
		if !IsRetryableError(unmountErr, retryCfg.RetryableErrors) {
			if !isNetworkFilesystem(fsType) {
				return unmountErr
			}
			klog.Warningf("Regular unmount of network filesystem %s (%s) failed, trying lazy unmount: %v", target, fsType, unmountErr)
			cmd = exec.CommandContext(cmdCtx, "umount", "-l", target)
			if lazyOutput, lazyErr := cmd.CombinedOutput(); lazyErr != nil {
				return errors.Join(
					unmountErr,
					fmt.Errorf("lazy unmount failed: %w, output: %s", lazyErr, string(lazyOutput)),
				)
			}
			return nil // Lazy unmount succeeded
		}

		// Return the error to trigger retry
		return unmountErr
	})
}

func getMountFilesystemType(target string) (string, error) {
	entry, procErr := readProcMountEntry("/proc/self/mounts", target)
	if procErr == nil {
		return entry.fsType, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), getMountTimeout())
	defer cancel()

	output, err := exec.CommandContext(ctx, "findmnt", "-n", "-o", "FSTYPE", "--mountpoint", target).Output()
	if err != nil {
		findmntErr := fmt.Errorf("failed to query mount filesystem type with findmnt: %w", err)
		if isNFSMountSource(entry.source) {
			return "", fmt.Errorf("cannot safely classify NFS-looking mount source %q: %w", entry.source, errors.Join(procErr, findmntErr))
		}
		return "", errors.Join(procErr, findmntErr)
	}
	fsType := strings.TrimSpace(string(output))
	if fsType == "" {
		return "", errors.Join(procErr, errors.New("findmnt returned an empty filesystem type"))
	}
	return fsType, nil
}

type procMountEntry struct {
	source string
	target string
	fsType string
}

func readProcMountEntry(path, target string) (procMountEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return procMountEntry{}, fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	return parseProcMounts(file, target)
}

func parseProcMounts(reader io.Reader, target string) (procMountEntry, error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		entry := procMountEntry{
			source: unescapeProcMountField(fields[0]),
			target: unescapeProcMountField(fields[1]),
		}
		if entry.target != target {
			continue
		}
		if len(fields) < 3 {
			return entry, fmt.Errorf("mount entry for %s has no filesystem type", target)
		}
		entry.fsType = unescapeProcMountField(fields[2])
		if entry.fsType == "" {
			return entry, fmt.Errorf("mount entry for %s has an empty filesystem type", target)
		}
		return entry, nil
	}
	if err := scanner.Err(); err != nil {
		return procMountEntry{}, fmt.Errorf("failed to read proc mounts: %w", err)
	}
	return procMountEntry{}, fmt.Errorf("mountpoint %s not found in proc mounts", target)
}

func unescapeProcMountField(value string) string {
	var builder strings.Builder
	builder.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+3 < len(value) {
			if decoded, err := strconv.ParseUint(value[i+1:i+4], 8, 8); err == nil {
				builder.WriteByte(byte(decoded))
				i += 3
				continue
			}
		}
		builder.WriteByte(value[i])
	}
	return builder.String()
}

func isNFSMountSource(source string) bool {
	return strings.LastIndex(source, ":/") > 0
}

func isNetworkFilesystem(fsType string) bool {
	return fsType == "nfs" || fsType == "nfs4"
}

// FormatAndMount formats a device and mounts it.
func FormatAndMount(devicePath, target, fsType string, options []string) error {
	return FormatAndMountWithContext(context.Background(), devicePath, target, fsType, options)
}

// FormatAndMountWithContext is FormatAndMount bounded by the inbound context's
// deadline as well as the configured format/mount timeouts.
func FormatAndMountWithContext(ctx context.Context, devicePath, target, fsType string, options []string) error {
	klog.V(4).Infof("FormatAndMount: device=%s, target=%s, fsType=%s", devicePath, target, fsType)
	if err := validateBlockFilesystemType(fsType); err != nil {
		return err
	}

	// Check if already formatted
	existingFS, err := GetFilesystemTypeWithContext(ctx, devicePath)
	if err != nil {
		return fmt.Errorf("failed to get filesystem type for %s: %w", devicePath, err)
	}

	if existingFS == "" {
		// Format the device
		if err := FormatDeviceWithContext(ctx, devicePath, fsType); err != nil {
			return err
		}
	} else if existingFS != fsType {
		return fmt.Errorf("device %s has filesystem %s, requested %s", devicePath, existingFS, fsType)
	}

	// Mount the device
	return MountWithContext(ctx, devicePath, target, fsType, options)
}

// FormatDevice formats a block device with the given filesystem type.
func FormatDevice(devicePath, fsType string) error {
	return FormatDeviceWithContext(context.Background(), devicePath, fsType)
}

// FormatDeviceWithContext is FormatDevice bounded by the inbound context's
// deadline as well as the configured format timeout.
func FormatDeviceWithContext(ctx context.Context, devicePath, fsType string) error {
	klog.Infof("Formatting device %s with %s", devicePath, fsType)
	if err := validateBlockFilesystemType(fsType); err != nil {
		return err
	}

	ctx, cancel, err := commandContext(ctx, getFormatTimeout())
	if err != nil {
		return err
	}
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
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("format failed: %w, output: %s", err, string(output))
	}

	return nil
}

func validateBlockFilesystemType(fsType string) error {
	switch fsType {
	case "ext4", "ext3", "xfs", "btrfs":
		return nil
	default:
		return fmt.Errorf("invalid argument: unsupported filesystem type %q; supported types are ext4, ext3, xfs, btrfs", fsType)
	}
}

// GetFilesystemType returns the filesystem type of a device.
func GetFilesystemType(devicePath string) (string, error) {
	return GetFilesystemTypeWithContext(context.Background(), devicePath)
}

// GetFilesystemTypeWithContext is GetFilesystemType bounded by the inbound
// context's deadline as well as the configured mount timeout.
func GetFilesystemTypeWithContext(ctx context.Context, devicePath string) (string, error) {
	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return "", err
	}
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
	return ResizeFilesystemWithContext(context.Background(), mountPath)
}

// ResizeFilesystemWithContext is ResizeFilesystem bounded by the inbound
// context's deadline as well as the configured format timeout.
func ResizeFilesystemWithContext(ctx context.Context, mountPath string) error {
	klog.Infof("Resizing filesystem at %s", mountPath)

	// Get the device path
	devicePath, err := GetDeviceFromMountPointWithContext(ctx, mountPath)
	if err != nil {
		return fmt.Errorf("failed to get device from mount point: %w", err)
	}

	// Get filesystem type
	fsType, err := GetFilesystemTypeWithContext(ctx, devicePath)
	if err != nil {
		return err
	}

	ctx, cancel, err := commandContext(ctx, getFormatTimeout())
	if err != nil {
		return err
	}
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
	return GetDeviceFromMountPointWithContext(context.Background(), mountPath)
}

// GetDeviceFromMountPointWithContext is GetDeviceFromMountPoint bounded by the
// inbound context's deadline as well as the configured mount timeout.
func GetDeviceFromMountPointWithContext(ctx context.Context, mountPath string) (string, error) {
	ctx, cancel, err := commandContext(ctx, getMountTimeout())
	if err != nil {
		return "", err
	}
	defer cancel()

	cmd := exec.CommandContext(ctx, "findmnt", "--first-only", "-n", "-o", "SOURCE", mountPath)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to find device: %w", err)
	}
	device := strings.TrimSpace(string(output))
	if line, _, found := strings.Cut(device, "\n"); found {
		device = strings.TrimSpace(line)
	}
	return device, nil
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
