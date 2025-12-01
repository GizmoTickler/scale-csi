// Package util provides utility functions for filesystem and mount operations.
package util

import (
	"fmt"
	"os"
	"os/exec"
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

	// Use findmnt to check mount status
	cmd := exec.Command("findmnt", "--mountpoint", path, "--noheadings")
	output, err := cmd.Output()
	if err != nil {
		// Exit code 1 means not mounted
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, err
	}

	return len(strings.TrimSpace(string(output))) > 0, nil
}

// Mount mounts a source to a target with the given filesystem type and options.
func Mount(source, target, fsType string, options []string) error {
	klog.V(4).Infof("Mounting %s to %s (fsType=%s, options=%v)", source, target, fsType, options)

	args := []string{}
	if fsType != "" {
		args = append(args, "-t", fsType)
	}
	if len(options) > 0 {
		args = append(args, "-o", strings.Join(options, ","))
	}
	args = append(args, source, target)

	cmd := exec.Command("mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount failed: %v, output: %s", err, string(output))
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

	args := []string{"--bind"}
	if len(options) > 0 {
		args = append(args, "-o", strings.Join(options, ","))
	}
	args = append(args, source, target)

	cmd := exec.Command("mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bind mount failed: %v, output: %s", err, string(output))
	}

	return nil
}

// Unmount unmounts a target path.
func Unmount(target string) error {
	klog.V(4).Infof("Unmounting %s", target)

	// Check if mounted
	mounted, err := IsMounted(target)
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}

	cmd := exec.Command("umount", target)
	if _, err := cmd.CombinedOutput(); err != nil {
		// Try lazy unmount
		cmd = exec.Command("umount", "-l", target)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("unmount failed: %v, output: %s", err, string(output))
		}
	}

	return nil
}

// FormatAndMount formats a device and mounts it.
func FormatAndMount(devicePath, target, fsType string, options []string) error {
	klog.V(4).Infof("FormatAndMount: device=%s, target=%s, fsType=%s", devicePath, target, fsType)

	// Check if already formatted
	existingFS, err := GetFilesystemType(devicePath)
	if err != nil {
		klog.Warningf("Failed to get filesystem type: %v", err)
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

	var cmd *exec.Cmd
	switch fsType {
	case "ext4":
		cmd = exec.Command("mkfs.ext4", "-F", devicePath)
	case "ext3":
		cmd = exec.Command("mkfs.ext3", "-F", devicePath)
	case "xfs":
		cmd = exec.Command("mkfs.xfs", "-f", devicePath)
	case "btrfs":
		cmd = exec.Command("mkfs.btrfs", "-f", devicePath)
	default:
		return fmt.Errorf("unsupported filesystem type: %s", fsType)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("format failed: %v, output: %s", err, string(output))
	}

	return nil
}

// GetFilesystemType returns the filesystem type of a device.
func GetFilesystemType(devicePath string) (string, error) {
	cmd := exec.Command("blkid", "-o", "value", "-s", "TYPE", devicePath)
	output, err := cmd.Output()
	if err != nil {
		// Device may not be formatted yet
		return "", nil
	}
	return strings.TrimSpace(string(output)), nil
}

// GetFilesystemStats returns filesystem statistics for a path.
func GetFilesystemStats(path string) (*FilesystemStats, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, fmt.Errorf("statfs failed: %v", err)
	}

	blockSize := int64(stat.Bsize)

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
		return fmt.Errorf("failed to get device from mount point: %v", err)
	}

	// Get filesystem type
	fsType, err := GetFilesystemType(devicePath)
	if err != nil {
		return err
	}

	// Resize based on filesystem type
	var cmd *exec.Cmd
	switch fsType {
	case "ext4", "ext3", "ext2":
		cmd = exec.Command("resize2fs", devicePath)
	case "xfs":
		cmd = exec.Command("xfs_growfs", mountPath)
	case "btrfs":
		cmd = exec.Command("btrfs", "filesystem", "resize", "max", mountPath)
	default:
		return fmt.Errorf("resize not supported for filesystem type: %s", fsType)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("resize failed: %v, output: %s", err, string(output))
	}

	return nil
}

// GetDeviceFromMountPoint returns the device path for a mount point.
func GetDeviceFromMountPoint(mountPath string) (string, error) {
	cmd := exec.Command("findmnt", "-n", "-o", "SOURCE", mountPath)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to find device: %v", err)
	}
	return strings.TrimSpace(string(output)), nil
}
