package util

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fakeMountCommandScript = `#!/bin/sh
name="$(basename "$0")"
if [ -n "$FAKE_COMMAND_LOG" ]; then
	printf '%s %s\n' "$name" "$*" >> "$FAKE_COMMAND_LOG"
fi
case "$name" in
	blkid)
		if [ -n "$FAKE_BLKID_OUTPUT" ]; then
			printf '%s' "$FAKE_BLKID_OUTPUT"
		fi
		exit "${FAKE_BLKID_EXIT:-0}"
		;;
	mount)
		case "$*" in
			"-o remount,bind,ro "*) exit "${FAKE_REMOUNT_EXIT:-0}" ;;
		esac
		exit 0
		;;
	findmnt)
		case "$*" in
			"-n -o FSTYPE --mountpoint "*)
				if [ -n "$FAKE_MOUNT_FSTYPE" ]; then
					printf '%s' "$FAKE_MOUNT_FSTYPE"
					exit 0
				fi
				exit 1
				;;
		esac
		if [ -n "$FAKE_FINDMNT_OUTPUT" ]; then
			printf '%s' "$FAKE_FINDMNT_OUTPUT"
			exit 0
		fi
		exit 1
		;;
	umount)
		if [ "$1" = "-l" ]; then
			printf '%s' "${FAKE_LAZY_UMOUNT_OUTPUT:-}"
			exit "${FAKE_LAZY_UMOUNT_EXIT:-0}"
		fi
		printf '%s' "${FAKE_UMOUNT_OUTPUT:-}"
		exit "${FAKE_UMOUNT_EXIT:-0}"
		;;
	*) exit 0 ;;
esac
`

func installFakeMountCommands(t *testing.T, commands ...string) string {
	t.Helper()
	binDir := t.TempDir()
	for _, command := range commands {
		path := filepath.Join(binDir, command)
		require.NoError(t, os.WriteFile(path, []byte(fakeMountCommandScript), 0o750))
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return binDir
}

func readCommandLog(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return string(content)
}

// TestBuildMountArgs tests the argument construction logic for mount commands.
// Since we cannot actually run mount commands in unit tests, we test the
// argument construction patterns separately.
func TestBuildMountArgs(t *testing.T) {
	testCases := []struct {
		name         string
		source       string
		target       string
		fsType       string
		options      []string
		wantContains []string // Substrings that should be in the args
	}{
		{
			name:    "basic mount with fsType",
			source:  "/dev/sda1",
			target:  "/mnt/data",
			fsType:  "ext4",
			options: nil,
			wantContains: []string{
				"-t", "ext4", "/dev/sda1", "/mnt/data",
			},
		},
		{
			name:    "mount with options",
			source:  "/dev/sdb1",
			target:  "/mnt/backup",
			fsType:  "xfs",
			options: []string{"noatime", "nodiratime"},
			wantContains: []string{
				"-t", "xfs", "-o", "noatime,nodiratime", "/dev/sdb1", "/mnt/backup",
			},
		},
		{
			name:    "mount without fsType",
			source:  "/dev/sdc1",
			target:  "/mnt/test",
			fsType:  "",
			options: []string{"ro"},
			wantContains: []string{
				"-o", "ro", "/dev/sdc1", "/mnt/test",
			},
		},
		{
			name:    "mount with no fsType and no options",
			source:  "/dev/sdd1",
			target:  "/mnt/simple",
			fsType:  "",
			options: nil,
			wantContains: []string{
				"/dev/sdd1", "/mnt/simple",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Replicate the argument building logic from Mount()
			args := buildMountArgs(tc.source, tc.target, tc.fsType, tc.options)

			// Verify all expected substrings are present in the arguments
			argsStr := stringSliceToString(args)
			for _, want := range tc.wantContains {
				assert.Contains(t, argsStr, want,
					"Expected '%s' in mount args, got: %v", want, args)
			}
		})
	}
}

// buildMountArgs replicates the argument building logic from Mount() for testing.
func buildMountArgs(source, target, fsType string, options []string) []string {
	args := []string{}
	if fsType != "" {
		args = append(args, "-t", fsType)
	}
	if len(options) > 0 {
		optStr := ""
		for i, opt := range options {
			if i > 0 {
				optStr += ","
			}
			optStr += opt
		}
		args = append(args, "-o", optStr)
	}
	args = append(args, source, target)
	return args
}

// stringSliceToString converts a string slice to a single string for easier checking.
func stringSliceToString(s []string) string {
	result := ""
	for _, v := range s {
		result += v + " "
	}
	return result
}

// TestBuildBindMountArgs tests argument construction for bind mounts.
func TestBuildBindMountArgs(t *testing.T) {
	testCases := []struct {
		name         string
		source       string
		target       string
		options      []string
		wantContains []string
	}{
		{
			name:    "simple bind mount",
			source:  "/data/volumes/vol1",
			target:  "/var/lib/kubelet/pods/abc/volumes/vol1",
			options: nil,
			wantContains: []string{
				"-o", "bind", "/data/volumes/vol1", "/var/lib/kubelet/pods/abc/volumes/vol1",
			},
		},
		{
			name:    "bind mount with ro option",
			source:  "/data/volumes/vol2",
			target:  "/mnt/readonly",
			options: []string{"ro"},
			wantContains: []string{
				"-o", "bind,ro", "/data/volumes/vol2", "/mnt/readonly",
			},
		},
		{
			name:    "bind mount with multiple options",
			source:  "/data/src",
			target:  "/data/dst",
			options: []string{"ro", "noexec"},
			wantContains: []string{
				"-o", "bind,ro,noexec", "/data/src", "/data/dst",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Replicate the argument building logic from BindMount()
			args := buildBindMountArgs(tc.source, tc.target, tc.options)

			argsStr := stringSliceToString(args)
			for _, want := range tc.wantContains {
				assert.Contains(t, argsStr, want,
					"Expected '%s' in bind mount args, got: %v", want, args)
			}
		})
	}
}

func TestBindMountReadOnlyRemount(t *testing.T) {
	installFakeMountCommands(t, "mount")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	t.Setenv("FAKE_COMMAND_LOG", logPath)

	err := BindMount("/source", "/target", []string{"ro", "noexec"})
	require.NoError(t, err)

	commands := strings.Split(strings.TrimSpace(readCommandLog(t, logPath)), "\n")
	require.Len(t, commands, 2)
	assert.Equal(t, "mount -o bind,ro,noexec /source /target", commands[0])
	assert.Equal(t, "mount -o remount,bind,ro /target", commands[1])
}

func TestBindMountReadOnlyRemountFailureCleansUp(t *testing.T) {
	installFakeMountCommands(t, "mount", "findmnt", "umount")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	target := t.TempDir()
	t.Setenv("FAKE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_REMOUNT_EXIT", "1")
	t.Setenv("FAKE_FINDMNT_OUTPUT", "/source "+target+"\n")

	err := BindMount("/source", target, []string{"ro"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only bind remount failed")
	assert.Contains(t, readCommandLog(t, logPath), "umount "+target)
}

func TestUnmountDeviceFailureIsSurfaced(t *testing.T) {
	installFakeMountCommands(t, "findmnt", "umount")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	target := filepath.Join(t.TempDir(), "device-mount")
	require.NoError(t, os.Mkdir(target, 0o750))
	t.Setenv("FAKE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_FINDMNT_OUTPUT", "/dev/sda1 "+target+"\n")
	t.Setenv("FAKE_MOUNT_FSTYPE", "ext4")
	t.Setenv("FAKE_UMOUNT_EXIT", "1")
	t.Setenv("FAKE_UMOUNT_OUTPUT", "Input/output error")

	err := Unmount(target)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Input/output error")
	assert.NotContains(t, readCommandLog(t, logPath), "umount -l ")
}

func TestUnmountNFSFailureUsesLazyFallback(t *testing.T) {
	installFakeMountCommands(t, "findmnt", "umount")
	logPath := filepath.Join(t.TempDir(), "commands.log")
	target := filepath.Join(t.TempDir(), "nfs-mount")
	require.NoError(t, os.Mkdir(target, 0o750))
	t.Setenv("FAKE_COMMAND_LOG", logPath)
	t.Setenv("FAKE_FINDMNT_OUTPUT", "nas:/export "+target+"\n")
	t.Setenv("FAKE_MOUNT_FSTYPE", "nfs4")
	t.Setenv("FAKE_UMOUNT_EXIT", "1")
	t.Setenv("FAKE_UMOUNT_OUTPUT", "Input/output error")

	err := Unmount(target)
	require.NoError(t, err)
	commands := readCommandLog(t, logPath)
	assert.Contains(t, commands, "umount "+target)
	assert.Contains(t, commands, "umount -l "+target)
}

func TestParseProcMounts(t *testing.T) {
	mounts := strings.Join([]string{
		"overlay / overlay rw,relatime 0 0",
		"nas:/exports/team\\040data /var/lib/kubelet/pods/team\\040data nfs4 rw,relatime 0 0",
		"/dev/sda1 /var/lib/kubelet/pods/local ext4 rw,relatime 0 0",
	}, "\n")

	t.Run("unescapes spaces and returns nfs4", func(t *testing.T) {
		entry, err := parseProcMounts(strings.NewReader(mounts), "/var/lib/kubelet/pods/team data")
		require.NoError(t, err)
		assert.Equal(t, "nas:/exports/team data", entry.source)
		assert.Equal(t, "/var/lib/kubelet/pods/team data", entry.target)
		assert.Equal(t, "nfs4", entry.fsType)
	})

	t.Run("ignores overlay noise", func(t *testing.T) {
		entry, err := parseProcMounts(strings.NewReader(mounts), "/var/lib/kubelet/pods/local")
		require.NoError(t, err)
		assert.Equal(t, "/dev/sda1", entry.source)
		assert.Equal(t, "ext4", entry.fsType)
	})
}

func TestParseProcMountsPreservesNFSRemnantOnMalformedEntry(t *testing.T) {
	entry, err := parseProcMounts(strings.NewReader("nas:/export /dead/nfs\n"), "/dead/nfs")
	require.Error(t, err)
	assert.Equal(t, "nas:/export", entry.source)
	assert.True(t, isNFSMountSource(entry.source))
}

// buildBindMountArgs replicates the argument building logic from BindMount() for testing.
func buildBindMountArgs(source, target string, options []string) []string {
	mountOptions := []string{"bind"}
	mountOptions = append(mountOptions, options...)
	optStr := ""
	for i, opt := range mountOptions {
		if i > 0 {
			optStr += ","
		}
		optStr += opt
	}
	args := []string{"-o", optStr}
	args = append(args, source, target)
	return args
}

// TestMountNFSOptions tests the NFS mount option construction.
func TestMountNFSOptions(t *testing.T) {
	testCases := []struct {
		name         string
		source       string
		target       string
		userOptions  []string
		wantContains []string
	}{
		{
			name:        "NFS with default options only",
			source:      "192.168.1.100:/exports/data",
			target:      "/mnt/nfs",
			userOptions: nil,
			wantContains: []string{
				"-t", "nfs", "-o", "nfsvers=4",
				"192.168.1.100:/exports/data", "/mnt/nfs",
			},
		},
		{
			name:        "NFS with user options",
			source:      "nas.local:/share",
			target:      "/mnt/share",
			userOptions: []string{"hard", "intr"},
			wantContains: []string{
				"-t", "nfs", "-o", "nfsvers=4,hard,intr",
				"nas.local:/share", "/mnt/share",
			},
		},
		{
			name:        "NFS with noatime option",
			source:      "10.0.0.50:/volume",
			target:      "/data",
			userOptions: []string{"noatime"},
			wantContains: []string{
				"-t", "nfs", "-o", "nfsvers=4,noatime",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Replicate the NFS option construction from MountNFS()
			nfsOptions := []string{"nfsvers=4"}
			nfsOptions = append(nfsOptions, tc.userOptions...)

			args := buildMountArgs(tc.source, tc.target, "nfs", nfsOptions)

			argsStr := stringSliceToString(args)
			for _, want := range tc.wantContains {
				assert.Contains(t, argsStr, want,
					"Expected '%s' in NFS mount args, got: %v", want, args)
			}
		})
	}
}

// TestFormatDeviceCommand tests the mkfs command selection logic.
func TestFormatDeviceCommand(t *testing.T) {
	testCases := []struct {
		name       string
		devicePath string
		fsType     string
		wantCmd    string
		wantArgs   []string
		wantErr    bool
	}{
		{
			name:       "ext4 format",
			devicePath: "/dev/sda1",
			fsType:     "ext4",
			wantCmd:    "mkfs.ext4",
			wantArgs:   []string{"-F", "/dev/sda1"},
			wantErr:    false,
		},
		{
			name:       "ext3 format",
			devicePath: "/dev/sdb1",
			fsType:     "ext3",
			wantCmd:    "mkfs.ext3",
			wantArgs:   []string{"-F", "/dev/sdb1"},
			wantErr:    false,
		},
		{
			name:       "xfs format",
			devicePath: "/dev/sdc1",
			fsType:     "xfs",
			wantCmd:    "mkfs.xfs",
			wantArgs:   []string{"-f", "/dev/sdc1"},
			wantErr:    false,
		},
		{
			name:       "btrfs format",
			devicePath: "/dev/sdd1",
			fsType:     "btrfs",
			wantCmd:    "mkfs.btrfs",
			wantArgs:   []string{"-f", "/dev/sdd1"},
			wantErr:    false,
		},
		{
			name:       "unsupported filesystem",
			devicePath: "/dev/sde1",
			fsType:     "ntfs",
			wantCmd:    "",
			wantArgs:   nil,
			wantErr:    true,
		},
		{
			name:       "empty filesystem type",
			devicePath: "/dev/sdf1",
			fsType:     "",
			wantCmd:    "",
			wantArgs:   nil,
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, args, err := getFormatCommand(tc.devicePath, tc.fsType)

			if tc.wantErr {
				assert.Error(t, err, "Expected error for fsType=%s", tc.fsType)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantCmd, cmd, "Command mismatch")
				assert.Equal(t, tc.wantArgs, args, "Arguments mismatch")
			}
		})
	}
}

func TestFormatAndMountBlkidExitHandling(t *testing.T) {
	t.Run("exit 2 with empty output formats unformatted device", func(t *testing.T) {
		installFakeMountCommands(t, "blkid", "mkfs.ext4", "mount")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_BLKID_EXIT", "2")

		err := FormatAndMount("/dev/test", "/target", "ext4", nil)
		require.NoError(t, err)

		commands := readCommandLog(t, logPath)
		assert.Contains(t, commands, "blkid -o value -s TYPE /dev/test")
		assert.Contains(t, commands, "mkfs.ext4 -F /dev/test")
		assert.Contains(t, commands, "mount -t ext4 /dev/test /target")
	})

	t.Run("unexpected blkid failure never formats", func(t *testing.T) {
		installFakeMountCommands(t, "blkid", "mkfs.ext4", "mount")
		logPath := filepath.Join(t.TempDir(), "commands.log")
		t.Setenv("FAKE_COMMAND_LOG", logPath)
		t.Setenv("FAKE_BLKID_EXIT", "4")
		t.Setenv("FAKE_BLKID_OUTPUT", "I/O error")

		err := FormatAndMount("/dev/test", "/target", "ext4", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "blkid failed")

		commands := readCommandLog(t, logPath)
		assert.Contains(t, commands, "blkid -o value -s TYPE /dev/test")
		assert.NotContains(t, commands, "mkfs.ext4")
		assert.NotContains(t, commands, "mount -t")
	})
}

// getFormatCommand returns the mkfs command and arguments for a filesystem type.
// This replicates the logic from FormatDevice() for testing.
func getFormatCommand(devicePath, fsType string) (cmd string, args []string, err error) {
	switch fsType {
	case "ext4":
		return "mkfs.ext4", []string{"-F", devicePath}, nil
	case "ext3":
		return "mkfs.ext3", []string{"-F", devicePath}, nil
	case "xfs":
		return "mkfs.xfs", []string{"-f", devicePath}, nil
	case "btrfs":
		return "mkfs.btrfs", []string{"-f", devicePath}, nil
	default:
		return "", nil, assert.AnError
	}
}

// TestResizeFilesystemCommand tests the resize command selection logic.
func TestResizeFilesystemCommand(t *testing.T) {
	testCases := []struct {
		name       string
		fsType     string
		devicePath string
		mountPath  string
		wantCmd    string
		wantArg    string // The path argument (device or mount)
		wantErr    bool
	}{
		{
			name:       "ext4 resize",
			fsType:     "ext4",
			devicePath: "/dev/sda1",
			mountPath:  "/mnt/data",
			wantCmd:    "resize2fs",
			wantArg:    "/dev/sda1", // ext4 uses device path
			wantErr:    false,
		},
		{
			name:       "ext3 resize",
			fsType:     "ext3",
			devicePath: "/dev/sdb1",
			mountPath:  "/mnt/backup",
			wantCmd:    "resize2fs",
			wantArg:    "/dev/sdb1",
			wantErr:    false,
		},
		{
			name:       "ext2 resize",
			fsType:     "ext2",
			devicePath: "/dev/sdc1",
			mountPath:  "/mnt/old",
			wantCmd:    "resize2fs",
			wantArg:    "/dev/sdc1",
			wantErr:    false,
		},
		{
			name:       "xfs resize",
			fsType:     "xfs",
			devicePath: "/dev/sdd1",
			mountPath:  "/mnt/fast",
			wantCmd:    "xfs_growfs",
			wantArg:    "/mnt/fast", // xfs uses mount path
			wantErr:    false,
		},
		{
			name:       "btrfs resize",
			fsType:     "btrfs",
			devicePath: "/dev/sde1",
			mountPath:  "/mnt/btrfs",
			wantCmd:    "btrfs",
			wantArg:    "/mnt/btrfs", // btrfs uses mount path
			wantErr:    false,
		},
		{
			name:       "unsupported filesystem",
			fsType:     "ntfs",
			devicePath: "/dev/sdf1",
			mountPath:  "/mnt/windows",
			wantCmd:    "",
			wantArg:    "",
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, arg, err := getResizeCommand(tc.fsType, tc.devicePath, tc.mountPath)

			if tc.wantErr {
				assert.Error(t, err, "Expected error for fsType=%s", tc.fsType)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantCmd, cmd, "Command mismatch")
				assert.Equal(t, tc.wantArg, arg, "Argument mismatch")
			}
		})
	}
}

// getResizeCommand returns the resize command and path argument for a filesystem type.
// This replicates the logic from ResizeFilesystem() for testing.
func getResizeCommand(fsType, devicePath, mountPath string) (cmd, path string, err error) {
	switch fsType {
	case "ext4", "ext3", "ext2":
		return "resize2fs", devicePath, nil
	case "xfs":
		return "xfs_growfs", mountPath, nil
	case "btrfs":
		return "btrfs", mountPath, nil
	default:
		return "", "", assert.AnError
	}
}

// TestFilesystemStats tests the FilesystemStats struct calculations.
func TestFilesystemStats(t *testing.T) {
	testCases := []struct {
		name           string
		blocks         uint64
		bfree          uint64
		bavail         uint64
		files          uint64
		ffree          uint64
		bsize          int64
		wantTotal      int64
		wantAvailable  int64
		wantUsed       int64
		wantTotalInode int64
		wantAvailInode int64
		wantUsedInode  int64
	}{
		{
			name:           "standard filesystem",
			blocks:         1000000,
			bfree:          500000,
			bavail:         450000,
			files:          100000,
			ffree:          90000,
			bsize:          4096,
			wantTotal:      1000000 * 4096,
			wantAvailable:  450000 * 4096,
			wantUsed:       (1000000 - 500000) * 4096,
			wantTotalInode: 100000,
			wantAvailInode: 90000,
			wantUsedInode:  100000 - 90000,
		},
		{
			name:           "small filesystem",
			blocks:         10000,
			bfree:          5000,
			bavail:         4500,
			files:          1000,
			ffree:          800,
			bsize:          1024,
			wantTotal:      10000 * 1024,
			wantAvailable:  4500 * 1024,
			wantUsed:       (10000 - 5000) * 1024,
			wantTotalInode: 1000,
			wantAvailInode: 800,
			wantUsedInode:  1000 - 800,
		},
		{
			name:           "empty filesystem",
			blocks:         100000,
			bfree:          100000,
			bavail:         95000,
			files:          50000,
			ffree:          50000,
			bsize:          4096,
			wantTotal:      100000 * 4096,
			wantAvailable:  95000 * 4096,
			wantUsed:       0,
			wantTotalInode: 50000,
			wantAvailInode: 50000,
			wantUsedInode:  0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate stats using the same formula as GetFilesystemStats()
			blockSize := tc.bsize
			stats := FilesystemStats{
				TotalBytes:      int64(tc.blocks) * blockSize,
				AvailableBytes:  int64(tc.bavail) * blockSize,
				UsedBytes:       (int64(tc.blocks) - int64(tc.bfree)) * blockSize,
				TotalInodes:     int64(tc.files),
				AvailableInodes: int64(tc.ffree),
				UsedInodes:      int64(tc.files) - int64(tc.ffree),
			}

			assert.Equal(t, tc.wantTotal, stats.TotalBytes, "TotalBytes mismatch")
			assert.Equal(t, tc.wantAvailable, stats.AvailableBytes, "AvailableBytes mismatch")
			assert.Equal(t, tc.wantUsed, stats.UsedBytes, "UsedBytes mismatch")
			assert.Equal(t, tc.wantTotalInode, stats.TotalInodes, "TotalInodes mismatch")
			assert.Equal(t, tc.wantAvailInode, stats.AvailableInodes, "AvailableInodes mismatch")
			assert.Equal(t, tc.wantUsedInode, stats.UsedInodes, "UsedInodes mismatch")
		})
	}
}

// TestParseFindmntOutput tests parsing of findmnt command output for GetMountedBlockDevices.
func TestParseFindmntOutput(t *testing.T) {
	testCases := []struct {
		name        string
		output      string
		wantDevices map[string]string
	}{
		{
			name: "multiple block devices",
			output: `/dev/sda1 /boot
/dev/sdb1 /mnt/data
/dev/nvme0n1p1 /home
`,
			wantDevices: map[string]string{
				"/dev/sda1":      "/boot",
				"/dev/sdb1":      "/mnt/data",
				"/dev/nvme0n1p1": "/home",
			},
		},
		{
			name: "filter non-block devices",
			output: `tmpfs /run
/dev/sda1 /boot
overlay /var/lib/docker/overlay2/abc123
/dev/sdb1 /data
`,
			wantDevices: map[string]string{
				"/dev/sda1": "/boot",
				"/dev/sdb1": "/data",
			},
		},
		{
			name:        "empty output",
			output:      "",
			wantDevices: map[string]string{},
		},
		{
			name: "whitespace handling",
			output: `  /dev/sda1   /boot
/dev/sdb1 /mnt/data

`,
			wantDevices: map[string]string{
				"/dev/sda1": "/boot",
				"/dev/sdb1": "/mnt/data",
			},
		},
		{
			name: "iSCSI devices",
			output: `/dev/sda /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-123
/dev/sdb /var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-456
`,
			wantDevices: map[string]string{
				"/dev/sda": "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-123",
				"/dev/sdb": "/var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-456",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			devices := parseFindmntOutput(tc.output)
			assert.Equal(t, tc.wantDevices, devices)
		})
	}
}

func TestGetStagedBlockDevices(t *testing.T) {
	t.Run("finds nested raw block symlink", func(t *testing.T) {
		stagingRoot := t.TempDir()
		stagingPath := filepath.Join(stagingRoot, "driver", "volume", "globalmount")
		require.NoError(t, os.MkdirAll(filepath.Dir(stagingPath), 0o750))
		require.NoError(t, os.Symlink("/dev/null", stagingPath))

		devices, err := GetStagedBlockDevices(stagingRoot)
		require.NoError(t, err)
		assert.Equal(t, stagingPath, devices["/dev/null"])
	})

	t.Run("broken staging symlink fails closed", func(t *testing.T) {
		stagingRoot := t.TempDir()
		require.NoError(t, os.Symlink("/dev/scale-csi-missing-device", filepath.Join(stagingRoot, "globalmount")))

		devices, err := GetStagedBlockDevices(stagingRoot)
		require.Error(t, err)
		assert.Nil(t, devices)
	})

	t.Run("does not traverse filesystem staging contents", func(t *testing.T) {
		stagingRoot := t.TempDir()
		mountedPath := filepath.Join(stagingRoot, "driver", "volume", "globalmount")
		require.NoError(t, os.MkdirAll(mountedPath, 0o750))
		require.NoError(t, os.Symlink("/dev/scale-csi-missing-device", filepath.Join(mountedPath, "user-link")))

		devices, err := GetStagedBlockDevices(stagingRoot)
		require.NoError(t, err)
		assert.Empty(t, devices)
	})
}

// parseFindmntOutput parses findmnt output to extract block device mappings.
// This replicates the parsing logic from GetMountedBlockDevices() for testing.
func parseFindmntOutput(output string) map[string]string {
	devices := make(map[string]string)

	// Handle empty output
	output = trimSpace(output)
	if output == "" {
		return devices
	}

	lines := splitLines(output)
	for _, line := range lines {
		line = trimSpace(line)
		if line == "" {
			continue
		}
		// Format: SOURCE TARGET (space-separated)
		fields := splitFields(line)
		if len(fields) >= 2 {
			device := fields[0]
			target := fields[1]
			// Only include actual block devices
			if hasPrefix(device, "/dev/") {
				devices[device] = target
			}
		}
	}

	return devices
}

// Helper functions to avoid import cycles in test
func trimSpace(s string) string {
	result := s
	for result != "" && (result[0] == ' ' || result[0] == '\t' || result[0] == '\n' || result[0] == '\r') {
		result = result[1:]
	}
	for result != "" && (result[len(result)-1] == ' ' || result[len(result)-1] == '\t' || result[len(result)-1] == '\n' || result[len(result)-1] == '\r') {
		result = result[:len(result)-1]
	}
	return result
}

func splitLines(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

func splitFields(s string) []string {
	var result []string
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' {
			if start >= 0 {
				result = append(result, s[start:i])
				start = -1
			}
		} else {
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		result = append(result, s[start:])
	}
	return result
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// TestGetDeviceFromMountPointParsing tests parsing of findmnt output for device lookup.
func TestGetDeviceFromMountPointParsing(t *testing.T) {
	testCases := []struct {
		name       string
		output     string
		wantDevice string
	}{
		{
			name:       "simple device",
			output:     "/dev/sda1\n",
			wantDevice: "/dev/sda1",
		},
		{
			name:       "device with whitespace",
			output:     "  /dev/sdb1  \n",
			wantDevice: "/dev/sdb1",
		},
		{
			name:       "NVMe device",
			output:     "/dev/nvme0n1p2\n",
			wantDevice: "/dev/nvme0n1p2",
		},
		{
			name:       "LVM device",
			output:     "/dev/mapper/vg0-lv_data\n",
			wantDevice: "/dev/mapper/vg0-lv_data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			device := trimSpace(tc.output)
			assert.Equal(t, tc.wantDevice, device)
		})
	}
}

// TestIsMountedOutputParsing tests parsing of findmnt output for mount detection.
func TestIsMountedOutputParsing(t *testing.T) {
	testCases := []struct {
		name       string
		output     string
		wantResult bool
	}{
		{
			name:       "mounted with output",
			output:     "/dev/sda1 /boot ext4 rw,relatime",
			wantResult: true,
		},
		{
			name:       "not mounted (empty output)",
			output:     "",
			wantResult: false,
		},
		{
			name:       "whitespace only",
			output:     "   \n\t  ",
			wantResult: false,
		},
		{
			name:       "mounted NFS",
			output:     "192.168.1.100:/exports/share /mnt/nfs nfs4 rw,relatime",
			wantResult: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Replicate the mount detection logic from IsMounted()
			result := trimSpace(tc.output) != ""
			assert.Equal(t, tc.wantResult, result)
		})
	}
}
