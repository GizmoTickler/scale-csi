package driver

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubISCSIRescanSeams restores the rescan seams after the test.
func stubISCSIRescanSeams(t *testing.T) {
	t.Helper()
	info, rescan, paths, resize := nodeGetISCSIInfo, nodeISCSIRescan, nodeMultipathPaths, nodeMultipathResize
	t.Cleanup(func() {
		nodeGetISCSIInfo, nodeISCSIRescan, nodeMultipathPaths, nodeMultipathResize = info, rescan, paths, resize
	})
}

func requireInternal(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), contains)
}

func TestRescanISCSIDevice(t *testing.T) {
	const iqn = "iqn.2005-10.org.freenas.ctl:block-vol"
	ctx := context.Background()

	t.Run("inspect error", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) {
			return "", nil, false, errors.New("sysfs unreadable")
		}
		requireInternal(t, rescanISCSIDevice(ctx, "/dev/dm-3"), "failed to inspect /dev/dm-3: sysfs unreadable")
	})

	t.Run("single device", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) { return "", nil, false, nil }
		nodeGetISCSIInfo = func(device string) (string, string, error) {
			assert.Equal(t, "/dev/sdb", device)
			return "192.0.2.30:3260", iqn, nil
		}
		var rescanned []string
		nodeISCSIRescan = func(_ context.Context, portal, gotIQN string) error {
			rescanned = append(rescanned, portal+" "+gotIQN)
			return nil
		}
		nodeMultipathResize = func(context.Context, string) error {
			t.Fatal("a single device is never resized through multipathd")
			return nil
		}
		require.NoError(t, rescanISCSIDevice(ctx, "/dev/sdb"))
		assert.Equal(t, []string{"192.0.2.30:3260 " + iqn}, rescanned)
	})

	t.Run("single device without a session", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) { return "", nil, false, nil }
		nodeGetISCSIInfo = func(string) (string, string, error) { return "", "", errors.New("no session") }
		requireInternal(t, rescanISCSIDevice(ctx, "/dev/sdb"), "failed to identify iSCSI session for /dev/sdb: no session")
	})

	t.Run("single device rescan error", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) { return "", nil, false, nil }
		nodeGetISCSIInfo = func(string) (string, string, error) { return "192.0.2.30:3260", iqn, nil }
		nodeISCSIRescan = func(context.Context, string, string) error { return errors.New("iscsiadm exit 21") }
		requireInternal(t, rescanISCSIDevice(ctx, "/dev/sdb"), "failed to rescan iSCSI device /dev/sdb: iscsiadm exit 21")
	})

	t.Run("map path without a session is skipped", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) {
			return "mpatha", []string{"/dev/sdb", "/dev/sdc", "/dev/sdd"}, true, nil
		}
		nodeGetISCSIInfo = func(device string) (string, string, error) {
			switch device {
			case "/dev/sdb", "/dev/sdd": // two paths through the same portal: one rescan
				return "192.0.2.30:3260", iqn, nil
			default:
				return "", "", errors.New("no session")
			}
		}
		var rescanned []string
		nodeISCSIRescan = func(_ context.Context, portal, _ string) error {
			rescanned = append(rescanned, portal)
			return nil
		}
		var resized []string
		nodeMultipathResize = func(_ context.Context, name string) error {
			resized = append(resized, name)
			return nil
		}
		require.NoError(t, rescanISCSIDevice(ctx, "/dev/dm-3"))
		assert.Equal(t, []string{"192.0.2.30:3260"}, rescanned)
		assert.Equal(t, []string{"mpatha"}, resized)
	})

	t.Run("map with no targets at all", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) {
			return "mpatha", []string{"/dev/sdb", "/dev/sdc"}, true, nil
		}
		nodeGetISCSIInfo = func(string) (string, string, error) { return "", "", errors.New("no session") }
		nodeISCSIRescan = func(context.Context, string, string) error {
			t.Fatal("nothing to rescan")
			return nil
		}
		requireInternal(t, rescanISCSIDevice(ctx, "/dev/dm-3"),
			"failed to identify an iSCSI session for multipath map mpatha (/dev/dm-3)")
	})

	t.Run("map path rescan error", func(t *testing.T) {
		stubISCSIRescanSeams(t)
		nodeMultipathPaths = func(string) (string, []string, bool, error) {
			return "mpatha", []string{"/dev/sdb"}, true, nil
		}
		nodeGetISCSIInfo = func(string) (string, string, error) { return "192.0.2.30:3260", iqn, nil }
		nodeISCSIRescan = func(context.Context, string, string) error { return errors.New("timed out") }
		nodeMultipathResize = func(context.Context, string) error {
			t.Fatal("a map is not resized after a failed path rescan")
			return nil
		}
		requireInternal(t, rescanISCSIDevice(ctx, "/dev/dm-3"),
			"failed to rescan iSCSI path 192.0.2.30:3260 of /dev/dm-3: timed out")
	})
}

// installScriptedMountCommands puts a findmnt and an umount on PATH whose
// behavior is driven by a marker the umount leaves beside path:
//
//	findmntBefore / findmntAfter: "mounted", "unmounted" (exit 1) or "error"
//	(exit 2), before and after the umount ran;
//	umountExit: the umount's exit status.
func installScriptedMountCommands(t *testing.T, path, findmntBefore, findmntAfter string, umountExit int) {
	t.Helper()
	marker := path + ".umount-ran"
	findmnt := `#!/bin/sh
case " $* " in *" FSTYPE "*) echo ext4; exit 0 ;; esac
state="` + findmntBefore + `"
if [ -e '` + marker + `' ]; then state="` + findmntAfter + `"; fi
case "$state" in
	mounted) echo "$3 /dev/sdb ext4 rw"; exit 0 ;;
	unmounted) exit 1 ;;
	*) echo "findmnt: cannot read mount table" >&2; exit 2 ;;
esac
`
	umount := "#!/bin/sh\n: > '" + marker + "'\necho 'umount: permission denied' >&2\nexit " + strconv.Itoa(umountExit) + "\n"
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "findmnt"), []byte(findmnt), 0o750)) //nolint:gosec // must be executable to stand in as findmnt
	require.NoError(t, os.WriteFile(filepath.Join(bin, "umount"), []byte(umount), 0o750))   //nolint:gosec // must be executable to stand in as umount
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestUnmountFully(t *testing.T) {
	ctx := context.Background()

	t.Run("check error with unmount error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mnt")
		installScriptedMountCommands(t, path, "error", "error", 0)
		requireInternal(t, unmountFully(ctx, path, "staging path"),
			"failed to unmount staging path and cannot verify mount status")
	})

	t.Run("check error alone", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mnt")
		installScriptedMountCommands(t, path, "mounted", "error", 0)
		requireInternal(t, unmountFully(ctx, path, "target path"),
			"cannot verify that target path "+path+" is unmounted")
	})

	t.Run("still mounted after a failed unmount", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mnt")
		installScriptedMountCommands(t, path, "mounted", "mounted", 1)
		requireInternal(t, unmountFully(ctx, path, "staging path"),
			"failed to unmount staging path (still mounted)")
	})

	t.Run("failed unmount but nothing mounted", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "mnt")
		installScriptedMountCommands(t, path, "mounted", "unmounted", 1)
		require.NoError(t, unmountFully(ctx, path, "staging path"))
	})
}

// Any removal failure other than a non-empty directory is reported by
// removeMountPoint and only logged by cleanupMountPoint.
func TestCleanupMountPointOtherRemovalErrorIsLogged(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	path := filepath.Join(file, "mnt") // ENOTDIR

	err := removeMountPoint(path)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errMountPointNotEmpty)
	assert.False(t, os.IsNotExist(err))

	require.NoError(t, cleanupMountPoint(path, "target path"))
}

// fakeHostPortAddr is a net.Addr whose String is host:port, as a listener's is.
type fakeHostPortAddr string

func (fakeHostPortAddr) Network() string  { return "tcp" }
func (a fakeHostPortAddr) String() string { return string(a) }

func TestNodeInterfaceIPs(t *testing.T) {
	orig := nodeInterfaceAddrs
	t.Cleanup(func() { nodeInterfaceAddrs = orig })

	nodeInterfaceAddrs = func() ([]net.Addr, error) { return nil, errors.New("netlink refused") }
	assert.Nil(t, nodeInterfaceIPs(), "a listing failure is no addresses")

	nodeInterfaceAddrs = func() ([]net.Addr, error) {
		return []net.Addr{
			fakeHostPortAddr("192.0.2.7:3260"),
			fakeHostPortAddr("[2001:db8::7]:3260"),
			&net.IPNet{IP: net.ParseIP("198.51.100.11"), Mask: net.CIDRMask(24, 32)},
			fakeHostPortAddr("not-an-ip"),
		}, nil
	}
	got := make([]string, 0, 3)
	for _, ip := range nodeInterfaceIPs() {
		got = append(got, ip.String())
	}
	assert.ElementsMatch(t, []string{"192.0.2.7", "2001:db8::7", "198.51.100.11"}, got)
}

func TestNodeIdentityDroppedIPsUnparsableNodeID(t *testing.T) {
	networks, err := parseNodeIdentityNetworks([]string{"192.168.201.0/24"})
	require.NoError(t, err)
	identity := NodeIdentity{Name: "k8s-1", IPs: []net.IP{net.ParseIP("192.168.201.1")}}
	assert.Nil(t, nodeIdentityDroppedIPs(identity, networks, nodeIdentityPrefix+"!!not base64!!"))
}

func TestValidateNodeIdentityNetworksAgainstShareAllowedNetworks(t *testing.T) {
	// Inside, outside, and a single address outside: all valid (outside only warns).
	require.NoError(t, validateNodeIdentityNetworks(&NFSConfig{
		NodeIdentityNetworks: []string{"192.168.201.0/24", "10.20.0.0/16", "172.16.5.9"},
		ShareAllowedNetworks: []string{"192.168.0.0/16"},
	}))
	// An invalid shareAllowedNetworks entry is not this check's to report.
	require.NoError(t, validateNodeIdentityNetworks(&NFSConfig{
		NodeIdentityNetworks: []string{"192.168.201.0/24"},
		ShareAllowedNetworks: []string{"not-a-network"},
	}))
	require.Error(t, validateNodeIdentityNetworks(&NFSConfig{
		NodeIdentityNetworks: []string{"not-a-network"},
		ShareAllowedNetworks: []string{"192.168.0.0/16"},
	}))
}

func TestNewDriverRejectsInvalidNodeIdentityNetworks(t *testing.T) {
	_, err := NewDriver(&DriverConfig{
		Name: "csi.scale.io", Version: "test", NodeID: "worker-a",
		Endpoint: "tcp://127.0.0.1:10000", RunNode: true,
		Config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/csi"},
			NFS: NFSConfig{Enabled: true, ShareHost: "192.0.2.10", NodeIdentityNetworks: []string{"not-a-network"}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nfs.nodeIdentityNetworks[0]")
}

// More fabric addresses than the 256-byte node_id holds: the node plugin
// still starts, leaving the tail out (and warning about it).
func TestNewDriverStartsWhenIdentityNetworkAddressesOverflowTheNodeID(t *testing.T) {
	interfaces := []string{identityTestMgmtIP + "/24"}
	for i := 1; i <= 24; i++ {
		interfaces = append(interfaces, "fd00:201::"+strconv.Itoa(i)+"/64")
	}
	fakeNodeIdentityHost(t, identityTestMgmtIP, interfaces...)

	drv, err := NewDriver(&DriverConfig{
		Name: "csi.scale.io", Version: "test", NodeID: "worker-a",
		Endpoint: "tcp://127.0.0.1:10000", RunNode: true,
		Config: &Config{
			ZFS: ZFSConfig{DatasetParentName: "tank/csi"},
			NFS: NFSConfig{Enabled: true, ShareHost: "192.0.2.10", NodeIdentityNetworks: []string{"fd00:201::/64"}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(drv.Stop)
	identity, err := parseNodeIdentity(drv.encodedNodeID)
	require.NoError(t, err)
	assert.Less(t, len(identity.IPs), 25, "the fixture must overflow the node_id")
	assert.NotEmpty(t, identity.IPs)
}
