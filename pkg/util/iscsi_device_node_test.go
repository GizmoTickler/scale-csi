package util

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

var (
	// sdb as the kernel numbers the disk of the NEW session...
	freshSDB = unix.Mkdev(8, 16)
	// ...and the node the previous session's sdb left behind.
	staleSDB = unix.Mkdev(65, 0)
)

// staleThenFresh reports the stale number for the first `stale` stats of sdb,
// then the current one, counting every stat.
func staleThenFresh(stale int32, stats *atomic.Int32) func(string) (uint64, bool) {
	return func(name string) (uint64, bool) {
		if name != "sdb" {
			return 0, false
		}
		if stats.Add(1) <= stale {
			return staleSDB, true
		}
		return freshSDB, true
	}
}

// iscsiSessionFixture lays out session 12 on host 4 with LUN 0 as sdb, as
// sysfs shows it right after login: the dev file carries the new number.
func iscsiSessionFixture(t *testing.T, iqn string) (sysClassRoot, devRoot string) {
	t.Helper()
	root := t.TempDir()
	sysClassRoot = filepath.Join(root, "sys", "class")
	devRoot = filepath.Join(root, "dev")
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "iscsi_host", "host4", "device", "session12"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "scsi_device", "4:0:0:0", "device", "block", "sdb"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(sysClassRoot, "iscsi_session", "session12"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(sysClassRoot, "iscsi_session", "session12", "targetname"), []byte(iqn+"\n"), 0o600))
	writeSysfsDev(t, filepath.Join(sysClassRoot, "block", "sdb"), freshSDB)
	require.NoError(t, os.MkdirAll(devRoot, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "sdb"), nil, 0o600))
	return sysClassRoot, devRoot
}

func TestFindDeviceForSessionRejectsStaleDevNode(t *testing.T) {
	sysClassRoot, devRoot := iscsiSessionFixture(t, "iqn.test:stale")

	fixedBlockDeviceNumbers(t, map[string]uint64{"sdb": staleSDB})
	devicePath, err := findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.Error(t, err, "a /dev node left by the previous session's disk must not be used")
	assert.Empty(t, devicePath)

	fixedBlockDeviceNumbers(t, map[string]uint64{})
	_, err = findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.Error(t, err, "a node that is not a block device must not be used")

	fixedBlockDeviceNumbers(t, map[string]uint64{"sdb": freshSDB})
	require.NoError(t, os.WriteFile(filepath.Join(sysClassRoot, "block", "sdb", "dev"), []byte("garbage\n"), 0o600))
	_, err = findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.Error(t, err, "an unreadable sysfs device number must not match")

	writeSysfsDev(t, filepath.Join(sysClassRoot, "block", "sdb"), freshSDB)
	devicePath, err = findDeviceForSessionInPaths("session12", 0, sysClassRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(devRoot, "sdb"), devicePath)
}

// stubDeviceWaitPaths points the device wait's lookups at a fake sysfs and /dev.
func stubDeviceWaitPaths(t *testing.T, sysClassRoot, devRoot string, sessions []ISCSISessionInfo, listErr error) {
	t.Helper()
	originalList := listISCSISessionsForDevice
	originalPortalFind := findISCSIDeviceForPortal
	originalFallback := findISCSIDeviceFallback
	t.Cleanup(func() {
		listISCSISessionsForDevice = originalList
		findISCSIDeviceForPortal = originalPortalFind
		findISCSIDeviceFallback = originalFallback
	})
	listISCSISessionsForDevice = func() ([]ISCSISessionInfo, error) { return sessions, listErr }
	findISCSIDeviceForPortal = func(portal, iqn string, lun int, sessions []ISCSISessionInfo) (string, error) {
		return findISCSIDeviceForPortalFromSessionsInPaths(portal, iqn, lun, sessions, sysClassRoot, devRoot)
	}
	findISCSIDeviceFallback = func(iqn string, lun int) (string, error) {
		return findISCSIDeviceInPaths(iqn, lun, sysClassRoot, devRoot)
	}
}

// The handover race: the other plugin logged out, this one logged in, and
// /dev/sdb is still the previous disk's node for a few polls. The wait must
// not hand that node to blkid (ENXIO), but wait for the current one.
func TestWaitForISCSIDeviceWaitsForCurrentDevNode(t *testing.T) {
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-handover"
	const portal = "192.0.2.40:3260"
	for _, test := range []struct {
		name     string
		sessions []ISCSISessionInfo
		listErr  error
		wait     func(string, string, int, time.Duration) (string, error)
	}{
		{
			name:     "portal scoped",
			sessions: []ISCSISessionInfo{{Portal: portal, IQN: iqn, SessionID: "12"}},
			wait:     waitForISCSIPortalDevice,
		},
		{
			name:     "single portal",
			sessions: []ISCSISessionInfo{{Portal: portal, IQN: iqn, SessionID: "12"}},
			wait:     waitForISCSIDevice,
		},
		{
			name:    "IQN fallback",
			listErr: errors.New("iscsiadm unavailable"),
			wait:    waitForISCSIDevice,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sysClassRoot, devRoot := iscsiSessionFixture(t, iqn)
			stubDeviceWaitPaths(t, sysClassRoot, devRoot, test.sessions, test.listErr)
			var stats atomic.Int32
			stubBlockDeviceNumber(t, staleThenFresh(3, &stats))

			devicePath, err := test.wait(portal, iqn, 0, 5*time.Second)
			require.NoError(t, err)
			assert.Equal(t, filepath.Join(devRoot, "sdb"), devicePath)
			assert.Equal(t, int32(4), stats.Load(), "the wait must poll past the stale node to the current one")
		})
	}
}

func TestWaitForISCSIDeviceTimesOutOnAStaleDevNode(t *testing.T) {
	const iqn = "iqn.2005-10.org.freenas.ctl:pvc-stale-forever"
	const portal = "192.0.2.41:3260"
	sysClassRoot, devRoot := iscsiSessionFixture(t, iqn)
	stubDeviceWaitPaths(t, sysClassRoot, devRoot, []ISCSISessionInfo{{Portal: portal, IQN: iqn, SessionID: "12"}}, nil)
	var stats atomic.Int32
	stubBlockDeviceNumber(t, staleThenFresh(1<<30, &stats))

	devicePath, err := waitForISCSIPortalDevice(portal, iqn, 0, 150*time.Millisecond)
	require.Error(t, err)
	assert.Empty(t, devicePath)
	assert.Contains(t, err.Error(), "timeout waiting for device (iqn="+iqn+", lun=0)")
	assert.Greater(t, stats.Load(), int32(1), "the stale node must be re-checked until the timeout")
}

// A map rebuilt right after the previous one was flushed: sysfs names the new
// dm-3 while /dev/mapper/<name> still links to the old dm-2 node.
func TestFindISCSIMultipathDeviceRejectsStaleMapNode(t *testing.T) {
	root := t.TempDir()
	sysBlockRoot := filepath.Join(root, "sys", "block")
	devRoot := filepath.Join(root, "dev")
	const wwid = "36001405a123456789abcdef000000007"
	dmRoot := filepath.Join(sysBlockRoot, "dm-3")
	require.NoError(t, os.MkdirAll(filepath.Join(dmRoot, "dm"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dmRoot, "dm", "uuid"), []byte("mpath-"+wwid+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dmRoot, "dm", "name"), []byte(wwid+"\n"), 0o600))
	writeSysfsDev(t, dmRoot, unix.Mkdev(253, 3))
	require.NoError(t, os.MkdirAll(filepath.Join(devRoot, "mapper"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "dm-2"), nil, 0o600))
	mapperPath := filepath.Join(devRoot, "mapper", wwid)
	require.NoError(t, os.Symlink("../dm-2", mapperPath))
	fixedBlockDeviceNumbers(t, map[string]uint64{"dm-2": unix.Mkdev(253, 2), "dm-3": unix.Mkdev(253, 3)})

	devicePath, err := findISCSIMultipathDeviceInPaths(wwid, sysBlockRoot, devRoot)
	require.Error(t, err, "neither the stale mapper link nor a missing dm-3 node is the map")
	assert.Empty(t, devicePath)

	// The dm node appears before udev rewrites the link: the node is the map.
	require.NoError(t, os.WriteFile(filepath.Join(devRoot, "dm-3"), nil, 0o600))
	devicePath, err = findISCSIMultipathDeviceInPaths(wwid, sysBlockRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(devRoot, "dm-3"), devicePath)

	// Once udev points the link at the map, the friendly path wins again.
	require.NoError(t, os.Remove(mapperPath))
	require.NoError(t, os.Symlink("../dm-3", mapperPath))
	devicePath, err = findISCSIMultipathDeviceInPaths(wwid, sysBlockRoot, devRoot)
	require.NoError(t, err)
	assert.Equal(t, mapperPath, devicePath)
}
