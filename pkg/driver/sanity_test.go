package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

var (
	nfsSanityConfig    = sanity.NewTestConfig()
	iscsiSanityConfig  = sanity.NewTestConfig()
	nfsSanityContext   *sanity.TestContext
	iscsiSanityContext *sanity.TestContext
)

var _ = Describe("scale-csi conformance", func() {
	Context("NFS full surface", func() {
		Describe("official csi-sanity suite", func() {
			nfsSanityContext = sanity.GinkgoTest(&nfsSanityConfig)
		})
	})

	Context("iSCSI controller only", func() {
		BeforeEach(func() {
			// csi-sanity's iSCSI Node Service cases require a real block device and
			// root-level session/mount operations, which the PATH fakes cannot model
			// safely. The controller suite still exercises the complete zvol/iSCSI
			// provisioning, snapshot, clone, pagination, and expansion surface.
			if strings.Contains(CurrentSpecReport().FullText(), "Node Service") {
				Skip("iSCSI node tests require a real block device and root privileges")
			}
		})

		Describe("official csi-sanity suite", func() {
			iscsiSanityContext = sanity.GinkgoTest(&iscsiSanityConfig)
		})
	})
})

func TestCSISanity(t *testing.T) {
	if testing.Short() {
		// The conformance suite is intentionally excluded from short unit-test runs.
		t.Skip("skipping the full CSI conformance suite in short mode")
	}

	installSanityNodeCommands(t)
	testRoot := t.TempDir()

	nfsEndpoint := startSanityDriver(t, filepath.Join(testRoot, "nfs.sock"), "nfs", true)
	// The Node service is registered only so controller publish checks can obtain
	// and validate a node ID. The iSCSI Node Service specs are skipped below and
	// no block-device operation is executed.
	iscsiEndpoint := startSanityDriver(t, filepath.Join(testRoot, "iscsi.sock"), "iscsi", true)

	configureSanityTest(&nfsSanityConfig, filepath.Join(testRoot, "nfs"), nfsEndpoint, "nfs")
	configureSanityTest(&iscsiSanityConfig, filepath.Join(testRoot, "iscsi"), iscsiEndpoint, "iscsi")

	RegisterFailHandler(Fail)
	defer func() {
		if nfsSanityContext != nil {
			nfsSanityContext.Finalize()
		}
		if iscsiSanityContext != nil {
			iscsiSanityContext.Finalize()
		}
	}()
	RunSpecs(t, "scale-csi csi-sanity suite")
}

func configureSanityTest(config *sanity.TestConfig, root, endpoint, protocol string) {
	*config = sanity.NewTestConfig()
	config.Address = endpoint
	config.TargetPath = filepath.Join(root, "target")
	config.StagingPath = filepath.Join(root, "staging")
	config.TestVolumeParameters = map[string]string{"protocol": protocol}
	config.CreateTargetDir = createSanityDirectory
	config.CreateStagingDir = createSanityDirectory
	config.RemoveTargetPath = os.RemoveAll
	config.RemoveStagingPath = os.RemoveAll
	config.CheckPath = checkSanityPath
}

func createSanityDirectory(path string) (string, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return "", err
	}
	return path, nil
}

func checkSanityPath(path string) (sanity.PathKind, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return sanity.PathIsNotFound, nil
		}
		return "", err
	}
	if info.Mode().IsRegular() {
		return sanity.PathIsFile, nil
	}
	if info.IsDir() {
		return sanity.PathIsDir, nil
	}
	return sanity.PathIsOther, nil
}

func startSanityDriver(t *testing.T, socketPath, protocol string, runNode bool) string {
	t.Helper()

	endpoint := "unix://" + socketPath
	config := sanityDriverConfig(protocol)

	// NewDriver constructs all production wiring. Replace only its external
	// TrueNAS boundary with the repository's stateful mock via the client
	// constructor seam, so no real connection is ever attempted.
	mockClient := truenas.NewMockClient()
	// The parent dataset always exists on a real backend; it now also carries the
	// driver's durable bookkeeping (in-flight markers, tombstone ledger).
	if _, parentErr := mockClient.DatasetCreate(context.Background(), &truenas.DatasetCreateParams{
		Name: config.ZFS.DatasetParentName, Type: "FILESYSTEM",
	}); parentErr != nil {
		t.Fatalf("create %s sanity parent dataset: %v", protocol, parentErr)
	}
	originalNewClient := newTrueNASClient
	newTrueNASClient = func(*truenas.ClientConfig) (truenas.ClientInterface, error) {
		return mockClient, nil
	}
	driver, err := NewDriver(&DriverConfig{
		Name:          config.DriverName,
		Version:       "csi-sanity",
		NodeID:        "csi-sanity-node",
		Endpoint:      endpoint,
		RunController: true,
		RunNode:       runNode,
		Config:        config,
	})
	newTrueNASClient = originalNewClient
	if err != nil {
		t.Fatalf("create %s sanity driver: %v", protocol, err)
	}

	if driver.serviceReloadDebouncer != nil {
		driver.serviceReloadDebouncer.Stop()
	}
	driver.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return mockClient.ServiceReload(ctx, service)
	})

	runErr := make(chan error, 1)
	go func() {
		runErr <- driver.Run()
	}()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ready := time.NewTicker(5 * time.Millisecond)
	defer ready.Stop()
	for !driver.ready.Load() {
		select {
		case err := <-runErr:
			t.Fatalf("start %s sanity driver: %v", protocol, err)
		case <-deadline.C:
			t.Fatalf("start %s sanity driver: timed out waiting for readiness", protocol)
		case <-ready.C:
		}
	}

	t.Cleanup(func() {
		driver.Stop()
		select {
		case err := <-runErr:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("stop %s sanity driver: %v", protocol, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("stop %s sanity driver: timed out", protocol)
		}
	})

	return endpoint
}

func sanityDriverConfig(protocol string) *Config {
	driverName := "org.scale.csi." + protocol
	return &Config{
		DriverName: driverName,
		TrueNAS: TrueNASConfig{
			Host:                  "127.0.0.1",
			Port:                  443,
			Protocol:              "https",
			APIKey:                "csi-sanity-mock",
			RequestTimeout:        5,
			ConnectTimeout:        5,
			WriteTimeout:          5,
			MaxConcurrentRequests: 10,
		},
		ZFS: ZFSConfig{
			DatasetParentName:   "tank/csi-sanity/" + protocol,
			DatasetEnableQuotas: true,
			ZvolBlocksize:       "16K",
			ZvolReadyTimeout:    5,
		},
		NFS: NFSConfig{
			ShareHost: "192.0.2.10",
		},
		ISCSI: ISCSIConfig{
			TargetPortal:          "192.0.2.10:3260",
			Interface:             "default",
			ExtentBlocksize:       512,
			ExtentRpm:             "SSD",
			DeviceWaitTimeout:     5,
			ServiceReloadDebounce: 1,
		},
		SessionGC: SessionGCConfig{
			Interval: 0,
		},
		// Reconcile requires live Kubernetes objects and is intentionally off in
		// the isolated csi-sanity harness.
		Reconcile: ReconcileConfig{Enabled: false},
		Node: NodeConfig{
			SessionCleanupDelay: 1,
		},
		Resilience: ResilienceConfig{
			Retry: RetryConfig{
				MaxAttempts:       1,
				InitialDelay:      1,
				MaxDelay:          1,
				BackoffMultiplier: 1,
			},
			RateLimiting: RateLimitConfig{
				MaxConcurrentRequests: 10,
				MaxConcurrentLogins:   2,
			},
		},
		CommandTimeouts: CommandTimeoutConfig{
			Mount:  5,
			Format: 5,
			ISCSI:  5,
			NVMe:   5,
		},
	}
}

const sanityNodeCommandScript = `#!/bin/sh
name="$(basename "$0")"
last=""
for arg in "$@"; do
	last="$arg"
done

case "$name" in
	findmnt)
		target="$last"
		next_mountpoint=false
		for arg in "$@"; do
			if [ "$next_mountpoint" = true ]; then target="$arg"; next_mountpoint=false; continue; fi
			if [ "$arg" = "--mountpoint" ]; then next_mountpoint=true; fi
		done
		line=""
		if [ -n "$FAKE_CSI_MOUNT_TABLE" ] && [ -f "$FAKE_CSI_MOUNT_TABLE" ]; then
			line="$(awk -F '\t' -v target="$target" '$1 == target { print; exit }' "$FAKE_CSI_MOUNT_TABLE")"
		fi
		if [ -n "$line" ]; then
			source="$(printf '%s\n' "$line" | cut -f2)"
			fstype="$(printf '%s\n' "$line" | cut -f3)"
			options="$(printf '%s\n' "$line" | cut -f4)"
			case " $* " in
				*" SOURCE,FSTYPE,OPTIONS "*) printf '%s %s %s\n' "$source" "$fstype" "$options" ;;
				*" FSTYPE "*) printf '%s\n' "$fstype" ;;
				*" SOURCE "*) printf '%s\n' "$source" ;;
				*" OPTIONS "*) printf '%s\n' "$options" ;;
				*) printf '%s\n' 'mounted' ;;
			esac
			exit 0
		fi
		exit 1
		;;
	mount)
		if [ -n "$FAKE_CSI_MOUNT_TABLE" ]; then
			touch "$FAKE_CSI_MOUNT_TABLE"
			previous=""
			source=""
			fstype="none"
			options="rw"
			next_fstype=false
			next_options=false
			mount_options=""
			for arg in "$@"; do
				if [ "$next_fstype" = true ]; then fstype="$arg"; next_fstype=false; continue; fi
				if [ "$next_options" = true ]; then mount_options="$arg"; next_options=false; continue; fi
				if [ "$arg" = "-t" ]; then next_fstype=true; continue; fi
				if [ "$arg" = "-o" ]; then next_options=true; continue; fi
				previous="$source"
				source="$arg"
			done
			case ",$mount_options," in *,ro,*|*,remount,bind,ro,*) options="ro" ;; esac
			if [ "$source" = "$last" ]; then source="$previous"; fi
			if printf '%s' "$mount_options" | grep -q 'remount'; then
				existing="$(awk -F '\t' -v target="$last" '$1 == target { print; exit }' "$FAKE_CSI_MOUNT_TABLE")"
				if [ -n "$existing" ]; then
					source="$(printf '%s\n' "$existing" | cut -f2)"
					fstype="$(printf '%s\n' "$existing" | cut -f3)"
				fi
			fi
			awk -F '\t' -v target="$last" '$1 != target' "$FAKE_CSI_MOUNT_TABLE" > "$FAKE_CSI_MOUNT_TABLE.tmp" || true
			printf '%s\t%s\t%s\t%s\n' "$last" "$source" "$fstype" "$options" >> "$FAKE_CSI_MOUNT_TABLE.tmp"
			mv "$FAKE_CSI_MOUNT_TABLE.tmp" "$FAKE_CSI_MOUNT_TABLE"
		fi
		exit 0
		;;
	umount)
		if [ -n "$FAKE_CSI_MOUNT_TABLE" ] && [ -f "$FAKE_CSI_MOUNT_TABLE" ]; then
			awk -F '\t' -v target="$last" '$1 != target' "$FAKE_CSI_MOUNT_TABLE" > "$FAKE_CSI_MOUNT_TABLE.tmp" || true
			mv "$FAKE_CSI_MOUNT_TABLE.tmp" "$FAKE_CSI_MOUNT_TABLE"
		fi
		exit 0
		;;
	blkid)
		exit 2
		;;
	iscsiadm)
		exit 97
		;;
	*)
		exit 0
		;;
esac
`

func installSanityNodeCommands(t *testing.T) {
	t.Helper()

	binDir := t.TempDir()
	for _, command := range []string{
		"blkid", "findmnt", "iscsiadm", "mount", "nvme", "resize2fs", "umount", "xfs_growfs",
	} {
		commandPath := filepath.Join(binDir, command)
		if err := os.WriteFile(commandPath, []byte(sanityNodeCommandScript), 0o750); err != nil {
			t.Fatalf("install fake node command %s: %v", command, err)
		}
	}

	mountTable := filepath.Join(t.TempDir(), "mounts")
	if err := os.WriteFile(mountTable, nil, 0o600); err != nil {
		t.Fatalf("create fake mount table: %v", err)
	}
	t.Setenv("FAKE_CSI_MOUNT_TABLE", mountTable)
	t.Setenv("PATH", fmt.Sprintf("%s%c%s", binDir, os.PathListSeparator, os.Getenv("PATH")))
}
