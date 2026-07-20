// Package main implements the TrueNAS Scale CSI driver entry point.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"go.uber.org/automaxprocs/maxprocs"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
)

const (
	cgroupV2MemoryLimitPath = "/sys/fs/cgroup/memory.max"
	cgroupV1MemoryLimitPath = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

var (
	// Version is set at build time
	Version = "dev"
	// GitCommit is set at build time
	GitCommit = "unknown"
)

func main() {
	// Define flags
	var (
		configFile  string
		endpoint    string
		nodeID      string
		driverName  string
		mode        string
		healthPort  int
		showVersion bool
	)

	flag.StringVar(&configFile, "config", "", "Path to driver configuration file (required)")
	flag.StringVar(&endpoint, "endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	flag.StringVar(&nodeID, "node-id", "", "Node ID (required for node mode)")
	flag.StringVar(&driverName, "driver-name", "csi.scale.io", "CSI driver name")
	flag.StringVar(&mode, "mode", "all", "Driver mode: controller, node, all, or reconcile")
	flag.IntVar(&healthPort, "health-port", 9809, "Port for health/metrics HTTP server (0 to disable)")
	flag.BoolVar(&showVersion, "version", false, "Show version and exit")

	klog.InitFlags(nil)
	flag.Parse()

	undoMaxProcs, err := maxprocs.Set(maxprocs.Logger(klog.Infof))
	if err != nil {
		klog.Warningf("Unable to configure GOMAXPROCS from cgroups: %v", err)
	} else {
		defer undoMaxProcs()
	}

	if memoryLimit, configured, memoryErr := setMemoryLimitFromCgroup(
		cgroupV2MemoryLimitPath,
		cgroupV1MemoryLimitPath,
		os.LookupEnv,
		debug.SetMemoryLimit,
	); memoryErr != nil {
		klog.Warningf("Unable to configure GOMEMLIMIT from cgroups: %v", memoryErr)
	} else if configured {
		klog.Infof("Set GOMEMLIMIT=%d from cgroup memory limit", memoryLimit)
	}

	if showVersion {
		fmt.Printf("Scale CSI Driver (for TrueNAS SCALE)\nVersion: %s\nCommit: %s\n", Version, GitCommit)
		return
	}

	if configFile == "" {
		klog.Fatal("--config is required")
	}

	// Load configuration
	cfg, err := driver.LoadConfig(configFile)
	if err != nil {
		klog.Fatalf("Failed to load config: %v", err)
	}

	// Override config with CLI flags if provided
	if driverName != "csi.scale.io" {
		cfg.DriverName = driverName
	}
	if cfg.DriverName == "" {
		cfg.DriverName = driverName
	}

	// Validate mode.
	switch mode {
	case "controller", "node", "all", "reconcile":
	default:
		klog.Fatalf("Invalid --mode %q; expected controller, node, all, or reconcile", mode)
	}
	runController := mode == "controller" || mode == "all"
	runNode := mode == "node" || mode == "all"
	reconcileOnce := mode == "reconcile"

	if runNode && nodeID == "" {
		// Try to get node ID from environment or hostname
		nodeID = os.Getenv("NODE_ID")
		if nodeID == "" {
			nodeID, _ = os.Hostname()
		}
		if nodeID == "" {
			klog.Fatal("--node-id is required for node mode")
		}
	}

	klog.Infof("Starting Scale CSI Driver version %s", Version)
	klog.Infof("Driver name: %s", cfg.DriverName)
	klog.Infof("Mode: %s (controller=%v, node=%v, reconcileOnce=%v)", mode, runController, runNode, reconcileOnce)
	klog.Infof("Endpoint: %s", endpoint)
	if runNode {
		klog.Infof("Node ID: %s", nodeID)
	}

	// Create driver
	drv, err := driver.NewDriver(&driver.DriverConfig{
		Name:          cfg.DriverName,
		Version:       Version,
		NodeID:        nodeID,
		Endpoint:      endpoint,
		RunController: runController || reconcileOnce,
		RunNode:       runNode,
		Config:        cfg,
		HealthPort:    healthPort,
	})
	if err != nil {
		klog.Fatalf("Failed to create driver: %v", err)
	}

	if reconcileOnce {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		minOrphanAge, parseErr := cfg.Reconcile.MinOrphanAgeDuration()
		if parseErr != nil {
			cancel()
			drv.Stop()
			klog.Fatalf("Invalid reconcile minimum orphan age: %v", parseErr)
		}
		report, reconcileErr := drv.ReconcileOrphans(ctx, driver.ReconcileOptions{
			Delete:       cfg.Reconcile.Delete.Enabled,
			MinOrphanAge: minOrphanAge,
		})
		cancel()
		drv.Stop()
		if reconcileErr != nil {
			klog.Fatalf("Orphan reconcile failed: %v", reconcileErr)
		}
		klog.Infof("Orphan reconcile complete: delete=%v orphanVolumes=%d orphanSnapshots=%d spentRestoreSnapshots=%d deletedVolumes=%d deletedSnapshots=%d deletedSpentRestoreSnapshots=%d skippedDeletes=%d",
			report.DeleteEnabled,
			report.OrphanVolumeCount,
			report.OrphanSnapshotCount,
			report.SpentRestoreSnapshotCount,
			len(report.DeletedVolumes),
			len(report.DeletedSnapshots),
			len(report.DeletedSpentRestoreObjects),
			len(report.SkippedDeletes),
		)
		return
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		klog.Infof("Received signal %v, shutting down", sig)
		drv.Stop()
	}()

	// Run driver
	if err := drv.Run(); err != nil {
		klog.Fatalf("Driver failed: %v", err)
	}
}

func setMemoryLimitFromCgroup(
	v2Path string,
	v1Path string,
	lookupEnv func(string) (string, bool),
	setLimit func(int64) int64,
) (memoryLimit int64, configured bool, err error) {
	if _, configured := lookupEnv("GOMEMLIMIT"); configured {
		return 0, false, nil
	}

	cgroupLimit, finite, err := readCgroupMemoryLimit(v2Path, v1Path)
	if err != nil || !finite {
		return 0, false, err
	}

	// Use 90% of the cgroup limit. Subtracting ceil(10%) avoids overflow near
	// MaxInt64 while keeping the configured value at or below 90%.
	reserve := cgroupLimit / 10
	if cgroupLimit%10 != 0 {
		reserve++
	}
	memoryLimit = cgroupLimit - reserve
	if memoryLimit <= 0 {
		return 0, false, nil
	}

	setLimit(memoryLimit)
	return memoryLimit, true, nil
}

func readCgroupMemoryLimit(v2Path, v1Path string) (limit int64, finite bool, err error) {
	contents, readErr := os.ReadFile(v2Path)
	if readErr == nil {
		return parseCgroupMemoryLimit(v2Path, contents, false)
	}
	if !os.IsNotExist(readErr) {
		return 0, false, fmt.Errorf("read cgroup v2 memory limit: %w", readErr)
	}

	contents, readErr = os.ReadFile(v1Path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read cgroup v1 memory limit: %w", readErr)
	}
	return parseCgroupMemoryLimit(v1Path, contents, true)
}

func parseCgroupMemoryLimit(path string, contents []byte, cgroupV1 bool) (limit int64, finite bool, err error) {
	value := strings.TrimSpace(string(contents))
	if value == "" || value == "max" {
		return 0, false, nil
	}

	parsedLimit, parseErr := strconv.ParseUint(value, 10, 64)
	if parseErr != nil {
		return 0, false, fmt.Errorf("parse cgroup memory limit %q from %s: %w", value, path, parseErr)
	}
	if parsedLimit == 0 {
		return 0, false, nil
	}
	// Cgroup v1 represents an unlimited memory controller using a page-aligned
	// value just below MaxInt64 (or, on some systems, MaxUint64).
	if cgroupV1 && parsedLimit >= uint64(math.MaxInt64-4095) {
		return 0, false, nil
	}
	if parsedLimit > math.MaxInt64 {
		return 0, false, fmt.Errorf("cgroup memory limit %q from %s exceeds int64", value, path)
	}
	return int64(parsedLimit), true, nil
}
