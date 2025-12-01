// Package main implements the TrueNAS Scale CSI driver entry point.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/GizmoTickler/scale-csi/pkg/driver"
	"k8s.io/klog/v2"
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
	flag.StringVar(&mode, "mode", "all", "Driver mode: controller, node, or all")
	flag.IntVar(&healthPort, "health-port", 9809, "Port for health/metrics HTTP server (0 to disable)")
	flag.BoolVar(&showVersion, "version", false, "Show version and exit")

	klog.InitFlags(nil)
	flag.Parse()

	if showVersion {
		fmt.Printf("Scale CSI Driver (for TrueNAS SCALE)\nVersion: %s\nCommit: %s\n", Version, GitCommit)
		os.Exit(0)
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

	// Validate mode
	runController := mode == "controller" || mode == "all"
	runNode := mode == "node" || mode == "all"

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
	klog.Infof("Mode: %s (controller=%v, node=%v)", mode, runController, runNode)
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
		RunController: runController,
		RunNode:       runNode,
		Config:        cfg,
		HealthPort:    healthPort,
	})
	if err != nil {
		klog.Fatalf("Failed to create driver: %v", err)
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
