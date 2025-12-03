package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

var (
	host       = flag.String("host", "", "TrueNAS host (or set TRUENAS_HOST)")
	apiKey     = flag.String("api-key", "", "TrueNAS API key (or set TRUENAS_API_KEY)")
	insecure   = flag.Bool("insecure", true, "Allow insecure TLS connections")
	timeout    = flag.Duration("timeout", 60*time.Second, "API timeout")
	command    = flag.String("cmd", "help", "Command to run: help, call, services, service-reload, datasets, iscsi-audit, snapshots")
	method     = flag.String("method", "", "API method to call (for 'call' command)")
	params     = flag.String("params", "", "JSON params for API call (for 'call' command)")
	dataset    = flag.String("dataset", "", "Dataset path for dataset commands")
	service    = flag.String("service", "", "Service name for service commands")
	verbose    = flag.Bool("v", false, "Verbose output")
)

func main() {
	flag.Parse()

	// Get credentials from flags or environment
	h := *host
	if h == "" {
		h = os.Getenv("TRUENAS_HOST")
	}
	if h == "" {
		fmt.Println("Error: TrueNAS host required. Use -host flag or TRUENAS_HOST env var")
		os.Exit(1)
	}

	k := *apiKey
	if k == "" {
		k = os.Getenv("TRUENAS_API_KEY")
	}
	if k == "" {
		fmt.Println("Error: API key required. Use -api-key flag or TRUENAS_API_KEY env var")
		os.Exit(1)
	}

	client, err := truenas.NewClient(&truenas.ClientConfig{
		Host:              h,
		Port:              443,
		Protocol:          "https",
		APIKey:            k,
		AllowInsecure:     *insecure,
		Timeout:           *timeout,
		MaxConcurrentReqs: 1,
		MaxConnections:    1,
	})
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	switch *command {
	case "help":
		printHelp()
	case "call":
		cmdCall(ctx, client)
	case "services":
		cmdServices(ctx, client)
	case "service-reload":
		cmdServiceReload(ctx, client)
	case "datasets":
		cmdDatasets(ctx, client)
	case "iscsi-audit":
		cmdISCSIAudit(ctx, client)
	case "snapshots":
		cmdSnapshots(ctx, client)
	case "test-errors":
		cmdTestErrors(ctx, client)
	case "nvmeof-audit":
		cmdNVMeoFAudit(ctx, client)
	default:
		fmt.Printf("Unknown command: %s\n", *command)
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println("TrueNAS Scale CSI Debug Tool")
	fmt.Println()
	fmt.Println("Usage: debug-api -host <host> -api-key <key> -cmd <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  help           Show this help message")
	fmt.Println("  call           Make a raw API call (-method required, -params optional)")
	fmt.Println("  services       List all services and their status")
	fmt.Println("  service-reload Reload a service (-service required)")
	fmt.Println("  datasets       List datasets (-dataset for parent path filter)")
	fmt.Println("  iscsi-audit    Audit iSCSI targets, extents, and associations")
	fmt.Println("  snapshots      List snapshots (-dataset for filter)")
	fmt.Println("  test-errors    Test various API error responses")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  debug-api -cmd services")
	fmt.Println("  debug-api -cmd call -method core.ping")
	fmt.Println("  debug-api -cmd call -method pool.dataset.query -params '[[\"id\",\"=\",\"tank/data\"]]'")
	fmt.Println("  debug-api -cmd service-reload -service iscsitarget")
	fmt.Println("  debug-api -cmd datasets -dataset flashstor/scale-csi")
	fmt.Println("  debug-api -cmd iscsi-audit")
	fmt.Println("  debug-api -cmd test-errors")
}

func cmdCall(ctx context.Context, client *truenas.Client) {
	if *method == "" {
		fmt.Println("Error: -method required for 'call' command")
		os.Exit(1)
	}

	var callParams []interface{}
	if *params != "" {
		if err := json.Unmarshal([]byte(*params), &callParams); err != nil {
			// Try as single param
			var single interface{}
			if err := json.Unmarshal([]byte(*params), &single); err != nil {
				fmt.Printf("Error parsing params: %v\n", err)
				os.Exit(1)
			}
			callParams = []interface{}{single}
		}
	}

	fmt.Printf("Calling: %s\n", *method)
	if *verbose && len(callParams) > 0 {
		fmt.Printf("Params: %v\n", callParams)
	}
	fmt.Println()

	result, err := client.Call(ctx, *method, callParams...)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		if apiErr, ok := err.(*truenas.APIError); ok {
			fmt.Printf("  Code: %d\n", apiErr.Code)
			fmt.Printf("  Message: %s\n", apiErr.Message)
		}
		os.Exit(1)
	}

	// Pretty print result
	jsonBytes, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Result:\n%s\n", string(jsonBytes))
}

func cmdServices(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== TrueNAS Services ===")
	fmt.Println()

	result, err := client.Call(ctx, "service.query")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	services, ok := result.([]interface{})
	if !ok {
		fmt.Println("Unexpected response format")
		os.Exit(1)
	}

	fmt.Printf("%-20s %-10s %-10s\n", "SERVICE", "STATE", "ENABLED")
	fmt.Println(strings.Repeat("-", 42))

	for _, svc := range services {
		if m, ok := svc.(map[string]interface{}); ok {
			name := m["service"]
			state := m["state"]
			enable := m["enable"]
			fmt.Printf("%-20v %-10v %-10v\n", name, state, enable)
		}
	}
}

func cmdServiceReload(ctx context.Context, client *truenas.Client) {
	if *service == "" {
		fmt.Println("Error: -service required for 'service-reload' command")
		os.Exit(1)
	}

	fmt.Printf("Reloading service: %s\n", *service)

	result, err := client.Call(ctx, "service.reload", *service)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		if apiErr, ok := err.(*truenas.APIError); ok {
			fmt.Printf("  Code: %d\n", apiErr.Code)
			fmt.Printf("  Message: %s\n", apiErr.Message)
		}
		os.Exit(1)
	}

	fmt.Printf("Result: %v\n", result)
}

func cmdDatasets(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== TrueNAS Datasets ===")
	fmt.Println()

	var filters []interface{}
	if *dataset != "" {
		// Filter to children of specified dataset
		filters = []interface{}{[]interface{}{"id", "^", *dataset}}
	}

	result, err := client.Call(ctx, "pool.dataset.query", filters, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	datasets, ok := result.([]interface{})
	if !ok {
		fmt.Println("Unexpected response format")
		os.Exit(1)
	}

	fmt.Printf("Found %d datasets:\n\n", len(datasets))

	for _, ds := range datasets {
		if m, ok := ds.(map[string]interface{}); ok {
			id := m["id"]
			dsType := m["type"]
			used := "?"
			if usedObj, ok := m["used"].(map[string]interface{}); ok {
				used = fmt.Sprintf("%v", usedObj["value"])
			}
			fmt.Printf("  %v (%v) - used: %s\n", id, dsType, used)
		}
	}
}

func cmdSnapshots(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== TrueNAS Snapshots ===")
	fmt.Println()

	var filters []interface{}
	if *dataset != "" {
		filters = []interface{}{[]interface{}{"dataset", "^", *dataset}}
	}

	result, err := client.Call(ctx, "zfs.snapshot.query", filters, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	snapshots, ok := result.([]interface{})
	if !ok {
		fmt.Println("Unexpected response format")
		os.Exit(1)
	}

	fmt.Printf("Found %d snapshots:\n\n", len(snapshots))

	for _, snap := range snapshots {
		if m, ok := snap.(map[string]interface{}); ok {
			id := m["id"]
			name := m["name"]
			fmt.Printf("  %v (name: %v)\n", id, name)
		}
	}
}

func cmdISCSIAudit(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== iSCSI Audit ===")
	fmt.Println()

	// Get targets
	fmt.Println("Fetching targets...")
	targetsResult, err := client.Call(ctx, "iscsi.target.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching targets: %v\n", err)
		os.Exit(1)
	}
	targets, _ := targetsResult.([]interface{})
	fmt.Printf("  Found %d targets\n", len(targets))

	// Get extents
	fmt.Println("Fetching extents...")
	extentsResult, err := client.Call(ctx, "iscsi.extent.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching extents: %v\n", err)
		os.Exit(1)
	}
	extents, _ := extentsResult.([]interface{})
	fmt.Printf("  Found %d extents\n", len(extents))

	// Get target-extent associations
	fmt.Println("Fetching target-extent associations...")
	assocsResult, err := client.Call(ctx, "iscsi.targetextent.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching associations: %v\n", err)
		os.Exit(1)
	}
	assocs, _ := assocsResult.([]interface{})
	fmt.Printf("  Found %d associations\n", len(assocs))

	// Get sessions
	fmt.Println("Fetching active sessions...")
	sessionsResult, err := client.Call(ctx, "iscsi.global.sessions")
	if err != nil {
		fmt.Printf("Warning: Could not fetch sessions: %v\n", err)
	}
	sessions, _ := sessionsResult.([]interface{})
	fmt.Printf("  Found %d active sessions\n", len(sessions))

	fmt.Println()

	// Build maps
	targetIDs := make(map[float64]bool)
	extentIDs := make(map[float64]bool)
	targetHasExtent := make(map[float64]bool)
	extentHasTarget := make(map[float64]bool)

	for _, t := range targets {
		if m, ok := t.(map[string]interface{}); ok {
			if id, ok := m["id"].(float64); ok {
				targetIDs[id] = true
			}
		}
	}

	for _, e := range extents {
		if m, ok := e.(map[string]interface{}); ok {
			if id, ok := m["id"].(float64); ok {
				extentIDs[id] = true
			}
		}
	}

	for _, a := range assocs {
		if m, ok := a.(map[string]interface{}); ok {
			if tid, ok := m["target"].(float64); ok {
				targetHasExtent[tid] = true
			}
			if eid, ok := m["extent"].(float64); ok {
				extentHasTarget[eid] = true
			}
		}
	}

	// Find orphans
	var orphanedTargets []map[string]interface{}
	var orphanedExtents []map[string]interface{}

	for _, t := range targets {
		if m, ok := t.(map[string]interface{}); ok {
			if id, ok := m["id"].(float64); ok {
				if !targetHasExtent[id] {
					orphanedTargets = append(orphanedTargets, m)
				}
			}
		}
	}

	for _, e := range extents {
		if m, ok := e.(map[string]interface{}); ok {
			if id, ok := m["id"].(float64); ok {
				if !extentHasTarget[id] {
					orphanedExtents = append(orphanedExtents, m)
				}
			}
		}
	}

	// Print results
	fmt.Println("=== Summary ===")
	fmt.Printf("Targets without extents: %d\n", len(orphanedTargets))
	for _, t := range orphanedTargets {
		fmt.Printf("  - ID: %.0f, Name: %v\n", t["id"], t["name"])
	}

	fmt.Printf("\nExtents without targets: %d\n", len(orphanedExtents))
	for _, e := range orphanedExtents {
		fmt.Printf("  - ID: %.0f, Name: %v, Disk: %v\n", e["id"], e["name"], e["disk"])
	}

	if len(orphanedTargets) == 0 && len(orphanedExtents) == 0 {
		fmt.Println("\nNo orphaned iSCSI resources found.")
	}
}

func cmdTestErrors(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== Testing API Error Responses ===")
	fmt.Println()

	tests := []struct {
		name   string
		method string
		params []interface{}
	}{
		{"Valid call (core.ping)", "core.ping", nil},
		{"Invalid method", "invalid.method.xyz", nil},
		{"Delete non-existent dataset", "pool.dataset.delete", []interface{}{"nonexistent/dataset/path", map[string]interface{}{"recursive": true}}},
		{"Get non-existent dataset", "pool.dataset.query", []interface{}{[][]interface{}{{"id", "=", "nonexistent/dataset"}}}},
		{"Reload valid service", "service.reload", []interface{}{"iscsitarget"}},
		{"Reload invalid service", "service.reload", []interface{}{"invalid_service_xyz"}},
		{"Delete non-existent snapshot", "zfs.snapshot.delete", []interface{}{"nonexistent/snap@test", map[string]interface{}{}}},
	}

	results := make([]struct {
		name    string
		result  interface{}
		err     error
		errCode int
		errMsg  string
	}, 0)

	for _, test := range tests {
		fmt.Printf("Testing: %s\n", test.name)
		fmt.Printf("  Method: %s\n", test.method)

		result, err := client.Call(ctx, test.method, test.params...)

		r := struct {
			name    string
			result  interface{}
			err     error
			errCode int
			errMsg  string
		}{name: test.name, result: result, err: err}

		if err != nil {
			if apiErr, ok := err.(*truenas.APIError); ok {
				r.errCode = apiErr.Code
				r.errMsg = apiErr.Message
				fmt.Printf("  Error Code: %d\n", apiErr.Code)
				fmt.Printf("  Error Message: %s\n", apiErr.Message)
			} else {
				fmt.Printf("  Error: %v\n", err)
			}
		} else {
			fmt.Printf("  Result: %v\n", result)
		}
		fmt.Println()

		results = append(results, r)
	}

	// Summary table
	fmt.Println("=== Error Code Summary ===")
	fmt.Println()
	fmt.Printf("%-40s %-10s %s\n", "TEST", "CODE", "MESSAGE")
	fmt.Println(strings.Repeat("-", 80))

	// Sort by error code
	sort.Slice(results, func(i, j int) bool {
		return results[i].errCode < results[j].errCode
	})

	for _, r := range results {
		code := "OK"
		msg := fmt.Sprintf("%v", r.result)
		if r.err != nil {
			code = fmt.Sprintf("%d", r.errCode)
			msg = r.errMsg
		}
		if len(msg) > 30 {
			msg = msg[:30] + "..."
		}
		fmt.Printf("%-40s %-10s %s\n", r.name, code, msg)
	}
}

func cmdNVMeoFAudit(ctx context.Context, client *truenas.Client) {
	fmt.Println("=== NVMe-oF Audit ===")
	fmt.Println()

	// Get subsystems
	fmt.Println("Fetching subsystems...")
	subsysResult, err := client.Call(ctx, "nvmet.subsys.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching subsystems: %v\n", err)
		os.Exit(1)
	}
	subsystems, _ := subsysResult.([]interface{})
	fmt.Printf("  Found %d subsystems\n", len(subsystems))

	// Get namespaces
	fmt.Println("Fetching namespaces...")
	nsResult, err := client.Call(ctx, "nvmet.namespace.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching namespaces: %v\n", err)
		os.Exit(1)
	}
	namespaces, _ := nsResult.([]interface{})
	fmt.Printf("  Found %d namespaces\n", len(namespaces))

	// Get ports
	fmt.Println("Fetching ports...")
	portsResult, err := client.Call(ctx, "nvmet.port.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching ports: %v\n", err)
		os.Exit(1)
	}
	ports, _ := portsResult.([]interface{})
	fmt.Printf("  Found %d ports\n", len(ports))

	// Get port-subsystem associations
	fmt.Println("Fetching port-subsystem associations...")
	portSubsysResult, err := client.Call(ctx, "nvmet.port_subsys.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		fmt.Printf("Error fetching port-subsystem associations: %v\n", err)
		os.Exit(1)
	}
	portSubsys, _ := portSubsysResult.([]interface{})
	fmt.Printf("  Found %d port-subsystem associations\n", len(portSubsys))

	fmt.Println()

	// Print ports
	fmt.Println("=== Ports ===")
	for _, p := range ports {
		if m, ok := p.(map[string]interface{}); ok {
			fmt.Printf("  Port ID: %.0f\n", m["id"])
			fmt.Printf("    Transport: %v\n", m["addr_trtype"])
			fmt.Printf("    Address: %v\n", m["addr_traddr"])
			fmt.Printf("    Port: %v\n", m["addr_trsvcid"])
			if subs, ok := m["subsystems"].([]interface{}); ok {
				fmt.Printf("    Subsystems: %v\n", subs)
			}
			fmt.Println()
		}
	}

	// Print port-subsystem associations
	fmt.Println("=== Port-Subsystem Associations ===")
	for _, ps := range portSubsys {
		if m, ok := ps.(map[string]interface{}); ok {
			portID := "?"
			subsysID := "?"
			subsysNQN := "?"
			if port, ok := m["port"].(map[string]interface{}); ok {
				if id, ok := port["id"].(float64); ok {
					portID = fmt.Sprintf("%.0f", id)
				}
			}
			if subsys, ok := m["subsys"].(map[string]interface{}); ok {
				if id, ok := subsys["id"].(float64); ok {
					subsysID = fmt.Sprintf("%.0f", id)
				}
				if nqn, ok := subsys["subnqn"].(string); ok {
					subsysNQN = nqn
				}
			}
			fmt.Printf("  Port %s <-> Subsystem %s (%s)\n", portID, subsysID, subsysNQN)
		}
	}

	// Build maps for orphan detection
	subsysIDs := make(map[float64]string) // id -> nqn
	subsysHasPort := make(map[float64]bool)

	for _, s := range subsystems {
		if m, ok := s.(map[string]interface{}); ok {
			if id, ok := m["id"].(float64); ok {
				nqn := ""
				if n, ok := m["subnqn"].(string); ok {
					nqn = n
				}
				subsysIDs[id] = nqn
			}
		}
	}

	for _, ps := range portSubsys {
		if m, ok := ps.(map[string]interface{}); ok {
			if subsys, ok := m["subsys"].(map[string]interface{}); ok {
				if id, ok := subsys["id"].(float64); ok {
					subsysHasPort[id] = true
				}
			}
		}
	}

	// Find orphaned subsystems (no port association)
	fmt.Println()
	fmt.Println("=== Subsystems Without Port Associations ===")
	orphanCount := 0
	for id, nqn := range subsysIDs {
		if !subsysHasPort[id] {
			fmt.Printf("  Subsystem %.0f: %s\n", id, nqn)
			orphanCount++
		}
	}
	if orphanCount == 0 {
		fmt.Println("  None - all subsystems have port associations")
	} else {
		fmt.Printf("\n  WARNING: %d subsystems are not accessible (no port association)\n", orphanCount)
	}
}
