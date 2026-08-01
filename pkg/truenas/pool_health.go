package truenas

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Pool status values of interest from pool.query.
const (
	PoolStatusOnline   = "ONLINE"
	PoolStatusDegraded = "DEGRADED"
	PoolStatusFaulted  = "FAULTED"
	PoolStatusOffline  = "OFFLINE"
	PoolStatusUnavail  = "UNAVAIL"
	PoolStatusRemoved  = "REMOVED"
)

// Scan function/state values from pool.query's `scan` object.
const (
	PoolScanFunctionScrub    = "SCRUB"
	PoolScanFunctionResilver = "RESILVER"
	PoolScanStateScanning    = "SCANNING"
	PoolScanStateFinished    = "FINISHED"
	PoolScanStateCanceled    = "CANCELED"
)

// PoolHealthSnapshot is the driver's view of a pool's health at a point in time.
//
// ZFS has NO per-dataset health signal: health is a per-POOL fact (plus
// per-DISK temperature). Every volume this driver manages lives under a single
// parent dataset on ONE pool, so a pool-level condition is legitimately
// attributable to every managed volume — which is exactly how it is fanned out
// onto per-PVC VolumeConditions. Finer-than-pool granularity is not obtainable.
type PoolHealthSnapshot struct {
	Pool           string
	Status         string
	Healthy        bool
	Warning        bool
	StatusDetail   string
	ScanFunction   string
	ScanState      string
	ScanPercentage float64
	ScanErrors     int64
	// Disks are the pool's member disk device names, used for the temperature
	// alert lookup.
	Disks []string
	// TemperatureAlerts is the number of member disks currently raising a
	// temperature alert (0 = healthy).
	TemperatureAlerts int
	SampledAt         time.Time
}

// Degraded reports a pool state that makes the data path genuinely at risk.
// OFFLINE/REMOVED are deliberately NOT included: an offline spare or a removed
// cache device does not make a volume abnormal.
func (s *PoolHealthSnapshot) Degraded() bool {
	if s == nil {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(s.Status)) {
	case PoolStatusDegraded, PoolStatusFaulted, PoolStatusUnavail:
		return true
	default:
		return false
	}
}

// Scanning reports an in-progress scrub or resilver.
func (s *PoolHealthSnapshot) Scanning() bool {
	if s == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(s.ScanState), PoolScanStateScanning)
}

// PoolHealth reads a pool's health, scan progress and member disk list in one
// pool.query. It performs NO writes.
func (c *Client) PoolHealth(ctx context.Context, pool string) (*PoolHealthSnapshot, error) {
	filters := [][]interface{}{{"name", "=", pool}}
	result, err := c.Call(ctx, "pool.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query pool %s: %w", pool, err)
	}
	return poolHealthFromQueryResult(pool, result)
}

// poolHealthFromQueryResult is the pool.query DECODER, split out from the call so
// a test double can feed it a real middleware response instead of hand-rolling
// the outcome. That matters most for the empty result: a valid pool.query that
// simply does not list the pool is an ANSWER, and it still has to come back as a
// failed sample.
func poolHealthFromQueryResult(pool string, result interface{}) (*PoolHealthSnapshot, error) {
	pools, ok := result.([]interface{})
	if !ok || len(pools) == 0 {
		return nil, fmt.Errorf("pool %s not found", pool)
	}
	entry, ok := pools[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected pool.query entry type %T", pools[0])
	}

	snapshot := &PoolHealthSnapshot{Pool: pool, SampledAt: time.Now()}
	if v, ok := entry["status"].(string); ok {
		snapshot.Status = strings.ToUpper(strings.TrimSpace(v))
	}
	if v, ok := entry["healthy"].(bool); ok {
		snapshot.Healthy = v
	}
	if v, ok := entry["warning"].(bool); ok {
		snapshot.Warning = v
	}
	if v, ok := entry["status_detail"].(string); ok {
		snapshot.StatusDetail = v
	}
	if scan, ok := entry["scan"].(map[string]interface{}); ok {
		if v, ok := scan["function"].(string); ok {
			snapshot.ScanFunction = strings.ToUpper(strings.TrimSpace(v))
		}
		if v, ok := scan["state"].(string); ok {
			snapshot.ScanState = strings.ToUpper(strings.TrimSpace(v))
		}
		if v, ok := scan["percentage"].(float64); ok {
			snapshot.ScanPercentage = v
		}
		if v, ok := scan["errors"].(float64); ok {
			snapshot.ScanErrors = int64(v)
		}
	}
	snapshot.Disks = poolTopologyDisks(entry["topology"])
	return snapshot, nil
}

// DiskTemperatureAlerts returns the member disks currently raising a temperature
// alert. The middleware REQUIRES an explicit names array; an empty list short
// circuits without an API call.
func (c *Client) DiskTemperatureAlerts(ctx context.Context, names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	result, err := c.Call(ctx, "disk.temperature_alerts", names)
	if err != nil {
		return nil, fmt.Errorf("failed to read disk temperature alerts: %w", err)
	}
	entries, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected disk.temperature_alerts response type %T", result)
	}
	alerts := make([]string, 0, len(entries))
	for _, item := range entries {
		switch value := item.(type) {
		case string:
			alerts = append(alerts, value)
		case map[string]interface{}:
			// Alert objects carry the device in one of these keys depending on the
			// alert class; fall back to the formatted text so an alert is never
			// silently dropped just because its shape is unfamiliar.
			for _, key := range []string{"device", "name", "formatted", "text"} {
				if s, ok := value[key].(string); ok && s != "" {
					alerts = append(alerts, s)
					break
				}
			}
		}
	}
	return alerts, nil
}

// poolTopologyDisks flattens every vdev's member device names across all
// topology classes (data, cache, dedup, log, spare, special).
func poolTopologyDisks(topology interface{}) []string {
	root, ok := topology.(map[string]interface{})
	if !ok {
		return nil
	}
	seen := make(map[string]struct{})
	var walk func(node interface{})
	walk = func(node interface{}) {
		switch value := node.(type) {
		case []interface{}:
			for _, item := range value {
				walk(item)
			}
		case map[string]interface{}:
			if disk, ok := value["disk"].(string); ok && disk != "" {
				seen[disk] = struct{}{}
			}
			if children, ok := value["children"]; ok {
				walk(children)
			}
		}
	}
	for _, class := range []string{"data", "cache", "dedup", "log", "spare", "special"} {
		walk(root[class])
	}
	disks := make([]string, 0, len(seen))
	for disk := range seen {
		disks = append(disks, disk)
	}
	sort.Strings(disks)
	return disks
}
