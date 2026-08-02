package truenas

import (
	"context"
	"fmt"
	"sync"
	"time"

	// The driver's runtime image is alpine-based and its Dockerfile `apk add`
	// list does NOT include the `tzdata` package, so /usr/share/zoneinfo does not
	// exist in the container even though it does on a developer Mac. Without this
	// import, time.LoadLocation would succeed in every test and fail in
	// production — the exact class of bug where the test environment disagrees
	// with the deployment. Embedding the IANA database in the binary (~450 KB)
	// makes zone resolution identical everywhere. Go still prefers a system
	// zoneinfo when one is present and falls back to this embedded copy.
	_ "time/tzdata"
)

// systemTimezoneTTL bounds how long a resolved NAS timezone is trusted before it
// is re-read.
//
// It is deliberately SHORT (GF2-fix3/B1-a). Round 2 used an hour here and
// memoized the result a second time on the Driver, so a zone reconfiguration —
// or a lookup that would now FAIL — could be bypassed for an hour while the
// stale value kept authorizing deletes. This is now the ONLY cache of the value,
// it is dropped on every reconnect (invalidateSystemTimezone), and an error is
// never cached. Five minutes bounds the stale-authorization window to something
// an operator can reason about while still collapsing bursts of scheduled
// DeleteVolume calls into a single round trip.
//
// The correctness of a scheduled-snapshot decision does NOT rest on this TTL:
// the driver additionally compares the live value against the zone RECORDED on
// the dataset when its task was created, and any difference fails closed.
const systemTimezoneTTL = 5 * time.Minute

// SystemTimezone returns the NAS's configured civil timezone.
//
// LIVE-VERIFIED (TrueNAS 26.0.0-BETA.1, read-only probe 2026-08-01):
//
//	midclt call system.general.config
//	-> {"id": 1, ..., "kbdmap": "us", "timezone": "America/New_York", ...}
//
// so the method is `system.general.config` and the field is `timezone`, carrying
// an IANA zone name. This is what makes exact name-vs-creation agreement
// possible: a periodic-snapshot task renders %Y%m%d-%H%M%S from this zone's
// civil clock, while the snapshot's `creation` property is UTC epoch seconds.
//
// The result is CACHED for systemTimezoneTTL, and the cache is dropped on
// reconnect (see invalidateSystemTimezone), so no caller pays a per-operation
// round trip. An error is never cached: callers fail CLOSED on error, and a
// cached failure would extend a transient outage into an hour of refusals.
func (c *Client) SystemTimezone(ctx context.Context) (*time.Location, error) {
	c.timezoneMu.RLock()
	if c.timezoneLoc != nil && time.Since(c.timezoneAt) < systemTimezoneTTL {
		cached := c.timezoneLoc
		c.timezoneMu.RUnlock()
		return cached, nil
	}
	c.timezoneMu.RUnlock()

	result, err := c.Call(ctx, "system.general.config")
	if err != nil {
		return nil, fmt.Errorf("failed to read the NAS timezone: %w", err)
	}
	name, err := systemTimezoneName(result)
	if err != nil {
		return nil, err
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		// With the embedded tzdata above this can only mean the NAS reports a
		// zone name the IANA database does not contain.
		return nil, fmt.Errorf("NAS timezone %q is not a loadable IANA zone: %w", name, err)
	}

	c.timezoneMu.Lock()
	c.timezoneLoc = loc
	c.timezoneAt = time.Now()
	c.timezoneMu.Unlock()
	return loc, nil
}

// systemTimezoneName extracts the `timezone` field from a system.general.config
// response. A missing or empty field is an error, never a silent UTC default: a
// wrong zone would silently misclassify every scheduled snapshot.
func systemTimezoneName(data interface{}) (string, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("unexpected system.general.config format %T", data)
	}
	name, ok := m["timezone"].(string)
	if !ok || name == "" {
		return "", fmt.Errorf("system.general.config carries no usable timezone field")
	}
	return name, nil
}

// invalidateSystemTimezone drops the cached zone. Called when the client
// reconnects, because a reconnect may be to a different backend (HA failover) or
// follow a middleware restart that applied a configuration change.
func (c *Client) invalidateSystemTimezone() {
	c.timezoneMu.Lock()
	c.timezoneLoc = nil
	c.timezoneAt = time.Time{}
	c.timezoneMu.Unlock()
}

// systemTimezoneCache is embedded in Client.
type systemTimezoneCache struct {
	timezoneMu  sync.RWMutex
	timezoneLoc *time.Location
	timezoneAt  time.Time
}
