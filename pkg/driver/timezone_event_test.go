package driver

// Event-emission tests for EventReasonNASTimezoneUnresolved (observability
// for the GF2-fix2/B1-a fail-closed timezone chain). The DECISIONS under test
// are already proven in controller_snapshot_schedule_test.go (preserve as
// foreign, FailedPrecondition); these tests prove only that the operator now
// gets a cluster-visible Warning Event NAMING the timezone as the cause, and
// that the happy path stays silent. Kept in a separate file to avoid
// collisions with concurrent work on the schedule tests.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// attachFakeRecorder wires a fake event recorder onto the driver and returns
// it. Call AFTER seeding so provisioning-time events do not pollute the drain.
func attachFakeRecorder(d *Driver) *record.FakeRecorder {
	rec := record.NewFakeRecorder(16)
	d.eventRecorder = &EventRecorder{recorder: rec, enabled: true}
	return rec
}

// timezoneUnresolvedEvents filters the drain down to the reason under test;
// DeleteVolume legitimately emits VolumeDeleteFailed beside it.
func timezoneUnresolvedEvents(rec *record.FakeRecorder) []string {
	var matched []string
	for _, event := range drainEvents(rec) {
		if strings.Contains(event, EventReasonNASTimezoneUnresolved) {
			matched = append(matched, event)
		}
	}
	return matched
}

// An unreadable live NAS zone during DeleteVolume must produce EXACTLY ONE
// Warning Event that names the timezone as the reason snapshots are preserved
// (one per operation, never per snapshot: two snapshots exist here).
func TestDeleteVolumeEmitsTimezoneUnresolvedEventWhenZoneUnreadable(t *testing.T) {
	client := truenas.NewMockClient()
	client.SystemTimezoneName = "America/New_York"
	d := newScheduleTestDriver(client)

	seedScheduledVolume(t, client, d, "sched-ev-nozone")
	fireScheduledSnapshot(t, client, time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC), "America/New_York")
	fireScheduledSnapshot(t, client, time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC), "America/New_York")
	client.SystemTimezoneErr = errors.New("injected system.general.config failure")
	rec := attachFakeRecorder(d)

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "sched-ev-nozone"})
	require.Error(t, err, "an unreadable NAS timezone must still fail closed")

	events := timezoneUnresolvedEvents(rec)
	require.Len(t, events, 1, "exactly one event per operation, even with multiple scheduled snapshots")
	assert.Contains(t, events[0], "Warning")
	assert.Contains(t, events[0], "could not resolve the NAS civil timezone (system.general.config)")
	assert.Contains(t, events[0], "fail closed")
	assert.Contains(t, events[0], "accumulate", "the event must state the operational consequence")
}

// A NAS timezone reconfiguration after the task was created must produce the
// same reason with the mismatch spelled out (stored vs. current zone).
func TestDeleteVolumeEmitsTimezoneUnresolvedEventWhenZoneChanged(t *testing.T) {
	client := truenas.NewMockClient()
	client.SystemTimezoneName = "America/New_York"
	d := newScheduleTestDriver(client)

	seedScheduledVolume(t, client, d, "sched-ev-tzmoved")
	fireScheduledSnapshot(t, client, time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC), "America/New_York")
	client.SystemTimezoneName = "Europe/Berlin"
	rec := attachFakeRecorder(d)

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "sched-ev-tzmoved"})
	require.Error(t, err, "a timezone change must still fail closed")

	events := timezoneUnresolvedEvents(rec)
	require.Len(t, events, 1)
	assert.Contains(t, events[0], `"America/New_York"`)
	assert.Contains(t, events[0], `"Europe/Berlin"`)
	assert.Contains(t, events[0], "reconfigured clock")
	assert.Contains(t, events[0], "fail closed")
}

// The third fail-closed branch, a dataset with NO locally-recorded task
// timezone (e.g. a clone that inherited the property non-locally), must also
// be named. Exercised at the scheduledSnapshotZone level, the exact seam
// DeleteVolume calls (controller.go wires it behind scheduledTaskSchema != "").
func TestScheduledSnapshotZoneEmitsEventWhenTimezoneRecordMissing(t *testing.T) {
	client := truenas.NewMockClient()
	d := newScheduleTestDriver(client)
	rec := attachFakeRecorder(d)

	ds := &truenas.Dataset{
		Name: "pool/parent/inherited-zone",
		UserProperties: map[string]truenas.UserProperty{
			PropSnapshotTaskTimezone: {Value: "UTC", Source: "pool/parent/source@snap"},
		},
	}
	require.Nil(t, d.scheduledSnapshotZone(context.Background(), ds, ds.Name))

	events := timezoneUnresolvedEvents(rec)
	require.Len(t, events, 1)
	assert.Contains(t, events[0], "no locally-recorded periodic-snapshot task timezone")
	assert.Contains(t, events[0], "fail closed")
}

// The happy path must stay SILENT: a resolvable, unchanged zone emits no
// NASTimezoneUnresolved event anywhere in a successful scheduled delete.
func TestDeleteVolumeEmitsNoTimezoneEventOnSuccess(t *testing.T) {
	client := truenas.NewMockClient()
	client.SystemTimezoneName = "America/New_York"
	d := newScheduleTestDriver(client)

	seedScheduledVolume(t, client, d, "sched-ev-owned")
	fireScheduledSnapshot(t, client, time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC), "America/New_York")
	rec := attachFakeRecorder(d)

	_, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: "sched-ev-owned"})
	require.NoError(t, err, "a provable scheduled snapshot must not block deletion")

	assert.Empty(t, timezoneUnresolvedEvents(rec),
		"a resolvable, unchanged NAS timezone must not raise the unresolved event")
}

// A nil event recorder (events disabled, or a unit-test driver without one)
// must never panic on the fail-closed path: the emitter guards like every
// other emitter in the driver.
func TestTimezoneUnresolvedEventNilRecorderIsSafe(t *testing.T) {
	client := truenas.NewMockClient()
	client.SystemTimezoneErr = errors.New("injected failure")
	d := newScheduleTestDriver(client) // eventRecorder deliberately nil

	ds := &truenas.Dataset{
		Name: "pool/parent/no-recorder",
		UserProperties: map[string]truenas.UserProperty{
			PropSnapshotTaskTimezone: {Value: "UTC", Source: "local"},
		},
	}
	assert.NotPanics(t, func() {
		assert.Nil(t, d.scheduledSnapshotZone(context.Background(), ds, ds.Name))
	})
}
