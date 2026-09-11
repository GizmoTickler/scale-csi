package driver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// assertOneISCSIGroupPerPortal is the invariant TrueNAS 26.0 enforces on both
// iscsi.target.create and iscsi.target.update: a "groups" array may not carry
// two entries for the same portal ID, on pain of a bare -32602 "Invalid params".
func assertOneISCSIGroupPerPortal(t *testing.T, groups []truenas.ISCSITargetGroup) {
	t.Helper()
	seen := make(map[int]int, len(groups))
	for _, group := range groups {
		seen[group.Portal]++
	}
	for portal, count := range seen {
		assert.LessOrEqualf(t, count, 1,
			"portal %d appears in %d groups; TrueNAS 26.0 rejects more than one", portal, count)
	}
}

// TestISCSIMultipathConvergenceDedupesTargetGroupsByPortal is the regression for
// the second half of D1. The portal dedupe landed at applyISCSIFence's
// iscsi.target.update but not at convergeISCSIMultipathTargetGroups', which is
// the other site that hands TrueNAS a groups array it assembled itself.
//
// Both subtests below are shapes convergence produces on its own — it appends
// (configured portals x CSI-owned templates) onto the target's existing groups
// without ever checking whether a portal now appears twice — and both make every
// iSCSI CreateVolume and ControllerPublishVolume for the target fail with a bare
// -32602 for as long as the shape persists. The mock's
// RejectDuplicatePortalISCSITargetGroups reproduces the live appliance
// constraint (see TestE2ERealDebug_D1IsolateISCSITargetUpdate); without it the
// mock is permissive and neither case fails.
func TestISCSIMultipathConvergenceDedupesTargetGroupsByPortal(t *testing.T) {
	t.Run("an operator group already on a configured portal", func(t *testing.T) {
		ctx := context.Background()
		client := truenas.NewMockClient()
		addGF6ISCSIPortals(client)
		client.RejectDuplicatePortalISCSITargetGroups = true

		ownedGroup, err := client.ISCSIInitiatorCreate(ctx, iscsiOwnedAllowAllInitiatorComment)
		require.NoError(t, err)
		operatorGroup, err := client.ISCSIInitiatorCreate(ctx, "operator scoped to storage VLAN A")
		require.NoError(t, err)

		target := &truenas.ISCSITarget{
			ID: 992, Name: "gf6-operator-portal-collision", Mode: "ISCSI",
			Groups: []truenas.ISCSITargetGroup{
				{Portal: 1, Initiator: ownedGroup.ID, AuthMethod: "NONE"},
				// The operator placed their own group on portal 2, which is also
				// a configured multipath portal. Convergence replicates the
				// CSI-owned template onto portal 2 as well, and portal 2 then
				// carries two groups.
				{Portal: 2, Initiator: operatorGroup.ID, AuthMethod: "NONE"},
			},
		}
		client.ISCSITargets[target.ID] = target
		d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
		d.config.ISCSI.Multipath = true
		d.config.ISCSI.Portals = []string{"192.0.2.11"}

		updated, err := d.convergeISCSIMultipathTargetGroups(ctx, target)
		require.NoError(t, err,
			"one operator-placed group on a configured portal must not make every iSCSI publish fail with -32602")
		assertOneISCSIGroupPerPortal(t, updated.Groups)
		assert.Contains(t, updated.Groups, truenas.ISCSITargetGroup{
			Portal: 2, Initiator: ownedGroup.ID, AuthMethod: "NONE",
		}, "the CSI-owned fencing template must be the group that survives on the shared portal")
	})

	t.Run("two CSI-owned templates replicated onto one portal", func(t *testing.T) {
		ctx := context.Background()
		client := truenas.NewMockClient()
		addGF6ISCSIPortals(client)
		client.RejectDuplicatePortalISCSITargetGroups = true

		firstOwned, err := client.ISCSIInitiatorCreate(ctx, iscsiOwnedAllowAllInitiatorComment)
		require.NoError(t, err)
		secondOwned, err := client.ISCSIInitiatorCreateWithInitiators(ctx,
			[]string{"iqn.1993-08.org.debian:gf6-worker"}, "scale-csi fencing: pool/parent/gf6-two-templates")
		require.NoError(t, err)

		// No operator is involved here at all. The target legitimately carries
		// two CSI-owned groups on two different portals, so BOTH are templates,
		// and the (portals x templates) product puts both of them on portal 2.
		target := &truenas.ISCSITarget{
			ID: 993, Name: "gf6-two-templates", Mode: "ISCSI",
			Groups: []truenas.ISCSITargetGroup{
				{Portal: 1, Initiator: firstOwned.ID, AuthMethod: "NONE"},
				{Portal: 3, Initiator: secondOwned.ID, AuthMethod: "NONE"},
			},
		}
		client.ISCSITargets[target.ID] = target
		d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
		d.config.ISCSI.Multipath = true
		d.config.ISCSI.Portals = []string{"192.0.2.11"}

		updated, err := d.convergeISCSIMultipathTargetGroups(ctx, target)
		require.NoError(t, err,
			"convergence must not send a groups array whose own portal x template product duplicates a portal")
		assertOneISCSIGroupPerPortal(t, updated.Groups)
	})
}

// TestISCSITargetCreateDedupesConfiguredTargetGroupsByPortal covers the third
// site that builds a groups array: an operator who lists two iscsi.targetGroups
// entries on one portal ID. Nothing validated that before, so every single
// iSCSI CreateVolume failed with a bare -32602 and no hint of the cause. The
// dedupe now sits on the path to the API (iscsiTargetCreateGroups) rather than
// at individual call sites, and logs which group it superseded.
func TestISCSITargetCreateDedupesConfiguredTargetGroupsByPortal(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	client.RejectDuplicatePortalISCSITargetGroups = true
	d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
	d.config.ISCSI.TargetGroups = []ISCSITargetGroup{
		{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
		{Portal: 1, Initiator: 1, AuthMethod: "CHAP"},
	}

	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-configured-collision", "iscsi"))
	require.NoError(t, err,
		"two iscsi.targetGroups entries on one portal must not make iSCSI CreateVolume fail with -32602")

	target, err := client.ISCSITargetFindByName(ctx, d.iscsiShareName("gf6-configured-collision"))
	require.NoError(t, err)
	require.NotNil(t, target)
	assertOneISCSIGroupPerPortal(t, target.Groups)
}
