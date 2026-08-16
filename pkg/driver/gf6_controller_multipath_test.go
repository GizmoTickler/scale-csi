package driver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

type iscsiTargetUpdateFailMock struct {
	*truenas.MockClient
	updateErr error
}

func (m *iscsiTargetUpdateFailMock) ISCSITargetUpdate(ctx context.Context, id int, groups []truenas.ISCSITargetGroup) (*truenas.ISCSITarget, error) {
	if m.updateErr != nil {
		return nil, m.updateErr
	}
	return m.MockClient.ISCSITargetUpdate(ctx, id, groups)
}

func newGF6ControllerDriver(t *testing.T, client truenas.ClientInterface, shareType ShareType) *Driver {
	t.Helper()
	d := &Driver{
		name: fencingDriverName(shareType),
		config: &Config{
			DriverName: fencingDriverName(shareType),
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			NFS: NFSConfig{Enabled: true, ShareHost: "192.0.2.20"},
			ISCSI: ISCSIConfig{
				Enabled:           true,
				TargetPortal:      "192.0.2.10:3260",
				DeviceWaitTimeout: 1,
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)
	return d
}

func addGF6ISCSIPortals(client *truenas.MockClient) {
	client.ISCSIPortals[2] = &truenas.ISCSIPortal{
		ID: 2, Tag: 2, Listen: []truenas.ISCSIPortalListen{{IP: "192.0.2.11", Port: 3260}},
	}
	client.ISCSIPortals[3] = &truenas.ISCSIPortal{
		ID: 3, Tag: 3, Listen: []truenas.ISCSIPortalListen{{IP: "2001:db8::12", Port: 3260}},
	}
}

func gf6ISCSINodeID(t *testing.T) string {
	t.Helper()
	nodeID, err := encodeNodeIdentity(NodeIdentity{
		Name: "gf6-worker", ISCSIIQN: "iqn.1993-08.org.debian:gf6-worker",
	})
	require.NoError(t, err)
	return nodeID
}

func gf6NFSNodeID(t *testing.T) string {
	t.Helper()
	nodeID, err := encodeNodeIdentity(NodeIdentity{
		Name: "gf6-worker", IPs: []net.IP{net.ParseIP("192.0.2.80")},
	})
	require.NoError(t, err)
	return nodeID
}

func TestISCSIMultipathCreateAndExistingPublishConvergeEveryPortal(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	addGF6ISCSIPortals(client)
	d := newGF6ControllerDriver(t, client, ShareTypeISCSI)

	// Provision in the pre-GF6 shape, then enable multipath. Publish must repair
	// this already-existing target before it returns the attach hint.
	createResponse, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-existing-iscsi", "iscsi"))
	require.NoError(t, err)
	assert.NotContains(t, createResponse.GetVolume().GetVolumeContext(), "portals")
	d.config.ISCSI.Multipath = true
	d.config.ISCSI.Portals = []string{"192.0.2.11", "2001:db8::12"}

	publishResponse, err := d.ControllerPublishVolume(ctx, iscsiPublishRequest("gf6-existing-iscsi", gf6ISCSINodeID(t)))
	require.NoError(t, err)
	assert.Equal(t,
		`["192.0.2.10:3260","192.0.2.11:3260","[2001:db8::12]:3260"]`,
		publishResponse.GetPublishContext()["portals"],
	)

	dataset, err := client.DatasetGet(ctx, "pool/parent/gf6-existing-iscsi")
	require.NoError(t, err)
	targetID, err := strconv.Atoi(datasetUserProperty(dataset, PropISCSITargetID))
	require.NoError(t, err)
	target, err := client.ISCSITargetGet(ctx, targetID)
	require.NoError(t, err)
	require.Len(t, target.Groups, 3)
	for _, portalID := range []int{1, 2, 3} {
		assert.Contains(t, target.Groups, truenas.ISCSITargetGroup{
			Portal: portalID, Initiator: target.Groups[0].Initiator, AuthMethod: "NONE",
		})
	}
}

func TestISCSIPublishAssociationFailureNeverAdvertisesPortals(t *testing.T) {
	ctx := context.Background()
	client := &iscsiTargetUpdateFailMock{MockClient: truenas.NewMockClient()}
	addGF6ISCSIPortals(client.MockClient)
	d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-association-failure", "iscsi"))
	require.NoError(t, err)
	d.config.ISCSI.Multipath = true
	d.config.ISCSI.Portals = []string{"192.0.2.11"}
	client.updateErr = fmt.Errorf("injected portal association failure")

	response, err := d.ControllerPublishVolume(ctx, iscsiPublishRequest("gf6-association-failure", gf6ISCSINodeID(t)))
	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), "failed to converge iSCSI multipath portal associations")
}

func TestISCSIPublishReloadFailureNeverAdvertisesPortals(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	addGF6ISCSIPortals(client)
	d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-reload-failure", "iscsi"))
	require.NoError(t, err)
	d.config.ISCSI.Multipath = true
	d.config.ISCSI.Portals = []string{"192.0.2.11"}
	d.serviceReloadDebouncer.Stop()
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(context.Context, string) error {
		return fmt.Errorf("injected iSCSI service reload failure")
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)

	response, err := d.ControllerPublishVolume(ctx, iscsiPublishRequest("gf6-reload-failure", gf6ISCSINodeID(t)))
	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), "failed to reload iSCSI service after multipath portal convergence")
}

func TestISCSIFencingIsIdenticalAcrossEveryPortal(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	addGF6ISCSIPortals(client)
	d := newGF6ControllerDriver(t, client, ShareTypeISCSI)
	d.config.Fencing.Mode = FencingModeStrict
	d.config.ISCSI.Multipath = true
	d.config.ISCSI.Portals = []string{"192.0.2.11"}
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-fenced-iscsi", "iscsi"))
	require.NoError(t, err)
	nodeID := gf6ISCSINodeID(t)
	_, err = d.ControllerPublishVolume(ctx, iscsiPublishRequest("gf6-fenced-iscsi", nodeID))
	require.NoError(t, err)

	dataset, err := client.DatasetGet(ctx, "pool/parent/gf6-fenced-iscsi")
	require.NoError(t, err)
	targetID := mustAtoi(t, datasetUserProperty(dataset, PropISCSITargetID))
	initiatorID := mustAtoi(t, datasetUserProperty(dataset, PropISCSIInitiatorID))
	target, err := client.ISCSITargetGet(ctx, targetID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []truenas.ISCSITargetGroup{
		{Portal: 1, Initiator: initiatorID, AuthMethod: "NONE"},
		{Portal: 2, Initiator: initiatorID, AuthMethod: "NONE"},
	}, target.Groups, "a portal missing the dynamic group would bypass SCST fencing")

	_, err = d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "gf6-fenced-iscsi", NodeId: nodeID,
	})
	require.NoError(t, err)
	target, err = client.ISCSITargetGet(ctx, targetID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []truenas.ISCSITargetGroup{
		{Portal: 1, Initiator: initiatorID, AuthMethod: "NONE"},
		{Portal: 2, Initiator: initiatorID, AuthMethod: "NONE"},
	}, target.Groups)
	initiator, err := client.ISCSIInitiatorGet(ctx, initiatorID)
	require.NoError(t, err)
	assert.Equal(t, iscsiDenyAllInitiators(), initiator.Initiators,
		"last unpublish must retain the same deny-all sentinel behind every portal")
}

func TestNFSTrunkingPublishContextSupportsPreExistingVolumes(t *testing.T) {
	ctx := context.Background()
	client := truenas.NewMockClient()
	d := newGF6ControllerDriver(t, client, ShareTypeNFS)
	createResponse, err := d.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-existing-nfs", "nfs"))
	require.NoError(t, err)
	assert.NotContains(t, createResponse.GetVolume().GetVolumeContext(), "addresses")

	d.config.NFS.Trunking = true
	d.config.NFS.Addresses = []string{"192.0.2.21", "2001:db8::22"}
	publishResponse, err := d.ControllerPublishVolume(ctx, nfsPublishRequest("gf6-existing-nfs", gf6NFSNodeID(t)))
	require.NoError(t, err)
	assert.Equal(t,
		`["192.0.2.20","192.0.2.21","2001:db8::22"]`,
		publishResponse.GetPublishContext()["addresses"],
	)
}

func TestGF6CreateTimeHintsAndDisabledLegacyContexts(t *testing.T) {
	ctx := context.Background()

	iscsiClient := truenas.NewMockClient()
	addGF6ISCSIPortals(iscsiClient)
	iscsiDriver := newGF6ControllerDriver(t, iscsiClient, ShareTypeISCSI)
	iscsiDriver.config.ISCSI.Multipath = true
	iscsiDriver.config.ISCSI.Portals = []string{"192.0.2.11"}
	iscsiResponse, err := iscsiDriver.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-new-iscsi", "iscsi"))
	require.NoError(t, err)
	assert.Equal(t, `["192.0.2.10:3260","192.0.2.11:3260"]`, iscsiResponse.GetVolume().GetVolumeContext()["portals"])

	nfsDriver := newGF6ControllerDriver(t, truenas.NewMockClient(), ShareTypeNFS)
	nfsDriver.config.NFS.Trunking = true
	nfsDriver.config.NFS.Addresses = []string{"192.0.2.21"}
	nfsResponse, err := nfsDriver.CreateVolume(ctx, apiCallCountVolumeRequest("gf6-new-nfs", "nfs"))
	require.NoError(t, err)
	assert.Equal(t, `["192.0.2.20","192.0.2.21"]`, nfsResponse.GetVolume().GetVolumeContext()["addresses"])
}
