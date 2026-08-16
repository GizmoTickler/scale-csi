package driver

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
	"github.com/GizmoTickler/scale-csi/pkg/util"
)

func TestControllerPublishVolumeNVMeoFMultipathAddresses(t *testing.T) {
	const wantAddresses = `["192.0.2.20","192.0.2.21","192.0.2.22"]`
	tests := []struct {
		name                 string
		volumeID             string
		addresses            []string
		removeShare          bool
		wantAddresses        string
		wantAssociationCalls int
		wantSubsystemCreates int
	}{
		{
			name:                 "fresh share",
			volumeID:             "gf51-fresh-share",
			addresses:            []string{"192.0.2.21", "192.0.2.22"},
			removeShare:          true,
			wantAddresses:        wantAddresses,
			wantAssociationCalls: 3,
			wantSubsystemCreates: 1,
		},
		{
			name:                 "already-existing share",
			volumeID:             "gf51-existing-share",
			addresses:            []string{"192.0.2.21", "192.0.2.22"},
			wantAddresses:        wantAddresses,
			wantAssociationCalls: 3,
		},
		{
			name:     "multipath disabled",
			volumeID: "gf51-multipath-off",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			client := newAPICallCountingClient()
			d := newMultipathAPICallCountDriver(t, client, test.addresses)
			_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest(test.volumeID, "nvmeof"))
			require.NoError(t, err)

			if test.removeShare {
				require.NoError(t, d.deleteNVMeoFShareForDataset(ctx, nil, "pool/parent/"+test.volumeID))
			}
			nodeID, err := encodeNodeIdentity(NodeIdentity{
				Name: "gf51-worker", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:gf51-worker",
			})
			require.NoError(t, err)
			client.resetCalls()

			response, err := d.ControllerPublishVolume(ctx, nvmeoFPublishRequest(test.volumeID, nodeID))
			require.NoError(t, err)
			_, calls := client.callSnapshot()
			assert.Equal(t, test.wantAssociationCalls, calls["NVMeoFPortSubsysCreate"],
				"publish must associate every address before advertising it")
			assert.Equal(t, test.wantSubsystemCreates, calls["NVMeoFSubsystemCreate"])

			if test.wantAddresses == "" {
				assert.NotContains(t, response.GetPublishContext(), "addresses")
				return
			}
			assert.Equal(t, test.wantAddresses, response.GetPublishContext()["addresses"],
				"publish and create must use the same deterministic addresses JSON")
		})
	}
}

// Regression for the already-exists publish path: the controller must never
// advertise an address set when convergence of that subsystem's port
// associations failed. Advertising first would send the node to paths the
// backend has not exposed.
func TestControllerPublishVolumeNVMeoFExistingAssociationFailureDoesNotAdvertise(t *testing.T) {
	ctx := context.Background()
	client := &nvmePortAssociationFailMock{MockClient: truenas.NewMockClient()}
	d := &Driver{
		name: "org.scale.csi.test",
		config: &Config{
			DriverName: "org.scale.csi.nvmeof",
			ZFS: ZFSConfig{
				DatasetParentName:   "pool/parent",
				DatasetEnableQuotas: true,
				ZvolReadyTimeout:    1,
			},
			NVMeoF: NVMeoFConfig{
				Enabled:               true,
				Transport:             "TCP",
				TransportAddress:      "192.0.2.20",
				TransportServiceID:    4420,
				SubsystemAllowAnyHost: true,
			},
		},
		truenasClient: client,
	}
	d.serviceReloadDebouncer = NewServiceReloadDebouncer(0, func(ctx context.Context, service string) error {
		return client.ServiceReload(ctx, service)
	})
	t.Cleanup(d.serviceReloadDebouncer.Stop)

	const volumeID = "gf51-existing-association-failure"
	_, err := d.CreateVolume(ctx, apiCallCountVolumeRequest(volumeID, "nvmeof"))
	require.NoError(t, err)

	// Flip multipath on after provisioning to exercise publish-time convergence
	// for an already-existing subsystem, then make that convergence fail.
	d.config.NVMeoF.Multipath = true
	d.config.NVMeoF.Addresses = []string{"192.0.2.21"}
	client.portSubsysCreateErr = fmt.Errorf("injected association failure")
	nodeID, err := encodeNodeIdentity(NodeIdentity{
		Name: "gf51-worker", NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:gf51-worker",
	})
	require.NoError(t, err)

	response, err := d.ControllerPublishVolume(ctx, nvmeoFPublishRequest(volumeID, nodeID))
	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), "failed to converge NVMe-oF multipath port associations")
}

type gf51NodeStageCalls struct {
	legacyURIs    []string
	multipathURIs []string
}

func runGF51NodeStage(t *testing.T, primaryAddress string, volumeContext, publishContext map[string]string) gf51NodeStageCalls {
	t.Helper()
	installFakeNodeCommands(t, "findmnt")
	originalInfo := nodeGetNVMeInfo
	originalList := nodeListNVMeSubsystems
	originalLegacyConnect := nvmeConnectWithSubsystems
	originalMultipathConnect := nvmeConnectPathWithSubsystems
	originalPolicy := nodeSetNVMeIOPolicy
	t.Cleanup(func() {
		nodeGetNVMeInfo = originalInfo
		nodeListNVMeSubsystems = originalList
		nvmeConnectWithSubsystems = originalLegacyConnect
		nvmeConnectPathWithSubsystems = originalMultipathConnect
		nodeSetNVMeIOPolicy = originalPolicy
	})

	const nqn = "nqn.2014-08.org.nvmexpress:uuid:gf51-stage"
	nodeGetNVMeInfo = func(string) (string, error) { return nqn, nil }
	nodeListNVMeSubsystems = func(context.Context) ([]util.NVMeSubsystem, error) { return nil, nil }
	calls := gf51NodeStageCalls{}
	nvmeConnectWithSubsystems = func(_ context.Context, gotNQN, uri string, _ *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		assert.Equal(t, nqn, gotNQN)
		calls.legacyURIs = append(calls.legacyURIs, uri)
		return "/dev/null", nil
	}
	nvmeConnectPathWithSubsystems = func(_ context.Context, gotNQN, uri string, _ *util.NVMeoFConnectOptions, _ []util.NVMeSubsystem) (string, error) {
		assert.Equal(t, nqn, gotNQN)
		calls.multipathURIs = append(calls.multipathURIs, uri)
		return "/dev/null", nil
	}
	nodeSetNVMeIOPolicy = func(string, string) error { return nil }

	requestVolumeContext := map[string]string{
		"node_attach_driver": "nvmeof",
		"nqn":                nqn,
		"transport":          "tcp",
		"address":            primaryAddress,
		"port":               "4420",
	}
	for key, value := range volumeContext {
		requestVolumeContext[key] = value
	}
	_, err := newTestNodeDriver(ShareTypeNVMeoF).NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "gf51-stage-volume",
		StagingTargetPath: filepath.Join(t.TempDir(), "stage"),
		VolumeContext:     requestVolumeContext,
		PublishContext:    publishContext,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
	})
	require.NoError(t, err)
	return calls
}

// This is the on-cluster GF-5.1 production reproduction: the immutable PV was
// provisioned before multipath and therefore has no addresses key, while a new
// ControllerPublishVolume supplies the associated paths in PublishContext.
func TestNodeStageVolumePreExistingPVUsesPublishContextAddresses(t *testing.T) {
	calls := runGF51NodeStage(t, "192.0.2.20", nil, map[string]string{
		"addresses": `["192.0.2.20","192.0.2.21"]`,
	})
	assert.Empty(t, calls.legacyURIs)
	assert.Equal(t, []string{
		"tcp://192.0.2.20:4420",
		"tcp://192.0.2.21:4420",
	}, calls.multipathURIs)
}

func TestNodeStageVolumePublishContextAddressesWin(t *testing.T) {
	calls := runGF51NodeStage(t, "192.0.2.30", map[string]string{
		"addresses": `["192.0.2.30","192.0.2.31"]`,
	}, map[string]string{
		"addresses": `["192.0.2.40","192.0.2.41"]`,
	})
	assert.Empty(t, calls.legacyURIs)
	assert.Equal(t, []string{
		"tcp://192.0.2.40:4420",
		"tcp://192.0.2.41:4420",
	}, calls.multipathURIs)
}

func TestNodeStageVolumeFallsBackToVolumeContextAddresses(t *testing.T) {
	calls := runGF51NodeStage(t, "192.0.2.50", map[string]string{
		"addresses": `["192.0.2.50","192.0.2.51"]`,
	}, nil)
	assert.Empty(t, calls.legacyURIs)
	assert.Equal(t, []string{
		"tcp://192.0.2.50:4420",
		"tcp://192.0.2.51:4420",
	}, calls.multipathURIs)
}

func TestNodeStageVolumeWithoutEitherAddressesHintUsesLegacySinglePath(t *testing.T) {
	calls := runGF51NodeStage(t, "192.0.2.60", nil, nil)
	assert.Equal(t, []string{"tcp://192.0.2.60:4420"}, calls.legacyURIs)
	assert.Empty(t, calls.multipathURIs,
		"the no-hint request must stay on the byte-identical legacy connect entry point")
}

func TestMalformedPublishContextWinsAndRemainsObservable(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(1)
	d := newTestNodeDriver(ShareTypeNVMeoF)
	d.eventRecorder = &EventRecorder{recorder: fakeRecorder, enabled: true}
	selected := nvmeoFStageContext(
		map[string]string{"addresses": `["192.0.2.70","192.0.2.71"]`},
		map[string]string{"addresses": "not-json"},
	)
	errorCounter := nvmePathConnectTotal.WithLabelValues(invalidNVMeMultipathAddressMetricLabel, "error")
	errorBefore := testutil.ToFloat64(errorCounter)

	addresses := d.nodeNVMeMultipathAddresses(selected, "nqn.2014-08.org.nvmexpress:uuid:gf51-malformed", PVRef("gf51-malformed"))
	assert.Nil(t, addresses)
	assert.Equal(t, errorBefore+1, testutil.ToFloat64(errorCounter))
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning "+EventReasonNVMePathDegraded)
		assert.Contains(t, event, "not-json")
		assert.Contains(t, event, "single-address fallback")
	default:
		t.Fatal("the malformed winning publish hint must retain the observable fallback")
	}
}
