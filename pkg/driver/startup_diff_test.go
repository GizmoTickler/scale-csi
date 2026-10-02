package driver

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// diffFixture is a strict NVMe-oF (multipath) controller over n attached
// volumes diff-0..n-1, spread over three nodes, provisioned the way
// CreateVolume leaves them, then converged by one full startup pass.
type diffFixture struct {
	d      *Driver
	client *diffCountingClient
	kube   *kubernetesfake.Clientset
	mu     sync.Mutex
	vaGets map[string]int
}

// diffCountingClient counts calls and, when hook is set, runs it inside
// NVMeoFHostSubsysList (the diff's last fleet read).
type diffCountingClient struct {
	*apiCallCountingClient
	hook               func()
	failHostSubsysList bool
}

func (c *diffCountingClient) NVMeoFHostSubsysList(ctx context.Context) ([]*truenas.NVMeoFHostSubsys, error) {
	if c.hook != nil {
		c.hook()
	}
	if c.failHostSubsysList {
		return nil, fmt.Errorf("injected host_subsys listing failure")
	}
	return c.apiCallCountingClient.NVMeoFHostSubsysList(ctx)
}

var diffNodes = []string{"k8s-0", "k8s-1", "k8s-2"}

func diffNodeIdentity(node string) NodeIdentity {
	index := map[string]int{"k8s-0": 0, "k8s-1": 1, "k8s-2": 2, "k8s-3": 3}[node]
	return NodeIdentity{Name: node, NVMeNQN: "nqn.2014-08.org.nvmexpress:uuid:" + node,
		IPs: []net.IP{net.ParseIP(fmt.Sprintf("192.0.2.%d", 10+index))}}
}

func diffNodeObjects(t *testing.T, node string) []runtime.Object {
	t.Helper()
	identity := diffNodeIdentity(node)
	id, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	return []runtime.Object{
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: node}, Spec: storagev1.CSINodeSpec{
			Drivers: []storagev1.CSINodeDriver{{Name: "csi.scale.io", NodeID: id}}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node}, Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: identity.IPs[0].String()}}}},
	}
}

func newDiffFixture(t *testing.T, n int) *diffFixture {
	t.Helper()
	ctx := context.Background()
	client := &diffCountingClient{apiCallCountingClient: newAPICallCountingClient()}
	d := &Driver{
		name: "csi.scale.io",
		config: &Config{
			DriverName: "csi.scale.io",
			Fencing:    FencingConfig{Mode: FencingModeStrict, StartupReconcileTimeout: "5s"},
			ZFS:        ZFSConfig{DatasetParentName: "pool/parent", ZvolReadyTimeout: 1},
			NVMeoF: NVMeoFConfig{Enabled: true, Transport: "TCP", TransportAddress: "192.0.2.20", TransportServiceID: 4420,
				Multipath: true, Addresses: []string{"192.0.2.21"}},
		},
		truenasClient:     client,
		nvmeResolvedHosts: make(map[string]int),
	}
	var objects []runtime.Object
	for _, node := range diffNodes {
		objects = append(objects, diffNodeObjects(t, node)...)
	}
	for i := 0; i < n; i++ {
		volumeID := fmt.Sprintf("diff-%d", i)
		datasetName := "pool/parent/" + volumeID
		ds, err := client.MockClient.DatasetCreate(ctx, &truenas.DatasetCreateParams{Name: datasetName, Type: "VOLUME", Volsize: testGiB})
		require.NoError(t, err)
		require.NoError(t, client.MockClient.DatasetSetUserProperties(ctx, datasetName, map[string]string{
			PropManagedResource: "true", PropDriverInstanceID: d.driverInstanceID(),
		}))
		require.NoError(t, d.createNVMeoFShareForDataset(ctx, ds, datasetName, volumeID, true, true, nil))
		pvName := "pv-" + volumeID
		objects = append(objects,
			&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName}, Spec: corev1.PersistentVolumeSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "csi.scale.io", VolumeHandle: volumeID, VolumeAttributes: map[string]string{"node_attach_driver": "nvmeof"}}}}},
			&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "va-" + volumeID}, Spec: storagev1.VolumeAttachmentSpec{
				Attacher: "csi.scale.io", NodeName: diffNodes[i%len(diffNodes)], Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName}},
				Status: storagev1.VolumeAttachmentStatus{Attached: true}},
		)
	}
	kube := kubernetesfake.NewSimpleClientset(objects...)
	f := &diffFixture{d: d, client: client, kube: kube, vaGets: map[string]int{}}
	kube.PrependReactor("get", "volumeattachments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		f.mu.Lock()
		f.vaGets[action.(clienttesting.GetAction).GetName()]++
		f.mu.Unlock()
		return false, nil, nil
	})
	d.eventRecorder = &EventRecorder{recorder: record.NewFakeRecorder(256), clientset: kube, enabled: true}
	require.NoError(t, d.reconcilePublishedAttachments(ctx), "the first pass converges every volume")
	return f
}

// perVolume returns the volumes the last pass converged on their own: the
// per-volume path re-reads each snapshot VolumeAttachment by name.
func (f *diffFixture) perVolume() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for name := range f.vaGets {
		out = append(out, name[len("va-"):])
	}
	sort.Strings(out)
	f.vaGets = map[string]int{}
	return out
}

func (f *diffFixture) subsystemOf(t *testing.T, volumeID string) *truenas.NVMeoFSubsystem {
	t.Helper()
	subsystem, err := f.client.MockClient.NVMeoFSubsystemFindByName(context.Background(), f.d.nvmeSubsystemName("pool/parent/"+volumeID))
	require.NoError(t, err)
	require.NotNil(t, subsystem)
	return subsystem
}

func (f *diffFixture) allowedNQNs(t *testing.T, volumeID string) []string {
	t.Helper()
	associations, err := f.client.MockClient.NVMeoFHostSubsysListBySubsystem(context.Background(), f.subsystemOf(t, volumeID).ID)
	require.NoError(t, err)
	var nqns []string
	for _, association := range associations {
		nqns = append(nqns, association.HostNQN)
	}
	sort.Strings(nqns)
	return nqns
}

// A restart with every record and fence in place reads the fleet once and
// converges no volume on its own: no volume lock, no per-volume call.
func TestStartupDiffLeavesConvergedVolumesAlone(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 6)
	f.perVolume()
	f.client.resetCalls()

	require.NoError(t, f.d.reconcilePublishedAttachments(ctx))
	assert.Empty(t, f.perVolume(), "every volume is already converged")
	_, calls := f.client.callSnapshot()
	// The datasets are read by name in one request at this size; the nvmet
	// tables once each; the two configured ports resolved once each.
	assert.Equal(t, map[string]int{
		"DatasetGetByNames": 1, "NVMeoFSubsystemList": 1, "NVMeoFNamespaceList": 1,
		"NVMeoFPortSubsysList": 1, "NVMeoFHostSubsysList": 1, "NVMeoFGetOrCreatePort": 2,
	}, calls)
	for i := 0; i < 6; i++ {
		volumeID := fmt.Sprintf("diff-%d", i)
		assert.False(t, f.d.startupGateStillPending(volumeID), "a converged volume is not held at publish")
	}
}

// Each kind of divergence sends exactly that volume down the per-volume
// path, which converges it; the others stay untouched.
func TestStartupDiffSendsDivergentVolumesToThePerVolumePath(t *testing.T) {
	ctx := context.Background()
	// blocked: the per-volume path refuses to converge it (a single-node
	// volume whose backend or records name another node), as it always has.
	blocked := map[string]bool{"foreign host allowed": true, "stale extra record": true}
	cases := map[string]func(t *testing.T, f *diffFixture){
		"foreign host allowed": func(t *testing.T, f *diffFixture) {
			host, err := f.client.MockClient.NVMeoFHostCreate(ctx, "nqn.2014-08.org.nvmexpress:uuid:intruder")
			require.NoError(t, err)
			_, err = f.client.MockClient.NVMeoFHostSubsysCreate(ctx, host.ID, f.subsystemOf(t, "diff-1").ID)
			require.NoError(t, err)
		},
		"attached node not allowed": func(t *testing.T, f *diffFixture) {
			associations, err := f.client.MockClient.NVMeoFHostSubsysListBySubsystem(ctx, f.subsystemOf(t, "diff-1").ID)
			require.NoError(t, err)
			require.NoError(t, f.client.MockClient.NVMeoFHostSubsysDelete(ctx, associations[0].ID))
		},
		"subsystem open to any host": func(t *testing.T, f *diffFixture) {
			_, err := f.client.MockClient.NVMeoFSubsystemUpdateAllowAnyHost(ctx, f.subsystemOf(t, "diff-1").ID, true)
			require.NoError(t, err)
		},
		"port link missing": func(t *testing.T, f *diffFixture) {
			links, err := f.client.MockClient.NVMeoFPortSubsysListBySubsystem(ctx, f.subsystemOf(t, "diff-1").ID)
			require.NoError(t, err)
			require.NoError(t, f.client.MockClient.NVMeoFPortSubsysDelete(ctx, links[0].ID))
		},
		"record missing": func(t *testing.T, f *diffFixture) {
			ds, err := f.client.MockClient.DatasetGet(ctx, "pool/parent/diff-1")
			require.NoError(t, err)
			require.NoError(t, f.d.publications().remove(ctx, ds.Name, ds, []string{publicationPropertyKey("k8s-1")}))
		},
		"stale extra record": func(t *testing.T, f *diffFixture) {
			ds, err := f.client.MockClient.DatasetGet(ctx, "pool/parent/diff-1")
			require.NoError(t, err)
			extra, err := newPublicationRecord(diffNodeIdentity("k8s-3"), 1, false)
			require.NoError(t, err)
			extra.AccessMode = 5 // MULTI_NODE_MULTI_WRITER: no conflict, only an extra
			require.NoError(t, f.d.publications().store(ctx, ds.Name, ds, publicationPropertyKey("k8s-3"), extra))
		},
		"stored namespace ID stale": func(t *testing.T, f *diffFixture) {
			require.NoError(t, f.client.MockClient.DatasetSetUserProperty(ctx, "pool/parent/diff-1", PropNVMeoFNamespaceID, "999999"))
		},
		"record under the stale-record sweep": func(t *testing.T, f *diffFixture) {
			f.d.stalePublicationRecordsSeen.Store(stalePublicationObservationKey("pool/parent/diff-1", publicationPropertyKey("k8s-1")), struct{}{})
		},
	}
	for name, diverge := range cases {
		t.Run(name, func(t *testing.T) {
			f := newDiffFixture(t, 3)
			want := []string{"nqn.2014-08.org.nvmexpress:uuid:k8s-1"}
			require.Equal(t, want, f.allowedNQNs(t, "diff-1"))
			diverge(t, f)
			f.perVolume()

			_, err := f.d.reconcilePublishedAttachmentsFor(ctx, nil)
			assert.Equal(t, []string{"diff-1"}, f.perVolume(), "only the divergent volume takes the per-volume path")
			if blocked[name] {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, want, f.allowedNQNs(t, "diff-1"), "the per-volume path converged it")
			assert.False(t, f.subsystemOf(t, "diff-1").AllowAnyHost)
		})
	}
}

// A node that re-registered (new NQN) while the controller was down: its
// stored record no longer matches the snapshot, so the volume is converged
// on its own and the fence follows the node's current identity.
func TestStartupDiffReconvergesAReRegisteredNode(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	csiNode, err := f.kube.StorageV1().CSINodes().Get(ctx, "k8s-1", metav1.GetOptions{})
	require.NoError(t, err)
	identity := diffNodeIdentity("k8s-1")
	identity.NVMeNQN = "nqn.2014-08.org.nvmexpress:uuid:k8s-1-new"
	id, err := encodeNodeIdentity(identity)
	require.NoError(t, err)
	csiNode.Spec.Drivers[0].NodeID = id
	_, err = f.kube.StorageV1().CSINodes().Update(ctx, csiNode, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.perVolume()

	require.NoError(t, f.d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, []string{"diff-1"}, f.perVolume())
	assert.Equal(t, []string{identity.NVMeNQN}, f.allowedNQNs(t, "diff-1"))
}

// A volume whose lock a live operation takes while the diff reads is not
// judged from those reads: it takes the per-volume path.
func TestStartupDiffDoesNotJudgeAVolumeALiveOperationTouched(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	f.perVolume()
	f.client.hook = func() {
		f.client.hook = nil
		require.True(t, f.d.acquireOperationLock(volumeLockKey("diff-2")))
		f.d.releaseOperationLock(volumeLockKey("diff-2"))
	}
	require.NoError(t, f.d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, []string{"diff-2"}, f.perVolume())
}

// A lock already held when the diff starts counts as touched too.
func TestStartupDiffDoesNotJudgeAVolumeLockedAtItsStart(t *testing.T) {
	setStartupTimings(t, 5*time.Second, time.Hour)
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	f.perVolume()
	require.True(t, f.d.acquireOperationLock(volumeLockKey("diff-0")))
	f.client.hook = func() {
		f.client.hook = nil
		f.d.releaseOperationLock(volumeLockKey("diff-0"))
	}
	require.NoError(t, f.d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, []string{"diff-0"}, f.perVolume())
}

// An association the backend did not expand is identified through the host
// table; a foreign one still sends its volume down the per-volume path.
func TestStartupDiffResolvesUnexpandedHostAssociations(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	host, err := f.client.MockClient.NVMeoFHostCreate(ctx, "nqn.2014-08.org.nvmexpress:uuid:intruder")
	require.NoError(t, err)
	_, err = f.client.MockClient.NVMeoFHostSubsysCreate(ctx, host.ID, f.subsystemOf(t, "diff-0").ID)
	require.NoError(t, err)
	f.client.MockClient.EmptyNVMeHostNQN = true
	f.perVolume()
	f.client.resetCalls()

	// The per-volume path refuses a single-node volume whose backend allows
	// another host, as it always has.
	_, err = f.d.reconcilePublishedAttachmentsFor(ctx, nil)
	require.Error(t, err)
	assert.Equal(t, []string{"diff-0"}, f.perVolume())
	_, calls := f.client.callSnapshot()
	assert.Equal(t, 1, calls["NVMeoFHostList"])
}

// A fleet read that fails leaves every volume to the per-volume path.
func TestStartupDiffFallsBackWhenAFleetReadFails(t *testing.T) {
	ctx := context.Background()
	f := newDiffFixture(t, 3)
	f.perVolume()
	f.client.failHostSubsysList = true
	require.NoError(t, f.d.reconcilePublishedAttachments(ctx))
	assert.Equal(t, []string{"diff-0", "diff-1", "diff-2"}, f.perVolume())
}
