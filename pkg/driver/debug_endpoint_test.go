package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// debugSecretSentinel is planted in every secret-capable config field a test
// driver carries; TestDebugStateNeverLeaksSecrets fails if it ever appears in
// a /debug/state response body.
const debugSecretSentinel = "SECRET-SENTINEL"

// newDebugTestDriver builds a Driver the way health_test.go does — direct
// struct literal, mock TrueNAS client — with a config that deliberately
// carries secret sentinels so leak tests exercise the real allowlist.
func newDebugTestDriver() *Driver {
	d := &Driver{
		name:          "csi.scale.io",
		version:       "test-version",
		nodeID:        "node-1",
		runController: true,
		runNode:       true,
		truenasClient: truenas.NewMockClient(),
		config: &Config{
			DriverName:       "csi.scale.io",
			DriverInstanceID: "csi.scale.io@tank/csi",
			TrueNAS: TrueNASConfig{
				Host:     "truenas.example.test",
				Port:     443,
				Protocol: "https",
				// The API key is THE secret the state dump must never leak.
				APIKey: debugSecretSentinel,
				// Not a secret (public cert material), but planted to prove the
				// response is an allowlist rather than "config minus apiKey".
				CACert: "CACERT-" + debugSecretSentinel,
			},
			ZFS: ZFSConfig{DatasetParentName: "tank/csi"},
			NFS: NFSConfig{Enabled: true, ShareHost: "192.0.2.10"},
		},
	}
	d.ready.Store(true)
	return d
}

// fetchDebugState runs the /debug/state handler through httptest and returns
// the decoded state plus the raw body for leak assertions.
func fetchDebugState(t *testing.T, server *DebugServer) (DebugState, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.handleState(recorder, httptest.NewRequest(http.MethodGet, "/debug/state", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	var state DebugState
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &state))
	return state, recorder.Body.String()
}

func TestDebugServerDisabledWhenListenAddressEmpty(t *testing.T) {
	d := newDebugTestDriver()
	// Empty and whitespace-only both mean "no listener at all": the nil return
	// is the entire enablement contract Driver.Run relies on.
	assert.Nil(t, NewDebugServer(d, ""))
	assert.Nil(t, NewDebugServer(d, "   "))
	assert.NotNil(t, NewDebugServer(d, "127.0.0.1:0"))
}

func TestDebugStateReportsHeldOperationLocks(t *testing.T) {
	d := newDebugTestDriver()
	require.True(t, d.acquireOperationLock("volume:pvc-wedged"))
	require.True(t, d.acquireOperationLock("snapshot:snap-wedged"))
	t.Cleanup(func() {
		d.releaseOperationLock("volume:pvc-wedged")
		d.releaseOperationLock("snapshot:snap-wedged")
	})

	server := NewDebugServer(d, "127.0.0.1:0")
	require.NotNil(t, server)

	state, _ := fetchDebugState(t, server)
	// Keys are sorted for a diff-stable dump.
	assert.Equal(t, []string{"snapshot:snap-wedged", "volume:pvc-wedged"}, state.OperationLocks)

	// A released lock must disappear from the next fetch.
	d.releaseOperationLock("snapshot:snap-wedged")
	state, _ = fetchDebugState(t, server)
	assert.Equal(t, []string{"volume:pvc-wedged"}, state.OperationLocks)
}

func TestDebugStateShape(t *testing.T) {
	d := newDebugTestDriver()
	d.storeStageRecord(nodeMountRecord{
		VolumeID:   "scale-nfs:tank/csi/pvc-staged",
		TargetPath: "/var/lib/kubelet/staging/pvc-staged",
	})
	d.storePublicationRecord(nodeMountRecord{
		VolumeID:   "scale-nfs:tank/csi/pvc-published",
		TargetPath: "/var/lib/kubelet/pods/x/volumes/pvc-published/mount",
		Readonly:   true,
	})

	server := NewDebugServer(d, "127.0.0.1:0")
	require.NotNil(t, server)
	state, _ := fetchDebugState(t, server)

	assert.Equal(t, "csi.scale.io", state.Driver.Name)
	assert.Equal(t, "test-version", state.Driver.Version)
	assert.Equal(t, "node-1", state.Driver.NodeID)
	assert.True(t, state.Driver.ControllerRunning)
	assert.True(t, state.Driver.NodeRunning)
	assert.True(t, state.Driver.Ready)
	assert.NotEmpty(t, state.Driver.StartedAt)
	assert.GreaterOrEqual(t, state.Driver.UptimeSeconds, float64(0))

	// Idle driver: empty ARRAY (not null) so jq pipelines keep working.
	assert.NotNil(t, state.OperationLocks)
	assert.Empty(t, state.OperationLocks)

	// The mock client reports connected with no circuit breaker configured.
	assert.True(t, state.TrueNAS.ClientConfigured)
	assert.True(t, state.TrueNAS.Connected)
	assert.Nil(t, state.TrueNAS.CircuitBreaker)

	assert.Equal(t, "csi.scale.io", state.Config.DriverName)
	assert.Equal(t, "csi.scale.io@tank/csi", state.Config.DriverInstanceID)
	assert.Equal(t, "truenas.example.test", state.Config.TrueNASHost)
	assert.Equal(t, 443, state.Config.TrueNASPort)
	assert.Equal(t, "https", state.Config.TrueNASProtocol)
	assert.Equal(t, "tank/csi", state.Config.DatasetParentName)
	assert.Equal(t, []string{"nfs"}, state.Config.EnabledProtocols)

	require.Equal(t, 1, state.NodeMounts.StagedCount)
	require.Equal(t, 1, state.NodeMounts.PublishedCount)
	assert.Equal(t, "scale-nfs:tank/csi/pvc-staged", state.NodeMounts.Staged[0].VolumeID)
	assert.Equal(t, "scale-nfs:tank/csi/pvc-published", state.NodeMounts.Published[0].VolumeID)
	assert.True(t, state.NodeMounts.Published[0].Readonly)
}

// TestDebugStateNeverLeaksSecrets is the security regression test for the
// DebugState allowlist: a config carrying a sentinel API key must never see
// that sentinel in the marshaled response, and the CACert sentinel proves the
// response is a field allowlist rather than a redacted whole-config marshal.
func TestDebugStateNeverLeaksSecrets(t *testing.T) {
	d := newDebugTestDriver()
	require.Equal(t, debugSecretSentinel, d.config.TrueNAS.APIKey, "test must plant the sentinel it asserts on")

	server := NewDebugServer(d, "127.0.0.1:0")
	require.NotNil(t, server)
	_, body := fetchDebugState(t, server)

	assert.NotContains(t, body, debugSecretSentinel,
		"/debug/state leaked a secret-capable config field; DebugState must remain an explicit non-secret allowlist")
	assert.NotContains(t, body, "apiKey")
	assert.NotContains(t, body, "api_key")
	// Allowlisted, non-secret fields must still be present — the endpoint has
	// to stay useful, not merely silent.
	assert.Contains(t, body, "truenas.example.test")
}

func TestDebugStateHandlerRejectsNonGET(t *testing.T) {
	d := newDebugTestDriver()
	server := NewDebugServer(d, "127.0.0.1:0")
	require.NotNil(t, server)

	recorder := httptest.NewRecorder()
	server.handleState(recorder, httptest.NewRequest(http.MethodPost, "/debug/state", strings.NewReader("{}")))
	assert.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
}

// TestDebugServerServesPprofAndStateOverHTTP exercises the real listener: a
// dedicated mux on a loopback ephemeral port serving both the pprof index and
// /debug/state, then a graceful Stop.
func TestDebugServerServesPprofAndStateOverHTTP(t *testing.T) {
	d := newDebugTestDriver()
	server := NewDebugServer(d, "127.0.0.1:0")
	require.NotNil(t, server)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, server.Stop(ctx))
	})
	require.NotEmpty(t, server.Addr(), "Addr must expose the resolved port for a :0 bind")

	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/state"} {
		resp, err := http.Get(fmt.Sprintf("http://%s%s", server.Addr(), path))
		require.NoError(t, err, path)
		body, readErr := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, readErr, path)
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "GET %s body: %s", path, body)
	}

	// Anything outside the two debug surfaces must 404: the dedicated mux
	// carries no metrics, health, or default-mux handlers.
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", server.Addr()))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// Mirrors TestHealthServerStartReturnsBindFailureSynchronously: an occupied
// port must fail Start (and therefore driver startup) synchronously.
func TestDebugServerStartReturnsBindFailureSynchronously(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, occupied.Close()) })

	server := NewDebugServer(newDebugTestDriver(), occupied.Addr().String())
	require.NotNil(t, server)
	err = server.Start()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind debug listener")
}

func TestLoadConfigDebugListenAddressDefaultsToDisabled(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
`)
	require.NoError(t, err)
	assert.Empty(t, cfg.Debug.ListenAddress, "debug endpoint must default to disabled")
}

func TestLoadConfigDebugListenAddressAcceptsLoopback(t *testing.T) {
	cfg, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
debug:
  listenAddress: 127.0.0.1:6060
`)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:6060", cfg.Debug.ListenAddress)
}

func TestLoadConfigDebugListenAddressRejectsInvalid(t *testing.T) {
	for _, listenAddress := range []string{"not an address", "127.0.0.1"} {
		_, err := loadTestConfig(t, requiredTestConfig+fmt.Sprintf(`
nfs:
  enabled: true
  shareHost: 192.0.2.10
debug:
  listenAddress: %q
`, listenAddress))
		require.Errorf(t, err, "listenAddress %q must be rejected", listenAddress)
		assert.Contains(t, err.Error(), "debug.listenAddress")
	}
}

func TestLoadConfigDebugListenAddressWarnsOnNonLoopback(t *testing.T) {
	originalWarningf := configWarningf
	t.Cleanup(func() { configWarningf = originalWarningf })
	var warnings []string
	configWarningf = func(format string, args ...interface{}) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	_, err := loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
debug:
  listenAddress: 0.0.0.0:6060
`)
	require.NoError(t, err, "a non-loopback bind is allowed (guarded-port deployment) but must warn")
	require.NotEmpty(t, warnings)
	assert.Contains(t, strings.Join(warnings, "\n"), "debug.listenAddress")

	// Loopback binds must stay quiet.
	warnings = nil
	_, err = loadTestConfig(t, requiredTestConfig+`
nfs:
  enabled: true
  shareHost: 192.0.2.10
debug:
  listenAddress: localhost:6060
`)
	require.NoError(t, err)
	for _, warning := range warnings {
		assert.NotContains(t, warning, "debug.listenAddress")
	}
}
