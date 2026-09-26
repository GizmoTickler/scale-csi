package util

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortSocketDir returns a directory whose socket paths stay under the 108-byte
// sun_path limit; t.TempDir() embeds the (long) test name.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ublk")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// fakeNVMeUblkDaemon serves the daemon protocol from a temp-dir unix socket.
// handle receives each decoded request and returns the raw response line.
type fakeNVMeUblkDaemon struct {
	socket   string
	mu       sync.Mutex
	requests []map[string]any
}

func startFakeNVMeUblkDaemon(t *testing.T, handle func(req map[string]any) string) *fakeNVMeUblkDaemon {
	t.Helper()
	fake := &fakeNVMeUblkDaemon{socket: filepath.Join(shortSocketDir(t), "d.sock")}
	listener, err := net.Listen("unix", fake.socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				line, readErr := bufio.NewReader(conn).ReadString('\n')
				if readErr != nil {
					return
				}
				var req map[string]any
				if json.Unmarshal([]byte(line), &req) != nil {
					_, _ = conn.Write([]byte(`{"ok":false,"error":"request is not JSON"}` + "\n"))
					return
				}
				fake.mu.Lock()
				fake.requests = append(fake.requests, req)
				fake.mu.Unlock()
				_, _ = conn.Write([]byte(handle(req) + "\n"))
			}(conn)
		}
	}()
	return fake
}

func (f *fakeNVMeUblkDaemon) recorded() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.requests...)
}

func testAttachRequest() NVMeUblkAttachRequest {
	return NVMeUblkAttachRequest{
		Volume:   "pvc-1",
		SubNQN:   "nqn.2011-06.com.example:pvc-1",
		Addrs:    []string{"192.0.2.10:4420", "[2001:db8::10]:4420"},
		HostNQN:  "nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-000000000001",
		HostID:   "00000000-0000-0000-0000-000000000001",
		Queues:   8,
		Depth:    64,
		ZeroCopy: true,
		NapiUs:   0,
	}
}

func TestNVMeUblkClientAttachSendsTheProtocolRequest(t *testing.T) {
	fake := startFakeNVMeUblkDaemon(t, func(map[string]any) string {
		return `{"ok":true,"dev_id":3,"path":"/dev/ublkb3"}`
	})
	device, err := NewNVMeUblkClient(fake.socket).Attach(context.Background(), testAttachRequest())
	require.NoError(t, err)
	assert.Equal(t, NVMeUblkDevice{Volume: "pvc-1", SubNQN: "nqn.2011-06.com.example:pvc-1", DevID: 3, Path: "/dev/ublkb3"}, device)

	requests := fake.recorded()
	require.Len(t, requests, 1)
	assert.Equal(t, map[string]any{
		"op":        "attach",
		"volume":    "pvc-1",
		"subnqn":    "nqn.2011-06.com.example:pvc-1",
		"addrs":     []any{"192.0.2.10:4420", "[2001:db8::10]:4420"},
		"hostnqn":   "nqn.2014-08.org.nvmexpress:uuid:00000000-0000-0000-0000-000000000001",
		"hostid":    "00000000-0000-0000-0000-000000000001",
		"queues":    float64(8),
		"depth":     float64(64),
		"zero_copy": true,
		// napi_us is always sent, so 0 explicitly means busy poll off rather
		// than "daemon default".
		"napi_us": float64(0),
	}, requests[0])
}

func TestNVMeUblkClientAttachReportsExistingDevice(t *testing.T) {
	fake := startFakeNVMeUblkDaemon(t, func(map[string]any) string {
		return `{"ok":true,"dev_id":0,"path":"/dev/ublkb0","existing":true}`
	})
	device, err := NewNVMeUblkClient(fake.socket).Attach(context.Background(), testAttachRequest())
	require.NoError(t, err)
	assert.True(t, device.Existing)
	assert.Equal(t, 0, device.DevID, "dev_id 0 is a real device, not an absent field")
	assert.Equal(t, "/dev/ublkb0", device.Path)
}

func TestNVMeUblkClientAttachValidatesBeforeDialing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NVMeUblkAttachRequest)
		want   string
	}{
		{"no volume", func(r *NVMeUblkAttachRequest) { r.Volume = "" }, "needs a volume"},
		{"no subsystem", func(r *NVMeUblkAttachRequest) { r.SubNQN = "" }, "needs a volume"},
		{"no addresses", func(r *NVMeUblkAttachRequest) { r.Addrs = nil }, "needs a volume"},
		{"host NQN without host ID", func(r *NVMeUblkAttachRequest) { r.HostID = "" }, "both a host NQN and a host ID"},
		{"host ID without host NQN", func(r *NVMeUblkAttachRequest) { r.HostNQN = "" }, "both a host NQN and a host ID"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewNVMeUblkClient("/nonexistent/should-not-dial.sock")
			dialed := false
			client.dial = func(context.Context, string) (net.Conn, error) {
				dialed = true
				return nil, errors.New("unexpected dial")
			}
			req := testAttachRequest()
			tc.mutate(&req)
			_, err := client.Attach(context.Background(), req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.False(t, dialed, "an invalid request must never reach the daemon")
		})
	}
}

func TestNVMeUblkClientResponses(t *testing.T) {
	tests := []struct {
		name     string
		response string
		call     func(*NVMeUblkClient) error
		wantErr  string
	}{
		{
			name:     "daemon error is surfaced",
			response: `{"ok":false,"error":"volume pvc-1 is already attached to a different subsystem (nqn.other)"}`,
			call: func(c *NVMeUblkClient) error {
				_, err := c.Attach(context.Background(), testAttachRequest())
				return err
			},
			wantErr: "nvmeublkd: volume pvc-1 is already attached to a different subsystem",
		},
		{
			name:     "refusal without a reason",
			response: `{"ok":false}`,
			call: func(c *NVMeUblkClient) error {
				_, err := c.List(context.Background())
				return err
			},
			wantErr: "without a reason",
		},
		{
			name:     "malformed JSON",
			response: `not json`,
			call: func(c *NVMeUblkClient) error {
				_, err := c.Detach(context.Background(), "pvc-1")
				return err
			},
			wantErr: "decode response",
		},
		{
			name:     "attach success without a device",
			response: `{"ok":true}`,
			call: func(c *NVMeUblkClient) error {
				_, err := c.Attach(context.Background(), testAttachRequest())
				return err
			},
			wantErr: "carries no device",
		},
		{
			name:     "oversized response",
			response: `{"ok":true,"pad":"` + strings.Repeat("x", nvmeUblkMaxResponseBytes+1) + `"}`,
			call: func(c *NVMeUblkClient) error {
				_, err := c.List(context.Background())
				return err
			},
			wantErr: "response exceeds",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := startFakeNVMeUblkDaemon(t, func(map[string]any) string { return tc.response })
			err := tc.call(NewNVMeUblkClient(fake.socket))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.False(t, errors.Is(err, ErrNVMeUblkDaemonUnavailable), "a daemon that answered is not unavailable")
		})
	}
}

func TestNVMeUblkClientDetachIsIdempotent(t *testing.T) {
	fake := startFakeNVMeUblkDaemon(t, func(req map[string]any) string {
		if req["volume"] == "pvc-gone" {
			return `{"ok":true,"absent":true}`
		}
		return `{"ok":true}`
	})
	client := NewNVMeUblkClient(fake.socket)

	absent, err := client.Detach(context.Background(), "pvc-1")
	require.NoError(t, err)
	assert.False(t, absent)

	absent, err = client.Detach(context.Background(), "pvc-gone")
	require.NoError(t, err)
	assert.True(t, absent)

	assert.Equal(t, []map[string]any{
		{"op": "detach", "volume": "pvc-1"},
		{"op": "detach", "volume": "pvc-gone"},
	}, fake.recorded())

	_, err = client.Detach(context.Background(), "")
	require.Error(t, err)
}

func TestNVMeUblkClientList(t *testing.T) {
	fake := startFakeNVMeUblkDaemon(t, func(map[string]any) string {
		return `{"ok":true,"devices":[{"volume":"pvc-1","subnqn":"nqn.a:pvc-1","dev_id":0,"path":"/dev/ublkb0",` +
			`"paths":[{"addr":"192.0.2.10:4420","up":true},{"addr":"192.0.2.11:4420","up":false}]}]}`
	})
	devices, err := NewNVMeUblkClient(fake.socket).List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []NVMeUblkDevice{{
		Volume: "pvc-1", SubNQN: "nqn.a:pvc-1", DevID: 0, Path: "/dev/ublkb0",
		Paths: []NVMeUblkPath{{Addr: "192.0.2.10:4420", Up: true}, {Addr: "192.0.2.11:4420", Up: false}},
	}}, devices)
	assert.Equal(t, []map[string]any{{"op": "list"}}, fake.recorded())
}

func TestNVMeUblkClientUnavailableSocket(t *testing.T) {
	t.Run("socket file absent", func(t *testing.T) {
		socket := filepath.Join(shortSocketDir(t), "missing.sock")
		_, err := NewNVMeUblkClient(socket).List(context.Background())
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNVMeUblkDaemonUnavailable)
		assert.Contains(t, err.Error(), socket, "the error must name the socket the operator has to check")
	})

	t.Run("stale socket file nobody listens on", func(t *testing.T) {
		socket := filepath.Join(shortSocketDir(t), "stale.sock")
		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)
		unixListener, ok := listener.(*net.UnixListener)
		require.True(t, ok)
		unixListener.SetUnlinkOnClose(false)
		require.NoError(t, listener.Close())

		_, err = NewNVMeUblkClient(socket).Detach(context.Background(), "pvc-1")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNVMeUblkDaemonUnavailable)
	})
}

func TestNVMeUblkClientHonoursContextDeadline(t *testing.T) {
	socket := filepath.Join(shortSocketDir(t), "slow.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		// Accept and never answer: a wedged daemon.
		<-release
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = NewNVMeUblkClient(socket).List(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second, "a wedged daemon must not outlive the caller's deadline")
}

func TestNVMeUblkClientHonoursCancellationWithoutDeadline(t *testing.T) {
	socket := filepath.Join(shortSocketDir(t), "hang.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		<-release
		_ = conn.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	_, err = NewNVMeUblkClient(socket).List(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewNVMeUblkClientDefaultsSocket(t *testing.T) {
	assert.Equal(t, DefaultNVMeUblkSocket, NewNVMeUblkClient("").SocketPath())
	assert.Equal(t, "/tmp/x.sock", NewNVMeUblkClient("/tmp/x.sock").SocketPath())
}

func TestIsNVMeUblkDevice(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/dev/ublkb0", true},
		{"/dev/ublkb17", true},
		{"ublkb3", true},
		{"/dev/ublkc0", false},   // the char device, never a staged block device
		{"/dev/ublkb0p1", false}, // partitions are never staged
		{"/dev/ublkb", false},    // no device number
		{"/dev/nvme0n1", false},  // kernel NVMe
		{"/dev/sda", false},      // SCSI
		{"/dev/xublkb0", false},  // prefix must be exact
		{"", false},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, IsNVMeUblkDevice(tc.path), tc.path)
	}
}

// A mounted ublk device has no SCSI ancestry, so its iSCSI identity lookup
// fails. Without the name-class exclusion that failure would count as an
// unknown device and veto every iSCSI session-GC pass on a node that stages
// even one volume through the userspace NVMe/TCP data path.
func TestIsPositivelyNotISCSIBackableExcludesUblk(t *testing.T) {
	assert.True(t, IsPositivelyNotISCSIBackable("/dev/ublkb0"))
	assert.True(t, IsPositivelyNotISCSIBackable("/dev/ublkb12"))
	assert.False(t, IsPositivelyNotISCSIBackable("/dev/sdb"))
	assert.False(t, IsPositivelyNotISCSIBackable("/dev/dm-3"))
}
