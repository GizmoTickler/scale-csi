package util

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"syscall"
	"time"
)

// DefaultNVMeUblkSocket is where nvmeublkd, the per-node userspace NVMe/TCP
// daemon, listens by default. The socket is root-only; the node plugin reaches
// it through a hostPath mount of its directory.
const DefaultNVMeUblkSocket = "/run/nvmeublk/nvmeublkd.sock"

// nvmeUblkMaxResponseBytes bounds one response line. A list of every device on
// a node is a few KiB; anything near this is not a daemon speaking the protocol.
const nvmeUblkMaxResponseBytes = 1 << 20

// ErrNVMeUblkDaemonUnavailable reports that nothing is listening on the
// daemon's control socket: the socket file is absent or refuses connections.
// It is distinct from a daemon that answered with an error, so a caller can
// tell "the daemon is not running" from "the daemon said no".
var ErrNVMeUblkDaemonUnavailable = errors.New("nvmeublkd control socket is unavailable")

// nvmeUblkDevicePathRegex matches the whole-disk block device a ublk server
// exposes. ublk has no partitions in this driver's use, so ublkbNpM is not a
// staged device and is deliberately not matched.
var nvmeUblkDevicePathRegex = regexp.MustCompile(`^ublkb\d+$`)

// IsNVMeUblkDevice reports whether devicePath names a ublk block device
// (/dev/ublkbN). The data behind such a device is served by a userspace
// daemon, so kernel NVMe sysfs knows nothing about it: identity questions
// must go to the daemon instead.
func IsNVMeUblkDevice(devicePath string) bool {
	if devicePath == "" {
		return false
	}
	return nvmeUblkDevicePathRegex.MatchString(filepath.Base(devicePath))
}

// NVMeUblkAttachRequest is one attach request. Volume is the caller's unique
// name for the device; the daemon is idempotent per Volume.
type NVMeUblkAttachRequest struct {
	Volume string
	SubNQN string
	// Addrs are the target portals, each "host:port".
	Addrs []string
	// HostNQN and HostID are the identity the daemon connects with. The daemon
	// uses its own node identity only when BOTH are empty, so a caller that
	// supplies one must supply the other.
	HostNQN  string
	HostID   string
	Queues   int
	Depth    int
	ZeroCopy bool
	NapiUs   int
}

// NVMeUblkPath is one target portal of an attached device.
type NVMeUblkPath struct {
	Addr string `json:"addr"`
	Up   bool   `json:"up"`
}

// NVMeUblkDevice describes one device the daemon serves.
type NVMeUblkDevice struct {
	Volume string         `json:"volume"`
	SubNQN string         `json:"subnqn"`
	DevID  int            `json:"dev_id"`
	Path   string         `json:"path"`
	Paths  []NVMeUblkPath `json:"paths,omitempty"`
	// Existing is set on an attach that found the volume already attached.
	Existing bool `json:"existing,omitempty"`
}

// NVMeUblkClient speaks nvmeublkd's control protocol: one JSON request per
// connection, answered by one JSON line. Every call is bounded by the
// caller's context.
type NVMeUblkClient struct {
	socketPath string
	dial       func(ctx context.Context, socketPath string) (net.Conn, error)
}

// NewNVMeUblkClient returns a client for the daemon listening on socketPath,
// or on DefaultNVMeUblkSocket when socketPath is empty.
func NewNVMeUblkClient(socketPath string) *NVMeUblkClient {
	if socketPath == "" {
		socketPath = DefaultNVMeUblkSocket
	}
	return &NVMeUblkClient{
		socketPath: socketPath,
		dial: func(ctx context.Context, socketPath string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
	}
}

// SocketPath returns the control socket this client talks to.
func (c *NVMeUblkClient) SocketPath() string { return c.socketPath }

type nvmeUblkAttachWire struct {
	Op       string   `json:"op"`
	Volume   string   `json:"volume"`
	SubNQN   string   `json:"subnqn"`
	Addrs    []string `json:"addrs"`
	HostNQN  string   `json:"hostnqn,omitempty"`
	HostID   string   `json:"hostid,omitempty"`
	Queues   int      `json:"queues,omitempty"`
	Depth    int      `json:"depth,omitempty"`
	ZeroCopy bool     `json:"zero_copy"`
	NapiUs   int      `json:"napi_us"`
}

type nvmeUblkResponse struct {
	OK       bool             `json:"ok"`
	Error    string           `json:"error"`
	DevID    *int             `json:"dev_id"`
	Path     string           `json:"path"`
	Existing bool             `json:"existing"`
	Absent   bool             `json:"absent"`
	Devices  []NVMeUblkDevice `json:"devices"`
}

// Attach asks the daemon to serve req as a ublk device and returns it. A
// repeated attach for the same volume and subsystem returns the existing
// device with Existing set; the same volume with a different subsystem fails.
func (c *NVMeUblkClient) Attach(ctx context.Context, req NVMeUblkAttachRequest) (NVMeUblkDevice, error) {
	if req.Volume == "" || req.SubNQN == "" || len(req.Addrs) == 0 {
		return NVMeUblkDevice{}, errors.New("nvmeublkd attach needs a volume, a subsystem NQN and at least one address")
	}
	if (req.HostNQN == "") != (req.HostID == "") {
		// The daemon silently falls back to its OWN identity unless both are
		// present; under strict fencing that is a foreign host and the connect
		// is refused, so refuse the half-specified request here instead.
		return NVMeUblkDevice{}, errors.New("nvmeublkd attach needs both a host NQN and a host ID, or neither")
	}
	resp, err := c.call(ctx, nvmeUblkAttachWire{
		Op:       "attach",
		Volume:   req.Volume,
		SubNQN:   req.SubNQN,
		Addrs:    req.Addrs,
		HostNQN:  req.HostNQN,
		HostID:   req.HostID,
		Queues:   req.Queues,
		Depth:    req.Depth,
		ZeroCopy: req.ZeroCopy,
		NapiUs:   req.NapiUs,
	})
	if err != nil {
		return NVMeUblkDevice{}, fmt.Errorf("attach %s: %w", req.Volume, err)
	}
	if resp.DevID == nil || resp.Path == "" {
		return NVMeUblkDevice{}, fmt.Errorf("attach %s: nvmeublkd response carries no device", req.Volume)
	}
	return NVMeUblkDevice{Volume: req.Volume, SubNQN: req.SubNQN, DevID: *resp.DevID, Path: resp.Path, Existing: resp.Existing}, nil
}

// Detach stops serving volume and deletes its device. It is idempotent:
// absent reports that the daemon had no such volume, which is success.
func (c *NVMeUblkClient) Detach(ctx context.Context, volume string) (absent bool, err error) {
	if volume == "" {
		return false, errors.New("nvmeublkd detach needs a volume")
	}
	resp, err := c.call(ctx, map[string]string{"op": "detach", "volume": volume})
	if err != nil {
		return false, fmt.Errorf("detach %s: %w", volume, err)
	}
	return resp.Absent, nil
}

// List returns every device the daemon serves.
func (c *NVMeUblkClient) List(ctx context.Context) ([]NVMeUblkDevice, error) {
	resp, err := c.call(ctx, map[string]string{"op": "list"})
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return resp.Devices, nil
}

// call sends one request and decodes the single response line. The whole
// exchange, dial included, is bounded by ctx: when ctx ends, the connection's
// deadline is moved into the past, so a wedged daemon cannot hang a CSI RPC.
func (c *NVMeUblkClient) call(ctx context.Context, request any) (nvmeUblkResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nvmeUblkResponse{}, fmt.Errorf("encode request: %w", err)
	}
	conn, err := c.dial(ctx, c.socketPath)
	if err != nil {
		if isNVMeUblkUnavailable(err) {
			return nvmeUblkResponse{}, fmt.Errorf("%w (%s): %w", ErrNVMeUblkDaemonUnavailable, c.socketPath, err)
		}
		return nvmeUblkResponse{}, fmt.Errorf("connect %s: %w", c.socketPath, err)
	}
	defer func() { _ = conn.Close() }()
	// Expiry and cancellation both end ctx, and AfterFunc runs only once ctx
	// is done, so a read or write unblocked here always observes ctx.Err().
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	defer stop()

	if _, err := conn.Write(append(payload, '\n')); err != nil {
		return nvmeUblkResponse{}, c.ioError(ctx, "send request", err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	var line []byte
	for {
		chunk, isPrefix, readErr := reader.ReadLine()
		if readErr != nil {
			return nvmeUblkResponse{}, c.ioError(ctx, "read response", readErr)
		}
		line = append(line, chunk...)
		if len(line) > nvmeUblkMaxResponseBytes {
			return nvmeUblkResponse{}, fmt.Errorf("response exceeds %d bytes", nvmeUblkMaxResponseBytes)
		}
		if !isPrefix {
			break
		}
	}
	var resp nvmeUblkResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return nvmeUblkResponse{}, fmt.Errorf("decode response: %w", err)
	}
	if !resp.OK {
		if resp.Error == "" {
			return nvmeUblkResponse{}, errors.New("nvmeublkd refused the request without a reason")
		}
		return nvmeUblkResponse{}, fmt.Errorf("nvmeublkd: %s", resp.Error)
	}
	return resp, nil
}

// ioError prefers the context's error when the context ended the exchange,
// so a caller sees context.DeadlineExceeded rather than an i/o timeout.
func (c *NVMeUblkClient) ioError(ctx context.Context, what string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%s: %w", what, ctxErr)
	}
	return fmt.Errorf("%s: %w", what, err)
}

func isNVMeUblkUnavailable(err error) bool {
	return errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}
