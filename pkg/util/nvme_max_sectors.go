package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NVMe-oF request-size clamp (nvmeof.maxSectorsKB).
//
// TrueNAS 27 ships a Linux 6.18 target whose nvmet_tcp_map_data() rejects any
// command carrying more than NVMET_TCP_MAXH2CDATA (4 MiB) of data with
// "SGL invalid data" (sct 0x0 / sc 0xf, DNR), while Identify Controller still
// reports MDTS=0 (no limit). A Linux initiator trusts MDTS, leaves
// max_sectors_kb at 32768 and issues commands of up to 32 MiB; every one of
// them fails, ext4 aborts its journal and remounts read-only.
//
// The size that reaches the wire is bounded by the PATH devices
// (nvme<S>c<C>n<N>, hidden gendisks under each controller), not only by the
// multipath head (nvme<S>n<N>): the path request queue merges bios back up to
// its own limit. Hidden gendisks have no /dev node and send no uevents, so a
// udev rule cannot reach them. The clamp therefore writes
// queue/max_sectors_kb on the head AND every path device of the subsystem,
// after the paths are connected and before anything is mounted or published.
// It only ever lowers a value.

// NVMeTCPAutoMaxSectorsKB is the clamp "auto" applies: NVMET_TCP_MAXH2CDATA
// (0x400000 bytes) in KiB.
const NVMeTCPAutoMaxSectorsKB = 4096

// nvmeMinMaxSectorsKB is the smallest value the block layer accepts on any
// architecture (one 4 KiB page); a 64 KiB-page kernel rejects less than 64,
// which surfaces as a write error at stage time.
const nvmeMinMaxSectorsKB = 4

// NVMeMaxSectorsMode selects how the clamp behaves.
type NVMeMaxSectorsMode int

const (
	// NVMeMaxSectorsAuto clamps TCP subsystems whose controller reports
	// MDTS=0 (or whose MDTS cannot be read) to NVMeTCPAutoMaxSectorsKB.
	NVMeMaxSectorsAuto NVMeMaxSectorsMode = iota
	// NVMeMaxSectorsFixed always clamps to LimitKB, whatever the transport
	// or MDTS.
	NVMeMaxSectorsFixed
	// NVMeMaxSectorsOff disables the clamp.
	NVMeMaxSectorsOff
)

// NVMeMaxSectorsPolicy is a parsed nvmeof.maxSectorsKB.
type NVMeMaxSectorsPolicy struct {
	Mode    NVMeMaxSectorsMode
	LimitKB int
}

func (p NVMeMaxSectorsPolicy) String() string {
	switch p.Mode {
	case NVMeMaxSectorsOff:
		return "off"
	case NVMeMaxSectorsFixed:
		return strconv.Itoa(p.LimitKB)
	default:
		return "auto"
	}
}

// ParseNVMeMaxSectorsPolicy parses nvmeof.maxSectorsKB: "" or "auto" is auto,
// "off" or "0" (or "false") disables the clamp, and a positive integer (KiB, at least 4) is
// a fixed ceiling.
func ParseNVMeMaxSectorsPolicy(raw string) (NVMeMaxSectorsPolicy, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", "auto":
		return NVMeMaxSectorsPolicy{Mode: NVMeMaxSectorsAuto}, nil
	case "off", "0", "false":
		// "false": a YAML 1.1 reader loads a bare `off` as a boolean.
		return NVMeMaxSectorsPolicy{Mode: NVMeMaxSectorsOff}, nil
	}
	kb, err := strconv.Atoi(value)
	if err != nil {
		return NVMeMaxSectorsPolicy{}, fmt.Errorf("nvmeof.maxSectorsKB %q is not auto, off or a whole number of KiB", raw)
	}
	if kb < nvmeMinMaxSectorsKB {
		return NVMeMaxSectorsPolicy{}, fmt.Errorf("nvmeof.maxSectorsKB %d must be at least %d (or 0/off to disable)", kb, nvmeMinMaxSectorsKB)
	}
	return NVMeMaxSectorsPolicy{Mode: NVMeMaxSectorsFixed, LimitKB: kb}, nil
}

// NVMeMaxSectorsClampOptions configures one clamp pass.
type NVMeMaxSectorsClampOptions struct {
	Policy NVMeMaxSectorsPolicy
	// Transport is the volume's transport, used only when sysfs does not
	// report one for any controller.
	Transport string
	// PathWait bounds how long the pass waits for every LIVE controller of
	// the subsystem to expose its namespace path device (the namespace scan
	// runs asynchronously after a connect). Zero checks once.
	PathWait time.Duration
}

// NVMeMaxSectorsDevice is one block device of the subsystem and what the
// clamp did to it.
type NVMeMaxSectorsDevice struct {
	Name string
	// Head is true for the native multipath head (nvme<S>n<N>).
	Head bool
	// BeforeKB is -1 when queue/max_sectors_kb could not be read.
	BeforeKB int
	AfterKB  int
}

// NVMeMaxSectorsClampResult describes one clamp pass.
type NVMeMaxSectorsClampResult struct {
	NQN       string
	Policy    NVMeMaxSectorsPolicy
	Transport string
	// MDTS is the controller's reported MDTS when it was queried
	// (MDTSQueried), -1 when the query failed.
	MDTS        int
	MDTSQueried bool
	// LimitKB is the ceiling enforced; 0 when nothing was enforced.
	LimitKB int
	// Skipped says why nothing was enforced, empty when LimitKB applies.
	Skipped string
	Devices []NVMeMaxSectorsDevice
	// Waiting lists controllers that are not live and have no path device
	// yet; their path device is created when they connect and is converged
	// by the periodic reconcile.
	Waiting []string
}

// Changed is the number of devices whose value was lowered.
func (r *NVMeMaxSectorsClampResult) Changed() int {
	if r == nil {
		return 0
	}
	changed := 0
	for _, device := range r.Devices {
		if device.BeforeKB != device.AfterKB {
			changed++
		}
	}
	return changed
}

// String is the one-line summary the stage logs: policy, transport, MDTS,
// the enforced ceiling and every device's before -> after value.
func (r *NVMeMaxSectorsClampResult) String() string {
	if r == nil {
		return "<nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "policy=%s transport=%s", r.Policy, r.Transport)
	switch {
	case r.MDTSQueried && r.MDTS < 0:
		b.WriteString(" mdts=unknown")
	case r.MDTSQueried:
		fmt.Fprintf(&b, " mdts=%d", r.MDTS)
	}
	if r.Skipped != "" {
		fmt.Fprintf(&b, " not enforced (%s)", r.Skipped)
	} else {
		fmt.Fprintf(&b, " limit=%dKiB changed=%d", r.LimitKB, r.Changed())
	}
	devices := make([]string, 0, len(r.Devices))
	for _, device := range r.Devices {
		before := strconv.Itoa(device.BeforeKB)
		if device.BeforeKB < 0 {
			before = "?"
		}
		devices = append(devices, fmt.Sprintf("%s %s->%d", device.Name, before, device.AfterKB))
	}
	fmt.Fprintf(&b, " devices=[%s]", strings.Join(devices, ", "))
	if len(r.Waiting) > 0 {
		fmt.Fprintf(&b, " waiting=[%s]", strings.Join(r.Waiting, ", "))
	}
	return b.String()
}

// ErrNVMeMaxSectorsClamp marks a clamp that was required and could not be
// proven in place.
var ErrNVMeMaxSectorsClamp = errors.New("NVMe max_sectors_kb clamp failed")

var (
	nvmeHeadDiskRegex      = regexp.MustCompile(`^nvme\d+n\d+$`)
	nvmeNamespaceDiskRegex = regexp.MustCompile(`^nvme\d+(c\d+)?n\d+$`)
)

// readNVMeControllerMDTS reads a controller's MDTS through `nvme id-ctrl`.
// Variable for testability.
var readNVMeControllerMDTS = nvmeIDCtrlMDTS

func nvmeIDCtrlMDTS(ctx context.Context, controller string) (int, error) {
	if !nvmeControllerRegex.MatchString(controller) {
		return 0, fmt.Errorf("invalid NVMe controller name %q", controller)
	}
	cmdCtx, cancel := context.WithTimeout(ctx, getNVMeTimeout())
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "nvme", "id-ctrl", filepath.Join(nvmeDevRoot, controller), "-o", "json")
	cmd.Env = localeInvariantEnv()
	HardenCmd(cmd)
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("id-ctrl %s failed: %w", controller, err)
	}
	return parseNVMeIDCtrlMDTS(output)
}

// parseNVMeIDCtrlMDTS reads "mdts" from `nvme id-ctrl -o json`.
func parseNVMeIDCtrlMDTS(output []byte) (int, error) {
	var idCtrl struct {
		MDTS *int `json:"mdts"`
	}
	if err := json.Unmarshal(output, &idCtrl); err != nil {
		return 0, fmt.Errorf("parse id-ctrl output: %w", err)
	}
	if idCtrl.MDTS == nil || *idCtrl.MDTS < 0 {
		return 0, errors.New("id-ctrl output has no mdts")
	}
	return *idCtrl.MDTS, nil
}

type nvmeQueueDevice struct {
	name     string
	queueDir string
	head     bool
}

type nvmeSubsysController struct {
	name      string
	state     string
	transport string
	hasPath   bool
}

type nvmeQueueSnapshot struct {
	found       bool
	controllers []nvmeSubsysController
	devices     []nvmeQueueDevice
}

// pendingLive lists live controllers that do not expose a path device yet.
func (s nvmeQueueSnapshot) pendingLive() []string {
	var pending []string
	for _, ctrl := range s.controllers {
		if ctrl.state == "live" && !ctrl.hasPath {
			pending = append(pending, ctrl.name)
		}
	}
	return pending
}

func listDirNames(dir string, keep func(string) bool) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if keep(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

// scanNVMeQueueDevices lists every block device of every subsystem directory
// carrying nqn (more than one with nvme_core.multipath=N): heads directly
// under the subsystem, and the namespace device under each controller the
// subsystem links (nvme<S>c<C>n<N> with native multipath, nvme<C>n<N>
// without). Path devices first, heads last, each group sorted.
func scanNVMeQueueDevices(subsystemRoot, nqn string) nvmeQueueSnapshot {
	var snap nvmeQueueSnapshot
	var heads []nvmeQueueDevice
	for _, subsys := range listDirNames(subsystemRoot, func(n string) bool { return strings.HasPrefix(n, "nvme-subsys") }) {
		dir := filepath.Join(subsystemRoot, subsys)
		if readSysfsTrimmed(filepath.Join(dir, "subsysnqn")) != nqn {
			continue
		}
		snap.found = true
		for _, name := range listDirNames(dir, nvmeHeadDiskRegex.MatchString) {
			heads = append(heads, nvmeQueueDevice{name: name, queueDir: filepath.Join(dir, name, "queue"), head: true})
		}
		for _, ctrl := range listDirNames(dir, nvmeControllerRegex.MatchString) {
			ctrlDir := filepath.Join(dir, ctrl)
			paths := listDirNames(ctrlDir, nvmeNamespaceDiskRegex.MatchString)
			snap.controllers = append(snap.controllers, nvmeSubsysController{
				name:      ctrl,
				state:     readSysfsTrimmed(filepath.Join(ctrlDir, "state")),
				transport: readSysfsTrimmed(filepath.Join(ctrlDir, "transport")),
				hasPath:   len(paths) > 0,
			})
			for _, name := range paths {
				snap.devices = append(snap.devices, nvmeQueueDevice{name: name, queueDir: filepath.Join(ctrlDir, name, "queue")})
			}
		}
	}
	snap.devices = append(snap.devices, heads...)
	return snap
}

func readMaxSectorsKB(queueDir string) int {
	value, err := strconv.Atoi(readSysfsTrimmed(filepath.Join(queueDir, "max_sectors_kb")))
	if err != nil || value < 0 {
		return -1
	}
	return value
}

func writeMaxSectorsKB(queueDir string, kb int) error {
	attr := filepath.Join(queueDir, "max_sectors_kb")
	file, err := os.OpenFile(attr, os.O_WRONLY|os.O_TRUNC, 0) // fixed sysfs root plus names matched against the device regexes
	if err != nil {
		return fmt.Errorf("open %s: %w", attr, err)
	}
	if _, err := file.WriteString(strconv.Itoa(kb)); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %s: %w", attr, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("write %s: %w", attr, err)
	}
	return nil
}

// ClampNVMeoFMaxSectors lowers queue/max_sectors_kb on the head and every
// path device of the subsystem carrying nqn, as opts.Policy requires. It never
// raises a value. An error (wrapping ErrNVMeMaxSectorsClamp) means the clamp
// was required and is not in place: the caller must not mount or publish.
func ClampNVMeoFMaxSectors(ctx context.Context, nqn string, opts NVMeMaxSectorsClampOptions) (*NVMeMaxSectorsClampResult, error) {
	return clampNVMeoFMaxSectorsAt(ctx, nvmeSubsystemClassRoot, nqn, opts)
}

func clampNVMeoFMaxSectorsAt(ctx context.Context, subsystemRoot, nqn string, opts NVMeMaxSectorsClampOptions) (*NVMeMaxSectorsClampResult, error) {
	result := &NVMeMaxSectorsClampResult{NQN: nqn, Policy: opts.Policy, Transport: strings.ToLower(strings.TrimSpace(opts.Transport))}
	if opts.Policy.Mode == NVMeMaxSectorsOff {
		result.Skipped = "disabled"
		return result, nil
	}
	fail := func(format string, args ...any) (*NVMeMaxSectorsClampResult, error) {
		return result, fmt.Errorf("%w for %s: %s", ErrNVMeMaxSectorsClamp, nqn, fmt.Sprintf(format, args...))
	}

	// Wait (bounded) until every live controller exposes its path device:
	// the namespace scan that creates it runs after `nvme connect` returns.
	deadline := time.Now().Add(opts.PathWait)
	interval := 50 * time.Millisecond
	var snap nvmeQueueSnapshot
	for {
		snap = scanNVMeQueueDevices(subsystemRoot, nqn)
		if snap.found && len(snap.devices) > 0 && len(snap.pendingLive()) == 0 {
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return fail("canceled waiting for path devices: %v", ctx.Err())
		case <-time.After(interval):
		}
		interval = min(interval*2, 200*time.Millisecond)
	}
	for _, ctrl := range snap.controllers {
		if ctrl.state != "live" && !ctrl.hasPath {
			result.Waiting = append(result.Waiting, ctrl.name)
		}
	}
	if !snap.found {
		return fail("subsystem is not in sysfs")
	}
	if len(snap.devices) == 0 {
		return fail("subsystem has no block devices in sysfs")
	}
	if pending := snap.pendingLive(); len(pending) > 0 {
		return fail("live controller(s) %s have no namespace path device yet", strings.Join(pending, ", "))
	}

	before := make([]int, len(snap.devices))
	for i, device := range snap.devices {
		before[i] = readMaxSectorsKB(device.queueDir)
		result.Devices = append(result.Devices, NVMeMaxSectorsDevice{Name: device.name, Head: device.head, BeforeKB: before[i], AfterKB: before[i]})
	}

	limit := opts.Policy.LimitKB
	if opts.Policy.Mode == NVMeMaxSectorsAuto {
		var transports []string
		for _, ctrl := range snap.controllers {
			if ctrl.transport != "" {
				transports = append(transports, strings.ToLower(ctrl.transport))
			}
		}
		if len(transports) > 0 {
			result.Transport = transports[0]
		}
		tcp := false
		for _, transport := range transports {
			tcp = tcp || transport == "tcp"
		}
		if len(transports) == 0 {
			tcp = result.Transport == "" || result.Transport == "tcp"
		}
		if !tcp {
			result.Skipped = "auto applies to tcp only"
			return result, nil
		}
		limit = NVMeTCPAutoMaxSectorsKB
		over := false
		for _, kb := range before {
			over = over || kb < 0 || kb > limit
		}
		if !over {
			// Already within the ceiling: nothing to lower, MDTS irrelevant.
			result.LimitKB = limit
			return result, nil
		}
		// One live controller answers for the subsystem. With none live, or a
		// failed query, MDTS is unknown and the clamp applies: lowering is
		// always safe, mounting unprotected is not.
		result.MDTSQueried = true
		result.MDTS = -1
		for _, ctrl := range snap.controllers {
			if ctrl.state != "live" {
				continue
			}
			if mdts, err := readNVMeControllerMDTS(ctx, ctrl.name); err == nil {
				result.MDTS = mdts
			}
			break
		}
		if result.MDTS > 0 {
			result.Skipped = fmt.Sprintf("controller reports MDTS=%d, which the kernel honors", result.MDTS)
			return result, nil
		}
	}
	result.LimitKB = limit

	var failures []string
	for i, device := range snap.devices {
		if before[i] >= 0 && before[i] <= limit {
			continue
		}
		if err := writeMaxSectorsKB(device.queueDir, limit); err != nil {
			failures = append(failures, err.Error())
			continue
		}
		after := readMaxSectorsKB(device.queueDir)
		result.Devices[i].AfterKB = after
		if after < 0 || after > limit {
			failures = append(failures, fmt.Sprintf("%s reads back %d after writing %d", device.name, after, limit))
		}
	}
	if len(failures) > 0 {
		return fail("%s", strings.Join(failures, "; "))
	}
	return result, nil
}
