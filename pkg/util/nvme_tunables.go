package util

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// nvmeControllerSysfsRoot is where the kernel lists every NVMe controller.
// Variable for testability.
var nvmeControllerSysfsRoot = "/sys/class/nvme"

// NVMeFabricsController is one fabrics (non-PCIe) controller as sysfs reports it.
type NVMeFabricsController struct {
	// Name is the controller's sysfs name, e.g. "nvme3".
	Name string
	// Transport is "tcp", "rdma", "fc" or "loop".
	Transport string
	// Address is the raw sysfs address string, e.g.
	// "traddr=192.0.2.10,trsvcid=4420,src_addr=192.0.2.20".
	Address string
	// SubsysNQN is the NQN of the subsystem the controller belongs to.
	SubsysNQN string
	// FastIOFailTmo is the controller's current fast_io_fail_tmo in seconds,
	// or -1 when the kernel reports "off".
	FastIOFailTmo int
}

// ListNVMeFabricsControllers returns every fabrics controller the kernel knows
// about. PCIe controllers are skipped: they have no fabrics timeouts.
func ListNVMeFabricsControllers() ([]NVMeFabricsController, error) {
	return listNVMeFabricsControllersAt(nvmeControllerSysfsRoot)
}

func listNVMeFabricsControllersAt(root string) ([]NVMeFabricsController, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", root, err)
	}
	controllers := make([]NVMeFabricsController, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "nvme") {
			continue
		}
		dir := filepath.Join(root, name)
		transport := readSysfsTrimmed(filepath.Join(dir, "transport"))
		if transport == "" || transport == "pcie" {
			continue
		}
		fastIOFail, ok := parseNVMeTimeout(readSysfsTrimmed(filepath.Join(dir, "fast_io_fail_tmo")))
		if !ok {
			// A kernel without the attribute (or an unreadable one) cannot be
			// reconciled; report nothing rather than a guessed value.
			continue
		}
		controllers = append(controllers, NVMeFabricsController{
			Name:          name,
			Transport:     transport,
			Address:       readSysfsTrimmed(filepath.Join(dir, "address")),
			SubsysNQN:     readSysfsTrimmed(filepath.Join(dir, "subsysnqn")),
			FastIOFailTmo: fastIOFail,
		})
	}
	return controllers, nil
}

// SetNVMeControllerFastIOFailTmo sets one live controller's fast_io_fail_tmo.
// A negative value disables it ("off"). The kernel applies the new value to the
// running controller; no reconnect is needed.
func SetNVMeControllerFastIOFailTmo(controller string, seconds int) error {
	return setNVMeControllerFastIOFailTmoAt(nvmeControllerSysfsRoot, controller, seconds)
}

func setNVMeControllerFastIOFailTmoAt(root, controller string, seconds int) error {
	if controller == "" || controller == "." || controller == ".." || filepath.Base(controller) != controller || !strings.HasPrefix(controller, "nvme") {
		return fmt.Errorf("invalid NVMe controller name %q", controller)
	}
	if seconds < 0 {
		seconds = -1
	}
	attr := filepath.Join(root, controller, "fast_io_fail_tmo")
	file, err := os.OpenFile(attr, os.O_WRONLY|os.O_TRUNC, 0) // fixed sysfs root plus validated basename
	if err != nil {
		return fmt.Errorf("open %s: %w", attr, err)
	}
	defer file.Close()
	if _, err := file.WriteString(strconv.Itoa(seconds)); err != nil {
		return fmt.Errorf("write %s: %w", attr, err)
	}
	return nil
}

// parseNVMeTimeout parses a fabrics timeout attribute: an integer number of
// seconds, or "off" (reported as -1).
func parseNVMeTimeout(raw string) (int, bool) {
	if raw == "off" {
		return -1, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return value, true
}

func readSysfsTrimmed(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
