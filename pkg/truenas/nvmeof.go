package truenas

import (
	"context"
	"fmt"
	"strings"
)

// NVMeoFSubsystem represents an NVMe-oF subsystem from the TrueNAS API.
type NVMeoFSubsystem struct {
	ID           int      `json:"id"`
	NQN          string   `json:"nqn"`
	Serial       string   `json:"serial"`
	AllowAnyHost bool     `json:"allow_any_host"`
	Hosts        []string `json:"hosts"`
	Namespaces   []int    `json:"namespaces"`
}

// NVMeoFNamespace represents an NVMe-oF namespace from the TrueNAS API.
type NVMeoFNamespace struct {
	ID         int    `json:"id"`
	Subsystem  int    `json:"subsystem"`
	NSID       int    `json:"nsid"`
	Device     string `json:"device"`
	DevicePath string `json:"device_path"`
	Enabled    bool   `json:"enabled"`
}

// NVMeoFPort represents an NVMe-oF port from the TrueNAS API.
type NVMeoFPort struct {
	ID         int    `json:"id"`
	Transport  string `json:"transport"`
	Address    string `json:"addr_traddr"`
	Port       int    `json:"addr_trsvcid"`
	Subsystems []int  `json:"subsystems"`
}

// NVMeoFSubsystemCreate creates a new NVMe-oF subsystem.
func (c *Client) NVMeoFSubsystemCreate(ctx context.Context, nqn string, serial string, allowAnyHost bool, hosts []string) (*NVMeoFSubsystem, error) {
	params := map[string]interface{}{
		"nqn":            nqn,
		"serial":         serial,
		"allow_any_host": allowAnyHost,
	}
	if len(hosts) > 0 {
		params["hosts"] = hosts
	}

	result, err := c.Call(ctx, "nvmet.subsys.create", params)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return c.NVMeoFSubsystemFindByNQN(ctx, nqn)
		}
		return nil, fmt.Errorf("failed to create NVMe-oF subsystem: %w", err)
	}

	return parseNVMeoFSubsystem(result)
}

// NVMeoFSubsystemDelete deletes an NVMe-oF subsystem.
func (c *Client) NVMeoFSubsystemDelete(ctx context.Context, id int) error {
	_, err := c.Call(ctx, "nvmet.subsys.delete", id)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete NVMe-oF subsystem: %w", err)
	}
	return nil
}

// NVMeoFSubsystemGet retrieves an NVMe-oF subsystem by ID.
func (c *Client) NVMeoFSubsystemGet(ctx context.Context, id int) (*NVMeoFSubsystem, error) {
	filters := [][]interface{}{{"id", "=", id}}
	result, err := c.Call(ctx, "nvmet.subsys.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get NVMe-oF subsystem: %w", err)
	}

	subsystems, ok := result.([]interface{})
	if !ok || len(subsystems) == 0 {
		return nil, fmt.Errorf("NVMe-oF subsystem not found: %d", id)
	}

	return parseNVMeoFSubsystem(subsystems[0])
}

// NVMeoFSubsystemFindByNQN finds an NVMe-oF subsystem by NQN.
func (c *Client) NVMeoFSubsystemFindByNQN(ctx context.Context, nqn string) (*NVMeoFSubsystem, error) {
	filters := [][]interface{}{{"nqn", "=", nqn}}
	result, err := c.Call(ctx, "nvmet.subsys.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query NVMe-oF subsystems: %w", err)
	}

	subsystems, ok := result.([]interface{})
	if !ok || len(subsystems) == 0 {
		return nil, nil
	}

	return parseNVMeoFSubsystem(subsystems[0])
}

// NVMeoFNamespaceCreate creates a new NVMe-oF namespace.
func (c *Client) NVMeoFNamespaceCreate(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error) {
	params := map[string]interface{}{
		"subsystem":   subsystemID,
		"device_path": devicePath,
		"enabled":     true,
	}

	result, err := c.Call(ctx, "nvmet.namespace.create", params)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return c.NVMeoFNamespaceFindByDevice(ctx, subsystemID, devicePath)
		}
		return nil, fmt.Errorf("failed to create NVMe-oF namespace: %w", err)
	}

	return parseNVMeoFNamespace(result)
}

// NVMeoFNamespaceDelete deletes an NVMe-oF namespace.
func (c *Client) NVMeoFNamespaceDelete(ctx context.Context, id int) error {
	_, err := c.Call(ctx, "nvmet.namespace.delete", id)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete NVMe-oF namespace: %w", err)
	}
	return nil
}

// NVMeoFNamespaceGet retrieves an NVMe-oF namespace by ID.
func (c *Client) NVMeoFNamespaceGet(ctx context.Context, id int) (*NVMeoFNamespace, error) {
	filters := [][]interface{}{{"id", "=", id}}
	result, err := c.Call(ctx, "nvmet.namespace.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get NVMe-oF namespace: %w", err)
	}

	namespaces, ok := result.([]interface{})
	if !ok || len(namespaces) == 0 {
		return nil, fmt.Errorf("NVMe-oF namespace not found: %d", id)
	}

	return parseNVMeoFNamespace(namespaces[0])
}

// NVMeoFNamespaceFindByDevice finds an NVMe-oF namespace by device path.
func (c *Client) NVMeoFNamespaceFindByDevice(ctx context.Context, subsystemID int, devicePath string) (*NVMeoFNamespace, error) {
	filters := [][]interface{}{
		{"subsystem", "=", subsystemID},
		{"device_path", "=", devicePath},
	}
	result, err := c.Call(ctx, "nvmet.namespace.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query NVMe-oF namespaces: %w", err)
	}

	namespaces, ok := result.([]interface{})
	if !ok || len(namespaces) == 0 {
		return nil, nil
	}

	return parseNVMeoFNamespace(namespaces[0])
}

// NVMeoFPortList lists all NVMe-oF ports.
func (c *Client) NVMeoFPortList(ctx context.Context) ([]*NVMeoFPort, error) {
	result, err := c.Call(ctx, "nvmet.port.query", []interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to list NVMe-oF ports: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	ports := make([]*NVMeoFPort, 0, len(items))
	for _, item := range items {
		port, err := parseNVMeoFPort(item)
		if err != nil {
			continue
		}
		ports = append(ports, port)
	}

	return ports, nil
}

// NVMeoFGetTransportAddresses gets available transport addresses for a transport type.
func (c *Client) NVMeoFGetTransportAddresses(ctx context.Context, transport string) ([]string, error) {
	result, err := c.Call(ctx, "nvmet.port.transport_address_choices", transport)
	if err != nil {
		return nil, fmt.Errorf("failed to get transport addresses: %w", err)
	}

	addrs, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	addresses := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if s, ok := addr.(string); ok {
			addresses = append(addresses, s)
		}
	}

	return addresses, nil
}

// parseNVMeoFSubsystem converts raw API response to NVMeoFSubsystem.
func parseNVMeoFSubsystem(data interface{}) (*NVMeoFSubsystem, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected NVMe-oF subsystem format")
	}

	subsys := &NVMeoFSubsystem{}

	if v, ok := m["id"].(float64); ok {
		subsys.ID = int(v)
	}
	if v, ok := m["nqn"].(string); ok {
		subsys.NQN = v
	}
	if v, ok := m["serial"].(string); ok {
		subsys.Serial = v
	}
	if v, ok := m["allow_any_host"].(bool); ok {
		subsys.AllowAnyHost = v
	}
	if hosts, ok := m["hosts"].([]interface{}); ok {
		for _, h := range hosts {
			if s, ok := h.(string); ok {
				subsys.Hosts = append(subsys.Hosts, s)
			}
		}
	}
	if namespaces, ok := m["namespaces"].([]interface{}); ok {
		for _, n := range namespaces {
			if id, ok := n.(float64); ok {
				subsys.Namespaces = append(subsys.Namespaces, int(id))
			}
		}
	}

	return subsys, nil
}

// parseNVMeoFNamespace converts raw API response to NVMeoFNamespace.
func parseNVMeoFNamespace(data interface{}) (*NVMeoFNamespace, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected NVMe-oF namespace format")
	}

	ns := &NVMeoFNamespace{}

	if v, ok := m["id"].(float64); ok {
		ns.ID = int(v)
	}
	if v, ok := m["subsystem"].(float64); ok {
		ns.Subsystem = int(v)
	}
	if v, ok := m["nsid"].(float64); ok {
		ns.NSID = int(v)
	}
	if v, ok := m["device"].(string); ok {
		ns.Device = v
	}
	if v, ok := m["device_path"].(string); ok {
		ns.DevicePath = v
	}
	if v, ok := m["enabled"].(bool); ok {
		ns.Enabled = v
	}

	return ns, nil
}

// parseNVMeoFPort converts raw API response to NVMeoFPort.
func parseNVMeoFPort(data interface{}) (*NVMeoFPort, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected NVMe-oF port format")
	}

	port := &NVMeoFPort{}

	if v, ok := m["id"].(float64); ok {
		port.ID = int(v)
	}
	if v, ok := m["transport"].(string); ok {
		port.Transport = v
	}
	if v, ok := m["addr_traddr"].(string); ok {
		port.Address = v
	}
	if v, ok := m["addr_trsvcid"].(float64); ok {
		port.Port = int(v)
	}
	if subsystems, ok := m["subsystems"].([]interface{}); ok {
		for _, s := range subsystems {
			if id, ok := s.(float64); ok {
				port.Subsystems = append(port.Subsystems, int(id))
			}
		}
	}

	return port, nil
}
