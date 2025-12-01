package truenas

import (
	"context"
	"fmt"
	"strings"
)

// ISCSITarget represents an iSCSI target from the TrueNAS API.
type ISCSITarget struct {
	ID     int                `json:"id"`
	Name   string             `json:"name"`
	Alias  string             `json:"alias"`
	Mode   string             `json:"mode"`
	Groups []ISCSITargetGroup `json:"groups"`
}

// ISCSITargetGroup represents a portal/initiator group for a target.
type ISCSITargetGroup struct {
	Portal     int    `json:"portal"`
	Initiator  int    `json:"initiator"`
	AuthMethod string `json:"authmethod"`
	Auth       *int   `json:"auth"`
}

// ISCSIExtent represents an iSCSI extent from the TrueNAS API.
type ISCSIExtent struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Disk        string `json:"disk"`
	Serial      string `json:"serial"`
	Path        string `json:"path"`
	Comment     string `json:"comment"`
	Naa         string `json:"naa"`
	Blocksize   int    `json:"blocksize"`
	Pblocksize  bool   `json:"pblocksize"`
	InsecureTpc bool   `json:"insecure_tpc"`
	Xen         bool   `json:"xen"`
	Rpm         string `json:"rpm"`
	Ro          bool   `json:"ro"`
	Enabled     bool   `json:"enabled"`
}

// ISCSITargetExtent represents a target-to-extent association.
type ISCSITargetExtent struct {
	ID     int `json:"id"`
	Target int `json:"target"`
	Extent int `json:"extent"`
	LunID  int `json:"lunid"`
}

// ISCSIGlobalConfig represents the global iSCSI configuration.
type ISCSIGlobalConfig struct {
	ID                 int    `json:"id"`
	Basename           string `json:"basename"`
	ISNSIP             string `json:"isns_servers"`
	PoolAvailThreshold int    `json:"pool_avail_threshold"`
}

// ISCSITargetCreate creates a new iSCSI target.
func (c *Client) ISCSITargetCreate(ctx context.Context, name string, alias string, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error) {
	// Convert groups to maps, omitting auth field when nil (TrueNAS API prefers no field vs null)
	groupMaps := make([]map[string]interface{}, len(groups))
	for i, g := range groups {
		gm := map[string]interface{}{
			"portal":     g.Portal,
			"initiator":  g.Initiator,
			"authmethod": g.AuthMethod,
		}
		if g.Auth != nil {
			gm["auth"] = *g.Auth
		}
		groupMaps[i] = gm
	}

	params := map[string]interface{}{
		"name":   name,
		"mode":   mode,
		"groups": groupMaps,
	}
	// Only include alias if non-empty
	if alias != "" {
		params["alias"] = alias
	}

	result, err := c.Call(ctx, "iscsi.target.create", params)
	if err != nil {
		// TrueNAS returns "Invalid params" when target already exists (not a helpful error message)
		// Check if target exists and return it if so
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Invalid params") {
			existing, findErr := c.ISCSITargetFindByName(ctx, name)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create iSCSI target: %w", err)
	}

	return parseISCSITarget(result)
}

// ISCSITargetDelete deletes an iSCSI target.
func (c *Client) ISCSITargetDelete(ctx context.Context, id int, force bool) error {
	_, err := c.Call(ctx, "iscsi.target.delete", id, force)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete iSCSI target: %w", err)
	}
	return nil
}

// ISCSITargetGet retrieves an iSCSI target by ID.
func (c *Client) ISCSITargetGet(ctx context.Context, id int) (*ISCSITarget, error) {
	filters := [][]interface{}{{"id", "=", id}}
	result, err := c.Call(ctx, "iscsi.target.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get iSCSI target: %w", err)
	}

	targets, ok := result.([]interface{})
	if !ok || len(targets) == 0 {
		return nil, fmt.Errorf("iSCSI target not found: %d", id)
	}

	return parseISCSITarget(targets[0])
}

// ISCSITargetFindByName finds an iSCSI target by name.
func (c *Client) ISCSITargetFindByName(ctx context.Context, name string) (*ISCSITarget, error) {
	filters := [][]interface{}{{"name", "=", name}}
	result, err := c.Call(ctx, "iscsi.target.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI targets: %w", err)
	}

	targets, ok := result.([]interface{})
	if !ok || len(targets) == 0 {
		return nil, nil
	}

	return parseISCSITarget(targets[0])
}

// ISCSIExtentCreate creates a new iSCSI extent.
func (c *Client) ISCSIExtentCreate(ctx context.Context, name string, diskPath string, comment string, blocksize int, rpm string) (*ISCSIExtent, error) {
	params := map[string]interface{}{
		"name":         name,
		"type":         "DISK",
		"disk":         diskPath,
		"comment":      comment,
		"blocksize":    blocksize,
		"pblocksize":   true,
		"insecure_tpc": true,
		"rpm":          rpm,
		"ro":           false,
		"enabled":      true,
	}

	result, err := c.Call(ctx, "iscsi.extent.create", params)
	if err != nil {
		// TrueNAS returns "Invalid params" when extent already exists
		// Check if extent exists and return it if so
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Invalid params") {
			existing, findErr := c.ISCSIExtentFindByName(ctx, name)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create iSCSI extent: %w", err)
	}

	return parseISCSIExtent(result)
}

// ISCSIExtentDelete deletes an iSCSI extent.
func (c *Client) ISCSIExtentDelete(ctx context.Context, id int, remove bool, force bool) error {
	_, err := c.Call(ctx, "iscsi.extent.delete", id, remove, force)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete iSCSI extent: %w", err)
	}
	return nil
}

// ISCSIExtentGet retrieves an iSCSI extent by ID.
func (c *Client) ISCSIExtentGet(ctx context.Context, id int) (*ISCSIExtent, error) {
	filters := [][]interface{}{{"id", "=", id}}
	result, err := c.Call(ctx, "iscsi.extent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to get iSCSI extent: %w", err)
	}

	extents, ok := result.([]interface{})
	if !ok || len(extents) == 0 {
		return nil, fmt.Errorf("iSCSI extent not found: %d", id)
	}

	return parseISCSIExtent(extents[0])
}

// ISCSIExtentFindByName finds an iSCSI extent by name.
func (c *Client) ISCSIExtentFindByName(ctx context.Context, name string) (*ISCSIExtent, error) {
	filters := [][]interface{}{{"name", "=", name}}
	result, err := c.Call(ctx, "iscsi.extent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI extents: %w", err)
	}

	extents, ok := result.([]interface{})
	if !ok || len(extents) == 0 {
		return nil, nil
	}

	return parseISCSIExtent(extents[0])
}

// ISCSITargetExtentCreate creates a target-to-extent association.
func (c *Client) ISCSITargetExtentCreate(ctx context.Context, targetID int, extentID int, lunID int) (*ISCSITargetExtent, error) {
	params := map[string]interface{}{
		"target": targetID,
		"extent": extentID,
	}
	if lunID >= 0 {
		params["lunid"] = lunID
	}

	result, err := c.Call(ctx, "iscsi.targetextent.create", params)
	if err != nil {
		// TrueNAS returns "Invalid params" when association already exists
		// Check if association exists and return it if so
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Invalid params") {
			existing, findErr := c.ISCSITargetExtentFind(ctx, targetID, extentID)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create target-extent association: %w", err)
	}

	return parseISCSITargetExtent(result)
}

// ISCSITargetExtentDelete deletes a target-to-extent association.
func (c *Client) ISCSITargetExtentDelete(ctx context.Context, id int, force bool) error {
	_, err := c.Call(ctx, "iscsi.targetextent.delete", id, force)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "not found") {
			return nil
		}
		return fmt.Errorf("failed to delete target-extent association: %w", err)
	}
	return nil
}

// ISCSITargetExtentFind finds a target-extent association.
func (c *Client) ISCSITargetExtentFind(ctx context.Context, targetID int, extentID int) (*ISCSITargetExtent, error) {
	filters := [][]interface{}{
		{"target", "=", targetID},
		{"extent", "=", extentID},
	}
	result, err := c.Call(ctx, "iscsi.targetextent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query target-extent associations: %w", err)
	}

	assocs, ok := result.([]interface{})
	if !ok || len(assocs) == 0 {
		return nil, nil
	}

	return parseISCSITargetExtent(assocs[0])
}

// ISCSIGlobalConfigGet retrieves the global iSCSI configuration.
func (c *Client) ISCSIGlobalConfigGet(ctx context.Context) (*ISCSIGlobalConfig, error) {
	result, err := c.Call(ctx, "iscsi.global.config")
	if err != nil {
		return nil, fmt.Errorf("failed to get iSCSI global config: %w", err)
	}

	return parseISCSIGlobalConfig(result)
}

// parseISCSITarget converts raw API response to ISCSITarget.
func parseISCSITarget(data interface{}) (*ISCSITarget, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI target format")
	}

	target := &ISCSITarget{}

	if v, ok := m["id"].(float64); ok {
		target.ID = int(v)
	}
	if v, ok := m["name"].(string); ok {
		target.Name = v
	}
	if v, ok := m["alias"].(string); ok {
		target.Alias = v
	}
	if v, ok := m["mode"].(string); ok {
		target.Mode = v
	}

	if groups, ok := m["groups"].([]interface{}); ok {
		for _, g := range groups {
			if gm, ok := g.(map[string]interface{}); ok {
				group := ISCSITargetGroup{}
				if v, ok := gm["portal"].(float64); ok {
					group.Portal = int(v)
				}
				if v, ok := gm["initiator"].(float64); ok {
					group.Initiator = int(v)
				}
				if v, ok := gm["authmethod"].(string); ok {
					group.AuthMethod = v
				}
				if v, ok := gm["auth"].(float64); ok {
					val := int(v)
					group.Auth = &val
				}
				target.Groups = append(target.Groups, group)
			}
		}
	}

	return target, nil
}

// parseISCSIExtent converts raw API response to ISCSIExtent.
func parseISCSIExtent(data interface{}) (*ISCSIExtent, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI extent format")
	}

	extent := &ISCSIExtent{}

	if v, ok := m["id"].(float64); ok {
		extent.ID = int(v)
	}
	if v, ok := m["name"].(string); ok {
		extent.Name = v
	}
	if v, ok := m["type"].(string); ok {
		extent.Type = v
	}
	if v, ok := m["disk"].(string); ok {
		extent.Disk = v
	}
	if v, ok := m["serial"].(string); ok {
		extent.Serial = v
	}
	if v, ok := m["path"].(string); ok {
		extent.Path = v
	}
	if v, ok := m["comment"].(string); ok {
		extent.Comment = v
	}
	if v, ok := m["naa"].(string); ok {
		extent.Naa = v
	}
	if v, ok := m["blocksize"].(float64); ok {
		extent.Blocksize = int(v)
	}
	if v, ok := m["pblocksize"].(bool); ok {
		extent.Pblocksize = v
	}
	if v, ok := m["insecure_tpc"].(bool); ok {
		extent.InsecureTpc = v
	}
	if v, ok := m["xen"].(bool); ok {
		extent.Xen = v
	}
	if v, ok := m["rpm"].(string); ok {
		extent.Rpm = v
	}
	if v, ok := m["ro"].(bool); ok {
		extent.Ro = v
	}
	if v, ok := m["enabled"].(bool); ok {
		extent.Enabled = v
	}

	return extent, nil
}

// parseISCSITargetExtent converts raw API response to ISCSITargetExtent.
func parseISCSITargetExtent(data interface{}) (*ISCSITargetExtent, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected target-extent format")
	}

	te := &ISCSITargetExtent{}

	if v, ok := m["id"].(float64); ok {
		te.ID = int(v)
	}
	if v, ok := m["target"].(float64); ok {
		te.Target = int(v)
	}
	if v, ok := m["extent"].(float64); ok {
		te.Extent = int(v)
	}
	if v, ok := m["lunid"].(float64); ok {
		te.LunID = int(v)
	}

	return te, nil
}

// parseISCSIGlobalConfig converts raw API response to ISCSIGlobalConfig.
func parseISCSIGlobalConfig(data interface{}) (*ISCSIGlobalConfig, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected global config format")
	}

	cfg := &ISCSIGlobalConfig{}

	if v, ok := m["id"].(float64); ok {
		cfg.ID = int(v)
	}
	if v, ok := m["basename"].(string); ok {
		cfg.Basename = v
	}

	return cfg, nil
}

// ISCSIExtentFindByDisk finds an iSCSI extent by disk path.
// This is useful for idempotent extent creation when the zvol path is known.
func (c *Client) ISCSIExtentFindByDisk(ctx context.Context, diskPath string) (*ISCSIExtent, error) {
	filters := [][]interface{}{{"disk", "=", diskPath}}
	result, err := c.Call(ctx, "iscsi.extent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI extents by disk: %w", err)
	}

	extents, ok := result.([]interface{})
	if !ok || len(extents) == 0 {
		return nil, nil
	}

	return parseISCSIExtent(extents[0])
}

// ISCSITargetExtentFindByTarget finds all target-extent associations for a target.
func (c *Client) ISCSITargetExtentFindByTarget(ctx context.Context, targetID int) ([]*ISCSITargetExtent, error) {
	filters := [][]interface{}{{"target", "=", targetID}}
	result, err := c.Call(ctx, "iscsi.targetextent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query target-extent associations: %w", err)
	}

	assocs, ok := result.([]interface{})
	if !ok {
		return nil, nil
	}

	var results []*ISCSITargetExtent
	for _, a := range assocs {
		te, err := parseISCSITargetExtent(a)
		if err != nil {
			continue
		}
		results = append(results, te)
	}

	return results, nil
}

// ISCSITargetExtentFindByExtent finds all target-extent associations for an extent.
func (c *Client) ISCSITargetExtentFindByExtent(ctx context.Context, extentID int) ([]*ISCSITargetExtent, error) {
	filters := [][]interface{}{{"extent", "=", extentID}}
	result, err := c.Call(ctx, "iscsi.targetextent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query target-extent associations: %w", err)
	}

	assocs, ok := result.([]interface{})
	if !ok {
		return nil, nil
	}

	var results []*ISCSITargetExtent
	for _, a := range assocs {
		te, err := parseISCSITargetExtent(a)
		if err != nil {
			continue
		}
		results = append(results, te)
	}

	return results, nil
}
