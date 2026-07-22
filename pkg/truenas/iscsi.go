package truenas

import (
	"context"
	"fmt"
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
	Portal       int      `json:"portal"`
	Initiator    int      `json:"initiator"`
	AuthMethod   string   `json:"authmethod"`
	Auth         *int     `json:"auth"`
	AuthNetworks []string `json:"auth_networks,omitempty"`
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
func (c *Client) ISCSITargetCreate(ctx context.Context, name, alias, mode string, groups []ISCSITargetGroup) (*ISCSITarget, error) {
	params := map[string]interface{}{
		"name":   name,
		"mode":   mode,
		"groups": iscsiTargetGroupMaps(groups),
	}
	// Only include alias if non-empty
	if alias != "" {
		params["alias"] = alias
	}

	result, err := c.Call(ctx, "iscsi.target.create", params)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous "Invalid params")
		LogAPIError(err, "ISCSITargetCreate error")

		// TrueNAS returns "Invalid params" when target already exists (not a helpful error message)
		// Check if target exists and return it if so
		if IsAlreadyExistsError(err) || MessageFallbackContains(err, "invalid params") {
			existing, findErr := c.ISCSITargetFindByName(ctx, name)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create iSCSI target: %w", err)
	}

	return parseISCSITarget(result)
}

func iscsiTargetGroupMaps(groups []ISCSITargetGroup) []map[string]interface{} {
	groupMaps := make([]map[string]interface{}, len(groups))
	for i, group := range groups {
		entry := map[string]interface{}{
			"portal":     group.Portal,
			"initiator":  group.Initiator,
			"authmethod": group.AuthMethod,
		}
		if group.Auth != nil {
			entry["auth"] = *group.Auth
		}
		if len(group.AuthNetworks) > 0 {
			entry["auth_networks"] = append([]string(nil), group.AuthNetworks...)
		}
		groupMaps[i] = entry
	}
	return groupMaps
}

// ISCSITargetUpdate atomically replaces a target's portal/initiator groups.
func (c *Client) ISCSITargetUpdate(ctx context.Context, id int, groups []ISCSITargetGroup) (*ISCSITarget, error) {
	result, err := c.Call(ctx, "iscsi.target.update", id, map[string]interface{}{
		"groups": iscsiTargetGroupMaps(groups),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update iSCSI target: %w", err)
	}
	return parseISCSITarget(result)
}

// ISCSITargetDelete deletes an iSCSI target.
func (c *Client) ISCSITargetDelete(ctx context.Context, id int, force bool) error {
	_, err := c.Call(ctx, "iscsi.target.delete", id, force)
	if err != nil {
		if IsNotFoundError(err) {
			return nil
		}
		if c.deleteVanishedTolerant(ctx, "iscsi.target.query", id) {
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

// ISCSITargetList returns every iSCSI target on the backend. It is used by the
// orphaned-share reconcile sweep, which must inspect all targets regardless of
// the volume they back.
func (c *Client) ISCSITargetList(ctx context.Context) ([]*ISCSITarget, error) {
	result, err := c.Call(ctx, "iscsi.target.query", [][]interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI targets: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI target response type")
	}

	targets := make([]*ISCSITarget, 0, len(items))
	for _, item := range items {
		target, parseErr := parseISCSITarget(item)
		if parseErr != nil {
			continue
		}
		targets = append(targets, target)
	}

	return targets, nil
}

// ISCSIExtentCreate creates a new iSCSI extent.
func (c *Client) ISCSIExtentCreate(ctx context.Context, name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string) (*ISCSIExtent, error) {
	params := iscsiExtentCreateParams(name, diskPath, comment, blocksize, physicalBlocksize, rpm)

	result, err := c.Call(ctx, "iscsi.extent.create", params)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous "Invalid params")
		LogAPIError(err, "ISCSIExtentCreate error")

		// TrueNAS returns "Invalid params" when extent already exists
		// Check if extent exists and return it if so
		if IsAlreadyExistsError(err) || MessageFallbackContains(err, "invalid params") {
			existing, findErr := c.ISCSIExtentFindByName(ctx, name)
			if findErr == nil && existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("failed to create iSCSI extent: %w", err)
	}

	return parseISCSIExtent(result)
}

func iscsiExtentCreateParams(name, diskPath, comment string, blocksize int, physicalBlocksize bool, rpm string) map[string]interface{} {
	return map[string]interface{}{
		"name":         name,
		"type":         "DISK",
		"disk":         diskPath,
		"comment":      comment,
		"blocksize":    blocksize,
		"pblocksize":   physicalBlocksize,
		"insecure_tpc": true,
		"rpm":          rpm,
		"ro":           false,
		"enabled":      true,
	}
}

// ISCSIExtentDelete deletes an iSCSI extent.
func (c *Client) ISCSIExtentDelete(ctx context.Context, id int, remove, force bool) error {
	_, err := c.Call(ctx, "iscsi.extent.delete", id, remove, force)
	if err != nil {
		if IsNotFoundError(err) {
			return nil
		}
		if c.deleteVanishedTolerant(ctx, "iscsi.extent.query", id) {
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
func (c *Client) ISCSITargetExtentCreate(ctx context.Context, targetID, extentID, lunID int) (*ISCSITargetExtent, error) {
	params := map[string]interface{}{
		"target": targetID,
		"extent": extentID,
	}
	if lunID >= 0 {
		params["lunid"] = lunID
	}

	result, err := c.Call(ctx, "iscsi.targetextent.create", params)
	if err != nil {
		// Log full error details before fallback logic (helps debug ambiguous "Invalid params")
		LogAPIError(err, "ISCSITargetExtentCreate error")

		// TrueNAS returns "Invalid params" when association already exists
		// Check if association exists and return it if so
		if IsAlreadyExistsError(err) || MessageFallbackContains(err, "invalid params") {
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
		if IsNotFoundError(err) {
			return nil
		}
		if c.deleteVanishedTolerant(ctx, "iscsi.targetextent.query", id) {
			return nil
		}
		return fmt.Errorf("failed to delete target-extent association: %w", err)
	}
	return nil
}

// ISCSITargetExtentGet retrieves a target-extent association by ID.
func (c *Client) ISCSITargetExtentGet(ctx context.Context, id int) (*ISCSITargetExtent, error) {
	filters := [][]interface{}{
		{"id", "=", id},
	}
	result, err := c.Call(ctx, "iscsi.targetextent.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query target-extent by ID: %w", err)
	}

	assocs, ok := result.([]interface{})
	if !ok || len(assocs) == 0 {
		return nil, nil
	}

	return parseISCSITargetExtent(assocs[0])
}

// ISCSITargetExtentFind finds a target-extent association.
func (c *Client) ISCSITargetExtentFind(ctx context.Context, targetID, extentID int) (*ISCSITargetExtent, error) {
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
			gm, ok := g.(map[string]interface{})
			if !ok {
				continue
			}
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
			switch values := gm["auth_networks"].(type) {
			case []interface{}:
				for _, value := range values {
					if network, ok := value.(string); ok {
						group.AuthNetworks = append(group.AuthNetworks, network)
					}
				}
			case []string:
				group.AuthNetworks = append(group.AuthNetworks, values...)
			}
			target.Groups = append(target.Groups, group)
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

// ISCSIExtentList returns every iSCSI extent on the backend. The orphaned-share
// reconcile sweep reads each extent's comment backreference to decide whether its
// backing dataset is gone.
func (c *Client) ISCSIExtentList(ctx context.Context) ([]*ISCSIExtent, error) {
	result, err := c.Call(ctx, "iscsi.extent.query", [][]interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI extents: %w", err)
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI extent response type")
	}

	extents := make([]*ISCSIExtent, 0, len(items))
	for _, item := range items {
		extent, parseErr := parseISCSIExtent(item)
		if parseErr != nil {
			continue
		}
		extents = append(extents, extent)
	}

	return extents, nil
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

// ISCSIPortalListen is one listen address of an iSCSI portal.
type ISCSIPortalListen struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// ISCSIPortal represents a TrueNAS iSCSI portal.
type ISCSIPortal struct {
	ID     int                 `json:"id"`
	Tag    int                 `json:"tag"`
	Listen []ISCSIPortalListen `json:"listen"`
}

// ISCSIInitiator represents a TrueNAS iSCSI initiator group. A nil Initiators
// list (JSON null) means allow-all; a non-nil empty list (JSON []) is deny-all.
type ISCSIInitiator struct {
	ID         int      `json:"id"`
	Initiators []string `json:"initiators"`
	Comment    string   `json:"comment"`
}

// ISCSIPortalList lists all iSCSI portals.
func (c *Client) ISCSIPortalList(ctx context.Context) ([]*ISCSIPortal, error) {
	result, err := c.Call(ctx, "iscsi.portal.query", [][]interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI portals: %w", err)
	}
	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	portals := make([]*ISCSIPortal, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		portal := &ISCSIPortal{}
		if v, ok := m["id"].(float64); ok {
			portal.ID = int(v)
		}
		if v, ok := m["tag"].(float64); ok {
			portal.Tag = int(v)
		}
		if listens, ok := m["listen"].([]interface{}); ok {
			for _, l := range listens {
				lm, ok := l.(map[string]interface{})
				if !ok {
					continue
				}
				entry := ISCSIPortalListen{}
				if ip, ok := lm["ip"].(string); ok {
					entry.IP = ip
				}
				if p, ok := lm["port"].(float64); ok {
					entry.Port = int(p)
				}
				portal.Listen = append(portal.Listen, entry)
			}
		}
		portals = append(portals, portal)
	}
	return portals, nil
}

// ISCSIInitiatorList lists all iSCSI initiator groups.
func (c *Client) ISCSIInitiatorList(ctx context.Context) ([]*ISCSIInitiator, error) {
	result, err := c.Call(ctx, "iscsi.initiator.query", [][]interface{}{}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI initiator groups: %w", err)
	}
	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	groups := make([]*ISCSIInitiator, 0, len(items))
	for _, item := range items {
		group, parseErr := parseISCSIInitiator(item)
		if parseErr != nil {
			continue
		}
		groups = append(groups, group)
	}
	return groups, nil
}

// ISCSIInitiatorCreate creates an allow-all iSCSI initiator group.
func (c *Client) ISCSIInitiatorCreate(ctx context.Context, comment string) (*ISCSIInitiator, error) {
	return c.ISCSIInitiatorCreateWithInitiators(ctx, nil, comment)
}

// ISCSIInitiatorCreateWithInitiators creates an exact initiator allowlist. The
// legacy allow-all path passes nil; fenced callers pass a non-nil list, where an
// empty list intentionally authorizes no initiators while retaining portals.
func (c *Client) ISCSIInitiatorCreateWithInitiators(ctx context.Context, initiators []string, comment string) (*ISCSIInitiator, error) {
	params := map[string]interface{}{
		"initiators": initiators,
		"comment":    comment,
	}
	result, err := c.Call(ctx, "iscsi.initiator.create", params)
	if err != nil {
		return nil, fmt.Errorf("failed to create iSCSI initiator group: %w", err)
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}
	return parseISCSIInitiator(m)
}

// ISCSIInitiatorGet returns one initiator group by ID.
func (c *Client) ISCSIInitiatorGet(ctx context.Context, id int) (*ISCSIInitiator, error) {
	result, err := c.Call(ctx, "iscsi.initiator.query", [][]interface{}{{"id", "=", id}}, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query iSCSI initiator group: %w", err)
	}
	items, ok := result.([]interface{})
	if !ok || len(items) == 0 {
		return nil, nil
	}
	return parseISCSIInitiator(items[0])
}

// ISCSIInitiatorUpdate replaces the exact initiator allowlist.
func (c *Client) ISCSIInitiatorUpdate(ctx context.Context, id int, initiators []string, comment string) (*ISCSIInitiator, error) {
	params := map[string]interface{}{"initiators": initiators}
	if comment != "" {
		params["comment"] = comment
	}
	result, err := c.Call(ctx, "iscsi.initiator.update", id, params)
	if err != nil {
		return nil, fmt.Errorf("failed to update iSCSI initiator group: %w", err)
	}
	return parseISCSIInitiator(result)
}

// ISCSIInitiatorDelete removes an initiator group idempotently.
func (c *Client) ISCSIInitiatorDelete(ctx context.Context, id int) error {
	_, err := c.Call(ctx, "iscsi.initiator.delete", id)
	if err == nil || IsNotFoundError(err) {
		return nil
	}
	if c.deleteVanishedTolerant(ctx, "iscsi.initiator.query", id) {
		return nil
	}
	return fmt.Errorf("failed to delete iSCSI initiator group: %w", err)
}

func parseISCSIInitiator(raw interface{}) (*ISCSIInitiator, error) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected iSCSI initiator group format")
	}
	group := &ISCSIInitiator{}
	switch id := m["id"].(type) {
	case float64:
		group.ID = int(id)
	case int:
		group.ID = id
	}
	if comment, ok := m["comment"].(string); ok {
		group.Comment = comment
	}
	switch initiators := m["initiators"].(type) {
	case []interface{}:
		// Preserve JSON [] as a non-nil empty slice. TrueNAS uses null for the
		// legacy allow-all group, while [] is the deliberate deny-all shape used
		// by fencing on last unpublish.
		group.Initiators = []string{}
		for _, initiator := range initiators {
			if value, ok := initiator.(string); ok {
				group.Initiators = append(group.Initiators, value)
			}
		}
	case []string:
		group.Initiators = append([]string{}, initiators...)
	}
	return group, nil
}
