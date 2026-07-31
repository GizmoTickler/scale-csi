package truenas

import (
	"context"
	"fmt"
	"strings"
)

// SetACLMethod is the middleware method that applies a filesystem ACL. It is a
// @job: the call returns a job id and the work completes asynchronously.
const SetACLMethod = "filesystem.setacl"

// ACLEntry is one access-control entry of an NFSv4 (or POSIX1E) ACL as
// `filesystem.getacl`/`filesystem.setacl` model it. Perms and Flags accept
// either the shorthand `{"BASIC": "FULL_CONTROL"}` form or an explicit bit map
// (`{"READ_DATA": true, "WRITE_DATA": true, ...}`), which is why they are
// untyped maps rather than a fixed struct.
type ACLEntry struct {
	Tag   string                 `json:"tag"`
	ID    *int                   `json:"id,omitempty"`
	Type  string                 `json:"type,omitempty"`
	Perms map[string]interface{} `json:"perms,omitempty"`
	Flags map[string]interface{} `json:"flags,omitempty"`
}

// FilesystemACL is the result of `filesystem.getacl`.
type FilesystemACL struct {
	Path string
	// ACLType is NFS4 or POSIX1E. Applying an NFSv4 dacl requires the dataset's
	// zfs acltype to be NFSV4.
	ACLType string
	// Trivial reports whether the ACL is merely the mode-derived 3-ACE default.
	// A successfully applied explicit dacl flips it to false.
	Trivial bool
	ACL     []ACLEntry
	// NFS41Flags carries autoinherit/protected.
	NFS41Flags map[string]bool
}

// SetACLOptions is the argument bundle for FilesystemSetACL.
type SetACLOptions struct {
	Path string
	DACL []ACLEntry
	// NFS41Flags sets autoinherit/protected. The driver sets protected=true so a
	// later chmod (notably kubelet's fsGroupPolicy=File recursive chown/chmod)
	// cannot silently recompute the ACL away from what was applied.
	NFS41Flags map[string]bool
	UID        *int
	GID        *int
}

// FilesystemGetACL reads the ACL of a path.
func (c *Client) FilesystemGetACL(ctx context.Context, path string) (*FilesystemACL, error) {
	result, err := c.Call(ctx, "filesystem.getacl", path)
	if err != nil {
		return nil, fmt.Errorf("failed to read ACL for %s: %w", path, err)
	}
	return parseFilesystemACL(path, result)
}

// FilesystemSetACL applies a dacl to a path. filesystem.setacl is a @job, so the
// call is dispatched and then awaited through the shared job-wait path; a job
// that ends in a terminal failure state surfaces as an error here.
func (c *Client) FilesystemSetACL(ctx context.Context, opts *SetACLOptions) error {
	if opts == nil || strings.TrimSpace(opts.Path) == "" {
		return fmt.Errorf("filesystem.setacl requires a path")
	}
	if len(opts.DACL) == 0 {
		return fmt.Errorf("filesystem.setacl requires a non-empty dacl for %s", opts.Path)
	}
	args := map[string]interface{}{
		"path": opts.Path,
		"dacl": opts.DACL,
	}
	if len(opts.NFS41Flags) > 0 {
		args["nfs41_flags"] = opts.NFS41Flags
	}
	if opts.UID != nil {
		args["uid"] = *opts.UID
	}
	if opts.GID != nil {
		args["gid"] = *opts.GID
	}

	result, err := c.Call(ctx, SetACLMethod, args)
	if err != nil {
		return fmt.Errorf("failed to dispatch %s for %s: %w", SetACLMethod, opts.Path, err)
	}
	jobID, err := replicationJobID(result)
	if err != nil {
		return fmt.Errorf("%s for %s returned an unusable job id: %w", SetACLMethod, opts.Path, err)
	}
	if err := c.waitForJob(ctx, jobID); err != nil {
		return fmt.Errorf("%s job %d for %s failed: %w", SetACLMethod, jobID, opts.Path, err)
	}
	return nil
}

// ACLTemplateDACL resolves a builtin (or user-defined) ACL template by name to
// its dacl. TrueNAS ships NFS4_OPEN, NFS4_RESTRICTED, NFS4_HOME,
// NFS4_DOMAIN_HOME and NFS4_ADMIN with acltype NFS4, plus POSIX1E variants.
func (c *Client) ACLTemplateDACL(ctx context.Context, name string) ([]ACLEntry, error) {
	filters := [][]interface{}{{"name", "=", name}}
	result, err := c.Call(ctx, "filesystem.acltemplate.query", filters, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query ACL template %s: %w", name, err)
	}
	entries, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected filesystem.acltemplate.query response type %T", result)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("ACL template %q not found", name)
	}
	template, ok := entries[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected ACL template entry type %T", entries[0])
	}
	if acltype, ok := template["acltype"].(string); ok && !strings.EqualFold(acltype, "NFS4") {
		return nil, fmt.Errorf("ACL template %q has acltype %s; only NFS4 templates are supported", name, acltype)
	}
	dacl := parseACLEntries(template["acl"])
	if len(dacl) == 0 {
		return nil, fmt.Errorf("ACL template %q carries no entries", name)
	}
	return dacl, nil
}

func parseFilesystemACL(path string, data interface{}) (*FilesystemACL, error) {
	m, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected filesystem.getacl response format %T", data)
	}
	acl := &FilesystemACL{Path: path}
	if v, ok := m["path"].(string); ok && v != "" {
		acl.Path = v
	}
	if v, ok := m["acltype"].(string); ok {
		acl.ACLType = v
	}
	if v, ok := m["trivial"].(bool); ok {
		acl.Trivial = v
	}
	acl.ACL = parseACLEntries(m["acl"])
	if v, ok := m["nfs41_flags"].(map[string]interface{}); ok {
		acl.NFS41Flags = make(map[string]bool, len(v))
		for key, value := range v {
			if flag, ok := value.(bool); ok {
				acl.NFS41Flags[key] = flag
			}
		}
	}
	return acl, nil
}

func parseACLEntries(data interface{}) []ACLEntry {
	items, ok := data.([]interface{})
	if !ok {
		return nil
	}
	entries := make([]ACLEntry, 0, len(items))
	for _, item := range items {
		raw, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		entry := ACLEntry{}
		if v, ok := raw["tag"].(string); ok {
			entry.Tag = v
		}
		if v, ok := raw["type"].(string); ok {
			entry.Type = v
		}
		if v, ok := raw["id"].(float64); ok && v >= 0 {
			id := int(v)
			entry.ID = &id
		}
		if v, ok := raw["perms"].(map[string]interface{}); ok {
			entry.Perms = v
		}
		if v, ok := raw["flags"].(map[string]interface{}); ok {
			entry.Flags = v
		}
		if entry.Tag == "" {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}
