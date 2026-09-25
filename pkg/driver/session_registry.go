package driver

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// sessionRegistry records, on the node's filesystem, the transport identities
// (NQNs) of the sessions this node plugin itself connected. Session GC only
// ever disconnects a session found here: a session to the same target portals
// that something else connected (an administrator, another initiator, a
// benchmark) is not this driver's to remove, however orphaned it looks.
//
// Entries live under the plugin's socket directory, a per-driver host path
// that survives plugin restarts. Each entry is one empty file whose name is
// the hex-encoded identity (NQNs contain ':' and may contain '/'), and whose
// modification time is when it was last recorded.
//
// Lifecycle: NodeStage records the NQN BEFORE connecting (a crash between the
// connect and the record would otherwise leave an unrecorded session GC could
// never collect); a successful disconnect by NodeUnstage or GC forgets it.
// GC also records the identities of currently staged volumes every pass, so
// sessions staged before this registry existed become collectable once seen.
type sessionRegistry struct {
	dir string
}

// newSessionRegistry prepares dir. An error means GC must not disconnect
// anything for this protocol, since ownership cannot be established.
func newSessionRegistry(dir string) (*sessionRegistry, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, fmt.Errorf("session registry directory is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create session registry %s: %w", dir, err)
	}
	return &sessionRegistry{dir: dir}, nil
}

func (r *sessionRegistry) path(id string) string {
	return filepath.Join(r.dir, hex.EncodeToString([]byte(id)))
}

// record marks id as connected by this plugin (and refreshes its timestamp).
func (r *sessionRegistry) record(id string) error {
	if r == nil || id == "" {
		return fmt.Errorf("session registry unavailable")
	}
	p := r.path(id)
	now := time.Now()
	if err := os.Chtimes(p, now, now); err == nil {
		return nil
	}
	tmp := p + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("record session %s: %w", id, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("record session %s: %w", id, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("record session %s: %w", id, err)
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("record session %s: %w", id, err)
	}
	return nil
}

// forget removes id; a missing entry is not an error.
func (r *sessionRegistry) forget(id string) error {
	if r == nil || id == "" {
		return nil
	}
	if err := os.Remove(r.path(id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("forget session %s: %w", id, err)
	}
	return nil
}

// has reports whether id was recorded. A read error other than "not found"
// reports false: without proof of ownership GC leaves the session alone.
func (r *sessionRegistry) has(id string) bool {
	if r == nil || id == "" {
		return false
	}
	_, err := os.Stat(r.path(id))
	return err == nil
}

// entries returns every recorded identity with the time it was last recorded.
func (r *sessionRegistry) entries() (map[string]time.Time, error) {
	if r == nil {
		return nil, fmt.Errorf("session registry unavailable")
	}
	dirEntries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]time.Time, len(dirEntries))
	for _, e := range dirEntries {
		if e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		raw, err := hex.DecodeString(e.Name())
		if err != nil || len(raw) == 0 {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out[string(raw)] = info.ModTime()
	}
	return out, nil
}

// forgetNVMeSession drops nqn's record after this plugin disconnected it.
func (d *Driver) forgetNVMeSession(nqn string) {
	if d.nvmeSessions == nil {
		return
	}
	if err := d.nvmeSessions.forget(nqn); err != nil {
		klog.Warningf("NVMe-oF unstage: %v", err)
	}
}
