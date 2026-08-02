package driver

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// StorageClass parameters that reach the remaining `sharing.nfs.create` fields
// the driver historically hard-coded or omitted. EVERY one of them is optional:
// when a StorageClass sets none, createNFSShareForDataset builds exactly the
// payload it built before GF5.
const (
	nfsSecurityParam        = "nfsSecurity"
	nfsExposeSnapshotsParam = "nfsExposeSnapshots"
	nfsReadOnlyParam        = "nfsReadOnly"
	nfsMaprootUserParam     = "nfsMaprootUser"
	nfsMaprootGroupParam    = "nfsMaprootGroup"
	nfsMapallUserParam      = "nfsMapallUser"
	nfsMapallGroupParam     = "nfsMapallGroup"
	nfsAllowedNetworksParam = "nfsAllowedNetworks"
	nfsAllowedHostsParam    = "nfsAllowedHosts"
)

// validNFSSecurityModes is the `security` enum sharing.nfs.create accepts.
var validNFSSecurityModes = map[string]struct{}{
	"SYS":   {},
	"KRB5":  {},
	"KRB5I": {},
	"KRB5P": {},
}

// nfsShareOptions carries the per-StorageClass NFS export overrides resolved at
// CreateVolume. Pointer fields distinguish "unset" (inherit the driver config /
// today's hard-coded value) from an explicit false/empty, which is what keeps
// the default create payload untouched.
type nfsShareOptions struct {
	security        []string
	exposeSnapshots *bool
	readOnly        *bool
	maprootUser     *string
	maprootGroup    *string
	mapallUser      *string
	mapallGroup     *string
	allowedNetworks []string
	allowedHosts    []string
}

// empty reports whether the StorageClass set no NFS export override at all.
func (o *nfsShareOptions) empty() bool {
	if o == nil {
		return true
	}
	return len(o.security) == 0 && o.exposeSnapshots == nil && o.readOnly == nil &&
		o.maprootUser == nil && o.maprootGroup == nil && o.mapallUser == nil &&
		o.mapallGroup == nil && o.allowedNetworks == nil && o.allowedHosts == nil
}

type nfsShareOptionsContextKey struct{}

// withNFSShareOptions threads the resolved per-StorageClass export overrides
// from CreateVolume down to the share builder, mirroring how the iSCSI CHAP
// resolution is threaded. Paths that do NOT carry it (ensureShareExists, the
// reconcilers, adoption) fall back to the driver config exactly as before — an
// existing share is never rewritten because a class gained an option.
func withNFSShareOptions(ctx context.Context, opts *nfsShareOptions) context.Context {
	if opts.empty() {
		return ctx
	}
	return context.WithValue(ctx, nfsShareOptionsContextKey{}, opts)
}

func nfsShareOptionsFromContext(ctx context.Context) *nfsShareOptions {
	opts, _ := ctx.Value(nfsShareOptionsContextKey{}).(*nfsShareOptions)
	return opts
}

// parseNFSShareOptions validates and resolves the NFS export parameters of a
// StorageClass. It returns an error (InvalidArgument) rather than silently
// dropping an unusable value, so a misconfigured class surfaces at CreateVolume
// instead of producing an export nobody can mount.
func (d *Driver) parseNFSShareOptions(params map[string]string) (*nfsShareOptions, error) {
	opts := &nfsShareOptions{}
	if len(params) == 0 {
		return opts, nil
	}

	if raw, ok := params[nfsSecurityParam]; ok {
		security, err := parseNFSSecurityList(nfsSecurityParam, raw, d.config != nil && d.config.NFS.KrbEnabled)
		if err != nil {
			return nil, err
		}
		opts.security = security
	}
	if err := d.validateNFSSecurity(opts.security); err != nil {
		return nil, err
	}

	if raw, ok := params[nfsExposeSnapshotsParam]; ok {
		value, err := parseNFSBoolParam(nfsExposeSnapshotsParam, raw)
		if err != nil {
			return nil, err
		}
		opts.exposeSnapshots = &value
	}
	if raw, ok := params[nfsReadOnlyParam]; ok {
		value, err := parseNFSBoolParam(nfsReadOnlyParam, raw)
		if err != nil {
			return nil, err
		}
		opts.readOnly = &value
	}
	for _, mapping := range []struct {
		param string
		field **string
	}{
		{nfsMaprootUserParam, &opts.maprootUser},
		{nfsMaprootGroupParam, &opts.maprootGroup},
		{nfsMapallUserParam, &opts.mapallUser},
		{nfsMapallGroupParam, &opts.mapallGroup},
	} {
		if raw, ok := params[mapping.param]; ok {
			value := strings.TrimSpace(raw)
			*mapping.field = &value
		}
	}
	if raw, ok := params[nfsAllowedNetworksParam]; ok {
		opts.allowedNetworks = splitNFSList(raw)
	}
	if raw, ok := params[nfsAllowedHostsParam]; ok {
		opts.allowedHosts = splitNFSList(raw)
	}
	if err := d.validateNFSFencingConflict(opts); err != nil {
		return nil, err
	}
	if err := d.validateNFSSquashConflict(opts); err != nil {
		return nil, err
	}
	if err := d.validateNFSSquashPartialClear(opts); err != nil {
		return nil, err
	}
	return opts, nil
}

// validateNFSFencingConflict (M1) refuses a StorageClass that sets an export
// allowlist under STRICT fencing, instead of accepting it and throwing it away.
//
// createNFSShareForDataset applies the per-class networks/hosts and then, under
// strict fencing, unconditionally resets both to [] so a new volume starts
// deny-all until ControllerPublishVolume writes a node identity. The two
// parameters are therefore a pure no-op in that mode — an operator who sets them
// gets neither the allowlist they asked for nor any signal that it was dropped.
// Strict fencing owns the allowlist by design, so the conflict is a
// configuration error, not a preference to be silently resolved.
func (d *Driver) validateNFSFencingConflict(opts *nfsShareOptions) error {
	if d.config == nil || d.config.Fencing.Mode != FencingModeStrict {
		return nil
	}
	if opts.allowedNetworks == nil && opts.allowedHosts == nil {
		return nil
	}
	set := make([]string, 0, 2)
	if opts.allowedNetworks != nil {
		set = append(set, nfsAllowedNetworksParam)
	}
	if opts.allowedHosts != nil {
		set = append(set, nfsAllowedHostsParam)
	}
	return status.Errorf(codes.InvalidArgument,
		"StorageClass parameter(s) %s cannot be combined with fencing.mode=strict: strict fencing owns the export allowlist and "+
			"creates every share with empty networks/hosts (deny-all until ControllerPublishVolume grants a node), so these values "+
			"would be silently discarded. Use fencing.mode=additive, or drop the parameter(s)",
		strings.Join(set, ", "))
}

// validateNFSSquashConflict (M2) enforces the TrueNAS rule that maproot_* and
// mapall_* are MUTUALLY EXCLUSIVE: sharing.nfs.create rejects a payload carrying
// both, with an opaque middleware error surfacing as a hard CreateVolume failure.
//
// The trap is that the driver's DEFAULT config sets maproot_user=root /
// maproot_group=wheel, so a StorageClass that innocently sets only nfsMapallUser
// produces a both-fields payload. Validate the EFFECTIVE payload (per-class
// override layered over the global config), not just the parameters, and say
// exactly how to resolve it.
func (d *Driver) validateNFSSquashConflict(opts *nfsShareOptions) error {
	var cfg NFSConfig
	if d.config != nil {
		cfg = d.config.NFS
	}
	maproot := effectiveNFSSquash(opts.maprootUser, cfg.ShareMaprootUser) != "" ||
		effectiveNFSSquash(opts.maprootGroup, cfg.ShareMaprootGroup) != ""
	mapall := effectiveNFSSquash(opts.mapallUser, cfg.ShareMapallUser) != "" ||
		effectiveNFSSquash(opts.mapallGroup, cfg.ShareMapallGroup) != ""
	if !maproot || !mapall {
		return nil
	}
	return status.Errorf(codes.InvalidArgument,
		"NFS export maproot_* and mapall_* are mutually exclusive in TrueNAS, but this StorageClass resolves to both "+
			"(maproot_user=%q maproot_group=%q mapall_user=%q mapall_group=%q). sharing.nfs.create would reject the payload. "+
			"Set %s=\"\" and %s=\"\" on the StorageClass to drop the inherited nfs.shareMaproot* defaults, or do not set %s/%s",
		effectiveNFSSquash(opts.maprootUser, cfg.ShareMaprootUser),
		effectiveNFSSquash(opts.maprootGroup, cfg.ShareMaprootGroup),
		effectiveNFSSquash(opts.mapallUser, cfg.ShareMapallUser),
		effectiveNFSSquash(opts.mapallGroup, cfg.ShareMapallGroup),
		nfsMaprootUserParam, nfsMaprootGroupParam, nfsMapallUserParam, nfsMapallGroupParam)
}

// validateNFSSquashPartialClear closes the half of the squash preflight the
// v1.5.0 drill walked straight through. A squash GROUP is meaningless without
// its user: `sharing.nfs.create` rejects the pair, and the operator sees the
// same opaque middleware error the GF5 validator was written to eliminate.
//
// Probed live on TrueNAS 26.0 (2026-08-02): a StorageClass setting
// `nfsMaprootUser: ""` while the shipped `nfs.shareMaprootGroup: wheel` default
// stayed in place produced
// `failed to create NFS share: TrueNAS API error [-32602]: Invalid params`.
// That is the shape an operator naturally produces by following the documented
// "clear the defaults" escape halfway. The mapall_* pair is refused on the same
// grounds: the group is an attribute of the user mapping, so it cannot stand
// alone.
//
// Both routes to the broken payload are covered, because it is the EFFECTIVE
// payload that is validated: clearing the user with an explicit empty parameter
// over a configured group, and setting only the group on a class whose effective
// user is empty.
func (d *Driver) validateNFSSquashPartialClear(opts *nfsShareOptions) error {
	var cfg NFSConfig
	if d.config != nil {
		cfg = d.config.NFS
	}
	for _, family := range []struct {
		userParam, groupParam string
		user, group           string
	}{
		{
			userParam: nfsMaprootUserParam, groupParam: nfsMaprootGroupParam,
			user:  effectiveNFSSquash(opts.maprootUser, cfg.ShareMaprootUser),
			group: effectiveNFSSquash(opts.maprootGroup, cfg.ShareMaprootGroup),
		},
		{
			userParam: nfsMapallUserParam, groupParam: nfsMapallGroupParam,
			user:  effectiveNFSSquash(opts.mapallUser, cfg.ShareMapallUser),
			group: effectiveNFSSquash(opts.mapallGroup, cfg.ShareMapallGroup),
		},
	} {
		if family.user != "" || family.group == "" {
			continue
		}
		return status.Errorf(codes.InvalidArgument,
			"NFS export %s=%q has no user to attach to: this StorageClass resolves to an empty %s, and TrueNAS rejects a squash group "+
				"without its user. Clear BOTH (%s=\"\" and %s=\"\") to drop the inherited nfs.share* defaults, or set %s to the user the "+
				"group belongs to",
			family.groupParam, family.group, family.userParam,
			family.userParam, family.groupParam, family.userParam)
	}
	return nil
}

// effectiveNFSSquash resolves what a squash field will actually be on the wire:
// the per-StorageClass override when present, otherwise the global config value.
func effectiveNFSSquash(override *string, configured string) string {
	if override != nil {
		return strings.TrimSpace(*override)
	}
	return strings.TrimSpace(configured)
}

// validateNFSSecurity rejects unusable Kerberos security fail-closed. KRB5* is
// meaningless (and breaks every mount) without nfs.config v4_krb plus a keytab,
// neither of which the driver can provision, so it demands an explicit
// `nfs.krbEnabled` acknowledgement from the operator.
func (d *Driver) validateNFSSecurity(security []string) error {
	krbEnabled := d.config != nil && d.config.NFS.KrbEnabled
	if _, err := normalizeNFSSecurityList("StorageClass parameter "+nfsSecurityParam, security, krbEnabled); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

// normalizeNFSSecurityList is THE single fail-closed gate for the NFS export
// `security` list. It is deliberately shared by EVERY path that can put a value
// into sharing.nfs.create's `security` field:
//
//   - the StorageClass `nfsSecurity` parameter (parseNFSShareOptions);
//   - the GLOBAL `nfs.shareSecurity` config key, both at config load
//     (validateConfig, so a hand-written ConfigMap refuses to start the
//     controller) and again where it is applied (createNFSShareForDataset),
//     defense in depth for a Config assembled in-process rather than parsed.
//
// It uppercases and de-duplicates, rejects anything outside the middleware's
// enum, and rejects KRB5* unless the operator has explicitly acknowledged that
// Kerberos really is configured (nfs.krbEnabled). A krb-only export on a box
// with no KDC/keytab makes EVERY mount of it fail with an opaque server error,
// so silently stamping one on every newly created export fleet-wide is far
// worse than refusing to start.
//
// source names the origin of the value for the error message. The returned
// error is a plain error; callers on a gRPC path wrap it with a code.
func normalizeNFSSecurityList(source string, security []string, krbEnabled bool) ([]string, error) {
	result := make([]string, 0, len(security))
	seen := make(map[string]struct{}, len(security))
	for _, item := range security {
		mode := strings.ToUpper(strings.TrimSpace(item))
		if mode == "" {
			continue
		}
		if _, ok := validNFSSecurityModes[mode]; !ok {
			return nil, fmt.Errorf("invalid %s value %q; valid options are: %s",
				source, item, strings.Join(sortedNFSSecurityModes(), ", "))
		}
		if strings.HasPrefix(mode, "KRB5") && !krbEnabled {
			return nil, fmt.Errorf(
				"%s requests NFS share security %q, which requires Kerberos on the TrueNAS NFS service; "+
					"set nfs.krbEnabled=true to acknowledge that nfs.config v4_krb and a keytab are configured. "+
					"Without them every mount of the export fails",
				source, mode)
		}
		if _, duplicate := seen[mode]; duplicate {
			continue
		}
		seen[mode] = struct{}{}
		result = append(result, mode)
	}
	return result, nil
}

func parseNFSSecurityList(param, raw string, krbEnabled bool) ([]string, error) {
	result, err := normalizeNFSSecurityList("StorageClass parameter "+param, splitNFSList(raw), krbEnabled)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(result) == 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"StorageClass parameter %s is set but empty; omit it to keep the TrueNAS default", param)
	}
	return result, nil
}

func sortedNFSSecurityModes() []string {
	modes := make([]string, 0, len(validNFSSecurityModes))
	for mode := range validNFSSecurityModes {
		modes = append(modes, mode)
	}
	sort.Strings(modes)
	return modes
}

func parseNFSBoolParam(param, raw string) (bool, error) {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, status.Errorf(codes.InvalidArgument,
			"invalid StorageClass parameter %s value %q; expected a boolean", param, raw)
	}
	return value, nil
}

func splitNFSList(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// applyNFSShareOptions layers the resolved per-StorageClass overrides onto the
// create payload the driver already built. It is a strict no-op for a nil/empty
// options set, which is the invariant that keeps existing volumes byte-identical.
func applyNFSShareOptions(params *truenas.NFSShareCreateParams, opts *nfsShareOptions) {
	if params == nil || opts.empty() {
		return
	}
	if len(opts.security) > 0 {
		params.Security = append([]string(nil), opts.security...)
	}
	if opts.exposeSnapshots != nil {
		params.ExposeSnapshots = *opts.exposeSnapshots
	}
	if opts.readOnly != nil {
		params.Ro = *opts.readOnly
	}
	if opts.maprootUser != nil {
		params.MaprootUser = *opts.maprootUser
	}
	if opts.maprootGroup != nil {
		params.MaprootGroup = *opts.maprootGroup
	}
	if opts.mapallUser != nil {
		params.MapallUser = *opts.mapallUser
	}
	if opts.mapallGroup != nil {
		params.MapallGroup = *opts.mapallGroup
	}
	// The fencing modes own networks/hosts. A StorageClass override is honored
	// only where the driver would otherwise publish the STATIC config lists;
	// strict mode's deny-all bootstrap and the fencing reconciler stay in charge.
	if opts.allowedNetworks != nil {
		params.Networks = append([]string(nil), opts.allowedNetworks...)
	}
	if opts.allowedHosts != nil {
		params.Hosts = append([]string(nil), opts.allowedHosts...)
	}
}

// ---------------------------------------------------------------------------
// NFS major-version preflight
// ---------------------------------------------------------------------------

// nfsMountVersionFromFlags extracts the requested NFS major-version token
// (NFSV3/NFSV4) from a StorageClass's mountOptions. Both `vers=` and `nfsvers=`
// are recognized (the kernel accepts either); `4.1`/`4.2` map to NFSV4 because
// the server-side protocol list is major-version only. It returns "" when the
// class pins no version, in which case there is nothing to preflight.
func nfsMountVersionFromFlags(flags []string) (token, raw string) {
	for _, flag := range flags {
		key, value, found := strings.Cut(strings.TrimSpace(flag), "=")
		if !found {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "vers", "nfsvers":
		default:
			continue
		}
		value = strings.TrimSpace(value)
		major, _, _ := strings.Cut(value, ".")
		switch major {
		case "3":
			token, raw = truenas.NFSProtocolV3, value
		case "4":
			token, raw = truenas.NFSProtocolV4, value
		}
	}
	return token, raw
}

// mountFlagsFromCapabilities collects the union of mountOptions across a
// CreateVolume request's capabilities so the controller can inspect the NFS
// version a StorageClass pins. It never mutates them.
func mountFlagsFromCapabilities(capabilities []*csi.VolumeCapability) []string {
	var flags []string
	for _, capability := range capabilities {
		if capability == nil || capability.GetMount() == nil {
			continue
		}
		flags = append(flags, capability.GetMount().GetMountFlags()...)
	}
	return flags
}

// preflightNFSVersion validates a StorageClass's pinned NFS version against the
// server's GLOBAL nfs.config protocols. Default-off: it costs zero API calls
// unless nfs.versionPreflight is enabled AND the class actually pins a version.
//
// CACHE SEMANTICS, because a memoized NEGATIVE is not the same thing as a
// memoized positive. A successful read is cached for the controller's lifetime —
// protocol enablement is operator-driven and effectively static. The rejection
// below, though, TELLS the operator to change that very setting, so keeping it
// cached across the fix makes the next CreateVolume repeat the same message,
// instructing them to do the thing they just did, until the controller pod is
// restarted. The rejection path therefore INVALIDATES the cache: the next
// attempt re-reads nfs.config and provisioning recovers with no restart.
func (d *Driver) preflightNFSVersion(ctx context.Context, mountFlags []string) error {
	if d.config == nil || !d.config.NFS.VersionPreflight {
		return nil
	}
	token, raw := nfsMountVersionFromFlags(mountFlags)
	if token == "" {
		return nil
	}
	cfg, err := d.nfsServiceConfig(ctx)
	if err != nil {
		// A preflight is an advisory guard, never a new provisioning failure mode:
		// if the service config cannot be read, fall through to the historical
		// behavior (mount succeeds or fails on its own merits).
		klog.Warningf("NFS version preflight skipped: %v", err)
		return nil
	}
	if cfg.SupportsMajorVersion(token) {
		return nil
	}
	// Drop the cached read the rejection was decided from, so an operator who
	// enables the version is not held behind a stale negative until a restart.
	d.invalidateNFSServiceConfig()
	return status.Errorf(codes.FailedPrecondition,
		"StorageClass mountOptions request NFS version %s but the TrueNAS NFS service only enables %s; enable it in the NFS service (or set nfs.ensureProtocols) before using this class",
		raw, strings.Join(cfg.Protocols, ","))
}

// nfsServiceConfig returns the cached global nfs.config, reading it at most once
// per controller lifetime. NFS protocol enablement is an operator-driven,
// effectively static setting; caching it keeps the preflight off the hot path.
func (d *Driver) nfsServiceConfig(ctx context.Context) (*truenas.NFSServiceConfig, error) {
	d.nfsServiceMu.Lock()
	defer d.nfsServiceMu.Unlock()
	if d.nfsServiceCfg != nil {
		return d.nfsServiceCfg, nil
	}
	cfg, err := d.truenasClient.NFSServiceConfig(ctx)
	if err != nil {
		return nil, err
	}
	d.nfsServiceCfg = cfg
	return cfg, nil
}

// invalidateNFSServiceConfig drops the memoized nfs.config so the next reader
// goes back to the appliance.
func (d *Driver) invalidateNFSServiceConfig() {
	d.nfsServiceMu.Lock()
	d.nfsServiceCfg = nil
	d.nfsServiceMu.Unlock()
}

// ensureNFSProtocols is the HARD-GATED, default-off managed enablement path. It
// mutates the GLOBAL NFS service so the configured major versions are enabled.
//
// HARD RULE: this is a service-wide write affecting every export on the
// appliance. It runs ONLY when nfs.ensureProtocols is non-empty, only adds
// versions (never removes one), and is a strict no-op when the server already
// enables everything requested.
func (d *Driver) ensureNFSProtocols(ctx context.Context) error {
	if d.config == nil || len(d.config.NFS.EnsureProtocols) == 0 || !d.config.NFS.Enabled {
		return nil
	}
	desired := make([]string, 0, len(d.config.NFS.EnsureProtocols))
	for _, protocol := range d.config.NFS.EnsureProtocols {
		token := strings.ToUpper(strings.TrimSpace(protocol))
		switch token {
		case truenas.NFSProtocolV3, truenas.NFSProtocolV4:
			desired = append(desired, token)
		default:
			return fmt.Errorf("invalid nfs.ensureProtocols entry %q; valid options are %s, %s",
				protocol, truenas.NFSProtocolV3, truenas.NFSProtocolV4)
		}
	}

	cfg, err := d.nfsServiceConfig(ctx)
	if err != nil {
		return err
	}
	// FAIL CLOSED (M3). "Only adds, never removes" is only true if the CURRENT
	// list is known COMPLETELY: nfs.update {protocols: X} SETS the list, it does
	// not union with it. Two ways the base can fail to be a complete picture, and
	// BOTH are refused:
	//
	//   - the list is empty — the field is absent, or the appliance genuinely
	//     enables nothing;
	//   - the list parsed only PARTIALLY (ProtocolsComplete=false) — a non-list
	//     container, or ANY item the reader could not turn into a token, e.g. a
	//     reshaped ["NFSV4", {"name":"NFSV5"}] on a future TrueNAS. Merging into a
	//     half-read base writes a list missing exactly the entries the reader
	//     could not see, silently DISABLING them.
	//
	// Either way the write would disable every major version missing from the
	// merged list appliance-wide and break every export, driver-managed or not.
	// An incompletely-read service config is never a safe basis for a
	// service-wide REPLACEMENT write.
	if !cfg.ProtocolsComplete || len(cfg.Protocols) == 0 {
		reason := cfg.ProtocolsAnomaly
		if reason == "" {
			reason = "the TrueNAS nfs.config reported an empty protocols list"
		}
		return fmt.Errorf(
			"nfs.ensureProtocols refused: %s, so the driver cannot prove which major versions are currently enabled; "+
				"writing the configured list %v would DISABLE every version missing from it for every export on the appliance",
			reason, desired)
	}

	merged := append([]string(nil), cfg.Protocols...)
	changed := false
	for _, token := range desired {
		if containsString(merged, token) {
			continue
		}
		merged = append(merged, token)
		changed = true
	}
	if !changed {
		klog.V(2).Infof("nfs.ensureProtocols: NFS service already enables %v", cfg.Protocols)
		return nil
	}
	sort.Strings(merged)
	klog.Warningf("nfs.ensureProtocols is set: updating the GLOBAL TrueNAS NFS service protocols %v -> %v (affects every export on this appliance)", cfg.Protocols, merged)
	updated, err := d.truenasClient.NFSServiceUpdate(ctx, map[string]interface{}{"protocols": merged})
	if err != nil {
		return fmt.Errorf("nfs.ensureProtocols update failed: %w", err)
	}
	d.nfsServiceMu.Lock()
	d.nfsServiceCfg = updated
	d.nfsServiceMu.Unlock()
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Node-side mount-option sanity
// ---------------------------------------------------------------------------

// warnOnNFSMountFlagConflicts logs (and NEVER drops or rewrites) mount options
// that are self-contradictory or silently ignored by the kernel. Rewriting a
// user's mountOptions at stage time would be a surprising behavior change on
// existing volumes; a log line is enough to explain a confusing mount.
func warnOnNFSMountFlagConflicts(flags []string) {
	versions := make([]string, 0, 2)
	nconnect := ""
	for _, flag := range flags {
		key, value, found := strings.Cut(strings.TrimSpace(flag), "=")
		if !found {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "vers", "nfsvers":
			value = strings.TrimSpace(value)
			if !containsString(versions, value) {
				versions = append(versions, value)
			}
		case "nconnect":
			nconnect = strings.TrimSpace(value)
		}
	}
	if len(versions) > 1 {
		klog.Warningf("NFS mountOptions request conflicting versions %v; the kernel applies the LAST one. Passing them through unchanged.", versions)
	}
	if nconnect != "" && len(versions) == 1 && strings.HasPrefix(versions[0], "3") {
		klog.Warningf("NFS mountOptions set nconnect=%s with vers=%s; nconnect multi-connection transport is an NFSv4.1+ feature and is ignored on v3. Passing it through unchanged.", nconnect, versions[0])
	}
}

// nfsMountFlags is the NFS staging path's mount-option accessor: the exact
// de-duplicated list volumeMountFlags has always produced, plus an advisory
// sanity pass. The returned slice is unchanged by the sanity pass.
func nfsMountFlags(volCap *csi.VolumeCapability) []string {
	flags := volumeMountFlags(volCap)
	warnOnNFSMountFlagConflicts(flags)
	return flags
}
