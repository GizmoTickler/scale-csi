package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
)

// StorageClass parameters that request an NFSv4 ACL on a freshly provisioned
// NFS volume. Exactly one may be set.
const (
	nfsACLTemplateParam = "nfsACLTemplate"
	nfsACLParam         = "nfsACL"
	// nfsACLModeParam selects the dataset's `aclmode`, which is the ONLY ZFS
	// property that governs what a chmod does to a non-trivial ACL. See
	// applyDatasetACLParams for why the default is deliberately left unchanged.
	nfsACLModeParam = "nfsACLMode"
)

// validACLModes is the aclmode subset the driver offers.
//
// DISCARD is deliberately NOT offered: it deletes the whole non-trivial ACL on
// the first chmod, which is the worst possible outcome for a feature whose point
// is to keep one.
var validACLModes = map[string]struct{}{
	"PASSTHROUGH": {},
	"RESTRICTED":  {},
}

// defaultACLMode is what the driver has always set alongside acltype=NFSV4, and
// what it keeps setting unless a StorageClass explicitly asks otherwise.
const defaultACLMode = "PASSTHROUGH"

// EventReasonNFSACLApplied / EventReasonNFSACLFailed / EventReasonNFSACLFsGroup
// surface the ACL outcome and the fsGroup hazard on the PVC.
const (
	EventReasonNFSACLApplied = "NFSACLApplied"
	EventReasonNFSACLFailed  = "NFSACLFailed"
	EventReasonNFSACLFsGroup = "NFSACLFsGroupConflict"
)

// supportedACLTemplates is the builtin NFS4 template set TrueNAS ships.
var supportedACLTemplates = map[string]struct{}{
	"NFS4_OPEN":        {},
	"NFS4_RESTRICTED":  {},
	"NFS4_HOME":        {},
	"NFS4_DOMAIN_HOME": {},
	"NFS4_ADMIN":       {},
}

// nfsACLOptions is the resolved ACL request for one CreateVolume. A nil/zero
// value means "no ACL parameter was set", which is the default and leaves both
// the dataset's acltype/aclmode and the volume's permissions exactly as they
// have always been.
type nfsACLOptions struct {
	template string
	dacl     []truenas.ACLEntry
	// aclMode is the requested dataset `aclmode`. Empty means "unset", which
	// resolves to defaultACLMode — the historical value.
	aclMode string
}

func (o *nfsACLOptions) empty() bool {
	return o == nil || (o.template == "" && len(o.dacl) == 0)
}

// resolvedACLMode is the aclmode this option set will stamp on the dataset.
func (o *nfsACLOptions) resolvedACLMode() string {
	if o != nil && o.aclMode != "" {
		return o.aclMode
	}
	return defaultACLMode
}

type nfsACLOptionsContextKey struct{}

func withNFSACLOptions(ctx context.Context, opts *nfsACLOptions) context.Context {
	if opts.empty() {
		return ctx
	}
	return context.WithValue(ctx, nfsACLOptionsContextKey{}, opts)
}

func nfsACLOptionsFromContext(ctx context.Context) *nfsACLOptions {
	opts, _ := ctx.Value(nfsACLOptionsContextKey{}).(*nfsACLOptions)
	return opts
}

// parseNFSACLOptions validates the ACL StorageClass parameters. It never
// contacts the backend: template resolution happens at apply time so a
// non-ACL volume costs zero extra API calls.
func parseNFSACLOptions(params map[string]string) (*nfsACLOptions, error) {
	opts := &nfsACLOptions{}
	if len(params) == 0 {
		return opts, nil
	}
	rawTemplate, hasTemplate := params[nfsACLTemplateParam]
	rawACL, hasACL := params[nfsACLParam]
	if hasTemplate && hasACL {
		return nil, status.Errorf(codes.InvalidArgument,
			"StorageClass parameters %s and %s are mutually exclusive", nfsACLTemplateParam, nfsACLParam)
	}

	rawMode, hasMode := params[nfsACLModeParam]
	if hasMode {
		if !hasTemplate && !hasACL {
			return nil, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %s requires %s or %s: aclmode only matters on a dataset that carries a driver-applied NFSv4 ACL",
				nfsACLModeParam, nfsACLTemplateParam, nfsACLParam)
		}
		mode := strings.ToUpper(strings.TrimSpace(rawMode))
		if _, ok := validACLModes[mode]; !ok {
			return nil, status.Errorf(codes.InvalidArgument,
				"invalid StorageClass parameter %s value %q; valid options are: %s",
				nfsACLModeParam, rawMode, strings.Join(sortedACLModes(), ", "))
		}
		opts.aclMode = mode
	}

	if hasTemplate {
		template := strings.ToUpper(strings.TrimSpace(rawTemplate))
		if template == "" {
			return nil, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %s is set but empty", nfsACLTemplateParam)
		}
		if _, ok := supportedACLTemplates[template]; !ok {
			return nil, status.Errorf(codes.InvalidArgument,
				"invalid StorageClass parameter %s value %q; valid options are: %s",
				nfsACLTemplateParam, rawTemplate, strings.Join(sortedACLTemplates(), ", "))
		}
		opts.template = template
		return opts, nil
	}

	if hasACL {
		var dacl []truenas.ACLEntry
		if err := json.Unmarshal([]byte(rawACL), &dacl); err != nil {
			return nil, status.Errorf(codes.InvalidArgument,
				"invalid StorageClass parameter %s: expected a JSON array of NFSv4 ACL entries: %v", nfsACLParam, err)
		}
		if len(dacl) == 0 {
			return nil, status.Errorf(codes.InvalidArgument,
				"StorageClass parameter %s is set but carries no ACL entries", nfsACLParam)
		}
		for index, entry := range dacl {
			if strings.TrimSpace(entry.Tag) == "" {
				return nil, status.Errorf(codes.InvalidArgument,
					"invalid StorageClass parameter %s: entry %d has no tag", nfsACLParam, index)
			}
		}
		opts.dacl = dacl
	}
	return opts, nil
}

func sortedACLTemplates() []string {
	templates := make([]string, 0, len(supportedACLTemplates))
	for template := range supportedACLTemplates {
		templates = append(templates, template)
	}
	sort.Strings(templates)
	return templates
}

func sortedACLModes() []string {
	modes := make([]string, 0, len(validACLModes))
	for mode := range validACLModes {
		modes = append(modes, mode)
	}
	sort.Strings(modes)
	return modes
}

// applyDatasetACLParams stamps the acltype/aclmode a dataset needs before an
// NFSv4 dacl can be applied. It runs ONLY when an ACL parameter is present; a
// volume without one keeps inheriting both properties from the parent exactly
// as before.
//
// # ACLMODE, AND WHY THE DEFAULT IS NOT CHANGED
//
// `aclmode` is the only ZFS property that governs what a chmod does to a
// non-trivial ACL. Per zfsprops(7):
//
//   - PASSTHROUGH (the historical default, unchanged): "no changes are made to
//     the ACL other than generating the necessary ACL entries to represent the
//     new mode" — the explicit USER/GROUP ACEs survive, the mode-bearing
//     owner@/group@/everyone@ ACEs are REWRITTEN on every chmod.
//   - RESTRICTED: a chmod that would alter the ACL fails with EPERM.
//
// Under CSIDriver.fsGroupPolicy=File, kubelet's SetVolumeOwnership issues a
// recursive chmod at publish, so PASSTHROUGH silently degrades a driver-applied
// ACL and RESTRICTED is the only lever that stops it.
//
// The default is nonetheless left at PASSTHROUGH, deliberately: flipping it
// would convert a silent, recoverable ACL degradation into a HARD publish
// failure for every fsGroup Pod on an ACL volume — and for any in-container
// chmod, which plenty of images do at startup — turning an explicitly
// best-effort feature (a failed setacl never blocks a bind) into a new
// mount-time outage class. Operators who want the loud behavior opt in per
// StorageClass with nfsACLMode=RESTRICTED.
func applyDatasetACLParams(params *truenas.DatasetCreateParams, opts *nfsACLOptions) {
	if params == nil || opts.empty() || params.Type == "VOLUME" {
		return
	}
	if params.Acltype == "" {
		params.Acltype = "NFSV4"
	}
	if params.Aclmode == "" {
		params.Aclmode = opts.resolvedACLMode()
	}
}

// applyNFSVolumeACL resolves and applies the requested NFSv4 ACL to a freshly
// provisioned NFS volume's dataset.
//
// It is deliberately BEST-EFFORT: an ACL failure produces a Warning event and a
// log line, never a failed CreateVolume. The volume is already provisioned,
// stamped and exported at this point; refusing to bind it because a cosmetic
// permission model could not be applied would be a strictly worse outcome, and
// the ACL can be re-applied out of band (risk R7).
//
// fsGroup HAZARD (risk R2): the shipped CSIDriver sets fsGroupPolicy=File, so
// kubelet recursively chown/chmods the volume to a Pod's securityContext.fsGroup
// at publish, which REWRITES the mode-bearing ACEs applied here.
//
// The REAL mitigations are workload-side and are stated in the Warning event
// below: run ACL-managed workloads with no securityContext.fsGroup, or install
// the driver with csidriver.fsGroupPolicy=None. The only SERVER-side lever that
// actually stops a chmod from touching the ACL is aclmode=RESTRICTED, offered
// per StorageClass via nfsACLMode (see applyDatasetACLParams for why it is not
// the default).
//
// nfs41_flags.protected is NOT a chmod guard. It is NFSv4.1 ACL4_PROTECTED /
// ZFS ZFS_ACL_PROTECTED, whose defined meaning (RFC 5661 §6.4.3.2, OpenZFS
// zfs_acl_inherit) is automatic-INHERITANCE suppression: "this ACL was set
// explicitly, do not re-derive it from the parent". It is not consulted on the
// chmod/SETATTR-mode path at all. It is set here because suppressing inheritance
// IS the correct semantic for an explicitly-applied ACL — not because it defends
// against fsGroup.
func (d *Driver) applyNFSVolumeACL(ctx context.Context, ds *truenas.Dataset, datasetName string, eventObject runtime.Object) {
	opts := nfsACLOptionsFromContext(ctx)
	if opts.empty() {
		return
	}
	path := ""
	if ds != nil {
		path = ds.Mountpoint
	}
	if path == "" {
		klog.Warningf("Skipping NFSv4 ACL for %s: dataset has no mountpoint", datasetName)
		d.recordWarningEvent(eventObject, EventReasonNFSACLFailed,
			fmt.Sprintf("Skipped NFSv4 ACL for %s: dataset has no mountpoint", datasetName))
		return
	}

	dacl := opts.dacl
	if opts.template != "" {
		resolved, err := d.truenasClient.ACLTemplateDACL(ctx, opts.template)
		if err != nil {
			klog.Warningf("Failed to resolve ACL template %s for %s: %v", opts.template, datasetName, err)
			d.recordWarningEvent(eventObject, EventReasonNFSACLFailed,
				fmt.Sprintf("Failed to resolve ACL template %s for %s: %v", opts.template, datasetName, err))
			return
		}
		dacl = resolved
	}

	// protected=true marks the ACL as explicitly set so the server does not
	// re-derive it from the parent's inheritable ACEs (ACL4_PROTECTED). It does
	// NOT protect against chmod — see the doc comment above.
	setErr := d.truenasClient.FilesystemSetACL(ctx, &truenas.SetACLOptions{
		Path:       path,
		DACL:       dacl,
		NFS41Flags: map[string]bool{"protected": true, "autoinherit": false},
	})
	if setErr != nil {
		klog.Warningf("Failed to apply NFSv4 ACL to %s (%s): %v", datasetName, path, setErr)
		d.recordWarningEvent(eventObject, EventReasonNFSACLFailed,
			fmt.Sprintf("Failed to apply NFSv4 ACL to %s: %v", datasetName, setErr))
		return
	}

	klog.Infof("Applied NFSv4 ACL (%s) to %s", opts.describe(), datasetName)
	d.recordNormalEvent(eventObject, EventReasonNFSACLApplied,
		fmt.Sprintf("Applied NFSv4 ACL (%s) to %s (aclmode=%s, nfs41_flags.protected=true — inheritance suppression, not a chmod guard)",
			opts.describe(), datasetName, opts.resolvedACLMode()))
	d.warnACLFsGroupConflict(eventObject, datasetName, opts.resolvedACLMode())
}

// warnACLFsGroupConflict emits the fsGroupPolicy hazard warning. fsGroupPolicy
// is a driver-global, effectively-immutable CSIDriver field, so it cannot be
// decided per StorageClass; the driver refuses to flip the shipped default
// (that would change fsGroup semantics for EVERY existing volume) and warns
// instead.
func (d *Driver) warnACLFsGroupConflict(eventObject runtime.Object, datasetName, aclMode string) {
	lever := "The dataset's aclmode is PASSTHROUGH, so that chmod SUCCEEDS and the ACL degrades silently; " +
		"nfs41_flags.protected does NOT prevent this (it only suppresses inheritance-driven recomputation). " +
		"Set the StorageClass parameter nfsACLMode=RESTRICTED to make such a chmod fail with EPERM instead — " +
		"note that this makes the publish fail loudly rather than degrade quietly."
	if strings.EqualFold(aclMode, "RESTRICTED") {
		lever = "The dataset's aclmode is RESTRICTED, so that chmod FAILS with EPERM and the publish fails loudly " +
			"rather than degrading the ACL silently."
	}
	message := fmt.Sprintf(
		"Volume %s carries a driver-applied NFSv4 ACL. This driver's CSIDriver.fsGroupPolicy is File, so kubelet recursively "+
			"chowns/chmods the volume to a Pod's securityContext.fsGroup at publish, which rewrites the mode-bearing ACEs. %s "+
			"The reliable fixes are workload-side: use Pods that set no fsGroup, or install the driver with csidriver.fsGroupPolicy=None.",
		datasetName, lever)
	klog.Warning(message)
	d.recordWarningEvent(eventObject, EventReasonNFSACLFsGroup, message)
}

func (o *nfsACLOptions) describe() string {
	switch {
	case o == nil:
		return "none"
	case o.template != "":
		return "template " + o.template
	default:
		return fmt.Sprintf("%d inline entries", len(o.dacl))
	}
}
