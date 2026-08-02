package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
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

// validateNFSACLContentSource rejects an ACL request the driver cannot honestly
// satisfy on a volume materialized from a content source (H3).
//
// acltype/aclmode are set exactly once, in the pool.dataset.create payload
// (applyDatasetACLParams -> createDataset). A snapshot clone, a volume clone and
// a detached replication copy all bypass createDataset entirely and accept no
// property payload, so the resulting dataset carries the ORIGIN's acltype and
// aclmode. nfsACLMode exists for exactly one purpose — opting into
// aclmode=RESTRICTED so an fsGroup chmod fails loudly instead of silently
// degrading the ACL — so honoring it "except on restores" would be the worst
// possible outcome: the operator asked for the loud behavior and would silently
// get whatever the origin happened to have.
//
// Only the MODE parameter is refused. nfsACL / nfsACLTemplate stay allowed on a
// content-source volume because filesystem.setacl acts on the materialized path
// and genuinely applies; a VolSync restore into an ACL-managed StorageClass must
// keep working. What the driver stops doing is CLAIMING an aclmode it did not
// set — see applyNFSVolumeACL.
func validateNFSACLContentSource(opts *nfsACLOptions, source *csi.VolumeContentSource) error {
	if opts == nil || opts.aclMode == "" || source == nil {
		return nil
	}
	return status.Errorf(codes.InvalidArgument,
		"StorageClass parameter %s=%q cannot be honored for a volume provisioned from a %s content source: "+
			"aclmode is fixed in the dataset CREATE payload, and a clone/restore inherits the origin dataset's "+
			"acltype/aclmode instead. Remove %s from the StorageClass used for restores (the ACL itself is still "+
			"applied), or restore into an empty volume provisioned by a class that sets it and copy the data in",
		nfsACLModeParam, opts.aclMode, contentSourceKind(source), nfsACLModeParam)
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
//
// CONTENT-SOURCE VOLUMES (H3). contentSource is non-nil when the dataset was
// materialized by a clone or a replication copy. Those paths never reach
// createDataset, so applyDatasetACLParams never ran and the dataset carries the
// ORIGIN's acltype/aclmode. The DACL below is still applied — filesystem.setacl
// acts on the materialized path and genuinely works — but this function must not
// report an aclmode the driver did not set. On the fresh path the reported mode
// IS the applied state: it is the value sent in the pool.dataset.create payload
// that created this dataset, so a successful create is proof it took effect.
func (d *Driver) applyNFSVolumeACL(
	ctx context.Context,
	ds *truenas.Dataset,
	datasetName string,
	eventObject runtime.Object,
	contentSource *csi.VolumeContentSource,
) {
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

	aclModeReport := aclModeStatement(opts, contentSource)
	klog.Infof("Applied NFSv4 ACL (%s) to %s (%s)", opts.describe(), datasetName, aclModeReport)
	d.recordNormalEvent(eventObject, EventReasonNFSACLApplied,
		fmt.Sprintf("Applied NFSv4 ACL (%s) to %s (%s, nfs41_flags.protected=true — inheritance suppression, not a chmod guard)",
			opts.describe(), datasetName, aclModeReport))
	d.warnACLFsGroupConflict(eventObject, datasetName, opts, contentSource)
}

// aclModeStatement reports what the driver ACTUALLY did about aclmode, never
// merely what the StorageClass asked for (H3).
func aclModeStatement(opts *nfsACLOptions, contentSource *csi.VolumeContentSource) string {
	if contentSource == nil {
		return fmt.Sprintf("aclmode=%s and acltype=NFSV4 set by the driver in the dataset create payload",
			opts.resolvedACLMode())
	}
	return fmt.Sprintf(
		"acltype/aclmode NOT set by the driver: this volume was materialized from a %s content source, which accepts no "+
			"property payload, so it inherits the ORIGIN dataset's acltype and aclmode — the driver cannot state which they are",
		contentSourceKind(contentSource))
}

// warnACLFsGroupConflict emits the fsGroupPolicy hazard warning. fsGroupPolicy
// is a driver-global, effectively-immutable CSIDriver field, so it cannot be
// decided per StorageClass; the driver refuses to flip the shipped default
// (that would change fsGroup semantics for EVERY existing volume) and warns
// instead.
func (d *Driver) warnACLFsGroupConflict(
	eventObject runtime.Object,
	datasetName string,
	opts *nfsACLOptions,
	contentSource *csi.VolumeContentSource,
) {
	lever := "The dataset's aclmode is PASSTHROUGH, so that chmod SUCCEEDS and the ACL degrades silently; " +
		"nfs41_flags.protected does NOT prevent this (it only suppresses inheritance-driven recomputation). " +
		"Set the StorageClass parameter nfsACLMode=RESTRICTED to make such a chmod fail with EPERM instead — " +
		"note that this makes the publish fail loudly rather than degrade quietly."
	switch {
	case contentSource != nil:
		// H3: the driver set no aclmode here, so it must not state one. Whether the
		// chmod degrades the ACL or fails with EPERM depends on the ORIGIN's aclmode.
		lever = "The driver did NOT set this volume's aclmode: it was materialized from a content source and inherits " +
			"the ORIGIN dataset's aclmode, so whether that chmod degrades the ACL (PASSTHROUGH) or fails with EPERM " +
			"(RESTRICTED) is the origin's setting, not this StorageClass's. nfsACLMode is rejected on content-source " +
			"requests precisely because it cannot be applied here; check the origin dataset's aclmode on the appliance."
	case strings.EqualFold(opts.resolvedACLMode(), "RESTRICTED"):
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
