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
)

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
}

func (o *nfsACLOptions) empty() bool {
	return o == nil || (o.template == "" && len(o.dacl) == 0)
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

// applyDatasetACLParams stamps the acltype/aclmode a dataset needs before an
// NFSv4 dacl can be applied. It runs ONLY when an ACL parameter is present; a
// volume without one keeps inheriting both properties from the parent exactly
// as before.
func applyDatasetACLParams(params *truenas.DatasetCreateParams, opts *nfsACLOptions) {
	if params == nil || opts.empty() || params.Type == "VOLUME" {
		return
	}
	if params.Acltype == "" {
		params.Acltype = "NFSV4"
	}
	if params.Aclmode == "" {
		params.Aclmode = "PASSTHROUGH"
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
// at publish, which REWRITES the mode-bearing ACEs applied here. Two mitigations
// are applied: nfs41_flags.protected=true (so a chmod cannot silently recompute
// the ACL from the mode) and a loud Warning event telling the operator to either
// omit fsGroup on ACL-managed workloads or install the driver with the chart's
// csidriver.fsGroupPolicy=None.
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

	// protected=true is the guard rail: it stops the server recomputing the ACL
	// from a subsequent chmod (which fsGroupPolicy=File performs at every publish).
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
		fmt.Sprintf("Applied NFSv4 ACL (%s) to %s with nfs41_flags.protected=true", opts.describe(), datasetName))
	d.warnACLFsGroupConflict(eventObject, datasetName)
}

// warnACLFsGroupConflict emits the fsGroupPolicy hazard warning. fsGroupPolicy
// is a driver-global, effectively-immutable CSIDriver field, so it cannot be
// decided per StorageClass; the driver refuses to flip the shipped default
// (that would change fsGroup semantics for EVERY existing volume) and warns
// instead.
func (d *Driver) warnACLFsGroupConflict(eventObject runtime.Object, datasetName string) {
	message := fmt.Sprintf(
		"Volume %s carries a driver-applied NFSv4 ACL. This driver's CSIDriver.fsGroupPolicy is File, so kubelet recursively chowns/chmods the volume to a Pod's securityContext.fsGroup at publish and will rewrite the mode-bearing ACEs. Use Pods that set no fsGroup, or install the driver with csidriver.fsGroupPolicy=None.",
		datasetName)
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
