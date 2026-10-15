package driver

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// nodeClampNVMeMaxSectors applies nvmeof.maxSectorsKB to one subsystem's head
// and path devices. Seam for tests.
var nodeClampNVMeMaxSectors = util.ClampNVMeoFMaxSectors

// nvmeMaxSectorsPathWait bounds how long a stage waits for every live
// controller of the subsystem to expose its path device. `nvme connect`
// returns before the namespace scan that creates it has finished; a path
// device that appears after the clamp would keep the 32 MiB default, which is
// exactly the device that issued the failing commands in the incident.
const nvmeMaxSectorsPathWait = 10 * time.Second

// nvmeMaxSectorsPolicy is the configured clamp. LoadConfig rejects an invalid
// value, so the fallback only covers a hand-built config: it keeps the
// protective default rather than silently disabling the clamp.
func (d *Driver) nvmeMaxSectorsPolicy() util.NVMeMaxSectorsPolicy {
	if d.config == nil {
		return util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsAuto}
	}
	policy, err := util.ParseNVMeMaxSectorsPolicy(d.config.NVMeoF.MaxSectorsKB)
	if err != nil {
		klog.Warningf("NVMe-oF max_sectors_kb: %v; using auto", err)
		return util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsAuto}
	}
	return policy
}

func recordNVMeMaxSectorsOutcome(result *util.NVMeMaxSectorsClampResult, err error) {
	if changed := result.Changed(); changed > 0 {
		nvmeControllerTunableCorrections.WithLabelValues("max_sectors_kb", "corrected").Add(float64(changed))
	}
	if err != nil {
		nvmeControllerTunableCorrections.WithLabelValues("max_sectors_kb", "error").Inc()
	}
}

// clampNVMeoFMaxSectors puts the request-size clamp in place on a subsystem
// that is about to be (or already is) staged. It runs after every path is
// connected and before format, mount or the raw-block staging link, and on
// every replay of a stage: whichever node agent connected the subsystem, the
// clamp reads and writes kernel state only, so a volume connected by the Go
// plugin and re-staged by the Rust agent (or the other way round) converges
// the same way.
//
// When the clamp is required and cannot be proven in place the stage fails
// (codes.Internal, retried by kubelet) rather than mount a volume whose next
// large write would be rejected by the target and take the filesystem
// read-only.
func (d *Driver) clampNVMeoFMaxSectors(ctx context.Context, nqn, transport string, eventObjects ...runtime.Object) error {
	policy := d.nvmeMaxSectorsPolicy()
	if policy.Mode == util.NVMeMaxSectorsOff {
		klog.V(4).Infof("NVMe-oF max_sectors_kb for %s: clamp disabled (nvmeof.maxSectorsKB=off)", nqn)
		return nil
	}
	result, err := nodeClampNVMeMaxSectors(ctx, nqn, util.NVMeMaxSectorsClampOptions{
		Policy:    policy,
		Transport: transport,
		PathWait:  nvmeMaxSectorsPathWait,
	})
	recordNVMeMaxSectorsOutcome(result, err)
	if err != nil {
		operationErr := status.Errorf(codes.Internal,
			"refusing to stage NVMe-oF subsystem %s without its request-size clamp (nvmeof.maxSectorsKB=%s); kubelet retries the stage: %v [%s]",
			nqn, policy, err, result)
		d.recordWarningEvent(firstEventObject(eventObjects), EventReasonNVMeMaxSectorsClampFailed, operationErr.Error())
		return operationErr
	}
	klog.Infof("NVMe-oF max_sectors_kb for %s: %s", nqn, result)
	return nil
}

// reconcileNVMeoFMaxSectors re-applies the clamp to every subsystem staged
// under this driver, once per session-GC tick. Path devices survive a
// controller reconnect (ctrl_loss_tmo=-1 never deletes a controller, and on
// current kernels the block layer keeps a user-set max_sectors_kb,
// max_user_sectors, across the namespace revalidation), so the only new path devices are those of controllers
// connected after the stage: a re-stage top-up (which clamps inline), or a
// connect made outside this plugin. This pass catches the latter. Ownership is
// positive, as for fast_io_fail_tmo: only subsystems backing a device staged
// under this driver's kubelet staging directory are touched. dryRun skips the
// pass entirely and logs that it did.
func (d *Driver) reconcileNVMeoFMaxSectors(ctx context.Context, dryRun bool) {
	if d.config == nil || d.config.NVMeoF.TransportAddress == "" {
		return
	}
	policy := d.nvmeMaxSectorsPolicy()
	if policy.Mode == util.NVMeMaxSectorsOff {
		return
	}
	owned, err := d.stagedNVMeoFNQNs()
	if err != nil {
		klog.Warningf("NVMe-oF max_sectors_kb: cannot prove subsystem ownership, skipping this pass: %v", err)
		return
	}
	if dryRun {
		if len(owned) > 0 {
			klog.Infof("NVMe-oF max_sectors_kb (dry run): would converge %d staged subsystem(s) to nvmeof.maxSectorsKB=%s", len(owned), policy)
		}
		return
	}
	for nqn := range owned {
		if ctx.Err() != nil {
			return
		}
		result, clampErr := nodeClampNVMeMaxSectors(ctx, nqn, util.NVMeMaxSectorsClampOptions{
			Policy:    policy,
			Transport: d.config.NVMeoF.Transport,
		})
		recordNVMeMaxSectorsOutcome(result, clampErr)
		switch {
		case clampErr != nil:
			klog.Warningf("NVMe-oF max_sectors_kb: %v [%s]", clampErr, result)
		case result.Changed() > 0:
			klog.Infof("NVMe-oF max_sectors_kb for %s (reconcile): %s", nqn, result)
		}
	}
}
