# Scale CSI Helm Chart

A Helm chart for deploying the Scale CSI driver for TrueNAS SCALE.

## Prerequisites

- Kubernetes 1.20+ — this is the floor the chart actually enforces
  (`Chart.yaml` `kubeVersion: ">=1.20.0-0"`), consistent with the pinned
  external-provisioner v6.3 and external-snapshotter v8.6 (both document a
  Kubernetes 1.20 minimum). Note that `CSIDriver.spec.storageCapacity` is
  immutable on Kubernetes 1.20–1.22, so toggling `capacity.enabled` on those
  versions requires a `CSIDriver` delete/recreate; it is mutable from 1.23+.
- Helm 3.8+
- TrueNAS SCALE with API access enabled
- The external snapshot CRDs/controller when `snapshotClass.create` is enabled
- `open-iscsi` on nodes that use iSCSI
- `nvme-cli` on nodes that use NVMe-oF

## Quick start

```bash
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --create-namespace \
  --set truenas.host=truenas.local \
  --set truenas.apiKey=1-xxxxx \
  --set zfs.parentDataset=tank/k8s/volumes
```

The driver supports API-key authentication only. If `truenas.existingSecret` is
used, that Secret must contain an `api-key` key.

### Images and immutable deployment

| Parameter | Description | Default |
|---|---|---|
| `image.repository` | Driver image repository | `ghcr.io/gizmotickler/scale-csi` |
| `image.tag` | Driver tag; empty derives `v<Chart.appVersion>` | `""` |
| `image.digest` | Optional `sha256:...` manifest digest; overrides tag | `""` |
| `sidecars.*.image` | Complete, overridable sidecar image reference | versioned upstream tag |

Every chart container reference is values-controlled. Defaults remain
human-readable tags instead of hard-coded digests so registry mirrors and
operator-managed multi-architecture overrides do not fight the chart. Renovate
is explicitly configured to update every sidecar tag and all full-SHA GitHub
Action pins. For an immutable deployment, set `image.digest` and override each
`sidecars.*.image` with `repository@sha256:<manifest-digest>` after validating
the target architectures. Release signatures and chart provenance can be
verified with the commands in the root README.

## Configuration

### Publication fencing and ownership

| Parameter | Description | Default |
|---|---|---|
| `driverInstanceId` | Stable owner stamped on every driver-created dataset/zvol; empty derives `<csiDriverName>@<zfs.parentDataset>` | `""` |
| `fencing.mode` | Backend publication **enforcement** policy: `off`, `additive`, or `strict`. Publication tracking is always on regardless of mode | `off` |
| `fencing.startupReconcileTimeout` | Timeout for each background startup convergence attempt | `10m` |
| `fencing.staleRecordGracePeriod` | Continuous VA absence before a stale publication record is revoked | `10m` |

`ControllerPublishVolume` always writes a durable publication record on the
volume dataset; these records are the source of truth for CSI single-node
exclusivity, same-node idempotency, stale-record takeover, and empty-node-id
unpublish, and they are maintained in **every** fencing mode (including `off`).
`fencing.mode` only governs whether the backend transport allowlist is also
mutated: in `additive`/`strict`, NVMe-oF authorizes the publishing node's host
NQN, iSCSI authorizes its initiator IQN, and NFS authorizes its node IP after
checking that IP against `nfs.shareAllowedNetworks`; `ControllerUnpublishVolume`
removes that identity. A node's NFS identity IPs are its Kubernetes address
(`status.hostIP`); when nodes reach the NAS over a separate storage network, list
that network in `nfs.nodeIdentityNetworks` so each node also reports its address
there, or the NAS refuses the mount from it. In `off` the allowlists are left untouched and the
publication records alone enforce exclusivity. The durable record also keeps
unpublish possible after the Kubernetes Node has disappeared.
If an operator force-removes a stuck VolumeAttachment finalizer, the periodic
controller reconcile revokes the stale backend grant after
`fencing.staleRecordGracePeriod`. An empty VA list with two or more records
engages a mass-revocation brake and increments
`scale_csi_fencing_stale_deferred_total`.

When a `ControllerPublishVolume` for a new node finds a stale publication record
for another node that has no live VolumeAttachment, the controller takes over
synchronously: it revokes the stale grant and grants the new node. Each
successful takeover increments
`scale_csi_fencing_takeover_total{reason="stale_record"}` and emits a
`FencingTakeover` warning event on the PersistentVolume. This is the most
dangerous operation on a live strict cluster, so alert on a non-zero rate of
this metric to catch unexpected node-identity churn or attachment-controller
misbehavior.

`additive` is the upgrade-safe transition mode when it is enabled in the
required sequence below. It adds per-node entries while
retaining configured/static backend entries and never removes an unknown legacy
entry automatically. `strict` ignores static entries for fenced volumes and
makes the live CSI publications the exact allowlist. `off` preserves the
pre-fencing behavior. Additive preserves broad legacy NFS allow-all shares,
iSCSI allow-all initiator groups, and NVMe allow-any-host policy until strict
cutover; strict replaces them with exact per-volume authorization. If a live
attachment still has a legacy node ID, additive startup reconciliation defers
that per-node fence and increments
`scale_csi_fencing_deferred_total{reason="missing_identity",protocol="..."}`.

> **Required upgrade sequence for v1.2.23:** keep `fencing.mode=off`; upgrade the
> node DaemonSet/image first; wait until every node's CSINode has re-registered;
> then enable `additive`. Move to `strict` only after
> `scale_csi_fencing_deferred_total` stays at zero. Strict mode gates controller
> readiness while background reconciliation retries transient dual-VA states;
> it does not terminate the CSI process.
>
> Roll the ConfigMap and v1.2.23 image together. A ConfigMap containing new
> fencing keys can make older strict-YAML driver pods fail their config parse.

Because the chart uses one image value for controller and node, perform the
node-first step directly before the Helm upgrade (adjust names/namespace if the
release is not named `scale-csi`):

```bash
kubectl -n scale-csi set image daemonset/scale-csi-node \
  scale-csi=ghcr.io/gizmotickler/scale-csi:v1.2.23
kubectl -n scale-csi rollout status daemonset/scale-csi-node
kubectl get csinode -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.drivers[?(@.name=="csi.scale.io")]}{.nodeID}{end}{"\n"}{end}'
```

Every listed driver node ID must begin with `sc1.`. Then update the release
values with the v1.2.23 image, the two new duration keys, and
`fencing.mode=additive`, and run the Helm upgrade. That upgrade rolls the
controller image and ConfigMap as one release operation; do not apply the new
ConfigMap separately to old controller pods.

Additive and strict modes require `controller.replicas=1`. The chart enforces
that invariant and uses a `Recreate` controller rollout so old and new
in-process fencing reconcilers never overlap. Equivalent raw manifests must
provide the same singleton, non-overlapping controller guarantee.

Existing driver-managed datasets from older versions do not have an ownership
stamp. On a legitimate same-name `CreateVolume` retry, the driver automatically
backfills and verifies the stamp only when both local legacy markers
(`scale-csi:managed_resource=true` and the matching
`scale-csi:csi_volume_name`) are
present. A present-but-different owner is always rejected. For datasets without
those local markers, deliberate manual adoption remains available, for example:

```bash
zfs set 'scale-csi:driver_instance_id=csi.scale.io@tank/k8s/volumes' \
  tank/k8s/volumes/pvc-example
```

Verify the dataset, its share/target, and cluster ownership before running that
command. Keep `driverInstanceId` stable across upgrades; changing it creates a
new ownership boundary.

### TrueNAS connection

| Parameter | Description | Default |
|---|---|---|
| `truenas.host` | TrueNAS hostname or IP (required; the driver refuses to start without it) | `""` |
| `truenas.port` | API port | `443` |
| `truenas.secure` | Use HTTPS | `true` |
| `truenas.skipTLSVerify` | Skip TLS verification | `false` |
| `truenas.caCert` | PEM-encoded CA certificate used instead of system roots | `""` |
| `truenas.caCertFile` | Path to a mounted PEM-encoded CA certificate file | `""` |
| `truenas.apiKey` | TrueNAS API key (required unless `existingSecret` is set) | `""` |
| `truenas.existingSecret` | Existing Secret containing `api-key` | `""` |
| `truenas.requestTimeout` | API request timeout in seconds | `60` |
| `truenas.connectTimeout` | Connection timeout in seconds | `10` |
| `truenas.writeTimeout` | WebSocket write timeout in seconds | `30` |
| `truenas.maxConcurrentRequests` | Maximum concurrent API requests | `10` |
| `truenas.maxConnections` | TrueNAS WebSocket connection pool size; chart default `null`/omitted (absent from the ConfigMap for rollback compatibility) → **effective driver default 5**. Accepted explicit range 1–16; explicit `0` or out-of-range fails validation (zero is preserved and rejected, not treated as omitted) | `null` |

### ZFS

| Parameter | Description | Default |
|---|---|---|
| `zfs.parentDataset` | Parent dataset for volumes (required; the driver refuses to start without it) | `""` |
| `zfs.enforceQuota` | Enable dataset quotas | `true` |
| `zfs.detachedVolumesFromSnapshots` | Create independent local send/receive copies from snapshots; volume-source copies remain clones | `false` |
| `zfs.zvolBlocksize` | Block size for zvols | `16K` |
| `zfs.zvolEnableReservation` | Thick-provision zvols with a full refreservation | `false` |
| `zfs.zvolReadyTimeout` | Zvol readiness timeout in seconds | `60` |
| `zfs.datasetProperties` | Additional ZFS dataset properties (e.g. `compression`, `dedup`) | `{}` |
| `zfs.destroyForeignSnapshotsOnDelete` | Allow recursive volume deletion to destroy non-CSI snapshots | `false` |
| `zfs.observeBusyBeforeDelete` | When to log and count whether TrueNAS still sees a dataset in use around its delete (two observation-only scans, about 1.3 s of middleware time; never block the delete): `on-failure` after a delete fails, `always` (or `true`) before every delete, `never` (or `false`) | `on-failure` |

Compression and deduplication are configured through `zfs.datasetProperties`
(e.g. `{compression: "zstd", dedup: "off"}`). When the map is empty, no
properties are set and new datasets inherit them from the parent dataset.

### Protocol configuration

Only enabled protocol blocks are rendered into the driver ConfigMap.

| Parameter | Description | Default |
|---|---|---|
| `nfs.enabled` | Render NFS configuration | `true` |
| `nfs.server` | NFS share host; falls back to `truenas.host` | `""` |
| `nfs.nconnect` | TCP connections per server address (`1..16`); unset omits the mount option | `null` |
| `nfs.trunking` | Opt into NFSv4.1+ multi-address session trunking | `false` |
| `nfs.addresses` | Up to 16 additional NFS server IP literals; required with trunking, with the effective set including the primary capped at 16 | `[]` |
| `nfs.shareAllowedNetworks` | CIDRs allowed to mount created shares | `[]` |
| `nfs.nodeIdentityNetworks` | Storage networks (CIDRs or IPs, up to 16) whose interface addresses each node adds to its node identity, so fencing grants a node mounting over a storage fabric its fabric address; setting it changes those nodes' node IDs | `[]` |
| `nfs.shareMaprootUser` | NFS maproot user | `root` |
| `nfs.shareMaprootGroup` | NFS maproot group | `wheel` |
| `nfs.shareMapallUser` | NFS mapall user | `""` |
| `nfs.shareMapallGroup` | NFS mapall group | `""` |
| `iscsi.enabled` | Render iSCSI configuration | `true` |
| `iscsi.portal` | Target portal host; falls back to `truenas.host` | `""` |
| `iscsi.portalPort` | Target portal port | `3260` |
| `iscsi.multipath` | Associate and log in through every configured portal, then stage the dm-multipath WWID map | `false` |
| `iscsi.portals` | Up to 16 additional iSCSI portal IP literals; required with multipath | `[]` |
| `iscsi.targetGroups` | Static portal/initiator groups for `off`/`additive`; when empty, the portal is resolved and fenced modes create a per-volume initiator group | `[]` |
| `iscsi.extentBlocksize` | Extent block size | `512` |
| `iscsi.extentDisablePhysicalBlocksize` | Disable extent physical-block-size reporting | `false` |
| `iscsi.extentRpm` | Extent RPM value | `SSD` |
| `iscsi.deviceWaitTimeout` | Device wait timeout in seconds | `60` |
| `iscsi.serviceReloadDebounce` | Service reload debounce in milliseconds | `2000` |
| `nvmeof.enabled` | Render NVMe-oF configuration | `false` |
| `nvmeof.transport` | Transport (`tcp` or `rdma`) | `tcp` |
| `nvmeof.address` | Target address; falls back to `truenas.host` | `""` |
| `nvmeof.port` | Target service ID/port | `4420` |
| `nvmeof.subsystemHosts` | Allowed host NQNs | `[]` |
| `nvmeof.subsystemAllowAnyHost` | Allow any host NQN | `false` |
| `nvmeof.dataPath` | Node data path for volumes whose StorageClass does not set `nvmeof/dataPath`: `kernel` or `ublk` | `kernel` |
| `nvmeof.ublk.enabled` | Allow StorageClasses to opt into the ublk data path while the default stays `kernel` | `false` |
| `nvmeof.ublk.maxVolumesPerNode` | Volumes one node must be able to serve through ublk at once (`1..128`); sizes each volume's default layout, and is the advertised CSI volume limit when `dataPath=ublk` | `32` |
| `nvmeof.ublk.queues` | ublk queues per device (`1..4096`); `0` lets the daemon size it for the node | `0` |
| `nvmeof.ublk.depth` | Per-queue ublk depth (`1..4096`); `0` lets the daemon choose | `0` |
| `nvmeof.ublk.zeroCopy` | ublk zero copy; needs kernel >= 6.16 | `true` |
| `nvmeof.ublk.napiUs` | NAPI busy-poll budget in µs while I/O is in flight; `0` disables | `200` |
| `nvmeof.ublk.attachTimeout` | Seconds one attach may take | `60` |
| `nvmeof.ublk.daemon.enabled` | Deploy the `nvmeublkd` DaemonSet whenever the ublk data path is in use; `false` to run the daemon as a host service | `true` |
| `nvmeof.ublk.daemon.image.repository` | nvmeublkd image (published per release tag, amd64) | `ghcr.io/gizmotickler/scale-csi-nvmeublk` |
| `nvmeof.ublk.daemon.image.tag` / `.digest` | Daemon version; defaults to the chart's release (`v<appVersion>`); digest wins | `""` |
| `nvmeof.ublk.daemon.terminationGracePeriodSeconds` | Must cover the daemon's 5 s drain | `15` |
| `nvmeof.ublk.daemon.priorityClassName` | Daemon pod priority | `system-node-critical` |
| `nvmeof.ublk.daemon.resources` | Daemon resources; no memory limit by default | requests `50m` / `256Mi` |
| `nvmeof.ublk.daemon.extraEnv` | Extra daemon environment (`NVMEUBLK_*` tuning) | `[]` |

> `iscsi.extentAvailThreshold` and `nvmeof.commandTimeout` were removed: neither
> was wired to anything (`nvmeof.commandTimeout` is superseded by
> `commandTimeouts.nvme`). The values schema still accepts both keys (ignored) so
> existing values files do not fail validation.

With `fencing.mode=off`, NVMe-oF requires an explicit
`nvmeof.subsystemHosts` allow-list unless `subsystemAllowAnyHost=true`. In
`additive` mode those hosts remain a compatibility allowlist alongside dynamic
publication entries. In `strict` mode they are ignored for fenced volumes.

The iSCSI IQN basename comes from the TrueNAS global iSCSI configuration; it is
not a chart or driver ConfigMap setting. The removed `iscsi.basename` and
`nvmeof.basename` values had no effect.

iSCSI multipath is opt-in. TrueNAS portal objects/listen addresses must already
exist; the controller creates or repairs each volume target's association with
their portal groups using only CSI-owned initiator/auth templates. Legacy
null/zero allow-all groups and operator-scoped groups remain on their original
portal and are never cross-copied; publish fails closed if another portal has no
existing association and no CSI-owned template is available. The node logs into
all advertised portals and stages the dm-multipath map
identified by the LUN's WWID. If device-mapper or `multipathd` is unavailable,
it emits `ISCSIMultipathUnavailable` and safely uses only the primary portal.
This is deliberately unlike NVMe's native kernel multipath. See the production
guide for host prerequisites. iSCSI CHAP remains supported and composes with
every portal. Restrict TCP 3260 to Kubernetes nodes — CHAP authenticates but
does not encrypt the session.

`nfs.nconnect` and `nfs.trunking` are independent. `nconnect=N` creates N TCP
connections to each server address; it is not failover and a single address
still follows one L3 route (though a layer3+4-hashed NAS bond can spread the
flows). Trunking uses `max_connect` plus mounts through the additional addresses
to let a Linux NFSv4.1+ client join transports that the server proves belong to
one server identity. Unsupported clients/servers and negotiated NFS versions
below 4.1 keep the primary mount available and emit a warning Event.

#### Userspace NVMe/TCP data path (ublk)

By default the node plugin stages NVMe-oF volumes with the kernel initiator
(`nvme connect`, native kernel multipath). The ublk data path instead asks
`nvmeublkd`, a per-node daemon, to serve the namespace from userspace as
`/dev/ublkbN` with its own multipath; the node plugin then formats, mounts or
block-publishes that device exactly as it would a kernel namespace. The node
plugin talks to the daemon over the root-only socket
`/run/nvmeublk/nvmeublkd.sock`, which the chart mounts into the node plugin
when ublk is in use.

Selection, per volume, at NodeStage:

1. The StorageClass parameter `nvmeof/dataPath: kernel|ublk`, set through the
   class's `extraParameters`. CreateVolume validates it and records it in the
   PV's volume context, so the volume keeps that data path for its life.
2. Otherwise `nvmeof.dataPath`, the install-wide default.

`nvmeof.ublk.enabled=true` lets classes opt in while the default stays
`kernel`; `nvmeof.dataPath=ublk` implies it. A class that asks for `ublk` on an
install where it is not enabled fails at CreateVolume.

When to use it. The kernel initiator is the default because it is the right
choice for most volumes: it needs nothing extra on the node and costs the
least CPU for light I/O. The ublk data path pays off where the initiator
itself is the bottleneck. Measured on 16-vCPU nodes over four 10 GbE paths to
one TrueNAS target, against a tuned kernel initiator on the same node:

| Workload | ublk vs kernel |
|---|---|
| 4K random read, queue depth 32 x 4 jobs | about 2.5x the IOPS at about 0.6x the CPU per I/O |
| 4K 70/30 mixed at depth | 1.8-2x the IOPS at about 0.8x the CPU per I/O |
| 4K reads next to a busy neighbour on the node | about 2x the IOPS |
| 4K random read, queue depth 64-128 x 8 jobs | 1.15-1.3x (the target is the limit) |
| 4K random write at depth | 1.3-1.7x |
| Large sequential and random reads (128K, 1M) | about 1.05-1.3x (the kernel is already near line rate) |
| 4K-16K read, queue depth 1 | 1.1-1.3x the IOPS at 1.1-1.5x the CPU per I/O |
| Large writes (64K, 128K) | 0.9-1.0x the throughput at 1.2-1.4x the CPU per I/O |
| Synchronous writes (`O_SYNC`, `fsync` per write) | 1.1-1.3x the IOPS at 1.8-2.7x the CPU per I/O |

Your numbers depend on the target, the links and the node; treat the table as
the shape of the trade, not a promise. Volumes that do little I/O gain nothing.

Prerequisites on every node that can stage a ublk volume:

- the `ublk_drv` kernel module, loaded at boot (for example
  `/etc/modules-load.d/ublk.conf`). Zero copy (`nvmeof.ublk.zeroCopy`, default
  on) needs kernel >= 6.16, and the daemon refuses the attach on an older
  kernel, so set it to `false` there. The node-wide reactor pool, which is
  what the figures above were measured with, also needs ublk batch I/O
  (kernel 7.x); on 6.16-6.x a zero-copy volume is served by its own threads;
- `nvmeublkd` running with `/run/nvmeublk` shared with the node plugin. The
  chart deploys it as a DaemonSet as soon as the ublk data path is in use (the
  image is published with every release, amd64 only); set
  `nvmeof.ublk.daemon.enabled=false` to run the same binary as a host service
  instead;
- NVMe/TCP (`nvmeof.transport: tcp`); the daemon speaks nothing else;
- a UUID-form host NQN from `nvme show-hostnqn`, or `/etc/nvme/hostid`. The
  daemon connects with the node's own host NQN and ID, which is what
  publication fencing admits for that node.

Sizing. With zero copy every volume's queues take a range of the daemon's
io_uring buffer tables, which have a fixed size, so a node holds a bounded
number of volumes and the daemon sizes each volume for
`nvmeof.ublk.maxVolumesPerNode`. On a node with 16 or more CPUs:

| `maxVolumesPerNode` | Default layout per volume |
|---|---|
| up to 16 | 8 queues x 256 tags |
| up to 32 (default) | 8 queues x 128 tags |
| up to 64 | 4 queues x 128 tags |
| up to 128 | 2 queues x 128 tags |

Smaller nodes have fewer reactors and queues (one queue per two CPUs, at
least 2) and the same rule applies. The 8 x 128 layout measured the same as
8 x 256 on every workload above. Fewer queues serve fewer concurrent
submitters in parallel and fewer tags bound what a volume can have
outstanding: with 4 x 128, a volume driven past 512 outstanding requests
waits for tags (4K reads at queue depth 128 x 8 jobs measured 0.8x the kernel
initiator there, against 1.15x with eight queues); below that it measured the
same. A node with fewer than 16 CPUs holds at most 64 volumes (2 x 128); 128 is
the most any node holds.

An attach past what fits is refused with an error naming this setting, and
with `nvmeof.dataPath=ublk` and zero copy the node plugin advertises the
budget as the node's CSI volume limit (unless `node.maxVolumesPerNode` is
set), so the scheduler stops placing volumes first. That limit counts every
volume of this driver on the node, whatever its protocol or data path. An
install that only lets classes opt in advertises no limit: keep the ublk
volumes a node can receive within the budget yourself. Size the budget for
the worst case, such as a drained node's volumes landing on the others.

The layout is fixed when a volume is attached: changing the budget, `queues`
or `depth` applies to volumes attached afterwards, and volumes of different
layouts pack less tightly than the table says. Pinning `queues` or `depth`
overrides the budget's sizing: the node then holds what that layout fits,
which can be fewer volumes than `maxVolumesPerNode`. Without zero copy there
is no such bound; each volume then has its own threads and buffers.

The daemon locks its memory. Budget about 80 MiB plus 6 MiB per attached
volume with zero copy (188 MiB with 32 volumes, 340 MiB with 64), or about
75 MiB per volume without. It runs eight reactor threads on a node with 16 or
more CPUs (half the CPUs, at least four) whatever the number of volumes, and
an idle volume keeps one connection per path; a busy one opens up to 16 more
per path and closes them after a minute of idleness.

Making it the default for an install:

```yaml
nvmeof:
  dataPath: ublk
  ublk:
    maxVolumesPerNode: 64
```

That one setting deploys the daemon, mounts its socket into the node plugin
and makes ublk what NodeStage uses. The daemon's image tag follows the chart's
release, so a chart upgrade also rolls the daemon: a handover on each node in
turn, during which that node's ublk volumes pause for a few seconds. To
upgrade the data path on your own schedule, pin `nvmeof.ublk.daemon.image.tag`.

A volume moves to the new default at its next NodeStage (for example when its
pod is rescheduled), not while it is staged; a class that must stay on the
kernel initiator can pin `nvmeof/dataPath: kernel`. Roll the change out a
node at a time and keep the daemon running for as long as any ublk volume is
staged.

Differences from the kernel path: session GC and `fast_io_fail_tmo`
convergence apply to kernel controllers only and never touch a ublk volume;
the daemon detaches a volume at NodeUnstage, and an unreachable daemon fails
the unstage rather than leaking the device. Online expansion of a staged ublk
volume is not possible (the daemon cannot grow a live device): NodeExpand
returns `FailedPrecondition` until the volume is staged again, for example by
restarting the pod. There is no garbage collection of ublk attachments yet.
Portals are fixed at attach: an attach that finds the volume already served
returns the existing device, so addresses added to the publish hint later reach
a ublk volume only when it is staged again (the kernel path converges them).

Turning the ublk data path off takes two steps, because the chart removes the
daemon together with the data path and a volume staged through ublk stops
doing I/O without it:

1. Set `nvmeof.dataPath=kernel` together with `nvmeof.ublk.enabled=true`.
   New stages use the kernel initiator; the daemon and its socket stay for
   the volumes still staged through ublk. Move those off as their pods
   restart (classes pinned to `nvmeof/dataPath: ublk` need a `kernel` class).
2. When `nvmeublk ctl '{"op":"list"}'` in the daemon pod on every node
   reports no devices, set `nvmeof.ublk.enabled=false`.

Do not go from `dataPath=ublk` straight to `kernel` with `ublk.enabled=false`.
While ublk volumes are still staged, the node plugin needs the daemon socket
to unstage, publish and replay them; switching the feature off first strands
those volumes until it is switched back on, and the daemon DaemonSet is
removed with the data path, which stops I/O on them.

Opt-in for one class while the default stays `kernel`:

```yaml
nvmeof:
  enabled: true
  ublk:
    enabled: true
storageClasses:
  - name: scale-nvmeof-ublk
    enabled: true
    protocol: nvmeof
    extraParameters:
      nvmeof/dataPath: ublk
```

#### iSCSI CHAP keys

| Parameter | Description | Default |
|---|---|---|
| `iscsi.chap.enabled` | Opt the controller into CHAP peer management. With `false`, nothing CHAP-related renders into the ConfigMap and targets stay `authmethod=NONE` | `false` |
| `iscsi.chap.tag` | Optional operator-pinned `iscsi.auth` tag; `0` derives a deterministic tag from the username (FNV-1a into `[1000,61000)`) | `0` |
| `iscsi.chap.mutual` | **Currently inert compatibility key** — production code never reads it; the effective per-volume mode is derived from the Secret's `mutualUsername` and stamped immutably. Do not rely on it as a default-mode hint | `false` |

Credentials are never set in chart values — they are supplied per StorageClass
via a Kubernetes Secret referenced by `storageClasses[].chapSecretName` (see the
StorageClasses table below). Tag derivation is username-based, so two classes
sharing a username/tag share one `iscsi.auth` peer; pin distinct positive Secret
tags for isolation. Full contract in the
[StorageClass CHAP reference](../../docs/reference/storageclass.md#iscsi-chap).

### StorageClasses

`storageClasses` is a list, so one release can create NFS, iSCSI, and NVMe-oF
classes. The driver-specific **ordinary** StorageClass parameters are `protocol`,
`snapshotRestoreMode`, and the internal CHAP opt-in marker `iscsi.chapSecret`;
the standardized CSI **Secret-ref** parameters
(`csi.storage.k8s.io/provisioner-secret-*`, `node-stage-secret-*`) are also
consumed, through the provisioner/node-stage request paths. Other TrueNAS, ZFS,
and protocol settings belong in the driver ConfigMap values above. When multiple
protocols are enabled, `protocol` is required and an omitted value returns
`InvalidArgument` instead of defaulting to NFS.

| Field | Description | Default in bundled class |
|---|---|---|
| `storageClasses[].name` | StorageClass name | `scale-nfs` |
| `storageClasses[].enabled` | Render this class (`false` ships an opt-in example disabled) | `true` |
| `storageClasses[].protocol` | `nfs`, `iscsi`, or `nvmeof` | `nfs` |
| `storageClasses[].snapshotRestoreMode` | `clone` or `detached`: how a snapshot-sourced PVC is provisioned (unset follows `zfs.detachedVolumesFromSnapshots`) | unset |
| `storageClasses[].chapSecretName` | Name of the CHAP Secret; renders **both** the provisioner-secret-name and node-stage-secret-name parameters. Requires `iscsi.chap.enabled` | unset |
| `storageClasses[].chapSecretNamespace` | Namespace for the CHAP Secret refs (renders both namespace parameters); defaults to the release namespace when unset | release namespace |
| `storageClasses[].isDefault` | Add the default-class annotation | `false` |
| `storageClasses[].reclaimPolicy` | `Delete` or `Retain` | `Delete` |
| `storageClasses[].allowVolumeExpansion` | Allow PVC expansion | `true` |
| `storageClasses[].volumeBindingMode` | Kubernetes binding mode | `Immediate` |
| `storageClasses[].mountOptions` | StorageClass mount options | `[nfsvers=4, noatime]` |
| `storageClasses[].extraParameters` | Additional CSI parameters such as secret references | `{}` |

`snapshotRestoreMode` chooses how a volume is provisioned from a snapshot
content source: `clone` keeps a cheap ZFS clone that shares blocks (and a
snapshot lifecycle) with its source, while `detached` builds an independent
local send/receive copy. Leave it unset to follow the global
`zfs.detachedVolumesFromSnapshots` default. Use `detached` for DR-restore
classes whose restored volumes must be fully independent; keep the dominant
hourly VolSync source-backup mounts on the default clone path so they stay
cheap.

Example:

```yaml
storageClasses:
  - name: scale-nfs
    protocol: nfs
    isDefault: true
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: [nfsvers=4, noatime]
    extraParameters: {}
  - name: scale-iscsi
    protocol: iscsi
    isDefault: false
    reclaimPolicy: Retain
    allowVolumeExpansion: true
    volumeBindingMode: WaitForFirstConsumer
    mountOptions: []
    extraParameters: {}
  # Opt-in DR-restore class: independent detached copies from snapshots.
  - name: scale-nvmeof-detached
    enabled: false
    protocol: nvmeof
    snapshotRestoreMode: detached
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: []
    extraParameters: {}
```

The old `storageClass` map remains supported for compatibility. When it is
non-empty, it takes precedence over the `storageClasses` list and renders one
class. The shared schema/template path accepts the former `create`, `name`,
`protocol`, `isDefault`, `reclaimPolicy`, `allowVolumeExpansion`,
`volumeBindingMode`, and `mountOptions` fields plus `extraParameters`, and also
the same `enabled`, `snapshotRestoreMode`, `chapSecretName`, and
`chapSecretNamespace` fields as a list entry. Note the semantic distinction:
legacy `create` and list-style `enabled` both gate rendering but are separate
fields. The deprecated path emits `protocol` and `mountOptions` only when they
are explicitly set. Migrate existing values files to `storageClasses` when
convenient.

### Snapshots

| Parameter | Description | Default |
|---|---|---|
| `snapshotClass.create` | Create a VolumeSnapshotClass | `false` |
| `snapshotClass.name` | VolumeSnapshotClass name | `scale-csi` |
| `snapshotClass.deletionPolicy` | `Delete` or `Retain` | `Delete` |
| `snapshotClass.labels` | Additional labels | `{}` |
| `snapshotClass.annotations` | Additional annotations | `{}` |

### Capacity-aware scheduling

CSIStorageCapacity tracking is strictly opt-in (default off); the default render
stays byte-identical without it. The external volume-health monitor sidecar was
removed with CSI spec v1.13, which dropped the `VolumeCondition` field it read;
`sidecars.healthMonitor` is still accepted and ignored.

| Parameter | Description | Default |
|---|---|---|
| `capacity.enabled` | Advertise `CSIDriver.spec.storageCapacity=true`, run the external-provisioner capacity controller, add its `csistoragecapacities` RBAC | `false` |
| `capacity.forImmediateBinding` | Also publish `CSIStorageCapacity` for `Immediate` classes (renders `--capacity-for-immediate-binding`); non-scheduler consumers only, no effect unless `capacity.enabled` | `false` |
| `capacity.reportMaximumVolumeSize` | Set `GetCapacityResponse.maximum_volume_size` to the parent's available bytes; appropriate **only** for thick/reserved zvol deployments, not thin overcommit | `false` |
| `capacity.gaugeEnabled` | Run a controller-only poll loop exporting `scale_csi_pool_available_bytes` / `scale_csi_pool_capacity_bytes` | `false` |
| `capacity.gaugeInterval` | Gauge cadence; values below `30s` clamp to `30s` | `60s` |

Operator caveats:

- **Scheduler prerequisite (WFFC).** external-provisioner publishes
  `CSIStorageCapacity` **only** for `WaitForFirstConsumer` classes, and the
  scheduler consults capacity only for WFFC binding. Enabling `capacity.enabled`
  against the `Immediate` bundled class starts the controller but creates no
  capacity objects.
- **Gauge/API cost.** `GetCapacity` is one `pool.dataset.query` per referencing
  class; the gauge loop performs one parent query per interval **per controller
  replica** (no leader-election gate) — the supported topology is `replicas=1`.
- **Disable cleanup.** Flipping capacity off can leave owner-referenced
  `CSIStorageCapacity` objects until the controller Deployment is deleted or they
  are removed manually. `CSIDriver.spec.storageCapacity` is mutable on Kubernetes
  1.23+ (immutable on 1.20–1.22).

### Workloads, RBAC, and metrics

| Parameter | Description | Default |
|---|---|---|
| `controller.enabled` | Deploy the controller | `true` |
| `controller.replicas` | Controller replicas; must be `1` for additive/strict fencing | `1` |
| `controller.priorityClassName` | Controller priority class | `system-cluster-critical` |
| `controller.podDisruptionBudget.enabled` | Create a controller PDB when replicas > 1 | `true` |
| `controller.podDisruptionBudget.minAvailable` | PDB minimum available | `""` |
| `controller.podDisruptionBudget.maxUnavailable` | PDB maximum unavailable | `1` |
| `controller.containerSecurityContext` | Controller driver container security context | non-root UID 65532, read-only root filesystem, no capabilities |
| `controller.podSecurityContext` | Controller-only pod security context, deep-merged over the shared `podSecurityContext` (controller keys win); the node DaemonSet deliberately gets no pod-level seccomp profile | `{seccompProfile: {type: RuntimeDefault}}` |
| `controller.resources` | Controller driver resources | requests `10m` CPU, `32Mi` memory; memory limit `256Mi` |
| `node.enabled` | Deploy the node DaemonSet | `true` |
| `node.implementation` | Node plugin binary: `rust` (scale-csi-node, the Rust node agent) or `go` (scale-csi). Both serve NVMe-oF, iSCSI and NFS and adopt what the other staged | `rust` |
| `node.rustNodes` | With `node.implementation: go`: node names that run the Rust agent in a second DaemonSet (`<fullname>-node-rust`) while the Go DaemonSet avoids them, a canary. Not with `node.implementation: rust` or `node.affinity` | `[]` |
| `node.priorityClassName` | Node priority class | `system-node-critical` |
| `node.sessionCleanupDelay` | Stale-session retry delay in milliseconds | `500` |
| `node.maxVolumesPerNode` | Maximum volumes advertised per node; `0` means unlimited/unset | `0` |
| `node.resources` | Node driver resources | requests `10m` CPU, `32Mi` memory; memory limit `256Mi` |
| `kubeletDir` | Host kubelet directory | `/var/lib/kubelet` |
| `serviceAccount.create` | Create component ServiceAccounts | `true` |
| `serviceAccount.controllerName` | Existing/custom controller ServiceAccount | generated or `default` |
| `serviceAccount.nodeName` | Existing/custom node ServiceAccount | generated or `default` |
| `serviceAccount.annotations` | ServiceAccount annotations | `{}` |
| `rbac.create` | Create ClusterRoles and bindings | `true` |
| `podSecurityContext` | Pod-level security context for both workloads | `{runAsNonRoot: false, fsGroup: 0}` |
| `securityContext` | Node driver container security context | privileged with `SYS_ADMIN` |
| `health.cacheTTL` | HTTP health-check cache TTL; `0s` disables reuse. The bundled probes run every 10s, so larger values trade freshness for backend load | `5s` |
| `metrics.enabled` | Create metrics Services | `true` |
| `metrics.port` | Driver health/readiness and metrics port | `9809` |
| `metrics.serviceMonitor.enabled` | Create controller and node ServiceMonitors | `false` |
| `metrics.serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |
| `metrics.serviceMonitor.interval` | Prometheus scrape interval | `30s` |
| `metrics.serviceMonitor.scrapeTimeout` | Prometheus scrape timeout | `10s` |
| `metrics.prometheusRule.enabled` | Create the bundled PrometheusRule | `false` |
| `metrics.prometheusRule.additionalLabels` | Additional PrometheusRule labels | `{}` |
| `metrics.prometheusRule.rules` | Replace the bundled alert rules when non-empty | `[]` |
| `metrics.prometheusRule.poolUsageThreshold` | Used-fraction threshold (0–1) for `ScaleCSIPoolNearFull`; the alert renders only when the bundled PrometheusRule **and** `capacity.gaugeEnabled` are both enabled | `0.85` |
| `metrics.dashboards.enabled` | Create a Grafana dashboard ConfigMap | `false` |
| `metrics.dashboards.annotations` | Dashboard ConfigMap annotations (for example, a folder selector) | `{}` |

The bundled PrometheusRule default render (`metrics.prometheusRule.enabled=true`,
other values at chart defaults) is **20** alerts / **11** `runbook_url`
annotations: `ScaleCSIControllerDown`, `ScaleCSICircuitBreakerOpen`,
`ScaleCSITrueNASConnectionDown`, `ScaleCSIHighTrueNASAPIFailureRate`,
`ScaleCSISustainedLockContention`, `ScaleCSIOperationErrorsSustained`,
`ScaleCSIOperationFailedPreconditionStuck`, `ScaleCSISpentRestoreSnapshotBacklog`,
`ScaleCSISessionGCDisconnects`, `ScaleCSIFencingTakeoverSpike`,
`ScaleCSIFencingProvenanceOverflow`, `ScaleCSIJobDispatcherUnsubscribed`,
`ScaleCSIDeleteResidualCleanupFailing`, `ScaleCSIOrphanVolumesDetected`,
`ScaleCSIOrphanSnapshotsDetected`, `ScaleCSIManualRecoveryTombstones`,
`ScaleCSIRemnantVolumesDetected`, `ScaleCSITombstoneBacklog`,
`ScaleCSITombstoneOldestStuck`, and `ScaleCSIReconcileStalled`. Enabling
`reconcile.delete.enabled` adds the three delete-pass reap alerts
(`ScaleCSITombstoneReapCapped`, `ScaleCSITombstoneReapStale`,
`ScaleCSITombstoneReapNeverRan`) for **23 / 14**. `ScaleCSIPoolNearFull` renders
only with `capacity.gaugeEnabled`; `ScaleCSIVolumeNearQuota` only with
`zfs.reportVolumeUsage`. The full alert → runbook mapping is in
[troubleshooting](../../docs/troubleshooting.md#alerts--runbook). The
dashboard ConfigMap is labeled `grafana_dashboard: "1"` for Grafana sidecar
discovery and uses only metrics exported by the driver.

### Orphan reconcile and guarded cleanup

| Parameter | Description | Default |
|---|---|---|
| `reconcile.enabled` | Run periodic orphan-object detection (plus always-on repair mutations) in the controller. Destructive orphan **deletion** stays gated behind `reconcile.delete.enabled` | `true` |
| `reconcile.interval` | Controller reconcile interval, including replication-job hygiene | `1h` |
| `reconcile.minOrphanAge` | Minimum backend object age before orphan classification | `24h` |
| `reconcile.tombstoneMinAge` | Minimum age of a **ledger-proven** deferred-deletion tombstone before the reaper may destroy it (capped at `minOrphanAge`; scan-fallback tombstones keep the full `minOrphanAge` gate). Keep it well below the cleanup CronJob period — a gate equal to the period stalls a blocked `DeleteVolume` for 24-48h | `1h` |
| `reconcile.alertAfter` | Prometheus alert hold time; keep greater than 2x the interval | `2h5m` |
| `reconcile.spentRestore.enabled` | Classify/reap spent VolSync restore VolumeSnapshots (`volsync-*-dst-dest*`); set false to skip this VolSync-specific classification while other reconcile phases still run | `true` |
| `reconcile.tombstoneReaper.scanFallback.enabled` | Bounded-scan fallback for the tombstone reaper. It runs on **every** enabled pass, independent of the ledger backlog, and issues no separate query (it reuses the pass's already-fetched recursive snapshot set), accepting at most 500 candidates. A candidate is reaped only when it has **no** ledger property at either bookkeeping location **and** carries retained creation-time identity exactly reproducing the driver's nonce-derived tombstone rename (retained snapshot/instance identity, exact tombstone name, local source-instance ownership, age gate, inheritance-mask guard). Recovers tombstones stranded by lost ledger entries | `false` |
| `reconcile.repair.maxPerRun` | Per-helper write cap for always-on (not gated by `delete.enabled`) stamp adoption and property-namespace migration. Each independently receives this full allowance. Separate from `delete.maxPerRun` so raising the gated deletion budget cannot amplify these repair writes | `5` |
| `reconcile.delete.enabled` | Create the opt-in guarded cleanup CronJob | `false` |
| `reconcile.delete.schedule` | Guarded cleanup CronJob schedule. ~6-hourly with a :20 offset (the production cadence); bounds worst-case tombstone wait to 6h against `tombstoneMinAge=1h` without 24 Jobs/day of pod startup + TrueNAS login. A daily pass under VolSync/kopiur churn is what produced a multi-day DeleteVolume stall | `20 4,10,16,22 * * *` |
| `reconcile.delete.maxPerRun` | Per-helper **destructive deletion** allowance (not a single pass-wide ceiling: `deleteDetectedOrphans` shares one counter across orphan volumes/snapshots/tombstones/spent-restore/remnants; share cleanup has its own full allowance; stranded-task sweep is uncapped). Always-on repair writes use `reconcile.repair.maxPerRun`, not this field. Only takes effect where `delete.enabled` is true | `1000` |
| `reconcile.delete.reapRecordPollInterval` | How often the controller re-reads the durable last-reap record from `.csi-bookkeeping`. Only runs when `delete.enabled` is true | `5m` |

Orphan **detection** is enabled by default (the destructive orphan-object
**deletion** stays gated behind `reconcile.delete.enabled`), but a reconcile pass
is **not** wholly read-only: independent of the delete gate it performs always-on
repair mutations — legacy ownership-stamp adoption, stale bookkeeping/marker and
publication repair, and the replication-job sweep's `core.job_abort` (which runs
even when `reconcile.enabled=false`). Detection exports
`scale_csi_orphan_volumes`, `scale_csi_orphan_snapshots`,
`scale_csi_spent_restore_snapshots`, orphan-byte gauges,
`scale_csi_reconcile_last_success_timestamp_seconds`, and
`scale_csi_reconcile_failures_total{phase}`. Driver-owned one-time replication
jobs reaped on request failure, startup, or a periodic pass increment
`scale_csi_replication_jobs_aborted_total{reason}`. Deletion remains
disabled unless `reconcile.delete.enabled=true`. The CronJob invokes
`--mode=reconcile`; backend cleanup always calls the driver's guarded CSI
`DeleteVolume` and `DeleteSnapshot` implementations, so clone, snapshot, and
foreign-snapshot dependency checks still apply. Spent VolSync restore snapshots
(matching `volsync-*-dst-dest*`) are classified whenever their source PVC is no
longer Bound. Classification is read-only and is NOT gated on the global
`zfs.detachedVolumesFromSnapshots` flag, so a StorageClass that opts into
`snapshotRestoreMode=detached` while the global default stays `clone` still has
its spent snapshots detected and reapable. TrueNAS 26.0 cannot persist a
property update on an existing snapshot, so this path performs no backend
writes. Deletion requires
the later of the Kubernetes VolumeSnapshot creation time and backend ZFS
snapshot creation time to exceed `reconcile.minOrphanAge`; clock skew can only
delay cleanup.

The replication-job sweep is always on, even when `reconcile.enabled=false` or
`reconcile.delete.enabled=false`. It calls `core.job_abort` only for active
`replication.run_onetime` jobs whose target is strictly below
`zfs.parentDataset` and which have no matching in-flight marker, or whose source
dataset is provably gone. A missing TARGET dataset is never an abort trigger: a
live detached copy (`only_from_scratch`) deliberately has no target until
`zfs receive` materializes it, whereas the source is present throughout a
legitimate copy. Jobs outside that dataset tree are never touched.

> **DANGER — one parent per cluster:** `zfs.parentDataset` MUST be unique to one
> Kubernetes cluster. Never point two live clusters at the same parent dataset.
> A cluster can only see its own PV and VolumeSnapshot handles, so it would
> classify the other cluster's managed backend objects as orphans.

The PDB renders only when `controller.replicas` is greater than one. Set at most
one of `controller.podDisruptionBudget.minAvailable` or `maxUnavailable`; clear
the default `maxUnavailable` to `""` when selecting `minAvailable`.
ConfigMap and chart-managed Secret checksums are added to both pod
templates so configuration and API-key changes trigger rollouts.

### Resource sizing

Steady-state measurements are approximately 15Mi memory and 1m CPU per driver
container. The chart requests 10m CPU and 32Mi memory with a 256Mi memory limit
for each driver. Every sidecar requests 10m CPU and 32Mi memory with a 128Mi
memory limit. All resource maps remain fully overridable.

When CPU or memory limits are configured, the driver adapts `GOMAXPROCS` and
`GOMEMLIMIT` from its cgroups. CSI liveness reports process health and no longer
depends on TrueNAS reachability, so a NAS blip or slow reconnect does not turn a
tight resource limit into a driver crash loop. Use `/readyz` and
`scale_csi_truenas_connection_status` for backend health.

### Controller sidecars

The provisioner, attacher, resizer, and snapshotter each expose `timeout`,
`workerThreads`, and `extraArgs`. The resizer maps `workerThreads` to its
`--workers` CLI flag; the other three use `--worker-threads`.

Every sidecar receives the hardened `sidecars.securityContext` baseline:
privilege escalation is disabled, the root filesystem is read-only, and all
Linux capabilities are dropped. The controller sidecars and liveness probe also
set `runAsUser: 65532`/`runAsNonRoot: true`; the node registrar stays root because
it creates and removes its registration socket in the root-owned hostPath mounted
at `/registration`. Set `sidecars.<name>.securityContext` to merge an explicit
per-sidecar override when an operator's image or filesystem policy requires it.

Leader-election flags render **unconditionally** on every capable controller
sidecar (provisioner, attacher, resizer, snapshotter, and the optional health
monitor), **including at a single replica** — so an `off`-mode RollingUpdate that
transiently runs two controller pods never has both acting as the active
provisioner/attacher. Only the preferred hostname anti-affinity (and the default
PDB) is conditional on `controller.replicas>1`. Additive/strict fencing still
require exactly one replica because their background reconcilers are singleton
writers.

| Parameter | Default |
|---|---|
| `sidecars.provisioner.timeout` | `300s` |
| `sidecars.provisioner.workerThreads` | `10` |
| `sidecars.provisioner.extraArgs` | `[]` |
| `sidecars.attacher.timeout` | `120s` |
| `sidecars.attacher.workerThreads` | `10` |
| `sidecars.attacher.extraArgs` | `[]` |
| `sidecars.resizer.timeout` | `120s` |
| `sidecars.resizer.workerThreads` | `10` |
| `sidecars.resizer.extraArgs` | `[]` |
| `sidecars.snapshotter.timeout` | `300s` |
| `sidecars.snapshotter.workerThreads` | `10` |
| `sidecars.snapshotter.extraArgs` | `[]` |

### Resilience and command timeouts

| Parameter | Description | Default |
|---|---|---|
| `resilience.circuitBreaker.enabled` | Enable the API circuit breaker | `false` |
| `resilience.circuitBreaker.failureThreshold` | Failures before opening | `5` |
| `resilience.circuitBreaker.timeout` | Open-state timeout in seconds | `30` |
| `resilience.retry.maxAttempts` | Maximum retry attempts | `3` |
| `resilience.retry.initialDelay` | Initial retry delay in milliseconds | `500` |
| `resilience.retry.maxDelay` | Maximum retry delay in milliseconds | `5000` |
| `resilience.retry.backoffMultiplier` | Exponential backoff multiplier | `2.0` |
| `resilience.rateLimiting.maxConcurrentLogins` | Concurrent login limit per portal | `2` |
| `commandTimeouts.mount` | Mount timeout in seconds | `30` |
| `commandTimeouts.format` | Format timeout in seconds | `300` |
| `commandTimeouts.iscsi` | `iscsiadm` timeout in seconds | `10` |
| `commandTimeouts.nvme` | `nvme` timeout in seconds | `30` |

> `resilience.rateLimiting.maxConcurrentRequests` was removed: it was never wired
> to anything. The API concurrency limit is `truenas.maxConcurrentRequests`. The
> values schema still accepts the old key (ignored) so existing values files do
> not fail validation.

### Session garbage collection

| Parameter | Description | Default |
|---|---|---|
| `sessionGC.enabled` | Enable periodic session garbage collection | `true` |
| `sessionGC.interval` | Interval in seconds | `300` |
| `sessionGC.gracePeriod` | Orphan grace period in seconds | `60` |
| `sessionGC.dryRun` | Log without disconnecting sessions | `false` |
| `sessionGC.runOnStartup` | Run once during startup | `true` |
| `sessionGC.startupDelay` | Startup delay in seconds | `5` |
| `sessionGC.iscsiEnabled` | Collect iSCSI sessions | `true` |
| `sessionGC.nvmeofEnabled` | Collect NVMe-oF sessions | `true` |

## Security

- `nfs.shareAllowedNetworks` is the upper bound used to accept dynamically
  reported node IPs. Keep it restricted to trusted node networks.
- With fencing enabled, NVMe-oF and iSCSI authorization is derived from live CSI
  publications. Static `nvmeof.subsystemHosts` entries are retained only by
  additive mode and ignored by strict mode.
- Prefer an externally managed Secret and set `truenas.existingSecret`; the
  Secret must contain `api-key`.
- The node DaemonSet requires a namespace permitted to run at the `privileged`
  Pod Security level because it uses host networking, the host PID namespace,
  privileged execution, and host mounts. For Pod Security Admission, label the
  target namespace with `pod-security.kubernetes.io/enforce: privileged` before
  installing the chart.

## Existing Secret example

```bash
kubectl create secret generic truenas-credentials \
  --namespace scale-csi \
  --from-literal=api-key=1-xxxxx
```

```yaml
truenas:
  host: truenas.local
  existingSecret: truenas-credentials
zfs:
  parentDataset: tank/k8s/volumes
```

## Upgrade and uninstall

The controller keeps publication records as `VolumePublication` objects (CRD
in `crds/`). Helm installs `crds/` on a first install but never on an upgrade:
apply the CRD before upgrading from a release without it, or with Flux set the
HelmRelease's `install.crds` and `upgrade.crds` to `CreateReplace`. The
controller refuses to start without it.

```bash
kubectl apply -f https://raw.githubusercontent.com/GizmoTickler/scale-csi/<tag>/charts/scale-csi/crds/volumepublications.scale-csi.io.yaml
helm upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  -f values.yaml

helm uninstall scale-csi --namespace scale-csi
```

PVCs and their data are not deleted merely because the chart is uninstalled.
