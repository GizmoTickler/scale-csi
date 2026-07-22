# Scale CSI Helm Chart

A Helm chart for deploying the Scale CSI driver for TrueNAS SCALE.

## Prerequisites

- Kubernetes 1.25+
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
removes that identity. In `off` the allowlists are left untouched and the
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
(`truenas-csi:managed_resource=true` and the matching
`truenas-csi:csi_volume_name`) are
present. A present-but-different owner is always rejected. For datasets without
those local markers, deliberate manual adoption remains available, for example:

```bash
zfs set 'truenas-csi:driver_instance_id=csi.scale.io@tank/k8s/volumes' \
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

Compression and deduplication are configured through `zfs.datasetProperties`
(e.g. `{compression: "zstd", dedup: "off"}`). When the map is empty, no
properties are set and new datasets inherit them from the parent dataset.

### Protocol configuration

Only enabled protocol blocks are rendered into the driver ConfigMap.

| Parameter | Description | Default |
|---|---|---|
| `nfs.enabled` | Render NFS configuration | `true` |
| `nfs.server` | NFS share host; falls back to `truenas.host` | `""` |
| `nfs.shareAllowedNetworks` | CIDRs allowed to mount created shares | `[]` |
| `nfs.shareMaprootUser` | NFS maproot user | `root` |
| `nfs.shareMaprootGroup` | NFS maproot group | `wheel` |
| `nfs.shareMapallUser` | NFS mapall user | `""` |
| `nfs.shareMapallGroup` | NFS mapall group | `""` |
| `iscsi.enabled` | Render iSCSI configuration | `true` |
| `iscsi.portal` | Target portal host; falls back to `truenas.host` | `""` |
| `iscsi.portalPort` | Target portal port | `3260` |
| `iscsi.targetGroups` | Static portal/initiator groups for `off`/`additive`; when empty, the portal is resolved and fenced modes create a per-volume initiator group | `[]` |
| `iscsi.extentBlocksize` | Extent block size | `512` |
| `iscsi.extentDisablePhysicalBlocksize` | Disable extent physical-block-size reporting | `false` |
| `iscsi.extentRpm` | Extent RPM value | `SSD` |
| `iscsi.extentAvailThreshold` | Extent available-space warning threshold | `0` |
| `iscsi.deviceWaitTimeout` | Device wait timeout in seconds | `60` |
| `iscsi.serviceReloadDebounce` | Service reload debounce in milliseconds | `2000` |
| `nvmeof.enabled` | Render NVMe-oF configuration | `false` |
| `nvmeof.transport` | Transport (`tcp` or `rdma`) | `tcp` |
| `nvmeof.address` | Target address; falls back to `truenas.host` | `""` |
| `nvmeof.port` | Target service ID/port | `4420` |
| `nvmeof.subsystemHosts` | Allowed host NQNs | `[]` |
| `nvmeof.subsystemAllowAnyHost` | Allow any host NQN | `false` |
| `nvmeof.commandTimeout` | `nvme` command timeout in seconds | `30` |

With `fencing.mode=off`, NVMe-oF requires an explicit
`nvmeof.subsystemHosts` allow-list unless `subsystemAllowAnyHost=true`. In
`additive` mode those hosts remain a compatibility allowlist alongside dynamic
publication entries. In `strict` mode they are ignored for fenced volumes.

The iSCSI IQN basename comes from the TrueNAS global iSCSI configuration; it is
not a chart or driver ConfigMap setting. The removed `iscsi.basename` and
`nvmeof.basename` values had no effect.

iSCSI uses one `iscsi.portal`; additional portal configuration is not exposed.
CHAP and dm-multipath are unsupported. Restrict TCP 3260 to Kubernetes nodes
using a dedicated storage network and firewall or SGACL policy.

### StorageClasses

`storageClasses` is a list, so one release can create NFS, iSCSI, and NVMe-oF
classes. The driver reads only `protocol` from ordinary StorageClass parameters;
TrueNAS, ZFS, and protocol settings belong in the driver ConfigMap values above.
When multiple protocols are enabled, `protocol` is required and an omitted
value returns `InvalidArgument` instead of defaulting to NFS.

| Field | Description | Default in bundled class |
|---|---|---|
| `storageClasses[].name` | StorageClass name | `scale-nfs` |
| `storageClasses[].enabled` | Render this class (`false` ships an opt-in example disabled) | `true` |
| `storageClasses[].protocol` | `nfs`, `iscsi`, or `nvmeof` | `nfs` |
| `storageClasses[].snapshotRestoreMode` | `clone` or `detached`: how a snapshot-sourced PVC is provisioned (unset follows `zfs.detachedVolumesFromSnapshots`) | unset |
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
non-empty, it takes precedence over `storageClasses` and renders one class. It
accepts the former `create`, `name`, `protocol`, `isDefault`, `reclaimPolicy`,
`allowVolumeExpansion`, `volumeBindingMode`, and `mountOptions` fields plus
`extraParameters`. The deprecated path emits `protocol` and `mountOptions` only
when they are explicitly set. Migrate existing values files to `storageClasses`
when convenient.

### Snapshots

| Parameter | Description | Default |
|---|---|---|
| `snapshotClass.create` | Create a VolumeSnapshotClass | `false` |
| `snapshotClass.name` | VolumeSnapshotClass name | `scale-csi` |
| `snapshotClass.deletionPolicy` | `Delete` or `Retain` | `Delete` |
| `snapshotClass.labels` | Additional labels | `{}` |
| `snapshotClass.annotations` | Additional annotations | `{}` |

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
| `controller.resources` | Controller driver resources | requests `10m` CPU, `32Mi` memory; memory limit `256Mi` |
| `node.enabled` | Deploy the node DaemonSet | `true` |
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
| `metrics.enabled` | Create metrics Services | `true` |
| `metrics.port` | Driver health/readiness and metrics port | `9809` |
| `metrics.serviceMonitor.enabled` | Create controller and node ServiceMonitors | `false` |
| `metrics.serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |
| `metrics.serviceMonitor.interval` | Prometheus scrape interval | `30s` |
| `metrics.serviceMonitor.scrapeTimeout` | Prometheus scrape timeout | `10s` |
| `metrics.prometheusRule.enabled` | Create the bundled PrometheusRule | `false` |
| `metrics.prometheusRule.additionalLabels` | Additional PrometheusRule labels | `{}` |
| `metrics.prometheusRule.rules` | Replace the bundled alert rules when non-empty | `[]` |
| `metrics.dashboards.enabled` | Create a Grafana dashboard ConfigMap | `false` |
| `metrics.dashboards.annotations` | Dashboard ConfigMap annotations (for example, a folder selector) | `{}` |

The bundled PrometheusRule alerts on a missing controller scrape target,
TrueNAS disconnection, an open circuit breaker, a high TrueNAS API failure
ratio, sustained CSI operation errors, and detected orphan volumes or snapshots.
The dashboard ConfigMap is labeled `grafana_dashboard: "1"`
for Grafana sidecar discovery and uses only metrics exported by the driver.

### Orphan reconcile and guarded cleanup

| Parameter | Description | Default |
|---|---|---|
| `reconcile.enabled` | Run periodic read-only orphan object detection in the controller | `true` |
| `reconcile.interval` | Controller reconcile interval, including replication-job hygiene | `1h` |
| `reconcile.minOrphanAge` | Minimum backend object age before orphan classification | `24h` |
| `reconcile.alertAfter` | Prometheus alert hold time; keep greater than 2x the interval | `2h5m` |
| `reconcile.delete.enabled` | Create the opt-in guarded cleanup CronJob | `false` |
| `reconcile.delete.schedule` | Guarded cleanup CronJob schedule | `0 4 * * *` |
| `reconcile.delete.maxPerRun` | Maximum successful deletions in one cleanup run | `5` |

Read-only detection is enabled by default and exports
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

Each controller sidecar exposes `timeout`, `workerThreads`, and `extraArgs`.
The resizer maps `workerThreads` to its `--workers` CLI flag; the other sidecars
use `--worker-threads`.

When `controller.replicas>1`, the provisioner, attacher, resizer, and snapshotter
receive leader-election flags and the pod template gets preferred hostname
anti-affinity. With one replica those flags are omitted. Fencing modes other
than `off` require exactly one replica because background fencing reconcilers
are singleton writers.

| Parameter | Default |
|---|---|
| `sidecars.provisioner.timeout` | `300s` |
| `sidecars.provisioner.workerThreads` | `10` |
| `sidecars.provisioner.extraArgs` | `[]` |
| `sidecars.attacher.timeout` | `300s` |
| `sidecars.attacher.workerThreads` | `10` |
| `sidecars.attacher.extraArgs` | `[]` |
| `sidecars.resizer.timeout` | `300s` |
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
| `resilience.rateLimiting.maxConcurrentRequests` | Concurrent API request limit | `10` |
| `resilience.rateLimiting.maxConcurrentLogins` | Concurrent login limit per portal | `2` |
| `commandTimeouts.mount` | Mount timeout in seconds | `30` |
| `commandTimeouts.format` | Format timeout in seconds | `300` |
| `commandTimeouts.iscsi` | `iscsiadm` timeout in seconds | `10` |
| `commandTimeouts.nvme` | `nvme` timeout in seconds | `30` |

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

```bash
helm upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  -f values.yaml

helm uninstall scale-csi --namespace scale-csi
```

PVCs and their data are not deleted merely because the chart is uninstalled.
