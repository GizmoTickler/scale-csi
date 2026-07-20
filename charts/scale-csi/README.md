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

## Configuration

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
| `zfs.detachedSnapshotsDatasetParentName` | Parent dataset for detached snapshots | `""` |
| `zfs.enforceQuota` | Enable dataset quotas | `true` |
| `zfs.zvolBlocksize` | Block size for zvols | `16K` |
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
| `iscsi.targetPortals` | Additional multipath portals | `[]` |
| `iscsi.namePrefix` | Target and extent name prefix | `""` |
| `iscsi.targetGroups` | Explicit portal/initiator group IDs; empty = auto-resolve from `iscsi.portal` | `[]` |
| `iscsi.extentBlocksize` | Extent block size | `512` |
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

NVMe-oF requires an explicit `nvmeof.subsystemHosts` allow-list by default.
Existing installations that relied on allow-any must explicitly set
`nvmeof.subsystemAllowAnyHost: true` during upgrade. The driver reconciles the
configured host list on publish and validates that restricted mode has at least
one host at startup.

The iSCSI IQN basename comes from the TrueNAS global iSCSI configuration; it is
not a chart or driver ConfigMap setting. The removed `iscsi.basename` and
`nvmeof.basename` values had no effect.

### StorageClasses

`storageClasses` is a list, so one release can create NFS, iSCSI, and NVMe-oF
classes. The driver reads only `protocol` from ordinary StorageClass parameters;
TrueNAS, ZFS, and protocol settings belong in the driver ConfigMap values above.

| Field | Description | Default in bundled class |
|---|---|---|
| `storageClasses[].name` | StorageClass name | `scale-nfs` |
| `storageClasses[].protocol` | `nfs`, `iscsi`, or `nvmeof` | `nfs` |
| `storageClasses[].isDefault` | Add the default-class annotation | `false` |
| `storageClasses[].reclaimPolicy` | `Delete` or `Retain` | `Delete` |
| `storageClasses[].allowVolumeExpansion` | Allow PVC expansion | `true` |
| `storageClasses[].volumeBindingMode` | Kubernetes binding mode | `Immediate` |
| `storageClasses[].mountOptions` | StorageClass mount options | `[nfsvers=4, noatime]` |
| `storageClasses[].extraParameters` | Additional CSI parameters such as secret references | `{}` |

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
```

The old `storageClass` map remains supported for compatibility. When it is
non-empty, it takes precedence over `storageClasses` and renders one class. It
accepts the former `create`, `name`, `protocol`, `isDefault`, `reclaimPolicy`,
`allowVolumeExpansion`, `volumeBindingMode`, and `mountOptions` fields plus
`extraParameters`. Migrate existing values files to `storageClasses` when
convenient.

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
| `controller.replicas` | Controller replicas | `1` |
| `controller.priorityClassName` | Controller priority class | `system-cluster-critical` |
| `controller.podDisruptionBudget.enabled` | Create a controller PDB | `false` |
| `controller.podDisruptionBudget.minAvailable` | PDB minimum available | `""` |
| `controller.podDisruptionBudget.maxUnavailable` | PDB maximum unavailable | `""` |
| `controller.containerSecurityContext` | Controller driver container security context | non-root UID 65532, read-only root filesystem, no capabilities |
| `controller.resources` | Controller driver resources | requests `10m` CPU, `32Mi` memory; no limits |
| `node.enabled` | Deploy the node DaemonSet | `true` |
| `node.priorityClassName` | Node priority class | `system-node-critical` |
| `node.sessionCleanupDelay` | Stale-session retry delay in milliseconds | `500` |
| `node.maxVolumesPerNode` | Maximum volumes advertised per node; `0` means unlimited/unset | `0` |
| `node.resources` | Node driver resources | requests `10m` CPU, `32Mi` memory; no limits |
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
ratio, and sustained CSI operation errors. The dashboard ConfigMap is labeled `grafana_dashboard: "1"`
for Grafana sidecar discovery and uses only metrics exported by the driver.

Set one of `controller.podDisruptionBudget.minAvailable` or `maxUnavailable`
when enabling the PDB. A PDB is useful only when `controller.replicas` is greater
than one. ConfigMap and chart-managed Secret checksums are added to both pod
templates so configuration and API-key changes trigger rollouts.

### Resource sizing

Steady-state measurements are approximately 15Mi memory and 1m CPU per driver
container. The chart therefore requests 10m CPU and 32Mi memory for the
controller and node driver containers without setting limits. Both resource
maps remain fully overridable; CSI sidecar resources remain empty by default.

When CPU or memory limits are configured, the driver adapts `GOMAXPROCS` and
`GOMEMLIMIT` from its cgroups. CSI liveness reports process health and no longer
depends on TrueNAS reachability, so a NAS blip or slow reconnect does not turn a
tight resource limit into a driver crash loop. Use `/readyz` and
`scale_csi_truenas_connection_status` for backend health.

### Controller sidecars

Each controller sidecar exposes `timeout`, `workerThreads`, and `extraArgs`.
The resizer maps `workerThreads` to its `--workers` CLI flag; the other sidecars
use `--worker-threads`.

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

- `nfs.shareAllowedNetworks: []` preserves the driver default but permits mounts
  from any network. Set explicit trusted CIDRs in production.
- `nvmeof.subsystemAllowAnyHost: false` requires `nvmeof.subsystemHosts` to
  contain each connecting node's `nvme show-hostnqn` output. Set allow-any to
  `true` only when that broader access is intentional. Keep network segmentation
  in place; host allowlisting is an additional control, not a replacement.
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
