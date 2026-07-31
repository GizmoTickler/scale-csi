# Deployment and configuration

This guide covers the bundled OCI Helm chart, based on the v1.4.0 release line.
The chart and image use the same release number: chart `1.4.0` deploys image
`v1.4.0` unless `image.tag` or `image.digest` is overridden. (The in-tree
`Chart.yaml` carries a `0.0.0-dev` placeholder that CI stamps with the tag at
release time.)

## Supported deployment matrix

| Protocol | TrueNAS SCALE | Node requirements | Kubernetes volume modes |
|---|---|---|---|
| NFS | 25.04+ | NFS client and kernel support | Filesystem; RWO, ROX, RWX |
| iSCSI | 25.04+ | `iscsiadm`, `iscsid`, `iscsi_tcp` | Filesystem or Block; RWO |
| NVMe-oF | 25.10+ | `nvme-cli`, `nvme_tcp`/`nvme_fabrics` | Filesystem or Block; RWO |

The NFS and iSCSI API clients also understand snapshot API generations found
on older and newer TrueNAS releases, but that is not a claim of full driver
support outside the matrix. Validate the exact TrueNAS patch and node data path
in staging.

## Helm

Create the API-key Secret first:

```bash
kubectl create namespace scale-csi
kubectl -n scale-csi create secret generic truenas-creds \
  --from-literal=api-key='1-replace-me'
```

Create `values.yaml`:

```yaml
truenas:
  host: nas.example.com
  existingSecret: truenas-creds
zfs:
  parentDataset: tank/k8s/volumes
nfs:
  enabled: true
iscsi:
  enabled: false
nvmeof:
  enabled: false
storageClasses:
  - name: scale-nfs
    protocol: nfs
    isDefault: true
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: [nfsvers=4, noatime]
    extraParameters: {}
```

Install the chart:

```bash
helm install scale-csi \
  oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --values values.yaml
```

The example deliberately avoids a soon-stale version literal. For a controlled
production rollout, verify the release signature and add the exact reviewed
version, for example `--version 1.4.0`. See the root README for image, chart,
and provenance verification commands.

## Flux

The current Flux OCI source shape uses `OCIRepository` plus `HelmRelease`. Pin
the exact release you reviewed; this example uses the v1.4.0 baseline:

```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: scale-csi
  namespace: scale-csi
spec:
  interval: 1h
  url: oci://ghcr.io/gizmotickler/charts/scale-csi
  layerSelector:
    mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip
    operation: copy
  ref:
    semver: "1.4.0"
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: scale-csi
  namespace: scale-csi
spec:
  interval: 1h
  releaseName: scale-csi
  chartRef:
    kind: OCIRepository
    name: scale-csi
  values:
    truenas:
      host: nas.example.com
      existingSecret: truenas-creds
    zfs:
      parentDataset: tank/k8s/volumes
    nfs:
      enabled: true
    iscsi:
      enabled: false
    nvmeof:
      enabled: false
```

The referenced Secret must already exist in `scale-csi` and contain `api-key`.

## Configuration truth

The schema-enforced source of truth is
[`charts/scale-csi/values.yaml`](../charts/scale-csi/values.yaml), with the
maintained table in the [chart README](../charts/scale-csi/README.md). Important
settings are:

| Setting | Meaning | Default |
|---|---|---|
| `csiDriverName` | Unified CSI provisioner name | `csi.scale.io` |
| `truenas.host` | TrueNAS API hostname/IP | required |
| `truenas.existingSecret` | Secret containing `api-key` | `""` |
| `zfs.parentDataset` | Exclusive per-cluster CSI parent | required |
| `zfs.datasetProperties` | ZFS properties for new datasets/zvols | `{}` (inherit) |
| `kubeletDir` | Host kubelet directory | `/var/lib/kubelet` |
| `controller.replicas` | Controller replicas; fencing modes require one | `1` |
| `controller.resources` / `node.resources` | Driver requests and limits | `10m`/`32Mi`, memory limit `256Mi` |
| `sidecars.*.resources` | Per-sidecar requests and limits | `10m`/`32Mi`, memory limit `128Mi` |

NFS mount options belong on each StorageClass's top-level `mountOptions` list.
ZFS compression, dedup, record size, and zvol block size are driver values, not
ordinary StorageClass parameters. The legacy `zfs.dedup`, `zfs.compression`, and
`zfs.compressionAlgorithm` chart values are still accepted for compatibility but
are deprecated and ignored; configure these through `zfs.datasetProperties`. The
`nfs.mountOptions`, `iscsi.basename`, and `node.kubeletHostPath` names are not
valid chart values.

> **One parent per cluster:** never point two live clusters at the same
> `zfs.parentDataset`. Reconcile can see only its own cluster objects and would
> classify the other cluster's managed backend objects as orphans.

## StorageClasses

The unified driver reads `protocol` from ordinary StorageClass parameters. It
also receives the standard `csi.storage.k8s.io/fstype` value through the CSI
volume capability for formatted block volumes. It does not consume ad-hoc
`dataset_*`, `zvol_*`, `mountOptions`, or `fsType` parameters.

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs-archive
provisioner: csi.scale.io
parameters:
  protocol: nfs
mountOptions:
  - nfsvers=4
  - noatime
reclaimPolicy: Retain
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-iscsi-xfs
provisioner: csi.scale.io
parameters:
  protocol: iscsi
  csi.storage.k8s.io/fstype: xfs
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

When more than one protocol is enabled, `protocol` is required and omission
returns `InvalidArgument`; it never silently provisions NFS. A single-protocol
legacy instance may omit it and uses its sole enabled protocol.

A StorageClass may also set `snapshotRestoreMode: clone` or `detached` to choose
how snapshot restores are materialized (clone pins the source snapshot; detached
is an independent copy). Chart `storageClasses[]` entries expose it as a
first-class field; it is only emitted when set. See the
[StorageClass reference](reference/storageclass.md#restore-mode).

## Capacity-aware scheduling

CSIStorageCapacity tracking is opt-in (`capacity.enabled`, default off). When
enabled, the chart advertises `CSIDriver.spec.storageCapacity=true` and runs the
external-provisioner capacity controller, which publishes `CSIStorageCapacity`
objects from the driver's `GetCapacity` (the parent dataset's ZFS `available`).

For this to influence pod scheduling you must use a StorageClass whose
`volumeBindingMode` is `WaitForFirstConsumer`:

- external-provisioner publishes capacity objects **only** for
  `WaitForFirstConsumer` classes by default; it ignores `Immediate` classes.
- The Kubernetes scheduler likewise consults storage capacity only for
  `WaitForFirstConsumer` binding.

The chart's bundled `scale-nfs` class is `Immediate`, so enabling
`capacity.enabled` against otherwise-default values starts the capacity
controller but creates **no** capacity objects and cannot affect scheduling.
Pair it with a `WaitForFirstConsumer` class (as in the `scale-iscsi-xfs` example
above) for scheduler integration.

If you need capacity published for non-scheduler consumers against `Immediate`
classes, set `capacity.forImmediateBinding: true` (default off), which adds the
provisioner's `--capacity-for-immediate-binding` flag.

The full `capacity.*` surface (all default off):

| Key | Effect / caveat |
|---|---|
| `capacity.enabled` | Advertise `CSIDriver.spec.storageCapacity=true`, run the capacity controller, add its RBAC |
| `capacity.forImmediateBinding` | Also publish `CSIStorageCapacity` for `Immediate` classes — non-scheduler consumers only; no effect unless `capacity.enabled` |
| `capacity.reportMaximumVolumeSize` | Set `GetCapacityResponse.maximum_volume_size` to the parent dataset's available bytes. Appropriate **only** for thick/reserved zvol deployments (`zfs.zvolEnableReservation: true`); under thin overcommit a hard maximum makes the scheduler wrongly reject legitimate volumes |
| `capacity.gaugeEnabled` | Run a controller-only poll loop exporting `scale_csi_pool_available_bytes` / `scale_csi_pool_capacity_bytes` |
| `capacity.gaugeInterval` | Gauge cadence; default `60s`, values below `30s` clamp to `30s` |
| `backendHealth.enabled` | Run a controller-only **read-only** pool-health poller (`pool.query` + `disk.temperature_alerts`) that fans pool health onto every managed PVC's `VolumeCondition` and exports the `scale_csi_pool_status`/`_healthy`/`_scan_state`/`_scan_errors`/`_disk_temp_alerts` gauges. Default off; see `docs/production.md` |
| `backendHealth.interval` | Health poll cadence; default `60s`, values below `30s` clamp to `30s` |
| `csidriver.fsGroupPolicy` | `CSIDriver.spec.fsGroupPolicy`; default `File` (unchanged). Set `None` only on a fresh install committed to driver-applied NFSv4 ACLs — see `docs/reference/storageclass.md` |
| `nfs.shareSecurity` | Default `sharing.nfs` security list (`SYS`/`KRB5`/`KRB5I`/`KRB5P`); empty omits the field, keeping the TrueNAS default |
| `nfs.shareExposeSnapshots` | Publish the read-only `.zfs/snapshot` tree through new exports |
| `nfs.krbEnabled` | Acknowledge that Kerberos is configured on the NFS service; required before `KRB5*` share security is accepted |
| `nfs.versionPreflight` | Validate a StorageClass's pinned NFS version against the global `nfs.config` protocols |
| `nfs.ensureProtocols` | **HARD RULE, opt-in:** mutate the GLOBAL TrueNAS NFS service to enable these major versions. Affects every export on the appliance, driver-managed or not |

Cost and cleanup caveats:

- `GetCapacity` is exactly one `pool.dataset.query` against the parent per
  referencing StorageClass; it is background provisioner load, not part of the
  CreateVolume/publish golden totals.
- The gauge loop samples immediately then every interval and performs **one parent
  dataset query per interval per controller replica** — it has **no
  leader-election gate**, so the supported/documented topology is `replicas=1`
  (one query/min).
- Disabling capacity after it has been on can leave owner-referenced
  `CSIStorageCapacity` objects behind (no finalizer) until the controller
  Deployment is deleted or the objects are removed manually.
- `metrics.prometheusRule.poolUsageThreshold` (default `0.85`) drives the
  `ScaleCSIPoolNearFull` alert, which renders only when the bundled PrometheusRule
  **and** `capacity.gaugeEnabled` are both enabled.

## Availability and topology

Leader election is enabled on the provisioner, attacher, resizer, and
snapshotter unconditionally, even at the default single replica.
`controller.replicas>1` additionally supplies preferred hostname anti-affinity
and, by default, a PDB with `maxUnavailable: 1`. `additive` and `strict`
fencing require exactly one controller because their background reconcilers are
singleton writers; chart schema and template guards enforce that invariant.

Topology is auto-detected per node from the standard
`topology.kubernetes.io/zone` and `topology.kubernetes.io/region` labels. There
is no `node.topology` chart value. See the [topology guide](guides/topology.md).

## Upgrade notes

- StorageClass `parameters` are immutable. To add the now-required `protocol`
  parameter, create a replacement StorageClass (often with a temporary name),
  move workload manifests to it, then delete/recreate the old class only after
  no manifests depend on its name. Existing PVs keep their original class and
  are not reprovisioned by this metadata migration.
- The node DaemonSet intentionally receives no `TRUENAS_API_KEY`. A node-only
  pod constructs **no TrueNAS management client at all** — stage, publish,
  unpublish, unstage, and local expansion use host tools exclusively, so a node
  pod initializes and reports ready even while TrueNAS is unreachable. There is
  no deferred/lazy API connection on node pods; do not assume the controller
  Secret is present on them after upgrading.
- ConfigMap changes restart controller and node pods. Rotating an externally
  managed `truenas.existingSecret` does not change the pod-template checksum;
  restart the **controller** workload explicitly after rotation (only the
  controller holds the TrueNAS API key — the node builds no API client, so it does
  not need a restart for that credential). CHAP Secrets are request-scoped CSI
  Secrets and need no driver rollout at all.

For fencing's node-first migration and the full production contract,
read [Production deployment](production.md).
