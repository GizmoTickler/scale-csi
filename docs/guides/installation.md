# Installation and configuration

This guide covers installing the chart in each supported way and the
configuration decisions you make at install time. For a guided first install,
including TrueNAS and node preparation, start with
[Getting started](../getting-started.md).

## Where the chart lives

The chart is published as an OCI artifact with every release:

```text
oci://ghcr.io/gizmotickler/charts/scale-csi
```

Chart version `X.Y.Z` deploys driver image `ghcr.io/gizmotickler/scale-csi:vX.Y.Z`
unless `image.tag` or `image.digest` is overridden. The in-tree `Chart.yaml`
carries a `0.0.0-dev` placeholder that CI stamps with the tag at release time,
so install from the registry rather than from a checkout unless you are
developing the chart. List the published versions on the
[releases page](https://github.com/GizmoTickler/scale-csi/releases), and verify
signatures with the commands in the [root README](../../README.md#verifying-releases).

## Helm

```bash
kubectl create namespace scale-csi
kubectl label namespace scale-csi pod-security.kubernetes.io/enforce=privileged
kubectl -n scale-csi create secret generic truenas-api-key \
  --from-literal=api-key='<your-truenas-api-key>'

helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --version <X.Y.Z> \
  --values values.yaml
```

Start `values.yaml` from one of the files in
[`examples/helm/`](../../examples/helm/): minimal NFS, iSCSI, or NVMe-oF;
multi-protocol; production; encryption; or a second driver instance. The chart
validates values against its JSON schema, so an unknown or misspelled key fails
the install.

## Flux

[`examples/gitops/flux/scale-csi.yaml`](../../examples/gitops/flux/scale-csi.yaml)
is a complete Namespace + `OCIRepository` + `HelmRelease`. Two settings matter
beyond the values:

- `install.crds` and `upgrade.crds` set to `CreateReplace`. The chart ships the
  `VolumePublication` CRD in `crds/`, which Helm installs only on first install;
  without `CreateReplace` an upgrade that changes the CRD never applies it.
- The API-key Secret comes from your secret tooling (SOPS, Sealed Secrets,
  External Secrets) and must contain `api-key`.

## Argo CD

[`examples/gitops/argocd/application.yaml`](../../examples/gitops/argocd/application.yaml)
sources the public OCI chart (`repoURL: ghcr.io/gizmotickler/charts`,
`chart: scale-csi`). Argo CD renders `crds/` by default, so the CRD is applied
on every sync. Create the namespace (with the `privileged` Pod Security label)
and the API-key Secret outside the Application, or from a separate one.

## Nomad and other orchestrators

The driver binary also runs without the chart; see [Nomad](nomad.md) and the
complete driver configuration files
[`examples/scale-nfs.yaml`](../../examples/scale-nfs.yaml),
[`scale-iscsi.yaml`](../../examples/scale-iscsi.yaml), and
[`scale-nvmeof.yaml`](../../examples/scale-nvmeof.yaml).

## Configuration truth

The schema-enforced source of truth is
[`charts/scale-csi/values.yaml`](../../charts/scale-csi/values.yaml), with the
maintained table in the [chart README](../../charts/scale-csi/README.md). Important
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
[StorageClass reference](../reference/storageclass.md#restore-mode).

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
| `csidriver.fsGroupPolicy` | `CSIDriver.spec.fsGroupPolicy`; default `File` (unchanged). Set `None` only on a fresh install committed to driver-applied NFSv4 ACLs — see the [StorageClass reference](../reference/storageclass.md#nfsv4-acls) |
| `nfs.shareSecurity` | Default `sharing.nfs` security list (`SYS`/`KRB5`/`KRB5I`/`KRB5P`); empty omits the field, keeping the TrueNAS default. `KRB5*` requires `nfs.krbEnabled=true`, enforced by the chart schema **and** at driver config load — the controller refuses to start otherwise |
| `nfs.shareExposeSnapshots` | Publish the read-only `.zfs/snapshot` tree through new exports |
| `nfs.krbEnabled` | Acknowledge that Kerberos is configured on the NFS service; required before `KRB5*` share security is accepted anywhere (global default or StorageClass parameter) |
| `nfs.shareMaproot*` / `nfs.shareMapall*` | Mutually exclusive in TrueNAS; setting both is rejected by the chart schema and at config load |
| `nfs.versionPreflight` | Validate a StorageClass's pinned NFS version against the global `nfs.config` protocols |
| `nfs.ensureProtocols` | **HARD RULE, opt-in:** mutate the GLOBAL TrueNAS NFS service to enable these major versions. Affects every export on the appliance, driver-managed or not. `nfs.update` **sets** the protocol list rather than unioning with it, so the driver hard-aborts the write when it cannot read the current list — an unparseable service config is never a safe basis for a service-wide write |

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
is no `node.topology` chart value. See the [topology guide](topology.md).

## Upgrades

Upgrading the chart, rotating credentials, migrating StorageClasses, and
upgrading TrueNAS underneath the driver are covered in [Upgrades](upgrades.md).
For the production contract (fencing, HA, security, monitoring) read
[Production](../production.md).
