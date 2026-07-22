# Deployment and configuration

This guide covers the bundled OCI Helm chart. The chart and image use the same
release number: chart `1.2.23` deploys image `v1.2.23` unless `image.tag` or
`image.digest` is overridden.

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
version, for example `--version 1.2.23`. See the root README for image, chart,
and provenance verification commands.

## Flux

The current Flux OCI source shape uses `OCIRepository` plus `HelmRelease`. Pin
the exact release you reviewed; this example uses the v1.2.23 baseline:

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
    semver: "1.2.23"
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
ordinary StorageClass parameters. The removed `zfs.dedup`, `zfs.compression`,
`zfs.compressionAlgorithm`, `nfs.mountOptions`, `iscsi.basename`, and
`node.kubeletHostPath` names are not valid chart values.

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

## Availability and topology

With `fencing.mode=off`, `controller.replicas>1` enables leader election on the
provisioner, attacher, resizer, and snapshotter. The chart supplies preferred
hostname anti-affinity and, by default, a PDB with `maxUnavailable: 1`.
`additive` and `strict` fencing require exactly one controller because their
background reconcilers are singleton writers; chart schema and template guards
enforce that invariant.

Topology is auto-detected per node from the standard
`topology.kubernetes.io/zone` and `topology.kubernetes.io/region` labels. There
is no `node.topology` chart value. See the [topology guide](guides/topology.md).

## Upgrade notes

- StorageClass `parameters` are immutable. To add the now-required `protocol`
  parameter, create a replacement StorageClass (often with a temporary name),
  move workload manifests to it, then delete/recreate the old class only after
  no manifests depend on its name. Existing PVs keep their original class and
  are not reprovisioned by this metadata migration.
- The node DaemonSet intentionally receives no `TRUENAS_API_KEY`. Routine stage,
  publish, unpublish, unstage, and local expansion use host tools and can run in
  lazy-connect mode. A node-side operation that actually needs the management
  API will fail without credentials; do not assume the controller Secret is
  present on node pods after upgrading.
- ConfigMap changes restart controller and node pods. Rotating an externally
  managed Secret does not change the pod-template checksum, so restart both
  workloads explicitly after rotation.

For fencing's node-first v1.2.23 migration and the full production contract,
read [Production deployment](production.md).
