# Deployment & Configuration Guide

This document provides detailed instructions for deploying the Scale CSI driver using Helm and Flux, along with a comprehensive configuration reference.

## Helm Deployment

### 1. Add the Helm Repository
The chart is hosted as an OCI artifact on GitHub Container Registry.

```bash
# Optional: Login if using private repository features
helm registry login ghcr.io -u <username>
```

### 2. Prepare Values File
Create a `values.yaml` file. You can start with one of the examples in the `examples/` directory of the repository.

**Minimal NFS Example:**
```yaml
csiDriverName: csi.scale.io
truenas:
  host: "nas.example.com"
  apiKey: "1-xxxxx"
zfs:
  parentDataset: "tank/k8s/nfs"
nfs:
  enabled: true
iscsi:
  enabled: false
```

### 3. Install the Chart

```bash
helm install scale-csi \
  oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --create-namespace \
  --values values.yaml \
  --version 2.2.9
```

## Flux Deployment

To deploy using Flux CD, you need a `HelmRepository` (OCI) and a `HelmRelease`.

### 1. HelmRepository (OCI)

```yaml
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: HelmRepository
metadata:
  name: scale-csi
  namespace: flux-system
spec:
  interval: 1h
  type: oci
  url: oci://ghcr.io/gizmotickler/charts/scale-csi
```

### 2. HelmRelease

```yaml
apiVersion: helm.toolkit.fluxcd.io/v2beta1
kind: HelmRelease
metadata:
  name: scale-csi
  namespace: scale-csi
spec:
  interval: 1h
  chart:
    spec:
      chart: scale-csi
      version: "2.2.9"
      sourceRef:
        kind: HelmRepository
        name: scale-csi
        namespace: flux-system
  install:
    createNamespace: true
  values:
    csiDriverName: csi.scale.io
    truenas:
      host: "nas.example.com"
      apiKey: "1-xxxxx" # Recommend using a Secret reference instead
      secure: true
    zfs:
      parentDataset: "tank/k8s/vols"
    nfs:
      enabled: true
    iscsi:
      enabled: false
```

### Using Secrets for Credentials (Recommended)

Instead of hardcoding the API key, create a secret and reference it:

```bash
kubectl create secret generic truenas-creds \
  --namespace scale-csi \
  --from-literal=api-key="1-xxxxx"
```

Then in your `values.yaml` or `HelmRelease`:

```yaml
truenas:
  existingSecret: "truenas-creds"
```

## Configuration Reference

| Parameter | Description | Default |
|-----------|-------------|---------|
| `csiDriverName` | Name of the CSI driver (must match StorageClass provisioner) | `csi.scale.io` |
| **TrueNAS Connection** | | |
| `truenas.host` | Hostname or IP of TrueNAS system | `""` |
| `truenas.port` | API port (443 for HTTPS, 80 for HTTP) | `443` |
| `truenas.secure` | Use HTTPS | `true` |
| `truenas.apiKey` | TrueNAS API Key (format: `1-xxx`) | `""` |
| `truenas.existingSecret` | Name of existing secret containing `api-key` | `""` |
| `truenas.skipTLSVerify` | Skip SSL certificate validation | `false` |
| **ZFS Configuration** | | |
| `zfs.parentDataset` | Parent dataset for all provisioned volumes | `""` |
| `zfs.dedup` | Enable ZFS deduplication | `false` |
| `zfs.compression` | Enable ZFS compression | `true` |
| `zfs.compressionAlgorithm` | Compression algorithm (lz4, zstd, etc.) | `lz4` |
| **Protocols** | | |
| `nfs.enabled` | Enable NFS driver support | `true` |
| `nfs.server` | NFS server address (defaults to `truenas.host`) | `""` |
| `nfs.mountOptions` | Default NFS mount options | `["nfsvers=4", "noatime"]` |
| `iscsi.enabled` | Enable iSCSI driver support | `true` |
| `iscsi.portal` | iSCSI portal address (defaults to `truenas.host`) | `""` |
| `iscsi.portalPort` | iSCSI portal port | `3260` |
| `iscsi.basename` | iSCSI IQN base name | `iqn.2005-10.org.freenas.ctl` |
| `nvmeof.enabled` | Enable NVMe-oF driver support | `false` |
| `nvmeof.transport` | NVMe-oF transport (tcp, rdma) | `tcp` |
| **Controller** | | |
| `controller.replicas` | Number of controller replicas | `1` |
| `controller.resources` | CPU/Memory limits/requests | (see values.yaml) |
| **Node** | | |
| `node.kubeletHostPath` | Path to kubelet directory (for microk8s/k0s) | `/var/lib/kubelet` |
| `node.resources` | CPU/Memory limits/requests | (see values.yaml) |
| **StorageClass** | | |
| `storageClass.create` | Create a default StorageClass | `true` |
| `storageClass.name` | Name of the StorageClass | `scale-nfs` |
| `storageClass.protocol` | Protocol for StorageClass (`nfs`, `iscsi`, `nvmeof`) | `nfs` |

## StorageClass Parameters

You can create additional StorageClasses with specific configurations.

### NFS StorageClass
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs-archive
provisioner: csi.scale.io
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
parameters:
  protocol: "nfs"
  # Optional: Override default mount options
  mountOptions: "nfsvers=4.2,noatime,soft"
  # Optional: Dataset properties
  dataset_recordsize: "1M"
  dataset_compression: "zstd"
```

### iSCSI StorageClass
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-iscsi-db
provisioner: csi.scale.io
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
parameters:
  protocol: "iscsi"
  # Optional: Zvol properties
  zvol_volblocksize: "16K"
  zvol_compression: "lz4"
  # Filesystem on the device
  fsType: "xfs"
```
