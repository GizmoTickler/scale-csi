# StorageClass Configuration Reference

This document provides a complete reference for configuring StorageClasses with the Scale CSI driver.

## Quick Reference

| Protocol | Provisioner | Access Modes | Volume Mode |
|----------|-------------|--------------|-------------|
| NFS | `csi.scale.io` | RWO, ROX, RWX | Filesystem |
| iSCSI | `csi.scale.io` | RWO | Block, Filesystem |
| NVMe-oF | `csi.scale.io` | RWO | Block, Filesystem |

## Basic StorageClass Structure

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs
provisioner: csi.scale.io
parameters:
  # Protocol-specific parameters here
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

## Common Parameters

These parameters apply to all storage protocols:

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `csi.storage.k8s.io/fstype` | Filesystem type for block volumes (ext4, xfs) | ext4 | No |

## NFS Parameters

NFS is the recommended protocol for shared storage workloads.

### StorageClass Example

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs
provisioner: csi.scale.io
parameters: {}
mountOptions:
  - nfsvers=4
  - noatime
  - rsize=1048576
  - wsize=1048576
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

### Mount Options

Common NFS mount options:

| Option | Description | Recommendation |
|--------|-------------|----------------|
| `nfsvers=4` | Use NFSv4 protocol | Recommended |
| `noatime` | Don't update access times | Reduces I/O |
| `rsize=1048576` | Read buffer size (1MB) | Performance |
| `wsize=1048576` | Write buffer size (1MB) | Performance |
| `hard` | Retry operations indefinitely | Data safety |
| `soft` | Fail operations after timeout | Responsive |

### Access Modes

NFS supports all access modes:
- `ReadWriteOnce` (RWO) - Single node read/write
- `ReadOnlyMany` (ROX) - Multiple nodes read-only
- `ReadWriteMany` (RWX) - Multiple nodes read/write

## iSCSI Parameters

iSCSI is recommended for block storage workloads requiring low latency.

### StorageClass Example

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-iscsi
provisioner: csi.scale.io
parameters:
  csi.storage.k8s.io/fstype: ext4
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

### Volume Modes

iSCSI supports both volume modes:

**Filesystem Mode** (default):
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc
spec:
  storageClassName: scale-iscsi
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

**Block Mode** (raw device):
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-block-pvc
spec:
  storageClassName: scale-iscsi
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

### Access Modes

iSCSI supports:
- `ReadWriteOnce` (RWO) - Single node read/write

## NVMe-oF Parameters

NVMe-oF provides the lowest latency for performance-critical workloads.

### StorageClass Example

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nvmeof
provisioner: csi.scale.io
parameters:
  csi.storage.k8s.io/fstype: xfs
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

### Prerequisites

- TrueNAS SCALE 25.10+
- `nvme-cli` package installed on nodes
- NVMe-oF kernel modules loaded (`nvme-tcp` or `nvme-rdma`)

### Access Modes

NVMe-oF supports:
- `ReadWriteOnce` (RWO) - Single node read/write

## Volume Binding Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `Immediate` | Provision immediately when PVC created | General use |
| `WaitForFirstConsumer` | Provision when pod scheduled | Topology-aware |

**Recommendation**: Use `WaitForFirstConsumer` for iSCSI/NVMe-oF to ensure volumes are created on accessible storage.

## Reclaim Policies

| Policy | Description | Use Case |
|--------|-------------|----------|
| `Delete` | Delete volume when PVC deleted | Non-persistent data |
| `Retain` | Keep volume after PVC deleted | Critical data |

## Example Configurations

### High-Performance Database

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-db-fast
provisioner: csi.scale.io
parameters:
  csi.storage.k8s.io/fstype: xfs
reclaimPolicy: Retain
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-data
spec:
  storageClassName: scale-db-fast
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

### Shared Media Storage

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-media
provisioner: csi.scale.io
parameters: {}
mountOptions:
  - nfsvers=4
  - noatime
reclaimPolicy: Retain
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: media-library
spec:
  storageClassName: scale-media
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Ti
```

### Ephemeral Build Cache

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-cache
provisioner: csi.scale.io
parameters: {}
mountOptions:
  - nfsvers=4
  - noatime
  - soft
  - timeo=30
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: Immediate
```

## Troubleshooting

### PVC Stuck in Pending

1. Check provisioner logs:
   ```bash
   kubectl logs -n scale-csi deploy/scale-csi-controller -c csi-provisioner
   ```

2. Check driver logs:
   ```bash
   kubectl logs -n scale-csi deploy/scale-csi-controller -c scale-csi
   ```

3. Verify TrueNAS connectivity:
   ```bash
   kubectl exec -n scale-csi deploy/scale-csi-controller -c scale-csi -- \
     cat /tmp/truenas-health
   ```

### Mount Failures

1. Check node plugin logs:
   ```bash
   kubectl logs -n scale-csi ds/scale-csi-node -c scale-csi
   ```

2. Verify NFS/iSCSI services on TrueNAS are running

3. Check network connectivity between node and TrueNAS
