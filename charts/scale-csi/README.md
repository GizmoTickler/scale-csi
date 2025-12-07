# Scale CSI Helm Chart

A Helm chart for deploying the Scale CSI driver for TrueNAS SCALE.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.8+
- TrueNAS SCALE with API access enabled
- For iSCSI: `open-iscsi` package installed on nodes
- For NVMe-oF: `nvme-cli` package installed on nodes

## Quick Start

```bash
helm repo add scale-csi oci://ghcr.io/gizmotickler/charts
helm install scale-csi scale-csi/scale-csi \
  --namespace scale-csi \
  --create-namespace \
  --set truenas.host=truenas.local \
  --set truenas.apiKey=1-xxxxx \
  --set zfs.parentDataset=tank/k8s/volumes
```

## Installation

### From OCI Registry

```bash
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --create-namespace \
  -f values.yaml
```

### From Source

```bash
git clone https://github.com/GizmoTickler/scale-csi
cd scale-csi
helm install scale-csi charts/scale-csi \
  --namespace scale-csi \
  --create-namespace \
  -f my-values.yaml
```

## Configuration

### Required Parameters

| Parameter | Description |
|-----------|-------------|
| `truenas.host` | TrueNAS SCALE hostname or IP |
| `truenas.apiKey` | API key for authentication |
| `zfs.parentDataset` | Parent dataset for volumes (e.g., `tank/k8s/volumes`) |

### TrueNAS Connection

| Parameter | Description | Default |
|-----------|-------------|---------|
| `truenas.host` | TrueNAS hostname/IP | `""` (required) |
| `truenas.port` | API port | `443` |
| `truenas.secure` | Use HTTPS | `true` |
| `truenas.skipTLSVerify` | Skip TLS verification | `false` |
| `truenas.apiKey` | API key | `""` |
| `truenas.existingSecret` | Use existing secret for credentials | `""` |
| `truenas.requestTimeout` | API request timeout (seconds) | `60` |
| `truenas.maxConcurrentRequests` | Max concurrent API requests | `10` |

### ZFS Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `zfs.parentDataset` | Parent dataset for volumes | `""` (required) |
| `zfs.dedup` | Enable deduplication | `false` |
| `zfs.compression` | Enable compression | `true` |
| `zfs.compressionAlgorithm` | Compression algorithm | `lz4` |
| `zfs.enforceQuota` | Enforce dataset quotas | `true` |
| `zfs.zvolReadyTimeout` | Timeout for zvol readiness (seconds) | `60` |

### NFS Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nfs.enabled` | Enable NFS support | `true` |
| `nfs.server` | NFS server address | `""` (uses truenas.host) |
| `nfs.mountOptions` | Default mount options | `["nfsvers=4", "noatime"]` |

### iSCSI Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `iscsi.enabled` | Enable iSCSI support | `true` |
| `iscsi.portal` | Target portal address | `""` (uses truenas.host) |
| `iscsi.portalPort` | Target portal port | `3260` |
| `iscsi.portalGroupId` | Portal group ID | `1` |
| `iscsi.initiatorGroupId` | Initiator group ID | `1` |
| `iscsi.basename` | IQN base name | `iqn.2005-10.org.freenas.ctl` |
| `iscsi.deviceWaitTimeout` | Device wait timeout (seconds) | `60` |
| `iscsi.serviceReloadDebounce` | Reload debounce (ms) | `2000` |

### NVMe-oF Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nvmeof.enabled` | Enable NVMe-oF support | `false` |
| `nvmeof.transport` | Transport type (tcp, rdma) | `tcp` |
| `nvmeof.address` | Target address | `""` (uses truenas.host) |
| `nvmeof.port` | Target port | `4420` |
| `nvmeof.basename` | NQN base name | `nqn.2014-08.org.nvmexpress` |

### StorageClass Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storageClass.create` | Create default StorageClass | `true` |
| `storageClass.name` | StorageClass name | `scale-nfs` |
| `storageClass.isDefault` | Set as default StorageClass | `false` |
| `storageClass.reclaimPolicy` | Reclaim policy | `Delete` |
| `storageClass.volumeBindingMode` | Volume binding mode | `Immediate` |
| `storageClass.allowVolumeExpansion` | Allow volume expansion | `true` |
| `storageClass.protocol` | Storage protocol | `nfs` |
| `storageClass.mountOptions` | Mount options for NFS | `["nfsvers=4", "noatime"]` |

### Controller Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `controller.enabled` | Deploy controller | `true` |
| `controller.replicas` | Number of replicas | `1` |
| `controller.resources` | Resource limits/requests | See values.yaml |
| `controller.nodeSelector` | Node selector | `{}` |
| `controller.tolerations` | Tolerations | Control plane tolerations |
| `controller.affinity` | Affinity rules | `{}` |

### Node Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `node.enabled` | Deploy node plugin | `true` |
| `node.resources` | Resource limits/requests | See values.yaml |
| `node.nodeSelector` | Node selector | `{}` |
| `node.tolerations` | Tolerations | All nodes |
| `node.affinity` | Affinity rules | `{}` |

## Example Configurations

### Basic NFS Setup

```yaml
truenas:
  host: truenas.local
  apiKey: 1-xxxxx

zfs:
  parentDataset: tank/k8s/volumes

storageClass:
  name: scale-nfs
  isDefault: true
```

### iSCSI with Custom Portal

```yaml
truenas:
  host: truenas.local
  apiKey: 1-xxxxx

zfs:
  parentDataset: tank/k8s/volumes

iscsi:
  enabled: true
  portal: 10.0.0.100
  portalPort: 3260

storageClass:
  name: scale-iscsi
  protocol: iscsi
  volumeBindingMode: WaitForFirstConsumer
```

### Using Existing Secret

```yaml
truenas:
  host: truenas.local
  existingSecret: truenas-credentials

zfs:
  parentDataset: tank/k8s/volumes
```

Create the secret:
```bash
kubectl create secret generic truenas-credentials \
  --namespace scale-csi \
  --from-literal=api-key=1-xxxxx
```

### High Availability Controller

```yaml
controller:
  replicas: 2
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app.kubernetes.io/component: controller
          topologyKey: kubernetes.io/hostname
```

## Upgrading

```bash
helm upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  -f values.yaml
```

## Uninstalling

```bash
helm uninstall scale-csi --namespace scale-csi
```

**Note**: PVCs and their data are not deleted when uninstalling the chart.

## Troubleshooting

### Check Controller Logs

```bash
kubectl logs -n scale-csi deploy/scale-csi-controller -c scale-csi
```

### Check Node Plugin Logs

```bash
kubectl logs -n scale-csi ds/scale-csi-node -c scale-csi
```

### Verify TrueNAS Connectivity

```bash
kubectl exec -n scale-csi deploy/scale-csi-controller -c scale-csi -- \
  cat /tmp/truenas-connection-status 2>/dev/null || echo "No status file"
```

### Check CSI Driver Registration

```bash
kubectl get csidrivers
kubectl get csinodes
```

## License

Apache 2.0
