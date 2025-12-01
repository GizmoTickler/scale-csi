# Scale CSI Driver

[![Build Status](https://img.shields.io/github/actions/workflow/status/GizmoTickler/scale-csi/ci.yml?branch=main&style=flat-square)](https://github.com/GizmoTickler/scale-csi/actions)
[![License](https://img.shields.io/github/license/GizmoTickler/scale-csi?style=flat-square)](LICENSE)

A Kubernetes CSI driver purpose-built for TrueNAS SCALE. Unlike general-purpose storage drivers that support multiple backends, Scale CSI focuses exclusively on TrueNAS SCALE 25.04+ and its modern WebSocket API.

## Why Scale CSI?

- **Zero SSH** - Communicates entirely via WebSocket JSON-RPC 2.0 (`wss://host/api/current`)
- **Single Focus** - Optimized specifically for TrueNAS SCALE, not a multi-backend abstraction
- **Modern API** - Built for SCALE 25.04+ versioned API from day one
- **Full Featured** - Snapshots, clones, volume expansion, and raw block volumes

## Supported Protocols

| Driver | Protocol | Min Version | Use Case |
|--------|----------|-------------|----------|
| `scale-nfs` | NFS | 25.04+ | Shared filesystem access (ReadWriteMany) |
| `scale-iscsi` | iSCSI | 25.04+ | Block storage with multipath support |
| `scale-nvmeof` | NVMe-oF | 25.10+ | High-performance block storage |

## Quick Start

```bash
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --create-namespace \
  --values values.yaml
```

## Requirements

### TrueNAS SCALE Setup

1. **Generate an API Key**: Settings → API Keys → Add
2. **Create parent datasets** for your volumes:
   ```
   tank/k8s/volumes    # For persistent volumes
   tank/k8s/snapshots  # For volume snapshots (sibling, not nested)
   ```
3. **Enable the storage service** you plan to use (NFS, iSCSI, or NVMe-oF)

The driver handles all share/target/subsystem creation automatically.

### Kubernetes Node Requirements

Your cluster nodes need the appropriate client tools installed for your chosen protocol.

<details>
<summary><strong>NFS Client Setup</strong></summary>

The NFS client package is required to mount NFS exports.

**Debian/Ubuntu:**
```bash
apt-get install -y nfs-common
```

**RHEL/Fedora:**
```bash
dnf install -y nfs-utils
```

**Verification:**
```bash
# Should return without error
showmount --version
```

</details>

<details>
<summary><strong>iSCSI Client Setup</strong></summary>

Block storage via iSCSI requires the initiator tools and optionally multipath for redundancy.

**Debian/Ubuntu:**
```bash
apt-get install -y open-iscsi
systemctl enable --now iscsid

# Optional: multipath for multiple portals
apt-get install -y multipath-tools
systemctl enable --now multipathd
```

**RHEL/Fedora:**
```bash
dnf install -y iscsi-initiator-utils
systemctl enable --now iscsid

# Optional: multipath for multiple portals
dnf install -y device-mapper-multipath
mpathconf --enable --with_multipathd y
systemctl enable --now multipathd
```

**Verification:**
```bash
iscsiadm --version
cat /etc/iscsi/initiatorname.iscsi  # Should show iqn.* identifier
```

</details>

<details>
<summary><strong>NVMe-oF Client Setup</strong></summary>

NVMe over Fabrics requires kernel module support and the nvme-cli tools.

**Load kernel modules:**
```bash
modprobe nvme-tcp  # For NVMe/TCP (most common)
# modprobe nvme-rdma  # For NVMe/RDMA
# modprobe nvme-fc    # For NVMe/FC

# Persist across reboots
echo "nvme-tcp" >> /etc/modules-load.d/nvme.conf
```

**Install management tools:**
```bash
# Debian/Ubuntu
apt-get install -y nvme-cli

# RHEL/Fedora
dnf install -y nvme-cli
```

**Verification:**
```bash
nvme version
lsmod | grep nvme_tcp
```

**Multipath Note:** NVMe supports native multipath (built into nvme-core) or device-mapper multipath. Check your current setting:
```bash
cat /sys/module/nvme_core/parameters/multipath  # Y = native, N = dm-multipath
```

</details>

<details>
<summary><strong>Talos Linux</strong></summary>

Talos requires system extensions since the base image is immutable.

**For iSCSI**, add the extension to your machine config:
```yaml
machine:
  install:
    extensions:
      - image: ghcr.io/siderolabs/iscsi-tools:v0.1.6
```

Then configure the CSI driver to use nsenter for host access:
```yaml
# values.yaml
node:
  hostPID: true
  driver:
    extraEnv:
      - name: ISCSIADM_HOST_STRATEGY
        value: nsenter
      - name: ISCSIADM_HOST_PATH
        value: /usr/local/sbin/iscsiadm
    iscsiDirHostPath: /usr/local/etc/iscsi
    iscsiDirHostPathType: ""
```

See [Talos System Extensions](https://www.talos.dev/latest/talos-guides/configuration/system-extensions/) for details.

</details>

### Namespace Configuration

The CSI node driver requires privileged access. Label your namespace accordingly:

```bash
kubectl label namespace scale-csi pod-security.kubernetes.io/enforce=privileged
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Architecture](docs/architecture.md) | How the driver works internally |
| [Deployment](docs/deployment.md) | Helm and Flux installation with full configuration reference |

## Network Ports

Ensure these ports are accessible from your Kubernetes nodes to TrueNAS:

| Service | Port | Required For |
|---------|------|--------------|
| HTTPS | 443 | WebSocket API (always required) |
| NFS | 2049 | `scale-nfs` driver |
| iSCSI | 3260 | `scale-iscsi` driver |
| NVMe-TCP | 4420 | `scale-nvmeof` driver |

## License

MIT - see [LICENSE](LICENSE)

---

*This project is not affiliated with iXsystems. TrueNAS is a registered trademark of iXsystems, Inc.*
