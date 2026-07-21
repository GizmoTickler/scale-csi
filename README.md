# Scale CSI Driver

[![Build Status](https://img.shields.io/github/actions/workflow/status/GizmoTickler/scale-csi/ci.yml?branch=main&style=flat-square)](https://github.com/GizmoTickler/scale-csi/actions)
[![License](https://img.shields.io/github/license/GizmoTickler/scale-csi?style=flat-square)](LICENSE)

A Kubernetes CSI driver purpose-built for TrueNAS SCALE. Unlike general-purpose storage drivers that support multiple backends, Scale CSI focuses exclusively on TrueNAS SCALE 25.04+ and its modern WebSocket API.

## Why Scale CSI?

- **Zero SSH** - Communicates entirely via WebSocket JSON-RPC 2.0 (`wss://host/api/current`)
- **Single Focus** - Optimized specifically for TrueNAS SCALE, not a multi-backend abstraction
- **Modern API** - Built for SCALE 25.04+ versioned API from day one
- **Full Featured** - Snapshots, clones, volume expansion, and raw block volumes
- **Backend Fencing** - CSI publish state is enforced through per-volume NFS,
  iSCSI, and NVMe-oF transport allowlists

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

New installations default to `fencing.mode: additive`. The node plugin embeds
its NFS IP, iSCSI initiator IQN, and NVMe host NQN in a versioned CSI node ID;
the controller persists those identities and enforces them in TrueNAS. Before
switching an upgraded installation to `strict`, follow the ownership and
rolling-upgrade procedure in the [chart documentation](charts/scale-csi/README.md#publication-fencing-and-ownership).

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

Deletion is fail-safe by default: if a volume has snapshots that were not
created by the CSI driver, `DeleteVolume` leaves the dataset intact. Set
`zfs.destroyForeignSnapshotsOnDelete: true` only when those snapshots are
deliberately disposable. To advertise a scheduler attach limit, set
`node.maxVolumesPerNode` to a positive value; its default `0` remains unlimited.

### Resource sizing

Steady-state use is approximately 15Mi memory and 1m CPU per driver container.
The Helm chart defaults the controller and node driver requests to 10m CPU and
32Mi memory, with no limits. If you configure CPU or memory limits, the Go
runtime adapts `GOMAXPROCS` and `GOMEMLIMIT` from the container cgroups.

CSI liveness now reports driver-process health rather than TrueNAS reachability,
so a NAS outage or slow reconnect does not cause a kubelet restart loop. Monitor
`/readyz` or `scale_csi_truenas_connection_status` for backend availability.

| Guide | Description |
|-------|-------------|
| [Architecture](docs/architecture.md) | How the driver works internally |
| [Deployment](docs/deployment.md) | Helm and Flux installation with full configuration reference |
| [Production](docs/production.md) | HA, security, monitoring, upgrades, and known limitations |
| [Disaster recovery](docs/guides/disaster-recovery.md) | ZFS replication + export auto-recreation for cross-site failover |
| [Topology](docs/guides/topology.md) | Zone/region-aware provisioning (advanced; single-backend usually doesn't need it) |
| [Snapshots](docs/guides/snapshots.md) | Snapshot and clone/restore workflow |

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
