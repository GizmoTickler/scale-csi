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

| StorageClass `protocol` | TrueNAS version | Use case |
|---|---|---|
| `nfs` | 25.04+ | Shared filesystem access (ReadWriteMany) |
| `iscsi` | 25.04+ | Single-path block storage |
| `nvmeof` | 25.10+ | High-performance block storage |

All classes use the unified provisioner name `csi.scale.io`. A multi-protocol
deployment requires the `protocol` StorageClass parameter so an omitted value
cannot accidentally provision NFS.

## Quick Start

```bash
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --create-namespace \
  --values values.yaml
```

Fencing defaults to `off`. `additive` is the explicit migration mode: upgrade
the node DaemonSet first, wait for every CSINode to re-register its versioned
transport identity, and only then enable `additive`. Enable `strict` only after
`scale_csi_fencing_deferred_total` remains at zero. Follow the complete
[rolling-upgrade procedure](charts/scale-csi/README.md#publication-fencing-and-ownership).

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

Block storage via iSCSI requires the initiator tools. scale-csi currently uses
one portal and refuses devices claimed by dm-multipath; do not enable multipath
for its LUNs.

**Debian/Ubuntu:**
```bash
apt-get install -y open-iscsi
systemctl enable --now iscsid
```

**RHEL/Fedora:**
```bash
dnf install -y iscsi-initiator-utils
systemctl enable --now iscsid
```

**Verification:**
```bash
iscsiadm --version
cat /etc/iscsi/initiatorname.iscsi  # Should show iqn.* identifier
```

The driver does not implement CHAP. Restrict TCP 3260 to the Kubernetes nodes
on a dedicated storage network (for example, a storage VLAN plus firewall or
SGACL policy).

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

The chart configures one NVMe-oF address and service ID. Multi-path NVMe-oF has
not been validated as a scale-csi deployment mode.

</details>

<details>
<summary><strong>Talos Linux</strong></summary>

Talos is not currently a turnkey chart target. The node pod already sets
`hostPID: true`, and the only supported environment override path is
`node.extraEnv`; values such as `node.hostPID`, `node.driver.extraEnv`, and
`node.iscsiDirHostPath` do not exist. The chart currently mounts the host's
`/etc/iscsi` and `/var/lib/iscsi` paths directly, so validate tool and state-path
compatibility on the exact Talos release before treating it as supported.

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
The Helm chart defaults each driver container to requests of 10m CPU and 32Mi
memory with a 256Mi memory limit. Each CSI sidecar requests 10m CPU and 32Mi
memory with a 128Mi memory limit. All resource maps are overridable. The Go
runtime adapts `GOMAXPROCS` and `GOMEMLIMIT` from the driver container cgroups.

CSI liveness now reports driver-process health rather than TrueNAS reachability,
so a NAS outage or slow reconnect does not cause a kubelet restart loop. Monitor
`/readyz` or `scale_csi_truenas_connection_status` for backend availability.

### Verify release signatures

Tag builds keyless-sign the multi-architecture image, keyless-sign the OCI Helm
chart, and attach an SLSA provenance attestation to the chart. Substitute the
release being installed:

```bash
VERSION=vX.Y.Z
IMAGE="ghcr.io/gizmotickler/scale-csi:${VERSION}"
CHART="ghcr.io/gizmotickler/charts/scale-csi:${VERSION#v}"
ISSUER="https://token.actions.githubusercontent.com"

cosign verify \
  --certificate-identity "https://github.com/GizmoTickler/scale-csi/.github/workflows/ci.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"
cosign verify \
  --certificate-identity "https://github.com/GizmoTickler/scale-csi/.github/workflows/helm-release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "$ISSUER" \
  "$CHART"
cosign verify-attestation \
  --type slsaprovenance \
  --certificate-identity "https://github.com/GizmoTickler/scale-csi/.github/workflows/helm-release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "$ISSUER" \
  "$CHART"
```

Release-image Trivy scans fail on unallowlisted HIGH or CRITICAL findings. The
documented exception ledger is [`.trivyignore`](.trivyignore).

| Guide | Description |
|-------|-------------|
| [Architecture](docs/architecture.md) | How the driver works internally |
| [Deployment](docs/deployment.md) | Helm and Flux installation with full configuration reference |
| [Production](docs/production.md) | HA, security, monitoring, upgrades, and known limitations |
| [Disaster recovery](docs/guides/disaster-recovery.md) | ZFS replication + export auto-recreation for cross-site failover |
| [Topology](docs/guides/topology.md) | Zone/region-aware provisioning (advanced; single-backend usually doesn't need it) |
| [Snapshots](docs/guides/snapshots.md) | Snapshot and clone/restore workflow |
| [Next release notes](docs/release-notes-next.md) | Draft breaking changes and upgrade actions after v1.2.23 |

## Network Ports

Ensure these ports are accessible from your Kubernetes nodes to TrueNAS:

| Service | Port | Required For |
|---------|------|--------------|
| HTTPS | 443 | WebSocket API (always required) |
| NFS | 2049 | NFS volumes |
| iSCSI | 3260 | iSCSI volumes |
| NVMe-TCP | 4420 | NVMe-oF volumes |

## License

MIT - see [LICENSE](LICENSE)

---

*This project is not affiliated with iXsystems. TrueNAS is a registered trademark of iXsystems, Inc.*
