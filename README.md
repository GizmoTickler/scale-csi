# Scale CSI Driver

[![Build Status](https://img.shields.io/github/actions/workflow/status/GizmoTickler/scale-csi/ci.yml?branch=main&style=flat-square)](https://github.com/GizmoTickler/scale-csi/actions)
[![Release](https://img.shields.io/github/v/release/GizmoTickler/scale-csi?style=flat-square)](https://github.com/GizmoTickler/scale-csi/releases/latest)
[![License](https://img.shields.io/github/license/GizmoTickler/scale-csi?style=flat-square)](LICENSE)

A Kubernetes [CSI](https://kubernetes-csi.github.io/docs/) driver built for
TrueNAS SCALE. It provisions ZFS-backed persistent volumes over **NFS**,
**iSCSI**, and **NVMe-oF (NVMe/TCP)** and talks to TrueNAS only through its
WebSocket JSON-RPC 2.0 API (`wss://<host>/api/current`) — no SSH, no REST.

- **One driver, three protocols** — every StorageClass uses the provisioner
  `csi.scale.io` and selects `protocol: nfs | iscsi | nvmeof`.
- **Full volume lifecycle** — dynamic provisioning, online expansion,
  snapshots, restores (clone or fully detached copy), PVC-to-PVC clones, and
  raw block volumes.
- **Safe by default** — the driver only touches datasets under the one parent
  you give it, refuses to delete data it cannot prove it owns, and keeps
  durable per-volume publication records for single-node attach exclusivity.
- **Production features** — optional backend fencing, iSCSI CHAP, ZFS-native
  encryption at rest, NFS session trunking, iSCSI and NVMe-oF multipath,
  Prometheus metrics, alerts, and a Grafana dashboard.
- **Two node agents in one image** — a Rust node agent (default) and the Go
  node plugin; switching between them needs no volume work.

## Compatibility

| Component | Supported |
|---|---|
| TrueNAS SCALE / Community Edition | 25.10, 26.x, 27.0 |
| Kubernetes | 1.20+ (chart floor); use a release still in upstream support |
| Helm | 3.8+ (OCI chart support) |
| Node architectures | `linux/amd64` images |

> **TrueNAS 27 removed the REST API.** scale-csi never used it: it speaks the
> WebSocket JSON-RPC API exclusively, so the REST removal does not affect it.

| `protocol` | Access modes | Volume modes | Node requirement |
|---|---|---|---|
| `nfs` | RWO, ROX, RWX | Filesystem | Kernel NFS client |
| `iscsi` | RWO | Filesystem, Block | `open-iscsi` (`iscsiadm` + running `iscsid`) |
| `nvmeof` | RWO | Filesystem, Block | `nvme-cli` + `nvme-tcp` kernel module |

Details, including what is and is not tested, are in the
[compatibility reference](docs/reference/compatibility.md).

## Quick start

You need a TrueNAS parent dataset, an API key, and the protocol's service
running. The [Getting started guide](docs/getting-started.md) walks through all
of it, including TrueNAS setup and node packages; the short version for NFS:

```bash
# 1. Namespace (the node plugin is privileged) and API-key Secret
kubectl create namespace scale-csi
kubectl label namespace scale-csi pod-security.kubernetes.io/enforce=privileged
kubectl -n scale-csi create secret generic truenas-api-key \
  --from-literal=api-key='<your-truenas-api-key>'

# 2. Install the chart from the OCI registry (latest release)
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi \
  --set truenas.host=truenas.example.com \
  --set truenas.existingSecret=truenas-api-key \
  --set zfs.parentDataset=tank/k8s/volumes \
  --set iscsi.enabled=false

# 3. Try it
kubectl apply -f https://raw.githubusercontent.com/GizmoTickler/scale-csi/main/examples/workloads/nfs-pvc-pod.yaml
kubectl get pvc scale-nfs-test -w
```

The chart creates a `scale-nfs` StorageClass by default. For iSCSI or
NVMe-oF, start from the matching values file in
[`examples/helm/`](examples/helm/).

## Documentation

| | |
|---|---|
| **Start here** | [Getting started](docs/getting-started.md) — zero to a working PVC |
| **Examples** | [examples/](examples/README.md) — Helm values, Flux, Argo CD, workload manifests |
| **Guides** | [Installation](docs/guides/installation.md) · [Upgrades & TrueNAS upgrades](docs/guides/upgrades.md) · [Snapshots](docs/guides/snapshots.md) · [Encryption](docs/guides/encryption.md) · [Disaster recovery](docs/guides/disaster-recovery.md) · [Topology](docs/guides/topology.md) · [Nomad](docs/guides/nomad.md) |
| **Operations** | [Production checklist](docs/production.md) · [Observability](docs/observability.md) · [Troubleshooting](docs/troubleshooting.md) |
| **Reference** | [StorageClass parameters](docs/reference/storageclass.md) · [Chart values](charts/scale-csi/README.md) · [Compatibility](docs/reference/compatibility.md) · [TrueNAS API map](docs/reference/truenas-api-map.md) |
| **Development** | [Architecture](docs/development/architecture.md) · [Contributing](.github/CONTRIBUTING.md) · [Fuzzing](docs/development/fuzzing.md) |
| **Changes** | [Release notes](docs/release-notes-next.md) · [GitHub releases](https://github.com/GizmoTickler/scale-csi/releases) |

The full index is [docs/README.md](docs/README.md).

## Network ports

| Port | From | Purpose |
|---|---|---|
| TCP 443 | Controller pods only | TrueNAS WebSocket API (node pods hold no API credentials) |
| TCP 2049 | Every node | NFS |
| TCP 3260 | Every node | iSCSI |
| TCP 4420 | Every node | NVMe/TCP |

## Verifying releases

Release images and the OCI chart are keyless-signed with cosign, and the
chart carries an SLSA provenance attestation:

```bash
# The latest release; set VERSION=vX.Y.Z instead to verify a pinned one.
VERSION=$(curl -fsSL https://api.github.com/repos/GizmoTickler/scale-csi/releases/latest | jq -r .tag_name)
ISSUER="https://token.actions.githubusercontent.com"
REPO="https://github.com/GizmoTickler/scale-csi/.github/workflows"

cosign verify \
  --certificate-identity "${REPO}/ci.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "${ISSUER}" \
  "ghcr.io/gizmotickler/scale-csi:${VERSION}"
cosign verify \
  --certificate-identity "${REPO}/helm-release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "${ISSUER}" \
  "ghcr.io/gizmotickler/charts/scale-csi:${VERSION#v}"
cosign verify-attestation --type slsaprovenance \
  --certificate-identity "${REPO}/helm-release.yml@refs/tags/${VERSION}" \
  --certificate-oidc-issuer "${ISSUER}" \
  "ghcr.io/gizmotickler/charts/scale-csi:${VERSION#v}"
```

Release-image Trivy scans fail on unallowlisted HIGH or CRITICAL findings; the
documented exceptions are in [`.trivyignore`](.trivyignore).

## License

MIT — see [LICENSE](LICENSE).

---

*This project is not affiliated with iXsystems. TrueNAS is a registered
trademark of iXsystems, Inc.*
