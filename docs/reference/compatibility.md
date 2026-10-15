# Compatibility

This page is the single source of truth for what scale-csi supports. Other
pages link here instead of repeating version numbers.

## TrueNAS

| TrueNAS release | Status | Notes |
|---|---|---|
| 27.0 | Supported | TrueNAS 27 removed the REST API. scale-csi uses only the WebSocket JSON-RPC 2.0 API (`wss://<host>:<port>/api/current`), so it is unaffected. |
| 26.x | Supported | The release the maintainer runs in production. The driver detects and caches the 26.0 `zfs.resource.snapshot.*` API generation automatically. |
| 25.10 | Supported | Oldest release with NVMe-oF support in the driver; the driver rejects NVMe-oF on anything older. |
| 25.04 | Not supported | The NFS and iSCSI clients still understand the 25.04 snapshot API generation, but this release is no longer tested. |
| 24.x and older | Not supported | The 24.x `zfs.snapshot.*` API generation is no longer supported by the client. |

Before any TrueNAS upgrade read
[Upgrading TrueNAS underneath scale-csi](../guides/upgrades.md#upgrading-truenas).

### Edition and services

The driver needs nothing beyond what the Community Edition ships: a pool, a
parent dataset, an API key, and the NFS, iSCSI, and/or NVMe-oF service for the
protocols you use. NVMe-oF uses the TCP transport (`nvmeof.transport: tcp`);
the RDMA transport value exists in the chart but is not validated.

## Kubernetes

| Item | Requirement |
|---|---|
| Kubernetes | 1.20 or later — the floor the chart enforces (`kubeVersion: ">=1.20.0-0"` in `Chart.yaml`). Run a Kubernetes minor that is still under upstream support; the pinned CSI sidecars track current upstream releases. |
| Helm | 3.8 or later (OCI registry support). CI uses Helm 4. |
| Pod Security | The install namespace must allow `privileged` pods (the node plugin needs host access). |
| Snapshots | The VolumeSnapshot CRDs and the external snapshot-controller, installed separately — see the [snapshot guide](../guides/snapshots.md#prerequisites). |
| `CSIStorageCapacity` | Optional (`capacity.enabled`). `CSIDriver.spec.storageCapacity` is immutable on Kubernetes 1.20–1.22. |

The CSI sidecar images and versions are pinned in
[`charts/scale-csi/values.yaml`](../../charts/scale-csi/values.yaml) under
`sidecars.*.image` and are kept current by Renovate. Each sidecar has its own
Kubernetes compatibility matrix upstream.

## Nodes

Images are published for `linux/amd64` only.

| Protocol | Host requirement |
|---|---|
| NFS | Kernel NFS client support (`nfs`, `nfsv4` modules). The node image ships `mount.nfs`; installing the distro NFS client package on the host is still recommended. |
| iSCSI | `iscsiadm` on the host and `iscsid` running. Optional multipath additionally needs `multipathd` and device-mapper. |
| NVMe-oF | `nvme` (nvme-cli) on the host and the `nvme-tcp` module loaded. The optional userspace data path (ublk) needs the `ublk_drv` module. |

Per-distribution install commands, including Flatcar, Fedora CoreOS, and
Talos notes, are in [Getting started](../getting-started.md#3-prepare-the-kubernetes-nodes).

The node plugin runs host binaries (`iscsiadm`, `nvme`, `multipathd`) through
small wrappers in the image that `chroot` into the host root or `nsenter` the
host's mount namespace; it does not install host packages or load kernel
modules for you.

## What is tested, and how

- **In repository CI:** unit tests, the official `csi-sanity` conformance suite
  against a mock TrueNAS backend (NFS full surface; iSCSI controller surface),
  chart rendering and schema tests, fuzz smoke tests, and the Rust node agent's
  tests. None of these talk to a real TrueNAS.
- **Maintainer-run, out of band:** `csi-sanity` controller suites against a real
  TrueNAS 26.0 system and node-plane suites on real Linux initiators for NFS,
  iSCSI, and NVMe-oF, plus continuous production use on TrueNAS 26.x with Flatcar Container
  Linux nodes.

Validate your exact TrueNAS release, protocol, and node operating system in a
staging cluster before production. The [production guide](../production.md)
lists the known limitations.
