# scale-csi documentation

New here? Read [Getting started](getting-started.md): it goes from an empty
TrueNAS and cluster to a mounted volume, then points to everything below.

## Getting started

| Page | What it covers |
|---|---|
| [Getting started](getting-started.md) | TrueNAS preparation (dataset, least-privilege API key, services), node packages per distribution, Helm install, test PVC, clean-up |
| [Examples](../examples/README.md) | Helm values per protocol and for production, Flux and Argo CD manifests, workload manifests |

## Guides

| Page | What it covers |
|---|---|
| [Installation](guides/installation.md) | Helm, Flux, Argo CD, and Nomad installs; key configuration decisions; capacity-aware scheduling |
| [Upgrades](guides/upgrades.md) | Upgrading and rolling back the chart, rotating the API key, and upgrading TrueNAS underneath the driver |
| [Snapshots and clones](guides/snapshots.md) | Snapshot CRDs, VolumeSnapshotClass, restore modes, clones, VolSync |
| [Encryption at rest](guides/encryption.md) | Encrypted StorageClasses and their operational trade-offs |
| [Disaster recovery](guides/disaster-recovery.md) | ZFS replication to a second TrueNAS and failover runbooks |
| [Topology](guides/topology.md) | Zone/region labels and multiple backends |
| [Nomad](guides/nomad.md) | Running the driver as Nomad CSI plugins |

## Operations

| Page | What it covers |
|---|---|
| [Production](production.md) | The production contract: API privileges, network, HA and outage behavior, security, monitoring, upgrade rules, known limitations |
| [Observability](observability.md) | Metrics endpoints, ServiceMonitor, PrometheusRule, Grafana dashboard, logs |
| [Troubleshooting](troubleshooting.md) | Diagnostics, common failures, recovery procedures, and the alert → runbook table |

These three pages stay at the top of `docs/` on purpose: the PrometheusRule
alerts shipped in the chart link to them by path.

## Reference

| Page | What it covers |
|---|---|
| [Compatibility](reference/compatibility.md) | Supported TrueNAS, Kubernetes, and node operating systems, and how each is tested |
| [StorageClass parameters](reference/storageclass.md) | Every parameter the driver understands, per protocol, including CHAP, NFS ACLs, encryption, and ublk |
| [Chart values](../charts/scale-csi/README.md) | Every Helm value, with defaults |
| [TrueNAS API map](reference/truenas-api-map.md) | The TrueNAS API surface, generated from a live appliance |
| [TrueNAS API gap analysis](reference/truenas-api-gap-analysis.md) | Which TrueNAS methods the driver uses, and which it could |
| [Release notes](release-notes-next.md) | Changes per release, with upgrade actions |

## Development

| Page | What it covers |
|---|---|
| [Contributing](../.github/CONTRIBUTING.md) | Building, testing, and submitting changes |
| [Architecture](development/architecture.md) | How the controller, node agents, and TrueNAS client work |
| [Fuzzing](development/fuzzing.md) | Running and extending the fuzz targets |
| [Design specs](development/specs/README.md) | Historical batch specifications (not maintained) |
