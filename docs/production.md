# Production deployment

This guide describes the behavior implemented by scale-csi v1.2.0 and the
bundled Helm chart. Review the [deployment guide](deployment.md) for installation
examples and the chart's [values reference](../charts/scale-csi/README.md) for
every setting.

## Prerequisites

### TrueNAS and API access

The NFS and iSCSI clients support the snapshot API generations used by TrueNAS
SCALE 24.x (`zfs.snapshot.*`), 25.04+ (`pool.snapshot.*`), and 26.0
(`zfs.resource.snapshot.*` for the reads and rename operations that moved). The
client probes and caches the available generation. NVMe-oF is different: the
driver rejects it before TrueNAS 25.10.

The controller plane has been validated live against a real TrueNAS 26.0
system: the official csi-sanity controller suites pass 52/52 for NFS and 52/52
for iSCSI against real datasets, zvols, shares, targets, and extents (node
specs excluded — they require real initiator hosts). That validation is what
surfaced the 26.0 middleware behaviors documented under Known limitations.
Still validate your exact TrueNAS patch release and protocol in a staging
cluster before production, and node-path behavior end to end.

Use a user-linked API key over HTTPS. API keys inherit the roles of their user.
On role-based TrueNAS releases, the built-in `SHARING_ADMIN` plus
`REPLICATION_ADMIN` roles cover the dataset/share and snapshot operations used
by the driver; `FULL_ADMIN` is not required. A custom privilege must cover the
equivalent dataset create/update/delete, NFS/iSCSI/NVMe sharing, snapshot
read/create/update/clone/rename/delete, service read/reload, and `system.info`
operations. Role names and method assignments differ between TrueNAS API
generations, so confirm a custom privilege against the API documentation served
by the target appliance. See the TrueNAS [role reference][truenas-rbac].

### Network and nodes

Allow the following paths; do not expose storage ports beyond the node networks:

| Source | Destination | Port | Purpose |
|---|---|---:|---|
| Controller and node pods | TrueNAS API | TCP 443 | JSON-RPC 2.0 WebSocket (`wss://<host>:443/api/current`) |
| Kubernetes nodes | TrueNAS NFS | TCP 2049 | NFS volume mounts |
| Kubernetes nodes | TrueNAS iSCSI portals | TCP 3260 | iSCSI discovery, login, and I/O |
| Kubernetes nodes | TrueNAS NVMe/TCP target | TCP 4420 | NVMe discovery, connect, and I/O |

The node image invokes host storage tools. Install the NFS client and kernel NFS
support for NFS; `iscsiadm`, `iscsid`, and the `iscsi_tcp` initiator module for
iSCSI; or `nvme-cli` and the `nvme_tcp`/`nvme_fabrics` modules for NVMe/TCP.
The chart mounts the host device, sysfs, udev, kubelet, and iSCSI paths; it does
not install host packages or load modules.

## Availability and outage behavior

For controller availability, use at least two `controller.replicas`. The
provisioner, attacher, resizer, and snapshotter sidecars all enable Lease-based
leader election, so only the elected sidecars issue CSI controller requests.
Enable `controller.podDisruptionBudget` only with multiple replicas and choose a
single non-empty `minAvailable` or `maxUnavailable`. The defaults use
`system-cluster-critical` for controller pods and `system-node-critical` for the
node DaemonSet.

The node component runs as a DaemonSet on all tolerated nodes. Established node
pods perform stage, publish, unpublish, and unstage through host NFS/iSCSI/NVMe
tools rather than through TrueNAS management API calls. During a management API
outage their readiness endpoint reports unavailable, and controller operations
fail or retry. A node pod that restarts while TrueNAS is unreachable cannot
initialize because every driver process establishes at least one API connection
at startup.

The API retry and circuit-breaker behavior comes from this values block:

```yaml
resilience:
  circuitBreaker:
    enabled: false
    failureThreshold: 5
    timeout: 30
  retry:
    maxAttempts: 3
    initialDelay: 500
    maxDelay: 5000
    backoffMultiplier: 2.0
  rateLimiting:
    maxConcurrentRequests: 10
    maxConcurrentLogins: 2
```

Retries apply only to connection-class failures; an ambiguous non-idempotent
mutation is not retried. The chart disables the circuit breaker by default. If
enabled, five consecutive failures open it for 30 seconds before half-open
probes are admitted. These controls do not replace protocol-level mount/login
timeouts under `commandTimeouts`.

## Security

- Prefer an externally managed Secret and set `truenas.existingSecret`; it must
  be in the release namespace and contain `api-key`. Do not also set
  `truenas.apiKey`. Rotate the TrueNAS key and Secret together.
- Set `nfs.shareAllowedNetworks` to the node CIDRs. Its empty default permits all
  networks accepted by TrueNAS for each dynamically created share.
- NVMe-oF host-NQN allowlisting is not functional in v1.2.0. Although the chart
  renders `nvmeof.subsystemHosts`, the 25.10+ create path does not resolve those
  NQNs to TrueNAS host IDs and passes no hosts. The default
  `nvmeof.subsystemAllowAnyHost: true` permits any host; setting it to `false`
  without separately managing host associations can create an unusable
  subsystem. Do not expose driver-managed NVMe-oF to an untrusted network.
- The chart's controller and node service accounts are separate, but both use
  ClusterRoles. The controller role can list Secrets cluster-wide for CSI
  sidecars. For strict least privilege, supply audited service accounts and
  RBAC with `serviceAccount.create: false` and `rbac.create: false`, limiting
  Secret reads to the namespaces and names referenced by StorageClasses and
  snapshot classes.
- The node driver is intentionally privileged with `SYS_ADMIN`, host PID/network
  access, hostPath mounts, and bidirectional mount propagation. The shared pod
  security context runs as root (`runAsNonRoot: false`, `fsGroup: 0`), and the
  chart does not set a seccomp profile. Isolate the namespace, enforce image
  provenance, and restrict who can alter the DaemonSet or its service account.

## Monitoring

`metrics.enabled` creates controller and headless node metrics Services.
Prometheus Operator users can enable `metrics.serviceMonitor.enabled`; enable
`metrics.prometheusRule.enabled` for the bundled rules and
`metrics.dashboards.enabled` for a Grafana sidecar-discoverable ConfigMap.

Watch these series:

- `scale_csi_operations_total` and `scale_csi_operations_duration_seconds` for
  CSI error rate and latency;
- `scale_csi_truenas_requests_total` and
  `scale_csi_truenas_requests_duration_seconds` for backend API health;
- `scale_csi_truenas_connection_status` for connectivity;
- `scale_csi_circuit_breaker_state`,
  `scale_csi_circuit_breaker_current_failures`, and the breaker counters for
  outage protection;
- `scale_csi_iscsi_sessions_total` for the sessions observed by node session
  garbage collection.

The registry also exposes `scale_csi_truenas_connections_active`,
`scale_csi_volumes_total`, and `scale_csi_snapshots_total`, but v1.2.0 does not
populate those gauges. Do not build production alerts from them.

The bundled rules alert when the controller target is absent for five minutes,
the circuit is open for two minutes, TrueNAS API failures exceed 10% for ten
minutes, or CSI operation errors exceed 0.01 operations/second for ten minutes.
Tune these thresholds to workload volume; ratios can be noisy at low traffic.

## Upgrades

1. Render and validate the release before applying it:

   ```bash
   helm lint charts/scale-csi
   helm template scale-csi charts/scale-csi \
     --set truenas.host=truenas.example \
     --set truenas.existingSecret=scale-csi-api \
     --set zfs.parentDataset=tank/kubernetes >/tmp/scale-csi.yaml
   ```

   `values.schema.json` rejects unknown and invalid values during Helm
   validation. It cannot validate backend reachability, credentials, host
   packages, or protocol configuration.

2. Do not change the immutable `CSIDriver.spec.attachRequired` or
   `volumeLifecycleModes` fields in place. The chart hard-codes them. A future
   change requires a deliberately planned delete/recreate of the `CSIDriver`
   object after workload impact is understood. Other `CSIDriver` fields are not
   all immutable on current Kubernetes releases; consult the target cluster's
   API reference rather than relying on the template comment. See the
   Kubernetes [`CSIDriver` API][csidriver-api].

3. ConfigMap changes roll both controller and node pods through checksum
   annotations. Changes to the chart-managed Secret do the same. Changes to an
   `existingSecret` do **not** alter a checksum annotation, so restart both
   workloads explicitly after rotating that Secret.

4. The chart pins provisioner, attacher, resizer, snapshotter,
   registrar, and liveness images. CSI sidecars have independent Kubernetes and
   CSI compatibility matrices; do not upgrade one image in isolation without
   checking its release notes. Snapshot support also requires cluster-installed
   snapshot CRDs and the common snapshot controller, which this chart does not
   install. Keep their API generation compatible with the snapshotter.

5. The repository does not define or test a controller/node version-skew matrix.
   Roll controller and node together, and exercise provision, attach, mount,
   expand, snapshot, restore, unmount, and delete in a staging namespace before
   production rollout.

## Known limitations in v1.2.0

- Fake-command conformance does not cover the iSCSI or NVMe-oF node paths.
  iSCSI runs the controller portion of `csi-sanity`; NVMe-oF has unit/controller
  tests but no protocol-specific sanity suite. Neither substitutes for node tests
  with real block devices and a real target.
- Live validation against a real TrueNAS 26.0 appliance now covers the full
  node plane on a real initiator host: csi-sanity including Node Service specs
  passes for NFS (75/75), iSCSI (real iscsiadm logins, device staging, mkfs,
  mounts), and NVMe-oF (real fabric connects). Tests named `e2e` in this
  repository use `MockClient`.
- NVMe-oF on TrueNAS 25.10+ REQUIRES `nvmeof.subsystemAllowAnyHost: true`:
  per-host NQN allowlisting is ignored by the middleware (see above), so
  subsystems without allow-any-host reject every fabric connect with
  "failed to write to nvme-fabrics device" (validated live). Use network
  segmentation (VLANs/SGACLs) as the effective access control for 4420.
- A TrueNAS NVMe-oF listener only materializes on a configured port once at
  least one subsystem is associated with it — a bare port shows no kernel
  listener, which is normal and self-resolves on first volume creation.
- `ControllerModifyVolume` returns `Unimplemented`. CSI volume group snapshot
  services are not registered or implemented.
- CSI volume and snapshot names share `sanitizeVolumeID`: `/` and spaces become
  `-`, a non-alphanumeric first byte is prefixed with `v`, and the result is
  truncated to 128 bytes. It is a byte-oriented sanitizer, not a general Unicode
  or arbitrary-ZFS-name normalizer. Snapshot short names are global within the
  configured parent from the CSI driver's perspective.
- Deleting a snapshot that still has clones renames it to an internal tombstone
  and requests deferred ZFS destruction. The snapshot disappears from CSI, but
  its referenced space remains charged until the last clone releases it.
- After upgrading a NAS from TrueNAS 25.x to 26.0, CSI snapshots created by
  older driver versions without `truenas-csi:csi_snapshot_name` are omitted
  from `ListSnapshots`. Restore and deletion by snapshot ID continue to work.
- TrueNAS 26.0 (including beta builds) silently ignores
  `pool.snapshot.update` requests that use `user_properties_update` or
  `user_properties_remove`. The driver writes snapshot identity properties at
  creation for correctness; tombstone names, rather than property removal, hide
  deferred deletions. This middleware behavior should be reported upstream.
- Driver-managed NVMe-oF host allowlisting is unavailable, as described in the
  security section.

[truenas-rbac]: https://api.truenas.com/v26.0/rbac.html
[csidriver-api]: https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/csi-driver-v1/
