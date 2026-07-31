# Production deployment

This guide describes the current scale-csi repository and bundled Helm chart,
based on the v1.4.0 release line. Review the [deployment guide](deployment.md)
for installation examples and the chart's
[values reference](../charts/scale-csi/README.md) for every setting.

## Prerequisites

### TrueNAS and API access

The NFS and iSCSI clients target the snapshot API generations used by TrueNAS
SCALE 25.04+ (`pool.snapshot.*` for mutations) and 26.0 (which adds
`zfs.resource.snapshot.*` for the user-property reads, rename, and destroy
operations that moved). TrueNAS 25.04 is the documented floor; the 24.x
`zfs.snapshot.*` generation is no longer supported. The client detects and
caches the 26.0 resource API separately. NVMe-oF is different: the driver
rejects it before TrueNAS 25.10.

The repository's automated conformance is the `TestCSISanity` suite, which runs
the official kubernetes-csi `csi-sanity` specs against the driver over a real
gRPC socket with a `MockClient` backend and PATH-faked node commands: the NFS
full surface (controller plus node specs) and the iSCSI controller surface (its
Node Service specs are skipped because they need a real block device and root
privileges). NVMe-oF has unit and controller-level tests but no protocol-specific
sanity suite. Tests named `e2e` in this repository also use `MockClient`; they
exercise driver logic, not a live appliance. Fake-command conformance is not a
substitute for validating your exact TrueNAS patch release, protocol, and the
node data path end to end on a real initiator host in a staging cluster before
production. The 26.0 middleware behaviors documented under Known limitations were
surfaced against a real TrueNAS 26.0 appliance.

Separately, and distinct from the in-repo automated suite above, the maintainer
ran the full `csi-sanity` controller suites live against a real TrueNAS 26.0
system (NFS 52/52, iSCSI 52/52, 2026-07-17) and the full node-plane suites
including the Node Service specs on real Linux initiator hosts for NFS, iSCSI,
and NVMe-oF (75/75 each, 2026-07-18) during the v1.2.x hardening program. These
are maintainer-attested out-of-band results against specific hardware and TrueNAS
builds; they are not reproducible from this repository's automated tests and do
not replace validating your own appliance and node data path.

Use a user-linked API key over HTTPS. API keys inherit the roles of their user.
On role-based TrueNAS releases, the built-in `SHARING_ADMIN` plus
`REPLICATION_ADMIN` roles cover the dataset/share and snapshot operations used
by the driver; `FULL_ADMIN` is not required. A custom privilege must cover the
equivalent dataset create/update/delete, NFS/iSCSI/NVMe sharing, snapshot
read/create/update/clone/rename/delete, service read/reload, and `system.info`
operations. Role names and method assignments differ between TrueNAS API
generations, so confirm a custom privilege against the API documentation served
by the target appliance. See the TrueNAS [role reference][truenas-rbac].

> **Exclude the CSI parent from periodic-snapshot and replication tasks.** The
> configured `zfs.parentDataset` subtree is exclusive driver territory. A
> TrueNAS periodic-snapshot task (or a replication task's snapshots) that covers
> the CSI parent will create snapshots the driver did not make. Those snapshots
> are *foreign* to the driver: by default `DeleteVolume` refuses to remove a
> dataset that carries them and returns `FailedPrecondition`, so PVC deletion
> stalls until the snapshots are gone or the task excludes the parent. Scope any
> such task to datasets *outside* `zfs.parentDataset`, or accept destructive
> cleanup by setting `zfs.destroyForeignSnapshotsOnDelete: true`.

### Network and nodes

Allow the following paths; do not expose storage ports beyond the node networks:

| Source | Destination | Port | Purpose |
|---|---|---:|---|
| Controller pods only | TrueNAS API | TCP 443 | JSON-RPC 2.0 WebSocket (`wss://<host>:443/api/current`). Node pods build no management client and do not need this path |
| Kubernetes nodes | TrueNAS NFS | TCP 2049 | NFS volume mounts |
| Kubernetes nodes | TrueNAS iSCSI portals | TCP 3260 | iSCSI discovery, login, and I/O |
| Kubernetes nodes | TrueNAS NVMe/TCP target | TCP 4420 | NVMe discovery, connect, and I/O |

The node image invokes host storage tools. Install the NFS client and kernel NFS
support for NFS; `iscsiadm`, `iscsid`, and the `iscsi_tcp` initiator module for
iSCSI; or `nvme-cli` and the `nvme_tcp`/`nvme_fabrics` modules for NVMe/TCP.
The chart mounts the host device, sysfs, udev, kubelet, and iSCSI paths; it does
not install host packages or load modules.

## Availability and outage behavior

The default is `controller.replicas: 1`. Leader election is enabled on every
capable controller sidecar (provisioner, attacher, resizer, snapshotter)
unconditionally — even at a single replica — so a `fencing.mode=off`
RollingUpdate that transiently runs two controller pods never has both acting as
the active provisioner/attacher. Replica counts greater than one additionally
add preferred hostname anti-affinity and, by default, a PDB with
`maxUnavailable: 1`. This is controller-availability groundwork, not a claim
that the driver has a distributed operation lock.
Additive and strict fencing require exactly one controller because their
background reconcilers are singleton writers; schema and template guards reject
any other replica count. The controller is restart-recovered: downtime pauses
provisioning, attachment, resize, and snapshot operations, but does not
interrupt workloads already using their volumes. The defaults use
`system-cluster-critical` for the controller pod and `system-node-critical` for
the node DaemonSet.

### Concurrency contract

Cross-process serialization of `CreateVolume` (and the other controller RPCs)
is a layered contract, not a single mechanism:

- The default single-replica controller deployment is the primary guarantee
  that only one controller process mutates the backend at a time. With
  `replicas>1`, each CSI sidecar elects its own leader; those independent
  elections improve failover but can select different pods and do not serialize
  every controller RPC through one process. The v1.3.0 template renders an
  explicit strategy in every mode: `off` uses `RollingUpdate` with
  `maxUnavailable: 25%` / `maxSurge: 25%`, so a rollout can briefly run an old
  and a new controller pod side by side; `additive`/`strict` use `Recreate`
  with `rollingUpdate: null` (their reconcilers are in-process singleton writers).
- The driver's operation locks are per process. They serialize work inside one
  controller but provide no exclusion between two controller processes.
- The durable in-flight creation markers, the tombstone ledger, and the
  recovery-nonce discipline narrow the windows a second concurrent writer
  could exploit, but none of them is an atomic compare-and-swap. The nonce is
  a write-then-verify sequence — an unconditional property write followed by a
  verifying re-read — so two writers whose write/verify windows do not
  interleave can each observe their own value and both report success. A
  detected lost race returns retryable `Aborted` instead of double-owning a
  dataset; an undetected one is tolerable only because both writers are the
  same driver instance writing identical identity values. The strongest
  concurrency contract therefore remains the singleton topology. Off-mode
  multi-replica rendering is deliberately HA groundwork and must be validated
  against the operator's workload; it does not turn these markers into a
  distributed lock. Two independent releases against the same parent, any
  multi-replica fenced deployment, and overlapping old/new fenced controllers
  remain out of contract.
- Upgrade note: tombstone-ledger entries written by pre-release builds lack
  the recorded creation identity (`created_at`) and are permanently skipped by
  the reaper (fail-closed). No released version ever wrote ledger entries, so
  this affects no real deployment.
- The configured `zfs.parentDataset` subtree is exclusive driver territory.
  The driver stores its bookkeeping as user properties on the parent dataset
  and treats child datasets as objects it may stamp, adopt, or (with durable
  provenance) destroy. Manually creating datasets or snapshots inside the
  parent — especially at names a PVC or VolumeSnapshot might use — is out of
  contract; place operator-managed data outside the parent dataset.
- Bookkeeping-relocation downgrade caveat: the optional
  `reconcile.bookkeeping.enabled` flag relocates the driver's durable
  bookkeeping (tombstone ledger + in-flight markers) to a dedicated
  `<parent>/.csi-bookkeeping` child dataset, reading from both locations while
  it is on. Once it has been true and entries live on the child, **do not flip
  it back off**: reads consult the child only while the flag is enabled, so
  disabling it orphans any child-side entries from crash recovery and garbage
  collection — they become invisible and can no longer be acted on. The
  supported way to drain bookkeeping off the parent is the
  `reconcile.bookkeeping.cleanupParent` flow (copy entries to the child, then
  remove the confirmed copies from the parent), not disabling the relocation.

The node component runs as a DaemonSet on all tolerated nodes and performs
stage, publish, unpublish, and unstage through host NFS/iSCSI/NVMe tools. A
node-only pod builds **no TrueNAS management client at all** (credential-free
since v1.2.22), so it has no deferred/lazy API connection: it initializes and
reports ready regardless of TrueNAS reachability, and every node RPC it serves
uses local host tools. During a management API outage only controller
operations fail or retry; node stage, publish, unpublish, unstage, and local
filesystem expansion remain available.

The API retry and circuit-breaker behavior comes from this values block:

```yaml
truenas:
  maxConcurrentRequests: 10   # the effective API concurrency semaphore
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
    maxConcurrentLogins: 2
```

> **The API concurrency limit is `truenas.maxConcurrentRequests`.** The former
> `resilience.rateLimiting.maxConcurrentRequests` key was never wired to anything
> and is now **deprecated and ignored**: the chart no longer renders it and the
> driver logs a warning if a configmap still sets it (the values schema keeps
> accepting it so old values files do not fail validation). Only
> `truenas.maxConcurrentRequests` reaches the client's API semaphore. Under
> `resilience.rateLimiting`, only `maxConcurrentLogins` (iSCSI login concurrency)
> is effective. Tune `truenas.maxConcurrentRequests` to protect an overloaded NAS.

Retries apply only to connection-class failures; an ambiguous non-idempotent
mutation is not retried. The circuit breaker is opt-in and disabled by default;
connection-only retry and the API concurrency semaphore provide the baseline
protection. If enabled, five consecutive failures open it for 30
seconds before half-open probes are admitted. These controls do not replace
protocol-level mount/login timeouts under `commandTimeouts`.

`requestTimeout` bounds each API call. It is applied as a hard per-call cap **only
to callers that supply no deadline of their own** (internal background work such as
session garbage collection), so a wedged-but-live TrueNAS request cannot pin an API
concurrency slot indefinitely. Calls that already carry a deadline — every CSI RPC,
which inherits the sidecar's `--timeout` — are bounded by that deadline instead, so a
legitimately long single operation (e.g. a large clone or recursive snapshot) is never
cut short at `requestTimeout`. In the worst case all `maxConcurrentRequests` slots can
be held by deadline-bearing calls for the length of their sidecar timeout; size the
semaphore and sidecar timeouts accordingly.

## Resource sizing

Steady-state measurements are approximately 15Mi memory and 1m CPU per driver
container. The chart requests 10m CPU and 32Mi memory for each controller and
node driver container and sets a 256Mi driver memory limit. Every CSI sidecar
requests 10m CPU and 32Mi memory with a 128Mi memory limit. Override the
corresponding resource map for measured workload needs.

When limits are set, `automaxprocs` derives `GOMAXPROCS` from the CPU cgroup and
the driver sets `GOMEMLIMIT` to 90% of the finite memory cgroup limit unless the
environment explicitly supplies `GOMEMLIMIT`. CSI liveness reports initialized
process health, independent of TrueNAS reachability, so a NAS blip or slow
reconnect does not cause a crash loop. Controller `/readyz` remains
backend-aware, while node-only `/readyz` is intentionally independent of
TrueNAS connectivity. Alert on `scale_csi_truenas_connection_status == 0` for
backend loss.

## Security

- Prefer an externally managed Secret and set `truenas.existingSecret`; it must
  be in the release namespace and contain `api-key`. Do not also set
  `truenas.apiKey`. Rotate the TrueNAS key and Secret together.
- Set `nfs.shareAllowedNetworks` to the node CIDRs. Its empty default permits all
  networks accepted by TrueNAS for each dynamically created share.
- The iSCSI initiator allowlist depends on `fencing.mode`, and CHAP composes with
  it rather than replacing it: in `off` the driver leaves configured/static
  backend allowlists alone (and the resolved default may be allow-all); `additive`
  retains legacy/static entries and **adds** the live CSI initiators; `strict`
  **replaces** them with the exact live publication set. In `additive`/`strict`
  the immutable per-volume CHAP authmethod/tag stays attached to the target group
  while fencing changes only the initiator allowlist — CHAP is an independent
  session-authentication layer that neither disables fencing nor implies allow-all
  access. CHAP session authentication is available but strictly opt-in
  (`iscsi.chap.enabled: true` plus a per-StorageClass CHAP Secret — see
  [the StorageClass reference](reference/storageclass.md#iscsi-chap)); with it
  off (the default) targets stay `authmethod=NONE`. CHAP authenticates the
  session — it does **not** encrypt data in flight — so network segmentation
  (such as a VLAN or SGACL) remains the confidentiality boundary for TCP 3260
  even when CHAP is on. The driver does not currently provide per-tenant iSCSI
  isolation. CHAP credentials are supplied per StorageClass via a Kubernetes
  Secret, are never written to the PV volume context, and are redacted from all
  driver logs, gRPC errors, and Kubernetes Events: password-setting `iscsiadm`
  failures surface only the parameter name and an exit class, never the command
  output.
- **Accepted host-trust exposure (CHAP).** CHAP session credentials are applied
  on the node by passing them to `iscsiadm` as `-v <value>` arguments, so the
  credential is briefly visible in the host process table (`/proc/<pid>/cmdline`)
  while the call runs, and open-iscsi persists the session credential in the host
  node database under `/var/lib/iscsi` (and `/etc/iscsi`) for as long as the node
  record exists. The node DaemonSet is privileged with `hostPID` and mounts these
  host paths, so any root-level actor on the node can read the credential. This is
  an explicit, accepted root-on-host trust assumption — CHAP protects against
  off-host initiators, not against a compromised node. Treat node root as
  equivalent to holding every CHAP secret staged on that node, and rely on
  network segmentation plus node hardening accordingly.
- `DeleteVolume` preserves non-CSI snapshots by default, including snapshots
  inherited from periodic-snapshot or replication tasks on the parent dataset.
  It returns `FailedPrecondition` until those snapshots are removed or the task
  excludes the CSI parent. Setting `zfs.destroyForeignSnapshotsOnDelete: true`
  explicitly permits recursive deletion of the dataset and those snapshots.
- The default `nvmeof.subsystemAllowAnyHost: false` denies unlisted initiator
  NQNs. Populate `nvmeof.subsystemHosts` with each node's NVMe host NQN —
  obtained by running `nvme show-hostnqn` on the node (nvme-cli derives a
  stable NQN from the machine identity even when `/etc/nvme/hostnqn` does not
  exist, as on Flatcar) — for every
  Kubernetes node that may use the StorageClass. The controller resolves or
  creates the corresponding TrueNAS host records and associates their IDs with
  each new subsystem. It does not auto-discover node NQNs; restricted mode with
  an empty host list fails provisioning rather than creating an unreachable
  subsystem. Host-NQN controls complement, but do not replace, network
  segmentation and filtering for the NVMe-oF listener.
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

Controller-side `VolumeCondition` reports **backend provisioning-metadata
health**, not mere existence. The driver derives the same declarative condition
for `ControllerGetVolume` and `ListVolumes`: an explicit local
`provision_success=false` is abnormal; a managed, successfully provisioned volume
is normal; a managed volume missing the legacy stamps is reported normal but
"unverified." It does **not** probe protocol or data-path health.
`NodeGetVolumeStats` separately detects stale-mount state before its stats gate,
so node-side evidence is distinct from the controller condition.

The optional external health-monitor sidecar
(`sidecars.healthMonitor.enabled`, default off; v0.18.0 controller sidecar) emits
PVC Events from controller-side volume conditions. Because this driver advertises
`LIST_VOLUMES`, that sidecar uses one periodic `ListVolumes` path rather than
per-PV `ControllerGetVolume`, so it observes the **controller/`ListVolumes`**
condition above — not node stale mounts or the data path. Node-side
`VolumeCondition` delivery depends on Kubernetes/kubelet's separate **alpha**
volume-health path and feature gating; enabling this controller sidecar alone
does not provide it. The sidecar's `interval` drives both its list-volumes and
fallback monitor cadence.

Watch these series:

- `scale_csi_operations_total` and `scale_csi_operations_duration_seconds` for
  CSI error rate and latency;
- `scale_csi_truenas_requests_total` and
  `scale_csi_truenas_requests_duration_seconds` for backend API health;
- `scale_csi_truenas_connection_status` for connectivity;
- `scale_csi_circuit_breaker_state`,
  `scale_csi_circuit_breaker_current_failures`, and the breaker counters for
  outage protection;
- `scale_csi_truenas_connections_active` for authenticated WebSocket pool
  connections;
- `scale_csi_iscsi_sessions_total` and `scale_csi_nvme_sessions_total` for the
  sessions observed by node session garbage collection;
- `scale_csi_node_connect_total` and
  `scale_csi_gc_sessions_disconnected_total` for per-transport node connection
  attempts and orphan cleanup.

Five metric families added in v1.4.0 (the existing documented names still match
`driver.MetricNames()`):

- `scale_csi_job_dispatcher_subscribed` — `1` while the `core.get_jobs`
  subscription is live, `0` in the pure-poll fallback; a persistent `0` (alerted
  by `ScaleCSIJobDispatcherUnsubscribed`) means investigate the WebSocket
  subscription.
- `scale_csi_manual_recovery_tombstones` — tombstones no provenance belt can
  prove, for operator inspection; **populated only while scan fallback is
  enabled**.
- `scale_csi_tombstone_reaped_total{path}` — reaper throughput by discovery path
  (ledger vs scan fallback).
- `scale_csi_pool_available_bytes` and `scale_csi_pool_capacity_bytes` — parent
  pool free/total; **present only when `capacity.gaugeEnabled`**, and they drive
  `ScaleCSIPoolNearFull`.

The bundled rules use distinct expressions, rate windows, and `for` durations —
do not collapse them into one sentence:

- `ScaleCSIControllerDown`: controller scrape target absent, held **5m**.
- `ScaleCSITrueNASConnectionDown`: TrueNAS disconnected, held **5m**.
- `ScaleCSICircuitBreakerOpen`: circuit breaker open, held **2m**.
- `ScaleCSIHighTrueNASAPIFailureRate`: TrueNAS API failure **ratio > 10%** over a
  **5m** rate window, held **10m**. It selects `status="error"`, whose meaning
  narrowed in Sprint 5 (benign outcomes no longer counted — see below).
- `ScaleCSIOperationErrorsSustained`: CSI operation error rate **> 0.02/s** over a
  **10m** rate window, excluding selected benign gRPC codes, held **15m** (not
  `> 0.01/s for ten minutes`).

Tune these thresholds to workload volume; ratios can be noisy at low traffic.

> **Benign `already exists` on the NVMe-oF path.** The driver treats an
> `AlreadyExists` response to `nvmet.host_subsys.create` as success (the
> host/subsystem association it wanted already exists). Since Sprint 5 this
> outcome is intentionally classified `status="benign_exists"`, **not**
> `status="error"`. A small, non-growing count of
> `scale_csi_truenas_requests_total{method="nvmet.host_subsys.create",status="benign_exists"}`
> is therefore expected by design during NVMe-oF provisioning and must not be
> read as an error sample. Real transport failures remain `status="error"`.

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
   `existingSecret` do **not** alter a checksum annotation. Because only the
   **controller** consumes the TrueNAS API key (the node builds no API client),
   after rotating the external `truenas.existingSecret` restart the **controller**
   workload and verify `/readyz`/backend auth; restarting node pods is not
   required for that credential. **CHAP Secrets are different** — they are
   request-scoped CSI Secrets, not pod configuration, so they need no driver
   rollout: a rotated CHAP Secret takes effect on the backend only when a later
   `CreateVolume` revalidates/re-keys the peer, and on the node only before a
   fresh login. Do not restart driver pods as a substitute for session turnover.

4. The chart declares versioned provisioner, attacher, resizer, snapshotter,
   registrar, and liveness image defaults, all overridable in values and tracked
   by Renovate. CSI sidecars have independent Kubernetes and
   CSI compatibility matrices; do not upgrade one image in isolation without
   checking its release notes. Snapshot support also requires cluster-installed
   snapshot CRDs and the common snapshot controller, which this chart does not
   install. Keep their API generation compatible with the snapshotter.

5. StorageClass parameters are immutable. A multi-protocol deployment now
   requires `parameters.protocol`; create a replacement StorageClass, update
   workload manifests, and then retire/recreate the old class deliberately.
   Existing bound PVs are not reprovisioned by this metadata migration. See the
   [StorageClass upgrade procedure](reference/storageclass.md#upgrade-add-protocol-safely).

6. The node DaemonSet intentionally receives no `TRUENAS_API_KEY` and
   **constructs no TrueNAS management client at all**. Every supported Node RPC
   (stage/publish/unpublish/unstage and local expansion) uses host tools and local
   state, so there is **no deferred/lazy management-API path** that could later
   fail for lack of credentials — node pods stay independent of management-API
   availability. Treat the absent key as an explicit security boundary when
   upgrading from manifests that injected it into every pod.

7. For the fencing migration, keep `fencing.mode=off`, upgrade the node
   DaemonSet/image first, and wait for every CSINode to re-register its versioned
   transport identity before enabling `additive`. Enable `strict` only after
   `scale_csi_fencing_deferred_total` remains at zero. Roll the
   controller image and its ConfigMap together; applying new fencing keys to an
   older strict-YAML binary can make that older pod fail configuration parsing.
   The chart uses a shared image value, so patch and await the node DaemonSet
   first, verify every driver CSINode node ID has the `sc1.` prefix, and only
   then run the Helm upgrade that changes the controller image, ConfigMap, and
   mode. The exact commands are in the chart's publication-fencing runbook.
   Outside this explicitly tested migration sequence, the repository does not
   promise arbitrary controller/node version skew. Exercise provision, attach,
   mount, expand, snapshot, restore, unmount, and delete in a staging namespace
   before production rollout.

## Current known limitations

- On the unfenced (`fencing.mode=off`) default path, driver-created iSCSI targets
  have no per-tenant initiator isolation — the resolved default allow-all
  initiator group makes storage-network segmentation the access boundary for TCP
  3260. `additive`/`strict` fencing narrow the initiator allowlist per volume (and
  compose with CHAP, which stays attached independently). CHAP session
  authentication is available (opt-in) but does not encrypt data in flight and has
  limitations: rotated secrets take effect only before a fresh login (established
  sessions are not re-authenticated), and deleting a CHAP StorageClass leaves its
  shared `iscsi.auth` peer behind for the operator to reap.
- Host dm-multipath ownership of TrueNAS iSCSI LUNs is unsupported. The node
  service refuses to stage an iSCSI device with a `dm-*` sysfs holder instead
  of formatting or mounting a raw component path.
- Foreign snapshots block `DeleteVolume` by default. Removing them or excluding
  the CSI parent from external snapshot tasks is required unless destructive
  cleanup is explicitly enabled with `zfs.destroyForeignSnapshotsOnDelete`.
- Automated conformance does not cover the iSCSI or NVMe-oF node paths. The
  `csi-sanity` suite runs the NFS full surface and the iSCSI *controller* surface
  against a `MockClient` backend with PATH-faked node commands; the iSCSI Node
  Service specs are skipped and NVMe-oF has no protocol-specific sanity suite.
  None of this substitutes for node tests with real block devices and a real
  target. Validate the node data path on a real initiator host in staging.
- With `fencing.mode=off`, NVMe-oF host-NQN allowlisting is configured
  statically through `nvmeof.subsystemHosts`. Additive and strict modes consume
  the host NQN registered by each node plugin and enforce per-volume host
  associations. Continue to use network segmentation (for example VLANs or
  SGACLs) to protect the NVMe-oF listener; host allowlisting is an additional
  control.
- A TrueNAS NVMe-oF listener only materializes on a configured port once at
  least one subsystem is associated with it — a bare port shows no kernel
  listener, which is normal and self-resolves on first volume creation.
- `ControllerModifyVolume` returns `Unimplemented`. CSI volume group snapshot
  services are not registered or implemented.
- CSI volume and snapshot names share `sanitizeVolumeID`: `/` and spaces become
  `-`, a first byte outside lowercase ASCII alphanumerics is prefixed with `v`,
  and the result is truncated to 128 bytes on a UTF-8 rune boundary. It is not a
  general arbitrary-ZFS-name normalizer. Snapshot short names are global within the
  configured parent from the CSI driver's perspective.
- Deleting a snapshot that still has clones renames it to an internal tombstone
  and requests deferred ZFS destruction. The snapshot disappears from CSI, but
  its referenced space remains charged until the last clone releases it. The
  reaper acts on tombstones through a durable ledger. Tombstones whose provenance
  no belt can prove (no ledger entry, no adoptable ownership stamp) are never
  destroyed automatically; they are surfaced as `manualRecoveryTombstones` in the
  reconcile summary for operator inspection — and `manualRecoveryTombstones` is
  populated **only while scan fallback is enabled**. The
  `reconcile.tombstoneReaper.scanFallback.enabled` flag (default **off**) adds a
  provenance-gated fallback that runs on **every** pass, independent of whether
  the ledger backlog is empty. It does not issue a separate query: it reuses the
  pass's already-fetched recursive, unpaginated snapshot set and processes at most
  500 accepted candidates. A candidate is authorized only when it has **no** ledger
  property at either bookkeeping location **and** carries retained creation-time
  identity that exactly reproduces the driver's nonce-derived tombstone rename —
  exact retained snapshot/instance identity, exact tombstone name, local
  source-instance ownership, the age gate, and the inheritance-mask guard. It never
  widens what counts as this driver's own object.
- The durable bookkeeping (tombstone ledger + in-flight markers) can be
  relocated off the inheritable parent onto a `<parent>/.csi-bookkeeping` child
  via `reconcile.bookkeeping.enabled`, so its user properties no longer inherit
  into every descendant snapshot. This is a one-way migration: once entries live
  on the child, do not disable the flag (see the concurrency contract's
  downgrade caveat). Inbound volume/snapshot IDs equal to the `.csi-bookkeeping`
  leaf are rejected with `InvalidArgument` before any TrueNAS access.
- Snapshot restores default to ZFS clones (`snapshotRestoreMode: clone`): a
  clone-restored volume pins its source snapshot until the volume is deleted, with
  deferred destroy handling the snapshot lifecycle. A StorageClass with
  `snapshotRestoreMode: detached` (or driver default
  `zfs.detachedVolumesFromSnapshots: true`) restores via local send/receive with
  no source-snapshot pin. Volume-to-volume clones (PVC dataSource) are always
  clone-backed regardless of that setting.
- After upgrading a NAS from TrueNAS 25.x to 26.0, CSI snapshots created by
  older driver versions without `truenas-csi:csi_snapshot_name` are omitted
  from `ListSnapshots`. Restore and deletion by snapshot ID continue to work.
- TrueNAS 26.0 (including beta builds) silently ignores
  `pool.snapshot.update` requests that use `user_properties_update` or
  `user_properties_remove`. The driver writes snapshot identity properties at
  creation for correctness; tombstone names, rather than property removal, hide
  deferred deletions. This middleware behavior should be reported upstream.

[truenas-rbac]: https://api.truenas.com/v26.0/rbac.html
[csidriver-api]: https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/csi-driver-v1/
