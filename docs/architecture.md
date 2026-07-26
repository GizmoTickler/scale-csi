# TrueNAS Scale CSI Architecture

This document explains the internal architecture and workflows of the TrueNAS Scale CSI driver.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        PVC[PersistentVolumeClaim]
        SC[StorageClass]
        Controller["CSI Controller<br/>(Deployment)"]
        Node["CSI Node<br/>(DaemonSet)"]
        Pod[Application Pod]
    end

    subgraph "TrueNAS SCALE"
        API["WebSocket API<br/>(JSON-RPC 2.0)"]
        Dataset[(ZFS Dataset/Zvol)]
        NFS[NFS Share]
        ISCSI[iSCSI Target]
        NVME[NVMe-oF Subsystem]
    end

    PVC --> SC
    SC --> Controller
    Controller -->|Create/Delete/Expand| API
    API --> Dataset
    API --> NFS
    API --> ISCSI
    API --> NVME
    Node -->|Mount| NFS
    Node -->|Login| ISCSI
    Node -->|Connect| NVME
    Pod --> Node
```

## Component Overview

The driver follows the standard Kubernetes CSI (Container Storage Interface) architecture, consisting of two main components:

### 1. CSI Controller (Deployment)
- **Type**: Deployment (usually 1 replica)
- **Responsibility**: Communicates with the TrueNAS API to manage storage resources.
- **Operations**:
  - `CreateVolume`: Creates ZFS datasets (NFS) or Zvols (iSCSI/NVMe-oF).
  - `DeleteVolume`: Deletes datasets/zvols.
  - `ControllerExpandVolume`: Resizes datasets/zvols (updates quotas/volsize).
  - `CreateSnapshot`: Creates ZFS snapshots.
  - `DeleteSnapshot`: Deletes ZFS snapshots.
  - `CreateVolume` (from Snapshot): Clones ZFS snapshots to new datasets/zvols.

### 2. CSI Node (DaemonSet)
- **Type**: DaemonSet (runs on every node)
- **Responsibility**: Mounts storage volumes to the Kubernetes node and Pods.
- **Operations**:
  - `NodeStageVolume`: Connects to storage (NFS mount, iSCSI login, NVMe connect) and formats device (if needed).
  - `NodePublishVolume`: Bind-mounts the staged volume into the Pod's container.
  - `NodeGetVolumeStats`: Reports storage usage (df/inodes) to Kubernetes.
  - `NodeExpandVolume`: Resizes the filesystem on the node (e.g., `resize2fs`).

## Communication Flow

All control plane operations use the **TrueNAS WebSocket JSON-RPC 2.0 API**.

```mermaid
sequenceDiagram
    participant K8s as Kubernetes
    participant Ctrl as CSI Controller
    participant API as TrueNAS API
    participant ZFS as ZFS Pool

    K8s->>Ctrl: CreateVolume RPC
    Ctrl->>API: WebSocket Connect (API Key)
    API-->>Ctrl: Connected
    Ctrl->>API: pool.dataset.create
    API->>ZFS: zfs create
    ZFS-->>API: Success
    API-->>Ctrl: Dataset Created
    Ctrl->>API: sharing.nfs.create
    API-->>Ctrl: Share Created
    Ctrl-->>K8s: Volume Ready
```

1. **Authentication**: The driver connects to `wss://<host>/api/current` using an API Key.
2. **Persistence**: The WebSocket connections are persistent and auto-reconnect.
3. **No SSH**: Unlike legacy drivers, this driver **does not** use SSH. All operations, including filesystem formatting (handled by the node), are done via API or local node tools.

### WebSocket connection pool and resilience pipeline

The controller does not use a single socket. It maintains a **pool of 5
WebSocket connections** (default `truenas.maxConnections`) and multiplexes
requests across them round-robin, so concurrent RPCs are not serialized behind
one connection. On top of that, a **10-slot semaphore** (default
`truenas.maxConcurrentRequests`) caps how many API calls are in flight at once,
protecting TrueNAS from overload.

Every backend call funnels through one resilience pipeline (`callRaw`):

- **Circuit breaker** (opt-in, `resilience.circuitBreaker.enabled`, default off):
  after N consecutive failures it opens for a timeout, then admits half-open
  probes.
- **Connection-class retry** with exponential backoff: only connection/transport
  failures are retried. An ambiguous non-idempotent mutation is **not** retried.
- **Per-call deadline**: `requestTimeout` bounds only calls that carry no
  deadline of their own (background work); CSI RPCs are bounded by the sidecar
  `--timeout` they inherit.

**Ambiguity taxonomy.** Two sentinel errors classify transport outcomes so the
retry logic never double-applies a non-idempotent write:

- `ErrTransportFailure` — the request failed before it could have been applied
  (e.g. "connection lost before request was sent" / "during authentication"). It
  is safe to retry and is recorded as a breaker failure.
- `ErrAmbiguousResult` — the request was written but no response was observed, so
  whether it took effect is unknown. It is not blindly retried; callers reconcile
  by re-reading state.

Because the WebSocket read limit on TrueNAS rejects inbound frames over ~64 kB
(close 1009), the driver chunks large property writes (for example the
bookkeeping migration and batched ledger removals) to stay well under that
limit.

## Storage Workflows

### NFS (Filesystem)

```mermaid
flowchart LR
    subgraph Controller
        A[CreateVolume] --> B[Create Dataset]
        B --> C[Create NFS Share]
    end
    subgraph Node
        D[NodeStageVolume] --> E[NFS Mount]
        E --> F[NodePublishVolume]
        F --> G[Bind Mount to Pod]
    end
    C --> D
```

1. **Provisioning**: Controller creates a ZFS dataset with `type=FILESYSTEM`.
2. **Exporting**: Controller creates an NFS share for the dataset path.
3. **Mounting**: Node uses `mount -t nfs` to mount the dataset path.
4. **Access**: Supports `ReadWriteMany` (RWX) and `ReadWriteOnce` (RWO).

### iSCSI (Block)

```mermaid
flowchart LR
    subgraph Controller
        A[CreateVolume] --> B[Create Zvol]
        B --> C[Create Target]
        C --> D[Create Extent]
        D --> E[Create Association]
    end
    subgraph Node
        F[NodeStageVolume] --> G[iSCSI Discovery]
        G --> H[iSCSI Login]
        H --> I[Format Device]
        I --> J[Mount to Staging]
        J --> K[Bind Mount to Pod]
    end
    E --> F
```

1. **Provisioning**: Controller creates a ZFS volume (`zvol`) with `type=VOLUME`.
2. **Exporting**: Controller creates an iSCSI Target, Extent, and TargetExtent mapping.
3. **Attachment**: Node uses `iscsiadm` to discover and login to the target.
4. **Formatting**: Node formats the device (ext4/xfs) if it's a new volume.
5. **Mounting**: Node mounts the formatted device.
6. **Access**: Primarily `ReadWriteOnce` (RWO).

### NVMe-oF (Block)

```mermaid
flowchart LR
    subgraph Controller
        A[CreateVolume] --> B[Create Zvol]
        B --> C[Create Subsystem]
        C --> D[Create Port]
        D --> E[Create Namespace]
    end
    subgraph Node
        F[NodeStageVolume] --> G[NVMe Connect]
        G --> H[Format Device]
        H --> I[Mount to Staging]
        I --> J[Bind Mount to Pod]
    end
    E --> F
```

1. **Provisioning**: Controller creates a ZFS volume (`zvol`).
2. **Exporting**: Controller creates an NVMe Subsystem, Port association, and Namespace.
3. **Attachment**: Node uses `nvme-cli` to connect to the subsystem (TCP/RDMA).
4. **Formatting**: Node formats the device.
5. **Mounting**: Node mounts the formatted device.
6. **Access**: `ReadWriteOnce` (RWO).

## Snapshots & Clones

```mermaid
flowchart TB
    subgraph "Snapshot Flow"
        A[CreateSnapshot] --> B[ZFS Snapshot]
        B --> C[Store Metadata]
    end
    subgraph "Clone Flow"
        D[CreateVolume from Snapshot] --> E[ZFS Clone]
        E --> F[Create Share]
    end
    C --> D
```

The driver leverages native ZFS capabilities:
- **Snapshots**: Instantaneous ZFS snapshots (`zfs snapshot`).
- **Clones**: ZFS clones (`zfs clone`) for creating new volumes from snapshots. This allows for instant provisioning of test environments from production data.

## Volume ID Format

The volume ID is the sanitized CSI request name and is also the child dataset
or zvol basename beneath `zfs.datasetParentName`. It does not encode the
protocol or full ZFS path. For example, a request named `pvc-abc123` produces
volume ID `pvc-abc123` and backend object
`tank/k8s/volumes/pvc-abc123`. Protocol identity is stored in volume context
and ZFS user properties.

## ZFS User Properties

The driver stores **all** durable state as ZFS user properties prefixed with
`truenas-csi:` — there is no external database, which is what makes the driver
restart-recoverable and ZFS-replication-friendly (see the
[disaster-recovery guide](guides/disaster-recovery.md)).

**Ownership and identity**

| Property | Description |
|----------|-------------|
| `truenas-csi:managed_resource` | Marks CSI-managed datasets (read with property source; a *local* value is required — an inherited one does not prove ownership) |
| `truenas-csi:driver_instance_id` | Stamps which driver instance owns the dataset; never overwritten once set (local, inherited, or foreign) |
| `truenas-csi:csi_volume_name` | Original PVC/request name |
| `truenas-csi:provision_success` | Marks provisioning as completed |
| `truenas-csi:requested_size_bytes` | Requested capacity |

**Content source (clones/restores)**

| Property | Description |
|----------|-------------|
| `truenas-csi:csi_volume_content_source_type` / `_id` | Records the snapshot or volume a restore/clone was created from |
| `truenas-csi:csi_volume_origin_snapshot` | The origin snapshot pinned by a clone-mode restore |

**Crash-consistency bookkeeping**

| Property | Description |
|----------|-------------|
| `truenas-csi:inflight_*` | In-flight creation markers — written before a mutation, cleared on success; the only handle a crash-recovery sweep can act on |
| `truenas-csi:recovery_nonce` | Write-then-verify identity token for lost-race detection |
| `truenas-csi:tombstone_*` | Deferred-delete tombstone ledger (v2 keys the entry by `CreateTXG`; v1 entries remain compatible) |
| `truenas-csi:publication_*` | Durable per-volume publication records (see fencing, below) |
| `truenas-csi:internal_resource` | Marks the `.csi-bookkeeping` child dataset when bookkeeping relocation is enabled |

**Backend share-object backreferences**

| Property | Description |
|----------|-------------|
| `truenas-csi:truenas_nfs_share_id` | Associated NFS share ID |
| `truenas-csi:truenas_iscsi_target_id` / `_extent_id` / `_targetextent_id` / `_initiator_id` | iSCSI object IDs |
| `truenas-csi:truenas_nvmeof_subsystem_id` / `_namespace_id` / `_portsubsys_id` | NVMe-oF object IDs |

Snapshots carry their own identity properties (`truenas-csi:csi_snapshot_name`,
`truenas-csi:csi_snapshot_source_volume_id`, `truenas-csi:csi_share_volume_context`).

## VolSync Integration

The driver fully supports the `Snapshot` copy method in VolSync:
1. **Backup**: VolSync requests a CSI Snapshot -> Driver creates ZFS Snapshot.
2. **Restore**: VolSync requests a PVC from Snapshot -> Driver creates ZFS Clone from Snapshot.

See [Snapshots and Clones Guide](guides/snapshots.md) for detailed usage instructions.

## Controller topology, node mode, and leader election

- **Controller** runs as a Deployment, default **1 replica**. This singleton
  topology is the primary cross-process serialization guarantee — the driver's
  operation locks are per-process and provide no exclusion between two controller
  processes.
- **Leader election** is enabled on every capable controller sidecar
  (provisioner, attacher, resizer, snapshotter) **unconditionally**, even at a
  single replica, so a `fencing.mode=off` RollingUpdate that briefly runs two
  controller pods never has both acting as the active provisioner/attacher.
- **Node** runs as a DaemonSet and is **credential-free**: it receives no
  `TRUENAS_API_KEY`. Stage/publish/unpublish/unstage and local filesystem
  expansion use host tools (`mount`, `iscsiadm`, `nvme`, `resize2fs`), so node
  pods start in lazy-connect mode and stay available during a management-API
  outage. A node operation that genuinely needs the management API fails without
  credentials by design.
- `additive`/`strict` fencing require **exactly one** controller replica because
  their background reconcilers are singleton writers; chart schema and template
  guards enforce that.

## Publication records and backend fencing

CSI publish state is always tracked in durable per-volume **publication records**
(`truenas-csi:publication_*`). Single-node exclusivity, same-node republish
idempotency, synchronous stale-record takeover, and empty-node-id unpublish are
enforced in **every** mode. `fencing.mode` governs only whether that state is
*also* pushed into the backend transport allowlists:

| Mode | Behavior |
|------|----------|
| `off` (default) | Records-only. No NFS/iSCSI/NVMe allowlist mutation. |
| `additive` | Adds the publishing node's identity to the backend allowlist without removing statically configured entries. The explicit migration mode. |
| `strict` | Per-volume publication records become the sole allowlist. |

Node identity is a stable base64url encoding (name + block-transport identity)
carried on the CSINode registration with an `sc1.` prefix. The node-first
migration (upgrade the DaemonSet, wait for every CSINode to re-register, watch
`scale_csi_fencing_deferred_total`) is described in
[Production](production.md#upgrades).

## Crash-consistency model

The driver has no external database, so it makes each mutation crash-recoverable
through ordered ZFS user-property writes:

- **In-flight markers** (`truenas-csi:inflight_*`) are written *before* a
  create/clone mutation and cleared on success. After a crash, a marked-but-
  unstamped dataset is the only thing a recovery sweep can act on.
- **Ownership stamps** (`managed_resource` + `driver_instance_id`, read with
  property *source* so inherited values never count) prove the object is this
  instance's. A `driver_instance_id` is never overwritten once present.
- **Content-source vs. ownership boundary**: a restored/cloned dataset records
  where it came from (`csi_volume_content_source_*`) separately from who owns it.
  A clone can inherit its source's protocol-foreign backreference properties, so
  the driver scrubs source-proven foreign IDs after stamping.
- **Recovery nonce** (`recovery_nonce`) is a write-then-verify token: a detected
  lost race returns retryable `Aborted` rather than double-owning a dataset. It
  is not an atomic compare-and-swap — the strongest concurrency contract remains
  the singleton controller (see [Production → Concurrency contract](production.md)).
- **Tombstone ledger** records deferred-destroy snapshots. v2 entries key on the
  snapshot's `CreateTXG`; v1 entries stay compatible. The reaper acts only on
  provenance it can prove.

## Reconcile loop and source layout

A controller-side reconcile pass (`ReconcileOrphans`, default hourly; also
runnable once via `--mode=reconcile`) detects and — only under
`reconcile.delete.enabled` — cleans CSI-managed backend objects with no live
Kubernetes reference. Detection is read-only; a shared `reconcile.delete.maxPerRun`
caps destructive actions per pass. The v1.3.0 refactor split the former
monolithic `reconcile.go` along its test seams into per-concern files:

| File | Concern |
|------|---------|
| `reconcile.go` | Pass orchestration + orphan volume/snapshot/tombstone classification |
| `reconcile_kubestate.go` | Live PV / VolumeAttachment / VolumeSnapshotContent hard-rechecks (not informer caches) |
| `reconcile_publications.go` | Stale publication-record repair |
| `reconcile_shares.go` | Orphaned NFS/iSCSI/NVMe-oF share detection and teardown |
| `reconcile_tombstones.go` | Tombstone reaper, scan-fallback, ledger sweep |
| `reconcile_remnants.go` | Stale in-flight marker sweep, remnant-orphan GC, orphaned replication-job sweep |
| `reconcile_spent_restore.go` | VolSync spent-restore snapshot classification |
| `reconcile_adoption.go` | Legacy ownership-stamp adoption |
| `provenance.go` | Ownership stamping, in-flight markers, tombstone ledger, bookkeeping relocation/chunking |
| `fencing.go` / `fence_resolution.go` | Publication records and backend allowlist enforcement |
| `share_backend.go` | `ShareBackend` interface + per-protocol selector |

## API-call cost (golden round-trip counts)

Hot paths are pinned by golden tests (`api_call_count_test.go`) so a regression
that adds a wasted round trip fails loudly. Representative controller costs:

| Operation | TrueNAS round trips |
|-----------|--------------------:|
| CreateVolume (fresh NFS) | 6 |
| CreateVolume (fresh iSCSI) | 14 |
| CreateVolume (clone from snapshot) | 12 |
| CreateVolume / CreateSnapshot (idempotent retry) | 2 |
| DeleteVolume (NFS / iSCSI) | 6 / 10 |
| ControllerPublish/Unpublish (fencing `off`, NFS) | 3 |
| ControllerPublish/Unpublish (`additive`, NFS) | 5 |
| ControllerPublish/Unpublish (`strict`, NVMe-oF, steady-state) | 9 |
