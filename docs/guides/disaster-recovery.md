# Disaster recovery and cross-site replication

scale-csi stores **all** of a volume's metadata as ZFS user-properties on the dataset
itself (`truenas-csi:*`) — there is no external database. This makes the driver
restart-recoverable and, importantly, makes ZFS-native replication a viable DR path:
when a dataset is replicated with `zfs send | zfs recv` (a TrueNAS replication task),
its user-properties travel with the stream, so the destination has everything the
driver needs to re-adopt the volume.

The one piece that does **not** replicate is the **export configuration** (the iSCSI
target/extent, NVMe-oF subsystem/namespace, or NFS share). Those objects live in
TrueNAS's configuration database, not on the pool, so a `zfs recv` restores the data
(the zvol/dataset) but not the export. The driver handles this automatically:
`ControllerPublishVolume` calls `ensureShareExists`, which **verifies each export
object by ID and recreates it if missing**. So a restored volume's export is rebuilt
the first time a pod attaches it — no manual re-export step.

> This is the exact path exercised by the `ensureShareExists` GET-by-ID verification:
> a stored `truenas-csi:*_id` property that no longer resolves on the destination is
> treated as "missing" and the target/namespace/share is recreated.

## What replicates vs. what you must back up separately

| Item | Travels with `zfs send`? | How to recover |
|------|--------------------------|----------------|
| Volume data (dataset/zvol) | ✅ yes | TrueNAS replication task |
| Volume metadata (`truenas-csi:*` props) | ✅ yes (with `--props`/"include properties") | replicated with the dataset |
| CSI snapshots (on the pool) | ✅ yes (recursive replication) | replicated; re-import the `VolumeSnapshotContent` |
| iSCSI/NVMe/NFS **export** config | ❌ no (config DB, not pool) | **auto-recreated** by `ensureShareExists` on publish |
| Kubernetes `PV` / `PVC` / `VolumeSnapshotContent` objects | ❌ no (etcd) | back up with Velero / GitOps and re-apply |

## Runbook: fail over to a DR TrueNAS

1. **Replicate the CSI parent dataset.** On the primary TrueNAS, create a replication
   task for `zfs.datasetParentName` (e.g. `pool/csi`) to the DR TrueNAS. Use
   **recursive** + **include dataset properties** so every child volume and its
   `truenas-csi:*` metadata (and CSI snapshots) are carried over.

2. **Point a scale-csi install at the DR TrueNAS.** Deploy the driver on the DR cluster
   (or repoint the existing one) with the **same** `zfs.datasetParentName` and the same
   `StorageClass` definitions (same protocols, portals/transport). Keep
   `controller.replicas: 1` (see [Production](../production.md) — the operation lock is
   per-process).

3. **Restore the Kubernetes objects.** Re-apply the `PV`/`PVC` objects (and any
   `VolumeSnapshotContent`) from your etcd/Velero/GitOps backup. The critical field is
   `spec.csi.volumeHandle`, which **is** the dataset name under the parent — it must
   match the replicated dataset. Static PVs bind by `volumeHandle`; the driver adopts
   the existing dataset via the idempotent-create / `ensureShareExists` path.

4. **Attach.** When a pod schedules onto the volume, `ControllerPublishVolume` →
   `ensureShareExists` verifies the export by ID and **recreates the iSCSI target /
   NVMe namespace / NFS share** that didn't replicate. `NodeStageVolume` then logs in
   and mounts. No manual export step is required.

5. **Verify.** Confirm the pod reaches `Running`, data is present, and a leak audit is
   clean (`iscsiadm -m session` / `nvme list-subsys` on the node show exactly the
   expected sessions).

## Notes and caveats

- **Snapshots restore, but their K8s objects don't.** CSI snapshots on the pool
  replicate with the dataset, but the `VolumeSnapshot`/`VolumeSnapshotContent` API
  objects live in etcd — re-import them (static `VolumeSnapshotContent` with the
  replicated snapshot's handle) if you need snapshot-driven restores on the DR side.
- **Replication direction / promotion.** ZFS replication targets are read-only until
  promoted; run the failover only after the destination dataset is promoted to
  read-write on the DR TrueNAS.
- **RPO is your replication schedule.** The driver adds no async buffering — your
  recovery point is whatever the TrueNAS replication task last completed.
- **Single-writer safety still applies.** Do not run workloads against the same
  volume on both sites simultaneously; the driver provides no cross-site fencing
  (this is the standard CSI shared-responsibility model — see
  [Production → Known limitations](../production.md)).
- **App-level consistency.** ZFS/CSI snapshots are crash-consistent. For databases,
  prefer the application's own backup/replication (e.g. CloudNativePG) for
  transaction-consistent DR rather than relying on volume snapshots alone.
