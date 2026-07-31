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
   task for the dataset configured as chart value `zfs.parentDataset` (driver
   config key `zfs.datasetParentName`, e.g. `pool/csi`) to the DR TrueNAS. Use
   **recursive** + **include dataset properties** so every child volume and its
   `truenas-csi:*` metadata (and CSI snapshots) are carried over.

   > **Snapshot-lifecycle cost of this task.** A periodic/replication task's own
   > snapshots created *under* the CSI parent are **foreign** to the driver and by
   > default block `DeleteVolume` (`FailedPrecondition`) on any volume that carries
   > them — see [Production](../production.md#current-known-limitations). This is a
   > deliberate tension with the "exclude the CSI parent from snapshot tasks" rule.
   > For DR, prefer a task snapshot **naming schema/hold** you can prune, exclude
   > the task's own snapshots from the volumes' delete path, or accept
   > `zfs.destroyForeignSnapshotsOnDelete: true` on the DR install. Otherwise later
   > PVC deletion on the DR cluster wedges until those task snapshots are removed.

2. **Point a scale-csi install at the DR TrueNAS.** Deploy the driver on the DR cluster
   (or repoint the existing one) with the **same** `zfs.parentDataset` value and the same
   `StorageClass` definitions (same protocols, portals/transport). Keep
   `controller.replicas: 1` (see [Production](../production.md) — the operation lock is
   per-process).

3. **Restore the Kubernetes objects.** Re-apply the `PV`/`PVC` objects (and any
   `VolumeSnapshotContent`) from your etcd/Velero/GitOps backup. The critical field is
   `spec.csi.volumeHandle`, which is the **volume ID — the sanitized child leaf
   only, not the full `pool/parent/leaf` path**. The driver rejects any handle
   containing a `/` and joins the leaf to its configured `zfs.datasetParentName`, so
   the leaf must equal the replicated dataset's basename and the DR install must use
   the same parent. Static PVs bind by `volumeHandle`; the driver adopts the
   existing dataset via the idempotent-create / `ensureShareExists` path.

4. **Attach.** When a pod schedules onto the volume, `ControllerPublishVolume` →
   `ensureShareExists` verifies the export by ID and **recreates the iSCSI target /
   NVMe namespace / NFS share** that didn't replicate. `NodeStageVolume` then logs in
   and mounts. No manual export step is required.

5. **Verify.** Confirm the pod reaches `Running`, data is present, and a leak audit is
   clean (`iscsiadm -m session` / `nvme list-subsys` on the node show exactly the
   expected sessions).

## Runbook: fencing takeover for a confirmed-dead node

In `fencing.mode: strict`, a `SINGLE_NODE` volume that still carries a durable
publication record for a *different* node is refused with `FailedPrecondition`
until the stale record is revoked. When the controller can prove the old node's
`VolumeAttachment` is gone, it revokes the stale record and grants the new node
**synchronously** (a "takeover"). This is the normal, safe recovery path — prefer
it over any manual intervention.

**(a) The empty-list brake is intentional, not a bug.** If the controller cannot
list `VolumeAttachments` (informer unsynced, API discontinuity) or the list comes
back **empty while a backend record still exists**, it treats that as *ambiguous*
— not as evidence of absence — and **keeps** `FailedPrecondition` instead of
taking over. A zero-result list can mean "the attachment is gone" *or* "the API
server momentarily returned nothing"; acting on the wrong reading would revoke a
grant a live node still holds. The controller fails safe and lets the periodic
reconcile (with its mass-absence brake and grace window) converge.

**(b) Force-removing a VolumeAttachment finalizer bypasses that brake.** The
escape hatch is:

```bash
kubectl patch volumeattachment <name> -p '{"metadata":{"finalizers":null}}'
```

This deletes the attachment object out from under the controller's safety check,
which can then observe "no attachment" and proceed to take over the volume. That
is **ONLY** safe once the old node is **CONFIRMED dead** — powered off, fenced at
the BMC/IPMI, or cordoned + drained + `NotReady` well beyond the pod grace period.
Force-removing the finalizer while the old node is still alive and mounted lets
two nodes hold the same `SINGLE_NODE` volume simultaneously, which **corrupts the
filesystem**. When in doubt, do not remove the finalizer.

**(c) Prefer the controller's synchronous takeover.** Before any manual finalizer
removal, watch the driver perform takeovers itself:

```promql
sum by (reason) (rate(scale_csi_fencing_takeover_total[1h]))
```

A `ScaleCSIFencingTakeoverSpike` alert fires on any takeover. If takeovers are
not happening and a volume is stuck `FailedPrecondition`, first confirm the node
is genuinely dead (step b), then — and only then — remove the finalizer. A
`ScaleCSIFencingProvenanceOverflow` alert is a different condition (publishes
refused because additive grant provenance exceeded its cap) and points at node
identity churn or stale backend hosts, not a dead node.

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
  [Production → Known limitations](../production.md#current-known-limitations)).
- **Publication records replicate too.** The durable per-volume publication
  records are `truenas-csi:publication_*` user-properties, so a replicated
  dataset can arrive on the DR side still carrying a record naming a primary-side
  node. This is safe: the driver performs synchronous stale-record takeover on
  publish, so the first attach on the DR cluster reconciles the record to the new
  node. It is another reason not to run both sites against the same volume at
  once.
- **App-level consistency.** ZFS/CSI snapshots are crash-consistent. For databases,
  prefer the application's own backup/replication (e.g. CloudNativePG) for
  transaction-consistent DR rather than relying on volume snapshots alone.
