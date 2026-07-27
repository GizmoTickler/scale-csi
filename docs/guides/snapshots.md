# Snapshots and Volume Cloning

Scale CSI leverages native ZFS capabilities for instant snapshots and space-efficient clones.

## Prerequisites

- Kubernetes 1.20+ with VolumeSnapshot CRDs installed
- Scale CSI driver deployed
- Snapshot controller deployed in cluster

### Installing Snapshot CRDs and Controller

If not already installed:

```bash
# Install VolumeSnapshot CRDs
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml

# Install snapshot controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/master/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml
```

## Creating a VolumeSnapshotClass

Create a VolumeSnapshotClass that references the Scale CSI driver:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: scale-snapclass
driver: csi.scale.io
deletionPolicy: Delete
```

### Deletion Policies

| Policy | Description |
|--------|-------------|
| `Delete` | Delete snapshot when VolumeSnapshot is deleted |
| `Retain` | Keep snapshot on TrueNAS after VolumeSnapshot deletion |

## Taking a Snapshot

### Basic Snapshot

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: my-snapshot
spec:
  volumeSnapshotClassName: scale-snapclass
  source:
    persistentVolumeClaimName: my-pvc
```

Apply the snapshot:

```bash
kubectl apply -f snapshot.yaml

# Check snapshot status
kubectl get volumesnapshot my-snapshot
```

### Snapshot Status

```bash
# Get detailed snapshot info
kubectl describe volumesnapshot my-snapshot

# Check if snapshot is ready
kubectl get volumesnapshot my-snapshot -o jsonpath='{.status.readyToUse}'
```

## Restoring from a Snapshot

Create a new PVC from the snapshot:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: restored-pvc
spec:
  storageClassName: scale-nfs
  dataSource:
    name: my-snapshot
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi  # Must be >= original size
```

**Important**: The restored PVC size must be greater than or equal to the original volume size.

### Restore mode: clone vs detached

By default a restore is a ZFS **clone**: it is instant and space-efficient, but
the restored volume pins its source snapshot until the restored volume is
deleted. To get a fully independent volume with no lingering snapshot
dependency, use a StorageClass whose `snapshotRestoreMode` parameter is
`detached` (an independent local send/receive copy — more time and space up
front, no source-snapshot pin afterward):

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs-detached-restore
provisioner: csi.scale.io
parameters:
  protocol: nfs
  snapshotRestoreMode: detached
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

The value is resolved per StorageClass; when unset it follows the driver's
`zfs.detachedVolumesFromSnapshots` default (`false` = clone). See the
[StorageClass reference](../reference/storageclass.md#restore-mode).

## Cloning a Volume

You can create a clone directly from an existing PVC without creating a snapshot first:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cloned-pvc
spec:
  storageClassName: scale-nfs
  dataSource:
    name: source-pvc
    kind: PersistentVolumeClaim
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi  # Must be >= source size
```

### How Cloning Works

1. CSI driver creates a temporary ZFS snapshot of the source volume
2. Clone is created from the snapshot using `zfs clone`
3. Temporary snapshot is tracked and cleaned up when the clone is deleted
4. Writes are independent (changes don't affect the source), but the clone is
   **not lifecycle-independent**: it pins that temporary origin snapshot until the
   clone is deleted

Volume-to-volume cloning is **always** clone-backed; `snapshotRestoreMode` only
governs restores whose source is a `VolumeSnapshot`.

## VolSync Integration

Scale CSI fully supports [VolSync](https://volsync.readthedocs.io/) for backup and disaster recovery.

### Example ReplicationSource

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationSource
metadata:
  name: my-backup
spec:
  sourcePVC: my-pvc
  trigger:
    schedule: "0 */6 * * *"  # Every 6 hours
  restic:
    pruneIntervalDays: 7
    repository: my-restic-repo
    retain:
      hourly: 6
      daily: 7
      weekly: 4
    copyMethod: Snapshot
    volumeSnapshotClassName: scale-snapclass
```

### VolSync Workflow

1. VolSync triggers backup on schedule
2. CSI creates ZFS snapshot via VolumeSnapshot
3. VolSync mounts snapshot and backs up data
4. Snapshot is deleted after backup completes

## Best Practices

### Snapshot Naming

Use descriptive names with timestamps:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: postgres-backup-2024-01-15
  labels:
    app: postgres
    backup-type: scheduled
spec:
  volumeSnapshotClassName: scale-snapclass
  source:
    persistentVolumeClaimName: postgres-data
```

### Scheduled Snapshots

Use a CronJob or operator like VolSync for scheduled snapshots:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: hourly-snapshot
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: snapshot-creator
          containers:
            - name: kubectl
              image: bitnami/kubectl
              command:
                - /bin/sh
                - -c
                - |
                  kubectl apply -f - <<EOF
                  apiVersion: snapshot.storage.k8s.io/v1
                  kind: VolumeSnapshot
                  metadata:
                    name: myapp-$(date +%Y%m%d-%H%M)
                    namespace: default
                  spec:
                    volumeSnapshotClassName: scale-snapclass
                    source:
                      persistentVolumeClaimName: myapp-data
                  EOF
          restartPolicy: OnFailure
```

### Cleanup Old Snapshots

Use labels and a cleanup job:

```bash
# Delete snapshots older than 7 days
kubectl get volumesnapshot -l app=myapp \
  -o jsonpath='{range .items[?(@.metadata.creationTimestamp<"2024-01-08")]}{.metadata.name}{"\n"}{end}' \
  | xargs kubectl delete volumesnapshot
```

## Limitations

1. **Snapshot Size**: Snapshots are point-in-time; they grow as source changes
2. **Cross-Pool**: Snapshots cannot span ZFS pools
3. **Clone Size**: Clone must be >= source volume size
4. **Foreign snapshots block volume delete**: `DeleteVolume` refuses (returns
   `FailedPrecondition`) when the dataset carries non-CSI snapshots — for
   example those made by a TrueNAS periodic-snapshot task covering the CSI
   parent. Remove them, exclude the parent from the task, or opt into
   `zfs.destroyForeignSnapshotsOnDelete`.
5. **Deleting a snapshot that still has clones**: the driver renames it to an
   internal tombstone and requests deferred ZFS destruction. The snapshot
   disappears from CSI immediately, but its referenced space stays charged until
   the last dependent clone (for example a `clone`-mode restore) is deleted.

## Troubleshooting

### Snapshot Stuck in "Pending"

```bash
# Check snapshot controller logs
kubectl logs -n kube-system deploy/snapshot-controller

# Check CSI controller logs
kubectl logs -n scale-csi deploy/scale-csi-controller -c scale-csi
```

### Clone Creation Slow

For large volumes or busy TrueNAS systems, increase the timeout:

```yaml
# In Helm values
zfs:
  zvolReadyTimeout: 120  # Increase from default 60 seconds
```

### Deleting a snapshot that still has clones

You do **not** need to delete dependent clones first to complete the CSI delete.
`DeleteSnapshot` renames a clone-backed snapshot to an internal tombstone and
requests deferred ZFS destruction: the `VolumeSnapshot` disappears from CSI
immediately, and the reconcile reaper destroys the underlying snapshot once its
last dependent clone releases it. Its referenced space stays charged until then.

To see which volumes still pin snapshots (clone-mode restores):

```bash
# List PVCs that were restored/cloned from a VolumeSnapshot
kubectl get pvc -o json | jq '.items[] | select(.spec.dataSource.kind=="VolumeSnapshot")'
```
