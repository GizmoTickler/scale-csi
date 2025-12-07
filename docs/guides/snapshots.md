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
3. Temporary snapshot is tracked and cleaned up when clone is deleted
4. Clone is immediately independent - changes don't affect source

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
4. **Deletion Order**: Cannot delete a volume with dependent snapshots

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

### "Snapshot has dependent clones" Error

Delete all clones created from the snapshot before deleting the snapshot:

```bash
# List PVCs that were cloned from snapshots
kubectl get pvc -o json | jq '.items[] | select(.spec.dataSource.kind=="VolumeSnapshot")'
```
