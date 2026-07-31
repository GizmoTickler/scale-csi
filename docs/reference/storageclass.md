# StorageClass reference

All protocols use the unified provisioner `csi.scale.io`.

| `protocol` | Access modes | Volume modes |
|---|---|---|
| `nfs` | RWO, ROX, RWX | Filesystem |
| `iscsi` | RWO | Filesystem, Block |
| `nvmeof` | RWO | Filesystem, Block |

## Parameters the driver understands

| Parameter | Meaning | Required |
|---|---|---|
| `protocol` | `nfs`, `iscsi`, or `nvmeof` | Yes when more than one protocol is enabled |
| `snapshotRestoreMode` | `clone` or `detached` — how a volume restored from a snapshot is materialized | No; default is `clone` unless the driver sets `zfs.detachedVolumesFromSnapshots` |
| `csi.storage.k8s.io/fstype` | Standard external-provisioner filesystem selection for formatted block volumes | No; block default is `ext4` |

`protocol` and `snapshotRestoreMode` are the scale-csi-specific ordinary
parameters. A multi-protocol driver returns `InvalidArgument` when `protocol`
is absent; it no longer silently chooses NFS. A single-protocol legacy
deployment may omit it and uses its sole enabled protocol.

`snapshotRestoreMode` selects how a snapshot content-source restore is
materialized, per StorageClass:

- `clone` (default) creates a ZFS clone. Restore is instant and
  space-efficient, but the clone pins its source snapshot for its whole life —
  the snapshot cannot be reclaimed until the restored volume is deleted.
- `detached` creates an independent local send/receive copy. It costs more time
  and space up front but has no dependency on the source snapshot afterward.

An invalid value returns `InvalidArgument` listing `clone, detached`. When the
parameter is omitted, the driver falls back to its `zfs.detachedVolumesFromSnapshots`
config default (chart value `zfs.detachedVolumesFromSnapshots`, default `false`
= clone).

ZFS properties, TrueNAS endpoints, and protocol service settings belong in the
driver configuration/Helm values. The driver ignores ad-hoc StorageClass
parameters such as `dataset_recordsize`, `dataset_compression`,
`zvol_volblocksize`, `zvol_compression`, `mountOptions`, and `fsType`.
`mountOptions` is a top-level StorageClass list, and the standardized filesystem
key is `csi.storage.k8s.io/fstype`.

## NFS

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs
provisioner: csi.scale.io
parameters:
  protocol: nfs
mountOptions:
  - nfsvers=4
  - noatime
  - hard
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

NFS supports `ReadWriteOnce`, `ReadOnlyMany`, and `ReadWriteMany`. Use hard
mount semantics for persistent data; soft mounts can surface application-visible
I/O errors during a transient server outage.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: shared-media
spec:
  storageClassName: scale-nfs
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Ti
```

## iSCSI

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-iscsi
provisioner: csi.scale.io
parameters:
  protocol: iscsi
  csi.storage.k8s.io/fstype: ext4
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

Filesystem-mode claim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: database-filesystem
spec:
  storageClassName: scale-iscsi
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

Raw-block claim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: database-block
spec:
  storageClassName: scale-iscsi
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```

iSCSI is single-path; dm-multipath is unsupported. CHAP authentication is
supported (see [iSCSI CHAP](#iscsi-chap) below) but CHAP authenticates the
session — it does not encrypt data in flight — so also protect TCP 3260 with
node-only network policy outside Kubernetes (for example a storage VLAN and
firewall/SGACL rules).

## iSCSI CHAP

CHAP is strictly opt-in and is configured per StorageClass. Two things must be
true for a class to use CHAP:

1. The controller is opted in via Helm (`iscsi.chap.enabled: true`). With this
   off (the default), no CHAP peers are managed and every target stays
   `authmethod=NONE`.
2. The StorageClass references a Kubernetes Secret holding the credential, via
   the standard CSI secret-ref parameters. The same Secret is used for both
   provisioning (`CreateVolume`) and node staging (`NodeStageVolume`).

### The CHAP Secret

One Secret per StorageClass credential, shared by all volumes of that class
(per-volume credentials are not supported — there is no CSI channel to deliver a
driver-generated per-volume secret to the node). TrueNAS enforces a secret
length of **12–16 characters inclusive** for both `password` and
`mutualPassword`; the driver rejects anything else with `InvalidArgument` before
calling TrueNAS.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: scale-iscsi-chap
  namespace: scale-csi
stringData:
  username:       chapuser          # required
  password:       chapsecret123     # required, 12-16 chars
  mutualUsername: peeruser          # optional; presence implies CHAP_MUTUAL
  mutualPassword: peersecret456     # required iff mutualUsername is set, 12-16 chars, != password
  tag:            "1234"            # optional operator-pinned iscsi.auth tag (omit to derive)
```

Legacy open-iscsi-style aliases are also accepted: `node.session.auth.username`,
`node.session.auth.password`, `node.session.auth.username_in`,
`node.session.auth.password_in`.

### The CHAP StorageClass

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-iscsi-chap
provisioner: csi.scale.io
parameters:
  protocol: iscsi
  csi.storage.k8s.io/provisioner-secret-name: scale-iscsi-chap
  csi.storage.k8s.io/provisioner-secret-namespace: scale-csi
  csi.storage.k8s.io/node-stage-secret-name: scale-iscsi-chap
  csi.storage.k8s.io/node-stage-secret-namespace: scale-csi
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

The chart renders those four parameters from a `storageClasses[]` entry that
sets `chapSecretName` (and optional `chapSecretNamespace`, defaulting to the
release namespace); see the bundled `scale-iscsi-chap` example in `values.yaml`.
The chart references the Secret by name/namespace only — credential values are
never templated into the chart or the ConfigMap.

Setting `mutualUsername`/`mutualPassword` in the Secret switches the class to
`CHAP_MUTUAL` (bidirectional) authentication; otherwise one-way CHAP is used.
The per-volume auth mode is stamped immutably at `CreateVolume` (as a local
dataset property) and every later fence/idempotent-rebuild path reads that stored
mode — never the controller-wide `iscsi.chap.mutual` Helm flag. This means one-way
and mutual StorageClasses coexist safely, and flipping the global flag or
restarting the controller never changes an existing volume's authmethod.

### Behavior and limitations

- **Session auth only.** CHAP is applied to the node session record
  (`node.session.auth.*`) before login. Discovery auth is out of scope.
- **Shared peer, not deleted per volume.** The driver creates one `iscsi.auth`
  peer per StorageClass credential (keyed by tag) and reuses it. `DeleteVolume`
  does not delete the peer; removing a CHAP StorageClass leaves its peer behind
  for the operator to reap.
- **Rotation.** Update the Secret's `password`/`mutualPassword` (keep the same
  `username` and tag). The next `CreateVolume` on the class detects the changed
  credential — the in-driver peer cache is validated by credential fingerprint,
  not just username — and re-keys the backend peer in place via
  `iscsi.auth.update` (no controller restart required); a redacted
  `ISCSICHAPRotated` Event records it. The tag is stable, so no target group is
  touched. Established sessions survive a rotation — CHAP is checked only at
  login — so a rotated secret takes effect on the next `NodeStageVolume`. To force
  immediate enforcement, cordon/drain the node to re-stage.
- **CHAP policy is immutable per volume.** The auth *mode* and *tag* are fixed for
  a volume's lifetime. An idempotent `CreateVolume` replay that would change the
  policy — enabling CHAP on a previously non-CHAP volume (or the reverse), or a
  different tag/mode — is rejected with `FailedPrecondition`. Only the secret
  *value* rotates (above); to change the policy, provision a new volume.
  Note: because the shared auth peer is ensured before the per-volume policy
  guard runs, a *rejected* policy-change replay can still have side effects on
  the shared peer — a mode-shape change (one-way↔mutual) re-keys the shared
  peer (you will see an `ISCSICHAPRotated` Event alongside the
  `FailedPrecondition`), and a username change creates a new peer that the
  rejected volume then never uses (one bounded orphan per username change).
  Existing volumes and their groups are unaffected either way.
- **Wrong secret.** A bad password fails `NodeStageVolume` fast with
  `Unauthenticated` and no discovery-retry storm; the pod stays
  `ContainerCreating` until the Secret is corrected.
- **Secrets are never persisted in the PV.** The only CHAP-related volume-context
  key is a non-secret mode flag (`chap: CHAP|CHAP_MUTUAL`); credentials are
  redacted from all logs and errors.

## NVMe-oF

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nvmeof
provisioner: csi.scale.io
parameters:
  protocol: nvmeof
  csi.storage.k8s.io/fstype: xfs
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
```

NVMe-oF requires TrueNAS SCALE 25.10+, `nvme-cli`, and the selected transport's
kernel modules on every eligible node. Set `nvmeof.subsystemHosts` or deliberately
choose `nvmeof.subsystemAllowAnyHost: true` when fencing is off.

## Restore mode

A class that restores snapshots into fully independent volumes (no lingering
snapshot dependency) sets `snapshotRestoreMode: detached`:

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

## Policies and binding

| Field | Options | Notes |
|---|---|---|
| `reclaimPolicy` | `Delete`, `Retain` | `Retain` preserves the backend object after PVC/PV release |
| `allowVolumeExpansion` | `true`, `false` | Online behavior depends on protocol, filesystem, and workload |
| `volumeBindingMode` | `Immediate`, `WaitForFirstConsumer` | The latter delays provisioning until scheduling |

The bundled chart does not expose controller topology configuration. Do not use
`allowedTopologies` as a scale-csi backend-routing guarantee; see the
[topology guide](../guides/topology.md).

## Upgrade: add `protocol` safely

Kubernetes treats StorageClass `parameters` as immutable. An in-place patch to
add `protocol` will be rejected. Create a replacement class, update workload
manifests, and then retire the old name deliberately:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs-v2
provisioner: csi.scale.io
parameters:
  protocol: nfs
mountOptions:
  - nfsvers=4
  - noatime
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

Existing bound PVs are not reprovisioned when application manifests start using
the new class; only new claims select it. If the original class name must be
preserved, delete and recreate that StorageClass only after every manifest and
default-class transition has been planned.

## Troubleshooting

```bash
kubectl get storageclass
kubectl describe pvc <claim>
kubectl -n scale-csi logs deploy/scale-csi-controller -c csi-provisioner
kubectl -n scale-csi logs deploy/scale-csi-controller -c scale-csi
kubectl -n scale-csi logs daemonset/scale-csi-node -c scale-csi
```

An error saying `StorageClass parameter "protocol" is required` means the
driver has multiple enabled protocol blocks and the class predates the explicit
selection requirement. Follow the immutable-class migration above.
