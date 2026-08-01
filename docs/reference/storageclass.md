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

## Block-protocol tuning

These parameters tune the iSCSI extent/target and the NVMe-oF subsystem per
StorageClass. Every one is optional: an omitted parameter uses the controller
default, so a class that sets none of them provisions exactly as it did before
these knobs existed.

| Parameter | Values | Applies to | Notes |
|---|---|---|---|
| `iscsi/blocksize` | `512`, `1024`, `2048`, `4096` | extent | Pair `4096` with a 16K `zvolBlocksize`. |
| `iscsi/pblocksize` | `true`, `false` | extent | Reports the physical blocksize to the initiator. |
| `iscsi/queuedCommands` | `32`, `128` | target | Per-target SCST queue depth. |
| `iscsi/insecureTpc` | `true`, `false` | extent | Default `true`. Set `false` to disable cross-LUN XCOPY/ODX. |
| `iscsi/readOnly` | `true`, `false` | extent | Read-only extent (restore-verify use cases). |
| `iscsi/availThreshold` | `1`–`99` | extent | Per-extent early-full warning percentage. |
| `iscsi/stableSerial` | `true`, `false` | extent | Derives a deterministic SCSI serial from the volume, so identity survives an extent rebuild. |
| `iscsi/authNetworks` | comma-separated CIDRs | target | Target-level network ACL. |
| `nvmeof/qidMax` | `1`–`65535` | subsystem | Maximum I/O queue count (a queue identifier is 16-bit). |
| `nvmeof/piEnable` | `true`, `false` | subsystem | T10-PI. Advanced — validate initiator support first. |

Invalid values return `InvalidArgument` at `CreateVolume`, and the chart's
`values.schema.json` rejects them earlier still, at `helm install`.

**Every parameter above is applied at volume CREATE and is then immutable for the
life of that volume.** No value is ever accepted and quietly ignored — see
[Mutability](#mutability-every-knob-is-fixed-at-create).

NVMe-oF **port** performance fields (`inlineDataSize`, `maxQueueSize`,
`portPiEnable`) are deliberately NOT StorageClass parameters — the port is
shared across volumes, so a per-class value would mutate a shared object under
other volumes. Supplying one returns `InvalidArgument`; configure them
install-wide under Helm `nvmeof.portPerf`. Those install-wide fields are applied
at port CREATE only: changing them on an install whose ports already exist is a
no-op, and the driver logs a warning naming each drifted field.

### These options are persisted per volume

The resolved options are stamped onto the volume's dataset as
`truenas-csi:block_*` / `truenas-csi:nvme_*` user properties at `CreateVolume`,
and **only** for the parameters the class actually set. Every later path that
rebuilds the share for an existing volume — `ControllerPublishVolume`, the
startup attachment reconcile, a DR/restore rebuild — reads those properties, so
the volume keeps its own geometry and safety settings instead of falling back to
the controller default. Without this, a restore rebuild would re-create the
extent at the default `512` over data laid out for `4096`, and would drop the
stable serial, read-only flag, `insecure_tpc`, target `auth_networks`,
`avail_threshold`, `qid_max` and `pi_enable`.

Resolution order is: StorageClass parameter → stored property → controller
default, merged **per key**. A class that sets only one knob therefore cannot
reset a volume's other stored values to the controller default.

### Mutability: every knob is fixed at create

There is exactly one rule, and it is the same for all ten parameters:

> A per-volume block-protocol parameter is applied when the volume is created and
> is **immutable** afterwards. If a `CreateVolume` for an existing volume
> resolves a value that is not already in effect, the call fails with
> `FailedPrecondition` naming the parameter. Nothing is accepted and ignored.

That includes turning a knob **off**. Emptying `iscsi/authNetworks` on a volume
whose target carries an ACL, and setting `iscsi/stableSerial: "false"` on a
volume whose serial is pinned, are changes like any other and are rejected the
same way — a dropped network ACL or an un-pinned SCSI identity is not something
to apply silently. A volume created with either knob off still replays at off
idempotently: "stableSerial is on" is decided from the volume's stamp, or from a
live serial equal to the deterministic one its name derives, never from the mere
presence of the serial TrueNAS auto-generates for every extent.

| Parameter | TrueNAS 26.0 API | Driver policy | Enforcement |
|---|---|---|---|
| `iscsi/blocksize` | mutable on `iscsi.extent.update`, but **not** over existing data | Immutable | `FailedPrecondition` (live extent **and** stored stamp) |
| `iscsi/pblocksize` | mutable on `iscsi.extent.update`, but **not** over existing data | Immutable | `FailedPrecondition` (live extent **and** stored stamp) |
| `iscsi/queuedCommands` | mutable (`iscsi.target.update` → `iscsi_parameters.QueuedCommands`) | Immutable by driver policy | `FailedPrecondition` |
| `iscsi/insecureTpc` | mutable (`iscsi.extent.update`) | Immutable by driver policy | `FailedPrecondition` |
| `iscsi/readOnly` | mutable (`iscsi.extent.update` → `ro`) | Immutable by driver policy | `FailedPrecondition` |
| `iscsi/availThreshold` | mutable (`iscsi.extent.update`) | Immutable by driver policy | `FailedPrecondition` |
| `iscsi/stableSerial` | mutable (`iscsi.extent.update` → `serial`) | Immutable — it *is* the volume's SCSI identity | `FailedPrecondition` |
| `iscsi/authNetworks` | mutable (`iscsi.target.update`) | Immutable by driver policy | `FailedPrecondition` |
| `nvmeof/qidMax` | mutable (`nvmet.subsys.update`) | Immutable by driver policy | `FailedPrecondition` |
| `nvmeof/piEnable` | mutable (`nvmet.subsys.update`) | Immutable — changes the block-integrity format | `FailedPrecondition` |

`blocksize` and `pblocksize` are immutable at the **data** level: a volume's
filesystem and partition table are laid out against the logical block size the
initiator sees. The other eight are mutable at the TrueNAS API level and are
immutable by deliberate driver policy, for three reasons:

1. **The stamp, not the backend object, is the source of truth.** Every
   publish / startup-reconcile / DR rebuild re-creates the share purely from the
   volume's stored properties. Pushing a new value onto a live target or extent
   without also re-stamping would drift, and the next rebuild would silently
   revert it; re-stamping on the existing-volume arm would add a new fatal write
   and a new crash window to a path whose whole job is to be idempotent.
2. **Kubernetes treats StorageClass `parameters` as immutable.** A changed value
   can therefore never reach the driver as an in-place operator edit — only from
   a deleted-and-recreated class, or a different class colliding on the same
   volume name. Neither is an intent-to-mutate signal for a volume that already
   holds data.
3. **`ro`, `insecure_tpc`, `auth_networks` and `pi_enable` are enforced while an
   initiator is connected.** Silently retargeting a live, mounted volume's safety
   posture is worse than refusing and saying exactly why.

How the check decides whether a value is "already in effect": the **live backend
object is authoritative** when it reports the field, and the volume's stored
stamp is the fallback **only** for a field TrueNAS omits from its query response.
So a same-value replay is never rejected, and a value that was never applied —
for example `iscsi/stableSerial` added to a class after its volumes were
provisioned — is rejected rather than acknowledged.

That rule holds for **all ten** knobs without exception: every field the check
reads is nullable in the driver's response model (`pblocksize`, `insecure_tpc`
and `ro` included), so "the backend did not report this" stays distinguishable
from "the backend reported false" and an omitted field can never masquerade as an
authoritative one.

A rebuild path that carries **no** StorageClass parameters (`ControllerPublish`,
the startup attachment reconcile, a DR restore) has no opinion at all and is
never rejected; it simply replays the volume's own stamp.

To change any of these, provision a new volume and migrate the data.

### Restoring a snapshot or cloning a volume

A ZFS clone shares its source's data byte-for-byte, so the clone's on-disk layout
is the **source's** layout. A restore or clone into a class whose
`iscsi/blocksize` or `iscsi/pblocksize` differs from the source volume's is
rejected with `FailedPrecondition` — Kubernetes restricts PVC-to-PVC cloning to a
single StorageClass but places no such restriction on restoring a
`VolumeSnapshot` into a different class, so this is reachable in exactly the
deployment two differently-tuned classes invite.

The source's geometry is read from its stamp when it has one, and from its **live
iSCSI extent** when it does not — which is the case for every volume provisioned
before these parameters existed. A pre-existing 4096 volume is therefore just as
protected as a stamped one: restoring its snapshot into a `blocksize: "512"`
class is rejected, not accepted with a 512-byte extent laid over 4096-geometry
data. The live lookup is issued only when the class opts into a geometry the
stamp cannot answer, so the default provisioning path costs exactly what it did
before.

A restore into a class that sets **no** geometry inherits the source's geometry
(the conservative direction) rather than reverting to the controller default, and
performs no lookup at all.

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

> **The effective per-class opt-in is the non-blank Secret username.** For
> chart-generated StorageClasses, CHAP engages at `CreateVolume` only when the
> global gate is on **and** the provisioner Secret carries a non-blank username
> (the driver's internal `iscsi.chapSecret` marker — the chart does not emit
> `iscsi.chapSecret=true`). Under current behavior a readable referenced Secret
> whose username is absent or blank **fails open** to `authmethod=NONE` rather
> than entering CHAP validation; only a request that has already selected CHAP
> fails closed on malformed or missing CHAP fields. Ensure the referenced Secret
> always carries a username.

### The CHAP Secret

One Secret per StorageClass credential, shared by all volumes of that class
(per-volume credentials are not supported — there is no CSI channel to deliver a
driver-generated per-volume secret to the node). The driver validates the Secret
locally and rejects it with `InvalidArgument` **before** calling TrueNAS (once
CHAP has been selected). The enforced rules are:

- `password` and, when present, `mutualPassword` must be **12–16 bytes**
  inclusive. The check uses Go `len`, so the boundary is bytes, not runes.
- Leading or trailing whitespace and the `#` character are rejected.
- `mutualPassword` is required if and only if `mutualUsername` is set, and it
  **must differ** from `password`.
- `tag`, if present, must be a **positive integer**.
- The accepted keys and aliases are listed below.

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
  tag:            "1234"            # optional positive-integer iscsi.auth tag (omit to derive)
```

Legacy open-iscsi-style aliases are also accepted: `node.session.auth.username`,
`node.session.auth.password`, `node.session.auth.username_in`,
`node.session.auth.password_in`.

### Peer identity and tag derivation

Peer identity is **tag-based**, and the tag is derived from the *username* — no
StorageClass identity participates. The precedence is:

1. a positive Secret `tag`, if set; otherwise
2. a positive global `iscsi.chap.tag`, if set; otherwise
3. FNV-1a of the username mapped into `[1000, 61000)`.

Consequences to plan for:

- Two StorageClasses that use the **same username** with no explicit tag share
  the **same derived tag and `iscsi.auth` peer**.
- A positive global `iscsi.chap.tag` makes **every** untagged class contend for
  one peer.
- Two **different** usernames that resolve to the same explicit or global tag
  collide and fail with `FailedPrecondition`.

Operators who need per-class peer isolation must pin **distinct positive Secret
`tag` values**.

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

> **`iscsi.chap.mutual` is currently inert.** Production code never reads
> `config.ISCSI.CHAP.Mutual`; the effective mode for a new volume is derived
> solely from whether the Secret carries a `mutualUsername`. Treat the flag as an
> ignored compatibility key, not a working default-mode hint, until it is
> implemented.

### Behavior and limitations

- **Session auth only.** CHAP is applied to the node session record
  (`node.session.auth.*`) before login. Discovery auth is out of scope.
- **Shared peer, keyed by tag, not deleted per volume.** The driver creates one
  `iscsi.auth` peer per **tag** (derived from the username as above) and reuses
  it, so two classes sharing a username/tag share one peer. `DeleteVolume` does
  not delete the peer; removing a CHAP StorageClass leaves its peer behind for the
  operator to reap. The peer lives in the TrueNAS configuration database, not on
  the pool, so it does **not** ZFS-replicate (see the DR guide for the peer
  recovery prerequisite).
- **Rotation.** Update the Secret's `password`/`mutualPassword` (keep the same
  `username` and tag). The next `CreateVolume` on the class detects the changed
  credential — the in-driver peer cache is validated by credential fingerprint,
  not just username — and re-keys the backend peer in place via
  `iscsi.auth.update` (no controller restart required); a redacted
  `ISCSICHAPRotated` Event records it. The tag is stable, so no target group is
  touched. **Established sessions survive a rotation**, and the new node
  credential is applied only before a **fresh login** (including after a
  stale-session disconnect) — not merely on the next `NodeStageVolume`, which can
  reuse a healthy pre-rotation session and its old credentials. For immediate
  enforcement, coordinate an unstage/logout or drain the node **and verify the old
  session is gone** (`iscsiadm -m session`); a controller restart does not force
  reauthentication.
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
