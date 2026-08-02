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
| `snapshotSchedule` | Five-field cron (`minute hour dom month dow`) for a driver-managed periodic-snapshot task scoped to the volume (GF2/E2) | No; default is the controller-wide `zfs.snapshotSchedule` (empty = off) |
| `snapshotRetention` | Bounded time-based retention for those snapshots, e.g. `24h`, `30d`, `2w`, `6M`, `1y` | No; default `zfs.snapshotRetention` (empty = 30d safety bound) |
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

### Driver-managed periodic snapshots (GF2/E2)

Setting `snapshotSchedule` makes the driver own ONE non-recursive
periodic-snapshot task scoped to each volume's dataset, so a PVC gets automatic
point-in-time snapshots with bounded retention and no external scheduler or
box-wide task covering the CSI parent. The task's naming schema is stamped on
the volume dataset BEFORE the task is created (so a task can never outlive its
binding), its id is stamped after, and it is removed at `DeleteVolume`.

**There is no `snapshotNamingSchema` parameter, by design.** Task-created
snapshots carry no CSI user properties (TrueNAS 26.0 cannot add properties to an
existing snapshot), so their provenance has to be assembled from durable state
the driver controls. The driver therefore mints the schema itself as
`csi-<volume>-<16-hex nonce>-%Y%m%d-%H%M%S`. A snapshot is treated as
driver-created — and therefore deleted with the volume rather than protected by
the foreign-snapshot guard — only when ALL of the following hold:

1. it sits on the volume's own dataset;
2. that dataset carries this driver instance's LOCAL ownership stamp;
3. that dataset carries a schema the driver's own algorithm re-derives
   byte-for-byte for this volume (nonce and all);
4. a driver-minted, non-recursive periodic-snapshot task carrying exactly that
   schema is observed alive on exactly that dataset at delete time (or was
   recorded alive by an earlier attempt of the same delete);
5. the snapshot's name is a complete, CANONICAL rendering of that schema whose
   timestamp is a real calendar instant; and
6. that instant is EXACTLY when the snapshot was created — its own `creation`
   property, rendered in the NAS's own civil timezone, within a ±2 second
   clock-skew allowance; and
7. that timezone is the one RECORDED on the dataset when the task was created
   (`truenas-csi:snapshot_task_timezone`, local source only) AND is still the
   NAS's live zone.

Anything else stays FOREIGN and is preserved by the default policy: an operator
snapshot named `csi-preupgrade`, a box-wide task using a `csi-` schema, a
replication-inherited name, a name with an impossible date, a non-canonical
volume segment, or a name whose timestamp does not match when the snapshot was
really taken.

**Timezone dependency.** A periodic-snapshot task renders `%Y%m%d-%H%M%S` from
the NAS's LOCAL civil clock, while a snapshot's `creation` property is UTC epoch
seconds. The driver therefore reads the NAS's zone from `system.general.config`
(`timezone`, an IANA name) and converts `creation` into that civil clock —
epoch → civil, which is total and unambiguous, so a DST fall-back repeated hour
or spring-forward gap introduces no slack.

Reading only the CURRENT zone is not enough, because not every reconfiguration
changes the civil fields: `America/New_York` → `America/Toronto` never does, and
a switch to a fixed `-05:00` does not for a winter-created snapshot. So the zone
in force when the TASK was created is WRITTEN DOWN on the volume's own dataset
(`truenas-csi:snapshot_task_timezone`) and compared against the live value. It is
write-once — a CreateVolume retry after a re-home must not overwrite the evidence
— and it is read only when its ZFS source is `local`, so a clone, a
replication-received dataset or a detached copy that merely INHERITS it proves
nothing. A task is never created at all when the zone cannot be read, because its
snapshots could never afterwards be proven.

There is no driver-level cache of the live zone. The single cache lives on the
API client, is dropped on reconnect, never caches a failure, and expires after 5
minutes; only a scheduled volume's `DeleteVolume` asks for it, so the default
path pays nothing. The driver image embeds the IANA database in the binary
(`time/tzdata`), so zone resolution behaves identically in the container and in
tests.

Three timezone failure modes exist, and all **fail closed** — the snapshot is
treated as foreign and PRESERVED, never destroyed:

- the zone cannot be read (API failure, or a value that is not a loadable IANA
  zone). Watch `scale_csi_nas_timezone_unresolved_total`; while it is climbing, a
  scheduled volume's `DeleteVolume` returns `FailedPrecondition`.
- the dataset carries no locally-sourced recorded zone (including an inherited
  one on a clone/received/detached copy).
- the recorded zone and the NAS's live zone DIFFER — i.e. the NAS was re-homed
  after the task was created. The window is deliberately NOT widened to absorb
  this: a false-foreign is a preserved snapshot, a false-owned is deleted data.
  Remove the snapshots, or set `zfs.destroyForeignSnapshotsOnDelete: true`, to
  let the delete proceed.

> **Trust boundary — stated plainly.** These checks do NOT establish "a snapshot
> this driver did not create cannot be deleted", and no claim to that effect
> should be made anywhere. The naming schema is READABLE by anyone who can read
> the dataset property or run `pool.snapshottask.query`, and TrueNAS 26.0 can
> neither stamp a user property on an existing snapshot nor attribute a snapshot
> to the task that made it. An actor with pool-write access on the CSI parent can
> therefore construct a snapshot indistinguishable from a task-created one — it
> need only carry the exact schema rendering and be created at the second its
> name encodes. **Storage-administrator access to `zfs.parentDataset` is a
> trusted boundary for this feature.** What the checks do guarantee is that no
> snapshot outside that trust boundary — an unrelated task, a replication
> stream, a clone-inherited property, a `csi-`-prefixed operator snapshot,
> another volume's schema, another driver instance — is ever destroyed as if the
> driver had made it.
>
> Reading (and pinning) the NAS timezone does not change that boundary; it only
> removes the accidental-collision slack. The timestamp check accepts a fixed
> 5-second window (±2s) around one specific instant, so a name whose timestamp
> was chosen anywhere within a day agrees by chance with probability 5/86400 ≈
> 5.8e-5 (over a week, 8.3e-6, and it keeps shrinking with the range). The
> earlier design accepted 241 of every 900 seconds — 26.8%, and that rate did not
> shrink with range at all. It is still not literally zero: an actor who creates
> the snapshot at the second its name encodes passes, which is precisely the
> storage-administrator case above and is unchanged by this.
>
> **Why this is WONTFIX and not a deferred fix.** Authorship is unprovable in
> principle on TrueNAS 26.0: it can neither stamp a property on an existing
> snapshot nor attribute one to the task that made it, so the driver cannot prove
> authorship of its OWN scheduled snapshots either. The alternative posture —
> treat every unprovable snapshot as foreign — would therefore wedge the
> `DeleteVolume` of every scheduled volume, permanently. Because this predicate
> exists solely to BLOCK the recursive destroy when something foreign is present,
> the residual is exactly and only this: **a foreign snapshot that matches both
> the nonce-bearing name and the creation second that name encodes will not block
> the destroy.** Nothing in this repository may claim more than that.

Retention is TIME-based only — TrueNAS 26.0 exposes no
count cap — so an empty `snapshotRetention` resolves to a 30d safety bound and
never grows unbounded snapshots. The feature is off until a schedule is set
(per-SC parameter or the controller-wide `zfs.snapshotSchedule`).

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: scale-nfs-pitr
provisioner: csi.scale.io
parameters:
  protocol: nfs
  snapshotSchedule: "0 */6 * * *"   # every six hours
  snapshotRetention: "2w"           # keep two weeks
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
```

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
the startup attachment reconcile, a DR restore) has no opinion about the eight
tuning knobs and is never rejected on them; it simply replays the volume's own
stamp. Geometry is different, and has its own rule — see
[Geometry is recorded, never guessed](#geometry-is-recorded-never-guessed).

To change any of these, provision a new volume and migrate the data.

### Restoring a snapshot or cloning a volume

A ZFS clone shares its source's data byte-for-byte, so the clone's on-disk layout
is the **source's** layout. A restore or clone into a class whose
`iscsi/blocksize` or `iscsi/pblocksize` differs from the source volume's is
rejected with `FailedPrecondition` — Kubernetes restricts PVC-to-PVC cloning to a
single StorageClass but places no such restriction on restoring a
`VolumeSnapshot` into a different class, so this is reachable in exactly the
deployment two differently-tuned classes invite.

For an **iSCSI** source, the source's geometry is read from **both** its stamp
and its **live iSCSI extent**, on every clone or restore. A pre-existing 4096 volume is
therefore just as protected as a stamped one: restoring its snapshot into a
`blocksize: "512"` class is rejected, not accepted with a 512-byte extent laid
over 4096-geometry data. Where the stamp and the live extent disagree, the clone
is refused and both values are named — a drifted source has no establishable
geometry, and the driver will not pick one for you.

A restore into an iSCSI class that sets **no** geometry inherits the **source's**
geometry, and that geometry is recorded on the destination. It does not revert to
the controller default. A class that names no geometry still produces an extent,
and gating the lookup on "did the class ask" is exactly what let the
controller-wide default — a helm value, not a StorageClass parameter — silently
supply the geometry for a no-opts restore. NVMe-oF uses its namespace path and
does not run the iSCSI geometry guard; NFS clones and every non-clone path pay
nothing for it.

NVMe-oF has a separate geometry surface: this driver's TrueNAS namespace
create/query model exposes no namespace block-size input or reported field, so
the zvol/platform owns that value. The iSCSI extent stamp, iSCSI live-extent
probe, and iSCSI recovery error do not apply to NVMe-oF clones or snapshot
restores, and the driver does not fabricate iSCSI geometry properties for them.

A **PVC-to-PVC clone** asks what the source is addressed through *now*, because
its temporary snapshot is taken from the source's current state: one source
`pool.dataset.query` (skipped here — the path already read the source) plus one
`iscsi.extent.query`.

### Snapshot geometry provenance

A **snapshot restore** asks a different question: what are the bytes *inside this
snapshot* addressed through. The source's current extent cannot answer it — a
source whose extent was re-created at a different geometry after the snapshot was
taken would hand the restore a layout the snapshot's data was never written
against. Provenance is therefore tied to the snapshot itself:

- For an **iSCSI** source, `CreateSnapshot` records the source's **live** geometry
  on the snapshot it takes. ZFS captures a dataset's user properties at snapshot
  time, so this is a durable point-in-time record. It costs one
  `iscsi.extent.query` per iSCSI zvol snapshot; filesystem and NVMe-oF snapshots
  pay no iSCSI geometry query.
- For iSCSI, the live extent is consulted on each snapshot, including for a
  volume that already records a geometry. A recorded value is a record of a
  record; the extent is the bytes. Where the two disagree — an extent re-created
  at a different geometry after the volume was stamped — `CreateSnapshot` **fails
  `FailedPrecondition`** and names both values, rather than capturing a stale
  record onto bytes it does not describe. (Such a volume is already unpublishable
  for the same reason, so nothing that worked stops working.)
- An iSCSI restore reads that captured record. When it is present and complete,
  the restore issues **no source read at all**. NVMe-oF restore reads no iSCSI
  geometry record and continues through namespace creation.
- An iSCSI snapshot that captured **no** geometry, whose source shows any history
  of having been block-addressed, **fails `FailedPrecondition`**. The driver will
  not lay a guessed geometry over a snapshot's data. A snapshot of a
  driver-provisioned zvol nothing has ever exported is unaffected — there is no
  layout to preserve. A snapshot of a zvol the driver did **not** create is not
  in that category: absence of the driver's bookkeeping is not evidence that the
  bytes are unaddressed, so it fails closed too.

> **Upgrade note.** Snapshots taken before this version carry no captured
> geometry, so restoring one of a block volume fails closed until its real
> geometry is recorded. Confirm the value the snapshot's data was written
> against, then record it on the **snapshot**:
>
> ```sh
> zfs set truenas-csi:block_blocksize=4096 \
>         truenas-csi:block_pblocksize=true tank/k8s/volumes/pvc-...@snap-...
> ```
>
> `truenas-csi:block_blocksize` must be one of 512, 1024, 2048, 4096; any other
> value is treated as untrusted and the restore keeps failing closed.
>
> Snapshots taken from this version on carry it automatically. NFS snapshots are
> unaffected.

### Geometry is recorded, never guessed

`iscsi.extentBlocksize` and `iscsi.extentDisablePhysicalBlocksize` are
install-wide defaults for **new** volumes only. Neither can reach a volume that
already holds data, in either direction:

- **Every extent the driver creates or observes is recorded on its dataset**
  (`truenas-csi:block_blocksize`, `truenas-csi:block_pblocksize`), including for
  a StorageClass that opts into nothing — what a volume *has* is a different fact
  from what its class *asked for*. Volumes provisioned before this shipped are
  recorded the first time the driver sees their extent alive (a publish, a
  startup reconcile, an idempotent replay). On a fresh create the record and the
  extent-ID witness also ride in the same fatal property update as the ownership
  stamp, so they are durable-or-rolled-back with the rest of provisioning. Every
  write is folded into a dataset update those paths already issue, so none of it
  costs an extra round trip.
- **The record must be COMPLETE.** Logical and physical block size are resolved
  together, from the same evidence, by one function. A volume that records only
  one of the two is refused rather than having the other filled in from today's
  install-wide default.
- **A StorageClass parameter is intent, not evidence.** An explicit
  `iscsi/blocksize` may only *agree* with what the storage is already known to
  be. It supplies a value only where the storage is **provably** free of
  block-addressed data, and that proof is POSITIVE, never the absence of the
  driver's own bookkeeping: either the zvol was created by this very call, or it
  carries this driver instance's `truenas-csi:driver_instance_id` ownership stamp
  with ZFS source `local` **and** no witness of ever having been exported. A zvol
  the driver did not create — imported, attached by an administrator, or restored
  by some other tool — cannot supply that proof and is refused. An *inherited*
  ownership stamp is the source dataset's fact, not the clone's, and does not
  count. Reconciliation may add a separate
  `truenas-csi:driver_instance_id_adopted` marker to a legacy dataset for
  cleanup ownership; that marker is not create-time provenance and does not
  qualify the dataset for this proof.
- **Two records that disagree are never combined.** Where a destination's own
  record and its content source's record both describe the same bytes and give
  different values, the driver names both and refuses. It never fills the
  missing half of one record from the other while keeping a contradicted value —
  that would manufacture a geometry that was never observed anywhere.
- **Changing `iscsi.extentBlocksize` never re-geometries an existing volume.** A
  rebuild whose extent is absent replays the volume's own recorded geometry. It
  never falls back to the current default, because the default may have moved
  since the data was written.
- **If the geometry cannot be established, the driver refuses.** A volume whose
  extent is gone *and* which carries no complete geometry record fails
  `FailedPrecondition` rather than being re-created at a guess. Recover by
  restoring the original extent, or by recording the real values:

  ```sh
  zfs set truenas-csi:block_blocksize=4096 \
          truenas-csi:block_pblocksize=true tank/k8s/volumes/pvc-...
  ```

  `truenas-csi:block_blocksize` must be one of **512, 1024, 2048, 4096** — the
  sizes an extent can actually be created at. A stored value outside that set (a
  typo in the command above) is treated as **untrusted**: it records nothing, the
  volume reads as unrecorded, and the rebuild keeps failing closed rather than
  acting on it. The same applies to a stored `queuedCommands`, `availThreshold`
  or `qidMax` outside its documented range.

- **An extent the driver did not create is validated before it is adopted.** If a
  create-error recovery or an "already exists" fallback returns an extent whose
  geometry differs from the one the create was authorized at, the driver refuses
  and names both values instead of adopting it and recording its geometry as this
  volume's truth.
- **The live extent is authoritative for what the data is; the record states the
  intent it was provisioned with.** Where the two disagree the driver names both
  and refuses, rather than silently preferring either.

#### What the record claims, exactly

Recording an extent's geometry says **what that extent reports now**. It is proof
of how the data is addressed today, and it is what stops any later rebuild — or
any later change to the install-wide defaults — from reaching the volume.

It is **not** proof of historical truth. A volume that was already corrupted
before this mechanism existed — an unstamped 512-byte extent laid over
4096-layout bytes by an old defaulted rebuild — is observationally
indistinguishable from a correct 512-byte volume. Recording its geometry freezes
the observable state; it cannot repair the history. Only an operator who knows
the original geometry can correct such a volume, by recording the real values and
re-creating the extent.

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
