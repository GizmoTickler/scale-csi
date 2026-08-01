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
   clock-skew allowance.

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
or spring-forward gap introduces no slack. The value is cached with a one-hour
TTL, warmed by the background reconcile pass and dropped on reconnect, so no CSI
operation pays a round trip for it, and an unscheduled volume never asks. The
driver image embeds the IANA database in the binary (`time/tzdata`), so zone
resolution behaves identically in the container and in tests.

Two timezone failure modes exist, and both **fail closed** — the snapshot is
treated as foreign and PRESERVED, never destroyed:

- the zone cannot be read (API failure, or a value that is not a loadable IANA
  zone). Watch `scale_csi_nas_timezone_unresolved_total`; while it is climbing, a
  scheduled volume's `DeleteVolume` returns `FailedPrecondition`.
- the NAS's timezone was CHANGED after the snapshots were taken. Their names then
  describe the old civil clock and no longer agree. The window is deliberately
  NOT widened to absorb this: a false-foreign is a preserved snapshot, a
  false-owned is deleted data. Remove the snapshots, or set
  `zfs.destroyForeignSnapshotsOnDelete: true`, to let the delete proceed.

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
> Reading the NAS timezone does not change that boundary; it only removes the
> accidental-collision slack. The timestamp check accepts a fixed 5-second window
> (±2s) around one specific instant, so a name whose timestamp was chosen
> anywhere within a day agrees by chance with probability 5/86400 ≈ 5.8e-5 (over
> a week, 8.3e-6, and it keeps shrinking with the range). The earlier design
> accepted 241 of every 900 seconds — 26.8%, and that rate did not shrink with
> range at all. It is still not literally zero: an actor who creates the snapshot
> at the second its name encodes passes, which is precisely the
> storage-administrator case above and is unchanged by this.

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
