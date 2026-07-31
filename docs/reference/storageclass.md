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
| `nfsSecurity` | Comma list of `SYS`, `KRB5`, `KRB5I`, `KRB5P` — the export's `security` list | No; unset omits the field (TrueNAS default AUTH_SYS) |
| `nfsExposeSnapshots` | Boolean — publish the dataset's read-only `.zfs/snapshot` tree through the export | No; default false |
| `nfsReadOnly` | Boolean — create the export read-only | No; default false |
| `nfsMaprootUser` / `nfsMaprootGroup` | Per-class override of the root mapping | No; defaults to `nfs.shareMaproot*` |
| `nfsMapallUser` / `nfsMapallGroup` | Per-class override of the all-users mapping | No; defaults to `nfs.shareMapall*` |
| `nfsAllowedNetworks` / `nfsAllowedHosts` | Comma lists overriding the static export allow-lists | No; defaults to `nfs.shareAllowed*`. Ignored in strict fencing mode, which owns these lists |
| `nfsACLTemplate` | `NFS4_OPEN`, `NFS4_RESTRICTED`, `NFS4_HOME`, `NFS4_DOMAIN_HOME`, `NFS4_ADMIN` — a builtin NFSv4 ACL applied at create | No; mutually exclusive with `nfsACL` |
| `nfsACL` | Explicit NFSv4 dacl as a JSON array | No; mutually exclusive with `nfsACLTemplate` |
| `zfsPerformanceClass` | `database`, `media`, `vm`, `backup`, `general` — a curated ZFS property preset | No; unset inherits the parent dataset's properties |
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
driver configuration/Helm values, or in the curated `zfsPerformanceClass` below.
The driver ignores ad-hoc StorageClass parameters such as `dataset_recordsize`,
`dataset_compression`, `zvol_volblocksize`, `zvol_compression`, `mountOptions`,
and `fsType`. `mountOptions` is a top-level StorageClass list, and the
standardized filesystem key is `csi.storage.k8s.io/fstype`.

## ZFS performance classes

`zfsPerformanceClass` applies a vetted ZFS property preset to newly provisioned
volumes. Every value is validated against the backend's own
`recordsize`/`compression`/`checksum` choice lists at `CreateVolume`, so a
mismatch is an `InvalidArgument` rather than an opaque `pool.dataset.create`
failure.

| Class | `recordsize` (fs) | `volblocksize` (zvol) | `sync` | `logbias` | `compression` | `primarycache` | `special_small_block_size` | `atime` |
|---|---|---|---|---|---|---|---|---|
| `database` | 16K | 16K | standard | latency | LZ4 | all | 16K | off |
| `media` | 1M | 64K | standard | throughput | ZSTD | all | — | off |
| `vm` | 64K | 16K | standard | latency | LZ4 | all | — | off |
| `backup` | 1M | 128K | standard | throughput | ZSTD | metadata | — | off |
| `general` | 128K | 16K | standard | latency | LZ4 | all | — | off |

Filesystem-only keys (`recordsize`, `atime`) are dropped for zvols and the
volume-only key (`volblocksize`) is dropped for filesystems, exactly as
`zfs.datasetProperties` already behaves.

The preset is layered **under** `zfs.datasetProperties`: an explicit operator
key always wins. The one exception matches pre-existing behavior — zvol
geometry has a single owner, so a `datasetProperties` `volblocksize` is still
warned-and-skipped when the class (or `zfs.zvolBlocksize`) already set it.

`special_small_block_size` requires the pool to have a `special`
allocation-class vdev. When there is none, the driver drops the property with a
warning rather than failing provisioning. Note the correct key is
`special_small_block_size`; `special_small_blocks` is rejected by the API.

### ⚠ Create-only vs live-tunable properties

| Create-only (**immutable**) | Live-tunable |
|---|---|
| `volblocksize` — zvol geometry, immutable in ZFS itself | `recordsize`, `sync`, `compression`, `checksum` |
| `logbias`, `primarycache`, `secondarycache` — rejected by `pool.dataset.update` | `atime`, `special_small_block_size`, `copies`, `readonly` |

A volume records the class it was **created** with. If a bound PVC's
StorageClass later names a different class:

- when the difference touches any create-only property, `CreateVolume` returns
  `FailedPrecondition` naming the offending properties. The request is
  physically impossible to satisfy in place; provision a new volume with the
  desired class and migrate the data;
- when only live-tunable properties differ, the request succeeds and the driver
  logs that existing datasets are **not** retuned. Even if they were,
  `recordsize`/`compression`/`checksum` apply only to NEW writes — blocks
  already on disk keep the geometry they were written with.

A volume provisioned before this feature existed carries no class stamp; it is
never wedged, only warned about.

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

### NFS version selection (v3 / v4 / v4.1)

**Version is a node-side mount option, not a share property.** There is no
`vers` field on `sharing.nfs.*`; a client picks the protocol with `vers=` /
`nfsvers=` and the TrueNAS NFS service must have that MAJOR version enabled
globally (`nfs.config` → `protocols`, typically `["NFSV3","NFSV4"]`). NFSv4.1 is
part of the server's `NFSV4` support and needs no extra server flag.

Set the version on the StorageClass's `mountOptions`. The driver passes them
through unchanged (deduplicated, as always).

Enable `nfs.versionPreflight` in the chart to have `CreateVolume` validate a
class's pinned version against the server's protocol list and return a clear
`FailedPrecondition` instead of letting the mount fail cryptically at
`NodeStageVolume`. It costs one cached `nfs.config` read per controller
lifetime and is off by default.

`nfs.ensureProtocols` can additively enable a version on the server, but that is
a **global service write affecting every export on the appliance** — it is
default-empty and should stay that way unless you have accepted that blast
radius.

### Validated performance mount-option profiles

| Profile | `mountOptions` | Use for |
|---|---|---|
| `v4.1-throughput` | `nfsvers=4.1`, `nconnect=8`, `hard`, `noatime`, `rsize=1048576`, `wsize=1048576` | Large sequential I/O (media, backups) |
| `v4.1-lowlat` | `nfsvers=4.1`, `nconnect=4`, `hard`, `noatime`, `ac` | Small random I/O (databases) |
| `v3-compat` | `nfsvers=3`, `hard`, `noatime` | Apps that need v3 locking/semantics |

`nconnect` opens N TCP connections behind ONE NFSv4.1 session (session
trunking). It is purely client-side — no server state, no TrueNAS setting. Older
kernels and NFSv3 ignore it silently, which is safe. The node logs a warning
(and changes nothing) when it sees `nconnect` with `vers=3` or two conflicting
`vers=` options.

For NFS-over-RDMA the server needs `nfs.config.rdma` enabled and clients mount
with `proto=rdma`. That is an advanced, out-of-default-scope profile.

### Export security and snapshot exposure

`nfsSecurity` sets the export's `security` list. Leaving it unset omits the
field entirely, which is the historical behavior and leaves TrueNAS on its
AUTH_SYS default.

`KRB5`/`KRB5I`/`KRB5P` require Kerberos on the NFS service (`nfs.config`
`v4_krb` plus a keytab). The driver **fails closed**: those modes are rejected
with `InvalidArgument` unless the operator sets `nfs.krbEnabled=true` to
acknowledge that Kerberos is actually configured.

Security is a **create-time** property of a volume. The driver never rewrites an
existing share's security, because flipping SYS→KRB5 on a live export breaks
every mounted client.

`nfsExposeSnapshots` publishes the volume's read-only `.zfs/snapshot` directory
through the export, which composes with the driver's snapshot machinery to give
in-place browsing of point-in-time copies. TrueNAS only honors it when the
export path is the dataset root — always true for CSI volumes.

### NFSv4 ACLs

`nfsACLTemplate` (a builtin TrueNAS NFS4 template) or `nfsACL` (an explicit
dacl) applies an NFSv4 ACL to a newly provisioned volume's dataset. When either
is set, the driver additionally creates the dataset with `acltype=NFSV4` and
`aclmode=PASSTHROUGH`; when neither is set, both properties keep inheriting from
the parent exactly as before.

`filesystem.setacl` is an asynchronous middleware job. ACL application is
**best-effort**: it runs after the dataset, its ownership stamps and its export
all exist, and a failure produces a Warning event on the PVC rather than a
failed `CreateVolume`. A volume never fails to bind because a permission model
could not be applied; re-apply out of band if needed.

#### ⚠ ACL × `fsGroup` — read this before enabling

This driver ships `CSIDriver.fsGroupPolicy: File`. Under that policy **kubelet
recursively chowns and chmods a volume to the Pod's `securityContext.fsGroup` at
every publish**, which rewrites the mode-bearing ACEs and silently defeats a
driver-applied ACL.

`fsGroupPolicy` is a driver-global field and effectively immutable on a live
`CSIDriver`, so it cannot be chosen per StorageClass and the shipped default is
deliberately **not** changed (flipping it would alter fsGroup semantics for every
existing volume).

Mitigations, in order of preference:

1. Run ACL-managed workloads with **no `securityContext.fsGroup`**. Nothing then
   rewrites the ACL.
2. For an installation fully committed to ACL-managed volumes, install the chart
   with `csidriver.fsGroupPolicy: None`. Do this on a **fresh installation**;
   changing it on an existing one requires recreating the CSIDriver object and
   changes fsGroup behavior for all its volumes.
3. The driver always sets `nfs41_flags.protected: true` on ACLs it applies, so a
   chmod cannot make the server recompute the ACL from the mode. This limits —
   but does not eliminate — the damage under `File`.

Every volume that receives a driver-applied ACL also gets a Warning event
(`NFSACLFsGroupConflict`) spelling this out.

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
