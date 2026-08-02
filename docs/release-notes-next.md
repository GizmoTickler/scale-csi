# Release notes — v1.6.0

## v1.6.0 — GF-Sprint 1: per-volume encryption at rest

One theme: **ZFS-native encryption at rest, opt-in per StorageClass, default
off.** A class that sets none of the new keys provisions the byte-identical
pre-encryption payload, the default Helm render is unchanged, and a deployment
that never touches encryption behaves exactly as before. Encryption is folded
into the single `pool.dataset.create` call (+0 RTT vs a plaintext create) and
works for NFS filesystems and iSCSI/NVMe-oF zvols alike. Everything here is
grounded in live nas01 probes (26.0.0-BETA.1, 2026-07-31); the API shapes are
pinned to that date and the backend is a BETA, so the drill is re-run before GA.

**Encryption is an availability hazard, not just a confidentiality feature.** A
locked dataset serves ZERO I/O. Read the upgrade note and the risk register
below before enabling it.

### ⚠ Upgrade note — encryption is default off, and stays that way

The controller-wide gate `encryption.enabled` defaults to `false`. With it off,
nothing encryption-related is parsed, stamped, or called, no `encryption:` block
renders into the ConfigMap, and a rolled-back binary that has no encryption
field still strict-parses the rendered config. A class opts in by setting
`encryptionSecretName`; the chart then emits the `encryption: "true"` parameter
and the provisioner / controller-publish / node-stage CSI secret refs (name and
namespace only — the passphrase is never templated into the chart or the
ConfigMap). Upgrading changes nothing for an install that does not opt in.

### Per-volume encryption, per StorageClass

- **The Secret contract.** One Kubernetes Secret per class, shared by all of its
  volumes. Keys: `passphrase` (required, **≥ 8 characters**, the ZFS minimum),
  `passphrasePrevious` (optional rotation window), `algorithm` (optional,
  default `AES-256-GCM`, validated against the probed backend choice set
  `{AES-128-CCM, AES-192-CCM, AES-256-CCM, AES-128-GCM, AES-192-GCM,
  AES-256-GCM}`), and `pbkdf2iters` (optional positive-integer override). The
  driver validates the Secret and rejects a bad one with `InvalidArgument`
  **before** any API call.
- **Create-time only.** Encryption is immutable for the life of a volume. There
  is no in-place encryption of an existing plaintext volume, and an idempotent
  `CreateVolume` replay that would flip encrypted↔plaintext is refused with
  `FailedPrecondition`. An encrypted create that also carries a content source
  (snapshot restore or clone) is refused with `InvalidArgument` before any
  mutation — restored bytes carry the origin's encryption state, so the driver
  will not stamp a key they do not hold. Restore to a plaintext class, or create
  new.
- **The passphrase is radioactive.** It exists only in the Secret, the
  short-lived parse, and request-scoped context. It never reaches a log, a gRPC
  status, a Kubernetes Event, the PV's `volumeContext`, or a dataset
  user-property. The only encryption-related volume-context key is a non-secret
  algorithm marker; the durable per-volume stamp is `truenas-csi:encryption =
  <algorithm>`, never the key.

### The availability model (read before enabling)

TrueNAS does **not** persist a passphrase key (`encryption_summary` reports
`key_present_in_database: false`), so the passphrase lives only in your Secret.

- **A nas01 reboot locks every encrypted volume**, and nothing on the appliance
  auto-unlocks it. A locked zvol has no backing block device; a locked
  filesystem has `mountpoint: null`. Both serve no I/O.
- **The unlock reconciler is best-effort, not a guarantee.** It runs at startup
  and on every reconcile pass, lists managed datasets stamped encrypted, gates on
  `encryption_summary.locked == true` (unlock is NOT idempotent — unlocking an
  unlocked dataset returns a FAILED job), resolves PV → StorageClass →
  controller-publish-secret → passphrase, and unlocks. **Recovery latency is the
  reconcile interval; during the window the affected pods see EIO.** Pair
  encrypted classes with pod liveness/restart so a pod that hit EIO re-stages
  after unlock. An operator who cannot tolerate that window should not encrypt.
- **Publish unlocks before the share is built**, so an extent always has a
  backing device before fencing converges. There is no node-side unlock (the node
  has no TrueNAS client); if unlock never happens, `NodeStageVolume` fails closed
  waiting for a device that never appears.
- **Health surfaces it.** A locked encrypted volume reports an abnormal
  `VolumeCondition` through `ControllerGetVolume` / `ListVolumes`.

### Key rotation

Set the new `passphrase` and keep the old one in `passphrasePrevious`. On the
next publish unlock the driver tries the new key first; on failure it unlocks
with the previous key and calls `pool.dataset.change_key` to rotate (`change_key`
requires the dataset unlocked). Once rotated the old key is dead, so a replay
lands on the new-key branch — rotation is idempotent by outcome. A redacted Event
records it. Both keys failing fails closed. Rotation is controller-side only;
there is no CSI rotate RPC and no node path.

### Clone inheritance

A `clone`-restore volume created from an encrypted origin inherits the origin's
key — its `encryption_root` is the **origin**, not itself. It is not
independently keyed: locking the origin locks the clone, and a clone cannot be
re-keyed (`change_key` on an inheriting child is refused by ZFS). Prefer
`snapshotRestoreMode: detached` for encrypted classes when you need key
independence (a detached copy has its own `encryption_root`); note a detached
restore is itself a content source and is therefore created plaintext.

### Risk register (stated bluntly)

- **R1 — locked dataset = dead I/O (HIGH, availability).** A nas01 reboot locks
  ALL encrypted volumes; running pods get EIO until the reconciler re-unlocks.
  Mitigation is the best-effort reconciler plus pod restart policy. This is an
  honest operational model, not a guarantee.
- **R2 — lost Secret = permanent data loss (CRITICAL).** The passphrase lives
  ONLY in the Kubernetes Secret; there is no TrueNAS-side escrow. Delete or
  rotate-away the Secret with no backup and the data is unrecoverable. Back up
  the Secret; use the `passphrasePrevious` window; the driver never auto-deletes
  it.
- **R3 — downgrade below v1.6.0 with encrypted volumes live (HIGH).** An older
  driver has no unlock logic, so a locked volume stays dead (not lost — keys are
  safe in the Secret). Do not roll back below encryption support while encrypted
  volumes exist; manual `pool.dataset.unlock` recovers. The
  `truenas-csi:encryption` user-prop is ignored by older drivers, so the rollback
  itself is safe; plaintext volumes are unaffected.
- **R4 — unlock is not idempotent (MED).** Unlocking an already-unlocked dataset
  returns FAILED; the driver always gates on `locked == true` and never treats
  that FAILED as volume-unhealthy.
- **R5 — clone shared-key surprise (MED).** `clone`-restore volumes inherit the
  origin key and cannot be re-keyed. Default encrypted classes to `detached`.
- **R6 — passphrase leak (HIGH).** Reuses and extends the CHAP redaction
  (`passphrase` is masked); asserted in tests; the key is never stamped as a
  dataset property and never carried in `volumeContext`.
- **R7 — pbkdf2iters cost (LOW).** The 1.3M-iteration default adds unlock CPU
  latency; acceptable, overridable, and off the steady-state I/O path.
- **R8 — BETA backend (MED).** nas01 runs 26.0.0-BETA.1; the encryption API shape
  could shift before GA. All shapes are pinned to the probe date; re-run the
  drill (`scripts/gf1-encryption-drill.sh`) before GA/merge.

Non-goals this sprint: KMIP / external key managers (design hook only), raw
hex-key mode (passphrase only), node-level dm-crypt/LUKS, in-place re-encryption
of existing plaintext volumes, encrypted send/recv replication to a DR target,
and automatic key escrow/cross-cluster key sync. The driver only ever UNLOCKS;
it never locks (locking is an operator/host action, exercised only by the drill
and tests).

Everything below this section is the v1.5.1 changelog, unchanged.

---

## v1.5.1 — live-drill fixes on top of v1.5.0

v1.5.0 was tagged but never deployed: a live drill against a real TrueNAS 26.0
appliance (nas01, 2026-08-02) failed it. v1.5.1 is that drill's fix list. No
flag defaults change and the default Helm render is unchanged.

- **`zfsPerformanceClass` was 100% non-functional (release blocker).** All five
  presets emitted `logbias` and `primarycache`, which TrueNAS 26.0 rejects, so
  every `CreateVolume` carrying the parameter failed with `Invalid params`. The
  presets now emit only properties the appliance accepts. Details and the
  corrected create-only/live-tunable split are in
  [ZFS performance classes](#zfs-performance-classes) below.
- **The mock let it ship.** `MockClient` accepted any dataset property while the
  real middleware is schema-strict, so every unit test and `csi-sanity` run
  passed against a payload the appliance would refuse. `MockClient` now validates
  `pool.dataset.create`/`.update` payload keys against a hand-maintained,
  PER-DATASET-TYPE classification — 26.0's models are per type, and the live
  errors are literally `data.FILESYSTEM.logbias` and `data.VOLUME.secondarycache`
  — so `recordsize` on a zvol and `volsize` on a filesystem now fail a unit test
  the way they fail on the appliance. The classification is deliberately NOT
  derived from the client's own structs: a coverage test diffs the struct JSON
  tags against it in both directions, so a new field with no classification entry
  fails the test instead of being auto-accepted. Which entries are probe-backed
  (`logbias`/`primarycache`/`secondarycache`, rejected on both types) and which
  are inferred from ZFS semantics is annotated key by key in
  `pkg/truenas/dataset.go`; it is a real gate for what has been established, not
  a complete model of 26.0. Test-only — production behavior is unchanged.
- **A partially-cleared NFS squash pair produced the exact opaque error the GF5
  preflight exists to remove.** `nfsMaprootUser: ""` alone, leaving the shipped
  `nfs.shareMaprootGroup: wheel`, orphaned the group and failed inside the
  middleware. A squash group with no user is now refused up front with an
  `InvalidArgument` naming both keys and the fix, for the `maproot_*` and
  `mapall_*` families alike — both were probed directly against
  `sharing.nfs.create`, which refuses each with
  `This field is required when map group is specified`. A user with no group is
  legal and still accepted. A globally orphaned pair (`nfs.shareMaprootGroup`
  set with an empty `nfs.shareMaprootUser`) additionally logs a WARNING at
  startup. It is not startup-fatal on purpose: a StorageClass can still supply
  the missing user, and refusing to start would take iSCSI and NVMe-oF
  provisioning down with it.
- **`DeleteVolume` blamed a foreign task for the driver's own tombstones.** A
  deferred `DeleteSnapshot` leaves a ledger-recorded tombstone; the following
  `DeleteVolume` refusal described it as "likely from a TrueNAS
  periodic-snapshot or replication task" and pointed the operator at
  `zfs.destroyForeignSnapshotsOnDelete`. The refusal (preserve-until-reaped)
  is unchanged and deliberate — the reconcile reaper clears these and the retry
  then succeeds — but the message now identifies driver-proven tombstones and
  drops the foreign-task advice when every blocking snapshot is one.
- **Sub-1GiB NFS volumes failed opaquely.** TrueNAS floors a dataset `refquota`
  at 1 GiB. `CreateVolume` now says so, scoped to volumes whose size is applied
  as a refquota (NFS with `zfs.datasetEnableQuotas`); zvols and quota-less NFS
  volumes are unaffected. The check runs on the CREATE path only, below the
  already-exists arm, so a sub-1GiB volume provisioned before
  `zfs.datasetEnableQuotas` was turned on keeps replaying idempotently instead of
  becoming an `InvalidArgument` for a dataset that exists and is healthy.

Everything below this section is the v1.5.0 changelog, unchanged except where a
v1.5.1 fix corrected a statement in it.

---

This document is the accumulated changelog from the v1.2.23 documentation
baseline through the **v1.5.0** release candidate, ordered newest-first. v1.5.0
is a backward-compatible **MINOR** release over v1.4.1 that lands the GF sprints
(storage-native data protection, block-volume geometry safety, NFS performance
and backend health) on top of it. Every new flag defaults OFF and the default
Helm render stays byte-identical to v1.4.1, but v1.5.0 **does** change one
runtime behavior for existing iSCSI volumes — read
[GF-Sprint 4](#gf-sprint-4--block-volume-geometry-safety) before upgrading.

The v1.4.0 entry below (iSCSI CHAP, CSIStorageCapacity tracking, opt-in
volume-health monitoring, clone-latency work, `truenas.maxConnections`, and an
observability taxonomy migration) and the v1.3.0 entry (which bundled the
v1.2.24–v1.2.35 fixes plus the batch 17–20
performance/resilience/maintainability work) are retained for history. Sections
after the per-release entries (Breaking change, Helm chart, Release governance)
are cross-cutting themes that span several of these releases.

## GF-Sprint 5 — NFS performance and backend health

Two themes. **NFS parity:** the `sharing.nfs.create` fields the driver used to
hard-code or omit are now a per-StorageClass surface, and a class that sets none
of them provisions the byte-identical pre-GF5 payload. **Backend health:** an
opt-in, READ-ONLY poller that looks at the pool underneath the volumes and says
what it sees — on every managed PVC's `VolumeCondition` and in Prometheus — with
an explicit account of when those two signals can disagree. Every flag defaults
OFF and the default Helm render stays byte-identical to v1.4.1.

### ⚠ Upgrade note — a leftover `nfs.shareMapall*` on an NFS-disabled install

`maproot_*` and `mapall_*` are mutually exclusive in TrueNAS, and the shipped
defaults set `nfs.shareMaprootUser: root` / `nfs.shareMaprootGroup: wheel`. From
v1.5.0 the controller validates that pair at config load instead of letting
every `sharing.nfs.create` fail with an opaque middleware error, and the chart
schema rejects the combination too.

That check is **scoped to `nfs.enabled`**. An install with NFS disabled and a
leftover `nfs.shareMapallUser` builds no NFS payload from those keys, was inert
on v1.4.1, and must not be turned into a controller crash-loop (or a blocked
`helm upgrade`) by an unused value. With NFS **enabled** the combination is a
hard startup error — it could never have provisioned a volume — and the fix is
the documented escape: clear the inherited maproot values.

The `nfs.shareSecurity` / `nfs.krbEnabled` half of the same validator is
deliberately unconditional. Both keys are new in this release, so no existing
install can be carrying a value it has not already seen.

### NFS export parameters, per StorageClass

- **`nfsSecurity`** — the export's `security` list (`SYS`, `KRB5`, `KRB5I`,
  `KRB5P`), with the controller-wide default `nfs.shareSecurity`. KRB5* is
  **fail-closed on `nfs.krbEnabled`** at all three gates that can reach the
  wire — the StorageClass parameter, config load, and the share builder —
  because a krb-only export on a box with no KDC or keytab makes every mount
  fail with an opaque server error. An empty list omits the field entirely, so
  an un-opted-in deployment marshals the pre-GF5 body.
- **`nfsMaprootUser`/`Group`, `nfsMapallUser`/`Group`** — per-class squash
  overrides. The mutual exclusion is validated against the **effective** payload
  (class override layered over the global config), which is the case that
  actually bites: with the shipped `maproot: root/wheel` default, a class that
  sets only `nfsMapallUser` resolves to both. The error names all four resolved
  values, and the way out is to clear the inherited pair
  (`nfsMaprootUser: ""`, `nfsMaprootGroup: ""`).
- **`nfsAllowedNetworks` / `nfsAllowedHosts`** — static per-class export
  allowlists. Under `fencing.mode: strict` they are **rejected** with
  `InvalidArgument` rather than accepted and discarded: strict fencing creates
  every share deny-all and owns the allowlist, so the parameters would have been
  a silent no-op.
- **`nfsReadOnly`**, **`nfsExposeSnapshots`** — read-only exports, and
  publishing the volume's read-only `.zfs/snapshot` tree through the export.
  `expose_snapshots` is `omitempty` on both the share struct and the create
  params, so an un-opted-in deployment's payload is unchanged.

All of these — plus `nfsACLMode` — are settable on a typed `storageClasses[]`
entry in the chart, not only through the untyped `extraParameters` map. The
squash keys are emitted whenever the entry **has** them, so
`nfsMaprootUser: ""` renders as an empty parameter rather than being dropped:
absent and empty mean different things to the driver, and empty is the
documented way to clear the default squash.

### NFSv4 ACLs, `aclmode`, and `fsGroup`

`nfsACLTemplate` (a builtin TrueNAS `NFS4_*` template) and `nfsACL` (an explicit
dacl, rendered as JSON) apply an ACL to newly provisioned volumes. They are
mutually exclusive, validated before any backend call, and the apply itself is
**best effort**: a failure emits a Warning Event and a log line, never a failed
bind. `acltype`/`aclmode` are stamped in the `pool.dataset.create` payload only
when an ACL was actually requested, and an operator-supplied `acltype` in
`zfs.datasetProperties` is never overridden.

`nfsACLMode` selects the dataset's `aclmode` — `PASSTHROUGH` (the historical
value, still the default) or `RESTRICTED`. `DISCARD` is deliberately not
offered: it deletes the whole non-trivial ACL on the first `chmod`.

**Read `docs/reference/storageclass.md` › "ACL × `fsGroup`" before enabling
this.** A pod `securityContext.fsGroup` makes kubelet `chown`/`chmod` the
volume, and under `PASSTHROUGH` that rewrites the mode-bearing ACEs of the ACL
the driver just applied. `nfsACLMode: RESTRICTED` is the only ZFS lever that
stops it, and it converts a silent, recoverable degradation into a **loud
publish failure** — including for the many images that `chmod` at startup. The
alternatives are running ACL-managed workloads with no `fsGroup`, or installing
with the new `csidriver.fsGroupPolicy: None` chart value (default stays `File`,
so the default render is unchanged; changing it on an existing install requires
recreating the CSIDriver object).

On a **content-source** volume (clone or snapshot restore) `aclmode` cannot be
set at all — there is no `pool.dataset.create` to carry it. `nfsACLMode` is
therefore refused with `InvalidArgument` **before any mutation**, rather than
materializing a volume whose `chmod` behavior is its origin's while the events
claim the requested mode. The dacl itself is still applied, and neither the
event text nor the docs report a mode the driver did not set.

### NFS version selection and the opt-in preflight

Version is a node-side mount option, not a share property: a class pins it with
`mountOptions: [nfsvers=4.1]` and the driver passes the list through unchanged.
The node side only **warns** (a conflicting second `vers=`, `nconnect` with v3)
and rewrites nothing.

`nfs.versionPreflight` (default off) validates a class's pinned major version
against the appliance's global `nfs.config` protocols at `CreateVolume` and
returns a clear `FailedPrecondition` instead of letting the mount fail
cryptically at `NodeStageVolume`. It costs zero API calls when it is off or when
the class pins no version, and it fails **open** if `nfs.config` cannot be read —
a preflight must never become a new provisioning failure mode.

Its cache has two rules worth knowing, because they are what make it usable:

- a **successful** read is cached for the controller's lifetime (protocol
  enablement is operator-driven and effectively static), but a **rejection**
  drops the cache. The error tells the operator to enable the version on the
  appliance; once they do, the next `CreateVolume` re-reads `nfs.config` and
  provisioning recovers with **no controller restart**;
- it gates **new** volumes only. It runs after the already-exists check, so an
  idempotent `CreateVolume` replay for an already-provisioned volume still
  succeeds while the server is missing the pinned version — CSI requires that
  replay to succeed, and a global protocol setting says nothing about an
  existing dataset.

`nfs.ensureProtocols` (default empty) can additively enable a major version on
the server, but it is a **global service write affecting every export on the
appliance**. It only ever adds, and it **fails closed** when the current
protocol list cannot be read COMPLETELY — `nfs.update {protocols: X}` SETS the
list rather than unioning with it, so merging into a partially-parsed base would
silently disable whatever the reader could not see, for every export on the box.

### ZFS performance classes

`zfsPerformanceClass` (`database`, `media`, `vm`, `backup`, `general`) applies a
vetted ZFS property preset — `recordsize`, `volblocksize`, `sync`,
`compression`, `special_small_block_size`, `atime` — to newly provisioned
volumes, validated against the backend's own
`recordsize`/`compression`/`checksum` choice lists so a mismatch is
`InvalidArgument` rather than an opaque `pool.dataset.create` failure. The
preset is layered **under** `zfs.datasetProperties` — an explicit operator key
always wins — and `special_small_block_size` is dropped with a warning on a pool
with no `special` vdev instead of failing provisioning. `volblocksize` is
**create-only** (immutable in ZFS itself), so a class change that would move a
zvol's geometry is rejected rather than applied; every other curated property is
live-tunable, so a filesystem class change is warned about rather than refused.

**No preset sets `logbias` or `primarycache`, and that is a backend limit rather
than a preference.** The v1.5.0 presets emitted both on the strength of a probe
of `pool.dataset.update` alone, generalized into "create-only through this API".
Probed properly on 2026-08-02 against live TrueNAS 26.0, `logbias`,
`primarycache` and `secondarycache` are rejected by `pool.dataset.create` **and**
`pool.dataset.update`, for FILESYSTEM and VOLUME alike (`Extra inputs are not
permitted`); they are absent from the 26.0 schema, and an audit of
`core.get_methods` found no alternative setter. Set either property out of band
with `zfs set` — e.g. on the CSI parent dataset, which new volumes inherit from —
if you need it. On a filesystem this makes `backup` and `media` resolve to the
same properties, since `primarycache=metadata` was all that separated them.

`MockClient` now enforces the 26.0 dataset payload schema, so a key the
appliance would reject fails a unit test instead of every `CreateVolume` against
a real appliance. `zfs.datasetProperties` still passes these three keys through —
the supported floor is 25.04 and only 26.0 was probed — but now logs a warning
naming the property and the `zfs set` alternative instead of leaving the
operator with `Invalid params`.

**The content-source exemption is deliberate and complete.** A PVC created from
a `dataSource` is materialized by a ZFS clone or a dataset copy; both inherit the
origin's geometry and accept no property payload, so the preset cannot be
applied — there is no API through which to apply it. The driver therefore
ignores the class on those volumes, **does not stamp it**, and emits a
`ZFSPerformanceClassIgnored` Warning Event. The stamp is written only on the
`createDataset` branch, an inherited stamp is scrubbed from a clone or a
detached replication copy, and `createVolumeExisting` independently treats a
content-source volume as unstamped — which is what keeps an exact replay
idempotent instead of a `FailedPrecondition`. Stamping a class that was never
applied would be worse than useless: the immutability guard treats the stamp as
ground truth.

### Backend health (`backendHealth.enabled`, default off)

A controller-only poll loop; each tick is at most **two bounded READ calls**
(`pool.query` + `disk.temperature_alerts`) and it writes nothing. Leaving it off
issues zero additional API calls and keeps `VolumeCondition` semantics
byte-identical to the pre-GF5 driver.

ZFS exposes no per-dataset health, so the pool verdict is fanned out onto every
managed PVC's `VolumeCondition` — a deliberate attribution, and the finest
granularity ZFS makes available. Severity is conservative:
`DEGRADED`/`FAULTED`/`UNAVAIL` are abnormal; an in-progress scrub or resilver, a
scan-error count and a disk temperature alert are descriptive messages only. A
routine monthly scrub must never mark every PVC in the cluster unhealthy. A
dataset-level `provision_success=false` still outranks any pool signal.

New gauges, all labeled by `pool`: `scale_csi_pool_status` (one-hot),
`scale_csi_pool_healthy`, `scale_csi_pool_scan_state` (one-hot across the whole
`function × state` domain), `scale_csi_pool_scan_errors`,
`scale_csi_pool_disk_temp_alerts` + `..._age_seconds`,
`scale_csi_pool_health_stale`, `scale_csi_pool_health_flip_pending` and
`scale_csi_pool_health_last_success_timestamp_seconds`. Alerts (rendered only
with the feature): `ScaleCSIPoolDegraded`, `ScaleCSIPoolScanErrors`,
`ScaleCSIPoolDiskTemperatureAlert`, `ScaleCSIPoolHealthStale`,
`ScaleCSIPoolConditionFlipPending`, `ScaleCSIPoolHealthProducerSkew`.

`scale_csi_pool_disk_temp_alerts` counts member **disks** with at least one
temperature alert — several alerts on one drive count once — plus any alert
whose disk the appliance did not identify, which are counted individually
because there is nothing to deduplicate them on.

Three properties are worth reading before wiring an alert to any of this:

- **The condition is debounced; the gauges are not.** A pool-health transition
  must be confirmed by two consecutive samples before it rewrites every managed
  PVC's condition, because the fan-out is fleet-wide and an unfiltered flap
  would churn N conditions and N Events per tick. Prometheus always sees the
  RAW sample, so a flap stays fully visible.
- **A cached snapshot has a TTL** of three times the effective (clamped) poll
  interval — 3m at the 60s default, never more than 6m. Past it the condition
  falls back to the exact pre-GF5 dataset-only semantics rather than asserting
  hours-old pool state as current fact, and `scale_csi_pool_health_stale` goes
  to 1 at the instant the snapshot stops being served.
- **The two signals can disagree, in seven named ways.** The canonical
  enumeration lives on `backendHealthFlipSamples` in `pkg/driver/backend_health.go`
  and is repeated verbatim in `prometheusrule.yaml`, `values.yaml`,
  `values.schema.json`, `docs/production.md` and `docs/deployment.md`:
  confirmation lag, alert hold, recovery, poll stall, observer lag, cold start
  and replica skew. The first three have an upper bound; the last four do not.
  Do not restate that list with a smaller count.

The poller has **no leader-election gate** — every controller replica runs it —
so the supported configuration is `controller.replicas: 1`, which the chart
enforces with both a schema `if/then` and a template `fail`, plus a
non-overlapping rollout (`maxSurge: 0`, or `Recreate` under fencing). That is a
singleton guard, not fencing: a drain or eviction can still overlap an old and a
new process, which is why `ScaleCSIPoolHealthProducerSkew` alerts on
`count by (pool) (scale_csi_pool_health_last_success_timestamp_seconds) > 1`
rather than the driver claiming to prevent it.

The CSI-facing snapshot and every backend-health metric are committed through
**one immutable generation** — a single atomic swap — so a `ControllerGetVolume`
and a Prometheus `Gather` can never observe different halves of the same sample.
That generation records the Driver that published it and is served back only to
that Driver, so a second Driver in the same process (a Driver that never enabled
the feature, on a pool it has never polled) is never handed someone else's pool
verdict.

## GF-Sprint 4 — Block-volume geometry safety

The theme is one sentence: **an extent's geometry is recorded, never guessed.**
Everything else in the sprint is either a per-volume tuning surface or a proof
that makes that sentence true. The default Helm render is unchanged, and a
StorageClass that sets none of the new parameters provisions exactly as it did
on v1.4.1.

### ⚠ Upgrade behavior change — iSCSI volumes with an absent extent

This is the one behavior change in the release, and it is deliberate.

Before v1.5.0, any path that had to (re-)create an iSCSI extent for an existing
volume — `ControllerPublishVolume`, the startup attachment reconcile, a DR
rebuild — created it at the **controller-wide default**
(`iscsi.extentBlocksize`, `iscsi.extentDisablePhysicalBlocksize`). For a volume
whose data was written against a different logical block size (a `4096` volume,
or any volume provisioned while the default was something else), that laid a
GUESSED geometry over existing data and corrupted it.

From v1.5.0, a volume whose extent is **absent** and whose geometry is
**unrecorded** — and which cannot prove it holds no block-addressed data — fails
`FailedPrecondition` instead. The reachable shape is a pre-GF-4 volume on a
TrueNAS restored from a configuration backup, or one whose extent an admin or an
upgrade removed. Recovery is to restore the original extent, or to record the
real geometry and retry:

```sh
zfs set truenas-csi:block_blocksize=4096 \
        truenas-csi:block_pblocksize=true tank/k8s/volumes/pvc-...
```

`block_blocksize` must be one of **512, 1024, 2048, 4096**; a value outside that
set is treated as untrusted, records nothing, and keeps the rebuild failing
closed rather than acting on a typo. Volumes the driver can see alive are
back-stamped automatically the first time it observes their extent (a publish, a
startup reconcile, an idempotent replay), so the exposure shrinks to zero on a
healthy install without any operator action.

**Blast radius, and why it is per-volume.** This refusal is PERMANENT: unlike
every other failure on the rebuild path it never self-heals on retry. It is
therefore classified as a per-volume operator condition rather than a
convergence failure — the startup attachment reconcile logs it, emits a
`StartupShareGeometryUnestablishable` **Warning Event on the PV** carrying the
`zfs set` recovery, and lets the pass converge for every other volume. In
`fencing.mode: strict` the controller still reaches ready, so `CreateVolume`,
`ControllerPublishVolume` and `ControllerExpandVolume` keep working
cluster-wide; only the affected volume is held. An absent extent exposes no data
path, so there is nothing on that volume to fence. RPC callers still receive the
unchanged `FailedPrecondition`.

### Ten per-volume block-protocol StorageClass parameters

`iscsi/blocksize`, `iscsi/pblocksize`, `iscsi/queuedCommands`,
`iscsi/insecureTpc`, `iscsi/readOnly`, `iscsi/availThreshold`,
`iscsi/stableSerial`, `iscsi/authNetworks`, `nvmeof/qidMax`,
`nvmeof/piEnable`. Each is optional; an omitted parameter uses the controller
default. Invalid values are rejected at `CreateVolume` (`InvalidArgument`) and
earlier still by the chart's `values.schema.json`.

The resolved values are stamped on the volume's dataset, so every later
publish / reconcile / DR rebuild replays the volume's own settings instead of
today's controller defaults.

**Immutability policy: every one of the ten is fixed at volume create.** A
`CreateVolume` for an existing volume that resolves a value not already in
effect fails `FailedPrecondition` naming the parameter — nothing is accepted and
quietly ignored, including turning a knob OFF. Several of these fields are
mutable at the TrueNAS API level; the driver deliberately does not reconcile
them onto a live object, because the volume's stamp (not the backend object) is
what every rebuild replays, and silently retargeting a mounted volume's safety
posture mid-flight is worse than refusing and saying why. See
[StorageClass reference › Mutability](reference/storageclass.md#mutability-every-knob-is-fixed-at-create).

### Snapshot geometry capture

`CreateSnapshot` on an iSCSI zvol now consults the **live extent** and captures
the resulting geometry onto the snapshot (one `iscsi.extent.query` per iSCSI
zvol snapshot; filesystem and NVMe-oF snapshots pay nothing). A snapshot restore
then answers from the geometry the snapshot itself captured, rather than from
what the source looks like at restore time — which is a different question once
a source's extent has been re-created at another geometry.

Two consequences worth knowing:

- Where the live extent and the volume's stamp **disagree**, `CreateSnapshot`
  fails `FailedPrecondition` naming both, rather than capturing a record it
  knows to be contradicted.
- Where the live extent could not be **read** (a transient API failure), the
  snapshot is still taken but explicitly records **no** geometry — the two keys
  are written with ZFS's `-` no-value sentinel, because a snapshot otherwise
  inherits its dataset's stamp and an unverified stamp is exactly what the live
  read exists to check. An iSCSI restore of such a snapshot fails closed with
  the same `zfs set` recovery — availability, not integrity. One scope limit: a
  cross-protocol restore of that geometry-less snapshot into an NVMe-oF class is
  outside the cross-protocol guard (which refuses only a positively recorded
  non-512 geometry) and keeps the unguarded pre-GF4 exposure — the platform
  derives the namespace LBA format unchecked.

Snapshots taken before v1.5.0 carry no captured geometry. Restoring one into an
iSCSI class succeeds as long as its source volume is still readable and
consistent; where it is not, the restore fails closed rather than guessing.

### Cross-protocol restore (iSCSI source → NVMe-oF class)

The driver makes no geometry claim about an NVMe-oF namespace — the zvol and the
platform own the LBA format. Restoring a snapshot of an iSCSI volume that
records a **non-512** logical block size into an NVMe-oF StorageClass is
therefore refused (`FailedPrecondition`): the same bytes would be presented
through a different addressing. Restore into an iSCSI class instead. A source
recorded at 512, or carrying no geometry record, is unaffected — and the check
costs no extra backend call.

### NVMe-oF chart values

- **`nvmeof.portPerf.{inlineDataSize,maxQueueSize,piEnable}`** — install-wide
  NVMe-oF port tuning. Deliberately NOT StorageClass parameters: the port is
  shared across volumes, so a per-class value would mutate a shared object under
  other volumes (supplying one returns `InvalidArgument`). Every field defaults
  to unset, which omits the API parameter and keeps port creation identical to a
  pre-GF-4 deployment. **CREATE-ONLY:** on an install whose ports already exist,
  changing them is a no-op and the driver logs a warning naming each drifted
  field.
- **`nvmeof.multipath` + `nvmeof.addresses`** — associates each subsystem with
  one port per address and advertises them all in the publish context.
  **HONEST SCOPE — CONTROLLER SIDE ONLY.** The node half is NOT shipped: the
  node still runs a single `nvme connect` to `address`, so enabling this today
  delivers NO multipath, NO load balancing and NO path failover. What it
  delivers is the backend exposure the node work will build on, at 2*(N-1) extra
  API calls per volume. There is also no ANA on this platform. Leave it off
  unless you are deliberately staging that exposure. `multipath: true` with an
  empty `addresses` is a startup validation error.

## GF-Sprint 2 — Storage-native data protection

Four opt-in features that use TrueNAS-native mechanisms to protect CSI volumes
and snapshots. Every flag defaults OFF and the default Helm render stays
byte-identical to v1.4.1; each new chart key ships values + schema +
removal-only configmap render + a render-assert in the same commit.

- **E1 — Deletion-proof CSI VolumeSnapshots (`zfs.holdCsiSnapshots`).** Places
  the fixed `truenas` ZFS hold on every CSI VolumeSnapshot at create so foreign
  actors (a box-wide periodic-task prune, an admin, replication retention) hit
  EBUSY on destroy. The driver's own destroy paths (DeleteSnapshot,
  handleSnapshotClones, reapTombstoneSnapshot) release the hold first — gated on
  driver provenance — so the hold never wedges the driver's lifecycle. Hold
  failure at create is non-fatal (metered + `SnapshotHoldFailed` event). Metrics:
  `scale_csi_snapshot_holds_total{operation,status}`.
- **E2 — Driver-managed periodic snapshots (per-SC `snapshotSchedule`).** The
  driver owns ONE non-recursive `pool.snapshottask` per scheduled volume dataset
  with bounded TIME-based retention (`snapshotRetention`, default 30d safety
  bound; 26.0 has no count cap and `pool.snapshottask.run` is broken, so it is
  never called). Task-created snapshots carry no CSI props and are treated as
  driver-created ONLY through a complete chain: the snapshot sits on the
  volume's own dataset, that dataset carries this instance's local ownership
  stamp plus a naming schema the driver's own algorithm re-derives byte-for-byte
  for this volume (`csi-<volume>-<16-hex nonce>-%Y%m%d-%H%M%S`, minted by the
  driver — there is deliberately no caller-chosen `snapshotNamingSchema`), a
  driver-minted non-recursive task carrying exactly that schema is observed alive
  on exactly that dataset, the snapshot's name is a complete CANONICAL rendering
  of that schema encoding a real calendar instant, and that instant is EXACTLY
  (±2s clock skew) when the snapshot was created — its `creation` property
  rendered in the NAS's own civil timezone. That zone is proven, not guessed:
  the IANA zone in force when the TASK was created is recorded on the volume's
  own dataset (`truenas-csi:snapshot_task_timezone`, write-once, read only when
  `source == local`, so a clone/received/detached copy that inherits it proves
  nothing), and the delete path requires it to still equal the NAS's LIVE
  `system.general.config` zone. This is what makes the FACT of a timezone
  reconfiguration detectable even when the civil fields coincide
  (`America/New_York` -> `America/Toronto`, or a switch to a fixed `-05:00` for a
  winter-created snapshot). There is no driver-level cache of the live zone; the
  single cache lives on the API client, is dropped on reconnect, never caches a
  failure, and has a short (5-minute) TTL. The image embeds `time/tzdata` so
  zone resolution is identical in-container and in tests. A task is NEVER created
  when the zone cannot be read, because its snapshots could never afterwards be
  proven. Snapshots that pass are excluded from the foreign guard and deleted
  with the volume; anything unprovable stays FOREIGN and is preserved — a
  missing/inherited zone record, an unreadable live zone
  (`scale_csi_nas_timezone_unresolved_total`), a stored-vs-live mismatch, or a
  missing corroborating task all fail CLOSED rather than widening the window.
  **Documented trust boundary — stated precisely, claiming nothing stronger:**
  this chain does NOT establish "a snapshot the driver did not create cannot be
  deleted", and TrueNAS 26.0 makes that unachievable in principle: it can
  neither stamp a user property on an EXISTING snapshot nor attribute a snapshot
  to the task that made it, so authorship is unprovable for a foreign snapshot
  AND for the driver's own. (The alternative posture — treat every unprovable
  snapshot as foreign — is unusable for exactly that reason: it would wedge the
  DeleteVolume of every scheduled volume.) The predicate exists only to BLOCK
  DeleteVolume's recursive destroy when something foreign is present, so the
  residual is correspondingly narrow and bounded: **a foreign snapshot that
  matches BOTH the driver-minted per-volume nonce-bearing name AND the creation
  SECOND that name encodes will not block that destroy.** Constructing one
  requires reading the schema (available through `pool.snapshottask.query`) and
  pool-write access on the CSI parent dataset. Storage-administrator access to
  `zfs.parentDataset` is therefore trusted; everything outside that one case
  (other tasks, replication, clone-inherited properties, `csi-` prefixed
  operator snapshots, other volumes' schemas, other driver instances, any name
  that is off by a second) is provably never destroyed as driver-owned. See
  `docs/reference/storageclass.md`.
  Counted in
  `scale_csi_scheduled_snapshots` (never an orphan/delete candidate). The schema
  binding — schema AND recorded zone — is stamped BEFORE the task is created, so
  a task can never outlive its binding, and the orphan reconcile sweeps tasks
  whose dataset is gone. DeleteVolume records its observation of the live task
  (`truenas-csi:snapshot_task_corroboration`) and VERIFIES that record with a
  source-bearing re-read BEFORE destroying the task; if the record does not land
  the task is deliberately left alive, so a DeleteVolume that fails later can
  still be retried instead of wedging behind the foreign guard forever.
  Controller defaults: `zfs.snapshotSchedule` / `zfs.snapshotRetention`. Metrics:
  `scale_csi_scheduled_snapshot_tasks_ensured_total`,
  `scale_csi_scheduled_snapshot_task_ensure_failed_total`,
  `scale_csi_scheduled_snapshot_task_delete_failed_total`,
  `scale_csi_stranded_snapshot_tasks_reaped_total`,
  `scale_csi_nas_timezone_unresolved_total`.
- **E3 — Lazy clone independence (`zfs.promoteRestoredClones`).** A background
  reconcile step promotes a clone-restored volume (`pool.dataset.promote`) to
  drop its origin-snapshot pin, letting the tombstone reaper reclaim the source
  snapshot and the source volume become destroyable. Promote is atomic but it
  MOVES the origin and every older-or-equal snapshot and re-parents siblings AND
  the source, so every gate is re-proven under the clone's and source volume's
  operation locks immediately before the call: a fresh source-bearing dataset
  read (the reconcile listing carries no property source), the AUTHORITATIVE
  per-snapshot dependent-clone query (which sees unmanaged clones the managed
  listing never contained), a refusal if any OTHER live CSI VolumeSnapshot — or
  any non-CSI snapshot at all — would migrate, and a RE-KEY of every migrating
  tombstone's ledger entry to its post-promote ID before the promote so
  provenance follows the snapshot. The migration set is computed from ALL three
  buckets of the pass's snapshot partition (CSI snapshots, tombstones AND
  unowned), because ZFS migrates by `createtxg` and not by ownership; the only
  class allowed to migrate is a tombstone, whose provenance is explicitly
  re-keyed. That set is also only as trustworthy as the listing it comes from,
  and the snapshot query returns a bare slice with no total, page token or
  completeness marker — a truncated result is indistinguishable from a complete
  one, so "no error" is NOT completeness. Completeness is therefore established
  POSITIVELY, by corroborating the pass's recursive parent walk against a second
  authoritative dataset-scoped inventory taken under the lock; the two must
  agree exactly on membership, and any disagreement, any unobtainable inventory,
  or any missing `createtxg` REFUSES the promote rather than reasoning from a
  set that might be short. Metrics:
  `scale_csi_clones_promoted_total{status}`,
  `scale_csi_clone_promotes_refused_total{reason}`.
- **E4 — Quota/usage reporting (`zfs.reportVolumeUsage`).** ControllerGetVolume
  fetches each volume's quota/usage (one `pool.dataset.query`) and reports a
  near-quota VolumeCondition (abnormal above 95% of the effective limit). Each
  limit is compared against the quantity ZFS actually bounds with it —
  `refquota` against `referenced`, `quota` against `used` (which includes
  snapshot-held space), taking the tighter when both are set — so a volume whose
  snapshots hold gigabytes is not misreported near-quota while its writable
  space is free; the condition message names the binding property and calls out
  snapshot-held space. Block volumes (zvols) are covered through
  `volsize`/`referenced` — a backend-occupancy figure, not guest-filesystem
  fullness. The per-volume gauges are ALSO published from the reconcile pass's
  existing dataset walk (zero extra API calls), because the shipped
  external-health-monitor sidecar drives ListVolumes rather than per-PV
  ControllerGetVolume — without that the `ScaleCSIVolumeNearQuota` alert would
  essentially never fire. Series are dropped at DeleteVolume and re-derived each
  pass, so a gauge cannot latch. Metrics: `scale_csi_volume_used_bytes{volume}`,
  `scale_csi_volume_quota_bytes{volume}`, `scale_csi_volume_near_quota{volume}`,
  and the `ScaleCSIVolumeNearQuota` alert.

**Final-verification hardening (v1.5.0 round).** The scheduled-snapshot
provenance chain is now diagnosable: a snapshot that carries the driver's
scheduled-name shape but fails the name/creation-skew/zone proof increments
`scale_csi_scheduled_snapshot_unproven_total{reason}` with a distinct warning,
and the DeleteVolume refusal reports how many such snapshots exist instead of
blaming a foreign task. `snapshotSchedule` cron fields are content-validated at
CreateVolume (`InvalidArgument` on a malformed field, as always documented —
previously only the field count was checked, so a typo'd schedule silently
provisioned without PITR). Every GF2 degradation counter now has a gated
PrometheusRule alert (task-ensure/task-delete failures, hold failures, timezone
resolution failures, promote refusals) so a silently absent protection pages
someone. The near-quota VolumeCondition upgrades an existing abnormal condition
instead of replacing it.

**Lifecycle safety notes.** Snapshot-hold release is fail-safe against
configuration history: any driver destroy refused with `EBUSY` ("has the
following holds") releases the hold UNCONDITIONALLY and retries once — including
the recursive `DeleteVolume` destroy, which first releases the holds on
driver-proven snapshots beneath the dataset — so turning `zfs.holdCsiSnapshots`
back off never wedges an already-held snapshot, and the default path still makes
zero extra calls. `docs/production.md` carries the mass-release rollback runbook.
Counter: `scale_csi_snapshot_hold_recoveries_total`.

## v1.4.0 — CHAP, capacity, volume health, clone latency, observability taxonomy

Five sprints of feature and hardening work. No existing config key, volume, or
on-disk property changes meaning; every new surface is opt-in or gated and the
default Helm render stays byte-identical to v1.3.0. `Chart.yaml` carries a
`0.0.0-dev` placeholder that CI stamps with the release tag.

Two additional fixes were found and live-verified during the pre-release drill
against a real TrueNAS 26.0 system:

- **Fresh-install iSCSI provisioning on TrueNAS 26.0 (fencing off).** The
  allow-all initiator-group path matched only `null` initiator lists, but 26.0
  returns `[]` for allow-all groups and rejects the `null` create shape — so a
  fresh install with no pre-existing group could never provision iSCSI volumes.
  Reuse now accepts empty non-fencing groups and creation always sends a list.
- **`zfs.resource.query` managed-dataset listing restored.** 26.0 returns
  native-property `source` as an object, which the v1.3.0 typed decoder
  rejected — silently degrading every reconcile listing to the slower
  `pool.dataset.query` fallback since v1.3.0. The decoder now tolerates both
  shapes (matching pre-v1.3.0 semantics), restoring the faster listing path.

### Headline features

- **iSCSI CHAP session authentication (end to end).** Opt-in, per-StorageClass
  one-way or mutual CHAP with tag-keyed `iscsi.auth` peers, immutable per-volume
  auth mode/tag, and in-place credential rotation. Credentials never reach the PV
  volume context and are redacted from logs, errors, and Events. Discovery
  authentication and dm-multipath remain unsupported, and CHAP authenticates but
  does **not** encrypt TCP 3260 — network segmentation is still required. Details
  in Sprint 2 below.
- **CSIStorageCapacity tracking (opt-in, `capacity.enabled`).** Advertises
  `CSIDriver.spec.storageCapacity=true` and runs the external-provisioner
  capacity controller. See the WFFC scheduler prerequisite in Upgrade notes.
- **Volume health monitoring (opt-in, `sidecars.healthMonitor.enabled`).** The
  v0.18.0 external-health-monitor controller sidecar emits PVC Events from the
  controller-side `VolumeCondition`. Node-side volume health depends on a
  separate Kubernetes alpha path (caveat below).
- **Clone provisioning latency.** New per-protocol golden round-trip counts and a
  removed standard-clone readiness poll (Sprint 3).
- **`truenas.maxConnections`** — WebSocket connection-pool sizing (Sprint 1).
- **Observability taxonomy.** The `scale_csi_truenas_requests_total{status}`
  label migrates to a 5-value outcome taxonomy, plus five new metric families,
  ten new alerts, and five new Event reasons (Sprint 5).

### Breaking changes

**None expected in v1.4.0.** Every new surface is backward-compatible. The
`CreateVolume` explicit-protocol requirement and the removed multi-portal setting
shipped in v1.3.0 (see the Breaking-change section far below) and are unchanged
here. Three never-wired keys are now deprecated-but-accepted — the chart no
longer renders them but `values.schema.json` still validates them so existing
values files pass: `resilience.rateLimiting.maxConcurrentRequests`,
`iscsi.extentAvailThreshold`, and `nvmeof.commandTimeout`.
`nfs.shareCommentTemplate` and `nvmeof.nameTemplate` are likewise
accepted-and-ignored compatibility keys.

### Upgrade and rollback notes

- **Rollback is safe (verified).** The cluster-shaped v1.4.0 ConfigMap emits
  **zero** keys unknown to the v1.3.0 strict (`KnownFields(true)`) config parser:
  every new config surface (`truenas.maxConnections`, `iscsi.chap.*`,
  `capacity.*`) is gated and renders nothing when off/default. The two new CHAP
  dataset properties (`truenas-csi:truenas_iscsi_auth_tag`/`_mode`) and the gated
  `chap` volume-context key are ignored by v1.3.0; marker/ledger formats are
  unchanged. A v1.4.0 → v1.3.0 downgrade against the deployed state is verified
  safe.
- **Capacity scheduler prerequisite (WaitForFirstConsumer).** external-provisioner
  publishes `CSIStorageCapacity` **only** for `WaitForFirstConsumer`
  StorageClasses and the scheduler consults capacity only for WFFC binding.
  Enabling `capacity.enabled` against the `Immediate` bundled class starts the
  controller but creates no capacity objects. `capacity.forImmediateBinding: true`
  publishes for Immediate classes for non-scheduler consumers only.
- **Health-monitor caveats.** Because this driver advertises `LIST_VOLUMES`, the
  sidecar uses one periodic `ListVolumes` path rather than per-PV
  `ControllerGetVolume`; that path reports backend provisioning-metadata health,
  not node stale mounts or the data path. Node-side `VolumeCondition` delivery
  depends on Kubernetes/kubelet's separate **alpha** volume-health feature gates;
  enabling this controller sidecar alone does not provide it.
- **CSIDriver.storageCapacity flip.** `spec.storageCapacity` is mutable on
  Kubernetes 1.23+, so a `helm upgrade` that flips `capacity.enabled` succeeds in
  place; on 1.20–1.22 (the chart's `kubeVersion` floor) the field is immutable.
  Flipping capacity **off** leaves owner-referenced `CSIStorageCapacity` objects
  behind (no finalizer) until the controller Deployment is deleted; remove them
  manually if undesired.
- All v1.3.0 upgrade contracts still apply: the fencing node-first migration, the
  singleton controller for additive/strict, and rolling the ConfigMap and image
  together.

### Sprint 1 — configuration truth and connection-pool sizing

- **`truenas.maxConnections`** sizes the controller's WebSocket connection pool.
  The **chart** default is `null`/omitted (the key is absent from the ConfigMap
  for rollback compatibility); the **driver** then applies its own default of
  **5**. Accepted explicit range is **1–16**; an explicit `0` or an out-of-range
  value fails validation (explicit zero is preserved and rejected, not treated as
  omitted). Node pods build no client and ignore it.
- **Deprecated-but-accepted keys.**
  `resilience.rateLimiting.maxConcurrentRequests` (use
  `truenas.maxConcurrentRequests`), `iscsi.extentAvailThreshold`,
  `nvmeof.commandTimeout` (use `commandTimeouts.nvme`), `nfs.shareCommentTemplate`,
  and `nvmeof.nameTemplate` are no longer rendered by the chart and are ignored by
  the driver; the schema still accepts them so old values files validate.

### Sprint 2 — iSCSI CHAP

- **Opt-in, per StorageClass.** Two conditions gate CHAP for a class: the
  controller is opted in (`iscsi.chap.enabled: true`), and the StorageClass
  references a Kubernetes Secret through the standard CSI provisioner- and
  node-stage secret-ref parameters (the chart renders all four from
  `storageClasses[].chapSecretName`/`chapSecretNamespace`, the latter defaulting
  to the release namespace). With `iscsi.chap.enabled: false` (default) no peers
  are managed and targets stay `authmethod=NONE`.
- **Effective per-class opt-in is the non-blank Secret username.** For
  chart-generated classes, CHAP engages at `CreateVolume` only when the global
  gate is on **and** the provisioner Secret carries a non-blank username (the
  driver's internal `iscsi.chapSecret` marker; the chart does not emit
  `iscsi.chapSecret=true`). A referenced Secret with an absent/blank username
  currently **fails open** to `authmethod=NONE`; only a request that has already
  selected CHAP fails closed on malformed CHAP fields.
- **Secret schema and validation.** Keys: `username`; `password` (12–16 **bytes**,
  Go `len`); optional `mutualUsername` (its presence selects `CHAP_MUTUAL`);
  `mutualPassword` (required iff `mutualUsername` set, 12–16 bytes, **must differ**
  from `password`); optional `tag` (must be a positive integer). Legacy open-iscsi
  aliases are accepted (`node.session.auth.username`/`password`/`username_in`/`password_in`).
  Validation also rejects leading/trailing whitespace and `#`. All such failures
  occur before any TrueNAS call, returning `InvalidArgument`, once CHAP has been
  selected.
- **Tag precedence, collision, and sharing.** Peer identity is tag-based and the
  tag is derived from the username (no StorageClass identity participates):
  positive Secret `tag`; else positive global `iscsi.chap.tag`; else FNV-1a of the
  username mapped into `[1000,61000)`. Two classes with the same username and no
  explicit tag **share** one derived tag/peer; a positive global tag makes every
  untagged class contend for one peer; two different usernames that resolve to the
  same explicit/global tag fail with `FailedPrecondition`. Pin distinct positive
  Secret tags for isolation.
- **Immutable per-volume policy.** The auth *mode* and *tag* are stamped immutably
  at `CreateVolume` as local dataset properties
  (`truenas-csi:truenas_iscsi_auth_tag`/`_mode`); every later fence/rebuild reads the
  stored value — never the controller-wide `iscsi.chap.mutual` flag. A replay that
  would change the policy is rejected with `FailedPrecondition`; only the secret
  *value* rotates. Because the shared peer is ensured before the per-volume guard,
  a *rejected* policy-change replay can still re-key a shared peer (with an
  `ISCSICHAPRotated` Event alongside the `FailedPrecondition`) or leave one bounded
  unused peer per username change; existing volumes and groups are unaffected.
- **`iscsi.chap.mutual` is presently inert.** Production code never reads it; the
  effective mode is selected solely by the Secret's `mutualUsername`. It is an
  ignored compatibility hint until implemented.
- **Rotation and enforcement.** Update the Secret's `password`/`mutualPassword`
  (keep the same username and tag); the next `CreateVolume` re-keys the backend
  peer in place via `iscsi.auth.update` (no controller restart) and emits a
  redacted `ISCSICHAPRotated` Event. Established sessions survive rotation — the
  node applies the new credential only before a **fresh login** (including after a
  stale-session disconnect), not merely on the next `NodeStageVolume`. Immediate
  enforcement requires coordinated unstage/logout or node drain plus verification
  that the old session is gone; a controller restart does not force reauth.
- **Peer persistence and DR prerequisite.** One `iscsi.auth` peer per class
  credential is created and reused; `DeleteVolume` does not delete it. The peer
  lives in the TrueNAS configuration database and does **not** ZFS-replicate — CHAP
  DR additionally requires the destination TrueNAS to already contain the same
  tag/username/mode/credential before publish (see the DR guide).
- **Host-trust exposure.** Credentials are briefly visible in the node host
  process table (`iscsiadm -v <value>`) and persist in open-iscsi node state under
  `/var/lib/iscsi`; the privileged node DaemonSet makes node root equivalent to
  holding every CHAP secret staged on that node. CHAP protects against off-host
  initiators, not a compromised node.
- **Fencing composition.** CHAP is an independent session-authentication layer:
  the immutable authmethod/tag stays on the target group while `additive`/`strict`
  fencing changes the initiator allowlist. CHAP neither disables fencing nor
  implies allow-all.
- **Events:** `ISCSICHAPRotated` (redacted rotation) and `ISCSICHAPFailed`.

### Sprint 3 — clone provisioning latency

- **Per-protocol golden round-trip counts (single-get fold).** The tested NFS
  snapshot-clone golden is now **10** TrueNAS round trips and the NFS
  volume-to-volume clone golden is **13**; the previous protocol-independent "12"
  predates the single-get response-verified fold. Golden counts are scoped by
  protocol and source type. **Honest note:** these are the values pinned by the
  in-repo golden fixtures; a live end-to-end re-measure against a real appliance
  is pending post-deploy.
- **Standard clone verification** now performs one `DatasetGet` and at most one
  fixed retry after 250 ms — it no longer calls `WaitForZvolReady`. Exhaustion
  maps to `Unavailable` so the sidecar retries; `Canceled`/`DeadlineExceeded` are
  preserved. Increasing `zfs.zvolReadyTimeout` does **not** extend this standard
  clone retry. The distinct detached snapshot-copy zvol-readiness path still uses
  `WaitForZvolReady` and maps readiness failure to `Internal`.

### Sprint 4 — capacity-aware scheduling and volume health

- **`capacity.*` keys (all opt-in, default off):** `capacity.enabled` (advertise
  `storageCapacity=true` + capacity controller + RBAC),
  `capacity.forImmediateBinding` (publish for Immediate classes; non-scheduler
  consumers only), `capacity.reportMaximumVolumeSize` (sets `maximum_volume_size`
  to the parent's available bytes — appropriate **only** for thick/reserved zvol
  deployments, not thin overcommit), `capacity.gaugeEnabled`, and
  `capacity.gaugeInterval` (default 60s; values below 30s clamp to 30s).
- **`GetCapacity`** issues exactly one `pool.dataset.query` against the parent per
  referencing class. The gauge loop samples immediately then every interval and
  performs **one parent dataset query per interval per controller replica** (no
  leader-election gate) — the supported topology is `replicas=1`. Neither is on
  the CreateVolume/publish/unpublish golden path.
- **Disable/rollback cleanup.** Flipping capacity off can leave owner-referenced
  `CSIStorageCapacity` objects until the controller Deployment is deleted or they
  are removed manually.
- **Pool metrics:** `scale_csi_pool_available_bytes` and
  `scale_csi_pool_capacity_bytes` (present only with `capacity.gaugeEnabled`).
- **`metrics.prometheusRule.poolUsageThreshold`** (default **0.85** used-fraction,
  schema-ranged) drives the `ScaleCSIPoolNearFull` alert, which renders only when
  the bundled PrometheusRule **and** `capacity.gaugeEnabled` are both enabled.
- **Controller `VolumeCondition`.** The same declarative condition is derived for
  `ControllerGetVolume` and `ListVolumes`: an explicit local
  `provision_success=false` is abnormal, a managed successfully-provisioned volume
  is normal, and missing legacy stamps report normal-but-"unverified." It is
  backend provisioning-metadata health — **not** existence-only and **not** a
  protocol/data-path probe. `NodeGetVolumeStats` separately detects stale-mount
  state before its stats gate. For this driver the external health monitor
  observes the controller/`ListVolumes` condition.
- **`sidecars.healthMonitor.*` keys:** `enabled` (default off), `image` (pinned
  v0.18.0), `interval` (renders both `--list-volumes-interval`, the active cadence
  here, and the fallback `--monitor-interval`), and `resources`. The sidecar emits
  PVC Events from controller-side conditions; node-side health needs the separate
  Kubernetes alpha path.

### Sprint 5 — observability taxonomy and new signals

- **Honest TrueNAS transport counter (`scale_csi_truenas_requests_total`).** The
  `status` label keeps its name but expands from `{success, error}` to a 5-value
  outcome taxonomy. Expected idempotent outcomes and lock-contention retries move
  OUT of `status="error"` into dedicated `benign_*` values, so the per-method
  transport counter now tells the same truth as the RPC-level
  `scale_csi_operations_total` (which has classified Aborted/NotFound/AlreadyExists
  as benign since v1.2.13). This fixes the live finding where
  `nvmet.host_subsys.create` showed a 13%+ `status="error"` rate that was entirely
  benign `AlreadyExists` from the unconditional enforcement-boundary create at
  publish. The classifier is method-agnostic, so the `iscsi.auth.*` peer-CRUD
  calls added for CHAP are classified `benign_exists` on their idempotent creates
  automatically.

  | series | change |
  |--------|--------|
  | `scale_csi_truenas_requests_total{status="success"}` | **unchanged** |
  | `scale_csi_truenas_requests_total{status="error"}` | **narrows** — benign EEXIST/ENOENT/lock-contention no longer counted here (intended); real errors only |
  | `scale_csi_truenas_requests_total{status="benign_exists"}` | **new** — expected idempotent AlreadyExists |
  | `scale_csi_truenas_requests_total{status="benign_notfound"}` | **new** — expected idempotent NotFound (deletes/reads) |
  | `scale_csi_truenas_requests_total{status="benign_aborted"}` | **new** — lock-contention retry / busy |
  | `scale_csi_truenas_requests_duration_seconds` | **unchanged** (no status label) |
  | cardinality | 2 → ≤5 values per method; `method` is a fixed API-method enum → still bounded |

  **Operator action — this is a semantic change, not just a rename.** Existing
  `status="success"` and `status="error"` selectors remain SYNTACTICALLY valid
  (no expression breaks), but their MEANING changes: `status="error"` now counts
  real failures only, so any panel/rule — including THIRD-PARTY dashboards — that
  summed `status="error"` as "all non-success" will read LOWER by exactly the
  benign volume that moved out. The built-in `ScaleCSIHighTrueNASAPIFailureRate`
  alert is affected the same way: it still selects `status="error"`, so EBUSY
  lock-contention abort-storms NO LONGER contribute to it (that signal is now
  covered by the new `ScaleCSISustainedLockContention` alert). Decide
  deliberately which population each of your queries should track:

  - **Real failures only** — keep `status="error"` (the new, honest value; no
    change needed).
  - **The old all-non-success population** — change the selector to
    `status!="success"` (or enumerate
    `status=~"error|benign_exists|benign_notfound|benign_aborted"`). This is the
    change a third-party panel/rule that intentionally tracked every non-success
    outcome MUST make to preserve its prior meaning.
  - **Contention signaling** — rely on the new `ScaleCSISustainedLockContention`
    alert / the "TrueNAS API Outcomes" dashboard band, or deliberately include
    `benign_aborted` alongside `error` in your own query.

- **Five new metric families.**
  - `scale_csi_job_dispatcher_subscribed` — `1` while the `core.get_jobs`
    subscription is live, `0` in the pure-poll fallback; drives
    `ScaleCSIJobDispatcherUnsubscribed`. Action: investigate a persistently `0`
    value (dead/reconnecting socket).
  - `scale_csi_manual_recovery_tombstones` — tombstones no belt can prove, for
    operator inspection; populated **only while scan fallback is enabled**.
  - `scale_csi_tombstone_reaped_total{path}` — reaper throughput by discovery path
    (ledger vs scan fallback).
  - `scale_csi_pool_available_bytes` and `scale_csi_pool_capacity_bytes` — opt-in,
    present only with `capacity.gaugeEnabled`; feed `ScaleCSIPoolNearFull`.

  The existing documented metric names continue to match `driver.MetricNames()`.

- **Ten new alerts** relative to `main` (the bundled PrometheusRule now renders
  **19** alerts total): `ScaleCSISustainedLockContention`,
  `ScaleCSIFencingTakeoverSpike`, `ScaleCSIFencingProvenanceOverflow`,
  `ScaleCSIJobDispatcherUnsubscribed`, `ScaleCSIDeleteResidualCleanupFailing`,
  `ScaleCSIManualRecoveryTombstones`, `ScaleCSIRemnantVolumesDetected`,
  `ScaleCSITombstoneBacklog`, `ScaleCSIReconcileStalled`, and
  `ScaleCSIPoolNearFull`. `ScaleCSIHighTrueNASAPIFailureRate` still selects
  `status="error"`, so EBUSY lock-contention storms are now covered by
  `ScaleCSISustainedLockContention` instead. Nine of the rendered alerts also
  carry a resolvable `runbook_url` annotation; see the troubleshooting
  Alerts → Runbook table for the full mapping.

- **Five new Event reasons:** `ISCSICHAPRotated`, `ReaperRefused`,
  `ReconcileGuardRefusal`, `FencingProvenanceOverflow`, and `ISCSICHAPFailed`.

### Post-requested-snapshot delta (49-commit head, `360e268`)

Three code-only commits landed after the audit snapshot: two harden typed JSON
decoding to exact legacy-decode equivalence (differential-fuzz findings), and one
folds the CHAP policy mode/tag for snapshot and volume clones into the **same
atomic ownership/content-source update**. That fold closes a crash window that
could otherwise leave an owned clone permanently rejected on retry — a correctness
fix recorded here for the nominated 49-commit head.

## v1.3.0 — publish/reconcile performance, subscribe job-wait, clone fold, ledger v2

Batches 17–20, plus an adversarial-review maintainability round.

### Performance

- **Publish/unpublish path resolution threading.** Intra-request re-resolution
  on the publish/unpublish path was eliminated and the round-trip cost is now
  pinned by golden tests. The strict-mode NVMe-oF steady-state republish is
  **9 TrueNAS round trips** (down from ~13); records-only `off`+NFS is 3 and
  `additive`+NFS is 5.
- **Reconcile N+1 elimination.** A reconcile pass now fetches the parent
  snapshot/dataset sets once and partitions them in memory (instead of a
  per-page/per-entry re-transfer), caches bookkeeping-dataset existence behind an
  atomic flag, and batches post-destroy tombstone-ledger removals to the end of
  the pass. This removes the previous O(N²) wire amplification. (The exact
  per-pass round-trip count varies with object, protocol, candidate, and failure
  counts and is not pinned by a golden fixture.)
- **Hybrid `core.subscribe` job-wait + typed decode.** List-heavy and
  job-bearing paths wait on `core.subscribe` job completion events with a polling
  fallback, and decode responses through typed JSON decoders instead of
  `map[string]interface{}` reflection.
- **Response-verified quota + content-source fold.** `CreateVolume` from a
  snapshot trusts the mutation response to verify quota and content-source
  instead of re-reading, folding the clone hot path. After the v1.4.0 single-get
  fold the current per-protocol goldens are **10** round trips for an NFS
  snapshot source and **13** for an NFS volume source (see v1.4.0 Sprint 3); the
  earlier protocol-independent "12" is superseded.
- **Attacher/resizer timeout.** The external-attacher and external-resizer
  sidecar `--timeout` is **120s** (was 300s), so a stuck TrueNAS publish or
  expand cannot pin the sidecar for five minutes. Provisioner and snapshotter
  remain 300s.

### Resilience and data safety

- **Scan-fallback tombstone reaper.** New opt-in
  `reconcile.tombstoneReaper.scanFallback.enabled` (default **off**). It runs on
  **every** pass, independent of the strict ledger backlog, and issues no
  separate query — it reuses the pass's already-fetched recursive, unpaginated
  snapshot set and processes at most 500 accepted candidates. A candidate is
  reaped only when it has **no** ledger property at either bookkeeping location
  and carries retained creation-time identity that exactly reproduces the
  driver's nonce-derived tombstone rename (retained snapshot/instance identity,
  exact tombstone name, local source-instance ownership, age gate, and the
  inheritance-mask guard). It never widens what counts as this driver's object.
  `manualRecoveryTombstones` inventory is populated **only while scan fallback is
  enabled**.
- **Tombstone ledger v2 (`CreateTXG`).** The ledger property key remains a hash
  of the tombstone snapshot ID; v2 additionally stores the snapshot's ZFS
  `CreateTXG` in the entry as an immutable identity predicate (so a
  delete/recreate at the same name cannot be confused), degrading to the v1
  full-ID + creation-seconds check when TXG is unavailable. v1 entries remain
  readable.
- **Clone-property scrub.** After stamping a clone, source-proven
  protocol-foreign inherited backreference properties are scrubbed so a clone
  never carries another protocol's stale share IDs.
- **Dangling staging-symlink self-heal.** A dangling node staging symlink is
  repaired in place instead of wedging the stage.
- **`IsNotFoundError` tightened.** Not-found detection matches the error
  `Message` (26.0 errno shape) rather than the full error string, avoiding
  false positives.

### Maintainability

- The monolithic `reconcile.go` was split along its test seams into per-concern
  files (`reconcile_kubestate.go`, `reconcile_publications.go`,
  `reconcile_shares.go`, `reconcile_tombstones.go`, `reconcile_remnants.go`,
  `reconcile_spent_restore.go`, `reconcile_adoption.go`) — pure moves, no
  behavior change. `LoadConfig` was split into
  `applyConfigDefaults`/`validateConfig`/sniffing helpers, and a shared dual-read
  bookkeeping helper was extracted. No configuration keys changed in this round.

No new chart keys are introduced by the maintainability round;
`reconcile.tombstoneReaper.scanFallback` is the one new configuration surface
(exposed in `values.yaml`, `values.schema.json`, and the ConfigMap).

## v1.2.35 — dual-read reap provenance + honest counters

From the 2026-07-24 live GC run:

- **`cleanupParent`-safe dual-read reap provenance.** The tombstone reaper proves
  provenance by reading *both* bookkeeping locations, so a reap stays correct
  mid-migration even after `cleanupParent` has removed the parent-side copy.
- **In-flight `VolumeSnapshotContent` tolerance.** A snapshot whose
  `VolumeSnapshotContent` is still being created is no longer misclassified.
- **Honest summary counters.** The reconcile summary line reports what was
  actually acted on (including `manualRecoveryTombstones`), not optimistic
  totals.

## Batch 16 (v1.2.34) — legacy stamp adoption

One fix from the 2026-07-23 04:00Z live GC run, in which the first age-eligible
tombstones were REFUSED by the reaper with "tombstone source dataset does not
carry this driver instance's ownership stamp". The migration-era volumes
(created before v1.2.21 introduced `truenas-csi:driver_instance_id` stamping)
carry LOCAL `managed_resource` + `csi_volume_name` but NO instance stamp, so the
reaper's instance belt refused their tombstones forever: ledger entries and
`-csi-deleted-` snapshots accumulated indefinitely (+16/h from hourly VolSync).
The stamps are adopted, not the reaper weakened.

- **Data safety — guarded stamp-adoption pass.** A new reconcile step runs in
  every pass (before tombstone sweeping, so a freshly adopted source unblocks
  reaping in the SAME pass) and stamps `driver_instance_id` plus a distinct
  `driver_instance_id_adopted` marker onto a legacy managed dataset only when
  ALL hold: it sits strictly under the CSI parent and is a valid volume leaf
  (not the `.csi-bookkeeping` dataset); a source-bearing re-read (the batch-12
  `DatasetGet` pattern, never the sourceless listing) proves a LOCAL
  `managed_resource=true` AND a LOCAL `csi_volume_name` matching the dataset
  leaf; the dataset carries NO existing `driver_instance_id` of ANY source; and
  a live **Bound** PersistentVolume of THIS driver references it (live clientset
  list, not informer caches). The write goes through the proven `stampAndMirror`
  user-property path and is verified by a source-bearing re-read before it
  counts as adopted. The adoption marker preserves cleanup ownership while
  excluding the dataset from the create-time iSCSI data-free proof. Adoptions
  are capped at `maxPerRun` per pass as a blast-radius bound and reported in
  `ReconcileReport.AdoptedStamps` (+ count in the detection-complete summary line
  and one klog line per adoption).
  - **Absolute rule:** an existing `driver_instance_id` — local, inherited, or
    foreign — is NEVER overwritten. A dataset stamped by another driver instance
    sharing a pool is left untouched (never hijacked). The absence is re-proved
    under the per-volume lock immediately before the write.
  - **Why a write runs in detection mode:** the step adds cleanup ownership to
    datasets that are provably this cluster's Bound volumes; it deletes nothing;
    and it is required for the delete-mode reaper to act on legacy tombstones.
    It is not gated by `delete.enabled`.
  - **Fail-safe:** a PV-list error, or an empty PV list for the whole driver,
    adopts NOTHING that pass (an API discontinuity is not evidence).
  - **Residual:** a legacy dataset that is NOT currently Bound is not adopted,
    so its tombstones stay refused (fail-safe); operators can bind it or clean it
    up manually. A driver-owned volume whose eleven witness properties are
    stripped can still pass the default data-free proof without an error; the
    fatal create-time fold added on this branch prevents new provisioning from
    entering that shape, but released volumes with a warning-only write failure
    may already have it.
- **Hygiene — comment typo.** The spent-restore classification comment referenced
  `deleteDetachedOrphans`; it now correctly reads `deleteDetectedOrphans`.

No new configuration keys are introduced, so no chart changes are required.

## Batch 15 (v1.2.33) — remnant-orphan GC + bookkeeping hardening

Three changes from the 2026-07-23 live incident, in which a controller OOM crash
loop manufactured unstamped clone datasets in the window between `zfs clone` and
the ownership stamp. Each was invisible to the orphan-volume classifier (no stamp
= no ownership proof), deliberately kept by the in-flight-marker sweep (the marker
was the only thing a retry could recover from), and never reclaimed because the
assumed same-name `CreateVolume` retry never comes under VolSync (it mints a new
PVC UID on failure). Each empty clone pinned a `-csi-deleted-` tombstone origin
and blocked ledger drain; cleanup was manual. This batch automates it safely.

- **Data safety — marker-based remnant-orphan GC.** A new reconcile phase
  detects a *remnant orphan*: a dataset that carries a valid local in-flight
  marker for this driver instance (parent or bookkeeping child, dual-read), is
  older than `minOrphanAge` (no new knob), still EXISTS and is UNSTAMPED (no
  local driver-instance/`managed_resource` ownership), and is referenced by no
  Kubernetes object (live PV/VolumeAttachment hard-recheck, not informer caches).
  Detection is always on; guarded destruction runs only under `delete.enabled`
  and counts against the shared `maxPerRun` cap. Immediately before the
  non-recursive, `force=false` destroy the phase re-fetches the marker (identical
  nonce) and the dataset (still unstamped), re-proves the ZFS origin binding
  (clone origin must equal the marker's recorded origin; a detached copy must
  have none), and re-checks Kubernetes absence — any change skips with an
  operator-visible reason. Children or snapshots under the remnant fail the
  delete (fail-safe). On success the marker is retired from both bookkeeping
  locations and a Warning event is recorded. A stamped dataset is left to the
  existing stale-marker sweep and orphan-volume pass, unchanged.
- **Security — inbound IDs can no longer target the bookkeeping dataset.**
  `datasetForID` now rejects any volume/snapshot ID equal to the bookkeeping
  child dataset's leaf (`.csi-bookkeeping`) with `InvalidArgument` before any
  TrueNAS access, so a crafted `volumeHandle` can never delete/expand/clone the
  driver's bookkeeping dataset. Guarded across every RPC entry class that
  resolves an inbound ID.
- **Docs — bookkeeping downgrade warning.** `docs/production.md` and the chart
  `values.yaml` now warn that once `reconcile.bookkeeping.enabled` has been true
  and entries live on the child, disabling it orphans child-side entries from
  reads; the `cleanupParent` flow is the supported path.

## v1.2.30–v1.2.32 — bookkeeping migration chunking

The bookkeeping-dataset migration copies parent entries to the
`<parent>/.csi-bookkeeping` child in batches bounded well under TrueNAS's 64 kB
WebSocket inbound limit (close 1009), and skips entries already present on the
child so a re-run is idempotent. This made the batch-14 relocation safe on real
appliances with many entries.

## Batch 14 (v1.2.29) — adversarial-verification fixes

Six fixes from the 2026-07-22 dual-reviewer adversarial verification. All are
behavior-preserving outside the scoped defects; every fix ships with regression
tests that fail on v1.2.28 and pass after.

- **Resilience — connection-loss errors now retry correctly.** Pre-send and
  pre-authentication connection losses ("connection lost before request was
  sent" / "connection lost during authentication") previously escaped the retry
  classifier and were recorded as circuit-breaker *successes*, so a flapping
  TrueNAS backend could fail to open the circuit and surface spurious hard
  failures. These errors now wrap the transport-failure sentinel: the call loop
  retries them and records a breaker failure.
- **Availability — service-reload debouncer no longer starves.** The reload
  debouncer used pure trailing-edge batching: a sustained request stream faster
  than the window (e.g. an attach storm) reset the timer on every request and
  postponed the iSCSI reload — and every caller blocked on it — indefinitely. It
  now uses leading-window batching: the first request of a batch arms the timer
  and later requests coalesce onto the same deadline, bounding worst-case reload
  latency to one window.
- **Correctness — orphan classifier ignores inherited `managed_resource`.** A
  user dataset nested under a live CSI volume inherits `managed_resource=true`
  and was misclassified as a CSI orphan (phantom report/metric entries and, under
  delete mode, a burned `maxPerRun` slot every pass). The classifier and
  revalidator now re-fetch candidates with property source and require a *local*
  `managed_resource` stamp, matching the codebase's existing source discipline.
- **Hygiene — orphan-share sweeps match canonical teardown.** The iSCSI sweep now
  also deletes the per-volume fencing initiator group, and the NVMe-oF sweep
  deletes port-subsystem associations before the subsystem, so sweeps no longer
  leak one initiator group per swept volume or fail forever on a dangling
  association.
- **Data safety — spent-restore reaper defers incomplete restores.** A source PVC
  that *exists* in Pending, Lost, or an unknown phase no longer counts as spent;
  only a Bound PVC (restore completed) or an absent PVC (restore torn down) may
  classify. Deferred snapshots log a line and record an operator-visible skip
  reason. A Released PVC still classifies as spent (its PV was let go), so
  existing VolSync teardown behavior is unchanged.
- **Efficiency — snapshot query amplification (TrueNAS 26.0).** The reconcile
  pass previously re-transferred the entire parent snapshot set once per 100-item
  page (O(N²) wire volume — >1 GB/hour measured at 16 volumes), and the tombstone
  sweep re-fetched that payload per ledger entry. The pass now fetches the
  snapshot set once and partitions in memory, and the sweep resolves tombstone
  existence from that in-pass listing. As a further (gated) step, the driver's
  bookkeeping (tombstone ledger and in-flight markers) can be relocated off the
  inheritable parent dataset onto a dedicated child dataset so its properties no
  longer bloat every descendant snapshot — see the configuration note below.

### Bookkeeping-dataset relocation (Fix 4b) is opt-in

The bookkeeping relocation is **disabled by default** because it touches
data-safety bookkeeping (crash-recovery provenance). Enable it with:

```yaml
reconcile:
  bookkeeping:
    enabled: true        # write new bookkeeping to <parent>/.csi-bookkeeping; read both locations
    cleanupParent: false # set true only after rollout to remove migrated entries from the parent
```

With `enabled: true`, new bookkeeping is written to a dedicated
`<parent>/.csi-bookkeeping` child dataset and reads consult both it and the
parent (lossless dual-read). The migration copies parent entries to the child;
those copies are removed from the parent only when `cleanupParent: true`, and
only after a confirmed copy. Until `cleanupParent` is enabled the migration is
strictly additive, so a mixed-version rollout (an older controller still reading
the parent) keeps working.

## Batch 13 (v1.2.28) — maintainability refactors

Behavior-preserving refactors: the provenance/recovery machinery was extracted
into `provenance.go`; dataset property write+mirror was unified into
`stampAndMirror`; a `ShareBackend` interface with a `backendForShareType`
selector replaced scattered per-protocol switches; and spent-restore
VolumeSnapshot classification was gated behind `reconcile.spentRestore.enabled`
(default true).

## Batch 12 (v1.2.27) — TrueNAS API performance + 25.04 floor

- Managed-dataset listing migrated to a path-scoped `zfs.resource.query`.
- `CreateVolume` stamps were consolidated and the `pool.dataset.update` response
  is trusted for verification instead of a re-read; the service-reload verb is
  cached.
- **25.04 became the documented floor**: the dead 24.x `zfs.snapshot.*` leg was
  removed.
- Stale-publication repair now uses a source-bearing re-fetch under
  `zfs.resource.query`; NVMe-oF off-mode coverage and block-share orphan sweeps
  were added.

## Batch 11 (v1.2.26) — CSI v1.12 spec conformance

Unconditional single-node exclusivity, per-RPC context deadlines on node paths,
idempotent delete semantics, and stricter volume-capability validation.

## Batch 10 (v1.2.25) — republish idempotency, takeover, per-class restore

- Same-node republish is idempotent; a stale publication record is taken over
  synchronously (with a takeover metric).
- Per-StorageClass snapshot restore mode (`snapshotRestoreMode: clone|detached`)
  and per-class spent-restore GC.
- Orphaned replication jobs are swept (with corrected job-sweep abort handling).
- Go 1.26.5 toolchain.

## Breaking change: explicit StorageClass protocol

`CreateVolume` now requires `parameters.protocol` when the running driver has
more than one protocol enabled. Missing selection returns gRPC
`InvalidArgument` with the valid `nfs`, `iscsi`, and `nvmeof` values. This
prevents an iSCSI- or NVMe-oF-intended class from silently provisioning NFS.
Single-protocol legacy configs retain a fallback to their sole enabled protocol.
An explicit `protocol` that names a protocol the driver does not serve is now
rejected up front with `InvalidArgument` listing only the enabled choices,
instead of failing later during share creation.

**Who is affected.** Every `CreateVolume` path is gated, not just Kubernetes
PVC binding:

- **Nomad users:** Nomad CSI volumes reach the same `CreateVolume` entry point,
  so multi-protocol Nomad clusters must set `protocol` in their CSI volume
  `parameters` too.
- **Restore-driven reprovisioning:** restoring a snapshot into a *new* PVC (or
  any content-source create that provisions a fresh volume) runs `CreateVolume`
  again and is subject to the same requirement. In-place restores that reuse an
  existing PV are not.

**Chart-managed StorageClasses previously injected `protocol` silently.** The
chart always rendered `parameters.protocol`, defaulting to `nfs` when a class
omitted it — so an iSCSI- or NVMe-oF-intended class without an explicit value
still rendered `protocol: nfs`, defeating the driver-side validation. The chart
now emits `protocol` only when a `storageClasses` entry sets it explicitly; when
unset, the driver's sole-enabled-protocol fallback or missing-parameter error
applies. The bundled default class still sets `protocol: nfs`.

StorageClass parameters are immutable. Create a replacement class containing
`protocol`, update workload manifests, and retire/recreate the old class only
after its name and default-class transition have been planned. Existing bound
PVs are not reprovisioned merely because new claims use the replacement class.

## Helm chart

- `controller.replicas` defaults to one. Leader election is now always enabled
  on all capable controller sidecars, even at a single replica: a
  `fencing.mode=off` RollingUpdate transiently runs two controller pods, and
  without leader election both would act as active provisioner/attacher.
  Replicas above one still add preferred hostname anti-affinity and a default
  PDB. Additive/strict fencing still requires exactly one replica.
- The controller Deployment now renders an explicit `strategy` block in every
  fencing mode so Helm always owns the field: `off` uses `RollingUpdate` with
  explicit default `maxUnavailable`/`maxSurge`, and additive/strict use
  `Recreate` with `rollingUpdate: null`. Off mode previously rendered no
  strategy, leaving a server-defaulted field that broke the off -> additive
  upgrade in production.
- Driver containers now request `10m` CPU/`32Mi` memory and have a `256Mi`
  memory limit. Every CSI sidecar requests `10m` CPU/`32Mi` memory and has a
  `128Mi` memory limit. All maps remain overridable.
- `image.digest` supports immutable driver deployment. Sidecar image strings
  already accept digest references, and Renovate explicitly tracks their tag
  defaults.
- The unused additional-iSCSI-portal chart setting is removed. Existing values
  files that attempted multi-portal configuration must remove that entry.
- iSCSI CHAP session authentication is now supported (opt-in, per StorageClass;
  one-way and mutual). The per-volume auth mode/tag are stamped immutably and
  reconstructed on fence passes; credentials never reach the PV volume context and
  are redacted from logs, errors, and Events. iSCSI multipath remains unsupported.
  CHAP authenticates the session but does not encrypt data in flight, so TCP 3260
  must still be protected by the storage-network trust boundary. See
  [iSCSI CHAP](reference/storageclass.md#iscsi-chap).

## Release governance

- Every GitHub Action reference is pinned to a full commit SHA with a version
  comment, and Renovate preserves/updates those pins.
- CI adds a distinct `CSI Sanity` check, govulncheck, CodeQL, and a tag-only
  Trivy gate that fails on unallowlisted HIGH or CRITICAL vulnerabilities.
- `golang.org/x/text` is updated to v0.39.0 to remediate the reachable
  GO-2026-5970 invalid-input infinite loop reported by govulncheck.
- Tag releases keyless-sign the pushed multi-architecture image. OCI Helm
  charts are keyless-signed and receive an SLSA provenance attestation.
- Image signing now waits for the tag Trivy scan to succeed, so a
  scan-rejected image is never left signed. The Helm chart publish/sign job runs
  only after the tag CI workflow completes successfully (via `workflow_run`),
  instead of publishing regardless of the CI outcome.

## Documentation compatibility

All shipped direct-driver examples pass strict YAML parsing. The README and the
deployment, Nomad, topology, StorageClass, production, troubleshooting,
architecture, snapshots, and disaster-recovery guides were re-audited against the
**v1.4.0** code and describe only implemented flags, values, metrics, and runtime
behavior. The v1.4.0 documentation pass folded in the release audit's
corrections: the CHAP opt-in/validation/tag/rotation/DR contract, the capacity
and volume-health chart keys and caveats, the per-protocol clone goldens
(10/13, replacing the stale 12), `truenas.maxConnections` (chart `null`/omitted,
driver default 5, range 1–16), the observability `status="error"` narrowing and
the new metric families/alerts, and the corrected node-runtime and
reconcile-mutation descriptions.
