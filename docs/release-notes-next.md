# Release notes — v1.3.0

This document is the accumulated changelog from the v1.2.23 documentation
baseline through the **v1.3.0** tag, ordered newest-first (v1.3.0 → v1.2.25).
v1.3.0 bundles the v1.2.24–v1.2.35 fixes plus the batch 17–20 performance,
resilience, and maintainability work. Sections after the per-release entries
(Breaking change, Helm chart, Release governance) are cross-cutting themes that
span several of these releases.

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
  instead of re-reading, folding the clone hot path to **12 round trips**.
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
  reaping in the SAME pass) and stamps `driver_instance_id` onto a legacy
  managed dataset only when ALL hold: it sits strictly under the CSI parent and
  is a valid volume leaf (not the `.csi-bookkeeping` dataset); a source-bearing
  re-read (the batch-12 `DatasetGet` pattern, never the sourceless listing)
  proves a LOCAL `managed_resource=true` AND a LOCAL `csi_volume_name` matching
  the dataset leaf; the dataset carries NO existing `driver_instance_id` of ANY
  source; and a live **Bound** PersistentVolume of THIS driver references it
  (live clientset list, not informer caches). The write goes through the proven
  `stampAndMirror` user-property path used at create time and is verified by a
  source-bearing re-read before it counts as adopted. Adoptions are capped at
  `maxPerRun` per pass as a blast-radius bound and reported in
  `ReconcileReport.AdoptedStamps` (+ count in the detection-complete summary line
  and one klog line per adoption).
  - **Absolute rule:** an existing `driver_instance_id` — local, inherited, or
    foreign — is NEVER overwritten. A dataset stamped by another driver instance
    sharing a pool is left untouched (never hijacked). The absence is re-proved
    under the per-volume lock immediately before the write.
  - **Why a write runs in detection mode:** the step only ADDS provenance to
    datasets that are provably this cluster's Bound volumes; it deletes nothing;
    and it is required for the delete-mode reaper to ever act on legacy
    tombstones. It is not gated by `delete.enabled`.
  - **Fail-safe:** a PV-list error, or an empty PV list for the whole driver,
    adopts NOTHING that pass (an API discontinuity is not evidence).
  - **Residual:** a legacy dataset that is NOT currently Bound is never adopted,
    so its tombstones stay refused (fail-safe); operators can bind it or clean it
    up manually.
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
- CHAP and iSCSI multipath remain unsupported; TCP 3260 must be protected by the
  storage-network trust boundary.

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
v1.3.0 code and describe only implemented flags, values, metrics, and runtime
behavior. This pass corrected stale chart keys in troubleshooting
(`nfs.server`/`iscsi.portal`/`nvmeof.address`), documented the
`snapshotRestoreMode` StorageClass parameter, and recorded the always-on leader
election and the WebSocket pool/resilience internals.
