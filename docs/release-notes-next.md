# Release notes — v1.4.0

This document is the accumulated changelog from the v1.2.23 documentation
baseline through the **v1.4.0** release candidate, ordered newest-first. v1.4.0
is a backward-compatible **MINOR** release over v1.3.0: it adds iSCSI CHAP,
CSIStorageCapacity tracking, opt-in volume-health monitoring, clone-latency
work, `truenas.maxConnections`, and an observability taxonomy migration, with
**no breaking change** to existing configuration or volumes and a verified
v1.4.0 → v1.3.0 rollback window. The v1.3.0 entry (which bundled the
v1.2.24–v1.2.35 fixes plus the batch 17–20 performance/resilience/maintainability
work) and earlier per-release entries are retained below for history. Sections
after the per-release entries (Breaking change, Helm chart, Release governance)
are cross-cutting themes that span several of these releases.

## v1.4.0 — CHAP, capacity, volume health, clone latency, observability taxonomy

Five sprints of feature and hardening work. No existing config key, volume, or
on-disk property changes meaning; every new surface is opt-in or gated and the
default Helm render stays byte-identical to v1.3.0. `Chart.yaml` carries a
`0.0.0-dev` placeholder that CI stamps with the release tag.

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
