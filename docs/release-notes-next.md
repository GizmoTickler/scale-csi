# Next release notes (draft)

This draft describes changes after v1.2.23. It is intentionally not a tag or a
final version announcement.

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

All shipped direct-driver examples now pass strict YAML parsing. The deployment,
Nomad, topology, StorageClass, production, troubleshooting, architecture, and
disaster-recovery guides now describe only implemented flags, values, and
runtime behavior.
