# Batch 15 — Remnant-orphan GC (marker-based) + bookkeeping hardening (v1.2.33)

Branch: `batch15-remnant-gc` off main (post-v1.2.32). Do NOT commit; leave dirty for
adversarial review. This file is untracked spec input — never git-add it.

## Motivating incident (2026-07-23, live)
The controller OOM crash loop manufactured 8 datasets killed in the window between
`zfs clone` and the `managed_resource`/instance stamp. Result: (a) invisible to the
orphan-volume classifier (no stamp = no ownership proof, fail-safe), (b) the
in-flight-marker sweep DELIBERATELY kept their markers ("dataset exists but is still
unstamped: the marker remains the only proof that lets a retry recover the remnant —
keep it", reconcile.go sweepStaleInflightMarkers), (c) the assumed same-name
CreateVolume retry NEVER comes under VolSync (it creates a NEW PVC UID on failure).
Each zombie was an empty clone pinning a `-csi-deleted-` tombstone origin, blocking
ledger drain. Cleanup was manual. This batch automates it safely.

## Item 1 — Remnant-orphan classification + guarded destroy (the fix)

New reconcile phase (detection always-on, deletion gated by opts.Delete like every
other sweep): a dataset is a REMNANT ORPHAN when ALL hold:
1. A valid LOCAL in-flight marker exists (parent or bookkeeping child; dual-read),
   marker.Version current, marker.Instance == this driver instance, marker.Dataset
   non-empty and strictly under the CSI parent (datasetForID-style validation).
2. marker age (StartedAt) > minOrphanAge (reuse existing config; no new knob).
3. The dataset marker.Dataset EXISTS and is UNSTAMPED: no LOCAL driver-instance /
   managed_resource ownership properties (if stamped, the existing stale-marker
   sweep already retires the marker — unchanged).
4. No Kubernetes object references it: no PV whose volumeHandle == path.Base
   (marker.Dataset) (use the same live API-list hard-recheck pattern as
   revalidateOrphanVolume / liveVolumeAttachmentExists — NOT informer caches),
   no PVC in Pending that would retry this exact volume name (volume names derive
   from PVC UID, so a missing PV is sufficient — document this reasoning in a
   comment), no VolumeAttachment for it.

Guarded destroy (opts.Delete only, counts against maxPerRun deletion cap):
- Re-fetch marker AND dataset immediately pre-destroy (live, not from the pass
  snapshot): marker still present + identical nonce; dataset still unstamped.
  Any change → skip with reason.
- Identity binding: for mode=clone, the dataset's actual ZFS origin must equal
  marker.Origin (fetch via the existing origin projection); mismatch → skip +
  report (never destroy). For mode=copy (detached), origin must be empty.
- Destroy: existing guarded dataset-delete path, NON-recursive, force=false —
  children or snapshots under it must fail the delete (fail-safe), surfaced as
  skip reason.
- On successful destroy: retire the marker via removeBookkeepingProperties (both
  locations) and record in report (DeletedRemnants) + a K8s Warning event on the
  driver (existing recordWarningEvent pattern) so operators see remnant reaps.

Report/observability: new ReconcileReport fields (RemnantVolumes []ReconcileObject,
DeletedRemnants []string), log lines mirroring the other sweeps' style, skip
reasons via recordReconcileSkip. Include remnant count in the "detection complete"
summary line.

Tests (must fail without the fix — reproduce tonight's zombie exactly):
- Marker written, dataset created (clone with origin = a tombstone-named snapshot),
  NO stamp, no PV in kubeState → NOT classified before minOrphanAge; classified
  after; destroy only under opts.Delete; marker retired after destroy; the origin
  snapshot becomes clone-free (assert clone gone).
- Stamped dataset → not classified (existing sweep path untouched, assert marker
  retired by the OLD path instead).
- Marker from another instance → untouched. Origin mismatch → skip, dataset
  survives. Live PV with matching handle → not classified. Pre-destroy re-fetch
  sees a stamp appear → skip, dataset survives. Deletion cap respected.
- Race-enabled; run within the existing reconcile test harness patterns.

## Item 2 — CreateVolume guard for the bookkeeping dataset name (Opus 4b note)
Reject (InvalidArgument) any inbound volume/snapshot ID that resolves to the
bookkeeping dataset name (`.csi-bookkeeping` leaf) at the existing datasetForID
validation layer, so a crafted volumeHandle can never target the bookkeeping
dataset for delete/expand/clone. One test per RPC entry class (reuse the SEC-F1
path-traversal test style).

## Item 3 — Docs: bookkeeping downgrade warning (Opus 4b note)
docs/production.md + chart values.yaml comment: once reconcile.bookkeeping.enabled
has been true and entries live on the child, disabling it orphans child-side
entries from reads — do not disable; cleanupParent flow is the supported path.
(values.yaml comment edit only — no schema/template changes.)

## Definition of done
- `make check` green (race), golangci-lint 2.12.2 clean locally, gofmt clean.
- NO new config keys (Item 1 reuses minOrphanAge). If review forces one anyway:
  full chart parity in the same batch — values.yaml + values.schema.json +
  configmap.yaml render + a helm template assertion (batch-14 lesson: flux-local
  rejected the deploy because driver config shipped without chart plumbing).
- US spelling everywhere incl. comments ("canceled", not "cancelled") — the
  misspell linter gates CI (batch-14 lesson).
- docs/release-notes-next.md Batch 15 section.
- Nothing committed. Print summary: files touched per item, test names, final
  make check tail, deviations.
