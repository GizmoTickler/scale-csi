# Batch 16 — Legacy stamp adoption (v1.2.34)

Branch: `batch16-stamp-adoption` off main (post-v1.2.33). Do NOT commit; leave dirty
for adversarial review. This file is untracked spec input — never git-add it.

## Motivating incident (2026-07-23 04:00Z GC run, live production)
The first age-eligible tombstones were REFUSED by the reaper: "tombstone source
dataset does not carry this driver instance's ownership stamp". Root cause: the
migration-era volumes (created 2026-07-20/21, BEFORE v1.2.21 introduced
`scale-csi:driver_instance_id` stamping) lack the stamp — verified live, e.g.
flashstor/scale-csi/pvc-111ae4b0-* has LOCAL managed_resource + csi_volume_name +
content-source props but NO driver_instance_id. The reaper's instance belt
therefore refuses their tombstones FOREVER: ledger entries and -csi-deleted-
snapshots for legacy sources accumulate indefinitely (+16/h from hourly VolSync).
The stamps must be adopted, not the reaper weakened.

## Item 1 — Guarded stamp-adoption pass

New reconcile step (runs in every reconcile pass, before tombstone sweeping so a
freshly adopted source unblocks reaping in the SAME pass; idempotent, cheap when
nothing qualifies). For each managed dataset from the existing listing, adopt-stamp
`driver_instance_id` when ALL hold:
1. Dataset is strictly under the CSI parent, valid volume leaf, NOT the
   `.csi-bookkeeping` dataset.
2. LOCAL `managed_resource == "true"` AND LOCAL `csi_volume_name` whose value
   matches the dataset leaf (source-bearing read — use the batch-12 DatasetGet
   pattern, NEVER trust flat/sourceless listings for the source check).
3. NO existing `driver_instance_id` property of ANY source. **Absolute rule: never
   overwrite or replace an existing instance stamp — local, inherited, or
   foreign — a dataset stamped by ANOTHER instance must be left untouched (a
   second driver install sharing a pool must never be hijacked).**
4. A live Bound PersistentVolume of THIS driver references it:
   `pv.Spec.CSI.Driver == d.name && pv.Spec.CSI.VolumeHandle == leaf &&
   pv.Status.Phase == Bound` via a LIVE API list (clientset List, not informers),
   with the standard fail-safe: list error or empty PV list for the whole driver →
   adopt NOTHING this pass (API discontinuity is not evidence).
5. Write via the proven stampAndMirror/user-property write path used at create
   time; verify the write (source-bearing re-read) before counting it adopted.

Report/observability: AdoptedStamps []string in ReconcileReport + count in the
detection-complete summary line + klog Info per adoption (one line, volume id).
This is a WRITE that runs in detection mode (not gated by opts.Delete) — justify
in a comment: it adds provenance to datasets that are provably this cluster's
Bound volumes; it deletes nothing; it is required for the delete-mode reaper to
ever act on legacy tombstones. Cap adoptions per pass (reuse maxPerRun) as a
blast-radius bound.

Residual (document in code comment + release notes): a legacy dataset that is
NOT currently Bound is never adopted — its tombstones stay refused (fail-safe);
operators can bind it or clean manually.

## Item 2 — Regression test reproducing the 04:00Z refusal end-to-end
Mock scenario: legacy dataset (LOCAL managed_resource + csi_volume_name, NO
instance stamp) + its aged tombstone ledger entry + Bound PV in kubeState.
Assert: pre-fix the tombstone reap is refused with the exact "ownership stamp"
reason; post-fix the SAME pass adopts the stamp and reaps the tombstone. Plus:
non-Bound PV → no adoption + still refused; foreign instance stamp → untouched
(assert value unchanged); inherited-only managed_resource → no adoption;
PV of another driver name → no adoption; empty PV list → no adoption (fail-safe);
adoption cap respected; already-stamped-by-us → no-op write-free.

## Item 3 — Comment typo (carried nit from v1.2.28 sign-off)
reconcile.go ~1155 comment says "deleteDetachedOrphans" — should read
"deleteDetectedOrphans". Fix the comment only.

## Definition of done
- NO new config keys → no chart changes required (if review forces one: full
  chart parity same-batch — values.yaml + schema + configmap + helm template
  assertion).
- `make check` green (race), golangci-lint 2.12.2 clean, gofmt clean, US spelling.
- docs/release-notes-next.md Batch 16 section.
- Nothing committed. Print summary: files touched, test names, final make check
  tail, deviations.
