# Batch 14 — Adversarial-verification fixes (v1.2.29)

Six fixes from the 2026-07-22 dual-reviewer adversarial verification (Fable + Qwen,
all findings independently re-verified with executed tests or live nas01 measurement).
Branch: `batch14-verification-fixes` off b71b1fd (v1.2.28). Do NOT commit — leave the
tree dirty for adversarial review. `make check` must be green (race-enabled) when done.
This file is untracked spec input; do not add it to git.

General rules:
- Preserve every existing safety gate. No behavior change outside the scoped fixes.
- Every fix ships with regression tests that FAIL on v1.2.28 semantics and PASS after.
- Match existing code style, error taxonomy, and logging conventions.
- No new API methods outside the live 26.0 catalog (all currently used methods are valid).

---

## Fix 1 (HIGH, resilience) — Bare connection-loss errors escape retry and corrupt the breaker

Problem: `pkg/truenas/client.go:1052` (`callWithGeneration`) returns bare
`fmt.Errorf("connection lost before request was sent")`; `client.go:635` and `:747`
(connect/auth path) return bare `"connection lost during authentication"`. None wrap
`ErrTransportFailure`, so `IsConnectionError()` (client.go:193) returns false → the
`Call` retry loop takes the "not a connection error" branch (client.go:1309-1320):
NO retry AND `circuitBreaker.RecordSuccess()`, resetting the failure count. During a
flapping backend the breaker can fail to open and callers get spurious hard failures.

Fix: wrap all three sites with `%w`+`ErrTransportFailure`. These are strictly
pre-send / pre-auth failures — `ErrTransportFailure` (not `ErrAmbiguousResult`) is the
correct semantic: the server cannot have applied anything. Audit the rest of client.go
for any other bare connection-ish `fmt.Errorf` on paths returned to `Call` and wrap
consistently (do NOT touch APIError paths or `ErrAmbiguousResult` sites).

Tests:
- `IsConnectionError` returns true for all three wrapped errors.
- A Call-loop test (mock/fake connection) proving: bare-error path previously
  short-circuited; now retries occur and the breaker records failure, not success.
- Race-enabled.

## Fix 2 (MEDIUM, availability) — Service-reload debouncer starves under sustained load

Problem: `pkg/driver/service_reload.go:78-84` — every `RequestReload` stops and
restarts the timer (pure trailing-edge). Proven: a request stream at 20ms intervals
with a 50ms window fires ZERO reloads until the stream stops. During attach storms
(window: 2s, config.go:669) iSCSI reload — and every caller blocked on resultCh —
is postponed indefinitely; `applyISCSIFence` (fencing.go:1389) treats reload failure
as fatal to ControllerPublishVolume.

Fix: arm the timer once per batch — the FIRST request in an idle period arms
`timer(window)`; subsequent requests within the window coalesce but do NOT reset the
deadline. After firing, the next request starts a new batch. (Leading-window batching:
max latency = window, storms coalesce, no starvation.)

Tests: sustained-stream test (mirror the proof test: 600ms stream at 20ms intervals,
50ms window) asserting reloads fire at ~window cadence during the stream (≥8 for those
numbers, allow scheduling slack — assert ≥5). Existing debounce-coalescing tests must
still pass. Race-enabled.

## Fix 3 (MEDIUM, logic) — Orphan classifier trusts inherited `managed_resource`

Problem: `pkg/driver/reconcile.go:241` (classifier) and `:1685`
(`revalidateOrphanVolume`) read `UserProperties[PropManagedResource].Value` without
the source=="local" discipline used elsewhere. A user dataset nested under a live CSI
volume inherits `managed_resource=true` + `csi_volume_name` and is classified as a CSI
orphan: phantom entries in reports/metrics, and under delete mode each phantom
permanently consumes a `maxPerRun` slot per pass (no data loss — delete resolves to a
nonexistent path — but the slot burn and false DELETED reporting are real).

Fix: apply the batch-12 source-bearing re-fetch pattern (`DatasetGet` returning
property source, as used for publication records) to confirm `managed_resource` is
LOCAL before classifying a dataset as a managed orphan, in BOTH sites. To bound API
cost, re-fetch only datasets that would otherwise be classified as orphans (candidate
set is small), not every listed dataset. A dataset whose managed_resource is inherited
(non-local) is NOT managed — skip with a debug log, no report entry.

Tests: regression test with a nested dataset carrying inherited (non-local)
managed_resource under a live volume → NOT classified, NOT revalidated, no slot
consumed. Reference implementation of the scenario exists in the Fable verification
worktree (`/private/tmp/scale-csi-fable-verify/pkg/driver/zz_fable_verify_test.go`) —
reimplement in repo style, do not copy verbatim.

## Fix 4 (HIGH, efficiency/scalability) — Snapshot query amplification (29 MB/call, O(N²))

Problem (live-measured on production TrueNAS 26.0): `zfs.resource.snapshot.query`
offers no server-side name filter or pagination, so every `SnapshotFindByName` /
`SnapshotListAll` / recursive `SnapshotGet` (pkg/truenas/snapshot.go:468-535,
`paginateSnapshots` :587) transfers EVERY snapshot under the CSI parent with
`get_user_properties:true` — currently 29.2 MB JSON per call (289 snapshots), because
each snapshot inherits ~45 `scale-csi:tombstone_*` ledger blobs + markers from the
parent dataset. `listAllManagedSnapshots` (pkg/driver/reconcile.go:905) re-fetches and
re-sorts that full payload PER 100-ITEM PAGE → O(N²) per reconcile pass; hourly
VolSync churn makes this >1 GB/hour of middleware JSON at 16 volumes.

Fix, two parts (both required):

4a. Kill the O(N²) and redundant refetches:
  - `listAllManagedSnapshots`: fetch ONCE, partition/sort/page in memory.
  - Within a single reconcile pass, reuse one snapshot listing across phases that
    currently each re-list (orphan-snapshot classification, tombstone sweep's
    per-ledger-entry `SnapshotGet` — resolve from the in-memory listing instead,
    ~45 × 1.7 MB saved).
  - Within a single CSI op, never issue the recursive listing more than once.
  - Do NOT cache across CSI ops / reconcile passes (staleness would undermine the
    safety gates — freshness at op scope is required).

4b. Stop the payload bloat at its source — move the tombstone ledger and in-flight
markers OFF the inheritable CSI parent dataset:
  - New home: a dedicated bookkeeping child dataset (e.g. `<parent>/.csi-bookkeeping`
    or a name consistent with existing conventions) that is NEVER used as a volume
    parent, so its local properties inherit to nothing. Its own snapshots are never
    taken. Creation must be idempotent and lazy (first write creates it).
  - Migration: on first ledger write (or a one-time startup migration step in
    startup_reconcile.go), read any ledger entries / markers present on the old parent
    location, copy them to the new home, then REMOVE them from the parent (pool.dataset
    user-property removal). Reads must consult BOTH locations until the parent is
    clean (upgrade window safety: an old controller may still write to the parent —
    the migration must be re-runnable and lossless; never delete a parent entry that
    failed to copy).
  - The tombstone reaper, ledger sweep, and remnant-recovery machinery keep identical
    semantics — only the storage location of the ledger changes. Every existing
    tombstone/ledger/marker test must pass with at most mechanical updates; add
    upgrade-path tests (entries on parent only, on both, on new only).
  - After 4b, snapshots under volumes no longer inherit ledger props; combined with
    4a this reduces reconcile wire volume by orders of magnitude.

This is the riskiest item of the batch (it touches data-safety bookkeeping). Be
conservative: if any part of 4b cannot be made provably lossless, implement 4a fully,
implement the dual-read layer, and leave parent-side cleanup behind a config flag
default-off — state clearly in the summary what was gated and why.

Tests: unit tests for single-fetch pagination equivalence (same results as today for
multi-page sets); reuse-within-pass test (call-count assertions — extend
api_call_count_test.go with reconcile-pass counts if practical); migration tests
(three states above); race-enabled.

## Fix 5 (MEDIUM, hygiene) — Orphan-GC sweeps diverge from canonical share teardown

Problem: the sweeps in pkg/driver/reconcile.go:
- `deleteOrphanedISCSIShare` (:729) deletes targetextent → extent → target but never
  deletes the per-volume fencing initiator group that canonical teardown removes
  (iscsi_share.go:395-399) → one leaked initiator group per swept volume.
- `deleteOrphanedNVMeoFShare` (:782) deletes namespaces → subsystem but skips the
  port-subsystem association deletion canonical teardown performs (nvmeof_share.go:266-270)
  → subsystem delete may fail forever (sweep errors every pass) or dangle.

Fix: make each sweep delete the same object set as its canonical teardown, using the
same lookups (initiator-group find by the volume's group naming; port-subsys
associations listed by subsystem id, deleted before the subsystem). Preserve the
sweeps' tolerant IsNotFoundError handling and error-report style.

Tests: sweep tests asserting the full object set is deleted (mock call assertions),
mirroring what canonical-teardown tests assert; NVMe-oF sweep with existing port
associations succeeds.

## Fix 6 (MEDIUM conditional, data-safety hardening) — Spent-restore reaper counts Pending/Lost PVC as spent

Problem: classification (pkg/driver/reconcile.go:1085-1088) and revalidation (:1769-1771)
exempt ONLY `ClaimBound` source PVCs; a PVC in Pending or Lost (restore stalled >24h)
counts as spent, and under opt-in delete mode the backend snapshot backing an
incomplete restore would be destroyed.

Fix: only these states may classify as spent:
- source PVC is Bound (restore completed), or
- source PVC does not exist at all (restore torn down).
A PVC that EXISTS in any non-Bound phase (Pending, Lost, or unknown) must exempt the
snapshot: skip with a log line + reconcile skip-reason so operators see the deferral.
Apply identically in classification and revalidation.

Tests: Pending PVC → not classified, not reaped, skip recorded; Lost PVC → same;
absent PVC → still classified (existing behavior preserved); Bound → exempt.

---

## Definition of done
- `make check` green (runs `go test -race -short ./...`).
- All new regression tests present and passing; no existing test deleted or weakened
  (mechanical updates for Fix 4b location change allowed).
- `docs/release-notes-next.md` updated with a Batch 14 section (concise, user-facing).
- Nothing committed. Print a summary of files touched + any deviations from this spec.
