# Batch 17 — Performance + live-state fixes (branch: batch17-perf, base: main @ 5ed0e74)

All findings verified by a 4-agent review pass 2026-07-25 (static perf trace + live cluster metrics + live nas01 backend audit). Line refs are from main @ 5ed0e74 — verify before editing. Behavior contracts (ambiguity taxonomy, provenance-before-mutation, per-volume locks, fail-closed guards) are LOAD-BEARING: none of these items may weaken a guard, reorder a mutation across a documented crash boundary, or convert a fail-closed path to fail-open.

## Definition of done (every item)
- `go test -race ./...` (NON-short — csi-sanity + fuzz seeds are !Short-gated) green.
- `golangci-lint run` 0 issues (v2.12.2 config in repo).
- Golden API-call-count tests updated WITH comment breakdowns explaining every RTT (existing style in `pkg/driver/api_call_count_test.go`).
- Any NEW driver config key REQUIRES same-commit chart plumbing: `charts/scale-csi/values.yaml` + `values.schema.json` + `templates/configmap.yaml` render + a helm-template assertion test (this DoD rule exists because batch 14 shipped driver config without chart plumbing and flux-local caught it).
- Commit per logical item (P1, P2, ... prefixes in commit messages).

## P1 — Publish/unpublish path: eliminate intra-request re-resolution (HIGH VALUE — live-proven cost)
Live evidence: `nvmet.host_subsys.create` error rate 13% (79/608 — the ONLY erroring TrueNAS method); ControllerPublishVolume avg 5.3s / p99 9.9s. Strict NVMe publish is ~13 RTTs today.

Current trace (strict NVMe, share exists, cached IDs valid, no takeover):
1. `DatasetGet` controller.go:1191
2-3. resolve namespace + subsystem in `EnsureShare` → nvmeof_share.go:77-84
4. UNCONDITIONAL repair-stamp write nvmeof_share.go:89-94 (written even when both IDs already match stored props)
5-6. namespace + subsystem resolved AGAIN in `validateBackendSingleNodeCompatibility` fencing.go:540-544
7. `NVMeoFHostFindByNQN` per exempt NQN fencing.go:561
8. `NVMeoFHostSubsysListBySubsystem` fencing.go:569
9. `storePublicationRecord` fencing.go:948
10-11. namespace + subsystem resolved A THIRD TIME in `applyNVMeFence` fencing.go:1403-1407
12. UNCONDITIONAL `NVMeoFHostSubsysCreate` fencing.go:1436 (relies on AlreadyExists tolerance — this is the 13% error rate)
13. `NVMeoFHostSubsysListBySubsystem` AGAIN fencing.go:1440

Changes:
- Thread the resolved namespace/subsystem objects (and the host-subsys association list) through `publishFencedVolume`'s phases as parameters. All reads happen within one per-volume-locked request; freshness is unchanged in practice (they are milliseconds apart today). Do NOT add any cross-request cache — parameter threading only. The one deliberate re-read after takeover (fencing.go:869-879, observing post-revocation state) MUST be preserved.
- Skip the EnsureShare repair-stamp write when `PropNVMeoFSubsystemID`/`PropNVMeoFNamespaceID` already carry the resolved values.
- Skip `NVMeoFHostSubsysCreate` when the threaded association list already shows the node's host association.
- Apply the same de-duplication to the unpublish path (~8 RTTs, same duplicate namespace/subsystem resolution inside `applyNVMeFence`) and, where it falls out naturally, to the iSCSI fence path.
- `reconcileStartupFencingVolume` / startup fencing path benefits automatically via the shared helpers — verify it compiles through the new signatures, no separate optimization needed.
Target: publish ≈6-7 RTTs. Expect the host_subsys.create error count to drop to ~0 (assert nothing in code depends on the AlreadyExists tolerance being exercised; keep the tolerance itself as a safety net).

## P2 — Golden API-call-count tests for publish/unpublish (test-only, locks in P1)
`TestControllerGoldenPathAPICallCounts` (api_call_count_test.go:533-691) pins Create/Delete volume+snapshot only. Add pinned cases with per-call comment breakdowns:
- (a) fencing off + NFS (records-only floor)
- (b) additive + NFS
- (c) strict + NVMe-oF single-node — publish AND unpublish
Build on `allowlistCountingClient` (fencing_test.go:135) / the existing counting client in api_call_count_test.go.

## P3 — Reconcile N+1 elimination (~90-95 RTTs per 10-min pass → ~15)
- (a) `adoptLegacyOwnershipStamps` reconcile.go:2273: the source-bearing `DatasetGet` happens BEFORE the "already has an instance stamp" presence check (reconcile.go:2290). Presence is decidable from the sourceless listing's flat `user_properties` — reorder so only unstamped candidates get the source-bearing GET. Steady state: 29 → 0 RTTs/pass. The overwrite-protection semantics ("never overwrite an existing stamp of ANY source") must be preserved exactly — the GET still happens (with source) before any write for actual adoption candidates.
- (b) Stale-publication reconcile reconcile.go:2631-2638: every dataset with `publication_*` keys gets an individual source-bearing `DatasetGet` every pass — with fencing on that is ALL ~28 attached volumes (~4k calls/day). Replace with ONE `pool.dataset.query` carrying `["id","in",[flagged names]]` returning source-bearing user properties (same DatasetGet projection/options — this must remain a SOURCE-BEARING read; do NOT use zfs.resource.query which loses user-property source). Per-record decision logic unchanged.
- (c) `detectOrphanedNVMeoFShares` reconcile.go:748: `NVMeoFNamespaceListBySubsystem` inside the subsystem loop (~28 calls/pass). Fetch one global namespace list, group client-side by subsystem id.
- (d) Duplicate parent/bookkeeping `DatasetGet`s within one pass: reconcile.go:259/275 and again in `classifyRemnantOrphans` reconcile.go:1878/1885. Thread the already-read datasets through. NOTE: `destroyRemnantOrphan`'s own live re-fetch under the per-volume lock is a deliberate guard — do NOT remove that one.
- (e) `classifySpentRestoreSnapshots` reconcile.go:1297 calls `findBackendSnapshotForHandle` per candidate; short names route to `SnapshotFindByName` → full recursive snapshot-set transfer PER candidate (snapshot.go:521-535). Resolve against the pass's already-fetched in-memory snapshots slice instead. Keep the live scoped re-fetch in `revalidateSpentRestoreSnapshot` (pre-delete guard) untouched.

## P4 — Bookkeeping-dataset existence flag
`ensureBookkeepingDataset` (provenance.go:85-96) does an unconditional `DatasetGet` of `.csi-bookkeeping` on EVERY marker/ledger write (~400/day live). Add an `atomic.Bool` set after first successful ensure, re-armed (cleared) on any bookkeeping write failure so a deleted-out-from-under dataset self-heals on the next write. −1 RTT per marker write, per tombstone-ledger write.

## P5 — Batch post-destroy ledger removals at pass end (nightly reap)
`removeBookkeepingProperties` (provenance.go:118-132) issues per-tombstone removals (parent AND child = 2 writes each, ~384/night). The sweep already retires leftovers best-effort (comment reconcile.go:1622-1624), so batching the POST-DESTROY removals of the whole pass into one multi-key `user_properties_update` remove per location at pass end is safe. HARD CONSTRAINT: TrueNAS 26.0 WS API rejects inbound messages >64kB (close 1009) — chunk batches ≤32kB (precedent: provenance.go migration batching). Per-reap provenance re-reads and the destroy ordering itself are UNCHANGED — only the post-destroy property removal is deferred and batched. If the batched removal fails, entries remain and the existing orphan-ledger sweep retires them later (same as today's failure mode).
Also: in a fully-migrated steady state (`bookkeeping.enabled` + parent drained) `removeBookkeepingProperties` still issues the parent-side removal first — one wasted RTT per retirement. Skip the parent write when the pass's parent read showed zero matching local keys for that entry.

## P6 — Scan-based tombstone reaper fallback (fixes 293 live-stranded snapshots, prevents recurrence)
Live fact: 965 `-csi-deleted-` snapshots on nas01, 672 ledger entries — the 293-snapshot difference (Jul-22/23 pre-relocation cohort, ~2.64 GiB, verified zero clones/zero holds) has NO ledger entry and the ledger-driven reaper can never touch it. Same stranding recurs after any future ledger loss.
Add to the nightly delete pass: snapshots under the CSI parent whose name matches the driver's exact tombstone rename format, with NO ledger entry (parent∪child dual-read), no dependent clones (dataset-side origin projection — `DatasetHasDependentClones`, the 26.0-correct method), and age > `reconcile.tombstoneScan.minAge` (new config, default 72h) → eligible for guarded destroy under the same delete-mode gate (`opts.Delete`), per-volume lock, and deletion cap as ledgered reaps. Report counter: `scanReapedTombstones` in the summary (honest counters — split from ledgered reaps).
Safety belts (all required): name-format match must be strict (the exact rename pattern incl. the driver marker — do not glob loosely); clone check via origin projection immediately before destroy; skip anything with a ledger entry (belongs to the ledgered path); never recursive destroy; ZFS deferred-destroy semantics same as `reapTombstoneSnapshot`.
NEW CONFIG KEY → full chart DoD applies (values.yaml + values.schema.json + configmap.yaml + helm-template assert).

## P7 — Scrub protocol-foreign inherited properties at clone stamp
Live fact: 16 clone-created zvols carry stale `truenas_iscsi_extent_id`/`truenas_iscsi_target_id`/`truenas_iscsi_targetextent_id`/`truenas_nfs_share_id` props inherited from their clone origins, referencing objects that no longer exist. ZFS clone inheritance reports source = origin-snapshot-name (NOT 'inherit', NOT 'local').
At clone-creation stamping time (the ownership/content-source stamp step), remove driver-namespace share-ID properties that (a) are not source=local and (b) reference a protocol other than the volume's own share type. Removal via the existing property-removal primitive. Do NOT touch source=local props, do NOT touch non-driver namespaces, do NOT touch the properties belonging to the volume's actual protocol (those get written by share creation). Add a regression test with a mock dataset carrying clone-inherited foreign props (mock must set the source field to the origin-snap-name shape — precedent: the B3 clone-inheritance mock from v1.2.23-era work).

## P8 — Chart values: sidecar tail-latency containment (values-only)
charts/scale-csi/values.yaml:363-409 — 4 sidecars × workerThreads:10 × timeout:300s vs the client's 10-slot semaphore: a wedged call can pin a slot for 300s during VolSync top-of-hour bursts. Change: attacher and resizer `timeout` 300s → 120s. Leave provisioner/snapshotter at 300s (long clone/copy operations are legitimate there — detached copies can exceed 120s). Leave workerThreads alone. Update values.schema.json only if it pins these values.

## Explicitly OUT OF SCOPE for this batch (do not implement)
- Merging quota/content-source/ownership stamps on the clone path (crosses documented crash boundary — separate adversarially-reviewed batch).
- Tombstone reap grouping by source volume (per-reap ledger re-read is deliberate provenance design).
- core.subscribe job-wait, typed JSON decode (client-layer batch 19).
- core.bulk anywhere (rejected: ambiguity-taxonomy damage).
- Any TTL/cross-request cache for share-object resolution (parameter threading only).
