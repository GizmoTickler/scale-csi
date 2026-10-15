# Batch 18 — Behavior-preserving refactor + two small fixes (branch: batch18-refactor, base: batch17-perf tip)

From the 2026-07-25 structural review. Everything here is behavior-preserving EXCEPT R9 (dangling-symlink self-heal) and R8 (IsNotFoundError tightening), which are behavior-adjacent and get their own tests. Reviewer will verify behavior-preservation by mechanical diff (sorted-multiset move verification for file splits, per-function extraction diffs) — structure commits so that is possible: pure-move commits contain ONLY moves, extraction commits ONLY extractions, fix commits ONLY the fix.

## Definition of done
- `go test -race ./...` (non-short) green; `golangci-lint run` 0 issues.
- Golden API-call-count tests unchanged (byte-identical expectations) except where a batch-17 rebase already moved them.
- No exported-symbol changes, no lock-order changes, no error-policy changes in pure-refactor commits.
- Commit per item (R1, R2, ... prefixes).

## R1 — Split reconcile.go along the seams the test files already use (pure git-mv-style moves, zero signature changes)
reconcile.go (2,869 lines) → keep `reconcile.go` (options/report types, reconcileOrphans entrypoint, deleteDetectedOrphans, startOrphanReconcile loop, small helpers) and move to:
- `reconcile_kubestate.go` — kubernetesReconcileState, loadKubernetesReconcileState, kubernetesReconcileClients, liveVolumeAttachmentExists, liveBoundVolumeHandles, hardRecheck*, remnantHasNoKubernetesReference
- `reconcile_shares.go` — comment-parsers, detectOrphaned{NFS,ISCSI,NVMeoF}Shares, deleteOrphaned*Share
- `reconcile_tombstones.go` — tombstone classification, reapTombstoneSnapshot, scan-fallback (batch-17 P6), sweepOrphanedTombstoneLedger, listedSnapshotIDs
- `reconcile_remnants.go` — sweepStaleInflightMarkers, localInflightMarkers, classifyRemnantOrphans, destroyRemnantOrphan, sweepOrphanedReplicationJobs, liveCopyMarkers
- `reconcile_spent_restore.go` — classifySpentRestoreSnapshots + revalidateSpentRestoreSnapshot
- `reconcile_publications.go` — stale-publication observation, reconcileStalePublicationRecords, revokeStalePublicationRecord
- `reconcile_adoption.go` — adoptLegacyOwnershipStamps, writeAndVerifyAdoptionStamp
Do NOT introduce a generic sweeper framework; every sweeper keeps its object-specific guard chain verbatim.

## R2 — Decompose the reconcileOrphans body (~362 lines) into named phase functions
Extract as-is, all state via parameters: readBookkeepingState (dual-read prologue), classifyOrphanVolumes, classifyOrphanSnapshots, classifyTombstones, the dry-run log block, the delete-gate tail. Straight extraction — no logic changes.

## R3 — Session-GC dedup in driver.go + dead-logic removal
- gcISCSISessions (696-807) and gcNVMeoFSessions (811-923) are line-for-line parallel; getExpectedISCSITargets/getExpectedNVMeoFNQNs likewise. Extract a shared core parameterized by a small per-protocol struct (list, match-scope, disconnect, expected-set). Keep the two seen-maps separate.
- DEAD LOGIC: driver.go:909-911 — the `strings.HasPrefix(nqn, "nqn.")` guard in the NVMe first-seen sweep is vestigial (separate sync.Maps since the shared-map era ended; the iSCSI sweep has no such guard). Remove it, with a commit message explaining why it is dead.
- Stale comment driver.go:653: doc comment names runSessionGC on runSessionGCWithProtocols.

## R4 — Node stage twins dedup
stageISCSIVolume (node.go:1406-1519) / stageNVMeoFVolume (1522-1631) share an identical skeleton. Extract the shared tail (symlink-or-format-mount) and the pre-emptive-disconnect block into helpers parameterized by a transport struct (listSessions, findSession, disconnect, connect, identify); iSCSI keeps nodeCheckISCSIMultipath as a post-connect hook. CAREFUL: the poll closures re-assign the captured `sessions` variable — preserve that exact behavior.

## R5 — Shared dual-read bookkeeping helper
The parent-GET + child-GET + IsNotFoundError-tolerant merge is hand-rolled ~5 times (reconcile.go:259-291 fail-open; 1661-1680 fail-closed; 1878-1896 fail-closed-return; 2050-2067 fail-closed; provenance.go:171-191 child-first). Extract ONE mechanical helper returning both datasets and both raw errors; EVERY caller keeps its own explicit fail-open/fail-closed decision and its own parse function. Do not flatten error policy. (Rebase note: batch-17 P3d threads these reads — adapt.)

## R6 — volumeLockKey constructor
17 inline `"volume:"+id` sites (controller.go 349/849/1181/1225/1400/1530/1667, reconcile.go 1633/1962/2353/2688, startup_reconcile.go:196, plus any batch-17 additions). Add `volumeLockKey(id)` next to nodeVolumeLockKey and optionally `withVolumeLock(id, fn)` for the acquire/Aborted/defer triple. Node keyspace stays separate (deliberately non-conflicting).

## R7 — controller.go extractions
- CreateVolume: extract validateCreateVolumeRequest (355-445, pure) and createVolumeExisting (448-601, the self-contained already-exists arm).
- Unify the three property-sniffing variants: DeleteVolume share-type sniffing (888-918), storedBlockProtocol (726-742), shareTypeForPublishedVolume (fencing.go:273) → one primitive with three thin call sites.
- Hygiene: sanitizeVolumeID method-vs-free-function wrapper (1791-1793) — pick one; rename the free snapshotListEntry (1854) to de-shadow the method (1888); hoist the empty-check in startOrphanReconcile's parse-then-overwrite (2787-2804); split LoadConfig (config.go:564-860) into applyConfigDefaults + validateConfig with the protocol-block back-compat sniffing (611-623) as a named helper.

## R8 — Mock errno migration, THEN IsNotFoundError tightening (this order)
- (a) mock_client.go returns `&APIError{Code:-1, Message:"dataset not found"}` at ~20 sites, so tests only ever exercise the substring fallback, never the errno path production 26.0 emits. Migrate mock errors to carry `Data` with errno ENOENT shape (keep the message), so golden tests validate the authoritative path.
- (b) THEN tighten the IsNotFoundError fallback (client.go:61-76) to match `apiErr.Message` only, not `FullError()` (which embeds the whole Data blob via %+v — a -1 error merely MENTIONING "not found" about a nested object is currently misclassified as authoritative absence; callers like the share-orphan TOCTOU guard and replicationJobDatasetMissing act on it). Keep FullError() for logs. Grep test fixtures first to confirm no recorded 26.0 not-found arrives message-less.

## R9 — Dangling staging-symlink self-heal (behavior-adjacent, small)
handleExistingStage (node.go:234-301, specifically 256-259): a DANGLING staging symlink (device vanished — node reboot with persisted staging dir, dropped session) makes EvalSymlinks fail → returns handled=true + codes.Internal → every kubelet retry fails identically FOREVER; the stage can never reach the reconnect path built to repair exactly this (disconnect stale session → reconnect → createSymlinkAtomic replaces the link).
Fix: when the entry is a symlink and EvalSymlinks fails with a not-exist-class error, clean up the stage record and return (false, nil) so the normal stage path repairs it. KEEP Internal (fail-closed) for identify errors on a RESOLVABLE device. Tests required: (1) dangling symlink → stage succeeds end-to-end via reconnect; (2) live-but-unidentifiable device → still fails closed with Internal.

## R10 — Fencing test harness (test-only)
newFencingTestHarness(t, mode, proto, opts...) returning driver+client+node-identity helpers; migrate fencing_test.go's hand-built setups (near-identical blocks, e.g. 34-76 vs 197+). Precedent: newTestNodeDriver (node_test.go:136). Do NOT convert narrative scenario tests to tables — they assert mid-flight state; only the genuinely param-only cases (protocol-compat AlreadyExists rejections, access-mode validation) may become tables. Estimated −400-600 lines.

## Explicitly NOT in scope
- Generic sweeper framework / lock-refetch-recheck template (rejected: flattens object-specific safety reasoning).
- Any change to fencing.go fence bodies beyond R7's sniffing primitive.
- hardRecheck* full-LIST pattern (deliberate: closes the legacy-name gap).
- Bookkeeping "migration complete" third state (deferred until after live soak of batch-17 P5's parent-skip).
