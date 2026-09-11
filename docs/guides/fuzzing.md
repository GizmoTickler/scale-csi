# Fuzzing

Scale CSI has exactly two untrusted-ish input surfaces: the TrueNAS WebSocket
JSON-RPC 2.0 API, and host command output (`nvme`, `iscsiadm`, `blkid`,
`/proc/mounts`, `findmnt`, sysfs). Go's native fuzzing (`go test -fuzz`)
targets both. This note is how to run the existing targets and how to add
more; it does not re-derive the invariants each target asserts — read the doc
comment on the `Fuzz*` function for that.

## Running

Fuzz targets run as ordinary unit tests (against their seed corpus, no
mutation) as part of `go test ./...` / CI. To actually fuzz one target for a
while:

```bash
go test -run '^$' -fuzz='^FuzzParseDatasetResourceOwnershipSafety$' -fuzztime=60s ./pkg/truenas/
```

- `-run '^$'` skips every non-fuzz test in the package so the binary goes
  straight to fuzzing.
- The `^...$` anchors on `-fuzz` matter: fuzz target names are matched as a
  regex, and several names in this repo are prefixes of others (e.g.
  `FuzzIsLocalUserPropertySource` is a substring-free full name in
  `pkg/driver`, but sloppy anchoring can pick up more than one target across
  packages if you ever run `-fuzz` against `./...`). `go test -fuzz` only
  fuzzes one package at a time regardless, so this mostly matters for
  matching within a single package that has several similarly-named targets
  (e.g. `pkg/truenas` has nine).
- Drop `-fuzztime` to fuzz indefinitely (until Ctrl-C); use a duration
  (`60s`, `5m`, ...) to bound a CI or local run.

Run every fuzz target in a package against its seed corpus only (no mutation,
fast, this is what CI does):

```bash
go test ./pkg/truenas/... ./pkg/driver/... ./pkg/util/... ./cmd/scale-csi/...
```

## Where the targets live

| Package | File | Surface |
|---|---|---|
| `cmd/scale-csi` | `fuzz_test.go` | cgroup memory-limit file parsing |
| `pkg/driver` | `fuzz_test.go` | volume/share ID sanitization; **local-ownership-stamp safety** (`isLocalUserPropertySource`, `datasetHasLocalUserProperty`, `datasetHasLocalOwnershipStamp`) |
| `pkg/truenas` | `fuzz_ownership_test.go` | JSON-RPC envelope/error decode, dataset/snapshot typed-vs-interface decode parity, CSI user-property namespace fold, per-parser decode safety (NFS/iSCSI/NVMe-oF/job) |
| `pkg/truenas` | `typed_decode_test.go` | typed-vs-interface dataset/snapshot decode parity (the original two targets this package started with) |
| `pkg/util` | `fuzz_test.go` | `/proc/mounts`, `nvme list-subsys` JSON, `iscsiadm -m session` text, `blkid -o export` text, NVMe-oF multipath address extraction, SCSI WWID / iSCSI portal normalization |

## Seed corpora

Two kinds of corpus entries exist, and both live under
`<package>/testdata/fuzz/<FuzzName>/`:

1. **Hand-written seeds** via `f.Add(...)` calls at the top of each `Fuzz*`
   function, in the test source itself. These should be real or
   realistically-shaped payloads — pulled from `pkg/truenas/testdata/*.json`
   (live-captured TrueNAS 26.0 fixtures), from the documented wire-shape
   comments in the parser being fuzzed (e.g. `nvmeof.go`'s `25.10+: "subnqn"
   is the NQN (was "nqn" pre-25.10)` comments), or from
   `docs/reference/truenas-api-map.md` / `truenas-api-methods.json`. Seeds
   invented without grounding in a real shape mostly waste fuzzing time
   re-discovering the same shallow cases the Go corpus minimizer already
   covers.
2. **Corpus files Go itself writes** under `testdata/fuzz/<FuzzName>/` when
   `-fuzz` finds an input that increases coverage, and — critically — when it
   finds a **crasher** (a failing input). A crasher file is a permanent
   regression test: it is replayed by every future `go test` run (fuzzing or
   not) for that target, so once found and committed, that exact bug can
   never silently come back.

When you find a crash or invariant violation, **do not delete or hand-edit
the generated corpus file** — commit it as-is. If you want a smaller/cleaner
regression pin for the same bug, add a hand-minimized `f.Add(...)` seed
*in addition to*, not instead of, the raw corpus file.

## Adding a new target

1. **Confirm the input actually reaches parsing logic.** Fuzzing a function
   three layers removed from the wire (e.g. something that only receives
   already-validated Go structs) mostly burns CPU. The two surfaces that
   matter here are the TrueNAS JSON-RPC responses (`pkg/truenas/*.go`) and
   host command stdout (`pkg/util/{nvme,iscsi,mount}.go`).
2. **Prefer a pure function.** If the real parsing logic is inline inside a
   function that also shells out or does I/O (e.g. `exec.Command(...).Output()`
   followed by a parse loop), extract the parse loop into its own unexported,
   side-effect-free function — see `parseSubsysJSON` (extracted from
   `listNVMeSubsystems`) and `parseISCSISessionLines` (extracted from
   `getISCSISessions`) for the pattern. This is a pure, behavior-preserving
   extraction, not a logic change: same regex/same branches, just callable
   without a subprocess.
3. **Pick a real oracle, not just "does not panic."** Panic-freedom is the
   floor, not the target. Prefer, in order:
   - **Differential**: two independent decode paths must agree (see the
     `...MatchesInterface` targets — typed struct decode vs. the legacy
     `interface{}` decode).
   - **Round-trip / idempotence**: parse → reserialize → reparse is a fixed
     point (`FuzzParseBlkidExportOutput`), or `f(f(x)) == f(x)`
     (`FuzzNormalizeSCSIWWID`, `FuzzCanonicalISCSIPortalForComparison`).
   - **Safety property**: for anything feeding an ownership/deletion
     decision, the property is "malformed or ambiguous input never yields a
     confident OWNED/local verdict" — see `FuzzDatasetHasLocalOwnershipStampFailsClosed`
     and `FuzzParseDatasetResourceOwnershipSafety`. Fail-closed is the
     invariant; a fuzz failure here means garbage input produced a confident
     "yes, delete this" and must be reported loudly, not quietly patched by
     whoever is triaging fuzz output.
4. **Seed from something real** (see above), run it for at least 60s locally
   before committing (`-fuzztime=60s`), and commit whatever
   `testdata/fuzz/<FuzzName>/` ends up containing.
5. **Keep it fast and hermetic.** No network, no filesystem outside
   `t.TempDir()`, no sleeps, no shared mutable package state across fuzz
   iterations unless it is genuinely meant to persist (e.g.
   `FuzzJobDispatcherOfferNeverPanics` intentionally reuses one
   `jobDispatcher` across iterations rather than spinning its goroutine up
   and down every call).

## What is deliberately not fuzzed

- **TLS/auth/connection-pool state machines** (`client.go`'s reconnect,
  circuit breaker, semaphore admission): these are concurrency/timing
  properties, not parsing, and are already covered by the `_race_test.go` /
  `_connection_loss_test.go` suite under `-race`. Fuzzing does not exercise
  timing.
- **`findmnt`/`GetFilesystemType` output**: reduced to a single
  `strings.TrimSpace` after the subprocess call — there is no parsing logic
  to fuzz once `/proc/self/mounts` (already covered by `FuzzParseProcMounts`)
  fails to resolve.
- **CSI gRPC request validation** (`controller.go`, `node.go` request
  structs): these come from the Kubernetes external-provisioner/attacher over
  a Unix socket, not from TrueNAS or host tools, and are typed protobuf
  messages, not free-form text/JSON reaching a hand-rolled parser.
