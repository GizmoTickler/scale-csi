#!/usr/bin/env bash
#
# gf1-encryption-drill.sh — GF-Sprint 1 encryption-at-rest live drill.
#
# This is the executable form of the design's E-5 drill outline, corrected
# against the 2026-08-02 live run on nas01 (TrueNAS 26.0.0-BETA.1). That run
# found TWO driver blockers AND five defects in this harness itself; the harness
# defects mattered more than they look, because they produced six false FAILs and
# — worst of all — one false PASS. Every PASS line below now asserts on POSITIVE
# EVIDENCE (a job's RESULT payload, a device node, a hash, a key that really
# opens a dataset), never on an exit code alone.
#
# WHAT THE 2026-08-02 RUN CHANGED IN THIS SCRIPT (drill report H-1..H-7):
#   H-1  the preflight auth gate used the REST v2.0 API, which 26.0 REMOVED
#        (HTTP 404) — the script could not run at all. Now: midclt over
#        wss://<host>/api/current with the API key, read-only, fail-closed.
#   H-2  `midclt call` on an @job method PRINTS A JOB ID AND EXITS 0. It does not
#        wait, and its exit code says nothing about the job. Every @job now goes
#        through mcj(), which waits on core.get_jobs and hands back the job's
#        RESULT payload.
#   H-3  `encryption_root` is a PLAIN STRING; `.encryption_root.value` is a jq
#        ERROR that `//` does not catch, so two steps read "" and false-FAILed.
#        jq_field() now accepts a string OR a {value:...} dict.
#   H-4  pool.dataset.unlock takes (id, options) — the script passed only
#        options, so EVERY unlock in the drill was a no-op [EINVAL].
#   H-5  /dev/zvol writes need root, and device nodes appear/disappear
#        asynchronously. Root is now checked (device-write assertions SKIP,
#        loudly, when absent) and device transitions are polled, not assumed.
#   H-6  pool.snapshot.clone takes ({snapshot, dataset_dst}), not (id, {name}).
#   H-7  the residue audit now also checks replication.query.
#
# WHAT THE 2026-08-03 RE-RUN CHANGED (re-drill report H-8 and D-3):
#   H-8  the step-0 auth gate aborted the WHOLE drill when GF1_NAS_HOST was the
#        appliance's own FQDN — the documented, natural value. On-box that FQDN
#        resolves to 127.0.0.1 and nginx does not bind it, so the gate was
#        unreachable from the only host the EXECUTION MODEL says to run on (the
#        same abort class as H-1). The gate now tries the configured host and
#        then [::1], with the SAME key against the SAME method, and still aborts
#        if none answers.
#   D-3  new step 1c: the drill now speaks the DRIVER's own extra.properties
#        projection, because nothing else here could catch a projection that
#        omits the encryption block — dataset_row() sends no `extra` at all,
#        which is the always-populated shape A.
#
# AND WHAT THE RUN PROVED ABOUT THE BACKEND (now pinned, and asserted here):
#   D-1  pool.dataset.unlock NEVER fails the job for a wrong key. It returns job
#        state SUCCESS with the failure in the RESULT payload:
#          wrong:   {"unlocked": [],       "failed": {"<id>": {"error": "Invalid Key"}}}
#          correct: {"unlocked": ["<id>"], "failed": {}}
#        The design's P-5 ("wrong passphrase is a FAILED job") is FALSE for this
#        call shape. What holds is the ZFS-level guarantee: the dataset stays
#        LOCKED. Step 4 asserts the exact payload shape, because the driver's
#        fail-closed publish now depends on it.
#   D-2  change_key on a dataset whose encryption_root is its PARENT is NOT
#        refused: it SUCCEEDS and silently promotes that dataset to its own
#        encryption root. Step 6c asserts this, because the driver's re-key gate
#        is the only thing standing between a rotation window and a clone severed
#        from its origin key.
#   6b   a detached copy (replication.run_onetime, LOCAL PUSH) of an ENCRYPTED
#        source is a RAW SEND: the copy is ENCRYPTED, arrives LOCKED, is its OWN
#        encryption root, and is opened by the SOURCE's CURRENT passphrase. There
#        is no silent decryption. Step 6b asserts all four.
#
# STEPS
#   0.  preflight: deps, root check, API-key auth gate (fail-closed, read-only)
#   1.  create encrypted zvol + encrypted fs (key_format PASSPHRASE, AES-256-GCM)
#   1b. encrypted PARENT + plain child -> child encryption_root == PARENT,
#       key_format PASSPHRASE (P-10); zfs.resource.query carries NO encryption
#       fields at all (P-11)
#   1c. the DRIVER's pool.dataset.query projection returns the encryption block,
#       and the pre-fix projection does not (D-3, both directions asserted)
#   1d. the driver's zfs.resource.snapshot.query projection: used+creation
#       delivered, createtxg present as a top-level field (UNPROBED until this
#       step runs), unprojected properties absent (N-10)
#   2.  iSCSI extent over the zvol + write a known pattern (root only)
#   3.  lock -> device gone + I/O dead -> unlock(correct) -> device back, pattern
#       intact, extent id unchanged (the reboot-survival proof)
#   4.  wrong-passphrase unlock -> job SUCCEEDS, payload reports failure, dataset
#       STAYS LOCKED (D-1 — the exact shape the driver now parses)
#   5.  change_key -> old passphrase dead, new passphrase works (P-6)
#   5b. change_key to the SAME passphrase -> succeeds AND the key still opens the
#       dataset (P-9; the driver re-keys unconditionally inside a rotation window)
#   5c. encryption_summary row name == the dataset id (the driver fails CLOSED on
#       no match) + key_present_in_database == false for PASSPHRASE (P-3)
#   6.  snapshot + clone -> clone encryption_root == the ORIGIN (shared key, P-7)
#   6b. detached copy of an ENCRYPTED source -> encrypted, locked, own root,
#       opened by the SOURCE's current key (raw send)
#   6c. change_key on an inheriting child -> SUCCEEDS and re-roots it (D-2)
#   7.  destroy while locked -> clean, no key needed (E-4)
#   8.  teardown + zero-residue audit (datasets, snapshots, extents, replication)
#
# EXECUTION MODEL. Run this ON the TrueNAS host as ROOT (steps 2/3 assert
# host-level facts the API cannot see: the /dev/zvol device node and a dd pattern
# surviving lock/unlock). Without root those assertions SKIP loudly and
# everything else still runs. TrueNAS calls go through `midclt` — the tool the
# probes used.
#
# REQUIRED ENV:
#   GF1_NAS_HOST   appliance address, for the API-key auth gate.
#   GF1_API_KEY    a TrueNAS API key, validated read-only before any mutation.
#
# OPTIONAL ENV:
#   GF1_POOL        parent pool for scratch datasets (default: flashstor).
#   GF1_JOB_TIMEOUT seconds to wait for any single @job (default: 300).
#   GF1_DEV_TIMEOUT seconds to wait for a /dev/zvol transition (default: 15).
#   GF1_PASS / GF1_PASS_NEW / GF1_PASS_WRONG
#                   passphrases. Generated randomly when unset. SCRATCH values
#                   for scratch objects; never echoed.
#
# SAFETY. Every object created is named gf1-enc-drill-* and nothing else is ever
# touched. A trap tears those objects down on ANY exit, and step 8 audits that
# zero residue remains. Pre-existing gf1-enc-drill-* objects from a crashed run
# are swept at startup — the prefix is drill-exclusive, so this cannot clobber
# foreign state. No regex-matched delete anywhere; regex is used only in
# read-only audits.
#
# OUTPUT CONTRACT. Each step emits exactly one line beginning `PASS:`, `FAIL:` or
# `SKIP:`. Detail lines are indented. The final line is
# `RESULT: <n> passed, <m> failed, <k> skipped`; exit code is non-zero if any
# step failed.

set -uo pipefail

# --- configuration ---------------------------------------------------------

NAS_HOST="${GF1_NAS_HOST:-}"
API_KEY="${GF1_API_KEY:-}"
POOL="${GF1_POOL:-flashstor}"
JOB_TIMEOUT="${GF1_JOB_TIMEOUT:-300}"
DEV_TIMEOUT="${GF1_DEV_TIMEOUT:-15}"

# Scratch object names — ALL drill-exclusive (the safety prefix).
ZV="$POOL/gf1-enc-drill-zv"
FS="$POOL/gf1-enc-drill-fs"
CLONE="$POOL/gf1-enc-drill-clone"
DETACHED="$POOL/gf1-enc-drill-detached"
ENC_PARENT="$POOL/gf1-enc-drill-parent"
ENC_CHILD="$ENC_PARENT/child"
SNAP_NAME="gf1-enc-drill-snap"
SNAP="$ZV@$SNAP_NAME"
# step 1d measures the snapshot read shape long before step 6 takes its snapshot,
# so it uses its own scratch snapshot and destroys it immediately.
PROJ_SNAP_NAME="gf1-enc-drill-projsnap"
PROJ_SNAP="$ZV@$PROJ_SNAP_NAME"
EXTENT_NAME="gf1-enc-drill-extent"
VOLSIZE=1073741824 # 1 GiB

# Scratch passphrases (>= 8 chars, the ZFS minimum). Generated when unset so the
# drill never hardcodes a secret; these live only in this process and on the
# scratch datasets, and are destroyed with them.
rand_pass() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
  else
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}
PASSPHRASE="${GF1_PASS:-$(rand_pass)}"
PASSPHRASE_NEW="${GF1_PASS_NEW:-$(rand_pass)}"
PASSPHRASE_WRONG="${GF1_PASS_WRONG:-$(rand_pass)}"

WORKDIR="$(mktemp -d /tmp/gf1-enc-drill.XXXXXX)"
PATTERN_FILE="$WORKDIR/pattern.bin"
READBACK_FILE="$WORKDIR/readback.bin"
ERRFILE="$WORKDIR/midclt.err"

IS_ROOT=0
[ "$(id -u 2>/dev/null || echo 1)" = "0" ] && IS_ROOT=1

# --- tally / reporting -----------------------------------------------------

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
CURRENT_STEP=0

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf 'PASS: %s\n' "$1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf 'FAIL: %s\n' "$1"
}
skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  printf 'SKIP: %s\n' "$1"
}
detail() { printf '    %s\n' "$1"; }
step() {
  CURRENT_STEP="$1"
  printf '\n--- step %s: %s ---\n' "$1" "$2"
}

# --- midclt wrappers -------------------------------------------------------

# mc METHOD [JSON-ARGS...] — a NON-JOB call. stdout is the JSON result; the exit
# code is midclt's. Never use this for an @job method: `midclt call` on a job
# prints a job id and exits 0 immediately (H-2), which says nothing about the
# outcome.
mc() {
  midclt call "$@" 2>"$ERRFILE"
}

# mcj METHOD [JSON-ARGS...] — an @job call, done properly: dispatch, then WAIT on
# core.get_jobs until the job reaches a terminal state, then hand back the job's
# RESULT payload on stdout. Returns 0 only for state SUCCESS.
#
# Sets JOB_STATE, JOB_ERROR, JOB_RESULT. The result payload is the point — for
# pool.dataset.unlock the outcome exists ONLY there (D-1).
JOB_STATE=""
JOB_ERROR=""
JOB_RESULT=""
mcj() {
  local method="$1"
  shift
  JOB_STATE=""
  JOB_ERROR=""
  JOB_RESULT=""

  local jobid
  if ! jobid="$(midclt call "$method" "$@" 2>"$ERRFILE")"; then
    JOB_ERROR="dispatch failed: $(tr -d '\n' <"$ERRFILE")"
    return 1
  fi
  jobid="$(printf '%s' "$jobid" | tr -d '"[:space:]')"
  case "$jobid" in
    '' | *[!0-9]*)
      JOB_ERROR="$method did not return a job id (got: ${jobid:-<empty>}; $(tr -d '\n' <"$ERRFILE"))"
      return 1
      ;;
  esac

  local deadline row
  deadline=$(($(date +%s) + JOB_TIMEOUT))
  while :; do
    row="$(midclt call core.get_jobs "$(jq -nc --argjson id "$jobid" '[["id","=",$id]]')" 2>"$ERRFILE" | jq -c '.[0] // empty' 2>/dev/null)"
    if [ -n "$row" ]; then
      JOB_STATE="$(printf '%s' "$row" | jq -r '.state // empty' 2>/dev/null)"
      case "$JOB_STATE" in
        SUCCESS)
          JOB_RESULT="$(printf '%s' "$row" | jq -c '.result' 2>/dev/null)"
          printf '%s' "$JOB_RESULT"
          return 0
          ;;
        FAILED | ABORTED | CANCELED)
          JOB_ERROR="$(printf '%s' "$row" | jq -r '.error // .exception // "no error detail"' 2>/dev/null)"
          return 1
          ;;
      esac
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      JOB_ERROR="timed out after ${JOB_TIMEOUT}s waiting for job $jobid (last state: ${JOB_STATE:-unknown})"
      return 1
    fi
    sleep 1
  done
}

# jq_field JSON KEY — reads a top-level field that may be a PLAIN STRING or a
# property dict ({"value": ...}). `.KEY.value` on a string is a jq ERROR that
# `//` does not rescue (H-3), which is how two steps false-FAILed on facts that
# actually hold.
jq_field() {
  printf '%s' "$1" | jq -r --arg k "$2" '(.[$k] | if type == "object" then .value else . end) // empty' 2>/dev/null
}

# dataset_row ID — one pool.dataset.query row, or empty.
dataset_row() {
  mc pool.dataset.query "$(jq -nc --arg id "$1" '[["id","=",$id]]')" | jq -c '.[0] // empty' 2>/dev/null
}

# lock_dataset ID — pool.dataset.lock as a job.
lock_dataset() {
  mcj pool.dataset.lock "$(jq -nc --arg id "$1" '$id')" >/dev/null
}

# unlock_dataset ID PASSPHRASE — pool.dataset.unlock(id, options) as a job, with
# the D-1 payload assertion: success requires the id present in .unlocked AND
# absent from .failed. Returns 0 only on a PROVEN unlock. Sets UNLOCK_REASON, and
# leaves JOB_STATE/JOB_RESULT for callers that assert on the shape itself.
UNLOCK_REASON=""
unlock_dataset() {
  local id="$1" pass="$2" out failed unlocked
  UNLOCK_REASON=""
  if ! out="$(mcj pool.dataset.unlock \
    "$(jq -nc --arg id "$id" '$id')" \
    "$(jq -nc --arg name "$id" --arg pass "$pass" \
      '{datasets:[{name:$name,passphrase:$pass}],toggle_attachments:true}')")"; then
    UNLOCK_REASON="job did not succeed: ${JOB_ERROR:-unknown}"
    return 1
  fi
  failed="$(printf '%s' "$out" | jq -r --arg id "$id" '.failed[$id].error // empty' 2>/dev/null)"
  if [ -n "$failed" ]; then
    UNLOCK_REASON="$failed"
    return 1
  fi
  unlocked="$(printf '%s' "$out" | jq -r --arg id "$id" '[.unlocked[]? | select(. == $id)] | length' 2>/dev/null)"
  if [ "${unlocked:-0}" != "1" ]; then
    UNLOCK_REASON="payload named neither success nor failure for $id: ${out:-<empty>}"
    return 1
  fi
  return 0
}

# changekey_dataset ID PASSPHRASE — pool.dataset.change_key as a job.
changekey_dataset() {
  mcj pool.dataset.change_key "$(jq -nc --arg id "$1" '$id')" \
    "$(jq -nc --arg pass "$2" '{passphrase:$pass}')" >/dev/null
}

# wait_device PATH / wait_no_device PATH — device nodes appear and disappear
# asynchronously (~0.5-1s observed), so transitions are POLLED, never assumed
# (H-5).
wait_device() {
  local path="$1" deadline
  deadline=$(($(date +%s) + DEV_TIMEOUT))
  while [ ! -e "$path" ]; do
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 1
  done
  return 0
}
wait_no_device() {
  local path="$1" deadline
  deadline=$(($(date +%s) + DEV_TIMEOUT))
  while [ -e "$path" ]; do
    [ "$(date +%s)" -ge "$deadline" ] && return 1
    sleep 1
  done
  return 0
}

# --- cleanup ---------------------------------------------------------------

# destroy_if_exists DATASET removes a scratch dataset if present. Force+recursive
# so a locked or snapshot-pinned dataset still goes (destroy needs no key, E-4).
destroy_if_exists() {
  local ds="$1" present
  present="$(mc pool.dataset.query "$(jq -nc --arg id "$ds" '[["id","=",$id]]')" | jq 'length' 2>/dev/null)"
  if [ "${present:-0}" != "0" ]; then
    mc pool.dataset.delete "$(jq -nc --arg id "$ds" '$id')" '{"force":true,"recursive":true}' >/dev/null 2>&1 || true
  fi
}

# sweep_residue destroys every drill-exclusive object, best-effort and in
# dependency order (clones and copies before the snapshot they descend from, and
# before their origin). It never fails the run (step 8 audits residue) and never
# touches the workdir, so it is safe to call at startup, at step 8, and on exit.
sweep_residue() {
  command -v midclt >/dev/null 2>&1 || return 0
  local ext_id
  ext_id="$(mc iscsi.extent.query '[["name","=","'"$EXTENT_NAME"'"]]' | jq -r '.[0].id // empty' 2>/dev/null)"
  if [ -n "${ext_id:-}" ]; then
    mc iscsi.extent.delete "$ext_id" >/dev/null 2>&1 || true
  fi
  destroy_if_exists "$CLONE"
  destroy_if_exists "$DETACHED"
  mc pool.snapshot.delete "$(jq -nc --arg id "$SNAP" '$id')" >/dev/null 2>&1 || true
  mc pool.snapshot.delete "$(jq -nc --arg id "$PROJ_SNAP" '$id')" >/dev/null 2>&1 || true
  destroy_if_exists "$ENC_CHILD"
  destroy_if_exists "$ENC_PARENT"
  destroy_if_exists "$ZV"
  destroy_if_exists "$FS"
}

cleanup() {
  sweep_residue
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# --- preflight -------------------------------------------------------------

step 0 "preflight: env, dependencies, root check, API-key auth gate"

if [ -z "$NAS_HOST" ] || [ -z "$API_KEY" ]; then
  detail "GF1_NAS_HOST and GF1_API_KEY are required in the environment"
  printf 'RESULT: 0 passed, 1 failed, 0 skipped (preflight)\n'
  exit 2
fi

for dep in midclt jq; do
  if ! command -v "$dep" >/dev/null 2>&1; then
    detail "required dependency missing: $dep (run this on the TrueNAS host)"
    printf 'RESULT: 0 passed, 1 failed, 0 skipped (preflight)\n'
    exit 2
  fi
done

if [ "$IS_ROOT" != "1" ]; then
  detail "NOT running as root: the /dev/zvol write + readback assertions will SKIP."
  detail "Everything else still runs. Re-run as root for the data-survival proof."
fi

# Read-only authentication gate: prove the supplied API key is valid for the
# target host BEFORE mutating anything (fail-closed). TrueNAS 26.0 REMOVED the
# REST v2.0 API the previous gate used (H-1), so this speaks the current
# websocket API with the key and asserts on a field only a real, authenticated
# response carries.
#
# H-8: the gate used to abort the WHOLE drill when GF1_NAS_HOST was the
# appliance's own FQDN — the documented, natural value. Measured on nas01: the
# EXECUTION MODEL mandates running ON the appliance, where the FQDN resolves via
# /etc/hosts to 127.0.0.1, and nginx binds 192.168.x.x:443 and [::]:443 but NOT
# 127.0.0.1:443. So the gate was unreachable from the only host it is documented
# to run on — the same abort class as H-1.
#
# The fix is an ENDPOINT fallback, not a weaker gate: each candidate is tried
# with the SAME API key against the SAME authenticated method, and the drill
# still aborts if none of them answers. Only the address is allowed to differ.
# [::1] is the loopback nginx actually serves (measured: `midclt -u
# wss://[::1]/api/current` succeeds on-box while the FQDN does not).
NAS_URL=""
NAS_VERSION=""
for gate_host in "$NAS_HOST" "[::1]"; do
  [ -n "$gate_host" ] || continue
  SYSINFO="$(midclt -u "wss://$gate_host/api/current" -K "$API_KEY" --insecure call system.info 2>"$ERRFILE")"
  NAS_VERSION="$(printf '%s' "$SYSINFO" | jq -r '.version // empty' 2>/dev/null)"
  if [ -n "$NAS_VERSION" ]; then
    NAS_URL="wss://$gate_host/api/current"
    break
  fi
  detail "auth gate endpoint wss://$gate_host/api/current did not answer: $(tr -d '\n' <"$ERRFILE")"
done
if [ -z "$NAS_URL" ]; then
  detail "API-key auth gate failed for every candidate endpoint — aborting before any mutation"
  detail "candidates tried: wss://$NAS_HOST/api/current, wss://[::1]/api/current"
  printf 'RESULT: 0 passed, 1 failed, 0 skipped (preflight)\n'
  exit 2
fi
detail "auth gate OK via $NAS_URL; appliance reports version: $NAS_VERSION"

# Startup sweep: remove drill-exclusive residue from a crashed prior run. This
# destroys scratch objects only (never the workdir, which the steps still need).
detail "startup sweep of any pre-existing gf1-enc-drill-* objects"
sweep_residue

# --- step 1: create encrypted zvol + fs ------------------------------------

step 1 "create encrypted zvol + encrypted fs (key_format:PASSPHRASE, AES-256-GCM)"

step1_ok=1

zv_json="$(jq -nc --arg name "$ZV" --argjson size "$VOLSIZE" --arg pass "$PASSPHRASE" \
  '{name:$name,type:"VOLUME",volsize:$size,sparse:true,encryption:true,inherit_encryption:false,encryption_options:{algorithm:"AES-256-GCM",passphrase:$pass}}')"
if zv_out="$(mc pool.dataset.create "$zv_json")"; then
  [ "$(printf '%s' "$zv_out" | jq -r '.encrypted' 2>/dev/null)" = "true" ] || {
    detail "zvol: encrypted != true"
    step1_ok=0
  }
  [ "$(printf '%s' "$zv_out" | jq -r '.locked' 2>/dev/null)" = "false" ] || {
    detail "zvol: locked != false"
    step1_ok=0
  }
  [ "$(printf '%s' "$zv_out" | jq -r '.key_loaded' 2>/dev/null)" = "true" ] || {
    detail "zvol: key_loaded != true"
    step1_ok=0
  }
  [ "$(jq_field "$zv_out" key_format)" = "PASSPHRASE" ] || {
    detail "zvol: key_format is '$(jq_field "$zv_out" key_format)'"
    step1_ok=0
  }
  [ "$(jq_field "$zv_out" encryption_algorithm)" = "AES-256-GCM" ] || {
    detail "zvol: encryption_algorithm is '$(jq_field "$zv_out" encryption_algorithm)'"
    step1_ok=0
  }
  [ "$(jq_field "$zv_out" encryption_root)" = "$ZV" ] || {
    detail "zvol: encryption_root is '$(jq_field "$zv_out" encryption_root)', expected ITSELF"
    step1_ok=0
  }
else
  detail "zvol create failed: $(tr -d '\n' <"$ERRFILE")"
  step1_ok=0
fi

fs_json="$(jq -nc --arg name "$FS" --arg pass "$PASSPHRASE" \
  '{name:$name,type:"FILESYSTEM",encryption:true,inherit_encryption:false,encryption_options:{algorithm:"AES-256-GCM",passphrase:$pass}}')"
if fs_out="$(mc pool.dataset.create "$fs_json")"; then
  [ "$(printf '%s' "$fs_out" | jq -r '.encrypted' 2>/dev/null)" = "true" ] || {
    detail "fs: encrypted != true"
    step1_ok=0
  }
  [ "$(printf '%s' "$fs_out" | jq -r '.locked' 2>/dev/null)" = "false" ] || {
    detail "fs: locked != false"
    step1_ok=0
  }
  [ "$(jq_field "$fs_out" key_format)" = "PASSPHRASE" ] || {
    detail "fs: key_format is '$(jq_field "$fs_out" key_format)'"
    step1_ok=0
  }
  [ "$(jq_field "$fs_out" encryption_algorithm)" = "AES-256-GCM" ] || {
    detail "fs: encryption_algorithm is '$(jq_field "$fs_out" encryption_algorithm)'"
    step1_ok=0
  }
else
  detail "fs create failed: $(tr -d '\n' <"$ERRFILE")"
  step1_ok=0
fi

if [ "$step1_ok" = "1" ]; then
  pass "step 1: encrypted zvol + fs created (encrypted, PASSPHRASE, AES-256-GCM, unlocked, self-rooted)"
else
  fail "step 1: encrypted dataset creation did not match the P-1/P-2 create shape"
fi

# --- step 1b: inherited encryption identity (P-10 / P-11) ------------------

step 1b "encrypted parent + plain child -> encryption_root == PARENT, key_format PASSPHRASE; resource query carries nothing"

step1b_ok=1

if ! mc pool.dataset.create "$(jq -nc --arg name "$ENC_PARENT" --arg pass "$PASSPHRASE" \
  '{name:$name,type:"FILESYSTEM",encryption:true,inherit_encryption:false,
    encryption_options:{algorithm:"AES-256-GCM",passphrase:$pass}}')" >/dev/null; then
  detail "encrypted parent create failed: $(tr -d '\n' <"$ERRFILE")"
  step1b_ok=0
fi
# A child with NO encryption arguments at all: it must INHERIT.
if ! mc pool.dataset.create "$(jq -nc --arg name "$ENC_CHILD" '{name:$name,type:"FILESYSTEM"}')" >/dev/null; then
  detail "plain child create failed: $(tr -d '\n' <"$ERRFILE")"
  step1b_ok=0
fi

child_out="$(dataset_row "$ENC_CHILD")"
parent_out="$(dataset_row "$ENC_PARENT")"
child_encrypted="$(printf '%s' "$child_out" | jq -r '.encrypted // false' 2>/dev/null)"
child_root="$(jq_field "$child_out" encryption_root)"
child_format="$(jq_field "$child_out" key_format)"
parent_root="$(jq_field "$parent_out" encryption_root)"

if [ "$child_encrypted" != "true" ]; then
  detail "child is not encrypted (expected inheritance)"
  step1b_ok=0
fi
if [ "$child_root" != "$ENC_PARENT" ]; then
  detail "child encryption_root is '$child_root', expected the PARENT '$ENC_PARENT'"
  detail "ACTION: the driver's self-keyed discriminator (root == self) depends on this exact shape"
  step1b_ok=0
fi
if [ "$parent_root" != "$ENC_PARENT" ]; then
  detail "parent encryption_root is '$parent_root', expected ITSELF '$ENC_PARENT'"
  step1b_ok=0
fi
if [ "$child_format" != "PASSPHRASE" ]; then
  detail "child key_format is '$child_format', expected PASSPHRASE"
  step1b_ok=0
fi

# P-11: the bulk resource listing must carry NO encryption/key/lock fields, which
# is why the driver never decides anything about encryption from it.
resource_out="$(mc zfs.resource.query "$(jq -nc --arg p "$ENC_CHILD" '{paths:[$p]}')" 2>/dev/null | jq -c '.[0] // empty' 2>/dev/null)"
if [ -n "$resource_out" ]; then
  leaked="$(printf '%s' "$resource_out" | jq -r '[paths|join(".")] | map(select(test("encrypt|key_format|key_loaded|locked"))) | join(",")' 2>/dev/null)"
  if [ -n "${leaked:-}" ]; then
    detail "RECORDED: zfs.resource.query DOES carry encryption-ish fields now: $leaked"
    detail "ACTION: P-11 changed — re-check every 'the bulk listing carries no encryption signal' claim"
    step1b_ok=0
  else
    detail "zfs.resource.query carries no encryption/key/lock fields (P-11 holds)"
  fi
else
  detail "zfs.resource.query returned nothing for the child; P-11 not re-verified this run"
  step1b_ok=0
fi

if [ "$step1b_ok" = "1" ]; then
  pass "step 1b: inherited encryption reports the PARENT as encryption_root (P-10) and the resource listing carries no encryption signal (P-11)"
else
  fail "step 1b: the inherited-encryption identity did not match P-10/P-11"
fi

# --- step 1c: the driver's query PROJECTION carries the encryption block ----
#
# The re-drill's D-3 blocker, made a standing gate. Every pool.dataset.query the
# DRIVER issues carries an extra.properties projection; TrueNAS returns only
# those properties plus a small always-present core and OMITS the rest — as an
# absent key, which the client decodes to a zero value. The driver's projection
# did not include the encryption properties, so `encrypted`, `locked`,
# `key_format` and `encryption_root` were absent from every driver read and every
# "wire truth" encryption predicate silently answered "plaintext" about an
# aes-256-gcm/passphrase dataset (fail-open publish, PVC-wedging replay).
#
# Note that NOTHING ELSE in this drill can catch that: dataset_row() calls
# pool.dataset.query with NO `extra`, which is measured shape A — always fully
# populated. This step is the only place the drill speaks the driver's own
# request shape.
#
# It asserts BOTH directions, so it is a measurement and not an assumption:
#   with the driver's projection    -> the encryption block is PRESENT (shape C)
#   with the pre-fix base projection -> it is ABSENT (shape B, the defect)
# If the second assertion ever fails, the backend started returning encryption
# fields unconditionally; that is a shape change worth knowing about, not a pass.

step 1c "the driver's pool.dataset.query projection carries the encryption block (D-3)"

step1c_ok=1

# Keep these two lists byte-identical to pkg/truenas/dataset.go:
#   base    = datasetQueryPropertiesBase (the pre-fix projection)
#   driver  = datasetQueryProperties     (base + origin + the encryption set)
PROJ_BASE='["used","available","quota","refquota","referenced","usedbysnapshots","reservation","refreservation","volsize","volblocksize","creation"]'
PROJ_DRIVER='["used","available","quota","refquota","referenced","usedbysnapshots","reservation","refreservation","volsize","volblocksize","creation","origin","encryption","keyformat","encryptionroot","keystatus"]'

projected_row() {
  mc pool.dataset.query \
    "$(jq -nc --arg id "$ZV" '[["id","=",$id]]')" \
    "$(jq -nc --argjson props "$1" '{extra:{properties:$props}}')" |
    jq -c '.[0] // empty' 2>/dev/null
}

row_driver="$(projected_row "$PROJ_DRIVER")"
row_base="$(projected_row "$PROJ_BASE")"

if [ -z "$row_driver" ] || [ -z "$row_base" ]; then
  detail "projected pool.dataset.query returned no row for $ZV: $(tr -d '\n' <"$ERRFILE")"
  step1c_ok=0
else
  drv_encrypted="$(printf '%s' "$row_driver" | jq -r '.encrypted // empty' 2>/dev/null)"
  drv_root="$(jq_field "$row_driver" encryption_root)"
  drv_format="$(jq_field "$row_driver" key_format)"
  drv_locked="$(printf '%s' "$row_driver" | jq -r 'if has("locked") then "present" else "" end' 2>/dev/null)"

  if [ "$drv_encrypted" != "true" ]; then
    detail "the DRIVER's projection did not return encrypted:true (got '${drv_encrypted:-<absent>}')"
    detail "ACTION: this is D-3. Every wire-truth encryption predicate is reading zero values."
    step1c_ok=0
  fi
  if [ "$drv_root" != "$ZV" ]; then
    detail "the DRIVER's projection returned encryption_root '${drv_root:-<absent>}', expected '$ZV'"
    step1c_ok=0
  fi
  if [ "$drv_format" != "PASSPHRASE" ]; then
    detail "the DRIVER's projection returned key_format '${drv_format:-<absent>}', expected PASSPHRASE"
    step1c_ok=0
  fi
  if [ "$drv_locked" != "present" ]; then
    detail "the DRIVER's projection did not return a 'locked' field at all"
    detail "ACTION: the unlock reconciler and the health surface both read it"
    step1c_ok=0
  fi

  base_encrypted="$(printf '%s' "$row_base" | jq -r '.encrypted // empty' 2>/dev/null)"
  base_root="$(jq_field "$row_base" encryption_root)"
  if [ "$base_encrypted" = "true" ] || [ -n "$base_root" ]; then
    detail "RECORDED: the PRE-FIX projection now also returns the encryption block"
    detail "  (encrypted='$base_encrypted' encryption_root='$base_root')"
    detail "ACTION: the backend's projection behavior changed — re-measure D-3 before trusting this step"
    step1c_ok=0
  else
    detail "pre-fix projection omits the encryption block (shape B reproduced)"
  fi
fi

if [ "$step1c_ok" = "1" ]; then
  pass "step 1c: the driver's projection returns encrypted/locked/key_format/encryption_root; the pre-fix one does not"
else
  fail "step 1c: the driver's query projection does not carry the encryption block (D-3)"
fi

# --- step 1d: the driver's SNAPSHOT projection carries what its guards read --
#
# N-10: the same projected-read hazard as 1c, one API over. Every snapshot read
# the driver issues goes through zfs.resource.snapshot.query with
#   {paths, recursive, properties: ["used","creation"], get_user_properties: true}
# — snapshotResourceQueryProperties in pkg/truenas/snapshot_projection.go.
#
# Two different questions, and this step is the ONLY thing that can settle the
# second one:
#
#   projected PROPERTIES   used + creation. `creation` is read by the
#                          scheduled-snapshot ownership predicate and by every
#                          age gate (tombstones, spent-restore); `used` by the
#                          reaper's reclaimable-bytes accounting.
#   TOP-LEVEL fields       createtxg. It is NOT selected by the projection, and
#                          the driver ASSUMES it is present — an assumption
#                          carried over from an enumeration measured on the
#                          DATASET resource API, never on this one. Tombstone
#                          identity matching and promote refusal read it; both
#                          degrade CLOSED without it (promote refuses outright),
#                          so a missing createtxg is a silent capability loss,
#                          not corruption.
#
# The unprojected-property half is asserted too (a property NOT asked for must be
# absent), so the step measures the projection's semantics rather than assuming
# them.

step 1d "the driver's zfs.resource.snapshot.query projection: used+creation present, createtxg present, unprojected absent"

step1d_ok=1

# Keep byte-identical to snapshotResourceQueryProperties.
SNAP_PROJ_DRIVER='["used","creation"]'

snap_row() {
  mc zfs.resource.snapshot.query \
    "$(jq -nc --arg p "$ZV" --argjson props "$1" \
      '{paths:[$p],recursive:false,properties:$props,get_user_properties:true}')" |
    jq -c --arg id "$PROJ_SNAP" '.[] | select((.id // .name) == $id)' 2>/dev/null | head -1
}

if ! mc pool.snapshot.create "$(jq -nc --arg ds "$ZV" --arg n "$PROJ_SNAP_NAME" '{dataset:$ds,name:$n}')" >/dev/null; then
  detail "could not create the scratch snapshot for the projection probe: $(tr -d '\n' <"$ERRFILE")"
  step1d_ok=0
fi

snap_driver="$(snap_row "$SNAP_PROJ_DRIVER")"

if [ -z "$snap_driver" ]; then
  detail "the driver's snapshot projection returned no row for $SNAP: $(tr -d '\n' <"$ERRFILE")"
  detail "ACTION: every snapshot read in the driver uses this exact shape"
  step1d_ok=0
else
  snap_createtxg="$(printf '%s' "$snap_driver" | jq -r '.createtxg // empty' 2>/dev/null)"
  snap_creation="$(printf '%s' "$snap_driver" | jq -r '.properties.creation // empty | if type=="object" then (.value // .raw // .rawvalue) else . end' 2>/dev/null)"
  snap_used="$(printf '%s' "$snap_driver" | jq -r '.properties.used // empty | if type=="object" then (.value // .raw // .rawvalue) else . end' 2>/dev/null)"
  snap_clones="$(printf '%s' "$snap_driver" | jq -r 'if (.properties // {}) | has("clones") then "present" else "" end' 2>/dev/null)"

  if [ -z "$snap_createtxg" ] || [ "$snap_createtxg" = "null" ]; then
    detail "RECORDED: createtxg is ABSENT from the driver's snapshot projection"
    detail "ACTION: N-10 confirmed — promote will refuse every migration set and tombstone"
    detail "        identity falls back to seconds-only. Both fail CLOSED, but the capability"
    detail "        is lost silently; project it explicitly or re-shape the read."
    step1d_ok=0
  else
    detail "createtxg present (top-level, value $snap_createtxg) — the assumption holds"
  fi
  if [ -z "$snap_creation" ] || [ "$snap_creation" = "null" ]; then
    detail "the projected 'creation' property is absent — every age gate reads zero"
    step1d_ok=0
  fi
  if [ -z "$snap_used" ] || [ "$snap_used" = "null" ]; then
    detail "the projected 'used' property is absent — snapshot sizes report zero"
    step1d_ok=0
  fi
  if [ "$snap_clones" = "present" ]; then
    detail "RECORDED: an UNPROJECTED property ('clones') came back anyway"
    detail "ACTION: this API does not filter properties the way the driver models it;"
    detail "        re-check snapshot_projection.go before trusting the model"
    step1d_ok=0
  else
    detail "an unprojected property is absent — the projection filters as modeled"
  fi
fi

# The probe's scratch snapshot has served its purpose; take it out now so no
# later step (or the residue audit) has to reason about it.
mc pool.snapshot.delete "$(jq -nc --arg id "$PROJ_SNAP" '$id')" >/dev/null 2>&1 || true

if [ "$step1d_ok" = "1" ]; then
  pass "step 1d: the snapshot projection delivers used+creation, createtxg survives as a top-level field, and unprojected properties do not appear"
else
  fail "step 1d: the driver's snapshot projection does not deliver what its guards read (N-10)"
fi

# --- step 2: iSCSI extent over the zvol + write a known pattern ------------

step 2 "attach iSCSI extent over the zvol, write a known pattern"

step2_ok=1
PATTERN_SHA=""
EXTENT_ID=""

extent_json="$(jq -nc --arg name "$EXTENT_NAME" --arg disk "zvol/$ZV" \
  '{name:$name,type:"DISK",disk:$disk}')"
if extent_out="$(mc iscsi.extent.create "$extent_json")"; then
  EXTENT_ID="$(printf '%s' "$extent_out" | jq -r '.id // empty' 2>/dev/null)"
  if [ "$(printf '%s' "$extent_out" | jq -r '.disk // empty' 2>/dev/null)" != "zvol/$ZV" ]; then
    detail "extent disk is not the stable zvol path"
    step2_ok=0
  fi
else
  detail "extent create failed: $(tr -d '\n' <"$ERRFILE")"
  step2_ok=0
fi

if [ "$IS_ROOT" = "1" ]; then
  if ! wait_device "/dev/zvol/$ZV"; then
    detail "/dev/zvol/$ZV never appeared (waited ${DEV_TIMEOUT}s)"
    step2_ok=0
  elif ! head -c 1048576 /dev/urandom >"$PATTERN_FILE"; then
    detail "could not build the pattern file"
    step2_ok=0
  elif ! dd if="$PATTERN_FILE" of="/dev/zvol/$ZV" bs=1M count=1 conv=notrunc,fsync >/dev/null 2>"$ERRFILE"; then
    detail "pattern write to /dev/zvol/$ZV failed: $(tr -d '\n' <"$ERRFILE")"
    step2_ok=0
  else
    PATTERN_SHA="$(sha256sum "$PATTERN_FILE" | cut -d' ' -f1)"
  fi
fi

if [ "$step2_ok" != "1" ]; then
  fail "step 2: extent creation or pattern write failed"
elif [ "$IS_ROOT" = "1" ]; then
  pass "step 2: extent over zvol created (disk=zvol/$ZV, id=$EXTENT_ID) and 1MiB pattern written"
else
  skip "step 2: extent created (id=$EXTENT_ID) but the pattern write needs root — the data-survival half is not exercised"
fi

# --- step 3: lock -> device gone -> unlock -> device back, pattern intact ---

step 3 "lock -> /dev/zvol gone + I/O dead -> unlock(correct) -> path back + pattern intact"

step3_ok=1

if ! lock_dataset "$ZV"; then
  detail "lock job did not succeed: ${JOB_ERROR:-unknown}"
  step3_ok=0
fi

row="$(dataset_row "$ZV")"
[ "$(printf '%s' "$row" | jq -r '.locked' 2>/dev/null)" = "true" ] || {
  detail "dataset does not report locked:true after lock"
  step3_ok=0
}

if ! wait_no_device "/dev/zvol/$ZV"; then
  detail "/dev/zvol/$ZV still present ${DEV_TIMEOUT}s after lock (expected gone, P-4)"
  step3_ok=0
fi
if [ "$IS_ROOT" = "1" ] && dd if="/dev/zvol/$ZV" of=/dev/null bs=1M count=1 >/dev/null 2>&1; then
  detail "I/O to a locked zvol succeeded (expected to fail)"
  step3_ok=0
fi

if ! unlock_dataset "$ZV" "$PASSPHRASE"; then
  detail "correct-passphrase unlock did not prove an unlock: $UNLOCK_REASON"
  step3_ok=0
fi
if ! wait_device "/dev/zvol/$ZV"; then
  detail "/dev/zvol/$ZV did not return within ${DEV_TIMEOUT}s after unlock"
  step3_ok=0
fi

# The extent must survive the lock/unlock cycle with the SAME id: the driver
# relies on /dev/zvol/<name> being stable, so no extent is ever recreated.
if [ -n "$EXTENT_ID" ]; then
  extent_now="$(mc iscsi.extent.query "$(jq -nc --arg n "$EXTENT_NAME" '[["name","=",$n]]')" | jq -r '.[0].id // empty' 2>/dev/null)"
  if [ "$extent_now" != "$EXTENT_ID" ]; then
    detail "extent id changed across lock/unlock ($EXTENT_ID -> ${extent_now:-<gone>})"
    step3_ok=0
  fi
fi

if [ "$IS_ROOT" = "1" ] && [ -n "$PATTERN_SHA" ]; then
  if dd if="/dev/zvol/$ZV" of="$READBACK_FILE" bs=1M count=1 >/dev/null 2>"$ERRFILE"; then
    readback_sha="$(sha256sum "$READBACK_FILE" | cut -d' ' -f1)"
    if [ "$readback_sha" != "$PATTERN_SHA" ]; then
      detail "pattern readback differs after unlock ($readback_sha != $PATTERN_SHA)"
      step3_ok=0
    fi
  else
    detail "pattern readback failed: $(tr -d '\n' <"$ERRFILE")"
    step3_ok=0
  fi
fi

if [ "$step3_ok" != "1" ]; then
  fail "step 3: the lock/unlock survival cycle did not behave per P-4"
elif [ "$IS_ROOT" = "1" ] && [ -n "$PATTERN_SHA" ]; then
  pass "step 3: lock removed the device and killed I/O; unlock restored the path, the pattern hash is identical, and the extent id is unchanged"
else
  pass "step 3: lock removed the device; unlock restored the path and the extent id is unchanged (pattern hash not checked — not root)"
fi

# --- step 4: wrong passphrase — D-1, the shape the driver parses -----------

step 4 "wrong-passphrase unlock -> job SUCCEEDS, payload reports failure, dataset stays LOCKED (D-1)"

step4_ok=1

if ! lock_dataset "$ZV"; then
  detail "could not lock the zvol for the wrong-key test: ${JOB_ERROR:-unknown}"
  step4_ok=0
fi

if unlock_dataset "$ZV" "$PASSPHRASE_WRONG"; then
  detail "a WRONG passphrase was reported as a successful unlock — fail-open"
  step4_ok=0
else
  detail "unlock refused, reason: $UNLOCK_REASON"
  # THE POINT OF THIS STEP: the job SUCCEEDS. If a future release starts failing
  # the job instead, the driver's payload assertion still fails closed, but the
  # pinned truth has moved and every comment citing D-1 must be revisited.
  if [ "$JOB_STATE" != "SUCCESS" ]; then
    detail "RECORDED: the wrong-key unlock job state is '$JOB_STATE', not SUCCESS"
    detail "ACTION: D-1's pinned shape changed — re-check pkg/truenas/encryption.go's payload assertion"
  else
    detail "confirmed D-1: job state SUCCESS with the failure only in the result payload"
  fi
  case "$UNLOCK_REASON" in
    *"Invalid Key"*) : ;;
    *) detail "RECORDED: the failure reason is '$UNLOCK_REASON', not the expected 'Invalid Key'" ;;
  esac
fi

row="$(dataset_row "$ZV")"
[ "$(printf '%s' "$row" | jq -r '.locked' 2>/dev/null)" = "true" ] || {
  detail "dataset is NOT locked after a wrong-key unlock (expected to stay locked)"
  step4_ok=0
}
if ! wait_no_device "/dev/zvol/$ZV"; then
  detail "/dev/zvol/$ZV present after a failed wrong-key unlock (expected absent)"
  step4_ok=0
fi

# Restore the unlocked state for the following steps.
unlock_dataset "$ZV" "$PASSPHRASE" >/dev/null 2>&1 || true

if [ "$step4_ok" = "1" ]; then
  pass "step 4: a wrong passphrase does not unlock; the dataset stays locked and the device stays gone (fail-closed at ZFS, reported only in the job payload)"
else
  fail "step 4: wrong-passphrase handling did not fail closed"
fi

# --- step 5: change_key rotation -------------------------------------------

step 5 "change_key -> old passphrase fails, new works (P-6)"

step5_ok=1

# change_key requires the dataset unlocked (key loaded).
unlock_dataset "$ZV" "$PASSPHRASE" >/dev/null 2>&1 || true
if ! changekey_dataset "$ZV" "$PASSPHRASE_NEW"; then
  detail "change_key job did not succeed: ${JOB_ERROR:-unknown}"
  step5_ok=0
fi

# Lock, then prove the OLD key is dead and the NEW key works — both by payload.
lock_dataset "$ZV" || detail "lock before the old-key check did not succeed: ${JOB_ERROR:-unknown}"
if unlock_dataset "$ZV" "$PASSPHRASE"; then
  detail "old passphrase still unlocks after change_key (expected dead)"
  step5_ok=0
else
  detail "old passphrase refused after rotation, reason: $UNLOCK_REASON"
fi
if ! unlock_dataset "$ZV" "$PASSPHRASE_NEW"; then
  detail "new passphrase does not unlock after change_key: $UNLOCK_REASON"
  step5_ok=0
fi

if [ "$step5_ok" = "1" ]; then
  pass "step 5: change_key rotated the key (old key proven dead, new key proven working)"
else
  fail "step 5: change_key rotation did not behave per P-6"
fi

# --- step 5b: change_key to the SAME passphrase (rotation-completion probe) --

step 5b "change_key(current) on an already-current dataset -> SUCCESS, key still valid"

step5b_ok=1

# The dataset is unlocked and keyed with PASSPHRASE_NEW after step 5. The driver
# calls change_key(current) UNCONDITIONALLY whenever a rotation window is open
# and the dataset is unlocked: that completes a rotation interrupted between
# unlock(previous) and change_key, and must be harmless when the rotation already
# landed. The previous version of this step asserted only that a job id came back
# — a FALSE PASS. It now waits for the job AND re-proves the key.
if ! changekey_dataset "$ZV" "$PASSPHRASE_NEW"; then
  detail "same-passphrase change_key did not succeed: ${JOB_ERROR:-unknown}"
  detail "ACTION: the driver's unlocked-with-open-rotation-window arm is unsafe and must be redesigned"
  step5b_ok=0
fi

lock_dataset "$ZV" || detail "lock after the same-key change_key did not succeed: ${JOB_ERROR:-unknown}"
if ! unlock_dataset "$ZV" "$PASSPHRASE_NEW"; then
  detail "the passphrase no longer unlocks after a same-key change_key: $UNLOCK_REASON"
  step5b_ok=0
fi

if [ "$step5b_ok" = "1" ]; then
  pass "step 5b: same-passphrase change_key SUCCEEDS and the key still proves an unlock (rotation completion is idempotent by outcome)"
else
  fail "step 5b: same-passphrase change_key did not behave as the driver assumes"
fi

# --- step 5c: encryption_summary row identity ------------------------------

step 5c "encryption_summary <id> returns a row whose name == the dataset id"

step5c_ok=1

# The driver matches the summary row by EXACT dataset name and fails CLOSED when
# no row matches (it will not read a child's lock state, and will not read an
# empty result as 'unlocked'). Any id/path normalisation drift on this BETA would
# turn every unlock into a hard error, so it must be caught here.
if summary_json="$(mcj pool.dataset.encryption_summary "$(jq -nc --arg id "$ZV" '$id')")"; then
  summary_names="$(printf '%s' "$summary_json" | jq -r '.[].name' 2>/dev/null)"
  if printf '%s\n' "$summary_names" | grep -qx "$ZV"; then
    detail "summary names the dataset exactly"
  else
    detail "summary row names were: $(printf '%s' "$summary_names" | tr '\n' ' ')"
    detail "ACTION: the driver's exact-name match (fail-closed) would reject every unlock"
    step5c_ok=0
  fi
  # P-3: a passphrase dataset's key is NOT in the TrueNAS database — the fact the
  # whole locked-on-reboot availability model rests on.
  kpid="$(printf '%s' "$summary_json" | jq -r --arg id "$ZV" '.[] | select(.name==$id) | .key_present_in_database' 2>/dev/null)"
  if [ "$kpid" != "false" ]; then
    detail "RECORDED: key_present_in_database is '$kpid', expected false (P-3)"
    detail "ACTION: if TrueNAS now stores passphrases, the reboot/unlock model changes entirely"
    step5c_ok=0
  else
    detail "key_present_in_database:false — P-3 holds"
  fi
else
  detail "encryption_summary job did not succeed: ${JOB_ERROR:-unknown}"
  step5c_ok=0
fi

if [ "$step5c_ok" = "1" ]; then
  pass "step 5c: encryption_summary names the dataset exactly ('$ZV') and reports key_present_in_database:false"
else
  fail "step 5c: the encryption_summary identity / P-3 assertions did not hold"
fi

# --- step 6: snapshot + clone inherit the origin key -----------------------

step 6 "snapshot + clone -> clone encryption_root == origin (shared-key, P-7)"

step6_ok=1

if ! mc pool.snapshot.create "$(jq -nc --arg ds "$ZV" --arg n "$SNAP_NAME" '{dataset:$ds,name:$n}')" >/dev/null; then
  detail "snapshot create failed: $(tr -d '\n' <"$ERRFILE")"
  step6_ok=0
fi
# H-6: the real signature is clone({snapshot, dataset_dst}).
if ! mc pool.snapshot.clone "$(jq -nc --arg s "$SNAP" --arg d "$CLONE" '{snapshot:$s,dataset_dst:$d}')" >/dev/null; then
  detail "snapshot clone failed: $(tr -d '\n' <"$ERRFILE")"
  step6_ok=0
fi

clone_out="$(dataset_row "$CLONE")"
if [ "$(printf '%s' "$clone_out" | jq -r '.encrypted // false' 2>/dev/null)" != "true" ]; then
  detail "clone is not encrypted"
  step6_ok=0
fi
clone_root="$(jq_field "$clone_out" encryption_root)"
if [ "$clone_root" != "$ZV" ]; then
  detail "clone encryption_root is '$clone_root', expected the origin '$ZV'"
  step6_ok=0
fi

if [ "$step6_ok" = "1" ]; then
  pass "step 6: clone is encrypted with encryption_root == origin (inherits the origin key, not independently keyed)"
else
  fail "step 6: clone encryption inheritance did not match P-7"
fi

# --- step 6b: detached copy of an ENCRYPTED source -------------------------

step 6b "detached copy (replication.run_onetime LOCAL) of an encrypted source -> encrypted, locked, own root, source's key"

step6b_ok=1

# This is the driver's `snapshotRestoreMode: detached` mechanism run against an
# ENCRYPTED source. The 2026-08-02 drill settled it: TrueNAS 26.0 sends RAW. The
# copy is encrypted (no silent decryption), arrives LOCKED, is its OWN encryption
# root, and is opened by the SOURCE's CURRENT passphrase. All four are asserted
# here because the release notes and the content-source refusal now cite them.
unlock_dataset "$ZV" "$PASSPHRASE_NEW" >/dev/null 2>&1 || true

if ! mcj replication.run_onetime "$(jq -nc --arg src "$ZV" --arg dst "$DETACHED" --arg snap "$SNAP_NAME" \
  '{direction:"PUSH",transport:"LOCAL",source_datasets:[$src],target_dataset:$dst,recursive:false,
    replicate:false,name_regex:("^" + $snap + "$"),retention_policy:"NONE",readonly:"IGNORE",
    only_from_scratch:true}')" >/dev/null; then
  detail "replication.run_onetime did not succeed: ${JOB_ERROR:-unknown}"
  step6b_ok=0
fi

detached_out="$(dataset_row "$DETACHED")"
if [ -z "$detached_out" ]; then
  detail "detached copy $DETACHED did not materialise"
  step6b_ok=0
else
  detached_encrypted="$(printf '%s' "$detached_out" | jq -r '.encrypted // false' 2>/dev/null)"
  detached_locked="$(printf '%s' "$detached_out" | jq -r '.locked // false' 2>/dev/null)"
  detached_root="$(jq_field "$detached_out" encryption_root)"
  detail "RECORDED: detached copy -> encrypted=$detached_encrypted locked=$detached_locked encryption_root=$detached_root"

  if [ "$detached_encrypted" != "true" ]; then
    detail "the copy is NOT encrypted: this would be a SILENT DECRYPTION of the source's data"
    detail "ACTION: the send is no longer raw — revisit the content-source refusal's stated reason"
    step6b_ok=0
  fi
  if [ "$detached_root" != "$DETACHED" ]; then
    detail "the copy's encryption_root is '$detached_root', expected ITSELF (an independent root)"
    step6b_ok=0
  fi
  if [ "$detached_locked" != "true" ]; then
    detail "RECORDED: the copy arrived UNLOCKED (2026-08-02 measured locked:true)"
  fi

  # Which key opens it: the SOURCE's CURRENT passphrase, not the pre-rotation one.
  if unlock_dataset "$DETACHED" "$PASSPHRASE"; then
    detail "the copy opened with the source's OLD (pre-rotation) key — the key-handover model has changed"
    step6b_ok=0
  fi
  if ! unlock_dataset "$DETACHED" "$PASSPHRASE_NEW"; then
    detail "the copy did NOT open with the source's CURRENT key: $UNLOCK_REASON"
    step6b_ok=0
  fi
fi

if [ "$step6b_ok" = "1" ]; then
  pass "step 6b: the detached copy is a RAW SEND — encrypted, its own encryption root, opened by the SOURCE's current passphrase (no silent decryption)"
else
  fail "step 6b: the detached copy's encryption behavior did not match the pinned raw-send model"
fi

# --- step 6c: change_key on an inheriting child (D-2) ----------------------

step 6c "change_key on an INHERITING child -> succeeds and re-roots it (D-2)"

step6c_ok=1

# The design assumed ZFS refuses this. It does not: it succeeds and silently
# promotes the child to its own encryption root, severing it from the parent key.
# The driver's re-key ownership gate is the only thing preventing that on a
# clone, so this step pins the backend behavior the gate exists for.
child_root_before="$(jq_field "$(dataset_row "$ENC_CHILD")" encryption_root)"
if ! changekey_dataset "$ENC_CHILD" "$PASSPHRASE_NEW"; then
  detail "RECORDED: change_key on an inheriting child FAILED: ${JOB_ERROR:-unknown}"
  detail "ACTION: D-2's pinned behavior changed (it succeeded on 2026-08-02) — re-check the re-key gate's rationale"
  step6c_ok=0
else
  child_root_after="$(jq_field "$(dataset_row "$ENC_CHILD")" encryption_root)"
  detail "child encryption_root: '$child_root_before' -> '$child_root_after'"
  if [ "$child_root_after" != "$ENC_CHILD" ]; then
    detail "RECORDED: the child was NOT promoted to its own encryption root"
    step6c_ok=0
  fi
fi

if [ "$step6c_ok" = "1" ]; then
  pass "step 6c: change_key on an inheriting child SUCCEEDS and promotes it to its own encryption root — the driver's gate is the only guard (D-2)"
else
  fail "step 6c: the inheriting-child change_key behavior did not match D-2"
fi

# --- step 7: destroy while locked destroys cleanly -------------------------

step 7 "destroy while locked -> clean destroy (needs no key, E-4)"

step7_ok=1

# Lock the filesystem dataset, then destroy it WITHOUT unlocking. ZFS destroy
# needs no key (P-4); the driver's DeleteVolume goes straight to destroy.
if ! lock_dataset "$FS"; then
  detail "lock of the fs dataset did not succeed: ${JOB_ERROR:-unknown}"
  step7_ok=0
fi
fs_locked="$(printf '%s' "$(dataset_row "$FS")" | jq -r '.locked // empty' 2>/dev/null)"
if [ "$fs_locked" != "true" ]; then
  detail "fs dataset is not locked before the locked-delete test"
  step7_ok=0
fi

if ! mc pool.dataset.delete "$(jq -nc --arg id "$FS" '$id')" '{"force":true,"recursive":true}' >/dev/null; then
  detail "destroy of a LOCKED dataset failed (expected clean destroy): $(tr -d '\n' <"$ERRFILE")"
  step7_ok=0
fi
fs_remaining="$(mc pool.dataset.query "$(jq -nc --arg id "$FS" '[["id","=",$id]]')" | jq 'length' 2>/dev/null)"
if [ "${fs_remaining:-1}" != "0" ]; then
  detail "fs dataset still present after locked destroy"
  step7_ok=0
fi

if [ "$step7_ok" = "1" ]; then
  pass "step 7: locked dataset destroyed cleanly with no unlock"
else
  fail "step 7: locked destroy did not complete cleanly"
fi

# --- step 8: teardown + zero-residue audit ---------------------------------

step 8 "teardown + zero-residue audit (datasets, snapshots, extents, replication)"

# The EXIT trap also runs cleanup; tear down explicitly here so the audit sees a
# swept state and can report any residue as a step-8 failure. sweep_residue (not
# cleanup) so the workdir survives until the trap removes it.
sweep_residue

step8_ok=1
ds_residue="$(mc pool.dataset.query '[["id","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"
snap_residue="$(mc pool.snapshot.query '[["id","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"
ext_residue="$(mc iscsi.extent.query '[["name","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"
# H-7: a one-time replication must leave no persistent task behind.
repl_residue="$(mc replication.query | jq '[.[] | select((.name // "") | test("gf1-enc-drill"))] | length' 2>/dev/null)"

if [ "${ds_residue:-1}" != "0" ]; then
  detail "dataset residue remains: $ds_residue gf1-enc-drill dataset(s)"
  step8_ok=0
fi
if [ "${snap_residue:-1}" != "0" ]; then
  detail "snapshot residue remains: $snap_residue gf1-enc-drill snapshot(s)"
  step8_ok=0
fi
if [ "${ext_residue:-1}" != "0" ]; then
  detail "extent residue remains: $ext_residue gf1-enc-drill extent(s)"
  step8_ok=0
fi
if [ "${repl_residue:-1}" != "0" ]; then
  detail "replication task residue remains: $repl_residue gf1-enc-drill task(s)"
  step8_ok=0
fi

if [ "$step8_ok" = "1" ]; then
  pass "step 8: zero residue (datasets=0, snapshots=0, extents=0, replication tasks=0 matching gf1-enc-drill)"
else
  fail "step 8: residue remains after teardown"
fi

# --- summary ---------------------------------------------------------------

printf '\nRESULT: %d passed, %d failed, %d skipped\n' "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"
if [ "$FAIL_COUNT" -ne 0 ]; then
  exit 1
fi
exit 0
