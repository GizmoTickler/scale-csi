#!/usr/bin/env bash
#
# gf1-encryption-drill.sh — GF-Sprint 1 encryption-at-rest live drill.
#
# This is the executable form of the design's E-5 drill outline (which the §0
# nas01 probes ARE). It exercises ZFS-native encryption against a real TrueNAS
# appliance and prints a machine-checkable PASS/FAIL line per step. It is the
# outline the live-drill agent runs before GA/merge (risk R8: the backend is a
# BETA and the API shapes are pinned to the probe date).
#
# EIGHT STEPS (verbatim from E-5):
#   1. create encrypted zvol + encrypted fs (assert key_format:PASSPHRASE, AES-256-GCM)
#   2. attach iSCSI extent over the zvol, write a known pattern
#   3. lock -> assert /dev/zvol gone + I/O dead -> unlock(correct) -> assert path
#      back + pattern intact (reboot-survival proof; extent survives, NO recreation)
#   4. wrong-passphrase unlock -> assert FAILED + stays locked (fail-closed, P-5)
#   5. change_key -> assert old passphrase fails, new works (P-6)
#   6. snapshot + clone -> assert clone encryption_root == origin (shared-key, P-7)
#   7. DeleteVolume while locked -> assert clean destroy (needs no key, E-4)
#   8. teardown + zero-residue audit (pool.dataset.query [id ~ gf1-enc-drill] == 0)
#
# EXECUTION MODEL. Run this ON the TrueNAS host (copy it there, or pipe it via
# `ssh <nas01> 'GF1_NAS_HOST=... GF1_API_KEY=... bash -s' < gf1-encryption-drill.sh`).
# It must run on-box because steps 2/3 assert host-level facts the API cannot see
# (the /dev/zvol device node appearing/disappearing, a dd pattern surviving
# lock/unlock). TrueNAS calls go through `midclt` — the exact tool the §0 probes
# used — which waits for @jobs synchronously and exits non-zero on a FAILED job
# (the native fail-closed signal).
#
# REQUIRED ENV:
#   GF1_NAS_HOST   the appliance address (used for the upfront API-key auth gate
#                  and to confirm we are pointed at the right box).
#   GF1_API_KEY    a TrueNAS API key. Used for a read-only REST authentication
#                  gate (GET /api/v2.0/system/info) that MUST succeed before any
#                  mutation: bad credential -> abort before touching anything.
#
# OPTIONAL ENV:
#   GF1_POOL       parent pool for the scratch datasets (default: flashstor, the
#                  probed pool).
#   GF1_SCHEME     http|https for the auth gate (default: https).
#   GF1_PORT       API port for the auth gate (default: 443).
#   GF1_TLS_VERIFY 1 to verify TLS on the auth gate (default: 0 = -k, a lab box).
#   GF1_PASS / GF1_PASS_NEW / GF1_PASS_WRONG
#                  passphrases. Generated randomly when unset. These are SCRATCH
#                  values for scratch objects, never real credentials; the script
#                  never echoes them.
#
# SAFETY. Every object this script creates is named gf1-enc-drill-* and nothing
# else is ever touched. A trap tears those objects down on ANY exit, and step 8
# audits that zero residue remains. If gf1-enc-drill-* objects already exist at
# startup (a crashed prior run), the startup sweep removes them first — the
# prefix is drill-exclusive, so this cannot clobber foreign state.
#
# OUTPUT CONTRACT. Each step emits exactly one line beginning `PASS:` or `FAIL:`
# (grep '^PASS:' / '^FAIL:' for the machine-readable summary). Detail lines are
# indented. The final line is `RESULT: <n> passed, <m> failed`; exit code is
# non-zero if any step failed.

set -uo pipefail

# --- configuration ---------------------------------------------------------

NAS_HOST="${GF1_NAS_HOST:-}"
API_KEY="${GF1_API_KEY:-}"
POOL="${GF1_POOL:-flashstor}"
SCHEME="${GF1_SCHEME:-https}"
PORT="${GF1_PORT:-443}"
TLS_VERIFY="${GF1_TLS_VERIFY:-0}"

# Scratch object names — ALL drill-exclusive (the safety prefix).
ZV="$POOL/gf1-enc-drill-zv"
FS="$POOL/gf1-enc-drill-fs"
CLONE="$POOL/gf1-enc-drill-clone"
SNAP_NAME="gf1-enc-drill-snap"
SNAP="$ZV@$SNAP_NAME"
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

# --- tally / reporting -----------------------------------------------------

PASS_COUNT=0
FAIL_COUNT=0
CURRENT_STEP=0

pass() { PASS_COUNT=$((PASS_COUNT + 1)); printf 'PASS: %s\n' "$1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); printf 'FAIL: %s\n' "$1"; }
detail() { printf '    %s\n' "$1"; }
step() { CURRENT_STEP="$1"; printf '\n--- step %s: %s ---\n' "$1" "$2"; }

# --- midclt wrapper --------------------------------------------------------

# mc METHOD [JSON-ARGS...] runs a midclt call, capturing stdout. Returns the
# midclt exit code: 0 on success, non-zero on a FAILED job (the fail-closed
# signal the drill asserts on). midclt progress/chatter goes to stderr and is
# captured in ERRFILE for diagnostics, never interleaved with the JSON result.
mc() {
  midclt call "$@" 2>"$ERRFILE"
}

# jq_get JSON EXPR extracts a field from a midclt result for assertions.
jq_get() { printf '%s' "$1" | jq -r "$2" 2>/dev/null; }

# --- cleanup ---------------------------------------------------------------

# destroy_if_exists DATASET removes a scratch dataset if present. Force+recursive
# so a locked or snapshot-pinned dataset still goes (destroy needs no key, E-4).
destroy_if_exists() {
  local ds="$1"
  local present
  present="$(mc pool.dataset.query "$(jq -nc --arg id "$ds" '[["id","=",$id]]')" | jq 'length' 2>/dev/null)"
  if [ "${present:-0}" != "0" ]; then
    mc pool.dataset.delete "$(jq -nc --arg id "$ds" '$id')" '{"force":true,"recursive":true}' >/dev/null || true
  fi
}

# sweep_residue destroys every drill-exclusive object, best-effort and in
# dependency order. It never fails the run (step 8 audits residue) and never
# touches the workdir, so it is safe to call at startup, at step 8, and on exit.
sweep_residue() {
  command -v midclt >/dev/null 2>&1 || return 0
  local ext_id
  ext_id="$(mc iscsi.extent.query '[["name","=","'"$EXTENT_NAME"'"]]' | jq -r '.[0].id // empty' 2>/dev/null)"
  if [ -n "${ext_id:-}" ]; then
    mc iscsi.extent.delete "$ext_id" >/dev/null 2>&1 || true
  fi
  mc pool.snapshot.delete "$(jq -nc --arg id "$SNAP" '$id')" >/dev/null 2>&1 || true
  destroy_if_exists "$CLONE"
  destroy_if_exists "$ZV"
  destroy_if_exists "$FS"
}

cleanup() {
  sweep_residue
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

# --- preflight -------------------------------------------------------------

step 0 "preflight: env, dependencies, API-key auth gate"

if [ -z "$NAS_HOST" ] || [ -z "$API_KEY" ]; then
  detail "GF1_NAS_HOST and GF1_API_KEY are required in the environment"
  printf 'RESULT: 0 passed, 1 failed (preflight)\n'
  exit 2
fi

for dep in midclt jq curl; do
  if ! command -v "$dep" >/dev/null 2>&1; then
    detail "required dependency missing: $dep (run this on the TrueNAS host)"
    printf 'RESULT: 0 passed, 1 failed (preflight)\n'
    exit 2
  fi
done

# Read-only authentication gate: prove the supplied API key is valid for the
# target host BEFORE mutating anything (fail-closed). Also confirms the box.
CURL_TLS=()
if [ "$TLS_VERIFY" != "1" ]; then CURL_TLS=(-k); fi
# ${CURL_TLS[@]+...} guards the empty-array expansion under `set -u` on older
# bash, where a bare "${CURL_TLS[@]}" on an empty array is an unbound variable.
SYSINFO_HTTP="$(curl ${CURL_TLS[@]+"${CURL_TLS[@]}"} -s -o "$WORKDIR/sysinfo.json" -w '%{http_code}' \
  -H "Authorization: Bearer $API_KEY" \
  "$SCHEME://$NAS_HOST:$PORT/api/v2.0/system/info" || true)"
if [ "$SYSINFO_HTTP" != "200" ]; then
  detail "API-key auth gate failed (HTTP ${SYSINFO_HTTP:-none}) for $SCHEME://$NAS_HOST:$PORT — aborting before any mutation"
  printf 'RESULT: 0 passed, 1 failed (preflight)\n'
  exit 2
fi
NAS_VERSION="$(jq_get "$(cat "$WORKDIR/sysinfo.json")" '.version // "unknown"')"
detail "auth gate OK; appliance reports version: $NAS_VERSION"

# Startup sweep: remove drill-exclusive residue from a crashed prior run. This
# destroys scratch objects only (never the workdir, which the steps still need).
detail "startup sweep of any pre-existing gf1-enc-drill-* objects"
sweep_residue

# --- step 1: create encrypted zvol + fs ------------------------------------

step 1 "create encrypted zvol + encrypted fs (key_format:PASSPHRASE, AES-256-GCM)"

step1_ok=1

zv_json="$(jq -nc --arg name "$ZV" --arg pass "$PASSPHRASE" --argjson vol "$VOLSIZE" \
  '{name:$name,type:"VOLUME",volsize:$vol,sparse:true,encryption:true,inherit_encryption:false,encryption_options:{algorithm:"AES-256-GCM",passphrase:$pass}}')"
if zv_out="$(mc pool.dataset.create "$zv_json")"; then
  for chk in '.encrypted==true' '.key_format.value=="PASSPHRASE"' '.encryption_algorithm.value=="AES-256-GCM"' '.locked==false' '.key_loaded==true'; do
    if [ "$(printf '%s' "$zv_out" | jq "$chk" 2>/dev/null)" != "true" ]; then
      detail "zvol assertion failed: $chk"
      step1_ok=0
    fi
  done
else
  detail "zvol create failed: $(cat "$ERRFILE")"
  step1_ok=0
fi

fs_json="$(jq -nc --arg name "$FS" --arg pass "$PASSPHRASE" \
  '{name:$name,type:"FILESYSTEM",encryption:true,inherit_encryption:false,encryption_options:{algorithm:"AES-256-GCM",passphrase:$pass}}')"
if fs_out="$(mc pool.dataset.create "$fs_json")"; then
  for chk in '.encrypted==true' '.key_format.value=="PASSPHRASE"' '.encryption_algorithm.value=="AES-256-GCM"' '.locked==false'; do
    if [ "$(printf '%s' "$fs_out" | jq "$chk" 2>/dev/null)" != "true" ]; then
      detail "fs assertion failed: $chk"
      step1_ok=0
    fi
  done
else
  detail "fs create failed: $(cat "$ERRFILE")"
  step1_ok=0
fi

if [ "$step1_ok" = "1" ]; then
  pass "step 1: encrypted zvol + fs created (encrypted, PASSPHRASE, AES-256-GCM, unlocked)"
else
  fail "step 1: encrypted dataset creation did not match the P-1/P-2 create shape"
fi

# --- step 2: iSCSI extent over the zvol + write a known pattern ------------

step 2 "attach iSCSI extent over the zvol, write a known pattern"

step2_ok=1

# A fixed, reproducible 1 MiB pattern so the post-unlock readback can be compared
# by hash. Written to the zvol device node while it is unlocked.
if ! head -c 1048576 /dev/urandom >"$PATTERN_FILE"; then
  detail "could not build the pattern file"
  step2_ok=0
fi
PATTERN_SHA="$(sha256sum "$PATTERN_FILE" | cut -d' ' -f1)"

extent_json="$(jq -nc --arg name "$EXTENT_NAME" --arg disk "zvol/$ZV" \
  '{name:$name,type:"DISK",disk:$disk}')"
if extent_out="$(mc iscsi.extent.create "$extent_json")"; then
  EXTENT_ID="$(jq_get "$extent_out" '.id // empty')"
  if [ "$(jq_get "$extent_out" '.disk // empty')" != "zvol/$ZV" ]; then
    detail "extent disk is not the stable zvol path"
    step2_ok=0
  fi
else
  detail "extent create failed: $(cat "$ERRFILE")"
  step2_ok=0
fi

# Write the pattern to offset 0 of the zvol backing device.
if ! dd if="$PATTERN_FILE" of="/dev/zvol/$ZV" bs=1M count=1 conv=notrunc,fsync >/dev/null 2>"$ERRFILE"; then
  detail "pattern write to /dev/zvol/$ZV failed: $(cat "$ERRFILE")"
  step2_ok=0
fi

if [ "$step2_ok" = "1" ]; then
  pass "step 2: extent over zvol created (disk=zvol/$ZV) and 1MiB pattern written"
else
  fail "step 2: extent creation or pattern write failed"
fi

# --- step 3: lock -> device gone + I/O dead -> unlock -> path back + intact -

step 3 "lock/unlock device-path survival + data intact (reboot-survival proof)"

step3_ok=1

if ! mc pool.dataset.lock "$(jq -nc --arg id "$ZV" '$id')" >/dev/null; then
  detail "lock failed: $(cat "$ERRFILE")"
  step3_ok=0
fi

# Locked: the block device node is GONE (P-4) and I/O fails.
if [ -e "/dev/zvol/$ZV" ]; then
  detail "/dev/zvol/$ZV still present after lock (expected gone)"
  step3_ok=0
fi
if dd if="/dev/zvol/$ZV" of=/dev/null bs=1M count=1 >/dev/null 2>&1; then
  detail "I/O succeeded on a locked zvol (expected failure)"
  step3_ok=0
fi
locked_state="$(mc pool.dataset.query "$(jq -nc --arg id "$ZV" '[["id","=",$id]]')" | jq -r '.[0].locked // empty' 2>/dev/null)"
if [ "$locked_state" != "true" ]; then
  detail "pool.dataset.query does not report locked=true after lock"
  step3_ok=0
fi

# Unlock with the correct passphrase: the stable path returns (P-4).
unlock_json="$(jq -nc --arg name "$ZV" --arg pass "$PASSPHRASE" \
  '{datasets:[{name:$name,passphrase:$pass}],toggle_attachments:true}')"
if ! mc pool.dataset.unlock "$unlock_json" >/dev/null; then
  detail "correct-passphrase unlock failed: $(cat "$ERRFILE")"
  step3_ok=0
fi
if [ ! -e "/dev/zvol/$ZV" ]; then
  detail "/dev/zvol/$ZV did not return after unlock"
  step3_ok=0
fi

# The extent survives with NO recreation and still references the stable path.
extent_disk="$(mc iscsi.extent.query '[["name","=","'"$EXTENT_NAME"'"]]' | jq -r '.[0].disk // empty' 2>/dev/null)"
if [ "$extent_disk" != "zvol/$ZV" ]; then
  detail "extent did not survive lock/unlock (disk=$extent_disk)"
  step3_ok=0
fi

# The pattern read back intact.
if dd if="/dev/zvol/$ZV" of="$READBACK_FILE" bs=1M count=1 >/dev/null 2>"$ERRFILE"; then
  READBACK_SHA="$(sha256sum "$READBACK_FILE" | cut -d' ' -f1)"
  if [ "$READBACK_SHA" != "$PATTERN_SHA" ]; then
    detail "pattern hash mismatch after unlock (data not intact)"
    step3_ok=0
  fi
else
  detail "pattern readback failed: $(cat "$ERRFILE")"
  step3_ok=0
fi

if [ "$step3_ok" = "1" ]; then
  pass "step 3: lock removed the device + killed I/O; correct unlock restored the stable path, the extent, and the data"
else
  fail "step 3: lock/unlock device-path survival or data integrity failed"
fi

# --- step 4: wrong-passphrase unlock fails closed --------------------------

step 4 "wrong-passphrase unlock -> FAILED + stays locked (fail-closed, P-5)"

step4_ok=1

# Re-lock so the wrong-key attempt starts from a locked dataset.
mc pool.dataset.lock "$(jq -nc --arg id "$ZV" '$id')" >/dev/null || true

wrong_json="$(jq -nc --arg name "$ZV" --arg pass "$PASSPHRASE_WRONG" \
  '{datasets:[{name:$name,passphrase:$pass}],toggle_attachments:true}')"
if mc pool.dataset.unlock "$wrong_json" >/dev/null 2>&1; then
  detail "wrong-passphrase unlock SUCCEEDED (expected FAILED)"
  step4_ok=0
fi

# Dataset must remain locked with no device.
locked_state="$(mc pool.dataset.query "$(jq -nc --arg id "$ZV" '[["id","=",$id]]')" | jq -r '.[0].locked // empty' 2>/dev/null)"
if [ "$locked_state" != "true" ]; then
  detail "dataset is not locked after a failed wrong-key unlock"
  step4_ok=0
fi
if [ -e "/dev/zvol/$ZV" ]; then
  detail "/dev/zvol/$ZV present after a failed wrong-key unlock (expected absent)"
  step4_ok=0
fi

# Restore state for later steps.
mc pool.dataset.unlock "$unlock_json" >/dev/null 2>&1 || true

if [ "$step4_ok" = "1" ]; then
  pass "step 4: wrong-passphrase unlock FAILED and the dataset stayed locked (fail-closed)"
else
  fail "step 4: wrong-passphrase unlock did not fail closed"
fi

# --- step 5: change_key rotation -------------------------------------------

step 5 "change_key -> old passphrase fails, new works (P-6)"

step5_ok=1

# change_key requires the dataset unlocked (key loaded).
mc pool.dataset.unlock "$unlock_json" >/dev/null 2>&1 || true
if ! mc pool.dataset.change_key "$(jq -nc --arg id "$ZV" '$id')" \
  "$(jq -nc --arg pass "$PASSPHRASE_NEW" '{passphrase:$pass}')" >/dev/null; then
  detail "change_key failed: $(cat "$ERRFILE")"
  step5_ok=0
fi

# Lock, then prove the OLD key is dead and the NEW key works.
mc pool.dataset.lock "$(jq -nc --arg id "$ZV" '$id')" >/dev/null || true

old_json="$unlock_json" # built with PASSPHRASE (the old key)
if mc pool.dataset.unlock "$old_json" >/dev/null 2>&1; then
  detail "old passphrase still unlocks after change_key (expected dead)"
  step5_ok=0
fi
mc pool.dataset.lock "$(jq -nc --arg id "$ZV" '$id')" >/dev/null 2>&1 || true

new_json="$(jq -nc --arg name "$ZV" --arg pass "$PASSPHRASE_NEW" \
  '{datasets:[{name:$name,passphrase:$pass}],toggle_attachments:true}')"
if ! mc pool.dataset.unlock "$new_json" >/dev/null; then
  detail "new passphrase does not unlock after change_key"
  step5_ok=0
fi

if [ "$step5_ok" = "1" ]; then
  pass "step 5: change_key rotated the key (old fails, new works)"
else
  fail "step 5: change_key rotation did not behave per P-6"
fi

# --- step 6: snapshot + clone inherit the origin key -----------------------

step 6 "snapshot + clone -> clone encryption_root == origin (shared-key, P-7)"

step6_ok=1

if ! mc pool.snapshot.create "$(jq -nc --arg ds "$ZV" --arg n "$SNAP_NAME" '{dataset:$ds,name:$n}')" >/dev/null; then
  detail "snapshot create failed: $(cat "$ERRFILE")"
  step6_ok=0
fi
if ! mc pool.snapshot.clone "$(jq -nc --arg id "$SNAP" '$id')" \
  "$(jq -nc --arg name "$CLONE" '{name:$name}')" >/dev/null; then
  detail "snapshot clone failed: $(cat "$ERRFILE")"
  step6_ok=0
fi

clone_out="$(mc pool.dataset.query "$(jq -nc --arg id "$CLONE" '[["id","=",$id]]')" | jq '.[0] // empty' 2>/dev/null)"
if [ "$(printf '%s' "$clone_out" | jq '.encrypted // empty' 2>/dev/null)" != "true" ]; then
  detail "clone is not encrypted"
  step6_ok=0
fi
clone_root="$(printf '%s' "$clone_out" | jq -r '.encryption_root.value // .encryption_root // empty' 2>/dev/null)"
if [ "$clone_root" != "$ZV" ]; then
  detail "clone encryption_root is '$clone_root', expected the origin '$ZV'"
  step6_ok=0
fi

if [ "$step6_ok" = "1" ]; then
  pass "step 6: clone is encrypted with encryption_root == origin (inherits the origin key, not independently keyed)"
else
  fail "step 6: clone encryption inheritance did not match P-7"
fi

# --- step 7: DeleteVolume while locked destroys cleanly --------------------

step 7 "DeleteVolume while locked -> clean destroy (needs no key, E-4)"

step7_ok=1

# Lock the filesystem dataset, then destroy it WITHOUT unlocking. ZFS destroy
# needs no key (P-4); the driver's DeleteVolume goes straight to destroy.
mc pool.dataset.lock "$(jq -nc --arg id "$FS" '$id')" >/dev/null || true
fs_locked="$(mc pool.dataset.query "$(jq -nc --arg id "$FS" '[["id","=",$id]]')" | jq -r '.[0].locked // empty' 2>/dev/null)"
if [ "$fs_locked" != "true" ]; then
  detail "fs dataset is not locked before the locked-delete test"
  step7_ok=0
fi

if ! mc pool.dataset.delete "$(jq -nc --arg id "$FS" '$id')" '{"force":true,"recursive":true}' >/dev/null; then
  detail "destroy of a LOCKED dataset failed (expected clean destroy): $(cat "$ERRFILE")"
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

step 8 "teardown + zero-residue audit (gf1-enc-drill query == 0)"

# The EXIT trap also runs cleanup; tear down explicitly here so the audit sees a
# swept state and can report any residue as a step-8 failure. sweep_residue (not
# cleanup) so the workdir survives until the trap removes it.
sweep_residue

step8_ok=1
ds_residue="$(mc pool.dataset.query '[["id","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"
snap_residue="$(mc pool.snapshot.query '[["id","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"
ext_residue="$(mc iscsi.extent.query '[["name","~","gf1-enc-drill"]]' | jq 'length' 2>/dev/null)"

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

if [ "$step8_ok" = "1" ]; then
  pass "step 8: zero residue (datasets=0, snapshots=0, extents=0 matching gf1-enc-drill)"
else
  fail "step 8: residue remains after teardown"
fi

# --- summary ---------------------------------------------------------------

printf '\nRESULT: %d passed, %d failed\n' "$PASS_COUNT" "$FAIL_COUNT"
if [ "$FAIL_COUNT" -ne 0 ]; then
  exit 1
fi
exit 0
