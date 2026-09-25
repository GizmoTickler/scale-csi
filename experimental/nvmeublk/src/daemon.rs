//! Per-node daemon: one process owns every nvmeublk device on the node and is
//! driven over a local control socket (scale-csi's node plugin is the client).
//!
//! Lifecycle:
//! - attach/detach create and delete devices; each device is served by its
//!   own queue threads (device.rs). Operations on one volume are serialised;
//!   operations on different volumes do not wait for each other. Their
//!   control commands run on op lanes (device::OP_LANES): at most four at
//!   once, plus one lane that only recovery may take.
//! - Every device is recorded in a state file under /run (so it survives a
//!   daemon restart but not a reboot, which also removes the devices).
//! - On start, every recorded device that still exists is reattached through
//!   ublk user recovery, in the background: the control socket serves at
//!   once, and the main loop starts one attempt per volume on an op lane as
//!   lanes come free. An attempt that reaches no path ends, and the volume
//!   is tried again a second later, holding no lane or thread meanwhile. A
//!   device not up within 60 s is given up, as a failed reattach always was.
//!   The kernel holds a device's I/O until it is reattached. Until then the
//!   volume is listed as recovering, and attach of it fails so that the
//!   caller retries. Detach of it cancels the recovery and deletes the
//!   device, failing the I/O it holds, so that a volume whose target is gone
//!   for good can still be unstaged; while an attempt is bringing the device
//!   up, detach fails instead and the caller retries.
//! - SIGTERM/SIGINT is a *handover*, not a teardown: new I/O parks, in-flight
//!   commands finish (bounded), each device is marked clean, and the process
//!   exits leaving the devices in place. A clean device is recovered without
//!   the write hold; after a crash (not clean) its writes are held for one
//!   fence, because the dead daemon's writes may still land on the target.
//!
//! Protocol: one JSON request per connection, one JSON response line.
//!   {"op":"attach", ...DeviceSpec}   -> {"ok":true,"dev_id":N,"path":"/dev/ublkbN"}
//!   {"op":"detach","volume":"..."}   -> {"ok":true}
//!                                       ({"ok":true,"cancelled":true} for a
//!                                       volume that was being recovered)
//!   {"op":"list"}                    -> {"ok":true,"devices":[...]}
//!                                       (a volume still being recovered has
//!                                       "recovering":true, its paths down)
//!   {"op":"stats","volume":"..."}    -> {"ok":true,"stats":{...}}

use crate::device::{self, DeviceSpec, OpLanes, Running, OP_LANES};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

pub const DEFAULT_SOCKET: &str = "/run/nvmeublk/nvmeublkd.sock";
pub const DEFAULT_STATE: &str = "/run/nvmeublk/state.json";

/// Between two tries at reaching a recorded device's target.
const RETRY: Duration = Duration::from_secs(1);
/// How soon the main loop looks again for a free lane while recovery
/// attempts wait for one.
const LANE_POLL: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct StateEntry {
    spec: DeviceSpec,
    dev_id: i32,
    /// Set only by a graceful handover that drained every in-flight command.
    #[serde(default)]
    clean: bool,
}

/// A device a previous daemon left behind, not served again yet: as it was
/// recorded, and where its recovery is.
struct Pending {
    entry: StateEntry,
    phase: Phase,
}

/// Where a pending device's recovery is. Attempts are numbered, and a phase
/// names the attempt that owns the volume: an attempt that lost it (to a
/// detach, or to the start deadline) changes nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    /// The next attempt is due then.
    Due(Instant),
    /// This attempt is reaching the target; a detach may still cancel it.
    Trying(u64),
    /// This attempt is bringing the device up, since then.
    Starting(u64, Instant),
    /// A detach is deleting the device.
    Cancelling,
}

/// What the daemon does beyond its own bookkeeping; replaced in tests.
#[derive(Clone, Copy)]
struct Hooks {
    /// One recovery attempt, on an op thread.
    attempt: fn(&Arc<Daemon>, &str, u64),
    /// Delete the device of a volume whose recovery a detach cancelled.
    delete: fn(i32) -> Result<()>,
    lanes: &'static OpLanes,
}

/// Lock order where several are held: `saving`, `devices`, `recovering`.
struct Daemon {
    devices: Mutex<BTreeMap<String, Running>>,
    recovering: Mutex<BTreeMap<String, Pending>>,
    state_path: String,
    /// Serialises the operations on one volume (attach, detach) so two
    /// requests for it cannot race.
    ops: VolumeLocks,
    /// Serialises writing the state file.
    saving: Mutex<()>,
    /// Numbers recovery attempts.
    attempts: AtomicU64,
    /// An eventfd that wakes the main loop when an attempt makes a volume due.
    wake: Option<OwnedFd>,
    hooks: Hooks,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn earliest(a: Option<Instant>, b: Instant) -> Option<Instant> {
    Some(a.map_or(b, |a| a.min(b)))
}

/// The state file's entries. A served device is clean only when `clean` says
/// so (a handover drained it). A device not served again yet keeps the flag
/// it was recorded with: this daemon has sent nothing for it, so what its
/// last server left still holds.
fn state_entries<'a>(
    serving: impl Iterator<Item = (&'a DeviceSpec, i32)>,
    recovering: impl Iterator<Item = &'a StateEntry>,
    clean: &BTreeMap<String, bool>,
) -> Vec<StateEntry> {
    serving
        .map(|(spec, dev_id)| StateEntry { spec: spec.clone(), dev_id, clean: *clean.get(&spec.volume).unwrap_or(&false) })
        .chain(recovering.cloned())
        .collect()
}

/// Per-volume operation locks: two operations on one volume never overlap,
/// operations on different volumes never wait for each other. A volume is in
/// `busy` only while an operation holds its lock.
#[derive(Default)]
struct VolumeLocks {
    busy: Mutex<BTreeSet<String>>,
    released: Condvar,
}

struct VolumeOp<'a> {
    locks: &'a VolumeLocks,
    volume: String,
}

impl VolumeLocks {
    fn lock(&self, volume: &str) -> VolumeOp<'_> {
        let mut busy = lock(&self.busy);
        while busy.contains(volume) {
            busy = self.released.wait(busy).unwrap_or_else(PoisonError::into_inner);
        }
        busy.insert(volume.to_string());
        VolumeOp { locks: self, volume: volume.to_string() }
    }
}

impl Drop for VolumeOp<'_> {
    /// Also on unwinding: a panicking operation must not wedge its volume.
    fn drop(&mut self) {
        lock(&self.locks.busy).remove(&self.volume);
        self.locks.released.notify_all();
    }
}

impl Daemon {
    fn new(state_path: &str) -> Self {
        Self::with_hooks(state_path, Hooks { attempt: Daemon::attempt, delete: device::delete_unserved, lanes: &OP_LANES })
    }

    fn with_hooks(state_path: &str, hooks: Hooks) -> Self {
        // SAFETY: eventfd returns a new descriptor, or -1.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        // SAFETY: fd is a descriptor nothing else owns.
        let wake = (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) });
        Daemon {
            devices: Mutex::new(BTreeMap::new()),
            recovering: Mutex::new(BTreeMap::new()),
            state_path: state_path.to_string(),
            ops: VolumeLocks::default(),
            saving: Mutex::new(()),
            attempts: AtomicU64::new(0),
            wake,
            hooks,
        }
    }

    fn wake(&self) {
        if let Some(fd) = &self.wake {
            let one = 1u64;
            // SAFETY: writes 8 bytes from `one` to an eventfd.
            unsafe { libc::write(fd.as_raw_fd(), (&one as *const u64).cast(), 8) };
        }
    }

    fn save(&self, clean: &BTreeMap<String, bool>) {
        let w = lock(&self.saving);
        self.write_state(&w, clean);
    }

    /// Write the state file. The caller holds `saving`, so writers never
    /// interleave on the temporary file; the snapshot is taken here, so the
    /// last write carries the latest state.
    fn write_state(&self, _saving: &MutexGuard<'_, ()>, clean: &BTreeMap<String, bool>) {
        let entries = {
            let devs = lock(&self.devices);
            let rec = lock(&self.recovering);
            state_entries(devs.values().map(|r| (&r.spec, r.dev_id)), rec.values().map(|p| &p.entry), clean)
        };
        let tmp = format!("{}.tmp", self.state_path);
        let data = serde_json::to_vec_pretty(&entries).unwrap_or_default();
        if std::fs::write(&tmp, data).and_then(|_| std::fs::rename(&tmp, &self.state_path)).is_err() {
            log::error!("could not write state file {}", self.state_path);
        }
    }

    fn attach(&self, spec: DeviceSpec) -> Result<Value> {
        let _op = self.ops.lock(&spec.volume);
        {
            let devs = lock(&self.devices);
            if let Some(r) = devs.get(&spec.volume) {
                if r.spec.subnqn != spec.subnqn {
                    bail!("volume {} is already attached to a different subsystem ({})", spec.volume, r.spec.subnqn);
                }
                // Idempotent: NodeStage retries must get the same device back.
                return Ok(json!({"ok": true, "dev_id": r.dev_id, "path": r.path(), "existing": true}));
            }
            // Its I/O is held until then; handing it out now would hang the caller.
            if let Some(p) = lock(&self.recovering).get(&spec.volume) {
                bail!("volume {} is still being recovered (ublk device {}); retry", spec.volume, p.entry.dev_id);
            }
        }
        let vol = spec.volume.clone();
        let r = device::start(spec, None)?;
        let out = json!({"ok": true, "dev_id": r.dev_id, "path": r.path()});
        lock(&self.devices).insert(vol, r);
        self.save(&BTreeMap::new());
        Ok(out)
    }

    fn detach(&self, volume: &str) -> Result<Value> {
        let _op = self.ops.lock(volume);
        let served = lock(&self.devices).remove(volume);
        let Some(r) = served else { return self.cancel_recovery(volume) };
        self.save(&BTreeMap::new());
        r.detach()?;
        Ok(json!({"ok": true}))
    }

    /// Detach of a volume that is not served again yet: its recovery stops
    /// and its device is deleted, failing the I/O the device holds. Refused
    /// while an attempt is bringing the device up (the caller retries).
    /// Idempotent: a volume nothing knows is absent.
    fn cancel_recovery(&self, volume: &str) -> Result<Value> {
        let dev_id = {
            let mut rec = lock(&self.recovering);
            let Some(p) = rec.get_mut(volume) else {
                // NodeUnstage retries after success must succeed.
                return Ok(json!({"ok": true, "absent": true}));
            };
            if matches!(p.phase, Phase::Starting(..) | Phase::Cancelling) {
                bail!("volume {volume} is still being recovered (ublk device {}); retry", p.entry.dev_id);
            }
            // No attempt can start the device from here on.
            p.phase = Phase::Cancelling;
            p.entry.dev_id
        };
        let deleted = {
            let _lane = self.hooks.lanes.acquire();
            (self.hooks.delete)(dev_id)
        };
        match deleted {
            Ok(()) => {
                lock(&self.recovering).remove(volume);
                self.save(&BTreeMap::new());
                log::info!("{volume}: detached while being recovered: recovery cancelled, ublk device {dev_id} deleted");
                Ok(json!({"ok": true, "cancelled": true}))
            }
            Err(e) => {
                if let Some(p) = lock(&self.recovering).get_mut(volume) {
                    p.phase = Phase::Due(Instant::now());
                }
                self.wake();
                Err(e.context(format!("cancel the recovery of volume {volume}")))
            }
        }
    }

    fn list(&self) -> Value {
        let devs = lock(&self.devices);
        let mut list: Vec<Value> = devs
            .values()
            .map(|r| {
                let paths: Vec<Value> = r.ctrls.paths.iter().map(|p| json!({"addr": p.addr.to_string(), "up": p.cntlid().is_some()})).collect();
                json!({"volume": r.spec.volume, "subnqn": r.spec.subnqn, "dev_id": r.dev_id, "path": r.path(), "paths": paths})
            })
            .collect();
        // The device node exists and belongs to the volume; nothing serves it yet.
        list.extend(lock(&self.recovering).values().map(|p| {
            let e = &p.entry;
            let paths: Vec<Value> = e.spec.addrs.iter().map(|a| json!({"addr": a, "up": false})).collect();
            json!({"volume": e.spec.volume, "subnqn": e.spec.subnqn, "dev_id": e.dev_id, "path": format!("/dev/ublkb{}", e.dev_id), "paths": paths, "recovering": true})
        }));
        json!({"ok": true, "devices": list})
    }

    fn stats(&self, volume: &str) -> Result<Value> {
        let devs = lock(&self.devices);
        if !devs.contains_key(volume) && lock(&self.recovering).contains_key(volume) {
            bail!("volume {volume} is still being recovered");
        }
        let r = devs.get(volume).with_context(|| format!("volume {volume} is not attached"))?;
        let s = &r.stats;
        let l = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        Ok(json!({"ok": true, "stats": {
            "inflight": s.inflight.load(Ordering::Relaxed), "done": l(&s.done),
            "failovers": l(&s.failovers), "resubmits": l(&s.resubmits), "parked": l(&s.parked), "fenced": l(&s.fenced),
            "path_errors": l(&s.path_errors), "protocol_errors": l(&s.protocol_errors), "no_path_eio": l(&s.no_path_eio),
            "reconnects": l(&s.reconnects), "stall_kills": l(&s.stall_kills), "epoch_kills": l(&s.epoch_kills),
            "zc_rx_bytes": l(&s.zc_bytes), "zc_tx_bytes": l(&s.zc_tx_bytes)
        }}))
    }

    /// Graceful handover: park new I/O everywhere, wait (bounded) for what is
    /// on the wire, record which devices drained, and exit without deleting
    /// any device.
    fn handover(&self) -> ! {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut clean = BTreeMap::new();
        loop {
            let devs = lock(&self.devices);
            let all = devs.values().all(|r| r.drain());
            if all || Instant::now() > deadline {
                for r in devs.values() {
                    clean.insert(r.spec.volume.clone(), r.stats.inflight.load(Ordering::Acquire) <= 0);
                }
                drop(devs);
                break;
            }
            drop(devs);
            std::thread::sleep(Duration::from_millis(20));
        }
        // Held until exit: a recovery that finishes meanwhile must not write
        // the state file again, marking the drained devices unclean.
        let _saving = self.final_state(&clean);
        let dirty: Vec<&String> = clean.iter().filter(|(_, c)| !**c).map(|(v, _)| v).collect();
        let recovering = lock(&self.recovering).len();
        log::info!(
            "handover: {} device(s) left for the next daemon ({recovering} not yet recovered); not drained (writes will be held on recovery): {:?}",
            clean.len() + recovering,
            dirty
        );
        std::process::exit(0)
    }

    /// Write the state file one last time and keep it: no other save writes
    /// it until the returned guard is dropped.
    fn final_state(&self, clean: &BTreeMap<String, bool>) -> MutexGuard<'_, ()> {
        let w = lock(&self.saving);
        self.write_state(&w, clean);
        w
    }

    /// Start the recovery attempts that are due, as far as lanes allow, and
    /// give up on devices not brought up within the start deadline. Returns
    /// how long until this has more to do (None: nothing until woken).
    fn dispatch(self: &Arc<Self>) -> Option<Duration> {
        let now = Instant::now();
        let deadline = device::START_DEADLINE;
        let (mut expired, mut due, mut next) = (Vec::new(), Vec::new(), None);
        {
            let mut rec = lock(&self.recovering);
            rec.retain(|vol, p| match p.phase {
                Phase::Starting(_, since) if now >= since + deadline => {
                    expired.push((vol.clone(), p.entry.dev_id));
                    false
                }
                _ => true,
            });
            for (vol, p) in rec.iter() {
                match p.phase {
                    Phase::Due(t) if t <= now => due.push((t, vol.clone())),
                    Phase::Due(t) => next = earliest(next, t),
                    Phase::Starting(_, since) => next = earliest(next, since + deadline),
                    Phase::Trying(_) | Phase::Cancelling => {}
                }
            }
        }
        for (vol, dev_id) in &expired {
            // As a start that timed out always was: dropped from the state.
            // If the device comes up later, it is served until it is stopped.
            log::error!("{vol}: could not reattach ublk device {dev_id}: it did not come up within {} s", deadline.as_secs());
        }
        if !expired.is_empty() {
            self.save(&BTreeMap::new());
        }
        due.sort();
        for (_, vol) in due {
            let Some(lane) = self.hooks.lanes.try_acquire_recovery() else {
                next = earliest(next, now + LANE_POLL);
                break;
            };
            let n = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
            match lock(&self.recovering).get_mut(&vol) {
                Some(p) if matches!(p.phase, Phase::Due(_)) => p.phase = Phase::Trying(n),
                _ => continue,
            }
            let (d, v, attempt) = (self.clone(), vol.clone(), self.hooks.attempt);
            if let Err(e) = device::spawn_op(lane, move || attempt(&d, &v, n)) {
                log::warn!("{vol}: cannot start a recovery attempt ({e}); retrying");
                self.retry(&vol, n);
                next = earliest(next, now + RETRY);
            }
        }
        next.map(|t| t.saturating_duration_since(Instant::now()))
    }

    /// Recovery attempt `n` for `vol`, on an op thread.
    fn attempt(self: &Arc<Self>, vol: &str, n: u64) {
        self.guarded(vol, n, || self.try_recover(vol, n));
    }

    /// Run `f` for attempt `n`. If it panics, the volume's recovery ends as a
    /// failed reattach does, so it cannot stay "being recovered" forever.
    fn guarded(&self, vol: &str, n: u64, f: impl FnOnce()) {
        if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            let what = p.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| p.downcast_ref::<String>().cloned()).unwrap_or_default();
            log::error!("{vol}: recovery attempt panicked: {what}");
            self.settle(vol, n, None);
        }
    }

    fn try_recover(&self, vol: &str, n: u64) {
        let Some(e) = lock(&self.recovering).get(vol).filter(|p| p.phase == Phase::Trying(n)).map(|p| p.entry.clone()) else {
            return;
        };
        let addrs = match device::addrs(&e.spec) {
            Ok(a) => a,
            Err(err) => {
                log::error!("{vol}: could not reattach ublk device {}: {err:#}", e.dev_id);
                return self.settle(vol, n, None);
            }
        };
        // A recovering device holds its I/O until a server reattaches, so an
        // unreachable target is tried again (retry), not given up.
        let ctrls = match device::connect_once(&e.spec, &addrs) {
            Ok(c) => c,
            Err(err) => {
                log::warn!("{vol}: recovery: no path reachable yet ({err:#}); retrying");
                return self.retry(vol, n);
            }
        };
        let c2 = ctrls.clone();
        if !self.start_recovery(vol, n, move |e, hold| device::start_here(e.spec.clone(), c2, Some((e.dev_id, hold)))) {
            // A detach took the volume over while its target was reached.
            ctrls.shutdown();
        }
    }

    /// Attempt `n` reached `vol`'s target. From here this daemon may send the
    /// device's I/O, so a crash from now on must hold its writes on the next
    /// recovery: the volume is recorded unclean, on disk, before `start`
    /// runs. `start` gets the entry as recorded and whether to hold writes
    /// now, the opposite of its recorded clean flag. False, without running
    /// `start`, when the attempt no longer owns the volume.
    fn start_recovery(&self, vol: &str, n: u64, start: impl FnOnce(&StateEntry, bool) -> Result<Running>) -> bool {
        let e = {
            let mut rec = lock(&self.recovering);
            let Some(p) = rec.get_mut(vol).filter(|p| p.phase == Phase::Trying(n)) else { return false };
            let e = p.entry.clone();
            p.entry.clean = false;
            p.phase = Phase::Starting(n, Instant::now());
            e
        };
        self.save(&BTreeMap::new());
        match start(&e, !e.clean) {
            Ok(r) => self.settle(vol, n, Some(r)),
            Err(err) => {
                log::error!("{vol}: could not reattach ublk device {}: {err:#}", e.dev_id);
                self.settle(vol, n, None);
            }
        }
        true
    }

    /// Attempt `n` could not reach the target: try again after RETRY.
    fn retry(&self, vol: &str, n: u64) {
        if let Some(p) = lock(&self.recovering).get_mut(vol).filter(|p| p.phase == Phase::Trying(n)) {
            p.phase = Phase::Due(Instant::now() + RETRY);
        }
        self.wake();
    }

    /// Attempt `n` is over: `vol` is served (`served`), or dropped from the
    /// state as a failed reattach always was. Both maps change in one step,
    /// so no state snapshot sees it twice or not at all. If the attempt no
    /// longer owns the volume (the start deadline passed), a device that
    /// came up anyway is served until it is stopped, unrecorded.
    fn settle(&self, vol: &str, n: u64, served: Option<Running>) {
        {
            let mut devs = lock(&self.devices);
            let mut rec = lock(&self.recovering);
            if !rec.get(vol).is_some_and(|p| matches!(p.phase, Phase::Trying(m) | Phase::Starting(m, _) if m == n)) {
                if let Some(r) = served {
                    log::error!("{vol}: ublk device {} came up after its recovery was given up; it is served until it is stopped", r.dev_id);
                }
                return;
            }
            rec.remove(vol);
            if let Some(r) = served {
                log::info!("{vol}: reattached /dev/ublkb{}", r.dev_id);
                devs.insert(vol.to_string(), r);
            }
        }
        self.save(&BTreeMap::new());
    }
}

fn handle(d: &Daemon, stream: UnixStream) {
    let mut w = match stream.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut line = String::new();
    if BufReader::new(stream).read_line(&mut line).is_err() {
        return;
    }
    let resp = (|| -> Result<Value> {
        let req: Value = serde_json::from_str(line.trim()).context("request is not JSON")?;
        match req.get("op").and_then(Value::as_str) {
            Some("attach") => d.attach(serde_json::from_value(req.clone()).context("bad attach request")?),
            Some("detach") => d.detach(req.get("volume").and_then(Value::as_str).context("detach needs volume")?),
            Some("list") => Ok(d.list()),
            Some("stats") => d.stats(req.get("volume").and_then(Value::as_str).context("stats needs volume")?),
            other => bail!("unknown op {other:?}"),
        }
    })()
    .unwrap_or_else(|e| json!({"ok": false, "error": format!("{e:#}")}));
    let _ = writeln!(w, "{resp}");
}

pub fn run(socket: &str, state_path: &str) -> Result<()> {
    std::fs::create_dir_all("/run/nvmeublk")?;
    let d = Arc::new(Daemon::new(state_path));

    // What a previous daemon left behind is served again in the background
    // (Daemon::dispatch); until then it stays recorded as it was.
    let previous: Vec<StateEntry> = std::fs::read(state_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    let now = Instant::now();
    for e in previous {
        if libublk::ctrl::UblkCtrl::new_simple(e.dev_id).is_err() {
            log::warn!("{}: ublk device {} is gone; dropping it from state", e.spec.volume, e.dev_id);
            continue;
        }
        lock(&d.recovering).insert(e.spec.volume.clone(), Pending { entry: e, phase: Phase::Due(now) });
    }
    d.save(&BTreeMap::new());

    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket).with_context(|| format!("bind {socket}"))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let dh = d.clone();
    ctrlc::set_handler(move || dh.handover())?;
    log::info!("nvmeublkd listening on {socket} ({} device(s) being recovered)", lock(&d.recovering).len());
    serve(&d, &listener)
}

/// The main loop: accept control connections, each handled on a thread of
/// its own, and start recovery attempts as they come due. Recovery starts
/// only here, so the socket is always bound first.
fn serve(d: &Arc<Daemon>, listener: &UnixListener) -> Result<()> {
    listener.set_nonblocking(true)?;
    let wake_fd = d.wake.as_ref().map_or(-1, |f| f.as_raw_fd());
    loop {
        let mut wait = d.dispatch();
        if wake_fd < 0 && !lock(&d.recovering).is_empty() {
            // Nothing wakes this loop: look again regularly instead.
            wait = Some(wait.map_or(LANE_POLL, |w| w.min(LANE_POLL)));
        }
        let timeout = wait.map_or(-1, |w| i32::try_from(w.as_millis() + 1).unwrap_or(i32::MAX));
        let mut fds = [
            libc::pollfd { fd: listener.as_raw_fd(), events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: wake_fd, events: libc::POLLIN, revents: 0 },
        ];
        // SAFETY: two initialised pollfds; poll ignores a negative fd.
        if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout) } < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                log::warn!("control socket poll: {e}");
                std::thread::sleep(Duration::from_millis(10));
            }
            continue;
        }
        if fds[1].revents != 0 {
            let mut n = 0u64;
            // SAFETY: reads the eventfd's 8-byte counter into `n`.
            unsafe { libc::read(wake_fd, (&mut n as *mut u64).cast(), 8) };
        }
        loop {
            match listener.accept() {
                Ok((s, _)) => {
                    let _ = s.set_nonblocking(false);
                    let d = d.clone();
                    // Per request. Detach runs its STOP/DEL here, holding an
                    // op lane; the io-wq worker behind them exits with it.
                    if let Err(e) = std::thread::Builder::new().name("nvq-ctl".into()).spawn(move || handle(&d, s)) {
                        log::warn!("control socket: no thread for a request ({e})");
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    log::warn!("control socket accept: {e}");
                    std::thread::sleep(Duration::from_millis(10));
                    break;
                }
            }
        }
    }
}

/// `nvmeublk ctl <json>`: send one request and print the response.
pub fn ctl(socket: &str, request: &str) -> Result<()> {
    let mut s = UnixStream::connect(socket).with_context(|| format!("connect {socket}"))?;
    writeln!(s, "{request}")?;
    let mut resp = String::new();
    BufReader::new(s).read_line(&mut resp)?;
    print!("{resp}");
    let v: Value = serde_json::from_str(resp.trim()).unwrap_or(Value::Null);
    if v.get("ok") != Some(&Value::Bool(true)) {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn entry(volume: &str, dev_id: i32, clean: bool) -> StateEntry {
        let spec = serde_json::from_value(json!({"volume": volume, "subnqn": "nqn.2026-09.test:sub", "addrs": ["192.0.2.1:4420"]})).unwrap();
        StateEntry { spec, dev_id, clean }
    }

    fn state_path(test: &str) -> String {
        std::env::temp_dir().join(format!("nvmeublk-{}-{test}.json", std::process::id())).to_string_lossy().into_owned()
    }

    fn no_attempt(_: &Arc<Daemon>, _: &str, _: u64) {
        panic!("no recovery attempt expected");
    }

    fn no_delete(_: i32) -> Result<()> {
        panic!("no device deletion expected");
    }

    /// A daemon with its own lanes, whose effects outside its bookkeeping
    /// are `attempt` and `delete`.
    fn daemon(test: &str, attempt: fn(&Arc<Daemon>, &str, u64), delete: fn(i32) -> Result<()>) -> Arc<Daemon> {
        let lanes: &'static OpLanes = Box::leak(Box::new(OpLanes::new()));
        Arc::new(Daemon::with_hooks(&state_path(test), Hooks { attempt, delete, lanes }))
    }

    fn pend(d: &Daemon, e: StateEntry, phase: Phase) {
        lock(&d.recovering).insert(e.spec.volume.clone(), Pending { entry: e, phase });
    }

    fn phase(d: &Daemon, vol: &str) -> Option<Phase> {
        lock(&d.recovering).get(vol).map(|p| p.phase)
    }

    fn on_disk(d: &Daemon) -> Vec<StateEntry> {
        serde_json::from_slice(&std::fs::read(&d.state_path).unwrap()).unwrap()
    }

    fn wait_for(what: &str, f: impl Fn() -> bool) {
        let t0 = Instant::now();
        while !f() {
            assert!(t0.elapsed() < Duration::from_secs(5), "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn one_volume_waits_for_itself_only() {
        let locks = Arc::new(VolumeLocks::default());
        let a = locks.lock("a");
        let (tx, rx) = mpsc::channel();
        let l = locks.clone();
        std::thread::spawn(move || {
            let _b = l.lock("b");
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).expect("an operation on volume b waited for volume a");
        let (tx, rx) = mpsc::channel();
        let l = locks.clone();
        let second = std::thread::spawn(move || {
            let _a = l.lock("a");
            tx.send(()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err(), "two operations on volume a overlapped");
        drop(a);
        rx.recv_timeout(Duration::from_secs(5)).expect("volume a was never released");
        second.join().unwrap();
        assert!(locks.busy.lock().unwrap().is_empty());
    }

    #[test]
    fn a_panicking_operation_releases_its_volume() {
        let locks = Arc::new(VolumeLocks::default());
        let l = locks.clone();
        assert!(std::thread::spawn(move || {
            let _a = l.lock("a");
            panic!("operation failed");
        })
        .join()
        .is_err());
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _a = locks.lock("a");
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).expect("volume a stayed locked after a panic");
    }

    /// A volume still being recovered stays in the state file with the flag
    /// it was recorded with, whatever a save (or a handover) says about the
    /// served ones; a served device is clean only when drained.
    #[test]
    fn state_keeps_unrecovered_volumes_and_their_flags() {
        let d = daemon("state", no_attempt, no_delete);
        pend(&d, entry("v1", 7, true), Phase::Due(Instant::now()));
        pend(&d, entry("v2", 8, false), Phase::Trying(1));
        d.save(&BTreeMap::from([("v1".to_string(), false), ("v2".to_string(), true)]));
        let back = on_disk(&d);
        let _ = std::fs::remove_file(&d.state_path);
        assert_eq!(back, vec![entry("v1", 7, true), entry("v2", 8, false)]);

        let (s3, s4) = (entry("v3", 1, false).spec, entry("v4", 2, false).spec);
        let served = [(&s3, 1), (&s4, 2)];
        let out = state_entries(served.into_iter(), std::iter::empty(), &BTreeMap::from([("v3".to_string(), true)]));
        assert_eq!(out, vec![entry("v3", 1, true), entry("v4", 2, false)]);
    }

    /// While its recovery runs in the background the volume is visible and
    /// owned: attach fails (the caller retries) instead of adding a second
    /// device, and so does detach while the device is being brought up.
    #[test]
    fn recovering_volume_is_listed_and_refuses_attach() {
        let d = daemon("recovering", no_attempt, no_delete);
        pend(&d, entry("v1", 7, true), Phase::Starting(1, Instant::now()));
        let list = d.list();
        let dev = &list["devices"][0];
        assert_eq!((dev["volume"].as_str(), dev["dev_id"].as_i64(), dev["path"].as_str()), (Some("v1"), Some(7), Some("/dev/ublkb7")));
        assert_eq!((dev["recovering"].as_bool(), dev["paths"][0]["up"].as_bool()), (Some(true), Some(false)));
        let err = d.attach(entry("v1", 0, false).spec).unwrap_err();
        assert!(format!("{err:#}").contains("being recovered"), "{err:#}");
        let err = d.detach("v1").unwrap_err();
        assert!(format!("{err:#}").contains("being recovered"), "{err:#}");
        assert!(d.stats("v1").is_err());
        assert_eq!(d.detach("v2").unwrap()["absent"], true);
        assert!(matches!(phase(&d, "v1"), Some(Phase::Starting(1, _))));
        let _ = std::fs::remove_file(&d.state_path);
    }

    /// A volume whose target is gone for good can still be unstaged: detach
    /// cancels its recovery, whether it waits for its next try or an attempt
    /// is reaching the target, and deletes its device. The attempt then
    /// changes nothing. If the device cannot be deleted the volume stays
    /// recorded and is tried again.
    #[test]
    fn detach_cancels_a_recovery_and_deletes_its_device() {
        static DELETED: Mutex<Vec<i32>> = Mutex::new(Vec::new());
        fn delete(id: i32) -> Result<()> {
            if id == 9 {
                bail!("EBUSY");
            }
            lock(&DELETED).push(id);
            Ok(())
        }
        let d = daemon("cancel", no_attempt, delete);
        pend(&d, entry("v1", 7, true), Phase::Due(Instant::now()));
        pend(&d, entry("v2", 8, true), Phase::Trying(5));
        pend(&d, entry("v3", 9, true), Phase::Due(Instant::now()));
        assert_eq!(d.detach("v1").unwrap()["cancelled"], true);
        assert_eq!(d.detach("v2").unwrap()["cancelled"], true);
        assert_eq!(*lock(&DELETED), vec![7, 8]);
        assert!(!d.start_recovery("v2", 5, |_, _| panic!("a cancelled recovery started its device")));
        d.retry("v2", 5);
        d.settle("v2", 5, None);
        assert_eq!((phase(&d, "v1"), phase(&d, "v2")), (None, None));
        assert!(d.detach("v3").is_err());
        assert!(matches!(phase(&d, "v3"), Some(Phase::Due(_))), "{:?}", phase(&d, "v3"));
        assert_eq!(on_disk(&d), vec![entry("v3", 9, true)]);
        assert_eq!(d.detach("v1").unwrap()["absent"], true);
        let _ = std::fs::remove_file(&d.state_path);
    }

    /// The data-integrity step of a recovery: before the device is started
    /// (START_USER_RECOVERY, after which this daemon may send its writes),
    /// the state file on disk already says unclean, so a crash from then on
    /// holds writes on the next recovery; and the writes are held now unless
    /// the previous daemon handed the device over clean.
    #[test]
    fn recovery_records_the_volume_unclean_before_it_starts() {
        for clean in [true, false] {
            let d = daemon(&format!("unclean-{clean}"), no_attempt, no_delete);
            pend(&d, entry("v1", 7, clean), Phase::Trying(1));
            pend(&d, entry("v2", 8, true), Phase::Due(Instant::now()));
            d.save(&BTreeMap::new());
            // An attempt that does not own the volume starts nothing.
            assert!(!d.start_recovery("v1", 2, |_, _| panic!("a stale attempt started the device")));
            assert_eq!(on_disk(&d)[0], entry("v1", 7, clean));
            let mut ran = false;
            assert!(d.start_recovery("v1", 1, |e, hold| {
                ran = true;
                assert_eq!(on_disk(&d), vec![entry("v1", 7, false), entry("v2", 8, true)], "clean={clean}: not recorded unclean before the start");
                assert_eq!(hold, !clean, "clean={clean}: writes held wrongly");
                assert_eq!(*e, entry("v1", 7, clean));
                bail!("no ublk device in tests")
            }));
            assert!(ran);
            // A failed start drops the volume, as a failed reattach always did.
            assert_eq!(phase(&d, "v1"), None);
            assert_eq!(on_disk(&d), vec![entry("v2", 8, true)]);
            let _ = std::fs::remove_file(&d.state_path);
        }
    }

    #[test]
    fn a_panicking_recovery_attempt_ends_the_recovery() {
        let d = daemon("panic", no_attempt, no_delete);
        pend(&d, entry("v1", 7, true), Phase::Trying(3));
        d.guarded("v1", 3, || panic!("recovery bug"));
        assert_eq!(phase(&d, "v1"), None, "a panicked attempt left its volume being recovered");
        assert!(on_disk(&d).is_empty());
        let _ = std::fs::remove_file(&d.state_path);
    }

    /// Each due volume gets its own attempt on its own lane at once: one
    /// whose attempt hangs on its target, or one whose target is down, does
    /// not hold up the others. A volume whose target is down is tried again
    /// a second later and holds no lane meanwhile.
    #[test]
    fn a_slow_or_unreachable_target_does_not_hold_up_other_volumes() {
        static STARTED: Mutex<Vec<String>> = Mutex::new(Vec::new());
        static RELEASE: Mutex<bool> = Mutex::new(false);
        fn attempt(d: &Arc<Daemon>, vol: &str, n: u64) {
            lock(&STARTED).push(vol.to_string());
            match vol {
                "down" => d.retry(vol, n),
                "slow" => {
                    wait_for("the slow target", || *lock(&RELEASE));
                    d.settle(vol, n, None);
                }
                _ => d.settle(vol, n, None),
            }
        }
        let d = daemon("slow", attempt, no_delete);
        let now = Instant::now();
        for (i, vol) in ["slow", "down", "up"].into_iter().enumerate() {
            pend(&d, entry(vol, i as i32, true), Phase::Due(now));
        }
        d.save(&BTreeMap::new());
        d.dispatch();
        wait_for("three attempts", || lock(&STARTED).len() == 3);
        wait_for("the reachable volume", || phase(&d, "up").is_none());
        wait_for("the down volume's retry", || matches!(phase(&d, "down"), Some(Phase::Due(_))));
        let Some(Phase::Due(t)) = phase(&d, "down") else { panic!() };
        assert!(t > now + RETRY / 2, "retried at once");
        // Not due yet: nothing starts, and the main loop sleeps until it is.
        let wait = d.dispatch().expect("the main loop would not wake for the retry");
        assert!(wait <= RETRY, "{wait:?}");
        assert_eq!(lock(&STARTED).len(), 3);
        assert!(matches!(phase(&d, "slow"), Some(Phase::Trying(_))));
        *lock(&RELEASE) = true;
        // Its save lands just after it leaves the map.
        wait_for("the slow volume's end in the state file", || on_disk(&d) == vec![entry("down", 1, true)]);
        assert_eq!(phase(&d, "slow"), None);
        let _ = std::fs::remove_file(&d.state_path);
    }

    #[test]
    fn recovery_waits_for_a_lane() {
        let d = daemon("lanes", no_attempt, no_delete);
        let held: Vec<_> = std::iter::from_fn(|| d.hooks.lanes.try_acquire_recovery()).collect();
        assert_eq!(held.len(), device::LANES);
        pend(&d, entry("v1", 7, true), Phase::Due(Instant::now()));
        let wait = d.dispatch().expect("the main loop would not look again for a lane");
        assert!(wait <= LANE_POLL, "{wait:?}");
        assert!(matches!(phase(&d, "v1"), Some(Phase::Due(_))));
        drop(held);
        let _ = std::fs::remove_file(&d.state_path);
    }

    #[test]
    fn a_device_not_up_within_the_deadline_is_given_up() {
        let d = daemon("deadline", no_attempt, no_delete);
        let since = Instant::now() - device::START_DEADLINE - Duration::from_secs(1);
        pend(&d, entry("v1", 7, true), Phase::Starting(4, since));
        pend(&d, entry("v2", 8, true), Phase::Starting(5, Instant::now()));
        let wait = d.dispatch().expect("no wake for v2's deadline");
        assert!(wait <= device::START_DEADLINE);
        assert_eq!(phase(&d, "v1"), None);
        assert_eq!(on_disk(&d), vec![entry("v2", 8, true)]);
        // Its attempt, when it ends, changes nothing.
        d.settle("v1", 4, None);
        assert_eq!(on_disk(&d).len(), 1);
        let _ = std::fs::remove_file(&d.state_path);
    }

    /// Once the handover has written the state file, no save lands after it
    /// before the process exits: a recovery finishing then would otherwise
    /// write the drained devices back as unclean.
    #[test]
    fn nothing_writes_the_state_file_after_the_handover() {
        let d = daemon("handover", no_attempt, no_delete);
        pend(&d, entry("v1", 7, true), Phase::Trying(1));
        let guard = d.final_state(&BTreeMap::new());
        let written = on_disk(&d);
        assert_eq!(written, vec![entry("v1", 7, true)]);
        let d2 = d.clone();
        let late = std::thread::spawn(move || d2.settle("v1", 1, None));
        std::thread::sleep(Duration::from_millis(200));
        assert!(!late.is_finished(), "a save went through after the handover's");
        assert_eq!(on_disk(&d), written);
        drop(guard);
        late.join().unwrap();
        let _ = std::fs::remove_file(&d.state_path);
    }

    /// The control socket answers while a recovery attempt is stuck on its
    /// target: recovery starts only from the loop that serves the socket.
    #[test]
    fn the_socket_serves_while_a_recovery_is_stuck() {
        static STUCK: Mutex<bool> = Mutex::new(true);
        static RAN: Mutex<bool> = Mutex::new(false);
        fn attempt(d: &Arc<Daemon>, vol: &str, n: u64) {
            *lock(&RAN) = true;
            while *lock(&STUCK) {
                std::thread::sleep(Duration::from_millis(2));
            }
            d.retry(vol, n);
        }
        let d = daemon("socket", attempt, no_delete);
        pend(&d, entry("v1", 7, true), Phase::Due(Instant::now()));
        let socket = state_path("socket.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        let d2 = d.clone();
        std::thread::spawn(move || serve(&d2, &listener));
        wait_for("the recovery attempt", || *lock(&RAN));
        let mut s = UnixStream::connect(&socket).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        writeln!(s, r#"{{"op":"list"}}"#).unwrap();
        let mut resp = String::new();
        BufReader::new(s).read_line(&mut resp).unwrap();
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!((v["devices"][0]["volume"].as_str(), v["devices"][0]["recovering"].as_bool()), (Some("v1"), Some(true)));
        *lock(&STUCK) = false;
        let _ = std::fs::remove_file(&socket);
        let _ = std::fs::remove_file(&d.state_path);
    }
}
