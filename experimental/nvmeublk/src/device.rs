//! One ublk device served by this process: its controllers, its ublk queues
//! and its lifecycle. `nvmeublk run` serves exactly one; the per-node daemon
//! (daemon.rs) serves many.
//!
//! A device has no thread of its own for its lifetime, only its queue
//! threads. Its control operations (bring-up, teardown) run on short-lived
//! threads that hold an op lane (`OpLanes`): each such thread has its own
//! control ring, so the io-wq worker the driver punts control commands to
//! exits with it, and the lanes bound how many of them run at once.

use crate::conn::Ident;
use crate::{ctrls, host_ident, qengine, queue_fn};
use anyhow::{anyhow, bail, Context, Result};
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder, UblkTargetThreads};
use libublk::io::UblkDev;
use libublk::UblkFlags;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// NVMe keep-alive timeout of the admin queues.
const KATO: Duration = Duration::from_secs(15);

/// How long a caller waits for a device to come up, op lane included.
pub const START_DEADLINE: Duration = Duration::from_secs(60);

fn d_queues() -> u16 {
    2
}
fn d_depth() -> u16 {
    64
}
fn d_io_timeout() -> u64 {
    5000
}
fn d_no_path() -> u64 {
    30000
}
fn d_one() -> usize {
    1
}
fn d_threads() -> u16 {
    4
}
fn d_chunk() -> u16 {
    2
}
fn d_rx_chunk() -> usize {
    32 * 1024
}

/// Everything needed to (re)create a device. Stored in the daemon's state
/// file, so a restarted daemon can reattach with the same configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DeviceSpec {
    /// Caller's name for the volume (scale-csi: the volume id). Unique per node.
    pub volume: String,
    pub subnqn: String,
    /// Target portals, "addr:port".
    pub addrs: Vec<String>,
    /// Host identity to connect with; the node's own if omitted.
    #[serde(default)]
    pub hostnqn: Option<String>,
    #[serde(default)]
    pub hostid: Option<String>,
    #[serde(default = "d_queues")]
    pub queues: u16,
    #[serde(default = "d_depth")]
    pub depth: u16,
    #[serde(default)]
    pub zero_copy: bool,
    /// NAPI busy-poll budget while I/O is in flight (us); 0 = off.
    #[serde(default)]
    pub napi_us: u32,
    #[serde(default = "d_io_timeout")]
    pub io_timeout_ms: u64,
    #[serde(default = "d_no_path")]
    pub no_path_timeout_ms: u64,
    /// Hold for writes orphaned by a failed path; KATO + 5 s if omitted.
    #[serde(default)]
    pub write_fence_ms: Option<u64>,
    /// TCP connections per path per queue (tuning; default 1).
    #[serde(default = "d_one")]
    pub conns_per_path: usize,
    /// Most bytes one header receive may take into the copy buffer. Payload
    /// that arrives inside it is copied; the rest of a read's payload goes
    /// zero copy. Smaller = less copying, more receives (tuning; default 32 KiB).
    #[serde(default = "d_rx_chunk")]
    pub rx_chunk: usize,
    /// ublk I/O threads per queue (UBLK_F_PER_IO_DAEMON; tuning, default 1).
    /// Each thread serves its own partition of the queue's tags with its own
    /// engine and connections.
    #[serde(default = "d_threads")]
    pub threads_per_queue: u16,
    /// Contiguous tag partitions instead of interleaved ones (tuning): blk-mq
    /// hands a submitter sequential tags, so one stream stays on one thread.
    #[serde(default)]
    pub seq_tags: bool,
    /// Chunked tag partition with several threads per queue (tuning): each
    /// thread owns runs of this many consecutive tags, dealt round-robin, so
    /// a low-depth stream stays on one thread for that many requests while a
    /// deep one spans all threads. 1 = plain interleave; clamped to
    /// depth / threads; ignored with seq_tags.
    #[serde(default = "d_chunk")]
    pub tag_chunk: u16,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Op lanes any control operation may take: attach, detach, and ending a
/// device that stopped on its own.
pub const GENERAL_LANES: usize = 4;
/// Every op lane: the general ones and one that only recovery may take.
pub const LANES: usize = GENERAL_LANES + 1;

/// Bounds the control operations that run at once, and with them the
/// threads that run them (`nvq-op-<lane>`) and those threads' io-wq
/// workers. Recovery takes the reserved lane, or a general lane that no
/// general operation is waiting for: a node recovering many volumes always
/// makes progress, and attach and detach still get the next free lane.
pub struct OpLanes {
    st: Mutex<LaneState>,
    freed: Condvar,
}

struct LaneState {
    busy: [bool; LANES],
    /// General operations waiting for a lane.
    waiting: usize,
}

/// This process's op lanes.
pub static OP_LANES: OpLanes = OpLanes::new();

/// A held op lane, freed when dropped.
pub struct OpLane {
    lanes: &'static OpLanes,
    n: usize,
}

impl OpLanes {
    pub const fn new() -> Self {
        OpLanes { st: Mutex::new(LaneState { busy: [false; LANES], waiting: 0 }), freed: Condvar::new() }
    }

    /// A general lane, waiting as long as it takes.
    pub fn acquire(&'static self) -> OpLane {
        match self.take_general(None) {
            Some(lane) => lane,
            None => unreachable!("a lane wait without a deadline ended without a lane"),
        }
    }

    /// A general lane, waiting for one until `deadline` at most.
    pub fn acquire_until(&'static self, deadline: Instant) -> Option<OpLane> {
        self.take_general(Some(deadline))
    }

    fn take_general(&'static self, deadline: Option<Instant>) -> Option<OpLane> {
        let mut st = lock(&self.st);
        st.waiting += 1;
        let n = loop {
            if let Some(n) = (0..GENERAL_LANES).find(|&n| !st.busy[n]) {
                break Some(n);
            }
            st = match deadline {
                None => self.freed.wait(st).unwrap_or_else(PoisonError::into_inner),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        break None;
                    }
                    self.freed.wait_timeout(st, d - now).unwrap_or_else(PoisonError::into_inner).0
                }
            };
        };
        st.waiting -= 1;
        let n = n?;
        st.busy[n] = true;
        Some(OpLane { lanes: self, n })
    }

    /// A lane for a recovery, if one is free now: the reserved lane, or a
    /// general one while no general operation is waiting for one.
    pub fn try_acquire_recovery(&'static self) -> Option<OpLane> {
        let mut st = lock(&self.st);
        let n = if !st.busy[GENERAL_LANES] {
            GENERAL_LANES
        } else if st.waiting == 0 {
            (0..GENERAL_LANES).find(|&n| !st.busy[n])?
        } else {
            return None;
        };
        st.busy[n] = true;
        Some(OpLane { lanes: self, n })
    }
}

impl Drop for OpLane {
    fn drop(&mut self) {
        lock(&self.lanes.st).busy[self.n] = false;
        self.lanes.freed.notify_all();
    }
}

/// Run `op` on a new thread named after `lane` (`nvq-op-<n>`), which holds
/// the lane until `op` returns. If no thread can be started, `op` is dropped
/// unrun and the lane freed.
pub fn spawn_op(lane: OpLane, op: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
    std::thread::Builder::new().name(format!("nvq-op-{}", lane.n)).spawn(move || {
        let _lane = lane;
        op()
    })?;
    Ok(())
}

/// Run `op` holding a general lane, on a new thread that first waits for
/// one (`nvq-op-wait`): for callers that must not block until a lane is free,
/// such as a queue thread whose device ends on its own (it holds the char
/// device open until it returns).
pub fn spawn_op_waiting(lanes: &'static OpLanes, op: impl FnOnce() + Send + 'static) -> std::io::Result<()> {
    std::thread::Builder::new().name("nvq-op-wait".into()).spawn(move || {
        let _lane = lanes.acquire();
        op()
    })?;
    Ok(())
}

/// Called whenever a device finishes ending on its own (set once, by the
/// daemon, to wake its main loop, which then forgets the device).
static ON_DEVICE_END: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>> = std::sync::OnceLock::new();

pub fn on_device_end(f: impl Fn() + Send + Sync + 'static) {
    let _ = ON_DEVICE_END.set(Box::new(f));
}

/// Marks an error after which the operation may simply be tried again later
/// (the device is untouched and still waits for a server): the kernel
/// answered EBUSY to START_USER_RECOVERY, or bring-up gave up on a queue
/// that could not be set up.
#[derive(Debug)]
pub struct TryAgain(pub String);

impl std::fmt::Display for TryAgain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TryAgain {}

/// Whether `e` (or an error it wraps) is a TryAgain. It must be the root
/// cause or a wrapped error, not a `.context(TryAgain(..))` layer.
pub fn is_try_again(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<TryAgain>().is_some())
}

/// How a device ends. Shared by its `Running`, its bring-up and its queue
/// threads, so that exactly one of them ends it: `detach`/`wait` take it; or,
/// when its queue threads all return first (it was stopped or deleted from
/// elsewhere, or its queue loops failed), it ends on its own, as the device's
/// own thread ended it once `run_target` returned before there were op
/// threads.
struct Ending<S> {
    st: Mutex<End<S>>,
    ended: Condvar,
}

struct End<S> {
    /// Stored when the device is up; taken by whoever ends it.
    served: Option<S>,
    /// Every queue thread has returned.
    queues_done: bool,
    /// The device is ending on its own: None while that runs, then how it went.
    on_its_own: Option<Option<Result<(), String>>>,
}

/// What `Ending::take` found.
enum Owner<S> {
    /// The caller ends the device.
    Own(S),
    /// It ended on its own, with this outcome.
    Ended(Result<(), String>),
}

impl<S> Ending<S> {
    fn new() -> Self {
        Ending { st: Mutex::new(End { served: None, queues_done: false, on_its_own: None }), ended: Condvar::new() }
    }

    /// The device is up. If its queue threads have all returned already it
    /// is handed back: it is ending on its own, and the caller ends it and
    /// reports through `ended`.
    fn up(&self, s: S) -> Option<S> {
        let mut e = lock(&self.st);
        if e.queues_done {
            e.on_its_own = Some(None);
            return Some(s);
        }
        e.served = Some(s);
        None
    }

    /// The last queue thread returned. True if nobody is ending the device:
    /// it is ending on its own, and the caller sees that it is ended
    /// (`take_own` then `ended`, or else `give_back`).
    fn queues_done(&self) -> bool {
        let mut e = lock(&self.st);
        e.queues_done = true;
        if e.served.is_some() {
            e.on_its_own = Some(None);
            return true;
        }
        false
    }

    /// For whoever ends a device that is ending on its own.
    fn take_own(&self) -> Option<S> {
        lock(&self.st).served.take()
    }

    /// The device that was ending on its own could not be ended (no thread
    /// for it). `prepare` readies it for waiting; its owner ends it instead.
    fn give_back(&self, prepare: impl FnOnce(&mut S)) {
        let mut e = lock(&self.st);
        if let Some(s) = e.served.as_mut() {
            prepare(s);
        }
        e.on_its_own = None;
        drop(e);
        self.ended.notify_all();
    }

    /// The device that was ending on its own has ended.
    fn ended(&self, r: Result<(), String>) {
        lock(&self.st).on_its_own = Some(Some(r));
        self.ended.notify_all();
        if let Some(f) = ON_DEVICE_END.get() {
            f();
        }
    }

    /// It ended on its own, or is ending so: nobody may hand it out again.
    fn has_ended(&self) -> bool {
        lock(&self.st).on_its_own.is_some()
    }

    /// It ended on its own and was deleted: only now is its id free for
    /// another device. While it is still ending, or deleting it failed, the
    /// id is still its own and the kernel device may still hold its I/O.
    fn is_gone(&self) -> bool {
        matches!(lock(&self.st).on_its_own, Some(Some(Ok(()))))
    }

    /// It ended on its own, but deleting it failed: why.
    fn end_failed(&self) -> Option<String> {
        match &lock(&self.st).on_its_own {
            Some(Some(Err(e))) => Some(e.clone()),
            _ => None,
        }
    }

    /// For detach/wait: take the device to end it, or wait until it has
    /// ended on its own.
    fn take(&self) -> Owner<S> {
        let mut e = lock(&self.st);
        loop {
            match &e.on_its_own {
                Some(Some(r)) => return Owner::Ended(r.clone()),
                Some(None) => e = self.ended.wait(e).unwrap_or_else(PoisonError::into_inner),
                None => return e.served.take().map_or(Owner::Ended(Ok(())), Owner::Own),
            }
        }
    }

    fn queues_are_done(&self) -> bool {
        lock(&self.st).queues_done
    }

    /// A detach that could not stop the device gives it back, and it ends on
    /// its own once stopped. Handed back again if its queue threads have all
    /// returned meanwhile: then nothing else would end it.
    fn put_back(&self, s: S) -> Option<S> {
        let mut e = lock(&self.st);
        if e.queues_done {
            return Some(s);
        }
        e.served = Some(s);
        None
    }
}

/// What a detach did with a device it took (`stop_or_put_back`).
enum Stop<S> {
    /// Stopped (or its queue threads had all returned): end it with `retire`.
    Retire(S),
    /// It could not be stopped and is still served, as it was: `draining` is
    /// clear again, so its I/O goes on as before.
    StillServed(anyhow::Error),
}

/// The step of a detach between taking the device and retiring it: set
/// `draining` (parked and fenced I/O fail with EIO, so nothing waits on
/// paths that are gone) and make its queue threads return with `kill`
/// (`Running::end_it`), unless they all have already (stopped elsewhere, or
/// its loops failed). `retire` then closes the char device before it sends
/// STOP: STOP of a live device waits for every request the server has
/// taken (ublk_wait_tagset_rqs_idle), and while the char device is open
/// only the server can complete them (UblkCtrl::wait_target). If `kill`
/// fails, the device goes back to where it was, served; it is retired
/// anyway if its queue threads returned meanwhile, since nothing else would
/// end it then.
fn stop_or_put_back<S, E: std::error::Error + Send + Sync + 'static>(end: &Ending<S>, s: S, draining: &AtomicBool, kill: impl FnOnce() -> Result<(), E>) -> Stop<S> {
    draining.store(true, Ordering::Release);
    if end.queues_are_done() {
        return Stop::Retire(s);
    }
    match kill() {
        Ok(()) => Stop::Retire(s),
        Err(e) => match end.put_back(s) {
            None => {
                draining.store(false, Ordering::Release);
                Stop::StillServed(anyhow::Error::new(e))
            }
            Some(s) => Stop::Retire(s),
        },
    }
}

/// A detach that did not end the device.
pub struct DetachFailed {
    pub error: anyhow::Error,
    /// The caller keeps the device, and a later detach can try again: it is
    /// still served, unchanged (its admin paths are up), or it ended on its
    /// own but could not be deleted, so the kernel device may still hold the
    /// volume's I/O (`Running::end_failed`).
    pub still_served: Option<Box<Running>>,
}

/// A device being served by this process.
pub struct Running {
    pub spec: DeviceSpec,
    pub dev_id: i32,
    pub stats: Arc<qengine::Stats>,
    pub ctrls: Arc<ctrls::Ctrls>,
    draining: Arc<AtomicBool>,
    pub quiesce: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    /// Set by a detach: the queue loops return (and close their rings), so
    /// that the device's char device can be closed before STOP.
    leave: Arc<AtomicBool>,
    /// Dropping a Running leaves the device served; it then ends on its own
    /// when it is stopped.
    end: Arc<Ending<Served>>,
}

impl Running {
    pub fn path(&self) -> String {
        format!("/dev/ublkb{}", self.dev_id)
    }

    /// Stop serving and delete the device, on this thread, holding a general
    /// op lane (waiting START_DEADLINE at most for one). Its queue threads
    /// return first and the char device is closed before STOP, so the I/O
    /// they had not completed fails with EIO (parked and fenced I/O, too)
    /// and this never waits on paths that are gone, nor on requests the
    /// queue threads would never complete. If
    /// the device has ended on its own, this waits until that is done and
    /// reports how it went.
    ///
    /// If the device cannot be stopped it is handed back still served, its
    /// admin paths untouched: shutting them down would leave a device that
    /// answers EIO and redials a controller the target has dropped.
    pub fn detach(self) -> std::result::Result<(), DetachFailed> {
        let r = self.end_it(true);
        self.detached(r)
    }

    /// What detach reports for how `end_it` went: the admin paths go down
    /// unless the caller keeps the device.
    fn detached(self, r: std::result::Result<(), (anyhow::Error, bool)>) -> std::result::Result<(), DetachFailed> {
        match r {
            Ok(()) => {
                self.ctrls.shutdown();
                Ok(())
            }
            Err((error, true)) => Err(DetachFailed { error, still_served: Some(Box::new(self)) }),
            Err((error, false)) => {
                self.ctrls.shutdown();
                Err(DetachFailed { error, still_served: None })
            }
        }
    }

    /// Wait until the device is stopped (for `nvmeublk run`, whose SIGINT
    /// stops it), then delete it.
    pub fn wait(self) -> Result<()> {
        let r = self.end_it(false).map_err(|(e, _)| e);
        self.ctrls.shutdown();
        r
    }

    /// Err((why, still served)).
    fn end_it(&self, detach: bool) -> std::result::Result<(), (anyhow::Error, bool)> {
        let s = match self.end.take() {
            Owner::Own(s) => s,
            // Deleting it failed: the kernel device may still hold the
            // volume's I/O, so the caller keeps it (detach).
            Owner::Ended(r) => return r.map_err(|e| (anyhow!("ublk device {} ended on its own: {e}", self.dev_id), detach)),
        };
        if !detach {
            return retire(s, &self.stop).map_err(|e| (e, false));
        }
        let Some(_lane) = OP_LANES.acquire_until(Instant::now() + START_DEADLINE) else {
            return match self.end.put_back(s) {
                None => Err((anyhow!("ublk device {}: no control lane free within {} s; retry", self.dev_id, START_DEADLINE.as_secs()), true)),
                Some(s) => retire(s, &self.stop).map_err(|e| (e, false)),
            };
        };
        // Never STOP_DEV here, with the queue threads running and the char
        // device open: STOP waits for every request they have taken, and one
        // they never complete (a tag whose request they lost, a queue thread
        // that is stuck, or this process exiting meanwhile, which a handover
        // does) hangs STOP in the kernel for good, with it this thread and
        // the process (unkillable once its io-wq worker is in STOP). The
        // queue loops return instead; `retire` joins them, closes the char
        // device (the driver then takes back every request they held) and
        // only then sends STOP and DEL.
        let leave = || {
            self.leave.store(true, Ordering::Release);
            Ok::<(), std::convert::Infallible>(())
        };
        match stop_or_put_back(&self.end, s, &self.draining, leave) {
            Stop::Retire(s) => retire(s, &self.stop).map_err(|e| (e, false)),
            Stop::StillServed(e) => Err((e.context(format!("stop ublk device {}", self.dev_id)), true)),
        }
    }

    /// Stop sending new I/O; true once nothing is on the wire and no write
    /// that was (or may have been) is waiting to go out again (`drained`).
    pub fn drain(&self) -> bool {
        self.quiesce.store(true, Ordering::Release);
        self.drained()
    }

    /// Nothing of this device's I/O can still land on the target: nothing on
    /// the wire, and no write or flush held back that went out before (a
    /// failed path's orphans waiting out the write fence, or a predecessor's
    /// writes held after a crash). Only then may a handover call it clean,
    /// which lets the next daemon send the kernel's reissued writes at once.
    pub fn drained(&self) -> bool {
        self.stats.inflight.load(Ordering::Acquire) <= 0 && self.stats.orphans.load(Ordering::Acquire) <= 0
    }

    /// The device ended (or is ending) on its own: it is no longer served,
    /// and its id may belong to another device soon.
    pub fn has_ended(&self) -> bool {
        self.end.has_ended()
    }

    /// It ended on its own and was deleted: its id may be another device's
    /// now, so it must not be recorded any more.
    pub fn is_gone(&self) -> bool {
        self.end.is_gone()
    }

    /// It ended on its own but could not be deleted: why. The kernel device
    /// may still exist, holding the volume's I/O.
    pub fn end_failed(&self) -> Option<String> {
        self.end.end_failed()
    }

    /// Some engine of this device failed (a task of it panicked): its I/O
    /// fails with EIO until the volume is detached and attached again.
    pub fn engine_failed(&self) -> bool {
        self.stats.engine_panics.load(Ordering::Relaxed) > 0
    }
}

#[cfg(test)]
impl Running {
    /// A Running with no ublk device behind it, for the daemon's tests:
    /// `ended` = it has ended on its own.
    pub fn for_tests(spec: DeviceSpec, dev_id: i32, ended: bool) -> Running {
        Self::for_tests_ending(spec, dev_id, ended.then_some(Some(Ok(()))))
    }

    /// As `for_tests`, with how far it has ended on its own: None: it has
    /// not; Some(None): it is ending; Some(Some(r)): it ended, with `r`.
    pub fn for_tests_ending(spec: DeviceSpec, dev_id: i32, on_its_own: Option<Option<Result<(), String>>>) -> Running {
        let end = Arc::new(Ending::new());
        lock(&end.st).on_its_own = on_its_own;
        let addrs = addrs(&spec).unwrap_or_default();
        Running {
            spec,
            dev_id,
            stats: Arc::new(qengine::Stats::default()),
            ctrls: ctrls::unconnected_for_tests(addrs),
            draining: Arc::new(AtomicBool::new(false)),
            quiesce: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            leave: Arc::new(AtomicBool::new(false)),
            end,
        }
    }
}

/// A device that is up: the control that added or recovered it (and later
/// deletes it) and the queue threads serving it.
///
/// Dropped without `retire` (only when nothing could end it), it leaves the
/// device alone: the queue threads keep serving it. The control must not
/// delete it then: DEL_DEV stops the device and waits for its release, and
/// the dropping thread may not even have a control ring.
struct Served {
    ctrl: UblkCtrl,
    threads: Option<UblkTargetThreads>,
}

impl Drop for Served {
    fn drop(&mut self) {
        if self.threads.is_some() {
            self.ctrl.disown();
        }
    }
}

/// Key of the volume name in a device's libublk json (`target_data`).
const OWNER_KEY: &str = "nvmeublk_volume";

/// The volume a ublk device was added for, from the json libublk keeps for
/// it (Some(None): the json names none, e.g. an older build added it; None:
/// no json at all).
pub fn owner(dev_id: i32) -> Option<Option<String>> {
    let path = UblkCtrl::new_simple(dev_id).ok()?.run_path();
    let text = std::fs::read_to_string(path).ok()?;
    Some(owner_in(&text))
}

fn owner_in(json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    v.get("target_data")?.get(OWNER_KEY)?.as_str().map(str::to_string)
}

/// Before a recovery attempt reaches the target: is `dev_id` still `volume`'s
/// device, and can it enter recovery now? Ok(true): go ahead. Ok(false):
/// EBUSY, the previous server still has it open (try again later). Err: not
/// this volume's device (its id went to another volume), or gone.
///
/// EBUSY clears once the old server's char device release has run, which is
/// deferred (and in zero-copy mode waits for its io_uring buffer references):
/// usually within milliseconds of its exit. So EBUSY is polled here, from
/// 1 ms doubling to 100 ms as libublk's start_user_recover does, for
/// RECOVER_POLL_BUDGET of sleep, before the attempt gives its lane up for a
/// whole daemon RETRY.
pub fn recovery_check(volume: &str, dev_id: i32) -> Result<bool> {
    let ctrl = std::cell::OnceCell::new();
    recovery_check_with(
        volume,
        dev_id,
        owner,
        || {
            let c = match ctrl.get() {
                Some(c) => c,
                None => {
                    let c = UblkCtrl::new_simple(dev_id)?;
                    ctrl.get_or_init(|| c)
                }
            };
            c.try_start_user_recover()
        },
        std::thread::sleep,
    )
}

/// Sleep budget of `recovery_check` while the driver answers EBUSY.
const RECOVER_POLL_BUDGET: Duration = Duration::from_millis(1500);

fn recovery_check_with(
    volume: &str,
    dev_id: i32,
    owner: impl FnOnce(i32) -> Option<Option<String>>,
    mut try_once: impl FnMut() -> std::result::Result<i32, libublk::UblkError>,
    mut sleep: impl FnMut(Duration),
) -> Result<bool> {
    match owner(dev_id) {
        Some(Some(o)) if o != volume => bail!("ublk device {dev_id} now belongs to volume {o}, not {volume}; leaving it alone"),
        _ => {}
    }
    let (mut slept, mut delay) = (Duration::ZERO, Duration::from_millis(1));
    loop {
        let r = try_once().with_context(|| format!("start user recovery of ublk device {dev_id}"))?;
        if r == -libc::EBUSY {
            if slept >= RECOVER_POLL_BUDGET {
                return Ok(false);
            }
            sleep(delay);
            slept += delay;
            delay = (delay * 2).min(Duration::from_millis(100));
            continue;
        }
        if r < 0 {
            bail!("start user recovery of ublk device {dev_id}: {}", std::io::Error::from_raw_os_error(-r));
        }
        return Ok(true);
    }
}

fn fault_dir(dev_id: impl std::fmt::Display) -> String {
    format!("/run/nvmeublk/dev{dev_id}")
}

/// End a device whose queue threads have returned or are returning (it was
/// stopped, or they failed): wait for them, then delete the device.
fn retire(mut s: Served, stop: &AtomicBool) -> Result<()> {
    let dev_id = s.ctrl.dev_info().dev_id;
    let Some(threads) = s.threads.take() else { return Ok(()) };
    // Joined with the char device still open: the id stays this device's
    // until `held` is dropped, even if it was deleted from elsewhere.
    let held = match s.ctrl.join_target(threads) {
        Ok(h) => h,
        // Only creating this thread's control ring fails; without one the
        // control could not delete the device when dropped.
        Err(e) => {
            s.ctrl.disown();
            return Err(anyhow::Error::new(e).context(format!("end ublk device {dev_id}")));
        }
    };
    stop.store(true, Ordering::Release);
    // What the device left under /run goes while its id is its own: once
    // DEL returns, a device added for another volume may get the same id,
    // and with it the same paths.
    let _ = std::fs::remove_dir_all(fault_dir(dev_id));
    let _ = std::fs::remove_file(s.ctrl.run_path());
    let stopped = held.is_some() && s.ctrl.read_dev_info().is_ok() && s.ctrl.dev_info().state as u32 == libublk::sys::UBLK_S_DEV_DEAD;
    if stopped {
        // Stopped already, by detach or from elsewhere (which may have
        // deleted it too). Delete it while it is held, so this reaches no
        // other device, without waiting for the id: `held` holds it.
        let r = s.ctrl.del_dev_async();
        drop(held);
        r.with_context(|| format!("delete ublk device {dev_id}"))?;
    } else {
        // Its queue threads returned while it was live. Close the char
        // device first: only its release aborts the requests they had taken,
        // and STOP waits for them (UblkCtrl::wait_target).
        drop(held);
        let _ = s.ctrl.stop_dev();
        s.ctrl.del_dev().with_context(|| format!("delete ublk device {dev_id}"))?;
    }
    Ok(())
}

/// Delete a device that no process serves any more (its recovery was
/// cancelled by a detach): the I/O it holds fails with EIO. A device that is
/// gone already is fine. A live one is not the device the volume left
/// behind (its id was reused) and is left alone.
pub fn delete_unserved(dev_id: i32) -> Result<()> {
    let c = match UblkCtrl::new_simple(dev_id) {
        Ok(c) => c,
        Err(libublk::UblkError::UringIOError(e)) if e == -libc::ENODEV => return Ok(()),
        Err(e) => return Err(anyhow::Error::new(e).context(format!("open ublk device {dev_id}"))),
    };
    if c.dev_info().state as u32 == libublk::sys::UBLK_S_DEV_LIVE {
        log::warn!("ublk device {dev_id} is live, so it is not the device the volume left behind; not deleting it");
        return Ok(());
    }
    // Before DEL, as in retire.
    let _ = std::fs::remove_dir_all(fault_dir(dev_id));
    let _ = std::fs::remove_file(c.run_path());
    c.del_dev().with_context(|| format!("delete ublk device {dev_id}"))?;
    Ok(())
}

fn ident(spec: &DeviceSpec) -> Ident {
    match (&spec.hostnqn, &spec.hostid) {
        (Some(nqn), Some(id)) => {
            let hex: String = id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            let mut hostid = [0u8; 16];
            for (i, b) in hex.as_bytes().chunks(2).take(16).enumerate() {
                hostid[i] = u8::from_str_radix(std::str::from_utf8(b).unwrap_or("0"), 16).unwrap_or(0);
            }
            Ident { hostnqn: nqn.trim().to_string(), hostid, subnqn: spec.subnqn.clone() }
        }
        _ => host_ident(&spec.subnqn),
    }
}

/// Start serving `spec`. `recover` = Some((dev_id, hold_writes)) reattaches
/// an existing ublk device after this process (or a predecessor) exited;
/// hold_writes is false only when the predecessor drained cleanly.
pub fn start(spec: DeviceSpec, recover: Option<(i32, bool)>) -> Result<Running> {
    let ctrls = connect(&spec, recover.is_some())?;
    start_connected(spec, ctrls, recover)
}

/// The target portals of `spec`, resolved.
pub fn addrs(spec: &DeviceSpec) -> Result<Vec<SocketAddr>> {
    spec.addrs.iter().map(|a| a.to_socket_addrs().with_context(|| format!("bad address {a}"))?.next().context("unresolvable")).collect()
}

/// One try at bringing up the admin queue of every reachable path of `spec`.
pub fn connect_once(spec: &DeviceSpec, addrs: &[SocketAddr]) -> Result<Arc<ctrls::Ctrls>> {
    ctrls::Ctrls::new(addrs.to_vec(), ident(spec), KATO)
}

/// Bring up the admin queue of every reachable path of `spec`. With `retry`
/// (a device being recovered) this keeps trying every second while no path
/// is reachable: a recovering device holds its I/O until a server
/// reattaches, so giving up here would leave it frozen.
pub fn connect(spec: &DeviceSpec, retry: bool) -> Result<Arc<ctrls::Ctrls>> {
    let addrs = addrs(spec)?;
    loop {
        match connect_once(spec, &addrs) {
            Ok(c) => return Ok(c),
            Err(e) if retry => {
                log::warn!("{}: recovery: no path reachable yet ({e:#}); retrying", spec.volume);
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
}

/// `start` once `connect` has brought the paths up: bring the device up on
/// an op thread (`start_here`), waiting START_DEADLINE at most for a lane
/// and the device together. A device that comes up only after that is
/// served on, unrecorded, and ends on its own when it is stopped, as the
/// device's own thread served it to its end before there were op threads.
pub fn start_connected(spec: DeviceSpec, ctrls: Arc<ctrls::Ctrls>, recover: Option<(i32, bool)>) -> Result<Running> {
    let deadline = Instant::now() + START_DEADLINE;
    let Some(lane) = OP_LANES.acquire_until(deadline) else {
        ctrls.shutdown();
        bail!("{}: no control lane free within {} s; retry", spec.volume, START_DEADLINE.as_secs());
    };
    // Rendezvous: the device is handed over only while this side waits.
    let (tx, rx) = mpsc::sync_channel::<Result<Running>>(0);
    let (s2, c2) = (spec.clone(), ctrls.clone());
    let spawned = spawn_op(lane, move || {
        let vol = s2.volume.clone();
        if let Err(mpsc::SendError(Ok(r))) = tx.send(start_here(s2, c2, recover, deadline)) {
            log::error!("{vol}: ublk device {} came up after its start deadline; it is served until it is stopped", r.dev_id);
        }
    });
    if let Err(e) = spawned {
        ctrls.shutdown();
        return Err(anyhow::Error::new(e).context(format!("{}: start a control thread", spec.volume)));
    }
    match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(r) => r,
        Err(mpsc::RecvTimeoutError::Timeout) => bail!("{}: device did not come up within {} s", spec.volume, START_DEADLINE.as_secs()),
        Err(mpsc::RecvTimeoutError::Disconnected) => bail!("{}: device bring-up failed (its thread panicked)", spec.volume),
    }
}

/// Bring the device up on the calling thread, which should hold an op lane:
/// its control ring, and the io-wq worker behind it, live as long as the
/// thread. The admin paths are shut down if this fails. Never waits past
/// `deadline`, nor for a queue thread that returned before the device
/// started: the lane is always freed.
pub fn start_here(spec: DeviceSpec, ctrls: Arc<ctrls::Ctrls>, recover: Option<(i32, bool)>, deadline: Instant) -> Result<Running> {
    let r = start_here_inner(spec, ctrls.clone(), recover, deadline);
    if r.is_err() {
        ctrls.shutdown();
    }
    r
}

fn start_here_inner(spec: DeviceSpec, ctrls: Arc<ctrls::Ctrls>, recover: Option<(i32, bool)>, deadline: Instant) -> Result<Running> {
    let info = ctrls.info.clone();
    let write_fence = Duration::from_millis(spec.write_fence_ms.unwrap_or(KATO.as_millis() as u64 + 5000));
    let hold = recover.is_some_and(|(_, hold)| hold);
    if spec.zero_copy {
        let feats = UblkCtrl::get_features().unwrap_or(0);
        let need = (libublk::sys::UBLK_F_AUTO_BUF_REG | libublk::sys::UBLK_F_USER_COPY) as u64;
        if feats & need != need {
            bail!("zero copy requested but this kernel's ublk lacks AUTO_BUF_REG/USER_COPY (features {feats:#x})");
        }
    }
    let stats = Arc::new(qengine::Stats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let draining = Arc::new(AtomicBool::new(false));
    let quiesce = Arc::new(AtomicBool::new(false));
    let end = Arc::new(Ending::new());
    let leave = Arc::new(AtomicBool::new(false));
    let dev_id = bring_up(&spec, recover, hold, write_fence, info, stats.clone(), ctrls.clone(), stop.clone(), draining.clone(), quiesce.clone(), leave.clone(), end.clone(), deadline)?;
    Ok(Running { spec, dev_id, stats, ctrls, draining, quiesce, stop, leave, end })
}

/// ublk feature flags a new device is added with. A recovered device keeps
/// the flags it was added with: libublk replaces these with the driver's
/// copy when it opens the device for recovery.
fn ublk_flags(zero_copy: bool) -> u64 {
    use libublk::sys::*;
    // USER_RECOVERY + REISSUE: a restarted daemon reattaches the device and
    // gets the I/O that was in flight back. QUIESCE: QUIESCE_DEV, the only
    // per-device cancel that keeps the device, for moving one device between
    // servers in-process later. The driver requires USER_RECOVERY for it and
    // uses it nowhere else, so it changes nothing today; but flags are fixed
    // at ADD, so a device added without it can never get it.
    let mut flags = (UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE | UBLK_F_QUIESCE) as u64;
    if zero_copy {
        // SUPPORT_ZERO_COPY on top of USER_COPY + AUTO_BUF_REG only enables
        // UBLK_IO_(UN)REGISTER_IO_BUF, which nothing here sends yet: every
        // other test of it in the driver is ORed with USER_COPY/AUTO_BUF_REG
        // (need_map_io, need_req_ref, dropping NEED_GET_DATA). Never in the
        // copying mode, where it would switch the driver's data copy off.
        flags |= (UBLK_F_USER_COPY | UBLK_F_AUTO_BUF_REG | UBLK_F_SUPPORT_ZERO_COPY) as u64;
    }
    flags
}

/// Counts a queue thread out when it returns or unwinds. The last one out
/// sets the stop flag and shuts the admin paths down, as the device's own
/// thread did once every queue thread had returned, and if nobody is ending
/// the device, it ends on its own (`end_on_its_own`).
struct QueueExit {
    exited: Arc<AtomicUsize>,
    total: usize,
    stop: Arc<AtomicBool>,
    ctrls: Arc<ctrls::Ctrls>,
    end: Arc<Ending<Served>>,
}

impl Drop for QueueExit {
    fn drop(&mut self) {
        if self.exited.fetch_add(1, Ordering::AcqRel) + 1 != self.total {
            return;
        }
        self.stop.store(true, Ordering::Release);
        self.ctrls.shutdown();
        if self.end.queues_done() {
            let stop = self.stop.clone();
            let finish = move |s: Served| {
                let dev_id = s.ctrl.dev_info().dev_id;
                let r = retire(s, &stop);
                match &r {
                    Ok(()) => log::warn!("ublk device {dev_id}: its queue threads all returned without a detach; deleted it"),
                    Err(e) => log::error!("ublk device {dev_id}: its queue threads all returned without a detach; deleting it failed: {e:#}"),
                }
                r.map_err(|e| format!("{e:#}"))
            };
            // If it cannot be ended now, the char device closes with the
            // queue threads (`release`): the device can then be recovered or
            // deleted, and is ended at detach.
            end_on_its_own(&self.end, &OP_LANES, finish, |s| {
                if let Some(t) = s.threads.as_mut() {
                    t.release();
                }
            });
        }
    }
}

/// Every queue thread of a device returned while nobody was ending it: it
/// was stopped or deleted from elsewhere, or its queue loops failed. It is
/// stopped (its I/O fails with EIO) and deleted, as by a detach, on an op
/// thread: this queue thread cannot, since until it has returned it keeps
/// the char device open.
fn end_on_its_own<S: Send + 'static>(
    end: &Arc<Ending<S>>,
    lanes: &'static OpLanes,
    finish: impl FnOnce(S) -> std::result::Result<(), String> + Send + 'static,
    release: impl FnOnce(&mut S),
) {
    let e2 = end.clone();
    // Not waiting for a lane here: this thread holds the char device open
    // until it returns, which would keep the device's I/O waiting and a DEL
    // from elsewhere hanging for as long as every lane is busy.
    let spawned = spawn_op_waiting(lanes, move || {
        let Some(s) = e2.take_own() else { return e2.ended(Ok(())) };
        let r = finish(s);
        e2.ended(r);
    });
    if let Err(e) = spawned {
        // Holding the char device open would keep its I/O waiting and a DEL
        // from elsewhere hanging until a detach. Let it close with the queue
        // threads: the device can then be recovered or deleted.
        log::error!("no thread to end a ublk device whose queue threads returned ({e}); it is ended at detach");
        end.give_back(release);
    }
}

/// Create (or reopen for recovery) the ublk device and start its queue
/// threads; returns its id once it is up.
#[allow(clippy::too_many_arguments)]
fn bring_up(
    spec: &DeviceSpec,
    recover: Option<(i32, bool)>,
    hold: bool,
    write_fence: Duration,
    info: crate::conn::NsInfo,
    stats: Arc<qengine::Stats>,
    ctrls: Arc<ctrls::Ctrls>,
    stop: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    quiesce: Arc<AtomicBool>,
    // The queue loops return once this is set: by a start that gave up, or
    // by a detach (`Running::leave`).
    abandoned: Arc<AtomicBool>,
    end: Arc<Ending<Served>>,
    deadline: Instant,
) -> Result<i32> {
    let (queues, depth) = (spec.queues.max(1), spec.depth.max(2));
    // Largest request the device takes. Zero copy moves data straight
    // between the socket and the request pages, so a larger cap costs no
    // buffer memory there (tag buffers are in-capsule sized) and keeps a 1M
    // request as one NVMe command; the copying mode allocates io_buf per tag.
    // NVMEUBLK_MAX_IO_KB (tuning): zero-copy default 1024, copying 512.
    let max_io = if spec.zero_copy { std::env::var("NVMEUBLK_MAX_IO_KB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1024) } else { 512 };
    let io_buf = (max_io.clamp(4, 32 * 1024) * 1024).min(info.mdts_bytes) as u32;
    let size = info.nsze << info.lba_shift;
    let lba_shift = info.lba_shift as u8;
    let flags = ublk_flags(spec.zero_copy);
    let threads = spec.threads_per_queue.clamp(1, depth);
    let tag_chunk = spec.tag_chunk.max(1);
    // Several threads per queue need UBLK_F_PER_IO_DAEMON, which the driver
    // advertises by itself (6.16+) and libublk checks after the device is
    // added; it is not a flag the server may request.
    let tag_flags = if threads > 1 && spec.seq_tags { UblkFlags::UBLK_DEV_F_SEQ_TAG_PARTITION } else { UblkFlags::empty() };
    let builder = UblkCtrlBuilder::default().name("nvmeublk").nr_queues(queues).depth(depth).io_buf_bytes(io_buf).ctrl_flags(flags).io_threads_per_queue(threads);
    let builder = match recover {
        Some((id, _)) => {
            // One try: the daemon checked it moments ago (recovery_check),
            // and an EBUSY now is tried again later, holding no lane.
            let r = UblkCtrl::new_simple(id)?.try_start_user_recover().context("start user recovery")?;
            if r == -libc::EBUSY {
                return Err(anyhow::Error::new(TryAgain(format!("ublk device {id} is not recoverable yet (EBUSY: its previous server still has it open)"))));
            }
            if r < 0 {
                bail!("start user recovery of ublk device {id}: {}", std::io::Error::from_raw_os_error(-r));
            }
            log::info!("{}: recovering ublk device {id}{}", spec.volume, if hold { " (writes held for one fence)" } else { " (clean handover)" });
            builder.id(id).dev_flags(UblkFlags::UBLK_DEV_F_RECOVER_DEV | tag_flags)
        }
        None => builder.dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV | tag_flags),
    };
    let ctrl = builder.build().context("create ublk device (is ublk_drv loaded?)")?;
    let dev_id = ctrl.dev_info().dev_id as i32;
    let fault_dir = fault_dir(dev_id);
    let _ = std::fs::create_dir_all(&fault_dir);
    // Fault injection fans a command out to one file per engine, and there
    // is one engine per io thread (engine id = queue * threads + thread).
    let _ = std::fs::write(format!("{fault_dir}/queues"), (queues * threads).to_string());
    let cfg = qengine::QConfig {
        io_timeout: Duration::from_millis(spec.io_timeout_ms),
        no_path_timeout: Duration::from_millis(spec.no_path_timeout_ms),
        max_attempts: 8,
        write_fence,
        // After a crash the dead server's writes may still be running on the
        // target, and REISSUE hands them to us: hold writes for one fence.
        hold_writes_until: hold.then(|| Instant::now() + write_fence),
        rx_offload: 0,
        cdev_fd: -1,
        conns_per_path: spec.conns_per_path.max(1),
        rx_chunk: spec.rx_chunk.max(64),
        napi_us: spec.napi_us,
        fault_dir,
        quiesce,
    };
    log::info!(
        "{}: {} blocks of {} B, {} queues x {} ({} threads/queue{}, tag chunk {}), zero_copy={} napi_us={} write fence {} ms",
        spec.volume,
        info.nsze,
        1u64 << info.lba_shift,
        queues,
        depth,
        threads,
        if spec.seq_tags { ", contiguous tags" } else { "" },
        tag_chunk,
        spec.zero_copy,
        spec.napi_us,
        write_fence.as_millis()
    );
    let (sq, stq, drq, eq) = (stats.clone(), stop.clone(), draining.clone(), end.clone());
    let exited = Arc::new(AtomicUsize::new(0));
    let ab = abandoned.clone();
    let volume = spec.volume.clone();
    let started = ctrl.start_target_until(
        move |dev: &mut UblkDev| {
            dev.set_default_params(size);
            // Whose device this is, for a recovery to check (`owner`).
            dev.set_target_json(serde_json::json!({ OWNER_KEY: volume }));
            dev.set_io_tag_chunk(tag_chunk);
            dev.tgt.params.basic.logical_bs_shift = lba_shift;
            dev.tgt.params.basic.physical_bs_shift = lba_shift.max(12);
            // Room on each queue ring for the network SQEs (recv + writev per
            // path, timer, reconnect wakeup) next to the ublk commands.
            dev.tgt.sq_depth = depth * 2 + 64;
            dev.tgt.cq_depth = depth * 2 + 64;
            Ok(())
        },
        move |qid, dev: &_| {
            // libublk runs one thread per (queue, io thread); the kernel may
            // have trimmed the queue count, so count from the device.
            let total = dev.dev_info.nr_hw_queues as usize * dev.io_threads_per_queue() as usize;
            let _out = QueueExit { exited: exited.clone(), total, stop: stq.clone(), ctrls: ctrls.clone(), end: eq.clone() };
            queue_fn(qid, dev, ctrls.clone(), sq.clone(), stq.clone(), drq.clone(), ab.clone(), cfg.clone())
        },
        Some(deadline),
    );
    let target = match started {
        Ok(t) => t,
        Err(e) => {
            // A queue thread returned before the device started (its setup
            // failed), or the deadline passed: START was cancelled. The queue
            // threads still running leave their loops.
            abandoned.store(true, Ordering::Release);
            let why = anyhow::Error::new(e).context(format!("start ublk device {dev_id}"));
            if recover.is_some() {
                // Not deleted: the device still holds the volume's I/O, and
                // once these threads are gone it can be recovered again.
                return Err(anyhow::Error::new(TryAgain(format!("{why:#}"))));
            }
            // What it left under /run goes first, while its id is its own
            // (as in retire): the id may be another volume's device's as soon
            // as DEL has run.
            let _ = std::fs::remove_dir_all(format!("/run/nvmeublk/dev{dev_id}"));
            let _ = std::fs::remove_file(ctrl.run_path());
            // Deleted without waiting for its id: DEL cancels the tags
            // FETCHed so far, and the id is freed once the queue threads
            // have closed the char device. Dropping `ctrl` then deletes
            // nothing more.
            if let Err(d) = ctrl.del_dev_async() {
                log::error!("{}: delete ublk device {dev_id} after a failed start: {d}", spec.volume);
            }
            return Err(why);
        }
    };
    log::info!("{}: serving /dev/ublkb{dev_id}", spec.volume);
    if let Some(s) = end.up(Served { ctrl, threads: Some(target) }) {
        // Its queue threads all returned during START: it ends on its own,
        // here, on this op thread.
        let r = retire(s, &stop);
        if let Err(e) = &r {
            log::error!("{}: ublk device {dev_id} stopped as it started; deleting it failed: {e:#}", spec.volume);
        } else {
            log::warn!("{}: ublk device {dev_id} stopped as it started; deleted it", spec.volume);
        }
        end.ended(r.map_err(|e| format!("{e:#}")));
    }
    Ok(dev_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use libublk::sys::*;

    #[test]
    fn new_devices_can_be_quiesced() {
        for zero_copy in [false, true] {
            let f = super::ublk_flags(zero_copy);
            assert_ne!(f & UBLK_F_QUIESCE as u64, 0, "zero_copy={zero_copy}");
            // ADD_DEV refuses QUIESCE without USER_RECOVERY.
            assert_ne!(f & UBLK_F_USER_RECOVERY as u64, 0, "zero_copy={zero_copy}");
            assert_ne!(f & UBLK_F_USER_RECOVERY_REISSUE as u64, 0, "zero_copy={zero_copy}");
        }
    }

    #[test]
    fn zero_copy_flag_only_with_user_copy_and_auto_buf_reg() {
        let zc = (UBLK_F_SUPPORT_ZERO_COPY | UBLK_F_USER_COPY | UBLK_F_AUTO_BUF_REG) as u64;
        // Copying mode: SUPPORT_ZERO_COPY alone would turn the driver's copy off.
        assert_eq!(super::ublk_flags(false) & zc, 0);
        assert_eq!(super::ublk_flags(true) & zc, zc);
    }

    /// libublk refuses any flag outside its UBLK_DRV_F_ALL with InvalidVal
    /// before it touches the driver (the device open fails with EACCES
    /// without root, and with root id -1 adds nothing). The unknown bit shows
    /// the check runs here; the device's flags must pass it.
    #[test]
    fn libublk_accepts_the_flags() {
        let new = |flags| UblkCtrl::new(None, -1, 1, 64, 4096, flags, 0, UblkFlags::empty());
        assert!(matches!(new(1u64 << 63), Err(libublk::UblkError::InvalidVal)), "libublk no longer validates flags");
        for zero_copy in [false, true] {
            assert!(!matches!(new(super::ublk_flags(zero_copy)), Err(libublk::UblkError::InvalidVal)), "zero_copy={zero_copy}");
        }
    }

    fn lanes() -> &'static OpLanes {
        Box::leak(Box::new(OpLanes::new()))
    }

    #[test]
    fn general_operations_get_four_lanes_and_never_the_reserved_one() {
        let l = lanes();
        let held: Vec<OpLane> = (0..GENERAL_LANES).map(|_| l.acquire()).collect();
        assert!(held.iter().all(|h| h.n < GENERAL_LANES));
        assert!(l.acquire_until(Instant::now() + Duration::from_millis(100)).is_none(), "a fifth general lane was handed out");
        // The reserved lane is still there for a recovery.
        let r = l.try_acquire_recovery().expect("recovery lost its reserved lane");
        assert_eq!(r.n, GENERAL_LANES);
        assert!(l.try_acquire_recovery().is_none(), "a sixth lane was handed out");
        drop(held);
        drop(r);
        assert!(lock(&l.st).busy.iter().all(|b| !b));
    }

    #[test]
    fn recovery_takes_no_general_lane_a_general_operation_waits_for() {
        let l = lanes();
        let r = l.try_acquire_recovery().unwrap();
        let held: Vec<OpLane> = (0..GENERAL_LANES).map(|_| l.acquire()).collect();
        let (tx, rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || tx.send(l.acquire().n).unwrap());
        while lock(&l.st).waiting == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut held = held;
        let freed = held.pop().unwrap().n;
        // The freed lane is the waiting attach's, not a recovery's.
        assert!(l.try_acquire_recovery().is_none(), "recovery took the lane an attach was waiting for");
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), freed);
        waiter.join().unwrap();
        drop((r, held));
        // With nobody waiting, recovery may use any free lane.
        let all: Vec<OpLane> = (0..LANES).map(|_| l.try_acquire_recovery().unwrap()).collect();
        assert_eq!(all.len(), LANES);
    }

    #[test]
    fn an_op_thread_holds_its_lane_until_it_returns() {
        let l = lanes();
        let (go, wait) = mpsc::channel::<()>();
        let (done, finished) = mpsc::channel();
        spawn_op(l.try_acquire_recovery().unwrap(), move || {
            assert_eq!(std::thread::current().name(), Some("nvq-op-4"));
            wait.recv().unwrap();
            done.send(()).unwrap();
        })
        .unwrap();
        assert!(lock(&l.st).busy[GENERAL_LANES]);
        go.send(()).unwrap();
        finished.recv_timeout(Duration::from_secs(5)).unwrap();
        let t0 = Instant::now();
        while lock(&l.st).busy[GENERAL_LANES] {
            assert!(t0.elapsed() < Duration::from_secs(5), "the lane outlived its op");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// The queue threads all return while nobody is ending the device (it
    /// was deleted from elsewhere, or its loops failed): the device ends on
    /// its own, and a later detach reports that instead of holding the
    /// device forever.
    #[test]
    fn a_device_whose_queues_all_return_ends_on_its_own() {
        let e = Arc::new(Ending::new());
        assert!(e.up(1u32).is_none());
        assert!(e.queues_done(), "nobody ends a device whose queue threads returned");
        let e2 = e.clone();
        let detach = std::thread::spawn(move || match e2.take() {
            Owner::Ended(r) => r,
            Owner::Own(_) => panic!("detach took a device that was ending on its own"),
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!detach.is_finished(), "detach did not wait for the device to end");
        assert_eq!(e.take_own(), Some(1));
        e.ended(Err("gone".into()));
        assert_eq!(detach.join().unwrap(), Err("gone".to_string()));
    }

    #[test]
    fn a_detached_device_does_not_end_on_its_own() {
        let e = Ending::new();
        assert!(e.up(1u32).is_none());
        assert!(matches!(e.take(), Owner::Own(1)));
        assert!(!e.queues_done(), "a device being detached also ended on its own");
    }

    #[test]
    fn a_device_whose_queues_returned_during_start_is_handed_back() {
        let e = Ending::new();
        assert!(!e.queues_done());
        assert_eq!(e.up(1u32), Some(1), "bring-up kept a device nobody will end");
        e.ended(Ok(()));
        assert!(matches!(e.take(), Owner::Ended(Ok(()))));
    }

    #[test]
    fn a_device_nothing_could_end_goes_back_to_its_owner() {
        let e = Ending::new();
        assert!(e.up(1u32).is_none());
        assert!(e.queues_done());
        let mut prepared = false;
        e.give_back(|_| prepared = true);
        assert!(prepared);
        assert!(matches!(e.take(), Owner::Own(1)));
    }

    #[test]
    fn a_failed_stop_puts_the_device_back_unless_its_queues_returned() {
        let e = Ending::new();
        assert!(e.up(1u32).is_none());
        let Owner::Own(s) = e.take() else { panic!() };
        assert_eq!(e.put_back(s), None);
        // Put back, it ends on its own once its queue threads return.
        assert!(e.queues_done());
        let e = Ending::new();
        assert!(e.up(2u32).is_none());
        let Owner::Own(s) = e.take() else { panic!() };
        assert!(!e.queues_done());
        assert_eq!(e.put_back(s), Some(2), "a device whose queues returned was put back where nothing ends it");
    }

    /// A detach whose STOP fails (the node is out of fds, say) leaves the
    /// device as it was: served, taken back by its Ending, and not draining
    /// (draining would fail every failover and parked request with EIO). The
    /// caller then keeps its admin paths up and keeps it recorded.
    #[test]
    fn a_failed_stop_leaves_the_device_served_and_not_draining() {
        let draining = AtomicBool::new(false);
        let e = Ending::new();
        assert!(e.up(1u32).is_none());
        let Owner::Own(s) = e.take() else { panic!() };
        let emfile = || Err::<(), _>(std::io::Error::from_raw_os_error(libc::EMFILE));
        match stop_or_put_back(&e, s, &draining, emfile) {
            Stop::StillServed(err) => assert!(format!("{err:#}").contains("24") || format!("{err:#}").to_lowercase().contains("too many"), "{err:#}"),
            Stop::Retire(_) => panic!("a device that could not be stopped was retired"),
        }
        assert!(!draining.load(Ordering::Acquire), "a device still served was left draining (every failover answers EIO)");
        assert!(matches!(e.take(), Owner::Own(1)), "the device was not put back");
        // A STOP that works retires it, draining.
        let e = Ending::new();
        assert!(e.up(2u32).is_none());
        let Owner::Own(s) = e.take() else { panic!() };
        assert!(matches!(stop_or_put_back(&e, s, &draining, || Ok::<(), std::io::Error>(())), Stop::Retire(2)));
        assert!(draining.load(Ordering::Acquire));
        // Queue threads all returned already: retired, and no STOP is sent.
        let e = Ending::new();
        assert!(e.up(3u32).is_none());
        let Owner::Own(s) = e.take() else { panic!() };
        assert!(!e.queues_done());
        let r = stop_or_put_back(&e, s, &draining, || -> Result<(), std::io::Error> { panic!("STOP sent to a device whose queues returned") });
        assert!(matches!(r, Stop::Retire(3)));
    }

    #[test]
    fn try_again_is_seen_through_context() {
        let e = anyhow::Error::new(TryAgain("busy".into())).context("start");
        assert!(is_try_again(&e));
        assert!(!is_try_again(&anyhow!("hard").context("start")));
    }

    #[test]
    fn the_owner_of_a_device_is_read_from_its_json() {
        assert_eq!(owner_in(r#"{"dev_info":{},"target_data":{"nvmeublk_volume":"pvc-1"}}"#), Some("pvc-1".into()));
        assert_eq!(owner_in(r#"{"dev_info":{}}"#), None);
        assert_eq!(owner_in("not json"), None);
    }

    /// A queue thread whose device ends on its own must not wait for a lane
    /// while it holds the char device open: the op runs on a thread of its
    /// own once a lane is free.
    #[test]
    fn ending_a_device_does_not_block_its_queue_thread_on_a_lane() {
        let l = lanes();
        let held: Vec<OpLane> = (0..GENERAL_LANES).map(|_| l.acquire()).collect();
        let (tx, rx) = mpsc::channel();
        let t0 = Instant::now();
        spawn_op_waiting(l, move || tx.send(()).unwrap()).unwrap();
        assert!(t0.elapsed() < Duration::from_millis(100), "the caller waited for a lane");
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err(), "ran without a lane");
        drop(held);
        rx.recv_timeout(Duration::from_secs(5)).expect("never ran once a lane was free");
    }

    /// The queue thread that finds its device ending on its own
    /// (QueueExit) calls end_on_its_own: with every general lane busy it
    /// must return at once, and the device ends once a lane is free.
    #[test]
    fn end_on_its_own_returns_at_once_with_every_lane_busy() {
        let l = lanes();
        let held: Vec<OpLane> = (0..GENERAL_LANES).map(|_| l.acquire()).collect();
        let e = Arc::new(Ending::new());
        assert!(e.up(7u32).is_none());
        assert!(e.queues_done());
        let (tx, rx) = mpsc::channel();
        let e2 = e.clone();
        std::thread::spawn(move || {
            end_on_its_own(&e2, l, |s| if s == 7 { Ok(()) } else { Err(format!("ended {s}")) }, |_| panic!("given back although a thread started"));
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(2)).expect("the queue thread waited for a lane");
        assert!(matches!(lock(&e.st).on_its_own, Some(None)), "the device ended without a lane");
        assert!(e.has_ended() && !e.is_gone(), "a device still ending counted as gone");
        drop(held);
        let t0 = Instant::now();
        while !e.is_gone() {
            assert!(t0.elapsed() < Duration::from_secs(5), "never ended once a lane was free");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// A device whose deletion failed is not gone: its id is still its own
    /// and the kernel device may still hold its I/O.
    #[test]
    fn a_device_whose_deletion_failed_is_not_gone() {
        let e: Ending<u32> = Ending::new();
        lock(&e.st).on_its_own = Some(Some(Err("EMFILE".into())));
        assert!(e.has_ended());
        assert!(!e.is_gone());
        assert_eq!(e.end_failed(), Some("EMFILE".to_string()));
        lock(&e.st).on_its_own = Some(Some(Ok(())));
        assert!(e.is_gone());
        assert_eq!(e.end_failed(), None);
    }

    /// Detach hands the device back, admin paths up, when it cannot stop it
    /// or when it ended on its own and could not be deleted; otherwise its
    /// admin paths go down.
    #[test]
    fn a_kept_device_keeps_its_admin_paths() {
        let spec: DeviceSpec = serde_json::from_value(serde_json::json!({"volume": "a", "subnqn": "nqn.2026-09.test:sub", "addrs": ["192.0.2.1:4420"]})).unwrap();
        let r = Running::for_tests(spec.clone(), 5, false);
        let ctrls = r.ctrls.clone();
        match r.detached(Err((anyhow!("STOP failed"), true))) {
            Err(DetachFailed { still_served: Some(r), .. }) => assert_eq!(r.dev_id, 5),
            _ => panic!("a device that could not be stopped was not handed back"),
        }
        assert!(!ctrls.is_shut_down(), "the admin paths of a device still served went down");
        let r = Running::for_tests(spec.clone(), 6, false);
        let ctrls = r.ctrls.clone();
        assert!(matches!(r.detached(Err((anyhow!("gone"), false))), Err(DetachFailed { still_served: None, .. })));
        assert!(ctrls.is_shut_down());
        let r = Running::for_tests(spec.clone(), 7, false);
        let ctrls = r.ctrls.clone();
        assert!(r.detached(Ok(())).is_ok());
        assert!(ctrls.is_shut_down());
        // Ended on its own, deleting it failed: kept by a detach.
        let r = Running::for_tests_ending(spec, 8, Some(Some(Err("EMFILE".into()))));
        match r.detach() {
            Err(DetachFailed { still_served: Some(r), .. }) => assert_eq!(r.end_failed().as_deref(), Some("EMFILE")),
            _ => panic!("a device that may still exist was dropped by a detach"),
        }
    }

    /// EBUSY right after the old server exits is polled with a short
    /// backoff, not handed back for a whole daemon RETRY.
    #[test]
    fn recovery_polls_a_short_ebusy() {
        let mut answers = vec![0, -libc::EBUSY, -libc::EBUSY, -libc::EBUSY];
        let mut slept = Vec::new();
        let r = recovery_check_with("a", 5, |_| Some(Some("a".into())), || Ok(answers.pop().unwrap()), |d| slept.push(d));
        assert!(r.unwrap(), "a device recoverable 7 ms later was put off");
        assert_eq!(slept, [1, 2, 4].map(Duration::from_millis));
        // EBUSY that lasts: given up after the budget.
        let mut total = Duration::ZERO;
        let r = recovery_check_with("a", 5, |_| None, || Ok(-libc::EBUSY), |d| total += d);
        assert!(!r.unwrap());
        assert!(total >= RECOVER_POLL_BUDGET && total <= RECOVER_POLL_BUDGET + Duration::from_millis(100), "{total:?}");
        // Another volume's device: not tried at all.
        let r = recovery_check_with("a", 5, |_| Some(Some("b".into())), || panic!("recovery started on another volume's device"), |_| {});
        assert!(r.is_err());
        // Another error: not polled.
        let r = recovery_check_with("a", 5, |_| None, || Ok(-libc::EINVAL), |_| panic!("slept on EINVAL"));
        assert!(r.is_err());
    }
}
