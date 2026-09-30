//! Batch I/O queue threads (`UBLK_F_BATCH_IO`, kernel 7.x; opt-in with
//! NVMEUBLK_BATCH_IO=1 or `DeviceSpec::batch_io`).
//!
//! In the default per-tag mode every blk-mq tag belongs to one fixed thread
//! (UBLK_F_PER_IO_DAEMON with tag chunks), so a low-depth stream whose
//! submitter moves between CPUs lands on different tags, threads and
//! connections, each of them cold. In batch mode tags are not partitioned:
//! every io thread of a queue runs its own engine (its own connections, as
//! before) and keeps its own multishot `FETCH_IO_CMDS` on the whole queue.
//!
//! Driver behaviour this relies on (drivers/block/ublk_drv.c, v7.2.5):
//! - A queue keeps its fetch commands in a list (`fcmd_head`); new tags go
//!   to the first one (`__ublk_acquire_fcmd`, list_first_entry). A fetch
//!   whose provided buffers ran out completes with -ENOBUFS and leaves the
//!   list (`__ublk_batch_dispatch` -> `ublk_batch_deinit_fetch_buf`); the
//!   tags stay queued and go to the next fetch. A posted fetch joins at the
//!   tail (`ublk_batch_attach`, list_add_tail).
//! - Any task may commit a tag (`io->task` is NULL in batch mode), but with
//!   AUTO_BUF_REG the request's pages are registered in the *fetching*
//!   ring (`__ublk_batch_prep_dispatch` registers through the fetch
//!   command), and a commit unregisters them only when it comes from that
//!   ring (`ublk_clear_auto_buf_reg`). So the thread that fetched a tag
//!   serves and commits it.
//! - PREP_IO_CMDS accepts each tag once (`__ublk_fetch`: -EINVAL when it is
//!   already active), and a fetch is refused with -ENODEV while the queue is
//!   canceling, which a recovering queue is until every tag is prepared
//!   again (`ublk_mark_io_ready`). Thread 0 of each queue prepares all its
//!   tags; the other threads post their fetches only after that.
//!
//! Spill: each thread provides one-tag fetch buffers as credits (libublk
//! `UblkBatchConfig::with_spill_tags`): it keeps `spill` minus the tags it
//! holds provided, and hands a credit back when it commits a tag. At low
//! depth the first thread on the list never runs out and serves everything;
//! the request that finds it holding `spill` ends its fetch with -ENOBUFS and
//! goes to the next thread. The spilled thread posts its fetch again at once
//! (at the tail) with up to `spill` more credits, so under pressure the
//! queue rotates over its threads in runs of `spill` requests.
//!
//! Hot lane (`DeviceSpec::hot_lane`, NVMEUBLK_HOT_LANE=1; design doc
//! nvmeublk-userspace-architecture.md §4): the threads of a queue are not
//! equal. One is the queue's *primary*: its fetch is first on the driver's
//! list, it may hold up to `depth - lease` requests (weighted) and keeps at
//! most `lease` credits provided at once. The others are *secondaries* with
//! a few credits each (`secondary`, 4) that are not given back while their
//! fetch is armed: a secondary takes at most that many requests per turn at
//! the head of the list, then its fetch ends and rejoins at the tail, so the
//! head returns to the primary within a few requests after it spilled. A
//! low-depth stream therefore stays on one thread, one set of connections
//! and one warm vCPU; depth spills.
//! - Warm window: the primary polls its ring without sleeping while it has
//!   commands on the wire and for `warm = clamp(1.5 x EWMA(wire RTT), 100 µs,
//!   1 ms)` after the last one, then sleeps (NAPI busy poll still runs inside
//!   the sleep for its budget). An idle queue spins no thread. Secondaries
//!   poll without sleeping only while they hold requests and saw an event in
//!   the last NVMEUBLK_SPIN_US.
//! - Watchdog: every thread stamps a heartbeat each turn of its loop and says
//!   when it sleeps in the ring on purpose. A secondary that finds the primary
//!   neither turning nor sleeping for `wedge` (1 s) takes the primary role
//!   over (its credits become the primary's), and a primary that finds it was
//!   replaced steps down. Meanwhile the lease bounds the damage: a primary
//!   that stops turning still has its driver task work run (the kernel runs
//!   it at the next return to user mode, and wakes an interruptible sleep for
//!   it), so its fetch takes at most `lease` more requests, runs out and
//!   leaves the list, and the queue's next requests go to the secondaries at
//!   once. Only the requests the stuck thread already took wait for it: their
//!   pages are registered in its ring (AUTO_BUF_REG), so no other thread can
//!   serve them. With `depth - lease` as the primary's cap there are always
//!   at least `lease` tags it cannot hold.

use crate::{ctrls, env_u64, qengine, serve_request, setup_queue_ring};
use libublk::helpers::IoBuf;
use libublk::io::{UblkBatchBuffers, UblkBatchCompletion, UblkBatchConfig, UblkBatchQueue, UblkDev, UblkQueue, UblkSharedSlot};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Flags under which the driver moves no data at fetch/commit (no per-tag
/// userspace buffers): user copy, zero copy, auto buffer registration.
const NO_MAP_IO: u64 = (libublk::sys::UBLK_F_USER_COPY | libublk::sys::UBLK_F_SUPPORT_ZERO_COPY | libublk::sys::UBLK_F_AUTO_BUF_REG) as u64;

/// Tag buffers of one queue in the copying mode: one per tag, shared by the
/// queue's threads (the driver copies to and from the address named by a
/// tag's last prepare or commit, and any thread may fetch the tag).
type SharedBufs = Arc<Vec<IoBuf<u8>>>;

enum Prep {
    Waiting,
    Ready(Option<SharedBufs>),
    Failed,
}

/// Hot-lane parameters (process-wide tuning, from the environment).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HotLane {
    /// Most credits the primary keeps provided at once while the queue is
    /// shallow (NVMEUBLK_HOT_LEASE, default 4); the primary holds at most
    /// depth - lease requests. Also the depth-mode threshold: more than
    /// `lease` requests held by the queue's threads switches it to depth
    /// mode, `lease / 2` or fewer switches it back.
    pub lease: u16,
    /// Credits of a secondary while the queue is shallow
    /// (NVMEUBLK_HOT_SECONDARY, default 4).
    pub secondary: u16,
    /// Depth mode: credits every thread gets per turn at the head of the
    /// driver's fetch list (NVMEUBLK_HOT_DEEP_LEASE, default 2), i.e. the
    /// run of consecutive requests one thread takes before the queue's next
    /// ones go to the next thread. Spill-only for every thread (no refill
    /// while armed: a thread that refilled after each fetch would keep the
    /// head at a closed-loop arrival rate and funnel the queue again), so the
    /// queue rotates over all its threads in runs of this many, as the
    /// per-tag layout's tag chunks do.
    pub deep_lease: u16,
    /// Depth mode: threads holding requests keep polling for NVMEUBLK_SPIN_US
    /// after their last event, as the per-tag loop does
    /// (NVMEUBLK_HOT_DEEP_SPIN, default off: at depth completions arrive
    /// faster than a sleep costs).
    pub deep_spin: bool,
    /// A primary neither turning nor sleeping this long is replaced
    /// (NVMEUBLK_HOT_WEDGE_MS, default 1000).
    pub wedge: Duration,
    /// Depth mode while the queue's requests are small (see `small_bytes`):
    /// the run length instead of `deep_lease` (NVMEUBLK_HOT_DEEP_LEASE_SMALL,
    /// default 4). Runs of 4 small requests put a turn's commands in one send
    /// and rotate the fetch half as often: read 4k 64:8 +33%, randread 4k
    /// 16:1 +11% (run gD); runs of 4 cost 1M read QD16 x1 17%, and 8 or 16
    /// collapsed 4k 16:1 to 29-36k IOPS.
    pub deep_lease_small: u16,
    /// A queue's requests are small while the moving average of their size
    /// is at most this many bytes (NVMEUBLK_HOT_SMALL_BYTES, default 8192);
    /// they stop being small above twice that.
    pub small_bytes: u32,
    /// Depth mode: a thread takes new requests only while it holds at most
    /// this share (percent) of its fair part of what the queue's threads
    /// hold (NVMEUBLK_HOT_DEEP_SHARE, 0 = no limit but the cap). The
    /// driver hands a queue's requests to its threads in turn whatever
    /// each already holds, so a thread that serves slower (its reactor
    /// preempted, say) piled up the backlog: read 16k 64:8 had one
    /// reactor's four connections at 23-30 commands each and another's at
    /// 5, and the queued ones were the slowest 1% (iso r1-u-read16k).
    pub deep_share_pct: u16,
    /// Whether the shallow primary spins while hot (NVMEUBLK_HOT_SPIN,
    /// default on). Off: it sleeps in its ring between events, like a
    /// secondary with nothing held, and relies on the guest halt-poll
    /// window (and NAPI busy poll, NVMEUBLK_NAPI_US) for a cheap wake.
    pub primary_spin: bool,
}

impl Default for HotLane {
    fn default() -> Self {
        HotLane { lease: 4, secondary: 4, deep_lease: 2, deep_spin: false, wedge: Duration::from_millis(1000), primary_spin: true, deep_lease_small: 4, small_bytes: 8192, deep_share_pct: DEEP_SHARE_DEFAULT }
    }
}

impl HotLane {
    pub fn from_env() -> Self {
        let d = HotLane::default();
        HotLane {
            lease: env_u64("NVMEUBLK_HOT_LEASE", d.lease as u64).clamp(1, u16::MAX as u64) as u16,
            secondary: env_u64("NVMEUBLK_HOT_SECONDARY", d.secondary as u64).clamp(1, u16::MAX as u64) as u16,
            deep_lease: env_u64("NVMEUBLK_HOT_DEEP_LEASE", d.deep_lease as u64).clamp(1, u16::MAX as u64) as u16,
            deep_spin: env_u64("NVMEUBLK_HOT_DEEP_SPIN", d.deep_spin as u64) != 0,
            wedge: Duration::from_millis(env_u64("NVMEUBLK_HOT_WEDGE_MS", d.wedge.as_millis() as u64).max(10)),
            primary_spin: env_u64("NVMEUBLK_HOT_SPIN", d.primary_spin as u64) != 0,
            deep_lease_small: env_u64("NVMEUBLK_HOT_DEEP_LEASE_SMALL", d.deep_lease_small as u64).clamp(1, u16::MAX as u64) as u16,
            small_bytes: env_u64("NVMEUBLK_HOT_SMALL_BYTES", d.small_bytes as u64).min(u32::MAX as u64 / 2) as u32,
            deep_share_pct: env_u64("NVMEUBLK_HOT_DEEP_SHARE", d.deep_share_pct as u64).min(10_000) as u16,
        }
    }

    /// (spill cap, lease, refill) of a thread in this role, for a queue of
    /// `depth` tags, shallow (`deep` false) or in depth mode.
    ///
    /// Shallow: the primary may hold all but `lease` tags and refills; a
    /// secondary has `secondary` spill-only credits, so the head of the
    /// driver's fetch list returns to the primary after a short run.
    /// Depth mode: every thread gets `deep_lease` spill-only credits per
    /// turn at the head, up to the primary's cap, so the queue rotates over
    /// all its threads in runs of `deep_lease` requests; the cap still leaves
    /// `lease` tags that one (possibly wedged) thread can never take.
    pub fn credits(&self, primary: bool, deep: bool, depth: u16) -> (u16, u16, bool) {
        self.credits_sized(primary, deep, false, depth)
    }

    /// As `credits`, for a queue whose requests are small (`small`: depth
    /// mode runs `deep_lease_small` long instead of `deep_lease`).
    pub fn credits_sized(&self, primary: bool, deep: bool, small: bool, depth: u16) -> (u16, u16, bool) {
        let depth = depth.max(2);
        let lease = self.lease.clamp(1, depth - 1);
        if deep {
            let run = if small { self.deep_lease_small } else { self.deep_lease };
            (depth - lease, run.clamp(1, depth - lease), false)
        } else if primary {
            (depth - lease, lease, true)
        } else {
            let c = self.secondary.clamp(1, depth);
            (c, c, false)
        }
    }

    /// Depth-mode hysteresis: the queue's threads hold `held` requests in
    /// all; `deep` is the mode now.
    pub fn next_deep(&self, deep: bool, held: u32) -> bool {
        let lease = self.lease.max(1) as u32;
        if deep { held > lease / 2 } else { held > lease }
    }

    /// Depth mode: the most requests one of `threads` threads may hold when
    /// the queue's threads hold `total` (see `deep_share_pct`): the share,
    /// rounded up to whole runs, at least one run, at most `cap`.
    pub fn deep_cap(&self, total: u32, threads: u16, cap: u16, run: u16) -> u16 {
        if self.deep_share_pct == 0 {
            return cap;
        }
        let run = run.max(1) as u32;
        let share = (total as u64 * self.deep_share_pct as u64).div_ceil(100 * threads.max(1) as u64) as u32;
        (share.div_ceil(run) * run).clamp(run, cap.max(1) as u32) as u16
    }

    /// Size-class hysteresis: the queue's moving average request size is
    /// `avg` bytes; `small` is the class now.
    pub fn next_small(&self, small: bool, avg: u32) -> bool {
        if small { avg <= self.small_bytes.saturating_mul(2) } else { avg <= self.small_bytes }
    }

    /// Whether a request's byte weight counts against this thread's cap:
    /// only the shallow primary's (a large request makes it spill sooner, so
    /// big transfers spread). A secondary's cap is a count of requests (a
    /// 1 MiB request weighing 17 credits parked a 4-credit secondary until it
    /// drained), and in depth mode the rotation spreads the bytes.
    pub fn weighs(&self, primary: bool, deep: bool) -> bool {
        primary && !deep
    }

    /// Whether this thread polls its ring without sleeping this turn.
    /// Shallow primary: while hot (`primary_hot`). Shallow secondary: while
    /// holding requests and within `spin_us` of its last event. Depth mode:
    /// nobody, or with deep_spin every thread by the secondary rule.
    pub fn spins(&self, primary: bool, deep: bool, hot: bool, holding: bool, recent_event: bool) -> bool {
        if deep {
            return self.deep_spin && holding && recent_event;
        }
        if primary { hot && self.primary_spin } else { holding && recent_event }
    }
}

/// The shallow primary is hot (spins) while it has commands on the wire and
/// saw an event within `spin_cap` (so a command that takes milliseconds, a
/// flush say, does not keep a core spinning for all of it), and for the warm
/// window after the wire went empty.
pub fn primary_hot(on_wire: bool, since_event: Duration, since_busy: Duration, rtt: Option<Duration>) -> bool {
    if on_wire { since_event < spin_cap(rtt) } else { since_busy < warm_window(rtt) }
}

/// The hot-lane primary's warm-window clock. A turn that finds the wire
/// empty right after a turn that had commands on it is still a busy moment:
/// the last completion has just come in. Counting busy time only from turns
/// that saw commands on the wire lost the warm window after any command
/// longer than the spin cap (a flush: 1-3 ms), because the thread slept
/// through it and its last busy turn was from before the sleep; the next
/// request (fio's write after its fsync) then found the primary asleep.
pub struct WarmClock {
    last_busy: Instant,
    was_on_wire: bool,
}

impl WarmClock {
    pub fn new(now: Instant) -> Self {
        WarmClock { last_busy: now, was_on_wire: false }
    }

    /// This turn: whether the primary is hot (see `primary_hot`).
    pub fn turn(&mut self, on_wire: bool, now: Instant, since_event: Duration, rtt: Option<Duration>) -> bool {
        if on_wire || self.was_on_wire {
            self.last_busy = now;
        }
        self.was_on_wire = on_wire;
        primary_hot(on_wire, since_event, now.saturating_duration_since(self.last_busy), rtt)
    }
}

/// How long a primary with commands on the wire keeps spinning without an
/// event: 4 x the warm window, at most 1 ms.
pub fn spin_cap(rtt: Option<Duration>) -> Duration {
    (warm_window(rtt) * 4).min(Duration::from_millis(1))
}

/// How long the primary keeps polling after its last command on the wire:
/// 1.5 x the wire round trip, within [100 µs, 1 ms]; 100 µs before any
/// sample.
pub fn warm_window(rtt: Option<Duration>) -> Duration {
    rtt.map_or(Duration::from_micros(100), |r| (r * 3 / 2).clamp(Duration::from_micros(100), Duration::from_millis(1)))
}

/// Monotonic nanoseconds since the first call (heartbeats).
fn mono_ns() -> u64 {
    static BASE: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
    BASE.elapsed().as_nanos() as u64
}

/// One queue thread's heartbeat: when it last turned its loop, and whether
/// it is sleeping in its ring on purpose (then it is not wedged: a request
/// for it wakes it).
pub(crate) struct Beat {
    at_ns: AtomicU64,
    waiting: AtomicBool,
}

impl Beat {
    pub(crate) fn new() -> Self { Self { at_ns: AtomicU64::new(mono_ns()), waiting: AtomicBool::new(false) } }
    pub(crate) fn stamp(&self, waiting: bool) {
        self.at_ns.store(mono_ns(), Ordering::Relaxed);
        self.waiting.store(waiting, Ordering::Relaxed);
    }
}

/// Whether thread `me` should take the primary role from `primary`, given
/// the primary's heartbeat.
fn should_take_over(me: u16, primary: u16, beat_ns: u64, waiting: bool, now_ns: u64, wedge: Duration) -> bool {
    me != primary && !waiting && now_ns.saturating_sub(beat_ns) > wedge.as_nanos() as u64
}

/// Fixed counts remain available as an A/B control. Keep the allocated pool
/// for failover; vary admission, never migrate a request's registered pages.
pub fn adaptive() -> bool {
    static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| env_u64("NVMEUBLK_ADAPTIVE", 0) != 0);
    *ENABLED
}

// A run of writes is required to consolidate; a read immediately restores
// parallelism. Packed class/streak makes concurrent observations atomic.
fn observed_queue_depth(published_total: u32, published_here: usize, owned_here: usize) -> usize {
    (published_total as usize).saturating_sub(published_here).saturating_add(owned_here)
}

fn next_mix(old: u32, write: bool, bytes: u64, light_read: bool, shallow_queue: bool) -> u32 {
    let class = if !write { if bytes == 0 { 0 } else if light_read { 3 } else if bytes >= 512 * 1024 { if shallow_queue { 10 } else { 9 } } else if shallow_queue { 7 } else { 4 } } else if bytes >= 65536 { if light_read { 6 } else { 2 } } else if light_read { 5 } else { 1 };
    let count = if old >> 16 == class { (old & 0xffff).saturating_add(1).min(64) } else { 1 };
    (class << 16) | count
}

/// Expand at the first concurrent read. Consolidate only after 64 matching
/// observations; gaps between concurrent read bursts must not collapse the
/// volume back onto one warm lane on every burst.
fn next_mode(current: u32, mix: u32) -> u32 {
    let class = mix >> 16;
    if class == 4 { return 4; }
    if class == 10 { return if matches!(current, 4 | 7) && mix & 0xffff < 64 { current } else { 10 }; }
    if class == 7 { return if current == 4 && mix & 0xffff < 64 { 4 } else { 7 }; }
    if mix & 0xffff >= 64 { return class; }
    if class == 0 || (matches!(current, 2 | 6) && !matches!(class, 2 | 6)) || (matches!(current, 1 | 5) && class == 3) { 0 } else { current }
}

fn active_limit(mode: u32) -> usize {
    match mode { 2 | 6 => 2, 1 | 5 | 9 | 10 => 4, _ => usize::MAX }
}

fn weight_reads(mode: u32) -> bool {
    active_limit(mode) == usize::MAX
}

/// Keep queue participation stable across completion batches. Instantaneous
/// zero/nonzero counts otherwise change the credit shares on every batch.
struct Activity { last: Instant, counted: bool }
impl Activity {
    fn new(now: Instant) -> Self { Self { last: now, counted: false } }
    fn update(&mut self, busy: bool, now: Instant) -> Option<bool> {
        if busy {
            self.last = now;
            if !self.counted { self.counted = true; return Some(true); }
        } else if self.counted && now.saturating_duration_since(self.last) >= Duration::from_millis(50) {
            self.counted = false;
            return Some(false);
        }
        None
    }
}

/// A device-wide mix keeps idle queues' primaries aligned with its active
/// queues. Otherwise an idle read primary on reactor 1 could make a global
/// primary fault stop both members of a consolidated write pair.
#[repr(align(64))]
struct ReactorLoad(AtomicU32);

struct Admission {
    mix: AtomicU32,
    mode: AtomicU32,
    loads: [ReactorLoad; 64],
    wakers: Mutex<Vec<std::task::Waker>>,
}

impl Default for Admission {
    fn default() -> Self {
        Self { mix: AtomicU32::new(0), mode: AtomicU32::new(0), loads: std::array::from_fn(|_| ReactorLoad(AtomicU32::new(0))), wakers: Mutex::new(Vec::new()) }
    }
}

/// What the threads of one queue share: whether thread 0 has prepared the
/// queue's tags (the others may post their fetches only then), the queue's
/// tag buffers in the copying mode, and (hot lane) which thread is the
/// primary and every thread's heartbeat.
pub struct QueueShared {
    st: Mutex<Prep>,
    cv: Condvar,
    primary: Arc<AtomicU16>,
    beats: Box<[Beat]>,
    reactor_beats: Box<[std::sync::OnceLock<Arc<Beat>>]>,
    /// Hot lane: requests the queue's threads hold (fetched, not committed),
    /// and whether the queue is in depth mode.
    held: AtomicU32,
    deep: AtomicBool,
    /// Moving average (1/8) of the size of the requests the queue's threads
    /// fetch, in bytes, and the size class it gives (HotLane::next_small).
    avg_bytes: AtomicU32,
    small: AtomicBool,
    admission: Arc<Admission>,
    seen_mode: AtomicU32,
    stopping: AtomicBool,
    placement: std::sync::OnceLock<Vec<usize>>,
    can_consolidate: AtomicBool,
    read_home: AtomicU16,
}

impl Default for QueueShared {
    fn default() -> Self {
        QueueShared::new(8)
    }
}

impl QueueShared {
    /// For a queue served by `threads` io threads.
    pub fn new(threads: u16) -> Self {
        Self::with_admission(threads, Arc::new(Admission::default()))
    }

    pub fn for_volume(queues: u16, threads: u16) -> Vec<Arc<Self>> {
        let admission = Arc::new(Admission::default());
        (0..queues).map(|_| Arc::new(Self::with_admission(threads, admission.clone()))).collect()
    }

    fn with_admission(threads: u16, admission: Arc<Admission>) -> Self {
        let now = mono_ns();
        let beats = (0..threads.max(1)).map(|_| Beat { at_ns: AtomicU64::new(now), waiting: AtomicBool::new(false) }).collect();
        let reactor_beats = (0..threads.max(1)).map(|_| std::sync::OnceLock::new()).collect();
        QueueShared { reactor_beats, st: Mutex::new(Prep::Waiting), cv: Condvar::new(), primary: Arc::new(AtomicU16::new(0)), beats, held: AtomicU32::new(0), deep: AtomicBool::new(false), avg_bytes: AtomicU32::new(0), small: AtomicBool::new(true), admission, seen_mode: AtomicU32::new(0), stopping: AtomicBool::new(false), placement: std::sync::OnceLock::new(), can_consolidate: AtomicBool::new(true), read_home: AtomicU16::new(0) }
    }

    fn beat(&self, thread: u16, waiting: bool) {
        if let Some(b) = self.beats.get(thread as usize) {
            b.at_ns.store(mono_ns(), Ordering::Relaxed);
            b.waiting.store(waiting, Ordering::Relaxed);
        }
    }

    fn heartbeat(&self, thread: u16) -> Option<&Beat> {
        self.reactor_beats.get(thread as usize)?.get().map(|b| &**b)
            .or_else(|| self.beats.get(thread as usize))
    }

    pub fn primary(&self) -> u16 {
        self.primary.load(Ordering::Acquire)
    }

    /// Threads serving the queue.
    pub fn threads(&self) -> u16 {
        self.beats.len() as u16
    }

    /// A thread's held count moved from `was` to `now`: update the queue's
    /// total. Returns the new total.
    fn publish_held(&self, was: usize, now: usize) -> u32 {
        let (was, now) = (was as u32, now as u32);
        if now >= was {
            self.held.fetch_add(now - was, Ordering::AcqRel) + (now - was)
        } else {
            self.held.fetch_sub(was - now, Ordering::AcqRel) - (was - now)
        }
    }

    /// Fold the sizes of requests just fetched into the queue's average and
    /// return its size class. Lossy under races between threads, which only
    /// delays the average a little.
    fn note_sizes(&self, hot: &HotLane, bytes: impl Iterator<Item = u64>) -> bool {
        let mut avg = self.avg_bytes.load(Ordering::Relaxed) as u64;
        let mut any = false;
        for b in bytes {
            avg = avg - avg / 8 + b.min(u32::MAX as u64) / 8;
            any = true;
        }
        let small = self.small.load(Ordering::Relaxed);
        if !any {
            return small;
        }
        self.avg_bytes.store(avg.min(u32::MAX as u64) as u32, Ordering::Relaxed);
        let next = hot.next_small(small, avg as u32);
        if next != small {
            self.small.store(next, Ordering::Relaxed);
        }
        next
    }

    /// The queue's mode after its threads' held total became `held`.
    fn update_deep(&self, hot: &HotLane, held: u32) -> bool {
        let deep = self.deep.load(Ordering::Relaxed);
        let next = hot.next_deep(deep, held);
        if next != deep {
            self.deep.store(next, Ordering::Relaxed);
        }
        next
    }

    pub(crate) fn bind_placement(&self, reactors: &[usize]) {
        assert_eq!(reactors.len(), self.threads() as usize);
        assert!(self.placement.set(reactors.to_vec()).is_ok());
    }

    pub(crate) fn bind_read_home(&self, reactor: usize) {
        let thread = (0..self.threads()).find(|&t| self.reactor(t) == reactor).unwrap_or(0);
        self.read_home.store(thread, Ordering::Release);
    }

    pub(crate) fn fixed_admission(&self) {
        self.can_consolidate.store(false, Ordering::Release);
    }

    fn reactor(&self, thread: u16) -> usize {
        self.placement.get().map_or(thread as usize, |rs| rs[thread as usize])
    }

    fn note_mix(&self, write: bool, bytes: u64, light_read: bool, shallow_queue: bool) {
        let mix = &self.admission.mix;
        let old = mix.load(Ordering::Relaxed);
        let next = if next_mix(old, write, bytes, light_read, shallow_queue) == old { old } else {
            let old = mix.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| Some(next_mix(old, write, bytes, light_read, shallow_queue))).unwrap();
            next_mix(old, write, bytes, light_read, shallow_queue)
        };
        let current = self.admission.mode.load(Ordering::Acquire);
        let desired = next_mode(current, next);
        if desired != current && self.admission.mode.compare_exchange(current, desired, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            for w in self.admission.wakers.lock().unwrap().iter() { w.wake_by_ref(); }
        }
    }

    fn follow_admission(&self, wedge: Duration) {
        if !self.can_consolidate.load(Ordering::Acquire) { return; }
        let mode = self.admission.mode.load(Ordering::Acquire);
        let old = self.seen_mode.load(Ordering::Relaxed);
        if old == mode || self.seen_mode.compare_exchange(old, mode, Ordering::AcqRel, Ordering::Acquire).is_err() { return; }
        // Sustained writes and light reads share a warm primary on reactor
        // 0. Depth reads return to the queue's original distributed lane.
        // Both write classes retain active peers outside their primary set.
        // Only future fetch credits change; no owned request moves rings.
        let desired = if matches!(mode, 7 | 10) { self.read_home.load(Ordering::Acquire) } else if mode != 0 {
            (0..self.threads()).find(|&t| self.reactor(t) == 0).unwrap_or(self.primary())
        } else { 0 };
        let current = self.primary();
        let live = self.heartbeat(desired).is_some_and(|b| b.waiting.load(Ordering::Acquire)
            || mono_ns().saturating_sub(b.at_ns.load(Ordering::Acquire)) <= wedge.as_nanos() as u64);
        if desired != current && live {
            let _ = self.primary.compare_exchange(current, desired, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    fn admits(&self, thread: u16, mix: u32, stopping: bool) -> bool {
        stopping || self.stopping.load(Ordering::Acquire) || !self.can_consolidate.load(Ordering::Acquire) || thread == self.primary() || self.reactor(thread) < active_limit(mix)
    }

    fn note_load(&self, thread: u16, on: bool) {
        if !self.can_consolidate.load(Ordering::Relaxed) { return; }
        let load = &self.admission.loads[self.reactor(thread)].0;
        if on { load.fetch_add(1, Ordering::Relaxed); } else { load.fetch_sub(1, Ordering::Relaxed); }
    }

    /// A shared reactor must divide its capacity among its busy queues.
    /// Equal per-queue shares otherwise overload the common write pair
    /// during reads/mixed traffic while less-shared read peers sit idle.
    fn weighted_cap(&self, hot: &HotLane, total: u32, cap: u16, run: u16, thread: u16, mode: u32) -> u16 {
        if hot.deep_share_pct == 0 { return cap; }
        let weight = |t| (1024 / self.admission.loads[self.reactor(t)].0.load(Ordering::Relaxed).max(1)).max(1) as u64;
        let sum: u64 = (0..self.threads()).filter(|&t| self.admits(t, mode, false)).map(weight).sum();
        let share = (total as u64 * hot.deep_share_pct as u64 * weight(thread)).div_ceil(100 * sum.max(1)) as u32;
        let run = run.max(1) as u32;
        (share.div_ceil(run) * run).clamp(run, cap.max(1) as u32) as u16
    }

    /// Watchdog, run by any thread of the queue: take the primary role over
    /// if the primary has stopped turning. Some(old primary) if it did.
    fn check_primary(&self, me: u16, wedge: Duration) -> Option<u16> {
        let p = self.primary();
        let b = self.heartbeat(p)?;
        if !should_take_over(me, p, b.at_ns.load(Ordering::Relaxed), b.waiting.load(Ordering::Relaxed), mono_ns(), wedge) {
            return None;
        }
        // Stamp our own heartbeat first: the CAS publishes it, so a thread
        // that sees us as the primary also sees us turning (otherwise a
        // third thread could take the role straight back off us).
        self.beat(me, false);
        self.primary.compare_exchange(p, me, Ordering::AcqRel, Ordering::Acquire).ok()
    }
}

impl QueueShared {
    fn publish(&self, p: Prep) {
        *self.st.lock().unwrap_or_else(PoisonError::into_inner) = p;
        self.cv.notify_all();
    }

    /// Whether thread 0 has prepared the queue (Some(true)), failed
    /// (Some(false)), or not yet (None) by `deadline`, waiting until one of
    /// them or until the bring-up is abandoned.
    pub fn prepared_by(&self, abandoned: &AtomicBool, deadline: Instant) -> Option<bool> {
        let mut st = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match &*st {
                Prep::Ready(_) => return Some(true),
                Prep::Failed => return Some(false),
                Prep::Waiting if abandoned.load(Ordering::Acquire) => return Some(false),
                Prep::Waiting => {}
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            st = self.cv.wait_timeout(st, left.min(Duration::from_millis(100))).unwrap_or_else(PoisonError::into_inner).0;
        }
    }

    /// Wait until thread 0 has prepared the queue: Some(its buffers), or
    /// None if it failed or the bring-up was abandoned meanwhile.
    fn wait(&self, abandoned: &AtomicBool) -> Option<Option<SharedBufs>> {
        let mut st = self.st.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match &*st {
                Prep::Ready(b) => return Some(b.clone()),
                Prep::Failed => return None,
                Prep::Waiting if abandoned.load(Ordering::Acquire) => return None,
                Prep::Waiting => {}
            }
            st = self.cv.wait_timeout(st, Duration::from_millis(100)).unwrap_or_else(PoisonError::into_inner).0;
        }
    }
}

/// Thread 0 of a queue reports how its preparation went, also when it
/// returns or unwinds early, so the queue's other threads never wait for it
/// forever.
struct PrepReport<'a> {
    shared: Option<&'a QueueShared>,
}

impl PrepReport<'_> {
    fn ready(mut self, bufs: Option<SharedBufs>) {
        if let Some(s) = self.shared.take() {
            s.publish(Prep::Ready(bufs));
        }
    }
}

impl Drop for PrepReport<'_> {
    fn drop(&mut self) {
        if let Some(s) = self.shared.take() {
            s.publish(Prep::Failed);
        }
    }
}

/// The batch configuration of one io thread: thread 0 prepares the tags;
/// `spill` is clamped to 1..=depth.
pub fn batch_config(leader: bool, spill: u16, depth: u16) -> UblkBatchConfig {
    UblkBatchConfig::new().with_prepare_tags(leader).with_spill_tags(spill.clamp(1, depth.max(1))).with_max_inflight_commits(4)
}

/// A consolidated reactor hosts many idle queues. One preposted probe per
/// shallow tenancy bounds the requests those idle queues can hand to a
/// stalled ring. QD1 still refills immediately; depth retains its batch lease.
fn lane_credits(hot: &HotLane, primary: bool, deep: bool, small: bool, depth: u16, adaptive: bool) -> (u16, u16, bool) {
    let mut credits = hot.credits_sized(primary, deep, small, depth);
    if adaptive && !deep {
        credits.1 = 1;
        if !primary { credits.0 = 1; }
    }
    credits
}

fn reactor_lease(lease: u16, reactor: usize, adaptive: bool) -> u16 {
    // A depth lease can outlive the traffic that created it. Keep the
    // common primary's preposted budget small even when it goes idle.
    if adaptive && reactor == 0 { 1 } else { lease }
}

fn bounded_primary_cap(hot: &HotLane, total: u32, active: u16, cap: u16, run: u16) -> u16 {
    if hot.deep_share_pct == 0 { return cap; }
    let mut bounded = *hot;
    bounded.deep_share_pct = bounded.deep_share_pct.min(100);
    cap.min(bounded.deep_cap(total, active, cap, run))
}

/// As `batch_config`, for a hot-lane thread in the given role.
pub fn hot_batch_config(leader: bool, primary: bool, hot: &HotLane, depth: u16) -> UblkBatchConfig {
    let (spill, lease, refill) = hot.credits(primary, false, depth);
    batch_config(leader, spill, depth).with_lease_tags(lease).with_refill(refill)
}

/// What one batch-queue tenancy serves and reports to: io thread `thread` of
/// ublk queue `qid` of a device (the device itself is passed separately).
pub struct TenancySpec {
    pub qid: u16,
    pub thread: u16,
    pub ctrls: Arc<ctrls::Ctrls>,
    pub stats: Arc<qengine::Stats>,
    pub stop: Arc<AtomicBool>,
    pub draining: Arc<AtomicBool>,
    pub abandoned: Arc<AtomicBool>,
    pub cfg: qengine::QConfig,
    pub shared: Arc<QueueShared>,
    pub spill: u16,
    pub hot: Option<HotLane>,
}

/// One io thread's worth of a batch queue: its multishot fetch on the
/// queue, its engine (connections), request completion destinations, and the hot-lane
/// state. A host loop drives it: a queue thread of its own (the per-volume
/// layout, `queue_fn`), or a reactor of the node-wide pool (reactor.rs),
/// which drives many tenancies of many devices on one ring. The host
/// polls the ring once for all of them: `before_wait` (commit, credit
/// policy, watchdog; whether it wants the host to keep polling), `on_cqe`
/// for each batch CQE of its queue, `after_wait` (hand fetched requests
/// to the engine (or copying-mode tag tasks), run the executors).
pub struct Tenancy {
    // Drop order (declaration order): the tag tasks and their executor,
    // then the engine (its Drop drives its tasks to their end on this
    // ring), then the batch transport, the queue, the device.
    ready_waker: Option<std::task::Waker>,
    direct: bool,
    shift: u32,
    cdev_fd: i32,
    arrive: Vec<smol::channel::Sender<()>>,
    tasks: Vec<smol::Task<()>>,
    /// The tag tasks' executor: this tenancy's own, or on a shared engine
    /// the one of every tenancy of the device on this reactor (EngineShare).
    exe: Rc<smol::LocalExecutor<'static>>,
    completions: Rc<RefCell<Vec<UblkBatchCompletion>>>,
    engine: Rc<qengine::QEngine>,
    /// Shared engine: this tenancy's hold on the reactor's entry (dropped
    /// with `engine`, so the last tenancy ends the engine where an own one
    /// ends).
    _share: Option<Rc<EngineShare<qengine::QEngine>>>,
    net_exe: Rc<smol::LocalExecutor<'static>>,
    batch: UblkBatchQueue<'static, 'static>,
    q: Rc<UblkQueue<'static>>,
    shared: Arc<QueueShared>,
    stats: Arc<qengine::Stats>,
    stop: Arc<AtomicBool>,
    abandoned: Arc<AtomicBool>,
    hot: Option<HotLane>,
    /// Hosted on a pool reactor (a ring shared with other tenancies): the
    /// fetch cannot be cancelled without the ring, so the tenancy serves
    /// until the device is stopped (see `before_wait`).
    pooled: bool,
    qid: u16,
    thread: u16,
    depth: u16,
    dev_id: i32,
    is_primary: bool,
    /// The engine is shared with the reactor's other tenancies of this
    /// device (its in-flight count and mode are not this tenancy's alone).
    shared_engine: bool,
    /// The mode this tenancy last reported to a shared engine.
    reported_deep: bool,
    warm: WarmClock,
    last_check: Instant,
    deep: bool,
    published_held: usize,
    activity: Activity,
    applied: Option<(u16, u16, bool)>,
    pending: Vec<UblkBatchCompletion>,
    arrived: Vec<u16>,
    seen_tags: u64,
    seen_spills: u64,
    seen_events: u64,
    last_event: Instant,
    spin: Duration,
    spin_idle: bool,
    weight_bytes: u64,
    /// Errors reported since the tenancy failed (pooled; rate-limited log).
    failed: Option<u64>,
    /// Keeps the device alive for a pooled tenancy (a queue thread's device
    /// is kept by libublk's thread). Last: dropped after the queue.
    _dev: Option<Arc<UblkDev>>,
}

/// NVMEUBLK_HOT_DEEP_SHARE by default (0 = off).
const DEEP_SHARE_DEFAULT: u16 = 125;

/// Whether pooled tenancies share one engine per device and reactor
/// (NVMEUBLK_SHARED_ENGINE): connections then follow the reactors a device
/// uses, not its queues x threads, so the queue count can follow the CPUs.
pub fn shared_engines() -> bool {
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| env_u64("NVMEUBLK_SHARED_ENGINE", SHARED_ENGINE_BY_DEFAULT as u64) != 0);
    *ON
}
const SHARED_ENGINE_BY_DEFAULT: bool = true;

/// Values shared by key on one thread, alive while someone holds them
/// (the pool's shared engines: key = the device's Ctrls address).
pub struct Shared<T> {
    v: Vec<(usize, std::rc::Weak<T>)>,
}

impl<T> Shared<T> {
    pub const fn new() -> Self {
        Shared { v: Vec::new() }
    }

    /// The live value under `key`, if any (dead entries are dropped).
    pub fn get(&mut self, key: usize) -> Option<Rc<T>> {
        self.v.retain(|(_, w)| w.strong_count() > 0);
        self.v.iter().find(|(k, _)| *k == key).and_then(|(_, w)| w.upgrade())
    }

    pub fn put(&mut self, key: usize, v: &Rc<T>) {
        self.v.retain(|(k, w)| w.strong_count() > 0 && *k != key);
        self.v.push((key, Rc::downgrade(v)));
    }
}

/// What a device's tenancies on one reactor share: the engine, its
/// network executor, and one executor for all their tag tasks. With a tag
/// executor per tenancy, a network completion that one tenancy's turn
/// drove woke a tag task of another tenancy whose turn had already run; the
/// request then waited for the reactor's next poll (up to the NAPI
/// spin-wait, or a sleep) before its tag task queued the result, and one
/// more turn for the commit. With one tag executor, whichever tenancy
/// drives the network also runs the woken tag tasks (QEngine::run_turn
/// runs both to quiescence), and every result is committed by its tenancy
/// before the next poll.
pub struct EngineShare<E> {
    pub engine: Rc<E>,
    pub net: Rc<smol::LocalExecutor<'static>>,
    pub tags: Rc<smol::LocalExecutor<'static>>,
}

/// The share under `key` in `reg`, or a new one (fresh executors, and the
/// engine `make` builds on the network executor), registered.
pub fn join_share<E>(reg: &mut Shared<EngineShare<E>>, key: usize, make: impl FnOnce(&Rc<smol::LocalExecutor<'static>>) -> Rc<E>) -> (Rc<EngineShare<E>>, bool) {
    if let Some(s) = reg.get(key) {
        return (s, false);
    }
    let net = Rc::new(smol::LocalExecutor::new());
    let engine = make(&net);
    let s = Rc::new(EngineShare { engine, net, tags: Rc::new(smol::LocalExecutor::new()) });
    reg.put(key, &s);
    (s, true)
}

thread_local! {
    /// Shared engines of this reactor thread, with their executors.
    static SHARED: RefCell<Shared<EngineShare<qengine::QEngine>>> = const { RefCell::new(Shared::new()) };
}

/// What the host does with a tenancy after `before_wait`.
pub enum Next {
    /// Keep serving; `spin`: poll the ring without sleeping this turn;
    /// `wedge`: fault injection asks the host to stop turning this long.
    Serve { spin: bool, wedge: Option<Duration> },
    /// Done serving (`clean`: its queue stopped and it handed everything
    /// back, or its bring-up was abandoned); `end` it and drop it.
    Leave { clean: bool },
}

impl Tenancy {
    /// Set the tenancy up on this thread's ring: the queue (on its own ring,
    /// or at `slot` of a ring shared with other tenancies), the batch
    /// transport with the hot-lane credits of its role, the engine, the tag
    /// tasks. Thread 0 of a queue prepares the queue's tags; the others
    /// wait for it first (their host must not be the one it runs on).
    ///
    /// # Safety
    /// `dev` must outlive the tenancy: a queue thread's device outlives the
    /// thread; a pooled tenancy passes the Arc it holds in `dev_arc`.
    pub unsafe fn new(spec: TenancySpec, dev: &'static UblkDev, dev_arc: Option<Arc<UblkDev>>, slot: Option<UblkSharedSlot>) -> Option<Tenancy> {
        let TenancySpec { qid, thread, ctrls, stats, stop, draining, abandoned, cfg, shared, spill, hot } = spec;
        let leader = thread == 0;
        shared.beat(thread, false);
        let report = PrepReport { shared: leader.then_some(&*shared) };
        let dev_id = dev.dev_info.dev_id as i32;
        let q = match slot {
            Some(s) => UblkQueue::new_shared(qid, dev, thread, s),
            None => UblkQueue::new_for_thread(qid, dev, thread),
        };
        let q_rc: Rc<UblkQueue<'static>> = match q {
            Ok(q) => Rc::new(q),
            Err(e) => {
                log::error!("ublk device {dev_id} queue {qid} thread {thread}: queue setup failed: {e}");
                return None;
            }
        };
        let depth = dev.dev_info.queue_depth;
        let copying = dev.dev_info.flags & NO_MAP_IO == 0;
        let bufs: Option<SharedBufs> = if leader {
            copying.then(|| Arc::new(dev.alloc_queue_io_bufs()))
        } else {
            match shared.wait(&abandoned) {
                Some(b) => b,
                None => {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: thread 0 did not prepare the queue; leaving");
                    return None;
                }
            }
        };
        if copying && bufs.is_none() {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: no tag buffers for the copying mode");
            return None;
        }
        let buffers = match &bufs {
            Some(b) => UblkBatchBuffers::Shared(b.clone()),
            None => UblkBatchBuffers::None,
        };
        let is_primary = hot.is_some() && shared.primary() == thread;
        let config = match &hot {
            Some(h) => {
                let adaptive = slot.is_some() && adaptive() && shared.can_consolidate.load(Ordering::Relaxed);
                let (cap, lease, refill) = lane_credits(h, is_primary, false, false, depth, adaptive);
                batch_config(leader, cap, depth).with_lease_tags(lease).with_refill(refill)
            },
            None => batch_config(leader, spill, depth),
        };
        // SAFETY: the queue lives in `q_rc`, which the tenancy keeps and
        // drops after the batch transport (field order).
        let qref: &'static UblkQueue<'static> = unsafe { &*Rc::as_ptr(&q_rc) };
        let batch = match UblkBatchQueue::new(qref, buffers, config) {
            Ok(b) => b,
            Err(e) => {
                log::error!("ublk device {dev_id} queue {qid} thread {thread}: batch setup failed: {e}");
                return None;
            }
        };
        report.ready(bufs.clone());

        let shift = ctrls.info.lba_shift;
        let net_exe: Rc<smol::LocalExecutor<'static>> = Rc::new(smol::LocalExecutor::new());
        let mut cfg = cfg;
        let user_copy = dev.dev_info.flags & libublk::sys::UBLK_F_USER_COPY as u64 != 0;
        cfg.cdev_fd = if user_copy { dev.tgt.fds[0] } else { -1 };
        cfg.path_offset = crate::env_u64("NVMEUBLK_PATH_OFFSET", dev.dev_info.dev_id as u64) as usize;
        let cdev_fd = cfg.cdev_fd;
        let zc = dev.dev_info.flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0;
        if zc {
            cfg.rx_offload = 0;
        }
        // Shared engine (pool): one engine per device and reactor, whatever
        // queues the reactor hosts tenancies of; else one per tenancy.
        let reactor = if slot.is_some() && shared_engines() { crate::reactor::this_reactor() } else { None };
        let key = Arc::as_ptr(&ctrls) as usize;
        let (engine, net_exe, exe, share) = match reactor {
            Some(r) => {
                let (p, _) = SHARED.with(|s| {
                    join_share(&mut s.borrow_mut(), key, |net| {
                        let engine = qengine::QEngine::new(r as u16, ctrls, cfg, net.clone(), stats.clone(), stop.clone(), draining);
                        engine.start();
                        engine
                    })
                });
                (p.engine.clone(), p.net.clone(), p.tags.clone(), Some(p))
            }
            None => {
                let eid = qid * dev.io_threads_per_queue() + thread;
                let engine = qengine::QEngine::new(eid, ctrls, cfg, net_exe.clone(), stats.clone(), stop.clone(), draining);
                engine.start();
                (engine, net_exe, Rc::new(smol::LocalExecutor::new()), None)
            }
        };
        let shared_engine = share.is_some();
        if hot.is_some() { engine.watch_primary(&shared.primary, thread); }

        // One task per tag: any tag may be fetched by this tenancy. A task
        // sleeps on its tag's channel until the tag is fetched here, serves
        // the request and queues its result for the next commit.
        let completions: Rc<RefCell<Vec<UblkBatchCompletion>>> = Rc::new(RefCell::new(Vec::with_capacity(depth as usize)));
        let mut arrive = Vec::with_capacity(depth as usize);
        let mut tasks = Vec::with_capacity(depth as usize);
        // AUTO_BUF_REG needs no per-tag userspace buffer or copy step.
        // Preserve the older task path as an A/B control and for copying mode.
        let direct = zc && env_u64("NVMEUBLK_DIRECT_BATCH", 1) != 0;
        for tag in 0..if direct { 0 } else { depth } {
            let (tx, rx) = smol::channel::bounded::<()>(1);
            arrive.push(tx);
            let (q, e, comp) = (q_rc.clone(), engine.clone(), completions.clone());
            let buf_ptr = bufs.as_ref().map_or(std::ptr::null_mut(), |b| b[tag as usize].as_mut_ptr());
            tasks.push(exe.spawn(async move {
                let (done_tx, done_rx) = smol::channel::bounded::<i32>(1);
                let ucopy = (cdev_fd >= 0).then(|| libublk::io::UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0));
                while rx.recv().await.is_ok() {
                    let res = serve_request(&q, tag, &e, shift, cdev_fd, zc, buf_ptr, ucopy, &done_tx, &done_rx).await;
                    comp.borrow_mut().push(UblkBatchCompletion::new(tag, res));
                }
            }));
        }
        let applied = hot.as_ref().map(|h| lane_credits(h, is_primary, false, false, depth, slot.is_some() && adaptive() && shared.can_consolidate.load(Ordering::Relaxed)));
        log::info!(
            "ublk device {dev_id} queue {qid} thread {thread}: batch I/O{}, {}{}",
            match slot {
                Some(s) => format!(" on a pool reactor (slot {}, buffers {}+{depth})", s.key, s.buf_base),
                None => String::new(),
            },
            match &hot {
                Some(_) => format!("hot lane {} (cap {}, lease {}, refill {})", if is_primary { "primary" } else { "secondary" }, batch.config().spill_tags(), batch.config().lease_tags(), batch.config().refill()),
                None => format!("spill at {} requests", batch.config().spill_tags()),
            },
            if leader { " (prepared the queue)" } else { "" }
        );
        let now = Instant::now();
        Some(Tenancy {
            ready_waker: None,
            direct,
            shift,
            cdev_fd,
            arrive,
            tasks,
            exe,
            completions,
            engine,
            _share: share,
            net_exe,
            batch,
            q: q_rc,
            shared,
            stats,
            stop,
            abandoned,
            hot,
            pooled: slot.is_some(),
            qid,
            thread,
            depth,
            dev_id,
            is_primary,
            shared_engine,
            reported_deep: false,
            warm: WarmClock::new(now),
            last_check: now,
            deep: false,
            published_held: 0,
            activity: Activity::new(now),
            applied,
            pending: Vec::with_capacity(depth as usize),
            arrived: Vec::with_capacity(depth as usize),
            seen_tags: 0,
            seen_spills: 0,
            seen_events: 0,
            last_event: now,
            spin: Duration::from_micros(env_u64("NVMEUBLK_SPIN_US", 100)),
            spin_idle: env_u64("NVMEUBLK_SPIN_IDLE", 1) != 0,
            weight_bytes: env_u64("NVMEUBLK_BATCH_WEIGHT_KB", 64) << 10,
            failed: None,
            _dev: dev_arc,
        })
    }

    /// Run the tag tasks and the engine's executors until nothing is
    /// runnable (QEngine::run_turn), accounted as one loop of the device.
    pub fn run_ops(&self) {
        let t0 = Instant::now();
        self.stats.loops.fetch_add(1, Ordering::Relaxed);
        let _turn = TurnTimer::new(&self.stats, t0);
        self.engine.run_turn(&self.exe, &self.net_exe);
        self.engine.arm_ready();
        self.stats.loop_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn run_ops_once(&self, turn: u64) {
        if self.engine.begin_turn(turn) { self.run_ops(); }
    }

    pub fn watch_ready(&mut self, waker: std::task::Waker) {
        self.shared.admission.wakers.lock().unwrap().push(waker.clone());
        self.engine.watch_ready(self.exe.clone(), waker.clone());
        self.ready_waker = Some(waker);
        self.engine.arm_ready();
    }

    /// Keep servicing commits and requests already owned by this tenancy.
    /// Idle fetches and timers wake us through the runnable set instead.
    pub fn needs_service(&self) -> bool {
        self.batch.owned_tag_count() > 0 || self.batch.inflight_commit_count() > 0
            || !self.pending.is_empty() || !self.completions.borrow().is_empty()
            || !self.arrived.is_empty() || self.failed.is_some()
    }

    /// Stamp this tenancy's heartbeat; `waiting`: its host is about to
    /// sleep in the ring on purpose (then it is not wedged).
    pub fn beat(&self, waiting: bool) {
        if !self.pooled { self.shared.beat(self.thread, waiting); }
    }

    pub(crate) fn bind_heartbeat(&self, beat: Arc<Beat>) {
        let _ = self.shared.reactor_beats[self.thread as usize].set(beat);
    }

    /// Whether this tenancy's queue is the primary's (hot lane).
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    /// Something happened for this tenancy just now (a batch CQE of its
    /// queue, or its engine received PDUs): the spin rules measure from it.
    fn note_events(&mut self) {
        let ev = self.engine.events();
        if ev != self.seen_events {
            self.seen_events = ev;
            self.last_event = Instant::now();
        }
    }

    /// A pooled tenancy that failed keeps serving (its fetch stays armed in
    /// the driver until the device stops) and has its device stopped;
    /// false for one on a queue thread of its own, which leaves.
    fn fail(&mut self, what: &str, e: &dyn std::fmt::Display) -> bool {
        let (dev_id, qid, thread) = (self.dev_id, self.qid, self.thread);
        if !self.pooled {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: {what}: {e}");
            return false;
        }
        let n = self.failed.get_or_insert(0);
        *n += 1;
        if *n == 1 {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: {what}: {e}; stopping the device");
            let spawned = crate::device::spawn_op_waiting(&crate::device::OP_LANES, move || match libublk::ctrl::UblkCtrl::new_simple(dev_id) {
                Ok(c) => {
                    if let Err(e) = c.kill_dev() {
                        log::error!("stop ublk device {dev_id} after a tenancy failed: {e}");
                    }
                }
                Err(e) => log::error!("open ublk device {dev_id} to stop it: {e}"),
            });
            if let Err(e) = spawned {
                log::error!("ublk device {dev_id}: no thread to stop it: {e}");
            }
        } else if n.is_power_of_two() {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: {what}: {e} ({n} errors)");
        }
        true
    }

    /// Before the host polls the ring: commit finished requests,
    /// follow the queue's mode, account, leave if done, and (hot lane) the
    /// watchdog and this tenancy's role; then whether it wants the host to
    /// keep polling without sleeping.
    pub fn before_wait(&mut self) -> Next {
        let (dev_id, qid, thread) = (self.dev_id, self.qid, self.thread);
        self.pending.append(&mut self.completions.borrow_mut());
        if !self.pending.is_empty() {
            match self.batch.try_submit_completions(&self.pending) {
                Ok(true) => self.pending.clear(),
                // Every commit slot is in flight: retried after its CQE.
                Ok(false) => {}
                Err(e) => {
                    if !self.fail("commit failed", &e) {
                        return Next::Leave { clean: false };
                    }
                }
            }
        }
        if let Some(h) = self.hot {
            if let Err(e) = follow_depth(&mut self.batch, &mut self.activity, self.pooled && adaptive(), self.thread, (self.stop.load(Ordering::Acquire) || self.engine.draining()), &self.shared, &h, self.is_primary, &mut self.deep, &mut self.published_held, &mut self.applied, self.depth) {
                if !self.fail("credit policy change failed", &e) {
                    return Next::Leave { clean: false };
                }
            }
        }
        let (tags, spills) = (self.batch.fetched_tag_count(), self.batch.spill_count());
        if tags != self.seen_tags {
            self.stats.batch_tags.fetch_add(tags - self.seen_tags, Ordering::Relaxed);
            self.seen_tags = tags;
        }
        if spills != self.seen_spills {
            self.stats.batch_spills.fetch_add(spills - self.seen_spills, Ordering::Relaxed);
            self.seen_spills = spills;
        }
        // A detach, or a bring-up that gave up: a queue thread leaves (its
        // ring's teardown cancels the fetch, and closing the char device
        // takes back the requests still held). A pooled tenancy cannot
        // cancel its fetch without the ring, which it shares: it serves on
        // until its device is stopped (detach stops it; a bring-up that
        // gave up deletes it), which ends the fetch.
        if !self.pooled && self.abandoned.load(Ordering::Acquire) {
            return Next::Leave { clean: true };
        }
        // Stopped: the driver aborted this fetch and every request taken is
        // committed. Hand the fetch buffers back, then leave.
        if self.batch.all_fetches_stopped() && self.batch.owned_tag_count() == 0 && self.pending.is_empty() && self.batch.inflight_commit_count() == 0 {
            match self.batch.try_begin_shutdown() {
                Ok(_) if self.batch.is_shutdown_complete() => return Next::Leave { clean: true },
                Ok(_) => {}
                Err(e) => {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: batch shutdown failed: {e}");
                    return Next::Leave { clean: false };
                }
            }
        }
        let mut wedge = None;
        // Hot lane: the watchdog (a few times a second, on any tenancy
        // whose host is awake anyway), and the role this tenancy has now.
        if let Some(h) = self.hot {
            if self.last_check.elapsed() >= Duration::from_millis(250) {
                self.last_check = Instant::now();
                if !self.stop.load(Ordering::Acquire) {
                    if let Some(old) = self.shared.check_primary(thread, h.wedge) {
                        self.stats.batch_takeovers.fetch_add(1, Ordering::Relaxed);
                        log::warn!("ublk device {dev_id} queue {qid}: primary thread {old} has not turned its loop for {:?}; thread {thread} takes over", h.wedge);
                    }
                }
            }
            let now_primary = self.shared.primary() == thread;
            if now_primary != self.is_primary {
                self.is_primary = now_primary;
                if now_primary { let _ = self.batch.set_fetch_enabled(true); }
                self.applied = None;
                if let Err(e) = follow_depth(&mut self.batch, &mut self.activity, self.pooled && adaptive(), self.thread,
                    (self.stop.load(Ordering::Acquire) || self.engine.draining()), &self.shared, &h, self.is_primary, &mut self.deep,
                    &mut self.published_held, &mut self.applied, self.depth) {
                    if !self.fail("role change failed", &e) { return Next::Leave { clean: false }; }
                }
                log::info!("ublk device {dev_id} queue {qid} thread {thread}: now the {}", if self.is_primary { "primary" } else { "secondary" });
            }
            // Fault injection "wedge <ms>": the primary stops turning (a
            // pool reactor then stops turning for all its tenancies). Only
            // a primary takes it: on a shared engine a secondary of another
            // queue must not consume it.
            if self.is_primary {
                if let Some(d) = self.engine.take_wedge() {
                    log::warn!("ublk device {dev_id} queue {qid} thread {thread}: fault injection: primary wedged for {d:?}");
                    wedge = Some(d);
                }
            }
        }
        if self.shared_engine {
            let deep = self.hot.is_none() || self.deep;
            self.engine.note_lane_deep(self.reported_deep, deep);
            self.reported_deep = deep;
        } else {
            self.engine.set_shallow(self.hot.is_some() && !self.deep);
        }
        // Wait for events. Hot-lane primary: the warm window; otherwise
        // adaptive polling as in the per-tag loop.
        self.note_events();
        let since_event = self.last_event.elapsed();
        let recent = !self.spin.is_zero() && since_event < self.spin;
        let spin = match self.hot {
            Some(h) => {
                // A shared engine's commands are not all this tenancy's: its
                // own are the requests it holds.
                let on_wire = if self.shared_engine { self.batch.owned_tag_count() > 0 } else { self.engine.inflight_here() > 0 };
                let hot = self.warm.turn(on_wire, Instant::now(), since_event, self.engine.wire_rtt());
                let holding = on_wire || self.batch.owned_tag_count() > 0;
                h.spins(self.is_primary, self.deep, hot, holding, recent)
            }
            None => recent && (self.spin_idle || self.engine.inflight_here() > 0 || self.batch.owned_tag_count() > 0),
        };
        Next::Serve { spin, wedge }
    }

    /// A batch CQE of this tenancy's queue (routed by its ring key).
    pub fn on_cqe(&mut self, cqe: &io_uring::cqueue::Entry) -> Result<(), ()> {
        self.last_event = Instant::now();
        let arrived = &mut self.arrived;
        match self.batch.handle_cqe(cqe, |_, tags| {
            arrived.extend_from_slice(tags);
            Ok(())
        }) {
            Ok(true) => Ok(()),
            Ok(false) => {
                log::warn!("ublk device {} queue {} thread {}: CQE {:#x} routed here is not this queue's", self.dev_id, self.qid, self.thread, cqe.user_data());
                Ok(())
            }
            Err(e) => {
                if self.fail("batch transport failed", &e) {
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    }

    /// After the host polled the ring and routed its CQEs: follow the
    /// queue's mode with what the tenancy now holds, weigh and settle
    /// credits, hand fetched requests to their tag tasks, run the
    /// executors. Err: leave (not clean).
    pub fn after_wait(&mut self) -> Result<(), ()> {
        self.dispatch_after_wait(true)
    }

    /// Shared reactors can dispatch every tenancy before driving each
    /// shared engine once, preserving send batching across ublk queues.
    pub fn dispatch_after_wait(&mut self, run: bool) -> Result<(), ()> {
        let (dev_id, qid, thread) = (self.dev_id, self.qid, self.thread);
        let mut failed: Option<libublk::UblkError> = None;
        // Weighted spill (NVMEUBLK_BATCH_WEIGHT_KB, default 64; 0 = off): a
        // request counts one extra credit per WEIGHT_KB of payload, so a
        // thread holding large requests spills sooner and big transfers
        // spread over the queue's threads, while small ones stay put. Hot
        // lane: the queue's mode moves with what its threads now hold
        // (before the credits are settled), and only the shallow primary
        // weighs requests (HotLane::weighs).
        if !self.arrived.is_empty() {
            if let Some(h) = self.hot {
                let q = &self.q;
                if self.pooled && adaptive() {
                    for &tag in &self.arrived {
                        let iod = q.get_iod(tag);
                        let write = iod.op_flags & 0xff == libublk::sys::UBLK_IO_OP_WRITE;
                        let bytes = (iod.nr_sectors as u64) << 9;
                        let queued = observed_queue_depth(self.shared.held.load(Ordering::Relaxed), self.published_held, self.batch.owned_tag_count());
                        let light_read = queued == 1 && self.stats.inflight.load(Ordering::Relaxed) <= 1;
                        self.shared.note_mix(write, bytes, light_read, queued <= h.lease.max(1) as usize);
                    }
                }
                self.shared.note_sizes(&h, self.arrived.iter().map(|&t| (q.get_iod(t).nr_sectors as u64) << 9));
            }
        }
        if let Some(h) = self.hot {
            if let Err(e) = follow_depth(&mut self.batch, &mut self.activity, self.pooled && adaptive(), self.thread, (self.stop.load(Ordering::Acquire) || self.engine.draining()), &self.shared, &h, self.is_primary, &mut self.deep, &mut self.published_held, &mut self.applied, self.depth) {
                failed.get_or_insert(e);
            }
        }
        let weigh = self.hot.as_ref().is_none_or(|h| h.weighs(self.is_primary, self.deep));
        if self.weight_bytes > 0 && weigh {
            for &tag in self.arrived.iter() {
                let bytes = (self.q.get_iod(tag).nr_sectors as u64) << 9;
                let extra = (bytes / self.weight_bytes).min(u16::MAX as u64) as u16;
                if extra > 0 {
                    self.batch.add_tag_weight(tag, extra);
                }
            }
        }
        if let Err(e) = self.batch.settle_credits() {
            failed.get_or_insert(e);
        }
        for tag in self.arrived.drain(..) {
            if self.direct {
                let pos = (self.cdev_fd >= 0).then(|| libublk::io::UblkIOCtx::ublk_user_copy_pos(self.qid, tag, 0));
                crate::submit_request(&self.q, tag, &self.engine, self.shift, self.cdev_fd, true,
                    std::ptr::null_mut(), pos,
                    qengine::Completion::Batch { tag, ready: self.completions.clone() });
            } else if self.arrive[tag as usize].try_send(()).is_err() {
                log::error!("ublk device {dev_id} queue {qid} thread {thread}: tag {tag} fetched while its task is busy or gone");
            }
        }
        if run { self.run_ops(); }
        if let Some(e) = failed {
            if !self.fail("batch transport failed", &e) {
                return Err(());
            }
        }
        Ok(())
    }

    /// The tenancy stops serving: report it (a clean leave is not a wedge,
    /// so no sibling takes over during teardown; an unclean one is, and a
    /// sibling takes its role) and give back what it held of the queue's
    /// total.
    pub fn end(&mut self, clean: bool) {
        if let Some(w) = self.ready_waker.take() {
            self.engine.unwatch_ready(&w);
            self.shared.admission.wakers.lock().unwrap().retain(|v| !v.will_wake(&w));
        }
        self.engine.unwatch_primary(&self.shared.primary, self.thread);
        self.is_primary = false;
        if self.shared_engine && self.reported_deep {
            self.engine.note_lane_deep(true, false);
            self.reported_deep = false;
        }
        self.shared.beat(self.thread, clean);
        if self.activity.counted {
            self.shared.note_load(self.thread, false);
            self.activity.counted = false;
        }
        self.shared.publish_held(self.published_held, 0);
        self.published_held = 0;
        log::info!("ublk device {} queue {} thread {}: batch loop ended ({} requests, {} spills)", self.dev_id, self.qid, self.thread, self.batch.fetched_tag_count(), self.batch.spill_count());
    }
}

/// Route the CQEs of one poll of this thread's ring: batch CQEs to the
/// tenancy whose ring key they carry (`deliver(key, cqe)`: None if there is
/// no such tenancy, else what its `on_cqe` said), SEND_ZC buffer-release
/// notices dropped, what `other` claims left to it, the rest (engine
/// futures) woken. Returns (batch CQEs no tenancy claimed, whether a
/// tenancy's `on_cqe` failed).
pub fn route_cqes(
    cqes: &[io_uring::cqueue::Entry],
    stats: Option<&qengine::Stats>,
    mut deliver: impl FnMut(u16, &io_uring::cqueue::Entry) -> Option<Result<(), ()>>,
    mut other: impl FnMut(&io_uring::cqueue::Entry) -> bool,
) -> (usize, bool) {
    let (mut unclaimed, mut failed) = (0, false);
    for cqe in cqes {
        if let Some(key) = libublk::io::batch_cqe_key(cqe.user_data()) {
            match deliver(key, cqe) {
                Some(Ok(())) => {}
                Some(Err(())) => failed = true,
                None => unclaimed += 1,
            }
            continue;
        }
        if other(cqe) {
            continue;
        }
        // A SEND_ZC buffer-release notification carries the send's
        // user_data, whose future already completed.
        if io_uring::cqueue::notif(cqe.flags()) {
            if let Some(s) = stats {
                s.zc_notif.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        libublk::uring_async::ublk_wake_task(cqe.user_data(), cqe);
    }
    (unclaimed, failed)
}

/// Take the CQEs of this thread's ring (those set aside by synchronous batch
/// setup or engine shutdown first) into `cqes`.
pub fn take_cqes(cqes: &mut Vec<io_uring::cqueue::Entry>) {
    cqes.clear();
    while let Some(c) = libublk::io::pop_deferred_queue_cqe() {
        cqes.push(c);
    }
    libublk::io::with_task_io_ring_mut(|r| cqes.extend(r.completion()));
}

/// The batch-mode queue thread (the per-volume layout): io thread
/// `libublk::io::io_thread_idx()` of queue `qid`, one tenancy on the
/// thread's own ring. Same engine, same request path (`serve_request`) as
/// the per-tag mode; the tags it serves are the ones its own fetch receives.
#[allow(clippy::too_many_arguments)]
pub fn queue_fn(
    qid: u16,
    dev: &UblkDev,
    ctrls: Arc<ctrls::Ctrls>,
    stats: Arc<qengine::Stats>,
    stop: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    abandoned: Arc<AtomicBool>,
    cfg: qengine::QConfig,
    shared: Arc<QueueShared>,
    spill: u16,
    hot: Option<HotLane>,
) {
    let thread = libublk::io::io_thread_idx();
    setup_queue_ring(qid, dev);
    let spec = TenancySpec { qid, thread, ctrls, stats: stats.clone(), stop, draining, abandoned, cfg, shared, spill, hot };
    // SAFETY: libublk's thread holds the device for as long as this
    // function runs, and the tenancy is dropped before it returns.
    let dev_static: &'static UblkDev = unsafe { &*(dev as *const UblkDev) };
    let Some(mut t) = (unsafe { Tenancy::new(spec, dev_static, None, None) }) else { return };
    let timeout = io_uring::types::Timespec::new().sec(20);
    let spin_wait = spin_wait_from_env();
    let mut cqes: Vec<io_uring::cqueue::Entry> = Vec::with_capacity(dev.tgt.cq_depth as usize);
    t.run_ops();
    let clean = loop {
        let spinning = match t.before_wait() {
            Next::Leave { clean } => break clean,
            Next::Serve { spin, wedge } => {
                if let Some(d) = wedge {
                    std::thread::sleep(d);
                }
                spin
            }
        };
        if !spinning {
            t.beat(true);
        }
        // A non-sleeping poll is all task work (inline receive copies,
        // commits): it counts as one turn.
        let polled = {
            let _turn = spinning.then(|| TurnTimer::new(&stats, Instant::now()));
            if spinning { spin_poll(&spin_wait, &timeout) } else { poll(1, &timeout) }
        };
        t.beat(false);
        if let Err(e) = polled {
            log::error!("ublk device {} queue {qid} thread {thread}: event loop failed: {e}", dev.dev_info.dev_id);
            break false;
        }
        take_cqes(&mut cqes);
        let key = t.q.ring_key();
        let (_, failed) = route_cqes(&cqes, Some(&stats), |k, c| (k == key).then(|| t.on_cqe(c)), |_| false);
        if failed || t.after_wait().is_err() {
            break false;
        }
    };
    t.end(clean);
    drop(t);
}

/// Hot lane: publish what this thread holds to its queue's total, follow the
/// queue's mode and size class, and switch this thread's credit policy when
/// they call for a different one than it last applied.
#[allow(clippy::too_many_arguments)]
fn follow_depth(batch: &mut UblkBatchQueue, activity: &mut Activity, adaptive: bool, thread: u16, stopping: bool, shared: &QueueShared, h: &HotLane, primary: bool, deep: &mut bool, published: &mut usize, applied: &mut Option<(u16, u16, bool)>, depth: u16) -> Result<(), libublk::UblkError> {
    if batch.all_fetches_stopped() { shared.stopping.store(true, Ordering::Release); }
    if adaptive { shared.follow_admission(h.wedge); }
    let primary = if adaptive { shared.primary() == thread } else { primary };
    let held = batch.owned_tag_count();
    if adaptive {
        if let Some(on) = activity.update(held > 0, Instant::now()) { shared.note_load(thread, on); }
    }
    let total = if held != *published {
        let t = shared.publish_held(*published, held);
        *published = held;
        t
    } else {
        shared.held.load(Ordering::Acquire)
    };
    *deep = shared.update_deep(h, total);
    let mut want = lane_credits(h, primary, *deep, shared.small.load(Ordering::Relaxed), depth, adaptive && shared.can_consolidate.load(Ordering::Relaxed));
    want.1 = reactor_lease(want.1, shared.reactor(thread), adaptive && shared.can_consolidate.load(Ordering::Relaxed));
    let mix = shared.admission.mode.load(Ordering::Relaxed);
    let active = if adaptive { (0..shared.threads()).filter(|&t| shared.admits(t, mix, stopping)).count() as u16 } else { shared.threads() };
    batch.set_fetch_enabled(!adaptive || shared.admits(thread, mix, stopping))?;
    if *deep {
        static WEIGHTED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| env_u64("NVMEUBLK_WEIGHTED_CREDITS", 1) != 0);
        want.0 = if adaptive && *WEIGHTED && weight_reads(mix) && shared.can_consolidate.load(Ordering::Relaxed) {
            shared.weighted_cap(h, total, want.0, want.1, thread, mix)
        } else { h.deep_cap(total, active, want.0, want.1) };
        if adaptive && shared.can_consolidate.load(Ordering::Relaxed) && shared.reactor(thread) == 0 {
            want.0 = bounded_primary_cap(h, total, active, want.0, want.1);
        }
    }
    if *applied != Some(want) {
        *applied = Some(want);
        batch.set_credit_policy(want.0, want.1, want.2)?;
    }
    if adaptive && total == 0 { batch.probe_parked_fetch()?; }
    Ok(())
}

/// Times one queue-thread turn into `Stats::long_turns` / `turn_max_ns`.
struct TurnTimer<'a> {
    stats: &'a qengine::Stats,
    t0: Instant,
}

impl<'a> TurnTimer<'a> {
    fn new(stats: &'a qengine::Stats, t0: Instant) -> Self {
        TurnTimer { stats, t0 }
    }
}

impl Drop for TurnTimer<'_> {
    fn drop(&mut self) {
        let ns = self.t0.elapsed().as_nanos() as u64;
        if ns > 50_000 {
            self.stats.long_turns.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.turn_max_ns.fetch_max(ns, Ordering::Relaxed);
    }
}

/// Submit what is queued and wait for `wait` completions at most `timeout`.
/// How a host polls its ring on a turn it does not sleep:
/// NVMEUBLK_SPIN_WAIT_US = 0: a non-blocking poll; N > 0 (default 10): while NAPI
/// busy poll is registered on the ring, a wait for one event of at most N µs,
/// so the kernel busy-polls the NICs (NAPI) in the wait instead of the thread
/// spinning in user space and the NIC's interrupt moderation deciding when a
/// payload's later segments are seen.
pub fn spin_poll(spin_wait: &Option<io_uring::types::Timespec>, idle_timeout: &io_uring::types::Timespec) -> std::io::Result<()> {
    match spin_wait {
        Some(ts) if crate::napi::state().0.is_some_and(|b| b > 0) => {
            let _ = idle_timeout;
            poll(1, ts)
        }
        _ => poll(0, idle_timeout),
    }
}

/// NVMEUBLK_SPIN_WAIT_US as a timespec (None when 0).
pub fn spin_wait_from_env() -> Option<io_uring::types::Timespec> {
    let us = env_u64("NVMEUBLK_SPIN_WAIT_US", 10);
    (us > 0).then(|| io_uring::types::Timespec::new().nsec((us.min(999_999) * 1000) as u32))
}

pub fn poll(wait: usize, timeout: &io_uring::types::Timespec) -> std::io::Result<()> {
    libublk::io::with_task_io_ring_mut(|r| {
        let args = io_uring::types::SubmitArgs::new().timespec(timeout);
        match r.submitter().submit_with_args(wait, &args) {
            Ok(_) => Ok(()),
            Err(e) if matches!(e.raw_os_error(), Some(libc::ETIME) | Some(libc::EINTR) | Some(libc::EBUSY) | Some(libc::EAGAIN)) => Ok(()),
            Err(e) => Err(e),
        }
    })
}

#[cfg(test)]
mod deep_share_tests {
    use super::HotLane;

    #[test]
    fn adaptive_configuration_probe() {
        let Ok(expected) = std::env::var("NVMEUBLK_TEST_ADAPTIVE_EXPECTED") else { return; };
        let expected = expected == "1";
        assert_eq!(super::adaptive(), expected);
        assert_eq!((crate::device::d_depth(), crate::device::d_threads()), if expected { (128,8) } else { (256,4) });
    }

    #[test]
    fn adaptive_pool_requires_explicit_opt_in() {
        for value in [None, Some("0"), Some("1")] {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child.args(["--exact", "batchq::deep_share_tests::adaptive_configuration_probe", "--nocapture"])
                .env_remove("NVMEUBLK_ADAPTIVE").env("NVMEUBLK_REACTORS", "8")
                .env("NVMEUBLK_SHARED_ENGINE", "1")
                .env("NVMEUBLK_TEST_ADAPTIVE_EXPECTED", if value == Some("1") { "1" } else { "0" });
            if let Some(value) = value { child.env("NVMEUBLK_ADAPTIVE", value); }
            let result = child.output().unwrap();
            assert!(result.status.success(), "setting {value:?}: {} {}", String::from_utf8_lossy(&result.stdout), String::from_utf8_lossy(&result.stderr));
        }
    }


    /// Depth mode caps what one thread holds at its share of the queue's
    /// total, so a slower thread stops taking requests and the others take
    /// them (join-the-shortest-queue by credits).
    #[test]
    fn large_reads_use_four_reactors_and_small_reads_restore_full_capacity() {
        use super::{QueueShared, active_limit};
        use std::sync::atomic::Ordering;
        let q = QueueShared::new(8);
        for _ in 0..64 { q.note_mix(false, 1 << 20, false, false); }
        assert_eq!(active_limit(q.admission.mode.load(Ordering::Relaxed)), 4);
        q.note_mix(false, 4096, false, false);
        assert_eq!(active_limit(q.admission.mode.load(Ordering::Relaxed)), usize::MAX);
    }

    #[test]
    fn shallow_probe_credits_bound_the_damage_of_a_shared_primary_wedge() {
        use super::{HotLane, lane_credits, bounded_primary_cap, reactor_lease};
        let h = HotLane::default();
        assert_eq!(lane_credits(&h, true, false, true, 256, true), (252, 1, true));
        assert_eq!(lane_credits(&h, false, false, true, 256, true), (1, 1, false));
        assert_eq!(lane_credits(&h, true, true, true, 256, true), h.credits_sized(true, true, true, 256));
        assert_eq!(lane_credits(&h, true, false, true, 256, false), h.credits(true, false, 256));
        assert_eq!(reactor_lease(4, 0, true), 1);
        assert_eq!(reactor_lease(4, 1, true), 4);
        assert_eq!(reactor_lease(4, 0, false), 4);
        assert_eq!(bounded_primary_cap(&h, 16, 4, 252, 4), 4);
        assert_eq!(bounded_primary_cap(&h, 16, 2, 252, 2), 8);
        // Seven idle probes plus the busy bulk queue cannot capture all 16
        // requests on the preferred primary before the watchdog can act.
        assert!(7 + bounded_primary_cap(&h, 16, 2, 252, 2) < 16);
    }

    #[test]
    fn read_weights_ignore_completion_gaps_and_writes_keep_batching() {
        use super::{Activity, weight_reads};
        use std::time::{Instant, Duration};
        let now = Instant::now();
        let mut a = Activity::new(now);
        assert_eq!(a.update(true, now), Some(true));
        assert_eq!(a.update(false, now + Duration::from_millis(5)), None);
        assert_eq!(a.update(true, now + Duration::from_millis(10)), None);
        assert_eq!(a.update(false, now + Duration::from_millis(59)), None);
        assert_eq!(a.update(false, now + Duration::from_millis(61)), Some(false));
        assert_eq!(a.update(false, now + Duration::from_millis(62)), None);
        for mode in [1, 2, 5, 6] { assert!(!weight_reads(mode)); }
        for mode in [0, 3, 4, 7] { assert!(weight_reads(mode)); }
    }

    #[test]
    fn classification_counts_the_whole_queue_not_just_one_fetching_tenancy() {
        use super::observed_queue_depth;
        assert_eq!(observed_queue_depth(8, 2, 1), 7);
        assert_eq!(observed_queue_depth(0, 0, 1), 1);
        assert_eq!(observed_queue_depth(4, 1, 1), 4);
    }

    #[test]
    fn credits_follow_busy_reactor_sharing_and_restore_when_queues_drain() {
        use super::{QueueShared, HotLane};
        let qs = QueueShared::for_volume(2, 4);
        qs[0].bind_placement(&[0, 4, 1, 2]); qs[1].bind_placement(&[0, 5, 1, 3]);
        let h = HotLane { deep_share_pct: 100, ..HotLane::default() };
        for q in &qs { for t in 0..4 { q.note_load(t, true); } }
        let busy = qs[0].weighted_cap(&h, 64, 252, 1, 0, 4);
        let peer = qs[0].weighted_cap(&h, 64, 252, 1, 1, 4);
        assert!(busy < peer, "the common reactor must not get the exclusive peer's share");
        for t in 0..4 { qs[1].note_load(t, false); }
        for t in 0..4 { assert_eq!(qs[0].weighted_cap(&h, 64, 252, 1, t, 4), 16); }
        for t in 0..4 { qs[0].note_load(t, false); }
        assert!(qs[0].admission.loads.iter().all(|l| l.0.load(std::sync::atomic::Ordering::Relaxed) == 0));
    }

    #[test]
    fn mix_changes_wake_parked_lanes_without_a_maintenance_delay() {
        use super::QueueShared;
        use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
        struct Count(AtomicUsize);
        impl std::task::Wake for Count {
            fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
        }
        let q = QueueShared::new(4);
        let count = Arc::new(Count(AtomicUsize::new(0)));
        for _ in 0..4 { q.admission.wakers.lock().unwrap().push(std::task::Waker::from(count.clone())); }
        for _ in 0..63 { q.note_mix(true, 65536, false, false); }
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
        q.note_mix(true, 65536, false, false);
        assert_eq!(count.0.load(Ordering::Relaxed), 4);
        q.note_mix(false, 4096, false, false);
        assert_eq!(count.0.load(Ordering::Relaxed), 8);
        q.note_mix(false, 4096, false, false);
        assert_eq!(count.0.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn idle_reads_keep_the_hot_lane_and_stopped_queues_wake_parked_fetches() {
        use super::{QueueShared, active_limit};
        use std::sync::atomic::Ordering;
        assert_eq!(active_limit(0), usize::MAX);
        assert_eq!(active_limit(64), usize::MAX);
        let q = QueueShared::new(4);
        let bulk = 2;
        assert!(q.admits(0, bulk, false));
        assert!(q.admits(1, bulk, false), "a bulk peer must keep taking work");
        assert!(!q.admits(3, bulk, false));
        assert!(q.admits(3, bulk, true));
        q.stopping.store(true, Ordering::Release);
        assert!(q.admits(3, bulk, false));
    }

    #[test]
    fn volume_depth_does_not_override_a_queues_qd1_warm_window() {
        use super::{QueueShared, HotLane};
        use std::sync::atomic::Ordering;
        let q = QueueShared::new(4);
        for mode in [1, 2, 4] {
            q.admission.mode.store(mode, Ordering::Relaxed);
            assert!(!q.update_deep(&HotLane::default(), 1));
            assert!(q.update_deep(&HotLane::default(), 8));
            assert!(!q.update_deep(&HotLane::default(), 1));
        }
    }

    #[test]
    fn concurrent_read_mode_retains_capacity_across_bursts_and_spreads_qd1_streams() {
        use super::QueueShared;
        use std::sync::atomic::Ordering;
        let q = QueueShared::new(4);
        q.bind_placement(&[0, 6, 1, 2]); q.bind_read_home(2);
        q.note_mix(false, 4096, false, false);
        assert_eq!(q.admission.mode.load(Ordering::Relaxed), 4);
        for _ in 0..63 { q.note_mix(false, 4096, true, true); }
        assert_eq!(q.admission.mode.load(Ordering::Relaxed), 4);
        q.note_mix(false, 4096, true, true);
        assert_eq!(q.admission.mode.load(Ordering::Relaxed), 3);
        q.note_mix(false, 4096, false, true);
        assert_eq!(q.admission.mode.load(Ordering::Relaxed), 7);
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.reactor(q.primary()), 2, "independent QD1 streams need distributed warm lanes");
        q.note_mix(false, 4096, false, false);
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.reactor(q.primary()), 0, "depth rotates over the available read peers");
    }

    #[test]
    fn dense_fallback_keeps_all_lanes_and_their_existing_primary() {
        use super::QueueShared;
        use std::sync::atomic::Ordering;
        let q = QueueShared::new(4);
        q.bind_placement(&[2, 4, 5, 6]);
        q.fixed_admission();
        for _ in 0..64 { q.note_mix(true, 65536, false, false); }
        q.follow_admission(std::time::Duration::from_secs(1));
        let mix = q.admission.mode.load(Ordering::Relaxed);
        assert!((0..4).all(|t| q.admits(t, mix, false)));
        assert_eq!(q.primary(), 0);
    }

    #[test]
    fn light_reads_move_the_warm_lane_without_suspending_read_fetches() {
        use super::QueueShared;
        use std::sync::atomic::Ordering;
        let q = QueueShared::new(4);
        q.bind_placement(&[2, 6, 0, 1]);
        for _ in 0..64 { q.note_mix(false, 4096, true, false); }
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.reactor(q.primary()), 0);
        let mix = q.admission.mode.load(Ordering::Relaxed);
        assert!((0..4).all(|t| q.admits(t, mix, false)), "light reads must not suspend fetches");
        q.note_mix(false, 4096, false, false);
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.reactor(q.primary()), 0, "concurrent reads retain a primary outside their peer set");
        for _ in 0..64 { q.note_mix(true, 4096, false, false); }
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.reactor(q.primary()), 0, "small writes must leave non-primary reactors active too");
    }

    #[test]
    fn idle_queues_share_the_active_volumes_admission_and_primary_set() {
        use super::QueueShared;
        let qs = QueueShared::for_volume(2, 4);
        qs[0].bind_placement(&[0, 1, 2, 4]);
        qs[1].bind_placement(&[2, 0, 1, 6]);
        for _ in 0..64 { qs[0].note_mix(true, 65536, false, false); }
        for q in &qs { q.follow_admission(std::time::Duration::from_secs(1)); }
        assert_eq!(qs[0].reactor(qs[0].primary()), 0);
        assert_eq!(qs[1].reactor(qs[1].primary()), 0, "idle read primary must not wedge the write peer");
        qs[0].note_mix(false, 4096, false, false);
        for q in &qs { q.follow_admission(std::time::Duration::from_secs(1)); }
        assert_eq!(qs[1].reactor(qs[1].primary()), 0);
    }

    #[test]
    fn adaptive_admission_follows_mix_and_always_keeps_the_primary() {
        use super::{next_mix, next_mode, active_limit, QueueShared};
        use std::sync::atomic::Ordering;
        let mut mix = 0;
        for _ in 0..63 { mix = next_mix(mix, true, 65536, false, false); }
        assert_eq!(active_limit(next_mode(0, mix)), usize::MAX);
        mix = next_mix(mix, true, 65536, false, false);
        assert_eq!(active_limit(next_mode(0, mix)), 2);
        mix = next_mix(mix, false, 4096, false, false);
        assert_eq!(active_limit(next_mode(0, mix)), usize::MAX);
        for _ in 0..64 { mix = next_mix(mix, true, 4096, false, false); }
        assert_eq!(active_limit(next_mode(0, mix)), 4);
        let q = QueueShared::new(4);
        q.bind_placement(&[2, 0, 1, 6]);
        for _ in 0..64 { q.note_mix(true, 65536, false, false); }
        q.follow_admission(std::time::Duration::from_secs(1));
        assert_eq!(q.primary(), 1, "bulk primary must be on reactor 0");
        let bulk = q.admission.mode.load(Ordering::Relaxed);
        assert_eq!((0..4).filter(|&t| q.admits(t, bulk, false)).collect::<Vec<_>>(), [1, 2]);
        q.primary.store(3, Ordering::Release); // watchdog promoted reactor 6
        assert!(q.admits(3, bulk, false), "watchdog overrides consolidation");
        q.beats[1].at_ns.store(0, Ordering::Release);
        q.seen_mode.store(0, Ordering::Relaxed);
        q.follow_admission(std::time::Duration::from_nanos(1));
        assert_eq!(q.primary(), 3, "never migrate back onto a wedged reactor");
        q.note_mix(false, 4096, false, false);
        q.follow_admission(std::time::Duration::from_nanos(1));
        assert_eq!(q.primary(), 3, "a read phase must not restore the wedged preferred primary");
    }

    #[test]
    fn a_deep_thread_holds_at_most_its_share() {
        let h = HotLane { deep_share_pct: 125, ..HotLane::default() };
        assert_eq!(h.deep_cap(256, 4, 252, 4), 80, "125% of 64, whole runs");
        assert_eq!(h.deep_cap(250, 4, 252, 4), 80, "rounded up to whole runs");
        assert_eq!(h.deep_cap(10, 4, 252, 4), 4, "never below one run");
        assert_eq!(h.deep_cap(4000, 4, 252, 2), 252, "never above the cap");
        assert_eq!(HotLane { deep_share_pct: 0, ..HotLane::default() }.deep_cap(256, 4, 252, 4), 252, "off: the cap");
    }

    /// Default layout of the isolation round: fair-share cap 125% and one
    /// engine per device and reactor.
    #[test]
    fn share_and_shared_engines_are_the_default() {
        assert_eq!(HotLane::default().deep_share_pct, 125);
        assert!(super::SHARED_ENGINE_BY_DEFAULT);
    }
}

#[cfg(test)]
mod shared_tests {
    use super::Shared;
    use std::rc::Rc;

    /// Pool tenancies of one device on one reactor share one engine: the
    /// registry hands out the live value under a key while a holder keeps
    /// it, a new one once all holders are gone, and keys stay apart.
    /// A device's tenancies on one reactor share one tag executor with the
    /// engine: a tag task of tenancy A, woken by a network completion that
    /// tenancy B's turn drives, runs in B's turn (it waited for the next
    /// poll when each tenancy had its own tag executor).
    #[test]
    fn tenancies_of_a_shared_engine_share_the_tag_executor() {
        let mut reg: Shared<super::EngineShare<u8>> = Shared::new();
        let (a, new_a) = super::join_share(&mut reg, 1, |_| Rc::new(0));
        let (b, new_b) = super::join_share(&mut reg, 1, |_| Rc::new(1));
        assert!(new_a && !new_b);
        assert!(Rc::ptr_eq(&a.engine, &b.engine) && Rc::ptr_eq(&a.net, &b.net));
        assert!(Rc::ptr_eq(&a.tags, &b.tags), "one tag executor per shared engine");
        let (other, _) = super::join_share(&mut reg, 2, |_| Rc::new(2));
        assert!(!Rc::ptr_eq(&a.tags, &other.tags), "another device has its own");
        // A's tag task waits for a completion the network executor delivers.
        let (tx, rx) = smol::channel::bounded::<()>(1);
        let done = Rc::new(std::cell::Cell::new(false));
        let d = done.clone();
        let task = a.tags.spawn(async move {
            rx.recv().await.unwrap();
            d.set(true);
        });
        while a.tags.try_tick() {}
        let net_task = b.net.spawn(async move { tx.send(()).await.unwrap() });
        // B's turn: its executors to quiescence, as QEngine::run_turn does.
        let mut progress = true;
        while progress {
            progress = false;
            while b.tags.try_tick() {
                progress = true;
            }
            while b.net.try_tick() {
                progress = true;
            }
        }
        assert!(done.get(), "A's request completed within B's turn");
        drop((task, net_task));
    }

    #[test]
    fn a_reactor_shares_one_engine_per_device() {
        let mut s: Shared<u32> = Shared::new();
        assert!(s.get(1).is_none());
        let a = Rc::new(7u32);
        s.put(1, &a);
        let b = s.get(1).expect("second tenancy of device 1 finds the engine");
        assert!(Rc::ptr_eq(&a, &b));
        assert!(s.get(2).is_none(), "another device has its own");
        drop(a);
        assert!(s.get(1).is_some(), "alive while a tenancy holds it");
        drop(b);
        assert!(s.get(1).is_none(), "gone with its last tenancy");
        assert!(s.v.is_empty(), "dead entries are dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_tenancy_uses_its_reactors_liveness() {
        let q = QueueShared::new(2);
        q.beat(0, true); // locally idle does not mean its reactor is asleep
        let reactor = Arc::new(Beat::new());
        q.reactor_beats[0].set(reactor.clone()).ok().unwrap();
        reactor.at_ns.store(10, Ordering::Relaxed);
        reactor.waiting.store(false, Ordering::Relaxed);
        let b = q.heartbeat(0).unwrap();
        assert!(should_take_over(1, 0, b.at_ns.load(Ordering::Relaxed), b.waiting.load(Ordering::Relaxed), 100, Duration::from_nanos(20)));
        reactor.waiting.store(true, Ordering::Relaxed);
        assert!(!should_take_over(1, 0, b.at_ns.load(Ordering::Relaxed), b.waiting.load(Ordering::Relaxed), 100, Duration::from_nanos(20)));
    }

    #[test]
    fn thread_zero_prepares_and_spill_is_clamped() {
        let c = batch_config(true, 16, 64);
        assert!(c.prepare_tags());
        assert_eq!(c.spill_tags(), 16);
        let c = batch_config(false, 16, 64);
        assert!(!c.prepare_tags());
        // 0 would turn spill mode off (one thread would take everything).
        assert_eq!(batch_config(false, 0, 64).spill_tags(), 1);
        assert_eq!(batch_config(false, 500, 64).spill_tags(), 64);
    }

    #[test]
    fn other_threads_wait_for_thread_zero() {
        let s = Arc::new(QueueShared::default());
        let ab = Arc::new(AtomicBool::new(false));
        let (s2, ab2) = (s.clone(), ab.clone());
        let t = std::thread::spawn(move || s2.wait(&ab2).map(|b| b.is_some()));
        std::thread::sleep(Duration::from_millis(20));
        PrepReport { shared: Some(&s) }.ready(Some(Arc::new(vec![IoBuf::<u8>::new(4096)])));
        assert_eq!(t.join().unwrap(), Some(true));
    }

    /// Hot lane: the primary may hold all but `lease` tags and keeps at
    /// most `lease` credits provided; a secondary has a few spill-only
    /// credits. The lease leaves tags a stuck primary can never take.
    #[test]
    fn hot_lane_credits_per_role() {
        let h = HotLane::default();
        assert_eq!(h.lease, 4, "lease 4 won 21 of 26 cells in the L3 run");
        assert_eq!(h.credits(true, false, 64), (60, 4, true));
        assert_eq!(h.credits(false, false, 64), (4, 4, false));
        let c = hot_batch_config(true, true, &h, 64);
        assert!(c.prepare_tags());
        assert_eq!((c.spill_tags(), c.lease_tags(), c.refill()), (60, 4, true));
        let c = hot_batch_config(false, false, &h, 64);
        assert_eq!((c.spill_tags(), c.lease_tags(), c.refill()), (4, 4, false));
        // Tiny queues: the primary still leaves at least one tag.
        let h16 = HotLane { lease: 16, ..h };
        assert_eq!(h16.credits(true, false, 8), (1, 7, true));
        assert_eq!(h16.credits(true, false, 2), (1, 1, true));
        let big = HotLane { lease: 100, secondary: 100, ..h };
        assert_eq!(big.credits(true, false, 64), (1, 63, true));
        assert_eq!(big.credits(false, false, 64), (64, 64, false));
    }

    /// Depth mode (L3b): every thread, secondaries included, takes short
    /// spill-only runs up to the primary's cap, so a deep queue is shared by
    /// all its threads instead of funnelled through the primary with the
    /// secondaries parked at 4 requests; and the primary does not refill at
    /// the head (it would keep the head at a closed-loop arrival rate).
    #[test]
    fn depth_mode_shares_the_queue_over_every_thread() {
        let h = HotLane::default();
        assert_eq!(h.credits(true, true, 64), (60, 2, false));
        assert_eq!(h.credits(false, true, 64), (60, 2, false), "a deep secondary has the primary's cap");
        let h = HotLane { deep_lease: 8, ..h };
        assert_eq!(h.credits(false, true, 64), (60, 8, false));
        // The run never exceeds the cap.
        let h = HotLane { lease: 60, deep_lease: 50, ..h };
        assert_eq!(h.credits(false, true, 64), (4, 4, false));
    }

    /// Depth mode runs follow the queue's request size: runs of
    /// `deep_lease_small` (4) while the average is small, `deep_lease` (2)
    /// otherwise; shallow credits do not depend on size.
    #[test]
    fn depth_mode_runs_follow_request_size() {
        let h = HotLane::default();
        assert_eq!((h.deep_lease, h.deep_lease_small, h.small_bytes), (2, 4, 8192));
        assert_eq!(h.credits_sized(true, true, true, 256), (252, 4, false));
        assert_eq!(h.credits_sized(false, true, true, 256), (252, 4, false));
        assert_eq!(h.credits_sized(false, true, false, 256), (252, 2, false));
        assert_eq!(h.credits_sized(true, false, true, 256), h.credits(true, false, 256), "shallow: size does not matter");
        assert_eq!(h.credits_sized(false, false, true, 256), h.credits(false, false, 256));
        // Hysteresis: small at <= 8 KiB, large above 16 KiB.
        assert!(h.next_small(false, 8192) && !h.next_small(false, 8193));
        assert!(h.next_small(true, 16384) && !h.next_small(true, 16385));
        // The queue's average: 4k requests are small, a run of 1 MiB ones is not,
        // and a lone 16k request among 4k ones does not flip it.
        let q = QueueShared::new(4);
        assert!(q.note_sizes(&h, std::iter::repeat(4096).take(32)));
        assert!(q.note_sizes(&h, std::iter::once(16384)));
        assert!(!q.note_sizes(&h, std::iter::repeat(1 << 20).take(4)));
        assert!(!q.note_sizes(&h, std::iter::empty()), "nothing fetched: unchanged");
        assert!(q.note_sizes(&h, std::iter::repeat(4096).take(64)), "back to small after a run of 4k");
    }

    #[test]
    fn depth_mode_has_hysteresis_around_the_lease() {
        let h = HotLane::default(); // lease 4
        assert!(!h.next_deep(false, 4), "QD4 on one queue stays on the primary");
        assert!(h.next_deep(false, 5));
        assert!(h.next_deep(true, 3), "no flapping just below the lease");
        assert!(!h.next_deep(true, 2));
        let q = QueueShared::new(4);
        assert_eq!(q.publish_held(0, 3), 3);
        assert_eq!(q.publish_held(0, 3), 6);
        assert!(q.update_deep(&h, 6));
        assert_eq!(q.publish_held(3, 0), 3);
        assert!(q.update_deep(&h, 3), "still deep at 3");
        assert_eq!(q.publish_held(3, 1), 1);
        assert!(!q.update_deep(&h, 1));
    }

    /// Byte weights count only against the shallow primary's cap: a 1 MiB
    /// request (17 credits) must not park a 4-credit secondary, and in depth
    /// mode the rotation spreads the bytes.
    #[test]
    fn only_the_shallow_primary_weighs_requests() {
        let h = HotLane::default();
        assert!(h.weighs(true, false));
        assert!(!h.weighs(false, false));
        assert!(!h.weighs(true, true));
        assert!(!h.weighs(false, true));
    }

    /// Spin only while shallow: at depth completions arrive faster than a
    /// sleep costs, and a spinning primary through a deep run made the hot
    /// lane cost more CPU per I/O than the kernel (L3 run).
    #[test]
    fn nothing_spins_in_depth_mode() {
        let h = HotLane::default();
        assert!(h.spins(true, false, true, true, true));
        assert!(!h.spins(true, false, false, false, false), "idle primary past its warm window sleeps");
        assert!(h.spins(false, false, false, true, true));
        assert!(!h.spins(false, false, true, false, true), "a secondary holding nothing sleeps");
        assert!(!h.spins(true, true, true, true, true));
        assert!(!h.spins(false, true, true, true, true));
        assert!(!HotLane { primary_spin: false, ..h }.spins(true, false, true, true, true), "primary spin off");
        let h = HotLane { deep_spin: true, ..h };
        assert!(h.spins(true, true, false, true, true), "deep_spin: every thread holding requests, as per-tag");
        assert!(h.spins(false, true, false, true, true));
        assert!(!h.spins(false, true, true, false, true));
        assert!(!h.spins(true, true, true, true, false), "no event within spin_us: sleep");
    }

    /// A primary waiting on a slow command (a flush: milliseconds) stops
    /// spinning ~1 ms after its last event; a normal round trip keeps it hot.
    #[test]
    fn a_slow_command_does_not_keep_the_primary_spinning() {
        let rtt = Some(Duration::from_micros(160));
        let us = Duration::from_micros;
        assert_eq!(spin_cap(rtt), us(960));
        assert_eq!(spin_cap(Some(Duration::from_millis(3))), Duration::from_millis(1));
        assert!(primary_hot(true, us(500), us(0), rtt), "within a normal round trip");
        assert!(!primary_hot(true, us(1500), us(0), rtt), "1.5 ms without an event on one command");
        assert!(primary_hot(false, us(5000), us(200), rtt), "warm window after the wire went empty");
        assert!(!primary_hot(false, us(5000), us(300), rtt));
    }

    /// The warm window follows the completion of a long command: the
    /// primary stopped spinning 1 ms into a 3 ms flush and slept; when the
    /// flush completes it is hot again for the warm window, so the next
    /// write finds it awake.
    #[test]
    fn the_warm_window_follows_a_long_command() {
        let rtt = Some(Duration::from_micros(160)); // warm window 240 us, spin cap 960 us
        let us = Duration::from_micros;
        let t0 = Instant::now();
        let mut w = WarmClock::new(t0);
        assert!(w.turn(true, t0, us(0), rtt), "flush on the wire, fresh event");
        assert!(!w.turn(true, t0 + us(1500), us(1500), rtt), "no event for 1.5 ms: sleeps");
        // The flush completes during the sleep; this turn reaped it.
        assert!(w.turn(false, t0 + us(3000), us(0), rtt), "warm right after the completion");
        assert!(w.turn(false, t0 + us(3200), us(200), rtt), "still inside the window");
        assert!(!w.turn(false, t0 + us(3300), us(300), rtt), "window over");
        // Idle stays cold.
        assert!(!w.turn(false, t0 + us(9000), us(6000), rtt));
    }

    #[test]
    fn warm_window_is_one_and_a_half_rtt_within_bounds() {
        assert_eq!(warm_window(None), Duration::from_micros(100));
        assert_eq!(warm_window(Some(Duration::from_micros(40))), Duration::from_micros(100));
        assert_eq!(warm_window(Some(Duration::from_micros(200))), Duration::from_micros(300));
        assert_eq!(warm_window(Some(Duration::from_millis(5))), Duration::from_millis(1));
    }

    #[test]
    fn watchdog_takes_over_only_from_a_primary_that_neither_turns_nor_sleeps() {
        let w = Duration::from_millis(1000);
        let s = 1_000_000_000u64;
        assert!(should_take_over(1, 0, 0, false, s + 1, w));
        assert!(!should_take_over(1, 0, 0, true, 10 * s, w), "sleeping in its ring is not wedged");
        assert!(!should_take_over(1, 0, s, false, s + s / 2, w), "turned 0.5 s ago");
        assert!(!should_take_over(0, 0, 0, false, 10 * s, w), "the primary does not replace itself");

        // Timing margins are wide: tests run in parallel on a loaded host.
        let q = QueueShared::new(4);
        let (short, long) = (Duration::from_millis(20), Duration::from_secs(30));
        q.beat(0, false);
        assert_eq!(q.check_primary(1, long), None, "fresh heartbeat");
        std::thread::sleep(Duration::from_millis(40));
        q.beat(0, true);
        assert_eq!(q.check_primary(1, short), None, "waiting in the ring");
        q.beat(0, false);
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(q.check_primary(2, short), Some(0), "stale and not waiting: taken over");
        assert_eq!(q.primary(), 2);
        assert_eq!(q.check_primary(1, long), None, "the new primary stamped its heartbeat when it took over");
        // Only one thread wins a takeover race.
        let q = std::sync::Arc::new(QueueShared::new(4));
        let short = Duration::from_millis(300);
        std::thread::sleep(Duration::from_millis(600));
        let winners: Vec<Option<u16>> = (1..4u16)
            .map(|t| {
                let q = q.clone();
                std::thread::spawn(move || q.check_primary(t, short))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        assert_eq!(winners.iter().filter(|w| w.is_some()).count(), 1, "{winners:?}");
        assert_ne!(q.primary(), 0);
    }

    #[test]
    fn thread_zero_failing_or_unwinding_releases_the_others() {
        let s = QueueShared::default();
        let ab = AtomicBool::new(false);
        drop(PrepReport { shared: Some(&s) });
        assert!(s.wait(&ab).is_none());

        let s = QueueShared::default();
        ab.store(true, Ordering::Release);
        assert!(s.wait(&ab).is_none(), "an abandoned bring-up must not wait for thread 0");
    }
}
