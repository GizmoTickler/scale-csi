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
use libublk::io::{UblkBatchBuffers, UblkBatchCompletion, UblkBatchConfig, UblkBatchQueue, UblkDev, UblkQueue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
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
    /// Most credits the primary keeps provided at once (NVMEUBLK_HOT_LEASE,
    /// default 16); the primary holds at most depth - lease requests.
    pub lease: u16,
    /// Credits of a secondary (NVMEUBLK_HOT_SECONDARY, default 4).
    pub secondary: u16,
    /// A primary neither turning nor sleeping this long is replaced
    /// (NVMEUBLK_HOT_WEDGE_MS, default 1000).
    pub wedge: Duration,
}

impl Default for HotLane {
    fn default() -> Self {
        HotLane { lease: 16, secondary: 4, wedge: Duration::from_millis(1000) }
    }
}

impl HotLane {
    pub fn from_env() -> Self {
        let d = HotLane::default();
        HotLane {
            lease: env_u64("NVMEUBLK_HOT_LEASE", d.lease as u64).clamp(1, u16::MAX as u64) as u16,
            secondary: env_u64("NVMEUBLK_HOT_SECONDARY", d.secondary as u64).clamp(1, u16::MAX as u64) as u16,
            wedge: Duration::from_millis(env_u64("NVMEUBLK_HOT_WEDGE_MS", d.wedge.as_millis() as u64).max(10)),
        }
    }

    /// (spill cap, lease, refill) of a thread in this role, for a queue of
    /// `depth` tags.
    pub fn credits(&self, primary: bool, depth: u16) -> (u16, u16, bool) {
        let depth = depth.max(2);
        if primary {
            let lease = self.lease.clamp(1, depth - 1);
            (depth - lease, lease, true)
        } else {
            let c = self.secondary.clamp(1, depth);
            (c, c, false)
        }
    }
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
struct Beat {
    at_ns: AtomicU64,
    waiting: AtomicBool,
}

/// Whether thread `me` should take the primary role from `primary`, given
/// the primary's heartbeat.
fn should_take_over(me: u16, primary: u16, beat_ns: u64, waiting: bool, now_ns: u64, wedge: Duration) -> bool {
    me != primary && !waiting && now_ns.saturating_sub(beat_ns) > wedge.as_nanos() as u64
}

/// What the threads of one queue share: whether thread 0 has prepared the
/// queue's tags (the others may post their fetches only then), the queue's
/// tag buffers in the copying mode, and (hot lane) which thread is the
/// primary and every thread's heartbeat.
pub struct QueueShared {
    st: Mutex<Prep>,
    cv: Condvar,
    primary: AtomicU16,
    beats: Box<[Beat]>,
}

impl Default for QueueShared {
    fn default() -> Self {
        QueueShared::new(8)
    }
}

impl QueueShared {
    /// For a queue served by `threads` io threads.
    pub fn new(threads: u16) -> Self {
        let now = mono_ns();
        let beats = (0..threads.max(1)).map(|_| Beat { at_ns: AtomicU64::new(now), waiting: AtomicBool::new(false) }).collect();
        QueueShared { st: Mutex::new(Prep::Waiting), cv: Condvar::new(), primary: AtomicU16::new(0), beats }
    }

    fn beat(&self, thread: u16, waiting: bool) {
        if let Some(b) = self.beats.get(thread as usize) {
            b.at_ns.store(mono_ns(), Ordering::Relaxed);
            b.waiting.store(waiting, Ordering::Relaxed);
        }
    }

    pub fn primary(&self) -> u16 {
        self.primary.load(Ordering::Acquire)
    }

    /// Watchdog, run by any thread of the queue: take the primary role over
    /// if the primary has stopped turning. Some(old primary) if it did.
    fn check_primary(&self, me: u16, wedge: Duration) -> Option<u16> {
        let p = self.primary();
        let b = self.beats.get(p as usize)?;
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

/// As `batch_config`, for a hot-lane thread in the given role.
pub fn hot_batch_config(leader: bool, primary: bool, hot: &HotLane, depth: u16) -> UblkBatchConfig {
    let (spill, lease, refill) = hot.credits(primary, depth);
    batch_config(leader, spill, depth).with_lease_tags(lease).with_refill(refill)
}

/// The batch-mode queue thread: io thread `libublk::io::io_thread_idx()` of
/// queue `qid`. Same engine, same request path (`serve_request`) as the
/// per-tag mode; the tags it serves are the ones its own fetch receives.
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
    let leader = thread == 0;
    shared.beat(thread, false);
    let report = PrepReport { shared: leader.then_some(&*shared) };
    setup_queue_ring(qid, dev);
    let q_rc = match UblkQueue::new(qid, dev) {
        Ok(q) => Rc::new(q),
        Err(e) => {
            log::error!("ublk device {} queue {qid} thread {thread}: queue setup failed: {e}", dev.dev_info.dev_id);
            return;
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
                log::error!("ublk device {} queue {qid} thread {thread}: thread 0 did not prepare the queue; leaving", dev.dev_info.dev_id);
                return;
            }
        }
    };
    if copying && bufs.is_none() {
        log::error!("ublk device {} queue {qid} thread {thread}: no tag buffers for the copying mode", dev.dev_info.dev_id);
        return;
    }
    let buffers = match &bufs {
        Some(b) => UblkBatchBuffers::Shared(b.clone()),
        None => UblkBatchBuffers::None,
    };
    let mut is_primary = hot.is_some() && shared.primary() == thread;
    let config = match &hot {
        Some(h) => hot_batch_config(leader, is_primary, h, depth),
        None => batch_config(leader, spill, depth),
    };
    let mut batch = match UblkBatchQueue::new(&q_rc, buffers, config) {
        Ok(b) => b,
        Err(e) => {
            log::error!("ublk device {} queue {qid} thread {thread}: batch setup failed: {e}", dev.dev_info.dev_id);
            return;
        }
    };
    report.ready(bufs.clone());

    let shift = ctrls.info.lba_shift;
    let net_exe: Rc<smol::LocalExecutor<'static>> = Rc::new(smol::LocalExecutor::new());
    let mut cfg = cfg;
    let user_copy = dev.dev_info.flags & libublk::sys::UBLK_F_USER_COPY as u64 != 0;
    cfg.cdev_fd = if user_copy { dev.tgt.fds[0] } else { -1 };
    let cdev_fd = cfg.cdev_fd;
    let zc = dev.dev_info.flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0;
    if zc {
        cfg.rx_offload = 0;
    }
    let eid = qid * dev.io_threads_per_queue() + thread;
    let engine = qengine::QEngine::new(eid, ctrls, cfg, net_exe.clone(), stats.clone(), stop.clone(), draining);
    engine.start();

    // One task per tag: any tag may be fetched by this thread. A task sleeps
    // on its tag's channel until the tag is fetched here, serves the request
    // and queues its result for the next commit.
    let completions: Rc<RefCell<Vec<UblkBatchCompletion>>> = Rc::new(RefCell::new(Vec::with_capacity(depth as usize)));
    let exe = smol::LocalExecutor::new();
    let mut arrive = Vec::with_capacity(depth as usize);
    let mut tasks = Vec::with_capacity(depth as usize);
    for tag in 0..depth {
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

    let run_ops = || {
        let t0 = Instant::now();
        stats.loops.fetch_add(1, Ordering::Relaxed);
        let mut progress = true;
        while progress {
            progress = false;
            while exe.try_tick() {
                progress = true;
            }
            while net_exe.try_tick() {
                progress = true;
            }
        }
        stats.loop_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    };
    let spin = Duration::from_micros(env_u64("NVMEUBLK_SPIN_US", 100));
    let spin_idle = env_u64("NVMEUBLK_SPIN_IDLE", 1) != 0;
    let timeout = io_uring::types::Timespec::new().sec(20);
    let weight_bytes = env_u64("NVMEUBLK_BATCH_WEIGHT_KB", 64) << 10;
    let mut last_event = Instant::now();
    // Hot lane: last turn with a command on the wire (warm window), and the
    // last watchdog check.
    let mut last_busy = Instant::now();
    let mut last_check = Instant::now();
    let mut pending: Vec<UblkBatchCompletion> = Vec::with_capacity(depth as usize);
    let mut cqes: Vec<io_uring::cqueue::Entry> = Vec::with_capacity(dev.tgt.cq_depth as usize);
    let mut arrived: Vec<u16> = Vec::with_capacity(depth as usize);
    let (mut seen_tags, mut seen_spills) = (0u64, 0u64);
    let dev_id = dev.dev_info.dev_id;
    log::info!(
        "ublk device {dev_id} queue {qid} thread {thread}: batch I/O, {}{}",
        match &hot {
            Some(_) => format!("hot lane {} (cap {}, lease {}, refill {})", if is_primary { "primary" } else { "secondary" }, batch.config().spill_tags(), batch.config().lease_tags(), batch.config().refill()),
            None => format!("spill at {} requests", batch.config().spill_tags()),
        },
        if leader { " (prepared the queue)" } else { "" }
    );

    // A thread that leaves because its queue stopped reports itself as
    // waiting (not wedged), so no sibling takes over during teardown; one
    // that leaves on an error does not, and a sibling takes its role.
    let mut clean_exit = false;
    run_ops();
    loop {
        // Results the tag tasks produced go back to the driver in one commit.
        pending.append(&mut completions.borrow_mut());
        if !pending.is_empty() {
            match batch.try_submit_completions(&pending) {
                Ok(true) => pending.clear(),
                // Every commit slot is in flight: retried after its CQE.
                Ok(false) => {}
                Err(e) => {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: commit failed: {e}");
                    break;
                }
            }
        }
        let (tags, spills) = (batch.fetched_tag_count(), batch.spill_count());
        if tags != seen_tags {
            stats.batch_tags.fetch_add(tags - seen_tags, Ordering::Relaxed);
            seen_tags = tags;
        }
        if spills != seen_spills {
            stats.batch_spills.fetch_add(spills - seen_spills, Ordering::Relaxed);
            seen_spills = spills;
        }
        // A detach, or a bring-up that gave up: leave; the ring's teardown
        // cancels the fetch, and closing the char device takes back the
        // requests still held.
        if abandoned.load(Ordering::Acquire) {
            clean_exit = true;
            break;
        }
        // Stopped: the driver aborted this thread's fetch and every request
        // it had taken is committed. Hand the fetch buffers back, then leave.
        if batch.all_fetches_stopped() && batch.owned_tag_count() == 0 && pending.is_empty() && batch.inflight_commit_count() == 0 {
            match batch.try_begin_shutdown() {
                Ok(_) if batch.is_shutdown_complete() => {
                    clean_exit = true;
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: batch shutdown failed: {e}");
                    break;
                }
            }
        }

        // Hot lane: the watchdog (a few times a second, on any thread that
        // is awake anyway), and the role this thread has now.
        if let Some(h) = &hot {
            if last_check.elapsed() >= Duration::from_millis(250) {
                last_check = Instant::now();
                if !stop.load(Ordering::Acquire) {
                    if let Some(old) = shared.check_primary(thread, h.wedge) {
                        stats.batch_takeovers.fetch_add(1, Ordering::Relaxed);
                        log::warn!("ublk device {dev_id} queue {qid}: primary thread {old} has not turned its loop for {:?}; thread {thread} takes over", h.wedge);
                    }
                }
            }
            let now_primary = shared.primary() == thread;
            if now_primary != is_primary {
                is_primary = now_primary;
                let (cap, lease, refill) = h.credits(is_primary, depth);
                if let Err(e) = batch.set_credit_policy(cap, lease, refill) {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: role change failed: {e}");
                    break;
                }
                log::info!("ublk device {dev_id} queue {qid} thread {thread}: now the {}", if is_primary { "primary" } else { "secondary" });
            }
            // Fault injection "wedge <ms>": the primary stops turning.
            if let Some(d) = engine.take_wedge() {
                if is_primary {
                    log::warn!("ublk device {dev_id} queue {qid} thread {thread}: fault injection: primary wedged for {d:?}");
                    std::thread::sleep(d);
                }
            }
        }

        // Wait for events. Hot-lane primary: the warm window; otherwise
        // adaptive polling as in the per-tag loop.
        let spinning = match &hot {
            Some(_) if is_primary => {
                if engine.inflight_here() > 0 {
                    last_busy = Instant::now();
                    true
                } else {
                    last_busy.elapsed() < warm_window(engine.wire_rtt())
                }
            }
            Some(_) => !spin.is_zero() && (engine.inflight_here() > 0 || batch.owned_tag_count() > 0) && last_event.elapsed() < spin,
            None => !spin.is_zero() && (spin_idle || engine.inflight_here() > 0 || batch.owned_tag_count() > 0) && last_event.elapsed() < spin,
        };
        if !spinning {
            shared.beat(thread, true);
        }
        let polled = poll(if spinning { 0 } else { 1 }, &timeout);
        shared.beat(thread, false);
        if let Err(e) = polled {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: event loop failed: {e}");
            break;
        }
        cqes.clear();
        while let Some(c) = libublk::io::pop_deferred_queue_cqe() {
            cqes.push(c);
        }
        libublk::io::with_task_io_ring_mut(|r| cqes.extend(r.completion()));
        if !cqes.is_empty() {
            last_event = Instant::now();
        }
        let mut failed = None;
        for cqe in &cqes {
            match batch.handle_cqe(cqe, |_, tags| {
                arrived.extend_from_slice(tags);
                Ok(())
            }) {
                Ok(true) => {}
                Ok(false) => {
                    // A SEND_ZC buffer-release notification carries the
                    // send's user_data, whose future already completed.
                    if io_uring::cqueue::notif(cqe.flags()) {
                        stats.zc_notif.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    libublk::uring_async::ublk_wake_task(cqe.user_data(), cqe);
                }
                Err(e) => {
                    failed.get_or_insert(e);
                }
            }
        }
        // Weighted spill (NVMEUBLK_BATCH_WEIGHT_KB, default 64; 0 = off):
        // a request counts one extra credit per WEIGHT_KB of payload, so a
        // thread holding large requests spills sooner and big transfers
        // spread over the queue's threads, while small ones stay put.
        if weight_bytes > 0 {
            for &tag in arrived.iter() {
                let bytes = (q_rc.get_iod(tag).nr_sectors as u64) << 9;
                let extra = (bytes / weight_bytes).min(u16::MAX as u64) as u16;
                if extra > 0 {
                    batch.add_tag_weight(tag, extra);
                }
            }
        }
        if let Err(e) = batch.settle_credits() {
            failed.get_or_insert(e);
        }
        for tag in arrived.drain(..) {
            if arrive[tag as usize].try_send(()).is_err() {
                log::error!("ublk device {dev_id} queue {qid} thread {thread}: tag {tag} fetched while its task is busy or gone");
            }
        }
        run_ops();
        if let Some(e) = failed {
            log::error!("ublk device {dev_id} queue {qid} thread {thread}: batch transport failed: {e}");
            break;
        }
    }
    shared.beat(thread, clean_exit);
    log::info!("ublk device {dev_id} queue {qid} thread {thread}: batch loop ended ({} requests, {} spills)", batch.fetched_tag_count(), batch.spill_count());
    // Drop order: the tag tasks and their executor, then the engine (its
    // Drop drives its tasks to their end on this ring), then the batch
    // transport and the queue.
    drop(arrive);
    drop(tasks);
    drop(exe);
    drop(engine);
    drop(net_exe);
    drop(batch);
}

/// Submit what is queued and wait for `wait` completions at most `timeout`.
fn poll(wait: usize, timeout: &io_uring::types::Timespec) -> std::io::Result<()> {
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
mod tests {
    use super::*;

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
        assert_eq!(h.credits(true, 64), (48, 16, true));
        assert_eq!(h.credits(false, 64), (4, 4, false));
        let c = hot_batch_config(true, true, &h, 64);
        assert!(c.prepare_tags());
        assert_eq!((c.spill_tags(), c.lease_tags(), c.refill()), (48, 16, true));
        let c = hot_batch_config(false, false, &h, 64);
        assert_eq!((c.spill_tags(), c.lease_tags(), c.refill()), (4, 4, false));
        // Tiny queues: the primary still leaves at least one tag.
        assert_eq!(h.credits(true, 8), (1, 7, true));
        assert_eq!(h.credits(true, 2), (1, 1, true));
        let big = HotLane { lease: 100, secondary: 100, ..h };
        assert_eq!(big.credits(true, 64), (1, 63, true));
        assert_eq!(big.credits(false, 64), (64, 64, false));
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
