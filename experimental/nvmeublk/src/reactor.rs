//! L7: the node-wide reactor pool (design doc nvmeublk-userspace-architecture.md
//! §4.2, §4.4 L7; SCOPE.md P1-P4, with batch mode instead of tag slices).
//!
//! Instead of Q x T queue threads per volume, each with its own ring, a
//! fixed pool of R reactor threads serves every queue of every volume. A
//! reactor owns one io_uring and hosts *tenancies* (batchq::Tenancy): io
//! thread t of ublk queue q of some device, i.e. one multishot fetch on the
//! queue with its hot-lane credits, one engine (connections) and the tag
//! tasks. A queue's T tenancies sit on T distinct reactors, its first
//! primary among the first `primaries` reactors, so the QD1 lanes of many
//! volumes share a few warm reactors instead of spinning one thread each.
//!
//! One turn of a reactor: every tenancy's `before_wait` (commits, credit
//! policy, watchdog, whether it wants the reactor to keep polling), one
//! poll of the ring (without sleeping while any tenancy is hot: the warm
//! window is per reactor), CQEs routed by the ring key batch commands carry
//! (libublk `UblkSharedSlot`), every tenancy's `after_wait`. NAPI busy poll
//! is a property of the ring (crate::napi), with the adaptive budget fed by
//! the reactor's blocking waits.
//!
//! Ring layout: a sparse file table of MAX_TENANCIES slots (tenancy key =
//! file slot = ring key) and a sparse buffer table of 16384 slots handed out
//! in 256-slot chunks. With batch I/O and AUTO_BUF_REG the driver registers
//! a request's pages at the index its tag's last PREP/COMMIT named, in
//! whichever ring fetches it, so a queue's range must be the same on every
//! reactor that hosts one of its tenancies: ranges are allocated here, by
//! the pool, free on all of them.
//!
//! Watchdog: a tenancy's heartbeat is stamped every reactor turn, so a
//! wedged reactor stops the heartbeats of all its tenancies, and each
//! queue's tenancies on other reactors take the primary role over as before
//! (batchq::QueueShared::check_primary): the wedged primary is re-armed on
//! another reactor. What the wedged reactor had already fetched waits for
//! it (its pages are registered in that ring).
//!
//! Lifecycle: a tenancy cannot cancel its fetch while the ring lives (the
//! driver cancels uring_cmds only on task or ring exit, or on STOP/QUIESCE
//! of the device), so a pooled device ends by STOP: its tenancies keep
//! serving until the driver ends their fetches, then hand their buffers
//! back and leave (device.rs `retire`). A tenancy that fails asks for its
//! device to be stopped and serves on until then.
//!
//! NVMEUBLK_REACTORS (0 = the per-volume threads, for A/B and fallback),
//! NVMEUBLK_REACTOR_PRIMARIES (reactors that take queues' primaries).

use crate::batchq::{self, Next, Tenancy};
use libublk::io::UblkSharedSlot;
use slab::Slab;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

/// Tenancies (and fixed-file slots) per reactor ring.
pub const MAX_TENANCIES: usize = 512;
/// The ring's buffer table (the kernel's maximum) and its allocation unit.
const BUF_SLOTS: u32 = 16384;
const CHUNK: u32 = 256;
const CHUNKS: usize = (BUF_SLOTS / CHUNK) as usize;
/// SQ and CQ sizes of a reactor ring (COOP rings cannot be resized).
const SQ_ENTRIES: u32 = 4096;
const CQ_ENTRIES: u32 = 32768;
/// user_data of the mailbox's multishot poll: no Target bit, and a slab key
/// no future has.
const MAILBOX_UD: u64 = 0x3fff_ffff_ffff_0000;

/// Whether the pool serves by default (NVMEUBLK_REACTORS unset); "auto"
/// asks for it with the default size.
const POOL_BY_DEFAULT: bool = false;

/// Reactor count by default (NVMEUBLK_REACTORS unset): half the CPUs, at
/// least 4 (a queue's four tenancies need four reactors), at most 8.
pub fn default_reactors(ncpu: usize) -> usize {
    (ncpu / 2).clamp(4, 8)
}

/// Reactors taking queues' primaries by default: half of them.
pub fn default_primaries(reactors: usize) -> usize {
    (reactors / 2).max(1)
}

thread_local! {
    /// The index of the reactor this thread is (None off the pool).
    static THIS_REACTOR: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// The index of the pool reactor the calling thread is, if it is one.
pub fn this_reactor() -> Option<usize> {
    THIS_REACTOR.with(|r| r.get())
}

/// A job for a reactor, run on its thread between turns.
type Job = Box<dyn FnOnce(&mut Reactor) + Send>;

/// Set a tenancy up on a reactor.
pub struct Attach {
    /// The queue's buffer range (allocated by `Pool::alloc_range`).
    pub buf_base: u16,
    pub depth: u16,
    /// Build the tenancy at the slot the reactor gives it (on the reactor
    /// thread); None if it failed.
    pub build: Box<dyn FnOnce(UblkSharedSlot) -> Option<Tenancy> + Send>,
    /// Run once the tenancy has stopped serving (or failed to start), after
    /// it is dropped.
    pub on_end: Box<dyn FnOnce() + Send>,
}

/// A reactor as the rest of the process sees it.
pub struct Handle {
    idx: usize,
    tid: AtomicI32,
    mail: Mutex<Vec<Job>>,
    efd: i32,
    /// Tenancies hosted.
    pub hosted: AtomicUsize,
    /// Turns, and turns that polled without sleeping (stats).
    pub turns: AtomicU64,
    pub spins: AtomicU64,
    /// Registered NAPI budget (µs + 1; 0 = none) and registrations (stats).
    pub napi: AtomicU32,
    pub napi_regs: AtomicU64,
}

impl Handle {
    fn send(&self, job: Job) {
        self.mail.lock().unwrap_or_else(PoisonError::into_inner).push(job);
        let one: u64 = 1;
        unsafe { libc::write(self.efd, &one as *const u64 as *const libc::c_void, 8) };
    }
}

/// The node's reactors and what is placed on them.
pub struct Pool {
    reactors: Vec<Arc<Handle>>,
    primaries: usize,
    place: Mutex<Placement>,
}

struct Placement {
    next_primary: usize,
    next_secondary: usize,
    /// Tenancies placed per reactor and not yet released.
    load: Vec<usize>,
    /// Per reactor: which 256-slot chunks of its buffer table are in use.
    chunks: Vec<u64>,
}

static POOL: OnceLock<Option<Pool>> = OnceLock::new();

/// The pool, started on first use; None when NVMEUBLK_REACTORS=0 or no
/// reactor could be started (the per-volume threads serve then).
pub fn pool() -> Option<&'static Pool> {
    POOL.get_or_init(|| {
        let ncpu = (unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) }).max(1) as usize;
        let r = match std::env::var("NVMEUBLK_REACTORS").ok().as_deref() {
            Some("auto") => default_reactors(ncpu),
            Some(v) => v.parse().unwrap_or(0),
            None if POOL_BY_DEFAULT => default_reactors(ncpu),
            None => 0,
        };
        if r == 0 {
            log::info!("reactor pool off (NVMEUBLK_REACTORS=0): per-volume queue threads");
            return None;
        }
        let r = r.min(64);
        let primaries = (crate::env_u64("NVMEUBLK_REACTOR_PRIMARIES", default_primaries(r) as u64) as usize).clamp(1, r);
        match Pool::start(r, primaries) {
            Ok(p) => {
                log::info!("reactor pool: {r} reactors, primaries on {primaries}");
                Some(p)
            }
            Err(e) => {
                log::error!("reactor pool could not start ({e}); per-volume queue threads");
                None
            }
        }
    })
    .as_ref()
}

impl Pool {
    fn start(r: usize, primaries: usize) -> std::io::Result<Pool> {
        let mut reactors = Vec::with_capacity(r);
        for idx in 0..r {
            let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            if efd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            reactors.push(Arc::new(Handle {
                idx,
                tid: AtomicI32::new(0),
                mail: Mutex::new(Vec::new()),
                efd,
                hosted: AtomicUsize::new(0),
                turns: AtomicU64::new(0),
                spins: AtomicU64::new(0),
                napi: AtomicU32::new(0),
                napi_regs: AtomicU64::new(0),
            }));
        }
        let ready = Arc::new((Mutex::new(0usize), Condvar::new()));
        for h in &reactors {
            let (h, ready) = (h.clone(), ready.clone());
            std::thread::Builder::new().name(format!("nvq-r{}", h.idx)).spawn(move || run(h, ready))?;
        }
        let (m, cv) = &*ready;
        let mut n = m.lock().unwrap_or_else(PoisonError::into_inner);
        let deadline = Instant::now() + Duration::from_secs(10);
        while *n < r {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(std::io::Error::other("reactors did not come up"));
            }
            n = cv.wait_timeout(n, left).unwrap_or_else(PoisonError::into_inner).0;
        }
        if reactors.iter().any(|h| h.tid.load(Ordering::Acquire) <= 0) {
            return Err(std::io::Error::other("a reactor failed to set its ring up"));
        }
        Ok(Pool { place: Mutex::new(Placement { next_primary: 0, next_secondary: 0, load: vec![0; r], chunks: vec![0; r] }), reactors, primaries })
    }

    pub fn reactors(&self) -> usize {
        self.reactors.len()
    }

    pub fn handles(&self) -> &[Arc<Handle>] {
        &self.reactors
    }

    /// Thread id of reactor `r`.
    pub fn tid(&self, r: usize) -> i32 {
        self.reactors[r].tid.load(Ordering::Acquire)
    }

    /// Reactors for `queues` queues of `threads` tenancies each: [q][t],
    /// distinct within a queue (`threads` <= reactors). Tenancy 0 (the
    /// queue's first primary) goes to the least loaded of the first
    /// `primaries` reactors, the others to the least loaded reactors the
    /// queue does not have yet (ties in rotation). Counted as load until
    /// `release_range`.
    pub fn place(&self, queues: u16, threads: u16) -> Vec<Vec<usize>> {
        let mut guard = self.place.lock().unwrap_or_else(PoisonError::into_inner);
        place_with(&mut guard, self.primaries, queues, threads)
    }

    /// A buffer range of `depth` slots free on every reactor in `rs`, taken
    /// on all of them; None if there is none.
    pub fn alloc_range(&self, rs: &[usize], depth: u16) -> Option<u16> {
        let mut p = self.place.lock().unwrap_or_else(PoisonError::into_inner);
        alloc_range_in(&mut p.chunks, rs, depth)
    }

    /// Give reactor `r`'s part of a range back (and its tenancy's load).
    pub fn release_range(&self, r: usize, base: u16, depth: u16) {
        let mut p = self.place.lock().unwrap_or_else(PoisonError::into_inner);
        let (first, n) = (base as usize / CHUNK as usize, chunks_for(depth));
        let mask = run_mask(first, n);
        p.chunks[r] &= !mask;
        p.load[r] = p.load[r].saturating_sub(1);
    }

    /// Host a tenancy on reactor `r`.
    pub fn attach(&self, r: usize, a: Attach) {
        self.reactors[r].send(Box::new(move |re: &mut Reactor| re.attach(a)));
    }
}

fn chunks_for(depth: u16) -> usize {
    (depth as usize).div_ceil(CHUNK as usize).max(1)
}

fn run_mask(first: usize, n: usize) -> u64 {
    if n >= 64 { u64::MAX } else { ((1u64 << n) - 1) << first }
}

/// `Pool::alloc_range` on explicit per-reactor chunk maps.
fn alloc_range_in(chunks: &mut [u64], rs: &[usize], depth: u16) -> Option<u16> {
    let used = rs.iter().fold(0u64, |a, &r| a | chunks[r]);
    let (first, mask) = alloc_chunks(used, depth)?;
    for &r in rs {
        chunks[r] |= mask;
    }
    Some((first * CHUNK as usize) as u16)
}

/// First run of chunks free in `used` for `depth` slots: (first chunk, mask).
fn alloc_chunks(used: u64, depth: u16) -> Option<(usize, u64)> {
    let n = chunks_for(depth);
    if n > CHUNKS {
        return None;
    }
    (0..=CHUNKS - n).map(|f| (f, run_mask(f, n))).find(|&(_, m)| used & m == 0)
}

/// `Pool::place` on explicit state (tests).
fn place_with(p: &mut Placement, primaries: usize, queues: u16, threads: u16) -> Vec<Vec<usize>> {
    let r = p.load.len();
    let threads = (threads as usize).min(r).max(1);
    let primaries = primaries.clamp(1, r);
    // The least loaded of `cands` (in rotation from `*next`), not in `taken`;
    // among equally loaded ones, reactors at or above `avoid_below` first
    // (secondaries keep off the primary reactors while others are as free:
    // one volume's two queues then use all eight reactors, not seven with
    // one hosting two tenancies (p1: read 16k 64:8 at 0.59x the kernel)).
    fn pick(load: &[usize], cands: usize, next: &mut usize, taken: &[usize], avoid_below: usize) -> Option<usize> {
        let best = (0..cands).map(|i| (*next + i) % cands).filter(|c| !taken.contains(c)).min_by_key(|&c| (load[c], c < avoid_below))?;
        *next = (best + 1) % cands;
        Some(best)
    }
    (0..queues)
        .map(|_| {
            let mut rs = Vec::with_capacity(threads);
            if let Some(c) = pick(&p.load, primaries, &mut p.next_primary, &rs, 0) {
                p.load[c] += 1;
                rs.push(c);
            }
            while rs.len() < threads {
                let Some(c) = pick(&p.load, r, &mut p.next_secondary, &rs, primaries) else { break };
                p.load[c] += 1;
                rs.push(c);
            }
            rs
        })
        .collect()
}

struct Hosted {
    t: Option<Tenancy>,
    on_end: Option<Box<dyn FnOnce() + Send>>,
    buf_base: u16,
    depth: u16,
}

/// A reactor's own state, on its thread.
pub struct Reactor {
    handle: Arc<Handle>,
    hosted: Slab<Hosted>,
}

impl Reactor {
    fn attach(&mut self, a: Attach) {
        let key = self.hosted.vacant_key();
        let Attach { buf_base, depth, build, on_end } = a;
        if key >= MAX_TENANCIES {
            log::error!("reactor {}: {MAX_TENANCIES} tenancies already; refusing another", self.handle.idx);
            release(self.handle.idx, buf_base, depth);
            on_end();
            return;
        }
        let slot = UblkSharedSlot { file_slot: key as u32, buf_base, key: key as u16 };
        match build(slot) {
            Some(t) => {
                t.run_ops();
                self.hosted.insert(Hosted { t: Some(t), on_end: Some(on_end), buf_base, depth });
                self.handle.hosted.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                // The key stays taken (see `remove`): a failed setup may
                // have left its buffer group or file slot in the ring.
                self.hosted.insert(Hosted { t: None, on_end: None, buf_base, depth });
                release(self.handle.idx, buf_base, depth);
                on_end();
            }
        }
    }

    /// A tenancy stopped serving: drop it, give its range back, report it.
    /// After an unclean end its key stays taken: its fetch buffer group may
    /// still be the ring's (libublk keeps a group it could not remove), and
    /// a CQE of its may still come.
    fn remove(&mut self, key: usize, clean: bool) {
        let Some(h) = self.hosted.get_mut(key) else { return };
        let t = h.t.take();
        let on_end = h.on_end.take();
        let (base, depth) = (h.buf_base, h.depth);
        if clean {
            self.hosted.remove(key);
        } else {
            log::warn!("reactor {}: tenancy {key} ended unclean; its slot stays reserved", self.handle.idx);
        }
        if let Some(mut t) = t {
            t.end(clean);
            drop(t);
        }
        release(self.handle.idx, base, depth);
        if let Some(f) = on_end {
            f();
        }
        self.handle.hosted.store(self.hosted.iter().filter(|(_, h)| h.t.is_some()).count(), Ordering::Relaxed);
    }

    fn drain_mail(&mut self) {
        let jobs: Vec<Job> = std::mem::take(&mut *self.handle.mail.lock().unwrap_or_else(PoisonError::into_inner));
        for j in jobs {
            j(self);
        }
    }
}

fn release(r: usize, base: u16, depth: u16) {
    if let Some(p) = POOL.get().and_then(|p| p.as_ref()) {
        p.release_range(r, base, depth);
    }
}

fn setup_ring(efd: i32) -> Result<(), String> {
    libublk::io::ublk_init_task_ring(|cell| {
        let ring = io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .setup_cqsize(CQ_ENTRIES)
            .setup_coop_taskrun()
            .build(SQ_ENTRIES)
            .map_err(libublk::UblkError::IOError)?;
        cell.set(std::cell::RefCell::new(ring)).map_err(|_| libublk::UblkError::OtherError(-libc::EEXIST))
    })
    .map_err(|e| format!("ring: {e}"))?;
    libublk::io::with_task_io_ring_mut(|r| {
        let s = r.submitter();
        s.register_files_sparse(MAX_TENANCIES as u32).map_err(|e| format!("sparse file table: {e}"))?;
        s.register_buffers_sparse(BUF_SLOTS).map_err(|e| format!("sparse buffer table: {e}"))?;
        Ok::<(), String>(())
    })?;
    arm_mailbox(efd)
}

fn arm_mailbox(efd: i32) -> Result<(), String> {
    let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(efd), libc::POLLIN as u32).multi(true).build().user_data(MAILBOX_UD);
    libublk::io::with_task_io_ring_mut(|r| unsafe { r.submission().push(&sqe) }).map_err(|_| "mailbox poll: SQ full".to_string())
}

fn run(handle: Arc<Handle>, ready: Arc<(Mutex<usize>, Condvar)>) {
    let tid = libublk::ctrl::UblkCtrl::init_queue_thread();
    THIS_REACTOR.with(|r| r.set(Some(handle.idx)));
    let ok = setup_ring(handle.efd);
    match &ok {
        Ok(()) => handle.tid.store(tid, Ordering::Release),
        Err(e) => log::error!("reactor {}: {e}", handle.idx),
    }
    {
        let (m, cv) = &*ready;
        *m.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        cv.notify_all();
    }
    if ok.is_err() {
        return;
    }
    let mut re = Reactor { handle: handle.clone(), hosted: Slab::with_capacity(64) };
    let mut cqes: Vec<io_uring::cqueue::Entry> = Vec::with_capacity(4096);
    let mut leaving: Vec<(usize, bool)> = Vec::new();
    let timeout = io_uring::types::Timespec::new().sec(20);
    let spin_wait = batchq::spin_wait_from_env();
    let mut mail = true;
    loop {
        if mail {
            mail = false;
            let mut v = 0u64;
            unsafe { libc::read(handle.efd, &mut v as *mut u64 as *mut libc::c_void, 8) };
            re.drain_mail();
        }
        let (mut spin, mut wedge) = (false, None::<Duration>);
        for (k, h) in re.hosted.iter_mut() {
            let Some(t) = h.t.as_mut() else { continue };
            match t.before_wait() {
                Next::Serve { spin: s, wedge: w } => {
                    spin |= s;
                    if w.is_some() {
                        wedge = wedge.max(w);
                    }
                }
                Next::Leave { clean } => leaving.push((k, clean)),
            }
        }
        for (k, clean) in leaving.drain(..) {
            re.remove(k, clean);
        }
        if let Some(d) = wedge {
            log::warn!("reactor {}: stalled for {d:?} (fault injection)", handle.idx);
            std::thread::sleep(d);
        }
        if !spin {
            for (_, h) in re.hosted.iter() {
                if let Some(t) = &h.t {
                    t.beat(true);
                }
            }
        }
        handle.turns.fetch_add(1, Ordering::Relaxed);
        if spin {
            handle.spins.fetch_add(1, Ordering::Relaxed);
        }
        let t0 = Instant::now();
        let polled = if spin { batchq::spin_poll(&spin_wait, &timeout) } else { batchq::poll(1, &timeout) };
        if !spin && !re.hosted.is_empty() {
            crate::napi::observe_wait(t0.elapsed());
        }
        for (_, h) in re.hosted.iter() {
            if let Some(t) = &h.t {
                t.beat(false);
            }
        }
        if let Err(e) = polled {
            // The ring itself failed: nothing on it can be served any more.
            log::error!("reactor {}: event loop failed: {e}; aborting", handle.idx);
            std::process::abort();
        }
        batchq::take_cqes(&mut cqes);
        let hosted = &mut re.hosted;
        let (unclaimed, _) = batchq::route_cqes(
            &cqes,
            None,
            |k, c| hosted.get_mut(k as usize).and_then(|h| h.t.as_mut()).map(|t| t.on_cqe(c)),
            |c| {
                if c.user_data() != MAILBOX_UD {
                    return false;
                }
                mail = true;
                if !io_uring::cqueue::more(c.flags()) {
                    if let Err(e) = arm_mailbox(handle.efd) {
                        log::error!("reactor {}: {e}", handle.idx);
                    }
                }
                true
            },
        );
        if unclaimed > 0 {
            log::debug!("reactor {}: {unclaimed} batch CQEs for no tenancy", handle.idx);
        }
        for (k, h) in re.hosted.iter_mut() {
            if let Some(t) = h.t.as_mut() {
                if t.after_wait().is_err() {
                    leaving.push((k, false));
                }
            }
        }
        for (k, clean) in leaving.drain(..) {
            re.remove(k, clean);
        }
        let (napi, regs) = crate::napi::state();
        handle.napi.store(napi.map_or(0, |u| u + 1), Ordering::Relaxed);
        handle.napi_regs.store(regs, Ordering::Relaxed);
    }
}

/// One line of pool stats (reactors: tenancies, turns and spinning turns
/// per second since `last`, NAPI budget).
pub fn stats_line(last: &mut Vec<(u64, u64)>, secs: f64) -> Option<String> {
    let p = POOL.get()?.as_ref()?;
    last.resize(p.reactors.len(), (0, 0));
    let mut parts = Vec::with_capacity(p.reactors.len());
    for (i, h) in p.reactors.iter().enumerate() {
        let (t, s) = (h.turns.load(Ordering::Relaxed), h.spins.load(Ordering::Relaxed));
        let (dt, ds) = (t - last[i].0, s - last[i].1);
        last[i] = (t, s);
        let napi = match h.napi.load(Ordering::Relaxed) {
            0 => "-".to_string(),
            n => format!("{}", n - 1),
        };
        parts.push(format!("r{i}:{}t {:.0}/s spin{:.0}% napi{napi}", h.hosted.load(Ordering::Relaxed), dt as f64 / secs, if dt > 0 { ds as f64 * 100.0 / dt as f64 } else { 0.0 }));
    }
    Some(format!("reactors [{}]", parts.join(" ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A queue's tenancies are on distinct reactors (the watchdog moves a
    /// wedged primary to another reactor, and the queue's buffer range is
    /// per reactor), its first primary on one of the primary reactors, and
    /// the rest spread over all of them.
    #[test]
    fn placement_spreads_a_queue_over_distinct_reactors() {
        let st = |r: usize| Placement { next_primary: 0, next_secondary: 0, load: vec![0; r], chunks: vec![0; r] };
        let mut s = st(8);
        let p = place_with(&mut s, 4, 2, 4);
        assert_eq!(p.len(), 2);
        for q in &p {
            assert_eq!(q.len(), 4);
            let mut d = q.clone();
            d.sort();
            d.dedup();
            assert_eq!(d.len(), 4, "distinct: {q:?}");
            assert!(q[0] < 4, "primary on a primary reactor: {q:?}");
        }
        assert_ne!(p[0][0], p[1][0], "queues' primaries rotate");
        let mut all: Vec<usize> = p.concat();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 8, "one volume's 2 x 4 tenancies on all 8 reactors: {p:?}");
        // Eight volumes x two queues over 8 reactors, primaries on 4: every
        // primary reactor gets 4 primaries, and the 64 tenancies spread within two (primaries are pinned to half the reactors).
        let mut s = st(8);
        let mut prim = [0; 8];
        for _ in 0..8 {
            for q in place_with(&mut s, 4, 2, 4) {
                prim[q[0]] += 1;
            }
        }
        assert_eq!(prim, [4, 4, 4, 4, 0, 0, 0, 0]);
        assert!(s.load.iter().max().unwrap() - s.load.iter().min().unwrap() <= 2, "balanced: {:?}", s.load);
        assert_eq!(s.load.iter().sum::<usize>(), 64);
        // Distinct even when one reactor is far less loaded than the rest.
        let mut s = st(4);
        s.load = vec![0, 50, 50, 50];
        let q = &place_with(&mut s, 1, 1, 4)[0];
        let mut d = q.clone();
        d.sort();
        d.dedup();
        assert_eq!(d.len(), 4, "{q:?}");
        // More threads than reactors: capped.
        let mut s = st(2);
        assert_eq!(place_with(&mut s, 1, 1, 4)[0].len(), 2);
    }

    /// Buffer ranges: the same range on every reactor of a queue, never
    /// overlapping another queue's on any shared reactor, reusable once
    /// released.
    #[test]
    fn buffer_ranges_are_free_on_every_reactor_of_the_queue() {
        assert_eq!(alloc_chunks(0, 256), Some((0, 1)));
        assert_eq!(alloc_chunks(0b1, 256), Some((1, 0b10)));
        assert_eq!(alloc_chunks(0b101, 512), Some((3, 0b11000)), "a run of two chunks");
        assert_eq!(alloc_chunks(0, 64), Some((0, 1)), "smaller depths take a whole chunk");
        assert_eq!(alloc_chunks(u64::MAX, 256), None);
        assert_eq!(alloc_chunks(0, 16384), Some((0, u64::MAX)));
        assert_eq!(alloc_chunks(0, 65535), None);
        let mut c = vec![0u64; 4];
        assert_eq!(alloc_range_in(&mut c, &[0, 1], 256), Some(0));
        assert_eq!(alloc_range_in(&mut c, &[2, 3], 256), Some(0), "disjoint reactors may reuse the range");
        assert_eq!(alloc_range_in(&mut c, &[1, 2], 256), Some(256), "chunk 0 is in use on reactors 1 and 2");
        assert_eq!(c, vec![0b1, 0b11, 0b11, 0b1], "taken on every reactor of the queue");
        c[1] &= !run_mask(0, 1);
        assert_eq!(alloc_range_in(&mut c, &[0, 1], 256), Some(512), "chunk 0 still used on reactor 0, chunk 1 on reactor 1");
    }

    #[test]
    fn default_sizing() {
        assert_eq!(default_reactors(16), 8);
        assert_eq!(default_reactors(4), 4);
        assert_eq!(default_reactors(64), 8);
        assert_eq!(default_primaries(8), 4);
        assert_eq!(default_primaries(1), 1);
    }
}
