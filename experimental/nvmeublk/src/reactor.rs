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
//! Executor wakeups and ublk CQEs select runnable tenancies. Requests still
//! owned and warm lanes stay active; idle tenancies are visited only when
//! woken or in a 250 ms watchdog/credit sweep. All selected queues dispatch
//! before each shared engine runs. A sleeping executor ticker retains the
//! wake registration, and external wakes interrupt the ring via eventfd.
//! NVMEUBLK_RUNNABLE=0 restores full scans for measurement.
//!
//! Ring layout: a sparse file table of MAX_TENANCIES slots (tenancy key =
//! file slot = ring key) and a sparse buffer table of 16384 slots handed out
//! in 128-slot chunks. With batch I/O and AUTO_BUF_REG the driver registers
//! a request's pages at the index its tag's last PREP/COMMIT named, in
//! whichever ring fetches it, so a queue's range must be the same on every
//! reactor that hosts one of its tenancies: ranges are allocated here, by
//! the pool, free on all of them.
//!
//! Watchdog: tenancies refer to their reactor's shared heartbeat, including
//! when idle. The reactor stamps it once per turn and marks deliberate ring
//! waits. A wedge therefore stops every hosted queue's heartbeat without a
//! hot-path scan. Requests already fetched still wait for the stalled ring:
//! their registered pages cannot be transferred to a different reactor.
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
const CHUNK: u32 = 128;
const CHUNKS: usize = (BUF_SLOTS / CHUNK) as usize;
/// SQ and CQ sizes of a reactor ring (COOP rings cannot be resized).
const SQ_ENTRIES: u32 = 4096;
const CQ_ENTRIES: u32 = 32768;
/// user_data of the mailbox's multishot poll: no Target bit, and a slab key
/// no future has.
const MAILBOX_UD: u64 = 0x3fff_ffff_ffff_0000;

/// Whether the pool serves by default (NVMEUBLK_REACTORS unset); "auto"
/// asks for it with the default size.
const POOL_BY_DEFAULT: bool = true;

/// Whether the pool will serve (NVMEUBLK_REACTORS unset or not 0), without
/// starting it.
pub fn pool_wanted() -> bool {
    match std::env::var("NVMEUBLK_REACTORS").ok().as_deref() {
        Some(v) if v != "auto" => v.parse::<usize>().unwrap_or(0) > 0,
        Some(_) => true,
        None => POOL_BY_DEFAULT,
    }
}

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
    beat: Arc<batchq::Beat>,
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
    /// New volumes admitted (`admit`) whose queues have not reserved their
    /// ranges yet, by admission id.
    admitted: Mutex<(u64, Vec<(u64, Demand)>)>,
}

/// What one pooled device takes from the buffer tables: `queues` ranges of
/// `depth` slots, each on `threads` reactors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Demand {
    pub queues: u16,
    pub threads: u16,
    pub depth: u16,
    pub adaptive: bool,
}

/// Why `Pool::admit` refused a new volume.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// The tables have no room for it as they are.
    Full,
    /// It fits now, but only in room that volumes ahead of it need: recorded
    /// volumes still being recovered (their count), or new volumes admitted
    /// but not placed yet.
    Promised(usize),
}

/// A new volume's admission, held until its queues have reserved their
/// ranges (or its start failed): it keeps the room from the next admission.
pub struct Admitted {
    pool: &'static Pool,
    id: u64,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.pool.admitted.lock().unwrap_or_else(PoisonError::into_inner).1.retain(|(id, _)| *id != self.id);
    }
}

#[derive(Clone)]
struct Placement {
    next_primary: usize,
    next_secondary: usize,
    /// Tenancies placed per reactor and not yet released.
    load: Vec<usize>,
    /// Per reactor: which 128-slot chunks of its buffer table are in use.
    chunks: Vec<u128>,
}

static POOL: OnceLock<Option<Pool>> = OnceLock::new();

/// The pool's shape (reactors, primary reactors) as `pool()` would start it,
/// without starting it; None when the pool is off (NVMEUBLK_REACTORS=0).
pub fn planned() -> Option<(usize, usize)> {
    let ncpu = (unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) }).max(1) as usize;
    let r = match std::env::var("NVMEUBLK_REACTORS").ok().as_deref() {
        Some("auto") => default_reactors(ncpu),
        Some(v) => v.parse().unwrap_or(0),
        None if POOL_BY_DEFAULT => default_reactors(ncpu),
        None => 0,
    };
    if r == 0 {
        return None;
    }
    let r = r.min(64);
    Some((r, (crate::env_u64("NVMEUBLK_REACTOR_PRIMARIES", default_primaries(r) as u64) as usize).clamp(1, r)))
}

/// The pool, started on first use; None when NVMEUBLK_REACTORS=0 or no
/// reactor could be started (the per-volume threads serve then).
pub fn pool() -> Option<&'static Pool> {
    POOL.get_or_init(|| {
        let Some((r, primaries)) = planned() else {
            log::info!("reactor pool off (NVMEUBLK_REACTORS=0): per-volume queue threads");
            return None;
        };
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
                beat: Arc::new(batchq::Beat::new()),
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
        Ok(Pool { place: Mutex::new(Placement { next_primary: 0, next_secondary: 0, load: vec![0; r], chunks: vec![0; r] }), reactors, primaries, admitted: Mutex::new((0, Vec::new())) })
    }

    pub fn primaries(&self) -> usize { self.primaries }

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
    pub fn reserve(&self, queues: u16, threads: u16, depth: u16, adaptive: bool) -> Option<Reservation> {
        let mut p = self.place.lock().unwrap_or_else(PoisonError::into_inner);
        reserve_in(&mut p, self.primaries, queues, threads, depth, adaptive)
    }

    /// Whether `reserve` would succeed now, without reserving anything.
    pub fn has_room(&self, queues: u16, threads: u16, depth: u16, adaptive: bool) -> bool {
        let mut p = self.place.lock().unwrap_or_else(PoisonError::into_inner).clone();
        reserve_in(&mut p, self.primaries, queues, threads, depth, adaptive).is_some()
    }

    /// Admit a new volume of demand `new`: it must fit after everything that
    /// is promised room but has not reserved it. `recovering` are the
    /// recorded volumes still being recovered: a daemon restart empties the
    /// tables, and each recorded device reserves its ranges again only when
    /// its recovery reaches its queues, so until then the tables look
    /// emptier than they are, and a new volume taking that room would leave
    /// a recorded one (whose I/O the kernel holds) with no place. New
    /// volumes admitted but not placed yet count the same way, so two
    /// attaches cannot both be admitted into the last free place. A
    /// recovering device that has already reserved is counted twice for a
    /// moment; that errs towards refusing, and the caller retries.
    pub fn admit(&'static self, recovering: &[Demand], new: Demand) -> Result<Admitted, Refused> {
        let mut admitted = self.admitted.lock().unwrap_or_else(PoisonError::into_inner);
        let place = self.place.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let ahead: Vec<Demand> = recovering.iter().copied().chain(admitted.1.iter().map(|(_, d)| *d)).collect();
        admit_in(&place, self.primaries, &ahead, new).map_err(|r| match r {
            Refused::Promised(_) => Refused::Promised(recovering.len()),
            full => full,
        })?;
        admitted.0 += 1;
        let id = admitted.0;
        admitted.1.push((id, new));
        Ok(Admitted { pool: self, id })
    }

    /// Volumes of this layout the pool's buffer tables hold when empty.
    pub fn capacity(&self, queues: u16, threads: u16, depth: u16, adaptive: bool) -> usize {
        capacity(self.reactors.len(), self.primaries, queues, threads, depth, adaptive)
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

fn run_mask(first: usize, n: usize) -> u128 {
    if n >= 128 { u128::MAX } else { ((1u128 << n) - 1) << first }
}

/// `Pool::alloc_range` on explicit per-reactor chunk maps.
fn alloc_range_in(chunks: &mut [u128], rs: &[usize], depth: u16) -> Option<u16> {
    let used = rs.iter().fold(0u128, |a, &r| a | chunks[r]);
    let (first, mask) = alloc_chunks(used, depth)?;
    for &r in rs {
        chunks[r] |= mask;
    }
    Some((first * CHUNK as usize) as u16)
}

/// First run of chunks free in `used` for `depth` slots: (first chunk, mask).
fn alloc_chunks(used: u128, depth: u16) -> Option<(usize, u128)> {
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

/// Every normal queue can use reactors 0 and 1 for bulk writes, and has a
/// read primary plus a backup outside the primary set. This preserves live
/// work during a global primary wedge even while writes are consolidated.
fn place_adaptive(p: &mut Placement, primaries: usize, queues: u16, threads: u16) -> Vec<Vec<usize>> {
    let r = p.load.len();
    let n = (threads as usize).clamp(1, r);
    let warm = if n >= 4 || n == 1 { primaries.clamp(1, r) } else { 1 };
    (0..queues).map(|_| {
        let primary = if n > 1 { 0 } else { p.next_primary % warm };
        p.next_primary = p.next_primary.wrapping_add(1);
        let mut rs = vec![primary];
        // Keep a read backup early in the driver's fetch rotation. Putting
        // it after three warm-primary reactors can leave it with no owned
        // work at moderate depth when those primaries all wedge together.
        if n >= 3 {
            let backup = (0..r).filter(|&i| i != primary)
                .min_by_key(|&i| (i < warm.max(2), p.load[i], (i + r - p.next_secondary % r) % r)).unwrap();
            p.next_secondary = (backup + 1) % r;
            rs.push(backup);
        }
        for i in 0..r.min(2) {
            if rs.len() < n && !rs.contains(&i) { rs.push(i); }
        }
        while rs.len() < n {
            let preferred = |i: usize| i < 4;
            let i = (0..r).filter(|i| !rs.contains(i))
                .min_by_key(|&i| (!preferred(i), p.load[i], (i + r - p.next_secondary % r) % r)).unwrap();
            p.next_secondary = (i + 1) % r;
            rs.push(i);
        }
        for &i in &rs { p.load[i] += 1; }
        rs
    }).collect()
}

pub struct Reservation {
    pub placement: Vec<Vec<usize>>,
    pub ranges: Vec<u16>,
    pub adaptive: bool,
}

/// Volumes of one layout (`queues` x `threads` tenancies x `depth` tags) that
/// an empty pool of `reactors` holds: every queue needs `depth` buffer slots
/// at the same index on each reactor hosting one of its tenancies, and a
/// ring's table is the kernel's maximum (BUF_SLOTS). Counted by reserving on
/// a scratch placement, so it is what `reserve` delivers, whatever the shape.
/// It holds for volumes of one layout; a node mixing layouts can refuse an
/// attach earlier, when no range of the new size is free on enough reactors.
pub fn capacity(reactors: usize, primaries: usize, queues: u16, threads: u16, depth: u16, adaptive: bool) -> usize {
    let mut p = Placement { next_primary: 0, next_secondary: 0, load: vec![0; reactors], chunks: vec![0; reactors] };
    let mut n = 0;
    while reserve_in(&mut p, primaries, queues, threads, depth, adaptive).is_some() {
        n += 1;
    }
    n
}

/// `Pool::admit` on an explicit placement: `new` must fit as the tables are,
/// and still fit once every demand in `ahead` has its place (one of those
/// that does not fit itself takes nothing).
fn admit_in(place: &Placement, primaries: usize, ahead: &[Demand], new: Demand) -> Result<(), Refused> {
    let fits = |p: &mut Placement, d: Demand| reserve_in(p, primaries, d.queues, d.threads, d.depth, d.adaptive).is_some();
    if !fits(&mut place.clone(), new) {
        return Err(Refused::Full);
    }
    let mut p = place.clone();
    for d in ahead {
        fits(&mut p, *d);
    }
    if fits(&mut p, new) { Ok(()) } else { Err(Refused::Promised(ahead.len())) }
}

/// Placement and buffer reservations are one transaction. A dense node can
/// use free slots outside the preferred pair with fixed admission instead
/// of losing the capacity of the old balanced pool. Failure changes nothing.
fn reserve_in(p: &mut Placement, primaries: usize, queues: u16, threads: u16, depth: u16, adaptive: bool) -> Option<Reservation> {
    let saved = p.clone();
    let placement = if adaptive { place_adaptive(p, primaries, queues, threads) } else { place_with(p, primaries, queues, threads) };
    let ranges: Option<Vec<_>> = placement.iter().map(|rs| alloc_range_in(&mut p.chunks, rs, depth)).collect();
    if let Some(ranges) = ranges { return Some(Reservation { placement, ranges, adaptive }); }
    *p = saved.clone();
    if !adaptive { return None; }
    let r = p.load.len();
    let n = (threads as usize).clamp(1, r);
    let warm = primaries.clamp(1, r);
    let chunks = chunks_for(depth);
    let mut placement = Vec::new();
    let mut ranges = Vec::new();
    for _ in 0..queues {
        let choice = (0..=CHUNKS.saturating_sub(chunks)).find_map(|first| {
            if chunks > CHUNKS { return None; }
            let mask = run_mask(first, chunks);
            let free: Vec<_> = (0..r).filter(|&i| p.chunks[i] & mask == 0).collect();
            if free.len() < n { return None; }
            let primary = free.iter().copied().filter(|&i| i < warm).min_by_key(|&i| p.load[i])?;
            let mut rs = vec![primary];
            if n > 1 && r > warm {
                rs.push(free.iter().copied().filter(|&i| i >= warm).min_by_key(|&i| p.load[i])?);
            }
            while rs.len() < n {
                rs.push(free.iter().copied().filter(|i| !rs.contains(i)).min_by_key(|&i| p.load[i])?);
            }
            Some((first, mask, rs))
        });
        let Some((first, mask, rs)) = choice else { *p = saved; return None; };
        for &i in &rs { p.chunks[i] |= mask; p.load[i] += 1; }
        ranges.push((first as u32 * CHUNK) as u16);
        placement.push(rs);
    }
    Some(Reservation { placement, ranges, adaptive: false })
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
    ready: Arc<crate::ready::Ready>,
    runnable: bool,
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
            Some(mut t) => {
                t.bind_heartbeat(self.handle.beat.clone());
                t.run_ops();
                if self.runnable { t.watch_ready(self.ready.waker(key)); }
                self.ready.mark(key);
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
    let runnable = crate::env_u64("NVMEUBLK_RUNNABLE", 1) != 0;
    let mut re = Reactor { handle: handle.clone(), hosted: Slab::with_capacity(64), ready: crate::ready::Ready::new(handle.efd), runnable };
    let mut active = Vec::with_capacity(64);
    let mut dispatch = Vec::with_capacity(64);
    let mut service = Vec::with_capacity(64);
    let mut woken = Vec::with_capacity(64);
    let mut engine_turn = 0u64;
    let mut sweep = Instant::now();
    let mut cqes: Vec<io_uring::cqueue::Entry> = Vec::with_capacity(4096);
    let mut leaving: Vec<(usize, bool)> = Vec::new();
    let coalesce = crate::env_u64("NVMEUBLK_COALESCE_TURNS", 1) != 0;
    let timeout = io_uring::types::Timespec::new().nsec(250_000_000);
    let spin_wait = batchq::spin_wait_from_env();
    let mut mail = true;
    loop {
        handle.beat.stamp(false);
        if mail {
            mail = false;
            let mut v = 0u64;
            unsafe { libc::read(handle.efd, &mut v as *mut u64 as *mut libc::c_void, 8) };
            re.drain_mail();
        }
        active.clear();
        std::mem::swap(&mut active, &mut service);
        woken.clear();
        re.ready.take(&mut woken);
        // An executor can wake from outside the ring. Drive that work before
        // waiting; it may be the producer of the SQE we would wait for.
        engine_turn = engine_turn.wrapping_add(1);
        for &k in &woken {
            if let Some(t) = re.hosted.get(k).and_then(|h| h.t.as_ref()) { t.run_ops_once(engine_turn); }
        }
        active.extend_from_slice(&woken);
        if !runnable || sweep.elapsed() >= Duration::from_millis(250) {
            active.extend(re.hosted.iter().map(|(k, _)| k));
            sweep = Instant::now();
        }
        active.sort_unstable(); active.dedup();
        let (mut spin, mut wedge) = (false, None::<Duration>);
        for &k in &active {
            let Some(h) = re.hosted.get_mut(k) else { continue };
            let Some(t) = h.t.as_mut() else { continue };
            match t.before_wait() {
                Next::Serve { spin: s, wedge: w } => {
                    spin |= s;
                    let keep = s || t.needs_service();
                    if keep { service.push(k); }
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
        handle.turns.fetch_add(1, Ordering::Relaxed);
        engine_turn = engine_turn.wrapping_add(1);
        if spin {
            handle.spins.fetch_add(1, Ordering::Relaxed);
        }
        dispatch.clear();
        dispatch.extend_from_slice(&service);
        if !runnable { dispatch.extend_from_slice(&active); }
        let can_sleep = re.ready.sleeping(!spin);
        handle.beat.stamp(!spin && can_sleep);
        let t0 = Instant::now();
        let polled = if spin || !can_sleep { batchq::spin_poll(&spin_wait, &timeout) } else { batchq::poll(1, &timeout) };
        re.ready.sleeping(false);
        handle.beat.stamp(false);
        if !spin && !re.hosted.is_empty() {
            crate::napi::observe_wait(t0.elapsed());
        }
        if let Err(e) = polled {
            // The ring itself failed: nothing on it can be served any more.
            log::error!("reactor {}: event loop failed: {e}; aborting", handle.idx);
            std::process::abort();
        }
        batchq::take_cqes(&mut cqes);
        let hosted = &mut re.hosted;
        let ready = &re.ready;
        let (unclaimed, _) = batchq::route_cqes(
            &cqes,
            None,
            |k, c| hosted.get_mut(k as usize).and_then(|h| h.t.as_mut()).map(|t| { ready.mark(k as usize); t.on_cqe(c) }),
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
        re.ready.take(&mut dispatch);
        dispatch.sort_unstable(); dispatch.dedup();
        for &k in &dispatch {
            let Some(h) = re.hosted.get_mut(k) else { continue };
            if let Some(t) = h.t.as_mut() {
                if t.dispatch_after_wait(!coalesce).is_err() {
                    leaving.push((k, false));
                }
                service.push(k); // commit any results before the next wait
            }
        }
        if coalesce {
            for &k in &dispatch {
                if let Some(t) = re.hosted.get(k).and_then(|h| h.t.as_ref()) { t.run_ops_once(engine_turn); }
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
    fn full_pool_geometry_keeps_sixteen_volumes_on_the_existing_buffer_tables() {
        let mut p = Placement { next_primary: 0, next_secondary: 0, load: vec![0; 8], chunks: vec![0; 8] };
        for _ in 0..16 { assert!(reserve_in(&mut p, 4, 8, 8, 128, true).unwrap().adaptive); }
        assert!(p.chunks.iter().all(|&c| c == u128::MAX));
        assert!(reserve_in(&mut p, 4, 8, 8, 128, true).is_none());
    }

    /// The buffer tables bound the volumes a node serves with zero copy:
    /// 16 of the full 8 x 256 layout on eight reactors, and twice as many
    /// for each halving of the depth (down to one 128-slot chunk) or of the
    /// queue count. The default layout is chosen from these (device.rs).
    #[test]
    fn capacity_is_what_the_buffer_tables_hold() {
        for (reactors, primaries, queues, threads, depth, volumes) in [
            (8, 4, 8, 4, 256, 16),
            (8, 4, 8, 4, 128, 32),
            (8, 4, 4, 4, 128, 64),
            (8, 4, 2, 4, 128, 128),
            (8, 4, 2, 4, 64, 128),
            (6, 3, 8, 4, 128, 16),
            (4, 2, 4, 4, 128, 32),
            (4, 2, 2, 4, 128, 64),
        ] {
            assert_eq!(capacity(reactors, primaries, queues, threads, depth, false), volumes, "{reactors} reactors, {queues} x {threads} x {depth}");
        }
    }

    /// After a daemon restart the tables are empty until each recorded
    /// volume's recovery reserves its ranges again. A new volume must not be
    /// admitted into room a recorded volume still needs: on a node at
    /// capacity the recorded one would find no place, and the kernel holds
    /// its I/O until it is served. (Checking only the tables as they are,
    /// as the first admission check did, admits it.)
    #[test]
    fn a_new_volume_is_not_admitted_into_room_recovering_volumes_need() {
        let d = Demand { queues: 8, threads: 4, depth: 128, adaptive: false };
        let empty = Placement { next_primary: 0, next_secondary: 0, load: vec![0; 8], chunks: vec![0; 8] };
        let cap = capacity(8, 4, d.queues, d.threads, d.depth, d.adaptive);
        assert_eq!(cap, 32);
        // The restart window: nothing reserved, `cap` recorded volumes to come.
        assert!(reserve_in(&mut empty.clone(), 4, d.queues, d.threads, d.depth, d.adaptive).is_some(), "the tables alone say there is room");
        assert_eq!(admit_in(&empty, 4, &vec![d; cap], d), Err(Refused::Promised(cap)));
        // One recorded volume fewer leaves exactly one place.
        assert_eq!(admit_in(&empty, 4, &vec![d; cap - 1], d), Ok(()));
        // Half recovered (reserved for real), half still to come: the same answer.
        let mut half = empty.clone();
        for _ in 0..cap / 2 {
            reserve_in(&mut half, 4, d.queues, d.threads, d.depth, d.adaptive).unwrap();
        }
        assert_eq!(admit_in(&half, 4, &vec![d; cap / 2], d), Err(Refused::Promised(cap / 2)));
        assert_eq!(admit_in(&half, 4, &vec![d; cap / 2 - 1], d), Ok(()));
        // Full as they are: refused as full, whatever is ahead.
        let mut full = half.clone();
        for _ in 0..cap / 2 {
            reserve_in(&mut full, 4, d.queues, d.threads, d.depth, d.adaptive).unwrap();
        }
        assert_eq!(admit_in(&full, 4, &[], d), Err(Refused::Full));
        // A smaller volume still fits beside a recorded set that leaves room for it.
        let small = Demand { queues: 2, threads: 4, depth: 128, adaptive: false };
        assert_eq!(admit_in(&empty, 4, &vec![d; cap - 1], small), Ok(()));
        assert_eq!(admit_in(&empty, 4, &vec![d; cap], small), Err(Refused::Promised(cap)));
    }

    /// Volumes come and go in any order, and a node at one layout still
    /// takes `capacity` of them: detaches leave no unusable holes.
    #[test]
    fn a_uniform_layout_refills_to_capacity_under_churn() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut rnd = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };
        for (q, t, d) in [(8u16, 4u16, 256u16), (8, 4, 128), (4, 4, 128), (2, 4, 128)] {
            let cap = capacity(8, 4, q, t, d, false);
            let mut p = Placement { next_primary: 0, next_secondary: 0, load: vec![0; 8], chunks: vec![0; 8] };
            let mut live: Vec<Reservation> = Vec::new();
            for step in 0..20_000 {
                if live.is_empty() || (live.len() < cap && rnd() % 100 < 55) {
                    let r = reserve_in(&mut p, 4, q, t, d, false);
                    assert!(r.is_some(), "{q} x {t} x {d}: refused volume {} of {cap} at step {step}", live.len() + 1);
                    live.push(r.unwrap());
                } else {
                    let v = live.swap_remove((rnd() % live.len() as u64) as usize);
                    for (rs, base) in v.placement.iter().zip(&v.ranges) {
                        let mask = run_mask(*base as usize / CHUNK as usize, chunks_for(d));
                        for &r in rs { p.chunks[r] &= !mask; p.load[r] = p.load[r].saturating_sub(1); }
                    }
                }
            }
            while live.len() < cap { live.push(reserve_in(&mut p, 4, q, t, d, false).expect("refill to capacity")); }
            assert!(reserve_in(&mut p, 4, q, t, d, false).is_none(), "{q} x {t} x {d}: took more than capacity");
        }
    }

    #[test]
    fn dense_reservations_keep_capacity_and_failed_attach_rolls_back() {
        let mut p = Placement { next_primary: 0, next_secondary: 0, load: vec![0; 8], chunks: vec![0; 8] };
        for volume in 0..16 {
            if volume == 15 {
                let before = p.clone();
                assert!(reserve_in(&mut p, 4, 9, 4, 256, true).is_none());
                assert_eq!(p.load, before.load, "partially reserved attach leaked load");
                assert_eq!(p.chunks, before.chunks, "partially reserved attach leaked buffers");
            }
            let reservation = reserve_in(&mut p, 4, 8, 4, 256, true).expect("lost balanced-pool capacity");
            assert_eq!(reservation.adaptive, volume < 8);
            assert_eq!(reservation.ranges.len(), 8);
        }
        assert!(p.chunks.iter().all(|&c| c == u128::MAX));
        assert_eq!(p.load, vec![64; 8]);
        let before = p.clone();
        assert!(reserve_in(&mut p, 4, 8, 4, 256, true).is_none());
        assert_eq!(p.load, before.load);
        assert_eq!(p.chunks, before.chunks);
        assert_eq!((p.next_primary, p.next_secondary), (before.next_primary, before.next_secondary));
    }

    #[test]
    fn adaptive_placement_keeps_bulk_peers_and_read_backups() {
        for r in [2, 4, 8] {
            for n in 2..=r {
                let mut p = Placement { next_primary: 0, next_secondary: 0, load: vec![0; r], chunks: vec![0; r] };
                let qs = place_adaptive(&mut p, default_primaries(r), 8, n as u16);
                let primaries: std::collections::BTreeSet<_> = qs.iter().map(|q| q[0]).collect();
                assert_eq!(primaries, [0].into_iter().collect(), "active peers must not also be global primaries");
                for q in qs {
                    if n >= 4 && r > default_primaries(r) { assert!(q[1] >= default_primaries(r), "read backup must be early"); }
                    assert!(q.contains(&0) && q.contains(&1), "bulk needs two live rings: {q:?}");
                    assert_eq!(q.iter().collect::<std::collections::BTreeSet<_>>().len(), n);
                    if r >= 4 { assert!(!primaries.contains(&q[1]), "read backup must be early in the fetch rotation: {q:?}"); }
                }
            }
        }
    }

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
        assert_eq!(alloc_chunks(0, 256), Some((0, 0b11)));
        assert_eq!(alloc_chunks(0b1, 256), Some((1, 0b110)));
        assert_eq!(alloc_chunks(0b101, 512), Some((3, 0b1111000)), "a run of four chunks");
        assert_eq!(alloc_chunks(0, 64), Some((0, 1)), "smaller depths take a whole chunk");
        assert_eq!(alloc_chunks(u128::MAX, 256), None);
        assert_eq!(alloc_chunks(0, 16384), Some((0, u128::MAX)));
        assert_eq!(alloc_chunks(0, 65535), None);
        let mut c = vec![0u128; 4];
        assert_eq!(alloc_range_in(&mut c, &[0, 1], 256), Some(0));
        assert_eq!(alloc_range_in(&mut c, &[2, 3], 256), Some(0), "disjoint reactors may reuse the range");
        assert_eq!(alloc_range_in(&mut c, &[1, 2], 256), Some(256), "chunk 0 is in use on reactors 1 and 2");
        assert_eq!(c, vec![0b11, 0b1111, 0b1111, 0b11], "taken on every reactor of the queue");
        c[1] &= !run_mask(0, 2);
        assert_eq!(alloc_range_in(&mut c, &[0, 1], 256), Some(512), "chunk 0 still used on reactor 0, chunk 1 on reactor 1");
    }

    /// The pool serves by default (L7 final matrix); NVMEUBLK_REACTORS=0
    /// keeps the per-volume threads.
    #[test]
    fn the_pool_is_the_default() {
        assert!(POOL_BY_DEFAULT);
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
