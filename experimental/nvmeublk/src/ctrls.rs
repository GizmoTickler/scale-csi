//! Per-path admin controllers for the io_uring engine: connect, enable,
//! identify, keep-alive and reconnect. One supervisor thread per process
//! serves every path of every device (see `Supervisor`).
//! I/O queues belong to the ublk queue threads (see qengine.rs); this layer
//! only tells them which controller to attach to and when it changed.

use crate::conn::{AdminConn, Ident, NsInfo};
use anyhow::{bail, Result};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

/// A mutex that does not poison. The supervisor takes the same locks as
/// the queue threads and the reconnect threads (cntlid, max_qsize); with
/// std's poisoning, one of those threads panicking while holding a lock
/// would make the supervisor's next lock panic too, and that one thread
/// keeps every volume's controllers alive. The data behind these locks
/// stays usable after a panic: each critical section is a single load or
/// store, or one supervisor pass over a device, which the next pass picks
/// up from whatever state the slots are in.
///
/// `lock()` keeps `Mutex::lock`'s signature, but its error type is
/// uninhabited, so `.lock().unwrap()` call sites (here and in qengine.rs)
/// compile unchanged and can no longer panic.
pub struct Lock<T>(Mutex<T>);

impl<T> Lock<T> {
    pub const fn new(v: T) -> Self {
        Lock(Mutex::new(v))
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, T>, std::convert::Infallible> {
        Ok(self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// Most admin reconnect threads running at once, process-wide. When a
/// target reboots, every path to it of every volume drops in the same
/// supervisor pass; uncapped, that pass starts one thread per path per
/// volume (200 at 100 two-path volumes), each blocking up to 3 s in connect
/// and 10 s in a read, with its stack mlocked. A due path that gets no
/// slot keeps its turn and asks again on the next pass (100 ms).
const MAX_RECONNECTS: usize = 32;

/// Most of those aimed at one address. Every path to an address fails the
/// same way while its portal is dead (each attempt blocks 3 s in connect)
/// or hung (it accepts TCP and never answers: 10 s in the first read).
/// Without this bound, the attempts of every volume's path to such a portal
/// fill the whole process-wide budget and free about one slot per pass, and
/// a path to a healthy portal whose controller just dropped waits longer
/// the more volumes the node serves: up to tens of seconds at 100-200
/// volumes, past the no-path timeout. With it, a dead or hung address holds
/// at most this many slots, whatever the number of volumes. A healthy
/// portal's attempts end within milliseconds, so it still gets this many
/// new ones every pass.
const MAX_RECONNECTS_PER_ADDR: usize = 8;

static RECONNECTS: ReconnectSlots = ReconnectSlots::new(MAX_RECONNECTS, MAX_RECONNECTS_PER_ADDR);

/// Counts the running reconnect threads against both caps.
struct ReconnectSlots {
    max: usize,
    max_per_addr: usize,
    busy: Lock<Busy>,
}

struct Busy {
    total: usize,
    /// Addresses with at least one reconnect running.
    per_addr: BTreeMap<SocketAddr, usize>,
}

/// One running reconnect's slot, freed when this is dropped: when its
/// thread ends, by return or by panic, or when the thread cannot be spawned
/// (the closure that owns it is dropped then).
struct ReconnectPermit {
    slots: &'static ReconnectSlots,
    addr: SocketAddr,
}

impl ReconnectSlots {
    const fn new(max: usize, max_per_addr: usize) -> Self {
        ReconnectSlots { max, max_per_addr, busy: Lock::new(Busy { total: 0, per_addr: BTreeMap::new() }) }
    }

    /// A slot for a reconnect to `addr`, if neither cap is reached.
    fn try_take(&'static self, addr: SocketAddr) -> Option<ReconnectPermit> {
        let mut b = self.busy.lock().unwrap();
        let at = b.per_addr.get(&addr).copied().unwrap_or(0);
        if b.total >= self.max || at >= self.max_per_addr {
            return None;
        }
        b.total += 1;
        b.per_addr.insert(addr, at + 1);
        Some(ReconnectPermit { slots: self, addr })
    }

    #[cfg(test)]
    fn running(&self) -> usize {
        self.busy.lock().unwrap().total
    }

    #[cfg(test)]
    fn running_to(&self, addr: SocketAddr) -> usize {
        self.busy.lock().unwrap().per_addr.get(&addr).copied().unwrap_or(0)
    }
}

impl Drop for ReconnectPermit {
    fn drop(&mut self) {
        let mut b = self.slots.busy.lock().unwrap();
        b.total -= 1;
        if let std::collections::btree_map::Entry::Occupied(mut e) = b.per_addr.entry(self.addr) {
            *e.get_mut() -= 1;
            if *e.get() == 0 {
                e.remove();
            }
        }
    }
}

/// A path due for a reconnect, as its device's tick found it.
#[derive(Clone, Copy, Debug)]
struct DuePath {
    path: usize,
    addr: SocketAddr,
    /// Attempts that failed since the path last had a controller: 0 for a
    /// path that just lost a healthy one (fence, keep-alive).
    failures: u32,
    /// Its device has no live controller: the volume has no path now.
    stranded: bool,
    /// Its place among its device's due paths: fewest failures first, then
    /// the device's path order, which rotates with the pass.
    nth: usize,
}

/// A due path in one pass: `rank` is its device's place in the pass order
/// (which rotates), `dev` the device's index in the pass's list.
#[derive(Clone, Copy, Debug)]
struct Due {
    rank: usize,
    dev: usize,
    p: DuePath,
}

/// Which due paths start a reconnect this pass, in this order, each while
/// its address and the process are under their caps:
/// 1. paths that just lost a healthy controller (no failed attempt yet)
///    before paths that have been failing, which are aimed at a dead or hung
///    portal more often than not;
/// 2. within each, a path of a volume that has no path left first;
/// 3. then fewer failures first (the failing paths of one dead address take
///    its slots in turn);
/// 4. then each device's first choice before any device's second, so a
///    target reboot hands its slots to as many volumes as it can;
/// 5. then the pass order, which rotates.
///
/// So a path to a healthy portal waits for a free slot, never behind the
/// attempts of every stranded path of a dead portal: those are capped per
/// address, and a slot that frees goes to it first, whatever the number of
/// volumes.
fn grant(mut due: Vec<Due>, slots: &'static ReconnectSlots) -> Vec<(Due, ReconnectPermit)> {
    due.sort_by_key(|d| (d.p.failures > 0, !d.p.stranded, d.p.failures, d.p.nth, d.rank));
    due.into_iter().filter_map(|d| slots.try_take(d.p.addr).map(|permit| (d, permit))).collect()
}

/// Reconnect thread name, as `top -H` shows it: "rc<path>-" and the last 8
/// characters of the subsystem NQN, which tell one volume's reconnects from
/// another's. Linux keeps 15 bytes; this is 12 for path < 10.
fn reconnect_thread_name(subnqn: &str, path: usize) -> String {
    let tail: String = subnqn.chars().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect();
    format!("rc{path}-{tail}")
}

pub struct CtrlPath {
    pub addr: SocketAddr,
    /// Controller id of the live admin queue, 0 while down.
    cntlid: Lock<u16>,
    /// Bumped every time the controller is lost or replaced. A queue whose
    /// I/O connection was made under an older epoch must reconnect: its
    /// queue belongs to a controller that no longer exists.
    pub epoch: AtomicU64,
    pub max_qsize: Lock<u16>,
    /// A queue thread saw this path fail under epoch N and asks for the whole
    /// controller to be torn down (0 = no request). Honoured only while the
    /// epoch is still N, so a late report cannot kill a fresh controller.
    fence_req: AtomicU64,
}

impl CtrlPath {
    pub fn cntlid(&self) -> Option<u16> {
        let c = *self.cntlid.lock().unwrap();
        (c != 0).then_some(c)
    }

    /// cntlid and the epoch it belongs to, read so that the pair is
    /// consistent: an I/O queue connected with this cntlid is valid only
    /// while the epoch is unchanged.
    pub fn snapshot(&self) -> Option<(u16, u64)> {
        let e1 = self.epoch.load(Ordering::Acquire);
        let c = self.cntlid()?;
        let e2 = self.epoch.load(Ordering::Acquire);
        (e1 == e2).then_some((c, e1))
    }

    /// Ask the supervisor to tear down this path's controller, as the kernel
    /// resets the whole controller on any queue error. Every queue's I/O
    /// connection to it then dies on the epoch bump, so the target sees the
    /// controller go away instead of one queue quietly disappearing.
    pub fn fence(&self, epoch: u64) {
        // Monotonic: a late report for an older epoch must not overwrite (and
        // so cancel) a pending request for the current one.
        self.fence_req.fetch_max(epoch, Ordering::AcqRel);
    }
}

/// Same namespace, not just the same size: compare the identifiers the
/// target reports, and fall back to geometry only when it reports none.
pub fn same_namespace(a: &NsInfo, b: &NsInfo) -> bool {
    let geometry = a.nsze == b.nsze && a.lba_shift == b.lba_shift;
    let a_has_id = a.nguid != [0; 16] || a.eui64 != [0; 8];
    let b_has_id = b.nguid != [0; 16] || b.eui64 != [0; 8];
    if !a_has_id && !b_has_id {
        return geometry;
    }
    geometry && a.nguid == b.nguid && a.eui64 == b.eui64
}

pub struct Ctrls {
    pub paths: Vec<Arc<CtrlPath>>,
    pub id: Arc<Ident>,
    pub info: NsInfo,
    kato: Duration,
    stop: AtomicBool,
    /// Supervisor-owned state per path.
    slots: Lock<Vec<PathSlot>>,
}

/// What the supervisor tracks for one path.
struct PathSlot {
    admin: Option<AdminConn>,
    backoff: Duration,
    /// Reconnect attempts that failed since this path last had a controller.
    failures: u32,
    next_try: Instant,
    last_ka: Instant,
    /// Keep-alive in flight: (cid, sent at).
    ka: Option<(u16, Instant)>,
    /// A reconnect running on its own thread (it blocks: TCP connect,
    /// enable, identify); its admin connection arrives here, already
    /// validated and with the path's epoch bumped.
    /// Err(true): the path now presents a different namespace (retry in 5 s).
    connecting: Option<Receiver<Result<AdminConn, bool>>>,
}

/// One thread for all admin controllers in the process. Per-device (and
/// per-path) supervisor threads cost 4 threads and 40 wakeups/s per volume;
/// this costs one thread and 10 wakeups/s in total. Nothing on it blocks:
/// keep-alives are sent and collected without waiting, and reconnects run
/// on short-lived threads, so one dead or hung target cannot delay the
/// keep-alives of every other volume past their KATO.
struct Supervisor {
    ctrls: Lock<Vec<Weak<Ctrls>>>,
    /// Set only once the supervisor thread was actually spawned: a failed
    /// spawn must be retried by the next register(), or every device from
    /// then on would get no keep-alive and no reconnect at all.
    running: Lock<bool>,
}

static SUPERVISOR: LazyLock<Supervisor> = LazyLock::new(|| Supervisor { ctrls: Lock::new(Vec::new()), running: Lock::new(false) });

impl Supervisor {
    fn register(c: &Arc<Ctrls>) -> Result<()> {
        let sv = &*SUPERVISOR;
        let mut running = sv.running.lock().unwrap();
        if !*running {
            std::thread::Builder::new().name("nvme-supervisor".into()).spawn(|| SUPERVISOR.run()).map_err(|e| anyhow::anyhow!("spawn admin supervisor: {e}"))?;
            *running = true;
        }
        sv.ctrls.lock().unwrap().push(Arc::downgrade(c));
        Ok(())
    }

    fn run(&self) {
        let mut turn = 0usize;
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let live: Vec<Arc<Ctrls>> = {
                let mut list = self.ctrls.lock().unwrap();
                list.retain(|w| w.strong_count() > 0);
                list.iter().filter_map(Weak::upgrade).collect()
            };
            Self::pass(&live, turn, &RECONNECTS);
            turn = turn.wrapping_add(1);
        }
    }

    /// One pass over every device, in two halves. First each device's
    /// tick (adopt finished reconnects, fences, keep-alives) reports its
    /// paths due for a reconnect; then `grant` picks which of them, over
    /// every device, start one now under the caps of `slots`, and the rest
    /// keep their turn for the next pass. Devices are visited from device
    /// `turn % len` and each device's paths from path `turn + index`, so
    /// the order that breaks ties moves every pass.
    ///
    /// A panic in one device's tick or reconnect start is caught and
    /// logged: it must not end the only thread that keeps every other
    /// device's controllers alive, and as its locks do not poison, the next
    /// pass runs normally.
    fn pass(live: &[Arc<Ctrls>], turn: usize, slots: &'static ReconnectSlots) {
        let n = live.len();
        let mut due = Vec::new();
        for rank in 0..n {
            let dev = turn.wrapping_add(rank) % n;
            let c = &live[dev];
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.tick(turn.wrapping_add(dev)))) {
                Ok(paths) => due.extend(paths.into_iter().map(|p| Due { rank, dev, p })),
                Err(_) => log::error!("admin supervisor: pass for {} panicked; continuing with the other devices", c.id.subnqn),
            }
        }
        for (d, permit) in grant(due, slots) {
            let c = &live[d.dev];
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.start_reconnect(d.p.path, permit))).is_err() {
                log::error!("admin supervisor: reconnect start for {} path {} panicked", c.id.subnqn, d.p.path);
            }
        }
    }
}

impl Ctrls {
    pub fn new(addrs: Vec<SocketAddr>, id: Ident, kato: Duration) -> Result<Arc<Self>> {
        let id = Arc::new(id);
        let mut info = None;
        let paths: Vec<Arc<CtrlPath>> = addrs
            .iter()
            .map(|a| Arc::new(CtrlPath { addr: *a, cntlid: Lock::new(0), epoch: AtomicU64::new(1), max_qsize: Lock::new(128), fence_req: AtomicU64::new(0) }))
            .collect();
        let mut admins = Vec::new();
        for p in &paths {
            match Self::bring_up(p, &id, kato) {
                Ok((a, i)) => {
                    match &info {
                        None => {
                            if i.nguid == [0; 16] && i.eui64 == [0; 8] {
                                log::warn!("{}: target reports no NGUID/EUI64; paths are matched by geometry only", p.addr);
                            }
                            info = Some(i);
                            admins.push(Some(a));
                        }
                        Some(first) if same_namespace(first, &i) => admins.push(Some(a)),
                        Some(_) => {
                            log::error!("{}: presents a different namespace than the first path; refusing this path", p.addr);
                            a.shutdown();
                            Self::lose(p);
                            admins.push(None);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("{}: {e:#}", p.addr);
                    admins.push(None);
                }
            }
        }
        let Some(info) = info else { bail!("no path reachable") };
        let now = Instant::now();
        let slots = admins
            .into_iter()
            .map(|admin| {
                let failures = u32::from(admin.is_none());
                PathSlot { admin, backoff: Duration::from_millis(250), failures, next_try: now, last_ka: now, ka: None, connecting: None }
            })
            .collect();
        let me = Arc::new(Ctrls { paths, id, info, kato, stop: AtomicBool::new(false), slots: Lock::new(slots) });
        Supervisor::register(&me)?;
        Ok(me)
    }

    fn bring_up(p: &CtrlPath, id: &Ident, kato: Duration) -> Result<(AdminConn, NsInfo)> {
        let mut a = AdminConn::connect(p.addr, id, kato.as_millis() as u32)?;
        let (info, mqes) = a.enable_and_identify()?;
        *p.cntlid.lock().unwrap() = a.cntlid;
        *p.max_qsize.lock().unwrap() = mqes.saturating_add(1).clamp(2, 128);
        log::info!("path {} admin up: cntlid {}", p.addr, a.cntlid);
        Ok((a, info))
    }

    fn lose(p: &CtrlPath) {
        *p.cntlid.lock().unwrap() = 0;
        p.epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// One supervisor pass over this device's paths, except for starting
    /// reconnects: returns the paths due for one, for the pass to choose
    /// from over every device (`grant`). `rot` rotates the path order.
    fn tick(self: &Arc<Self>, rot: usize) -> Vec<DuePath> {
        let mut slots = self.slots.lock().unwrap();
        if self.stop.load(Ordering::Acquire) {
            for slot in slots.iter_mut() {
                if let Some(a) = slot.admin.take() {
                    a.shutdown();
                }
                // A reconnect that finishes after stop must not leave its
                // new controller's socket open until the device is dropped.
                if let Some(rx) = slot.connecting.as_ref() {
                    match rx.try_recv() {
                        Ok(Ok(a)) => {
                            a.shutdown();
                            slot.connecting = None;
                        }
                        Ok(Err(_)) | Err(TryRecvError::Disconnected) => slot.connecting = None,
                        Err(TryRecvError::Empty) => {}
                    }
                }
            }
            return Vec::new();
        }
        let mut due: Vec<usize> = (0..slots.len()).filter(|&i| self.tick_path(i, &mut slots[i])).collect();
        let n = slots.len();
        let first = rot % n.max(1);
        due.sort_by_key(|&i| (slots[i].failures, (i + n - first) % n));
        let stranded = slots.iter().all(|s| s.admin.is_none());
        due.into_iter().enumerate().map(|(nth, i)| DuePath { path: i, addr: self.paths[i].addr, failures: slots[i].failures, stranded, nth }).collect()
    }

    /// Supervisor pass over one path. True when it is due for a reconnect.
    fn tick_path(self: &Arc<Self>, i: usize, s: &mut PathSlot) -> bool {
        let p = self.paths[i].clone();
        let now = Instant::now();
        // Adopt a finished reconnect before looking at fence requests: a queue
        // may already have raised one against the new controller's epoch.
        if s.admin.is_none() {
            if let Some(rx) = s.connecting.as_ref() {
                match rx.try_recv() {
                    Ok(Ok(a)) => {
                        s.connecting = None;
                        s.admin = Some(a);
                        s.backoff = Duration::from_millis(250);
                        s.failures = 0;
                        s.last_ka = now;
                        s.ka = None;
                    }
                    Ok(Err(true)) => {
                        s.connecting = None;
                        s.failures = s.failures.saturating_add(1);
                        s.next_try = now + Duration::from_secs(5);
                    }
                    Ok(Err(false)) | Err(TryRecvError::Disconnected) => {
                        s.connecting = None;
                        s.failures = s.failures.saturating_add(1);
                        s.next_try = now + s.backoff;
                        s.backoff = (s.backoff * 2).min(Duration::from_secs(2));
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }
        }
        // A fence request is consumed only while there is a controller to tear
        // down; with none, a current-epoch request stays pending for the one
        // being adopted (a stale one fails the epoch test once it is).
        if s.admin.is_some() {
            let req = p.fence_req.swap(0, Ordering::AcqRel);
            if req != 0 && req == p.epoch.load(Ordering::Acquire) {
                if let Some(a) = s.admin.take() {
                    log::warn!("path {i} ({}): I/O failure under epoch {req}; tearing down controller {}", p.addr, a.cntlid);
                    a.shutdown();
                    Self::lose(&p);
                    s.ka = None;
                    // Give the target a moment to see every queue close before
                    // a new controller is created on this path.
                    s.next_try = now + Duration::from_millis(200);
                    return false;
                }
            }
        }
        if let Some(a) = s.admin.as_mut() {
            // A keep-alive response is due within min(KATO, 10 s), the bound
            // the blocking read used to put on it.
            let ka_timeout = self.kato.min(Duration::from_secs(10));
            let failed = match s.ka {
                Some((cid, sent)) => match a.ka_poll(cid) {
                    Ok(true) => {
                        s.ka = None;
                        None
                    }
                    Ok(false) if sent.elapsed() < ka_timeout => None,
                    Ok(false) => Some(format!("no keep-alive response in {} ms", ka_timeout.as_millis())),
                    Err(e) => Some(format!("{e:#}")),
                },
                None if s.last_ka.elapsed() >= self.kato / 3 => {
                    s.last_ka = now;
                    match a.ka_send() {
                        Ok(cid) => {
                            s.ka = Some((cid, now));
                            None
                        }
                        Err(e) => Some(format!("{e:#}")),
                    }
                }
                None => None,
            };
            if let Some(why) = failed {
                log::warn!("path {i} ({}): keep-alive failed ({why}); dropping controller", p.addr);
                if let Some(a) = s.admin.take() {
                    a.shutdown();
                }
                s.ka = None;
                Self::lose(&p);
                s.next_try = now;
            }
            return false;
        }
        // Not while a reconnect still runs, nor before its backoff is over.
        s.connecting.is_none() && now >= s.next_try
    }

    /// Start path `i`'s reconnect, on its own thread: TCP connect, enable
    /// and identify block. `permit` is its slot (`grant`), held by the
    /// thread for its whole life. Nothing is started if the path stopped
    /// being due since the tick (the device was shut down).
    fn start_reconnect(self: &Arc<Self>, i: usize, permit: ReconnectPermit) {
        let mut slots = self.slots.lock().unwrap();
        let s = &mut slots[i];
        if self.stop.load(Ordering::Acquire) || s.admin.is_some() || s.connecting.is_some() {
            return;
        }
        // Validation and the epoch bump happen on the thread, in the same
        // order the per-path supervisor used, before the supervisor sees the
        // connection.
        let (tx, rx) = mpsc::channel();
        let me = self.clone();
        let spawned = std::thread::Builder::new().name(reconnect_thread_name(&self.id.subnqn, i)).spawn(move || {
            // Held for the thread's whole life: its slot frees when it ends.
            let _permit = permit;
            let p = &me.paths[i];
            let r = match Self::bring_up(p, &me.id, me.kato) {
                Ok((a, _)) if me.stop.load(Ordering::Acquire) => {
                    a.shutdown();
                    Self::lose(p);
                    Err(false)
                }
                Ok((a, info)) if same_namespace(&me.info, &info) => {
                    // New controller: queues attached to the old one are dead.
                    p.epoch.fetch_add(1, Ordering::AcqRel);
                    Ok(a)
                }
                Ok((a, _)) => {
                    log::error!("path {i}: now presents a different namespace; refusing this path");
                    a.shutdown();
                    Self::lose(p);
                    Err(true)
                }
                Err(e) => {
                    log::debug!("path {i} reconnect failed: {e:#}");
                    Err(false)
                }
            };
            let _ = tx.send(r);
        });
        match spawned {
            Ok(_) => s.connecting = Some(rx),
            Err(e) => {
                log::warn!("path {i}: cannot spawn reconnect thread: {e}");
                s.next_try = Instant::now() + s.backoff;
            }
        }
    }

    /// A queue's I/O connection failed on a live-looking controller: the
    /// controller may be wedged. Force the supervisor to replace it only if
    /// keep-alive agrees; here we just record nothing and let keep-alive decide.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::net::TcpListener;

    /// A device with every path down and due for a reconnect. Built without
    /// a target (Ctrls::new needs one) and never registered with the process
    /// supervisor, so only the test ticks it.
    fn down_ctrls(addrs: Vec<SocketAddr>) -> Arc<Ctrls> {
        down_ctrls_nqn("nqn.test:ctrls", addrs)
    }

    fn down_ctrls_nqn(subnqn: &str, addrs: Vec<SocketAddr>) -> Arc<Ctrls> {
        let now = Instant::now();
        let paths = addrs.iter().map(|a| Arc::new(CtrlPath { addr: *a, cntlid: Lock::new(0), epoch: AtomicU64::new(1), max_qsize: Lock::new(128), fence_req: AtomicU64::new(0) })).collect();
        let slots = addrs.iter().map(|_| down_slot(now)).collect();
        let id = Ident { hostnqn: "nqn.2014-08.org.nvmexpress:uuid:test".into(), hostid: [0; 16], subnqn: subnqn.into() };
        let info = NsInfo { nsze: 1 << 20, lba_shift: 9, nguid: [0; 16], eui64: [0; 8], incapsule_bytes: 8192, mdts_bytes: 1 << 20 };
        Arc::new(Ctrls { paths, id: Arc::new(id), info, kato: Duration::from_secs(15), stop: AtomicBool::new(false), slots: Lock::new(slots) })
    }

    fn down_slot(now: Instant) -> PathSlot {
        PathSlot { admin: None, backoff: Duration::from_millis(250), failures: 0, next_try: now, last_ka: now, ka: None, connecting: None }
    }

    /// A reconnect budget of the test's own, so tests running in parallel
    /// do not share the process one.
    fn test_slots(max: usize, max_per_addr: usize) -> &'static ReconnectSlots {
        Box::leak(Box::new(ReconnectSlots::new(max, max_per_addr)))
    }

    /// A portal that completes the TCP handshake and never answers: a
    /// reconnect to it blocks in its first read (10 s), like one to a hung
    /// target. Dropping it resets those connections, which ends them.
    fn hung_portal() -> (TcpListener, SocketAddr) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let a = l.local_addr().unwrap();
        (l, a)
    }

    fn wait_all_ended(slots: &ReconnectSlots) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while slots.running() != 0 {
            assert!(Instant::now() < deadline, "reconnect slots not freed by their ended threads");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn thread_names() -> Vec<String> {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
            .map(|n| n.trim_end().to_string())
            .collect()
    }

    #[test]
    fn reconnect_slots_cap_and_free() {
        let (x, y): (SocketAddr, SocketAddr) = ("192.0.2.1:4420".parse().unwrap(), "192.0.2.2:4420".parse().unwrap());
        let slots = test_slots(3, 2);
        let a = slots.try_take(x).expect("first slot");
        let b = slots.try_take(x).expect("second slot");
        assert!(slots.try_take(x).is_none(), "a third reconnect to one address must wait");
        let c = slots.try_take(y).expect("another address has its own share");
        assert!(slots.try_take(y).is_none(), "the process-wide cap holds too");
        drop(a);
        assert_eq!((slots.running(), slots.running_to(x)), (2, 1));
        let d = slots.try_take(x).expect("a dropped permit frees its slot");
        // A reconnect thread that panics frees its slot as it unwinds.
        let r = std::thread::scope(|sc| {
            sc.spawn(move || {
                let _permit = d;
                panic!("reconnect thread dies");
            })
            .join()
        });
        assert!(r.is_err());
        let _e = slots.try_take(x).expect("a panicked thread's permit frees its slot");
        drop((b, c));
        assert_eq!((slots.running(), slots.running_to(x), slots.running_to(y)), (1, 1, 0));
    }

    /// A target reboot drops every path at once. One pass must start no
    /// more than MAX_RECONNECTS reconnect threads, and the rest keep their
    /// turn for a later pass.
    #[test]
    fn target_reboot_reconnects_are_capped() {
        let slots = test_slots(MAX_RECONNECTS, MAX_RECONNECTS_PER_ADDR);
        let portals: Vec<(TcpListener, SocketAddr)> = (0..5).map(|_| hung_portal()).collect();
        let devs: Vec<Arc<Ctrls>> = (0..50).map(|d| down_ctrls_nqn(&format!("nqn.test:reboot{d:03}"), vec![portals[d % 5].1])).collect();
        Supervisor::pass(&devs, 0, slots);
        let now = Instant::now();
        let started = devs.iter().filter(|c| c.slots.lock().unwrap()[0].connecting.is_some()).count();
        let waiting = devs.iter().filter(|c| c.slots.lock().unwrap().iter().all(|s| s.connecting.is_none() && s.next_try <= now)).count();
        assert_eq!(started, MAX_RECONNECTS, "reconnect threads started in one pass");
        assert_eq!(waiting, 50 - MAX_RECONNECTS, "paths over the cap stay due for the next pass");
        assert_eq!(slots.running(), MAX_RECONNECTS);
        for (_, a) in &portals {
            assert!(slots.running_to(*a) <= MAX_RECONNECTS_PER_ADDR);
        }
        // Each reconnect thread names its volume and path for top -H.
        let names = thread_names();
        assert!(names.contains(&reconnect_thread_name("nqn.test:reboot000", 0)), "no thread named {:?} in {names:?}", reconnect_thread_name("nqn.test:reboot000", 0));
        // The target goes away: the blocked reconnects fail, and each ended
        // thread gives its slot back.
        drop(portals);
        wait_all_ended(slots);
        devs.iter().for_each(|c| c.shutdown());
    }

    /// A hung (or dead) portal's reconnects must not starve a path to a
    /// healthy portal, however many volumes it strands. 200 volumes keep
    /// reconnecting to a portal whose every attempt blocks for 10 s; then
    /// one volume, last in the pass order, loses its controller on another
    /// portal. Its reconnect must start on the next pass. (Under one
    /// process-wide pool, the hung attempts held every slot for their 10 s
    /// and it did not start for as long.)
    #[test]
    fn a_hung_portal_does_not_starve_a_healthy_path() {
        let slots = test_slots(MAX_RECONNECTS, MAX_RECONNECTS_PER_ADDR);
        let (hung, h) = hung_portal();
        let (other, o) = hung_portal();
        let mut devs: Vec<Arc<Ctrls>> = (0..200).map(|_| down_ctrls(vec![h])).collect();
        for c in &devs {
            c.slots.lock().unwrap()[0].failures = 5;
        }
        let v = down_ctrls(vec![h, o]);
        {
            let mut s = v.slots.lock().unwrap();
            s[0].failures = 5;
            // Healthy for now.
            s[1].next_try = Instant::now() + Duration::from_secs(3600);
        }
        devs.push(v.clone());
        for turn in 0..3 {
            Supervisor::pass(&devs, turn, slots);
        }
        assert_eq!(slots.running_to(h), MAX_RECONNECTS_PER_ADDR, "the hung portal holds its own share, no more");
        // v's controller on the other portal drops (a fence or a keep-alive
        // timeout): the path is due, with no failed attempt behind it.
        v.slots.lock().unwrap()[1].next_try = Instant::now();
        Supervisor::pass(&devs, 3, slots);
        assert!(v.slots.lock().unwrap()[1].connecting.is_some(), "the healthy portal's path did not start its reconnect on the next pass");
        assert_eq!(slots.running_to(o), 1);
        drop((hung, other));
        wait_all_ended(slots);
        devs.iter().for_each(|c| c.shutdown());
    }

    fn due(rank: usize, path: usize, addr: SocketAddr, failures: u32, stranded: bool, nth: usize) -> Due {
        Due { rank, dev: rank, p: DuePath { path, addr, failures, stranded, nth } }
    }

    #[test]
    fn grant_order() {
        let addr = |i: u8| SocketAddr::from(([192, 0, 2, i], 4420));
        let (a, b) = (addr(1), addr(2));

        // A target reboot: 200 two-path volumes lose both controllers at
        // once. The slots go to one path of as many volumes as the caps
        // allow before any volume gets its second.
        let slots = test_slots(MAX_RECONNECTS, MAX_RECONNECTS_PER_ADDR);
        let reboot = (0..200).flat_map(|d| [(0, a), (1, b)].map(|(path, x)| due(d, path, x, 0, true, (path + d) % 2))).collect();
        let g = grant(reboot, slots);
        let volumes: BTreeSet<usize> = g.iter().map(|(d, _)| d.dev).collect();
        assert_eq!(g.len(), 2 * MAX_RECONNECTS_PER_ADDR);
        assert_eq!(volumes.len(), g.len(), "a volume got a second path before another got its first");
        assert_eq!((slots.running_to(a), slots.running_to(b)), (MAX_RECONNECTS_PER_ADDR, MAX_RECONNECTS_PER_ADDR));
        drop(g);

        // Four dead portals hold the whole pool; 200 failing paths to them
        // wait, all ahead in the pass order of one volume whose path to a
        // healthy portal just lost its controller.
        let dead: Vec<SocketAddr> = (3..7).map(addr).collect();
        let mut held = Vec::new();
        for &x in &dead {
            for _ in 0..MAX_RECONNECTS_PER_ADDR {
                held.push(slots.try_take(x).unwrap());
            }
        }
        let mut waiting: Vec<Due> = (0..200).map(|d| due(d, 0, dead[d % 4], 7, true, 0)).collect();
        waiting.push(due(200, 1, a, 0, false, 0));
        assert!(grant(waiting.clone(), slots).is_empty(), "no slot is free");
        // One doomed attempt ends: the freed slot goes to the fresh path,
        // not to the 200 in front of it.
        drop(held.pop());
        let g = grant(waiting, slots);
        assert_eq!(g.iter().map(|(d, _)| d.rank).collect::<Vec<_>>(), vec![200]);
        drop((g, held));

        // Among fresh paths, a volume left with no path goes first; among
        // failing ones, the fewest failures, so a dead portal's paths take
        // its slots in turn.
        let one = test_slots(1, 1);
        let g = grant(vec![due(0, 0, a, 0, false, 0), due(1, 0, a, 0, true, 0)], one);
        assert_eq!(g[0].0.rank, 1);
        drop(g);
        let g = grant(vec![due(0, 0, a, 5, true, 0), due(1, 0, a, 2, true, 0), due(2, 0, a, 9, true, 0)], one);
        assert_eq!(g[0].0.rank, 1);
    }

    /// A device's due paths come out in its rotated order, a path with no
    /// failed attempt first, and flagged stranded while no controller is up.
    #[test]
    fn tick_reports_due_paths_in_rotated_order() {
        let addrs: Vec<SocketAddr> = (1..=3).map(|i| SocketAddr::from(([192, 0, 2, i], 4420))).collect();
        let c = down_ctrls(addrs);
        let order = |c: &Arc<Ctrls>, rot| c.tick(rot).iter().map(|d| (d.path, d.nth, d.stranded)).collect::<Vec<_>>();
        assert_eq!(order(&c, 0), vec![(0, 0, true), (1, 1, true), (2, 2, true)]);
        assert_eq!(order(&c, 4), vec![(1, 0, true), (2, 1, true), (0, 2, true)]);
        c.slots.lock().unwrap()[1].failures = 3;
        assert_eq!(order(&c, 4), vec![(2, 0, true), (0, 1, true), (1, 2, true)]);
        c.shutdown();
        assert!(c.tick(0).is_empty(), "a stopped device reconnects nothing");
    }

    #[test]
    fn reconnect_thread_name_fits_comm() {
        assert_eq!(reconnect_thread_name("nqn.2011-06.com.truenas:uuid:1b2c:pvc-6f0c21ab", 1), "rc1-6f0c21ab");
        assert_eq!(reconnect_thread_name("short", 0), "rc0-short");
        assert!(reconnect_thread_name("nqn.2011-06.com.truenas:x", 9).len() <= 15);
    }

    #[test]
    fn a_panic_under_a_path_lock_does_not_poison_it() {
        let c = down_ctrls(vec!["127.0.0.1:4420".parse().unwrap()]);
        let p = c.paths[0].clone();
        let p2 = p.clone();
        let r = std::thread::spawn(move || {
            let _held = p2.cntlid.lock().unwrap();
            panic!("a thread dies holding the cntlid lock");
        })
        .join();
        assert!(r.is_err());
        // What the queue threads and the supervisor do next.
        assert_eq!(p.cntlid(), None);
        assert_eq!(p.snapshot(), None);
        Ctrls::lose(&p);
        assert_eq!(p.epoch.load(Ordering::Acquire), 2);
    }

    /// One device's pass panicking must not stop the pass over the others,
    /// nor poison the device's own slots for the next pass.
    #[test]
    fn a_panicking_device_does_not_stop_the_supervisor() {
        let slots = test_slots(MAX_RECONNECTS, MAX_RECONNECTS_PER_ADDR);
        // A slot with no path behind it: its pass panics indexing the paths,
        // while holding the device's slots lock.
        let bad = down_ctrls(vec![]);
        bad.slots.lock().unwrap().push(down_slot(Instant::now()));
        // A healthy device with a failed reconnect waiting to be adopted: its
        // pass consumes it and doubles the backoff, and starts no thread.
        let good = down_ctrls(vec!["127.0.0.1:4420".parse().unwrap()]);
        let (tx, rx) = mpsc::channel();
        tx.send(Err(false)).unwrap();
        good.slots.lock().unwrap()[0].connecting = Some(rx);
        Supervisor::pass(&[bad.clone(), good.clone()], 0, slots);
        {
            let slots = good.slots.lock().unwrap();
            assert!(slots[0].connecting.is_none(), "the device after the panicking one was not ticked");
            assert_eq!(slots[0].backoff, Duration::from_millis(500));
            assert_eq!(slots[0].failures, 1);
        }
        assert_eq!(slots.running(), 0);
        good.shutdown();
        // The next pass over the panicking device locks its slots again.
        Supervisor::pass(std::slice::from_ref(&bad), 1, slots);
        assert_eq!(bad.slots.lock().unwrap().len(), 1);
    }
}
