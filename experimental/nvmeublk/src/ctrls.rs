//! Per-path admin controllers for the io_uring engine: connect, enable,
//! identify, keep-alive and reconnect. One supervisor thread per process
//! serves every path of every device (see `Supervisor`).
//! I/O queues belong to the ublk queue threads (see qengine.rs); this layer
//! only tells them which controller to attach to and when it changed.

use crate::conn::{AdminConn, Ident, NsInfo};
use anyhow::{bail, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
/// and 10 s in a read, with its stack mlocked. A path that finds no free
/// slot keeps its turn and asks again on the next pass (100 ms).
const MAX_RECONNECTS: usize = 32;

static RECONNECTS: ReconnectSlots = ReconnectSlots::new(MAX_RECONNECTS);

/// Counts the running reconnect threads against a cap.
struct ReconnectSlots {
    busy: AtomicUsize,
    max: usize,
}

/// One running reconnect's slot, freed when this is dropped: when its
/// thread ends, by return or by panic, or when the thread cannot be spawned
/// (the closure that owns it is dropped then).
struct ReconnectPermit<'a>(&'a ReconnectSlots);

impl ReconnectSlots {
    const fn new(max: usize) -> Self {
        ReconnectSlots { busy: AtomicUsize::new(0), max }
    }

    fn try_take(&self) -> Option<ReconnectPermit<'_>> {
        self.busy.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < self.max).then_some(n + 1)).ok()?;
        Some(ReconnectPermit(self))
    }
}

impl Drop for ReconnectPermit<'_> {
    fn drop(&mut self) {
        self.0.busy.fetch_sub(1, Ordering::AcqRel);
    }
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
            Self::pass(&live, turn);
            turn = turn.wrapping_add(1);
        }
    }

    /// One pass over every device, starting at device `start % len`. The
    /// start moves every pass: while reconnect threads are capped, the
    /// devices first in line would otherwise take every slot that frees up,
    /// and the paths of a dead target could keep a live target's paths from
    /// reconnecting. A panic in one device's pass is caught and logged: it
    /// must not end the only thread that keeps every other device's
    /// controllers alive, and as its locks do not poison, the next pass runs
    /// normally.
    fn pass(live: &[Arc<Ctrls>], start: usize) {
        let n = live.len();
        for c in live.iter().cycle().skip(start % n.max(1)).take(n) {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.tick())).is_err() {
                log::error!("admin supervisor: pass for {} panicked; continuing with the other devices", c.id.subnqn);
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
            .map(|admin| PathSlot { admin, backoff: Duration::from_millis(250), next_try: now, last_ka: now, ka: None, connecting: None })
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

    /// One supervisor pass over this device's paths.
    fn tick(self: &Arc<Self>) {
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
            return;
        }
        for (i, slot) in slots.iter_mut().enumerate() {
            self.tick_path(i, slot);
        }
    }

    fn tick_path(self: &Arc<Self>, i: usize, s: &mut PathSlot) {
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
                        s.last_ka = now;
                        s.ka = None;
                    }
                    Ok(Err(true)) => {
                        s.connecting = None;
                        s.next_try = now + Duration::from_secs(5);
                    }
                    Ok(Err(false)) | Err(TryRecvError::Disconnected) => {
                        s.connecting = None;
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
                    return;
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
            return;
        }
        if s.connecting.is_some() {
            return; // reconnect still running
        }
        if now < s.next_try {
            return;
        }
        // At the process-wide cap: keep this path's turn (next_try is left
        // as it is) and ask again on the next pass.
        let Some(permit) = RECONNECTS.try_take() else { return };
        // Reconnect on its own thread: TCP connect, enable and identify block.
        // Validation and the epoch bump happen there, in the same order the
        // per-path supervisor used, before the supervisor sees the connection.
        let (tx, rx) = mpsc::channel();
        let me = self.clone();
        let spawned = std::thread::Builder::new().name(format!("nvme-reconn-{i}")).spawn(move || {
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
                s.next_try = now + s.backoff;
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

    /// A device with every path down and due for a reconnect. Built without
    /// a target (Ctrls::new needs one) and never registered with the process
    /// supervisor, so only the test ticks it.
    fn down_ctrls(addrs: Vec<SocketAddr>) -> Arc<Ctrls> {
        let now = Instant::now();
        let paths = addrs.iter().map(|a| Arc::new(CtrlPath { addr: *a, cntlid: Lock::new(0), epoch: AtomicU64::new(1), max_qsize: Lock::new(128), fence_req: AtomicU64::new(0) })).collect();
        let slots = addrs.iter().map(|_| down_slot(now)).collect();
        let id = Ident { hostnqn: "nqn.2014-08.org.nvmexpress:uuid:test".into(), hostid: [0; 16], subnqn: "nqn.test:ctrls".into() };
        let info = NsInfo { nsze: 1 << 20, lba_shift: 9, nguid: [0; 16], eui64: [0; 8], incapsule_bytes: 8192, mdts_bytes: 1 << 20 };
        Arc::new(Ctrls { paths, id: Arc::new(id), info, kato: Duration::from_secs(15), stop: AtomicBool::new(false), slots: Lock::new(slots) })
    }

    fn down_slot(now: Instant) -> PathSlot {
        PathSlot { admin: None, backoff: Duration::from_millis(250), next_try: now, last_ka: now, ka: None, connecting: None }
    }

    #[test]
    fn reconnect_slots_cap_and_free() {
        let slots = ReconnectSlots::new(2);
        let a = slots.try_take().expect("first slot");
        let b = slots.try_take().expect("second slot");
        assert!(slots.try_take().is_none(), "a third reconnect must wait for a free slot");
        drop(a);
        let c = slots.try_take().expect("a dropped permit frees its slot");
        // A reconnect thread that panics frees its slot as it unwinds.
        let r = std::thread::scope(|sc| {
            sc.spawn(move || {
                let _permit = c;
                panic!("reconnect thread dies");
            })
            .join()
        });
        assert!(r.is_err());
        let _d = slots.try_take().expect("a panicked thread's permit frees its slot");
        drop(b);
    }

    /// A target reboot drops every path at once. One pass must start no
    /// more than MAX_RECONNECTS reconnect threads, and the rest keep their
    /// turn for a later pass.
    #[test]
    fn target_reboot_reconnects_are_capped() {
        // A target that completes the TCP handshake and never answers: each
        // reconnect blocks in its first read, so none ends during the pass.
        let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = target.local_addr().unwrap();
        let c = down_ctrls(vec![addr; MAX_RECONNECTS + 8]);
        c.tick();
        let (started, waiting) = {
            let slots = c.slots.lock().unwrap();
            let started = slots.iter().filter(|s| s.connecting.is_some()).count();
            let now = Instant::now();
            (started, slots.iter().filter(|s| s.connecting.is_none() && s.next_try <= now).count())
        };
        assert_eq!(started, MAX_RECONNECTS, "reconnect threads started in one pass");
        assert_eq!(waiting, 8, "paths over the cap stay due for the next pass");
        assert_eq!(RECONNECTS.busy.load(Ordering::Acquire), MAX_RECONNECTS);
        // The target goes away: the blocked reconnects fail, and each ended
        // thread gives its slot back.
        drop(target);
        let deadline = Instant::now() + Duration::from_secs(20);
        while RECONNECTS.busy.load(Ordering::Acquire) != 0 {
            assert!(Instant::now() < deadline, "reconnect slots not freed by their ended threads");
            std::thread::sleep(Duration::from_millis(20));
        }
        c.shutdown();
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
        Supervisor::pass(&[bad.clone(), good.clone()], 0);
        {
            let slots = good.slots.lock().unwrap();
            assert!(slots[0].connecting.is_none(), "the device after the panicking one was not ticked");
            assert_eq!(slots[0].backoff, Duration::from_millis(500));
        }
        good.shutdown();
        // The next pass over the panicking device locks its slots again.
        Supervisor::pass(&[bad.clone()], 1);
        assert_eq!(bad.slots.lock().unwrap().len(), 1);
    }
}
