//! Per-path admin controllers for the io_uring engine: connect, enable,
//! identify, keep-alive and reconnect. One supervisor thread per process
//! serves every path of every device (see `Supervisor`).
//! I/O queues belong to the ublk queue threads (see qengine.rs); this layer
//! only tells them which controller to attach to and when it changed.

use crate::conn::{AdminConn, Ident, NsInfo};
use anyhow::{bail, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

pub struct CtrlPath {
    pub addr: SocketAddr,
    /// Controller id of the live admin queue, 0 while down.
    cntlid: Mutex<u16>,
    /// Bumped every time the controller is lost or replaced. A queue whose
    /// I/O connection was made under an older epoch must reconnect: its
    /// queue belongs to a controller that no longer exists.
    pub epoch: AtomicU64,
    pub max_qsize: Mutex<u16>,
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
    slots: Mutex<Vec<PathSlot>>,
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
    ctrls: Mutex<Vec<Weak<Ctrls>>>,
    /// Set only once the supervisor thread was actually spawned: a failed
    /// spawn must be retried by the next register(), or every device from
    /// then on would get no keep-alive and no reconnect at all.
    running: Mutex<bool>,
}

static SUPERVISOR: LazyLock<Supervisor> = LazyLock::new(|| Supervisor { ctrls: Mutex::new(Vec::new()), running: Mutex::new(false) });

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
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let live: Vec<Arc<Ctrls>> = {
                let mut list = self.ctrls.lock().unwrap();
                list.retain(|w| w.strong_count() > 0);
                list.iter().filter_map(Weak::upgrade).collect()
            };
            for c in live {
                c.tick();
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
            .map(|a| Arc::new(CtrlPath { addr: *a, cntlid: Mutex::new(0), epoch: AtomicU64::new(1), max_qsize: Mutex::new(128), fence_req: AtomicU64::new(0) }))
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
        let me = Arc::new(Ctrls { paths, id, info, kato, stop: AtomicBool::new(false), slots: Mutex::new(slots) });
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
        // Reconnect on its own thread: TCP connect, enable and identify block.
        // Validation and the epoch bump happen there, in the same order the
        // per-path supervisor used, before the supervisor sees the connection.
        let (tx, rx) = mpsc::channel();
        let me = self.clone();
        let spawned = std::thread::Builder::new().name(format!("nvme-reconn-{i}")).spawn(move || {
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
