//! io_uring-native data path. Each ublk queue thread owns one NVMe/TCP I/O
//! queue per path and drives every socket operation (batched writev, recv,
//! timers) as SQEs on the ublk queue's own io_uring. An I/O never leaves its
//! queue thread: submit, network and completion are all one executor, so
//! there is no sender/receiver thread hop and no cross-thread wakeup.
//!
//! Blocking work that cannot be an SQE (TCP connect + NVMe Connect handshake
//! on reconnect) runs on a helper thread; only the finished socket comes back.

use crate::conn::connect_io_queue;
use crate::ctrls::Ctrls;
use crate::pdu::*;
use libublk::uring_async::ublk_submit_sqe_async;
use libublk::UblkUringData;
use smol::channel::{Receiver, Sender};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::net::{Shutdown, TcpStream};
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

pub const NSID: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Op {
    Read,
    Write,
    Flush,
}

#[derive(Default)]
pub struct Stats {
    pub failovers: AtomicU64,
    pub resubmits: AtomicU64,
    pub parked: AtomicU64,
    pub no_path_eio: AtomicU64,
    pub reconnects: AtomicU64,
    pub stall_kills: AtomicU64,
    pub epoch_kills: AtomicU64,
    /// Latency split: capsule on the wire -> response, and ublk submit -> response.
    pub done: AtomicU64,
    pub wire_ns: AtomicU64,
    pub total_ns: AtomicU64,
    pub direct_rx: AtomicU64,
}

#[derive(Clone)]
pub struct QConfig {
    pub io_timeout: Duration,
    pub no_path_timeout: Duration,
    pub max_attempts: u32,
}

/// One block request as seen by the engine. `buf` is the tag's IoBuf, owned
/// by the tag task until `done` delivers the result.
pub struct Pending {
    pub op: Op,
    pub slba: u64,
    pub nlb: u32,
    pub buf: *mut u8,
    pub len: usize,
    pub done: Sender<i32>,
    pub first: Instant,
    /// When the current attempt went on the wire. The stall watchdog keys on
    /// this, not `first`: a request failed over from a stalled path is
    /// already old, and must not condemn the healthy path it lands on.
    pub sent: Instant,
    pub attempts: u32,
}

impl Pending {
    fn finish(self, res: i32) {
        let _ = self.done.try_send(res);
    }
    fn ok_res(&self) -> i32 {
        if self.op == Op::Flush { 0 } else { self.len as i32 }
    }
}

/// One outbound PDU: header bytes plus an optional payload borrowed from a
/// request buffer that stays valid until that request completes.
struct OutMsg {
    head: Vec<u8>,
    data: *const u8,
    len: usize,
}

struct QConn {
    path: usize,
    epoch: u64,
    stream: TcpStream,
    fd: i32,
    maxh2c: usize,
    dead: Cell<bool>,
    /// Fault injection: the receiver stops reading (no socket error), so
    /// in-flight commands hang until the stall watchdog fails the path.
    stalled: Cell<bool>,
    inflight: RefCell<HashMap<u16, Pending>>,
    free: RefCell<Vec<u16>>,
    tx: Sender<OutMsg>,
    /// cid whose C2H payload is being received straight into its buffer.
    /// That request must not be resubmitted while the Recv is in flight, or
    /// a late write could land in a buffer another path already completed.
    rx_direct: Cell<Option<u16>>,
    /// Where fail_conn parks the rx_direct request; the receiver resubmits
    /// it once its Recv has returned.
    held: RefCell<Option<Pending>>,
}

impl QConn {
    fn oldest(&self) -> Option<Duration> {
        self.inflight.borrow().values().map(|p| p.sent.elapsed()).max()
    }
}

enum Direct {
    No,
    Done,
    Failed,
}

type ConnectResult = (usize, u64, anyhow::Result<(TcpStream, u32)>);

pub struct QEngine {
    qid: u16,
    ctrls: Arc<Ctrls>,
    cfg: QConfig,
    incapsule: usize,
    conns: RefCell<Vec<Option<Rc<QConn>>>>,
    connecting: RefCell<Vec<bool>>,
    next_try: RefCell<Vec<Instant>>,
    backoff: RefCell<Vec<Duration>>,
    parked: RefCell<VecDeque<Pending>>,
    exe: Rc<smol::LocalExecutor<'static>>,
    results_tx: mpsc::Sender<ConnectResult>,
    results_rx: mpsc::Receiver<ConnectResult>,
    wake_efd: i32,
    pub stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
}

fn io_sqe_res(r: Result<i32, libublk::UblkError>) -> i32 {
    r.unwrap_or(-libc::EIO)
}

impl QEngine {
    pub fn new(qid: u16, ctrls: Arc<Ctrls>, cfg: QConfig, exe: Rc<smol::LocalExecutor<'static>>, stats: Arc<Stats>, stop: Arc<AtomicBool>) -> Rc<Self> {
        let n = ctrls.paths.len();
        let (results_tx, results_rx) = mpsc::channel();
        let wake_efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        Rc::new(QEngine {
            qid,
            incapsule: ctrls.info.incapsule_bytes,
            ctrls,
            cfg,
            conns: RefCell::new(vec![None; n]),
            connecting: RefCell::new(vec![false; n]),
            next_try: RefCell::new(vec![Instant::now(); n]),
            backoff: RefCell::new(vec![Duration::from_millis(250); n]),
            parked: RefCell::new(VecDeque::new()),
            exe,
            results_tx,
            results_rx,
            wake_efd,
            stats,
            stop,
        })
    }

    /// Start the timer and reconnect-result tasks. Connections come up on
    /// the first timer tick.
    pub fn start(self: &Rc<Self>) {
        let me = self.clone();
        self.exe.spawn(async move { me.timer_task().await }).detach();
        let me = self.clone();
        self.exe.spawn(async move { me.results_task().await }).detach();
    }

    fn live(&self) -> Vec<Rc<QConn>> {
        self.conns.borrow().iter().flatten().filter(|c| !c.dead.get()).cloned().collect()
    }

    /// Dispatch to the live connection with the fewest outstanding commands;
    /// park if none is live.
    pub fn submit(&self, mut p: Pending) {
        let mut live = self.live();
        live.sort_by_key(|c| c.inflight.borrow().len());
        for c in live {
            match self.try_submit(&c, p) {
                Ok(()) => return,
                Err(back) => p = back,
            }
        }
        self.stats.parked.fetch_add(1, Ordering::Relaxed);
        self.parked.borrow_mut().push_back(p);
    }

    fn try_submit(&self, c: &Rc<QConn>, mut p: Pending) -> Result<(), Pending> {
        let Some(cid) = c.free.borrow_mut().pop() else { return Err(p) };
        let inline = p.op == Op::Write && p.len <= self.incapsule;
        let sqe = match p.op {
            Op::Read => rw_cmd(OPC_READ, cid, NSID, p.slba, p.nlb, p.len as u32, false),
            Op::Write => rw_cmd(OPC_WRITE, cid, NSID, p.slba, p.nlb, p.len as u32, inline),
            Op::Flush => flush_cmd(cid, NSID),
        };
        let (data, len) = if inline { (p.buf as *const u8, p.len) } else { (std::ptr::null(), 0) };
        let head = capsule_header(&sqe, len);
        p.sent = Instant::now();
        c.inflight.borrow_mut().insert(cid, p);
        if c.tx.try_send(OutMsg { head, data, len }).is_err() {
            c.free.borrow_mut().push(cid);
            let p = c.inflight.borrow_mut().remove(&cid).expect("just inserted");
            return Err(p);
        }
        Ok(())
    }

    fn resubmit(&self, mut p: Pending) {
        p.attempts += 1;
        if p.attempts > self.cfg.max_attempts {
            log::error!("q{} {:?} slba {}: giving up after {} attempts", self.qid, p.op, p.slba, p.attempts - 1);
            p.finish(-libc::EIO);
            return;
        }
        self.stats.resubmits.fetch_add(1, Ordering::Relaxed);
        self.submit(p);
    }

    /// Tear a connection down and move its in-flight commands elsewhere.
    fn fail_conn(&self, c: &Rc<QConn>, why: &str) {
        if c.dead.replace(true) {
            return;
        }
        let _ = c.stream.shutdown(Shutdown::Both);
        c.tx.close();
        {
            let mut conns = self.conns.borrow_mut();
            if conns[c.path].as_ref().is_some_and(|x| Rc::ptr_eq(x, c)) {
                conns[c.path] = None;
            }
        }
        let direct = c.rx_direct.get();
        let mut orphans = Vec::new();
        for (cid, p) in c.inflight.borrow_mut().drain() {
            if Some(cid) == direct {
                *c.held.borrow_mut() = Some(p);
            } else {
                orphans.push(p);
            }
        }
        self.next_try.borrow_mut()[c.path] = Instant::now();
        if self.stop.load(Ordering::Acquire) {
            for p in orphans {
                p.finish(-libc::EIO);
            }
            return;
        }
        if orphans.is_empty() {
            log::warn!("q{} path {}: {why}", self.qid, c.path);
        } else {
            self.stats.failovers.fetch_add(1, Ordering::Relaxed);
            log::warn!("q{} path {}: {why}; failing over {} in-flight", self.qid, c.path, orphans.len());
        }
        for p in orphans {
            self.resubmit(p);
        }
    }

    fn complete(&self, c: &QConn, cid: u16, ok: bool, status: u16) {
        let p = c.inflight.borrow_mut().remove(&cid);
        match p {
            Some(p) => {
                c.free.borrow_mut().push(cid);
                self.stats.done.fetch_add(1, Ordering::Relaxed);
                self.stats.wire_ns.fetch_add(p.sent.elapsed().as_nanos() as u64, Ordering::Relaxed);
                self.stats.total_ns.fetch_add(p.first.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if ok {
                    let r = p.ok_res();
                    p.finish(r);
                } else {
                    log::warn!("q{} path {}: {:?} slba {} failed status {status:#x}", self.qid, c.path, p.op, p.slba);
                    p.finish(-libc::EIO);
                }
            }
            None => log::warn!("q{} path {}: completion for unknown cid {cid}", self.qid, c.path),
        }
    }

    fn install(self: &Rc<Self>, path: usize, epoch: u64, stream: TcpStream, maxh2c: u32, qsize: u16) {
        // Blocking fd on purpose: io_uring honours O_NONBLOCK and would hand
        // back -EAGAIN instead of arming a poll, turning the receiver into a spin.
        let _ = stream.set_nonblocking(false);
        let (tx, rx) = smol::channel::unbounded::<OutMsg>();
        let c = Rc::new(QConn {
            path,
            epoch,
            fd: stream.as_raw_fd(),
            stream,
            maxh2c: maxh2c as usize,
            dead: Cell::new(false),
            stalled: Cell::new(false),
            inflight: RefCell::new(HashMap::new()),
            free: RefCell::new((1..qsize).rev().collect()),
            tx,
            rx_direct: Cell::new(None),
            held: RefCell::new(None),
        });
        self.conns.borrow_mut()[path] = Some(c.clone());
        self.backoff.borrow_mut()[path] = Duration::from_millis(250);
        let (me, c2) = (self.clone(), c.clone());
        self.exe.spawn(async move { me.sender_task(c2, rx).await }).detach();
        let (me, c2) = (self.clone(), c);
        self.exe.spawn(async move { me.receiver_task(c2).await }).detach();
        log::info!("q{} path {path} I/O queue {} up", self.qid, self.qid + 1);
        let parked: Vec<Pending> = self.parked.borrow_mut().drain(..).collect();
        for p in parked {
            self.submit(p);
        }
    }

    async fn sender_task(self: Rc<Self>, c: Rc<QConn>, rx: Receiver<OutMsg>) {
        let mut batch: Vec<OutMsg> = Vec::with_capacity(64);
        while let Ok(first) = rx.recv().await {
            batch.push(first);
            while batch.len() < 256 {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            let mut iov: Vec<libc::iovec> = Vec::with_capacity(batch.len() * 2);
            for m in &batch {
                iov.push(libc::iovec { iov_base: m.head.as_ptr() as *mut _, iov_len: m.head.len() });
                if m.len > 0 {
                    iov.push(libc::iovec { iov_base: m.data as *mut _, iov_len: m.len });
                }
            }
            let mut idx = 0usize;
            while idx < iov.len() {
                let n = iov.len() - idx;
                let sqe = io_uring::opcode::Writev::new(io_uring::types::Fd(c.fd), iov[idx..].as_ptr() as *const _, n.min(1024) as u32).build();
                let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
                if r == -libc::EAGAIN || r == -libc::EINTR {
                    continue;
                }
                if r <= 0 {
                    self.fail_conn(&c, &format!("send failed ({r})"));
                    return;
                }
                // Advance past fully written iovecs; trim a partial one.
                let mut left = r as usize;
                while left > 0 {
                    let l = iov[idx].iov_len;
                    if left >= l {
                        left -= l;
                        idx += 1;
                    } else {
                        iov[idx].iov_base = unsafe { (iov[idx].iov_base as *mut u8).add(left) } as *mut _;
                        iov[idx].iov_len -= left;
                        left = 0;
                    }
                }
            }
            batch.clear();
        }
    }

    async fn receiver_task(self: Rc<Self>, c: Rc<QConn>) {
        let mut buf = vec![0u8; 256 * 1024];
        let (mut start, mut end) = (0usize, 0usize);
        let pause = io_uring::types::Timespec::new().nsec(50_000_000);
        loop {
            while c.stalled.get() && !c.dead.get() {
                let sqe = io_uring::opcode::Timeout::new(&pause).build();
                let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
            }
            if c.dead.get() {
                return;
            }
            if end == buf.len() {
                if start > 0 {
                    buf.copy_within(start..end, 0);
                    end -= start;
                    start = 0;
                } else {
                    buf.resize(buf.len() * 2, 0);
                }
            }
            let sqe = io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), buf[end..].as_mut_ptr(), (buf.len() - end) as u32).build();
            let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
            if c.dead.get() {
                return;
            }
            if r == -libc::EAGAIN || r == -libc::EINTR {
                continue;
            }
            if r <= 0 {
                self.fail_conn(&c, if r == 0 { "connection closed" } else { "receive failed" });
                return;
            }
            end += r as usize;
            // Consume every complete PDU in the buffer.
            loop {
                if end - start < CH_LEN {
                    break;
                }
                let plen = u32::from_le_bytes(buf[start + 4..start + 8].try_into().unwrap()) as usize;
                if plen < CH_LEN || plen > 64 << 20 {
                    self.fail_conn(&c, "malformed PDU length");
                    return;
                }
                if end - start < plen {
                    if plen > buf.len() {
                        buf.resize(plen.next_power_of_two(), 0);
                    }
                    break;
                }
                if let Err(e) = self.handle_pdu(&c, &buf[start..start + plen]) {
                    self.fail_conn(&c, &e);
                    return;
                }
                start += plen;
            }
            if start < end {
                match self.try_direct(&c, &buf[start..end]).await {
                    Direct::No => {}
                    Direct::Done => {
                        start = 0;
                        end = 0;
                    }
                    Direct::Failed => return,
                }
            }
            if start == end {
                start = 0;
                end = 0;
            }
        }
    }

    /// A partial C2HData PDU sits at the tail of the staging buffer: copy the
    /// payload bytes already here and receive the rest straight into the
    /// request's buffer, skipping the staging copy and the compaction memmove
    /// (together they capped a queue at ~0.5 GB/s of 128K reads).
    async fn try_direct(&self, c: &Rc<QConn>, part: &[u8]) -> Direct {
        if part.len() < CH_LEN || part[0] != PDU_C2H_DATA {
            return Direct::No;
        }
        let (flags, hlen, pdo) = (part[1], part[2] as usize, part[3] as usize);
        let plen = u32::from_le_bytes(part[4..8].try_into().unwrap()) as usize;
        if part.len() < pdo || pdo < hlen || hlen < CH_LEN + 16 {
            return Direct::No;
        }
        let h = parse_data_hdr(&part[CH_LEN..hlen]);
        let (off, len) = (h.off as usize, h.len as usize);
        // Only the plain layout (no data digest, payload ends the PDU).
        if plen != pdo + len {
            return Direct::No;
        }
        let dest = {
            let inflight = c.inflight.borrow();
            match inflight.get(&h.cid) {
                Some(p) if off + len <= p.len => unsafe { p.buf.add(off) },
                _ => return Direct::No, // handle_pdu reports it once whole
            }
        };
        let have = part.len() - pdo;
        unsafe { std::ptr::copy_nonoverlapping(part[pdo..].as_ptr(), dest, have) };
        c.rx_direct.set(Some(h.cid));
        self.stats.direct_rx.fetch_add(1, Ordering::Relaxed);
        let mut got = have;
        let mut failed = None;
        while got < len {
            let sqe = io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), unsafe { dest.add(got) }, (len - got) as u32)
                .flags(libc::MSG_WAITALL)
                .build();
            let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
            if c.dead.get() {
                break;
            }
            if r == -libc::EAGAIN || r == -libc::EINTR {
                continue;
            }
            if r <= 0 {
                failed = Some(if r == 0 { "connection closed" } else { "receive failed" });
                break;
            }
            got += r as usize;
        }
        if got < len {
            self.fail_conn(c, failed.unwrap_or("connection lost during data receive"));
        }
        c.rx_direct.set(None);
        if let Some(p) = c.held.borrow_mut().take() {
            if self.stop.load(Ordering::Acquire) {
                p.finish(-libc::EIO);
            } else {
                self.resubmit(p);
            }
        }
        if got < len {
            return Direct::Failed;
        }
        if flags & FLAG_C2H_SUCCESS != 0 {
            self.complete(c, h.cid, true, 0);
        }
        Direct::Done
    }

    fn handle_pdu(&self, c: &QConn, pdu: &[u8]) -> Result<(), String> {
        let (ptype, flags, hlen, pdo) = (pdu[0], pdu[1], pdu[2] as usize, pdu[3] as usize);
        match ptype {
            PDU_C2H_DATA => {
                let h = parse_data_hdr(&pdu[CH_LEN..hlen]);
                let (off, len) = (h.off as usize, h.len as usize);
                let inflight = c.inflight.borrow();
                let Some(p) = inflight.get(&h.cid) else { return Err(format!("C2HData for unknown cid {}", h.cid)) };
                if off + len > p.len || pdo + len > pdu.len() {
                    return Err(format!("C2HData out of range: cid {} off {off} len {len}", h.cid));
                }
                unsafe { std::ptr::copy_nonoverlapping(pdu[pdo..pdo + len].as_ptr(), p.buf.add(off), len) };
                drop(inflight);
                if flags & FLAG_C2H_SUCCESS != 0 {
                    self.complete(c, h.cid, true, 0);
                }
            }
            PDU_R2T => {
                let h = parse_data_hdr(&pdu[CH_LEN..hlen]);
                let (off, len) = (h.off as usize, h.len as usize);
                let (buf, blen) = {
                    let inflight = c.inflight.borrow();
                    let Some(p) = inflight.get(&h.cid) else { return Err(format!("R2T for unknown cid {}", h.cid)) };
                    (p.buf, p.len)
                };
                if off + len > blen {
                    return Err(format!("R2T out of range: cid {} off {off} len {len}", h.cid));
                }
                let mut sent = 0;
                while sent < len {
                    let n = (len - sent).min(c.maxh2c);
                    let head = h2c_header(h.cid, h.ttag, (off + sent) as u32, n, sent + n == len);
                    let data = unsafe { buf.add(off + sent) } as *const u8;
                    if c.tx.try_send(OutMsg { head, data, len: n }).is_err() {
                        return Err("sender gone while answering R2T".into());
                    }
                    sent += n;
                }
            }
            PDU_CAPSULE_RESP => {
                let cqe = Cqe::parse(&pdu[CH_LEN..CH_LEN + 16]);
                self.complete(c, cqe.cid, cqe.sc() == 0, cqe.status);
            }
            PDU_C2H_TERM => return Err("target terminated the connection".into()),
            t => return Err(format!("unexpected PDU type {t:#x}")),
        }
        Ok(())
    }

    /// 100ms housekeeping: reconnect, stale-controller and stall detection,
    /// parked-I/O expiry, and fault injection.
    async fn timer_task(self: Rc<Self>) {
        let ts = io_uring::types::Timespec::new().nsec(100_000_000);
        while !self.stop.load(Ordering::Acquire) {
            let sqe = io_uring::opcode::Timeout::new(&ts).build();
            let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
            self.fault_injection();
            for i in 0..self.ctrls.paths.len() {
                let ctrl = &self.ctrls.paths[i];
                let conn = self.conns.borrow()[i].clone();
                match conn {
                    Some(c) => {
                        if c.epoch != ctrl.epoch.load(Ordering::Acquire) {
                            self.stats.epoch_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "controller replaced");
                        } else if c.oldest().is_some_and(|a| a > self.cfg.io_timeout) {
                            self.stats.stall_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "request stalled past io_timeout");
                        }
                    }
                    None => self.maybe_connect(i),
                }
            }
            self.expire_parked();
        }
        let conns: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().cloned().collect();
        for c in conns {
            self.fail_conn(&c, "shutting down");
        }
    }

    fn maybe_connect(&self, i: usize) {
        if self.connecting.borrow()[i] || Instant::now() < self.next_try.borrow()[i] {
            return;
        }
        let ctrl = self.ctrls.paths[i].clone();
        let Some(cntlid) = ctrl.cntlid() else { return };
        let epoch = ctrl.epoch.load(Ordering::Acquire);
        let qsize = *ctrl.max_qsize.lock().unwrap();
        self.connecting.borrow_mut()[i] = true;
        let (tx, efd, id, qid) = (self.results_tx.clone(), self.wake_efd, self.ctrls.id.clone(), self.qid + 1);
        std::thread::spawn(move || {
            let r = connect_io_queue(ctrl.addr, &id, cntlid, qid, qsize);
            let _ = tx.send((i, epoch, r));
            let one: u64 = 1;
            unsafe { libc::write(efd, &one as *const u64 as *const libc::c_void, 8) };
        });
    }

    async fn results_task(self: Rc<Self>) {
        let mut v = Box::new(0u64);
        while !self.stop.load(Ordering::Acquire) {
            let sqe = io_uring::opcode::Read::new(io_uring::types::Fd(self.wake_efd), &mut *v as *mut u64 as *mut u8, 8).build();
            let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
            while let Ok((i, epoch, r)) = self.results_rx.try_recv() {
                self.connecting.borrow_mut()[i] = false;
                let current = self.ctrls.paths[i].epoch.load(Ordering::Acquire);
                match r {
                    Ok((stream, maxh2c)) if epoch == current => {
                        let qsize = *self.ctrls.paths[i].max_qsize.lock().unwrap();
                        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                        self.install(i, epoch, stream, maxh2c, qsize);
                    }
                    Ok(_) => log::debug!("q{} path {i}: connected to a controller that was since replaced; retrying", self.qid),
                    Err(e) => {
                        let mut b = self.backoff.borrow_mut();
                        log::debug!("q{} path {i}: I/O queue connect failed: {e:#}", self.qid);
                        self.next_try.borrow_mut()[i] = Instant::now() + b[i];
                        b[i] = (b[i] * 2).min(Duration::from_secs(2));
                    }
                }
            }
        }
    }

    fn expire_parked(&self) {
        if self.cfg.no_path_timeout.is_zero() || self.parked.borrow().is_empty() {
            return;
        }
        let expired: Vec<Pending> = {
            let mut q = self.parked.borrow_mut();
            let (old, keep): (VecDeque<Pending>, VecDeque<Pending>) = q.drain(..).partition(|p| p.first.elapsed() > self.cfg.no_path_timeout);
            *q = keep;
            old.into_iter().collect()
        };
        for p in expired {
            self.stats.no_path_eio.fetch_add(1, Ordering::Relaxed);
            p.finish(-libc::EIO);
        }
    }

    /// `echo "kill N" > /run/nvmeublk-fault` fails path N's I/O queue on every
    /// ublk queue; `stall N` stops that queue's receiver from reading so the
    /// stall watchdog must catch it. Each queue thread consumes its own copy.
    fn fault_injection(&self) {
        let path = format!("/run/nvmeublk-fault.q{}", self.qid);
        let global = "/run/nvmeublk-fault";
        if let Ok(cmd) = std::fs::read_to_string(global) {
            // Fan the command out to one file per queue, then drop the original.
            let n = std::fs::read_to_string("/run/nvmeublk-queues").ok().and_then(|s| s.trim().parse::<u16>().ok()).unwrap_or(1);
            for q in 0..n {
                let _ = std::fs::write(format!("/run/nvmeublk-fault.q{q}"), &cmd);
            }
            let _ = std::fs::remove_file(global);
        }
        let Ok(cmd) = std::fs::read_to_string(&path) else { return };
        let _ = std::fs::remove_file(&path);
        let mut it = cmd.split_whitespace();
        let (Some(verb), Some(Ok(i))) = (it.next(), it.next().map(str::parse::<usize>)) else { return };
        let Some(Some(c)) = self.conns.borrow().get(i).cloned() else { return };
        match verb {
            "kill" => self.fail_conn(&c, "fault injection: kill"),
            "stall" => {
                log::warn!("q{} fault injection: path {i} goes silent", self.qid);
                c.stalled.set(true);
            }
            _ => {}
        }
    }
}
