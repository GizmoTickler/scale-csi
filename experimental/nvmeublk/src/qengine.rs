//! io_uring-native data path. Each ublk queue thread owns one NVMe/TCP I/O
//! queue per path and drives every socket operation (batched writev, recv,
//! timers) as SQEs on the ublk queue's own io_uring. An I/O never leaves its
//! queue thread: submit, network and completion are all one executor, so
//! there is no sender/receiver thread hop and no cross-thread wakeup.
//!
//! Blocking work that cannot be an SQE (TCP connect + NVMe Connect handshake
//! on reconnect) runs on a helper thread; only the finished socket comes back.
//!
//! Failover rules:
//! - Reads move to another path at once: a late reply on the dead path can
//!   no longer reach the request (the socket is shut down first).
//! - Writes and flushes are *fenced*: held for `write_fence` before they are
//!   re-sent. The old target may still execute the original; re-sending at
//!   once would let that stale write land after a newer write to the same
//!   LBA that was acknowledged on another path. The fence is sized to the
//!   keep-alive timeout plus a quiesce margin, the NVMe-oF bound on how long
//!   a target keeps a controller (and its commands) after losing the host.
//! - Any data-path failure tears down the path's whole controller, on every
//!   queue, rather than one I/O socket (as the kernel resets a controller).

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

/// Largest staging receive. Small PDUs still batch several per recv; a large
/// C2HData payload lands in staging only for its first bytes, and the rest is
/// received straight into the request buffer (see `try_direct`), so a 128K
/// read is not copied twice in user space.
const RX_CHUNK_DEFAULT: usize = 32 * 1024;

/// io_uring UAPI: send/recv on a registered (fixed) buffer, index in buf_index.
const IORING_RECVSEND_FIXED_BUF: u16 = 1 << 2;

/// Zero-copy receive primitive: 0 = fixed-buffer RECV with MSG_WAITALL
/// (kernel 7.x; one SQE per payload), 1 = READ_FIXED on the socket (every
/// kernel with AUTO_BUF_REG; may take several SQEs). Starts from the
/// configured preference and drops to READ_FIXED the first time the kernel
/// refuses a fixed-buffer RECV with EINVAL.
static ZC_RECV_MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);

pub fn set_zc_recv_preference(recv: bool) {
    ZC_RECV_MODE.store(if recv { 0 } else { 1 }, Ordering::Relaxed);
}


/// Wait until `efd` (an eventfd) is readable, then drain it. POLL_ADD always
/// arms a poll; a READ SQE on a non-blocking eventfd returns -EAGAIN at once
/// on kernels that honour O_NONBLOCK for io_uring reads, which would spin.
async fn wait_eventfd(efd: i32) {
    let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(efd), libc::POLLIN as u32).build();
    let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
    let mut v = 0u64;
    unsafe { libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8) };
}

/// Write `len` bytes at `data` into a ublk request at copy position `pos`.
fn ucopy_write(cdev_fd: i32, pos: u64, data: *const u8, len: usize) -> Result<(), i32> {
    let mut done = 0usize;
    while done < len {
        let n = unsafe { libc::pwrite(cdev_fd, data.add(done) as *const libc::c_void, len - done, (pos + done as u64) as libc::off_t) };
        if n > 0 {
            done += n as usize;
        } else {
            let e = if n == 0 { libc::EIO } else { std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO) };
            if e != libc::EINTR {
                return Err(e);
            }
        }
    }
    Ok(())
}

/// Receives the bulk of a large C2HData payload on its own thread, straight
/// into the request buffer, while the queue thread keeps serving the other
/// paths. On a 6.12 kernel a read costs two copies (socket -> buffer, buffer
/// -> bio pages at commit); with both on the queue thread one stream is
/// capped at one core. This puts the first copy on another core for large
/// transfers only, where the handoff latency is noise.
struct RxHelper {
    jobs: std::sync::mpsc::Sender<(i32, usize, usize, i32, u64)>,
    efd: i32,
    result: Arc<std::sync::atomic::AtomicI64>,
}

impl RxHelper {
    fn spawn(name: String) -> Option<Self> {
        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if efd < 0 {
            return None;
        }
        let (jobs, rx) = std::sync::mpsc::channel::<(i32, usize, usize, i32, u64)>();
        let result = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let res = result.clone();
        let spawned = std::thread::Builder::new().name(name).spawn(move || {
            // libublk pins the queue thread to its blk-mq CPU group and a
            // spawned thread inherits that mask; the helper exists to run on
            // a different core, so let it use any CPU.
            unsafe {
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                for cpu in 0..(libc::sysconf(libc::_SC_NPROCESSORS_CONF).max(1) as usize).min(libc::CPU_SETSIZE as usize) {
                    libc::CPU_SET(cpu, &mut set);
                }
                libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
            }
            for (fd, ptr, len, cdev, pos) in rx {
                let mut got = 0usize;
                let r = loop {
                    if got == len {
                        break got as i64;
                    }
                    let n = unsafe { libc::recv(fd, (ptr + got) as *mut libc::c_void, len - got, libc::MSG_WAITALL) };
                    if n > 0 {
                        got += n as usize;
                    } else if n == 0 {
                        break got as i64; // peer closed: short
                    } else {
                        let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO);
                        if e != libc::EINTR {
                            break if got > 0 { got as i64 } else { -(e as i64) };
                        }
                    }
                };
                // Hand the bytes to the ublk request from this core, while they
                // are still in its cache (USER_COPY). A failed copy reports as
                // a failed receive: the request must not complete.
                let r = if r == len as i64 && cdev >= 0 {
                    match ucopy_write(cdev, pos, ptr as *const u8, len) {
                        Ok(()) => r,
                        Err(e) => -(e as i64),
                    }
                } else {
                    r
                };
                res.store(r, Ordering::Release);
                let one: u64 = 1;
                unsafe { libc::write(efd, &one as *const u64 as *const libc::c_void, 8) };
            }
            unsafe { libc::close(efd) };
        });
        spawned.ok().map(|_| RxHelper { jobs, efd, result })
    }
}

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
    pub fenced: AtomicU64,
    pub no_path_eio: AtomicU64,
    pub reconnects: AtomicU64,
    pub stall_kills: AtomicU64,
    pub epoch_kills: AtomicU64,
    pub path_errors: AtomicU64,
    pub protocol_errors: AtomicU64,
    /// Latency split: capsule on the wire -> response, and ublk submit -> response.
    pub done: AtomicU64,
    pub wire_ns: AtomicU64,
    pub total_ns: AtomicU64,
    pub direct_rx: AtomicU64,
    /// Read payload bytes received straight into ublk request pages.
    pub zc_bytes: AtomicU64,
    pub zc_rx_ops: AtomicU64,
    /// Write payload bytes sent straight from ublk request pages.
    pub zc_tx_bytes: AtomicU64,
    /// Stage split (ns sums): capsule queued -> its Writev done; Writev done ->
    /// first C2H byte (reads); first byte -> completion (reads).
    pub q2w_ns: AtomicU64,
    pub q2w_n: AtomicU64,
    pub w2d_ns: AtomicU64,
    pub d2c_ns: AtomicU64,
    pub rd_n: AtomicU64,
    pub loops: AtomicU64,
    pub loop_ns: AtomicU64,
}

#[derive(Clone)]
pub struct QConfig {
    pub io_timeout: Duration,
    pub no_path_timeout: Duration,
    pub max_attempts: u32,
    /// How long a write or flush orphaned by a failed path waits before it
    /// may be re-sent elsewhere.
    pub write_fence: Duration,
    /// After a crash recovery, hold every write until this instant: the dead
    /// daemon's writes may still be executing on the target, and the kernel
    /// reissues them to us.
    pub hold_writes_until: Option<Instant>,
    /// Payload remainders at least this large are received on the
    /// connection's helper thread; 0 disables the offload.
    pub rx_offload: usize,
    /// /dev/ublkcN when the device runs with UBLK_F_USER_COPY (-1: off).
    /// Read data is then written into the ublk request with pwrite at the
    /// tag's copy position, by whichever thread received it; the kernel does
    /// no copy at commit.
    pub cdev_fd: i32,
    /// NVMe I/O queues (TCP connections) per path for each ublk queue. A
    /// connection receives one PDU at a time; more of them let one ublk
    /// queue overlap its per-PDU handoffs on a large-read stream.
    pub conns_per_path: usize,
    /// Largest staging receive (see RX_CHUNK_DEFAULT).
    pub rx_chunk: usize,
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
    /// Read payload bytes received for the current attempt, in order.
    pub rx: usize,
    /// H2CData PDUs queued to the sender and not yet written.
    pub h2c_queued: u32,
    /// Write payload bytes the target has asked for (R2T) or received
    /// in-capsule, in order. A write succeeds only when this covers it all.
    pub tx_cov: usize,
    /// A success that arrived while data PDUs were still queued: completed
    /// once the sender has written them, so the buffer is really released.
    pub deferred_sc: Option<u16>,
    /// Path that last failed this request with a path error; the retry goes
    /// elsewhere if any other path is live.
    pub avoid_path: Option<usize>,
    /// Stage timestamps for the latency split.
    pub wired: Option<Instant>,
    pub first_data: Option<Instant>,
    /// USER_COPY position of this request's buffer in /dev/ublkcN.
    pub ucopy: Option<u64>,
    /// Zero copy (UBLK_F_AUTO_BUF_REG): the request's own pages are
    /// registered in this queue ring's buffer table at this index, so read
    /// payload is received from the socket straight into them.
    pub zc_index: Option<u16>,
}

impl Pending {
    pub fn new(op: Op, slba: u64, nlb: u32, buf: *mut u8, len: usize, done: Sender<i32>, ucopy: Option<u64>, zc_index: Option<u16>) -> Self {
        let now = Instant::now();
        Pending { op, slba, nlb, buf, len, done, first: now, sent: now, attempts: 0, rx: 0, h2c_queued: 0, tx_cov: 0, deferred_sc: None, avoid_path: None, wired: None, first_data: None, ucopy, zc_index }
    }
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
    cid: u16,
    h2c: bool,
    queued: Instant,
    /// Zero copy: the payload is `len` bytes at this offset of the ring's
    /// registered buffer `index` (the ublk request's own pages), not `data`.
    fixed: Option<(u16, usize)>,
}

struct QConn {
    path: usize,
    /// Index into the engine's connection slots (path * conns_per_path + k).
    slot: usize,
    epoch: u64,
    stream: TcpStream,
    fd: i32,
    maxh2c: usize,
    dead: Cell<bool>,
    /// Fault injection: the receiver stops reading (no socket error), so
    /// in-flight commands hang until the stall watchdog fails the path.
    stalled: Cell<bool>,
    inflight: RefCell<HashMap<u16, Pending>>,
    /// Free command slots (1..qsize). The wire cid is slot | generation << 8,
    /// and the generation moves on every reuse, so a stale or duplicated
    /// completion from the target cannot match the slot's next command.
    free: RefCell<Vec<u16>>,
    generation: RefCell<Vec<u8>>,
    tx: Sender<OutMsg>,
    /// cid whose C2H payload is being received straight into its buffer.
    /// That request must not be resubmitted while the Recv is in flight, or
    /// a late write could land in a buffer another path already completed.
    rx_direct: Cell<Option<u16>>,
    /// Where fail_conn parks the rx_direct request; the receiver resubmits
    /// it once its Recv has returned.
    held: RefCell<Option<Pending>>,
    /// Bulk receive thread for large payloads (None: offload disabled).
    helper: Option<RxHelper>,
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

/// Why a connection is being dropped; decides whether the path's controller
/// is torn down with it.
#[derive(Clone, Copy, PartialEq)]
enum Cause {
    /// The data path saw it fail (socket error, stall, protocol violation,
    /// injected fault): fence the whole controller.
    Failure,
    /// The controller was already replaced, or we are shutting down.
    Retired,
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
    /// Writes/flushes waiting out the write fence: (release time, request).
    fenced: RefCell<Vec<(Instant, Pending)>>,
    exe: Rc<smol::LocalExecutor<'static>>,
    results_tx: mpsc::Sender<ConnectResult>,
    results_rx: mpsc::Receiver<ConnectResult>,
    wake_efd: i32,
    pub stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    /// Set by the shutdown handler: parked and fenced I/O fails with EIO so
    /// the device can be deleted instead of waiting on paths that are gone.
    draining: Arc<AtomicBool>,
}

fn io_sqe_res(r: Result<i32, libublk::UblkError>) -> i32 {
    r.unwrap_or(-libc::EIO)
}

impl QEngine {
    pub fn new(
        qid: u16,
        ctrls: Arc<Ctrls>,
        cfg: QConfig,
        exe: Rc<smol::LocalExecutor<'static>>,
        stats: Arc<Stats>,
        stop: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
    ) -> Rc<Self> {
        let n = ctrls.paths.len() * cfg.conns_per_path.max(1);
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
            fenced: RefCell::new(Vec::new()),
            exe,
            results_tx,
            results_rx,
            wake_efd,
            stats,
            stop,
            draining,
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

    /// Largest write sent inside the command capsule.
    pub fn incapsule(&self) -> usize {
        self.incapsule
    }

    fn live(&self) -> Vec<Rc<QConn>> {
        self.conns.borrow().iter().flatten().filter(|c| !c.dead.get()).cloned().collect()
    }

    /// Entry point for new block requests from ublk.
    pub fn submit(&self, p: Pending) {
        if p.op != Op::Read {
            if let Some(t) = self.cfg.hold_writes_until {
                if Instant::now() < t {
                    self.fence(p, t);
                    return;
                }
            }
        }
        self.dispatch(p);
    }

    /// Send to the live connection with the fewest outstanding commands;
    /// park if none has a free slot.
    fn dispatch(&self, mut p: Pending) {
        let mut live = self.live();
        if let Some(ap) = p.avoid_path {
            if live.iter().any(|c| c.path != ap) {
                live.retain(|c| c.path != ap);
            }
        }
        live.sort_by_key(|c| c.inflight.borrow().len());
        for c in live {
            match self.try_submit(&c, p) {
                Ok(()) => return,
                Err(back) => p = back,
            }
        }
        if self.draining.load(Ordering::Acquire) {
            p.finish(-libc::EIO);
            return;
        }
        self.stats.parked.fetch_add(1, Ordering::Relaxed);
        self.parked.borrow_mut().push_back(p);
    }

    fn try_submit(&self, c: &Rc<QConn>, mut p: Pending) -> Result<(), Pending> {
        let Some(slot) = c.free.borrow_mut().pop() else { return Err(p) };
        let cid = slot | (c.generation.borrow()[slot as usize] as u16) << 8;
        let inline = p.op == Op::Write && p.len <= self.incapsule;
        let sqe = match p.op {
            Op::Read => rw_cmd(OPC_READ, cid, NSID, p.slba, p.nlb, p.len as u32, false),
            Op::Write => rw_cmd(OPC_WRITE, cid, NSID, p.slba, p.nlb, p.len as u32, inline),
            Op::Flush => flush_cmd(cid, NSID),
        };
        let (data, len) = if inline { (p.buf as *const u8, p.len) } else { (std::ptr::null(), 0) };
        let head = capsule_header(&sqe, len);
        p.sent = Instant::now();
        p.rx = 0;
        p.h2c_queued = 0;
        p.tx_cov = if inline { p.len } else { 0 };
        p.deferred_sc = None;
        p.wired = None;
        p.first_data = None;
        c.inflight.borrow_mut().insert(cid, p);
        if c.tx.try_send(OutMsg { head, data, len, cid, h2c: false, queued: Instant::now(), fixed: None }).is_err() {
            let p = c.inflight.borrow_mut().remove(&cid).expect("just inserted");
            self.free_cid(c, cid);
            return Err(p);
        }
        Ok(())
    }

    /// Put `len` received bytes at `off` of request `p`: into the ublk
    /// request directly (USER_COPY), or into the tag buffer the kernel copies
    /// from at commit.
    fn deliver(&self, p: &Pending, off: usize, data: *const u8, len: usize) -> Result<(), String> {
        match p.ucopy {
            Some(pos) => ucopy_write(self.cfg.cdev_fd, pos + off as u64, data, len).map_err(|e| format!("copy into ublk request failed: errno {e}")),
            None => {
                unsafe { std::ptr::copy_nonoverlapping(data, p.buf.add(off), len) };
                Ok(())
            }
        }
    }

    fn free_cid(&self, c: &QConn, cid: u16) {
        let slot = cid & 0xff;
        let mut g = c.generation.borrow_mut();
        g[slot as usize] = g[slot as usize].wrapping_add(1);
        c.free.borrow_mut().push(slot);
    }

    /// A completion freed a slot: give it to the oldest parked request.
    fn kick_parked(&self) {
        let next = self.parked.borrow_mut().pop_front();
        if let Some(p) = next {
            self.dispatch(p);
        }
    }

    fn resubmit(&self, mut p: Pending) {
        p.attempts += 1;
        if p.attempts > self.cfg.max_attempts {
            log::error!("q{} {:?} slba {}: giving up after {} attempts", self.qid, p.op, p.slba, p.attempts - 1);
            p.finish(-libc::EIO);
            return;
        }
        self.stats.resubmits.fetch_add(1, Ordering::Relaxed);
        self.dispatch(p);
    }

    fn fence(&self, p: Pending, until: Instant) {
        self.stats.fenced.fetch_add(1, Ordering::Relaxed);
        self.fenced.borrow_mut().push((until, p));
    }

    /// Move a request off a path that failed it.
    fn failover(&self, p: Pending) {
        if self.stop.load(Ordering::Acquire) || self.draining.load(Ordering::Acquire) {
            p.finish(-libc::EIO);
        } else if p.op == Op::Read {
            self.resubmit(p);
        } else {
            self.fence(p, Instant::now() + self.cfg.write_fence);
        }
    }

    /// Tear a connection down and move its in-flight commands elsewhere.
    fn fail_conn(&self, c: &Rc<QConn>, why: &str, cause: Cause) {
        if c.dead.replace(true) {
            return;
        }
        // Shut the socket before anything is resubmitted: a Recv or Writev
        // still queued on it then fails instead of touching a request buffer.
        let _ = c.stream.shutdown(Shutdown::Both);
        c.tx.close();
        {
            let mut conns = self.conns.borrow_mut();
            if conns[c.slot].as_ref().is_some_and(|x| Rc::ptr_eq(x, c)) {
                conns[c.slot] = None;
            }
        }
        if cause == Cause::Failure {
            self.ctrls.paths[c.path].fence(c.epoch);
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
        self.next_try.borrow_mut()[c.slot] = Instant::now();
        if orphans.is_empty() {
            log::warn!("q{} path {}: {why}", self.qid, c.path);
        } else {
            self.stats.failovers.fetch_add(1, Ordering::Relaxed);
            let writes = orphans.iter().filter(|p| p.op != Op::Read).count();
            log::warn!("q{} path {}: {why}; failing over {} in-flight ({writes} writes/flushes fenced)", self.qid, c.path, orphans.len());
        }
        for p in orphans {
            self.failover(p);
        }
    }

    /// Finish command `cid` with NVMe status `sc` (0 = success). Refuses,
    /// leaving the request in flight for fail_conn to fail over, when the
    /// completion is inconsistent with what was transferred.
    fn complete(&self, c: &QConn, cid: u16, sc: u16) -> Result<(), String> {
        {
            let inflight = c.inflight.borrow();
            let Some(p) = inflight.get(&cid) else { return Err(format!("completion for unknown cid {cid:#x}")) };
            if sc == 0 && p.op == Op::Read && p.rx != p.len {
                return Err(format!("read cid {cid:#x} completed after {} of {} bytes", p.rx, p.len));
            }
            if sc == 0 && p.op == Op::Write && p.tx_cov != p.len {
                return Err(format!("write cid {cid:#x} reported success with {} of {} bytes requested", p.tx_cov, p.len));
            }
            if sc != 0 && p.h2c_queued > 0 {
                // Its data PDUs would still go out from a buffer about to be
                // reused, and the target may have reused the transfer tag.
                return Err(format!("write cid {cid:#x} failed ({sc:#x}) with {} data PDUs still queued", p.h2c_queued));
            }
        }
        if sc == 0 {
            let mut inflight = c.inflight.borrow_mut();
            let p = inflight.get_mut(&cid).expect("checked above");
            if p.h2c_queued > 0 {
                // The target cannot have all the data yet from its point of
                // view unless our sender wrote it; wait for the sender to
                // finish so no queued PDU outlives the request's buffer.
                p.deferred_sc = Some(sc);
                return Ok(());
            }
        }
        let mut p = c.inflight.borrow_mut().remove(&cid).expect("checked above");
        self.free_cid(c, cid);
        self.stats.done.fetch_add(1, Ordering::Relaxed);
        self.stats.wire_ns.fetch_add(p.sent.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.total_ns.fetch_add(p.first.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let (Some(w), Some(d)) = (p.wired, p.first_data) {
            self.stats.rd_n.fetch_add(1, Ordering::Relaxed);
            self.stats.w2d_ns.fetch_add(d.saturating_duration_since(w).as_nanos() as u64, Ordering::Relaxed);
            self.stats.d2c_ns.fetch_add(d.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        if sc == 0 {
            let r = p.ok_res();
            p.finish(r);
        } else if is_path_error(sc) {
            self.stats.path_errors.fetch_add(1, Ordering::Relaxed);
            p.avoid_path = Some(c.path);
            log::warn!("q{} path {}: {:?} slba {} path error {sc:#x}; failing over", self.qid, c.path, p.op, p.slba);
            self.failover(p);
        } else {
            log::warn!("q{} path {}: {:?} slba {} failed status {sc:#x}", self.qid, c.path, p.op, p.slba);
            p.finish(-libc::EIO);
        }
        self.kick_parked();
        Ok(())
    }

    fn k(&self) -> usize {
        self.cfg.conns_per_path.max(1)
    }

    fn install(self: &Rc<Self>, slot: usize, epoch: u64, stream: TcpStream, maxh2c: u32, qsize: u16) {
        let path = slot / self.k();
        // Blocking fd on purpose: io_uring honours O_NONBLOCK and would hand
        // back -EAGAIN instead of arming a poll, turning the receiver into a spin.
        let _ = stream.set_nonblocking(false);
        let (tx, rx) = smol::channel::unbounded::<OutMsg>();
        let slots = qsize.min(128);
        let c = Rc::new(QConn {
            path,
            slot,
            epoch,
            fd: stream.as_raw_fd(),
            stream,
            maxh2c: maxh2c as usize,
            dead: Cell::new(false),
            stalled: Cell::new(false),
            inflight: RefCell::new(HashMap::new()),
            free: RefCell::new((1..slots).rev().collect()),
            generation: RefCell::new(vec![0; slots as usize]),
            tx,
            rx_direct: Cell::new(None),
            held: RefCell::new(None),
            helper: if self.cfg.rx_offload > 0 { RxHelper::spawn(format!("nvme-rx-q{}p{path}c{}", self.qid, slot % self.k())) } else { None },
        });
        self.conns.borrow_mut()[slot] = Some(c.clone());
        self.backoff.borrow_mut()[slot] = Duration::from_millis(250);
        let (me, c2) = (self.clone(), c.clone());
        self.exe.spawn(async move { me.sender_task(c2, rx).await }).detach();
        let (me, c2) = (self.clone(), c);
        self.exe.spawn(async move { me.receiver_task(c2).await }).detach();
        log::info!("q{} path {path} I/O queue {} up", self.qid, self.qid as usize * self.k() + slot % self.k() + 1);
        let parked: Vec<Pending> = self.parked.borrow_mut().drain(..).collect();
        for p in parked {
            self.dispatch(p);
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
            // Headers and copied payloads go out in batched writev calls; a
            // zero-copy payload is written from its registered buffer in
            // between, so the byte stream keeps the order of the batch.
            let mut iov: Vec<libc::iovec> = Vec::with_capacity(batch.len() * 2);
            for m in &batch {
                iov.push(libc::iovec { iov_base: m.head.as_ptr() as *mut _, iov_len: m.head.len() });
                if let Some((idx, off)) = m.fixed {
                    if !self.write_iov(&c, &mut iov).await || !self.write_fixed(&c, idx, off, m.len).await {
                        return;
                    }
                    iov.clear();
                } else if m.len > 0 {
                    iov.push(libc::iovec { iov_base: m.data as *mut _, iov_len: m.len });
                }
            }
            if !self.write_iov(&c, &mut iov).await {
                return;
            }
            let now = Instant::now();
            let mut deferred = Vec::new();
            let mut inflight = c.inflight.borrow_mut();
            for m in &batch {
                let Some(p) = inflight.get_mut(&m.cid) else { continue };
                if m.h2c {
                    p.h2c_queued = p.h2c_queued.saturating_sub(1);
                    if p.h2c_queued == 0 {
                        if let Some(sc) = p.deferred_sc.take() {
                            deferred.push((m.cid, sc));
                        }
                    }
                } else {
                    p.wired = Some(now);
                    self.stats.q2w_ns.fetch_add((now - m.queued).as_nanos() as u64, Ordering::Relaxed);
                    self.stats.q2w_n.fetch_add(1, Ordering::Relaxed);
                }
            }
            drop(inflight);
            batch.clear();
            for (cid, sc) in deferred {
                if let Err(e) = self.complete(&c, cid, sc) {
                    self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed);
                    self.fail_conn(&c, &e, Cause::Failure);
                    return;
                }
            }
        }
    }

    /// Write all of `iov` (headers and copied payloads). False: the
    /// connection is gone and has been failed.
    async fn write_iov(&self, c: &Rc<QConn>, iov: &mut [libc::iovec]) -> bool {
        let mut idx = 0usize;
        while idx < iov.len() {
            let n = iov.len() - idx;
            let sqe = io_uring::opcode::Writev::new(io_uring::types::Fd(c.fd), iov[idx..].as_ptr() as *const _, n.min(1024) as u32).build();
            let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
            if c.dead.get() {
                return false;
            }
            if r == -libc::EAGAIN || r == -libc::EINTR {
                continue;
            }
            if r <= 0 {
                self.fail_conn(c, &format!("send failed ({r})"), Cause::Failure);
                return false;
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
        true
    }

    /// Write `len` bytes at offset `off` of registered buffer `idx` to the
    /// socket (WRITE_FIXED: the generic write path imports the kernel buffer
    /// on every kernel with ublk AUTO_BUF_REG; a fixed-buffer SEND is refused
    /// before 7.x).
    async fn write_fixed(&self, c: &Rc<QConn>, idx: u16, off: usize, len: usize) -> bool {
        let mut done = 0usize;
        while done < len {
            let sqe = io_uring::opcode::WriteFixed::new(io_uring::types::Fd(c.fd), (off + done) as *const u8, (len - done) as u32, idx)
                .offset(u64::MAX)
                .build();
            let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
            if c.dead.get() {
                return false;
            }
            if r == -libc::EAGAIN || r == -libc::EINTR {
                continue;
            }
            if r <= 0 {
                self.fail_conn(c, &format!("zero-copy send failed ({r})"), Cause::Failure);
                return false;
            }
            done += r as usize;
            self.stats.zc_tx_bytes.fetch_add(r as u64, Ordering::Relaxed);
        }
        true
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
            // Keep at least one chunk of room: slide the unparsed tail down,
            // and grow only when a single PDU needs more than the buffer.
            let chunk = if self.cfg.rx_chunk == 0 { RX_CHUNK_DEFAULT } else { self.cfg.rx_chunk };
            if buf.len() - end < chunk && start > 0 {
                buf.copy_within(start..end, 0);
                end -= start;
                start = 0;
            }
            if end == buf.len() {
                buf.resize(buf.len() * 2, 0);
            }
            let want = (buf.len() - end).min(chunk);
            let sqe = io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), buf[end..].as_mut_ptr(), want as u32).build();
            let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
            if c.dead.get() {
                return;
            }
            if r == -libc::EAGAIN || r == -libc::EINTR {
                continue;
            }
            if r <= 0 {
                self.fail_conn(&c, if r == 0 { "connection closed" } else { "receive failed" }, Cause::Failure);
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
                    self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed);
                    self.fail_conn(&c, &format!("malformed PDU length {plen}"), Cause::Failure);
                    return;
                }
                if end - start < plen {
                    if plen > buf.len() {
                        buf.resize(plen.next_power_of_two(), 0);
                    }
                    break;
                }
                if let Err(e) = self.handle_pdu(&c, &buf[start..start + plen]) {
                    self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed);
                    self.fail_conn(&c, &e, Cause::Failure);
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
    /// request's buffer, skipping the staging copy.
    async fn try_direct(&self, c: &Rc<QConn>, part: &[u8]) -> Direct {
        if part.len() < CH_LEN || part[0] != PDU_C2H_DATA {
            return Direct::No;
        }
        let (flags, hlen, pdo) = (part[1], part[2] as usize, part[3] as usize);
        if part.len() < hlen.max(CH_LEN) || part.len() < pdo {
            return Direct::No; // header or padding not all here yet
        }
        if let Err(e) = check_pdu_header(&part[..hlen.max(CH_LEN)]) {
            self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed);
            self.fail_conn(c, &e, Cause::Failure);
            return Direct::Failed;
        }
        let h = parse_data_hdr(&part[CH_LEN..hlen]);
        let (off, len) = (h.off as usize, h.len as usize);
        let have = part.len() - pdo;
        let (dest, ucopy, zc_index) = {
            let mut inflight = c.inflight.borrow_mut();
            match inflight.get_mut(&h.cid) {
                Some(p) if p.op == Op::Read && off == p.rx && off + len <= p.len => {
                    p.first_data.get_or_insert_with(Instant::now);
                    if let Err(e) = self.deliver(p, off, part[pdo..].as_ptr(), have) {
                        drop(inflight);
                        self.fail_conn(c, &e, Cause::Failure);
                        return Direct::Failed;
                    }
                    (p.buf.wrapping_add(off), p.ucopy.map(|pos| pos + off as u64), p.zc_index)
                }
                _ => return Direct::No, // handle_pdu reports it once whole
            }
        };
        c.rx_direct.set(Some(h.cid));
        self.stats.direct_rx.fetch_add(1, Ordering::Relaxed);
        let mut got = have;
        let mut failed = None;
        if let Some(idx) = zc_index {
            // Zero copy: socket -> the request's registered pages, at byte
            // offset off+got of the kernel buffer (its base address is 0).
            while got < len && !c.dead.get() {
                // READ_FIXED on the socket: the generic read path imports the
                // kernel-registered buffer on every kernel with ublk zero copy
                // (a fixed-buffer RECV is refused with EINVAL before 7.x). It
                // may return short, so loop until the payload is complete.
                let use_recv = ZC_RECV_MODE.load(Ordering::Relaxed) == 0;
                let sqe = if use_recv {
                    io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), (off + got) as *mut u8, (len - got) as u32)
                        .ioprio(IORING_RECVSEND_FIXED_BUF)
                        .buf_group(idx)
                        .flags(libc::MSG_WAITALL)
                        .build()
                } else {
                    io_uring::opcode::ReadFixed::new(io_uring::types::Fd(c.fd), (off + got) as *mut u8, (len - got) as u32, idx)
                        .offset(u64::MAX)
                        .build()
                };
                let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
                if use_recv && r == -libc::EINVAL {
                    // Prep-time refusal: nothing was consumed from the socket.
                    if ZC_RECV_MODE.swap(1, Ordering::Relaxed) == 0 {
                        log::info!("fixed-buffer RECV not supported by this kernel; using READ_FIXED");
                    }
                    continue;
                }
                if c.dead.get() {
                    break;
                }
                if r == -libc::EAGAIN || r == -libc::EINTR {
                    continue;
                }
                if r <= 0 {
                    failed = Some(if r == 0 { "connection closed" } else { "zero-copy receive failed" });
                    if r < 0 {
                        log::warn!("q{}: fixed-buffer read failed: {r}", self.qid);
                    }
                    break;
                }
                got += r as usize;
            }
            self.stats.zc_bytes.fetch_add((got - have) as u64, Ordering::Relaxed);
            self.stats.zc_rx_ops.fetch_add(1, Ordering::Relaxed);
        } else if let Some(hp) = c.helper.as_ref().filter(|_| len - got >= self.cfg.rx_offload) {
            // The receiver task owns this socket's read side, and it waits
            // here, so the helper is the only reader until it reports back.
            let (cdev, pos) = match ucopy {
                Some(pos) => (self.cfg.cdev_fd, pos + got as u64),
                None => (-1, 0),
            };
            if hp.jobs.send((c.fd, dest as usize + got, len - got, cdev, pos)).is_ok() {
                wait_eventfd(hp.efd).await;
                let r = hp.result.load(Ordering::Acquire);
                if r > 0 {
                    got += r as usize;
                }
                if got < len && !c.dead.get() {
                    failed = Some(if r == 0 { "connection closed" } else { "receive failed" });
                }
            }
        }
        let ring_from = got;
        while got < len && failed.is_none() && !c.dead.get() {
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
        if got == len && got > ring_from {
            if let Some(pos) = ucopy {
                // Received on the ring into the tag buffer: copy it across.
                if let Err(e) = ucopy_write(self.cfg.cdev_fd, pos + ring_from as u64, unsafe { dest.add(ring_from) }, len - ring_from) {
                    failed = Some("copy into ublk request failed");
                    log::warn!("q{}: pwrite to ublk request failed: errno {e}", self.qid);
                    got = ring_from;
                }
            }
        }
        if got < len {
            self.fail_conn(c, failed.unwrap_or("connection lost during data receive"), Cause::Failure);
        }
        c.rx_direct.set(None);
        let held = c.held.borrow_mut().take();
        if let Some(p) = held {
            self.failover(p);
        }
        if got < len {
            return Direct::Failed;
        }
        if let Some(p) = c.inflight.borrow_mut().get_mut(&h.cid) {
            p.rx += len;
        }
        if flags & FLAG_C2H_SUCCESS != 0 {
            if let Err(e) = self.complete(c, h.cid, 0) {
                self.stats.protocol_errors.fetch_add(1, Ordering::Relaxed);
                self.fail_conn(c, &e, Cause::Failure);
                return Direct::Failed;
            }
        }
        Direct::Done
    }

    fn handle_pdu(&self, c: &QConn, pdu: &[u8]) -> Result<(), String> {
        check_pdu_header(pdu)?;
        let (ptype, flags, hlen, pdo) = (pdu[0], pdu[1], pdu[2] as usize, pdu[3] as usize);
        match ptype {
            PDU_C2H_DATA => {
                let h = parse_data_hdr(&pdu[CH_LEN..hlen]);
                let (off, len) = (h.off as usize, h.len as usize);
                {
                    let mut inflight = c.inflight.borrow_mut();
                    let Some(p) = inflight.get_mut(&h.cid) else { return Err(format!("C2HData for unknown cid {:#x}", h.cid)) };
                    if p.op != Op::Read {
                        return Err(format!("C2HData for {:?} cid {:#x}", p.op, h.cid));
                    }
                    if off != p.rx || off + len > p.len {
                        return Err(format!("C2HData out of order/range: cid {:#x} off {off} len {len}, have {} of {}", h.cid, p.rx, p.len));
                    }
                    p.first_data.get_or_insert_with(Instant::now);
                    self.deliver(p, off, pdu[pdo..pdo + len].as_ptr(), len)?;
                    p.rx += len;
                }
                if flags & FLAG_C2H_SUCCESS != 0 {
                    self.complete(c, h.cid, 0)?;
                }
            }
            PDU_R2T => {
                let h = parse_data_hdr(&pdu[CH_LEN..hlen]);
                let (off, len) = (h.off as usize, h.len as usize);
                let mut inflight = c.inflight.borrow_mut();
                let Some(p) = inflight.get_mut(&h.cid) else { return Err(format!("R2T for unknown cid {:#x}", h.cid)) };
                if p.op != Op::Write || off != p.tx_cov || off + len > p.len {
                    return Err(format!("R2T invalid: {:?} cid {:#x} off {off} len {len}, {} of {} requested so far", p.op, h.cid, p.tx_cov, p.len));
                }
                p.tx_cov += len;
                let mut sent = 0;
                while sent < len {
                    let n = (len - sent).min(c.maxh2c);
                    let head = h2c_header(h.cid, h.ttag, (off + sent) as u32, n, sent + n == len);
                    // Not dereferenced when the payload goes out zero-copy (then
                    // the tag buffer is only in-capsule sized), hence wrapping_add.
                    let data = p.buf.wrapping_add(off + sent) as *const u8;
                    // A large write in zero-copy mode was never copied into
                    // our buffer: send it from the request's registered pages.
                    let fixed = if p.op == Op::Write { p.zc_index.map(|idx| (idx, off + sent)) } else { None };
                    if c.tx.try_send(OutMsg { head, data, len: n, cid: h.cid, h2c: true, queued: Instant::now(), fixed }).is_err() {
                        return Err("sender gone while answering R2T".into());
                    }
                    p.h2c_queued += 1;
                    sent += n;
                }
            }
            PDU_CAPSULE_RESP => {
                let cqe = Cqe::parse(&pdu[CH_LEN..CH_LEN + 16]);
                self.complete(c, cqe.cid, cqe.sc())?;
            }
            PDU_C2H_TERM => return Err("target terminated the connection".into()),
            t => return Err(format!("unexpected PDU type {t:#x}")),
        }
        Ok(())
    }

    /// 100ms housekeeping: reconnect, stale-controller and stall detection,
    /// parked-I/O expiry, fenced-write release, and fault injection.
    async fn timer_task(self: Rc<Self>) {
        let ts = io_uring::types::Timespec::new().nsec(100_000_000);
        while !self.stop.load(Ordering::Acquire) {
            let sqe = io_uring::opcode::Timeout::new(&ts).build();
            let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
            self.fault_injection();
            for i in 0..self.ctrls.paths.len() * self.k() {
                let ctrl = &self.ctrls.paths[i / self.k()];
                let conn = self.conns.borrow()[i].clone();
                match conn {
                    Some(c) => {
                        if c.epoch != ctrl.epoch.load(Ordering::Acquire) {
                            self.stats.epoch_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "controller replaced", Cause::Retired);
                        } else if c.oldest().is_some_and(|a| a > self.cfg.io_timeout) {
                            self.stats.stall_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "request stalled past io_timeout", Cause::Failure);
                        }
                    }
                    None => self.maybe_connect(i),
                }
            }
            self.expire_parked();
            self.release_fenced();
        }
        let conns: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().cloned().collect();
        for c in conns {
            self.fail_conn(&c, "shutting down", Cause::Retired);
        }
    }

    fn maybe_connect(&self, i: usize) {
        if self.connecting.borrow()[i] || Instant::now() < self.next_try.borrow()[i] {
            return;
        }
        let ctrl = self.ctrls.paths[i / self.k()].clone();
        let Some((cntlid, epoch)) = ctrl.snapshot() else { return };
        let qsize = *ctrl.max_qsize.lock().unwrap();
        self.connecting.borrow_mut()[i] = true;
        let qid = (self.qid as usize * self.k() + i % self.k() + 1) as u16;
        let (tx, efd, id) = (self.results_tx.clone(), self.wake_efd, self.ctrls.id.clone());
        std::thread::spawn(move || {
            let r = connect_io_queue(ctrl.addr, &id, cntlid, qid, qsize);
            let _ = tx.send((i, epoch, r));
            let one: u64 = 1;
            unsafe { libc::write(efd, &one as *const u64 as *const libc::c_void, 8) };
        });
    }

    async fn results_task(self: Rc<Self>) {
        while !self.stop.load(Ordering::Acquire) {
            wait_eventfd(self.wake_efd).await;
            while let Ok((i, epoch, r)) = self.results_rx.try_recv() {
                self.connecting.borrow_mut()[i] = false;
                let current = self.ctrls.paths[i / self.k()].epoch.load(Ordering::Acquire);
                match r {
                    Ok((stream, maxh2c)) if epoch == current => {
                        let qsize = *self.ctrls.paths[i / self.k()].max_qsize.lock().unwrap();
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
        if self.draining.load(Ordering::Acquire) {
            let all: Vec<Pending> = self.parked.borrow_mut().drain(..).collect();
            for p in all {
                p.finish(-libc::EIO);
            }
            return;
        }
        if self.cfg.no_path_timeout.is_zero() || self.parked.borrow().is_empty() {
            return;
        }
        // Only requests parked for lack of a path expire; a queue that is
        // merely full keeps its requests (they move on as slots free up).
        if !self.live().is_empty() {
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

    fn release_fenced(&self) {
        if self.fenced.borrow().is_empty() {
            return;
        }
        let draining = self.draining.load(Ordering::Acquire);
        let now = Instant::now();
        let due: Vec<Pending> = {
            let mut f = self.fenced.borrow_mut();
            let (due, keep): (Vec<_>, Vec<_>) = f.drain(..).partition(|(t, _)| draining || *t <= now);
            *f = keep;
            due.into_iter().map(|(_, p)| p).collect()
        };
        for p in due {
            if draining {
                p.finish(-libc::EIO);
            } else {
                self.resubmit(p);
            }
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
        let targets: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().filter(|c| c.path == i).cloned().collect();
        for c in targets {
            match verb {
                "kill" => self.fail_conn(&c, "fault injection: kill", Cause::Failure),
                "stall" => {
                    log::warn!("q{} fault injection: path {i} goes silent", self.qid);
                    c.stalled.set(true);
                }
                _ => {}
            }
        }
    }
}
