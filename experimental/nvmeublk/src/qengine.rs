//! io_uring-native data path. Each ublk queue thread owns one NVMe/TCP I/O
//! queue per path and drives every socket operation (batched writev, recv,
//! timers) as SQEs on the ublk queue's own io_uring. An I/O never leaves its
//! queue thread: submit, network and completion are all one executor, so
//! there is no sender/receiver thread hop and no cross-thread wakeup.
//!
//! Connecting an I/O queue (socket, TCP connect, ICReq and NVMe Connect) is
//! SQEs on the same ring too, driven by a task of the engine: no thread is
//! spawned for it, however many queues reconnect at once.
//!
//! Lifetime: the engine's own tasks keep it alive (each holds an
//! `Rc<Engine>`, and the engine holds their executor). The queue thread holds
//! a `QEngine` handle instead; dropping the last handle shuts the engine down
//! (closes its connections, lets its tasks end on the ring) so that it, its
//! sockets and its buffers are freed with the queue. A task that panics fails
//! the engine loudly (see `spawn_task`) instead of disappearing silently.
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

use crate::conn::{check_io_connect_resp, icreq_pdu, icresp_maxh2c, io_connect_capsule, tune_socket};
use crate::ctrls::Ctrls;
use crate::pdu::*;
use anyhow::{anyhow, bail, Context as _};
use libublk::uring_async::ublk_submit_sqe_async;
use libublk::UblkUringData;
use smol::channel::{Receiver, Sender};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

pub const NSID: u32 = 1;

/// Staging buffer each connection starts with (and returns to after an
/// oversized staged PDU): room for one RX_CHUNK receive plus a partial PDU.
const RX_STAGING_BASE: usize = 64 * 1024;

/// Length of the PDU at the front of `part` once its header (and data
/// offset) is complete; None while more header bytes are needed.
fn staged_pdu_len(part: &[u8]) -> Option<usize> {
    if part.len() < CH_LEN {
        return None;
    }
    let (hlen, pdo) = (part[2] as usize, part[3] as usize);
    if part.len() < hlen.max(pdo).max(CH_LEN) {
        return None;
    }
    Some(u32::from_le_bytes(part[4..8].try_into().unwrap()) as usize)
}

/// Largest staging receive. Small PDUs still batch several per recv; a large
/// C2HData payload lands in staging only for its first bytes, and the rest is
/// received straight into the request buffer (see `try_direct`), so a 128K
/// read is not copied twice in user space.
const RX_CHUNK_DEFAULT: usize = 32 * 1024;

/// io_uring UAPI: send/recv on a registered (fixed) buffer, index in buf_index.
const IORING_RECVSEND_FIXED_BUF: u16 = 1 << 2;

static ASYNC_RX_MIN: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_ASYNC_RX_MIN").ok().and_then(|v| v.parse().ok()).unwrap_or(0));

static DIRECT_SEND: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_DIRECT_SEND").map_or(true, |v| v != "0"));

pub static SEND_ZC: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_SEND_ZC").is_ok_and(|v| v != "0"));

static LINK_HDR: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_LINK_HDR").map_or(true, |v| v != "0"));

/// Per-I/O trace (NVMEUBLK_TRACE_DIR, diagnostics): one line per completed
/// request, "local_port cid sent wired first_data done" in CLOCK_REALTIME
/// nanoseconds (0 = not recorded), to join with a packet capture.
static TRACE_DIR: std::sync::LazyLock<Option<String>> = std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_TRACE_DIR").ok().filter(|d| !d.is_empty()));

thread_local! {
    static TRACE: RefCell<Option<(std::io::BufWriter<std::fs::File>, Instant, u128, u64)>> = const { RefCell::new(None) };
}

fn trace_io(fd: i32, cid: u16, p: &Pending) {
    let Some(dir) = TRACE_DIR.as_ref() else { return };
    let port = unsafe {
        let mut sa: libc::sockaddr_in = std::mem::zeroed();
        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        libc::getsockname(fd, &mut sa as *mut _ as *mut libc::sockaddr, &mut len);
        u16::from_be(sa.sin_port)
    };
    TRACE.with(|t| {
        let mut t = t.borrow_mut();
        if t.is_none() {
            let tid = unsafe { libc::gettid() };
            let Ok(f) = std::fs::File::create(format!("{dir}/io-{tid}.txt")) else { return };
            let rt = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
            *t = Some((std::io::BufWriter::new(f), Instant::now(), rt, 0));
        }
        let (w, base, rt, n) = t.as_mut().unwrap();
        let ns = |i: Option<Instant>| i.map_or(0, |i| if i >= *base { *rt + (i - *base).as_nanos() } else { rt.saturating_sub((*base - i).as_nanos()) });
        use std::io::Write;
        let _ = writeln!(w, "{port} {cid} {} {} {} {}", ns(Some(p.sent)), ns(p.wired), ns(p.first_data), ns(Some(Instant::now())));
        *n += 1;
        if *n % 256 == 0 {
            let _ = w.flush();
        }
    });
}

static IDLE_DISCONNECT: std::sync::LazyLock<Duration> = std::sync::LazyLock::new(|| {
    Duration::from_secs(std::env::var("NVMEUBLK_IDLE_DISCONNECT_S").ok().and_then(|v| v.parse().ok()).unwrap_or(60))
});

static RX_EXACT_MIN: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_RX_EXACT_MIN").ok().and_then(|v| v.parse().ok()).unwrap_or(0));

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
    /// Commands on the wire right now, across this device's queues.
    pub inflight: std::sync::atomic::AtomicI64,
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
    /// Of queued->wired: until the sender task picked the command up.
    pub q2s_ns: AtomicU64,
    /// Time spent awaiting writev completions, and how many.
    pub wv_ns: AtomicU64,
    pub wv_n: AtomicU64,
    /// Next-PDU header receives linked behind a payload receive.
    pub linked_hdr: AtomicU64,
    /// SEND_ZC buffer-release notifications seen.
    pub zc_notif: AtomicU64,
    /// Zero-copy payload receive: header parsed -> payload in (ns, count).
    pub zc_rx_ns: AtomicU64,
    pub zc_rx_n: AtomicU64,
    pub w2d_ns: AtomicU64,
    pub d2c_ns: AtomicU64,
    pub rd_n: AtomicU64,
    pub loops: AtomicU64,
    pub loop_ns: AtomicU64,
    /// Engine tasks that panicked; the first one fails its engine.
    pub engine_panics: AtomicU64,
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
    /// NAPI busy-poll budget (us) while this queue has I/O in flight; 0 = off.
    /// Registered on the first submit after an idle period and dropped after
    /// an idle timer tick, so an idle volume costs no polling.
    pub napi_us: u32,
    /// Fault-injection directory for this device (`<dir>/fault`).
    pub fault_dir: String,
    /// Set while the daemon drains for a graceful restart: new requests park
    /// instead of going out, so in-flight work can finish.
    pub quiesce: Arc<AtomicBool>,
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

/// Bytes a linked receive takes of the next PDU: every PDU a controller sends
/// (CapsuleResp, C2HData, R2T, C2HTermReq) has at least this much header, so
/// a whole response capsule, or a data/R2T header, arrives in one receive.
const HDR_PREFETCH: usize = 24;

/// An in-flight ring receive whose result the receive loop still owes.
type PendingRx = std::pin::Pin<Box<dyn std::future::Future<Output = Result<i32, libublk::UblkError>>>>;

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
    /// No I/O for NVMEUBLK_IDLE_DISCONNECT_S: dropped to free the socket and
    /// the target's queue; reconnected on the next request.
    Idle,
    /// The queue dropped its engine (the device is going away): closed
    /// quietly, as Retired otherwise.
    Closing,
}

/// Time limits of an I/O-queue connect, as the blocking dial had them: the
/// TCP connect, then each handshake exchange (ICReq/ICResp, Connect).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a shut-down engine drives its tasks to their end before it
/// gives up and leaks itself instead (see `Engine::shutdown`).
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

/// An I/O-queue connect in flight on the queue's ring, for one slot.
#[derive(Clone, Copy)]
struct Dialing {
    /// Its socket, once created (-1 before). Shutting the socket down ends
    /// whichever SQE the connect waits on: that is how a connect is timed
    /// out or aborted.
    fd: i32,
    /// When the current step must be done; the timer aborts it after that.
    deadline: Instant,
    /// The timer aborted it for its deadline (names the error).
    expired: bool,
}

/// What an engine task does: names it in logs, and tells a panic what the
/// dead task leaves behind.
enum TaskKind {
    Timer,
    Connect(usize),
    Sender(usize),
    /// A dead receiver can no longer release the request it holds.
    Receiver(Rc<QConn>),
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Timer => write!(f, "timer"),
            TaskKind::Connect(slot) => write!(f, "connect (slot {slot})"),
            TaskKind::Sender(slot) => write!(f, "sender (slot {slot})"),
            TaskKind::Receiver(c) => write!(f, "receiver (slot {})", c.slot),
        }
    }
}

/// The queue thread's handle on its engine. The engine's tasks each hold an
/// `Rc<Engine>` and the engine holds their executor: a cycle that alone
/// would keep a detached device's engine, sockets and (mlocked) buffers
/// alive for good. Dropping the last handle breaks it (`Engine::shutdown`).
/// Drop it on the queue thread, after the queue's event loop has ended.
pub struct QEngine {
    core: Rc<Engine>,
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
        Rc::new(QEngine { core: Engine::new(qid, ctrls, cfg, exe, stats, stop, draining) })
    }

    /// Start the timer task. Connections come up on its first tick.
    pub fn start(&self) {
        self.core.start();
    }

    /// Largest write sent inside the command capsule.
    pub fn incapsule(&self) -> usize {
        self.core.incapsule
    }

    /// Commands outstanding on this queue's connections.
    pub fn inflight_here(&self) -> usize {
        self.core.inflight_here()
    }

    /// Entry point for new block requests from ublk.
    pub fn submit(&self, p: Pending) {
        self.core.submit(p);
    }
}

impl Drop for QEngine {
    fn drop(&mut self) {
        self.core.shutdown();
    }
}

struct Engine {
    qid: u16,
    ctrls: Arc<Ctrls>,
    cfg: QConfig,
    incapsule: usize,
    conns: RefCell<Vec<Option<Rc<QConn>>>>,
    /// Connects in flight, per slot.
    connecting: RefCell<Vec<Option<Dialing>>>,
    next_try: RefCell<Vec<Instant>>,
    backoff: RefCell<Vec<Duration>>,
    parked: RefCell<VecDeque<Pending>>,
    /// Writes/flushes waiting out the write fence: (release time, request).
    fenced: RefCell<Vec<(Instant, Pending)>>,
    exe: Rc<smol::LocalExecutor<'static>>,
    /// timerfd the timer task sleeps on, so `wake` can end its sleep early
    /// (-1 if none could be made: it then sleeps on a ring timeout).
    tick_fd: i32,
    pub stats: Arc<Stats>,
    stop: Arc<AtomicBool>,
    /// Set by the shutdown handler: parked and fenced I/O fails with EIO so
    /// the device can be deleted instead of waiting on paths that are gone.
    draining: Arc<AtomicBool>,
    napi_on: Cell<bool>,
    /// Requests submitted since the last timer tick (idle detection).
    submitted: Cell<u64>,
    /// Last time this queue had a request (lazy I/O connections).
    last_active: Cell<Instant>,
    /// The I/O connections were dropped for idleness (not failure): the next
    /// request reconnects at once instead of waiting for the timer.
    idle_dropped: Cell<bool>,
    /// Engine tasks spawned and not yet ended (see `spawn_task`).
    tasks: Cell<usize>,
    /// The queue dropped its handle: no new connections; tasks wind down.
    closing: Cell<bool>,
    /// An engine task panicked: every request now fails with EIO.
    failed: Cell<bool>,
}

fn io_sqe_res(r: Result<i32, libublk::UblkError>) -> i32 {
    r.unwrap_or(-libc::EIO)
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.tick_fd >= 0 {
            unsafe { libc::close(self.tick_fd) };
        }
    }
}

impl Engine {
    fn new(
        qid: u16,
        ctrls: Arc<Ctrls>,
        cfg: QConfig,
        exe: Rc<smol::LocalExecutor<'static>>,
        stats: Arc<Stats>,
        stop: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
    ) -> Rc<Self> {
        let n = ctrls.paths.len() * cfg.conns_per_path.max(1);
        let tick_fd = unsafe { libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_CLOEXEC | libc::TFD_NONBLOCK) };
        Rc::new(Engine {
            qid,
            incapsule: ctrls.info.incapsule_bytes,
            ctrls,
            cfg,
            conns: RefCell::new(vec![None; n]),
            connecting: RefCell::new(vec![None; n]),
            next_try: RefCell::new(vec![Instant::now(); n]),
            backoff: RefCell::new(vec![Duration::from_millis(250); n]),
            parked: RefCell::new(VecDeque::new()),
            fenced: RefCell::new(Vec::new()),
            exe,
            tick_fd,
            stats,
            stop,
            draining,
            napi_on: Cell::new(false),
            submitted: Cell::new(0),
            last_active: Cell::new(Instant::now()),
            idle_dropped: Cell::new(false),
            tasks: Cell::new(0),
            closing: Cell::new(false),
            failed: Cell::new(false),
        })
    }

    fn start(self: &Rc<Self>) {
        let me = self.clone();
        self.spawn_task(TaskKind::Timer, async move { me.timer_task().await });
    }

    fn live(&self) -> Vec<Rc<QConn>> {
        self.conns.borrow().iter().flatten().filter(|c| !c.dead.get()).cloned().collect()
    }

    fn set_napi(&self, on: bool) {
        if self.cfg.napi_us == 0 || self.napi_on.get() == on {
            return;
        }
        let mut napi = io_uring::types::Napi::new().set_busy_poll_timeout(self.cfg.napi_us).set_prefer_busy_poll(true);
        let r = libublk::with_task_io_ring_mut(|ring| if on { ring.submitter().register_napi(&mut napi) } else { ring.submitter().unregister_napi(&mut napi) });
        match r {
            Ok(()) => self.napi_on.set(on),
            Err(e) => log::debug!("q{}: NAPI {}: {e}", self.qid, if on { "register" } else { "unregister" }),
        }
    }

    fn inflight_here(&self) -> usize {
        self.conns.borrow().iter().flatten().map(|c| c.inflight.borrow().len()).sum()
    }

    fn submit(self: &Rc<Self>, p: Pending) {
        if self.failed.get() {
            // Never sent, so nothing can still land on the target: fail now.
            p.finish(-libc::EIO);
            return;
        }
        self.submitted.set(self.submitted.get() + 1);
        self.last_active.set(Instant::now());
        self.set_napi(true);
        // Idle-disconnected: bring the connections back now rather than on
        // the next (idle, 1 s) timer tick; the request parks until one is up.
        // Only after an idle drop: after a failure the timer's backoff and
        // the controller's fence/reconnect decide when to reconnect.
        if self.idle_dropped.get() && !self.conns.borrow().iter().any(|c| c.as_ref().is_some_and(|c| !c.dead.get())) {
            for i in 0..self.ctrls.paths.len() * self.k() {
                if self.conns.borrow()[i].is_none() {
                    self.maybe_connect(i);
                }
            }
        }
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
        if self.failed.get() {
            // A failed engine sends nothing more. Retries end here: a read
            // failed over, or a write or flush once its fence has passed.
            p.finish(-libc::EIO);
            return;
        }
        if self.cfg.quiesce.load(Ordering::Acquire) {
            self.parked.borrow_mut().push_back(p);
            return;
        }
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
        // Zero copy: in-capsule write data goes out straight from the
        // request's registered pages, right behind the capsule header (the
        // sender keeps the byte order), so there is no tag buffer to fill.
        let fixed = if inline { p.zc_index.map(|idx| (idx, 0usize)) } else { None };
        let head = capsule_header(&sqe, len);
        p.sent = Instant::now();
        p.rx = 0;
        p.h2c_queued = 0;
        p.tx_cov = if inline { p.len } else { 0 };
        p.deferred_sc = None;
        p.wired = None;
        p.first_data = None;
        c.inflight.borrow_mut().insert(cid, p);
        if c.tx.try_send(OutMsg { head, data, len, cid, h2c: false, queued: Instant::now(), fixed }).is_err() {
            let p = c.inflight.borrow_mut().remove(&cid).expect("just inserted");
            self.free_cid(c, cid);
            return Err(p);
        }
        self.stats.inflight.fetch_add(1, Ordering::Relaxed);
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
        if self.stop.load(Ordering::Acquire) || self.draining.load(Ordering::Acquire) || self.closing.get() {
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
        // A failed connection is reset rather than closed gracefully when its
        // fd goes: SO_LINGER 0 frees its send and retransmit queues at once,
        // including pages a SEND_ZC still references, instead of holding
        // them (and the requests failed over elsewhere) until TCP gives up.
        if cause == Cause::Failure {
            let lg = libc::linger { l_onoff: 1, l_linger: 0 };
            unsafe { libc::setsockopt(c.fd, libc::SOL_SOCKET, libc::SO_LINGER, &lg as *const _ as *const libc::c_void, std::mem::size_of::<libc::linger>() as u32) };
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
        let drained = c.inflight.borrow().len() as i64;
        self.stats.inflight.fetch_sub(drained, Ordering::Relaxed);
        for (cid, p) in c.inflight.borrow_mut().drain() {
            if Some(cid) == direct {
                *c.held.borrow_mut() = Some(p);
            } else {
                orphans.push(p);
            }
        }
        self.next_try.borrow_mut()[c.slot] = Instant::now();
        if orphans.is_empty() && matches!(cause, Cause::Idle | Cause::Closing) {
            log::debug!("q{} path {}: {why}", self.qid, c.path);
        } else if orphans.is_empty() {
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
        if TRACE_DIR.is_some() {
            trace_io(c.fd, cid, &p);
        }
        self.stats.inflight.fetch_sub(1, Ordering::Relaxed);
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
        self.idle_dropped.set(false);
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
        self.spawn_task(TaskKind::Sender(slot), async move { me.sender_task(c2, rx).await });
        let (me, c2) = (self.clone(), c.clone());
        self.spawn_task(TaskKind::Receiver(c), async move { me.receiver_task(c2).await });
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
            let picked = Instant::now();
            for m in batch.iter().filter(|m| !m.h2c) {
                self.stats.q2s_ns.fetch_add((picked - m.queued).as_nanos() as u64, Ordering::Relaxed);
            }
            // Headers and copied payloads go out in batched writev calls; a
            // zero-copy payload is written from its registered buffer in
            // between, so the byte stream keeps the order of the batch.
            let mut iov: Vec<libc::iovec> = Vec::with_capacity(batch.len() * 2);
            for m in &batch {
                iov.push(libc::iovec { iov_base: m.head.as_ptr() as *mut _, iov_len: m.head.len() });
                if let Some((idx, off)) = m.fixed {
                    if !self.write_iov_more(&c, &mut iov, true).await || !self.write_fixed(&c, idx, off, m.len).await {
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
        self.write_iov_more(c, iov, false).await
    }

    /// `more`: the payload follows at once (MSG_MORE on a direct send, so
    /// TCP can put the header and the start of the payload in one segment).
    async fn write_iov_more(&self, c: &Rc<QConn>, iov: &mut [libc::iovec], more: bool) -> bool {
        let mut idx = 0usize;
        let mut direct = *DIRECT_SEND;
        while idx < iov.len() {
            let n = iov.len() - idx;
            let t0 = Instant::now();
            let r = if direct {
                // Direct send (NVMEUBLK_DIRECT_SEND, default on): a command
                // queued on the ring is only issued at the next ring entry,
                // behind that batch's inline receive copies and commits
                // (~130 us at a 128K read stream). A non-blocking sendmsg
                // puts it on the wire now; the ring takes over only when
                // the socket is full.
                let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
                msg.msg_iov = iov[idx..].as_mut_ptr();
                msg.msg_iovlen = n.min(1024) as _;
                let r = unsafe { libc::sendmsg(c.fd, &msg, libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL | if more { libc::MSG_MORE } else { 0 }) };
                if r >= 0 {
                    r as i32
                } else {
                    let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO);
                    if e == libc::EAGAIN || e == libc::EWOULDBLOCK {
                        direct = false;
                        continue;
                    }
                    -e
                }
            } else {
                let sqe = io_uring::opcode::Writev::new(io_uring::types::Fd(c.fd), iov[idx..].as_ptr() as *const _, n.min(1024) as u32).build();
                io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await)
            };
            self.stats.wv_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.stats.wv_n.fetch_add(1, Ordering::Relaxed);
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
            // NVMEUBLK_SEND_ZC (tuning): send the payload with SEND_ZC from
            // the request's registered pages (the NIC reads them; no copy
            // into socket buffers). Its completion comes first; a second
            // NOTIF completion follows when the network stack releases the
            // pages, which the event loop swallows. Page lifetime past that
            // point is the kernel's: the registration holds a reference to
            // the ublk request until the last user drops it. WRITE_FIXED
            // (default) copies the payload into the socket.
            let sqe = if *SEND_ZC {
                io_uring::opcode::SendZc::new(io_uring::types::Fd(c.fd), (off + done) as *const u8, (len - done) as u32)
                    .buf_index(Some(idx))
                    .flags(libc::MSG_NOSIGNAL)
                    .build()
            } else {
                io_uring::opcode::WriteFixed::new(io_uring::types::Fd(c.fd), (off + done) as *const u8, (len - done) as u32, idx)
                    .offset(u64::MAX)
                    .build()
            };
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
        let mut buf = vec![0u8; RX_STAGING_BASE];
        let (mut start, mut end) = (0usize, 0usize);
        let pause = io_uring::types::Timespec::new().nsec(50_000_000);
        // Next-PDU header: a receive linked behind the last payload receive
        // (see try_direct) lands it in `hdr`; `pending` is that receive.
        let mut hdr = vec![0u8; HDR_PREFETCH];
        let mut pending: Option<PendingRx> = None;
        loop {
            let mut have_hdr = false;
            if let Some(h) = pending.take() {
                let r = io_sqe_res(h.await);
                if c.dead.get() {
                    return;
                }
                if r > 0 {
                    buf[end..end + r as usize].copy_from_slice(&hdr[..r as usize]);
                    end += r as usize;
                    have_hdr = true;
                } else if r != -libc::ECANCELED {
                    self.fail_conn(&c, if r == 0 { "connection closed" } else { "receive failed" }, Cause::Failure);
                    return;
                }
            }
            while c.stalled.get() && !c.dead.get() {
                let sqe = io_uring::opcode::Timeout::new(&pause).build();
                let _ = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await;
            }
            if c.dead.get() {
                return;
            }
            if !have_hdr {
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
                let mut want = (buf.len() - end).min(chunk);
                // Exact-header receive (NVMEUBLK_RX_EXACT_MIN bytes, tuning):
                // while a zero-copy read with at least that much payload still
                // to come is in flight here, take only up to the end of the next
                // PDU header, so its payload lands in the request pages whole
                // instead of partly in staging (and a pwrite to move it across).
                let exact = self.exact_need(&c, &buf[start..end]);
                if let Some(n) = exact {
                    want = want.min(n);
                }
                let sqe = io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), buf[end..].as_mut_ptr(), want as u32)
                    .flags(if exact.is_some() { libc::MSG_WAITALL } else { 0 })
                    .build();
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
            }
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
                    // Not grown here: a large C2HData payload goes straight
                    // into its request (try_direct below), so growing the
                    // staging buffer to the PDU size would only pin memory.
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
                match self.try_direct(&c, &buf[start..end], hdr.as_mut_ptr(), &mut pending).await {
                    Direct::No => {
                        // A PDU whose header is complete but that cannot be
                        // received in place must be staged whole: make room.
                        if let Some(plen) = staged_pdu_len(&buf[start..end]) {
                            if plen > buf.len() - start {
                                buf.copy_within(start..end, 0);
                                end -= start;
                                start = 0;
                                if plen > buf.len() {
                                    buf.resize(plen.next_power_of_two(), 0);
                                }
                            }
                        }
                    }
                    Direct::Done => {
                        start = 0;
                        end = 0;
                    }
                    Direct::Failed => {
                        // The connection is shut down, so a linked header
                        // receive ends now; let it finish before `hdr` goes.
                        if let Some(h) = pending.take() {
                            let _ = h.await;
                        }
                        return;
                    }
                }
            }
            if start == end {
                start = 0;
                end = 0;
                // Give back a buffer grown for an oversized staged PDU.
                if buf.len() > RX_STAGING_BASE {
                    buf.truncate(RX_STAGING_BASE);
                    buf.shrink_to_fit();
                }
            }
        }
    }

    /// Bytes still missing before the PDU at the front of `part` can be acted
    /// on without touching payload: its common header, then the rest of its
    /// header and padding for C2HData (whose payload `try_direct` receives in
    /// place), or the whole PDU otherwise. None when exact receive is off,
    /// when no large zero-copy read is outstanding on `c`, or when nothing is
    /// missing.
    fn exact_need(&self, c: &QConn, part: &[u8]) -> Option<usize> {
        let min = *RX_EXACT_MIN;
        if min == 0 {
            return None;
        }
        let large = c.inflight.borrow().values().any(|p| p.op == Op::Read && p.zc_index.is_some() && p.len - p.rx >= min);
        if !large {
            return None;
        }
        let need = if part.len() < CH_LEN {
            CH_LEN
        } else if part[0] == PDU_C2H_DATA {
            (part[2] as usize).max(part[3] as usize).max(CH_LEN)
        } else {
            u32::from_le_bytes(part[4..8].try_into().unwrap()) as usize
        };
        need.checked_sub(part.len()).filter(|&n| n > 0)
    }

    /// A partial C2HData PDU sits at the tail of the staging buffer: copy the
    /// payload bytes already here and receive the rest straight into the
    /// request's buffer, skipping the staging copy.
    async fn try_direct(&self, c: &Rc<QConn>, part: &[u8], hdr: *mut u8, pending: &mut Option<PendingRx>) -> Direct {
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
        let t_parse = Instant::now();
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
                // Large payloads (NVMEUBLK_ASYNC_RX_MIN bytes and up, tuning):
                // hand the copy to an io-wq worker instead of doing it inline
                // in this queue thread's submit, so the queue keeps turning
                // (commands out, completions back) while the data lands.
                let sqe = match *ASYNC_RX_MIN {
                    min if min > 0 && len - got >= min => sqe.flags(io_uring::squeue::Flags::ASYNC),
                    _ => sqe,
                };
                // Linked header receive (NVMEUBLK_LINK_HDR, default on with
                // exact-header receive): the next PDU's common header is read
                // as soon as this payload completes, in the same ring entry,
                // not on a later event-loop turn. Both SQEs are pushed back to
                // back (each future pushes on its first poll; nothing yields
                // in between). Only the payload is awaited here: the next PDU
                // may only come after this read completes, so the receive
                // loop takes the header receive over. A short or failed
                // payload breaks the link and the header receive consumes
                // nothing (-ECANCELED).
                let r = if use_recv && *LINK_HDR && *RX_EXACT_MIN > 0 && pending.is_none() {
                    let hsqe = io_uring::opcode::Recv::new(io_uring::types::Fd(c.fd), hdr, HDR_PREFETCH as u32).flags(libc::MSG_WAITALL).build();
                    let mut pf = Box::pin(ublk_submit_sqe_async(sqe.flags(io_uring::squeue::Flags::IO_LINK), UblkUringData::Target as u64));
                    let mut hf: PendingRx = Box::pin(ublk_submit_sqe_async(hsqe, UblkUringData::Target as u64));
                    let first = smol::future::poll_once(&mut pf).await;
                    if let Some(res) = smol::future::poll_once(&mut hf).await {
                        hf = Box::pin(async move { res });
                    }
                    *pending = Some(hf);
                    self.stats.linked_hdr.fetch_add(1, Ordering::Relaxed);
                    io_sqe_res(match first {
                        Some(r) => r,
                        None => pf.await,
                    })
                } else {
                    io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await)
                };
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
            self.stats.zc_rx_ns.fetch_add(t_parse.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.stats.zc_rx_n.fetch_add(1, Ordering::Relaxed);
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
        // 100 ms while there is work (stall watchdog, parked/fenced requests,
        // reconnects); once a second when the queue is fully idle, so an idle
        // volume costs next to nothing.
        let (busy, idle_tick) = (Duration::from_millis(100), Duration::from_secs(1));
        let mut idle = false;
        while !self.stop.load(Ordering::Acquire) && !self.closing.get() {
            self.nap(if idle { idle_tick } else { busy }).await;
            if self.closing.get() {
                break;
            }
            self.fault_injection();
            self.expire_dials();
            // Lazy I/O connections (NVMEUBLK_IDLE_DISCONNECT_S, default 60,
            // 0 = always connected): after that long without a request the
            // queue drops its I/O connections, so an idle volume holds only
            // its admin connections; submit() reconnects on demand.
            if self.submitted.get() > 0 || self.inflight_here() > 0 || !self.parked.borrow().is_empty() || !self.fenced.borrow().is_empty() {
                self.last_active.set(Instant::now());
            }
            let want_conns = IDLE_DISCONNECT.is_zero() || self.last_active.get().elapsed() < *IDLE_DISCONNECT;
            for i in 0..self.ctrls.paths.len() * self.k() {
                let ctrl = &self.ctrls.paths[i / self.k()];
                let conn = self.conns.borrow()[i].clone();
                match conn {
                    Some(c) if !want_conns && c.inflight.borrow().is_empty() => {
                        self.idle_dropped.set(true);
                        self.fail_conn(&c, "idle; I/O connection dropped", Cause::Idle);
                    }
                    Some(c) => {
                        if c.epoch != ctrl.epoch.load(Ordering::Acquire) {
                            self.stats.epoch_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "controller replaced", Cause::Retired);
                        } else if c.oldest().is_some_and(|a| a > self.cfg.io_timeout) {
                            self.stats.stall_kills.fetch_add(1, Ordering::Relaxed);
                            self.fail_conn(&c, "request stalled past io_timeout", Cause::Failure);
                        }
                    }
                    None if want_conns => self.maybe_connect(i),
                    None => {}
                }
            }
            self.expire_parked();
            self.release_fenced();
            // A failed engine never reconnects: missing connections are not
            // work that needs the fast tick.
            let all_up = self.failed.get() || !want_conns || self.conns.borrow().iter().all(|c| c.is_some());
            idle = self.submitted.replace(0) == 0 && self.inflight_here() == 0 && self.parked.borrow().is_empty() && self.fenced.borrow().is_empty() && all_up;
            if idle {
                self.set_napi(false);
            }
        }
        let conns: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().cloned().collect();
        for c in conns {
            self.fail_conn(&c, "shutting down", Cause::Retired);
        }
        self.abort_dials();
    }

    /// Start connecting slot `i`'s I/O queue, unless a connect is already on
    /// its way or the slot's backoff has not passed. The connect is a task on
    /// this queue's ring (`connect_io_queue`); `connected` takes its result.
    fn maybe_connect(self: &Rc<Self>, i: usize) {
        if self.closing.get() || self.failed.get() || self.connecting.borrow()[i].is_some() || Instant::now() < self.next_try.borrow()[i] {
            return;
        }
        let ctrl = self.ctrls.paths[i / self.k()].clone();
        let Some((cntlid, epoch)) = ctrl.snapshot() else { return };
        let qsize = *ctrl.max_qsize.lock().unwrap();
        self.connecting.borrow_mut()[i] = Some(Dialing { fd: -1, deadline: Instant::now() + CONNECT_TIMEOUT, expired: false });
        let qid = (self.qid as usize * self.k() + i % self.k() + 1) as u16;
        // Unlike the thread this used to be, spawning a task cannot fail
        // (submit() calls this on a tag's io task, which must not panic).
        let me = self.clone();
        self.spawn_task(TaskKind::Connect(i), async move {
            let r = me.connect_io_queue(i, ctrl.addr, cntlid, qid, qsize).await;
            me.connected(i, epoch, r);
        });
    }

    /// A connect ended: install the queue if its controller is still the
    /// current one, retry at once if it was replaced meanwhile, back off if
    /// the connect failed.
    fn connected(self: &Rc<Self>, i: usize, epoch: u64, r: anyhow::Result<(TcpStream, u32)>) {
        // Cleared in the same poll that closes the socket (here, as `r`
        // drops, or on connect_io_queue's error return just before): the
        // timer and aborts, which shut the listed fd down, run on this thread
        // and so never see an fd that is closed (and maybe reused).
        self.connecting.borrow_mut()[i] = None;
        if self.stop.load(Ordering::Acquire) || self.closing.get() || self.failed.get() {
            return; // a connection made meanwhile closes here
        }
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

    /// Dial, handshake and Connect I/O queue `qid` on controller `cntlid`
    /// for slot `i`, all as SQEs on this queue's ring: the same steps and
    /// bytes as the blocking conn::connect_io_queue, without its thread. The
    /// timer bounds each step (`expire_dials`): the TCP connect by
    /// CONNECT_TIMEOUT, each handshake exchange by HANDSHAKE_TIMEOUT.
    async fn connect_io_queue(&self, i: usize, addr: SocketAddr, cntlid: u16, qid: u16, qsize: u16) -> anyhow::Result<(TcpStream, u32)> {
        let domain = if addr.is_ipv4() { libc::AF_INET } else { libc::AF_INET6 };
        let ty = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
        // A blocking socket, as install() wants it; the ring's CONNECT, SEND
        // and RECV still never block the thread (they wait in a poll).
        let sqe = io_uring::opcode::Socket::new(domain, ty, 0).build();
        let mut fd = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
        if fd == -libc::EINVAL {
            // No IORING_OP_SOCKET (before 5.19); socket(2) does not block.
            fd = unsafe { libc::socket(domain, ty, 0) };
            if fd < 0 {
                fd = -errno();
            }
        }
        if fd < 0 {
            bail!("socket: {}", std::io::Error::from_raw_os_error(-fd));
        }
        // Owns the socket from here: every early return closes it.
        let stream = unsafe { TcpStream::from_raw_fd(fd) };
        tune_socket(fd)?;
        self.dial_step(i, fd, CONNECT_TIMEOUT)?;
        // The kernel copies the address when it prepares the SQE.
        let (sa, len) = sockaddr(&addr);
        let sqe = io_uring::opcode::Connect::new(io_uring::types::Fd(fd), &sa as *const _ as *const libc::sockaddr, len).build();
        let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
        if r < 0 {
            return Err(self.dial_error(i, r)).with_context(|| format!("connect {addr}"));
        }
        self.dial_step(i, fd, HANDSHAKE_TIMEOUT)?;
        send_all(fd, &icreq_pdu()).await.map_err(|r| self.dial_error(i, r)).context("send ICReq")?;
        let mut icresp = [0u8; 128];
        recv_exact(fd, &mut icresp).await.map_err(|r| self.dial_error(i, r)).context("read ICResp")?;
        let maxh2c = icresp_maxh2c(&icresp)?;
        self.dial_step(i, fd, HANDSHAKE_TIMEOUT)?;
        send_all(fd, &io_connect_capsule(&self.ctrls.id, cntlid, qid, qsize)).await.map_err(|r| self.dial_error(i, r)).context("send command capsule")?;
        // The response as read_hdr takes it off a stream: the common header,
        // then the rest of the header it announces; read_hdr then checks it.
        let mut h = vec![0u8; CH_LEN];
        recv_exact(fd, &mut h).await.map_err(|r| self.dial_error(i, r)).context("read PDU common header")?;
        h.resize((h[2] as usize).max(CH_LEN), 0);
        recv_exact(fd, &mut h[CH_LEN..]).await.map_err(|r| self.dial_error(i, r)).context("read PDU header")?;
        let (ch, psh) = read_hdr(&mut &h[..])?;
        check_io_connect_resp(qid, &ch, &psh)?;
        // Not installed if the timer shut the socket down (or the engine
        // was closed) after the last receive completed.
        self.dial_check(i)?;
        Ok((stream, maxh2c.max(4096)))
    }

    /// Refuse to go on with slot `i`'s connect once it was aborted: timed
    /// out, or the engine is closing or failed.
    fn dial_check(&self, i: usize) -> anyhow::Result<()> {
        if self.connecting.borrow()[i].is_some_and(|d| d.expired) {
            bail!("timed out");
        }
        if self.closing.get() || self.failed.get() {
            bail!("aborted: the engine is {}", if self.failed.get() { "failed" } else { "shutting down" });
        }
        Ok(())
    }

    /// Start the next step of slot `i`'s connect: its socket `fd` may now be
    /// shut down to abort it, and must be done within `limit`. Checked and
    /// armed with no await in between, so an abort cannot slip past it.
    fn dial_step(&self, i: usize, fd: i32, limit: Duration) -> anyhow::Result<()> {
        self.dial_check(i)?;
        if let Some(d) = self.connecting.borrow_mut()[i].as_mut() {
            d.fd = fd;
            d.deadline = Instant::now() + limit;
        }
        Ok(())
    }

    /// The error for a connect step that ended with `r` (0: peer closed).
    fn dial_error(&self, i: usize, r: i32) -> anyhow::Error {
        if self.connecting.borrow()[i].is_some_and(|d| d.expired) {
            anyhow!("timed out")
        } else if r == 0 {
            anyhow!("connection closed")
        } else {
            std::io::Error::from_raw_os_error(-r).into()
        }
    }

    /// Timer: abort connect steps that ran past their deadline.
    fn expire_dials(&self) {
        let now = Instant::now();
        for d in self.connecting.borrow_mut().iter_mut().flatten() {
            if d.fd >= 0 && !d.expired && now >= d.deadline {
                d.expired = true;
                unsafe { libc::shutdown(d.fd, libc::SHUT_RDWR) };
            }
        }
    }

    /// Abort every connect in flight: its pending SQE completes at once, and
    /// the connect task then ends without installing anything.
    fn abort_dials(&self) {
        for d in self.connecting.borrow().iter().flatten() {
            if d.fd >= 0 {
                unsafe { libc::shutdown(d.fd, libc::SHUT_RDWR) };
            }
        }
    }

    /// Sleep for `d` on the ring, or until `wake`. The timerfd is re-armed
    /// for each nap; without one, a ring timeout does (and `wake` cannot).
    async fn nap(&self, d: Duration) {
        let spec = libc::itimerspec {
            it_interval: libc::timespec { tv_sec: 0, tv_nsec: 0 },
            // (a zero it_value would disarm the timer instead)
            it_value: libc::timespec { tv_sec: d.as_secs() as _, tv_nsec: d.subsec_nanos().max(1) as _ },
        };
        if self.tick_fd >= 0 && unsafe { libc::timerfd_settime(self.tick_fd, 0, &spec, std::ptr::null_mut()) } == 0 {
            wait_eventfd(self.tick_fd).await; // POLLIN, then reads the expiry count
        } else {
            let ts = io_uring::types::Timespec::from(d);
            let _ = ublk_submit_sqe_async(io_uring::opcode::Timeout::new(&ts).build(), UblkUringData::Target as u64).await;
        }
    }

    /// End the timer's current nap now. Only `shutdown` needs this: it sets
    /// `closing` first, so the timer, suspended in its nap, exits at once.
    fn wake(&self) {
        let now = libc::itimerspec { it_interval: libc::timespec { tv_sec: 0, tv_nsec: 0 }, it_value: libc::timespec { tv_sec: 0, tv_nsec: 1 } };
        if self.tick_fd >= 0 {
            unsafe { libc::timerfd_settime(self.tick_fd, 0, &now, std::ptr::null_mut()) };
        }
    }

    /// Spawn an engine task. Every task goes through here, for two reasons:
    /// - It is counted, so `shutdown` knows when the last one has ended.
    /// - It runs under catch_unwind. The executor takes a task's panic as
    ///   its result, and a detached task's result goes nowhere: a panicking
    ///   timer used to end every reconnect, stall kill and fence release of
    ///   this queue in silence, leaving its I/O hung. Now the panic is
    ///   logged and fails the engine (`task_panicked`).
    fn spawn_task(self: &Rc<Self>, kind: TaskKind, fut: impl Future<Output = ()> + 'static) {
        self.tasks.set(self.tasks.get() + 1);
        self.exe.spawn(Guarded { fut: Some(Box::pin(fut)), kind: Some(kind), engine: Rc::downgrade(self) }).detach();
    }

    /// An engine task panicked (`msg`): log it loudly and fail the engine.
    /// A failed engine closes its connections and fails every request with
    /// EIO: new ones and reads at once, writes and flushes that were on the
    /// wire once their write fence has passed (never re-sent). Nothing waits
    /// forever on a task that is gone, and nothing more goes to the target.
    fn task_panicked(self: &Rc<Self>, kind: Option<TaskKind>, msg: &str) {
        self.stats.engine_panics.fetch_add(1, Ordering::Relaxed);
        let name = kind.as_ref().map_or("?".to_string(), |k| k.to_string());
        let first = !self.failed.replace(true);
        log::error!(
            "q{}: engine task {name} PANICKED: {msg}; {}",
            self.qid,
            if first { "engine failed: connections closed, its I/O now fails with EIO" } else { "engine already failed" }
        );
        if first {
            let conns: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().cloned().collect();
            for c in conns {
                self.fail_conn(&c, "engine failed", Cause::Retired);
            }
            self.abort_dials();
            let parked: Vec<Pending> = self.parked.borrow_mut().drain(..).collect();
            for p in parked {
                p.finish(-libc::EIO);
            }
        }
        match kind {
            // Only its own receiver may release the request a connection
            // holds (a Recv may be writing into it); this one is gone. Its
            // payload receive is not in flight: the task was running.
            Some(TaskKind::Receiver(c)) => {
                c.rx_direct.set(None);
                let held = c.held.borrow_mut().take();
                if let Some(p) = held {
                    self.failover(p);
                }
            }
            // The timer releases fenced writes; without it they would hang.
            // Restart it once; if it panics again, fail them now.
            Some(TaskKind::Timer) if first && !self.closing.get() => {
                let me = self.clone();
                self.spawn_task(TaskKind::Timer, async move { me.timer_task().await });
            }
            Some(TaskKind::Timer) => {
                let fenced: Vec<(Instant, Pending)> = self.fenced.borrow_mut().drain(..).collect();
                for (_, p) in fenced {
                    p.finish(-libc::EIO);
                }
            }
            _ => {}
        }
    }

    /// The queue dropped its handle: close every connection and abort every
    /// connect, then drive the engine's tasks on this thread's ring until
    /// the last has ended. They drop their `Rc<Engine>` as they end, which
    /// breaks the cycle through the executor: the engine, its sockets and
    /// buffers are then freed. A task is never dropped while an SQE of its
    /// own may still write into its memory: if some task has not ended
    /// within SHUTDOWN_DRAIN, the engine is leaked instead (with a warning).
    fn shutdown(self: &Rc<Self>) {
        if self.closing.replace(true) {
            return;
        }
        if std::thread::panicking() {
            // Unwinding: driving tasks now risks a second panic (an abort).
            std::mem::forget(self.clone());
            return;
        }
        let conns: Vec<Rc<QConn>> = self.conns.borrow().iter().flatten().cloned().collect();
        for c in conns {
            self.fail_conn(&c, "engine shut down", Cause::Closing);
        }
        self.abort_dials();
        // The queue is gone: nobody waits on these any more.
        let parked: Vec<Pending> = self.parked.borrow_mut().drain(..).collect();
        for p in parked {
            p.finish(-libc::EIO);
        }
        let fenced: Vec<(Instant, Pending)> = self.fenced.borrow_mut().drain(..).collect();
        for (_, p) in fenced {
            p.finish(-libc::EIO);
        }
        if self.napi_on.replace(false) {
            // As set_napi(false), through the accessor that cannot panic.
            with_ring(|r| r.submitter().unregister_napi(&mut io_uring::types::Napi::new()));
        }
        self.wake();
        let deadline = Instant::now() + SHUTDOWN_DRAIN;
        loop {
            while self.exe.try_tick() {}
            if self.tasks.get() == 0 {
                return;
            }
            if Instant::now() >= deadline || reap_ring(Duration::from_millis(50)).is_none() {
                log::warn!("q{}: {} engine task(s) did not end on shutdown; leaking the engine rather than freeing memory an SQE may still use", self.qid, self.tasks.get());
                std::mem::forget(self.clone());
                return;
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

    /// `echo "kill N" > <fault_dir>/fault` fails path N's I/O queue on every
    /// ublk queue; `stall N` stops that queue's receiver from reading so the
    /// stall watchdog must catch it; `panic` makes the timer task panic, so
    /// the engine must fail (EIO) instead of hanging. Each queue thread
    /// consumes its own copy.
    fn fault_injection(&self) {
        let dir = &self.cfg.fault_dir;
        let path = format!("{dir}/fault.q{}", self.qid);
        let global = format!("{dir}/fault");
        if let Ok(cmd) = std::fs::read_to_string(&global) {
            // Fan the command out to one file per queue, then drop the original.
            let n = std::fs::read_to_string(format!("{dir}/queues")).ok().and_then(|s| s.trim().parse::<u16>().ok()).unwrap_or(1);
            for q in 0..n {
                let _ = std::fs::write(format!("{dir}/fault.q{q}"), &cmd);
            }
            let _ = std::fs::remove_file(&global);
        }
        let Ok(cmd) = std::fs::read_to_string(&path) else { return };
        let _ = std::fs::remove_file(&path);
        if cmd.trim() == "panic" {
            // Drill for the task panic guard (spawn_task): the timer dies,
            // and the engine must fail loudly rather than hang its I/O.
            panic!("fault injection: q{} timer task panic", self.qid);
        }
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

/// An engine task as `spawn_task` runs it: counted, and panic-proof.
struct Guarded<F> {
    fut: Option<Pin<Box<F>>>,
    kind: Option<TaskKind>,
    engine: Weak<Engine>,
}

impl<F: Future<Output = ()>> Future for Guarded<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let Some(fut) = this.fut.as_mut() else { return Poll::Ready(()) };
        let panic = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(())) => {
                this.fut = None; // drops its Rc<Engine> before the count moves
                None
            }
            Err(payload) => {
                // Leaked, not dropped: the task stopped mid-flight, and an SQE
                // it issued may still write into its memory (a receive into
                // its buffers). Whatever it owned (a socket) leaks with it.
                std::mem::forget(this.fut.take());
                Some(panic_message(payload.as_ref()))
            }
        };
        if let Some(e) = this.engine.upgrade() {
            e.tasks.set(e.tasks.get().saturating_sub(1));
            if let Some(msg) = panic {
                e.task_panicked(this.kind.take(), &msg);
            }
        }
        this.kind = None;
        Poll::Ready(())
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| payload.downcast_ref::<String>().cloned()).unwrap_or_else(|| "(non-string panic payload)".into())
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)
}

/// Send all of `buf` on socket `fd` with SEND SQEs on this thread's ring.
/// Err: the result that ended it (0, or -errno).
async fn send_all(fd: i32, buf: &[u8]) -> Result<(), i32> {
    let mut done = 0usize;
    while done < buf.len() {
        let sqe = io_uring::opcode::Send::new(io_uring::types::Fd(fd), buf[done..].as_ptr(), (buf.len() - done) as u32).flags(libc::MSG_NOSIGNAL).build();
        let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
        if r == -libc::EAGAIN || r == -libc::EINTR {
            continue;
        }
        if r <= 0 {
            return Err(r);
        }
        done += r as usize;
    }
    Ok(())
}

/// Receive exactly `buf.len()` bytes from socket `fd` with RECV SQEs on this
/// thread's ring. Err: 0 if the peer closed first, else -errno.
async fn recv_exact(fd: i32, buf: &mut [u8]) -> Result<(), i32> {
    let mut got = 0usize;
    while got < buf.len() {
        let sqe = io_uring::opcode::Recv::new(io_uring::types::Fd(fd), buf[got..].as_mut_ptr(), (buf.len() - got) as u32).flags(libc::MSG_WAITALL).build();
        let r = io_sqe_res(ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await);
        if r == -libc::EAGAIN || r == -libc::EINTR {
            continue;
        }
        if r <= 0 {
            return Err(r);
        }
        got += r as usize;
    }
    Ok(())
}

/// `addr` as a C socket address for a CONNECT SQE.
fn sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut ss: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = unsafe { &mut *(&mut ss as *mut libc::sockaddr_storage as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = a.port().to_be();
            sin.sin_addr = libc::in_addr { s_addr: u32::from_ne_bytes(a.ip().octets()) };
            std::mem::size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            let sin6 = unsafe { &mut *(&mut ss as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = a.port().to_be();
            sin6.sin6_flowinfo = a.flowinfo();
            sin6.sin6_addr = libc::in6_addr { s6_addr: a.ip().octets() };
            sin6.sin6_scope_id = a.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>()
        }
    };
    (ss, len as libc::socklen_t)
}

/// Run `f` on this thread's queue ring. None when the thread has no ring or
/// it is in use; unlike libublk's accessors this never panics, so it is
/// safe from a Drop.
fn with_ring<R>(f: impl FnOnce(&mut io_uring::IoUring<io_uring::squeue::Entry>) -> R) -> Option<R> {
    let mut f = Some(f);
    let mut out = None;
    let _ = libublk::io::ublk_init_task_ring(|cell| {
        if let (Some(Ok(mut ring)), Some(f)) = (cell.get().map(|r| r.try_borrow_mut()), f.take()) {
            out = Some(f(&mut ring));
        }
        Ok(())
    });
    out
}

/// Submit, wait up to `wait` for a completion, and wake the futures of the
/// target SQEs that completed: the queue loop's job, for an engine shutting
/// down after that loop has ended. None: no usable ring on this thread.
fn reap_ring(wait: Duration) -> Option<()> {
    let cqes: Vec<io_uring::cqueue::Entry> = with_ring(|r| {
        let ts = io_uring::types::Timespec::from(wait);
        let _ = r.submitter().submit_with_args(1, &io_uring::types::SubmitArgs::new().timespec(&ts));
        r.completion().collect()
    })?;
    for cqe in cqes {
        // Only our own SQEs (Target bit); a SEND_ZC buffer-release notice
        // carries a future key that already completed (see the queue loop).
        if cqe.user_data() & UblkUringData::Target as u64 != 0 && !io_uring::cqueue::notif(cqe.flags()) {
            libublk::uring_async::ublk_wake_task(cqe.user_data(), &cqe);
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    //! The engine against a minimal NVMe/TCP target on loopback, driven on a
    //! test thread's own io_uring the way a ublk queue thread drives it.
    use super::*;
    use crate::conn::Ident;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// What the target saw.
    #[derive(Default)]
    struct Seen {
        /// I/O queue Connects: (qid, cntlid, 0-based sqsize).
        io_connects: Vec<(u16, u16, u16)>,
        /// I/O queue connections that have since closed.
        io_closed: usize,
    }

    #[derive(Default)]
    struct TargetCfg {
        /// Wait this long before each ICResp.
        icresp_delay: Duration,
        /// Never answer an I/O queue Connect.
        hang_io_connect: bool,
    }

    struct Target {
        addr: SocketAddr,
        seen: Arc<Mutex<Seen>>,
        /// While set, I/O commands go unanswered.
        hold_io: Arc<AtomicBool>,
    }

    impl Target {
        fn start(bind: &str, cfg: TargetCfg) -> Option<Target> {
            let l = TcpListener::bind(bind).ok()?;
            let addr = l.local_addr().ok()?;
            let (seen, hold_io, cfg) = (Arc::new(Mutex::new(Seen::default())), Arc::new(AtomicBool::new(false)), Arc::new(cfg));
            let (s2, h2) = (seen.clone(), hold_io.clone());
            std::thread::spawn(move || {
                for s in l.incoming().flatten() {
                    let (seen, hold, cfg) = (s2.clone(), h2.clone(), cfg.clone());
                    std::thread::spawn(move || serve(s, &seen, &hold, &cfg));
                }
            });
            Some(Target { addr, seen, hold_io })
        }
    }

    fn pattern(slba: u64, len: usize) -> Vec<u8> {
        (0..len).map(|i| (slba as usize * 7 + i) as u8).collect()
    }

    fn resp(cid: u16, dw0: u32) -> Vec<u8> {
        let mut r = vec![0u8; 24];
        r[0] = PDU_CAPSULE_RESP;
        r[2] = 24;
        r[4..8].copy_from_slice(&24u32.to_le_bytes());
        r[8..12].copy_from_slice(&dw0.to_le_bytes());
        r[20..22].copy_from_slice(&cid.to_le_bytes());
        r
    }

    fn c2h(cid: u16, data: &[u8]) -> Vec<u8> {
        let mut p = vec![0u8; DATA_HLEN];
        p[0] = PDU_C2H_DATA;
        p[1] = FLAG_LAST_PDU | FLAG_C2H_SUCCESS;
        p[2] = DATA_HLEN as u8;
        p[3] = DATA_HLEN as u8;
        p[4..8].copy_from_slice(&((DATA_HLEN + data.len()) as u32).to_le_bytes());
        p[8..10].copy_from_slice(&cid.to_le_bytes());
        p[16..20].copy_from_slice(&(data.len() as u32).to_le_bytes());
        p.extend_from_slice(data);
        p
    }

    /// One connection: ICReq, then command capsules until the host closes.
    fn serve(mut s: TcpStream, seen: &Mutex<Seen>, hold: &AtomicBool, cfg: &TargetCfg) {
        let mut io_queue = false;
        let _ = (|| -> std::io::Result<()> {
            let mut icreq = [0u8; 128];
            s.read_exact(&mut icreq)?;
            std::thread::sleep(cfg.icresp_delay);
            let mut ic = [0u8; 128];
            ic[0] = PDU_IC_RESP;
            ic[2] = 128;
            ic[4..8].copy_from_slice(&128u32.to_le_bytes());
            ic[12..16].copy_from_slice(&131072u32.to_le_bytes());
            s.write_all(&ic)?;
            loop {
                let mut ch = [0u8; CH_LEN];
                s.read_exact(&mut ch)?;
                let mut rest = vec![0u8; u32::from_le_bytes(ch[4..8].try_into().unwrap()) as usize - CH_LEN];
                s.read_exact(&mut rest)?;
                let (sqe, data) = rest.split_at(64);
                let cid = u16::from_le_bytes([sqe[2], sqe[3]]);
                let u16_at = |o: usize| u16::from_le_bytes([sqe[o], sqe[o + 1]]);
                let out = match (sqe[0], io_queue) {
                    (OPC_FABRICS, _) if sqe[4] == FCTYPE_CONNECT => {
                        let qid = u16_at(42);
                        if qid == 0 {
                            resp(cid, 1) // admin: cntlid 1
                        } else {
                            io_queue = true;
                            seen.lock().unwrap().io_connects.push((qid, u16::from_le_bytes([data[16], data[17]]), u16_at(44)));
                            if cfg.hang_io_connect {
                                continue;
                            }
                            resp(cid, 0)
                        }
                    }
                    // Property Get: CAP (MQES 127), else CSTS (RDY).
                    (OPC_FABRICS, _) if sqe[4] == FCTYPE_PROP_GET => resp(cid, if sqe[44] == 0 { 127 } else { 1 }),
                    (OPC_ADMIN_IDENTIFY, false) => {
                        let mut id = vec![0u8; 4096];
                        if sqe[40] == 1 {
                            id[1792..1796].copy_from_slice(&4u32.to_le_bytes()); // ioccsz: no in-capsule data
                        } else {
                            id[0..8].copy_from_slice(&2048u64.to_le_bytes()); // nsze
                            id[104..120].copy_from_slice(&[1; 16]); // nguid
                            id[128..132].copy_from_slice(&(9u32 << 16).to_le_bytes()); // 512 B blocks
                        }
                        c2h(cid, &id)
                    }
                    (_, true) if hold.load(Ordering::Acquire) => continue,
                    (OPC_READ, true) => {
                        let slba = u64::from_le_bytes(sqe[40..48].try_into().unwrap());
                        let nlb = u32::from_le_bytes(sqe[48..52].try_into().unwrap()) as usize + 1;
                        c2h(cid, &pattern(slba, nlb * 512))
                    }
                    _ => resp(cid, 0), // Property Set, keep-alive, flush
                };
                s.write_all(&out)?;
            }
        })();
        if io_queue {
            seen.lock().unwrap().io_closed += 1;
        }
    }

    /// Run `f` on a new thread with its own io_uring, as a queue thread has.
    fn on_ring_thread(f: impl FnOnce() + Send + 'static) {
        std::thread::spawn(move || {
            libublk::io::ublk_init_task_ring(|cell| {
                if cell.get().is_none() {
                    let ring = io_uring::IoUring::builder().setup_cqsize(256).setup_coop_taskrun().build(128).map_err(libublk::UblkError::IOError)?;
                    let _ = cell.set(RefCell::new(ring));
                }
                Ok(())
            })
            .unwrap();
            f();
        })
        .join()
        .unwrap();
    }

    /// The queue loop: tick the engine's tasks, then submit and wake the
    /// futures whose SQEs completed. Until `done`, at most `limit`.
    fn drive_until(exe: &smol::LocalExecutor<'static>, limit: Duration, mut done: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + limit;
        loop {
            while exe.try_tick() {}
            if done() {
                return true;
            }
            if Instant::now() >= end {
                return false;
            }
            let cqes: Vec<io_uring::cqueue::Entry> = libublk::with_task_io_ring_mut(|r| {
                let ts = io_uring::types::Timespec::new().nsec(5_000_000);
                let _ = r.submitter().submit_with_args(1, &io_uring::types::SubmitArgs::new().timespec(&ts));
                r.completion().collect()
            });
            for c in cqes {
                if !io_uring::cqueue::notif(c.flags()) {
                    libublk::uring_async::ublk_wake_task(c.user_data(), &c);
                }
            }
        }
    }

    fn wait_for(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + limit;
        while !done() {
            if Instant::now() >= end {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }

    /// Threads of this process whose name starts with `prefix`.
    fn threads_named(prefix: &str) -> usize {
        let Ok(d) = std::fs::read_dir("/proc/self/task") else { return 0 };
        d.flatten().filter(|t| std::fs::read_to_string(t.path().join("comm")).is_ok_and(|c| c.starts_with(prefix))).count()
    }

    struct Rig {
        e: Rc<QEngine>,
        exe: Rc<smol::LocalExecutor<'static>>,
        stats: Arc<Stats>,
        ctrls: Arc<Ctrls>,
        fault_dir: String,
        _dir: TempDir,
    }

    struct TempDir(String);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn rig(t: &Target, name: &str, write_fence: Duration) -> Rig {
        let id = Ident { hostnqn: "nqn.2014-08.org.nvmexpress:uuid:test".into(), hostid: [7; 16], subnqn: "nqn.test:sub".into() };
        let ctrls = Ctrls::new(vec![t.addr], id, Duration::from_secs(15)).unwrap();
        let fault_dir = std::env::temp_dir().join(format!("nvmeublk-qengine-{}-{name}", std::process::id())).to_string_lossy().into_owned();
        let _ = std::fs::create_dir_all(&fault_dir);
        let cfg = QConfig {
            io_timeout: Duration::from_secs(5),
            no_path_timeout: Duration::ZERO,
            max_attempts: 8,
            write_fence,
            hold_writes_until: None,
            rx_offload: 0,
            cdev_fd: -1,
            conns_per_path: 1,
            rx_chunk: RX_CHUNK_DEFAULT,
            napi_us: 0,
            fault_dir: fault_dir.clone(),
            quiesce: Arc::new(AtomicBool::new(false)),
        };
        let exe = Rc::new(smol::LocalExecutor::new());
        let stats = Arc::new(Stats::default());
        let e = QEngine::new(0, ctrls.clone(), cfg, exe.clone(), stats.clone(), Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
        Rig { e, exe, stats, ctrls, _dir: TempDir(fault_dir.clone()), fault_dir }
    }

    /// Submit `op` at LBA 8 over `buf`; the receiver gets its result.
    fn request(e: &QEngine, op: Op, buf: &mut [u8]) -> Receiver<i32> {
        let (tx, rx) = smol::channel::bounded(1);
        let len = if op == Op::Flush { 0 } else { buf.len() };
        e.submit(Pending::new(op, 8, (buf.len() / 512).max(1) as u32, buf.as_mut_ptr(), len, tx, None, None));
        rx
    }

    /// Q5: the I/O queue is dialled, handshaken and connected by SQEs on the
    /// queue's ring. The target holds each ICResp for 300 ms, so a connect
    /// thread (the old way) would be seen.
    #[test]
    fn io_queue_connects_on_the_ring_without_a_thread() {
        let t = Target::start("127.0.0.1:0", TargetCfg { icresp_delay: Duration::from_millis(300), ..Default::default() }).unwrap();
        on_ring_thread(move || {
            let r = rig(&t, "ring-connect", Duration::from_secs(20));
            r.e.start();
            let mut buf = vec![0u8; 4096];
            let rx = request(&r.e, Op::Read, &mut buf);
            let mut conn_threads = 0;
            let done = drive_until(&r.exe, Duration::from_secs(10), || {
                conn_threads = conn_threads.max(threads_named("nvme-conn"));
                !rx.is_empty()
            });
            assert!(done, "the read never completed");
            assert_eq!(rx.try_recv(), Ok(4096));
            assert!(buf == pattern(8, 4096), "wrong read data");
            assert_eq!(conn_threads, 0, "the I/O queue was connected on a thread");
            // qid = queue * conns_per_path + k + 1; the admin queue's cntlid; 0-based sqsize.
            assert_eq!(t.seen.lock().unwrap().io_connects, vec![(1, 1, 127)]);
            assert_eq!(r.stats.reconnects.load(Ordering::Relaxed), 1);
            drop(r.e);
            r.ctrls.shutdown();
        });
    }

    /// Q5 over IPv6 (skipped where ::1 is unavailable).
    #[test]
    fn io_queue_connects_over_ipv6() {
        let Some(t) = Target::start("[::1]:0", TargetCfg::default()) else { return };
        on_ring_thread(move || {
            let r = rig(&t, "ring-connect-v6", Duration::from_secs(20));
            r.e.start();
            let mut buf = vec![0u8; 4096];
            let rx = request(&r.e, Op::Read, &mut buf);
            assert!(drive_until(&r.exe, Duration::from_secs(10), || !rx.is_empty()), "the read never completed");
            assert_eq!(rx.try_recv(), Ok(4096));
            assert_eq!(t.seen.lock().unwrap().io_connects.len(), 1);
            drop(r.e);
            r.ctrls.shutdown();
        });
    }

    /// Q5: a connect step that overruns its time limit is aborted by the
    /// timer, backs off, and is retried (the limits themselves are 3 s and
    /// 10 s; the test moves the deadline instead of waiting).
    #[test]
    fn a_connect_step_past_its_deadline_is_aborted_and_retried() {
        let t = Target::start("127.0.0.1:0", TargetCfg { hang_io_connect: true, ..Default::default() }).unwrap();
        on_ring_thread(move || {
            let r = rig(&t, "dial-deadline", Duration::from_secs(20));
            r.e.start();
            assert!(drive_until(&r.exe, Duration::from_secs(5), || t.seen.lock().unwrap().io_connects.len() == 1));
            r.e.core.connecting.borrow_mut()[0].as_mut().expect("a connect in flight").deadline = Instant::now();
            assert!(drive_until(&r.exe, Duration::from_secs(5), || t.seen.lock().unwrap().io_connects.len() == 2), "no retry after the deadline");
            assert!(wait_for(Duration::from_secs(2), || t.seen.lock().unwrap().io_closed >= 1), "the timed-out connection stayed open");
            assert_eq!(r.e.core.backoff.borrow()[0], Duration::from_millis(500), "one failure doubles the backoff");
            drop(r.e);
            r.ctrls.shutdown();
        });
    }

    /// Q7: dropping the queue's handle shuts the engine down on the ring, and
    /// the engine is freed with its connection (the task <-> engine cycle
    /// kept both for good).
    #[test]
    fn dropping_the_handle_frees_the_engine_and_its_connection() {
        let t = Target::start("127.0.0.1:0", TargetCfg::default()).unwrap();
        on_ring_thread(move || {
            let r = rig(&t, "free", Duration::from_secs(20));
            r.e.start();
            let mut buf = vec![0u8; 4096];
            let rx = request(&r.e, Op::Read, &mut buf);
            assert!(drive_until(&r.exe, Duration::from_secs(10), || !rx.is_empty()), "the read never completed");
            let t0 = Instant::now();
            drop(r.e);
            assert!(t0.elapsed() < Duration::from_secs(1), "shutdown took {:?}", t0.elapsed());
            assert_eq!(Arc::strong_count(&r.stats), 1, "the engine was not freed");
            assert!(wait_for(Duration::from_secs(2), || t.seen.lock().unwrap().io_closed == 1), "the I/O connection stayed open");
            r.ctrls.shutdown();
        });
    }

    /// Q7: a connect still in flight does not keep the engine alive either.
    #[test]
    fn dropping_the_handle_aborts_a_connect_in_flight() {
        let t = Target::start("127.0.0.1:0", TargetCfg { hang_io_connect: true, ..Default::default() }).unwrap();
        on_ring_thread(move || {
            let r = rig(&t, "abort-dial", Duration::from_secs(20));
            r.e.start();
            // Connect sent, never answered: the connect waits on the ring.
            assert!(drive_until(&r.exe, Duration::from_secs(5), || t.seen.lock().unwrap().io_connects.len() == 1));
            let t0 = Instant::now();
            drop(r.e);
            assert!(t0.elapsed() < Duration::from_secs(1), "shutdown took {:?}", t0.elapsed());
            assert_eq!(Arc::strong_count(&r.stats), 1, "the engine was not freed");
            assert!(wait_for(Duration::from_secs(2), || t.seen.lock().unwrap().io_closed == 1), "the connecting socket stayed open");
            r.ctrls.shutdown();
        });
    }

    /// Q10: a panicking engine task (here the timer, by fault injection)
    /// fails the engine: the flush it had on the wire ends in EIO once the
    /// write fence has passed (the timer is restarted to release it), new
    /// I/O fails at once, and the connection is closed. The panic used to
    /// vanish into the executor and leave the flush hanging.
    #[test]
    fn a_panicking_engine_task_fails_the_engine_instead_of_hanging() {
        let t = Target::start("127.0.0.1:0", TargetCfg::default()).unwrap();
        on_ring_thread(move || {
            let fence = Duration::from_millis(300);
            let r = rig(&t, "panic", fence);
            r.e.start();
            let mut buf = vec![0u8; 4096];
            let rx = request(&r.e, Op::Read, &mut buf);
            assert!(drive_until(&r.exe, Duration::from_secs(10), || !rx.is_empty()), "the read never completed");
            assert_eq!(rx.try_recv(), Ok(4096));
            t.hold_io.store(true, Ordering::Release);
            let flush = request(&r.e, Op::Flush, &mut []);
            drive_until(&r.exe, Duration::from_millis(200), || false);
            std::fs::write(format!("{}/queues", r.fault_dir), "1").unwrap();
            std::fs::write(format!("{}/fault", r.fault_dir), "panic").unwrap();
            let t0 = Instant::now();
            assert!(drive_until(&r.exe, Duration::from_secs(5), || !flush.is_empty()), "the flush hung");
            assert_eq!(flush.try_recv(), Ok(-libc::EIO));
            assert!(t0.elapsed() >= fence, "the flush failed before its write fence ({:?})", t0.elapsed());
            let rx = request(&r.e, Op::Read, &mut buf);
            assert!(drive_until(&r.exe, Duration::from_millis(100), || !rx.is_empty()), "a new read hung");
            assert_eq!(rx.try_recv(), Ok(-libc::EIO));
            assert_eq!(r.stats.engine_panics.load(Ordering::Relaxed), 1);
            assert!(wait_for(Duration::from_secs(2), || t.seen.lock().unwrap().io_closed == 1), "the I/O connection stayed open");
            drop(r.e);
            r.ctrls.shutdown();
        });
    }
}
