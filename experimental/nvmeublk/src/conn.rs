//! One NVMe/TCP controller path: a synchronous admin queue and one pipelined
//! I/O queue with its own receiver thread.

use crate::pdu::*;
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::os::fd::AsRawFd as _;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::io::IoSlice;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub const NSID: u32 = 1;

/// Completion handle shared with the ublk io task. `efd` is an eventfd the
/// io task awaits through io_uring; writing it wakes exactly that tag.
pub struct Done {
    pub res: AtomicI32,
    pub efd: i32,
}

impl Done {
    pub fn complete(&self, res: i32) {
        self.res.store(res, Ordering::Release);
        let one: u64 = 1;
        unsafe { libc::write(self.efd, &one as *const u64 as *const libc::c_void, 8) };
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Op {
    Read,
    Write,
    Flush,
}

/// A block request. `buf` points at the ublk tag's IoBuf, which the tag owns
/// until `done` fires, so the engine may read/write it without copying.
pub struct Req {
    pub op: Op,
    pub slba: u64,
    pub nlb: u32,
    pub buf: *mut u8,
    pub len: usize,
    pub done: Arc<Done>,
    pub first_submit: Instant,
    pub attempts: u32,
}
unsafe impl Send for Req {}

impl Req {
    pub fn complete_ok(&self) {
        let res = if self.op == Op::Flush { 0 } else { self.len as i32 };
        self.done.complete(res);
    }
}

#[derive(Clone, Debug)]
pub struct NsInfo {
    pub nsze: u64,
    pub lba_shift: u32,
    /// Namespace identity (Identify Namespace NGUID and EUI64). Every path
    /// and every reconnect must present the same one, or it is a different
    /// namespace that merely has the same size.
    pub nguid: [u8; 16],
    pub eui64: [u8; 8],
    pub incapsule_bytes: usize,
    pub mdts_bytes: usize,
}

pub struct Ident {
    pub hostnqn: String,
    pub hostid: [u8; 16],
    pub subnqn: String,
}

fn dial(addr: SocketAddr) -> Result<TcpStream> {
    let s = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).with_context(|| format!("connect {addr}"))?;
    tune_socket(s.as_raw_fd())?;
    Ok(s)
}

/// Socket options every NVMe/TCP connection gets, whether it is dialled
/// here (blocking) or on a queue's io_uring (qengine.rs).
pub fn tune_socket(fd: i32) -> Result<()> {
    let one: libc::c_int = 1;
    if unsafe { libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as u32) } != 0 {
        return Err(std::io::Error::last_os_error()).context("TCP_NODELAY");
    }
    // NVMEUBLK_RCVBUF (bytes, tuning; 0 = kernel autotuning): a fixed
    // receive buffer, so the advertised window does not have to grow with
    // the measured drain rate first.
    if let Some(n) = std::env::var("NVMEUBLK_RCVBUF").ok().and_then(|v| v.parse::<libc::c_int>().ok()).filter(|&n| n > 0) {
        unsafe { libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &n as *const _ as *const libc::c_void, std::mem::size_of::<libc::c_int>() as u32) };
    }
    Ok(())
}

/// Synchronous admin queue. Only the multipath maintenance thread uses it.
pub struct AdminConn {
    s: TcpStream,
    cid: u16,
    pub cntlid: u16,
    pub maxh2c: u32,
    /// Bytes of a keep-alive response read so far (ka_poll).
    ka_rx: Vec<u8>,
}

impl AdminConn {
    pub fn connect(addr: SocketAddr, id: &Ident, kato_ms: u32) -> Result<Self> {
        let mut s = dial(addr)?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        let (_cpda, maxh2c) = ic_handshake(&mut s)?;
        let mut a = AdminConn { s, cid: 0, cntlid: 0, maxh2c, ka_rx: Vec::new() };
        let (sqe, data) = connect_cmd(a.next_cid(), 0, 31, 0, kato_ms, 0xffff, &id.hostid, &id.subnqn, &id.hostnqn);
        let cqe = a.exec(&sqe, &data, None)?;
        if cqe.sc() != 0 {
            bail!("admin Connect rejected: status {:#x}", cqe.status);
        }
        a.cntlid = (cqe.dw0 & 0xffff) as u16;
        Ok(a)
    }

    fn next_cid(&mut self) -> u16 {
        self.cid = self.cid.wrapping_add(1) % 31;
        self.cid
    }

    /// Send one admin command and wait for its completion, collecting any
    /// C2HData into `out`.
    fn exec(&mut self, sqe: &Sqe, data: &[u8], mut out: Option<&mut [u8]>) -> Result<Cqe> {
        write_capsule(&mut self.s, sqe, data)?;
        let cid = sqe.cid();
        // Data must arrive in order and cover the whole buffer before any
        // success is believed; a short or out-of-order transfer is an error.
        let want = out.as_deref().map_or(0, |o| o.len());
        let mut got = 0usize;
        loop {
            let (ch, psh) = read_hdr(&mut self.s)?;
            match ch.ptype {
                PDU_C2H_DATA => {
                    let h = parse_data_hdr(&psh);
                    if h.cid != cid {
                        bail!("admin C2HData for cid {} while waiting for {cid}", h.cid);
                    }
                    let mut skip = vec![0u8; ch.pdo as usize - ch.hlen as usize];
                    self.s.read_exact(&mut skip)?;
                    let mut chunk = vec![0u8; h.len as usize];
                    self.s.read_exact(&mut chunk)?;
                    let Some(o) = out.as_deref_mut() else { bail!("admin C2HData for a command without data") };
                    let end = h.off as usize + h.len as usize;
                    if h.off as usize != got || end > o.len() {
                        bail!("admin C2HData off {} len {} with {got} of {} received", h.off, h.len, o.len());
                    }
                    o[h.off as usize..end].copy_from_slice(&chunk);
                    got = end;
                    if ch.flags & FLAG_C2H_SUCCESS != 0 {
                        if got != want {
                            bail!("admin command reported success after {got} of {want} bytes");
                        }
                        return Ok(Cqe { cid, ..Default::default() });
                    }
                }
                PDU_CAPSULE_RESP => {
                    let cqe = Cqe::parse(&psh);
                    if cqe.cid != cid {
                        bail!("admin completion for cid {} while waiting for {cid}", cqe.cid);
                    }
                    if cqe.sc() == 0 && got != want {
                        bail!("admin command reported success after {got} of {want} bytes");
                    }
                    return Ok(cqe);
                }
                t => bail!("unexpected PDU {t:#x} on admin queue"),
            }
        }
    }

    fn prop_get(&mut self, off: u32, size8: bool) -> Result<u64> {
        let sqe = prop_get_cmd(self.next_cid(), off, size8);
        let c = self.exec(&sqe, &[], None)?;
        if c.sc() != 0 {
            bail!("Property Get {off:#x} failed: {:#x}", c.status);
        }
        Ok(c.dw0 as u64 | ((c.dw1 as u64) << 32))
    }

    /// Enable the controller and identify namespace 1.
    pub fn enable_and_identify(&mut self) -> Result<(NsInfo, u16)> {
        let cap = self.prop_get(0x0, true)?;
        let mqes = (cap & 0xffff) as u16;
        let cc: u64 = 1 | (6 << 16) | (4 << 20);
        let sqe = prop_set_cmd(self.next_cid(), 0x14, cc, false);
        let c = self.exec(&sqe, &[], None)?;
        if c.sc() != 0 {
            bail!("CC.EN set failed: {:#x}", c.status);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.prop_get(0x1c, false)? & 1 == 0 {
            if Instant::now() > deadline {
                bail!("controller never reported CSTS.RDY");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let mut ctrl = vec![0u8; 4096];
        let sqe = identify_cmd(self.next_cid(), 0, 1);
        let c = self.exec(&sqe, &[], Some(&mut ctrl))?;
        if c.sc() != 0 {
            bail!("Identify Controller failed: {:#x}", c.status);
        }
        let mdts = ctrl[77];
        let ioccsz = u32::from_le_bytes(ctrl[1792..1796].try_into().unwrap()) as usize;
        let mut ns = vec![0u8; 4096];
        let sqe = identify_cmd(self.next_cid(), NSID, 0);
        let c = self.exec(&sqe, &[], Some(&mut ns))?;
        if c.sc() != 0 {
            bail!("Identify Namespace failed: {:#x}", c.status);
        }
        let nsze = u64::from_le_bytes(ns[0..8].try_into().unwrap());
        let flbas = (ns[26] & 0x0f) as usize;
        let lbaf = u32::from_le_bytes(ns[128 + 4 * flbas..132 + 4 * flbas].try_into().unwrap());
        let lba_shift = (lbaf >> 16) & 0xff;
        let info = NsInfo {
            nsze,
            lba_shift,
            nguid: ns[104..120].try_into().unwrap(),
            eui64: ns[120..128].try_into().unwrap(),
            // ioccsz counts 16-byte units and includes the 64-byte SQE.
            incapsule_bytes: (ioccsz * 16).saturating_sub(64),
            mdts_bytes: if mdts == 0 { usize::MAX } else { 4096usize << mdts },
        };
        Ok((info, mqes))
    }

    /// Keep-alive in two halves for the shared supervisor, which must never
    /// block on one controller: send the command (a 72-byte write)...
    pub fn ka_send(&mut self) -> Result<u16> {
        let sqe = keep_alive_cmd(self.next_cid());
        self.s.set_write_timeout(Some(Duration::from_secs(1)))?;
        write_capsule(&mut self.s, &sqe, &[])?;
        self.ka_rx.clear();
        Ok(sqe.cid())
    }

    /// ...then collect its response without blocking: Ok(true) once it has
    /// arrived and succeeded, Ok(false) while it is still outstanding. After
    /// bring-up the admin queue carries nothing but keep-alives, so the next
    /// PDU must be this command's response capsule.
    pub fn ka_poll(&mut self, cid: u16) -> Result<bool> {
        const RESP_LEN: usize = CH_LEN + 16;
        let mut buf = [0u8; RESP_LEN];
        while self.ka_rx.len() < RESP_LEN {
            let want = RESP_LEN - self.ka_rx.len();
            let n = unsafe { libc::recv(self.s.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, want, libc::MSG_DONTWAIT) };
            if n > 0 {
                self.ka_rx.extend_from_slice(&buf[..n as usize]);
                continue;
            }
            if n == 0 {
                bail!("admin connection closed");
            }
            let e = std::io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EAGAIN) => return Ok(false),
                Some(libc::EINTR) => continue,
                _ => return Err(e.into()),
            }
        }
        if self.ka_rx[0] != PDU_CAPSULE_RESP {
            bail!("unexpected PDU {:#x} on admin queue", self.ka_rx[0]);
        }
        let cqe = Cqe::parse(&self.ka_rx[CH_LEN..RESP_LEN]);
        self.ka_rx.clear();
        if cqe.cid != cid {
            bail!("admin completion for cid {} while waiting for keep-alive {cid}", cqe.cid);
        }
        if cqe.sc() != 0 {
            bail!("Keep Alive failed: {:#x}", cqe.status);
        }
        Ok(true)
    }

    pub fn keep_alive(&mut self) -> Result<()> {
        let sqe = keep_alive_cmd(self.next_cid());
        let c = self.exec(&sqe, &[], None)?;
        if c.sc() != 0 {
            bail!("Keep Alive failed: {:#x}", c.status);
        }
        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.s.shutdown(Shutdown::Both);
    }
}

struct Pending {
    dead: bool,
    free: Vec<u16>,
    inflight: HashMap<u16, Req>,
}

pub enum SubmitErr {
    /// Every CID on this queue is in use; try another path or wait.
    Busy(Req),
    /// The connection has failed; the request was never sent.
    Dead(Req),
}

/// One outbound PDU: header bytes plus an optional payload borrowed from a
/// request buffer that stays valid until that request completes.
struct Msg {
    head: Vec<u8>,
    data: *const u8,
    len: usize,
}
unsafe impl Send for Msg {}

/// Sender thread body: block for one message, then drain whatever else is
/// queued and push the whole batch out with vectored writes. Keeps socket
/// syscalls (and, on loopback, the peer's inline receive processing) off the
/// ublk queue threads, and coalesces many small capsules into one write.
fn run_sender(conn: Arc<IoConn>, mut w: TcpStream, rx: Receiver<Msg>) {
    let mut batch: Vec<Msg> = Vec::with_capacity(64);
    while let Ok(first) = rx.recv() {
        batch.push(first);
        let mut bytes = batch[0].head.len() + batch[0].len;
        while bytes < 512 * 1024 && batch.len() < 256 {
            match rx.try_recv() {
                Ok(m) => {
                    bytes += m.head.len() + m.len;
                    batch.push(m);
                }
                Err(_) => break,
            }
        }
        if let Err(e) = write_batch(&mut w, &batch) {
            if !conn.is_dead() {
                log::warn!("path {}: send failed: {e:#}", conn.path);
            }
            conn.kill();
            return;
        }
        batch.clear();
    }
}

fn write_batch(w: &mut TcpStream, batch: &[Msg]) -> std::io::Result<()> {
    let mut slices: Vec<&[u8]> = Vec::with_capacity(batch.len() * 2);
    for m in batch {
        slices.push(&m.head);
        if m.len > 0 {
            slices.push(unsafe { std::slice::from_raw_parts(m.data, m.len) });
        }
    }
    let (mut idx, mut off) = (0usize, 0usize);
    while idx < slices.len() {
        let iov: Vec<IoSlice> = std::iter::once(IoSlice::new(&slices[idx][off..]))
            .chain(slices[idx + 1..].iter().take(1023).map(|s| IoSlice::new(s)))
            .collect();
        let mut n = w.write_vectored(&iov)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        while n > 0 {
            let left = slices[idx].len() - off;
            if n >= left {
                n -= left;
                idx += 1;
                off = 0;
            } else {
                off += n;
                n = 0;
            }
        }
    }
    Ok(())
}

static DISABLE_SQFLOW: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var("NVMEUBLK_DISABLE_SQFLOW").is_ok_and(|v| v != "0"));

/// Dial, handshake and Connect one I/O queue (`qid` >= 1) on controller
/// `cntlid`. Blocking; returns the ready socket and the target's maxh2cdata.
/// The queue threads do the same on their io_uring (qengine.rs), with the
/// pieces below, so both paths put the same bytes on the wire.
pub fn connect_io_queue(addr: SocketAddr, id: &Ident, cntlid: u16, qid: u16, qsize: u16) -> Result<(TcpStream, u32)> {
    let mut s = dial(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    let (_cpda, maxh2c) = ic_handshake(&mut s)?;
    s.write_all(&io_connect_capsule(id, cntlid, qid, qsize)).context("send command capsule")?;
    let (ch, psh) = read_hdr(&mut s)?;
    check_io_connect_resp(qid, &ch, &psh)?;
    // Completions are waited on indefinitely; stalls are the watchdog's job.
    s.set_read_timeout(None)?;
    Ok((s, maxh2c.max(4096)))
}

/// The ICReq PDU that `ic_handshake` sends: pfv 0, hpda 0, no digests,
/// maxr2t 0 (one outstanding R2T per command).
pub fn icreq_pdu() -> [u8; 128] {
    let mut req = [0u8; 128];
    req[0] = PDU_IC_REQ;
    req[2] = 128; // hlen
    req[4..8].copy_from_slice(&128u32.to_le_bytes()); // plen
    req
}

/// Validate a received ICResp (all 128 bytes) exactly as the blocking
/// handshake does, by running `ic_handshake` over it; its ICReq goes
/// nowhere. Returns the target's maxh2cdata.
pub fn icresp_maxh2c(resp: &[u8]) -> Result<u32> {
    struct Replay<'a>(&'a [u8]);
    impl Read for Replay<'_> {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(b)
        }
    }
    impl Write for Replay<'_> {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    Ok(ic_handshake(&mut Replay(resp))?.1)
}

/// The Fabrics Connect command capsule (header and data, as sent) for I/O
/// queue `qid` of controller `cntlid`.
pub fn io_connect_capsule(id: &Ident, cntlid: u16, qid: u16, qsize: u16) -> Vec<u8> {
    // NVMEUBLK_DISABLE_SQFLOW (tuning): nothing here uses the SQ head, so
    // trade it for one PDU less per read (see CATTR_DISABLE_SQFLOW).
    let cattr = if *DISABLE_SQFLOW { CATTR_DISABLE_SQFLOW } else { 0 };
    let (sqe, data) = connect_cmd(0, qid, qsize - 1, cattr, 0, cntlid, &id.hostid, &id.subnqn, &id.hostnqn);
    let mut v = Vec::with_capacity(CMD_HLEN + data.len());
    let _ = write_capsule(&mut v, &sqe, &data); // a Vec never refuses a write
    v
}

/// Check the response to an I/O queue Connect, as `read_hdr` returned it.
pub fn check_io_connect_resp(qid: u16, ch: &Ch, psh: &[u8]) -> Result<()> {
    if ch.ptype != PDU_CAPSULE_RESP {
        bail!("expected Connect response on I/O queue {qid}, got {:#x}", ch.ptype);
    }
    let cqe = Cqe::parse(psh);
    if cqe.sc() != 0 {
        bail!("I/O queue {qid} Connect rejected: {:#x}", cqe.status);
    }
    Ok(())
}

/// A pipelined I/O queue on one path.
pub struct IoConn {
    pub path: usize,
    tx: Mutex<Option<Sender<Msg>>>,
    ctl: TcpStream,
    pending: Mutex<Pending>,
    freed: Condvar,
    info: NsInfo,
    maxh2c: usize,
    dead: AtomicBool,
    /// Fault injection: while set, the receiver stops reading, modelling a
    /// path that goes silent without any socket error.
    pub stall: AtomicBool,
}

impl IoConn {
    pub fn connect(path: usize, addr: SocketAddr, id: &Ident, cntlid: u16, qsize: u16, info: NsInfo) -> Result<(Arc<Self>, TcpStream)> {
        let (s, maxh2c) = connect_io_queue(addr, id, cntlid, 1, qsize)?;
        let reader = s.try_clone()?;
        let ctl = s.try_clone()?;
        let (tx, rx) = channel::<Msg>();
        let conn = Arc::new(IoConn {
            path,
            tx: Mutex::new(Some(tx)),
            ctl,
            pending: Mutex::new(Pending { dead: false, free: (1..qsize).rev().collect(), inflight: HashMap::new() }),
            freed: Condvar::new(),
            info,
            maxh2c: maxh2c.max(4096) as usize,
            dead: AtomicBool::new(false),
            stall: AtomicBool::new(false),
        });
        let c2 = conn.clone();
        std::thread::Builder::new().name(format!("nvme-tx-{path}")).spawn(move || run_sender(c2, s, rx))?;
        Ok((conn, reader))
    }

    fn send(&self, m: Msg) -> bool {
        match self.tx.lock().unwrap().as_ref() {
            Some(tx) => tx.send(m).is_ok(),
            None => false,
        }
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    pub fn outstanding(&self) -> usize {
        self.pending.lock().unwrap().inflight.len()
    }

    /// Queue a request on this path. Never blocks on CID exhaustion.
    pub fn try_submit(&self, req: Req) -> Result<(), SubmitErr> {
        let mut p = self.pending.lock().unwrap();
        if p.dead {
            return Err(SubmitErr::Dead(req));
        }
        let Some(cid) = p.free.pop() else { return Err(SubmitErr::Busy(req)) };
        let inline = req.op == Op::Write && req.len <= self.info.incapsule_bytes;
        let sqe = match req.op {
            Op::Read => rw_cmd(OPC_READ, cid, NSID, req.slba, req.nlb, req.len as u32, false),
            Op::Write => rw_cmd(OPC_WRITE, cid, NSID, req.slba, req.nlb, req.len as u32, inline),
            Op::Flush => flush_cmd(cid, NSID),
        };
        let (data, len) = if inline { (req.buf as *const u8, req.len) } else { (std::ptr::null(), 0) };
        let head = capsule_header(&sqe, len);
        // Register before the capsule can reach the wire, so a fast response
        // always finds its request. Then enqueue; no syscall under the lock.
        p.inflight.insert(cid, req);
        drop(p);
        if !self.send(Msg { head, data, len }) {
            let back = {
                let mut p = self.pending.lock().unwrap();
                let r = p.inflight.remove(&cid);
                if r.is_some() {
                    p.free.push(cid);
                }
                r
            };
            self.kill();
            // If the receiver already drained it, it was handed back for
            // resubmission there; only a request still here is ours to return.
            return match back {
                Some(r) => Err(SubmitErr::Dead(r)),
                None => Ok(()),
            };
        }
        Ok(())
    }

    /// Wait until a CID frees up (or the connection dies).
    pub fn wait_for_cid(&self, timeout: Duration) {
        let p = self.pending.lock().unwrap();
        if p.free.is_empty() && !p.dead {
            let _ = self.freed.wait_timeout(p, timeout);
        }
    }

    /// Tear the connection down; the receiver thread notices and drains.
    pub fn kill(&self) {
        self.dead.store(true, Ordering::Release);
        let _ = self.ctl.shutdown(Shutdown::Both);
        // Dropping the channel ends the sender thread.
        self.tx.lock().unwrap().take();
    }

    /// Oldest in-flight request's age, for the stall detector.
    pub fn oldest_inflight(&self) -> Option<Duration> {
        let p = self.pending.lock().unwrap();
        p.inflight.values().map(|r| r.first_submit.elapsed()).max()
    }

    /// Mark dead and hand back everything in flight for resubmission.
    fn drain(&self) -> Vec<Req> {
        self.dead.store(true, Ordering::Release);
        let mut p = self.pending.lock().unwrap();
        p.dead = true;
        let reqs: Vec<Req> = p.inflight.drain().map(|(_, r)| r).collect();
        self.freed.notify_all();
        reqs
    }

    fn finish(&self, cid: u16, ok: bool, status: u16) {
        let req = {
            let mut p = self.pending.lock().unwrap();
            let r = p.inflight.remove(&cid);
            if r.is_some() {
                p.free.push(cid);
            }
            r
        };
        self.freed.notify_one();
        match req {
            Some(r) if ok => r.complete_ok(),
            Some(r) => {
                log::warn!("path {}: cid {cid} {:?} slba {} failed status {status:#x}", self.path, r.op, r.slba);
                r.done.complete(-libc::EIO);
            }
            None => log::warn!("path {}: completion for unknown cid {cid}", self.path),
        }
    }

    /// Look up where C2HData for `cid` lands, without holding the lock
    /// across the socket read. The request cannot complete meanwhile: only
    /// this receiver thread completes requests on this connection.
    fn target(&self, cid: u16) -> Option<(*mut u8, usize, Op)> {
        let p = self.pending.lock().unwrap();
        p.inflight.get(&cid).map(|r| (r.buf, r.len, r.op))
    }

    fn answer_r2t(&self, cid: u16, ttag: u16, off: usize, len: usize) -> Result<()> {
        let Some((buf, blen, op)) = self.target(cid) else { bail!("R2T for unknown cid {cid}") };
        if op != Op::Write || off + len > blen {
            bail!("R2T out of range: cid {cid} off {off} len {len} buflen {blen}");
        }
        let mut sent = 0;
        while sent < len {
            let n = (len - sent).min(self.maxh2c);
            let head = h2c_header(cid, ttag, (off + sent) as u32, n, sent + n == len);
            if !self.send(Msg { head, data: unsafe { buf.add(off + sent) } as *const u8, len: n }) {
                bail!("sender gone while answering R2T");
            }
            sent += n;
        }
        Ok(())
    }

    fn receive(&self, r: &mut TcpStream) -> Result<()> {
        loop {
            while self.stall.load(Ordering::Acquire) && !self.is_dead() {
                std::thread::sleep(Duration::from_millis(50));
            }
            let ch = read_ch(r)?;
            let mut psh = vec![0u8; (ch.hlen as usize).saturating_sub(CH_LEN)];
            r.read_exact(&mut psh)?;
            match ch.ptype {
                PDU_C2H_DATA => {
                    let h = parse_data_hdr(&psh);
                    let pad = (ch.pdo as usize).saturating_sub(ch.hlen as usize);
                    if pad > 0 {
                        let mut skip = vec![0u8; pad];
                        r.read_exact(&mut skip)?;
                    }
                    let Some((buf, blen, _)) = self.target(h.cid) else { bail!("C2HData for unknown cid {}", h.cid) };
                    let (off, len) = (h.off as usize, h.len as usize);
                    if off + len > blen {
                        bail!("C2HData out of range: cid {} off {off} len {len} buflen {blen}", h.cid);
                    }
                    let dst = unsafe { std::slice::from_raw_parts_mut(buf.add(off), len) };
                    r.read_exact(dst)?;
                    if ch.flags & FLAG_C2H_SUCCESS != 0 {
                        self.finish(h.cid, true, 0);
                    }
                }
                PDU_R2T => {
                    let h = parse_data_hdr(&psh);
                    self.answer_r2t(h.cid, h.ttag, h.off as usize, h.len as usize)?;
                }
                PDU_CAPSULE_RESP => {
                    let c = Cqe::parse(&psh);
                    self.finish(c.cid, c.sc() == 0, c.status);
                }
                PDU_C2H_TERM => bail!("target terminated the connection (C2HTermReq)"),
                t => bail!("unexpected PDU type {t:#x}"),
            }
        }
    }

    /// Receiver thread body. Returns the requests that were in flight when
    /// the connection failed, for the caller to resubmit elsewhere.
    pub fn run_receiver(self: &Arc<Self>, mut r: TcpStream) -> Vec<Req> {
        if let Err(e) = self.receive(&mut r) {
            if !self.is_dead() {
                log::warn!("path {}: I/O queue failed: {e:#}", self.path);
            }
        }
        let _ = r.shutdown(Shutdown::Both);
        self.drain()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A duplex stream for `ic_handshake`: records what it writes, replays `rx`.
    struct Script {
        tx: Vec<u8>,
        rx: std::io::Cursor<Vec<u8>>,
    }
    impl Read for Script {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            self.rx.read(b)
        }
    }
    impl Write for Script {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.tx.extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn icresp(dgst: u8, maxh2c: u32) -> Vec<u8> {
        let mut r = vec![0u8; 128];
        r[0] = PDU_IC_RESP;
        r[2] = 128;
        r[4..8].copy_from_slice(&128u32.to_le_bytes());
        r[11] = dgst;
        r[12..16].copy_from_slice(&maxh2c.to_le_bytes());
        r
    }

    /// The ring-native connect (qengine.rs) builds its handshake from these
    /// helpers; they must match the blocking handshake byte for byte and
    /// check by check.
    #[test]
    fn ring_handshake_pieces_match_the_blocking_handshake() {
        let mut s = Script { tx: Vec::new(), rx: std::io::Cursor::new(icresp(0, 65536)) };
        let (_, maxh2c) = ic_handshake(&mut s).unwrap();
        assert_eq!(s.tx, icreq_pdu().to_vec());
        assert_eq!(icresp_maxh2c(&icresp(0, 65536)).unwrap(), maxh2c);
        assert!(icresp_maxh2c(&icresp(1, 65536)).is_err(), "digests must be refused");
        let mut bad = icresp(0, 65536);
        bad[0] = PDU_CAPSULE_RESP;
        assert!(icresp_maxh2c(&bad).is_err(), "a non-ICResp must be refused");
        assert!(icresp_maxh2c(&icresp(0, 65536)[..100]).is_err(), "a short ICResp must be refused");

        let id = Ident { hostnqn: "nqn.2014-08.org.nvmexpress:uuid:h".into(), hostid: [7; 16], subnqn: "nqn.test:sub".into() };
        let cap = io_connect_capsule(&id, 0x21, 5, 128);
        assert_eq!(cap.len(), CMD_HLEN + 1024);
        let sqe = &cap[CH_LEN..CMD_HLEN];
        assert_eq!((cap[0], sqe[0], sqe[4]), (PDU_CAPSULE_CMD, OPC_FABRICS, FCTYPE_CONNECT));
        assert_eq!(u16::from_le_bytes([sqe[42], sqe[43]]), 5, "qid");
        assert_eq!(u16::from_le_bytes([sqe[44], sqe[45]]), 127, "0-based sqsize");
        assert_eq!(u16::from_le_bytes([cap[CMD_HLEN + 16], cap[CMD_HLEN + 17]]), 0x21, "cntlid");

        let mut resp = [0u8; 24];
        resp[0] = PDU_CAPSULE_RESP;
        resp[2] = 24;
        resp[4..8].copy_from_slice(&24u32.to_le_bytes());
        let (ch, psh) = read_hdr(&mut &resp[..]).unwrap();
        assert!(check_io_connect_resp(5, &ch, &psh).is_ok());
        resp[22..24].copy_from_slice(&(0x182u16 << 1).to_le_bytes()); // Connect Invalid Parameters
        let (ch, psh) = read_hdr(&mut &resp[..]).unwrap();
        assert!(check_io_connect_resp(5, &ch, &psh).is_err());
    }
}
