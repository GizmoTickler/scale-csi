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

use crate::{ctrls, env_u64, qengine, serve_request, setup_queue_ring};
use libublk::helpers::IoBuf;
use libublk::io::{UblkBatchBuffers, UblkBatchCompletion, UblkBatchConfig, UblkBatchQueue, UblkDev, UblkQueue};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// What the threads of one queue share: whether thread 0 has prepared the
/// queue's tags (the others may post their fetches only then), and the
/// queue's tag buffers in the copying mode.
pub struct QueueShared {
    st: Mutex<Prep>,
    cv: Condvar,
}

impl Default for QueueShared {
    fn default() -> Self {
        QueueShared { st: Mutex::new(Prep::Waiting), cv: Condvar::new() }
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
) {
    let thread = libublk::io::io_thread_idx();
    let leader = thread == 0;
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
    let mut batch = match UblkBatchQueue::new(&q_rc, buffers, batch_config(leader, spill, depth)) {
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
    let engine = qengine::QEngine::new(eid, ctrls, cfg, net_exe.clone(), stats.clone(), stop, draining);
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
    let mut pending: Vec<UblkBatchCompletion> = Vec::with_capacity(depth as usize);
    let mut cqes: Vec<io_uring::cqueue::Entry> = Vec::with_capacity(dev.tgt.cq_depth as usize);
    let mut arrived: Vec<u16> = Vec::with_capacity(depth as usize);
    let (mut seen_tags, mut seen_spills) = (0u64, 0u64);
    let dev_id = dev.dev_info.dev_id;
    log::info!("ublk device {dev_id} queue {qid} thread {thread}: batch I/O, spill at {} requests{}", batch.config().spill_tags(), if leader { " (prepared the queue)" } else { "" });

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
            break;
        }
        // Stopped: the driver aborted this thread's fetch and every request
        // it had taken is committed. Hand the fetch buffers back, then leave.
        if batch.all_fetches_stopped() && batch.owned_tag_count() == 0 && pending.is_empty() && batch.inflight_commit_count() == 0 {
            match batch.try_begin_shutdown() {
                Ok(_) if batch.is_shutdown_complete() => break,
                Ok(_) => {}
                Err(e) => {
                    log::error!("ublk device {dev_id} queue {qid} thread {thread}: batch shutdown failed: {e}");
                    break;
                }
            }
        }

        // Wait for events (adaptive polling as in the per-tag loop).
        let hot = !spin.is_zero() && (spin_idle || engine.inflight_here() > 0 || batch.owned_tag_count() > 0) && last_event.elapsed() < spin;
        if let Err(e) = poll(if hot { 0 } else { 1 }, &timeout) {
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
