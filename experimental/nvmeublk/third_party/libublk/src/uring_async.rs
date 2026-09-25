use crate::io::UblkQueue;
use crate::with_queue_ring_mut_internal;
use crate::UblkError;
use io_uring::{cqueue, opcode, squeue, types, IoUring};
use slab::Slab;
use std::cell::RefCell;
use std::os::fd::AsRawFd;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct FutureData {
    waker: Option<Waker>,
    result: Option<i32>,
}

std::thread_local! {
    static MY_SLAB: RefCell<Slab<FutureData>> = RefCell::new(Slab::new());
}

/// Where a future's slab key lives in its user_data:
///
/// ```text
///   63       62         61 ........ 16   15 ...... 0
///   Target   NonAsync   slab key         tag / caller data
/// ```
///
/// The key field is 46 bits wide, starting above the 16-bit tag and ending
/// below the two `UblkUringData` flag bits, so a key can never change the
/// tag or turn an io command into target io.  2^46 live futures would need
/// over a PiB of slab, so every key a process can hold fits (`new()` still
/// checks).  The key used to be cut to 16 bits, so future 65,536 shared
/// future 0's user_data and got its CQE; leaked futures (removed from the
/// slab only on Ready) make that many keys reachable.
const KEY_SHIFT: u32 = 16;
const KEY_BITS: u32 = 46;
const KEY_MAX: u64 = (1_u64 << KEY_BITS) - 1;
const KEY_MASK: u64 = KEY_MAX << KEY_SHIFT;

/// User code creates one future with user_data used for submitting
/// uring OP, then future.await returns this uring OP's result.
pub struct UblkUringOpFuture {
    pub user_data: u64,
}

impl UblkUringOpFuture {
    fn get_key(data: u64) -> usize {
        // A key that does not fit usize cannot be in the slab either:
        // map it to usize::MAX, which no slab entry has.
        usize::try_from((data & KEY_MASK) >> KEY_SHIFT).unwrap_or(usize::MAX)
    }

    /// Pack slab `key` above the tag bits of `tgt_io`, or None if it does
    /// not fit the key field.  Bits of `tgt_io` inside the key field are
    /// dropped, since OR-ing them in would corrupt the key.
    fn key_to_user_data(key: usize, tgt_io: u64) -> Option<u64> {
        let key = u64::try_from(key).ok().filter(|k| *k <= KEY_MAX)?;
        Some((key << KEY_SHIFT) | (tgt_io & !KEY_MASK))
    }

    fn try_new(tgt_io: u64) -> Result<Self, UblkError> {
        MY_SLAB.with(|refcell| {
            let mut map = refcell.borrow_mut();

            let key = map.insert(FutureData {
                waker: None,
                result: None,
            });
            match Self::key_to_user_data(key, tgt_io) {
                Some(user_data) => {
                    log::trace!("uring: new future data {:x}/{:x}", user_data, key);
                    Ok(UblkUringOpFuture { user_data })
                }
                None => {
                    map.remove(key);
                    Err(UblkError::OtherError(-libc::EOVERFLOW))
                }
            }
        })
    }

    /// Create a future whose user_data carries a unique slab key.  Bits of
    /// `tgt_io` in the key field (16..=61) are dropped; `new_validate()`
    /// rejects them instead.
    pub fn new(tgt_io: u64) -> Self {
        // Unreachable: the slab would need 2^46 live entries (see KEY_BITS),
        // and its allocation aborts the process long before that.
        Self::try_new(tgt_io).expect("uring future slab key overflow")
    }

    /// Like `new()`, but fails with `InvalidVal` if `data` has bits in the
    /// key field (16..=61), and with `OtherError(-EOVERFLOW)` rather than
    /// panicking if the slab ever outgrew the key field.
    pub fn new_validate(data: u64) -> Result<Self, UblkError> {
        if data & KEY_MASK != 0 {
            return Err(UblkError::InvalidVal);
        }

        Self::try_new(data)
    }

    /// Resolve this future to `res` without a CQE, for an op that failed
    /// before reaching the kernel.  Only valid while no SQE carrying this
    /// future's user_data is in flight.
    pub(crate) fn complete(&self, res: i32) {
        MY_SLAB.with(|refcell| {
            if let Some(fd) = refcell.borrow_mut().get_mut(Self::get_key(self.user_data)) {
                fd.result = Some(res);
                if let Some(w) = &fd.waker {
                    w.wake_by_ref();
                }
            }
        })
    }

    /// Free the slab entry of a future whose SQE was never queued; no CQE
    /// will ever come for it, and dropping it would leak the entry.
    fn discard_unsubmitted(self) {
        MY_SLAB.with(|refcell| {
            refcell
                .borrow_mut()
                .try_remove(Self::get_key(self.user_data));
        })
    }
}

impl Future for UblkUringOpFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        MY_SLAB.with(|refcell| {
            let mut map = refcell.borrow_mut();
            let key = Self::get_key(self.user_data);
            match map.get_mut(key) {
                None => {
                    log::trace!("uring: null slab data {:x}/{:x}", self.user_data, key);
                    Poll::Pending
                }
                Some(fd) => match fd.result {
                    Some(result) => {
                        map.remove(key);
                        log::trace!(
                            "uring: uring io ready data {:x}/{:x} ready",
                            self.user_data,
                            key
                        );
                        Poll::Ready(result)
                    }
                    None => {
                        fd.waker = Some(cx.waker().clone());
                        log::trace!(
                            "uring: uring io pending data {:x}/{:x}",
                            self.user_data,
                            key
                        );
                        Poll::Pending
                    }
                },
            }
        })
    }
}

/// Wakeup the pending task, which will be marked as runnable
/// by smol, and the task's future poll() will be run by smol
/// executor's try_tick()
#[inline]
pub fn ublk_wake_task(data: u64, cqe: &cqueue::Entry) {
    MY_SLAB.with(|refcell| {
        let mut map = refcell.borrow_mut();

        log::trace!(
            "ublk_wake_task: data {:x} user_data {:x} result {}",
            data,
            cqe.user_data(),
            cqe.result()
        );
        let key = UblkUringOpFuture::get_key(data);
        if let Some(fd) = map.get_mut(key) {
            fd.result = Some(cqe.result());
            if let Some(w) = &fd.waker {
                w.wake_by_ref();
            }
        }
    })
}

fn ublk_try_reap_cqe<S: squeue::EntryMarker>(
    ring: &mut IoUring<S>,
    nr_waits: usize,
) -> Option<cqueue::Entry> {
    match ring.submit_and_wait(nr_waits) {
        Err(_) => None,
        _ => ring.completion().next(),
    }
}

fn ublk_process_queue_io(
    exe: &smol::LocalExecutor,
    q: &UblkQueue,
    nr_waits: usize,
) -> Result<i32, UblkError> {
    let res = if !q.is_stopping() {
        q.flush_and_wake_io_tasks(|data, cqe, _| ublk_wake_task(data, cqe), nr_waits)
    } else {
        crate::io::with_queue_ring_mut_internal!(|r: &mut IoUring<squeue::Entry>| {
            match ublk_try_reap_cqe(r, nr_waits) {
                Some(cqe) => {
                    let user_data = cqe.user_data();
                    ublk_wake_task(user_data, &cqe);
                    Ok(1)
                }
                None => Ok(0),
            }
        })
    };
    while exe.try_tick() {}

    res
}

/// Run one task in this local Executor until the task is finished
#[deprecated(
    since = "0.5.0",
    note = "use run_uring_tasks() with custom polling logic instead"
)]
pub fn ublk_run_task<T, F>(
    exe: &smol::LocalExecutor,
    task: &smol::Task<T>,
    handler: F,
) -> Result<(), UblkError>
where
    F: Fn(&smol::LocalExecutor) -> Result<(), UblkError>,
{
    // make sure the spawned task is started by `try_tick()`
    while exe.try_tick() {}
    while !task.is_finished() {
        handler(exe)?;
    }
    Ok(())
}

/// Run one IO task in this local Executor until the task is finished
pub fn ublk_run_io_task<T>(
    exe: &smol::LocalExecutor,
    task: &smol::Task<T>,
    q: &UblkQueue,
    nr_waits: usize,
) -> Result<(), UblkError> {
    let handler = move |exe: &smol::LocalExecutor| -> Result<(), UblkError> {
        let _ = ublk_process_queue_io(exe, q, nr_waits)?;
        Ok(())
    };

    #[allow(deprecated)]
    ublk_run_task(exe, task, handler)
}

/// Run one control task in this local Executor until the task is finished,
/// control task is queued in the thread_local io_uring CTRL_URING.
///
/// The current queue is passed in because some control command depends on
/// IO command, such as START command, so ublk_run_ctrl_task() has to drive
/// both data and control urings.
///
/// Rust isn't friendly for using native poll or epoll, so use one dedicated
/// uring for polling data and control urings.
pub fn ublk_run_ctrl_task<T>(
    exe: &smol::LocalExecutor,
    q: &UblkQueue,
    task: &smol::Task<T>,
) -> Result<(), UblkError> {
    let mut pr: IoUring<squeue::Entry, cqueue::Entry> = IoUring::builder().build(4)?;
    let ctrl_fd =
        crate::ctrl::with_ctrl_ring_internal!(|ring: &IoUring<squeue::Entry128>| ring.as_raw_fd());
    let q_fd = q.as_raw_fd();
    let mut poll_q = true;
    let mut poll_ctrl = true;

    while exe.try_tick() {}
    while !task.is_finished() {
        log::debug!(
            "poll ring: submit and wait, ctrl_fd {} q_fd {}",
            ctrl_fd,
            q_fd
        );

        if poll_q {
            let q_e = opcode::PollAdd::new(types::Fd(q_fd), (libc::POLLIN | libc::POLLOUT) as _);
            let _ = unsafe { pr.submission().push(&q_e.build().user_data(0x01)) };
            poll_q = false;
        }
        if poll_ctrl {
            let ctrl_e =
                opcode::PollAdd::new(types::Fd(ctrl_fd), (libc::POLLIN | libc::POLLOUT) as _);
            let _ = unsafe { pr.submission().push(&ctrl_e.build().user_data(0x02)) };
            poll_ctrl = false;
        }

        pr.submit_and_wait(1)?;
        let cqes: Vec<cqueue::Entry> = pr.completion().map(Into::into).collect();
        for cqe in cqes {
            if cqe.user_data() == 0x1 {
                poll_q = true;
            }
            if cqe.user_data() == 0x2 {
                poll_ctrl = true;
            }
        }

        ublk_process_queue_io(exe, q, 0)?;
        let entry =
            crate::ctrl::with_ctrl_ring_mut_internal!(|ring: &mut IoUring<squeue::Entry128>| {
                ublk_try_reap_cqe(ring, 0)
            });
        if let Some(cqe) = entry {
            ublk_wake_task(cqe.user_data(), &cqe);
            while exe.try_tick() {}
        }
    }
    //PollAdd will be canceled automatically

    Ok(())
}

/// Abstract uring task runner that doesn't depend on specific async executor
///
/// # Arguments:
///
/// * `q`: UblkQueue instance
/// * `run_ops`: Closure to run executor operations (replaces `while exe.try_tick() {}`)
/// * `is_done`: Closure to check if all tasks are finished (replaces task checking)
/// * `poll_uring`: Async closure for uring polling logic - returns bool indicating timeout
/// * `reap_event_ops`: Closure to handle CQE reaping operations - receives timeout bool
///
/// This API abstracts the common uring event handling pattern from handle_uring_events(),
/// making it executor-agnostic and more flexible for different use cases.
///
/// # Examples:
///
/// ```no_run
/// use libublk::{run_uring_tasks, UblkError, ublk_reap_events_with_handler};
/// use libublk::uring_async::ublk_wake_task;
/// use libublk::io::with_queue_ring_mut;
///
/// async fn example_usage(q: &libublk::io::UblkQueue<'_>, exe: &smol::LocalExecutor<'_>, tasks: Vec<smol::Task<()>>) -> Result<(), UblkError> {
///     // Basic usage with smol executor
///     let run_ops = || {
///         while exe.try_tick() {}
///     };
///     let is_done = || tasks.iter().all(|task| task.is_finished());
///     let poll_uring = || async { Ok(false) }; // Simplified for example - no timeout
///     let reap_event_ops = |poll_timeout| {
///         with_queue_ring_mut(q, |r| {
///             ublk_reap_events_with_handler(r, |cqe| {
///                 ublk_wake_task(cqe.user_data(), cqe);
///             })
///         })
///     };
///
///     run_uring_tasks(poll_uring, reap_event_ops, run_ops, is_done).await?;
///     Ok(())
/// }
///
/// async fn example_custom_polling(q: &libublk::io::UblkQueue<'_>) -> Result<(), UblkError> {
///     // Alternative: Custom polling with async file
///     let run_ops = || {};
///     let is_done = || true; // Example condition
///     let poll_uring = || async {
///         libublk::io::with_queue_ring_mut(q, |r| r.submit_and_wait(0))?;
///         Ok(false) // Return false for no timeout
///     };
///     let reap_event_ops = |poll_timeout| {
///         with_queue_ring_mut(q, |r| {
///             ublk_reap_events_with_handler(r, |cqe| {
///                 ublk_wake_task(cqe.user_data(), cqe);
///             })
///         })
///     };
///     run_uring_tasks(poll_uring, reap_event_ops, run_ops, is_done).await?;
///     Ok(())
/// }
/// ```
pub async fn run_uring_tasks<R, I, P, F, W>(
    mut poll_uring: P,
    reap_event_ops: W,
    run_ops: R,
    is_done: I,
) -> Result<(), UblkError>
where
    R: Fn(),
    I: Fn() -> bool,
    P: FnMut() -> F,
    F: std::future::Future<Output = Result<bool, UblkError>>,
    W: Fn(bool) -> Result<bool, UblkError>,
{
    run_ops();
    loop {
        let (poll_timeout, failed) = match poll_uring().await {
            Ok(t) => (t, false),
            _ => (false, true),
        };

        let aborted = reap_event_ops(poll_timeout)?;
        run_ops();

        if (aborted || failed) && is_done() {
            break;
        }
    }
    Ok(())
}

/// Reap completion queue entries and handle them with a custom closure
pub fn ublk_reap_events_with_handler<T, F>(
    ring: &mut io_uring::IoUring<T>,
    mut cqe_handler: F,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
    F: FnMut(&io_uring::cqueue::Entry),
{
    let mut aborted = false;
    loop {
        match ring.completion().next() {
            Some(cqe) => {
                cqe_handler(&cqe);
                if cqe.result() == crate::sys::UBLK_IO_RES_ABORT {
                    aborted = true;
                }
            }
            _ => break,
        };
    }
    Ok(aborted)
}

/// Reap completion queue entries with queue state update and idle management
///
/// This function combines the basic functionality of reaping io events with
/// queue state management similar to what `flush_and_wake_io_tasks()` does.
/// It processes completion queue entries and updates the queue state by:
/// - Counting IO commands completed
/// - Detecting abort conditions
/// - Managing queue idle state based on poll timeout and timeout CQEs
/// - Calling the provided waker_ops closure for each completion
///
/// # Arguments
///
/// * `q`: UblkQueue instance
/// * `poll_timeout`: Boolean indicating if polling timeout occurred
/// * `timeout_data`: Optional timeout user_data to check for timeout CQEs
/// * `waker_ops`: Closure called for each completion queue entry
///
/// # Returns
///
/// Returns `Ok(aborted)` where `aborted` indicates if any IO command was aborted,
/// or an error if the operation failed.
pub fn ublk_reap_io_events_with_update_queue<F>(
    q: &UblkQueue<'_>,
    poll_timeout: bool,
    timeout_data: Option<u64>,
    mut waker_ops: F,
) -> Result<bool, UblkError>
where
    F: FnMut(&io_uring::cqueue::Entry),
{
    crate::io::with_queue_ring_mut_internal!(|ring: &mut IoUring<squeue::Entry>| {
        let mut cmd_cnt = 0u32;
        let mut aborted = false;
        let mut has_timeout = poll_timeout;

        // Builtin closure for counting commands and detecting aborts and timeouts
        let builtin_closure = |cqe: &io_uring::cqueue::Entry| {
            let user_data = cqe.user_data();

            // Check if this is a timeout CQE
            if let Some(timeout_user_data) = timeout_data {
                log::debug!("Timeout CQE received, result: {}", cqe.result());
                if user_data == timeout_user_data && cqe.result() == -libc::ETIME {
                    has_timeout = true;
                }
            }

            // Count IO commands and check for abort
            if crate::io::UblkIOCtx::is_io_command(user_data) {
                cmd_cnt += 1;
                if cqe.result() == crate::sys::UBLK_IO_RES_ABORT {
                    aborted = true;
                }
            }

            // Call the passed waker_ops closure for non-timeout events
            waker_ops(cqe);
        };

        // Reap events with our combined handler
        let result = ublk_reap_events_with_handler(ring, builtin_closure);
        // Handle queue idle state based on CQE types received
        if has_timeout {
            if ring.submission().is_empty() {
                q.enter_queue_idle();
            }
        } else {
            q.exit_queue_idle();
        }

        // Update queue state if we processed any IO commands
        if cmd_cnt > 0 {
            q.update_state_batch(cmd_cnt, aborted);
        }

        result
    })
}

/// Wait and handle I/O events for a ublk queue with customizable polling and completion callbacks
///
/// This function provides a high-level interface for handling I/O events on a ublk queue.
/// It uses the underlying `run_uring_tasks` infrastructure with default uring polling via
/// `with_queue_ring_mut()` and `uring_poll_fn()`.
///
/// # Parameters
/// * `q` - The UblkQueue to handle events for
/// * `idle_secs` - Timeout in seconds for uring polling (None for no timeout)
/// * `run_ops` - Closure called periodically to run operations (e.g., executor tick)
/// * `is_done` - Closure that returns true when event handling should stop
///
/// # Returns
/// Returns `Ok(())` when `is_done()` returns true, or an error if I/O operations fail.
///
/// # Example
/// ```rust,no_run
/// use libublk::uring_async::wait_and_handle_io_events;
/// use libublk::io::UblkQueue;
/// use libublk::UblkError;
///
/// async fn handle_events(q: &UblkQueue<'_>) -> Result<(), UblkError> {
///     let run_ops = || {
///         // Run executor or other periodic operations
///     };
///     let is_done = || {
///         // Check if we should stop handling events
///         false
///     };
///
///     wait_and_handle_io_events(q, Some(20), run_ops, is_done).await
/// }
/// ```
pub async fn wait_and_handle_io_events<R, I>(
    q: &UblkQueue<'_>,
    idle_secs: Option<u64>,
    run_ops: R,
    is_done: I,
) -> Result<(), UblkError>
where
    R: Fn(),
    I: Fn() -> bool,
{
    // Use default uring polling (no smol::Async)
    let poll_uring = || async {
        let timeout = idle_secs.map(|secs| io_uring::types::Timespec::new().sec(secs));
        uring_poll_io_fn::<io_uring::squeue::Entry>(q, timeout, 1)
    };

    let reap_event = |poll_timeout| {
        ublk_reap_io_events_with_update_queue(q, poll_timeout, None, |cqe| {
            ublk_wake_task(cqe.user_data(), cqe)
        })
    };

    run_uring_tasks(poll_uring, reap_event, run_ops, is_done).await
}

pub(crate) fn uring_poll_fn<T>(
    r: &mut io_uring::IoUring<T>,
    timeout: Option<io_uring::types::Timespec>,
    to_wait: usize,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
{
    let ret = if let Some(ts) = timeout {
        let args = io_uring::types::SubmitArgs::new().timespec(&ts);
        r.submitter().submit_with_args(to_wait, &args)
    } else {
        r.submit_and_wait(to_wait)
    };

    match ret {
        Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => Ok(true),
        Err(err) => Err(UblkError::IOError(err)),
        Ok(_) => Ok(false),
    }
}

pub fn uring_poll_io_fn<T>(
    q: &UblkQueue,
    timeout: Option<io_uring::types::Timespec>,
    to_wait: usize,
) -> Result<bool, UblkError>
where
    T: io_uring::squeue::EntryMarker,
{
    crate::io::with_queue_ring_mut_internal!(|r: &mut IoUring<squeue::Entry>| {
        let stopping = q.is_stopping();
        let res = uring_poll_fn(r, timeout, if stopping { 0 } else { to_wait });
        if stopping {
            Err(UblkError::QueueIsDown)
        } else {
            res
        }
    })
}

#[inline]
pub(crate) fn __ublk_submit_sqe_async(
    sqe: io_uring::squeue::Entry,
    user_data: u64,
) -> Result<UblkUringOpFuture, UblkError> {
    let f = UblkUringOpFuture::new_validate(user_data)?;
    let sqe = sqe.user_data(f.user_data);

    // Flush and retry while the SQ is full, and retry transient flush
    // errors (EAGAIN, ENOMEM, EINTR) with backoff, as the old loop retried
    // every flush error.  An error every retry would hit (a failed ring)
    // is returned instead of spinning forever, and the never-queued
    // future's slab entry is freed.
    let res = with_queue_ring_mut_internal!(|r: &mut IoUring<squeue::Entry>| {
        crate::io::queue_ring_push_sqe(r, &sqe)
    });
    if let Err(e) = res {
        f.discard_unsubmitted();
        return Err(e);
    }

    Ok(f)
}

/// Submit an io_uring submission queue entry asynchronously
///
/// This function submits an io_uring SQE and returns a future that completes
/// when the operation finishes.
///
/// # Arguments
///
/// * `sqe` - The io_uring submission queue entry to submit
/// * `user_data` - User data to associate with this operation. This value is used
///   to identify the operation when the completion queue entry is received.
///
/// # Important: Marking Target I/O Operations
///
/// **When issuing ublk uring_cmd operations in the same io_uring context as the queue's
/// thread-local ring, you MUST set the `UblkUringData::Target` bit in the `user_data` parameter.**
///
/// This is critical for proper operation classification:
/// - `UblkUringData::Target` bit set: Indicates this is a target I/O operation
/// - `UblkUringData::Target` bit NOT set: Indicates this is a ublk I/O command from the driver
///
/// The library uses this bit to distinguish between:
/// 1. **IO commands from ublk driver** - Commands originating from `/dev/ublkbN` that need to be
///    handled by the target implementation
/// 2. **Target IO operations** - IO operations submitted by the target code itself (e.g., reads/writes
///    to backing storage, uring_cmd operations for ublk communication)
///
/// # Example
///
/// ```no_run
/// use libublk::uring_async::ublk_submit_sqe_async;
/// use libublk::UblkUringData;
/// use io_uring::opcode;
///
/// async fn example() -> Result<(), libublk::UblkError> {
///     // When submitting a ublk uring_cmd in the same io_uring context,
///     // mark it as target I/O by setting the Target bit
///     let sqe = opcode::Nop::new().build();
///     let result = ublk_submit_sqe_async(sqe, UblkUringData::Target as u64).await?;
///
///     println!("Operation completed with result: {}", result);
///     Ok(())
/// }
/// ```
///
/// # Returns
///
/// Returns `Ok(i32)` with the operation result on success, or `Err(UblkError)` on failure.
///
/// # Errors
///
/// This function can return errors if:
/// - The io_uring submission queue is full and flushing it fails with an
///   error that retrying cannot clear (the ring failed: EBADF, EBADFD,
///   EOWNERDEAD, EEXIST, ...).  Transient flush errors (EAGAIN, ENOMEM,
///   EINTR) are retried, not returned.
/// - The user_data validation fails (see `UblkUringOpFuture::new_validate`)
pub async fn ublk_submit_sqe_async(
    sqe: io_uring::squeue::Entry,
    user_data: u64,
) -> Result<i32, UblkError> {
    let f = __ublk_submit_sqe_async(sqe, user_data)?;

    Ok(f.await)
}

/// Wait and handle incoming IO command
///
/// # Arguments:
///
/// * `q`: UblkQueue instance
/// * `exe`: Local async Executor
///
/// Called in queue context. won't return unless error is observed.
/// Wait and handle any incoming cqe until queue is down.
///
/// This should be the only foreground thing done in queue thread.
#[deprecated(
    since = "0.5.0",
    note = "use wait_and_handle_io_events() instead for better async integration"
)]
pub fn ublk_wait_and_handle_ios(exe: &smol::LocalExecutor, q: &UblkQueue) {
    loop {
        while exe.try_tick() {}
        if q.flush_and_wake_io_tasks(|data, cqe, _| ublk_wake_task(data, cqe), 1)
            .is_err()
        {
            break;
        }
    }
    q.unregister_io_bufs();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::ublk_join_io_tasks;
    use io_uring::opcode;
    use std::time::{Duration, Instant};

    /// Test ublk_submit_sqe_async with NOP operation
    #[test]
    fn test_ublk_submit_sqe_async_nop() -> Result<(), UblkError> {
        let exe = smol::LocalExecutor::new();
        let mut tasks = Vec::new();

        // Create a NOP operation
        let task = exe.spawn(async {
            let nop_sqe = opcode::Nop::new().build().user_data(12345);

            match ublk_submit_sqe_async(nop_sqe, 12345).await {
                Ok(result) => {
                    log::debug!("NOP operation completed with result: {}", result);
                    assert_eq!(result, 0); // NOP should return 0
                }
                Err(e) => {
                    panic!("NOP operation failed: {}", e);
                }
            }
        });

        tasks.push(task);
        ublk_join_io_tasks(&exe, tasks)
    }

    /// Test ublk_submit_sqe_async with timeout operation
    #[test]
    fn test_ublk_submit_sqe_async_timeout() -> Result<(), UblkError> {
        let exe = smol::LocalExecutor::new();
        let mut tasks = Vec::new();

        // Create a timeout operation (100ms)
        let task = exe.spawn(async {
            let timeout_spec = io_uring::types::Timespec::new().sec(0).nsec(100_000_000); // 100ms

            let timeout_sqe = opcode::Timeout::new(&timeout_spec as *const _)
                .build()
                .user_data(54321);

            let start = Instant::now();

            match ublk_submit_sqe_async(timeout_sqe, 54321).await {
                Ok(result) => {
                    let elapsed = start.elapsed();
                    log::debug!(
                        "Timeout operation completed with result: {} after {:?}",
                        result,
                        elapsed
                    );

                    // Timeout should complete in approximately 100ms
                    assert!(elapsed >= Duration::from_millis(90));
                    assert!(elapsed <= Duration::from_millis(200));
                    assert_eq!(result, -62); // -ETIME
                }
                Err(e) => {
                    panic!("Timeout operation failed: {}", e);
                }
            }
        });

        tasks.push(task);
        ublk_join_io_tasks(&exe, tasks)
    }

    /// Test ublk_submit_sqe_async with multiple concurrent operations
    #[test]
    fn test_ublk_submit_sqe_async_concurrent() -> Result<(), UblkError> {
        let exe = smol::LocalExecutor::new();
        let mut tasks = Vec::new();

        // Create multiple concurrent NOP operations
        for i in 0..5 {
            let task = exe.spawn(async move {
                let user_data = 1000 + i;
                let nop_sqe = opcode::Nop::new().build().user_data(user_data);

                match ublk_submit_sqe_async(nop_sqe, user_data).await {
                    Ok(result) => {
                        log::debug!("Concurrent NOP {} completed with result: {}", i, result);
                        assert_eq!(result, 0);
                    }
                    Err(e) => {
                        panic!("Concurrent NOP {} failed: {}", i, e);
                    }
                }
            });
            tasks.push(task);
        }

        ublk_join_io_tasks(&exe, tasks)
    }

    /// More than 65,536 live futures (SCOPE L5/R9): every future's slab key
    /// has to round-trip through its user_data, so no two live futures
    /// share a user_data and a CQE wakes exactly the future that submitted
    /// it.  The old packing cut the key to 16 bits, so future 65,536 got
    /// future 0's user_data and its CQE completed future 0.
    #[test]
    fn test_future_keys_beyond_16_bits() {
        // Own thread: MY_SLAB is thread-local, so keys start at 0 here.
        std::thread::spawn(|| {
            let target = crate::UblkUringData::Target as u64;
            let high = 1_usize << 16;
            let mut futs: Vec<UblkUringOpFuture> = (0..high + 2)
                .map(|_| UblkUringOpFuture::new(target))
                .collect();

            let mut seen = std::collections::HashSet::new();
            for (key, f) in futs.iter().enumerate() {
                assert_eq!(UblkUringOpFuture::get_key(f.user_data), key);
                assert!(
                    seen.insert(f.user_data),
                    "user_data {:x} reused",
                    f.user_data
                );
                assert_ne!(f.user_data & target, 0);
            }

            // Complete future 65,536 through a real CQE carrying its user_data.
            let mut ring = IoUring::new(4).unwrap();
            let sqe = opcode::Nop::new().build().user_data(futs[high].user_data);
            unsafe { ring.submission().push(&sqe).unwrap() };
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().unwrap();
            ublk_wake_task(cqe.user_data(), &cqe);

            // Poll future 0 first: with aliased keys it would take the CQE.
            let poll = |f: &mut UblkUringOpFuture| smol::block_on(smol::future::poll_once(f));
            assert_eq!(poll(&mut futs[0]), None);
            assert_eq!(poll(&mut futs[high]), Some(0));
        })
        .join()
        .unwrap();
    }

    /// The key field sits between the tag and the flag bits: the largest
    /// key round-trips without touching either, one past it is refused,
    /// and caller bits inside the field cannot corrupt the key.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_future_key_field_bounds() {
        let flags = crate::UblkUringData::Target as u64 | crate::UblkUringData::NonAsync as u64;
        let max = KEY_MAX as usize;

        let ud = UblkUringOpFuture::key_to_user_data(max, flags | 0xffff).unwrap();
        assert_eq!(UblkUringOpFuture::get_key(ud), max);
        assert_eq!(ud & !KEY_MASK, flags | 0xffff);
        assert_eq!(UblkUringOpFuture::key_to_user_data(max + 1, 0), None);

        let ud = UblkUringOpFuture::key_to_user_data(5, KEY_MASK | 7).unwrap();
        assert_eq!(UblkUringOpFuture::get_key(ud), 5);
        assert_eq!(ud & 0xffff, 7);

        assert!(UblkUringOpFuture::new_validate(1 << 61).is_err());
        assert!(UblkUringOpFuture::new_validate(flags | 0xffff).is_ok());
    }

    /// An SQE that cannot be queued because the ring failed returns an
    /// error and frees its future's slab entry (it used to spin forever),
    /// and `complete()` resolves a future without a CQE.
    #[test]
    fn test_ublk_submit_sqe_async_failed_ring() {
        // Own thread: its own queue ring and slab.
        std::thread::spawn(|| {
            // A disabled ring accepts SQEs but io_uring_enter() fails with
            // EBADFD, so the flush of a full SQ fails.
            crate::io::ublk_init_task_ring(|cell| {
                let ring = IoUring::builder().setup_r_disabled().build(2)?;
                let _ = cell.set(std::cell::RefCell::new(ring));
                Ok(())
            })
            .unwrap();
            let target = crate::UblkUringData::Target as u64;
            let slab_len = || MY_SLAB.with(|s| s.borrow().len());

            // Two SQEs fill the SQ without a flush.
            let _queued: Vec<_> = (0..2)
                .map(|_| __ublk_submit_sqe_async(opcode::Nop::new().build(), target).unwrap())
                .collect();
            assert_eq!(slab_len(), 2);

            match __ublk_submit_sqe_async(opcode::Nop::new().build(), target) {
                Err(UblkError::IOError(e)) => assert_eq!(e.raw_os_error(), Some(libc::EBADFD)),
                Err(e) => panic!("unexpected error {e}"),
                Ok(_) => panic!("SQE queued on a full, failed ring"),
            }
            assert_eq!(slab_len(), 2);

            let mut f = UblkUringOpFuture::new(0);
            f.complete(-libc::EBADFD);
            assert_eq!(
                smol::block_on(smol::future::poll_once(&mut f)),
                Some(-libc::EBADFD)
            );
            assert_eq!(slab_len(), 2);
        })
        .join()
        .unwrap();
    }

    /// Test ublk_submit_sqe_async error handling with invalid operation
    #[test]
    fn test_ublk_submit_sqe_async_error_handling() -> Result<(), UblkError> {
        let exe = smol::LocalExecutor::new();
        let mut tasks = Vec::new();

        // Create an operation that should fail (invalid file descriptor)
        let task = exe.spawn(async {
            use io_uring::types::Fd;

            let invalid_fd = Fd(-1); // Invalid file descriptor
            let close_sqe = opcode::Close::new(invalid_fd).build().user_data(99999);

            match ublk_submit_sqe_async(close_sqe, 99999).await {
                Ok(result) => {
                    log::debug!("Close operation completed with result: {}", result);
                    // Close with invalid fd should return -EBADF (-9)
                    assert_eq!(result, -9);
                }
                Err(e) => {
                    // This is also acceptable behavior
                    log::debug!("Close operation failed as expected: {}", e);
                }
            }
        });

        tasks.push(task);
        ublk_join_io_tasks(&exe, tasks)
    }
}
