//! Queue-level transport for Linux `UBLK_F_BATCH_IO`.
//!
//! This module owns the generic kernel communication needed by batch ublk
//! queues: preparing IO tags and buffers, maintaining a multishot fetch,
//! recycling provided buffers, and committing groups of results. Target
//! implementations remain responsible for interpreting each fetched tag and
//! performing the actual backend IO.
//!
//! # Several io threads per queue
//!
//! The driver keeps a list of multishot `FETCH_IO_CMDS` per queue and hands
//! every new tag to the *first* one on it; a fetch leaves the list when its
//! provided buffers run out (it completes with `-ENOBUFS`), and a fetch that
//! is posted again joins the list at its tail. Several threads (each with its
//! own io_uring and its own [`UblkBatchQueue`]) may therefore serve one queue:
//! exactly one of them builds the queue with
//! [`UblkBatchConfig::with_prepare_tags`]`(true)` (the `PREP_IO_CMDS` of every
//! tag), and each one then keeps one fetch of its own. A tag is committed by
//! the thread that fetched it (with `UBLK_F_AUTO_BUF_REG` the request buffer
//! is registered in the fetching ring and is only unregistered by a commit
//! from that ring).
//!
//! [`UblkBatchConfig::with_spill_tags`]`(n)` turns the provided buffers into
//! per-thread credits: one buffer holds one tag, a thread keeps `n` minus the
//! tags it holds provided, and gives a buffer back only when it commits a
//! tag. The first thread on the list therefore takes every request until it
//! holds `n`; the next request finds no buffer, its fetch completes with
//! `-ENOBUFS` and leaves the list, and the next thread's fetch takes over. The
//! spilled thread posts its fetch again at once, at the tail, with up to `n`
//! more credits (never more than the queue depth in total), so a queue under
//! pressure rotates over its threads in runs of `n` requests, and a queue
//! whose in-flight count stays below `n` stays on one warm thread.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::mem::{size_of, size_of_val, transmute};
use std::sync::Arc;

use io_uring::{cqueue, opcode, squeue, types};

use crate::helpers::IoBuf;
use crate::io::{defer_queue_cqe, with_task_io_ring_mut, RawSqe, UblkIOCtx, UblkQueue};
use crate::{sys, UblkError};

const IORING_URING_CMD_MULTISHOT: u32 = 1 << 1;
// This user_data encoding is local to the batch transport and is not
// coordinated library-wide. Other users on the same ring must avoid it.
const BATCH_USER_DATA_MAGIC: u64 = 0x424b;
const BATCH_USER_DATA_ID_SHIFT: u32 = 24;
const BATCH_USER_DATA_MAGIC_SHIFT: u32 = 40;
const BATCH_FETCH_OP: u32 = 0xf0;
const BATCH_COMMIT_OP: u32 = 0xf1;
const BATCH_PROVIDE_OP: u32 = 0xf2;
const BATCH_PREP_OP: u32 = 0xf3;
const BATCH_SETUP_PROVIDE_OP: u32 = 0xf4;
const BATCH_REMOVE_OP: u32 = 0xf5;
/// Zoned devices need the zone-append LBA element field, which this
/// transport does not produce.
const UNSUPPORTED_BATCH_FLAGS: u64 = sys::UBLK_F_ZONED as u64;
/// Device flags under which the driver moves no data at fetch/commit time
/// (`ublk_dev_need_map_io()` is false): batch elements then carry no buffer
/// address.
const NO_MAP_IO_FLAGS: u64 =
    (sys::UBLK_F_USER_COPY | sys::UBLK_F_SUPPORT_ZERO_COPY | sys::UBLK_F_AUTO_BUF_REG) as u64;

std::thread_local! {
    static BATCH_BUFFER_GROUPS: RefCell<HashSet<u16>> = RefCell::new(HashSet::new());
}

/// Configuration for the multishot batch fetch and commit pools.
///
/// The default keeps eight provided fetch buffers, two fetch commands, and two
/// commit batches in flight, with space for 128 tags per fetch buffer. Counts
/// must be nonzero, and the fetch-command count cannot exceed the fetch-buffer
/// count. [`UblkBatchQueue::new`] validates these relationships.
///
/// The default fetch buffer group is `0x7000`, an arbitrary high-numbered group
/// rather than an io_uring-reserved value. A batch queue must own its group
/// exclusively on the thread-local io_uring, so callers must choose a different
/// group when target-specific operations on the same ring already use it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkBatchConfig {
    fetch_buffer_count: u16,
    fetch_command_count: u16,
    max_inflight_commits: u16,
    tags_per_fetch_buffer: u16,
    fetch_buffer_group: u16,
    prepare_tags: bool,
    spill_tags: u16,
    lease_tags: u16,
    refill: bool,
}

impl UblkBatchConfig {
    /// Construct the default batch queue configuration.
    pub const fn new() -> Self {
        Self {
            fetch_buffer_count: 8,
            fetch_command_count: 2,
            max_inflight_commits: 2,
            tags_per_fetch_buffer: 128,
            fetch_buffer_group: 0x7000,
            prepare_tags: true,
            spill_tags: 0,
            lease_tags: 0,
            refill: true,
        }
    }

    /// Set the nonzero number of provided fetch buffers.
    #[must_use]
    pub const fn with_fetch_buffer_count(mut self, count: u16) -> Self {
        self.fetch_buffer_count = count;
        self
    }

    /// Set the nonzero number of multishot fetch commands kept in flight.
    ///
    /// This cannot exceed [`fetch_buffer_count`](Self::fetch_buffer_count).
    #[must_use]
    pub const fn with_fetch_command_count(mut self, count: u16) -> Self {
        self.fetch_command_count = count;
        self
    }

    /// Set the nonzero maximum number of completion batches kept in flight.
    #[must_use]
    pub const fn with_max_inflight_commits(mut self, count: u16) -> Self {
        self.max_inflight_commits = count;
        self
    }

    /// Set the nonzero maximum number of tags returned in one fetch buffer.
    #[must_use]
    pub const fn with_tags_per_fetch_buffer(mut self, count: u16) -> Self {
        self.tags_per_fetch_buffer = count;
        self
    }

    /// Set the io_uring provided-buffer group used by batch fetches.
    #[must_use]
    pub const fn with_fetch_buffer_group(mut self, group: u16) -> Self {
        self.fetch_buffer_group = group;
        self
    }

    /// Whether this batch queue sends the queue's `PREP_IO_CMDS` (default
    /// true). With several io threads on one queue exactly one of them
    /// prepares the tags, and the others build their batch queues only after
    /// it has: the driver accepts each tag's preparation once, and refuses a
    /// fetch (`-ENODEV`) while a recovering queue is not prepared yet.
    #[must_use]
    pub const fn with_prepare_tags(mut self, prepare: bool) -> Self {
        self.prepare_tags = prepare;
        self
    }

    /// Spill mode (0 = off, the default): one tag per provided buffer, and
    /// buffers as credits, so this queue's fetch leaves the driver's list
    /// (and the next io thread's fetch takes over) once it holds `tags`
    /// requests; see the module documentation. It replaces
    /// the fetch buffer and fetch command counts: one fetch command, a pool of
    /// queue-depth one-tag buffers, `tags` of them provided at first. Clamped
    /// to the queue depth.
    #[must_use]
    pub const fn with_spill_tags(mut self, tags: u16) -> Self {
        self.spill_tags = tags;
        self
    }

    /// Spill mode: most credits provided at any one time (0 = no lease, the
    /// default: up to `spill_tags` at once). With a lease the spill threshold
    /// is also a hard cap: after the fetch runs out it is posted again only
    /// with credits that keep held requests (weighted) plus credits within
    /// `spill_tags`, else it waits for a commit. A thread that stops turning
    /// its loop (wedged) then takes at most `lease` more requests before its
    /// fetch leaves the driver's list and the queue's next thread takes over.
    #[must_use]
    pub const fn with_lease_tags(mut self, tags: u16) -> Self {
        self.lease_tags = tags;
        self
    }

    /// Spill mode: whether credits are given back as requests are committed
    /// while the fetch is armed (default true). False makes a spill-only
    /// thread: its fetch takes at most its credits, then ends (`-ENOBUFS`)
    /// and goes back to the tail of the driver's list with fresh credits. A
    /// thread that reached the head of the list only because the thread
    /// ahead of it spilled so hands the head back after a bounded run.
    #[must_use]
    pub const fn with_refill(mut self, refill: bool) -> Self {
        self.refill = refill;
        self
    }

    /// Return the lease (0: none).
    pub const fn lease_tags(&self) -> u16 {
        self.lease_tags
    }

    /// Return whether committed requests' credits are given back while the
    /// fetch is armed.
    pub const fn refill(&self) -> bool {
        self.refill
    }

    /// Return the number of provided fetch buffers.
    pub const fn fetch_buffer_count(&self) -> u16 {
        self.fetch_buffer_count
    }

    /// Return the number of multishot fetch commands kept in flight.
    pub const fn fetch_command_count(&self) -> u16 {
        self.fetch_command_count
    }

    /// Return the maximum number of completion batches kept in flight.
    pub const fn max_inflight_commits(&self) -> u16 {
        self.max_inflight_commits
    }

    /// Return the maximum number of tags returned in one fetch buffer.
    pub const fn tags_per_fetch_buffer(&self) -> u16 {
        self.tags_per_fetch_buffer
    }

    /// Return the io_uring provided-buffer group used by batch fetches.
    pub const fn fetch_buffer_group(&self) -> u16 {
        self.fetch_buffer_group
    }

    /// Return whether this batch queue prepares the queue's tags.
    pub const fn prepare_tags(&self) -> bool {
        self.prepare_tags
    }

    /// Return the spill threshold (0: spill mode off).
    pub const fn spill_tags(&self) -> u16 {
        self.spill_tags
    }

    /// The configuration actually used for a queue of `depth` tags: spill
    /// mode derives its fetch pools from the depth.
    fn effective(self, depth: u32) -> Self {
        if self.spill_tags == 0 {
            return self;
        }
        let depth = depth.clamp(1, u16::MAX as u32) as u16;
        Self {
            fetch_buffer_count: depth,
            fetch_command_count: 1,
            tags_per_fetch_buffer: 1,
            spill_tags: self.spill_tags.min(depth),
            lease_tags: self.lease_tags.min(depth),
            ..self
        }
    }
}

impl Default for UblkBatchConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Buffer strategy used by a batch queue.
#[non_exhaustive]
#[derive(Debug)]
pub enum UblkBatchBuffers {
    /// One userspace IO buffer for every queue tag.
    IoBufs(Vec<IoBuf<u8>>),
    /// One userspace IO buffer for every queue tag, shared by all the batch
    /// queues (io threads) serving the same ublk queue. The driver copies
    /// to and from the address given by a tag's last prepare or commit, and
    /// a tag may be fetched by any of the queue's threads, so they must all
    /// name the same buffer for it. A tag is held by one thread at a time.
    Shared(Arc<Vec<IoBuf<u8>>>),
    /// No userspace IO buffers: the device moves data itself
    /// (`UBLK_F_USER_COPY`, `UBLK_F_SUPPORT_ZERO_COPY` or
    /// `UBLK_F_AUTO_BUF_REG`), and batch elements carry no address.
    None,
}

enum BatchBufs {
    Owned(Vec<IoBuf<u8>>),
    Shared(Arc<Vec<IoBuf<u8>>>),
    None,
}

impl BatchBufs {
    fn slice(&self) -> &[IoBuf<u8>] {
        match self {
            BatchBufs::Owned(v) => v,
            BatchBufs::Shared(v) => v,
            BatchBufs::None => &[],
        }
    }
}

/// Result for one completed ublk request.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UblkBatchCompletion {
    /// Queue-wide IO tag passed to [`UblkBatchQueue::handle_cqe`]'s request
    /// callback.
    pub tag: u16,
    /// Number of bytes completed or a negative errno.
    pub result: i32,
}

impl UblkBatchCompletion {
    /// Construct a completion.
    pub const fn new(tag: u16, result: i32) -> Self {
        Self { tag, result }
    }
}

/// `struct ublk_elem_header` followed by a buffer address
/// (`UBLK_BATCH_F_HAS_BUF_ADDR`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct BatchElement {
    tag: u16,
    buffer_index: u16,
    result: i32,
    buffer_address: u64,
}

/// `struct ublk_elem_header` alone: devices that need no buffer address.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct ElemHeader {
    tag: u16,
    buffer_index: u16,
    result: i32,
}

/// Which batch element layout a device takes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ElemLayout {
    /// The driver copies data between the request and a userspace buffer:
    /// each element carries the tag's buffer address.
    BufAddr,
    /// No address. With `auto_reg` (`UBLK_F_AUTO_BUF_REG`) the element's
    /// buffer index is the tag: the driver registers the request's pages at
    /// that index of the fetching ring's buffer table.
    BufIndex { auto_reg: bool, base: u16 },
}

impl ElemLayout {
    fn for_flags(flags: u64) -> Self {
        if flags & NO_MAP_IO_FLAGS == 0 {
            ElemLayout::BufAddr
        } else {
            ElemLayout::BufIndex {
                auto_reg: flags & sys::UBLK_F_AUTO_BUF_REG as u64 != 0,
                base: 0,
            }
        }
    }

    fn elem_bytes(self) -> usize {
        match self {
            ElemLayout::BufAddr => size_of::<BatchElement>(),
            ElemLayout::BufIndex { .. } => size_of::<ElemHeader>(),
        }
    }

    fn header_flags(self) -> u16 {
        match self {
            ElemLayout::BufAddr => sys::UBLK_BATCH_F_HAS_BUF_ADDR as u16,
            ElemLayout::BufIndex { .. } => 0,
        }
    }

    /// The layout of `queue`: its device's flags, and its buffer range on
    /// a shared ring (`UblkQueue::buf_index`).
    fn for_queue(queue: &UblkQueue<'_>) -> Self {
        match Self::for_flags(queue.dev.dev_info.flags) {
            ElemLayout::BufIndex { auto_reg, .. } => ElemLayout::BufIndex {
                auto_reg,
                base: queue.buf_index(0),
            },
            l => l,
        }
    }

    fn buffer_index(self, tag: u16) -> u16 {
        match self {
            ElemLayout::BufIndex { auto_reg: true, base } => base.wrapping_add(tag),
            _ => 0,
        }
    }
}

/// A group of batch elements in one layout.
#[derive(Debug)]
enum Elems {
    Addr(Vec<BatchElement>),
    Index(Vec<ElemHeader>),
}

impl Elems {
    fn with_capacity(layout: ElemLayout, capacity: usize) -> Self {
        match layout {
            ElemLayout::BufAddr => Elems::Addr(Vec::with_capacity(capacity)),
            ElemLayout::BufIndex { .. } => Elems::Index(Vec::with_capacity(capacity)),
        }
    }

    fn len(&self) -> usize {
        match self {
            Elems::Addr(v) => v.len(),
            Elems::Index(v) => v.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Elems::Addr(v) => v.clear(),
            Elems::Index(v) => v.clear(),
        }
    }

    fn push(&mut self, tag: u16, buffer_index: u16, result: i32, buffer_address: u64) {
        match self {
            Elems::Addr(v) => v.push(BatchElement {
                tag,
                buffer_index,
                result,
                buffer_address,
            }),
            Elems::Index(v) => v.push(ElemHeader {
                tag,
                buffer_index,
                result,
            }),
        }
    }

    fn tag(&self, index: usize) -> u16 {
        match self {
            Elems::Addr(v) => v[index].tag,
            Elems::Index(v) => v[index].tag,
        }
    }

    fn elem_bytes(&self) -> usize {
        match self {
            Elems::Addr(_) => size_of::<BatchElement>(),
            Elems::Index(_) => size_of::<ElemHeader>(),
        }
    }

    /// Address of element `index` (one past the end is allowed).
    fn addr_at(&self, index: usize) -> u64 {
        match self {
            Elems::Addr(v) => v[index..].as_ptr() as u64,
            Elems::Index(v) => v[index..].as_ptr() as u64,
        }
    }

    fn tags(&self) -> impl Iterator<Item = u16> + '_ {
        (0..self.len()).map(|i| self.tag(i))
    }
}

struct InflightCommit {
    elements: Elems,
    offset: usize,
}

impl InflightCommit {
    fn remaining_len(&self) -> usize {
        self.elements.len() - self.offset
    }

    fn remaining_addr(&self) -> u64 {
        self.elements.addr_at(self.offset)
    }

    fn advance(&mut self, result: i32) -> Result<bool, UblkError> {
        let element_bytes = self.elements.elem_bytes();
        if result <= 0 || result as usize % element_bytes != 0 {
            return Err(UblkError::OtherError(if result < 0 {
                result
            } else {
                -libc::EIO
            }));
        }

        let completed = result as usize / element_bytes;
        if completed > self.elements.len() - self.offset {
            return Err(UblkError::OtherError(-libc::EIO));
        }
        self.offset += completed;
        Ok(self.offset == self.elements.len())
    }
}

struct BufferRegistrationGuard<'queue, 'dev> {
    queue: &'queue UblkQueue<'dev>,
    completed: bool,
}

impl BufferRegistrationGuard<'_, '_> {
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for BufferRegistrationGuard<'_, '_> {
    fn drop(&mut self) {
        if !self.completed {
            self.queue.dev.notify_queue_setup_failed();
        }
    }
}

struct BufferGroupRegistration {
    group: u16,
    release_on_drop: bool,
}

impl BufferGroupRegistration {
    fn claim(group: u16) -> Result<Self, UblkError> {
        let claimed = BATCH_BUFFER_GROUPS.with(|groups| groups.borrow_mut().insert(group));
        if !claimed {
            return Err(UblkError::OtherError(-libc::EADDRINUSE));
        }
        Ok(Self {
            group,
            release_on_drop: true,
        })
    }

    fn retain_until_queue_shutdown(&mut self) {
        self.release_on_drop = false;
    }

    fn release_after_cleanup(&mut self) {
        self.release_on_drop = true;
    }
}

impl Drop for BufferGroupRegistration {
    fn drop(&mut self) {
        if self.release_on_drop {
            release_buffer_group(self.group);
        }
    }
}

/// Generic transport state for one `UBLK_F_BATCH_IO` queue.
///
/// Create the [`UblkQueue`] first, then construct this transport with the
/// queue's IO buffers. The target can continue using
/// [`UblkQueue::flush_and_wake_io_tasks`] and pass every CQE to
/// [`handle_cqe`](Self::handle_cqe). An `Ok(false)` return means that the CQE
/// does not belong to this transport and must be handled by the target.
///
/// Before dropping the transport, stop the ublk queue and keep handling CQEs
/// until [`try_begin_shutdown`](Self::try_begin_shutdown) starts cleanup and
/// [`is_shutdown_complete`](Self::is_shutdown_complete) returns true. Dropping
/// it earlier deliberately retains allocations that the kernel may still
/// reference.
///
/// # Limitations
///
/// - `UBLK_F_ZONED` is rejected: zone append needs the zone-LBA element field.
/// - With `UBLK_F_USER_COPY`, `UBLK_F_SUPPORT_ZERO_COPY` or
///   `UBLK_F_AUTO_BUF_REG` elements carry no buffer address; with
///   `UBLK_F_AUTO_BUF_REG` each tag's buffer index is the tag itself, so the
///   ring needs a sparse buffer table of at least the queue depth
///   ([`UblkQueue::new`] registers one).
/// - Batch queues must use [`UblkQueue::flush_and_wake_io_tasks`] or drain
///   [`crate::io::pop_deferred_queue_cqe`] themselves. Synchronous
///   setup preserves unrelated queue CQEs for that loop, but the generic async
///   event-loop helpers do not consume those deferred CQEs.
/// - Batch CQE `user_data` reserves the target bit, queue ID in bits `0..=15`,
///   operations `0xf0..=0xf5` in bits `16..=23`, command ID in bits `24..=39`,
///   and marker `0x424b` in bits `40..=55`. Target-specific operations on the
///   same ring must not use that encoding.
///
/// # Example
///
/// ```no_run
/// use libublk::io::{
///     UblkBatchBuffers, UblkBatchCompletion, UblkBatchConfig, UblkBatchQueue, UblkDev,
///     UblkQueue,
/// };
/// use libublk::UblkError;
///
/// fn handle_queue(qid: u16, dev: &UblkDev) -> Result<(), UblkError> {
///     let queue = UblkQueue::new(qid, dev)?;
///     let buffers = dev.alloc_queue_io_bufs();
///     let mut batch = UblkBatchQueue::new(
///         &queue,
///         UblkBatchBuffers::IoBufs(buffers),
///         UblkBatchConfig::default(),
///     )?;
///     let mut pending = Vec::new();
///
///     loop {
///         let mut batch_error = None;
///         queue.flush_and_wake_io_tasks(
///             |_user_data, cqe, _is_last| match batch.handle_cqe(cqe, |batch, tags| {
///                 for &tag in tags {
///                     let iod = queue.get_iod(tag);
///                     let bytes = (iod.nr_sectors << 9) as i32;
///                     let result = match iod.op_flags & 0xff {
///                         libublk::sys::UBLK_IO_OP_READ => {
///                             batch.io_buf_mut(tag).unwrap().zero_buf();
///                             bytes
///                         }
///                         libublk::sys::UBLK_IO_OP_WRITE => bytes,
///                         libublk::sys::UBLK_IO_OP_FLUSH => 0,
///                         _ => -libc::EOPNOTSUPP,
///                     };
///                     pending.push(UblkBatchCompletion::new(tag, result));
///                 }
///                 Ok(())
///             }) {
///                 Ok(true) => {}
///                 Ok(false) => {
///                     // Handle target-specific CQEs here.
///                 }
///                 Err(error) => batch_error = Some(error),
///             },
///             1,
///         )?;
///         if let Some(error) = batch_error {
///             return Err(error);
///         }
///         if !pending.is_empty() && batch.try_submit_completions(&pending)? {
///             pending.clear();
///         }
///         if batch.try_begin_shutdown()? && batch.is_shutdown_complete() {
///             break;
///         }
///     }
///     Ok(())
/// }
/// ```
pub struct UblkBatchQueue<'queue, 'dev> {
    queue: &'queue UblkQueue<'dev>,
    buffers: BatchBufs,
    layout: ElemLayout,
    fetch_buffers: Vec<Box<[u16]>>,
    config: UblkBatchConfig,
    inflight_commits: HashMap<u16, InflightCommit>,
    commit_pool: Vec<Elems>,
    next_commit_id: u16,
    owned_tags: Vec<bool>,
    /// Spill mode: extra credit weight each held tag counts for (large
    /// requests), and their sum; set by the target via `add_tag_weight`.
    tag_extra: Vec<u16>,
    owned_extra: usize,
    topup_pending: bool,
    fetch_enabled: bool,
    /// Number of `true` entries in `owned_tags`.
    owned_count: usize,
    tag_scratch: Vec<bool>,
    request_scratch: Vec<u16>,
    commit_failed: bool,
    inflight_fetches: HashSet<u16>,
    fetch_stopped: bool,
    provide_inflight: HashSet<u16>,
    /// Buffers provided to the kernel and not consumed by a fetch, counted
    /// from when the provide is queued: io_uring may post the multishot
    /// fetch CQE that consumed a buffer before the CQE of the provide that
    /// supplied it. Also what shutdown removes.
    remove_remaining: u16,
    shutdown_started: bool,
    shutdown_complete: bool,
    /// Spill mode: fetch buffers neither provided nor being provided.
    free_slots: Vec<u16>,
    /// Spill mode: the fetch that ended while no buffer was left, posted
    /// again once one is.
    parked_fetch: Option<u16>,
    /// An error from buffer top-up after a commit was already submitted,
    /// reported by the next `handle_cqe`.
    pending_error: Option<UblkError>,
    spills: u64,
    rearms: u64,
    fetched_tags: u64,
}

impl<'queue, 'dev> UblkBatchQueue<'queue, 'dev> {
    /// Prepare all queue tags (unless configured not to) and start the
    /// configured multishot batch fetches.
    ///
    /// The batch queue owns `buffers` because their addresses are supplied to
    /// the kernel and must remain valid while batch IO is active.
    ///
    /// # Arguments:
    ///
    /// * `queue`: ublk queue created for a device with `UBLK_F_BATCH_IO`
    /// * `buffers`: buffer strategy and resources used by the batch queue
    /// * `config`: multishot fetch and commit pool configuration
    ///
    /// # Returns:
    ///
    /// The initialized batch queue.
    ///
    /// # Errors:
    ///
    /// Returns `EOPNOTSUPP` if batch IO is disabled or the device is zoned.
    /// Returns [`UblkError::InvalidVal`] if a device that needs buffer
    /// addresses gets no buffers, the buffer count does not match the queue
    /// depth, an IO buffer is smaller than `max_io_buf_bytes`, a configured
    /// count is zero, the fetch-command count exceeds the fetch-buffer count,
    /// or the initial fetches cannot fit in the thread-local io_uring submission
    /// queue. Returns `EADDRINUSE` if another batch queue on that ring already
    /// owns the configured fetch buffer group.
    pub fn new(
        queue: &'queue UblkQueue<'dev>,
        buffers: UblkBatchBuffers,
        config: UblkBatchConfig,
    ) -> Result<Self, UblkError> {
        let buffers = match buffers {
            UblkBatchBuffers::IoBufs(v) => BatchBufs::Owned(v),
            UblkBatchBuffers::Shared(v) => BatchBufs::Shared(v),
            UblkBatchBuffers::None => BatchBufs::None,
        };
        let mut registration = BufferRegistrationGuard {
            queue,
            completed: false,
        };
        if queue.dev.dev_info.flags & sys::UBLK_F_BATCH_IO as u64 == 0 {
            return Err(UblkError::OtherError(-libc::EOPNOTSUPP));
        }
        let layout = ElemLayout::for_queue(queue);
        let mut config = config.effective(queue.get_depth());
        if let Some(slot) = queue.shared_slot() {
            // One provided-buffer group per queue on a shared ring.
            if config.fetch_buffer_group == UblkBatchConfig::new().fetch_buffer_group {
                config.fetch_buffer_group = config.fetch_buffer_group.wrapping_add(slot.key);
            }
        }
        Self::validate(queue, layout, buffers.slice(), config)?;
        let submission_capacity = with_task_io_ring_mut(|ring| ring.submission().capacity());
        validate_initial_fetch_capacity(config.fetch_command_count, submission_capacity)?;
        let mut group_registration = BufferGroupRegistration::claim(config.fetch_buffer_group)?;

        let mut fetch_buffers = (0..config.fetch_buffer_count)
            .map(|_| vec![0_u16; config.tags_per_fetch_buffer as usize].into_boxed_slice())
            .collect::<Vec<_>>();
        let initial = initial_provided(config);
        let mut provided_buffers = 0;
        for (buffer_id, buffer) in fetch_buffers.iter_mut().enumerate().take(initial as usize) {
            if let Err(error) = Self::provide_fetch_buffer_sync(
                queue,
                config.fetch_buffer_group,
                buffer_id as u16,
                buffer,
            ) {
                if let Err(cleanup_error) = Self::remove_fetch_buffers_sync(
                    queue,
                    config.fetch_buffer_group,
                    provided_buffers,
                ) {
                    log::error!(
                        "failed to clean batch buffer group {} after setup error: {}",
                        config.fetch_buffer_group,
                        cleanup_error
                    );
                    std::mem::forget(fetch_buffers);
                } else {
                    group_registration.release_after_cleanup();
                }
                return Err(error);
            }
            provided_buffers += 1;
            if provided_buffers == 1 {
                // From this point the ring may retain this group even if
                // construction subsequently fails. Do not permit a new batch
                // queue to reuse it until the ring itself is torn down.
                group_registration.retain_until_queue_shutdown();
            }
        }

        if config.prepare_tags {
            if let Err(error) = Self::prepare_tags(queue, layout, buffers.slice()) {
                if let Err(cleanup_error) = Self::remove_fetch_buffers_sync(
                    queue,
                    config.fetch_buffer_group,
                    provided_buffers,
                ) {
                    log::error!(
                        "failed to clean batch buffer group {} after tag preparation error: {}",
                        config.fetch_buffer_group,
                        cleanup_error
                    );
                    std::mem::forget(fetch_buffers);
                } else {
                    group_registration.release_after_cleanup();
                }
                return Err(error);
            }
        }
        if layout == ElemLayout::BufAddr {
            for (tag, buffer) in buffers.slice().iter().enumerate() {
                queue.register_io_buf_internal(tag as u16, buffer);
            }
        }

        let mut inflight_fetches = preallocated_id_set(config.fetch_command_count);
        inflight_fetches.extend(0..config.fetch_command_count);
        Self::enqueue_initial_fetches(queue, config);
        queue
            .dev
            .notify_buffer_registration_complete(queue.is_mlock_failed());
        registration.complete();

        let depth = queue.get_depth() as usize;
        Ok(Self {
            queue,
            buffers,
            layout,
            fetch_buffers,
            config,
            inflight_commits: preallocated_commit_map(config.max_inflight_commits),
            commit_pool: (0..config.max_inflight_commits)
                .map(|_| Elems::with_capacity(layout, depth))
                .collect(),
            next_commit_id: 0,
            owned_tags: vec![false; depth],
            tag_extra: vec![0; depth],
            owned_extra: 0,
            topup_pending: false,
            fetch_enabled: true,
            owned_count: 0,
            tag_scratch: vec![false; depth],
            request_scratch: Vec::with_capacity(config.tags_per_fetch_buffer as usize),
            commit_failed: false,
            inflight_fetches,
            fetch_stopped: false,
            provide_inflight: preallocated_id_set(config.fetch_buffer_count),
            remove_remaining: provided_buffers,
            shutdown_started: false,
            shutdown_complete: false,
            free_slots: (initial..config.fetch_buffer_count).rev().collect(),
            parked_fetch: None,
            pending_error: None,
            spills: 0,
            rearms: 0,
            fetched_tags: 0,
        })
    }

    /// Return the underlying ublk queue.
    #[inline(always)]
    pub fn queue(&self) -> &UblkQueue<'dev> {
        self.queue
    }

    /// Return the configuration in effect (spill mode derives its pools).
    #[inline(always)]
    pub fn config(&self) -> UblkBatchConfig {
        self.config
    }

    /// Return the userspace IO buffer for a currently fetched `tag`.
    #[inline(always)]
    pub fn io_buf(&self, tag: u16) -> Option<&IoBuf<u8>> {
        self.owned_tags
            .get(tag as usize)
            .copied()
            .filter(|owned| *owned)
            .and_then(|_| self.buffers.slice().get(tag as usize))
    }

    /// Return the mutable userspace IO buffer for a currently fetched `tag`
    /// (`None` for shared buffers, which are not exclusively this queue's).
    ///
    /// Target code can use this method to fill the buffer before completing a
    /// READ request.
    #[inline(always)]
    pub fn io_buf_mut(&mut self, tag: u16) -> Option<&mut IoBuf<u8>> {
        let owned = self.owned_tags.get(tag as usize).copied().unwrap_or(false);
        match &mut self.buffers {
            BatchBufs::Owned(v) if owned => v.get_mut(tag as usize),
            _ => None,
        }
    }

    /// Return the number of completion batches awaiting kernel acknowledgement.
    #[inline(always)]
    pub fn inflight_commit_count(&self) -> usize {
        self.inflight_commits.len()
    }

    /// Return the number of tags fetched by this queue and not yet handed
    /// to [`try_submit_completions`](Self::try_submit_completions).
    #[inline(always)]
    pub fn owned_tag_count(&self) -> usize {
        self.owned_count
    }

    /// Return how many times this queue's fetch ended because it had no
    /// buffer left (spill mode: it held its share and the next thread took
    /// over).
    #[inline(always)]
    pub fn add_tag_weight(&mut self, tag: u16, extra: u16) {
        if let Some(w) = self.tag_extra.get_mut(tag as usize) {
            if self.owned_tags.get(tag as usize).copied().unwrap_or(false) {
                *w = w.saturating_add(extra);
                self.owned_extra += extra as usize;
            }
        }
    }

    /// Spill mode: top the credits up after a fetch, once the fetched
    /// requests have been weighed. A no-op when nothing is pending.
    pub fn settle_credits(&mut self) -> Result<(), UblkError> {
        if self.spill_mode() && std::mem::take(&mut self.topup_pending) {
            if self.config.refill {
                self.top_up()?;
            }
            if let Some(id) = self.parked_fetch {
                if !self.config.refill {
                    self.overdraft()?;
                }
                self.arm_or_park(id)?;
            }
        }
        Ok(())
    }

    pub fn spill_count(&self) -> u64 {
        self.spills
    }

    /// Spill mode: change this thread's credit policy (see
    /// [`UblkBatchConfig::with_spill_tags`], [`with_lease_tags`](UblkBatchConfig::with_lease_tags),
    /// [`with_refill`](UblkBatchConfig::with_refill)) while it runs, e.g. to
    /// promote a secondary thread when its queue's primary stopped turning.
    /// Credits already provided stay provided; new ones follow the new
    /// policy. Values are clamped to the queue depth.
    pub fn set_credit_policy(&mut self, spill: u16, lease: u16, refill: bool) -> Result<(), UblkError> {
        if !self.spill_mode() {
            return Err(UblkError::InvalidVal);
        }
        let depth = (self.owned_tags.len().max(1)).min(u16::MAX as usize) as u16;
        self.config.spill_tags = spill.clamp(1, depth);
        self.config.lease_tags = lease.min(depth);
        self.config.refill = refill;
        if refill {
            self.top_up()?;
        }
        if let Some(id) = self.parked_fetch {
            if !refill {
                self.overdraft()?;
            }
            self.arm_or_park(id)?;
        }
        Ok(())
    }

    /// Stop replenishing fetch credits without cancelling owned requests or
    /// touching registered pages. Already provided credits drain normally.
    /// Re-enable before device shutdown so a parked fetch observes ABORT.
    pub fn set_fetch_enabled(&mut self, enabled: bool) -> Result<(), UblkError> {
        let resume = reconcile_fetch(enabled, self.fetch_enabled, self.parked_fetch.is_some());
        self.fetch_enabled = enabled;
        if resume { self.after_tags_released()?; }
        Ok(())
    }

    /// An empty queue needs a new FETCH submission to hand an ENOBUFS
    /// event to the next fetcher. Keep one idle probe without enabling
    /// normal replenishment on a suspended lane. Owned requests stay put.
    pub fn probe_parked_fetch(&mut self) -> Result<(), UblkError> {
        if !idle_probe_needed(self.fetch_enabled, self.parked_fetch.is_some(), self.owned_count, self.inflight_fetches.len()) { return Ok(()); }
        if self.provided() == 0 && !self.provide_slot()? { return Ok(()); }
        if let Some(id) = self.parked_fetch { self.arm_or_park(id)?; }
        Ok(())
    }

    /// Spill mode: credits provided right now.
    pub fn provided_credit_count(&self) -> usize {
        self.provided()
    }

    /// Return how many times a fetch was posted again after it ended.
    #[inline(always)]
    pub fn rearm_count(&self) -> u64 {
        self.rearms
    }

    /// Return how many tags this queue has fetched.
    #[inline(always)]
    pub fn fetched_tag_count(&self) -> u64 {
        self.fetched_tags
    }

    /// Return whether all fetch commands stopped with `UBLK_IO_RES_ABORT`
    /// (or were refused with `-ENODEV` by a queue that is being torn down).
    #[inline(always)]
    pub fn all_fetches_stopped(&self) -> bool {
        self.fetch_stopped
    }

    /// Try to start removing the provided-buffer group from io_uring.
    ///
    /// Call this while draining a stopped queue. Continue passing CQEs to
    /// [`handle_cqe`](Self::handle_cqe) until
    /// [`is_shutdown_complete`](Self::is_shutdown_complete) returns true.
    ///
    /// # Returns:
    ///
    /// `Ok(false)` until the multishot fetch and all completion and
    /// buffer-provide operations have finished. `Ok(true)` means buffer removal
    /// has started or already completed.
    ///
    /// # Errors:
    ///
    /// Returns an error if queuing the provided-buffer removal fails.
    pub fn try_begin_shutdown(&mut self) -> Result<bool, UblkError> {
        if self.shutdown_complete || self.shutdown_started {
            return Ok(true);
        }
        if !shutdown_ready(
            self.fetch_stopped,
            self.queue.is_stopping(),
            self.inflight_commits.len(),
            self.provide_inflight.len(),
        ) {
            return Ok(false);
        }
        self.shutdown_started = true;
        if self.remove_remaining == 0 {
            self.shutdown_complete = true;
            return Ok(true);
        }
        if let Err(error) = self.submit_remove_buffers() {
            self.shutdown_started = false;
            return Err(error);
        }
        Ok(true)
    }

    /// Return whether all kernel references to the owned buffers were removed.
    #[inline(always)]
    pub fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete
    }

    /// Try to submit a group of target results to the kernel.
    ///
    /// Multiple completion groups may be in flight. Each group receives an
    /// internal command ID, and partial kernel commits are resubmitted
    /// automatically by [`handle_cqe`](Self::handle_cqe). In spill mode the
    /// committed tags' credits are provided again, and a fetch that ran out
    /// of buffers is posted again.
    ///
    /// # Arguments:
    ///
    /// * `completions`: results for fetched tags that are ready to be committed
    ///
    /// # Returns:
    ///
    /// `Ok(true)` when the completion command is queued successfully or the
    /// input is empty. `Ok(false)` means all configured commit slots are in
    /// flight; the caller retains the input and can retry after handling CQEs.
    ///
    /// # Errors:
    ///
    /// Returns `EIO` after a previous non-empty completion command failed.
    /// Returns [`UblkError::InvalidVal`] for a tag that was not fetched, a
    /// duplicate tag, or an oversized group. The input slice is never modified.
    pub fn try_submit_completions(
        &mut self,
        completions: &[UblkBatchCompletion],
    ) -> Result<bool, UblkError> {
        if completions.is_empty() {
            return Ok(true);
        }
        if self.commit_failed {
            return Err(UblkError::OtherError(-libc::EIO));
        }
        let Some(mut elements) = acquire_commit_buffer(&mut self.commit_pool) else {
            return Ok(false);
        };
        elements.clear();
        self.tag_scratch.fill(false);
        for completion in completions {
            let tag = completion.tag as usize;
            if !completion_tag_is_valid(&self.owned_tags, &mut self.tag_scratch, tag) {
                self.commit_pool.push(elements);
                return Err(UblkError::InvalidVal);
            }
            let address = match self.layout {
                ElemLayout::BufAddr => match self.buffers.slice().get(tag) {
                    Some(buffer) => buffer.as_ptr() as u64,
                    None => {
                        self.commit_pool.push(elements);
                        return Err(UblkError::InvalidVal);
                    }
                },
                ElemLayout::BufIndex { .. } => 0,
            };
            elements.push(
                completion.tag,
                self.layout.buffer_index(completion.tag),
                completion.result,
                address,
            );
        }
        if elements.len() > u16::MAX as usize {
            elements.clear();
            self.commit_pool.push(elements);
            return Err(UblkError::InvalidVal);
        }
        let commit_id = match self.allocate_commit_id() {
            Ok(commit_id) => commit_id,
            Err(error) => {
                elements.clear();
                self.commit_pool.push(elements);
                return Err(error);
            }
        };
        let mut released_extra = 0usize;
        for tag in elements.tags() {
            self.owned_tags[tag as usize] = false;
            released_extra += std::mem::take(&mut self.tag_extra[tag as usize]) as usize;
        }
        self.owned_extra = self.owned_extra.saturating_sub(released_extra);
        let released = elements.len();
        self.owned_count -= released;

        self.inflight_commits.insert(
            commit_id,
            InflightCommit {
                elements,
                offset: 0,
            },
        );
        if let Err(error) = self.submit_inflight_commit(commit_id) {
            if let Some(mut commit) = self.inflight_commits.remove(&commit_id) {
                for tag in commit.elements.tags() {
                    self.owned_tags[tag as usize] = true;
                }
                self.owned_count += released;
                commit.elements.clear();
                self.commit_pool.push(commit.elements);
            }
            return Err(error);
        }
        // The commit is queued: its tags are no longer ours whatever happens
        // below, so a failure to hand credits back is reported later.
        if let Err(error) = self.after_tags_released() {
            self.pending_error.get_or_insert(error);
        }
        Ok(true)
    }

    /// Consume one CQE if it belongs to the batch transport.
    ///
    /// Every CQE reaped from [`UblkQueue::flush_and_wake_io_tasks`] should be
    /// passed to this method before target-specific handling. Fetched request
    /// tags are passed to `requests` using reusable transport storage. Fetch
    /// buffers are recycled and non-terminal multishot fetches are rearmed
    /// automatically.
    ///
    /// # Arguments:
    ///
    /// * `cqe`: completion queue entry reaped from the queue's io_uring
    /// * `requests`: callback that processes each fetched group of request tags
    ///
    /// # Returns:
    ///
    /// Returns `Ok(true)` when the CQE was consumed by the batch transport and
    /// `Ok(false)` when it belongs to target-specific IO. The `requests`
    /// callback receives each fetched tag batch without allocating.
    ///
    /// # Errors:
    ///
    /// Returns an error for malformed kernel results or if recycling a fetch
    /// buffer, rearming a fetch, or resubmitting a partial commit fails.
    pub fn handle_cqe<F>(&mut self, cqe: &cqueue::Entry, mut requests: F) -> Result<bool, UblkError>
    where
        F: FnMut(&mut Self, &[u16]) -> Result<(), UblkError>,
    {
        let handled = self.handle_cqe_inner(cqe, &mut requests);
        if handled.is_ok() {
            if let Some(error) = self.pending_error.take() {
                return Err(error);
            }
        }
        handled
    }

    fn handle_cqe_inner<F>(&mut self, cqe: &cqueue::Entry, requests: &mut F) -> Result<bool, UblkError>
    where
        F: FnMut(&mut Self, &[u16]) -> Result<(), UblkError>,
    {
        let key = self.queue.ring_key();
        let Some((operation, command_id)) = parse_batch_user_data(cqe.user_data(), key) else {
            return Ok(false);
        };
        match operation {
            BATCH_FETCH_OP => self
                .handle_fetch_cqe(command_id, cqe, requests)
                .map(|()| true),
            BATCH_COMMIT_OP => self
                .handle_commit_cqe(command_id, cqe.result())
                .map(|()| true),
            BATCH_PROVIDE_OP => {
                if !self.provide_inflight.remove(&command_id) {
                    return Err(UblkError::InvalidVal);
                }
                // The buffer was counted as provided when its SQE was queued
                // (a fetch may consume it, and post its CQE, before this CQE
                // arrives). A failed provide never reached the group.
                let result = check_zero_result(cqe.result());
                if result.is_err() {
                    let _ = consume_provided_buffer(&mut self.remove_remaining);
                    if self.spill_mode() {
                        self.free_slots.push(command_id);
                    }
                }
                self.mark_queue_stopping_if_batch_drained();
                result?;
                Ok(true)
            }
            BATCH_REMOVE_OP if command_id == 0 => {
                self.handle_remove_cqe(cqe.result()).map(|()| true)
            }
            _ => Ok(false),
        }
    }

    #[inline(always)]
    fn spill_mode(&self) -> bool {
        self.config.spill_tags != 0
    }

    /// Buffers in the kernel's group or on their way there.
    #[inline(always)]
    fn provided(&self) -> usize {
        self.remove_remaining as usize
    }

    /// Provide one free buffer slot; false if none is free.
    fn provide_slot(&mut self) -> Result<bool, UblkError> {
        let Some(buffer_id) = self.free_slots.pop() else {
            return Ok(false);
        };
        if !self.provide_inflight.insert(buffer_id) {
            return Err(UblkError::InvalidVal);
        }
        let result = match self.fetch_buffers.get_mut(buffer_id as usize) {
            Some(buffer) => Self::provide_fetch_buffer(
                self.queue,
                self.config.fetch_buffer_group,
                buffer_id,
                buffer,
            ),
            None => Err(UblkError::InvalidVal),
        };
        if let Err(error) = result {
            self.provide_inflight.remove(&buffer_id);
            self.free_slots.push(buffer_id);
            return Err(error);
        }
        restore_provided_buffer(&mut self.remove_remaining, self.config.fetch_buffer_count)?;
        Ok(true)
    }

    /// Spill mode: bring the credits back up to the spill threshold.
    fn top_up(&mut self) -> Result<(), UblkError> {
        let n = leased_top_up_count(
            self.config.spill_tags as usize,
            self.config.lease_tags as usize,
            self.provided(),
            self.owned_count + self.owned_extra,
            self.free_slots.len(),
        );
        for _ in 0..admitted_credits(self.fetch_enabled, n) {
            if !self.provide_slot()? {
                break;
            }
        }
        Ok(())
    }

    /// Spill mode, after the fetch ran out of buffers: up to another
    /// threshold's worth of credits for the fetch posted at the list's tail.
    fn overdraft(&mut self) -> Result<(), UblkError> {
        let n = if self.config.lease_tags != 0 {
            // Leased: the threshold is a hard cap (see with_lease_tags).
            leased_top_up_count(
                self.config.spill_tags as usize,
                self.config.lease_tags as usize,
                self.provided(),
                self.owned_count + self.owned_extra,
                self.free_slots.len(),
            )
        } else {
            spill_overdraft_count(
                self.owned_tags.len(),
                self.config.spill_tags as usize,
                self.provided(),
                self.owned_count,
                self.free_slots.len(),
            )
        };
        for _ in 0..admitted_credits(self.fetch_enabled, n) {
            if !self.provide_slot()? {
                break;
            }
        }
        Ok(())
    }

    /// Spill mode: post `fetch_id` again if a buffer is provided for it,
    /// else keep it parked until a commit frees one.
    fn arm_or_park(&mut self, fetch_id: u16) -> Result<(), UblkError> {
        if self.provided() == 0 {
            self.parked_fetch = Some(fetch_id);
            return Ok(());
        }
        self.parked_fetch = None;
        Self::submit_fetch(self.queue, self.config, fetch_id)?;
        self.inflight_fetches.insert(fetch_id);
        self.rearms += 1;
        Ok(())
    }

    fn after_tags_released(&mut self) -> Result<(), UblkError> {
        if !self.spill_mode() {
            return Ok(());
        }
        if self.config.refill {
            self.top_up()?;
        }
        if let Some(fetch_id) = self.parked_fetch {
            if !self.config.refill {
                // Spill-only: credits come back only with a new fetch.
                self.overdraft()?;
            }
            self.arm_or_park(fetch_id)?;
        }
        Ok(())
    }

    fn validate(
        queue: &UblkQueue<'_>,
        layout: ElemLayout,
        buffers: &[IoBuf<u8>],
        config: UblkBatchConfig,
    ) -> Result<(), UblkError> {
        validate_batch_flags(queue.dev.dev_info.flags)?;
        validate_batch_config(config)?;
        match layout {
            ElemLayout::BufAddr => validate_io_buffers(
                buffers,
                queue.get_depth() as usize,
                queue.dev.dev_info.max_io_buf_bytes as usize,
            ),
            ElemLayout::BufIndex { .. } => Ok(()),
        }
    }

    fn prepare_tags(
        queue: &UblkQueue<'_>,
        layout: ElemLayout,
        buffers: &[IoBuf<u8>],
    ) -> Result<(), UblkError> {
        let depth = queue.get_depth() as usize;
        let mut elements = Elems::with_capacity(layout, depth);
        for tag in 0..depth {
            let address = match layout {
                ElemLayout::BufAddr => buffers
                    .get(tag)
                    .ok_or(UblkError::InvalidVal)?
                    .as_ptr() as u64,
                ElemLayout::BufIndex { .. } => 0,
            };
            elements.push(tag as u16, layout.buffer_index(tag as u16), 0, address);
        }
        let entry = batch_command(
            queue.file_slot(),
            sys::UBLK_U_IO_PREP_IO_CMDS,
            batch_header(queue.get_qid(), layout, elements.len()),
            elements.addr_at(0),
            batch_user_data(BATCH_PREP_OP, queue.ring_key(), 0),
        );
        let result = submit_and_wait(
            queue,
            entry,
            batch_user_data(BATCH_PREP_OP, queue.ring_key(), 0),
        )?;
        check_zero_result(result)
    }

    fn allocate_commit_id(&mut self) -> Result<u16, UblkError> {
        allocate_command_id(&self.inflight_commits, &mut self.next_commit_id)
    }

    fn recycle_commit(&mut self, commit_id: u16) {
        recycle_commit_buffer(&mut self.inflight_commits, &mut self.commit_pool, commit_id);
    }

    fn submit_inflight_commit(&self, commit_id: u16) -> Result<(), UblkError> {
        let commit = self
            .inflight_commits
            .get(&commit_id)
            .ok_or(UblkError::InvalidVal)?;
        let entry = batch_command(
            self.queue.file_slot(),
            sys::UBLK_U_IO_COMMIT_IO_CMDS,
            batch_header(self.queue.get_qid(), self.layout, commit.remaining_len()),
            commit.remaining_addr(),
            batch_user_data(BATCH_COMMIT_OP, self.queue.ring_key(), commit_id),
        );
        self.queue.ublk_submit_sqe_sync(entry)
    }

    fn handle_commit_cqe(&mut self, commit_id: u16, result: i32) -> Result<(), UblkError> {
        let advance = {
            let commit = self
                .inflight_commits
                .get_mut(&commit_id)
                .ok_or(UblkError::InvalidVal)?;
            commit.advance(result)
        };
        let complete = match advance {
            Ok(value) => value,
            Err(error) => {
                self.recycle_commit(commit_id);
                self.commit_failed = true;
                self.mark_queue_stopping_if_batch_drained();
                return Err(error);
            }
        };
        if complete {
            self.recycle_commit(commit_id);
            self.mark_queue_stopping_if_batch_drained();
            Ok(())
        } else {
            if let Err(error) = self.submit_inflight_commit(commit_id) {
                self.recycle_commit(commit_id);
                self.commit_failed = true;
                self.mark_queue_stopping_if_batch_drained();
                return Err(error);
            }
            Ok(())
        }
    }

    fn handle_fetch_cqe<F>(
        &mut self,
        fetch_id: u16,
        cqe: &cqueue::Entry,
        requests: &mut F,
    ) -> Result<(), UblkError>
    where
        F: FnMut(&mut Self, &[u16]) -> Result<(), UblkError>,
    {
        if !self.inflight_fetches.contains(&fetch_id) {
            return Err(UblkError::InvalidVal);
        }
        let result = cqe.result();
        let flags = cqe.flags();
        let terminal = match classify_fetch_cqe(result, flags) {
            FetchCqeAction::Stop => {
                self.inflight_fetches.remove(&fetch_id);
                log::debug!(
                    "batch queue {} fetch stopped with {}",
                    self.queue.get_qid(),
                    result
                );
                self.fetch_stopped =
                    self.inflight_fetches.is_empty() && self.parked_fetch.is_none();
                self.mark_queue_stopping_if_batch_drained();
                return Ok(());
            }
            FetchCqeAction::RearmError => {
                self.inflight_fetches.remove(&fetch_id);
                log::debug!(
                    "batch queue {} fetch failed with {}, rearming",
                    self.queue.get_qid(),
                    result
                );
                if self.spill_mode() {
                    if result == -libc::ENOBUFS {
                        // Out of credits: this fetch left the driver's list
                        // and the next io thread's fetch takes the queue's
                        // new requests. Back in at the tail.
                        self.spills += 1;
                        self.overdraft()?;
                    }
                    return self.arm_or_park(fetch_id);
                }
                Self::submit_fetch(self.queue, self.config, fetch_id)?;
                self.inflight_fetches.insert(fetch_id);
                self.rearms += 1;
                return Ok(());
            }
            FetchCqeAction::Requests { terminal } => terminal,
        };
        if terminal {
            self.inflight_fetches.remove(&fetch_id);
        }

        let Some(buffer_id) = cqueue::buffer_select(flags) else {
            if result != 0 {
                return Err(UblkError::InvalidVal);
            }
            // Nothing selected, nothing delivered.
            if terminal {
                self.rearm_after_terminal(fetch_id)?;
            }
            return Ok(());
        };
        consume_provided_buffer(&mut self.remove_remaining)?;
        let mut tags = std::mem::take(&mut self.request_scratch);
        tags.clear();
        let handled = self.take_fetched(buffer_id, result, &mut tags).and_then(|()| {
            if terminal {
                self.rearm_after_terminal(fetch_id)?;
            }
            requests(self, &tags)
        });
        tags.clear();
        self.request_scratch = tags;
        handled
    }

    /// Claim the tags a fetch CQE delivered in `buffer_id` and recycle the
    /// buffer (spill mode: return it to the free slots and top up).
    fn take_fetched(
        &mut self,
        buffer_id: u16,
        result: i32,
        tags: &mut Vec<u16>,
    ) -> Result<(), UblkError> {
        {
            let buffer = self
                .fetch_buffers
                .get(buffer_id as usize)
                .ok_or(UblkError::InvalidVal)?;
            copy_fetched_tags(tags, buffer, result as usize)?;
        }
        claim_tags(&mut self.owned_tags, tags, &mut self.tag_scratch)?;
        self.owned_count += tags.len();
        self.fetched_tags += tags.len() as u64;
        if self.spill_mode() {
            // Credits are topped up by `settle_credits` once the target has
            // weighed the requests it just fetched (`add_tag_weight`).
            self.free_slots.push(buffer_id);
            self.topup_pending = true;
            return Ok(());
        }
        if !self.provide_inflight.insert(buffer_id) {
            return Err(UblkError::InvalidVal);
        }
        let provide_result = {
            let buffer = self
                .fetch_buffers
                .get_mut(buffer_id as usize)
                .ok_or(UblkError::InvalidVal)?;
            Self::provide_fetch_buffer(
                self.queue,
                self.config.fetch_buffer_group,
                buffer_id,
                buffer,
            )
        };
        if let Err(error) = provide_result {
            self.provide_inflight.remove(&buffer_id);
            return Err(error);
        }
        restore_provided_buffer(&mut self.remove_remaining, self.config.fetch_buffer_count)
    }

    fn rearm_after_terminal(&mut self, fetch_id: u16) -> Result<(), UblkError> {
        if self.spill_mode() {
            return self.arm_or_park(fetch_id);
        }
        Self::submit_fetch(self.queue, self.config, fetch_id)?;
        self.inflight_fetches.insert(fetch_id);
        self.rearms += 1;
        Ok(())
    }

    fn submit_remove_buffers(&self) -> Result<(), UblkError> {
        if self.remove_remaining == 0 {
            return Ok(());
        }
        let entry = remove_buffers_entry(
            self.remove_remaining,
            self.config.fetch_buffer_group,
            batch_user_data(BATCH_REMOVE_OP, self.queue.ring_key(), 0),
        );
        self.queue.ublk_submit_sqe_sync(entry)
    }

    fn handle_remove_cqe(&mut self, result: i32) -> Result<(), UblkError> {
        if !self.shutdown_started {
            return Err(UblkError::InvalidVal);
        }
        if result == -libc::ENOENT {
            // The group is empty already: a buffer the count still held was
            // consumed without a CQE (a fetch CQE that could not be posted).
            self.remove_remaining = 0;
            self.shutdown_complete = true;
            return Ok(());
        }
        apply_removed_buffers(&mut self.remove_remaining, result)?;
        if self.remove_remaining == 0 {
            self.shutdown_complete = true;
        } else if let Err(error) = self.submit_remove_buffers() {
            self.shutdown_started = false;
            return Err(error);
        }
        Ok(())
    }

    fn submit_fetch(
        queue: &UblkQueue<'_>,
        config: UblkBatchConfig,
        fetch_id: u16,
    ) -> Result<(), UblkError> {
        queue.ublk_submit_sqe_sync(fetch_entry(queue, config, fetch_id))
    }

    fn enqueue_initial_fetches(queue: &UblkQueue<'_>, config: UblkBatchConfig) {
        with_task_io_ring_mut(|ring| {
            let mut submission = ring.submission();
            let available = submission.capacity().saturating_sub(submission.len());
            assert!(available >= config.fetch_command_count as usize);
            for fetch_id in 0..config.fetch_command_count {
                let entry = fetch_entry(queue, config, fetch_id);
                unsafe {
                    submission
                        .push(&entry)
                        .expect("initial batch fetch capacity was prevalidated");
                }
            }
        });
    }

    fn provide_fetch_buffer_sync(
        queue: &UblkQueue<'_>,
        group: u16,
        buffer_id: u16,
        buffer: &mut [u16],
    ) -> Result<(), UblkError> {
        let result = submit_and_wait(
            queue,
            provide_buffer_entry(
                group,
                buffer_id,
                buffer,
                batch_user_data(BATCH_SETUP_PROVIDE_OP, queue.ring_key(), buffer_id),
            ),
            batch_user_data(BATCH_SETUP_PROVIDE_OP, queue.ring_key(), buffer_id),
        )?;
        check_zero_result(result)
    }

    fn provide_fetch_buffer(
        queue: &UblkQueue<'_>,
        group: u16,
        buffer_id: u16,
        buffer: &mut [u16],
    ) -> Result<(), UblkError> {
        queue.ublk_submit_sqe_sync(provide_buffer_entry(
            group,
            buffer_id,
            buffer,
            batch_user_data(BATCH_PROVIDE_OP, queue.ring_key(), buffer_id),
        ))
    }

    fn remove_fetch_buffers_sync(
        queue: &UblkQueue<'_>,
        group: u16,
        count: u16,
    ) -> Result<(), UblkError> {
        let user_data = batch_user_data(BATCH_REMOVE_OP, queue.ring_key(), 0);
        let mut remaining = count;
        while remaining != 0 {
            let result = submit_and_wait(
                queue,
                remove_buffers_entry(remaining, group, user_data),
                user_data,
            )?;
            apply_removed_buffers(&mut remaining, result)?;
        }
        Ok(())
    }

    fn mark_queue_stopping_if_batch_drained(&self) {
        if batch_activity_drained(
            self.fetch_stopped,
            self.inflight_commits.len(),
            self.provide_inflight.len(),
        ) {
            self.queue.mark_stopping();
        }
    }
}

impl Drop for UblkBatchQueue<'_, '_> {
    fn drop(&mut self) {
        if self.shutdown_complete {
            release_buffer_group(self.config.fetch_buffer_group);
            return;
        }

        log::warn!(
            "batch queue {} dropped before shutdown completed; retaining kernel-referenced buffers",
            self.queue.get_qid()
        );
        std::mem::forget(std::mem::replace(&mut self.buffers, BatchBufs::None));
        std::mem::forget(std::mem::take(&mut self.fetch_buffers));
        std::mem::forget(std::mem::take(&mut self.inflight_commits));
    }
}

/// Buffers provided when a batch queue is built: all of them, or in spill
/// mode the spill threshold's worth.
fn initial_provided(config: UblkBatchConfig) -> u16 {
    if config.spill_tags != 0 {
        let lease = if config.lease_tags != 0 { config.lease_tags } else { u16::MAX };
        config.spill_tags.min(lease).min(config.fetch_buffer_count)
    } else {
        config.fetch_buffer_count
    }
}

/// Spill mode: buffers to provide so that credits (`provided`) plus held
/// tags (`owned`) reach `limit`, bounded by the free slots.
#[cfg(test)]
fn spill_top_up_count(limit: usize, provided: usize, owned: usize, free: usize) -> usize {
    leased_top_up_count(limit, 0, provided, owned, free)
}

/// As `spill_top_up_count`, and with a lease (nonzero) never more than
/// `lease` credits provided at once.
fn leased_top_up_count(limit: usize, lease: usize, provided: usize, owned: usize, free: usize) -> usize {
    let room = limit.saturating_sub(provided + owned);
    let room = if lease != 0 { room.min(lease.saturating_sub(provided)) } else { room };
    room.min(free)
}

/// Only an unowned, disabled parked fetch needs an idle handoff probe.
fn idle_probe_needed(enabled: bool, parked: bool, owned: usize, fetches: usize) -> bool {
    !enabled && parked && owned == 0 && fetches == 0
}

fn reconcile_fetch(enabled: bool, was_enabled: bool, parked: bool) -> bool {
    enabled && (!was_enabled || parked)
}

/// Admission never revokes credits already handed to the kernel. It only
/// gates replenishment, whether driven by a fetch, a spill or a commit.
fn admitted_credits(enabled: bool, available: usize) -> usize {
    if enabled { available } else { 0 }
}

fn spill_overdraft_count(
    depth: usize,
    spill: usize,
    provided: usize,
    owned: usize,
    free: usize,
) -> usize {
    depth.saturating_sub(provided + owned).min(spill).min(free)
}

fn batch_header(qid: u16, layout: ElemLayout, elements: usize) -> sys::ublk_batch_io {
    sys::ublk_batch_io {
        q_id: qid,
        flags: layout.header_flags(),
        nr_elem: elements as u16,
        elem_bytes: layout.elem_bytes() as u8,
        reserved: 0,
        reserved2: 0,
    }
}

fn batch_fetch_header(qid: u16) -> sys::ublk_batch_io {
    sys::ublk_batch_io {
        q_id: qid,
        flags: 0,
        nr_elem: 0,
        elem_bytes: size_of::<u16>() as u8,
        reserved: 0,
        reserved2: 0,
    }
}

fn batch_user_data(operation: u32, qid: u16, command_id: u16) -> u64 {
    crate::UblkUringData::Target as u64
        | qid as u64
        | ((operation & 0xff) as u64) << 16
        | (command_id as u64) << BATCH_USER_DATA_ID_SHIFT
        | BATCH_USER_DATA_MAGIC << BATCH_USER_DATA_MAGIC_SHIFT
}

/// The ring-local key (`UblkQueue::ring_key`) of the batch queue a CQE
/// belongs to, or None if it is not a batch command's: for a loop serving
/// several batch queues on one ring, to route each CQE to its queue.
pub fn batch_cqe_key(user_data: u64) -> Option<u16> {
    let target = crate::UblkUringData::Target as u64;
    let reserved = 0x7f_u64 << 56;
    if user_data & target == 0
        || user_data & reserved != 0
        || (user_data >> BATCH_USER_DATA_MAGIC_SHIFT) & 0xffff != BATCH_USER_DATA_MAGIC
    {
        return None;
    }
    Some(UblkIOCtx::user_data_to_tag(user_data) as u16)
}

fn parse_batch_user_data(user_data: u64, qid: u16) -> Option<(u32, u16)> {
    let target = crate::UblkUringData::Target as u64;
    let reserved = 0x7f_u64 << 56;
    if user_data & target == 0
        || user_data & reserved != 0
        || UblkIOCtx::user_data_to_tag(user_data) != qid as u32
        || (user_data >> BATCH_USER_DATA_MAGIC_SHIFT) & 0xffff != BATCH_USER_DATA_MAGIC
    {
        return None;
    }
    Some((
        UblkIOCtx::user_data_to_op(user_data),
        ((user_data >> BATCH_USER_DATA_ID_SHIFT) & 0xffff) as u16,
    ))
}

fn release_buffer_group(group: u16) {
    BATCH_BUFFER_GROUPS.with(|groups| {
        groups.borrow_mut().remove(&group);
    });
}

fn batch_command(
    file_slot: u32,
    command: u32,
    header: sys::ublk_batch_io,
    address: u64,
    user_data: u64,
) -> squeue::Entry {
    opcode::UringCmd16::new(types::Fixed(file_slot), command)
        .cmd(unsafe { transmute::<sys::ublk_batch_io, [u8; 16]>(header) })
        .addr(Some(address))
        .build()
        .user_data(user_data)
}

fn provide_buffer_entry(
    group: u16,
    buffer_id: u16,
    buffer: &mut [u16],
    user_data: u64,
) -> squeue::Entry {
    opcode::ProvideBuffers::new(
        buffer.as_mut_ptr().cast::<u8>(),
        size_of_val(buffer) as i32,
        1,
        group,
        buffer_id,
    )
    .build()
    .user_data(user_data)
}

fn remove_buffers_entry(count: u16, group: u16, user_data: u64) -> squeue::Entry {
    opcode::RemoveBuffers::new(count, group)
        .build()
        .user_data(user_data)
}

fn fetch_entry(queue: &UblkQueue<'_>, config: UblkBatchConfig, fetch_id: u16) -> squeue::Entry {
    let header = batch_fetch_header(queue.get_qid());
    let mut entry = batch_command(
        queue.file_slot(),
        sys::UBLK_U_IO_FETCH_IO_CMDS,
        header,
        0,
        batch_user_data(BATCH_FETCH_OP, queue.ring_key(), fetch_id),
    )
    .flags(squeue::Flags::BUFFER_SELECT);
    unsafe {
        let raw: &mut RawSqe = transmute(&mut entry);
        raw.rw_flags |= IORING_URING_CMD_MULTISHOT;
        raw.buf_index = config.fetch_buffer_group;
    }
    entry
}

fn validate_batch_flags(flags: u64) -> Result<(), UblkError> {
    if flags & UNSUPPORTED_BATCH_FLAGS != 0 {
        Err(UblkError::OtherError(-libc::EOPNOTSUPP))
    } else {
        Ok(())
    }
}

fn validate_batch_config(config: UblkBatchConfig) -> Result<(), UblkError> {
    if config.fetch_buffer_count == 0
        || config.fetch_command_count == 0
        || config.fetch_command_count > config.fetch_buffer_count
        || config.max_inflight_commits == 0
        || config.tags_per_fetch_buffer == 0
    {
        Err(UblkError::InvalidVal)
    } else {
        Ok(())
    }
}

fn validate_initial_fetch_capacity(fetch_commands: u16, capacity: usize) -> Result<(), UblkError> {
    if fetch_commands as usize > capacity {
        Err(UblkError::InvalidVal)
    } else {
        Ok(())
    }
}

fn validate_io_buffers(
    buffers: &[IoBuf<u8>],
    queue_depth: usize,
    required_bytes: usize,
) -> Result<(), UblkError> {
    if buffers.len() != queue_depth
        || buffers
            .iter()
            .any(|buffer| buffer.as_ptr().is_null() || buffer.len() < required_bytes)
    {
        Err(UblkError::InvalidVal)
    } else {
        Ok(())
    }
}

fn shutdown_ready(
    fetch_stopped: bool,
    queue_stopping: bool,
    inflight_commits: usize,
    inflight_provides: usize,
) -> bool {
    queue_stopping && batch_activity_drained(fetch_stopped, inflight_commits, inflight_provides)
}

fn batch_activity_drained(
    fetch_stopped: bool,
    inflight_commits: usize,
    inflight_provides: usize,
) -> bool {
    fetch_stopped && inflight_commits == 0 && inflight_provides == 0
}

fn completion_tag_is_valid(owned_tags: &[bool], seen: &mut [bool], tag: usize) -> bool {
    if !owned_tags.get(tag).copied().unwrap_or(false) || seen.get(tag).copied().unwrap_or(true) {
        return false;
    }
    seen[tag] = true;
    true
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchCqeAction {
    Stop,
    RearmError,
    Requests { terminal: bool },
}

fn classify_fetch_cqe(result: i32, flags: u32) -> FetchCqeAction {
    // ABORT: the driver cancelled the fetch (queue stopping, or its ring
    // is exiting). ENODEV: a fetch posted again after that was refused
    // (ublk_batch_attach sees force_abort or canceling); posting it again
    // would only spin.
    if result == sys::UBLK_IO_RES_ABORT || result == -libc::ENODEV {
        FetchCqeAction::Stop
    } else if result < 0 {
        FetchCqeAction::RearmError
    } else {
        FetchCqeAction::Requests {
            terminal: !cqueue::more(flags),
        }
    }
}

fn copy_fetched_tags(
    scratch: &mut Vec<u16>,
    buffer: &[u16],
    bytes: usize,
) -> Result<(), UblkError> {
    if bytes > size_of_val(buffer) || bytes % size_of::<u16>() != 0 {
        return Err(UblkError::OtherError(-libc::EIO));
    }
    scratch.clear();
    scratch.extend_from_slice(&buffer[..bytes / size_of::<u16>()]);
    Ok(())
}

fn check_zero_result(result: i32) -> Result<(), UblkError> {
    match result {
        0 => Ok(()),
        result if result < 0 => Err(UblkError::OtherError(result)),
        _ => Err(UblkError::OtherError(-libc::EIO)),
    }
}

fn consume_provided_buffer(available: &mut u16) -> Result<(), UblkError> {
    *available = available.checked_sub(1).ok_or(UblkError::InvalidVal)?;
    Ok(())
}

fn restore_provided_buffer(available: &mut u16, capacity: u16) -> Result<(), UblkError> {
    if *available >= capacity {
        return Err(UblkError::InvalidVal);
    }
    *available += 1;
    Ok(())
}

fn apply_removed_buffers(remaining: &mut u16, result: i32) -> Result<(), UblkError> {
    if result <= 0 || result as u32 > *remaining as u32 {
        return Err(UblkError::OtherError(if result < 0 {
            result
        } else {
            -libc::EIO
        }));
    }
    *remaining -= result as u16;
    Ok(())
}

fn preallocated_commit_map(capacity: u16) -> HashMap<u16, InflightCommit> {
    HashMap::with_capacity(capacity as usize)
}

fn preallocated_id_set(capacity: u16) -> HashSet<u16> {
    HashSet::with_capacity(capacity as usize)
}

fn allocate_command_id<T>(inflight: &HashMap<u16, T>, next_id: &mut u16) -> Result<u16, UblkError> {
    for _ in 0..=u16::MAX {
        let command_id = *next_id;
        *next_id = next_id.wrapping_add(1);
        if !inflight.contains_key(&command_id) {
            return Ok(command_id);
        }
    }
    Err(UblkError::OtherError(-libc::EBUSY))
}

fn acquire_commit_buffer(pool: &mut Vec<Elems>) -> Option<Elems> {
    pool.pop()
}

fn recycle_commit_buffer(
    inflight: &mut HashMap<u16, InflightCommit>,
    pool: &mut Vec<Elems>,
    commit_id: u16,
) -> bool {
    let Some(mut commit) = inflight.remove(&commit_id) else {
        return false;
    };
    commit.elements.clear();
    pool.push(commit.elements);
    true
}

fn claim_tags(owned_tags: &mut [bool], tags: &[u16], seen: &mut [bool]) -> Result<(), UblkError> {
    if seen.len() != owned_tags.len() {
        return Err(UblkError::InvalidVal);
    }
    seen.fill(false);
    for tag in tags {
        let tag = *tag as usize;
        if owned_tags.get(tag).copied().unwrap_or(true) || seen[tag] {
            return Err(UblkError::OtherError(-libc::EIO));
        }
        seen[tag] = true;
    }
    for tag in tags {
        owned_tags[*tag as usize] = true;
    }
    Ok(())
}

fn submit_and_wait(
    queue: &UblkQueue<'_>,
    entry: squeue::Entry,
    expected_user_data: u64,
) -> Result<i32, UblkError> {
    queue.ublk_submit_sqe_sync(entry)?;
    loop {
        let cqe = with_task_io_ring_mut(|ring| {
            ring.submit_and_wait(1).map_err(UblkError::IOError)?;
            ring.completion().next().ok_or(UblkError::InvalidVal)
        })?;
        if cqe.user_data() == expected_user_data {
            return Ok(cqe.result());
        }
        defer_queue_cqe(cqe);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_element_matches_uapi_layout() {
        assert_eq!(size_of::<BatchElement>(), 16);
        assert_eq!(std::mem::align_of::<BatchElement>(), 8);
        assert_eq!(std::mem::offset_of!(BatchElement, result), 4);
        assert_eq!(std::mem::offset_of!(BatchElement, buffer_address), 8);
    }

    #[test]
    fn batch_header_describes_buffer_address_elements() {
        let header = batch_header(7, ElemLayout::BufAddr, 32);
        assert_eq!(header.q_id, 7);
        assert_eq!(header.nr_elem, 32);
        assert_eq!(header.elem_bytes as usize, size_of::<BatchElement>());
        assert_eq!(header.flags, sys::UBLK_BATCH_F_HAS_BUF_ADDR as u16);
    }

    #[test]
    fn fetch_header_describes_u16_tags() {
        let header = batch_fetch_header(5);
        assert_eq!(header.q_id, 5);
        assert_eq!(header.nr_elem, 0);
        assert_eq!(header.flags, 0);
        assert_eq!(header.elem_bytes as usize, size_of::<u16>());
    }

    #[test]
    fn partial_commit_advances_by_element_bytes() {
        let mut commit = InflightCommit {
            elements: Elems::Addr(vec![BatchElement::default(); 3]),
            offset: 0,
        };
        assert!(!commit.advance(size_of::<BatchElement>() as i32).unwrap());
        assert_eq!(commit.remaining_len(), 2);
        assert!(commit
            .advance((2 * size_of::<BatchElement>()) as i32)
            .unwrap());
    }

    #[test]
    fn partial_commit_rejects_invalid_byte_count() {
        let mut commit = InflightCommit {
            elements: Elems::Addr(vec![BatchElement::default()]),
            offset: 0,
        };
        assert!(commit.advance(1).is_err());
        assert!(commit
            .advance((2 * size_of::<BatchElement>()) as i32)
            .is_err());
    }

    /// Several batch queues on one shared ring: each CQE names its queue's
    /// ring-local key, so a loop can route it; other CQEs are not batch
    /// CQEs. Keys, not queue ids: two devices' queue 0 share a ring.
    #[test]
    fn batch_cqes_route_by_ring_key() {
        for key in [0u16, 3, 300, 0x7fff] {
            for op in [BATCH_FETCH_OP, BATCH_COMMIT_OP, BATCH_PROVIDE_OP, BATCH_REMOVE_OP] {
                let ud = batch_user_data(op, key, 9);
                assert_eq!(batch_cqe_key(ud), Some(key));
                assert_eq!(parse_batch_user_data(ud, key), Some((op, 9)));
                assert_eq!(parse_batch_user_data(ud, key ^ 1), None, "another queue's CQE");
            }
        }
        assert_eq!(batch_cqe_key(UblkIOCtx::build_user_data(3, BATCH_FETCH_OP, 7, true)), None);
        assert_eq!(batch_cqe_key(UblkIOCtx::build_user_data_async(3, 1, 7)), None);
        assert_eq!(batch_cqe_key(0), None);
    }

    /// On a shared ring a queue's zero-copy buffer indexes start at its
    /// range's base (every ring serving the queue reserves the same range).
    #[test]
    fn shared_ring_buffer_indexes_start_at_the_base() {
        let zc = ElemLayout::BufIndex { auto_reg: true, base: 512 };
        assert_eq!(zc.buffer_index(0), 512);
        assert_eq!(zc.buffer_index(9), 521);
        let ucopy = ElemLayout::BufIndex { auto_reg: false, base: 512 };
        assert_eq!(ucopy.buffer_index(9), 0, "no auto registration: no index");
    }

    #[test]
    fn batch_user_data_values_are_target_operations_and_distinct() {
        let target = crate::UblkUringData::Target as u64;
        let fetch = batch_user_data(BATCH_FETCH_OP, 3, 7);
        let commit = batch_user_data(BATCH_COMMIT_OP, 3, 7);
        let next_commit = batch_user_data(BATCH_COMMIT_OP, 3, 8);
        let provide = batch_user_data(BATCH_PROVIDE_OP, 3, 7);
        assert_ne!(fetch & target, 0);
        assert_ne!(commit & target, 0);
        assert_ne!(provide & target, 0);
        assert_ne!(fetch, commit);
        assert_ne!(commit, provide);
        assert_ne!(commit, next_commit);
        assert_ne!(fetch, batch_user_data(BATCH_FETCH_OP, 4, 7));
        assert_eq!(UblkIOCtx::user_data_to_tag(fetch), 3);
        assert_eq!(UblkIOCtx::user_data_to_op(fetch), BATCH_FETCH_OP);
        assert_eq!(fetch, 0x8042_4b00_07f0_0003);
        assert_eq!(parse_batch_user_data(fetch, 3), Some((BATCH_FETCH_OP, 7)));
        assert_eq!(parse_batch_user_data(fetch, 4), None);
        assert_eq!(parse_batch_user_data(fetch & !target, 3), None);
        assert_eq!(
            parse_batch_user_data(UblkIOCtx::build_user_data(3, BATCH_FETCH_OP, 7, true), 3,),
            None
        );
    }

    #[test]
    fn concurrent_command_ids_skip_ids_that_are_in_flight() {
        let inflight = HashMap::from([(0, ()), (1, ()), (u16::MAX, ())]);
        let mut next_id = 0;
        assert_eq!(allocate_command_id(&inflight, &mut next_id).unwrap(), 2);
        assert_eq!(next_id, 3);

        next_id = u16::MAX;
        assert_eq!(allocate_command_id(&inflight, &mut next_id).unwrap(), 2);
        assert_eq!(next_id, 3);
    }

    #[test]
    fn default_config_enables_multiple_fetch_commands() {
        let config = UblkBatchConfig::default();
        assert!(config.fetch_command_count() > 1);
        assert!(config.fetch_command_count() <= config.fetch_buffer_count());
        assert!(config.max_inflight_commits() > 1);
    }

    #[test]
    fn config_builder_exposes_each_batch_limit() {
        let config = UblkBatchConfig::new()
            .with_fetch_buffer_count(4)
            .with_fetch_command_count(3)
            .with_max_inflight_commits(5)
            .with_tags_per_fetch_buffer(64)
            .with_fetch_buffer_group(0x1234);

        assert_eq!(config.fetch_buffer_count(), 4);
        assert_eq!(config.fetch_command_count(), 3);
        assert_eq!(config.max_inflight_commits(), 5);
        assert_eq!(config.tags_per_fetch_buffer(), 64);
        assert_eq!(config.fetch_buffer_group(), 0x1234);
    }

    #[test]
    fn invalid_batch_configurations_are_rejected() {
        let valid = UblkBatchConfig::default();
        assert!(validate_batch_config(valid).is_ok());

        for invalid in [
            valid.with_fetch_buffer_count(0),
            valid.with_fetch_command_count(0),
            valid.with_fetch_command_count(valid.fetch_buffer_count() + 1),
            valid.with_max_inflight_commits(0),
            valid.with_tags_per_fetch_buffer(0),
        ] {
            assert!(validate_batch_config(invalid).is_err());
        }
    }

    #[test]
    fn initial_fetches_must_fit_in_submission_queue() {
        assert!(validate_initial_fetch_capacity(4, 4).is_ok());
        assert!(validate_initial_fetch_capacity(5, 4).is_err());
    }

    #[test]
    fn invalid_io_buffer_counts_and_sizes_are_rejected() {
        let buffers = vec![IoBuf::<u8>::new(8), IoBuf::<u8>::new(8)];
        assert!(validate_io_buffers(&buffers, 2, 8).is_ok());
        assert!(validate_io_buffers(&buffers, 1, 8).is_err());
        assert!(validate_io_buffers(&buffers, 2, 9).is_err());
    }

    #[test]
    fn unowned_and_duplicate_completion_tags_are_rejected() {
        let owned = [true, false];
        let mut seen = [false; 2];
        assert!(completion_tag_is_valid(&owned, &mut seen, 0));
        assert!(!completion_tag_is_valid(&owned, &mut seen, 0));

        seen.fill(false);
        assert!(!completion_tag_is_valid(&owned, &mut seen, 1));
        assert!(!completion_tag_is_valid(&owned, &mut seen, 2));
    }

    #[test]
    fn shutdown_requires_all_batch_activity_to_finish() {
        assert!(shutdown_ready(true, true, 0, 0));
        assert!(!shutdown_ready(false, true, 0, 0));
        assert!(!shutdown_ready(true, false, 0, 0));
        assert!(!shutdown_ready(true, true, 1, 0));
        assert!(!shutdown_ready(true, true, 0, 1));
    }

    #[test]
    fn queue_stopping_waits_for_all_batch_activity() {
        assert!(batch_activity_drained(true, 0, 0));
        assert!(!batch_activity_drained(false, 0, 0));
        assert!(!batch_activity_drained(true, 1, 0));
        assert!(!batch_activity_drained(true, 0, 1));
        assert!(!batch_activity_drained(false, 1, 1));
    }

    #[test]
    fn fetch_cqe_errors_stop_or_rearm_as_required() {
        assert_eq!(
            classify_fetch_cqe(sys::UBLK_IO_RES_ABORT, 0),
            FetchCqeAction::Stop
        );
        assert_eq!(
            classify_fetch_cqe(-libc::ENOBUFS, 0),
            FetchCqeAction::RearmError
        );
        assert_eq!(
            classify_fetch_cqe(0, 1 << 1),
            FetchCqeAction::Requests { terminal: false }
        );
        assert_eq!(
            classify_fetch_cqe(0, 0),
            FetchCqeAction::Requests { terminal: true }
        );
    }

    #[test]
    fn request_scratch_reuses_its_allocation() {
        let mut scratch = Vec::with_capacity(4);
        let allocation = scratch.as_ptr();
        copy_fetched_tags(&mut scratch, &[1, 2, 3, 4], 4).unwrap();
        assert_eq!(scratch, [1, 2]);
        assert_eq!(scratch.as_ptr(), allocation);

        copy_fetched_tags(&mut scratch, &[3, 4, 5, 6], 8).unwrap();
        assert_eq!(scratch, [3, 4, 5, 6]);
        assert_eq!(scratch.as_ptr(), allocation);
        assert!(copy_fetched_tags(&mut scratch, &[1], 1).is_err());
        assert!(copy_fetched_tags(&mut scratch, &[1], 4).is_err());
    }

    #[test]
    fn completed_commit_buffer_returns_to_pool_without_reallocation() {
        let mut elements = Vec::with_capacity(4);
        elements.push(BatchElement::default());
        let allocation = elements.as_ptr() as u64;
        let mut inflight = HashMap::from([(
            7,
            InflightCommit {
                elements: Elems::Addr(elements),
                offset: 0,
            },
        )]);
        let mut pool = Vec::new();

        assert!(recycle_commit_buffer(&mut inflight, &mut pool, 7));
        assert!(inflight.is_empty());
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].len(), 0);
        assert_eq!(pool[0].addr_at(0), allocation);
        assert!(!recycle_commit_buffer(&mut inflight, &mut pool, 7));
    }

    #[test]
    fn commit_backpressure_preserves_caller_completions() {
        let completions = vec![UblkBatchCompletion::new(7, 4096)];
        let mut pool = Vec::new();

        assert!(acquire_commit_buffer(&mut pool).is_none());
        assert_eq!(completions, [UblkBatchCompletion::new(7, 4096)]);
    }

    #[test]
    fn inflight_tracking_is_preallocated_to_configured_limits() {
        let commits = preallocated_commit_map(3);
        let fetches = preallocated_id_set(4);
        let provides = preallocated_id_set(8);

        assert!(commits.capacity() >= 3);
        assert!(fetches.capacity() >= 4);
        assert!(provides.capacity() >= 8);
    }

    #[test]
    fn buffer_groups_are_exclusive_within_a_thread_ring() {
        let group = 0x7ffe;
        let first = BufferGroupRegistration::claim(group).unwrap();
        assert!(BufferGroupRegistration::claim(group).is_err());
        drop(first);
        assert!(BufferGroupRegistration::claim(group).is_ok());
    }

    #[test]
    fn cleaned_setup_releases_retained_buffer_group() {
        let group = 0x7ffd;
        let mut registration = BufferGroupRegistration::claim(group).unwrap();
        registration.retain_until_queue_shutdown();
        assert!(BufferGroupRegistration::claim(group).is_err());

        registration.release_after_cleanup();
        drop(registration);
        assert!(BufferGroupRegistration::claim(group).is_ok());
    }

    #[test]
    fn claim_tags_does_not_partially_update_on_error() {
        let mut owned_tags = vec![false; 4];
        let mut seen = vec![false; 4];
        assert!(claim_tags(&mut owned_tags, &[1, 1], &mut seen).is_err());
        assert_eq!(owned_tags, vec![false; 4]);

        assert!(claim_tags(&mut owned_tags, &[2, 4], &mut seen).is_err());
        assert_eq!(owned_tags, vec![false; 4]);

        claim_tags(&mut owned_tags, &[1, 3], &mut seen).unwrap();
        assert_eq!(owned_tags, vec![false, true, false, true]);
    }

    #[test]
    fn zero_result_check_rejects_errors_and_unexpected_counts() {
        assert!(check_zero_result(0).is_ok());
        assert!(check_zero_result(-libc::ENOBUFS).is_err());
        assert!(check_zero_result(1).is_err());
    }

    #[test]
    fn provided_buffer_accounting_tracks_consumption_and_restore() {
        let mut available = 2;
        consume_provided_buffer(&mut available).unwrap();
        assert_eq!(available, 1);
        restore_provided_buffer(&mut available, 2).unwrap();
        assert_eq!(available, 2);

        assert!(restore_provided_buffer(&mut available, 2).is_err());
        available = 0;
        assert!(consume_provided_buffer(&mut available).is_err());
    }

    #[test]
    fn setup_cleanup_accepts_partial_removals_and_rejects_bad_counts() {
        let mut remaining = 4;
        apply_removed_buffers(&mut remaining, 2).unwrap();
        assert_eq!(remaining, 2);
        apply_removed_buffers(&mut remaining, 2).unwrap();
        assert_eq!(remaining, 0);

        let mut remaining = 2;
        assert!(apply_removed_buffers(&mut remaining, 0).is_err());
        assert!(apply_removed_buffers(&mut remaining, 3).is_err());
        assert!(apply_removed_buffers(&mut remaining, -libc::EIO).is_err());
        assert_eq!(remaining, 2);
    }

    #[test]
    fn only_zoned_devices_are_rejected() {
        assert!(validate_batch_flags(0).is_ok());
        for flag in [
            sys::UBLK_F_USER_COPY,
            sys::UBLK_F_SUPPORT_ZERO_COPY,
            sys::UBLK_F_AUTO_BUF_REG,
        ] {
            assert!(validate_batch_flags(flag as u64).is_ok());
        }
        assert!(validate_batch_flags(sys::UBLK_F_ZONED as u64).is_err());
    }

    /// The driver's element size check (ublk_check_batch_cmd_flags): 8
    /// bytes plus 8 for a buffer address; HAS_BUF_ADDR only for devices
    /// that map IO (ublk_check_batch_cmd), buffer index = tag for
    /// AUTO_BUF_REG (the fetching ring's table slot).
    #[test]
    fn element_layout_follows_device_flags() {
        assert_eq!(size_of::<ElemHeader>(), 8);
        assert_eq!(std::mem::offset_of!(ElemHeader, buffer_index), 2);
        assert_eq!(std::mem::offset_of!(ElemHeader, result), 4);

        let copy = ElemLayout::for_flags(sys::UBLK_F_BATCH_IO as u64);
        assert_eq!(copy, ElemLayout::BufAddr);
        let h = batch_header(1, copy, 4);
        assert_eq!(h.elem_bytes, 16);
        assert_eq!(h.flags, sys::UBLK_BATCH_F_HAS_BUF_ADDR as u16);
        assert_eq!(copy.buffer_index(9), 0);

        let zc = ElemLayout::for_flags(
            (sys::UBLK_F_BATCH_IO
                | sys::UBLK_F_AUTO_BUF_REG
                | sys::UBLK_F_USER_COPY
                | sys::UBLK_F_SUPPORT_ZERO_COPY) as u64,
        );
        assert_eq!(zc, ElemLayout::BufIndex { auto_reg: true, base: 0 });
        let h = batch_header(1, zc, 4);
        assert_eq!(h.elem_bytes, 8);
        assert_eq!(h.flags, 0);
        assert_eq!(zc.buffer_index(9), 9);

        let ucopy = ElemLayout::for_flags((sys::UBLK_F_BATCH_IO | sys::UBLK_F_USER_COPY) as u64);
        assert_eq!(ucopy, ElemLayout::BufIndex { auto_reg: false, base: 0 });
        assert_eq!(ucopy.buffer_index(9), 0);
    }

    #[test]
    fn packed_elements_advance_by_their_own_size() {
        let mut e = Elems::with_capacity(ElemLayout::BufIndex { auto_reg: true, base: 0 }, 4);
        for tag in [3u16, 5, 7] {
            e.push(tag, tag, 4096, 0);
        }
        assert_eq!(e.tags().collect::<Vec<_>>(), [3, 5, 7]);
        assert_eq!(e.addr_at(1) - e.addr_at(0), 8);
        let mut commit = InflightCommit { elements: e, offset: 0 };
        assert!(!commit.advance(8).unwrap());
        assert_eq!(commit.remaining_len(), 2);
        assert_eq!(commit.remaining_addr() - commit.elements.addr_at(0), 8);
        // A 16-byte step is two packed elements, not one.
        assert!(commit.advance(16).unwrap());
    }

    #[test]
    fn spill_mode_derives_its_fetch_pools_from_the_depth() {
        let c = UblkBatchConfig::new().with_spill_tags(16).effective(64);
        assert_eq!(c.fetch_buffer_count(), 64);
        assert_eq!(c.fetch_command_count(), 1);
        assert_eq!(c.tags_per_fetch_buffer(), 1);
        assert_eq!(c.spill_tags(), 16);
        assert_eq!(initial_provided(c), 16);
        assert!(validate_batch_config(c).is_ok());
        // Clamped to the depth.
        let c = UblkBatchConfig::new().with_spill_tags(200).effective(64);
        assert_eq!(c.spill_tags(), 64);
        assert_eq!(initial_provided(c), 64);
        // Off: untouched.
        let d = UblkBatchConfig::new();
        assert_eq!(d.effective(64), d);
        assert_eq!(initial_provided(d), d.fetch_buffer_count());
        assert!(UblkBatchConfig::new().prepare_tags());
        assert!(!UblkBatchConfig::new().with_prepare_tags(false).prepare_tags());
    }

    /// Credits + held tags stay at the threshold while below it: a thread
    /// at low depth never runs out, one that holds `spill` tags has none.
    #[test]
    fn idle_probe_hands_off_a_parked_fetch_without_moving_owned_io() {
        assert!(idle_probe_needed(false, true, 0, 0));
        assert!(!idle_probe_needed(true, true, 0, 0));
        assert!(!idle_probe_needed(false, false, 0, 0));
        assert!(!idle_probe_needed(false, true, 1, 0));
        assert!(!idle_probe_needed(false, true, 0, 1));
    }

    #[test]
    fn enabled_parked_fetch_retries_even_without_a_policy_transition() {
        assert!(reconcile_fetch(true, true, true));
        assert!(reconcile_fetch(true, false, true));
        assert!(reconcile_fetch(true, false, false));
        assert!(!reconcile_fetch(true, true, false));
        assert!(!reconcile_fetch(false, true, true));
    }

    #[test]
    fn suspension_gates_replenishment_but_resume_keeps_the_credit_bounds() {
        // A fetched credit becomes an owned request; a commit then releases
        // it. Neither event should replenish a suspended tenancy.
        for (provided, held) in [(3, 1), (3, 0), (0, 4), (0, 0)] {
            let available = leased_top_up_count(16, 4, provided, held, 64);
            assert_eq!(admitted_credits(false, available), 0);
            let resumed = admitted_credits(true, available);
            assert!(resumed + provided <= 4);
            assert!(resumed + provided + held <= 16);
        }
        assert_eq!(admitted_credits(true, leased_top_up_count(16, 4, 0, 0, 64)), 4);
    }

    #[test]
    fn spill_top_up_keeps_credits_plus_held_at_the_threshold() {
        // Idle: all 16 credits provided.
        assert_eq!(spill_top_up_count(16, 16, 0, 48), 0);
        // One tag fetched (its buffer consumed): give one back.
        assert_eq!(spill_top_up_count(16, 15, 1, 49), 0);
        // One tag committed: its credit returns.
        assert_eq!(spill_top_up_count(16, 15, 0, 49), 1);
        // Holding 16: no credits, the next request spills.
        assert_eq!(spill_top_up_count(16, 0, 16, 64), 0);
        // After an overdraft (32 held + credits), commits give nothing back
        // until held + credits drop below the threshold again.
        assert_eq!(spill_top_up_count(16, 10, 20, 34), 0);
        assert_eq!(spill_top_up_count(16, 3, 10, 51), 3);
        // Bounded by free slots.
        assert_eq!(spill_top_up_count(16, 0, 0, 5), 5);
    }

    /// Hot lane: a lease bounds the credits provided at once (what a thread
    /// that stops turning can still take), and the threshold is a hard cap.
    #[test]
    fn leased_top_up_bounds_provided_credits() {
        // Primary: cap 48, lease 16. Idle: 16 provided, not 48.
        assert_eq!(leased_top_up_count(48, 16, 0, 0, 64), 16);
        assert_eq!(leased_top_up_count(48, 16, 16, 0, 48), 0);
        // Holding 20 with 10 provided: 6 more (lease), not 18.
        assert_eq!(leased_top_up_count(48, 16, 10, 20, 34), 6);
        // Near the cap: the cap wins.
        assert_eq!(leased_top_up_count(48, 16, 0, 44, 20), 4);
        // Weighted holding over the cap: nothing.
        assert_eq!(leased_top_up_count(48, 16, 0, 60, 60), 0);
        // No lease: the old rule.
        assert_eq!(leased_top_up_count(16, 0, 3, 10, 51), spill_top_up_count(16, 3, 10, 51));
        // Initial credits follow the lease.
        let c = UblkBatchConfig::new().with_spill_tags(48).with_lease_tags(16).effective(64);
        assert_eq!(initial_provided(c), 16);
        let c = UblkBatchConfig::new().with_spill_tags(4).with_lease_tags(4).with_refill(false).effective(64);
        assert_eq!(initial_provided(c), 4);
        assert!(!c.refill());
        assert_eq!(UblkBatchConfig::new().with_spill_tags(16).effective(64).lease_tags(), 0);
        assert!(UblkBatchConfig::new().refill());
    }

    #[test]
    fn spill_overdraft_never_exceeds_the_depth() {
        // Spilled at 16 of 64: another 16 for the fetch at the tail.
        assert_eq!(spill_overdraft_count(64, 16, 0, 16, 64), 16);
        // Near the depth: only what is left.
        assert_eq!(spill_overdraft_count(64, 16, 0, 60, 64), 4);
        // Holding the whole queue: nothing (the fetch parks until a commit).
        assert_eq!(spill_overdraft_count(64, 16, 0, 64, 64), 0);
        // A spill with credits still provided (CQ overflow, not buffers):
        // the total stays within the depth.
        assert_eq!(spill_overdraft_count(64, 16, 40, 20, 24), 4);
    }

    #[test]
    fn fetch_refused_by_a_dying_queue_stops() {
        assert_eq!(classify_fetch_cqe(-libc::ENODEV, 0), FetchCqeAction::Stop);
    }
}
