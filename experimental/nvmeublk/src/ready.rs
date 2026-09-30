//! A bounded runnable set for a reactor. Executor sleepers and ublk CQEs
//! enqueue keys; repeated wakes coalesce. Idle tenancies need no per-turn scan.
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

pub struct Ready {
    bits: [AtomicU64; 8],
    sleeping: AtomicBool,
    fd: i32,
}
impl Ready {
    pub fn new(fd: i32) -> Arc<Self> {
        Arc::new(Self { bits: std::array::from_fn(|_| AtomicU64::new(0)), sleeping: AtomicBool::new(false), fd })
    }
    pub fn mark(&self, key: usize) {
        self.bits[key / 64].fetch_or(1 << (key % 64), Ordering::SeqCst);
        // External executor wakes must also interrupt a sleeping ring. The
        // ring CQE path has sleeping=false and does not write the eventfd.
        if self.sleeping.load(Ordering::SeqCst) && self.fd >= 0 {
            let one = 1u64;
            unsafe { libc::write(self.fd, &one as *const _ as *const libc::c_void, 8); }
        }
    }
    pub fn take(&self, out: &mut Vec<usize>) {
        for (word, a) in self.bits.iter().enumerate() {
            let mut bits = a.swap(0, Ordering::AcqRel);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                out.push(word * 64 + bit);
                bits &= bits - 1;
            }
        }
    }
    pub fn sleeping(&self, on: bool) -> bool {
        self.sleeping.store(on, Ordering::SeqCst);
        // A mark before the sleep flag is published must prevent sleep;
        // a mark afterwards writes the eventfd. No lost wakeup window.
        on && self.bits.iter().all(|b| b.load(Ordering::SeqCst) == 0)
    }
    pub fn waker(self: &Arc<Self>, key: usize) -> Waker {
        Waker::from(Arc::new(KeyWake { ready: self.clone(), key }))
    }
}
struct KeyWake { ready: Arc<Ready>, key: usize }
impl Wake for KeyWake {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) { self.ready.mark(self.key); }
}

/// One shared engine can wake several tenancies. Registration changes only
/// on attach/detach; the executor notifies once when it becomes runnable.
#[derive(Default)]
pub struct Group { wakers: Mutex<Vec<Waker>>, pub driving: AtomicBool }
impl Group {
    pub fn add(&self, w: Waker) { self.wakers.lock().unwrap().push(w); }
    pub fn remove(&self, w: &Waker) { self.wakers.lock().unwrap().retain(|v| !v.will_wake(w)); }
}
impl Wake for Group {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        // run_turn drains all three executors to quiescence already. Waking
        // the reactor as those tasks wake one another adds a redundant turn.
        if self.driving.load(Ordering::Acquire) { return; }
        for w in self.wakers.lock().unwrap().iter() { w.wake_by_ref(); }
    }
}

/// Retain the executor's sleeping ticker, so its next runnable task wakes
/// the reactor. Dropping a temporary tick future would unregister the wake.
pub struct Watch {
    future: Pin<Box<dyn Future<Output = ()>>>,
    waker: Waker,
}
impl Watch {
    pub fn new(exe: std::rc::Rc<smol::LocalExecutor<'static>>, waker: Waker) -> Self {
        Self { future: Box::pin(async move { loop { exe.tick().await; } }), waker }
    }
    pub fn arm(&mut self) {
        assert!(matches!(self.future.as_mut().poll(&mut Context::from_waker(&self.waker)), Poll::Pending));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_external_wake_interrupts_a_sleeping_ring() {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        assert!(fd >= 0);
        let ready = Ready::new(fd);
        assert!(ready.sleeping(true));
        let other = ready.clone();
        std::thread::spawn(move || other.mark(123)).join().unwrap();
        let mut value = 0u64;
        assert_eq!(unsafe { libc::read(fd, &mut value as *mut _ as *mut libc::c_void, 8) }, 8);
        assert_eq!(value, 1);
        assert!(!ready.sleeping(true));
        ready.sleeping(false);
        let mut keys = Vec::new(); ready.take(&mut keys); assert_eq!(keys, vec![123]);
        unsafe { libc::close(fd); }
    }

    #[test]
    fn idle_executors_wake_only_their_runnable_group() {
        let ready = Ready::new(-1);
        let mut groups = Vec::new();
        let mut watches = Vec::new();
        let mut senders = Vec::new();
        for key in 0..512 {
            let exe = std::rc::Rc::new(smol::LocalExecutor::new());
            let group = Arc::new(Group::default());
            group.add(ready.waker(key));
            let (tx, rx) = smol::channel::bounded::<()>(1);
            exe.spawn(async move { rx.recv().await.unwrap(); }).detach();
            let mut watch = Watch::new(exe, Waker::from(group.clone()));
            watch.arm();
            groups.push(group); watches.push(watch); senders.push(tx);
        }
        let mut keys = Vec::new(); ready.take(&mut keys); keys.clear();
        assert!(ready.sleeping(true));
        senders[317].try_send(()).unwrap();
        assert!(!ready.sleeping(true), "work queued before sleeping prevents sleep");
        ready.sleeping(false);
        ready.take(&mut keys);
        assert_eq!(keys, vec![317], "511 idle executors must not be visited");
        watches[317].arm();
        keys.clear(); ready.take(&mut keys);
        groups[317].driving.store(true, Ordering::Release);
        groups[317].wake_by_ref();
        keys.clear(); ready.take(&mut keys); assert!(keys.is_empty());
        groups[317].driving.store(false, Ordering::Release);
        // Duplicate notifications coalesce, including the top key.
        let w = ready.waker(511);
        groups[317].add(w.clone());
        groups[317].wake_by_ref(); groups[317].wake_by_ref();
        keys.clear(); ready.take(&mut keys); assert_eq!(keys, vec![317, 511]);
        groups[317].remove(&w);
        groups[317].wake_by_ref();
        keys.clear(); ready.take(&mut keys); assert_eq!(keys, vec![317]);
    }
}
