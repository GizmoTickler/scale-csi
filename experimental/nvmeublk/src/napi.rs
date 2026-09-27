//! NAPI busy poll for a queue ring, managed per ring (per thread) instead of
//! per engine.
//!
//! io_uring's NAPI registration belongs to the ring: `register_napi` sets the
//! busy-poll budget for every wait on it and `unregister_napi` turns it off
//! for all. Engines used to register and unregister it themselves, which is
//! fine while a ring has one engine (the per-volume threads) and wrong on a
//! reactor ring hosting many: the first engine to go idle turned busy
//! polling off for the others. Engines now say whether they want it
//! (`want`), and the ring has it while any of them does.
//!
//! Budget. With a fixed budget every blocking wait spins that long when no
//! event comes in time: in 128k randwrite 32:4 (responses every ~0.4 ms per
//! thread) NAPI busy polling was ~52% of the daemon's time (gaps/prof4),
//! while turning the budget down or off everywhere cost the small-block
//! depth cells 4-72% (gaps/gF). The adaptive budget (NVMEUBLK_NAPI_ADAPT=1)
//! follows the ring's observed waits instead: the host reports
//! how long each blocking wait lasted (`observe_wait`), and every
//! `EVAL_SAMPLES` waits the budget becomes the smallest step that would have
//! caught three quarters of them polling (`budget_for`), up to the
//! configured maximum, or 0 when even the maximum would not have.

use std::cell::RefCell;
use std::time::{Duration, Instant};

/// Whether the budget adapts by default (NVMEUBLK_NAPI_ADAPT unset).
const ADAPT_BY_DEFAULT: bool = false;

/// Budget steps (µs) the adaptive budget chooses from, below the maximum.
const STEPS: [u32; 4] = [25, 50, 100, 200];
/// Waits per evaluation of the adaptive budget.
const EVAL_SAMPLES: usize = 64;
/// ... or this long, with at least MIN_SAMPLES waits.
const EVAL_EVERY: Duration = Duration::from_millis(100);
const MIN_SAMPLES: usize = 8;
/// Re-registration (it drops the ring's learnt NAPI ids, which come back
/// with the next receives) at most this often.
const MIN_REREG: Duration = Duration::from_millis(50);

/// The budget (µs) for a ring whose recent blocking waits lasted `waits_us`:
/// the smallest step no smaller than 1.2 x their 75th percentile, capped at
/// `max_us`; 0 (no busy polling) when that is above `max_us`. A wait that
/// busy polling would have caught lasts no longer than the gap to its event;
/// one it would not have lasts the gap plus a wakeup, so the percentile is
/// an upper bound on the gaps and the budget errs on the polling side.
pub fn budget_for(waits_us: &mut [u32], max_us: u32) -> u32 {
    if waits_us.is_empty() || max_us == 0 {
        return max_us;
    }
    waits_us.sort_unstable();
    let p75 = waits_us[((waits_us.len() * 3) / 4).min(waits_us.len() - 1)];
    let need = p75.saturating_mul(6) / 5;
    if need > max_us {
        return 0;
    }
    STEPS.iter().copied().find(|&s| s >= need && s <= max_us).unwrap_or(max_us)
}

struct Ctl {
    /// Engines on this ring that want busy polling, and the largest budget
    /// any of them asked for.
    users: u32,
    max_us: u32,
    adaptive: bool,
    /// What the ring has now: None = unregistered.
    registered: Option<u32>,
    /// Budget the adaptive policy wants.
    want_us: u32,
    samples: Vec<u32>,
    last_eval: Instant,
    last_reg: Instant,
    /// Registrations done (stats).
    regs: u64,
}

thread_local! {
    static CTL: RefCell<Ctl> = RefCell::new(Ctl {
        users: 0,
        max_us: 0,
        adaptive: crate::env_u64("NVMEUBLK_NAPI_ADAPT", ADAPT_BY_DEFAULT as u64) != 0,
        registered: None,
        want_us: 0,
        samples: Vec::with_capacity(EVAL_SAMPLES),
        last_eval: Instant::now(),
        last_reg: Instant::now() - MIN_REREG,
        regs: 0,
    });
}

/// An engine on this thread's ring starts (`on`) or stops wanting NAPI busy
/// polling with a budget of `us` µs. The ring has it while any engine does.
pub fn want(on: bool, us: u32) {
    if us == 0 {
        return;
    }
    CTL.with(|c| {
        let mut c = c.borrow_mut();
        if on {
            c.users += 1;
            c.max_us = c.max_us.max(us);
            if c.registered.is_none() {
                // First user after the ring had none: start from the maximum
                // (the adaptive budget re-learns from there).
                c.want_us = c.max_us;
                c.samples.clear();
            }
        } else {
            c.users = c.users.saturating_sub(1);
        }
        apply(&mut c, true);
    });
}

/// The ring's host waited `waited` in a blocking wait (NAPI busy polling
/// included, if registered).
pub fn observe_wait(waited: Duration) {
    CTL.with(|c| {
        let mut c = c.borrow_mut();
        if !c.adaptive || c.users == 0 || c.max_us == 0 {
            return;
        }
        let us = waited.as_micros().min(u32::MAX as u128) as u32;
        c.samples.push(us);
        let now = Instant::now();
        if c.samples.len() >= EVAL_SAMPLES || (c.samples.len() >= MIN_SAMPLES && now.duration_since(c.last_eval) >= EVAL_EVERY) {
            let max = c.max_us;
            let mut s = std::mem::take(&mut c.samples);
            c.want_us = budget_for(&mut s, max);
            s.clear();
            c.samples = s;
            c.last_eval = now;
            apply(&mut c, false);
        }
    });
}

/// (budget registered now, if any; registrations so far) for stats.
pub fn state() -> (Option<u32>, u64) {
    CTL.with(|c| {
        let c = c.borrow();
        (c.registered, c.regs)
    })
}

fn apply(c: &mut Ctl, force: bool) {
    let target = if c.users == 0 { None } else if c.adaptive { Some(c.want_us) } else { Some(c.max_us) };
    if target == c.registered {
        return;
    }
    if !force && c.registered.is_some() && target.is_some() && c.last_reg.elapsed() < MIN_REREG {
        return;
    }
    let r = with_ring(|ring| match target {
        Some(us) => {
            let mut napi = io_uring::types::Napi::new().set_busy_poll_timeout(us).set_prefer_busy_poll(true);
            ring.submitter().register_napi(&mut napi)
        }
        None => ring.submitter().unregister_napi(&mut io_uring::types::Napi::new()),
    });
    match r {
        Some(Ok(())) => {
            c.registered = target;
            c.last_reg = Instant::now();
            c.regs += 1;
        }
        Some(Err(e)) => log::debug!("NAPI {target:?}: {e}"),
        None => {}
    }
}

/// This thread's queue ring, if it has one and it is not in use (never
/// panics, so it is safe from a Drop).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Short waits (events come while polling): a small budget that still
    /// catches them. Waits longer than the maximum: no busy polling. The
    /// fixed-budget behaviour (always the maximum) spun the whole budget on
    /// every wait of a write-heavy depth cell (gaps/prof4).
    #[test]
    fn the_budget_follows_the_observed_waits() {
        let mut short: Vec<u32> = (0..64).map(|i| 5 + i % 10).collect();
        assert_eq!(budget_for(&mut short, 200), 25);
        let mut mid: Vec<u32> = (0..64).map(|i| 40 + i % 30).collect();
        assert_eq!(budget_for(&mut mid, 200), 100, "p75 ~62 us -> 75 -> step 100");
        let mut long: Vec<u32> = (0..64).map(|i| 300 + i).collect();
        assert_eq!(budget_for(&mut long, 200), 0, "gaps beyond the maximum: polling would only burn CPU");
        let mut near: Vec<u32> = (0..64).map(|i| 120 + i % 30).collect();
        assert_eq!(budget_for(&mut near, 200), 200);
        // A fifth of long waits does not turn polling off.
        let mut mixed: Vec<u32> = (0..64).map(|i| if i % 5 == 0 { 5000 } else { 10 }).collect();
        assert_eq!(budget_for(&mut mixed, 200), 25);
        // Never above the configured maximum; 0 configured stays 0.
        let mut m: Vec<u32> = vec![30; 16];
        assert_eq!(budget_for(&mut m, 40), 40);
        assert_eq!(budget_for(&mut m, 0), 0);
        assert_eq!(budget_for(&mut [], 200), 200, "no samples: the maximum");
    }

    /// Busy polling is a property of the ring: it stays on while any engine
    /// on it wants it (one engine going idle used to turn it off for all).
    #[test]
    fn the_ring_has_napi_while_any_engine_wants_it() {
        std::thread::spawn(|| {
            // A ring for this thread (as a queue thread or reactor has).
            libublk::io::ublk_init_task_ring(|cell| {
                let ring = io_uring::IoUring::builder().build(8).map_err(libublk::UblkError::IOError)?;
                let _ = cell.set(RefCell::new(ring));
                Ok(())
            })
            .unwrap();
            CTL.with(|c| c.borrow_mut().adaptive = false);
            want(true, 200);
            want(true, 200);
            let on = state().0;
            want(false, 200);
            assert_eq!(state().0, on, "one engine idle: still on for the other");
            want(false, 200);
            assert_eq!(state().0, None, "no engine wants it: off");
            if on.is_none() {
                eprintln!("NAPI registration unsupported here; checked the bookkeeping only");
            } else {
                assert_eq!(on, Some(200));
            }
        })
        .join()
        .unwrap();
    }
}
