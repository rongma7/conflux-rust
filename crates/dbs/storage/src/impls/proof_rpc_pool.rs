// Copyright 2024 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

//! Dedicated single-threaded rayon pools and a concurrency limiter for
//! proof RPC calls.
//!
//! LVMT proof generation uses rayon `par_iter` inside the `amt` crate.
//! A naive isolation — a single N-thread pool shared by all requests —
//! does *not* bound tail latency under concurrent peers: rayon workers
//! waiting at a `join` happily steal the top-level task of another
//! request and run it to completion on top of the stack, forcing the
//! first request to wait for the stolen one. Measured on 8 peers with
//! one 8-thread pool: p99 ~2.5 s, max ~7 s.
//!
//! Instead the pool here holds `N` **single-threaded** rayon pools.
//! Each in-flight request runs on one dedicated worker so the nested
//! stealing path is gone. The trade-off: each request loses the
//! intra-request par_iter parallelism, so a lone request is slower
//! than it would be on a single multi-thread pool. Under 8 concurrent
//! peers, upstream's reference bench reports p99 ~250 ms with 8
//! single-threaded pools against p99 2.5 s with one 8-thread pool.
//!
//! A single list of free pool indices acts as both the "which pool to
//! use" and the "is there a free pool at all" signal: `pop()` returning
//! `None` means the pool is saturated. The slot is wrapped in a Drop
//! guard, so a panic inside the installed closure still returns the
//! pool to the free list instead of permanently leaking it.
//!
//! The single config knob is set via `StorageConfiguration`:
//! - `lvmt_proof_pool_threads` — number of single-threaded pools, and
//!   therefore both the compute budget and the in-flight cap. Excess
//!   requests get a busy error.

use super::errors::{Error, Result};
use parking_lot::Mutex;
use rayon::ThreadPool;
use std::sync::Arc;

pub struct ProofRpcPool {
    pools: Vec<Arc<ThreadPool>>,
    /// Indices of pools that are currently free. `pop()` returning
    /// `None` is the single source of truth for "all pools busy".
    available: Arc<Mutex<Vec<usize>>>,
}

/// Holds a reserved pool index until dropped. Drop runs on both the
/// success path and during unwinding from a panic inside `install`, so
/// a panicking closure cannot permanently leak a pool slot.
struct PoolSlot<'a> {
    available: &'a Mutex<Vec<usize>>,
    idx: usize,
}

impl<'a> Drop for PoolSlot<'a> {
    fn drop(&mut self) { self.available.lock().push(self.idx); }
}

impl ProofRpcPool {
    pub fn new(num_pools: usize) -> Result<Self> {
        if num_pools == 0 {
            return Err(Error::Msg(
                "proof RPC pool requires at least one worker".into(),
            ));
        }
        let mut pools = Vec::with_capacity(num_pools);
        for i in 0..num_pools {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(1)
                .thread_name(move |_| format!("proof-rpc-{}", i))
                .build()
                .map_err(|e| {
                    Error::Msg(format!(
                        "failed to build proof RPC pool {}: {}",
                        i, e
                    ))
                })?;
            pools.push(Arc::new(pool));
        }
        let available = Arc::new(Mutex::new((0..num_pools).collect()));
        Ok(Self { pools, available })
    }

    pub fn num_pools(&self) -> usize { self.pools.len() }

    pub fn available_permits(&self) -> usize { self.available.lock().len() }

    /// Reserves a free pool and runs `f` on it. Returns `Err(Busy)`
    /// when every pool is already in use. If `f` panics, the panic is
    /// propagated to the caller and the slot is still returned to the
    /// free list.
    pub fn try_run<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        let idx = self.available.lock().pop().ok_or_else(|| {
            Error::Msg("proof RPC pool busy, retry later".into())
        })?;
        let _slot = PoolSlot {
            available: &self.available,
            idx,
        };
        Ok(self.pools[idx].install(f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn try_run_installs_and_releases_slot() {
        let pool = ProofRpcPool::new(2).unwrap();
        assert_eq!(pool.available_permits(), 2);
        let x: u64 = pool.try_run(|| 7u64 * 6).unwrap();
        assert_eq!(x, 42);
        assert_eq!(pool.available_permits(), 2);
    }

    #[test]
    fn try_run_returns_busy_when_saturated() {
        let pool = Arc::new(ProofRpcPool::new(1).unwrap());
        let started = Arc::new(AtomicUsize::new(0));
        let hold = Arc::new(Mutex::new(()));
        let held = hold.lock();

        let pool_inner = Arc::clone(&pool);
        let started_inner = Arc::clone(&started);
        let hold_inner = Arc::clone(&hold);
        let t = std::thread::spawn(move || {
            pool_inner
                .try_run(|| {
                    started_inner.fetch_add(1, Ordering::SeqCst);
                    let _g = hold_inner.lock();
                })
                .unwrap();
        });

        while started.load(Ordering::SeqCst) == 0 {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert!(
            pool.try_run(|| 1u32).is_err(),
            "second run must be busy with only one pool"
        );

        drop(held);
        t.join().unwrap();
        assert_eq!(pool.available_permits(), 1);
    }

    #[test]
    fn try_run_returns_slot_on_panic() {
        let pool = ProofRpcPool::new(2).unwrap();
        assert_eq!(pool.available_permits(), 2);

        // Silence the panic print from the rayon worker so test output
        // stays readable; the catch_unwind below still observes it.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || pool.try_run::<_, ()>(|| panic!("boom")).unwrap(),
        ));
        std::panic::set_hook(prev_hook);

        assert!(result.is_err(), "panic must propagate to the caller");
        assert_eq!(
            pool.available_permits(),
            2,
            "slot must be returned after the closure panics"
        );

        // Pool is still fully usable after the panic.
        for _ in 0..4 {
            let x = pool.try_run(|| 1u32).unwrap();
            assert_eq!(x, 1);
        }
        assert_eq!(pool.available_permits(), 2);
    }
}
