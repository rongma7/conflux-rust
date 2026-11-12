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
//! Instead the pool here holds `N` **single-threaded** rayon pools plus
//! a matching capacity gate. Each in-flight request runs on one
//! dedicated worker so the nested stealing path is gone. The trade-off:
//! each request loses the intra-request par_iter parallelism, so a
//! lone request is slower than it would be on a single multi-thread
//! pool. Under 8 concurrent peers, upstream's reference bench reports
//! p99 ~250 ms with 8 single-threaded pools against p99 2.5 s with one
//! 8-thread pool.
//!
//! The single config knob is set via `StorageConfiguration`:
//! - `lvmt_proof_pool_threads` — number of single-threaded pools, and
//!   therefore both the compute budget and the in-flight cap. Excess
//!   requests get a busy error.

use super::errors::{Error, Result};
use parking_lot::Mutex;
use rayon::ThreadPool;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ProofRpcPool {
    pools: Vec<Arc<ThreadPool>>,
    sem: Arc<Semaphore>,
    /// Indices of pools that are currently free. Protected by a mutex
    /// but only touched briefly on acquire / release. The semaphore
    /// guarantees this list is non-empty whenever a permit has been
    /// taken.
    available: Arc<Mutex<Vec<usize>>>,
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
        let sem = Arc::new(Semaphore::new(num_pools));
        let available = Arc::new(Mutex::new((0..num_pools).collect()));
        Ok(Self {
            pools,
            sem,
            available,
        })
    }

    pub fn num_pools(&self) -> usize { self.pools.len() }

    pub fn available_permits(&self) -> usize { self.sem.available_permits() }

    /// Acquires a permit without waiting, then runs `f` on the pool
    /// that permit reserves. Returns `Err(Busy)` if all pools are in
    /// use.
    pub fn try_run<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        let permit = self.sem.try_acquire().map_err(|_| {
            Error::Msg("proof RPC pool busy, retry later".into())
        })?;
        let idx = self
            .available
            .lock()
            .pop()
            .expect("semaphore invariant: available has >= 1 slot while a permit is held");
        let result = self.pools[idx].install(f);
        self.available.lock().push(idx);
        drop(permit);
        Ok(result)
    }

    /// Acquires a permit asynchronously (awaiting if all pools are in
    /// use), then runs `f` on the pool that permit reserves. Must be
    /// called from within a tokio runtime.
    pub async fn run_async<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        let permit = self.sem.acquire().await.map_err(|e| {
            Error::Msg(format!("proof RPC semaphore closed: {}", e))
        })?;
        let idx = self
            .available
            .lock()
            .pop()
            .expect("semaphore invariant: available has >= 1 slot while a permit is held");
        let result = self.pools[idx].install(f);
        self.available.lock().push(idx);
        drop(permit);
        Ok(result)
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
}
