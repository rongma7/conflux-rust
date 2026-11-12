//! Throwaway benchmark for LVMT proof reads through ProofRpcPool.
//!
//! Seeds a small LvmtStorage, then measures get_with_proof + verify
//! latency at single-thread and 8-thread concurrency through the pool.

use amt::{AmtParams, CreateMode};
use ark_bls12_381::Bls12_381;
use cfx_storage::ProofRpcPool;
use cfx_storage2::{verify_lvmt_proof, LvmtStorage};
use ethereum_types::H256;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha12Rng;
use std::{
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

type PE = Bls12_381;

const LEVEL: usize = 16;
const COMMITS: usize = 10;
const KEYS_PER_COMMIT: usize = 10_000;
const QUERY_ITERS: usize = 100;
const KEY_LEN: usize = 32;
const VALUE_LEN: usize = 64;

fn gen_entries(
    rng: &mut ChaCha12Rng, n: usize,
) -> Vec<(Box<[u8]>, Option<Box<[u8]>>)> {
    (0..n)
        .map(|_| {
            let mut k = vec![0u8; KEY_LEN];
            rng.fill_bytes(&mut k);
            let mut v = vec![0u8; VALUE_LEN];
            rng.fill_bytes(&mut v);
            (k.into_boxed_slice(), Some(v.into_boxed_slice()))
        })
        .collect()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty());
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn summarize(label: &str, mut xs: Vec<Duration>) {
    xs.sort();
    let sum: Duration = xs.iter().sum();
    let mean = sum / xs.len() as u32;
    println!(
        "{:32}  n={:5}  min={:>8.2}ms  p50={:>8.2}ms  p99={:>8.2}ms  max={:>8.2}ms  mean={:>8.2}ms",
        label,
        xs.len(),
        xs[0].as_secs_f64() * 1e3,
        percentile(&xs, 0.50).as_secs_f64() * 1e3,
        percentile(&xs, 0.99).as_secs_f64() * 1e3,
        xs.last().unwrap().as_secs_f64() * 1e3,
        mean.as_secs_f64() * 1e3,
    );
}

fn main() {
    println!(
        "LVMT proof bench: commits={} keys/commit={} query_iters={}",
        COMMITS, KEYS_PER_COMMIT, QUERY_ITERS
    );

    // AMT params from the repo's pp dir.
    let pp_dir = Path::new("/conflux-rust/pp");
    print!("loading AMT params... ");
    let t0 = Instant::now();
    let pp: AmtParams<PE> = AmtParams::from_dir_mont(
        pp_dir,
        LEVEL,
        LEVEL,
        CreateMode::Neither,
        None,
    );
    println!("{:.1}s", t0.elapsed().as_secs_f64());

    // Temp dir for RocksDB.
    let tmp = tempfile::tempdir().unwrap();
    let hist_path = tmp.path().join("historical");
    let pend_path = tmp.path().join("pending");

    print!(
        "seeding {} commits x {} keys... ",
        COMMITS, KEYS_PER_COMMIT
    );
    let t0 = Instant::now();
    let mut storage =
        LvmtStorage::new_from_paths(&hist_path, &pend_path).unwrap();

    let mut rng = ChaCha12Rng::seed_from_u64(0xC0FFEE);
    let mut first_keys: Vec<Box<[u8]>> = vec![];
    let mut prev_commit = None;
    let mut last_commit_id = None;
    for i in 0..COMMITS {
        let mut commit_bytes = [0u8; 32];
        commit_bytes[0..8].copy_from_slice(&((i + 1) as u64).to_be_bytes());
        let commit_id = H256::from(commit_bytes);
        let entries = gen_entries(&mut rng, KEYS_PER_COMMIT);
        if i == 0 {
            first_keys = entries.iter().map(|(k, _)| k.clone()).collect();
        }
        {
            let mut store = storage.as_manager().unwrap();
            store
                .commit(
                    prev_commit,
                    commit_id,
                    entries.into_iter(),
                    &pp,
                )
                .unwrap();
        }
        prev_commit = Some(commit_id);
        last_commit_id = Some(commit_id);
    }
    println!("{:.1}s", t0.elapsed().as_secs_f64());

    let target_commit = last_commit_id.unwrap();

    // Sample 200 keys from the first commit (deeper in history): keeps
    // queries over keys that actually exist and haven't been overwritten.
    let sample: Vec<Box<[u8]>> = first_keys.iter().step_by(KEYS_PER_COMMIT / 200).cloned().take(200).collect();
    assert!(sample.len() >= 100, "need at least 100 sample keys");

    // State root for verify.
    let state_root = storage
        .as_reader()
        .unwrap()
        .get_state_root(target_commit)
        .unwrap()
        .expect("state root for target commit");

    // Warmup: one proof + verify so caches are populated.
    {
        let reader = storage.as_reader().unwrap();
        let (_, proof) =
            reader.get_with_proof(target_commit, &sample[0], &pp).unwrap();
        let _ = verify_lvmt_proof(&pp, &state_root, &sample[0], &proof);
    }

    // ------------------------------------------------------------------
    // Single-thread: get_with_proof, verify
    // ------------------------------------------------------------------
    let mut prove_times = Vec::with_capacity(QUERY_ITERS);
    let mut verify_times = Vec::with_capacity(QUERY_ITERS);
    for i in 0..QUERY_ITERS {
        let key = &sample[i % sample.len()];
        let reader = storage.as_reader().unwrap();
        let t = Instant::now();
        let (_, proof) =
            reader.get_with_proof(target_commit, key, &pp).unwrap();
        prove_times.push(t.elapsed());
        let t = Instant::now();
        let _ = verify_lvmt_proof(&pp, &state_root, key, &proof).unwrap();
        verify_times.push(t.elapsed());
    }
    println!();
    println!("=== single-thread (hot cache) ===");
    summarize("get_with_proof", prove_times);
    summarize("verify_lvmt_proof", verify_times);

    // ------------------------------------------------------------------
    // Sequential submit, N single-threaded pools — one request at a
    // time, each on its own 1-thread pool so par_iter degrades to
    // serial.
    // ------------------------------------------------------------------
    let pool_seq = Arc::new(ProofRpcPool::new(8).unwrap());
    let mut seq_times = Vec::with_capacity(QUERY_ITERS);
    for i in 0..QUERY_ITERS {
        let key = &sample[i % sample.len()];
        let t = Instant::now();
        pool_seq
            .try_run(|| {
                let reader = storage.as_reader().unwrap();
                let (_, proof) = reader
                    .get_with_proof(target_commit, key, &pp)
                    .unwrap();
                verify_lvmt_proof(&pp, &state_root, key, &proof).unwrap()
            })
            .unwrap();
        seq_times.push(t.elapsed());
    }
    println!();
    println!("=== sequential submit via 8 single-threaded pools ===");
    summarize("get_with_proof+verify", seq_times);

    // ------------------------------------------------------------------
    // 8 concurrent peers, varying pool count — RPC load shape.
    // ------------------------------------------------------------------
    for &num_pools in &[8usize, 4usize, 2usize, 1usize] {
        let pool = Arc::new(ProofRpcPool::new(num_pools).unwrap());

        println!();
        println!(
            "=== 8 concurrent peers, {} single-threaded pools ===",
            num_pools
        );

        let (all, total_busy): (Vec<Duration>, u32) = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(8);
            for tid in 0..8usize {
                let pool = Arc::clone(&pool);
                let storage_ref = &storage;
                let sample_ref = &sample;
                let pp_ref = &pp;
                handles.push(scope.spawn(move || {
                    let mut times = Vec::with_capacity(QUERY_ITERS);
                    let mut busies = 0u32;
                    for i in 0..QUERY_ITERS {
                        let key =
                            &sample_ref[(tid * 17 + i) % sample_ref.len()];
                        let t = Instant::now();
                        let res = pool.try_run(|| {
                            let reader = storage_ref.as_reader().unwrap();
                            let (_, proof) = reader
                                .get_with_proof(target_commit, key, pp_ref)
                                .unwrap();
                            verify_lvmt_proof(
                                pp_ref,
                                &state_root,
                                key,
                                &proof,
                            )
                            .unwrap()
                        });
                        match res {
                            Ok(_) => times.push(t.elapsed()),
                            Err(_) => busies += 1,
                        }
                    }
                    (times, busies)
                }));
            }
            let mut all = vec![];
            let mut total_busy = 0u32;
            for h in handles {
                let (times, busies) = h.join().unwrap();
                all.extend(times);
                total_busy += busies;
            }
            (all, total_busy)
        });
        println!("  busy rejections: {}", total_busy);
        summarize("get_with_proof+verify", all);
    }

    println!();
    println!("state root: {:?}", state_root);
    drop(tmp);
}
