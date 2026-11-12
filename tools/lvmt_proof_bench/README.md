# lvmt_proof_bench

End-to-end benchmark for LVMT proof reads through `ProofRpcPool`.
Standalone crate, same pattern as `tools/consensus_bench`.

## Run

```
cd tools/lvmt_proof_bench
CXXFLAGS="-include cstdint" cargo run --release
```

Expects the workspace's AMT power-of-tau parameters at `/conflux-rust/pp/`.

## What it measures

Seeds a small `LvmtStorage` (10 commits × 10K keys = 100K keys), then
times `get_with_proof` + `verify_lvmt_proof` under four shapes:

1. **Single-thread, no pool.** `amt::par_iter` runs on the global rayon
   pool. Baseline.
2. **Sequential submit, 8-thread pool.** One query at a time through
   `ProofRpcPool::try_run`. Matches upstream's "N threads" bench shape.
3. **8 concurrent peers, 8-thread pool.** Each peer submits queries
   independently. Matches real RPC load.
4. **Busy rejection.** `max_concurrent = 2`, 8 peers hammer the pool;
   expect ~75% of requests to receive the busy error.

## Caveat

Dataset is 400x smaller than upstream's 40M-key bench. At 100K keys most
proofs sit at depth 0, so absolute latency is lower than real deployment.
Numbers are useful for *comparing shapes* (no pool vs. pool, sequential
vs. concurrent peers, busy rejection rate), not as a production headline.

To reflect real conditions better, bump `COMMITS` / `KEYS_PER_COMMIT` in
`src/main.rs`; proof depth climbs once the root AMT's ~400K slot capacity
is exceeded.
