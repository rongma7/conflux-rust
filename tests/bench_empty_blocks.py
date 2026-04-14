#!/usr/bin/env python3
"""Benchmark: measure time for each batch of generate_empty_blocks(1000).
Uses pprof-rs via test RPC to capture CPU profiles at early and late stages."""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_framework.test_framework import ConfluxTestFramework


class BenchEmptyBlocks(ConfluxTestFramework):
    def set_test_params(self):
        self.num_nodes = 1
        self.conf_parameters = {
            "executive_trace": "true",
            "public_rpc_apis": "\"cfx,debug,test,pubsub,trace\"",
            "cip1559_transition_height": str(99999999),
            "cip151_transition_height": str(99999999),
            "cip645_transition_height": str(99999999),
        }

    def setup_network(self):
        self.setup_nodes()

    def run_test(self):
        time.sleep(3)
        node = self.nodes[0]

        batch_size = 1000
        num_batches = 15
        times = []

        # Profile capture: start profiling before batch, stop after batch
        profile_early_batch = 2   # profile during batch 3
        profile_late_batch = 13   # profile during batch 14

        print(f"\n{'='*60}")
        print(f"Benchmarking generate_empty_blocks({batch_size}) x {num_batches}")
        print(f"{'='*60}")
        print(f"{'Batch':>6} | {'Blocks so far':>14} | {'Time (s)':>10} | {'Blocks/s':>10}")
        print(f"{'-'*6}-+-{'-'*14}-+-{'-'*10}-+-{'-'*10}")

        total_start = time.time()
        for i in range(num_batches):
            # Start profiling before the target batch
            if i == profile_early_batch:
                print(f"\n--- Starting EARLY CPU profile (batch {i+1}) ---")
                try:
                    result = node.test_startCpuProfile(99)
                    print(f"  {result}")
                except Exception as e:
                    print(f"  Failed to start profiler: {e}")

            elif i == profile_late_batch:
                print(f"\n--- Starting LATE CPU profile (batch {i+1}) ---")
                try:
                    result = node.test_startCpuProfile(99)
                    print(f"  {result}")
                except Exception as e:
                    print(f"  Failed to start profiler: {e}")

            start = time.time()
            node.test_generateEmptyBlocks(batch_size)
            elapsed = time.time() - start
            times.append(elapsed)
            total_blocks = (i + 1) * batch_size
            rate = batch_size / elapsed if elapsed > 0 else float('inf')
            print(f"{i+1:>6} | {total_blocks:>14} | {elapsed:>10.3f} | {rate:>10.1f}")

            # Stop profiling after the target batch
            if i == profile_early_batch:
                print(f"--- Stopping EARLY CPU profile ---")
                try:
                    result = node.test_stopCpuProfile("/tmp/cpu_early")
                    print(f"  {result}")
                except Exception as e:
                    print(f"  Failed to stop profiler: {e}")

            elif i == profile_late_batch:
                print(f"--- Stopping LATE CPU profile ---")
                try:
                    result = node.test_stopCpuProfile("/tmp/cpu_late")
                    print(f"  {result}")
                except Exception as e:
                    print(f"  Failed to stop profiler: {e}")

        total_elapsed = time.time() - total_start

        print(f"\n{'='*60}")
        print(f"Total time: {total_elapsed:.3f}s")
        print(f"Average per batch: {sum(times)/len(times):.3f}s")

        first5_avg = sum(times[:5]) / 5
        last5_avg = sum(times[-5:]) / 5
        print(f"\nFirst 5 batches avg: {first5_avg:.3f}s")
        print(f"Last 5 batches avg:  {last5_avg:.3f}s")
        print(f"Slowdown factor:      {last5_avg/first5_avg:.2f}x")

        print(f"\nProfiles saved:")
        print(f"  Early: /tmp/cpu_early.pb  /tmp/cpu_early.svg")
        print(f"  Late:  /tmp/cpu_late.pb   /tmp/cpu_late.svg")
        print(f"\nView flamegraphs in browser:")
        print(f"  open /tmp/cpu_early.svg")
        print(f"  open /tmp/cpu_late.svg")


if __name__ == "__main__":
    BenchEmptyBlocks().main()
