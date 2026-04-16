#!/usr/bin/env python3
"""Benchmark: measure block generation throughput with real transactions.

Each batch: send TXS_PER_BLOCK transactions to the mempool, then generate one
block that packs them. This exercises the full execution pipeline including
state reads/writes, unlike bench_empty_blocks which only measures overhead.
"""

import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from conflux.utils import priv_to_addr
from test_framework.test_framework import ConfluxTestFramework
import eth_utils


class BenchTxBlocks(ConfluxTestFramework):
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

        from conflux.rpc import RpcClient
        client = RpcClient(node)

        priv_key = client.GENESIS_PRI_KEY
        sender = client.GENESIS_ADDR
        receiver = client.COINBASE_ADDR

        txs_per_block = 20      # transactions packed per block
        num_batches = 200        # total batches
        blocks_per_batch = 50    # blocks per batch (each with txs_per_block txs)
        # Total: 200 * 50 = 10,000 blocks, 200,000 transactions

        print(f"\n{'='*70}")
        print(f"Benchmarking: {blocks_per_batch} blocks/batch × {txs_per_block} txs/block × {num_batches} batches")
        print(f"Total: {num_batches * blocks_per_batch} blocks, {num_batches * blocks_per_batch * txs_per_block} transactions")
        print(f"{'='*70}")
        print(f"{'Batch':>6} | {'Blocks':>10} | {'Time (s)':>10} | {'Blk/s':>8} | {'TX/s':>8}")
        print(f"{'-'*6}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*8}")

        nonce = client.get_nonce(sender)
        epoch_height = client.epoch_number()
        times = []

        total_start = time.time()
        for batch in range(num_batches):
            # Refresh epoch_height periodically to stay within valid range
            if batch % 10 == 0:
                epoch_height = client.epoch_number()

            start = time.time()

            for _ in range(blocks_per_batch):
                # Send transactions to mempool
                for _ in range(txs_per_block):
                    tx = client.new_tx(
                        sender=sender,
                        receiver=receiver,
                        nonce=nonce,
                        gas_price=1,
                        gas=21000,
                        value=1,
                        priv_key=priv_key,
                        storage_limit=0,
                        epoch_height=epoch_height,
                    )
                    client.send_tx(tx, wait_for_receipt=False, wait_for_catchup=False)
                    nonce += 1

                # Generate one block packing all txs from pool
                client.generate_block(txs_per_block)

            elapsed = time.time() - start
            times.append(elapsed)
            total_blocks = (batch + 1) * blocks_per_batch
            blk_rate = blocks_per_batch / elapsed if elapsed > 0 else float('inf')
            tx_rate = (blocks_per_batch * txs_per_block) / elapsed if elapsed > 0 else float('inf')
            print(f"{batch+1:>6} | {total_blocks:>10} | {elapsed:>10.3f} | {blk_rate:>8.1f} | {tx_rate:>8.0f}")

        total_elapsed = time.time() - total_start

        print(f"\n{'='*70}")
        print(f"Total time: {total_elapsed:.3f}s")
        print(f"Total blocks: {num_batches * blocks_per_batch}")
        print(f"Total transactions: {num_batches * blocks_per_batch * txs_per_block}")
        print(f"Average per batch: {sum(times)/len(times):.3f}s")

        first5_avg = sum(times[:5]) / 5
        last5_avg = sum(times[-5:]) / 5
        print(f"\nFirst 5 batches avg: {first5_avg:.3f}s")
        print(f"Last 5 batches avg:  {last5_avg:.3f}s")
        print(f"Slowdown factor:      {last5_avg/first5_avg:.2f}x")


if __name__ == "__main__":
    BenchTxBlocks().main()
