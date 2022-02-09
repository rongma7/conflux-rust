// Copyright 2019-2021 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

use std::{cmp::min, sync::Arc};

use jsonrpc_core::{Error as RpcError, Result as RpcResult};
use rlp::Rlp;

use cfx_statedb::StateDbExt;
use cfx_types::{
    Address, AddressSpaceUtil, BigEndianHash, Space, H160, H256, U256, U64,
};
use cfxcore::{
    executive::{
        revert_reason_decode, ExecutionError, ExecutionOutcome, TxDropError,
    },
    observer::ErrorUnwind,
    rpc_errors::{
        invalid_params_check, Error as CfxRpcError, Result as CfxRpcResult,
    },
    vm, ConsensusGraph, SharedConsensusGraph, SharedSynchronizationService,
    SharedTransactionPool,
};
use primitives::{
    filter::LogFilter, receipt::EVM_SPACE_SUCCESS, Action, Block,
    BlockHashOrEpochNumber, Eip155Transaction, EpochNumber, PhantomBlock,
    SignedTransaction, StorageKey, StorageValue, TransactionOutcome,
    TransactionWithSignature,
};
use std::convert::TryInto;

use crate::rpc::{
    error_codes::{
        call_execution_error, internal_error, invalid_params,
        request_rejected_in_catch_up_mode, unimplemented, unknown_block,
    },
    impls::RpcImplConfiguration,
    traits::eth::{Eth, EthFilter},
    types::{
        eth::{
            Block as RpcBlock, BlockNumber, CallRequest, EthRpcLogFilter,
            FilterChanges, Log, Receipt, SyncInfo, SyncStatus, Transaction,
        },
        Bytes, Index, MAX_GAS_CALL_REQUEST,
    },
};

pub struct EthHandler {
    config: RpcImplConfiguration,
    consensus: SharedConsensusGraph,
    sync: SharedSynchronizationService,
    tx_pool: SharedTransactionPool,
}

impl EthHandler {
    pub fn new(
        config: RpcImplConfiguration, consensus: SharedConsensusGraph,
        sync: SharedSynchronizationService, tx_pool: SharedTransactionPool,
    ) -> Self
    {
        EthHandler {
            config,
            consensus,
            sync,
            tx_pool,
        }
    }

    fn consensus_graph(&self) -> &ConsensusGraph {
        self.consensus
            .as_any()
            .downcast_ref::<ConsensusGraph>()
            .expect("downcast should succeed")
    }
}

pub fn sign_call(
    chain_id: u32, request: CallRequest,
) -> RpcResult<SignedTransaction> {
    let max_gas = U256::from(MAX_GAS_CALL_REQUEST);
    let gas = min(request.gas.unwrap_or(max_gas), max_gas);
    let from = request.from.unwrap_or_else(|| Address::random());

    Ok(Eip155Transaction {
        nonce: request.nonce.unwrap_or_default(),
        action: request.to.map_or(Action::Create, |addr| Action::Call(addr)),
        gas,
        gas_price: request.gas_price.unwrap_or(1.into()),
        value: request.value.unwrap_or_default(),
        chain_id: Some(chain_id),
        data: request.data.unwrap_or_default().into_vec(),
    }
    .fake_sign(from.with_evm_space()))
}

fn block_tx_by_index(
    phantom_block: Option<PhantomBlock>, idx: usize,
) -> Option<Transaction> {
    match phantom_block {
        None => None,
        Some(pb) => match pb.transactions.get(idx) {
            None => None,
            Some(tx) => {
                let block_number = Some(pb.pivot_header.height().into());
                let receipt = pb.receipts.get(idx).unwrap();
                let status = receipt.outcome_status.in_space(Space::Ethereum);
                let contract_address = match status == EVM_SPACE_SUCCESS {
                    true => Transaction::deployed_contract_address(&tx),
                    false => None,
                };
                Some(Transaction::from_signed(
                    &tx,
                    (
                        Some(pb.pivot_header.hash()),
                        block_number,
                        Some(idx.into()),
                    ),
                    (Some(status.into()), contract_address),
                ))
            }
        },
    }
}

impl EthHandler {
    fn get_blocks_by_number(
        &self, block_num: BlockNumber,
    ) -> jsonrpc_core::Result<Option<Vec<Arc<Block>>>> {
        let epoch_hashes = self
            .consensus
            .get_block_hashes_by_epoch(block_num.try_into()?)
            .map_err(RpcError::invalid_params)?;

        let epoch_blocks = self
            .consensus
            .get_data_manager()
            .blocks_by_hash_list(&epoch_hashes, false /* update_cache */);

        Ok(epoch_blocks)
    }

    // Get pivot block hash by epoch number
    #[allow(dead_code)]
    fn get_block_hash_by_number(
        &self, block_number: BlockNumber,
    ) -> Option<H256> {
        match self.get_blocks_by_number(block_number) {
            Ok(Some(blocks)) => match blocks.last() {
                None => None,
                Some(b) => Some(b.hash()),
            },
            _ => None,
        }
    }

    fn exec_transaction(
        &self, request: CallRequest, epoch: Option<BlockNumber>,
    ) -> CfxRpcResult<ExecutionOutcome> {
        let consensus_graph = self.consensus_graph();
        let epoch = epoch.unwrap_or_default().try_into()?;

        let chain_id = self.consensus.best_chain_id();
        let signed_tx = sign_call(chain_id.in_evm_space(), request)?;
        trace!("call tx {:?}", signed_tx);
        consensus_graph.call_virtual(&signed_tx, epoch)
    }

    fn send_transaction_with_signature(
        &self, tx: TransactionWithSignature,
    ) -> CfxRpcResult<H256> {
        if self.sync.catch_up_mode() {
            warn!("Ignore send_transaction request {}. Cannot send transaction when the node is still in catch-up mode.", tx.hash());
            bail!(request_rejected_in_catch_up_mode(None));
        }
        let (signed_trans, failed_trans) =
            self.tx_pool.insert_new_transactions(vec![tx]);
        // FIXME: how is it possible?
        if signed_trans.len() + failed_trans.len() > 1 {
            // This should never happen
            error!("insert_new_transactions failed, invalid length of returned result vector {}", signed_trans.len() + failed_trans.len());
            Ok(H256::zero().into())
        } else if signed_trans.len() + failed_trans.len() == 0 {
            // For tx in transactions_pubkey_cache, we simply ignore them
            debug!("insert_new_transactions ignores inserted transactions");
            // FIXME: this is not invalid params
            bail!(invalid_params("tx", String::from("tx already exist")))
        } else if signed_trans.is_empty() {
            let tx_err = failed_trans.iter().next().expect("Not empty").1;
            // FIXME: this is not invalid params
            bail!(invalid_params("tx", tx_err))
        } else {
            let tx_hash = signed_trans[0].hash();
            self.sync.append_received_transactions(signed_trans);
            Ok(tx_hash.into())
        }
    }

    fn construct_rpc_receipt(
        &self, b: &PhantomBlock, idx: usize, prior_log_index: &mut usize,
    ) -> jsonrpc_core::Result<Receipt> {
        if b.transactions.len() != b.receipts.len() {
            return Err(internal_error("Inconsistent state"));
        }

        if b.transactions.len() != b.errors.len() {
            return Err(internal_error("Inconsistent state"));
        }

        if idx >= b.transactions.len() {
            return Err(internal_error("Inconsistent state"));
        }

        let tx = &b.transactions[idx];
        let receipt = &b.receipts[idx];

        if receipt.logs.iter().any(|l| l.space != Space::Ethereum) {
            return Err(internal_error("Inconsistent state"));
        }

        let contract_address = match receipt.outcome_status {
            TransactionOutcome::Success => {
                Transaction::deployed_contract_address(tx)
            }
            _ => None,
        };

        let transaction_hash = tx.hash();
        let transaction_index: U256 = idx.into();
        let block_hash = b.pivot_header.hash();
        let block_number: U256 = b.pivot_header.height().into();

        let logs: Vec<_> = receipt
            .logs
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, log)| Log {
                address: log.address,
                topics: log.topics,
                data: Bytes(log.data),
                block_hash,
                block_number,
                transaction_hash,
                transaction_index,
                log_index: Some((*prior_log_index + idx).into()),
                transaction_log_index: Some(idx.into()),
                removed: false,
            })
            .collect();

        *prior_log_index += logs.len();

        let gas_used = match idx {
            0 => receipt.accumulated_gas_used,
            idx => {
                receipt.accumulated_gas_used
                    - b.receipts[idx - 1].accumulated_gas_used
            }
        };

        let tx_exec_error_msg = if b.errors[idx].is_empty() {
            None
        } else {
            Some(b.errors[idx].clone())
        };

        Ok(Receipt {
            transaction_hash,
            transaction_index,
            block_hash,
            from: tx.sender().address,
            to: match tx.action() {
                Action::Create => None,
                Action::Call(addr) => Some(*addr),
            },
            block_number,
            cumulative_gas_used: receipt.accumulated_gas_used,
            gas_used,
            contract_address,
            logs,
            logs_bloom: receipt.log_bloom,
            status_code: receipt
                .outcome_status
                .in_space(Space::Ethereum)
                .into(),
            effective_gas_price: *tx.gas_price(),
            tx_exec_error_msg,
        })
    }

    fn get_tx_from_txpool(&self, hash: H256) -> Option<Transaction> {
        let tx = self.tx_pool.get_transaction(&hash)?;

        if tx.space() == Space::Ethereum {
            Some(Transaction::from_signed(
                &tx,
                (None, None, None),
                (None, None),
            ))
        } else {
            None
        }
    }
}

impl Eth for EthHandler {
    fn client_version(&self) -> jsonrpc_core::Result<String> {
        info!("RPC Request: web3_clientVersion");
        // TODO
        Ok(format!("Conflux"))
    }

    fn net_version(&self) -> jsonrpc_core::Result<String> {
        info!("RPC Request: net_version");
        Ok(format!("{}", self.consensus.best_chain_id().in_evm_space()))
    }

    fn protocol_version(&self) -> jsonrpc_core::Result<String> {
        info!("RPC Request: eth_protocolVersion");
        // 65 is a common ETH version now
        Ok(format!("{}", 65))
    }

    fn syncing(&self) -> jsonrpc_core::Result<SyncStatus> {
        info!("RPC Request: eth_syncing");
        if self.sync.catch_up_mode() {
            Ok(
                // Now pass some statistics of Conflux just to make the
                // interface happy
                SyncStatus::Info(SyncInfo {
                    starting_block: U256::from(self.consensus.block_count()),
                    current_block: U256::from(self.consensus.block_count()),
                    highest_block: U256::from(
                        self.sync.get_synchronization_graph().block_count(),
                    ),
                    warp_chunks_amount: None,
                    warp_chunks_processed: None,
                }),
            )
        } else {
            Ok(SyncStatus::None)
        }
    }

    fn hashrate(&self) -> jsonrpc_core::Result<U256> {
        info!("RPC Request: eth_hashrate");
        // We do not mine
        Ok(U256::zero())
    }

    fn author(&self) -> jsonrpc_core::Result<H160> {
        info!("RPC Request: eth_coinbase");
        // We do not care this, just return zero address
        Ok(H160::zero())
    }

    fn is_mining(&self) -> jsonrpc_core::Result<bool> {
        info!("RPC Request: eth_mining");
        // We do not mine from ETH perspective
        Ok(false)
    }

    fn chain_id(&self) -> jsonrpc_core::Result<Option<U64>> {
        info!("RPC Request: eth_chainId");
        return Ok(Some(self.consensus.best_chain_id().in_evm_space().into()));
    }

    fn gas_price(&self) -> jsonrpc_core::Result<U256> {
        info!("RPC Request: eth_gasPrice");
        // TODO: Change this
        Ok(U256::from(5000000000u64))
    }

    fn max_priority_fee_per_gas(&self) -> jsonrpc_core::Result<U256> {
        info!("RPC Request: eth_maxPriorityFeePerGas");
        // TODO: Change this
        Ok(U256::from(20000000000u64))
    }

    fn accounts(&self) -> jsonrpc_core::Result<Vec<H160>> {
        info!("RPC Request: eth_accounts");
        // TODO: EVM core: discussion: do we really need this? Maybe not,
        // because EVM has enough dev tools and don't need dev mode.
        // We do not expect people to use the ETH rpc to manage accounts
        Ok(vec![])
    }

    fn block_number(&self) -> jsonrpc_core::Result<U256> {
        let consensus_graph = self.consensus_graph();
        let epoch_num = EpochNumber::LatestState;
        info!("RPC Request: eth_blockNumber()");
        match consensus_graph.get_height_from_epoch_number(epoch_num.into()) {
            Ok(height) => Ok(height.into()),
            Err(e) => Err(jsonrpc_core::Error::invalid_params(e)),
        }
    }

    fn balance(
        &self, address: H160, num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<U256> {
        let epoch_num = num.unwrap_or_default().try_into()?;

        info!(
            "RPC Request: eth_getBalance address={:?} epoch_num={:?}",
            address, epoch_num
        );

        let state_db = self
            .consensus
            .get_state_db_by_epoch_number(epoch_num, "num")?;
        let acc = state_db
            .get_account(&address.with_evm_space())
            .map_err(|err| CfxRpcError::from(err))?;

        Ok(acc.map_or(U256::zero(), |acc| acc.balance).into())
    }

    fn storage_at(
        &self, address: H160, position: U256, block_num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<H256> {
        let epoch_num = block_num.unwrap_or_default().try_into()?;

        info!(
            "RPC Request: eth_getStorageAt address={:?}, position={:?}, block_num={:?})",
            address, position, epoch_num
        );

        let state_db = self
            .consensus
            .get_state_db_by_epoch_number(epoch_num, "epoch_number")?;

        let position: H256 = H256::from_uint(&position);

        let key = StorageKey::new_storage_key(&address, position.as_ref())
            .with_evm_space();

        Ok(
            match state_db
                .get::<StorageValue>(key)
                .map_err(|err| CfxRpcError::from(err))?
            {
                Some(entry) => H256::from_uint(&entry.value).into(),
                None => H256::zero(),
            },
        )
    }

    fn block_by_hash(
        &self, hash: H256, include_txs: bool,
    ) -> jsonrpc_core::Result<Option<RpcBlock>> {
        info!(
            "RPC Request: eth_getBlockByHash hash={:?} include_txs={:?}",
            hash, include_txs
        );

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_hash(&hash)
                .map_err(RpcError::invalid_params)?
        };

        match phantom_block {
            None => Ok(None),
            Some(pb) => Ok(Some(RpcBlock::from_phantom(&pb, include_txs))),
        }
    }

    fn block_by_number(
        &self, block_num: BlockNumber, include_txs: bool,
    ) -> jsonrpc_core::Result<Option<RpcBlock>> {
        info!("RPC Request: eth_getBlockByNumber block_number={:?} include_txs={:?}", block_num, include_txs);

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_number(block_num.try_into()?, None)
                .map_err(RpcError::invalid_params)?
        };

        match phantom_block {
            None => Ok(None),
            Some(pb) => Ok(Some(RpcBlock::from_phantom(&pb, include_txs))),
        }
    }

    fn transaction_count(
        &self, address: H160, num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<U256> {
        info!(
            "RPC Request: eth_getTransactionCount address={:?} block_number={:?}",
            address, num
        );

        let nonce = match num {
            Some(BlockNumber::Pending) => {
                self.tx_pool.get_next_nonce(&address.with_evm_space())
            }
            _ => {
                let num = num.unwrap_or_default().try_into()?;

                self.consensus_graph().next_nonce(
                    address.with_evm_space(),
                    BlockHashOrEpochNumber::EpochNumber(num),
                    "num",
                )?
            }
        };

        Ok(nonce)
    }

    fn block_transaction_count_by_hash(
        &self, hash: H256,
    ) -> jsonrpc_core::Result<Option<U256>> {
        info!(
            "RPC Request: eth_getBlockTransactionCountByHash hash={:?}",
            hash,
        );

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_hash(&hash)
                .map_err(RpcError::invalid_params)?
        };

        match phantom_block {
            None => Ok(None),
            Some(pb) => Ok(Some(pb.transactions.len().into())),
        }
    }

    fn block_transaction_count_by_number(
        &self, block_num: BlockNumber,
    ) -> jsonrpc_core::Result<Option<U256>> {
        info!(
            "RPC Request: eth_getBlockTransactionCountByNumber block_number={:?}",
            block_num
        );

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_number(block_num.try_into()?, None)
                .map_err(RpcError::invalid_params)?
        };

        match phantom_block {
            None => Ok(None),
            Some(pb) => Ok(Some(pb.transactions.len().into())),
        }
    }

    fn block_uncles_count_by_hash(
        &self, hash: H256,
    ) -> jsonrpc_core::Result<Option<U256>> {
        info!("RPC Request: eth_getUncleCountByBlockHash hash={:?}", hash);

        // TODO(thegaram): only return Some(_) for pivot block
        let maybe_block = self
            .consensus
            .get_data_manager()
            .block_by_hash(&hash, false);

        Ok(maybe_block.map(|_| 0.into()))
    }

    fn block_uncles_count_by_number(
        &self, block_num: BlockNumber,
    ) -> jsonrpc_core::Result<Option<U256>> {
        info!(
            "RPC Request: eth_getUncleCountByBlockNumber block_number={:?}",
            block_num
        );

        // TODO(thegaram): enough to just check if pivot exists
        let maybe_block = self.get_blocks_by_number(block_num)?;
        Ok(maybe_block.map(|_| 0.into()))
    }

    fn code_at(
        &self, address: H160, epoch_num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<Bytes> {
        let epoch_num = epoch_num.unwrap_or_default().try_into()?;

        info!(
            "RPC Request: eth_getCode address={:?} epoch_num={:?}",
            address, epoch_num
        );

        let state_db = self
            .consensus
            .get_state_db_by_epoch_number(epoch_num, "num")?;

        let address = address.with_evm_space();

        let code = match state_db
            .get_account(&address)
            .map_err(|err| CfxRpcError::from(err))?
        {
            Some(acc) => match state_db
                .get_code(&address, &acc.code_hash)
                .map_err(|err| CfxRpcError::from(err))?
            {
                Some(code) => (*code.code).clone(),
                _ => vec![],
            },
            None => vec![],
        };

        Ok(Bytes::new(code))
    }

    fn send_raw_transaction(&self, raw: Bytes) -> jsonrpc_core::Result<H256> {
        info!(
            "RPC Request: eth_sendRawTransaction / eth_submitTransaction raw={:?}",
            raw,
        );
        let tx: TransactionWithSignature =
            invalid_params_check("raw", Rlp::new(&raw.into_vec()).as_val())?;

        if tx.space() != Space::Ethereum {
            bail!(invalid_params("tx", "Incorrect transaction space"));
        }

        if tx.recover_public().is_err() {
            bail!(invalid_params(
                "tx",
                "Can not recover pubkey for Ethereum like tx"
            ));
        }

        let r = self.send_transaction_with_signature(tx)?;
        Ok(r)
    }

    fn submit_transaction(&self, raw: Bytes) -> jsonrpc_core::Result<H256> {
        self.send_raw_transaction(raw)
    }

    fn call(
        &self, request: CallRequest, block_num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<Bytes> {
        info!(
            "RPC Request: eth_call request={:?}, block_num={:?}",
            request, block_num
        );
        // TODO: EVM core: Check the EVM error message. To make the
        // assert_error_eq test case in solidity project compatible.
        let epoch = block_num.map(Into::into);
        match self.exec_transaction(request, epoch)? {
            ExecutionOutcome::NotExecutedDrop(TxDropError::OldNonce(expected, got)) => {
                bail!(call_execution_error(
                    "Transaction can not be executed".into(),
                    format! {"nonce is too old expected {:?} got {:?}", expected, got}.into_bytes()
                ))
            }
            ExecutionOutcome::NotExecutedDrop(TxDropError::InvalidRecipientAddress(recipient)) => {
                bail!(call_execution_error(
                    "Transaction can not be executed".into(),
                    format! {"invalid recipient address {:?}", recipient}.into_bytes()
                ))
            }
            ExecutionOutcome::NotExecutedToReconsiderPacking(e) => {
                bail!(call_execution_error(
                    "Transaction can not be executed".into(),
                    format! {"{:?}", e}.into_bytes()
                ))
            }
            ExecutionOutcome::ExecutionErrorBumpNonce(
                ExecutionError::VmError(vm::Error::Reverted),
                executed,
            ) => bail!(call_execution_error(
                format!("execution reverted: {}", revert_reason_decode(&executed.output)),
                executed.output
            )),
            ExecutionOutcome::ExecutionErrorBumpNonce(e, _) => {
                bail!(call_execution_error(
                    "Transaction execution failed".into(),
                    format! {"{:?}", e}.into_bytes()
                ))
            }
            ExecutionOutcome::Finished(executed) => Ok(executed.output.into()),
        }
    }

    fn estimate_gas(
        &self, request: CallRequest, block_num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<U256> {
        info!(
            "RPC Request: eth_estimateGas request={:?}, block_num={:?}",
            request, block_num
        );
        // TODO: EVM core: same as call
        let executed = match self.exec_transaction(request, block_num)? {
            ExecutionOutcome::NotExecutedDrop(TxDropError::OldNonce(expected, got)) => {
                bail!(call_execution_error(
                    "Can not estimate: transaction can not be executed".into(),
                    format! {"nonce is too old expected {:?} got {:?}", expected, got}.into_bytes()
                ))
            }
            ExecutionOutcome::NotExecutedDrop(TxDropError::InvalidRecipientAddress(recipient)) => {
                bail!(call_execution_error(
                    "Can not estimate: transaction can not be executed".into(),
                    format! {"invalid recipient address {:?}", recipient}.into_bytes()
                ))
            }
            ExecutionOutcome::NotExecutedToReconsiderPacking(e) => {
                bail!(call_execution_error(
                    "Can not estimate: transaction can not be executed".into(),
                    format! {"{:?}", e}.into_bytes()
                ))
            }
            ExecutionOutcome::ExecutionErrorBumpNonce(
                ExecutionError::VmError(vm::Error::Reverted),
                executed,
            ) => {
                // When a revert exception happens, there is usually an error in the sub-calls.
                // So we return the trace information for debugging contract.
                let errors = ErrorUnwind::from_traces(executed.trace).errors.iter()
                    .map(|(addr, error)| {
                        format!("{}: {}", addr, error)
                    })
                    .collect::<Vec<String>>();

                // Decode revert error
                let revert_error = revert_reason_decode(&executed.output);
                let revert_error = if !revert_error.is_empty() {
                    format!(": {}.", revert_error)
                } else {
                    format!(".")
                };

                // Try to fetch the innermost error.
                let innermost_error = if errors.len() > 0 {
                    format!(" Innermost error is at {}.", errors[0])
                } else {
                    String::default()
                };

                bail!(call_execution_error(
                    format!("Estimation isn't accurate: transaction is reverted{}{}",
                        revert_error, innermost_error),
                    errors.join("\n").into_bytes(),
                ))
            }
            ExecutionOutcome::ExecutionErrorBumpNonce(e, _) => {
                bail!(call_execution_error(
                    format! {"Can not estimate: transaction execution failed, \
                    all gas will be charged (execution error: {:?})", e}
                    .into(),
                    format! {"{:?}", e}.into_bytes()
                ))
            }
            ExecutionOutcome::Finished(executed) => executed,
        };

        // In case of unlimited full gas charge at some VM call, or if there are
        // infinite loops, the total estimated gas used is very close to
        // MAX_GAS_CALL_REQUEST, 0.8 is chosen to check if it's close.
        const TOO_MUCH_GAS_USED: u64 =
            (0.8 * (MAX_GAS_CALL_REQUEST as f32)) as u64;
        // TODO: this value should always be Some(..) unless incorrect
        // implementation. Should return an error for server bugs later.
        let estimated_gas_limit =
            executed.estimated_gas_limit.unwrap_or(U256::zero());
        if estimated_gas_limit >= U256::from(TOO_MUCH_GAS_USED) {
            bail!(call_execution_error(
                format!(
                    "Gas too high. Most likely there are problems within the contract code. \
                    gas {}",
                    estimated_gas_limit
                ),
                format!(
                    "gas {}", estimated_gas_limit
                )
                .into_bytes(),
            ));
        }
        Ok(estimated_gas_limit)
    }

    fn transaction_by_hash(
        &self, hash: H256,
    ) -> jsonrpc_core::Result<Option<Transaction>> {
        info!("RPC Request: eth_getTransactionByHash({:?})", hash);

        let tx_index = match self
            .consensus
            .get_data_manager()
            .transaction_index_by_hash(&hash, false /* update_cache */)
        {
            None => return Ok(self.get_tx_from_txpool(hash)),
            Some(tx_index) => tx_index,
        };

        let epoch_num =
            match self.consensus.get_block_epoch_number(&tx_index.block_hash) {
                None => return Ok(self.get_tx_from_txpool(hash)),
                Some(n) => n,
            };

        let maybe_block = self
            .consensus_graph()
            .get_phantom_block_by_number(EpochNumber::Number(epoch_num), None)
            .map_err(RpcError::invalid_params)?;

        let phantom_block = match maybe_block {
            None => return Ok(self.get_tx_from_txpool(hash)),
            Some(b) => b,
        };

        for (idx, tx) in phantom_block.transactions.iter().enumerate() {
            if tx.hash() == hash {
                let tx = block_tx_by_index(Some(phantom_block), idx);
                return Ok(tx);
            }
        }

        Ok(self.get_tx_from_txpool(hash))
    }

    fn transaction_by_block_hash_and_index(
        &self, hash: H256, idx: Index,
    ) -> jsonrpc_core::Result<Option<Transaction>> {
        info!("RPC Request: eth_getTransactionByBlockHashAndIndex hash={:?}, idx={:?}", hash, idx);

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_hash(&hash)
                .map_err(RpcError::invalid_params)?
        };

        Ok(block_tx_by_index(phantom_block, idx.value()))
    }

    fn transaction_by_block_number_and_index(
        &self, block_num: BlockNumber, idx: Index,
    ) -> jsonrpc_core::Result<Option<Transaction>> {
        info!("RPC Request: eth_getTransactionByBlockNumberAndIndex block_num={:?}, idx={:?}", block_num, idx);

        let phantom_block = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            self.consensus_graph()
                .get_phantom_block_by_number(block_num.try_into()?, None)
                .map_err(RpcError::invalid_params)?
        };

        Ok(block_tx_by_index(phantom_block, idx.value()))
    }

    fn transaction_receipt(
        &self, tx_hash: H256,
    ) -> jsonrpc_core::Result<Option<Receipt>> {
        info!(
            "RPC Request: eth_getTransactionReceipt tx_hash={:?}",
            tx_hash
        );

        let tx_index =
            match self.consensus.get_data_manager().transaction_index_by_hash(
                &tx_hash, false, /* update_cache */
            ) {
                None => return Ok(None),
                Some(tx_index) => tx_index,
            };

        let epoch_num =
            match self.consensus.get_block_epoch_number(&tx_index.block_hash) {
                None => return Ok(None),
                Some(n) => n,
            };

        let maybe_block = self
            .consensus_graph()
            .get_phantom_block_by_number(EpochNumber::Number(epoch_num), None)
            .map_err(RpcError::invalid_params)?;

        let phantom_block = match maybe_block {
            None => return Ok(None),
            Some(b) => b,
        };

        let mut prior_log_index = 0;

        for (idx, tx) in phantom_block.transactions.iter().enumerate() {
            if tx.hash() == tx_hash {
                let receipt = self.construct_rpc_receipt(
                    &phantom_block,
                    idx,
                    &mut prior_log_index,
                )?;

                return Ok(Some(receipt));
            }

            prior_log_index += phantom_block.receipts[idx].logs.len();
        }

        Ok(None)
    }

    fn uncle_by_block_hash_and_index(
        &self, hash: H256, idx: Index,
    ) -> jsonrpc_core::Result<Option<RpcBlock>> {
        info!(
            "RPC Request: eth_getUncleByBlockHashAndIndex hash={:?}, idx={:?}",
            hash, idx
        );
        // We do not have uncle block
        Ok(None)
    }

    fn uncle_by_block_number_and_index(
        &self, block_num: BlockNumber, idx: Index,
    ) -> jsonrpc_core::Result<Option<RpcBlock>> {
        info!("RPC Request: eth_getUncleByBlockNumberAndIndex block_num={:?}, idx={:?}", block_num, idx);
        // We do not have uncle block
        Ok(None)
    }

    fn logs(&self, filter: EthRpcLogFilter) -> jsonrpc_core::Result<Vec<Log>> {
        info!("RPC Request: eth_getLogs({:?})", filter);

        if let Some(max_limit) = self.config.get_logs_filter_max_limit {
            if filter.limit.is_none() || filter.limit.unwrap() > max_limit {
                // fail so that different behavior from eth is easy to detect
                bail!(invalid_params(
                    "filter",
                    format!("This node only allows filters with `limit` set to {} or less", max_limit))
                );
            }
        }

        let filter: LogFilter =
            filter.into_primitive(self.consensus.clone())?;

        let logs = self
            .consensus_graph()
            .logs(filter)
            .map_err(|err| CfxRpcError::from(err))?;

        Ok(logs
            .iter()
            .cloned()
            .map(|l| Log::try_from_localized(l, self.consensus.clone()))
            .collect::<Result<_, _>>()?)
    }

    fn submit_hashrate(&self, _: U256, _: H256) -> jsonrpc_core::Result<bool> {
        info!("RPC Request: eth_submitHashrate");
        // We do not care mining
        Ok(false)
    }

    fn block_receipts(
        &self, block_num: Option<BlockNumber>,
    ) -> jsonrpc_core::Result<Vec<Receipt>> {
        info!(
            "RPC Request: parity_getBlockReceipts block_number={:?}",
            block_num
        );

        let block_num = block_num.unwrap_or_default();

        let b = {
            // keep read lock to ensure consistent view
            let _inner = self.consensus_graph().inner.read();

            let phantom_block = match block_num {
                BlockNumber::Hash { hash, .. } => self
                    .consensus_graph()
                    .get_phantom_block_by_hash(&hash)
                    .map_err(RpcError::invalid_params)?,
                _ => self
                    .consensus_graph()
                    .get_phantom_block_by_number(block_num.try_into()?, None)
                    .map_err(RpcError::invalid_params)?,
            };

            match phantom_block {
                None => return Err(unknown_block()),
                Some(b) => b,
            }
        };

        let mut block_receipts = vec![];
        let mut prior_log_index = 0;

        for idx in 0..b.receipts.len() {
            block_receipts.push(self.construct_rpc_receipt(
                &b,
                idx,
                &mut prior_log_index,
            )?);
        }

        Ok(block_receipts)
    }
}

impl EthFilter for EthHandler {
    fn new_filter(&self, _: EthRpcLogFilter) -> jsonrpc_core::Result<U256> {
        warn!("RPC Request (Not Supported!): eth_newFilter");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }

    fn new_block_filter(&self) -> jsonrpc_core::Result<U256> {
        warn!("RPC Request (Not Supported!): eth_newBlockFilter");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }

    fn new_pending_transaction_filter(&self) -> jsonrpc_core::Result<U256> {
        warn!("RPC Request (Not Supported!): eth_newPendingTransactionFilter");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }

    fn filter_changes(&self, _: Index) -> jsonrpc_core::Result<FilterChanges> {
        warn!("RPC Request (Not Supported!): eth_getFilterChanges");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }

    fn filter_logs(&self, _: Index) -> jsonrpc_core::Result<Vec<Log>> {
        warn!("RPC Request (Not Supported!): eth_getFilterLogs");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }

    fn uninstall_filter(&self, _: Index) -> jsonrpc_core::Result<bool> {
        warn!("RPC Request (Not Supported!): eth_uninstallFilter");
        bail!(unimplemented(Some(
            "ETH Filter RPC not implemented!".into()
        )));
    }
}