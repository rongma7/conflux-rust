// Copyright 2026 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

//! The LVMT variant of the storage engine, implementing the same
//! `StorageEngine` trait the layered MPT engine implements. The caller
//! chooses between the two at construction time via `StorageConfiguration`.

use std::sync::Arc;

use cfx_internal_common::StateRootWithAuxInfo;
use parking_lot::Mutex;
use primitives::{EpochId, MerkleHash, StateRoot, StorageKeyWithSpace};

use crate::{
    impls::{errors::*, proof_rpc_pool::ProofRpcPool, state_proof::StateProof},
    state::{Changeset, CommitMeta, StorageView},
    state_manager::{
        ConsensusRecoveryView, OpenOptions, RecoveryPlan, StateConfirmedView,
        StateIndex, StorageEngine, StorageVersion,
    },
    MptKeyValue, StorageConfiguration,
};

/// An LVMT-backed read view. Wraps the backend state object and converts
/// storage2's error flavor into the storage crate's `Error`.
pub struct LvmtStorageView(storage2::LvmtState);

impl StorageView for LvmtStorageView {
    fn get(
        &self, access_key: StorageKeyWithSpace,
    ) -> Result<Option<Box<[u8]>>> {
        let key = access_key.to_key_bytes();
        self.0.get(&key).map_err(backend_to_storage_err)
    }

    fn iter_prefix(
        &self, access_key_prefix: StorageKeyWithSpace,
    ) -> Result<Box<dyn Iterator<Item = MptKeyValue>>> {
        let lower = access_key_prefix.to_key_bytes();
        let upper = storage2::to_key_prefix_iter_upper_bound(&lower);
        let pairs = self
            .0
            .iter_range(&lower, upper.as_deref())
            .map_err(backend_to_storage_err)?;
        Ok(Box::new(pairs.into_iter().map(|(k, v)| (k, v))))
    }

    /// Hold the LVMT backend read lock once for the whole batch so a caller
    /// (e.g. `GlobalStat::loaded`) that reads many keys up front amortizes
    /// the lock over the batch rather than paying for every entry.
    fn get_batch(
        &self, keys: &[StorageKeyWithSpace],
    ) -> Result<Vec<Option<Box<[u8]>>>> {
        let key_bytes: Vec<Vec<u8>> =
            keys.iter().map(|k| k.to_key_bytes()).collect();
        let key_refs: Vec<&[u8]> =
            key_bytes.iter().map(|k| k.as_slice()).collect();
        self.0.get_batch(&key_refs).map_err(backend_to_storage_err)
    }
}

/// The LVMT state engine. Holds the backend handle (shared with every
/// `LvmtStorageView` it has handed out) plus the storage configuration the
/// node booted with.
pub struct LvmtStorageEngine {
    manager: Arc<storage2::LvmtStateManager>,
    conf: StorageConfiguration,
    /// Function-level lock that serializes `notify_state_confirmed` so its
    /// read-compute-write dance against the backend and the openable lower
    /// bound cannot interleave across callers. The unit payload is
    /// intentional: this guard only provides mutual exclusion.
    maintenance_lock: Mutex<()>,
    /// Dedicated single-threaded rayon pools for LVMT proof RPC. Isolates
    /// amt's `par_iter` from the global rayon pool so a burst of proof
    /// requests cannot contend with state commit.
    proof_rpc_pool: Arc<ProofRpcPool>,
}

impl LvmtStorageEngine {
    pub fn new(conf: StorageConfiguration) -> Result<Self> {
        let historical = conf.path_storage_dir.join("lvmt_historical");
        let pending = conf.path_storage_dir.join("lvmt_pending");
        // cfx-storage2's `LvmtStorage::new` is the recovery hook: on open it
        // runs pending-part recovery when the pending database is non-empty
        // and falls back to a clean bootstrap on failure. Hosting it here is
        // all the host side has to do for LVMT shutdown recovery.
        let had_pending = pending.exists();
        let manager = storage2::LvmtStateManager::new_arc(historical, pending)
            .map_err(backend_to_storage_err)?;
        info!(
            "LVMT storage engine opened (pending_db preexisting = {})",
            had_pending
        );
        let proof_rpc_pool =
            Arc::new(ProofRpcPool::new(conf.lvmt_proof_pool_threads)?);
        Ok(Self {
            manager,
            conf,
            maintenance_lock: Mutex::new(()),
            proof_rpc_pool,
        })
    }

    pub fn proof_rpc_pool(&self) -> &Arc<ProofRpcPool> { &self.proof_rpc_pool }

    /// Reads the committed state at `epoch` and returns the value together
    /// with an LVMT proof against that state's root. Used by the light-node
    /// proof RPC path through `StorageManager::get_with_proof`.
    pub fn get_with_proof(
        &self, epoch: &EpochId, key: StorageKeyWithSpace,
    ) -> Result<(Option<Box<[u8]>>, StateProof)> {
        let state = self
            .manager
            .open_state_at_epoch(epoch_to_s2(epoch))
            .map_err(backend_to_storage_err)?
            .ok_or_else(|| {
                Error::Msg(format!(
                    "LVMT state at epoch {:?} is not available",
                    epoch
                ))
            })?;
        let key_bytes = key.to_key_bytes();
        let (value, proof_bytes) = state
            .get_with_proof(&key_bytes)
            .map_err(backend_to_storage_err)?;
        Ok((value, StateProof::Lvmt(proof_bytes)))
    }

    /// Looks up the state root stored for a given epoch, with the aux info
    /// placeholder slots the on-wire row demands. `Ok(None)` when the epoch
    /// is not committed here.
    pub fn state_root_at(
        &self, epoch: &EpochId,
    ) -> Result<Option<StateRootWithAuxInfo>> {
        let state = self
            .manager
            .open_state_at_epoch(epoch_to_s2(epoch))
            .map_err(backend_to_storage_err)?;
        Ok(state
            .and_then(|s| s.cached_state_root())
            .map(|root| StateRootWithAuxInfo::from_state_root(lvmt_root(root))))
    }
}

fn epoch_to_s2(id: &EpochId) -> storage2::H256 {
    storage2::H256::from_slice(id.as_bytes())
}

fn s2_to_merkle(h: storage2::H256) -> MerkleHash {
    MerkleHash::from_slice(h.as_bytes())
}

fn backend_to_storage_err(e: storage2::Error) -> Error {
    Error::Msg(format!("LVMT backend: {}", e))
}

/// Builds the one state root form the layered engine committed via
/// `StateRoot::genesis`: LVMT condenses a whole state into one hash and
/// parks it on the `delta_root` slot, leaving the snapshot and
/// intermediate-delta slots empty. This matches what the aux info decoder
/// expects after the refactor: its four placeholder slots are zero and
/// `state_root_hash` is the single hash that matters.
fn lvmt_root(single_hash: storage2::H256) -> StateRoot {
    StateRoot::genesis(&s2_to_merkle(single_hash))
}

impl StorageEngine for LvmtStorageEngine {
    fn open_state(
        self: Arc<Self>, version: StorageVersion, _opts: OpenOptions,
    ) -> Result<Option<Box<dyn StorageView>>> {
        match version {
            StorageVersion::Empty => Ok(Some(Box::new(LvmtStorageView(
                self.manager.open_state_for_genesis_write(),
            )))),
            StorageVersion::Epoch(epoch) => {
                let state = self
                    .manager
                    .open_state_at_epoch(epoch_to_s2(&epoch))
                    .map_err(backend_to_storage_err)?;
                Ok(state.map(|s| {
                    Box::new(LvmtStorageView(s)) as Box<dyn StorageView>
                }))
            }
        }
    }

    fn commit(
        self: Arc<Self>, parent: StorageVersion, changeset: Changeset,
        meta: CommitMeta,
    ) -> Result<StateRoot> {
        let mut state = match parent {
            StorageVersion::Empty => {
                self.manager.open_state_for_genesis_write()
            }
            StorageVersion::Epoch(parent_id) => self
                .manager
                .open_state_for_next_epoch(epoch_to_s2(&parent_id))
                .map_err(backend_to_storage_err)?,
        };
        for (key, value) in changeset {
            match value {
                Some(v) => {
                    state.set(&key, v).map_err(backend_to_storage_err)?
                }
                None => state.delete(&key).map_err(backend_to_storage_err)?,
            }
        }
        let root = state
            .commit(epoch_to_s2(&meta.epoch_id))
            .map_err(backend_to_storage_err)?;
        Ok(lvmt_root(root))
    }

    fn preview_genesis_root(
        self: Arc<Self>, changeset: &Changeset,
    ) -> Result<StateRoot> {
        let mut state = self.manager.open_state_for_genesis_write();
        for (key, value) in changeset {
            match value {
                Some(v) => {
                    state.set(key, v.clone()).map_err(backend_to_storage_err)?
                }
                None => state.delete(key).map_err(backend_to_storage_err)?,
            }
        }
        let root =
            state.compute_state_root().map_err(backend_to_storage_err)?;
        Ok(lvmt_root(root))
    }

    fn usable_as_base(&self, base: &EpochId) -> Result<bool> {
        Ok(self
            .manager
            .open_state_at_epoch(epoch_to_s2(base))
            .map_err(backend_to_storage_err)?
            .is_some())
    }

    /// LVMT does not rebuild snapshots, so the engine never pulls the replay
    /// start height below the one consensus proposed.
    fn plan_recovery(&self, view: &dyn ConsensusRecoveryView) -> RecoveryPlan {
        RecoveryPlan {
            recompute_start_height: view.proposed_recompute_start_height(),
        }
    }

    /// Marks the confirmed epoch as the new pivot, advances the openable
    /// lower bound at snapshot boundaries, and kicks off the
    /// pending-to-history migration plus the pending-area cleanup on a
    /// detached thread.
    ///
    /// The computation of `maintained_state_height_lower_bound` and
    /// `first_available_state_height` mirrors the MPT engine so that the
    /// `StateAvailabilityBoundary` moves at the same cadence under either
    /// backend. The returned value is the new physical openable lower bound;
    /// any failure along the way is logged and the previous bound is kept.
    fn notify_state_confirmed(&self, view: &dyn StateConfirmedView) -> u64 {
        // Serialize concurrent callers so the read-compute-write dance
        // against the backend and the openable lower bound cannot
        // interleave. No deadlock risk: this lock is private to this
        // engine, nothing outside acquires it, and the caller holds no
        // storage locks when it enters here.
        let _guard = self.maintenance_lock.lock();
        let confirmed_height = view.confirmed_height();
        let snapshot_epoch_count =
            self.conf.consensus_param.snapshot_epoch_count;
        let additional_gap = (self.conf.additional_maintained_snapshot_count
            * snapshot_epoch_count) as u64;
        let maintained_state_height_lower_bound =
            confirmed_height.saturating_sub(additional_gap);

        let pivot_hash = match view
            .pivot_hash_at_height(maintained_state_height_lower_bound)
        {
            Ok(hash) => hash,
            Err(msg) => {
                warn!(
                    "LVMT notify_state_confirmed({}): pivot lookup failed: {}; maintenance failed",
                    maintained_state_height_lower_bound, msg
                );
                return 0;
            }
        };
        let pivot_s2 = epoch_to_s2(&pivot_hash);

        let confirmed_intermediate_height = maintained_state_height_lower_bound
            - StateIndex::height_to_delta_height(
                maintained_state_height_lower_bound,
                snapshot_epoch_count,
            ) as u64;
        let confirmed_snapshot_height = confirmed_intermediate_height
            .saturating_sub(snapshot_epoch_count as u64);
        let first_available_state_height = if confirmed_snapshot_height > 0 {
            confirmed_snapshot_height + 1
        } else {
            0
        };

        let non_pivot_removed = match self.manager.make_pivot(pivot_s2) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "LVMT notify_state_confirmed({}): make_pivot failed: {:?}; maintenance failed",
                    maintained_state_height_lower_bound, e
                );
                return 0;
            }
        };
        let adjust_pending_root = match self
            .manager
            .is_newer_than_pending_root(first_available_state_height)
        {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "LVMT notify_state_confirmed({}): is_newer_than_pending_root failed: {:?}; maintenance failed",
                    first_available_state_height, e
                );
                return 0;
            }
        };

        if non_pivot_removed || adjust_pending_root {
            let migrate = if adjust_pending_root {
                Some((first_available_state_height, pivot_s2))
            } else {
                None
            };
            match self.manager.maintenance_deferred(migrate) {
                Ok(m) => m.spawn(),
                Err(e) => {
                    warn!(
                        "LVMT notify_state_confirmed({}): maintenance_deferred failed: {:?}; maintenance failed",
                        first_available_state_height, e
                    );
                    return 0;
                }
            }
        }

        info!(
            "LVMT notify_state_confirmed: confirmed_height {} \
             first_available_state_height {}",
            confirmed_height, first_available_state_height,
        );
        first_available_state_height
    }
}
