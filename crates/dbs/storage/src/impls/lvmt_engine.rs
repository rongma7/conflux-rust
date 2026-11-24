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
        StorageEngine, StorageVersion,
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
/// `LvmtStorageView` it has handed out).
pub struct LvmtStorageEngine {
    manager: Arc<storage2::LvmtStateManager>,
    /// Function-level lock that serializes `notify_state_confirmed` so its
    /// read-compute-write dance against the backend and the openable lower
    /// bound cannot interleave across callers. The unit payload is
    /// intentional: this guard only provides mutual exclusion.
    maintenance_lock: Mutex<()>,
    /// Dedicated single-threaded rayon pools for LVMT proof RPC. Isolates
    /// amt's `par_iter` from the global rayon pool so a burst of proof
    /// requests cannot contend with state commit.
    proof_rpc_pool: Arc<ProofRpcPool>,
    /// Pending-part retention window in commits (= epochs). Non-pivot
    /// branches whose fork point is above `confirmed_height - W` stay
    /// reachable in pending so mining preflight can replay them. Mirrors
    /// the MPT engine's `additional_maintained_snapshot_count ×
    /// snapshot_epoch_count` window on a per-commit granularity — the
    /// layered MPT engine reaps snapshot-period chunks, LVMT reaps per
    /// commit. See `工作总结/T3-step2-lvmt-alignment-draft-2026-11-24.md`
    /// §3 direction A.
    pending_retention_commits: u64,
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
        // Explicit override wins; otherwise fall back to the MPT engine's
        // equivalent retention window so LVMT's user-visible fork-mining
        // reach stays on parity with MPT by default.
        let pending_retention_commits =
            conf.lvmt_pending_retention_commits.unwrap_or_else(|| {
                u64::from(conf.additional_maintained_snapshot_count)
                    * u64::from(conf.consensus_param.snapshot_epoch_count)
            });
        info!(
            "LVMT pending retention window: {} commits (override={:?}, \
             additional_maintained_snapshot_count={}, snapshot_epoch_count={})",
            pending_retention_commits,
            conf.lvmt_pending_retention_commits,
            conf.additional_maintained_snapshot_count,
            conf.consensus_param.snapshot_epoch_count,
        );
        Ok(Self {
            manager,
            maintenance_lock: Mutex::new(()),
            proof_rpc_pool,
            pending_retention_commits,
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

        // The pending part has to cover `(target_height, pivot_tip]`:
        // consensus can still reorg there, mining's blame / deferred-state
        // preflight can replay non-pivot forks there, and RPC reads pivot
        // epoch state out of it. `target_height` sits `pending_retention
        // _commits` below `confirmed_height` so forks whose point is in
        // `(target, confirmed]` stay reachable — the MPT engine's
        // `confirmed - additional_maintained_snapshot_count × snapshot
        // _epoch_count` window, per commit instead of per snapshot period.
        let target_height =
            confirmed_height.saturating_sub(self.pending_retention_commits);
        // Mirrors the MPT engine's `H ≤ physical_openable_lower_bound`
        // short-circuit in `maintain_state_confirmed`: the target has not
        // yet risen above the engine's openable lower bound (0 under LVMT
        // until a historical GC path lands), so nothing to prune or
        // migrate this round.
        if target_height == 0 {
            info!(
                "LVMT notify_state_confirmed: confirmed_height {} within \
                 retention window {} (target {}), skipping maintenance",
                confirmed_height, self.pending_retention_commits, target_height,
            );
            return 0;
        }
        let pivot_hash = match view.pivot_hash_at_height(target_height) {
            Ok(hash) => hash,
            Err(msg) => {
                warn!(
                    "LVMT notify_state_confirmed({}, target {}): pivot lookup failed: {}; maintenance failed",
                    confirmed_height, target_height, msg
                );
                return 0;
            }
        };
        let pivot_s2 = epoch_to_s2(&pivot_hash);

        let non_pivot_removed = match self.manager.make_pivot(pivot_s2) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "LVMT notify_state_confirmed({}, target {}): make_pivot failed: {:?}; maintenance failed",
                    confirmed_height, target_height, e
                );
                return 0;
            }
        };
        let adjust_pending_root = match self
            .manager
            .is_newer_than_pending_root(target_height)
        {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "LVMT notify_state_confirmed({}, target {}): is_newer_than_pending_root failed: {:?}; maintenance failed",
                    confirmed_height, target_height, e
                );
                return 0;
            }
        };

        if non_pivot_removed || adjust_pending_root {
            let migrate = if adjust_pending_root {
                Some((target_height, pivot_s2))
            } else {
                None
            };
            match self.manager.maintenance_deferred(migrate) {
                Ok(m) => m.spawn(),
                Err(e) => {
                    warn!(
                        "LVMT notify_state_confirmed({}, target {}): maintenance_deferred failed: {:?}; maintenance failed",
                        confirmed_height, target_height, e
                    );
                    return 0;
                }
            }
        }

        // "RPC lowest readable state height." Advancing this needs a
        // historical GC path cfx-storage2 does not yet expose, so stay at
        // 0 (archive behaviour): everything in historical is readable and
        // nothing is reclaimed on disk. When the GC API lands, return the
        // floor it reports.
        let first_available_state_height = 0u64;
        info!(
            "LVMT notify_state_confirmed: confirmed_height {} target {} \
             pending_new_root_height {} first_available_state_height {}",
            confirmed_height,
            target_height,
            target_height,
            first_available_state_height,
        );
        first_available_state_height
    }
}
