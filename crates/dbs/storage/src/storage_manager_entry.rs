// Copyright 2026 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

//! The public `StorageManager`: a thin wrapper that owns whichever backend
//! the `use_lvmt_storage` flag in `StorageConfiguration` selected, and the
//! external surface callers depend on no matter which backend is running.
//!
//! The wrapper itself implements `StorageEngine` (forwarded to the chosen
//! backend). MPT-only helpers used at startup (`needs_state_index_migration`,
//! `migrate_state_index`, `log_usage`, `notify_genesis_hash`) are forwarded
//! only when the MPT backend is active; the LVMT backend no-ops them.

use std::sync::Arc;

use cfx_internal_common::StateRootWithAuxInfo;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use primitives::{EpochId, StateRoot};

use crate::{
    impls::{
        errors::*, lvmt_engine::LvmtStorageEngine,
        proof_rpc_pool::ProofRpcPool, state::State as MptState,
        state_export::StateExport,
        state_manager::StateManager as MptStateManager,
        state_proof::StateProof,
        storage_manager::storage_manager::StorageManager as MptSnapshotStorageManager,
    },
    state::{Changeset, CommitMeta, StorageView},
    state_manager::{
        ConsensusRecoveryView, OpenOptions, RecoveryPlan, StateConfirmedView,
        StorageEngine, StorageVersion,
    },
    StorageConfiguration,
};
use primitives::StorageKeyWithSpace;

pub struct StorageManager {
    engine: Arc<dyn StorageEngine>,
    /// Kept as an owned `Arc` when the MPT backend is active, so the
    /// startup-time helpers below can call methods that are not on the
    /// `StorageEngine` trait.
    mpt_specific: Option<Arc<MptStateManager>>,
    /// The symmetric handle for LVMT-specific calls.
    lvmt_specific: Option<Arc<LvmtStorageEngine>>,
    /// A lightweight handle on the configuration the backend booted with,
    /// mirroring what each backend stores internally.
    config: StorageConfiguration,
}

impl StorageManager {
    pub fn new(conf: StorageConfiguration) -> Result<Self> {
        let config = conf.clone();
        if conf.use_lvmt_storage {
            let lvmt = Arc::new(LvmtStorageEngine::new(conf)?);
            let engine: Arc<dyn StorageEngine> = lvmt.clone();
            Ok(Self {
                engine,
                mpt_specific: None,
                lvmt_specific: Some(lvmt),
                config,
            })
        } else {
            let mpt = Arc::new(MptStateManager::new(conf)?);
            let engine: Arc<dyn StorageEngine> = mpt.clone();
            Ok(Self {
                engine,
                mpt_specific: Some(mpt),
                lvmt_specific: None,
                config,
            })
        }
    }

    /// The `(state_root, aux_info)` pair for one already-committed epoch, or
    /// `Ok(None)` when the epoch is not on this node. Lets the startup path
    /// retrieve the genesis state root without first opening a per-backend
    /// concrete state object.
    pub fn state_root_at(
        &self, epoch: &EpochId,
    ) -> Result<Option<StateRootWithAuxInfo>> {
        if let Some(m) = &self.mpt_specific {
            return Ok(m
                .open_layered_state(epoch, OpenOptions::new(), false)?
                .map(|s| s.get_state_root().unwrap()));
        }
        if let Some(l) = &self.lvmt_specific {
            return l.state_root_at(epoch);
        }
        Ok(None)
    }

    pub fn config(&self) -> &StorageConfiguration { &self.config }

    pub fn is_lvmt(&self) -> bool { self.mpt_specific.is_none() }

    /// Returns the MPT engine when that is what is running; `None` on LVMT.
    /// Used by paths that explicitly need the layered engine (snapshot
    /// sync, state export, operational tooling).
    pub fn as_mpt(&self) -> Option<&Arc<MptStateManager>> {
        self.mpt_specific.as_ref()
    }

    /// MPT-only. Returns `Ok(false)` on LVMT: there is no commitment-row
    /// table to migrate.
    pub fn needs_state_index_migration(&self) -> Result<bool> {
        match &self.mpt_specific {
            Some(m) => m.needs_state_index_migration(),
            None => Ok(false),
        }
    }

    /// MPT-only. No-ops on LVMT.
    pub fn migrate_state_index(
        &self, state_root_of_epoch: &dyn Fn(&EpochId) -> Option<StateRoot>,
    ) -> Result<()> {
        match &self.mpt_specific {
            Some(m) => m.migrate_state_index(state_root_of_epoch),
            None => Ok(()),
        }
    }

    /// MPT-only. No-ops on LVMT.
    pub fn log_usage(&self) {
        if let Some(m) = &self.mpt_specific {
            m.log_usage();
        }
    }

    /// MPT-only. No-ops on LVMT: LVMT does not stash the genesis hash for
    /// the single-MPT replica path.
    pub fn notify_genesis_hash(&self, genesis_hash: EpochId) {
        if let Some(m) = &self.mpt_specific {
            m.notify_genesis_hash(genesis_hash);
        }
    }

    /// Opens the layered `State` directly, bypassing `StorageView`. Used by
    /// the light protocol and RPC paths that read per-layer merkle data,
    /// which only the MPT backend produces. Returns `Ok(None)` on LVMT so
    /// callers can gracefully bail without a panic.
    pub fn open_layered_state(
        self: &Arc<Self>, epoch_id: &EpochId, opts: OpenOptions,
        open_mpt_snapshot: bool,
    ) -> Result<Option<MptState>> {
        match &self.mpt_specific {
            Some(m) => m.open_layered_state(epoch_id, opts, open_mpt_snapshot),
            None => Ok(None),
        }
    }

    /// MPT-only. Panics on LVMT: the snapshot manager handles chunked
    /// snapshot sync, which LVMT does not use.
    pub fn get_storage_manager(&self) -> &MptSnapshotStorageManager {
        self.mpt_specific
            .as_ref()
            .expect(
                "get_storage_manager is only available on the MPT backend; \
                 LVMT does not run the chunked snapshot sync path",
            )
            .get_storage_manager()
    }

    /// MPT-only. Panics on LVMT, same reason as `get_storage_manager`.
    pub fn get_storage_manager_arc(&self) -> &Arc<MptSnapshotStorageManager> {
        self.mpt_specific
            .as_ref()
            .expect(
                "get_storage_manager_arc is only available on the MPT backend",
            )
            .get_storage_manager_arc()
    }

    /// Opens the state for the operational export path (`state_dump`), which
    /// walks the trie through a callback the `StorageView` surface does not
    /// offer. Returns `Ok(None)` on LVMT — the `state_dump` tool currently
    /// relies on MPT-shaped iteration.
    pub fn open_state_for_export(
        self: &Arc<Self>, epoch_hash: &EpochId, opts: OpenOptions,
    ) -> Result<Option<StateExport>> {
        match &self.mpt_specific {
            Some(m) => m.open_state_for_export(epoch_hash, opts),
            None => Ok(None),
        }
    }

    /// `(value, state_proof)` for `key` in the already-committed state at
    /// `epoch`. Dispatches by backend: MPT returns a three-tier trie proof,
    /// LVMT returns an AMT+MMR proof in bytes. The light protocol feeds the
    /// closure into `proof_rpc_pool` so amt `par_iter` stays off the global
    /// rayon pool.
    pub fn get_with_proof(
        self: &Arc<Self>, epoch: &EpochId, key: StorageKeyWithSpace,
    ) -> Result<(Option<Box<[u8]>>, StateProof)> {
        if let Some(m) = &self.mpt_specific {
            let state = m
                .open_layered_state(epoch, OpenOptions::new(), true)?
                .ok_or_else(|| {
                    Error::Msg(format!(
                        "MPT state at epoch {:?} is not available",
                        epoch
                    ))
                })?;
            let (value, proof) = state.get_with_proof(key)?;
            return Ok((value, proof));
        }
        if let Some(l) = &self.lvmt_specific {
            return l.get_with_proof(epoch, key);
        }
        Err(Error::Msg("no backend selected".into()))
    }

    /// Dedicated rayon pool for LVMT proof RPC. `None` on MPT, where trie
    /// proofs don't need isolation.
    pub fn proof_rpc_pool(&self) -> Option<&Arc<ProofRpcPool>> {
        self.lvmt_specific.as_ref().map(|l| l.proof_rpc_pool())
    }
}

impl StorageEngine for StorageManager {
    fn open_state(
        self: Arc<Self>, version: StorageVersion, opts: OpenOptions,
    ) -> Result<Option<Box<dyn StorageView>>> {
        Arc::clone(&self.engine).open_state(version, opts)
    }

    fn commit(
        self: Arc<Self>, parent: StorageVersion, changeset: Changeset,
        meta: CommitMeta,
    ) -> Result<StateRoot> {
        Arc::clone(&self.engine).commit(parent, changeset, meta)
    }

    fn preview_genesis_root(
        self: Arc<Self>, changeset: &Changeset,
    ) -> Result<StateRoot> {
        Arc::clone(&self.engine).preview_genesis_root(changeset)
    }

    fn usable_as_base(&self, base: &EpochId) -> Result<bool> {
        self.engine.usable_as_base(base)
    }

    fn plan_recovery(&self, view: &dyn ConsensusRecoveryView) -> RecoveryPlan {
        self.engine.plan_recovery(view)
    }

    fn notify_state_confirmed(&self, view: &dyn StateConfirmedView) -> u64 {
        self.engine.notify_state_confirmed(view)
    }
}

impl MallocSizeOf for StorageManager {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        match &self.mpt_specific {
            Some(m) => m.size_of(ops),
            None => 0,
        }
    }
}
