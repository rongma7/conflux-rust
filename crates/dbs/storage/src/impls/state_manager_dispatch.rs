// Copyright 2024 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

//! Runtime dispatch layer that lets the node choose between the original
//! MPT storage backend (`impls::state_manager::StateManager`) and the LVMT
//! backend (`impls::state_manager2::StateManager2`) based on
//! `StorageConfiguration::use_lvmt_storage`.
//!
//! The public types `StateManager` / `StorageState` are the ones consumers
//! see from `cfx_storage`; both are enums that forward to whichever backend
//! was selected at construction time.

use std::sync::Arc;

use std::collections::HashSet;

use cfx_internal_common::{
    consensus_api::StateMaintenanceTrait, StateAvailabilityBoundary,
    StateRootWithAuxInfo,
};
use cfx_types::{AddressWithSpace, Space};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use parking_lot::RwLock;
use primitives::{
    EpochId, MerkleHash, NodeMerkleTriplet, StaticBool, StorageKeyWithSpace,
    StorageRoot,
};

use crate::{
    impls::{
        errors::*,
        node_merkle_proof::NodeMerkleProof,
        state::State as MptState,
        state_manager::StateManager as MptStateManagerInner,
        state_manager2::{
            LvmtStateManagerWithConf, StateManager2 as LvmtStateManagerInner,
            WrappedLvmtState,
        },
        state_proof::StateProof,
        storage_manager::storage_manager::StorageManager as MptStorageManager,
    },
    state::{StateDbGetOriginalMethods, StateTraitExt},
    storage_db::SnapshotInfo,
    StorageConfiguration, StorageRootProof,
};
use storage2::{
    state::StateTrait as StorageStateTrait, state_manager::StateIndex,
    state_manager::StateManagerTrait as StorageManagerTrait, MptKeyValue,
    Result as Storage2Result,
};

/// The public state manager seen from `cfx_storage`.
///
/// It owns one of two concrete backends and forwards every method call to
/// the chosen variant. Backend is selected once at construction time from
/// `StorageConfiguration::use_lvmt_storage` and never changes afterwards.
pub struct StateManager {
    backend: Backend,
}

enum Backend {
    Mpt(Arc<MptStateManagerInner>),
    Lvmt(Arc<LvmtStateManagerInner>),
}

/// Reference to the inner storage-manager object held by a backend.
///
/// Returned by `StateManager::get_storage_manager`; it forwards the small
/// method set that consumer code actually calls on that object.
pub enum StorageBackendRef<'a> {
    Mpt(&'a MptStorageManager),
    Lvmt(&'a LvmtStateManagerWithConf),
}

impl<'a> StorageBackendRef<'a> {
    pub fn get_snapshot_epoch_count(&self) -> u32 {
        match self {
            Self::Mpt(m) => m.get_snapshot_epoch_count(),
            Self::Lvmt(l) => l.get_snapshot_epoch_count(),
        }
    }

    pub fn get_snapshot_manager(
        &self,
    ) -> &(dyn crate::snapshot_manager::SnapshotManagerTrait<
        SnapshotDb = crate::impls::state_manager::SnapshotDb,
        SnapshotDbManager = crate::impls::state_manager::SnapshotDbManager,
    > + Send
             + Sync) {
        match self {
            Self::Mpt(m) => m.get_snapshot_manager(),
            Self::Lvmt(l) => l.get_snapshot_manager(),
        }
    }

    pub fn get_snapshot_info_at_epoch(
        &self, snapshot_epoch_id: &EpochId,
    ) -> Option<SnapshotInfo> {
        match self {
            Self::Mpt(m) => m.get_snapshot_info_at_epoch(snapshot_epoch_id),
            Self::Lvmt(l) => l.get_snapshot_info_at_epoch(snapshot_epoch_id),
        }
    }

    pub fn maintain_state_confirmed<ConsensusInner: StateMaintenanceTrait>(
        &self, consensus_inner: &ConsensusInner,
        stable_checkpoint_height: u64, era_epoch_count: u64,
        confirmed_height: u64,
        state_availability_boundary: &RwLock<StateAvailabilityBoundary>,
    ) -> Result<()> {
        match self {
            Self::Mpt(m) => m.maintain_state_confirmed(
                consensus_inner,
                stable_checkpoint_height,
                era_epoch_count,
                confirmed_height,
                state_availability_boundary,
            ),
            Self::Lvmt(l) => l.maintain_state_confirmed(
                consensus_inner,
                stable_checkpoint_height,
                era_epoch_count,
                confirmed_height,
                state_availability_boundary,
            ),
        }
    }

    pub fn storage_conf(&self) -> &StorageConfiguration {
        match self {
            Self::Mpt(m) => &m.storage_conf,
            Self::Lvmt(l) => &l.storage_conf,
        }
    }

    pub fn intermediate_trie_root_merkle(
        &self,
    ) -> &RwLock<Option<MerkleHash>> {
        match self {
            Self::Mpt(m) => &m.intermediate_trie_root_merkle,
            Self::Lvmt(l) => &l.intermediate_trie_root_merkle,
        }
    }

    pub fn persist_state_from_initialization(
        &self,
    ) -> &RwLock<Option<(Option<EpochId>, HashSet<EpochId>, u64, Option<u64>)>>
    {
        match self {
            Self::Mpt(m) => &m.persist_state_from_initialization,
            Self::Lvmt(l) => &l.persist_state_from_initialization,
        }
    }
}

impl StateManager {
    pub fn new(conf: StorageConfiguration) -> Result<Self> {
        let backend = if conf.use_lvmt_storage {
            info!("state manager backend: LVMT (storage2)");
            Backend::Lvmt(Arc::new(LvmtStateManagerInner::new(conf)?))
        } else {
            info!("state manager backend: MPT (storage)");
            Backend::Mpt(Arc::new(MptStateManagerInner::new(conf)?))
        };
        Ok(StateManager { backend })
    }

    pub fn log_usage(&self) {
        match &self.backend {
            Backend::Mpt(m) => m.log_usage(),
            Backend::Lvmt(l) => l.log_usage(),
        }
    }

    pub fn notify_genesis_hash(&self, genesis_hash: EpochId) {
        match &self.backend {
            Backend::Mpt(m) => m.notify_genesis_hash(genesis_hash),
            Backend::Lvmt(l) => l.notify_genesis_hash(genesis_hash),
        }
    }

    pub fn config(&self) -> &StorageConfiguration {
        match &self.backend {
            Backend::Mpt(m) => m.config(),
            Backend::Lvmt(l) => l.config(),
        }
    }

    pub fn get_storage_manager(&self) -> StorageBackendRef<'_> {
        match &self.backend {
            Backend::Mpt(m) => {
                StorageBackendRef::Mpt(m.get_storage_manager())
            }
            Backend::Lvmt(l) => {
                StorageBackendRef::Lvmt(l.get_storage_manager())
            }
        }
    }

    /// Returns a reference to the inner MPT storage manager Arc.
    /// Panics when the LVMT backend is active; matches the current
    /// behavior of StateManager2::get_storage_manager_arc (unimplemented).
    pub fn get_storage_manager_arc(&self) -> &Arc<MptStorageManager> {
        match &self.backend {
            Backend::Mpt(m) => m.get_storage_manager_arc(),
            Backend::Lvmt(_) => {
                unimplemented!(
                    "get_storage_manager_arc is not implemented for the LVMT backend"
                )
            }
        }
    }

    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, state_index: StateIndex, try_open: bool,
        open_mpt_snapshot: bool,
    ) -> Result<Option<StorageState>> {
        match &self.backend {
            Backend::Mpt(m) => Ok(m
                .get_state_no_commit_inner(
                    state_index,
                    try_open,
                    open_mpt_snapshot,
                )?
                .map(StorageState::Mpt)),
            Backend::Lvmt(l) => Ok(l
                .get_state_no_commit_inner(
                    state_index,
                    try_open,
                    open_mpt_snapshot,
                )?
                .map(StorageState::Lvmt)),
        }
    }
}

impl MallocSizeOf for StateManager {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize { 0 }
}

impl StorageManagerTrait for StateManager {
    fn get_state_no_commit(
        self: &Arc<Self>, epoch_id: StateIndex, try_open: bool,
        space: Option<Space>,
    ) -> Storage2Result<Option<Box<dyn StorageStateTrait>>> {
        match &self.backend {
            Backend::Mpt(m) => m.get_state_no_commit(epoch_id, try_open, space),
            Backend::Lvmt(l) => {
                l.get_state_no_commit(epoch_id, try_open, space)
            }
        }
    }

    fn get_state_for_next_epoch(
        self: &Arc<Self>, parent_epoch_id: StateIndex,
        recover_mpt_during_construct_pivot_state: bool,
    ) -> Storage2Result<Option<Box<dyn StorageStateTrait>>> {
        match &self.backend {
            Backend::Mpt(m) => m.get_state_for_next_epoch(
                parent_epoch_id,
                recover_mpt_during_construct_pivot_state,
            ),
            Backend::Lvmt(l) => l.get_state_for_next_epoch(
                parent_epoch_id,
                recover_mpt_during_construct_pivot_state,
            ),
        }
    }

    fn get_state_for_genesis_write(
        self: &Arc<Self>,
    ) -> Box<dyn StorageStateTrait> {
        match &self.backend {
            Backend::Mpt(m) => m.get_state_for_genesis_write(),
            Backend::Lvmt(l) => l.get_state_for_genesis_write(),
        }
    }
}

/// The concrete state object returned by `get_state_no_commit_inner`.
///
/// It covers the trait surface previously served by the LVMT
/// `WrappedLvmtState` (`StateDbGetOriginalMethods`, `StorageStateTrait`,
/// `StateTraitExt`). For the LVMT variant the methods that were already
/// `unimplemented!()` on `WrappedLvmtState` stay unimplemented; nothing
/// changes for LVMT consumers.
pub enum StorageState {
    Mpt(MptState),
    Lvmt(WrappedLvmtState),
}

impl StateDbGetOriginalMethods for StorageState {
    fn get_original_raw_with_proof(
        &self, key: StorageKeyWithSpace,
    ) -> Result<(Option<Box<[u8]>>, StateProof)> {
        match self {
            Self::Mpt(s) => s.get_original_raw_with_proof(key),
            Self::Lvmt(s) => s.get_original_raw_with_proof(key),
        }
    }

    fn get_original_storage_root(
        &self, address: &AddressWithSpace,
    ) -> Result<StorageRoot> {
        match self {
            Self::Mpt(s) => s.get_original_storage_root(address),
            Self::Lvmt(s) => s.get_original_storage_root(address),
        }
    }

    fn get_original_storage_root_with_proof(
        &self, address: &AddressWithSpace,
    ) -> Result<(StorageRoot, StorageRootProof)> {
        match self {
            Self::Mpt(s) => s.get_original_storage_root_with_proof(address),
            Self::Lvmt(s) => s.get_original_storage_root_with_proof(address),
        }
    }
}

impl StateTraitExt for StorageState {
    fn get_with_proof(
        &self, access_key: StorageKeyWithSpace,
    ) -> Result<(Option<Box<[u8]>>, StateProof)> {
        match self {
            Self::Mpt(s) => s.get_with_proof(access_key),
            Self::Lvmt(_) => unimplemented!(
                "LVMT: StateTraitExt::get_with_proof is not implemented"
            ),
        }
    }

    fn get_node_merkle_all_versions<WithProof: StaticBool>(
        &self, access_key: StorageKeyWithSpace,
    ) -> Result<(NodeMerkleTriplet, NodeMerkleProof)> {
        match self {
            Self::Mpt(s) => {
                s.get_node_merkle_all_versions::<WithProof>(access_key)
            }
            Self::Lvmt(_) => unimplemented!(
                "LVMT: StateTraitExt::get_node_merkle_all_versions is not implemented"
            ),
        }
    }
}

impl StorageStateTrait for StorageState {
    fn get(
        &self, access_key: StorageKeyWithSpace,
    ) -> Storage2Result<Option<Box<[u8]>>> {
        match self {
            Self::Mpt(s) => s.get(access_key),
            Self::Lvmt(s) => s.get(access_key),
        }
    }

    fn set(
        &mut self, access_key: StorageKeyWithSpace, value: Box<[u8]>,
    ) -> Storage2Result<()> {
        match self {
            Self::Mpt(s) => s.set(access_key, value),
            Self::Lvmt(s) => s.set(access_key, value),
        }
    }

    fn delete(
        &mut self, access_key: StorageKeyWithSpace,
    ) -> Storage2Result<()> {
        match self {
            Self::Mpt(s) => s.delete(access_key),
            Self::Lvmt(s) => s.delete(access_key),
        }
    }

    fn delete_test_only(
        &mut self, access_key: StorageKeyWithSpace,
    ) -> Storage2Result<Option<Box<[u8]>>> {
        match self {
            Self::Mpt(s) => s.delete_test_only(access_key),
            Self::Lvmt(s) => s.delete_test_only(access_key),
        }
    }

    fn delete_all(
        &mut self, access_key_prefix: StorageKeyWithSpace,
    ) -> Storage2Result<Option<Vec<MptKeyValue>>> {
        match self {
            Self::Mpt(s) => s.delete_all(access_key_prefix),
            Self::Lvmt(s) => s.delete_all(access_key_prefix),
        }
    }

    fn read_all(
        &mut self, access_key_prefix: StorageKeyWithSpace,
    ) -> Storage2Result<Option<Vec<MptKeyValue>>> {
        match self {
            Self::Mpt(s) => s.read_all(access_key_prefix),
            Self::Lvmt(s) => s.read_all(access_key_prefix),
        }
    }

    fn read_all_with_callback(
        &mut self, access_key_prefix: StorageKeyWithSpace,
        callback: &mut dyn FnMut(MptKeyValue), only_account_key: bool,
    ) -> Storage2Result<()> {
        match self {
            Self::Mpt(s) => s.read_all_with_callback(
                access_key_prefix,
                callback,
                only_account_key,
            ),
            Self::Lvmt(s) => s.read_all_with_callback(
                access_key_prefix,
                callback,
                only_account_key,
            ),
        }
    }

    fn get_batch(
        &self, keys: &[StorageKeyWithSpace],
    ) -> Storage2Result<Vec<Option<Box<[u8]>>>> {
        match self {
            Self::Mpt(s) => s.get_batch(keys),
            Self::Lvmt(s) => s.get_batch(keys),
        }
    }

    fn compute_state_root(&mut self) -> Storage2Result<StateRootWithAuxInfo> {
        match self {
            Self::Mpt(s) => s.compute_state_root(),
            Self::Lvmt(s) => s.compute_state_root(),
        }
    }

    fn get_state_root(&self) -> Storage2Result<StateRootWithAuxInfo> {
        match self {
            Self::Mpt(s) => s.get_state_root(),
            Self::Lvmt(s) => s.get_state_root(),
        }
    }

    fn commit(
        &mut self, epoch: EpochId,
    ) -> Storage2Result<StateRootWithAuxInfo> {
        match self {
            Self::Mpt(s) => s.commit(epoch),
            Self::Lvmt(s) => s.commit(epoch),
        }
    }
}
