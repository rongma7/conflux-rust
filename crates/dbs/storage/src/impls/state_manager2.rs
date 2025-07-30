// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub struct LvmtStateManagerWithConf {
    lvmt_manager: Arc<LvmtStateManager>,
    pub storage_conf: StorageConfiguration,
    // used during startup for the next compute epoch
    pub intermediate_trie_root_merkle: RwLock<Option<MerkleHash>>,
    pub persist_state_from_initialization: // todo
        RwLock<Option<(Option<EpochId>, HashSet<EpochId>, u64, Option<u64>)>>,
}

impl LvmtStateManagerWithConf {
    pub fn new_arc(storage_conf: StorageConfiguration) -> Arc<Self> {
        let lvmt_manager =
            LvmtStateManager::new_arc(storage_conf.path_storage_dir.join("lvmt"));
        Arc::new(Self { lvmt_manager, storage_conf, intermediate_trie_root_merkle: RwLock::new(None), persist_state_from_initialization: RwLock::new(None) })
    }
}

// Methods for LvmtStateManagerWithConf as a peer of StorageManager
impl LvmtStateManagerWithConf {
    pub fn get_snapshot_epoch_count(&self) -> u32 {
        self.storage_conf.consensus_param.snapshot_epoch_count
    }

    pub fn maintain_state_confirmed<ConsensusInner: StateMaintenanceTrait>(
        &self, consensus_inner: &ConsensusInner, stable_checkpoint_height: u64,
        era_epoch_count: u64, confirmed_height: u64,
        state_availability_boundary: &RwLock<StateAvailabilityBoundary>,
    ) -> Result<()> {
        unimplemented!()
    }

    pub fn get_snapshot_manager(
        &self,
    ) -> &(dyn SnapshotManagerTrait<
        SnapshotDb = SnapshotDb,
        SnapshotDbManager = SnapshotDbManager,
    > + Send + Sync) {
        unimplemented!()
    }

    pub fn get_snapshot_info_at_epoch(
        &self, _snapshot_epoch_id: &EpochId,
    ) -> Option<SnapshotInfo> {
        unimplemented!()
    }
}

pub struct StateManager2 {
    lvmt_manager: Arc<LvmtStateManagerWithConf>,
    pub number_committed_nodes: AtomicUsize,
}

impl MallocSizeOf for StateManager2 {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        let size = 0;
        size
    }
}

impl StateManager2 {
    pub fn new(conf: StorageConfiguration) -> Result<Self> {
        debug!("Storage conf {:?}", conf);
        // Make sure sqlite temp directory is using the data disk instead of the
        // system disk.
        std::env::set_var("SQLITE_TMPDIR", conf.path_snapshot_dir.clone());

        let lvmt_manager =
            LvmtStateManagerWithConf::new_arc(conf);

        Ok(Self {
            lvmt_manager,
            number_committed_nodes: Default::default(),
        })
    }

    pub fn log_usage(&self) {
        debug!(
            "number of nodes committed to db {}",
            self.number_committed_nodes.load(Ordering::Relaxed),
        );
    }

    pub fn get_storage_manager(&self) -> &LvmtStateManagerWithConf {
        &self.lvmt_manager
    }

    pub fn get_storage_manager_arc(&self) -> &Arc<StorageManager> {
        unimplemented!()
    }

    pub fn notify_genesis_hash(&self, _genesis_hash: EpochId) { () }

    pub fn config(&self) -> &StorageConfiguration { &self.lvmt_manager.storage_conf }

    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, _state_index: StateIndex, _try_open: bool,
        _open_mpt_snapshot: bool,
    ) -> Result<Option<State>> {
        unimplemented!()
    }

    pub fn commit(
        &self, write_schema: <Database as DatabaseTrait>::WriteSchema,
    ) -> Result<()> {
        self.lvmt_manager.lvmt_manager.commit(write_schema)
    }
}

impl StateManagerTrait for StateManager2 {
    fn get_state_no_commit(
        self: &Arc<Self>, state_index: StateIndex, try_open: bool,
        space: Option<Space>,
    ) -> Result<Option<Box<dyn StateTrait>>> {
        debug!("read state from lvmt state: epoch={}", state_index.epoch_id);
        self.lvmt_manager.lvmt_manager
            .get_state_no_commit(state_index, try_open, space)
    }

    fn get_state_for_genesis_write(self: &Arc<Self>) -> Box<dyn StateTrait> {
        self.lvmt_manager.lvmt_manager.get_state_for_genesis_write()
    }

    fn get_state_for_next_epoch(
        self: &Arc<Self>, parent_epoch_id: StateIndex,
        recover_mpt_during_construct_pivot_state: bool,
    ) -> Result<Option<Box<dyn StateTrait>>> {
        self.lvmt_manager.lvmt_manager.get_state_for_next_epoch(
            parent_epoch_id,
            recover_mpt_during_construct_pivot_state,
        )
    }
}

use crate::{
    impls::errors::*, snapshot_manager::SnapshotManagerTrait, state::*, state_manager::*, storage_db::SnapshotInfo, StorageConfiguration
};
use cfx_internal_common::{consensus_api::StateMaintenanceTrait, StateAvailabilityBoundary};
use cfx_types::Space;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use parking_lot::RwLock;
use primitives::{EpochId, MerkleHash};
use std::{collections::HashSet, sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
}};
use storage2::{Database, DatabaseTrait, LvmtStateManager};

use super::{state_manager::{SnapshotDb, SnapshotDbManager}, storage_manager::StorageManager};
