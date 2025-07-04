// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

pub struct StateManager2 {
    lvmt_manager: Arc<LvmtStateManager>,
    pub storage_conf: StorageConfiguration,
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
            LvmtStateManager::new_arc(conf.path_storage_dir.join("lvmt"));

        Ok(Self {
            lvmt_manager,
            storage_conf: conf,
            number_committed_nodes: Default::default(),
        })
    }

    pub fn log_usage(&self) {
        debug!(
            "number of nodes committed to db {}",
            self.number_committed_nodes.load(Ordering::Relaxed),
        );
    }

    pub fn get_storage_manager(&self) -> &StorageManager { unimplemented!() }

    pub fn get_storage_manager_arc(&self) -> &Arc<StorageManager> {
        unimplemented!()
    }

    pub fn notify_genesis_hash(&self, _genesis_hash: EpochId) {
        unimplemented!()
    }

    pub fn config(&self) -> &StorageConfiguration { &self.storage_conf }

    pub fn get_snapshot_epoch_count(&self) -> u32 {
        self.storage_conf.consensus_param.snapshot_epoch_count
    }

    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, _state_index: StateIndex, _try_open: bool,
        _open_mpt_snapshot: bool,
    ) -> Result<Option<State>> {
        unimplemented!()
    }
}

impl StateManagerTrait for StateManager2 {
    fn get_state_no_commit(
        self: &Arc<Self>, state_index: StateIndex, try_open: bool,
        space: Option<Space>,
    ) -> Result<Option<Box<dyn StateTrait>>> {
        debug!("read state from lvmt state: epoch={}", state_index.epoch_id);
        self.lvmt_manager
            .get_state_no_commit(state_index, try_open, space)
    }

    fn get_state_for_genesis_write(self: &Arc<Self>) -> Box<dyn StateTrait> {
        self.lvmt_manager.get_state_for_genesis_write()
    }

    fn get_state_for_next_epoch(
        self: &Arc<Self>, parent_epoch_id: StateIndex,
        recover_mpt_during_construct_pivot_state: bool,
    ) -> Result<Option<Box<dyn StateTrait>>> {
        self.lvmt_manager.get_state_for_next_epoch(
            parent_epoch_id,
            recover_mpt_during_construct_pivot_state,
        )
    }
}

use crate::{
    impls::errors::*, state::*, state_manager::*, StorageConfiguration,
};
use cfx_types::Space;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use primitives::EpochId;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use storage2::LvmtStateManager;

use super::storage_manager::StorageManager;
