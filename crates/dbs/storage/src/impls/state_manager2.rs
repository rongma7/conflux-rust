// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/
pub struct StateManager2 {
    lvmt_manager: Arc<LvmtStateManager>,
    pub number_committed_nodes: AtomicUsize,
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
            number_committed_nodes: Default::default(),
        })
    }

    pub fn log_usage(&self) {
        debug!(
            "number of nodes committed to db {}",
            self.number_committed_nodes.load(Ordering::Relaxed),
        );
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
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use storage2::LvmtStateManager;
