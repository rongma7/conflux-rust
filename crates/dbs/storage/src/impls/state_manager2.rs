// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

impl Drop for LvmtStateManagerWithConf {
    fn drop(&mut self) {
        let arc_count = Arc::strong_count(&self.lvmt_manager);
        info!("=== LvmtStateManagerWithConf::drop START ===");
        info!("Arc<LvmtStateManager> refcount: {}", arc_count);
        
        // 如果 arc_count > 1，说明还有其他地方持有引用
        if arc_count > 1 {
            error!("WARNING: LvmtStateManager is still referenced by {} other owners!", arc_count - 1);
        }
        
        info!("Attempting to acquire write lock on shutdown_lock...");
        let mut shutdown_guard = self.shutdown_lock.write();
        info!("Write lock acquired!");
        
        *shutdown_guard = true;
        
        let final_arc_count = Arc::strong_count(&self.lvmt_manager);
        info!("=== LvmtStateManagerWithConf::drop END ===");
        info!("Arc<LvmtStateManager> final refcount: {}", final_arc_count);
        
        // 如果 final_arc_count > 1，LvmtStateManager 不会被 drop！
        if final_arc_count > 1 {
            error!("CRITICAL: LvmtStateManager will NOT be dropped! Still {} references!", final_arc_count - 1);
        }
    }
}

pub struct WrappedLvmtState(pub LvmtState);

// impl Drop for State {
//     fn drop(&mut self) {
//         if self.dirty {
//             panic!("State is dirty however is not committed before free.");
//         }
//     }
// }
// impl Drop for WrappedLvmtState {
//     fn drop(&mut self) {
//         todo!()
//     }
// }

impl Deref for WrappedLvmtState {
    type Target = LvmtState;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WrappedLvmtState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl StateDbGetOriginalMethods for WrappedLvmtState {
    fn get_original_raw_with_proof(
        &self, _key: primitives::StorageKeyWithSpace,
    ) -> Result<(Option<Box<[u8]>>, crate::StateProof)> {
        unimplemented!()
    }

    fn get_original_storage_root(
        &self, _address: &cfx_types::AddressWithSpace,
    ) -> Result<primitives::StorageRoot> {
        unimplemented!()
    }

    fn get_original_storage_root_with_proof(
        &self, _address: &cfx_types::AddressWithSpace,
    ) -> Result<(primitives::StorageRoot, crate::StorageRootProof)> {
        unimplemented!()
    }
}

pub struct LvmtStateManagerWithConf {
    lvmt_manager: Arc<LvmtStateManager>,
    pub storage_conf: StorageConfiguration,
    // used during startup for the next compute epoch
    pub intermediate_trie_root_merkle: RwLock<Option<MerkleHash>>,
    pub persist_state_from_initialization:
        RwLock<Option<(Option<EpochId>, HashSet<EpochId>, u64, Option<u64>)>>,

    // ------------------------------------------------------------------
    // Locking strategy (two-level lock):
    // ------------------------------------------------------------------
    // 1. shutdown_lock (RwLock<bool>):
    //    - Read lock: Acquired during normal maintenance operations
    //    - Write lock: Acquired during shutdown to block new operations
    //    - Purpose: Coordinate shutdown with ongoing operations
    //
    // 2. maintenance_lock (Mutex<()>):
    //    - Purpose: Serialize all concurrent maintenance operations
    //    - Ensures only one thread modifies state at a time
    //
    // Lock ordering (to prevent deadlock):
    //    Always acquire shutdown_lock BEFORE maintenance_lock
    //
    // Shutdown safety:
    //    - New operations check shutdown_lock first (try_read)
    //    - Shutdown acquires write lock, blocking until all operations complete
    //    - After shutdown, try_read() succeeds but returns early (shutdown == true)
    // ------------------------------------------------------------------
    shutdown_lock: RwLock<bool>,
    maintenance_lock: Mutex<()>,
}

impl LvmtStateManagerWithConf {
    pub fn new_arc(storage_conf: StorageConfiguration) -> Arc<Self> {
        let lvmt_manager = LvmtStateManager::new_arc(
            storage_conf.path_storage_dir.join("lvmt_historical"),
            storage_conf.path_storage_dir.join("lvmt_pending"),
        );
        Arc::new(Self {
            lvmt_manager,
            storage_conf,
            intermediate_trie_root_merkle: RwLock::new(None),
            persist_state_from_initialization: RwLock::new(None),
            shutdown_lock: RwLock::new(false),
            maintenance_lock: Mutex::new(()),
        })
    }
}

// impl Drop for LvmtStateManagerWithConf {
//     fn drop(&mut self) {
//         info!("LvmtStateManagerWithConf: starting graceful shutdown");
        
//         // ------------------------------------------------------------------
//         // Acquire write lock on shutdown_lock
//         // ------------------------------------------------------------------
//         // This will block until all ongoing maintenance operations (holding read locks)
//         // complete. Once acquired, no new maintenance operations can start.
//         let mut shutdown_guard = self.shutdown_lock.write();
//         *shutdown_guard = true;
        
//         info!("LvmtStateManagerWithConf: all maintenance operations completed");

//         // Important: The lvmt_manager (Arc<LvmtStateManager>) will be dropped here,
//         // but only after all maintenance operations have completed.
//         // The shutdown_guard ensures no new operations can start.
//     }
// }

// Methods for LvmtStateManagerWithConf as a peer of StorageManager
impl LvmtStateManagerWithConf {
    pub fn get_snapshot_epoch_count(&self) -> u32 {
        self.storage_conf.consensus_param.snapshot_epoch_count
    }

    // The input parameter `state_availability_boundary` may be modified in this
    // function. The `state_availability_boundary.lower_bound` refers to the
    // maximum height that has no slibling from this moment on; in storage2,
    // the pending tree root is at that height. This function make
    // `maintained_state_height_lower_bound` has no slibling at this moment.
    // Temporarily, the computations of
    // `state_availability_boundary.lower_bound` and
    // `maintained_state_height_lower_bound` remain the same as those of
    // storage1. non-pivot to remove: all heights
    // old-pivot to remove: height < confirmed_snapshot_height, and
    // !extra_snapshots_to_keep (todo) but since we will use
    // `first_available_state_height` as the new root, we actually
    // remove (i.e., move from pending part to historical part) old-pivot:
    // height < first_available_state_height.
    pub fn maintain_state_confirmed<ConsensusInner: StateMaintenanceTrait>(
        &self, consensus_inner: &ConsensusInner,
        _stable_checkpoint_height: u64, _era_epoch_count: u64,
        confirmed_height: u64,
        state_availability_boundary: &RwLock<StateAvailabilityBoundary>,
    ) -> Result<()> {
        let shutdown_guard = match self.shutdown_lock.try_read() {
            Some(guard) => guard,
            None => return Ok(()),
        };

        if *shutdown_guard {
            return Ok(());
        }

        // ------------------------------------------------------------------
        // This serializes all concurrent calls to this function.
        // ------------------------------------------------------------------
        // Once a thread acquires the lock, any other threads attempting to call this
        // function will block here. The `_` in `_guard` signifies that the
        // variable is intentionally unused; its lifetime is what's critical. The lock
        // is automatically released when `_guard` goes out of scope (RAII).
        //
        // No deadlock risk: This lock is private and internal to this module. No
        // other code can lock `state_availability_boundary` or `lvmt_manager` first
        // and then attempt to acquire `maintenance_lock`, thus preventing a circular wait.
        let _guard = self.maintenance_lock.lock();

        // Both locks held; safe to proceed

        // compute `maintained_state_height_lower_bound`
        let additional_state_height_gap =
            (self.storage_conf.additional_maintained_snapshot_count
                * self.get_snapshot_epoch_count()) as u64;
        let maintained_state_height_lower_bound =
            if confirmed_height > additional_state_height_gap {
                confirmed_height - additional_state_height_gap
            } else {
                0
            };
        if maintained_state_height_lower_bound
            <= state_availability_boundary.read().lower_bound
        {
            return Ok(());
        }
        let maintained_epoch_id = consensus_inner
            .get_pivot_hash_from_epoch_number(
                maintained_state_height_lower_bound,
            )?;

        // compute the new `state_availability_boundary.lower_bound`
        let confirmed_intermediate_height = maintained_state_height_lower_bound
            - StateIndex::height_to_delta_height(
                maintained_state_height_lower_bound,
                self.get_snapshot_epoch_count(),
            ) as u64;

        let confirmed_snapshot_height = if confirmed_intermediate_height
            > self.get_snapshot_epoch_count() as u64
        {
            confirmed_intermediate_height
                - self.get_snapshot_epoch_count() as u64
        } else {
            0
        };
        let first_available_state_height = if confirmed_snapshot_height > 0 {
            confirmed_snapshot_height + 1
        } else {
            0
        };

        let non_pivot_removed =
            self.lvmt_manager.make_pivot(maintained_epoch_id)?;
        let adjust_pending_root = self
            .lvmt_manager
            .is_newer_than_pending_root(first_available_state_height)?;
        if non_pivot_removed || adjust_pending_root {
            {
                // TODO: Archive node may do something different.
                let state_boundary = &mut *state_availability_boundary.write();
                if first_available_state_height > state_boundary.lower_bound {
                    state_boundary
                        .adjust_lower_bound(first_available_state_height);
                }
            }

            // change pending root to be the new
            // `state_availability_boundary.lower_bound`
            if adjust_pending_root {
                self.lvmt_manager.confirmed_pending_to_history(
                    first_available_state_height,
                    maintained_epoch_id,
                )?;
            }
        }

        info!("maintain_state_confirmed: finished");

        // TODO: background_cleanup. Put the above codes in a code block first.
        // let storage_clone_for_cleanup = self.clone();
        // task::spawn(async move {
        //     log!("[Cleanup Task] Started background cleanup.");

        //     // storage_clone_for_cleanup.cleanup_old_files().await;

        //     info!("[Cleanup Task] Background cleanup finished.");
        // });

        // info!("[Main Task] Function returning immediately, cleanup is running in background.");
        
        Ok(())
    }

    pub fn get_snapshot_manager(
        &self,
    ) -> &(dyn SnapshotManagerTrait<
        SnapshotDb = SnapshotDb,
        SnapshotDbManager = SnapshotDbManager,
    > + Send
             + Sync) {
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

        let lvmt_manager = LvmtStateManagerWithConf::new_arc(conf);

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

    pub fn config(&self) -> &StorageConfiguration {
        &self.lvmt_manager.storage_conf
    }

    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, state_index: StateIndex, _try_open: bool,
        _open_mpt_snapshot: bool,
    ) -> Result<Option<WrappedLvmtState>> {
        Ok(self.lvmt_manager.lvmt_manager.get_state_no_commit_inner(state_index)?.map(|s| WrappedLvmtState(s)))
    }
}

impl StateManagerTrait for StateManager2 {
    fn get_state_no_commit(
        self: &Arc<Self>, state_index: StateIndex, try_open: bool,
        space: Option<Space>,
    ) -> Result<Option<Box<dyn StateTrait>>> {
        debug!("read state from lvmt state: epoch={}", state_index.epoch_id);
        self.lvmt_manager.lvmt_manager.get_state_no_commit(
            state_index,
            try_open,
            space,
        )
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
    impls::errors::*, snapshot_manager::SnapshotManagerTrait, state::*,
    state_manager::*, storage_db::SnapshotInfo, StorageConfiguration,
};
use cfx_internal_common::{
    consensus_api::StateMaintenanceTrait, StateAvailabilityBoundary,
};
use cfx_types::Space;
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use parking_lot::{Mutex, RwLock};
use primitives::{EpochId, MerkleHash};
use std::{
    collections::HashSet, ops::{Deref, DerefMut}, sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    }
};
use storage2::{LvmtState, LvmtStateManager};

use super::{
    state_manager::{SnapshotDb, SnapshotDbManager},
    storage_manager::StorageManager,
};
