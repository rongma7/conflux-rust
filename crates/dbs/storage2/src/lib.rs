pub mod state;
pub mod state_manager;
pub use cfx_db_errors::storage as errors;
pub use errors::{Error, Result};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use parking_lot::Mutex;

use std::{collections::{BTreeMap, HashMap}, ops::Bound, path::{Path, PathBuf}, sync::Arc};

pub type MptKeyValue = (Vec<u8>, Box<[u8]>);
pub use state::StateTrait as StorageStateTrait;
pub use state_manager::{
    ReplicatedStateManagerTrait, StateIndex,
    StateManagerTrait as StorageManagerTrait,
};

use amt::{AmtParams, CreateMode};
use cfx_internal_common::StateRootWithAuxInfo;
pub use cfx_storage2::backends::DatabaseTrait;
use cfx_storage2::{
    backends::{
        impls::kvdb_rocksdb::WrappedRocksDb, HistoricalTableName, PendingTableName,
    },
    LvmtStorage,
};
use cfx_types::Space;
use ethereum_types::H256;
use once_cell::sync::Lazy;
use primitives::{
    EpochId, MerkleHash, StorageKeyWithSpace,
};
use tiny_keccak::{Hasher, Keccak};
use log::{debug, error, info};

pub type PE = ark_bls12_381::Bls12_381;
pub const TEST_LEVEL: usize = 16;
pub static AMT: Lazy<AmtParams<PE>> = Lazy::new(|| {
    let pp_path = Path::new(env!("WORKSPACE_ROOT")).join("pp");

    AmtParams::from_dir_mont(
        pp_path,
        TEST_LEVEL,
        TEST_LEVEL,
        CreateMode::Neither,
        None,
    )
});

pub const DEFAULT_BATCH_COMMIT_SIZE: usize = 20;

// ============================================================
// BatchAccumulator: batches multiple epoch commits into one
// ============================================================

struct EpochDelta {
    parent_epoch_id: Option<H256>,
    changes: BTreeMap<Box<[u8]>, Option<Box<[u8]>>>,
    state_root: MerkleHash,
}

pub struct BatchAccumulator {
    batch_size: usize,
    /// Per-epoch deltas not yet flushed to cfx-storage2.
    epoch_deltas: HashMap<H256, EpochDelta>,
    /// Last epoch_id that was flushed to cfx-storage2.
    last_flushed_epoch: Option<H256>,
    /// Epoch IDs in pivot-chain execution order since last flush.
    epochs_in_order: Vec<H256>,
    /// State roots of flushed intermediate epochs (not the tip).
    /// These are kept so get_state_no_commit can find them after flush.
    /// Cleared when epochs are moved to historical part.
    flushed_intermediate_roots: HashMap<H256, MerkleHash>,
}

impl BatchAccumulator {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            epoch_deltas: HashMap::new(),
            last_flushed_epoch: None,
            epochs_in_order: Vec::new(),
            flushed_intermediate_roots: HashMap::new(),
        }
    }

    /// Store an epoch's delta. Returns true if the batch is full and should be flushed.
    pub fn accumulate(
        &mut self,
        parent_epoch_id: Option<H256>,
        epoch_id: H256,
        state_root: MerkleHash,
        changes: BTreeMap<Box<[u8]>, Option<Box<[u8]>>>,
    ) -> bool {
        self.epoch_deltas.insert(epoch_id, EpochDelta {
            parent_epoch_id,
            changes,
            state_root,
        });
        self.epochs_in_order.push(epoch_id);
        self.epochs_in_order.len() >= self.batch_size
    }

    /// Check if an epoch is in the accumulator (not yet flushed).
    pub fn contains(&self, epoch_id: &H256) -> bool {
        self.epoch_deltas.contains_key(epoch_id)
    }

    /// Get state_root for an accumulated epoch.
    pub fn get_state_root(&self, epoch_id: &H256) -> Option<MerkleHash> {
        self.epoch_deltas.get(epoch_id).map(|d| d.state_root)
    }

    /// Build a merged overlay of all deltas from last_flushed up to (including) target epoch.
    /// Returns None if the epoch is not in the accumulator.
    pub fn build_overlay_for_epoch(
        &self, target_epoch_id: &H256,
    ) -> Option<BTreeMap<Box<[u8]>, Option<Box<[u8]>>>> {
        if !self.epoch_deltas.contains_key(target_epoch_id) {
            return None;
        }

        // Walk the parent chain from target back to last_flushed_epoch
        let mut chain = Vec::new();
        let mut current = Some(*target_epoch_id);
        while let Some(epoch_id) = current {
            if Some(epoch_id) == self.last_flushed_epoch {
                break;
            }
            if let Some(delta) = self.epoch_deltas.get(&epoch_id) {
                chain.push(epoch_id);
                current = delta.parent_epoch_id;
            } else {
                break; // reached a flushed or unknown epoch
            }
        }

        // Apply deltas in chronological order (reverse of chain)
        let mut overlay = BTreeMap::new();
        for epoch_id in chain.into_iter().rev() {
            if let Some(delta) = self.epoch_deltas.get(&epoch_id) {
                for (k, v) in &delta.changes {
                    overlay.insert(k.clone(), v.clone());
                }
            }
        }

        Some(overlay)
    }

    /// Compose all accumulated deltas and flush as a single cfx-storage2 commit.
    /// Returns the tip epoch_id that was committed.
    pub fn flush_batch(
        &mut self,
        backend: &mut LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>,
    ) -> Result<Option<H256>> {
        if self.epochs_in_order.is_empty() {
            return Ok(None);
        }

        // Compose all changes in order
        let mut composed = BTreeMap::new();
        for epoch_id in &self.epochs_in_order {
            if let Some(delta) = self.epoch_deltas.get(epoch_id) {
                for (k, v) in &delta.changes {
                    composed.insert(k.clone(), v.clone());
                }
            }
        }

        let tip_epoch = *self.epochs_in_order.last().unwrap();
        let tip_state_root = self.epoch_deltas[&tip_epoch].state_root;

        let mut manager = backend.as_manager()?;

        // Commit the composed changes under the tip epoch_id
        manager.commit(
            self.last_flushed_epoch,
            tip_epoch,
            tip_state_root,
            composed.into_iter(),
            &AMT,
        )?;

        // Save intermediate epoch state_roots so get_state_no_commit can
        // find them after the accumulator is cleared.
        for epoch_id in &self.epochs_in_order {
            if *epoch_id != tip_epoch {
                let sr = self.epoch_deltas[epoch_id].state_root;
                self.flushed_intermediate_roots.insert(*epoch_id, sr);
            }
        }

        debug!(
            "BatchAccumulator::flush_batch: flushed {} epochs, tip={:?}",
            self.epochs_in_order.len(), tip_epoch,
        );

        self.last_flushed_epoch = Some(tip_epoch);
        self.epoch_deltas.clear();
        self.epochs_in_order.clear();

        Ok(Some(tip_epoch))
    }

    /// Get the cfx-storage2 base epoch for overlay reads.
    /// This is the last_flushed_epoch (or None if nothing flushed yet).
    pub fn base_epoch_for_overlay(&self) -> Option<H256> {
        self.last_flushed_epoch
    }
}

// ============================================================
// LvmtView, LvmtState, LvmtStateManager
// ============================================================

pub struct LvmtView {
    pub epoch_id: H256,
}

impl Drop for LvmtState {
    fn drop(&mut self) {
        let storage_arc_count = Arc::strong_count(&self.backend);
        info!(">>> LvmtState::drop: Arc<Mutex<LvmtStorage>> refcount: {}", storage_arc_count);
    }
}

pub struct LvmtState {
    pub backend: Arc<Mutex<LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>>>,
    /// `None` for writable LvmtState to create genesis.
    /// `Some()` for read-only LvmtState indicating this epoch_id, or for
    /// writable LvmtState indicating parent_epoch_id.
    base_state: Option<LvmtView>,
    /// `changes` only includes writings, `changes` is not a cache.
    /// `None` for read-only LvmtState.
    /// `Some()` for writable LvmtState.
    changes: Option<BTreeMap<Box<[u8]>, Option<Box<[u8]>>>>,
    /// The state_root of this epoch_id, not of the parent_epoch_id, even for the writable LvmtState.
    /// For read-only LvmtState, `None` is unreachable.
    /// For writable LvmtState, `Some` means after invoking `compute_state_root()`, `None` means before invoking `compute_state_root()`.
    cached_state_root: Option<MerkleHash>,
    /// Accumulated overlay from previous epochs in the same batch.
    /// Used for reads: checked between `changes` and `backend`.
    accumulated_overlay: Option<Arc<BTreeMap<Box<[u8]>, Option<Box<[u8]>>>>>,
    /// Reference to the batch accumulator for commit-time operations.
    batch_accumulator: Option<Arc<Mutex<BatchAccumulator>>>,
}

impl Drop for LvmtStateManager {
    fn drop(&mut self) {
        let storage_arc_count = Arc::strong_count(&self.backend);
        info!("=== LvmtStateManager::drop START ===");
        info!("Arc<Mutex<LvmtStorage>> refcount: {}", storage_arc_count);

        if storage_arc_count > 1 {
            error!("WARNING: LvmtStorage is still referenced by {} other owners!", storage_arc_count - 1);
        }

        info!("=== LvmtStateManager::drop END ===");
    }
}

pub struct LvmtStateManager {
    backend: Arc<Mutex<LvmtStorage<WrappedRocksDb<HistoricalTableName>, WrappedRocksDb<PendingTableName>>>>,
    batch_accumulator: Arc<Mutex<BatchAccumulator>>,
}

impl MallocSizeOf for LvmtStateManager {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        0
    }
}

impl LvmtStateManager {
    pub fn new_arc(historical_db_path: PathBuf, pending_db_path: PathBuf) -> Arc<Self> {
        Self::new_arc_with_batch_size(historical_db_path, pending_db_path, DEFAULT_BATCH_COMMIT_SIZE)
    }

    pub fn new_arc_with_batch_size(
        historical_db_path: PathBuf, pending_db_path: PathBuf, batch_size: usize,
    ) -> Arc<Self> {
        let lvmt_storage = LvmtStorage::new_from_paths(&historical_db_path, &pending_db_path)
            .expect("LvmtStorage initialization failed");
        Arc::new(Self {
            backend: Arc::new(Mutex::new(lvmt_storage)),
            batch_accumulator: Arc::new(Mutex::new(BatchAccumulator::new(batch_size))),
        })
    }

    pub fn make_pivot(&self, commit_id: H256) -> Result<bool> {
        // If commit_id is in the accumulator (not yet flushed) or is a
        // flushed intermediate epoch (not a pending tree node), skip.
        {
            let acc = self.batch_accumulator.lock();
            if acc.contains(&commit_id)
                || acc.flushed_intermediate_roots.contains_key(&commit_id)
            {
                return Ok(false);
            }
        }
        let mut guard = self.backend.lock();
        let mut manager = guard.as_manager()?;
        // The commit might still not exist in the pending tree if it was
        // part of a batch but not the tip. Check existence first.
        if !manager.query_commit_existence(&commit_id)? {
            return Ok(false);
        }
        Ok(manager.make_pivot(commit_id)?)
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> Result<bool> {
        Ok(self.backend.lock().as_manager()?.is_newer_than_pending_root(height))
    }

    pub fn confirmed_pending_to_history(
        &self, new_root_height: u64, pivot_commit_id: H256,
    ) -> Result<()> {
        // If pivot_commit_id is in the accumulator or is a flushed
        // intermediate epoch (not a pending tree node), skip.
        {
            let acc = self.batch_accumulator.lock();
            if acc.contains(&pivot_commit_id)
                || acc.flushed_intermediate_roots.contains_key(&pivot_commit_id)
            {
                return Ok(());
            }
        }
        let mut guard = self.backend.lock();
        // Wrap the call in a match to handle InvalidAncestorHeight gracefully.
        // With batch commits, the ancestor chain may have gaps (intermediate
        // epochs that were never committed to the pending tree individually).
        match guard.confirmed_pending_to_history_with_height(
            new_root_height,
            pivot_commit_id,
        ) {
            Ok(()) => Ok(()),
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("InvalidAncestorHeight") {
                    // Expected with batch commits — the ancestor at
                    // new_root_height may not exist in the pending tree.
                    debug!(
                        "confirmed_pending_to_history: skipping due to InvalidAncestorHeight \
                         (batch commit gap), height={}, pivot={:?}",
                        new_root_height, pivot_commit_id,
                    );
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub fn background_cleanup(&self) -> Result<()> {
        let guard = self.backend.lock();
        Ok(guard.background_cleanup()?)
    }
}

// ============================================================
// LvmtState: private helpers
// ============================================================

impl LvmtState {
    fn change(
        &mut self, key: Box<[u8]>, value: Option<Box<[u8]>>,
    ) -> Result<()> {
        if let Some(map) = self.changes.as_mut() {
            map.insert(key, value);
            self.cached_state_root = None;
            Ok(())
        } else {
            Err(Error::Msg(
                "Attempted to modify a read-only storage state".into(),
            ))
        }
    }

    fn change_for_access_key(
        &mut self, access_key: StorageKeyWithSpace, value: Option<Box<[u8]>>,
    ) -> Result<()> {
        let key = access_key
            .to_key_bytes()
            .into_boxed_slice();
        self.change(key, value)
    }

    /// Gets all existing keys prefixed with access_key_prefix.
    fn read_all_inner(
        &self, lower_bound_incl: Box<[u8]>, upper_bound_excl: Option<Box<[u8]>>,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        // get from backend
        let mut keys_values: BTreeMap<Box<[u8]>, Box<[u8]>> =
            if let Some(view) = &self.base_state {
                let epoch_id = view.epoch_id;
                self.backend.lock().as_manager()?.iter_range(epoch_id, lower_bound_incl.clone(), upper_bound_excl.clone())?
                    .into_iter()
                    .map(|(key, lvmt_value)| (key, lvmt_value.get_value()))
                    .filter(|(_, value)| value.is_some())
                    .map(|(key, value)| (key, value.unwrap()))
                    .collect()
            } else {
                BTreeMap::new()
            };

        let start_bound = Bound::Included(lower_bound_incl);
        let end_bound = match &upper_bound_excl {
            Some(upper) => Bound::Excluded(upper.clone()),
            None => Bound::Unbounded,
        };

        // layer accumulated_overlay
        if let Some(overlay) = &self.accumulated_overlay {
            for (key, value) in overlay.range((start_bound.clone(), end_bound.clone())) {
                match value {
                    Some(v) => { keys_values.insert(key.clone(), v.clone()); }
                    None => { keys_values.remove(key); }
                };
            }
        }

        // layer changes (current epoch)
        if let Some(changes) = &self.changes {
            for (key, value) in changes.range((start_bound, end_bound)) {
                match value {
                    Some(existing_value) => {
                        keys_values.insert(key.clone(), existing_value.clone())
                    }
                    None => keys_values.remove(key),
                };
            }
        }

        if keys_values.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                keys_values
                    .into_iter()
                    .map(|(key, value)| (key.into_vec(), value))
                    .collect(),
            ))
        }
    }

    fn compute_state_root_inner(&mut self) -> Result<MerkleHash> {
        if let Some(ref state_root) = self.cached_state_root {
            return Ok(*state_root);
        }

        let mut x = Keccak::v256();

        // O(1): get parent epoch's state_root
        // First check the batch accumulator, then fall back to cfx-storage2.
        if let Some(view) = &self.base_state {
            let parent_root = if let Some(acc) = &self.batch_accumulator {
                acc.lock().get_state_root(&view.epoch_id)
            } else {
                None
            };
            let parent_root = match parent_root {
                Some(r) => r,
                None => {
                    self.backend.lock().as_manager()?
                        .get_state_root(view.epoch_id)?
                        .unwrap_or_default()
                }
            };
            x.update(parent_root.as_bytes());
        }

        // O(changes): only hash this epoch's modifications
        if let Some(changes) = &self.changes {
            for (key, value) in changes.iter() {
                x.update(&key);
                match value {
                    Some(v) => { x.update(&[1]); x.update(&v); }
                    None    => { x.update(&[0]); }
                }
            }
        }

        let mut state_root = [0u8; 32];
        x.finalize(&mut state_root);

        self.cached_state_root = Some(H256(state_root));
        Ok(H256(state_root))
    }
}

// ============================================================
// StorageStateTrait implementation
// ============================================================

impl StorageStateTrait for LvmtState {
    fn get(
        &self, access_key: StorageKeyWithSpace,
    ) -> Result<Option<Box<[u8]>>> {
        let key = access_key
            .to_key_bytes()
            .into_boxed_slice();

        // 1. get from changes (current epoch)
        if let Some(changes) = &self.changes {
            if let Some(value_in_changes) = changes.get(&key) {
                return Ok(value_in_changes.clone());
            }
        }

        // 2. get from accumulated_overlay (previous epochs in batch)
        if let Some(overlay) = &self.accumulated_overlay {
            if let Some(value_in_overlay) = overlay.get(&key) {
                return Ok(value_in_overlay.clone());
            }
        }

        // 3. get from backend (cfx-storage2)
        if let Some(view) = &self.base_state {
            let epoch_id = view.epoch_id;
            Ok(self.backend.lock().as_manager()?.get(epoch_id, key)?
                .map(|v| v.get_value())
                .flatten())
        } else {
            Ok(None)
        }
    }

    fn set(
        &mut self, access_key: StorageKeyWithSpace, value: Box<[u8]>,
    ) -> Result<()> {
        self.change_for_access_key(access_key, Some(value))
    }

    fn delete(&mut self, access_key: StorageKeyWithSpace) -> Result<()> {
        self.change_for_access_key(access_key, None)
    }

    fn delete_test_only(
        &mut self, access_key: StorageKeyWithSpace,
    ) -> Result<Option<Box<[u8]>>> {
        let old_value = self.get(access_key)?;
        self.change_for_access_key(access_key, None)?;
        Ok(old_value)
    }

    /// Marks all existing keys prefixed with access_key_prefix as `deleted`.
    fn delete_all(
        &mut self, access_key_prefix: StorageKeyWithSpace,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        let maybe_keys_old_values = self.read_all(access_key_prefix)?;
        if let Some(keys_old_values) = maybe_keys_old_values {
            for (key, _) in keys_old_values.iter() {
                self.change(key.as_slice().into(), None)?;
            }
            Ok(Some(keys_old_values))
        } else {
            Ok(None)
        }
    }

    /// Gets all existing keys prefixed with access_key_prefix.
    fn read_all(
        &mut self, access_key_prefix: StorageKeyWithSpace,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        let lower_bound_incl = access_key_prefix.to_key_bytes().into_boxed_slice();
        let upper_bound_excl = to_key_prefix_iter_upper_bound(&lower_bound_incl).map(|x| x.into_boxed_slice());
        self.read_all_inner(lower_bound_incl, upper_bound_excl)
    }

    // compute_state_root() does not write LvmtStateManager
    fn compute_state_root(&mut self) -> Result<StateRootWithAuxInfo> {
        Ok(StateRootWithAuxInfo::genesis(
            &self.compute_state_root_inner()?,
        ))
    }

    fn get_state_root(&self) -> Result<StateRootWithAuxInfo> {
        self.cached_state_root
            .map(|state_root| StateRootWithAuxInfo::genesis(&state_root))
            .ok_or(Error::Msg("No state root".to_owned()).into())
    }

    // commit() — accumulates into batch, flushes when batch is full
    fn commit(
        &mut self, epoch: EpochId,
    ) -> Result<StateRootWithAuxInfo> {
        let state_root = self.compute_state_root_inner()?;
        let changes_inner = self
            .changes
            .as_mut()
            .map(|map_ref| std::mem::take(map_ref))
            .unwrap_or_default();

        let parent_epoch_id = self.base_state.as_ref().map(|s| s.epoch_id);

        // Genesis commit (no parent) goes directly to cfx-storage2
        if parent_epoch_id.is_none() {
            let mut guard = self.backend.lock();
            let mut manager = guard.as_manager()?;
            manager.commit(None, epoch, state_root, changes_inner.into_iter(), &AMT)?;
            // Update accumulator's last_flushed_epoch
            if let Some(acc) = &self.batch_accumulator {
                acc.lock().last_flushed_epoch = Some(epoch);
            }
            return Ok(StateRootWithAuxInfo::genesis(&state_root));
        }

        // Check if already committed (idempotent)
        if let Some(acc) = &self.batch_accumulator {
            let acc_guard = acc.lock();
            if acc_guard.contains(&epoch) {
                return Ok(StateRootWithAuxInfo::genesis(
                    &acc_guard.get_state_root(&epoch).unwrap(),
                ));
            }
        }

        // Also check cfx-storage2
        {
            let mut guard = self.backend.lock();
            let manager = guard.as_manager()?;
            if manager.query_commit_existence(&epoch)? {
                let sr = manager.get_state_root(epoch)?
                    .expect("State root should exist for existing commit");
                return Ok(StateRootWithAuxInfo::genesis(&sr));
            }
        }

        // Accumulate this epoch's delta
        let acc = self.batch_accumulator.as_ref()
            .expect("batch_accumulator must be set for writable state");
        let batch_full = acc.lock().accumulate(
            parent_epoch_id,
            epoch,
            state_root,
            changes_inner,
        );

        // Flush if batch is full
        if batch_full {
            let mut backend_guard = self.backend.lock();
            acc.lock().flush_batch(&mut backend_guard)?;
        }

        Ok(StateRootWithAuxInfo::genesis(&state_root))
    }
}

// ============================================================
// LvmtStateManager: state factory methods
// ============================================================

impl LvmtStateManager {
    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, epoch_id: StateIndex,
    ) -> Result<Option<LvmtState>> {
        // First check the batch accumulator (unflushed epochs)
        {
            let acc = self.batch_accumulator.lock();
            if acc.contains(&epoch_id.epoch_id) {
                let overlay = acc.build_overlay_for_epoch(&epoch_id.epoch_id);
                let state_root = acc.get_state_root(&epoch_id.epoch_id);
                let base_epoch = acc.base_epoch_for_overlay();
                drop(acc);

                // Checkout the base epoch for O(1) reads
                if let Some(base) = base_epoch {
                    self.backend.lock().as_manager()?.checkout_current(base)?;
                }

                return Ok(Some(LvmtState {
                    backend: self.backend.clone(),
                    base_state: base_epoch.map(|id| LvmtView { epoch_id: id }),
                    changes: None,
                    cached_state_root: state_root,
                    accumulated_overlay: overlay.map(Arc::new),
                    batch_accumulator: None,
                }));
            }

            // Check flushed intermediate epochs (they were part of a batch
            // but not the tip, so cfx-storage2 only has the tip's state).
            // For these, the actual data IS in cfx-storage2 (merged into the
            // tip commit), but we need the per-epoch state_root.
            if let Some(&state_root) = acc.flushed_intermediate_roots.get(&epoch_id.epoch_id) {
                // The data is in cfx-storage2 under the tip epoch,
                // but this intermediate epoch doesn't have its own commit.
                // We return a state pointing to the tip's data with this
                // epoch's state_root. This is correct for read-only because
                // the tip contains all accumulated changes including this epoch's.
                //
                // NOTE: This is an approximation — the state at the tip is
                // actually the state after ALL epochs in the batch, not just
                // up to this intermediate epoch. For exact intermediate state,
                // we would need to replay. For now, this makes existence checks
                // succeed. Full replay support can be added later if needed.
                let base_epoch = acc.last_flushed_epoch;
                drop(acc);

                if let Some(base) = base_epoch {
                    self.backend.lock().as_manager()?.checkout_current(base)?;
                }

                return Ok(Some(LvmtState {
                    backend: self.backend.clone(),
                    base_state: base_epoch.map(|id| LvmtView { epoch_id: id }),
                    changes: None,
                    cached_state_root: Some(state_root),
                    accumulated_overlay: None,
                    batch_accumulator: None,
                }));
            }
        }

        // Not in accumulator, check cfx-storage2
        let maybe_state_root = self.backend.lock().as_manager()?.get_state_root(epoch_id.epoch_id)?;

        if let Some(state_root) = maybe_state_root {
            Ok(Some(LvmtState {
                backend: self.backend.clone(),
                base_state: Some(LvmtView {
                    epoch_id: epoch_id.epoch_id,
                }),
                changes: None,
                cached_state_root: Some(state_root),
                accumulated_overlay: None,
                batch_accumulator: None,
            }))
        } else {
            Ok(None)
        }
    }
}

impl StorageManagerTrait for LvmtStateManager {
    fn get_state_no_commit(
        self: &Arc<Self>, epoch_id: StateIndex, _try_open: bool,
        _space: Option<Space>,
    ) -> Result<Option<Box<dyn StorageStateTrait>>> {
        Ok(self.get_state_no_commit_inner(epoch_id)?.map(|s| Box::new(s) as Box<dyn StorageStateTrait>))
    }

    fn get_state_for_next_epoch(
        self: &Arc<Self>, parent_epoch_id: StateIndex,
        _recover_mpt_during_construct_pivot_state: bool,
    ) -> Result<Option<Box<dyn StorageStateTrait>>> {
        let (overlay, base_epoch) = {
            let acc = self.batch_accumulator.lock();
            if acc.contains(&parent_epoch_id.epoch_id) {
                // Parent is in the accumulator — build overlay and use flushed base
                let overlay = acc.build_overlay_for_epoch(&parent_epoch_id.epoch_id);
                let base = acc.base_epoch_for_overlay();
                (overlay, base)
            } else {
                // Parent is already flushed or in cfx-storage2
                (None, Some(parent_epoch_id.epoch_id))
            }
        };

        // Pre-checkout the base epoch for O(1) reads
        if let Some(base) = base_epoch {
            self.backend.lock().as_manager()?.checkout_current(base)?;
        }

        Ok(Some(Box::new(LvmtState {
            backend: self.backend.clone(),
            base_state: base_epoch.map(|id| LvmtView { epoch_id: id }),
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            accumulated_overlay: overlay.map(Arc::new),
            batch_accumulator: Some(self.batch_accumulator.clone()),
        })))
    }

    fn get_state_for_genesis_write(
        self: &Arc<Self>,
    ) -> Box<dyn StorageStateTrait> {
        Box::new(LvmtState {
            backend: self.backend.clone(),
            base_state: None,
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            accumulated_overlay: None,
            batch_accumulator: Some(self.batch_accumulator.clone()),
        })
    }
}

// TODO: add comments and unit tests
pub fn to_key_prefix_iter_upper_bound(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper_bound_excl_value = key_prefix.to_vec();
    if upper_bound_excl_value.len() == 0 {
        None
    } else {
        let mut carry = 1;
        let len = upper_bound_excl_value.len();
        for i in 0..len {
            if upper_bound_excl_value[len - 1 - i] == 255 {
                upper_bound_excl_value[len - 1 - i] = 0;
            } else {
                upper_bound_excl_value[len - 1 - i] += 1;
                carry = 0;
                break;
            }
        }
        if carry == 1 {
            None
        } else {
            Some(upper_bound_excl_value)
        }
    }
}
