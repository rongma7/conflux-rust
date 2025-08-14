#[macro_use]
extern crate log;

pub mod state;
pub mod state_manager;
pub use cfx_db_errors::storage as errors;
pub use errors::{Error, Result};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use parking_lot::Mutex;

use std::{borrow::Cow, collections::BTreeMap, fs, path::{Path, PathBuf}, sync::Arc};

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
        impls::kvdb_rocksdb::open_database, TableName, TableReader,
        TableSchema, WriteSchemaTrait,
    },
    LvmtStorage, LvmtStore,
};
use cfx_types::Space;
use ethereum_types::H256;
use once_cell::sync::Lazy;
use primitives::{
    DeltaMptKeyPadding, EpochId, MerkleHash, StorageKeyWithSpace,
    MERKLE_NULL_NODE,
};
use tiny_keccak::{Hasher, Keccak};

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
pub type Database = kvdb_rocksdb::Database;

#[derive(Clone, Copy)]
pub struct StateRootTable;

impl TableSchema for StateRootTable {
    type Key = H256;
    type Value = MerkleHash;

    const NAME: TableName = TableName::StateRoot;
}

pub struct LvmtDatabase {
    storage: Arc<Mutex<LvmtStorage<Database>>>,
}

impl LvmtDatabase {
    pub fn new(storage: Arc<Mutex<LvmtStorage<Database>>>) -> Result<Self> {
        Ok(Self {
            storage: storage.clone(),
        })
    }

    pub fn with_data<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&LvmtStore) -> Result<R> {
        let storage_guard = self.storage.lock();
        let data = storage_guard.as_manager().map_err(|_| {
            Error::Msg("Err in LvmtStorage::as_manager()".into())
        })?;

        let result = f(&data)?;

        drop(data);

        Ok(result)
    }

    pub fn with_state_roots<F, R>(&self, f: F) -> Result<R>
    where F: FnOnce(&TableReader<'_, StateRootTable>) -> Result<R> {
        let state_roots = Arc::new(
            self.storage
                .lock()
                .get_backend()
                .view::<StateRootTable>()
                .map_err(|_| {
                    Error::Msg("Err in get view of StateRootTable".into())
                })?,
        );

        let state_roots_reader: TableReader<'_, StateRootTable> = state_roots;
        let result = f(&state_roots_reader)?;

        drop(state_roots_reader);

        Ok(result)
    }

    pub fn commit(
        &self, write_schema: <Database as DatabaseTrait>::WriteSchema,
    ) -> Result<()> {
        let mut storage_guard = self.storage.lock();
        storage_guard
            .commit(write_schema)
            .map_err(|_| Error::Msg("Err in commit write_schema to db".into()))
    }

    pub fn make_pivot(&self, commit_id: H256) -> Result<bool> {
        let storage_guard = self.storage.lock();
        storage_guard
            .make_pivot(commit_id)
            .map_err(|_| Error::Msg("Err in make_pivot".into()))
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> bool {
        let storage_guard = self.storage.lock();
        storage_guard.is_newer_than_pending_root(height)
    }

    pub fn confirmed_pending_to_history(
        &self, new_root_height: u64, pivot_commit_id: H256,
        write_schema: &<Database as DatabaseTrait>::WriteSchema,
    ) -> Result<()> {
        let storage_guard = self.storage.lock();
        storage_guard
            .confirmed_pending_to_history_with_height(
                new_root_height,
                pivot_commit_id,
                write_schema,
            )
            .map_err(|_| {
                Error::Msg("Err in confirmed_pending_to_history".into())
            })
    }
}

pub struct LvmtView {
    // pub state: LvmtSnapshot<'static>,
    pub epoch_id: H256,
}

pub struct LvmtState {
    manager: Arc<LvmtStateManager>,
    /// `None` for writable LvmtState to create genesis.
    /// `Some()` for read-only LvmtState indicating this epoch_id, or for
    /// writable LvmtState indicating parent_epoch_id.
    base_state: Option<LvmtView>,
    /// `changes` only includes writings, `changes` is not a cache.
    /// `None` for read-only LvmtState.
    /// `Some()` for writable LvmtState.
    changes: Option<BTreeMap<Box<[u8]>, Option<Box<[u8]>>>>,
    /// `Some()` only for writable LvmtState after invoking
    /// compute_state_root().
    cached_state_root: Option<MerkleHash>,
    /// Information obtained from `StateIndex`.
    delta_trie_key_padding: DeltaMptKeyPadding,
}

pub struct LvmtStateManager {
    backend: Arc<LvmtDatabase>,
}

impl MallocSizeOf for LvmtStateManager {
    fn size_of(&self, _ops: &mut MallocSizeOfOps) -> usize {
        let size = 0;
        size
    }
}

impl LvmtStateManager {
    pub fn new_arc(db_path: PathBuf) -> Arc<Self> {
        if !db_path.exists() {
            fs::create_dir_all(&db_path).expect("db path create error");
        }

        let db = open_database(
            TableName::max_index() + 1,
            db_path.to_str().expect("Path contains invalid UTF-8"),
        )
        .expect("LvmtDb initialize error");
        let lvmt_storage = LvmtStorage::new(Arc::new(db))
            .expect("LvmtStorage initialize error");
        let backend = Arc::new(
            LvmtDatabase::new(Arc::new(Mutex::new(lvmt_storage)))
                .expect("LvmtDatabase initialize error"),
        );
        Arc::new(Self { backend })
    }

    pub fn commit(
        &self, write_schema: <Database as DatabaseTrait>::WriteSchema,
    ) -> Result<()> {
        self.backend.commit(write_schema)
    }

    pub fn make_pivot(&self, commit_id: H256) -> Result<bool> {
        self.backend.make_pivot(commit_id)
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> bool {
        self.backend.is_newer_than_pending_root(height)
    }

    pub fn confirmed_pending_to_history(
        &self, new_root_height: u64, pivot_commit_id: H256,
        write_schema: &<Database as DatabaseTrait>::WriteSchema,
    ) -> Result<()> {
        self.backend.confirmed_pending_to_history(
            new_root_height,
            pivot_commit_id,
            write_schema,
        )
    }
}

impl LvmtState {
    fn change(
        &mut self, key: Box<[u8]>, value: Option<Box<[u8]>>,
    ) -> Result<()> {
        if let Some(map) = self.changes.as_mut() {
            map.insert(key, value);
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
            .to_delta_mpt_key_bytes(&self.delta_trie_key_padding)
            .into_boxed_slice();
        self.change(key, value)
    }

    /// Gets all existing keys prefixed with access_key_prefix.
    fn read_all_inner(
        &self, key_prefix: Box<[u8]>,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        // get from backend
        let mut keys_values: BTreeMap<Box<[u8]>, Box<[u8]>> =
            if let Some(view) = &self.base_state {
                let epoch_id = view.epoch_id;
                self.manager.backend.with_data(|data| {
                    Ok(data
                        .iter_prefix(epoch_id, key_prefix.clone())
                        .map_err(|_| {
                            Error::Msg("Err in LvmtStore::iter_prefix".into())
                        })?
                        .into_iter()
                        .map(|(key, lvmt_value)| (key, lvmt_value.get_value()))
                        .filter(|(_, value)| value.is_some())
                        .map(|(key, value)| (key, value.unwrap()))
                        .collect())
                })?
            } else {
                BTreeMap::new()
            };

        // get from changes, overwrite directly for the same keys
        if let Some(changes) = &self.changes {
            for (key, value) in changes.range(key_prefix.clone()..) {
                if !key.as_ref().starts_with(key_prefix.as_ref()) {
                    break;
                }
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
            return Ok(*state_root); // todo: compute again?
        }

        let mut x = Keccak::v256();
        let iter_all = self.read_all_inner(Box::from([]))?.unwrap_or_default();

        iter_all.iter().for_each(|(k, v)| {
            x.update(&k);
            x.update(&v);
        });

        let mut state_root = [0u8; 32];
        x.finalize(&mut state_root);

        self.cached_state_root = Some(H256(state_root));
        Ok(H256(state_root))
    }
}

impl StorageStateTrait for LvmtState {
    fn get(
        &self, access_key: StorageKeyWithSpace,
    ) -> Result<Option<Box<[u8]>>> {
        let key = access_key
            .to_delta_mpt_key_bytes(&self.delta_trie_key_padding)
            .into_boxed_slice();

        // get from changes
        if let Some(changes) = &self.changes {
            if let Some(value_in_changes) = changes.get(&key) {
                return Ok(value_in_changes.clone());
            }
        }

        // not in changes, then get from backend
        if let Some(view) = &self.base_state {
            let epoch_id = view.epoch_id;
            Ok(self.manager.backend.with_data(|data| {
                Ok(data
                    .get(epoch_id, key)
                    .map_err(|_| {
                        Error::Msg("Fail to get from LvmtStore".into())
                    })?
                    .map(|v| v.get_value())
                    .flatten())
            })?)
        } else {
            Ok(None)
        }
    }

    fn set(
        &mut self, access_key: StorageKeyWithSpace, value: Box<[u8]>,
    ) -> Result<()> {
        self.change_for_access_key(access_key, Some(value))
    }

    // TODO: is it valid to delete a non-existing key? delete() deals with this
    // err.
    fn delete(&mut self, access_key: StorageKeyWithSpace) -> Result<()> {
        let old_value = self.get(access_key)?;
        if old_value.is_none() {
            Err(Error::Msg(
                "Attempted to delete a non-existing key from a storage state"
                    .into(),
            ))
        } else {
            self.change_for_access_key(access_key, None)
        }
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
    /// TODO: this Option is for what?
    fn read_all(
        &mut self, access_key_prefix: StorageKeyWithSpace,
    ) -> Result<Option<Vec<MptKeyValue>>> {
        let key_prefix = access_key_prefix
            .to_delta_mpt_key_bytes(&self.delta_trie_key_padding)
            .into_boxed_slice();
        self.read_all_inner(key_prefix)
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

    // commit() write LvmtStateManager
    fn commit(
        &mut self, epoch: EpochId,
        write_schema: &<Database as DatabaseTrait>::WriteSchema,
    ) -> Result<StateRootWithAuxInfo> {
        if self.manager.backend.with_data(|data| {
            Ok(data
                .query_commit_existence(&epoch)
                .map_err(|e| {dbg!(e); Error::Msg("Fail to query_commit_existence in LvmtStore".into())})?)
        })? {
            let maybe_state_root =
                self.manager.backend.with_state_roots(|state_roots| {
                    Ok(state_roots
                        .get(&epoch)
                        .map_err(|_| {
                            Error::Msg("Err in reading StateRootTable".into())
                        })?
                        .map(|sr| sr.into_owned()))
                })?;
            let state_root = maybe_state_root.expect("State root should be existing for existing commit in Lvmt");
            return Ok(StateRootWithAuxInfo::genesis(&state_root))
        }

        info!(
            "Before commit to pending part. Backend strong_count: {}, weak_count: {}",
            Arc::strong_count(&self.manager.backend),
            Arc::weak_count(&self.manager.backend)
        );

        let state_root = self.compute_state_root_inner()?;
        let changes_inner = self
            .changes
            .as_mut()
            .map(|map_ref| std::mem::take(map_ref))
            .unwrap_or_default();

        // commit state_root
        write_schema.write::<StateRootTable>((
            Cow::Owned(epoch),
            Some(Cow::Owned(state_root)),
        ));
        // commit data (to pending part)
        self.manager.backend.with_data(|data| {
            Ok(data
                .commit(
                    self.base_state.as_ref().map(|state| state.epoch_id),
                    epoch,
                    changes_inner.into_iter(),
                    &write_schema,
                    &AMT,
                )
                .map_err(|e| {dbg!(e); Error::Msg("Fail to commit LvmtStore".into())})?)
        })?;
        // commit data (to historical part) // TODO: how to determine new_root
        // self.manager.backend.storage.confirmed_pending_to_history(todo!(),
        // &write_schema);

        // TODO: should invoke backend.backend.commit to make sure all data in
        // write_schema are written,       this invocation should be
        // outside commit() function but before any read operation of any state.

        info!(
            "After commit to pending part. Backend strong_count: {}, weak_count: {}",
            Arc::strong_count(&self.manager.backend),
            Arc::weak_count(&self.manager.backend)
        );

        Ok(StateRootWithAuxInfo::genesis(&state_root))
    }
}

impl LvmtStateManager {
    pub fn get_state_no_commit_inner(
        self: &Arc<Self>, epoch_id: StateIndex
    ) -> Result<Option<LvmtState>> {
        let maybe_state_root =
            self.backend.with_state_roots(|state_roots| {
                Ok(state_roots
                    .get(&epoch_id.epoch_id)
                    .map_err(|_| {
                        Error::Msg("Err in reading StateRootTable".into())
                    })?
                    .map(|sr| sr.into_owned()))
            })?;

        if maybe_state_root.is_none() {
            return Ok(None);
        }

        if let Some(state_root) = maybe_state_root {
            Ok(Some(LvmtState {
                manager: self.clone(),
                base_state: Some(LvmtView {
                    epoch_id: epoch_id.epoch_id,
                }),
                changes: None,
                cached_state_root: Some(state_root),
                delta_trie_key_padding: epoch_id.delta_mpt_key_padding,
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
        Ok(Some(Box::new(LvmtState {
            manager: self.clone(),
            base_state: Some(LvmtView {
                epoch_id: parent_epoch_id.epoch_id,
            }),
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            delta_trie_key_padding: parent_epoch_id.delta_mpt_key_padding,
        })))
    }

    fn get_state_for_genesis_write(
        self: &Arc<Self>,
    ) -> Box<dyn StorageStateTrait> {
        Box::new(LvmtState {
            manager: self.clone(),
            base_state: None,
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            delta_trie_key_padding: StorageKeyWithSpace::delta_mpt_padding(
                &MERKLE_NULL_NODE,
                &MERKLE_NULL_NODE,
            ),
        })
    }
}
