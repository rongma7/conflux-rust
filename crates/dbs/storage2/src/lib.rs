//! LVMT storage backend, isolated from the host project's type world.
//!
//! Every value this crate accepts or returns is in cfx-storage2's own types
//! (ethereum-types 0.9, primitive-types 0.7). The storage crate above us
//! lives in ethereum-types 0.16; the two never meet directly — the storage
//! crate hands us 32-byte arrays and treats what it gets back as the state
//! root bytes, same width, no version crossing in either direction.

use std::{
    collections::BTreeMap,
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
};

use amt::{AmtParams, CreateMode};
use cfx_storage2::{
    backends::{
        impls::kvdb_rocksdb::WrappedRocksDb, HistoricalTableName,
        PendingTableName,
    },
    verify_lvmt_proof, LvmtProof, LvmtStorage, PendingCleanup, PendingSnapshot,
    PreparedCommit,
};
pub use ethereum_types::H256;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

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

/// What every operation this crate exposes can fail with. The upper storage
/// crate converts this into its own `cfx_db_errors::storage::Error`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LVMT backend error: {0}")]
    Backend(String),
    #[error("read-only state cannot be written")]
    ReadOnlyState,
    #[error("read-only state has no base to prove against")]
    NoBaseState,
    #[error("state root was asked for before it was computed")]
    StateRootUncomputed,
    #[error("malformed LVMT proof: {0}")]
    MalformedProof(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn backend_err<E: std::fmt::Debug>(e: E) -> Error {
    Error::Backend(format!("{:?}", e))
}

type Backend = LvmtStorage<
    WrappedRocksDb<HistoricalTableName>,
    WrappedRocksDb<PendingTableName>,
>;

/// A read-only or writable LVMT state view at a given epoch.
pub struct LvmtState {
    backend: Arc<RwLock<Backend>>,
    /// `None` on the writable state used for the genesis commit.
    /// `Some(epoch_id)` on a read-only state (and on a writable state built
    /// for the next epoch of an existing one, where it is the parent's id).
    base_epoch_id: Option<H256>,
    /// `None` on a read-only state. `Some(..)` on a writable state, including
    /// the genesis one.
    changes: Option<BTreeMap<Box<[u8]>, Option<Box<[u8]>>>>,
    /// Filled by `compute_state_root`; `commit` consumes `cached_prepared`
    /// once it has it, skipping the second compute pass.
    cached_state_root: Option<H256>,
    cached_prepared: Option<PreparedCommit>,
}

impl LvmtState {
    pub fn is_writable(&self) -> bool { self.changes.is_some() }

    pub fn get(&self, key: &[u8]) -> Result<Option<Box<[u8]>>> {
        if let Some(changes) = &self.changes {
            if let Some(v) = changes.get(key) {
                return Ok(v.clone());
            }
        }
        if let Some(epoch_id) = self.base_epoch_id {
            let guard = self.backend.read();
            let reader = guard.as_reader().map_err(backend_err)?;
            let value = reader
                .get(epoch_id, key.to_vec().into_boxed_slice())
                .map_err(backend_err)?
                .map(|v| v.get_value())
                .flatten();
            Ok(value)
        } else {
            Ok(None)
        }
    }

    pub fn get_batch(&self, keys: &[&[u8]]) -> Result<Vec<Option<Box<[u8]>>>> {
        let mut results: Vec<Option<Box<[u8]>>> =
            Vec::with_capacity(keys.len());
        let mut need_backend: Vec<(usize, Box<[u8]>)> = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(changes) = &self.changes {
                if let Some(v) = changes.get(*key) {
                    results.push(v.clone());
                    continue;
                }
            }
            results.push(None);
            need_backend.push((i, key.to_vec().into_boxed_slice()));
        }

        if !need_backend.is_empty() {
            if let Some(epoch_id) = self.base_epoch_id {
                let guard = self.backend.read();
                let reader = guard.as_reader().map_err(backend_err)?;
                for (idx, key) in need_backend {
                    let value = reader
                        .get(epoch_id, key)
                        .map_err(backend_err)?
                        .map(|v| v.get_value())
                        .flatten();
                    results[idx] = value;
                }
            }
        }

        Ok(results)
    }

    /// Yields every key-value pair the state holds in `[lower, upper)`,
    /// overlaying the pending changeset on top of the base-state iteration.
    pub fn iter_range(
        &self, lower: &[u8], upper: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Box<[u8]>)>> {
        let lower_box: Box<[u8]> = lower.to_vec().into_boxed_slice();
        let upper_box: Option<Box<[u8]>> =
            upper.map(|u| u.to_vec().into_boxed_slice());

        let mut keys_values: BTreeMap<Box<[u8]>, Box<[u8]>> =
            if let Some(epoch_id) = self.base_epoch_id {
                let guard = self.backend.read();
                let reader = guard.as_reader().map_err(backend_err)?;
                reader
                    .iter_range(epoch_id, lower_box.clone(), upper_box.clone())
                    .map_err(backend_err)?
                    .into_iter()
                    .filter_map(|(k, lvmt_value)| {
                        lvmt_value.get_value().map(|v| (k, v))
                    })
                    .collect()
            } else {
                BTreeMap::new()
            };

        if let Some(changes) = &self.changes {
            let start = Bound::Included(lower_box);
            let end = match upper_box {
                Some(u) => Bound::Excluded(u),
                None => Bound::Unbounded,
            };
            for (key, value) in changes.range((start, end)) {
                match value {
                    Some(v) => {
                        keys_values.insert(key.clone(), v.clone());
                    }
                    None => {
                        keys_values.remove(key);
                    }
                }
            }
        }

        Ok(keys_values
            .into_iter()
            .map(|(k, v)| (k.into_vec(), v))
            .collect())
    }

    pub fn set(&mut self, key: &[u8], value: Box<[u8]>) -> Result<()> {
        self.change(key.to_vec().into_boxed_slice(), Some(value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.change(key.to_vec().into_boxed_slice(), None)
    }

    fn change(
        &mut self, key: Box<[u8]>, value: Option<Box<[u8]>>,
    ) -> Result<()> {
        let map = self.changes.as_mut().ok_or(Error::ReadOnlyState)?;
        map.insert(key, value);
        self.cached_state_root = None;
        self.cached_prepared = None;
        Ok(())
    }

    /// Returns the proof in cfx-storage2's own `LvmtProof::to_bytes()` wire
    /// shape. The caller is responsible for any further wrapping.
    pub fn get_with_proof(
        &self, key: &[u8],
    ) -> Result<(Option<Box<[u8]>>, Vec<u8>)> {
        let epoch_id = self.base_epoch_id.ok_or(Error::NoBaseState)?;
        let guard = self.backend.read();
        let reader = guard.as_reader().map_err(backend_err)?;
        let (value, proof) = reader
            .get_with_proof(epoch_id, key, &AMT)
            .map_err(backend_err)?;
        let value = value.and_then(|v| v.get_value()).map(Into::into);
        Ok((value, proof.to_bytes()))
    }

    /// Computes the state root the current changeset would commit to, and
    /// caches the `PreparedCommit` so a following `commit` call reuses it.
    pub fn compute_state_root(&mut self) -> Result<H256> {
        if let Some(root) = self.cached_state_root {
            return Ok(root);
        }
        let base = self.base_epoch_id;
        let changes_iter = self
            .changes
            .as_ref()
            .into_iter()
            .flat_map(|map| map.iter())
            .map(|(k, v)| (k.clone(), v.clone()));

        let prepared = self
            .backend
            .read()
            .prepare_commit(base, changes_iter, &AMT)
            .map_err(backend_err)?;
        let state_root = prepared.state_root();

        self.cached_state_root = Some(state_root);
        self.cached_prepared = Some(prepared);
        Ok(state_root)
    }

    pub fn cached_state_root(&self) -> Option<H256> { self.cached_state_root }

    /// Writes the changeset to disk under `epoch` and returns the state
    /// root of this commit.
    pub fn commit(&mut self, epoch: H256) -> Result<H256> {
        let changes = self
            .changes
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        let cached_prepared = self.cached_prepared.take();

        let mut guard = self.backend.write();
        let mut manager = guard.as_manager().map_err(backend_err)?;

        if manager
            .query_commit_existence(&epoch)
            .map_err(backend_err)?
        {
            let root = manager
                .get_state_root(epoch)
                .map_err(backend_err)?
                .expect("committed epoch must have a state root");
            self.cached_state_root = Some(root);
            return Ok(root);
        }

        let state_root = match cached_prepared {
            Some(prepared) => manager
                .commit_prepared(epoch, prepared)
                .map_err(backend_err)?,
            None => manager
                .commit(self.base_epoch_id, epoch, changes.into_iter(), &AMT)
                .map_err(backend_err)?,
        };

        self.cached_state_root = Some(state_root);
        Ok(state_root)
    }
}

/// Snapshot write + pending GC handed out by
/// [`LvmtStateManager::maintenance_deferred`]. Both parts are detached from
/// the backend lock; run them on a background thread.
pub struct LvmtMaintenance {
    pub snapshot: Option<PendingSnapshot<WrappedRocksDb<PendingTableName>>>,
    pub cleanup: Option<PendingCleanup<WrappedRocksDb<PendingTableName>>>,
}

impl LvmtMaintenance {
    pub fn spawn(self) {
        if self.snapshot.is_none() && self.cleanup.is_none() {
            return;
        }
        let LvmtMaintenance { snapshot, cleanup } = self;
        let spawned = std::thread::Builder::new()
            .name("lvmt-maintain".into())
            .spawn(move || {
                if let Some(snap) = snapshot {
                    if let Err(e) = snap.run() {
                        log::error!(
                            "LVMT pending snapshot task failed: {:?}",
                            e
                        );
                    }
                }
                if let Some(cleanup) = cleanup {
                    if let Err(e) = cleanup.run() {
                        log::error!(
                            "LVMT pending cleanup task failed: {:?}",
                            e
                        );
                    }
                }
            });
        if let Err(e) = spawned {
            log::error!(
                "failed to spawn LVMT maintenance thread: {:?}; skipping snapshot/cleanup this round",
                e
            );
        }
    }
}

pub struct LvmtStateManager {
    backend: Arc<RwLock<Backend>>,
}

impl LvmtStateManager {
    pub fn new_arc(
        historical_db_path: PathBuf, pending_db_path: PathBuf,
    ) -> Result<Arc<Self>> {
        let lvmt_storage =
            LvmtStorage::new_from_paths(&historical_db_path, &pending_db_path)
                .map_err(backend_err)?;
        Ok(Arc::new(Self {
            backend: Arc::new(RwLock::new(lvmt_storage)),
        }))
    }

    /// Opens a read-only state at the given committed epoch. Returns `None`
    /// when the epoch is not on this node.
    pub fn open_state_at_epoch(
        self: &Arc<Self>, epoch_id: H256,
    ) -> Result<Option<LvmtState>> {
        let maybe_state_root = self
            .backend
            .read()
            .as_reader()
            .map_err(backend_err)?
            .get_state_root(epoch_id)
            .map_err(backend_err)?;
        Ok(maybe_state_root.map(|state_root| LvmtState {
            backend: self.backend.clone(),
            base_epoch_id: Some(epoch_id),
            changes: None,
            cached_state_root: Some(state_root),
            cached_prepared: None,
        }))
    }

    /// Opens a writable state that will commit as the child epoch of
    /// `parent_epoch_id`.
    pub fn open_state_for_next_epoch(
        self: &Arc<Self>, parent_epoch_id: H256,
    ) -> Result<LvmtState> {
        // Warms the key_value_store's CurrentMap to the parent, so that
        // subsequent gets hit the O(1) path.
        self.backend
            .read()
            .as_reader()
            .map_err(backend_err)?
            .checkout_current(parent_epoch_id)
            .map_err(backend_err)?;
        Ok(LvmtState {
            backend: self.backend.clone(),
            base_epoch_id: Some(parent_epoch_id),
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            cached_prepared: None,
        })
    }

    /// Opens a writable state whose commit becomes the genesis epoch.
    pub fn open_state_for_genesis_write(self: &Arc<Self>) -> LvmtState {
        LvmtState {
            backend: self.backend.clone(),
            base_epoch_id: None,
            changes: Some(BTreeMap::new()),
            cached_state_root: None,
            cached_prepared: None,
        }
    }

    pub fn make_pivot(&self, commit_id: H256) -> Result<bool> {
        let mut guard = self.backend.write();
        let mut manager = guard.as_manager().map_err(backend_err)?;
        manager.make_pivot(commit_id).map_err(backend_err)
    }

    /// Acquires the backend write lock once, does the lock-held portion of
    /// the pending-to-history migration (when `migrate` is `Some`) and the
    /// snapshot/cleanup bookkeeping, then returns the detached tasks for a
    /// background thread.
    pub fn maintenance_deferred(
        &self, migrate: Option<(u64, H256)>,
    ) -> Result<LvmtMaintenance> {
        let mut guard = self.backend.write();
        let snapshot = if let Some((new_root_height, pivot_commit_id)) = migrate
        {
            guard
                .confirmed_pending_to_history_with_height_deferring_snapshot(
                    new_root_height,
                    pivot_commit_id,
                )
                .map_err(backend_err)?
        } else {
            None
        };
        let cleanup = guard.pending_cleanup().map_err(backend_err)?;
        Ok(LvmtMaintenance { snapshot, cleanup })
    }

    pub fn is_newer_than_pending_root(&self, height: u64) -> Result<bool> {
        Ok(self
            .backend
            .read()
            .as_reader()
            .map_err(backend_err)?
            .is_newer_than_pending_root(height))
    }
}

/// Verifies an LVMT proof produced by [`LvmtState::get_with_proof`].
/// Returns `Ok(Some(value))` if the key is present (empty value = deleted),
/// `Ok(None)` if the key was never written, and `Err` if the proof does not
/// check out against `state_root`.
pub fn verify_lvmt(
    state_root: &H256, key: &[u8], proof_bytes: &[u8],
) -> Result<Option<Box<[u8]>>> {
    let proof = LvmtProof::from_bytes(proof_bytes)
        .map_err(|e| Error::MalformedProof(format!("{:?}", e)))?;
    let proven = verify_lvmt_proof(&AMT, state_root, key, &proof)
        .map_err(|e| Error::Backend(format!("proof invalid: {:?}", e)))?;
    Ok(proven.and_then(|v| v.get_value()))
}

/// Computes the exclusive upper bound a key prefix translates into for a
/// range iteration. Returns `None` when the prefix is all-`0xff`, meaning
/// the iteration runs to the end.
pub fn to_key_prefix_iter_upper_bound(key_prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = key_prefix.to_vec();
    if upper.is_empty() {
        return None;
    }
    let len = upper.len();
    for i in 0..len {
        if upper[len - 1 - i] == 255 {
            upper[len - 1 - i] = 0;
        } else {
            upper[len - 1 - i] += 1;
            return Some(upper);
        }
    }
    None
}
