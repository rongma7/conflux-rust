// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

// StateManager is the single entry-point to access State for any epoch.
// StateManager manages internal mutability and is thread-safe.
//
// The concrete type is a thin dispatch wrapper (`impls::state_manager_dispatch
// ::StateManager`) that holds either the original MPT backend or the LVMT
// backend; the choice is made at construction time from
// `StorageConfiguration::use_lvmt_storage`.
pub use super::impls::state_manager_dispatch::StateManager;

pub type SharedStateManager = Arc<StateManager>;

use std::sync::Arc;

pub use storage2::{StateIndex, StorageManagerTrait as StateManagerTrait};
