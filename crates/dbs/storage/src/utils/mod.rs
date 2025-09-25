// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

#[macro_use]
pub mod deref_plus_impl_or_borrow_self;
#[macro_use]
pub mod tuple;
pub mod guarded_value;
pub mod wrap;

pub use storage2::to_key_prefix_iter_upper_bound;

pub mod access_mode {
    pub trait AccessMode {
        const READ_ONLY: bool;
    }

    pub struct Read;
    pub struct Write;

    impl AccessMode for Read {
        const READ_ONLY: bool = true;
    }

    impl AccessMode for Write {
        const READ_ONLY: bool = false;
    }
}

/// The purpose of this trait is to create a new value of a passed object,
/// when the passed object is the value, simply move the value;
/// when the passed object is the reference, create the new value by clone.
/// Other extension is possible.
///
/// The trait is used by ChildrenTable and slab.
pub trait WrappedCreateFrom<FromType, ToType> {
    fn take(x: FromType) -> ToType;
    /// Unoptimized default implementation.
    fn take_from(dest: &mut ToType, x: FromType) { *dest = Self::take(x); }
}

/*
/// This is correct but we don't use this implementation.
impl<T> WrappedCreateFrom<T, UnsafeCell<T>> for UnsafeCell<T> {
    fn take(val: T) -> UnsafeCell<T> { UnsafeCell::new(val) }
}
*/

impl<'x, T: Clone> WrappedCreateFrom<&'x T, UnsafeCell<T>> for UnsafeCell<T> {
    fn take(val: &'x T) -> UnsafeCell<T> { UnsafeCell::new(val.clone()) }

    fn take_from(dest: &mut UnsafeCell<T>, x: &'x T) {
        dest.get_mut().clone_from(x);
    }
}

pub trait UnsafeCellExtension<T: Sized> {
    fn get_ref(&self) -> &T;
    fn get_mut(&mut self) -> &mut T;
    unsafe fn get_as_mut(&self) -> &mut T;
}

impl<T: Sized> UnsafeCellExtension<T> for UnsafeCell<T> {
    fn get_ref(&self) -> &T { unsafe { &*self.get() } }

    fn get_mut(&mut self) -> &mut T { unsafe { &mut *self.get() } }

    unsafe fn get_as_mut(&self) -> &mut T { &mut *self.get() }
}

/// Only used by storage benchmark due to incompatibility of rlp crate version.
pub trait StateRootWithAuxInfoToFromRlpBytes {
    fn to_rlp_bytes(&self) -> Vec<u8>;
    fn from_rlp_bytes(bytes: &[u8]) -> Result<StateRootWithAuxInfo>;
}

/// Only used by storage benchmark due to incompatibility of rlp crate version.
impl StateRootWithAuxInfoToFromRlpBytes for StateRootWithAuxInfo {
    fn to_rlp_bytes(&self) -> Vec<u8> { self.rlp_bytes() }

    fn from_rlp_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::decode(&Rlp::new(bytes))?)
    }
}

use crate::Result;
use cfx_internal_common::StateRootWithAuxInfo;
use rlp::{Decodable, Encodable, Rlp};
use std::cell::UnsafeCell;
