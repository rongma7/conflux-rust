// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

// FIXME: What's the proper way to express: 1) Proof not available;
// FIXME: 2) What if Intermediate Delta Root is MERKLE_NULL_NODE.
// TODO: Maybe create a new class for special situation when
// TODO: a full node does not have full state proof, but it
// TODO: could provide a shortcut proof with snapshot_proof
// TODO: at intermediate_epoch_id with delta_proof.
#[derive(Clone, Debug, Default, PartialEq, RlpEncodable, RlpDecodable)]
pub struct MptStateProof {
    pub delta_proof: Option<TrieProof>,
    pub intermediate_proof: Option<TrieProof>,
    pub snapshot_proof: Option<TrieProof>,
}

impl MptStateProof {
    pub fn with_delta(
        &mut self, maybe_delta_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.delta_proof = maybe_delta_proof;
        self
    }

    pub fn with_intermediate(
        &mut self, maybe_intermediate_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.intermediate_proof = maybe_intermediate_proof;
        self
    }

    pub fn with_snapshot(
        &mut self, maybe_snapshot_proof: Option<TrieProof>,
    ) -> &mut Self {
        self.snapshot_proof = maybe_snapshot_proof;
        self
    }

    pub fn is_valid_kv(
        &self, key: &Vec<u8>, value: Option<&[u8]>, root: StateRoot,
        maybe_intermediate_padding: Option<DeltaMptKeyPadding>,
    ) -> bool {
        // Something is wrong when intermediate_proof exists but we are not able
        // to get a intermediate padding.
        if self.intermediate_proof.is_some()
            && maybe_intermediate_padding.is_none()
        {
            return false;
        }

        let delta_root = &root.delta_root;
        let intermediate_root = &root.intermediate_delta_root;
        let snapshot_root = &root.snapshot_root;

        let delta_mpt_padding = StorageKeyWithSpace::delta_mpt_padding(
            &snapshot_root,
            &intermediate_root,
        );

        let storage_key =
            match StorageKeyWithSpace::from_key_bytes::<CheckInput>(&key) {
                Ok(k) => k,
                Err(e) => {
                    warn!("Checking proof with invalid key: {:?}", e);
                    return false;
                }
            };

        let delta_mpt_key =
            storage_key.to_delta_mpt_key_bytes(&delta_mpt_padding);
        let maybe_intermediate_mpt_key = maybe_intermediate_padding
            .as_ref()
            .map(|p| storage_key.to_delta_mpt_key_bytes(p));

        let tombstone_value = MptValue::<Box<[u8]>>::TombStone.unwrap();
        let delta_value = if value.is_some() {
            // Actual value.
            value.clone()
        } else {
            // Tombstone value.
            Some(&*tombstone_value)
        };

        // The delta proof must prove the key-value or key non-existence.
        match &self.delta_proof {
            Some(proof) => {
                // Existence proof.
                if proof.is_valid_kv(&delta_mpt_key, delta_value, delta_root) {
                    return true;
                }
                // Non-existence proof.
                if !proof.is_valid_kv(&delta_mpt_key, None, delta_root) {
                    return false;
                }
            }
            None => {
                // When delta trie exists, the proof can't be empty.
                if delta_root.ne(&MERKLE_NULL_NODE) {
                    return false;
                }
            }
        }

        // Now check intermediate_proof since it's required. Same logic applies.
        match &self.intermediate_proof {
            Some(proof) => {
                if proof.is_valid_kv(
                    // It's guaranteed that
                    // maybe_intermediate_mpt_key.is_some().
                    maybe_intermediate_mpt_key.as_ref().unwrap(),
                    delta_value,
                    intermediate_root,
                ) {
                    return true;
                }
                if !proof.is_valid_kv(
                    maybe_intermediate_mpt_key.as_ref().unwrap(),
                    None,
                    intermediate_root,
                ) {
                    return false;
                }
            }
            None => {
                // When intermediate trie exists, the proof can't be empty.
                if intermediate_root.ne(&MERKLE_NULL_NODE) {
                    return false;
                }
            }
        }

        // At last, check snapshot
        match &self.snapshot_proof {
            None => false,
            Some(proof) => proof.is_valid_kv(key, value, snapshot_root),
        }
    }
}

const TAG_MPT: u8 = 0;
const TAG_LVMT: u8 = 1;

/// Dispatched proof: MPT's three-tier Merkle-Patricia proof, or LVMT's
/// AMT+MMR proof (carried as bytes; decode with `LvmtProof::from_bytes`).
#[derive(Clone, Debug, PartialEq)]
pub enum StateProof {
    Mpt(MptStateProof),
    Lvmt(Vec<u8>),
}

impl Default for StateProof {
    fn default() -> Self { Self::Mpt(MptStateProof::default()) }
}

impl StateProof {
    /// MPT-only. Panics on LVMT.
    pub fn with_delta(
        &mut self, maybe_delta_proof: Option<TrieProof>,
    ) -> &mut Self {
        match self {
            Self::Mpt(p) => {
                p.with_delta(maybe_delta_proof);
                self
            }
            Self::Lvmt(_) => {
                panic!("StateProof::with_delta on an LVMT variant")
            }
        }
    }

    /// MPT-only. Panics on LVMT.
    pub fn with_intermediate(
        &mut self, maybe_intermediate_proof: Option<TrieProof>,
    ) -> &mut Self {
        match self {
            Self::Mpt(p) => {
                p.with_intermediate(maybe_intermediate_proof);
                self
            }
            Self::Lvmt(_) => {
                panic!("StateProof::with_intermediate on an LVMT variant")
            }
        }
    }

    /// MPT-only. Panics on LVMT.
    pub fn with_snapshot(
        &mut self, maybe_snapshot_proof: Option<TrieProof>,
    ) -> &mut Self {
        match self {
            Self::Mpt(p) => {
                p.with_snapshot(maybe_snapshot_proof);
                self
            }
            Self::Lvmt(_) => {
                panic!("StateProof::with_snapshot on an LVMT variant")
            }
        }
    }

    pub fn is_valid_kv(
        &self, key: &Vec<u8>, value: Option<&[u8]>, root: StateRoot,
        maybe_intermediate_padding: Option<DeltaMptKeyPadding>,
    ) -> bool {
        match self {
            Self::Mpt(p) => p.is_valid_kv(
                key,
                value,
                root,
                maybe_intermediate_padding,
            ),
            Self::Lvmt(bytes) => {
                // LVMT puts its single H256 state root into `delta_root`
                // via `StateRoot::genesis(..)`, so that's what verify checks.
                match storage2::verify_lvmt(&root.delta_root, key, bytes) {
                    Ok(Some(proven_value)) => {
                        value.map(|v| v == &*proven_value).unwrap_or(false)
                    }
                    Ok(None) => value.is_none(),
                    Err(e) => {
                        warn!("LVMT proof check failed: {:?}", e);
                        false
                    }
                }
            }
        }
    }
}

impl Encodable for StateProof {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(2);
        match self {
            Self::Mpt(p) => {
                s.append(&TAG_MPT);
                s.append(p);
            }
            Self::Lvmt(bytes) => {
                s.append(&TAG_LVMT);
                s.append(bytes);
            }
        }
    }
}

impl Decodable for StateProof {
    fn decode(rlp: &Rlp) -> std::result::Result<Self, DecoderError> {
        if rlp.item_count()? != 2 {
            return Err(DecoderError::RlpIncorrectListLen);
        }
        let tag: u8 = rlp.val_at(0)?;
        match tag {
            TAG_MPT => Ok(Self::Mpt(rlp.val_at(1)?)),
            TAG_LVMT => Ok(Self::Lvmt(rlp.val_at(1)?)),
            _ => Err(DecoderError::Custom("unknown StateProof variant tag")),
        }
    }
}

use crate::impls::merkle_patricia_trie::TrieProof;
use primitives::{
    CheckInput, DeltaMptKeyPadding, MptValue, StateRoot, StorageKeyWithSpace,
    MERKLE_NULL_NODE,
};
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use rlp_derive::{RlpDecodable, RlpEncodable};
