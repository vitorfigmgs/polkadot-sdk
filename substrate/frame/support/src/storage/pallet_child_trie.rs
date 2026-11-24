// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-pallet child tries.
//!
//! By default every storage item of a pallet lives in the main (top) trie under the key
//! `Twox128(pallet_prefix) ++ Twox128(storage_prefix) ++ hashed_key`. This module allows a pallet
//! to keep all of its storage items inside a *child trie that belongs only to that pallet*
//! instead.
//!
//! # Layout
//!
//! * Every pallet gets exactly one child trie. Its storage key is
//!   [`PALLET_CHILD_TRIE_PREFIX`]` ++ Twox128(pallet_prefix)`. All pallet child trie storage keys
//!   have the same length, therefore no pallet child trie key can be a prefix of another one, and
//!   deleting the child trie of one pallet can never touch the child trie of another pallet. The
//!   pallet hash is 128 bits wide, the same width used for the pallet prefix in the main trie.
//! * Inside the child trie, keys follow the regular FRAME hashing scheme, minus the pallet prefix:
//!   `Twox128(storage_prefix) ++ hashed_key`. This means a regular storage item at main trie key
//!   `Twox128(pallet_prefix) ++ rest` ends up at child trie key `rest` once migrated.
//!
//! # Storage types
//!
//! * [`PalletChildTrieValue`]: equivalent of [`crate::storage::types::StorageValue`].
//! * [`PalletChildTrieMap`]: equivalent of [`crate::storage::types::StorageMap`].
//!
//! Both are parametrised by a [`StorageInstance`], exactly like the regular storage types, which
//! provides the pallet prefix (used to select the child trie) and the storage prefix (used inside
//! the child trie).
//!
//! # Migration
//!
//! [`migrate_pallet_to_child_trie`] moves the storage of an existing pallet from the main trie into
//! its child trie. It can be called with a limit to spread the migration over several blocks, and
//! [`MigrateToPalletChildTrie`] wraps it as an [`OnRuntimeUpgrade`] that performs the migration in
//! a single runtime upgrade. The on-chain [`StorageVersion`](crate::traits::StorageVersion) of the
//! pallet is *never* moved: FRAME reads it from the main trie.
//!
//! # Change tries
//!
//! The original request also asked for change tries to cover child tries. Change tries have since
//! been removed from Substrate, so there is nothing left to extend.

use crate::{
	storage::{
		child::{self, ChildInfo},
		types::{OptionQuery, QueryKindTrait},
		unhashed, ChildTriePrefixIterator,
	},
	traits::{
		Get, GetDefault, OnRuntimeUpgrade, PalletInfoAccess, StorageInstance,
		STORAGE_VERSION_STORAGE_KEY_POSTFIX,
	},
	weights::{RuntimeDbWeight, Weight},
	ReversibleStorageHasher, StorageHasher,
};
use alloc::vec::Vec;
#[cfg(feature = "try-runtime")]
use codec::{Decode, Encode};
use codec::{EncodeLike, FullCodec};
use core::marker::PhantomData;
use sp_io::{hashing::twox_128, KillStorageResult};

/// Prefix of the storage key of every pallet child trie.
///
/// The full storage key of the child trie of a pallet is `PALLET_CHILD_TRIE_PREFIX ++
/// Twox128(pallet_prefix)`.
pub const PALLET_CHILD_TRIE_PREFIX: &[u8] = b":pallet:";

/// Length of the hashed pallet / storage prefix.
const HASHED_PREFIX_LEN: usize = 16;

/// The storage key (inside the `:child_storage:default:` namespace) of the child trie of the
/// pallet named `pallet_prefix`.
pub fn pallet_child_trie_storage_key(pallet_prefix: &[u8]) -> Vec<u8> {
	let mut key = Vec::with_capacity(PALLET_CHILD_TRIE_PREFIX.len() + HASHED_PREFIX_LEN);
	key.extend_from_slice(PALLET_CHILD_TRIE_PREFIX);
	key.extend_from_slice(&twox_128(pallet_prefix));
	key
}

/// The [`ChildInfo`] of the child trie owned by the pallet named `pallet_prefix`.
fn pallet_child_info(pallet_prefix: &[u8]) -> ChildInfo {
	ChildInfo::new_default(&pallet_child_trie_storage_key(pallet_prefix))
}

/// Remove up to `limit` keys of the child trie of the pallet named `pallet_prefix`.
///
/// `None` removes everything. Keys written in the current block (the overlay) are always
/// removed and do not count toward `limit`. When the result is
/// [`KillStorageResult::SomeRemaining`], call again (typically in a later block) to continue.
pub fn clear_pallet_child_trie(pallet_prefix: &[u8], limit: Option<u32>) -> KillStorageResult {
	let child_info = pallet_child_info(pallet_prefix);
	sp_io::default_child_storage::storage_kill(child_info.storage_key(), limit)
}

/// Whether the child trie of the pallet named `pallet_prefix` holds any key.
pub fn pallet_child_trie_is_empty(pallet_prefix: &[u8]) -> bool {
	let child_info = pallet_child_info(pallet_prefix);
	let storage_key = child_info.storage_key();
	!sp_io::default_child_storage::exists(storage_key, &[])
		&& sp_io::default_child_storage::next_key(storage_key, &[]).is_none()
}

/// Key of a storage item inside its pallet child trie: `Twox128(storage_prefix) ++ suffix`.
fn item_key(storage_prefix: &str, suffix: &[u8]) -> Vec<u8> {
	let mut key = Vec::with_capacity(HASHED_PREFIX_LEN + suffix.len());
	key.extend_from_slice(&twox_128(storage_prefix.as_bytes()));
	key.extend_from_slice(suffix);
	key
}

/// A storage value kept in the child trie of its pallet.
///
/// Behaves like [`crate::storage::types::StorageValue`], but reads and writes the child trie of
/// `Prefix::pallet_prefix()` at key `Twox128(Prefix::STORAGE_PREFIX)`.
pub struct PalletChildTrieValue<Prefix, Value, QueryKind = OptionQuery, OnEmpty = GetDefault>(
	PhantomData<(Prefix, Value, QueryKind, OnEmpty)>,
);

impl<Prefix, Value, QueryKind, OnEmpty> PalletChildTrieValue<Prefix, Value, QueryKind, OnEmpty>
where
	Prefix: StorageInstance,
	Value: FullCodec,
	QueryKind: QueryKindTrait<Value, OnEmpty>,
	OnEmpty: Get<QueryKind::Query> + 'static,
{
	/// The child trie this value lives in.
	pub fn child_info() -> ChildInfo {
		pallet_child_info(Prefix::pallet_prefix().as_bytes())
	}

	/// The key of this value inside the child trie.
	pub fn child_key() -> Vec<u8> {
		item_key(Prefix::STORAGE_PREFIX, &[])
	}

	/// Whether a value is explicitly stored.
	pub fn exists() -> bool {
		child::exists(&Self::child_info(), &Self::child_key())
	}

	/// Load the value, or `OnEmpty` (as defined by `QueryKind`) if nothing is stored.
	pub fn get() -> QueryKind::Query {
		QueryKind::from_optional_value_to_query(child::get(&Self::child_info(), &Self::child_key()))
	}

	/// Load the value, or `Err(())` if nothing is stored.
	pub fn try_get() -> Result<Value, ()> {
		child::get(&Self::child_info(), &Self::child_key()).ok_or(())
	}

	/// Store a value.
	pub fn put<Arg: EncodeLike<Value>>(val: Arg) {
		child::put(&Self::child_info(), &Self::child_key(), &val)
	}

	/// Store the query: a query that maps to no value removes the entry.
	pub fn set(val: QueryKind::Query) {
		match QueryKind::from_query_to_optional_value(val) {
			Some(v) => Self::put(v),
			None => Self::kill(),
		}
	}

	/// Remove the value.
	pub fn kill() {
		child::kill(&Self::child_info(), &Self::child_key())
	}

	/// Remove the value and return what was stored.
	pub fn take() -> QueryKind::Query {
		QueryKind::from_optional_value_to_query(child::take(
			&Self::child_info(),
			&Self::child_key(),
		))
	}

	/// Mutate the value.
	pub fn mutate<R, F: FnOnce(&mut QueryKind::Query) -> R>(f: F) -> R {
		Self::try_mutate(|v| Ok::<R, core::convert::Infallible>(f(v)))
			.expect("`Infallible` can never be constructed; qed")
	}

	/// Mutate the value if the closure returns `Ok`. Storage is left untouched on `Err`.
	pub fn try_mutate<R, E, F: FnOnce(&mut QueryKind::Query) -> Result<R, E>>(
		f: F,
	) -> Result<R, E> {
		let mut val = Self::get();
		let res = f(&mut val)?;
		Self::set(val);
		Ok(res)
	}
}

/// A storage map kept in the child trie of its pallet.
///
/// Behaves like [`crate::storage::types::StorageMap`], but reads and writes the child trie of
/// `Prefix::pallet_prefix()` at keys `Twox128(Prefix::STORAGE_PREFIX) ++ Hasher(key.encode())`.
pub struct PalletChildTrieMap<
	Prefix,
	Hasher,
	Key,
	Value,
	QueryKind = OptionQuery,
	OnEmpty = GetDefault,
>(PhantomData<(Prefix, Hasher, Key, Value, QueryKind, OnEmpty)>);

impl<Prefix, Hasher, Key, Value, QueryKind, OnEmpty>
	PalletChildTrieMap<Prefix, Hasher, Key, Value, QueryKind, OnEmpty>
where
	Prefix: StorageInstance,
	Hasher: StorageHasher,
	Key: FullCodec,
	Value: FullCodec,
	QueryKind: QueryKindTrait<Value, OnEmpty>,
	OnEmpty: Get<QueryKind::Query> + 'static,
{
	/// The child trie this map lives in.
	pub fn child_info() -> ChildInfo {
		pallet_child_info(Prefix::pallet_prefix().as_bytes())
	}

	/// The prefix shared by all the keys of this map inside the child trie.
	pub fn map_prefix() -> Vec<u8> {
		item_key(Prefix::STORAGE_PREFIX, &[])
	}

	/// The key of the entry `key` inside the child trie.
	pub fn child_key_for<KeyArg: EncodeLike<Key>>(key: KeyArg) -> Vec<u8> {
		key.using_encoded(|encoded| {
			item_key(Prefix::STORAGE_PREFIX, Hasher::hash(encoded).as_ref())
		})
	}

	/// Whether an entry is explicitly stored for `key`.
	pub fn contains_key<KeyArg: EncodeLike<Key>>(key: KeyArg) -> bool {
		child::exists(&Self::child_info(), &Self::child_key_for(key))
	}

	/// Load the value at `key`, or `OnEmpty` (as defined by `QueryKind`) if nothing is stored.
	pub fn get<KeyArg: EncodeLike<Key>>(key: KeyArg) -> QueryKind::Query {
		QueryKind::from_optional_value_to_query(child::get(
			&Self::child_info(),
			&Self::child_key_for(key),
		))
	}

	/// Load the value at `key`, or `Err(())` if nothing is stored.
	pub fn try_get<KeyArg: EncodeLike<Key>>(key: KeyArg) -> Result<Value, ()> {
		child::get(&Self::child_info(), &Self::child_key_for(key)).ok_or(())
	}

	/// Store `val` at `key`.
	pub fn insert<KeyArg: EncodeLike<Key>, ValArg: EncodeLike<Value>>(key: KeyArg, val: ValArg) {
		child::put(&Self::child_info(), &Self::child_key_for(key), &val)
	}

	/// Store the query at `key`: a query that maps to no value removes the entry.
	pub fn set<KeyArg: EncodeLike<Key>>(key: KeyArg, val: QueryKind::Query) {
		let child_key = Self::child_key_for(key);
		match QueryKind::from_query_to_optional_value(val) {
			Some(v) => child::put(&Self::child_info(), &child_key, &v),
			None => child::kill(&Self::child_info(), &child_key),
		}
	}

	/// Remove the entry at `key`.
	pub fn remove<KeyArg: EncodeLike<Key>>(key: KeyArg) {
		child::kill(&Self::child_info(), &Self::child_key_for(key))
	}

	/// Remove the entry at `key` and return what was stored.
	pub fn take<KeyArg: EncodeLike<Key>>(key: KeyArg) -> QueryKind::Query {
		QueryKind::from_optional_value_to_query(child::take(
			&Self::child_info(),
			&Self::child_key_for(key),
		))
	}

	/// Mutate the value at `key`.
	pub fn mutate<KeyArg: EncodeLike<Key>, R, F: FnOnce(&mut QueryKind::Query) -> R>(
		key: KeyArg,
		f: F,
	) -> R {
		Self::try_mutate(key, |v| Ok::<R, core::convert::Infallible>(f(v)))
			.expect("`Infallible` can never be constructed; qed")
	}

	/// Mutate the value at `key` if the closure returns `Ok`. Storage is left untouched on `Err`.
	pub fn try_mutate<KeyArg, R, E, F>(key: KeyArg, f: F) -> Result<R, E>
	where
		KeyArg: EncodeLike<Key>,
		F: FnOnce(&mut QueryKind::Query) -> Result<R, E>,
	{
		let child_key = Self::child_key_for(key);
		let mut val =
			QueryKind::from_optional_value_to_query(child::get(&Self::child_info(), &child_key));
		let res = f(&mut val)?;
		match QueryKind::from_query_to_optional_value(val) {
			Some(v) => child::put(&Self::child_info(), &child_key, &v),
			None => child::kill(&Self::child_info(), &child_key),
		}
		Ok(res)
	}

	/// Remove up to `limit` entries of this map, leaving the rest of the pallet child trie
	/// untouched. Entries written in the current block do not count toward `limit`.
	pub fn clear(limit: Option<u32>) -> KillStorageResult {
		let child_info = Self::child_info();
		sp_io::default_child_storage::clear_prefix(
			child_info.storage_key(),
			&Self::map_prefix(),
			limit,
		)
	}
}

impl<Prefix, Hasher, Key, Value, QueryKind, OnEmpty>
	PalletChildTrieMap<Prefix, Hasher, Key, Value, QueryKind, OnEmpty>
where
	Prefix: StorageInstance,
	Hasher: StorageHasher + ReversibleStorageHasher,
	Key: FullCodec,
	Value: FullCodec,
	QueryKind: QueryKindTrait<Value, OnEmpty>,
	OnEmpty: Get<QueryKind::Query> + 'static,
{
	/// Iterate over all `(key, value)` entries of this map, in no particular order.
	///
	/// Entries whose key or value fail to decode are skipped.
	pub fn iter() -> ChildTriePrefixIterator<(Key, Value)> {
		ChildTriePrefixIterator::with_prefix_over_key::<Hasher>(
			&Self::child_info(),
			&Self::map_prefix(),
		)
	}

	/// Iterate over all keys of this map.
	pub fn iter_keys() -> impl Iterator<Item = Key> {
		Self::iter().map(|(k, _)| k)
	}

	/// Remove every entry of this map, returning them while doing so.
	pub fn drain() -> ChildTriePrefixIterator<(Key, Value)> {
		Self::iter().drain()
	}
}

/// Outcome of a call to [`migrate_pallet_to_child_trie`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrationProgress {
	/// Entries moved from the main trie into the pallet child trie.
	pub moved: u32,
	/// Legacy entries dropped because the child trie already held a value for the same key.
	pub skipped: u32,
	/// Whether the main trie holds no more entries of the pallet to migrate.
	pub complete: bool,
}

impl MigrationProgress {
	/// Upper bound of the weight consumed by the call that produced this progress.
	pub fn weight(&self, db: RuntimeDbWeight) -> Weight {
		let processed = self.moved.saturating_add(self.skipped) as u64;
		// Per entry: find next key, read the legacy value, check the child trie. Plus the final
		// lookup that finds no more entries (or the limit), and the storage version key we skip.
		let reads = processed.saturating_mul(3).saturating_add(2);
		// Per entry: remove the legacy key, and write to the child trie when moved.
		let writes = processed.saturating_add(self.moved as u64);
		db.reads_writes(reads, writes)
	}
}

/// The main trie key holding the on-chain storage version of the pallet.
fn storage_version_key(pallet_prefix: &[u8]) -> Vec<u8> {
	let mut key = twox_128(pallet_prefix).to_vec();
	key.extend_from_slice(&twox_128(STORAGE_VERSION_STORAGE_KEY_POSTFIX));
	key
}

/// The first main trie key strictly after `cursor` (or equal to it if `inclusive`) that starts
/// with `prefix`.
fn next_key_with_prefix(prefix: &[u8], cursor: &[u8], inclusive: bool) -> Option<Vec<u8>> {
	if inclusive && sp_io::storage::exists(cursor) {
		return Some(cursor.to_vec());
	}
	sp_io::storage::next_key(cursor).filter(|k| k.starts_with(prefix))
}

/// Move the storage of the pallet named `pallet_prefix` from the main trie into its child trie.
///
/// Every main trie key `Twox128(pallet_prefix) ++ rest` is moved to `rest` in the pallet child
/// trie, so data written by the regular storage types becomes readable through
/// [`PalletChildTrieValue`] and [`PalletChildTrieMap`] with the same storage prefix, hasher and
/// key.
///
/// * At most `limit` entries are processed per call (`None` means no limit). The migration resumes
///   where it stopped on the next call, so it can be spread over several blocks.
/// * A value already present in the child trie is newer than the legacy one, so it is kept and the
///   legacy entry is dropped (counted as `skipped`).
/// * The on-chain storage version of the pallet stays in the main trie.
/// * Calling it again after completion does nothing.
pub fn migrate_pallet_to_child_trie(pallet_prefix: &[u8], limit: Option<u32>) -> MigrationProgress {
	let prefix = twox_128(pallet_prefix);
	let version_key = storage_version_key(pallet_prefix);
	let child_info = pallet_child_info(pallet_prefix);
	let mut progress = MigrationProgress::default();

	let mut next = next_key_with_prefix(&prefix, &prefix, true);
	while let Some(key) = next {
		if key == version_key {
			next = next_key_with_prefix(&prefix, &key, false);
			continue;
		}
		if limit.map_or(false, |l| progress.moved.saturating_add(progress.skipped) >= l) {
			return progress;
		}

		let child_key = &key[HASHED_PREFIX_LEN..];
		if child::exists(&child_info, child_key) {
			progress.skipped = progress.skipped.saturating_add(1);
		} else if let Some(value) = unhashed::get_raw(&key) {
			child::put_raw(&child_info, child_key, &value);
			progress.moved = progress.moved.saturating_add(1);
		}
		unhashed::kill(&key);

		next = next_key_with_prefix(&prefix, &key, false);
	}

	progress.complete = true;
	progress
}

/// Number of main trie entries of the pallet still waiting to be migrated.
pub fn pending_legacy_entries(pallet_prefix: &[u8]) -> u32 {
	let prefix = twox_128(pallet_prefix);
	let version_key = storage_version_key(pallet_prefix);
	let mut count = 0u32;
	let mut next = next_key_with_prefix(&prefix, &prefix, true);
	while let Some(key) = next {
		if key != version_key {
			count = count.saturating_add(1);
		}
		next = next_key_with_prefix(&prefix, &key, false);
	}
	count
}

/// Runtime upgrade that moves all the storage of `Pallet` into its child trie in one go.
///
/// Use [`migrate_pallet_to_child_trie`] with a limit instead when the pallet holds too much data
/// to migrate within a single block.
pub struct MigrateToPalletChildTrie<Pallet, DbWeight>(PhantomData<(Pallet, DbWeight)>);

impl<Pallet: PalletInfoAccess, DbWeight: Get<RuntimeDbWeight>> OnRuntimeUpgrade
	for MigrateToPalletChildTrie<Pallet, DbWeight>
{
	fn on_runtime_upgrade() -> Weight {
		let progress = migrate_pallet_to_child_trie(Pallet::name().as_bytes(), None);
		log::info!(
			target: "runtime::storage",
			"Migrated pallet {} to its child trie: {} moved, {} superseded",
			Pallet::name(),
			progress.moved,
			progress.skipped,
		);
		progress.weight(DbWeight::get())
	}

	#[cfg(feature = "try-runtime")]
	fn pre_upgrade() -> Result<Vec<u8>, sp_runtime::TryRuntimeError> {
		Ok(pending_legacy_entries(Pallet::name().as_bytes()).encode())
	}

	#[cfg(feature = "try-runtime")]
	fn post_upgrade(state: Vec<u8>) -> Result<(), sp_runtime::TryRuntimeError> {
		let before = u32::decode(&mut &state[..])
			.map_err(|_| sp_runtime::TryRuntimeError::Other("invalid pre_upgrade state"))?;
		frame_support::ensure!(
			pending_legacy_entries(Pallet::name().as_bytes()) == 0,
			"pallet storage left in the main trie"
		);
		frame_support::ensure!(
			before == 0 || !pallet_child_trie_is_empty(Pallet::name().as_bytes()),
			"pallet child trie is empty after migrating"
		);
		Ok(())
	}
}

#[cfg(test)]
mod tests_ndk5my;
#[cfg(test)]
mod probe_tmp;
