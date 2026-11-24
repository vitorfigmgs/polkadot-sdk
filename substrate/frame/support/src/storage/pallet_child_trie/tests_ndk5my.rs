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

//! Behaviour tests for per-pallet child tries.
//!
//! Each test describes one scenario and announces every check with an "it should ..." line.
//! Run with `--nocapture` to see the narrative.

use crate::{
	storage::{
		pallet_child_trie::{
			clear_pallet_child_trie, migrate_pallet_to_child_trie, pallet_child_trie_is_empty,
			pending_legacy_entries, MigrateToPalletChildTrie, MigrationProgress,
			PalletChildTrieMap, PalletChildTrieValue,
		},
		types::{StorageMap, StorageValue, ValueQuery},
	},
	traits::{
		CrateVersion, Get, OnRuntimeUpgrade, PalletInfoAccess, StorageInstance, StorageVersion,
	},
	weights::RuntimeDbWeight,
	Blake2_128Concat, Twox64Concat,
};
use alloc::{vec, vec::Vec};
use sp_io::{hashing::twox_128, KillStorageResult, TestExternalities};

/// Narrates a behaviour test: the scenario under test and what each check expects.
struct Log;

impl Log {
	/// Start a scenario.
	fn scenario(description: &str) -> Self {
		println!("Scenario: {description}");
		Log
	}

	/// Announce the next assertion (or group of assertions).
	fn it(&self, expectation: &str) {
		assert!(expectation.starts_with("should "), "expectations start with \"should\"");
		println!("  it {expectation}");
	}
}

// --- A "Staking" and a "Treasury" pallet, each with a value and a map. ---

macro_rules! storage_instance {
	($name:ident, $pallet:literal, $item:literal) => {
		struct $name;
		impl StorageInstance for $name {
			fn pallet_prefix() -> &'static str {
				$pallet
			}
			const STORAGE_PREFIX: &'static str = $item;
		}
	};
}

storage_instance!(StakingEraPrefix, "Staking", "CurrentEra");
storage_instance!(StakingLedgerPrefix, "Staking", "Ledger");
storage_instance!(StakingBondedPrefix, "Staking", "Bonded");
storage_instance!(TreasuryEraPrefix, "Treasury", "CurrentEra");
storage_instance!(TreasuryProposalsPrefix, "Treasury", "Proposals");
storage_instance!(StakingNominatorsPrefix, "Staking", "Nominators");

// Storage in the pallet's own child trie.
type CurrentEra = PalletChildTrieValue<StakingEraPrefix, u32, ValueQuery>;
type Ledger = PalletChildTrieMap<StakingLedgerPrefix, Blake2_128Concat, u64, u128>;
type Bonded = PalletChildTrieMap<StakingBondedPrefix, Twox64Concat, u64, u64>;
type Nominators = PalletChildTrieMap<StakingNominatorsPrefix, Twox64Concat, u32, u64>;
type TreasuryEra = PalletChildTrieValue<TreasuryEraPrefix, u32, ValueQuery>;
type Proposals = PalletChildTrieMap<TreasuryProposalsPrefix, Twox64Concat, u32, u64>;

// The very same items, stored the regular way in the main trie (before migrating).
type LegacyCurrentEra = StorageValue<StakingEraPrefix, u32, ValueQuery>;
type LegacyLedger = StorageMap<StakingLedgerPrefix, Blake2_128Concat, u64, u128>;
type LegacyBonded = StorageMap<StakingBondedPrefix, Twox64Concat, u64, u64>;
type LegacyProposals = StorageMap<TreasuryProposalsPrefix, Twox64Concat, u32, u64>;

struct StakingPallet;
impl PalletInfoAccess for StakingPallet {
	fn index() -> usize {
		1
	}
	fn name() -> &'static str {
		"Staking"
	}
	fn name_hash() -> [u8; 16] {
		twox_128(b"Staking")
	}
	fn module_name() -> &'static str {
		"pallet_staking"
	}
	fn crate_version() -> CrateVersion {
		CrateVersion::new(1, 0, 0)
	}
}

struct StopAfterThree;
impl Get<Option<u32>> for StopAfterThree {
	fn get() -> Option<u32> {
		Some(3)
	}
}

struct TestDbWeight;
impl Get<RuntimeDbWeight> for TestDbWeight {
	fn get() -> RuntimeDbWeight {
		RuntimeDbWeight { read: 1, write: 10 }
	}
}

const STAKING: &[u8] = b"Staking";
const TREASURY: &[u8] = b"Treasury";

/// Whether the main trie holds any key of the pallet.
fn main_trie_has_pallet_data(pallet: &[u8]) -> bool {
	let prefix = twox_128(pallet);
	sp_io::storage::exists(&prefix)
		|| sp_io::storage::next_key(&prefix).map_or(false, |k| k.starts_with(&prefix))
}

#[test]
fn pallet_storage_lives_in_the_pallet_child_trie_not_in_the_main_trie() {
	let log = Log::scenario("a pallet writes a value and a map entry to its child trie storage");
	TestExternalities::new_empty().execute_with(|| {
		CurrentEra::put(7);
		Ledger::insert(1, 1_000);

		log.it("should read back what was written");
		assert_eq!(CurrentEra::get(), 7);
		assert_eq!(Ledger::get(1), Some(1_000));

		log.it("should leave the main trie without any key of the pallet");
		assert!(!main_trie_has_pallet_data(STAKING));
		assert!(!LegacyCurrentEra::exists());
		assert!(!LegacyLedger::contains_key(1));

		log.it("should make the pallet child trie non-empty");
		assert!(!pallet_child_trie_is_empty(STAKING));
	});
}

#[test]
fn pallets_get_isolated_child_tries() {
	let log = Log::scenario("two pallets use a storage item with the same name");
	TestExternalities::new_empty().execute_with(|| {
		CurrentEra::put(1);
		TreasuryEra::put(2);

		log.it("should keep each pallet's value independent");
		assert_eq!(CurrentEra::get(), 1);
		assert_eq!(TreasuryEra::get(), 2);

		log.it("should not affect the other pallet when one pallet removes its value");
		CurrentEra::kill();
		assert_eq!(CurrentEra::get(), 0);
		assert_eq!(TreasuryEra::get(), 2);
	});
}

#[test]
fn maps_with_the_same_key_in_different_pallets_are_isolated() {
	let log = Log::scenario("a Treasury map and a Staking map both hold an entry for key 5");
	TestExternalities::new_empty().execute_with(|| {
		Proposals::insert(5, 50);
		Nominators::insert(5, 500);

		log.it("should keep each pallet's entry for the same key independent");
		assert_eq!(Proposals::get(5), Some(50));
		assert_eq!(Nominators::get(5), Some(500));

		log.it("should not affect the other pallet when one pallet removes its entry");
		Nominators::remove(5);
		assert_eq!(Nominators::get(5), None);
		assert_eq!(Proposals::get(5), Some(50));

		log.it("should not yield the other pallet's entries when iterating");
		Nominators::insert(6, 600);
		assert_eq!(Proposals::iter().collect::<Vec<_>>(), vec![(5, 50)]);
		assert_eq!(Nominators::iter().collect::<Vec<_>>(), vec![(6, 600)]);
	});
}

#[test]
fn values_honour_query_kinds_and_failed_mutations() {
	let log = Log::scenario("a value is read when empty, mutated and taken");
	TestExternalities::new_empty().execute_with(|| {
		log.it("should return the default when nothing is stored");
		assert!(!CurrentEra::exists());
		assert_eq!(CurrentEra::get(), 0);
		assert_eq!(CurrentEra::try_get(), Err(()));

		log.it("should persist a successful mutation");
		CurrentEra::mutate(|era| *era += 5);
		assert_eq!(CurrentEra::get(), 5);

		log.it("should leave storage untouched when a mutation fails");
		let result: Result<(), &str> = CurrentEra::try_mutate(|era| {
			*era = 99;
			Err("rejected")
		});
		assert_eq!(result, Err("rejected"));
		assert_eq!(CurrentEra::get(), 5);

		log.it("should return the stored value and empty the slot when taken");
		assert_eq!(CurrentEra::take(), 5);
		assert!(!CurrentEra::exists());
	});
}

#[test]
fn maps_support_insert_read_and_remove() {
	let log = Log::scenario("a map entry is inserted, overwritten, mutated and removed");
	TestExternalities::new_empty().execute_with(|| {
		log.it("should report missing keys as absent");
		assert!(!Ledger::contains_key(1));
		assert_eq!(Ledger::get(1), None);

		log.it("should keep the last written value");
		Ledger::insert(1, 10);
		Ledger::insert(1, 20);
		assert_eq!(Ledger::get(1), Some(20));

		log.it("should remove the entry when a mutation sets it to None");
		Ledger::mutate(1, |v| *v = None);
		assert!(!Ledger::contains_key(1));

		log.it("should return and remove an entry when taken");
		Ledger::insert(2, 30);
		assert_eq!(Ledger::take(2), Some(30));
		assert_eq!(Ledger::take(2), None);

		log.it("should forget an entry once removed");
		Ledger::insert(3, 40);
		Ledger::remove(3);
		assert_eq!(Ledger::get(3), None);
	});
}

#[test]
fn iterating_a_map_only_yields_its_own_entries() {
	let log = Log::scenario("two maps of the same pallet share its child trie");
	TestExternalities::new_empty().execute_with(|| {
		Ledger::insert(1, 100);
		Ledger::insert(2, 200);
		Bonded::insert(1, 9);
		CurrentEra::put(3);

		log.it("should list every entry of the map and nothing else");
		let mut ledger: Vec<_> = Ledger::iter().collect();
		ledger.sort();
		assert_eq!(ledger, vec![(1, 100), (2, 200)]);
		assert_eq!(Bonded::iter_keys().collect::<Vec<_>>(), vec![1]);

		log.it("should empty only the drained map");
		assert_eq!(Ledger::drain().count(), 2);
		assert_eq!(Ledger::iter().count(), 0);
		assert_eq!(Bonded::get(1), Some(9));
		assert_eq!(CurrentEra::get(), 3);
	});
}

#[test]
fn clearing_a_pallet_child_trie_removes_all_of_its_storage_only() {
	let log = Log::scenario("a pallet's whole storage is deleted at once");
	TestExternalities::new_empty().execute_with(|| {
		CurrentEra::put(1);
		Ledger::insert(1, 1);
		Bonded::insert(1, 1);
		TreasuryEra::put(2);
		Proposals::insert(5, 50);

		let result = clear_pallet_child_trie(STAKING, None);

		log.it("should report that nothing remains");
		assert!(matches!(result, KillStorageResult::AllRemoved(_)));

		log.it("should remove every item of the pallet");
		assert!(!CurrentEra::exists());
		assert!(!Ledger::contains_key(1));
		assert!(!Bonded::contains_key(1));
		assert!(pallet_child_trie_is_empty(STAKING));

		log.it("should keep the storage of every other pallet");
		assert_eq!(TreasuryEra::get(), 2);
		assert_eq!(Proposals::get(5), Some(50));
	});
}

#[test]
fn clearing_a_large_pallet_child_trie_can_be_spread_over_several_blocks() {
	let log = Log::scenario("a pallet child trie with 10 entries is cleared 4 entries per block");
	let mut ext = TestExternalities::new_empty();
	ext.execute_with(|| (0..10u64).for_each(|i| Ledger::insert(i, i as u128)));
	ext.commit_all().expect("committing test storage never fails; qed");

	log.it("should delete only up to the limit and report that entries remain");
	ext.execute_with(|| {
		assert!(matches!(
			clear_pallet_child_trie(STAKING, Some(4)),
			KillStorageResult::SomeRemaining(4)
		));
		assert!(!pallet_child_trie_is_empty(STAKING));
	});
	ext.commit_all().expect("committing test storage never fails; qed");

	log.it("should finish the deletion in later blocks");
	ext.execute_with(|| {
		assert!(matches!(
			clear_pallet_child_trie(STAKING, Some(4)),
			KillStorageResult::SomeRemaining(4)
		));
	});
	ext.commit_all().expect("committing test storage never fails; qed");
	ext.execute_with(|| {
		assert!(matches!(
			clear_pallet_child_trie(STAKING, Some(4)),
			KillStorageResult::AllRemoved(2)
		));
		assert!(pallet_child_trie_is_empty(STAKING));
		assert_eq!(Ledger::iter().count(), 0);
	});
}

#[test]
fn runtime_upgrade_moves_existing_pallet_storage_into_its_child_trie() {
	let log =
		Log::scenario("a live pallet with main trie storage is migrated in a runtime upgrade");
	TestExternalities::new_empty().execute_with(|| {
		LegacyCurrentEra::put(12);
		LegacyLedger::insert(1, 1_000);
		LegacyLedger::insert(2, 2_000);
		LegacyBonded::insert(7, 77);

		let weight = MigrateToPalletChildTrie::<StakingPallet, TestDbWeight>::on_runtime_upgrade();

		log.it("should make all existing data readable through the child trie storage");
		assert_eq!(CurrentEra::get(), 12);
		assert_eq!(Ledger::get(1), Some(1_000));
		assert_eq!(Ledger::get(2), Some(2_000));
		assert_eq!(Bonded::get(7), Some(77));

		log.it("should remove the pallet's data from the main trie");
		assert!(!LegacyCurrentEra::exists());
		assert_eq!(LegacyLedger::iter().count(), 0);
		assert_eq!(pending_legacy_entries(STAKING), 0);

		log.it("should charge a weight covering a write for every moved entry");
		assert!(weight.ref_time() >= 4 * 10);

		log.it("should charge a proof size, since the migration reads from the trie");
		assert!(weight.proof_size() > 0);
	});
}

#[test]
fn migration_keeps_other_pallets_and_the_storage_version_in_the_main_trie() {
	let log = Log::scenario("one pallet is migrated while another pallet stays as it is");
	TestExternalities::new_empty().execute_with(|| {
		StorageVersion::new(3).put::<StakingPallet>();
		LegacyLedger::insert(1, 1);
		LegacyProposals::insert(5, 50);

		let progress = migrate_pallet_to_child_trie(STAKING, None);

		log.it("should only move the migrated pallet's items");
		assert_eq!(progress, MigrationProgress { moved: 1, skipped: 0, complete: true });
		assert_eq!(Ledger::get(1), Some(1));

		log.it("should leave other pallets in the main trie");
		assert_eq!(LegacyProposals::get(5), Some(50));
		assert!(pallet_child_trie_is_empty(TREASURY));

		log.it("should keep the pallet's on-chain storage version readable");
		assert_eq!(StorageVersion::get::<StakingPallet>(), StorageVersion::new(3));
	});
}

#[test]
fn running_the_migration_again_changes_nothing() {
	let log = Log::scenario("the migration runs a second time, and on a pallet with no storage");
	TestExternalities::new_empty().execute_with(|| {
		LegacyLedger::insert(1, 10);
		migrate_pallet_to_child_trie(STAKING, None);
		let root_after_first_run = sp_io::storage::root(sp_runtime::StateVersion::V1);

		log.it("should move nothing and report completion the second time");
		let progress = migrate_pallet_to_child_trie(STAKING, None);
		assert_eq!(progress, MigrationProgress { moved: 0, skipped: 0, complete: true });

		log.it("should leave the state exactly as it was");
		assert_eq!(sp_io::storage::root(sp_runtime::StateVersion::V1), root_after_first_run);
		assert_eq!(Ledger::get(1), Some(10));

		log.it("should be a no-op for a pallet that never stored anything");
		let progress = migrate_pallet_to_child_trie(b"EmptyPallet", None);
		assert_eq!(progress, MigrationProgress { moved: 0, skipped: 0, complete: true });
		assert!(pallet_child_trie_is_empty(b"EmptyPallet"));
	});
}

#[test]
fn migration_can_be_spread_over_several_blocks() {
	let log = Log::scenario("a pallet with 10 entries is migrated 4 entries per block");
	TestExternalities::new_empty().execute_with(|| {
		(0..10u64).for_each(|i| LegacyBonded::insert(i, i * 10));

		log.it("should move at most the limit and report that work remains");
		let first = migrate_pallet_to_child_trie(STAKING, Some(4));
		assert_eq!(first, MigrationProgress { moved: 4, skipped: 0, complete: false });
		assert_eq!(pending_legacy_entries(STAKING), 6);

		log.it("should resume where it stopped until everything is moved");
		let second = migrate_pallet_to_child_trie(STAKING, Some(4));
		let third = migrate_pallet_to_child_trie(STAKING, Some(4));
		assert_eq!(second, MigrationProgress { moved: 4, skipped: 0, complete: false });
		assert_eq!(third, MigrationProgress { moved: 2, skipped: 0, complete: true });

		log.it("should lose no entry along the way");
		let mut bonded: Vec<_> = Bonded::iter().collect();
		bonded.sort();
		assert_eq!(bonded, (0..10u64).map(|i| (i, i * 10)).collect::<Vec<_>>());

		log.it("should move nothing with a zero limit");
		LegacyBonded::insert(99, 1);
		let idle = migrate_pallet_to_child_trie(STAKING, Some(0));
		assert_eq!(idle, MigrationProgress { moved: 0, skipped: 0, complete: false });
		assert_eq!(LegacyBonded::get(99), Some(1));
	});
}

#[test]
fn migration_never_overwrites_newer_child_trie_data() {
	let log = Log::scenario(
		"during a lazy migration the pallet already wrote new values to its child trie",
	);
	TestExternalities::new_empty().execute_with(|| {
		LegacyLedger::insert(1, 100);
		LegacyLedger::insert(2, 200);
		Ledger::insert(1, 111);

		let progress = migrate_pallet_to_child_trie(STAKING, None);

		log.it("should keep the newer child trie value instead of the stale one");
		assert_eq!(Ledger::get(1), Some(111));
		assert_eq!(progress, MigrationProgress { moved: 1, skipped: 1, complete: true });

		log.it("should still move entries that have no newer value");
		assert_eq!(Ledger::get(2), Some(200));

		log.it("should drop the stale entries from the main trie");
		assert!(!LegacyLedger::contains_key(1));
		assert!(!main_trie_has_pallet_data(STAKING));
	});
}

#[test]
fn entries_deleted_during_a_staged_migration_stay_deleted() {
	let log = Log::scenario("a pallet deletes entries before the migration has reached them");
	TestExternalities::new_empty().execute_with(|| {
		LegacyLedger::insert(1, 100);
		LegacyLedger::insert(2, 200);
		LegacyLedger::insert(3, 300);
		LegacyCurrentEra::put(4);
		migrate_pallet_to_child_trie(STAKING, Some(0));

		log.it("should not bring back an entry that was removed through the child trie type");
		Ledger::remove(1);
		assert_eq!(Ledger::take(2), None);
		CurrentEra::kill();
		migrate_pallet_to_child_trie(STAKING, None);
		assert_eq!(Ledger::get(1), None);
		assert_eq!(Ledger::get(2), None);
		assert!(!CurrentEra::exists());

		log.it("should still move the entries that were not deleted");
		assert_eq!(Ledger::get(3), Some(300));
	});
}

#[test]
fn clearing_during_a_staged_migration_removes_unmigrated_entries_too() {
	let log = Log::scenario("a pallet is cleared while only part of it has been migrated");
	TestExternalities::new_empty().execute_with(|| {
		(0..4u64).for_each(|i| LegacyBonded::insert(i, i));
		StorageVersion::new(2).put::<StakingPallet>();
		migrate_pallet_to_child_trie(STAKING, Some(2));
		clear_pallet_child_trie(STAKING, None);

		log.it("should leave nothing to migrate afterwards");
		assert_eq!(pending_legacy_entries(STAKING), 0);
		let progress = migrate_pallet_to_child_trie(STAKING, None);
		assert_eq!(progress, MigrationProgress { moved: 0, skipped: 0, complete: true });
		assert_eq!(Bonded::iter().count(), 0);

		log.it("should keep the pallet's storage version");
		assert_eq!(StorageVersion::get::<StakingPallet>(), StorageVersion::new(2));
	});
}

#[test]
fn draining_a_map_during_a_staged_migration_leaves_other_maps_alone() {
	let log = Log::scenario("one map of a partly migrated pallet is drained");
	TestExternalities::new_empty().execute_with(|| {
		LegacyBonded::insert(1, 1);
		LegacyLedger::insert(1, 1);
		Bonded::insert(2, 2);

		let drained: Vec<_> = Bonded::drain().collect();

		log.it("should yield the entries the map already holds in its child trie");
		assert_eq!(drained, vec![(2, 2)]);

		log.it("should remove that map's migrated and unmigrated entries");
		assert_eq!(Bonded::iter().count(), 0);
		migrate_pallet_to_child_trie(STAKING, None);
		assert_eq!(Bonded::iter().count(), 0);

		log.it("should still migrate the other map");
		assert_eq!(Ledger::get(1), Some(1));
	});
}

#[test]
fn a_capped_runtime_upgrade_moves_only_the_cap_and_can_be_run_again() {
	let log = Log::scenario("a runtime upgrade is configured to move at most 3 entries per run");
	TestExternalities::new_empty().execute_with(|| {
		(0..7u64).for_each(|i| LegacyBonded::insert(i, i));

		MigrateToPalletChildTrie::<StakingPallet, TestDbWeight, StopAfterThree>::on_runtime_upgrade(
		);

		log.it("should leave the remaining entries to a later run");
		assert_eq!(Bonded::iter().count(), 3);
		assert_eq!(pending_legacy_entries(STAKING), 4);

		log.it("should finish the migration over repeated runs without losing entries");
		MigrateToPalletChildTrie::<StakingPallet, TestDbWeight, StopAfterThree>::on_runtime_upgrade(
		);
		MigrateToPalletChildTrie::<StakingPallet, TestDbWeight, StopAfterThree>::on_runtime_upgrade(
		);
		assert_eq!(pending_legacy_entries(STAKING), 0);
		assert_eq!(Bonded::iter().count(), 7);
	});
}
