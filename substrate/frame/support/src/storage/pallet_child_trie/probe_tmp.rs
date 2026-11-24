// TEMPORARY probe tests — delete after evaluation.
use super::*;
use crate::{
	storage::types::{StorageMap, StorageValue},
	Twox64Concat,
};
use sp_io::TestExternalities;

struct LPrefix;
impl StorageInstance for LPrefix {
	fn pallet_prefix() -> &'static str {
		"P"
	}
	const STORAGE_PREFIX: &'static str = "L";
}
type L = PalletChildTrieMap<LPrefix, Twox64Concat, u64, u64>;
type Legacy = StorageMap<LPrefix, Twox64Concat, u64, u64>;
type LegacyV = StorageValue<LPrefix, u64>;

#[test]
fn probe_resurrection_after_delete_during_lazy_migration() {
	TestExternalities::new_empty().execute_with(|| {
		Legacy::insert(1, 10);
		// New code deletes the entry before migration reaches it.
		L::remove(1);
		migrate_pallet_to_child_trie(b"P", None);
		println!("PROBE resurrection: L::get(1) = {:?}", L::get(1));
	});
}

#[test]
fn probe_no_read_through_mid_migration() {
	TestExternalities::new_empty().execute_with(|| {
		(0..4u64).for_each(|i| Legacy::insert(i, i));
		migrate_pallet_to_child_trie(b"P", Some(2));
		let visible = (0..4u64).filter(|i| L::get(i).is_some()).count();
		println!("PROBE read-through: {visible}/4 visible mid-migration");
	});
}

#[test]
fn probe_clear_limit_on_overlay() {
	TestExternalities::new_empty().execute_with(|| {
		(0..10u64).for_each(|i| L::insert(i, i));
		let r = clear_pallet_child_trie(b"P", Some(1));
		println!("PROBE overlay clear Some(1): {:?}, remaining {}", r, L::iter().count());
	});
}

#[test]
fn probe_iter_keys_skips_bad_values() {
	TestExternalities::new_empty().execute_with(|| {
		L::insert(1, 1);
		let k = L::child_key_for(2u64);
		child::put_raw(&L::child_info(), &k, &[1u8]); // undecodable u64
		println!(
			"PROBE iter_keys: {} keys (2 stored), contains_key(2)={}",
			L::iter_keys().count(),
			L::contains_key(2)
		);
	});
}

#[test]
fn probe_value_migrated_across_commits() {
	let mut ext = TestExternalities::new_empty();
	ext.execute_with(|| {
		LegacyV::put(5);
		(0..5u64).for_each(|i| Legacy::insert(i, i));
	});
	ext.commit_all().unwrap();
	let mut calls = 0;
	loop {
		let p = ext.execute_with(|| migrate_pallet_to_child_trie(b"P", Some(2)));
		ext.commit_all().unwrap();
		calls += 1;
		if p.complete {
			break
		}
	}
	ext.execute_with(|| {
		println!(
			"PROBE committed resume: {calls} calls, {} entries, pending {}",
			L::iter().count(),
			pending_legacy_entries(b"P")
		);
	});
}

#[test]
fn probe_bare_prefix_key() {
	TestExternalities::new_empty().execute_with(|| {
		unhashed::put_raw(&twox_128(b"P"), &[7u8]);
		let p = migrate_pallet_to_child_trie(b"P", None);
		println!(
			"PROBE bare prefix: {:?}, empty={}",
			p,
			pallet_child_trie_is_empty(b"P")
		);
	});
}

#[test]
fn probe_weight_noop() {
	TestExternalities::new_empty().execute_with(|| {
		let p = migrate_pallet_to_child_trie(b"Nothing", None);
		println!("PROBE noop weight: {:?}", p.weight(RuntimeDbWeight { read: 1, write: 10 }));
	});
}
