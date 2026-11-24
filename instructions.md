# Per-pallet child tries

Let a pallet keep all of its storage in a child trie of its own, and let an existing pallet's storage be moved there. Implement this in `frame-support`.

## Interface

All items are exported from `frame_support::storage::pallet_child_trie`. `Prefix` is any `StorageInstance`.

### Types

- **`PalletChildTrieValue<Prefix, Value, QueryKind = OptionQuery, OnEmpty = GetDefault>`**: a single stored value that lives in the pallet's child trie.
- **`PalletChildTrieMap<Prefix, Hasher, Key, Value, QueryKind = OptionQuery, OnEmpty = GetDefault>`**: a key-to-value collection that lives in the pallet's child trie.
- **`MigrationProgress`**: the result of one migration call.
  - `moved: u32`: the number of entries moved into the child trie by this call.
  - `skipped: u32`: the number of legacy entries dropped by this call because the child trie already held a value for that key.
  - `complete: bool`: `true` when no migratable entries of the pallet remain in the main trie.
  - Derives `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`.
- **`MigrateToPalletChildTrie<Pallet, DbWeight, Limit = ()>`**: a runtime-upgrade hook that migrates `Pallet`. Requires `Pallet: PalletInfoAccess`, `DbWeight: Get<RuntimeDbWeight>` and `Limit: Get<Option<u32>>`. The default `()` means no limit; `Some(n)` caps the entries moved per run.

### `PalletChildTrieValue` methods

- **`exists()`**: reports whether a value is stored.
- **`get()`**: returns the stored value, or the query kind's empty result when none is stored.
- **`try_get()`**: returns the stored value, or `Err(())` when none is stored.
- **`put(value)`**: stores a value, replacing any existing one.
- **`kill()`**: removes the stored value.
- **`take()`**: removes the stored value and returns it, using the query kind's result type.
- **`mutate(f)`**: passes the current value to a closure, stores whatever the closure leaves behind, and returns the closure's result.
- **`try_mutate(f)`**: like `mutate`, but the closure returns a `Result`. On `Err`, nothing is stored and the error is returned.

### `PalletChildTrieMap` methods

- **`contains_key(key)`**: reports whether an entry exists for `key`.
- **`get(key)`**: returns the entry's value, or the query kind's empty result when none exists.
- **`try_get(key)`**: returns the entry's value, or `Err(())` when none exists.
- **`insert(key, value)`**: stores a value for `key`, replacing any existing one.
- **`remove(key)`**: deletes the entry for `key`.
- **`take(key)`**: deletes the entry for `key` and returns its value, using the query kind's result type.
- **`mutate(key, f)`**: passes the current value for `key` to a closure, stores whatever the closure leaves behind, and returns the closure's result.
- **`try_mutate(key, f)`**: like `mutate`, but the closure returns a `Result`. On `Err`, nothing is stored and the error is returned.
- **`iter()`**: yields every `(key, value)` pair of this map, in no particular order.
- **`iter_keys()`**: yields every key of this map.
- **`drain()`**: yields every `(key, value)` pair of this map and removes each one as it is yielded.

### Free functions

In each, `pallet_name` is the pallet's name as a byte slice (`&[u8]`), e.g. `b"Staking"`, the same string `PalletInfoAccess::name()` returns.

- **`pallet_child_trie_is_empty(pallet_name)`**: reports whether the pallet's child trie holds no data.
- **`clear_pallet_child_trie(pallet_name, limit)`**: deletes the pallet's data, including entries not yet migrated. `None` deletes everything; `Some(n)` deletes at most `n` entries. Returns `sp_io::KillStorageResult`.
- **`migrate_pallet_to_child_trie(pallet_name, limit)`**: moves the pallet's main-trie data into its child trie. `None` moves everything; `Some(n)` moves at most `n` entries. Returns `MigrationProgress`.
- **`pending_legacy_entries(pallet_name)`**: returns how many of the pallet's main-trie entries are still waiting to be migrated.

`OptionQuery`, `ValueQuery`, and the existing hashers (e.g. `Blake2_128Concat`, `Twox64Concat`) must work. Each method listed above behaves like its same-named counterpart on `StorageValue` / `StorageMap`. The free functions have no such counterpart; their behavior is defined below.

## Requirements

### Placement
1. Written values and map entries read back unchanged.
2. Writing leaves no key of the pallet in the main trie. A regular `StorageValue` / `StorageMap` with the same `Prefix` sees nothing.
3. `pallet_child_trie_is_empty` is `true` for a pallet with no data and `false` once it has data.

### Isolation
4. Two pallets with an item of the same name keep independent data. Writing or removing one never affects the other.

### Values
5. When empty: `exists()` is `false`, `get()` returns the query kind's empty value, `try_get()` is `Err(())`.
6. `mutate` persists its change.
7. A `try_mutate` closure returning `Err(e)` returns `Err(e)` and leaves storage unchanged.
8. `take()` returns the stored value and empties the slot.

### Maps
9. A missing key gives `contains_key == false` and `get == None`.
10. `insert` overwrites; the last value wins.
11. A `mutate` that sets the value to `None` removes the entry.
12. `take` returns the value and removes it; a second `take` returns `None`.
13. `remove` deletes the entry.

### Iteration
14. `iter()` yields exactly that map's `(key, value)` pairs and nothing from other items of the pallet. `iter_keys()` yields that map's keys.
15. `drain()` yields that map's entries and leaves the map empty. Other items of the pallet are untouched.

### Clearing
16. `clear_pallet_child_trie(pallet, None)` returns `AllRemoved(_)`, empties every item of that pallet (`pallet_child_trie_is_empty` is `true`), and leaves other pallets untouched.
17. With `Some(limit)`, each call deletes at most `limit` entries persisted by earlier blocks. Entries written earlier in the same block are always deleted and don't count toward `limit`. It returns `SomeRemaining(n)` while entries remain, and `AllRemoved(n)` on the final call. Clearing 10 persisted entries with limit 4 gives `SomeRemaining(4)`, `SomeRemaining(4)`, `AllRemoved(2)`, after which the pallet is empty.

### Migration
18. `migrate_pallet_to_child_trie(pallet, None)` moves every main-trie entry of the pallet into its child trie, except the pallet's storage version. Data written earlier through a regular `StorageValue` / `StorageMap` with the same `Prefix`, hasher and key then reads back unchanged through the matching new type, and no other entry of the pallet remains in the main trie.
19. The pallet's on-chain storage version is not moved: `StorageVersion::get::<Pallet>()` is the same before and after.
20. Other pallets' main-trie storage is untouched and their child tries remain empty.
21. `MigrationProgress` reports `moved`, `skipped` (legacy entries dropped, see 22), and `complete` (`true` iff no migratable main-trie entries of the pallet remain).
22. If the child trie already holds a value for a key, that value is kept. The legacy entry is removed from the main trie and counted in `skipped`. Other entries are still moved.
23. With `Some(limit)`, a call processes at most `limit` entries and later calls resume where it stopped, losing no entry. Migrating 10 entries with limit 4 gives `{moved:4, complete:false}`, `{moved:4, complete:false}`, `{moved:2, complete:true}`. `Some(0)` with entries pending gives `{moved:0, skipped:0, complete:false}` and moves nothing.
24. Running the migration after completion returns `{moved:0, skipped:0, complete:true}` and leaves the state root unchanged. The same applies to a pallet that never stored anything, whose child trie stays empty.
25. `pending_legacy_entries` counts the pallet's remaining main-trie entries, excluding the storage version: 6 after migrating 4 of 10, and 0 when complete.
26. While a migration is only partly done, deleting an entry through the new types (`kill`, `remove`, `take`, or a `mutate` to `None`) also deletes its not yet migrated copy, so a later migration step never brings it back.
27. While a migration is only partly done, `clear_pallet_child_trie` also deletes the pallet's not yet migrated entries, but never the storage version. Likewise a map's `drain()` also deletes that map's not yet migrated entries (without yielding them). Other maps' entries are still migrated afterwards.
28. Until a migration completes, reads through the new types are only guaranteed to see entries already in the child trie. Entries not yet migrated may not be visible.
29. `MigrateToPalletChildTrie::<Pallet, DbWeight>::on_runtime_upgrade()` fully migrates the pallet named `Pallet::name()` (as in 18) and returns a weight with `ref_time` at least `DbWeight.write × entries moved` and a non-zero `proof_size`.
30. With `Limit = Some(n)`, one `on_runtime_upgrade()` run moves at most `n` entries. Running it again continues, and repeated runs move every entry without loss.

## Out of scope
- `#[pallet::storage]` macro integration and metadata changes.