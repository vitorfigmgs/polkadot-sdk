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
- **`MigrateToPalletChildTrie<Pallet, DbWeight>`**: a runtime-upgrade hook that migrates `Pallet` completely. Requires `Pallet: PalletInfoAccess` and `DbWeight: Get<RuntimeDbWeight>`.

### `PalletChildTrieValue` methods

- **`exists()`**: reports whether a value is stored.
- **`get()`**: returns the stored value, or the query kind's empty result when none is stored.
- **`try_get()`**: returns the stored value, or `Err(())` when none is stored.
- **`put(value)`**: stores a value, replacing any existing one.
- **`kill()`**: removes the stored value.
- **`take()`**: removes the stored value and returns it, using the query kind's result type.
- **`mutate(f)`**: passes the current value to a closure, stores whatever the closure leaves behind, and returns the closure's result.
- **`try_mutate(f)`**: like `mutate`, but the closure returns a `Result`. On `Err`, nothing is stored and the error is returned.
- **`child_key()`**: returns the key this value occupies inside the pallet's child trie.

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
- **`child_key_for(key)`**: returns the child trie key of the entry for `key`.

### Free functions

- **`pallet_child_trie_is_empty(pallet_name)`**: reports whether the pallet's child trie holds no data.
- **`clear_pallet_child_trie(pallet_name, limit)`**: deletes the pallet's child-trie data. `None` deletes everything; `Some(n)` deletes at most `n` entries. Returns `sp_io::KillStorageResult`.
- **`migrate_pallet_to_child_trie(pallet_name, limit)`**: moves the pallet's main-trie data into its child trie. `None` moves everything; `Some(n)` moves at most `n` entries. Returns `MigrationProgress`.
- **`pending_legacy_entries(pallet_name)`**: returns how many of the pallet's main-trie entries are still waiting to be migrated.

`OptionQuery`, `ValueQuery`, and the existing hashers (e.g. `Blake2_128Concat`, `Twox64Concat`) must work. Each method and function behaves like its same-named counterpart on `StorageValue` / `StorageMap`.

## Requirements

### Placement
1. Written values and map entries read back unchanged.
2. Writing leaves no key of the pallet in the main trie. A regular `StorageValue` / `StorageMap` with the same `Prefix` sees nothing.
3. `pallet_child_trie_is_empty` is `true` for a pallet with no data and `false` once it has data.

### Isolation
4. Two pallets with an item of the same name keep independent data. Writing or removing one never affects the other.

### Key layout
5. `Map::child_key_for(k)` equals `LegacyMap::hashed_key_for(k)[16..]`, and `Value::child_key()` equals `LegacyValue::hashed_key()[16..]`.

### Values
6. When empty: `exists()` is `false`, `get()` returns the query kind's empty value, `try_get()` is `Err(())`.
7. `mutate` persists its change.
8. A `try_mutate` closure returning `Err(e)` returns `Err(e)` and leaves storage unchanged.
9. `take()` returns the stored value and empties the slot.

### Maps
10. A missing key gives `contains_key == false` and `get == None`.
11. `insert` overwrites; the last value wins.
12. A `mutate` that sets the value to `None` removes the entry.
13. `take` returns the value and removes it; a second `take` returns `None`.
14. `remove` deletes the entry.

### Iteration
15. `iter()` yields exactly that map's `(key, value)` pairs and nothing from other items of the pallet. `iter_keys()` yields that map's keys.
16. `drain()` yields that map's entries and leaves the map empty. Other items of the pallet are untouched.

### Clearing
17. `clear_pallet_child_trie(pallet, None)` returns `AllRemoved(_)`, empties every item of that pallet (`pallet_child_trie_is_empty` is `true`), and leaves other pallets untouched.
18. With `Some(limit)` on committed state, each call deletes at most `limit` entries. It returns `SomeRemaining(n)` while entries remain, and `AllRemoved(n)` on the final call. Clearing 10 entries with limit 4 gives `SomeRemaining(4)`, `SomeRemaining(4)`, `AllRemoved(2)`, after which the pallet is empty.

### Migration
19. `migrate_pallet_to_child_trie(pallet, None)` moves every main-trie entry of the pallet into its child trie at the position given by rule 5. Data written earlier through regular storage types then reads back unchanged through the matching new types, and no entry of the pallet remains in the main trie.
20. The pallet's on-chain storage version is not moved: `StorageVersion::get::<Pallet>()` is the same before and after.
21. Other pallets' main-trie storage is untouched and their child tries remain empty.
22. `MigrationProgress` reports `moved`, `skipped` (legacy entries dropped, see 23), and `complete` (`true` iff no migratable main-trie entries of the pallet remain).
23. If the child trie already holds a value for a key, that value is kept. The legacy entry is removed from the main trie and counted in `skipped`. Other entries are still moved.
24. With `Some(limit)`, a call processes at most `limit` entries and later calls resume where it stopped, losing no entry. Migrating 10 entries with limit 4 gives `{moved:4, complete:false}`, `{moved:4, complete:false}`, `{moved:2, complete:true}`. `Some(0)` with entries pending gives `{moved:0, skipped:0, complete:false}` and moves nothing.
25. Running the migration after completion returns `{moved:0, skipped:0, complete:true}` and leaves the state root unchanged. The same applies to a pallet that never stored anything, whose child trie stays empty.
26. `pending_legacy_entries` counts the pallet's remaining main-trie entries, excluding the storage version: 6 after migrating 4 of 10, and 0 when complete.
27. `MigrateToPalletChildTrie::<Pallet, DbWeight>::on_runtime_upgrade()` fully migrates the pallet named `Pallet::name()` (as in 19) and returns a weight with `ref_time` at least `DbWeight.write × entries moved`.

## Out of scope
- `#[pallet::storage]` macro integration and metadata changes.
- Change tries.