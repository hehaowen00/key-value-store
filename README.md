# key-value-store

Bitcask inspired key value store

## API


| Function | Description |
|----------| ----------- |
| `fn open(opts: Options) -> Bitcask` | Open a new or an existing bitcask file |
| `fn flush(&mut self)` | Write data and index to disk |
| `fn exists(&self, key: &[u8]) -> bool` | Check if key exists in index |
| `fn keys(&self) -> Keys` | Returns an iterator over all keys in key value store |
| `fn get(&self, key: &[u8]) -> Option<Value>` | Fetch a value from the key value store |
| `fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>` | Insert a value into the key value store, replacing the existing value if the key exists |
| `fn delete(&mut self, key: &[u8]) -> bool` | Removes a key from the index and marks the data as deleted. Returns true if an entry was found and deleted. |
| `fn merge(&mut self)` | Perform compactation on data files |

## Types

### Options
```rust
{
    base_dir: PathBuf,
    cache_size: u64,
    file_size_limit: u64,
    max_key_size: u64,
    max_value_size: u64,
    bytes_deleted_ratio: f64,
    entries_deleted_ratio: f64
}
```

### Value
```rust
{
    timestamp: u64,
    value: Vec<u8>,
}
```

## Notes

Entries are not guaranteed to be persisted to disk until the data is flush by either reaching the limit on the writer or manually calling the `flush` method.

Unlike bitcask, compaction is performed on all files including the current file used for writing to.
