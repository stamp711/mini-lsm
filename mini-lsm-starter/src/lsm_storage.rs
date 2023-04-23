#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use crate::util::map_bound;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    path: PathBuf,
    flush_lock: Mutex<()>,
    block_cache: BlockCache,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            path: path.as_ref().to_owned(),
            flush_lock: Mutex::new(()),
            block_cache: BlockCache::new(4 * bytesize::GIB),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let i = self.inner.read().clone();

        // Search in memtables, from latest to earliest.
        if let Some(val) = i
            .imm_memtables
            .iter()
            .chain(std::iter::once(&i.memtable))
            .rev()
            .find_map(|memtable| memtable.get(key))
        {
            // Empty value means the key is deleted.
            if val.is_empty() {
                return Ok(None);
            }
            return Ok(Some(val));
        }

        // Search in L0 SSTables, from latest to earliest.
        for table in i.l0_sstables.iter().rev() {
            let iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;
            if iter.is_valid() && iter.key() == key {
                // Empty value means the key is deleted.
                let val =
                    (!iter.value().is_empty()).then_some(Bytes::copy_from_slice(iter.value()));
                return Ok(val);
            }
        }

        // TODO(after SST merge): Search in L1 - L6 SSTables.
        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, &[]);
        Ok(())
    }

    fn path_of_sst_with_id(&self, id: usize) -> PathBuf {
        self.path.join(format!("{}.sst", id))
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let _flush_guard = self.flush_lock.lock();

        let memtable;
        let sst_id;

        // 1. Move current memtable to immutable memtables.
        {
            let mut g = self.inner.write();
            let mut snapshot = g.as_ref().clone();
            memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
            sst_id = snapshot.next_sst_id;
            snapshot.imm_memtables.push(memtable.clone());
            *g = Arc::new(snapshot);
        }

        // 2. Flush the memtable to disk.
        let mut builder = SsTableBuilder::new(4 * bytesize::KIB as usize); // 4 KIB SST size.
        memtable.flush(&mut builder)?;
        let sst = Arc::new(builder.build(sst_id, None, self.path_of_sst_with_id(sst_id))?);

        // 3. Move the SSTable to L0.
        {
            let mut g = self.inner.write();
            let mut snapshot = g.as_ref().clone();
            snapshot.imm_memtables.pop();
            snapshot.l0_sstables.push(sst);
            snapshot.next_sst_id += 1;
            *g = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let i = self.inner.read().clone();

        // Create merged iterators over memtables.
        let memtable_iters = i
            .imm_memtables
            .iter()
            .chain(std::iter::once(&i.memtable))
            .rev()
            .map(|memtable| Box::new(memtable.scan(lower, upper)))
            .collect();
        let merged_memtable_iter = MergeIterator::create(memtable_iters);

        // Create merged iterators over L0 SSTables, but in range (lower..).
        let mut l0_iters = vec![];
        for table in i.l0_sstables.iter().rev().cloned() {
            let iter = match lower {
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(table, key)?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table, key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
            };
            l0_iters.push(Box::new(iter));
        }
        let merged_l0_iter = MergeIterator::create(l0_iters);

        let iter = TwoMergeIterator::create(merged_memtable_iter, merged_l0_iter)?;

        LsmIterator::new_with_upper_bound(iter, map_bound(upper)).map(FusedIterator::new)
    }
}
