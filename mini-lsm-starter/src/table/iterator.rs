use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_idx: usize,
    block_iter: BlockIterator,
}

fn seek_table_to_first(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
    Ok((
        0,
        BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
    ))
}

fn seek_table_to_key(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
    let block_idx = table.find_block_idx(key);
    let mut block_iter =
        BlockIterator::create_and_seek_to_key(table.read_block_cached(block_idx)?, key);

    if !block_iter.is_valid() {
        // This means that the key is larger than the largest key in the block.
        // We need to seek to the first key in the next block.
        let block_idx = block_idx + 1;
        if block_idx < table.num_of_blocks() {
            block_iter =
                BlockIterator::create_and_seek_to_first(table.read_block_cached(block_idx)?);
        }
    }

    Ok((block_idx, block_iter))
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_idx, block_iter) = seek_table_to_first(&table)?;
        Ok(Self {
            table,
            block_idx,
            block_iter,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (block_idx, block_iter) = seek_table_to_first(&self.table)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_idx, block_iter) = seek_table_to_key(&table, key)?;
        Ok(Self {
            table,
            block_idx,
            block_iter,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = seek_table_to_key(&self.table, key)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            // If this is not the last block, we need to seek to the first key in the next block.
            if self.block_idx + 1 < self.table.num_of_blocks() {
                let next_block_idx = self.block_idx + 1;
                let next_block_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(next_block_idx)?,
                );
                self.block_idx = next_block_idx;
                self.block_iter = next_block_iter;
            }
        }
        Ok(())
    }
}
