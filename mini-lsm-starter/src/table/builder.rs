use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, SsTable};
use crate::{block::BlockBuilder, lsm_storage::BlockCache, table::FileObject};

const BLOCK_ALIGN: usize = 4 * 1024;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    block_size: usize,
    data: Vec<u8>,
    current_block_builder: BlockBuilder,
    current_block_first_key: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            block_size,
            data: vec![],
            current_block_builder: BlockBuilder::new(block_size),
            current_block_first_key: vec![],
        }
    }

    /// Adds a key-value pair to SSTable
    /// Added keys must be in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty());

        let added = self.current_block_builder.add(key, value);

        if self.current_block_first_key.is_empty() {
            self.current_block_first_key = key.to_vec();
            assert!(added); // Adding the first key to a block must succeed.
        }

        if !added {
            self.finish_current_block();
            // Add to the new block.
            let added = self.current_block_builder.add(key, value);
            assert!(added);
            self.current_block_first_key = key.to_vec();
        }
    }

    fn finish_current_block(&mut self) {
        assert!(!self.current_block_first_key.is_empty());
        assert!(!self.current_block_builder.is_empty());

        let builder = std::mem::replace(
            &mut self.current_block_builder,
            BlockBuilder::new(self.block_size),
        );
        let block = builder.build();
        let encoded = block.encode();
        let meta = BlockMeta {
            offset: self.data.len(),
            len: encoded.len(),
            first_key: std::mem::take(&mut self.current_block_first_key).into(),
        };
        self.data.extend(encoded);
        self.meta.push(meta);

        // Round up data to the next block alignment.
        let padding = (BLOCK_ALIGN - self.data.len() % BLOCK_ALIGN) % BLOCK_ALIGN;
        self.data.resize(self.data.len() + padding, 0);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        let builder_size = if self.current_block_builder.is_empty() {
            0
        } else {
            self.current_block_builder.estimated_size()
        };
        self.data.len() + builder_size
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.current_block_builder.is_empty() {
            self.finish_current_block();
        }

        let block_meta_offset = self.data.len();
        assert!(
            block_meta_offset < u32::MAX as usize,
            "meta block offset is too large"
        );
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32_le(block_meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
