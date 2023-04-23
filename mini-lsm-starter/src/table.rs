#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::fs::File;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

const SIZEOF_U32: usize = std::mem::size_of::<u32>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block. Encoded as u32.
    pub offset: usize,
    /// Length of this data block. Encoded as u32.
    pub len: usize,
    /// The first key of the data block. Encoded as key_length (u16) + key ([u8]).
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        // Calculate the estimated size of encoded block.
        let mut estimated_size = 0;
        for meta in block_meta {
            assert!(meta.offset < u32::MAX as usize, "offset too large for u32");
            estimated_size += std::mem::size_of::<u32>();
            assert!(meta.len < u32::MAX as usize, "len too large for u32");
            estimated_size += std::mem::size_of::<u32>();
            assert!(
                meta.first_key.len() < u16::MAX as usize,
                "key too large for u16"
            );
            estimated_size += std::mem::size_of::<u16>();
            estimated_size += meta.first_key.len();
        }

        buf.reserve(estimated_size);

        for meta in block_meta {
            buf.put_u32_le(meta.offset as u32);
            buf.put_u32_le(meta.len as u32);
            buf.put_u16_le(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut res = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u32_le() as usize;
            let len = buf.get_u32_le() as usize;
            let key_len = buf.get_u16_le() as usize;
            let first_key = buf.copy_to_bytes(key_len);
            res.push(BlockMeta {
                offset,
                len,
                first_key,
            });
        }
        res
    }
}

/// A file object.
pub struct FileObject(File, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut res = vec![0; len as usize];
        self.0.read_exact_at(&mut res, offset)?;
        Ok(res)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        Ok(Self(
            File::options().read(true).open(path)?,
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_size = file.size();
        let block_meta_offset = file
            .read(file_size - SIZEOF_U32 as u64, SIZEOF_U32 as u64)?
            .as_slice()
            .get_u32_le() as usize;
        let block_meta_len = file_size - SIZEOF_U32 as u64 - block_meta_offset as u64;

        let block_metas = BlockMeta::decode_block_meta(
            file.read(block_meta_offset as u64, block_meta_len)?
                .as_slice(),
        );

        Ok(Self {
            file,
            block_metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_metas[block_idx];
        let buf = self.file.read(meta.offset as u64, meta.len as u64)?;
        Ok(Arc::new(Block::decode(&buf)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut l = 0;
        let mut r = self.block_metas.len();
        // Target block is in [l, r).
        while r - l > 1 {
            let mid = (l + r) / 2;
            if self.block_metas[mid].first_key > key {
                r = mid;
            } else {
                l = mid
            }
        }
        // Only one block left.
        l
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
