use bytes::BufMut;

use super::Block;
use super::SIZEOF_U16;

/// Builds a block.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
    current_block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(block_size),
            offsets: Default::default(),
            block_size,
            current_block_size: SIZEOF_U16, // num_of_elements is u16
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.current_block_size
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// Added keys must be in sorted order.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        assert!(key.len() < u16::MAX as usize, "key size exceeds limit");
        assert!(value.len() < u16::MAX as usize, "value size exceeds limit");

        let entry_size = SIZEOF_U16 + key.len() + SIZEOF_U16 + value.len(); // Encoded key_len & value_len are u16
        let entry_total_size = entry_size + SIZEOF_U16; // Plus the encoded offset (u16)

        // Check if the block is full
        if !self.is_empty() && self.current_block_size + entry_total_size > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.put_u16_le(key.len() as _);
        self.data.extend(key);
        self.data.put_u16_le(value.len() as _);
        self.data.extend(value);

        self.current_block_size += entry_total_size;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
