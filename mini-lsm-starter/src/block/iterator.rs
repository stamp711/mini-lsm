use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut this = Self::new(block);
        this.seek_to_first();
        this
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut this = Self::new(block);
        this.seek_to_key(key);
        this
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid iterator");
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid iterator");
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
        } else {
            let (key, value) = self.block.get_entry(idx);
            self.key = key.to_vec();
            self.value = value.to_vec();
        }
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut l = 0;
        let mut r = self.block.offsets.len();
        while l < r {
            let mid = (l + r) / 2;
            self.seek_to_idx(mid);
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => l = mid + 1,
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => r = mid,
            }
        }
        assert_eq!(l, r);
        self.seek_to_idx(l);
    }
}
