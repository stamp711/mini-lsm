#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::{Bound, RangeBounds};

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    bound: (Bound<Bytes>, Bound<Bytes>),
}

impl LsmIterator {
    pub(crate) fn new_with_upper_bound(
        inner: LsmIteratorInner,
        upper_bound: Bound<Bytes>,
    ) -> Result<Self> {
        let mut this = Self {
            inner,
            bound: (Bound::Unbounded, upper_bound),
        };
        // Handle the case where the first value is empty.
        if this.is_valid() && this.value().is_empty() {
            this.next()?;
        }

        Ok(this)
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.inner.is_valid() && self.bound.contains(self.inner.key())
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        while self.is_valid() && self.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.iter.is_valid() {
            self.iter.next()
        } else {
            Ok(())
        }
    }
}
