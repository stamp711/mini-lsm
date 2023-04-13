use std::cmp;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse()) // Reverse the order to make a min-heap.
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let iters: BinaryHeap<_> = iters
            .into_iter()
            .enumerate()
            .filter_map(|(idx, iter)| iter.is_valid().then_some(HeapWrapper(idx, iter)))
            .collect();
        let mut this = MergeIterator {
            iters,
            current: None,
        };
        // Seek to first key. In case of all iterators being invalid, this will be `None`, indicating the merge iterator is invalid.
        this.seek_to_next_in_heap();
        this
    }

    fn seek_to_next_in_heap(&mut self) {
        assert!(self.current.is_none());
        self.current = self.iters.pop();
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|hw| hw.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current_key = self.current.as_mut().unwrap().1.key();

        // Skip the current key in all iterators other than the current iterator.
        while let Some(mut pm) = self.iters.peek_mut() {
            assert!(pm.1.key() >= current_key);
            if pm.1.key() == current_key {
                // Skip this key in this iter.
                if let e @ Err(_) = pm.1.next() {
                    // If seek errors, remove it from the heap and report error.
                    return e;
                }
                // If this iter becomes invalid, remove it from the heap.
                if !pm.1.is_valid() {
                    PeekMut::pop(pm);
                }
            } else {
                // Remaining keys are larger than current key.
                break;
            }
        }

        // Seek the current iterator to the next key and put it back to the heap.
        let mut current = self.current.take().unwrap();
        current.1.next()?;
        if current.1.is_valid() {
            self.iters.push(current);
        }
        self.seek_to_next_in_heap();
        Ok(())
    }
}
