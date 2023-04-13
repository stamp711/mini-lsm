use anyhow::Result;

use super::StorageIterator;

enum Selection {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    current: Selection,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut this = Self {
            a,
            b,
            current: Selection::A, // Does not matter for now.
        };
        this.current = this.select();
        Ok(this)
    }

    fn select(&self) -> Selection {
        let a_key = self.a.is_valid().then_some(()).map(|()| self.a.key());
        let b_key = self.b.is_valid().then_some(()).map(|()| self.b.key());
        match (a_key, b_key) {
            (None, _) => Selection::B,
            (Some(_), None) => Selection::A,
            (Some(ak), Some(bk)) => {
                if ak <= bk {
                    Selection::A
                } else {
                    Selection::B
                }
            }
        }
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        match self.current {
            Selection::A => self.a.key(),
            Selection::B => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.current {
            Selection::A => self.a.value(),
            Selection::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.current {
            Selection::A => self.a.is_valid(),
            Selection::B => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.current {
            Selection::A => {
                // Need to check if B has the same key and skip.
                let current_key = self.a.key();
                if self.b.is_valid() && self.b.key() == current_key {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            Selection::B => {
                self.b.next()?;
            }
        }
        self.current = self.select();
        Ok(())
    }
}
