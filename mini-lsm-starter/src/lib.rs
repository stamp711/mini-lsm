pub mod block;
pub mod iterators;
pub mod lsm_iterator;
pub mod lsm_storage;
pub mod mem_table;
pub mod table;

#[cfg(test)]
mod tests;

pub(crate) mod util {
    use std::ops::Bound;

    use bytes::Bytes;

    pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}
