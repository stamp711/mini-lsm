mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut b =
            BytesMut::with_capacity(self.data.len() + SIZEOF_U16 * self.offsets.len() + SIZEOF_U16);
        // Data
        b.extend_from_slice(&self.data);
        // Offsets
        for offset in &self.offsets {
            b.put_u16_le(*offset);
        }
        // num_of_elements
        b.put_u16_le(self.offsets.len() as u16);
        b.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16_le() as usize;

        let data_and_offsets = &data[..data.len() - SIZEOF_U16];
        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U16 * num_of_elements;

        let data = &data_and_offsets[..data_end];
        let offsets = &data_and_offsets[data_end..];

        Self {
            data: data.to_vec(),
            offsets: offsets
                .chunks_exact(2)
                .map(|mut chunk| chunk.get_u16_le())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests;
