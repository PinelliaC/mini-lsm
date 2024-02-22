mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.

/**
 * ----------------------------------------------------------------------------------------------------
 * |             Data Section             |              Offset Section             |      Extra      |
 * ----------------------------------------------------------------------------------------------------
 * | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
 * ----------------------------------------------------------------------------------------------------
 *
 *
 * -----------------------------------------------------------------------
 * |                           Entry #1                            | ... |
 * -----------------------------------------------------------------------
 * | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
 * -----------------------------------------------------------------------
 */
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut result = self.data.clone();
        let number_of_elements = self.offsets.len() as u16;
        for offset in &self.offsets {
            result.put_u16(*offset);
        }
        result.put_u16(number_of_elements);
        result.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let number_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // offset section
        let data_end = data.len() - SIZEOF_U16 - number_of_elements * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // data section
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}
