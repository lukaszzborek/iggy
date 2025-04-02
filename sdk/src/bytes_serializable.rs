use crate::error::IggyError;
use bytes::{Bytes, BytesMut};

/// The trait represents the logic responsible for serializing and deserializing the struct to and from bytes.
pub trait BytesSerializable {
    /// Serializes the struct to bytes.
    fn to_bytes(&self) -> Bytes;

    /// Deserializes the struct from bytes.
    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized;

    /// Write the struct to a buffer.
    fn write_to_buffer(&self, _buf: &mut BytesMut) {
        unimplemented!();
    }

    /// Get the byte-size of the struct.
    fn get_buffer_size(&self) -> usize {
        unimplemented!();
    }
}
