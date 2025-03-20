use crate::{
    error::IggyError,
    prelude::{BytesSerializable, IggyMessage},
};
use bytes::{Buf, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// The wrapper on top of the collection of messages that are polled from the partition.
/// It consists of the following fields:
/// - `partition_id`: the identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `messages`: the collection of messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct PolledMessages {
    /// The identifier of the partition. If it's '0', then there's no partition assigned to the consumer group member.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The count of messages.
    pub count: u32,
    /// The collection of messages.
    pub messages: Vec<IggyMessage>,
}

impl PolledMessages {
    pub fn as_bytes(self) -> Bytes {
        todo!()
    }

    pub fn empty() -> Self {
        Self {
            partition_id: 0,
            current_offset: 0,
            count: 0,
            messages: Vec::new(),
        }
    }
}

impl BytesSerializable for PolledMessages {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let partition_id = u32::from_le_bytes(
            bytes[0..4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let current_offset = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let messages = IggyMessage::from_raw_bytes(bytes.slice(16..), count)?;
        Ok(Self {
            partition_id,
            current_offset,
            count,
            messages,
        })
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        todo!()
    }

    fn get_buffer_size(&self) -> u32 {
        unimplemented!();
    }
}
