use super::{Partitioning, PartitioningKind};
use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, SEND_MESSAGES_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::MAX_PAYLOAD_SIZE;
use crate::models::messaging::{IggyMessage, IggyMessagesBatch, INDEX_SIZE};
use crate::prelude::{IggyMessageViewIterator, IGGY_MESSAGE_HEADER_SIZE};
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// `SendMessages` command is used to send messages to a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitioning` - to which partition the messages should be sent - either provided by the client or calculated by the server.
/// - `batch` - collection of messages to be sent.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SendMessages {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// To which partition the messages should be sent - either provided by the client or calculated by the server.
    pub partitioning: Partitioning,
    /// Messages collection
    pub batch: IggyMessagesBatch,
}

impl SendMessages {
    pub fn as_bytes(
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &[IggyMessage],
    ) -> Bytes {
        let stream_id_size = stream_id.get_buffer_size();
        let topic_id_size = topic_id.get_buffer_size();
        let partitioning_size = partitioning.get_buffer_size();
        let messages_count = messages.len() as u32;
        let messages_count_size = std::mem::size_of::<u32>() as u32;
        let indexes_size = messages_count * INDEX_SIZE as u32;

        let total_size = stream_id_size
            + topic_id_size
            + partitioning_size
            + messages_count_size
            + indexes_size
            + messages
                .iter()
                .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
                .sum::<u32>();

        let mut bytes = BytesMut::with_capacity(total_size as usize);

        stream_id.write_to_buffer(&mut bytes);
        topic_id.write_to_buffer(&mut bytes);
        partitioning.write_to_buffer(&mut bytes);
        bytes.put_u32_le(messages_count);

        let mut current_position = bytes.len();

        bytes.put_bytes(0, indexes_size as usize);

        let mut msgs_size: u32 = 0;
        for message in messages.iter() {
            message.write_to_buffer(&mut bytes);
            msgs_size += message.get_size_bytes().as_bytes_u64() as u32;
            write_value_at(&mut bytes, 0u64.to_le_bytes(), current_position);
            write_value_at(&mut bytes, msgs_size.to_le_bytes(), current_position + 4);
            write_value_at(&mut bytes, 0u64.to_le_bytes(), current_position + 8);
            current_position += INDEX_SIZE;
        }

        let result = bytes.freeze();

        debug_assert_eq!(
            total_size as usize,
            result.len(),
            "Calculated size doesn't match actual bytes length",
        );

        result
    }
}

fn write_value_at<const N: usize>(slice: &mut [u8], value: [u8; N], position: usize) {
    let slice = &mut slice[position..position + N];
    let ptr = slice.as_mut_ptr();
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, N);
    }
}

impl Default for SendMessages {
    fn default() -> Self {
        SendMessages {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partitioning: Partitioning::default(),
            batch: IggyMessagesBatch::empty(),
        }
    }
}

impl Command for SendMessages {
    fn code(&self) -> u32 {
        SEND_MESSAGES_CODE
    }
}

impl Validatable<IggyError> for SendMessages {
    fn validate(&self) -> Result<(), IggyError> {
        if self.partitioning.value.len() > 255
            || (self.partitioning.kind != PartitioningKind::Balanced
                && self.partitioning.value.is_empty())
        {
            return Err(IggyError::InvalidKeyValueLength);
        }

        self.batch.validate()?;

        Ok(())
    }
}

impl BytesSerializable for SendMessages {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(_bytes: Bytes) -> Result<SendMessages, IggyError> {
        panic!("should not be used")
    }
}

impl Display for SendMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let messages_count = self.batch.count();
        let messages_size = self.batch.size();
        write!(
            f,
            "{}|{}|{}|messages_count:{}|messages_size:{}",
            self.stream_id, self.topic_id, self.partitioning, messages_count, messages_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_of_type_balanced_should_have_empty_value() {
        let key = Partitioning::balanced();
        assert_eq!(key.kind, PartitioningKind::Balanced);
        assert_eq!(key.length, 0);
        assert!(key.value.is_empty());
        assert_eq!(
            PartitioningKind::from_code(1).unwrap(),
            PartitioningKind::Balanced
        );
    }

    #[test]
    fn key_of_type_partition_should_have_value_of_const_length_4() {
        let partition_id = 1234u32;
        let key = Partitioning::partition_id(partition_id);
        assert_eq!(key.kind, PartitioningKind::PartitionId);
        assert_eq!(key.length, 4);
        assert_eq!(key.value, partition_id.to_le_bytes());
        assert_eq!(
            PartitioningKind::from_code(2).unwrap(),
            PartitioningKind::PartitionId
        );
    }

    #[test]
    fn key_of_type_messages_key_should_have_value_of_dynamic_length() {
        let messages_key = "hello world";
        let key = Partitioning::messages_key_str(messages_key).unwrap();
        assert_eq!(key.kind, PartitioningKind::MessagesKey);
        assert_eq!(key.length, messages_key.len() as u8);
        assert_eq!(key.value, messages_key.as_bytes());
        assert_eq!(
            PartitioningKind::from_code(3).unwrap(),
            PartitioningKind::MessagesKey
        );
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_0_should_fail() {
        let messages_key = "";
        let key = Partitioning::messages_key_str(messages_key);
        assert!(key.is_err());
    }

    #[test]
    fn key_of_type_messages_key_that_has_length_greater_than_255_should_fail() {
        let messages_key = "a".repeat(256);
        let key = Partitioning::messages_key_str(&messages_key);
        assert!(key.is_err());
    }
}
