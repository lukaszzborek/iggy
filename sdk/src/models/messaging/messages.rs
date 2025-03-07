use super::{message_view::IggyMessageViewIterator, IggyMessage, IggyMessagesMut};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// An immutable messages container that holds a buffer of messages
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessages {
    /// The number of messages in the buffer
    #[serde(skip)]
    count: u32,
    /// The buffer containing the messages
    buffer: Bytes,
}

impl IggyMessages {
    /// Create a new messages container from a buffer
    pub fn new(buffer: Bytes) -> Self {
        Self { buffer, count: 0 }
    }

    pub fn from_messages(messages: &[IggyMessage]) -> Self {
        let mut buffer = BytesMut::new();
        for message in messages {
            buffer.extend_from_slice(&message.to_bytes());
        }
        Self::new(buffer.freeze())
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Get access to the underlying buffer
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

impl BytesSerializable for IggyMessages {
    fn to_bytes(&self) -> Bytes {
        self.buffer.clone()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut messages_count = 0;
        let iterator = IggyMessageViewIterator::new(&bytes);

        for result in iterator {
            result?;
            messages_count += 1;
        }

        Ok(Self {
            buffer: bytes,
            count: messages_count,
        })
    }
}

impl Sizeable for IggyMessages {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl From<IggyMessagesMut> for IggyMessages {
    fn from(messages: IggyMessagesMut) -> Self {
        let count = messages.count();
        let buffer = messages.into_inner().freeze();

        Self { buffer, count }
    }
}
