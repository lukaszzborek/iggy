use super::{
    message_view::IggyMessageViewMutIterator, IggyMessage, IggyMessageViewIterator, IggyMessages,
};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// A container for mutable messages
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessagesMut {
    /// The number of messages in the buffer
    #[serde(skip)]
    count: u32,
    /// The buffer containing the messages
    buffer: BytesMut,
}

impl BytesSerializable for IggyMessagesMut {
    fn to_bytes(&self) -> Bytes {
        self.buffer.clone().freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Self {
            buffer: BytesMut::from(&bytes[..]),
            count: 0,
        })
    }
}

impl Sizeable for IggyMessagesMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl IggyMessagesMut {
    /// Create a new messages container from a buffer
    pub fn new(buffer: BytesMut) -> Self {
        Self { buffer, count: 0 }
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut buffer = BytesMut::with_capacity(messages_size as usize);
        for message in messages {
            buffer.extend_from_slice(&message.to_bytes());
        }
        Self::new(buffer)
    }

    /// Extends the messages container with another set of mutable messages by consuming them
    pub fn extend(&mut self, messages: IggyMessagesMut) {
        let count = messages.count();
        let buffer = messages.into_inner();
        self.buffer.put(buffer);
        self.count += count;
    }

    /// Sets the capacity of the messages container
    pub fn set_capacity(&mut self, capacity: u32) {
        self.buffer.reserve(capacity as usize);
    }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.buffer)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Set the message count
    pub fn set_count(&mut self, count: u32) {
        self.count = count;
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Get the buffer
    pub fn into_inner(self) -> BytesMut {
        self.buffer
    }
}
