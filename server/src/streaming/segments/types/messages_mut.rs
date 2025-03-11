use super::{message_view_mut::IggyMessageViewMutIterator, IggyMessages};
use crate::streaming::segments::Index;
use bytes::{BufMut, BytesMut};
use iggy::prelude::*;
use lending_iterator::prelude::*;
use std::ops::Deref;

/// A container for mutable messages
#[derive(Debug, PartialEq, Default)]
pub struct IggyMessagesMut {
    /// The number of messages in the buffer
    count: u32,
    /// The buffer containing the messages
    buffer: BytesMut,
}

impl Sizeable for IggyMessagesMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl IggyMessagesMut {
    /// Create a new messages container with a specified capacity
    pub fn with_capacity(capacity: u32) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity as usize),
            count: 0,
        }
    }

    /// Create a new messages container from a existing buffer of bytes
    pub fn from_bytes(bytes: BytesMut, messages_count: u32) -> Self {
        Self {
            buffer: bytes,
            count: messages_count,
        }
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut messages_mut = Self::with_capacity(messages_size);

        for message in messages {
            messages_mut.buffer.extend_from_slice(&message.to_bytes());
        }
        messages_mut.count = messages.len() as u32;
        messages_mut
    }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Prepares all messages in the batch for persistence by setting their offsets, timestamps,
    /// and other necessary fields. This method should be called before the messages are written to disk.
    /// Returns the prepared, immutable messages.
    pub fn prepare_for_persistence(
        mut self,
        base_offset: u64,
        timestamp: u64,
        current_position: u32,
        indexes: &mut Vec<Index>,
    ) -> IggyMessages {
        let max_messages = self.count;
        let mut current_offset = base_offset;
        let mut current_position = current_position;
        let mut iter = self.iter_mut();
        let mut processed_count = 0;

        while let Some(mut message) = iter.next() {
            debug_assert!(processed_count <= max_messages);
            processed_count += 1;

            message.msg_header_mut().set_offset(current_offset);
            message.msg_header_mut().set_timestamp(timestamp);

            message.update_checksum();

            current_offset += 1;
            current_position += message.size() as u32;

            indexes.push(Index {
                offset: (current_offset - base_offset) as u32,
                position: current_position,
                timestamp,
            });
        }

        let buffer = self.buffer.freeze();
        IggyMessages::new(buffer, self.count)
    }
}

impl Deref for IggyMessagesMut {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl From<&[IggyMessage]> for IggyMessagesMut {
    fn from(messages: &[IggyMessage]) -> Self {
        let messages_size: u32 = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
            .sum();
        Self::from_messages(messages, messages_size)
    }
}
