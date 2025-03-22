use super::message_view_mut::IggyMessageViewMutIterator;
use crate::streaming::segments::indexes::IggyIndexesMut;
use bytes::{BufMut, BytesMut};
use iggy::models::messaging::{IggyMessagesBatch, INDEX_SIZE};
use iggy::prelude::*;
use lending_iterator::prelude::*;
use std::ops::Deref;

/// A container for mutable messages
#[derive(Debug, Default)]
pub struct IggyMessagesBatchMut {
    /// The byte-indexes of messages in the buffer, represented as array of u32's
    /// Each index points to the END position of a message (start of next message)
    indexes: IggyIndexesMut,
    /// The buffer containing the messages
    messages: BytesMut,
}

impl Sizeable for IggyMessagesBatchMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages.len() as u64)
    }
}

impl IggyMessagesBatchMut {
    /// Create an empty messages container with bytes capacity to prevent realloc()
    pub fn with_capacity(bytes_capacity: usize) -> Self {
        Self {
            indexes: IggyIndexesMut::with_capacity(bytes_capacity / INDEX_SIZE),
            messages: BytesMut::with_capacity(bytes_capacity),
        }
    }

    /// Create a new messages container from a existing buffer of bytes
    pub fn from_indexes_and_messages(indexes: IggyIndexesMut, messages: BytesMut) -> Self {
        Self { indexes, messages }
    }

    /// Create a new messages container from a slice of messages
    pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
        let mut messages_buffer = BytesMut::with_capacity(messages_size as usize);
        let mut indexes_buffer = IggyIndexesMut::with_capacity(messages.len());
        let mut position = 0;

        for message in messages {
            let bytes = message.to_bytes();
            messages_buffer.put_slice(&bytes);
            indexes_buffer.insert(0, position, 0);
            position += message.get_size_bytes().as_bytes_u32();
        }

        Self::from_indexes_and_messages(indexes_buffer, messages_buffer)
    }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.messages)
    }

    /// Create a lending iterator over immutable messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        debug_assert_eq!(self.indexes.len() % INDEX_SIZE, 0);
        self.indexes.len() as u32 / INDEX_SIZE as u32
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.messages.len() as u32
    }

    /// Makes the messages batch immutable
    pub fn make_immutable(self) -> IggyMessagesBatch {
        let messages_count = self.count();
        let indexes = self.indexes.make_immutable();
        let messages = self.messages.freeze();
        IggyMessagesBatch::new(indexes, messages, messages_count)
    }

    /// Prepares all messages in the batch for persistence by setting their offsets, timestamps,
    /// and other necessary fields. Returns the prepared, immutable messages.
    pub fn prepare_for_persistence(
        self,
        start_offset: u64,
        base_offset: u64,
        current_position: u32,
        segment_indexes: &mut IggyIndexesMut,
    ) -> IggyMessagesBatch {
        let messages_count = self.count();
        let timestamp = IggyTimestamp::now().as_micros();

        let mut curr_abs_offset = base_offset;
        let mut current_position = current_position;
        let (mut indexes, mut messages) = self.decompose();
        let mut iter = IggyMessageViewMutIterator::new(&mut messages);
        let mut curr_rel_offset: u32 = 0;

        // TODO: fix crash when idx cache is disabled
        while let Some(mut message) = iter.next() {
            message.header_mut().set_offset(curr_abs_offset);
            message.header_mut().set_timestamp(timestamp);
            message.update_checksum();

            current_position += message.size() as u32;

            indexes.set_offset_at(curr_rel_offset, curr_abs_offset as u32);
            indexes.set_position_at(curr_rel_offset, current_position);
            indexes.set_timestamp_at(curr_rel_offset, timestamp);

            println!(
                "Message {} offset: {}, position: {}, timestamp: {}",
                curr_rel_offset, curr_abs_offset, current_position, timestamp
            );

            curr_abs_offset += 1;
            curr_rel_offset += 1;
        }
        indexes.set_unsaved_messages_count(messages_count);

        segment_indexes.concatenate(indexes);

        let relative_offset = base_offset - start_offset;
        let indexes = segment_indexes
            .slice_by_offset(
                relative_offset as u32,
                relative_offset as u32 + messages_count,
            )
            .unwrap();

        IggyMessagesBatch::new(indexes, messages.freeze(), messages_count)
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn decompose(self) -> (IggyIndexesMut, BytesMut) {
        (self.indexes, self.messages)
    }
}

impl Deref for IggyMessagesBatchMut {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}

/// Iterator over messages batch that yields immutable message views
pub struct IggyMessageViewIterator<'a> {
    buffer: &'a BytesMut,
    position: usize,
}

impl<'a> IggyMessageViewIterator<'a> {
    pub fn new(buffer: &'a BytesMut) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<'a> Iterator for IggyMessageViewIterator<'a> {
    type Item = IggyMessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let remaining = &self.buffer[self.position..];
        if remaining.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return None;
        }

        let header_view = IggyMessageHeaderView::new(remaining);
        let message_size = header_view.payload_length() as usize
            + header_view.user_headers_length() as usize
            + IGGY_MESSAGE_HEADER_SIZE as usize;

        if message_size > remaining.len() {
            return None;
        }

        let message_view =
            IggyMessageView::new(&self.buffer[self.position..self.position + message_size]);
        self.position += message_size;

        Some(message_view)
    }
}
