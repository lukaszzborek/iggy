use super::message_view_mut::IggyMessageViewMutIterator;
use crate::streaming::segments::indexes::IggyIndexesMut;
use crate::streaming::utils::random_id;
use bytes::{BufMut, BytesMut};
use iggy::messages::MAX_PAYLOAD_SIZE;
use iggy::models::messaging::{IggyMessagesBatch, INDEX_SIZE};
use iggy::prelude::*;
use iggy::utils::timestamp::IggyTimestamp;
use lending_iterator::prelude::*;
use std::ops::Deref;
use tracing::trace;

/// A container for mutable messages that are being prepared for persistence.
///
/// `IggyMessagesBatchMut` holds both the raw message data in a `BytesMut` buffer
/// and the corresponding index data that allows for efficient message lookup.
#[derive(Debug, Default)]
pub struct IggyMessagesBatchMut {
    /// The index data for all messages in the buffer
    indexes: IggyIndexesMut,

    /// The buffer containing the serialized message data
    messages: BytesMut,
}

impl Sizeable for IggyMessagesBatchMut {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages.len() as u64)
    }
}

impl IggyMessagesBatchMut {
    /// Creates an empty messages container with the specified capacity to avoid reallocations.
    ///
    /// # Arguments
    ///
    /// * `bytes_capacity` - The expected total size of all messages in bytes
    pub fn with_capacity(bytes_capacity: usize) -> Self {
        let index_capacity = bytes_capacity / INDEX_SIZE + 1; // Add 1 to avoid rounding down to 0
        Self {
            indexes: IggyIndexesMut::with_capacity(index_capacity),
            messages: BytesMut::with_capacity(bytes_capacity),
        }
    }

    /// Creates a new messages container from existing index and message buffers.
    ///
    /// # Arguments
    ///
    /// * `indexes` - Preprocessed index data
    /// * `messages` - Serialized message data
    pub fn from_indexes_and_messages(indexes: IggyIndexesMut, messages: BytesMut) -> Self {
        Self { indexes, messages }
    }

    /// Creates a new messages container from a slice of IggyMessage objects.
    ///
    /// # Arguments
    ///
    /// * `messages` - Slice of message objects to store
    /// * `messages_size` - Total size of all messages in bytes
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

    /// Creates a lending iterator that yields mutable views of messages.
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.messages)
    }

    /// Creates an iterator that yields immutable views of messages.
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Returns the number of messages in the batch.
    pub fn count(&self) -> u32 {
        let index_count = self.indexes.len() as u32 / INDEX_SIZE as u32;
        debug_assert_eq!(
            self.indexes.len() % INDEX_SIZE,
            0,
            "Index buffer length must be a multiple of INDEX_SIZE"
        );
        index_count
    }

    /// Returns the total size of all messages in bytes.
    pub fn size(&self) -> u32 {
        self.messages.len() as u32
    }

    /// Prepares all messages in the batch for persistence by setting their offsets,
    /// timestamps, and other necessary fields.
    ///
    /// # Arguments
    ///
    /// * `start_offset` - The starting offset of the segment
    /// * `base_offset` - The base offset for this batch of messages
    /// * `current_position` - The current position in the segment
    /// * `segment_indexes` - The segment's index data, which will be updated
    ///
    /// # Returns
    ///
    /// An immutable `IggyMessagesBatch` ready for persistence
    pub fn prepare_for_persistence(
        self,
        start_offset: u64,
        base_offset: u64,
        current_position: u32,
    ) -> IggyMessagesBatch {
        let messages_count = self.count();
        if messages_count == 0 {
            return IggyMessagesBatch::empty();
        }

        let timestamp = IggyTimestamp::now().as_micros();

        let (mut indexes, mut messages) = self.decompose();

        let mut curr_abs_offset = base_offset;
        let mut curr_position = current_position;

        let mut iter = IggyMessageViewMutIterator::new(&mut messages);
        let mut curr_rel_offset: u32 = 0;

        while let Some(mut message) = iter.next() {
            message.header_mut().set_offset(curr_abs_offset);
            message.header_mut().set_timestamp(timestamp);
            if message.header().id() == 0 {
                message.header_mut().set_id(random_id::get_uuid());
            }
            message.update_checksum();

            let message_size = message.size() as u32;
            curr_position += message_size;

            let relative_offset = (curr_abs_offset - start_offset) as u32;
            indexes.set_offset_at(curr_rel_offset, relative_offset);
            indexes.set_position_at(curr_rel_offset, curr_position);
            indexes.set_timestamp_at(curr_rel_offset, timestamp);

            curr_abs_offset += 1;
            curr_rel_offset += 1;
        }

        IggyMessagesBatch::new(
            indexes.make_immutable(current_position),
            messages.freeze(),
            messages_count,
        )
    }

    /// Returns the first timestamp in the batch
    pub fn first_timestamp(&self) -> u64 {
        IggyMessageView::new(&self.messages).header().timestamp()
    }

    /// Returns the last timestamp in the batch
    pub fn last_timestamp(&self) -> u64 {
        let last_message_offset = self.indexes.get(self.count() - 1).unwrap().offset();
        IggyMessageView::new(&self.messages[last_message_offset as usize..])
            .header()
            .timestamp()
    }

    /// Checks if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Decomposes the batch into its constituent parts.
    pub fn decompose(self) -> (IggyIndexesMut, BytesMut) {
        (self.indexes, self.messages)
    }

    /// Helper method to read a position (u32) from the byte array at the given index
    fn position_at(&self, position_index: u32) -> u32 {
        if let Some(index) = self.indexes.get(position_index) {
            index.position()
        } else {
            0
        }
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
        if index >= self.count() as usize {
            return None;
        }

        // TODO: refactor this
        let start_position = if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize
        };

        let end_position = if index == self.count() as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize
        };

        debug_assert!(
            start_position <= self.messages.len(),
            "Start message position {} exceeds buffer length {}, index: {}, self.count: {}",
            start_position,
            self.messages.len(),
            index,
            self.count(),
        );
        debug_assert!(
            end_position <= self.messages.len(),
            "End message position {} exceeds buffer length {}, index: {}, self.count: {}",
            end_position,
            self.messages.len(),
            index,
            self.count(),
        );

        Some(IggyMessageView::new(
            &self.messages[start_position..end_position],
        ))
    }
}

impl Validatable<IggyError> for IggyMessagesBatchMut {
    fn validate(&self) -> Result<(), IggyError> {
        if self.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        let indexes_count = self.indexes.count();
        let indexes_size = self.indexes.size();

        if indexes_size % INDEX_SIZE as u32 != 0 {
            tracing::error!(
                "Indexes size {} is not a multiple of index size {}",
                indexes_size,
                INDEX_SIZE
            );
            return Err(IggyError::InvalidIndexesByteSize(indexes_size));
        }

        if indexes_count != self.count() {
            tracing::error!(
                "Indexes count {} does not match messages count {}",
                indexes_count,
                self.count()
            );
            return Err(IggyError::InvalidIndexesCount(indexes_count, self.count()));
        }

        let mut messages_count = 0;
        let mut messages_size = 0;

        for i in 0..self.count() {
            if let Some(index_view) = self.indexes.get(i) {
                if index_view.offset() != 0 {
                    tracing::error!("Non-zero offset {} at index: {}", index_view.offset(), i);
                    return Err(IggyError::NonZeroOffset(index_view.offset() as u64, i));
                }
                if index_view.timestamp() != 0 {
                    tracing::error!(
                        "Non-zero timestamp {} at index: {}",
                        index_view.timestamp(),
                        i
                    );
                    return Err(IggyError::NonZeroTimestamp(index_view.timestamp(), i));
                }
            } else {
                tracing::error!("Index {} is missing", i);
                return Err(IggyError::MissingIndex(i));
            }

            if let Some(message) = self.get(i as usize) {
                if message.payload().len() as u32 > MAX_PAYLOAD_SIZE {
                    tracing::error!(
                        "Message payload size {} B exceeds maximum payload size {} B",
                        message.payload().len(),
                        MAX_PAYLOAD_SIZE
                    );
                    return Err(IggyError::TooBigMessagePayload);
                }

                if let Some(user_headers) = message.user_headers() {
                    tracing::error!("message user headers size {}", user_headers.len());
                    if user_headers.len() as u32 > MAX_USER_HEADERS_SIZE {
                        tracing::error!(
                            "Message user headers size {} B exceeds maximum size {} B",
                            user_headers.len(),
                            MAX_USER_HEADERS_SIZE
                        );
                        return Err(IggyError::TooBigHeadersPayload);
                    }
                }

                messages_size += message.size();
                messages_count += 1;
            } else {
                tracing::error!("Missing index {}", i);
                return Err(IggyError::MissingIndex(i));
            }
        }

        if indexes_count != messages_count {
            tracing::error!(
                "Indexes count {} does not match messages count {}",
                indexes_count,
                messages_count
            );
            return Err(IggyError::InvalidMessagesCount);
        }

        if messages_size != self.messages.len() as u32 {
            tracing::error!(
                "Messages size {} does not match messages buffer size {}",
                messages_size,
                self.messages.len() as u64
            );
            return Err(IggyError::InvalidMessagesSize(
                messages_size,
                self.messages.len() as u32,
            ));
        }

        Ok(())
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
            trace!(
                "Buffer too small for message header at position {}, buffer len: {}",
                self.position,
                self.buffer.len()
            );
            return None;
        }

        let header_view = IggyMessageHeaderView::new(remaining);
        let message_size = header_view.payload_length() as usize
            + header_view.user_headers_length() as usize
            + IGGY_MESSAGE_HEADER_SIZE as usize;

        if message_size > remaining.len() {
            trace!(
                "Message size {} exceeds remaining buffer size {} at position {}",
                message_size,
                remaining.len(),
                self.position
            );
            return None;
        }

        let message_view =
            IggyMessageView::new(&self.buffer[self.position..self.position + message_size]);
        self.position += message_size;

        Some(message_view)
    }
}
