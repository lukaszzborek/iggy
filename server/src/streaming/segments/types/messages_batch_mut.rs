use super::message_view_mut::IggyMessageViewMutIterator;
use crate::streaming::segments::indexes::IggyIndexesMut;
use bytes::{BufMut, Bytes, BytesMut};
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
    /// Create a new messages container from a existing buffer of bytes
    pub fn from_bytes(indexes: IggyIndexesMut, messages: BytesMut) -> Self {
        Self { indexes, messages }
    }

    // /// Create a new messages container from a slice of messages
    // pub fn from_messages(messages: &[IggyMessage], messages_size: u32) -> Self {
    //     let mut buffer = BytesMut::with_capacity(messages_size as usize);
    //     for message in messages {
    //         let bytes = message.to_bytes();
    //         buffer.put_slice(&bytes);
    //     }

    //     Self::from_bytes(buffer, messages.len() as u32)
    // }

    /// Create a lending iterator over mutable messages
    pub fn iter_mut(&mut self) -> IggyMessageViewMutIterator {
        IggyMessageViewMutIterator::new(&mut self.messages)
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

            curr_abs_offset += 1;
            curr_rel_offset += 1;
        }
        indexes.set_unsaved_messages_count(messages_count);

        segment_indexes.append(indexes);

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

// impl From<&[IggyMessage]> for IggyMessagesBatchMut {
//     fn from(messages: &[IggyMessage]) -> Self {
//         let messages_size: u32 = messages
//             .iter()
//             .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
//             .sum();
//         Self::from_messages(messages, messages_size)
//     }
// }
