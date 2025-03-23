use super::{IggyIndexes, IggyMessageView, IggyMessageViewIterator};
use crate::{
    error::IggyError,
    models::messaging::INDEX_SIZE,
    prelude::{BytesSerializable, IggyByteSize, Sizeable},
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, Index};

/// An immutable messages container that holds a buffer of messages
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessagesBatch {
    /// The number of messages in the batch
    count: u32,
    /// The byte-indexes of messages in the buffer, represented as array of u32's
    /// Offsets are relative
    /// Each index position points to the END position of a message (start of next message)
    indexes: IggyIndexes,
    /// The buffer containing the messages
    messages: Bytes,
}

impl IggyMessagesBatch {
    /// Create a new messages container from a buffer
    pub fn new(indexes: IggyIndexes, messages: Bytes, count: u32) -> Self {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(indexes.len() % INDEX_SIZE, 0);
            let indexes_count = indexes.len() / INDEX_SIZE;
            debug_assert_eq!(indexes_count, count as usize);

            if count > 0 {
                let first_position = indexes.get(0).unwrap().position();

                let iter = IggyMessageViewIterator::new(&messages);
                let mut current_position = first_position;

                for (i, message) in iter.enumerate().skip(1) {
                    if i < count as usize {
                        current_position += message.size();
                        let expected_position = indexes.get(i as u32).unwrap().position();
                        debug_assert_eq!(
                            current_position, expected_position,
                            "Message {} position mismatch: {} != {}",
                            i, current_position, expected_position
                        );
                    }
                }
            }
        }

        Self {
            count,
            indexes,
            messages,
        }
    }

    /// Creates a empty messages container
    pub fn empty() -> Self {
        Self::new(IggyIndexes::empty(), BytesMut::new().freeze(), 0)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.messages)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        debug_assert_eq!(self.indexes.len() % INDEX_SIZE, 0);

        // For N messages, we might have either N or N+1 indexes
        // N+1 indexes define the boundaries of N messages (fence post pattern)
        let indexes_count = self.indexes.len() / INDEX_SIZE;
        debug_assert!(
            self.count as usize == indexes_count || self.count as usize == indexes_count - 1,
            "Mismatch between message count and indexes count ({}) != {}",
            self.count,
            indexes_count
        );

        self.count
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.messages.len() as u32
    }

    /// Get access to the underlying buffer
    pub fn buffer(&self) -> &[u8] {
        &self.messages
    }

    pub fn indexes_slice(&self) -> &[u8] {
        &self.indexes
    }

    /// Decompose the batch into its components
    pub fn decompose(self) -> (IggyIndexes, Bytes, u32) {
        (self.indexes, self.messages, self.count)
    }

    /// Get index of first message
    pub fn first_offset(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.header().offset())
            .unwrap_or(0)
    }

    /// Get timestamp of first message
    pub fn first_timestamp(&self) -> u64 {
        self.iter()
            .next()
            .map(|msg| msg.header().timestamp())
            .unwrap_or(0)
    }

    /// Get offset of last message
    pub fn last_offset(&self) -> u64 {
        self.iter()
            .last()
            .map(|msg| msg.header().offset())
            .unwrap_or(0)
    }

    /// Get timestamp of last message
    pub fn last_timestamp(&self) -> u64 {
        self.iter()
            .last()
            .map(|msg| msg.header().timestamp())
            .unwrap_or(0)
    }

    /// Helper method to read a base position (u32) from the byte array at the given index
    fn base_position_at(&self, position_index: u32) -> u32 {
        tracing::error!("base_position = {}", self.indexes.base_position());
        if let Some(index) = self.indexes.get(position_index) {
            index.position() - self.indexes.base_position()
        } else {
            0
        }
    }

    /// Helper method to read a position (u32) from the byte array at the given index
    fn position_at(&self, position_index: u32) -> u32 {
        if let Some(index) = self.indexes.get(position_index) {
            index.position()
        } else {
            0
        }
    }

    /// Returns a contiguous slice (as a new `IggyMessagesBatch`) of up to `count` messages
    /// whose message headers have an offset greater than or equal to the provided `start_offset`.
    pub fn slice_by_offset(&self, start_offset: u64, count: u32) -> Option<Self> {
        if self.is_empty() || count == 0 {
            return None;
        }

        // TODO(hubcio): this can be optimized via binary search
        let first_message_index = self
            .iter()
            .position(|msg| msg.header().offset() >= start_offset);

        first_message_index?;

        let first_message_index = first_message_index.unwrap();
        let last_message_index =
            std::cmp::min(first_message_index + count as usize, self.count() as usize);

        let sub_indexes = self.indexes.slice_by_offset(
            first_message_index as u32,
            (last_message_index - first_message_index) as u32,
        )?;

        let first_message_position = if first_message_index == 0 {
            0
        } else {
            self.position_at(first_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        let last_message_position = if last_message_index >= self.count() as usize {
            self.messages.len()
        } else {
            self.position_at(last_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        debug_assert!(
            first_message_position <= self.messages.len(),
            "First message position {} exceeds buffer length {}",
            first_message_position,
            self.messages.len()
        );
        debug_assert!(
            last_message_position <= self.messages.len(),
            "Last message position {} exceeds buffer length {}",
            last_message_position,
            self.messages.len()
        );

        let sub_buffer = self
            .messages
            .slice(first_message_position..last_message_position);

        Some(IggyMessagesBatch {
            count: (last_message_index - first_message_index) as u32,
            indexes: sub_indexes,
            messages: sub_buffer,
        })
    }

    /// Returns a contiguous slice (as a new `IggyMessagesBatch`) of up to `count` messages
    /// whose message headers have a timestamp greater than or equal to the provided `timestamp`.
    ///
    /// If no messages meet the criteria, returns `None`.
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<Self> {
        if self.is_empty() || count == 0 {
            return None;
        }

        // TODO(hubcio): this can be optimized via binary search
        let first_message_index = self
            .iter()
            .position(|msg| msg.header().timestamp() >= timestamp);

        first_message_index?;

        let first_message_index = first_message_index.unwrap();
        let last_message_index =
            std::cmp::min(first_message_index + count as usize, self.count() as usize);

        let sub_indexes = self.indexes.slice_by_offset(
            first_message_index as u32,
            (last_message_index - first_message_index) as u32,
        )?;

        let first_message_position = if first_message_index == 0 {
            0
        } else {
            self.position_at(first_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        let last_message_position = if last_message_index >= self.count() as usize {
            self.messages.len()
        } else {
            self.position_at(last_message_index as u32 - 1) as usize
                - self.indexes.base_position() as usize
        };

        debug_assert!(
            first_message_position <= self.messages.len(),
            "First message position {} exceeds buffer length {}",
            first_message_position,
            self.messages.len()
        );
        debug_assert!(
            last_message_position <= self.messages.len(),
            "Last message position {} exceeds buffer length {}",
            last_message_position,
            self.messages.len()
        );

        let sub_buffer = self
            .messages
            .slice(first_message_position..last_message_position);

        Some(IggyMessagesBatch {
            count: (last_message_index - first_message_index) as u32,
            indexes: sub_indexes,
            messages: sub_buffer,
        })
    }

    /// Get the message at the specified index.
    /// Returns None if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<IggyMessageView> {
        if index >= self.count as usize {
            return None;
        }

        let start_position = if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize
        };

        let end_position = if index == self.count as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize
        };

        Some(IggyMessageView::new(
            &self.messages[start_position..end_position],
        ))
    }
}

impl Index<usize> for IggyMessagesBatch {
    type Output = [u8];

    /// Get the message bytes at the specified index
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds (>= count)
    fn index(&self, index: usize) -> &Self::Output {
        if index >= self.count as usize {
            panic!(
                "Index out of bounds: the len is {} but the index is {}",
                self.count, index
            );
        }

        let start_position = if index == 0 {
            0
        } else {
            self.position_at(index as u32 - 1) as usize
        };

        let end_position = if index == self.count as usize - 1 {
            self.messages.len()
        } else {
            self.position_at(index as u32) as usize
        };

        &self.messages[start_position..end_position]
    }
}

impl BytesSerializable for IggyMessagesBatch {
    fn to_bytes(&self) -> Bytes {
        self.messages.clone()
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("don't use");
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.count);
        buf.put_slice(&self.indexes);
        buf.put_slice(&self.messages);
    }

    fn get_buffer_size(&self) -> u32 {
        4 + self.indexes.len() as u32 + self.messages.len() as u32
    }
}

impl Sizeable for IggyMessagesBatch {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages.len() as u64)
    }
}

impl Deref for IggyMessagesBatch {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}
