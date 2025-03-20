use super::{index_view::IggyIndexView, INDEX_SIZE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, Index as StdIndex};

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct IggyIndexes {
    base_position: u32,
    buffer: Bytes,
}

impl IggyIndexes {
    /// Creates a new empty container
    pub fn new(indexes: Bytes, base_position: u32) -> Self {
        Self {
            buffer: indexes,
            base_position,
        }
    }

    pub fn empty() -> Self {
        Self {
            buffer: Bytes::new(),
            base_position: 0,
        }
    }

    pub fn into_inner(self) -> Bytes {
        self.buffer
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        self.buffer.len() as u32 / INDEX_SIZE as u32
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Gets the size of all indexes messages
    pub fn messages_size(&self) -> u32 {
        self.last_position() - self.base_position
    }

    /// Gets a view of the index at the specified position
    pub fn get(&self, position: u32) -> Option<IggyIndexView> {
        if position >= self.count() {
            return None;
        }

        let start = position as usize * INDEX_SIZE;
        let end = start + INDEX_SIZE;

        if end <= self.buffer.len() {
            Some(IggyIndexView::new(&self.buffer[start..end]))
        } else {
            None
        }
    }

    pub fn slice_by_offset(&self, relative_start_offset: u32, count: u32) -> Option<IggyIndexes> {
        let available_count = self.count().saturating_sub(relative_start_offset);

        let required_count = count;
        let actual_count = std::cmp::min(required_count, available_count);

        if actual_count == 0 || relative_start_offset >= self.count() {
            return None;
        }

        let end_pos = relative_start_offset + actual_count;
        let start_byte = relative_start_offset as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;
        let slice = self.buffer.slice(start_byte..end_byte);

        let i = relative_start_offset.saturating_sub(1);
        if i == 0 {
            tracing::error!("IDX 0 base_position: 0");
            Some(IggyIndexes::new(slice, 0))
        } else {
            let base_position = self.get(relative_start_offset - 1).unwrap().position();
            tracing::error!("base_position: {base_position}");
            Some(IggyIndexes::new(slice, base_position))
        }
    }

    /// Gets a last index
    pub fn last(&self) -> Option<IggyIndexView> {
        if self.count() == 0 {
            return None;
        }

        Some(IggyIndexView::new(
            &self.buffer[(self.count() - 1) as usize * INDEX_SIZE..],
        ))
    }

    /// Finds an index by timestamp using binary search
    /// If an exact match isn't found, returns the index with the nearest timestamp
    /// that is greater than or equal to the requested timestamp
    pub fn find_by_timestamp(&self, timestamp: u64) -> Option<IggyIndexView> {
        if self.count() == 0 {
            return None;
        }

        let first_idx = self.get(0)?;
        if timestamp <= first_idx.timestamp() {
            tracing::trace!(
                "Requested timestamp {} is less than any available",
                timestamp
            );
            return Some(first_idx);
        }

        let last_saved_idx = self.get(self.count() - 1)?;
        if timestamp > last_saved_idx.timestamp() {
            return None;
        }

        let mut left = 0;
        let mut right = self.count() as isize - 1;
        let mut result: Option<IggyIndexView> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let view = self.get(mid as u32).unwrap();
            let current_timestamp = view.timestamp();

            match current_timestamp.cmp(&timestamp) {
                std::cmp::Ordering::Equal => {
                    result = Some(view);
                    right = mid - 1;
                }
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    result = Some(view);
                    right = mid - 1;
                }
            }
        }

        result
    }

    // /// Calculate boundary information for reading messages from disk based on cached indexes.
    // ///
    // /// This computes the exact file position, bytes to read, and expected message count
    // /// based on the provided relative offsets using in-memory index data.
    // ///
    // /// Returns None if the requested offset is out of bounds.
    // pub fn calculate_cached_read_boundary_by_offset(
    //     &self,
    //     relative_start_offset: u32,
    //     relative_end_offset: u32,
    // ) -> Option<ReadBoundary> {
    //     if self.count() == 0 {
    //         trace!("No indexes available in memory");
    //         return None;
    //     }

    //     if relative_start_offset >= self.count() {
    //         trace!(
    //             "Start offset {} is out of bounds. Total cached indexes: {}",
    //             relative_start_offset,
    //             self.count(),
    //         );
    //         return None;
    //     }

    //     let effective_end_offset = relative_end_offset.min(self.count() - 1);

    //     tracing::error!(
    //         "start offset: {}, end offset: {}",
    //         relative_start_offset,
    //         effective_end_offset
    //     );

    //     // With our new index interpretation:
    //     // - For messages after the first one, the start position comes from the PREVIOUS index
    //     // - For the first message (offset 0), the start position is implicitly 0
    //     // - The end position always comes from the current index

    //     let start_position = if relative_start_offset > 0 {
    //         // For non-first messages, get start position from previous index
    //         let prev_index = self.get(relative_start_offset - 1)?;
    //         prev_index.position()
    //     } else {
    //         // For the first message, start position is 0
    //         0
    //     };

    //     // The end position comes from the last index we want to read
    //     let last_index = self.get(effective_end_offset)?;
    //     let end_position = last_index.position();

    //     let bytes = end_position - start_position;
    //     let messages_count = effective_end_offset - relative_start_offset + 1;

    //     trace!(
    //         "Calculated read boundary from cached indexes: start_pos={}, bytes={}, count={}",
    //         start_position,
    //         bytes,
    //         messages_count
    //     );

    //     Some(ReadBoundary::new(start_position, bytes, messages_count))
    // }

    // /// Calculate boundary information for reading messages from disk based on a timestamp.
    // ///
    // /// This finds the index with timestamp closest to the requested timestamp and returns
    // /// the boundary information to read the messages from that point forward.
    // ///
    // /// Returns None if the timestamp is not found or there are no indexes.
    // pub fn calculate_cached_read_boundary_by_timestamp(
    //     &self,
    //     timestamp: u64,
    //     messages_count: u32,
    // ) -> Option<ReadBoundary> {
    //     let start_index = self.find_by_timestamp(timestamp)?;

    //     tracing::trace!("Found start_index: {}", start_index);

    //     let start_position_in_array = start_index.offset() - self.first_offset();
    //     let end_position_in_array =
    //         (start_position_in_array + messages_count - 1).min(self.count() - 1);

    //     tracing::trace!(
    //         "Calculated end_position_in_array: {}",
    //         end_position_in_array
    //     );
    //     let end_index = self.get(end_position_in_array)?;

    //     tracing::trace!("Found end_index: {:?}", end_index);

    //     let start_position = start_index.position();
    //     // Check to prevent overflow in case end_index.position() < start_position
    //     let bytes = if end_index.position() >= start_position {
    //         end_index.position() - start_position
    //     } else {
    //         tracing::warn!(
    //             "End index position {} is less than start position {}. Using 0 bytes.",
    //             end_index.position(),
    //             start_position
    //         );
    //         0
    //     };

    //     // Ensure we don't have underflow when calculating messages count
    //     let actual_messages_count = if end_position_in_array >= start_position_in_array {
    //         end_position_in_array - start_position_in_array + 1
    //     } else {
    //         tracing::warn!(
    //             "End position {} is less than start position {}. Using 1 message.",
    //             end_position_in_array,
    //             start_position_in_array
    //         );
    //         1
    //     };

    //     trace!(
    //         "Calculated read boundary by timestamp: start_pos={}, bytes={}, count={}",
    //         start_position,
    //         bytes,
    //         actual_messages_count
    //     );

    //     Some(ReadBoundary::new(
    //         start_position,
    //         bytes,
    //         actual_messages_count,
    //     ))
    // }

    pub fn base_position(&self) -> u32 {
        self.base_position
    }

    /// Helper method to get the first index offset
    pub fn first_offset(&self) -> u32 {
        let offset = self.get(0).map(|idx| idx.offset()).unwrap_or(0);
        offset
    }

    /// Helper method to get the first index position
    pub fn first_position(&self) -> u32 {
        let position = self.get(0).map(|idx| idx.position()).unwrap_or(0);
        position
    }

    /// Helper method to get the first timestamp
    pub fn first_timestamp(&self) -> u64 {
        self.get(0).map(|idx| idx.timestamp()).unwrap_or(0)
    }

    /// Helper method to get the last index offset
    pub fn last_offset(&self) -> u32 {
        self.get(self.count() - 1)
            .map(|idx| idx.offset())
            .unwrap_or(0)
    }

    /// Helper method to get the last index position
    pub fn last_position(&self) -> u32 {
        self.get(self.count() - 1)
            .map(|idx| idx.position())
            .unwrap_or(0)
    }

    /// Helper method to get the last timestamp
    pub fn last_timestamp(&self) -> u64 {
        self.get(self.count() - 1)
            .map(|idx| idx.timestamp())
            .unwrap_or(0)
    }
}

impl StdIndex<usize> for IggyIndexes {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * INDEX_SIZE;
        let end = start + INDEX_SIZE;
        &self.buffer[start..end]
    }
}

impl Deref for IggyIndexes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}
