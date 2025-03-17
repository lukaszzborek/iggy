use super::index_view::IggyIndexView;
use super::read_boundary::ReadBoundary;
use bytes::{BufMut, BytesMut};
use std::ops::{Deref, Index as StdIndex};
use tracing::trace;

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Debug, Default, Clone)]
pub struct IggyIndexesMut {
    buffer: BytesMut,
    count: u32,
    unsaved_count: u32,
}

impl IggyIndexesMut {
    /// Creates a new empty container
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            count: 0,
            unsaved_count: 0,
        }
    }

    /// Creates a new container with the specified capacity in bytes
    pub fn with_capacity(index_count: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(index_count * 16),
            count: 0,
            unsaved_count: 0,
        }
    }

    /// Adds a new unsaved index to the container
    pub fn add_unsaved_index(&mut self, offset: u32, position: u32, timestamp: u64) {
        self.buffer.put_u32_le(offset);
        self.buffer.put_u32_le(position);
        self.buffer.put_u64_le(timestamp);
        self.count += 1;
        self.unsaved_count += 1;
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Gets the size of the buffer in bytes
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Gets a view of the index at the specified position
    pub fn get(&self, position: u32) -> Option<IggyIndexView> {
        if position >= self.count {
            return None;
        }

        let start = (position * 16) as usize;
        let end = start + 16;

        if end <= self.buffer.len() {
            Some(IggyIndexView::new(&self.buffer[start..end]))
        } else {
            None
        }
    }

    /// Gets a last index
    pub fn last(&self) -> Option<IggyIndexView> {
        if self.count == 0 {
            return None;
        }

        Some(IggyIndexView::new(
            &self.buffer[(self.count - 1) as usize * 16..],
        ))
    }

    /// Finds an index by timestamp using binary search
    /// If an exact match isn't found, returns the index with the nearest timestamp
    /// that is greater than or equal to the requested timestamp
    pub fn find_by_timestamp(&self, timestamp: u64) -> Option<IggyIndexView> {
        if self.count == 0 {
            return None;
        }

        // Check if we should return the first index (requested timestamp is less than any available)
        let first_idx = self.get(0)?;
        if timestamp <= first_idx.timestamp() {
            tracing::trace!(
                "Requested timestamp {} is less than any available",
                timestamp
            );
            return Some(first_idx);
        }

        // Check if we should return None (requested timestamp is greater than any available)
        let last_saved_idx = self.get(self.count - 1)?;
        if timestamp > last_saved_idx.timestamp() {
            return None;
        }

        // Binary search to find the earliest index with timestamp >= requested timestamp
        let mut left = 0;
        let mut right = self.count as isize - 1;
        let mut result: Option<IggyIndexView> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let view = self.get(mid as u32).unwrap();
            let current_timestamp = view.timestamp();

            match current_timestamp.cmp(&timestamp) {
                std::cmp::Ordering::Equal => {
                    // Found an exact match, but we need to find the FIRST index with this timestamp
                    result = Some(view);
                    // Continue searching to the left to find the earliest message with this timestamp
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

        // Return the closest index with timestamp >= requested timestamp
        result
    }

    /// Gets a slice of the buffer containing the last N unsaved indexes.
    /// This enables zero-copy access to only the most recent unsaved indexes.
    ///
    /// # Returns
    /// A byte slice containing only the last N unsaved indexes
    /// If the buffer is empty, returns an empty slice
    pub fn get_unsaved_indexes(&self) -> &[u8] {
        if self.count == 0 || self.unsaved_count == 0 {
            tracing::error!("No unsaved indexes");
            return &[];
        }

        let start_idx = (self.count - self.unsaved_count) as usize * 16;

        &self.buffer[start_idx..]
    }

    /// Mark all unsaved indexes as saved
    pub fn mark_saved(&mut self) {
        self.unsaved_count = 0;
    }

    /// Clears the container, removing all indexes
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.count = 0;
        self.unsaved_count = 0;
    }

    /// Calculate boundary information for reading messages from disk based on cached indexes.
    ///
    /// This computes the exact file position, bytes to read, and expected message count
    /// based on the provided relative offsets using in-memory index data.
    ///
    /// Returns None if the requested offset is out of bounds.
    pub fn calculate_cached_read_boundary_by_offset(
        &self,
        relative_start_offset: u32,
        relative_end_offset: u32,
    ) -> Option<ReadBoundary> {
        // Check if we have any indexes
        if self.count == 0 {
            trace!("No indexes available in memory");
            return None;
        }

        // Validate the requested offsets
        if relative_start_offset >= self.count - self.unsaved_count {
            trace!(
                "Start offset {} is out of bounds. Total cached indexes: {}, unsaved count: {}",
                relative_start_offset,
                self.count,
                self.unsaved_count
            );
            return None;
        }

        // Clamp the end offset to the available range
        let effective_end_offset = relative_end_offset.min(self.count - self.unsaved_count - 1);

        // Get position information from the cached indexes
        let first_index = self.get(relative_start_offset)?;
        let last_index = self.get(effective_end_offset)?;

        // Calculate boundary information
        let start_position = first_index.position();
        let bytes = last_index.position() - start_position;
        let messages_count = effective_end_offset - relative_start_offset + 1;

        trace!(
            "Calculated read boundary from cached indexes: start_pos={}, bytes={}, count={}",
            start_position,
            bytes,
            messages_count
        );

        Some(ReadBoundary::new(start_position, bytes, messages_count))
    }

    /// Calculate boundary information for reading messages from disk based on a timestamp.
    ///
    /// This finds the index with timestamp closest to the requested timestamp and returns
    /// the boundary information to read the messages from that point forward.
    ///
    /// Returns None if the timestamp is not found or there are no indexes.
    pub fn calculate_cached_read_boundary_by_timestamp(
        &self,
        timestamp: u64,
        messages_count: u32,
    ) -> Option<ReadBoundary> {
        let start_index = self.find_by_timestamp(timestamp)?;

        tracing::trace!("Found start_index: {}", start_index);

        let start_position_in_array = start_index.offset() - self.first_offset();
        // Calculate the end position without unnecessarily limiting by unsaved_count
        // We want to include as many messages as possible up to messages_count
        let end_position_in_array =
            (start_position_in_array + messages_count - 1).min(self.count - 1);

        tracing::trace!(
            "Calculated end_position_in_array: {}",
            end_position_in_array
        );
        let end_index = self.get(end_position_in_array)?;

        tracing::trace!("Found end_index: {:?}", end_index);

        let start_position = start_index.position();
        // Check to prevent overflow in case end_index.position() < start_position
        let bytes = if end_index.position() >= start_position {
            end_index.position() - start_position
        } else {
            tracing::warn!(
                "End index position {} is less than start position {}. Using 0 bytes.",
                end_index.position(),
                start_position
            );
            0
        };

        // Ensure we don't have underflow when calculating messages count
        let actual_messages_count = if end_position_in_array >= start_position_in_array {
            end_position_in_array - start_position_in_array + 1
        } else {
            tracing::warn!(
                "End position {} is less than start position {}. Using 1 message.",
                end_position_in_array,
                start_position_in_array
            );
            1
        };

        trace!(
            "Calculated read boundary by timestamp: start_pos={}, bytes={}, count={}",
            start_position,
            bytes,
            actual_messages_count
        );

        Some(ReadBoundary::new(
            start_position,
            bytes,
            actual_messages_count,
        ))
    }

    /// Helper method to get the first index offset
    fn first_offset(&self) -> u32 {
        if self.count == 0 {
            return 0;
        }

        self.get(0).map(|idx| idx.offset()).unwrap_or(0)
    }
}

impl StdIndex<usize> for IggyIndexesMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * 16;
        let end = start + 16;
        &self.buffer[start..end]
    }
}

impl Deref for IggyIndexesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}
