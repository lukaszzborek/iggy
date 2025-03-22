use bytes::{BufMut, BytesMut};
use iggy::models::messaging::{IggyIndexView, IggyIndexes, INDEX_SIZE};
use std::fmt;
use std::ops::{Deref, Index as StdIndex};

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default, Clone)]
pub struct IggyIndexesMut {
    buffer: BytesMut,
    unsaved_count: u32,
}

impl IggyIndexesMut {
    /// Creates a new empty container
    pub fn empty() -> Self {
        Self {
            buffer: BytesMut::new(),
            unsaved_count: 0,
        }
    }

    /// Creates indexes from bytes
    pub fn from_bytes(indexes: BytesMut) -> Self {
        Self {
            buffer: indexes,
            unsaved_count: 0,
        }
    }

    /// Creates a new container with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity * INDEX_SIZE),
            unsaved_count: 0,
        }
    }

    /// Makes the indexes immutable
    pub fn make_immutable(self) -> IggyIndexes {
        IggyIndexes::new(self.buffer.freeze(), self.unsaved_count)
    }

    /// Sets the number of unsaved messages
    pub fn set_unsaved_messages_count(&mut self, count: u32) {
        self.unsaved_count = count;
    }

    /// Inserts a new index at the end of buffer
    pub fn insert(&mut self, offset: u32, position: u32, timestamp: u64) {
        self.buffer.put_u32_le(offset);
        self.buffer.put_u32_le(position);
        self.buffer.put_u64_le(timestamp);
    }

    /// Appends another IggyIndexesMut instance to this one. Other indexes buffer is consumed.
    pub fn concatenate(&mut self, other: IggyIndexesMut) {
        self.buffer.put_slice(&other.buffer);
        self.unsaved_count += other.unsaved_count;
    }

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        self.buffer.len() as u32 / INDEX_SIZE as u32
    }

    /// Checks if the container is empty
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Gets the size of the buffer in bytes
    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    /// Gets a view of the index at the specified position
    pub fn get(&self, position: u32) -> Option<IggyIndexView> {
        if position >= self.count() {
            return None;
        }

        let start = (position * INDEX_SIZE as u32) as usize;
        let end = start + INDEX_SIZE;

        if end <= self.buffer.len() {
            Some(IggyIndexView::new(&self.buffer[start..end]))
        } else {
            None
        }
    }

    // Set the offset at the given index position
    pub fn set_offset_at(&mut self, index: u32, offset: u32) {
        let pos = index as usize * INDEX_SIZE;
        self.buffer[pos..pos + 4].copy_from_slice(&offset.to_le_bytes());
    }

    // Set the position at the given index
    pub fn set_position_at(&mut self, index: u32, position: u32) {
        let pos = (index as usize * INDEX_SIZE) + 4;
        self.buffer[pos..pos + 4].copy_from_slice(&position.to_le_bytes());
    }

    // Set the timestamp at the given index
    pub fn set_timestamp_at(&mut self, index: u32, timestamp: u64) {
        let pos = (index as usize * INDEX_SIZE) + 8;
        self.buffer[pos..pos + 8].copy_from_slice(&timestamp.to_le_bytes());
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

    /// Gets a slice of the buffer containing the last N unsaved indexes.
    pub fn get_unsaved_indexes(&self) -> &[u8] {
        if self.count() == 0 || self.unsaved_count == 0 {
            tracing::error!("No unsaved indexes");
            return &[];
        }

        let start_idx = (self.count() - self.unsaved_count) as usize * 16;

        &self.buffer[start_idx..]
    }

    /// Mark all unsaved indexes as saved
    pub fn mark_saved(&mut self) {
        self.unsaved_count = 0;
    }

    /// Clears the container, removing all indexes
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.unsaved_count = 0;
    }

    /// Slices the container to return a view of a specific range of indexes
    pub fn slice_by_offset(&self, relative_start_offset: u32, count: u32) -> Option<IggyIndexes> {
        let available_count = self.count().saturating_sub(relative_start_offset);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 || relative_start_offset >= self.count() {
            return None;
        }

        let end_pos = relative_start_offset + actual_count;

        let start_byte = relative_start_offset as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;
        let slice = BytesMut::from(&self.buffer[start_byte..end_byte]);

        let i = relative_start_offset.saturating_sub(1);
        if i == 0 {
            Some(IggyIndexes::new(slice.freeze(), 0))
        } else {
            let base_position = self.get(relative_start_offset - 1).unwrap().position();
            Some(IggyIndexes::new(slice.freeze(), base_position))
        }
    }

    /// Loads indexes from cache based on timestamp
    pub fn slice_by_timestamp(&self, timestamp: u64, count: u32) -> Option<IggyIndexes> {
        if self.count() == 0 {
            return None;
        }

        let start_index_pos = self.binary_search_position_for_timestamp_sync(timestamp)?;

        let available_count = self.count().saturating_sub(start_index_pos);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 {
            return None;
        }

        let end_pos = start_index_pos + actual_count;

        let start_byte = start_index_pos as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;
        let slice = BytesMut::from(&self.buffer[start_byte..end_byte]);

        let base_position = if start_index_pos > 0 {
            self.get(start_index_pos - 1).unwrap().position()
        } else {
            0
        };

        Some(IggyIndexes::new(slice.freeze(), base_position))
    }

    /// Find the position of the index with timestamp closest to (but not exceeding) the target
    fn binary_search_position_for_timestamp_sync(&self, target_timestamp: u64) -> Option<u32> {
        if self.count() == 0 {
            return None;
        }

        let last_index = self.get(self.count() - 1)?;
        if target_timestamp > last_index.timestamp() {
            return Some(self.count() - 1);
        }

        let first_index = self.get(0)?;
        if target_timestamp <= first_index.timestamp() {
            return Some(0);
        }

        let mut low = 0;
        let mut high = self.count() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_index = self.get(mid)?;
            let mid_timestamp = mid_index.timestamp();

            match mid_timestamp.cmp(&target_timestamp) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
            }
        }

        if low > 0 {
            Some(low - 1)
        } else {
            Some(0)
        }
    }
}

impl StdIndex<usize> for IggyIndexesMut {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        let start = index * INDEX_SIZE;
        let end = start + INDEX_SIZE;
        &self.buffer[start..end]
    }
}

impl Deref for IggyIndexesMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl fmt::Debug for IggyIndexesMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();

        if count == 0 {
            return write!(f, "IggyIndexesMut {{ count: 0, indexes: [] }}");
        }

        writeln!(f, "IggyIndexesMut {{")?;
        writeln!(f, "    count: {},", count)?;
        writeln!(f, "    unsaved_count: {},", self.unsaved_count)?;
        writeln!(f, "    indexes: [")?;

        for i in 0..count {
            if let Some(index) = self.get(i) {
                writeln!(
                    f,
                    "        {{ offset: {}, position: {}, timestamp: {} }},",
                    index.offset(),
                    index.position(),
                    index.timestamp()
                )?;
            }
        }

        writeln!(f, "    ]")?;
        write!(f, "}}")
    }
}
