use super::{index_view::IggyIndexView, INDEX_SIZE};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Deref, Index as StdIndex};

/// A container for binary-encoded index data.
/// Optimized for efficient storage and I/O operations.
#[derive(Default, Clone, Serialize, Deserialize, PartialEq)]
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

    /// Gets the number of indexes in the container
    pub fn count(&self) -> u32 {
        self.buffer.len() as u32 / INDEX_SIZE as u32
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

    /// Gets a slice of the container
    pub fn slice_by_offset(&self, relative_start_offset: u32, count: u32) -> Option<IggyIndexes> {
        if self.count() == 0 || relative_start_offset >= self.count() {
            return None;
        }

        let available_count = self.count().saturating_sub(relative_start_offset);
        let actual_count = std::cmp::min(count, available_count);

        if actual_count == 0 {
            return None;
        }

        let end_pos = relative_start_offset + actual_count;
        let start_byte = relative_start_offset as usize * INDEX_SIZE;
        let end_byte = end_pos as usize * INDEX_SIZE;

        if end_byte > self.buffer.len() {
            return None;
        }

        let slice = self.buffer.slice(start_byte..end_byte);

        let base_position = if relative_start_offset > 0 {
            match self.get(relative_start_offset - 1) {
                Some(index) => index.position(),
                None => self.base_position,
            }
        } else {
            self.base_position
        };

        Some(IggyIndexes::new(slice, base_position))
    }

    /// Gets a first index
    pub fn first(&self) -> Option<IggyIndexView> {
        if self.count() == 0 {
            return None;
        }

        Some(IggyIndexView::new(&self.buffer[0..INDEX_SIZE]))
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

impl fmt::Debug for IggyIndexes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();

        if count == 0 {
            return write!(
                f,
                "IggyIndexes {{ count: 0, base_position: {}, indexes: [] }}",
                self.base_position
            );
        }

        writeln!(f, "IggyIndexes {{")?;
        writeln!(f, "    count: {},", count)?;
        writeln!(f, "    base_position: {},", self.base_position)?;
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
