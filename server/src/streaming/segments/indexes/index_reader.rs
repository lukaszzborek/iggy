use super::{
    index::IggyIndex, index_view::IggyIndexView, IggyIndexesMut, ReadBoundary, INDEX_SIZE,
};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::{
    fs::{File, OpenOptions},
    io::ErrorKind,
    os::unix::fs::FileExt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::task::spawn_blocking;
use tracing::{error, trace};

/// A dedicated struct for reading from the index file.
#[derive(Debug)]
pub struct IndexReader {
    file_path: String,
    file: Arc<File>,
    index_size_bytes: Arc<AtomicU64>,
}

impl IndexReader {
    /// Opens the index file in read-only mode.
    pub async fn new(file_path: &str, index_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let actual_index_size = file
            .metadata()
            .with_error_context(|error| {
                format!("Failed to get metadata of index file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        index_size_bytes.store(actual_index_size, Ordering::Release);

        trace!("Opened index file for reading: {file_path}, size: {actual_index_size}",);
        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            index_size_bytes,
        })
    }

    /// Loads all indexes from the index file into the optimized binary format.
    pub async fn load_all_indexes_impl(&self) -> Result<IggyIndexesMut, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Index file {} is empty.", self.file_path);
            return Ok(IggyIndexesMut::new());
        }

        let buf = match self.read_at(0, file_size).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(IggyIndexesMut::new())
            }
            Err(error) => {
                error!(
                    "Error reading batch header at offset 0 in file {}: {error}",
                    self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };
        let index_count = file_size / INDEX_SIZE;
        let mut indexes = IggyIndexesMut::with_capacity(index_count as usize);

        for chunk in buf.chunks_exact(INDEX_SIZE as usize) {
            let view = IggyIndexView::new(chunk);
            indexes.add_unsaved_index(view.offset(), view.position(), view.timestamp());
        }

        if indexes.count() != index_count {
            error!(
                "Loaded {} indexes from disk, expected {}, file {} is probably corrupted!",
                indexes.count(),
                index_count,
                self.file_path
            );
        }

        Ok(indexes)
    }

    /// Calculate boundary information for reading messages from disk.
    ///
    /// This computes the exact file position, bytes to read, and expected message count
    /// based on the provided relative offsets
    pub async fn calculate_disk_read_boundary_by_offset(
        &self,
        relative_start_offset: u32,
        relative_end_offset: u32,
    ) -> Result<Option<ReadBoundary>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE;

        if file_size == 0 || total_indexes == 0 {
            trace!(
                "Index file {} is empty, cannot calculate read boundary",
                self.file_path
            );
            return Ok(None);
        }

        if relative_start_offset >= total_indexes {
            trace!(
                "Start offset {} is out of bounds. Total indexes: {}",
                relative_start_offset,
                total_indexes
            );
            return Ok(None);
        }

        let effective_end_offset = relative_end_offset.min(total_indexes - 1);

        let first_index = match self.load_nth_index(relative_start_offset).await? {
            Some(idx) => idx,
            None => {
                trace!("Failed to load index at position {}", relative_start_offset);
                return Ok(None);
            }
        };

        let last_index = match self.load_nth_index(effective_end_offset).await? {
            Some(idx) => idx,
            None => {
                trace!("Failed to load index at position {}", effective_end_offset);
                return Ok(None);
            }
        };

        let start_position = first_index.position;
        let bytes = last_index.position - start_position;
        let messages_count = effective_end_offset - relative_start_offset + 1;

        trace!(
            "Calculated read boundary: start_pos={}, bytes={}, count={}",
            start_position,
            bytes,
            messages_count
        );

        Ok(Some(ReadBoundary::new(
            start_position,
            bytes,
            messages_count,
        )))
    }

    /// Calculate boundary information for reading messages from disk based on a timestamp.
    ///
    /// This finds the index with timestamp closest to the requested timestamp and returns
    /// the boundary information to read the messages from that point forward.
    pub async fn calculate_disk_read_boundary_by_timestamp(
        &self,
        timestamp: u64,
        messages_count: u32,
    ) -> Result<Option<ReadBoundary>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Index file is empty");
            return Ok(None);
        }

        let start_index = match self.find_index_by_timestamp(timestamp).await? {
            Some(index) => index,
            None => return Ok(None),
        };

        let total_indexes = file_size / INDEX_SIZE;
        let start_position_in_array = start_index.offset;

        let available_count = total_indexes - start_position_in_array;
        let requested_count = messages_count;
        let actual_count = std::cmp::min(available_count, requested_count);

        // If actual count is 0, we can't read any messages
        if actual_count == 0 {
            return Ok(None);
        }

        let end_position_in_array = start_position_in_array + actual_count - 1;

        if end_position_in_array >= total_indexes {
            return Ok(None);
        }

        let end_index = match self.load_nth_index(end_position_in_array as u32).await? {
            Some(index) => index,
            None => return Ok(None),
        };

        let start_position = start_index.position;
        // Check to prevent overflow in case end_index.position < start_position
        let bytes = if end_index.position >= start_position {
            end_index.position - start_position
        } else {
            // This shouldn't normally happen but we need to handle it
            tracing::warn!(
                "End index position {} is less than start position {}. Using 0 bytes.",
                end_index.position,
                start_position
            );
            0
        };

        // Ensure we don't have underflow when calculating messages count
        // This is safer than the original calculation
        let actual_messages_count = if end_position_in_array >= start_position_in_array {
            (end_position_in_array - start_position_in_array + 1) as u32
        } else {
            tracing::warn!(
                "End position {} is less than start position {}. Using 0 messages.",
                end_position_in_array,
                start_position_in_array
            );
            0
        };

        trace!(
            "Calculated disk read boundary by timestamp {timestamp}, count {messages_count}: start_pos={start_position}, bytes={bytes}, count={actual_messages_count}",
        );

        Ok(Some(ReadBoundary::new(
            start_position,
            bytes,
            actual_messages_count,
        )))
    }

    /// Finds the index with the timestamp greater than or equal to the given timestamp
    /// Uses binary search for efficiency
    async fn find_index_by_timestamp(
        &self,
        timestamp: u64,
    ) -> Result<Option<IggyIndex>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            return Ok(None);
        }

        let total_indexes = file_size / INDEX_SIZE;
        if total_indexes == 0 {
            return Ok(None);
        }

        let first_idx = self.load_nth_index(0).await?;
        if first_idx.is_none() {
            return Ok(None);
        }

        let first_idx = first_idx.unwrap();

        // If the requested timestamp is less than the first index,
        // return the first index since that's the earliest available
        if timestamp <= first_idx.timestamp {
            tracing::trace!(
                "Requested timestamp {} is less than or equal to first index timestamp {}",
                timestamp,
                first_idx.timestamp
            );
            return Ok(Some(first_idx));
        }

        let last_idx = self.load_nth_index(total_indexes - 1).await?;
        if last_idx.is_none() {
            return Ok(None);
        }
        let last_idx = last_idx.unwrap();

        // If the requested timestamp is greater than the last index,
        // we can't find any valid indexes
        if timestamp > last_idx.timestamp {
            tracing::trace!(
                "Requested timestamp {} is greater than last index timestamp {}",
                timestamp,
                last_idx.timestamp
            );
            return Ok(None);
        }

        let mut left = 0;
        let mut right = total_indexes - 1;
        let mut result: Option<IggyIndex> = None;

        // Binary search to find the first index with timestamp >= requested timestamp
        while left <= right {
            let mid = left + (right - left) / 2;
            let index = match self.load_nth_index(mid).await? {
                Some(idx) => idx,
                None => break,
            };

            match index.timestamp.cmp(&timestamp) {
                std::cmp::Ordering::Equal => {
                    // Found exact match
                    return Ok(Some(index));
                }
                std::cmp::Ordering::Less => {
                    // Index timestamp is less than requested, look in right half
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    // Index timestamp is greater than requested
                    result = Some(index); // This could be our answer, but there might be a better one
                    right = mid - 1; // Look in left half for a potentially better match
                }
            }
        }

        // If we didn't find an exact match but reached here,
        // we need to return the first index that's greater than the timestamp
        if result.is_none() {
            // Scan forward to find the first index >= timestamp
            for i in left..total_indexes {
                let index = match self.load_nth_index(i).await? {
                    Some(idx) => idx,
                    None => continue,
                };

                if index.timestamp >= timestamp {
                    result = Some(index);
                    break;
                }
            }
        }

        if result.is_none() && left < total_indexes {
            result = self.load_nth_index(left).await?;
        }

        Ok(result)
    }

    /// Returns the size of the index file in bytes.
    fn file_size(&self) -> u32 {
        self.index_size_bytes.load(Ordering::Acquire) as u32
    }

    /// Reads a specified number of bytes from the index file at a given offset.
    async fn read_at(&self, offset: u32, len: u32) -> Result<Vec<u8>, std::io::Error> {
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = vec![0u8; len as usize];
            file.read_exact_at(&mut buf, offset as u64)?;
            Ok(buf)
        })
        .await?
    }

    /// Gets the nth index from the index file.
    ///
    /// The index position is 0-based (first index is at position 0).
    /// Returns None if the specified position is out of bounds.
    async fn load_nth_index(&self, position: u32) -> Result<Option<IggyIndex>, IggyError> {
        let file_size = self.file_size();
        let total_indexes = file_size / INDEX_SIZE;

        if position >= total_indexes {
            trace!(
                "Index position {} is out of bounds. Total indexes: {}",
                position,
                total_indexes
            );
            return Ok(None);
        }

        let offset = position * INDEX_SIZE;

        let buf = match self.read_at(offset, INDEX_SIZE).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => {
                error!(
                    "Error reading index at position {} (offset {}) in file {}: {error}",
                    position, offset, self.file_path
                );
                return Err(IggyError::CannotReadFile);
            }
        };

        Ok(Some(IggyIndexView::new(&buf).to_index()))
    }
}
