use super::{indexes::ReadBoundary, IggyBatch};
use crate::streaming::segments::segment::Segment;
use error_set::ErrContext;
use iggy::prelude::*;
use tracing::trace;

const COMPONENT: &str = "STREAMING_SEGMENT";

impl Segment {
    pub fn get_messages_count(&self) -> u64 {
        if self.size_bytes == 0 {
            return 0;
        }

        self.end_offset - self.start_offset + 1
    }

    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyBatch, IggyError> {
        if count == 0 {
            return Ok(IggyBatch::default());
        }

        trace!(
            "Getting {count} messages by timestamp {timestamp}, current_offset: {}...",
            self.end_offset
        );

        // Handle empty accumulator case
        if self.accumulator.is_empty() {
            return self
                .load_messages_from_disk_by_timestamp(timestamp, count)
                .await;
        }

        let accumulator_first_timestamp = self.accumulator.base_timestamp();
        let accumulator_last_timestamp = self.accumulator.max_timestamp();

        // Case 1: Requested timestamp is higher than any available timestamp
        if timestamp > accumulator_last_timestamp {
            return Ok(IggyBatch::empty());
        }

        // Case 2: Requested timestamp falls within accumulator range only
        if timestamp >= accumulator_first_timestamp {
            // Get all messages from accumulator with timestamp >= the requested timestamp
            return Ok(self.accumulator.get_messages_by_timestamp(timestamp, count));
        }

        // Case 3: Timestamp is lower than accumulator's first timestamp
        // Need to get messages from disk and potentially combine with accumulator
        let messages_from_disk = self
            .load_messages_from_disk_by_timestamp(timestamp, count)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to load messages from disk by timestamp, stream ID: {}, topic ID: {}, partition ID: {}, timestamp: {timestamp}",
                    self.stream_id, self.topic_id, self.partition_id
                )
            })?;

        // If we got enough messages from disk or there are no messages from disk,
        // we don't need to consider messages from the accumulator
        if messages_from_disk.count() >= count {
            return Ok(messages_from_disk);
        }

        // If we need more messages, get them from accumulator, respecting the original timestamp
        // This ensures we don't miss messages with the same or very close timestamps
        let remaining_count = count - messages_from_disk.count();
        let accumulator_messages = self
            .accumulator
            .get_messages_by_timestamp(timestamp, remaining_count);

        // Combine the messages
        let mut result = messages_from_disk;
        result.add_batch(accumulator_messages);

        Ok(result)
    }

    async fn load_messages_from_disk_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyBatch, IggyError> {
        let read_boundary = self
            .calculate_read_boundary_by_timestamp(timestamp, count)
            .await?;

        if read_boundary.is_none() {
            return Ok(IggyBatch::empty());
        }

        let read_boundary = read_boundary.unwrap();

        // Add defensive check to ensure read_boundary.messages_count doesn't exceed count
        // This prevents reading more messages than asked for
        if read_boundary.messages_count > count {
            let adjusted_boundary =
                ReadBoundary::new(read_boundary.start_position, read_boundary.bytes, count);
            return self
                .messages_reader
                .as_ref()
                .unwrap()
                .load_messages_impl(adjusted_boundary)
                .await
                .with_error_context(|error| {
                    format!(
                        "Failed to load messages from segment file by timestamp: {self}. {error}"
                    )
                });
        }

        self.messages_reader
            .as_ref()
            .unwrap()
            .load_messages_impl(read_boundary)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file by timestamp: {self}. {error}")
            })
    }

    pub async fn get_messages_by_offset(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<IggyBatch, IggyError> {
        if count == 0 {
            return Ok(IggyBatch::default());
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        if end_offset > self.end_offset {
            end_offset = self.end_offset;
        }

        if self.accumulator.is_empty() {
            return self.load_messages_from_disk(offset, count).await;
        }

        let accumulator_first_msg_offset = self.accumulator.base_offset();
        let accumulator_last_msg_offset = self.accumulator.max_offset();

        // Case 1: All messages are in messages_require_to_save buffer
        if offset >= accumulator_first_msg_offset && end_offset <= accumulator_last_msg_offset {
            return Ok(self.accumulator.get_messages_by_offset(offset, count));
        }

        // Case 2: All messages are on disk
        if end_offset < accumulator_first_msg_offset {
            return self.load_messages_from_disk(offset, count).await;
        }

        // Case 3: Messages span disk and messages_require_to_save buffer boundary

        // Load messages from disk up to the messages_require_to_save buffer boundary
        let mut messages = self
                .load_messages_from_disk(offset, (accumulator_first_msg_offset - offset) as u32)
                .await.with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - failed to load messages from disk, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {offset}, end offset :{}",
            self.stream_id, self.topic_id, self.partition_id, accumulator_first_msg_offset - 1
        ))?;

        // Load remaining messages from messages_require_to_save buffer
        let buffer_start = std::cmp::max(offset, accumulator_first_msg_offset);
        let buffer_count = (end_offset - buffer_start + 1) as u32;
        let buffer_messages = self
            .accumulator
            .get_messages_by_offset(buffer_start, buffer_count);

        messages.add_batch(buffer_messages);

        Ok(messages)
    }

    /// Loads and verifies message checksums from the log file.
    pub async fn load_message_checksums(&self) -> Result<(), IggyError> {
        // self.log_reader
        //     .as_ref()
        //     .unwrap()
        //     .load_batches_by_range_with_callback(&IndexRange::max_range(), |batch| {
        //         for message in batch.into_messages_iter() {
        //             let calculated_checksum = checksum::calculate(&message.payload);
        //             trace!(
        //                 "Loaded message for offset: {}, checksum: {}, expected: {}",
        //                 message.offset,
        //                 calculated_checksum,
        //                 message.checksum
        //             );
        //             if calculated_checksum != message.checksum {
        //                 return Err(IggyError::InvalidMessageChecksum(
        //                     calculated_checksum,
        //                     message.checksum,
        //                     message.offset,
        //                 ));
        //             }
        //         }
        //         Ok(())
        //     })
        //     .await
        //     .with_error_context(|error| {
        //         format!("Failed to load batches by max range for {}. {error}", self)
        //     })?;
        // Ok(())

        todo!()
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids(&self) -> Result<Vec<u128>, IggyError> {
        trace!("Loading message IDs from log file: {}", self.log_path);
        let ids = self
            .messages_reader
            .as_ref()
            .unwrap()
            .load_message_ids_impl()
            .await
            .with_error_context(|error| {
                format!("Failed to load message IDs, error: {error} for {self}")
            })?;
        trace!("Loaded {} message IDs from log file.", ids.len());
        Ok(ids)
    }

    async fn calculate_read_boundary_by_offset(
        &self,
        relative_start_offset: u32,
        relative_end_offset: u32,
    ) -> Result<Option<ReadBoundary>, IggyError> {
        let read_boundary = if self.config.segment.cache_indexes {
            self.indexes.calculate_cached_read_boundary_by_offset(
                relative_start_offset,
                relative_end_offset,
            )
        } else {
            self.index_reader
                .as_ref()
                .unwrap()
                .calculate_disk_read_boundary_by_offset(relative_start_offset, relative_end_offset)
                .await?
        };
        Ok(read_boundary)
    }

    async fn calculate_read_boundary_by_timestamp(
        &self,
        timestamp: u64,
        messages_count: u32,
    ) -> Result<Option<ReadBoundary>, IggyError> {
        let read_boundary = if self.config.segment.cache_indexes {
            self.indexes
                .calculate_cached_read_boundary_by_timestamp(timestamp, messages_count)
        } else {
            self.index_reader
                .as_ref()
                .unwrap()
                .calculate_disk_read_boundary_by_timestamp(timestamp, messages_count)
                .await?
        };
        Ok(read_boundary)
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyBatch, IggyError> {
        tracing::trace!(
                "Loading {count} messages from disk, start_offset: {start_offset}, current_offset: {}...",
                self.end_offset
            );
        let relative_start_offset = (start_offset - self.start_offset) as u32;
        let relative_end_offset = relative_start_offset + count - 1;

        let read_boundary = self
            .calculate_read_boundary_by_offset(relative_start_offset, relative_end_offset)
            .await?;

        if read_boundary.is_none() {
            return Ok(IggyBatch::empty());
        }
        let read_boundary = read_boundary.unwrap();

        let msgs = self
            .messages_reader
            .as_ref()
            .unwrap()
            .load_messages_impl(read_boundary)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file: {self}. {error}")
            })?;

        Ok(msgs)
    }
}
