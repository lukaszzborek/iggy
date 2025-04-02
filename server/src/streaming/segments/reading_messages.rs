use std::sync::atomic::Ordering;

use super::IggyMessagesBatchSet;
use crate::streaming::segments::segment::Segment;
use error_set::ErrContext;
use iggy::models::messaging::IggyIndexes;
use iggy::prelude::*;
use tracing::trace;

const COMPONENT: &str = "STREAMING_SEGMENT";

// TODO: test when offset > u32

impl Segment {
    pub fn get_messages_size(&self) -> IggyByteSize {
        IggyByteSize::from(self.messages_size.load(Ordering::Relaxed))
    }

    pub fn get_messages_count(&self) -> u32 {
        if self.get_messages_size() == 0 {
            return 0;
        }

        (self.end_offset - self.start_offset + 1) as u32
    }

    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        trace!(
            "Getting {count} messages by timestamp {timestamp}, current_offset: {}...",
            self.end_offset
        );

        // Case 0: Accumulator is empty, so all messages have to be on disk
        if self.accumulator.is_empty() {
            return self
                .load_messages_from_disk_by_timestamp(timestamp, count)
                .await;
        }

        let accumulator_first_timestamp = self.accumulator.first_timestamp();
        let accumulator_last_timestamp = self.accumulator.last_timestamp();

        // Case 1: Requested timestamp is higher than any available timestamp
        if timestamp > accumulator_last_timestamp {
            return Ok(IggyMessagesBatchSet::empty());
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
        let mut out = messages_from_disk;
        out.add_batch_set(accumulator_messages);

        Ok(out)
    }

    pub async fn get_messages_by_offset(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        if end_offset > self.end_offset {
            end_offset = self.end_offset;
        }

        trace!(
            "Getting messages by offset: {}, count: {}, segment start_offset: {}, segment end_offset: {}",
            offset,
            count,
            self.start_offset,
            self.end_offset
        );

        // Case 0: Accumulator is empty, so all messages have to be on disk
        if self.accumulator.is_empty() {
            return self.load_messages_from_disk_by_offset(offset, count).await;
        }

        let accumulator_first_msg_offset = self.accumulator.first_offset();
        let accumulator_last_msg_offset = self.accumulator.last_offset();

        // Case 1: All messages are in accumulator buffer
        if offset >= accumulator_first_msg_offset && end_offset <= accumulator_last_msg_offset {
            return Ok(self.accumulator.get_messages_by_offset(offset, count));
        }

        // Case 2: All messages are on disk
        if end_offset < accumulator_first_msg_offset {
            return self.load_messages_from_disk_by_offset(offset, count).await;
        }

        // Case 3: Messages span disk and accumulator buffer boundary
        // Calculate how many messages we need from disk
        let disk_count = if offset < accumulator_first_msg_offset {
            ((accumulator_first_msg_offset - offset) as u32).min(count)
        } else {
            0
        };

        let mut combined_batch_set = IggyMessagesBatchSet::empty();

        // Load messages from disk if needed
        if disk_count > 0 {
            let disk_messages = self
            .load_messages_from_disk_by_offset(offset, disk_count)
            .await
            .with_error_context(|error| {
                format!(
                    "STREAMING_SEGMENT (error: {error}) - failed to load messages from disk, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {offset}, count: {disk_count}",
                    self.stream_id, self.topic_id, self.partition_id
                )
            })?;

            if !disk_messages.is_empty() {
                combined_batch_set.add_batch_set(disk_messages);
            }
        }

        // Calculate how many more messages we need from the accumulator
        let remaining_count = count - combined_batch_set.count();

        if remaining_count > 0 {
            let accumulator_start_offset = std::cmp::max(offset, accumulator_first_msg_offset);

            let accumulator_messages = self
                .accumulator
                .get_messages_by_offset(accumulator_start_offset, remaining_count);

            if !accumulator_messages.is_empty() {
                combined_batch_set.add_batch_set(accumulator_messages);
            }
        }

        Ok(combined_batch_set)
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids(&self) -> Result<Vec<u128>, IggyError> {
        let messages_count = self.get_messages_count();
        trace!(
            "Loading message IDs for {messages_count} messages from log file: {}",
            self.messages_path
        );

        let indexes = self.load_indexes_by_offset(0, messages_count).await?;

        if indexes.is_none() {
            return Ok(vec![]);
        }

        let indexes = indexes.unwrap();

        let ids = self
            .messages_reader
            .as_ref()
            .unwrap()
            .load_all_message_ids_from_disk(indexes, messages_count)
            .await
            .with_error_context(|error| {
                format!("Failed to load message IDs, error: {error} for {self}")
            })?;
        trace!("Loaded {} message IDs from log file.", ids.len());
        Ok(ids)
    }

    async fn load_indexes_by_offset(
        &self,
        relative_start_offset: u32,
        count: u32,
    ) -> Result<Option<IggyIndexes>, IggyError> {
        let indexes = if self.config.segment.cache_indexes {
            self.indexes.slice_by_offset(relative_start_offset, count)
        } else {
            self.index_reader
                .as_ref()
                .expect("Index reader not initialized")
                .load_from_disk_by_offset(relative_start_offset, count)
                .await?
        };
        Ok(indexes)
    }

    async fn load_indexes_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<Option<IggyIndexes>, IggyError> {
        let indexes = if self.config.segment.cache_indexes {
            self.indexes.slice_by_timestamp(timestamp, count)
        } else {
            self.index_reader
                .as_ref()
                .unwrap()
                .load_from_disk_by_timestamp(timestamp, count)
                .await?
        };
        Ok(indexes)
    }

    async fn load_messages_from_disk_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        tracing::trace!(
            "Loading {count} messages from disk, start_offset: {start_offset}, end_offset: {}...",
            self.end_offset
        );
        let relative_start_offset = (start_offset - self.start_offset) as u32;

        let indexes_to_read = self
            .load_indexes_by_offset(relative_start_offset, count)
            .await?;

        if indexes_to_read.is_none() {
            return Ok(IggyMessagesBatchSet::empty());
        }
        let indexes_to_read = indexes_to_read.unwrap();

        let msgs = self
            .messages_reader
            .as_ref()
            .expect("Messages reader not initialized")
            .load_messages_from_disk(indexes_to_read)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file: {self}. {error}")
            })?;

        tracing::trace!(
            "Loaded {} messages ({} bytes) from disk (requested {count} messages), start_offset: {start_offset}, end_offset: {}",
            msgs.count(),
            msgs.size(),
            self.end_offset
        );

        Ok(msgs)
    }

    async fn load_messages_from_disk_by_timestamp(
        &self,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        tracing::trace!(
            "Loading {count} messages from disk, timestamp: {timestamp}, current_timestamp: {}...",
            self.end_timestamp
        );

        let indexes_to_read = self.load_indexes_by_timestamp(timestamp, count).await?;

        if indexes_to_read.is_none() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let indexes_to_read = indexes_to_read.unwrap();

        self.messages_reader
            .as_ref()
            .expect("Messages reader not initialized")
            .load_messages_from_disk(indexes_to_read)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file by timestamp: {self}. {error}")
            })
    }
}
