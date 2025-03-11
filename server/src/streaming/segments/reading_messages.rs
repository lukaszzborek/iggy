use super::{IggyMessages, IggyMessagesBatch, Index};
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

        self.current_offset - self.start_offset + 1
    }

    pub async fn get_messages_by_timestamp(
        &self,
        start_timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatch, IggyError> {
        todo!()

        // if count == 0 {
        //     return Ok(IggyMessagesMut::empty());
        // }

        // let index_opt = self.load_index_for_timestamp(start_timestamp).await?;

        // let Some(index) = index_opt else {
        //     trace!("No messages found for timestamp: {}", start_timestamp);
        //     return Ok(IggyMessagesMut::empty());
        // };

        // let offset = self.start_offset + index.offset as u64;
        // trace!("Found offset {} for timestamp {}", offset, start_timestamp);

        // self.get_messages_by_offset(offset, count).await
    }

    pub async fn get_messages_by_offset(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatch, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatch::default());
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        if end_offset > self.current_offset {
            end_offset = self.current_offset;
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        if self.accumulator.is_empty() {
            return Ok(IggyMessagesBatch::from(
                self.load_messages_from_disk(offset, count).await?,
            ));
        }

        let accumulator_first_msg_offset = self.accumulator.base_offset();
        let accumulator_last_msg_offset = self.accumulator.max_offset();

        // Case 1: All messages are in messages_require_to_save buffer
        if offset >= accumulator_first_msg_offset && end_offset <= accumulator_last_msg_offset {
            return Ok(IggyMessagesBatch::from(
                self.accumulator.get_messages_by_offset(offset, end_offset),
            ));
        }

        // Case 2: All messages are on disk
        if end_offset < accumulator_first_msg_offset {
            return Ok(IggyMessagesBatch::from(
                self.load_messages_from_disk(offset, count).await?,
            ));
        }

        // Case 3: Messages span disk and messages_require_to_save buffer boundary

        // Load messages from disk up to the messages_require_to_save buffer boundary
        let disk_messages = self
                .load_messages_from_disk(offset, (accumulator_first_msg_offset - offset) as u32)
                .await.with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - failed to load messages from disk, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {offset}, end offset :{}",
            self.stream_id, self.topic_id, self.partition_id, accumulator_first_msg_offset - 1
        ))?;

        // Load remaining messages from messages_require_to_save buffer
        let buffer_start = std::cmp::max(offset, accumulator_first_msg_offset);
        let buffer_messages = self
            .accumulator
            .get_messages_by_offset(buffer_start, end_offset);

        Ok(IggyMessagesBatch::from(vec![
            disk_messages,
            buffer_messages,
        ]))
    }

    pub async fn load_index_for_timestamp(
        &self,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        if timestamp < self.start_timestamp {
            trace!(
                "Timestamp {} is earlier than segment start timestamp {}",
                timestamp,
                self.start_timestamp
            );
            return Ok(Some(Index::default()));
        }

        if timestamp > self.end_timestamp {
            trace!(
                "Timestamp {} is later than segment end timestamp {}",
                timestamp,
                self.end_timestamp
            );
            return Ok(None);
        }

        trace!("Loading index for timestamp: {}", timestamp);
        let index = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_index_for_timestamp_impl(timestamp)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to load index for timestamp: {timestamp} for {}. {error}",
                    self
                )
            })?;

        trace!("Loaded index: {:?}", index);
        Ok(index)
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

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyMessages, IggyError> {
        tracing::trace!(
                "Loading {count} messages from disk, start_offset: {start_offset}, current_offset: {}...",
                self.current_offset
            );

        let relative_start_offset = (start_offset - self.start_offset) as u32;
        let relative_end_offset = relative_start_offset + count - 1;

        let first_index = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_nth_index(relative_start_offset)
            .await?;

        if first_index.is_none() {
            return Ok(IggyMessages::empty());
        }

        let first_index = first_index.unwrap();

        let last_index = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_nth_index(relative_end_offset)
            .await?
            .unwrap();

        let start_pos = first_index.position;
        let count_bytes = last_index.position - start_pos;

        self.messages_reader
            .as_ref()
            .unwrap()
            .load_messages_impl(start_pos, count_bytes, count)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file: {self}. {error}")
            })
    }
}
