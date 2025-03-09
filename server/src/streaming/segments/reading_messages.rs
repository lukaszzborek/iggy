use super::Index;
use crate::streaming::segments::segment::Segment;
use bytes::BytesMut;
use error_set::ErrContext;
use iggy::prelude::*;
use std::sync::Arc;
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
        count: usize,
    ) -> Result<Vec<Arc<()>>, IggyError> {
        todo!();
        // if count == 0 {
        //     return Ok(Vec::new());
        // }

        // let mut messages = Vec::with_capacity(count);
        // let mut remaining = count;

        // let disk_messages = self
        //     .load_messages_from_disk_by_timestamp(start_timestamp, remaining)
        //     .await?;
        // let disk_count = disk_messages.len();
        // messages.extend(disk_messages);
        // remaining -= disk_count;

        // if remaining > 0 {
        //     if let Some(messages_accumulator) = &self.unsaved_messages {
        //         let buffer_messages =
        //             messages_accumulator.get_messages_by_timestamp(start_timestamp, remaining);
        //         messages.extend(buffer_messages);
        //     }
        // }

        // // Ensure we return exactly requested count (truncate if buffer had more)
        // messages.truncate(count);
        // Ok(messages)
    }

    pub async fn get_messages_by_offset(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<IggyMessages, IggyError> {
        if count == 0 {
            return Ok(IggyMessages::default());
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

        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_messages.is_none() {
            return self.load_messages_from_disk(offset, count).await;
        }

        let messages_accumulator = self.unsaved_messages.as_ref().unwrap();
        if messages_accumulator.is_empty() {
            return self.load_messages_from_disk(offset, count).await;
        }

        let accumulator_first_msg_offset = messages_accumulator.base_offset();
        let accumulator_last_msg_offset = messages_accumulator.max_offset();

        // Case 1: All messages are in messages_require_to_save buffer
        if offset >= accumulator_first_msg_offset && end_offset <= accumulator_last_msg_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        // Case 2: All messages are on disk
        if end_offset < accumulator_first_msg_offset {
            return self.load_messages_from_disk(offset, count).await;
        }

        // Case 3: Messages span disk and messages_require_to_save buffer boundary
        let mut buffer = BytesMut::new();

        // Load messages from disk up to the messages_require_to_save buffer boundary
        if offset < accumulator_first_msg_offset {
            let disk_messages = self
                .load_messages_from_disk(offset, (accumulator_first_msg_offset - offset) as u32)
                .await.with_error_context(|error| format!(
            "{COMPONENT} (error: {error}) - failed to load messages from disk, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {offset}, end offset :{}",
            self.stream_id, self.topic_id, self.partition_id, accumulator_first_msg_offset - 1
        ))?;
            buffer.extend_from_slice(disk_messages.buffer());
        }

        // Load remaining messages from messages_require_to_save buffer
        let buffer_start = std::cmp::max(offset, accumulator_first_msg_offset);
        let buffer_messages = self.load_messages_from_unsaved_buffer(buffer_start, end_offset);
        buffer.extend_from_slice(buffer_messages.buffer());

        let total_count = if buffer.is_empty() {
            0
        } else {
            IggyMessageViewIterator::new(&buffer).count() as u32
        };

        Ok(IggyMessages::new(buffer.freeze(), total_count))
    }

    pub async fn get_all_messages(&self) -> Result<IggyMessages, IggyError> {
        //TODO: Fix me
        /*
        self.get_messages_by_offset(self.start_offset, self.get_messages_count() as u32)
            .await
            */
        todo!()
    }

    fn load_messages_from_unsaved_buffer(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> IggyMessages {
        let messages_accumulator = self.unsaved_messages.as_ref().unwrap();
        messages_accumulator.get_messages_by_offset(start_offset, end_offset)
    }

    pub async fn load_index_for_timestamp(
        &self,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        todo!()

        // trace!("Loading index for timestamp: {}", timestamp);
        // let index = self
        //     .index_reader
        //     .as_ref()
        //     .unwrap()
        //     .load_index_for_timestamp_impl(timestamp)
        //     .await
        //     .with_error_context(|error| {
        //         format!(
        //             "Failed to load index for timestamp: {timestamp} for {}. {error}",
        //             self
        //         )
        //     })?;

        // trace!("Loaded index: {:?}", index);
        // Ok(index)
    }

    async fn load_messages_from_disk_by_timestamp(
        &self,
        start_timestamp: u64,
        count: usize,
    ) -> Result<Vec<Arc<()>>, IggyError> {
        //TODO Fix me
        /*
        let index = self.load_index_for_timestamp(start_timestamp).await?;
        let Some(index) = index else {
            return Ok(Vec::new());
        };

        let index_range = IndexRange {
            start: index,
            end: Index {
                offset: u32::MAX,
                position: u32::MAX,
                timestamp: u64::MAX,
            },
        };
        let batches = self.load_batches_by_range(&index_range).await?;

        let mut messages = Vec::with_capacity(count);
        for batch in batches {
            for msg in batch.into_messages_iter() {
                if msg.timestamp >= start_timestamp {
                    messages.push(Arc::new(msg));
                    if messages.len() >= count {
                        break;
                    }
                }
            }
            if messages.len() >= count {
                break;
            }
        }

        Ok(messages)
        */
        todo!()
    }

    /// Loads and verifies message checksums from the log file.
    pub async fn load_message_checksums(&self) -> Result<(), IggyError> {
        //TODO: Fix me
        /*
        self.log_reader
            .as_ref()
            .unwrap()
            .load_batches_by_range_with_callback(&IndexRange::max_range(), |batch| {
                for message in batch.into_messages_iter() {
                    let calculated_checksum = checksum::calculate(&message.payload);
                    trace!(
                        "Loaded message for offset: {}, checksum: {}, expected: {}",
                        message.offset,
                        calculated_checksum,
                        message.checksum
                    );
                    if calculated_checksum != message.checksum {
                        return Err(IggyError::InvalidMessageChecksum(
                            calculated_checksum,
                            message.checksum,
                            message.offset,
                        ));
                    }
                }
                Ok(())
            })
            .await
            .with_error_context(|error| {
                format!("Failed to load batches by max range for {}. {error}", self)
            })?;
        Ok(())
        */
        todo!()
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids(&self) -> Result<Vec<u128>, IggyError> {
        todo!()

        // trace!("Loading message IDs from log file: {}", self.log_path);
        // let ids = self
        //     .log_reader
        //     .as_ref()
        //     .unwrap()
        //     .load_message_ids_impl()
        //     .await
        //     .with_error_context(|error| {
        //         format!("Failed to load message IDs, error: {error} for {self}")
        //     })?;
        // trace!("Loaded {} message IDs from log file.", ids.len());
        // Ok(ids)
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyMessages, IggyError> {
        trace!(
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
            .await?
            .unwrap();

        let last_index = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_nth_index(relative_end_offset)
            .await?
            .unwrap();

        let start_pos = first_index.position;
        let count_bytes = last_index.position - start_pos;

        let messages = self
            .messages_reader
            .as_ref()
            .unwrap()
            .load_messages_impl(start_pos, count_bytes, count)
            .await
            .with_error_context(|error| {
                format!("Failed to load messages from segment file: {self}. {error}")
            })?;
        Ok(messages)
    }
}
