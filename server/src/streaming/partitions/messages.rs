use crate::streaming::partitions::partition::Partition;
use crate::streaming::partitions::COMPONENT;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use std::sync::atomic::Ordering;
use tracing::trace;

impl Partition {
    /// Retrieves messages by timestamp (up to a specified count).
    pub async fn get_messages_by_timestamp(
        &self,
        timestamp: IggyTimestamp,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        trace!(
            "Getting {count} messages by timestamp: {} for partition: {}...",
            timestamp.as_micros(),
            self.partition_id
        );

        if self.segments.is_empty() || count == 0 {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let query_ts = timestamp.as_micros();

        let filtered_segments: Vec<&Segment> = self
            .segments
            .iter()
            .filter(|segment| segment.end_timestamp() >= query_ts)
            .collect();

        Self::get_messages_from_segments_by_timestamp(filtered_segments, query_ts, count).await
    }

    // Retrieves messages by offset (up to a specified count).
    pub async fn get_messages_by_offset(
        &self,
        start_offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        trace!(
            "Getting {count} messages for start offset: {start_offset} for partition: {}, current offset: {}...",
            self.partition_id,
            self.current_offset
        );

        if self.segments.is_empty() || start_offset > self.current_offset || count == 0 {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let start_segment_idx = match self
            .segments
            .iter()
            .rposition(|segment| segment.start_offset() <= start_offset)
        {
            Some(idx) => idx,
            None => return Ok(IggyMessagesBatchSet::empty()),
        };

        let relevant_segments: Vec<&Segment> = self.segments[start_segment_idx..].iter().collect();

        Self::get_messages_from_segments(relevant_segments, start_offset, count).await
    }

    // Retrieves the first messages (up to a specified count).
    pub async fn get_first_messages(&self, count: u32) -> Result<IggyMessagesBatchSet, IggyError> {
        self.get_messages_by_offset(0, count).await
    }

    // Retrieves the last messages (up to a specified count).
    pub async fn get_last_messages(&self, count: u32) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut requested_count = count as u64;
        if requested_count > self.current_offset + 1 {
            requested_count = self.current_offset + 1
        }
        let start_offset = 1 + self.current_offset - requested_count;
        self.get_messages_by_offset(start_offset, requested_count as u32)
            .await
    }

    // Retrieves the next messages for a polling consumer (up to a specified count).
    pub async fn get_next_messages(
        &self,
        consumer: PollingConsumer,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let (consumer_offsets, consumer_id) = match consumer {
            PollingConsumer::Consumer(consumer_id, _) => (&self.consumer_offsets, consumer_id),
            PollingConsumer::ConsumerGroup(group_id, _) => (&self.consumer_group_offsets, group_id),
        };

        let consumer_offset = consumer_offsets.get(&consumer_id);
        if consumer_offset.is_none() {
            trace!(
                "Consumer: {} hasn't stored offset for partition: {}, returning the first messages...",
                consumer_id,
                self.partition_id
            );
            return self.get_first_messages(count).await;
        }

        let consumer_offset = consumer_offset.unwrap();
        if consumer_offset.offset == self.current_offset {
            trace!(
                "Consumer: {} has the latest offset: {} for partition: {}, returning empty messages...",
                consumer_id,
                consumer_offset.offset,
                self.partition_id
            );
            return Ok(IggyMessagesBatchSet::empty());
        }

        let offset = consumer_offset.offset + 1;
        trace!(
            "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
            consumer_id,
            self.partition_id,
            offset
        );

        self.get_messages_by_offset(offset, count).await
    }

    /// Retrieves messages from multiple segments.
    async fn get_messages_from_segments(
        segments: Vec<&Segment>,
        offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut remaining_count = count;
        let mut current_offset = offset;
        let mut batches = IggyMessagesBatchSet::empty();

        for segment in segments {
            if remaining_count == 0 {
                break;
            }

            let messages = segment
            .get_messages_by_offset(current_offset, remaining_count)
            .await
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get messages from segment, segment: {}, \
                     offset: {}, count: {}",
                    segment, current_offset, remaining_count
                )
            })?;

            let messages_count = messages.count();
            if messages_count == 0 {
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);

            if let Some(last_offset) = messages.last_offset() {
                current_offset = last_offset + 1;
            } else if messages_count > 0 {
                current_offset += messages_count as u64;
            }

            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    /// Retrieves messages from multiple segments by timestamp.
    async fn get_messages_from_segments_by_timestamp(
        segments: Vec<&Segment>,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();

        for segment in segments {
            if remaining_count == 0 {
                break;
            }

            let messages = segment
                .get_messages_by_timestamp(timestamp, remaining_count)
                .await
                .with_error_context(|error| {
                    format!(
                        "{COMPONENT} (error: {error}) - failed to get messages from segment by timestamp, \
                         segment: {}, timestamp: {}, count: {}",
                        segment, timestamp, remaining_count
                    )
                })?;

            let messages_count = messages.count();
            remaining_count = remaining_count.saturating_sub(messages_count);

            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    pub async fn append_messages(
        &mut self,
        messages: IggyMessagesBatchMut,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        trace!(
            "Appending {} messages of size {} to partition with ID: {}...",
            messages.count(),
            messages.size(),
            self.partition_id
        );

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        if last_segment.is_closed() {
            let start_offset = last_segment.end_offset() + 1;
            trace!(
                    "Current segment is closed, creating new segment with start offset: {} for partition with ID: {}...",
                    start_offset, self.partition_id
                );
            self.add_persisted_segment(start_offset).await.with_error_context(|error| format!(
                    "{COMPONENT} (error: {error}) - failed to add persisted segment, partition: {}, start offset: {}",
                    self, start_offset,
                ))?
        }

        let current_offset = if !self.should_increment_offset {
            0
        } else {
            self.current_offset + 1
        };

        // TODO(implement deduplication based on seq number)
        // Use a sequence number on the SendMessages and hold local collection of the last few sequence numbers.
        /*
        let mut retained_messages = Vec::with_capacity(messages.len());
        if let Some(message_deduplicator) = &self.message_deduplicator {
            for message in messages {
                if !message_deduplicator.try_insert(&message.id).await {
                    warn!(
                        "Ignored the duplicated message ID: {} for partition with ID: {}.",
                        message.id, self.partition_id
                    );
                    continue;
                }
                let now = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, now, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        } else {
            for message in messages {
                let now = IggyTimestamp::now().as_micros();
                let message_offset = base_offset + messages_count as u64;
                let message = Arc::new(RetainedMessage::new(message_offset, now, message));
                retained_messages.push(message.clone());
                messages_count += 1;
            }
        }
        if messages_count == 0 {
            return Ok(());
        }
        */

        let messages_count = {
            let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
            last_segment
                 .append_batch(current_offset, messages)
                 .await
                 .with_error_context(|error| {
                     format!(
                         "{COMPONENT} (error: {error}) - failed to append batch into last segment: {last_segment}",
                     )
                 })?
        };

        // Handle the case when messages_count is 0 to avoid integer underflow
        let last_offset = if messages_count == 0 {
            current_offset
        } else {
            current_offset + messages_count as u64 - 1
        };

        if self.should_increment_offset {
            self.current_offset = last_offset;
        } else {
            self.should_increment_offset = true;
            self.current_offset = last_offset;
        }

        self.unsaved_messages_count += messages_count;

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        if self.unsaved_messages_count >= self.config.partition.messages_required_to_save
            || last_segment.is_full().await
        {
            trace!(
                    "Segment with start offset: {} for partition with ID: {} will be persisted on disk...",
                    last_segment.start_offset(),
                    self.partition_id
                );

            last_segment.persist_messages(confirmation).await.unwrap();
            self.unsaved_messages_count = 0;
        }

        Ok(())
    }

    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    pub async fn flush_unsaved_buffer(&mut self, fsync: bool) -> Result<(), IggyError> {
        let _fsync = fsync;
        if self.unsaved_messages_count == 0 {
            return Ok(());
        }

        let last_segment = self.segments.last_mut().ok_or(IggyError::SegmentNotFound)?;
        trace!(
            "Segment with start offset: {} for partition with ID: {} will be forcefully persisted on disk...",
            last_segment.start_offset(),
            self.partition_id
        );

        last_segment.persist_messages(None).await.unwrap();

        self.unsaved_messages_count = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::{MessageDeduplicationConfig, SystemConfig};
    use crate::streaming::persistence::persister::{FileWithSyncPersister, PersisterKind};
    use crate::streaming::storage::SystemStorage;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn given_disabled_message_deduplication_all_messages_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(false).await;
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition.append_messages(batch, None).await.unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.count(), messages_count);
    }

    #[tokio::test]
    async fn given_enabled_message_deduplication_only_messages_with_unique_id_should_be_appended() {
        let (mut partition, _tempdir) = create_partition(true).await;
        let messages = create_messages();
        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
        let messages_count = messages.len() as u32;
        let unique_messages_count = 3;

        partition.append_messages(batch, None).await.unwrap();

        let loaded_messages = partition
            .get_messages_by_offset(0, messages_count)
            .await
            .unwrap();
        assert_eq!(loaded_messages.count(), unique_messages_count);
    }

    async fn create_partition(deduplication_enabled: bool) -> (Partition, TempDir) {
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let with_segment = true;
        let temp_dir = TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: temp_dir.path().to_path_buf().to_str().unwrap().to_string(),
            message_deduplication: MessageDeduplicationConfig {
                enabled: deduplication_enabled,
                ..Default::default()
            },
            ..Default::default()
        });
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));

        (
            Partition::create(
                stream_id,
                topic_id,
                partition_id,
                with_segment,
                config,
                storage,
                IggyExpiry::NeverExpire,
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU32::new(0)),
                IggyTimestamp::now(),
            )
            .await,
            temp_dir,
        )
    }

    fn create_messages() -> Vec<IggyMessage> {
        vec![
            create_message(1, "message 1"),
            create_message(2, "message 2"),
            create_message(3, "message 3"),
            create_message(2, "message 3.2"),
            create_message(1, "message 1.2"),
            create_message(3, "message 3.3"),
        ]
    }

    fn create_message(id: u128, payload: &str) -> IggyMessage {
        IggyMessage::with_id(id, Bytes::from(payload.to_string()))
    }
}
