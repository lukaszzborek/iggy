use super::IggyMessagesMut;
use crate::streaming::segments::segment::Segment;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::prelude::*;
use std::sync::atomic::Ordering;
use tracing::{info, trace};

impl Segment {
    pub async fn append_batch(
        &mut self,
        current_offset: u64,
        messages: IggyMessagesMut,
    ) -> Result<u32, IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }
        let messages_size = messages.size();
        let messages_accumulator = &mut self.accumulator;
        let messages_count = messages_accumulator.coalesce_batch(
            current_offset,
            self.last_index_position,
            &mut self.indexes,
            messages,
        );

        if self.current_offset == 0 {
            self.start_timestamp = messages_accumulator.batch_base_timestamp();
        }
        self.end_timestamp = messages_accumulator.batch_max_timestamp();
        self.current_offset = messages_accumulator.max_offset();

        self.size_bytes += IggyByteSize::from(messages_size as u64);

        self.size_of_parent_stream
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.size_of_parent_topic
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.size_of_parent_partition
            .fetch_add(messages_size as u64, Ordering::AcqRel);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count as u64, Ordering::SeqCst);

        Ok(messages_count)
    }

    pub async fn persist_messages(
        &mut self,
        confirmation: Option<Confirmation>,
    ) -> Result<usize, IggyError> {
        if self.accumulator.is_empty() {
            return Ok(0);
        }

        let unsaved_messages_number = self.accumulator.unsaved_messages_count();
        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id
        );

        let accumulator = std::mem::take(&mut self.accumulator);
        
        let batches = accumulator.materialize();
        let confirmation = match confirmation {
            Some(val) => val,
            None => self.config.segment.server_confirmation,
        };

        let saved_bytes = self
            .messages_writer
            .as_mut()
            .unwrap()
            .save_batches(batches, confirmation)
            .await
            .with_error_context(|error| format!("Failed to save batch for {self}. {error}",))?;

        self.last_index_position += saved_bytes.as_bytes_u64() as u32;

        debug_assert!(self.last_index_position == self.indexes.last().unwrap().position);

        self.index_writer
            .as_mut()
            .unwrap()
            .save_indexes(&self.indexes)
            .await
            .with_error_context(|error| format!("Failed to save index for {self}. {error}"))?;

        self.indexes.clear();
        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.shutdown_writing().await;
            info!(
                "Closed segment with start offset: {}, end offset: {} for partition with ID: {}.",
                self.start_offset, self.end_offset, self.partition_id
            );
        }
        Ok(unsaved_messages_number)
    }
}
