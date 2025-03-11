use super::{IggyMessages, IggyMessagesBatch, IggyMessagesMut, Index};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::mem::take;

/// A container that accumulates messages before they are written to disk
#[derive(Debug, Default)]
pub struct MessagesAccumulator {
    /// Base offset of the first message
    base_offset: u64,

    /// Base timestamp of the first message
    base_timestamp: u64,

    /// Current size of all accumulated messages
    current_size: IggyByteSize,

    /// Current maximum offset
    current_offset: u64,

    /// Current maximum timestamp
    current_timestamp: u64,

    /// A buffer containing all accumulated messages
    batches: IggyMessagesBatch,

    /// Number of messages in the accumulator
    messages_count: u32,
}

impl MessagesAccumulator {
    /// Coalesces a batch of messages into the accumulator
    /// This method also prepares the messages for persistence by setting their
    /// offset, timestamp, record size, and checksum fields
    /// Returns the number of messages in the batch
    pub fn coalesce_batch(
        &mut self,
        current_offset: u64,
        current_position: u32,
        indexes: &mut Vec<Index>,
        messages: IggyMessagesMut,
    ) -> u32 {
        let batch_messages_count = messages.count();

        if batch_messages_count == 0 {
            return 0;
        }

        if self.batches.is_empty() {
            self.base_offset = current_offset;
            self.base_timestamp = IggyTimestamp::now().as_micros();
            self.current_offset = current_offset;
            self.current_timestamp = self.base_timestamp;
        }

        let batch = messages.prepare_for_persistence(
            self.base_offset,
            self.current_timestamp,
            current_position,
            indexes,
        );
        let batch_size = batch.size();

        self.batches.add(batch);

        self.messages_count += batch_messages_count;
        self.current_size += IggyByteSize::from(batch_size as u64);
        self.current_offset = self.base_offset + self.messages_count as u64 - 1;
        self.current_timestamp = IggyTimestamp::now().as_micros();

        batch_messages_count
    }

    pub fn get_messages_by_offset(&self, start_offset: u64, end_offset: u64) -> IggyMessages {
        todo!();
    }

    /// Checks if the accumulator is empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() || self.messages_count == 0
    }

    /// Returns the number of unsaved messages
    pub fn unsaved_messages_count(&self) -> usize {
        self.messages_count as usize
    }

    /// Returns the maximum offset in the accumulator
    pub fn max_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the maximum timestamp in the accumulator
    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    /// Returns the base offset of the accumulator
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the base timestamp of the accumulator
    pub fn batch_base_timestamp(&self) -> u64 {
        self.base_timestamp
    }

    /// Takes ownership of the accumulator and returns the messages and indexes
    pub fn materialize(self) -> IggyMessagesBatch {
        self.batches
    }
}
