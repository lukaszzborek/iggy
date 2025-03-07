use bytes::BytesMut;
use iggy::models::{IggyMessages, IggyMessagesMut};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use std::mem;

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
    /// A single IggyMessagesMut instance containing all messages
    messages: Option<IggyMessagesMut>,
    /// Number of messages in the accumulator
    messages_count: u32,
}

impl MessagesAccumulator {
    /// Coalesces a batch of messages into the accumulator
    /// Returns the number of messages in the batch
    pub fn coalesce_batch(&mut self, current_offset: u64, batch: IggyMessagesMut) -> u32 {
        let batch_messages_count = batch.count();

        if batch_messages_count == 0 {
            return 0;
        }

        if self.messages.is_none() {
            self.base_offset = current_offset;
            self.base_timestamp = IggyTimestamp::now().as_micros();
            self.current_offset = current_offset;
            self.current_timestamp = self.base_timestamp;
        }

        let batch_size = batch.size();

        if let Some(messages) = &mut self.messages {
            messages.extend(batch);
        } else {
            self.messages = Some(batch);
            // self.messages.as_mut().unwrap().set_capacity(1_200_000); // TODO(hubcio): Add new config and multiply this by k factor
        }

        self.messages_count += batch_messages_count;
        self.current_size += IggyByteSize::from(batch_size as u64);
        self.current_offset = self.base_offset + self.messages_count as u64 - 1;
        self.current_timestamp = IggyTimestamp::now().as_micros();

        batch_messages_count
    }

    /// Checks if the accumulator is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_none() || self.messages_count == 0
    }

    /// Returns the number of unsaved messages
    pub fn unsaved_messages_count(&self) -> usize {
        self.messages_count as usize
    }

    /// Returns the maximum offset in the accumulator
    pub fn batch_max_offset(&self) -> u64 {
        self.current_offset
    }

    /// Returns the maximum timestamp in the accumulator
    pub fn batch_max_timestamp(&self) -> u64 {
        self.current_timestamp
    }

    /// Returns the base offset of the accumulator
    pub fn batch_base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Returns the base timestamp of the accumulator
    pub fn batch_base_timestamp(&self) -> u64 {
        self.base_timestamp
    }

    /// Takes ownership of the accumulator and returns the messages
    pub fn materialize(self) -> IggyMessages {
        self.messages.unwrap().into()
    }
}
