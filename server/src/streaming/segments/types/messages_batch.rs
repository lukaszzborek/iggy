use super::messages::IggyMessages;
use iggy::prelude::*;

/// A batch container for multiple IggyMessages objects
#[derive(Debug, Default)]
pub struct IggyMessagesBatch {
    /// The collection of message containers
    messages: Vec<IggyMessages>,
    /// Total number of messages across all containers
    count: u32,
    /// Total size in bytes across all containers
    size: u32,
}

impl IggyMessagesBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
            count: 0,
            size: 0,
        }
    }

    /// Create a new batch with a specified initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            messages: Vec::with_capacity(capacity),
            count: 0,
            size: 0,
        }
    }

    /// Create a batch from an existing vector of IggyMessages
    pub fn from_vec(messages: Vec<IggyMessages>) -> Self {
        let mut batch = Self::with_capacity(messages.len());
        for msg in messages {
            batch.add(msg);
        }
        batch
    }

    /// Add a message container to the batch
    pub fn add(&mut self, messages: IggyMessages) {
        self.count += messages.count();
        self.size += messages.size();
        self.messages.push(messages);
    }

    /// Add another batch of messages to the batch
    pub fn add_batch(&mut self, other: IggyMessagesBatch) {
        self.count += other.messages_count();
        self.size += other.size();
        self.messages.extend(other.messages);
    }

    /// Get the total number of messages in the batch
    pub fn messages_count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Get the number of message containers in the batch
    pub fn containers_count(&self) -> usize {
        self.messages.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() || self.count == 0
    }

    /// Get a reference to the underlying vector of message containers
    pub fn inner(&self) -> &Vec<IggyMessages> {
        &self.messages
    }

    /// Consume the batch, returning the underlying vector of message containers
    pub fn into_inner(self) -> Vec<IggyMessages> {
        self.messages
    }

    /// Iterate over all message containers in the batch
    pub fn iter(&self) -> impl Iterator<Item = &IggyMessages> {
        self.messages.iter()
    }
}

impl Sizeable for IggyMessagesBatch {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size as u64)
    }
}

impl From<Vec<IggyMessages>> for IggyMessagesBatch {
    fn from(messages: Vec<IggyMessages>) -> Self {
        Self::from_vec(messages)
    }
}

impl From<IggyMessages> for IggyMessagesBatch {
    fn from(messages: IggyMessages) -> Self {
        Self::from_vec(vec![messages])
    }
}
