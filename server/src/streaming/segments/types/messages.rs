use bytes::{Bytes, BytesMut};
use iggy::prelude::*;
use std::ops::Deref;

/// An immutable messages container that holds a buffer of messages
#[derive(Debug, PartialEq)]
pub struct IggyMessages {
    /// The number of messages in the buffer
    count: u32,
    /// The buffer containing the messages
    buffer: Bytes,
}

impl IggyMessages {
    /// Create a new messages container from a buffer
    pub fn new(buffer: Bytes, count: u32) -> Self {
        Self { buffer, count }
    }

    /// Creates a empty messages container
    pub fn empty() -> Self {
        Self::new(BytesMut::new().freeze(), 0)
    }

    /// Create iterator over messages
    pub fn iter(&self) -> IggyMessageViewIterator {
        IggyMessageViewIterator::new(&self.buffer)
    }

    /// Get the number of messages
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Get the total size of all messages in bytes
    pub fn size(&self) -> u32 {
        self.buffer.len() as u32
    }

    /// Get access to the underlying buffer
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
}

impl Sizeable for IggyMessages {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl Deref for IggyMessages {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.buffer()
    }
}
