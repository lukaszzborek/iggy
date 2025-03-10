use std::collections::HashMap;

use super::{
    message_view::IggyMessageViewIterator, IggyMessage, IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE,
};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// An immutable messages container that holds a buffer of messages
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct IggyMessages {
    /// The number of messages in the buffer
    #[serde(skip)]
    count: u32,
    /// The buffer containing the messages
    buffer: Bytes,
}

impl IggyMessages {
    /// Create a new messages container from a buffer
    pub fn new(buffer: Bytes, count: u32) -> Self {
        Self { buffer, count }
    }

    /// Creates a empty messages container with capacity
    pub fn with_capacity(capacity: u32) -> Self {
        Self::new(BytesMut::with_capacity(capacity as usize).freeze(), 0)
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

    /// Get access to the underlying buffer shallow  copy
    pub fn shallow_copy(&self) -> Bytes {
        self.buffer.clone()
    }

    pub fn into_inner(self) -> Bytes {
        self.buffer
    }

    pub fn to_messages(self) -> Vec<IggyMessage> {
        let mut messages = Vec::with_capacity(self.count as usize);
        let mut position = 0;
        let buf_len = self.buffer.len();

        while position < buf_len {
            if position + IGGY_MESSAGE_HEADER_SIZE as usize > buf_len {
                break;
            }
            let header_bytes = self
                .buffer
                .slice(position..position + IGGY_MESSAGE_HEADER_SIZE as usize);
            let header = match IggyMessageHeader::from_bytes(header_bytes) {
                Ok(h) => h,
                Err(_) => break,
            };
            position += IGGY_MESSAGE_HEADER_SIZE as usize;

            let payload_end = position + header.payload_length as usize;
            if payload_end > buf_len {
                break;
            }
            let payload = self.buffer.slice(position..payload_end);
            position = payload_end;

            let headers: Option<HashMap<super::HeaderKey, super::HeaderValue>> = if header.headers_length > 0 {
                let headers_end = position + header.headers_length as usize;
                if headers_end > buf_len {
                    break;
                }
                let headers_bytes = self.buffer.slice(position..headers_end);
                position = headers_end;

                match HashMap::from_bytes(headers_bytes) {
                    Ok(map) => Some(map),
                    Err(_) => break,
                }
            } else {
                None
            };

            messages.push(IggyMessage {
                header,
                payload,
                headers,
            });
        }

        messages
    }
}

impl BytesSerializable for IggyMessages {
    fn to_bytes(&self) -> Bytes {
        self.buffer.clone()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let mut messages_count = 0;
        let iterator: IggyMessageViewIterator<'_> = IggyMessageViewIterator::new(&bytes);

        for _ in iterator {
            messages_count += 1;
        }

        Ok(Self {
            buffer: bytes,
            count: messages_count,
        })
    }
}

impl Sizeable for IggyMessages {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.buffer.len() as u64)
    }
}

impl Default for IggyMessages {
    fn default() -> Self {
        Self::new(BytesMut::new().freeze(), 0)
    }
}
