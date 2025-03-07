use super::message_header::{IggyMessageHeader, IGGY_MESSAGE_HEADER_SIZE};
use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::models::header::{HeaderKey, HeaderValue};
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::utils::timestamp::IggyTimestamp;
use crate::utils::varint::IggyVarInt;
use ::lending_iterator::prelude::*;
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;

// Re-export related message structs
pub use super::message_view_mut::{IggyMessageViewMut, IggyMessageViewMutIterator};
pub use super::messages::IggyMessages;
pub use super::messages_mut::IggyMessagesMut;

/// A zero-copy view of a message in a buffer
#[derive(Debug, PartialEq)]
pub struct IggyMessageView<'a> {
    /// The buffer containing the message
    buffer: &'a [u8],
    /// The offset of the header within the buffer
    header_offset: usize,
    /// The offset of the payload within the buffer
    payload_offset: usize,
    /// The length of the payload
    payload_length: u32,
    /// The offset of the headers within the buffer
    headers_offset: usize,
    /// The length of the headers
    headers_length: u32,
}

impl<'a> IggyMessageView<'a> {
    /// Create a new message view from a buffer
    ///
    /// # Safety
    /// The caller must ensure that the buffer contains a valid message at the given offset
    pub fn new(buffer: &'a [u8], start_offset: usize) -> Result<Self, IggyError> {
        if buffer.len() < start_offset + IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }

        let header_slice = &buffer[start_offset..start_offset + IGGY_MESSAGE_HEADER_SIZE as usize];

        // The message header format must match IggyMessageHeader layout
        // Message header (60 bytes):
        // - Record size (4 bytes): 0-4
        // - Message ID (16 bytes): 4-20
        // - Offset (8 bytes): 20-28
        // - Timestamp (8 bytes): 28-36
        // - Origin timestamp (8 bytes): 36-44
        // - Headers length (4 bytes): 44-48
        // - Payload length (4 bytes): 48-52
        // - Checksum (8 bytes): 52-60

        let headers_length = u32::from_le_bytes(
            header_slice[44..48]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let payload_length = u32::from_le_bytes(
            header_slice[48..52]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        const MAX_REASONABLE_SIZE: u32 = 100 * 1024 * 1024;
        if payload_length > MAX_REASONABLE_SIZE || headers_length > MAX_REASONABLE_SIZE {
            tracing::error!(
                "Unreasonable message size detected - payload: {} bytes, headers: {} bytes",
                payload_length,
                headers_length
            );
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        let header_offset = start_offset;
        let payload_offset = start_offset + IGGY_MESSAGE_HEADER_SIZE as usize;
        let headers_offset = payload_offset + payload_length as usize;

        let total_size =
            IGGY_MESSAGE_HEADER_SIZE as usize + payload_length as usize + headers_length as usize;
        if buffer.len() < start_offset + total_size {
            tracing::error!(
                "2 Invalid message payload buffer.len(): {}, start_offset: {}, total_size: {}",
                buffer.len(),
                start_offset,
                total_size
            );
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        Ok(Self {
            buffer,
            header_offset,
            payload_offset,
            payload_length,
            headers_offset,
            headers_length,
        })
    }

    /// Get the message header
    pub fn header(&self) -> Result<IggyMessageHeader, IggyError> {
        let header_end = self.header_offset + IGGY_MESSAGE_HEADER_SIZE as usize;
        let header_bytes = &self.buffer[self.header_offset..header_end];

        // Convert to Bytes for compatibility with existing code
        let bytes = Bytes::copy_from_slice(header_bytes);
        IggyMessageHeader::from_bytes(bytes)
    }

    /// Get the message ID
    pub fn id(&self) -> u128 {
        let id_bytes = &self.buffer[self.header_offset + 4..self.header_offset + 20];
        u128::from_le_bytes(id_bytes.try_into().unwrap())
    }

    /// Get the message offset
    pub fn offset(&self) -> u64 {
        let offset_bytes = &self.buffer[self.header_offset + 20..self.header_offset + 28];
        u64::from_le_bytes(offset_bytes.try_into().unwrap())
    }

    /// Get the message timestamp
    pub fn timestamp(&self) -> u64 {
        let timestamp_bytes = &self.buffer[self.header_offset + 28..self.header_offset + 36];
        u64::from_le_bytes(timestamp_bytes.try_into().unwrap())
    }

    /// Get the message origin timestamp
    pub fn origin_timestamp(&self) -> u64 {
        let timestamp_bytes = &self.buffer[self.header_offset + 36..self.header_offset + 44];
        u64::from_le_bytes(timestamp_bytes.try_into().unwrap())
    }

    /// Get the payload as a slice
    pub fn payload(&self) -> &'a [u8] {
        &self.buffer[self.payload_offset..self.payload_offset + self.payload_length as usize]
    }

    /// Get the headers as bytes
    pub fn headers_bytes(&self) -> &'a [u8] {
        if self.headers_length == 0 {
            &[]
        } else {
            &self.buffer[self.headers_offset..self.headers_offset + self.headers_length as usize]
        }
    }

    /// Parse and get the headers as a HashMap
    pub fn headers(&self) -> Result<Option<HashMap<HeaderKey, HeaderValue>>, IggyError> {
        if self.headers_length == 0 {
            return Ok(None);
        }

        let headers_bytes = self.headers_bytes();
        let headers_map = parse_headers(headers_bytes)?;
        Ok(Some(headers_map))
    }

    /// Get the total size of the message in bytes
    pub fn size(&self) -> usize {
        IGGY_MESSAGE_HEADER_SIZE as usize
            + self.payload_length as usize
            + self.headers_length as usize
    }

    /// Get the total range this message occupies in the buffer
    pub fn range(&self) -> Range<usize> {
        self.header_offset..(self.header_offset + self.size())
    }
}

/// Iterator over immutable message views in a buffer
pub struct IggyMessageViewIterator<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> IggyMessageViewIterator<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

impl<'a> Iterator for IggyMessageViewIterator<'a> {
    type Item = Result<IggyMessageView<'a>, IggyError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let result = IggyMessageView::new(self.buffer, self.position);
        match &result {
            Ok(view) => {
                self.position += view.size();
            }
            Err(_) => {}
        }

        Some(result)
    }
}

/// Parse headers from raw bytes
fn parse_headers(bytes: &[u8]) -> Result<HashMap<HeaderKey, HeaderValue>, IggyError> {
    let mut result = HashMap::new();
    let mut position = 0;

    while position < bytes.len() {
        let (key_len_size, key_len) = IggyVarInt::decode(&bytes[position..]);
        position += key_len_size;

        if position + key_len as usize > bytes.len() {
            return Err(IggyError::InvalidCommand);
        }

        let key_bytes = &bytes[position..position + key_len as usize];
        let key_str = std::str::from_utf8(key_bytes).map_err(|_| IggyError::InvalidCommand)?;
        let key = HeaderKey::new(key_str)?;
        position += key_len as usize;

        let (value_len_size, value_len) = IggyVarInt::decode(&bytes[position..]);
        position += value_len_size;

        if position + value_len as usize > bytes.len() {
            return Err(IggyError::InvalidCommand);
        }

        let value_bytes = &bytes[position..position + value_len as usize];
        let bytes_value = Bytes::copy_from_slice(value_bytes);
        let value = HeaderValue::from_raw(&bytes_value)?;
        position += value_len as usize;

        result.insert(key, value);
    }

    Ok(result)
}
