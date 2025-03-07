use super::message_header::IGGY_MESSAGE_HEADER_SIZE;
use crate::error::IggyError;
use ::lending_iterator::prelude::*;
use std::ops::Range;

/// A mutable view of a message for in-place modifications
#[derive(Debug)]
pub struct IggyMessageViewMut<'a> {
    /// The buffer containing the message
    buffer: &'a mut [u8],
    /// The offset of the header within the buffer
    header_offset: u32,
    /// The offset of the payload within the buffer
    payload_offset: u32,
    /// The length of the payload
    payload_length: u32,
    /// The length of the headers
    headers_length: u32,
}

impl<'a> IggyMessageViewMut<'a> {
    /// Create a new mutable message view from a buffer
    pub fn new(buffer: &'a mut [u8], start_offset: u32) -> Result<Self, IggyError> {
        let buffer_len = buffer.len() as u32;
        if buffer_len < start_offset + IGGY_MESSAGE_HEADER_SIZE {
            return Err(IggyError::InvalidCommand);
        }

        let header_bytes =
            &buffer[start_offset as usize..(start_offset + IGGY_MESSAGE_HEADER_SIZE) as usize];

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
            header_bytes[44..48]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );

        let payload_length = u32::from_le_bytes(
            header_bytes[48..52]
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
        let payload_offset = start_offset + IGGY_MESSAGE_HEADER_SIZE;

        let total_size = IGGY_MESSAGE_HEADER_SIZE + payload_length + headers_length;
        if buffer_len < start_offset + total_size {
            tracing::error!(
                "1 Invalid message payload length: {}, expected: {}",
                buffer_len - start_offset,
                total_size
            );
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        Ok(Self {
            buffer,
            header_offset,
            payload_offset,
            payload_length,
            headers_length,
        })
    }

    /// Update the offset in the header
    pub fn update_offset(&mut self, offset: u64) {
        let offset_position = self.header_offset + 20;
        let bytes = offset.to_le_bytes();
        self.buffer[offset_position as usize..(offset_position + 8) as usize]
            .copy_from_slice(&bytes);
    }

    /// Update the timestamp in the header
    pub fn update_timestamp(&mut self, timestamp: u64) {
        let timestamp_position = self.header_offset + 28;
        let bytes = timestamp.to_le_bytes();
        self.buffer[timestamp_position as usize..(timestamp_position + 8) as usize]
            .copy_from_slice(&bytes);
    }

    /// Get a mutable reference to the payload
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
            [self.payload_offset as usize..(self.payload_offset + self.payload_length) as usize]
    }

    /// Get the range this message occupies in the buffer
    pub fn range(&self) -> Range<usize> {
        self.header_offset as usize..(self.header_offset as usize + self.size())
    }

    /// Get the size of the message in bytes
    pub fn size(&self) -> usize {
        IGGY_MESSAGE_HEADER_SIZE as usize
            + self.payload_length as usize
            + self.headers_length as usize
    }
}

/// Iterator over mutable message views in a buffer
pub struct IggyMessageViewMutIterator<'a> {
    buffer: &'a mut [u8],
    position: usize,
}

impl<'a> IggyMessageViewMutIterator<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }
}

#[gat]
impl LendingIterator for IggyMessageViewMutIterator<'_> {
    type Item<'next> = Result<IggyMessageViewMut<'next>, IggyError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let buffer_slice = &mut self.buffer[self.position..];
        let result = IggyMessageViewMut::new(buffer_slice, 0);

        if let Ok(view) = &result {
            self.position += view.size();
        }

        Some(result)
    }
}
