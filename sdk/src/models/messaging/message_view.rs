use bytes::{Bytes, BytesMut};

use super::message_header::*;
use super::message_header_view::IggyMessageHeaderView;
use crate::error::IggyError;
use crate::prelude::BytesSerializable;
use std::iter::Iterator;

/// A immutable view of a message.
#[derive(Debug)]
pub struct IggyMessageView<'a> {
    buffer: &'a [u8],
    payload_offset: usize,
    headers_offset: usize,
}

impl<'a> IggyMessageView<'a> {
    /// Creates a new immutable message view from a buffer.
    pub fn new(buffer: &'a [u8]) -> Self {
        let header_view = IggyMessageHeaderView::new(&buffer[IGGY_MESSAGE_HEADER_RANGE]);
        let payload_len = header_view.payload_length() as usize;
        let payload_offset = IGGY_MESSAGE_HEADER_SIZE as usize;
        let headers_offset = payload_offset + payload_len;

        Self {
            buffer,
            payload_offset,
            headers_offset,
        }
    }

    /// Returns an immutable header view.
    pub fn header(&self) -> IggyMessageHeaderView<'_> {
        IggyMessageHeaderView::new(&self.buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize])
    }

    /// Returns an immutable slice of the user headers.
    pub fn user_headers(&self) -> &[u8] {
        &self.buffer[self.headers_offset..]
    }

    /// Returns the size of the entire message.
    pub fn size(&self) -> u32 {
        let header_view = self.header();
        IGGY_MESSAGE_HEADER_SIZE + header_view.payload_length() + header_view.headers_length()
    }

    /// Returns a reference to the payload portion.
    pub fn payload(&self) -> &[u8] {
        let header_view = self.header();
        let payload_len = header_view.payload_length() as usize;
        &self.buffer[self.payload_offset..self.payload_offset + payload_len]
    }

    /// Validates that the message view is properly formatted and has valid data.
    pub fn validate(&self) -> Result<(), IggyError> {
        if self.buffer.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        let header = self.header();
        let payload_len = header.payload_length() as usize;
        let headers_len = header.headers_length() as usize;
        let total_size = IGGY_MESSAGE_HEADER_SIZE as usize + payload_len + headers_len;

        if self.buffer.len() < total_size {
            return Err(IggyError::InvalidMessagePayloadLength);
        }
        Ok(())
    }
}

impl BytesSerializable for IggyMessageView<'_> {
    fn to_bytes(&self) -> Bytes {
        panic!("should not be used")
    }

    fn from_bytes(_bytes: Bytes) -> Result<Self, IggyError> {
        panic!("should not be used")
    }

    fn write_to_buffer(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(self.buffer);
    }

    fn get_buffer_size(&self) -> u32 {
        self.buffer.len() as u32
    }
}

/// Iterator over immutable message views in a buffer.
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
    type Item = IggyMessageView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let remaining = &self.buffer[self.position..];
        let view = IggyMessageView::new(remaining);
        self.position += view.size() as usize;
        Some(view)
    }
}
