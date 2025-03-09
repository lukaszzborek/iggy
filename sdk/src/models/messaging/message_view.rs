use super::message_header::*;
use super::IggyMessageHeaderView;
use crate::error::IggyError;
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
    pub fn new(buffer: &'a [u8]) -> Result<Self, IggyError> {
        if buffer.len() < IGGY_MESSAGE_HEADER_SIZE as usize {
            return Err(IggyError::InvalidCommand);
        }
        // Create a header view from the header slice.
        let header_view =
            IggyMessageHeaderView::new(&buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize])?;
        let payload_len = header_view.payload_length() as usize;
        let headers_len = header_view.headers_length() as usize;
        let total_size = IGGY_MESSAGE_HEADER_SIZE as usize + payload_len + headers_len;

        if buffer.len() < total_size {
            return Err(IggyError::InvalidMessagePayloadLength);
        }

        Ok(Self {
            buffer,
            payload_offset: IGGY_MESSAGE_HEADER_SIZE as usize,
            headers_offset: IGGY_MESSAGE_HEADER_SIZE as usize + payload_len,
        })
    }

    /// Returns an immutable header view.
    pub fn msg_header(&self) -> Result<IggyMessageHeaderView<'_>, IggyError> {
        IggyMessageHeaderView::new(&self.buffer[0..IGGY_MESSAGE_HEADER_SIZE as usize])
    }

    /// Returns an immutable slice of the user headers.
    pub fn headers(&self) -> &[u8] {
        &self.buffer[self.headers_offset..]
    }

    /// Returns the size of the entire message.
    pub fn size(&self) -> usize {
        let header_view = self.msg_header().expect("header must be valid");
        IGGY_MESSAGE_HEADER_SIZE as usize
            + header_view.payload_length() as usize
            + header_view.headers_length() as usize
    }

    /// Returns a reference to the payload portion.
    pub fn payload(&self) -> &[u8] {
        let header_view = self.msg_header().expect("header must be valid");
        let payload_len = header_view.payload_length() as usize;
        &self.buffer[self.payload_offset..self.payload_offset + payload_len]
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
    type Item = Result<IggyMessageView<'a>, IggyError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        let remaining = &self.buffer[self.position..];
        match IggyMessageView::new(remaining) {
            Ok(view) => {
                self.position += view.size();
                Some(Ok(view))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
