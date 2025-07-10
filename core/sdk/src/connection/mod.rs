use std::{io::IoSlice, pin::Pin};

use bytes::Bytes;
use iggy_common::IggyError;

pub mod tcp;
pub mod quic;

pub trait ConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>>;
    fn is_alive(&self) -> Pin<Box<dyn Future<Output = bool>>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>>;
}

pub trait StreamConnectionFactory: ConnectionFactory {
    type Stream: StreamPair;

    fn open_stream(&self) -> Pin<Box<dyn Future<Output = Result<Self::Stream, IggyError>> + Send + '_>>;
}

pub trait StreamPair: Send {
    fn send_vectored<'a>(&'a mut self, bufs: &'a [IoSlice<'_>]) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + 'a>>;
    fn read_buf<'a>(&'a mut self, buf: &'a mut [u8]) -> Pin<Box<dyn Future<Output = Result<usize, IggyError>> + Send + 'a>>;
}
