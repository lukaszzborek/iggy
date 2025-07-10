pub mod tcp;
pub mod tls;

use std::{io, net::SocketAddr, pin::Pin};
use futures::{AsyncRead, AsyncWrite};
use tokio::{io::{AsyncReadExt, BufReader}, net::tcp::OwnedReadHalf};

use crate::connection::StreamPair;

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub trait SocketFactory {
    type Stream: AsyncStream;

    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

pub struct TokioTcpStream {
    reader: BufReader<OwnedReadHalf>,
    buf: 
}

impl StreamPair for TokioTcpStream {
    fn read_chunk<'a>(&'a mut self, at_most: usize) -> Pin<Box<dyn Future<Output = Result<Option<bytes::Bytes>, iggy_common::IggyError>> + Send + 'a>> {

    }
}
