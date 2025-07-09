pub mod tcp;
pub mod tls;

use std::{io, net::SocketAddr, pin::Pin};
use futures::{AsyncRead, AsyncWrite};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub trait SocketFactory {
    type Stream: AsyncStream;

    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

