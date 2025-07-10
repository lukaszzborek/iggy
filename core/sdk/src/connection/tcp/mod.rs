pub mod tcp;
pub mod tls;

use futures::{AsyncRead, AsyncWrite};
use tracing::error;
use std::{io, net::SocketAddr, pin::Pin};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream},
};
use iggy_common::IggyError;

use crate::connection::StreamPair;

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub trait SocketFactory {
    type Stream: AsyncStream;

    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

pub struct TokioTcpStream {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl StreamPair for TokioTcpStream {
    fn send_vectored<'a>(
        &'a mut self,
        bufs: &'a [io::IoSlice<'_>],
    ) -> Pin<Box<dyn Future<Output = Result<(), iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.writer.write_vectored(bufs).await.map_err(|e| {
                error!(
                    "Failed to write data to the TCP connection: {e}",
                );
                IggyError::TcpError
            })?;
            Ok(())
        })
    }

    fn read_buf<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.reader.read_exact(buf).await.map_err(|e| {
                error!(
                    "Failed to read data from the TCP connection: {e}",
                );
                IggyError::TcpError
            })
        })
    }
}

impl TokioTcpStream {
    fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}
