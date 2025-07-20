pub mod tcp;
pub mod tls;

use bytes::BytesMut;
use futures::{AsyncRead, AsyncWrite};
use tracing::error;
use std::{io, net::SocketAddr, pin::Pin};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream},
};
use iggy_common::IggyError;

use crate::{connection::StreamPair, proto::runtime::sync};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

pub trait SocketFactory {
    type Stream: AsyncStream;

    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>>;
}

#[derive(Debug)]
pub struct TokioTcpStream {
    reader: sync::Mutex<BufReader<OwnedReadHalf>>,
    writer: sync::Mutex<BufWriter<OwnedWriteHalf>>,
}

impl StreamPair for TokioTcpStream {
    fn send_vectored<'a>(
        &'a self,
        bufs: &'a [io::IoSlice<'_>],
    ) -> Pin<Box<dyn Future<Output = Result<(), iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            let mut w = self.writer.lock().await;
            w.write_vectored(bufs).await.map_err(|_| IggyError::TcpError)?;
            w.flush().await.map_err(|_| IggyError::TcpError)?;
            Ok(())
            // for val in bufs {
            //     self.writer.write(val).await.map_err(|e| {
            //         error!(
            //             "Failed to write data to the TCP connection: {e}",
            //         );
            //         IggyError::TcpError
            //     })?;
            // }
            // // self.writer.write_vectored(bufs).await.map_err(|e| {
            // //     error!(
            // //         "Failed to write data to the TCP connection: {e}",
            // //     );
            // //     IggyError::TcpError
            // // })?;
            // self.writer.flush().await.map_err(|e| {
            //     error!(
            //         "Failed to write data to the TCP connection: {e}",
            //     );
            //     IggyError::TcpError
            // })?;
            // Ok(())
        })
    }

    fn read_buf<'a>(
        &'a self,
        buf: &'a mut BytesMut,
    ) -> Pin<Box<dyn Future<Output = Result<usize, iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.reader.lock().await.read_buf(buf).await.map_err(|e| {
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
        writer.try_write(buf)
        Self {
            reader: sync::Mutex::new(BufReader::new(reader)),
            writer: sync::Mutex::new(BufWriter::new(writer)),
        }
    }
}
