use crate::connection::tcp::SocketFactory;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub type TokioCompatStream = tokio_util::compat::Compat<tokio::net::TcpStream>;

pub struct TokioTcpFactory;

impl SocketFactory for TokioTcpFactory {
    type Stream = TokioCompatStream;

    fn connect(&self, addr: std::net::SocketAddr) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        Box::pin(async move {
            let stream = tokio::net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream.compat())
        })
    }
}
