use std::sync::Arc;

use crate::connection::tcp::SocketFactory;
use iggy_common::TcpClientConfig;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub type TokioCompatStream = tokio_util::compat::Compat<tokio::net::TcpStream>;

pub struct TokioTcpFactory {
    config: Arc<TcpClientConfig>,
}

impl SocketFactory for TokioTcpFactory {
    type Stream = TokioCompatStream;

    fn connect(&self) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let sa = self.config.server_address.clone();
        Box::pin(async move {
            let stream = tokio::net::TcpStream::connect(sa).await?;
            stream.set_nodelay(true)?;
            Ok(stream.compat())
        })
    }
}

impl TokioTcpFactory {
    pub fn new(config: TcpClientConfig) -> Self {
        Self { config: Arc::new(config) }
    }
}
