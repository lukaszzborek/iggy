use std::{net::SocketAddr, sync::Arc};

use crate::{
    connection::{tcp::{SocketFactory, TokioTcpStream}, ConnectionFactory},
    proto::runtime::sync,
};
use iggy_common::IggyError;
use iggy_common::TcpClientConfig;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tracing::error;

pub type TokioCompatStream = tokio_util::compat::Compat<tokio::net::TcpStream>;

pub struct TokioTcpFactory {
    pub(crate) config: Arc<TcpClientConfig>,
    client_address: Arc<sync::Mutex<Option<SocketAddr>>>,
    pub(crate) stream: Arc<sync::Mutex<Option<TokioTcpStream>>>,
}

impl ConnectionFactory for TokioTcpFactory {

    fn connect(
        &self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>>
    {
        let addr = self.client_address.clone();
        let confg = self.config.clone();
        let tokio_tcp_stream = self.stream.clone();
        Box::pin(async move {
            let mut address = addr.lock().await;
            let mut tokio_tcp_stream = tokio_tcp_stream.lock().await;

            let conn = TcpStream::connect(&confg.server_address)
                .await
                .map_err(|error| {
                    error!("Failed to establish TCP connection to the server: {error}",);
                    IggyError::CannotEstablishConnection
                })?;
            let client_address = conn.local_addr().map_err(|error| {
                error!("Failed to get the local address of the client: {error}",);
                IggyError::CannotEstablishConnection
            })?;
            // let remote_address = stream.peer_addr().map_err(|error| {
            //     error!("Failed to get the remote address of the server: {error}",);
            //     IggyError::CannotEstablishConnection
            // })?;
            let _ = address.insert(client_address);
            if let Err(e) = conn.set_nodelay(confg.nodelay) {
                error!("Failed to set the nodelay option on the client: {e}");
            }
            // TODO add tls
            let _ = tokio_tcp_stream.insert(TokioTcpStream::new(conn));

            Ok(())
        })
    }

    // TODO пока заглушка, нужно подумать насчет того, как это делать
    fn is_alive(&self) -> std::pin::Pin<Box<dyn Future<Output = bool>>> {
        let conn = self.stream.clone();
        Box::pin(async move {
            let conn = conn.lock().await;
            match conn.as_ref() {
                Some(c) => true,
                None => false,
            }
        })
    }

    fn shutdown(&self) -> std::pin::Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>> {
        let conn = self.stream.clone();
        Box::pin(async move {
            if let Some(mut conn) = conn.lock().await.take() {
                conn.writer.shutdown().await.map_err(|e| {
                    error!(
                        "Failed to shutdown the TCP connection to the TCP connection: {e}",
                    );
                    IggyError::TcpError
                })?;
            }
            Ok(())
        })
    }
}
