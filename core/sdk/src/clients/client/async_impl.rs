use iggy_binary_protocol::{Client, SystemClient};
use iggy_common::{
    ConnectionStringUtils, DiagnosticEvent, IggyError, TransportProtocol, broadcast::Recv,
    locking::IggySharedMutFn,
};
use tracing::{debug, error, warn};

use crate::{
    http::http_client::HttpClient,
    prelude::{ClientWrapper, IggyClient, TcpClient},
    quic::quic_client::QuicClient,
    runtime::RuntimeExecutor,
};

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(ClientWrapper::Tcp(TcpClient::default()))
    }
}

impl IggyClient {
    /// Creates a new `IggyClient` from the provided connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => Ok(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Quic => Ok(IggyClient::new(ClientWrapper::Quic(
                QuicClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Http => Ok(IggyClient::new(ClientWrapper::Http(
                HttpClient::from_connection_string(connection_string)?,
            ))),
        }
    }
}

#[maybe_async::async_impl]
#[async_trait::async_trait]
impl Client for IggyClient {
    async fn connect(&self) -> Result<(), IggyError> {
        let heartbeat_interval;
        {
            let client = self.client.read().await;
            client.connect().await?;
            heartbeat_interval = client.heartbeat_interval().await;
        }

        let client = self.client.clone();
        let rt = self.rt.clone();
        self.rt.spawn(async move {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(error) = client.read().await.ping().await {
                    error!("There was an error when sending a heartbeat. {error}");
                    if error == IggyError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                }
                rt.sleep(heartbeat_interval).await
            }
        });
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().await.disconnect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.client.read().await.shutdown().await
    }

    async fn subscribe_events(&self) -> Recv<DiagnosticEvent> {
        self.client.read().await.subscribe_events().await
    }
}
