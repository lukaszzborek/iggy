use iggy_binary_protocol::{Client, SystemClient};
use iggy_common::{
    ConnectionStringUtils, DiagnosticEvent, IggyError, TransportProtocol, broadcast::Recv,
    locking::IggySharedMutFn,
};
use tracing::{debug, error, warn};

use crate::{
    prelude::{ClientWrapper, IggyClient, TcpClient},
    runtime::RuntimeExecutor,
};
use iggy_common::TcpClientConfig;
use std::sync::Arc;

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(ClientWrapper::Tcp(
            TcpClient::create(Arc::new(TcpClientConfig::default())).unwrap(),
        ))
    }
}

impl IggyClient {
    // Creates a new `IggyClient` from the provided connection string.
    // Note: In sync mode, this always creates a plain TCP client without TLS.
    // To use TLS, please use the client_provider with appropriate TcpClientConfig.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => Ok(IggyClient::new(ClientWrapper::Tcp(
                TcpClient::from_connection_string(connection_string)?,
            ))),
            _ => Err(IggyError::InvalidConfiguration),
        }
    }
}

#[maybe_async::sync_impl]
impl Client for IggyClient {
    fn connect(&self) -> Result<(), IggyError> {
        let heartbeat_interval;
        {
            let client = self.client.read();
            client.connect()?;
            heartbeat_interval = client.heartbeat_interval();
        }

        let client = self.client.clone();
        let rt = self.rt.clone();
        self.rt.spawn(move || {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(e) = client.read().ping() {
                    error!("There was an error when sending a heartbeat. {e}");
                    if e == IggyError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                }
                rt.sleep(heartbeat_interval);
            }
        });
        Ok(())
    }

    fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().disconnect()
    }

    fn shutdown(&self) -> Result<(), IggyError> {
        self.client.read().shutdown()
    }

    fn subscribe_events(&self) -> Recv<DiagnosticEvent> {
        self.client.read().subscribe_events()
    }
}
