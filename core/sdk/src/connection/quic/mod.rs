use std::sync::Arc;

use iggy_common::QuicClientConfig;
use quinn::{ClientConfig, Endpoint};

use crate::connection::SocketFactory;

pub struct QuicFactory {
    config: ClientConfig,
    quic_config: Arc<QuicClientConfig>, // rename to QuicFabricConfig
}

impl SocketFactory for QuicFactory {
    type Stream = Arc<quinn::Connection>;

    fn connect(&self, addr: std::net::SocketAddr) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let endpoint = Endpoint::client(addr)?;
        let config = self.config.clone();
        endpoint.set_default_client_config(config);

        let connecting = endpoint.connect(addr, &self.quic_config.server_name)?;
        Box::pin(async move {
            let connection = connecting.await?;
            Ok(stream)
        })
    }
}
