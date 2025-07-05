use std::sync::Arc;
use std::{io, net::SocketAddr, pin::Pin, time::Duration};
use iggy_common::{IggyError, QuicClientConfig};
use rustls::crypto::CryptoProvider;
use tracing::{error, warn};
use crate::quic::skip_server_verification::SkipServerVerification;

use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, VarInt};

pub trait QuicFactory {
    type Conn;
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<Self::Conn, IggyError>> + Send>>;
}

pub struct QuinnFactory {
    config: Arc<QuicClientConfig>,
    ep: Arc<Endpoint>,
    server_address: SocketAddr,
}

impl QuicFactory for QuinnFactory {
    type Conn = Connection;

    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<Self::Conn, IggyError>> + Send>> {
        let ep  = self.ep.clone();
        let sn  = self.config.server_name.clone();
        let sa = self.server_address.clone();
        Box::pin(async move {
            let connecting = ep
                .connect(sa, &sn)
                .map_err(|_| IggyError::CannotEstablishConnection)?;

            connecting
                .await
                .map_err(|_| IggyError::CannotEstablishConnection)
        })
    }
}

impl QuinnFactory {
    pub fn new(config: QuicClientConfig) -> Result<Self, IggyError> {
        let cfg = Arc::new(config);

        let server_address = cfg
            .server_address
            .parse::<SocketAddr>()
            .map_err(|error| {
                error!("Invalid server address: {error}");
                IggyError::InvalidServerAddress
            })?;
        let client_address = if server_address.is_ipv6()
            && cfg.client_address == QuicClientConfig::default().client_address
        {
            "[::1]:0"
        } else {
            &cfg.client_address
        }
        .parse::<SocketAddr>()
        .map_err(|error| {
            error!("Invalid client address: {error}");
            IggyError::InvalidClientAddress
        })?;

        let quic_config = configure(&cfg)?;
        let endpoint = Endpoint::client(client_address);
        if endpoint.is_err() {
            error!("Cannot create client endpoint");
            return Err(IggyError::CannotCreateEndpoint);
        }

        let mut endpoint = endpoint.unwrap();
        endpoint.set_default_client_config(quic_config);

        Ok(Self { config: cfg, ep: Arc::new(endpoint), server_address })
    }
}

fn configure(config: &QuicClientConfig) -> Result<ClientConfig, IggyError> {
    let max_concurrent_bidi_streams = VarInt::try_from(config.max_concurrent_bidi_streams);
    if max_concurrent_bidi_streams.is_err() {
        error!(
            "Invalid 'max_concurrent_bidi_streams': {}",
            config.max_concurrent_bidi_streams
        );
        return Err(IggyError::InvalidConfiguration);
    }

    let receive_window = VarInt::try_from(config.receive_window);
    if receive_window.is_err() {
        error!("Invalid 'receive_window': {}", config.receive_window);
        return Err(IggyError::InvalidConfiguration);
    }

    let mut transport = quinn::TransportConfig::default();
    transport.initial_mtu(config.initial_mtu);
    transport.send_window(config.send_window);
    transport.receive_window(receive_window.unwrap());
    transport.datagram_send_buffer_size(config.datagram_send_buffer_size as usize);
    transport.max_concurrent_bidi_streams(max_concurrent_bidi_streams.unwrap());
    if config.keep_alive_interval > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval)));
    }
    if config.max_idle_timeout > 0 {
        let max_idle_timeout =
            IdleTimeout::try_from(Duration::from_millis(config.max_idle_timeout));
        if max_idle_timeout.is_err() {
            error!("Invalid 'max_idle_timeout': {}", config.max_idle_timeout);
            return Err(IggyError::InvalidConfiguration);
        }
        transport.max_idle_timeout(Some(max_idle_timeout.unwrap()));
    }

    if CryptoProvider::get_default().is_none() {
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!(
                "Failed to install rustls crypto provider. Error: {:?}. This may be normal if another thread installed it first.",
                e
            );
        }
    }
    let mut client_config = match config.validate_certificate {
        true => ClientConfig::with_platform_verifier(),
        false => {
            match QuinnQuicClientConfig::try_from(
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            ) {
                Ok(config) => ClientConfig::new(Arc::new(config)),
                Err(error) => {
                    error!("Failed to create QUIC client configuration: {error}");
                    return Err(IggyError::InvalidConfiguration);
                }
            }
        }
    };
    client_config.transport_config(Arc::new(transport));
    Ok(client_config)
}
