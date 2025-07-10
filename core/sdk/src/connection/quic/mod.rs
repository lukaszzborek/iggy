use std::io::IoSlice;
use std::sync::Arc;
use std::{io, net::SocketAddr, pin::Pin, time::Duration};
use bytes::Bytes;
use iggy_common::{IggyError, QuicClientConfig};
use rustls::crypto::CryptoProvider;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, warn};
use crate::connection::{ConnectionFactory, StreamConnectionFactory, StreamPair};
use crate::proto::runtime::sync;
use crate::quic::skip_server_verification::SkipServerVerification;

use quinn::crypto::rustls::QuicClientConfig as QuinnQuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, RecvStream, SendStream, VarInt};

pub struct QuinnStreamPair {
    send: SendStream,
    recv: RecvStream,
}

impl StreamPair for QuinnStreamPair {
    fn send_vectored<'a>(&'a mut self, bufs: &'a [IoSlice<'_>]) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.send.write_vectored(bufs).await.map_err(|e| {
                error!("Failed to write vectored buffs to quic conn: {e}");
                IggyError::QuicError
            })?;
            self.send.finish().map_err(|e: quinn::ClosedStream| {
                error!("Failed to finish sending data: {e}");
                IggyError::QuicError
            })?;
            Ok(())
        })
    }

    fn read_buf<'a>(&'a mut self, mut buf: &'a mut [u8]) -> Pin<Box<dyn Future<Output = Result<usize, IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.recv.read_buf(&mut buf).await.map_err(|e| {
                error!("Failed to read chunk: {e}");
                IggyError::QuicError
            })
        })
    }
}

pub struct QuinnFactory {
    config: Arc<QuicClientConfig>,
    ep: Arc<Endpoint>,
    connection: Arc<sync::Mutex<Option<Connection>>>,
    server_address: SocketAddr,
}

impl ConnectionFactory for QuinnFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>> {
        let ep  = self.ep.clone();
        let sn  = self.config.server_name.clone();
        let sa = self.server_address.clone();
        let conn = self.connection.clone();
        Box::pin(async move {
            let mut connection = conn.lock().await;
            let connecting = ep
                .connect(sa, &sn)
                .map_err(|_| IggyError::CannotEstablishConnection)?;

            let new_conn = connecting
                .await
                .map_err(|_| IggyError::CannotEstablishConnection)?;
            let _ = connection.insert(new_conn);
            Ok(())
        })
    }

    fn is_alive(&self) -> Pin<Box<dyn Future<Output = bool>>> {
        let conn = self.connection.clone();
        Box::pin(async move {
            let conn = conn.lock().await;
            match conn.as_ref() {
                Some(c) => c.close_reason().is_some(),
                None => false,
            }
        })
    }

    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), IggyError>> + Send + Sync>> {
        let conn = self.connection.clone();
        let ep  = self.ep.clone();
        Box::pin(async move {
            if let Some(conn) = conn.lock().await.take() {
                conn.close(0u32.into(), b"");
            }
            ep.wait_idle().await;
            Ok(())
        })
    }
}

impl StreamConnectionFactory for QuinnFactory {
    type Stream = QuinnStreamPair;

    fn open_stream(&self) -> Pin<Box<dyn Future<Output = Result<Self::Stream, IggyError>> + Send + '_>> {
        let conn = self.connection.clone();
        Box::pin(async move {
            let conn = {
                let guard = conn.lock().await;
                guard.clone().ok_or(IggyError::NotConnected)?
            };
            let (send, recv) = conn.open_bi().await.map_err(|e| {
                error!("Failed to open a bidirectional stream: {e}");
                IggyError::QuicError 
            })?;
            Ok(QuinnStreamPair { send, recv })
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

        Ok(Self { config: cfg, ep: Arc::new(endpoint), server_address, connection: Arc::new(sync::Mutex::new(None)) })
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
