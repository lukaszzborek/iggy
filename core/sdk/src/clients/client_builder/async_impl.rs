use iggy_common::{AutoLogin, IggyDuration};
use iggy_common::{
    ConnectionStringUtils, HttpClientConfigBuilder, IggyError, QuicClientConfigBuilder,
    TcpClientConfigBuilder, TransportProtocol,
};
use std::sync::Arc;

use crate::prelude::IggyClient;
use crate::{
    http::http_client::HttpClient,
    prelude::{ClientWrapper, IggyClientBuilder, TcpClient},
    quic::quic_client::QuicClient,
};

impl IggyClientBuilder {
    /// Creates a new `IggyClientBuilder` from the provided connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, IggyError> {
        let mut builder = Self::new();

        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => {
                builder.client = Some(ClientWrapper::Tcp(TcpClient::from_connection_string(
                    connection_string,
                )?));
            }
            TransportProtocol::Quic => {
                builder.client = Some(ClientWrapper::Quic(QuicClient::from_connection_string(
                    connection_string,
                )?));
            }
            TransportProtocol::Http => {
                builder.client = Some(ClientWrapper::Http(HttpClient::from_connection_string(
                    connection_string,
                )?));
            }
        }

        Ok(builder)
    }

    /// This method provides fluent API for the TCP client configuration.
    /// It returns the `TcpClientBuilder` instance, which allows to configure the TCP client with custom settings or using defaults.
    /// This should be called after the non-protocol specific methods, such as `with_partitioner`, `with_encryptor` or `with_message_handler`.
    pub fn with_tcp(self) -> TcpClientBuilder {
        TcpClientBuilder {
            config: TcpClientConfigBuilder::default(),
            parent_builder: self,
        }
    }

    /// This method provides fluent API for the QUIC client configuration.
    /// It returns the `QuicClientBuilder` instance, which allows to configure the QUIC client with custom settings or using defaults.
    /// This should be called after the non-protocol specific methods, such as `with_partitioner`, `with_encryptor` or `with_message_handler`.
    pub fn with_quic(self) -> QuicClientBuilder {
        QuicClientBuilder {
            config: QuicClientConfigBuilder::default(),
            parent_builder: self,
        }
    }

    /// This method provides fluent API for the HTTP client configuration.
    /// It returns the `HttpClientBuilder` instance, which allows to configure the HTTP client with custom settings or using defaults.
    /// This should be called after the non-protocol specific methods, such as `with_partitioner`, `with_encryptor` or `with_message_handler`.
    pub fn with_http(self) -> HttpClientBuilder {
        HttpClientBuilder {
            config: HttpClientConfigBuilder::default(),
            parent_builder: self,
        }
    }
}

#[derive(Debug, Default)]
pub struct TcpClientBuilder {
    config: TcpClientConfigBuilder,
    parent_builder: IggyClientBuilder,
}

impl TcpClientBuilder {
    /// Sets the server address for the TCP client.
    pub fn with_server_address(mut self, server_address: String) -> Self {
        self.config = self.config.with_server_address(server_address);
        self
    }

    /// Sets the auto sign in during connection.
    pub fn with_auto_sign_in(mut self, auto_sign_in: AutoLogin) -> Self {
        self.config = self.config.with_auto_sign_in(auto_sign_in);
        self
    }

    /// Sets the number of max retries when connecting to the server.
    pub fn with_reconnection_max_retries(mut self, reconnection_retries: Option<u32>) -> Self {
        self.config = self
            .config
            .with_reconnection_max_retries(reconnection_retries);
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, reconnection_interval: IggyDuration) -> Self {
        self.config = self
            .config
            .with_reconnection_interval(reconnection_interval);
        self
    }

    /// Sets whether to use TLS when connecting to the server.
    pub fn with_tls_enabled(mut self, tls_enabled: bool) -> Self {
        self.config = self.config.with_tls_enabled(tls_enabled);
        self
    }

    /// Sets the domain to use for TLS when connecting to the server.
    pub fn with_tls_domain(mut self, tls_domain: String) -> Self {
        self.config = self.config.with_tls_domain(tls_domain);
        self
    }

    /// Sets the path to the CA file for TLS.
    pub fn with_tls_ca_file(mut self, tls_ca_file: String) -> Self {
        self.config = self.config.with_tls_ca_file(tls_ca_file);
        self
    }

    /// Sets whether to validate the TLS certificate.
    pub fn with_tls_validate_certificate(mut self, tls_validate_certificate: bool) -> Self {
        self.config = self
            .config
            .with_tls_validate_certificate(tls_validate_certificate);
        self
    }

    /// Sets the nodelay option for the TCP socket.
    pub fn with_no_delay(mut self) -> Self {
        self.config = self.config.with_no_delay();
        self
    }

    /// Builds the parent `IggyClient` with TCP configuration.
    pub fn build(self) -> Result<IggyClient, IggyError> {
        let client = TcpClient::create(Arc::new(self.config.build()?))?;
        let client = self
            .parent_builder
            .with_client(ClientWrapper::Tcp(client))
            .build()?;
        Ok(client)
    }
}

#[derive(Debug, Default)]
pub struct QuicClientBuilder {
    config: QuicClientConfigBuilder,
    parent_builder: IggyClientBuilder,
}

impl QuicClientBuilder {
    /// Sets the server address for the QUIC client.
    pub fn with_server_address(mut self, server_address: String) -> Self {
        self.config = self.config.with_server_address(server_address);
        self
    }

    /// Sets the auto sign in during connection.
    pub fn with_auto_sign_in(mut self, auto_sign_in: AutoLogin) -> Self {
        self.config = self.config.with_auto_sign_in(auto_sign_in);
        self
    }

    /// Sets the number of retries when connecting to the server.
    pub fn with_reconnection_max_retries(mut self, reconnection_retries: Option<u32>) -> Self {
        self.config = self
            .config
            .with_reconnection_max_retries(reconnection_retries);
        self
    }

    /// Sets the interval between retries when connecting to the server.
    pub fn with_reconnection_interval(mut self, reconnection_interval: IggyDuration) -> Self {
        self.config = self
            .config
            .with_reconnection_interval(reconnection_interval);
        self
    }

    /// Sets the server name for the QUIC client.
    pub fn with_server_name(mut self, server_name: String) -> Self {
        self.config = self.config.with_server_name(server_name);
        self
    }

    /// Builds the parent `IggyClient` with QUIC configuration.
    pub fn build(self) -> Result<IggyClient, IggyError> {
        let client = QuicClient::create(Arc::new(self.config.build()))?;
        let client = self
            .parent_builder
            .with_client(ClientWrapper::Quic(client))
            .build()?;
        Ok(client)
    }
}

#[derive(Debug, Default)]
pub struct HttpClientBuilder {
    config: HttpClientConfigBuilder,
    parent_builder: IggyClientBuilder,
}

#[cfg(not(feature = "sync"))]
impl HttpClientBuilder {
    /// Sets the server address for the HTTP client.
    pub fn with_api_url(mut self, api_url: String) -> Self {
        self.config = self.config.with_api_url(api_url);
        self
    }

    /// Sets the number of retries for the HTTP client.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.config = self.config.with_retries(retries);
        self
    }

    /// Builds the parent `IggyClient` with HTTP configuration.
    pub fn build(self) -> Result<IggyClient, IggyError> {
        let client = HttpClient::create(Arc::new(self.config.build()))?;
        let client = self
            .parent_builder
            .with_client(ClientWrapper::Http(client))
            .build()?;
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use iggy_common::TransportProtocol;

    use super::*;

    #[tokio::test]
    async fn should_succeed_with_quic_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }
}
