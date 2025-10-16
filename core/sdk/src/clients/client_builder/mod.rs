/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::client_wrappers::ClientWrapper;
use crate::clients::client::IggyClient;
use crate::prelude::{EncryptorKind, IggyError, Partitioner};
use std::sync::Arc;
use tracing::error;

#[cfg(not(feature = "sync"))]
mod async_impl;

#[cfg(feature = "sync")]
mod sync_impl;

/// The builder for the `IggyClient` instance, which allows to configure and provide custom implementations for the partitioner, encryptor or message handler.
#[derive(Debug, Default)]
pub struct IggyClientBuilder {
    client: Option<ClientWrapper>,
    partitioner: Option<Arc<dyn Partitioner>>,
    encryptor: Option<Arc<EncryptorKind>>,
}

impl IggyClientBuilder {
    /// Creates a new `IggyClientBuilder`.
    /// This is not enough to build the `IggyClient` instance. You need to provide the client configuration or the client implementation for the specific transport.
    pub fn new() -> Self {
        IggyClientBuilder::default()
    }

    /// Apply the provided client implementation for the specific transport. Setting client clears the client config.
    pub fn with_client(mut self, client: ClientWrapper) -> Self {
        self.client = Some(client);
        self
    }

    /// Use the custom partitioner implementation.
    pub fn with_partitioner(mut self, partitioner: Arc<dyn Partitioner>) -> Self {
        self.partitioner = Some(partitioner);
        self
    }

    /// Use the custom encryptor implementation.
    pub fn with_encryptor(mut self, encryptor: Arc<EncryptorKind>) -> Self {
        self.encryptor = Some(encryptor);
        self
    }

    /// Build the `IggyClient` instance.
    /// This method returns an error if the client is not provided.
    /// If the client is provided, it creates the `IggyClient` instance with the provided configuration.
    /// To provide the client configuration, use the `with_tcp`, `with_quic` or `with_http` methods.
    pub fn build(self) -> Result<IggyClient, IggyError> {
        let Some(client) = self.client else {
            error!("Client is not provided");
            return Err(IggyError::InvalidConfiguration);
        };

        Ok(IggyClient::create(client, self.partitioner, self.encryptor))
    }
}

#[cfg(test)]
mod tests {
    use iggy_common::TransportProtocol;

    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client_builder = IggyClientBuilder::from_connection_string(value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "iggy://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
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
    fn should_succeed_with_tcp_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client_builder = IggyClientBuilder::from_connection_string(&value);
        assert!(client_builder.is_ok());
    }
}
