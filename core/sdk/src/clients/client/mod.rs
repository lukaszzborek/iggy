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

use crate::clients::client_builder::IggyClientBuilder;
use crate::runtime::Runtime;
use crate::runtime::default_runtime;
use iggy_common::locking::{IggySharedMut, IggySharedMutFn};

#[cfg(not(feature = "sync"))]
mod async_impl;

#[cfg(feature = "sync")]
mod sync_impl;

use crate::client_wrappers::ClientWrapper;
use crate::prelude::EncryptorKind;
use crate::prelude::IggyConsumerBuilder;
use crate::prelude::IggyError;
use crate::prelude::IggyProducerBuilder;
use iggy_common::{Consumer, Partitioner};
use std::fmt::Debug;
use std::sync::Arc;
use tracing::info;

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
///
/// It also provides the additional builders for the standalone consumer, consumer group, and producer.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IggyClient {
    pub(crate) client: IggySharedMut<ClientWrapper>,
    partitioner: Option<Arc<dyn Partitioner>>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
    rt: Arc<Runtime>,
}

impl IggyClient {
    /// Creates a new `IggyClientBuilder`.
    pub fn builder() -> IggyClientBuilder {
        IggyClientBuilder::new()
    }

    /// Creates a new `IggyClientBuilder` from the provided connection string.
    pub fn builder_from_connection_string(
        connection_string: &str,
    ) -> Result<IggyClientBuilder, IggyError> {
        IggyClientBuilder::from_connection_string(connection_string)
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport.
    pub fn new(client: ClientWrapper) -> Self {
        let client = IggySharedMut::new(client);
        IggyClient {
            client,
            partitioner: None,
            encryptor: None,
            rt: Arc::new(default_runtime()),
        }
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport and the optional implementations for the `partitioner` and `encryptor`.
    pub fn create(
        client: ClientWrapper,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = IggySharedMut::new(client);
        IggyClient {
            client,
            partitioner,
            encryptor,
            rt: Arc::new(default_runtime()),
        }
    }

    /// Returns the underlying client implementation for the specific transport.
    pub fn client(&self) -> IggySharedMut<ClientWrapper> {
        self.client.clone()
    }

    /// Returns the builder for the standalone consumer.
    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the consumer group.
    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the producer.
    pub fn producer(&self, stream: &str, topic: &str) -> Result<IggyProducerBuilder, IggyError> {
        Ok(IggyProducerBuilder::new(
            self.client.clone(),
            stream.try_into()?,
            stream.to_owned(),
            topic.try_into()?,
            topic.to_owned(),
            self.encryptor.clone(),
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use iggy_common::TransportProtocol;

    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = IggyClient::from_connection_string(value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_err());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[cfg(not(feature = "sync"))]
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[cfg(not(feature = "sync"))]
    #[tokio::test]
    async fn should_succeed_with_quic_protocol_using_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[cfg(not(feature = "sync"))]
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
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[cfg(not(feature = "sync"))]
    #[test]
    fn should_succeed_with_http_protocol_with_pat() {
        let connection_string_prefix = "iggy+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "iggypat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = IggyClient::from_connection_string(&value);
        assert!(client.is_ok());
    }
}
